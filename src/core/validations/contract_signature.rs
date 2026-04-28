//! On-chain signature verification for EOA, ERC-1271, and ERC-6492 signatures.
//!
//! Dispatches in a single `eth_call`:
//! * **Address has code** → ERC-1271 `isValidSignature(bytes32,bytes)` → match
//!   `0x1626ba7e` magic value.
//! * **Address has no code** → deploy AmbireTech's `ValidateSigOffchain` helper,
//!   which internally checks the `0x64926492…` ERC-6492 suffix and otherwise
//!   falls through to `ecrecover` for plain EOA signatures.
//!
//! The Rust side is inlined from `royal-markets/eth-signature-verifier` (MIT,
//! © 2024 Royal Markets, Inc.), which in turn extracted and lightly modified it
//! from Reown's (formerly WalletConnect's) `reown-com/erc6492` (MIT, © 2024
//! WalletConnect, Inc.).
//!
//! The Solidity helper (`ValidateSigOffchain` / `UniversalSigValidator`) and its
//! compiled bytecode live in [`contract_signature/`](./contract_signature/);
//! see [`contract_signature/README.md`](./contract_signature/README.md) for
//! provenance and the bit-exact build recipe.
//!
//! The committed `contract_signature/ValidateSigOffchain.bytecode` must match
//! SHA-256 `7929acc3c7f14ff8fcfe6b7752a6b0327811da4697ce7b3bb438bf7b237ab410`.
//! This invariant is enforced on every PR that touches `contract_signature/`
//! by the `verify-contract-signature-bytecode` CI job.

use alloy_primitives::{Address, Bytes, FixedBytes};
use alloy_provider::Provider;
use alloy_rpc_types::{TransactionInput, TransactionRequest};
use alloy_sol_types::{sol, SolCall, SolConstructor};
use alloy_transport::{RpcError, Transport, TransportErrorKind};

/// Counterfactual-deployment helper from AmbireTech's ERC-6492 reference
/// implementation. See [`contract_signature/README.md`](./contract_signature/README.md).
const VALIDATE_SIG_OFFCHAIN_BYTECODE: &[u8] =
    include_bytes!("contract_signature/ValidateSigOffchain.bytecode");

/// First byte of the return value indicates success for `ValidateSigOffchain`.
const SUCCESS_RESULT: u8 = 0x01;

/// EIP-1271 magic value returned by `isValidSignature` on success.
const MAGIC_VALUE: u32 = 0x1626ba7e;

sol! {
    contract ValidateSigOffchain {
        constructor(address _signer, bytes32 _hash, bytes memory _signature);
    }

    contract Erc1271 {
        function isValidSignature(bytes32 _hash, bytes calldata _signature) external view returns (bytes4);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum Verification {
    Valid,
    Invalid,
}

impl Verification {
    pub fn is_valid(self) -> bool {
        matches!(self, Verification::Valid)
    }
}

pub type SignatureRpcError = RpcError<TransportErrorKind>;

/// A reverting contract call means the signature didn't validate on-chain; only
/// propagate the RPC error for transport-level failures. Applied uniformly to
/// both branches so a wallet that `require()`s on invalid signatures is reported
/// as `Invalid` rather than surfaced as a transport error.
fn reverts_mean_invalid(e: SignatureRpcError) -> Result<Verification, SignatureRpcError> {
    if let Some(error_response) = e.as_error_resp() {
        if error_response.message.starts_with("execution reverted") {
            return Ok(Verification::Invalid);
        }
    }
    Err(e)
}

/// Verifies an EOA, ERC-1271, or ERC-6492 signature against the given address and message hash.
///
/// * If the address has code, the call is dispatched to `isValidSignature` (ERC-1271).
/// * Otherwise, the `ValidateSigOffchain` helper is deployed in an `eth_call` to cover
///   EOA signatures and counterfactually-deployed ERC-6492 smart wallets.
pub async fn verify_signature<S, P, T>(
    signature: S,
    address: Address,
    message: FixedBytes<32>,
    provider: P,
) -> Result<Verification, SignatureRpcError>
where
    S: Into<Bytes>,
    P: Provider<T>,
    T: Transport + Clone,
{
    let has_code = !provider.get_code_at(address).await?.is_empty();
    if has_code {
        let call_request = TransactionRequest::default()
            .to(address)
            .input(TransactionInput::new(
                Erc1271::isValidSignatureCall {
                    _hash: message,
                    _signature: signature.into(),
                }
                .abi_encode()
                .into(),
            ));

        match provider.call(&call_request).await {
            Ok(result) => {
                let magic = result.get(..4);
                if magic == Some(&MAGIC_VALUE.to_be_bytes()[..]) {
                    Ok(Verification::Valid)
                } else {
                    Ok(Verification::Invalid)
                }
            }
            Err(e) => reverts_mean_invalid(e),
        }
    } else {
        let call = ValidateSigOffchain::constructorCall {
            _signer: address,
            _hash: message,
            _signature: signature.into(),
        };
        let deploy_bytes: Vec<u8> = VALIDATE_SIG_OFFCHAIN_BYTECODE
            .iter()
            .copied()
            .chain(call.abi_encode())
            .collect();

        let tx = TransactionRequest::default().input(TransactionInput::new(deploy_bytes.into()));

        match provider.call(&tx).await {
            Ok(result) => match result.first() {
                Some(&SUCCESS_RESULT) => Ok(Verification::Valid),
                _ => Ok(Verification::Invalid),
            },
            Err(e) => reverts_mean_invalid(e),
        }
    }
}

/// Test-only helpers shared across this module's own tests and
/// `verification.rs` tests. Not part of the public API.
#[cfg(test)]
pub(crate) mod test_support {
    use super::SignatureRpcError;
    use alloy_json_rpc::{ErrorPayload, RequestPacket, Response, ResponsePacket, ResponsePayload};
    use alloy_primitives::Bytes;
    use alloy_provider::RootProvider;
    use alloy_rpc_client::RpcClient;
    use alloy_transport::{RpcError, TransportError};
    use serde_json::value::RawValue;
    use std::borrow::Cow;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use tower::Service;

    /// A canned reply for a single JSON-RPC call.
    #[derive(Clone, Debug)]
    pub enum MockReply {
        /// Return a JSON-RPC success whose `result` field is this hex string
        /// (e.g. `"0x"`, `"0x60806040"`, or a 32-byte word).
        Ok(&'static str),
        /// Return a JSON-RPC error whose `message` starts with this string.
        Err(&'static str),
    }

    /// In-process `tower::Service` transport. Handles exactly the JSON-RPC
    /// methods that `verify_signature` issues (`eth_getCode`, `eth_call`) with
    /// one canned response per method.
    #[derive(Clone)]
    pub struct MockTransport {
        get_code: Arc<Mutex<Option<MockReply>>>,
        call: Arc<Mutex<Option<MockReply>>>,
    }

    impl MockTransport {
        pub fn new(get_code: MockReply, call: MockReply) -> Self {
            Self {
                get_code: Arc::new(Mutex::new(Some(get_code))),
                call: Arc::new(Mutex::new(Some(call))),
            }
        }

        fn handle_one(&self, req: &alloy_json_rpc::SerializedRequest) -> Response {
            let id = req.id().clone();
            let slot = match req.method() {
                "eth_getCode" => &self.get_code,
                "eth_call" => &self.call,
                other => panic!("unexpected JSON-RPC method in mock: {other}"),
            };
            let reply = slot
                .lock()
                .unwrap()
                .take()
                .expect("mock transport called more times than configured");
            match reply {
                MockReply::Ok(hex) => {
                    let json = serde_json::to_string(hex).unwrap();
                    let raw = RawValue::from_string(json).unwrap();
                    Response {
                        id,
                        payload: ResponsePayload::Success(raw),
                    }
                }
                MockReply::Err(msg) => Response {
                    id,
                    payload: ResponsePayload::Failure(ErrorPayload {
                        code: 3,
                        message: Cow::Borrowed(msg),
                        data: None,
                    }),
                },
            }
        }
    }

    impl Service<RequestPacket> for MockTransport {
        type Response = ResponsePacket;
        type Error = TransportError;
        type Future = Pin<Box<dyn Future<Output = Result<ResponsePacket, TransportError>> + Send>>;

        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: RequestPacket) -> Self::Future {
            let this = self.clone();
            Box::pin(async move {
                match req {
                    RequestPacket::Single(req) => Ok(ResponsePacket::Single(this.handle_one(&req))),
                    RequestPacket::Batch(_) => {
                        unreachable!("verify_signature never issues batched requests")
                    }
                }
            })
        }
    }

    pub fn mock_provider(get_code: MockReply, call: MockReply) -> RootProvider<MockTransport> {
        let transport = MockTransport::new(get_code, call);
        let client = RpcClient::new(transport, true);
        RootProvider::<MockTransport>::new(client)
    }

    /// Construct a JSON-RPC error response with `code=3` for unit-testing
    /// `reverts_mean_invalid` without a provider.
    pub fn err_resp(message: &str) -> SignatureRpcError {
        RpcError::ErrorResp(ErrorPayload {
            code: 3,
            message: Cow::Owned(message.to_owned()),
            data: None,
        })
    }

    /// Signature bytes used by the integration tests; the mock transport never
    /// interprets them.
    pub fn dummy_sig() -> Bytes {
        Bytes::from(vec![0xccu8; 65])
    }

    /// 32-byte word whose first byte is `SUCCESS_RESULT` (0x01).
    pub const SUCCESS_WORD: &str =
        "0x0100000000000000000000000000000000000000000000000000000000000000";
    /// 32-byte word of zeros — `ValidateSigOffchain`'s "invalid" return.
    pub const FAILURE_WORD: &str =
        "0x0000000000000000000000000000000000000000000000000000000000000000";
    /// ERC-1271 magic value (0x1626ba7e) right-padded to 32 bytes.
    pub const MAGIC_WORD: &str =
        "0x1626ba7e00000000000000000000000000000000000000000000000000000000";
    /// Non-magic bytes4 right-padded to 32 bytes.
    pub const BAD_MAGIC_WORD: &str =
        "0xdeadbeef00000000000000000000000000000000000000000000000000000000";
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    /// Keep in sync with the SHA-256 pinned in the module-level doc comment and
    /// the `verify-contract-signature-bytecode` CI workflow. A mismatch here means
    /// `ValidateSigOffchain.bytecode` was changed (or regenerated from edited
    /// Solidity) without updating the rest of the invariants.
    const EXPECTED_BYTECODE_SHA256: &str =
        "7929acc3c7f14ff8fcfe6b7752a6b0327811da4697ce7b3bb438bf7b237ab410";

    /// Expected length of the committed `ValidateSigOffchain` bytecode. Pinned
    /// as a cheap redundant check on top of the SHA-256; if they disagree, the
    /// pinned hash is probably stale.
    const EXPECTED_BYTECODE_LEN: usize = 5537;

    #[test]
    fn bytecode_length_matches_pin() {
        assert_eq!(
            VALIDATE_SIG_OFFCHAIN_BYTECODE.len(),
            EXPECTED_BYTECODE_LEN,
            "vendored bytecode length drifted from pin"
        );
    }

    #[test]
    fn bytecode_starts_with_solidity_constructor_preamble() {
        // Every Solidity-compiled contract begins with the standard memory-pointer
        // initialization: PUSH1 0x80 PUSH1 0x40 MSTORE CALLVALUE DUP1 ISZERO.
        // Matching this guards against someone replacing the file with garbage.
        assert_eq!(
            &VALIDATE_SIG_OFFCHAIN_BYTECODE[..9],
            &[0x60, 0x80, 0x60, 0x40, 0x52, 0x34, 0x80, 0x15, 0x61],
        );
    }

    #[test]
    fn bytecode_matches_pinned_sha256() {
        let hash = hex::encode(Sha256::digest(VALIDATE_SIG_OFFCHAIN_BYTECODE));
        assert_eq!(hash, EXPECTED_BYTECODE_SHA256);
    }

    #[test]
    fn magic_value_matches_erc1271_selector() {
        // The ERC-1271 magic value is defined as the 4-byte selector of
        // `isValidSignature(bytes32,bytes)`. If the sol! binding and the pinned
        // constant ever disagree, one of them is wrong.
        assert_eq!(
            MAGIC_VALUE.to_be_bytes(),
            Erc1271::isValidSignatureCall::SELECTOR,
        );
    }

    #[test]
    fn magic_value_bytes_are_stable() {
        assert_eq!(MAGIC_VALUE.to_be_bytes(), [0x16, 0x26, 0xba, 0x7e]);
    }

    #[test]
    fn success_result_is_one() {
        assert_eq!(SUCCESS_RESULT, 0x01);
    }

    #[test]
    fn erc1271_call_encoding_starts_with_selector() {
        let call = Erc1271::isValidSignatureCall {
            _hash: FixedBytes::<32>::from([0x11u8; 32]),
            _signature: Bytes::from(vec![0xaa; 65]),
        };
        let encoded = call.abi_encode();
        assert_eq!(&encoded[..4], &MAGIC_VALUE.to_be_bytes());
        // selector (4) + bytes32 (32) + offset (32) + length (32) + padded 65-byte sig (96) = 196
        assert_eq!(encoded.len(), 196);
    }

    #[test]
    fn validate_sig_offchain_constructor_encoding_is_nonempty() {
        let call = ValidateSigOffchain::constructorCall {
            _signer: Address::from([0x22u8; 20]),
            _hash: FixedBytes::<32>::from([0x33u8; 32]),
            _signature: Bytes::from(vec![0xbb; 65]),
        };
        let encoded = call.abi_encode();
        // A constructor has no selector; encoding is just the three ABI-encoded
        // arguments. For (address, bytes32, bytes-with-65-byte-payload) this is:
        // 32 (address) + 32 (bytes32) + 32 (offset) + 32 (length) + 96 (padded sig) = 224
        assert_eq!(encoded.len(), 224);
    }

    #[test]
    fn deploy_payload_is_bytecode_then_constructor_args() {
        // Mirrors what `verify_signature` does for the no-code branch: concatenate
        // the helper bytecode with the ABI-encoded constructor args. Protects
        // against reordering regressions.
        let call = ValidateSigOffchain::constructorCall {
            _signer: Address::from([0x22u8; 20]),
            _hash: FixedBytes::<32>::from([0x33u8; 32]),
            _signature: Bytes::from(vec![0xbb; 65]),
        };
        let args = call.abi_encode();
        let deploy_bytes: Vec<u8> = VALIDATE_SIG_OFFCHAIN_BYTECODE
            .iter()
            .copied()
            .chain(args.iter().copied())
            .collect();
        assert_eq!(
            deploy_bytes.len(),
            VALIDATE_SIG_OFFCHAIN_BYTECODE.len() + args.len()
        );
        assert_eq!(&deploy_bytes[..9], &VALIDATE_SIG_OFFCHAIN_BYTECODE[..9]);
        assert_eq!(
            &deploy_bytes[VALIDATE_SIG_OFFCHAIN_BYTECODE.len()..],
            args.as_slice(),
        );
    }

    #[test]
    fn verification_is_valid_predicate() {
        assert!(Verification::Valid.is_valid());
        assert!(!Verification::Invalid.is_valid());
    }

    // ----------------------------------------------------------------------
    // `reverts_mean_invalid` unit tests — exercise the RPC-error mapping with
    // hand-constructed `ErrorPayload`s, no provider involved.
    // ----------------------------------------------------------------------

    use super::test_support::*;

    #[test]
    fn reverts_mean_invalid_maps_plain_revert() {
        let r = reverts_mean_invalid(err_resp("execution reverted"));
        assert_eq!(r.unwrap(), Verification::Invalid);
    }

    #[test]
    fn reverts_mean_invalid_matches_revert_with_details() {
        // Providers often append reason strings after "execution reverted".
        let r = reverts_mean_invalid(err_resp(
            "execution reverted: SignatureValidator#recoverSigner: invalid signature length",
        ));
        assert_eq!(r.unwrap(), Verification::Invalid);
    }

    #[test]
    fn reverts_mean_invalid_preserves_non_revert_errors() {
        // Non-revert errors should continue to propagate.
        let r = reverts_mean_invalid(err_resp("insufficient funds"));
        assert!(r.is_err());
    }

    // ----------------------------------------------------------------------
    // `verify_signature` integration tests via an in-process MockTransport
    // (defined in `test_support`). No network, no anvil. Each test exercises
    // exactly the two JSON-RPC calls `verify_signature` issues:
    // `eth_getCode` and `eth_call`.
    // ----------------------------------------------------------------------

    fn dummy_inputs() -> (Bytes, Address, FixedBytes<32>) {
        (
            dummy_sig(),
            Address::from([0u8; 20]),
            FixedBytes::<32>::from([0u8; 32]),
        )
    }

    #[tokio::test]
    async fn verify_signature_no_code_valid_is_valid() {
        let (sig, addr, hash) = dummy_inputs();
        let p = mock_provider(MockReply::Ok("0x"), MockReply::Ok(SUCCESS_WORD));
        let r = verify_signature(sig, addr, hash, p).await.unwrap();
        assert_eq!(r, Verification::Valid);
    }

    #[tokio::test]
    async fn verify_signature_no_code_failure_byte_is_invalid() {
        let (sig, addr, hash) = dummy_inputs();
        let p = mock_provider(MockReply::Ok("0x"), MockReply::Ok(FAILURE_WORD));
        let r = verify_signature(sig, addr, hash, p).await.unwrap();
        assert_eq!(r, Verification::Invalid);
    }

    #[tokio::test]
    async fn verify_signature_no_code_revert_is_invalid() {
        let (sig, addr, hash) = dummy_inputs();
        let p = mock_provider(MockReply::Ok("0x"), MockReply::Err("execution reverted"));
        let r = verify_signature(sig, addr, hash, p).await.unwrap();
        assert_eq!(r, Verification::Invalid);
    }

    #[tokio::test]
    async fn verify_signature_has_code_magic_value_is_valid() {
        let (sig, addr, hash) = dummy_inputs();
        let p = mock_provider(MockReply::Ok("0x60806040"), MockReply::Ok(MAGIC_WORD));
        let r = verify_signature(sig, addr, hash, p).await.unwrap();
        assert_eq!(r, Verification::Valid);
    }

    #[tokio::test]
    async fn verify_signature_has_code_wrong_magic_is_invalid() {
        let (sig, addr, hash) = dummy_inputs();
        let p = mock_provider(MockReply::Ok("0x60806040"), MockReply::Ok(BAD_MAGIC_WORD));
        let r = verify_signature(sig, addr, hash, p).await.unwrap();
        assert_eq!(r, Verification::Invalid);
    }

    #[tokio::test]
    async fn verify_signature_has_code_revert_is_invalid() {
        let (sig, addr, hash) = dummy_inputs();
        let p = mock_provider(
            MockReply::Ok("0x60806040"),
            MockReply::Err("execution reverted"),
        );
        let r = verify_signature(sig, addr, hash, p).await.unwrap();
        assert_eq!(r, Verification::Invalid);
    }

    #[tokio::test]
    async fn verify_signature_non_revert_error_propagates() {
        let (sig, addr, hash) = dummy_inputs();
        let p = mock_provider(MockReply::Ok("0x"), MockReply::Err("internal server error"));
        let r = verify_signature(sig, addr, hash, p).await;
        assert!(r.is_err(), "non-revert RPC errors must not be swallowed");
    }
}
