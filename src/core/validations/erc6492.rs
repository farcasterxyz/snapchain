//! On-chain signature verification for EOA, ERC-1271, and ERC-6492 signatures.
//!
//! The Rust side is inlined from `royal-markets/eth-signature-verifier` (MIT,
//! © 2024 Royal Markets, Inc.), which in turn extracted and lightly modified it
//! from Reown's (formerly WalletConnect's) `reown-com/erc6492` (MIT, © 2024
//! WalletConnect, Inc.).
//!
//! The Solidity helper (`ValidateSigOffchain` / `UniversalSigValidator`) and its
//! compiled bytecode live in [`erc6492/`](./erc6492/); see
//! [`erc6492/README.md`](./erc6492/README.md) for provenance and the
//! bit-exact build recipe.
//!
//! The committed `erc6492/ValidateSigOffchain.bytecode` must match SHA-256
//! `7929acc3c7f14ff8fcfe6b7752a6b0327811da4697ce7b3bb438bf7b237ab410`. This
//! invariant is enforced on every PR that touches `erc6492/` by the
//! `verify-erc6492-bytecode` CI job.

use alloy_primitives::{Address, Bytes, FixedBytes};
use alloy_provider::Provider;
use alloy_rpc_types::{TransactionInput, TransactionRequest};
use alloy_sol_types::{sol, SolCall, SolConstructor};
use alloy_transport::{RpcError, Transport, TransportErrorKind};

/// Counterfactual-deployment helper from AmbireTech's ERC-6492 reference
/// implementation. See [`erc6492/README.md`](./erc6492/README.md).
const VALIDATE_SIG_OFFCHAIN_BYTECODE: &[u8] =
    include_bytes!("erc6492/ValidateSigOffchain.bytecode");

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

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    /// Keep in sync with the SHA-256 pinned in the module-level doc comment and
    /// the `verify-erc6492-bytecode` CI workflow. A mismatch here means
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
}
