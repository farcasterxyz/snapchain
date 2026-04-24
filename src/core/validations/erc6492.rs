//! On-chain signature verification for EOA, ERC-1271, and ERC-6492 signatures.
//!
//! The Rust side is inlined from eth-signature-verifier (MIT, © 2024 Royal Markets,
//! Inc.), which in turn extracted it from WalletConnect/erc6492 (MIT, © 2024
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

        let result = provider.call(&call_request).await?;
        let magic = result.get(..4);
        if magic == Some(&MAGIC_VALUE.to_be_bytes()[..]) {
            Ok(Verification::Valid)
        } else {
            Ok(Verification::Invalid)
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
            Err(e) => {
                if let Some(error_response) = e.as_error_resp() {
                    if error_response.message.starts_with("execution reverted") {
                        return Ok(Verification::Invalid);
                    }
                }
                Err(e)
            }
        }
    }
}
