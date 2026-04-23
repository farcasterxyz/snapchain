use alloy_dyn_abi::TypedData;
use alloy_primitives::{Address, PrimitiveSignature};
use alloy_sol_types::{sol, SolType};
use serde_json::{json, Value};

use crate::core::validations::error::ValidationError;

// EIP-712 domain shared by KEY_ADD and KEY_REMOVE custody signatures. The FIP specifies a single
// domain "Farcaster KeyAdd" for both primary types; the primary type itself disambiguates.
pub const KEY_DOMAIN_NAME: &str = "Farcaster KeyAdd";
pub const KEY_DOMAIN_VERSION: &str = "1";

// ETH mainnet. We are not verifying any onchain state, we just picked eth.
pub const ETH_MAINNET_CHAIN_ID: u32 = 1;

#[derive(Clone, Copy)]
pub struct KeyAddPayload<'a> {
    pub fid: u64,
    pub key: &'a [u8],
    pub key_type: u32,
    pub scopes: &'a [i32],
    pub ttl: u32,
    pub nonce: u32,
    pub deadline: u32,
}

#[derive(Clone, Copy)]
pub struct KeyRemovePayload<'a> {
    pub fid: u64,
    pub key: &'a [u8],
    pub nonce: u32,
    pub deadline: u32,
}

fn key_eip712_domain(chain_id: u32) -> Value {
    json!({
        "name": KEY_DOMAIN_NAME,
        "version": KEY_DOMAIN_VERSION,
        "chainId": chain_id,
    })
}

fn eip712_domain_type() -> Value {
    json!([
        { "name": "name", "type": "string" },
        { "name": "version", "type": "string" },
        { "name": "chainId", "type": "uint256" },
    ])
}

fn key_add_types() -> Value {
    json!({
        "EIP712Domain": eip712_domain_type(),
        "KeyAdd": [
            { "name": "fid", "type": "uint256" },
            { "name": "key", "type": "bytes" },
            { "name": "keyType", "type": "uint32" },
            { "name": "scopes", "type": "uint32[]" },
            { "name": "ttl", "type": "uint32" },
            { "name": "nonce", "type": "uint32" },
            { "name": "deadline", "type": "uint256" },
        ],
    })
}

fn key_remove_types() -> Value {
    json!({
        "EIP712Domain": eip712_domain_type(),
        "KeyRemove": [
            { "name": "fid", "type": "uint256" },
            { "name": "key", "type": "bytes" },
            { "name": "nonce", "type": "uint32" },
            { "name": "deadline", "type": "uint256" },
        ],
    })
}

pub fn key_add_typed_data(
    payload: &KeyAddPayload,
    chain_id: u32,
) -> Result<TypedData, ValidationError> {
    // scopes is `int32` on the wire (proto limitation) but `uint32[]` in the signed payload; any
    // negative value cannot be represented and makes the signature unrecoverable by design.
    let scopes: Vec<u32> = payload
        .scopes
        .iter()
        .map(|s| u32::try_from(*s).map_err(|_| ValidationError::InvalidData))
        .collect::<Result<_, _>>()?;

    let json = json!({
        "types": key_add_types(),
        "primaryType": "KeyAdd",
        "domain": key_eip712_domain(chain_id),
        "message": {
            "fid": payload.fid,
            "key": format!("0x{}", hex::encode(payload.key)),
            "keyType": payload.key_type,
            "scopes": scopes,
            "ttl": payload.ttl,
            "nonce": payload.nonce,
            "deadline": payload.deadline,
        },
    });
    serde_json::from_value::<TypedData>(json).map_err(|_| ValidationError::InvalidData)
}

pub fn key_remove_typed_data(
    payload: &KeyRemovePayload,
    chain_id: u32,
) -> Result<TypedData, ValidationError> {
    let json = json!({
        "types": key_remove_types(),
        "primaryType": "KeyRemove",
        "domain": key_eip712_domain(chain_id),
        "message": {
            "fid": payload.fid,
            "key": format!("0x{}", hex::encode(payload.key)),
            "nonce": payload.nonce,
            "deadline": payload.deadline,
        },
    });
    serde_json::from_value::<TypedData>(json).map_err(|_| ValidationError::InvalidData)
}

fn recover_from_typed_data(
    typed_data: &TypedData,
    signature: &[u8],
) -> Result<Address, ValidationError> {
    if signature.len() != 65 {
        return Err(ValidationError::InvalidSignature);
    }
    let prehash = typed_data
        .eip712_signing_hash()
        .map_err(|_| ValidationError::InvalidHash)?;
    let sig = PrimitiveSignature::from_bytes_and_parity(
        &signature[0..64],
        signature[64] != 0x1b && signature[64] != 0x00,
    );
    sig.recover_address_from_prehash(&prehash)
        .map_err(|_| ValidationError::InvalidSignature)
}

/// Recovers the Ethereum address that signed the EIP-712 `KeyAdd` custody authorization.
pub fn recover_key_add_custody_address(
    payload: &KeyAddPayload,
    signature: &[u8],
    chain_id: u32,
) -> Result<Address, ValidationError> {
    let typed_data = key_add_typed_data(payload, chain_id)?;
    recover_from_typed_data(&typed_data, signature)
}

/// Recovers the Ethereum address that signed the EIP-712 `KeyRemove` custody authorization.
/// Only applies to `signature_type = 1` (custody); self-revocation uses an Ed25519 signature
/// validated separately.
pub fn recover_key_remove_custody_address(
    payload: &KeyRemovePayload,
    signature: &[u8],
    chain_id: u32,
) -> Result<Address, ValidationError> {
    let typed_data = key_remove_typed_data(payload, chain_id)?;
    recover_from_typed_data(&typed_data, signature)
}

// -- SignedKeyRequest metadata validation --------------------------------------------------------
//
// KEY_ADD is entirely off-chain — there is no validator contract in the loop. The metadata struct
// keeps the `SignedKeyRequest*` naming for continuity with Farcaster tooling, but the EIP-712
// domain is Snapchain's own; chainId is purely cosmetic (wallets display it), and there is no
// `verifyingContract` because nothing is actually verifying on-chain. The signature binds the
// app's `requestFid` to the key — without this, an attacker could spoof any `requestFid` and
// consume another app's nonce counter.

sol! {
    // Same field layout as the on-chain `SignedKeyRequestValidator.SignedKeyRequestMetadata`
    // struct, kept for ecosystem/tooling continuity. Also declared in
    // `src/connectors/onchain_events/mod.rs` for decoding on-chain signer events; duplicated here
    // to keep the module graph one-way (`connectors` → `core::validations`, never the reverse).
    // TODO: extract a shared module (e.g. `core::chain_types` or a crate-level `protocol_types`)
    // that both `connectors` and `core::validations` can depend on, and move this `sol!` block
    // there so the definition lives in exactly one place.
    struct SignedKeyRequestMetadata {
        uint256 requestFid;
        address requestSigner;
        bytes signature;
        uint256 deadline;
    }
}

// `SignedKeyRequest` rides on the same EIP-712 domain (`KEY_DOMAIN_NAME` / `KEY_DOMAIN_VERSION`)
// as `KeyAdd` and `KeyRemove` — it's a separate primary type, but an off-chain-only wallet-UX
// concern, so there's no reason to invent a parallel domain. Only the metadata-type discriminator
// is distinct, since that lives on the proto, not in the EIP-712 payload.
pub const METADATA_TYPE_SIGNED_KEY_REQUEST: u32 = 1;

#[derive(Debug, Clone)]
pub struct VerifiedSignedKeyRequest {
    pub request_fid: u64,
    pub request_signer: Address,
    pub deadline: u64,
}

fn signed_key_request_types() -> Value {
    json!({
        "EIP712Domain": eip712_domain_type(),
        "SignedKeyRequest": [
            { "name": "requestFid", "type": "uint256" },
            { "name": "key", "type": "bytes" },
            { "name": "deadline", "type": "uint256" },
        ],
    })
}

fn signed_key_request_typed_data(
    request_fid: u64,
    key: &[u8],
    deadline: u64,
    chain_id: u32,
) -> Result<TypedData, ValidationError> {
    let json = json!({
        "types": signed_key_request_types(),
        "primaryType": "SignedKeyRequest",
        "domain": key_eip712_domain(chain_id),
        "message": {
            "requestFid": request_fid,
            "key": format!("0x{}", hex::encode(key)),
            "deadline": deadline,
        },
    });
    serde_json::from_value::<TypedData>(json).map_err(|_| ValidationError::InvalidSignedKeyRequest)
}

/// Verifies the `SignedKeyRequestMetadata` embedded in a `KeyAddBody`:
///
/// 1. `metadata_type` is 1 (SignedKeyRequest) and `metadata` is non-empty
/// 2. The ABI-encoded metadata decodes cleanly
/// 3. `deadline >= current_timestamp` (farcaster epoch seconds)
/// 4. The EIP-712 signature over `(requestFid, key, deadline)` recovers `requestSigner`
///
/// Returns the verified `requestFid` (used downstream as the `appFid` for self-revocation nonce
/// scoping) and the recovered `requestSigner`. The caller is responsible for the final check —
/// that `requestSigner` equals the custody address of `requestFid` from the `IdRegisterEvent`
/// lookup — because that lookup is storage-dependent.
pub fn verify_signed_key_request_metadata(
    metadata_type: u32,
    metadata: &[u8],
    key: &[u8],
    current_timestamp: u64,
    chain_id: u32,
) -> Result<VerifiedSignedKeyRequest, ValidationError> {
    if metadata_type != METADATA_TYPE_SIGNED_KEY_REQUEST {
        return Err(ValidationError::InvalidMetadataType);
    }
    if metadata.is_empty() {
        return Err(ValidationError::InvalidSignedKeyRequest);
    }

    let decoded = SignedKeyRequestMetadata::abi_decode(metadata, true)
        .map_err(|_| ValidationError::InvalidSignedKeyRequest)?;

    let deadline =
        u64::try_from(decoded.deadline).map_err(|_| ValidationError::InvalidSignedKeyRequest)?;
    if deadline < current_timestamp {
        return Err(ValidationError::SignedKeyRequestExpired);
    }

    let request_fid =
        u64::try_from(decoded.requestFid).map_err(|_| ValidationError::InvalidSignedKeyRequest)?;

    let typed_data = signed_key_request_typed_data(request_fid, key, deadline, chain_id)?;

    // Map any recovery failure (bad length, bad parity, unrecoverable sig) to the same
    // request-metadata error so callers don't need to distinguish — they're all "this metadata
    // isn't valid" from the caller's perspective.
    let recovered = recover_from_typed_data(&typed_data, decoded.signature.as_ref())
        .map_err(|_| ValidationError::InvalidSignedKeyRequest)?;

    if recovered != decoded.requestSigner {
        return Err(ValidationError::InvalidSignedKeyRequest);
    }

    Ok(VerifiedSignedKeyRequest {
        request_fid,
        request_signer: decoded.requestSigner,
        deadline,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;

    fn sign(signer: &PrivateKeySigner, typed_data: &TypedData) -> Vec<u8> {
        let prehash = typed_data.eip712_signing_hash().unwrap();
        let sig = signer.sign_hash_sync(&prehash).unwrap();
        sig.into()
    }

    fn sample_key_add_payload<'a>(key: &'a [u8; 32], scopes: &'a [i32]) -> KeyAddPayload<'a> {
        KeyAddPayload {
            fid: 1234,
            key,
            key_type: 1,
            scopes,
            ttl: 86_400,
            nonce: 7,
            deadline: 1_700_000_000,
        }
    }

    #[test]
    fn key_add_roundtrip_recovers_signer_address() {
        let signer = PrivateKeySigner::random();
        let key = [0x42u8; 32];
        let scopes = [1, 2, 3, 11, 13];
        let payload = sample_key_add_payload(&key, &scopes);

        let typed_data = key_add_typed_data(&payload, ETH_MAINNET_CHAIN_ID).unwrap();
        let sig_bytes = sign(&signer, &typed_data);

        let recovered =
            recover_key_add_custody_address(&payload, &sig_bytes, ETH_MAINNET_CHAIN_ID).unwrap();
        assert_eq!(recovered, signer.address());
    }

    #[test]
    fn key_remove_roundtrip_recovers_signer_address() {
        let signer = PrivateKeySigner::random();
        let key = [0xAAu8; 32];
        let payload = KeyRemovePayload {
            fid: 1234,
            key: &key,
            nonce: 8,
            deadline: 1_700_000_000,
        };

        let typed_data = key_remove_typed_data(&payload, ETH_MAINNET_CHAIN_ID).unwrap();
        let sig_bytes = sign(&signer, &typed_data);

        let recovered =
            recover_key_remove_custody_address(&payload, &sig_bytes, ETH_MAINNET_CHAIN_ID).unwrap();
        assert_eq!(recovered, signer.address());
    }

    // A signature over one payload must not recover the same address for a mutated payload —
    // otherwise the custody authorization doesn't actually bind to the scopes/ttl/nonce fields.
    #[test]
    fn key_add_tampered_field_breaks_recovery() {
        let signer = PrivateKeySigner::random();
        let key = [0x42u8; 32];
        let scopes = [1, 2];
        let payload = sample_key_add_payload(&key, &scopes);
        let typed_data = key_add_typed_data(&payload, ETH_MAINNET_CHAIN_ID).unwrap();
        let sig_bytes = sign(&signer, &typed_data);

        let mutated_scopes = [1, 2, 3];
        let tampered = KeyAddPayload {
            scopes: &mutated_scopes,
            ..payload
        };
        let recovered =
            recover_key_add_custody_address(&tampered, &sig_bytes, ETH_MAINNET_CHAIN_ID).unwrap();
        assert_ne!(recovered, signer.address());

        let tampered = KeyAddPayload { ttl: 1, ..payload };
        let recovered =
            recover_key_add_custody_address(&tampered, &sig_bytes, ETH_MAINNET_CHAIN_ID).unwrap();
        assert_ne!(recovered, signer.address());

        let tampered = KeyAddPayload {
            nonce: payload.nonce + 1,
            ..payload
        };
        let recovered =
            recover_key_add_custody_address(&tampered, &sig_bytes, ETH_MAINNET_CHAIN_ID).unwrap();
        assert_ne!(recovered, signer.address());
    }

    #[test]
    fn key_add_chain_id_binding() {
        let signer = PrivateKeySigner::random();
        let key = [0x42u8; 32];
        let scopes = [1];
        let payload = sample_key_add_payload(&key, &scopes);

        let typed_data = key_add_typed_data(&payload, ETH_MAINNET_CHAIN_ID).unwrap();
        let sig_bytes = sign(&signer, &typed_data);

        // Signature produced for OP mainnet (10) must not recover on OP Sepolia (11155420).
        let recovered = recover_key_add_custody_address(&payload, &sig_bytes, 11_155_420).unwrap();
        assert_ne!(recovered, signer.address());
    }

    #[test]
    fn invalid_signature_length_returns_error() {
        let key = [0x42u8; 32];
        let scopes = [1];
        let payload = sample_key_add_payload(&key, &scopes);
        let err = recover_key_add_custody_address(&payload, &[0u8; 64], ETH_MAINNET_CHAIN_ID)
            .unwrap_err();
        assert_eq!(err, ValidationError::InvalidSignature);
    }

    #[test]
    fn negative_scope_value_rejected() {
        let key = [0x42u8; 32];
        let scopes = [-1i32];
        let payload = sample_key_add_payload(&key, &scopes);
        let err = key_add_typed_data(&payload, ETH_MAINNET_CHAIN_ID).unwrap_err();
        assert_eq!(err, ValidationError::InvalidData);
    }

    // -- SignedKeyRequest metadata validation tests ----------------------------------------------

    use alloy_primitives::{Bytes, U256};
    use alloy_sol_types::SolValue;

    const TEST_DEADLINE: u64 = 1_700_000_000;
    const TEST_CURRENT_TS: u64 = 1_600_000_000;

    // Builds a fully-signed `SignedKeyRequestMetadata` ABI blob. The returned signer is what the
    // validator should recover — tests use it to assert recovery, tamper-detection, etc.
    fn build_signed_metadata(
        signer: &PrivateKeySigner,
        request_fid: u64,
        key: &[u8],
        deadline: u64,
    ) -> Bytes {
        let typed_data =
            signed_key_request_typed_data(request_fid, key, deadline, ETH_MAINNET_CHAIN_ID)
                .unwrap();
        let sig_bytes: Vec<u8> = sign(signer, &typed_data);

        let metadata = SignedKeyRequestMetadata {
            requestFid: U256::from(request_fid),
            requestSigner: signer.address(),
            signature: Bytes::from(sig_bytes),
            deadline: U256::from(deadline),
        };
        Bytes::from(metadata.abi_encode())
    }

    #[test]
    fn signed_key_request_verifies_with_matching_signer() {
        let signer = PrivateKeySigner::random();
        let key = [0x11u8; 32];
        let metadata = build_signed_metadata(&signer, 5678, &key, TEST_DEADLINE);

        let verified = verify_signed_key_request_metadata(
            METADATA_TYPE_SIGNED_KEY_REQUEST,
            metadata.as_ref(),
            &key,
            TEST_CURRENT_TS,
            ETH_MAINNET_CHAIN_ID,
        )
        .unwrap();

        assert_eq!(verified.request_fid, 5678);
        assert_eq!(verified.request_signer, signer.address());
        assert_eq!(verified.deadline, TEST_DEADLINE);
    }

    #[test]
    fn signed_key_request_rejects_wrong_metadata_type() {
        let signer = PrivateKeySigner::random();
        let key = [0x11u8; 32];
        let metadata = build_signed_metadata(&signer, 5678, &key, TEST_DEADLINE);

        let err = verify_signed_key_request_metadata(
            2, // anything other than 1 is unsupported
            metadata.as_ref(),
            &key,
            TEST_CURRENT_TS,
            ETH_MAINNET_CHAIN_ID,
        )
        .unwrap_err();
        assert_eq!(err, ValidationError::InvalidMetadataType);
    }

    #[test]
    fn signed_key_request_rejects_empty_metadata() {
        let err = verify_signed_key_request_metadata(
            METADATA_TYPE_SIGNED_KEY_REQUEST,
            &[],
            &[0u8; 32],
            TEST_CURRENT_TS,
            ETH_MAINNET_CHAIN_ID,
        )
        .unwrap_err();
        assert_eq!(err, ValidationError::InvalidSignedKeyRequest);
    }

    #[test]
    fn signed_key_request_rejects_malformed_abi_bytes() {
        let err = verify_signed_key_request_metadata(
            METADATA_TYPE_SIGNED_KEY_REQUEST,
            &[0xDEu8; 16], // not a valid ABI-encoded SignedKeyRequestMetadata
            &[0u8; 32],
            TEST_CURRENT_TS,
            ETH_MAINNET_CHAIN_ID,
        )
        .unwrap_err();
        assert_eq!(err, ValidationError::InvalidSignedKeyRequest);
    }

    #[test]
    fn signed_key_request_rejects_expired_deadline() {
        let signer = PrivateKeySigner::random();
        let key = [0x11u8; 32];
        let deadline = 1_500_000_000;
        let metadata = build_signed_metadata(&signer, 5678, &key, deadline);

        let err = verify_signed_key_request_metadata(
            METADATA_TYPE_SIGNED_KEY_REQUEST,
            metadata.as_ref(),
            &key,
            TEST_CURRENT_TS, // > deadline
            ETH_MAINNET_CHAIN_ID,
        )
        .unwrap_err();
        assert_eq!(err, ValidationError::SignedKeyRequestExpired);
    }

    // If the signed key differs from the key embedded in the outer `KeyAddBody`, the recovered
    // address will not match `requestSigner` — this is exactly the spoof attack the metadata
    // validation is supposed to block.
    #[test]
    fn signed_key_request_rejects_mismatched_outer_key() {
        let signer = PrivateKeySigner::random();
        let signed_key = [0x11u8; 32];
        let metadata = build_signed_metadata(&signer, 5678, &signed_key, TEST_DEADLINE);

        let outer_key = [0x22u8; 32];
        let err = verify_signed_key_request_metadata(
            METADATA_TYPE_SIGNED_KEY_REQUEST,
            metadata.as_ref(),
            &outer_key,
            TEST_CURRENT_TS,
            ETH_MAINNET_CHAIN_ID,
        )
        .unwrap_err();
        assert_eq!(err, ValidationError::InvalidSignedKeyRequest);
    }

    // The signed payload binds to `requestFid`, so tampering with the outer ABI struct's
    // `requestFid` after the fact breaks recovery.
    #[test]
    fn signed_key_request_rejects_spoofed_request_fid() {
        let signer = PrivateKeySigner::random();
        let key = [0x11u8; 32];
        let typed_data =
            signed_key_request_typed_data(5678, &key, TEST_DEADLINE, ETH_MAINNET_CHAIN_ID).unwrap();
        let sig_bytes: Vec<u8> = sign(&signer, &typed_data);

        let tampered = SignedKeyRequestMetadata {
            requestFid: U256::from(9999u64), // tampered
            requestSigner: signer.address(),
            signature: Bytes::from(sig_bytes),
            deadline: U256::from(TEST_DEADLINE),
        };
        let encoded = Bytes::from(tampered.abi_encode());

        let err = verify_signed_key_request_metadata(
            METADATA_TYPE_SIGNED_KEY_REQUEST,
            encoded.as_ref(),
            &key,
            TEST_CURRENT_TS,
            ETH_MAINNET_CHAIN_ID,
        )
        .unwrap_err();
        assert_eq!(err, ValidationError::InvalidSignedKeyRequest);
    }

    #[test]
    fn signed_key_request_rejects_request_signer_mismatch() {
        let signer = PrivateKeySigner::random();
        let other = PrivateKeySigner::random();
        let key = [0x11u8; 32];

        // Sign correctly, but put someone else's address in `requestSigner`.
        let typed_data =
            signed_key_request_typed_data(5678, &key, TEST_DEADLINE, ETH_MAINNET_CHAIN_ID).unwrap();
        let sig_bytes: Vec<u8> = sign(&signer, &typed_data);

        let tampered = SignedKeyRequestMetadata {
            requestFid: U256::from(5678u64),
            requestSigner: other.address(), // recovered will be `signer`, not `other`
            signature: Bytes::from(sig_bytes),
            deadline: U256::from(TEST_DEADLINE),
        };
        let encoded = Bytes::from(tampered.abi_encode());

        let err = verify_signed_key_request_metadata(
            METADATA_TYPE_SIGNED_KEY_REQUEST,
            encoded.as_ref(),
            &key,
            TEST_CURRENT_TS,
            ETH_MAINNET_CHAIN_ID,
        )
        .unwrap_err();
        assert_eq!(err, ValidationError::InvalidSignedKeyRequest);
    }
}
