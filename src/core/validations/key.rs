use alloy_dyn_abi::TypedData;
use alloy_primitives::{Address, PrimitiveSignature};
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
}
