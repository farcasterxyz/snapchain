//! EIP-712 typed-data helpers for KEY_ADD / KEY_REMOVE custody signatures and the embedded
//! SignedKeyRequest metadata. Lifted from `snapchain::core::validations::key` so the CLI can
//! avoid depending on the snapchain library crate.

use alloy_dyn_abi::TypedData;
use serde_json::{json, Value};

pub const KEY_DOMAIN_NAME: &str = "Farcaster KeyAdd";
pub const KEY_DOMAIN_VERSION: &str = "1";

pub const ETH_MAINNET_CHAIN_ID: u32 = 1;

pub const METADATA_TYPE_SIGNED_KEY_REQUEST: u32 = 1;

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

pub fn key_add_typed_data(
    payload: &KeyAddPayload,
    chain_id: u32,
) -> Result<TypedData, serde_json::Error> {
    // scopes is `int32` on the wire but `uint32[]` in the signed payload. We don't accept
    // negative scope discriminants — proto only emits non-negative values for `MessageType`.
    let scopes: Vec<u32> = payload
        .scopes
        .iter()
        .map(|s| u32::try_from(*s).expect("MessageType discriminants are non-negative"))
        .collect();

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
    serde_json::from_value::<TypedData>(json)
}

pub fn key_remove_typed_data(
    payload: &KeyRemovePayload,
    chain_id: u32,
) -> Result<TypedData, serde_json::Error> {
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
    serde_json::from_value::<TypedData>(json)
}

pub fn signed_key_request_typed_data(
    request_fid: u64,
    key: &[u8],
    deadline: u64,
    chain_id: u32,
) -> Result<TypedData, serde_json::Error> {
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
    serde_json::from_value::<TypedData>(json)
}
