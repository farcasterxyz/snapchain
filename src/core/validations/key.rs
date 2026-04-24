use alloy_dyn_abi::TypedData;
use alloy_primitives::{Address, PrimitiveSignature};
use alloy_sol_types::{sol, SolType};
use serde_json::{json, Value};

use crate::core::validations::error::ValidationError;
use crate::proto::{KeyAddBody, KeyRemoveBody, MessageType};

// Ed25519 public keys are 32 bytes on the wire.
pub const ED25519_PUBLIC_KEY_LEN: usize = 32;

// Only key type currently defined by the FIP.
pub const KEY_TYPE_ED25519: u32 = 1;

/// Authorization mode for KEY_REMOVE. Wire representation is a `uint32` (the proto stays
/// `uint32 signature_type = 3` for ecosystem-tooling compatibility), but every call site inside
/// Rust should go through this enum: `TryFrom<u32>` is the boundary, and downstream match
/// statements get exhaustiveness-checked by the compiler.
///
/// The explicit `#[repr(u32)]` + discriminant values pin the wire encoding, so a future
/// round-trip like `KeyRemoveSignatureType::Custody as u32 == 1` is compile-time guaranteed —
/// renaming a variant cannot drift the on-wire value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum KeyRemoveSignatureType {
    /// EIP-712 custody signature; recovered signer must equal the FID's custody address.
    /// Consumes the user-nonce namespace.
    Custody = 1,
    /// Standard Ed25519 signature from the key being removed (the Message envelope signature is
    /// sufficient — the signer IS the key). Consumes the app-nonce namespace scoped by the
    /// verified `requestFid` cached at KEY_ADD time. Enables mass revocation by a compromised app.
    SelfRevoke = 2,
}

impl TryFrom<u32> for KeyRemoveSignatureType {
    type Error = ValidationError;
    fn try_from(v: u32) -> Result<Self, Self::Error> {
        match v {
            1 => Ok(Self::Custody),
            2 => Ok(Self::SelfRevoke),
            _ => Err(ValidationError::InvalidSignatureType),
        }
    }
}

// Maximum sliding TTL window accepted in a KeyAddBody. FIP specifies 90 days; larger values are
// rejected at static validation so a misbehaving signer can't request an effectively-permanent key.
pub const MAX_KEY_TTL_SECONDS: u32 = 90 * 24 * 60 * 60;

// Cap on active *gasless* keys per FID. On-chain signers are not counted here — they have their
// own cap at the L2 KeyRegistry contract and are outside this limit. Enforced at KEY_ADD merge
// time by `merge_key_add`: a gasless KEY_ADD that would push the per-FID count to or past this
// value is rejected with `ActiveKeyCapExceeded`.
pub const MAX_GASLESS_KEYS_PER_FID: u32 = 1000;

// Upper bound on the number of entries in `KeyAddBody.scopes`. Scopes are semantically a set of
// `MessageType` discriminants, so by pigeonhole any list longer than the MessageType enum's
// variant count is guaranteed to contain duplicates. Keep this in sync with the enum in
// `proto/definitions/message.proto` — bumping a new MessageType adds one slot.
pub const MAX_KEY_ADD_SCOPES: usize = MessageType::VARIANT_COUNT;

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

// -- Static body validation ----------------------------------------------------------------------
//
// Field-level checks that do not depend on engine state. Signature recovery, custody comparison,
// nonce ordering, and key-already-exists checks live in the engine layer — not here.

/// Fail-closed check that every scope is a recognized `MessageType` enum value. Returns
/// `EmptyScopes` when the list is empty and `InvalidScope(raw)` for any bad value, including the
/// custody-level operations (`KEY_ADD` / `KEY_REMOVE`) which a signer must never be able to
/// authorize — allowing those would let a signer mint or revoke other signers.
fn validate_key_add_scopes(scopes: &[i32]) -> Result<(), ValidationError> {
    if scopes.is_empty() {
        return Err(ValidationError::EmptyScopes);
    }
    if scopes.len() > MAX_KEY_ADD_SCOPES {
        return Err(ValidationError::TooManyScopes(MAX_KEY_ADD_SCOPES));
    }
    for scope in scopes {
        match MessageType::try_from(*scope) {
            Ok(MessageType::None) | Ok(MessageType::KeyAdd) | Ok(MessageType::KeyRemove) => {
                return Err(ValidationError::InvalidScope(*scope));
            }
            Ok(_) => (),
            Err(_) => return Err(ValidationError::InvalidScope(*scope)),
        }
    }
    Ok(())
}

pub fn validate_key_add_body(body: &KeyAddBody) -> Result<(), ValidationError> {
    if body.key.len() != ED25519_PUBLIC_KEY_LEN {
        return Err(ValidationError::InvalidKeyLength);
    }
    if body.key_type != KEY_TYPE_ED25519 {
        return Err(ValidationError::InvalidKeyType);
    }
    if body.deadline == 0 {
        return Err(ValidationError::MissingDeadline);
    }
    if body.ttl == 0 || body.ttl > MAX_KEY_TTL_SECONDS {
        return Err(ValidationError::InvalidTtl(MAX_KEY_TTL_SECONDS));
    }
    if body.metadata_type != METADATA_TYPE_SIGNED_KEY_REQUEST {
        return Err(ValidationError::InvalidMetadataType);
    }
    if body.metadata.is_empty() {
        return Err(ValidationError::MissingMetadata);
    }
    validate_key_add_scopes(&body.scopes)?;
    Ok(())
}

pub fn validate_key_remove_body(body: &KeyRemoveBody) -> Result<(), ValidationError> {
    if body.key.len() != ED25519_PUBLIC_KEY_LEN {
        return Err(ValidationError::InvalidKeyLength);
    }
    // Reject unknown discriminants here; downstream merge code can then match exhaustively on the
    // typed enum and the compiler enforces coverage of all variants.
    KeyRemoveSignatureType::try_from(body.signature_type)?;
    if body.deadline == 0 {
        return Err(ValidationError::MissingDeadline);
    }
    Ok(())
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

    // -- Static body validation tests -----------------------------------------------------------

    fn sample_key_add_body() -> KeyAddBody {
        KeyAddBody {
            key: vec![0x42u8; 32],
            key_type: KEY_TYPE_ED25519,
            custody_signature: vec![0u8; 65],
            deadline: 1_700_000_000,
            nonce: 1,
            metadata: vec![0xDE, 0xAD, 0xBE, 0xEF],
            metadata_type: METADATA_TYPE_SIGNED_KEY_REQUEST,
            registration_tx_hash: vec![],
            scopes: vec![MessageType::CastAdd as i32],
            ttl: 86_400,
        }
    }

    fn sample_key_remove_body() -> KeyRemoveBody {
        KeyRemoveBody {
            key: vec![0xAAu8; 32],
            signature: vec![0u8; 65],
            signature_type: KeyRemoveSignatureType::Custody as u32,
            deadline: 1_700_000_000,
            nonce: 2,
        }
    }

    #[test]
    fn key_add_body_accepts_well_formed_input() {
        let body = sample_key_add_body();
        assert_eq!(validate_key_add_body(&body), Ok(()));
    }

    #[test]
    fn key_add_body_rejects_wrong_key_length() {
        let mut body = sample_key_add_body();
        body.key = vec![0x42u8; 31];
        assert_eq!(
            validate_key_add_body(&body),
            Err(ValidationError::InvalidKeyLength)
        );
    }

    #[test]
    fn key_add_body_rejects_unknown_key_type() {
        let mut body = sample_key_add_body();
        body.key_type = 2;
        assert_eq!(
            validate_key_add_body(&body),
            Err(ValidationError::InvalidKeyType)
        );
    }

    #[test]
    fn key_add_body_rejects_missing_deadline() {
        let mut body = sample_key_add_body();
        body.deadline = 0;
        assert_eq!(
            validate_key_add_body(&body),
            Err(ValidationError::MissingDeadline)
        );
    }

    #[test]
    fn key_add_body_rejects_zero_ttl() {
        let mut body = sample_key_add_body();
        body.ttl = 0;
        assert_eq!(
            validate_key_add_body(&body),
            Err(ValidationError::InvalidTtl(MAX_KEY_TTL_SECONDS))
        );
    }

    #[test]
    fn key_add_body_rejects_ttl_over_max() {
        let mut body = sample_key_add_body();
        body.ttl = MAX_KEY_TTL_SECONDS + 1;
        assert_eq!(
            validate_key_add_body(&body),
            Err(ValidationError::InvalidTtl(MAX_KEY_TTL_SECONDS))
        );
    }

    #[test]
    fn key_add_body_accepts_ttl_at_max() {
        let mut body = sample_key_add_body();
        body.ttl = MAX_KEY_TTL_SECONDS;
        assert_eq!(validate_key_add_body(&body), Ok(()));
    }

    #[test]
    fn key_add_body_rejects_unknown_metadata_type() {
        let mut body = sample_key_add_body();
        body.metadata_type = 2;
        assert_eq!(
            validate_key_add_body(&body),
            Err(ValidationError::InvalidMetadataType)
        );
    }

    #[test]
    fn key_add_body_rejects_empty_metadata() {
        let mut body = sample_key_add_body();
        body.metadata = vec![];
        assert_eq!(
            validate_key_add_body(&body),
            Err(ValidationError::MissingMetadata)
        );
    }

    #[test]
    fn key_add_body_rejects_empty_scopes() {
        let mut body = sample_key_add_body();
        body.scopes = vec![];
        assert_eq!(
            validate_key_add_body(&body),
            Err(ValidationError::EmptyScopes)
        );
    }

    #[test]
    fn key_add_body_rejects_invalid_scope_value() {
        let mut body = sample_key_add_body();
        // 9999 is far outside the MessageType enum range.
        body.scopes = vec![MessageType::CastAdd as i32, 9999];
        assert_eq!(
            validate_key_add_body(&body),
            Err(ValidationError::InvalidScope(9999))
        );
    }

    #[test]
    fn key_add_body_rejects_none_scope() {
        let mut body = sample_key_add_body();
        body.scopes = vec![MessageType::None as i32];
        assert_eq!(
            validate_key_add_body(&body),
            Err(ValidationError::InvalidScope(0))
        );
    }

    #[test]
    fn key_add_body_rejects_key_add_scope() {
        let mut body = sample_key_add_body();
        body.scopes = vec![MessageType::CastAdd as i32, MessageType::KeyAdd as i32];
        assert_eq!(
            validate_key_add_body(&body),
            Err(ValidationError::InvalidScope(MessageType::KeyAdd as i32))
        );
    }

    #[test]
    fn key_add_body_rejects_too_many_scopes() {
        let mut body = sample_key_add_body();
        // Enum-valid but over-length: we pick one legal scope and repeat it past the cap.
        body.scopes = vec![MessageType::CastAdd as i32; MAX_KEY_ADD_SCOPES + 1];
        assert_eq!(
            validate_key_add_body(&body),
            Err(ValidationError::TooManyScopes(MAX_KEY_ADD_SCOPES))
        );
    }

    #[test]
    fn key_add_body_accepts_scopes_at_max() {
        let mut body = sample_key_add_body();
        body.scopes = vec![MessageType::CastAdd as i32; MAX_KEY_ADD_SCOPES];
        assert_eq!(validate_key_add_body(&body), Ok(()));
    }

    #[test]
    fn key_add_body_rejects_key_remove_scope() {
        let mut body = sample_key_add_body();
        body.scopes = vec![MessageType::KeyRemove as i32];
        assert_eq!(
            validate_key_add_body(&body),
            Err(ValidationError::InvalidScope(MessageType::KeyRemove as i32))
        );
    }

    #[test]
    fn key_remove_body_accepts_custody_sig_type() {
        let body = sample_key_remove_body();
        assert_eq!(validate_key_remove_body(&body), Ok(()));
    }

    #[test]
    fn key_remove_body_accepts_self_sig_type() {
        let mut body = sample_key_remove_body();
        body.signature_type = KeyRemoveSignatureType::SelfRevoke as u32;
        assert_eq!(validate_key_remove_body(&body), Ok(()));
    }

    #[test]
    fn key_remove_body_rejects_wrong_key_length() {
        let mut body = sample_key_remove_body();
        body.key = vec![0xAAu8; 33];
        assert_eq!(
            validate_key_remove_body(&body),
            Err(ValidationError::InvalidKeyLength)
        );
    }

    #[test]
    fn key_remove_body_rejects_unknown_signature_type() {
        let mut body = sample_key_remove_body();
        body.signature_type = 3;
        assert_eq!(
            validate_key_remove_body(&body),
            Err(ValidationError::InvalidSignatureType)
        );
    }

    #[test]
    fn key_remove_body_rejects_missing_deadline() {
        let mut body = sample_key_remove_body();
        body.deadline = 0;
        assert_eq!(
            validate_key_remove_body(&body),
            Err(ValidationError::MissingDeadline)
        );
    }
}
