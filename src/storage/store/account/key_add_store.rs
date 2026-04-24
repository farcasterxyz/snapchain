//! On-disk store for active off-chain (gasless) Ed25519 keys — the "KEY_ADD" store.
//!
//! Populated by the KEY_ADD merge path and cleared by KEY_REMOVE. Distinct from on-chain signer
//! events: on-chain signers live in `onchain_event_store.rs` under `RootPrefix::OnChainEvent`,
//! gasless keys live here under `RootPrefix::GaslessKey`. The two spaces are unified at read time
//! (NEYN-10580 / #8 "combined active keys") — for now callers that need "is this key active?"
//! check both, gasless-first.
//!
//! Storage layout:
//! ```text
//! [RootPrefix::GaslessKey (1B)] [UserPostfix::GaslessKeyByFid (1B)] [FID (4B, BE)] [PublicKey (32B)]
//!     -> prost-encoded GaslessKeyRecord
//!
//! [RootPrefix::GaslessKey (1B)] [UserPostfix::GaslessKeyByPublicKey (1B)] [PublicKey (32B)]
//!     -> FID (4B, BE)
//! ```
//!
//! The by-public-key index enforces global uniqueness *within the gasless-signer namespace*:
//! a given Ed25519 key may hold a gasless registration for at most one FID at a time.
//! On-chain signers are explicitly out of scope — they are not indexed here and their
//! uniqueness (or non-uniqueness) is unaffected. The index is written and cleared alongside
//! the primary record by `merge_key_add` / `merge_key_remove` so both gasless indices stay
//! in lock-step.

use prost::Message;

use super::{get_from_db_or_txn, make_fid_key, read_fid_key, FID_BYTES};
use crate::core::error::HubError;
use crate::core::validations::key::ED25519_PUBLIC_KEY_LEN;
use crate::proto;
use crate::storage::{
    constants::{RootPrefix, UserPostfix},
    db::{RocksDB, RocksDbTransactionBatch},
};

/// On-disk record for an active off-chain Ed25519 key.
///
/// Defined as a Rust-native prost type (not in `.proto`) because this record is purely a
/// storage-layer concern. It never crosses a network boundary and no SDK consumer needs it —
/// surfacing gasless keys over RPC (NEYN-10578) will define its own response proto that joins
/// this record with `last_used_at` from the sibling store. Using `#[derive(prost::Message)]`
/// keeps the wire-codec story uniform with the rest of the codebase while avoiding pollution
/// of `message.proto` with a type that has no wire consumers. The on-disk encoding is
/// bit-identical to what `prost-build` would produce from an equivalent `.proto` definition.
///
/// ## Shape: embedded Message + one derived-field cache
///
/// This record is a *superset* of the originating KEY_ADD `proto::Message` — field 1 embeds
/// the full message envelope, so `record.message` alone is sufficient to reconstruct the
/// wire-format message byte-for-byte. Everything carried by `KeyAddBody` (key, key_type,
/// custody_signature, deadline, nonce, metadata, metadata_type, registration_tx_hash, scopes,
/// ttl) and by the outer `Message` envelope (fid, timestamp, network, hash, signature,
/// signature_scheme) is recoverable from that single field with no loss.
///
/// Keeping the message as the source of truth avoids drift: there is exactly one copy of each
/// KEY_ADD byte on disk, and any future proto evolution on `KeyAddBody` or `Message` flows
/// through without migrating this record.
///
/// ## Why `request_fid` is cached as a separate field
///
/// `request_fid` is the verified `requestFid` from `SignedKeyRequestMetadata`. It is NOT a
/// plain field on the wire message — it lives inside the ABI-encoded metadata blob at
/// `record.message.data.body.key_add_body.metadata`. Recovering it at read time costs, per
/// call:
///   1. ABI-decode the metadata bytes into `SignedKeyRequestMetadata` (dynamic Solidity ABI
///      parse with variable-length `bytes` fields),
///   2. Rebuild the EIP-712 typed data for `SignedKeyRequest`,
///   3. Run ECDSA public-key recovery on the embedded signature,
///   4. Compare the recovered address to the stored `requestSigner`.
///
/// That's ~10µs of crypto per read on typical hardware — negligible once at KEY_ADD merge
/// time (the write path has to do this anyway, as part of validation) but load-bearing on
/// any hot read path. The two concrete read paths that need `request_fid` are:
///   * Self-revocation KEY_REMOVE (`signature_type = 2`) — uses `request_fid` as the key
///     into the app-nonce store on EVERY self-revocation. NEYN-10574.
///   * RPC surfacing (`GetGaslessKey` / `ListKeysByFid`) — every query. NEYN-10578.
///
/// Verifying once at write time and caching the result here turns an O(reads) cost into an
/// O(writes) cost.
///
/// ## Why scopes/ttl/timestamp/hash are NOT also cached here
///
/// Those are plain field accesses on `record.message.data.body.key_add_body` (or on the
/// outer `Message`) — we'd just be pointer-chasing through a protobuf that's
/// already deserialized by the time the caller reads the record. Duplicating them would be
/// storage bloat and a drift risk (two places to update, two to keep in sync).
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GaslessKeyRecord {
    #[prost(message, optional, tag = "1")]
    pub message: ::core::option::Option<proto::Message>,
    #[prost(uint64, tag = "2")]
    pub request_fid: u64,
}

// Byte sizes for the key layout; mirrors key_last_used_at_store.rs for consistency.
const ROOT_PREFIX_BYTES: usize = 1;
const USER_POSTFIX_BYTES: usize = 1;
const GASLESS_KEY_BY_FID_KEY_BYTES: usize =
    ROOT_PREFIX_BYTES + USER_POSTFIX_BYTES + FID_BYTES + ED25519_PUBLIC_KEY_LEN;
const GASLESS_KEY_BY_PUBLIC_KEY_KEY_BYTES: usize =
    ROOT_PREFIX_BYTES + USER_POSTFIX_BYTES + ED25519_PUBLIC_KEY_LEN;

fn validate_key_len(public_key: &[u8]) -> Result<(), HubError> {
    if public_key.len() != ED25519_PUBLIC_KEY_LEN {
        return Err(HubError {
            code: "bad_request.validation_failure".to_string(),
            message: format!(
                "gasless-key store: expected {}-byte key, got {}",
                ED25519_PUBLIC_KEY_LEN,
                public_key.len()
            ),
        });
    }
    Ok(())
}

pub fn make_gasless_key_by_fid_key(fid: u64, public_key: &[u8]) -> Result<Vec<u8>, HubError> {
    validate_key_len(public_key)?;
    let mut key = Vec::with_capacity(GASLESS_KEY_BY_FID_KEY_BYTES);
    key.push(RootPrefix::GaslessKey as u8);
    key.push(UserPostfix::GaslessKeyByFid as u8);
    key.extend_from_slice(&make_fid_key(fid));
    key.extend_from_slice(public_key);
    Ok(key)
}

pub fn get_gasless_key_record(
    db: &RocksDB,
    txn: &RocksDbTransactionBatch,
    fid: u64,
    public_key: &[u8],
) -> Result<Option<GaslessKeyRecord>, HubError> {
    let key = make_gasless_key_by_fid_key(fid, public_key)?;
    match get_from_db_or_txn(db, txn, &key)? {
        None => Ok(None),
        Some(bytes) => GaslessKeyRecord::decode(bytes.as_slice())
            .map(Some)
            .map_err(|e| HubError {
                code: "internal_error".to_string(),
                message: format!("corrupt GaslessKeyRecord at fid={}: {}", fid, e),
            }),
    }
}

pub fn put_gasless_key_record(
    db: &RocksDB,
    txn: &mut RocksDbTransactionBatch,
    fid: u64,
    public_key: &[u8],
    record: &GaslessKeyRecord,
) -> Result<(), HubError> {
    let _ = db;
    let key = make_gasless_key_by_fid_key(fid, public_key)?;
    txn.put(key, record.encode_to_vec());
    Ok(())
}

pub fn delete_gasless_key_record(
    db: &RocksDB,
    txn: &mut RocksDbTransactionBatch,
    fid: u64,
    public_key: &[u8],
) -> Result<(), HubError> {
    let _ = db;
    let key = make_gasless_key_by_fid_key(fid, public_key)?;
    txn.delete(key);
    Ok(())
}

// ---------------------------------------------------------------------------
// By-public-key index (global-uniqueness owner map)
// ---------------------------------------------------------------------------

pub fn make_gasless_key_by_public_key_key(public_key: &[u8]) -> Result<Vec<u8>, HubError> {
    validate_key_len(public_key)?;
    let mut key = Vec::with_capacity(GASLESS_KEY_BY_PUBLIC_KEY_KEY_BYTES);
    key.push(RootPrefix::GaslessKey as u8);
    key.push(UserPostfix::GaslessKeyByPublicKey as u8);
    key.extend_from_slice(public_key);
    Ok(key)
}

/// Returns the FID currently claiming `public_key` as a gasless signer, or `None` if the key
/// has no gasless registration. Scope is gasless-only — this returns `None` even if the key is
/// an active on-chain signer for some FID, by design. Reads through the in-flight txn batch so
/// conflict checks within the same commit see writes staged earlier in the batch; this is
/// what lets two KEY_ADDs for the same key in one shard commit be mutually exclusive.
pub fn get_gasless_key_owner_fid(
    db: &RocksDB,
    txn: &RocksDbTransactionBatch,
    public_key: &[u8],
) -> Result<Option<u64>, HubError> {
    let key = make_gasless_key_by_public_key_key(public_key)?;
    match get_from_db_or_txn(db, txn, &key)? {
        None => Ok(None),
        Some(bytes) => {
            if bytes.len() != FID_BYTES {
                return Err(HubError {
                    code: "internal_error".to_string(),
                    message: format!(
                        "corrupt gasless-key owner entry: expected {} bytes, got {}",
                        FID_BYTES,
                        bytes.len()
                    ),
                });
            }
            Ok(Some(read_fid_key(&bytes, 0)))
        }
    }
}

pub fn put_gasless_key_owner(
    db: &RocksDB,
    txn: &mut RocksDbTransactionBatch,
    public_key: &[u8],
    fid: u64,
) -> Result<(), HubError> {
    let _ = db;
    let key = make_gasless_key_by_public_key_key(public_key)?;
    txn.put(key, make_fid_key(fid));
    Ok(())
}

pub fn delete_gasless_key_owner(
    db: &RocksDB,
    txn: &mut RocksDbTransactionBatch,
    public_key: &[u8],
) -> Result<(), HubError> {
    let _ = db;
    let key = make_gasless_key_by_public_key_key(public_key)?;
    txn.delete(key);
    Ok(())
}
