//! Sliding-TTL `last_used_at` store for gasless keys.
//!
//! Implements the OAuth2-style sliding-expiry model from the off-chain signers FIP: every
//! validated user message signed by a TTL'd key bumps `last_used_at = message.timestamp`, and a
//! key is rejected once `last_used_at + ttl < current_block_timestamp`. There are no refresh
//! tokens — *use* is the renewal signal. Revocation (`KEY_REMOVE`) deletes the entry and does
//! not extend expiry.
//!
//! Skip entirely for on-chain signers (which have `ttl == 0`): those keep the hot path untouched.
//! Gasless keys are required by `validate_key_add_body` to have `0 < ttl <= MAX_KEY_TTL_SECONDS`
//! (90 days), so every call site into this store can assume a bounded, non-zero ttl — in
//! particular, `stored + ttl` cannot overflow u32 for any realistic farcaster-epoch timestamp.
//!
//! Storage layout:
//! ```text
//! [RootPrefix::GaslessKey (1B)] [UserPostfix::GaslessKeyLastUsedAt (1B)] [FID (4B, BE)] [PublicKey (32B)]
//!     -> u32 last_used_at (4B big-endian, farcaster-epoch seconds)
//! ```

use super::{get_from_db_or_txn, make_fid_key, FID_BYTES};
use crate::core::error::HubError;
use crate::core::validations::key::ED25519_PUBLIC_KEY_LEN;
use crate::storage::{
    constants::{RootPrefix, UserPostfix},
    db::{RocksDB, RocksDbTransactionBatch},
};

// Byte sizes for the key layout.
const ROOT_PREFIX_BYTES: usize = 1;
const USER_POSTFIX_BYTES: usize = 1;
const LAST_USED_AT_KEY_BYTES: usize =
    ROOT_PREFIX_BYTES + USER_POSTFIX_BYTES + FID_BYTES + ED25519_PUBLIC_KEY_LEN;

// Value is a single big-endian u32 timestamp.
const LAST_USED_AT_VALUE_BYTES: usize = 4;

/// Sliding-TTL throttle window for `check_and_bump_last_used_at`. A valid-use bump that would
/// advance the stored timestamp by this many seconds or less is skipped (the stored value is
/// left unchanged). The *expiry* check still runs every call — throttling only suppresses the
/// `txn.put`, so a heavy sender on a valid key no longer amplifies RocksDB writes 1:1 with
/// message volume. The threshold is a constant so the decision is deterministic across nodes
/// replaying the same block.
pub const SLIDING_TTL_THROTTLE_SECONDS: u64 = 300;

fn validate_key_len(public_key: &[u8]) -> Result<(), HubError> {
    if public_key.len() != ED25519_PUBLIC_KEY_LEN {
        return Err(HubError {
            code: "bad_request.validation_failure".to_string(),
            message: format!(
                "gasless-key last_used_at: expected {}-byte key, got {}",
                ED25519_PUBLIC_KEY_LEN,
                public_key.len()
            ),
        });
    }
    Ok(())
}

pub fn make_last_used_at_key(fid: u64, public_key: &[u8]) -> Result<Vec<u8>, HubError> {
    validate_key_len(public_key)?;
    let mut key = Vec::with_capacity(LAST_USED_AT_KEY_BYTES);
    key.push(RootPrefix::GaslessKey as u8);
    key.push(UserPostfix::GaslessKeyLastUsedAt as u8);
    key.extend_from_slice(&make_fid_key(fid));
    key.extend_from_slice(public_key);
    Ok(key)
}

pub fn get_last_used_at(
    db: &RocksDB,
    txn: &RocksDbTransactionBatch,
    fid: u64,
    public_key: &[u8],
) -> Result<Option<u32>, HubError> {
    let key = make_last_used_at_key(fid, public_key)?;
    match get_from_db_or_txn(db, txn, &key)? {
        None => Ok(None),
        Some(bytes) => {
            if bytes.len() != LAST_USED_AT_VALUE_BYTES {
                return Err(HubError {
                    code: "internal_error".to_string(),
                    message: format!(
                        "corrupt gasless-key last_used_at value: expected {} bytes, got {}",
                        LAST_USED_AT_VALUE_BYTES,
                        bytes.len()
                    ),
                });
            }
            let mut buf = [0u8; LAST_USED_AT_VALUE_BYTES];
            buf.copy_from_slice(&bytes);
            Ok(Some(u32::from_be_bytes(buf)))
        }
    }
}

/// Initializes the `last_used_at` counter for a newly-added gasless key to `message.timestamp`.
/// Called from the `KEY_ADD` merge path after all other validation passes. Writes unconditionally:
/// KEY_ADD-on-existing-key is already rejected upstream, and a re-add after KEY_REMOVE finds no
/// entry (KEY_REMOVE deletes it) so the overwrite is a clean insert.
pub fn init_last_used_at(
    db: &RocksDB,
    txn: &mut RocksDbTransactionBatch,
    fid: u64,
    public_key: &[u8],
    message_timestamp: u32,
) -> Result<(), HubError> {
    let _ = db; // unused today; kept in signature for symmetry with get/check_and_bump
    let key = make_last_used_at_key(fid, public_key)?;
    txn.put(key, message_timestamp.to_be_bytes().to_vec());
    Ok(())
}

/// Deletes the `last_used_at` entry for a revoked gasless key. Called from the `KEY_REMOVE` merge
/// path. No-op if the entry is already absent; RocksDB tolerates deletes of missing keys.
pub fn delete_last_used_at(
    db: &RocksDB,
    txn: &mut RocksDbTransactionBatch,
    fid: u64,
    public_key: &[u8],
) -> Result<(), HubError> {
    let _ = db;
    let key = make_last_used_at_key(fid, public_key)?;
    txn.delete(key);
    Ok(())
}

/// Validates that the gasless key is not expired and stages a `last_used_at` bump to
/// `message_timestamp`. Called from the per-message validation hook for every user message signed
/// by a TTL'd key.
///
/// ## Expiry rule
///
/// A key is *not expired* when `stored + ttl >= current_block_timestamp`. Equality accepts — the
/// key expires strictly when `stored + ttl < current_block_timestamp`.
///
/// ## Side effect on acceptance
///
/// Stages `last_used_at = message_timestamp` via `txn.put(...)`. The caller commits the txn batch
/// when the surrounding message is successfully merged; on rollback the counter is unchanged.
///
/// ## Parameters
///
/// * `ttl` — the key's TTL in seconds from its `KeyAddBody`. Required to be in
///   `(0, MAX_KEY_TTL_SECONDS]` by `validate_key_add_body`; on-chain keys (`ttl == 0`) skip this
///   store entirely and must not reach this function.
/// * `message_timestamp` — farcaster-epoch seconds of the message being validated. On acceptance
///   this replaces the stored `last_used_at`.
/// * `current_block_timestamp` — farcaster-epoch seconds of the block currently being merged;
///   the reference point for the expiry comparison.
///
/// ## Errors
///
/// * `bad_request.validation_failure` — key is expired under the rule above. Message includes
///   `stored`, `ttl`, and `current_block_timestamp` so the decision is reconstructable from logs.
///   The stored value is left unchanged.
/// * `internal_error` — `last_used_at` entry missing (KEY_ADD never initialized it, or this was
///   called for a non-gasless key) or the stored value is corrupt. Contract violation, not a
///   user-facing rejection.
pub fn check_and_bump_last_used_at(
    db: &RocksDB,
    txn: &mut RocksDbTransactionBatch,
    fid: u64,
    public_key: &[u8],
    ttl: u32,
    message_timestamp: u32,
    current_block_timestamp: u64,
) -> Result<(), HubError> {
    let key = make_last_used_at_key(fid, public_key)?;
    let stored = match get_from_db_or_txn(db, txn, &key)? {
        Some(bytes) => {
            if bytes.len() != LAST_USED_AT_VALUE_BYTES {
                return Err(HubError {
                    code: "internal_error".to_string(),
                    message: format!(
                        "corrupt gasless-key last_used_at value: expected {} bytes, got {}",
                        LAST_USED_AT_VALUE_BYTES,
                        bytes.len()
                    ),
                });
            }
            let mut buf = [0u8; LAST_USED_AT_VALUE_BYTES];
            buf.copy_from_slice(&bytes);
            u32::from_be_bytes(buf) as u64
        }
        None => {
            // Missing entry means the key was never initialized via `init_last_used_at` — either
            // the engine didn't call `init_last_used_at` on KEY_ADD, or this is being invoked for
            // a non-gasless key. Either is a contract violation; surface as internal_error so it's
            // loud in logs rather than silently accepting the message.
            return Err(HubError {
                code: "internal_error".to_string(),
                message: format!(
                    "gasless-key last_used_at entry missing for fid={} (expected KEY_ADD to have initialized it)",
                    fid
                ),
            });
        }
    };

    // `stored + ttl` cannot overflow u32: `ttl` is capped at `MAX_KEY_TTL_SECONDS` by
    // `validate_key_add_body` and `stored` is a farcaster-epoch second count well below
    // u32 saturation for any realistic block. See the module docstring for the full invariant.
    if stored + (ttl as u64) < current_block_timestamp {
        return Err(HubError {
            code: "bad_request.validation_failure".to_string(),
            message: format!(
                "gasless-key last_used_at expired: stored={} + ttl={} < current_block_timestamp={}",
                stored, ttl, current_block_timestamp
            ),
        });
    }

    // Sliding-TTL throttle (NEYN-10579): skip the put unless the bump would advance `stored` by
    // strictly more than SLIDING_TTL_THROTTLE_SECONDS. The key is still valid (expiry check above
    // already accepted it); we just leave the counter alone to cut write amplification. Strict
    // `>` also implicitly blocks any non-increasing `message_timestamp`: if
    // `message_ts > stored + 300` holds then `message_ts > stored` trivially — out-of-order
    // messages (which would only arise from a caller contract violation) can never overwrite.
    let message_ts_u64 = message_timestamp as u64;
    if message_ts_u64 > stored + SLIDING_TTL_THROTTLE_SECONDS {
        txn.put(key, message_timestamp.to_be_bytes().to_vec());
    }
    Ok(())
}
