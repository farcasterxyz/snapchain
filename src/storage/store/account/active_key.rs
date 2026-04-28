//! Unified active-key lookup across on-chain signer events and the gasless-key store.
//!
//! NEYN-10575 introduces scope enforcement for off-chain (gasless) keys: a KEY_ADD declares a
//! list of allowed `MessageType` values ("scopes"), and every user message signed by that key
//! must fall within that list. This module centralizes the lookup that answers
//!
//! > Is `signer` an active key for `fid`, and if so, what (if any) constraints apply?
//!
//! so validation sites in both `ShardEngine` and `BlockEngine` can call one function and
//! pattern-match on the result.
//!
//! ## Lookup order: on-chain first, gasless on miss
//!
//! On-chain signers are grandfathered (no scope check) and are the hot path today. Keeping them
//! first means the common case stays at one RocksDB read. A miss on on-chain falls through to
//! the gasless-key store — a second read, but only for off-chain traffic.
//!
//! The order also avoids a correctness surprise: if the same Ed25519 key somehow appeared in
//! both stores (it shouldn't by construction — on-chain keys are minted via the L2 KeyRegistry
//! and off-chain keys via custody-signed KEY_ADD), preferring on-chain yields the more lenient
//! (scope-free) interpretation. That matches the grandfathering intent.
//!
//! ## Scopes as a bitmask
//!
//! Scopes are stored as a `repeated int32` list in `KeyAddBody`, but evaluated as a `u64`
//! bitmask: bit N is set iff `MessageType` value N is allowed. Admission check is one `AND`.
//! `MessageType` enum values currently top out at 17 (`MESSAGE_TYPE_KEY_REMOVE`), so a `u64`
//! comfortably fits the full table with headroom.
//!
//! Mask construction is O(scopes.len()) and runs once per read; the per-message check itself
//! is O(1). The record is not mutated to cache the mask — `GaslessKeyRecord` intentionally
//! stores only the originating message plus the expensive-to-recompute `request_fid` (see the
//! doc header on `GaslessKeyRecord`), and a bitwise shift-OR over ≤15 ints is cheap enough to
//! keep on the read path.

use std::sync::Arc;

use super::{get_gasless_key_record, OnchainEventStore};
use crate::proto::{self, message_data::Body, MessageType};
use crate::storage::db::{RocksDB, RocksDbTransactionBatch};
use crate::storage::store::account::onchain_event_store::OnchainEventStorageError;

/// Result of resolving `(fid, signer)` against the active-key indexes.
///
/// `OnChain` carries no data because on-chain signers bypass scope enforcement entirely — they
/// predate the scope model and are grandfathered. `Gasless` carries the bitmask plus the `ttl`
/// value from the originating KEY_ADD so downstream validation (scope check now, sliding
/// expiry in NEYN-10576) can operate without a second lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActiveKey {
    OnChain,
    Gasless { scopes_mask: u64, ttl_seconds: u32 },
}

impl ActiveKey {
    /// Returns true iff `msg_type` is admitted by this key's scope. On-chain keys admit
    /// everything; gasless keys admit only the types whose bit is set in `scopes_mask`.
    pub fn admits(&self, msg_type: MessageType) -> bool {
        match self {
            ActiveKey::OnChain => true,
            ActiveKey::Gasless { scopes_mask, .. } => {
                let bit = message_type_bit(msg_type);
                (scopes_mask & bit) != 0
            }
        }
    }
}

/// Compute the `u64` scope bitmask from the `KeyAddBody.scopes` list.
///
/// Unknown/out-of-range scope values are dropped rather than erroring — `validate_key_add_body`
/// already rejects any KEY_ADD whose scopes list contains an invalid `MessageType` (see
/// `core::validations::key::validate_key_add_scopes`), so by the time a `GaslessKeyRecord` is
/// persisted the list is already clean. Silent drop here is defense in depth against a future
/// MessageType variant being added without a corresponding store-layer update.
pub fn compute_scopes_mask(scopes: &[i32]) -> u64 {
    let mut mask: u64 = 0;
    for &scope in scopes {
        // MessageType enum values are small non-negative integers; anything else is invalid.
        if let Ok(bit_pos) = u32::try_from(scope) {
            if bit_pos < 64 {
                mask |= 1u64 << bit_pos;
            }
        }
    }
    mask
}

/// Bit position for a `MessageType` in the scope bitmask.
#[inline]
fn message_type_bit(msg_type: MessageType) -> u64 {
    let v = msg_type as i32;
    // `MessageType::None` (0) should never be admitted by any scope — but encoding it as bit 0
    // is harmless: `validate_key_add_scopes` rejects `MessageType::None` in the scopes list, so
    // no `GaslessKeyRecord` can have bit 0 set.
    if let Ok(bit_pos) = u32::try_from(v) {
        if bit_pos < 64 {
            return 1u64 << bit_pos;
        }
    }
    0
}

/// Resolve `(fid, signer)` against both signer indexes. Returns `None` if the key is not active
/// in either.
///
/// The on-chain store is checked first so the hot path remains a single RocksDB read for
/// messages signed by on-chain keys. A gasless-only signer incurs one additional read.
pub fn get_active_key(
    onchain_event_store: &OnchainEventStore,
    db: &Arc<RocksDB>,
    txn_batch: &RocksDbTransactionBatch,
    fid: u64,
    signer: &[u8],
) -> Result<Option<ActiveKey>, OnchainEventStorageError> {
    if onchain_event_store
        .get_active_signer(fid, signer.to_vec(), Some(txn_batch))?
        .is_some()
    {
        return Ok(Some(ActiveKey::OnChain));
    }

    // Gasless store miss/corrupt-record errors are surfaced as on-chain-store errors so the
    // caller can treat them uniformly. The DB is the same RocksDB instance; any error here is
    // a storage failure, not a validation failure.
    let record = get_gasless_key_record(db, txn_batch, fid, signer)
        .map_err(OnchainEventStorageError::HubError)?;
    let Some(record) = record else {
        return Ok(None);
    };

    // A corrupt record whose embedded message lacks `data.body.key_add_body` would have failed
    // to merge in the first place (see `merge_key_add`). Treat it as a miss defensively rather
    // than panicking on a malformed row.
    let (scopes_mask, ttl_seconds) = extract_scope_and_ttl(&record.message).unwrap_or((0, 0));
    Ok(Some(ActiveKey::Gasless {
        scopes_mask,
        ttl_seconds,
    }))
}

fn extract_scope_and_ttl(message: &Option<proto::Message>) -> Option<(u64, u32)> {
    let key_add = match message.as_ref()?.data.as_ref()?.body.as_ref()? {
        Body::KeyAddBody(body) => body,
        _ => return None,
    };
    Some((compute_scopes_mask(&key_add.scopes), key_add.ttl))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{
        self, message_data::Body, KeyAddBody, MessageData, MessageType, SignerEventType,
    };
    use crate::storage::db::{self, RocksDbTransactionBatch};
    use crate::storage::store::account::{
        put_gasless_key_record, GaslessKeyRecord, StoreEventHandler,
    };
    use crate::utils::factory::{events_factory, signers};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn fresh_store() -> (OnchainEventStore, Arc<db::RocksDB>, TempDir) {
        let dir = tempfile::TempDir::new().unwrap();
        let db_path = dir.path().join("active_key.db");
        let rocks = db::RocksDB::new(db_path.to_str().unwrap());
        rocks.open().unwrap();
        let rocks = Arc::new(rocks);
        let store = OnchainEventStore::new(rocks.clone(), StoreEventHandler::new());
        (store, rocks, dir)
    }

    fn sample_gasless_record(signer_pubkey: &[u8], scopes: Vec<i32>, ttl: u32) -> GaslessKeyRecord {
        GaslessKeyRecord {
            message: Some(proto::Message {
                data: Some(MessageData {
                    r#type: MessageType::KeyAdd as i32,
                    fid: 0,
                    timestamp: 0,
                    network: 0,
                    body: Some(Body::KeyAddBody(KeyAddBody {
                        key: signer_pubkey.to_vec(),
                        key_type: 1,
                        custody_signature: vec![],
                        deadline: 0,
                        nonce: 0,
                        metadata: vec![],
                        metadata_type: 1,
                        registration_tx_hash: vec![],
                        scopes,
                        ttl,
                    })),
                }),
                hash: vec![],
                hash_scheme: 0,
                signature: vec![],
                signature_scheme: 0,
                signer: vec![],
                data_bytes: None,
            }),
            request_fid: 0,
        }
    }

    #[test]
    fn mask_is_empty_for_empty_scopes() {
        assert_eq!(compute_scopes_mask(&[]), 0);
    }

    #[test]
    fn mask_sets_expected_bits() {
        // CAST_ADD = 1, REACTION_ADD = 3, LINK_ADD = 5
        let mask = compute_scopes_mask(&[1, 3, 5]);
        assert_eq!(mask, (1u64 << 1) | (1u64 << 3) | (1u64 << 5));
    }

    #[test]
    fn mask_silently_drops_out_of_range_values() {
        // Negative values and values >= 64 are dropped (validation elsewhere prevents these
        // from reaching the store, but the mask path is defensive).
        let mask = compute_scopes_mask(&[-1, 1, 64, 100]);
        assert_eq!(mask, 1u64 << 1);
    }

    #[test]
    fn onchain_variant_admits_any_message_type() {
        let k = ActiveKey::OnChain;
        assert!(k.admits(MessageType::CastAdd));
        assert!(k.admits(MessageType::LinkRemove));
        assert!(k.admits(MessageType::UserDataAdd));
    }

    #[test]
    fn gasless_variant_admits_only_scoped_types() {
        let k = ActiveKey::Gasless {
            scopes_mask: compute_scopes_mask(&[MessageType::CastAdd as i32]),
            ttl_seconds: 3600,
        };
        assert!(k.admits(MessageType::CastAdd));
        assert!(!k.admits(MessageType::ReactionAdd));
        assert!(!k.admits(MessageType::LinkAdd));
    }

    #[test]
    fn gasless_variant_with_empty_mask_admits_nothing() {
        let k = ActiveKey::Gasless {
            scopes_mask: 0,
            ttl_seconds: 3600,
        };
        assert!(!k.admits(MessageType::CastAdd));
        assert!(!k.admits(MessageType::None));
    }

    #[test]
    fn get_active_key_returns_none_when_no_record_in_either_store() {
        let (store, rocks, _dir) = fresh_store();
        let txn = RocksDbTransactionBatch::new();
        let result = get_active_key(&store, &rocks, &txn, 42, &[0xAA; 32]).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn get_active_key_returns_onchain_for_an_onchain_signer() {
        let (store, rocks, _dir) = fresh_store();
        let signing_key = signers::generate_signer();
        let pubkey = signing_key.verifying_key().as_bytes().to_vec();
        let event =
            events_factory::create_signer_event(7, signing_key, SignerEventType::Add, None, None);

        let mut merge_txn = RocksDbTransactionBatch::new();
        store.merge_onchain_event(event, &mut merge_txn).unwrap();
        rocks.commit(merge_txn).unwrap();

        let txn = RocksDbTransactionBatch::new();
        let result = get_active_key(&store, &rocks, &txn, 7, &pubkey).unwrap();
        assert_eq!(result, Some(ActiveKey::OnChain));
    }

    #[test]
    fn get_active_key_returns_gasless_variant_with_mask_and_ttl() {
        let (store, rocks, _dir) = fresh_store();
        let signer_pubkey = [0xCC; 32];
        let scopes = vec![MessageType::CastAdd as i32, MessageType::LinkAdd as i32];
        let record = sample_gasless_record(&signer_pubkey, scopes.clone(), 7200);

        let mut merge_txn = RocksDbTransactionBatch::new();
        put_gasless_key_record(&rocks, &mut merge_txn, 9, &signer_pubkey, &record).unwrap();
        rocks.commit(merge_txn).unwrap();

        let txn = RocksDbTransactionBatch::new();
        let result = get_active_key(&store, &rocks, &txn, 9, &signer_pubkey).unwrap();
        match result {
            Some(ActiveKey::Gasless {
                scopes_mask,
                ttl_seconds,
            }) => {
                assert_eq!(scopes_mask, compute_scopes_mask(&scopes));
                assert_eq!(ttl_seconds, 7200);
                // Positive and negative admission inside the scope bitmask.
                let k = ActiveKey::Gasless {
                    scopes_mask,
                    ttl_seconds,
                };
                assert!(k.admits(MessageType::CastAdd));
                assert!(k.admits(MessageType::LinkAdd));
                assert!(!k.admits(MessageType::ReactionAdd));
            }
            other => panic!("expected Gasless, got {other:?}"),
        }
    }

    #[test]
    fn onchain_takes_priority_when_key_is_in_both_stores() {
        // Belt-and-suspenders: construction shouldn't let the same key appear in both stores, but
        // if it ever did, Fork C (on-chain first) should yield the more lenient OnChain variant.
        let (store, rocks, _dir) = fresh_store();
        let signing_key = signers::generate_signer();
        let pubkey = signing_key.verifying_key().as_bytes().to_vec();
        let event =
            events_factory::create_signer_event(3, signing_key, SignerEventType::Add, None, None);

        let mut merge_txn = RocksDbTransactionBatch::new();
        store.merge_onchain_event(event, &mut merge_txn).unwrap();
        let record = sample_gasless_record(&pubkey, vec![MessageType::CastAdd as i32], 3600);
        put_gasless_key_record(&rocks, &mut merge_txn, 3, &pubkey, &record).unwrap();
        rocks.commit(merge_txn).unwrap();

        let txn = RocksDbTransactionBatch::new();
        let result = get_active_key(&store, &rocks, &txn, 3, &pubkey).unwrap();
        assert_eq!(result, Some(ActiveKey::OnChain));
    }
}
