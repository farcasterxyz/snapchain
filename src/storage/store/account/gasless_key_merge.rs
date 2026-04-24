//! Free-function orchestrators for `KEY_ADD` and `KEY_REMOVE` merges.
//!
//! These were originally methods on `ShardEngine` (NEYN-10573 / NEYN-10574). NEYN-10580 routes
//! both message types to shard 0, so `BlockEngine` now also needs to run them — the functions
//! were lifted here so both engines can call the same code without duplication or an awkward
//! trait abstraction. Each engine passes its own `db` and `onchain_event_store`; the merge
//! logic is otherwise identical.
//!
//! `ShardEngine` still dispatches to these (via `ShardEngine::merge_message`) because
//! `BlockEvent` replay of KEY_ADD / KEY_REMOVE on shards 1..N re-runs the merge against each
//! shard's local DB. See NEYN-10580 for the propagation model.

use std::sync::Arc;

use super::{
    check_and_set_app_nonce, check_and_set_user_nonce, decrement_gasless_key_count,
    delete_gasless_key_owner, delete_gasless_key_record, delete_last_used_at,
    get_gasless_key_count, get_gasless_key_owner_fid, get_gasless_key_record,
    increment_gasless_key_count, init_last_used_at, put_gasless_key_owner, put_gasless_key_record,
    GaslessKeyRecord, OnchainEventStore,
};
use crate::core::error::HubError;
use crate::core::message::HubEventExt;
use crate::core::validations::error::ValidationError;
use crate::core::validations::key::{
    recover_key_add_custody_address, recover_key_remove_custody_address,
    verify_signed_key_request_metadata, KeyAddPayload, KeyRemovePayload, KeyRemoveSignatureType,
    ETH_MAINNET_CHAIN_ID, MAX_GASLESS_KEYS_PER_FID,
};
use crate::proto::{self, hub_event, message_data::Body, HubEvent, HubEventType};
use crate::storage::db::{RocksDB, RocksDbTransactionBatch};
use crate::storage::store::engine::MessageValidationError;

/// Orchestrates the full KEY_ADD flow for NEYN-10573: metadata verification against the
/// `requestFid`'s custody address, EIP-712 custody-signature recovery for the adding FID,
/// conflict resolution against both the gasless-key index and the on-chain signer index,
/// nonce CAS, record write, and `last_used_at` initialization.
///
/// Static body validation (key length, key_type, scopes, ttl bound, deadline presence) is
/// already handled upstream by `validations::message::validate_message` via
/// `key::validate_key_add_body` (NEYN-10571). This function assumes that ran and focuses on
/// the state-dependent work.
///
/// ## Ordering rationale
///
/// Steps are ordered so that cheap/pure checks fail first and expensive state writes happen
/// last — any early `Err` leaves the txn batch unchanged. The one exception is the nonce CAS,
/// which has to stage a write even on "success", but that stage is rolled back if any later
/// step fails (the whole txn batch is discarded together on error).
///
/// ## Resubmission (NEYN-10624)
///
/// A KEY_ADD for a key that this FID already owns in the gasless store is treated as a
/// scope/TTL update rather than a collision. The update is authorized only when the new
/// message's verified `requestFid` matches the stored `request_fid` on the existing record —
/// i.e. only the originating requesting fid can modify its own key's scopes or sliding-TTL
/// window. Mismatched owner FID surfaces as `KeyClaimedByDifferentFid` (cross-user take-over
/// attempt) and mismatched `request_fid` as `KeyRegisteredByDifferentRequestingFid` (cross-app
/// take-over attempt). The nonce CAS continues to enforce strict monotonicity, so a
/// resubmission must carry a higher nonce than any prior KEY_ADD / KEY_REMOVE on the user
/// namespace for this FID. On upsert, the previous KEY_ADD message rides along in the emitted
/// `MergeMessageBody.deleted_messages`, mirroring the shape of KEY_REMOVE events.
///
/// ## Per-FID gasless-key cap (NEYN-10579)
///
/// A KEY_ADD that would push the per-FID gasless count to or past `MAX_GASLESS_KEYS_PER_FID`
/// (1000) is rejected with `ActiveKeyCapExceeded`. The count is tracked by a per-FID counter
/// entry mutated alongside the primary record. On-chain signers are explicitly not counted —
/// they have their own cap at the L2 KeyRegistry contract and are tracked under a separate
/// storage namespace. The cap is not enforced on the resubmission (upsert) branch: replacing
/// an existing record is net-zero for the counter, so an FID already at the cap can still
/// modify a key it already owns.
///
/// Pending-FID resolution via `registration_tx_hash` is still out of scope — owned by
/// NEYN-10577.
pub fn merge_key_add(
    db: &Arc<RocksDB>,
    onchain_event_store: &OnchainEventStore,
    msg: &proto::Message,
    txn_batch: &mut RocksDbTransactionBatch,
) -> Result<HubEvent, MessageValidationError> {
    let message_data = msg
        .data
        .as_ref()
        .ok_or(MessageValidationError::NoMessageData)?;
    let fid = message_data.fid;
    let key_add_body = match &message_data.body {
        Some(Body::KeyAddBody(body)) => body,
        _ => {
            return Err(MessageValidationError::InvalidMessageType(
                message_data.r#type,
            ))
        }
    };

    // Use the message's own timestamp as the reference for deadline checks. This is
    // deterministic across nodes (replay-safe) and upstream `validate_message` has already
    // bounded `message.timestamp` to a reasonable window around current block time.
    let current_timestamp = message_data.timestamp as u64;
    let chain_id = ETH_MAINNET_CHAIN_ID;

    // Step 3 (SignedKeyRequest metadata validation): verify the ABI-encoded metadata blob,
    // then compare the recovered `requestSigner` against the custody address on file for
    // `requestFid`. The first is pure crypto; the second is a storage lookup and lives
    // here — keeping `core::validations` free of storage deps (see NEYN-10570 note).
    let verified = verify_signed_key_request_metadata(
        key_add_body.metadata_type,
        &key_add_body.metadata,
        &key_add_body.key,
        current_timestamp,
        chain_id,
    )?;

    let request_fid_event = onchain_event_store
        .get_id_register_event_by_fid(verified.request_fid, Some(txn_batch))
        .map_err(|_| MessageValidationError::MissingFid)?
        .ok_or(ValidationError::InvalidSignedKeyRequest)?;
    let request_fid_custody = match request_fid_event.body {
        Some(proto::on_chain_event::Body::IdRegisterEventBody(b)) => b,
        _ => return Err(ValidationError::InvalidSignedKeyRequest.into()),
    };
    if request_fid_custody.to.as_slice() != verified.request_signer.as_slice() {
        return Err(ValidationError::SignedKeyRequestCustodyMismatch.into());
    }

    // Step 5 (EIP-712 custody recovery for this FID's KeyAdd payload). The recovered
    // address must match the custody address currently on file for `fid`.
    let payload = KeyAddPayload {
        fid,
        key: &key_add_body.key,
        key_type: key_add_body.key_type,
        scopes: &key_add_body.scopes,
        ttl: key_add_body.ttl,
        nonce: key_add_body.nonce,
        deadline: key_add_body.deadline,
    };
    let recovered =
        recover_key_add_custody_address(&payload, &key_add_body.custody_signature, chain_id)?;
    let this_fid_event = onchain_event_store
        .get_id_register_event_by_fid(fid, Some(txn_batch))
        .map_err(|_| MessageValidationError::MissingFid)?
        .ok_or(MessageValidationError::MissingFid)?;
    let this_fid_custody = match this_fid_event.body {
        Some(proto::on_chain_event::Body::IdRegisterEventBody(b)) => b,
        _ => return Err(MessageValidationError::MissingFid),
    };
    if this_fid_custody.to.as_slice() != recovered.as_slice() {
        return Err(ValidationError::InvalidSignature.into());
    }

    // Step 8 (conflict resolution). On-chain first so a signer that was minted via L2 doesn't
    // silently get shadowed by a gasless registration; this branch also short-circuits before
    // the gasless reads below on that path. The same-FID on-chain collision stays a reject —
    // resubmission applies only to the gasless store (NEYN-10624).
    let existing_onchain = onchain_event_store
        .get_active_signer(fid, key_add_body.key.clone(), Some(txn_batch))
        .map_err(|_| MessageValidationError::MissingSigner)?;
    if existing_onchain.is_some() {
        return Err(ValidationError::KeyAlreadyRegisteredOnchain.into());
    }

    // Gasless by-public-key index is authoritative for "which FID (if any) currently claims
    // this key as a gasless signer?" — written and deleted in lock-step with the primary
    // by-FID record. Three outcomes:
    //   * None                    → first-time add for this FID; proceed.
    //   * Some(other_fid) ≠ fid   → cross-user collision; reject with
    //                               `KeyClaimedByDifferentFid`.
    //   * Some(fid)               → same-FID resubmission; authorize as an upsert iff the
    //                               new message's verified `requestFid` matches the stored
    //                               `request_fid` on the prior record (NEYN-10624).
    //                               Mismatch rejects with
    //                               `KeyRegisteredByDifferentRequestingFid`.
    //
    // A `Some(fid)` with no primary record is an internal inconsistency (owner index and
    // by-fid record are supposed to move in lockstep) — surfaced as an `internal_error`
    // rather than a user-facing rejection, so it's loud in logs.
    //
    // On the upsert branch the prior `GaslessKeyRecord` is carried forward so its embedded
    // KEY_ADD message can ride in the emitted event's `deleted_messages`.
    let prior_record = match get_gasless_key_owner_fid(db, txn_batch, &key_add_body.key)? {
        None => None,
        Some(owner_fid) if owner_fid != fid => {
            return Err(ValidationError::KeyClaimedByDifferentFid.into());
        }
        Some(_) => {
            let record = get_gasless_key_record(db, txn_batch, fid, &key_add_body.key)?
                .ok_or_else(|| HubError {
                    code: "internal_error".to_string(),
                    message: format!(
                        "gasless owner index has fid={} for key but primary record missing",
                        fid
                    ),
                })?;
            if record.request_fid != verified.request_fid {
                return Err(ValidationError::KeyRegisteredByDifferentRequestingFid.into());
            }
            Some(record)
        }
    };

    // Per-FID gasless-key cap (NEYN-10579). Skipped entirely on the upsert path — replacing an
    // existing record doesn't grow the counter, so the cap check would be a false reject for
    // an FID already at 1000 that's legitimately modifying scope/TTL on a key it already owns.
    // For first-time adds: read through the txn batch so staged same-batch increments are
    // visible — two KEY_ADDs in one commit see each other's counter bumps and the cap is
    // enforced precisely at 1000.
    if prior_record.is_none() {
        let gasless_count = get_gasless_key_count(db, txn_batch, fid)?;
        if gasless_count >= MAX_GASLESS_KEYS_PER_FID {
            return Err(ValidationError::ActiveKeyCapExceeded(MAX_GASLESS_KEYS_PER_FID).into());
        }
    }

    // Step 4 (nonce CAS). Stages the bump on txn_batch; rolls back with everything else
    // if any later step fails. The store rejects `new_nonce <= stored`, which implicitly
    // covers `nonce == 0` (stored defaults to 0). This also makes resubmission safe against
    // replay: a second KEY_ADD carrying the same or lower nonce than the first is rejected
    // here, so the upsert path can't be driven by a replayed original message.
    check_and_set_user_nonce(db, txn_batch, fid, key_add_body.nonce)?;

    // State writes. Record embeds the full message (source of truth) plus the cached
    // request_fid. `put_gasless_key_record` overwrites unconditionally, so an upsert simply
    // replaces the prior embedded message with the new one — scopes/TTL/etc. on that message
    // are re-read on every scope evaluation (see `active_key.rs`). The owner-index write is
    // a no-op re-put on resubmission (same fid → same value) but issued unconditionally for
    // symmetry with the first-time path. `last_used_at` is reset to the new message's
    // timestamp: the sliding-TTL window restarts on each accepted KEY_ADD.
    let record = GaslessKeyRecord {
        message: Some(msg.clone()),
        request_fid: verified.request_fid,
    };
    put_gasless_key_record(db, txn_batch, fid, &key_add_body.key, &record)?;
    put_gasless_key_owner(db, txn_batch, &key_add_body.key, fid)?;
    init_last_used_at(
        db,
        txn_batch,
        fid,
        &key_add_body.key,
        message_data.timestamp,
    )?;
    // Increment the per-FID gasless-key counter in lock-step with the record write, but only
    // on the first-time-add branch. On upsert (resubmission) the net count is unchanged — we're
    // replacing a record, not adding one — and bumping the counter would cause
    // `decrement_gasless_key_count` on a future KEY_REMOVE to underflow or run short.
    let is_first_time_add = prior_record.is_none();
    if is_first_time_add {
        increment_gasless_key_count(db, txn_batch, fid)?;
    }

    let deleted_messages: Vec<proto::Message> =
        prior_record.and_then(|r| r.message).into_iter().collect();

    Ok(HubEvent::new_event(
        HubEventType::MergeMessage,
        hub_event::Body::MergeMessageBody(proto::MergeMessageBody {
            message: Some(msg.clone()),
            deleted_messages,
        }),
    ))
}

/// Orchestrates the KEY_REMOVE flow for NEYN-10574: deactivates a gasless key under one of two
/// authorization modes, then clears the sibling `last_used_at` entry.
///
/// ## Why we look up the record first
///
/// The `GaslessKeyRecord` lookup serves three purposes in one read:
///   1. Enforces "key is currently active for this FID" — absent record => `KeyNotRegistered`.
///      This is also the spam-protection rail for self-revocation: an attacker can't consume
///      app-nonce counters by flooding KEY_REMOVE messages against keys that don't exist.
///   2. Provides the cached `request_fid` used by the self-revocation nonce scope. Recomputing
///      it would require an ABI-decode + EIP-712 recover on every self-revoke (~10µs) — the
///      whole point of caching it in the record is to avoid that on hot paths.
///   3. Supplies the prior KEY_ADD message that rides along in the emitted event's
///      `deleted_messages`, matching the convention used by other remove-type merges
///      (see `store.rs:229`).
///
/// ## Authorization modes
///
/// * `signature_type == 1` (custody) — inner `body.signature` is an EIP-712 signature over the
///   `KeyRemove` typed data; the recovered address must match `fid`'s on-file custody
///   address. Replay protection advances the **user** nonce namespace for `fid`.
/// * `signature_type == 2` (self) — the outer `message.signer` is the Ed25519 key being
///   revoked, and the envelope signature (already verified upstream by `validate_message`)
///   proves the holder authorized this removal. We just assert `message.signer == body.key`;
///   no additional crypto needed here. Replay protection advances the **app** nonce namespace
///   for the verified `request_fid` recovered at KEY_ADD time.
///
/// The inner `body.signature` field is unused for self-revocation — the envelope already
/// carries an equivalent signature. Keeping the field on the wire preserves a uniform shape
/// across both modes for client libraries, at no cost here.
///
/// On-chain signers are intentionally out of scope — they are removed by on-chain
/// `SIGNER_EVENT_TYPE_REMOVE` events via the Key Registry contract, not by this path.
pub fn merge_key_remove(
    db: &Arc<RocksDB>,
    onchain_event_store: &OnchainEventStore,
    msg: &proto::Message,
    txn_batch: &mut RocksDbTransactionBatch,
) -> Result<HubEvent, MessageValidationError> {
    let message_data = msg
        .data
        .as_ref()
        .ok_or(MessageValidationError::NoMessageData)?;
    let fid = message_data.fid;
    let key_remove_body = match &message_data.body {
        Some(Body::KeyRemoveBody(body)) => body,
        _ => {
            return Err(MessageValidationError::InvalidMessageType(
                message_data.r#type,
            ))
        }
    };
    let chain_id = ETH_MAINNET_CHAIN_ID;

    // Active-key lookup. Does triple duty: existence check, request_fid source for
    // self-revocation, and prior-message source for the emitted event's deleted_messages.
    let record = get_gasless_key_record(db, txn_batch, fid, &key_remove_body.key)?
        .ok_or(ValidationError::KeyNotRegistered)?;

    // `?` here is belt-and-suspenders: `validate_user_message` already ran
    // `validate_key_remove_body`, which rejects unknown discriminants via the same TryFrom. If
    // that upstream contract is ever bypassed (e.g. a future internal injection path), this
    // re-check keeps the function safe in isolation. Exhaustive match below means adding a
    // future variant forces us to update this site — the constant-based match couldn't.
    let sig_type = KeyRemoveSignatureType::try_from(key_remove_body.signature_type)?;
    match sig_type {
        KeyRemoveSignatureType::Custody => {
            let payload = KeyRemovePayload {
                fid,
                key: &key_remove_body.key,
                nonce: key_remove_body.nonce,
                deadline: key_remove_body.deadline,
            };
            let recovered =
                recover_key_remove_custody_address(&payload, &key_remove_body.signature, chain_id)?;
            let this_fid_event = onchain_event_store
                .get_id_register_event_by_fid(fid, Some(txn_batch))
                .map_err(|_| MessageValidationError::MissingFid)?
                .ok_or(MessageValidationError::MissingFid)?;
            let this_fid_custody = match this_fid_event.body {
                Some(proto::on_chain_event::Body::IdRegisterEventBody(b)) => b,
                _ => return Err(MessageValidationError::MissingFid),
            };
            if this_fid_custody.to.as_slice() != recovered.as_slice() {
                return Err(ValidationError::InvalidSignature.into());
            }
            check_and_set_user_nonce(db, txn_batch, fid, key_remove_body.nonce)?;
        }
        KeyRemoveSignatureType::SelfRevoke => {
            // Self-revocation. The envelope signer is the key being revoked, and the envelope
            // Ed25519 signature was already verified upstream. Asserting signer == body.key
            // is all that's left — any other signer means this message is not authorized by
            // the key's holder.
            if msg.signer.as_slice() != key_remove_body.key.as_slice() {
                return Err(ValidationError::InvalidSignature.into());
            }
            check_and_set_app_nonce(db, txn_batch, record.request_fid, key_remove_body.nonce)?;
        }
    }

    // State writes. Order: record first (the authoritative existence signal), then the
    // gasless by-public-key owner entry (releases the gasless-namespace uniqueness claim),
    // then last_used_at (cleanup of the sibling store). Clearing record first matches the
    // existing read-order convention; clearing the owner entry releases the gasless claim
    // so a subsequent gasless KEY_ADD from any FID can succeed.
    delete_gasless_key_record(db, txn_batch, fid, &key_remove_body.key)?;
    delete_gasless_key_owner(db, txn_batch, &key_remove_body.key)?;
    delete_last_used_at(db, txn_batch, fid, &key_remove_body.key)?;
    // Decrement the per-FID gasless-key counter. The record existence check at the top of this
    // function guarantees this decrement corresponds to a real prior KEY_ADD; the counter stays
    // in sync with the number of distinct records on disk.
    decrement_gasless_key_count(db, txn_batch, fid)?;

    Ok(HubEvent::new_event(
        HubEventType::MergeMessage,
        hub_event::Body::MergeMessageBody(proto::MergeMessageBody {
            message: Some(msg.clone()),
            deleted_messages: record.message.into_iter().collect(),
        }),
    ))
}
