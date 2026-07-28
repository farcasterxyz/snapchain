use super::{get_from_db_or_txn, make_fid_key, StoreEventHandler, TRUE_VALUE};
use crate::core::error::HubError;
use crate::core::message::HubEventExt;
use crate::core::util::FarcasterTime;
use crate::proto::{
    self, on_chain_event, on_chain_event::Body, ChannelOwnerChangeCause, ChannelRegisterBody,
    ChannelRegisterEventType, FarcasterNetwork, HubEvent, HubEventType, IdRegisterEventBody,
    IdRegisterEventType, MergeOnChainEventBody, OnChainEvent, OnChainEventType, SignerEventBody,
    SignerEventType, TierType,
};
use crate::proto::{LendStorageBody, StorageUnitType};
use crate::storage::constants::{OnChainEventPostfix, RootPrefix, PAGE_SIZE_MAX};
use crate::storage::db::{PageOptions, RocksDB, RocksDbTransactionBatch, RocksdbError};
use crate::storage::store::account::StoreOptions;
use crate::storage::util::increment_vec_u8;
use crate::version::version::{EngineVersion, ProtocolFeature};
use alloy_primitives::keccak256;
use prost::{DecodeError, Message};
use std::collections::VecDeque;
use std::sync::Arc;
use thiserror::Error;
use tracing::warn;

static PAGE_SIZE: usize = 1000;
const EVM_ADDRESS_LENGTH: usize = 20;
const CHANNEL_LABEL_LENGTH: usize = 32;

pub const UNIT_TYPE_LEGACY_CUTOFF_TIMESTAMP: u32 = 1724889600; // 2024-08-29 Midnight UTC
const UNIT_TYPE_2024_CUTOFF_TIMESTAMP: u32 = 1752685200; // 2025-07-16 5PM UTC (Engine version 6)
const UNIT_TYPE_2024_CUTOFF_TIMESTAMP_TESTNET: u32 = 1752426000; // 2025-07-13 5PM UTC (a few days earlier than mainnet)

// Boundary between the existing 2025 cohort (extended +1 year by the 2026 expiry extension) and
// newly-rented units that keep the standard 1-year validity. Must equal the EngineVersion::V18
// activation timestamp for each network (see StorageExpiryExtension2026 in version.rs).
const UNIT_TYPE_2025_CUTOFF_TIMESTAMP: u32 = 1782147600; // 2026-06-22 5PM UTC (Engine version 18)
const UNIT_TYPE_2025_CUTOFF_TIMESTAMP_TESTNET: u32 = 1781283600; // 2026-06-12 5PM UTC (a few days earlier than mainnet)
const ONE_YEAR_IN_SECONDS: u32 = 365 * 24 * 60 * 60;
const SUPPORTED_SIGNER_KEY_TYPE: u32 = 1;

#[derive(Error, Debug)]
pub enum OnchainEventStorageError {
    #[error(transparent)]
    RocksdbError(#[from] RocksdbError),

    #[error(transparent)]
    HubError(#[from] HubError),

    #[error("Invalid event type calculating storage slots ")]
    InvalidStorageRentEventType,

    #[error(transparent)]
    DecodeError(#[from] DecodeError),

    #[error("Unexpected event type")]
    UnexpectedEventType,

    #[error("Duplicate onchain event")]
    DuplicateOnchainEvent,
}

/** A page of messages returned from various APIs */
pub struct OnchainEventsPage {
    pub onchain_events: Vec<OnChainEvent>,
    pub next_page_token: Option<Vec<u8>>,
}

/// Storage-internal materialized channel-ownership record, persisted under
/// `ChannelRegisterByChannelKey`. Hand-rolled rather than defined in
/// `proto/definitions/` because it is a private on-disk value, not a wire/API type —
/// mirroring the `GaslessKeyRecord` pattern in `key_add_store.rs`. The prost field
/// tags below are therefore a stable on-disk contract: do not reorder or reuse them.
/// `owner_address` is the raw 20-byte EVM address. No fid is stored: this record is a
/// pure fold over channel events, and resolving an owner address to an fid is left to
/// the read/query layer, deliberately not done here. `channel_key` is a denormalized
/// copy of the key this record is stored under and must never be read as authoritative.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ChannelOwner {
    #[prost(string, tag = "1")]
    pub channel_key: String,
    #[prost(bytes = "vec", tag = "2")]
    pub owner_address: Vec<u8>,
    #[prost(uint64, tag = "3")]
    pub expiry: u64,
    #[prost(uint32, tag = "4")]
    pub block_number: u32,
    #[prost(uint32, tag = "5")]
    pub tx_index: u32,
    #[prost(uint32, tag = "6")]
    pub log_index: u32,
}

fn make_block_number_key(block_number: u32) -> Vec<u8> {
    block_number.to_be_bytes().to_vec()
}

fn make_log_index_key(log_index: u32) -> Vec<u8> {
    log_index.to_be_bytes().to_vec()
}

fn make_onchain_event_type_prefix(onchain_event_type: OnChainEventType) -> Vec<u8> {
    vec![
        RootPrefix::OnChainEvent as u8,
        OnChainEventPostfix::OnChainEvents as u8,
        onchain_event_type as u8,
    ]
}

fn make_onchain_event_primary_key(onchain_event: &OnChainEvent) -> Vec<u8> {
    let mut primary_key = make_onchain_event_type_prefix(onchain_event.r#type());
    primary_key.extend(make_fid_key(onchain_event.fid));
    primary_key.extend(make_block_number_key(onchain_event.block_number));
    primary_key.extend(make_log_index_key(onchain_event.log_index));

    primary_key
}

pub fn merge_onchain_event(
    db: &RocksDB,
    txn: &mut RocksDbTransactionBatch,
    onchain_event: OnChainEvent,
    store_opts: &StoreOptions,
) -> Result<(), OnchainEventStorageError> {
    let primary_key = make_onchain_event_primary_key(&onchain_event);
    if !store_opts.conflict_free {
        if let Some(_) = get_from_db_or_txn(db, txn, &primary_key)? {
            return Err(OnchainEventStorageError::DuplicateOnchainEvent);
        }
    }
    txn.put(primary_key, onchain_event.encode_to_vec());
    build_secondary_indices(db, txn, &onchain_event)?;
    Ok(())
}

pub fn signer_body(onchain_event: OnChainEvent) -> Option<SignerEventBody> {
    if let on_chain_event::Body::SignerEventBody(body) = onchain_event.body? {
        Some(body)
    } else {
        None
    }
}

fn make_id_register_by_fid_key(fid: u64) -> Vec<u8> {
    let mut id_register_by_fid_key = vec![
        RootPrefix::OnChainEvent as u8,
        OnChainEventPostfix::IdRegisterByFid as u8,
    ];
    id_register_by_fid_key.extend(make_fid_key(fid));
    id_register_by_fid_key
}

fn make_signer_onchain_event_by_signer_key(fid: u64, key: Vec<u8>) -> Vec<u8> {
    let mut signer_key = vec![
        RootPrefix::OnChainEvent as u8,
        OnChainEventPostfix::SignerByFid as u8,
    ];
    signer_key.extend(make_fid_key(fid));
    signer_key.extend(key);
    signer_key
}

fn make_channel_register_by_channel_key(channel_key: &str) -> Vec<u8> {
    let mut key = vec![
        RootPrefix::OnChainEvent as u8,
        OnChainEventPostfix::ChannelRegisterByChannelKey as u8,
    ];
    key.extend(channel_key.as_bytes());
    key
}

fn make_channel_register_channel_key_by_label_key(label: &[u8]) -> Vec<u8> {
    let mut key = vec![
        RootPrefix::OnChainEvent as u8,
        OnChainEventPostfix::ChannelRegisterChannelKeyByLabel as u8,
    ];
    key.extend(label);
    key
}

fn make_channel_register_by_owner_address_prefix(owner_address: &[u8]) -> Vec<u8> {
    let mut key = vec![
        RootPrefix::OnChainEvent as u8,
        OnChainEventPostfix::ChannelRegisterByOwnerAddress as u8,
    ];
    key.extend(owner_address);
    key
}

fn make_channel_register_by_owner_address_key(owner_address: &[u8], channel_key: &str) -> Vec<u8> {
    let mut key = make_channel_register_by_owner_address_prefix(owner_address);
    key.extend(channel_key.as_bytes());
    key
}

fn incoming_channel_event_is_older(owner: &ChannelOwner, event: &OnChainEvent) -> bool {
    (event.block_number, event.tx_index, event.log_index)
        < (owner.block_number, owner.tx_index, owner.log_index)
}

fn channel_owner_from_event(
    onchain_event: &OnChainEvent,
    body: &ChannelRegisterBody,
) -> ChannelOwner {
    ChannelOwner {
        channel_key: body.channel_key.clone(),
        owner_address: body.owner_address.clone(),
        expiry: body.expiry,
        block_number: onchain_event.block_number,
        tx_index: onchain_event.tx_index,
        log_index: onchain_event.log_index,
    }
}

/// Stamps the event's chain position onto an existing record. This triple is the
/// ordering watermark compared by `incoming_channel_event_is_older`, so RENEW and
/// TRANSFER must refresh it whenever they win.
fn set_channel_owner_event_position(owner: &mut ChannelOwner, onchain_event: &OnChainEvent) {
    owner.block_number = onchain_event.block_number;
    owner.tx_index = onchain_event.tx_index;
    owner.log_index = onchain_event.log_index;
}

// Routes through the shared `put_/delete_channel_key_by_owner_address` helpers,
// which validate the 20-byte EVM address. On the merge path this validation is
// unreachable: callers gate the new owner on `validate_channel_owner_address`
// first (Register/Transfer, early-return on failure), and the old owner is read
// back from a `ChannelOwner` record that was itself written only through that
// same gate. So the `?` never fires on a validly-constructed DB, keeping this
// behavior-identical to the pre-consolidation inline writes.
fn move_channel_owner_address_index(
    txn: &mut RocksDbTransactionBatch,
    channel_key: &str,
    old_owner_address: Option<&[u8]>,
    new_owner_address: &[u8],
) -> Result<(), OnchainEventStorageError> {
    if let Some(old_owner_address) = old_owner_address {
        if old_owner_address != new_owner_address {
            delete_channel_key_by_owner_address(txn, old_owner_address, channel_key)?;
        }
    }

    put_channel_key_by_owner_address(txn, new_owner_address, channel_key)?;
    Ok(())
}

fn validate_channel_label(label: &[u8]) -> bool {
    if label.len() != CHANNEL_LABEL_LENGTH {
        warn!(
            "Skipping channel register index update with invalid label length {}",
            label.len()
        );
        return false;
    }
    true
}

fn validate_channel_owner_address(owner_address: &[u8]) -> bool {
    // Delegates to the same length check the public helpers use so the
    // "owner address is a 20-byte EVM address" invariant lives in one place;
    // on the merge path a bad address is skipped with a warn rather than erroring.
    if let Err(err) = validate_evm_address(owner_address) {
        warn!("Skipping channel register index update: {}", err);
        return false;
    }
    true
}

fn validate_channel_key_label(channel_key: &str, label: &[u8]) -> bool {
    if keccak256(channel_key.as_bytes()).as_slice() != label {
        warn!(
            "Skipping channel register index update: keccak256(channel_key {}) != label 0x{}",
            channel_key,
            hex::encode(label)
        );
        return false;
    }
    true
}

fn read_channel_owner_by_channel_key(
    db: &RocksDB,
    txn: &RocksDbTransactionBatch,
    channel_key: &str,
) -> Result<Option<ChannelOwner>, OnchainEventStorageError> {
    match get_from_db_or_txn(db, txn, &make_channel_register_by_channel_key(channel_key))? {
        Some(bytes) => Ok(Some(ChannelOwner::decode(bytes.as_slice())?)),
        None => Ok(None),
    }
}

/// The ownership change a channel-register fold actually recorded. Produced only
/// when a REGISTER or TRANSFER wrote a new owner; consumed by the data-shard
/// replica arm to emit a `ChannelOwnerChangeHint`. `owner_address` is the final
/// recorded owner (so a hint never carries an address the fold skipped or lost to
/// LWW), and `cause` is deliberately limited to REGISTER/TRANSFER — RENEW extends
/// expiry without changing ownership, so it produces no change (the proto
/// `ChannelOwnerChangeCause` enum has no RENEW variant by design).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ChannelOwnerChange {
    pub channel_key: String,
    pub owner_address: Vec<u8>,
    pub cause: ChannelOwnerChangeCause,
}

fn build_secondary_indices_for_channel_register(
    db: &RocksDB,
    txn: &mut RocksDbTransactionBatch,
    onchain_event: &OnChainEvent,
    channel_register_body: &ChannelRegisterBody,
) -> Result<Option<ChannelOwnerChange>, OnchainEventStorageError> {
    if !validate_channel_label(&channel_register_body.label) {
        return Ok(None);
    }

    // Each arm writes exactly as before; the only added behavior is reporting which
    // writes were an ownership change. REGISTER/TRANSFER that actually wrote →
    // `Some`; every skip (validation, LWW-older, unknown label/key), RENEW, and the
    // unrecognized-type arm → `None`.
    let change = match channel_register_body.event_type() {
        ChannelRegisterEventType::Register => {
            if !validate_channel_owner_address(&channel_register_body.owner_address)
                || !validate_channel_key_label(
                    &channel_register_body.channel_key,
                    &channel_register_body.label,
                )
            {
                return Ok(None);
            }

            let by_channel_key =
                make_channel_register_by_channel_key(&channel_register_body.channel_key);

            let existing_owner =
                read_channel_owner_by_channel_key(db, txn, &channel_register_body.channel_key)?;
            if let Some(existing_owner) = &existing_owner {
                if incoming_channel_event_is_older(existing_owner, onchain_event) {
                    return Ok(None);
                }
            }

            let channel_owner = channel_owner_from_event(onchain_event, channel_register_body);
            txn.put(
                make_channel_register_channel_key_by_label_key(&channel_register_body.label),
                channel_register_body.channel_key.as_bytes().to_vec(),
            );
            move_channel_owner_address_index(
                txn,
                &channel_register_body.channel_key,
                existing_owner
                    .as_ref()
                    .map(|owner| owner.owner_address.as_slice()),
                &channel_owner.owner_address,
            )?;
            txn.put(by_channel_key, channel_owner.encode_to_vec());
            Some(ChannelOwnerChange {
                channel_key: channel_register_body.channel_key.clone(),
                owner_address: channel_owner.owner_address,
                cause: ChannelOwnerChangeCause::Register,
            })
        }
        ChannelRegisterEventType::Renew => {
            if !validate_channel_key_label(
                &channel_register_body.channel_key,
                &channel_register_body.label,
            ) {
                return Ok(None);
            }

            let Some(mut channel_owner) =
                read_channel_owner_by_channel_key(db, txn, &channel_register_body.channel_key)?
            else {
                warn!(
                    "Skipping channel renew for unknown channel key {}",
                    channel_register_body.channel_key
                );
                return Ok(None);
            };
            if incoming_channel_event_is_older(&channel_owner, onchain_event) {
                return Ok(None);
            }
            channel_owner.expiry = channel_register_body.expiry;
            set_channel_owner_event_position(&mut channel_owner, onchain_event);
            txn.put(
                make_channel_register_by_channel_key(&channel_register_body.channel_key),
                channel_owner.encode_to_vec(),
            );
            // Renew applies its write but is not an ownership change: an expiry
            // extension keeps the same owner, so no hint fires.
            None
        }
        ChannelRegisterEventType::Transfer => {
            if !validate_channel_owner_address(&channel_register_body.owner_address) {
                return Ok(None);
            }

            let Some(channel_key) = get_from_db_or_txn(
                db,
                txn,
                &make_channel_register_channel_key_by_label_key(&channel_register_body.label),
            )?
            else {
                warn!(
                    "Skipping channel transfer for unknown label 0x{}",
                    hex::encode(&channel_register_body.label)
                );
                return Ok(None);
            };
            let channel_key =
                String::from_utf8(channel_key).map_err(|err| DecodeError::new(err.to_string()))?;
            let Some(mut channel_owner) = read_channel_owner_by_channel_key(db, txn, &channel_key)?
            else {
                warn!(
                    "Skipping channel transfer for unknown channel key {}",
                    channel_key
                );
                return Ok(None);
            };
            if incoming_channel_event_is_older(&channel_owner, onchain_event) {
                return Ok(None);
            }
            let old_owner_address = channel_owner.owner_address.clone();
            channel_owner.owner_address = channel_register_body.owner_address.clone();
            set_channel_owner_event_position(&mut channel_owner, onchain_event);
            move_channel_owner_address_index(
                txn,
                &channel_key,
                Some(&old_owner_address),
                &channel_owner.owner_address,
            )?;
            txn.put(
                make_channel_register_by_channel_key(&channel_key),
                channel_owner.encode_to_vec(),
            );
            Some(ChannelOwnerChange {
                channel_key,
                owner_address: channel_owner.owner_address,
                cause: ChannelOwnerChangeCause::Transfer,
            })
        }
        ChannelRegisterEventType::None => {
            // `event_type()` maps any unrecognized i32 (including 0, or a future on-chain
            // channel event type this binary predates) to `None`. The raw event is still
            // persisted, but it builds no index; warn so the gap is diagnosable rather
            // than silent.
            warn!(
                "Skipping channel register index update for unrecognized event_type {} (channel_key {})",
                channel_register_body.event_type, channel_register_body.channel_key
            );
            None
        }
    };

    Ok(change)
}

/// Data-shard replica entry point: runs ONLY the channel-register
/// secondary-index fold against `db`/`txn` — it does not write the primary
/// onchain-event record and does not touch the trie (those live in the shard-0
/// merge path). Non-channel events fold to `None`. Returns the ownership change a
/// REGISTER/TRANSFER recorded so the caller can emit a `ChannelOwnerChangeHint`.
pub(crate) fn fold_channel_register_replica(
    db: &RocksDB,
    txn: &mut RocksDbTransactionBatch,
    onchain_event: &OnChainEvent,
) -> Result<Option<ChannelOwnerChange>, OnchainEventStorageError> {
    match &onchain_event.body {
        Some(on_chain_event::Body::ChannelRegisterEventBody(channel_register_body)) => {
            build_secondary_indices_for_channel_register(
                db,
                txn,
                onchain_event,
                channel_register_body,
            )
        }
        _ => Ok(None),
    }
}

fn build_secondary_indices_for_id_register(
    db: &RocksDB,
    txn: &mut RocksDbTransactionBatch,
    onchain_event: &OnChainEvent,
    id_register_event_body: &IdRegisterEventBody,
) -> Result<(), OnchainEventStorageError> {
    if id_register_event_body.event_type() == IdRegisterEventType::ChangeRecovery {
        // change recovery events are not indexed (id and custody address are the same)
        return Ok(());
    }
    let id_register_by_fid_key = make_id_register_by_fid_key(onchain_event.fid);
    match get_event_by_secondary_key(db, id_register_by_fid_key.clone(), Some(txn))? {
        Some(existing_event) => {
            if existing_event.block_number > onchain_event.block_number {
                return Ok(());
            }
        }
        None => {}
    };
    let primary_key = make_onchain_event_primary_key(&onchain_event);
    txn.put(id_register_by_fid_key, primary_key);
    Ok(())
}

fn build_secondary_indices_for_signer(
    db: &RocksDB,
    txn: &mut RocksDbTransactionBatch,
    onchain_event: &OnChainEvent,
    signer_event_body: &SignerEventBody,
) -> Result<(), OnchainEventStorageError> {
    let signer_key =
        make_signer_onchain_event_by_signer_key(onchain_event.fid, signer_event_body.key.clone());
    match get_event_by_secondary_key(db, signer_key.clone(), Some(txn))? {
        Some(existing_event) => {
            if existing_event.block_number > onchain_event.block_number {
                return Ok(());
            }
            let existing_event_body = signer_body(onchain_event.clone())
                .ok_or(OnchainEventStorageError::UnexpectedEventType)?;
            if existing_event_body.event_type() == SignerEventType::Remove
                && signer_event_body.event_type() == SignerEventType::Add
            {
                return Ok(());
            }
        }
        None => {}
    };

    if signer_event_body.event_type() == SignerEventType::AdminReset {
        let mut next_page_token = None;
        let mut start_prefix = make_onchain_event_type_prefix(OnChainEventType::EventTypeSigner);
        start_prefix.extend(make_fid_key(onchain_event.fid));
        let stop_prefix = increment_vec_u8(&start_prefix);

        loop {
            let events_page = get_onchain_events(
                db,
                &PageOptions {
                    page_size: None,
                    page_token: next_page_token,
                    reverse: false,
                },
                start_prefix.clone(),
                stop_prefix.clone(),
            )?;

            let onchain_event = events_page.onchain_events.into_iter().find(|event| {
                match signer_body(event.clone()) {
                    None => false,
                    Some(body) => {
                        if body.event_type() == SignerEventType::Add
                            && body.key == signer_event_body.key
                        {
                            true
                        } else {
                            false
                        }
                    }
                }
            });
            if let Some(onchain_event) = onchain_event {
                txn.put(
                    signer_key.clone(),
                    make_onchain_event_primary_key(&onchain_event),
                );
                break;
            }

            next_page_token = events_page.next_page_token;
            if next_page_token.is_none() {
                break;
            }
        }
        return Ok(());
    }

    txn.put(signer_key, make_onchain_event_primary_key(onchain_event));
    Ok(())
}

fn build_secondary_indices(
    db: &RocksDB,
    txn: &mut RocksDbTransactionBatch,
    onchain_event: &OnChainEvent,
) -> Result<(), OnchainEventStorageError> {
    if let Some(body) = &onchain_event.body {
        match body {
            Body::IdRegisterEventBody(id_register_event_body) => {
                build_secondary_indices_for_id_register(
                    db,
                    txn,
                    onchain_event,
                    id_register_event_body,
                )?
            }
            on_chain_event::Body::SignerEventBody(signer_event_body) => {
                build_secondary_indices_for_signer(db, txn, onchain_event, signer_event_body)?
            }
            on_chain_event::Body::ChannelRegisterEventBody(channel_register_body) => {
                // The primary merge path builds the index but ignores the ownership-change
                // outcome; hint emission happens only on the data-shard replica path
                // (`fold_channel_register_replica`).
                build_secondary_indices_for_channel_register(
                    db,
                    txn,
                    onchain_event,
                    channel_register_body,
                )?;
            }
            on_chain_event::Body::SignerMigratedEventBody(_)
            | on_chain_event::Body::StorageRentEventBody(_)
            | on_chain_event::Body::TierPurchaseEventBody(_) => {}
        }
    };

    Ok(())
}

// Read/write surface for the by-owner-address index. The production merge path
// goes through these helpers so validation, key layout, and values stay
// centralized.
pub fn put_channel_key_by_owner_address(
    txn: &mut RocksDbTransactionBatch,
    owner_address: &[u8],
    channel_key: &str,
) -> Result<(), OnchainEventStorageError> {
    validate_evm_address(owner_address)?;
    txn.put(
        make_channel_register_by_owner_address_key(owner_address, channel_key),
        vec![TRUE_VALUE],
    );
    Ok(())
}

pub fn delete_channel_key_by_owner_address(
    txn: &mut RocksDbTransactionBatch,
    owner_address: &[u8],
    channel_key: &str,
) -> Result<(), OnchainEventStorageError> {
    validate_evm_address(owner_address)?;
    txn.delete(make_channel_register_by_owner_address_key(
        owner_address,
        channel_key,
    ));
    Ok(())
}

pub fn get_channel_keys_by_owner_address(
    db: &RocksDB,
    owner_address: &[u8],
    page_options: &PageOptions,
) -> Result<(Vec<String>, Option<Vec<u8>>), OnchainEventStorageError> {
    validate_evm_address(owner_address)?;
    let start_prefix = make_channel_register_by_owner_address_prefix(owner_address);
    let stop_prefix = increment_vec_u8(&start_prefix);
    let channel_key_offset = start_prefix.len();
    let mut channel_keys = vec![];
    let mut last_key = vec![];

    db.for_each_iterator_by_prefix_paged(
        Some(start_prefix),
        Some(stop_prefix),
        page_options,
        |key, _value| {
            let channel_key = String::from_utf8(key[channel_key_offset..].to_vec())
                .map_err(|e| HubError::from(DecodeError::new(e.to_string())))?;
            channel_keys.push(channel_key);

            if channel_keys.len() >= page_options.page_size.unwrap_or(PAGE_SIZE_MAX) {
                last_key = key.to_vec();
                return Ok(true);
            }

            Ok(false)
        },
    )
    .map_err(OnchainEventStorageError::HubError)?;

    let next_page_token = if last_key.is_empty() {
        None
    } else {
        Some(last_key)
    };

    Ok((channel_keys, next_page_token))
}

/// Composite paged scan across a sorted, ascending, deduped list of owner
/// addresses. The by-owner-address index key (`address ++ channel_key`) is
/// treated as one globally ordered cursor over the whole sequence, so this pages
/// across an address boundary transparently. Returns up to `page_size`
/// `(owner_address, channel_key)` pairs and a `next_page_token` equal to the last
/// index key scanned whenever the page filled. A `None` token means the sequence
/// was fully enumerated; a present token means "call again" but does not
/// guarantee more results — a page that fills exactly on the last entry yields one
/// final empty page. This matches `get_channel_keys_by_owner_address`, whose
/// single-address token has the same boundary behavior.
///
/// Winner resolution is the caller's job: `owner_addresses` must already be
/// sorted ascending and deduped. A stored `page_token` is routed to a single
/// address — addresses fully below it are skipped, the token's own address
/// resumes strictly after it, and addresses above it start fresh — so a token
/// minted from one address can never mis-scan another.
///
/// There is no snapshot isolation across pages: `owner_addresses` is re-derived
/// per request, so if the set changes between pages the cursor follows standard
/// paging semantics — an address removed after the token was minted stops
/// contributing (its earlier entries were already returned), and an address
/// added below the token is not revisited until a fresh enumeration.
pub fn get_channel_keys_for_owner_addresses(
    db: &RocksDB,
    owner_addresses: &[Vec<u8>],
    page_token: Option<&[u8]>,
    page_size: usize,
) -> Result<(Vec<(Vec<u8>, String)>, Option<Vec<u8>>), OnchainEventStorageError> {
    let mut results: Vec<(Vec<u8>, String)> = Vec::new();
    let mut last_key: Option<Vec<u8>> = None;

    for owner_address in owner_addresses {
        if results.len() >= page_size {
            break;
        }

        // Route the composite cursor to this single address.
        let address_token = match page_token {
            Some(token) => {
                let prefix = make_channel_register_by_owner_address_prefix(owner_address);
                if token.starts_with(&prefix) {
                    Some(token.to_vec()) // resume within the token's own address
                } else if token < prefix.as_slice() {
                    None // address sorts above the token: scan from the start
                } else {
                    continue; // address sorts below the token: already returned
                }
            }
            None => None,
        };

        let page_options = PageOptions {
            page_size: Some(page_size - results.len()),
            page_token: address_token,
            reverse: false,
        };
        let (channel_keys, next_token) =
            get_channel_keys_by_owner_address(db, owner_address, &page_options)?;
        for channel_key in channel_keys {
            results.push((owner_address.clone(), channel_key));
        }
        // A non-empty token means this address stopped at `page_size`; carry it so
        // it becomes `next_page_token` once the page is full.
        if next_token.is_some() {
            last_key = next_token;
        }
    }

    let next_page_token = if results.len() >= page_size {
        last_key
    } else {
        None
    };
    Ok((results, next_page_token))
}

fn validate_evm_address(owner_address: &[u8]) -> Result<(), OnchainEventStorageError> {
    if owner_address.len() != EVM_ADDRESS_LENGTH {
        return Err(HubError::validation_failure(
            format!(
                "expected {}-byte EVM address, got {}",
                EVM_ADDRESS_LENGTH,
                owner_address.len()
            )
            .as_str(),
        )
        .into());
    }

    Ok(())
}

fn get_event_by_secondary_key(
    db: &RocksDB,
    secondary_key: Vec<u8>,
    txn_batch: Option<&RocksDbTransactionBatch>,
) -> Result<Option<OnChainEvent>, OnchainEventStorageError> {
    let txn_batch = if let Some(txn_batch) = txn_batch {
        txn_batch
    } else {
        &mut RocksDbTransactionBatch::new()
    };
    match get_from_db_or_txn(db, txn_batch, &secondary_key)? {
        Some(event_primary_key) => match get_from_db_or_txn(db, txn_batch, &event_primary_key)? {
            Some(onchain_event) => {
                let onchain_event = OnChainEvent::decode(onchain_event.as_slice())?;
                Ok(Some(onchain_event))
            }
            None => Ok(None),
        },
        None => Ok(None),
    }
}

pub fn get_onchain_events(
    db: &RocksDB,
    page_options: &PageOptions,
    start_prefix: Vec<u8>,
    stop_prefix: Vec<u8>,
) -> Result<OnchainEventsPage, OnchainEventStorageError> {
    let mut onchain_events = vec![];
    let mut last_key = vec![];
    db.for_each_iterator_by_prefix_paged(
        Some(start_prefix),
        Some(stop_prefix),
        page_options,
        |key, value| {
            let onchain_event = OnChainEvent::decode(value).map_err(|e| HubError::from(e))?;
            onchain_events.push(onchain_event);

            if onchain_events.len() >= page_options.page_size.unwrap_or(PAGE_SIZE_MAX) {
                last_key = key.to_vec();
                return Ok(true); // Stop iterating
            }

            Ok(false) // Continue iterating
        },
    )
    .map_err(|e| OnchainEventStorageError::HubError(e))?; // TODO: Return the right error
    let next_page_token = if !last_key.is_empty() {
        Some(last_key)
    } else {
        None
    };

    Ok(OnchainEventsPage {
        onchain_events,
        next_page_token,
    })
}

pub fn get_onchain_events_with_filter<F>(
    db: &RocksDB,
    page_options: &PageOptions,
    event_type: OnChainEventType,
    fid: Option<u64>,
    filter: F,
) -> Result<OnchainEventsPage, OnchainEventStorageError>
where
    F: Fn(&OnChainEvent) -> bool,
{
    let mut start_prefix = make_onchain_event_type_prefix(event_type);
    if let Some(fid) = &fid {
        start_prefix.extend(make_fid_key(*fid));
    }
    let stop_prefix = increment_vec_u8(&start_prefix);

    let mut onchain_events = vec![];
    let mut last_key = vec![];
    db.for_each_iterator_by_prefix(
        Some(start_prefix),
        Some(stop_prefix),
        page_options,
        |key, value| {
            let onchain_event = OnChainEvent::decode(value).map_err(|e| HubError::from(e))?;
            if filter(&onchain_event) {
                onchain_events.push(onchain_event);

                if onchain_events.len() >= page_options.page_size.unwrap_or(PAGE_SIZE_MAX) {
                    last_key = key.to_vec();
                    return Ok(true); // Stop iterating
                }
            }

            Ok(false) // Continue iterating
        },
    )
    .map_err(|e| OnchainEventStorageError::HubError(e))?; // TODO: Return the right error
    let next_page_token = if !last_key.is_empty() {
        Some(last_key)
    } else {
        None
    };

    Ok(OnchainEventsPage {
        onchain_events,
        next_page_token,
    })
}

#[derive(Clone, Debug)]
pub struct StorageSlot {
    units_legacy: u32,
    units_2024: u32,
    units_2025: u32,
    pub invalidate_at: u32,
}

impl StorageSlot {
    pub fn new(
        units_legacy: u32,
        units_2024: u32,
        units_2025: u32,
        invalidate_at: u32,
    ) -> StorageSlot {
        StorageSlot {
            units_legacy,
            units_2024,
            units_2025,
            invalidate_at,
        }
    }

    pub fn units_for(&self, unit_type: proto::StorageUnitType) -> u32 {
        match unit_type {
            proto::StorageUnitType::UnitTypeLegacy => self.units_legacy,
            proto::StorageUnitType::UnitType2024 => self.units_2024,
            proto::StorageUnitType::UnitType2025 => self.units_2025,
        }
    }

    pub fn unit_type_2024_cutoff(network: FarcasterNetwork) -> u32 {
        if network == FarcasterNetwork::Mainnet {
            UNIT_TYPE_2024_CUTOFF_TIMESTAMP
        } else {
            UNIT_TYPE_2024_CUTOFF_TIMESTAMP_TESTNET
        }
    }

    pub fn unit_type_2025_cutoff(network: FarcasterNetwork) -> u32 {
        if network == FarcasterNetwork::Mainnet {
            UNIT_TYPE_2025_CUTOFF_TIMESTAMP
        } else {
            UNIT_TYPE_2025_CUTOFF_TIMESTAMP_TESTNET
        }
    }

    pub fn from_storage_lend(storage_lend: &LendStorageBody) -> StorageSlot {
        let mut storage_slot = StorageSlot::new(0, 0, 0, u32::MAX);
        match storage_lend.unit_type() {
            StorageUnitType::UnitType2024 => {
                storage_slot.units_2024 = storage_lend.num_units as u32
            }
            StorageUnitType::UnitTypeLegacy => {
                storage_slot.units_legacy = storage_lend.num_units as u32
            }
            StorageUnitType::UnitType2025 => {
                storage_slot.units_2025 = storage_lend.num_units as u32
            }
        }

        storage_slot
    }

    pub fn from_event(
        onchain_event: &OnChainEvent,
        network: FarcasterNetwork,
        engine_version: EngineVersion,
    ) -> Result<StorageSlot, OnchainEventStorageError> {
        if let Some(body) = &onchain_event.body {
            return match body {
                on_chain_event::Body::StorageRentEventBody(storage_rent_event) => {
                    let slot;

                    let unit_type_2024_cutoff_timestamp = Self::unit_type_2024_cutoff(network);
                    let unit_type_2025_cutoff_timestamp = Self::unit_type_2025_cutoff(network);

                    // NOTE(Jul 2025): We have 3 types of storages units based on when they were rented.
                    // As part of the storage redenomination FIP, we're also extended the expiry of all
                    // previously rented storage units by 1 year, in addition to the previous 1-year extension.
                    // So legacy units are valid for 3 years (original 1 year validity + 2 extensions),
                    // 2024 units for 2 years (one extension), and 2025 units for 1 year (no extensions).
                    // Original Storage Extension: https://github.com/farcasterxyz/protocol/discussions/191
                    // Storage Redenomination: https://github.com/farcasterxyz/protocol/discussions/229
                    //
                    // NOTE(Jun 2026): Once the StorageExpiryExtension2026 feature is enabled (engine
                    // version V18), we extend the expiry of every already-rented unit by one more year:
                    // legacy units become valid for 4 years, 2024 units for 3 years, and the existing
                    // 2025 cohort (rented before UNIT_TYPE_2025_CUTOFF_TIMESTAMP) for 2 years. Units
                    // rented at/after the 2025 cutoff are "new rentals" and keep the standard 1-year
                    // validity. The cohort and new rentals are both UnitType2025 (no new unit type);
                    // only their invalidate_at differs.
                    let extension =
                        if engine_version.is_enabled(ProtocolFeature::StorageExpiryExtension2026) {
                            1
                        } else {
                            0
                        };

                    if onchain_event.block_timestamp < UNIT_TYPE_LEGACY_CUTOFF_TIMESTAMP as u64 {
                        slot = StorageSlot::new(
                            storage_rent_event.units,
                            0,
                            0,
                            onchain_event.block_timestamp as u32
                                + (ONE_YEAR_IN_SECONDS * (3 + extension)),
                        );
                    } else if onchain_event.block_timestamp < unit_type_2024_cutoff_timestamp as u64
                    {
                        slot = StorageSlot::new(
                            0,
                            storage_rent_event.units,
                            0,
                            onchain_event.block_timestamp as u32
                                + (ONE_YEAR_IN_SECONDS * (2 + extension)),
                        );
                    } else if onchain_event.block_timestamp < unit_type_2025_cutoff_timestamp as u64
                    {
                        slot = StorageSlot::new(
                            0,
                            0,
                            storage_rent_event.units,
                            onchain_event.block_timestamp as u32
                                + (ONE_YEAR_IN_SECONDS * (1 + extension)),
                        );
                    } else {
                        slot = StorageSlot::new(
                            0,
                            0,
                            storage_rent_event.units,
                            onchain_event.block_timestamp as u32 + ONE_YEAR_IN_SECONDS,
                        );
                    };
                    Ok(slot)
                }
                _ => Err(OnchainEventStorageError::InvalidStorageRentEventType),
            };
        }
        Err(OnchainEventStorageError::InvalidStorageRentEventType)
    }

    pub fn is_active(&self) -> bool {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;
        current_time < self.invalidate_at
    }

    pub fn merge(&mut self, other: &StorageSlot) -> bool {
        if !other.is_active() {
            return false;
        }
        if !self.is_active() {
            *self = other.clone();
            return true;
        }
        self.units_legacy += other.units_legacy;
        self.units_2024 += other.units_2024;
        self.units_2025 += other.units_2025;
        self.invalidate_at = std::cmp::min(self.invalidate_at, other.invalidate_at);
        true
    }

    pub fn sub(&mut self, other: &StorageSlot) {
        if other.is_active() {
            self.units_legacy -= other.units_legacy;
            self.units_2024 -= other.units_2024;
            self.units_2025 -= other.units_2025;
        }
    }
}

#[derive(Clone)]
pub struct OnchainEventStore {
    pub(crate) db: Arc<RocksDB>,
    pub store_event_handler: Arc<StoreEventHandler>,
    store_opts: StoreOptions,
}

impl OnchainEventStore {
    pub fn new(db: Arc<RocksDB>, store_event_handler: Arc<StoreEventHandler>) -> OnchainEventStore {
        OnchainEventStore {
            db,
            store_event_handler,
            store_opts: StoreOptions::default(),
        }
    }

    pub fn new_with_opts(
        db: Arc<RocksDB>,
        store_event_handler: Arc<StoreEventHandler>,
        store_opts: StoreOptions,
    ) -> OnchainEventStore {
        OnchainEventStore {
            db,
            store_event_handler,
            store_opts,
        }
    }

    pub fn merge_onchain_event(
        &self,
        onchain_event: OnChainEvent,
        txn: &mut RocksDbTransactionBatch,
    ) -> Result<HubEvent, OnchainEventStorageError> {
        merge_onchain_event(&self.db, txn, onchain_event.clone(), &self.store_opts)?;
        let hub_event = &mut HubEvent::new_event(
            HubEventType::MergeOnChainEvent,
            proto::hub_event::Body::MergeOnChainEventBody(MergeOnChainEventBody {
                on_chain_event: Some(onchain_event.clone()),
            }),
        );
        let id = self
            .store_event_handler
            .commit_transaction(txn, hub_event)?;
        hub_event.id = id;
        Ok(hub_event.clone())
    }

    pub fn get_all_onchain_events(
        &self,
        page_options: &PageOptions,
    ) -> Result<OnchainEventsPage, OnchainEventStorageError> {
        let start_prefix = vec![
            RootPrefix::OnChainEvent as u8,
            OnChainEventPostfix::OnChainEvents as u8,
        ];
        get_onchain_events(
            &self.db,
            page_options,
            start_prefix.clone(),
            increment_vec_u8(&start_prefix),
        )
    }

    pub fn get_onchain_events(
        &self,
        event_type: OnChainEventType,
        fid: Option<u64>,
    ) -> Result<Vec<OnChainEvent>, OnchainEventStorageError> {
        let mut onchain_events = vec![];
        let mut next_page_token = None;
        let mut start_prefix = make_onchain_event_type_prefix(event_type);
        if let Some(fid) = &fid {
            start_prefix.extend(make_fid_key(*fid));
        }
        let stop_prefix = increment_vec_u8(&start_prefix);

        loop {
            let onchain_events_page = get_onchain_events(
                &self.db,
                &PageOptions {
                    page_size: Some(PAGE_SIZE),
                    page_token: next_page_token,
                    reverse: false,
                },
                start_prefix.clone(),
                stop_prefix.clone(),
            )?;
            onchain_events.extend(onchain_events_page.onchain_events);
            if onchain_events_page.next_page_token.is_none() {
                break;
            } else {
                next_page_token = onchain_events_page.next_page_token
            }
        }

        Ok(onchain_events)
    }

    pub fn get_signers(
        &self,
        fid: Option<u64>,
        page_options: &PageOptions,
    ) -> Result<OnchainEventsPage, OnchainEventStorageError> {
        get_onchain_events_with_filter(
            &self.db,
            &page_options,
            OnChainEventType::EventTypeSigner,
            fid,
            |onchain_event: &OnChainEvent| match &onchain_event.body {
                None => false,
                Some(body) => match body {
                    on_chain_event::Body::SignerEventBody(signer_event_body) => {
                        if let Ok(active_signer) = self.get_active_signer(
                            onchain_event.fid,
                            signer_event_body.key.clone(),
                            None,
                        ) {
                            active_signer.is_some()
                        } else {
                            false
                        }
                    }
                    _ => false,
                },
            },
        )
    }

    pub fn get_fids(
        &self,
        page_options: &PageOptions,
    ) -> Result<(Vec<u64>, Option<Vec<u8>>), OnchainEventStorageError> {
        let start_prefix = make_onchain_event_type_prefix(OnChainEventType::EventTypeIdRegister);
        let onchain_events_page = get_onchain_events(
            &self.db,
            page_options,
            start_prefix.clone(),
            increment_vec_u8(&start_prefix),
        )?;

        let fids = onchain_events_page
            .onchain_events
            .iter()
            .filter(|event| {
                // Filter out events that are not IdRegisterEventBody.event_type == IdRegisterEventType::Register
                if let Some(Body::IdRegisterEventBody(body)) = &event.body {
                    body.event_type() == IdRegisterEventType::Register
                } else {
                    false
                }
            })
            .map(|event| event.fid)
            .collect();
        let next_page_token = onchain_events_page.next_page_token;

        Ok((fids, next_page_token))
    }

    #[inline]
    pub fn get_id_register_event_by_fid(
        &self,
        fid: u64,
        txn_batch: Option<&RocksDbTransactionBatch>,
    ) -> Result<Option<OnChainEvent>, OnchainEventStorageError> {
        get_event_by_secondary_key(&self.db, make_id_register_by_fid_key(fid), txn_batch)
    }

    /// Returns the materialized owner record for a channel key, or `None` if unknown.
    /// Does not filter on `expiry` — callers decide whether an expired registration
    /// counts as absent. The `block_number`/`tx_index`/`log_index` fields are an internal
    /// ordering watermark and are not a stable contract for consumers.
    pub fn get_channel_owner(
        &self,
        channel_key: &str,
        txn_batch: Option<&RocksDbTransactionBatch>,
    ) -> Result<Option<ChannelOwner>, OnchainEventStorageError> {
        let empty_txn = RocksDbTransactionBatch::new();
        let txn = txn_batch.unwrap_or(&empty_txn);
        read_channel_owner_by_channel_key(&self.db, txn, channel_key)
    }

    /// Reads the ChannelKeyByLabel index (label -> channel_key). Test-only: the
    /// production TRANSFER path resolves the label internally, so there is no
    /// non-test caller; tests use it to assert the index materialized.
    #[cfg(test)]
    pub fn get_channel_key_by_label(
        &self,
        label: &[u8],
    ) -> Result<Option<String>, OnchainEventStorageError> {
        let empty_txn = RocksDbTransactionBatch::new();
        match get_from_db_or_txn(
            &self.db,
            &empty_txn,
            &make_channel_register_channel_key_by_label_key(label),
        )? {
            Some(bytes) => Ok(Some(
                String::from_utf8(bytes).map_err(|err| DecodeError::new(err.to_string()))?,
            )),
            None => Ok(None),
        }
    }

    pub fn get_active_signer(
        &self,
        fid: u64,
        signer: Vec<u8>,
        txn_batch: Option<&RocksDbTransactionBatch>,
    ) -> Result<Option<OnChainEvent>, OnchainEventStorageError> {
        let signer_key = make_signer_onchain_event_by_signer_key(fid, signer);
        let signer = get_event_by_secondary_key(&self.db, signer_key, txn_batch)
            .map_err(|e| OnchainEventStorageError::from(e))?;
        if let Some(signer) = signer {
            if let Some(body) = &signer.body {
                if let on_chain_event::Body::SignerEventBody(signer_event_body) = body {
                    // Only return the signer if it's active (not removed) and the key type is supported
                    if signer_event_body.event_type() == SignerEventType::Add
                        && signer_event_body.key_type == SUPPORTED_SIGNER_KEY_TYPE
                    {
                        return Ok(Some(signer));
                    }
                }
            }
        }
        Ok(None)
    }
    pub fn tier_subscription_exires_at(
        &self,
        tier_type: TierType,
        fid: u64,
        as_of: Option<&FarcasterTime>,
    ) -> Result<u64, OnchainEventStorageError> {
        // TODO(aditi): This is pretty expensive, we may want to add caching for fid -> tier expiration to speed up.
        // Sorted by block timestamp
        let tier_purchase_events = self
            .get_onchain_events(OnChainEventType::EventTypeTierPurchase, Some(fid))?
            .into_iter()
            .filter(|event| match &event.body {
                Some(on_chain_event::Body::TierPurchaseEventBody(body)) => {
                    if body.tier_type == tier_type as i32 {
                        true
                    } else {
                        false
                    }
                }
                _ => false,
            });

        let mut expires_at = 0;
        for tier_purchase in tier_purchase_events {
            match tier_purchase.body.unwrap() {
                on_chain_event::Body::TierPurchaseEventBody(body) => {
                    if let Some(as_of) = as_of {
                        if tier_purchase.block_timestamp > as_of.to_unix_seconds() {
                            break;
                        }
                    };
                    let extend_by = body.for_days * 24 * 60 * 60;
                    expires_at = tier_purchase.block_timestamp.max(expires_at) + extend_by;
                }
                _ => {}
            };
        }
        Ok(expires_at)
    }

    pub fn is_tier_subscription_active_at(
        &self,
        tier_type: TierType,
        fid: u64,
        timestamp: &FarcasterTime,
    ) -> Result<bool, OnchainEventStorageError> {
        let expires_at = self.tier_subscription_exires_at(tier_type, fid, Some(timestamp))?;
        Ok(expires_at >= timestamp.to_unix_seconds())
    }

    pub fn get_storage_slot_for_fid(
        &self,
        fid: u64,
        network: FarcasterNetwork,
        engine_version: EngineVersion,
        pending_events: &[OnChainEvent],
        lent_storage: &StorageSlot,
        borrowed_storage: &StorageSlot,
    ) -> Result<StorageSlot, OnchainEventStorageError> {
        let rent_events =
            self.get_onchain_events(OnChainEventType::EventTypeStorageRent, Some(fid))?;
        let mut storage_slot = StorageSlot::new(0, 0, 0, 0);
        for rent_event in rent_events {
            storage_slot.merge(&StorageSlot::from_event(
                &rent_event,
                network,
                engine_version,
            )?);
        }
        // Now, virtually merge any pending rent events from the current transaction
        for event in pending_events {
            if event.fid == fid && event.r#type() == OnChainEventType::EventTypeStorageRent {
                storage_slot.merge(&StorageSlot::from_event(event, network, engine_version)?);
            }
        }

        storage_slot.sub(lent_storage);
        storage_slot.merge(borrowed_storage);

        Ok(storage_slot)
    }

    #[inline]
    pub fn exists(&self, onchain_event: &OnChainEvent) -> Result<bool, OnchainEventStorageError> {
        let primary_key = make_onchain_event_primary_key(onchain_event);
        match self.db.get(&primary_key)? {
            None => Ok(false),
            Some(_) => Ok(true),
        }
    }
}

pub struct FIDIterator {
    db: Arc<RocksDB>,
    last_fid: u64,
    fids: VecDeque<u64>,
    error: Option<OnchainEventStorageError>,
}

impl FIDIterator {
    const PAGE_SIZE_MAX: usize = 100;

    pub fn new(db: Arc<RocksDB>, start_fid: u64) -> Self {
        FIDIterator {
            db,
            last_fid: start_fid,
            fids: VecDeque::new(),
            error: None,
        }
    }

    /// Returns the error that ended iteration, if any. `next()` cannot surface
    /// fetch failures — it ends iteration exactly like normal exhaustion — so a
    /// caller that must distinguish "complete" from "failed" (e.g. a migration
    /// that deletes data only after a full pass) checks this after the loop.
    pub fn take_error(&mut self) -> Option<OnchainEventStorageError> {
        self.error.take()
    }

    fn fetch(&mut self) -> Result<Option<u64>, OnchainEventStorageError> {
        let mut start_prefix =
            make_onchain_event_type_prefix(OnChainEventType::EventTypeIdRegister);
        let stop_prefix = increment_vec_u8(&start_prefix);
        start_prefix.extend(make_fid_key(self.last_fid + 1));

        let page_options = PageOptions {
            page_size: Some(Self::PAGE_SIZE_MAX),
            page_token: None,
            reverse: false,
        };

        let mut last_fid: u64 = 0;

        self.db
            .for_each_iterator_by_prefix_paged(
                Some(start_prefix),
                Some(stop_prefix),
                &page_options,
                |_key, value| {
                    let onchain_event =
                        OnChainEvent::decode(value).map_err(|e| HubError::from(e))?;

                    // Filter out events that are not IdRegisterEventBody.event_type == IdRegisterEventType::Register
                    if let Some(Body::IdRegisterEventBody(body)) = &onchain_event.body {
                        if body.event_type() != IdRegisterEventType::Register {
                            return Ok(false); // Skip this event
                        }
                    } else {
                        return Ok(false); // Skip this event
                    }

                    if self.fids.back() == Some(&onchain_event.fid) {
                        // Skip this ID register event. There is a small number of FIDs that have 2 ID register events
                        // because of an old issue. See FIDs 20617, 20671 for eg.
                    } else {
                        self.fids.push_back(onchain_event.fid);
                        last_fid = onchain_event.fid;
                    }

                    if self.fids.len() >= page_options.page_size.unwrap_or(PAGE_SIZE_MAX) {
                        return Ok(true); // Stop iterating
                    }

                    Ok(false) // Continue iterating
                },
            )
            .map_err(|e| OnchainEventStorageError::HubError(e))?;

        let ref_fid = if last_fid > 0 { Some(last_fid) } else { None };
        Ok(ref_fid)
    }
}

impl Iterator for FIDIterator {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.fids.is_empty() {
            match self.fetch() {
                Ok(None) => {
                    // Done fetching, no more FIDs
                    return None;
                }
                Err(err) => {
                    // Iteration ends indistinguishably from exhaustion here;
                    // callers that need the distinction use take_error().
                    self.error = Some(err);
                    return None;
                }
                Ok(Some(_fid)) => {}
            }
        }

        if let Some(fid) = self.fids.pop_front() {
            self.last_fid = fid;
            return Some(fid);
        }

        None
    }
}
