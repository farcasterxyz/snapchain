use std::sync::Arc;

use prost::{DecodeError, Message};

use super::{get_from_db_or_txn, make_fid_key, StoreEventHandler};
use crate::core::error::HubError;
use crate::core::util::FarcasterTime;
use crate::proto::{
    self, on_chain_event, FarcasterNetwork, IdRegisterEventBody, IdRegisterEventType, OnChainEvent,
    OnChainEventType, SignerEventBody, SignerEventType, TierType,
};
use crate::proto::{HubEvent, HubEventType, MergeOnChainEventBody};
use crate::storage::constants::{OnChainEventPostfix, RootPrefix, PAGE_SIZE_MAX};
use crate::storage::db::{PageOptions, RocksDB, RocksDbTransactionBatch, RocksdbError};
use crate::storage::util::increment_vec_u8;
use thiserror::Error;

static PAGE_SIZE: usize = 1000;

const UNIT_TYPE_LEGACY_CUTOFF_TIMESTAMP: u32 = 1724889600; // 2024-08-29 Midnight UTC
const UNIT_TYPE_2024_CUTOFF_TIMESTAMP: u32 = 1752685200; // 2025-07-16 5PM UTC (Engine version 6)
const UNIT_TYPE_2024_CUTOFF_TIMESTAMP_TESTNET: u32 = 1752426000; // 2025-07-13 5PM UTC (a few days earlier than mainnet)
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
) -> Result<(), OnchainEventStorageError> {
    let primary_key = make_onchain_event_primary_key(&onchain_event);
    if let Some(_) = get_from_db_or_txn(db, txn, &primary_key)? {
        return Err(OnchainEventStorageError::DuplicateOnchainEvent);
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
    match get_event_by_secondary_key(db, id_register_by_fid_key.clone())? {
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
    match get_event_by_secondary_key(db, signer_key.clone())? {
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
        loop {
            let events_page = get_onchain_events(
                db,
                &PageOptions {
                    page_size: None,
                    page_token: next_page_token,
                    reverse: false,
                },
                OnChainEventType::EventTypeSigner,
                Some(onchain_event.fid),
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
            on_chain_event::Body::IdRegisterEventBody(id_register_event_body) => {
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
            on_chain_event::Body::SignerMigratedEventBody(_)
            | on_chain_event::Body::StorageRentEventBody(_)
            | on_chain_event::Body::TierPurchaseEventBody(_) => {}
        }
    };

    Ok(())
}

fn get_event_by_secondary_key(
    db: &RocksDB,
    secondary_key: Vec<u8>,
) -> Result<Option<OnChainEvent>, OnchainEventStorageError> {
    match db.get(&secondary_key)? {
        Some(event_primary_key) => match db.get(&event_primary_key)? {
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
    event_type: OnChainEventType,
    fid: Option<u64>,
) -> Result<OnchainEventsPage, OnchainEventStorageError> {
    let mut start_prefix = make_onchain_event_type_prefix(event_type);

    if let Some(fid) = &fid {
        start_prefix.extend(make_fid_key(*fid));
    }

    let stop_prefix = increment_vec_u8(&start_prefix);

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
    let next_page_token = if last_key.len() > 0 {
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
    let next_page_token = if last_key.len() > 0 {
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

    pub fn from_event(
        onchain_event: &OnChainEvent,
        network: FarcasterNetwork,
    ) -> Result<StorageSlot, OnchainEventStorageError> {
        if let Some(body) = &onchain_event.body {
            return match body {
                on_chain_event::Body::StorageRentEventBody(storage_rent_event) => {
                    let slot;

                    let unit_type_2024_cutoff_timestamp = if network == FarcasterNetwork::Mainnet {
                        UNIT_TYPE_2024_CUTOFF_TIMESTAMP
                    } else {
                        UNIT_TYPE_2024_CUTOFF_TIMESTAMP_TESTNET
                    };

                    // NOTE(Jul 2025): We have 3 types of storages units based on when they were rented.
                    // As part of the storage redenomination FIP, we're also extended the expiry of all
                    // previously rented storage units by 1 year, in addition to the previous 1-year extension.
                    // So legacy units are valid for 3 years (original 1 year validity + 2 extensions),
                    // 2024 units for 2 years (one extension), and 2025 units for 1 year (no extensions).
                    // Original Storage Extension: https://github.com/farcasterxyz/protocol/discussions/191
                    // Storage Redenomination: https://github.com/farcasterxyz/protocol/discussions/229

                    if onchain_event.block_timestamp < UNIT_TYPE_LEGACY_CUTOFF_TIMESTAMP as u64 {
                        slot = StorageSlot::new(
                            storage_rent_event.units,
                            0,
                            0,
                            onchain_event.block_timestamp as u32 + (ONE_YEAR_IN_SECONDS * 3),
                        );
                    } else if onchain_event.block_timestamp < unit_type_2024_cutoff_timestamp as u64
                    {
                        slot = StorageSlot::new(
                            0,
                            storage_rent_event.units,
                            0,
                            onchain_event.block_timestamp as u32 + (ONE_YEAR_IN_SECONDS * 2),
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
}

#[derive(Clone)]
pub struct OnchainEventStore {
    pub(crate) db: Arc<RocksDB>,
    store_event_handler: Arc<StoreEventHandler>,
}

impl OnchainEventStore {
    pub fn new(db: Arc<RocksDB>, store_event_handler: Arc<StoreEventHandler>) -> OnchainEventStore {
        OnchainEventStore {
            db,
            store_event_handler,
        }
    }

    pub fn merge_onchain_event(
        &self,
        onchain_event: OnChainEvent,
        txn: &mut RocksDbTransactionBatch,
    ) -> Result<HubEvent, OnchainEventStorageError> {
        merge_onchain_event(&self.db, txn, onchain_event.clone())?;
        let hub_event = &mut HubEvent::from(
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

    pub fn get_onchain_events(
        &self,
        event_type: OnChainEventType,
        fid: Option<u64>,
    ) -> Result<Vec<OnChainEvent>, OnchainEventStorageError> {
        let mut onchain_events = vec![];
        let mut next_page_token = None;
        loop {
            let onchain_events_page = get_onchain_events(
                &self.db,
                &PageOptions {
                    page_size: Some(PAGE_SIZE),
                    page_token: next_page_token,
                    reverse: false,
                },
                event_type,
                fid,
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
                        if let Ok(active_signer) =
                            self.get_active_signer(onchain_event.fid, signer_event_body.key.clone())
                        {
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
        let onchain_events_page = get_onchain_events(
            &self.db,
            page_options,
            OnChainEventType::EventTypeIdRegister,
            None,
        )?;

        let fids = onchain_events_page
            .onchain_events
            .iter()
            .map(|event| event.fid)
            .collect();
        let next_page_token = onchain_events_page.next_page_token;

        Ok((fids, next_page_token))
    }

    #[inline]
    pub fn get_id_register_event_by_fid(
        &self,
        fid: u64,
    ) -> Result<Option<OnChainEvent>, OnchainEventStorageError> {
        get_event_by_secondary_key(&self.db, make_id_register_by_fid_key(fid))
    }

    pub fn get_active_signer(
        &self,
        fid: u64,
        signer: Vec<u8>,
    ) -> Result<Option<OnChainEvent>, OnchainEventStorageError> {
        let signer_key = make_signer_onchain_event_by_signer_key(fid, signer);
        let signer = get_event_by_secondary_key(&self.db, signer_key)
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
    ) -> Result<StorageSlot, OnchainEventStorageError> {
        let rent_events =
            self.get_onchain_events(OnChainEventType::EventTypeStorageRent, Some(fid))?;
        let mut storage_slot = StorageSlot::new(0, 0, 0, 0);
        for rent_event in rent_events {
            storage_slot.merge(&StorageSlot::from_event(&rent_event, network)?);
        }
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
