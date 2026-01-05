use super::account::{
    EventsPage, HubEventStorageExt, ReactionStore, ReactionStoreDef, StorageSlot, UserDataStore,
    UserDataStoreDef, VerificationStore, VerificationStoreDef,
};
use crate::core::error::HubError;
use crate::core::util::FarcasterTime;
use crate::hyper::StateContext;
use crate::network::http_server::TierType;
use crate::proto::{
    self, HubEvent, OnChainEvent, StorageLimit, StorageLimitsResponse, StorageUnitDetails,
    StorageUnitType, StoreType,
};
use crate::proto::{MessageType, TierDetails};
use crate::storage::constants::{RootPrefix, PAGE_SIZE_MAX};
use crate::storage::db::{PageOptions, RocksDB, RocksDbTransactionBatch, RocksdbError};
use crate::storage::store::account::{
    BlockEventStore, CastStore, CastStoreDef, IntoU8, LinkStore, OnchainEventStorageError,
    OnchainEventStore, StorageLendStore, StorageLendStoreDef, Store, StoreEventHandler,
    StoreOptions, UsernameProofStore, UsernameProofStoreDef,
};
use crate::storage::store::shard::ShardStore;
use crate::storage::trie::merkle_trie;
use crate::storage::trie::merkle_trie::TrieKey;
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::{error, info};

#[derive(Error, Debug)]
pub enum StoresError {
    #[error(transparent)]
    OnchainEventError(#[from] OnchainEventStorageError),

    #[error("unsupported message type")]
    UnsupportedMessageType(MessageType),

    #[error("store error")]
    StoreError {
        inner: HubError, // TODO: move away from HubError when we can
        hash: Vec<u8>,
    },
}

#[derive(Clone)]
pub struct Stores {
    pub block_event_store: BlockEventStore,
    pub shard_store: ShardStore,
    pub cast_store: Store<CastStoreDef>,
    pub link_store: Store<LinkStore>,
    pub reaction_store: Store<ReactionStoreDef>,
    pub user_data_store: Store<UserDataStoreDef>,
    pub verification_store: Store<VerificationStoreDef>,
    pub onchain_event_store: OnchainEventStore,
    pub username_proof_store: Store<UsernameProofStoreDef>,
    pub storage_lend_store: Store<StorageLendStoreDef>,
    pub db: Arc<RocksDB>,
    pub trie: merkle_trie::MerkleTrie,
    pub store_limits: StoreLimits,
    pub event_handler: Arc<StoreEventHandler>,
    pub shard_id: u32,
    pub statsd: StatsdClientWrapper,
    prune_lock: Arc<RwLock<bool>>,
    pub network: proto::FarcasterNetwork,
}

#[derive(Clone, Debug)]
pub struct Limits {
    pub casts: u32,
    pub links: u32,
    pub reactions: u32,
    pub user_data: u32,
    pub user_name_proofs: u32,
    pub verifications: u32,
    pub storage_lends: u32,
}

impl Limits {
    pub fn default() -> Limits {
        Self::of_type(StorageUnitType::UnitType2025)
    }

    pub fn of_type(unit_type: StorageUnitType) -> Limits {
        match unit_type {
            // Original limits, units rented before Aug 24, 2024
            StorageUnitType::UnitTypeLegacy => Limits {
                casts: 5000,
                links: 2500,
                reactions: 2500,
                user_data: 50,
                user_name_proofs: 5,
                verifications: 25,
                storage_lends: 1,
            },
            // Extended Storage limits https://github.com/farcasterxyz/protocol/discussions/191
            // Units rented after Aug 24, 2024
            StorageUnitType::UnitType2024 => Limits {
                casts: 2000,
                links: 1000,
                reactions: 1000,
                user_data: 50,
                user_name_proofs: 5,
                verifications: 25,
                storage_lends: 1,
            },
            // Storage Redenomination FIP: https://github.com/farcasterxyz/protocol/discussions/229
            // Units rented after Jul 16, 2025
            StorageUnitType::UnitType2025 => Limits {
                casts: 100,
                links: 200,
                reactions: 200,
                user_data: 25,
                user_name_proofs: 2,
                verifications: 5,
                storage_lends: 1,
            },
        }
    }

    pub fn legacy() -> Limits {
        Self::of_type(StorageUnitType::UnitTypeLegacy)
    }

    #[cfg(test)]
    fn for_message_type(&self, message_type: MessageType) -> u32 {
        self.for_store_type(Limits::message_type_to_store_type(message_type))
    }

    fn message_type_to_store_type(message_type: MessageType) -> StoreType {
        match message_type {
            MessageType::CastAdd => StoreType::Casts,
            MessageType::CastRemove => StoreType::Casts,
            MessageType::ReactionAdd => StoreType::Reactions,
            MessageType::ReactionRemove => StoreType::Reactions,
            MessageType::LinkAdd => StoreType::Links,
            MessageType::LinkRemove => StoreType::Links,
            MessageType::LinkCompactState => StoreType::Links,
            MessageType::VerificationAddEthAddress => StoreType::Verifications,
            MessageType::VerificationRemove => StoreType::Verifications,
            MessageType::UserDataAdd => StoreType::UserData,
            MessageType::UsernameProof => StoreType::UsernameProofs,
            MessageType::FrameAction => StoreType::None,
            MessageType::LendStorage => StoreType::StorageLends,
            MessageType::None => StoreType::None,
        }
    }

    fn store_type_to_message_types(store_type: StoreType) -> Vec<MessageType> {
        match store_type {
            StoreType::Casts => vec![MessageType::CastAdd, MessageType::CastRemove],
            StoreType::Links => vec![
                MessageType::LinkAdd,
                MessageType::LinkRemove,
                MessageType::LinkCompactState,
            ],
            StoreType::Reactions => vec![MessageType::ReactionAdd, MessageType::ReactionRemove],
            StoreType::UserData => vec![MessageType::UserDataAdd],
            StoreType::Verifications => vec![
                MessageType::VerificationAddEthAddress,
                MessageType::VerificationRemove,
            ],
            StoreType::UsernameProofs => vec![MessageType::UsernameProof],
            StoreType::StorageLends => vec![MessageType::LendStorage],
            StoreType::None => vec![],
        }
    }

    fn for_store_type(&self, store_type: StoreType) -> u32 {
        match store_type {
            StoreType::Casts => self.casts,
            StoreType::Links => self.links,
            StoreType::Reactions => self.reactions,
            StoreType::UserData => self.user_data,
            StoreType::Verifications => self.verifications,
            StoreType::UsernameProofs => self.user_name_proofs,
            StoreType::StorageLends => self.storage_lends,
            StoreType::None => 0,
        }
    }
}

#[derive(Clone, Debug)]
pub struct StoreLimits {
    limits_2025: Limits,
    limits_2024: Limits,
    limits_legacy: Limits,
}

impl StoreLimits {
    pub fn new(limits_2025: Limits, limits_2024: Limits, limits_legacy: Limits) -> StoreLimits {
        StoreLimits {
            limits_2025,
            limits_2024,
            limits_legacy,
        }
    }

    pub fn max_messages(&self, slot: &StorageSlot, store_type: StoreType) -> u32 {
        [
            StorageUnitType::UnitType2025,
            StorageUnitType::UnitType2024,
            StorageUnitType::UnitTypeLegacy,
        ]
        .iter()
        .map(|&unit_type| {
            self.for_type(unit_type).for_store_type(store_type) * slot.units_for(unit_type)
        })
        .sum()
    }

    pub fn for_type(&self, unit_type: StorageUnitType) -> &Limits {
        match unit_type {
            StorageUnitType::UnitTypeLegacy => &self.limits_legacy,
            StorageUnitType::UnitType2024 => &self.limits_2024,
            StorageUnitType::UnitType2025 => &self.limits_2025,
        }
    }
}

impl Stores {
    pub fn with_state_context(&self, ctx: StateContext) -> Self {
        Self {
            block_event_store: self.block_event_store.clone(),
            shard_store: self.shard_store.clone(),
            cast_store: self.cast_store.with_state_context(ctx),
            link_store: self.link_store.with_state_context(ctx),
            reaction_store: self.reaction_store.with_state_context(ctx),
            user_data_store: self.user_data_store.with_state_context(ctx),
            verification_store: self.verification_store.with_state_context(ctx),
            onchain_event_store: self.onchain_event_store.with_state_context(ctx),
            username_proof_store: self.username_proof_store.with_state_context(ctx),
            storage_lend_store: self.storage_lend_store.with_state_context(ctx),
            db: self.db.clone(),
            trie: self.trie.clone(),
            store_limits: self.store_limits.clone(),
            event_handler: self.event_handler.clone(),
            shard_id: self.shard_id,
            statsd: self.statsd.clone(),
            prune_lock: self.prune_lock.clone(),
            network: self.network,
        }
    }
}

impl StoreLimits {
    pub fn default() -> StoreLimits {
        StoreLimits {
            limits_legacy: Limits::of_type(StorageUnitType::UnitTypeLegacy),
            limits_2024: Limits::of_type(StorageUnitType::UnitType2024),
            limits_2025: Limits::of_type(StorageUnitType::UnitType2025),
        }
    }
}

impl Stores {
    pub fn new(
        db: Arc<RocksDB>,
        shard_id: u32,
        trie: merkle_trie::MerkleTrie,
        store_limits: StoreLimits,
        network: proto::FarcasterNetwork,
        statsd: StatsdClientWrapper,
    ) -> Stores {
        Self::new_with_opts(
            db,
            shard_id,
            trie,
            store_limits,
            network,
            statsd,
            StoreOptions::default(),
        )
    }

    pub fn new_with_opts(
        db: Arc<RocksDB>,
        shard_id: u32,
        mut trie: merkle_trie::MerkleTrie,
        store_limits: StoreLimits,
        network: proto::FarcasterNetwork,
        statsd: StatsdClientWrapper,
        store_opts: StoreOptions,
    ) -> Stores {
        trie.initialize(&db).unwrap();

        let event_handler = StoreEventHandler::new();
        let shard_store = ShardStore::new(db.clone(), shard_id);
        let cast_store =
            CastStore::new_with_opts(db.clone(), event_handler.clone(), 100, store_opts.clone());
        let reaction_store = ReactionStore::new_with_opts(
            db.clone(),
            event_handler.clone(),
            100,
            store_opts.clone(),
        );
        let link_store =
            LinkStore::new_with_opts(db.clone(), event_handler.clone(), 100, store_opts.clone());
        let user_data_store = UserDataStore::new_with_opts(
            db.clone(),
            event_handler.clone(),
            100,
            store_opts.clone(),
        );
        let verification_store = VerificationStore::new_with_opts(
            db.clone(),
            event_handler.clone(),
            100,
            store_opts.clone(),
        );
        let username_proof_store = UsernameProofStore::new_with_opts(
            db.clone(),
            event_handler.clone(),
            100,
            store_opts.clone(),
        );

        let onchain_event_store =
            OnchainEventStore::new_with_opts(db.clone(), event_handler.clone(), store_opts.clone());

        let storage_lend_store = StorageLendStore::new_with_opts(
            db.clone(),
            event_handler.clone(),
            100,
            store_opts.clone(),
        );

        Stores {
            shard_id,
            trie,
            shard_store,
            cast_store,
            link_store,
            reaction_store,
            user_data_store,
            verification_store,
            onchain_event_store,
            username_proof_store,
            storage_lend_store,
            db: db.clone(),
            store_limits,
            event_handler,
            network,
            statsd,
            prune_lock: Arc::new(RwLock::new(false)),
            block_event_store: BlockEventStore { db: db.clone() },
        }
    }

    fn make_schema_version_key() -> Vec<u8> {
        vec![RootPrefix::DBSchemaVersion as u8]
    }

    pub fn get_schema_version(&self) -> Result<u32, RocksdbError> {
        match self.db.get(&Self::make_schema_version_key())? {
            Some(bytes) => Ok(u32::from_be_bytes(bytes.try_into().unwrap_or_default())),
            None => Ok(0), // Default to version 0
        }
    }

    pub fn set_schema_version(&self, version: u32) -> Result<(), RocksdbError> {
        self.db
            .put(&Self::make_schema_version_key(), &version.to_be_bytes())
    }

    pub fn get_storage_slot_for_fid(
        &self,
        fid: u64,
        count_borrowed_storage: bool,
        pending_events: &[OnChainEvent],
    ) -> Result<StorageSlot, StoresError> {
        let lent_storage = StorageLendStore::get_lent_storage(&self.storage_lend_store, fid)
            .map_err(|err| StoresError::StoreError {
                inner: err,
                hash: vec![],
            })?;
        let borrowed_storage = if count_borrowed_storage {
            StorageLendStore::get_borrowed_storage(&self.storage_lend_store, fid).map_err(
                |err| StoresError::StoreError {
                    inner: err,
                    hash: vec![],
                },
            )?
        } else {
            StorageSlot::new(0, 0, 0, u32::MAX)
        };
        let slot = self
            .onchain_event_store
            .get_storage_slot_for_fid(
                fid,
                self.network,
                pending_events,
                &lent_storage,
                &borrowed_storage,
            )
            .map_err(|e| StoresError::OnchainEventError(e))?;

        Ok(slot)
    }

    pub fn get_usage(
        &self,
        fid: u64,
        message_type: MessageType,
        txn_batch: &mut RocksDbTransactionBatch,
    ) -> Result<(u32, u32), StoresError> {
        let store_type = Limits::message_type_to_store_type(message_type);
        let message_count = self.get_usage_by_store_type(fid, store_type, txn_batch)?;
        let count_borrowed_storage = store_type != StoreType::StorageLends;
        let slot = self.get_storage_slot_for_fid(fid, count_borrowed_storage, &[])?;
        let max_messages = self.store_limits.max_messages(&slot, store_type);

        Ok((message_count, max_messages))
    }

    // Usage is defined at the store level, but the trie accounts for message by type, this function maps between the two
    fn get_usage_by_store_type(
        &self,
        fid: u64,
        store_type: StoreType,
        txn_batch: &mut RocksDbTransactionBatch,
    ) -> Result<u32, StoresError> {
        let mut total_count = 0;
        for each_message_type in Limits::store_type_to_message_types(store_type) {
            let count = self
                .trie
                .get_count(
                    &self.db,
                    txn_batch,
                    &TrieKey::for_message_type(fid, each_message_type.into_u8()),
                )
                .map_err(|e| StoresError::StoreError {
                    inner: HubError::internal_db_error(&format!("Trie error: {}", e)),
                    hash: TrieKey::for_message_type(fid, each_message_type.into_u8()).to_vec(),
                })? as u32;
            total_count += count;
        }
        Ok(total_count)
    }

    pub fn is_pro_user(
        &self,
        fid: u64,
        block_timestamp: &FarcasterTime,
    ) -> Result<bool, OnchainEventStorageError> {
        Ok(self.onchain_event_store.is_tier_subscription_active_at(
            proto::TierType::Pro,
            fid,
            block_timestamp,
        )?)
    }

    pub fn get_storage_limits(&self, fid: u64) -> Result<StorageLimitsResponse, StoresError> {
        let txn_batch = &mut RocksDbTransactionBatch::new();
        let mut limits = vec![];
        let purchased_slot = self
            .onchain_event_store
            .get_storage_slot_for_fid(
                fid,
                self.network,
                &[],
                &StorageSlot::new(0, 0, 0, u32::MAX),
                &StorageSlot::new(0, 0, 0, u32::MAX),
            )
            .map_err(|e| StoresError::OnchainEventError(e))?;
        let borrowed_slot = StorageLendStore::get_borrowed_storage(&self.storage_lend_store, fid)
            .map_err(|err| StoresError::StoreError {
            inner: err,
            hash: vec![],
        })?;
        let lent_slot =
            StorageLendStore::get_lent_storage(&self.storage_lend_store, fid).map_err(|err| {
                StoresError::StoreError {
                    inner: err,
                    hash: vec![],
                }
            })?;
        let net_slot = self
            .onchain_event_store
            .get_storage_slot_for_fid(fid, self.network, &[], &lent_slot, &borrowed_slot)
            .map_err(|e| StoresError::OnchainEventError(e))?;
        for store_type in vec![
            StoreType::Casts,
            StoreType::Links,
            StoreType::Reactions,
            StoreType::UserData,
            StoreType::Verifications,
            StoreType::UsernameProofs,
            StoreType::StorageLends,
        ] {
            // You can't lend out borrowed storage. The limit is the number of units you've purchased.
            let slot = if store_type == StoreType::StorageLends {
                purchased_slot.clone()
            } else {
                net_slot.clone()
            };
            let used = self.get_usage_by_store_type(fid, store_type, txn_batch)?;
            // TODO(aditi): Should subtract 1 here for storage lends because we require keeping 1 storage unit available for revoking lent storage
            let max_messages = self.store_limits.max_messages(&slot, store_type);
            let name = match store_type {
                StoreType::None => "NONE",
                StoreType::Casts => "CASTS",
                StoreType::Links => "LINKS",
                StoreType::Reactions => "REACTIONS",
                StoreType::UserData => "USER_DATA",
                StoreType::Verifications => "VERIFICATIONS",
                StoreType::UsernameProofs => "USERNAME_PROOFS",
                StoreType::StorageLends => "STORAGE_LENDS",
            };
            let limit = StorageLimit {
                store_type: store_type.try_into().unwrap(),
                name: name.to_string(),
                limit: max_messages as u64,
                used: used as u64,
                earliest_timestamp: 0, // Deprecate?
                earliest_hash: vec![], // Deprecate?
            };
            limits.push(limit);
        }

        let response = StorageLimitsResponse {
            limits,
            units: net_slot.units_for(StorageUnitType::UnitTypeLegacy)
                + net_slot.units_for(StorageUnitType::UnitType2024)
                + net_slot.units_for(StorageUnitType::UnitType2025),
            unit_details: vec![
                StorageUnitDetails {
                    unit_type: StorageUnitType::UnitTypeLegacy as i32,
                    unit_size: net_slot.units_for(StorageUnitType::UnitTypeLegacy),
                    purchased_unit_size: purchased_slot.units_for(StorageUnitType::UnitTypeLegacy),
                    borrowed_unit_size: borrowed_slot.units_for(StorageUnitType::UnitTypeLegacy),
                    lent_unit_size: lent_slot.units_for(StorageUnitType::UnitTypeLegacy),
                },
                StorageUnitDetails {
                    unit_type: StorageUnitType::UnitType2024 as i32,
                    unit_size: net_slot.units_for(StorageUnitType::UnitType2024),
                    purchased_unit_size: purchased_slot.units_for(StorageUnitType::UnitType2024),
                    borrowed_unit_size: borrowed_slot.units_for(StorageUnitType::UnitType2024),
                    lent_unit_size: lent_slot.units_for(StorageUnitType::UnitType2024),
                },
                StorageUnitDetails {
                    unit_type: StorageUnitType::UnitType2025 as i32,
                    unit_size: net_slot.units_for(StorageUnitType::UnitType2025),
                    purchased_unit_size: purchased_slot.units_for(StorageUnitType::UnitType2025),
                    borrowed_unit_size: borrowed_slot.units_for(StorageUnitType::UnitType2025),
                    lent_unit_size: lent_slot.units_for(StorageUnitType::UnitType2025),
                },
            ],
            tier_subscriptions: vec![TierDetails {
                tier_type: TierType::Pro as i32,
                expires_at: self.onchain_event_store.tier_subscription_exires_at(
                    proto::TierType::Pro,
                    fid,
                    None,
                )?,
            }],
        };
        Ok(response)
    }

    pub fn revoke_messages(
        &self,
        fid: u64,
        key: &Vec<u8>,
        txn_batch: &mut RocksDbTransactionBatch,
    ) -> Result<Vec<HubEvent>, StoresError> {
        let mut revoke_events = Vec::new();
        // TODO: Dedup once we have a unified interface for stores
        revoke_events.extend(
            self.cast_store
                .revoke_messages_by_signer(fid, key, txn_batch)
                .map_err(|e| StoresError::StoreError {
                    inner: e,
                    hash: key.clone(),
                })?,
        );
        revoke_events.extend(
            self.link_store
                .revoke_messages_by_signer(fid, key, txn_batch)
                .map_err(|e| StoresError::StoreError {
                    inner: e,
                    hash: key.clone(),
                })?,
        );
        revoke_events.extend(
            self.reaction_store
                .revoke_messages_by_signer(fid, key, txn_batch)
                .map_err(|e| StoresError::StoreError {
                    inner: e,
                    hash: key.clone(),
                })?,
        );
        revoke_events.extend(
            self.user_data_store
                .revoke_messages_by_signer(fid, key, txn_batch)
                .map_err(|e| StoresError::StoreError {
                    inner: e,
                    hash: key.clone(),
                })?,
        );
        revoke_events.extend(
            self.verification_store
                .revoke_messages_by_signer(fid, key, txn_batch)
                .map_err(|e| StoresError::StoreError {
                    inner: e,
                    hash: key.clone(),
                })?,
        );
        Ok(revoke_events)
    }

    pub fn get_events(
        &self,
        start_id: u64,
        stop_id: Option<u64>,
        page_options: Option<PageOptions>,
    ) -> Result<EventsPage, HubError> {
        HubEvent::get_events(self.db.clone(), start_id, stop_id, page_options)
    }

    pub fn get_event(&self, event_id: u64) -> Result<HubEvent, HubError> {
        HubEvent::get_event(self.db.clone(), event_id)
    }

    pub fn get_next_height_by_timestamp(&self, timestamp: u64) -> Option<u64> {
        self.shard_store
            .get_next_height_by_timestamp(timestamp)
            .unwrap_or_else(|e| {
                error!(
                    "Error getting next height by timestamp for shard {}: {}",
                    self.shard_id, e
                );
                None
            })
    }

    pub async fn prune_events_until(
        &self,
        timestamp: u64,
        throttle: Duration,
        page_options: Option<PageOptions>,
    ) -> Result<u32, HubError> {
        if *(self.prune_lock.read().await) {
            info!(
                "Prune lock is already held for shard {}. Skipping prune.",
                self.shard_id
            );
            return Err(HubError::internal_db_error("pruning already running"));
        }
        {
            let mut prune_lock = self.prune_lock.write().await;
            *prune_lock = true;
        }
        let stop_height = self.get_next_height_by_timestamp(timestamp);

        let page_options = page_options.unwrap_or(PageOptions {
            page_size: Some(PAGE_SIZE_MAX),
            ..PageOptions::default()
        });

        let start = std::time::Instant::now();
        let count = if let Some(stop_height) = stop_height {
            info!(
                "Pruning events for shard {} older than timestamp: {}, height: {}",
                self.shard_id, timestamp, stop_height
            );
            let result =
                HubEvent::prune_events_util(self.db.clone(), stop_height, &page_options, throttle)
                    .await;
            if let Err(e) = &result {
                error!("Error pruning events for shard {}: {}", self.shard_id, e);
            }
            result.unwrap_or(0)
        } else {
            0
        };

        {
            let mut prune_lock = self.prune_lock.write().await;
            *prune_lock = false;
        }
        let elapsed = start.elapsed();
        info!(
            "Pruning events complete for shard {}. Pruned {} events in {} seconds",
            self.shard_id,
            count,
            elapsed.as_secs()
        );
        self.statsd
            .count_with_shard(self.shard_id, "prune.events", count as u64, vec![]);
        self.statsd.time_with_shard(
            self.shard_id,
            "prune.events_time_ms",
            elapsed.as_millis() as u64,
        );
        Ok(count)
    }

    pub async fn prune_shard_chunks_until(
        &self,
        timestamp: u64,
        throttle: Duration,
        page_options: Option<PageOptions>,
    ) -> Result<u32, HubError> {
        if *(self.prune_lock.read().await) {
            info!(
                "Prune lock is already held for shard {}. Skipping prune.",
                self.shard_id
            );
            return Err(HubError::internal_db_error("pruning already running"));
        }
        {
            let mut prune_lock = self.prune_lock.write().await;
            *prune_lock = true;
        }

        let stop_height = self.get_next_height_by_timestamp(timestamp);
        let start = std::time::Instant::now();

        let count = if let Some(stop_height) = stop_height {
            info!(
                "Pruning shard {} blocks older than timestamp: {}, height: {}",
                self.shard_id, timestamp, stop_height
            );
            let page_options = page_options.unwrap_or(PageOptions {
                page_size: Some(PAGE_SIZE_MAX),
                ..PageOptions::default()
            });
            self.shard_store
                .prune_until(stop_height, &page_options, throttle)
                .await
                .unwrap_or_else(|e| {
                    error!("Error pruning shard {} blocks: {}", self.shard_id, e);
                    0
                })
        } else {
            info!("No shard chunks to prune for shard {}", self.shard_id);
            0
        };

        {
            let mut prune_lock = self.prune_lock.write().await;
            *prune_lock = false;
        }

        let elapsed = start.elapsed();
        self.statsd.time_with_shard(
            self.shard_id,
            "prune.chunks_time_ms",
            elapsed.as_millis() as u64,
        );
        self.statsd
            .count_with_shard(self.shard_id, "prune.shard_chunks", count as u64, vec![]);
        info!(
            "Pruning shard chunks complete for shard {}. Pruned {} chunks in {} seconds",
            self.shard_id,
            count,
            elapsed.as_secs()
        );
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_limits() {
        let limits = Limits::default();
        assert_eq!(limits.casts, 100);
        assert_eq!(limits.links, 200);
        assert_eq!(limits.reactions, 200);
        assert_eq!(limits.user_data, 25);
        assert_eq!(limits.user_name_proofs, 2);
        assert_eq!(limits.verifications, 5);
    }

    #[test]
    fn test_2024_limits() {
        let limits = Limits::of_type(StorageUnitType::UnitType2024);
        assert_eq!(limits.casts, 2000);
        assert_eq!(limits.links, 1000);
        assert_eq!(limits.reactions, 1000);
        assert_eq!(limits.user_data, 50);
        assert_eq!(limits.user_name_proofs, 5);
        assert_eq!(limits.verifications, 25);
    }

    #[test]
    fn test_legacy_limits() {
        let limits = Limits::legacy();
        assert_eq!(limits.casts, 5000);
        assert_eq!(limits.links, 2500);
        assert_eq!(limits.reactions, 2500);
        assert_eq!(limits.user_data, 50);
        assert_eq!(limits.user_name_proofs, 5);
        assert_eq!(limits.verifications, 25);
    }

    #[test]
    fn test_limit_for_message() {
        let limits = Limits::default();
        assert_eq!(limits.for_message_type(MessageType::CastAdd), 100);
        assert_eq!(limits.for_message_type(MessageType::CastRemove), 100);
        assert_eq!(limits.for_message_type(MessageType::ReactionAdd), 200);
        assert_eq!(limits.for_message_type(MessageType::ReactionRemove), 200);
        assert_eq!(limits.for_message_type(MessageType::LinkCompactState), 200);
        assert_eq!(limits.for_message_type(MessageType::FrameAction), 0);
        assert_eq!(limits.for_message_type(MessageType::None), 0);
    }

    #[test]
    fn test_max_messages() {
        let store_limits = &StoreLimits::default();
        let limits_legacy = &store_limits.limits_legacy;
        let limits_2024 = &store_limits.limits_2024;
        let limits = &store_limits.limits_2025;
        let storage_slot_legacy = StorageSlot::new(1, 0, 0, 0);
        let storage_slot_2025 = StorageSlot::new(0, 0, 1, 0);
        let storage_slot_mixed = StorageSlot::new(3, 2, 1, 0);
        let storage_slot_none = StorageSlot::new(0, 0, 0, 0);

        assert_eq!(
            store_limits.max_messages(&storage_slot_2025, StoreType::Casts),
            limits.casts * 1
        );
        assert_eq!(
            store_limits.max_messages(&storage_slot_legacy, StoreType::Casts),
            limits_legacy.casts * 1
        );

        assert_eq!(
            store_limits.max_messages(&storage_slot_mixed, StoreType::Links),
            (limits_legacy.links * 3) + (limits_2024.links * 2) + (limits.links * 1)
        );

        assert_eq!(
            store_limits.max_messages(&storage_slot_none, StoreType::Links),
            0
        );
    }
}
