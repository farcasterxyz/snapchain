use super::account::{
    EventsPage, ReactionStore, ReactionStoreDef, UserDataStore, UserDataStoreDef,
    VerificationStore, VerificationStoreDef,
};
use crate::core::error::HubError;
use crate::proto::MessageType;
use crate::proto::{
    HubEvent, StorageLimit, StorageLimitsResponse, StorageUnitDetails, StorageUnitType, StoreType,
};
use crate::storage::constants::PAGE_SIZE_MAX;
use crate::storage::db::{PageOptions, RocksDB, RocksDbTransactionBatch};
use crate::storage::store::account::{
    CastStore, CastStoreDef, IntoU8, LinkStore, OnchainEventStorageError, OnchainEventStore, Store,
    StoreEventHandler, UsernameProofStore, UsernameProofStoreDef,
};
use crate::storage::store::shard::ShardStore;
use crate::storage::trie::merkle_trie;
use crate::storage::trie::merkle_trie::TrieKey;
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
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
    pub shard_store: ShardStore,
    pub cast_store: Store<CastStoreDef>,
    pub link_store: Store<LinkStore>,
    pub reaction_store: Store<ReactionStoreDef>,
    pub user_data_store: Store<UserDataStoreDef>,
    pub verification_store: Store<VerificationStoreDef>,
    pub onchain_event_store: OnchainEventStore,
    pub username_proof_store: Store<UsernameProofStoreDef>,
    pub(crate) db: Arc<RocksDB>,
    pub(crate) trie: merkle_trie::MerkleTrie,
    pub store_limits: StoreLimits,
    pub event_handler: Arc<StoreEventHandler>,
    pub shard_id: u32,
    pub statsd: StatsdClientWrapper,
}

#[derive(Clone, Debug)]
pub struct Limits {
    pub casts: u32,
    pub links: u32,
    pub reactions: u32,
    pub user_data: u32,
    pub user_name_proofs: u32,
    pub verifications: u32,
}

impl Limits {
    pub fn default() -> Limits {
        Limits {
            casts: 2000,
            links: 1000,
            reactions: 1000,
            user_data: 50,
            user_name_proofs: 5,
            verifications: 25,
        }
    }

    pub fn legacy() -> Limits {
        Limits {
            casts: 5000,
            links: 2500,
            reactions: 2500,
            user_data: 50,
            user_name_proofs: 5,
            verifications: 25,
        }
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
            StoreType::None => 0,
        }
    }
}

#[derive(Clone, Debug)]
pub struct StoreLimits {
    pub limits: Limits,
    pub legacy_limits: Limits,
}

impl StoreLimits {
    pub fn max_messages(&self, units: u32, legacy_units: u32, store_type: StoreType) -> u32 {
        units * self.limits.for_store_type(store_type)
            + legacy_units * self.legacy_limits.for_store_type(store_type)
    }
}

impl StoreLimits {
    pub fn default() -> StoreLimits {
        StoreLimits {
            limits: Limits::default(),
            legacy_limits: Limits::legacy(),
        }
    }
}

impl Stores {
    pub fn new(
        db: Arc<RocksDB>,
        shard_id: u32,
        mut trie: merkle_trie::MerkleTrie,
        store_limits: StoreLimits,
        statsd: StatsdClientWrapper,
    ) -> Stores {
        trie.initialize(&db).unwrap();

        let event_handler = StoreEventHandler::new();
        let shard_store = ShardStore::new(db.clone(), shard_id);
        let cast_store = CastStore::new(db.clone(), event_handler.clone(), 100);
        let link_store = LinkStore::new(db.clone(), event_handler.clone(), 100);
        let reaction_store = ReactionStore::new(db.clone(), event_handler.clone(), 100);
        let user_data_store = UserDataStore::new(db.clone(), event_handler.clone(), 100);
        let verification_store = VerificationStore::new(db.clone(), event_handler.clone(), 100);
        let onchain_event_store = OnchainEventStore::new(db.clone(), event_handler.clone());
        let username_proof_store = UsernameProofStore::new(db.clone(), event_handler.clone(), 100);
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
            db: db.clone(),
            store_limits,
            event_handler,
            statsd,
        }
    }

    pub fn get_usage(
        &self,
        fid: u64,
        message_type: MessageType,
        txn_batch: &mut RocksDbTransactionBatch,
    ) -> Result<(u32, u32), StoresError> {
        let store_type = Limits::message_type_to_store_type(message_type);
        let message_count = self.get_usage_by_store_type(fid, store_type, txn_batch);
        let slot = self
            .onchain_event_store
            .get_storage_slot_for_fid(fid)
            .map_err(|e| StoresError::OnchainEventError(e))?;
        let max_messages =
            self.store_limits
                .max_messages(slot.units, slot.legacy_units, store_type);

        Ok((message_count, max_messages))
    }

    // Usage is defined at the store level, but the trie accounts for message by type, this function maps between the two
    fn get_usage_by_store_type(
        &self,
        fid: u64,
        store_type: StoreType,
        txn_batch: &mut RocksDbTransactionBatch,
    ) -> u32 {
        let mut total_count = 0;
        for each_message_type in Limits::store_type_to_message_types(store_type) {
            let count = self.trie.get_count(
                &self.db,
                txn_batch,
                &TrieKey::for_message_type(fid, each_message_type.into_u8()),
            ) as u32;
            total_count += count;
        }
        total_count
    }

    pub fn get_storage_limits(&self, fid: u64) -> Result<StorageLimitsResponse, StoresError> {
        let slot = self
            .onchain_event_store
            .get_storage_slot_for_fid(fid)
            .map_err(|e| StoresError::OnchainEventError(e))?;

        let txn_batch = &mut RocksDbTransactionBatch::new();
        let mut limits = vec![];
        for store_type in vec![
            StoreType::Casts,
            StoreType::Links,
            StoreType::Reactions,
            StoreType::UserData,
            StoreType::Verifications,
            StoreType::UsernameProofs,
        ] {
            let used = self.get_usage_by_store_type(fid, store_type, txn_batch);
            let max_messages =
                self.store_limits
                    .max_messages(slot.units, slot.legacy_units, store_type);
            let name = match store_type {
                StoreType::None => "NONE",
                StoreType::Casts => "CASTS",
                StoreType::Links => "LINKS",
                StoreType::Reactions => "REACTIONS",
                StoreType::UserData => "USER_DATA",
                StoreType::Verifications => "VERIFICATIONS",
                StoreType::UsernameProofs => "USERNAME_PROOFS",
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
            units: slot.units + slot.legacy_units,
            unit_details: vec![
                StorageUnitDetails {
                    unit_type: StorageUnitType::UnitTypeLegacy as i32,
                    unit_size: slot.legacy_units,
                },
                StorageUnitDetails {
                    unit_type: StorageUnitType::UnitType2024 as i32,
                    unit_size: slot.units,
                },
            ],
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
        let stop_height = self.get_next_height_by_timestamp(timestamp);

        let page_options = page_options.unwrap_or(PageOptions {
            page_size: Some(PAGE_SIZE_MAX),
            ..PageOptions::default()
        });

        if let Some(stop_height) = stop_height {
            info!(
                "Pruning events for shard {} older than timestamp: {}, height: {}",
                self.shard_id, timestamp, stop_height
            );
            let start = std::time::Instant::now();
            let count =
                HubEvent::prune_events_util(self.db.clone(), stop_height, &page_options, throttle)
                    .await?;
            let elapsed = start.elapsed();
            self.statsd
                .count_with_shard(self.shard_id, "prune.events", count as u64);
            self.statsd.time_with_shard(
                self.shard_id,
                "prune.events_time_ms",
                elapsed.as_millis() as u64,
            );
            info!(
                "Pruning events complete for shard {}. Pruned {} events in {} seconds",
                self.shard_id,
                count,
                elapsed.as_secs()
            );
            Ok(count)
        } else {
            info!("No events to prune for shard {}", self.shard_id);
            self.statsd
                .count_with_shard(self.shard_id, "prune.events", 0);
            Ok(0)
        }
    }

    pub async fn prune_shard_chunks_until(
        &self,
        timestamp: u64,
        throttle: Duration,
        page_options: Option<PageOptions>,
    ) -> Result<u32, HubError> {
        let stop_height = self.get_next_height_by_timestamp(timestamp);
        if let Some(stop_height) = stop_height {
            info!(
                "Pruning shard {} blocks older than timestamp: {}, height: {}",
                self.shard_id, timestamp, stop_height
            );
            let page_options = page_options.unwrap_or(PageOptions {
                page_size: Some(PAGE_SIZE_MAX),
                ..PageOptions::default()
            });

            let start = std::time::Instant::now();
            let count = self
                .shard_store
                .prune_until(stop_height, &page_options, throttle)
                .await
                .unwrap_or_else(|e| {
                    error!("Error pruning shard {} blocks: {}", self.shard_id, e);
                    0
                });
            let elapsed = start.elapsed();
            self.statsd.time_with_shard(
                self.shard_id,
                "prune.chunks_time_ms",
                elapsed.as_millis() as u64,
            );
            self.statsd
                .count_with_shard(self.shard_id, "prune.shard_chunks", count as u64);
            info!(
                "Pruning shard chunks complete for shard {}. Pruned {} chunks in {} seconds",
                self.shard_id,
                count,
                elapsed.as_secs()
            );
            Ok(count)
        } else {
            info!("No shard chunks to prune for shard {}", self.shard_id);
            Ok(0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_limits() {
        let limits = Limits::default();
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
        assert_eq!(limits.for_message_type(MessageType::CastAdd), 2000);
        assert_eq!(limits.for_message_type(MessageType::CastRemove), 2000);
        assert_eq!(limits.for_message_type(MessageType::ReactionAdd), 1000);
        assert_eq!(limits.for_message_type(MessageType::ReactionRemove), 1000);
        assert_eq!(limits.for_message_type(MessageType::LinkCompactState), 1000);
        assert_eq!(limits.for_message_type(MessageType::FrameAction), 0);
        assert_eq!(limits.for_message_type(MessageType::None), 0);
    }

    #[test]
    fn test_max_messages() {
        let store_limits = &StoreLimits::default();
        let legacy_limits = &store_limits.legacy_limits;
        let limits = &store_limits.limits;
        assert_eq!(
            store_limits.max_messages(1, 0, StoreType::Casts),
            limits.casts * 1
        );
        assert_eq!(
            store_limits.max_messages(0, 1, StoreType::Casts),
            legacy_limits.casts * 1
        );

        assert_eq!(
            store_limits.max_messages(3, 2, StoreType::Links),
            (limits.links * 3) + (legacy_limits.links * 2)
        );

        assert_eq!(store_limits.max_messages(0, 0, StoreType::Links), 0);
    }
}
