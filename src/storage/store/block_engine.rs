use crate::consensus::proposer::ProposalSource;
use crate::core::error::HubError;
use crate::core::validations;
use crate::core::{types::Height, util::FarcasterTime};
use crate::hyper::StateContext;
use crate::mempool::mempool::MempoolMessagesRequest;
use crate::proto::{
    self, block_event_data, Block, BlockEvent, BlockEventData, BlockEventType, FarcasterNetwork,
    HeartbeatEventBody, HubEvent, MergeMessageBody, MessageType, OnChainEvent, ShardChunkWitness,
    Transaction,
};
use crate::storage::db::{RocksDB, RocksDbTransactionBatch};
use crate::storage::store::account::{
    BlockEventStore, OnchainEventStorageError, OnchainEventStore, StorageLendStore,
    StorageLendStoreDef, StorageSlot, Store, StoreEventHandler,
};
use crate::storage::store::engine_metrics::Metrics;
use crate::storage::store::mempool_poller::{MempoolMessage, MempoolPoller, MempoolPollerError};
use crate::storage::store::BlockStore;
use crate::storage::trie::merkle_trie::{self, MerkleTrie, TrieKey};
use crate::storage::trie::{self};
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use crate::version::version::{EngineVersion, ProtocolFeature};
use itertools::Itertools;
use prost::Message;
use std::sync::Arc;
use std::u32;
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::{error, warn};

#[derive(Error, Debug)]
pub enum BlockEngineError {
    #[error(transparent)]
    TrieError(#[from] trie::errors::TrieError),

    #[error(transparent)]
    MempoolPollerError(#[from] MempoolPollerError),

    #[error("merkle trie root hash mismatch")]
    HashMismatch,

    #[error("events hash mismatch")]
    EventsHashMismatch,

    #[error(transparent)]
    OnchainEventError(#[from] OnchainEventStorageError),

    #[error(transparent)]
    HubError(#[from] HubError),
}
#[derive(Error, Debug, Clone)]
pub enum MessageValidationError {
    #[error("message has no data")]
    NoMessageData,

    #[error("unknown fid")]
    MissingFid,

    #[error("invalid signer")]
    MissingSigner,

    #[error("invalid message type")]
    InvalidMessageType,

    #[error("insufficient storage")]
    InsufficientStorage,

    #[error(transparent)]
    HubError(#[from] HubError),

    #[error(transparent)]
    MessageValidationError(#[from] validations::error::ValidationError),
}

#[derive(Clone)]
pub struct BlockStores {
    pub block_store: BlockStore,
    pub block_event_store: BlockEventStore,
    pub onchain_event_store: OnchainEventStore,
    pub storage_lend_store: Store<StorageLendStoreDef>,
    pub network: FarcasterNetwork,
    pub db: Arc<RocksDB>,
    pub trie: MerkleTrie,
    pub event_handler: Arc<StoreEventHandler>,
}

impl BlockStores {
    pub fn new(db: Arc<RocksDB>, trie: MerkleTrie, network: FarcasterNetwork) -> Self {
        let store_event_handler = StoreEventHandler::new();
        BlockStores {
            block_store: BlockStore::new(db.clone()),
            block_event_store: BlockEventStore { db: db.clone() },
            onchain_event_store: OnchainEventStore::new(db.clone(), store_event_handler.clone()),
            storage_lend_store: StorageLendStore::new(db.clone(), store_event_handler.clone(), 100),
            network,
            db: db.clone(),
            trie,
            event_handler: store_event_handler,
        }
    }

    pub fn with_state_context(&self, ctx: StateContext) -> Self {
        BlockStores {
            block_store: self.block_store.clone(),
            block_event_store: self.block_event_store.clone(),
            onchain_event_store: self.onchain_event_store.with_state_context(ctx),
            storage_lend_store: self.storage_lend_store.with_state_context(ctx),
            network: self.network,
            db: self.db.clone(),
            trie: self.trie.clone(),
            event_handler: self.event_handler.clone(),
        }
    }

    pub fn get_block_by_event_seqnum(&self, seqnum: u64) -> Option<Block> {
        let block_event = self
            .block_event_store
            .get_block_event_by_seqnum(seqnum)
            .ok()??;
        self.block_store
            .get_block_by_height(block_event.block_number())
            .ok()?
    }

    pub fn get_storage_slot_for_fid(
        &self,
        fid: u64,
        pending_onchain_events: &Vec<OnChainEvent>,
        count_lent_storage: bool,
        count_borrowed_storage: bool,
    ) -> Option<StorageSlot> {
        let lent_storage = if count_lent_storage {
            StorageLendStore::get_lent_storage(&self.storage_lend_store, fid).ok()?
        } else {
            StorageSlot::new(0, 0, 0, u32::MAX)
        };

        let borrowed_storage = if count_borrowed_storage {
            StorageLendStore::get_borrowed_storage(&self.storage_lend_store, fid).ok()?
        } else {
            StorageSlot::new(0, 0, 0, u32::MAX)
        };

        self.onchain_event_store
            .get_storage_slot_for_fid(
                fid,
                self.network,
                pending_onchain_events.as_slice(),
                &lent_storage,
                &borrowed_storage,
            )
            .ok()
    }
}

pub struct BlockEngine {
    stores: BlockStores,
    pub network: FarcasterNetwork,
    pub mempool_poller: MempoolPoller,
    shard_id: u64,
    db: Arc<RocksDB>,
    metrics: Metrics,
}

// Shard state root and the transactions
#[derive(Clone, Debug)]
pub struct BlockStateChange {
    pub timestamp: FarcasterTime,
    pub new_state_root: Vec<u8>,
    pub events_hash: Vec<u8>,
    pub transactions: Vec<Transaction>,
    pub events: Vec<BlockEvent>,
}

impl BlockEngine {
    pub fn new(
        mut trie: MerkleTrie,
        statsd_client: StatsdClientWrapper,
        db: Arc<RocksDB>,
        max_messages_per_block: u32,
        messages_request_tx: Option<mpsc::Sender<MempoolMessagesRequest>>,
        network: FarcasterNetwork,
    ) -> Self {
        trie.initialize(&db).unwrap();
        BlockEngine {
            stores: BlockStores::new(db.clone(), trie, network),
            shard_id: 0,
            mempool_poller: MempoolPoller {
                max_messages_per_block,
                messages_request_tx,
                network,
                shard_id: 0,
                statsd_client: statsd_client.clone(),
            },
            db,
            metrics: Metrics {
                statsd_client,
                shard_id: 0,
            },
            network,
        }
    }

    pub fn stores(&self) -> BlockStores {
        self.stores.clone()
    }

    pub fn stores_with_state_context(&self, ctx: StateContext) -> BlockStores {
        self.stores.with_state_context(ctx)
    }

    pub fn trie_root_hash(&self) -> Vec<u8> {
        self.stores.trie.root_hash().unwrap()
    }

    pub fn trie_key_exists(&mut self, ctx: &merkle_trie::Context, sync_id: &Vec<u8>) -> bool {
        self.stores
            .trie
            .exists(ctx, &self.db, sync_id.as_ref())
            .unwrap_or_else(|err| {
                error!("Error checking if sync id exists: {:?}", err);
                false
            })
    }

    fn set_height(&self, version: &EngineVersion, height: Height) {
        if version.is_enabled(ProtocolFeature::EventIdBugFix) {
            self.stores
                .event_handler
                .set_current_height(height.block_number);
        }
    }

    pub fn validate_user_message(
        &self,
        message: &proto::Message,
        storage_slot: &StorageSlot,
        timestamp: &FarcasterTime,
        version: EngineVersion,
        txn_batch: &mut RocksDbTransactionBatch,
    ) -> Result<(), MessageValidationError> {
        // Ensure message data is present
        let message_data = message
            .data
            .as_ref()
            .ok_or(MessageValidationError::NoMessageData)?;

        let is_pro_user = self
            .stores
            .onchain_event_store
            .is_tier_subscription_active_at(proto::TierType::Pro, message.fid(), timestamp)
            .map_err(|err| HubError::internal_db_error(&err.to_string()))?;

        validations::message::validate_message(
            message,
            self.network,
            is_pro_user,
            timestamp,
            version,
        )?;

        // 1. Check that the user has a custody address
        self.stores
            .onchain_event_store
            .get_id_register_event_by_fid(message_data.fid, Some(txn_batch))
            .map_err(|_| MessageValidationError::MissingFid)?
            .ok_or(MessageValidationError::MissingFid)?;

        // 2. Check that the user has a valid signer
        self.stores
            .onchain_event_store
            .get_active_signer(message_data.fid, message.signer.clone(), Some(txn_batch))
            .map_err(|_| MessageValidationError::MissingSigner)?
            .ok_or(MessageValidationError::MissingSigner)?;

        match message_data
            .body
            .as_ref()
            .ok_or(MessageValidationError::NoMessageData)?
        {
            crate::proto::message_data::Body::LendStorageBody(lend_storage) => {
                let total_storage_purchased = self
                    .stores
                    .get_storage_slot_for_fid(message_data.fid, &vec![], false, false)
                    .ok_or(MessageValidationError::InsufficientStorage)?;

                // Restricts who can lend storage to some reasonable set of users. Don't enforce this limit in devnet and testnet so we can test with fewer storage units.
                if self.network == FarcasterNetwork::Mainnet
                    && total_storage_purchased.units_for(lend_storage.unit_type()) < 1000
                {
                    return Err(MessageValidationError::InsufficientStorage);
                }

                let num_units_available = storage_slot.units_for(lend_storage.unit_type());
                let num_units_required =
                    if version.is_enabled(ProtocolFeature::StorageLendingLimitFix) {
                        // Retain 1 unit for the lender so the lender is able to revoke lent storage. There are a couple places that fail if the user has no active storage. Maintaining 1 storage unit is easier and safer than bypassing these validations for storage lends.
                        lend_storage.num_units + 1
                    } else {
                        lend_storage.num_units
                    };
                if num_units_available < num_units_required as u32 {
                    return Err(MessageValidationError::InsufficientStorage);
                }
            }
            _ => return Err(MessageValidationError::InvalidMessageType),
        }

        Ok(())
    }

    fn merge_message(
        &self,
        message: &proto::Message,
        txn_batch: &mut RocksDbTransactionBatch,
    ) -> Result<Vec<proto::HubEvent>, MessageValidationError> {
        match message.msg_type() {
            MessageType::LendStorage => Ok(StorageLendStore::merge(
                &self.stores.storage_lend_store,
                message,
                txn_batch,
            )?),
            _ => return Err(MessageValidationError::InvalidMessageType),
        }
    }

    fn on_merge_message(
        &mut self,
        storage_slot: &mut StorageSlot,
        merge_message_body: &MergeMessageBody,
    ) -> Result<(), BlockEngineError> {
        if let Some(added_message) = &merge_message_body.message {
            match added_message.data.as_ref().unwrap().body.as_ref().unwrap() {
                proto::message_data::Body::LendStorageBody(lend_storage_body) => {
                    storage_slot.sub(&StorageSlot::from_storage_lend(&lend_storage_body));
                }
                _ => {}
            }
        }

        for deleted_message in &merge_message_body.deleted_messages {
            match deleted_message
                .data
                .as_ref()
                .unwrap()
                .body
                .as_ref()
                .unwrap()
            {
                proto::message_data::Body::LendStorageBody(lend_storage_body) => {
                    storage_slot.merge(&StorageSlot::from_storage_lend(&lend_storage_body));
                }
                _ => {}
            }
        }

        Ok(())
    }

    pub(crate) fn replay_snapchain_txn(
        &mut self,
        trie_ctx: &merkle_trie::Context,
        snapchain_txn: &Transaction,
        txn_batch: &mut RocksDbTransactionBatch,
        timestamp: &FarcasterTime,
        version: EngineVersion,
    ) -> Result<(Vec<u8>, Vec<HubEvent>, Vec<MessageValidationError>), BlockEngineError> {
        let mut hub_events = vec![];
        let mut validation_errors = vec![];
        for message in &snapchain_txn.system_messages {
            if let Some(ref onchain_event) = message.on_chain_event {
                match self
                    .stores
                    .onchain_event_store
                    .merge_onchain_event(onchain_event.clone(), txn_batch)
                {
                    Ok(event) => {
                        hub_events.push(event);
                    }
                    Err(err) => {
                        // Duplicate error is expected
                        warn!("Unable to merge onchain event: {:#?}", err.to_string())
                    }
                }
            }
        }

        let mut storage_slot = self
            .storage_slot_for_transaction(snapchain_txn, true, false)
            .unwrap();

        for message in &snapchain_txn.user_messages {
            match self.validate_user_message(message, &storage_slot, timestamp, version, txn_batch)
            {
                Ok(()) => match message.msg_type() {
                    MessageType::LendStorage => {
                        if version.is_enabled(ProtocolFeature::StorageLending) {
                            if let Ok(events) = self.merge_message(message, txn_batch) {
                                for event in &events {
                                    match event.body.as_ref().unwrap() {
                                        proto::hub_event::Body::MergeMessageBody(
                                            merge_message_body,
                                        ) => self.on_merge_message(
                                            &mut storage_slot,
                                            &merge_message_body,
                                        )?,
                                        _ => {}
                                    }
                                }
                                hub_events.extend(events);
                            }
                        }
                    }
                    _ => {}
                },
                Err(err) => {
                    warn!(
                        fid = snapchain_txn.fid,
                        "Error merging message {}",
                        err.to_string()
                    );
                    validation_errors.push(err);
                }
            }
        }

        for event in &hub_events {
            self.stores
                .trie
                .update_for_event(trie_ctx, &self.db, &event, txn_batch)?;
        }

        let account_root =
            self.stores
                .trie
                .get_hash(&self.db, txn_batch, &TrieKey::for_fid(snapchain_txn.fid))?;

        Ok((account_root, hub_events, validation_errors))
    }

    fn heartbeat_block_interval(&self) -> u64 {
        match self.network {
            FarcasterNetwork::Devnet => 5,
            FarcasterNetwork::None | FarcasterNetwork::Testnet | FarcasterNetwork::Mainnet => 100,
        }
    }

    fn build_block_event(data: BlockEventData) -> BlockEvent {
        let hash = blake3::hash(data.encode_to_vec().as_slice())
            .as_bytes()
            .to_vec();
        BlockEvent {
            hash,
            data: Some(data),
        }
    }

    fn generate_block_events(
        &self,
        height: Height,
        timestamp: &FarcasterTime,
        hub_events: Vec<HubEvent>,
        txn: &mut RocksDbTransactionBatch,
    ) -> (Vec<BlockEvent>, Vec<u8>) {
        let mut events = vec![];
        let mut max_block_event_seqnum = self.stores.block_event_store.max_seqnum().unwrap();
        for hub_event in hub_events {
            match hub_event.body.unwrap() {
                proto::hub_event::Body::MergeMessageBody(merge_message_body) => {
                    if let Some(message) = merge_message_body.message {
                        match message.msg_type() {
                            MessageType::LendStorage => {
                                max_block_event_seqnum += 1;
                                let data = BlockEventData {
                                    seqnum: max_block_event_seqnum,
                                    r#type: BlockEventType::MergeMessage as i32,
                                    block_number: height.block_number,
                                    event_index: events.len() as u64,
                                    block_timestamp: timestamp.to_u64(),
                                    body: Some(block_event_data::Body::MergeMessageEventBody(
                                        proto::MergeMessageEventBody {
                                            message: Some(message),
                                        },
                                    )),
                                };
                                let event = Self::build_block_event(data);
                                events.push(event);
                            }
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }

        if height.block_number % self.heartbeat_block_interval() == 0 {
            max_block_event_seqnum += 1;
            let data = BlockEventData {
                seqnum: max_block_event_seqnum,
                r#type: BlockEventType::Heartbeat as i32,
                block_number: height.block_number,
                event_index: events.len() as u64,
                block_timestamp: timestamp.to_u64(),
                body: Some(block_event_data::Body::HeartbeatEventBody(
                    HeartbeatEventBody {},
                )),
            };
            let event = Self::build_block_event(data);
            // Store these events so
            // (1) It's possible to figuure out the max seqnum easily
            // (2) It's possible to query over them in an rpc and see what has been produced.
            events.push(event);
        }

        for event in events.iter() {
            self.stores
                .block_event_store
                .put_block_event(&event, txn)
                .unwrap();
        }

        let events_hash = if events.is_empty() {
            vec![]
        } else {
            let mut events_hasher = blake3::Hasher::new();
            for event in events.iter() {
                events_hasher.update(&event.hash);
            }
            events_hasher.finalize().as_bytes().to_vec()
        };

        (events, events_hash)
    }

    fn storage_slot_for_transaction(
        &self,
        snapchain_txn: &Transaction,
        count_lent_storage: bool,
        count_borrowed_storage: bool,
    ) -> Option<StorageSlot> {
        let pending_onchain_events: Vec<OnChainEvent> = snapchain_txn
            .system_messages
            .iter()
            .filter_map(|vm| vm.on_chain_event.clone())
            .collect();

        self.stores.get_storage_slot_for_fid(
            snapchain_txn.fid,
            &pending_onchain_events,
            count_lent_storage,
            count_borrowed_storage,
        )
    }

    fn prepare_proposal(
        &mut self,
        txn_batch: &mut RocksDbTransactionBatch,
        messages: Vec<MempoolMessage>,
        height: Height,
        timestamp: &FarcasterTime,
        version: EngineVersion,
    ) -> Result<BlockStateChange, BlockEngineError> {
        self.metrics.count(
            "recv_messages",
            messages.len() as u64,
            Metrics::proposal_source_tags(ProposalSource::Propose),
        );

        let mut snapchain_txns = MempoolPoller::create_transactions_from_mempool(messages)?
            .into_iter()
            .filter_map(|mut transaction| {
                // TODO(aditi): We could share this code with the shard engine but there may be other things we want to add here. For example, it may make sense to exclude validator messages and user messages that aren't intended for shard 0 here so a bug in the mempool won't impact the protocol in a significant way.
                let storage_slot = self.storage_slot_for_transaction(&transaction, true, true)?;

                // Drop events if storage slot is inactive
                if !storage_slot.is_active() {
                    transaction.user_messages = vec![];
                }

                if transaction.system_messages.is_empty() && transaction.user_messages.is_empty() {
                    return None;
                } else {
                    return Some(transaction);
                }
            })
            .collect_vec();

        self.set_height(&version, height);

        let mut all_hub_events = vec![];
        for snapchain_txn in &mut snapchain_txns {
            let (account_root, hub_events, _) = self.replay_snapchain_txn(
                &merkle_trie::Context::new(),
                &snapchain_txn,
                txn_batch,
                timestamp,
                version,
            )?;
            snapchain_txn.account_root = account_root;
            all_hub_events.extend_from_slice(&hub_events);
        }

        let (events, events_hash) =
            self.generate_block_events(height, timestamp, all_hub_events, txn_batch);

        self.metrics
            .publish_transaction_counts(&snapchain_txns, ProposalSource::Propose);

        let new_root_hash = self.stores.trie.root_hash()?;

        let result = BlockStateChange {
            timestamp: timestamp.clone(),
            new_state_root: new_root_hash.clone(),
            transactions: snapchain_txns,
            events_hash,
            events,
        };

        Ok(result)
    }

    pub fn propose_state_change(
        &mut self,
        messages: Vec<MempoolMessage>,
        height: Height,
        timestamp: Option<FarcasterTime>,
    ) -> BlockStateChange {
        let now = std::time::Instant::now();
        let mut txn = RocksDbTransactionBatch::new();

        let timestamp = timestamp.unwrap_or(FarcasterTime::current());
        let version = EngineVersion::version_for(&timestamp, self.network);
        let state_change = if version.is_enabled(ProtocolFeature::WriteDataToShardZero) {
            let result = self
                .prepare_proposal(&mut txn, messages, height, &timestamp, version)
                .unwrap();

            self.stores.trie.reload(&self.db).unwrap();
            result
        } else {
            BlockStateChange {
                events: vec![],
                new_state_root: vec![],
                timestamp: FarcasterTime::current(),
                events_hash: vec![],
                transactions: vec![],
            }
        };

        let proposal_duration = now.elapsed();
        self.metrics
            .time_with_shard("propose_time", proposal_duration.as_millis() as u64);

        self.metrics.count("propose.invoked", 1, vec![]);
        state_change
    }

    fn replay_proposal(
        &mut self,
        txn_batch: &mut RocksDbTransactionBatch,
        transactions: &[Transaction],
        shard_root: &[u8],
        events_hash: &Vec<u8>,
        source: ProposalSource,
        height: Height,
        timestamp: &FarcasterTime,
        version: EngineVersion,
    ) -> Result<(), BlockEngineError> {
        let now = std::time::Instant::now();
        // TODO(aditi): We probably only want to check this if we're in a test env (maybe only if the network is Devnet)
        // Validate that the trie is in a good place to start with
        match self.get_last_block() {
            None => { // There are places where it's hard to provide a parent hash-- e.g. tests so make this an option and skip validation if not present
            }
            Some(block) => match self.stores.trie.root_hash() {
                Err(err) => {
                    warn!(
                        source = source.to_string(),
                        "Unable to compute trie root hash {:#?}", err
                    )
                }
                Ok(root_hash) => {
                    let parent_shard_root = block.header.unwrap().state_root;
                    if root_hash != parent_shard_root {
                        warn!(
                            shard_id = self.shard_id,
                            our_shard_root = hex::encode(&root_hash),
                            parent_shard_root = hex::encode(parent_shard_root),
                            source = source.to_string(),
                            "Parent shard root mismatch"
                        );
                    }
                }
            },
        }

        self.set_height(&version, height);

        let mut all_hub_events = vec![];
        for snapchain_txn in transactions {
            let (account_root, hub_events, _) = self.replay_snapchain_txn(
                &merkle_trie::Context::new(),
                snapchain_txn,
                txn_batch,
                timestamp,
                version,
            )?;
            // Reject early if account roots fail to match (shard roots will definitely fail)
            if &account_root != &snapchain_txn.account_root {
                warn!(
                    fid = snapchain_txn.fid,
                    new_account_root = hex::encode(&account_root),
                    tx_account_root = hex::encode(&snapchain_txn.account_root),
                    source = source.to_string(),
                    num_system_messages = snapchain_txn.system_messages.len(),
                    num_user_messages = snapchain_txn.user_messages.len(),
                    "Account root mismatch"
                );
                return Err(BlockEngineError::HashMismatch);
            }

            all_hub_events.extend_from_slice(&hub_events);
        }

        let (_block_events, computed_events_hash) =
            self.generate_block_events(height, timestamp, all_hub_events, txn_batch);

        if computed_events_hash != *events_hash {
            warn!(
                shard_id = self.shard_id,
                expected_events_hash = hex::encode(events_hash),
                actual_events_hash = hex::encode(computed_events_hash),
                "Events hash mismatch"
            );
            return Err(BlockEngineError::EventsHashMismatch);
        }

        let root1 = self.stores.trie.root_hash()?;
        if &root1 != shard_root {
            warn!(
                shard_id = self.shard_id,
                new_shard_root = hex::encode(&root1),
                tx_shard_root = hex::encode(shard_root),
                source = source.to_string(),
                num_txns = transactions.len(),
                "Shard root mismatch"
            );
            return Err(BlockEngineError::HashMismatch);
        }

        let elapsed = now.elapsed();
        self.metrics
            .time_with_shard("replay_proposal_time", elapsed.as_millis() as u64);

        Ok(())
    }

    pub fn validate_state_change(
        &mut self,
        shard_state_change: &BlockStateChange,
        height: Height,
    ) -> bool {
        let version = EngineVersion::version_for(&shard_state_change.timestamp, self.network);
        if !version.is_enabled(ProtocolFeature::WriteDataToShardZero) {
            return true;
        }
        let mut txn = RocksDbTransactionBatch::new();

        let now = std::time::Instant::now();
        let transactions = &shard_state_change.transactions;
        let shard_root = &shard_state_change.new_state_root;

        let proposal_result = self.replay_proposal(
            &mut txn,
            transactions,
            shard_root,
            &shard_state_change.events_hash,
            ProposalSource::Validate,
            height,
            &shard_state_change.timestamp,
            version,
        );

        if let Err(ref err) = proposal_result {
            error!("State change validation failed: {}", err);
        }

        self.stores.trie.reload(&self.db).unwrap();
        let elapsed = now.elapsed();
        self.metrics
            .time_with_shard("validate_time", elapsed.as_millis() as u64);

        if proposal_result.is_ok() {
            self.metrics.count("validate.true", 1, vec![]);
        } else {
            self.metrics.count("validate.false", 1, vec![]);
        }

        proposal_result.is_ok()
    }

    pub fn commit_block(&mut self, block: &Block) {
        let height = block.header.as_ref().unwrap().height.unwrap();
        self.metrics.gauge("block_height", height.block_number);
        let block_timestamp = block.header.as_ref().unwrap().timestamp;
        // If block timestamp is ahead of current (only in tests), don't overflow
        self.metrics.gauge(
            "block_delay_seconds",
            FarcasterTime::current().to_u64().max(block_timestamp) - block_timestamp,
        );
        self.metrics.count(
            "block_shards",
            block
                .shard_witness
                .as_ref()
                .unwrap()
                .shard_chunk_witnesses
                .len() as u64,
            vec![],
        );

        let version =
            EngineVersion::version_for(&FarcasterTime::new(block_timestamp), self.network);
        if version.is_enabled(ProtocolFeature::WriteDataToShardZero) {
            let mut txn = RocksDbTransactionBatch::new();
            match self.replay_proposal(
                &mut txn,
                &block.transactions,
                &block.header.as_ref().unwrap().state_root,
                &block.header.as_ref().unwrap().events_hash,
                ProposalSource::Commit,
                height,
                &FarcasterTime::new(block_timestamp),
                version,
            ) {
                Err(err) => {
                    error!("State change commit failed: {}", err);
                    panic!("State change commit failed: {}", err);
                }
                Ok(()) => {
                    self.db.commit(txn).unwrap();
                    let result = self.stores.block_store.put_block(block);
                    if result.is_err() {
                        error!("Failed to store block: {:?}", result.err());
                    }
                    self.stores.trie.reload(&self.db).unwrap();
                    self.metrics
                        .publish_transaction_counts(&block.transactions, ProposalSource::Commit);
                    self.metrics.count(
                        "block_events",
                        block.events.len() as u64,
                        Metrics::proposal_source_tags(ProposalSource::Commit),
                    );
                    let max_block_event_seqnum =
                        self.stores.block_event_store.max_seqnum().unwrap();
                    self.metrics
                        .gauge("block_event_seqnum", max_block_event_seqnum);
                    // TODO(aditi): We need to add the post-commit hooks for replication for shard 0.
                }
            }
        } else {
            let result = self.stores.block_store.put_block(block);
            if result.is_err() {
                error!("Failed to store block: {:?}", result.err());
            }
        }
    }

    pub fn get_last_block(&self) -> Option<Block> {
        match self.stores.block_store.get_last_block() {
            Ok(block) => block,
            Err(err) => {
                error!("Unable to obtain last block {:#?}", err);
                None
            }
        }
    }

    pub fn current_state_root(&self) -> Vec<u8> {
        self.stores.trie.root_hash().unwrap_or_default()
    }

    pub fn get_block_by_height(&self, height: Height) -> Option<Block> {
        if height.shard_index != 0 {
            error!(
                shard_id = 0,
                requested_shard_id = height.shard_index,
                "Requested shard chunk from incorrect shard"
            );

            return None;
        }
        match self
            .stores
            .block_store
            .get_block_by_height(height.block_number)
        {
            Ok(block) => block,
            Err(err) => {
                error!("No block at height {:#?}", err);
                None
            }
        }
    }

    pub fn get_confirmed_height(&self) -> Height {
        let shard_index = 0;
        match self.stores.block_store.max_block_number() {
            Ok(block_num) => Height::new(shard_index, block_num),
            Err(_) => Height::new(shard_index, 0),
        }
    }

    pub fn get_min_height(&self) -> Height {
        let shard_index = 0;
        match self.stores.block_store.min_block_number() {
            Ok(block_num) => Height::new(shard_index, block_num),
            // In case of no blocks, return height 1
            Err(_) => Height::new(shard_index, 1),
        }
    }

    pub fn get_last_shard_witness(
        &self,
        height: Height,
        shard_id: u32,
    ) -> Option<ShardChunkWitness> {
        let previous_height = height.decrement()?;
        let previous_block = self.get_block_by_height(previous_height)?;
        let previous_shard_witness = previous_block.shard_witness?;
        previous_shard_witness
            .shard_chunk_witnesses
            .iter()
            .find(|witness| witness.height.unwrap().shard_index == shard_id)
            .cloned()
    }

    pub fn simulate_message(
        &mut self,
        message: &proto::Message,
    ) -> Result<(), MessageValidationError> {
        let mut txn = RocksDbTransactionBatch::new();
        let snapchain_txn = Transaction {
            fid: message.fid() as u64,
            account_root: vec![],
            system_messages: vec![],
            user_messages: vec![message.clone()],
        };
        let version = EngineVersion::current(self.network);
        let (_, _, errors) = self
            .replay_snapchain_txn(
                &merkle_trie::Context::new(),
                &snapchain_txn,
                &mut txn,
                &FarcasterTime::current(),
                version,
            )
            .map_err(|err| {
                MessageValidationError::HubError(HubError::invalid_internal_state(&err.to_string()))
            })?;

        self.stores.trie.reload(&self.db).map_err(|e| {
            MessageValidationError::HubError(HubError::invalid_internal_state(&e.to_string()))
        })?;

        if !errors.is_empty() {
            return Err(errors[0].clone());
        } else {
            Ok(())
        }
    }
}
