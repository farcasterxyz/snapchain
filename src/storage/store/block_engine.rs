use crate::consensus::proposer::ProposalSource;
use crate::core::{types::Height, util::FarcasterTime};
use crate::mempool::mempool::MempoolMessagesRequest;
use crate::proto::{
    block_event_data, Block, BlockEvent, BlockEventData, BlockEventType, FarcasterNetwork,
    HeartbeatEventBody, OnChainEvent, ShardChunkWitness, Transaction,
};
use crate::storage::db::{RocksDB, RocksDbTransactionBatch};
use crate::storage::store::account::{
    BlockEventStore, OnchainEventStorageError, OnchainEventStore, StoreEventHandler,
};
use crate::storage::store::engine_metrics::Metrics;
use crate::storage::store::mempool_poller::{MempoolMessage, MempoolPoller, MempoolPollerError};
use crate::storage::store::BlockStore;
use crate::storage::trie::merkle_trie::{self, MerkleTrie, TrieKey};
use crate::storage::trie::{self};
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use blake3::Hash;
use itertools::Itertools;
use prost::Message;
use std::sync::Arc;
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
}

pub struct BlockEngine {
    block_store: BlockStore,
    block_event_store: BlockEventStore,
    pub onchain_event_store: OnchainEventStore,
    trie: MerkleTrie,
    network: FarcasterNetwork,
    pub mempool_poller: MempoolPoller,
    shard_id: u64,
    db: Arc<RocksDB>,
    metrics: Metrics,
    heartbeat_block_interval: u64,
}

// Shard state root and the transactions
#[derive(Clone, Debug)]
pub struct ShardStateChange {
    pub timestamp: FarcasterTime,
    pub new_state_root: Vec<u8>,
    pub events_hash: Vec<u8>,
    pub transactions: Vec<Transaction>,
    pub events: Vec<BlockEvent>,
}

impl BlockEngine {
    pub fn new(
        block_store: BlockStore,
        mut trie: MerkleTrie,
        statsd_client: StatsdClientWrapper,
        db: Arc<RocksDB>,
        max_messages_per_block: u32,
        messages_request_tx: Option<mpsc::Sender<MempoolMessagesRequest>>,
        network: FarcasterNetwork,
        heartbeat_block_interval: u64,
    ) -> Self {
        trie.initialize(&db).unwrap();
        BlockEngine {
            block_store,
            block_event_store: BlockEventStore { db: db.clone() },
            onchain_event_store: OnchainEventStore::new(db.clone(), StoreEventHandler::new()),
            trie,
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
            heartbeat_block_interval,
            network,
        }
    }

    #[cfg(test)]
    pub fn trie_root_hash(&self) -> Vec<u8> {
        self.trie.root_hash().unwrap()
    }

    pub fn trie_key_exists(&mut self, ctx: &merkle_trie::Context, sync_id: &Vec<u8>) -> bool {
        self.trie
            .exists(ctx, &self.db, sync_id.as_ref())
            .unwrap_or_else(|err| {
                error!("Error checking if sync id exists: {:?}", err);
                false
            })
    }

    pub(crate) fn replay_snapchain_txn(
        &mut self,
        trie_ctx: &merkle_trie::Context,
        snapchain_txn: &Transaction,
        txn_batch: &mut RocksDbTransactionBatch,
    ) -> Result<Vec<u8>, BlockEngineError> {
        // TODO(aditi): Fill this in, it's a no-op for now
        for message in &snapchain_txn.system_messages {
            if let Some(ref onchain_event) = message.on_chain_event {
                match self
                    .onchain_event_store
                    .merge_onchain_event(onchain_event.clone(), txn_batch)
                {
                    Ok(event) => self
                        .trie
                        .update_for_event(trie_ctx, &self.db, &event, txn_batch)?,
                    Err(err) => {
                        error!("Unable to merge onchain event: {:#?}", err.to_string())
                    }
                }
            }
        }

        let account_root =
            self.trie
                .get_hash(&self.db, txn_batch, &TrieKey::for_fid(snapchain_txn.fid));

        Ok(account_root)
    }

    fn generate_block_events(
        &self,
        height: Height,
        timestamp: &FarcasterTime,
        txn: &mut RocksDbTransactionBatch,
    ) -> (Vec<BlockEvent>, Hash) {
        let mut events = vec![];
        if height.block_number % self.heartbeat_block_interval == 0 {
            let event_seqnum = self.block_event_store.max_seqnum().unwrap() + 1;
            let data = BlockEventData {
                seqnum: event_seqnum,
                r#type: BlockEventType::Heartbeat as i32,
                block_number: height.block_number,
                event_index: events.len() as u64,
                block_timestamp: timestamp.to_u64(),
                body: Some(block_event_data::Body::HeartbeatEventBody(
                    HeartbeatEventBody {},
                )),
            };
            let hash = blake3::hash(data.encode_to_vec().as_slice())
                .as_bytes()
                .to_vec();
            let event = BlockEvent {
                hash,
                data: Some(data),
            };
            // Store these events so
            // (1) It's possible to figuure out the max seqnum easily
            // (2) It's possible to query over them in an rpc and see what has been produced.
            self.block_event_store.put_block_event(&event, txn).unwrap();
            events.push(event);
        }

        let mut events_hasher = blake3::Hasher::new();
        for event in events.iter() {
            events_hasher.update(&event.hash);
        }
        let events_hash = events_hasher.finalize();

        (events, events_hash)
    }

    fn prepare_proposal(
        &mut self,
        txn_batch: &mut RocksDbTransactionBatch,
        messages: Vec<MempoolMessage>,
        height: Height,
        timestamp: &FarcasterTime,
    ) -> Result<ShardStateChange, BlockEngineError> {
        self.metrics.count(
            "recv_messages",
            messages.len() as u64,
            Metrics::proposal_source_tags(ProposalSource::Propose),
        );

        let mut snapchain_txns = MempoolPoller::create_transactions_from_mempool(messages)?
            .into_iter()
            .filter_map(|mut transaction| {
                let pending_onchain_events: Vec<OnChainEvent> = transaction
                    .system_messages
                    .iter()
                    .filter_map(|vm| vm.on_chain_event.clone())
                    .collect();

                let storage_slot = self
                    .onchain_event_store
                    .get_storage_slot_for_fid(
                        transaction.fid,
                        self.network,
                        pending_onchain_events.as_slice(),
                    )
                    .ok()?;

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

        for snapchain_txn in &mut snapchain_txns {
            let account_root =
                self.replay_snapchain_txn(&merkle_trie::Context::new(), &snapchain_txn, txn_batch)?;
            snapchain_txn.account_root = account_root;
        }

        let (events, events_hash) = self.generate_block_events(height, timestamp, txn_batch);

        self.metrics
            .publish_transaction_counts(&snapchain_txns, ProposalSource::Propose);

        let new_root_hash = self.trie.root_hash()?;

        let result = ShardStateChange {
            timestamp: timestamp.clone(),
            new_state_root: new_root_hash.clone(),
            transactions: snapchain_txns,
            events_hash: events_hash.as_bytes().to_vec(),
            events,
        };

        Ok(result)
    }

    pub fn propose_state_change(
        &mut self,
        messages: Vec<MempoolMessage>,
        height: Height,
    ) -> ShardStateChange {
        let now = std::time::Instant::now();
        let mut txn = RocksDbTransactionBatch::new();

        let timestamp = FarcasterTime::current();
        let result = self
            .prepare_proposal(&mut txn, messages, height, &timestamp)
            .unwrap(); //TODO: don't unwrap()

        // TODO: this should probably operate automatically via drop trait
        self.trie.reload(&self.db).unwrap();

        let proposal_duration = now.elapsed();
        self.metrics
            .time_with_shard("propose_time", proposal_duration.as_millis() as u64);

        self.metrics.count("propose.invoked", 1, vec![]);
        result
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
    ) -> Result<Vec<BlockEvent>, BlockEngineError> {
        let now = std::time::Instant::now();
        // Validate that the trie is in a good place to start with
        match self.get_last_block() {
            None => { // There are places where it's hard to provide a parent hash-- e.g. tests so make this an option and skip validation if not present
            }
            Some(block) => match self.trie.root_hash() {
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

        for snapchain_txn in transactions {
            let account_root =
                self.replay_snapchain_txn(&merkle_trie::Context::new(), snapchain_txn, txn_batch)?;
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
        }

        let (block_events, computed_events_hash) =
            self.generate_block_events(height, timestamp, txn_batch);

        if computed_events_hash.as_bytes().to_vec() != *events_hash {
            warn!(
                shard_id = self.shard_id,
                expected_events_hash = hex::encode(events_hash),
                actual_events_hash = hex::encode(computed_events_hash.as_bytes().to_vec()),
                "Events hash mismatch"
            );
            return Err(BlockEngineError::EventsHashMismatch);
        }

        let root1 = self.trie.root_hash()?;
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

        Ok(block_events)
    }

    pub fn validate_state_change(
        &mut self,
        shard_state_change: &ShardStateChange,
        height: Height,
    ) -> bool {
        let mut txn = RocksDbTransactionBatch::new();

        let now = std::time::Instant::now();
        let transactions = &shard_state_change.transactions;
        let shard_root = &shard_state_change.new_state_root;
        // We ignore the events here, we don't know what they are yet. If state roots match, the events should match

        let proposal_result = self.replay_proposal(
            &mut txn,
            transactions,
            shard_root,
            &shard_state_change.events_hash,
            ProposalSource::Validate,
            height,
            &shard_state_change.timestamp,
        );

        match proposal_result {
            Err(ref err) => {
                error!("State change validation failed: {}", err);
            }
            Ok(ref _events) => {}
        }

        self.trie.reload(&self.db).unwrap();
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
        self.metrics.gauge(
            "block_delay_seconds",
            FarcasterTime::current().to_u64() - block_timestamp,
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

        let mut txn = RocksDbTransactionBatch::new();
        match self.replay_proposal(
            &mut txn,
            &block.transactions,
            &block.header.as_ref().unwrap().state_root,
            &block.header.as_ref().unwrap().events_hash,
            ProposalSource::Commit,
            height,
            &FarcasterTime::new(block_timestamp),
        ) {
            Err(err) => {
                error!("State change commit failed: {}", err);
                panic!("State change commit failed: {}", err);
            }
            Ok(_events) => {
                self.db.commit(txn).unwrap();
                let result = self.block_store.put_block(block);
                if result.is_err() {
                    error!("Failed to store block: {:?}", result.err());
                }
                self.trie.reload(&self.db).unwrap();
                // TODO(aditi): We need to add the post-commit hooks for replication for shard 0.
            }
        }
    }

    pub fn get_last_block(&self) -> Option<Block> {
        match self.block_store.get_last_block() {
            Ok(block) => block,
            Err(err) => {
                error!("Unable to obtain last block {:#?}", err);
                None
            }
        }
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
        match self.block_store.get_block_by_height(height.block_number) {
            Ok(block) => block,
            Err(err) => {
                error!("No block at height {:#?}", err);
                None
            }
        }
    }

    pub fn get_confirmed_height(&self) -> Height {
        let shard_index = 0;
        match self.block_store.max_block_number() {
            Ok(block_num) => Height::new(shard_index, block_num),
            Err(_) => Height::new(shard_index, 0),
        }
    }

    pub fn get_min_height(&self) -> Height {
        let shard_index = 0;
        match self.block_store.min_block_number() {
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
}
