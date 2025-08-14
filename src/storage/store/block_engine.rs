use crate::consensus::proposer::ProposalSource;
use crate::core::{types::Height, util::FarcasterTime};
use crate::mempool::mempool::MempoolMessagesRequest;
use crate::proto::{
    block_event, Block, BlockEvent, BlockEventType, FarcasterNetwork, HeartbeatEventBody,
    ShardChunkWitness, Transaction,
};
use crate::storage::db::{RocksDB, RocksDbTransactionBatch};
use crate::storage::store::account::{BlockEventStore, OnchainEventStorageError};
use crate::storage::store::engine_metrics::Metrics;
use crate::storage::store::mempool_poller::{MempoolMessage, MempoolPoller, MempoolPollerError};
use crate::storage::store::BlockStore;
use crate::storage::trie::merkle_trie::{MerkleTrie, TrieKey};
use crate::storage::trie::{self};
use crate::utils::statsd_wrapper::StatsdClientWrapper;
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

    #[error(transparent)]
    OnchainEventError(#[from] OnchainEventStorageError),
}

pub struct BlockEngine {
    block_store: BlockStore,
    block_event_store: BlockEventStore,
    trie: MerkleTrie,
    pub mempool_poller: MempoolPoller,
    shard_id: u64,
    db: Arc<RocksDB>,
    metrics: Metrics,
    heartbeat_block_interval: u64,
}

// Shard state root and the transactions
#[derive(Clone)]
pub struct ShardStateChange {
    pub timestamp: FarcasterTime,
    pub new_state_root: Vec<u8>,
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
        }
    }

    pub(crate) fn replay_snapchain_txn(
        &mut self,
        snapchain_txn: &Transaction,
        txn_batch: &mut RocksDbTransactionBatch,
    ) -> Result<Vec<u8>, BlockEngineError> {
        // TODO(aditi): Fill this in, it's a no-op for now

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
    ) -> Vec<BlockEvent> {
        let mut events = vec![];
        if height.block_number % self.heartbeat_block_interval == 0 {
            let event_seqnum = self.block_event_store.max_seqnum().unwrap() + 1;
            let mut event = BlockEvent {
                seqnum: event_seqnum,
                hash: vec![],
                r#type: BlockEventType::Heartbeat as i32,
                block_number: height.block_number,
                event_index: events.len() as u64,
                block_timestamp: timestamp.to_u64(),
                body: Some(block_event::Body::HeartbeatEventBody(HeartbeatEventBody {})),
            };
            event.hash = blake3::hash(event.encode_to_vec().as_slice())
                .as_bytes()
                .to_vec();
            self.block_event_store.put_block_event(&event, txn).unwrap();
            events.push(event);
        }
        events
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

        let mut snapchain_txns = self
            .mempool_poller
            .create_transactions_from_mempool(messages)?;
        for snapchain_txn in &mut snapchain_txns {
            let account_root = self.replay_snapchain_txn(&snapchain_txn, txn_batch)?;
            snapchain_txn.account_root = account_root;
        }

        let events = self.generate_block_events(height, timestamp, txn_batch);

        self.metrics
            .publish_transaction_counts(&snapchain_txns, ProposalSource::Propose);

        let new_root_hash = self.trie.root_hash()?;
        let result = ShardStateChange {
            timestamp: timestamp.clone(),
            new_state_root: new_root_hash.clone(),
            transactions: snapchain_txns,
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
            let account_root = self.replay_snapchain_txn(snapchain_txn, txn_batch)?;
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

        let block_events = self.generate_block_events(height, timestamp, txn_batch);

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
            ProposalSource::Validate,
            height,
            &shard_state_change.timestamp,
        );

        let mut valid = true;

        match proposal_result {
            Err(ref err) => {
                error!("State change validation failed: {}", err);
                valid = false;
            }
            Ok(events) => {
                if events != shard_state_change.events {
                    valid = false;
                }
            }
        }

        self.trie.reload(&self.db).unwrap();
        let elapsed = now.elapsed();
        self.metrics
            .time_with_shard("validate_time", elapsed.as_millis() as u64);

        if valid {
            self.metrics.count("validate.true", 1, vec![]);
        } else {
            self.metrics.count("validate.false", 1, vec![]);
        }

        valid
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
