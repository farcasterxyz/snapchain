use super::account::{OnchainEventStorageError, OnchainEventsPage};
use super::shard::ShardStore;
use crate::core::error::HubError;
use crate::core::types::{proto, Height};
use crate::proto::onchain_event::{OnChainEvent, OnChainEventType};
use crate::proto::{msg as message, snapchain};
use crate::storage::db;
use crate::storage::db::{PageOptions, RocksDB, RocksDbTransactionBatch};
use crate::storage::store::account::{
    CastStore, MessagesPage, OnchainEventStore, Store, StoreEventHandler,
};
use crate::storage::store::BlockStore;
use crate::storage::trie;
use crate::storage::trie::merkle_trie;
use message::MessageType;
use snapchain::{Block, ShardChunk, Transaction};
use std::iter;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::{error, warn};

#[derive(Error, Debug)]
enum EngineError {
    #[error(transparent)]
    TrieError(#[from] trie::errors::TrieError),

    #[error("store error")]
    StoreError {
        inner: HubError, // TODO: move away from HubError when we can
        hash: Vec<u8>,
    },

    #[error("unsupported message type")]
    UnsupportedMessageType(MessageType),

    #[error("merkle trie root hash mismatch")]
    HashMismatch,

    #[error("message has no data")]
    NoMessageData,

    #[error("invalid message type")]
    InvalidMessageType,

    #[error("message receive error")]
    MessageReceiveError(#[from] mpsc::error::TryRecvError),

    #[error(transparent)]
    MergeOnchainEventError(#[from] OnchainEventStorageError),
}

impl EngineError {
    pub fn new_store_error(hash: Vec<u8>) -> impl FnOnce(HubError) -> Self {
        move |inner: HubError| EngineError::StoreError { inner, hash }
    }
}

#[derive(Clone)]
pub enum Message {
    UserMessage(message::Message),
    ValidatorMessage(snapchain::ValidatorMessage),
}

// Shard state root and the transactions
#[derive(Clone)]
pub struct ShardStateChange {
    pub shard_id: u32,
    pub new_state_root: Vec<u8>,
    pub transactions: Vec<Transaction>,
}

pub struct ShardEngine {
    shard_id: u32,
    shard_store: ShardStore,
    messages_rx: mpsc::Receiver<Message>,
    messages_tx: mpsc::Sender<Message>,
    trie: merkle_trie::MerkleTrie,
    cast_store: Store,
    pub db: Arc<RocksDB>,
    onchain_event_store: OnchainEventStore,
}

fn encode_vec(data: &[Vec<u8>]) -> String {
    data.iter()
        .map(|vec| hex::encode(vec))
        .collect::<Vec<String>>()
        .join(", ")
}

impl ShardEngine {
    pub fn new(shard_id: u32, shard_store: ShardStore) -> ShardEngine {
        let db = &*shard_store.db;

        // TODO: adding the trie here introduces many calls that want to return errors. Rethink unwrap strategy.
        let mut txn_batch = RocksDbTransactionBatch::new();
        let mut trie = merkle_trie::MerkleTrie::new();
        trie.initialize(db, &mut txn_batch).unwrap();

        // TODO: The empty trie currently has some issues with the newly added commit/rollback code. Remove when we can.
        trie.insert(db, &mut txn_batch, vec![vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])
            .unwrap();
        db.commit(txn_batch).unwrap();
        trie.reload(db).unwrap();

        let event_handler = StoreEventHandler::new(None, None, None);
        let db = shard_store.db.clone();
        let cast_store = CastStore::new(shard_store.db.clone(), event_handler.clone(), 100);
        let onchain_event_store =
            OnchainEventStore::new(shard_store.db.clone(), event_handler.clone());

        let (messages_tx, messages_rx) = mpsc::channel::<Message>(10_000);
        ShardEngine {
            shard_id,
            shard_store,
            messages_rx,
            messages_tx,
            trie,
            cast_store,
            db,
            onchain_event_store,
        }
    }

    pub fn messages_tx(&self) -> mpsc::Sender<Message> {
        self.messages_tx.clone()
    }

    pub(crate) fn trie_root_hash(&self) -> Vec<u8> {
        self.trie.root_hash().unwrap()
    }

    fn prepare_proposal(
        &mut self,
        txn_batch: &mut RocksDbTransactionBatch,
        shard_id: u32,
    ) -> Result<ShardStateChange, EngineError> {
        let mut messages = Vec::new();

        loop {
            match self.messages_rx.try_recv() {
                Ok(msg) => messages.push(msg),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(err) => return Err(EngineError::from(err)),
            }
        }

        let mut merged_user_messages: Vec<message::Message> = vec![];
        let mut merged_system_messages: Vec<proto::ValidatorMessage> = vec![];

        for msg in &messages {
            match msg {
                Message::ValidatorMessage(msg) => {
                    if let Some(onchain_event) = &msg.on_chain_event {
                        match self
                            .onchain_event_store
                            .merge_onchain_event(onchain_event.clone(), txn_batch)
                        {
                            Ok(_) => {
                                merged_system_messages.push(msg.clone());
                                // TODO(aditi): Insert into trie.
                            }
                            Err(err) => {
                                error!("Unable to merge onchain event: {}", err)
                            }
                        }
                    }
                }
                Message::UserMessage(msg) => {
                    let data = msg.data.as_ref().ok_or(EngineError::NoMessageData)?;
                    let msg_type = MessageType::try_from(data.r#type)
                        .or(Err(EngineError::InvalidMessageType))?;
                    match msg_type {
                        MessageType::CastAdd => {
                            self.cast_store
                                .merge(&msg, txn_batch)
                                .map_err(EngineError::new_store_error(msg.hash.clone()))?;
                            merged_user_messages.push(msg.clone());
                            self.trie
                                .insert(&*self.db, txn_batch, vec![msg.hash.clone()])?;
                        }
                        unhandled_type => {
                            return Err(EngineError::UnsupportedMessageType(unhandled_type));
                        }
                    }
                }
            }
        }

        // TODO: Group by fid so we only have a single txn per block per fid
        let mut transactions = vec![];
        let snap_txn = snapchain::Transaction {
            fid: 1234,                               //TODO
            account_root: vec![5, 5, 6, 6],          //TODO
            system_messages: merged_system_messages, //TODO
            user_messages: merged_user_messages,
        };
        transactions.push(snap_txn);

        let new_root_hash = self.trie.root_hash()?;

        let result = ShardStateChange {
            shard_id,
            new_state_root: new_root_hash.clone(),
            transactions,
        };

        Ok(result)
    }

    pub fn propose_state_change(&mut self, shard: u32) -> ShardStateChange {
        let mut txn = RocksDbTransactionBatch::new();
        let result = self.prepare_proposal(&mut txn, shard).unwrap(); //TODO: don't unwrap()

        // TODO: use drop trait?
        self.trie.reload(&*self.db).unwrap();

        result
    }

    fn replay_proposal(
        &mut self,
        txn_batch: &mut RocksDbTransactionBatch,
        transactions: &[Transaction],
        shard_root: &[u8],
    ) -> Result<(), EngineError> {
        let mut merged_messages: Vec<message::Message> = vec![];

        for snap_txn in transactions {
            for msg in &snap_txn.user_messages {
                let data = msg.data.as_ref().ok_or(EngineError::NoMessageData)?;
                let msg_type =
                    MessageType::try_from(data.r#type).or(Err(EngineError::InvalidMessageType))?;

                match msg_type {
                    MessageType::CastAdd => {
                        self.cast_store
                            .merge(msg, txn_batch)
                            .map_err(EngineError::new_store_error(msg.hash.clone()))?;

                        merged_messages.push(msg.clone());

                        self.trie
                            .insert(&*self.db, txn_batch, vec![msg.hash.clone()])?;
                    }

                    unhandled_type => {
                        return Err(EngineError::UnsupportedMessageType(unhandled_type));
                    }
                }
            }

            for msg in &snap_txn.system_messages {
                if let Some(onchain_event) = &msg.on_chain_event {
                    self.onchain_event_store
                        .merge_onchain_event(onchain_event.clone(), txn_batch)?;

                    // TODO(aditi): Insert into the trie
                }
            }
        }

        let root1 = self.trie.root_hash()?;

        if &root1 != shard_root {
            return Err(EngineError::HashMismatch);
        }

        Ok(())
    }

    pub fn validate_state_change(&mut self, shard_state_change: &ShardStateChange) -> bool {
        let mut txn = RocksDbTransactionBatch::new();

        let transactions = &shard_state_change.transactions;
        let shard_root = &shard_state_change.new_state_root;

        let mut result = true;

        if let Err(err) = self.replay_proposal(&mut txn, transactions, shard_root) {
            error!("State change validation failed: {}", err);
            result = false;
        }

        self.trie.reload(&*self.shard_store.db).unwrap();
        result
    }

    pub fn commit_shard_chunk(&mut self, shard_chunk: ShardChunk) {
        let mut txn = RocksDbTransactionBatch::new();

        let shard_root = &shard_chunk.header.as_ref().unwrap().shard_root;
        let transactions = &shard_chunk.transactions;

        if let Err(err) = self.replay_proposal(&mut txn, transactions, shard_root) {
            error!("State change commit failed: {}", err);
            panic!("State change commit failed: {}", err);
        }

        self.db.commit(txn).unwrap();
        self.trie.reload(&*self.shard_store.db).unwrap();

        match self.shard_store.put_shard_chunk(shard_chunk) {
            Err(err) => {
                error!("Unable to write shard chunk to store {}", err)
            }
            Ok(()) => {}
        }
    }

    pub fn get_confirmed_height(&self) -> Height {
        match self.shard_store.max_block_number() {
            Ok(block_num) => Height::new(self.shard_id, block_num),
            Err(_) => Height::new(self.shard_id, 0),
        }
    }

    pub fn get_last_shard_chunk(&self) -> Option<ShardChunk> {
        match self.shard_store.get_last_shard_chunk() {
            Ok(shard_chunk) => shard_chunk,
            Err(err) => {
                error!("Unable to obtain last shard chunk {:#?}", err);
                None
            }
        }
    }

    pub fn get_casts_by_fid(&self, fid: u32) -> Result<MessagesPage, HubError> {
        CastStore::get_cast_adds_by_fid(&self.cast_store, fid, &PageOptions::default())
    }

    pub fn get_onchain_events(
        &self,
        event_type: OnChainEventType,
        fid: u32,
    ) -> Result<Vec<OnChainEvent>, OnchainEventStorageError> {
        self.onchain_event_store.get_onchain_events(event_type, fid)
    }
}

pub struct BlockEngine {
    block_store: BlockStore,
}

impl BlockEngine {
    pub fn new(block_store: BlockStore) -> Self {
        BlockEngine { block_store }
    }

    pub fn commit_block(&mut self, block: Block) {
        let result = self.block_store.put_block(block);
        if result.is_err() {
            error!("Failed to store block: {:?}", result.err());
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

    pub fn get_confirmed_height(&self) -> Height {
        let shard_index = 0;
        match self.block_store.max_block_number() {
            Ok(block_num) => Height::new(shard_index, block_num),
            Err(_) => Height::new(shard_index, 0),
        }
    }
}
