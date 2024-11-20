use super::shard::ShardStore;
use crate::core::error::HubError;
use crate::core::types::{proto, Height};
use crate::proto::{message, snapchain};
use crate::storage::db;
use crate::storage::db::{PageOptions, RocksDbTransactionBatch};
use crate::storage::store::account::{CastStore, MessagesPage, Store};
use crate::storage::store::BlockStore;
use crate::storage::trie::merkle_trie;
use snapchain::{Block, ShardChunk, Transaction};
use std::iter;
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::{error, warn};

#[derive(Error, Debug)]
enum EngineError {
    #[error("trie error: {source}")]
    TrieError {
        #[from]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("unable to merge cast message during commit: {0}")]
    MergeCastMessageError(String),

    #[error("unsupported message type: {0}")]
    UnsupportedMessageType(String),

    #[error("merkle trie root hash mismatch")]
    HashMismatch,
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
    messages_rx: mpsc::Receiver<message::Message>,
    messages_tx: mpsc::Sender<message::Message>,
    trie: merkle_trie::MerkleTrie,
    cast_store: Store,
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
        let mut trie = merkle_trie::MerkleTrie::new().unwrap();
        trie.initialize(db, &mut txn_batch).unwrap();

        // TODO: The empty trie currently has some issues with the newly added commit/rollback code. Remove when we can.
        trie.insert(db, &mut txn_batch, vec![vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])
            .unwrap();
        db.commit(txn_batch).unwrap();
        trie.reload(db).unwrap();

        let cast_store = CastStore::new(shard_store.db.clone(), 100);

        let (messages_tx, messages_rx) = mpsc::channel::<message::Message>(10_000);
        ShardEngine {
            shard_id,
            shard_store,
            messages_rx,
            messages_tx,
            trie,
            cast_store,
        }
    }

    pub fn messages_tx(&self) -> mpsc::Sender<message::Message> {
        self.messages_tx.clone()
    }

    pub(crate) fn trie_root_hash(&self) -> Vec<u8> {
        self.trie.root_hash().unwrap()
    }

    pub fn propose_state_change(&mut self, shard: u32) -> ShardStateChange {
        //TODO: return Result instead of .unwrap() ?
        let it = iter::from_fn(|| self.messages_rx.try_recv().ok());
        let user_messages: Vec<message::Message> = it.collect();

        warn!(
            shard,
            items = self.trie.items().unwrap(),
            "propose – before insert",
        );

        let db = &*self.shard_store.db;

        let mut txn_batch = RocksDbTransactionBatch::new();

        let mut merged_messages: Vec<message::Message> = vec![];

        for msg in &user_messages {
            if msg.is_type(message::MessageType::CastAdd) {
                match self.cast_store.merge(msg, &mut txn_batch) {
                    Ok(_) => {
                        merged_messages.push(msg.clone());
                        let res = self.trie.insert(db, &mut txn_batch, vec![msg.hash.clone()]);
                        if res.is_err() {
                            error!("Unable to insert message into trie: {}", res.err().unwrap());
                            panic!("Unable to insert message into trie");
                        }
                        warn!(
                            hash = hex::encode(&msg.hash),
                            num_messages = merged_messages.len(),
                            "propose - merged_message"
                        );
                    }
                    Err(err) => {
                        error!("Unable to merge cast message: {}", err);
                    }
                }
            } else {
                error!(
                    msg_type = msg.msg_type(),
                    "Unsupported message type during propose"
                );
            }
        }
        // TODO: Group by fid so we only have a single txn per block per fid
        let mut transactions = vec![];
        let snap_txn = snapchain::Transaction {
            fid: 1234,                      //TODO
            account_root: vec![5, 5, 6, 6], //TODO
            system_messages: vec![],        //TODO
            user_messages: merged_messages,
        };
        transactions.push(snap_txn);

        let new_root_hash = self.trie.root_hash().unwrap();
        let count = self.trie.items().unwrap();

        warn!(
            shard,
            new_state_root = hex::encode(&new_root_hash),
            count,
            "propose - after insert"
        );

        let result = ShardStateChange {
            shard_id: shard,
            new_state_root: new_root_hash.clone(),
            transactions,
        };

        // TODO: this should probably operate automatically via drop trait
        self.trie.reload(db).unwrap();

        warn!(
            shard,
            reloaded_root_hash = hex::encode(&self.trie.root_hash().unwrap()),
            count,
            "propose - reloaded"
        );

        result

        // TODO:
        // Create a db transaction
        // For each user message, add to the store and merkle trie
        // Evict invalid messages from the mempool
        // Construct a ShardStateChange with the valid transactions and the new state root
        // Rollback the transaction
        // Return the state change
    }

    fn replay_proposal(
        trie: &mut merkle_trie::MerkleTrie,
        db: &db::RocksDB,
        txn_batch: &mut RocksDbTransactionBatch,
        cast_store: &Store,
        transactions: &[Transaction],
        shard_root: &[u8],
    ) -> Result<(), EngineError> {
        let mut merged_messages: Vec<message::Message> = vec![];

        for snap_txn in transactions {
            for msg in &snap_txn.user_messages {
                if msg.is_type(message::MessageType::CastAdd) {
                    cast_store.merge(msg, txn_batch).map_err(|err| {
                        EngineError::MergeCastMessageError(format!(
                            "Message hash: {}, Error: {}",
                            hex::encode(&msg.hash),
                            err
                        ))
                    })?;

                    merged_messages.push(msg.clone());

                    trie.insert(db, txn_batch, vec![msg.hash.clone()])
                        .map_err(|e| EngineError::TrieError {
                            source: Box::new(e),
                        })?;
                } else {
                    return Err(EngineError::UnsupportedMessageType(
                        msg.msg_type().to_string(),
                    ));
                }
            }
        }

        let root1 = trie.root_hash().map_err(|e| EngineError::TrieError {
            source: Box::new(e),
        })?;

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

        let db = &*self.shard_store.db;
        if let Err(err) = Self::replay_proposal(
            &mut self.trie,
            db,
            &mut txn,
            &self.cast_store,
            transactions,
            shard_root,
        ) {
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

        let db = &*self.shard_store.db;
        if let Err(err) = Self::replay_proposal(
            &mut self.trie,
            db,
            &mut txn,
            &self.cast_store,
            transactions,
            shard_root,
        ) {
            error!("State change commit failed: {}", err);
            panic!("State change commit failed: {}", err);
        }

        db.commit(txn).unwrap();
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
