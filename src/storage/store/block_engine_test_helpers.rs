use crate::core::types::Height;
use crate::core::util::FarcasterTime;
use crate::proto::{Block, BlockHeader, FarcasterNetwork, OnChainEvent, ShardWitness};
use crate::storage::db::RocksDB;
use crate::storage::store::block_engine::{BlockEngine, BlockStateChange};
use crate::storage::store::mempool_poller::MempoolMessage;
use crate::storage::store::test_helper::statsd_client;
use crate::storage::store::BlockStore;
use crate::storage::trie::merkle_trie::MerkleTrie;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::mpsc;

pub fn setup(network: Option<FarcasterNetwork>) -> (BlockEngine, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = RocksDB::new(db_path.to_str().unwrap());
    db.open().unwrap();
    let db = Arc::new(db);

    let block_store = BlockStore::new(db.clone());
    let trie = MerkleTrie::new(16).unwrap();
    let statsd_client = statsd_client();
    let (tx, _rx) = mpsc::channel(100);

    let block_engine = BlockEngine::new(
        block_store,
        trie,
        statsd_client,
        db,
        100,
        Some(tx),
        network.unwrap_or(FarcasterNetwork::Devnet),
    );

    (block_engine, temp_dir)
}

fn default_block() -> Block {
    Block {
        header: Some(BlockHeader {
            height: Some(Height::new(0, 1)),
            parent_hash: vec![0; 32],
            state_root: vec![],
            events_hash: vec![],
            timestamp: FarcasterTime::current().to_u64(),
            chain_id: 0,
            version: 0,
            shard_witnesses_hash: vec![],
        }),
        shard_witness: Some(ShardWitness {
            shard_chunk_witnesses: vec![],
        }),
        transactions: vec![],
        events: vec![],
        hash: vec![],
        commits: None,
    }
}

pub fn state_change_to_block(block_number: u64, change: &BlockStateChange) -> Block {
    let mut block = default_block();

    block.header.as_mut().unwrap().state_root = change.new_state_root.clone();
    block.header.as_mut().unwrap().events_hash = change.events_hash.clone();
    block.header.as_mut().unwrap().height = Some(Height {
        shard_index: 0,
        block_number,
    });
    block.header.as_mut().unwrap().timestamp = change.timestamp.clone().into();
    block.transactions = change.transactions.clone();
    block.events = change.events.clone();
    block
}

pub fn validate_and_commit_state_change(
    engine: &mut BlockEngine,
    state_change: &BlockStateChange,
) -> Block {
    let height = engine.get_confirmed_height().increment();

    let valid = engine.validate_state_change(state_change, height);
    assert!(valid);

    let block = state_change_to_block(height.block_number, state_change);
    engine.commit_block(&block);
    assert_eq!(state_change.new_state_root, engine.trie_root_hash());
    block
}

pub fn commit_event(engine: &mut BlockEngine, event: &OnChainEvent) -> Block {
    let height = engine.get_confirmed_height().increment();
    let state_change =
        engine.propose_state_change(vec![MempoolMessage::OnchainEvent(event.clone())], height);

    validate_and_commit_state_change(engine, &state_change)
}
