use crate::core::types::Height;
use crate::core::util::FarcasterTime;
use crate::proto::{
    self, Block, BlockEvent, BlockHeader, FarcasterNetwork, OnChainEvent, ShardWitness,
    StorageUnitType,
};
use crate::storage::db::RocksDB;
use crate::storage::store::block_engine::{BlockEngine, BlockStateChange};
use crate::storage::store::mempool_poller::MempoolMessage;
use crate::storage::store::test_helper::statsd_client;
use crate::storage::store::BlockStore;
use crate::storage::trie::merkle_trie::{self, MerkleTrie, TrieKey};
use crate::utils::factory::events_factory;
use ed25519_dalek::SigningKey;
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
    let trie = MerkleTrie::new().unwrap();
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

#[cfg(test)]
pub fn message_exists_in_trie(engine: &mut BlockEngine, msg: &proto::Message) -> bool {
    TrieKey::for_message(&msg)
        .iter()
        .all(|key| engine.trie_key_exists(&merkle_trie::Context::new(), &key))
}

pub fn default_block() -> Block {
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

#[derive(Clone)]
pub enum Validity {
    Valid,
    Invalid,
}

pub fn commit_message(
    engine: &mut BlockEngine,
    msg: &crate::proto::Message,
    validity: Validity,
) -> Block {
    let height = engine.get_confirmed_height().increment();
    let state_change =
        engine.propose_state_change(vec![MempoolMessage::UserMessage(msg.clone())], height);
    if state_change.transactions.is_empty() {
        panic!("Failed to propose message");
    }
    let block = validate_and_commit_state_change(engine, &state_change);

    let in_trie = TrieKey::for_message(&msg)
        .iter()
        .all(|key| engine.trie_key_exists(&merkle_trie::Context::new(), &key));

    let should_be_in_trie = match validity {
        Validity::Valid => true,
        Validity::Invalid => false,
    };

    assert_eq!(in_trie, should_be_in_trie);

    block
}

pub fn commit_messages(
    engine: &mut BlockEngine,
    msgs: Vec<(&crate::proto::Message, Validity)>,
) -> Block {
    use itertools::Itertools;

    let height = engine.get_confirmed_height().increment();
    let state_change = engine.propose_state_change(
        msgs.clone()
            .into_iter()
            .map(|(msg, _)| MempoolMessage::UserMessage(msg.clone()))
            .collect_vec(),
        height,
    );

    if state_change.transactions.is_empty() {
        panic!("Failed to propose message");
    }

    let block = validate_and_commit_state_change(engine, &state_change);
    assert_eq!(
        state_change.new_state_root,
        block.header.as_ref().unwrap().state_root
    );

    for (msg, validity) in msgs {
        let in_trie = TrieKey::for_message(&msg)
            .iter()
            .all(|key| engine.trie_key_exists(&merkle_trie::Context::new(), &key));

        let should_be_in_trie = match validity {
            Validity::Valid => true,
            Validity::Invalid => false,
        };

        assert_eq!(in_trie, should_be_in_trie);
    }

    block
}

pub fn register_user(
    fid: u64,
    signer: SigningKey,
    custody_address: Vec<u8>,
    storage_units: u32,
    engine: &mut BlockEngine,
) {
    // Create storage rent event with specified units
    let storage_event = events_factory::create_rent_event(
        fid,
        storage_units,
        StorageUnitType::UnitType2025,
        false,
        FarcasterNetwork::Devnet,
    );
    commit_event(engine, &storage_event);

    // Create ID register event
    let id_register_event = events_factory::create_id_register_event(
        fid,
        crate::proto::IdRegisterEventType::Register,
        custody_address,
        None,
    );
    commit_event(engine, &id_register_event);

    // Create signer event
    let signer_event = events_factory::create_signer_event(
        fid,
        signer,
        crate::proto::SignerEventType::Add,
        None,
        None,
    );
    commit_event(engine, &signer_event);
}

pub fn assert_merge_message_event(block_event: &BlockEvent, message: &proto::Message) {
    assert_eq!(
        block_event.data.as_ref().unwrap().r#type,
        crate::proto::BlockEventType::MergeMessage as i32
    );
    if let Some(crate::proto::block_event_data::Body::MergeMessageEventBody(merge_message_event)) =
        &block_event.data.as_ref().unwrap().body
    {
        assert_eq!(merge_message_event.message.as_ref().unwrap(), message);
    } else {
        panic!("Expected MergeMessageEventBody");
    }
}

pub fn assert_storage_balance(
    block_engine: &BlockEngine,
    fid: u64,
    unit_type: StorageUnitType,
    num_units: u32,
) {
    assert_eq!(
        block_engine
            .stores()
            .get_storage_slot_for_fid(fid, &vec![], true)
            .unwrap()
            .units_for(unit_type),
        num_units
    )
}
