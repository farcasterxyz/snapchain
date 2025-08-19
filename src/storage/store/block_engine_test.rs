#[cfg(test)]
mod tests {
    use crate::core::types::Height;
    use crate::core::util::FarcasterTime;
    use crate::proto::{Block, BlockHeader, FarcasterNetwork, ShardWitness, ValidatorMessage};
    use crate::storage::db::RocksDB;
    use crate::storage::store::block_engine::{BlockEngine, ShardStateChange};
    use crate::storage::store::mempool_poller::MempoolMessage;
    use crate::storage::store::test_helper::{statsd_client, FID_FOR_TEST};
    use crate::storage::store::BlockStore;
    use crate::storage::trie::merkle_trie::MerkleTrie;
    use crate::utils::factory::{events_factory, messages_factory};
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::sync::mpsc;

    fn setup() -> (BlockEngine, TempDir) {
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
            FarcasterNetwork::Devnet,
            5, // heartbeat_block_interval
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

    pub fn state_change_to_block(block_number: u64, change: &ShardStateChange) -> Block {
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
        state_change: &ShardStateChange,
    ) -> Block {
        let height = engine.get_confirmed_height().increment();

        let valid = engine.validate_state_change(state_change, height);
        assert!(valid);

        let block = state_change_to_block(height.block_number, state_change);
        engine.commit_block(&block);
        assert_eq!(state_change.new_state_root, engine.trie_root_hash());
        block
    }

    #[tokio::test]
    async fn test_empty_block() {
        let (mut block_engine, _temp_dir) = setup();
        let height = block_engine.get_confirmed_height().increment();
        let state_change = block_engine.propose_state_change(vec![], height);

        assert_eq!(state_change.transactions.len(), 0);
        assert!(state_change.events.is_empty());
        assert!(state_change.new_state_root.is_empty());
        assert_eq!(
            state_change.events_hash,
            blake3::hash(b"").as_bytes().to_vec()
        );

        validate_and_commit_state_change(&mut block_engine, &state_change);
        assert_eq!(block_engine.get_confirmed_height(), height);
    }

    #[tokio::test]
    async fn test_all_messages_dropped() {
        let (mut block_engine, _temp_dir) = setup();
        let height = block_engine.get_confirmed_height().increment();
        // These messages are included in the transaction list but not included in the state root.
        let messages = vec![
            MempoolMessage::ValidatorMessage(ValidatorMessage {
                on_chain_event: Some(events_factory::create_onchain_event(FID_FOR_TEST)),
                fname_transfer: None,
            }),
            MempoolMessage::UserMessage(messages_factory::casts::create_cast_add(
                FID_FOR_TEST + 1,
                "hi",
                None,
                None,
            )),
        ];
        let state_change = block_engine.propose_state_change(messages, height);
        assert_eq!(state_change.transactions.len(), 2);
        assert!(state_change.events.is_empty());
        assert!(state_change.new_state_root.is_empty());
        assert_eq!(
            state_change.events_hash,
            blake3::hash(b"").as_bytes().to_vec()
        );

        validate_and_commit_state_change(&mut block_engine, &state_change);
        assert_eq!(block_engine.get_confirmed_height(), height);
    }

    #[tokio::test]
    #[should_panic(expected = "State change commit failed: merkle trie root hash mismatch")]
    async fn test_invalid_state_root() {
        let (mut block_engine, _temp_dir) = setup();
        let height = block_engine.get_confirmed_height().increment();
        let invalid_hash = hex::decode("ffffffffffffffffffffffffffffffffffffffff").unwrap();

        let mut state_change = block_engine.propose_state_change(vec![], height);

        let valid = block_engine.validate_state_change(&state_change, height);
        assert!(valid);

        state_change.new_state_root = invalid_hash;
        let valid = block_engine.validate_state_change(&state_change, height);
        assert!(!valid);

        block_engine.commit_block(&state_change_to_block(height.block_number, &state_change));
    }

    #[tokio::test]
    #[should_panic(expected = "State change commit failed: events hash mismatch")]
    async fn test_invalid_events_hash() {
        let (mut block_engine, _temp_dir) = setup();
        let height = block_engine.get_confirmed_height().increment();
        let invalid_hash = hex::decode("ffffffffffffffffffffffffffffffffffffffff").unwrap();

        let mut state_change = block_engine.propose_state_change(vec![], height);

        let valid = block_engine.validate_state_change(&state_change, height);
        assert!(valid);

        state_change.events_hash = invalid_hash;
        let valid = block_engine.validate_state_change(&state_change, height);
        assert!(!valid);

        block_engine.commit_block(&state_change_to_block(height.block_number, &state_change));
    }

    #[tokio::test]
    async fn test_heartbeat_generated_on_interval() {
        let (mut block_engine, _temp_dir) = setup();
        for _ in 0..4 {
            let height = block_engine.get_confirmed_height().increment();
            let state_change = block_engine.propose_state_change(vec![], height);
            assert!(state_change.events.is_empty());
            validate_and_commit_state_change(&mut block_engine, &state_change);
        }

        let height = block_engine.get_confirmed_height().increment();
        let state_change = block_engine.propose_state_change(vec![], height);
        assert_eq!(state_change.events.len(), 1);
        assert_eq!(state_change.events[0].seqnum, 1);
        assert_eq!(state_change.events[0].block_number, height.block_number);
        assert_eq!(state_change.events[0].event_index, 0);
        validate_and_commit_state_change(&mut block_engine, &state_change);

        for _ in 0..4 {
            let height = block_engine.get_confirmed_height().increment();
            let state_change = block_engine.propose_state_change(vec![], height);
            assert!(state_change.events.is_empty());
            validate_and_commit_state_change(&mut block_engine, &state_change);
        }

        // Check that seqnum is incremented properly
        let height = block_engine.get_confirmed_height().increment();
        let state_change = block_engine.propose_state_change(vec![], height);
        assert_eq!(state_change.events.len(), 1);
        assert_eq!(state_change.events[0].seqnum, 2);
        assert_eq!(state_change.events[0].block_number, height.block_number);
        assert_eq!(state_change.events[0].event_index, 0);
        validate_and_commit_state_change(&mut block_engine, &state_change);
    }
}
