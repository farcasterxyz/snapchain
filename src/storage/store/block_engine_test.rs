#[cfg(test)]
mod tests {
    use crate::core::util::FarcasterTime;
    use crate::proto::{FarcasterNetwork, StorageUnitType};
    use crate::storage::store::block_engine::BlockStateChange;
    use crate::storage::store::block_engine_test_helpers::*;
    use crate::storage::store::mempool_poller::MempoolMessage;
    use crate::storage::store::test_helper::{trie_ctx, FID_FOR_TEST};
    use crate::storage::trie::merkle_trie::TrieKey;
    use crate::utils::factory::{events_factory, messages_factory};

    #[tokio::test]
    async fn test_trie_updated_only_on_commit() {
        let (mut block_engine, _temp_dir) = setup(None);
        let onchain_event = events_factory::create_rent_event(
            FID_FOR_TEST,
            1,
            StorageUnitType::UnitType2025,
            false,
            FarcasterNetwork::Devnet,
        );
        let height = block_engine.get_confirmed_height().increment();
        let state_change = block_engine.propose_state_change(
            vec![MempoolMessage::OnchainEvent(onchain_event.clone())],
            height,
        );
        assert!(!state_change.new_state_root.is_empty());
        assert!(block_engine.trie_root_hash().is_empty());

        block_engine.validate_state_change(&state_change, height);
        assert!(block_engine.trie_root_hash().is_empty());

        block_engine.commit_block(&state_change_to_block(height.block_number, &state_change));
        assert_eq!(block_engine.trie_root_hash(), state_change.new_state_root);
    }

    #[tokio::test]
    async fn test_empty_block() {
        let (mut block_engine, _temp_dir) = setup(None);
        let height = block_engine.get_confirmed_height().increment();
        let state_change = block_engine.propose_state_change(vec![], height);

        assert_eq!(state_change.transactions.len(), 0);
        assert!(state_change.events.is_empty());
        assert!(state_change.new_state_root.is_empty());
        assert!(state_change.events_hash.is_empty());

        validate_and_commit_state_change(&mut block_engine, &state_change);
        assert_eq!(block_engine.get_confirmed_height(), height);
    }

    #[tokio::test]
    async fn test_mainnet_propose_validate_commit() {
        // Test that the pipeline works while new features are not active on mainnet
        let (mut block_engine, _temp_dir) = setup(Some(FarcasterNetwork::Mainnet));
        let height = block_engine.get_confirmed_height().increment();
        let state_change = block_engine.propose_state_change(vec![], height);

        assert_eq!(state_change.transactions.len(), 0);
        assert!(state_change.events.is_empty());
        assert!(state_change.new_state_root.is_empty());
        assert!(state_change.events_hash.is_empty());

        validate_and_commit_state_change(&mut block_engine, &state_change);
        assert_eq!(block_engine.get_confirmed_height(), height);
    }

    #[tokio::test]
    async fn test_validate_and_commit_old_blocks() {
        // Test that validate and commit will work for old blocks even after features are active
        let (mut block_engine, _temp_dir) = setup(Some(FarcasterNetwork::Mainnet));

        let height = block_engine.get_confirmed_height().increment();
        validate_and_commit_state_change(
            &mut block_engine,
            &BlockStateChange {
                timestamp: FarcasterTime::from_unix_seconds(1752685200),
                new_state_root: vec![],
                events_hash: vec![],
                transactions: vec![],
                events: vec![],
            },
        );
        assert_eq!(block_engine.get_confirmed_height(), height);
    }

    #[tokio::test]
    async fn test_user_messages_dropped_if_no_storage() {
        let (mut block_engine, _temp_dir) = setup(None);
        let height = block_engine.get_confirmed_height().increment();
        // These messages are included in the transaction list but not included in the state root.
        let messages = vec![MempoolMessage::UserMessage(
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "hi", None, None),
        )];
        let state_change = block_engine.propose_state_change(messages, height);
        assert_eq!(state_change.transactions.len(), 0);
        assert!(state_change.events.is_empty());
        assert!(state_change.new_state_root.is_empty());
        assert!(state_change.events_hash.is_empty());

        validate_and_commit_state_change(&mut block_engine, &state_change);
        assert_eq!(block_engine.get_confirmed_height(), height);
    }

    #[tokio::test]
    async fn test_user_messages_put_in_block_if_storage_purchased() {
        let (mut block_engine, _temp_dir) = setup(None);
        let onchain_event = events_factory::create_rent_event(
            FID_FOR_TEST,
            1,
            StorageUnitType::UnitType2025,
            false,
            FarcasterNetwork::Devnet,
        );
        commit_event(&mut block_engine, &onchain_event);

        let height = block_engine.get_confirmed_height().increment();
        let initial_state_root = block_engine.trie_root_hash();
        let messages = vec![MempoolMessage::UserMessage(
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "hi", None, None),
        )];

        // The message is included in the block but doesn't impact trie state.
        let state_change = block_engine.propose_state_change(messages, height);
        assert_eq!(state_change.transactions.len(), 1);
        assert_eq!(state_change.new_state_root, initial_state_root);

        validate_and_commit_state_change(&mut block_engine, &state_change);
        assert_eq!(block_engine.get_confirmed_height(), height);
    }

    #[tokio::test]
    #[should_panic(expected = "State change commit failed: merkle trie root hash mismatch")]
    async fn test_invalid_state_root() {
        let (mut block_engine, _temp_dir) = setup(None);
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
        let (mut block_engine, _temp_dir) = setup(None);
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
    async fn test_merge_onchain_event() {
        let (mut block_engine, _temp_dir) = setup(None);
        let onchain_event = events_factory::create_rent_event(
            FID_FOR_TEST,
            1,
            StorageUnitType::UnitType2025,
            false,
            FarcasterNetwork::Devnet,
        );
        let block = commit_event(&mut block_engine, &onchain_event);
        // Don't generate any block events for onchain events
        assert!(block.events.is_empty());
        assert!(
            block_engine.trie_key_exists(trie_ctx(), &TrieKey::for_onchain_event(&onchain_event))
        );
        assert_eq!(
            block.header.as_ref().unwrap().state_root,
            block_engine.trie_root_hash()
        );
        let storage_slot = block_engine
            .stores()
            .onchain_event_store
            .get_storage_slot_for_fid(FID_FOR_TEST, FarcasterNetwork::Devnet, &[])
            .unwrap();
        assert_eq!(storage_slot.units_for(StorageUnitType::UnitType2025), 1);
    }

    #[tokio::test]
    async fn test_heartbeat_generated_on_interval() {
        let (mut block_engine, _temp_dir) = setup(None);
        // The heartbeat interval is 5 blocks, generate the first 4 where there will be no events
        for _ in 0..4 {
            let height = block_engine.get_confirmed_height().increment();
            let state_change = block_engine.propose_state_change(vec![], height);
            assert!(state_change.events.is_empty());
            validate_and_commit_state_change(&mut block_engine, &state_change);
        }

        let height = block_engine.get_confirmed_height().increment();
        let state_change = block_engine.propose_state_change(vec![], height);
        assert_eq!(state_change.events.len(), 1);
        assert_eq!(state_change.events[0].data.as_ref().unwrap().seqnum, 1);
        assert_eq!(
            state_change.events[0].data.as_ref().unwrap().block_number,
            height.block_number
        );
        assert_eq!(state_change.events[0].data.as_ref().unwrap().event_index, 0);
        validate_and_commit_state_change(&mut block_engine, &state_change);

        // The heartbeat interval is 5 blocks, generate the next 4 where there will be no events
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
        assert_eq!(state_change.events[0].data.as_ref().unwrap().seqnum, 2);
        assert_eq!(
            state_change.events[0].data.as_ref().unwrap().block_number,
            height.block_number
        );
        assert_eq!(state_change.events[0].data.as_ref().unwrap().event_index, 0);
        validate_and_commit_state_change(&mut block_engine, &state_change);
    }
}
