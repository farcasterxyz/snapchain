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
    use ed25519_dalek::{SecretKey, SigningKey};
    use hex::FromHex;
    use prost::Message;

    pub fn default_custody_address() -> Vec<u8> {
        "000000000000000000".to_string().encode_to_vec()
    }

    pub fn default_signer() -> SigningKey {
        SigningKey::from_bytes(
            &SecretKey::from_hex(
                "1000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap(),
        )
    }

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
            None,
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
        let state_change = block_engine.propose_state_change(vec![], height, None);

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
        let state_change = block_engine.propose_state_change(vec![], height, None);

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
        let state_change = block_engine.propose_state_change(messages, height, None);
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
        let state_change = block_engine.propose_state_change(messages, height, None);
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

        let mut state_change = block_engine.propose_state_change(vec![], height, None);

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

        let mut state_change = block_engine.propose_state_change(vec![], height, None);

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
            .get_storage_slot_for_fid(FID_FOR_TEST, &vec![], true, true)
            .unwrap();
        assert_eq!(storage_slot.units_for(StorageUnitType::UnitType2025), 1);
    }

    #[tokio::test]
    async fn test_heartbeat_generated_on_interval() {
        let (mut block_engine, _temp_dir) = setup(None);
        // The heartbeat interval is 5 blocks, generate the first 4 where there will be no events
        for _ in 0..4 {
            let height = block_engine.get_confirmed_height().increment();
            let state_change = block_engine.propose_state_change(vec![], height, None);
            assert!(state_change.events.is_empty());
            validate_and_commit_state_change(&mut block_engine, &state_change);
        }

        let height = block_engine.get_confirmed_height().increment();
        let state_change = block_engine.propose_state_change(vec![], height, None);
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
            let state_change = block_engine.propose_state_change(vec![], height, None);
            assert!(state_change.events.is_empty());
            validate_and_commit_state_change(&mut block_engine, &state_change);
        }

        // Check that seqnum is incremented properly
        let height = block_engine.get_confirmed_height().increment();
        let state_change = block_engine.propose_state_change(vec![], height, None);
        assert_eq!(state_change.events.len(), 1);
        assert_eq!(state_change.events[0].data.as_ref().unwrap().seqnum, 2);
        assert_eq!(
            state_change.events[0].data.as_ref().unwrap().block_number,
            height.block_number
        );
        assert_eq!(state_change.events[0].data.as_ref().unwrap().event_index, 0);
        validate_and_commit_state_change(&mut block_engine, &state_change);
    }

    #[tokio::test]
    async fn test_storage_lend_message_merged() {
        let (mut block_engine, _temp_dir) = setup(None);

        // Register user with storage
        register_user(
            FID_FOR_TEST,
            default_signer(),
            default_custody_address(),
            1000,
            &mut block_engine,
        );

        let lend_message = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST,
            FID_FOR_TEST + 1,
            100,
            StorageUnitType::UnitType2025,
            None,
            None,
        );

        let block = commit_message(&mut block_engine, &lend_message, Validity::Valid);

        // Should generate one block event for the storage lend
        assert_eq!(block.events.len(), 1);
        assert_merge_message_event(&block.events[0], &lend_message);
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST,
            StorageUnitType::UnitType2025,
            900,
        );
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST + 1,
            StorageUnitType::UnitType2025,
            100,
        );
    }

    #[tokio::test]
    async fn test_multiple_storage_lends_in_same_transaction() {
        let (mut block_engine, _temp_dir) = setup(None);

        // Register user with only 250 units of storage - not enough for all lends
        register_user(
            FID_FOR_TEST,
            default_signer(),
            default_custody_address(),
            400, // Only 300 units - insufficient for all lends (100 + 200 + 150 = 450)
            &mut block_engine,
        );

        // Create multiple lend messages from same FID to different recipients
        let lend_message1 = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST,
            FID_FOR_TEST + 1,
            100, // This should succeed (400 - 100 = 300 remaining)
            StorageUnitType::UnitType2025,
            None,
            None,
        );
        let lend_message2 = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST,
            FID_FOR_TEST + 2,
            200, // This should succeed (300 - 200 = 100 remaining)
            StorageUnitType::UnitType2025,
            None,
            None,
        );
        let lend_message3 = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST,
            FID_FOR_TEST + 3,
            150, // This should fail (still insufficient storage)
            StorageUnitType::UnitType2025,
            None,
            None,
        );

        let block = commit_messages(
            &mut block_engine,
            vec![
                (&lend_message1, Validity::Valid),
                (&lend_message2, Validity::Valid),
                (&lend_message3, Validity::Invalid),
            ],
        );

        // Should only generate one block event for the successful lend (the first one)
        // The other two should fail during merge due to insufficient storage
        assert_eq!(block.events.len(), 2);
        assert_eq!(block.events[1].seqnum(), block.events[0].seqnum() + 1);
        assert_merge_message_event(&block.events[0], &lend_message1);
        assert_merge_message_event(&block.events[1], &lend_message2);
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST,
            StorageUnitType::UnitType2025,
            100,
        );
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST + 1,
            StorageUnitType::UnitType2025,
            100,
        );
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST + 2,
            StorageUnitType::UnitType2025,
            200,
        );
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST + 3,
            StorageUnitType::UnitType2025,
            0,
        );
    }

    #[tokio::test]
    async fn test_borrowed_storage_cannot_be_lent() {
        let (mut block_engine, _temp_dir) = setup(None);

        // Register FID_FOR_TEST + 1 with some storage to lend to FID_FOR_TEST + 2
        register_user(
            FID_FOR_TEST + 1,
            default_signer(),
            default_custody_address(),
            500,
            &mut block_engine,
        );

        // Register FID_FOR_TEST + 2 so they can receive lent storage
        register_user(
            FID_FOR_TEST + 2,
            default_signer(),
            default_custody_address(),
            0, // No initial storage
            &mut block_engine,
        );

        // FID_FOR_TEST + 1 lends storage to FID_FOR_TEST + 2
        let lend_message1 = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST + 1,
            FID_FOR_TEST + 2,
            300,
            StorageUnitType::UnitType2025,
            None,
            None,
        );
        let block = commit_message(&mut block_engine, &lend_message1, Validity::Valid);
        assert_merge_message_event(&block.events[0], &lend_message1);
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST + 1,
            StorageUnitType::UnitType2025,
            200,
        );
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST + 2,
            StorageUnitType::UnitType2025,
            300,
        );

        // Now FID_FOR_TEST + 2 tries to lend storage they don't own
        // They have 300 borrowed units, but 0 owned units
        let lend_message2 = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST + 2, // Borrower trying to lend
            FID_FOR_TEST,     // Different recipient
            100,              // Amount they don't actually own
            StorageUnitType::UnitType2025,
            None,
            None,
        );

        let block = commit_message(&mut block_engine, &lend_message2, Validity::Invalid);

        // No block events should be generated for failed storage lend
        assert_eq!(block.events.len(), 0);
    }

    #[tokio::test]
    async fn test_lender_can_take_back_storage_by_setting_to_zero() {
        let (mut block_engine, _temp_dir) = setup(None);

        // Register lender with storage
        register_user(
            FID_FOR_TEST,
            default_signer(),
            default_custody_address(),
            500,
            &mut block_engine,
        );

        // Lender lends 300 units to borrower
        let lend_message1 = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST,
            FID_FOR_TEST + 1,
            300,
            StorageUnitType::UnitType2025,
            None,
            None,
        );
        let block1 = commit_message(&mut block_engine, &lend_message1, Validity::Valid);
        assert_eq!(block1.events.len(), 1);
        assert_merge_message_event(&block1.events[0], &lend_message1);

        // Verify initial balances after lending
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST,
            StorageUnitType::UnitType2025,
            200,
        ); // 500 - 300
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST + 1,
            StorageUnitType::UnitType2025,
            300,
        ); // borrowed

        // Lender takes back storage by setting lend to 0
        let lend_message2 = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST,
            FID_FOR_TEST + 1,
            0, // Setting to 0 takes back the storage
            StorageUnitType::UnitType2025,
            Some(lend_message1.data.as_ref().unwrap().timestamp + 1),
            None,
        );
        let block2 = commit_message(&mut block_engine, &lend_message2, Validity::Invalid); // Mark as invalid because we don't expect this message to be in the trie
        assert_merge_message_event(&block2.events[0], &lend_message2);
        assert_prune_message_event(&block2.events[1], &lend_message2);

        // Verify balances after taking back storage
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST,
            StorageUnitType::UnitType2025,
            500,
        ); // Back to original
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST + 1,
            StorageUnitType::UnitType2025,
            0,
        ); // No more borrowed storage

        // The old message shouldn't get merged again if it's far enough in the past
        commit_message_at(
            &mut block_engine,
            &lend_message1,
            FarcasterTime::new(lend_message1.data.as_ref().unwrap().timestamp as u64 + (60 * 11)),
            Validity::Invalid,
        );
    }

    #[tokio::test]
    async fn test_user_with_low_total_storage_cannot_lend() {
        let (mut block_engine, _temp_dir) = setup(None);

        // Register user with only 50 units of storage (less than 100 required minimum)
        register_user(
            FID_FOR_TEST,
            default_signer(),
            default_custody_address(),
            50, // Only 50 units - below the 100 unit minimum for lending
            &mut block_engine,
        );

        // Attempt to create a storage lend message
        let lend_message = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST,
            FID_FOR_TEST + 1,
            10, // Try to lend 10 units
            StorageUnitType::UnitType2025,
            None,
            None,
        );

        // The message should be invalid due to insufficient total storage
        let block = commit_message(&mut block_engine, &lend_message, Validity::Invalid);

        // No block events should be generated for failed storage lend
        assert_eq!(block.events.len(), 0);

        // Storage balances should remain unchanged
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST,
            StorageUnitType::UnitType2025,
            50, // Original amount
        );
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST + 1,
            StorageUnitType::UnitType2025,
            0, // No borrowed storage
        );
    }
}
