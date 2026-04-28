#[cfg(test)]
mod tests {
    use crate::core::util::FarcasterTime;
    use crate::proto::{BlockEventType, FarcasterNetwork, StorageUnitType};
    use crate::storage::store::block_engine::BlockStateChange;
    use crate::storage::store::block_engine_test_helpers::*;
    use crate::storage::store::mempool_poller::MempoolMessage;
    use crate::storage::store::test_helper::{trie_ctx, FID_FOR_TEST};
    use crate::storage::trie::merkle_trie::TrieKey;
    use crate::utils::factory::{events_factory, messages_factory, signers};

    #[tokio::test]
    async fn test_trie_updated_only_on_commit() {
        let (mut block_engine, _temp_dir) = setup();
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
        let (mut block_engine, _temp_dir) = setup();
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
        let (mut block_engine, _temp_dir) = setup_with_options(BlockEngineOptions {
            network: FarcasterNetwork::Mainnet,
            ..BlockEngineOptions::default()
        });
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
        let (mut block_engine, _temp_dir) = setup_with_options(BlockEngineOptions {
            network: FarcasterNetwork::Mainnet,
            ..BlockEngineOptions::default()
        });

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
        let (mut block_engine, _temp_dir) = setup();
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
        let (mut block_engine, _temp_dir) = setup();
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
        let (mut block_engine, _temp_dir) = setup();
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
        let (mut block_engine, _temp_dir) = setup();
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
        let (mut block_engine, _temp_dir) = setup();
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
        let (mut block_engine, _temp_dir) = setup();
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
        let (mut block_engine, _temp_dir) = setup();

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
        let (mut block_engine, _temp_dir) = setup();

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
        let (mut block_engine, _temp_dir) = setup();

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
        let (mut block_engine, _temp_dir) = setup();

        // Register lender with storage
        register_user(
            FID_FOR_TEST,
            default_signer(),
            default_custody_address(),
            500,
            &mut block_engine,
        );

        // Make sure to retain 1 unit for the lender so the lender can revoke.
        let invalid_lend_message = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST,
            FID_FOR_TEST + 1,
            500,
            StorageUnitType::UnitType2025,
            None,
            None,
        );
        commit_message(&mut block_engine, &invalid_lend_message, Validity::Invalid);

        // Lender lends 300 units to borrower
        let lend_message1 = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST,
            FID_FOR_TEST + 1,
            499,
            StorageUnitType::UnitType2025,
            None,
            None,
        );
        let block1 = commit_message(&mut block_engine, &lend_message1, Validity::Valid);
        assert_eq!(
            block1
                .events
                .iter()
                .filter(|event| event.data.as_ref().unwrap().r#type() != BlockEventType::Heartbeat)
                .count(),
            1
        );
        assert_merge_message_event(&block1.events[0], &lend_message1);

        // Verify initial balances after lending
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST,
            StorageUnitType::UnitType2025,
            1,
        ); // 500 - 499
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST + 1,
            StorageUnitType::UnitType2025,
            499,
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
        let (mut block_engine, _temp_dir) = setup_with_options(BlockEngineOptions {
            network: FarcasterNetwork::Mainnet,
            ..Default::default()
        });

        register_user(
            FID_FOR_TEST,
            default_signer(),
            default_custody_address(),
            500, // Only 500 units - below the 1000 unit minimum for lending
            &mut block_engine,
        );

        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST,
            StorageUnitType::UnitType2025,
            500, // Original amount
        );
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST + 1,
            StorageUnitType::UnitType2025,
            0, // No borrowed storage
        );

        let future_time = FarcasterTime::from_unix_seconds(1761019200);

        // Attempt to create a storage lend message
        let lend_message = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST,
            FID_FOR_TEST + 1,
            10, // Try to lend 10 units
            StorageUnitType::UnitType2025,
            Some(future_time.to_u64() as u32 - 1),
            None,
        );

        // The message should be invalid due to insufficient total storage
        let block = commit_message_at(
            &mut block_engine,
            &lend_message,
            future_time.clone(),
            Validity::Invalid,
        );

        // No block events should be generated for failed storage lend
        assert_eq!(block.events.len(), 0);

        // Storage balances should remain unchanged
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST,
            StorageUnitType::UnitType2025,
            500, // Original amount
        );
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST + 1,
            StorageUnitType::UnitType2025,
            0, // No borrowed storage
        );

        register_user(
            FID_FOR_TEST,
            default_signer(),
            default_custody_address(),
            500,
            &mut block_engine,
        );

        // Goes through if the user gets 1000 units
        let lend_message = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST,
            FID_FOR_TEST + 1,
            10, // Try to lend 10 units
            StorageUnitType::UnitType2025,
            Some(future_time.to_u64() as u32 - 1),
            None,
        );

        commit_message_at(
            &mut block_engine,
            &lend_message,
            future_time,
            Validity::Valid,
        );

        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST,
            StorageUnitType::UnitType2025,
            990,
        );
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST + 1,
            StorageUnitType::UnitType2025,
            10,
        );
    }

    // ----------------------------------------------------------------------------------------
    // KEY_ADD / KEY_REMOVE engine integration tests (NEYN-10618)
    //
    // Devnet routes V16 unconditionally (per NEYN-10625), so feature gating doesn't trip up the
    // default `setup()` path. Tests pin custody addresses to real `PrivateKeySigner`s so the
    // EIP-712 recovery checks inside `merge_key_add` / `merge_key_remove` get exercised end-to-
    // end. Failures land as silent merge errors (BlockEngine swallows them via `if let Ok(...)`
    // in `replay_snapchain_txn`), so failure tests assert on absence-of-event + trie-omission
    // rather than a specific error variant; specific-variant coverage lives at the unit level
    // in `gasless_key_merge_test.rs`.
    // ----------------------------------------------------------------------------------------
    mod key_add_remove_tests {
        use super::*;
        use crate::core::util::calculate_message_hash;
        use crate::proto::{self, message_data::Body, MessageType};
        use crate::storage::store::account::{
            get_active_key, get_gasless_key_owner_fid, get_gasless_key_record, get_last_used_at,
            get_user_nonce, ActiveKey,
        };
        use crate::storage::store::block_engine::BlockEngine;
        use alloy_signer_local::PrivateKeySigner;
        use ed25519_dalek::{Signer, SigningKey};
        use prost::Message;

        const REQUEST_FID: u64 = FID_FOR_TEST + 100;
        const STORAGE_UNITS: u32 = 1000;

        fn address_bytes(signer: &PrivateKeySigner) -> Vec<u8> {
            signer.address().as_slice().to_vec()
        }

        /// Re-signs the envelope after the body has been mutated post-factory. Keeps the
        /// envelope hash + Ed25519 signature consistent so static validation passes and we can
        /// observe merge-time rejection.
        fn re_sign_envelope(mut msg: proto::Message, signer: &SigningKey) -> proto::Message {
            let data = msg.data.as_ref().expect("message has data").clone();
            let bytes = data.encode_to_vec();
            let hash = calculate_message_hash(&bytes);
            msg.signature = signer.sign(&hash).to_bytes().to_vec();
            msg.hash = hash;
            msg
        }

        /// Convenience wrapper that registers an FID with a real Ethereum custody address (for
        /// EIP-712 recovery) plus the supplied Ed25519 signer (used for active-key validation
        /// of non-gasless messages, e.g. casts later in this suite).
        fn register_user_eth(
            fid: u64,
            custody: &PrivateKeySigner,
            signer: SigningKey,
            engine: &mut BlockEngine,
        ) {
            register_user(fid, signer, address_bytes(custody), STORAGE_UNITS, engine);
        }

        /// Drops an on-chain `SIGNER_ADD` event for `(fid, gasless_key)` so a subsequent gasless
        /// KEY_ADD trips the on-chain-collision branch in `merge_key_add`.
        fn add_onchain_signer(engine: &mut BlockEngine, fid: u64, signer: &SigningKey) {
            let event = events_factory::create_signer_event(
                fid,
                signer.clone(),
                proto::SignerEventType::Add,
                None,
                None,
            );
            commit_event(engine, &event);
        }

        fn build_key_add(
            fid_custody: &PrivateKeySigner,
            app_custody: &PrivateKeySigner,
            envelope: &SigningKey,
            scopes: Vec<MessageType>,
            ttl: u32,
            nonce: u32,
        ) -> proto::Message {
            let now = messages_factory::farcaster_time();
            messages_factory::keys::create_key_add(
                FID_FOR_TEST,
                fid_custody,
                REQUEST_FID,
                app_custody,
                envelope,
                scopes,
                ttl,
                nonce,
                now + 1_000_000, // deadline well past block timestamp
                Some(now),
            )
        }

        // -- Happy path ------------------------------------------------------------------------

        #[tokio::test]
        async fn test_key_add_message_merged() {
            let (mut block_engine, _temp_dir) = setup();
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let envelope = signers::generate_signer();
            let envelope_pubkey = envelope.verifying_key().to_bytes();

            register_user_eth(
                FID_FOR_TEST,
                &fid_custody,
                default_signer(),
                &mut block_engine,
            );
            register_user_eth(
                REQUEST_FID,
                &app_custody,
                signers::generate_signer(),
                &mut block_engine,
            );

            let key_add = build_key_add(
                &fid_custody,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd],
                3600,
                1,
            );
            let block = commit_message(&mut block_engine, &key_add, Validity::Valid);

            assert_eq!(block.events.len(), 1);
            assert_merge_message_event(&block.events[0], &key_add);

            // State assertions hit the same store readers used by the active-key validation path.
            let txn = crate::storage::db::RocksDbTransactionBatch::new();
            let db = block_engine.stores().db.clone();
            let record = get_gasless_key_record(&db, &txn, FID_FOR_TEST, &envelope_pubkey)
                .unwrap()
                .expect("gasless key record persisted");
            assert_eq!(record.request_fid, REQUEST_FID);
            assert_eq!(
                get_gasless_key_owner_fid(&db, &txn, &envelope_pubkey).unwrap(),
                Some(FID_FOR_TEST),
            );
            assert_eq!(
                get_last_used_at(&db, &txn, FID_FOR_TEST, &envelope_pubkey).unwrap(),
                Some(key_add.data.as_ref().unwrap().timestamp),
            );
            assert_eq!(get_user_nonce(&db, &txn, FID_FOR_TEST).unwrap(), Some(1));
        }

        // -- Failure paths ---------------------------------------------------------------------

        #[tokio::test]
        async fn test_key_add_rejects_expired_deadline() {
            let (mut block_engine, _temp_dir) = setup();
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let envelope = signers::generate_signer();

            register_user_eth(
                FID_FOR_TEST,
                &fid_custody,
                default_signer(),
                &mut block_engine,
            );
            register_user_eth(
                REQUEST_FID,
                &app_custody,
                signers::generate_signer(),
                &mut block_engine,
            );

            // deadline strictly less than message timestamp → SignedKeyRequestExpired at merge.
            let now = messages_factory::farcaster_time();
            let key_add = messages_factory::keys::create_key_add(
                FID_FOR_TEST,
                &fid_custody,
                REQUEST_FID,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd],
                3600,
                1,
                now - 1,
                Some(now),
            );
            let block = commit_message(&mut block_engine, &key_add, Validity::Invalid);
            assert!(
                block.events.iter().all(|e| !matches!(
                    e.data.as_ref().unwrap().r#type(),
                    proto::BlockEventType::MergeMessage
                )),
                "expired KEY_ADD must not emit a MergeMessage event",
            );
        }

        #[tokio::test]
        async fn test_key_add_rejects_malformed_metadata() {
            let (mut block_engine, _temp_dir) = setup();
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let envelope = signers::generate_signer();

            register_user_eth(
                FID_FOR_TEST,
                &fid_custody,
                default_signer(),
                &mut block_engine,
            );
            register_user_eth(
                REQUEST_FID,
                &app_custody,
                signers::generate_signer(),
                &mut block_engine,
            );

            let mut key_add = build_key_add(
                &fid_custody,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd],
                3600,
                1,
            );
            // Truncate metadata so abi_decode fails at verify_signed_key_request_metadata.
            if let Some(Body::KeyAddBody(body)) = key_add.data.as_mut().unwrap().body.as_mut() {
                body.metadata = vec![0xde, 0xad];
            }
            let key_add = re_sign_envelope(key_add, &envelope);

            let block = commit_message(&mut block_engine, &key_add, Validity::Invalid);
            assert!(block.events.iter().all(|e| !matches!(
                e.data.as_ref().unwrap().r#type(),
                proto::BlockEventType::MergeMessage
            )));
        }

        #[tokio::test]
        async fn test_key_add_rejects_unregistered_request_fid() {
            let (mut block_engine, _temp_dir) = setup();
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let envelope = signers::generate_signer();

            register_user_eth(
                FID_FOR_TEST,
                &fid_custody,
                default_signer(),
                &mut block_engine,
            );
            // Intentionally do NOT register REQUEST_FID — get_id_register_event_by_fid returns
            // None inside merge_key_add → InvalidSignedKeyRequest.

            let key_add = build_key_add(
                &fid_custody,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd],
                3600,
                1,
            );
            let block = commit_message(&mut block_engine, &key_add, Validity::Invalid);
            assert!(block.events.iter().all(|e| !matches!(
                e.data.as_ref().unwrap().r#type(),
                proto::BlockEventType::MergeMessage
            )));
        }

        #[tokio::test]
        async fn test_key_add_rejects_request_signer_custody_mismatch() {
            let (mut block_engine, _temp_dir) = setup();
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let other_custody = PrivateKeySigner::random();
            let envelope = signers::generate_signer();

            register_user_eth(
                FID_FOR_TEST,
                &fid_custody,
                default_signer(),
                &mut block_engine,
            );
            // Register REQUEST_FID with a custody DIFFERENT from app_custody so the
            // request_fid_custody.to vs verified.request_signer comparison fails.
            register_user_eth(
                REQUEST_FID,
                &other_custody,
                signers::generate_signer(),
                &mut block_engine,
            );

            let key_add = build_key_add(
                &fid_custody,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd],
                3600,
                1,
            );
            let block = commit_message(&mut block_engine, &key_add, Validity::Invalid);
            assert!(block.events.iter().all(|e| !matches!(
                e.data.as_ref().unwrap().r#type(),
                proto::BlockEventType::MergeMessage
            )));
        }

        #[tokio::test]
        async fn test_key_add_rejects_invalid_custody_signature() {
            let (mut block_engine, _temp_dir) = setup();
            let real_custody = PrivateKeySigner::random();
            let wrong_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let envelope = signers::generate_signer();

            // FID_FOR_TEST is registered with `real_custody`'s address, but the inner KeyAdd
            // EIP-712 signature is produced by `wrong_custody`. Custody recovery in
            // merge_key_add returns wrong_custody.address(), which mismatches real_custody.address().
            register_user_eth(
                FID_FOR_TEST,
                &real_custody,
                default_signer(),
                &mut block_engine,
            );
            register_user_eth(
                REQUEST_FID,
                &app_custody,
                signers::generate_signer(),
                &mut block_engine,
            );

            let key_add = build_key_add(
                &wrong_custody,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd],
                3600,
                1,
            );
            let block = commit_message(&mut block_engine, &key_add, Validity::Invalid);
            assert!(block.events.iter().all(|e| !matches!(
                e.data.as_ref().unwrap().r#type(),
                proto::BlockEventType::MergeMessage
            )));
        }

        #[tokio::test]
        async fn test_key_add_rejects_replay_same_nonce() {
            let (mut block_engine, _temp_dir) = setup();
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let envelope = signers::generate_signer();

            register_user_eth(
                FID_FOR_TEST,
                &fid_custody,
                default_signer(),
                &mut block_engine,
            );
            register_user_eth(
                REQUEST_FID,
                &app_custody,
                signers::generate_signer(),
                &mut block_engine,
            );

            // Pin distinct timestamps so the two messages have distinct envelope hashes (and
            // therefore distinct trie keys). Both still claim nonce=1, so the second is
            // rejected at merge time by check_and_set_user_nonce.
            let now = messages_factory::farcaster_time();
            let first = messages_factory::keys::create_key_add(
                FID_FOR_TEST,
                &fid_custody,
                REQUEST_FID,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd],
                3600,
                1,
                now + 1_000_000,
                Some(now),
            );
            commit_message(&mut block_engine, &first, Validity::Valid);

            let replay = messages_factory::keys::create_key_add(
                FID_FOR_TEST,
                &fid_custody,
                REQUEST_FID,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd],
                3600,
                1,
                now + 1_000_000,
                Some(now + 1),
            );
            let block = commit_message(&mut block_engine, &replay, Validity::Invalid);
            // No second MergeMessage event for the replay.
            let merge_events: Vec<_> = block
                .events
                .iter()
                .filter(|e| {
                    matches!(
                        e.data.as_ref().unwrap().r#type(),
                        proto::BlockEventType::MergeMessage
                    )
                })
                .collect();
            assert!(merge_events.is_empty());
        }

        #[tokio::test]
        async fn test_key_add_rejects_lower_nonce() {
            let (mut block_engine, _temp_dir) = setup();
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let envelope = signers::generate_signer();

            register_user_eth(
                FID_FOR_TEST,
                &fid_custody,
                default_signer(),
                &mut block_engine,
            );
            register_user_eth(
                REQUEST_FID,
                &app_custody,
                signers::generate_signer(),
                &mut block_engine,
            );

            let first = build_key_add(
                &fid_custody,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd],
                3600,
                5,
            );
            commit_message(&mut block_engine, &first, Validity::Valid);

            // Different envelope key but same FID — the user-nonce store is per-fid, not per-key,
            // so a fresh KEY_ADD on the same fid with nonce <= 5 must be rejected.
            let envelope2 = signers::generate_signer();
            let backwards = build_key_add(
                &fid_custody,
                &app_custody,
                &envelope2,
                vec![MessageType::CastAdd],
                3600,
                3,
            );
            let block = commit_message(&mut block_engine, &backwards, Validity::Invalid);
            assert!(block.events.iter().all(|e| !matches!(
                e.data.as_ref().unwrap().r#type(),
                proto::BlockEventType::MergeMessage
            )));
        }

        #[tokio::test]
        async fn test_key_add_rejects_already_registered_onchain() {
            let (mut block_engine, _temp_dir) = setup();
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let envelope = signers::generate_signer();

            register_user_eth(
                FID_FOR_TEST,
                &fid_custody,
                default_signer(),
                &mut block_engine,
            );
            register_user_eth(
                REQUEST_FID,
                &app_custody,
                signers::generate_signer(),
                &mut block_engine,
            );
            // Pre-merge an on-chain SIGNER_ADD for the same (fid, key) the gasless KEY_ADD will
            // target. merge_key_add's get_active_signer check fires before any state writes.
            add_onchain_signer(&mut block_engine, FID_FOR_TEST, &envelope);

            let key_add = build_key_add(
                &fid_custody,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd],
                3600,
                1,
            );
            let block = commit_message(&mut block_engine, &key_add, Validity::Invalid);
            assert!(block.events.iter().all(|e| !matches!(
                e.data.as_ref().unwrap().r#type(),
                proto::BlockEventType::MergeMessage
            )));
        }

        // -- Concurrency-within-commit ---------------------------------------------------------

        #[tokio::test]
        async fn test_key_add_two_in_same_commit_same_fid_nonce_cas() {
            let (mut block_engine, _temp_dir) = setup();
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let env_a = signers::generate_signer();
            let env_b = signers::generate_signer();

            register_user_eth(
                FID_FOR_TEST,
                &fid_custody,
                default_signer(),
                &mut block_engine,
            );
            register_user_eth(
                REQUEST_FID,
                &app_custody,
                signers::generate_signer(),
                &mut block_engine,
            );

            // Both messages claim nonce=1 on the same FID. Within one batch,
            // check_and_set_user_nonce sees the staged write from `a` when validating `b`.
            let a = build_key_add(
                &fid_custody,
                &app_custody,
                &env_a,
                vec![MessageType::CastAdd],
                3600,
                1,
            );
            let b = build_key_add(
                &fid_custody,
                &app_custody,
                &env_b,
                vec![MessageType::CastAdd],
                3600,
                1,
            );
            let block = commit_messages(
                &mut block_engine,
                vec![(&a, Validity::Valid), (&b, Validity::Invalid)],
            );
            // Exactly one MergeMessage event (for `a`), no event for `b`.
            let merge_events: Vec<_> = block
                .events
                .iter()
                .filter(|e| {
                    matches!(
                        e.data.as_ref().unwrap().r#type(),
                        proto::BlockEventType::MergeMessage
                    )
                })
                .collect();
            assert_eq!(merge_events.len(), 1);
            assert_merge_message_event(merge_events[0], &a);
        }

        #[tokio::test]
        async fn test_key_add_two_in_same_commit_same_key_resubmission() {
            let (mut block_engine, _temp_dir) = setup();
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let envelope = signers::generate_signer();

            register_user_eth(
                FID_FOR_TEST,
                &fid_custody,
                default_signer(),
                &mut block_engine,
            );
            register_user_eth(
                REQUEST_FID,
                &app_custody,
                signers::generate_signer(),
                &mut block_engine,
            );

            // Same FID, same envelope key, same app — second is a same-app resubmission and
            // must hit the upsert path (gasless_key_merge.rs:180-199), with deleted_messages
            // carrying the first message.
            let first = build_key_add(
                &fid_custody,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd],
                3600,
                1,
            );
            let second = build_key_add(
                &fid_custody,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd, MessageType::ReactionAdd],
                7200,
                2,
            );
            // The upsert deletes `first`'s trie key when `second` merges, so `first` ends up
            // not-in-trie post-batch even though it merged successfully. The helper's per-msg
            // `in_trie` check therefore needs `Validity::Invalid` for `first`.
            let block = commit_messages(
                &mut block_engine,
                vec![(&first, Validity::Invalid), (&second, Validity::Valid)],
            );
            // Two MergeMessage events: first for `first`, second carrying `first` in
            // deleted_messages.
            let merge_events: Vec<_> = block
                .events
                .iter()
                .filter(|e| {
                    matches!(
                        e.data.as_ref().unwrap().r#type(),
                        proto::BlockEventType::MergeMessage
                    )
                })
                .collect();
            assert_eq!(merge_events.len(), 2);
            assert_merge_message_event(merge_events[0], &first);
            assert_merge_message_event(merge_events[1], &second);

            let txn = crate::storage::db::RocksDbTransactionBatch::new();
            let db = block_engine.stores().db.clone();
            let envelope_pubkey = envelope.verifying_key().to_bytes();
            let record = get_gasless_key_record(&db, &txn, FID_FOR_TEST, &envelope_pubkey)
                .unwrap()
                .expect("upsert leaves a single record under the latest message");
            // Stored record reflects the resubmission's broader scopes / longer ttl.
            let body = match record
                .message
                .as_ref()
                .unwrap()
                .data
                .as_ref()
                .unwrap()
                .body
                .as_ref()
                .unwrap()
            {
                Body::KeyAddBody(b) => b,
                _ => panic!("expected KeyAddBody"),
            };
            assert_eq!(body.ttl, 7200);
            assert_eq!(
                body.scopes,
                vec![MessageType::CastAdd as i32, MessageType::ReactionAdd as i32],
            );
        }

        // -- KEY_REMOVE custody path -----------------------------------------------------------

        #[tokio::test]
        async fn test_key_remove_custody_signature_merged() {
            let (mut block_engine, _temp_dir) = setup();
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let envelope = signers::generate_signer();
            let envelope_pubkey: [u8; 32] = envelope.verifying_key().to_bytes();

            register_user_eth(
                FID_FOR_TEST,
                &fid_custody,
                default_signer(),
                &mut block_engine,
            );
            register_user_eth(
                REQUEST_FID,
                &app_custody,
                signers::generate_signer(),
                &mut block_engine,
            );

            let key_add = build_key_add(
                &fid_custody,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd],
                3600,
                1,
            );
            commit_message(&mut block_engine, &key_add, Validity::Valid);

            // KEY_REMOVE custody. Envelope can be any active key on the FID — use the FID's
            // registered Ed25519 default signer (skips active-signer lookup via the bypass).
            let now = messages_factory::farcaster_time();
            let remove = messages_factory::keys::create_key_remove_custody(
                FID_FOR_TEST,
                &fid_custody,
                &default_signer(),
                &envelope_pubkey,
                2,
                now + 1_000_000,
                Some(now + 1),
            );
            let block = commit_message(&mut block_engine, &remove, Validity::Valid);
            let merge_events: Vec<_> = block
                .events
                .iter()
                .filter(|e| {
                    matches!(
                        e.data.as_ref().unwrap().r#type(),
                        proto::BlockEventType::MergeMessage
                    )
                })
                .collect();
            assert_eq!(merge_events.len(), 1);
            assert_merge_message_event(merge_events[0], &remove);

            // State assertions: record + owner + last_used_at all cleared.
            let txn = crate::storage::db::RocksDbTransactionBatch::new();
            let db = block_engine.stores().db.clone();
            assert!(
                get_gasless_key_record(&db, &txn, FID_FOR_TEST, &envelope_pubkey)
                    .unwrap()
                    .is_none()
            );
            assert!(get_gasless_key_owner_fid(&db, &txn, &envelope_pubkey)
                .unwrap()
                .is_none());
            assert_eq!(
                get_last_used_at(&db, &txn, FID_FOR_TEST, &envelope_pubkey).unwrap(),
                None,
            );
            // User nonce advanced to 2.
            assert_eq!(get_user_nonce(&db, &txn, FID_FOR_TEST).unwrap(), Some(2));
        }

        #[tokio::test]
        async fn test_key_remove_custody_rejects_invalid_signature() {
            let (mut block_engine, _temp_dir) = setup();
            let fid_custody = PrivateKeySigner::random();
            let wrong_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let envelope = signers::generate_signer();
            let envelope_pubkey: [u8; 32] = envelope.verifying_key().to_bytes();

            register_user_eth(
                FID_FOR_TEST,
                &fid_custody,
                default_signer(),
                &mut block_engine,
            );
            register_user_eth(
                REQUEST_FID,
                &app_custody,
                signers::generate_signer(),
                &mut block_engine,
            );
            let key_add = build_key_add(
                &fid_custody,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd],
                3600,
                1,
            );
            commit_message(&mut block_engine, &key_add, Validity::Valid);

            // Sign with wrong_custody so the EIP-712 recovery returns a different address than
            // FID_FOR_TEST's registered custody.
            let now = messages_factory::farcaster_time();
            let remove = messages_factory::keys::create_key_remove_custody(
                FID_FOR_TEST,
                &wrong_custody,
                &default_signer(),
                &envelope_pubkey,
                2,
                now + 1_000_000,
                Some(now + 1),
            );
            let block = commit_message(&mut block_engine, &remove, Validity::Invalid);
            // The KEY_ADD's MergeMessage already lives in a prior block; this block should have
            // no new MergeMessage event for the rejected KEY_REMOVE.
            let merge_events: Vec<_> = block
                .events
                .iter()
                .filter(|e| {
                    matches!(
                        e.data.as_ref().unwrap().r#type(),
                        proto::BlockEventType::MergeMessage
                    )
                })
                .collect();
            assert!(merge_events.is_empty());

            // Record still present.
            let txn = crate::storage::db::RocksDbTransactionBatch::new();
            let db = block_engine.stores().db.clone();
            assert!(
                get_gasless_key_record(&db, &txn, FID_FOR_TEST, &envelope_pubkey)
                    .unwrap()
                    .is_some()
            );
        }

        #[tokio::test]
        async fn test_key_remove_rejects_unknown_key() {
            let (mut block_engine, _temp_dir) = setup();
            let fid_custody = PrivateKeySigner::random();

            register_user_eth(
                FID_FOR_TEST,
                &fid_custody,
                default_signer(),
                &mut block_engine,
            );

            // No KEY_ADD landed — the gasless record for `unknown_key` does not exist.
            let unknown_key: [u8; 32] = signers::generate_signer().verifying_key().to_bytes();
            let now = messages_factory::farcaster_time();
            let remove = messages_factory::keys::create_key_remove_custody(
                FID_FOR_TEST,
                &fid_custody,
                &default_signer(),
                &unknown_key,
                1,
                now + 1_000_000,
                Some(now),
            );
            let block = commit_message(&mut block_engine, &remove, Validity::Invalid);
            assert!(block.events.iter().all(|e| !matches!(
                e.data.as_ref().unwrap().r#type(),
                proto::BlockEventType::MergeMessage
            )));
        }

        // -- KEY_REMOVE self-revoke path -------------------------------------------------------

        #[tokio::test]
        async fn test_key_remove_self_revoke_merged() {
            let (mut block_engine, _temp_dir) = setup();
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let envelope = signers::generate_signer();
            let envelope_pubkey: [u8; 32] = envelope.verifying_key().to_bytes();

            register_user_eth(
                FID_FOR_TEST,
                &fid_custody,
                default_signer(),
                &mut block_engine,
            );
            register_user_eth(
                REQUEST_FID,
                &app_custody,
                signers::generate_signer(),
                &mut block_engine,
            );
            let key_add = build_key_add(
                &fid_custody,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd],
                3600,
                1,
            );
            commit_message(&mut block_engine, &key_add, Validity::Valid);

            // Self-revoke: envelope IS the key being revoked. Body.signature is empty; envelope
            // Ed25519 sig + body.key match are sufficient. Nonce lives in the app namespace
            // for the verified request_fid (REQUEST_FID).
            let now = messages_factory::farcaster_time();
            let remove = messages_factory::keys::create_key_remove_self_revoke(
                FID_FOR_TEST,
                &envelope,
                1,
                now + 1_000_000,
                Some(now + 1),
            );
            let block = commit_message(&mut block_engine, &remove, Validity::Valid);
            let merge_events: Vec<_> = block
                .events
                .iter()
                .filter(|e| {
                    matches!(
                        e.data.as_ref().unwrap().r#type(),
                        proto::BlockEventType::MergeMessage
                    )
                })
                .collect();
            assert_eq!(merge_events.len(), 1);
            assert_merge_message_event(merge_events[0], &remove);

            let txn = crate::storage::db::RocksDbTransactionBatch::new();
            let db = block_engine.stores().db.clone();
            assert!(
                get_gasless_key_record(&db, &txn, FID_FOR_TEST, &envelope_pubkey)
                    .unwrap()
                    .is_none()
            );
        }

        #[tokio::test]
        async fn test_key_remove_self_revoke_rejects_envelope_signer_mismatch() {
            let (mut block_engine, _temp_dir) = setup();
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let envelope = signers::generate_signer();
            let envelope_pubkey: [u8; 32] = envelope.verifying_key().to_bytes();

            register_user_eth(
                FID_FOR_TEST,
                &fid_custody,
                default_signer(),
                &mut block_engine,
            );
            register_user_eth(
                REQUEST_FID,
                &app_custody,
                signers::generate_signer(),
                &mut block_engine,
            );
            let key_add = build_key_add(
                &fid_custody,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd],
                3600,
                1,
            );
            commit_message(&mut block_engine, &key_add, Validity::Valid);

            // Self-revoke targeting the gasless key, but the envelope is signed by a *different*
            // Ed25519 key (the FID's default signer). msg.signer != body.key → InvalidSignature.
            let now = messages_factory::farcaster_time();
            let other_signer = default_signer();
            let mut remove = messages_factory::keys::create_key_remove_self_revoke(
                FID_FOR_TEST,
                &other_signer,
                1,
                now + 1_000_000,
                Some(now + 1),
            );
            // Override body.key to point at the gasless key, then re-sign envelope.
            if let Some(Body::KeyRemoveBody(body)) = remove.data.as_mut().unwrap().body.as_mut() {
                body.key = envelope_pubkey.to_vec();
            }
            let remove = re_sign_envelope(remove, &other_signer);

            let block = commit_message(&mut block_engine, &remove, Validity::Invalid);
            assert!(block.events.iter().all(|e| !matches!(
                e.data.as_ref().unwrap().r#type(),
                proto::BlockEventType::MergeMessage
            )));

            // Gasless record must still be in place.
            let txn = crate::storage::db::RocksDbTransactionBatch::new();
            let db = block_engine.stores().db.clone();
            assert!(
                get_gasless_key_record(&db, &txn, FID_FOR_TEST, &envelope_pubkey)
                    .unwrap()
                    .is_some()
            );
        }

        // -- Active-key lookup smoke test (precondition for downstream cast tests) -------------

        #[tokio::test]
        async fn test_active_key_lookup_returns_gasless_record() {
            // Once a KEY_ADD merges, get_active_key (used by validate_user_message for non-key
            // messages) must surface the gasless registration. This is the precondition for the
            // ShardEngine cast-by-gasless-key tests; if it ever broke, the whole feature would
            // be dead code.
            let (mut block_engine, _temp_dir) = setup();
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let envelope = signers::generate_signer();
            let envelope_pubkey = envelope.verifying_key().to_bytes();

            register_user_eth(
                FID_FOR_TEST,
                &fid_custody,
                default_signer(),
                &mut block_engine,
            );
            register_user_eth(
                REQUEST_FID,
                &app_custody,
                signers::generate_signer(),
                &mut block_engine,
            );
            let key_add = build_key_add(
                &fid_custody,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd],
                3600,
                1,
            );
            commit_message(&mut block_engine, &key_add, Validity::Valid);

            let txn = crate::storage::db::RocksDbTransactionBatch::new();
            let db = block_engine.stores().db.clone();
            let active = get_active_key(
                &block_engine.stores().onchain_event_store,
                &db,
                &txn,
                FID_FOR_TEST,
                &envelope_pubkey,
            )
            .unwrap()
            .expect("active key lookup must surface the gasless record");
            match active {
                ActiveKey::Gasless { ttl_seconds, .. } => assert_eq!(ttl_seconds, 3600),
                ActiveKey::OnChain => {
                    panic!("expected ActiveKey::Gasless, got OnChain")
                }
            }
        }
    }
}
