#[cfg(test)]
mod tests {
    use crate::core::util::{
        calculate_message_hash, from_farcaster_time, get_farcaster_time, FarcasterTime,
    };
    use crate::proto::reaction_body::Target;
    use crate::proto::HubEvent;
    use crate::proto::{self, CastId, Embed, FarcasterNetwork, HubEventType, ReactionType};
    use crate::proto::{FnameTransfer, ShardChunk, UserNameProof};
    use crate::proto::{OnChainEvent, OnChainEventType};
    use crate::storage::db::{PageOptions, RocksDbTransactionBatch};
    use crate::storage::store::account::{HubEventIdGenerator, HubEventStorageExt, UserDataStore};
    use crate::storage::store::engine::{MessageValidationError, ShardEngine, ShardStateChange};
    use crate::storage::store::mempool_poller::{MempoolMessage, MempoolPoller};
    use crate::storage::store::stores::StoreLimits;
    use crate::storage::store::test_helper::{
        self, assert_block_confirmed_event, block_event_exists, commit_block_events, commit_event,
        commit_event_at, commit_message_at, commit_messages, default_custody_address,
        key_exists_in_trie, limits, trie_ctx, EngineOptions, FID3_FOR_TEST,
    };
    use crate::storage::store::test_helper::{
        commit_message, message_exists_in_trie, register_user, FID2_FOR_TEST, FID_FOR_TEST,
    };
    use crate::storage::trie::merkle_trie::TrieKey;
    use crate::utils::factory::events_factory::create_merge_message_event;
    use crate::utils::factory::signers::generate_signer;
    use crate::utils::factory::{self, events_factory, messages_factory, time, username_factory};
    use crate::version::version::{EngineVersion, ProtocolFeature};
    use base64::prelude::*;
    use ed25519_dalek::Signer;
    use informalsystems_malachitebft_core_types::Round;
    use prost::Message;

    fn from_hex(s: &str) -> Vec<u8> {
        hex::decode(s).unwrap()
    }

    fn to_hex(b: &[u8]) -> String {
        hex::encode(b)
    }

    fn default_message(text: &str) -> proto::Message {
        messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            text,
            Some(0),
            Some(&test_helper::default_signer()),
        )
    }

    fn default_onchain_event() -> OnChainEvent {
        events_factory::create_onchain_event(FID_FOR_TEST)
    }

    fn entities() -> (proto::Message, proto::Message) {
        let msg1 = default_message("msg1");
        let msg2 = default_message("msg2");

        assert_eq!(
            "eb1850b43b2dd25935222c9137f5fa71b02b9689",
            to_hex(&msg1.hash),
        );

        assert_eq!(
            "ee0fcb6344d22ea2af4f97859108eb5a3c6650fd",
            to_hex(&msg2.hash),
        );

        (msg1, msg2)
    }

    fn assert_event_id(event: &HubEvent, expected_block: Option<u64>, expected_event_seq: u64) {
        // Take the last 14 bits of event.id and assert it's equal to event_seq
        let (block, seq) = HubEventIdGenerator::extract_height_and_seq(event.id);
        if let Some(expected_block) = expected_block {
            assert_eq!(block, expected_block);
        }
        assert_eq!(seq, expected_event_seq);
    }

    fn assert_merge_event(event: &HubEvent, merged_message: &proto::Message, event_seq: u64) {
        let generated_event = match &event.body {
            Some(proto::hub_event::Body::MergeMessageBody(msg)) => msg,
            _ => panic!("Unexpected event type: {:?}", event.body),
        };
        assert_eq!(
            to_hex(&merged_message.hash),
            to_hex(&generated_event.message.as_ref().unwrap().hash)
        );
        assert_event_id(event, None, event_seq);
    }

    fn assert_prune_event(event: &HubEvent, pruned_message: &proto::Message, event_seq: u64) {
        let generated_event = match &event.body {
            Some(proto::hub_event::Body::PruneMessageBody(msg)) => msg,
            _ => panic!("Unexpected event type: {:?}", event.body),
        };
        assert_eq!(
            to_hex(&pruned_message.hash),
            to_hex(&generated_event.message.as_ref().unwrap().hash)
        );
        assert_event_id(event, None, event_seq);
    }

    fn assert_onchain_hub_event(event: &HubEvent, onchain_event: &OnChainEvent, event_seq: u64) {
        let generated_event = match &event.body {
            Some(proto::hub_event::Body::MergeOnChainEventBody(onchain)) => onchain,
            _ => panic!("Unexpected event type: {:?}", event.body),
        }
        .on_chain_event
        .as_ref()
        .unwrap();
        assert_eq!(
            to_hex(&onchain_event.transaction_hash),
            to_hex(&generated_event.transaction_hash)
        );
        assert_eq!(&onchain_event.r#type, &generated_event.r#type);
        assert_event_id(event, None, event_seq);
    }

    /// Helper function to receive the next event from the event receiver, optionally skipping BLOCK_CONFIRMED events
    async fn recv_next_event(
        event_rx: &mut tokio::sync::broadcast::Receiver<HubEvent>,
        skip_block_confirmed: bool,
    ) -> HubEvent {
        let event = event_rx.recv().await.unwrap();
        if skip_block_confirmed && event.r#type == proto::HubEventType::BlockConfirmed as i32 {
            // Skip BLOCK_CONFIRMED event and get the next one
            event_rx.recv().await.unwrap()
        } else {
            event
        }
    }

    /// Helper function to try receive the next event from the event receiver, optionally skipping BLOCK_CONFIRMED events
    fn try_recv_next_event(
        event_rx: &mut tokio::sync::broadcast::Receiver<HubEvent>,
        skip_block_confirmed: bool,
    ) -> Result<HubEvent, tokio::sync::broadcast::error::TryRecvError> {
        let event = event_rx.try_recv()?;
        if skip_block_confirmed && event.r#type == proto::HubEventType::BlockConfirmed as i32 {
            // Skip BLOCK_CONFIRMED event and get the next one
            event_rx.try_recv()
        } else {
            Ok(event)
        }
    }

    async fn assert_commit_fails(
        engine: &mut ShardEngine,
        msg: &proto::Message,
        error_code: &str,
        error_message: &str,
    ) -> ShardChunk {
        let state_change =
            engine.propose_state_change(1, vec![MempoolMessage::UserMessage(msg.clone())], None);

        if state_change.transactions.is_empty() {
            panic!("Failed to propose message");
        }

        let chunk = test_helper::validate_and_commit_state_change(engine, &state_change).await;
        assert_eq!(
            state_change.new_state_root,
            chunk.header.as_ref().unwrap().shard_root
        );
        // We don't fail the transaction for reject messages, they are just not included in the trie
        assert!(!message_exists_in_trie(engine, msg));

        assert_eq!(state_change.events.len(), 1);
        assert_failure_event(
            state_change.events[0].clone(),
            msg,
            error_code,
            error_message,
        );

        chunk
    }

    fn assert_failure_event(
        event: HubEvent,
        msg: &proto::Message,
        error_code: &str,
        error_message: &str,
    ) {
        assert_eq!(event.r#type, proto::HubEventType::MergeFailure as i32);
        let (err_code, err_msg) = match event.body {
            Some(proto::hub_event::Body::MergeFailure(body)) => {
                assert_eq!(&body.message.unwrap(), msg);
                (body.code, body.reason)
            }
            _ => panic!("Unexpected event type: {:?}", event.body),
        };
        assert_eq!(err_code, error_code);
        assert_eq!(err_msg, error_message);
    }

    #[tokio::test]
    async fn test_engine_basic_propose() {
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        // State root starts empty
        assert_eq!("", to_hex(&engine.trie_root_hash()));

        // Propose empty transaction
        let state_change = engine.propose_state_change(1, vec![], None);
        assert_eq!(1, state_change.shard_id);
        assert_eq!(state_change.transactions.len(), 0);
        // No messages so, new state root should be same as before
        assert_eq!("", to_hex(&state_change.new_state_root));
        // Root hash is not updated until commit
        assert_eq!("", to_hex(&engine.trie_root_hash()));

        // Propose a message that doesn't require storage
        let state_change = engine.propose_state_change(
            1,
            vec![MempoolMessage::OnchainEvent(
                events_factory::create_onchain_event(FID_FOR_TEST),
            )],
            None,
        );

        assert_eq!(1, state_change.shard_id);
        assert_eq!(state_change.transactions.len(), 1);
        assert_eq!(40, to_hex(&state_change.new_state_root).len());
        // Root hash is not updated until commit
        assert_eq!("", to_hex(&engine.trie_root_hash()));
    }

    #[tokio::test]
    #[should_panic(expected = "State change commit failed: merkle trie root hash mismatch")]
    async fn test_engine_commit_with_mismatched_hash() {
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        let mut state_change = engine.propose_state_change(1, vec![], None);
        let invalid_hash = from_hex("ffffffffffffffffffffffffffffffffffffffff");

        {
            let valid = engine
                .validate_state_change(&state_change, engine.get_confirmed_height().increment());
            assert!(valid);
        }

        {
            state_change.new_state_root = invalid_hash.clone();
            let valid = engine
                .validate_state_change(&state_change, engine.get_confirmed_height().increment());
            assert!(!valid);
        }

        let mut chunk = test_helper::default_shard_chunk();

        chunk.header.as_mut().unwrap().shard_root = invalid_hash;

        engine.commit_shard_chunk(&chunk).await;
    }

    #[tokio::test]
    async fn test_engine_rejects_message_with_invalid_hash() {
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;
        let mut message = default_message("msg1");
        let current_timestamp = message.data.as_ref().unwrap().timestamp;
        // Modify the message so the hash is no longer correct
        message.data.as_mut().unwrap().timestamp = current_timestamp + 1;

        assert_commit_fails(
            &mut engine,
            &message,
            "bad_request.validation_failure",
            "invalid hash",
        )
        .await;
    }

    #[tokio::test]
    async fn test_engine_rejects_message_with_invalid_signature() {
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;
        let mut message = default_message("msg1");
        let current_timestamp = message.data.as_ref().unwrap().timestamp;
        // Modify the message so the signatures is no longer correct
        message.data.as_mut().unwrap().timestamp = current_timestamp + 1;
        message.hash = calculate_message_hash(&message.data.as_ref().unwrap().encode_to_vec());

        assert_commit_fails(
            &mut engine,
            &message,
            "bad_request.validation_failure",
            "invalid signature",
        )
        .await;
    }

    #[tokio::test]
    async fn test_engine_commit_no_messages_happy_path() {
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        let state_change = engine.propose_state_change(1, vec![], None);
        let expected_roots = vec![""];

        test_helper::validate_and_commit_state_change(&mut engine, &state_change).await;

        assert_eq!(expected_roots[0], to_hex(&engine.trie_root_hash()));

        let valid =
            engine.validate_state_change(&state_change, engine.get_confirmed_height().increment());
        assert!(valid);
    }

    #[tokio::test]
    async fn test_engine_commit_with_single_message() {
        // enable_logging();
        let (msg1, _) = entities();
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        test_helper::register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;
        let mut event_rx = engine.get_senders().events_tx.subscribe();

        // Registering a user generates events
        let initial_events_count = HubEvent::get_events(engine.db.clone(), 0, None, None)
            .unwrap()
            .events
            .len();
        assert_eq!(6, initial_events_count);

        let state_change =
            engine.propose_state_change(1, vec![MempoolMessage::UserMessage(msg1.clone())], None);

        assert_eq!(1, state_change.transactions.len());
        assert_eq!(1, state_change.transactions[0].user_messages.len());

        // propose does not write to the store
        let casts_result = engine.get_casts_by_fid(msg1.fid());
        test_helper::assert_messages_empty(&casts_result);

        // No events are generated either
        let events = HubEvent::get_events(engine.db.clone(), 0, None, None).unwrap();
        assert_eq!(initial_events_count, events.events.len());

        // And it's not inserted into the trie
        assert_eq!(message_exists_in_trie(&mut engine, &msg1), false);

        let valid =
            engine.validate_state_change(&state_change, engine.get_confirmed_height().increment());
        assert!(valid);

        // validate does not write to the store
        let casts_result = engine.get_casts_by_fid(msg1.fid());
        test_helper::assert_messages_empty(&casts_result);

        test_helper::validate_and_commit_state_change(&mut engine, &state_change).await;

        // commit does write to the store
        let casts_result = engine.get_casts_by_fid(msg1.fid());
        test_helper::assert_contains_all_messages(&casts_result, &[&msg1]);

        // And events are generated
        let mut events = HubEvent::get_events(engine.db.clone(), 0, None, None).unwrap();
        assert_eq!(initial_events_count + 2, events.events.len()); // +2 for block confirmed and message event

        // Receive the merge message event (skipping block confirmed event)
        let mut generated_event = recv_next_event(&mut event_rx, true).await;
        // Timestamp is populated on the generated event but it's not stored in the db. Set to 0 for both so that the equality assertion doesn't fail.
        generated_event.timestamp = 0;
        events
            .events
            .get_mut(initial_events_count + 1)
            .unwrap()
            .timestamp = 0;
        assert_eq!(generated_event, events.events[initial_events_count + 1]);

        assert_merge_event(&generated_event, &msg1, 1);

        // The message exists in the trie
        assert_eq!(message_exists_in_trie(&mut engine, &msg1), true);
    }

    #[tokio::test]
    async fn test_engine_commit_delete_message() {
        let timestamp = messages_factory::farcaster_time();
        let cast =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "msg1", Some(timestamp), None);
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        test_helper::register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;

        commit_message(&mut engine, &cast).await;

        // The cast is present in the store and the trie
        let casts_result = engine.get_casts_by_fid(cast.fid());
        test_helper::assert_contains_all_messages(&casts_result.unwrap(), &[&cast]);
        assert_eq!(message_exists_in_trie(&mut engine, &cast), true);

        // Delete the cast
        let delete_cast = messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &cast.hash,
            Some(timestamp + 1),
            None,
        );

        commit_message(&mut engine, &delete_cast).await;

        // The cast is not present in the store
        let casts_result = engine.get_casts_by_fid(FID_FOR_TEST);
        let messages = casts_result.unwrap().messages;
        assert_eq!(0, messages.len());

        // The cast is not present in the trie, but the remove message is
        assert_eq!(message_exists_in_trie(&mut engine, &cast), false);
        assert_eq!(message_exists_in_trie(&mut engine, &delete_cast), true);
    }

    #[tokio::test]
    async fn test_commit_link_messages() {
        let timestamp = messages_factory::farcaster_time();
        let target_fid = 15;
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        test_helper::register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;

        let link_add1 = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            "follow",
            target_fid,
            Some(timestamp),
            None,
        );

        commit_message(&mut engine, &link_add1).await;
        let link_result = engine.get_links_by_fid(FID_FOR_TEST);
        assert_eq!(1, link_result.unwrap().messages.len());

        let link_add2 = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            "follow",
            target_fid + 1, // target fid is different from the target fid in the compact state
            Some(timestamp + 1),
            None,
        );

        commit_message(&mut engine, &link_add2).await;
        let link_result = engine.get_links_by_fid(FID_FOR_TEST);
        assert_eq!(2, link_result.unwrap().messages.len());

        let link_remove = messages_factory::links::create_link_remove(
            FID_FOR_TEST,
            "follow",
            target_fid,
            Some(timestamp + 2),
            None,
        );

        commit_message(&mut engine, &link_remove).await;

        let link_result = engine.get_links_by_fid(FID_FOR_TEST);
        assert_eq!(1, link_result.unwrap().messages.len());
        assert!(!message_exists_in_trie(&mut engine, &link_add1));

        let link_compact_state = messages_factory::links::create_link_compact_state(
            FID_FOR_TEST,
            "follow",
            vec![target_fid],
            Some(timestamp + 2),
            None,
        );

        commit_message(&mut engine, &link_compact_state).await;

        let link_result = engine.get_link_compact_state_messages_by_fid(FID_FOR_TEST);
        assert_eq!(1, link_result.unwrap().messages.len());
        let link_result = engine.get_links_by_fid(FID_FOR_TEST);
        assert_eq!(0, link_result.unwrap().messages.len());
        assert!(message_exists_in_trie(&mut engine, &link_compact_state));
        assert!(!message_exists_in_trie(&mut engine, &link_add2));
        assert!(!message_exists_in_trie(&mut engine, &link_remove))
    }

    #[tokio::test]
    async fn test_commit_reaction_messages() {
        let timestamp = messages_factory::farcaster_time();
        let target_url = "exampleurl".to_string();
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        test_helper::register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;

        let reaction_add = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            Target::TargetUrl(target_url.clone()),
            Some(timestamp),
            None,
        );

        commit_message(&mut engine, &reaction_add).await;

        let reaction_result = engine.get_reactions_by_fid(FID_FOR_TEST);
        assert_eq!(1, reaction_result.unwrap().messages.len());

        let reaction_remove = messages_factory::reactions::create_reaction_remove(
            FID_FOR_TEST,
            ReactionType::Like,
            Target::TargetUrl(target_url.clone()),
            Some(timestamp),
            None,
        );

        commit_message(&mut engine, &reaction_remove).await;

        let reaction_result = engine.get_reactions_by_fid(FID_FOR_TEST);
        assert_eq!(0, reaction_result.unwrap().messages.len());
    }

    #[tokio::test]
    async fn test_commit_user_data_messages() {
        let timestamp = messages_factory::farcaster_time();
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        test_helper::register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;

        let user_data_add = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST,
            proto::UserDataType::Bio,
            &"Hi it's me".to_string(),
            Some(timestamp),
            Some(&test_helper::default_signer()),
        );

        commit_message(&mut engine, &user_data_add).await;

        let user_data_result = engine.get_user_data_by_fid(FID_FOR_TEST);
        assert_eq!(1, user_data_result.unwrap().messages.len());
    }

    #[tokio::test]
    async fn test_commit_verification_messages() {
        let timestamp = messages_factory::farcaster_time();
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        test_helper::register_user(
            FID3_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;

        let verification_add = messages_factory::verifications::create_verification_add(
            FID3_FOR_TEST,
            0,
            hex::decode("91031dcfdea024b4d51e775486111d2b2a715871").unwrap(),
            hex::decode("b72c63d61f075b36fb66a9a867b50836cef19d653a3c09005628738677bcb25f25b6b6e6d2e1d69cd725327b3c020deef9e2575a22dc8ed08f88bc75718ce1cb1c").unwrap(),
            hex::decode("d74860c4bbf574d5ad60f03a478a30f990e05ac723e138a5c860cdb3095f4296").unwrap(),
            Some(timestamp),
            None,
        );

        commit_message(&mut engine, &verification_add).await;

        let verification_result = engine.get_verifications_by_fid(FID3_FOR_TEST);
        assert_eq!(1, verification_result.unwrap().messages.len());

        let verification_remove = messages_factory::verifications::create_verification_remove(
            FID3_FOR_TEST,
            hex::decode("91031dcfdea024b4d51e775486111d2b2a715871").unwrap(),
            Some(timestamp + 1),
            None,
        );

        commit_message(&mut engine, &verification_remove).await;

        let verification_result = engine.get_verifications_by_fid(FID_FOR_TEST);
        assert_eq!(0, verification_result.unwrap().messages.len());
    }

    #[tokio::test]
    async fn test_validate_ethereum_address_with_verification() {
        let timestamp = messages_factory::farcaster_time();
        let (mut engine, _tmpdir) = test_helper::new_engine().await;

        // Register a user
        test_helper::register_user(
            FID3_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;

        // Add Ethereum verification
        let eth_address = "91031dcfdea024b4d51e775486111d2b2a715871";
        let verification_add = messages_factory::verifications::create_verification_add(
            FID3_FOR_TEST,
            0, // Protocol::Ethereum
            hex::decode(eth_address).unwrap(),
            hex::decode("b72c63d61f075b36fb66a9a867b50836cef19d653a3c09005628738677bcb25f25b6b6e6d2e1d69cd725327b3c020deef9e2575a22dc8ed08f88bc75718ce1cb1c").unwrap(),
            hex::decode("d74860c4bbf574d5ad60f03a478a30f990e05ac723e138a5c860cdb3095f4296").unwrap(),
            Some(timestamp),
            None,
        );

        commit_message(&mut engine, &verification_add).await;

        // Verify the verification was added
        let verification_result = engine.get_verifications_by_fid(FID3_FOR_TEST);
        assert_eq!(1, verification_result.unwrap().messages.len());

        // Empty transaction batch
        let mut txn_batch = RocksDbTransactionBatch::new();

        // Now validate the Ethereum address verification
        let eth_address_bytes = hex::decode(eth_address).unwrap();
        let result = engine.verify_fid_owns_address(
            FID3_FOR_TEST,
            proto::Protocol::Ethereum,
            &eth_address_bytes,
            &mut txn_batch,
        );
        assert!(result.is_ok(), "Ethereum address validation should succeed");

        // Validate with wrong FID should fail
        let wrong_fid = FID3_FOR_TEST + 1;
        let wrong_fid_result = engine.verify_fid_owns_address(
            wrong_fid,
            proto::Protocol::Ethereum,
            &eth_address_bytes,
            &mut txn_batch,
        );
        assert!(
            wrong_fid_result.is_err(),
            "Validation with wrong FID should fail"
        );
        assert_eq!(
            wrong_fid_result.unwrap_err().to_string(),
            "address is not part of any verification",
            "Should fail with correct error message"
        );

        // Validate with wrong protocol (Solana) should fail
        let wrong_protocol_result = engine.verify_fid_owns_address(
            FID3_FOR_TEST,
            proto::Protocol::Solana,
            &eth_address_bytes,
            &mut txn_batch,
        );
        assert!(
            wrong_protocol_result.is_err(),
            "Validation with wrong protocol should fail"
        );
        assert_eq!(
            wrong_protocol_result.unwrap_err().to_string(),
            "address is not part of any verification",
            "Should fail with correct error message"
        );
    }

    #[tokio::test]
    async fn test_primary_address_revoked_when_verification_deleted() {
        let timestamp = messages_factory::farcaster_time();
        let (mut engine, _tmpdir) = test_helper::new_engine().await;

        // Register a user
        test_helper::register_user(
            FID3_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;

        // Test Ethereum primary address revocation
        let eth_address = "91031dcfdea024b4d51e775486111d2b2a715871";
        let eth_address_bytes = hex::decode(eth_address).unwrap();
        // Generate the proper checksummed address
        let address_instance = alloy_primitives::Address::from_slice(&eth_address_bytes);
        let eth_address_checksummed = address_instance.to_checksum(None);

        // Step 1: Add Ethereum verification
        let verification_add = messages_factory::verifications::create_verification_add(
            FID3_FOR_TEST,
            0, // Protocol::Ethereum
            eth_address_bytes.clone(),
            hex::decode("b72c63d61f075b36fb66a9a867b50836cef19d653a3c09005628738677bcb25f25b6b6e6d2e1d69cd725327b3c020deef9e2575a22dc8ed08f88bc75718ce1cb1c").unwrap(),
            hex::decode("d74860c4bbf574d5ad60f03a478a30f990e05ac723e138a5c860cdb3095f4296").unwrap(),
            Some(timestamp),
            None,
        );
        commit_message(&mut engine, &verification_add).await;

        // Step 2: Set the Ethereum address as primary address
        let primary_address_msg = messages_factory::user_data::create_user_data_add(
            FID3_FOR_TEST,
            proto::UserDataType::UserDataPrimaryAddressEthereum,
            &eth_address_checksummed.to_string(),
            Some(timestamp + 1),
            None,
        );
        commit_message(&mut engine, &primary_address_msg).await;

        // Verify the primary address was set
        let user_data_result = engine.get_user_data_by_fid_and_type(
            FID3_FOR_TEST,
            proto::UserDataType::UserDataPrimaryAddressEthereum,
        );
        assert!(user_data_result.is_ok(), "Primary address should be set");
        let user_data = user_data_result.unwrap();
        if let Some(data) = &user_data.data {
            if let Some(proto::message_data::Body::UserDataBody(body)) = &data.body {
                assert_eq!(
                    body.value, eth_address_checksummed,
                    "Primary address should match"
                );
            }
        }

        // Step 3: Remove the verification (this should also revoke the primary address)
        let verification_remove = messages_factory::verifications::create_verification_remove(
            FID3_FOR_TEST,
            eth_address_bytes.clone(),
            Some(timestamp + 2),
            None,
        );
        commit_message(&mut engine, &verification_remove).await;

        // Step 4: Verify the primary address was automatically revoked
        let user_data_result_after = engine.get_user_data_by_fid_and_type(
            FID3_FOR_TEST,
            proto::UserDataType::UserDataPrimaryAddressEthereum,
        );
        assert!(
            user_data_result_after.is_err(),
            "Primary address should be automatically revoked when verification is removed"
        );

        // Verify the user data message no longer exists in the trie
        assert!(
            !test_helper::message_exists_in_trie(&mut engine, &primary_address_msg),
            "User data message should not exist in trie after revocation"
        );

        // Verify verifications were actually removed
        let verification_result = engine.get_verifications_by_fid(FID3_FOR_TEST);
        assert_eq!(
            0,
            verification_result.unwrap().messages.len(),
            "All verifications should be removed"
        );
    }

    #[tokio::test]
    async fn test_primary_address_validation_requires_verification() {
        let timestamp = messages_factory::farcaster_time();
        let (mut engine, _tmpdir) = test_helper::new_engine().await;

        // Register a user
        test_helper::register_user(
            FID3_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;

        // Try to set a primary address without having a verification for it
        let eth_address = "1234567890abcdef1234567890abcdef12345678";
        let eth_address_bytes = hex::decode(eth_address).unwrap();
        let address_instance = alloy_primitives::Address::from_slice(&eth_address_bytes);
        let eth_address_checksummed = address_instance.to_checksum(None);

        let primary_address_msg = messages_factory::user_data::create_user_data_add(
            FID3_FOR_TEST,
            proto::UserDataType::UserDataPrimaryAddressEthereum,
            &eth_address_checksummed,
            Some(timestamp),
            None,
        );

        // This should fail validation
        assert_commit_fails(
            &mut engine,
            &primary_address_msg,
            "bad_request.validation_failure",
            "address is not part of any verification",
        )
        .await;

        // Verify no primary address was set
        let user_data_result = engine.get_user_data_by_fid_and_type(
            FID3_FOR_TEST,
            proto::UserDataType::UserDataPrimaryAddressEthereum,
        );
        assert!(
            user_data_result.is_err(),
            "Primary address should not be set without verification"
        );
    }

    #[tokio::test]
    async fn test_removing_non_primary_verification_keeps_primary_address() {
        let timestamp = messages_factory::farcaster_time();
        let (mut engine, _tmpdir) = test_helper::new_engine().await;

        // Register a user
        test_helper::register_user(
            FID3_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;

        // Add 2 verifications
        let primary_address = "91031dcfdea024b4d51e775486111d2b2a715871";
        let primary_address_bytes = hex::decode(primary_address).unwrap();
        let primary_address_instance =
            alloy_primitives::Address::from_slice(&primary_address_bytes);
        let primary_address_checksummed = primary_address_instance.to_checksum(None);

        // Add verification for primary address
        let primary_verification_add = messages_factory::verifications::create_verification_add(
            FID3_FOR_TEST,
            0, // Protocol::Ethereum
            primary_address_bytes.clone(),
            hex::decode("b72c63d61f075b36fb66a9a867b50836cef19d653a3c09005628738677bcb25f25b6b6e6d2e1d69cd725327b3c020deef9e2575a22dc8ed08f88bc75718ce1cb1c").unwrap(),
            hex::decode("d74860c4bbf574d5ad60f03a478a30f990e05ac723e138a5c860cdb3095f4296").unwrap(),
            Some(timestamp),
            None,
        );
        commit_message(&mut engine, &primary_verification_add).await;

        let other_address = "182327170fc284caaa5b1bc3e3878233f529d741";
        let other_address_bytes = hex::decode(other_address).unwrap();

        // Add verification for other address owned by FID 2
        let other_verification_add = messages_factory::verifications::create_verification_add(
            FID3_FOR_TEST,
            0, // Protocol::Ethereum
            other_address_bytes.clone(),
            BASE64_STANDARD.decode("TaU+v+BdZnIJc5CBir69j1taejse9uFgFSUOx3AYH1t7rPH6p8YlAmTbO9poXMRunbGcAmtGibn0DL1wXmIEkhs=").unwrap(),
            hex::decode("e9ddee7d7fe82a1f326b8c624b9c8031ba7561bf9d92c76067a9d0c01b5ba424").unwrap(),
            Some(timestamp + 1),
            None,
        );
        commit_message(&mut engine, &other_verification_add).await;

        // Set the primary address
        let primary_address_msg = messages_factory::user_data::create_user_data_add(
            FID3_FOR_TEST,
            proto::UserDataType::UserDataPrimaryAddressEthereum,
            &primary_address_checksummed,
            Some(timestamp + 1),
            None,
        );
        commit_message(&mut engine, &primary_address_msg).await;

        // Verify the primary address was set
        let user_data_result = engine.get_user_data_by_fid_and_type(
            FID3_FOR_TEST,
            proto::UserDataType::UserDataPrimaryAddressEthereum,
        );
        assert!(user_data_result.is_ok(), "Primary address should be set");

        // Try to remove a verification for the secondary address
        let other_verification_remove = messages_factory::verifications::create_verification_remove(
            FID3_FOR_TEST,
            other_address_bytes.clone(),
            Some(timestamp + 2),
            None,
        );

        // This should succeed
        commit_message(&mut engine, &other_verification_remove).await;

        // Verify the primary address is STILL set (should not be revoked)
        let user_data_result_after = engine.get_user_data_by_fid_and_type(
            FID3_FOR_TEST,
            proto::UserDataType::UserDataPrimaryAddressEthereum,
        );
        assert!(
            user_data_result_after.is_ok(),
            "Primary address should still be set when removing non-primary verification"
        );
        let user_data_after = user_data_result_after.unwrap();
        if let Some(data) = &user_data_after.data {
            if let Some(proto::message_data::Body::UserDataBody(body)) = &data.body {
                assert_eq!(
                    body.value, primary_address_checksummed,
                    "Primary address should still match after attempting to remove other verification"
                );
            }
        }

        // Verify the user data message still exists in the trie
        assert!(
            test_helper::message_exists_in_trie(&mut engine, &primary_address_msg),
            "User data message should still exist in trie when removing non-primary verification"
        );

        // Verify we still have the original verification
        let verification_result = engine.get_verifications_by_fid(FID3_FOR_TEST);
        assert_eq!(
            1,
            verification_result.unwrap().messages.len(),
            "Should still have the original verification"
        );
    }

    #[tokio::test]
    async fn test_commit_username_proof_messages() {
        let timestamp = messages_factory::farcaster_time();
        let (mut engine, _tmpdir) = test_helper::new_engine_with_options(EngineOptions {
            network: Some(FarcasterNetwork::Testnet), // To test basename support
            ..Default::default()
        })
        .await;
        let owner = "owner".to_string().encode_to_vec();
        let signature = "signature".to_string();
        let signer = test_helper::default_signer();

        test_helper::register_user(
            FID_FOR_TEST,
            signer.clone(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;

        let user_data_add = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST,
            proto::UserDataType::Username,
            &"username.eth".to_string(),
            Some(timestamp),
            Some(&signer),
        );
        // Cannot set username without a username proof
        assert_commit_fails(
            &mut engine,
            &user_data_add,
            "not_found",
            "NotFound: Username proof not found for name username.eth",
        )
        .await;

        let username_proof_add = messages_factory::username_proof::create_username_proof(
            FID_FOR_TEST as u64,
            proto::UserNameType::UsernameTypeEnsL1,
            "username.eth".to_string().clone(),
            owner.clone(),
            signature.clone(),
            timestamp as u64,
            Some(&signer),
        );

        let base_username_proof_add = messages_factory::username_proof::create_username_proof(
            FID_FOR_TEST as u64,
            proto::UserNameType::UsernameTypeBasename,
            "username.base.eth".to_string().clone(),
            owner,
            signature.clone(),
            timestamp as u64,
            Some(&signer),
        );

        commit_message(&mut engine, &username_proof_add).await;

        // Cannot add basenames on old engine versions
        let before_base_support = &FarcasterTime::from_unix_seconds(1748950000);
        assert_eq!(
            engine
                .version_for(before_base_support)
                .is_enabled(ProtocolFeature::Basenames),
            false
        );
        commit_message_at(&mut engine, &base_username_proof_add, before_base_support).await;
        assert!(!TrieKey::for_message(&base_username_proof_add)
            .iter()
            .all(|key| engine.trie_key_exists(&trie_ctx(), &key)));

        // Works on the latest engine version
        commit_message(&mut engine, &base_username_proof_add).await;

        {
            let username_proof_result = engine.get_username_proofs_by_fid(FID2_FOR_TEST);
            assert!(username_proof_result.is_ok());

            let messages_bytes_len = username_proof_result.unwrap().messages.len();
            assert_eq!(0, messages_bytes_len);
        }
        {
            let username_proof_result = engine.get_username_proofs_by_fid(FID_FOR_TEST);
            assert!(username_proof_result.is_ok());

            let messages_len = username_proof_result.unwrap().messages.len();
            assert_eq!(2, messages_len);
        }

        // Allows setting the proof as the username
        let userdata_add = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST,
            proto::UserDataType::Username,
            &"username.eth".to_string(),
            Some(timestamp + 1),
            Some(&signer),
        );
        commit_message(&mut engine, &userdata_add).await;

        let base_userdata_add = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST,
            proto::UserDataType::Username,
            &"username.base.eth".to_string(),
            Some(timestamp + 2),
            Some(&signer),
        );
        commit_message(&mut engine, &base_userdata_add).await;
    }

    #[tokio::test]
    async fn test_account_roots() {
        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "msg1", None, None);
        let (mut engine, _tmpdir) = test_helper::new_engine().await;

        let txn = &mut RocksDbTransactionBatch::new();
        let account_root = engine
            .get_stores()
            .trie
            .get_hash(&engine.db, txn, &TrieKey::for_fid(FID_FOR_TEST))
            .unwrap();
        let shard_root = engine.get_stores().trie.root_hash().unwrap();

        // Account root and shard root is empty initially
        assert_eq!(account_root.len(), 0);
        assert_eq!(shard_root.len(), 0);

        test_helper::register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;
        commit_message(&mut engine, &cast).await;

        let updated_account_root = engine
            .get_stores()
            .trie
            .get_hash(&engine.db, txn, &TrieKey::for_fid(FID_FOR_TEST))
            .unwrap();
        let updated_shard_root = engine.get_stores().trie.root_hash().unwrap();
        // Account root is not empty after a message is committed
        assert_eq!(updated_account_root.len() > 0, true);
        assert_ne!(updated_shard_root, shard_root);

        let another_fid_event = events_factory::create_onchain_event(FID_FOR_TEST + 1);
        test_helper::commit_event(&mut engine, &another_fid_event).await;

        let account_root_another_fid = engine
            .get_stores()
            .trie
            .get_hash(&engine.db, txn, &TrieKey::for_fid(FID_FOR_TEST + 1))
            .unwrap();
        let account_root_original_fid = engine
            .get_stores()
            .trie
            .get_hash(&engine.db, txn, &TrieKey::for_fid(FID_FOR_TEST))
            .unwrap();
        let latest_shard_root = engine.get_stores().trie.root_hash().unwrap();
        // Only the account root for the new fid and the shard root is updated, original fid account root remains the same
        assert_eq!(account_root_another_fid.len() > 0, true);
        assert_eq!(account_root_original_fid, updated_account_root);
        assert_ne!(latest_shard_root, updated_shard_root);
    }

    #[tokio::test]
    async fn test_engine_send_messages_one_by_one() {
        // enable_logging();
        let (msg1, msg2) = entities();
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        let mut previous_root = "".to_string();

        let height = engine.get_confirmed_height();
        assert_eq!(height.shard_index, 1);
        assert_eq!(height.block_number, 0);

        test_helper::register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;

        {
            let state_change = engine.propose_state_change(
                1,
                vec![MempoolMessage::UserMessage(msg1.clone())],
                None,
            );

            assert_eq!(1, state_change.shard_id);
            assert_eq!(state_change.transactions.len(), 1);
            assert_eq!(1, state_change.transactions[0].user_messages.len());

            let prop_msg = &state_change.transactions[0].user_messages[0];
            assert_eq!(to_hex(&prop_msg.hash), to_hex(&msg1.hash));

            assert_ne!(previous_root, to_hex(&state_change.new_state_root));
            previous_root = to_hex(&state_change.new_state_root);

            test_helper::validate_and_commit_state_change(&mut engine, &state_change).await;

            assert_eq!(previous_root, to_hex(&engine.trie_root_hash()));

            let height = engine.get_confirmed_height();
            assert_eq!(height.shard_index, 1);
            // assert_eq!(height.block_number, 1); // TODO
        }

        {
            let state_change = engine.propose_state_change(
                1,
                vec![MempoolMessage::UserMessage(msg2.clone())],
                None,
            );

            assert_eq!(1, state_change.shard_id);
            assert_eq!(state_change.transactions.len(), 1);
            assert_eq!(1, state_change.transactions[0].user_messages.len());

            let prop_msg = &state_change.transactions[0].user_messages[0];
            assert_eq!(to_hex(&prop_msg.hash), to_hex(&msg2.hash));

            assert_ne!(previous_root, to_hex(&state_change.new_state_root));
            previous_root = to_hex(&state_change.new_state_root);

            test_helper::validate_and_commit_state_change(&mut engine, &state_change).await;

            assert_eq!(previous_root, to_hex(&engine.trie_root_hash()));

            let height = engine.get_confirmed_height();
            assert_eq!(height.shard_index, 1);
            // assert_eq!(height.block_number, 2); // TODO
        }
    }

    #[tokio::test]
    async fn test_engine_send_two_messages() {
        // enable_logging();
        let (msg1, msg2) = entities();
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        test_helper::register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;
        let mut previous_root = "".to_string();

        {
            let messages = vec![
                MempoolMessage::UserMessage(msg1.clone()),
                MempoolMessage::UserMessage(msg2.clone()),
            ];
            let state_change = engine.propose_state_change(1, messages, None);

            assert_eq!(1, state_change.shard_id);
            assert_eq!(state_change.transactions.len(), 1);
            assert_eq!(2, state_change.transactions[0].user_messages.len());

            let prop_msg_1 = &state_change.transactions[0].user_messages[0];
            assert_eq!(to_hex(&prop_msg_1.hash), to_hex(&msg1.hash));

            let prop_msg_2 = &state_change.transactions[0].user_messages[1];
            assert_eq!(to_hex(&prop_msg_2.hash), to_hex(&msg2.hash));

            // State root has changed
            assert_ne!(previous_root, to_hex(&state_change.new_state_root));
            previous_root = to_hex(&state_change.new_state_root);

            test_helper::validate_and_commit_state_change(&mut engine, &state_change).await;

            // Committed state root is the same as what was proposed
            assert_eq!(previous_root, to_hex(&engine.trie_root_hash()));

            let height = engine.get_confirmed_height();
            assert_eq!(height.shard_index, 1);
            // assert_eq!(height.block_number, 1); // TODO
        }
    }

    #[tokio::test]
    async fn test_simulate_bulk_messages_invalid_message_in_batch() {
        let (mut engine, _tmpdir) = test_helper::new_engine().await;

        // 1. Register user
        register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;

        let initial_root_hash = engine.trie_root_hash();

        // 2. Create a batch with a valid message followed by an invalid one.
        let valid_cast = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "This is a valid message.",
            None,
            Some(&test_helper::default_signer()),
        );
        let invalid_cast_text = "a".repeat(321); // Exceeds CastAdd limit
        let invalid_cast = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            &invalid_cast_text,
            None,
            Some(&test_helper::default_signer()),
        );

        let messages_batch = vec![valid_cast.clone(), invalid_cast.clone()];

        // 3. Simulate the batch
        let result = engine.simulate_bulk_messages(&messages_batch);

        println!("Simulation result: {:?}", result);

        // 4. Assert failure and state integrity
        assert!(
            result[0].is_err(),
            "If there are any errors, the first message should fail"
        );
        assert!(
            result[1].is_err(),
            "Simulation should fail for second message"
        );

        let validation_error = result[1].as_ref().unwrap_err();
        assert!(
            matches!(
                validation_error,
                MessageValidationError::MessageValidationError(
                    crate::core::validations::error::ValidationError::TextTooLong
                )
            ),
            "Error should be for the invalid message"
        );

        let final_root_hash = engine.trie_root_hash();
        assert_eq!(
            initial_root_hash, final_root_hash,
            "Trie root should not change after a failed simulation"
        );

        // Verify that NEITHER message was committed, confirming atomicity
        assert!(
            !message_exists_in_trie(&mut engine, &valid_cast),
            "Valid message should not be in the trie after a failed batch simulation"
        );
        assert!(
            !message_exists_in_trie(&mut engine, &invalid_cast),
            "Invalid message should not be in the trie"
        );
    }

    #[tokio::test]
    async fn test_simulate_bulk_messages_empty_batch() {
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;

        let initial_root_hash = engine.trie_root_hash();

        let messages_batch: Vec<proto::Message> = vec![];

        let result = engine.simulate_bulk_messages(&messages_batch);

        assert!(
            result.len() == 0,
            "Simulating an empty batch should succeed"
        );

        let final_root_hash = engine.trie_root_hash();
        assert_eq!(
            initial_root_hash, final_root_hash,
            "Trie root should not change after simulating an empty batch"
        );
    }

    #[tokio::test]
    async fn test_simulate_bulk_messages_valid_batch() {
        let (mut engine, _tmpdir) = test_helper::new_engine().await;

        // 1. Register user
        register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;

        let initial_root_hash = engine.trie_root_hash();

        // 2. Create a batch with a valid message followed by an invalid one.
        let valid_cast1 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "This is a valid message.",
            None,
            Some(&test_helper::default_signer()),
        );

        let valid_cast2 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "This is another valid message.",
            None,
            Some(&test_helper::default_signer()),
        );

        let messages_batch = vec![valid_cast1.clone(), valid_cast2.clone()];

        // 3. Simulate the batch
        let result = engine.simulate_bulk_messages(&messages_batch);

        // 4. Assert all success and state integrity
        assert!(
            result[0].is_ok(),
            "Simulation should succeed for first message"
        );
        assert!(
            result[1].is_ok(),
            "Simulation should succeed for second message"
        );

        let final_root_hash = engine.trie_root_hash();
        assert_eq!(
            initial_root_hash, final_root_hash,
            "Trie root should not change after a failed simulation"
        );

        // Verify that NEITHER message was committed, confirming atomicity
        assert!(
            !message_exists_in_trie(&mut engine, &valid_cast1),
            "Valid message should not be in the trie after a simulation"
        );
        assert!(
            !message_exists_in_trie(&mut engine, &valid_cast2),
            "Valid message should not be in the trie after a simulation"
        );
    }

    #[tokio::test]
    async fn test_simulate_bulk_messages_username_proof_and_user_data_add() {
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        let signer = test_helper::default_signer();
        let owner = test_helper::default_custody_address();
        let timestamp = time::farcaster_time();
        let username = "testuser.eth".to_string();

        // 1. Register user
        register_user(FID_FOR_TEST, signer.clone(), owner.clone(), &mut engine).await;

        // Verify that setting the username fails before the proof is available
        let initial_user_data_add = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST,
            proto::UserDataType::Username,
            &username,
            Some(timestamp),
            Some(&signer),
        );
        let pre_check_result = engine.simulate_message(&initial_user_data_add);
        assert!(
            pre_check_result.is_err(),
            "Setting username should fail without a proof"
        );

        let initial_root_hash = engine.trie_root_hash();

        // 2. Create a batch: 1. Add UsernameProof, 2. Set username with UserDataAdd
        let username_proof_add = messages_factory::username_proof::create_username_proof(
            FID_FOR_TEST,
            proto::UserNameType::UsernameTypeEnsL1,
            username.clone(),
            owner,
            "signature".to_string(), // Signature is not validated in this path
            timestamp as u64,
            Some(&signer),
        );

        let user_data_add = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST,
            proto::UserDataType::Username,
            &username,
            Some(timestamp + 1), // Timestamp must be after the proof
            Some(&signer),
        );

        let messages_batch = vec![username_proof_add.clone(), user_data_add.clone()];

        // 3. Simulate the batch
        let result = engine.simulate_bulk_messages(&messages_batch);

        println!("Simulation result: {:?}", result);

        // 4. Assert success and state integrity
        assert!(
            result.iter().all(|r| r.is_ok()),
            "Simulation of UsernameProof and dependent UserDataAdd should succeed"
        );

        // 5. Verify that the engine's state has not been modified
        let final_root_hash = engine.trie_root_hash();
        assert_eq!(
            initial_root_hash, final_root_hash,
            "Trie root should not change after a successful simulation"
        );

        // Verify that neither message was actually committed to the trie or DB
        assert!(
            !message_exists_in_trie(&mut engine, &username_proof_add),
            "UsernameProof should not be in the trie after simulation"
        );
        assert!(
            !message_exists_in_trie(&mut engine, &user_data_add),
            "UserDataAdd should not be in the trie after simulation"
        );
    }

    #[tokio::test]
    async fn test_bulk_username_proof_and_user_data_add_commit() {
        // 1. Setup
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        let signer = test_helper::default_signer();
        let owner = test_helper::default_custody_address();
        let timestamp = time::farcaster_time();
        let username = "testuser.eth".to_string();

        register_user(FID_FOR_TEST, signer.clone(), owner.clone(), &mut engine).await;
        let mut event_rx = engine.get_senders().events_tx.subscribe();
        let initial_root_hash = engine.trie_root_hash();

        // 2. Create a batch: 1. Add UsernameProof, 2. Set username with UserDataAdd
        let username_proof_add = messages_factory::username_proof::create_username_proof(
            FID_FOR_TEST,
            proto::UserNameType::UsernameTypeEnsL1,
            username.clone(),
            owner,
            "signature".to_string(), // Signature is not validated in this path for devnet
            timestamp as u64,
            Some(&signer),
        );

        let user_data_add = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST,
            proto::UserDataType::Username,
            &username,
            Some(timestamp + 1), // Timestamp must be after the proof
            Some(&signer),
        );

        let messages_batch = vec![
            MempoolMessage::UserMessage(user_data_add.clone()),
            MempoolMessage::UserMessage(username_proof_add.clone()),
        ];

        // 3. Propose and commit the batch of messages
        let state_change = engine.propose_state_change(1, messages_batch, None);
        test_helper::validate_and_commit_state_change(&mut engine, &state_change).await;

        // 4. Assertions
        let final_root_hash = engine.trie_root_hash();
        assert_ne!(
            initial_root_hash, final_root_hash,
            "Trie root should change after a successful commit"
        );
        assert_eq!(
            state_change.new_state_root, final_root_hash,
            "Final trie root should match the state change root"
        );

        // Assert that both messages exist in the trie
        assert!(
            message_exists_in_trie(&mut engine, &username_proof_add),
            "UsernameProof message should be in the trie after commit"
        );
        assert!(
            message_exists_in_trie(&mut engine, &user_data_add),
            "UserDataAdd message should be in the trie after commit"
        );

        // Assert that both messages exist in their respective stores
        let proof_result = engine.get_username_proofs_by_fid(FID_FOR_TEST).unwrap();
        assert_eq!(
            proof_result.messages.len(),
            1,
            "UsernameProof should be in the store"
        );
        assert_eq!(proof_result.messages[0].hash, username_proof_add.hash);

        let user_data_result = engine.get_user_data_by_fid(FID_FOR_TEST).unwrap();
        assert_eq!(
            user_data_result.messages.len(),
            1,
            "UserDataAdd should be in the store"
        );
        assert_eq!(user_data_result.messages[0].hash, user_data_add.hash);

        // Assert that the correct events were emitted
        let block_confirmed_event = recv_next_event(&mut event_rx, false).await;
        assert_eq!(
            block_confirmed_event.r#type,
            HubEventType::BlockConfirmed as i32
        );

        let event1 = recv_next_event(&mut event_rx, false).await;
        let event2 = recv_next_event(&mut event_rx, false).await;

        // We need to check for both event types, as their order is not guaranteed.
        let mut seen_username_proof_event = false;
        let mut seen_user_data_event = false;

        // Check first event
        match event1.body.as_ref().unwrap() {
            proto::hub_event::Body::MergeUsernameProofBody(body) => {
                assert_eq!(
                    body.username_proof_message.as_ref().unwrap().hash,
                    username_proof_add.hash
                );
                seen_username_proof_event = true;
            }
            proto::hub_event::Body::MergeMessageBody(body) => {
                assert_eq!(body.message.as_ref().unwrap().hash, user_data_add.hash);
                seen_user_data_event = true;
            }
            _ => panic!("Unexpected event type for event1"),
        }

        // Check second event
        match event2.body.as_ref().unwrap() {
            proto::hub_event::Body::MergeUsernameProofBody(body) => {
                assert_eq!(
                    body.username_proof_message.as_ref().unwrap().hash,
                    username_proof_add.hash
                );
                seen_username_proof_event = true;
            }
            proto::hub_event::Body::MergeMessageBody(body) => {
                assert_eq!(body.message.as_ref().unwrap().hash, user_data_add.hash);
                seen_user_data_event = true;
            }
            _ => panic!("Unexpected event type for event2"),
        }

        assert!(
            seen_username_proof_event,
            "MergeUsernameProof event was not seen"
        );
        assert!(seen_user_data_event, "MergeMessage event was not seen");

        // Ensure no other events were emitted
        assert!(
            try_recv_next_event(&mut event_rx, false).is_err(),
            "There should be no more events"
        );
    }

    #[tokio::test]
    async fn test_simulate_verification_and_user_name_proof() {
        // 1. Setup
        let (mut engine, _tmpdir) = test_helper::new_engine_with_options(EngineOptions {
            network: Some(FarcasterNetwork::Testnet), // To test ENS support
            ..Default::default()
        })
        .await;
        let signer = test_helper::default_signer();
        let custody_address = test_helper::default_custody_address();
        let timestamp = time::farcaster_time();
        let username = "testuser.eth".to_string();

        register_user(FID3_FOR_TEST, signer.clone(), custody_address, &mut engine).await;

        let initial_root_hash = engine.trie_root_hash();

        // 2. Create a batch:
        //    - Message 1: Verify the owner_address for the FID
        //    - Message 2: Add a UsernameProof for an ENS name owned by owner_address
        let owner_address = hex::decode("91031dcfdea024b4d51e775486111d2b2a715871").unwrap();
        let verification_add = messages_factory::verifications::create_verification_add(
            FID3_FOR_TEST,
            0, // EOA verification
            owner_address.clone(),
            hex::decode("b72c63d61f075b36fb66a9a867b50836cef19d653a3c09005628738677bcb25f25b6b6e6d2e1d69cd725327b3c020deef9e2575a22dc8ed08f88bc75718ce1cb1c").unwrap(),
            hex::decode("d74860c4bbf574d5ad60f03a478a30f990e05ac723e138a5c860cdb3095f4296").unwrap(),
            Some(timestamp),
            Some(&signer),
        );

        let username_proof_add = messages_factory::username_proof::create_username_proof(
            FID3_FOR_TEST,
            proto::UserNameType::UsernameTypeEnsL1,
            username.clone(),
            owner_address,
            "signature".to_string(), // Signature is not validated in this path for devnet
            (timestamp + 1) as u64,
            Some(&signer),
        );

        let messages_batch = vec![verification_add.clone(), username_proof_add.clone()];

        // 3. Simulate the batch
        let result = engine.simulate_bulk_messages(&messages_batch);

        // 4. Assert success and state integrity
        assert!(
            result.iter().all(|r| r.is_ok()),
            "Simulation of verification and dependent username proof should succeed"
        );

        // 5. Verify that the engine's state has not been modified
        let final_root_hash = engine.trie_root_hash();
        assert_eq!(
            initial_root_hash, final_root_hash,
            "Trie root should not change after a successful simulation"
        );

        // Verify that neither message was actually committed to the trie or DB
        assert!(
            !message_exists_in_trie(&mut engine, &verification_add),
            "Verification should not be in the trie after simulation"
        );
        assert!(
            !message_exists_in_trie(&mut engine, &username_proof_add),
            "UsernameProof should not be in the trie after simulation"
        );
    }

    #[tokio::test]
    async fn test_bulk_verification_and_user_name_proof() {
        // 1. Setup
        let (mut engine, _tmpdir) = test_helper::new_engine_with_options(EngineOptions {
            network: Some(FarcasterNetwork::Testnet), // To test ENS support
            ..Default::default()
        })
        .await;
        let signer = test_helper::default_signer();
        let custody_address = test_helper::default_custody_address();
        let timestamp = time::farcaster_time();
        let username = "testuser.eth".to_string();

        register_user(FID3_FOR_TEST, signer.clone(), custody_address, &mut engine).await;
        let mut event_rx = engine.get_senders().events_tx.subscribe();
        let initial_root_hash = engine.trie_root_hash();

        // 2. Create a batch:
        //    - Message 1: Verify the owner_address for the FID
        //    - Message 2: Add a UsernameProof for an ENS name owned by owner_address
        let owner_address = hex::decode("91031dcfdea024b4d51e775486111d2b2a715871").unwrap();
        let verification_add = messages_factory::verifications::create_verification_add(
            FID3_FOR_TEST,
            0, // EOA verification
            owner_address.clone(),
            hex::decode("b72c63d61f075b36fb66a9a867b50836cef19d653a3c09005628738677bcb25f25b6b6e6d2e1d69cd725327b3c020deef9e2575a22dc8ed08f88bc75718ce1cb1c").unwrap(),
            hex::decode("d74860c4bbf574d5ad60f03a478a30f990e05ac723e138a5c860cdb3095f4296").unwrap(),
            Some(timestamp),
            Some(&signer),
        );

        let username_proof_add = messages_factory::username_proof::create_username_proof(
            FID3_FOR_TEST,
            proto::UserNameType::UsernameTypeEnsL1,
            username.clone(),
            owner_address,
            "signature".to_string(), // Signature is not validated in this path for devnet
            (timestamp + 1) as u64,
            Some(&signer),
        );

        let messages_batch = vec![
            MempoolMessage::UserMessage(verification_add.clone()),
            MempoolMessage::UserMessage(username_proof_add.clone()),
        ];

        // 3. Propose and commit the batch of messages
        let state_change = engine.propose_state_change(1, messages_batch, None);
        test_helper::validate_and_commit_state_change(&mut engine, &state_change).await;

        // 4. Assertions
        let final_root_hash = engine.trie_root_hash();
        assert_ne!(
            initial_root_hash, final_root_hash,
            "Trie root should change after a successful commit"
        );
        assert_eq!(
            state_change.new_state_root, final_root_hash,
            "Final trie root should match the state change root"
        );

        // Assert that both messages exist in the trie (i.e., both succeeded and were committed)
        assert!(
            message_exists_in_trie(&mut engine, &verification_add),
            "VerificationAdd message should be in the trie after commit"
        );
        assert!(
            message_exists_in_trie(&mut engine, &username_proof_add),
            "UsernameProof message should be in the trie after commit"
        );

        // Assert that both messages exist in their respective stores
        let verification_result = engine.get_verifications_by_fid(FID3_FOR_TEST).unwrap();
        assert_eq!(
            verification_result.messages.len(),
            1,
            "Verification should be in the store"
        );
        assert_eq!(verification_result.messages[0].hash, verification_add.hash);

        let proof_result = engine.get_username_proofs_by_fid(FID3_FOR_TEST).unwrap();
        assert_eq!(
            proof_result.messages.len(),
            1,
            "UsernameProof should be in the store"
        );
        assert_eq!(proof_result.messages[0].hash, username_proof_add.hash);

        // Assert that the correct events were emitted
        let block_confirmed_event = recv_next_event(&mut event_rx, false).await;
        assert_eq!(
            block_confirmed_event.r#type,
            HubEventType::BlockConfirmed as i32
        );

        let event1 = recv_next_event(&mut event_rx, false).await;
        let event2 = recv_next_event(&mut event_rx, false).await;

        let mut seen_verification_event = false;
        let mut seen_username_proof_event = false;

        match event1.body.as_ref().unwrap() {
            proto::hub_event::Body::MergeMessageBody(body) => {
                if body.message.as_ref().unwrap().hash == verification_add.hash {
                    seen_verification_event = true;
                }
            }
            proto::hub_event::Body::MergeUsernameProofBody(body) => {
                if body.username_proof_message.as_ref().unwrap().hash == username_proof_add.hash {
                    seen_username_proof_event = true;
                }
            }
            _ => panic!("Unexpected event type for event1"),
        }
        match event2.body.as_ref().unwrap() {
            proto::hub_event::Body::MergeMessageBody(body) => {
                if body.message.as_ref().unwrap().hash == verification_add.hash {
                    seen_verification_event = true;
                }
            }
            proto::hub_event::Body::MergeUsernameProofBody(body) => {
                if body.username_proof_message.as_ref().unwrap().hash == username_proof_add.hash {
                    seen_username_proof_event = true;
                }
            }
            _ => panic!("Unexpected event type for event2"),
        }

        assert!(
            seen_verification_event,
            "MergeMessage for verification event was not seen"
        );
        assert!(
            seen_username_proof_event,
            "MergeUsernameProof event was not seen"
        );

        assert!(
            try_recv_next_event(&mut event_rx, false).is_err(),
            "There should be no more events"
        );
    }

    #[tokio::test]
    async fn test_bulk_register_add_signer_and_cast_commit() {
        // 1. Setup
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        let mut event_rx = engine.get_senders().events_tx.subscribe();

        let new_fid = FID3_FOR_TEST;
        let new_signer = generate_signer();
        let custody_address = default_custody_address();
        let timestamp = time::farcaster_time();

        let initial_root_hash = engine.trie_root_hash();

        // 2. Create a batch of dependent on-chain events and a message
        //    Event 1: Register the new FID
        let id_register_event = events_factory::create_id_register_event(
            new_fid,
            proto::IdRegisterEventType::Register,
            custody_address,
            None,
        );

        //    Event 2: Add a new signer for this FID
        let signer_add_event = events_factory::create_signer_event(
            new_fid,
            new_signer.clone(),
            proto::SignerEventType::Add,
            None,
            None,
        );

        //    Message 3: Create a cast signed by the new signer
        let cast_add = messages_factory::casts::create_cast_add(
            new_fid,
            "Hello, Farcaster!",
            Some(timestamp),
            Some(&new_signer),
        );

        let messages_batch = vec![
            MempoolMessage::OnchainEvent(test_helper::default_storage_event(new_fid)),
            MempoolMessage::OnchainEvent(id_register_event.clone()),
            MempoolMessage::OnchainEvent(signer_add_event.clone()),
            MempoolMessage::UserMessage(cast_add.clone()),
        ];

        // 3. Propose and commit the entire batch
        let state_change = engine.propose_state_change(1, messages_batch, None);
        let chunk = test_helper::validate_and_commit_state_change(&mut engine, &state_change).await;

        // 4. Assertions

        // 4a. Assert state integrity
        let final_root_hash = engine.trie_root_hash();
        assert_ne!(
            initial_root_hash, final_root_hash,
            "Trie root should change after a successful commit"
        );
        assert_eq!(
            state_change.new_state_root, final_root_hash,
            "Final trie root should match the state change root"
        );
        assert_eq!(
            chunk.header.as_ref().unwrap().shard_root,
            final_root_hash,
            "ShardChunk root hash should match the final trie root"
        );

        // 4b. Assert existence in stores and trie
        let stores = engine.get_stores();

        // Verify ID registration
        let stored_id_event = stores
            .onchain_event_store
            .get_id_register_event_by_fid(new_fid, None)
            .unwrap();
        assert!(
            stored_id_event.is_some(),
            "ID Register event should be in the store"
        );
        assert_eq!(
            stored_id_event.unwrap().transaction_hash,
            id_register_event.transaction_hash
        );
        assert!(key_exists_in_trie(
            &mut engine,
            &TrieKey::for_onchain_event(&id_register_event)
        ));

        // Verify signer registration
        let stored_signer = stores
            .onchain_event_store
            .get_active_signer(
                new_fid,
                new_signer.verifying_key().to_bytes().to_vec(),
                None,
            )
            .unwrap();

        assert!(
            stored_signer.is_some(),
            "Signer should be active in the store"
        );
        assert!(key_exists_in_trie(
            &mut engine,
            &TrieKey::for_onchain_event(&signer_add_event)
        ));

        // Verify Cast message
        let stored_casts = stores
            .cast_store
            .get_adds_by_fid::<fn(&proto::Message) -> bool>(new_fid, &PageOptions::default(), None)
            .unwrap();

        assert_eq!(
            stored_casts.messages.len(),
            1,
            "CastAdd message should be in the store"
        );
        assert_eq!(stored_casts.messages[0].hash, cast_add.hash);
        assert!(message_exists_in_trie(&mut engine, &cast_add));

        // 4c. Assert event emission
        let block_confirmed_event = recv_next_event(&mut event_rx, false).await;
        assert_eq!(
            block_confirmed_event.r#type,
            HubEventType::BlockConfirmed as i32
        );

        let event1 = recv_next_event(&mut event_rx, false).await;
        let event2 = recv_next_event(&mut event_rx, false).await;
        let event3 = recv_next_event(&mut event_rx, false).await;
        let event4 = recv_next_event(&mut event_rx, false).await;

        let mut seen_storage_event = false;
        let mut seen_id_register = false;
        let mut seen_signer_add = false;
        let mut seen_cast_add = false;

        for event in [event1, event2, event3, event4] {
            match event.body.as_ref().unwrap() {
                proto::hub_event::Body::MergeOnChainEventBody(body) => {
                    let event_body = body.on_chain_event.as_ref().unwrap();
                    if event_body.r#type() == proto::OnChainEventType::EventTypeStorageRent {
                        seen_storage_event = true;
                    } else if event_body.transaction_hash == id_register_event.transaction_hash {
                        seen_id_register = true;
                    } else if event_body.transaction_hash == signer_add_event.transaction_hash {
                        seen_signer_add = true;
                    }
                }
                proto::hub_event::Body::MergeMessageBody(body) => {
                    if body.message.as_ref().unwrap().hash == cast_add.hash {
                        seen_cast_add = true;
                    }
                }
                _ => panic!("Unexpected event type received: {:?}", event.r#type),
            }
        }

        assert!(
            seen_storage_event,
            "MergeOnChainEvent for storage rent was not seen"
        );
        assert!(
            seen_id_register,
            "MergeOnChainEvent for ID registration was not seen"
        );
        assert!(
            seen_signer_add,
            "MergeOnChainEvent for signer add was not seen"
        );
        assert!(seen_cast_add, "MergeMessage for CastAdd was not seen");

        assert!(
            try_recv_next_event(&mut event_rx, false).is_err(),
            "There should be no more events"
        );
    }

    #[tokio::test]
    async fn test_add_remove_in_same_tx_respects_crdt_rules() {
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;

        let ts = time::farcaster_time();
        let cast1 = &messages_factory::casts::create_cast_add(FID_FOR_TEST, "msg1", Some(ts), None);
        let cast2 = &messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &cast1.hash,
            Some(ts + 10),
            None,
        );
        let cast3 = &messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &cast1.hash,
            Some(ts + 20),
            None,
        );
        let mut event_rx = engine.get_senders().events_tx.subscribe();
        let messages = vec![
            MempoolMessage::UserMessage(cast1.clone()),
            MempoolMessage::UserMessage(cast2.clone()),
            MempoolMessage::UserMessage(cast3.clone()),
        ];
        let state_change = engine.propose_state_change(1, messages, None);
        test_helper::validate_and_commit_state_change(&mut engine, &state_change).await;

        // We merged an add, a remove and a second remove which should win over the first (later timestamp)
        // In the end, the add and the intermediate remove should not exist
        assert!(!TrieKey::for_message(&cast1)
            .iter()
            .all(|key| engine.trie_key_exists(&trie_ctx(), &key)));
        assert!(!TrieKey::for_message(&cast2)
            .iter()
            .all(|key| engine.trie_key_exists(&trie_ctx(), &key)));
        assert!(TrieKey::for_message(&cast3)
            .iter()
            .all(|key| engine.trie_key_exists(&trie_ctx(), &key)));

        let messages = &engine
            .get_stores()
            .cast_store
            .get_all_messages_by_fid(FID_FOR_TEST, None, None, &PageOptions::default())
            .unwrap();
        test_helper::assert_contains_all_messages(messages, &[cast3]);

        // We receive merge events for the add and the intermediate remove, even though it would never get committed to the db
        assert_merge_event(
            &try_recv_next_event(&mut event_rx, true).unwrap(),
            &cast1,
            1,
        );
        assert_merge_event(
            &try_recv_next_event(&mut event_rx, false).unwrap(),
            &cast2,
            2,
        );
        assert_merge_event(
            &try_recv_next_event(&mut event_rx, false).unwrap(),
            &cast3,
            3,
        );
    }

    #[tokio::test]
    async fn test_engine_send_onchain_event() {
        let onchain_event = default_onchain_event();
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        let mut event_rx = engine.get_senders().events_tx.subscribe();
        let state_change = engine.propose_state_change(
            1,
            vec![MempoolMessage::OnchainEvent(onchain_event.clone())],
            None,
        );
        assert_eq!(1, state_change.shard_id);
        assert_eq!(state_change.transactions.len(), 1);
        assert_eq!(1, state_change.transactions[0].system_messages.len());

        // No hub events are generated until after commit
        let events = HubEvent::get_events(engine.db.clone(), 0, None, None).unwrap();
        assert_eq!(0, events.events.len());
        assert!(event_rx.try_recv().is_err());

        test_helper::validate_and_commit_state_change(&mut engine, &state_change).await;

        let height = engine.get_confirmed_height();
        assert_eq!(height.shard_index, 1);

        let stored_onchain_events = engine
            .get_onchain_events(OnChainEventType::EventTypeIdRegister, FID_FOR_TEST)
            .unwrap();
        assert_eq!(stored_onchain_events.len(), 1);

        // Hub events are generated
        let mut events = HubEvent::get_events(engine.db.clone(), 0, None, None).unwrap();
        assert_eq!(2, events.events.len());

        // Receive the merge onchain event (skipping block confirmed event)
        let mut received_event = recv_next_event(&mut event_rx, true).await;
        // Timestamp is populated on the received event but it's not stored in the db. Set to 0 for both so that the equality assertion doesn't fail.
        received_event.timestamp = 0;
        events.events.get_mut(1).unwrap().timestamp = 0;
        assert_eq!(received_event, events.events[1]);
        assert!(event_rx.try_recv().is_err()); // only 2 events

        let generated_event = match events.events[1].clone().body {
            Some(proto::hub_event::Body::MergeOnChainEventBody(e)) => e,
            _ => panic!("Unexpected event type"),
        };
        assert_eq!(
            to_hex(&onchain_event.transaction_hash),
            to_hex(&generated_event.on_chain_event.unwrap().transaction_hash)
        );
        assert_event_id(&received_event, Some(1), 1); // sequence 1
    }

    #[tokio::test]
    async fn test_event_ids() {
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;

        let cast1 = default_message("cast1");
        let cast2 = default_message("cast2");
        let state_change = engine.propose_state_change(
            1,
            vec![
                MempoolMessage::UserMessage(cast1.clone()),
                MempoolMessage::UserMessage(cast2.clone()),
            ],
            None,
        );
        test_helper::validate_and_commit_state_change(&mut engine, &state_change).await;

        let cast3 = default_message("cast3");
        let cast4 = default_message("cast4");
        let state_change = engine.propose_state_change(
            1,
            vec![
                MempoolMessage::UserMessage(cast3.clone()),
                MempoolMessage::UserMessage(cast4.clone()),
            ],
            None,
        );
        test_helper::validate_and_commit_state_change(&mut engine, &state_change).await;

        // Ignore first 3 blocks which are user registration events
        let events = HubEvent::get_events(
            engine.db.clone(),
            HubEventIdGenerator::make_event_id_for_block_number(4),
            None,
            None,
        )
        .unwrap();
        assert_eq!(6, events.events.len());

        // Find the merge events (skip BlockConfirmed events)
        let merge_events: Vec<&HubEvent> = events
            .events
            .iter()
            .filter(|e| e.r#type == proto::HubEventType::MergeMessage as i32)
            .collect();
        assert_eq!(merge_events.len(), 4);

        // First two events are in block 1, second two are in block 2. sequence resets for each block
        assert_merge_event(merge_events[0], &cast1, 1);
        assert_event_id(merge_events[0], Some(4), 1);
        assert_merge_event(merge_events[1], &cast2, 2);
        assert_event_id(merge_events[1], Some(4), 2);

        assert_merge_event(merge_events[2], &cast3, 1); // cast3 is in block 5
        assert_event_id(merge_events[2], Some(5), 1);

        assert_merge_event(merge_events[3], &cast4, 2);
        assert_event_id(merge_events[3], Some(5), 2);
    }

    #[tokio::test]
    async fn test_messages_not_merged_with_no_storage() {
        let (mut engine, _tmpdir) = test_helper::new_engine().await;

        let cast_add =
            messages_factory::casts::create_cast_add(FID_FOR_TEST + 1, "no storage", None, None);

        assert_eq!("", to_hex(&engine.trie_root_hash()));
        let state_change = engine.propose_state_change(
            1,
            vec![MempoolMessage::UserMessage(cast_add.clone())],
            None,
        );

        assert_eq!(0, state_change.transactions.len());
        assert_eq!("", to_hex(&state_change.new_state_root));
    }

    #[tokio::test]
    async fn test_messages_with_invalid_network_are_not_merged() {
        let (mut engine, _tmpdir) = test_helper::new_engine().await;

        let signer = test_helper::default_signer();
        register_user(
            FID_FOR_TEST,
            signer.clone(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;
        let mut cast_add =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "invalid network", None, None);
        cast_add.data.as_mut().unwrap().network = 0;
        cast_add.hash = calculate_message_hash(&cast_add.data.as_ref().unwrap().encode_to_vec());
        cast_add.signature = signer.sign(&cast_add.hash).to_bytes().to_vec();

        assert_commit_fails(
            &mut engine,
            &cast_add,
            "bad_request.validation_failure",
            "invalid network",
        )
        .await;
    }

    #[tokio::test]
    async fn test_messages_pruned_with_exceeded_storage() {
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        test_helper::register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;
        let mut event_rx = engine.get_senders().events_tx.subscribe();
        let current_time = factory::time::farcaster_time();
        let cast1 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "msg1",
            Some(current_time),
            None,
        );
        let cast2 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "msg2",
            Some(current_time + 1),
            None,
        );
        let cast3 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "msg3",
            Some(current_time + 2),
            None,
        );
        let cast4 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "msg4",
            Some(current_time + 3),
            None,
        );
        let cast5 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "msg5",
            Some(current_time + 4),
            None,
        );

        // Default size in tests is 4 casts, so first four messages should merge without issues
        commit_message(&mut engine, &cast1).await;
        assert_merge_event(
            &try_recv_next_event(&mut event_rx, true).unwrap(),
            &cast1,
            1,
        );
        commit_message(&mut engine, &cast2).await;
        assert_merge_event(
            &try_recv_next_event(&mut event_rx, true).unwrap(),
            &cast2,
            1,
        );
        commit_message(&mut engine, &cast3).await;
        assert_merge_event(
            &try_recv_next_event(&mut event_rx, true).unwrap(),
            &cast3,
            1,
        );
        commit_message(&mut engine, &cast4).await;
        assert_merge_event(
            &try_recv_next_event(&mut event_rx, true).unwrap(),
            &cast4,
            1,
        );

        // Fifth message should be merged, but should cause cast1 to be pruned
        commit_message(&mut engine, &cast5).await;
        assert_merge_event(
            &try_recv_next_event(&mut event_rx, true).unwrap(),
            &cast5,
            1,
        );
        assert_prune_event(
            &try_recv_next_event(&mut event_rx, false).unwrap(),
            &cast1,
            2,
        );

        // Prunes are reflected in the trie
        assert_eq!(message_exists_in_trie(&mut engine, &cast1), false);
        assert_eq!(message_exists_in_trie(&mut engine, &cast2), true);
        assert_eq!(message_exists_in_trie(&mut engine, &cast3), true);
        assert_eq!(message_exists_in_trie(&mut engine, &cast4), true);
        assert_eq!(message_exists_in_trie(&mut engine, &cast5), true);
    }

    #[tokio::test]
    async fn test_messages_partially_merged_with_insufficient_storage() {
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        let signer = test_helper::default_signer();
        test_helper::register_user(
            FID_FOR_TEST,
            signer.clone(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;
        let mut event_rx = engine.get_senders().events_tx.subscribe();
        let current_time = factory::time::farcaster_time();
        let cast1 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "msg1",
            Some(current_time),
            None,
        );
        let cast2 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "msg2",
            Some(current_time + 1),
            Some(&signer),
        );
        let cast3 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "msg3",
            Some(current_time + 2),
            Some(&signer),
        );
        let cast4 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "msg4",
            Some(current_time + 3),
            Some(&signer),
        );
        let cast5 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "msg5",
            Some(current_time + 4),
            Some(&signer),
        );
        let cast6 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "msg6",
            Some(current_time + 5),
            Some(&signer),
        );

        // Send first three messages in one block, which should mean there is 1 message left in storage
        let messages = vec![
            MempoolMessage::UserMessage(cast1.clone()),
            MempoolMessage::UserMessage(cast2.clone()),
            MempoolMessage::UserMessage(cast3.clone()),
        ];
        let state_change = engine.propose_state_change(1, messages, None);
        test_helper::validate_and_commit_state_change(&mut engine, &state_change).await;
        let _block_confirmed_event1 = &event_rx.try_recv().unwrap();
        assert_merge_event(&event_rx.try_recv().unwrap(), &cast1, 1);
        assert_merge_event(&event_rx.try_recv().unwrap(), &cast2, 2);
        assert_merge_event(&event_rx.try_recv().unwrap(), &cast3, 3);

        // Now send the last three messages, all of them should be merged, and the first two should be pruned
        let messages = vec![
            MempoolMessage::UserMessage(cast4.clone()),
            MempoolMessage::UserMessage(cast5.clone()),
            MempoolMessage::UserMessage(cast6.clone()),
        ];
        let state_change = engine.propose_state_change(1, messages, None);
        let _chunk =
            test_helper::validate_and_commit_state_change(&mut engine, &state_change).await;
        // Receive the merge and prune events (skipping block confirmed event)
        assert_merge_event(
            &try_recv_next_event(&mut event_rx, true).unwrap(),
            &cast4,
            1,
        );
        assert_merge_event(
            &try_recv_next_event(&mut event_rx, false).unwrap(),
            &cast5,
            2,
        );
        assert_merge_event(
            &try_recv_next_event(&mut event_rx, false).unwrap(),
            &cast6,
            3,
        );
        assert_prune_event(
            &try_recv_next_event(&mut event_rx, false).unwrap(),
            &cast1,
            4,
        );
        assert_prune_event(
            &try_recv_next_event(&mut event_rx, false).unwrap(),
            &cast2,
            5,
        );

        let user_messages = _chunk.transactions[0]
            .user_messages
            .iter()
            .map(|m| to_hex(&m.hash))
            .collect::<Vec<String>>();
        assert_eq!(
            user_messages,
            vec![
                to_hex(&cast4.hash),
                to_hex(&cast5.hash),
                to_hex(&cast6.hash)
            ]
        );

        // Prunes are reflected in the trie
        assert_eq!(message_exists_in_trie(&mut engine, &cast2), false);
        assert_eq!(message_exists_in_trie(&mut engine, &cast3), true);
        assert_eq!(message_exists_in_trie(&mut engine, &cast4), true);
        assert_eq!(message_exists_in_trie(&mut engine, &cast5), true);
        assert_eq!(message_exists_in_trie(&mut engine, &cast6), true);
    }

    #[tokio::test]
    async fn test_revoking_a_signer_does_not_delete_all_messages_from_that_signer() {
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        let signer = generate_signer();
        let another_signer = generate_signer();
        let timestamp = factory::time::farcaster_time();
        let msg1 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "msg1",
            Some(timestamp),
            Some(&signer),
        );
        let msg2 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "msg2",
            Some(timestamp + 1),
            Some(&signer),
        );
        let same_fid_different_signer = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "msg3",
            None,
            Some(&another_signer),
        );
        let different_fid_same_signer =
            messages_factory::casts::create_cast_add(FID_FOR_TEST + 1, "msg4", None, Some(&signer));
        test_helper::register_user(
            FID_FOR_TEST,
            signer.clone(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;
        let another_signer_event = events_factory::create_signer_event(
            FID_FOR_TEST,
            another_signer.clone(),
            proto::SignerEventType::Add,
            None,
            None,
        );
        test_helper::commit_event(&mut engine, &another_signer_event).await;
        test_helper::register_user(
            FID_FOR_TEST + 1,
            signer.clone(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;
        let mut event_rx = engine.get_senders().events_tx.subscribe();

        commit_message(&mut engine, &msg1).await;
        let _ = &event_rx.try_recv().unwrap(); // Ignore BLOCK_CONFIRMED event
        let _ = &event_rx.try_recv().unwrap(); // Ignore merge event
        commit_message(&mut engine, &msg2).await;
        let _ = &event_rx.try_recv().unwrap(); // Ignore BLOCK_CONFIRMED event
        let _ = &event_rx.try_recv().unwrap(); // Ignore merge event
        commit_message(&mut engine, &same_fid_different_signer).await;
        let _ = &event_rx.try_recv().unwrap(); // Ignore BLOCK_CONFIRMED event
        let _ = &event_rx.try_recv().unwrap(); // Ignore merge event
        commit_message(&mut engine, &different_fid_same_signer).await;
        let _ = &event_rx.try_recv().unwrap(); // Ignore BLOCK_CONFIRMED event
        let _ = &event_rx.try_recv().unwrap(); // Ignore merge event

        // All 4 messages exist
        let messages = engine.get_casts_by_fid(FID_FOR_TEST).unwrap();
        assert_eq!(3, messages.messages.len());
        let messages = engine.get_casts_by_fid(FID_FOR_TEST + 1).unwrap();
        assert_eq!(1, messages.messages.len());

        // Revoke a single signer
        let revoke_timestamp = (from_farcaster_time((timestamp + 3) as u64) / 1000) as u32;
        let revoke_event = events_factory::create_signer_event(
            FID_FOR_TEST,
            signer.clone(),
            proto::SignerEventType::Remove,
            Some(revoke_timestamp),
            None,
        );
        test_helper::commit_event(&mut engine, &revoke_event).await;
        // First receive BLOCK_CONFIRMED event
        let _ = &event_rx.try_recv().unwrap();
        // Then receive the onchain event
        assert_onchain_hub_event(&event_rx.try_recv().unwrap(), &revoke_event, 1);
        assert_eq!(
            event_rx.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        );

        assert_eq!(event_rx.try_recv().is_err(), true); // No more events
        let messages = engine.get_casts_by_fid(FID_FOR_TEST).unwrap();
        assert_eq!(3, messages.messages.len());

        // Different Fid with the same signer is unaffected
        let messages = engine.get_casts_by_fid(FID_FOR_TEST + 1).unwrap();
        assert_eq!(1, messages.messages.len());

        // Submitting a message from the revoked signer should fail
        let post_revoke_message = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "after revoke",
            Some(timestamp + 5),
            Some(&signer),
        );
        assert_commit_fails(
            &mut engine,
            &post_revoke_message,
            "bad_request.validation_failure",
            "invalid signer",
        )
        .await;
    }

    #[tokio::test]
    async fn test_merge_fname() {
        let (mut engine, _tmpdir) = test_helper::new_engine().await;

        test_helper::register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            hex::decode("711aa8ec273dae42e51732fe1be2b15ee53b00a4").unwrap(),
            &mut engine,
        )
        .await;

        let fname = &"acp".to_string();

        let mut event_rx = engine.get_senders().events_tx.subscribe();
        let fname_transfer = &FnameTransfer{
          id: 1234,
          from_fid: 0,
          proof: Some(UserNameProof{
            timestamp: 1660233642,
            name: fname.as_bytes().to_vec(),
            owner: hex::decode("711aa8ec273dae42e51732fe1be2b15ee53b00a4").unwrap(),
            signature: hex::decode("ebd1b040a4961c5ea751e8ec867d4af6fdbf80ade6775d33dad94ab1c0423dc64a2f684d0e48b89f2958a2385b91743647161ade04e6628a166b5bd1579d86ff1b").unwrap(),
            fid: 1234,
            r#type: 1,
          }),
        };

        let state_change = engine.propose_state_change(
            1,
            vec![MempoolMessage::FnameTransfer(fname_transfer.clone())],
            None,
        );
        test_helper::validate_and_commit_state_change(&mut engine, &state_change).await;

        // Emits a hub event for the user name proof
        let transfer_event = &try_recv_next_event(&mut event_rx, true).unwrap();
        assert_eq!(
            transfer_event.r#type,
            proto::HubEventType::MergeUsernameProof as i32
        );
        assert_eq!(event_rx.try_recv().is_err(), true); // No more events

        // fname exists in the trie and in the db
        assert!(test_helper::key_exists_in_trie(
            &mut engine,
            &TrieKey::for_fname(FID_FOR_TEST, fname)
        ));
        let proof = engine.get_fname_proof(fname).unwrap();
        assert!(proof.is_some());
        assert_eq!(proof.unwrap().fid, FID_FOR_TEST);
    }

    #[tokio::test]
    async fn test_merge_fname_with_signing() {
        let signer = alloy_signer_local::PrivateKeySigner::random();
        let (mut engine, _tmpdir) = test_helper::new_engine_with_options(EngineOptions {
            fname_signer_address: Some(signer.address()),
            ..EngineOptions::default()
        })
        .await;

        test_helper::register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;

        let fname = &"acp".to_string();

        let timestamp = factory::time::farcaster_time();

        let mut event_rx = engine.get_senders().events_tx.subscribe();
        let fname_transfer = username_factory::create_transfer(
            FID_FOR_TEST,
            fname,
            Some(timestamp),
            None,
            Some(test_helper::default_custody_address()),
            signer.clone(),
        );

        let state_change = engine.propose_state_change(
            1,
            vec![MempoolMessage::FnameTransfer(fname_transfer.clone())],
            None,
        );
        test_helper::validate_and_commit_state_change(&mut engine, &state_change).await;

        // Emits a hub event for the user name proof
        // Receive the actual username proof event (skipping BLOCK_CONFIRMED event)
        let transfer_event = &try_recv_next_event(&mut event_rx, true).unwrap();
        assert_eq!(
            transfer_event.r#type,
            proto::HubEventType::MergeUsernameProof as i32
        );
        assert_eq!(event_rx.try_recv().is_err(), true); // No more events

        // fname exists in the trie and in the db
        assert!(test_helper::key_exists_in_trie(
            &mut engine,
            &TrieKey::for_fname(FID_FOR_TEST, fname)
        ));
        let proof = engine.get_fname_proof(fname).unwrap();
        assert!(proof.is_some());
        assert_eq!(proof.unwrap().fid, FID_FOR_TEST);

        let fname_transfer = username_factory::create_transfer(
            0,
            fname,
            Some(timestamp + 1),
            Some(FID_FOR_TEST),
            Some(test_helper::default_custody_address()),
            signer,
        );

        let state_change = engine.propose_state_change(
            1,
            vec![MempoolMessage::FnameTransfer(fname_transfer.clone())],
            None,
        );
        test_helper::validate_and_commit_state_change(&mut engine, &state_change).await;

        // First receive BLOCK_CONFIRMED event
        let _block_confirmed_event = &event_rx.try_recv().unwrap();
        // Then receive the actual username proof event
        let transfer_event = &event_rx.try_recv().unwrap();
        assert_eq!(
            transfer_event.r#type,
            proto::HubEventType::MergeUsernameProof as i32
        );

        // don't insert an fname for fid 0 into the trie
        assert!(!test_helper::key_exists_in_trie(
            &mut engine,
            &TrieKey::for_fname(0, fname)
        ));
    }

    #[tokio::test]
    async fn test_fname_transfer() {
        let fname_signer = alloy_signer_local::PrivateKeySigner::random();
        let fname_signer_address = fname_signer.address();
        let (mut engine1, _) = test_helper::new_engine_with_options(EngineOptions {
            shard_id: 1,
            fname_signer_address: Some(fname_signer_address.clone()),
            ..Default::default()
        })
        .await;
        let (mut engine2, _) = test_helper::new_engine_with_options(EngineOptions {
            shard_id: 2,
            fname_signer_address: Some(fname_signer_address.clone()),
            ..Default::default()
        })
        .await;

        let fid1 = FID_FOR_TEST;
        let signer = generate_signer();
        let fid2 = FID2_FOR_TEST;
        let timestamp = factory::time::farcaster_time();
        register_user(
            fid1,
            signer.clone(),
            default_custody_address(),
            &mut engine1,
        )
        .await;
        register_user(
            fid2,
            signer.clone(),
            default_custody_address(),
            &mut engine2,
        )
        .await;

        let fname = "username".to_string();
        let fname_register = username_factory::create_transfer(
            fid1,
            &fname,
            Some(timestamp),
            Some(0),
            Some(default_custody_address()),
            fname_signer.clone(),
        );
        test_helper::commit_fname_transfer(&mut engine1, &fname_register).await;
        assert!(key_exists_in_trie(
            &mut engine1,
            &TrieKey::for_fname(fid1, &fname)
        ));

        let fid1_username_msg = messages_factory::user_data::create_user_data_add(
            fid1,
            proto::UserDataType::Username,
            &fname,
            Some(timestamp + 1),
            Some(&signer),
        );
        commit_message(&mut engine1, &fid1_username_msg).await;
        assert!(TrieKey::for_message(&fid1_username_msg)
            .iter()
            .all(|key| engine1.trie_key_exists(&trie_ctx(), &key)));

        let is_username_present = |engine: &ShardEngine, fid: u64| {
            let result =
                UserDataStore::get_username_proof_by_fid(&engine.get_stores().user_data_store, fid);
            assert!(result.is_ok());
            result.unwrap().is_some()
        };

        assert_eq!(is_username_present(&engine1, fid1), true);
        assert_eq!(is_username_present(&engine1, fid2), false);

        // Now transfer the fname to fid2, on a different shard
        let fname_transfer = username_factory::create_transfer(
            fid2,
            &fname,
            Some(timestamp + 2),
            Some(fid1),
            Some(default_custody_address()),
            fname_signer.clone(),
        );
        // Send transfer to both shards
        test_helper::commit_fname_transfer(&mut engine1, &fname_transfer).await;
        test_helper::commit_fname_transfer(&mut engine2, &fname_transfer).await;

        // The fname should not exist in the trie for the original fid, and must exist for the new fid
        assert_eq!(
            key_exists_in_trie(&mut engine1, &TrieKey::for_fname(fid1, &fname)),
            false
        );
        assert_eq!(
            key_exists_in_trie(&mut engine2, &TrieKey::for_fname(fid2, &fname)),
            true
        );
        // Username has been revoked
        assert_eq!(
            message_exists_in_trie(&mut engine1, &fid1_username_msg),
            false
        );

        // TODO: Engine 1 is still tracking the fname for fid2. It should not, but at the engine level we
        // don't have a way to fix this yet. Since engines don't know about other shards. We work around
        // this by sending the transfer to all shards in the mempool. In this particular way, this
        // test is not reflective of what happens in prod, but leaving the assert as a reminder of current behavior.
        assert_eq!(
            key_exists_in_trie(&mut engine1, &TrieKey::for_fname(fid2, &fname)),
            true
        );
        assert_eq!(
            key_exists_in_trie(&mut engine2, &TrieKey::for_fname(fid1, &fname)),
            false
        );

        // Username proof only should only exist on engine2 for fid2
        // It's currently also present on engine1, but this is a bug that will be fixed in the future
        assert_eq!(is_username_present(&engine1, fid1), false);
        assert_eq!(is_username_present(&engine1, fid2), true);
        assert_eq!(is_username_present(&engine2, fid1), false);
        assert_eq!(is_username_present(&engine2, fid2), true);

        // deregister the fname
        let fname_transfer = username_factory::create_transfer(
            0,
            &fname,
            Some(timestamp + 3),
            Some(fid2),
            Some(default_custody_address()),
            fname_signer.clone(),
        );
        // Mirror existing behavior in the mempool where fname transfers are sent to all shards
        test_helper::commit_fname_transfer(&mut engine1, &fname_transfer).await;
        test_helper::commit_fname_transfer(&mut engine2, &fname_transfer).await;
        assert_eq!(
            key_exists_in_trie(&mut engine2, &TrieKey::for_fname(fid2, &fname)),
            false
        );

        // After deregistering, the fname should not exist in either engine
        assert_eq!(is_username_present(&engine1, fid1), false);
        assert_eq!(is_username_present(&engine1, fid2), false);
        assert_eq!(is_username_present(&engine2, fid1), false);
        assert_eq!(is_username_present(&engine2, fid2), false);
    }

    #[tokio::test]
    async fn test_merge_ens_username() {
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        let ens_name = &"farcaster.eth".to_string();
        let owner = test_helper::default_custody_address();
        let signature = "signature".to_string();
        let signer = test_helper::default_signer();
        let timestamp = messages_factory::farcaster_time();

        test_helper::register_user(FID_FOR_TEST, signer.clone(), owner.clone(), &mut engine).await;

        let username_proof_add = messages_factory::username_proof::create_username_proof(
            FID_FOR_TEST as u64,
            proto::UserNameType::UsernameTypeEnsL1,
            ens_name.clone(),
            owner,
            signature.clone(),
            timestamp as u64,
            Some(&signer),
        );

        commit_message(&mut engine, &username_proof_add).await;
        let committed_username_proof = engine.get_username_proofs_by_fid(FID_FOR_TEST).unwrap();
        assert_eq!(committed_username_proof.messages.len(), 1);

        let username_add = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST as u64,
            proto::UserDataType::Username,
            ens_name,
            Some(timestamp + 1),
            Some(&signer),
        );

        // We had a bug where this commit would fail because we looked in the wrong store to find the username proof
        commit_message(&mut engine, &username_add).await;
    }

    #[tokio::test]
    async fn test_username_revoked_when_proof_transferred() {
        let signer = alloy_signer_local::PrivateKeySigner::random();
        let (mut engine, _tmpdir) = test_helper::new_engine_with_options(EngineOptions {
            fname_signer_address: Some(signer.address()),
            ..EngineOptions::default()
        })
        .await;

        test_helper::register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;

        let fname = &"farcaster".to_string();
        test_helper::register_fname(
            FID_FOR_TEST,
            fname,
            None,
            Some(test_helper::default_custody_address()),
            &mut engine,
            FarcasterNetwork::Mainnet,
            signer.clone(),
        )
        .await;

        let fid_username_msg = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST,
            proto::UserDataType::Username,
            fname,
            None,
            None,
        );
        commit_message(&mut engine, &fid_username_msg).await;

        assert!(test_helper::key_exists_in_trie(
            &mut engine,
            &TrieKey::for_fname(FID_FOR_TEST, fname)
        ));
        assert!(message_exists_in_trie(&mut engine, &fid_username_msg),);

        let original_fid_user_data = engine.get_user_data_by_fid(FID_FOR_TEST).unwrap();
        assert_eq!(original_fid_user_data.messages.len(), 1);

        // Now transfer the fname, and the username userdata add should be revoked
        let transfer = username_factory::create_transfer(
            FID2_FOR_TEST,
            fname,
            Some(time::current_timestamp() + 10),
            Some(FID_FOR_TEST),
            Some(test_helper::default_custody_address()),
            signer,
        );
        test_helper::commit_fname_transfer(&mut engine, &transfer).await;

        // Fname has moved to the new fid and the username userdata is revoked
        assert!(test_helper::key_exists_in_trie(
            &mut engine,
            &TrieKey::for_fname(FID2_FOR_TEST, fname)
        ));

        assert!(!test_helper::key_exists_in_trie(
            &mut engine,
            &TrieKey::for_fname(FID_FOR_TEST, fname)
        ));
        assert!(!TrieKey::for_message(&fid_username_msg)
            .iter()
            .all(|key| engine.trie_key_exists(&trie_ctx(), &key)));

        let original_fid_user_data = engine.get_user_data_by_fid(FID_FOR_TEST).unwrap();
        assert_eq!(original_fid_user_data.messages.len(), 0);
    }

    #[tokio::test]
    async fn test_missing_id_registration() {
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        test_helper::commit_event(
            &mut engine,
            &test_helper::default_storage_event(FID_FOR_TEST),
        )
        .await;
        test_helper::commit_event(
            &mut engine,
            &events_factory::create_signer_event(
                FID_FOR_TEST,
                test_helper::default_signer(),
                proto::SignerEventType::Add,
                None,
                None,
            ),
        )
        .await;
        assert_commit_fails(
            &mut engine,
            &default_message("msg1"),
            "bad_request.validation_failure",
            "unknown fid",
        )
        .await;
        let messages = engine.get_casts_by_fid(FID_FOR_TEST).unwrap();
        assert_eq!(0, messages.messages.len());
        let id_register = events_factory::create_id_register_event(
            FID_FOR_TEST,
            proto::IdRegisterEventType::Register,
            vec![],
            None,
        );
        test_helper::commit_event(&mut engine, &id_register).await;
        commit_message(&mut engine, &default_message("msg1")).await;
        let messages = engine.get_casts_by_fid(FID_FOR_TEST).unwrap();
        assert_eq!(1, messages.messages.len());
    }

    #[tokio::test]
    async fn test_missing_signer() {
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        test_helper::commit_event(
            &mut engine,
            &test_helper::default_storage_event(FID_FOR_TEST),
        )
        .await;
        test_helper::commit_event(
            &mut engine,
            &events_factory::create_id_register_event(
                FID_FOR_TEST,
                proto::IdRegisterEventType::Register,
                vec![],
                None,
            ),
        )
        .await;
        assert_commit_fails(
            &mut engine,
            &default_message("msg1"),
            "bad_request.validation_failure",
            "invalid signer",
        )
        .await;
        let messages = engine.get_casts_by_fid(FID_FOR_TEST).unwrap();
        assert_eq!(0, messages.messages.len());
        test_helper::commit_event(
            &mut engine,
            &events_factory::create_signer_event(
                FID_FOR_TEST,
                test_helper::default_signer(),
                proto::SignerEventType::Add,
                None,
                None,
            ),
        )
        .await;
        commit_message(&mut engine, &default_message("msg1")).await;
        let messages = engine.get_casts_by_fid(FID_FOR_TEST).unwrap();
        assert_eq!(1, messages.messages.len());
    }

    #[tokio::test]
    async fn test_merge_failure_event() {
        let single_message_limit = StoreLimits::new(limits::one(), limits::zero(), limits::zero());
        let (mut engine, _tmpdir) = test_helper::new_engine_with_options(EngineOptions {
            limits: Some(single_message_limit),
            ..Default::default()
        })
        .await;
        register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;

        let timestamp = time::farcaster_time();
        let hash = messages_factory::generate_random_message_hash();
        let remove_message = messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &hash,
            Some(timestamp),
            Some(&test_helper::default_signer()),
        );
        commit_message(&mut engine, &remove_message).await;

        let current_height = engine.get_confirmed_height().increment();
        engine.start_round(current_height, Round::Nil);

        // We can't use assert_commit_fails here, because it checks against existence in the trie, and duplicate will exist already
        let state_change = engine.propose_state_change(
            1,
            vec![MempoolMessage::UserMessage(remove_message.clone())],
            None,
        );
        assert_eq!(state_change.events.len(), 1);
        assert_failure_event(
            state_change.events[0].clone(),
            &remove_message,
            "bad_request.duplicate",
            "message has already been merged",
        );

        // We had a bug where all merge failure events were missing event ids
        assert_eq!(
            state_change.events[0].id,
            HubEventIdGenerator::make_event_id_for_block_number(current_height.block_number) + 1 // 0 is reserved for block confirmed
        );

        let conflicting_message = messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &hash,
            Some(timestamp - 1),
            Some(&test_helper::default_signer()),
        );

        assert_commit_fails(
            &mut engine,
            &conflicting_message,
            "bad_request.conflict",
            "message conflicts with a more recent remove",
        )
        .await;
    }

    #[tokio::test]
    async fn test_fname_validation() {
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        let fname = &"acp".to_string();
        test_helper::register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            hex::decode("711aa8ec273dae42e51732fe1be2b15ee53b00a4").unwrap(),
            &mut engine,
        )
        .await;
        test_helper::register_user(
            FID2_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;

        // When fname is not registered, message is not merged
        {
            let msg = messages_factory::user_data::create_user_data_add(
                FID_FOR_TEST,
                proto::UserDataType::Username,
                fname,
                None,
                None,
            );
            assert_commit_fails(
                &mut engine,
                &msg,
                "bad_request.validation_failure",
                "fname is not registered for fid",
            )
            .await;
        }

        let fname = &"acp".to_string();

        let fname_transfer = FnameTransfer{
          id: 1234,
          from_fid: 0,
          proof: Some(UserNameProof{
            timestamp: 1660233642,
            name: fname.as_bytes().to_vec(),
            owner: hex::decode("711aa8ec273dae42e51732fe1be2b15ee53b00a4").unwrap(),
            signature: hex::decode("ebd1b040a4961c5ea751e8ec867d4af6fdbf80ade6775d33dad94ab1c0423dc64a2f684d0e48b89f2958a2385b91743647161ade04e6628a166b5bd1579d86ff1b").unwrap(),
            fid: 1234,
            r#type: 1,
          }),
        };
        let state_change = engine.propose_state_change(
            1,
            vec![MempoolMessage::FnameTransfer(fname_transfer)],
            None,
        );

        test_helper::validate_and_commit_state_change(&mut engine, &state_change).await;

        // When fname is owned by a different fid, message is not merged
        {
            let msg = messages_factory::user_data::create_user_data_add(
                FID2_FOR_TEST,
                proto::UserDataType::Username,
                fname,
                None,
                None,
            );
            assert_commit_fails(
                &mut engine,
                &msg,
                "bad_request.validation_failure",
                "fname is not registered for fid",
            )
            .await;
        }

        // When fname is registered and owned by the same fid, message is merged
        {
            let msg = messages_factory::user_data::create_user_data_add(
                FID_FOR_TEST,
                proto::UserDataType::Username,
                fname,
                None,
                None,
            );
            commit_message(&mut engine, &msg).await;
        }
        let message =
            engine.get_user_data_by_fid_and_type(FID_FOR_TEST, proto::UserDataType::Username);
        assert_eq!(message.is_ok(), true);

        // Allows resetting username to blank
        {
            let msg = messages_factory::user_data::create_user_data_add(
                FID_FOR_TEST,
                proto::UserDataType::Username,
                &"".to_string(),
                Some(time::farcaster_time() + 10),
                None,
            );
            commit_message(&mut engine, &msg).await;
        }

        let message =
            engine.get_user_data_by_fid_and_type(FID_FOR_TEST, proto::UserDataType::Username);
        assert_eq!(message.is_ok(), true);
    }

    #[tokio::test]
    async fn test_simulate_message() {
        let (mut engine, _tmpdir) = test_helper::new_engine().await;

        let message = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "msg1",
            Some(get_farcaster_time().unwrap() as u32),
            Some(&test_helper::default_signer()),
        );

        let result = engine.simulate_message(&message);
        assert_eq!(result.is_ok(), false);
        assert_eq!(result.unwrap_err().to_string(), "unknown fid");

        register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;

        let result = engine.simulate_message(&message);
        assert_eq!(result.is_ok(), true);

        commit_message(&mut engine, &message).await;
        let remove_message = messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &message.hash,
            Some(message.data.unwrap().timestamp + 10),
            Some(&test_helper::default_signer()),
        );

        commit_message(&mut engine, &remove_message).await;

        // duplicates are returned as errors
        let result = engine.simulate_message(&remove_message);
        assert_eq!(result.is_err(), true);
        assert_eq!(
            result
                .unwrap_err()
                .to_string()
                .starts_with("bad_request.duplicate"),
            true
        );

        // conflicts are returned as errors
        let remove_message2 = messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &message.hash,
            Some(remove_message.data.unwrap().timestamp - 1),
            Some(&test_helper::default_signer()),
        );
        let result = engine.simulate_message(&remove_message2);
        assert_eq!(result.is_err(), true);
        assert_eq!(
            result
                .unwrap_err()
                .to_string()
                .starts_with("bad_request.conflict"),
            true
        );
    }

    #[tokio::test]
    async fn test_revoke_signer_bug() {
        let (mut engine, _tmpdir) = test_helper::new_engine_with_options(EngineOptions {
            network: Some(FarcasterNetwork::Mainnet),
            ..Default::default()
        })
        .await;
        register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;

        let bad_signer = generate_signer();
        // Register a signer
        let signer_event = events_factory::create_signer_event(
            FID_FOR_TEST,
            bad_signer.clone(),
            proto::SignerEventType::Add,
            None,
            None,
        );
        test_helper::commit_event(&mut engine, &signer_event).await;

        let timestamp = FarcasterTime::from_unix_seconds(1747333801); // 1s after EngineVersion::V2 is activated
        let version = engine.version_for(&timestamp);
        assert_eq!(version, EngineVersion::V1);
        assert_eq!(version.is_enabled(ProtocolFeature::SignerRevokeBug), true);

        let bad_signer_cast = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "msg1",
            Some(timestamp.to_u64() as u32),
            Some(&bad_signer),
        );
        let good_signer_cast = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "msg2",
            Some(timestamp.to_u64() as u32 + 1),
            Some(&test_helper::default_signer()),
        );

        commit_messages(
            &mut engine,
            vec![bad_signer_cast.clone(), good_signer_cast.clone()],
        )
        .await;

        // Revoke the signer
        let revoke_event = events_factory::create_signer_event(
            FID_FOR_TEST,
            bad_signer.clone(),
            proto::SignerEventType::Remove,
            Some(timestamp.to_unix_seconds() as u32),
            None,
        );
        test_helper::commit_event_at(&mut engine, &revoke_event, &timestamp).await;

        // Both casts should still exist
        assert_eq!(message_exists_in_trie(&mut engine, &bad_signer_cast), true); // Not revoked due to bug
        assert_eq!(message_exists_in_trie(&mut engine, &good_signer_cast), true);

        // Now, revoke the good signer using the current timestamp
        let current_time = FarcasterTime::current();
        let good_signer_revoke_event = events_factory::create_signer_event(
            FID_FOR_TEST,
            test_helper::default_signer(),
            proto::SignerEventType::Remove,
            Some(current_time.to_unix_seconds() as u32),
            None,
        );

        test_helper::commit_event_at(&mut engine, &good_signer_revoke_event, &current_time).await;

        // As of the latest changes, both will still exist. We will only reject new messages with the signer.
        assert_eq!(message_exists_in_trie(&mut engine, &bad_signer_cast), true); // Still exists due to bug
        assert_eq!(message_exists_in_trie(&mut engine, &good_signer_cast), true);
    }

    #[tokio::test]
    async fn pro_tier_purchase_is_recorded_only_after_feature_is_active() {
        let purchase_pro_at_time =
            async |mut engine: &mut ShardEngine, time_unix_seconds: u64, success: bool| {
                let time = &FarcasterTime::from_unix_seconds(time_unix_seconds);
                assert_eq!(
                    engine
                        .version_for(time)
                        .is_enabled(ProtocolFeature::FarcasterPro),
                    success
                );
                let pro_event = events_factory::create_pro_user_event(
                    FID_FOR_TEST,
                    1,
                    Some(time.to_unix_seconds() as u32),
                );
                commit_event_at(&mut engine, &pro_event, time).await;
                assert_eq!(
                    key_exists_in_trie(&mut engine, &TrieKey::for_onchain_event(&pro_event)),
                    success
                );
                assert_eq!(
                    engine.get_stores().is_pro_user(FID_FOR_TEST, time).unwrap(),
                    success
                );
            };
        let (mut engine, _tmpdir) = test_helper::new_engine_with_options(EngineOptions {
            network: Some(FarcasterNetwork::Testnet), // To test pro support
            ..Default::default()
        })
        .await;
        // Before active
        purchase_pro_at_time(&mut engine, 1748950000, false).await;

        // After active
        purchase_pro_at_time(&mut engine, 1748970001, true).await
    }

    #[tokio::test]
    async fn pro_users_get_10k_casts() {
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;
        let pro_event = events_factory::create_pro_user_event(
            FID_FOR_TEST,
            1,
            Some(time::current_timestamp_with_offset(-1)),
        );
        let long_cast = messages_factory::casts::create_cast_add_rich(
            FID_FOR_TEST,
            &"a".repeat(9999),
            Some(proto::CastType::TenKCast),
            vec![],
            None,
            vec![],
            None,
            None,
        );

        commit_message_at(&mut engine, &long_cast, &FarcasterTime::current()).await;
        assert!(!TrieKey::for_message(&long_cast)
            .iter()
            .all(|key| engine.trie_key_exists(&trie_ctx(), &key)));

        commit_event(&mut engine, &pro_event).await;
        assert!(engine.trie_key_exists(
            test_helper::trie_ctx(),
            &TrieKey::for_onchain_event(&pro_event)
        ));

        commit_message_at(&mut engine, &long_cast, &FarcasterTime::current()).await;
        assert!(TrieKey::for_message(&long_cast)
            .iter()
            .all(|key| engine.trie_key_exists(&trie_ctx(), &key)));
    }

    #[tokio::test]
    async fn pro_users_get_four_embeds() {
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;
        let pro_event = events_factory::create_pro_user_event(
            FID_FOR_TEST,
            1,
            Some(time::current_timestamp_with_offset(-1)),
        );
        let four_embeds = messages_factory::casts::create_cast_add_rich(
            FID_FOR_TEST,
            "test",
            Some(proto::CastType::Cast),
            vec![
                Embed {
                    embed: Some(proto::embed::Embed::Url("abcde".to_string())),
                },
                Embed {
                    embed: Some(proto::embed::Embed::Url("fghi".to_string())),
                },
                Embed {
                    embed: Some(proto::embed::Embed::CastId(CastId {
                        fid: FID_FOR_TEST + 1,
                        hash: rand::random::<[u8; 20]>().to_vec(),
                    })),
                },
                Embed {
                    embed: Some(proto::embed::Embed::Url("jklmn".to_string())),
                },
            ],
            None,
            vec![],
            None,
            None,
        );
        commit_message_at(&mut engine, &four_embeds, &FarcasterTime::current()).await;
        assert!(!TrieKey::for_message(&four_embeds)
            .iter()
            .all(|key| engine.trie_key_exists(&trie_ctx(), &key)));

        commit_event(&mut engine, &pro_event).await;
        assert!(engine.trie_key_exists(
            test_helper::trie_ctx(),
            &TrieKey::for_onchain_event(&pro_event)
        ));

        commit_message_at(&mut engine, &four_embeds, &FarcasterTime::current()).await;
        assert!(TrieKey::for_message(&four_embeds)
            .iter()
            .all(|key| engine.trie_key_exists(&trie_ctx(), &key)));
    }

    #[tokio::test]
    async fn pro_users_get_banners() {
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;
        let banner = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST,
            proto::UserDataType::Banner,
            &"image".to_string(),
            None,
            None,
        );
        let pro_event = events_factory::create_pro_user_event(
            FID_FOR_TEST,
            1,
            Some(time::current_timestamp_with_offset(-1)),
        );
        commit_message_at(&mut engine, &banner, &FarcasterTime::current()).await;
        assert!(!TrieKey::for_message(&banner)
            .iter()
            .all(|key| engine.trie_key_exists(&trie_ctx(), &key)));

        commit_event(&mut engine, &pro_event).await;
        assert!(engine.trie_key_exists(
            test_helper::trie_ctx(),
            &TrieKey::for_onchain_event(&pro_event)
        ));

        commit_message_at(&mut engine, &banner, &FarcasterTime::current()).await;
        assert!(TrieKey::for_message(&banner)
            .iter()
            .all(|key| engine.trie_key_exists(&trie_ctx(), &key)));
    }

    #[tokio::test]
    async fn test_block_confirmed_event_is_always_first() {
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        let mut event_rx = engine.get_senders().events_tx.subscribe();

        // Register user to create some events
        test_helper::register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;

        // Drain all previous events
        while event_rx.try_recv().is_ok() {}

        // Test with multiple messages
        let message1 = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test1", None, None);
        let message2 = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test2", None, None);
        let message3 = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test3", None, None);

        let _chunk =
            test_helper::commit_messages(&mut engine, vec![message1, message2, message3]).await;

        // Verify BLOCK_CONFIRMED event is received first
        let first_event = event_rx.recv().await.unwrap();
        assert_eq!(
            first_event.r#type,
            proto::HubEventType::BlockConfirmed as i32
        );

        // Verify BLOCK_CONFIRMED event has correct data
        if let Some(proto::hub_event::Body::BlockConfirmedBody(body)) = &first_event.body {
            assert_eq!(
                body.block_number,
                _chunk.header.as_ref().unwrap().height.unwrap().block_number
            );
            assert_eq!(
                body.shard_index,
                _chunk.header.as_ref().unwrap().height.unwrap().shard_index
            );
            assert_eq!(body.timestamp, _chunk.header.as_ref().unwrap().timestamp);
            assert_eq!(body.total_events, 4); // BLOCK_CONFIRMED + 3 MergeMessage events
            assert_eq!(
                body.event_counts_by_type[&(HubEventType::BlockConfirmed as i32)],
                1
            );
            assert_eq!(
                body.event_counts_by_type[&(HubEventType::MergeMessage as i32)],
                3
            );
            // If there are no events for a type, that type does not appear in the mapping
            assert_eq!(
                body.event_counts_by_type
                    .get(&(HubEventType::MergeOnChainEvent as i32)),
                None
            )
        } else {
            panic!("Expected BlockConfirmedBody");
        }

        // Verify we receive 3 message events after BLOCK_CONFIRMED
        for _ in 0..3 {
            let event = event_rx.recv().await.unwrap();
            assert_eq!(event.r#type, proto::HubEventType::MergeMessage as i32);
        }
    }

    #[tokio::test]
    async fn test_block_confirmed_event_sequence_number() {
        // Test that events are committed to the database in the correct order.
        // This is distinct from the previous test which checks that events are emitted
        // from the channel in the right order.
        let (mut engine, _tmpdir) = test_helper::new_engine().await;

        // Register user
        test_helper::register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;

        // Create and commit a message
        let message = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test", None, None);
        let _chunk = test_helper::commit_message(&mut engine, &message).await;

        // Get events from database for this specific block
        let block_number = _chunk.header.as_ref().unwrap().height.unwrap().block_number;
        let events = HubEvent::get_events(engine.db.clone(), 0, None, None).unwrap();

        // Find BLOCK_CONFIRMED event for this block
        let block_confirmed_event = events
            .events
            .iter()
            .find(|e| {
                e.r#type == proto::HubEventType::BlockConfirmed as i32
                    && HubEventIdGenerator::extract_height_and_seq(e.id).0 == block_number
            })
            .expect("BLOCK_CONFIRMED event not found");

        // Verify BLOCK_CONFIRMED has sequence 0
        let (event_block_number, sequence) =
            HubEventIdGenerator::extract_height_and_seq(block_confirmed_event.id);
        assert_eq!(event_block_number, block_number);
        assert_eq!(sequence, 0);

        // Verify message event has sequence 1
        let message_event = events
            .events
            .iter()
            .find(|e| {
                e.r#type == proto::HubEventType::MergeMessage as i32
                    && HubEventIdGenerator::extract_height_and_seq(e.id).0 == block_number
            })
            .expect("MergeMessage event not found");
        let (_, sequence) = HubEventIdGenerator::extract_height_and_seq(message_event.id);
        assert_eq!(sequence, 1);
    }

    #[tokio::test]
    async fn test_block_confirmed_event_with_no_messages() {
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        let mut event_rx = engine.get_senders().events_tx.subscribe();

        // Create empty state change
        let state_change = engine.propose_state_change(1, vec![], None);
        let _chunk =
            test_helper::validate_and_commit_state_change(&mut engine, &state_change).await;

        // Verify only BLOCK_CONFIRMED event is received
        let event = event_rx.recv().await.unwrap();
        assert_eq!(event.r#type, proto::HubEventType::BlockConfirmed as i32);

        // Verify BLOCK_CONFIRMED has correct total_events (just itself)
        if let Some(proto::hub_event::Body::BlockConfirmedBody(body)) = &event.body {
            assert_eq!(body.total_events, 1); // Only BLOCK_CONFIRMED event
            assert_eq!(
                body.block_number,
                _chunk.header.as_ref().unwrap().height.unwrap().block_number
            );
            assert_eq!(
                body.shard_index,
                _chunk.header.as_ref().unwrap().height.unwrap().shard_index
            );
        } else {
            panic!("Expected BlockConfirmedBody");
        }

        // Verify no more events are received
        let timeout_result =
            tokio::time::timeout(std::time::Duration::from_millis(100), event_rx.recv()).await;
        assert!(timeout_result.is_err()); // Should timeout, no more events
    }

    #[tokio::test]
    async fn test_post_commit() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let (mut engine, _tmpdir) = test_helper::new_engine_with_options(EngineOptions {
            post_commit_tx: Some(tx),
            ..Default::default()
        })
        .await;

        let mut handles = vec![];

        let handle = tokio::spawn(async move {
            let result =
                tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv()).await;

            assert!(
                result.is_ok() && result.as_ref().unwrap().is_some(),
                "Did not receive a successful post-commit hook"
            );

            let value = result.unwrap().unwrap();

            assert!(
                value.channel.send(true).is_ok(),
                "Failed to send post-commit callback"
            );
        });
        handles.push(handle);

        let handle = tokio::spawn(async move {
            test_helper::register_user(
                FID_FOR_TEST,
                test_helper::default_signer(),
                test_helper::default_custody_address(),
                &mut engine,
            )
            .await;
        });
        handles.push(handle);

        for handle in handles {
            if let Err(e) = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await {
                panic!("Task failed: {:?}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_post_commit_does_not_block_on_receiver() {
        let (tx, mut _rx) = tokio::sync::mpsc::channel(1);
        let (mut engine, _tmpdir) = test_helper::new_engine_with_options(EngineOptions {
            post_commit_tx: Some(tx),
            ..Default::default()
        })
        .await;

        let commit_future = test_helper::register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        );

        let result = tokio::time::timeout(std::time::Duration::from_secs(1), commit_future).await;

        assert!(
            result.is_ok(),
            "Post-commit hook should not block on receiver"
        );
    }

    #[tokio::test]
    async fn test_merge_block_events() {
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        let mut event_rx = engine.get_senders().events_tx.subscribe();

        // Don't merge event unless all previous have been merged
        let block_event2 = events_factory::create_heartbeat_event(2);
        commit_block_events(&mut engine, vec![&block_event2]).await;
        assert!(!block_event_exists(&engine, &block_event2));
        let block_confirmed = assert_block_confirmed_event(event_rx.recv().await.unwrap());
        assert_eq!(block_confirmed.max_block_event_seqnum, 0);

        // Ordering within a transaction matters
        let block_event1 = events_factory::create_heartbeat_event(1);
        commit_block_events(&mut engine, vec![&block_event2, &block_event1]).await;
        assert!(block_event_exists(&engine, &block_event1));
        assert!(!block_event_exists(&engine, &block_event2));
        let block_confirmed = assert_block_confirmed_event(event_rx.recv().await.unwrap());
        assert_eq!(block_confirmed.max_block_event_seqnum, 1);

        // Merge multiple block events in a single block
        let block_event3 = events_factory::create_heartbeat_event(3);
        let block_event4 = events_factory::create_heartbeat_event(4);
        commit_block_events(
            &mut engine,
            vec![&block_event2, &block_event3, &block_event4],
        )
        .await;
        let block_confirmed = assert_block_confirmed_event(event_rx.recv().await.unwrap());
        assert!(block_event_exists(&engine, &block_event2));
        assert!(block_event_exists(&engine, &block_event3));
        assert!(block_event_exists(&engine, &block_event4));
        assert_eq!(block_confirmed.max_block_event_seqnum, 4);
    }

    #[tokio::test]
    async fn test_cached_txn_validation_with_extra_block_event() {
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        let mut event_rx = engine.get_senders().events_tx.subscribe();

        // Create a heartbeat block event
        let block_event1 = events_factory::create_heartbeat_event(1);

        // Propose a state change with the block event
        let state_change = engine.propose_state_change(
            1,
            vec![MempoolMessage::BlockEvent {
                for_shard: 1,
                message: block_event1.clone(),
            }],
            None,
        );

        // Validate the proposal
        let height = engine.get_confirmed_height();
        engine.start_round(height.increment(), Round::Nil);
        let valid = engine.validate_state_change(&state_change, height.increment());
        assert!(valid);

        // Create empty shard chunk
        let new_state_change = ShardStateChange {
            shard_id: 1,
            timestamp: state_change.timestamp,
            new_state_root: state_change.new_state_root,
            events: vec![],
            transactions: vec![],
            version: state_change.version,
            max_block_event_seqnum: 0,
        };
        let chunk =
            test_helper::state_change_to_shard_chunk(1, height.block_number + 1, &new_state_change);
        engine.commit_shard_chunk(&chunk).await;

        assert!(!block_event_exists(&engine, &block_event1));
        let block_confirmed = assert_block_confirmed_event(event_rx.recv().await.unwrap());
        assert_eq!(block_confirmed.max_block_event_seqnum, 0);
    }

    #[tokio::test]
    async fn test_cached_txn_validation_with_missing_block_event() {
        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        let mut event_rx = engine.get_senders().events_tx.subscribe();

        // Propose a state change with no block event
        let state_change = engine.propose_state_change(1, vec![], None);

        // Validate the proposal
        let height = engine.get_confirmed_height();
        engine.start_round(height.increment(), Round::Nil);
        let valid = engine.validate_state_change(&state_change, height.increment());
        assert!(valid);

        let block_event1 = events_factory::create_heartbeat_event(1);
        // Create shard chunk with heartbeat event
        let new_state_change = ShardStateChange {
            shard_id: 1,
            timestamp: state_change.timestamp,
            new_state_root: state_change.new_state_root,
            events: vec![],
            transactions: MempoolPoller::create_transactions_from_mempool(vec![
                MempoolMessage::BlockEvent {
                    for_shard: 1,
                    message: block_event1.clone(),
                },
            ])
            .unwrap(),
            version: state_change.version,
            max_block_event_seqnum: 1,
        };
        let chunk =
            test_helper::state_change_to_shard_chunk(1, height.block_number + 1, &new_state_change);
        engine.commit_shard_chunk(&chunk).await;

        assert!(block_event_exists(&engine, &block_event1));
        let block_confirmed = assert_block_confirmed_event(event_rx.recv().await.unwrap());
        assert_eq!(block_confirmed.max_block_event_seqnum, 1);
    }

    #[tokio::test]
    async fn test_storage_lending() {
        let (mut engine, _temp_dir) = test_helper::new_engine().await;

        let lender_fid = FID_FOR_TEST;
        register_user(
            lender_fid,
            generate_signer(),
            default_custody_address(),
            &mut engine,
        )
        .await;

        let borrower_fid = FID2_FOR_TEST;
        register_user(
            borrower_fid,
            generate_signer(),
            default_custody_address(),
            &mut engine,
        )
        .await;

        let lend_message = messages_factory::storage_lend::create_storage_lend(
            lender_fid,
            borrower_fid,
            1, // Lend 1 unit
            crate::proto::StorageUnitType::UnitType2025,
            Some(1),
            None,
        );
        let storage_lend_block_event = create_merge_message_event(lend_message, 1);
        commit_block_events(&mut engine, vec![&storage_lend_block_event]).await;

        // Verify the borrower now has storage
        let borrower_storage = engine
            .get_stores()
            .get_storage_slot_for_fid(borrower_fid, true, &vec![])
            .unwrap();
        assert_eq!(
            borrower_storage.units_for(crate::proto::StorageUnitType::UnitType2025),
            2
        );
        // Verify the lender's storage was reduced
        let lender_storage = engine
            .get_stores()
            .get_storage_slot_for_fid(lender_fid, true, &vec![])
            .unwrap();
        // Lender should have default storage minus 1 unit lent
        assert_eq!(
            lender_storage.units_for(crate::proto::StorageUnitType::UnitType2025),
            0
        );

        // Reclaim the lent storage
        let lend_message = messages_factory::storage_lend::create_storage_lend(
            lender_fid,
            borrower_fid,
            0, // Set lent storage to 0
            crate::proto::StorageUnitType::UnitType2025,
            Some(2),
            None,
        );
        commit_block_events(
            &mut engine,
            vec![&create_merge_message_event(lend_message.clone(), 2)],
        )
        .await;
        // Verify the lender's storage was returned
        let borrower_storage = engine
            .get_stores()
            .get_storage_slot_for_fid(borrower_fid, true, &vec![])
            .unwrap();
        assert_eq!(
            borrower_storage.units_for(crate::proto::StorageUnitType::UnitType2025),
            1
        );
        let lender_storage = engine
            .get_stores()
            .get_storage_slot_for_fid(lender_fid, true, &vec![])
            .unwrap();
        assert_eq!(
            lender_storage.units_for(crate::proto::StorageUnitType::UnitType2025),
            1
        );
        assert!(!message_exists_in_trie(&mut engine, &lend_message))
    }
}
