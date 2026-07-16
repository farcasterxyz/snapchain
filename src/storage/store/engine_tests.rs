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

    const PRE_V20_TESTNET_UNIX_TIMESTAMP: u64 = 1_784_124_000;

    fn pre_v20_testnet_time(offset_seconds: u64) -> FarcasterTime {
        FarcasterTime::from_unix_seconds(PRE_V20_TESTNET_UNIX_TIMESTAMP + offset_seconds)
    }

    fn pre_v20_testnet_message_timestamp(offset_seconds: u64) -> u32 {
        pre_v20_testnet_time(offset_seconds).to_u64() as u32
    }

    async fn new_pre_v20_testnet_engine() -> (ShardEngine, tempfile::TempDir) {
        test_helper::new_engine_with_options(EngineOptions {
            network: Some(FarcasterNetwork::Testnet),
            ..EngineOptions::default()
        })
        .await
    }

    async fn commit_pre_v20_message(engine: &mut ShardEngine, message: &proto::Message) {
        let block_time = FarcasterTime::new(message.data.as_ref().unwrap().timestamp as u64);
        assert_eq!(
            EngineVersion::version_for(&block_time, FarcasterNetwork::Testnet),
            EngineVersion::V19
        );
        test_helper::commit_message_at(engine, message, &block_time).await;
        assert!(test_helper::message_exists_in_trie(engine, message));
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
    async fn test_engine_block_link_survives_follow_compaction_at_v19() {
        // End-to-end through the engine on devnet (V19): the engine derives
        // scope_link_compaction = is_enabled(BlockLinks) = true and threads it down, so a
        // follow compact state compacts only follows and leaves a block link intact.
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

        // A follow add the later compact state will sweep (its target isn't listed), and a
        // block add to the same target that must survive a follow compaction.
        let follow_add = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            "follow",
            target_fid,
            Some(timestamp),
            None,
        );
        commit_message(&mut engine, &follow_add).await;
        let block_add = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            "block",
            target_fid,
            Some(timestamp),
            None,
        );
        commit_message(&mut engine, &block_add).await;

        let follow_compact_state = messages_factory::links::create_link_compact_state(
            FID_FOR_TEST,
            "follow",
            vec![target_fid + 100],
            Some(timestamp + 1),
            None,
        );
        commit_message(&mut engine, &follow_compact_state).await;

        // Compaction ran (the non-target follow is gone) but was type-scoped (block survives).
        assert!(!message_exists_in_trie(&mut engine, &follow_add));
        assert!(message_exists_in_trie(&mut engine, &block_add));
    }

    #[tokio::test]
    async fn test_engine_block_link_deleted_by_follow_compaction_pre_v19() {
        // End-to-end on testnet at a pre-V19 timestamp: the engine derives
        // scope_link_compaction = false, so a follow compact state compacts type-blind and
        // deletes the block link too — the legacy behavior preserved for deterministic replay.
        // Pinned to a fixed instant inside the V18 window (unix 1782000000, ~2026-06-20) rather
        // than wall-clock time, which drifts past the 2026-07-15 14:00 UTC testnet V19 cutover.
        let timestamp = FarcasterTime::from_unix_seconds(1782000000).to_u64() as u32;
        let target_fid = 15;
        let (mut engine, _tmpdir) = test_helper::new_engine_with_options(EngineOptions {
            network: Some(FarcasterNetwork::Testnet),
            ..Default::default()
        })
        .await;
        test_helper::register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;

        let block_add = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            "block",
            target_fid,
            Some(timestamp),
            None,
        );
        commit_message(&mut engine, &block_add).await;

        let follow_compact_state = messages_factory::links::create_link_compact_state(
            FID_FOR_TEST,
            "follow",
            vec![target_fid + 100],
            Some(timestamp + 1),
            None,
        );
        commit_message(&mut engine, &follow_compact_state).await;

        // Pre-V19, compaction is type-blind: the follow compact state deletes the block too.
        assert!(!message_exists_in_trie(&mut engine, &block_add));
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
        let timestamp = pre_v20_testnet_message_timestamp(0);
        let (mut engine, _tmpdir) = new_pre_v20_testnet_engine().await;
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

        commit_pre_v20_message(&mut engine, &verification_add).await;

        let verification_result = engine.get_verifications_by_fid(FID3_FOR_TEST);
        assert_eq!(1, verification_result.unwrap().messages.len());

        let verification_remove = messages_factory::verifications::create_verification_remove(
            FID3_FOR_TEST,
            hex::decode("91031dcfdea024b4d51e775486111d2b2a715871").unwrap(),
            Some(timestamp + 1),
            None,
        );

        commit_pre_v20_message(&mut engine, &verification_remove).await;

        let verification_result = engine.get_verifications_by_fid(FID_FOR_TEST);
        assert_eq!(0, verification_result.unwrap().messages.len());
    }

    #[tokio::test]
    async fn test_data_shard_verification_admission_splits_at_v20() {
        let pre_v20_block_time = pre_v20_testnet_time(0);
        let timestamp = pre_v20_block_time.to_u64() as u32;
        let verification_add = messages_factory::verifications::create_verification_add(
            FID3_FOR_TEST,
            0,
            hex::decode("91031dcfdea024b4d51e775486111d2b2a715871").unwrap(),
            hex::decode("b72c63d61f075b36fb66a9a867b50836cef19d653a3c09005628738677bcb25f25b6b6e6d2e1d69cd725327b3c020deef9e2575a22dc8ed08f88bc75718ce1cb1c").unwrap(),
            hex::decode("d74860c4bbf574d5ad60f03a478a30f990e05ac723e138a5c860cdb3095f4296").unwrap(),
            Some(timestamp),
            None,
        );
        let verification_remove = messages_factory::verifications::create_verification_remove(
            FID3_FOR_TEST,
            hex::decode("91031dcfdea024b4d51e775486111d2b2a715871").unwrap(),
            Some(timestamp + 1),
            None,
        );

        let (mut pre_v20_engine, _tmpdir) = new_pre_v20_testnet_engine().await;
        register_user(
            FID3_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut pre_v20_engine,
        )
        .await;
        commit_pre_v20_message(&mut pre_v20_engine, &verification_add).await;

        let (mut post_v20_engine, _tmpdir) = test_helper::new_engine().await;
        register_user(
            FID3_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut post_v20_engine,
        )
        .await;
        for message in [&verification_add, &verification_remove] {
            let error = post_v20_engine
                .validate_user_message(
                    message,
                    &FarcasterTime::new(message.data.as_ref().unwrap().timestamp as u64),
                    EngineVersion::V20,
                    &mut RocksDbTransactionBatch::new(),
                )
                .unwrap_err();
            assert!(matches!(
                error,
                MessageValidationError::InvalidMessageType(message_type)
                    if message_type == message.msg_type() as i32
            ));
        }
    }

    #[tokio::test]
    async fn test_validate_ethereum_address_with_verification() {
        let timestamp = pre_v20_testnet_message_timestamp(0);
        let (mut engine, _tmpdir) = new_pre_v20_testnet_engine().await;

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

        commit_pre_v20_message(&mut engine, &verification_add).await;

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
        let timestamp = pre_v20_testnet_message_timestamp(0);
        let (mut engine, _tmpdir) = new_pre_v20_testnet_engine().await;

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
        commit_pre_v20_message(&mut engine, &verification_add).await;

        // Step 2: Set the Ethereum address as primary address
        let primary_address_msg = messages_factory::user_data::create_user_data_add(
            FID3_FOR_TEST,
            proto::UserDataType::UserDataPrimaryAddressEthereum,
            &eth_address_checksummed.to_string(),
            Some(timestamp + 1),
            None,
        );
        commit_pre_v20_message(&mut engine, &primary_address_msg).await;

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
        commit_pre_v20_message(&mut engine, &verification_remove).await;

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
        let timestamp = pre_v20_testnet_message_timestamp(0);
        let (mut engine, _tmpdir) = new_pre_v20_testnet_engine().await;

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
        commit_pre_v20_message(&mut engine, &primary_verification_add).await;

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
        commit_pre_v20_message(&mut engine, &other_verification_add).await;

        // Set the primary address
        let primary_address_msg = messages_factory::user_data::create_user_data_add(
            FID3_FOR_TEST,
            proto::UserDataType::UserDataPrimaryAddressEthereum,
            &primary_address_checksummed,
            Some(timestamp + 1),
            None,
        );
        commit_pre_v20_message(&mut engine, &primary_address_msg).await;

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
        commit_pre_v20_message(&mut engine, &other_verification_remove).await;

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
        assert!(!updated_account_root.is_empty());
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
        assert!(!account_root_another_fid.is_empty());
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
            result.is_empty(),
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
            .get_storage_slot_for_fid(borrower_fid, EngineVersion::latest(), true, &vec![])
            .unwrap();
        assert_eq!(
            borrower_storage.units_for(crate::proto::StorageUnitType::UnitType2025),
            2
        );
        // Verify the lender's storage was reduced
        let lender_storage = engine
            .get_stores()
            .get_storage_slot_for_fid(lender_fid, EngineVersion::latest(), true, &vec![])
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
            .get_storage_slot_for_fid(borrower_fid, EngineVersion::latest(), true, &vec![])
            .unwrap();
        assert_eq!(
            borrower_storage.units_for(crate::proto::StorageUnitType::UnitType2025),
            1
        );
        let lender_storage = engine
            .get_stores()
            .get_storage_slot_for_fid(lender_fid, EngineVersion::latest(), true, &vec![])
            .unwrap();
        assert_eq!(
            lender_storage.units_for(crate::proto::StorageUnitType::UnitType2025),
            1
        );
        assert!(!message_exists_in_trie(&mut engine, &lend_message))
    }

    mod verification_replay_tests {
        use super::*;
        use crate::proto::{hub_event, MergeMessageBody, Message};
        use crate::storage::store::account::{make_ts_hash, VerificationStore};

        const VERIFICATION_FID: u64 = FID3_FOR_TEST;
        const VERIFICATION_ADDRESS_HEX: &str = "91031dcfdea024b4d51e775486111d2b2a715871";
        const VERIFICATION_CLAIM_SIGNATURE_HEX: &str = "b72c63d61f075b36fb66a9a867b50836cef19d653a3c09005628738677bcb25f25b6b6e6d2e1d69cd725327b3c020deef9e2575a22dc8ed08f88bc75718ce1cb1c";
        const VERIFICATION_BLOCK_HASH_HEX: &str =
            "d74860c4bbf574d5ad60f03a478a30f990e05ac723e138a5c860cdb3095f4296";

        fn verification_address() -> Vec<u8> {
            hex::decode(VERIFICATION_ADDRESS_HEX).unwrap()
        }

        fn verification_add(timestamp: u32) -> Message {
            messages_factory::verifications::create_verification_add(
                VERIFICATION_FID,
                0,
                verification_address(),
                hex::decode(VERIFICATION_CLAIM_SIGNATURE_HEX).unwrap(),
                hex::decode(VERIFICATION_BLOCK_HASH_HEX).unwrap(),
                Some(timestamp),
                None,
            )
        }

        fn verification_remove(timestamp: u32) -> Message {
            verification_remove_for_address(verification_address(), timestamp)
        }

        fn verification_remove_for_address(address: Vec<u8>, timestamp: u32) -> Message {
            messages_factory::verifications::create_verification_remove(
                VERIFICATION_FID,
                address,
                Some(timestamp),
                None,
            )
        }

        async fn replay_verification(
            engine: &mut ShardEngine,
            message: &Message,
            seqnum: u64,
        ) -> ShardStateChange {
            replay_verifications(engine, &[(message.clone(), seqnum)]).await
        }

        /// Replay several BlockEvents inside a single block, in the order given.
        async fn replay_verifications(
            engine: &mut ShardEngine,
            messages: &[(Message, u64)],
        ) -> ShardStateChange {
            let state_change = engine.propose_state_change(
                engine.shard_id(),
                messages
                    .iter()
                    .map(|(message, seqnum)| MempoolMessage::BlockEvent {
                        for_shard: engine.shard_id(),
                        message: create_merge_message_event(message.clone(), *seqnum),
                    })
                    .collect(),
                None,
            );
            test_helper::validate_and_commit_state_change(engine, &state_change).await;
            state_change
        }

        fn merge_body_for<'a>(
            state_change: &'a ShardStateChange,
            message: &Message,
        ) -> &'a MergeMessageBody {
            let matches = state_change
                .events
                .iter()
                .filter_map(|event| match &event.body {
                    Some(hub_event::Body::MergeMessageBody(body))
                        if body.message.as_ref() == Some(message) =>
                    {
                        Some(body)
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(matches.len(), 1, "expected exactly one replay merge event");
            matches[0]
        }

        fn assert_verification_index(engine: &ShardEngine, expected: Option<&Message>) {
            let stores = engine.get_stores();
            let entries = VerificationStore::get_verifications_by_address(
                &stores.verification_store,
                &verification_address(),
                None,
            )
            .unwrap();
            match expected {
                Some(message) => assert_eq!(
                    entries,
                    vec![(
                        VERIFICATION_FID,
                        make_ts_hash(message.data.as_ref().unwrap().timestamp, &message.hash)
                            .unwrap()
                    )]
                ),
                None => assert!(entries.is_empty()),
            }
        }

        #[tokio::test]
        async fn active_replay_preserves_message_index_trie_event_and_by_fid_read() {
            let (mut engine, _temp_dir) = test_helper::new_engine().await;
            let add = verification_add(messages_factory::farcaster_time());

            let state_change = replay_verification(&mut engine, &add, 1).await;

            let stores = engine.get_stores();
            assert_eq!(
                VerificationStore::get_verification_add(
                    &stores.verification_store,
                    VERIFICATION_FID,
                    &verification_address(),
                    None,
                )
                .unwrap(),
                Some(add.clone())
            );
            assert_verification_index(&engine, Some(&add));
            assert!(message_exists_in_trie(&mut engine, &add));
            assert_eq!(
                engine
                    .get_verifications_by_fid(VERIFICATION_FID)
                    .unwrap()
                    .messages,
                vec![add.clone()]
            );
            assert!(merge_body_for(&state_change, &add)
                .deleted_messages
                .is_empty());
        }

        #[tokio::test]
        async fn replicator_replay_bypasses_post_v20_direct_admission_reject() {
            let (mut engine, _temp_dir) = test_helper::new_engine().await;
            let add = verification_add(messages_factory::farcaster_time());
            engine
                .commit_replicator_message_for_test(&add)
                .expect("replicator must bypass direct admission");
            assert!(message_exists_in_trie(&mut engine, &add));
        }

        // A replay can carry a message the fid shard already holds verbatim. Force override must
        // treat that as a no-op, not as a supersede of the row by itself: a self-supersede would
        // list the message in its own event's `deleted_messages`, and because
        // `MerkleTrie::update_for_event` applies inserts before deletes on a hash-keyed trie key,
        // it would drop the message from the trie while the store kept the row -- a store/trie
        // divergence, and an empty-trie `UnableToReloadRoot` panic in the degenerate case.
        #[tokio::test]
        async fn identical_replay_is_idempotent_across_store_index_and_trie() {
            let (mut engine, _temp_dir) = test_helper::new_engine().await;
            register_user(
                VERIFICATION_FID,
                test_helper::default_signer(),
                test_helper::default_custody_address(),
                &mut engine,
            )
            .await;
            let add = verification_add(messages_factory::farcaster_time());

            replay_verification(&mut engine, &add, 1).await;
            let state_change = replay_verification(&mut engine, &add, 2).await;

            let stores = engine.get_stores();
            assert_eq!(
                VerificationStore::get_verification_add(
                    &stores.verification_store,
                    VERIFICATION_FID,
                    &verification_address(),
                    None,
                )
                .unwrap(),
                Some(add.clone())
            );
            assert_verification_index(&engine, Some(&add));
            assert!(
                message_exists_in_trie(&mut engine, &add),
                "identical replay must leave the trie key intact"
            );
            assert_eq!(
                engine
                    .get_verifications_by_fid(VERIFICATION_FID)
                    .unwrap()
                    .messages,
                vec![add.clone()]
            );
            assert!(
                merge_body_for(&state_change, &add)
                    .deleted_messages
                    .is_empty(),
                "a message must never appear in its own deleted_messages"
            );
        }

        // End-to-end cutover straddle. A verification merged LIVE on a data shard before the V20
        // cutover, then the SAME verification arriving again by shard-0 forced replay after
        // cutover. This is the window case `routing.rs` documents: self-supersede keeps the store,
        // by-address index, and trie mutually consistent (the replay is a state no-op), while the
        // event stream is deliberately NOT idempotent -- the replay still emits a fresh
        // MergeMessage HubEvent with empty `deleted_messages`, which is why a subscriber may see
        // the verification twice even though state converges.
        //
        // `commit_replicator_message_for_test` stands in for the pre-cutover live merge: it seeds
        // the row the data shard would already hold from the pre-V20 regime, which post-V20 direct
        // admission now rejects (that rejection is exactly what makes shard 0 the only live path).
        #[tokio::test]
        async fn cutover_straddle_live_then_replay_converges_state_but_re_emits_event() {
            let (mut engine, _temp_dir) = test_helper::new_engine().await;
            register_user(
                VERIFICATION_FID,
                test_helper::default_signer(),
                test_helper::default_custody_address(),
                &mut engine,
            )
            .await;
            let add = verification_add(messages_factory::farcaster_time());

            // 1. Pre-cutover: the data shard already holds the live-merged verification.
            engine
                .commit_replicator_message_for_test(&add)
                .expect("pre-cutover live merge must succeed");
            {
                let stores = engine.get_stores();
                assert_eq!(
                    VerificationStore::get_verification_add(
                        &stores.verification_store,
                        VERIFICATION_FID,
                        &verification_address(),
                        None,
                    )
                    .unwrap(),
                    Some(add.clone()),
                );
            }
            assert_verification_index(&engine, Some(&add));
            assert!(message_exists_in_trie(&mut engine, &add));
            let root_after_live_merge = engine.trie_root_hash();

            // 2. Post-cutover: the SAME verification arrives again by shard-0 forced replay.
            let state_change = replay_verification(&mut engine, &add, 1).await;

            // 3. State converges -- the redundant replay is a no-op on store, index, and trie.
            {
                let stores = engine.get_stores();
                assert_eq!(
                    VerificationStore::get_verification_add(
                        &stores.verification_store,
                        VERIFICATION_FID,
                        &verification_address(),
                        None,
                    )
                    .unwrap(),
                    Some(add.clone()),
                    "replaying an already-held verification must not change the add row",
                );
            }
            assert_verification_index(&engine, Some(&add));
            assert!(
                message_exists_in_trie(&mut engine, &add),
                "self-supersede must leave the trie key intact",
            );
            assert_eq!(
                engine
                    .get_verifications_by_fid(VERIFICATION_FID)
                    .unwrap()
                    .messages,
                vec![add.clone()],
            );
            assert_eq!(
                engine.trie_root_hash(),
                root_after_live_merge,
                "the state root must be identical before and after the redundant replay",
            );

            // 4. The event stream is NOT idempotent: the replay still emits a fresh MergeMessage
            //    for the same message (the duplicate a subscriber may observe), and a message must
            //    never supersede itself, so its own `deleted_messages` stays empty.
            let merge_body = merge_body_for(&state_change, &add);
            assert!(
                merge_body.deleted_messages.is_empty(),
                "a re-merged message must never appear in its own deleted_messages",
            );
        }

        // Two BlockEvents for the same (fid, address) in one block. Conflict discovery reads
        // through the shared txn batch, so the second must see the first's uncommitted write and
        // supersede it. BlockEvent order wins over embedded timestamps, hence the older remove.
        #[tokio::test]
        async fn two_replays_for_one_address_in_one_block_settle_on_the_last() {
            let (mut engine, _temp_dir) = test_helper::new_engine().await;
            let timestamp = messages_factory::farcaster_time();
            let add = verification_add(timestamp + 10);
            let remove = verification_remove(timestamp);

            replay_verifications(&mut engine, &[(add.clone(), 1), (remove.clone(), 2)]).await;

            let stores = engine.get_stores();
            assert_eq!(
                VerificationStore::get_verification_remove(
                    &stores.verification_store,
                    VERIFICATION_FID,
                    &verification_address(),
                )
                .unwrap(),
                Some(remove.clone())
            );
            assert_eq!(
                VerificationStore::get_verification_add(
                    &stores.verification_store,
                    VERIFICATION_FID,
                    &verification_address(),
                    None,
                )
                .unwrap(),
                None
            );
            assert_verification_index(&engine, None);
            assert!(!message_exists_in_trie(&mut engine, &add));
            assert!(message_exists_in_trie(&mut engine, &remove));
        }

        #[tokio::test]
        async fn replayed_older_remove_force_overrides_newer_add() {
            let (mut engine, _temp_dir) = test_helper::new_engine().await;
            register_user(
                VERIFICATION_FID,
                test_helper::default_signer(),
                test_helper::default_custody_address(),
                &mut engine,
            )
            .await;
            let timestamp = messages_factory::farcaster_time();
            let existing_add = verification_add(timestamp + 10);
            engine
                .commit_replicator_message_for_test(&existing_add)
                .unwrap();

            let replayed_remove = verification_remove(timestamp);
            let state_change = replay_verification(&mut engine, &replayed_remove, 1).await;

            let stores = engine.get_stores();
            assert_eq!(
                VerificationStore::get_verification_add(
                    &stores.verification_store,
                    VERIFICATION_FID,
                    &verification_address(),
                    None,
                )
                .unwrap(),
                None
            );
            assert_eq!(
                VerificationStore::get_verification_remove(
                    &stores.verification_store,
                    VERIFICATION_FID,
                    &verification_address(),
                )
                .unwrap(),
                Some(replayed_remove.clone())
            );
            assert_verification_index(&engine, None);
            assert!(!message_exists_in_trie(&mut engine, &existing_add));
            assert!(message_exists_in_trie(&mut engine, &replayed_remove));
            assert!(engine
                .get_verifications_by_fid(VERIFICATION_FID)
                .unwrap()
                .messages
                .is_empty());
            assert_eq!(
                merge_body_for(&state_change, &replayed_remove).deleted_messages,
                vec![existing_add]
            );
        }

        #[tokio::test]
        async fn replayed_older_add_intentionally_force_overrides_newer_remove() {
            let (mut engine, _temp_dir) = test_helper::new_engine().await;
            register_user(
                VERIFICATION_FID,
                test_helper::default_signer(),
                test_helper::default_custody_address(),
                &mut engine,
            )
            .await;
            let timestamp = messages_factory::farcaster_time();
            let existing_remove = verification_remove(timestamp + 10);
            engine
                .commit_replicator_message_for_test(&existing_remove)
                .unwrap();

            let replayed_add = verification_add(timestamp);
            let state_change = replay_verification(&mut engine, &replayed_add, 1).await;

            // Intended protocol rule, not a bug: shard-0 consensus order wins even though the
            // replayed add's embedded timestamp is older than the data-shard tombstone.
            let stores = engine.get_stores();
            assert_eq!(
                VerificationStore::get_verification_remove(
                    &stores.verification_store,
                    VERIFICATION_FID,
                    &verification_address(),
                )
                .unwrap(),
                None
            );
            assert_eq!(
                VerificationStore::get_verification_add(
                    &stores.verification_store,
                    VERIFICATION_FID,
                    &verification_address(),
                    None,
                )
                .unwrap(),
                Some(replayed_add.clone())
            );
            assert_verification_index(&engine, Some(&replayed_add));
            assert!(!message_exists_in_trie(&mut engine, &existing_remove));
            assert!(message_exists_in_trie(&mut engine, &replayed_add));
            assert_eq!(
                engine
                    .get_verifications_by_fid(VERIFICATION_FID)
                    .unwrap()
                    .messages,
                vec![replayed_add.clone()]
            );
            assert_eq!(
                merge_body_for(&state_change, &replayed_add).deleted_messages,
                vec![existing_remove]
            );
        }

        #[tokio::test]
        async fn replayed_remove_revokes_primary_address_on_data_shard() {
            let (mut engine, _temp_dir) = test_helper::new_engine().await;
            register_user(
                VERIFICATION_FID,
                test_helper::default_signer(),
                test_helper::default_custody_address(),
                &mut engine,
            )
            .await;
            let timestamp = messages_factory::farcaster_time();
            let add = verification_add(timestamp);
            engine.commit_replicator_message_for_test(&add).unwrap();

            let checksummed =
                alloy_primitives::Address::from_slice(&verification_address()).to_checksum(None);
            let primary_address = messages_factory::user_data::create_user_data_add(
                VERIFICATION_FID,
                proto::UserDataType::UserDataPrimaryAddressEthereum,
                &checksummed,
                Some(timestamp + 1),
                None,
            );
            commit_message(&mut engine, &primary_address).await;

            let remove = verification_remove(timestamp + 2);
            let state_change = replay_verification(&mut engine, &remove, 1).await;

            assert!(engine
                .get_user_data_by_fid_and_type(
                    VERIFICATION_FID,
                    proto::UserDataType::UserDataPrimaryAddressEthereum,
                )
                .is_err());
            assert!(!message_exists_in_trie(&mut engine, &primary_address));
            assert!(state_change.events.iter().any(|event| {
                matches!(
                    &event.body,
                    Some(hub_event::Body::RevokeMessageBody(body))
                        if body.message.as_ref() == Some(&primary_address)
                )
            }));
            assert_eq!(
                merge_body_for(&state_change, &remove).deleted_messages,
                vec![add]
            );
        }

        #[tokio::test]
        async fn replay_prunes_combined_verification_state_to_storage_cap() {
            let five_verification_limits =
                StoreLimits::new(limits::legacy(), limits::legacy(), limits::legacy());
            let (mut engine, _temp_dir) = test_helper::new_engine_with_options(EngineOptions {
                limits: Some(five_verification_limits),
                ..EngineOptions::default()
            })
            .await;
            register_user(
                VERIFICATION_FID,
                test_helper::default_signer(),
                test_helper::default_custody_address(),
                &mut engine,
            )
            .await;
            let timestamp = messages_factory::farcaster_time();
            let pre_activation_rows = (1u8..=5)
                .enumerate()
                .map(|(index, byte)| {
                    verification_remove_for_address(
                        vec![byte; 20],
                        timestamp + u32::try_from(index).unwrap(),
                    )
                })
                .collect::<Vec<_>>();
            for message in &pre_activation_rows {
                engine.commit_replicator_message_for_test(message).unwrap();
            }

            let replayed_add = verification_add(timestamp + 10);
            let state_change = replay_verification(&mut engine, &replayed_add, 1).await;

            let stores = engine.get_stores();
            assert_eq!(
                VerificationStore::get_verification_remove(
                    &stores.verification_store,
                    VERIFICATION_FID,
                    &[1; 20],
                )
                .unwrap(),
                None,
                "the oldest pre-activation row must be pruned"
            );
            for (index, message) in pre_activation_rows.iter().enumerate().skip(1) {
                assert!(message_exists_in_trie(&mut engine, message));
                assert!(VerificationStore::get_verification_remove(
                    &stores.verification_store,
                    VERIFICATION_FID,
                    &[u8::try_from(index + 1).unwrap(); 20],
                )
                .unwrap()
                .is_some());
            }
            assert!(message_exists_in_trie(&mut engine, &replayed_add));
            assert_eq!(
                stores
                    .get_usage(
                        VERIFICATION_FID,
                        proto::MessageType::VerificationAddEthAddress,
                        &mut RocksDbTransactionBatch::new(),
                    )
                    .unwrap(),
                (5, 5)
            );
            assert!(state_change.events.iter().any(|event| {
                matches!(
                    &event.body,
                    Some(hub_event::Body::PruneMessageBody(body))
                        if body.message.as_ref() == Some(&pre_activation_rows[0])
                )
            }));
        }
    }

    // ----------------------------------------------------------------------------------------
    // KEY_ADD / KEY_REMOVE BlockEvent replay + downstream-message validation (NEYN-10618 +
    // NEYN-10626 in-process slice).
    //
    // These tests model how a non-shard-0 ShardEngine learns about gasless keys: shard 0
    // (BlockEngine) merges KEY_ADD / KEY_REMOVE and emits a `MergeMessageEventBody`; shards
    // 1..N receive those BlockEvents through `propose_state_change`, which dispatches them via
    // `handle_block_event` → `merge_message`. The replay path bypasses `validate_user_message`
    // by design — the BlockEvent itself is the authority — but the underlying merge code
    // still reads on-chain id_register events from the shard's local store, so both FIDs must
    // already be registered locally.
    // ----------------------------------------------------------------------------------------
    mod gasless_key_replay_tests {
        use super::*;
        use crate::storage::store::account::{
            get_active_key, get_gasless_key_owner_fid, get_gasless_key_record, get_last_used_at,
            ActiveKey,
        };
        use alloy_signer_local::PrivateKeySigner;

        const REQUEST_FID: u64 = FID_FOR_TEST + 100;

        fn address_bytes(signer: &PrivateKeySigner) -> Vec<u8> {
            signer.address().as_slice().to_vec()
        }

        async fn register_eth(
            engine: &mut ShardEngine,
            fid: u64,
            custody: &PrivateKeySigner,
            signer: ed25519_dalek::SigningKey,
        ) {
            register_user(fid, signer, address_bytes(custody), engine).await;
        }

        fn build_key_add(
            fid_custody: &PrivateKeySigner,
            app_custody: &PrivateKeySigner,
            envelope: &ed25519_dalek::SigningKey,
            scopes: Vec<proto::MessageType>,
            ttl: u32,
            nonce: u32,
            timestamp: u32,
        ) -> proto::Message {
            messages_factory::keys::create_key_add(
                FID_FOR_TEST,
                fid_custody,
                REQUEST_FID,
                app_custody,
                envelope,
                scopes,
                ttl,
                nonce,
                timestamp + 1_000_000,
                Some(timestamp),
            )
        }

        // -- replay-path tests ------------------------------------------------------------

        #[tokio::test]
        async fn test_shard_engine_replays_key_add_block_event() {
            let (mut engine, _temp_dir) = test_helper::new_engine().await;
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let envelope = generate_signer();
            let envelope_pubkey = envelope.verifying_key().to_bytes();

            register_eth(&mut engine, FID_FOR_TEST, &fid_custody, generate_signer()).await;
            register_eth(&mut engine, REQUEST_FID, &app_custody, generate_signer()).await;

            let timestamp = messages_factory::farcaster_time();
            let key_add = build_key_add(
                &fid_custody,
                &app_custody,
                &envelope,
                vec![proto::MessageType::CastAdd],
                3600,
                1,
                timestamp,
            );
            let block_event = create_merge_message_event(key_add.clone(), 1);
            commit_block_events(&mut engine, vec![&block_event]).await;

            let stores = engine.get_stores();
            let txn = RocksDbTransactionBatch::new();
            let record = get_gasless_key_record(&stores.db, &txn, FID_FOR_TEST, &envelope_pubkey)
                .unwrap()
                .expect("gasless record must materialize on the shard via replay");
            assert_eq!(record.request_fid, REQUEST_FID);
            assert_eq!(
                get_gasless_key_owner_fid(&stores.db, &txn, &envelope_pubkey).unwrap(),
                Some(FID_FOR_TEST),
            );
            assert_eq!(
                get_last_used_at(&stores.db, &txn, FID_FOR_TEST, &envelope_pubkey).unwrap(),
                Some(timestamp),
            );
        }

        #[tokio::test]
        async fn test_shard_engine_skips_pre_feature_block_event_replay() {
            // Rollback safety: a validator running on Mainnet (where V16 / GaslessSigners is
            // not yet active) that receives a BlockEvent wrapping a KEY_ADD must reject the
            // replay rather than merging into a pre-feature shard. The gate inside
            // `handle_block_event` matches the analogous live-admission gate in
            // `validate_user_message`, returning `InvalidMessageType` so the caller's `warn!`
            // surfaces the unusual replay attempt rather than silently no-op'ing.
            let (mut engine, _temp_dir) = test_helper::new_engine_with_options(EngineOptions {
                network: Some(FarcasterNetwork::Mainnet),
                ..Default::default()
            })
            .await;
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let envelope = generate_signer();
            let envelope_pubkey = envelope.verifying_key().to_bytes();

            register_eth(&mut engine, FID_FOR_TEST, &fid_custody, generate_signer()).await;
            register_eth(&mut engine, REQUEST_FID, &app_custody, generate_signer()).await;

            let timestamp = messages_factory::farcaster_time();
            let key_add = build_key_add(
                &fid_custody,
                &app_custody,
                &envelope,
                vec![proto::MessageType::CastAdd],
                3600,
                1,
                timestamp,
            );
            // BlockEvent default block_timestamp=0 → version_for(0, Mainnet) is pre-V16, so
            // GaslessSigners is disabled and the gate inside handle_block_event must fire.
            let block_event = create_merge_message_event(key_add, 1);
            commit_block_events(&mut engine, vec![&block_event]).await;

            // No gasless record materializes — the merge was short-circuited before any state
            // writes happened.
            let stores = engine.get_stores();
            let txn = RocksDbTransactionBatch::new();
            assert!(
                get_gasless_key_record(&stores.db, &txn, FID_FOR_TEST, &envelope_pubkey)
                    .unwrap()
                    .is_none()
            );
        }

        #[tokio::test]
        async fn test_shard_engine_replays_key_remove_block_event() {
            let (mut engine, _temp_dir) = test_helper::new_engine().await;
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let envelope = generate_signer();
            let envelope_pubkey: [u8; 32] = envelope.verifying_key().to_bytes();

            register_eth(&mut engine, FID_FOR_TEST, &fid_custody, generate_signer()).await;
            register_eth(&mut engine, REQUEST_FID, &app_custody, generate_signer()).await;

            let timestamp = messages_factory::farcaster_time();
            let key_add = build_key_add(
                &fid_custody,
                &app_custody,
                &envelope,
                vec![proto::MessageType::CastAdd],
                3600,
                1,
                timestamp,
            );
            let add_block_event = create_merge_message_event(key_add, 1);
            commit_block_events(&mut engine, vec![&add_block_event]).await;

            let key_remove = messages_factory::keys::create_key_remove_custody(
                FID_FOR_TEST,
                &fid_custody,
                &test_helper::default_signer(),
                &envelope_pubkey,
                2,
                timestamp + 1_000_000,
                Some(timestamp + 1),
            );
            let remove_block_event = create_merge_message_event(key_remove, 2);
            commit_block_events(&mut engine, vec![&remove_block_event]).await;

            let stores = engine.get_stores();
            let txn = RocksDbTransactionBatch::new();
            assert!(
                get_gasless_key_record(&stores.db, &txn, FID_FOR_TEST, &envelope_pubkey)
                    .unwrap()
                    .is_none()
            );
            assert!(
                get_gasless_key_owner_fid(&stores.db, &txn, &envelope_pubkey)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                get_last_used_at(&stores.db, &txn, FID_FOR_TEST, &envelope_pubkey).unwrap(),
                None,
            );
        }

        // -- downstream message e2e (cast signed by gasless key after replay) -------------

        #[tokio::test]
        async fn test_cast_signed_by_gasless_key_validates_after_replay() {
            let (mut engine, _temp_dir) = test_helper::new_engine().await;
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let gasless = generate_signer();

            register_eth(&mut engine, FID_FOR_TEST, &fid_custody, generate_signer()).await;
            register_eth(&mut engine, REQUEST_FID, &app_custody, generate_signer()).await;

            let timestamp = messages_factory::farcaster_time();
            let key_add = build_key_add(
                &fid_custody,
                &app_custody,
                &gasless,
                vec![proto::MessageType::CastAdd],
                3600,
                1,
                timestamp,
            );
            commit_block_events(&mut engine, vec![&create_merge_message_event(key_add, 1)]).await;

            // Cast signed by the gasless key validates and merges. Active-key lookup must
            // surface the gasless record (NEYN-10580 wired this).
            let cast = messages_factory::casts::create_cast_add(
                FID_FOR_TEST,
                "hello from gasless",
                Some(timestamp + 2),
                Some(&gasless),
            );
            commit_message(&mut engine, &cast).await;
            assert!(message_exists_in_trie(&mut engine, &cast));

            // Sanity check: active-key resolution returns Gasless, not OnChain.
            let txn = RocksDbTransactionBatch::new();
            let stores = engine.get_stores();
            let pubkey = gasless.verifying_key().to_bytes();
            let active = get_active_key(
                &stores.onchain_event_store,
                &stores.db,
                &txn,
                FID_FOR_TEST,
                &pubkey,
            )
            .unwrap()
            .expect("active key must resolve");
            assert!(matches!(active, ActiveKey::Gasless { .. }));
        }

        #[tokio::test]
        async fn test_cast_with_unscoped_message_type_rejected() {
            let (mut engine, _temp_dir) = test_helper::new_engine().await;
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let gasless = generate_signer();

            register_eth(&mut engine, FID_FOR_TEST, &fid_custody, generate_signer()).await;
            register_eth(&mut engine, REQUEST_FID, &app_custody, generate_signer()).await;

            let timestamp = messages_factory::farcaster_time();
            // Scope KEY_ADD to CastAdd only.
            let key_add = build_key_add(
                &fid_custody,
                &app_custody,
                &gasless,
                vec![proto::MessageType::CastAdd],
                3600,
                1,
                timestamp,
            );
            commit_block_events(&mut engine, vec![&create_merge_message_event(key_add, 1)]).await;

            // Reaction signed by the same gasless key is out-of-scope → admit check rejects.
            let reaction = messages_factory::reactions::create_reaction_add(
                FID_FOR_TEST,
                ReactionType::Like,
                Target::TargetCastId(CastId {
                    fid: FID2_FOR_TEST,
                    hash: vec![1; 20],
                }),
                Some(timestamp + 2),
                Some(&gasless),
            );
            let state_change = engine.propose_state_change(
                engine.shard_id(),
                vec![MempoolMessage::UserMessage(reaction.clone())],
                None,
            );
            // The reaction must not produce a successful merge.
            assert!(!message_exists_in_trie_after(&mut engine, &state_change, &reaction).await);
        }

        #[tokio::test]
        async fn test_cast_after_key_remove_rejected() {
            let (mut engine, _temp_dir) = test_helper::new_engine().await;
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let gasless = generate_signer();
            let pubkey: [u8; 32] = gasless.verifying_key().to_bytes();

            register_eth(&mut engine, FID_FOR_TEST, &fid_custody, generate_signer()).await;
            register_eth(&mut engine, REQUEST_FID, &app_custody, generate_signer()).await;

            let timestamp = messages_factory::farcaster_time();
            let key_add = build_key_add(
                &fid_custody,
                &app_custody,
                &gasless,
                vec![proto::MessageType::CastAdd],
                3600,
                1,
                timestamp,
            );
            commit_block_events(&mut engine, vec![&create_merge_message_event(key_add, 1)]).await;

            // First cast accepted while key is live.
            let cast1 = messages_factory::casts::create_cast_add(
                FID_FOR_TEST,
                "before revoke",
                Some(timestamp + 1),
                Some(&gasless),
            );
            commit_message(&mut engine, &cast1).await;
            assert!(message_exists_in_trie(&mut engine, &cast1));

            // KEY_REMOVE replays.
            let key_remove = messages_factory::keys::create_key_remove_custody(
                FID_FOR_TEST,
                &fid_custody,
                &test_helper::default_signer(),
                &pubkey,
                2,
                timestamp + 1_000_000,
                Some(timestamp + 2),
            );
            commit_block_events(
                &mut engine,
                vec![&create_merge_message_event(key_remove, 2)],
            )
            .await;

            // Subsequent cast signed by the (now-revoked) gasless key must be rejected.
            let cast2 = messages_factory::casts::create_cast_add(
                FID_FOR_TEST,
                "after revoke",
                Some(timestamp + 3),
                Some(&gasless),
            );
            let state_change = engine.propose_state_change(
                engine.shard_id(),
                vec![MempoolMessage::UserMessage(cast2.clone())],
                None,
            );
            assert!(!message_exists_in_trie_after(&mut engine, &state_change, &cast2).await);
        }

        #[tokio::test]
        async fn test_cast_after_ttl_expiry_rejected() {
            let (mut engine, _temp_dir) = test_helper::new_engine().await;
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let gasless = generate_signer();

            register_eth(&mut engine, FID_FOR_TEST, &fid_custody, generate_signer()).await;
            register_eth(&mut engine, REQUEST_FID, &app_custody, generate_signer()).await;

            // Anchor in the past so the "1000-seconds-later" expired-cast block timestamp
            // is still ≤ FarcasterTime::current() at commit time. ShardEngine commit metrics
            // panic on a future block timestamp (block_delay underflows).
            let timestamp = messages_factory::farcaster_time() - 2000;
            // Short TTL of 10 seconds — sliding window starts at message.timestamp.
            let key_add = build_key_add(
                &fid_custody,
                &app_custody,
                &gasless,
                vec![proto::MessageType::CastAdd],
                10,
                1,
                timestamp,
            );
            commit_block_events(&mut engine, vec![&create_merge_message_event(key_add, 1)]).await;

            // Cast at timestamp+5 (within TTL) — accepted, bumps last_used_at to now.
            let cast_ok = messages_factory::casts::create_cast_add(
                FID_FOR_TEST,
                "within ttl",
                Some(timestamp + 5),
                Some(&gasless),
            );
            commit_message_at(
                &mut engine,
                &cast_ok,
                &FarcasterTime::new((timestamp + 5) as u64),
            )
            .await;
            assert!(message_exists_in_trie(&mut engine, &cast_ok));

            // Cast well past last_used_at + ttl — sliding-TTL bump rejects.
            let cast_expired = messages_factory::casts::create_cast_add(
                FID_FOR_TEST,
                "past ttl",
                Some(timestamp + 1000),
                Some(&gasless),
            );
            let state_change = engine.propose_state_change(
                engine.shard_id(),
                vec![MempoolMessage::UserMessage(cast_expired.clone())],
                Some(FarcasterTime::new((timestamp + 1000) as u64)),
            );
            assert!(!message_exists_in_trie_after(&mut engine, &state_change, &cast_expired).await);
        }

        /// Helper: validate + commit an already-built state_change and report whether `msg`
        /// landed in the trie. Used by failure tests where we don't want a panic on rejection
        /// (which `commit_message` does).
        async fn message_exists_in_trie_after(
            engine: &mut ShardEngine,
            state_change: &ShardStateChange,
            msg: &proto::Message,
        ) -> bool {
            test_helper::validate_and_commit_state_change(engine, state_change).await;
            TrieKey::for_message(msg)
                .iter()
                .all(|key| engine.trie_key_exists(trie_ctx(), key))
        }
    }

    // ----------------------------------------------------------------------------------------
    // MergeOnChainEvent BlockEvent replica fold + ownership hints.
    //
    // Shard 0 fans channel-register events to every data shard as a MergeOnChainEvent BlockEvent;
    // `handle_block_event` runs the trie-free replica fold against the shard's own
    // OnchainEventStore and emits a ChannelOwnerChangeHint whenever a REGISTER/TRANSFER records a
    // new owner. These tests drive that arm through the real block-event commit path. The
    // pre-feature reject test is unchanged: the gate still ships WITH the type.
    // ----------------------------------------------------------------------------------------
    mod channel_ownership_events_block_event_tests {
        use super::*;
        use crate::storage::store::account::get_channel_keys_by_owner_address;
        use alloy_primitives::keccak256;

        const CHANNEL_KEY: &str = "pets";

        fn channel_label(channel_key: &str) -> Vec<u8> {
            keccak256(channel_key.as_bytes()).to_vec()
        }

        fn channel_event(
            channel_key: &str,
            label_source: &str,
            owner_byte: u8,
            event_type: proto::ChannelRegisterEventType,
            expiry: u64,
            block_number: u32,
        ) -> OnChainEvent {
            events_factory::create_channel_register_event(
                channel_key,
                channel_label(label_source),
                vec![owner_byte; 20],
                expiry,
                event_type,
                block_number,
                0,
            )
        }

        fn channel_register_onchain_event() -> OnChainEvent {
            channel_event(
                CHANNEL_KEY,
                CHANNEL_KEY,
                0xCC,
                proto::ChannelRegisterEventType::Register,
                1_900_000_000,
                100,
            )
        }

        // Commits one MergeOnChainEvent BlockEvent (seqnum-chained) through the data-shard path.
        async fn apply_channel_block_event(
            engine: &mut ShardEngine,
            onchain_event: OnChainEvent,
            seqnum: u64,
        ) {
            let block_event =
                events_factory::create_merge_on_chain_event_event(onchain_event, seqnum);
            commit_block_events(engine, vec![&block_event]).await;
        }

        fn owner_change_hints(engine: &ShardEngine) -> Vec<HubEvent> {
            HubEvent::get_events(engine.db.clone(), 0, None, None)
                .unwrap()
                .events
                .into_iter()
                .filter(|event| event.r#type == HubEventType::ChannelOwnerChangeHint as i32)
                .collect()
        }

        fn assert_hint(
            hint: &HubEvent,
            channel_key: &str,
            owner_address: &[u8],
            cause: proto::ChannelOwnerChangeCause,
        ) {
            let body = match hint.body.as_ref().unwrap() {
                proto::hub_event::Body::ChannelOwnerChangeHintBody(body) => body,
                other => panic!("expected ChannelOwnerChangeHintBody, got {:?}", other),
            };
            assert_eq!(body.channel_key, channel_key);
            assert_eq!(body.owner_address, owner_address);
            assert_eq!(body.cause, cause as i32);
            // A subscriber-visible event id, assigned by the shared HubEventIdGenerator.
            assert!(hint.id > 0, "hint must carry a normal (nonzero) event id");
        }

        fn owner_channels(engine: &ShardEngine, owner_byte: u8) -> Vec<String> {
            get_channel_keys_by_owner_address(
                &engine.get_stores().onchain_event_store.db,
                &vec![owner_byte; 20],
                &PageOptions::default(),
            )
            .unwrap()
            .0
        }

        #[tokio::test]
        async fn test_active_merge_on_chain_event_block_event_folds_replica_and_hints() {
            // Devnet runs V20, so the arm folds the fanned-out REGISTER into this shard's own
            // replica: it materializes all three secondary indexes (ByChannelKey,
            // ChannelKeyByLabel, ByOwnerAddress), emits exactly one REGISTER hint, and — being a
            // secondary-index-only fold — never touches the trie.
            let (mut engine, _temp_dir) = test_helper::new_engine().await;
            assert!(EngineVersion::latest().is_enabled(ProtocolFeature::ChannelOwnershipEvents));

            let root_before = engine.trie_root_hash();
            apply_channel_block_event(&mut engine, channel_register_onchain_event(), 1).await;

            let stores = engine.get_stores();
            // ByChannelKey.
            let owner = stores
                .onchain_event_store
                .get_channel_owner(CHANNEL_KEY, None)
                .unwrap()
                .unwrap();
            assert_eq!(owner.channel_key, CHANNEL_KEY);
            assert_eq!(owner.owner_address, vec![0xCC; 20]);
            // ChannelKeyByLabel.
            assert_eq!(
                stores
                    .onchain_event_store
                    .get_channel_key_by_label(&channel_label(CHANNEL_KEY))
                    .unwrap(),
                Some(CHANNEL_KEY.to_string())
            );
            // ByOwnerAddress.
            assert_eq!(owner_channels(&engine, 0xCC), vec![CHANNEL_KEY.to_string()]);

            // Exactly one REGISTER hint carrying the recorded owner.
            let hints = owner_change_hints(&engine);
            assert_eq!(hints.len(), 1);
            assert_hint(
                &hints[0],
                CHANNEL_KEY,
                &vec![0xCC; 20],
                proto::ChannelOwnerChangeCause::Register,
            );

            // Trie-free: the replica lives entirely in RocksDB secondary indexes.
            assert_eq!(
                to_hex(&root_before),
                to_hex(&engine.trie_root_hash()),
                "replica fold must not touch the trie"
            );
        }

        #[tokio::test]
        async fn test_renew_updates_expiry_and_emits_no_hint() {
            // RENEW extends expiry without changing ownership — the index is updated but no hint
            // fires (the cause enum has no RENEW variant by design).
            let (mut engine, _temp_dir) = test_helper::new_engine().await;
            apply_channel_block_event(&mut engine, channel_register_onchain_event(), 1).await;

            let renew = channel_event(
                CHANNEL_KEY,
                CHANNEL_KEY,
                0xCC,
                proto::ChannelRegisterEventType::Renew,
                2_000_000_000,
                101,
            );
            apply_channel_block_event(&mut engine, renew, 2).await;

            let owner = engine
                .get_stores()
                .onchain_event_store
                .get_channel_owner(CHANNEL_KEY, None)
                .unwrap()
                .unwrap();
            assert_eq!(owner.expiry, 2_000_000_000, "renew updates expiry");
            // Only the register's hint — renew adds none.
            assert_eq!(owner_change_hints(&engine).len(), 1);
        }

        #[tokio::test]
        async fn test_transfer_moves_owner_and_hints_new_owner() {
            // TRANSFER rebinds the channel to a new owner (resolved via ChannelKeyByLabel), moves
            // the ByOwnerAddress index, and emits a TRANSFER hint carrying the NEW owner.
            let (mut engine, _temp_dir) = test_helper::new_engine().await;
            let register = channel_event(
                CHANNEL_KEY,
                CHANNEL_KEY,
                0xAA,
                proto::ChannelRegisterEventType::Register,
                1_900_000_000,
                100,
            );
            apply_channel_block_event(&mut engine, register, 1).await;

            // A transfer carries the label (not the channel_key) and the new owner.
            let transfer = channel_event(
                "",
                CHANNEL_KEY,
                0xBB,
                proto::ChannelRegisterEventType::Transfer,
                0,
                101,
            );
            apply_channel_block_event(&mut engine, transfer, 2).await;

            let owner = engine
                .get_stores()
                .onchain_event_store
                .get_channel_owner(CHANNEL_KEY, None)
                .unwrap()
                .unwrap();
            assert_eq!(
                owner.owner_address,
                vec![0xBB; 20],
                "owner moved to new address"
            );
            // ByOwnerAddress moved: old owner empty, new owner holds the channel.
            assert!(owner_channels(&engine, 0xAA).is_empty());
            assert_eq!(owner_channels(&engine, 0xBB), vec![CHANNEL_KEY.to_string()]);

            let hints = owner_change_hints(&engine);
            assert_eq!(hints.len(), 2, "one REGISTER hint, one TRANSFER hint");
            assert_hint(
                &hints[1],
                CHANNEL_KEY,
                &vec![0xBB; 20],
                proto::ChannelOwnerChangeCause::Transfer,
            );
        }

        #[tokio::test]
        async fn test_lww_older_event_is_skipped_and_emits_no_hint() {
            // A later-arriving but chain-older REGISTER loses LWW: no write, no hint.
            let (mut engine, _temp_dir) = test_helper::new_engine().await;
            let register = channel_event(
                CHANNEL_KEY,
                CHANNEL_KEY,
                0xAA,
                proto::ChannelRegisterEventType::Register,
                1_900_000_000,
                100,
            );
            apply_channel_block_event(&mut engine, register, 1).await;

            let older = channel_event(
                CHANNEL_KEY,
                CHANNEL_KEY,
                0xDD,
                proto::ChannelRegisterEventType::Register,
                1_900_000_000,
                50, // earlier block → older chain position
            );
            apply_channel_block_event(&mut engine, older, 2).await;

            let owner = engine
                .get_stores()
                .onchain_event_store
                .get_channel_owner(CHANNEL_KEY, None)
                .unwrap()
                .unwrap();
            assert_eq!(
                owner.owner_address,
                vec![0xAA; 20],
                "older event must not overwrite"
            );
            assert_eq!(
                owner_change_hints(&engine).len(),
                1,
                "no hint for the skipped event"
            );
        }

        #[tokio::test]
        async fn test_transfer_with_unknown_label_skips_and_emits_no_hint() {
            // A transfer whose label resolves to no registered channel is warned and skipped —
            // no owner index, no hint.
            let (mut engine, _temp_dir) = test_helper::new_engine().await;
            let transfer = channel_event(
                "",
                "ghost",
                0xBB,
                proto::ChannelRegisterEventType::Transfer,
                0,
                100,
            );
            apply_channel_block_event(&mut engine, transfer, 1).await;

            assert!(engine
                .get_stores()
                .onchain_event_store
                .get_channel_owner("ghost", None)
                .unwrap()
                .is_none());
            assert!(owner_change_hints(&engine).is_empty());
        }

        #[tokio::test]
        async fn test_double_application_is_idempotent_and_still_hints() {
            // On the route_fid(0) shard a channel event is merged first as a system message
            // (primary + fold, no hint) and THEN re-applied as a block event. The block-event
            // replica fold re-runs at the same chain position: strict-`<` LWW rewrites
            // byte-identical index values, and the hint still fires — pinning "REGISTER/TRANSFER
            // hints on every shard's stream" even where the event was already merged.
            let (mut engine, _temp_dir) = test_helper::new_engine().await;
            let register = channel_register_onchain_event();

            // System-message path: materializes the index, emits a MergeOnChainEvent (no hint).
            commit_event(&mut engine, &register).await;
            let owner_after_system = engine
                .get_stores()
                .onchain_event_store
                .get_channel_owner(CHANNEL_KEY, None)
                .unwrap()
                .unwrap();
            assert!(
                owner_change_hints(&engine).is_empty(),
                "system merge emits no hint"
            );

            // Block-event path on the SAME shard: no block events exist yet, so seqnum 1.
            apply_channel_block_event(&mut engine, register, 1).await;

            let owner_after_block = engine
                .get_stores()
                .onchain_event_store
                .get_channel_owner(CHANNEL_KEY, None)
                .unwrap()
                .unwrap();
            assert_eq!(
                owner_after_system, owner_after_block,
                "re-application at equal chain position is byte-identical"
            );
            assert_eq!(owner_channels(&engine, 0xCC), vec![CHANNEL_KEY.to_string()]);
            // The block-event path emits the hint even though the event was already merged.
            let hints = owner_change_hints(&engine);
            assert_eq!(hints.len(), 1);
            assert_hint(
                &hints[0],
                CHANNEL_KEY,
                &vec![0xCC; 20],
                proto::ChannelOwnerChangeCause::Register,
            );
        }

        #[tokio::test]
        async fn test_route_fid_zero_burst_coalesces_to_latest_hint() {
            // Consumer-contract pin (hub_event.proto): on the route_fid(0) shard the
            // system-message path leads the fan-out, so a rapid same-channel burst can be
            // COALESCED down to just the latest hint on THIS shard — not every event hints
            // here, only the latest one.
            //
            // Sequence: the system-message path merges REGISTER@100 then TRANSFER@101 first,
            // advancing this shard's owner index to owner 0xBB @ block 101. The fanned-out
            // block events then arrive in chain order:
            //   - REGISTER@100 loses the strict-`<` LWW against the stored @101 → no write,
            //     NO hint (it hinted on every fan-out-only shard, which never saw the
            //     system-message lead — see test_transfer_moves_owner_and_hints_new_owner,
            //     where the same REGISTER+TRANSFER via block events alone yields TWO hints).
            //   - TRANSFER@101 re-applies at the equal chain position → byte-identical write,
            //     Some(change) → the one TRANSFER hint this shard emits.
            // The final owner state is identical to a fan-out-only shard; only the hint
            // stream is coalesced. Deterministic per chain history.
            let (mut engine, _temp_dir) = test_helper::new_engine().await;
            assert!(EngineVersion::latest().is_enabled(ProtocolFeature::ChannelOwnershipEvents));

            let register = channel_event(
                CHANNEL_KEY,
                CHANNEL_KEY,
                0xAA,
                proto::ChannelRegisterEventType::Register,
                1_900_000_000,
                100,
            );
            // A transfer carries the label (not the channel_key) and the new owner.
            let transfer = channel_event(
                "",
                CHANNEL_KEY,
                0xBB,
                proto::ChannelRegisterEventType::Transfer,
                0,
                101,
            );

            // System-message path leads: it merges both events (index → 0xBB @ 101) and
            // emits no hint.
            commit_event(&mut engine, &register).await;
            commit_event(&mut engine, &transfer).await;
            assert!(
                owner_change_hints(&engine).is_empty(),
                "system-message merges emit no hint"
            );

            // Fanned-out REGISTER@100 arrives after the index already advanced to @101: it
            // loses LWW and is coalesced away — no write, no hint.
            apply_channel_block_event(&mut engine, register, 1).await;
            let owner = engine
                .get_stores()
                .onchain_event_store
                .get_channel_owner(CHANNEL_KEY, None)
                .unwrap()
                .unwrap();
            assert_eq!(
                owner.owner_address,
                vec![0xBB; 20],
                "chain-older REGISTER must not overwrite the newer transfer"
            );
            assert!(
                owner_change_hints(&engine).is_empty(),
                "the coalesced (LWW-losing) REGISTER emits no hint on this shard"
            );

            // Fanned-out TRANSFER@101 re-applies at the equal position and emits the one hint.
            apply_channel_block_event(&mut engine, transfer, 2).await;
            let hints = owner_change_hints(&engine);
            assert_eq!(
                hints.len(),
                1,
                "only the latest event in the burst hints on route_fid(0)"
            );
            assert_hint(
                &hints[0],
                CHANNEL_KEY,
                &vec![0xBB; 20],
                proto::ChannelOwnerChangeCause::Transfer,
            );
            assert_eq!(owner_channels(&engine, 0xBB), vec![CHANNEL_KEY.to_string()]);
        }

        #[tokio::test]
        async fn test_pre_feature_merge_on_chain_event_block_event_is_rejected() {
            // On Mainnet the block's timestamp (0) resolves to a pre-V20 version, so
            // ChannelOwnershipEvents is inactive and the arm returns `InvalidMessageType`. The
            // caller warns and swallows the error, so the block still commits — but no state is
            // produced: the merge is short-circuited before any fold or trie write.
            let (mut engine, _temp_dir) = test_helper::new_engine_with_options(EngineOptions {
                network: Some(FarcasterNetwork::Mainnet),
                ..Default::default()
            })
            .await;

            let root_before = engine.trie_root_hash();
            let block_event = events_factory::create_merge_on_chain_event_event(
                channel_register_onchain_event(),
                1,
            );
            commit_block_events(&mut engine, vec![&block_event]).await;

            assert!(block_event_exists(&engine, &block_event));
            assert_eq!(
                to_hex(&root_before),
                to_hex(&engine.trie_root_hash()),
                "rejected MergeOnChainEvent replay must not touch the trie"
            );
            let owner = engine
                .get_stores()
                .onchain_event_store
                .get_channel_owner(CHANNEL_KEY, None)
                .unwrap();
            assert!(
                owner.is_none(),
                "pre-feature replay must not fold any owner index"
            );
        }
    }

    // ----------------------------------------------------------------------------------------
    // Verification-merge channel-owner hints.
    //
    // When an Ethereum verification is force-replayed from shard 0, the engine scans THIS shard's
    // own ByOwnerAddress replica (built by the channel-register block-event fold) and emits one
    // ChannelOwnerChangeHint per channel the verified address owns, cause VERIFICATION_ADD /
    // VERIFICATION_REMOVE. The hook is STRUCTURALLY unable to fail, alter, or panic the merge:
    // every error warns and yields fewer hints, and the merge result + trie are untouched.
    //
    // Most tests drive the real BlockEvent replay + forced merge + trie + hint sequence. Two edges
    // — the pre-V20 gate and the Solana-protocol skip — call the emitter directly with an explicit
    // version because no running network can construct those states through replay.
    // ----------------------------------------------------------------------------------------
    mod channel_ownership_events_verification_hint_tests {
        use super::*;
        use crate::storage::constants::{OnChainEventPostfix, RootPrefix};
        use alloy_primitives::keccak256;

        // Canonical EOA verification fixture: a valid claim signature for FID3_FOR_TEST over this
        // address on devnet (reused from `test_commit_verification_messages`). The channels these
        // tests register are OWNED by exactly this address, so the merged verification's replica
        // scan finds them.
        const VERIFIED_ADDRESS_HEX: &str = "91031dcfdea024b4d51e775486111d2b2a715871";
        const CLAIM_SIGNATURE_HEX: &str = "b72c63d61f075b36fb66a9a867b50836cef19d653a3c09005628738677bcb25f25b6b6e6d2e1d69cd725327b3c020deef9e2575a22dc8ed08f88bc75718ce1cb1c";
        const BLOCK_HASH_HEX: &str =
            "d74860c4bbf574d5ad60f03a478a30f990e05ac723e138a5c860cdb3095f4296";

        fn verified_address() -> Vec<u8> {
            hex::decode(VERIFIED_ADDRESS_HEX).unwrap()
        }

        // A fresh devnet engine (V20 active) with FID3_FOR_TEST registered so the fixture
        // verification can merge.
        async fn new_verifier_engine() -> (ShardEngine, tempfile::TempDir) {
            let (mut engine, tmpdir) = test_helper::new_engine().await;
            assert!(EngineVersion::latest().is_enabled(ProtocolFeature::ChannelOwnershipEvents));
            test_helper::register_user(
                FID3_FOR_TEST,
                test_helper::default_signer(),
                test_helper::default_custody_address(),
                &mut engine,
            )
            .await;
            (engine, tmpdir)
        }

        fn verification_add(timestamp: u32) -> proto::Message {
            messages_factory::verifications::create_verification_add(
                FID3_FOR_TEST,
                0,
                verified_address(),
                hex::decode(CLAIM_SIGNATURE_HEX).unwrap(),
                hex::decode(BLOCK_HASH_HEX).unwrap(),
                Some(timestamp),
                None,
            )
        }

        fn verification_remove(timestamp: u32) -> proto::Message {
            messages_factory::verifications::create_verification_remove(
                FID3_FOR_TEST,
                verified_address(),
                Some(timestamp),
                None,
            )
        }

        async fn replay_message(engine: &mut ShardEngine, message: &proto::Message) {
            let block_event =
                events_factory::create_merge_message_event(message.clone(), next_seqnum(engine));
            test_helper::commit_block_events(engine, vec![&block_event]).await;
        }

        async fn commit_verification(engine: &mut ShardEngine, message: &proto::Message) {
            replay_message(engine, message).await;
            assert!(test_helper::message_exists_in_trie(engine, message));
        }

        // The next fan-out block-event seqnum for THIS engine. Block events are silently skipped
        // unless their seqnum is exactly max+1; querying it keeps the tests robust to any other
        // block events (they never advance on this single-shard test path today, but we don't
        // assume that).
        fn next_seqnum(engine: &ShardEngine) -> u64 {
            engine
                .get_stores()
                .block_event_store
                .max_seqnum()
                .unwrap_or(0)
                + 1
        }

        async fn apply_channel_event(engine: &mut ShardEngine, event: OnChainEvent) {
            let seqnum = next_seqnum(engine);
            let block_event = events_factory::create_merge_on_chain_event_event(event, seqnum);
            test_helper::commit_block_events(engine, vec![&block_event]).await;
        }

        // Register `channel_key` to `owner` via the channel-register block-event fold path (populates
        // the shard's own ByOwnerAddress replica). REGISTER hint fires as a side effect.
        async fn register_channel(engine: &mut ShardEngine, channel_key: &str, owner: Vec<u8>) {
            let event = events_factory::create_channel_register_event(
                channel_key,
                keccak256(channel_key.as_bytes()).to_vec(),
                owner,
                1_900_000_000,
                proto::ChannelRegisterEventType::Register,
                (next_seqnum(engine) as u32) + 1000, // block_number: strictly increasing, arbitrary
                0,
            );
            apply_channel_event(engine, event).await;
        }

        // Transfer the channel labeled by `label_source` to `new_owner`. A transfer carries the
        // label (not the channel_key) and the new owner.
        async fn transfer_channel(
            engine: &mut ShardEngine,
            label_source: &str,
            new_owner: Vec<u8>,
            block_number: u32,
        ) {
            let event = events_factory::create_channel_register_event(
                "",
                keccak256(label_source.as_bytes()).to_vec(),
                new_owner,
                0,
                proto::ChannelRegisterEventType::Transfer,
                block_number,
                0,
            );
            apply_channel_event(engine, event).await;
        }

        fn channel_owner_address(engine: &ShardEngine, channel_key: &str) -> Option<Vec<u8>> {
            engine
                .get_stores()
                .onchain_event_store
                .get_channel_owner(channel_key, None)
                .unwrap()
                .map(|owner| owner.owner_address)
        }

        fn owner_change_hints(engine: &ShardEngine) -> Vec<HubEvent> {
            HubEvent::get_events(engine.db.clone(), 0, None, None)
                .unwrap()
                .events
                .into_iter()
                .filter(|event| event.r#type == HubEventType::ChannelOwnerChangeHint as i32)
                .collect()
        }

        // Decode a hint's body. Every event reaching this is already filtered to
        // `type == ChannelOwnerChangeHint` (by `owner_change_hints`), so the body variant is
        // guaranteed; a mismatch is a test-harness bug and panics loudly.
        fn hint_body(hint: &HubEvent) -> &proto::ChannelOwnerChangeHintBody {
            match hint.body.as_ref().unwrap() {
                proto::hub_event::Body::ChannelOwnerChangeHintBody(body) => body,
                other => panic!("expected ChannelOwnerChangeHintBody, got {:?}", other),
            }
        }

        // Only VERIFICATION_* hints, in emission (id) order — filters out the REGISTER/TRANSFER
        // hints that the block-event fold emits while seeding the replica.
        fn verification_hints(engine: &ShardEngine) -> Vec<HubEvent> {
            owner_change_hints(engine)
                .into_iter()
                .filter(|hint| {
                    let cause = hint_body(hint).cause;
                    cause == proto::ChannelOwnerChangeCause::VerificationAdd as i32
                        || cause == proto::ChannelOwnerChangeCause::VerificationRemove as i32
                })
                .collect()
        }

        fn assert_hint(
            hint: &HubEvent,
            channel_key: &str,
            owner_address: &[u8],
            cause: proto::ChannelOwnerChangeCause,
        ) {
            let body = hint_body(hint);
            assert_eq!(body.channel_key, channel_key);
            assert_eq!(body.owner_address, owner_address);
            assert_eq!(body.cause, cause as i32);
            assert!(hint.id > 0, "hint must carry a normal (nonzero) event id");
        }

        #[tokio::test]
        async fn test_verification_add_for_channel_owner_emits_one_hint() {
            let (mut engine, _tmp) = new_verifier_engine().await;
            register_channel(&mut engine, "pets", verified_address()).await;

            let ts = messages_factory::farcaster_time();
            commit_verification(&mut engine, &verification_add(ts)).await;

            let hints = verification_hints(&engine);
            assert_eq!(
                hints.len(),
                1,
                "one VERIFICATION_ADD hint for the owned channel"
            );
            assert_hint(
                &hints[0],
                "pets",
                &verified_address(),
                proto::ChannelOwnerChangeCause::VerificationAdd,
            );
        }

        #[tokio::test]
        async fn test_verification_add_for_non_owner_emits_no_hint() {
            let (mut engine, _tmp) = new_verifier_engine().await;
            // A channel exists, but it's owned by a DIFFERENT address — the verified address owns
            // nothing, so the scan is empty.
            register_channel(&mut engine, "pets", vec![0xAB; 20]).await;

            let ts = messages_factory::farcaster_time();
            commit_verification(&mut engine, &verification_add(ts)).await;

            assert!(
                verification_hints(&engine).is_empty(),
                "no hint when the verified address owns no channels"
            );
        }

        #[tokio::test]
        async fn test_normal_replay_type_emits_no_channel_owner_hint() {
            let (mut engine, _tmp) = new_verifier_engine().await;
            register_channel(&mut engine, "pets", verified_address()).await;
            let hints_before = owner_change_hints(&engine).len();

            let lend = messages_factory::storage_lend::create_storage_lend(
                FID3_FOR_TEST,
                FID_FOR_TEST,
                1,
                crate::proto::StorageUnitType::UnitType2025,
                Some(messages_factory::farcaster_time()),
                None,
            );
            replay_message(&mut engine, &lend).await;

            assert_eq!(
                owner_change_hints(&engine).len(),
                hints_before,
                "a successful Normal replay must not emit a channel-owner hint"
            );
        }

        #[tokio::test]
        async fn test_verification_add_emits_hints_in_ascending_channel_key_order() {
            let (mut engine, _tmp) = new_verifier_engine().await;
            // Register three channels owned by the verified address in NON-sorted order; the
            // replica scan is by ascending key, so hints must come out apple, banana, cherry.
            register_channel(&mut engine, "banana", verified_address()).await;
            register_channel(&mut engine, "cherry", verified_address()).await;
            register_channel(&mut engine, "apple", verified_address()).await;

            let ts = messages_factory::farcaster_time();
            commit_verification(&mut engine, &verification_add(ts)).await;

            let hints = verification_hints(&engine);
            assert_eq!(hints.len(), 3, "one hint per owned channel");
            for (hint, channel_key) in hints.iter().zip(["apple", "banana", "cherry"]) {
                assert_hint(
                    hint,
                    channel_key,
                    &verified_address(),
                    proto::ChannelOwnerChangeCause::VerificationAdd,
                );
            }
        }

        #[tokio::test]
        async fn test_verification_remove_emits_hint_with_remove_cause() {
            let (mut engine, _tmp) = new_verifier_engine().await;
            register_channel(&mut engine, "pets", verified_address()).await;

            let ts = messages_factory::farcaster_time();
            // Add first (so the remove has something to remove and merges), then remove.
            commit_verification(&mut engine, &verification_add(ts)).await;
            commit_verification(&mut engine, &verification_remove(ts + 1)).await;

            let hints = verification_hints(&engine);
            assert_eq!(hints.len(), 2, "one ADD hint, then one REMOVE hint");
            assert_hint(
                &hints[0],
                "pets",
                &verified_address(),
                proto::ChannelOwnerChangeCause::VerificationAdd,
            );
            assert_hint(
                &hints[1],
                "pets",
                &verified_address(),
                proto::ChannelOwnerChangeCause::VerificationRemove,
            );
        }

        #[tokio::test]
        async fn test_failed_forced_verification_replay_emits_no_hint() {
            // The emitter runs only after `merge_replayed_verification` succeeds. A malformed
            // verification that dispatches to the forced arm but fails the store merge must not
            // emit a hint even though its address owns a channel.
            let (mut engine, _tmp) = new_verifier_engine().await;
            register_channel(&mut engine, "pets", verified_address()).await;

            let ts = messages_factory::farcaster_time();
            let mut malformed = verification_add(ts);
            malformed.data.as_mut().unwrap().body = Some(
                proto::message_data::Body::VerificationRemoveBody(proto::VerificationRemoveBody {
                    address: verified_address(),
                    protocol: proto::Protocol::Ethereum as i32,
                }),
            );
            replay_message(&mut engine, &malformed).await;

            assert!(
                verification_hints(&engine).is_empty(),
                "a failed forced replay must not emit a verification hint"
            );
            assert!(!test_helper::message_exists_in_trie(
                &mut engine,
                &malformed
            ));
        }

        #[tokio::test]
        async fn test_verification_merge_survives_corrupt_replica() {
            // THE structural-safety pin. Corrupt the shard's ByOwnerAddress replica so the scan's
            // UTF-8 decode fails, then merge a real Ethereum verification for that address. The
            // merge MUST still succeed and be trie-indexed; the hook warns and emits no hint. The
            // merge is provably unaffected by replica corruption.
            let (mut engine, _tmp) = new_verifier_engine().await;
            register_channel(&mut engine, "pets", verified_address()).await;

            // Write garbage bytes under a ByOwnerAddress key for the verified address: the
            // channel_key suffix is invalid UTF-8, so `get_channel_keys_by_owner_address`'s
            // `String::from_utf8` errors and the whole scan returns Err.
            let mut corrupt_key = vec![
                RootPrefix::OnChainEvent as u8,
                OnChainEventPostfix::ChannelRegisterByOwnerAddress as u8,
            ];
            corrupt_key.extend_from_slice(&verified_address());
            corrupt_key.extend_from_slice(&[0xFF, 0xFE]); // invalid UTF-8 channel_key
            let mut txn = RocksDbTransactionBatch::new();
            txn.put(corrupt_key, vec![1u8]);
            engine
                .get_stores()
                .onchain_event_store
                .db
                .commit(txn)
                .unwrap();

            let ts = messages_factory::farcaster_time();
            let add = verification_add(ts);
            commit_verification(&mut engine, &add).await;

            // The verification merged and is trie-indexed — untouched by the replica corruption.
            assert!(
                test_helper::message_exists_in_trie(&mut engine, &add),
                "the verification must merge and be trie-indexed despite replica corruption"
            );
            assert_eq!(
                1,
                engine
                    .get_verifications_by_fid(FID3_FOR_TEST)
                    .unwrap()
                    .messages
                    .len(),
                "the verification is present in the store"
            );
            // The corrupt scan errored out, so no hint (or partial) — never a merge failure.
            assert!(
                verification_hints(&engine).is_empty(),
                "corrupt replica yields no hint, not a failed merge"
            );
        }

        #[tokio::test]
        async fn test_hints_do_not_touch_the_trie() {
            // Trie discipline: emitting AND persisting a verification hint mutates zero trie
            // state. The hook is invoked directly on a populated replica and the shard root is
            // captured before and after — it must be byte-identical. Hints flow to the event
            // store only; the hook never calls `update_trie`, and `for_hub_event` pins
            // `ChannelOwnerChangeHintBody` to zero trie keys, so a hint contributes nothing to the
            // root. (A two-engine before/after of the full merge is confounded by the random
            // transaction_hash the onchain-event factory stamps on register_user's trie-indexed
            // events, so this isolates the hook itself, deterministically.)
            let (mut engine, _tmp) = new_verifier_engine().await;
            register_channel(&mut engine, "pets", verified_address()).await;

            let root_before = engine.trie_root_hash();

            let add = verification_add(messages_factory::farcaster_time());
            let mut txn = RocksDbTransactionBatch::new();
            let hints = engine.emit_channel_owner_hints_for_verification(
                &add,
                &mut txn,
                EngineVersion::V20,
            );
            assert_eq!(hints.len(), 1, "the hook emits the hint");
            // Persist the hint's writes: only the event store is touched, never the trie.
            engine
                .get_stores()
                .onchain_event_store
                .db
                .commit(txn)
                .unwrap();

            assert_eq!(
                to_hex(&root_before),
                to_hex(&engine.trie_root_hash()),
                "emitting and persisting a hint must not touch the shard root"
            );
        }

        #[tokio::test]
        async fn test_pre_v20_gate_suppresses_hints_on_a_populated_replica() {
            // The pre-V20 gate on the exact state no running network exhibits: a populated
            // replica with the feature off. Built on devnet (so the replica exists), then the
            // hook is called directly with a pre-V20 vs V20 version — the ONLY difference between
            // "no hints" and "the hint" is the version, proving the gate is the discriminator.
            let (mut engine, _tmp) = new_verifier_engine().await;
            register_channel(&mut engine, "pets", verified_address()).await;
            let add = verification_add(messages_factory::farcaster_time());

            let mut txn_pre = RocksDbTransactionBatch::new();
            let pre_v20 = engine.emit_channel_owner_hints_for_verification(
                &add,
                &mut txn_pre,
                EngineVersion::V19,
            );
            assert!(
                pre_v20.is_empty(),
                "pre-V20 (feature off) emits no hint even on a populated replica"
            );

            let mut txn_v20 = RocksDbTransactionBatch::new();
            let v20 = engine.emit_channel_owner_hints_for_verification(
                &add,
                &mut txn_v20,
                EngineVersion::V20,
            );
            assert_eq!(v20.len(), 1, "the same inputs at V20 emit the hint");
            assert_hint(
                &v20[0],
                "pets",
                &verified_address(),
                proto::ChannelOwnerChangeCause::VerificationAdd,
            );
        }

        #[tokio::test]
        async fn test_solana_verification_emits_no_hint() {
            // Solana verifications never own channels: the replica holds validated 20-byte EVM
            // addresses only, so the hook gates on protocol == Ethereum. A Solana verification can
            // not merge through the normal path in a unit test (no ed25519 fixture), so we pin the
            // protocol gate directly: the same verified address owns a channel, but flipping the
            // body's protocol to Solana suppresses the hint that the Ethereum path would emit.
            let (mut engine, _tmp) = new_verifier_engine().await;
            register_channel(&mut engine, "pets", verified_address()).await;

            let mut solana_add = verification_add(messages_factory::farcaster_time());
            if let Some(proto::message_data::Body::VerificationAddAddressBody(body)) =
                solana_add.data.as_mut().and_then(|data| data.body.as_mut())
            {
                body.protocol = proto::Protocol::Solana as i32;
            }

            let mut txn = RocksDbTransactionBatch::new();
            let hints = engine.emit_channel_owner_hints_for_verification(
                &solana_add,
                &mut txn,
                EngineVersion::V20,
            );
            assert!(
                hints.is_empty(),
                "a Solana-protocol verification takes no hint even when the address owns a channel"
            );
        }

        #[tokio::test]
        async fn test_malformed_verification_body_skips_without_panic() {
            // Adversarial-input pin (plan risk 1): the message body is untrusted network input.
            // A message whose type passes the filter but whose body is missing or the WRONG
            // variant — plus one with no `data` at all — must hit the warn-and-skip path: no
            // hint, and crucially no panic (no unwrap on body/message data). The replica is
            // populated for this address, so a well-formed body WOULD emit; the empties below are
            // the malformed body's doing, not an empty replica.
            let (mut engine, _tmp) = new_verifier_engine().await;
            register_channel(&mut engine, "pets", verified_address()).await;

            // Sanity: a well-formed add for this owner emits one hint (so "empty" below is meaningful).
            let mut txn = RocksDbTransactionBatch::new();
            assert_eq!(
                engine
                    .emit_channel_owner_hints_for_verification(
                        &verification_add(messages_factory::farcaster_time()),
                        &mut txn,
                        EngineVersion::V20,
                    )
                    .len(),
                1,
                "sanity: a well-formed add for this owner emits one hint"
            );

            // 1. type == VerificationAddEthAddress but body = None.
            let mut no_body = verification_add(messages_factory::farcaster_time());
            no_body.data.as_mut().unwrap().body = None;

            // 2. type == VerificationAddEthAddress but body is a DIFFERENT (Remove) variant.
            let mut wrong_variant = verification_add(messages_factory::farcaster_time());
            wrong_variant.data.as_mut().unwrap().body = Some(
                proto::message_data::Body::VerificationRemoveBody(proto::VerificationRemoveBody {
                    address: verified_address(),
                    protocol: proto::Protocol::Ethereum as i32,
                }),
            );

            // 3. No `data` at all — msg_type() falls back to None, so the hook takes no hint.
            let mut no_data = verification_add(messages_factory::farcaster_time());
            no_data.data = None;

            for (label, msg) in [
                ("body = None", &no_body),
                ("wrong body variant", &wrong_variant),
                ("data = None", &no_data),
            ] {
                let mut txn = RocksDbTransactionBatch::new();
                let hints = engine.emit_channel_owner_hints_for_verification(
                    msg,
                    &mut txn,
                    EngineVersion::V20,
                );
                assert!(
                    hints.is_empty(),
                    "malformed message ({label}) must emit no hint and not panic"
                );
            }
        }

        #[tokio::test]
        async fn test_hint_fan_out_is_capped() {
            // One verification for an address owning many channels must not emit an
            // unbounded number of hints — that would draw down the shared 16384/block
            // event-id budget. Drive the cap at a small value via the `_capped` seam:
            // 5 owned channels registered in non-sorted order, cap 3 → exactly the 3
            // LOWEST channel_keys, in ascending order (truncation is order-deterministic,
            // not insertion-ordered). Production uses the 256 constant.
            let (mut engine, _tmp) = new_verifier_engine().await;
            for key in ["e", "b", "d", "a", "c"] {
                register_channel(&mut engine, key, verified_address()).await;
            }

            let add = verification_add(messages_factory::farcaster_time());
            let mut txn = RocksDbTransactionBatch::new();
            let hints = engine.emit_channel_owner_hints_for_verification_capped(
                &add,
                &mut txn,
                EngineVersion::V20,
                3,
            );

            assert_eq!(hints.len(), 3, "fan-out truncated to the cap");
            for (hint, channel_key) in hints.iter().zip(["a", "b", "c"]) {
                assert_hint(
                    hint,
                    channel_key,
                    &verified_address(),
                    proto::ChannelOwnerChangeCause::VerificationAdd,
                );
            }
        }

        #[tokio::test]
        async fn test_production_wrapper_caps_fan_out_at_the_constant() {
            // The truncation tests above drive the `_capped` seam with a small max, so they
            // pass regardless of what the PUBLIC wrapper forwards — a regression that made the
            // wrapper pass `usize::MAX` (removing the very budget bound this feature adds) would
            // leave them green. This test guards the one production seam: register CAP+1 channels
            // to a single address, call the public wrapper (no explicit max), and assert it
            // truncates to exactly `MAX_CHANNEL_OWNER_HINTS_PER_VERIFICATION`. Uses the constant
            // (not a literal) so the expectation follows any future change to the cap.
            let cap = ShardEngine::MAX_CHANNEL_OWNER_HINTS_PER_VERIFICATION;
            let (mut engine, _tmp) = new_verifier_engine().await;
            for i in 0..(cap + 1) {
                // Zero-padded so ascending byte order is stable and distinct per channel.
                register_channel(&mut engine, &format!("chan_{:04}", i), verified_address()).await;
            }

            let add = verification_add(messages_factory::farcaster_time());
            let mut txn = RocksDbTransactionBatch::new();
            let hints = engine.emit_channel_owner_hints_for_verification(
                &add,
                &mut txn,
                EngineVersion::V20,
            );

            assert_eq!(
                hints.len(),
                cap,
                "public wrapper must truncate fan-out to MAX_CHANNEL_OWNER_HINTS_PER_VERIFICATION",
            );
        }

        #[tokio::test]
        async fn test_replayed_verification_hint_fan_out_is_capped_at_256() {
            let (mut engine, _tmp) = new_verifier_engine().await;
            let first_seqnum = next_seqnum(&engine);
            let block_events = (0u64..257)
                .map(|index| {
                    let channel_key = format!("channel-{index:03}");
                    let event = events_factory::create_channel_register_event(
                        &channel_key,
                        keccak256(channel_key.as_bytes()).to_vec(),
                        verified_address(),
                        1_900_000_000,
                        proto::ChannelRegisterEventType::Register,
                        (index as u32) + 3000,
                        0,
                    );
                    events_factory::create_merge_on_chain_event_event(event, first_seqnum + index)
                })
                .collect::<Vec<_>>();
            test_helper::commit_block_events(&mut engine, block_events.iter().collect()).await;

            commit_verification(
                &mut engine,
                &verification_add(messages_factory::farcaster_time()),
            )
            .await;

            let hints = verification_hints(&engine);
            assert_eq!(hints.len(), 256, "replay uses the production hint cap");
            assert_hint(
                &hints[0],
                "channel-000",
                &verified_address(),
                proto::ChannelOwnerChangeCause::VerificationAdd,
            );
            assert_hint(
                &hints[255],
                "channel-255",
                &verified_address(),
                proto::ChannelOwnerChangeCause::VerificationAdd,
            );
        }

        #[tokio::test]
        async fn test_truncated_hints_do_not_touch_the_trie() {
            // Truncation must be as trie-inert as normal emission: capping the fan-out
            // mutates zero trie state, so it can never move the shard root (and thus can
            // never affect the merge). Mirrors `test_hints_do_not_touch_the_trie`, but on
            // the truncation path. (That a REAL merge survives ANY hook outcome — error,
            // empty, or this truncation, which is strictly milder than the scan error
            // tested there — is pinned by `test_verification_merge_survives_corrupt_replica`.)
            let (mut engine, _tmp) = new_verifier_engine().await;
            for key in ["a", "b", "c", "d", "e"] {
                register_channel(&mut engine, key, verified_address()).await;
            }

            let root_before = engine.trie_root_hash();

            let add = verification_add(messages_factory::farcaster_time());
            let mut txn = RocksDbTransactionBatch::new();
            let hints = engine.emit_channel_owner_hints_for_verification_capped(
                &add,
                &mut txn,
                EngineVersion::V20,
                3,
            );
            assert_eq!(hints.len(), 3, "the cap truncated to 3 hints");
            engine
                .get_stores()
                .onchain_event_store
                .db
                .commit(txn)
                .unwrap();

            assert_eq!(
                to_hex(&root_before),
                to_hex(&engine.trie_root_hash()),
                "emitting truncated hints must not touch the shard root"
            );
        }

        #[tokio::test]
        async fn test_cap_above_owned_count_emits_every_hint() {
            // Regression guard: the cap only truncates when owned channels EXCEED it.
            // With the cap well above the owned count, every owned channel still gets a
            // hint — the normal path is unchanged by the cap.
            let (mut engine, _tmp) = new_verifier_engine().await;
            for key in ["a", "b", "c"] {
                register_channel(&mut engine, key, verified_address()).await;
            }

            let add = verification_add(messages_factory::farcaster_time());
            let mut txn = RocksDbTransactionBatch::new();
            let hints = engine.emit_channel_owner_hints_for_verification_capped(
                &add,
                &mut txn,
                EngineVersion::V20,
                256,
            );

            assert_eq!(
                hints.len(),
                3,
                "a cap above the owned count emits one hint per channel"
            );
        }

        #[tokio::test]
        async fn test_end_to_end_ownership_lifecycle() {
            // Capstone: a channel is registered by one address, transferred to the verified
            // address, then verified and unverified — exercising all four hint causes across the
            // onchain-fold and forced-verification replay legs, with GetChannelOwner resolving
            // correctly after each step.
            //
            // Order note: the plan sketches register → verify → transfer → remove, but a transfer
            // that moves a channel AWAY from an address strands it (the address then owns nothing,
            // so a later remove correctly emits no hint). To exercise a meaningful closing
            // VERIFICATION_REMOVE, the transfer here moves the channel TO the verified address
            // before it verifies: register(other) → transfer(→verified) → verify → remove. All
            // four causes fire once each.
            let (mut engine, _tmp) = new_verifier_engine().await;
            let other_owner = vec![0xAA; 20];

            // 1. REGISTER "art" to another address.
            register_channel(&mut engine, "art", other_owner.clone()).await;
            assert_eq!(
                channel_owner_address(&engine, "art"),
                Some(other_owner.clone()),
                "after register, owner is the original address"
            );

            // 2. TRANSFER "art" to the verified address.
            transfer_channel(&mut engine, "art", verified_address(), 2_000_000_000).await;
            assert_eq!(
                channel_owner_address(&engine, "art"),
                Some(verified_address()),
                "after transfer, owner is the verified address"
            );

            // 3. VERIFICATION_ADD for the verified address (now owns "art").
            let ts = messages_factory::farcaster_time();
            commit_verification(&mut engine, &verification_add(ts)).await;
            assert_eq!(
                channel_owner_address(&engine, "art"),
                Some(verified_address()),
                "verification does not change registry ownership"
            );

            // 4. VERIFICATION_REMOVE for the verified address (still owns "art").
            commit_verification(&mut engine, &verification_remove(ts + 1)).await;
            assert_eq!(
                channel_owner_address(&engine, "art"),
                Some(verified_address()),
                "removing a verification does not change registry ownership"
            );

            // The full hint sequence across both replay legs, in emission order.
            let hints = owner_change_hints(&engine);
            let causes: Vec<i32> = hints.iter().map(|hint| hint_body(hint).cause).collect();
            assert_eq!(
                causes,
                vec![
                    proto::ChannelOwnerChangeCause::Register as i32,
                    proto::ChannelOwnerChangeCause::Transfer as i32,
                    proto::ChannelOwnerChangeCause::VerificationAdd as i32,
                    proto::ChannelOwnerChangeCause::VerificationRemove as i32,
                ],
                "all four causes fire once each, in lifecycle order"
            );
            // Every hint concerns "art"; the two verification hints carry the verified address.
            assert_hint(
                &hints[2],
                "art",
                &verified_address(),
                proto::ChannelOwnerChangeCause::VerificationAdd,
            );
            assert_hint(
                &hints[3],
                "art",
                &verified_address(),
                proto::ChannelOwnerChangeCause::VerificationRemove,
            );
        }
    }
}
