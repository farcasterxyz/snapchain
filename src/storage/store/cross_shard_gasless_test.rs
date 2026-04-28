// Cross-shard end-to-end coverage for the gasless-key propagation model
// (NEYN-10626). Other tests cover each engine in isolation:
//   * `block_engine_test::key_add_remove_tests` — BlockEngine merges KEY_ADD
//     and emits the right `BlockEvent`.
//   * `engine_tests::gasless_key_replay_tests` — a ShardEngine fed a synthetic
//     `BlockEvent::MergeMessage` populates its local gasless store and a
//     downstream cast signed by that key validates.
//
// What's missing in either is the wire between them: nothing exercises
// "BlockEvent shape that BlockEngine actually emits → fed to a real
// ShardEngine". These tests close that gap by running both engines in the
// same process, taking the `Block` produced by a real BlockEngine commit, and
// replaying its `MergeMessageEventBody` events through the ShardEngine's
// `handle_block_event` path. Seqnums are re-issued because the two engines
// have independent `block_event_store`s; the payload — the part NEYN-10626 is
// about — is preserved verbatim.

#[cfg(test)]
mod tests {
    use crate::proto::{self, Block, MessageType};
    use crate::storage::db::RocksDbTransactionBatch;
    use crate::storage::store::account::{get_active_key, get_gasless_key_record, ActiveKey};
    use crate::storage::store::block_engine_test_helpers::{self as block_helpers, Validity};
    use crate::storage::store::engine::ShardEngine;
    use crate::storage::store::mempool_poller::MempoolMessage;
    use crate::storage::store::test_helper::{
        self, commit_block_events, commit_message, message_exists_in_trie, register_user, trie_ctx,
        FID_FOR_TEST,
    };
    use crate::storage::trie::merkle_trie::TrieKey;
    use crate::utils::factory::events_factory::create_merge_message_event;
    use crate::utils::factory::messages_factory;
    use crate::utils::factory::signers::generate_signer;
    use alloy_signer_local::PrivateKeySigner;

    const REQUEST_FID: u64 = FID_FOR_TEST + 100;
    const STORAGE_UNITS: u32 = 1000;

    fn address_bytes(signer: &PrivateKeySigner) -> Vec<u8> {
        signer.address().as_slice().to_vec()
    }

    /// Pulls every `MergeMessage` BlockEvent out of `block.events`, re-wraps
    /// each embedded `proto::Message` with a fresh seqnum the ShardEngine
    /// expects, and feeds them through `commit_block_events`. That helper
    /// drives `propose_state_change → replay_snapchain_txn →
    /// handle_block_event` — the same path production uses.
    ///
    /// Re-numbering is required because the BlockEngine's seqnums depend on
    /// its own `block_event_store` history (heartbeats, prior merges), while
    /// the ShardEngine's `replay_snapchain_txn` requires `seqnum ==
    /// last_block_event_seqnum + 1`. The payload — the part NEYN-10626 is
    /// about — passes through unchanged.
    async fn propagate_merges_to_shard(
        block: &Block,
        shard_engine: &mut ShardEngine,
        next_seqnum: &mut u64,
    ) {
        let merges: Vec<proto::Message> = block
            .events
            .iter()
            .filter_map(|event| {
                let body = event.data.as_ref()?.body.as_ref()?;
                match body {
                    proto::block_event_data::Body::MergeMessageEventBody(merge) => {
                        merge.message.clone()
                    }
                    _ => None,
                }
            })
            .collect();

        let owned: Vec<proto::BlockEvent> = merges
            .into_iter()
            .map(|msg| {
                let event = create_merge_message_event(msg, *next_seqnum);
                *next_seqnum += 1;
                event
            })
            .collect();
        let refs: Vec<&proto::BlockEvent> = owned.iter().collect();
        commit_block_events(shard_engine, refs).await;
    }

    fn build_key_add(
        fid_custody: &PrivateKeySigner,
        app_custody: &PrivateKeySigner,
        envelope: &ed25519_dalek::SigningKey,
        scopes: Vec<MessageType>,
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

    /// Registers `fid` on both engines with the same Ethereum custody address
    /// bytes. Both engines need the on-chain state independently because they
    /// have separate DBs (as in production); KEY_ADD merge needs it on the
    /// BlockEngine side for EIP-712 recovery, and `validate_user_message`
    /// needs it on the ShardEngine side for the active-signer lookup.
    async fn register_on_both(
        block_engine: &mut crate::storage::store::block_engine::BlockEngine,
        shard_engine: &mut ShardEngine,
        fid: u64,
        custody: &PrivateKeySigner,
        signer: ed25519_dalek::SigningKey,
    ) {
        let custody_bytes = address_bytes(custody);
        block_helpers::register_user(
            fid,
            signer.clone(),
            custody_bytes.clone(),
            STORAGE_UNITS,
            block_engine,
        );
        register_user(fid, signer, custody_bytes, shard_engine).await;
    }

    #[tokio::test]
    async fn test_block_engine_emitted_key_add_replays_into_shard() {
        let (mut block_engine, _block_dir) = block_helpers::setup();
        let (mut shard_engine, _shard_dir) = test_helper::new_engine().await;

        let fid_custody = PrivateKeySigner::random();
        let app_custody = PrivateKeySigner::random();
        let gasless = generate_signer();
        let gasless_pubkey: [u8; 32] = gasless.verifying_key().to_bytes();

        register_on_both(
            &mut block_engine,
            &mut shard_engine,
            FID_FOR_TEST,
            &fid_custody,
            generate_signer(),
        )
        .await;
        register_on_both(
            &mut block_engine,
            &mut shard_engine,
            REQUEST_FID,
            &app_custody,
            generate_signer(),
        )
        .await;

        let timestamp = messages_factory::farcaster_time();
        let key_add = build_key_add(
            &fid_custody,
            &app_custody,
            &gasless,
            vec![MessageType::CastAdd],
            3600,
            1,
            timestamp,
        );

        // BlockEngine merges and emits the BlockEvent we'll feed to the shard.
        let block = block_helpers::commit_message(&mut block_engine, &key_add, Validity::Valid);
        let merge_event = block
            .events
            .iter()
            .find(|e| {
                matches!(
                    e.data.as_ref().and_then(|d| d.body.as_ref()),
                    Some(proto::block_event_data::Body::MergeMessageEventBody(_))
                )
            })
            .expect("BlockEngine must emit a MergeMessage event for a valid KEY_ADD");
        block_helpers::assert_merge_message_event(merge_event, &key_add);

        let mut next_seqnum: u64 = 1;
        propagate_merges_to_shard(&block, &mut shard_engine, &mut next_seqnum).await;

        // Replay populated the shard-local gasless store.
        let stores = shard_engine.get_stores();
        let txn = RocksDbTransactionBatch::new();
        let record = get_gasless_key_record(&stores.db, &txn, FID_FOR_TEST, &gasless_pubkey)
            .unwrap()
            .expect("gasless record must materialize on shard via real BlockEvent");
        assert_eq!(record.request_fid, REQUEST_FID);

        let active = get_active_key(
            &stores.onchain_event_store,
            &stores.db,
            &txn,
            FID_FOR_TEST,
            &gasless_pubkey,
        )
        .unwrap()
        .expect("active key must resolve on shard");
        assert!(matches!(active, ActiveKey::Gasless { .. }));

        // The cast signed by the gasless key validates and merges via the
        // combined signer-set lookup wired up in NEYN-10575.
        let cast = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "hello from gasless",
            Some(timestamp + 2),
            Some(&gasless),
        );
        commit_message(&mut shard_engine, &cast).await;
        assert!(message_exists_in_trie(&mut shard_engine, &cast));
    }

    #[tokio::test]
    async fn test_block_engine_emitted_key_remove_invalidates_shard_signer() {
        let (mut block_engine, _block_dir) = block_helpers::setup();
        let (mut shard_engine, _shard_dir) = test_helper::new_engine().await;

        let fid_custody = PrivateKeySigner::random();
        let app_custody = PrivateKeySigner::random();
        let gasless = generate_signer();
        let gasless_pubkey: [u8; 32] = gasless.verifying_key().to_bytes();

        register_on_both(
            &mut block_engine,
            &mut shard_engine,
            FID_FOR_TEST,
            &fid_custody,
            generate_signer(),
        )
        .await;
        register_on_both(
            &mut block_engine,
            &mut shard_engine,
            REQUEST_FID,
            &app_custody,
            generate_signer(),
        )
        .await;

        let timestamp = messages_factory::farcaster_time();
        let key_add = build_key_add(
            &fid_custody,
            &app_custody,
            &gasless,
            vec![MessageType::CastAdd],
            3600,
            1,
            timestamp,
        );
        let add_block = block_helpers::commit_message(&mut block_engine, &key_add, Validity::Valid);
        let mut next_seqnum: u64 = 1;
        propagate_merges_to_shard(&add_block, &mut shard_engine, &mut next_seqnum).await;

        // Cast accepted while the key is live — sanity check that the wire is
        // working end-to-end before we exercise the revocation path.
        let cast_pre_revoke = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "before revoke",
            Some(timestamp + 1),
            Some(&gasless),
        );
        commit_message(&mut shard_engine, &cast_pre_revoke).await;
        assert!(message_exists_in_trie(&mut shard_engine, &cast_pre_revoke));

        // BlockEngine commits the self-revoke. Self-revoke skips the inner
        // EIP-712 signature: the envelope's Ed25519 signature stands in.
        let key_remove = messages_factory::keys::create_key_remove_self_revoke(
            FID_FOR_TEST,
            &gasless,
            2,
            timestamp + 1_000_000,
            Some(timestamp + 2),
        );
        let remove_block =
            block_helpers::commit_message(&mut block_engine, &key_remove, Validity::Valid);
        propagate_merges_to_shard(&remove_block, &mut shard_engine, &mut next_seqnum).await;

        // Shard-local gasless record gone after replay.
        let stores = shard_engine.get_stores();
        let txn = RocksDbTransactionBatch::new();
        assert!(
            get_gasless_key_record(&stores.db, &txn, FID_FOR_TEST, &gasless_pubkey)
                .unwrap()
                .is_none()
        );

        // A subsequent cast signed by the (now-revoked) gasless key must not
        // merge — `validate_user_message`'s active-signer lookup misses both
        // the on-chain set and the cleared gasless store.
        let cast_post_revoke = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "after revoke",
            Some(timestamp + 3),
            Some(&gasless),
        );
        let state_change = shard_engine.propose_state_change(
            shard_engine.shard_id(),
            vec![MempoolMessage::UserMessage(cast_post_revoke.clone())],
            None,
        );
        test_helper::validate_and_commit_state_change(&mut shard_engine, &state_change).await;
        assert!(!TrieKey::for_message(&cast_post_revoke)
            .iter()
            .all(|key| shard_engine.trie_key_exists(trie_ctx(), key)));
    }
}
