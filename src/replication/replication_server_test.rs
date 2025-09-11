#[cfg(test)]
mod tests {
    use crate::{
        proto::{self, replication_service_server::ReplicationService},
        replication::{
            replication_test_utils::replication_test_utils::*,
            replicator::{self},
            ReplicationServer,
        },
        storage::{
            db::RocksDbTransactionBatch,
            store::{
                engine::{PostCommitMessage, ShardEngine},
                test_helper::{self},
            },
            trie::merkle_trie::{Context, MerkleTrie},
        },
        utils::factory::{self, messages_factory},
    };
    use tempfile::TempDir;

    async fn fetch_transactions(
        replication_server: &ReplicationServer,
        shard_id: u32,
        height: u64,
    ) -> Result<
        (
            Vec<proto::ShardTrieEntryWithMessage>,
            Vec<proto::FidAccountRootHash>,
            proto::GetShardSnapshotMetadataResponse,
        ),
        Box<dyn std::error::Error>,
    > {
        // Fetch shard snapshot metadata for shard_id: 1
        let snapshot_request = proto::GetShardSnapshotMetadataRequest { shard_id: 1 };
        let snapshot_response = replication_server
            .get_shard_snapshot_metadata(tonic::Request::new(snapshot_request))
            .await?
            .into_inner();

        let mut trie_messages = vec![];
        let mut fid_account_roots = vec![];

        for vts in 0..255 {
            let mut next_page_token: Option<String> = None;

            loop {
                let request = proto::GetShardTransactionsRequest {
                    shard_id,
                    trie_virtual_shard: vts,
                    height,
                    page_token: next_page_token.clone(),
                };

                // Call the server method and handle the Result
                let response = replication_server
                    .get_shard_transactions(tonic::Request::new(request))
                    .await?
                    .into_inner();

                // Extend trie_messages and fid_account_roots with the response
                trie_messages.extend(response.trie_messages);
                fid_account_roots.extend(response.fid_account_roots);

                // Update the page token for the next iteration
                next_page_token = response.next_page_token;

                // If no more pages, break out of the inner loop and move to the next vts
                if next_page_token.is_none() {
                    break;
                }
            }
        }

        Ok((trie_messages, fid_account_roots, snapshot_response))
    }

    async fn replicate_engine(
        source_engine: &ShardEngine,
        dest_engine: &mut ShardEngine,
        replication_server: &ReplicationServer,
        fids: Vec<u64>,
    ) {
        let height = source_engine.get_confirmed_height().block_number;
        let (trie_messages, fid_account_roots, snapshot_metadata) =
            fetch_transactions(replication_server, source_engine.shard_id(), height)
                .await
                .unwrap();

        // There are 14 messages inserted, and all of them should be returned
        assert_eq!(trie_messages.len(), 14);
        // 2 FIDs were used, and both should be returned
        assert_eq!(fid_account_roots.len(), 2);
        // Multiple snapshots are present
        assert!(snapshot_metadata.snapshots.len() > 1);

        // Find the highest snapshot
        let snapshot = snapshot_metadata
            .snapshots
            .iter()
            .reduce(|a, b| if a.height > b.height { a } else { b })
            .unwrap();

        // Header of the highest snapshot
        let shard_chunk_header = snapshot
            .shard_chunk
            .as_ref()
            .unwrap()
            .clone()
            .header
            .as_ref()
            .unwrap()
            .clone();

        assert_eq!(snapshot.shard_id, 1);
        assert_eq!(shard_chunk_header.height.as_ref().unwrap().shard_index, 1);

        // The trie root should match this
        let metadata_shard_root = shard_chunk_header.shard_root.clone();

        // First, we'll try to reconstruct the trie independently from the trie keys, to see if we get the same hashes
        let dest_db = dest_engine.db.clone();
        let mut trie = MerkleTrie::new().unwrap();
        trie.initialize(&dest_db).unwrap();

        let all_trie_keys = trie_messages
            .iter()
            .map(|te| te.trie_key.clone())
            .collect::<Vec<_>>();

        // Add all the trie keys
        let inserted = trie
            .insert(
                &Context::new(),
                &dest_db,
                &mut RocksDbTransactionBatch::new(),
                all_trie_keys
                    .iter()
                    .map(|k| k.as_slice())
                    .collect::<Vec<_>>(),
            )
            .unwrap();

        assert!(inserted.iter().all(|b| *b));

        // The total messages across all FIDs should match exactly the number of keys we inserted
        let total_num_messages = fid_account_roots
            .iter()
            .map(|r| r.num_messages as usize)
            .sum::<usize>();
        assert_eq!(all_trie_keys.len(), total_num_messages);

        // The roots should match, so we know that all the trie entries came over correctly.
        assert_eq!(metadata_shard_root, trie.root_hash().unwrap());
        // Reload the trie to reset it. We'll insert the keys again when we merge the messages
        trie.reload(&dest_db).unwrap();

        // Now, go over the individual messages, insert into the dest engine
        let mut txn_batch = RocksDbTransactionBatch::new();
        for trie_message in trie_messages {
            let inserted = dest_engine
                .replay_replicator_message(&mut txn_batch, &trie_message)
                .expect("Failed to replay replicator message");
            assert!(fids.contains(&inserted.fid));
            assert!(all_trie_keys.contains(&inserted.trie_key));
            assert!(inserted.hub_event.body.is_some());

            trie.insert(
                &Context::new(),
                &dest_db,
                &mut txn_batch,
                vec![inserted.trie_key.as_slice()],
            )
            .unwrap();
        }
        dest_db.commit(txn_batch).unwrap();

        // The roots should match
        assert_eq!(metadata_shard_root, trie.root_hash().unwrap());
    }

    #[tokio::test]
    async fn test_replication() {
        // open tmp dir for database
        let tmp_dir = TempDir::new().unwrap();

        let (post_commit_tx, post_commit_rx) = tokio::sync::mpsc::channel::<PostCommitMessage>(1);

        let (signer, mut engine) =
            new_engine_with_fname_signer(&tmp_dir, Some(post_commit_tx)).await; // source engine
        let (_, mut new_engine) = new_engine_with_fname_signer(&tmp_dir, None).await; // engine to replicate to

        let (replicator, replication_server) = setup_replicator(&mut engine);
        let spawned_replicator = replicator.clone();
        tokio::spawn(async move {
            replicator::run(spawned_replicator, post_commit_rx).await;
        });

        // Note: we're using FID3_FOR_TEST here because the address verification message contains
        // that FID.
        let fid = test_helper::FID3_FOR_TEST;
        let fid_signer = register_fid(&mut engine, fid).await;

        let fid2 = 2000;
        let fid2_signer = register_fid(&mut engine, fid2).await;

        // Running timestamp
        let mut timestamp = factory::time::farcaster_time();

        timestamp += 1;

        let fname = &"replica-test".to_string();

        register_fname(
            &mut engine,
            &signer,
            fid,
            Some(&fid_signer),
            fname,
            Some(timestamp),
        )
        .await;

        set_bio(
            &mut engine,
            fid2,
            Some(&fid2_signer),
            &"hello".to_string(),
            Some(timestamp),
        )
        .await;

        timestamp += 1;

        transfer_fname(
            &mut engine,
            &signer,
            fid,
            Some(&fid_signer),
            fid2,
            fname,
            Some(timestamp),
        )
        .await;

        timestamp += 1;

        let cast = send_cast(
            &mut engine,
            fid,
            Some(&fid_signer),
            "hello world",
            Some(timestamp),
        )
        .await;

        timestamp += 1;

        create_link(&mut engine, fid, Some(&fid_signer), fid2, Some(timestamp)).await;
        create_compact_link(
            &mut engine,
            fid2,
            Some(&fid2_signer),
            vec![fid],
            Some(timestamp),
        )
        .await;
        like_cast(
            &mut engine,
            fid2,
            Some(&fid2_signer),
            &cast,
            Some(timestamp),
        )
        .await;

        timestamp += 1;

        // Note: has to use FID3_FOR_TEST
        let address_verification_add = messages_factory::verifications::create_verification_add(
            fid,
            0,
            hex::decode("91031dcfdea024b4d51e775486111d2b2a715871").unwrap(),
            hex::decode("b72c63d61f075b36fb66a9a867b50836cef19d653a3c09005628738677bcb25f25b6b6e6d2e1d69cd725327b3c020deef9e2575a22dc8ed08f88bc75718ce1cb1c").unwrap(),
            hex::decode("d74860c4bbf574d5ad60f03a478a30f990e05ac723e138a5c860cdb3095f4296").unwrap(),
            Some(timestamp),
            Some(&fid_signer),
        );

        commit_message(&mut engine, &address_verification_add).await;

        timestamp += 1;

        let username_proof_add = messages_factory::username_proof::create_username_proof(
            fid,
            proto::UserNameType::UsernameTypeEnsL1,
            "username.eth".to_string().clone(),
            hex::decode("91031dcfdea024b4d51e775486111d2b2a715871").unwrap(),
            "signature".to_string(),
            timestamp as u64,
            Some(&fid_signer),
        );
        commit_message(&mut engine, &username_proof_add).await;

        // Note that snapshots are added to the replicator as we commit messages

        replicate_engine(
            &engine,
            &mut new_engine,
            &replication_server,
            vec![fid, fid2],
        )
        .await;
    }
}
