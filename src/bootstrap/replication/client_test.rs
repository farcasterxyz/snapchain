#[cfg(test)]
mod tests {
    use crate::{
        bootstrap::replication::{
            error::BootstrapError,
            rpc_client::RpcClientsManager,
            service::{ReplicatorBootstrap, ReplicatorBootstrapConfig, WorkUnitResponse},
            test_utils::replication_test_utils::*,
        },
        cfg::Config,
        network::replication::{replicator, ReplicationServer},
        proto::{
            self,
            replication_service_server::{ReplicationService, ReplicationServiceServer},
            GetShardSnapshotMetadataResponse, GetShardTransactionsResponse, ShardSnapshotMetadata,
            ShardTrieEntryWithMessage,
        },
        storage::{
            db::{RocksDB, RocksDbTransactionBatch},
            store::{
                block_engine_test_helpers,
                engine::{PostCommitMessage, ShardEngine},
                test_helper::{self, statsd_client},
            },
            trie::merkle_trie::{Context, MerkleTrie},
        },
        utils::factory::{self, messages_factory},
    };
    use rand::random;
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tonic::transport::Server;
    use tonic::{Request, Response, Status};

    // Mock Server Implementation
    #[derive(Default)]
    struct MockReplicationService {
        // Map (shard_id) -> metadata_response
        metadata_responses:
            Arc<Mutex<HashMap<u32, Result<GetShardSnapshotMetadataResponse, Status>>>>,
        // Map (shard_id, height, vts, page_token) -> transactions_response
        transactions_responses: Arc<
            Mutex<
                HashMap<
                    (u32, u64, u32, Option<String>),
                    Result<GetShardTransactionsResponse, Status>,
                >,
            >,
        >,
        // Counter for calls
        request_counts: Arc<Mutex<HashMap<String, u32>>>,
        // Error rate for simulating failures (0.0 to 1.0)
        error_rate: f64,
    }

    #[tonic::async_trait]
    impl ReplicationService for MockReplicationService {
        async fn get_shard_snapshot_metadata(
            &self,
            request: Request<proto::GetShardSnapshotMetadataRequest>,
        ) -> Result<Response<GetShardSnapshotMetadataResponse>, Status> {
            let mut counts = self.request_counts.lock().unwrap();
            *counts
                .entry("get_shard_snapshot_metadata".to_string())
                .or_insert(0) += 1;

            let shard_id = request.into_inner().shard_id;
            let locked_responses = self.metadata_responses.lock().unwrap();
            if let Some(res) = locked_responses.get(&shard_id) {
                match res {
                    Ok(data) => Ok(Response::new(data.clone())),
                    Err(status) => Err(status.clone()),
                }
            } else {
                // return empty for shard-0
                if shard_id == 0 {
                    return Ok(Response::new(GetShardSnapshotMetadataResponse {
                        snapshots: vec![ShardSnapshotMetadata::default()],
                    }));
                }

                Err(Status::not_found(format!(
                    "No metadata for shard_id {}",
                    shard_id
                )))
            }
        }

        async fn get_shard_transactions(
            &self,
            request: Request<proto::GetShardTransactionsRequest>,
        ) -> Result<Response<GetShardTransactionsResponse>, Status> {
            if self.error_rate > 0.0 && rand::random::<f64>() < self.error_rate {
                return Err(Status::internal("Simulated error"));
            }

            let mut counts = self.request_counts.lock().unwrap();
            *counts
                .entry("get_shard_transactions".to_string())
                .or_insert(0) += 1;

            let inner = request.into_inner();
            let key = (
                inner.shard_id,
                inner.height,
                inner.trie_virtual_shard,
                inner.page_token,
            );

            let mut locked_responses = self.transactions_responses.lock().unwrap();

            // First, peek at the response without removing it.
            if let Some(res) = locked_responses.get(&key) {
                return match res {
                    // If it's a success, clone and return it.
                    Ok(data) => Ok(Response::new(data.clone())),
                    // If it's an error, we consume it (for retry tests) and return the error.
                    Err(status) => {
                        let cloned_status = status.clone();
                        // Now that we know it's an error, we can remove it.
                        locked_responses.remove(&key);
                        Err(cloned_status)
                    }
                };
            }

            // If we get here, the key was not found in the map at all.
            Err(Status::not_found(format!(
                "No transactions for key {:?}",
                key
            )))
        }
    }

    /// Helper to spawn a mock server and get its address.
    async fn spawn_mock_server(
        mock_service: MockReplicationService,
    ) -> (SocketAddr, oneshot::Sender<()>) {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_future = Server::builder()
            .add_service(ReplicationServiceServer::new(mock_service))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    shutdown_rx.await.ok();
                },
            );

        tokio::spawn(server_future);

        (addr, shutdown_tx)
    }

    #[tokio::test]
    async fn test_basic_correctness_and_initialization() {
        let mock_service = MockReplicationService::default();
        let shard_id = 1;
        let height = 100;

        // --- Setup Mock Responses ---
        let metadata = GetShardSnapshotMetadataResponse {
            snapshots: vec![ShardSnapshotMetadata {
                shard_id,
                height,
                ..Default::default()
            }],
        };
        mock_service
            .metadata_responses
            .lock()
            .unwrap()
            .insert(shard_id, Ok(metadata));

        let transactions = GetShardTransactionsResponse {
            trie_messages: vec![ShardTrieEntryWithMessage::default()],
            next_page_token: Some("page_2".to_string()),
            ..Default::default()
        };
        mock_service
            .transactions_responses
            .lock()
            .unwrap()
            .insert((shard_id, height, 0, None), Ok(transactions));

        // --- Start Server ---
        let (addr, shutdown_tx) = spawn_mock_server(mock_service).await;
        let peer_addr = format!("http://{}", addr);

        // --- Test ---
        let manager = RpcClientsManager::new(
            peer_addr.clone(),
            shard_id,
            ReplicatorBootstrapConfig::default(),
        )
        .await
        .unwrap();
        assert_eq!(manager.get_metadata().height, height);

        let vts = 0;
        let response = manager.get_shard_transactions(vts, None).await.unwrap();

        assert_eq!(response.trie_messages.len(), 1);
        assert_eq!(response.next_page_token, Some("page_2".to_string()));

        manager.close().await;
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn test_full_pagination() {
        let mock_service = MockReplicationService::default();
        let shard_id = 2;
        let height = 200;
        let vts = 1;

        // --- Setup Mock Responses for 3 pages ---
        let metadata = GetShardSnapshotMetadataResponse {
            snapshots: vec![ShardSnapshotMetadata {
                shard_id,
                height,
                ..Default::default()
            }],
        };
        mock_service
            .metadata_responses
            .lock()
            .unwrap()
            .insert(shard_id, Ok(metadata));
        // Page 1
        let page1 = GetShardTransactionsResponse {
            next_page_token: Some("page_2".to_string()),
            ..Default::default()
        };
        mock_service
            .transactions_responses
            .lock()
            .unwrap()
            .insert((shard_id, height, vts as u32, None), Ok(page1));
        // Page 2
        let page2 = GetShardTransactionsResponse {
            next_page_token: Some("page_3".to_string()),
            ..Default::default()
        };
        mock_service.transactions_responses.lock().unwrap().insert(
            (shard_id, height, vts as u32, Some("page_2".to_string())),
            Ok(page2),
        );
        // Page 3 (final)
        let page3 = GetShardTransactionsResponse {
            next_page_token: None,
            ..Default::default()
        };
        mock_service.transactions_responses.lock().unwrap().insert(
            (shard_id, height, vts as u32, Some("page_3".to_string())),
            Ok(page3),
        );

        // --- Start Server ---
        let (addr, shutdown_tx) = spawn_mock_server(mock_service).await;
        let peer_addr = format!("http://{}", addr);

        // --- Test ---
        let manager =
            RpcClientsManager::new(peer_addr, shard_id, ReplicatorBootstrapConfig::default())
                .await
                .unwrap();

        // Request page 1
        let resp1 = manager.get_shard_transactions(vts, None).await.unwrap();
        assert_eq!(resp1.next_page_token, Some("page_2".to_string()));

        // Request page 2
        let resp2 = manager
            .get_shard_transactions(vts, Some("page_2".to_string()))
            .await
            .unwrap();
        assert_eq!(resp2.next_page_token, Some("page_3".to_string()));

        // Request page 3
        let resp3 = manager
            .get_shard_transactions(vts, Some("page_3".to_string()))
            .await
            .unwrap();
        assert_eq!(resp3.next_page_token, None);

        manager.close().await;
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn test_add_peer_fails_on_mismatched_height() {
        let mock_service_1 = MockReplicationService::default();
        let shard_id = 3;
        let correct_height = 300;

        // Peer 1 has the correct height
        let metadata1 = GetShardSnapshotMetadataResponse {
            snapshots: vec![ShardSnapshotMetadata {
                shard_id,
                height: correct_height,
                ..Default::default()
            }],
        };
        mock_service_1
            .metadata_responses
            .lock()
            .unwrap()
            .insert(shard_id, Ok(metadata1));
        let (addr1, shutdown1) = spawn_mock_server(mock_service_1).await;

        // --- Test ---
        let manager = RpcClientsManager::new(
            format!("http://{}", addr1),
            shard_id,
            ReplicatorBootstrapConfig::default(),
        )
        .await
        .unwrap();

        // Peer 2 has the wrong height
        let mock_service_2 = MockReplicationService::default();
        let metadata2 = GetShardSnapshotMetadataResponse {
            snapshots: vec![ShardSnapshotMetadata {
                shard_id,
                height: 999,
                ..Default::default()
            }],
        };

        mock_service_2
            .metadata_responses
            .lock()
            .unwrap()
            .insert(shard_id, Ok(metadata2));
        let (addr2, shutdown2) = spawn_mock_server(mock_service_2).await;

        // Try to add the new peer with wrong height, should not be added
        let result = manager.add_new_peer(format!("http://{}", addr2)).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);

        // Check internal state to see if the peer was added
        {
            let inner_arc = manager.inner();
            let inner = inner_arc.lock().await;
            assert_eq!(
                inner.peer_manager.peer_addresses.len(),
                1,
                "Peer with wrong height should not be added"
            );
        }

        manager.close().await;
        let _ = shutdown1.send(());
        let _ = shutdown2.send(());
    }

    #[tokio::test]
    async fn test_retries_are_handled() {
        let mock_service = MockReplicationService::default();
        let shard_id = 4;
        let height = 400;
        let vts = 0;

        let request_key = (shard_id, height, vts as u32, None);

        // Setup metadata
        let metadata = GetShardSnapshotMetadataResponse {
            snapshots: vec![ShardSnapshotMetadata {
                shard_id,
                height,
                ..Default::default()
            }],
        };
        mock_service
            .metadata_responses
            .lock()
            .unwrap()
            .insert(shard_id, Ok(metadata));

        // --- Setup Mock Responses for retry ---
        {
            let mut responses = mock_service.transactions_responses.lock().unwrap();
            // First two calls fail
            responses.insert(request_key.clone(), Err(Status::unavailable("server busy")));
            responses.insert(request_key.clone(), Err(Status::unavailable("server busy")));
            // Third call succeeds
            let success_response = GetShardTransactionsResponse {
                next_page_token: Some("success".to_string()),
                ..Default::default()
            };
            responses.insert(request_key.clone(), Ok(success_response));
        }

        // --- Start Server ---
        let (addr, shutdown_tx) = spawn_mock_server(mock_service).await;

        // --- Test ---
        let manager = RpcClientsManager::new(
            format!("http://{}", addr),
            shard_id,
            ReplicatorBootstrapConfig::default(),
        )
        .await
        .unwrap();
        let response = manager.get_shard_transactions(vts, None).await;

        // It should succeed after retries
        assert!(response.is_ok());
        assert_eq!(
            response.unwrap().next_page_token,
            Some("success".to_string())
        );

        manager.close().await;
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn test_vts_affinity() {
        let mock_service_1 = MockReplicationService::default();
        let mock_service_2 = MockReplicationService::default();
        let shard_id = 5;
        let height = 500;

        // --- Setup Metadata for both servers ---
        let metadata = GetShardSnapshotMetadataResponse {
            snapshots: vec![ShardSnapshotMetadata {
                shard_id,
                height,
                ..Default::default()
            }],
        };
        mock_service_1
            .metadata_responses
            .lock()
            .unwrap()
            .insert(shard_id, Ok(metadata.clone()));
        mock_service_2
            .metadata_responses
            .lock()
            .unwrap()
            .insert(shard_id, Ok(metadata));

        // --- Setup Transaction responses for both servers ---
        let tx_data = GetShardTransactionsResponse::default();
        mock_service_1
            .transactions_responses
            .lock()
            .unwrap()
            .insert((shard_id, height, 0, None), Ok(tx_data.clone()));
        mock_service_2
            .transactions_responses
            .lock()
            .unwrap()
            .insert((shard_id, height, 1, None), Ok(tx_data));

        // --- Start Servers ---
        let (addr1, shutdown1) = spawn_mock_server(mock_service_1).await;
        let (addr2, shutdown2) = spawn_mock_server(mock_service_2).await;
        let peer_addr_1 = format!("http://{}", addr1);
        let peer_addr_2 = format!("http://{}", addr2);

        // --- Test ---
        let manager = RpcClientsManager::new(
            peer_addr_1.clone(),
            shard_id,
            ReplicatorBootstrapConfig::default(),
        )
        .await
        .unwrap();

        let result = manager.add_new_peer(peer_addr_2.clone()).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);

        // Request VTS 0 - this should establish affinity
        let _ = manager.get_shard_transactions(0, None).await.unwrap();

        // Check which peer was used. Because of the round-robin logic `vts % num_peers`,
        // vts 0 will go to peer 0 (addr1).
        {
            let inner_arc = manager.inner();
            let inner = inner_arc.lock().await;
            let vts_0_peer = inner.peer_manager.vts_peer_affinity.get(&0).cloned();
            assert_eq!(vts_0_peer, Some(peer_addr_1.clone()));
        }

        // Request VTS 1 - this should establish affinity
        let _ = manager.get_shard_transactions(1, None).await.unwrap();

        // VTS 1 should go to peer 1 (addr2).
        {
            let inner_arc = manager.inner();
            let inner = inner_arc.lock().await;
            let vts_1_peer = inner.peer_manager.vts_peer_affinity.get(&1).cloned();
            assert_eq!(vts_1_peer, Some(peer_addr_2.clone()));
        }

        // Make a second request for VTS 0. It should reuse the same peer.
        let _ = manager.get_shard_transactions(0, None).await.unwrap();
        {
            let inner_arc = manager.inner();
            let inner = inner_arc.lock().await;
            let vts_0_peer_again = inner.peer_manager.vts_peer_affinity.get(&0).cloned();
            assert_eq!(vts_0_peer_again, Some(peer_addr_1));
        }

        manager.close().await;
        let _ = shutdown1.send(());
        let _ = shutdown2.send(());
    }

    async fn setup_source_engine_with_test_data() -> (
        TempDir,
        ShardEngine,
        ReplicationServer,
        alloy_signer_local::PrivateKeySigner,
        u64,
        u64,
    ) {
        // Open tmp dir for database
        let tmp_dir = TempDir::new().unwrap();

        let (post_commit_tx, post_commit_rx) = tokio::sync::mpsc::channel::<PostCommitMessage>(1);

        let (signer, mut source_engine) =
            new_engine_with_fname_signer(&tmp_dir, Some(post_commit_tx)).await; // source engine

        let (mut block_engine, _) = block_engine_test_helpers::setup();

        let (replicator, replication_server) =
            setup_replicator(&mut source_engine, &mut block_engine);
        let spawned_replicator = replicator.clone();
        tokio::spawn(async move {
            replicator::run(spawned_replicator, post_commit_rx).await;
        });

        // Note: we're using FID3_FOR_TEST here because the address verification message contains
        // that FID.
        let fid = test_helper::FID3_FOR_TEST;
        let fid_signer = register_fid(&mut source_engine, fid).await;

        let fid2 = 2000;
        let fid2_signer = register_fid(&mut source_engine, fid2).await;

        // Running timestamp
        let mut timestamp = factory::time::farcaster_time();

        timestamp += 1;

        let fname = &"replica-test".to_string();

        register_fname(
            &mut source_engine,
            &signer,
            fid,
            Some(&fid_signer),
            fname,
            Some(timestamp),
        )
        .await;

        set_bio(
            &mut source_engine,
            fid2,
            Some(&fid2_signer),
            &"hello".to_string(),
            Some(timestamp),
        )
        .await;

        timestamp += 1;

        transfer_fname(
            &mut source_engine,
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
            &mut source_engine,
            fid,
            Some(&fid_signer),
            "hello world",
            Some(timestamp),
        )
        .await;

        timestamp += 1;

        create_link(
            &mut source_engine,
            fid,
            Some(&fid_signer),
            fid2,
            Some(timestamp),
        )
        .await;
        create_compact_link(
            &mut source_engine,
            fid2,
            Some(&fid2_signer),
            vec![fid],
            Some(timestamp),
        )
        .await;
        like_cast(
            &mut source_engine,
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

        commit_message(&mut source_engine, &address_verification_add).await;

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
        commit_message(&mut source_engine, &username_proof_add).await;

        (
            tmp_dir,
            source_engine,
            replication_server,
            signer,
            fid,
            fid2,
        )
    }

    async fn fetch_transactions(
        replication_server: &ReplicationServer,
        shard_id: u32,
        height: u64,
    ) -> Result<
        (
            HashMap<
                u32,
                (
                    Vec<proto::ShardTrieEntryWithMessage>,
                    Vec<proto::FidAccountRootHash>,
                ),
            >,
            proto::GetShardSnapshotMetadataResponse,
        ),
        Box<dyn std::error::Error>,
    > {
        // Fetch shard snapshot metadata for shard_id
        let snapshot_request = proto::GetShardSnapshotMetadataRequest { shard_id };
        let snapshot_response = replication_server
            .get_shard_snapshot_metadata(tonic::Request::new(snapshot_request))
            .await?
            .into_inner();

        let mut vts_data: HashMap<
            u32,
            (
                Vec<proto::ShardTrieEntryWithMessage>,
                Vec<proto::FidAccountRootHash>,
            ),
        > = HashMap::new();

        for vts in 0..256 {
            let mut trie_messages = vec![];
            let mut fid_account_roots = vec![];
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

            vts_data.insert(vts, (trie_messages, fid_account_roots));
        }

        Ok((vts_data, snapshot_response))
    }

    async fn replicate_engine(
        source_engine: &ShardEngine,
        dest_engine: &mut ShardEngine,
        replication_server: &ReplicationServer,
        fids: Vec<u64>,
    ) {
        let height = source_engine.get_confirmed_height().block_number;
        let (vts_data, snapshot_metadata) =
            fetch_transactions(replication_server, source_engine.shard_id(), height)
                .await
                .unwrap();

        // Concatenate all trie_messages and fid_account_roots from all vts
        let mut trie_messages = vec![];
        let mut fid_account_roots = vec![];
        for (_vts, (msgs, roots)) in &vts_data {
            trie_messages.extend(msgs.clone());
            fid_account_roots.extend(roots.clone());
        }

        // There are 14 messages inserted, and all of them should be returned
        let expected_num_messages = 14;
        assert_eq!(trie_messages.len(), expected_num_messages);
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
        assert_eq!(snapshot.num_items, expected_num_messages as u64);

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
    async fn test_replication_with_engine() {
        let (tmp_dir, source_engine, replication_server, _signer, fid, fid2) =
            setup_source_engine_with_test_data().await;

        let (_, mut new_engine) = new_engine_with_fname_signer(&tmp_dir, None).await; // engine to replicate to

        replicate_engine(
            &source_engine,
            &mut new_engine,
            &replication_server,
            vec![fid, fid2],
        )
        .await;
    }

    #[tokio::test]
    async fn test_replication_client_bootstrap_single_peer() {
        let (tmp_dir, source_engine, replication_server, _signer, _fid, _fid2) =
            setup_source_engine_with_test_data().await;

        // Now, start the replication server on a free port
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_addr = format!("http://{}", addr);

        let replication_service = ReplicationServiceServer::new(replication_server);

        tokio::spawn(async move {
            Server::builder()
                .add_service(replication_service)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        // Wait a bit for server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Set up destination engine
        let dest_rocksdb_dir = format!("{}/dest", tmp_dir.path().to_str().unwrap());

        // Create a config for the bootstrap
        let mut config = Config::default();
        config.rocksdb_dir = dest_rocksdb_dir.clone();
        config.consensus.shard_ids = vec![1]; // Only shard 1 for this test
        config.fc_network = source_engine.network.clone();
        config.snapshot.replication_peers = vec![server_addr];

        let bootstrap = ReplicatorBootstrap::new(statsd_client(), &config);

        // Perform the bootstrap
        let result = bootstrap.bootstrap_using_replication().await;

        // Assert that bootstrap succeeded
        match &result {
            Ok(WorkUnitResponse::Finished) => {}
            _ => panic!("Bootstrap failed with result: {:?}", result),
        }

        // Now, check that the shard roots match
        let source_root = source_engine.get_stores().trie.root_hash().unwrap();

        // Open the destination DB and check the root
        let dest_db = RocksDB::open_shard_db(&dest_rocksdb_dir, 1);
        let mut dest_trie = MerkleTrie::new().unwrap();
        dest_trie.initialize(&dest_db).unwrap();
        let dest_root = dest_trie.root_hash().unwrap();

        assert_eq!(source_root, dest_root);
    }

    #[tokio::test]
    async fn test_replication_client_bootstrap_multi_peer() {
        let (tmp_dir, source_engine, replication_server, _signer, _fid, _fid2) =
            setup_source_engine_with_test_data().await;

        // 2. Fetch the actual data from the real replication server to populate our mocks.
        let height = source_engine.get_confirmed_height().block_number;
        let shard_id = source_engine.shard_id();
        let (vts_data, snapshot_metadata) =
            fetch_transactions(&replication_server, shard_id, height)
                .await
                .unwrap();

        // Start three mock replication servers on free ports, all with the same data
        let mut server_addrs = vec![];
        let mut shutdown_txs = vec![];
        let mut request_counts_vec = vec![];

        for _ in 0..3 {
            // --- Setup the Mock Server ---
            let mock_service = MockReplicationService::default();

            // 2a. Mock the metadata response.
            mock_service
                .metadata_responses
                .lock()
                .unwrap()
                .insert(shard_id, Ok(snapshot_metadata.clone()));

            // 2b. For each vts, set the response with normal data (no duplicates)
            for vts in 0..256 {
                let (trie_messages, fid_account_roots) =
                    vts_data.get(&vts).cloned().unwrap_or_default();

                let response = GetShardTransactionsResponse {
                    trie_messages,
                    fid_account_roots,
                    next_page_token: None, // No pagination, one big response.
                };

                mock_service
                    .transactions_responses
                    .lock()
                    .unwrap()
                    .insert((shard_id, height, vts, None), Ok(response));
            }

            // Clone the request_counts to check later
            let request_counts = Arc::clone(&mock_service.request_counts);
            request_counts_vec.push(request_counts);

            // --- Start the Mock Server ---
            let (addr, shutdown_tx) = spawn_mock_server(mock_service).await;
            let server_addr = format!("http://{}", addr);
            server_addrs.push(server_addr);
            shutdown_txs.push(shutdown_tx);
        }

        // Wait a bit for servers to start
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Set up destination engine
        let dest_rocksdb_dir = format!("{}/dest_multi", tmp_dir.path().to_str().unwrap());

        // Create a config for the bootstrap
        let mut config = Config::default();
        config.rocksdb_dir = dest_rocksdb_dir.clone();
        config.consensus.shard_ids = vec![1]; // Only shard 1 for this test
        config.fc_network = source_engine.network.clone();
        config.snapshot.replication_peers = server_addrs;

        let bootstrap = ReplicatorBootstrap::new(statsd_client(), &config);

        // Perform the bootstrap
        let result = bootstrap.bootstrap_using_replication().await;

        // Assert that bootstrap succeeded
        match &result {
            Ok(WorkUnitResponse::Finished) => {}
            _ => panic!("Bootstrap failed with result: {:?}", result),
        }

        // Now, check that the shard roots match
        let source_root = source_engine.get_stores().trie.root_hash().unwrap();

        // Open the destination DB and check the root
        let dest_db = RocksDB::open_shard_db(&dest_rocksdb_dir, 1);
        let mut dest_trie = MerkleTrie::new().unwrap();
        dest_trie.initialize(&dest_db).unwrap();
        let dest_root = dest_trie.root_hash().unwrap();

        assert_eq!(source_root, dest_root);

        // Assert that the request_counts for both methods are 3 each for each of the 3 mock servers
        for request_counts in request_counts_vec {
            let counts = request_counts.lock().unwrap();
            assert!(
                *counts.get("get_shard_snapshot_metadata").unwrap_or(&0) >= 1,
                "get_shard_snapshot_metadata should be called at least once for each peer"
            );
            assert!(
                *counts.get("get_shard_transactions").unwrap_or(&0) > 10,
                "get_shard_transactions should be called multiple times for each peer"
            );
        }

        // Cleanup
        for shutdown_tx in shutdown_txs {
            let _ = shutdown_tx.send(());
        }
    }

    #[tokio::test]
    async fn test_replication_client_bootstrap_multi_page() {
        // let _ = tracing_subscriber::fmt()
        //     .with_max_level(tracing::Level::INFO)
        //     .try_init();

        let (tmp_dir, source_engine, replication_server, _signer, _fid, _fid2) =
            setup_source_engine_with_test_data().await;

        // Now, start the replication server on a free port
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_addr = format!("http://{}", addr);

        let replication_service = ReplicationServiceServer::new(replication_server);

        tokio::spawn(async move {
            Server::builder()
                .add_service(replication_service)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        // Wait a bit for server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Set up destination engine
        let dest_rocksdb_dir = format!("{}/dest", tmp_dir.path().to_str().unwrap());

        // Create a config for the bootstrap
        let mut config = Config::default();
        config.rocksdb_dir = dest_rocksdb_dir.clone();
        config.consensus.shard_ids = vec![1]; // Only shard 1 for this test
        config.fc_network = source_engine.network.clone();
        config.snapshot.replication_peers = vec![server_addr];

        let bootstrap = ReplicatorBootstrap::new_with_config(
            statsd_client(),
            &config,
            ReplicatorBootstrapConfig {
                // Set to commit every message individually, which will cause many pages
                max_messages_in_write_session: 1,
                ..Default::default()
            },
        );

        // Perform the bootstrap
        let result = bootstrap.bootstrap_using_replication().await;

        // Assert that bootstrap succeeded
        match &result {
            Ok(WorkUnitResponse::Finished) => {}
            _ => panic!("Bootstrap failed with result: {:?}", result),
        }

        // Now, check that the shard roots match
        let source_root = source_engine.get_stores().trie.root_hash().unwrap();

        // Open the destination DB and check the root
        let dest_db = RocksDB::open_shard_db(&dest_rocksdb_dir, 1);
        let mut dest_trie = MerkleTrie::new().unwrap();
        dest_trie.initialize(&dest_db).unwrap();
        let dest_root = dest_trie.root_hash().unwrap();

        assert_eq!(source_root, dest_root);
    }

    #[tokio::test]
    async fn test_replication_client_handles_rpc_errors() {
        let (tmp_dir, source_engine, replication_server, _signer, _fid, _fid2) =
            setup_source_engine_with_test_data().await;

        // Fetch the actual data from the real replication server to populate our mock.
        let height = source_engine.get_confirmed_height().block_number;
        let shard_id = source_engine.shard_id();
        let (vts_data, snapshot_metadata) =
            fetch_transactions(&replication_server, shard_id, height)
                .await
                .unwrap();

        // --- Setup the Mock Server ---
        let mut mock_service = MockReplicationService::default();
        mock_service.error_rate = 0.1; // 10% of the vts will return an error

        // Mock the metadata response.
        mock_service
            .metadata_responses
            .lock()
            .unwrap()
            .insert(shard_id, Ok(snapshot_metadata));

        // For each vts, set the response with normal data (no pagination)
        for vts in 0..256 {
            let (trie_messages, fid_account_roots) =
                vts_data.get(&vts).cloned().unwrap_or_default();

            let response = GetShardTransactionsResponse {
                trie_messages,
                fid_account_roots,
                next_page_token: None, // No pagination, one big response.
            };

            mock_service
                .transactions_responses
                .lock()
                .unwrap()
                .insert((shard_id, height, vts, None), Ok(response));
        }

        // --- Start the Mock Server ---
        let (addr, shutdown_tx) = spawn_mock_server(mock_service).await;
        let server_addr = format!("http://{}", addr);

        // Wait a bit for server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Set up destination engine
        let dest_rocksdb_dir = format!("{}/dest_errors", tmp_dir.path().to_str().unwrap());

        // Create a config for the bootstrap
        let mut config = Config::default();
        config.rocksdb_dir = dest_rocksdb_dir.clone();
        config.consensus.shard_ids = vec![shard_id];
        config.fc_network = source_engine.network.clone();
        config.snapshot.replication_peers = vec![server_addr];

        let bootstrap = ReplicatorBootstrap::new_with_config(
            statsd_client(),
            &config,
            ReplicatorBootstrapConfig {
                max_rpc_retries: 100, // Allow many retries to overcome errors
                rpc_retry_delay: Duration::from_secs(0),
                ..Default::default()
            },
        );

        // Perform the bootstrap
        let result = bootstrap.bootstrap_using_replication().await;

        // Assert that bootstrap succeeded despite RPC errors
        match &result {
            Ok(WorkUnitResponse::Finished) => {}
            _ => panic!("Bootstrap failed with result: {:?}", result),
        }

        // Now, check that the shard roots match
        let source_root = source_engine.get_stores().trie.root_hash().unwrap();

        // Open the destination DB and check the root
        let dest_db = RocksDB::open_shard_db(&dest_rocksdb_dir, shard_id);
        let mut dest_trie = MerkleTrie::new().unwrap();
        dest_trie.initialize(&dest_db).unwrap();
        let dest_root = dest_trie.root_hash().unwrap();

        assert_eq!(source_root, dest_root);

        // Cleanup
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn test_replication_client_rpc_error_propogates() {
        let (tmp_dir, source_engine, replication_server, _signer, _fid, _fid2) =
            setup_source_engine_with_test_data().await;

        // Fetch the actual data from the real replication server to populate our mock.
        let height = source_engine.get_confirmed_height().block_number;
        let shard_id = source_engine.shard_id();
        let (_, snapshot_metadata) = fetch_transactions(&replication_server, shard_id, height)
            .await
            .unwrap();

        // --- Setup the Mock Server ---
        let mut mock_service = MockReplicationService::default();
        mock_service.error_rate = 1.0; // only return errors

        // Mock the metadata response.
        mock_service
            .metadata_responses
            .lock()
            .unwrap()
            .insert(shard_id, Ok(snapshot_metadata));

        // --- Start the Mock Server ---
        let (addr, shutdown_tx) = spawn_mock_server(mock_service).await;
        let server_addr = format!("http://{}", addr);

        // Wait a bit for server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Set up destination engine
        let dest_rocksdb_dir = format!("{}/dest_errors", tmp_dir.path().to_str().unwrap());

        // Create a config for the bootstrap
        let mut config = Config::default();
        config.rocksdb_dir = dest_rocksdb_dir.clone();
        config.consensus.shard_ids = vec![shard_id];
        config.fc_network = source_engine.network.clone();
        config.snapshot.replication_peers = vec![server_addr];

        let bootstrap = ReplicatorBootstrap::new_with_config(
            statsd_client(),
            &config,
            ReplicatorBootstrapConfig {
                rpc_retry_delay: Duration::from_secs(0),
                ..Default::default()
            },
        );

        // Perform the bootstrap
        let result = bootstrap.bootstrap_using_replication().await;
        assert!(matches!(result, Err(BootstrapError::RpcError(_))));
        assert!(result.unwrap_err().to_string().contains("Simulated error"));

        // Cleanup
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn test_replication_duplicate_messages() {
        // 1. Create an engine with test data using the existing helper.
        let (tmp_dir, source_engine, replication_server, _signer, _fid, _fid2) =
            setup_source_engine_with_test_data().await;

        // 2. Fetch the actual data from the real replication server to populate our mock.
        let height = source_engine.get_confirmed_height().block_number;
        let shard_id = source_engine.shard_id();
        let (vts_data, snapshot_metadata) =
            fetch_transactions(&replication_server, shard_id, height)
                .await
                .unwrap();

        // --- Setup the Mock Server ---
        let mock_service = MockReplicationService::default();

        // 2a. Mock the metadata response.
        mock_service
            .metadata_responses
            .lock()
            .unwrap()
            .insert(shard_id, Ok(snapshot_metadata));

        // 2c. For each vts, create duplicated trie_messages and set the response
        for vts in 0..256 {
            let (trie_messages, fid_account_roots) =
                vts_data.get(&vts).cloned().unwrap_or_default();

            let duplicated_trie_messages = [trie_messages.clone(), trie_messages.clone()].concat();

            let response = GetShardTransactionsResponse {
                trie_messages: duplicated_trie_messages,
                fid_account_roots,
                next_page_token: None, // No pagination, one big response.
            };

            mock_service
                .transactions_responses
                .lock()
                .unwrap()
                .insert((shard_id, height, vts, None), Ok(response));
        }

        // --- Start the Mock Server ---
        let (addr, shutdown_tx) = spawn_mock_server(mock_service).await;
        let server_addr = format!("http://{}", addr);

        // --- Configure and run the bootstrap client against the mock server ---
        let dest_rocksdb_dir = format!("{}/dest_duplicates", tmp_dir.path().to_str().unwrap());

        let mut config = Config::default();
        config.rocksdb_dir = dest_rocksdb_dir.clone();
        config.consensus.shard_ids = vec![shard_id];
        config.fc_network = source_engine.network.clone();
        config.snapshot.replication_peers = vec![server_addr];

        let bootstrap = ReplicatorBootstrap::new(statsd_client(), &config);

        // 3. Perform the bootstrap. We expect this to succeed despite the duplicate messages.
        let result = bootstrap.bootstrap_using_replication().await;

        // Assert that bootstrap succeeded.
        match &result {
            Ok(WorkUnitResponse::Finished) => {}
            _ => panic!("Bootstrap failed with result: {:?}", result),
        }

        // 3. Verify that the roots match. This proves the duplicates were handled idempotently.
        let source_root = source_engine.get_stores().trie.root_hash().unwrap();

        // Open the destination DB and check its root hash.
        let dest_db = RocksDB::open_shard_db(&dest_rocksdb_dir, shard_id);
        let mut dest_trie = MerkleTrie::new().unwrap();
        dest_trie.initialize(&dest_db).unwrap();
        let dest_root = dest_trie.root_hash().unwrap();

        assert!(!source_root.is_empty(), "Source root should not be empty");
        assert_eq!(
            source_root, dest_root,
            "Source and destination trie roots must match"
        );

        // Cleanup
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn test_replication_missing_messages() {
        // 1. Create an engine with test data using the existing helper.
        let (tmp_dir, source_engine, replication_server, _signer, _fid, _fid2) =
            setup_source_engine_with_test_data().await;

        // 2. Fetch the actual data from the real replication server to populate our mock.
        let height = source_engine.get_confirmed_height().block_number;
        let shard_id = source_engine.shard_id();
        let (vts_data, snapshot_metadata) =
            fetch_transactions(&replication_server, shard_id, height)
                .await
                .unwrap();

        // --- Setup the Mock Server ---
        let mock_service = MockReplicationService::default();

        // 2a. Mock the metadata response.
        mock_service
            .metadata_responses
            .lock()
            .unwrap()
            .insert(shard_id, Ok(snapshot_metadata));

        // 2b. For each vts, withhold the last message from trie_messages if size > 0 and set the response
        for vts in 0..256 {
            let (mut trie_messages, fid_account_roots) =
                vts_data.get(&vts).cloned().unwrap_or_default();

            trie_messages.pop(); // Remove the last message

            let response = GetShardTransactionsResponse {
                trie_messages,
                fid_account_roots,
                next_page_token: None, // No pagination, one big response.
            };

            mock_service
                .transactions_responses
                .lock()
                .unwrap()
                .insert((shard_id, height, vts, None), Ok(response));
        }

        // --- Start the Mock Server ---
        let (addr, shutdown_tx) = spawn_mock_server(mock_service).await;
        let server_addr = format!("http://{}", addr);

        // --- Configure and run the bootstrap client against the mock server ---
        let dest_rocksdb_dir = format!("{}/dest_missing", tmp_dir.path().to_str().unwrap());

        let mut config = Config::default();
        config.rocksdb_dir = dest_rocksdb_dir.clone();
        config.consensus.shard_ids = vec![shard_id];
        config.fc_network = source_engine.network.clone();
        config.snapshot.replication_peers = vec![server_addr];

        let bootstrap = ReplicatorBootstrap::new(statsd_client(), &config);

        // 3. Perform the bootstrap. We expect this to fail due to missing messages.
        let result = bootstrap.bootstrap_using_replication().await;

        // Assert that bootstrap failed with AccountRootMismatch error.
        assert!(matches!(
            result.unwrap_err(),
            BootstrapError::AccountRootMismatch(_)
        ));

        // Cleanup
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn test_replication_wrong_fid_account_root() {
        // 1. Create an engine with test data using the existing helper.
        let (tmp_dir, source_engine, replication_server, _signer, _fid, _fid2) =
            setup_source_engine_with_test_data().await;

        // 2. Fetch the actual data from the real replication server to populate our mock.
        let height = source_engine.get_confirmed_height().block_number;
        let shard_id = source_engine.shard_id();
        let (vts_data, snapshot_metadata) =
            fetch_transactions(&replication_server, shard_id, height)
                .await
                .unwrap();

        // --- Setup the Mock Server ---
        let mock_service = MockReplicationService::default();

        // 2a. Mock the metadata response.
        mock_service
            .metadata_responses
            .lock()
            .unwrap()
            .insert(shard_id, Ok(snapshot_metadata));

        // 2b. For each vts, change the fid_account_roots to have random 12-byte account_root_hash and set the response
        for vts in 0..256 {
            let (trie_messages, mut fid_account_roots) =
                vts_data.get(&vts).cloned().unwrap_or_default();

            // Modify each fid_account_root to have a random 32-byte account_root_hash
            for root in &mut fid_account_roots {
                root.account_root_hash = random::<[u8; 32]>().to_vec();
            }

            let response = GetShardTransactionsResponse {
                trie_messages,
                fid_account_roots,
                next_page_token: None, // No pagination, one big response.
            };

            mock_service
                .transactions_responses
                .lock()
                .unwrap()
                .insert((shard_id, height, vts, None), Ok(response));
        }

        // --- Start the Mock Server ---
        let (addr, shutdown_tx) = spawn_mock_server(mock_service).await;
        let server_addr = format!("http://{}", addr);

        // --- Configure and run the bootstrap client against the mock server ---
        let dest_rocksdb_dir = format!("{}/dest_wrong_roots", tmp_dir.path().to_str().unwrap());

        let mut config = Config::default();
        config.rocksdb_dir = dest_rocksdb_dir.clone();
        config.consensus.shard_ids = vec![shard_id];
        config.fc_network = source_engine.network.clone();
        config.snapshot.replication_peers = vec![server_addr];

        let bootstrap = ReplicatorBootstrap::new(statsd_client(), &config);

        // 3. Perform the bootstrap. We expect this to fail due to wrong account roots.
        let result = bootstrap.bootstrap_using_replication().await;

        // Assert that bootstrap failed with AccountRootMismatch error.
        assert!(matches!(
            result.unwrap_err(),
            BootstrapError::AccountRootMismatch(_)
        ));

        // Cleanup
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn test_replication_wrong_trie_key() {
        // 1. Create an engine with test data using the existing helper.
        let (tmp_dir, source_engine, replication_server, _signer, _fid, _fid2) =
            setup_source_engine_with_test_data().await;

        // 2. Fetch the actual data from the real replication server to populate our mock.
        let height = source_engine.get_confirmed_height().block_number;
        let shard_id = source_engine.shard_id();
        let (vts_data, snapshot_metadata) =
            fetch_transactions(&replication_server, shard_id, height)
                .await
                .unwrap();

        // --- Setup the Mock Server ---
        let mock_service = MockReplicationService::default();

        // 2a. Mock the metadata response.
        mock_service
            .metadata_responses
            .lock()
            .unwrap()
            .insert(shard_id, Ok(snapshot_metadata));

        // 2b. For each vts, change the trie_messages to have random 12-byte trie_key and set the response
        for vts in 0..256 {
            let (mut trie_messages, fid_account_roots) =
                vts_data.get(&vts).cloned().unwrap_or_default();

            // Modify each trie_message to have a random 12-byte trie_key
            for message in &mut trie_messages {
                message.trie_key = random::<[u8; 12]>().to_vec();
            }

            let response = GetShardTransactionsResponse {
                trie_messages,
                fid_account_roots,
                next_page_token: None, // No pagination, one big response.
            };

            mock_service
                .transactions_responses
                .lock()
                .unwrap()
                .insert((shard_id, height, vts, None), Ok(response));
        }

        // --- Start the Mock Server ---
        let (addr, shutdown_tx) = spawn_mock_server(mock_service).await;
        let server_addr = format!("http://{}", addr);

        // --- Configure and run the bootstrap client against the mock server ---
        let dest_rocksdb_dir = format!("{}/dest_wrong_trie_keys", tmp_dir.path().to_str().unwrap());

        let mut config = Config::default();
        config.rocksdb_dir = dest_rocksdb_dir.clone();
        config.consensus.shard_ids = vec![shard_id];
        config.fc_network = source_engine.network.clone();
        config.snapshot.replication_peers = vec![server_addr];

        let bootstrap = ReplicatorBootstrap::new(statsd_client(), &config);

        // 3. Perform the bootstrap. We expect this to fail due to wrong trie keys.
        let result = bootstrap.bootstrap_using_replication().await;

        // Assert that bootstrap failed with "Generated trie key mismatch".
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("does not match expected trie key"));

        // Cleanup
        let _ = shutdown_tx.send(());
    }
}
