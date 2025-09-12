#[cfg(test)]
mod tests {
    use crate::{
        bootstrap::{
            replication_rpc_client::RpcClientsManager,
            service::{ReplicatorBootstrap, WorkUnitResponse},
        },
        cfg::Config,
        proto::{
            self,
            replication_service_server::{ReplicationService, ReplicationServiceServer},
            GetShardSnapshotMetadataResponse, GetShardTransactionsResponse, ShardSnapshotMetadata,
            ShardTrieEntryWithMessage,
        },
        replication::{
            replication_test_utils::replication_test_utils::*,
            replicator::{self},
        },
        storage::{
            db::RocksDB,
            store::{
                engine::PostCommitMessage,
                test_helper::{self},
            },
            trie::merkle_trie::MerkleTrie,
        },
        utils::factory::{self, messages_factory},
    };
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tokio::time::sleep;
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
                Err(Status::not_found("No metadata for this shard_id"))
            }
        }

        async fn get_shard_transactions(
            &self,
            request: Request<proto::GetShardTransactionsRequest>,
        ) -> Result<Response<GetShardTransactionsResponse>, Status> {
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
        let manager = RpcClientsManager::new(peer_addr.clone(), shard_id)
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
        let manager = RpcClientsManager::new(peer_addr, shard_id).await.unwrap();

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
        let manager = RpcClientsManager::new(format!("http://{}", addr1), shard_id)
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

        // Try to add the new peer
        let handle = manager.add_new_peer(format!("http://{}", addr2));

        let result = handle.await.unwrap();
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
        let manager = RpcClientsManager::new(format!("http://{}", addr), shard_id)
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
        let manager = RpcClientsManager::new(peer_addr_1.clone(), shard_id)
            .await
            .unwrap();
        let handle = manager.add_new_peer(peer_addr_2.clone());
        sleep(Duration::from_millis(100)).await; // allow add_peer to complete

        let result = handle.await.unwrap();
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

    #[tokio::test]
    async fn test_replication_client_bootstrap() {
        // Open tmp dir for database
        let tmp_dir = TempDir::new().unwrap();

        let (post_commit_tx, post_commit_rx) = tokio::sync::mpsc::channel::<PostCommitMessage>(1);

        let (signer, mut source_engine) =
            new_engine_with_fname_signer(&tmp_dir, Some(post_commit_tx)).await; // source engine

        let (replicator, replication_server) = setup_replicator(&mut source_engine);
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
        config.statsd.addr = "127.0.0.1:8125".to_string();
        config.statsd.prefix = "test".to_string();
        config.statsd.use_tags = true;
        config.snapshot.replication_peers = vec![server_addr];

        let bootstrap = ReplicatorBootstrap::new(&config);

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
}
