#[cfg(test)]
mod tests {
    use crate::{
        bootstrap::service::{ReplicatorBootstrap, WorkUnitResponse},
        cfg::Config,
        proto::{self, replication_service_server::ReplicationServiceServer},
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
    use std::time::Duration;
    use tempfile::TempDir;
    use tonic::transport::Server;

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
        config.trie_branching_factor = 16;
        config.statsd.addr = "127.0.0.1:8125".to_string();
        config.statsd.prefix = "test".to_string();
        config.statsd.use_tags = true;

        let bootstrap = ReplicatorBootstrap::new(&config);

        // Perform the bootstrap
        let result = bootstrap.bootstrap_using_replication(server_addr).await;

        // Assert that bootstrap succeeded
        match &result {
            Ok(WorkUnitResponse::Finished) => {}
            _ => panic!("Bootstrap failed with result: {:?}", result),
        }

        // Now, check that the shard roots match
        let source_root = source_engine.get_stores().trie.root_hash().unwrap();

        // Open the destination DB and check the root
        let dest_db = RocksDB::open_shard_db(&dest_rocksdb_dir, 1);
        let mut dest_trie = MerkleTrie::new(16).unwrap();
        dest_trie.initialize(&dest_db).unwrap();
        let dest_root = dest_trie.root_hash().unwrap();

        assert_eq!(source_root, dest_root);
    }
}
