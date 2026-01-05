#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tokio::sync::{broadcast, mpsc, oneshot};

    use crate::{
        consensus::consensus::SystemMessage,
        core::util::to_farcaster_time,
        mempool::mempool::{self, Mempool, MempoolMessagesRequest},
        network::gossip::{Config, SnapchainGossip},
        proto::{
            self, Block, BlockHeader, FarcasterNetwork, Height, ShardChunk, ShardHeader,
            StorageUnitType, Transaction,
        },
        storage::store::{
            block_engine::BlockEngine,
            block_engine_test_helpers,
            engine::ShardEngine,
            mempool_poller::MempoolMessage,
            test_helper::{self, commit_event, default_storage_event, FID_FOR_TEST},
        },
        utils::{
            factory::{
                events_factory,
                messages_factory::{self, storage_lend::create_storage_lend},
            },
            statsd_wrapper::StatsdClientWrapper,
        },
    };

    use self::test_helper::{default_custody_address, default_signer};

    use std::time::Duration;

    use crate::mempool::mempool::{MempoolRequest, MempoolSource};
    use crate::mempool::routing::{MessageRouter, ShardRouter};
    use crate::utils::factory::username_factory;
    use libp2p::identity::ed25519::Keypair;
    use messages_factory::casts::create_cast_add;

    const HOST_FOR_TEST: &str = "127.0.0.1";
    const PORT_FOR_TEST: u32 = 9388;

    async fn setup(
        config: Option<Config>,
        enable_rate_limits: bool,
        num_shards: u32,
    ) -> (
        HashMap<u32, ShardEngine>,
        BlockEngine,
        Option<SnapchainGossip>,
        Mempool,
        mpsc::Sender<MempoolRequest>,
        mpsc::Sender<MempoolMessagesRequest>,
        broadcast::Sender<ShardChunk>,
        broadcast::Sender<Block>,
        mpsc::Receiver<SystemMessage>,
    ) {
        let keypair = Keypair::generate();
        let statsd_client = StatsdClientWrapper::new(
            cadence::StatsdClient::builder("", cadence::NopMetricSink {}).build(),
            true,
        );

        let (system_tx, system_rx) = mpsc::channel::<SystemMessage>(100);
        let (mempool_tx, mempool_rx) = mpsc::channel(100);
        let (messages_request_tx, messages_request_rx) = mpsc::channel(100);
        let (shard_decision_tx, shard_decision_rx) = broadcast::channel(100);
        let (block_decision_tx, block_decision_rx) = broadcast::channel(100);
        let mut shard_senders = HashMap::new();
        let mut shard_stores = HashMap::new();
        let mut engines = HashMap::new();
        for i in 1..num_shards + 1 {
            let (engine, _) = test_helper::new_engine().await;
            shard_senders.insert(i, engine.get_senders());
            shard_stores.insert(i, engine.get_stores());
            engines.insert(i, engine);
        }

        let (block_engine, _) = block_engine_test_helpers::setup();

        let gossip = match config {
            Some(config) => Some(
                SnapchainGossip::create(
                    keypair.clone(),
                    &config,
                    Some(system_tx),
                    false,
                    proto::FarcasterNetwork::Devnet,
                    statsd_client.clone(),
                    Vec::new(),
                )
                .await
                .unwrap(),
            ),
            None => None,
        };
        let gossip_tx = match &gossip {
            Some(gossip) => gossip.tx.clone(),
            None => mpsc::channel(100).0,
        };

        let mut mempool_config = mempool::Config::default();
        mempool_config.enable_rate_limits = enable_rate_limits;
        let mempool = Mempool::new(
            mempool_config,
            FarcasterNetwork::Devnet,
            mempool_rx,
            messages_request_rx,
            num_shards,
            shard_stores,
            block_engine.stores(),
            gossip_tx,
            shard_decision_rx,
            block_decision_rx,
            statsd_client,
        );

        (
            engines,
            block_engine,
            gossip,
            mempool,
            mempool_tx,
            messages_request_tx,
            shard_decision_tx,
            block_decision_tx,
            system_rx,
        )
    }

    async fn pull_message(
        messages_request_tx: &mpsc::Sender<MempoolMessagesRequest>,
        shard_id: u32,
        expected_message: Option<MempoolMessage>,
    ) {
        // Setup channel to retrieve messages
        let (mempool_retrieval_tx, mempool_retrieval_rx) = oneshot::channel();

        // Query mempool for the messages
        messages_request_tx
            .send(MempoolMessagesRequest {
                shard_id,
                max_messages_per_block: 1,
                message_tx: mempool_retrieval_tx,
            })
            .await
            .unwrap();

        let result = mempool_retrieval_rx.await.unwrap();
        if let Some(expected_message) = expected_message {
            assert_eq!(result.len(), 1);
            assert_eq!(result[0], expected_message)
        } else {
            assert_eq!(result.len(), 0)
        }
    }

    #[tokio::test]
    async fn test_duplicate_user_message_is_invalid() {
        let (mut engines, _, _, mut mempool, _, _, _, _, _) = setup(None, false, 1).await;
        let mut engine = engines.get_mut(&1).unwrap();
        test_helper::register_user(
            1234,
            default_signer(),
            default_custody_address(),
            &mut engine,
        )
        .await;
        let cast = create_cast_add(1234, "hello", None, None);
        let valid = mempool.message_is_valid(1, &MempoolMessage::UserMessage(cast.clone()));
        assert!(valid.is_ok());
        test_helper::commit_message(&mut engine, &cast).await;
        let valid = mempool.message_is_valid(1, &MempoolMessage::UserMessage(cast.clone()));
        assert!(!valid.is_ok())
    }

    #[tokio::test]
    async fn test_duplicate_block_event_is_invalid() {
        let (mut engines, _, _, mut mempool, _, _, _, _, _) = setup(None, false, 1).await;
        let mut engine = engines.get_mut(&1).unwrap();
        let block_event = events_factory::create_heartbeat_event(1);
        let valid = mempool.message_is_valid(
            1,
            &MempoolMessage::BlockEvent {
                for_shard: 1,
                message: block_event.clone(),
            },
        );
        assert!(valid.is_ok());

        test_helper::commit_block_events(&mut engine, vec![&block_event]).await;
        let valid = mempool.message_is_valid(
            1,
            &MempoolMessage::BlockEvent {
                for_shard: 1,
                message: block_event.clone(),
            },
        );
        assert!(!valid.is_ok())
    }

    #[tokio::test]
    async fn test_duplicate_onchain_event_is_valid() {
        let (mut engines, _, _, mut mempool, _, _, _, _, _) = setup(None, false, 1).await;
        let mut engine = engines.get_mut(&1).unwrap();
        let onchain_event = events_factory::create_rent_event(
            1234,
            10,
            proto::StorageUnitType::UnitType2025,
            false,
            proto::FarcasterNetwork::Devnet,
        );
        let valid =
            mempool.message_is_valid(1, &MempoolMessage::OnchainEvent(onchain_event.clone()));
        assert!(valid.is_ok());
        test_helper::commit_event(&mut engine, &onchain_event).await;
        let valid =
            mempool.message_is_valid(1, &MempoolMessage::OnchainEvent(onchain_event.clone()));
        // Mempool allows duplicate on-chain events
        assert!(valid.is_ok())
    }

    #[tokio::test]
    async fn test_duplicate_fname_transfer_is_valid() {
        let (mut engines, _, _, mut mempool, _, _, _, _, _) = setup(None, false, 1).await;
        let mut engine = engines.get_mut(&1).unwrap();
        test_helper::register_user(
            1,
            default_signer(),
            hex::decode("8773442740c17c9d0f0b87022c722f9a136206ed").unwrap(),
            &mut engine,
        )
        .await;
        let signer = alloy_signer_local::PrivateKeySigner::random();
        let fname_transfer =
            username_factory::create_transfer(1, "farcaster", None, None, None, signer.clone());
        let valid =
            mempool.message_is_valid(1, &MempoolMessage::FnameTransfer(fname_transfer.clone()));
        assert!(valid.is_ok());
        test_helper::commit_fname_transfer(&mut engine, &fname_transfer).await;

        // Transferring the same fname again should be valid
        let fname_transfer =
            username_factory::create_transfer(2, "farcaster", None, Some(1), None, signer);
        let valid = mempool.message_is_valid(1, &MempoolMessage::FnameTransfer(fname_transfer));
        assert!(valid.is_ok())
    }

    #[tokio::test]
    async fn test_rate_limits_applied() {
        let (mut engines, _, _, mut mempool, _, _, _, _, _) = setup(None, true, 1).await;
        let mut engine = engines.get_mut(&1).unwrap();

        let id_register_event = events_factory::create_id_register_event(
            FID_FOR_TEST,
            proto::IdRegisterEventType::Register,
            default_custody_address(),
            None,
        );
        commit_event(&mut engine, &id_register_event).await;
        let signer_event = events_factory::create_signer_event(
            FID_FOR_TEST,
            default_signer(),
            proto::SignerEventType::Add,
            None,
            None,
        );
        commit_event(&mut engine, &signer_event).await;

        let cast = create_cast_add(FID_FOR_TEST, "hello", None, None);
        let valid = mempool.message_is_valid(1, &MempoolMessage::UserMessage(cast));
        assert!(!valid.is_ok());

        commit_event(&mut engine, &default_storage_event(FID_FOR_TEST)).await;

        let cast = create_cast_add(FID_FOR_TEST, "hello", None, None);
        let valid = mempool.message_is_valid(1, &MempoolMessage::UserMessage(cast));
        assert!(valid.is_ok());
    }

    #[tokio::test]
    async fn test_copying_fname_transfer() {
        let (
            _,
            _,
            _,
            mut mempool,
            mempool_tx,
            _request_tx,
            _shard_decision_tx,
            _block_decision_tx,
            _,
        ) = setup(None, false, 2).await;
        tokio::spawn(async move {
            mempool.run().await;
        });

        let first_fid = 20270;
        let second_fid = 211428;
        // Ensure that the two fids are routed to different shards
        let router = ShardRouter {};
        assert_ne!(
            router.route_fid(first_fid, 2),
            router.route_fid(second_fid, 2)
        );

        // Make sure both transfers are routed to both shards
        let signer = alloy_signer_local::PrivateKeySigner::random();
        let fname_transfer = username_factory::create_transfer(
            first_fid,
            "farcaster",
            None,
            Some(second_fid),
            None,
            signer.clone(),
        );
        mempool_tx
            .send(MempoolRequest::AddMessage(
                MempoolMessage::FnameTransfer(fname_transfer.clone()),
                MempoolSource::Local,
                None,
            ))
            .await
            .unwrap();

        let fname_transfer = username_factory::create_transfer(
            second_fid,
            "farcaster",
            None,
            Some(first_fid),
            None,
            signer,
        );
        mempool_tx
            .send(MempoolRequest::AddMessage(
                MempoolMessage::FnameTransfer(fname_transfer.clone()),
                MempoolSource::Local,
                None,
            ))
            .await
            .unwrap();

        let (req, res) = oneshot::channel();
        mempool_tx.send(MempoolRequest::GetSize(req)).await.unwrap();
        let size = res.await.unwrap();
        assert_eq!(*size.get(&1).unwrap(), 2);
        assert_eq!(*size.get(&2).unwrap(), 2);
    }

    #[tokio::test]
    async fn test_mempool_size() {
        let (
            _,
            _,
            _,
            mut mempool,
            mempool_tx,
            _request_tx,
            _shard_decision_tx,
            _block_decision_tx,
            _,
        ) = setup(None, false, 1).await;
        tokio::spawn(async move {
            mempool.run().await;
        });

        let (req, res) = oneshot::channel();
        mempool_tx.send(MempoolRequest::GetSize(req)).await.unwrap();
        let size = res.await.unwrap();
        assert_eq!(size.len(), 0);

        mempool_tx
            .send(MempoolRequest::AddMessage(
                MempoolMessage::UserMessage(create_cast_add(123, "hello", None, None)),
                MempoolSource::Local,
                None,
            ))
            .await
            .unwrap();
        mempool_tx
            .send(MempoolRequest::AddMessage(
                MempoolMessage::UserMessage(create_cast_add(435, "hello2", None, None)),
                MempoolSource::Local,
                None,
            ))
            .await
            .unwrap();

        let (req, res) = oneshot::channel();
        mempool_tx.send(MempoolRequest::GetSize(req)).await.unwrap();
        let size = res.await.unwrap();
        assert_eq!(size.len(), 1);
        assert_eq!(size[&1], 2);
    }

    #[tokio::test]
    async fn test_mempool_prioritization() {
        let (
            _,
            _,
            _,
            mut mempool,
            mempool_tx,
            messages_request_tx,
            _shard_decision_tx,
            _block_decision_tx,
            _,
        ) = setup(None, false, 1).await;

        // Spawn mempool task
        tokio::spawn(async move {
            mempool.run().await;
        });

        let fid = 1234;
        // Cast has lower timestamp and arrives first, but onchain event is still processed first
        let onchain_event = events_factory::create_rent_event(
            fid,
            1,
            proto::StorageUnitType::UnitType2025,
            false,
            proto::FarcasterNetwork::Devnet,
        );

        let cast = create_cast_add(
            fid,
            "hello",
            Some(to_farcaster_time(onchain_event.block_timestamp * 1000).unwrap() as u32 - 1),
            None,
        );

        mempool_tx
            .send(MempoolRequest::AddMessage(
                MempoolMessage::UserMessage(cast.clone()),
                MempoolSource::Local,
                None,
            ))
            .await
            .unwrap();

        mempool_tx
            .send(MempoolRequest::AddMessage(
                MempoolMessage::OnchainEvent(onchain_event.clone()),
                MempoolSource::Local,
                None,
            ))
            .await
            .unwrap();

        // Wait for processing
        tokio::time::sleep(Duration::from_secs(1)).await;

        pull_message(
            &messages_request_tx,
            1,
            Some(MempoolMessage::OnchainEvent(onchain_event)),
        )
        .await;
        pull_message(
            &messages_request_tx,
            1,
            Some(MempoolMessage::UserMessage(cast.clone())),
        )
        .await;
    }

    #[tokio::test]
    async fn test_mempool_eviction() {
        let (
            mut engines,
            _,
            _,
            mut mempool,
            mempool_tx,
            messages_request_tx,
            shard_decision_tx,
            _block_decision_tx,
            _,
        ) = setup(None, false, 1).await;
        let mut engine = engines.get_mut(&1).unwrap();
        test_helper::register_user(
            1234,
            default_signer(),
            default_custody_address(),
            &mut engine,
        )
        .await;

        // Spawn mempool task
        tokio::spawn(async move {
            mempool.run().await;
        });

        let fid = 1234;

        let cast1 = create_cast_add(fid, "hello", None, None);
        let cast2 = create_cast_add(fid, "world", None, None);

        let _ = mempool_tx
            .send(MempoolRequest::AddMessage(
                MempoolMessage::UserMessage(cast1.clone()),
                MempoolSource::Local,
                None,
            ))
            .await;
        let _ = mempool_tx
            .send(MempoolRequest::AddMessage(
                MempoolMessage::UserMessage(cast2),
                MempoolSource::Local,
                None,
            ))
            .await;

        // Wait for cast processing
        tokio::time::sleep(Duration::from_secs(1)).await;

        let transaction = Transaction {
            fid,
            user_messages: vec![cast1],
            system_messages: vec![],
            account_root: vec![],
        };

        let header = ShardHeader {
            height: Some(Height {
                shard_index: 1,
                block_number: 1,
            }),
            timestamp: 0,
            parent_hash: vec![],
            shard_root: vec![],
        };

        // Create fake chunk with cast1
        let chunk = ShardChunk {
            header: Some(header),
            hash: vec![],
            transactions: vec![transaction],
            commits: None,
        };

        let _ = shard_decision_tx.send(chunk);

        // Wait for chunk processing
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Setup channel to retrieve messages
        let (mempool_retrieval_tx, mempool_retrieval_rx) = oneshot::channel();

        // Query mempool for the messages
        messages_request_tx
            .send(MempoolMessagesRequest {
                shard_id: 1,
                max_messages_per_block: 2,
                message_tx: mempool_retrieval_tx,
            })
            .await
            .unwrap();

        let result = mempool_retrieval_rx.await.unwrap();
        // We expect one of the added casts to have been evicted
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].fid(), fid);
    }
    #[tokio::test]
    async fn test_mempool_eviction_shard0() {
        let (
            _engines,
            mut block_engine,
            _,
            mut mempool,
            mempool_tx,
            messages_request_tx,
            _shard_decision_tx,
            block_decision_tx,
            _,
        ) = setup(None, false, 1).await;
        block_engine_test_helpers::register_user(
            1234,
            default_signer(),
            default_custody_address(),
            5,
            &mut block_engine,
        );

        // Spawn mempool task
        tokio::spawn(async move {
            mempool.run().await;
        });

        let fid = 1234;

        let storage_lend1 = create_storage_lend(
            fid,
            fid + 1,
            1,
            StorageUnitType::UnitType2025,
            Some(1),
            None,
        );

        let storage_lend2 = create_storage_lend(
            fid,
            fid + 1,
            1,
            StorageUnitType::UnitType2025,
            Some(2),
            None,
        );

        let _ = mempool_tx
            .send(MempoolRequest::AddMessage(
                MempoolMessage::UserMessage(storage_lend1.clone()),
                MempoolSource::Local,
                None,
            ))
            .await;

        let _ = mempool_tx
            .send(MempoolRequest::AddMessage(
                MempoolMessage::UserMessage(storage_lend2.clone()),
                MempoolSource::Local,
                None,
            ))
            .await;

        // Wait for processing
        tokio::time::sleep(Duration::from_secs(1)).await;

        let transaction = Transaction {
            fid,
            user_messages: vec![storage_lend1],
            system_messages: vec![],
            account_root: vec![],
        };

        let header = BlockHeader {
            height: Some(Height {
                shard_index: 0,
                block_number: 1,
            }),
            ..Default::default()
        };

        // Create fake block
        let block = Block {
            header: Some(header),
            transactions: vec![transaction],
            ..Default::default()
        };

        let _ = block_decision_tx.send(block);

        // Wait for block processing
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Setup channel to retrieve messages
        let (mempool_retrieval_tx, mempool_retrieval_rx) = oneshot::channel();

        // Query mempool for the messages
        messages_request_tx
            .send(MempoolMessagesRequest {
                shard_id: 0,
                max_messages_per_block: 2,
                message_tx: mempool_retrieval_tx,
            })
            .await
            .unwrap();

        let result = mempool_retrieval_rx.await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].mempool_key().identity(), storage_lend2.hex_hash())
    }

    #[tokio::test]
    async fn test_mempool_gossip() {
        // Create configs with different ports
        let node1_addr = format!("/ip4/{HOST_FOR_TEST}/udp/{PORT_FOR_TEST}/quic-v1");
        let node2_port = PORT_FOR_TEST + 1;
        let node2_addr = format!("/ip4/{HOST_FOR_TEST}/udp/{node2_port}/quic-v1");
        let config1 = Config::new(node1_addr.clone(), node2_addr.clone());
        let config2 = Config::new(node2_addr.clone(), node1_addr.clone());

        let (
            _,
            _,
            gossip1,
            mut mempool1,
            mempool_tx1,
            _mempool_requests_tx1,
            _shard_decision_tx1,
            _block_decision_tx1,
            _,
        ) = setup(Some(config1), false, 1).await;
        let (
            _,
            _,
            gossip2,
            mut mempool2,
            mempool_tx2,
            mempool_requests_tx2,
            _shard_decision_tx1,
            _block_decision_tx1,
            mut system_rx2,
        ) = setup(Some(config2), false, 1).await;

        // Spawn gossip tasks
        tokio::spawn(async move {
            gossip1.unwrap().start().await;
        });
        tokio::spawn(async move {
            gossip2.unwrap().start().await;
        });

        // Spawn mempool tasks
        tokio::spawn(async move {
            mempool1.run().await;
        });
        tokio::spawn(async move {
            mempool2.run().await;
        });

        // Wait for connection to establish
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Create a test message
        let cast: proto::Message = create_cast_add(1234, "hello", None, None);
        let cast2: proto::Message = create_cast_add(3214, "hello 2", None, None);

        // Add message to mempool 1
        mempool_tx1
            .send(MempoolRequest::AddMessage(
                MempoolMessage::UserMessage(cast.clone()),
                MempoolSource::Local,
                None,
            ))
            .await
            .unwrap();

        // Inserting the same message twice should not be re-broadcasted
        mempool_tx1
            .send(MempoolRequest::AddMessage(
                MempoolMessage::UserMessage(cast),
                MempoolSource::Local,
                None,
            ))
            .await
            .unwrap();

        // Another message received via gossip should not be re-broadcasted
        mempool_tx1
            .send(MempoolRequest::AddMessage(
                MempoolMessage::UserMessage(cast2),
                MempoolSource::Gossip,
                None,
            ))
            .await
            .unwrap();

        // Wait for gossip
        tokio::time::sleep(Duration::from_secs(1)).await;

        let mut received_messages = 0;
        // Should be received through the system message of gossip 2
        while let Ok(msg) = system_rx2.try_recv() {
            if let SystemMessage::Mempool(mempool_message) = msg {
                // Manually forward to the mempool
                mempool_tx2.send(mempool_message).await.unwrap();
                received_messages += 1;
            }
        }
        assert_eq!(received_messages, 1);

        // Wait for the message to be processed
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Setup channel to retrieve message
        let (mempool_retrieval_tx, mempool_retrieval_rx) = oneshot::channel();

        // Query mempool 2 for the message
        mempool_requests_tx2
            .send(MempoolMessagesRequest {
                shard_id: 1,
                max_messages_per_block: 1,
                message_tx: mempool_retrieval_tx,
            })
            .await
            .unwrap();

        let result = mempool_retrieval_rx.await.unwrap();
        assert_eq!(result.len(), 1); // Only the first cast should be received
        assert_eq!(result[0].fid(), 1234);
    }

    #[tokio::test]
    async fn test_mempool_error() {
        let (
            mut engines,
            _,
            _,
            mut mempool,
            mempool_tx,
            _request_tx,
            _shard_decision_tx,
            _block_decision_tx,
            _,
        ) = setup(None, false, 1).await;
        let mut engine = engines.get_mut(&1).unwrap();

        test_helper::register_user(
            1234,
            default_signer(),
            default_custody_address(),
            &mut engine,
        )
        .await;

        tokio::spawn(async move {
            mempool.run().await;
        });

        let message = create_cast_add(1234, "hello", None, None);

        test_helper::commit_message(&mut engine, &message.clone()).await;

        let (req, res) = oneshot::channel();

        mempool_tx
            .send(MempoolRequest::AddMessage(
                MempoolMessage::UserMessage(message),
                MempoolSource::Local,
                Some(req),
            ))
            .await
            .unwrap();

        let result = res.await.unwrap();

        let error = result.unwrap_err();
        assert_eq!(error.code, "bad_request.duplicate");
    }

    #[tokio::test]
    async fn test_onchain_event_for_migration_routing() {
        // Setup with 2 shards
        let (
            _,
            _,
            _,
            mut mempool,
            mempool_tx,
            messages_request_tx,
            _shard_decision_tx,
            _block_decision_tx,
            _,
        ) = setup(None, false, 2).await;

        tokio::spawn(async move {
            mempool.run().await;
        });

        let fid = 1234;
        // Ensure this FID routes to shard 2
        let router = ShardRouter {};
        assert_eq!(router.route_fid(fid, 2), 1);

        let onchain_event = events_factory::create_rent_event(
            fid,
            10,
            proto::StorageUnitType::UnitType2025,
            false,
            proto::FarcasterNetwork::Devnet,
        );

        mempool_tx
            .send(MempoolRequest::AddMessage(
                MempoolMessage::OnchainEventForMigration(onchain_event.clone()),
                MempoolSource::Local,
                None,
            ))
            .await
            .unwrap();

        // Wait for processing
        tokio::time::sleep(Duration::from_millis(100)).await;

        pull_message(
            &messages_request_tx,
            0,
            Some(MempoolMessage::OnchainEventForMigration(
                onchain_event.clone(),
            )),
        )
        .await;
        pull_message(&messages_request_tx, 1, None).await;
    }
}
