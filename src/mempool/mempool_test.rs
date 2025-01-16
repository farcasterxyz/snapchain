#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tokio::sync::{broadcast, mpsc, oneshot};

    use crate::{
        consensus::consensus::SystemMessage,
        mempool::mempool::{Mempool, MempoolMessagesRequest},
        network::gossip::{Config, SnapchainGossip},
        proto::{self, FnameTransfer, UserNameProof, UserNameType, ValidatorMessage},
        storage::store::{
            engine::{MempoolMessage, ShardEngine},
            test_helper,
        },
        utils::{
            factory::{events_factory, messages_factory},
            statsd_wrapper::StatsdClientWrapper,
        },
    };

    use self::test_helper::{default_custody_address, default_signer};

    use std::time::Duration;

    use libp2p::identity::ed25519::Keypair;

    fn setup() -> (ShardEngine, Mempool) {
        let statsd_client = StatsdClientWrapper::new(
            cadence::StatsdClient::builder("", cadence::NopMetricSink {}).build(),
            true,
        );

        let (_mempool_tx, mempool_rx) = mpsc::channel(100);
        let (_mempool_tx, messages_request_rx) = mpsc::channel(100);
        let (gossip_tx, _gossip_rx) = mpsc::channel(100);
        let (_shard_decision_tx, shard_decision_rx) = broadcast::channel(100);
        let (engine, _) = test_helper::new_engine();
        let mut shard_senders = HashMap::new();
        shard_senders.insert(1, engine.get_senders());
        let mut shard_stores = HashMap::new();
        shard_stores.insert(1, engine.get_stores());
        let mempool = Mempool::new(
            1024,
            mempool_rx,
            messages_request_rx,
            1,
            shard_stores,
            gossip_tx,
            shard_decision_rx,
            statsd_client,
        );
        (engine, mempool)
    }

    #[tokio::test]
    async fn test_duplicate_user_message_is_invalid() {
        let (mut engine, mut mempool) = setup();
        test_helper::register_user(
            1234,
            default_signer(),
            default_custody_address(),
            &mut engine,
        )
        .await;
        let cast = messages_factory::casts::create_cast_add(1234, "hello", None, None);
        let valid = mempool.message_is_valid(&MempoolMessage::UserMessage(cast.clone()));
        assert!(valid);
        test_helper::commit_message(&mut engine, &cast).await;
        let valid = mempool.message_is_valid(&MempoolMessage::UserMessage(cast.clone()));
        assert!(!valid)
    }

    #[tokio::test]
    async fn test_duplicate_onchain_event_is_invalid() {
        let (mut engine, mut mempool) = setup();
        let onchain_event = events_factory::create_rent_event(1234, Some(10), None, false);
        let valid = mempool.message_is_valid(&MempoolMessage::ValidatorMessage(ValidatorMessage {
            on_chain_event: Some(onchain_event.clone()),
            fname_transfer: None,
        }));
        assert!(valid);
        test_helper::commit_event(&mut engine, &onchain_event).await;
        let valid = mempool.message_is_valid(&MempoolMessage::ValidatorMessage(ValidatorMessage {
            on_chain_event: Some(onchain_event.clone()),
            fname_transfer: None,
        }));
        assert!(!valid)
    }

    #[tokio::test]
    async fn test_duplicate_fname_transfer_is_invalid() {
        let (mut engine, mut mempool) = setup();
        test_helper::register_user(
            1234,
            default_signer(),
            default_custody_address(),
            &mut engine,
        )
        .await;
        let fname_transfer = FnameTransfer {
            id: 1234,
            from_fid: 0,
            proof: Some(UserNameProof {
                timestamp: messages_factory::farcaster_time() as u64,
                name: "farcaster".as_bytes().to_vec(),
                owner: default_custody_address(),
                signature: "signature".as_bytes().to_vec(),
                fid: 1234,
                r#type: UserNameType::UsernameTypeEnsL1 as i32,
            }),
        };
        let valid = mempool.message_is_valid(&MempoolMessage::ValidatorMessage(ValidatorMessage {
            on_chain_event: None,
            fname_transfer: Some(fname_transfer.clone()),
        }));
        assert!(valid);
        test_helper::commit_fname_transfer(&mut engine, &fname_transfer).await;
        let valid = mempool.message_is_valid(&MempoolMessage::ValidatorMessage(ValidatorMessage {
            on_chain_event: None,
            fname_transfer: Some(fname_transfer),
        }));
        assert!(!valid)
    }

    const HOST_FOR_TEST: &str = "127.0.0.1";
    const PORT_FOR_TEST: u32 = 9388;

    fn setup_gossip_mempool(
        config: Config,
    ) -> (
        SnapchainGossip,
        Mempool,
        mpsc::Sender<MempoolMessage>,
        mpsc::Sender<MempoolMessagesRequest>,
    ) {
        let keypair = Keypair::generate();
        let statsd_client = StatsdClientWrapper::new(
            cadence::StatsdClient::builder("", cadence::NopMetricSink {}).build(),
            true,
        );

        let (system_tx, _) = mpsc::channel::<SystemMessage>(100);
        let (mempool_tx, mempool_rx) = mpsc::channel(100);
        let (messages_request_tx, messages_request_rx) = mpsc::channel(100);
        let (_shard_decision_tx, shard_decision_rx) = broadcast::channel(100);
        let (engine, _) = test_helper::new_engine();
        let mut shard_senders = HashMap::new();
        shard_senders.insert(1, engine.get_senders());
        let mut shard_stores = HashMap::new();
        shard_stores.insert(1, engine.get_stores());

        let gossip =
            SnapchainGossip::create(keypair.clone(), config, system_tx, mempool_tx.clone())
                .unwrap();

        let mempool = Mempool::new(
            1024,
            mempool_rx,
            messages_request_rx,
            1,
            shard_stores,
            gossip.tx.clone(),
            shard_decision_rx,
            statsd_client,
        );

        (gossip, mempool, mempool_tx.clone(), messages_request_tx)
    }

    #[tokio::test]
    async fn test_mempool_gossip() {
        // Create configs with different ports
        let node1_addr = format!("/ip4/{HOST_FOR_TEST}/udp/{PORT_FOR_TEST}/quic-v1");
        let node2_port = PORT_FOR_TEST + 1;
        let node2_addr = format!("/ip4/{HOST_FOR_TEST}/udp/{node2_port}/quic-v1");
        let config1 = Config::new(node1_addr.clone(), node2_addr.clone());
        let config2 = Config::new(node2_addr.clone(), node1_addr.clone());

        let (mut gossip1, mut mempool1, mempool_tx1, _mempool_requests_tx1) =
            setup_gossip_mempool(config1);
        let (mut gossip2, mut mempool2, _mempool_tx2, mempool_requests_tx2) =
            setup_gossip_mempool(config2);

        // Spawn gossip tasks
        tokio::spawn(async move {
            gossip1.start().await;
        });
        tokio::spawn(async move {
            gossip2.start().await;
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
        let cast: proto::Message =
            messages_factory::casts::create_cast_add(1234, "hello", None, None);

        // Add message to mempool 1
        mempool_tx1
            .send(MempoolMessage::UserMessage(cast))
            .await
            .unwrap();

        // Wait for gossip
        tokio::time::sleep(Duration::from_secs(5)).await;

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
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].fid(), 1234);
    }
}
