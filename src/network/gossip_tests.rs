use crate::consensus::consensus::SystemMessage;
use crate::core::types::SnapchainValidatorContext;
use crate::mempool::mempool::{MempoolRequest, MempoolSource};
use crate::network::gossip::{Config, GossipEvent, SnapchainGossip};
use crate::proto::{FarcasterNetwork, Message, MessageData};
use crate::storage::store::mempool_poller::MempoolMessage;
use crate::storage::store::test_helper::statsd_client;
use crate::utils::factory::messages_factory;
use libp2p::identity::ed25519::Keypair;
use prost::Message as _;
use serial_test::serial;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::{select, time};

const HOST_FOR_TEST: &str = "127.0.0.1";
const BASE_PORT_FOR_TEST: u32 = 9382;

/// Re-publishes `message` on `publisher_tx` every 200ms, polling `receiver` for
/// a matching `SystemMessage::Mempool(...)` delivery, and returns `true` as
/// soon as one arrives (or `false` once `deadline` elapses). Breaks the
/// publisher loop early if the gossip event channel closes — without that, a
/// dead gossip task would burn the full deadline before failing.
///
/// Repeat sends are safe: gossipsub message-id dedup ensures the receiver sees
/// each unique payload at most once. This is the right shape for tests where
/// the publisher hasn't yet formed a mesh with subscribed peers — the first
/// few `publish` calls can return `InsufficientPeers` and are only warn-logged
/// and dropped by `SnapchainGossip::publish`.
async fn publish_until_received(
    publisher_tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
    receiver: &mut mpsc::Receiver<SystemMessage>,
    message: Message,
    deadline: Duration,
) -> bool {
    let expected_hash = message.hash.clone();

    let cast_for_publisher = message;
    let publisher = tokio::spawn(async move {
        loop {
            if publisher_tx
                .send(GossipEvent::BroadcastMempoolMessage(
                    MempoolMessage::UserMessage(cast_for_publisher.clone()),
                ))
                .await
                .is_err()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });

    let end = tokio::time::Instant::now() + deadline;
    let received = loop {
        if tokio::time::Instant::now() >= end {
            break false;
        }
        match tokio::time::timeout(Duration::from_millis(200), receiver.recv()).await {
            Ok(Some(SystemMessage::Mempool(MempoolRequest::AddMessage(
                MempoolMessage::UserMessage(msg),
                MempoolSource::Gossip,
                _,
            )))) if msg.hash == expected_hash => break true,
            _ => {}
        }
    };

    publisher.abort();
    received
}

async fn wait_for_message(
    system_rx: &mut mpsc::Receiver<SystemMessage>,
    expected_message: Message,
) -> u32 {
    let deadline = time::Instant::now() + Duration::from_secs(2);
    let mut receive_counts = 0;
    loop {
        let timeout = time::sleep_until(deadline);
        select! {
            received = system_rx.recv()  => {
                match received {
                    Some(SystemMessage::Mempool(msg))  => {
                        match msg {
                            MempoolRequest::AddMessage(MempoolMessage::UserMessage(actual_message), source, _) => {
                                receive_counts += 1;
                                assert_eq!(actual_message.hash, expected_message.hash);
                                assert_eq!(actual_message.data.is_some(), true);
                                match &expected_message.data {
                                    Some(msg_data) => {
                                        assert_eq!(msg_data, &actual_message.data.unwrap());
                                    },
                                    None => {
                                        let msg_data = MessageData::decode(actual_message.data_bytes.as_ref().unwrap().as_slice()).unwrap();
                                        assert_eq!(msg_data, actual_message.data.unwrap());
                                    }
                                }
                                assert_eq!(source, MempoolSource::Gossip);
                            },
                            _ => {
                                panic!("Received unexpected message");
                            },
                        }
                    },
                    _ => {},
                }
            }
            _ = timeout => {
                break;
            }
        }
    }

    return receive_counts;
}

#[tokio::test]
#[serial]
async fn test_gossip_communication() {
    // Create two keypairs for our test nodes
    let keypair1 = Keypair::generate();
    let keypair2 = Keypair::generate();
    let keypair3 = Keypair::generate();

    // Create configs with different ports
    let node1_addr = format!("/ip4/{HOST_FOR_TEST}/udp/{BASE_PORT_FOR_TEST}/quic-v1");

    let node2_port = BASE_PORT_FOR_TEST + 1;
    let node2_addr = format!("/ip4/{HOST_FOR_TEST}/udp/{node2_port}/quic-v1");

    let node3_port = BASE_PORT_FOR_TEST + 2;
    let node3_addr = format!("/ip4/{HOST_FOR_TEST}/udp/{node3_port}/quic-v1");

    let config1 = Config::new(node1_addr.clone(), node2_addr.clone())
        .with_contact_info_interval(Duration::from_millis(100));
    let config2 = Config::new(node2_addr.clone(), node1_addr.clone())
        .with_contact_info_interval(Duration::from_millis(100));
    let config3 = Config::new(node3_addr.clone(), node2_addr.clone())
        .with_contact_info_interval(Duration::from_millis(100));

    // Create channels for system messages
    let (system_tx1, _) = mpsc::channel::<SystemMessage>(100);
    let (system_tx2, _) = mpsc::channel::<SystemMessage>(100);
    let (system_tx3, mut system_rx3) = mpsc::channel::<SystemMessage>(100);

    // Create gossip instances
    let mut gossip1 = SnapchainGossip::create(
        keypair1.clone(),
        &config1,
        Some(system_tx1),
        true,
        FarcasterNetwork::Devnet,
        statsd_client(),
    )
    .await
    .unwrap();
    let mut gossip2 = SnapchainGossip::create(
        keypair2.clone(),
        &config2,
        Some(system_tx2),
        false,
        FarcasterNetwork::Devnet,
        statsd_client(),
    )
    .await
    .unwrap();

    let mut gossip3 = SnapchainGossip::create(
        keypair3.clone(),
        &config3,
        Some(system_tx3),
        false,
        FarcasterNetwork::Devnet,
        statsd_client(),
    )
    .await
    .unwrap();

    let gossip_tx1 = gossip1.tx.clone();

    // Spawn gossip tasks
    tokio::spawn(async move {
        gossip1.start().await;
    });
    tokio::spawn(async move {
        gossip2.start().await;
    });
    tokio::spawn(async move {
        gossip3.start().await;
    });

    // Wait for connection to establish
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Create a test message
    let mut cast_add = messages_factory::casts::create_cast_add(123, "test", None, None);
    // Set data to None, so we can test that gossip populates data from data_bytes
    cast_add.data_bytes = Some(cast_add.data.unwrap().encode_to_vec());
    cast_add.data = None;

    let mempool_msg = MempoolMessage::UserMessage(cast_add.clone());
    // Send message from node1 to node2
    gossip_tx1
        .send(GossipEvent::BroadcastMempoolMessage(mempool_msg.clone()))
        .await
        .unwrap();

    // Sending the same message twice will cause the second message to be dropped
    gossip_tx1
        .send(GossipEvent::BroadcastMempoolMessage(mempool_msg))
        .await
        .unwrap();

    // Wait for message to be received with timeout. Node 1 is not connected directly to node 3 via bootstrap, but autodiscovery should cause it to find node 3.
    let receive_counts = wait_for_message(&mut system_rx3, cast_add).await;
    assert_eq!(receive_counts, 1);
}

/// Regression test for #865: read nodes must subscribe to MEMPOOL_TOPIC so that
/// they receive mempool gossip from validator peers. Before the fix, a read
/// node never joined the mempool mesh and never delivered inbound mempool
/// messages to its system channel, regardless of how many validator peers
/// were publishing.
#[tokio::test]
#[serial]
async fn test_read_node_receives_mempool_gossip() {
    let validator_keypair = Keypair::generate();
    let read_node_keypair = Keypair::generate();

    let validator_port = BASE_PORT_FOR_TEST + 20;
    let validator_addr = format!("/ip4/{HOST_FOR_TEST}/udp/{validator_port}/quic-v1");

    let read_node_port = BASE_PORT_FOR_TEST + 21;
    let read_node_addr = format!("/ip4/{HOST_FOR_TEST}/udp/{read_node_port}/quic-v1");

    let validator_config = Config::new(validator_addr.clone(), read_node_addr.clone())
        .with_contact_info_interval(Duration::from_millis(100));
    let read_node_config = Config::new(read_node_addr.clone(), validator_addr.clone())
        .with_contact_info_interval(Duration::from_millis(100));

    let (validator_system_tx, _) = mpsc::channel::<SystemMessage>(100);
    let (read_node_system_tx, mut read_node_system_rx) = mpsc::channel::<SystemMessage>(100);

    let mut validator_gossip = SnapchainGossip::create(
        validator_keypair,
        &validator_config,
        Some(validator_system_tx),
        false,
        FarcasterNetwork::Devnet,
        statsd_client(),
    )
    .await
    .unwrap();

    let mut read_node_gossip = SnapchainGossip::create(
        read_node_keypair,
        &read_node_config,
        Some(read_node_system_tx),
        true,
        FarcasterNetwork::Devnet,
        statsd_client(),
    )
    .await
    .unwrap();

    let validator_tx = validator_gossip.tx.clone();

    tokio::spawn(async move { validator_gossip.start().await });
    tokio::spawn(async move { read_node_gossip.start().await });

    let cast_add = messages_factory::casts::create_cast_add(456, "regression", None, None);
    let received = publish_until_received(
        validator_tx,
        &mut read_node_system_rx,
        cast_add,
        Duration::from_secs(10),
    )
    .await;
    assert!(
        received,
        "read node did not receive mempool gossip from validator within deadline"
    );
}

/// Multi-hop variant of the regression test. Topology:
///
/// ```text
///   read_node_1  <-->  read_node_2  <-->  validator
/// ```
///
/// `read_node_1` and `validator` only bootstrap to `read_node_2`, never to
/// each other, and autodiscovery is left off (default). For a mempool message
/// published on `read_node_1` to reach `validator`, gossipsub must forward
/// it through `read_node_2`'s mempool-topic mesh. Crucially, this is
/// distinguishable from the fanout-fallback path that makes the simpler 2-node
/// regression pass even when read nodes don't subscribe: in the bug scenario
/// `read_node_2` isn't subscribed to MEMPOOL_TOPIC, `read_node_1`'s only
/// direct peer that knows the topic is no one, fanout has nothing to fall back
/// to, and the publish never leaves `read_node_1`.
#[tokio::test]
#[serial]
async fn test_read_node_relays_mempool_gossip() {
    let read_node_1_keypair = Keypair::generate();
    let read_node_2_keypair = Keypair::generate();
    let validator_keypair = Keypair::generate();

    let read_node_1_port = BASE_PORT_FOR_TEST + 30;
    let read_node_1_addr = format!("/ip4/{HOST_FOR_TEST}/udp/{read_node_1_port}/quic-v1");

    let read_node_2_port = BASE_PORT_FOR_TEST + 31;
    let read_node_2_addr = format!("/ip4/{HOST_FOR_TEST}/udp/{read_node_2_port}/quic-v1");

    let validator_port = BASE_PORT_FOR_TEST + 32;
    let validator_addr = format!("/ip4/{HOST_FOR_TEST}/udp/{validator_port}/quic-v1");

    // read_node_1 and validator only bootstrap to read_node_2, never to each
    // other. read_node_2 has no bootstrap (it'll be dialled by the others).
    let read_node_1_config = Config::new(read_node_1_addr.clone(), read_node_2_addr.clone())
        .with_contact_info_interval(Duration::from_millis(100));
    let read_node_2_config = Config::new(read_node_2_addr.clone(), "".to_string())
        .with_contact_info_interval(Duration::from_millis(100));
    let validator_config = Config::new(validator_addr.clone(), read_node_2_addr.clone())
        .with_contact_info_interval(Duration::from_millis(100));

    let (read_node_1_system_tx, _) = mpsc::channel::<SystemMessage>(100);
    let (read_node_2_system_tx, _) = mpsc::channel::<SystemMessage>(100);
    let (validator_system_tx, mut validator_system_rx) = mpsc::channel::<SystemMessage>(100);

    let mut read_node_1_gossip = SnapchainGossip::create(
        read_node_1_keypair,
        &read_node_1_config,
        Some(read_node_1_system_tx),
        true,
        FarcasterNetwork::Devnet,
        statsd_client(),
    )
    .await
    .unwrap();

    let mut read_node_2_gossip = SnapchainGossip::create(
        read_node_2_keypair,
        &read_node_2_config,
        Some(read_node_2_system_tx),
        true,
        FarcasterNetwork::Devnet,
        statsd_client(),
    )
    .await
    .unwrap();

    let mut validator_gossip = SnapchainGossip::create(
        validator_keypair,
        &validator_config,
        Some(validator_system_tx),
        false,
        FarcasterNetwork::Devnet,
        statsd_client(),
    )
    .await
    .unwrap();

    let read_node_1_tx = read_node_1_gossip.tx.clone();

    tokio::spawn(async move { read_node_1_gossip.start().await });
    tokio::spawn(async move { read_node_2_gossip.start().await });
    tokio::spawn(async move { validator_gossip.start().await });

    let cast_add = messages_factory::casts::create_cast_add(789, "multi-hop", None, None);
    let received = publish_until_received(
        read_node_1_tx,
        &mut validator_system_rx,
        cast_add,
        Duration::from_secs(15),
    )
    .await;
    assert!(
        received,
        "validator did not receive mempool gossip relayed through read_node_2 within deadline"
    );
}

#[tokio::test]
#[serial]
async fn test_bootstrap_peer_reconnection() {
    // Create two keypairs for our test nodes
    let keypair1 = Keypair::generate();
    let keypair2 = Keypair::generate();

    // Create configs with different ports
    let node1_port = BASE_PORT_FOR_TEST + 10; // Use different ports from other tests
    let node1_addr = format!("/ip4/{HOST_FOR_TEST}/udp/{node1_port}/quic-v1");

    let node2_port = BASE_PORT_FOR_TEST + 11;
    let node2_addr = format!("/ip4/{HOST_FOR_TEST}/udp/{node2_port}/quic-v1");

    // Node1 will try to connect to Node2
    let config1 = Config::new(node1_addr.clone(), node2_addr.clone())
        .with_bootstrap_reconnect_interval(Duration::from_secs(1))
        .with_announce_address(node1_addr.clone());

    let config2 = Config::new(node2_addr.clone(), node1_addr.clone())
        .with_announce_address(node2_addr.clone());

    // Create channels for system messages
    let (system_tx1, _) = mpsc::channel::<SystemMessage>(100);
    let (system_tx2, _) = mpsc::channel::<SystemMessage>(100);

    // Start node2 first
    let mut gossip2 = SnapchainGossip::create(
        keypair2.clone(),
        &config2,
        Some(system_tx2),
        false,
        FarcasterNetwork::Devnet,
        statsd_client(),
    )
    .await
    .unwrap();

    let node2_handle = tokio::spawn(async move {
        gossip2.start().await;
    });

    // Create node1 instance
    let mut gossip1 = SnapchainGossip::create(
        keypair1.clone(),
        &config1,
        Some(system_tx1),
        false,
        FarcasterNetwork::Devnet,
        statsd_client(),
    )
    .await
    .unwrap();

    let gossip_tx1 = gossip1.tx.clone();

    // Start node1
    tokio::spawn(async move {
        gossip1.start().await;
    });

    // Wait for initial connection
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Now stop node2 (this will cause the connection to be lost)
    node2_handle.abort();
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Create a new node2 and start it again (to test reconnection)
    let (system_tx2, mut system_rx2) = mpsc::channel::<SystemMessage>(100);
    let mut gossip2 = SnapchainGossip::create(
        keypair2.clone(),
        &Config::new(node2_addr.clone(), node1_addr.clone()),
        Some(system_tx2),
        false,
        FarcasterNetwork::Devnet,
        statsd_client(),
    )
    .await
    .unwrap();

    tokio::spawn(async move {
        gossip2.start().await;
    });

    // Wait for reconnection attempts (timer is 30 seconds, but we only wait a bit)
    tokio::time::sleep(Duration::from_secs(2)).await;

    let cast_add = messages_factory::casts::create_cast_add(123, "test", None, None);
    let mempool_msg = MempoolMessage::UserMessage(cast_add.clone());
    // Send message from node1 to node2
    gossip_tx1
        .send(GossipEvent::BroadcastMempoolMessage(mempool_msg.clone()))
        .await
        .unwrap();

    let receive_counts = wait_for_message(&mut system_rx2, cast_add).await;
    assert_eq!(receive_counts, 1);
}
