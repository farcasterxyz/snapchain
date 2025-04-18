use crate::consensus::consensus::SystemMessage;
use crate::mempool::mempool::{MempoolRequest, MempoolSource};
use crate::network::gossip::{Config, GossipEvent, SnapchainGossip};
use crate::proto::{FarcasterNetwork, Message};
use crate::storage::store::engine::MempoolMessage;
use crate::storage::store::test_helper::statsd_client;
use crate::utils::factory::messages_factory;
use libp2p::identity::ed25519::Keypair;
use serial_test::serial;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::{select, time};

const HOST_FOR_TEST: &str = "127.0.0.1";
const BASE_PORT_FOR_TEST: u32 = 9382;

async fn wait_for_message(system_rx: &mut mpsc::Receiver<SystemMessage>, message: Message) -> u32 {
    let deadline = time::Instant::now() + Duration::from_secs(2);
    let mut receive_counts = 0;
    loop {
        let timeout = time::sleep_until(deadline);
        select! {
            received = system_rx.recv()  => {
                match received {
                    Some(SystemMessage::Mempool(msg))  => {
                        match msg {
                            MempoolRequest::AddMessage(MempoolMessage::UserMessage(data), source) => {
                                receive_counts += 1;
                                assert_eq!(data, message);
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
        system_tx1,
        true,
        FarcasterNetwork::Devnet,
        statsd_client(),
    )
    .await
    .unwrap();
    let mut gossip2 = SnapchainGossip::create(
        keypair2.clone(),
        &config2,
        system_tx2,
        false,
        FarcasterNetwork::Devnet,
        statsd_client(),
    )
    .await
    .unwrap();

    let mut gossip3 = SnapchainGossip::create(
        keypair3.clone(),
        &config3,
        system_tx3,
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
    let cast_add = messages_factory::casts::create_cast_add(123, "test", None, None);
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
        system_tx2,
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
        system_tx1,
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
        system_tx2,
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
