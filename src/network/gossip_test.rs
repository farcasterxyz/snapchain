use crate::consensus::consensus::SystemMessage;
use crate::mempool::mempool::{MempoolRequest, MempoolSource};
use crate::network::gossip::{Config, GossipEvent, SnapchainGossip};
use crate::storage::store::engine::MempoolMessage;
use crate::utils::factory::messages_factory;
use libp2p::identity::ed25519::Keypair;
use serial_test::serial;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::{select, time};

const HOST_FOR_TEST: &str = "127.0.0.1";
const BASE_PORT_FOR_TEST: u32 = 9382;

#[tokio::test]
#[serial]
async fn test_gossip_communication() {
    // Create two keypairs for our test nodes
    let keypair1 = Keypair::generate();
    let keypair2 = Keypair::generate();

    // Create configs with different ports
    let node1_addr = format!("/ip4/{HOST_FOR_TEST}/udp/{BASE_PORT_FOR_TEST}/quic-v1");
    let node2_port = BASE_PORT_FOR_TEST + 1;
    let node2_addr = format!("/ip4/{HOST_FOR_TEST}/udp/{node2_port}/quic-v1");
    let config1 = Config::new(node1_addr.clone(), node2_addr.clone());
    let config2 = Config::new(node2_addr.clone(), node1_addr.clone());

    // Create channels for system messages
    let (system_tx1, _) = mpsc::channel::<SystemMessage>(100);
    let (system_tx2, mut system_rx2) = mpsc::channel::<SystemMessage>(100);

    // Create gossip instances
    let mut gossip1 =
        SnapchainGossip::create(keypair1.clone(), &config1, system_tx1, false).unwrap();
    let mut gossip2 =
        SnapchainGossip::create(keypair2.clone(), &config2, system_tx2, false).unwrap();

    let gossip_tx1 = gossip1.tx.clone();

    // Spawn gossip tasks
    tokio::spawn(async move {
        gossip1.start().await;
    });
    tokio::spawn(async move {
        gossip2.start().await;
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

    // Wait for message to be received with timeout
    let deadline = time::Instant::now() + Duration::from_secs(2);
    let mut receive_counts = 0;
    loop {
        let timeout = time::sleep_until(deadline);
        select! {
            received = system_rx2.recv()  => {
                match received {
                    Some(SystemMessage::Mempool(msg))  => {
                        match msg {
                            MempoolRequest::AddMessage(MempoolMessage::UserMessage(data), source) => {
                                receive_counts += 1;
                                assert_eq!(data, cast_add);
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
    assert_eq!(receive_counts, 1);
}
