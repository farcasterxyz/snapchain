use crate::consensus::consensus::SystemMessage;
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

/// The mesh-diagnostics responder serves only validator peers: a request from a
/// configured validator is answered, one from a non-validator is refused.
#[tokio::test]
#[serial]
async fn test_diagnostics_only_serves_validators() {
    let keypair1 = Keypair::generate();
    let keypair2 = Keypair::generate();
    let keypair3 = Keypair::generate();

    let peer_id = |kp: &Keypair| libp2p::identity::PublicKey::from(kp.public()).to_peer_id();
    let pid1 = peer_id(&keypair1); // responder
    let pid2 = peer_id(&keypair2); // validator requester

    let base = BASE_PORT_FOR_TEST + 20;
    let node1_addr = format!("/ip4/{HOST_FOR_TEST}/udp/{base}/quic-v1");
    let node2_addr = format!("/ip4/{HOST_FOR_TEST}/udp/{}/quic-v1", base + 1);
    let node3_addr = format!("/ip4/{HOST_FOR_TEST}/udp/{}/quic-v1", base + 2);

    // node2 and node3 both bootstrap to node1 so both connect to the responder.
    let config1 = Config::new(node1_addr.clone(), node2_addr.clone());
    let config2 = Config::new(node2_addr.clone(), node1_addr.clone());
    let config3 = Config::new(node3_addr.clone(), node1_addr.clone());

    let (system_tx1, _) = mpsc::channel::<SystemMessage>(100);
    let (system_tx2, _) = mpsc::channel::<SystemMessage>(100);
    let (system_tx3, _) = mpsc::channel::<SystemMessage>(100);

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

    // node1 serves diagnostics only to node2 (a "validator"), not node3.
    gossip1.set_validator_peers(std::collections::HashSet::from([pid2]));

    let gossip_tx2 = gossip2.tx.clone();
    let gossip_tx3 = gossip3.tx.clone();

    tokio::spawn(async move {
        gossip1.start().await;
    });
    tokio::spawn(async move {
        gossip2.start().await;
    });
    tokio::spawn(async move {
        gossip3.start().await;
    });

    // Let connections establish.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // node2 is a validator → request is served.
    let (tx, rx) = tokio::sync::oneshot::channel();
    gossip_tx2
        .send(GossipEvent::SendMeshViewRequest(
            pid1,
            crate::proto::GetMeshViewRequest::default(),
            tx,
        ))
        .await
        .unwrap();
    let allowed = time::timeout(Duration::from_secs(4), rx)
        .await
        .expect("validator request timed out")
        .expect("oneshot dropped");
    assert!(
        allowed.is_ok(),
        "validator peer should be served, got {allowed:?}"
    );

    // node3 is not a validator → request is refused (OutboundFailure → Err).
    let (tx, rx) = tokio::sync::oneshot::channel();
    gossip_tx3
        .send(GossipEvent::SendMeshViewRequest(
            pid1,
            crate::proto::GetMeshViewRequest::default(),
            tx,
        ))
        .await
        .unwrap();
    let refused = time::timeout(Duration::from_secs(8), rx)
        .await
        .expect("non-validator request timed out")
        .expect("oneshot dropped");
    assert!(
        refused.is_err(),
        "non-validator peer should be refused, got {refused:?}"
    );
}

/// Mimic the RPC layer's validator classification so the renderer's
/// validator-only graph/counters exercise these peers.
fn classify_as_validators(mut view: crate::proto::MeshView) -> crate::proto::MeshView {
    if let Some(l) = view.local.as_mut() {
        l.is_validator = true;
    }
    for p in view.peers.iter_mut() {
        p.node_type = crate::proto::MeshNodeType::Validator as i32;
    }
    view
}

#[tokio::test]
#[serial]
async fn mesh_view_tags_direct_peers_as_direct_links() {
    // Two validators configured as each other's libp2p direct/explicit peers (the
    // mainnet/testnet topology). Gossipsub forwards published messages to explicit
    // peers but keeps them OUT of `mesh_peers()`, so the peer must report
    // `direct_peer=true` + consensus `in_mesh=false` and render as a `◆` direct
    // link — never a `○` sub-only or a partition. This is the testnet repro.
    let keypair1 = Keypair::generate();
    let keypair2 = Keypair::generate();
    let peer_id = |kp: &Keypair| libp2p::identity::PublicKey::from(kp.public()).to_peer_id();
    let pid2 = peer_id(&keypair2);

    let base = BASE_PORT_FOR_TEST + 30;
    let node1_addr = format!("/ip4/{HOST_FOR_TEST}/udp/{base}/quic-v1");
    let node2_addr = format!("/ip4/{HOST_FOR_TEST}/udp/{}/quic-v1", base + 1);

    // Each node lists the OTHER's PeerId as a direct peer — exactly what
    // setup_local_testnet now generates by default.
    let config1 = Config::new(node1_addr.clone(), node2_addr.clone())
        .with_direct_peers(peer_id(&keypair2).to_base58());
    let config2 = Config::new(node2_addr.clone(), node1_addr.clone())
        .with_direct_peers(peer_id(&keypair1).to_base58());

    let (system_tx1, _) = mpsc::channel::<SystemMessage>(100);
    let (system_tx2, _) = mpsc::channel::<SystemMessage>(100);

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

    let gossip_tx1 = gossip1.tx.clone();
    tokio::spawn(async move {
        gossip1.start().await;
    });
    tokio::spawn(async move {
        gossip2.start().await;
    });

    // Poll until node1 sees node2 as a connected direct peer with at least one
    // visible subscription. (Explicit peers can race their per-topic resub under
    // CPU contention from other tests' leaked gossip loops, so we don't pin to a
    // specific topic here — the consensus-specific `◆`/no-partition rendering is
    // covered deterministically by the render.rs unit tests.)
    let deadline = time::Instant::now() + Duration::from_secs(15);
    let mut found = None;
    while time::Instant::now() < deadline {
        let (tx, rx) = tokio::sync::oneshot::channel();
        gossip_tx1.send(GossipEvent::GetMeshView(tx)).await.unwrap();
        let view: crate::proto::MeshView = time::timeout(Duration::from_secs(2), rx)
            .await
            .expect("mesh view timed out")
            .expect("oneshot dropped");
        if let Some(p) = view.peers.iter().find(|p| p.peer_id == pid2.to_bytes()) {
            if p.direct_peer && p.topics.iter().any(|t| t.subscribed) {
                found = Some((view.clone(), p.clone()));
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    let (view, peer) = found.expect("node1 never saw node2 as a subscribed direct peer");

    // The gossip-layer invariant behind the testnet bug: an explicit/direct peer
    // is tagged `direct_peer` and is excluded from the gossipsub mesh on EVERY
    // topic it is subscribed to (so it would render `◆`, never `●`).
    assert!(peer.direct_peer, "node2 should be a direct peer");
    assert!(
        peer.topics.iter().any(|t| t.subscribed),
        "node2 should be subscribed to at least one topic"
    );
    assert!(
        peer.topics.iter().all(|t| !t.in_mesh),
        "explicit/direct peers must never appear in the gossipsub mesh: {:?}",
        peer.topics
    );

    // End-to-end render: a subscribed direct peer shows up as a `◆` direct link.
    let view = classify_as_validators(view);
    let topics = crate::network::mesh::render::parse_topics(Some("consensus,mempool,contact-info"));
    let rendered = crate::network::mesh::render::render_mesh_view(&view, &topics);
    assert!(
        rendered.contains("◆"),
        "expected a direct-link glyph for the direct peer:\n{rendered}"
    );
}

#[tokio::test]
#[serial]
async fn mesh_view_tags_meshed_peers_as_in_mesh() {
    // Without direct_peers the two validators form an emergent gossipsub mesh: the
    // peer reports consensus `in_mesh=true` (and `direct_peer=false`) and renders
    // `●`. Guards the original (non-explicit-peer) mesh-rendering path.
    let keypair1 = Keypair::generate();
    let keypair2 = Keypair::generate();
    let peer_id = |kp: &Keypair| libp2p::identity::PublicKey::from(kp.public()).to_peer_id();
    let pid2 = peer_id(&keypair2);

    let base = BASE_PORT_FOR_TEST + 32;
    let node1_addr = format!("/ip4/{HOST_FOR_TEST}/udp/{base}/quic-v1");
    let node2_addr = format!("/ip4/{HOST_FOR_TEST}/udp/{}/quic-v1", base + 1);

    let config1 = Config::new(node1_addr.clone(), node2_addr.clone());
    let config2 = Config::new(node2_addr.clone(), node1_addr.clone());

    let (system_tx1, _) = mpsc::channel::<SystemMessage>(100);
    let (system_tx2, _) = mpsc::channel::<SystemMessage>(100);

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

    let gossip_tx1 = gossip1.tx.clone();
    tokio::spawn(async move {
        gossip1.start().await;
    });
    tokio::spawn(async move {
        gossip2.start().await;
    });

    // Poll until node2 grafts into node1's consensus mesh (a few heartbeats).
    let deadline = time::Instant::now() + Duration::from_secs(20);
    let mut found = None;
    while time::Instant::now() < deadline {
        let (tx, rx) = tokio::sync::oneshot::channel();
        gossip_tx1.send(GossipEvent::GetMeshView(tx)).await.unwrap();
        let view: crate::proto::MeshView = time::timeout(Duration::from_secs(2), rx)
            .await
            .expect("mesh view timed out")
            .expect("oneshot dropped");
        if let Some(p) = view.peers.iter().find(|p| p.peer_id == pid2.to_bytes()) {
            if p.topics.iter().any(|t| t.topic == "consensus" && t.in_mesh) {
                found = Some((view.clone(), p.clone()));
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let (view, peer) = found.expect("node2 never joined node1's consensus mesh");

    assert!(
        !peer.direct_peer,
        "node2 is not configured as a direct peer"
    );
    let consensus = peer
        .topics
        .iter()
        .find(|t| t.topic == "consensus")
        .expect("consensus membership");
    assert!(consensus.in_mesh, "a meshed peer should report in_mesh");

    let view = classify_as_validators(view);
    let topics = crate::network::mesh::render::parse_topics(None);
    let rendered = crate::network::mesh::render::render_mesh_view(&view, &topics);
    assert!(
        rendered.contains("●"),
        "expected an in-mesh glyph:\n{rendered}"
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
