use ed25519_dalek::SigningKey;
use hex;
use informalsystems_malachitebft_metrics::SharedRegistry;
use libp2p::identity::ed25519::Keypair;
use snapchain::connectors::onchain_events::ChainClients;
use snapchain::consensus::consensus::{SystemMessage, ValidatorSetConfig};
use snapchain::consensus::proposer::GENESIS_MESSAGE;
use snapchain::mempool::mempool::{
    self, Mempool, MempoolMessagesRequest, MempoolRequest, MempoolSource,
};
use snapchain::mempool::routing;
use snapchain::network::gossip::SnapchainGossip;
use snapchain::network::server::MyHubService;
use snapchain::node::snapchain_node::SnapchainNode;
use snapchain::node::snapchain_read_node::SnapchainReadNode;
use snapchain::proto::hub_service_server::HubServiceServer;
use snapchain::proto::{self, Height};
use snapchain::proto::{Block, FarcasterNetwork, IdRegisterEventType, SignerEventType};
use snapchain::storage::db::{PageOptions, RocksDB};
use snapchain::storage::store::account::{CastStore, OnchainEventStore};
use snapchain::storage::store::engine::MempoolMessage;
use snapchain::storage::store::node_local_state::LocalStateStore;
use snapchain::storage::store::stores::Stores;
use snapchain::storage::store::BlockStore;
use snapchain::utils::factory::{self, messages_factory};
use snapchain::utils::statsd_wrapper::StatsdClientWrapper;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tokio::time;
use tonic::transport::Server;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

const HOST_FOR_TEST: &str = "127.0.0.1";
const BASE_PORT_FOR_TEST: u32 = 9482;

fn get_available_port() -> u32 {
    let mut port = BASE_PORT_FOR_TEST + (rand::random::<u32>() % 1000);
    loop {
        if let Ok(listener) = std::net::TcpListener::bind(format!("{}:{}", HOST_FOR_TEST, port)) {
            listener.set_nonblocking(true).unwrap();
            return port;
        }
        port += 1;
    }
}

fn make_tmp_path() -> String {
    tempfile::tempdir()
        .unwrap()
        .path()
        .as_os_str()
        .to_string_lossy()
        .to_string()
}

async fn wait_for<F>(f: F, timeout: time::Duration, tick: time::Duration) -> Option<()>
where
    F: Fn() -> Option<()>,
{
    let start = tokio::time::Instant::now();

    loop {
        if f().is_some() {
            return Some(());
        }

        if start.elapsed() > timeout {
            return None;
        }

        tokio::time::sleep(tick).await;
    }
}

trait Node {
    fn block_store(&self) -> &BlockStore;
    fn shard_stores(&self) -> &HashMap<u32, Stores>;

    fn num_blocks(&self) -> usize {
        let blocks_page = self
            .block_store()
            .get_blocks(0, None, &PageOptions::default())
            .unwrap();
        blocks_page.blocks.len()
    }

    fn num_shard_chunks(&self) -> usize {
        let mut count = 0;
        for (_shard_id, stores) in self.shard_stores().iter() {
            count += stores.shard_store.get_shard_chunks(0, None).unwrap().len();
        }
        count
    }

    fn total_messages(&self) -> usize {
        let mut messages_count = 0;
        for (_shard_id, stores) in self.shard_stores().iter() {
            for chunk in stores.shard_store.get_shard_chunks(0, None).unwrap() {
                for tx in chunk.transactions.iter() {
                    messages_count += tx.user_messages.len();
                }
            }
        }
        messages_count
    }

    fn fid_registered(&self, fid: u64) -> Option<()> {
        for stores in self.shard_stores().values() {
            let result =
                OnchainEventStore::get_id_register_event_by_fid(&stores.onchain_event_store, fid);
            if result.is_ok() && result.unwrap().is_some() {
                return Some(());
            }
        }

        return None;
    }

    fn cast_added(&self, fid: u64, hash: Vec<u8>) -> Option<()> {
        for stores in self.shard_stores().values() {
            let result = CastStore::get_cast_add(&stores.cast_store, fid, hash.clone());
            if result.is_ok() && result.unwrap().is_some() {
                return Some(());
            }
        }
        None
    }
}

struct NodeForTest {
    node: SnapchainNode,
    db: Arc<RocksDB>,
    block_store: BlockStore,
    mempool_tx: mpsc::Sender<MempoolRequest>,
    handles: Vec<tokio::task::JoinHandle<()>>,
}

impl Node for NodeForTest {
    fn block_store(&self) -> &BlockStore {
        &self.block_store
    }
    fn shard_stores(&self) -> &HashMap<u32, Stores> {
        &self.node.shard_stores
    }
}

impl Drop for NodeForTest {
    fn drop(&mut self) {
        self.node.stop();
        for handle in self.handles.iter() {
            handle.abort();
        }
        self.db.destroy().unwrap();
        for (_, stores) in self.node.shard_stores.iter_mut() {
            stores.shard_store.db.destroy().unwrap();
        }
    }
}

struct ReadNodeForTest {
    handles: Vec<tokio::task::JoinHandle<()>>,
    node: SnapchainReadNode,
    db: Arc<RocksDB>,
    block_store: BlockStore,
    _messages_request_rx: mpsc::Receiver<MempoolMessagesRequest>,
}

impl Node for ReadNodeForTest {
    fn block_store(&self) -> &BlockStore {
        &self.block_store
    }
    fn shard_stores(&self) -> &HashMap<u32, Stores> {
        &self.node.shard_stores
    }
}

impl Drop for ReadNodeForTest {
    fn drop(&mut self) {
        self.node.stop();
        for handle in self.handles.iter() {
            handle.abort();
        }
        self.db.destroy().unwrap();
        for (_, stores) in self.node.shard_stores.iter_mut() {
            stores.shard_store.db.destroy().unwrap();
        }
    }
}

impl ReadNodeForTest {
    pub async fn create(
        keypair: Keypair,
        num_shards: u32,
        validator_sets: &Vec<ValidatorSetConfig>,
        gossip_address: String,
        bootstrap_address: String,
    ) -> Self {
        let statsd_client = StatsdClientWrapper::new(
            cadence::StatsdClient::builder("", cadence::NopMetricSink {}).build(),
            true,
        );

        let config = snapchain::network::gossip::Config::new(gossip_address, bootstrap_address);

        let mut consensus_config = snapchain::consensus::consensus::Config::default();
        consensus_config =
            consensus_config.with((1..=num_shards).collect(), validator_sets.clone());

        let (system_tx, mut system_rx) = mpsc::channel::<SystemMessage>(100);

        let fc_network = FarcasterNetwork::Testnet;
        let mut gossip = SnapchainGossip::create(
            keypair.clone(),
            &config,
            system_tx.clone(),
            false,
            fc_network,
            statsd_client.clone(),
        )
        .await
        .unwrap();
        let gossip_tx = gossip.tx.clone();

        let registry = SharedRegistry::global();
        let peer_id = gossip.swarm.local_peer_id().clone();
        println!("StartNode read node peer id: {}", peer_id.to_string());
        let data_dir = &make_tmp_path();
        let db = Arc::new(RocksDB::new(data_dir));
        db.open().unwrap();
        let block_store = BlockStore::new(db.clone());
        let (messages_request_tx, messages_request_rx) = mpsc::channel(100);
        let node = SnapchainReadNode::create(
            keypair.clone(),
            consensus_config,
            peer_id,
            gossip_tx.clone(),
            system_tx.clone(),
            messages_request_tx,
            block_store.clone(),
            make_tmp_path(),
            statsd_client.clone(),
            16,
            fc_network,
            registry,
        )
        .await;

        let mut join_handles = Vec::new();

        let handle = tokio::spawn(async move {
            gossip.start().await;
        });
        join_handles.push(handle);

        let node_for_dispatch = node.clone();
        let handle = tokio::spawn(async move {
            loop {
                if let Some(system_event) = system_rx.recv().await {
                    match system_event {
                        SystemMessage::MalachiteNetwork(event_shard, event) => {
                            node_for_dispatch.dispatch_network_event(event_shard, event);
                        }
                        _ => {
                            // noop
                        }
                    }
                }
            }
        });
        join_handles.push(handle);

        Self {
            handles: join_handles,
            node,
            db: db.clone(),
            block_store,
            _messages_request_rx: messages_request_rx,
        }
    }
}

impl NodeForTest {
    pub async fn create(
        keypair: Keypair,
        num_shards: u32,
        grpc_port: u32,
        validator_sets: &Vec<ValidatorSetConfig>,
        gossip_address: String,
        bootstrap_address: String,
    ) -> Self {
        let statsd_client = StatsdClientWrapper::new(
            cadence::StatsdClient::builder("", cadence::NopMetricSink {}).build(),
            true,
        );
        let config =
            snapchain::network::gossip::Config::new(gossip_address.clone(), bootstrap_address)
                .with_announce_address(gossip_address);

        let mut consensus_config = snapchain::consensus::consensus::Config::default();
        consensus_config =
            consensus_config.with((1..=num_shards).collect(), validator_sets.clone());
        consensus_config.block_time = time::Duration::from_millis(250);

        let (system_tx, mut system_rx) = mpsc::channel::<SystemMessage>(100);
        let fc_network = FarcasterNetwork::Testnet;

        let mut gossip = SnapchainGossip::create(
            keypair.clone(),
            &config,
            system_tx.clone(),
            false,
            fc_network,
            statsd_client.clone(),
        )
        .await
        .unwrap();
        let gossip_tx = gossip.tx.clone();

        let registry = SharedRegistry::global();
        let peer_id = gossip.swarm.local_peer_id().clone();
        println!("StartNode validator peer id: {}", peer_id.to_string());
        let (block_tx, mut block_rx) = mpsc::channel::<Block>(100);
        let data_dir = &make_tmp_path();
        let db = Arc::new(RocksDB::new(data_dir));
        db.open().unwrap();
        let block_store = BlockStore::new(db.clone());
        let global_db = RocksDB::open_global_db(&data_dir);
        let node_local_store = LocalStateStore::new(global_db);
        let (messages_request_tx, messages_request_rx) = mpsc::channel(100);
        let (shard_decision_tx, shard_decision_rx) = broadcast::channel(100);
        let node = SnapchainNode::create(
            keypair.clone(),
            consensus_config,
            peer_id,
            gossip_tx.clone(),
            shard_decision_tx,
            Some(block_tx),
            messages_request_tx,
            block_store.clone(),
            node_local_store,
            make_tmp_path(),
            statsd_client.clone(),
            16,
            fc_network,
            registry,
        )
        .await;

        let node_id = node.id();
        let assert_valid_block = move |block: &Block| {
            let header = block.header.as_ref().unwrap();
            let chunks_count = block
                .shard_witness
                .as_ref()
                .unwrap()
                .shard_chunk_witnesses
                .len();
            info!(
                hash = hex::encode(&block.hash),
                height = header.height.as_ref().map(|h| h.block_number),
                id = node_id,
                chunks = chunks_count,
                "decided block",
            );

            if block.header.as_ref().unwrap().height.unwrap() == Height::new(0, 1) {
                assert_eq!(
                    block.header.as_ref().unwrap().parent_hash,
                    GENESIS_MESSAGE.as_bytes().to_vec()
                )
            }

            assert_eq!(chunks_count, num_shards as usize);
        };

        let mut join_handles = Vec::new();

        let handle = tokio::spawn(async move {
            while let Some(block) = block_rx.recv().await {
                assert_valid_block(&block);
            }
        });
        join_handles.push(handle);

        let grpc_addr = format!("0.0.0.0:{}", grpc_port);
        let addr = grpc_addr.clone();
        let (mempool_tx, mempool_rx) = mpsc::channel(100);
        let mut mempool = Mempool::new(
            mempool::Config::default(),
            mempool_rx,
            messages_request_rx,
            num_shards,
            node.shard_stores.clone(),
            gossip_tx,
            shard_decision_rx,
            statsd_client.clone(),
        );
        let handle = tokio::spawn(async move { mempool.run().await });
        join_handles.push(handle);

        let service = MyHubService::new(
            "".to_string(),
            block_store.clone(),
            node.shard_stores.clone(),
            node.shard_senders.clone(),
            statsd_client.clone(),
            num_shards,
            FarcasterNetwork::Testnet,
            Box::new(routing::EvenOddRouterForTest {}),
            mempool_tx.clone(),
            ChainClients {
                chain_api_map: Default::default(),
            },
            "".to_string(),
            "".to_string(),
        );

        let handle = tokio::spawn(async move {
            let grpc_socket_addr: SocketAddr = addr.parse().unwrap();
            let resp = Server::builder()
                .add_service(HubServiceServer::new(service))
                .serve(grpc_socket_addr)
                .await;

            let msg = "grpc server stopped";
            match resp {
                Ok(()) => error!(msg),
                Err(e) => error!(error = ?e, "{}", msg),
            }
        });
        join_handles.push(handle);

        let handle = tokio::spawn(async move {
            gossip.start().await;
        });
        join_handles.push(handle);

        let node_for_dispatch = node.clone();
        let handle = tokio::spawn(async move {
            loop {
                if let Some(system_event) = system_rx.recv().await {
                    match system_event {
                        SystemMessage::MalachiteNetwork(event_shard, event) => {
                            node_for_dispatch.dispatch(event_shard, event);
                        }
                        _ => {
                            // noop
                        }
                    }
                }
            }
        });
        join_handles.push(handle);

        Self {
            node,
            db: db.clone(),
            block_store,
            mempool_tx,
            handles: join_handles,
        }
    }

    pub async fn add_message(
        &self,
        message: MempoolMessage,
        source: MempoolSource,
        rx: Option<tokio::sync::oneshot::Sender<Result<(), snapchain::core::error::HubError>>>,
    ) -> Result<(), mpsc::error::SendError<MempoolRequest>> {
        self.mempool_tx
            .send(MempoolRequest::AddMessage(message, source, rx))
            .await
    }
}

pub struct TestNetwork {
    num_validator_nodes: u32,
    num_shards: u32,
    keypairs: Vec<Keypair>,
    validator_sets: Vec<ValidatorSetConfig>,
    gossip_addresses: Vec<String>,
    nodes: Vec<NodeForTest>,
    read_nodes: Vec<ReadNodeForTest>,
    test_fids: HashMap<u64, (SigningKey, Vec<u8>)>, // FID -> (signer, address)
}

impl TestNetwork {
    // These networks can be created in parallel, so make sure the base port is far enough part to avoid conflicts
    pub async fn create(num_validator_nodes: u32, num_shards: u32) -> Self {
        let mut keypairs = Vec::new();
        let mut validator_addresses = vec![];
        let mut node_addresses = vec![];
        for _ in 0..num_validator_nodes {
            let keypair = Keypair::generate();
            keypairs.push(keypair.clone());
            validator_addresses.push(hex::encode(keypair.public().to_bytes()));
            let port = get_available_port();
            let gossip_address = format!("/ip4/{HOST_FOR_TEST}/udp/{port}/quic-v1");
            node_addresses.push(gossip_address);
        }
        let validator_sets = vec![ValidatorSetConfig {
            effective_at: 0,
            validator_public_keys: validator_addresses,
            shard_ids: (1..=num_shards).collect(),
        }];
        Self {
            num_validator_nodes,
            num_shards,
            keypairs,
            validator_sets,
            gossip_addresses: node_addresses,
            nodes: vec![],
            read_nodes: vec![],
            test_fids: HashMap::new(),
        }
    }

    async fn start_validator_node(&mut self, index: u32) {
        let keypair = self.keypairs[index as usize].clone();
        let gossip_address = self.gossip_addresses[index as usize].clone();
        let grpc_port = get_available_port();
        let node = NodeForTest::create(
            keypair,
            self.num_shards,
            grpc_port,
            &self.validator_sets,
            gossip_address,
            self.gossip_addresses.join(","),
        )
        .await;
        self.nodes.push(node);
    }

    async fn start_read_node(&mut self) {
        let keypair = Keypair::generate();
        let port = get_available_port();
        let gossip_address = format!("/ip4/{HOST_FOR_TEST}/udp/{port}/quic-v1");
        let node = ReadNodeForTest::create(
            keypair,
            self.num_shards,
            &self.validator_sets,
            gossip_address,
            self.gossip_addresses.join(","),
        )
        .await;
        self.read_nodes.push(node);
    }

    async fn start_validators(&mut self) {
        for i in 0..self.num_validator_nodes {
            self.start_validator_node(i).await;
        }
    }

    pub fn max_block_height(&self) -> usize {
        self.nodes
            .iter()
            .map(|node| node.num_blocks())
            .max()
            .unwrap_or(0)
    }

    pub async fn register_fid(&mut self, fid: u64) {
        assert!(
            !self.nodes.is_empty(),
            "No nodes in the network to register FID"
        );

        let signer = factory::signers::generate_signer();
        let address = factory::address::generate_random_address();

        let on_chain_events = vec![
            factory::events_factory::create_rent_event(fid, None, None, false),
            factory::events_factory::create_signer_event(
                fid,
                signer.clone(),
                SignerEventType::Add,
                None,
                None,
            ),
            factory::events_factory::create_id_register_event(
                fid,
                IdRegisterEventType::Register,
                address.clone(),
                None,
            ),
        ];

        for event in on_chain_events {
            let result = self.nodes[0]
                .add_message(
                    MempoolMessage::ValidatorMessage(proto::ValidatorMessage {
                        on_chain_event: Some(event),
                        fname_transfer: None,
                    }),
                    MempoolSource::Local,
                    None,
                )
                .await;

            assert!(
                result.is_ok(),
                "Failed to register FID {}: {:?}",
                fid,
                result.err()
            );
        }

        self.test_fids.insert(fid, (signer, address));
    }

    fn fid_registered(&self, fid: u64) -> Option<()> {
        self.nodes
            .iter()
            .any(|node| node.fid_registered(fid).is_some())
            .then_some(())
    }

    pub async fn wait_for_fid(&self, fid: u64) -> Option<()> {
        wait_for(
            || self.fid_registered(fid),
            tokio::time::Duration::from_secs(5),
            tokio::time::Duration::from_millis(100),
        )
        .await
    }

    pub async fn send_cast(&mut self, fid: u64, text: &str) -> proto::Message {
        assert!(
            !self.nodes.is_empty(),
            "No nodes in the network to send cast"
        );
        assert!(
            self.test_fids.contains_key(&fid),
            "FID {} is not registered in the network",
            fid
        );

        let (signer, _) = self.test_fids.get(&fid).unwrap();
        let message = messages_factory::casts::create_cast_add(fid, text, None, Some(&signer));

        for i in 0..self.nodes.len() {
            let result = self.nodes[i]
                .add_message(
                    MempoolMessage::UserMessage(message.clone()),
                    MempoolSource::Local,
                    None,
                )
                .await;

            assert!(
                result.is_ok(),
                "Failed to send cast message (fid: {}, text: {}): {:?}",
                fid,
                text,
                result.err()
            );
        }

        message
    }

    fn cast_added(&self, fid: u64, hash: Vec<u8>) -> Option<()> {
        self.nodes
            .iter()
            .any(|node| node.cast_added(fid, hash.clone()).is_some())
            .then_some(())
    }

    pub async fn wait_for_cast(&self, fid: u64, hash: Vec<u8>) -> Option<()> {
        wait_for(
            || self.cast_added(fid, hash.clone()),
            tokio::time::Duration::from_secs(5),
            tokio::time::Duration::from_millis(100),
        )
        .await
    }

    // Checks if all validator nodes have reached at least `height` blocks.
    fn block_reached(&self, height: usize) -> Option<()> {
        self.nodes
            .iter()
            .all(|node| node.num_blocks() >= height)
            .then_some(())
    }

    // Waits for all validator nodes to reach at least `height` blocks.
    pub async fn wait_for_block(&self, height: usize) -> Option<()> {
        wait_for(
            || self.block_reached(height),
            tokio::time::Duration::from_secs(15),
            tokio::time::Duration::from_millis(100),
        )
        .await
    }

    // Checks if all reader nodes have reached at least `height` blocks.
    fn read_block_reached(&self, height: usize) -> Option<()> {
        self.read_nodes
            .iter()
            .all(|node| node.num_blocks() >= height)
            .then_some(())
    }

    // Waits for all reader nodes to reach at least `height` blocks.
    pub async fn read_wait_for_block(&self, height: usize) -> Option<()> {
        wait_for(
            || self.read_block_reached(height),
            tokio::time::Duration::from_secs(15),
            tokio::time::Duration::from_millis(100),
        )
        .await
    }
}

fn assert_network_has_num_blocks(network: &TestNetwork, num_blocks: usize) {
    for (i, node) in network.nodes.iter().enumerate() {
        assert!(
            node.num_blocks() >= num_blocks,
            "Node {} should have confirmed at least {} blocks, but has {}",
            i,
            num_blocks,
            node.num_blocks()
        );
    }

    for (i, node) in network.read_nodes.iter().enumerate() {
        assert!(
            node.num_blocks() >= num_blocks,
            "Read Node {} should have confirmed at least {} blocks, but has {}",
            i,
            num_blocks,
            node.num_blocks()
        );
    }
}

fn assert_network_has_num_shard_chunks(network: &TestNetwork, num_chunks: usize) {
    for (i, node) in network.nodes.iter().enumerate() {
        assert!(
            node.num_shard_chunks() >= num_chunks,
            "Node {} should have confirmed at least {} shard chunks, but has {}",
            i,
            num_chunks,
            node.num_shard_chunks()
        );
    }

    for (i, node) in network.read_nodes.iter().enumerate() {
        assert!(
            node.num_shard_chunks() >= num_chunks,
            "Read Node {} should have confirmed at least {} shard chunks, but has {}",
            i,
            num_chunks,
            node.num_shard_chunks()
        );
    }
}

fn assert_network_has_messages(network: &TestNetwork, num_messages: usize) {
    for (i, node) in network.nodes.iter().enumerate() {
        assert!(
            node.total_messages() >= num_messages,
            "Node {} should have at least {} messages, but has {}",
            i,
            num_messages,
            node.total_messages()
        );
    }

    for (i, node) in network.read_nodes.iter().enumerate() {
        assert!(
            node.total_messages() >= num_messages,
            "Read Node {} should have at least {} messages, but has {}",
            i,
            num_messages,
            node.total_messages()
        );
    }
}

fn assert_network_has_cast(network: &TestNetwork, fid: u64, hash: Vec<u8>) {
    for (i, node) in network.nodes.iter().enumerate() {
        assert!(
            node.cast_added(fid, hash.clone()).is_some(),
            "Node {} should have cast message with fid {} and hash {}, but it was not found",
            i,
            fid,
            hex::encode(hash)
        );
    }

    for (i, node) in network.read_nodes.iter().enumerate() {
        assert!(
            node.cast_added(fid, hash.clone()).is_some(),
            "Read Node {} should have cast message with fid {} and hash {}, but it was not found",
            i,
            fid,
            hex::encode(hash)
        );
    }
}

// Registers an FID and waits until at least 1 node confirms it.
async fn register_fid_and_wait(network: &mut TestNetwork, fid: u64) {
    network.register_fid(fid).await;
    assert!(
        network.wait_for_fid(fid).await.is_some(),
        "Failed to register FID 1000 in the network"
    );
}

// Sends a cast message and waits until at least 1 node confirms it.
async fn send_cast_and_wait(network: &mut TestNetwork, fid: u64, text: &str) -> proto::Message {
    let cast = network.send_cast(fid, text).await;
    assert!(
        network
            .wait_for_cast(fid, cast.hash.clone())
            .await
            .is_some(),
        "Failed to find cast message in the network"
    );
    cast
}

#[tokio::test]
async fn test_basic_consensus() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .try_init();

    let num_shards = 2;
    let mut network = TestNetwork::create(3, num_shards).await;
    network.start_validators().await;

    register_fid_and_wait(&mut network, 1000).await;
    let cast = send_cast_and_wait(&mut network, 1000, "Hello, world").await;

    assert_network_has_num_blocks(&network, 1);
    assert_network_has_num_shard_chunks(&network, 3);
    assert_network_has_messages(&network, 1);
    assert_network_has_cast(&network, 1000, cast.hash.clone());
}

#[tokio::test]
async fn test_basic_sync() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .try_init();

    // Set up shard and validators
    let num_shards = 2;
    let mut network = TestNetwork::create(4, num_shards).await;

    // Start the first three nodes
    for i in 0..3 {
        network.start_validator_node(i).await;
    }

    register_fid_and_wait(&mut network, 1000).await;
    let cast = send_cast_and_wait(&mut network, 1000, "Hello, world").await;

    // Wait for the next block to arrive on all nodes
    let next_block_height = network.max_block_height() + 1;
    network.wait_for_block(next_block_height).await;

    assert_network_has_num_blocks(&network, next_block_height);
    assert_network_has_num_shard_chunks(&network, 2);
    assert_network_has_messages(&network, 1);

    // Add the node to the network and start producing blocks again.
    network.start_validator_node(3).await;

    // Wait for all nodes to reach the same block height
    let target_height = network.max_block_height();
    network.wait_for_block(target_height).await;

    assert_network_has_num_blocks(&network, target_height);
    assert_network_has_num_shard_chunks(&network, 2);
    assert_network_has_messages(&network, 1);
    assert_network_has_cast(&network, 1000, cast.hash.clone());
}

#[tokio::test]
async fn test_read_node() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .try_init();

    let num_shards = 2;
    let mut network = TestNetwork::create(3, num_shards).await;
    network.start_validators().await;

    register_fid_and_wait(&mut network, 1000).await;
    let cast = send_cast_and_wait(&mut network, 1000, "Hello, world").await;

    // Wait for the next block to arrive on all nodes
    network.wait_for_block(network.max_block_height() + 1).await;

    network.start_read_node().await;
    network.start_read_node().await;

    let target_height = network.max_block_height();
    assert!(
        network.read_wait_for_block(target_height).await.is_some(),
        "Read nodes did not reach the target block height"
    );

    assert_network_has_num_blocks(&network, target_height);
    assert_network_has_cast(&network, 1000, cast.hash.clone());
}
