use ed25519_dalek::SigningKey;
use hex;
use informalsystems_malachitebft_metrics::SharedRegistry;
use libp2p::identity::ed25519::Keypair;
use serial_test::serial;
use snapchain::connectors::onchain_events::ChainClients;
use snapchain::consensus::consensus::{SystemMessage, ValidatorSetConfig};
use snapchain::consensus::proposer::GENESIS_MESSAGE;
use snapchain::mempool::mempool::{
    self, Mempool, MempoolMessagesRequest, MempoolRequest, MempoolSource,
};
use snapchain::mempool::routing::{self, MessageRouter, ShardRouter};
use snapchain::network::gossip::SnapchainGossip;
use snapchain::network::server::MyHubService;
use snapchain::node::snapchain_node::SnapchainNode;
use snapchain::node::snapchain_read_node::SnapchainReadNode;
use snapchain::proto::hub_service_server::HubServiceServer;
use snapchain::proto::{self, Height};
use snapchain::proto::{Block, FarcasterNetwork, IdRegisterEventType, SignerEventType};
use snapchain::storage::db::{PageOptions, RocksDB, RocksDbTransactionBatch};
use snapchain::storage::store::account::{CastStore, OnchainEventStore, UserDataStore};
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

async fn wait_for<F, T>(f: F, timeout: time::Duration, tick: time::Duration) -> Option<T>
where
    F: Fn() -> Option<T>,
{
    let start = tokio::time::Instant::now();

    loop {
        if let Some(result) = f() {
            return Some(result);
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

    fn fid_registered(&self, fid: u64) -> Option<proto::OnChainEvent> {
        for stores in self.shard_stores().values() {
            if let Ok(result) =
                OnchainEventStore::get_id_register_event_by_fid(&stores.onchain_event_store, fid)
            {
                if result.is_some() {
                    return Some(result.unwrap());
                }
            }
        }

        return None;
    }

    fn cast_added(&self, fid: u64, hash: Vec<u8>) -> Option<proto::Message> {
        for stores in self.shard_stores().values() {
            if let Ok(result) = CastStore::get_cast_add(&stores.cast_store, fid, hash.clone()) {
                if result.is_some() {
                    return result;
                }
            }
        }
        None
    }

    fn get_username_proof(&self, fname: String) -> Option<proto::UserNameProof> {
        for stores in self.shard_stores().values() {
            let proof = UserDataStore::get_username_proof(
                &stores.user_data_store,
                &mut RocksDbTransactionBatch::new(),
                &fname.as_bytes().to_vec(),
            )
            .unwrap();

            if proof.is_some() {
                return proof;
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
            fc_network,
            mempool_rx,
            messages_request_rx,
            num_shards,
            node.shard_stores.clone(),
            gossip_tx.clone(),
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
            gossip_tx.clone(),
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

    pub async fn wait_for_fid(&self, fid: u64) -> Option<proto::OnChainEvent> {
        wait_for(
            || {
                if self
                    .nodes
                    .iter()
                    .all(|node| node.fid_registered(fid).is_some())
                {
                    return self.nodes[0].fid_registered(fid);
                }

                None
            },
            tokio::time::Duration::from_secs(5),
            tokio::time::Duration::from_millis(100),
        )
        .await
    }

    pub async fn register_and_wait_for_fid(&mut self, fid: u64) -> Option<proto::OnChainEvent> {
        self.register_fid(fid).await;
        self.wait_for_fid(fid).await
    }

    pub async fn send_cast(&mut self, fid: u64, text: &str) -> proto::Message {
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

    pub async fn wait_for_cast(&self, fid: u64, hash: Vec<u8>) -> Option<proto::Message> {
        wait_for(
            || {
                if self
                    .nodes
                    .iter()
                    .all(|node| node.cast_added(fid, hash.clone()).is_some())
                {
                    return self.nodes[0].cast_added(fid, hash.clone());
                }

                None
            },
            tokio::time::Duration::from_secs(5),
            tokio::time::Duration::from_millis(100),
        )
        .await
    }

    pub async fn send_and_wait_for_cast(&mut self, fid: u64, text: &str) -> Option<proto::Message> {
        let message = self.send_cast(fid, text).await;
        self.wait_for_cast(fid, message.hash.clone()).await
    }

    // Waits for all validator nodes to reach at least `height` blocks.
    pub async fn wait_for_block(&self, height: usize) -> Option<()> {
        wait_for(
            || {
                self.nodes
                    .iter()
                    .all(|node| node.num_blocks() >= height)
                    .then_some(())
            },
            tokio::time::Duration::from_secs(15),
            tokio::time::Duration::from_millis(100),
        )
        .await
    }

    // Waits for all reader nodes to reach at least `height` blocks.
    pub async fn read_wait_for_block(&self, height: usize) -> Option<()> {
        wait_for(
            || {
                self.read_nodes
                    .iter()
                    .all(|node| node.num_blocks() >= height)
                    .then_some(())
            },
            tokio::time::Duration::from_secs(15),
            tokio::time::Duration::from_millis(100),
        )
        .await
    }

    // Waits for a username to be registered to a specific FID.
    pub async fn wait_for_username_registered_to_fid(
        &self,
        fid: u64,
        fname: String,
    ) -> Option<proto::UserNameProof> {
        wait_for(
            || {
                let all_have_proof = self.nodes.iter().all(|node| {
                    if let Some(proof) = node.get_username_proof(fname.clone()) {
                        proof.fid == fid
                    } else {
                        false
                    }
                });

                if all_have_proof {
                    return self.nodes[0].get_username_proof(fname.clone());
                }

                None
            },
            tokio::time::Duration::from_secs(5),
            tokio::time::Duration::from_millis(100),
        )
        .await
    }
}

fn on_all_nodes<F>(network: &TestNetwork, f: F)
where
    F: Fn(&dyn Node, bool, usize) -> (),
{
    for (i, node) in network.nodes.iter().enumerate() {
        f(node, false, i);
    }

    for (i, node) in network.read_nodes.iter().enumerate() {
        f(node, true, i);
    }
}

fn assert_network_has_num_blocks(network: &TestNetwork, num_blocks: usize) {
    on_all_nodes(network, |node, is_read, index| {
        assert!(
            node.num_blocks() >= num_blocks,
            "Node (read={}, idx={}) should have confirmed at least {} blocks, but has {}",
            is_read,
            index,
            num_blocks,
            node.num_blocks()
        );
    });
}

fn assert_network_has_num_shard_chunks(network: &TestNetwork, num_chunks: usize) {
    on_all_nodes(network, |node, is_read, index| {
        assert!(
            node.num_shard_chunks() >= num_chunks,
            "Node (read={}, idx={}) should have confirmed at least {} shard chunks, but has {}",
            is_read,
            index,
            num_chunks,
            node.num_shard_chunks()
        );
    });
}

fn assert_network_has_messages(network: &TestNetwork, num_messages: usize) {
    on_all_nodes(network, |node, is_read, index| {
        assert!(
            node.total_messages() >= num_messages,
            "Node (read={}, idx={}) should have at least {} messages, but has {}",
            is_read,
            index,
            num_messages,
            node.total_messages()
        );
    });
}

fn assert_network_has_cast(network: &TestNetwork, fid: u64, hash: Vec<u8>) {
    on_all_nodes(network, |node, is_read, index| {
        assert!(
            node.cast_added(fid, hash.clone()).is_some(),
            "Node (read={}, idx={}) should have cast message with fid {} and hash {}, but it was not found",
            is_read,
            index,
            fid,
            hex::encode(hash.clone())
        );
    });
}

#[tokio::test]
#[serial]
async fn test_basic_consensus() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .try_init();

    let num_shards = 2;
    let mut network = TestNetwork::create(3, num_shards).await;
    network.start_validators().await;

    network.register_and_wait_for_fid(1000).await;
    let cast = network
        .send_and_wait_for_cast(1000, "Hello, world")
        .await
        .unwrap();

    // Wait for nodes to reach the next block height
    let next_block_height = network.max_block_height() + 1;
    network.wait_for_block(next_block_height).await;

    assert_network_has_num_blocks(&network, next_block_height);
    assert_network_has_num_shard_chunks(&network, 3);
    assert_network_has_messages(&network, 1);

    // Assert that all nodes have the cast message
    assert_network_has_cast(&network, 1000, cast.hash.clone());
}

#[tokio::test]
#[serial]
async fn test_basic_sync() {
    // Set up shard and validators
    let num_shards = 2;
    let mut network = TestNetwork::create(4, num_shards).await;

    // Start the first three nodes
    for i in 0..3 {
        network.start_validator_node(i).await;
    }

    network.register_and_wait_for_fid(1000).await;
    let cast = network
        .send_and_wait_for_cast(1000, "Hello, world")
        .await
        .unwrap();

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

    // Assert that all nodes have the cast message
    assert_network_has_cast(&network, 1000, cast.hash.clone());
}

#[tokio::test]
#[serial]
async fn test_read_node() {
    let num_shards = 2;
    let mut network = TestNetwork::create(3, num_shards).await;
    network.start_validators().await;

    network.register_and_wait_for_fid(1000).await;
    let cast = network
        .send_and_wait_for_cast(1000, "Hello, world")
        .await
        .unwrap();

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

    // Assert that all nodes have the cast message
    assert_network_has_cast(&network, 1000, cast.hash.clone());
}

#[tokio::test]
#[serial]
async fn test_cross_shard_interactions() {
    let num_shards = 2;
    let mut network = TestNetwork::create(3, num_shards).await;
    network.start_validators().await;

    let first_fid = 20270;
    let second_fid = 211428;

    network.register_and_wait_for_fid(first_fid).await;
    network.register_and_wait_for_fid(second_fid).await;

    let router = ShardRouter {};
    // Ensure that the two fids are routed to different shards
    assert_ne!(
        router.route_fid(first_fid, num_shards),
        router.route_fid(second_fid, num_shards)
    );

    let fname = "erica";

    let transfer1 = proto::ValidatorMessage {
        on_chain_event: None,
        fname_transfer: Some(proto::FnameTransfer {
            id: 43782,
            from_fid: 0,
            proof: Some(proto::UserNameProof {
                timestamp: 1741384226,
                name: fname.as_bytes().to_vec(),
                fid: second_fid,
                owner: hex::decode("2b4d92e7626c5fc56cb4641f6f758563de1f6bdc").unwrap(),
                signature: hex::decode("050b42fdda7b0a7309a1fb8a2cbc9a5f4bbf241aec74f53191f9665d9b9f572d4f452ac807911af7b6980219482d6f7fda7f99f23ab19c961b4701b9934fa2f91b").unwrap(),
                r#type: proto::UserNameType::UsernameTypeFname as i32,
            }),
        })
    };

    let transfer2 = proto::ValidatorMessage {
        on_chain_event: None,
        fname_transfer: Some(proto::FnameTransfer {
            id: 829595,
            from_fid: second_fid,
            proof: Some(proto::UserNameProof {
                timestamp: 1741384226,
                name: fname.as_bytes().to_vec(),
                fid: first_fid,
                owner: hex::decode("92ce59c18a97646e9a7e011653d8417d3a08bb2b").unwrap(),
                signature: hex::decode("00c3601c515edffe208e7128f47f89c2fb7b8e0beaaf615158305ddf02818a71679a8e7062503be59a19d241bd0b47396a3c294cfafd0d5478db1ae8249463bd1c").unwrap(),
                r#type: proto::UserNameType::UsernameTypeFname as i32,
            }),
        })
    };

    let node = &network.nodes[0];

    node.add_message(
        MempoolMessage::ValidatorMessage(transfer1.clone()),
        MempoolSource::Local,
        None,
    )
    .await
    .unwrap();

    node.add_message(
        MempoolMessage::ValidatorMessage(transfer2.clone()),
        MempoolSource::Local,
        None,
    )
    .await
    .unwrap();

    // Wait for the proof by fname + fid to exist
    network
        .wait_for_username_registered_to_fid(first_fid, fname.to_string())
        .await
        .unwrap();
}
