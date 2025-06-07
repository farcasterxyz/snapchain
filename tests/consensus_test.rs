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
use snapchain::mempool::routing;
use snapchain::mempool::routing::{MessageRouter, ShardRouter};
use snapchain::network::gossip::SnapchainGossip;
use snapchain::network::server::MyHubService;
use snapchain::node::snapchain_node::SnapchainNode;
use snapchain::node::snapchain_read_node::SnapchainReadNode;
use snapchain::proto::hub_service_server::HubServiceServer;
use snapchain::proto::{self, Height};
use snapchain::proto::{Block, FarcasterNetwork, IdRegisterEventType, SignerEventType};
use snapchain::storage::db::{PageOptions, RocksDB, RocksDbTransactionBatch};
use snapchain::storage::store::account::UserDataStore;
use snapchain::storage::store::engine::MempoolMessage;
use snapchain::storage::store::node_local_state::LocalStateStore;
use snapchain::storage::store::stores::Stores;
use snapchain::storage::store::BlockStore;
use snapchain::utils::factory::{self, messages_factory};
use snapchain::utils::statsd_wrapper::StatsdClientWrapper;
use std::collections::{BTreeSet, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::{broadcast, mpsc};
use tokio::time;
use tonic::transport::Server;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

const HOST_FOR_TEST: &str = "127.0.0.1";
const BASE_PORT_FOR_TEST: u32 = 9482;

struct NodeForTest {
    pub node: SnapchainNode,
    db: Arc<RocksDB>,
    block_store: BlockStore,
    mempool_tx: mpsc::Sender<MempoolRequest>,
    handles: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for NodeForTest {
    fn drop(&mut self) {
        self.stop();
        self.db.destroy().unwrap();
        for (_, stores) in self.node.shard_stores.iter_mut() {
            stores.shard_store.db.destroy().unwrap();
        }
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

pub async fn num_blocks(block_store: &BlockStore) -> usize {
    let blocks_page = block_store
        .get_blocks(0, None, &PageOptions::default())
        .unwrap();
    blocks_page.blocks.len()
}

pub async fn num_shard_chunks(shard_stores: &HashMap<u32, Stores>) -> usize {
    let mut count = 0;
    for (_shard_id, stores) in shard_stores.iter() {
        count += stores.shard_store.get_shard_chunks(0, None).unwrap().len();
    }

    count
}

pub async fn total_messages(shard_stores: &HashMap<u32, Stores>) -> usize {
    let mut messages_count = 0;
    for (_shard_id, stores) in shard_stores.iter() {
        for chunk in stores.shard_store.get_shard_chunks(0, None).unwrap() {
            for tx in chunk.transactions.iter() {
                messages_count += tx.user_messages.len();
            }
        }
    }
    messages_count
}
struct ReadNodeForTest {
    handles: Vec<tokio::task::JoinHandle<()>>,
    node: SnapchainReadNode,
    db: Arc<RocksDB>,
    block_store: BlockStore,
    _messages_request_rx: mpsc::Receiver<MempoolMessagesRequest>,
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

    pub async fn num_blocks(&self) -> usize {
        num_blocks(&self.block_store).await
    }

    pub async fn num_shard_chunks(&self) -> usize {
        num_shard_chunks(&self.node.shard_stores).await
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

    pub fn id(&self) -> String {
        self.node.id()
    }

    pub async fn num_blocks(&self) -> usize {
        num_blocks(&self.block_store).await
    }

    pub async fn num_shard_chunks(&self) -> usize {
        num_shard_chunks(&self.node.shard_stores).await
    }

    pub async fn total_messages(&self) -> usize {
        total_messages(&self.node.shard_stores).await
    }

    pub fn stop(&self) {
        self.node.stop();
        for handle in self.handles.iter() {
            handle.abort();
        }
    }
}

pub struct TestNetwork {
    num_validator_nodes: u32,
    num_shards: u32,
    base_grpc_port: u32,
    keypairs: Vec<Keypair>,
    validator_sets: Vec<ValidatorSetConfig>,
    gossip_addresses: Vec<String>,
    nodes: Vec<NodeForTest>,
    read_nodes: Vec<ReadNodeForTest>,
}

impl TestNetwork {
    // These networks can be created in parallel, so make sure the base port is far enough part to avoid conflicts
    pub async fn create(num_validator_nodes: u32, num_shards: u32, base_grpc_port: u32) -> Self {
        let mut keypairs = Vec::new();
        let mut validator_addresses = vec![];
        let mut node_addresses = vec![];
        for i in 0..num_validator_nodes {
            let keypair = Keypair::generate();
            keypairs.push(keypair.clone());
            validator_addresses.push(hex::encode(keypair.public().to_bytes()));
            let port = BASE_PORT_FOR_TEST + i;
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
            base_grpc_port,
            keypairs,
            validator_sets,
            gossip_addresses: node_addresses,
            nodes: vec![],
            read_nodes: vec![],
        }
    }

    async fn start_validator_node(&mut self, index: u32) {
        let keypair = self.keypairs[index as usize].clone();
        let gossip_address = self.gossip_addresses[index as usize].clone();
        let grpc_port = self.base_grpc_port + index;
        let node = NodeForTest::create(
            keypair,
            self.num_shards,
            grpc_port,
            &self.validator_sets,
            gossip_address,
            self.gossip_addresses[0].clone(),
        )
        .await;
        self.nodes.push(node);
    }

    async fn start_read_node(&mut self, index: u32) {
        let keypair = Keypair::generate();
        let port = BASE_PORT_FOR_TEST + self.num_validator_nodes + index;
        let gossip_address = format!("/ip4/{HOST_FOR_TEST}/udp/{port}/quic-v1");
        let node = ReadNodeForTest::create(
            keypair,
            self.num_shards,
            &self.validator_sets,
            gossip_address,
            self.gossip_addresses[0].clone(),
        )
        .await;
        self.read_nodes.push(node);
    }

    async fn start_validators(&mut self) {
        for i in 0..self.num_validator_nodes {
            self.start_validator_node(i).await;
        }
    }

    pub async fn produce_blocks(&mut self, num_blocks: u64) {
        let timeout = tokio::time::Duration::from_secs(10);
        let start = tokio::time::Instant::now();
        let mut timer = time::interval(tokio::time::Duration::from_millis(100));

        let num_nodes = self.nodes.len();

        let mut node_ids_with_blocks = BTreeSet::new();
        loop {
            let _ = timer.tick().await;
            for node in self.nodes.iter_mut() {
                if node.num_blocks().await >= num_blocks as usize {
                    node_ids_with_blocks.insert(node.id());
                    if node_ids_with_blocks.len() == num_nodes {
                        break;
                    }
                }
            }

            if start.elapsed() > timeout {
                break;
            }
        }
    }
}

async fn register_fid(fid: u64, messages_tx: Sender<MempoolRequest>) -> SigningKey {
    let signer = factory::signers::generate_signer();
    let address = factory::address::generate_random_address();
    messages_tx
        .send(MempoolRequest::AddMessage(
            MempoolMessage::ValidatorMessage(proto::ValidatorMessage {
                on_chain_event: Some(factory::events_factory::create_rent_event(
                    fid, None, None, false,
                )),
                fname_transfer: None,
            }),
            MempoolSource::Local,
            None,
        ))
        .await
        .unwrap();
    messages_tx
        .send(MempoolRequest::AddMessage(
            MempoolMessage::ValidatorMessage(proto::ValidatorMessage {
                on_chain_event: Some(factory::events_factory::create_signer_event(
                    fid,
                    signer.clone(),
                    SignerEventType::Add,
                    None,
                    None,
                )),
                fname_transfer: None,
            }),
            MempoolSource::Local,
            None,
        ))
        .await
        .unwrap();
    messages_tx
        .send(MempoolRequest::AddMessage(
            MempoolMessage::ValidatorMessage(proto::ValidatorMessage {
                on_chain_event: Some(factory::events_factory::create_id_register_event(
                    fid,
                    IdRegisterEventType::Register,
                    address,
                    None,
                )),
                fname_transfer: None,
            }),
            MempoolSource::Local,
            None,
        ))
        .await
        .unwrap();

    signer
}

async fn transfer_fname(transfer: proto::FnameTransfer, messages_tx: Sender<MempoolRequest>) {
    messages_tx
        .send(MempoolRequest::AddMessage(
            MempoolMessage::ValidatorMessage(proto::ValidatorMessage {
                on_chain_event: None,
                fname_transfer: Some(transfer),
            }),
            MempoolSource::Local,
            None,
        ))
        .await
        .unwrap();
}

async fn send_messages(messages_tx: mpsc::Sender<MempoolRequest>) {
    let mut i: i32 = 0;
    let prefix = vec![0, 0, 0, 0, 0, 0];
    let fid = 123;
    let signer = register_fid(fid, messages_tx.clone()).await;

    loop {
        info!(i, "sending message");

        let mut hash = prefix.clone();
        hash.extend_from_slice(&i.to_be_bytes()); // just for now

        let message = MempoolMessage::UserMessage(messages_factory::casts::create_cast_add(
            fid,
            format!("Cast {}", i).as_str(),
            None,
            Some(&signer),
        ));
        messages_tx
            .send(MempoolRequest::AddMessage(
                message,
                MempoolSource::Local,
                None,
            ))
            .await
            .unwrap();
        i += 1;
        tokio::time::sleep(time::Duration::from_millis(200)).await;
    }
}

#[tokio::test]
#[serial]
async fn test_basic_consensus() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .try_init();

    let num_shards = 2;
    let mut network = TestNetwork::create(3, num_shards, 3380).await;
    network.start_validators().await;

    let messages_tx1 = network.nodes[0].mempool_tx.clone();

    tokio::spawn(async move { send_messages(messages_tx1).await });

    network.produce_blocks(3).await;

    for i in 0..network.nodes.len() {
        assert!(
            network.nodes[i].num_blocks().await >= 3,
            "Node {} should have confirmed blocks",
            i
        );

        assert!(
            network.nodes[i].num_shard_chunks().await >= 3,
            "Node {} should have confirmed blocks",
            i
        );

        assert!(
            network.nodes[i].total_messages().await > 0,
            "Node {} should have messages",
            i
        );
    }
}

async fn wait_for_blocks(node: &NodeForTest, num_blocks: usize) {
    let timeout = tokio::time::Duration::from_secs(10);
    let start = tokio::time::Instant::now();
    let mut timer = time::interval(tokio::time::Duration::from_millis(10));
    let existing_blocks = node.num_blocks().await;

    loop {
        let _ = timer.tick().await;
        if node.num_blocks().await >= existing_blocks + num_blocks {
            break;
        }
        if start.elapsed() > timeout {
            break;
        }
    }

    assert!(
        node.num_blocks().await >= existing_blocks + num_blocks,
        "Node should have confirmed blocks"
    );
    assert!(
        node.num_shard_chunks().await >= existing_blocks + num_blocks,
        "Node should have confirmed shard chunks"
    );
}

#[tokio::test]
#[serial]
async fn test_basic_sync() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .try_init();

    // Set up shard and validators
    let num_shards = 1;

    let mut network = TestNetwork::create(4, num_shards, 3220).await;

    // Start the first three nodes
    for i in 0..3 {
        network.start_validator_node(i).await;
    }

    let messages_tx1 = network.nodes[0].mempool_tx.clone();

    tokio::spawn(async move { send_messages(messages_tx1).await });

    network.produce_blocks(3).await;

    for i in 0..3 {
        assert!(
            network.nodes[i].num_shard_chunks().await >= 3,
            "Node {} should have confirmed chunks",
            i
        );
        assert!(
            network.nodes[i].num_blocks().await >= 3,
            "Node {} should have confirmed blocks",
            i
        );
    }

    // Add the node to the network and start producing blocks again.
    network.start_validator_node(3).await;

    // When we start it the new node should not have any blocks or chunks
    assert_eq!(network.nodes[3].num_shard_chunks().await, 0);
    assert_eq!(network.nodes[3].num_blocks().await, 0);

    network.produce_blocks(10).await;

    // After producing some blocks, the node should've sync'd and have blocks and chunks
    // TODO: The actual number successfully sync'd varies. Figure out why and add a stricter check on exact number of blocks
    wait_for_blocks(&network.nodes[3], 1).await;
}

async fn wait_for_read_node_blocks(node: &ReadNodeForTest, num_blocks: usize) {
    let timeout = tokio::time::Duration::from_secs(6);
    let start = tokio::time::Instant::now();
    let mut timer = time::interval(tokio::time::Duration::from_millis(10));

    loop {
        let _ = timer.tick().await;
        if node.num_blocks().await >= num_blocks {
            break;
        }
        if start.elapsed() > timeout {
            break;
        }
    }

    assert!(
        node.num_blocks().await >= num_blocks,
        "Node should have confirmed blocks. Num blocks {}",
        node.num_blocks().await
    );
    assert!(
        node.num_shard_chunks().await >= num_blocks,
        "Node should have confirmed shard chunks. Num shard chunks {}",
        node.num_shard_chunks().await
    );
}

// TODO: Fix flaky test
#[ignore]
#[tokio::test]
#[serial]
async fn test_read_node() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .try_init();

    let num_shards = 2;
    let mut network = TestNetwork::create(3, num_shards, 3380).await;
    network.start_validators().await;

    let messages_tx1 = network.nodes[0].mempool_tx.clone();

    tokio::spawn(async move { send_messages(messages_tx1).await });

    network.start_read_node(0).await;
    network.start_read_node(1).await;

    for node in network.nodes.iter() {
        wait_for_blocks(node, 3).await;
    }

    for read_node in network.read_nodes.iter() {
        // TODO: The actual number successfully sync'd varies. Figure out why and add a stricter check on exact number of blocks
        wait_for_read_node_blocks(read_node, 1).await;
    }
}

#[tokio::test]
#[serial]
async fn test_cross_shard_interactions() {
    let num_shards = 2;
    let mut network = TestNetwork::create(3, num_shards, 3380).await;
    network.start_validators().await;

    let messages_tx = network.nodes[0].mempool_tx.clone();

    let first_fid = 20270;
    let second_fid = 211428;

    register_fid(first_fid, messages_tx.clone()).await;
    register_fid(second_fid, messages_tx.clone()).await;

    let router = ShardRouter {};
    // Ensure that the two fids are routed to different shards
    assert_ne!(
        router.route_fid(first_fid, 2),
        router.route_fid(second_fid, 2)
    );

    let fname = "erica";

    transfer_fname(
        proto::FnameTransfer {
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
        },
        messages_tx.clone(),
    )
    .await;

    transfer_fname(
        proto::FnameTransfer {
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
        },
        messages_tx.clone(),
    )
    .await;

    tokio::time::sleep(time::Duration::from_millis(200)).await;

    network.produce_blocks(1).await;

    for i in 0..network.nodes.len() {
        assert!(
            network.nodes[i].num_blocks().await > 0,
            "Node {} should have confirmed blocks",
            i
        );

        let node = &network.nodes[i].node;
        node.shard_stores.iter().for_each(|(_, stores)| {
            let proof = UserDataStore::get_username_proof(
                &stores.user_data_store,
                &mut RocksDbTransactionBatch::new(),
                &fname.as_bytes().to_vec(),
            )
            .unwrap()
            .unwrap();

            assert_eq!(proof.fid, first_fid);
        });
    }
}
