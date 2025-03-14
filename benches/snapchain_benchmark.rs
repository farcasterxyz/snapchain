use ed25519_dalek::SigningKey;
use hex;
use informalsystems_malachitebft_metrics::SharedRegistry;
use libp2p::identity::ed25519::Keypair;
use snapchain::consensus::consensus::SystemMessage;
use snapchain::mempool::mempool::{
    self, Mempool, MempoolMessageWithSource, MempoolMessagesRequest, MempoolSource,
};
use snapchain::mempool::routing;
use snapchain::network::gossip::SnapchainGossip;
use snapchain::network::server::MyHubService;
use snapchain::node::snapchain_node::SnapchainNode;
use snapchain::node::snapchain_read_node::SnapchainReadNode;
use snapchain::proto::hub_service_server::HubServiceServer;
use snapchain::proto::{self};
use snapchain::proto::{Block, FarcasterNetwork, IdRegisterEventType, SignerEventType};
use snapchain::storage::db::{PageOptions, RocksDB};
use snapchain::storage::store::engine::MempoolMessage;
use snapchain::storage::store::node_local_state::LocalStateStore;
use snapchain::storage::store::stores::Stores;
use snapchain::storage::store::BlockStore;
use snapchain::utils::factory::{self, messages_factory};
use snapchain::utils::statsd_wrapper::StatsdClientWrapper;
use std::collections::{BTreeSet, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::mpsc::Sender;
use tokio::sync::{broadcast, mpsc};
use tokio::time;
use tonic::transport::Server;
use tracing::error;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

const HOST_FOR_TEST: &str = "127.0.0.1";
const BASE_PORT_FOR_TEST: u32 = 9390;

struct NodeForTest {
    node: SnapchainNode,
    db: Arc<RocksDB>,
    block_store: BlockStore,
    mempool_tx: mpsc::Sender<MempoolMessageWithSource>,
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
        validator_addresses: &Vec<String>,
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
            consensus_config.with((1..=num_shards).collect(), validator_addresses.clone());

        let (system_tx, mut system_rx) = mpsc::channel::<SystemMessage>(100);

        let mut gossip =
            SnapchainGossip::create(keypair.clone(), &config, system_tx.clone(), false).unwrap();
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
        validator_addresses: &Vec<String>,
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
            consensus_config.with((1..=num_shards).collect(), validator_addresses.clone());

        let (system_tx, mut system_rx) = mpsc::channel::<SystemMessage>(100);

        let mut gossip =
            SnapchainGossip::create(keypair.clone(), &config, system_tx.clone(), false).unwrap();
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
            FarcasterNetwork::Testnet,
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
            println!(
                "decided block, hash: {}, height: {:?}, id: {}, chunks: {}",
                hex::encode(&block.hash),
                header.height.as_ref().map(|h| h.block_number),
                node_id,
                chunks_count,
            );
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
            block_store.clone(),
            node.shard_stores.clone(),
            node.shard_senders.clone(),
            statsd_client.clone(),
            num_shards,
            Box::new(routing::EvenOddRouterForTest {}),
            mempool_tx.clone(),
            None,
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
    validator_addresses: Vec<String>,
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
        Self {
            num_validator_nodes,
            num_shards,
            base_grpc_port,
            keypairs,
            validator_addresses,
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
            &self.validator_addresses,
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
            &self.validator_addresses,
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

async fn register_fid(fid: u64, messages_tx: Sender<MempoolMessageWithSource>) -> SigningKey {
    let signer = factory::signers::generate_signer();
    let address = factory::address::generate_random_address();
    messages_tx
        .send((
            MempoolMessage::ValidatorMessage(proto::ValidatorMessage {
                on_chain_event: Some(factory::events_factory::create_rent_event(
                    fid, None, None, false,
                )),
                fname_transfer: None,
            }),
            MempoolSource::Local,
        ))
        .await
        .unwrap();
    messages_tx
        .send((
            MempoolMessage::ValidatorMessage(proto::ValidatorMessage {
                on_chain_event: Some(factory::events_factory::create_signer_event(
                    fid,
                    signer.clone(),
                    SignerEventType::Add,
                    None,
                )),
                fname_transfer: None,
            }),
            MempoolSource::Local,
        ))
        .await
        .unwrap();
    messages_tx
        .send((
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
        ))
        .await
        .unwrap();

    signer
}

fn mempool_to_settlement(base_port: u32) -> std::time::Duration {
    let rt = Builder::new_multi_thread()
        .enable_time()
        .enable_io()
        .build()
        .unwrap();

    rt.block_on(async {
        let num_shards = 2;
        let mut network = TestNetwork::create(3, num_shards, base_port).await;
        network.start_validators().await;

        let messages_tx1 = network.nodes[0].mempool_tx.clone();
        network.produce_blocks(1).await;

        let signer = register_fid(123, messages_tx1.clone()).await;
        let start = std::time::Instant::now();

        // Send messages
        tokio::spawn(async move {
            let mut i = 0i32;
            loop {
                let message =
                    MempoolMessage::UserMessage(messages_factory::casts::create_cast_add(
                        123,
                        format!("Cast {}", i).as_str(),
                        None,
                        Some(&signer),
                    ));
                messages_tx1
                    .send((message, MempoolSource::Local))
                    .await
                    .unwrap();

                // there needs to be some sleep interval or the working thread for benchmarking will be exhausted
                tokio::time::sleep(time::Duration::from_micros(10)).await;
                i += 1;
            }
        });

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

            println!("{} messages", network.nodes[i].total_messages().await);
            assert!(
                network.nodes[i].total_messages().await > 0,
                "Node {} should have messages",
                i
            );
        }
        start.elapsed()
    })
}

fn mempool_to_settlement_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("mempool_sequential");

    group
        .sample_size(10)
        .measurement_time(Duration::from_secs(5))
        .warm_up_time(Duration::from_secs(1));

    for i in 0..10 {
        let base_port = BASE_PORT_FOR_TEST + (i as u32 * 10);

        group.bench_with_input(
            BenchmarkId::new("mempool_test", i),
            &base_port,
            |b, &base_port| b.iter(|| mempool_to_settlement(base_port)),
        );
    }

    group.finish();
}

// criterion_group!(benches, mempool_benchmark);
criterion_group!(benches, mempool_to_settlement_benchmark);
criterion_main!(benches);
