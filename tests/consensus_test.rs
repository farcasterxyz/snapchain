use ed25519_dalek::SigningKey;
use hex;
use informalsystems_malachitebft_metrics::SharedRegistry;
use libp2p::identity::ed25519::Keypair;
use libp2p::PeerId;
use serial_test::serial;
use snapchain::connectors::onchain_events::ChainClients;
use snapchain::consensus::consensus::{SystemMessage, ValidatorSetConfig};
use snapchain::consensus::proposer::GENESIS_MESSAGE;
use snapchain::mempool::block_receiver::{self, BlockReceiver};
use snapchain::mempool::mempool::{
    self, Mempool, MempoolMessagesRequest, MempoolRequest, MempoolSource,
};
use snapchain::mempool::routing::{self, MessageRouter, ShardRouter};
use snapchain::network::gossip::SnapchainGossip;
use snapchain::network::server::MyHubService;
use snapchain::node::snapchain_node::SnapchainNode;
use snapchain::node::snapchain_read_node::SnapchainReadNode;
use snapchain::proto::hub_service_client::HubServiceClient;
use snapchain::proto::hub_service_server::HubServiceServer;
use snapchain::proto::{self, CastId, Height, HubEventType, StorageUnitType, SubscribeRequest};
use snapchain::proto::{Block, FarcasterNetwork, IdRegisterEventType, SignerEventType};
use snapchain::storage::db::{PageOptions, RocksDB, RocksDbTransactionBatch};
use snapchain::storage::store::account::{CastStore, OnchainEventStore, UserDataStore};
use snapchain::storage::store::block_engine::BlockStores;
use snapchain::storage::store::mempool_poller::MempoolMessage;
use snapchain::storage::store::node_local_state::LocalStateStore;
use snapchain::storage::store::stores::Stores;
use snapchain::storage::trie::merkle_trie::{self, TrieKey};
use snapchain::utils::factory::{self, messages_factory};
use snapchain::utils::statsd_wrapper::StatsdClientWrapper;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::sync::{broadcast, mpsc};
use tokio::time;
use tonic::transport::{Channel, Server};
use tonic::Request;
use tracing::{error, info};
// use tracing_subscriber::EnvFilter;

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
    fn block_stores(&self) -> &BlockStores;
    fn shard_stores(&self) -> &HashMap<u32, Stores>;

    fn num_blocks(&self) -> usize {
        let blocks_page = self
            .block_stores()
            .block_store
            .get_blocks(0, None, &PageOptions::default())
            .unwrap();
        blocks_page.blocks.len()
    }

    fn num_shard_chunks(&self, shard_id: u32) -> usize {
        let stores = self.shard_stores().get(&shard_id).unwrap();
        stores.shard_store.get_shard_chunks(0, None).unwrap().len()
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

    fn num_block_events(&self) -> u64 {
        self.block_stores().block_event_store.max_seqnum().unwrap()
    }

    fn fid_registered(&self, fid: u64) -> Option<proto::OnChainEvent> {
        for stores in self.shard_stores().values() {
            if let Ok(result) = OnchainEventStore::get_id_register_event_by_fid(
                &stores.onchain_event_store,
                fid,
                None,
            ) {
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
    mempool_tx: mpsc::Sender<MempoolRequest>,
    handles: Vec<tokio::task::JoinHandle<()>>,
    grpc_port: u32,
    global_data_dir: String,
    shards_data_dir: String,
    keep_db_on_drop: bool,
    peer_id: PeerId,
    peer_blocklist: Arc<Mutex<HashSet<PeerId>>>,
    /// Shared `MyHubService` so the HTTP smoke test can boot an HTTP server
    /// against the same underlying stores + mempool wiring as the gRPC server.
    hub_service: Arc<MyHubService>,
}

impl Node for NodeForTest {
    fn block_stores(&self) -> &BlockStores {
        &self.node.block_stores
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
        if !self.keep_db_on_drop {
            self.db.destroy().unwrap();
            for (_, stores) in self.node.shard_stores.iter_mut() {
                stores.shard_store.db.destroy().unwrap();
            }
        }
    }
}

struct ReadNodeForTest {
    handles: Vec<tokio::task::JoinHandle<()>>,
    node: SnapchainReadNode,
    db: Arc<RocksDB>,
    _messages_request_rx: mpsc::Receiver<MempoolMessagesRequest>,
}

impl Node for ReadNodeForTest {
    fn block_stores(&self) -> &BlockStores {
        &self.node.block_stores
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

        let fc_network = FarcasterNetwork::Devnet;
        let mut gossip = SnapchainGossip::create(
            keypair.clone(),
            &config,
            Some(system_tx.clone()),
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
        let (_, messages_request_rx) = mpsc::channel(100);
        let node = SnapchainReadNode::create(
            keypair.clone(),
            consensus_config,
            peer_id,
            gossip_tx.clone(),
            system_tx.clone(),
            make_tmp_path(),
            statsd_client.clone(),
            fc_network,
            registry,
            None,
            None,
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
        Self::create_with_options(
            keypair,
            num_shards,
            grpc_port,
            validator_sets,
            gossip_address,
            bootstrap_address,
            None,
            None,
            false,
        )
        .await
    }

    /// Like `create`, but lets the caller pin the on-disk paths and choose whether the DBs
    /// should survive a `Drop`. Used by warm-restart and crash-recovery tests so the
    /// validator can be torn down and brought back up against the same (or fresh) state.
    pub async fn create_with_options(
        keypair: Keypair,
        num_shards: u32,
        grpc_port: u32,
        validator_sets: &Vec<ValidatorSetConfig>,
        gossip_address: String,
        bootstrap_address: String,
        global_data_dir: Option<String>,
        shards_data_dir: Option<String>,
        keep_db_on_drop: bool,
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
        // TODO(aditi): We need to figure out the right values for these timeouts. 1s (default) is long, but these are too short.
        // consensus_config.propose_time = time::Duration::from_millis(250);
        // consensus_config.prevote_time = time::Duration::from_millis(100);
        // consensus_config.precommit_time = time::Duration::from_millis(100);
        // consensus_config.step_delta = time::Duration::from_millis(100);
        consensus_config.sync_status_update_interval = time::Duration::from_millis(250); // This is aggressive but makes sync faster in the tests

        let (system_tx, mut system_rx) = mpsc::channel::<SystemMessage>(100);
        let fc_network = FarcasterNetwork::Devnet;

        let mut gossip = SnapchainGossip::create(
            keypair.clone(),
            &config,
            Some(system_tx.clone()),
            false,
            fc_network,
            statsd_client.clone(),
        )
        .await
        .unwrap();
        let gossip_tx = gossip.tx.clone();

        let registry = SharedRegistry::global();
        let peer_id = gossip.swarm.local_peer_id().clone();
        // Captured before `gossip` is moved into the spawn task below — tests
        // reach in to install partition-blocklist entries from this handle.
        let peer_blocklist = gossip.peer_blocklist_handle();
        println!("StartNode validator peer id: {}", peer_id.to_string());
        let (block_tx, mut block_rx) = broadcast::channel::<Block>(100);
        let global_data_dir = global_data_dir.unwrap_or_else(make_tmp_path);
        let shards_data_dir = shards_data_dir.unwrap_or_else(make_tmp_path);
        let db = Arc::new(RocksDB::new(&global_data_dir));
        db.open().unwrap();
        let global_db = RocksDB::open_global_db(&global_data_dir);
        let node_local_store = LocalStateStore::new(global_db);
        let (messages_request_tx, messages_request_rx) = mpsc::channel(100);
        let (shard_decision_tx, shard_decision_rx) = broadcast::channel(100);
        let node = SnapchainNode::create(
            keypair.clone(),
            consensus_config.clone(),
            peer_id,
            gossip_tx.clone(),
            shard_decision_tx,
            Some(block_tx.clone()),
            messages_request_tx,
            node_local_store,
            shards_data_dir.clone(),
            statsd_client.clone(),
            fc_network,
            registry,
            None,
            None,
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
            while let Ok(block) = block_rx.recv().await {
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
            node.block_stores.clone(),
            gossip_tx.clone(),
            shard_decision_rx,
            block_tx.subscribe(),
            statsd_client.clone(),
        );
        let handle = tokio::spawn(async move { mempool.run().await });
        join_handles.push(handle);

        let hub_service = Arc::new(MyHubService::new(
            "".to_string(),
            node.block_stores.clone(),
            node.shard_stores.clone(),
            node.shard_senders.clone(),
            statsd_client.clone(),
            num_shards,
            FarcasterNetwork::Devnet,
            // Match the router used inside `Mempool` (`ShardRouter`). Using a different
            // router here would route gRPC `submit_message` simulations to a different
            // shard than the one mempool/consensus committed the on-chain registrations
            // to, causing "unknown fid" failures despite the FID being registered.
            Box::new(routing::ShardRouter {}),
            mempool_tx.clone(),
            gossip_tx.clone(),
            ChainClients {
                chain_api_map: Default::default(),
            },
            "".to_string(),
            "".to_string(),
        ));

        let grpc_service = hub_service.clone();
        let handle = tokio::spawn(async move {
            let grpc_socket_addr: SocketAddr = addr.parse().unwrap();
            let resp = Server::builder()
                .add_service(HubServiceServer::from_arc(grpc_service))
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

        for shard_id in 1..num_shards + 1 {
            let senders = node.shard_senders.get(&shard_id).unwrap();
            let mut block_receiver = BlockReceiver {
                shard_id: shard_id,
                stores: node.shard_stores.get(&shard_id).unwrap().clone(),
                block_rx: block_tx.subscribe(),
                mempool_tx: mempool_tx.clone(),
                system_tx: system_tx.clone(),
                event_rx: senders.events_tx.subscribe(),
                validator_sets: consensus_config.to_stored_validator_sets(shard_id),
                config: block_receiver::Config {
                    enabled: true,
                    ..block_receiver::Config::default()
                },
            };
            let handle = tokio::spawn(async move { block_receiver.run().await });
            join_handles.push(handle);
        }

        let node_for_dispatch = node.clone();
        let handle = tokio::spawn(async move {
            loop {
                if let Some(system_event) = system_rx.recv().await {
                    match system_event {
                        SystemMessage::MalachiteNetwork(event_shard, event) => {
                            node_for_dispatch.dispatch(event_shard, event);
                        }
                        SystemMessage::BlockRequest {
                            block_event_seqnum,
                            block_tx,
                        } => {
                            let block = node_for_dispatch
                                .block_stores
                                .get_block_by_event_seqnum(block_event_seqnum);
                            block_tx.send(block).unwrap();
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
            mempool_tx,
            handles: join_handles,
            grpc_port,
            global_data_dir,
            shards_data_dir,
            keep_db_on_drop,
            peer_id,
            peer_blocklist,
            hub_service,
        }
    }

    /// Returns a shared handle to this validator's `MyHubService`. Used by the
    /// HTTP smoke test to boot an HTTP server on top of the same stores and
    /// mempool wiring that the gRPC server is using.
    pub fn hub_service(&self) -> Arc<MyHubService> {
        self.hub_service.clone()
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

    /// Dial this validator's gRPC listener and return a fresh client.
    /// The server binds 0.0.0.0:{grpc_port}; clients dial 127.0.0.1.
    pub async fn client(&self) -> HubServiceClient<Channel> {
        let addr = format!("http://127.0.0.1:{}", self.grpc_port);
        // Retry briefly because the server may still be starting up on the first call after creation.
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
        loop {
            match HubServiceClient::connect(addr.clone()).await {
                Ok(client) => return client,
                Err(e) => {
                    if tokio::time::Instant::now() >= deadline {
                        panic!("Failed to connect to gRPC at {}: {:?}", addr, e);
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                }
            }
        }
    }

    pub async fn submit_message_via_grpc(
        &self,
        message: proto::Message,
    ) -> Result<proto::Message, tonic::Status> {
        let mut client = self.client().await;
        client
            .submit_message(Request::new(message))
            .await
            .map(|r| r.into_inner())
    }

    pub async fn get_cast_via_grpc(
        &self,
        fid: u64,
        hash: Vec<u8>,
    ) -> Result<proto::Message, tonic::Status> {
        let mut client = self.client().await;
        client
            .get_cast(Request::new(CastId { fid, hash }))
            .await
            .map(|r| r.into_inner())
    }

    pub fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    /// Inserts the given peer ids into this validator's gossip blocklist —
    /// any inbound gossip whose immediate sender matches will be dropped.
    /// Used by partition tests; production leaves the set empty.
    pub fn block_peers(&self, peers: &[PeerId]) {
        let mut set = self.peer_blocklist.lock().unwrap();
        for p in peers {
            set.insert(*p);
        }
    }

    pub fn clear_blocklist(&self) {
        self.peer_blocklist.lock().unwrap().clear();
    }
}

pub struct TestNetwork {
    num_validator_nodes: u32,
    num_shards: u32,
    keypairs: Vec<Keypair>,
    validator_sets: Vec<ValidatorSetConfig>,
    gossip_addresses: Vec<String>,
    /// Slot per validator. `None` means the validator is currently stopped or paused.
    /// Slots stay index-stable so `restart_validator_node(i)` lands at the same index.
    nodes: Vec<Option<NodeForTest>>,
    read_nodes: Vec<ReadNodeForTest>,
    test_fids: HashMap<u64, (SigningKey, Vec<u8>)>, // FID -> (signer, address)
}

impl TestNetwork {
    // These networks can be created in parallel, so make sure the base port is far enough part to avoid conflicts
    pub async fn create(num_validator_nodes: u32, num_shards: u32) -> Self {
        let validator_sets = vec![ValidatorSetConfig {
            effective_at: 0,
            // Filled in after we generate keypairs below.
            validator_public_keys: vec![],
            shard_ids: (1..=num_shards).collect(),
        }];
        Self::create_with_validator_sets(num_validator_nodes, num_shards, validator_sets).await
    }

    /// Creates a TestNetwork with explicit `ValidatorSetConfig` entries. The
    /// `validator_public_keys` of each entry is replaced with the freshly-generated
    /// keypairs so callers don't need to know addresses up-front; if a passed entry
    /// already has a non-empty list, those addresses are kept (used by rotation tests
    /// that want to express "validators [0,1,3] effective at H").
    pub async fn create_with_validator_sets(
        num_validator_nodes: u32,
        num_shards: u32,
        mut validator_sets: Vec<ValidatorSetConfig>,
    ) -> Self {
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

        // For any set whose validator_public_keys is empty, default to "all validators".
        // For sets where the caller passed indices (encoded as the literal string `idx:N`),
        // resolve them against the freshly generated keypairs.
        for set in &mut validator_sets {
            if set.validator_public_keys.is_empty() {
                set.validator_public_keys = validator_addresses.clone();
            } else {
                set.validator_public_keys = set
                    .validator_public_keys
                    .iter()
                    .map(|key| {
                        if let Some(rest) = key.strip_prefix("idx:") {
                            let idx: usize = rest.parse().expect("idx:<n> must parse");
                            validator_addresses[idx].clone()
                        } else {
                            key.clone()
                        }
                    })
                    .collect();
            }
            if set.shard_ids.is_empty() {
                set.shard_ids = (1..=num_shards).collect();
            }
        }

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

    /// Iterates validators that are currently running (i.e. not stopped/paused).
    fn live_nodes(&self) -> impl Iterator<Item = &NodeForTest> {
        self.nodes.iter().filter_map(|n| n.as_ref())
    }

    /// First running validator — convenience for "any healthy node" reads/submissions.
    fn first_live_node(&self) -> &NodeForTest {
        self.live_nodes()
            .next()
            .expect("no live validators in the network")
    }

    /// Bootstrap address list using only live validators' gossip addresses, used when
    /// (re)starting a node so it dials peers that are actually accepting connections.
    fn live_bootstrap_addresses(&self) -> String {
        let live_indices: std::collections::HashSet<usize> = self
            .nodes
            .iter()
            .enumerate()
            .filter_map(|(i, n)| n.as_ref().map(|_| i))
            .collect();
        self.gossip_addresses
            .iter()
            .enumerate()
            .filter_map(|(i, addr)| live_indices.contains(&i).then(|| addr.clone()))
            .collect::<Vec<_>>()
            .join(",")
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
        // Pad the slot vector if needed and place the node at its assigned index.
        while self.nodes.len() <= index as usize {
            self.nodes.push(None);
        }
        self.nodes[index as usize] = Some(node);
    }

    /// Stops the validator at `index` without preserving its on-disk state. The
    /// surviving validators must keep producing blocks if their voting power exceeds
    /// the BFT threshold. Use `restart_validator_node` to bring it back from a fresh
    /// DB (catching up via consensus sync) or `resume_validator_node` to bring it
    /// back against the original DB (after a `pause_validator_node`).
    pub async fn stop_validator_node(&mut self, index: usize) {
        if let Some(node) = self.nodes.get_mut(index).and_then(|s| s.take()) {
            // Drop runs cleanup synchronously: stops the node, aborts handles, destroys DBs
            // (because `keep_db_on_drop` was false for the default code path).
            drop(node);
            // Give the OS a moment to release the gossip + gRPC ports.
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }

    /// Brings a stopped validator back online with a fresh tempdir DB. The
    /// keypair (and therefore validator-set identity) is preserved, so the node
    /// rejoins the existing validator set and catches up via consensus sync.
    pub async fn restart_validator_node(&mut self, index: usize) {
        assert!(
            self.nodes.get(index).map(|s| s.is_none()).unwrap_or(true),
            "restart_validator_node called on a slot that is still occupied"
        );
        let keypair = self.keypairs[index].clone();
        // Allocate a fresh gossip address — re-binding the original port
        // immediately after a stop is racy, especially with QUIC.
        let port = get_available_port();
        let new_gossip_address = format!("/ip4/{HOST_FOR_TEST}/udp/{port}/quic-v1");
        self.gossip_addresses[index] = new_gossip_address.clone();
        let grpc_port = get_available_port();
        let bootstrap = self.live_bootstrap_addresses();
        let node = NodeForTest::create(
            keypair,
            self.num_shards,
            grpc_port,
            &self.validator_sets,
            new_gossip_address,
            bootstrap,
        )
        .await;
        while self.nodes.len() <= index {
            self.nodes.push(None);
        }
        self.nodes[index] = Some(node);
    }

    /// Pauses a validator while keeping its RocksDBs intact, so a subsequent
    /// `resume_validator_node` boots against the same on-disk state. Tests the
    /// WAL replay / state recovery path that production hits on every deploy.
    pub async fn pause_validator_node(&mut self, index: usize) -> (String, String) {
        let slot = self.nodes.get_mut(index).expect("validator index in range");
        let mut node = slot.take().expect("validator already paused/stopped");
        node.keep_db_on_drop = true;
        let global = node.global_data_dir.clone();
        let shards = node.shards_data_dir.clone();
        drop(node);
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        (global, shards)
    }

    /// Resumes a paused validator using its preserved on-disk state. Allocates
    /// fresh gossip + gRPC ports because the previous ports may not be reusable
    /// immediately after the bind was released.
    pub async fn resume_validator_node(
        &mut self,
        index: usize,
        global_data_dir: String,
        shards_data_dir: String,
    ) {
        let keypair = self.keypairs[index].clone();
        let port = get_available_port();
        let new_gossip_address = format!("/ip4/{HOST_FOR_TEST}/udp/{port}/quic-v1");
        self.gossip_addresses[index] = new_gossip_address.clone();
        let grpc_port = get_available_port();
        let bootstrap = self.live_bootstrap_addresses();
        let node = NodeForTest::create_with_options(
            keypair,
            self.num_shards,
            grpc_port,
            &self.validator_sets,
            new_gossip_address,
            bootstrap,
            Some(global_data_dir),
            Some(shards_data_dir),
            false, // resume's lifetime ends with the test, default cleanup is fine
        )
        .await;
        while self.nodes.len() <= index {
            self.nodes.push(None);
        }
        self.nodes[index] = Some(node);
    }

    /// Installs blocklists on the two given partitions so that gossip flowing
    /// across the partition boundary is dropped on receive. Calling on already
    /// partitioned nodes is additive. `heal_partition` clears every node's set.
    pub fn partition_validators(&self, side_a: &[usize], side_b: &[usize]) {
        let a_peers: Vec<PeerId> = side_a
            .iter()
            .filter_map(|i| {
                self.nodes
                    .get(*i)
                    .and_then(|s| s.as_ref())
                    .map(|n| n.peer_id())
            })
            .collect();
        let b_peers: Vec<PeerId> = side_b
            .iter()
            .filter_map(|i| {
                self.nodes
                    .get(*i)
                    .and_then(|s| s.as_ref())
                    .map(|n| n.peer_id())
            })
            .collect();
        for i in side_a {
            if let Some(node) = self.nodes.get(*i).and_then(|s| s.as_ref()) {
                node.block_peers(&b_peers);
            }
        }
        for i in side_b {
            if let Some(node) = self.nodes.get(*i).and_then(|s| s.as_ref()) {
                node.block_peers(&a_peers);
            }
        }
    }

    pub fn heal_partition(&self) {
        for slot in &self.nodes {
            if let Some(node) = slot.as_ref() {
                node.clear_blocklist();
            }
        }
    }

    /// Like `wait_for_block`, but only requires the validators at the given
    /// indices to reach `height`. Used by crash/partition tests where some
    /// validators are intentionally offline or isolated.
    pub async fn wait_for_block_on_subset(
        &self,
        indices: &[usize],
        height: usize,
        timeout: tokio::time::Duration,
    ) -> Option<()> {
        wait_for(
            || {
                indices
                    .iter()
                    .filter_map(|i| self.nodes.get(*i).and_then(|s| s.as_ref()))
                    .all(|node| node.num_blocks() >= height)
                    .then_some(())
            },
            timeout,
            tokio::time::Duration::from_millis(100),
        )
        .await
    }

    /// Polls the gRPC `GetCast` endpoint of the validators at the given indices
    /// until they all return the message, or the timeout expires. Used by
    /// crash/partition tests so the dead/isolated validators don't fail the wait.
    pub async fn wait_for_cast_on_subset(
        &self,
        indices: &[usize],
        fid: u64,
        hash: Vec<u8>,
        timeout: tokio::time::Duration,
    ) -> Option<proto::Message> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let mut all_ok = true;
            let mut last_message: Option<proto::Message> = None;
            for i in indices {
                let Some(node) = self.nodes.get(*i).and_then(|s| s.as_ref()) else {
                    all_ok = false;
                    break;
                };
                match node.get_cast_via_grpc(fid, hash.clone()).await {
                    Ok(msg) => last_message = Some(msg),
                    Err(_) => {
                        all_ok = false;
                        break;
                    }
                }
            }
            if all_ok && last_message.is_some() {
                return last_message;
            }
            if tokio::time::Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }

    /// Returns the latest committed block height observed by validator at `index`.
    pub fn block_height_at(&self, index: usize) -> usize {
        self.nodes
            .get(index)
            .and_then(|s| s.as_ref())
            .map(|n| n.num_blocks())
            .unwrap_or(0)
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

    pub fn max_block_event_seqnum(&self) -> u64 {
        self.live_nodes()
            .map(|node| node.num_block_events())
            .max()
            .unwrap_or(0)
    }

    pub fn max_block_height(&self) -> usize {
        self.live_nodes()
            .map(|node| node.num_blocks())
            .max()
            .unwrap_or(0)
    }

    pub fn max_shard_height(&self, shard_id: u32) -> usize {
        self.live_nodes()
            .map(|node| node.num_shard_chunks(shard_id))
            .max()
            .unwrap_or(0)
    }

    pub async fn register_fid(&mut self, fid: u64) {
        let signer = factory::signers::generate_signer();
        let address = factory::address::generate_random_address();

        let on_chain_events = vec![
            factory::events_factory::create_rent_event(
                fid,
                100,
                StorageUnitType::UnitType2025,
                false,
                FarcasterNetwork::Devnet,
            ),
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
            let result = self
                .first_live_node()
                .add_message(
                    MempoolMessage::OnchainEvent(event),
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
                    .live_nodes()
                    .all(|node| node.fid_registered(fid).is_some())
                {
                    return self.first_live_node().fid_registered(fid);
                }

                None
            },
            tokio::time::Duration::from_secs(15),
            tokio::time::Duration::from_millis(100),
        )
        .await
    }

    pub async fn register_and_wait_for_fid(&mut self, fid: u64) -> Option<proto::OnChainEvent> {
        self.register_fid(fid).await;
        self.wait_for_fid(fid).await
    }

    /// Submits a fresh cast to validator 0 (or the first live one) over gRPC.
    /// Unlike the old in-process pre-seed loop, this exercises the
    /// `MempoolMessage::UserMessage` gossip path: only one node receives the
    /// message directly; the rest must learn about it via libp2p mempool gossip.
    pub async fn send_cast(&mut self, fid: u64, text: &str) -> proto::Message {
        let (signer, _) = self.test_fids.get(&fid).unwrap();
        let message = messages_factory::casts::create_cast_add(fid, text, None, Some(&signer));

        let result = self
            .first_live_node()
            .submit_message_via_grpc(message.clone())
            .await;
        assert!(
            result.is_ok(),
            "Failed to submit cast via gRPC (fid: {}, text: {}): {:?}",
            fid,
            text,
            result.err()
        );

        message
    }

    pub async fn wait_for_cast(&self, fid: u64, hash: Vec<u8>) -> Option<proto::Message> {
        wait_for(
            || {
                if self
                    .live_nodes()
                    .all(|node| node.cast_added(fid, hash.clone()).is_some())
                {
                    return self.first_live_node().cast_added(fid, hash.clone());
                }

                None
            },
            tokio::time::Duration::from_secs(15),
            tokio::time::Duration::from_millis(100),
        )
        .await
    }

    /// gRPC-based variant of `wait_for_cast`: polls every live validator's gRPC
    /// `GetCast` endpoint until they all return the message. Verifies the gRPC
    /// read path in addition to consensus propagation.
    pub async fn wait_for_cast_via_grpc(&self, fid: u64, hash: Vec<u8>) -> Option<proto::Message> {
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(15);
        loop {
            let mut all_ok = true;
            let mut last_message: Option<proto::Message> = None;
            for node in self.live_nodes() {
                match node.get_cast_via_grpc(fid, hash.clone()).await {
                    Ok(msg) => {
                        last_message = Some(msg);
                    }
                    Err(_) => {
                        all_ok = false;
                        break;
                    }
                }
            }
            if all_ok && last_message.is_some() {
                return last_message;
            }
            if tokio::time::Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }

    pub async fn wait_for_next_block_event(&self) -> Option<()> {
        let target_seqnum = self.max_block_event_seqnum() + 1;
        wait_for(
            || {
                for node in self.live_nodes() {
                    if node.block_stores().block_event_store.max_seqnum().unwrap() != target_seqnum
                    {
                        return None;
                    }

                    for stores in node.shard_stores().values() {
                        if stores.block_event_store.max_seqnum().unwrap() != target_seqnum {
                            return None;
                        }
                    }
                }
                Some(())
            },
            tokio::time::Duration::from_secs(15),
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
        self.wait_for_block_with_timeout(height, tokio::time::Duration::from_secs(15))
            .await
    }

    /// Like `wait_for_block`, but with a configurable budget — recovery tests
    /// that re-sync a stopped validator may need longer than the default 15s.
    pub async fn wait_for_block_with_timeout(
        &self,
        height: usize,
        timeout: tokio::time::Duration,
    ) -> Option<()> {
        wait_for(
            || {
                self.live_nodes()
                    .all(|node| node.num_blocks() >= height)
                    .then_some(())
            },
            timeout,
            tokio::time::Duration::from_millis(100),
        )
        .await
    }

    // Waits for all validator nodes to reach at least `height` blocks.
    pub async fn wait_for_shard_chunk(&self, shard_id: u32, height: usize) -> Option<()> {
        wait_for(
            || {
                self.live_nodes()
                    .all(|node| node.num_shard_chunks(shard_id) >= height)
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

    pub async fn wait_for_next_block_on_all_shards(&self) {
        let next_block_height = self.max_block_height() + 1;
        self.wait_for_block(next_block_height).await.unwrap();

        for shard_id in 1..self.num_shards + 1 {
            let next_shard_height = self.max_shard_height(shard_id) + 1;
            self.wait_for_shard_chunk(shard_id, next_shard_height)
                .await
                .unwrap();
        }
    }

    // Waits for all read nodes to reach at least `height` for the specified shard.
    pub async fn read_wait_for_shard_chunk(&self, shard_id: u32, height: usize) -> Option<()> {
        wait_for(
            || {
                self.read_nodes
                    .iter()
                    .all(|node| node.num_shard_chunks(shard_id) >= height)
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
                let all_have_proof = self.live_nodes().all(|node| {
                    if let Some(proof) = node.get_username_proof(fname.clone()) {
                        proof.fid == fid
                    } else {
                        false
                    }
                });

                if all_have_proof {
                    return self.first_live_node().get_username_proof(fname.clone());
                }

                None
            },
            tokio::time::Duration::from_secs(15),
            tokio::time::Duration::from_millis(100),
        )
        .await
    }
}

fn on_all_nodes<F>(network: &TestNetwork, f: F)
where
    F: Fn(&dyn Node, bool, usize) -> (),
{
    for (i, slot) in network.nodes.iter().enumerate() {
        if let Some(node) = slot.as_ref() {
            f(node, false, i);
        }
    }

    for (i, node) in network.read_nodes.iter().enumerate() {
        f(node, true, i);
    }
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

/// Determinism check: every committed block (and shard chunk) must be byte-identical
/// across all live validators. Catches HashMap-iteration order leaks, time-dependent
/// fields, and other sources of nondeterminism that would otherwise show up only as
/// state-hash mismatches in production.
fn assert_blocks_match_across_validators(network: &TestNetwork) {
    let live: Vec<&NodeForTest> = network.live_nodes().collect();
    if live.len() < 2 {
        return;
    }

    let common_block_height = live
        .iter()
        .map(|n| n.num_blocks())
        .min()
        .expect("at least one live node");

    for height in 1..=common_block_height as u64 {
        let mut reference: Option<(usize, Vec<u8>)> = None;
        for (i, node) in live.iter().enumerate() {
            let block = node
                .block_stores()
                .block_store
                .get_block_by_height(height)
                .unwrap_or_else(|e| {
                    panic!("validator {i} block_store error at height {height}: {e:?}")
                })
                .unwrap_or_else(|| panic!("validator {i} missing block at height {height}"));
            match &reference {
                None => reference = Some((i, block.hash.clone())),
                Some((ref_idx, ref_hash)) => assert_eq!(
                    &block.hash,
                    ref_hash,
                    "block hash mismatch at height {height}: validator {ref_idx} = {}, validator {i} = {}",
                    hex::encode(ref_hash),
                    hex::encode(&block.hash)
                ),
            }
        }
    }

    for shard_id in 1..=network.num_shards {
        let common_shard_height = live
            .iter()
            .map(|n| n.num_shard_chunks(shard_id))
            .min()
            .expect("at least one live node");

        for height in 1..=common_shard_height as u64 {
            let mut reference: Option<(usize, Vec<u8>)> = None;
            for (i, node) in live.iter().enumerate() {
                let chunk = node
                    .shard_stores()
                    .get(&shard_id)
                    .unwrap()
                    .shard_store
                    .get_chunk_by_height(height)
                    .unwrap_or_else(|e| {
                        panic!(
                            "validator {i} shard {shard_id} store error at height {height}: {e:?}"
                        )
                    })
                    .unwrap_or_else(|| {
                        panic!("validator {i} missing shard {shard_id} chunk at height {height}")
                    });
                match &reference {
                    None => reference = Some((i, chunk.hash.clone())),
                    Some((ref_idx, ref_hash)) => assert_eq!(
                        &chunk.hash,
                        ref_hash,
                        "shard {shard_id} chunk hash mismatch at height {height}: validator {ref_idx} = {}, validator {i} = {}",
                        hex::encode(ref_hash),
                        hex::encode(&chunk.hash)
                    ),
                }
            }
        }
    }
}

#[tokio::test]
#[serial]
async fn test_basic_consensus() {
    // Useful for debugging
    // let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    // let _ = tracing_subscriber::fmt()
    //     .with_env_filter(env_filter)
    //     .try_init();

    let num_shards = 2;
    let mut network = TestNetwork::create(3, num_shards).await;
    network.start_validators().await;

    network.register_and_wait_for_fid(1000).await.unwrap();
    let cast = network
        .send_and_wait_for_cast(1000, "Hello, world")
        .await
        .unwrap();

    // Wait for nodes to reach the next block height
    network.wait_for_next_block_on_all_shards().await;

    assert_network_has_messages(&network, 1);
    // Assert that all nodes have the cast message
    assert_network_has_cast(&network, 1000, cast.hash.clone());
    assert_blocks_match_across_validators(&network);
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

    network.register_and_wait_for_fid(1000).await.unwrap();
    let cast = network
        .send_and_wait_for_cast(1000, "Hello, world")
        .await
        .unwrap();

    network.wait_for_next_block_on_all_shards().await;

    assert_network_has_messages(&network, 1);

    // Add the node to the network and start producing blocks again.
    network.start_validator_node(3).await;

    // Wait for all nodes to reach the same block height
    let target_height = network.max_block_height();
    network.wait_for_block(target_height).await.unwrap();

    for shard_id in 1..num_shards + 1 {
        let target_height = network.max_shard_height(shard_id);
        network
            .wait_for_shard_chunk(shard_id, target_height)
            .await
            .unwrap();
    }

    assert_network_has_messages(&network, 1);
    // Assert that all nodes have the cast message
    assert_network_has_cast(&network, 1000, cast.hash.clone());
    assert_blocks_match_across_validators(&network);
}

#[tokio::test]
#[serial]
async fn test_read_node() {
    let num_shards = 2;
    let mut network = TestNetwork::create(3, num_shards).await;
    network.start_validators().await;

    network.register_and_wait_for_fid(1000).await.unwrap();
    let cast = network
        .send_and_wait_for_cast(1000, "Hello, world")
        .await
        .unwrap();

    // Wait for the next block to arrive on all nodes
    network.wait_for_next_block_on_all_shards().await;

    network.start_read_node().await;
    network.start_read_node().await;

    let target_height = network.max_block_height();
    assert!(
        network.read_wait_for_block(target_height).await.is_some(),
        "Read nodes did not reach the target block height"
    );

    for shard_id in 1..num_shards + 1 {
        let target_height = network.max_shard_height(shard_id);
        network
            .read_wait_for_shard_chunk(shard_id, target_height)
            .await
            .unwrap();
    }

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

    network.register_and_wait_for_fid(first_fid).await.unwrap();
    network.register_and_wait_for_fid(second_fid).await.unwrap();

    let router = ShardRouter {};
    // Ensure that the two fids are routed to different shards
    assert_ne!(
        router.route_fid(first_fid, num_shards),
        router.route_fid(second_fid, num_shards)
    );

    let fname = "erica";

    let transfer1 = proto::FnameTransfer {
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
    };

    let transfer2 = proto::FnameTransfer {
        id: 829595,
        from_fid: second_fid,
        proof: Some(proto::UserNameProof {
            timestamp: 1741384227,
            name: fname.as_bytes().to_vec(),
            fid: first_fid,
            owner: hex::decode("92ce59c18a97646e9a7e011653d8417d3a08bb2b").unwrap(),
            signature: hex::decode("00c3601c515edffe208e7128f47f89c2fb7b8e0beaaf615158305ddf02818a71679a8e7062503be59a19d241bd0b47396a3c294cfafd0d5478db1ae8249463bd1c").unwrap(),
            r#type: proto::UserNameType::UsernameTypeFname as i32,
        }),
    };

    let node = network.nodes[0].as_ref().unwrap();

    node.add_message(
        MempoolMessage::FnameTransfer(transfer1),
        MempoolSource::Local,
        None,
    )
    .await
    .unwrap();

    node.add_message(
        MempoolMessage::FnameTransfer(transfer2),
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

#[tokio::test]
#[serial]
async fn test_decoupling_shard_0_from_other_shards() {
    let num_shards = 2;
    let mut network = TestNetwork::create(3, num_shards).await;
    network.start_validators().await;

    // Register some data to both shards to make sure the setup is working
    let first_fid = 20270;
    let second_fid = 211428;

    network.register_and_wait_for_fid(first_fid).await.unwrap();
    network.register_and_wait_for_fid(second_fid).await.unwrap();

    // Corrupt the trie for both shards on one of the nodes so consensus halts on shards 1 and 2.
    for stores in network.nodes[0]
        .as_mut()
        .unwrap()
        .node
        .shard_stores
        .values_mut()
    {
        let mut txn = RocksDbTransactionBatch::new();
        let message = messages_factory::casts::create_cast_add(123455, "hi", None, None);
        stores
            .trie
            .insert(
                &merkle_trie::Context::new(),
                &stores.db,
                &mut txn,
                vec![&TrieKey::for_message(&message)[0]],
            )
            .unwrap();
        stores.db.commit(txn).unwrap();
    }

    let mut max_shard_height = 0;
    for stores in network.nodes[0].as_ref().unwrap().shard_stores().values() {
        max_shard_height = max_shard_height.max(stores.shard_store.max_block_number().unwrap());
    }

    // Make sure that shard 0 proceeds to a height beyond the last shard chunk successfully produced for the purpose of testing that shard 0 works even if other shards are down
    network
        .wait_for_block(max_shard_height as usize + 4)
        .await
        .unwrap();

    let last_block = network.nodes[0]
        .as_ref()
        .unwrap()
        .block_stores()
        .block_store
        .get_last_block()
        .unwrap()
        .unwrap();
    let last_shard_witnesses = last_block.shard_witness.unwrap().shard_chunk_witnesses;
    assert_eq!(last_shard_witnesses.len() as u32, num_shards);

    let previous_block = network.nodes[0]
        .as_ref()
        .unwrap()
        .block_stores()
        .block_store
        .get_block_by_height(last_block.header.unwrap().height.unwrap().block_number - 1)
        .unwrap()
        .unwrap();
    let prev_shard_witnesses = previous_block.shard_witness.unwrap().shard_chunk_witnesses;
    assert_eq!(last_shard_witnesses, prev_shard_witnesses);
}

#[tokio::test]
#[serial]
async fn test_cross_shard_communication() {
    let num_shards = 2;
    let mut network = TestNetwork::create(3, num_shards).await;
    network.start_validators().await;

    network.wait_for_next_block_event().await.unwrap();
    network.wait_for_next_block_event().await.unwrap();

    assert!(network.max_block_event_seqnum() >= network.max_block_height() as u64 / 5);
}

/// Crash a single validator after the network has been producing blocks, verify
/// the surviving 3-of-4 supermajority continues consensus, then bring the
/// validator back from a fresh DB and assert it catches up via consensus sync.
#[tokio::test]
#[serial]
async fn test_validator_crash_and_recovery() {
    let num_shards = 1;
    let mut network = TestNetwork::create(4, num_shards).await;
    network.start_validators().await;

    network.register_and_wait_for_fid(1000).await.unwrap();
    let cast1 = network
        .send_and_wait_for_cast(1000, "before crash")
        .await
        .unwrap();

    network.wait_for_next_block_on_all_shards().await;
    let height_at_crash = network.max_block_height();

    // Stop validator 0. Surviving voting power is 3/4, comfortably above the
    // 2/3 BFT threshold, so consensus must keep advancing.
    network.stop_validator_node(0).await;

    let cast2 = network.send_cast(1000, "during outage").await;
    // wait_for_cast iterates only live nodes (slot 0 is None), so this asserts
    // the 3 survivors merged the cast.
    network
        .wait_for_cast(1000, cast2.hash.clone())
        .await
        .expect("survivors did not merge cast2");

    // Survivors must continue producing blocks past the height we observed
    // before the crash. Bumped timeout — sync over fresh peers may need it.
    network
        .wait_for_block_on_subset(
            &[1, 2, 3],
            height_at_crash + 2,
            tokio::time::Duration::from_secs(30),
        )
        .await
        .expect("survivors did not advance after crash");

    // Bring the crashed validator back up against a fresh tempdir DB. It rejoins
    // with the same keypair (and therefore validator-set identity) and must
    // catch up via consensus sync — this exercises the warm-discovery + sync
    // path that production hits on every replacement deploy.
    network.restart_validator_node(0).await;

    let target = network.max_block_height();
    network
        .wait_for_block_with_timeout(target, tokio::time::Duration::from_secs(60))
        .await
        .expect("restarted validator did not catch up");

    // Both casts should now be visible everywhere (the second one was missed
    // during downtime and is recovered through sync).
    assert_network_has_cast(&network, 1000, cast1.hash.clone());
    assert_network_has_cast(&network, 1000, cast2.hash.clone());
    assert_blocks_match_across_validators(&network);
}

/// Pause a validator while preserving its on-disk state, advance the network,
/// then resume the same validator against the original RocksDBs. Verifies WAL
/// replay + state recovery — the path production hits on every restart-in-place.
#[tokio::test]
#[serial]
async fn test_warm_restart() {
    let num_shards = 1;
    let mut network = TestNetwork::create(3, num_shards).await;
    network.start_validators().await;

    network.register_and_wait_for_fid(1000).await.unwrap();
    let cast1 = network
        .send_and_wait_for_cast(1000, "before pause")
        .await
        .unwrap();
    network.wait_for_next_block_on_all_shards().await;
    let messages_before = network.first_live_node().total_messages();

    // Pause validator 1 with `keep_db_on_drop: true` so RocksDB survives the
    // drop. The remaining two validators (2/3 supermajority) keep producing.
    let (global_dir, shards_dir) = network.pause_validator_node(1).await;

    let cast2 = network
        .send_and_wait_for_cast(1000, "during pause")
        .await
        .unwrap();
    network.wait_for_next_block_on_all_shards().await;

    // Re-create validator 1 against the preserved data dirs. After resume, it
    // must replay its WAL, sync the missed window, and converge on the same
    // total_messages count as the never-paused validators.
    network
        .resume_validator_node(1, global_dir, shards_dir)
        .await;

    let target = network.max_block_height();
    network
        .wait_for_block_with_timeout(target, tokio::time::Duration::from_secs(60))
        .await
        .expect("resumed validator did not catch up");

    assert_network_has_cast(&network, 1000, cast1.hash.clone());
    assert_network_has_cast(&network, 1000, cast2.hash.clone());

    let resumed = network.nodes[1].as_ref().unwrap();
    assert!(
        resumed.total_messages() >= messages_before,
        "resumed validator should retain at least the pre-pause message count"
    );
    assert_blocks_match_across_validators(&network);
}

/// Subscribe to the gRPC HubEvent stream on validator 1, submit a cast on
/// validator 0, and assert the corresponding `MERGE_MESSAGE` event is delivered
/// over the wire within budget.
#[tokio::test]
#[serial]
async fn test_subscribe_stream() {
    use futures::StreamExt;
    let num_shards = 1;
    let mut network = TestNetwork::create(2, num_shards).await;
    network.start_validators().await;

    network.register_and_wait_for_fid(1001).await.unwrap();

    // Open the subscribe stream on validator 1 — explicitly NOT the validator
    // we'll submit to, so the test exercises gossip + per-shard hub-event fanout.
    let mut subscriber = network.nodes[1].as_ref().unwrap().client().await;
    let stream = subscriber
        .subscribe(Request::new(SubscribeRequest {
            event_types: vec![HubEventType::MergeMessage as i32],
            from_id: None,
            shard_index: None,
        }))
        .await
        .expect("subscribe RPC failed")
        .into_inner();

    // Submit a cast on validator 0.
    let cast = network.send_cast(1001, "subscribe-me").await;

    // Walk the stream until we either see the matching merge event or the
    // budget expires. We tolerate other event types arriving first.
    let target_hash = cast.hash.clone();
    let mut stream = stream;
    let found = tokio::time::timeout(tokio::time::Duration::from_secs(20), async {
        while let Some(event) = stream.next().await {
            let event = event.expect("stream returned an error");
            if event.r#type == HubEventType::MergeMessage as i32 {
                if let Some(proto::hub_event::Body::MergeMessageBody(body)) = event.body {
                    if let Some(message) = body.message {
                        if message.hash == target_hash {
                            return true;
                        }
                    }
                }
            }
        }
        false
    })
    .await
    .expect("did not receive merge_message event before timeout");
    assert!(
        found,
        "subscribe stream ended before delivering the cast event"
    );
}

/// Submit a mix of valid and signature-corrupted casts via gRPC. The invalid
/// ones must be rejected at the boundary; the valid ones must still land in
/// blocks; consensus must keep advancing through the spam.
#[tokio::test]
#[serial]
async fn test_mempool_invalid_message_isolation() {
    let num_shards = 1;
    let mut network = TestNetwork::create(3, num_shards).await;
    network.start_validators().await;

    network.register_and_wait_for_fid(1002).await.unwrap();
    let (signer, _) = network.test_fids.get(&1002).unwrap().clone();

    let num_invalid = 200;
    let num_valid = 10;

    // Build valid casts first (each gets a unique text so hashes differ).
    let valid_casts: Vec<proto::Message> = (0..num_valid)
        .map(|i| {
            messages_factory::casts::create_cast_add(
                1002,
                &format!("valid-{i}"),
                None,
                Some(&signer),
            )
        })
        .collect();

    // Build invalid casts by flipping one signature byte. Each is a separate
    // valid-looking message that fails signature verification at the boundary.
    let invalid_casts: Vec<proto::Message> = (0..num_invalid)
        .map(|i| {
            let mut msg = messages_factory::casts::create_cast_add(
                1002,
                &format!("invalid-{i}"),
                None,
                Some(&signer),
            );
            if !msg.signature.is_empty() {
                msg.signature[0] ^= 0x01;
            }
            msg
        })
        .collect();

    let target = network.first_live_node();

    // Submit invalid first to make sure the spam doesn't starve valid traffic.
    for msg in &invalid_casts {
        let result = target.submit_message_via_grpc(msg.clone()).await;
        assert!(
            result.is_err(),
            "invalid cast was accepted by submit_message: hash={}",
            hex::encode(&msg.hash)
        );
    }

    for msg in &valid_casts {
        let result = target.submit_message_via_grpc(msg.clone()).await;
        assert!(
            result.is_ok(),
            "valid cast was rejected by submit_message: {:?}",
            result.err()
        );
    }

    // All valid casts must land on every node within the standard budget.
    for msg in &valid_casts {
        network
            .wait_for_cast(1002, msg.hash.clone())
            .await
            .unwrap_or_else(|| {
                panic!(
                    "valid cast did not propagate to all validators: hash={}",
                    hex::encode(&msg.hash)
                )
            });
    }

    // Consensus must still be advancing — i.e. the spam didn't stall the
    // proposers. wait_for_next_block_on_all_shards completes only if every
    // live validator advances to a new height + new chunk per shard.
    network.wait_for_next_block_on_all_shards().await;
}

/// Configure two validator sets — set0 effective at genesis with validators
/// [0,1,2], set1 effective at height H with validators [0,1,3]. After enough
/// blocks past H, validator 2's commit signatures stop appearing and validator
/// 3's begin. Verifies the rotation is honored end-to-end.
#[tokio::test]
#[serial]
async fn test_validator_set_rotation() {
    let num_shards = 1;
    let rotation_height: u64 = 5;

    let validator_sets = vec![
        ValidatorSetConfig {
            effective_at: 0,
            validator_public_keys: vec!["idx:0".into(), "idx:1".into(), "idx:2".into()],
            shard_ids: vec![],
        },
        ValidatorSetConfig {
            effective_at: rotation_height,
            validator_public_keys: vec!["idx:0".into(), "idx:1".into(), "idx:3".into()],
            shard_ids: vec![],
        },
    ];

    let mut network = TestNetwork::create_with_validator_sets(4, num_shards, validator_sets).await;
    network.start_validators().await;

    // Wait for blocks past the rotation point with enough margin to observe
    // post-rotation commits.
    network
        .wait_for_block_with_timeout(
            (rotation_height + 5) as usize,
            tokio::time::Duration::from_secs(45),
        )
        .await
        .expect("network did not advance past rotation height");

    let v2_pub = network.keypairs[2].public().to_bytes().to_vec();
    let v3_pub = network.keypairs[3].public().to_bytes().to_vec();

    // Inspect each validator's view of the chain. We use validator 0 as the
    // canonical reader; determinism is checked separately.
    let reader = network.first_live_node();
    let mut v2_signed_after_rotation = 0;
    let mut v3_signed_before_rotation = 0;
    let mut v3_signed_after_rotation = 0;

    let max_height = reader.num_blocks() as u64;
    for height in 1..=max_height {
        let block = reader
            .block_stores()
            .block_store
            .get_block_by_height(height)
            .unwrap()
            .unwrap();
        let commits = match block.commits.as_ref() {
            Some(c) => c,
            None => continue,
        };
        let signed_by_v2 = commits.signatures.iter().any(|s| s.signer == v2_pub);
        let signed_by_v3 = commits.signatures.iter().any(|s| s.signer == v3_pub);
        if height >= rotation_height && signed_by_v2 {
            v2_signed_after_rotation += 1;
        }
        if height < rotation_height && signed_by_v3 {
            v3_signed_before_rotation += 1;
        }
        if height >= rotation_height && signed_by_v3 {
            v3_signed_after_rotation += 1;
        }
    }

    assert_eq!(
        v2_signed_after_rotation, 0,
        "validator 2 should not sign blocks at or after rotation height {rotation_height}"
    );
    assert_eq!(
        v3_signed_before_rotation, 0,
        "validator 3 should not sign blocks before rotation height {rotation_height}"
    );
    assert!(
        v3_signed_after_rotation > 0,
        "validator 3 should sign at least one block after rotation height {rotation_height}"
    );
}

/// HTTP server end-to-end: stand up the production HTTP layer (axum/hyper +
/// `start_http_server` helper) on top of a 1-validator network, then drive it
/// with `reqwest`. Exercises the Tokio `TcpListener` path that the in-process
/// `http_server_test.rs` unit tests skip.
#[tokio::test]
#[serial]
async fn test_http_server_smoke() {
    use snapchain::network::http_server::{
        spawn_http_server, Config as HttpConfig, HubHttpServiceImpl,
    };

    let num_shards = 1;
    let mut network = TestNetwork::create(1, num_shards).await;
    network.start_validators().await;

    network.register_and_wait_for_fid(2000).await.unwrap();
    let cast = network
        .send_and_wait_for_cast(2000, "http-test")
        .await
        .unwrap();

    let http_port = get_available_port();
    let listener = tokio::net::TcpListener::bind(format!("{HOST_FOR_TEST}:{http_port}"))
        .await
        .expect("bind http listener");
    let http_service = HubHttpServiceImpl {
        service: network.first_live_node().hub_service(),
    };
    let _http_handle = spawn_http_server(listener, http_service, HttpConfig::default());

    // Tiny settle delay so the spawned accept loop is ready before the first request.
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let base = format!("http://{HOST_FOR_TEST}:{http_port}");
    let client = reqwest::Client::new();

    // /v1/info — sanity-check that we get a 200 with the expected shard count.
    let info = client
        .get(format!("{base}/v1/info"))
        .send()
        .await
        .expect("GET /v1/info failed");
    assert_eq!(info.status(), reqwest::StatusCode::OK, "/v1/info status");
    let info_json: serde_json::Value = info.json().await.expect("/v1/info json");
    assert_eq!(
        info_json["num_shards"].as_u64(),
        Some(num_shards as u64),
        "/v1/info num_shards mismatch: {info_json}"
    );

    // /v1/castById — pull back the cast we previously submitted via gRPC. Hash
    // is sent hex-encoded with the 0x prefix per the JSON encoding the HTTP
    // layer expects.
    let hash_hex = format!("0x{}", hex::encode(&cast.hash));
    let by_id = client
        .get(format!("{base}/v1/castById"))
        .query(&[("fid", "2000"), ("hash", hash_hex.as_str())])
        .send()
        .await
        .expect("GET /v1/castById failed");
    assert_eq!(
        by_id.status(),
        reqwest::StatusCode::OK,
        "/v1/castById status"
    );
    let cast_json: serde_json::Value = by_id.json().await.expect("/v1/castById json");
    assert_eq!(
        cast_json["hash"].as_str(),
        Some(hash_hex.as_str()),
        "/v1/castById returned a different hash: {cast_json}"
    );
}

/// Partition a 5-validator network into A=[0,1,2] (3/5 voting power = supermajority)
/// and B=[3,4] (2/5, below 2/3). A side must keep advancing and accept new
/// casts; B side must NOT make progress on its own. Healing the partition
/// restores convergence across all 5 validators.
#[tokio::test]
#[serial]
async fn test_network_partition() {
    let num_shards = 1;
    let mut network = TestNetwork::create(5, num_shards).await;
    network.start_validators().await;

    // Let the network warm up so we have a baseline height shared by everyone.
    network.register_and_wait_for_fid(1003).await.unwrap();
    network
        .send_and_wait_for_cast(1003, "before partition")
        .await
        .unwrap();
    network.wait_for_next_block_on_all_shards().await;
    let baseline_height = network.max_block_height();

    // Drop gossip across the partition boundary on both sides — symmetric block.
    network.partition_validators(&[0, 1, 2], &[3, 4]);

    // Side A (3 validators, 60% voting power, ≥ 2/3) must keep producing blocks
    // and accept submissions. We submit on node 0 (an A-side member) and
    // verify the A nodes each see the cast.
    let signer = network.test_fids.get(&1003).unwrap().0.clone();
    let cast_a = messages_factory::casts::create_cast_add(1003, "a-side", None, Some(&signer));
    network.nodes[0]
        .as_ref()
        .unwrap()
        .submit_message_via_grpc(cast_a.clone())
        .await
        .expect("A-side submit failed");

    network
        .wait_for_cast_on_subset(
            &[0, 1, 2],
            1003,
            cast_a.hash.clone(),
            tokio::time::Duration::from_secs(30),
        )
        .await
        .expect("A-side did not converge on its own cast");

    // Snapshot B's height — it must remain stuck (only 2/5 voting power).
    let b_height_before = network.block_height_at(3).max(network.block_height_at(4));
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    let b_height_after = network.block_height_at(3).max(network.block_height_at(4));
    assert!(
        b_height_after <= b_height_before + 1,
        "B side should not advance more than the in-flight commit (before={b_height_before}, after={b_height_after})"
    );

    // Heal the partition. Both casts (the pre-partition one and the A-side one)
    // must converge on every validator within the recovery budget.
    network.heal_partition();

    let target = network.max_block_height().max(baseline_height + 3);
    network
        .wait_for_block_with_timeout(target, tokio::time::Duration::from_secs(60))
        .await
        .expect("network did not converge after healing partition");

    assert_network_has_cast(&network, 1003, cast_a.hash.clone());
    assert_blocks_match_across_validators(&network);
}
