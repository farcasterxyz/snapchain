use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use informalsystems_malachitebft_metrics::{Metrics, SharedRegistry};
use snapchain::connectors::fname::FnameRequest;
use snapchain::connectors::onchain_events::{ChainClients, OnchainEventsRequest};
use snapchain::consensus::consensus::SystemMessage;
use snapchain::hyper as snapchain_hyper;
use snapchain::mempool::block_receiver::BlockReceiver;
use snapchain::mempool::mempool::{Mempool, MempoolRequest, ReadNodeMempool};
use snapchain::mempool::routing;
use snapchain::network::admin_server::MyAdminService;
use snapchain::network::gossip::{GossipEvent, SnapchainGossip};
use snapchain::network::http_server::HubHttpServiceImpl;
use snapchain::network::replication::{self, ReplicationServer, Replicator};
use snapchain::network::server::MyHubService;
use snapchain::node::snapchain_node::SnapchainNode;
use snapchain::node::snapchain_read_node::SnapchainReadNode;
use snapchain::proto::admin_service_server::AdminServiceServer;
use snapchain::proto::hub_service_server::HubServiceServer;
use snapchain::proto::replication_service_server::ReplicationServiceServer;
use snapchain::storage::db::snapshot::{download_snapshots, BootstrapMethod};
use snapchain::storage::db::RocksDB;
use snapchain::storage::store::block_engine::BlockStores;
use snapchain::storage::store::engine::{PostCommitMessage, Senders};
use snapchain::storage::store::node_local_state::{self, LocalStateStore};
use snapchain::storage::store::stores::Stores;
use snapchain::utils::statsd_wrapper::StatsdClientWrapper;
use snapchain_hyper::CAPABILITY_HYPER;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::net::SocketAddr;
use std::process;
use std::sync::Arc;
use std::{fs, net};
use tokio::net::TcpListener;
use tokio::select;
use tokio::signal::ctrl_c;
use tokio::sync::{broadcast, mpsc, watch};
use tokio_cron_scheduler::JobScheduler;
use tonic::transport::Server;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

const VERSION: Option<&str> = option_env!("CARGO_PKG_VERSION");

async fn start_servers(
    app_config: &snapchain::cfg::Config,
    mut gossip: SnapchainGossip,
    mempool_tx: mpsc::Sender<MempoolRequest>,
    shutdown_tx: mpsc::Sender<()>,
    onchain_events_request_tx: broadcast::Sender<OnchainEventsRequest>,
    fname_request_tx: broadcast::Sender<FnameRequest>,
    statsd_client: StatsdClientWrapper,
    shard_stores: HashMap<u32, Stores>,
    shard_senders: HashMap<u32, Senders>,
    block_stores: BlockStores,
    chain_clients: ChainClients,
    replicator: Option<Arc<replication::replicator::Replicator>>,
    local_state_store: LocalStateStore,
    api_handler: Option<snapchain::api::ApiHttpHandler>,
) {
    let grpc_addr = app_config.rpc_address.clone();
    let grpc_socket_addr: SocketAddr = grpc_addr.parse().unwrap();

    let admin_service = MyAdminService::new(
        app_config.admin_rpc_auth.clone(),
        mempool_tx.clone(),
        onchain_events_request_tx,
        fname_request_tx,
        shard_stores.clone(),
        block_stores.clone(),
        app_config.snapshot.clone(),
        app_config.fc_network,
        statsd_client.clone(),
        local_state_store,
    );

    let service = Arc::new(MyHubService::new(
        app_config.rpc_auth.clone(),
        block_stores.clone(),
        shard_stores.clone(),
        shard_senders,
        statsd_client.clone(),
        app_config.consensus.num_shards,
        app_config.fc_network,
        Box::new(routing::ShardRouter {}),
        mempool_tx.clone(),
        gossip.tx.clone(),
        chain_clients,
        VERSION.unwrap_or("unknown").to_string(),
        gossip.swarm.local_peer_id().to_string(),
    ));

    let replication_service = if let Some(replicator) = replicator {
        let service = ReplicationServiceServer::new(ReplicationServer::new(
            replicator,
            block_stores.clone(),
            statsd_client.clone(),
        ));
        Some(service)
    } else {
        None
    };

    let grpc_service = service.clone();
    let grpc_shutdown_tx = shutdown_tx.clone();

    tokio::spawn(async move {
        info!(grpc_addr = grpc_addr, "GrpcService listening",);
        let mut server = Server::builder().add_service(HubServiceServer::from_arc(grpc_service));

        if admin_service.enabled() {
            let admin_service = AdminServiceServer::new(admin_service);
            server = server.add_service(admin_service);
        }

        if let Some(replication_service) = replication_service {
            server = server.add_service(replication_service);
        }

        let resp = server.serve(grpc_socket_addr).await;

        let msg = "grpc server stopped";
        match resp {
            Ok(()) => error!(msg),
            Err(e) => error!(error = ?e, "{}", msg),
        }

        grpc_shutdown_tx.send(()).await.ok();
    });

    let http_addr = app_config.http_address.clone();
    let http_socket_addr: SocketAddr = http_addr.parse().unwrap();

    let http_shutdown_tx = shutdown_tx.clone();
    let http_server_config = app_config.http_server.clone();
    tokio::spawn(async move {
        let listener = TcpListener::bind(http_socket_addr).await.unwrap();
        info!(http_addr = http_addr, "HttpService listening",);

        let http_service = HubHttpServiceImpl {
            service: service.clone(),
        };
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let io = TokioIo::new(stream);
                    let http_server_config = http_server_config.clone();
                    let service_clone = http_service.clone();
                    let api = api_handler.clone();
                    tokio::spawn(async move {
                        let mut router =
                            snapchain::network::http_server::Router::new(service_clone);
                        if let Some(handler) = api {
                            router = router.with_api_handler(handler);
                        }
                        if let Err(err) = http1::Builder::new()
                            .serve_connection(
                                io,
                                service_fn(|r| router.handle(r, &http_server_config)),
                            )
                            .await
                        {
                            error!("Error serving connection: {}", err);
                        }
                    });
                }
                Err(e) => {
                    error!("Error accepting connection: {}", e);
                    break;
                }
            }
        }

        http_shutdown_tx.send(()).await.ok();
    });

    // Start gossip last
    tokio::spawn(async move {
        info!("Starting gossip");
        gossip.start().await;
        info!("Gossip Stopped");
    });
}

async fn schedule_background_jobs(
    app_config: &snapchain::cfg::Config,
    shard_stores: HashMap<u32, Stores>,
    block_stores: BlockStores,
    sync_complete_rx: watch::Receiver<bool>,
    statsd_client: StatsdClientWrapper,
) {
    let sched = JobScheduler::new().await.unwrap();
    let mut jobs = vec![];
    if app_config.read_node {
        if let Some(block_retention) = app_config.pruning.block_retention {
            let schedule = "0 0 10 * * *"; // 10am UTC every day
            let job = snapchain::jobs::block_pruning::block_pruning_job(
                schedule,
                block_retention,
                block_stores.clone(),
                shard_stores.clone(),
                sync_complete_rx,
            )
            .unwrap();
            jobs.push(job);
        }
    }

    let event_pruning_job = snapchain::jobs::event_pruning::event_pruning_job(
        "0 0 0 * * *", // midnight UTC every day
        app_config.pruning.event_retention,
        shard_stores.clone(),
    )
    .unwrap();
    jobs.push(event_pruning_job);

    if app_config.snapshot.snapshot_upload_enabled() {
        let snapshot_upload_job = snapchain::jobs::snapshot_upload::snapshot_upload_job(
            "0 0 5 * * *", // 5 AM UTC every day
            app_config.snapshot.clone(),
            app_config.fc_network,
            block_stores.clone(),
            shard_stores.clone(),
            statsd_client,
        )
        .unwrap();
        jobs.push(snapshot_upload_job);
    }

    for job in jobs {
        sched.add(job).await.unwrap();
    }

    sched.start().await.unwrap();
}

fn is_dir_empty(path: &str) -> std::io::Result<bool> {
    let mut entries = fs::read_dir(path)?;
    Ok(entries.next().is_none())
}

fn create_replicator(
    app_config: &snapchain::cfg::Config,
    shard_stores: HashMap<u32, Stores>,
    statsd_client: StatsdClientWrapper,
) -> Result<Arc<replication::Replicator>, Box<dyn Error>> {
    let soft_limit = Replicator::ensure_ulimit();
    if soft_limit < Replicator::ULIMIT_MIN {
        error!("The current file descriptor limit ({}) is too low to start the replicator. Replicator will be disabled", soft_limit);
        return Err(format!("File descriptor limit too low: {}", soft_limit).into());
    }

    if soft_limit < Replicator::ULIMIT_RECOMMENDED {
        warn!("The current file descriptor limit ({}) is too low. Please set it to at > {} to ensure stable operation of the replicator", soft_limit, Replicator::ULIMIT_RECOMMENDED);
    }

    info!(
        "Starting replicator with file descriptor limit: {}",
        soft_limit
    );

    let replication_stores = Arc::new(replication::ReplicationStores::new(
        shard_stores,
        statsd_client.clone(),
        app_config.fc_network.clone(),
    ));
    let replicator = replication::Replicator::new_with_options(
        replication_stores,
        statsd_client,
        replication::ReplicatorSnapshotOptions {
            interval: app_config.replication.snapshot_interval,
            max_age: app_config.replication.snapshot_max_age,
        },
    );

    Ok(Arc::new(replicator))
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();

    let app_config = match snapchain::cfg::load_and_merge_config(args) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Error: {}", e);
            process::exit(1);
        }
    };

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    match app_config.log_format.as_str() {
        "text" => tracing_subscriber::fmt().with_env_filter(env_filter).init(),
        "json" => tracing_subscriber::fmt()
            .json()
            .with_env_filter(env_filter)
            .init(),
        _ => {
            return Err(format!("Invalid log format: {}", app_config.log_format).into());
        }
    }

    if app_config.clear_db {
        let db_dir = format!("{}", app_config.rocksdb_dir);
        if std::path::Path::new(&db_dir).exists() {
            let remove_result = std::fs::remove_dir_all(db_dir.clone());
            if let Err(e) = remove_result {
                error!("Failed to clear db at {:?}: {}", db_dir, e);
            }
            let create_result = std::fs::create_dir_all(db_dir.clone());
            if let Err(e) = create_result {
                error!("Failed to create db dir at {:?}: {}", db_dir, e);
            }
            warn!("Cleared db at {:?}", db_dir);
        } else {
            warn!("No db to clear at {:?}", db_dir);
        }
    }

    if app_config.statsd.prefix == "" {
        // TODO: consider removing this check
        return Err("statsd prefix must be specified in config".into());
    }

    let (statsd_host, statsd_port) = match app_config.statsd.addr.split_once(':') {
        Some((host, port)) => {
            if host.is_empty() || port.is_empty() {
                return Err("statsd address must be in the format host:port".into());
            }
            Ok((host.to_string(), port.parse::<u16>()?))
        }
        None => Err(format!(
            "invalid statsd address: {}",
            app_config.statsd.addr
        )),
    }?;

    let host = (statsd_host, statsd_port);
    let socket = net::UdpSocket::bind("0.0.0.0:0").unwrap();
    let sink = cadence::UdpMetricSink::from(host, socket)?;
    let statsd_client =
        cadence::StatsdClient::builder(app_config.statsd.prefix.as_str(), sink).build();
    let statsd_client = StatsdClientWrapper::new(statsd_client, app_config.statsd.use_tags);

    // We only use snapshots if the db directory doesn't exist or is empty.
    // If the user sets [force_load_db_from_snapshot], load the snapshot without checking directory contents.
    let db_is_empty = !fs::exists(app_config.rocksdb_dir.clone()).unwrap()
        || is_dir_empty(&app_config.rocksdb_dir).unwrap();

    if db_is_empty {
        match app_config.snapshot.bootstrap_method {
            BootstrapMethod::Replicate => {
                use rustls::crypto::{self, ring};
                use snapchain::bootstrap::replication::service::{
                    ReplicatorBootstrap, WorkUnitResponse,
                };
                use tokio::time::{sleep, Duration};

                // Initialize SSL for rustls
                crypto::CryptoProvider::install_default(ring::default_provider())
                    .expect("Failed to install rustls crypto provider");

                info!("Starting node with replication bootstrap");
                let replicator = ReplicatorBootstrap::new(statsd_client.clone(), &app_config);

                match replicator.bootstrap_using_replication().await {
                    Ok(r) => {
                        // Check for the specific success response
                        if r == WorkUnitResponse::Finished {
                            info!("Bootstrap using replication was successful. Will start snapchain now...");
                            // Sleep for 5 seconds to allow any pending logs to be flushed and the gossip to shutdown and free the port
                            sleep(Duration::from_secs(5)).await;
                        } else {
                            error!(
                                "Replication bootstrap stopped with status: {:?}. Exiting.",
                                r
                            );
                            process::exit(1);
                        }
                    }
                    Err(e) => {
                        error!("Replication bootstrap failed:\n{}\nPlease check your network connection and restart to resume.", e);
                        process::exit(1);
                    }
                }
            }
            BootstrapMethod::Snapshot => {
                if app_config.snapshot.force_load_db_from_snapshot
                    || app_config.snapshot.load_db_from_snapshot
                {
                    info!("Downloading snapshots (legacy method)");
                    let mut shard_ids = app_config.consensus.shard_ids.clone();
                    shard_ids.push(0);
                    // Raise if the download fails. If there's a persistent issue, disable snapshot download.
                    download_snapshots(
                        app_config.fc_network,
                        &app_config.snapshot,
                        app_config.rocksdb_dir.clone(),
                        shard_ids,
                    )
                    .await
                    .unwrap();
                }
            }
        }
    } else if app_config.snapshot.force_load_db_from_snapshot {
        // Force snapshot load even if DB exists
        info!("Force downloading snapshots");
        let mut shard_ids = app_config.consensus.shard_ids.clone();
        shard_ids.push(0);
        download_snapshots(
            app_config.fc_network,
            &app_config.snapshot,
            app_config.rocksdb_dir.clone(),
            shard_ids,
        )
        .await
        .unwrap();
    };

    let keypair = app_config.consensus.keypair().clone();

    let (system_tx, mut system_rx) = mpsc::channel::<SystemMessage>(1000);
    let (mempool_tx, mempool_rx) = mpsc::channel(app_config.mempool.queue_size as usize);

    let node_capabilities = if app_config.hyper.enabled {
        vec![CAPABILITY_HYPER.to_string()]
    } else {
        vec![]
    };

    let gossip_result = SnapchainGossip::create(
        keypair.clone(),
        &app_config.gossip,
        Some(system_tx.clone()),
        app_config.read_node,
        app_config.fc_network,
        statsd_client.clone(),
        node_capabilities,
    )
    .await;

    if let Err(e) = gossip_result {
        error!(error = ?e, "Failed to create SnapchainGossip");
        return Ok(());
    }

    let gossip = gossip_result?;
    let local_peer_id = gossip.swarm.local_peer_id().clone();
    let read_or_validator = if app_config.read_node {
        "read"
    } else {
        "validator"
    };
    info!(
        "Starting Snapchain {} node with public key: {} ({})",
        read_or_validator,
        hex::encode(keypair.public().to_bytes()),
        local_peer_id.to_string()
    );

    let gossip_tx = gossip.tx.clone();

    let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);

    let registry = SharedRegistry::global();
    // Use the new non-global metrics registry when we upgrade to newer version of malachite
    let _ = Metrics::register(registry);
    let (messages_request_tx, messages_request_rx) = mpsc::channel(100);

    let chains_clients = ChainClients::new(&app_config);

    let (sync_complete_tx, sync_complete_rx) = watch::channel(false);

    let (onchain_events_request_tx, onchain_events_request_rx) = broadcast::channel(100);
    let (fname_request_tx, fname_request_rx) = broadcast::channel(100);

    let global_db = RocksDB::open_global_db(&app_config.rocksdb_dir);
    let local_state_store = LocalStateStore::new(global_db);

    if app_config.read_node {
        // Setup post-commit channel if replication is enabled
        let (engine_post_commit_tx, engine_post_commit_rx) = if app_config.replication.enable {
            // TODO: consider increasing the buffer size to prevent blocking across multiple shards
            let (tx, rx) = mpsc::channel::<PostCommitMessage>(1);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        let node = SnapchainReadNode::create(
            keypair.clone(),
            app_config.consensus.clone(),
            local_peer_id,
            gossip_tx.clone(),
            system_tx.clone(),
            app_config.rocksdb_dir.clone(),
            statsd_client.clone(),
            app_config.fc_network,
            registry,
            engine_post_commit_tx,
        )
        .await;

        schedule_background_jobs(
            &app_config,
            node.shard_stores.clone(),
            node.block_stores.clone(),
            sync_complete_rx,
            statsd_client.clone(),
        )
        .await;

        let mut mempool = ReadNodeMempool::new(
            mempool_rx,
            app_config.consensus.num_shards,
            node.shard_stores.clone(),
            node.block_stores.clone(),
            gossip_tx.clone(),
            statsd_client.clone(),
            app_config.fc_network,
        );
        tokio::spawn(async move { mempool.run().await });

        // Setup replication if enabled
        let replicator: Option<Arc<replication::Replicator>> = if app_config.replication.enable {
            let replicator = create_replicator(
                &app_config,
                node.shard_stores.clone(),
                statsd_client.clone(),
            );

            match replicator {
                Ok(replicator) => {
                    let spawned_replicator = replicator.clone();
                    tokio::spawn(async move {
                        replication::replicator::run(
                            spawned_replicator,
                            engine_post_commit_rx.unwrap(),
                        )
                        .await;
                    });
                    Some(replicator)
                }
                Err(e) => {
                    error!("Could not create replicator: {}", e);
                    None
                }
            }
        } else {
            None
        };

        // Initialize API indexing system if enabled
        let api_system = {
            let hub_event_senders: Vec<(u32, broadcast::Sender<snapchain::proto::HubEvent>)> = node
                .shard_senders
                .iter()
                .map(|(shard_id, senders)| (*shard_id, senders.events_tx.clone()))
                .collect();
            snapchain::api::initialize(
                &app_config.api,
                node.block_stores.db.clone(),
                hub_event_senders,
            )
        };

        let api_handler = api_system.as_ref().map(|s| s.http_handler.clone());

        start_servers(
            &app_config,
            gossip,
            mempool_tx,
            shutdown_tx,
            onchain_events_request_tx,
            fname_request_tx,
            statsd_client,
            node.shard_stores.clone(),
            node.shard_senders.clone(),
            node.block_stores.clone(),
            chains_clients,
            replicator,
            local_state_store.clone(),
            api_handler,
        )
        .await;

        let mut shards_finished_syncing = HashSet::new();
        loop {
            select! {
                _ = ctrl_c() => {
                    info!("Received Ctrl-C, shutting down");
                    if let Some(api) = api_system {
                        api.shutdown().await;
                    }
                    node.stop();
                    return Ok(());
                }
                _ = shutdown_rx.recv() => {
                    error!("Received shutdown signal, shutting down");
                    if let Some(api) = api_system {
                        api.shutdown().await;
                    }
                    node.stop();
                    return Ok(());
                }
                Some(msg) = system_rx.recv() => {
                    match msg {
                        SystemMessage::ReadNodeFinishedInitialSync {shard_id} => {
                            info!({shard_id}, "Initial sync completed for shard");
                            shards_finished_syncing.insert(shard_id);
                            // [num_shards] doesn't account for the block shard, so account for it manually
                            if shards_finished_syncing.len() as u32 == app_config.consensus.num_shards + 1 {
                                info!("Initial sync completed for all shards");

                                if let Err(err) = sync_complete_tx.send(true)
                                {
                                    // This happens if there's no block retention threshold configured
                                    info!("Could not send sync complete message to jobs: {}", err.to_string());
                                }

                                if let Err(err) =
                                gossip_tx.send(GossipEvent::SubscribeToDecidedValuesTopic()).await {
                                    panic!("Could not send sync complete message to gossip: {}", err.to_string());
                                }
                            }
                        }
                        SystemMessage::BlockRequest {block_event_seqnum: _ , block_tx: _ } => {},
                        SystemMessage::MalachiteNetwork(shard, event) => {
                            // Forward to appropriate consensus actors
                            node.dispatch_network_event(shard, event);
                        },
                        SystemMessage::Mempool(_) => {},// No need to store mempool messages from other nodes in read nodes
                        SystemMessage::DecidedValueForReadNode(decided_value) => {
                            node.dispatch_decided_value(decided_value);
                        }
                        SystemMessage::ExitWithError(err) => {
                            error!("Exiting due to: {}", err);
                            if let Some(api) = api_system {
                                api.shutdown().await;
                            }
                            node.stop();
                            return Err(err.into());
                        }
                    }
                }
            }
        }
    } else {
        let (shard_decision_tx, shard_decision_rx) = broadcast::channel(100);

        let (block_tx, block_rx) = broadcast::channel(1000);

        // Setup post-commit channel if replication is enabled
        let (engine_post_commit_tx, engine_post_commit_rx) = if app_config.replication.enable {
            // TODO: consider increasing the buffer size to prevent blocking across multiple shards
            let (tx, rx) = mpsc::channel::<PostCommitMessage>(1);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        let node = SnapchainNode::create(
            keypair.clone(),
            app_config.consensus.clone(),
            local_peer_id,
            gossip_tx.clone(),
            shard_decision_tx,
            Some(block_tx.clone()),
            messages_request_tx,
            local_state_store.clone(),
            app_config.rocksdb_dir.clone(),
            statsd_client.clone(),
            app_config.fc_network,
            registry,
            engine_post_commit_tx,
        )
        .await;

        schedule_background_jobs(
            &app_config,
            node.shard_stores.clone(),
            node.block_stores.clone(),
            sync_complete_rx,
            statsd_client.clone(),
        )
        .await;

        let mut mempool = Mempool::new(
            app_config.mempool.clone(),
            app_config.fc_network,
            mempool_rx,
            messages_request_rx,
            app_config.consensus.num_shards,
            node.shard_stores.clone(),
            node.block_stores.clone(),
            gossip_tx.clone(),
            shard_decision_rx,
            block_rx,
            statsd_client.clone(),
        );
        tokio::spawn(async move { mempool.run().await });

        if !app_config.fnames.disable {
            let mut fetcher = snapchain::connectors::fname::Fetcher::new(
                app_config.fnames.clone(),
                mempool_tx.clone(),
                statsd_client.clone(),
                local_state_store.clone(),
                fname_request_rx,
            );

            tokio::spawn(async move {
                fetcher.run().await;
            });
        }

        if !app_config.onchain_events.rpc_url.is_empty() {
            let mut onchain_events_subscriber =
                snapchain::connectors::onchain_events::Subscriber::new(
                    &app_config.onchain_events,
                    node_local_state::Chain::Optimism,
                    mempool_tx.clone(),
                    statsd_client.clone(),
                    local_state_store.clone(),
                    onchain_events_request_rx,
                )?;
            tokio::spawn(async move {
                let result = onchain_events_subscriber.run().await;
                match result {
                    Ok(()) => {}
                    Err(e) => {
                        error!("Error subscribing to on chain events on optimism {:#?}", e);
                    }
                }
            });
        }

        if !app_config.base_onchain_events.rpc_url.is_empty() {
            let mut onchain_events_subscriber =
                snapchain::connectors::onchain_events::Subscriber::new(
                    &app_config.base_onchain_events,
                    node_local_state::Chain::Base,
                    mempool_tx.clone(),
                    statsd_client.clone(),
                    local_state_store.clone(),
                    onchain_events_request_tx.subscribe(),
                )?;
            tokio::spawn(async move {
                let result = onchain_events_subscriber.run().await;
                match result {
                    Ok(()) => {}
                    Err(e) => {
                        error!("Error subscribing to on chain events on base {:#?}", e);
                    }
                }
            });
        }

        // Setup replication if enabled
        let replicator: Option<Arc<replication::Replicator>> = if app_config.replication.enable {
            let replicator = create_replicator(
                &app_config,
                node.shard_stores.clone(),
                statsd_client.clone(),
            );

            match replicator {
                Ok(replicator) => {
                    let spawned_replicator = replicator.clone();
                    tokio::spawn(async move {
                        replication::replicator::run(
                            spawned_replicator,
                            engine_post_commit_rx.unwrap(),
                        )
                        .await;
                    });
                    Some(replicator)
                }
                Err(e) => {
                    error!("Could not create replicator: {}", e);
                    None
                }
            }
        } else {
            None
        };

        if app_config.block_receiver.enabled {
            for shard_id in app_config.consensus.shard_ids.iter() {
                let senders = node.shard_senders.get(shard_id).unwrap();
                let mut block_receiver = BlockReceiver {
                    shard_id: *shard_id,
                    stores: node.shard_stores.get(shard_id).unwrap().clone(),
                    block_rx: block_tx.subscribe(),
                    mempool_tx: mempool_tx.clone(),
                    system_tx: system_tx.clone(),
                    event_rx: senders.events_tx.subscribe(),
                    validator_sets: app_config.consensus.to_stored_validator_sets(*shard_id),
                    config: app_config.block_receiver.clone(),
                };
                tokio::spawn(async move { block_receiver.run().await });
            }
        }

        if let Some(hyper_engine) = node.hyper_block_engine.clone() {
            let mut hyper_block_rx = block_tx.subscribe();
            let hyper_gossip_tx = gossip_tx.clone();
            tokio::spawn(async move {
                loop {
                    match hyper_block_rx.recv().await {
                        Ok(block) => {
                            let state_root = {
                                let mut engine = hyper_engine.lock().await;
                                engine.commit_block(&block);
                                engine.current_state_root()
                            };
                            let envelope =
                                snapchain_hyper::build_envelope_for_block(&block, state_root)
                                    .into();
                            if let Err(err) = hyper_gossip_tx
                                .send(GossipEvent::BroadcastHyperEnvelope(envelope))
                                .await
                            {
                                warn!(reason = ?err, "Failed to broadcast hyper envelope");
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            warn!(skipped, "Hyper block subscriber lagged behind");
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            });
        }

        // Initialize API indexing system if enabled
        let api_system = {
            let hub_event_senders: Vec<(u32, broadcast::Sender<snapchain::proto::HubEvent>)> = node
                .shard_senders
                .iter()
                .map(|(shard_id, senders)| (*shard_id, senders.events_tx.clone()))
                .collect();
            snapchain::api::initialize(
                &app_config.api,
                node.block_stores.db.clone(),
                hub_event_senders,
            )
        };

        let api_handler = api_system.as_ref().map(|s| s.http_handler.clone());

        start_servers(
            &app_config,
            gossip,
            mempool_tx.clone(),
            shutdown_tx.clone(),
            onchain_events_request_tx,
            fname_request_tx,
            statsd_client,
            node.shard_stores.clone(),
            node.shard_senders.clone(),
            node.block_stores.clone(),
            chains_clients,
            replicator,
            local_state_store.clone(),
            api_handler,
        )
        .await;

        // TODO(aditi): We may want to reconsider this code when we upload snapshots on a schedule.
        if app_config.snapshot.backup_on_startup {
            let shard_ids = app_config.consensus.shard_ids.clone();
            let block_stores = node.block_stores.clone();
            let mut dbs = HashMap::new();
            dbs.insert(0, block_stores.db.clone());
            node.shard_stores
                .iter()
                .for_each(|(shard_id, shard_store)| {
                    dbs.insert(*shard_id, shard_store.shard_store.db.clone());
                });
            tokio::spawn(async move {
                info!(
                    "Backing up {:?} shard databases to {:?}",
                    shard_ids, app_config.snapshot.backup_dir
                );
                let timestamp = chrono::Utc::now().timestamp_millis();
                dbs.iter().for_each(|(shard_id, db)| {
                    snapchain::storage::db::backup::backup_db(
                        db.clone(),
                        &app_config.snapshot.backup_dir,
                        *shard_id,
                        timestamp,
                    )
                    .unwrap();
                });
            });
        }

        // Kick it off
        loop {
            select! {
                _ = ctrl_c() => {
                    info!("Received Ctrl-C, shutting down");
                    if let Some(api) = api_system {
                        api.shutdown().await;
                    }
                    node.stop();
                    return Ok(());
                }
                _ = shutdown_rx.recv() => {
                    error!("Received shutdown signal, shutting down");
                    if let Some(api) = api_system {
                        api.shutdown().await;
                    }
                    node.stop();
                    return Ok(());
                }
                Some(msg) = system_rx.recv() => {
                    match msg {
                        SystemMessage::MalachiteNetwork(shard, event) => {
                            // Forward to appropriate consensus actors
                            node.dispatch(shard, event);
                        },
                        SystemMessage::Mempool(msg) => {
                            let res = mempool_tx.try_send(msg);
                            if let Err(e) = res {
                                warn!("Failed to add to local mempool: {:?}", e);
                            }
                        },
                        SystemMessage::BlockRequest {block_event_seqnum, block_tx } => {
                            let block= node.block_stores.get_block_by_event_seqnum(block_event_seqnum);
                            block_tx.send(block).unwrap();
                        },
                        SystemMessage::DecidedValueForReadNode(_) => {
                            // Ignore these for validator nodes
                        }
                        SystemMessage::ReadNodeFinishedInitialSync{shard_id: _} => {
                            // Ignore these for validator nodes
                            sync_complete_tx.send(true)?; // TODO: is this necessary?
                        },
                        SystemMessage::ExitWithError(err) => {
                            error!("Exiting due to: {}", err);
                            if let Some(api) = api_system {
                                api.shutdown().await;
                            }
                            node.stop();
                            return Err(err.into());
                        }
                    }
                }
            }
        }
    }
}
