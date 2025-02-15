use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use informalsystems_malachitebft_metrics::{Metrics, SharedRegistry};
use snapchain::connectors::onchain_events::{L1Client, RealL1Client};
use snapchain::consensus::consensus::SystemMessage;
use snapchain::mempool::mempool::Mempool;
use snapchain::mempool::routing;
use snapchain::network::admin_server::{DbManager, MyAdminService};
use snapchain::network::gossip::SnapchainGossip;
use snapchain::network::http_server::HubHttpServiceImpl;
use snapchain::network::server::MyHubService;
use snapchain::node::snapchain_node::SnapchainNode;
use snapchain::proto::admin_service_server::AdminServiceServer;
use snapchain::proto::hub_service_server::HubServiceServer;
use snapchain::storage::db::snapshot::download_snapshots;
use snapchain::storage::db::RocksDB;
use snapchain::storage::store::node_local_state::LocalStateStore;
use snapchain::storage::store::BlockStore;
use snapchain::utils::statsd_wrapper::StatsdClientWrapper;
use std::collections::HashMap;
use std::error::Error;
use std::net;
use std::net::SocketAddr;
use std::process;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::select;
use tokio::signal::ctrl_c;
use tokio::sync::{broadcast, mpsc};
use tonic::transport::Server;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

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

    if app_config.snapshot.load_db_from_snapshot {
        let mut shard_ids = app_config.consensus.shard_ids.clone();
        shard_ids.push(0);
        for shard_id in shard_ids {
            // Raise if the download fails. If there's a persistent issue, disable snapshot download.
            download_snapshots(
                app_config.fc_network,
                &app_config.snapshot,
                app_config.rocksdb_dir.clone(),
                shard_id,
            )
            .await
            .unwrap();
        }
    }

    if app_config.statsd.prefix == "" {
        // TODO: consider removing this check
        return Err("statsd prefix must be specified in config".into());
    }

    // TODO: parsing to SocketAddr only allows for IPs, DNS names won't work
    let (statsd_host, statsd_port) = match app_config.statsd.addr.parse::<SocketAddr>() {
        Ok(addr) => Ok((addr.ip().to_string(), addr.port())),
        Err(e) => Err(format!("invalid statsd address: {}", e)),
    }?;

    let mut db_manager = DbManager::new(app_config.rocksdb_dir.clone().as_str());
    db_manager.maybe_destroy_databases().unwrap();

    let host = (statsd_host, statsd_port);
    let socket = net::UdpSocket::bind("0.0.0.0:0").unwrap();
    let sink = cadence::UdpMetricSink::from(host, socket)?;
    let statsd_client =
        cadence::StatsdClient::builder(app_config.statsd.prefix.as_str(), sink).build();
    let statsd_client = StatsdClientWrapper::new(statsd_client, app_config.statsd.use_tags);

    let addr = app_config.gossip.address.clone();
    let grpc_addr = app_config.rpc_address.clone();
    let grpc_socket_addr: SocketAddr = grpc_addr.parse()?;
    let http_addr = app_config.http_address.clone();
    let http_socket_addr: SocketAddr = http_addr.parse()?;
    let block_db = RocksDB::open_shard_db(app_config.rocksdb_dir.as_str(), 0);
    let block_store = BlockStore::new(block_db);

    info!(addr = addr, grpc_addr = grpc_addr, "HubService listening",);

    let keypair = app_config.consensus.keypair().clone();

    info!(
        "Starting Snapchain node with public key: {}",
        hex::encode(keypair.public().to_bytes())
    );

    let (system_tx, mut system_rx) = mpsc::channel::<SystemMessage>(100);
    let (mempool_tx, mempool_rx) = mpsc::channel(app_config.mempool.queue_size as usize);

    let gossip_result =
        SnapchainGossip::create(keypair.clone(), app_config.gossip, system_tx.clone());

    if let Err(e) = gossip_result {
        error!(error = ?e, "Failed to create SnapchainGossip");
        return Ok(());
    }

    let mut gossip = gossip_result?;
    let local_peer_id = gossip.swarm.local_peer_id().clone();
    let gossip_tx = gossip.tx.clone();

    tokio::spawn(async move {
        info!("Starting gossip");
        gossip.start().await;
        info!("Gossip Stopped");
    });

    let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);

    let registry = SharedRegistry::global();
    // Use the new non-global metrics registry when we upgrade to newer version of malachite
    let _ = Metrics::register(registry);
    let (messages_request_tx, messages_request_rx) = mpsc::channel(100);
    let (shard_decision_tx, shard_decision_rx) = broadcast::channel(100);

    let global_db = RocksDB::open_global_db(&app_config.rocksdb_dir);
    let local_state_store = LocalStateStore::new(global_db);

    let node = SnapchainNode::create(
        keypair.clone(),
        app_config.consensus.clone(),
        local_peer_id,
        gossip_tx.clone(),
        shard_decision_tx,
        None,
        messages_request_tx,
        block_store.clone(),
        local_state_store.clone(),
        app_config.rocksdb_dir.clone(),
        statsd_client.clone(),
        app_config.trie_branching_factor,
        app_config.fc_network,
        registry,
    )
    .await;

    let mut mempool = Mempool::new(
        app_config.mempool.clone(),
        mempool_rx,
        messages_request_rx,
        app_config.consensus.num_shards,
        node.shard_stores.clone(),
        gossip_tx.clone(),
        shard_decision_rx,
        statsd_client.clone(),
    );
    tokio::spawn(async move { mempool.run().await });

    let admin_service = MyAdminService::new(
        db_manager,
        mempool_tx.clone(),
        node.shard_stores.clone(),
        block_store.clone(),
        app_config.snapshot.clone(),
        app_config.fc_network,
    );

    if !app_config.fnames.disable {
        let mut fetcher = snapchain::connectors::fname::Fetcher::new(
            app_config.fnames.clone(),
            mempool_tx.clone(),
            statsd_client.clone(),
            local_state_store.clone(),
        );

        tokio::spawn(async move {
            fetcher.run().await;
        });
    }

    if !app_config.onchain_events.rpc_url.is_empty() {
        let mut onchain_events_subscriber = snapchain::connectors::onchain_events::Subscriber::new(
            app_config.onchain_events,
            mempool_tx.clone(),
            statsd_client.clone(),
            local_state_store,
        )?;
        tokio::spawn(async move {
            let result = onchain_events_subscriber.run().await;
            match result {
                Ok(()) => {}
                Err(e) => {
                    error!("Error subscribing to on chain events {:#?}", e);
                }
            }
        });
    }

    let rpc_shard_stores = node.shard_stores.clone();
    let rpc_shard_senders = node.shard_senders.clone();

    let rpc_block_store = block_store.clone();
    let l1_client: Option<Box<dyn L1Client>> = match RealL1Client::new(app_config.l1_rpc_url) {
        Ok(client) => Some(Box::new(client)),
        Err(_) => None,
    };
    let mempool_tx_for_service = mempool_tx.clone();
    let service = Arc::new(MyHubService::new(
        rpc_block_store,
        rpc_shard_stores,
        rpc_shard_senders,
        statsd_client.clone(),
        app_config.consensus.num_shards,
        Box::new(routing::ShardRouter {}),
        mempool_tx_for_service,
        l1_client,
    ));
    let grpc_service = service.clone();
    let grpc_shutdown_tx = shutdown_tx.clone();
    tokio::spawn(async move {
        let resp = Server::builder()
            .add_service(HubServiceServer::from_arc(grpc_service))
            .add_service(AdminServiceServer::new(admin_service))
            .serve(grpc_socket_addr)
            .await;

        let msg = "grpc server stopped";
        match resp {
            Ok(()) => error!(msg),
            Err(e) => error!(error = ?e, "{}", msg),
        }

        grpc_shutdown_tx.send(()).await.ok();
    });

    let http_shutdown_tx = shutdown_tx.clone();
    tokio::spawn(async move {
        let listener = TcpListener::bind(http_socket_addr).await.unwrap();

        let http_service = HubHttpServiceImpl {
            service: service.clone(),
        };
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let io = TokioIo::new(stream);
                    let service_clone = http_service.clone();
                    tokio::spawn(async move {
                        let router = snapchain::network::http_server::Router::new(service_clone);
                        if let Err(err) = http1::Builder::new()
                            .serve_connection(io, service_fn(|r| router.handle(r)))
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

    // TODO(aditi): We may want to reconsider this code when we upload snapshots on a schedule.
    if app_config.snapshot.backup_on_startup {
        let shard_ids = app_config.consensus.shard_ids.clone();
        let block_db = block_store.db.clone();
        let mut dbs = HashMap::new();
        dbs.insert(0, block_db.clone());
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
                RocksDB::backup_db(
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
                node.stop();
                return Ok(());
            }
            _ = shutdown_rx.recv() => {
                error!("Received shutdown signal, shutting down");
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
                }
            }
        }
    }
}
