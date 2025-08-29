use crate::bootstrap::error::BootstrapError;
use crate::cfg::Config;
use crate::proto::replication_service_client::ReplicationServiceClient;
use crate::proto::{self, GetShardSnapshotMetadataRequest, ShardSnapshotMetadata};
use crate::storage::constants::RootPrefix;
use crate::storage::db::{RocksDB, RocksDbTransactionBatch};
use crate::storage::trie::merkle_trie::{self, TrieKey};
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use crate::version::version::EngineVersion;
use base64::{engine::general_purpose, Engine as _};
use futures::channel::oneshot;
use futures::future::try_join_all;
use rand::seq::IteratorRandom;
use rand::seq::SliceRandom;
use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::{net, u64};
use tokio::signal::ctrl_c;
use tokio::sync::mpsc;
use tonic::transport::Channel;
use tracing::{debug, error, info};

pub enum WorkUnitResponse {
    // Still working on it
    Working = 1,
    // User manually stopped it (CTRL+C). Will resume after snapchain is restarted
    Stopped = 2,
    // Bootstrap was finished, but there were some errors. Should continue with startup
    PartiallyComplete = 3,
    // Successfully finished, ready to continue with startup
    Finished = 4,
}

/// Request to commit a transaction batch to the database
struct DbCommitRequest {
    txn_batch: RocksDbTransactionBatch,
    num_messages: u64,
    response_tx: oneshot::Sender<Result<(), BootstrapError>>,
}

struct WorkUnit {
    current_status: proto::ReplicationTriePartStatus,
    peer_address: String,
    shutdown_signal: Arc<AtomicBool>,
    db_commit_tx: mpsc::UnboundedSender<DbCommitRequest>,
    response_tx: oneshot::Sender<Result<WorkUnitResponse, BootstrapError>>,
}

pub struct ReplicatorBootstrap {
    shutdown: Arc<AtomicBool>,
}

impl ReplicatorBootstrap {
    pub fn new() -> Self {
        Self {
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    fn make_work_unit_key(shard_id: u8, virtual_trie_shard: u8) -> Vec<u8> {
        let mut key = Vec::with_capacity(3);
        key.push(RootPrefix::ReplicationBootstrapStatus as u8);
        key.push(shard_id);
        key.push(virtual_trie_shard);
        key
    }

    /// Bootstrap a node from replication instead of snapshot download
    pub async fn bootstrap_using_replication(
        &self,
        app_config: &Config,
    ) -> Result<WorkUnitResponse, BootstrapError> {
        info!("Starting replication-based bootstrap");

        // Fetch metadata and determine target height
        // Get the peer address from the client's channel
        let peer_address = "http://127.0.0.1:3383";
        let mut client = ReplicationServiceClient::connect(peer_address)
            .await
            .map_err(|e| BootstrapError::PeerConnectionError(e.to_string()))?;

        let shard_metadata = self
            .get_shard_snapshots(&mut client, app_config.consensus.shard_ids.to_vec())
            .await?;

        // Initialize databases and replay transactions for each shard
        // Note: We're not replicating shard 0 (block shard) for now as requested
        let shard_ids = app_config.consensus.shard_ids.clone();

        // Initialize statsd client for engines
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
        let socket = UdpSocket::bind("0.0.0.0:0")?;
        let sink = cadence::UdpMetricSink::from(host, socket).map_err(|e| {
            BootstrapError::GenericError(format!("Failed to create UDP metric sink: {}", e))
        })?;
        let statsd_client =
            cadence::StatsdClient::builder(app_config.statsd.prefix.as_str(), sink).build();
        let statsd_client = StatsdClientWrapper::new(statsd_client, app_config.statsd.use_tags);

        for shard_id in shard_ids {
            let metadata = shard_metadata.get(&shard_id).ok_or_else(|| {
                BootstrapError::MetadataFetchError(format!(
                    "No metadata found for shard {}",
                    shard_id
                ))
            })?;

            let target_height = metadata.height;

            if target_height == 0 {
                return Err(BootstrapError::GenericError(format!(
                    "No valid snapshot found for shard {}. Skipping bootstrap.",
                    shard_id
                )));
            }
            info!(
                "Bootstrapping shard {} at height {}",
                shard_id, target_height
            );

            // Clone necessary data for the spawned task
            let peer_address_clone = peer_address.to_string();
            let network = app_config.fc_network;
            let trie_branching_factor = app_config.trie_branching_factor;
            let statsd_client_clone = statsd_client.clone();
            let rocksdb_dir = format!("{}.snapshot", app_config.rocksdb_dir.clone());

            // Spawn a task for this shard
            let shutdown_signal = self.shutdown.clone();
            let metadata = metadata.clone();

            // Replay transactions for this shard
            let result = Self::start_shard_replication(
                &peer_address_clone,
                network,
                shard_id,
                trie_branching_factor,
                target_height,
                &statsd_client_clone,
                &rocksdb_dir,
                metadata,
                shutdown_signal,
            )
            .await;

            match &result {
                Ok(_) => info!("Completed bootstrap for shard {}", shard_id),
                Err(e) => error!("Failed bootstrap for shard {}: {}", shard_id, e),
            }
        }

        Ok(WorkUnitResponse::Stopped)
    }

    async fn start_shard_replication(
        peer_address: &str,
        network: crate::proto::FarcasterNetwork,
        shard_id: u32,
        trie_branching_factor: u32,
        target_height: u64,
        statsd_client: &StatsdClientWrapper,
        rocksdb_dir: &str,
        metadata: ShardSnapshotMetadata,
        shutdown_signal: Arc<AtomicBool>,
    ) -> Result<WorkUnitResponse, BootstrapError> {
        let db = RocksDB::open_bulk_write_shard_db(&rocksdb_dir, shard_id);

        // Create a channel for DB commit requests
        let (db_commit_tx, mut db_commit_rx) = mpsc::unbounded_channel::<DbCommitRequest>();

        // Clone db for the DB writer thread
        let db_writer = db.clone();

        // Spawn the DB writer thread
        let db_writer_task = tokio::spawn(async move {
            info!("DB writer thread started for shard {}", shard_id);
            let mut total_messages = 0;
            let total_start_time = std::time::Instant::now();

            while let Some(commit_request) = db_commit_rx.recv().await {
                let start_time = std::time::Instant::now();
                total_messages += commit_request.num_messages;

                let result = db_writer.commit(commit_request.txn_batch).map_err(|e| {
                    error!("DB writer - Failed to commit transaction: {}", e);
                    BootstrapError::DatabaseError(format!("Failed to commit transaction: {}", e))
                });
                let elapsed_time = start_time.elapsed();
                let total_elapsed = total_start_time.elapsed();
                info!(
                "DB writer({}) - Commit took {:?}. Writing at {:.1}k msg/sec {:.2}M total messages",
                shard_id,
                elapsed_time,
                total_messages as f64 / 1000.0 / total_elapsed.as_secs_f64(),
                total_messages as f64 / 1_000_000.0
            );

                // Send the result back to the requesting thread
                if let Err(_) = commit_request.response_tx.send(result) {
                    error!("DB writer - Failed to send commit result - receiver dropped");
                }
            }

            info!("DB writer thread finished for shard {}", shard_id);
        });

        // Go over each Trie's Virtual Shard ID (VSI) and sync for it.
        for vsi in 0..merkle_trie::TRIE_SHARD_SIZE {
            // 1. Read the progress for this from the DB

            // 2. Create a work item for this VSI

            // 3. Start iterating over this VSI and merging messages
        }

        // Drop the original sender so the DB writer thread can exit
        drop(db_commit_tx);

        // Wait for the DB writer thread to finish
        if let Err(e) = db_writer_task.await {
            error!("DB writer task failed: {}", e);
            return Err(BootstrapError::DatabaseError(format!(
                "DB writer task failed: {}",
                e
            )));
        }

        db.close();

        Ok(WorkUnitResponse::Finished)
    }

    /// Determine the highest block for each shard.
    async fn get_shard_snapshots(
        &self,
        client: &mut ReplicationServiceClient<Channel>,
        all_shards: Vec<u32>,
    ) -> Result<HashMap<u32, ShardSnapshotMetadata>, BootstrapError> {
        let mut shard_metadata = HashMap::new();

        // TODO: Shard 0 doesn't have a snapshot, so we skip it?

        for shard_id in all_shards {
            let request = GetShardSnapshotMetadataRequest { shard_id };

            match client.get_shard_snapshot_metadata(request).await {
                Ok(response) => {
                    let metadata_response = response.into_inner();

                    if metadata_response.snapshots.is_empty() {
                        error!("No snapshots found for shard {}", shard_id);
                        return Err(BootstrapError::MetadataFetchError(
                            "No snapshots found".into(),
                        ));
                    }

                    // Find the largest height in the response, and use that
                    let mut latest_snapshot = metadata_response.snapshots[0].clone();
                    for snapshot in metadata_response.snapshots {
                        if snapshot.height > latest_snapshot.height {
                            latest_snapshot = snapshot;
                        }
                    }

                    shard_metadata.insert(shard_id, latest_snapshot);
                }
                Err(e) => {
                    error!("Failed to get metadata for shard {}: {}", shard_id, e);
                    return Err(BootstrapError::MetadataFetchError(e.to_string()));
                }
            }
        }

        Ok(shard_metadata)
    }
}
