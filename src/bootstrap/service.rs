use crate::bootstrap::error::BootstrapError;
use crate::cfg::Config;
use crate::consensus::proposer::ProposalSource;
use crate::core::error::HubError;
use crate::core::util::FarcasterTime;
use crate::proto::get_shard_transactions_request::Cursor;
use crate::proto::replication_service_client::ReplicationServiceClient;
use crate::proto::{
    ChildHashDebugInfo, GetShardSnapshotMetadataRequest, GetShardTransactionsRequest,
    ShardSnapshotMetadata, SortOrderTypes, Transaction, TrieNodeDebugInfo,
};
use crate::storage::constants::RootPrefix;
use crate::storage::db::{RocksDB, RocksDbTransactionBatch};
use crate::storage::store::account::StoreOptions;
use crate::storage::store::engine::ShardEngine;
use crate::storage::store::stores::StoreLimits;
use crate::storage::trie::errors::TrieError;
use crate::storage::trie::merkle_trie::{self, TrieKey};
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use crate::version::version::EngineVersion;
use base64::{engine::general_purpose, Engine as _};
use futures::future::try_join_all;
use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::{net, u64};
use tokio::signal::ctrl_c;
use tokio::sync::{mpsc, oneshot};
use tonic::transport::Channel;
use tracing::{debug, error, info};

/// FID range for work distribution
#[derive(Debug, Clone, PartialEq)]
struct FidRange {
    shard_id: u32,
    fid_start: u64,
    fid_end: u64,
}

impl FidRange {
    /// Serialize the FidRange to a Vec<u8>
    fn to_vec(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(20); // 4 + 8 + 8 bytes
        bytes.extend_from_slice(&self.shard_id.to_le_bytes());
        bytes.extend_from_slice(&self.fid_start.to_le_bytes());
        bytes.extend_from_slice(&self.fid_end.to_le_bytes());
        bytes
    }

    /// Deserialize a FidRange from a Vec<u8>
    fn from_vec(bytes: Vec<u8>) -> Result<Self, String> {
        if bytes.len() != 20 {
            return Err(format!(
                "Invalid byte length: expected 20, got {}",
                bytes.len()
            ));
        }

        let shard_id = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let fid_start = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
        let fid_end = u64::from_le_bytes(bytes[12..20].try_into().unwrap());

        Ok(FidRange {
            shard_id,
            fid_start,
            fid_end,
        })
    }
}

/// Tracks completed FID ranges and merges consecutive ranges
#[derive(Debug, Clone)]
struct CompletedRanges {
    ranges: Vec<FidRange>,
}

impl CompletedRanges {
    fn new() -> Self {
        Self { ranges: Vec::new() }
    }

    /// Add a completed range and merge with consecutive ranges
    fn add_range(&mut self, new_range: FidRange) {
        // Insert the range in the correct position
        let insert_pos = self
            .ranges
            .binary_search_by_key(&new_range.fid_start, |r| r.fid_start)
            .unwrap_or_else(|pos| pos);

        self.ranges.insert(insert_pos, new_range);

        // Merge consecutive ranges
        self.merge_consecutive_ranges();
    }

    /// Merge consecutive ranges
    fn merge_consecutive_ranges(&mut self) {
        if self.ranges.is_empty() {
            return;
        }

        let mut merged = Vec::new();
        let mut current = self.ranges[0].clone();

        for range in self.ranges.iter().skip(1) {
            if current.fid_end + 1 == range.fid_start {
                // Ranges are consecutive, merge them
                current.fid_end = range.fid_end;
            } else {
                // Not consecutive, push current and start new one
                merged.push(current);
                current = range.clone();
            }
        }
        merged.push(current);

        self.ranges = merged;
    }

    /// Get the highest consecutive FID starting from 1
    fn get_highest_consecutive_fid(&self, shard_id: u32) -> FidRange {
        if self.ranges.is_empty() {
            return FidRange {
                shard_id,
                fid_start: 0,
                fid_end: 0,
            };
        }

        // Check if we have a range starting from 1
        if let Some(first_range) = self.ranges.first() {
            if first_range.fid_start == 1 {
                return first_range.clone();
            }
        }

        FidRange {
            shard_id,
            fid_start: 0,
            fid_end: 0,
        }
    }

    /// Check if a FID range is already completed
    fn is_range_completed(&self, range: &FidRange) -> bool {
        for completed_range in &self.ranges {
            if completed_range.shard_id == range.shard_id
                && completed_range.fid_start <= range.fid_start
                && completed_range.fid_end >= range.fid_end
            {
                return true;
            }
        }
        false
    }

    /// Serialize the CompletedRanges to a Vec<u8>
    fn to_vec(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Write the number of ranges
        bytes.extend_from_slice(&(self.ranges.len() as u32).to_le_bytes());

        // Write each range
        for range in &self.ranges {
            bytes.extend_from_slice(&range.to_vec());
        }

        bytes
    }

    /// Deserialize a CompletedRanges from a Vec<u8>
    fn from_vec(bytes: Vec<u8>) -> Result<Self, String> {
        if bytes.len() < 4 {
            return Err("Invalid byte length: too short".to_string());
        }

        let num_ranges = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let expected_len = 4 + (num_ranges * 20); // 4 bytes for count + 20 bytes per range

        if bytes.len() != expected_len {
            return Err(format!(
                "Invalid byte length: expected {}, got {}",
                expected_len,
                bytes.len()
            ));
        }

        let mut ranges = Vec::new();
        let mut offset = 4;

        for _ in 0..num_ranges {
            let range_bytes = bytes[offset..offset + 20].to_vec();
            let range = FidRange::from_vec(range_bytes)?;
            ranges.push(range);
            offset += 20;
        }

        Ok(CompletedRanges { ranges })
    }
}

/// Request to commit a transaction batch to the database
struct DbCommitRequest {
    txn_batch: RocksDbTransactionBatch,
    num_messages: u64,
    response_tx: oneshot::Sender<Result<(), BootstrapError>>,
}

/// Work unit for worker threads
struct WorkUnit {
    range: FidRange,
    peer_address: String,
    db_commit_tx: mpsc::UnboundedSender<DbCommitRequest>,
    response_tx: oneshot::Sender<Result<WorkUnitResponse, BootstrapError>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WorkUnitResponse {
    Stopped,
    CompletedRange,
    PartiallyComplete,
    NoMoreFIDs(HashMap<u32, u64>), // Maps shard_id to highest FID processed
}

/// Signal to stop worker threads
#[derive(Clone)]
struct StopSignal;

/// Message that can be sent to worker threads
enum WorkerMessage {
    Work(WorkUnit),
    Stop(StopSignal),
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

    /// Bootstrap a node from replication instead of snapshot download
    pub async fn bootstrap_using_replication(
        &self,
        app_config: &Config,
    ) -> Result<WorkUnitResponse, Box<dyn Error>> {
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
        let socket = net::UdpSocket::bind("0.0.0.0:0")?;
        let sink = cadence::UdpMetricSink::from(host, socket)?;
        let statsd_client =
            cadence::StatsdClient::builder(app_config.statsd.prefix.as_str(), sink).build();
        let statsd_client = StatsdClientWrapper::new(statsd_client, app_config.statsd.use_tags);

        // Create tasks for each shard to run in parallel
        let mut shard_tasks = Vec::new();

        for shard_id in shard_ids {
            let metadata = shard_metadata.get(&shard_id).ok_or_else(|| {
                BootstrapError::MetadataFetchError(format!(
                    "No metadata found for shard {}",
                    shard_id
                ))
            })?;

            let target_height = metadata.height;
            let highest_fid = metadata.highest_fid;

            if target_height == 0 || highest_fid == 0 {
                return Err(Box::new(HubError::unavailable(
                    format!(
                        "No valid snapshot found for shard {}. Skipping bootstrap.",
                        shard_id
                    )
                    .as_str(),
                )));
            }
            info!(
                "Bootstrapping shard {} at height {}, highest_fid = {}",
                shard_id, target_height, highest_fid
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

            let task = tokio::spawn(async move {
                info!("Starting parallel bootstrap for shard {}", shard_id);

                // Replay transactions for this shard
                let result = start_shard_replication_outer(
                    &peer_address_clone,
                    network,
                    shard_id,
                    trie_branching_factor,
                    target_height,
                    highest_fid,
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

                (shard_id, result)
            });

            shard_tasks.push(task);
        }

        // Wait for either all shard tasks to complete or a shutdown signal
        let results;
        info!("Waiting for shutdown signal or shard tasks to complete");
        tokio::select! {
            // Wait for all shard tasks to complete normally
            res = try_join_all(&mut shard_tasks) => {
                results = res.map_err(|e| {
                    Box::new(BootstrapError::TransactionReplayError(format!(
                        "Shard task join error: {}",
                        e
                    ))) as Box<dyn Error>
                })?;
            }
            // Handle shutdown signal
            _ = ctrl_c() => {
                info!("Shutdown signal received, stopping all shard replication tasks");

                // Broadcast shutdown to all shard tasks
                self.shutdown.store(true, Ordering::SeqCst);

                // After sending the shutdown signal, we still need to wait for the tasks
                // to finish their cleanup. We can re-await the `try_join_all` future.
                // We expect it to return errors as tasks might be cancelled, which is okay.
                info!("Waiting for shard tasks to shut down gracefully...");

                // We re-assign `shard_tasks` into the `try_join_all` to consume it.
                // The result is ignored here because we are intentionally shutting down.
                // The tasks should handle the shutdown signal and exit gracefully.
                let _ = try_join_all(shard_tasks).await;

                info!("All shard tasks have been shut down.");
                return Ok(WorkUnitResponse::Stopped);
            }
        }

        let all_shards_finished = results.iter().all(|(_, res)| match res {
            Ok(WorkUnitResponse::NoMoreFIDs(_)) => true,
            _ => false,
        });

        if all_shards_finished {
            // Collect all the highest FID per shard hashmaps into one
            let mut combined_fids = HashMap::new();
            for (_, res) in results {
                if let Ok(WorkUnitResponse::NoMoreFIDs(fids)) = res {
                    combined_fids.extend(fids);
                }
            }

            return Ok(WorkUnitResponse::NoMoreFIDs(combined_fids));
        }

        // This part now only runs if the tasks completed without a shutdown signal.
        // Check if any shard returned an error
        for (shard_id, result) in results {
            if let Err(e) = result {
                error!("Shard {} failed during bootstrap: {}", shard_id, e);
                return Err(Box::new(e) as Box<dyn Error>);
            }
            info!("Shard {} completed successfully", shard_id);
        }

        Ok(WorkUnitResponse::PartiallyComplete)
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

async fn start_shard_replication_outer(
    peer_address: &str,
    network: crate::proto::FarcasterNetwork,
    shard_id: u32,
    trie_branching_factor: u32,
    target_height: u64,
    highest_fid: u64,
    statsd_client: &StatsdClientWrapper,
    rocksdb_dir: &str,
    metadata: ShardSnapshotMetadata,
    shutdown_signal: Arc<AtomicBool>,
) -> Result<WorkUnitResponse, BootstrapError> {
    // ============== TEMP
    if let Err(e) = post_process_trie(
        rocksdb_dir,
        trie_branching_factor,
        shard_id,
        highest_fid,
        metadata,
        shutdown_signal,
    )
    .await
    {
        error!("Error post processing trie for shard {}: {}", shard_id, e);
        return Err(BootstrapError::PostProcessError(e.to_string()));
    }

    return Ok(WorkUnitResponse::Stopped);

    // Go over the FIDs in chunks of 10k and import them into the DB in batches. This allows the
    // DB to compact and flush every 10k FIDs, managing the SST files more efficiently.

    let mut start_fid = 1;
    let step = 10_000;
    loop {
        if shutdown_signal.load(Ordering::SeqCst) {
            info!("Shutdown signal received, stopping shard replication");
            return Ok(WorkUnitResponse::Stopped);
        }

        let max_fid = start_fid + step as u64 - 1;
        let completed = start_shard_replication(
            peer_address,
            network,
            shard_id,
            trie_branching_factor,
            target_height,
            statsd_client,
            rocksdb_dir,
            max_fid,
            shutdown_signal.clone(),
        )
        .await?;

        if let WorkUnitResponse::NoMoreFIDs(_) = completed {
            info!("No more FIDs to process for shard {}. ", shard_id);

            // if let Err(e) = post_process_trie(
            //     rocksdb_dir,
            //     trie_branching_factor,
            //     shard_id,
            //     highest_fid,
            //     metadata,
            //     shutdown_signal,
            // )
            // .await
            // {
            //     error!("Error post processing trie for shard {}: {}", shard_id, e);
            //     return Err(BootstrapError::PostProcessError(e.to_string()));
            // }

            return Ok(completed);
        }

        start_fid += step;
    }
}

fn pretty_print_debug_info(info: &TrieNodeDebugInfo) {
    println!(
        "xPrefix: {}",
        general_purpose::STANDARD.encode(&info.xprefix)
    );
    println!("hash: {}", general_purpose::STANDARD.encode(&info.hash));
    println!("'childHashes':");
    for child in &info.child_hashes {
        println!(
            "{{ \n'char': {}\n'hash': {}\n }},\n",
            child.char,
            general_purpose::STANDARD.encode(&child.hash)
        );
    }
    println!("]")
}

fn get_one_node_in_007(db: &RocksDB, trie: &merkle_trie::MerkleTrie) {
    let mut xprefix = vec![1, 2, 0, 0, 0, 0, 7, 5];
    let mut node;
    while xprefix.len() != 10 {
        let maybe_node = trie
            .get_x_node(&db, &xprefix)
            .ok_or_else(|| format!("Failed to get trie node for prefix {:?}", xprefix));
        if maybe_node.is_err() {
            error!("{}", maybe_node.err().unwrap());
            return;
        }
        node = maybe_node.unwrap();
        let first_child = node.children().keys().next().unwrap();
        xprefix.push(*first_child as u8);
    }

    info!("007 root node is {:?}", xprefix);
}

fn get_trie_debug_info(
    db: &RocksDB,
    trie: &merkle_trie::MerkleTrie,
    fid: u64,
) -> Result<Vec<TrieNodeDebugInfo>, String> {
    let mut result = vec![];

    let fid_key = TrieKey::for_fid(fid);
    let xfid_key = trie.get_x_key(&fid_key);

    // For every prefix in xfid_key, get the node's hash
    for i in 0..(xfid_key.len() + 1) {
        let xprefix = xfid_key[..i].to_vec();

        let node = trie
            .get_x_node(&db, &xprefix)
            .ok_or_else(|| format!("Failed to get trie node for prefix {:?}", xprefix))?;
        let node_hash = node.hash();

        let child_hashes = node
            .child_hashes()
            .iter()
            .map(|(k, v)| ChildHashDebugInfo {
                char: *k as u32,
                hash: v.clone(),
            })
            .collect();

        let debug_info = TrieNodeDebugInfo {
            xprefix,
            hash: node_hash,
            child_hashes,
        };
        result.push(debug_info);
    }

    return Ok(result);
}

async fn post_process_trie(
    rocksdb_dir: &str,
    trie_branching_factor: u32,
    shard_id: u32,
    highest_fid: u64,
    metadata: ShardSnapshotMetadata,
    shutdown_signal: Arc<AtomicBool>,
) -> Result<(), TrieError> {
    info!(
        "PPTrie: Post-processing trie for shard {}. Highest FID: {}",
        shard_id, highest_fid
    );

    let db = RocksDB::open_bulk_write_shard_db(&rocksdb_dir, shard_id);
    let mut trie = merkle_trie::MerkleTrie::new(trie_branching_factor)?;
    trie.initialize(&db)?;

    // =====================TEMP
    // get_one_node_in_007(&db, &trie);
    // return Ok(());

    let fid3_debug_info = get_trie_debug_info(&db, &trie, 29974);
    if let Ok(info) = fid3_debug_info {
        for i in info {
            pretty_print_debug_info(&i);
        }
    }
    return Ok(());
    // ====================TEMP

    let trie_ctx = merkle_trie::Context::new();

    // Go over all the keys for this FID in the trie
    for fid in 0..highest_fid + 1 {
        if shutdown_signal.load(Ordering::SeqCst) {
            info!(
                "PPTrie {}: Shutdown signal received during post-processing at FID {}. Stopping.",
                shard_id, fid
            );
            return Ok(());
        }

        let fid_root_trie_key = TrieKey::for_fid(fid);
        let mut txn_batch = RocksDbTransactionBatch::new();

        match trie.attach_to_root(&trie_ctx, &db, &mut txn_batch, &fid_root_trie_key) {
            Ok((is_attached, was_created)) => {
                if is_attached && was_created {
                    info!(
                        "PPTrie {}: Successfully attached trie node for FID {}",
                        shard_id, fid
                    );
                } else if is_attached && !was_created {
                    info!(
                        "PPTrie {}: Trie node for FID {} already exists, skipping attachment",
                        shard_id, fid
                    );
                } else {
                    info!(
                        "PPTrie {}: Trie node for FID {} doesn't belong to shard",
                        shard_id, fid
                    );
                }
            }
            Err(e) => {
                error!(
                    "PPTrie {}: Failed to attach trie node for FID {}: {:?}",
                    shard_id, fid, e
                );
            }
        }

        if txn_batch.len() > 0 {
            db.commit(txn_batch).map_err(|e| TrieError::DatabaseError {
                source: Box::new(e),
            })?;
        }
        trie.reload(&db)?;
    }

    // Get the trie root and see if it matches
    let expected_shard_root = metadata.shard_chunk.unwrap().header.unwrap().shard_root;
    let trie_root = trie.root_hash().unwrap();

    info!(
        "PPTrie: Finished post-processing trie for shard {}. expected: {:?} actual {:?}",
        shard_id,
        hex::encode(expected_shard_root),
        hex::encode(trie_root)
    );

    Ok(())
}

async fn start_shard_replication(
    peer_address: &str,
    network: crate::proto::FarcasterNetwork,
    shard_id: u32,
    trie_branching_factor: u32,
    target_height: u64,
    statsd_client: &StatsdClientWrapper,
    rocksdb_dir: &str,
    max_fid: u64,
    shutdown_signal: Arc<AtomicBool>,
) -> Result<WorkUnitResponse, BootstrapError> {
    info!(
        "Starting parallel shard replication for shard {} up to height {} upto fid {}",
        shard_id, target_height, max_fid
    );

    let db = RocksDB::open_bulk_write_shard_db(&rocksdb_dir, shard_id);

    // Read previously saved completed ranges for this shard
    let initial_completed_ranges = match read_completed_ranges(&db, shard_id).await {
        Ok(ranges) => ranges,
        Err(e) => {
            error!(
                "Failed to read completed ranges for shard {}: {}",
                shard_id, e
            );
            CompletedRanges::new()
        }
    };
    info!(
        "Initial completed ranges for shard {}: {:?}",
        shard_id, initial_completed_ranges
    );

    // Generate work ranges
    let chunk_size = 100u64;
    let num_workers = 12;
    // Track work state - use the initial completed ranges from the parameters
    let mut completed_ranges = initial_completed_ranges;
    let mut pending_work = HashMap::new();
    let mut worker_queue: VecDeque<usize> = (0..num_workers).collect();

    let highest_consecutive = completed_ranges.get_highest_consecutive_fid(shard_id);
    let mut current_fid = if highest_consecutive.fid_end > 0 {
        highest_consecutive.fid_end + 1
    } else {
        1u64
    };

    let mut all_work_sent = false;
    if current_fid > max_fid {
        info!(
            "No work to do for shard {} as current fid {} exceeds max fid {}",
            shard_id, current_fid, max_fid
        );
        db.close();
        return Ok(WorkUnitResponse::CompletedRange);
    }

    info!(
        "start_shard_replication for shard {} starting work at highest fid {}",
        shard_id, current_fid
    );

    let statsd_client = statsd_client.clone();

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

    // Create worker threads
    let worker_channels = create_workers_for_shard(
        num_workers,
        network,
        shard_id,
        trie_branching_factor,
        target_height,
        statsd_client.clone(),
        db.clone(),
    )
    .await?;

    let mut last_shard_fid = None;

    loop {
        // Check for shutdown signal first
        if shutdown_signal.load(Ordering::SeqCst) {
            info!(
                "Shutdown signal received for shard {}, stopping replication",
                shard_id
            );
            all_work_sent = true; // Mark as done to exit the loop
        }

        // Send work to available workers
        while !worker_queue.is_empty() && !all_work_sent && current_fid <= max_fid {
            let worker_idx = worker_queue.pop_front().unwrap();

            let end_fid = std::cmp::min(current_fid + chunk_size - 1, max_fid);

            let work_range = FidRange {
                shard_id,
                fid_start: current_fid,
                fid_end: end_fid,
            };

            // Check if this range is already completed
            if completed_ranges.is_range_completed(&work_range) {
                info!(
                    "Range {} to {} for shard {} already completed, skipping",
                    current_fid, end_fid, shard_id
                );

                // Put the worker back in the queue and move to next range
                worker_queue.push_back(worker_idx);
                current_fid = end_fid + 1;
                if current_fid > max_fid {
                    all_work_sent = true;
                }
                continue;
            }

            let (response_tx, response_rx) = oneshot::channel();
            let work_unit = WorkUnit {
                range: work_range,
                peer_address: peer_address.to_string(),
                db_commit_tx: db_commit_tx.clone(),
                response_tx,
            };

            if let Err(_) = worker_channels[worker_idx]
                .send(WorkerMessage::Work(work_unit))
                .await
            {
                error!("Failed to send work to worker {}", worker_idx);
                return Err(BootstrapError::TransactionReplayError(
                    "Worker communication failed".to_string(),
                ));
            }

            pending_work.insert(current_fid, (response_rx, worker_idx));

            current_fid = end_fid + 1;
            if current_fid > max_fid {
                all_work_sent = true;
            }

            debug!(
                "Sent work for FID range {} to {} to worker {}",
                current_fid, end_fid, worker_idx
            );
        }

        // Check for completed work
        let mut completed_work = Vec::new();
        for (start_fid, (rx, worker_idx)) in &mut pending_work {
            if let Ok(result) = rx.try_recv() {
                completed_work.push((*start_fid, result, *worker_idx));
            }
        }

        // Process completed work
        for (start_fid, result, worker_idx) in completed_work {
            pending_work.remove(&start_fid);

            match result {
                Ok(wu_response) => {
                    let end_fid = std::cmp::min(start_fid + chunk_size - 1, max_fid);
                    info!(
                        "Work completed for FID range {} to {}. Result {:?}",
                        start_fid, end_fid, wu_response
                    );

                    if let WorkUnitResponse::NoMoreFIDs(r) = wu_response {
                        last_shard_fid = Some(r);
                        all_work_sent = true;
                    }

                    // Add the completed range
                    completed_ranges.add_range(FidRange {
                        shard_id,
                        fid_start: start_fid,
                        fid_end: end_fid,
                    });
                    update_completed_ranges(&db, completed_ranges.clone()).await?;

                    // Make worker available again
                    worker_queue.push_back(worker_idx);
                }
                Err(e) => {
                    error!(
                        "Work failed for FID range starting at {}: {}. Retrying...",
                        start_fid, e
                    );
                    // Retry the work
                    let (response_tx, response_rx) = oneshot::channel();
                    let work_unit = WorkUnit {
                        range: FidRange {
                            shard_id,
                            fid_start: start_fid,
                            fid_end: start_fid,
                        },
                        peer_address: peer_address.to_string(),
                        db_commit_tx: db_commit_tx.clone(),
                        response_tx,
                    };

                    if let Err(_) = worker_channels[worker_idx]
                        .send(WorkerMessage::Work(work_unit))
                        .await
                    {
                        error!("Failed to send work to worker {}", worker_idx);
                        return Err(BootstrapError::TransactionReplayError(
                            "Worker communication failed".to_string(),
                        ));
                    }

                    pending_work.insert(start_fid, (response_rx, worker_idx));
                }
            }
        }

        // Check if we're done
        if all_work_sent && pending_work.is_empty() {
            break;
        }

        // Small delay to avoid busy waiting
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }

    // Send stop signal to all workers
    info!(
        "Sending stop signal to all {} workers for shard {}. (last_shard_fid = {:?})",
        worker_channels.len(),
        shard_id,
        last_shard_fid
    );
    for channel in worker_channels {
        let _ = channel.send(WorkerMessage::Stop(StopSignal)).await;
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

    // Check if we completed normally or were shut down
    let completed_normally = all_work_sent && pending_work.is_empty();
    if completed_normally {
        info!(
            "All parallel replication threads completed successfully for shard {}",
            shard_id
        );
    } else {
        info!(
            "Shard {} replication stopped due to shutdown signal. Completed ranges: {:?}",
            shard_id, completed_ranges
        );
    }

    db.close();
    info!("DB closed for shard {}", shard_id);

    if last_shard_fid.is_some() {
        return Ok(WorkUnitResponse::NoMoreFIDs(last_shard_fid.unwrap()));
    } else {
        return Ok(WorkUnitResponse::CompletedRange);
    }
}

/// Create worker threads for shard replication
async fn create_workers_for_shard(
    num_workers: usize,
    network: crate::proto::FarcasterNetwork,
    shard_id: u32,
    trie_branching_factor: u32,
    target_height: u64,
    statsd_client: StatsdClientWrapper,
    db: Arc<RocksDB>,
) -> Result<Vec<mpsc::Sender<WorkerMessage>>, BootstrapError> {
    let mut worker_channels = Vec::new();

    let store_opts = StoreOptions {
        conflict_free: true, // All messages will be free of conflicts, since these are from a already-merged snapshot
        save_hub_events: false, // No need for HubEvents, which are emitted only from "live" nodes
    };

    for worker_id in 0..num_workers {
        let (worker_tx, mut worker_rx) = mpsc::channel::<WorkerMessage>(100);
        let network = network.clone();
        let statsd_client = statsd_client.clone();

        let trie = merkle_trie::MerkleTrie::new(trie_branching_factor).unwrap();
        let mut thread_engine = ShardEngine::new_with_opts(
            db.clone(),
            network,
            trie,
            shard_id,
            StoreLimits::default(),
            statsd_client,
            256,
            None,
            None,
            None,
            store_opts.clone(),
        )
        .await
        .map_err(|e| BootstrapError::DatabaseError(e.to_string()))?;

        // Spawn worker thread
        tokio::spawn(async move {
            debug!("Worker {} started for shard {}", worker_id, shard_id);

            // Process work items
            while let Some(message) = worker_rx.recv().await {
                match message {
                    WorkerMessage::Work(work_unit) => {
                        debug!(
                            "Worker {} processing FID range {} to {}",
                            worker_id, work_unit.range.fid_start, work_unit.range.fid_end
                        );

                        // Connect to peer for this work unit
                        let mut thread_client =
                            match ReplicationServiceClient::connect(work_unit.peer_address.clone())
                                .await
                            {
                                Ok(client) => client,
                                Err(e) => {
                                    let _ = work_unit.response_tx.send(Err(
                                        BootstrapError::PeerConnectionError(e.to_string()),
                                    ));
                                    continue;
                                }
                            };

                        // Call replay_shard_transactions with the work unit's range
                        let result = replay_shard_transactions(
                            &mut thread_client,
                            &mut thread_engine,
                            work_unit.range.shard_id,
                            target_height,
                            &work_unit.range,
                            work_unit.db_commit_tx,
                        )
                        .await;

                        // Send result back
                        let _ = work_unit.response_tx.send(result);
                    }
                    WorkerMessage::Stop(_) => {
                        info!("Worker {} received stop signal, exiting", worker_id);
                        break;
                    }
                }
            }

            debug!("Worker {} finished for shard {}", worker_id, shard_id);
        });

        worker_channels.push(worker_tx);
    }

    Ok(worker_channels)
}

fn make_replication_bootstrap_status_key(shard_id: u32) -> Vec<u8> {
    vec![RootPrefix::ReplicationStatusKey as u8, shard_id as u8]
}

/// Update the completed ranges
async fn update_completed_ranges(
    db: &Arc<RocksDB>,
    completed_ranges: CompletedRanges,
) -> Result<(), BootstrapError> {
    if !completed_ranges.ranges.is_empty() {
        if let Some(first_range) = completed_ranges.ranges.first() {
            let shard_id = first_range.shard_id;

            db.put(
                &make_replication_bootstrap_status_key(shard_id),
                &completed_ranges.to_vec(),
            )
            .map_err(|e| BootstrapError::DatabaseError(e.to_string()))?;
        }
    }

    Ok(())
}

/// Read the completed ranges
async fn read_completed_ranges(
    db: &Arc<RocksDB>,
    shard_id: u32,
) -> Result<CompletedRanges, BootstrapError> {
    let key = make_replication_bootstrap_status_key(shard_id);
    match db.get(&key) {
        Ok(Some(bytes)) => match CompletedRanges::from_vec(bytes) {
            Ok(ranges) => Ok(ranges),
            Err(e) => Err(BootstrapError::DatabaseError(format!(
                "Failed to deserialize CompletedRanges: {}",
                e
            ))),
        },
        _ => Ok(CompletedRanges::new()),
    }
}

// Helper function to check if the account roots match
fn check_account_roots(
    engine: &mut ShardEngine,
    fid: u64,
    expected_root: &[u8],
) -> Result<(), BootstrapError> {
    // Get the account root from the store
    let stores_account_root = engine.get_stores().trie.get_hash(
        &engine.db,
        &mut RocksDbTransactionBatch::new(),
        &TrieKey::for_fid(fid),
    );

    if stores_account_root != expected_root {
        Err(BootstrapError::AccountRootMismatch(format!(
            "Account root mismatch for FID {}: expected {}, got {}",
            fid,
            hex::encode(expected_root),
            hex::encode(stores_account_root)
        )))
    } else {
        info!(
            "Account root for FID {} matches: {}",
            fid,
            hex::encode(expected_root)
        );
        Ok(())
    }
}

async fn commit_pending_fids(
    engine: &mut ShardEngine,
    db_commit_tx: &mpsc::UnboundedSender<DbCommitRequest>,
    txn_batch: RocksDbTransactionBatch,
    num_messages: u64,
    fids_to_check: Vec<(u64, Vec<u8>)>,
) -> Result<(), BootstrapError> {
    // Send the transaction batch to the DB writer thread for commit
    let (response_tx, response_rx) = oneshot::channel();
    if let Err(_) = db_commit_tx.send(DbCommitRequest {
        txn_batch,
        num_messages,
        response_tx,
    }) {
        return Err(BootstrapError::DatabaseError(format!(
            "DB writer thread unavailable. Trying to send transaction batch for FIDs: {:?}",
            fids_to_check
        )));
    }

    // Wait for the commit to complete
    let commit_result = response_rx.await.map_err(|_| {
        BootstrapError::DatabaseError("DB writer response channel closed".to_string())
    })?;

    // Handle the commit result
    commit_result?;

    // Reload the trie after commit
    engine
        .get_stores()
        .trie
        .reload(&engine.db)
        .map_err(|e| BootstrapError::DatabaseError(format!("Failed to reload trie: {}", e)))?;

    // Check the account roots for the processed FIDs
    for (fid, account_root) in fids_to_check {
        check_account_roots(engine, fid, &account_root)?;
    }

    Ok(())
}

/// Replay transactions for a specific shard from the replication service
async fn replay_shard_transactions(
    client: &mut ReplicationServiceClient<Channel>,
    engine: &mut ShardEngine,
    shard_id: u32,
    height: u64,
    range: &FidRange,
    db_commit_tx: mpsc::UnboundedSender<DbCommitRequest>,
) -> Result<WorkUnitResponse, BootstrapError> {
    let mut page_token: Option<Vec<u8>> = None;
    let mut total_transactions = 0;
    let mut last_fid = 0;
    let mut last_fid_account_root = vec![];

    // We'll send the server the order in which we want the messages to be sent, so the engine can process them correctly
    let sys_messages_map = ShardEngine::system_message_sort_order_map();
    let system_message_types = Some(SortOrderTypes {
        sort_order: (0..sys_messages_map.len())
            .map(|i| sys_messages_map.iter().find(|(_, &v)| v == i).unwrap().0)
            .map(|m| *m as u32) // Convert to u32 to send over wire
            .collect::<Vec<_>>(),
    });
    let user_messages_map = ShardEngine::user_message_sort_order_map();
    let user_message_types = Some(SortOrderTypes {
        sort_order: (0..user_messages_map.len())
            .map(|i| user_messages_map.iter().find(|(_, &v)| v == i).unwrap().0)
            .map(|m| *m as u32) // Convert to u32 to send over wire
            .collect::<Vec<_>>(),
    });

    // TXN batch to hold all changes
    let mut txn_batch = RocksDbTransactionBatch::new();
    // FIDs to check for account root consistency that were a part of this transaction
    let mut fids_to_check = vec![];
    // Num messages in this transaction
    let mut num_messages = 0;

    let mut all_fids_exhausted = false;

    'pagingloop: loop {
        let cursor = if let Some(token) = &page_token {
            Some(Cursor::PageToken(token.clone()))
        } else {
            Some(Cursor::StartFid(range.fid_start))
        };

        let system_message_types = system_message_types.clone();
        let user_message_types = user_message_types.clone();

        let request = GetShardTransactionsRequest {
            shard_id,
            height,
            cursor,
            system_message_types,
            user_message_types,
        };

        let req_start = std::time::Instant::now();
        match client.get_shard_transactions(request).await {
            Ok(response) => {
                let response = response.into_inner();
                let transactions = response.transactions;

                if transactions.is_empty() {
                    info!("No more transactions to replay for shard {}", shard_id);
                    break;
                }

                // Replay each transaction
                for transaction in &transactions {
                    if transaction.fid > last_fid && last_fid != 0 {
                        fids_to_check.push((last_fid, last_fid_account_root.clone()));
                    }

                    if range.fid_end > 0 && transaction.fid > range.fid_end {
                        // Stop replaying if we reach the end of the range
                        break 'pagingloop;
                    }

                    replay_transaction(engine, transaction, &mut txn_batch).await?;

                    total_transactions += 1;
                    num_messages += transaction.user_messages.len() as u64
                        + transaction.system_messages.len() as u64;

                    last_fid = transaction.fid;
                    last_fid_account_root = transaction.account_root.clone();
                }

                // Check if there are more pages
                if response.next_page_token.is_none()
                    || response.next_page_token.as_ref().unwrap().is_empty()
                {
                    info!("No next page token for shard {}", shard_id);
                    all_fids_exhausted = true;
                    fids_to_check.push((last_fid, last_fid_account_root.clone()));
                    break 'pagingloop;
                } else {
                    page_token = response.next_page_token;
                }
            }
            Err(e) => {
                let req_time = req_start.elapsed();
                error!(
                    client_call_time_ms = format!("{:.2}", req_time.as_millis()),
                    "Failed to get transactions for shard {}: {}", shard_id, e
                );
                return Err(BootstrapError::TransactionReplayError(e.to_string()));
            }
        }

        if num_messages > 20_000 {
            commit_pending_fids(
                engine,
                &db_commit_tx,
                txn_batch,
                num_messages,
                fids_to_check,
            )
            .await?;

            // Clear everything
            txn_batch = RocksDbTransactionBatch::new();
            fids_to_check = vec![];
            num_messages = 0;
        }
    }

    // Commit the last set
    commit_pending_fids(
        engine,
        &db_commit_tx,
        txn_batch,
        num_messages,
        fids_to_check,
    )
    .await?;

    info!(
        "Replayed {} total transactions for shard {}",
        total_transactions, shard_id
    );

    if all_fids_exhausted {
        let mut fids = HashMap::new();
        fids.insert(shard_id, last_fid);
        return Ok(WorkUnitResponse::NoMoreFIDs(fids));
    } else {
        return Ok(WorkUnitResponse::Stopped);
    }
}

/// Replay a single transaction using the ShardEngine
async fn replay_transaction(
    engine: &mut ShardEngine,
    transaction: &Transaction,
    mut txn_batch: &mut RocksDbTransactionBatch,
) -> Result<(), BootstrapError> {
    let trie_ctx = merkle_trie::Context::new();
    let timestamp = FarcasterTime::current();
    let version = EngineVersion::current(engine.network);

    // Set the block height to 0 (by resetting the event id) for bootstrap. This makes the hub events
    // get proper IDs.
    engine.reset_event_id();

    match engine.replay_snapchain_txn(
        &trie_ctx,
        transaction,
        &mut txn_batch,
        ProposalSource::Replication,
        version,
        &timestamp,
    ) {
        Ok((_account_root, _events, validation_errors)) => {
            if !validation_errors.is_empty() {
                info!(
                    "Transaction for FID {} had {} validation errors during bootstrap replay",
                    transaction.fid,
                    validation_errors.len()
                );
            }

            // TODO: We don't generate HubEvents during bootstrap, so we skip that part, correct?

            Ok(())
        }
        Err(e) => Err(BootstrapError::TransactionReplayError(e.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_completed_ranges_basic() {
        let mut ranges = CompletedRanges::new();

        // Add a range
        ranges.add_range(FidRange {
            shard_id: 1,
            fid_start: 1,
            fid_end: 10,
        });
        assert_eq!(
            ranges.get_highest_consecutive_fid(1),
            FidRange {
                shard_id: 1,
                fid_start: 1,
                fid_end: 10
            }
        );

        // Add another consecutive range
        ranges.add_range(FidRange {
            shard_id: 1,
            fid_start: 11,
            fid_end: 20,
        });
        assert_eq!(
            ranges.get_highest_consecutive_fid(1),
            FidRange {
                shard_id: 1,
                fid_start: 1,
                fid_end: 20
            }
        );
        assert_eq!(ranges.ranges.len(), 1); // Should be merged

        // Add a non-consecutive range
        ranges.add_range(FidRange {
            shard_id: 1,
            fid_start: 30,
            fid_end: 40,
        });
        assert_eq!(
            ranges.get_highest_consecutive_fid(1),
            FidRange {
                shard_id: 1,
                fid_start: 1,
                fid_end: 20
            }
        ); // Still 20
        assert_eq!(ranges.ranges.len(), 2); // Should not be merged
    }

    #[test]
    fn test_completed_ranges_out_of_order() {
        let mut ranges = CompletedRanges::new();

        // Add ranges out of order
        ranges.add_range(FidRange {
            shard_id: 1,
            fid_start: 21,
            fid_end: 30,
        });
        assert_eq!(
            ranges.get_highest_consecutive_fid(1),
            FidRange {
                shard_id: 1,
                fid_start: 0,
                fid_end: 0
            }
        ); // Doesn't start from 1

        ranges.add_range(FidRange {
            shard_id: 1,
            fid_start: 11,
            fid_end: 20,
        });
        assert_eq!(
            ranges.get_highest_consecutive_fid(1),
            FidRange {
                shard_id: 1,
                fid_start: 0,
                fid_end: 0
            }
        ); // Still doesn't start from 1

        ranges.add_range(FidRange {
            shard_id: 1,
            fid_start: 1,
            fid_end: 10,
        });
        assert_eq!(
            ranges.get_highest_consecutive_fid(1),
            FidRange {
                shard_id: 1,
                fid_start: 1,
                fid_end: 30
            }
        ); // Now all ranges are consecutive from 1
        assert_eq!(ranges.ranges.len(), 1); // All should be merged
    }

    #[test]
    fn test_completed_ranges_gap_filling() {
        let mut ranges = CompletedRanges::new();

        // Add first and third ranges
        ranges.add_range(FidRange {
            shard_id: 1,
            fid_start: 1,
            fid_end: 10,
        });
        ranges.add_range(FidRange {
            shard_id: 1,
            fid_start: 21,
            fid_end: 30,
        });
        assert_eq!(
            ranges.get_highest_consecutive_fid(1),
            FidRange {
                shard_id: 1,
                fid_start: 1,
                fid_end: 10
            }
        );
        assert_eq!(ranges.ranges.len(), 2);

        // Fill the gap
        ranges.add_range(FidRange {
            shard_id: 1,
            fid_start: 11,
            fid_end: 20,
        });
        assert_eq!(
            ranges.get_highest_consecutive_fid(1),
            FidRange {
                shard_id: 1,
                fid_start: 1,
                fid_end: 30
            }
        );
        assert_eq!(ranges.ranges.len(), 1); // All merged
    }

    #[test]
    fn test_completed_ranges_resume_logic() {
        let mut ranges = CompletedRanges::new();

        // Simulate some completed work with gaps
        ranges.add_range(FidRange {
            shard_id: 1,
            fid_start: 1,
            fid_end: 10,
        });
        ranges.add_range(FidRange {
            shard_id: 1,
            fid_start: 11,
            fid_end: 20,
        });
        ranges.add_range(FidRange {
            shard_id: 1,
            fid_start: 31,
            fid_end: 40,
        }); // Gap at 21-30

        // Should be able to resume from 21 (highest consecutive is 20)
        let highest_consecutive = ranges.get_highest_consecutive_fid(1);
        assert_eq!(
            highest_consecutive,
            FidRange {
                shard_id: 1,
                fid_start: 1,
                fid_end: 20
            }
        );

        // When we fill the gap, it should update correctly
        ranges.add_range(FidRange {
            shard_id: 1,
            fid_start: 21,
            fid_end: 30,
        });
        assert_eq!(
            ranges.get_highest_consecutive_fid(1),
            FidRange {
                shard_id: 1,
                fid_start: 1,
                fid_end: 40
            }
        );
        assert_eq!(ranges.ranges.len(), 1); // All merged
    }

    #[test]
    fn test_completed_ranges_serialization() {
        let mut ranges = CompletedRanges::new();

        // Add some ranges
        ranges.add_range(FidRange {
            shard_id: 1,
            fid_start: 1,
            fid_end: 10,
        });
        ranges.add_range(FidRange {
            shard_id: 1,
            fid_start: 20,
            fid_end: 30,
        });

        // Serialize and deserialize
        let serialized = ranges.to_vec();
        let deserialized = CompletedRanges::from_vec(serialized).unwrap();

        // Check that deserialized ranges match original
        assert_eq!(ranges.ranges.len(), deserialized.ranges.len());
        for (original, deserialized) in ranges.ranges.iter().zip(deserialized.ranges.iter()) {
            assert_eq!(original.shard_id, deserialized.shard_id);
            assert_eq!(original.fid_start, deserialized.fid_start);
            assert_eq!(original.fid_end, deserialized.fid_end);
        }
    }

    #[test]
    fn test_completed_ranges_is_range_completed() {
        let mut ranges = CompletedRanges::new();

        // Add some completed ranges
        ranges.add_range(FidRange {
            shard_id: 1,
            fid_start: 1,
            fid_end: 10,
        });
        ranges.add_range(FidRange {
            shard_id: 1,
            fid_start: 20,
            fid_end: 30,
        });

        // Test ranges that should be completed
        assert!(ranges.is_range_completed(&FidRange {
            shard_id: 1,
            fid_start: 1,
            fid_end: 5,
        }));
        assert!(ranges.is_range_completed(&FidRange {
            shard_id: 1,
            fid_start: 25,
            fid_end: 30,
        }));
        assert!(ranges.is_range_completed(&FidRange {
            shard_id: 1,
            fid_start: 1,
            fid_end: 10,
        }));

        // Test ranges that should not be completed
        assert!(!ranges.is_range_completed(&FidRange {
            shard_id: 1,
            fid_start: 11,
            fid_end: 15,
        }));
        assert!(!ranges.is_range_completed(&FidRange {
            shard_id: 1,
            fid_start: 5,
            fid_end: 15,
        }));
        assert!(!ranges.is_range_completed(&FidRange {
            shard_id: 2, // Different shard
            fid_start: 1,
            fid_end: 10,
        }));
    }

    #[test]
    fn test_completed_ranges_empty_serialization() {
        let ranges = CompletedRanges::new();
        let serialized = ranges.to_vec();
        let deserialized = CompletedRanges::from_vec(serialized).unwrap();

        assert_eq!(deserialized.ranges.len(), 0);
    }
}
