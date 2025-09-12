use crate::bootstrap::error::BootstrapError;
use crate::bootstrap::replication_rpc_client::RpcClientsManager;
use crate::cfg::Config;
use crate::core::validations;
use crate::core::validations::message::validate_message_hash;
use crate::proto::shard_trie_entry_with_message::TrieMessage;
use crate::proto::{self, ReplicationTriePartStatus, ShardSnapshotMetadata};
use crate::storage::store::node_local_state::LocalStateStore;
use crate::storage::{
    constants::RootPrefix,
    db::{PageOptions, RocksDB, RocksDbTransactionBatch},
    store::{
        account::StoreOptions,
        block::BlockStore,
        engine::{EngineError, ShardEngine},
        shard::ShardStore,
        stores::StoreLimits,
    },
    trie::merkle_trie::{self, TrieKey},
    util::increment_vec_u8,
};
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use ed25519_dalek::{Signature, VerifyingKey};
use futures::future;
use prost::Message as _;
use std::{
    collections::HashMap,
    net::UdpSocket,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    u64,
};
use tokio::signal::ctrl_c;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tracing::{debug, error, info};

#[derive(Clone, Debug, PartialEq)]
pub enum WorkUnitResponse {
    None = 0,
    // Still working on it
    Working = 1,
    // User manually stopped it (CTRL+C). Will resume after snapchain is restarted
    Stopped = 2,
    // DB Session expired (i.e., batch is full)
    DbStopped = 3,
    // Bootstrap was finished, but there were some errors. Should continue with startup
    PartiallyComplete = 4,
    // Successfully finished, ready to continue with startup
    Finished = 5,
}

/// Request to commit a transaction batch to the database
enum DbCommitResponse {
    Continue,
    Stop,
}

struct DbCommitRequest {
    txn_batch: RocksDbTransactionBatch,
    num_messages: usize,
    response_tx: oneshot::Sender<Result<DbCommitResponse, BootstrapError>>,
}

struct TrieInsertRequest {
    trie_keys: Vec<Vec<u8>>,
    response_tx: oneshot::Sender<Result<RocksDbTransactionBatch, BootstrapError>>,
}

#[derive(Clone)]
struct WorkUnit {
    current_status: proto::ReplicationTriePartStatus,
    rpc_client_manager: Arc<RpcClientsManager>,
    shutdown_signal: Arc<AtomicBool>,
    db_commit_tx: mpsc::Sender<DbCommitRequest>,
    trie_insert_tx: mpsc::Sender<TrieInsertRequest>,
}

#[derive(Clone)]
pub struct ReplicatorBootstrap {
    shutdown: Arc<AtomicBool>,
    network: crate::proto::FarcasterNetwork,
    statsd_client: StatsdClientWrapper,
    shard_ids: Vec<u32>,
    rocksdb_dir: String,
    initial_peer_list: Vec<String>,
}

impl ReplicatorBootstrap {
    pub fn new(app_config: &Config) -> Self {
        let (statsd_host, statsd_port) = match app_config.statsd.addr.split_once(':') {
            Some((host, port)) => {
                if host.is_empty() || port.is_empty() {
                    panic!("statsd address must be in the format host:port");
                }
                (host.to_string(), port.parse::<u16>().unwrap())
            }
            None => panic!("invalid statsd address: {}", app_config.statsd.addr),
        };

        let host = (statsd_host, statsd_port);
        let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
        let sink = cadence::UdpMetricSink::from(host, socket).unwrap();
        let statsd_client_inner =
            cadence::StatsdClient::builder(app_config.statsd.prefix.as_str(), sink).build();
        let statsd_client =
            StatsdClientWrapper::new(statsd_client_inner, app_config.statsd.use_tags);

        // Get the list of peers from the config
        let initial_peer_list = app_config.snapshot.replication_peers.clone();

        Self {
            shutdown: Arc::new(AtomicBool::new(false)),
            network: app_config.fc_network,
            statsd_client,
            shard_ids: app_config.consensus.shard_ids.clone(),
            rocksdb_dir: app_config.rocksdb_dir.clone(),
            initial_peer_list,
        }
    }

    fn get_snapshot_rocksdb_dir(&self) -> String {
        format!("{}.snapshot", self.rocksdb_dir)
    }

    fn get_or_gen_vts_status(
        db: &RocksDB,
        shard_id: u32,
        virtual_trie_shard: u32,
        metadata: &ShardSnapshotMetadata,
    ) -> Result<ReplicationTriePartStatus, BootstrapError> {
        let status =
            LocalStateStore::read_work_unit(db, shard_id, virtual_trie_shard).map_err(|e| {
                BootstrapError::DatabaseError(format!("Failed to read work unit: {:?}", e))
            })?;

        // If there's no status for this shard/vts, create an empty Status
        match status {
            Some(status) => {
                debug!("Read existing status {:?}", status);
                Ok(status)
            }
            None => {
                debug!(
                    "Creating new status for {} height {} vts {}",
                    shard_id, metadata.height, virtual_trie_shard
                );
                Ok(ReplicationTriePartStatus {
                    shard_id,
                    height: metadata.height,
                    virtual_trie_shard,
                    last_response: 0,
                    last_fid: None,
                    next_page_token: None,
                })
            }
        }
    }

    /// Bootstrap a node from replication instead of snapshot download
    pub async fn bootstrap_using_replication(&self) -> Result<WorkUnitResponse, BootstrapError> {
        info!("Starting replication-based bootstrap");

        let peer_addresses = self.initial_peer_list.clone();
        // If no peers are configured, we can't proceed, so error out
        if peer_addresses.is_empty() {
            return Err(BootstrapError::GenericError(
                "No replication peers configured".into(),
            ));
        }

        // Initialize databases and replay transactions for each shard
        // Note: We're not replicating shard 0 (block shard) for now
        let shard_ids = self.shard_ids.clone();

        // For each shard, create a new RPC Client object, seeded by the the first peer
        let rpc_client_managers = future::join_all(
            shard_ids
                .iter()
                .map(|shard_id| RpcClientsManager::new(peer_addresses[0].clone(), *shard_id)),
        )
        .await
        .into_iter()
        .zip(shard_ids.iter())
        .map(|(res, shard_id)| res.map(|mgr| (*shard_id, Arc::new(mgr))))
        .collect::<Result<HashMap<_, _>, BootstrapError>>()?;

        // Add the rest of the peers into all the rpc client managers
        for (_, rpc_client_manager) in rpc_client_managers.iter() {
            for peer in peer_addresses.iter().skip(1) {
                rpc_client_manager.add_new_peer(peer.clone());
            }
        }

        // Create tasks for each shard to run in parallel
        let mut shard_tasks = JoinSet::new();

        for shard_id in shard_ids {
            let rpc_client_manager = match rpc_client_managers.get(&shard_id) {
                None => {
                    return Err(BootstrapError::MetadataFetchError(format!(
                        "No RPC client found for shard {}, couldn't fetch metadata",
                        shard_id
                    )))
                }
                Some(mgr) => mgr.clone(),
            };
            let shard_metadata = rpc_client_manager.get_metadata();
            let target_height = shard_metadata.height;

            if target_height == 0 {
                return Err(BootstrapError::GenericError(format!(
                    "No valid snapshot found for shard {}. Skipping bootstrap.",
                    shard_id
                )));
            }

            let rocksdb_dir = self.get_snapshot_rocksdb_dir();
            info!(
                "Bootstrapping shard {} at height {}",
                shard_id, target_height
            );

            // Spawn a task for this shard
            let shutdown_signal = self.shutdown.clone();
            let metadata = shard_metadata.clone();

            let self_clone = self.clone();
            shard_tasks.spawn(async move {
                // Replay transactions for this shard
                self_clone
                    .start_shard_replication_main(
                        rpc_client_manager,
                        shard_id,
                        &rocksdb_dir,
                        metadata,
                        shutdown_signal,
                    )
                    .await
            });
        }

        // Wait for either all shard tasks to complete or a shutdown signal
        info!("Waiting for shutdown signal or shard tasks to complete");

        let mut results = Vec::new();

        let shutdown_and_drain_tasks_fn =
            async |shard_tasks: &mut JoinSet<Result<WorkUnitResponse, BootstrapError>>| {
                self.shutdown.store(true, Ordering::SeqCst);
                info!("Waiting for shard tasks to shut down gracefully...");
                while let Some(res) = shard_tasks.join_next().await {
                    // Ignore results since we're shutting down
                    let _ = res;
                }

                for (_, rpc_client_manager) in rpc_client_managers.iter() {
                    rpc_client_manager.close().await;
                }
                info!("All shard tasks have been shut down.");
            };

        loop {
            if shard_tasks.is_empty() {
                // All tasks completed successfully
                // Write the final metadata from the server into the new DB.
                self.write_final_metadata_to_db(
                    &rpc_client_managers,
                    &self.get_snapshot_rocksdb_dir(),
                )
                .await?;

                info!("Replication bootstrap finished. Promoting snapshot to main DB.");

                // Rename the completed snapshot DB to the main DB name
                if let Err(e) = std::fs::rename(&self.get_snapshot_rocksdb_dir(), &self.rocksdb_dir)
                {
                    error!("FATAL: Failed to rename snapshot DB: {}. Please do it manually or clear the DB directory.", e);
                    return Ok(WorkUnitResponse::PartiallyComplete);
                }

                return Ok(WorkUnitResponse::Finished);
            }

            tokio::select! {
                Some(res) = shard_tasks.join_next() => {
                    match res {
                        Ok(Ok(work_response)) => {
                            results.push(work_response);
                            info!("Shard task completed successfully");
                        }
                        Ok(Err(e)) => {
                            error!("Shard task failed: {}", e);
                            // Shutdown all remaining tasks as well
                            shutdown_and_drain_tasks_fn(&mut shard_tasks).await;

                            return Err(e);
                        }
                        Err(e) => {
                            error!("Shard task join error: {}", e);
                            return Err(BootstrapError::TransactionReplayError(format!(
                                "Shard task join error: {}",
                                e
                            )));
                        }
                    }
                }
                _ = ctrl_c() => {
                    info!("Shutdown signal received, stopping all shard replication tasks");
                    shutdown_and_drain_tasks_fn(&mut shard_tasks).await;

                    return Ok(WorkUnitResponse::Stopped);
                }
            }
        }
    }

    async fn start_shard_replication_main(
        &self,
        rpc_client_manager: Arc<RpcClientsManager>,
        shard_id: u32,
        rocksdb_dir: &str,
        metadata: ShardSnapshotMetadata,
        shutdown_signal: Arc<AtomicBool>,
    ) -> Result<WorkUnitResponse, BootstrapError> {
        loop {
            if shutdown_signal.load(Ordering::SeqCst) {
                info!("Shutdown signal received, stopping shard replication");
                return Ok(WorkUnitResponse::Stopped);
            }

            let result = self
                .start_shard_replication_batch(
                    rpc_client_manager.clone(),
                    shard_id,
                    rocksdb_dir,
                    metadata.clone(),
                    shutdown_signal.clone(),
                )
                .await?;

            match result {
                WorkUnitResponse::DbStopped => {
                    info!(
                        "DB session expired, restarting shard replication for shard {}",
                        shard_id
                    );
                    continue;
                }
                WorkUnitResponse::Stopped => {
                    info!("Shard replication stopped for shard {}", shard_id);
                    return Ok(WorkUnitResponse::Stopped);
                }
                WorkUnitResponse::PartiallyComplete | WorkUnitResponse::Finished => {
                    info!("Shard replication completed for shard {}", shard_id);

                    // PostProcess the trie
                    self.verify_shard_roots(rocksdb_dir, shard_id, metadata)
                        .await?;

                    // Clean up the work unit statuses and the account root hashes that were stored in the DB
                    self.cleanup_db(rocksdb_dir, shard_id).await?;

                    return Ok(result);
                }
                WorkUnitResponse::Working | WorkUnitResponse::None => {
                    let reason = format!("Shard {} work stopped for an unknown reason", shard_id);
                    return Err(BootstrapError::GenericError(reason));
                }
            }
        }
    }

    /// Clean up the work unit statuses and account root hashes stored in the DB for a shard
    async fn cleanup_db(&self, rocksdb_dir: &str, shard_id: u32) -> Result<(), BootstrapError> {
        info!("Cleaning up bootstrap data for shard {}", shard_id);

        let db = RocksDB::open_bulk_write_shard_db(rocksdb_dir, shard_id);

        // Create the prefix for all bootstrap data for this shard
        let start_prefix = vec![RootPrefix::ReplicationBootstrapStatus as u8, shard_id as u8];
        let stop_prefix = increment_vec_u8(&start_prefix);

        // Delete all keys with this prefix
        let mut deleted_count = 0u64;
        db.for_each_iterator_by_prefix(
            Some(start_prefix),
            Some(stop_prefix),
            &PageOptions::default(),
            |key, _value| {
                db.del(key)?;
                deleted_count += 1;
                return Ok(false); // continue
            },
        )
        .map_err(|e| {
            BootstrapError::DatabaseError(format!(
                "Failed to clean up bootstrap data for shard {}: {:?}",
                shard_id, e
            ))
        })?;

        db.close();

        info!(
            "Successfully cleaned up bootstrap data for shard {}, deleting {} keys",
            shard_id, deleted_count
        );
        Ok(())
    }

    /// Create a dedicated thread to write transactions into the DB. The RocksDB we use is multi threaded,
    /// but writing to the DB directly from multiple threads creates a lot of lock contention, and we don't want to
    /// write a lot of retry logic. Plus, by keeping the DB writer on one thread, we can create "backpressure" to all
    /// the other threads, slowing down them as needed without overwhelming RocksDB
    fn create_db_writer_task(
        &self,
        db: Arc<RocksDB>,
        shard_id: u32,
        mut db_commit_rx: mpsc::Receiver<DbCommitRequest>,
    ) -> tokio::task::JoinHandle<()> {
        // Important Note: We will write these many messages to the DB in a single "batch" (See start_shard_replication_batch)
        // The DB type we use is optimized for bulk writing, but unfortunately doesn't allow us to compact or flush the keys.
        // They are flushed and the DB compacted only when it is closed.
        // If we do nothing and write continuously, the writes slow down A LOT, bringing the replication to a crawl.
        // So, as a work around, every 5M messages, we close and reopen the DB. This flushes and finializes all the keys written
        // so far, saves progress, and creates a proper "checkpoint" for us to resume, in case things go wrong or the user
        // presses CTRL+C etc...
        let max_messages_in_write_session = 5_000_000;

        tokio::spawn(async move {
            info!("DB writer thread started for shard {}", shard_id);
            let mut total_messages = 0;
            let total_start_time = std::time::Instant::now();

            while let Some(commit_request) = db_commit_rx.recv().await {
                let start_time = std::time::Instant::now();
                total_messages += commit_request.num_messages;

                let result = db.commit(commit_request.txn_batch);
                let elapsed_time = start_time.elapsed();
                let total_elapsed = total_start_time.elapsed();
                info!(
                    "DB writer({}) - Commit took {:?}. Writing at {:.1}k msg/sec {:.2}M total messages",
                    shard_id,
                    elapsed_time,
                    total_messages as f64 / 1000.0 / total_elapsed.as_secs_f64(),
                    total_messages as f64 / 1_000_000.0
                );

                let response = match result {
                    Ok(()) => {
                        // If we've exceeded the total number of messages in the write session, stop the writer
                        // and allow the DB to reload
                        if total_messages > max_messages_in_write_session {
                            Ok(DbCommitResponse::Stop)
                        } else {
                            Ok(DbCommitResponse::Continue)
                        }
                    }
                    Err(e) => {
                        error!("DB writer - Failed to commit transaction: {}", e);
                        Err(BootstrapError::DatabaseError(format!(
                            "Failed to commit transaction: {}",
                            e
                        )))
                    }
                };

                // Send the result back to the requesting thread
                if let Err(_) = commit_request.response_tx.send(response) {
                    error!("Failed to send DB commit response from DB Writer thread");
                    break;
                }
            }

            info!("DB writer thread finished for shard {}", shard_id);
        })
    }

    /// Create a dedicated thread (per shard) to write to the Trie. This way, we can keep all the trie nodes
    /// in memory (upto the max limit, see the DB_writer thread), which allows us to save ~12 db reads per insert
    /// speeding up the insertion of trie keys. Additionally, we'll always keep the trie hashes correct, since the writes
    /// from multiple parts of the trie are serialized.
    fn create_trie_merges_task(
        &self,
        trie_db: Arc<RocksDB>,
        shard_id: u32,
        mut trie_insert_rx: mpsc::Receiver<TrieInsertRequest>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            debug!("Trie merges thread started for shard {}", shard_id);
            let mut trie = merkle_trie::MerkleTrie::new().unwrap();
            trie.initialize(&trie_db).unwrap();

            while let Some(insert_request) = trie_insert_rx.recv().await {
                let mut txn_batch = RocksDbTransactionBatch::new();
                let trie_keys = insert_request.trie_keys;

                let result = trie.insert(
                    &merkle_trie::Context::new(), // Not tracking trie perf during replication
                    &trie_db,
                    &mut txn_batch,
                    trie_keys
                        .iter()
                        .map(|v| v.as_slice())
                        .collect::<Vec<&[u8]>>(),
                );

                let response = match result {
                    Ok(_) => Ok(txn_batch),
                    Err(e) => Err(BootstrapError::TrieError(e)),
                };

                if insert_request.response_tx.send(response).is_err() {
                    error!("Failed to send response for trie insert request");
                    break;
                }
            }
            debug!("Trie merges thread finished for shard {}", shard_id);
        })
    }

    /// Replicate one "batch" of the shard's tries. This method will spawn upto 4 threads, each working on an independent part of the
    /// trie (by virtual trie shard or "vts"). It will pick up from where it left off, and fetch the next set of pages for each vts
    /// and attempt to merge them. If there's an error, it will stop immediately and return
    /// When the DB writer task instructs to exit, it saves its progress and closes all the threads and returns.
    /// It will also save progress and return if the main loop instructs it to via `shutdown_signal` if for eg. CTRL+C is pressed
    async fn start_shard_replication_batch(
        &self,
        rpc_client_manager: Arc<RpcClientsManager>,
        shard_id: u32,
        rocksdb_dir: &str,
        metadata: ShardSnapshotMetadata,
        shutdown_signal: Arc<AtomicBool>,
    ) -> Result<WorkUnitResponse, BootstrapError> {
        // Fetch upto these many virtual trie shards in parallel. Note that this can't be too big, as we are
        // primarily bottlenecked by DB writing. Increasing this wont help performance much because of the backpressure
        // from the DB writer thread.
        let max_num_parallel_vts = 4;

        let db = RocksDB::open_bulk_write_shard_db(&rocksdb_dir, shard_id);

        // Create a channel for DB commit requests
        let (db_commit_tx, db_commit_rx) = mpsc::channel::<DbCommitRequest>(max_num_parallel_vts);

        // Spawn the DB writer thread
        let db_writer_task = self.create_db_writer_task(db.clone(), shard_id, db_commit_rx);

        // Spawn a thread to handle trie merges
        let (trie_insert_tx, trie_insert_rx) =
            mpsc::channel::<TrieInsertRequest>(max_num_parallel_vts);
        let trie_merges_task = self.create_trie_merges_task(db.clone(), shard_id, trie_insert_rx);

        // Go over each Trie's Virtual Trie Shard (vts) and sync for it.
        let mut vts_results = vec![WorkUnitResponse::None; merkle_trie::TRIE_SHARD_SIZE as usize];
        let mut work_units: Vec<(u32, WorkUnit)> = vec![];

        for vts in 0..merkle_trie::TRIE_SHARD_SIZE {
            // 1. Read the progress for this from the DB
            let current_status = Self::get_or_gen_vts_status(&db, shard_id, vts, &metadata)?;
            if current_status.last_response == WorkUnitResponse::Finished as u32 {
                debug!("Shard {} vts {} already finished, skipping", shard_id, vts);
                vts_results[vts as usize] = WorkUnitResponse::Finished;
                continue;
            }

            // 2. Create a work item for this vts
            let rpc_client_manager = rpc_client_manager.clone();
            let shutdown_signal = shutdown_signal.clone();
            let db_commit_tx = db_commit_tx.clone();
            let trie_insert_tx = trie_insert_tx.clone();

            let work_item = WorkUnit {
                current_status,
                rpc_client_manager,
                shutdown_signal,
                db_commit_tx,
                trie_insert_tx,
            };

            work_units.push((vts, work_item));
        }

        // Now process the collected work units, up to 2 at a time
        let mut join_set: JoinSet<Result<(u32, WorkUnitResponse), BootstrapError>> = JoinSet::new();
        let mut work_index = 0;

        // Setting this to true stops all further work, and returns this function
        let mut stop_more_work = false;

        // Track if there were any errors on any of the threads
        let mut errored = None;

        loop {
            if shutdown_signal.load(Ordering::SeqCst) {
                stop_more_work = true;
            }

            // Spawn up to 2 tasks if not stopped
            while !stop_more_work
                && join_set.len() < max_num_parallel_vts
                && work_index < work_units.len()
            {
                let (vts, work_item) = work_units[work_index].clone();

                let db_clone = db.clone();

                let self_clone = self.clone();
                join_set.spawn(async move {
                    self_clone
                        .process_vts_work_item(work_item, db_clone)
                        .await
                        .map(|res| (vts, res))
                });

                work_index += 1;
            }

            if join_set.is_empty() {
                // No more tasks to process. We are either stopped or finished, either way just exit
                break;
            }

            // Wait for at least one task to complete
            if let Some(join_res) = join_set.join_next().await {
                match join_res {
                    Ok(Ok((vts, result))) => {
                        vts_results[vts as usize] = result.clone();
                        if result == WorkUnitResponse::DbStopped {
                            stop_more_work = true;
                        }
                    }
                    Ok(Err(e)) => {
                        // Propagate errors and stop all work
                        errored = Some(e);
                        stop_more_work = true;
                    }
                    Err(e) => {
                        if !e.is_cancelled() {
                            return Err(BootstrapError::GenericError(format!(
                                "Task join error: {}",
                                e
                            )));
                        }
                    }
                }
            }
        }

        // Drop the original sender and any unfinished work_units so the DB/trie writer threads can exit
        drop(db_commit_tx);
        drop(trie_insert_tx);
        drop(work_units);

        // Wait for both the DB writer and trie merges tasks to finish and propagate any errors
        db_writer_task.await?;
        trie_merges_task.await?;

        db.close();

        // If there was an error, return that
        if errored.is_some() {
            return Err(errored.unwrap());
        }

        // If any of the vts_results is "DbStopped", then return stopped
        if vts_results
            .iter()
            .any(|r| *r == WorkUnitResponse::DbStopped)
        {
            return Ok(WorkUnitResponse::DbStopped);
        }

        // If ALL of the vts results are "finished", return finished
        if vts_results.iter().all(|r| *r == WorkUnitResponse::Finished) {
            return Ok(WorkUnitResponse::Finished);
        }

        // Say we are stopped, and can resume
        return Ok(WorkUnitResponse::Stopped);
    }

    // Actually fetch and merge messages into the DB and trie from the remote node for this shard/vts
    async fn process_vts_work_item(
        &self,
        work_item: WorkUnit,
        db: Arc<RocksDB>,
    ) -> Result<WorkUnitResponse, BootstrapError> {
        let response;

        let shard_id = work_item.current_status.shard_id;
        info!(
            "Processing Shard:{} vts: {}",
            shard_id, work_item.current_status.virtual_trie_shard
        );

        // 1. Create the engine and trie objects
        let mut status = work_item.current_status;

        let store_opts = StoreOptions {
            conflict_free: true, // All messages will be free of conflicts, since these are from a already-merged snapshot
            save_hub_events: false, // No need for HubEvents, which are emitted only from "live" nodes
        };

        let trie = merkle_trie::MerkleTrie::new().unwrap();
        let mut thread_engine = ShardEngine::new_with_opts(
            db.clone(),
            self.network,
            trie,
            shard_id,
            StoreLimits::default(),
            self.statsd_client.clone(),
            256,
            None,
            None,
            None,
            store_opts.clone(),
        )
        .await
        .map_err(|e| BootstrapError::DatabaseError(e.to_string()))?;

        loop {
            let mut last_fid = status.last_fid;
            let mut fids_to_check = vec![];

            // 2. Use the next_page_token to fetch the page of messages from the server
            let messages_page = work_item
                .rpc_client_manager
                .get_shard_transactions(
                    status.virtual_trie_shard as u8,
                    status.next_page_token.clone(),
                )
                .await?;

            let trie_messages = messages_page.trie_messages;
            let mut txn_batch = RocksDbTransactionBatch::new();

            // Write the account roots to the DB, so we may check them later
            for fid_root in &messages_page.fid_account_roots {
                LocalStateStore::write_account_root(
                    &mut txn_batch,
                    status.shard_id,
                    status.virtual_trie_shard as u8,
                    fid_root.fid,
                    fid_root,
                );
            }

            let next_page_token = messages_page.next_page_token;

            // Set the block height to 0 (by resetting the event id) for bootstrap. This makes the hub events
            // get proper IDs.
            thread_engine.reset_event_id();

            let mut trie_keys = vec![];
            let num_messages = trie_messages.len();

            // Validate all the message signatures
            Self::validate_messages_signatures(&trie_messages)?;

            // 3. Start going through all onchain_events and messages for fids > work_item.last_fid
            for trie_message_entry in &trie_messages {
                let trie_key = trie_message_entry.trie_key.clone();
                trie_keys.push(trie_key.clone());

                match thread_engine.replay_replicator_message(&mut txn_batch, trie_message_entry) {
                    Ok(m) => {
                        let generated_trie_key = m.trie_key;
                        let fid = m.fid;

                        if generated_trie_key != trie_key {
                            return Err(BootstrapError::GenericError(format!(
                                "Generated trie key {:?} does not match expected trie key {:?} for {:?}",
                                generated_trie_key, trie_key, trie_message_entry
                            )));
                        }

                        if last_fid.is_none() {
                            last_fid = Some(fid);
                        }

                        if fid > last_fid.unwrap() {
                            fids_to_check.push(last_fid.unwrap());
                            last_fid = Some(fid);
                        }
                    }
                    Err(e) => {
                        // We'll ignore duplicate errors. If the message is already in the DB, then great, we move on.
                        if let EngineError::StoreError(he) = &e {
                            if he.code != "bad_request.duplicate" {
                                error!(
                                    "store/error: Failed to replay message with error: {:?}",
                                    he
                                );
                                return Err(e.into());
                            }
                        } else {
                            error!("other/error: Failed to replay message with error: {:?}", e);
                            return Err(e.into());
                        }
                    }
                };
            }

            // Insert all the trie keys into the trie
            let (response_tx, response_rx) = oneshot::channel();
            work_item
                .trie_insert_tx
                .send(TrieInsertRequest {
                    trie_keys,
                    response_tx,
                })
                .await?;
            let trie_txn_batch = response_rx.await??;

            txn_batch.merge(trie_txn_batch);

            // Verify account root for fid, update last_fid in the DB
            Self::check_fid_roots(
                &thread_engine,
                status.shard_id,
                status.virtual_trie_shard as u8,
                &db,
                &mut txn_batch,
                fids_to_check,
            )?;

            // 5. Now that the account roots match, commit to DB via db_commit_tx

            // First, add the work status to the txn_batch so it gets commited atomically with the work done
            status.last_fid = last_fid;
            status.next_page_token = next_page_token.clone();
            status.last_response = WorkUnitResponse::Working as u32;
            LocalStateStore::write_work_unit(&mut txn_batch, &status);

            // commit via db_commit_tx
            let (response_tx, response_rx) = oneshot::channel();
            work_item
                .db_commit_tx
                .send(DbCommitRequest {
                    txn_batch,
                    num_messages,
                    response_tx,
                })
                .await?;

            // Wait for the commit to complete. The double ?? are to handle both the channel cancelled and the DB writer errors
            let db_response = response_rx.await??;

            // Reload the trie after commit, so we get the correct numbers from the DB. Note that the trie thread has it's own copy of the
            // in memory trie, so we need to reload from the DB here to get the correct numbers
            thread_engine.get_stores().trie.reload(&db)?;

            // Check if shutdown signal is set, if it is, break out
            if work_item.shutdown_signal.load(Ordering::Relaxed) {
                info!(
                    "Shutdown signal received, stopping replication for shard {}, vts {}",
                    shard_id, status.virtual_trie_shard
                );

                response = Ok(WorkUnitResponse::Stopped);
                break;
            }

            if let DbCommitResponse::Stop = db_response {
                response = Ok(WorkUnitResponse::DbStopped);
                break;
            }

            if next_page_token.is_none() {
                // All done.
                // Write to the DB that we're all done
                status.last_response = WorkUnitResponse::Finished as u32;
                let mut txn_batch = RocksDbTransactionBatch::new();
                LocalStateStore::write_work_unit(&mut txn_batch, &status);

                // commit via db_commit_tx
                let (response_tx, response_rx) = oneshot::channel();
                work_item
                    .db_commit_tx
                    .send(DbCommitRequest {
                        txn_batch,
                        num_messages: 0,
                        response_tx,
                    })
                    .await?;

                // Wait for the commit to complete. The double ?? are to handle both the channel cancelled and the DB writer errors
                response_rx.await??;

                response = Ok(WorkUnitResponse::Finished);
                break;
            }
        } // loop to next page from the server

        return response;
    }

    fn check_fid_roots(
        thread_engine: &ShardEngine,
        shard_id: u32,
        virtual_trie_shard: u8,
        db: &RocksDB,
        txn_batch: &mut RocksDbTransactionBatch,
        fids_to_check: Vec<u64>,
    ) -> Result<(), BootstrapError> {
        for fid in &fids_to_check {
            let expected_account = LocalStateStore::read_account_root(
                db,
                shard_id,
                virtual_trie_shard,
                *fid,
                txn_batch,
            )
            .ok_or_else(|| {
                BootstrapError::AccountRootMismatch(format!(
                    "No account root found for fid {}. FIDs are {:?}",
                    fid, fids_to_check
                ))
            })?;

            // First check if the number of messages is correct
            let actual_num_messages_trie = thread_engine.get_stores().trie.get_count(
                &db,
                txn_batch,
                &TrieKey::for_fid(*fid),
            )?;
            let expected_num_messages = expected_account.num_messages;

            if expected_num_messages != actual_num_messages_trie {
                error!(
                    "Message count mismatch for fid  {}/{}/{}. expected {}, got {} in trie",
                    shard_id,
                    virtual_trie_shard,
                    fid,
                    expected_num_messages,
                    actual_num_messages_trie
                );
            }

            let actual_root = thread_engine.get_stores().trie.get_hash(
                &db,
                txn_batch,
                &TrieKey::for_fid(*fid),
            )?;

            let expected_root = expected_account.account_root_hash;
            if expected_root != actual_root {
                return Err(BootstrapError::AccountRootMismatch(format!(
                    "Account root mismatch for fid  {}/{}/{}. expected {}, got {}",
                    shard_id,
                    virtual_trie_shard,
                    fid,
                    hex::encode(expected_root),
                    hex::encode(actual_root),
                )));
            } else {
                debug!(
                    "Account root matched for fid {}/{}/{}: {}",
                    shard_id,
                    virtual_trie_shard,
                    fid,
                    hex::encode(actual_root)
                );
            }
        }

        Ok(())
    }

    // Check that the shard roots match
    async fn verify_shard_roots(
        &self,
        rocksdb_dir: &str,
        shard_id: u32,
        metadata: ShardSnapshotMetadata,
    ) -> Result<(), BootstrapError> {
        info!("Post-processing trie for shard {}.", shard_id);

        let db = RocksDB::open_bulk_write_shard_db(&rocksdb_dir, shard_id);
        let mut trie = merkle_trie::MerkleTrie::new()?;
        trie.initialize(&db)?;

        // Get the trie root and see if it matches
        let expected_shard_root = metadata.shard_chunk.unwrap().header.unwrap().shard_root;
        let trie_root = trie.root_hash().unwrap();

        if expected_shard_root == trie_root {
            info!(
                "Trie root matched for shard {}: {}",
                shard_id,
                hex::encode(&trie_root)
            );
            Ok(())
        } else {
            let expected = hex::encode(&expected_shard_root);
            let actual = hex::encode(&trie_root);
            error!(
                "Trie Root mismatch for shard {}. expected: {:?} actual {:?}",
                shard_id, expected, actual
            );

            return Err(BootstrapError::StateRootMismatch {
                shard_id,
                expected,
                actual,
            });
        }
    }

    // Bulk validate all the signatures. We use the dalek library for batch verification
    fn validate_messages_signatures(
        trie_messages: &Vec<proto::ShardTrieEntryWithMessage>,
    ) -> Result<(), BootstrapError> {
        // We'll collect all info needed to verify signatures as we process messages
        let len = trie_messages.len();
        let mut signatures = Vec::with_capacity(len);
        let mut verifying_keys = Vec::with_capacity(len);
        let mut message_hashes = Vec::with_capacity(len);

        for trie_message_entry in trie_messages.iter() {
            if let Some(TrieMessage::UserMessage(m)) = &trie_message_entry.trie_message {
                // Validate message hash first
                if m.data_bytes.is_some() {
                    validate_message_hash(m.hash_scheme, &m.data_bytes.as_ref().unwrap(), &m.hash)?;
                } else {
                    if m.data.is_none() {
                        return Err(BootstrapError::ValidationError(
                            validations::error::ValidationError::MissingData,
                        ));
                    }

                    validate_message_hash(
                        m.hash_scheme,
                        &m.data.as_ref().unwrap().encode_to_vec(),
                        &m.hash,
                    )?;
                }

                if let (Ok(sig), Ok(key)) = (
                    Signature::from_slice(&m.signature),
                    VerifyingKey::from_bytes(m.signer.as_slice().try_into().unwrap()),
                ) {
                    signatures.push(sig);
                    verifying_keys.push(key);
                    message_hashes.push(m.hash.as_slice());
                } else {
                    error!("Failed to parse signature fid:{} message:{:?}", m.fid(), m);
                    return Err(BootstrapError::ValidationError(
                        validations::error::ValidationError::InvalidSignature,
                    ));
                }
            }
        }

        if !signatures.is_empty() {
            ed25519_dalek::verify_batch(&message_hashes, &signatures, &verifying_keys).map_err(
                |e| {
                    // Since we cannot know which message failed, we will not process any
                    // messages in this batch if the verification fails.
                    error!("Batch signature verification failed: {}", e);
                    BootstrapError::ValidationError(
                        validations::error::ValidationError::InvalidSignature,
                    )
                },
            )?;
        }

        Ok(())
    }

    // At the end of the bootstrap process, we need to write the final metadata to the database
    // which includes the highest ShardChunks for shard-1 and shard-2 and the highest block for shard-0
    async fn write_final_metadata_to_db(
        &self,
        rpc_client_managers: &HashMap<u32, Arc<RpcClientsManager>>,
        rocksdb_dir: &str,
    ) -> Result<(), BootstrapError> {
        info!("Writing final block and shard chunks to database...");

        for (shard_id, rpc_client_manager) in rpc_client_managers {
            let metadata = rpc_client_manager.get_metadata();
            if *shard_id == 0 {
                // Handle Block for Shard 0
                if let Some(block) = &metadata.block {
                    let header = block.header.as_ref().ok_or_else(|| {
                        BootstrapError::MetadataFetchError("Block header is missing".to_string())
                    })?;
                    let height = header.height.as_ref().ok_or_else(|| {
                        BootstrapError::MetadataFetchError("Block height is missing".to_string())
                    })?;
                    info!(
                        "Writing block for shard 0 at height {}",
                        height.block_number
                    );
                    let block_db = RocksDB::open_shard_db(rocksdb_dir, 0);
                    let block_store = BlockStore::new(block_db.clone());
                    block_store
                        .put_block(block)
                        .map_err(|e| BootstrapError::DatabaseError(e.to_string()))?;
                    block_db.close();
                } else {
                    return Err(BootstrapError::MetadataFetchError(format!(
                        "No block found for shard 0. Metadata = {:?}",
                        metadata
                    )));
                }
            } else {
                // Handle ShardChunk for Data Shards
                if let Some(shard_chunk) = &metadata.shard_chunk {
                    let header = shard_chunk.header.as_ref().ok_or_else(|| {
                        BootstrapError::MetadataFetchError(format!(
                            "ShardChunk header is missing for shard {}",
                            shard_id
                        ))
                    })?;
                    let height = header.height.as_ref().ok_or_else(|| {
                        BootstrapError::MetadataFetchError(format!(
                            "ShardChunk height is missing for shard {}",
                            shard_id
                        ))
                    })?;
                    info!(
                        "Writing ShardChunk for shard {} at height {}",
                        shard_id, height.block_number
                    );
                    let shard_db = RocksDB::open_shard_db(rocksdb_dir, *shard_id);
                    let shard_store = ShardStore::new(shard_db.clone(), *shard_id);
                    shard_store
                        .put_shard_chunk(shard_chunk)
                        .map_err(|e| BootstrapError::DatabaseError(e.to_string()))?;
                    shard_db.close();
                } else {
                    return Err(BootstrapError::MetadataFetchError(format!(
                        "No ShardChunk found for shard {}. Metadata = {:?}",
                        shard_id, metadata
                    )));
                }
            }
        }

        info!("Successfully wrote final metadata.");
        Ok(())
    }
}
