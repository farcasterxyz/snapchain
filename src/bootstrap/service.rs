use crate::bootstrap::error::BootstrapError;
use crate::cfg::Config;
use crate::core::validations;
use crate::core::validations::message::validate_message_hash;
use crate::proto::shard_trie_entry_with_message::TrieMessage;
use crate::proto::{
    self, replication_service_client::ReplicationServiceClient, GetShardSnapshotMetadataRequest,
    GetShardTransactionsRequest, ReplicationTriePartStatus, ShardSnapshotMetadata,
};
use crate::storage::trie::errors::TrieError;
use crate::storage::{
    constants::RootPrefix,
    db::{RocksDB, RocksDbTransactionBatch},
    store::{
        account::{make_fid_key, StoreOptions},
        engine::{EngineError, ShardEngine},
        stores::StoreLimits,
    },
    trie::merkle_trie::{self, TrieKey},
};
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use ed25519_dalek::{Signature, VerifyingKey};
use futures::channel::oneshot;
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
use tokio::task::JoinSet;
use tonic::transport::Channel;
use tracing::{debug, error, info};

#[derive(Clone, Debug, PartialEq)]
pub enum WorkUnitResponse {
    None = 0,
    // Still working on it
    Working = 1,
    // User manually stopped it (CTRL+C). Will resume after snapchain is restarted
    Stopped = 2,
    // DB Session expired
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

#[derive(Clone)]
struct WorkUnit {
    current_status: proto::ReplicationTriePartStatus,
    peer_address: String,
    shutdown_signal: Arc<AtomicBool>,
    db_commit_tx: mpsc::UnboundedSender<DbCommitRequest>,
}

#[derive(Clone)]
pub struct ReplicatorBootstrap {
    shutdown: Arc<AtomicBool>,
    network: crate::proto::FarcasterNetwork,
    trie_branching_factor: u32,
    statsd_client: StatsdClientWrapper,
    shard_ids: Vec<u32>,
    rocksdb_dir: String,
}

const WORK_UNIT_POSTFIX: u8 = 1u8;
const FID_ACCOUNT_ROOT_POSTFIX: u8 = 2u8;

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

        Self {
            shutdown: Arc::new(AtomicBool::new(false)),
            network: app_config.fc_network,
            trie_branching_factor: app_config.trie_branching_factor,
            statsd_client,
            shard_ids: app_config.consensus.shard_ids.clone(),
            rocksdb_dir: app_config.rocksdb_dir.clone(),
        }
    }

    fn make_work_unit_key(shard_id: u8, virtual_trie_shard: u8) -> Vec<u8> {
        let mut key = Vec::with_capacity(3);
        key.push(RootPrefix::ReplicationBootstrapStatus as u8);
        key.push(shard_id);
        key.push(virtual_trie_shard);
        key.push(WORK_UNIT_POSTFIX);
        key
    }

    fn make_account_root_key(shard_id: u8, virtual_trie_shard: u8, fid: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(3);
        key.push(RootPrefix::ReplicationBootstrapStatus as u8);
        key.push(shard_id);
        key.push(virtual_trie_shard);
        key.push(FID_ACCOUNT_ROOT_POSTFIX);
        key.extend(make_fid_key(fid));
        key
    }

    fn read_work_unit(
        db: &RocksDB,
        shard_id: u32,
        virtual_trie_shard: u32,
    ) -> Result<Option<proto::ReplicationTriePartStatus>, BootstrapError> {
        let key = Self::make_work_unit_key(shard_id as u8, virtual_trie_shard as u8);
        match db.get(&key) {
            Ok(Some(bytes)) => match proto::ReplicationTriePartStatus::decode(&bytes[..]) {
                Ok(status) => Ok(Some(status)),
                Err(e) => Err(BootstrapError::GenericError(format!(
                    "Failed to decode ReplicationTriePartStatus: {}",
                    e
                ))),
            },
            Ok(None) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn write_work_unit(
        txn_batch: &mut RocksDbTransactionBatch,
        status: &proto::ReplicationTriePartStatus,
    ) {
        let key = Self::make_work_unit_key(status.shard_id as u8, status.virtual_trie_shard as u8);
        let bytes = status.encode_to_vec();

        txn_batch.put(key, bytes)
    }

    fn write_account_root(
        shard_id: u8,
        virtual_trie_shard: u8,
        fid: u64,
        txn_batch: &mut RocksDbTransactionBatch,
        account_root_hash: Vec<u8>,
    ) {
        let key = Self::make_account_root_key(shard_id, virtual_trie_shard, fid);

        txn_batch.put(key, account_root_hash)
    }

    fn read_account_root(
        shard_id: u8,
        virtual_trie_shard: u8,
        fid: u64,
        txn_batch: &mut RocksDbTransactionBatch,
        db: &RocksDB,
    ) -> Option<Vec<u8>> {
        let key = Self::make_account_root_key(shard_id, virtual_trie_shard, fid);

        if let Some(Some(bytes)) = txn_batch.batch.get(&key) {
            return Some(bytes.clone());
        }

        match db.get(&key) {
            Ok(Some(bytes)) => Some(bytes),
            Ok(None) => None,
            Err(e) => {
                error!("Failed to read account root: {}", e);
                None
            }
        }
    }

    fn get_or_gen_vts_status(
        db: &RocksDB,
        shard_id: u32,
        virtual_trie_shard: u32,
        metadata: &ShardSnapshotMetadata,
    ) -> Result<ReplicationTriePartStatus, BootstrapError> {
        let status = Self::read_work_unit(db, shard_id, virtual_trie_shard)?;

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

        // Fetch metadata and determine target height
        // Get the peer address from the client's channel
        let peer_address = "http://127.0.0.1:3383";

        let shard_metadata = {
            let mut client = ReplicationServiceClient::connect(peer_address).await?;

            self.get_shard_snapshots(&mut client, self.shard_ids.clone())
                .await?
        };

        // Initialize databases and replay transactions for each shard
        // Note: We're not replicating shard 0 (block shard) for now as requested
        let shard_ids = self.shard_ids.clone();

        // Create tasks for each shard to run in parallel
        let mut shard_tasks = JoinSet::new();

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

            // Clone necessary data for the spawned task
            let peer_address_clone = peer_address.to_string();
            let rocksdb_dir = format!("{}.snapshot", self.rocksdb_dir);

            info!(
                "Bootstrapping shard {} at height {} with trie_branching_factor {}",
                shard_id, target_height, self.trie_branching_factor
            );

            // Spawn a task for this shard
            let shutdown_signal = self.shutdown.clone();
            let metadata = metadata.clone();

            let self_clone = self.clone();
            shard_tasks.spawn(async move {
                // Replay transactions for this shard
                self_clone
                    .start_shard_replication_outer(
                        &peer_address_clone,
                        shard_id,
                        target_height,
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

        loop {
            if shard_tasks.is_empty() {
                // All tasks completed successfully (since we check for errors below)
                // Process results if needed; for now, assuming success means Finished
                // But based on the original code, it returned Stopped - adjust as necessary
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
                            return Err(BootstrapError::TransactionReplayError(e.to_string()));
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

                    // Broadcast shutdown to all shard tasks
                    self.shutdown.store(true, Ordering::SeqCst);

                    info!("Waiting for shard tasks to shut down gracefully...");

                    // Drain the remaining tasks
                    while let Some(res) = shard_tasks.join_next().await {
                        // Ignore results since we're shutting down
                        let _ = res;
                    }

                    info!("All shard tasks have been shut down.");
                    return Ok(WorkUnitResponse::Stopped);
                }
            }
        }
    }

    async fn start_shard_replication_outer(
        &self,
        peer_address: &str,
        shard_id: u32,
        _target_height: u64,
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
                .start_shard_replication(
                    peer_address,
                    shard_id,
                    _target_height,
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
                    self.post_process_trie(rocksdb_dir, shard_id, metadata)
                        .await?;

                    return Ok(result);
                }
                WorkUnitResponse::Working | WorkUnitResponse::None => {
                    let reason = format!("Shard {} work stopped for an unknown reason", shard_id);
                    return Err(BootstrapError::GenericError(reason));
                }
            }
        }
    }

    async fn start_shard_replication(
        &self,
        peer_address: &str,
        shard_id: u32,
        _target_height: u64,
        rocksdb_dir: &str,
        metadata: ShardSnapshotMetadata,
        shutdown_signal: Arc<AtomicBool>,
    ) -> Result<WorkUnitResponse, BootstrapError> {
        let db = RocksDB::open_bulk_write_shard_db(&rocksdb_dir, shard_id);

        // Create a channel for DB commit requests
        let (db_commit_tx, mut db_commit_rx) = mpsc::unbounded_channel::<DbCommitRequest>();

        // Clone db for the DB writer thread
        let db_writer = db.clone();

        let max_messages_in_write_session = 100_000;

        // Spawn the DB writer thread
        let db_writer_task = tokio::spawn(async move {
            info!("DB writer thread started for shard {}", shard_id);
            let mut total_messages = 0;
            let total_start_time = std::time::Instant::now();

            while let Some(commit_request) = db_commit_rx.recv().await {
                let start_time = std::time::Instant::now();
                total_messages += commit_request.num_messages;

                let result = db_writer.commit(commit_request.txn_batch);
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
        });

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

            // 2. Create a work item for this VSI
            let peer_address = peer_address.to_string();
            let shutdown_signal = shutdown_signal.clone();
            let db_commit_tx = db_commit_tx.clone();

            let work_item = WorkUnit {
                current_status,
                peer_address,
                shutdown_signal,
                db_commit_tx,
            };

            work_units.push((vts, work_item));
        }

        // Now process the collected work units, up to 2 at a time
        let mut join_set: JoinSet<Result<(u32, WorkUnitResponse), BootstrapError>> = JoinSet::new();
        let mut work_index = 0;

        let mut stop_more_work = false;

        loop {
            if shutdown_signal.load(Ordering::SeqCst) {
                stop_more_work = true;
            }

            // Spawn up to 2 tasks if not stopped
            while !stop_more_work && join_set.len() < 1 && work_index < work_units.len() {
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
                        // Propagate errors immediately, as in the original
                        return Err(e);
                    }
                    Err(e) => {
                        if !e.is_cancelled() {
                            error!("Task join error: {}", e);
                            return Err(BootstrapError::GenericError(format!(
                                "Task join error: {}",
                                e
                            )));
                        }
                    }
                }
            }
        }

        // Drop the original sender and any unfinished work_units so the DB writer thread can exit
        drop(db_commit_tx);
        drop(work_units);

        // Wait for the DB writer thread to finish
        if let Err(e) = db_writer_task.await {
            error!("DB writer task failed: {}", e);
            return Err(BootstrapError::DatabaseError(format!(
                "DB writer task failed: {}",
                e
            )));
        }

        db.close();

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

    async fn process_vts_work_item(
        &self,
        work_item: WorkUnit,
        db: Arc<RocksDB>,
    ) -> Result<WorkUnitResponse, BootstrapError> {
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

        let trie = merkle_trie::MerkleTrie::new(self.trie_branching_factor).unwrap();
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

        let mut thread_client =
            ReplicationServiceClient::connect(work_item.peer_address.clone()).await?;

        loop {
            let mut last_fid = status.last_fid;
            let mut fids_to_check = vec![];

            // 2. Use the next_page_token to fetch the page of messages from the server
            let messages_page = thread_client
                .get_shard_transactions(GetShardTransactionsRequest {
                    shard_id: status.shard_id,
                    height: status.height,
                    trie_virtual_shard: status.virtual_trie_shard,
                    page_token: status.next_page_token.clone(),
                })
                .await?
                .into_inner();

            let trie_messages = messages_page.trie_messages;

            let mut txn_batch = RocksDbTransactionBatch::new();

            // Write the account roots to the DB, so we may check them later
            for fid_root in &messages_page.fid_account_roots {
                Self::write_account_root(
                    status.shard_id as u8,
                    status.virtual_trie_shard as u8,
                    fid_root.fid,
                    &mut txn_batch,
                    fid_root.account_root_hash.clone(),
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
                    Ok((fid, generated_trie_key, hub_event)) => {
                        if generated_trie_key != trie_key {
                            return Err(BootstrapError::GenericError(format!(
                                "Generated trie key {:?} does not match expected trie key {:?} for {:?}. HubEvent {:?}",
                                generated_trie_key, trie_key, trie_message_entry, hub_event
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
            thread_engine.update_trie_batch(
                &merkle_trie::Context::new(), // Not tracking trie perf during replication
                &mut txn_batch,
                trie_keys,
            )?;

            // Verify account root for fid, update last_fid in the DB
            Self::check_fid_roots(
                &thread_engine,
                status.shard_id as u8,
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
            Self::write_work_unit(&mut txn_batch, &status);

            // commit via db_commit_tx
            let (response_tx, response_rx) = oneshot::channel();
            work_item.db_commit_tx.send(DbCommitRequest {
                txn_batch,
                num_messages,
                response_tx,
            })?;

            // Wait for the commit to complete. The double ?? are to handle both the channel cancelled and the DB writer errors
            let db_response = response_rx.await??;

            // Reload the trie after commit
            thread_engine.get_stores().trie.reload(&db)?;

            // Check if shutdown signal is set, if it is, break out
            if work_item.shutdown_signal.load(Ordering::Relaxed) {
                info!(
                    "Shutdown signal received, stopping replication for shard {}, vts {}",
                    shard_id, status.virtual_trie_shard
                );

                return Ok(WorkUnitResponse::Stopped);
            }

            if let DbCommitResponse::Stop = db_response {
                return Ok(WorkUnitResponse::DbStopped);
            }

            if next_page_token.is_none() {
                // All done.

                // TODO: Check the last fid

                // Write to the DB that we're all done
                status.last_response = WorkUnitResponse::Finished as u32;
                let mut txn_batch = RocksDbTransactionBatch::new();
                Self::write_work_unit(&mut txn_batch, &status);

                // commit via db_commit_tx
                let (response_tx, response_rx) = oneshot::channel();
                work_item.db_commit_tx.send(DbCommitRequest {
                    txn_batch,
                    num_messages: 0,
                    response_tx,
                })?;

                // Wait for the commit to complete. The double ?? are to handle both the channel cancelled and the DB writer errors
                response_rx.await??;

                return Ok(WorkUnitResponse::Finished);
            }
        } // loop to next page from the server
    }

    fn check_fid_roots(
        thread_engine: &ShardEngine,
        shard_id: u8,
        virtual_trie_shard: u8,
        db: &RocksDB,
        txn_batch: &mut RocksDbTransactionBatch,
        fids_to_check: Vec<u64>,
    ) -> Result<(), BootstrapError> {
        for fid in &fids_to_check {
            let expected_root =
                Self::read_account_root(shard_id, virtual_trie_shard, *fid, txn_batch, db)
                    .ok_or_else(|| {
                        BootstrapError::AccountRootMismatch(format!(
                            "No account root found for fid {}. FIDs are {:?}",
                            fid, fids_to_check
                        ))
                    })?;

            let actual_root = thread_engine.get_stores().trie.get_hash(
                &db,
                txn_batch,
                &TrieKey::for_fid(*fid),
            )?;

            if expected_root != actual_root {
                return Err(BootstrapError::AccountRootMismatch(format!(
                    "Account root mismatch for fid  ({}-{}){}. expected {}, got {}",
                    shard_id,
                    virtual_trie_shard,
                    fid,
                    hex::encode(expected_root),
                    hex::encode(actual_root)
                )));
            } else {
                info!(
                    "Account root matched for fid ({}-{}){}: {}",
                    shard_id,
                    virtual_trie_shard,
                    fid,
                    hex::encode(actual_root)
                );
            }
        }

        Ok(())
    }

    async fn post_process_trie(
        &self,
        rocksdb_dir: &str,
        shard_id: u32,
        metadata: ShardSnapshotMetadata,
    ) -> Result<(), TrieError> {
        info!("PPTrie: Post-processing trie for shard {}.", shard_id);

        let db = RocksDB::open_bulk_write_shard_db(&rocksdb_dir, shard_id);
        let mut trie = merkle_trie::MerkleTrie::new(self.trie_branching_factor)?;
        trie.initialize(&db)?;

        // TODO: Do attach_to_root and recalculate_hashes on all the vts

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

                    info!(
                        "Added metadata for shard {} at height {}",
                        shard_id, latest_snapshot.height
                    );
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
}
