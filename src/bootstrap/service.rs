use crate::bootstrap::error::BootstrapError;
use crate::cfg::Config;
use crate::proto::replication_service_client::ReplicationServiceClient;
use crate::proto::shard_trie_entry_with_message::TrieMessage;
use crate::proto::{
    self, FidAccountRootHash, GetShardSnapshotMetadataRequest, GetShardTransactionsRequest,
    ReplicationTriePartStatus, ShardSnapshotMetadata,
};
use crate::storage::constants::RootPrefix;
use crate::storage::db::{RocksDB, RocksDbTransactionBatch};
use crate::storage::store::account::StoreOptions;
use crate::storage::store::engine::ShardEngine;
use crate::storage::store::stores::StoreLimits;
use crate::storage::trie::merkle_trie::{self, TrieKey};
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use crate::version::version::EngineVersion;
use base64::{engine::general_purpose, Engine as _};
use futures::channel::oneshot;
use futures::future::try_join_all;
use prost::Message;
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

struct WorkUnit {
    current_status: proto::ReplicationTriePartStatus,
    peer_address: String,
    shutdown_signal: Arc<AtomicBool>,
    db_commit_tx: mpsc::UnboundedSender<DbCommitRequest>,
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
    pub async fn bootstrap_using_replication(
        &self,
        app_config: &Config,
    ) -> Result<WorkUnitResponse, BootstrapError> {
        info!("Starting replication-based bootstrap");

        // Fetch metadata and determine target height
        // Get the peer address from the client's channel
        let peer_address = "http://127.0.0.1:3383";

        let shard_metadata = {
            let mut client = ReplicationServiceClient::connect(peer_address).await?;

            self.get_shard_snapshots(&mut client, app_config.consensus.shard_ids.to_vec())
                .await?
        };

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

            if target_height == 0 {
                return Err(BootstrapError::GenericError(format!(
                    "No valid snapshot found for shard {}. Skipping bootstrap.",
                    shard_id
                )));
            }

            // Clone necessary data for the spawned task
            let peer_address_clone = peer_address.to_string();
            let network = app_config.fc_network;
            let trie_branching_factor = app_config.trie_branching_factor;
            let statsd_client_clone = statsd_client.clone();
            let rocksdb_dir = format!("{}.snapshot", app_config.rocksdb_dir.clone());

            info!(
                "Bootstrapping shard {} at height {} with trie_branching_factor {}",
                shard_id, target_height, trie_branching_factor
            );

            // Spawn a task for this shard
            let shutdown_signal = self.shutdown.clone();
            let metadata = metadata.clone();

            let task = tokio::spawn(async move {
                // Replay transactions for this shard
                let result = Self::start_shard_replication_outer(
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
                    BootstrapError::TransactionReplayError(format!(
                        "Shard task join error: {}",
                        e
                    ))
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

        Ok(WorkUnitResponse::Stopped)
    }

    async fn start_shard_replication_outer(
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
        loop {
            if shutdown_signal.load(Ordering::SeqCst) {
                info!("Shutdown signal received, stopping shard replication");
                return Ok(WorkUnitResponse::Stopped);
            }

            let result = Self::start_shard_replication(
                peer_address,
                network,
                shard_id,
                trie_branching_factor,
                target_height,
                statsd_client,
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

                    // TODO: PostProcess the trie

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

        let mut stopped = false;

        // Go over each Trie's Virtual Trie Shard (vts) and sync for it.
        let mut vts_results = vec![WorkUnitResponse::None; merkle_trie::TRIE_SHARD_SIZE as usize];
        for vts in 0..merkle_trie::TRIE_SHARD_SIZE {
            if shutdown_signal.load(Ordering::SeqCst) {
                info!(
                    "Shutdown signal received, stopping processing for shard {}",
                    shard_id
                );
                stopped = true;
                break;
            }

            // 1. Read the progress for this from the DB
            let current_status = Self::get_or_gen_vts_status(&db, shard_id, vts, &metadata)?;
            if current_status.last_response == WorkUnitResponse::Finished as u32 {
                debug!("Shard {} vts {} already finished, skipping", shard_id, vts);
                continue;
            }

            info!("Shard {} vts loop for vts: {}", shard_id, vts);

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

            // 3. Start iterating over this VSI and merging messages
            let result = Self::process_vts_work_item(
                work_item,
                network,
                shard_id,
                trie_branching_factor,
                target_height,
                statsd_client,
                db.clone(),
            )
            .await?;

            vts_results[vts as usize] = result.clone();
            if let WorkUnitResponse::DbStopped = result {
                break;
            }
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

        if stopped {
            return Ok(WorkUnitResponse::Stopped);
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

        // Say we are partially complete
        return Ok(WorkUnitResponse::PartiallyComplete);
    }

    async fn process_vts_work_item(
        work_item: WorkUnit,
        network: crate::proto::FarcasterNetwork,
        shard_id: u32,
        trie_branching_factor: u32,
        target_height: u64,
        statsd_client: &StatsdClientWrapper,
        db: Arc<RocksDB>,
    ) -> Result<WorkUnitResponse, BootstrapError> {
        // 1. Create the engine and trie objects
        let mut status = work_item.current_status;

        let network = network.clone();
        let statsd_client = statsd_client.clone();

        let store_opts = StoreOptions {
            conflict_free: true, // All messages will be free of conflicts, since these are from a already-merged snapshot
            save_hub_events: false, // No need for HubEvents, which are emitted only from "live" nodes
        };

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

        let mut thread_client =
            ReplicationServiceClient::connect(work_item.peer_address.clone()).await?;

        // When we get an FID's messages, we don't know if they will spill over to the next page until we get the next
        // page. So, we need to save this page's last_fid's account_root, because we may need it in the next pass to
        // verify the account root hash. If the next page doesn't have any messages for last_fid, it won't contain the
        // account_root, so we need to save it.
        let mut last_fid_account_root = vec![];

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
            let mut fid_account_roots: HashMap<u64, Vec<u8>> = messages_page
                .fid_account_roots
                .iter()
                .map(|r| (r.fid, r.account_root_hash.clone()))
                .collect();
            // Extend the fid_account_roots with the one from last time, it will be verified in this pass
            if !last_fid_account_root.is_empty() && last_fid.is_some() {
                fid_account_roots.insert(last_fid.unwrap(), last_fid_account_root.clone());
            }

            let next_page_token = messages_page.next_page_token;

            debug!(
                "Transactions page has {} messages, and the fids are {:?}",
                trie_messages.len(),
                fid_account_roots.iter().map(|(f, _)| f).collect::<Vec<_>>()
            );

            // Set the block height to 0 (by resetting the event id) for bootstrap. This makes the hub events
            // get proper IDs.
            thread_engine.reset_event_id();

            let mut trie_keys = vec![];
            let num_messages = trie_messages.len();
            let mut txn_batch = RocksDbTransactionBatch::new();

            // 3. Start going through all onchain_events and messages for fids > work_item.last_fid
            for trie_message_entry in trie_messages {
                let trie_key = trie_message_entry.trie_key.clone();
                trie_keys.push(trie_key.clone());

                let (fid, generated_trie_key, hub_event) =
                    thread_engine.replay_replicator_message(&mut txn_batch, &trie_message_entry)?;

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

            // Insert all the trie keys into the trie
            thread_engine.update_trie_batch(
                &merkle_trie::Context::new(), // Not tracking trie perf during replication
                &mut txn_batch,
                trie_keys,
            )?;

            // Verify account root for fid, update last_fid in the DB
            Self::check_fid_roots(
                &thread_engine,
                &db,
                &mut txn_batch,
                fids_to_check,
                &fid_account_roots,
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

            // Keep the last_fid's account_root_hash, because we might need to verify it next pass
            if last_fid.is_some() {
                last_fid_account_root = fid_account_roots
                    .get(&last_fid.unwrap())
                    .cloned()
                    .unwrap_or(vec![]);
            }

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
        db: &RocksDB,
        txn_batch: &mut RocksDbTransactionBatch,
        fids_to_check: Vec<u64>,
        fid_account_roots: &HashMap<u64, Vec<u8>>,
    ) -> Result<(), BootstrapError> {
        for fid in fids_to_check {
            let expected_root = fid_account_roots.get(&fid).cloned().ok_or_else(|| {
                BootstrapError::GenericError(format!(
                    "No account root found for fid {}. FIDs are {:?}",
                    fid,
                    fid_account_roots.iter().map(|(f, _)| f).collect::<Vec<_>>()
                ))
            })?;

            let actual_root =
                thread_engine
                    .get_stores()
                    .trie
                    .get_hash(&db, txn_batch, &TrieKey::for_fid(fid))?;

            if expected_root != actual_root {
                return Err(BootstrapError::AccountRootMismatch(format!(
                    "Account root mismatch for fid {}. expected {}, got {}",
                    fid,
                    hex::encode(expected_root),
                    hex::encode(actual_root)
                )));
            } else {
                info!(
                    "Account root matched for fid {}: {}",
                    fid,
                    hex::encode(actual_root)
                );
            }
        }

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
}
