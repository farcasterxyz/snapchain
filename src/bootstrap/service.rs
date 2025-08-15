use crate::bootstrap::error::BootstrapError;
use crate::cfg::Config;
use crate::core::util::FarcasterTime;
use crate::proto::get_shard_transactions_request::Cursor;
use crate::proto::replication_service_client::ReplicationServiceClient;
use crate::proto::{
    GetShardSnapshotMetadataRequest, GetShardTransactionsRequest, SortOrderTypes, Transaction,
};
use crate::storage::db::{RocksDB, RocksDbTransactionBatch};
use crate::storage::store::engine::{ProposalSource, ShardEngine};
use crate::storage::store::stores::StoreLimits;
use crate::storage::trie::merkle_trie::{self, TrieKey};
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use crate::version::version::EngineVersion;
use futures::future::try_join_all;
use std::error::Error;
use std::net;
use tokio::sync::{mpsc, oneshot};
use tonic::transport::Channel;
use tracing::{error, info};

/// Request to commit a transaction batch to the database
struct DbCommitRequest {
    txn_batch: RocksDbTransactionBatch,
    num_messages: u64,
    response_tx: oneshot::Sender<Result<(), BootstrapError>>,
}

/// Bootstrap a node from replication instead of snapshot download
pub async fn bootstrap_from_replication(app_config: &Config) -> Result<(), Box<dyn Error>> {
    info!("Starting replication-based bootstrap");

    // Fetch metadata and determine target height
    // Get the peer address from the client's channel
    let peer_address = "http://127.0.0.1:4383"; // Same hardcoded address as in bootstrap_from_replication
    let mut client = ReplicationServiceClient::connect(peer_address)
        .await
        .map_err(|e| BootstrapError::PeerConnectionError(e.to_string()))?;

    // Define the FID ranges to get (In 100k chunks, max 10 threads)
    let fid_chunk_size = 100_000;
    let max_threads = 10;
    let mut ranges = Vec::new();
    for i in 0..(max_threads - 1) {
        ranges.push((i * fid_chunk_size, (i + 1) * fid_chunk_size));
    }
    ranges.push((fid_chunk_size * ranges.len() as u64, 0)); // Last range goes to the end

    let target_height = 12247700; //determine_target_height(&mut client, &app_config.consensus.shard_ids).await?;
    info!(
        "Target height for bootstrap: {}. FID ranges: {:?}",
        target_height, ranges
    );

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

    for shard_id in shard_ids {
        info!("Bootstrapping shard {}", shard_id);

        // Replay transactions for this shard
        start_shard_replication(
            ranges.clone(),
            peer_address,
            app_config.fc_network,
            shard_id,
            app_config.trie_branching_factor,
            target_height,
            &statsd_client,
            &app_config.rocksdb_dir,
        )
        .await?;

        info!("Completed bootstrap for shard {}", shard_id);
    }

    info!("Replication bootstrap completed successfully");
    Ok(())
}

async fn start_shard_replication(
    ranges: Vec<(u64, u64)>,
    peer_address: &str,
    network: crate::proto::FarcasterNetwork,
    shard_id: u32,
    trie_branching_factor: u32,
    target_height: u64,
    statsd_client: &StatsdClientWrapper,
    rocksdb_dir: &str,
) -> Result<(), BootstrapError> {
    info!(
        "Starting parallel shard replication for shard {} up to height {}",
        shard_id, target_height
    );

    let db = RocksDB::open_shard_db(&rocksdb_dir, shard_id);

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
                "DB writer - Commit took {:?}. Writing at {:.1}k msg/sec",
                elapsed_time,
                total_messages as f64 / 1000.0 / total_elapsed.as_secs_f64()
            );

            // Send the result back to the requesting thread
            if let Err(_) = commit_request.response_tx.send(result) {
                error!("DB writer - Failed to send commit result - receiver dropped");
            }
        }

        info!("DB writer thread finished for shard {}", shard_id);
    });

    // Create tasks for each FID range
    let mut tasks = Vec::new();

    for (i, (start_fid, end_fid)) in ranges.iter().enumerate() {
        let peer_address = peer_address.to_string();
        let statsd_client = statsd_client.clone();
        let start_fid = *start_fid;
        let end_fid = *end_fid;
        let db_commit_tx = db_commit_tx.clone();

        let db = db.clone();
        let task = tokio::spawn(async move {
            info!(
                "Thread {} starting replication for FID range {} to {}",
                i + 1,
                start_fid,
                if end_fid == 0 {
                    "end".to_string()
                } else {
                    end_fid.to_string()
                }
            );

            // Create a new client for this thread
            let mut thread_client = ReplicationServiceClient::connect(peer_address)
                .await
                .map_err(|e| BootstrapError::PeerConnectionError(e.to_string()))?;

            // Create a new engine for this thread

            let trie = merkle_trie::MerkleTrie::new(trie_branching_factor).unwrap();
            let mut thread_engine = ShardEngine::new(
                db,
                network,
                trie,
                shard_id,
                StoreLimits::default(),
                statsd_client,
                256,
                None,
                None,
                None,
            );

            // Call replay_shard_transactions with the specified range
            replay_shard_transactions(
                &mut thread_client,
                &mut thread_engine,
                shard_id,
                target_height,
                start_fid,
                end_fid,
                db_commit_tx,
            )
            .await
        });

        tasks.push(task);
    }

    // Wait for all tasks to complete and collect results
    let results = try_join_all(tasks)
        .await
        .map_err(|e| BootstrapError::TransactionReplayError(format!("Task join error: {}", e)))?;

    // Drop the original sender so the DB writer thread can exit when all workers are done
    drop(db_commit_tx);

    // Check if any task returned an error
    for (i, result) in results.into_iter().enumerate() {
        if let Err(e) = result {
            error!("Thread {} failed during replication: {}", i + 1, e);
            return Err(e);
        }
        info!("Thread {} completed successfully", i + 1);
    }

    // Wait for the DB writer thread to finish
    if let Err(e) = db_writer_task.await {
        error!("DB writer task failed: {}", e);
        return Err(BootstrapError::DatabaseError(format!(
            "DB writer task failed: {}",
            e
        )));
    }

    info!(
        "All parallel replication threads completed successfully for shard {}",
        shard_id
    );
    Ok(())
}

/// Replay transactions for a specific shard from the replication service
async fn replay_shard_transactions(
    client: &mut ReplicationServiceClient<Channel>,
    engine: &mut ShardEngine,
    shard_id: u32,
    height: u64,
    start_fid: u64,
    end_fid: u64,
    db_commit_tx: mpsc::UnboundedSender<DbCommitRequest>,
) -> Result<(), BootstrapError> {
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

    loop {
        let cursor = if let Some(token) = &page_token {
            Some(Cursor::PageToken(token.clone()))
        } else {
            Some(Cursor::StartFid(start_fid))
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

                // TXN batch to hold all changes
                let mut txn_batch = RocksDbTransactionBatch::new();
                // FIDs to check for account root consistency that were a part of this transaction
                let mut fids_to_check = vec![];
                // Num messages in this transaction
                let mut num_messages = 0;

                // Replay each transaction
                for transaction in &transactions {
                    if transaction.fid > last_fid && last_fid != 0 {
                        fids_to_check.push((last_fid, last_fid_account_root.clone()));
                    }
                    if end_fid != 0 && transaction.fid > end_fid {
                        info!(
                            "Reached end FID {} for shard {}. Stopping replay for this thread",
                            end_fid, shard_id
                        );
                        break;
                    }

                    replay_transaction(engine, transaction, &mut txn_batch).await?;

                    total_transactions += 1;
                    num_messages += transaction.user_messages.len() as u64
                        + transaction.system_messages.len() as u64;

                    last_fid = transaction.fid;
                    last_fid_account_root = transaction.account_root.clone();
                }

                // Send the transaction batch to the DB writer thread for commit
                let (response_tx, response_rx) = oneshot::channel();
                if let Err(_) = db_commit_tx.send(DbCommitRequest {
                    txn_batch,
                    num_messages,
                    response_tx,
                }) {
                    return Err(BootstrapError::DatabaseError(format!(
                        "Thread {} - DB writer thread unavailable",
                        start_fid
                    )));
                }

                // Wait for the commit to complete
                let commit_result = response_rx.await.map_err(|_| {
                    BootstrapError::DatabaseError("DB writer response channel closed".to_string())
                })?;

                // Handle the commit result
                commit_result?;

                // Reload the trie after commit
                engine.get_stores().trie.reload(&engine.db).map_err(|e| {
                    BootstrapError::DatabaseError(format!("Failed to reload trie: {}", e))
                })?;

                // Check the account roots for the processed FIDs
                for (fid, account_root) in fids_to_check {
                    check_account_roots(engine, fid, &account_root)?;
                }

                // Check if there are more pages
                if let Some(next_token) = response.next_page_token {
                    if !next_token.is_empty() {
                        page_token = Some(next_token);
                    } else {
                        info!("No more pages available for shard {}", shard_id);
                        break;
                    }
                } else {
                    info!("No next page token provided for shard {}", shard_id);
                    break;
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
    }

    // Check the last account's roots
    check_account_roots(engine, last_fid, &last_fid_account_root)?;

    info!(
        "Replayed {} total transactions for shard {}",
        total_transactions, shard_id
    );
    Ok(())
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

/// Determine the highest common block number for which all shards have a snapshot
async fn determine_target_height(
    client: &mut ReplicationServiceClient<Channel>,
    shard_ids: &[u32],
) -> Result<u64, BootstrapError> {
    let mut min_height = u64::MAX;

    // Check all configured shards plus block shard (0)
    let all_shards = shard_ids.to_vec();

    // TODO: Shard 0 doesn't have a snapshot, so we skip it?

    for shard_id in all_shards {
        let request = GetShardSnapshotMetadataRequest { shard_id };

        match client.get_shard_snapshot_metadata(request).await {
            Ok(response) => {
                let metadata_response = response.into_inner();

                // Get the latest snapshot (assuming they're ordered by height)
                if let Some(latest_metadata) = metadata_response.snapshots.last() {
                    if latest_metadata.height < min_height {
                        min_height = latest_metadata.height;
                    }
                    info!(
                        "Shard {} has snapshot at height {}",
                        shard_id, latest_metadata.height
                    );
                } else {
                    error!("No snapshots found for shard {}", shard_id);
                    return Err(BootstrapError::MetadataFetchError(format!(
                        "No snapshots found for shard {}",
                        shard_id
                    )));
                }
            }
            Err(e) => {
                error!("Failed to get metadata for shard {}: {}", shard_id, e);
                return Err(BootstrapError::MetadataFetchError(e.to_string()));
            }
        }
    }

    if min_height == u64::MAX {
        return Err(BootstrapError::MetadataFetchError(
            "No valid snapshots found".to_string(),
        ));
    }

    Ok(min_height)
}
