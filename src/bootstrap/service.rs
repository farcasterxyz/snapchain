use crate::bootstrap::error::BootstrapError;
use crate::cfg::Config;
use crate::core::util::FarcasterTime;
use crate::proto::get_shard_transactions_request::Cursor;
use crate::proto::replication_service_client::ReplicationServiceClient;
use crate::proto::{GetShardSnapshotMetadataRequest, GetShardTransactionsRequest, Transaction};
use crate::storage::db::{RocksDB, RocksDbTransactionBatch};
use crate::storage::store::engine::{ProposalSource, ShardEngine};
use crate::storage::store::stores::StoreLimits;
use crate::storage::trie::merkle_trie::{self, TrieKey};
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use crate::version::version::EngineVersion;
use std::error::Error;
use std::net;
use tonic::transport::Channel;
use tracing::{error, info, warn};

/// Bootstrap a node from replication instead of snapshot download
pub async fn bootstrap_from_replication(app_config: &Config) -> Result<(), Box<dyn Error>> {
    info!("Starting replication-based bootstrap");

    // For now, use a hardcoded peer as requested
    let peer_address = "http://127.0.0.1:4383";
    info!("Connecting to hardcoded peer: {}", peer_address);

    // Create a replication client
    let mut client = match ReplicationServiceClient::connect(peer_address).await {
        Ok(client) => client,
        Err(e) => {
            error!("Failed to connect to peer {}: {}", peer_address, e);
            return Err(BootstrapError::PeerConnectionError(e.to_string()).into());
        }
    };

    // Fetch metadata and determine target height
    let target_height =
        determine_target_height(&mut client, &app_config.consensus.shard_ids).await?;
    info!("Target height for bootstrap: {}", target_height);

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

        // Initialize empty database for this shard
        let db = RocksDB::open_shard_db(&app_config.rocksdb_dir, shard_id);

        // Create a read-only ShardEngine for replaying transactions
        let trie = merkle_trie::MerkleTrie::new(app_config.trie_branching_factor).unwrap();
        let mut engine = ShardEngine::new(
            db,
            app_config.fc_network,
            trie,
            shard_id,
            StoreLimits::default(),
            statsd_client.clone(),
            256,  // max_messages_per_block - default value
            None, // messages_request_tx - None for read-only
            None, // fname_signer_address
            None, // post_commit_tx
        );

        // Replay transactions for this shard
        replay_shard_transactions(&mut client, &mut engine, shard_id, target_height).await?;

        info!("Completed bootstrap for shard {}", shard_id);
    }

    info!("Replication bootstrap completed successfully");
    Ok(())
}

/// Replay transactions for a specific shard from the replication service
async fn replay_shard_transactions(
    client: &mut ReplicationServiceClient<Channel>,
    engine: &mut ShardEngine,
    shard_id: u32,
    height: u64,
) -> Result<(), BootstrapError> {
    info!(
        "Replaying transactions for shard {} at height {}",
        shard_id, height
    );

    let mut page_token: Option<Vec<u8>> = None;
    let mut total_transactions = 0;
    let mut last_fid = 0;
    let mut last_fid_account_root = vec![];
    let start_fid = 0;

    loop {
        let cursor = if let Some(token) = &page_token {
            Some(Cursor::PageToken(token.clone()))
        } else {
            Some(Cursor::StartFid(start_fid))
        };

        let request = GetShardTransactionsRequest {
            shard_id,
            height,
            cursor,
        };

        match client.get_shard_transactions(request).await {
            Ok(response) => {
                let response = response.into_inner();
                let transactions = response.transactions;

                if transactions.is_empty() {
                    info!("No more transactions to replay for shard {}", shard_id);
                    break;
                }

                let tx_first_fid = transactions.first().map_or(0, |tx| tx.fid);
                let tx_last_fid = transactions.last().map_or(0, |tx| tx.fid);

                let is_page_token_greater_than_last =
                    response.next_page_token.as_ref().map_or(false, |token| {
                        token > &page_token.clone().unwrap_or_default()
                    });
                if !is_page_token_greater_than_last {
                    warn!("Page token is not greater than last token for shard {}. Last token: {} next token {}", 
                    shard_id, hex::encode(&page_token.unwrap_or_default()), hex::encode(response.next_page_token.as_ref().unwrap_or(&vec![])));
                }

                info!(
                    "Retrieved {} transactions for shard {}. Next page token: {}. Fids: {} to {}.",
                    transactions.len(),
                    shard_id,
                    response
                        .next_page_token
                        .as_ref()
                        .map(|t| hex::encode(t))
                        .unwrap_or_else(|| "<none>".to_string()),
                    tx_first_fid,
                    tx_last_fid,
                );

                // Replay each transaction
                for transaction in &transactions {
                    if transaction.fid > last_fid && last_fid != 0 {
                        // Check the account root for the last FID to see if it matches

                        // Get the account root from the store
                        let stores_account_root = engine.get_stores().trie.get_hash(
                            &engine.db,
                            &mut RocksDbTransactionBatch::new(),
                            &TrieKey::for_fid(last_fid),
                        );

                        if stores_account_root != last_fid_account_root {
                            error!(
                                "Account root mismatch for FID {}: expected {}, got {}",
                                last_fid,
                                hex::encode(last_fid_account_root),
                                hex::encode(stores_account_root)
                            );
                            // TODO: Return Error
                        } else {
                            info!(
                                "Account root for FID {} matches: {}",
                                last_fid,
                                hex::encode(last_fid_account_root)
                            );
                        }
                    }
                    replay_transaction(engine, transaction)?;
                    total_transactions += 1;

                    last_fid = transaction.fid;
                    last_fid_account_root = transaction.account_root.clone();
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
                error!("Failed to get transactions for shard {}: {}", shard_id, e);
                return Err(BootstrapError::TransactionReplayError(e.to_string()));
            }
        }
    }

    info!(
        "Replayed {} total transactions for shard {}",
        total_transactions, shard_id
    );
    Ok(())
}

/// Replay a single transaction using the ShardEngine
fn replay_transaction(
    engine: &mut ShardEngine,
    transaction: &Transaction,
) -> Result<(), BootstrapError> {
    let mut txn_batch = RocksDbTransactionBatch::new();
    let trie_ctx = merkle_trie::Context::new();
    let timestamp = FarcasterTime::current(); // TODO: Use proper timestamp from transaction/block
    let version = EngineVersion::current(engine.network);

    // Set the block height to 0 (by resetting the event id) for bootstrap
    engine.reset_event_id();

    match engine.replay_snapchain_txn(
        &trie_ctx,
        transaction,
        &mut txn_batch,
        ProposalSource::Commit, // Using Commit since this is for bootstrap
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

            // Commit the transaction batch to the database
            engine.db.commit(txn_batch).map_err(|e| {
                BootstrapError::DatabaseError(format!("Failed to commit transaction: {}", e))
            })?;

            // Reload the trie after commit
            engine.get_stores().trie.reload(&engine.db).map_err(|e| {
                BootstrapError::DatabaseError(format!("Failed to reload trie: {}", e))
            })?;

            Ok(())
        }
        Err(e) => {
            error!(
                "Failed to replay transaction for FID {}: {}",
                transaction.fid, e
            );
            Err(BootstrapError::TransactionReplayError(e.to_string()))
        }
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
