use crate::core::error::HubError;
use crate::mempool::mempool::MempoolSource;
use crate::proto::message_data::Body;
use crate::proto::{ExternalData, Message, RevalidateMessage};
use crate::storage::constants::PAGE_SIZE_MAX;
use crate::storage::db::PageOptions;
use crate::storage::store::account::UsernameProofStore;
use crate::storage::store::mempool_poller::MempoolMessage;
use crate::storage::store::node_local_state::LocalStateStore;
use crate::storage::store::stores::Stores;
use crate::{connectors::onchain_events::ChainClients, mempool::mempool::MempoolRequest};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio_cron_scheduler::{Job, JobSchedulerError};
use tracing::{error, info, warn};

const THROTTLE: Duration = Duration::from_millis(100);

pub fn revalidate_ens_proofs_job(
    schedule: &str,
    shard_stores: HashMap<u32, Stores>,
    local_state_store: LocalStateStore,
    chain_clients: Arc<ChainClients>,
    mempool_tx: mpsc::Sender<MempoolRequest>,
) -> Result<Job, JobSchedulerError> {
    Job::new_async(schedule, move |_, _| {
        let shard_stores = shard_stores.clone();
        let local_state_store = local_state_store.clone();
        let chain_clients = chain_clients.clone();
        let mempool_tx = mempool_tx.clone();
        Box::pin(async move {
            info!("Starting ENS proof revalidation job");

            for (shard_id, stores) in shard_stores.iter() {
                info!("Revalidating ENS proofs for shard {}", shard_id);

                if let Err(e) = revalidate_ens_proofs_for_shard(
                    stores,
                    &local_state_store,
                    &chain_clients,
                    &mempool_tx,
                )
                .await
                {
                    error!(
                        "Failed to revalidate ENS proofs for shard {}: {}",
                        shard_id, e
                    );
                }

                tokio::time::sleep(THROTTLE).await;
            }

            info!("Completed ENS proof revalidation job");
        })
    })
}

async fn revalidate_ens_proofs_for_shard(
    stores: &Stores,
    local_state_store: &LocalStateStore,
    chain_clients: &ChainClients,
    mempool_tx: &mpsc::Sender<MempoolRequest>,
) -> Result<(), HubError> {
    // Check if we have a previous run to resume from
    let start_fid = match local_state_store.get_last_processed_fid_for_ens_revalidation() {
        Ok(Some(last_fid)) => {
            info!("Resuming ENS proof validation from FID: {}", last_fid + 1);
            last_fid + 1
        }
        Ok(None) => {
            info!("Starting fresh ENS proof validation");
            0
        }
        Err(e) => {
            warn!("Failed to get last processed FID, starting fresh: {}", e);
            0
        }
    };

    let mut page_options = PageOptions {
        page_size: Some(PAGE_SIZE_MAX),
        page_token: None,
        reverse: false,
    };

    let mut fid_count = 0;

    // Paginate through all FIDs using the onchain event store
    loop {
        match stores.onchain_event_store.get_fids(&page_options) {
            Ok((fids, next_page_token)) => {
                for fid in fids {
                    // Skip FIDs we've already processed
                    if fid <= start_fid {
                        continue;
                    }

                    fid_count += 1;

                    revalidate_username_proofs_for_fid(fid, stores, chain_clients, mempool_tx)
                        .await;

                    if let Err(e) =
                        local_state_store.set_last_processed_fid_for_ens_revalidation(fid)
                    {
                        warn!("Failed to save progress for FID {}: {}", fid, e);
                    }

                    if fid_count % 100 == 0 {
                        tokio::time::sleep(THROTTLE).await;
                    }
                }

                // Check if there are more pages
                if let Some(token) = next_page_token {
                    page_options.page_token = Some(token);
                } else {
                    break;
                }
            }
            Err(e) => {
                error!("Failed to get FIDs from onchain event store: {}", e);
                break;
            }
        }
    }

    // Clear the progress tracking since we've completed the job
    if let Err(e) = local_state_store.clear_ens_revalidation_job_state() {
        warn!("Failed to clear ENS revalidation job state: {}", e);
    } else {
        info!("Cleared ENS revalidation job state - all FIDs processed");
    }

    info!("Completed ENS proof validation");

    Ok(())
}

async fn revalidate_username_proofs_for_fid(
    fid: u64,
    stores: &Stores,
    chain_clients: &ChainClients,
    mempool_tx: &mpsc::Sender<MempoolRequest>,
) {
    // Get all username proofs for this FID (with pagination)
    let mut username_proof_page_options = PageOptions {
        page_size: Some(PAGE_SIZE_MAX),
        page_token: None,
        reverse: false,
    };

    let mut num_messages_processed = 0;

    // Paginate through all username proofs for this FID
    loop {
        match UsernameProofStore::get_username_proofs_by_fid(
            &stores.username_proof_store,
            fid,
            &username_proof_page_options,
        ) {
            Ok(username_proofs_page) => {
                // Process all messages in this page
                for message in &username_proofs_page.messages {
                    if let Err(err) =
                        validate_ens_proof(message, stores, chain_clients, &mempool_tx).await
                    {
                        error!("Unable to run ens proof validation : {}", err.to_string());
                    }
                    num_messages_processed += 1;
                }

                // Check if there are more pages for this FID
                if let Some(page_token) = username_proofs_page.next_page_token {
                    username_proof_page_options.page_token = Some(page_token);
                } else {
                    break;
                }
            }
            Err(_) => {
                // FID has no username proofs, break out of pagination loop
                break;
            }
        }
    }

    info!(
        "Validated {} ens proofs for fid {}",
        num_messages_processed, fid
    );
}

async fn validate_ens_proof(
    message: &Message,
    stores: &Stores,
    chain_clients: &ChainClients,
    mempool_tx: &mpsc::Sender<MempoolRequest>,
) -> Result<(), HubError> {
    let data = message
        .data
        .as_ref()
        .ok_or_else(|| HubError::validation_failure("Message data is missing"))?;

    let username_proof = match &data.body {
        Some(Body::UsernameProofBody(body)) => body,
        _ => return Err(HubError::validation_failure("Not a username proof message")),
    };

    let resolved_address = chain_clients
        .resolve_ens_address(&username_proof)
        .await
        .map_err(|e| {
            HubError::validation_failure(&format!("Failed to resolve ENS name: {}", e.to_string()))
        })?;

    if let Err(_) =
        stores.validate_ens_username_proof(message.fid(), &username_proof, &resolved_address)
    {
        mempool_tx
            .send(MempoolRequest::AddMessage(
                MempoolMessage::RevalidateMessage(RevalidateMessage {
                    message: Some(message.clone()),
                    external_data: Some(ExternalData {
                        ens_resolved_address: Some(resolved_address),
                    }),
                }),
                MempoolSource::Local,
                None,
            ))
            .await
            .map_err(|_| HubError::unavailable("can't send message over mempool tx"))?;
    }

    Ok(())
}
