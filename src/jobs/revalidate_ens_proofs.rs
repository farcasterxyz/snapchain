use crate::connectors::onchain_events::ChainAPI;
use crate::core::error::HubError;
use crate::proto::message_data::Body;
use crate::proto::{Message, UserNameType};
use crate::storage::constants::PAGE_SIZE_MAX;
use crate::storage::db::PageOptions;
use crate::storage::store::account::UsernameProofStore;
use crate::storage::store::node_local_state::LocalStateStore;
use crate::storage::store::stores::Stores;
use std::collections::HashMap;
use tokio::time::Duration;
use tokio_cron_scheduler::{Job, JobSchedulerError};
use tracing::{error, info, warn};

const THROTTLE: Duration = Duration::from_millis(100);

pub fn revalidate_ens_proofs_job(
    schedule: &str,
    shard_stores: HashMap<u32, Stores>,
    local_state_store: LocalStateStore,
    chain_api: impl ChainAPI + Clone + 'static,
) -> Result<Job, JobSchedulerError> {
    Job::new_async(schedule, move |_, _| {
        let shard_stores = shard_stores.clone();
        let chain_api = chain_api.clone();
        let local_state_store = local_state_store.clone();
        Box::pin(async move {
            info!("Starting ENS proof revalidation job");

            for (shard_id, stores) in shard_stores.iter() {
                info!("Revalidating ENS proofs for shard {}", shard_id);

                if let Err(e) =
                    revalidate_ens_proofs_for_shard(stores, &local_state_store, &chain_api).await
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
    chain_api: &impl ChainAPI,
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

    let mut total_processed = 0;
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

                    // Get all username proofs for this FID (with pagination)
                    let mut username_proof_page_options = PageOptions {
                        page_size: Some(PAGE_SIZE_MAX),
                        page_token: None,
                        reverse: false,
                    };

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
                                    if let Err(e) = validate_ens_proof(message, chain_api).await {
                                        warn!("ENS proof validation failed for FID {}: {}", fid, e);
                                    } else {
                                        total_processed += 1;
                                    }
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

                    if let Err(e) =
                        local_state_store.set_last_processed_fid_for_ens_revalidation(fid)
                    {
                        warn!("Failed to save progress for FID {}: {}", fid, e);
                    }

                    if total_processed % 100 == 0 {
                        tokio::time::sleep(THROTTLE).await;
                        info!(
                            "Processed {} FIDs, validated {} ENS proofs so far (last FID: {})",
                            fid_count, total_processed, fid
                        );
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

    info!(
        "Completed ENS proof validation: processed {} FIDs, validated {} ENS proofs total",
        fid_count, total_processed
    );

    Ok(())
}

async fn validate_ens_proof(message: &Message, chain_api: &impl ChainAPI) -> Result<(), HubError> {
    let data = message
        .data
        .as_ref()
        .ok_or_else(|| HubError::validation_failure("Message data is missing"))?;

    let username_proof_body = match &data.body {
        Some(Body::UsernameProofBody(body)) => body,
        _ => return Err(HubError::validation_failure("Not a username proof message")),
    };

    if username_proof_body.r#type != UserNameType::UsernameTypeEnsL1 as i32 {
        return Ok(());
    }

    let ens_name = std::str::from_utf8(&username_proof_body.name)
        .map_err(|_| HubError::validation_failure("ENS name is not valid UTF-8"))?;

    if !ens_name.ends_with(".eth") {
        return Ok(());
    }

    let resolved_address = chain_api
        .resolve_ens_name(ens_name.to_string())
        .await
        .map_err(|e| {
            HubError::validation_failure(
                format!("Failed to resolve ENS name '{}': {}", ens_name, e).as_str(),
            )
        })?;

    if resolved_address.to_vec() != username_proof_body.owner {
        // TODO: Handle invalid ENS proof - consider removing or marking as invalid
        return Err(HubError::validation_failure(
            "ENS resolved address does not match proof owner",
        ));
    }

    Ok(())
}
