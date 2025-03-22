use crate::core::util;
use crate::storage::constants::PAGE_SIZE_MAX;
use crate::storage::db::PageOptions;
use crate::storage::store::stores::Stores;
use crate::storage::store::BlockStore;
use std::collections::HashMap;
use tokio::sync::watch;
use tokio::time::Duration;
use tokio_cron_scheduler::{Job, JobSchedulerError};
use tracing::{error, info};

pub fn job_block_pruning(
    schedule: &str,
    block_retention: Duration,
    block_store: BlockStore,
    shard_stores: HashMap<u32, Stores>,
    sync_complete_rx: watch::Receiver<bool>,
) -> Result<Job, JobSchedulerError> {
    Job::new_async(schedule, move |_, _| {
        // TODO: Can these clones be avoided?
        let sync_complete_rx = sync_complete_rx.clone();
        let block_store = block_store.clone();
        let shard_stores = shard_stores.clone();
        Box::pin(async move {
            // Wait for sync to complete before pruning
            let sync_complete = *sync_complete_rx.borrow();
            if !sync_complete {
                info!("Sync not complete, skipping block pruning");
                return;
            }

            let cutoff_timestamp =
                util::get_farcaster_time().unwrap() - (block_retention.as_secs() as u64);
            let stop_height = block_store
                .get_next_height_by_timestamp(cutoff_timestamp)
                .unwrap_or_else(|e| {
                    error!("Error getting next height by timestamp: {}", e);
                    None
                });

            let page_options = PageOptions {
                page_size: Some(PAGE_SIZE_MAX),
                ..PageOptions::default()
            };
            let throttle = tokio::time::Duration::from_millis(100); // TODO: make const or configurable
            if let Some(stop_height) = stop_height {
                info!(
                    "Pruning blocks older than timestamp: {}, height: {}",
                    cutoff_timestamp, stop_height
                );
                block_store
                    .prune_until(stop_height, &page_options, throttle)
                    .await
                    .unwrap_or_else(|e| {
                        error!("Error pruning blocks: {}", e);
                        0
                    });
            }

            for (shard_id, stores) in shard_stores.iter() {
                let shard_store = &stores.shard_store;
                let stop_height = shard_store
                    .get_next_height_by_timestamp(cutoff_timestamp)
                    .unwrap_or_else(|e| {
                        error!(
                            "Error getting next height by timestamp for shard {}: {}",
                            shard_id, e
                        );
                        None
                    });
                if let Some(stop_height) = stop_height {
                    info!(
                        "Pruning shard {} blocks older than timestamp: {}, height: {}",
                        shard_id, cutoff_timestamp, stop_height
                    );
                    shard_store
                        .prune_until(stop_height, &page_options, throttle)
                        .await
                        .unwrap_or_else(|e| {
                            error!("Error pruning shard {} blocks: {}", shard_id, e);
                            0
                        });
                }
            }
        })
    })
}
