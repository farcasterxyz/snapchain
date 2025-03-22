use crate::core::util;
use crate::storage::constants::PAGE_SIZE_MAX;
use crate::storage::db::PageOptions;
use crate::storage::store::BlockStore;
use tokio::sync::watch;
use tokio::time::Duration;
use tokio_cron_scheduler::{Job, JobSchedulerError};
use tracing::{error, info};

pub fn job_block_pruning(
    schedule: &str,
    block_retention: Duration,
    block_store: BlockStore,
    sync_complete_rx: watch::Receiver<bool>,
) -> Result<Job, JobSchedulerError> {
    Job::new(schedule, move |_, _| {
        // Wait for sync to complete before pruning
        let sync_complete = *sync_complete_rx.borrow();
        if !sync_complete {
            info!("Sync not complete, skipping block pruning");
            return;
        }

        let page_options = PageOptions {
            page_size: Some(PAGE_SIZE_MAX),
            ..PageOptions::default()
        };

        let cutoff_timestamp =
            util::get_farcaster_time().unwrap() - (block_retention.as_secs() as u64);
        let stop_height = block_store
            .get_next_height_by_timestamp(0, cutoff_timestamp)
            .unwrap_or_else(|e| {
                error!("Error getting next height by timestamp: {}", e);
                None
            });

        let throttle = tokio::time::Duration::from_millis(100); // TODO: make const or configurable
        let block_store = block_store.clone();
        stop_height.map(|stop_height| async move {
            block_store
                .prune_until(stop_height, &page_options, throttle)
                .await
        });
    })
}
