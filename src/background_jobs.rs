use crate::storage::constants::PAGE_SIZE_MAX;
use crate::storage::db::PageOptions;
use crate::storage::store::BlockStore;
use tokio::time::Duration;
use tokio_cron_scheduler::{Job, JobSchedulerError};
use tokio_util::sync::CancellationToken;
use tracing::error;

pub fn job_block_pruning(
    schedule: &str,
    cutoff_timestamp: u64,
    throttle: Duration,
    block_store: BlockStore,
    cancel: CancellationToken,
) -> Result<Job, JobSchedulerError> {
    Job::new_async(schedule, move |_uuid, _l| {
        let block_store = block_store.clone(); // Get Arc for this job (can this clone be avoided?)
        let cancel = cancel.clone(); // (can this clone be avoided?)
        Box::pin(async move {
            let page_options = PageOptions {
                page_size: Some(PAGE_SIZE_MAX),
                ..PageOptions::default()
            };

            let stop_height = block_store
                .get_next_height_by_timestamp(0, cutoff_timestamp)
                .unwrap_or_else(|e| {
                    error!("Error getting next height by timestamp: {}", e);
                    None
                });

            stop_height.map(|stop_height| {
                block_store.prune_until(stop_height, &page_options, throttle, Some(cancel))
            });
        })
    })
}
