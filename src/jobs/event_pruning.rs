use crate::core::util::get_farcaster_time;
use crate::storage::store::stores::Stores;
use std::collections::HashMap;
use std::time::Duration;
use tokio_cron_scheduler::{Job, JobSchedulerError};
use tracing::error;

const THROTTLE: Duration = Duration::from_millis(200);

pub fn event_pruning_job(
    schedule: &str,
    event_retention: Duration,
    shard_stores: HashMap<u32, Stores>,
) -> Result<Job, JobSchedulerError> {
    Job::new_async(schedule, move |_, _| {
        let shard_stores = shard_stores.clone();
        Box::pin(async move {
            for (_shard_id, stores) in shard_stores.iter() {
                let cutoff_timestamp =
                    get_farcaster_time().unwrap() - (event_retention.as_secs() as u64);
                stores
                    .prune_events_until(cutoff_timestamp, THROTTLE, None)
                    .await
                    .unwrap_or_else(|e| {
                        error!("Error pruning events: {}", e);
                        0
                    });
            }
        })
    })
}
