use crate::core::util::get_farcaster_time;
use crate::storage::constants::PAGE_SIZE_MAX;
use crate::storage::db::PageOptions;
use crate::storage::store::stores::Stores;
use std::collections::HashMap;
use std::time::Duration;
use tokio_cron_scheduler::{Job, JobSchedulerError};
use tracing::{error, info};

const THROTTLE: Duration = Duration::from_millis(100);

pub fn event_pruning_job(
    schedule: &str,
    event_retention: Duration,
    shard_stores: HashMap<u32, Stores>,
) -> Result<Job, JobSchedulerError> {
    Job::new_async(schedule, move |_, _| {
        let shard_stores = shard_stores.clone();
        Box::pin(async move {
            for (shard_id, stores) in shard_stores.iter() {
                let shard_store = &stores.shard_store;
                let cutoff_timestamp =
                    get_farcaster_time().unwrap() - (event_retention.as_secs() as u64);
                let stop_height = shard_store
                    .get_next_height_by_timestamp(cutoff_timestamp)
                    .unwrap_or_else(|e| {
                        error!(
                            "Error getting next height by timestamp for shard {}: {}",
                            shard_id, e
                        );
                        None
                    });

                let page_options = PageOptions {
                    page_size: Some(PAGE_SIZE_MAX),
                    ..PageOptions::default()
                };
                if let Some(stop_height) = stop_height {
                    info!(
                        "Pruning events for shard {} older than timestamp: {}, height: {}",
                        shard_id, cutoff_timestamp, stop_height
                    );
                    stores
                        .prune_events_until(stop_height, &page_options, THROTTLE)
                        .await
                        .unwrap_or_else(|e| {
                            error!("Error pruning events: {}", e);
                            0
                        });
                }
            }
        })
    })
}
