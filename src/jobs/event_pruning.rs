use crate::core::util::get_farcaster_time;
use crate::storage::store::stores::Stores;
use std::collections::HashMap;
use std::time::Duration;
use tokio_cron_scheduler::{Job, JobSchedulerError};
use tracing::error;

const THROTTLE: Duration = Duration::from_millis(200);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_creation_with_empty_shard_map() {
        let result = event_pruning_job("0/1 * * * * *", Duration::from_secs(86400), HashMap::new());
        assert!(
            result.is_ok(),
            "expected job creation to succeed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_cutoff_timestamp_precedes_now_by_retention() {
        let retention = Duration::from_secs(3600);
        let now = get_farcaster_time().unwrap();
        let cutoff = now - retention.as_secs() as u64;
        assert!(cutoff < now, "cutoff should be before now");
        assert_eq!(
            now - cutoff,
            retention.as_secs() as u64,
            "difference should equal retention in farcaster seconds"
        );
    }

    #[test]
    fn test_longer_retention_produces_lower_cutoff() {
        let now = get_farcaster_time().unwrap();
        let short = Duration::from_secs(3600);
        let long = Duration::from_secs(7 * 24 * 3600);
        let cutoff_short = now - short.as_secs() as u64;
        let cutoff_long = now - long.as_secs() as u64;
        assert!(
            cutoff_long < cutoff_short,
            "longer retention should prune further back in time"
        );
    }
}

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
