use crate::core::util::get_farcaster_time;
use crate::storage::store::stores::Stores;
use std::collections::HashMap;
use std::time::Duration;
use tokio_cron_scheduler::{Job, JobSchedulerError};
use tracing::error;

const THROTTLE: Duration = Duration::from_millis(200);

fn cutoff_timestamp(now_farcaster_secs: u64, event_retention: Duration) -> u64 {
    now_farcaster_secs.saturating_sub(event_retention.as_secs())
}

pub fn event_pruning_job(
    schedule: &str,
    event_retention: Duration,
    shard_stores: HashMap<u32, Stores>,
) -> Result<Job, JobSchedulerError> {
    Job::new_async(schedule, move |_, _| {
        let shard_stores = shard_stores.clone();
        Box::pin(async move {
            let cutoff = cutoff_timestamp(get_farcaster_time().unwrap(), event_retention);
            for (_shard_id, stores) in shard_stores.iter() {
                stores
                    .prune_events_until(cutoff, THROTTLE, None)
                    .await
                    .unwrap_or_else(|e| {
                        error!("Error pruning events: {}", e);
                        0
                    });
            }
        })
    })
}

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
    fn test_cutoff_timestamp_subtracts_retention() {
        assert_eq!(
            cutoff_timestamp(1_000_000, Duration::from_secs(3600)),
            1_000_000 - 3600
        );
    }

    #[test]
    fn test_cutoff_timestamp_saturates_when_retention_exceeds_now() {
        assert_eq!(
            cutoff_timestamp(100, Duration::from_secs(1_000)),
            0,
            "cutoff should saturate at 0 instead of underflowing"
        );
    }

    #[test]
    fn test_longer_retention_produces_lower_cutoff() {
        let now = 1_000_000u64;
        let cutoff_short = cutoff_timestamp(now, Duration::from_secs(3600));
        let cutoff_long = cutoff_timestamp(now, Duration::from_secs(7 * 24 * 3600));
        assert!(
            cutoff_long < cutoff_short,
            "longer retention should prune further back in time"
        );
    }
}
