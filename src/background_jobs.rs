use crate::storage::constants::PAGE_SIZE_MAX;
use crate::storage::db::{PageOptions, RocksDB};
use crate::storage::store::BlockStore;
use tokio_cron_scheduler::{Job, JobScheduler, JobSchedulerError};

pub fn job_block_pruning(
    schedule: &str,
    cutoff_timestamp: u64,
    block_store: BlockStore,
) -> Result<Job, JobSchedulerError> {
    Job::new_async(schedule, move |_uuid, _l| {
        let block_store = block_store.clone(); // Get Arc for this job (can this clone be avoided?)
        Box::pin(async move {
            let page_options = PageOptions {
                page_size: Some(PAGE_SIZE_MAX),
                ..PageOptions::default()
            };

            loop {
                let mut total = 0u32;
                let done = true;
                let count = 0;
                //let (count, done) = block_store
                //    .prune_page(&page_options, |header| header.timestamp >= cutoff_timestamp)
                //    .unwrap_or_else(|e| {
                //        // TODO: handle error
                //        eprintln!("Error pruning block store: {}", e);

                //        (0, true) // stop iterating if there's an error
                //    });
                if done {
                    break;
                }

                // TODO: better logging
                total += count;
                println!("Pruned {} blocks", total);
            }
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_job_block_pruning() {
        let sched = JobScheduler::new().await.unwrap();
        let schedule = "1/5 * * * * *"; // Every 5 seconds

        let job = Job::new_async(schedule, move |_uuid, _l| {
            Box::pin(async move {
                println!("Running job...");
            })
        })
        .unwrap();

        sched.add(job).await.unwrap();
        sched.start().await.unwrap();

        sleep(Duration::from_secs(15)).await; // Let the job run a few times
    }
}
