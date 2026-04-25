use crate::storage::constants::PAGE_SIZE_MAX;
use crate::storage::db::PageOptions;
use crate::storage::store::stores::Stores;
use crate::{core::util, storage::store::block_engine::BlockStores};
use std::collections::HashMap;
use tokio::sync::watch;
use tokio::time::Duration;
use tokio_cron_scheduler::{Job, JobSchedulerError};
use tracing::{error, info};

const THROTTLE: Duration = Duration::from_millis(100);

/// Returns the cutoff farcaster timestamp before which blocks should be pruned,
/// or `None` if pruning should be skipped (sync not yet complete).
fn pruning_cutoff(
    now_farcaster_secs: u64,
    block_retention: Duration,
    sync_complete: bool,
) -> Option<u64> {
    if !sync_complete {
        return None;
    }
    Some(now_farcaster_secs.saturating_sub(block_retention.as_secs()))
}

pub fn block_pruning_job(
    schedule: &str,
    block_retention: Duration,
    block_stores: BlockStores,
    shard_stores: HashMap<u32, Stores>,
    sync_complete_rx: watch::Receiver<bool>,
) -> Result<Job, JobSchedulerError> {
    Job::new_async(schedule, move |_, _| {
        // TODO: Can these clones be avoided?
        let sync_complete_rx = sync_complete_rx.clone();
        let block_stores = block_stores.clone();
        let shard_stores = shard_stores.clone();
        Box::pin(async move {
            let cutoff_timestamp = match pruning_cutoff(
                util::get_farcaster_time().unwrap(),
                block_retention,
                *sync_complete_rx.borrow(),
            ) {
                Some(cutoff) => cutoff,
                None => {
                    info!("Sync not complete, skipping block pruning");
                    return;
                }
            };

            let stop_height = block_stores
                .block_store
                .get_next_height_by_timestamp(cutoff_timestamp)
                .unwrap_or_else(|e| {
                    error!("Error getting next height by timestamp: {}", e);
                    None
                });

            let page_options = PageOptions {
                page_size: Some(PAGE_SIZE_MAX),
                ..PageOptions::default()
            };
            if let Some(stop_height) = stop_height {
                info!(
                    "Pruning blocks older than timestamp: {}, height: {}",
                    cutoff_timestamp, stop_height
                );
                let count = block_stores
                    .block_store
                    .prune_until(stop_height, &page_options, THROTTLE)
                    .await
                    .unwrap_or_else(|e| {
                        error!("Error pruning blocks: {}", e);
                        0
                    });
                info!("Pruned {} blocks", count);
            }

            for (shard_id, stores) in shard_stores.iter() {
                stores
                    .prune_shard_chunks_until(cutoff_timestamp, THROTTLE, None)
                    .await
                    .unwrap_or_else(|e| {
                        error!("Error pruning shard {} chunks: {}", shard_id, e);
                        0
                    });
            }
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::FarcasterNetwork;
    use crate::storage::db::RocksDB;
    use crate::storage::trie::merkle_trie::MerkleTrie;
    use std::sync::Arc;

    fn make_block_stores(dir: &std::path::Path) -> BlockStores {
        let db = Arc::new(RocksDB::new(dir.to_str().unwrap()));
        db.open().unwrap();
        BlockStores::new(db, MerkleTrie::new().unwrap(), FarcasterNetwork::Devnet)
    }

    #[test]
    fn test_job_creation_with_sync_not_complete() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let block_stores = make_block_stores(&tmpdir.path().join("db"));
        let (_tx, rx) = watch::channel(false);
        let result = block_pruning_job(
            "0/1 * * * * *",
            Duration::from_secs(86400 * 30),
            block_stores,
            HashMap::new(),
            rx,
        );
        assert!(
            result.is_ok(),
            "expected job creation to succeed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_job_creation_with_sync_complete() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let block_stores = make_block_stores(&tmpdir.path().join("db"));
        let (_tx, rx) = watch::channel(true);
        let result = block_pruning_job(
            "0/1 * * * * *",
            Duration::from_secs(86400 * 30),
            block_stores,
            HashMap::new(),
            rx,
        );
        assert!(
            result.is_ok(),
            "expected job creation to succeed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_pruning_cutoff_returns_none_when_sync_incomplete() {
        assert_eq!(
            pruning_cutoff(1_000_000, Duration::from_secs(3600), false),
            None,
            "pruning should be skipped while sync is incomplete"
        );
    }

    #[test]
    fn test_pruning_cutoff_returns_value_when_sync_complete() {
        assert_eq!(
            pruning_cutoff(1_000_000, Duration::from_secs(3600), true),
            Some(1_000_000 - 3600)
        );
    }

    #[test]
    fn test_pruning_cutoff_saturates_when_retention_exceeds_now() {
        assert_eq!(
            pruning_cutoff(100, Duration::from_secs(1_000), true),
            Some(0),
            "cutoff should saturate at 0 instead of underflowing"
        );
    }
}
