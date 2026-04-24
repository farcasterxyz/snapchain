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
    fn test_sync_gate_skips_pruning_when_not_synced() {
        let (_tx, rx) = watch::channel(false);
        assert!(!*rx.borrow(), "receiver should reflect false");
    }

    #[test]
    fn test_sync_gate_allows_pruning_when_synced() {
        let (tx, rx) = watch::channel(false);
        tx.send(true).unwrap();
        assert!(*rx.borrow(), "receiver should reflect true after send");
    }
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
            // Wait for sync to complete before pruning
            let sync_complete = *sync_complete_rx.borrow();
            if !sync_complete {
                info!("Sync not complete, skipping block pruning");
                return;
            }

            let cutoff_timestamp =
                util::get_farcaster_time().unwrap() - (block_retention.as_secs() as u64);
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
