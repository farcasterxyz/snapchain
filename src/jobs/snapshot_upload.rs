use crate::proto::FarcasterNetwork;
use crate::storage;
use crate::storage::db::snapshot::{clear_old_snapshots, SnapshotError};
use crate::storage::db::RocksDB;
use crate::storage::store::block_engine::BlockStores;
use crate::storage::store::stores::Stores;
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_cron_scheduler::{Job, JobSchedulerError};
use tracing::{error, info, warn};

const STALE_BACKUP_THRESHOLD: Duration = Duration::from_secs(12 * 60 * 60);

async fn backup_and_upload(
    fc_network: FarcasterNetwork,
    snapshot_config: storage::db::snapshot::Config,
    shard_id: u32,
    db: Arc<RocksDB>,
    now: i64,
    statsd_client: StatsdClientWrapper,
) -> Result<(), SnapshotError> {
    info!(shard_id, "Starting backup for shard");
    statsd_client.emit_jemalloc_stats();

    let backup_dir = snapshot_config.backup_dir.clone();
    let tar_gz_path = storage::db::backup::backup_db(db, &backup_dir, shard_id, now)?;

    info!(shard_id, "Backup complete, starting upload for shard");
    statsd_client.emit_jemalloc_stats();

    storage::db::snapshot::upload_to_s3(
        fc_network,
        tar_gz_path,
        &snapshot_config,
        shard_id,
        &statsd_client,
    )
    .await?;
    clear_old_snapshots(fc_network, &snapshot_config, shard_id).await?;

    info!(shard_id, "Upload complete for shard");
    statsd_client.emit_jemalloc_stats();

    Ok(())
}

pub async fn upload_snapshot(
    snapshot_config: storage::db::snapshot::Config,
    fc_network: FarcasterNetwork,
    block_stores: BlockStores,
    shard_stores: HashMap<u32, Stores>,
    statsd_client: StatsdClientWrapper,
    only_for_shard_ids: Option<HashSet<u32>>,
) -> Result<(), SnapshotError> {
    let backup_dir = &snapshot_config.backup_dir;
    std::fs::create_dir_all(backup_dir)?;

    // Check if the backup directory has contents from a previous run
    let has_contents = std::fs::read_dir(backup_dir)?.next().is_some();
    if has_contents {
        let age = match std::fs::metadata(backup_dir)?.modified() {
            Ok(modified) => match modified.elapsed() {
                Ok(duration) => duration,
                Err(err) => {
                    warn!(
                        error = ?err,
                        "Backup directory mtime is in the future; treating as stale"
                    );
                    STALE_BACKUP_THRESHOLD + Duration::from_secs(1)
                }
            },
            Err(err) => {
                warn!(
                    error = ?err,
                    "Unable to read backup directory mtime; treating as stale"
                );
                STALE_BACKUP_THRESHOLD + Duration::from_secs(1)
            }
        };
        if age > STALE_BACKUP_THRESHOLD {
            warn!(
                age_hours = age.as_secs() / 3600,
                path = %backup_dir,
                "Removing stale backup contents"
            );
            for entry in std::fs::read_dir(backup_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    std::fs::remove_dir_all(&path)?;
                } else {
                    std::fs::remove_file(&path)?;
                }
            }
        } else {
            return Err(SnapshotError::UploadAlreadyInProgress);
        }
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();

    if only_for_shard_ids
        .as_ref()
        .map_or(true, |shard_ids| shard_ids.contains(&0))
    {
        if let Err(err) = backup_and_upload(
            fc_network,
            snapshot_config.clone(),
            0,
            block_stores.db.clone(),
            now as i64,
            statsd_client.clone(),
        )
        .await
        {
            error!(
                shard = 0,
                "Unable to upload snapshot for shard {}",
                err.to_string()
            )
        }
    }

    for (shard, stores) in shard_stores.iter() {
        if only_for_shard_ids
            .as_ref()
            .map_or(true, |shard_ids| shard_ids.contains(shard))
        {
            if let Err(err) = backup_and_upload(
                fc_network,
                snapshot_config.clone(),
                *shard,
                stores.db.clone(),
                now as i64,
                statsd_client.clone(),
            )
            .await
            {
                error!(
                    shard,
                    "Unable to upload snapshot for shard {}",
                    err.to_string()
                );
            }
        }
    }

    // Clear backup directory contents but keep the directory itself (may be a bind mount)
    if let Ok(entries) = std::fs::read_dir(&snapshot_config.backup_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            let result = if path.is_dir() {
                std::fs::remove_dir_all(&path)
            } else {
                std::fs::remove_file(&path)
            };
            if let Err(err) = result {
                info!("Unable to remove backup file {:?}: {}", path, err);
            }
        }
    }

    info!("Snapshot upload complete, emitting jemalloc stats after cleanup");
    statsd_client.emit_jemalloc_stats();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::FarcasterNetwork;
    use crate::storage::db::snapshot::{Config, SnapshotError};
    use crate::storage::db::RocksDB;
    use crate::storage::store::block_engine::BlockStores;
    use crate::storage::store::test_helper;
    use crate::storage::trie::merkle_trie::MerkleTrie;
    use std::collections::HashSet;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn make_block_stores(tmpdir: &TempDir) -> BlockStores {
        let db = Arc::new(RocksDB::new(tmpdir.path().join("db").to_str().unwrap()));
        db.open().unwrap();
        BlockStores::new(db, MerkleTrie::new().unwrap(), FarcasterNetwork::Devnet)
    }

    fn config_with_backup_dir(dir: &std::path::Path) -> Config {
        Config {
            backup_dir: dir.to_str().unwrap().to_string(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_returns_in_progress_when_backup_dir_has_recent_contents() {
        let tmpdir = TempDir::new().unwrap();
        let backup_dir = tmpdir.path().join("backup");
        std::fs::create_dir_all(&backup_dir).unwrap();
        std::fs::write(backup_dir.join("existing.tar.gz"), b"data").unwrap();

        let config = config_with_backup_dir(&backup_dir);
        let block_stores = make_block_stores(&tmpdir);
        let statsd = test_helper::statsd_client();

        let result = upload_snapshot(
            config,
            FarcasterNetwork::Devnet,
            block_stores,
            HashMap::new(),
            statsd,
            None,
        )
        .await;

        assert!(
            matches!(result, Err(SnapshotError::UploadAlreadyInProgress)),
            "expected UploadAlreadyInProgress, got {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_empty_shard_filter_skips_all_backups() {
        let tmpdir = TempDir::new().unwrap();
        let backup_dir = tmpdir.path().join("backup");
        std::fs::create_dir_all(&backup_dir).unwrap();

        let config = config_with_backup_dir(&backup_dir);
        let block_stores = make_block_stores(&tmpdir);
        let statsd = test_helper::statsd_client();

        let result = upload_snapshot(
            config,
            FarcasterNetwork::Devnet,
            block_stores,
            HashMap::new(),
            statsd,
            Some(HashSet::new()),
        )
        .await;

        assert!(
            result.is_ok(),
            "expected Ok when shard filter excludes all shards, got {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_stale_backup_contents_are_removed() {
        let tmpdir = TempDir::new().unwrap();
        let backup_dir = tmpdir.path().join("backup");
        std::fs::create_dir_all(&backup_dir).unwrap();
        let stale_file = backup_dir.join("stale.tar.gz");
        std::fs::write(&stale_file, b"old content").unwrap();

        // Set mtime to 13 hours ago (past 12h STALE_BACKUP_THRESHOLD)
        let stale_time = filetime::FileTime::from_unix_time(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64
                - 13 * 3600,
            0,
        );
        filetime::set_file_mtime(&backup_dir, stale_time).unwrap();

        let config = config_with_backup_dir(&backup_dir);
        let block_stores = make_block_stores(&tmpdir);
        let statsd = test_helper::statsd_client();

        // Call with empty shard filter so no actual backup is attempted after cleanup
        let result = upload_snapshot(
            config,
            FarcasterNetwork::Devnet,
            block_stores,
            HashMap::new(),
            statsd,
            Some(HashSet::new()),
        )
        .await;

        assert!(
            result.is_ok(),
            "expected Ok after stale cleanup, got {:?}",
            result
        );
        assert!(
            !stale_file.exists(),
            "stale file should have been removed before upload"
        );
    }

    #[test]
    fn test_stale_backup_threshold_is_12_hours() {
        assert_eq!(
            STALE_BACKUP_THRESHOLD.as_secs(),
            12 * 60 * 60,
            "stale backup threshold should be exactly 12 hours"
        );
    }
}

pub fn snapshot_upload_job(
    schedule: &str,
    snapshot_config: storage::db::snapshot::Config,
    fc_network: FarcasterNetwork,
    block_stores: BlockStores,
    shard_stores: HashMap<u32, Stores>,
    statsd_client: StatsdClientWrapper,
) -> Result<Job, JobSchedulerError> {
    Job::new_async(schedule, move |_, _| {
        let snapshot_config = snapshot_config.clone();
        let block_stores = block_stores.clone();
        let shard_stores = shard_stores.clone();
        let statsd_client = statsd_client.clone();
        Box::pin(async move {
            if let Err(err) = upload_snapshot(
                snapshot_config,
                fc_network,
                block_stores,
                shard_stores,
                statsd_client,
                None,
            )
            .await
            {
                error!("Error uploading snapshots {}", err.to_string());
            }
        })
    })
}
