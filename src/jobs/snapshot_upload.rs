use crate::proto::FarcasterNetwork;
use crate::storage;
use crate::storage::db::snapshot::{clear_old_snapshots, SnapshotError};
use crate::storage::db::RocksDB;
use crate::storage::store::stores::Stores;
use crate::storage::store::BlockStore;
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_cron_scheduler::{Job, JobSchedulerError};
use tracing::{error, info};

async fn backup_and_upload(
    fc_network: FarcasterNetwork,
    snapshot_config: storage::db::snapshot::Config,
    shard_id: u32,
    db: Arc<RocksDB>,
    now: i64,
    statsd_client: StatsdClientWrapper,
) -> Result<(), SnapshotError> {
    let backup_dir = snapshot_config.backup_dir.clone();
    let tar_gz_path = storage::db::backup::backup_db(db, &backup_dir, shard_id, now)?;
    storage::db::snapshot::upload_to_s3(
        fc_network,
        tar_gz_path,
        &snapshot_config,
        shard_id,
        &statsd_client,
    )
    .await?;
    clear_old_snapshots(fc_network, &snapshot_config, shard_id).await?;
    Ok(())
}

pub async fn upload_snapshot(
    snapshot_config: storage::db::snapshot::Config,
    fc_network: FarcasterNetwork,
    block_store: BlockStore,
    shard_stores: HashMap<u32, Stores>,
    statsd_client: StatsdClientWrapper,
    only_for_shard_ids: Option<HashSet<u32>>,
) -> Result<(), SnapshotError> {
    if std::fs::exists(snapshot_config.backup_dir.clone())? {
        return Err(SnapshotError::UploadAlreadyInProgress);
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
            block_store.db.clone(),
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

    if let Err(err) = std::fs::remove_dir_all(snapshot_config.backup_dir.clone()) {
        info!("Unable to remove snapshot directory: {}", err.to_string());
    }

    Ok(())
}

pub fn snapshot_upload_job(
    schedule: &str,
    snapshot_config: storage::db::snapshot::Config,
    fc_network: FarcasterNetwork,
    block_store: BlockStore,
    shard_stores: HashMap<u32, Stores>,
    statsd_client: StatsdClientWrapper,
) -> Result<Job, JobSchedulerError> {
    Job::new_async(schedule, move |_, _| {
        let snapshot_config = snapshot_config.clone();
        let block_store = block_store.clone();
        let shard_stores = shard_stores.clone();
        let statsd_client = statsd_client.clone();
        Box::pin(async move {
            if let Err(err) = upload_snapshot(
                snapshot_config,
                fc_network,
                block_store,
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
