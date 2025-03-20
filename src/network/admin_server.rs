use crate::mempool::mempool::MempoolMessageWithSource;
use crate::network::rpc_extensions::authenticate_request;
use crate::proto::admin_service_server::AdminService;
use crate::proto::{Empty, FarcasterNetwork};
use crate::storage;
use crate::storage::db::snapshot::clear_snapshots;
use crate::storage::db::RocksDB;
use crate::storage::store::stores::Stores;
use crate::storage::store::BlockStore;
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use rocksdb;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};
use tracing::{error, info};

pub struct MyAdminService {
    allowed_users: HashMap<String, String>,
    pub mempool_tx: mpsc::Sender<MempoolMessageWithSource>,
    snapshot_config: storage::db::snapshot::Config,
    shard_stores: HashMap<u32, Stores>,
    block_store: BlockStore,
    fc_network: FarcasterNetwork,
    statsd_client: StatsdClientWrapper,
}

#[derive(Debug, Error)]
pub enum AdminServiceError {
    #[error(transparent)]
    RocksDBError(#[from] rocksdb::Error),

    #[error(transparent)]
    IoError(#[from] io::Error),
}

impl MyAdminService {
    pub fn new(
        rpc_auth: String,
        mempool_tx: mpsc::Sender<MempoolMessageWithSource>,
        shard_stores: HashMap<u32, Stores>,
        block_store: BlockStore,
        snapshot_config: storage::db::snapshot::Config,
        fc_network: FarcasterNetwork,
        statsd_client: StatsdClientWrapper,
    ) -> Self {
        let mut allowed_users = HashMap::new();
        for auth in rpc_auth.split(",") {
            let parts: Vec<&str> = auth.split(":").collect();
            if parts.len() == 2 {
                allowed_users.insert(parts[0].to_string(), parts[1].to_string());
            }
        }

        Self {
            allowed_users,
            mempool_tx,
            shard_stores,
            block_store,
            snapshot_config,
            fc_network,
            statsd_client,
        }
    }

    pub fn enabled(&self) -> bool {
        !self.allowed_users.is_empty()
    }

    async fn backup_and_upload(
        fc_network: FarcasterNetwork,
        snapshot_config: storage::db::snapshot::Config,
        shard_id: u32,
        db: Arc<RocksDB>,
        now: i64,
        statsd_client: StatsdClientWrapper,
    ) -> Result<(), Status> {
        // TODO(aditi): Eventually, we should upload a metadata file. For now, just clear all existing snapshots on s3 and only keep 1 snapshot per shard
        clear_snapshots(fc_network, &snapshot_config, shard_id)
            .await
            .map_err(|err| Status::from_error(Box::new(err)))?;
        let backup_dir = snapshot_config.backup_dir.clone();
        let tar_gz_path = RocksDB::backup_db(db, &backup_dir, shard_id, now)
            .map_err(|err| Status::from_error(Box::new(err)))?;
        storage::db::snapshot::upload_to_s3(
            fc_network,
            tar_gz_path,
            &snapshot_config,
            shard_id,
            &statsd_client,
        )
        .await
        .map_err(|err| Status::from_error(Box::new(err)))?;
        Ok(())
    }
}

#[tonic::async_trait]
impl AdminService for MyAdminService {
    // This should probably go in a separate "DebugService" that's not mounted for production

    // async fn submit_on_chain_event(
    //     &self,
    //     request: Request<OnChainEvent>,
    // ) -> Result<Response<OnChainEvent>, Status> {
    //     info!("Received call to [submit_on_chain_event] RPC");
    //
    //     let onchain_event = request.into_inner();
    //
    //     let fid = onchain_event.fid;
    //     if fid == 0 {
    //         return Err(Status::invalid_argument(
    //             "no fid or invalid fid".to_string(),
    //         ));
    //     }
    //
    //     let result = self.mempool_tx.try_send((
    //         MempoolMessage::ValidatorMessage(ValidatorMessage {
    //             on_chain_event: Some(onchain_event.clone()),
    //             fname_transfer: None,
    //         }),
    //         MempoolSource::RPC,
    //     ));
    //
    //     match result {
    //         Ok(()) => {
    //             let response = Response::new(onchain_event);
    //             Ok(response)
    //         }
    //         Err(err) => Err(Status::from_error(Box::new(err))),
    //     }
    // }
    //
    // async fn submit_user_name_proof(
    //     &self,
    //     request: Request<UserNameProof>,
    // ) -> Result<Response<UserNameProof>, Status> {
    //     info!("Received call to [submit_user_name_proof] RPC");
    //
    //     let username_proof = request.into_inner();
    //
    //     let fid = username_proof.fid;
    //     if fid == 0 {
    //         return Err(Status::invalid_argument(
    //             "no fid or invalid fid".to_string(),
    //         ));
    //     }
    //
    //     let result = self.mempool_tx.try_send((
    //         MempoolMessage::ValidatorMessage(ValidatorMessage {
    //             on_chain_event: None,
    //             fname_transfer: Some(FnameTransfer {
    //                 id: username_proof.fid,
    //                 from_fid: 0, // Assume the username is being transfer from the "root" fid to the one in the username proof
    //                 proof: Some(username_proof.clone()),
    //             }),
    //         }),
    //         MempoolSource::RPC,
    //     ));
    //
    //     match result {
    //         Ok(()) => {
    //             let response = Response::new(username_proof);
    //             Ok(response)
    //         }
    //         Err(err) => Err(Status::from_error(Box::new(err))),
    //     }
    // }

    async fn upload_snapshot(
        &self,
        request: Request<Empty>,
    ) -> std::result::Result<Response<Empty>, Status> {
        authenticate_request(&request, &self.allowed_users)?;

        if std::fs::exists(self.snapshot_config.backup_dir.clone())? {
            return Err(Status::aborted("snapshot already in progress"));
        }

        let fc_network = self.fc_network.clone();
        let snapshot_config = self.snapshot_config.clone();
        let shard_stores = self.shard_stores.clone();
        let block_store = self.block_store.clone();
        let statsd_client = self.statsd_client.clone();
        tokio::spawn(async move {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis();

            if let Err(err) = Self::backup_and_upload(
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

            for (shard, stores) in shard_stores.iter() {
                if let Err(err) = Self::backup_and_upload(
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

            if let Err(err) = std::fs::remove_dir_all(snapshot_config.backup_dir.clone()) {
                info!("Unable to remove snapshot directory: {}", err.to_string());
            }
        });

        Ok(Response::new(Empty {}))
    }
}
