use crate::mempool::mempool::{MempoolMessageWithSource, MempoolSource};
use crate::proto::admin_service_server::AdminService;
use crate::proto::{self, Empty, FarcasterNetwork, FnameTransfer, OnChainEvent};
use crate::proto::{UserNameProof, ValidatorMessage};
use crate::storage;
use crate::storage::db::snapshot::clear_snapshots;
use crate::storage::db::RocksDB;
use crate::storage::store::engine::MempoolMessage;
use crate::storage::store::stores::Stores;
use crate::storage::store::BlockStore;
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use rocksdb;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{io, path, process};
use thiserror::Error;
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};
use tracing::{error, info, warn};

pub struct DbManager {
    db_dir: String,
    admin_db_dir: String,
    db: Option<rocksdb::TransactionDB>,
}

impl DbManager {
    pub fn new(db_dir: &str) -> Self {
        let admin_db_dir = path::Path::new(db_dir)
            .join("admin")
            .to_string_lossy()
            .into_owned();

        Self {
            db_dir: db_dir.to_string(),
            admin_db_dir,
            db: None,
        }
    }

    pub fn maybe_destroy_databases(&mut self) -> Result<(), AdminServiceError> {
        let db = rocksdb::TransactionDB::open_default(&self.admin_db_dir)?;
        if db.get(DB_DESTROY_KEY)?.is_some() {
            db.delete(DB_DESTROY_KEY)?; // we're about to remove but do this anyway
            drop(db);
            warn!(db_dir = &self.db_dir, "destroying all databases");
            std::fs::remove_dir_all(&self.db_dir)?;
            let db = rocksdb::TransactionDB::open_default(&self.admin_db_dir)?;
            self.db.replace(db);
        } else {
            self.db.replace(db);
        }

        Ok(())
    }

    fn schedule_destruction(&self) -> Result<(), Status> {
        if let Some(ref db) = self.db {
            db.put(DB_DESTROY_KEY, &[]).map_err(|err| {
                Status::internal(format!(
                    "failed to schedule destruction of databases: {}",
                    err,
                ))
            })
        } else {
            Err(Status::internal("admin database is not open"))
        }
    }
}

pub struct MyAdminService {
    db_manager: DbManager,
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

const DB_DESTROY_KEY: &[u8] = b"__destroy_all_databases_on_start__";

impl MyAdminService {
    pub fn new(
        db_manager: DbManager,
        mempool_tx: mpsc::Sender<MempoolMessageWithSource>,
        shard_stores: HashMap<u32, Stores>,
        block_store: BlockStore,
        snapshot_config: storage::db::snapshot::Config,
        fc_network: FarcasterNetwork,
        statsd_client: StatsdClientWrapper,
    ) -> Self {
        Self {
            db_manager,
            mempool_tx,
            shard_stores,
            block_store,
            snapshot_config,
            fc_network,
            statsd_client,
        }
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
    async fn terminate(
        &self,
        request: Request<proto::TerminateRequest>,
    ) -> Result<Response<proto::TerminateResponse>, Status> {
        let destroy_database = request.get_ref().destroy_database;

        if destroy_database {
            if let Err(err) = self.db_manager.schedule_destruction() {
                const TEXT: &str = "failed to schedule database destruction";
                warn!(err = err.to_string(), TEXT);
                return Err(Status::internal(format!("{}: {}", TEXT, err)));
            }
        }

        tokio::spawn(async move {
            warn!(destroy_database, "terminate scheduled");

            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            process::exit(0);
        });

        let response = Response::new(proto::TerminateResponse {});
        Ok(response)
    }

    async fn submit_on_chain_event(
        &self,
        request: Request<OnChainEvent>,
    ) -> Result<Response<OnChainEvent>, Status> {
        info!("Received call to [submit_on_chain_event] RPC");

        let onchain_event = request.into_inner();

        let fid = onchain_event.fid;
        if fid == 0 {
            return Err(Status::invalid_argument(
                "no fid or invalid fid".to_string(),
            ));
        }

        let result = self.mempool_tx.try_send((
            MempoolMessage::ValidatorMessage(ValidatorMessage {
                on_chain_event: Some(onchain_event.clone()),
                fname_transfer: None,
            }),
            MempoolSource::RPC,
        ));

        match result {
            Ok(()) => {
                let response = Response::new(onchain_event);
                Ok(response)
            }
            Err(err) => Err(Status::from_error(Box::new(err))),
        }
    }

    async fn submit_user_name_proof(
        &self,
        request: Request<UserNameProof>,
    ) -> Result<Response<UserNameProof>, Status> {
        info!("Received call to [submit_user_name_proof] RPC");

        let username_proof = request.into_inner();

        let fid = username_proof.fid;
        if fid == 0 {
            return Err(Status::invalid_argument(
                "no fid or invalid fid".to_string(),
            ));
        }

        let result = self.mempool_tx.try_send((
            MempoolMessage::ValidatorMessage(ValidatorMessage {
                on_chain_event: None,
                fname_transfer: Some(FnameTransfer {
                    id: username_proof.fid,
                    from_fid: 0, // Assume the username is being transfer from the "root" fid to the one in the username proof
                    proof: Some(username_proof.clone()),
                }),
            }),
            MempoolSource::RPC,
        ));

        match result {
            Ok(()) => {
                let response = Response::new(username_proof);
                Ok(response)
            }
            Err(err) => Err(Status::from_error(Box::new(err))),
        }
    }

    async fn upload_snapshot(
        &self,
        _request: Request<Empty>,
    ) -> std::result::Result<Response<Empty>, Status> {
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
