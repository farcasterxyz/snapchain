use crate::connectors::onchain_events::OnchainEventsRequest;
use crate::jobs::snapshot_upload::upload_snapshot;
use crate::mempool::mempool::{MempoolRequest, MempoolSource};
use crate::network::rpc_extensions::authenticate_request;
use crate::network::server::MEMPOOL_ADD_REQUEST_TIMEOUT;
use crate::proto::admin_service_server::AdminService;
use crate::proto::{
    self, Empty, FarcasterNetwork, FnameTransfer, OnChainEvent, RetryOnchainEventsRequest,
    UploadSnapshotRequest, UserNameProof, ValidatorMessage,
};
use crate::storage;
use crate::storage::store::engine::MempoolMessage;
use crate::storage::store::stores::Stores;
use crate::storage::store::BlockStore;
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use rocksdb;
use std::collections::HashMap;
use std::io;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use tonic::{Request, Response, Status};
use tracing::{error, info};

pub struct MyAdminService {
    allowed_users: HashMap<String, String>,
    pub mempool_tx: mpsc::Sender<MempoolRequest>,
    onchain_events_request_tx: mpsc::Sender<OnchainEventsRequest>,
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
        mempool_tx: mpsc::Sender<MempoolRequest>,
        onchain_events_request_tx: mpsc::Sender<OnchainEventsRequest>,
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
            onchain_events_request_tx,
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

    // Allow debug operations only on Devnet or Unspecified networks
    fn allow_debug(&self) -> bool {
        matches!(
            self.fc_network,
            FarcasterNetwork::None | FarcasterNetwork::Devnet
        )
    }
}

#[tonic::async_trait]
impl AdminService for MyAdminService {
    // This should probably go in a separate "DebugService" that's not mounted for production

    async fn submit_on_chain_event(
        &self,
        request: Request<OnChainEvent>,
    ) -> Result<Response<OnChainEvent>, Status> {
        info!("Received call to [submit_on_chain_event] RPC");

        if !self.allow_debug() {
            return Err(Status::permission_denied(
                "submit_on_chain_event is not supported on this network".to_string(),
            ));
        }

        let onchain_event = request.into_inner();

        let fid = onchain_event.fid;
        if fid == 0 {
            return Err(Status::invalid_argument(
                "no fid or invalid fid".to_string(),
            ));
        }

        let (tx, rx) = oneshot::channel();
        self.mempool_tx
            .try_send(MempoolRequest::AddMessage(
                MempoolMessage::ValidatorMessage(ValidatorMessage {
                    on_chain_event: Some(onchain_event.clone()),
                    fname_transfer: None,
                }),
                MempoolSource::RPC,
                Some(tx),
            ))
            .map_err(|err| Status::from_error(Box::new(err)))?;

        match timeout(MEMPOOL_ADD_REQUEST_TIMEOUT, rx).await {
            Ok(Ok(_)) => {}
            Ok(Err(err)) => {
                error!(
                    "Error receiving message from mempool channel: {:?}",
                    err.to_string()
                );
                return Err(Status::internal("Error adding to mempool"));
            }
            Err(_) => {
                error!("Timeout receiving message from mempool channel",);
                return Err(Status::internal("Error adding to mempool"));
            }
        };

        let response = Response::new(onchain_event);
        Ok(response)
    }

    async fn submit_user_name_proof(
        &self,
        request: Request<UserNameProof>,
    ) -> Result<Response<UserNameProof>, Status> {
        info!("Received call to [submit_user_name_proof] RPC");

        if !self.allow_debug() {
            return Err(Status::permission_denied(
                "submit_user_name_proof is not supported on this network".to_string(),
            ));
        }

        let username_proof = request.into_inner();

        let fid = username_proof.fid;
        if fid == 0 {
            return Err(Status::invalid_argument(
                "no fid or invalid fid".to_string(),
            ));
        }

        let (tx, rx) = oneshot::channel();
        self.mempool_tx
            .try_send(MempoolRequest::AddMessage(
                MempoolMessage::ValidatorMessage(ValidatorMessage {
                    on_chain_event: None,
                    fname_transfer: Some(FnameTransfer {
                        id: username_proof.fid,
                        from_fid: 0, // Assume the username is being transfer from the "root" fid to the one in the username proof
                        proof: Some(username_proof.clone()),
                    }),
                }),
                MempoolSource::RPC,
                Some(tx),
            ))
            .map_err(|err| Status::from_error(Box::new(err)))?;

        match timeout(MEMPOOL_ADD_REQUEST_TIMEOUT, rx).await {
            Ok(Ok(_)) => {}
            Ok(Err(err)) => {
                error!(
                    "Error receiving message from mempool channel: {:?}",
                    err.to_string()
                );
                return Err(Status::internal("Error adding to mempool"));
            }
            Err(_) => {
                error!("Timeout receiving message from mempool channel",);
                return Err(Status::internal("Error adding to mempool"));
            }
        };

        let response = Response::new(username_proof);
        Ok(response)
    }

    async fn retry_onchain_events(
        &self,
        request: Request<RetryOnchainEventsRequest>,
    ) -> std::result::Result<Response<Empty>, Status> {
        match request.into_inner().kind {
            None => {}
            Some(kind) => match kind {
                proto::retry_onchain_events_request::Kind::Fid(fid) => {
                    self.onchain_events_request_tx
                        .send(OnchainEventsRequest::RetryFid(fid))
                        .await
                        .map_err(|err| Status::from_error(Box::new(err)))?;
                }
                proto::retry_onchain_events_request::Kind::BlockRange(retry_block_number_range) => {
                    self.onchain_events_request_tx
                        .send(OnchainEventsRequest::RetryBlockRange {
                            start_block_number: retry_block_number_range.start_block_number,
                            stop_block_number: retry_block_number_range.stop_block_number,
                        })
                        .await
                        .map_err(|err| Status::from_error(Box::new(err)))?;
                }
            },
        }
        Ok(Response::new(Empty {}))
    }

    async fn upload_snapshot(
        &self,
        request: Request<UploadSnapshotRequest>,
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
        let shard_ids = if request.get_ref().shard_indexes.is_empty() {
            None
        } else {
            Some(request.into_inner().shard_indexes.into_iter().collect())
        };

        tokio::spawn(async move {
            if let Err(err) = upload_snapshot(
                snapshot_config,
                fc_network,
                block_store,
                shard_stores,
                statsd_client,
                shard_ids,
            )
            .await
            {
                error!("Error uploading snapshot {}", err.to_string());
            }
        });

        Ok(Response::new(Empty {}))
    }
}
