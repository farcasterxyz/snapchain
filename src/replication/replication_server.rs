use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::mempool::routing;
use crate::proto;
use crate::replication::replicator::Replicator;

pub struct ReplicationServer {
    replicator: Arc<Replicator>,
    message_router: Box<dyn routing::MessageRouter>,
    num_shards: u32,
}

impl ReplicationServer {
    const MESSAGE_LIMIT: usize = 1_000; // Maximum number of messages to fetch per page

    pub fn new(
        replicator: Arc<Replicator>,
        message_router: Box<dyn routing::MessageRouter>,
        num_shards: u32,
    ) -> Self {
        ReplicationServer {
            replicator,
            message_router,
            num_shards,
        }
    }
}

#[tonic::async_trait]
impl proto::replication_service_server::ReplicationService for ReplicationServer {
    async fn get_shard_snapshot_metadata(
        &self,
        request: Request<proto::GetShardSnapshotMetadataRequest>,
    ) -> Result<Response<proto::GetShardSnapshotMetadataResponse>, Status> {
        let request = request.into_inner();

        let snapshots = match self.replicator.get_snapshot_metadata(request.shard_id) {
            Ok(metadata) => metadata
                .into_iter()
                .map(|(height, timestamp)| proto::ShardSnapshotMetadata {
                    shard_id: request.shard_id,
                    height,
                    timestamp,
                })
                .collect(),
            Err(e) => {
                return Err(Status::internal(format!(
                    "Failed to get snapshot metadata: {}",
                    e
                )));
            }
        };

        Ok(Response::new(proto::GetShardSnapshotMetadataResponse {
            snapshots,
        }))
    }

    async fn get_shard_transactions(
        &self,
        request: Request<proto::GetShardTransactionsRequest>,
    ) -> Result<Response<proto::GetShardTransactionsResponse>, Status> {
        let request = request.into_inner();

        let results = self.replicator.transactions_for_shard_and_height(
            request.shard_id,
            request.height,
            request.page_token.clone(),
            Self::MESSAGE_LIMIT,
        );

        let response = match results {
            Ok((transactions, page_token)) => proto::GetShardTransactionsResponse {
                transactions,
                next_page_token: page_token,
            },
            Err(e) => {
                return Err(Status::internal(format!(
                    "Failed to get transactions: {}",
                    e
                )));
            }
        };

        Ok(Response::new(response))
    }

    // IMPORTANT: this is a temporary endpoint for debugging purposes only. It will eventually be
    // removed, and SHOULD NOT be used for production purposes.
    async fn get_replication_transactions_by_fid(
        &self,
        request: Request<proto::GetReplicationTransactionsByFidRequest>,
    ) -> Result<Response<proto::GetReplicationTransactionsByFidResponse>, Status> {
        let request = request.into_inner();
        let shard = self.message_router.route_fid(request.fid, self.num_shards);
        let transaction = self
            .replicator
            .latest_transactions_for_fid(shard, request.fid)?;

        Ok(Response::new(
            proto::GetReplicationTransactionsByFidResponse {
                transaction: transaction,
                ..Default::default()
            },
        ))
    }
}
