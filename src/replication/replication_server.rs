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

        if request.trie_virtual_shard > u8::MAX as u32 {
            return Err(Status::invalid_argument(format!(
                "trie_virtual_shard {} is out of range",
                request.trie_virtual_shard
            )));
        }

        let results = self.replicator.transactions_for_trie_prefix(
            request.shard_id,
            request.height,
            request.trie_virtual_shard as u8,
            request.page_token.clone(),
        );

        match results {
            Ok(r) => Ok(Response::new(r)),
            Err(e) => {
                return Err(Status::internal(format!(
                    "Failed to get transactions: {}",
                    e
                )));
            }
        }
    }
}
