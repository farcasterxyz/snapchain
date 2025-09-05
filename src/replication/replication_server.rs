use crate::proto;
use crate::replication::replicator::Replicator;
use crate::storage::store::BlockStore;
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct ReplicationServer {
    replicator: Arc<Replicator>,
    block_store: BlockStore,
}

impl ReplicationServer {
    pub fn new(replicator: Arc<Replicator>, block_store: BlockStore) -> Self {
        ReplicationServer {
            replicator,
            block_store,
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

        // We store the snapshots by the data shards, so there's no snapshot for shard-0. That's OK,
        // because for shard-0, we just need the highest block number, so get it from the block_store instead
        let snapshots = if request.shard_id == 0 {
            let block = self
                .block_store
                .get_last_block()
                .map_err(|e| Status::internal(format!("Failed to get block by height: {}", e)))?
                .ok_or(Status::not_found("No blocks found in block store"))?;

            let block_clone = block.clone();

            let block_header = block
                .header
                .ok_or(Status::not_found("No block header found"))?;
            let block_height = block_header
                .height
                .ok_or(Status::not_found("No block height found"))?;

            vec![proto::ShardSnapshotMetadata {
                shard_id: request.shard_id,
                height: block_height.block_number,
                timestamp: block_header.timestamp,
                block: Some(block_clone),
                shard_chunk: None,
            }]
        } else {
            // For a regular shard, get it from the replicator
            match self.replicator.get_snapshot_metadata(request.shard_id) {
                Ok(metadata) => {
                    let mut snapshots = Vec::new();
                    for (height, timestamp) in metadata {
                        // Fetch the ShardChunk for the given shard and height from the replicator for non-zero shard_id
                        let shard_chunk = if request.shard_id != 0 {
                            self.replicator
                                .get_shard_chunk_by_height(request.shard_id, height)
                                .map_err(|e| {
                                    Status::internal(format!(
                                        "Failed to get shard chunk by height: {}",
                                        e
                                    ))
                                })?
                        } else {
                            None
                        };

                        snapshots.push(proto::ShardSnapshotMetadata {
                            shard_id: request.shard_id,
                            height,
                            timestamp,
                            block: None,
                            shard_chunk,
                        });
                    }
                    snapshots
                }
                Err(e) => {
                    return Err(Status::internal(format!(
                        "Failed to get snapshot metadata: {}",
                        e
                    )));
                }
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

        let results = self.replicator.messages_for_trie_prefix(
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
