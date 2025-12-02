use crate::proto;
use crate::storage::store::block_engine::BlockStores;
use crate::{
    network::replication::replicator::Replicator, utils::statsd_wrapper::StatsdClientWrapper,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct ReplicationServer {
    replicator: Arc<Replicator>,
    block_stores: BlockStores,
    statsd_client: StatsdClientWrapper,
}

impl ReplicationServer {
    pub fn new(
        replicator: Arc<Replicator>,
        block_stores: BlockStores,
        statsd_client: StatsdClientWrapper,
    ) -> Self {
        ReplicationServer {
            replicator,
            block_stores,
            statsd_client,
        }
    }

    /// For shard 0, we return a fixed block number as the "latest" block and ask the client to sync (over p2p)
    /// from that height. We do this because there will be onchain events, storage lending events etc... in shard-0
    /// after this block, and we need the client to get them and merge them properly
    fn get_shard0_block_height(&self) -> u64 {
        match self.replicator.network() {
            proto::FarcasterNetwork::Mainnet => 13396000, // ~Sep 03, 2025
            proto::FarcasterNetwork::Testnet => 13302000, // ~Sep 03, 2025
            _ => 0, // For devnet, we can just return 0 and let the client sync from genesis
        }
    }
}

#[tonic::async_trait]
impl proto::replication_service_server::ReplicationService for ReplicationServer {
    async fn get_shard_snapshot_metadata(
        &self,
        request: Request<proto::GetShardSnapshotMetadataRequest>,
    ) -> Result<Response<proto::GetShardSnapshotMetadataResponse>, Status> {
        self.statsd_client
            .incr("replication.get_shard_snapshot_metadata");

        let request = request.into_inner();

        // We store the snapshots by the data shards, so there's no snapshot for shard-0. That's OK,
        // because for shard-0, we just need the block number to start syncing from, so get it from the block_store.
        let snapshots = if request.shard_id == 0 {
            let block = self
                .block_stores
                .block_store
                .get_block_by_height(self.get_shard0_block_height())
                .map_err(|e| Status::internal(format!("Failed to get block by height: {}", e)))?;

            let block_header = block.as_ref().map(|b| b.header.clone()).flatten();
            let height = block_header
                .as_ref()
                .map(|h| h.height)
                .flatten()
                .map(|h| h.block_number)
                .unwrap_or(0); // Return 0 if no height, so client will sync from genesis (eg. devnet)
            let timestamp = block_header.map(|h| h.timestamp).unwrap_or(0);

            vec![proto::ShardSnapshotMetadata {
                shard_id: request.shard_id,
                height,
                timestamp,
                block,
                shard_chunk: None,
                num_items: 0, // We don't track number of items for shard-0
            }]
        } else {
            // For a regular shard, get it from the replicator
            match self.replicator.get_snapshot_metadata(request.shard_id) {
                Ok(metadatas) => {
                    let mut snapshots = Vec::new();
                    for metadata in metadatas {
                        // Fetch the ShardChunk for the given shard and height from the replicator for non-zero shard_id
                        let shard_chunk = if request.shard_id != 0 {
                            self.replicator
                                .get_shard_chunk_by_height(request.shard_id, metadata.height)
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
                            height: metadata.height,
                            timestamp: metadata.timestamp,
                            num_items: metadata.num_items as u64,
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
        // statsd counter for requests by peer
        self.statsd_client
            .incr("replication.get_shard_transactions");

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
            request.fid,
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
