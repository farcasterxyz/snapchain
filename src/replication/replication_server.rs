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
    const FID_RANGE: u64 = 1_000;

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

    fn parse_page_token(page_token: Vec<u8>) -> Result<u64, Status> {
        if page_token.is_empty() {
            return Ok(1);
        }
        if page_token.len() != 8 {
            return Err(Status::invalid_argument("Invalid page token length"));
        }
        let mut buf = [0; 8];
        buf.copy_from_slice(page_token.as_slice());
        Ok(u64::from_be_bytes(buf))
    }
}

#[tonic::async_trait]
impl proto::replication_service_server::ReplicationService for ReplicationServer {
    async fn get_replication_transactions(
        &self,
        request: Request<proto::GetReplicationTransactionsRequest>,
    ) -> Result<Response<proto::GetReplicationTransactionsResponse>, Status> {
        let request = request.into_inner();
        let start_fid: u64 = Self::parse_page_token(request.page_token)?;
        let end_fid = start_fid + Self::FID_RANGE - 1;

        let (sys, user) = self.replicator.transactions_for_fid_range(
            request.block_number,
            request.shard_id,
            start_fid,
            end_fid,
        )?;

        let next_page_token = if sys.is_empty() {
            vec![]
        } else {
            let next_fid = end_fid + 1;
            next_fid.to_be_bytes().to_vec()
        };

        Ok(Response::new(proto::GetReplicationTransactionsResponse {
            system_transactions: sys,
            user_transactions: user,
            next_page_token,
            ..Default::default()
        }))
    }

    // IMPORTANT: this is a temporary endpoint for debugging purposes only. It will eventually be
    // removed, and SHOULD NOT be used for production purposes.
    async fn get_replication_transactions_by_fid(
        &self,
        request: Request<proto::GetReplicationTransactionsByFidRequest>,
    ) -> Result<Response<proto::GetReplicationTransactionsByFidResponse>, Status> {
        let request = request.into_inner();
        let shard = self.message_router.route_fid(request.fid, self.num_shards);
        let (sys, user) = self
            .replicator
            .latest_transactions_for_fid(shard, request.fid)?;

        Ok(Response::new(
            proto::GetReplicationTransactionsByFidResponse {
                system_transaction: sys,
                user_transaction: user,
                ..Default::default()
            },
        ))
    }
}
