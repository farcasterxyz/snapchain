use crate::proto::{Block, FullProposal, Height};
use crate::storage::constants::RootPrefix;
use crate::storage::db::{RocksDB, RocksdbError};
use informalsystems_malachitebft_core_types::Round;
use prost::Message;
use std::sync::Arc;
use thiserror::Error;
use tracing::error;

#[derive(Error, Debug)]
pub enum ProposalStorageError {
    #[error(transparent)]
    RocksdbError(#[from] RocksdbError),

    #[error("Block missing header")]
    BlockMissingHeader,

    #[error("Block missing height")]
    BlockMissingHeight,

    #[error("Too many blocks in result")]
    TooManyBlocksInResult,

    #[error("Error decoding shard chunk")]
    DecodeError(#[from] prost::DecodeError),
}

/** A page of messages returned from various APIs */
pub struct BlockPage {
    pub blocks: Vec<Block>,
    pub next_page_token: Option<Vec<u8>>,
}

fn make_proposal_key(height: u64, round: i64) -> Vec<u8> {
    // Store the prefix in the first byte so there's no overlap across different stores
    let mut key = vec![RootPrefix::Block as u8];
    // Store the block number in the next 8 bytes
    key.extend_from_slice(&height.to_be_bytes());
    key.extend_from_slice(&round.to_be_bytes());

    key
}

pub fn put_proposal(db: &RocksDB, proposal: FullProposal) -> Result<(), ProposalStorageError> {
    // TODO: We need to introduce a transaction model
    let mut txn = db.txn();
    let height = proposal.height();
    let round = proposal.round();
    let primary_key = make_proposal_key(height.as_u64(), round.as_i64());
    txn.put(primary_key, proposal.encode_to_vec());
    db.commit(txn)?;
    Ok(())
}

#[derive(Default, Clone)]
pub struct ProposalStore {
    db: Arc<RocksDB>,
}

impl ProposalStore {
    pub fn new(db: Arc<RocksDB>) -> ProposalStore {
        ProposalStore { db }
    }

    pub fn put_proposal(&self, proposal: FullProposal) -> Result<(), ProposalStorageError> {
        put_proposal(&self.db, proposal)
    }

    pub fn delete_proposal(
        &self,
        height: Height,
        round: Round,
    ) -> Result<(), ProposalStorageError> {
        let primary_key = make_proposal_key(height.as_u64(), round.as_i64());
        self.db.del(&primary_key)?;
        Ok(())
    }

    pub fn get_proposal(
        &self,
        height: Height,
        round: Round,
    ) -> Result<Option<FullProposal>, ProposalStorageError> {
        let proposal_key = make_proposal_key(height.as_u64(), round.as_i64());
        let proposal = self.db.get(&proposal_key)?;
        match proposal {
            None => Ok(None),
            Some(proposal) => {
                let proposal = FullProposal::decode(proposal.as_slice()).map_err(|e| {
                    error!("Error decoding full proposal: {:?}", e);
                    ProposalStorageError::DecodeError(e)
                })?;
                Ok(Some(proposal))
            }
        }
    }
}
