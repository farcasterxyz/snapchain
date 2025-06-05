use std::sync::Arc;

use crate::core::error::HubError;
use crate::proto::FnameState;
use crate::proto::FullProposal;
use crate::proto::Height;
use crate::proto::OnChainEventState;
use crate::storage::constants::RootPrefix;
use crate::storage::db::PageOptions;
use crate::storage::db::RocksDB;
use crate::storage::db::RocksdbError;
use crate::storage::util::increment_vec_u8;
use informalsystems_malachitebft_core_types::Round;
use prost::DecodeError;
use prost::Message;
use thiserror::Error;
use tracing::error;

#[derive(Error, Debug)]
pub enum LocalStateError {
    #[error(transparent)]
    DecodeError(#[from] DecodeError),

    #[error(transparent)]
    RocksdbError(#[from] RocksdbError),

    #[error(transparent)]
    HubError(#[from] HubError),
}

#[derive(Clone)]
pub struct LocalStateStore {
    db: Arc<RocksDB>,
}

pub enum DataType {
    OptimismOnchainEvent = 1,
    FnameTransfer = 2,
    BaseOnchainEvent = 3,
}

#[derive(Clone, strum_macros::Display)]
pub enum Chain {
    Optimism = 1,
    Base = 2,
}

impl LocalStateStore {
    pub fn new(db: Arc<RocksDB>) -> Self {
        LocalStateStore { db }
    }

    fn make_onchain_event_primary_key(chain: Chain) -> Vec<u8> {
        vec![
            RootPrefix::NodeLocalState as u8,
            match chain {
                Chain::Optimism => DataType::OptimismOnchainEvent as u8,
                Chain::Base => DataType::BaseOnchainEvent as u8,
            },
        ]
    }

    fn make_proposal_key(shard_index: u32, height: u64, round: i64) -> Vec<u8> {
        let mut key = Self::make_height_prefix(shard_index, height);
        key.extend_from_slice(&round.to_be_bytes());
        key
    }

    fn make_height_prefix(shard_index: u32, height: u64) -> Vec<u8> {
        let mut key = vec![RootPrefix::Block as u8];
        key.extend_from_slice(&shard_index.to_be_bytes());
        key.extend_from_slice(&height.to_be_bytes());
        key
    }

    pub fn put_proposal(&self, proposal: FullProposal) -> Result<(), LocalStateError> {
        let height = proposal.height();
        let round = proposal.round();
        let shard_index = proposal.shard_id().unwrap();
        let primary_key = Self::make_proposal_key(shard_index, height.as_u64(), round.as_i64());
        self.db.put(&primary_key, &proposal.encode_to_vec())?;
        Ok(())
    }

    pub fn delete_proposals(
        &self,
        shard_index: u32,
        height: Height,
    ) -> Result<(), LocalStateError> {
        let start_prefix = Self::make_height_prefix(shard_index, height.as_u64());
        let stop_prefix = increment_vec_u8(&start_prefix);
        self.db.for_each_iterator_by_prefix(
            Some(start_prefix),
            Some(stop_prefix),
            &PageOptions::default(),
            |key, _| {
                self.db.del(&key)?;
                Ok(false)
            },
        )?;
        Ok(())
    }

    pub fn get_proposal(
        &self,
        shard_index: u32,
        height: Height,
        round: Round,
    ) -> Result<Option<FullProposal>, LocalStateError> {
        let proposal_key = Self::make_proposal_key(shard_index, height.as_u64(), round.as_i64());
        let proposal = self.db.get(&proposal_key)?;
        match proposal {
            None => Ok(None),
            Some(proposal) => {
                let proposal = FullProposal::decode(proposal.as_slice()).map_err(|e| {
                    error!("Error decoding full proposal: {:?}", e);
                    LocalStateError::DecodeError(e)
                })?;
                Ok(Some(proposal))
            }
        }
    }

    pub fn set_latest_block_number(
        &self,
        chain: Chain,
        block_number: u64,
    ) -> Result<(), LocalStateError> {
        Ok(self.db.put(
            &Self::make_onchain_event_primary_key(chain),
            &OnChainEventState {
                last_l2_block: block_number,
            }
            .encode_to_vec(),
        )?)
    }

    pub fn get_latest_block_number(&self, chain: Chain) -> Result<Option<u64>, LocalStateError> {
        match self.db.get(&Self::make_onchain_event_primary_key(chain))? {
            Some(state) => Ok(Some(
                OnChainEventState::decode(state.as_slice())?.last_l2_block,
            )),
            None => Ok(None),
        }
    }

    fn make_fname_transfer_primary_key() -> Vec<u8> {
        vec![
            RootPrefix::NodeLocalState as u8,
            DataType::FnameTransfer as u8,
        ]
    }

    pub fn set_latest_fname_transfer_id(&self, transfer_id: u64) -> Result<(), LocalStateError> {
        Ok(self.db.put(
            &Self::make_fname_transfer_primary_key(),
            &FnameState {
                last_fname_proof: transfer_id,
            }
            .encode_to_vec(),
        )?)
    }

    pub fn get_latest_fname_transfer_id(&self) -> Result<Option<u64>, LocalStateError> {
        match self.db.get(&Self::make_fname_transfer_primary_key())? {
            Some(state) => Ok(Some(FnameState::decode(state.as_slice())?.last_fname_proof)),
            None => Ok(None),
        }
    }
}
