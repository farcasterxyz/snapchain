use std::sync::Arc;

use crate::proto::FnameState;
use crate::proto::OnChainEventState;
use crate::storage::constants::RootPrefix;
use crate::storage::db::RocksDB;
use crate::storage::db::RocksdbError;
use prost::DecodeError;
use prost::Message;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum IngestStateError {
    #[error(transparent)]
    DecodeError(#[from] DecodeError),

    #[error(transparent)]
    RocksdbError(#[from] RocksdbError),
}

#[derive(Clone)]
pub struct LocalStateStore {
    db: Arc<RocksDB>,
}

pub enum DataType {
    OnchainEvent = 1,
    FnameTransfer = 2,
}

impl LocalStateStore {
    pub fn new(db: Arc<RocksDB>) -> Self {
        LocalStateStore { db }
    }

    fn make_onchain_event_primary_key() -> Vec<u8> {
        vec![
            RootPrefix::NodeLocalState as u8,
            DataType::OnchainEvent as u8,
        ]
    }

    pub fn set_latest_block_number(&self, block_number: u64) -> Result<(), IngestStateError> {
        Ok(self.db.put(
            &Self::make_onchain_event_primary_key(),
            &OnChainEventState {
                last_l2_block: block_number,
            }
            .encode_to_vec(),
        )?)
    }

    pub fn get_latest_block_number(&self) -> Result<Option<u64>, IngestStateError> {
        match self.db.get(&Self::make_onchain_event_primary_key())? {
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

    pub fn set_latest_fname_transfer_id(&self, transfer_id: u64) -> Result<(), IngestStateError> {
        Ok(self.db.put(
            &Self::make_fname_transfer_primary_key(),
            &FnameState {
                last_fname_proof: transfer_id,
            }
            .encode_to_vec(),
        )?)
    }

    pub fn get_latest_fname_transfer_id(&self) -> Result<Option<u64>, IngestStateError> {
        match self.db.get(&Self::make_fname_transfer_primary_key())? {
            Some(state) => Ok(Some(FnameState::decode(state.as_slice())?.last_fname_proof)),
            None => Ok(None),
        }
    }
}
