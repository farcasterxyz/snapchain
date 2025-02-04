use crate::storage::db::RocksdbError;
use prost::DecodeError;
use thiserror::Error;
#[derive(Error, Debug)]
pub enum IngestStateError {
    #[error(transparent)]
    DecodeError(#[from] DecodeError),

    #[error(transparent)]
    RocksdbError(#[from] RocksdbError),
}

pub mod onchain_events {
    use crate::proto::OnChainEventState;
    use crate::storage::constants::RootPrefix;
    use crate::storage::db::RocksDB;
    use prost::Message;

    fn make_primary_key() -> Vec<u8> {
        vec![RootPrefix::OnChainEventState as u8]
    }

    pub fn put_state(
        db: &RocksDB,
        state: OnChainEventState,
    ) -> Result<(), super::IngestStateError> {
        Ok(db.put(&make_primary_key(), &state.encode_to_vec())?)
    }

    pub fn get_state(db: &RocksDB) -> Result<Option<OnChainEventState>, super::IngestStateError> {
        match db.get(&make_primary_key())? {
            Some(hub_state) => Ok(Some(OnChainEventState::decode(hub_state.as_slice())?)),
            None => Ok(None),
        }
    }
}
