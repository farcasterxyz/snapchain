use crate::core::error::HubError;
use crate::proto::BlockEvent;
use crate::storage::constants::{RootPrefix, PAGE_SIZE_MAX};
use crate::storage::db::{PageOptions, RocksDB, RocksDbTransactionBatch, RocksdbError};
use prost::Message;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum BlockEventStorageError {
    #[error(transparent)]
    RocksdbError(#[from] RocksdbError),

    #[error(transparent)]
    HubError(#[from] HubError),

    #[error("Too many blocks in result")]
    TooManyBlocksInResult,

    #[error("Error decoding shard chunk")]
    DecodeError(#[from] prost::DecodeError),

    #[error("Event missing body")]
    EventMissingBody,
}

/** A page of messages returned from various APIs */
pub struct BlockEventPage {
    pub block_events: Vec<BlockEvent>,
    pub next_page_token: Option<Vec<u8>>,
}

fn make_block_event_key(seqnum: u64) -> Vec<u8> {
    // Store the prefix in the first byte so there's no overlap across different stores
    let mut key = vec![RootPrefix::BlockEvent as u8];
    // Store the sequence number in the next 8 bytes
    key.extend_from_slice(&seqnum.to_be_bytes());

    key
}

fn get_block_page_by_prefix(
    db: &RocksDB,
    page_options: &PageOptions,
    start_prefix: Option<Vec<u8>>,
    stop_prefix: Option<Vec<u8>>,
) -> Result<BlockEventPage, BlockEventStorageError> {
    let mut block_events = Vec::new();
    let mut last_key = vec![];

    let start_prefix = match start_prefix {
        None => make_block_event_key(0),
        Some(key) => key,
    };

    let stop_prefix = match stop_prefix {
        None => {
            // Covers everything up to the end of the shard keys
            vec![RootPrefix::BlockEvent as u8 + 1]
        }
        Some(key) => key,
    };

    db.for_each_iterator_by_prefix_paged(
        Some(start_prefix),
        Some(stop_prefix),
        page_options,
        |key, value| {
            let block_event = BlockEvent::decode(value).map_err(|e| HubError::from(e))?;
            block_events.push(block_event);

            if block_events.len() >= page_options.page_size.unwrap_or(PAGE_SIZE_MAX) {
                last_key = key.to_vec();
                return Ok(true); // Stop iterating
            }

            Ok(false) // Continue iterating
        },
    )
    .map_err(|_| BlockEventStorageError::TooManyBlocksInResult)?; // TODO: Return the right error

    let next_page_token = if last_key.len() > 0 {
        Some(last_key)
    } else {
        None
    };

    Ok(BlockEventPage {
        block_events,
        next_page_token,
    })
}

fn get_last_block_event(db: &RocksDB) -> Result<Option<BlockEvent>, BlockEventStorageError> {
    let start_block_key = make_block_event_key(0);
    let block_page = get_block_page_by_prefix(
        db,
        &PageOptions {
            reverse: true,
            page_size: Some(1),
            page_token: None,
        },
        Some(start_block_key),
        None,
    )?;

    if block_page.block_events.len() > 1 {
        return Err(BlockEventStorageError::TooManyBlocksInResult);
    }

    Ok(block_page.block_events.get(0).cloned())
}

pub fn put_block_event(
    block_event: &BlockEvent,
    txn: &mut RocksDbTransactionBatch,
) -> Result<(), BlockEventStorageError> {
    let primary_key = make_block_event_key(
        block_event
            .data
            .as_ref()
            .ok_or(BlockEventStorageError::EventMissingBody)?
            .seqnum,
    );
    txn.put(primary_key.clone(), block_event.encode_to_vec());
    Ok(())
}

#[derive(Default, Clone)]
pub struct BlockEventStore {
    pub db: Arc<RocksDB>,
}

impl BlockEventStore {
    pub fn new(db: Arc<RocksDB>) -> BlockEventStore {
        BlockEventStore { db }
    }

    pub fn put_block_event(
        &self,
        block_event: &BlockEvent,
        txn: &mut RocksDbTransactionBatch,
    ) -> Result<(), BlockEventStorageError> {
        put_block_event(block_event, txn)
    }

    pub fn get_block_event_by_seqnum(
        &self,
        seqnum: u64,
    ) -> Result<Option<BlockEvent>, BlockEventStorageError> {
        let key = make_block_event_key(seqnum);
        match self.db.get(&key)? {
            None => Ok(None),
            Some(event) => Ok(Some(BlockEvent::decode(event.as_slice())?)),
        }
    }

    pub fn get_last_block_event(&self) -> Result<Option<BlockEvent>, BlockEventStorageError> {
        get_last_block_event(&self.db)
    }

    pub fn max_seqnum(&self) -> Result<u64, BlockEventStorageError> {
        match get_last_block_event(&self.db)? {
            None => Ok(0),
            Some(event) => Ok(event
                .data
                .as_ref()
                .ok_or(BlockEventStorageError::EventMissingBody)?
                .seqnum),
        }
    }
}
