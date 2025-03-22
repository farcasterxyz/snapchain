use super::super::constants::PAGE_SIZE_MAX;
use crate::core::error::HubError;
use crate::proto;
use crate::proto::Block;
use crate::storage::constants::RootPrefix;
use crate::storage::db::{PageOptions, RocksDB, RocksdbError};
use prost::Message;
use std::sync::Arc;
use thiserror::Error;
use tokio::time::Duration;
use tracing::{error, info};

// TODO(aditi): This code definitely needs unit tests
#[derive(Error, Debug)]
pub enum BlockStorageError {
    #[error(transparent)]
    RocksdbError(#[from] RocksdbError),

    #[error("Block missing from storage")]
    BlockMissing,

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

#[inline]
fn make_block_key(block_number: u64) -> Vec<u8> {
    // Store the prefix in the first byte so there's no overlap across different stores
    let mut key = vec![RootPrefix::Block as u8];
    // Store the block number in the next 8 bytes
    key.extend_from_slice(&block_number.to_be_bytes());

    key
}

#[inline]
fn make_block_timestamp_index(shard_index: u32, timestamp: u64) -> Vec<u8> {
    let mut key = vec![RootPrefix::BlockIndex as u8];
    key.extend_from_slice(&shard_index.to_be_bytes());
    key.extend_from_slice(&timestamp.to_be_bytes());
    key
}

fn get_block_page_by_prefix(
    db: &RocksDB,
    page_options: &PageOptions,
    start_prefix: Option<Vec<u8>>,
    stop_prefix: Option<Vec<u8>>,
) -> Result<BlockPage, BlockStorageError> {
    let mut blocks = Vec::new();
    let mut last_key = vec![];

    let start_prefix = match start_prefix {
        None => make_block_key(0),
        Some(key) => key,
    };

    let stop_prefix = match stop_prefix {
        None => {
            // Covers everything up to the end of the shard keys
            vec![RootPrefix::Block as u8 + 1]
        }
        Some(key) => key,
    };

    db.for_each_iterator_by_prefix_paged(
        Some(start_prefix),
        Some(stop_prefix),
        page_options,
        |key, value| {
            let block = Block::decode(value).map_err(|e| HubError::from(e))?;
            blocks.push(block);

            if blocks.len() >= page_options.page_size.unwrap_or(PAGE_SIZE_MAX) {
                last_key = key.to_vec();
                return Ok(true); // Stop iterating
            }

            Ok(false) // Continue iterating
        },
    )
    .map_err(|_| BlockStorageError::TooManyBlocksInResult)?; // TODO: Return the right error

    let next_page_token = if last_key.len() > 0 {
        Some(last_key)
    } else {
        None
    };

    Ok(BlockPage {
        blocks,
        next_page_token,
    })
}

enum FirstOrLast {
    First,
    Last,
}

fn get_first_or_last_block(
    db: &RocksDB,
    first_or_last: FirstOrLast,
) -> Result<Option<Block>, BlockStorageError> {
    let start_block_key = make_block_key(0);
    let block_page = get_block_page_by_prefix(
        db,
        &PageOptions {
            reverse: match first_or_last {
                FirstOrLast::First => false,
                FirstOrLast::Last => true,
            },
            page_size: Some(1),
            page_token: None,
        },
        Some(start_block_key),
        None,
    )?;

    if block_page.blocks.len() > 1 {
        return Err(BlockStorageError::TooManyBlocksInResult);
    }

    Ok(block_page.blocks.get(0).cloned())
}

pub fn get_current_header(db: &RocksDB) -> Result<Option<proto::BlockHeader>, BlockStorageError> {
    let last_block = get_first_or_last_block(db, FirstOrLast::Last)?;
    match last_block {
        None => Ok(None),
        Some(block) => Ok(block.header),
    }
}

pub fn get_blocks_in_range(
    db: &RocksDB,
    page_options: &PageOptions,
    start_block_number: u64,
    stop_block_number: Option<u64>,
) -> Result<BlockPage, BlockStorageError> {
    let start_primary_key = make_block_key(start_block_number);
    let stop_prefix = stop_block_number.map(|block_number| make_block_key(block_number));

    get_block_page_by_prefix(db, page_options, Some(start_primary_key), stop_prefix)
}

pub fn put_block(db: &RocksDB, block: &Block) -> Result<(), BlockStorageError> {
    let mut txn = db.txn();
    let header = block
        .header
        .as_ref()
        .ok_or(BlockStorageError::BlockMissingHeader)?;
    let height = header
        .height
        .as_ref()
        .ok_or(BlockStorageError::BlockMissingHeight)?;
    let primary_key = make_block_key(height.block_number);
    txn.put(primary_key.clone(), block.encode_to_vec());

    let timestamp_index_key = make_block_timestamp_index(0, header.timestamp);

    if db.get(&timestamp_index_key)? == None {
        txn.put(timestamp_index_key, primary_key);
    }

    db.commit(txn)?;
    Ok(())
}

#[derive(Default, Clone)]
pub struct BlockStore {
    pub db: Arc<RocksDB>,
}

impl BlockStore {
    pub fn new(db: Arc<RocksDB>) -> BlockStore {
        BlockStore { db }
    }

    #[inline]
    pub fn put_block(&self, block: &Block) -> Result<(), BlockStorageError> {
        put_block(&self.db, block)
    }

    #[inline]
    pub fn get_first_block(&self) -> Result<Option<Block>, BlockStorageError> {
        get_first_or_last_block(&self.db, FirstOrLast::First)
    }

    #[inline]
    pub fn get_last_block(&self) -> Result<Option<Block>, BlockStorageError> {
        get_first_or_last_block(&self.db, FirstOrLast::Last)
    }

    #[inline]
    pub fn get_block_by_height(&self, height: u64) -> Result<Option<Block>, BlockStorageError> {
        let block_key = make_block_key(height);
        let block = self.db.get(&block_key)?;
        match block {
            None => Ok(None),
            Some(block) => {
                let block = Block::decode(block.as_slice()).map_err(|e| {
                    error!("Error decoding shard chunk: {:?}", e);
                    BlockStorageError::DecodeError(e)
                })?;
                Ok(Some(block))
            }
        }
    }

    #[inline]
    pub fn min_block_number(&self) -> Result<u64, BlockStorageError> {
        let first_block = self.get_first_block()?;
        match first_block {
            None => Err(BlockStorageError::BlockMissing),
            Some(block) => match block.header {
                None => Err(BlockStorageError::BlockMissingHeader),
                Some(header) => match header.height {
                    None => Err(BlockStorageError::BlockMissingHeight),
                    Some(height) => Ok(height.block_number),
                },
            },
        }
    }

    #[inline]
    pub fn max_block_number(&self) -> Result<u64, BlockStorageError> {
        let current_header = get_current_header(&self.db)?;
        match current_header {
            None => Ok(0),
            Some(header) => match header.height {
                None => Ok(0),
                Some(height) => Ok(height.block_number),
            },
        }
    }

    #[inline]
    pub fn max_block_timestamp(&self) -> Result<u64, BlockStorageError> {
        let current_header = get_current_header(&self.db)?;
        match current_header {
            None => Ok(0),
            Some(header) => Ok(header.timestamp),
        }
    }

    #[inline]
    pub fn get_blocks(
        &self,
        start_block_number: u64,
        stop_block_number: Option<u64>,
        page_options: &PageOptions,
    ) -> Result<BlockPage, BlockStorageError> {
        get_blocks_in_range(
            &self.db,
            page_options,
            start_block_number,
            stop_block_number,
        )
    }

    // Returns the next block height with a timestamp greater than or equal to
    // the given timestamp for the specified shard index.
    pub fn get_next_height_by_timestamp(
        &self,
        timestamp: u64,
    ) -> Result<Option<u64>, BlockStorageError> {
        let shard_index = 0; // Block store uses shard index 0.
        let timestamp_index_key = make_block_timestamp_index(shard_index, timestamp);
        self.db
            .get_next_by_index(vec![RootPrefix::BlockIndex as u8], timestamp_index_key)
            .map_err(|_| BlockStorageError::TooManyBlocksInResult)? // TODO: Return the right error
            .map(|bytes| {
                let block = Block::decode(bytes.as_slice())
                    .map_err(|e| BlockStorageError::DecodeError(e))?;
                let header = block
                    .header
                    .as_ref()
                    .ok_or(BlockStorageError::BlockMissingHeader)?;
                let height = header
                    .height
                    .as_ref()
                    .ok_or(BlockStorageError::BlockMissingHeight)?;
                Ok(height.block_number)
            })
            .transpose()
    }

    // Prune blocks with height less than stop_height. Returns the total number
    // of blocks pruned. Sleeps after each page for the throttle duration and
    // will stop if a shutdown is requested.
    pub async fn prune_until(
        &self,
        stop_height: u64,
        page_options: &PageOptions,
        throttle: Duration,
    ) -> Result<u32, BlockStorageError> {
        let total_pruned = self
            .db
            .delete_paginated(
                Some(make_block_key(0)),
                Some(make_block_key(stop_height)),
                page_options,
                throttle,
                Some(|total_pruned: u32| {
                    info!("Pruning blocks... pruned: {}", total_pruned);
                }),
            )
            .await
            .map_err(|_| BlockStorageError::TooManyBlocksInResult)?; // TODO: Return the right error
        info!("Pruning complete. blocks pruned: {}", total_pruned);
        Ok(total_pruned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{BlockHeader, Height};

    fn make_db(dir: &tempfile::TempDir, filename: &str) -> Arc<RocksDB> {
        let db_path = dir.path().join(filename);

        let db = Arc::new(RocksDB::new(db_path.to_str().unwrap()));
        db.open().unwrap();

        db
    }

    fn make_block(block_number: u64, timestamp: u64) -> Block {
        let header = BlockHeader {
            height: Some(Height {
                shard_index: 0,
                block_number,
            }),
            timestamp,
            ..BlockHeader::default()
        };
        Block {
            header: Some(header),
            ..Block::default()
        }
    }

    fn setup_db(blocks: u64) -> BlockStore {
        let blocks_dir = tempfile::tempdir().unwrap();
        let db = make_db(&blocks_dir, "test_db");
        let store = BlockStore::new(db);

        let number_to_timestamp = |n| n * 100;
        // Add some blocks to the db for testing
        (1..=blocks).for_each(|i| {
            let block = make_block(i, number_to_timestamp(i));
            store.put_block(&block).unwrap();
        });

        store
    }

    #[test]
    fn test_get_next_height_by_timestamp() {
        let store = setup_db(100);
        let timestamp = 500;
        let next_height = store
            .get_next_height_by_timestamp(timestamp)
            .expect("Failed to get next height by timestamp")
            .expect("Expected a valid height");
        assert_eq!(5, next_height);

        let timestamp = 450;
        let next_height = store
            .get_next_height_by_timestamp(timestamp)
            .expect("Failed to get next height by timestamp")
            .expect("Expected a valid height");
        assert_eq!(5, next_height);
    }

    #[tokio::test]
    async fn test_prune_until() {
        let store = setup_db(100);

        let stop_height = 42;
        let page_size = 10;
        let page_options = PageOptions {
            page_size: Some(page_size),
            ..PageOptions::default()
        };
        let pruned = store
            .prune_until(stop_height, &page_options, Duration::ZERO)
            .await
            .expect("Failed to prune blocks");

        assert_eq!((stop_height - 1) as u32, pruned);
        assert_eq!(stop_height, store.min_block_number().unwrap());

        // Check that get_blocks does not error after pruning
        let blocks = store
            .get_blocks(1, Some(stop_height), &PageOptions::default())
            .expect("Failed to get blocks after pruning");
        assert_eq!(0, blocks.blocks.len());

        let blocks = store
            .get_blocks(stop_height, Some(stop_height + 1), &PageOptions::default())
            .expect("Failed to get blocks after pruning");
        assert_eq!(1, blocks.blocks.len());

        let blocks = store
            .get_blocks(1, Some(stop_height + 1), &PageOptions::default())
            .expect("Failed to get blocks after pruning");
        assert_eq!(1, blocks.blocks.len());
    }
}
