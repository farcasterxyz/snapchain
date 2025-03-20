use super::super::constants::PAGE_SIZE_MAX;
use crate::core::error::HubError;
use crate::proto;
use crate::proto::{Block, BlockHeader};
use crate::storage::constants::RootPrefix;
use crate::storage::db::{PageOptions, RocksDB, RocksdbError};
use prost::Message;
use std::sync::Arc;
use thiserror::Error;
use tracing::error;

// TODO(aditi): This code definitely needs unit tests
#[derive(Error, Debug)]
pub enum BlockStorageError {
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

pub fn get_last_block(db: &RocksDB) -> Result<Option<Block>, BlockStorageError> {
    let start_block_key = make_block_key(0);
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

    if block_page.blocks.len() > 1 {
        return Err(BlockStorageError::TooManyBlocksInResult);
    }

    Ok(block_page.blocks.get(0).cloned())
}

pub fn get_current_header(db: &RocksDB) -> Result<Option<proto::BlockHeader>, BlockStorageError> {
    let last_block = get_last_block(db)?;
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
    pub fn get_last_block(&self) -> Result<Option<Block>, BlockStorageError> {
        get_last_block(&self.db)
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

    // TODO: remove
    pub fn some_method(&self) -> Result<(), BlockStorageError> {
        eprintln!("Placeholder for some method");
        Ok(())
    }

    // Prune one page of blocks as specified by page_options from height 0 until
    // but excluding the first block that matches the condition `f`. Returns the
    // number of blocks pruned, and a boolean which is true if iteration stopped
    // due to a block matching the condition, false otherwise.
    pub fn prune_until<F>(
        &self,
        page_options: &PageOptions,
        f: F,
    ) -> Result<(u32, bool), BlockStorageError>
    where
        F: Fn(&BlockHeader) -> bool,
    {
        let page = get_block_page_by_prefix(&self.db, &page_options, None, None)?;
        let mut txn = self.db.txn();
        let mut done = false;
        let mut count = 0u32;
        for block in page.blocks {
            let header = block
                .header
                .as_ref()
                .ok_or(BlockStorageError::BlockMissingHeader)?;
            if !f(&header) {
                let height = header
                    .height
                    .as_ref()
                    .ok_or(BlockStorageError::BlockMissingHeight)?;
                let primary_key = make_block_key(height.block_number);
                txn.delete(primary_key);
                count += 1;
            } else {
                done = true;
                break; // Stop pruning once we find a block matching the condition
            }
        }

        self.db
            .commit(txn)
            .map_err(|e| BlockStorageError::from(e))?;
        Ok((count, done))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::Height;

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

    #[test]
    fn test_prune_until() {
        let blocks_dir = tempfile::tempdir().unwrap();
        let db = make_db(&blocks_dir, "test_db");
        let store = BlockStore::new(db);

        let number_to_timestamp = |n| n * 100;
        // Add some blocks to the db for testing
        (1..=100).for_each(|i| {
            let block = make_block(i, number_to_timestamp(i));
            store.put_block(&block).unwrap();
        });

        let cutoff_block = 42;
        let cutoff_timestamp = number_to_timestamp(cutoff_block);
        let page_size = 10;
        let page_options = PageOptions {
            page_size: Some(page_size),
            ..PageOptions::default()
        };

        let mut pruned = 0u32;
        loop {
            let (count, done) = store
                .prune_until(&page_options, |header| header.timestamp >= cutoff_timestamp)
                .unwrap();
            pruned += count;
            assert!(count <= page_size as u32);
            let expected_done = pruned as u64 == cutoff_block - 1;
            assert_eq!(expected_done, done);

            if done {
                break;
            }
        }

        // Verify that first block after pruning is the cutoff block
        let page = store
            .get_blocks(0, None, &page_options)
            .expect("Failed to get blocks");
        assert!(page.blocks.len() > 0);
        let header = page.blocks[0].header.as_ref().unwrap();
        assert!(cutoff_block == header.height.expect("Missing height").block_number);
    }
}
