use crate::proto::Block;
use crate::storage::db::{PageOptions, RocksDB};
use crate::storage::store::block::{BlockStorageError, BlockStore};
use std::sync::Arc;

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup_test_db() -> (TempDir, Arc<RocksDB>) {
        let temp_dir = TempDir::new().unwrap();
        let db = Arc::new(RocksDB::new(temp_dir.path().to_str().unwrap()));
        db.open().unwrap();
        (temp_dir, db)
    }

    #[test]
    fn test_block_store_operations() {
        let (_temp_dir, db) = setup_test_db();
        let store = BlockStore::new(db);

        // Test empty store
        assert!(store.get_last_block().unwrap().is_none());
        assert_eq!(store.max_block_number().unwrap(), 0);

        // Test block insertion and retrieval
        let test_block = Block {
            header: Some(crate::proto::BlockHeader {
                height: Some(crate::proto::Height {
                    block_number: 1,
                    shard_index: 0,
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        store.put_block(test_block.clone()).unwrap();

        let retrieved_block = store.get_last_block().unwrap().unwrap();
        assert_eq!(
            retrieved_block.header.unwrap().height.unwrap().block_number,
            1
        );
        assert_eq!(store.max_block_number().unwrap(), 1);
    }

    #[test]
    fn test_block_error_handling() {
        let (_temp_dir, db) = setup_test_db();
        let store = BlockStore::new(db);

        // Test missing header error
        let invalid_block = Block::default();
        let result = store.put_block(invalid_block);
        assert!(matches!(result, Err(BlockStorageError::BlockMissingHeader)));

        // Test missing height error
        let invalid_block = Block {
            header: Some(crate::proto::BlockHeader::default()),
            ..Default::default()
        };
        let result = store.put_block(invalid_block);
        assert!(matches!(result, Err(BlockStorageError::BlockMissingHeight)));
    }

    #[test]
    fn test_block_range_queries() {
        let (_temp_dir, db) = setup_test_db();
        let store = BlockStore::new(db);

        // Insert multiple blocks
        for i in 1..=5 {
            let block = Block {
                header: Some(crate::proto::BlockHeader {
                    height: Some(crate::proto::Height {
                        block_number: i,
                        shard_index: 0,
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            };
            store.put_block(block).unwrap();
        }

        // Test range queries (ranges are [start, stop) - stop is exclusive)
        let blocks = store
            .get_blocks(1, Some(3), &PageOptions::default())
            .unwrap();
        assert_eq!(blocks.blocks.len(), 2, "Expected 2 blocks in range [1,3)");

        let blocks = store
            .get_blocks(3, Some(6), &PageOptions::default())
            .unwrap();
        assert_eq!(blocks.blocks.len(), 3, "Expected 3 blocks in range [3,6)");

        let blocks = store
            .get_blocks(1, Some(6), &PageOptions::default())
            .unwrap();
        assert_eq!(blocks.blocks.len(), 5, "Expected 5 blocks in total");
    }
}
