use crate::proto::{Block, ShardChunk};
use crate::storage::db::RocksDB;
use crate::storage::store::block::BlockStore;
use crate::storage::store::shard::ShardStore;
use std::sync::Arc;

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup_test_stores() -> (TempDir, BlockStore, ShardStore) {
        let temp_dir = TempDir::new().unwrap();
        let db = Arc::new(RocksDB::new(temp_dir.path().to_str().unwrap()));
        db.open().unwrap();
        let block_store = BlockStore::new(db.clone());
        let shard_store = ShardStore::new(db);
        (temp_dir, block_store, shard_store)
    }

    #[test]
    fn test_block_shard_synchronization() {
        let (_temp_dir, block_store, shard_store) = setup_test_stores();

        // Insert blocks and corresponding shards
        for i in 1..=3 {
            // Insert block
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
            block_store.put_block(block).unwrap();

            // Insert corresponding shard
            let shard = ShardChunk {
                header: Some(crate::proto::ShardHeader {
                    height: Some(crate::proto::Height {
                        block_number: i,
                        shard_index: 0,
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            };
            shard_store.put_shard_chunk(&shard).unwrap();
        }

        // Verify synchronization
        assert_eq!(block_store.max_block_number().unwrap(), 3);
        assert_eq!(shard_store.max_block_number().unwrap(), 3);

        // Verify data consistency
        let blocks = block_store
            .get_blocks(0, Some(4), &Default::default())
            .unwrap();
        let shards = shard_store.get_shard_chunks(0, Some(4)).unwrap();

        assert_eq!(blocks.blocks.len(), 3);
        assert_eq!(shards.len(), 3);

        // Verify heights match
        for (block, shard) in blocks.blocks.iter().zip(shards.iter()) {
            let block_height = block
                .header
                .as_ref()
                .unwrap()
                .height
                .as_ref()
                .unwrap()
                .block_number;
            let shard_height = shard
                .header
                .as_ref()
                .unwrap()
                .height
                .as_ref()
                .unwrap()
                .block_number;
            assert_eq!(block_height, shard_height);
        }
    }

    #[test]
    fn test_error_propagation() {
        let (_temp_dir, block_store, shard_store) = setup_test_stores();

        // Test invalid block and shard
        let invalid_block = Block::default();
        let invalid_shard = ShardChunk::default();

        // Both operations should fail
        assert!(block_store.put_block(invalid_block).is_err());
        assert!(shard_store.put_shard_chunk(&invalid_shard).is_err());

        // Verify both stores remain empty
        assert_eq!(block_store.max_block_number().unwrap(), 0);
        assert_eq!(shard_store.max_block_number().unwrap(), 0);
    }
}
