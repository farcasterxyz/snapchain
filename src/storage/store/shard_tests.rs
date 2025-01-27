use crate::proto::ShardChunk;
use crate::storage::db::RocksDB;
use crate::storage::store::shard::{ShardStorageError, ShardStore};
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
    fn test_shard_store_operations() {
        let (_temp_dir, db) = setup_test_db();
        let store = ShardStore::new(db);

        // Test empty store
        assert!(store.get_last_shard_chunk().unwrap().is_none());
        assert_eq!(store.max_block_number().unwrap(), 0);

        // Test shard insertion and retrieval
        let test_shard = ShardChunk {
            header: Some(crate::proto::ShardHeader {
                height: Some(crate::proto::Height {
                    block_number: 1,
                    shard_index: 0,
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        store.put_shard_chunk(&test_shard).unwrap();

        let retrieved_shard = store.get_last_shard_chunk().unwrap().unwrap();
        assert_eq!(
            retrieved_shard.header.unwrap().height.unwrap().block_number,
            1
        );
        assert_eq!(store.max_block_number().unwrap(), 1);
    }

    #[test]
    fn test_shard_error_handling() {
        let (_temp_dir, db) = setup_test_db();
        let store = ShardStore::new(db);

        // Test missing header error
        let invalid_shard = ShardChunk::default();
        let result = store.put_shard_chunk(&invalid_shard);
        assert!(matches!(result, Err(ShardStorageError::ShardMissingHeader)));

        // Test missing height error
        let invalid_shard = ShardChunk {
            header: Some(crate::proto::ShardHeader::default()),
            ..Default::default()
        };
        let result = store.put_shard_chunk(&invalid_shard);
        assert!(matches!(result, Err(ShardStorageError::ShardMissingHeight)));
    }

    #[test]
    fn test_shard_range_queries() {
        let (_temp_dir, db) = setup_test_db();
        let store = ShardStore::new(db);

        // Insert multiple shards
        for i in 1..=5 {
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
            store.put_shard_chunk(&shard).unwrap();
        }

        // Test range queries (ranges are [start, stop) - stop is exclusive)
        let shards = store.get_shard_chunks(1, Some(3)).unwrap();
        assert_eq!(shards.len(), 2, "Expected 2 shards in range [1,3)");

        let shards = store.get_shard_chunks(3, Some(6)).unwrap();
        assert_eq!(shards.len(), 3, "Expected 3 shards in range [3,6)");

        let shards = store.get_shard_chunks(1, Some(6)).unwrap();
        assert_eq!(shards.len(), 5, "Expected 5 shards in total");
    }
}
