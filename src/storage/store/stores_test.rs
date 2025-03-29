#[cfg(test)]
mod tests {
    use crate::core::util::get_farcaster_time;
    use crate::proto::ReactionType;
    use crate::storage::db::PageOptions;
    use crate::storage::store::{stores, test_helper};
    use crate::storage::{db, trie};
    use crate::utils::factory::{hub_events_factory, messages_factory, shard_chunk_factory};
    use std::sync::Arc;
    use std::time::Duration;

    fn create_stores() -> stores::Stores {
        let dir = tempfile::TempDir::new().unwrap();
        let db_path = dir.path().join("a.db");

        let db = db::RocksDB::new(db_path.to_str().unwrap());
        db.open().unwrap();

        let trie = trie::merkle_trie::MerkleTrie::new(16).unwrap();
        let limits = stores::StoreLimits {
            limits: test_helper::limits::test(),
            legacy_limits: test_helper::limits::zero(),
        };

        stores::Stores::new(Arc::new(db), 1, trie, limits)
    }

    #[tokio::test]
    async fn test_event_pruning() {
        let stores = create_stores();
        let shard_id = 1;
        let message = messages_factory::reactions::create_reaction_add(
            123,
            ReactionType::Like,
            "".to_string(),
            None,
            None,
        );

        let one_day_in_seconds = 24 * 60 * 60;
        let mut current_time = get_farcaster_time().unwrap() - (11 * one_day_in_seconds);

        for i in 0..10 {
            let current_height = i;
            current_time += one_day_in_seconds;

            let shard_chunk = shard_chunk_factory::create_shard_chunk(
                shard_id,
                Some(current_height),
                Some(current_time),
            );
            stores.shard_store.put_shard_chunk(&shard_chunk).unwrap();
            stores.event_handler.set_current_height(current_height);
            let mut txn = db::RocksDbTransactionBatch::new();
            for _ in 0..3 {
                let mut event = hub_events_factory::create_merge_event(&message);
                stores
                    .event_handler
                    .commit_transaction(&mut txn, &mut event)
                    .unwrap();
            }
            stores.db.commit(txn).unwrap();
        }

        let events = stores.get_events(0, None, None).unwrap();
        assert_eq!(events.events.len(), 10 * 3); // 10 chunks, 3 events each

        let result = stores
            .prune_events_until(8, &PageOptions::default(), Duration::from_secs(0))
            .await;
        assert_eq!(result.is_ok(), true);
        assert_eq!(result.unwrap(), 8 * 3); // 8 chunks pruned
        let events = stores.get_events(0, None, None).unwrap();
        assert_eq!(events.events.len(), 2 * 3); // 2 chunks, 3 events each
    }
}
