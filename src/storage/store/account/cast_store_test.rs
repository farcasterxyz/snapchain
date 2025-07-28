#[cfg(test)]
mod tests {
    use super::super::super::test_helper::FID_FOR_TEST;
    use crate::proto::{self as message, CastType};
    use crate::storage::db::{PageOptions, RocksDB, RocksDbTransactionBatch};
    use crate::storage::store::account::{CastStore, CastStoreDef, Store, StoreEventHandler};
    use crate::utils::factory::{events_factory, messages_factory, time};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_store() -> (Store<CastStoreDef>, Arc<RocksDB>, TempDir) {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = RocksDB::new(db_path.to_str().unwrap());
        db.open().unwrap();
        let db = Arc::new(db);

        let event_handler = StoreEventHandler::new();
        let store = CastStore::new(db.clone(), event_handler.clone(), 10);

        (store, db.clone(), temp_dir)
    }

    #[tokio::test]
    async fn test_get_cast_add_fails_if_missing() {
        let (store, _db, _temp_dir) = create_test_store();

        let cast_add =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "Test cast", None, None);

        let result = CastStore::get_cast_add(&store, FID_FOR_TEST, cast_add.hash.clone());
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_get_cast_add_fails_with_incorrect_values() {
        let (store, db, _temp_dir) = create_test_store();

        let cast_add =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "Test cast", None, None);

        let mut txn = RocksDbTransactionBatch::new();
        let _result = store.merge(&cast_add, &mut txn).unwrap();
        db.commit(txn).unwrap();

        // Test with invalid fid
        let invalid_fid = 999999;
        let result = CastStore::get_cast_add(&store, invalid_fid, cast_add.hash.clone());
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());

        // Test with invalid hash
        let invalid_hash = vec![1, 2, 3, 4, 5];
        let result = CastStore::get_cast_add(&store, FID_FOR_TEST, invalid_hash);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_get_cast_add_succeeds_with_message() {
        let (store, db, _temp_dir) = create_test_store();

        let cast_add =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "Test cast", None, None);

        let mut txn = RocksDbTransactionBatch::new();
        let _result = store.merge(&cast_add, &mut txn).unwrap();
        db.commit(txn).unwrap();
        let result = CastStore::get_cast_add(&store, FID_FOR_TEST, cast_add.hash.clone());
        assert!(result.is_ok());
        let retrieved_cast = result.unwrap().unwrap();
        assert_eq!(retrieved_cast, cast_add);
    }

    #[tokio::test]
    async fn test_get_cast_remove_fails_if_missing() {
        let (store, _db, _temp_dir) = create_test_store();

        let target_hash = (0..20).map(|_| rand::random::<u8>()).collect();

        let result = CastStore::get_cast_remove(&store, FID_FOR_TEST, target_hash);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_get_cast_remove_fails_with_incorrect_values() {
        let (store, db, _temp_dir) = create_test_store();

        let target_hash = (0..20).map(|_| rand::random::<u8>()).collect();
        let cast_remove =
            messages_factory::casts::create_cast_remove(FID_FOR_TEST, &target_hash, None, None);

        let mut txn = RocksDbTransactionBatch::new();
        let _result = store.merge(&cast_remove, &mut txn).unwrap();
        db.commit(txn).unwrap();

        // Test with invalid fid
        let invalid_fid = 999999;
        let result = CastStore::get_cast_remove(&store, invalid_fid, target_hash.clone());
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());

        // Test with invalid hash
        let invalid_hash = vec![1, 2, 3, 4, 5];
        let result = CastStore::get_cast_remove(&store, FID_FOR_TEST, invalid_hash);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_get_cast_remove_succeeds() {
        let (store, db, _temp_dir) = create_test_store();

        let target_hash = (0..20).map(|_| rand::random::<u8>()).collect();
        let cast_remove =
            messages_factory::casts::create_cast_remove(FID_FOR_TEST, &target_hash, None, None);

        let mut txn = RocksDbTransactionBatch::new();
        let _result = store.merge(&cast_remove, &mut txn).unwrap();
        db.commit(txn).unwrap();

        let result = CastStore::get_cast_remove(&store, FID_FOR_TEST, target_hash);
        assert!(result.is_ok());

        let retrieved_remove = result.unwrap().unwrap();
        assert_eq!(retrieved_remove, cast_remove);
    }

    #[tokio::test]
    async fn test_get_cast_adds_by_fid_returns_cast_adds() {
        let (store, db, _temp_dir) = create_test_store();

        let cast_add =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "Test cast", None, None);

        let mut txn = RocksDbTransactionBatch::new();
        let _result = store.merge(&cast_add, &mut txn).unwrap();
        db.commit(txn).unwrap();

        let result = CastStore::get_cast_adds_by_fid(&store, FID_FOR_TEST, &PageOptions::default());

        assert!(result.is_ok());
        let page = result.unwrap();
        assert_eq!(page.messages.len(), 1);
        assert_eq!(page.messages[0], cast_add);
        assert!(page.next_page_token.is_none());
    }

    #[tokio::test]
    async fn test_get_cast_adds_by_fid_with_invalid_fid() {
        let (store, db, _temp_dir) = create_test_store();

        let cast_add =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "Test cast", None, None);

        let mut txn = RocksDbTransactionBatch::new();
        let _result = store.merge(&cast_add, &mut txn).unwrap();
        db.commit(txn).unwrap();

        let invalid_fid = 999999;
        let result = CastStore::get_cast_adds_by_fid(&store, invalid_fid, &PageOptions::default());

        assert!(result.is_ok());
        let page = result.unwrap();
        assert_eq!(page.messages.len(), 0);
        assert!(page.next_page_token.is_none());
    }

    #[tokio::test]
    async fn test_get_cast_adds_by_fid_returns_empty_without_messages() {
        let (store, _db, _temp_dir) = create_test_store();

        let result = CastStore::get_cast_adds_by_fid(&store, FID_FOR_TEST, &PageOptions::default());

        assert!(result.is_ok());
        let page = result.unwrap();
        assert_eq!(page.messages.len(), 0);
        assert!(page.next_page_token.is_none());
    }

    #[tokio::test]
    async fn test_get_cast_adds_by_fid_chronological_order_with_pagination() {
        let (store, db, _temp_dir) = create_test_store();

        let timestamp1 = time::farcaster_time();

        let cast_add1 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "First cast",
            Some(timestamp1),
            None,
        );

        let cast_add2 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "Second cast",
            Some(timestamp1 + 1),
            None,
        );

        let cast_remove = messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &(0..20).map(|_| rand::random::<u8>()).collect(),
            Some(timestamp1 + 10),
            None,
        );

        let mut txn = RocksDbTransactionBatch::new();
        let _result = store.merge(&cast_remove, &mut txn).unwrap();
        let _result1 = store.merge(&cast_add1, &mut txn).unwrap();
        let _result2 = store.merge(&cast_add2, &mut txn).unwrap();
        db.commit(txn).unwrap();

        // Test getting all results
        let result = CastStore::get_cast_adds_by_fid(&store, FID_FOR_TEST, &PageOptions::default());
        assert!(result.is_ok());
        let page = result.unwrap();
        assert_eq!(page.messages.len(), 2);
        assert_eq!(page.messages, vec![cast_add1.clone(), cast_add2.clone()]);

        // Test pagination with page size 1
        let page_options = PageOptions {
            page_size: Some(1),
            page_token: None,
            reverse: false,
        };
        let result1 = CastStore::get_cast_adds_by_fid(&store, FID_FOR_TEST, &page_options);
        assert!(result1.is_ok());
        let page1 = result1.unwrap();
        assert_eq!(page1.messages.len(), 1);
        assert_eq!(page1.messages, vec![cast_add1.clone()]);
        assert!(page1.next_page_token.is_some());

        // Get second page
        let page_options2 = PageOptions {
            page_size: None,
            page_token: page1.next_page_token,
            reverse: false,
        };
        let result2 = CastStore::get_cast_adds_by_fid(&store, FID_FOR_TEST, &page_options2);
        assert!(result2.is_ok());
        let page2 = result2.unwrap();
        assert_eq!(page2.messages.len(), 1);
        assert_eq!(page2.messages, vec![cast_add2.clone()]);
        assert!(page2.next_page_token.is_none());
    }

    #[tokio::test]
    async fn test_get_cast_removes_by_fid_with_invalid_fid() {
        let (store, db, _temp_dir) = create_test_store();

        let timestamp = time::farcaster_time();
        let cast_add = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "Test cast",
            Some(timestamp),
            None,
        );
        let cast_remove = messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &cast_add.hash,
            Some(timestamp + 1),
            None,
        );

        let mut txn = RocksDbTransactionBatch::new();
        let _result = store.merge(&cast_remove, &mut txn).unwrap();
        db.commit(txn).unwrap();

        let invalid_fid = 999999;
        let result =
            CastStore::get_cast_removes_by_fid(&store, invalid_fid, &PageOptions::default());

        assert!(result.is_ok());
        let page = result.unwrap();
        assert_eq!(page.messages.len(), 0);
        assert!(page.next_page_token.is_none());
    }

    #[tokio::test]
    async fn test_get_cast_removes_by_fid_returns_empty_without_messages() {
        let (store, _db, _temp_dir) = create_test_store();

        let result =
            CastStore::get_cast_removes_by_fid(&store, FID_FOR_TEST, &PageOptions::default());

        assert!(result.is_ok());
        let page = result.unwrap();
        assert_eq!(page.messages.len(), 0);
        assert!(page.next_page_token.is_none());
    }

    #[tokio::test]
    async fn test_get_cast_removes_by_fid_chronological_order_with_pagination() {
        let (store, db, _temp_dir) = create_test_store();

        let timestamp1 = time::farcaster_time();

        let cast_add1 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "First cast",
            Some(timestamp1),
            None,
        );

        let cast_remove1 = messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &(0..20).map(|_| rand::random::<u8>()).collect(),
            Some(timestamp1 + 1),
            None,
        );

        let cast_remove2 = messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &(0..20).map(|_| rand::random::<u8>()).collect(),
            Some(timestamp1 + 2),
            None,
        );

        let mut txn = RocksDbTransactionBatch::new();
        let _result1 = store.merge(&cast_remove1, &mut txn).unwrap();
        let _result2 = store.merge(&cast_remove2, &mut txn).unwrap();
        let _result3 = store.merge(&cast_add1, &mut txn).unwrap();
        db.commit(txn).unwrap();

        let result =
            CastStore::get_cast_removes_by_fid(&store, FID_FOR_TEST, &PageOptions::default());
        assert!(result.is_ok());
        let page = result.unwrap();
        assert_eq!(page.messages.len(), 2);
        assert_eq!(
            page.messages,
            vec![cast_remove1.clone(), cast_remove2.clone()]
        );

        // Test pagination with page size 1
        let page_options = PageOptions {
            page_size: Some(1),
            page_token: None,
            reverse: false,
        };
        let result1 = CastStore::get_cast_removes_by_fid(&store, FID_FOR_TEST, &page_options);
        assert!(result1.is_ok());
        let page1 = result1.unwrap();
        assert_eq!(page1.messages.len(), 1);
        assert_eq!(page1.messages, vec![cast_remove1.clone()]);
        assert!(page1.next_page_token.is_some());

        // Get second page
        let page_options2 = PageOptions {
            page_size: None,
            page_token: page1.next_page_token,
            reverse: false,
        };
        let result2 = CastStore::get_cast_removes_by_fid(&store, FID_FOR_TEST, &page_options2);
        assert!(result2.is_ok());
        let page2 = result2.unwrap();
        assert_eq!(page2.messages.len(), 1);
        assert_eq!(page2.messages, vec![cast_remove2.clone()]);
        assert!(page2.next_page_token.is_none());
    }

    #[tokio::test]
    async fn test_get_casts_by_parent_returns_empty_if_no_casts_exist() {
        let (store, _db, _temp_dir) = create_test_store();

        let parent_cast_id = message::CastId {
            fid: 1234,
            hash: vec![1, 2, 3, 4, 5],
        };
        let parent = message::cast_add_body::Parent::ParentCastId(parent_cast_id);

        let result = CastStore::get_casts_by_parent(&store, &parent, &PageOptions::default());

        assert!(result.is_ok());
        let page = result.unwrap();
        assert_eq!(page.messages.len(), 0);
        assert!(page.next_page_token.is_none());
    }

    #[tokio::test]
    async fn test_get_casts_by_parent_returns_empty_for_different_parent() {
        let (store, db, _temp_dir) = create_test_store();

        // Create a cast with a parent
        let parent_cast_id = message::CastId {
            fid: 1234,
            hash: vec![1, 2, 3, 4, 5],
        };

        let cast_with_parent = messages_factory::casts::create_cast_add_rich(
            FID_FOR_TEST,
            "Child cast",
            Some(CastType::Cast),
            vec![],
            Some(parent_cast_id.clone()),
            None,
            None,
        );

        let mut txn = RocksDbTransactionBatch::new();
        let _result = store.merge(&cast_with_parent, &mut txn).unwrap();
        db.commit(txn).unwrap();

        // Query with a different parent
        let different_parent = message::CastId {
            fid: 5678,
            hash: vec![6, 7, 8, 9, 10],
        };
        let parent = message::cast_add_body::Parent::ParentCastId(different_parent);

        let result = CastStore::get_casts_by_parent(&store, &parent, &PageOptions::default());

        assert!(result.is_ok());
        let page = result.unwrap();
        assert_eq!(page.messages.len(), 0);

        // Test with parent URL
        let parent_url =
            message::cast_add_body::Parent::ParentUrl("https://example.com".to_string());
        let result = CastStore::get_casts_by_parent(&store, &parent_url, &PageOptions::default());
        assert!(result.is_ok());
        let page = result.unwrap();
        assert_eq!(page.messages.len(), 0);
    }

    #[tokio::test]
    async fn test_get_casts_by_parent_returns_casts_by_parent_cast_id() {
        let (store, db, _temp_dir) = create_test_store();

        let timestamp1 = time::farcaster_time();

        let parent_cast_id = message::CastId {
            fid: 1234,
            hash: vec![1, 2, 3, 4, 5],
        };

        let cast1 = messages_factory::casts::create_cast_add_rich(
            FID_FOR_TEST,
            "First child cast",
            Some(CastType::Cast),
            vec![],
            Some(parent_cast_id.clone()),
            Some(timestamp1),
            None,
        );

        let cast2 = messages_factory::casts::create_cast_add_rich(
            FID_FOR_TEST,
            "Second child cast",
            Some(CastType::Cast),
            vec![],
            Some(parent_cast_id.clone()),
            Some(timestamp1 + 1),
            None,
        );

        let mut txn = RocksDbTransactionBatch::new();
        let _result1 = store.merge(&cast1, &mut txn).unwrap();
        let _result2 = store.merge(&cast2, &mut txn).unwrap();
        db.commit(txn).unwrap();

        let parent = message::cast_add_body::Parent::ParentCastId(parent_cast_id);
        let page_options = PageOptions {
            page_size: None,
            page_token: None,
            reverse: false,
        };
        let result = CastStore::get_casts_by_parent(&store, &parent, &page_options);

        assert!(result.is_ok());
        let page = result.unwrap();
        assert_eq!(page.messages.len(), 2);
        assert_eq!(page.messages, vec![cast1.clone(), cast2.clone()]);
        assert!(page.next_page_token.is_none());

        // Test pagination
        let page_options = PageOptions {
            page_size: Some(1),
            page_token: None,
            reverse: false,
        };
        let result1 = CastStore::get_casts_by_parent(&store, &parent, &page_options);
        assert!(result1.is_ok());
        let page1 = result1.unwrap();
        assert_eq!(page1.messages.len(), 1);
        assert_eq!(page1.messages, vec![cast1.clone()]);

        let page_options2 = PageOptions {
            page_size: None,
            page_token: page1.next_page_token,
            reverse: false,
        };
        let result2 = CastStore::get_casts_by_parent(&store, &parent, &page_options2);
        assert!(result2.is_ok());
        let page2 = result2.unwrap();
        assert_eq!(page2.messages.len(), 1);
        assert_eq!(page2.messages, vec![cast2.clone()]);

        // Test reverse order
        let page_options_reverse = PageOptions {
            page_size: None,
            page_token: None,
            reverse: true,
        };
        let result3 = CastStore::get_casts_by_parent(&store, &parent, &page_options_reverse);
        assert!(result3.is_ok());
        let page3 = result3.unwrap();
        assert_eq!(page3.messages.len(), 2);
        assert_eq!(page3.messages, vec![cast2, cast1]);
    }

    #[tokio::test]
    async fn test_get_casts_by_parent_returns_casts_by_parent_url() {
        let (store, db, _temp_dir) = create_test_store();

        let timestamp1 = time::farcaster_time();
        let timestamp2 = timestamp1 + 1;
        let parent_url = "https://example.com/post/123".to_string();

        let cast1 = messages_factory::casts::create_cast_add_rich(
            FID_FOR_TEST,
            "First reply to URL",
            Some(CastType::Cast),
            vec![],
            None,
            Some(timestamp1),
            None,
        );

        // Manually set the parent URL
        let mut cast1_with_parent = cast1.clone();
        if let Some(message::MessageData {
            body: Some(message::message_data::Body::CastAddBody(ref mut cast_body)),
            ..
        }) = &mut cast1_with_parent.data
        {
            cast_body.parent = Some(message::cast_add_body::Parent::ParentUrl(
                parent_url.clone(),
            ));
        }

        let cast2 = messages_factory::casts::create_cast_add_rich(
            FID_FOR_TEST,
            "Second reply to URL",
            Some(CastType::Cast),
            vec![],
            None,
            Some(timestamp2),
            None,
        );

        // Manually set the parent URL
        let mut cast2_with_parent = cast2.clone();
        if let Some(message::MessageData {
            body: Some(message::message_data::Body::CastAddBody(ref mut cast_body)),
            ..
        }) = &mut cast2_with_parent.data
        {
            cast_body.parent = Some(message::cast_add_body::Parent::ParentUrl(
                parent_url.clone(),
            ));
        }

        let mut txn = RocksDbTransactionBatch::new();
        let _result1 = store.merge(&cast1_with_parent, &mut txn).unwrap();
        let _result2 = store.merge(&cast2_with_parent, &mut txn).unwrap();
        db.commit(txn).unwrap();

        let parent = message::cast_add_body::Parent::ParentUrl(parent_url);
        let result = CastStore::get_casts_by_parent(&store, &parent, &PageOptions::default());

        assert!(result.is_ok());
        let page = result.unwrap();
        assert_eq!(page.messages.len(), 2);
        assert_eq!(
            page.messages,
            vec![cast1_with_parent.clone(), cast2_with_parent.clone()]
        );
        assert!(page.next_page_token.is_none());

        // Test pagination
        let page_options = PageOptions {
            page_size: Some(1),
            page_token: None,
            reverse: false,
        };
        let result1 = CastStore::get_casts_by_parent(&store, &parent, &page_options);
        assert!(result1.is_ok());
        let page1 = result1.unwrap();
        assert_eq!(page1.messages.len(), 1);
        assert_eq!(page1.messages, vec![cast1_with_parent.clone()]);

        let page_options2 = PageOptions {
            page_size: None,
            page_token: page1.next_page_token,
            reverse: false,
        };
        let result2 = CastStore::get_casts_by_parent(&store, &parent, &page_options2);
        assert!(result2.is_ok());
        let page2 = result2.unwrap();
        assert_eq!(page2.messages.len(), 1);
        assert_eq!(page2.messages, vec![cast2_with_parent.clone()]);

        // Test reverse order
        let page_options_reverse = PageOptions {
            page_size: None,
            page_token: None,
            reverse: true,
        };
        let result3 = CastStore::get_casts_by_parent(&store, &parent, &page_options_reverse);
        assert!(result3.is_ok());
        let page3 = result3.unwrap();
        assert_eq!(page3.messages.len(), 2);
        assert_eq!(page3.messages, vec![cast2_with_parent, cast1_with_parent]);
    }

    #[tokio::test]
    async fn test_get_casts_by_mention_returns_empty_if_no_casts_exist() {
        let (store, _db, _temp_dir) = create_test_store();

        let mention_fid = 9999;
        let result = CastStore::get_casts_by_mention(&store, mention_fid, &PageOptions::default());

        assert!(result.is_ok());
        let page = result.unwrap();
        assert_eq!(page.messages.len(), 0);
        assert!(page.next_page_token.is_none());
    }

    #[tokio::test]
    async fn test_get_casts_by_mention_returns_empty_for_different_fid() {
        let (store, db, _temp_dir) = create_test_store();

        let cast_add =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "Test cast", None, None);

        let mut txn = RocksDbTransactionBatch::new();
        let _result = store.merge(&cast_add, &mut txn).unwrap();
        db.commit(txn).unwrap();

        let different_mention_fid = 9999;
        let result =
            CastStore::get_casts_by_mention(&store, different_mention_fid, &PageOptions::default());

        assert!(result.is_ok());
        let page = result.unwrap();
        assert_eq!(page.messages.len(), 0);
    }

    #[tokio::test]
    async fn test_get_casts_by_mention_returns_casts_with_mentions() {
        let (store, db, _temp_dir) = create_test_store();

        let timestamp1 = time::farcaster_time();
        let mention_fid = 5678;

        let cast1 = messages_factory::casts::create_cast_add_rich(
            FID_FOR_TEST,
            "Cast with mention",
            Some(CastType::Cast),
            vec![],
            None,
            Some(timestamp1),
            None,
        );

        // Manually add mentions to the cast
        let mut cast1_with_mentions = cast1.clone();
        if let Some(message::MessageData {
            body: Some(message::message_data::Body::CastAddBody(ref mut cast_body)),
            ..
        }) = &mut cast1_with_mentions.data
        {
            cast_body.mentions = vec![mention_fid];
        }

        let cast2 = messages_factory::casts::create_cast_add_rich(
            FID_FOR_TEST,
            "Another cast with mention",
            Some(CastType::Cast),
            vec![],
            None,
            Some(timestamp1 + 1),
            None,
        );

        let mut cast2_with_mentions = cast2.clone();
        if let Some(message::MessageData {
            body: Some(message::message_data::Body::CastAddBody(ref mut cast_body)),
            ..
        }) = &mut cast2_with_mentions.data
        {
            cast_body.mentions = vec![mention_fid];
        }

        let mut txn = RocksDbTransactionBatch::new();
        let _result1 = store.merge(&cast1_with_mentions, &mut txn).unwrap();
        let _result2 = store.merge(&cast2_with_mentions, &mut txn).unwrap();
        db.commit(txn).unwrap();

        let result = CastStore::get_casts_by_mention(&store, mention_fid, &PageOptions::default());

        assert!(result.is_ok());
        let page = result.unwrap();
        assert_eq!(page.messages.len(), 2);
        assert_eq!(
            page.messages,
            vec![cast1_with_mentions.clone(), cast2_with_mentions.clone()]
        );
        assert!(page.next_page_token.is_none());

        // Test pagination
        let page_options = PageOptions {
            page_size: Some(1),
            page_token: None,
            reverse: false,
        };
        let result1 = CastStore::get_casts_by_mention(&store, mention_fid, &page_options);
        assert!(result1.is_ok());
        let page1 = result1.unwrap();
        assert_eq!(page1.messages.len(), 1);
        assert_eq!(page1.messages, vec![cast1_with_mentions.clone()]);

        let page_options2 = PageOptions {
            page_size: None,
            page_token: page1.next_page_token,
            reverse: false,
        };
        let result2 = CastStore::get_casts_by_mention(&store, mention_fid, &page_options2);
        assert!(result2.is_ok());
        let page2 = result2.unwrap();
        assert_eq!(page2.messages.len(), 1);
        assert_eq!(page2.messages, vec![cast2_with_mentions.clone()]);

        // Test reverse order
        let page_options_reverse = PageOptions {
            page_size: None,
            page_token: None,
            reverse: true,
        };
        let result3 = CastStore::get_casts_by_mention(&store, mention_fid, &page_options_reverse);
        assert!(result3.is_ok());
        let page3 = result3.unwrap();
        assert_eq!(page3.messages.len(), 2);
        assert_eq!(
            page3.messages,
            vec![cast2_with_mentions, cast1_with_mentions]
        );
    }

    #[tokio::test]
    async fn test_merge_fails_with_invalid_message_type() {
        let (store, _db, _temp_dir) = create_test_store();

        let reaction_add = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            message::ReactionType::Like,
            "target".to_string(),
            None,
            None,
        );

        let mut txn = RocksDbTransactionBatch::new();
        let result = store.merge(&reaction_add, &mut txn);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_merge_cast_add_succeeds() {
        let (store, db, _temp_dir) = create_test_store();

        let cast_add =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "Test cast", None, None);

        let mut txn = RocksDbTransactionBatch::new();
        let result = store.merge(&cast_add, &mut txn);
        assert!(result.is_ok());
        db.commit(txn).unwrap();

        // Verify the cast was stored
        let retrieved = CastStore::get_cast_add(&store, FID_FOR_TEST, cast_add.hash.clone());
        assert!(retrieved.is_ok());
        assert!(retrieved.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_merge_cast_add_fails_if_merged_twice() {
        let (store, db, _temp_dir) = create_test_store();

        let cast_add =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "Test cast", None, None);

        let mut txn1 = RocksDbTransactionBatch::new();
        let result1 = store.merge(&cast_add, &mut txn1);
        assert!(result1.is_ok());
        db.commit(txn1).unwrap();

        let mut txn2 = RocksDbTransactionBatch::new();
        let result2 = store.merge(&cast_add, &mut txn2);
        assert!(result2.is_err());
        let error = result2.unwrap_err();
        assert_eq!(error.code, "bad_request.duplicate");
        assert_eq!(error.message, "message has already been merged");
    }

    #[tokio::test]
    async fn test_merge_cast_remove_succeeds() {
        let (store, db, _temp_dir) = create_test_store();

        let timestamp = time::farcaster_time();
        let cast_add = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "Test cast",
            Some(timestamp),
            None,
        );
        let cast_remove = messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &cast_add.hash,
            Some(timestamp + 1),
            None,
        );

        // First merge the cast add
        let mut txn = RocksDbTransactionBatch::new();
        let _result = store.merge(&cast_add, &mut txn).unwrap();
        db.commit(txn).unwrap();

        // Then merge the cast remove
        let mut txn2 = RocksDbTransactionBatch::new();
        let result = store.merge(&cast_remove, &mut txn2);
        assert!(result.is_ok());
        db.commit(txn2).unwrap();

        // Verify the remove was stored and the add was removed
        let remove_result = CastStore::get_cast_remove(&store, FID_FOR_TEST, cast_add.hash.clone());
        assert!(remove_result.is_ok());
        assert!(remove_result.unwrap().is_some());

        let add_result = CastStore::get_cast_add(&store, FID_FOR_TEST, cast_add.hash.clone());
        assert!(add_result.is_ok());
        assert!(add_result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_merge_cast_remove_fails_if_merged_twice() {
        let (store, db, _temp_dir) = create_test_store();

        let timestamp = time::farcaster_time();
        let cast_add = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "Test cast",
            Some(timestamp),
            None,
        );
        let cast_remove = messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &cast_add.hash,
            Some(timestamp + 1),
            None,
        );

        let mut txn1 = RocksDbTransactionBatch::new();
        let result1 = store.merge(&cast_remove, &mut txn1);
        assert!(result1.is_ok());
        db.commit(txn1).unwrap();

        let mut txn2 = RocksDbTransactionBatch::new();
        let result2 = store.merge(&cast_remove, &mut txn2);
        assert!(result2.is_err());
        let error = result2.unwrap_err();
        assert_eq!(error.code, "bad_request.duplicate");
        assert_eq!(error.message, "message has already been merged");
    }

    #[tokio::test]
    async fn test_cast_remove_wins_over_cast_add_with_later_timestamp() {
        let (store, db, _temp_dir) = create_test_store();

        let timestamp = time::farcaster_time();
        let cast_add = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "Test cast",
            Some(timestamp),
            None,
        );

        let cast_remove_earlier = messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &cast_add.hash,
            Some(timestamp - 1),
            None,
        );

        let mut txn1 = RocksDbTransactionBatch::new();
        let _result1 = store.merge(&cast_remove_earlier, &mut txn1).unwrap();
        db.commit(txn1).unwrap();

        let mut txn2 = RocksDbTransactionBatch::new();
        let result = store.merge(&cast_add, &mut txn2);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.code, "bad_request.conflict");
        assert_eq!(error.message, "message conflicts with a more recent remove");

        // Verify the remove still exists and add doesn't
        let remove_result = CastStore::get_cast_remove(&store, FID_FOR_TEST, cast_add.hash.clone());
        assert!(remove_result.is_ok());
        assert!(remove_result.unwrap().is_some());

        let add_result = CastStore::get_cast_add(&store, FID_FOR_TEST, cast_add.hash.clone());
        assert!(add_result.is_ok());
        assert!(add_result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_cast_remove_wins_over_cast_add_with_earlier_timestamp() {
        let (store, db, _temp_dir) = create_test_store();

        let timestamp = time::farcaster_time();
        let cast_add = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "Test cast",
            Some(timestamp),
            None,
        );
        let cast_remove = messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &cast_add.hash,
            Some(timestamp + 1),
            None,
        );

        let mut txn = RocksDbTransactionBatch::new();
        let _result = store.merge(&cast_remove, &mut txn).unwrap();
        db.commit(txn).unwrap();

        let mut txn2 = RocksDbTransactionBatch::new();
        let result = store.merge(&cast_add, &mut txn2);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.code, "bad_request.conflict");
        assert_eq!(error.message, "message conflicts with a more recent remove");

        // Verify the remove still exists and add doesn't
        let remove_result = CastStore::get_cast_remove(&store, FID_FOR_TEST, cast_add.hash.clone());
        assert!(remove_result.is_ok());
        assert!(remove_result.unwrap().is_some());

        let add_result = CastStore::get_cast_add(&store, FID_FOR_TEST, cast_add.hash.clone());
        assert!(add_result.is_ok());
        assert!(add_result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_revoke_fails_with_invalid_message_type() {
        let (store, _db, _temp_dir) = create_test_store();

        let reaction_add = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            message::ReactionType::Like,
            "target".to_string(),
            None,
            None,
        );

        let mut txn = RocksDbTransactionBatch::new();
        let result = store.revoke(&reaction_add, &mut txn);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.code, "bad_request.invalid_param");
        assert_eq!(error.message, "invalid message type");
    }

    #[tokio::test]
    async fn test_revoke_succeeds_with_cast_add() {
        let (store, db, _temp_dir) = create_test_store();

        let cast_add =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "Test cast", None, None);

        let mut txn = RocksDbTransactionBatch::new();
        let _result = store.merge(&cast_add, &mut txn).unwrap();
        db.commit(txn).unwrap();

        let mut txn2 = RocksDbTransactionBatch::new();
        let result = store.revoke(&cast_add, &mut txn2);
        assert!(result.is_ok());
        db.commit(txn2).unwrap();

        let get_result = CastStore::get_cast_add(&store, FID_FOR_TEST, cast_add.hash.clone());
        assert!(get_result.is_ok());
        assert!(get_result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_revoke_succeeds_with_cast_remove() {
        let (store, db, _temp_dir) = create_test_store();

        let timestamp = time::farcaster_time();
        let cast_add = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "Test cast",
            Some(timestamp),
            None,
        );
        let cast_remove = messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &cast_add.hash,
            Some(timestamp + 1),
            None,
        );

        let mut txn = RocksDbTransactionBatch::new();
        let _result = store.merge(&cast_remove, &mut txn).unwrap();
        db.commit(txn).unwrap();

        let mut txn2 = RocksDbTransactionBatch::new();
        let result = store.revoke(&cast_remove, &mut txn2);
        assert!(result.is_ok());
        db.commit(txn2).unwrap();

        let get_result = CastStore::get_cast_remove(&store, FID_FOR_TEST, cast_add.hash.clone());
        assert!(get_result.is_ok());
        assert!(get_result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_revoke_succeeds_with_unmerged_message() {
        let (store, db, _temp_dir) = create_test_store();

        let cast_add =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "Test cast", None, None);

        // Don't merge the message first
        let mut txn = RocksDbTransactionBatch::new();
        let result = store.revoke(&cast_add, &mut txn);
        assert!(result.is_ok());
        db.commit(txn).unwrap();

        let get_result = CastStore::get_cast_add(&store, FID_FOR_TEST, cast_add.hash.clone());
        assert!(get_result.is_ok());
        assert!(get_result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_prune_messages_with_size_limit() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = RocksDB::new(db_path.to_str().unwrap());
        db.open().unwrap();
        let db = Arc::new(db);

        let event_handler = StoreEventHandler::new();
        let store = CastStore::new(db.clone(), event_handler.clone(), 3); // Size limit of 3

        // Create storage rent event for the fid
        let _rent_event = events_factory::create_rent_event(
            FID_FOR_TEST,
            1,
            message::StorageUnitType::UnitType2025,
            false,
            message::FarcasterNetwork::Devnet,
        );

        let timestamp = time::farcaster_time();

        // Create 5 cast add messages
        let mut cast_adds = vec![];
        for i in 0..5 {
            let cast_add = messages_factory::casts::create_cast_add(
                FID_FOR_TEST,
                &format!("Cast message {}", i),
                Some(timestamp + i),
                None,
            );
            cast_adds.push(cast_add);
        }

        // Merge all messages
        for cast_add in &cast_adds {
            let mut txn = RocksDbTransactionBatch::new();
            let _result = store.merge(cast_add, &mut txn).unwrap();
            db.commit(txn).unwrap();
        }

        // Prune messages (requires current_count, max_count, and transaction)
        let mut txn = RocksDbTransactionBatch::new();
        let result = store.prune_messages(FID_FOR_TEST, 5, 3, &mut txn);
        assert!(result.is_ok());
        db.commit(txn).unwrap();

        // Verify that only the latest 3 messages remain
        let page_options = PageOptions {
            page_size: None,
            page_token: None,
            reverse: false,
        };
        let remaining_messages =
            CastStore::get_cast_adds_by_fid(&store, FID_FOR_TEST, &page_options);
        assert!(remaining_messages.is_ok());
        let page = remaining_messages.unwrap();
        assert_eq!(page.messages.len(), 3);

        // Verify the earliest 2 messages were pruned
        for i in 0..2 {
            let result = CastStore::get_cast_add(&store, FID_FOR_TEST, cast_adds[i].hash.clone());
            assert!(result.is_ok());
            assert!(result.unwrap().is_none());
        }

        // Verify the latest 3 messages still exist
        for i in 2..5 {
            let result = CastStore::get_cast_add(&store, FID_FOR_TEST, cast_adds[i].hash.clone());
            assert!(result.is_ok());
            assert!(result.unwrap().is_some());
        }
    }

    #[tokio::test]
    async fn test_prune_messages_no_op_when_no_messages() {
        let (store, db, _temp_dir) = create_test_store();

        let mut txn = RocksDbTransactionBatch::new();
        let result = store.prune_messages(FID_FOR_TEST, 0, 10, &mut txn);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0); // No messages pruned
        db.commit(txn).unwrap();
    }
}
