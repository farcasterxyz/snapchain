#[cfg(test)]
mod tests {
    use super::super::super::test_helper::FID_FOR_TEST;
    use crate::proto::{self as message, hub_event, CastType, HubEventType};
    use crate::storage::db::{PageOptions, RocksDB, RocksDbTransactionBatch};
    use crate::storage::store::account::{CastStore, CastStoreDef, Store, StoreEventHandler};
    use crate::utils::factory::{messages_factory, time};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_store() -> (Store<CastStoreDef>, Arc<RocksDB>, TempDir) {
        create_test_store_with_prune_limit(10)
    }

    fn create_test_store_with_prune_limit(
        prune_size_limit: u32,
    ) -> (Store<CastStoreDef>, Arc<RocksDB>, TempDir) {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = RocksDB::new(db_path.to_str().unwrap());
        db.open().unwrap();
        let db = Arc::new(db);

        let event_handler = StoreEventHandler::new();
        let store = CastStore::new(db.clone(), event_handler.clone(), prune_size_limit);

        (store, db.clone(), temp_dir)
    }

    fn merge_messages(
        store: &Store<CastStoreDef>,
        db: &Arc<RocksDB>,
        messages: Vec<&message::Message>,
    ) {
        let mut txn = RocksDbTransactionBatch::new();
        for message in messages {
            let result = store.merge(message, &mut txn).unwrap();
            assert_eq!(result.r#type(), HubEventType::MergeMessage);
            match &result.body {
                Some(hub_event::Body::MergeMessageBody(body)) => {
                    assert_eq!(*body.message.as_ref().unwrap(), *message)
                }
                _ => {
                    panic!("Unexpected event")
                }
            }
        }
        db.commit(txn).unwrap();
    }

    fn revoke_message(store: &Store<CastStoreDef>, db: &Arc<RocksDB>, message: &message::Message) {
        let mut txn = RocksDbTransactionBatch::new();

        let result = store.revoke(message, &mut txn).unwrap();
        assert_eq!(result.r#type(), HubEventType::RevokeMessage);
        match &result.body {
            Some(hub_event::Body::RevokeMessageBody(body)) => {
                assert_eq!(*body.message.as_ref().unwrap(), *message)
            }
            _ => {
                panic!("Unexpected event")
            }
        }
        db.commit(txn).unwrap();
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

        merge_messages(&store, &db, vec![&cast_add]);

        // Test with invalid fid
        let invalid_fid = 999999;
        let result = CastStore::get_cast_add(&store, invalid_fid, cast_add.hash.clone());
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());

        // Test with invalid hash
        let invalid_hash = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
        ];
        let result = CastStore::get_cast_add(&store, FID_FOR_TEST, invalid_hash);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_get_cast_add_succeeds_with_message() {
        let (store, db, _temp_dir) = create_test_store();

        let cast_add =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "Test cast", None, None);

        merge_messages(&store, &db, vec![&cast_add]);
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

        merge_messages(&store, &db, vec![&cast_remove]);

        // Test with invalid fid
        let invalid_fid = 999999;
        let result = CastStore::get_cast_remove(&store, invalid_fid, target_hash.clone());
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());

        // Test with invalid hash
        let invalid_hash = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
        ];
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

        merge_messages(&store, &db, vec![&cast_remove]);

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

        merge_messages(&store, &db, vec![&cast_add]);

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

        merge_messages(&store, &db, vec![&cast_add]);

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

        merge_messages(&store, &db, vec![&cast_remove, &cast_add1, &cast_add2]);

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

        merge_messages(&store, &db, vec![&cast_remove]);

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

        merge_messages(&store, &db, vec![&cast_remove1, &cast_remove2, &cast_add1]);

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
            hash: vec![
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
            ],
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
            hash: vec![
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
            ],
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

        merge_messages(&store, &db, vec![&cast_with_parent]);

        // Query with a different parent
        let different_parent = message::CastId {
            fid: 5678,
            hash: vec![
                6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
            ],
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
            hash: vec![
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
            ],
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

        merge_messages(&store, &db, vec![&cast1, &cast2]);

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

        merge_messages(&store, &db, vec![&cast1_with_parent, &cast2_with_parent]);

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

        merge_messages(&store, &db, vec![&cast_add]);

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

        merge_messages(
            &store,
            &db,
            vec![&cast1_with_mentions, &cast2_with_mentions],
        );

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

        merge_messages(&store, &db, vec![&cast_add]);

        // Verify the cast was stored
        let retrieved = CastStore::get_cast_add(&store, FID_FOR_TEST, cast_add.hash.clone());
        assert!(retrieved.is_ok());
        assert!(retrieved.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_merge_cast_add_succeeds_for_long_cast() {
        let (store, db, _temp_dir) = create_test_store();

        // Create a long cast message (320 characters)
        let long_text = "a".repeat(320);
        let cast_add =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, &long_text, None, None);

        merge_messages(&store, &db, vec![&cast_add]);

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

        merge_messages(&store, &db, vec![&cast_add]);

        let mut txn2 = RocksDbTransactionBatch::new();
        let result2 = store.merge(&cast_add, &mut txn2);
        assert!(result2.is_err());
        let error = result2.unwrap_err();
        assert_eq!(error.code, "bad_request.duplicate");
        assert_eq!(error.message, "message has already been merged");
    }

    #[tokio::test]
    async fn test_merge_cast_add_fails_conflicting_cast_remove_with_later_timestamp() {
        let (store, db, _temp_dir) = create_test_store();

        let timestamp1 = time::farcaster_time();

        let cast_add = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "Test cast",
            Some(timestamp1 + 1), // Later timestamp
            None,
        );

        // Create cast remove with earlier timestamp
        let cast_remove = messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &cast_add.hash,
            Some(timestamp1), // Earlier timestamp
            None,
        );

        // Merge remove first
        merge_messages(&store, &db, vec![&cast_remove]);

        // Try to merge add with later timestamp - should fail due to remove wins
        let mut txn2 = RocksDbTransactionBatch::new();
        let result2 = store.merge(&cast_add, &mut txn2);
        assert!(result2.is_err());
        let error = result2.unwrap_err();
        assert_eq!(error.code, "bad_request.conflict");
        assert_eq!(error.message, "message conflicts with a more recent remove");
    }

    #[tokio::test]
    async fn test_merge_cast_add_fails_with_conflicting_cast_remove_with_later_timestamp() {
        let (store, db, _temp_dir) = create_test_store();

        let timestamp1 = time::farcaster_time();

        let cast_add = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "Test cast",
            Some(timestamp1), // Earlier timestamp
            None,
        );

        // Create cast remove with later timestamp
        let cast_remove = messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &cast_add.hash,
            Some(timestamp1 + 1), // Later timestamp
            None,
        );

        // Merge remove first
        merge_messages(&store, &db, vec![&cast_remove]);

        // Try to merge add with earlier timestamp - should fail due to remove with later timestamp
        let mut txn2 = RocksDbTransactionBatch::new();
        let result2 = store.merge(&cast_add, &mut txn2);
        assert!(result2.is_err());
        let error = result2.unwrap_err();
        assert_eq!(error.code, "bad_request.conflict");
        assert_eq!(error.message, "message conflicts with a more recent remove");
    }

    #[tokio::test]
    async fn test_merge_cast_add_fails_with_conflicting_cast_remove_with_later_hash() {
        let (store, db, _temp_dir) = create_test_store();

        let timestamp = time::farcaster_time();
        let cast_add = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "Test cast",
            Some(timestamp),
            None,
        );

        // Create cast remove with same timestamp but later hash (lexicographically)
        let mut cast_remove = messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &cast_add.hash,
            Some(timestamp), // Same timestamp
            None,
        );
        // Make sure remove has later hash than add
        cast_remove.hash = vec![255; 20];

        // Merge remove first
        merge_messages(&store, &db, vec![&cast_remove]);

        // Try to merge add - should fail due to conflicting remove with later hash
        let mut txn2 = RocksDbTransactionBatch::new();
        let result2 = store.merge(&cast_add, &mut txn2);
        assert!(result2.is_err());
        let error = result2.unwrap_err();
        assert_eq!(error.code, "bad_request.conflict");
        assert_eq!(error.message, "message conflicts with a more recent remove");
    }

    #[tokio::test]
    async fn test_merge_cast_add_fails_with_conflicting_cast_remove_with_earlier_hash() {
        let (store, db, _temp_dir) = create_test_store();

        let timestamp = time::farcaster_time();
        let cast_add = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "Test cast",
            Some(timestamp),
            None,
        );

        // Create cast remove with same timestamp but earlier hash (lexicographically)
        let mut cast_remove = messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &cast_add.hash,
            Some(timestamp), // Same timestamp
            None,
        );
        // Make sure remove has earlier hash than add
        cast_remove.hash = vec![0; 20];

        // Merge remove first
        merge_messages(&store, &db, vec![&cast_remove]);

        // Try to merge add - should fail due to conflicting remove with earlier hash (remove wins)
        let mut txn2 = RocksDbTransactionBatch::new();
        let result2 = store.merge(&cast_add, &mut txn2);
        assert!(result2.is_err());
        let error = result2.unwrap_err();
        assert_eq!(error.code, "bad_request.conflict");
        assert_eq!(error.message, "message conflicts with a more recent remove");
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
        merge_messages(&store, &db, vec![&cast_add, &cast_remove]);

        // Verify the remove was stored and the add was removed
        let remove_result = CastStore::get_cast_remove(&store, FID_FOR_TEST, cast_add.hash.clone());
        assert!(remove_result.is_ok());
        assert!(remove_result.unwrap().is_some());

        let add_result = CastStore::get_cast_add(&store, FID_FOR_TEST, cast_add.hash.clone());
        assert!(add_result.is_ok());
        assert!(add_result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_merge_cast_remove_succeeds_for_long_cast() {
        let (store, db, _temp_dir) = create_test_store();

        let timestamp = time::farcaster_time();
        let long_text = "a".repeat(320);
        let cast_add = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            &long_text,
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
        merge_messages(&store, &db, vec![&cast_add, &cast_remove]);

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
    async fn test_cast_remove_succeeds_with_later_timestamp() {
        let (store, db, _temp_dir) = create_test_store();

        let timestamp1 = time::farcaster_time();
        let timestamp2 = timestamp1 + 1;

        let cast_add = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "Test cast",
            Some(timestamp1),
            None,
        );
        let cast_remove = messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &cast_add.hash,
            Some(timestamp2),
            None,
        );

        // Merge add first
        merge_messages(&store, &db, vec![&cast_add, &cast_remove]);

        // Verify remove exists and add is gone
        let remove_result = CastStore::get_cast_remove(&store, FID_FOR_TEST, cast_add.hash.clone());
        assert!(remove_result.is_ok());
        assert!(remove_result.unwrap().is_some());

        let add_result = CastStore::get_cast_add(&store, FID_FOR_TEST, cast_add.hash.clone());
        assert!(add_result.is_ok());
        assert!(add_result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_cast_remove_fails_with_earlier_timestamp() {
        let (store, db, _temp_dir) = create_test_store();

        let timestamp1 = time::farcaster_time();
        let timestamp2 = timestamp1 - 1;

        let cast_remove1 = messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &vec![
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
            ],
            Some(timestamp1),
            None,
        );
        let cast_remove2 = messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &vec![
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
            ],
            Some(timestamp2),
            None,
        );

        // Merge first remove
        merge_messages(&store, &db, vec![&cast_remove1]);

        // Try to merge second remove with earlier timestamp - should fail
        let mut txn2 = RocksDbTransactionBatch::new();
        let result2 = store.merge(&cast_remove2, &mut txn2);
        assert!(result2.is_err());
        let error = result2.unwrap_err();
        assert_eq!(error.code, "bad_request.conflict");
    }

    #[tokio::test]
    async fn test_cast_remove_succeeds_with_later_timestamp_vs_cast_remove() {
        let (store, db, _temp_dir) = create_test_store();

        let timestamp1 = time::farcaster_time();

        let target_hash = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
        ];
        let cast_remove1 = messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &target_hash,
            Some(timestamp1),
            None,
        );
        let cast_remove2 = messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &target_hash,
            Some(timestamp1 + 1), // Later timestamp
            None,
        );

        // Merge first remove
        merge_messages(&store, &db, vec![&cast_remove1, &cast_remove2]);

        // Verify the second remove replaced the first
        let retrieved = CastStore::get_cast_remove(&store, FID_FOR_TEST, target_hash);
        assert!(retrieved.is_ok());
        let retrieved_remove = retrieved.unwrap().unwrap();
        assert_eq!(retrieved_remove, cast_remove2);
    }

    #[tokio::test]
    async fn test_cast_remove_fails_with_earlier_timestamp_vs_cast_remove() {
        let (store, db, _temp_dir) = create_test_store();

        let timestamp1 = time::farcaster_time();
        let timestamp2 = timestamp1 - 1;

        let target_hash = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
        ];
        let cast_remove1 = messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &target_hash,
            Some(timestamp1),
            None,
        );
        let cast_remove2 = messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &target_hash,
            Some(timestamp2), // Earlier timestamp
            None,
        );

        // Merge first remove (later timestamp)
        merge_messages(&store, &db, vec![&cast_remove1]);

        // Try to merge second remove with earlier timestamp - should fail
        let mut txn2 = RocksDbTransactionBatch::new();
        let result2 = store.merge(&cast_remove2, &mut txn2);
        assert!(result2.is_err());
        let error = result2.unwrap_err();
        assert_eq!(error.code, "bad_request.conflict");
        assert_eq!(error.message, "message conflicts with a more recent remove");
    }

    #[tokio::test]
    async fn test_cast_remove_succeeds_with_later_hash_vs_cast_remove() {
        let (store, db, _temp_dir) = create_test_store();

        let timestamp = time::farcaster_time();
        let target_hash = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
        ];

        let cast_remove1 = messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &target_hash,
            Some(timestamp),
            None,
        );

        // Create second remove with same timestamp but later hash (lexicographically)
        let mut cast_remove2 = cast_remove1.clone();
        cast_remove2.hash = vec![255; 20]; // Lexicographically later hash

        // Merge first remove
        merge_messages(&store, &db, vec![&cast_remove1, &cast_remove2]);

        // Verify the second remove replaced the first
        let retrieved = CastStore::get_cast_remove(&store, FID_FOR_TEST, target_hash);
        assert!(retrieved.is_ok());
        let retrieved_remove = retrieved.unwrap().unwrap();
        assert_eq!(retrieved_remove, cast_remove2);
    }

    #[tokio::test]
    async fn test_cast_remove_fails_with_earlier_hash_vs_cast_remove() {
        let (store, db, _temp_dir) = create_test_store();

        let timestamp = time::farcaster_time();
        let target_hash = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
        ];

        let cast_remove1 = messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &target_hash,
            Some(timestamp),
            None,
        );

        // Create second remove with same timestamp but earlier hash (lexicographically)
        let mut cast_remove2 = cast_remove1.clone();
        cast_remove2.hash = vec![0; 20]; // Lexicographically earlier hash

        // Merge first remove (with later hash)
        merge_messages(&store, &db, vec![&cast_remove1]);

        // Try to merge second remove with earlier hash - should fail
        let mut txn2 = RocksDbTransactionBatch::new();
        let result2 = store.merge(&cast_remove2, &mut txn2);
        assert!(result2.is_err());
        let error = result2.unwrap_err();
        assert_eq!(error.code, "bad_request.conflict");
        assert_eq!(error.message, "message conflicts with a more recent remove");
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

        merge_messages(&store, &db, vec![&cast_remove]);

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
    async fn test_cast_remove_vs_cast_add_succeeds_with_earlier_hash() {
        let (store, db, _temp_dir) = create_test_store();

        let timestamp = time::farcaster_time();
        let cast_add = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "Test cast",
            Some(timestamp),
            None,
        );
        let mut cast_remove = messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &cast_add.hash,
            Some(timestamp), // Same timestamp
            None,
        );
        // Make sure remove has earlier hash than add (lexicographically)
        cast_remove.hash = vec![0; 20];

        // Merge add first
        merge_messages(&store, &db, vec![&cast_add, &cast_remove]);

        // Verify remove exists and add is gone
        let remove_result = CastStore::get_cast_remove(&store, FID_FOR_TEST, cast_add.hash.clone());
        assert!(remove_result.is_ok());
        assert!(remove_result.unwrap().is_some());

        let add_result = CastStore::get_cast_add(&store, FID_FOR_TEST, cast_add.hash.clone());
        assert!(add_result.is_ok());
        assert!(add_result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_cast_remove_vs_cast_add_succeeds_with_later_hash() {
        let (store, db, _temp_dir) = create_test_store();

        let timestamp = time::farcaster_time();
        let cast_add = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "Test cast",
            Some(timestamp),
            None,
        );
        let mut cast_remove = messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &cast_add.hash,
            Some(timestamp), // Same timestamp
            None,
        );
        // Make sure remove has later hash than add (lexicographically)
        cast_remove.hash = vec![255; 20];

        // Merge add first
        merge_messages(&store, &db, vec![&cast_add, &cast_remove]);

        // Verify remove exists and add is gone
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

        merge_messages(&store, &db, vec![&cast_add]);

        revoke_message(&store, &db, &cast_add);

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

        merge_messages(&store, &db, vec![&cast_remove]);
        revoke_message(&store, &db, &cast_remove);

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
        revoke_message(&store, &db, &cast_add);

        let get_result = CastStore::get_cast_add(&store, FID_FOR_TEST, cast_add.hash.clone());
        assert!(get_result.is_ok());
        assert!(get_result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_revoke_deletes_all_keys_relating_to_cast_with_parent_url() {
        let (store, db, _temp_dir) = create_test_store();

        let timestamp = time::farcaster_time();
        let parent_url = "https://example.com/post/123".to_string();
        let mention_fid = 5678u64;

        // Create a cast with parent URL and mentions
        let cast_add = messages_factory::casts::create_cast_add_rich(
            FID_FOR_TEST,
            "Cast with parent URL and mentions",
            Some(CastType::Cast),
            vec![],
            None,
            Some(timestamp),
            None,
        );

        // Manually set the parent URL and mentions
        let mut cast_with_parent_and_mentions = cast_add.clone();
        if let Some(message::MessageData {
            body: Some(message::message_data::Body::CastAddBody(ref mut cast_body)),
            ..
        }) = &mut cast_with_parent_and_mentions.data
        {
            cast_body.parent = Some(message::cast_add_body::Parent::ParentUrl(
                parent_url.clone(),
            ));
            cast_body.mentions = vec![mention_fid];
        }

        // Merge the cast
        merge_messages(&store, &db, vec![&cast_with_parent_and_mentions]);

        // Verify cast exists
        let result = CastStore::get_cast_add(&store, FID_FOR_TEST, cast_add.hash.clone());
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());

        // Verify cast is found by parent URL
        let parent = message::cast_add_body::Parent::ParentUrl(parent_url);
        let parent_result =
            CastStore::get_casts_by_parent(&store, &parent, &PageOptions::default());
        assert!(parent_result.is_ok());
        assert_eq!(parent_result.unwrap().messages.len(), 1);

        // Verify cast is found by mention
        let mention_result =
            CastStore::get_casts_by_mention(&store, mention_fid, &PageOptions::default());
        assert!(mention_result.is_ok());
        assert_eq!(mention_result.unwrap().messages.len(), 1);

        // Revoke the cast
        revoke_message(&store, &db, &cast_with_parent_and_mentions);

        // Verify cast no longer exists
        let result = CastStore::get_cast_add(&store, FID_FOR_TEST, cast_add.hash.clone());
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());

        // Verify cast is no longer found by parent URL
        let parent =
            message::cast_add_body::Parent::ParentUrl("https://example.com/post/123".to_string());
        let parent_result =
            CastStore::get_casts_by_parent(&store, &parent, &PageOptions::default());
        assert!(parent_result.is_ok());
        assert_eq!(parent_result.unwrap().messages.len(), 0);

        // Verify cast is no longer found by mention
        let mention_result =
            CastStore::get_casts_by_mention(&store, mention_fid, &PageOptions::default());
        assert!(mention_result.is_ok());
        assert_eq!(mention_result.unwrap().messages.len(), 0);
    }

    #[tokio::test]
    async fn test_prune_messages_with_size_limit() {
        let (store, db, _temp_dir) = create_test_store_with_prune_limit(3);

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
        merge_messages(&store, &db, cast_adds.iter().collect());

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

    #[tokio::test]
    async fn test_merge_accepts_message_which_would_be_immediately_pruned() {
        let (store, db, _temp_dir) = create_test_store_with_prune_limit(3);

        let timestamp = time::farcaster_time();

        // Create and merge 3 cast add messages (at the limit)
        let mut cast_adds = vec![];
        for i in 0..3 {
            let cast_add = messages_factory::casts::create_cast_add(
                FID_FOR_TEST,
                &format!("Cast message {}", i),
                Some(timestamp + i + 10), // Use timestamps well in the future
                None,
            );
            cast_adds.push(cast_add);
        }

        // Merge all messages
        merge_messages(&store, &db, cast_adds.iter().collect());

        // Try to merge a message with an older timestamp that would be immediately pruned
        let old_cast_add = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "Old cast message",
            Some(timestamp), // Much older timestamp
            None,
        );

        let mut txn = RocksDbTransactionBatch::new();
        let result = store.merge(&old_cast_add, &mut txn);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_prune_messages_earliest_add_messages() {
        let (store, db, _temp_dir) = create_test_store_with_prune_limit(3);

        let timestamp = time::farcaster_time();

        // Create 5 cast add messages with different timestamps
        let mut cast_adds = vec![];
        for i in 0..5 {
            let cast_add = messages_factory::casts::create_cast_add(
                FID_FOR_TEST,
                &format!("Bundle cast message {}", i),
                Some(timestamp + i),
                None,
            );
            cast_adds.push(cast_add);
        }

        // Merge all messages in a single transaction
        merge_messages(&store, &db, cast_adds.iter().collect());

        // Prune messages
        let mut txn = RocksDbTransactionBatch::new();
        let result = store.prune_messages(FID_FOR_TEST, 5, 3, &mut txn);
        assert!(result.is_ok());
        db.commit(txn).unwrap();

        // Verify that only the latest 3 messages remain
        let page_options = PageOptions::default();
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
    async fn test_prune_messages_earliest_remove_messages() {
        let (store, db, _temp_dir) = create_test_store_with_prune_limit(3);

        let timestamp = time::farcaster_time();

        // Create 5 cast remove messages with different timestamps
        let mut cast_removes = vec![];
        for i in 0..5 {
            let target_hash: Vec<u8> = (0..20).map(|j| (i * 20 + j) as u8).collect();
            let cast_remove = messages_factory::casts::create_cast_remove(
                FID_FOR_TEST,
                &target_hash,
                Some(timestamp + i),
                None,
            );
            cast_removes.push(cast_remove);
        }

        // Merge all remove messages
        merge_messages(&store, &db, cast_removes.iter().collect());

        // Prune messages
        let mut txn = RocksDbTransactionBatch::new();
        let result = store.prune_messages(FID_FOR_TEST, 5, 3, &mut txn);
        assert!(result.is_ok());
        db.commit(txn).unwrap();

        // Verify that only the latest 3 messages remain
        let page_options = PageOptions::default();
        let remaining_messages =
            CastStore::get_cast_removes_by_fid(&store, FID_FOR_TEST, &page_options);
        assert!(remaining_messages.is_ok());
        let page = remaining_messages.unwrap();
        assert_eq!(page.messages.len(), 3);

        // Verify the earliest 2 remove messages were pruned
        for i in 0..2 {
            let target_hash: Vec<u8> = (0..20).map(|j| (i * 20 + j) as u8).collect();
            let result = CastStore::get_cast_remove(&store, FID_FOR_TEST, target_hash);
            assert!(result.is_ok());
            assert!(result.unwrap().is_none());
        }

        // Verify the latest 3 remove messages still exist
        for i in 2..5 {
            let target_hash: Vec<u8> = (0..20).map(|j| (i * 20 + j) as u8).collect();
            let result = CastStore::get_cast_remove(&store, FID_FOR_TEST, target_hash);
            assert!(result.is_ok());
            assert!(result.unwrap().is_some());
        }
    }

    #[tokio::test]
    async fn test_prune_messages_earliest_mixed_messages() {
        let (store, db, _temp_dir) = create_test_store_with_prune_limit(3);

        let timestamp = time::farcaster_time();

        // Create mix of add and remove messages
        let cast_add1 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "Mixed message 1",
            Some(timestamp + 1),
            None,
        );
        let cast_remove1 = messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &vec![
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
            ],
            Some(timestamp + 2),
            None,
        );
        let cast_add2 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "Mixed message 2",
            Some(timestamp + 3),
            None,
        );
        let cast_remove2 = messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &vec![
                6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
            ],
            Some(timestamp + 4),
            None,
        );
        let cast_add3 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "Mixed message 3",
            Some(timestamp + 5),
            None,
        );

        // Merge all messages
        let messages = vec![
            &cast_add1,
            &cast_remove1,
            &cast_add2,
            &cast_remove2,
            &cast_add3,
        ];
        merge_messages(&store, &db, messages);

        // Prune messages (keep only 3)
        let mut txn = RocksDbTransactionBatch::new();
        let result = store.prune_messages(FID_FOR_TEST, 5, 3, &mut txn);
        assert!(result.is_ok());
        db.commit(txn).unwrap();

        // Verify total remaining messages
        let add_page =
            CastStore::get_cast_adds_by_fid(&store, FID_FOR_TEST, &PageOptions::default()).unwrap();
        let remove_page =
            CastStore::get_cast_removes_by_fid(&store, FID_FOR_TEST, &PageOptions::default())
                .unwrap();
        let total_remaining = add_page.messages.len() + remove_page.messages.len();
        assert_eq!(total_remaining, 3);

        // Verify the earliest messages were pruned (add1 and remove1)
        let add1_result = CastStore::get_cast_add(&store, FID_FOR_TEST, cast_add1.hash);
        assert!(add1_result.is_ok());
        assert!(add1_result.unwrap().is_none());

        let remove1_result = CastStore::get_cast_remove(
            &store,
            FID_FOR_TEST,
            vec![
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
            ],
        );
        assert!(remove1_result.is_ok());
        assert!(remove1_result.unwrap().is_none());
    }
}
