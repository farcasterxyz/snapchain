#[cfg(test)]
mod tests {
    use super::super::super::test_helper::FID_FOR_TEST;
    use crate::hyper::StateContext;
    use crate::proto::cast_add_body::Parent;
    use crate::proto::reaction_body::Target;
    use crate::proto::{self as message, hub_event, CastType, HubEvent, HubEventType};
    use crate::storage::db::{PageOptions, RocksDB, RocksDbTransactionBatch};
    use crate::storage::store::account::{
        CastStore, CastStoreDef, HubEventStorageExt, Store, StoreEventHandler, StoreOptions,
    };
    use crate::utils::factory::{messages_factory, time};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_store() -> (Store<CastStoreDef>, Arc<RocksDB>, TempDir) {
        create_test_store_with_prune_limit(10)
    }

    fn create_conflict_free_test_store() -> (Store<CastStoreDef>, Arc<RocksDB>, TempDir) {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = RocksDB::new(db_path.to_str().unwrap());
        db.open().unwrap();
        let db = Arc::new(db);

        let event_handler = StoreEventHandler::new();
        let store = CastStore::new_with_opts(
            db.clone(),
            event_handler.clone(),
            10,
            StoreOptions {
                conflict_free: true,
                save_hub_events: false,
            },
        );

        (store, db.clone(), temp_dir)
    }

    fn create_test_store_with_prune_limit(
        prune_size_limit: u32,
    ) -> (Store<CastStoreDef>, Arc<RocksDB>, TempDir) {
        create_test_store_with_prune_limit_ctx(prune_size_limit, StateContext::Legacy)
    }

    fn create_test_store_with_prune_limit_ctx(
        prune_size_limit: u32,
        ctx: StateContext,
    ) -> (Store<CastStoreDef>, Arc<RocksDB>, TempDir) {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = RocksDB::new(db_path.to_str().unwrap());
        db.open().unwrap();
        let db = Arc::new(db);

        let event_handler = StoreEventHandler::new();
        let store = Store::new_with_store_def_ctx(
            db.clone(),
            event_handler.clone(),
            CastStoreDef::new(prune_size_limit),
            StoreOptions::default(),
            ctx,
        );

        (store, db.clone(), temp_dir)
    }

    fn merge_message_failure(
        store: &Store<CastStoreDef>,
        message: &message::Message,
        err_code: String,
        err_message: String,
    ) {
        let mut txn = RocksDbTransactionBatch::new();
        let result = store.merge(&message, &mut txn);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.code, err_code);
        assert_eq!(error.message, err_message);
    }

    fn merge_message_with_conflicts(
        store: &Store<CastStoreDef>,
        db: &Arc<RocksDB>,
        message: &message::Message,
        deleted_messages: Vec<message::Message>,
    ) {
        let mut txn = RocksDbTransactionBatch::new();
        let result = store.merge(message, &mut txn).unwrap();
        assert_eq!(result.r#type(), HubEventType::MergeMessage);
        match &result.body {
            Some(hub_event::Body::MergeMessageBody(body)) => {
                assert_eq!(*body.message.as_ref().unwrap(), *message);
                assert_eq!(*body.deleted_messages, deleted_messages);
            }
            _ => {
                panic!("Unexpected event")
            }
        }
        db.commit(txn).unwrap();
    }

    fn merge_messages(
        store: &Store<CastStoreDef>,
        db: &Arc<RocksDB>,
        messages: Vec<&message::Message>,
    ) -> Vec<HubEvent> {
        let mut txn = RocksDbTransactionBatch::new();
        let mut events = Vec::new();

        for message in messages {
            let result = store.merge(message, &mut txn).unwrap();
            events.push(result.clone());
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
        events
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
        assert!(result.unwrap().is_none());

        // Test with invalid hash
        let invalid_hash = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
        ];
        let result = CastStore::get_cast_add(&store, FID_FOR_TEST, invalid_hash);
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_get_cast_add_succeeds_with_message() {
        let (store, db, _temp_dir) = create_test_store();

        let cast_add =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "Test cast", None, None);

        merge_messages(&store, &db, vec![&cast_add]);
        let retrieved_cast = CastStore::get_cast_add(&store, FID_FOR_TEST, cast_add.hash.clone())
            .unwrap()
            .unwrap();
        assert_eq!(retrieved_cast, cast_add);
    }

    #[tokio::test]
    async fn test_get_cast_remove_fails_if_missing() {
        let (store, _db, _temp_dir) = create_test_store();

        let target_hash = (0..20).map(|_| rand::random::<u8>()).collect();

        let result = CastStore::get_cast_remove(&store, FID_FOR_TEST, target_hash);
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
        assert!(result.unwrap().is_none());

        // Test with invalid hash
        let invalid_hash = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
        ];
        let result = CastStore::get_cast_remove(&store, FID_FOR_TEST, invalid_hash);
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_get_cast_remove_succeeds() {
        let (store, db, _temp_dir) = create_test_store();

        let target_hash = (0..20).map(|_| rand::random::<u8>()).collect();
        let cast_remove =
            messages_factory::casts::create_cast_remove(FID_FOR_TEST, &target_hash, None, None);

        merge_messages(&store, &db, vec![&cast_remove]);

        let retrieved_remove = CastStore::get_cast_remove(&store, FID_FOR_TEST, target_hash)
            .unwrap()
            .unwrap();
        assert_eq!(retrieved_remove, cast_remove);
    }

    #[tokio::test]
    async fn test_get_cast_adds_by_fid_returns_cast_adds() {
        let (store, db, _temp_dir) = create_test_store();

        let cast_add =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "Test cast", None, None);

        merge_messages(&store, &db, vec![&cast_add]);

        let page =
            CastStore::get_cast_adds_by_fid(&store, FID_FOR_TEST, &PageOptions::default()).unwrap();

        assert_eq!(page.messages, vec![cast_add]);
        assert!(page.next_page_token.is_none());
    }

    #[tokio::test]
    async fn test_get_cast_adds_by_fid_with_invalid_fid() {
        let (store, db, _temp_dir) = create_test_store();

        let cast_add =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "Test cast", None, None);

        merge_messages(&store, &db, vec![&cast_add]);

        let invalid_fid = 999999;
        let page =
            CastStore::get_cast_adds_by_fid(&store, invalid_fid, &PageOptions::default()).unwrap();

        assert_eq!(page.messages.len(), 0);
        assert!(page.next_page_token.is_none());
    }

    #[tokio::test]
    async fn test_get_cast_adds_by_fid_returns_empty_without_messages() {
        let (store, _db, _temp_dir) = create_test_store();

        let page =
            CastStore::get_cast_adds_by_fid(&store, FID_FOR_TEST, &PageOptions::default()).unwrap();

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
        let result =
            CastStore::get_cast_adds_by_fid(&store, FID_FOR_TEST, &PageOptions::default()).unwrap();
        assert_eq!(result.messages, vec![cast_add1.clone(), cast_add2.clone()]);

        // Test pagination with page size 1
        let page_options = PageOptions {
            page_size: Some(1),
            page_token: None,
            reverse: false,
        };
        let result1 = CastStore::get_cast_adds_by_fid(&store, FID_FOR_TEST, &page_options).unwrap();
        assert_eq!(result1.messages, vec![cast_add1.clone()]);
        assert!(result1.next_page_token.is_some());

        // Get second page
        let page_options2 = PageOptions {
            page_size: None,
            page_token: result1.next_page_token,
            reverse: false,
        };
        let result2 =
            CastStore::get_cast_adds_by_fid(&store, FID_FOR_TEST, &page_options2).unwrap();
        assert_eq!(result2.messages, vec![cast_add2.clone()]);
        assert!(result2.next_page_token.is_none());
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
            CastStore::get_cast_removes_by_fid(&store, invalid_fid, &PageOptions::default())
                .unwrap();

        assert_eq!(result.messages.len(), 0);
        assert!(result.next_page_token.is_none());
    }

    #[tokio::test]
    async fn test_get_cast_removes_by_fid_returns_empty_without_messages() {
        let (store, _db, _temp_dir) = create_test_store();

        let result =
            CastStore::get_cast_removes_by_fid(&store, FID_FOR_TEST, &PageOptions::default())
                .unwrap();

        assert_eq!(result.messages.len(), 0);
        assert!(result.next_page_token.is_none());
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
            CastStore::get_cast_removes_by_fid(&store, FID_FOR_TEST, &PageOptions::default())
                .unwrap();
        assert_eq!(
            result.messages,
            vec![cast_remove1.clone(), cast_remove2.clone()]
        );

        // Test pagination with page size 1
        let page_options = PageOptions {
            page_size: Some(1),
            page_token: None,
            reverse: false,
        };
        let result1 =
            CastStore::get_cast_removes_by_fid(&store, FID_FOR_TEST, &page_options).unwrap();
        assert_eq!(result1.messages, vec![cast_remove1.clone()]);
        assert!(result1.next_page_token.is_some());

        // Get second page
        let page_options2 = PageOptions {
            page_size: None,
            page_token: result1.next_page_token,
            reverse: false,
        };
        let result2 =
            CastStore::get_cast_removes_by_fid(&store, FID_FOR_TEST, &page_options2).unwrap();
        assert_eq!(result2.messages, vec![cast_remove2.clone()]);
        assert!(result2.next_page_token.is_none());
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

        let result =
            CastStore::get_casts_by_parent(&store, &parent, &PageOptions::default()).unwrap();

        assert_eq!(result.messages.len(), 0);
        assert!(result.next_page_token.is_none());
    }

    #[tokio::test]
    async fn test_get_casts_by_parent_returns_empty_for_different_parent() {
        let (store, db, _temp_dir) = create_test_store();

        // Create a cast with a parent
        let parent = Parent::ParentCastId(message::CastId {
            fid: 1234,
            hash: vec![
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
            ],
        });

        let cast_with_parent = messages_factory::casts::create_cast_add_rich(
            FID_FOR_TEST,
            "Child cast",
            Some(CastType::Cast),
            vec![],
            Some(parent.clone()),
            vec![],
            None,
            None,
        );

        merge_messages(&store, &db, vec![&cast_with_parent]);

        // Query with a different parent
        let different_parent = Parent::ParentCastId(message::CastId {
            fid: 5678,
            hash: vec![
                6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
            ],
        });

        let result =
            CastStore::get_casts_by_parent(&store, &different_parent, &PageOptions::default())
                .unwrap();
        assert_eq!(result.messages.len(), 0);

        // Test with parent URL
        let parent_url =
            message::cast_add_body::Parent::ParentUrl("https://example.com".to_string());
        let result =
            CastStore::get_casts_by_parent(&store, &parent_url, &PageOptions::default()).unwrap();
        assert_eq!(result.messages.len(), 0);
    }

    #[tokio::test]
    async fn test_get_casts_by_parent_returns_casts_by_parent_cast_id() {
        let (store, db, _temp_dir) = create_test_store();

        let timestamp1 = time::farcaster_time();

        let parent = Parent::ParentCastId(message::CastId {
            fid: 1234,
            hash: vec![
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
            ],
        });

        let cast1 = messages_factory::casts::create_cast_add_rich(
            FID_FOR_TEST,
            "First child cast",
            Some(CastType::Cast),
            vec![],
            Some(parent.clone()),
            vec![],
            Some(timestamp1),
            None,
        );

        let cast2 = messages_factory::casts::create_cast_add_rich(
            FID_FOR_TEST,
            "Second child cast",
            Some(CastType::Cast),
            vec![],
            Some(parent.clone()),
            vec![],
            Some(timestamp1 + 1),
            None,
        );

        merge_messages(&store, &db, vec![&cast1, &cast2]);

        let page_options = PageOptions {
            page_size: None,
            page_token: None,
            reverse: false,
        };
        let result = CastStore::get_casts_by_parent(&store, &parent, &page_options).unwrap();

        assert_eq!(result.messages, vec![cast1.clone(), cast2.clone()]);
        assert!(result.next_page_token.is_none());

        // Test pagination
        let page_options = PageOptions {
            page_size: Some(1),
            page_token: None,
            reverse: false,
        };
        let result1 = CastStore::get_casts_by_parent(&store, &parent, &page_options).unwrap();
        assert_eq!(result1.messages, vec![cast1.clone()]);

        let page_options2 = PageOptions {
            page_size: None,
            page_token: result1.next_page_token,
            reverse: false,
        };
        let result2 = CastStore::get_casts_by_parent(&store, &parent, &page_options2).unwrap();
        assert_eq!(result2.messages, vec![cast2.clone()]);

        // Test reverse order
        let page_options_reverse = PageOptions {
            page_size: None,
            page_token: None,
            reverse: true,
        };
        let result3 =
            CastStore::get_casts_by_parent(&store, &parent, &page_options_reverse).unwrap();
        assert_eq!(result3.messages, vec![cast2, cast1]);
    }

    #[tokio::test]
    async fn test_get_casts_by_parent_returns_casts_by_parent_url() {
        let (store, db, _temp_dir) = create_test_store();

        let timestamp1 = time::farcaster_time();
        let timestamp2 = timestamp1 + 1;
        let parent = Parent::ParentUrl("https://example.com/post/123".to_string());

        let cast1 = messages_factory::casts::create_cast_add_rich(
            FID_FOR_TEST,
            "First reply to URL",
            Some(CastType::Cast),
            vec![],
            Some(parent.clone()),
            vec![],
            Some(timestamp1),
            None,
        );

        let cast2 = messages_factory::casts::create_cast_add_rich(
            FID_FOR_TEST,
            "Second reply to URL",
            Some(CastType::Cast),
            vec![],
            Some(parent.clone()),
            vec![],
            Some(timestamp2),
            None,
        );

        merge_messages(&store, &db, vec![&cast1, &cast2]);

        let page =
            CastStore::get_casts_by_parent(&store, &parent, &PageOptions::default()).unwrap();

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
        assert_eq!(page1.messages, vec![cast1.clone()]);

        let page_options2 = PageOptions {
            page_size: None,
            page_token: page1.next_page_token,
            reverse: false,
        };
        let result2 = CastStore::get_casts_by_parent(&store, &parent, &page_options2);
        assert!(result2.is_ok());
        let page2 = result2.unwrap();
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
        assert_eq!(page3.messages, vec![cast2, cast1]);
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
            CastStore::get_casts_by_mention(&store, different_mention_fid, &PageOptions::default())
                .unwrap();
        assert_eq!(result.messages.len(), 0);
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
            vec![mention_fid],
            Some(timestamp1),
            None,
        );

        let cast2 = messages_factory::casts::create_cast_add_rich(
            FID_FOR_TEST,
            "Another cast with mention",
            Some(CastType::Cast),
            vec![],
            None,
            vec![mention_fid],
            Some(timestamp1 + 1),
            None,
        );

        merge_messages(&store, &db, vec![&cast1, &cast2]);

        let result = CastStore::get_casts_by_mention(&store, mention_fid, &PageOptions::default());

        assert!(result.is_ok());
        let page = result.unwrap();
        assert_eq!(page.messages, vec![cast1.clone(), cast2.clone()]);
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
        assert_eq!(page1.messages, vec![cast1.clone()]);

        let page_options2 = PageOptions {
            page_size: None,
            page_token: page1.next_page_token,
            reverse: false,
        };
        let page2 = CastStore::get_casts_by_mention(&store, mention_fid, &page_options2).unwrap();
        assert_eq!(page2.messages, vec![cast2.clone()]);

        // Test reverse order
        let page_options_reverse = PageOptions {
            page_size: None,
            page_token: None,
            reverse: true,
        };
        let page3 =
            CastStore::get_casts_by_mention(&store, mention_fid, &page_options_reverse).unwrap();
        assert_eq!(page3.messages, vec![cast2, cast1]);
    }

    #[tokio::test]
    async fn test_merge_fails_with_invalid_message_type() {
        let (store, _db, _temp_dir) = create_test_store();

        let reaction_add = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            message::ReactionType::Like,
            Target::TargetUrl("target".to_string()),
            None,
            None,
        );

        merge_message_failure(
            &store,
            &reaction_add,
            "bad_request.validation_failure".to_string(),
            "invalid message type".to_string(),
        );
    }

    #[tokio::test]
    async fn test_merge_cast_add_succeeds() {
        let (store, db, _temp_dir) = create_test_store();

        let cast_add =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "Test cast", None, None);

        merge_messages(&store, &db, vec![&cast_add]);

        // Verify the cast was stored
        let retrieved = CastStore::get_cast_add(&store, FID_FOR_TEST, cast_add.hash.clone());
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
        assert!(retrieved.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_merge_cast_add_fails_if_merged_twice() {
        let (store, db, _temp_dir) = create_test_store();

        let cast_add =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "Test cast", None, None);

        merge_messages(&store, &db, vec![&cast_add]);

        merge_message_failure(
            &store,
            &cast_add,
            "bad_request.duplicate".to_string(),
            "message has already been merged".to_string(),
        );
    }

    #[tokio::test]
    async fn test_merge_cast_add_dup_success_for_conflict_free() {
        let (store, db, _temp_dir) = create_conflict_free_test_store();

        let cast_add =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "Test cast", None, None);

        let events = merge_messages(&store, &db, vec![&cast_add]);
        let id1 = events[0].id;

        // Merging the same message again should work, but overwrite the existing one
        let events = merge_messages(&store, &db, vec![&cast_add]);
        let id2 = events[0].id;

        // Since we set generate hub events to false, both ids should be 0
        assert_eq!(id1, 0);
        assert_eq!(id2, 0);

        // Read that there is only 1 message in the DB
        let messages =
            CastStore::get_cast_adds_by_fid(&store, FID_FOR_TEST, &PageOptions::default()).unwrap();
        assert_eq!(messages.messages.len(), 1);

        let db_hub_events = HubEvent::get_events(db, id1, None, None).unwrap();

        // No events are saved to DB
        assert_eq!(db_hub_events.events.len(), 0);
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
        merge_message_failure(
            &store,
            &cast_add,
            "bad_request.conflict".to_string(),
            "message conflicts with a more recent remove".to_string(),
        );
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
        merge_message_failure(
            &store,
            &cast_add,
            "bad_request.conflict".to_string(),
            "message conflicts with a more recent remove".to_string(),
        );
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
        merge_message_failure(
            &store,
            &cast_add,
            "bad_request.conflict".to_string(),
            "message conflicts with a more recent remove".to_string(),
        );
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
        merge_message_failure(
            &store,
            &cast_add,
            "bad_request.conflict".to_string(),
            "message conflicts with a more recent remove".to_string(),
        );
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
        merge_messages(&store, &db, vec![&cast_add]);
        merge_message_with_conflicts(&store, &db, &cast_remove, vec![cast_add.clone()]);

        // Verify the remove was stored and the add was removed
        let remove_result = CastStore::get_cast_remove(&store, FID_FOR_TEST, cast_add.hash.clone());
        assert!(remove_result.unwrap().is_some());

        let add_result = CastStore::get_cast_add(&store, FID_FOR_TEST, cast_add.hash.clone());
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
        merge_messages(&store, &db, vec![&cast_add]);
        merge_message_with_conflicts(&store, &db, &cast_remove, vec![cast_add.clone()]);

        // Verify the remove was stored and the add was removed
        let remove_result = CastStore::get_cast_remove(&store, FID_FOR_TEST, cast_add.hash.clone());
        assert!(remove_result.unwrap().is_some());

        let add_result = CastStore::get_cast_add(&store, FID_FOR_TEST, cast_add.hash.clone());
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

        merge_messages(&store, &db, vec![&cast_remove]);

        merge_message_failure(
            &store,
            &cast_remove,
            "bad_request.duplicate".to_string(),
            "message has already been merged".to_string(),
        );
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
        merge_messages(&store, &db, vec![&cast_add]);
        merge_message_with_conflicts(&store, &db, &cast_remove, vec![cast_add.clone()]);

        // Verify remove exists and add is gone
        let remove_result = CastStore::get_cast_remove(&store, FID_FOR_TEST, cast_add.hash.clone());
        assert!(remove_result.unwrap().is_some());

        let add_result = CastStore::get_cast_add(&store, FID_FOR_TEST, cast_add.hash.clone());
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
        merge_message_failure(
            &store,
            &cast_remove2,
            "bad_request.conflict".to_string(),
            "message conflicts with a more recent remove".to_string(),
        );
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
        merge_messages(&store, &db, vec![&cast_remove1]);
        merge_message_with_conflicts(&store, &db, &cast_remove2, vec![cast_remove1.clone()]);

        // Verify the second remove replaced the first
        let retrieved_remove = CastStore::get_cast_remove(&store, FID_FOR_TEST, target_hash)
            .unwrap()
            .unwrap();
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
        merge_message_failure(
            &store,
            &cast_remove2,
            "bad_request.conflict".to_string(),
            "message conflicts with a more recent remove".to_string(),
        );
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
        merge_messages(&store, &db, vec![&cast_remove1]);
        merge_message_with_conflicts(&store, &db, &cast_remove2, vec![cast_remove1.clone()]);

        // Verify the second remove replaced the first
        let retrieved_remove = CastStore::get_cast_remove(&store, FID_FOR_TEST, target_hash)
            .unwrap()
            .unwrap();
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
        merge_message_failure(
            &store,
            &cast_remove2,
            "bad_request.conflict".to_string(),
            "message conflicts with a more recent remove".to_string(),
        );
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

        merge_message_failure(
            &store,
            &cast_add,
            "bad_request.conflict".to_string(),
            "message conflicts with a more recent remove".to_string(),
        );

        // Verify the remove still exists and add doesn't
        let remove_result = CastStore::get_cast_remove(&store, FID_FOR_TEST, cast_add.hash.clone());
        assert!(remove_result.unwrap().is_some());

        let add_result = CastStore::get_cast_add(&store, FID_FOR_TEST, cast_add.hash.clone());
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

        merge_message_failure(
            &store,
            &cast_add,
            "bad_request.conflict".to_string(),
            "message conflicts with a more recent remove".to_string(),
        );

        // Verify the remove still exists and add doesn't
        let remove_result = CastStore::get_cast_remove(&store, FID_FOR_TEST, cast_add.hash.clone());
        assert!(remove_result.unwrap().is_some());

        let add_result = CastStore::get_cast_add(&store, FID_FOR_TEST, cast_add.hash.clone());
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
        merge_messages(&store, &db, vec![&cast_add]);
        merge_message_with_conflicts(&store, &db, &cast_remove, vec![cast_add.clone()]);

        // Verify remove exists and add is gone
        let remove_result = CastStore::get_cast_remove(&store, FID_FOR_TEST, cast_add.hash.clone());
        assert!(remove_result.unwrap().is_some());

        let add_result = CastStore::get_cast_add(&store, FID_FOR_TEST, cast_add.hash.clone());
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
        merge_messages(&store, &db, vec![&cast_add]);
        merge_message_with_conflicts(&store, &db, &cast_remove, vec![cast_add.clone()]);

        // Verify remove exists and add is gone
        let remove_result = CastStore::get_cast_remove(&store, FID_FOR_TEST, cast_add.hash.clone());
        assert!(remove_result.unwrap().is_some());

        let add_result = CastStore::get_cast_add(&store, FID_FOR_TEST, cast_add.hash.clone());
        assert!(add_result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_revoke_fails_with_invalid_message_type() {
        let (store, _db, _temp_dir) = create_test_store();

        let reaction_add = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            message::ReactionType::Like,
            Target::TargetUrl("target".to_string()),
            None,
            None,
        );

        let mut txn = RocksDbTransactionBatch::new();
        let error = store.revoke(&reaction_add, &mut txn).unwrap_err();
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
        assert!(get_result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_revoke_deletes_all_keys_relating_to_cast_with_parent_url() {
        let (store, db, _temp_dir) = create_test_store();

        let timestamp = time::farcaster_time();
        let parent = Parent::ParentUrl("https://example.com/post/123".to_string());
        let mention_fid = 5678u64;

        // Create a cast with parent URL and mentions
        let cast_add = messages_factory::casts::create_cast_add_rich(
            FID_FOR_TEST,
            "Cast with parent URL and mentions",
            Some(CastType::Cast),
            vec![],
            Some(parent.clone()),
            vec![mention_fid],
            Some(timestamp),
            None,
        );

        // Merge the cast
        merge_messages(&store, &db, vec![&cast_add]);

        // Verify cast exists
        let result = CastStore::get_cast_add(&store, FID_FOR_TEST, cast_add.hash.clone());
        assert!(result.unwrap().is_some());

        // Verify cast is found by parent URL
        let parent_result =
            CastStore::get_casts_by_parent(&store, &parent, &PageOptions::default());
        assert_eq!(parent_result.unwrap().messages.len(), 1);

        // Verify cast is found by mention
        let mention_result =
            CastStore::get_casts_by_mention(&store, mention_fid, &PageOptions::default());
        assert_eq!(mention_result.unwrap().messages.len(), 1);

        // Revoke the cast
        revoke_message(&store, &db, &cast_add);

        // Verify cast no longer exists
        let result = CastStore::get_cast_add(&store, FID_FOR_TEST, cast_add.hash.clone());
        assert!(result.unwrap().is_none());

        // Verify cast is no longer found by parent URL
        let parent =
            message::cast_add_body::Parent::ParentUrl("https://example.com/post/123".to_string());
        let parent_result =
            CastStore::get_casts_by_parent(&store, &parent, &PageOptions::default());
        assert_eq!(parent_result.unwrap().messages.len(), 0);

        // Verify cast is no longer found by mention
        let mention_result =
            CastStore::get_casts_by_mention(&store, mention_fid, &PageOptions::default());
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
        let result = store.prune_messages(FID_FOR_TEST, 5, 3, &mut txn).unwrap();
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
            let event = &result[i];
            let message = &cast_adds[i];
            assert_eq!(event.r#type(), HubEventType::PruneMessage);
            match &event.body {
                Some(hub_event::Body::PruneMessageBody(body)) => {
                    assert_eq!(*body.message.as_ref().unwrap(), *message)
                }
                _ => {
                    panic!("Unexpected event")
                }
            }

            let result = CastStore::get_cast_add(&store, FID_FOR_TEST, cast_adds[i].hash.clone());
            assert!(result.unwrap().is_none());
        }

        // Verify the latest 3 messages still exist
        for i in 2..5 {
            let result = CastStore::get_cast_add(&store, FID_FOR_TEST, cast_adds[i].hash.clone());
            assert!(result.unwrap().is_some());
        }
    }

    #[tokio::test]
    async fn test_prune_messages_skipped_for_hyper_context() {
        let (store, db, _temp_dir) = create_test_store_with_prune_limit_ctx(1, StateContext::Hyper);

        let timestamp = time::farcaster_time();
        let first = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "first cast",
            Some(timestamp),
            None,
        );
        let second = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "second cast",
            Some(timestamp + 1),
            None,
        );

        merge_messages(&store, &db, vec![&first, &second]);

        let mut txn = RocksDbTransactionBatch::new();
        let result = store.prune_messages(FID_FOR_TEST, 2, 1, &mut txn).unwrap();
        assert!(result.is_empty());
        db.commit(txn).unwrap();

        let remaining =
            CastStore::get_cast_adds_by_fid(&store, FID_FOR_TEST, &PageOptions::default()).unwrap();
        assert_eq!(remaining.messages.len(), 2);
    }

    #[tokio::test]
    async fn test_hyper_store_retains_messages_after_legacy_prune() {
        let (legacy_store, legacy_db, _legacy_dir) =
            create_test_store_with_prune_limit_ctx(1, StateContext::Legacy);
        let (hyper_store, hyper_db, _hyper_dir) =
            create_test_store_with_prune_limit_ctx(1, StateContext::Hyper);

        let timestamp = time::farcaster_time();
        let first = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "legacy cast",
            Some(timestamp),
            None,
        );
        let second = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "hyper cast",
            Some(timestamp + 1),
            None,
        );

        merge_messages(&legacy_store, &legacy_db, vec![&first, &second]);
        let first_hyper = first.clone();
        let second_hyper = second.clone();
        merge_messages(&hyper_store, &hyper_db, vec![&first_hyper, &second_hyper]);

        let mut txn = RocksDbTransactionBatch::new();
        let pruned = legacy_store
            .prune_messages(FID_FOR_TEST, 2, 1, &mut txn)
            .unwrap();
        assert_eq!(pruned.len(), 1);
        legacy_db.commit(txn).unwrap();

        let legacy_first =
            CastStore::get_cast_add(&legacy_store, FID_FOR_TEST, first.hash.clone()).unwrap();
        assert!(legacy_first.is_none());
        let legacy_second =
            CastStore::get_cast_add(&legacy_store, FID_FOR_TEST, second.hash.clone()).unwrap();
        assert!(legacy_second.is_some());

        let hyper_first =
            CastStore::get_cast_add(&hyper_store, FID_FOR_TEST, first.hash.clone()).unwrap();
        assert!(hyper_first.is_some());
        let hyper_second =
            CastStore::get_cast_add(&hyper_store, FID_FOR_TEST, second.hash.clone()).unwrap();
        assert!(hyper_second.is_some());
    }

    #[tokio::test]
    async fn test_prune_messages_no_op_when_no_messages() {
        let (store, db, _temp_dir) = create_test_store();

        let mut txn = RocksDbTransactionBatch::new();
        let result = store.prune_messages(FID_FOR_TEST, 0, 10, &mut txn);
        assert_eq!(result.unwrap().len(), 0); // No messages pruned
        db.commit(txn).unwrap();
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
            cast_removes.push((cast_remove, target_hash));
        }

        // Merge all remove messages
        merge_messages(
            &store,
            &db,
            cast_removes.iter().map(|(msg, _)| msg).collect(),
        );

        // Prune messages
        let mut txn = RocksDbTransactionBatch::new();
        let result = store.prune_messages(FID_FOR_TEST, 5, 3, &mut txn).unwrap();
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
            let (message, target_hash) = &cast_removes[i];
            let event = &result[i];
            assert_eq!(event.r#type(), HubEventType::PruneMessage);
            match &event.body {
                Some(hub_event::Body::PruneMessageBody(body)) => {
                    assert_eq!(*body.message.as_ref().unwrap(), *message)
                }
                _ => {
                    panic!("Unexpected event")
                }
            }
            let result = CastStore::get_cast_remove(&store, FID_FOR_TEST, target_hash.clone());
            assert!(result.unwrap().is_none());
        }

        // Verify the latest 3 remove messages still exist
        for i in 2..5 {
            let (_, target_hash) = &cast_removes[i];
            let result = CastStore::get_cast_remove(&store, FID_FOR_TEST, target_hash.clone());
            assert!(result.unwrap().is_some());
        }
    }
}
