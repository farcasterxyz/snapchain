#[cfg(test)]
mod tests {
    use super::super::super::test_helper::FID_FOR_TEST;
    use crate::proto::link_body::Target;
    use crate::proto::{self as message, hub_event, HubEventType};
    use crate::storage::db::{PageOptions, RocksDB, RocksDbTransactionBatch};
    use crate::storage::store::account::{LinkStore, Store, StoreEventHandler, StoreOptions};
    use crate::storage::util::{decrement_vec_u8, increment_vec_u8};
    use crate::utils::factory::messages_factory;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_store() -> (Store<LinkStore>, Arc<RocksDB>, TempDir) {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = RocksDB::new(db_path.to_str().unwrap());
        db.open().unwrap();
        let db = Arc::new(db);

        let event_handler = StoreEventHandler::new();
        let store = LinkStore::new(db.clone(), event_handler.clone(), 10);

        (store, db.clone(), temp_dir)
    }

    fn create_test_conflict_free_store() -> (Store<LinkStore>, Arc<RocksDB>, TempDir) {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = RocksDB::new(db_path.to_str().unwrap());
        db.open().unwrap();
        let db = Arc::new(db);

        let event_handler = StoreEventHandler::new();
        let store = LinkStore::new_with_opts(
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

    fn merge_message_success(
        store: &Store<LinkStore>,
        db: &Arc<RocksDB>,
        message: &message::Message,
    ) {
        let mut txn = RocksDbTransactionBatch::new();
        let result = store.merge(&message, &mut txn).unwrap();
        assert_eq!(result.r#type(), HubEventType::MergeMessage);
        match &result.body {
            Some(hub_event::Body::MergeMessageBody(body)) => {
                assert_eq!(*body.message.as_ref().unwrap(), *message);
                assert_eq!(*body.deleted_messages, Vec::<message::Message>::new());
            }
            _ => {
                panic!("Unexpected event")
            }
        }
        db.commit(txn).unwrap();
    }

    fn merge_message_with_conflicts(
        store: &Store<LinkStore>,
        db: &Arc<RocksDB>,
        message: &message::Message,
        deleted_messages: Vec<message::Message>,
    ) {
        let mut txn = RocksDbTransactionBatch::new();
        let result = store.merge(&message, &mut txn).unwrap();
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

    fn merge_message_failure(
        store: &Store<LinkStore>,
        message: &message::Message,
        err_code: &str,
        err_message: &str,
    ) {
        let mut txn = RocksDbTransactionBatch::new();
        let result = store.merge(&message, &mut txn);
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert_eq!(err.code, err_code);
        assert_eq!(err.message, err_message);
    }

    fn revoke_message_success(
        store: &Store<LinkStore>,
        db: &Arc<RocksDB>,
        message: &message::Message,
    ) {
        let mut txn = RocksDbTransactionBatch::new();
        let result = store.revoke(&message, &mut txn).unwrap();
        assert_eq!(result.r#type(), HubEventType::RevokeMessage);
        match &result.body {
            Some(hub_event::Body::RevokeMessageBody(body)) => {
                assert_eq!(*body.message.as_ref().unwrap(), *message);
            }
            _ => {
                panic!("Unexpected event")
            }
        }
        db.commit(txn).unwrap();
    }

    fn revoke_message_failure(
        store: &Store<LinkStore>,
        message: &message::Message,
        err_code: &str,
        err_message: &str,
    ) {
        let mut txn = RocksDbTransactionBatch::new();
        let result = store.revoke(&message, &mut txn);
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert_eq!(err.code, err_code);
        assert!(err.message.contains(err_message));
    }

    // Test constants
    const TARGET_FID: u64 = FID_FOR_TEST + 1;
    const LINK_TYPE_FOLLOW: &str = "follow";
    const LINK_TYPE_ENDORSE: &str = "endorse";

    #[test]
    fn test_get_link_add_fails_if_no_link_add_is_present() {
        let (store, _db, _temp_dir) = create_test_store();

        let result = LinkStore::get_link_add(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_get_link_add_fails_if_only_link_remove_exists_for_the_target() {
        let (store, db, _temp_dir) = create_test_store();

        let link_remove = messages_factory::links::create_link_remove(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            None,
            None,
        );

        merge_message_success(&store, &db, &link_remove);

        let result = LinkStore::get_link_add(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_get_link_add_fails_if_the_wrong_fid_is_provided() {
        let (store, db, _temp_dir) = create_test_store();

        let link_add = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            None,
            None,
        );

        merge_message_success(&store, &db, &link_add);

        let wrong_fid = FID_FOR_TEST + 2;
        let result = LinkStore::get_link_add(
            &store,
            wrong_fid,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_get_link_add_fails_if_the_wrong_link_type_is_provided() {
        let (store, db, _temp_dir) = create_test_store();

        let link_add = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            None,
            None,
        );

        merge_message_success(&store, &db, &link_add);

        let result = LinkStore::get_link_add(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_ENDORSE.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_get_link_add_fails_if_the_wrong_target_is_provided() {
        let (store, db, _temp_dir) = create_test_store();

        let link_add = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            None,
            None,
        );

        merge_message_success(&store, &db, &link_add);

        let wrong_target = TARGET_FID + 1;
        let result = LinkStore::get_link_add(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(wrong_target)),
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_get_link_add_returns_message_if_it_exists_for_the_target() {
        let (store, db, _temp_dir) = create_test_store();

        let link_add = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            None,
            None,
        );

        merge_message_success(&store, &db, &link_add);

        let retrieved = LinkStore::get_link_add(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap()
        .unwrap();
        assert_eq!(retrieved, link_add);
    }

    #[test]
    fn test_get_link_remove_fails_if_no_link_remove_is_present() {
        let (store, _db, _temp_dir) = create_test_store();

        let result = LinkStore::get_link_remove(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_get_link_remove_fails_if_only_link_add_exists_for_the_target() {
        let (store, db, _temp_dir) = create_test_store();

        let link_add = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            None,
            None,
        );

        merge_message_success(&store, &db, &link_add);

        let result = LinkStore::get_link_remove(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_get_link_remove_fails_if_the_wrong_fid_is_provided() {
        let (store, db, _temp_dir) = create_test_store();

        let link_remove = messages_factory::links::create_link_remove(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            None,
            None,
        );

        merge_message_success(&store, &db, &link_remove);

        let wrong_fid = FID_FOR_TEST + 2;
        let result = LinkStore::get_link_remove(
            &store,
            wrong_fid,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_get_link_remove_fails_if_the_wrong_link_type_is_provided() {
        let (store, db, _temp_dir) = create_test_store();

        let link_remove = messages_factory::links::create_link_remove(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            None,
            None,
        );

        merge_message_success(&store, &db, &link_remove);

        let result = LinkStore::get_link_remove(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_ENDORSE.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_get_link_remove_fails_if_the_wrong_target_is_provided() {
        let (store, db, _temp_dir) = create_test_store();

        let link_remove = messages_factory::links::create_link_remove(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            None,
            None,
        );

        merge_message_success(&store, &db, &link_remove);

        let wrong_target = TARGET_FID + 1;
        let result = LinkStore::get_link_remove(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(wrong_target)),
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_get_link_remove_returns_message_if_it_exists_for_the_target() {
        let (store, db, _temp_dir) = create_test_store();

        let link_remove = messages_factory::links::create_link_remove(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            None,
            None,
        );

        merge_message_success(&store, &db, &link_remove);

        let retrieved = LinkStore::get_link_remove(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap()
        .unwrap();
        assert_eq!(retrieved, link_remove);
    }

    #[test]
    fn test_get_link_adds_by_fid_returns_link_add_messages_in_chronological_order_according_to_page_options(
    ) {
        let (store, db, _temp_dir) = create_test_store();

        let current_time = 1640995200; // Jan 1, 2022

        let link_add1 = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time),
            None,
        );

        let link_add2 = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID + 1,
            Some(current_time + 2),
            None,
        );

        let link_add_endorse = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_ENDORSE,
            TARGET_FID,
            Some(current_time + 1),
            None,
        );

        merge_message_success(&store, &db, &link_add2);
        merge_message_success(&store, &db, &link_add1);
        merge_message_success(&store, &db, &link_add_endorse);

        // Test default retrieval (all messages in chronological order)
        let all_results = LinkStore::get_link_adds_by_fid(
            &store,
            FID_FOR_TEST,
            "".to_string(),
            &PageOptions::default(),
        )
        .unwrap();
        assert_eq!(
            all_results.messages,
            vec![
                link_add1.clone(),
                link_add_endorse.clone(),
                link_add2.clone()
            ]
        );
        assert!(all_results.next_page_token.is_none());

        // Test pagination
        let page_options = PageOptions {
            page_size: Some(1),
            page_token: None,
            reverse: false,
        };
        let page1_results =
            LinkStore::get_link_adds_by_fid(&store, FID_FOR_TEST, "".to_string(), &page_options)
                .unwrap();
        assert_eq!(page1_results.messages, vec![link_add1]);
        assert!(page1_results.next_page_token.is_some());

        let page2_options = PageOptions {
            page_size: None,
            page_token: page1_results.next_page_token,
            reverse: false,
        };
        let page2_results =
            LinkStore::get_link_adds_by_fid(&store, FID_FOR_TEST, "".to_string(), &page2_options)
                .unwrap();
        assert_eq!(page2_results.messages, vec![link_add_endorse, link_add2]);
        assert!(page2_results.next_page_token.is_none());
    }

    #[test]
    fn test_get_link_adds_by_fid_returns_link_add_messages_by_type() {
        let (store, db, _temp_dir) = create_test_store();

        let link_add_follow = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            None,
            None,
        );

        let link_add_endorse = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_ENDORSE,
            TARGET_FID,
            None,
            None,
        );

        merge_message_success(&store, &db, &link_add_follow);
        merge_message_success(&store, &db, &link_add_endorse);

        // Test filtering by "follow" type
        let follow_results = LinkStore::get_link_adds_by_fid(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            &PageOptions::default(),
        )
        .unwrap();
        assert_eq!(follow_results.messages, vec![link_add_follow]);
        assert!(follow_results.next_page_token.is_none());

        // Test filtering by "endorse" type
        let endorse_results = LinkStore::get_link_adds_by_fid(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_ENDORSE.to_string(),
            &PageOptions::default(),
        )
        .unwrap();
        assert_eq!(endorse_results.messages, vec![link_add_endorse]);
        assert!(endorse_results.next_page_token.is_none());
    }

    #[test]
    fn test_get_link_adds_by_fid_returns_empty_array_if_no_link_add_exists() {
        let (store, _db, _temp_dir) = create_test_store();

        let results = LinkStore::get_link_adds_by_fid(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            &PageOptions::default(),
        )
        .unwrap();
        assert_eq!(results.messages, Vec::<message::Message>::new());
        assert!(results.next_page_token.is_none());
    }

    #[test]
    fn test_get_link_adds_by_fid_returns_empty_array_if_no_link_add_exists_even_if_link_remove_exists(
    ) {
        let (store, db, _temp_dir) = create_test_store();

        let link_remove = messages_factory::links::create_link_remove(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            None,
            None,
        );

        merge_message_success(&store, &db, &link_remove);

        let results = LinkStore::get_link_adds_by_fid(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            &PageOptions::default(),
        )
        .unwrap();
        assert_eq!(results.messages, Vec::<message::Message>::new());
        assert!(results.next_page_token.is_none());
    }

    #[test]
    fn test_get_link_removes_by_fid_returns_link_remove_if_it_exists() {
        let (store, db, _temp_dir) = create_test_store();

        let link_remove = messages_factory::links::create_link_remove(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            None,
            None,
        );

        merge_message_success(&store, &db, &link_remove);

        let results = LinkStore::get_link_removes_by_fid(
            &store,
            FID_FOR_TEST,
            "follow".to_string(),
            &PageOptions::default(),
        )
        .unwrap();
        assert_eq!(results.messages, vec![link_remove]);
        assert!(results.next_page_token.is_none());
    }

    #[test]
    fn test_get_link_removes_by_fid_returns_empty_array_if_no_link_remove_exists() {
        let (store, _db, _temp_dir) = create_test_store();

        let results = LinkStore::get_link_removes_by_fid(
            &store,
            FID_FOR_TEST,
            "follow".to_string(),
            &PageOptions::default(),
        )
        .unwrap();
        assert_eq!(results.messages, Vec::<message::Message>::new());
        assert!(results.next_page_token.is_none());
    }

    #[test]
    fn test_get_link_removes_by_fid_returns_empty_array_if_no_link_remove_exists_even_if_link_adds_exists(
    ) {
        let (store, db, _temp_dir) = create_test_store();

        let link_add = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            None,
            None,
        );

        merge_message_success(&store, &db, &link_add);

        let results = LinkStore::get_link_removes_by_fid(
            &store,
            FID_FOR_TEST,
            "follow".to_string(),
            &PageOptions::default(),
        )
        .unwrap();
        assert_eq!(results.messages, Vec::<message::Message>::new());
        assert!(results.next_page_token.is_none());
    }

    #[test]
    fn test_get_all_link_messages_by_fid_returns_link_remove_if_it_exists() {
        let (store, db, _temp_dir) = create_test_store();

        let link_add = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(1),
            None,
        );

        let link_remove = messages_factory::links::create_link_remove(
            FID_FOR_TEST,
            LINK_TYPE_ENDORSE,
            TARGET_FID + 1,
            Some(2),
            None,
        );

        merge_message_success(&store, &db, &link_add);
        merge_message_success(&store, &db, &link_remove);

        let results = store
            .get_all_messages_by_fid(FID_FOR_TEST, None, None, &PageOptions::default())
            .unwrap();
        assert_eq!(results.messages, vec![link_add, link_remove]);
        assert!(results.next_page_token.is_none());
    }

    #[test]
    fn test_get_all_link_messages_by_fid_returns_empty_array_if_no_messages_exist() {
        let (store, _db, _temp_dir) = create_test_store();

        let results = store
            .get_all_messages_by_fid(FID_FOR_TEST, None, None, &PageOptions::default())
            .unwrap();
        assert_eq!(results.messages, Vec::<message::Message>::new());
        assert!(results.next_page_token.is_none());
    }

    #[test]
    fn test_get_links_by_target_returns_empty_array_if_no_links_exist() {
        let (store, _db, _temp_dir) = create_test_store();

        let results = LinkStore::get_links_by_target(
            &store,
            &Target::TargetFid(TARGET_FID),
            LINK_TYPE_FOLLOW.to_string(),
            &PageOptions::default(),
        )
        .unwrap();
        assert_eq!(results.messages, Vec::<message::Message>::new());
        assert!(results.next_page_token.is_none());
    }

    #[test]
    fn test_get_links_by_target_returns_links_if_they_exist_for_a_target_in_chronological_order_and_according_to_page_options(
    ) {
        let (store, db, _temp_dir) = create_test_store();

        let current_time = 1640995200; // Jan 1, 2022

        let link_add1 = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time),
            None,
        );

        let link_add2 = messages_factory::links::create_link_add(
            FID_FOR_TEST + 1,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time + 2),
            None,
        );

        let link_remove = messages_factory::links::create_link_remove(
            FID_FOR_TEST + 2,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time + 1),
            None,
        );

        merge_message_success(&store, &db, &link_add2);
        merge_message_success(&store, &db, &link_add1);
        merge_message_success(&store, &db, &link_remove);

        // Test default retrieval (all messages in chronological order)
        let all_results = LinkStore::get_links_by_target(
            &store,
            &Target::TargetFid(TARGET_FID),
            "".to_string(),
            &PageOptions::default(),
        )
        .unwrap();
        assert_eq!(
            all_results.messages,
            vec![link_add1.clone(), link_add2.clone()]
        );
        assert!(all_results.next_page_token.is_none());

        // Test pagination
        let page_options = PageOptions {
            page_size: Some(1),
            page_token: None,
            reverse: false,
        };
        let page1_results = LinkStore::get_links_by_target(
            &store,
            &Target::TargetFid(TARGET_FID),
            "".to_string(),
            &page_options,
        )
        .unwrap();
        assert_eq!(page1_results.messages, vec![link_add1]);
        assert!(page1_results.next_page_token.is_some());

        let page2_options = PageOptions {
            page_size: None,
            page_token: page1_results.next_page_token,
            reverse: false,
        };
        let page2_results = LinkStore::get_links_by_target(
            &store,
            &Target::TargetFid(TARGET_FID),
            "".to_string(),
            &page2_options,
        )
        .unwrap();
        assert_eq!(page2_results.messages, vec![link_add2]);
        assert!(page2_results.next_page_token.is_none());
    }

    #[test]
    fn test_get_links_by_target_returns_empty_array_if_links_exist_for_a_different_target() {
        let (store, db, _temp_dir) = create_test_store();

        let link_add = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID + 1,
            None,
            None,
        );

        merge_message_success(&store, &db, &link_add);

        let results = LinkStore::get_links_by_target(
            &store,
            &Target::TargetFid(TARGET_FID),
            LINK_TYPE_FOLLOW.to_string(),
            &PageOptions::default(),
        )
        .unwrap();
        assert_eq!(results.messages, Vec::<message::Message>::new());
        assert!(results.next_page_token.is_none());
    }

    #[test]
    fn test_get_links_by_target_with_type_returns_empty_array_if_no_links_exist() {
        let (store, _db, _temp_dir) = create_test_store();

        let results = LinkStore::get_links_by_target(
            &store,
            &Target::TargetFid(TARGET_FID),
            LINK_TYPE_FOLLOW.to_string(),
            &PageOptions::default(),
        )
        .unwrap();
        assert_eq!(results.messages, Vec::<message::Message>::new());
        assert!(results.next_page_token.is_none());
    }

    #[test]
    fn test_get_links_by_target_with_type_returns_empty_array_if_links_exist_for_the_target_with_different_type(
    ) {
        let (store, db, _temp_dir) = create_test_store();

        let link_add = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_ENDORSE,
            TARGET_FID,
            None,
            None,
        );

        merge_message_success(&store, &db, &link_add);

        let results = LinkStore::get_links_by_target(
            &store,
            &Target::TargetFid(TARGET_FID),
            LINK_TYPE_FOLLOW.to_string(),
            &PageOptions::default(),
        )
        .unwrap();
        assert_eq!(results.messages, Vec::<message::Message>::new());
        assert!(results.next_page_token.is_none());
    }

    #[test]
    fn test_get_links_by_target_with_type_returns_empty_array_if_links_exist_for_the_type_with_different_target(
    ) {
        let (store, db, _temp_dir) = create_test_store();

        let link_add = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID + 1,
            None,
            None,
        );

        merge_message_success(&store, &db, &link_add);

        let results = LinkStore::get_links_by_target(
            &store,
            &Target::TargetFid(TARGET_FID),
            LINK_TYPE_FOLLOW.to_string(),
            &PageOptions::default(),
        )
        .unwrap();
        assert_eq!(results.messages, Vec::<message::Message>::new());
        assert!(results.next_page_token.is_none());
    }

    #[test]
    fn test_get_links_by_target_with_type_returns_links_if_they_exist_for_the_target_and_type() {
        let (store, db, _temp_dir) = create_test_store();

        let current_time = 1640995200; // Jan 1, 2022

        let link_add1 = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time),
            None,
        );

        let link_add2 = messages_factory::links::create_link_add(
            FID_FOR_TEST + 1,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time + 2),
            None,
        );

        let link_add_different_type = messages_factory::links::create_link_add(
            FID_FOR_TEST + 2,
            LINK_TYPE_ENDORSE,
            TARGET_FID,
            Some(current_time + 1),
            None,
        );

        merge_message_success(&store, &db, &link_add2);
        merge_message_success(&store, &db, &link_add1);
        merge_message_success(&store, &db, &link_add_different_type);

        // Test filtering by "follow" type
        let follow_results = LinkStore::get_links_by_target(
            &store,
            &Target::TargetFid(TARGET_FID),
            LINK_TYPE_FOLLOW.to_string(),
            &PageOptions::default(),
        )
        .unwrap();
        assert_eq!(
            follow_results.messages,
            vec![link_add1.clone(), link_add2.clone()]
        );
        assert!(follow_results.next_page_token.is_none());

        // Test pagination with type filter
        let page_options = PageOptions {
            page_size: Some(1),
            page_token: None,
            reverse: false,
        };
        let page1_results = LinkStore::get_links_by_target(
            &store,
            &Target::TargetFid(TARGET_FID),
            LINK_TYPE_FOLLOW.to_string(),
            &page_options,
        )
        .unwrap();
        assert_eq!(page1_results.messages, vec![link_add1]);
        assert!(page1_results.next_page_token.is_some());

        let page2_options = PageOptions {
            page_size: None,
            page_token: page1_results.next_page_token,
            reverse: false,
        };
        let page2_results = LinkStore::get_links_by_target(
            &store,
            &Target::TargetFid(TARGET_FID),
            LINK_TYPE_FOLLOW.to_string(),
            &page2_options,
        )
        .unwrap();
        assert_eq!(page2_results.messages, vec![link_add2]);
        assert!(page2_results.next_page_token.is_none());
    }

    // Merge conflict resolution tests

    #[test]
    fn test_merge_fails_with_invalid_message_type() {
        let (store, _db, _temp_dir) = create_test_store();

        let cast_add =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "Hello world", None, None);

        merge_message_failure(
            &store,
            &cast_add,
            "bad_request.validation_failure",
            "invalid message type",
        );
    }

    #[test]
    fn test_merge_link_add_succeeds() {
        let (store, db, _temp_dir) = create_test_store();

        let link_add = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            None,
            None,
        );

        merge_message_success(&store, &db, &link_add);

        // Verify the message was stored
        let retrieved = LinkStore::get_link_add(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap()
        .unwrap();
        assert_eq!(retrieved, link_add);
    }

    #[test]
    fn test_merge_link_add_fails_if_merged_twice() {
        let (store, db, _temp_dir) = create_test_store();

        let link_add = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            None,
            None,
        );

        merge_message_success(&store, &db, &link_add);
        merge_message_failure(
            &store,
            &link_add,
            "bad_request.duplicate",
            "message has already been merged",
        );

        // Verify only one message is stored
        let retrieved = LinkStore::get_link_add(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap()
        .unwrap();
        assert_eq!(retrieved, link_add);
    }

    #[test]
    fn test_merge_link_add_succeeds_with_a_later_timestamp() {
        let (store, db, _temp_dir) = create_test_store();

        let current_time = 1640995200;

        let link_add = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time),
            None,
        );

        let link_add_later = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time + 10),
            None,
        );

        merge_message_success(&store, &db, &link_add);
        merge_message_with_conflicts(&store, &db, &link_add_later, vec![link_add]);

        // Verify the later message is stored
        let retrieved = LinkStore::get_link_add(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap()
        .unwrap();
        assert_eq!(retrieved, link_add_later);
    }

    #[test]
    fn test_merge_link_add_fails_with_an_earlier_timestamp() {
        let (store, db, _temp_dir) = create_test_store();

        let current_time = 1640995200;

        let link_add = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time + 10),
            None,
        );

        let link_add_earlier = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time),
            None,
        );

        merge_message_success(&store, &db, &link_add);
        merge_message_failure(
            &store,
            &link_add_earlier,
            "bad_request.conflict",
            "message conflicts with a more recent add",
        );

        // Verify the original message is still stored
        let retrieved = LinkStore::get_link_add(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap()
        .unwrap();
        assert_eq!(retrieved, link_add);
    }

    #[test]
    fn test_merge_link_add_succeeds_with_a_higher_hash() {
        let (store, db, _temp_dir) = create_test_store();

        let current_time = 1640995200;

        let link_add = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time),
            None,
        );

        let mut link_add_higher = link_add.clone();
        // Ensure higher hash by incrementing the last byte
        link_add_higher.hash = increment_vec_u8(&link_add.hash);

        merge_message_success(&store, &db, &link_add);
        merge_message_with_conflicts(&store, &db, &link_add_higher, vec![link_add]);

        // Verify the higher hash message is stored
        let retrieved = LinkStore::get_link_add(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap()
        .unwrap();
        assert_eq!(retrieved, link_add_higher);
    }

    #[test]
    fn test_merge_link_add_fails_with_a_lower_hash() {
        let (store, db, _temp_dir) = create_test_store();

        let current_time = 1640995200;

        let link_add = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time),
            None,
        );

        let mut link_add_higher = link_add.clone();
        // Ensure higher hash by incrementing the last byte
        link_add_higher.hash = increment_vec_u8(&link_add.hash);

        merge_message_success(&store, &db, &link_add_higher);
        merge_message_failure(
            &store,
            &link_add,
            "bad_request.conflict",
            "message conflicts with a more recent add",
        );

        // Verify the higher hash message is still stored
        let retrieved = LinkStore::get_link_add(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap()
        .unwrap();
        assert_eq!(retrieved, link_add_higher);
    }

    #[test]
    fn test_merge_link_add_succeeds_with_a_later_timestamp_vs_link_remove() {
        let (store, db, _temp_dir) = create_test_store();

        let current_time = 1640995200;

        let link_remove_earlier = messages_factory::links::create_link_remove(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time),
            None,
        );

        let link_add = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time + 10),
            None,
        );

        merge_message_success(&store, &db, &link_remove_earlier);
        merge_message_with_conflicts(&store, &db, &link_add, vec![link_remove_earlier]);

        // Verify the link add is stored and remove is gone
        let retrieved_add = LinkStore::get_link_add(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap()
        .unwrap();
        assert_eq!(retrieved_add, link_add);

        let retrieved_remove = LinkStore::get_link_remove(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap();
        assert!(retrieved_remove.is_none());
    }

    #[test]
    fn test_merge_link_add_fails_with_an_earlier_timestamp_vs_link_remove() {
        let (store, db, _temp_dir) = create_test_store();

        let current_time = 1640995200;

        let link_remove = messages_factory::links::create_link_remove(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time + 10),
            None,
        );

        let link_add = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time),
            None,
        );

        merge_message_success(&store, &db, &link_remove);
        merge_message_failure(
            &store,
            &link_add,
            "bad_request.conflict",
            "message conflicts with a more recent remove",
        );

        // Verify the link remove is still stored and add is not
        let retrieved_remove = LinkStore::get_link_remove(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap()
        .unwrap();
        assert_eq!(retrieved_remove, link_remove);

        let retrieved_add = LinkStore::get_link_add(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap();
        assert!(retrieved_add.is_none());
    }

    #[test]
    fn test_merge_link_add_fails_if_remove_has_a_higher_hash() {
        let (store, db, _temp_dir) = create_test_store();

        let current_time = 1640995200;

        let link_add = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time),
            None,
        );

        let mut link_remove_higher = messages_factory::links::create_link_remove(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time),
            None,
        );
        // Give remove a higher hash than add
        link_remove_higher.hash = increment_vec_u8(&link_add.hash);

        merge_message_success(&store, &db, &link_remove_higher);
        merge_message_failure(
            &store,
            &link_add,
            "bad_request.conflict",
            "message conflicts with a more recent remove",
        );

        // Verify remove-over-add priority: remove wins even with higher hash
        let retrieved_remove = LinkStore::get_link_remove(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap()
        .unwrap();
        assert_eq!(retrieved_remove, link_remove_higher);

        let retrieved_add = LinkStore::get_link_add(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap();
        assert!(retrieved_add.is_none());
    }

    #[test]
    fn test_merge_link_add_fails_if_remove_has_a_lower_hash() {
        let (store, db, _temp_dir) = create_test_store();

        let current_time = 1640995200;

        let link_add = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time),
            None,
        );

        let mut link_remove_lower = messages_factory::links::create_link_remove(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time),
            None,
        );
        // Give remove a lower hash than add by giving add a higher hash
        link_remove_lower.hash = decrement_vec_u8(&link_add.hash);

        merge_message_success(&store, &db, &link_remove_lower);
        merge_message_failure(
            &store,
            &link_add,
            "bad_request.conflict",
            "message conflicts with a more recent remove",
        );

        // Verify remove-over-add priority: remove wins even with lower hash
        let retrieved_remove = LinkStore::get_link_remove(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap()
        .unwrap();
        assert_eq!(retrieved_remove, link_remove_lower);

        let retrieved_add = LinkStore::get_link_add(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap();
        assert!(retrieved_add.is_none());
    }

    // LinkRemove merge tests

    #[test]
    fn test_merge_link_remove_succeeds() {
        let (store, db, _temp_dir) = create_test_store();

        let link_remove = messages_factory::links::create_link_remove(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            None,
            None,
        );

        merge_message_success(&store, &db, &link_remove);

        // Verify the message was stored
        let retrieved = LinkStore::get_link_remove(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap()
        .unwrap();
        assert_eq!(retrieved, link_remove);
    }

    #[test]
    fn test_merge_link_remove_fails_if_merged_twice() {
        let (store, db, _temp_dir) = create_test_store();

        let link_remove = messages_factory::links::create_link_remove(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            None,
            None,
        );

        merge_message_success(&store, &db, &link_remove);
        merge_message_failure(
            &store,
            &link_remove,
            "bad_request.duplicate",
            "message has already been merged",
        );

        // Verify only one message is stored
        let retrieved = LinkStore::get_link_remove(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap()
        .unwrap();
        assert_eq!(retrieved, link_remove);
    }

    #[test]
    fn test_merge_link_remove_before_compact_state_fails() {
        let (store, db, _temp_dir) = create_test_store();

        let link_compact_state = messages_factory::links::create_link_compact_state(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            vec![TARGET_FID],
            None,
            None,
        );
        merge_message_success(&store, &db, &link_compact_state);

        let link_remove = messages_factory::links::create_link_remove(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(link_compact_state.data.unwrap().timestamp - 1),
            None,
        );

        merge_message_failure(
            &store,
            &link_remove,
            "bad_request.prunable",
            "Remove message earlier than the compact state message will be immediately pruned",
        );
    }

    #[test]
    fn test_merge_link_remove_before_compact_state_succeeds_for_conflict_free() {
        let (store, db, _temp_dir) = create_test_conflict_free_store();

        let link_compact_state = messages_factory::links::create_link_compact_state(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            vec![TARGET_FID],
            None,
            None,
        );
        merge_message_success(&store, &db, &link_compact_state);

        let link_remove = messages_factory::links::create_link_remove(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(link_compact_state.data.unwrap().timestamp - 1),
            None,
        );
        merge_message_success(&store, &db, &link_remove);

        // Verify the message was stored
        let retrieved = LinkStore::get_link_remove(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap()
        .unwrap();
        assert_eq!(retrieved, link_remove);
    }

    #[test]
    fn test_merge_link_remove_succeeds_with_a_later_timestamp() {
        let (store, db, _temp_dir) = create_test_store();

        let current_time = 1640995200;

        let link_remove = messages_factory::links::create_link_remove(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time),
            None,
        );

        let link_remove_later = messages_factory::links::create_link_remove(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time + 10),
            None,
        );

        merge_message_success(&store, &db, &link_remove);
        merge_message_with_conflicts(&store, &db, &link_remove_later, vec![link_remove]);

        // Verify the later message is stored
        let retrieved = LinkStore::get_link_remove(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap()
        .unwrap();
        assert_eq!(retrieved, link_remove_later);
    }

    #[test]
    fn test_merge_link_remove_fails_with_an_earlier_timestamp() {
        let (store, db, _temp_dir) = create_test_store();

        let current_time = 1640995200;

        let link_remove_later = messages_factory::links::create_link_remove(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time + 10),
            None,
        );

        let link_remove = messages_factory::links::create_link_remove(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time),
            None,
        );

        merge_message_success(&store, &db, &link_remove_later);
        merge_message_failure(
            &store,
            &link_remove,
            "bad_request.conflict",
            "message conflicts with a more recent remove",
        );

        // Verify the later message is still stored
        let retrieved = LinkStore::get_link_remove(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap()
        .unwrap();
        assert_eq!(retrieved, link_remove_later);
    }

    #[test]
    fn test_merge_link_remove_succeeds_with_a_higher_hash() {
        let (store, db, _temp_dir) = create_test_store();

        let current_time = 1640995200;

        let link_remove = messages_factory::links::create_link_remove(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time),
            None,
        );

        let mut link_remove_higher = link_remove.clone();
        // Ensure higher hash by incrementing the last byte
        link_remove_higher.hash = increment_vec_u8(&link_remove.hash);

        merge_message_success(&store, &db, &link_remove);
        merge_message_with_conflicts(&store, &db, &link_remove_higher, vec![link_remove]);

        // Verify the higher hash message is stored
        let retrieved = LinkStore::get_link_remove(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap()
        .unwrap();
        assert_eq!(retrieved, link_remove_higher);
    }

    #[test]
    fn test_merge_link_remove_fails_with_a_lower_hash() {
        let (store, db, _temp_dir) = create_test_store();

        let current_time = 1640995200;

        let link_remove = messages_factory::links::create_link_remove(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time),
            None,
        );

        let mut link_remove_higher = link_remove.clone();
        // Ensure higher hash by incrementing the last byte
        link_remove_higher.hash = increment_vec_u8(&link_remove.hash);

        merge_message_success(&store, &db, &link_remove_higher);
        merge_message_failure(
            &store,
            &link_remove,
            "bad_request.conflict",
            "message conflicts with a more recent remove",
        );

        // Verify the higher hash message is still stored
        let retrieved = LinkStore::get_link_remove(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap()
        .unwrap();
        assert_eq!(retrieved, link_remove_higher);
    }

    #[test]
    fn test_merge_link_remove_succeeds_with_a_later_timestamp_vs_link_add() {
        let (store, db, _temp_dir) = create_test_store();

        let current_time = 1640995200;

        let link_add = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time),
            None,
        );

        let link_remove = messages_factory::links::create_link_remove(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time + 10),
            None,
        );

        merge_message_success(&store, &db, &link_add);
        merge_message_with_conflicts(&store, &db, &link_remove, vec![link_add]);

        // Verify the link remove is stored and add is gone
        let retrieved_remove = LinkStore::get_link_remove(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap()
        .unwrap();
        assert_eq!(retrieved_remove, link_remove);

        let retrieved_add = LinkStore::get_link_add(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap();
        assert!(retrieved_add.is_none());
    }

    #[test]
    fn test_merge_link_remove_fails_with_an_earlier_timestamp_vs_link_add() {
        let (store, db, _temp_dir) = create_test_store();

        let current_time = 1640995200;

        let link_add_later = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time + 10),
            None,
        );

        let link_remove = messages_factory::links::create_link_remove(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time),
            None,
        );

        merge_message_success(&store, &db, &link_add_later);
        merge_message_failure(
            &store,
            &link_remove,
            "bad_request.conflict",
            "message conflicts with a more recent add",
        );

        // Verify the link add is still stored and remove is not
        let retrieved_add = LinkStore::get_link_add(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap()
        .unwrap();
        assert_eq!(retrieved_add, link_add_later);

        let retrieved_remove = LinkStore::get_link_remove(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap();
        assert!(retrieved_remove.is_none());
    }

    #[test]
    fn test_merge_link_remove_succeeds_with_a_lower_hash_vs_link_add() {
        let (store, db, _temp_dir) = create_test_store();

        let current_time = 1640995200;

        let link_add_later = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time),
            None,
        );

        let link_remove = messages_factory::links::create_link_remove(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time),
            None,
        );

        merge_message_success(&store, &db, &link_add_later);
        merge_message_with_conflicts(&store, &db, &link_remove, vec![link_add_later]);

        // Verify remove-over-add priority: remove wins even with lower hash
        let retrieved_remove = LinkStore::get_link_remove(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap()
        .unwrap();
        assert_eq!(retrieved_remove, link_remove);

        let retrieved_add = LinkStore::get_link_add(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap();
        assert!(retrieved_add.is_none());
    }

    #[test]
    fn test_merge_link_remove_succeeds_with_a_higher_hash_vs_link_add() {
        let (store, db, _temp_dir) = create_test_store();

        let current_time = 1640995200;

        let link_add_earlier = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time),
            None,
        );

        let link_remove = messages_factory::links::create_link_remove(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time),
            None,
        );

        merge_message_success(&store, &db, &link_add_earlier);
        merge_message_with_conflicts(&store, &db, &link_remove, vec![link_add_earlier]);

        // Verify remove-over-add priority: remove wins even with higher hash
        let retrieved_remove = LinkStore::get_link_remove(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap()
        .unwrap();
        assert_eq!(retrieved_remove, link_remove);

        let retrieved_add = LinkStore::get_link_add(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap();
        assert!(retrieved_add.is_none());
    }

    #[test]
    fn test_handles_conflicting_messages_when_type_is_max_length() {
        let (store, db, _temp_dir) = create_test_store();

        let max_type_link_add = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            "follower", // 8 bytes, max length
            TARGET_FID,
            Some(1640995200),
            None,
        );

        let later_max_type_link_add = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            "follower",
            TARGET_FID,
            Some(1640995201),
            None,
        );

        merge_message_success(&store, &db, &max_type_link_add);
        merge_message_with_conflicts(
            &store,
            &db,
            &later_max_type_link_add,
            vec![max_type_link_add],
        );

        let retrieved = LinkStore::get_link_add(
            &store,
            FID_FOR_TEST,
            "follower".to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap()
        .unwrap();
        assert_eq!(retrieved, later_max_type_link_add);
    }

    // Tests 57-61: Revoke message tests
    #[test]
    fn test_revoke_fails_with_invalid_message_type() {
        let (store, _db, _temp_dir) = create_test_store();

        let cast_add =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "Hello world", None, None);

        revoke_message_failure(
            &store,
            &cast_add,
            "bad_request.invalid_param",
            "invalid message type",
        );
    }

    #[test]
    fn test_revoke_deletes_all_keys_relating_to_the_link() {
        let (store, db, _temp_dir) = create_test_store();

        let link_add = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            None,
            None,
        );

        merge_message_success(&store, &db, &link_add);
        revoke_message_success(&store, &db, &link_add);

        // The message should no longer be retrievable via any API
        let result = LinkStore::get_link_add(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap();
        assert!(result.is_none());

        // Should not appear in get_link_adds_by_fid
        let adds_result = LinkStore::get_link_adds_by_fid(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            &PageOptions::default(),
        )
        .unwrap();
        assert!(adds_result.messages.is_empty());

        // Should not appear in get_all_messages_by_fid
        let all_result = store
            .get_all_messages_by_fid(FID_FOR_TEST, None, None, &PageOptions::default())
            .unwrap();
        assert!(all_result.messages.is_empty());

        // Should not appear in get_links_by_target
        let target_result = LinkStore::get_links_by_target(
            &store,
            &Target::TargetFid(TARGET_FID),
            LINK_TYPE_FOLLOW.to_string(),
            &PageOptions::default(),
        )
        .unwrap();
        assert!(target_result.messages.is_empty());
    }

    #[test]
    fn test_revoke_succeeds_with_link_add() {
        let (store, db, _temp_dir) = create_test_store();

        let link_add = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            None,
            None,
        );

        merge_message_success(&store, &db, &link_add);
        revoke_message_success(&store, &db, &link_add);

        // The message should no longer be retrievable
        let result = LinkStore::get_link_add(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_revoke_succeeds_with_link_remove() {
        let (store, db, _temp_dir) = create_test_store();

        let link_remove = messages_factory::links::create_link_remove(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            None,
            None,
        );

        merge_message_success(&store, &db, &link_remove);
        revoke_message_success(&store, &db, &link_remove);

        // The message should no longer be retrievable
        let result = LinkStore::get_link_remove(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_revoke_succeeds_with_unmerged_message() {
        let (store, db, _temp_dir) = create_test_store();

        let link_add = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            None,
            None,
        );

        // Revoke without merging first
        revoke_message_success(&store, &db, &link_add);

        // The message should not be retrievable
        let result = LinkStore::get_link_add(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap();
        assert!(result.is_none());
    }

    // Tests 62 and 65: Prune message tests
    #[test]
    fn test_prune_messages_no_ops_when_no_messages_have_been_merged() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = RocksDB::new(db_path.to_str().unwrap());
        db.open().unwrap();
        let db = Arc::new(db);

        let event_handler = StoreEventHandler::new();
        let store = LinkStore::new(db.clone(), event_handler.clone(), 3); // size limit = 3

        let mut txn = RocksDbTransactionBatch::new();
        let result = store.prune_messages(FID_FOR_TEST, 0, 2, &mut txn).unwrap();
        assert_eq!(result.len(), 0);
        db.commit(txn).unwrap();
    }

    #[test]
    fn test_prune_messages_prunes_earliest_messages() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = RocksDB::new(db_path.to_str().unwrap());
        db.open().unwrap();
        let db = Arc::new(db);

        let event_handler = StoreEventHandler::new();
        let store = LinkStore::new(db.clone(), event_handler.clone(), 3);

        let current_time = 1640995200;

        let add1 = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID,
            Some(current_time + 1),
            None,
        );
        let remove2 = messages_factory::links::create_link_remove(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID + 1,
            Some(current_time + 2),
            None,
        );
        let add3 = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID + 2,
            Some(current_time + 3),
            None,
        );
        let remove4 = messages_factory::links::create_link_remove(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID + 3,
            Some(current_time + 4),
            None,
        );
        let add5 = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW,
            TARGET_FID + 4,
            Some(current_time + 5),
            None,
        );

        merge_message_success(&store, &db, &add1);
        merge_message_success(&store, &db, &remove2);
        merge_message_success(&store, &db, &add3);
        merge_message_success(&store, &db, &remove4);
        merge_message_success(&store, &db, &add5);

        let mut txn = RocksDbTransactionBatch::new();
        let pruned_events = store.prune_messages(FID_FOR_TEST, 5, 3, &mut txn).unwrap();
        assert_eq!(pruned_events.len(), 2);

        // Extract messages from pruned events and verify they are the earliest ones
        let mut pruned_messages = Vec::new();
        for event in &pruned_events {
            if let Some(hub_event::Body::PruneMessageBody(body)) = &event.body {
                pruned_messages.push(body.message.as_ref().unwrap().clone());
            }
        }

        assert_eq!(pruned_messages, vec![add1.clone(), remove2.clone()]);

        db.commit(txn).unwrap();

        // Verify the pruned messages are no longer retrievable
        let result1 = LinkStore::get_link_add(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID)),
        )
        .unwrap();
        assert!(result1.is_none());

        let result2 = LinkStore::get_link_remove(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID + 1)),
        )
        .unwrap();
        assert!(result2.is_none());

        // Verify remaining messages are still accessible
        let result3 = LinkStore::get_link_add(
            &store,
            FID_FOR_TEST,
            LINK_TYPE_FOLLOW.to_string(),
            Some(Target::TargetFid(TARGET_FID + 2)),
        )
        .unwrap()
        .unwrap();
        assert_eq!(result3, add3);
    }
}
