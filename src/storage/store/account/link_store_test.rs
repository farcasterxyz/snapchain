#[cfg(test)]
mod tests {
    use super::super::super::test_helper::FID_FOR_TEST;
    use crate::proto::link_body::Target;
    use crate::proto::{self as message, hub_event, HubEventType, MessageType};
    use crate::storage::db::{PageOptions, RocksDB, RocksDbTransactionBatch};
    use crate::storage::store::account::{LinkStore, Store, StoreEventHandler};
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
        assert!(err.message.contains(err_message));
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
            None,
            None,
        );

        let link_remove = messages_factory::links::create_link_remove(
            FID_FOR_TEST,
            LINK_TYPE_ENDORSE,
            TARGET_FID + 1,
            None,
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
            vec![link_add1.clone(), link_remove.clone(), link_add2.clone()]
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
        assert_eq!(page2_results.messages, vec![link_remove, link_add2]);
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
}
