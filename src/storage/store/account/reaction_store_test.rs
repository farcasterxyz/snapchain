#[cfg(test)]
mod tests {
    use super::super::super::test_helper::FID_FOR_TEST;
    use crate::proto::{self as message, hub_event, HubEventType, ReactionType};
    use crate::storage::db::{PageOptions, RocksDB, RocksDbTransactionBatch};
    use crate::storage::store::account::{
        message_bytes_decode, ReactionStore, ReactionStoreDef, Store, StoreEventHandler,
    };
    use crate::storage::util::{decrement_vec_u8, increment_vec_u8};
    use crate::utils::factory::messages_factory;
    use prost::Message;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_store() -> (Store<ReactionStoreDef>, Arc<RocksDB>, TempDir) {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = RocksDB::new(db_path.to_str().unwrap());
        db.open().unwrap();
        let db = Arc::new(db);

        let event_handler = StoreEventHandler::new();
        let store = ReactionStore::new(db.clone(), event_handler.clone(), 10);

        (store, db.clone(), temp_dir)
    }

    fn merge_message_success(
        store: &Store<ReactionStoreDef>,
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
        store: &Store<ReactionStoreDef>,
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
        store: &Store<ReactionStoreDef>,
        message: &message::Message,
        err_code: &str,
        err_message: &str,
    ) {
        let mut txn = RocksDbTransactionBatch::new();
        let result = store.merge(&message, &mut txn);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.code, err_code);
        assert_eq!(error.message, err_message);
    }

    fn revoke_message_success(
        store: &Store<ReactionStoreDef>,
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
        store: &Store<ReactionStoreDef>,
        message: &message::Message,
        err_code: &str,
        err_message: &str,
    ) {
        let mut txn = RocksDbTransactionBatch::new();
        let result = store.revoke(&message, &mut txn);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.code, err_code);
        assert_eq!(error.message, err_message);
    }

    // getReactionAdd - fails if no ReactionAdd is present
    #[test]
    fn test_get_reaction_add_fails_if_missing() {
        let (store, _db, _temp_dir) = create_test_store();

        // Test with a cast target
        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let result = ReactionStore::get_reaction_add(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target),
        );
        assert!(result.unwrap().is_none());
    }

    // getReactionAdd - fails if only ReactionRemove exists for the target
    #[test]
    fn test_get_reaction_add_fails_if_only_remove_exists() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        // Add a ReactionRemove first
        let reaction_remove = messages_factory::reactions::create_reaction_remove(
            FID_FOR_TEST,
            ReactionType::Like,
            message::reaction_body::Target::TargetUrl("https://example.com".to_string()),
            None,
            None,
        );
        merge_message_success(&store, &db, &reaction_remove);

        let result = ReactionStore::get_reaction_add(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target),
        );
        assert!(result.unwrap().is_none());
    }

    // getReactionAdd - fails if the wrong fid is provided
    #[test]
    fn test_get_reaction_add_fails_wrong_fid() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        // Add a ReactionAdd with FID_FOR_TEST
        let reaction_add = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            None,
            None,
        );
        merge_message_success(&store, &db, &reaction_add);

        // Try to get with wrong FID
        let result = ReactionStore::get_reaction_add(
            &store,
            FID_FOR_TEST + 1,
            ReactionType::Like as i32,
            Some(target),
        );
        assert!(result.unwrap().is_none());
    }

    // getReactionAdd - fails if the wrong reaction type is provided
    #[test]
    fn test_get_reaction_add_fails_wrong_reaction_type() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        // Add a ReactionAdd with Like type
        let reaction_add = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            None,
            None,
        );
        merge_message_success(&store, &db, &reaction_add);

        // Try to get with Recast type
        let result = ReactionStore::get_reaction_add(
            &store,
            FID_FOR_TEST,
            ReactionType::Recast as i32,
            Some(target),
        );
        assert!(result.unwrap().is_none());
    }

    // getReactionAdd - fails if the wrong target is provided
    #[test]
    fn test_get_reaction_add_fails_wrong_target() {
        let (store, db, _temp_dir) = create_test_store();

        let cast1 =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast 1", None, None);
        let target1 = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast1.data.as_ref().unwrap().fid,
            hash: cast1.hash.clone(),
        });

        let cast2 =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast 2", None, None);
        let target2 = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast2.data.as_ref().unwrap().fid,
            hash: cast2.hash.clone(),
        });

        // Add a ReactionAdd with target1
        let reaction_add = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target1.clone(),
            None,
            None,
        );
        merge_message_success(&store, &db, &reaction_add);

        // Try to get with target2
        let result = ReactionStore::get_reaction_add(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target2),
        );
        assert!(result.unwrap().is_none());
    }

    // getReactionAdd - returns message if it exists for the target
    #[test]
    fn test_get_reaction_add_returns_message_if_exists() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        // Add a ReactionAdd
        let reaction_add = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            None,
            None,
        );
        merge_message_success(&store, &db, &reaction_add);

        // Should retrieve the message
        let result = ReactionStore::get_reaction_add(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target),
        );
        let retrieved = result.unwrap().unwrap();
        assert_eq!(retrieved, reaction_add);
    }

    // getReactionRemove - fails if no ReactionRemove is present
    #[test]
    fn test_get_reaction_remove_fails_if_missing() {
        let (store, _db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let result = ReactionStore::get_reaction_remove(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target),
        );
        assert!(result.unwrap().is_none());
    }

    // getReactionRemove - fails if only ReactionAdd exists for the target
    #[test]
    fn test_get_reaction_remove_fails_if_only_add_exists() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        // Add a ReactionAdd first
        let reaction_add = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            None,
            None,
        );
        merge_message_success(&store, &db, &reaction_add);

        let result = ReactionStore::get_reaction_remove(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target),
        );
        assert!(result.unwrap().is_none());
    }

    // getReactionRemove - fails if the wrong fid is provided
    #[test]
    fn test_get_reaction_remove_fails_wrong_fid() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        // Add a ReactionRemove with FID_FOR_TEST
        let reaction_remove = messages_factory::reactions::create_reaction_remove(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            None,
            None,
        );
        merge_message_success(&store, &db, &reaction_remove);

        // Try to get with wrong FID
        let result = ReactionStore::get_reaction_remove(
            &store,
            FID_FOR_TEST + 1,
            ReactionType::Like as i32,
            Some(target),
        );
        assert!(result.unwrap().is_none());
    }

    // getReactionRemove - fails if the wrong reaction type is provided
    #[test]
    fn test_get_reaction_remove_fails_wrong_reaction_type() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        // Add a ReactionRemove with Like type
        let reaction_remove = messages_factory::reactions::create_reaction_remove(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            None,
            None,
        );
        merge_message_success(&store, &db, &reaction_remove);

        // Try to get with Recast type
        let result = ReactionStore::get_reaction_remove(
            &store,
            FID_FOR_TEST,
            ReactionType::Recast as i32,
            Some(target),
        );
        assert!(result.unwrap().is_none());
    }

    // getReactionRemove - fails if the wrong target is provided
    #[test]
    fn test_get_reaction_remove_fails_wrong_target() {
        let (store, db, _temp_dir) = create_test_store();

        let cast1 =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast 1", None, None);
        let target1 = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast1.data.as_ref().unwrap().fid,
            hash: cast1.hash.clone(),
        });

        let cast2 =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast 2", None, None);
        let target2 = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast2.data.as_ref().unwrap().fid,
            hash: cast2.hash.clone(),
        });

        // Add a ReactionRemove with target1
        let reaction_remove = messages_factory::reactions::create_reaction_remove(
            FID_FOR_TEST,
            ReactionType::Like,
            target1.clone(),
            None,
            None,
        );
        merge_message_success(&store, &db, &reaction_remove);

        // Try to get with target2
        let result = ReactionStore::get_reaction_remove(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target2),
        );
        assert!(result.unwrap().is_none());
    }

    // getReactionRemove - returns message if it exists for the target
    #[test]
    fn test_get_reaction_remove_returns_message_if_exists() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        // Add a ReactionRemove
        let reaction_remove = messages_factory::reactions::create_reaction_remove(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            None,
            None,
        );
        merge_message_success(&store, &db, &reaction_remove);

        // Should retrieve the message
        let result = ReactionStore::get_reaction_remove(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target),
        );
        let retrieved = result.unwrap().unwrap();
        assert_eq!(retrieved, reaction_remove);
    }

    // dataBytes only Reaction messages - merges and retrieves ReactionAdd with just reaction add
    #[test]
    fn test_databytes_only_reaction_message() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        // Create a ReactionAdd message
        let mut reaction_add = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            None,
            None,
        );

        // Convert message to have dataBytes instead of data
        let data_bytes = reaction_add.data.as_ref().unwrap().encode_to_vec();
        reaction_add.data_bytes = Some(data_bytes);
        message_bytes_decode(&mut reaction_add);

        // Should merge successfully
        merge_message_success(&store, &db, &reaction_add);

        // Should retrieve the message
        let result = ReactionStore::get_reaction_add(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target),
        );
        let retrieved = result.unwrap().unwrap();
        assert_eq!(retrieved, reaction_add);
    }

    // getReactionAddsByFid - returns ReactionAdd messages in chronological order according to pageOptions
    #[test]
    fn test_get_reaction_adds_by_fid_chronological_order() {
        let (store, db, _temp_dir) = create_test_store();

        let cast1 =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast 1", None, None);
        let cast2 =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast 2", None, None);
        let cast3 =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast 3", None, None);

        let target1 = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast1.data.as_ref().unwrap().fid,
            hash: cast1.hash.clone(),
        });
        let target2 = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast2.data.as_ref().unwrap().fid,
            hash: cast2.hash.clone(),
        });
        let target3 = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast3.data.as_ref().unwrap().fid,
            hash: cast3.hash.clone(),
        });

        // Create reactions with different timestamps and different targets
        let reaction_add1 = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target1.clone(),
            Some(1000),
            None,
        );
        let reaction_add2 = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target2.clone(),
            Some(1002),
            None,
        );
        let reaction_add_recast = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Recast,
            target3.clone(),
            Some(1001),
            None,
        );

        // Merge in non-chronological order
        merge_message_success(&store, &db, &reaction_add2);
        merge_message_success(&store, &db, &reaction_add1);
        merge_message_success(&store, &db, &reaction_add_recast);

        // Should return all in chronological order
        let page_options = PageOptions {
            page_size: None,
            page_token: None,
            reverse: false,
        };
        let result =
            ReactionStore::get_reaction_adds_by_fid(&store, FID_FOR_TEST, 0, &page_options)
                .unwrap();
        assert_eq!(
            result.messages,
            vec![
                reaction_add1.clone(),
                reaction_add_recast.clone(),
                reaction_add2.clone()
            ]
        );

        // Test pagination
        let page_options_1 = PageOptions {
            page_size: Some(1),
            page_token: None,
            reverse: false,
        };
        let page1 =
            ReactionStore::get_reaction_adds_by_fid(&store, FID_FOR_TEST, 0, &page_options_1)
                .unwrap();
        assert_eq!(page1.messages, vec![reaction_add1.clone()]);
        assert!(page1.next_page_token.is_some());

        // Second page
        let page_options_2 = PageOptions {
            page_size: None,
            page_token: page1.next_page_token,
            reverse: false,
        };
        let page2 =
            ReactionStore::get_reaction_adds_by_fid(&store, FID_FOR_TEST, 0, &page_options_2)
                .unwrap();
        assert_eq!(page2.messages, vec![reaction_add_recast, reaction_add2]);
        assert!(page2.next_page_token.is_none());
    }

    // getReactionAddsByFid - returns ReactionAdd messages by type
    #[test]
    fn test_get_reaction_adds_by_fid_by_type() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let reaction_add_like = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            None,
            None,
        );
        let reaction_add_recast = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Recast,
            target.clone(),
            None,
            None,
        );

        merge_message_success(&store, &db, &reaction_add_like);
        merge_message_success(&store, &db, &reaction_add_recast);

        let page_options = PageOptions {
            page_size: None,
            page_token: None,
            reverse: false,
        };

        // Get only Like reactions
        let result_like = ReactionStore::get_reaction_adds_by_fid(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            &page_options,
        )
        .unwrap();
        assert_eq!(result_like.messages, vec![reaction_add_like]);

        // Get only Recast reactions
        let result_recast = ReactionStore::get_reaction_adds_by_fid(
            &store,
            FID_FOR_TEST,
            ReactionType::Recast as i32,
            &page_options,
        )
        .unwrap();
        assert_eq!(result_recast.messages, vec![reaction_add_recast]);
    }

    // getReactionAddsByFid - returns empty array if no ReactionAdd exists
    #[test]
    fn test_get_reaction_adds_by_fid_empty_if_none() {
        let (store, _db, _temp_dir) = create_test_store();

        let page_options = PageOptions {
            page_size: None,
            page_token: None,
            reverse: false,
        };

        let result =
            ReactionStore::get_reaction_adds_by_fid(&store, FID_FOR_TEST, 0, &page_options)
                .unwrap();
        assert_eq!(result.messages, Vec::<message::Message>::new());
        assert!(result.next_page_token.is_none());
    }

    // getReactionAddsByFid - returns empty array if no ReactionAdd exists, even if ReactionRemove exists
    #[test]
    fn test_get_reaction_adds_by_fid_empty_if_only_removes() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let reaction_remove = messages_factory::reactions::create_reaction_remove(
            FID_FOR_TEST,
            ReactionType::Like,
            target,
            None,
            None,
        );
        merge_message_success(&store, &db, &reaction_remove);

        let page_options = PageOptions {
            page_size: None,
            page_token: None,
            reverse: false,
        };

        let result =
            ReactionStore::get_reaction_adds_by_fid(&store, FID_FOR_TEST, 0, &page_options)
                .unwrap();
        assert_eq!(result.messages, Vec::<message::Message>::new());
        assert!(result.next_page_token.is_none());
    }

    // getReactionRemovesByFid - returns ReactionRemove if it exists
    #[test]
    fn test_get_reaction_removes_by_fid_returns_if_exists() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let reaction_remove_like = messages_factory::reactions::create_reaction_remove(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            Some(1),
            None,
        );
        let reaction_remove_recast = messages_factory::reactions::create_reaction_remove(
            FID_FOR_TEST,
            ReactionType::Recast,
            target.clone(),
            Some(2), // To ensure consistent ordering for assertions in test
            None,
        );

        merge_message_success(&store, &db, &reaction_remove_like);
        merge_message_success(&store, &db, &reaction_remove_recast);

        let page_options = PageOptions {
            page_size: None,
            page_token: None,
            reverse: false,
        };

        let result =
            ReactionStore::get_reaction_removes_by_fid(&store, FID_FOR_TEST, 0, &page_options)
                .unwrap();
        assert_eq!(
            result.messages,
            vec![reaction_remove_like, reaction_remove_recast]
        );
        assert!(result.next_page_token.is_none());
    }

    // getReactionRemovesByFid - returns empty array if no ReactionRemove exists
    #[test]
    fn test_get_reaction_removes_by_fid_empty_if_none() {
        let (store, _db, _temp_dir) = create_test_store();

        let page_options = PageOptions {
            page_size: None,
            page_token: None,
            reverse: false,
        };

        let result =
            ReactionStore::get_reaction_removes_by_fid(&store, FID_FOR_TEST, 0, &page_options)
                .unwrap();
        assert_eq!(result.messages, Vec::<message::Message>::new());
        assert!(result.next_page_token.is_none());
    }

    // getReactionRemovesByFid - returns empty array if no ReactionRemove exists, even if ReactionAdds exists
    #[test]
    fn test_get_reaction_removes_by_fid_empty_if_only_adds() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let reaction_add = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target,
            None,
            None,
        );
        merge_message_success(&store, &db, &reaction_add);

        let page_options = PageOptions {
            page_size: None,
            page_token: None,
            reverse: false,
        };

        let result =
            ReactionStore::get_reaction_removes_by_fid(&store, FID_FOR_TEST, 0, &page_options)
                .unwrap();
        assert_eq!(result.messages, Vec::<message::Message>::new());
        assert!(result.next_page_token.is_none());
    }

    // getAllReactionMessagesByFid - returns ReactionRemove if it exists
    #[test]
    fn test_get_all_reaction_messages_by_fid_returns_if_exists() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let reaction_add = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            None,
            None,
        );
        let reaction_remove_recast = messages_factory::reactions::create_reaction_remove(
            FID_FOR_TEST,
            ReactionType::Recast,
            target.clone(),
            Some(reaction_add.data.as_ref().unwrap().timestamp + 1), // To guarantee ordering that we can assert on
            None,
        );

        merge_message_success(&store, &db, &reaction_add);
        merge_message_success(&store, &db, &reaction_remove_recast);

        let page_options = PageOptions {
            page_size: None,
            page_token: None,
            reverse: false,
        };

        let result = store
            .get_all_messages_by_fid(FID_FOR_TEST, None, None, &page_options)
            .unwrap();
        assert_eq!(result.messages, vec![reaction_add, reaction_remove_recast]);
        assert!(result.next_page_token.is_none());
    }

    // getAllReactionMessagesByFid - returns empty array if no messages exist
    #[test]
    fn test_get_all_reaction_messages_by_fid_empty_if_none() {
        let (store, _db, _temp_dir) = create_test_store();

        let page_options = PageOptions {
            page_size: None,
            page_token: None,
            reverse: false,
        };

        let result = store
            .get_all_messages_by_fid(FID_FOR_TEST, None, None, &page_options)
            .unwrap();
        assert_eq!(result.messages, Vec::<message::Message>::new());
        assert!(result.next_page_token.is_none());
    }

    // getReactionsByTarget - returns empty array if no reactions exist
    #[test]
    fn test_get_reactions_by_target_empty_if_none() {
        let (store, _db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let result =
            ReactionStore::get_reactions_by_target(&store, &target, 0, &PageOptions::default())
                .unwrap();
        assert_eq!(result.messages, Vec::<message::Message>::new());
        assert!(result.next_page_token.is_none());
    }

    // getReactionsByTarget - returns reactions if they exist for a target in chronological order and according to pageOptions
    #[test]
    fn test_get_reactions_by_target_chronological_order() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let reaction_add1 = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            Some(1000),
            None,
        );
        let reaction_add_recast = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Recast,
            target.clone(),
            Some(1001),
            None,
        );
        let reaction_same_target = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST + 1,
            ReactionType::Like,
            target.clone(),
            Some(1002),
            None,
        );

        merge_message_success(&store, &db, &reaction_add1);
        merge_message_success(&store, &db, &reaction_add_recast);
        merge_message_success(&store, &db, &reaction_same_target);

        // Should return all in chronological order
        let result =
            ReactionStore::get_reactions_by_target(&store, &target, 0, &PageOptions::default())
                .unwrap();
        assert_eq!(
            result.messages,
            vec![
                reaction_add1.clone(),
                reaction_add_recast.clone(),
                reaction_same_target.clone()
            ]
        );
        assert!(result.next_page_token.is_none());

        // Test pagination
        let page_options_1 = PageOptions {
            page_size: Some(1),
            page_token: None,
            reverse: false,
        };
        let page1 =
            ReactionStore::get_reactions_by_target(&store, &target, 0, &page_options_1).unwrap();
        assert_eq!(page1.messages, vec![reaction_add1.clone()]);
        assert!(page1.next_page_token.is_some());

        // Second page
        let page_options_2 = PageOptions {
            page_size: None,
            page_token: page1.next_page_token,
            reverse: false,
        };
        let page2 =
            ReactionStore::get_reactions_by_target(&store, &target, 0, &page_options_2).unwrap();
        assert_eq!(
            page2.messages,
            vec![reaction_add_recast.clone(), reaction_same_target.clone()]
        );
        assert!(page2.next_page_token.is_none());

        // Test reverse order
        let page_options_reverse = PageOptions {
            page_size: None,
            page_token: None,
            reverse: true,
        };
        let result_reverse =
            ReactionStore::get_reactions_by_target(&store, &target, 0, &page_options_reverse)
                .unwrap();
        assert_eq!(
            result_reverse.messages,
            vec![
                reaction_same_target.clone(),
                reaction_add_recast.clone(),
                reaction_add1.clone()
            ]
        );
    }

    // getReactionsByTarget - returns reactions for a target url
    #[test]
    fn test_get_reactions_by_target_url() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let cast_target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });
        let url_target =
            message::reaction_body::Target::TargetUrl("https://example.com".to_string());

        let reaction_cast = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            cast_target.clone(),
            Some(1000),
            None,
        );
        let reaction_url1 = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            url_target.clone(),
            Some(1001),
            None,
        );
        let reaction_url2 = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Recast,
            url_target.clone(),
            Some(1002),
            None,
        );

        merge_message_success(&store, &db, &reaction_cast);
        merge_message_success(&store, &db, &reaction_url1);
        merge_message_success(&store, &db, &reaction_url2);

        // Should return only URL reactions
        let result =
            ReactionStore::get_reactions_by_target(&store, &url_target, 0, &PageOptions::default())
                .unwrap();
        assert_eq!(result.messages, vec![reaction_url1, reaction_url2]);
        assert!(result.next_page_token.is_none());
    }

    // getReactionsByTarget - returns empty array if reactions exist for a different target
    #[test]
    fn test_get_reactions_by_target_different_target() {
        let (store, db, _temp_dir) = create_test_store();

        let cast1 =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast 1", None, None);
        let target1 = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast1.data.as_ref().unwrap().fid,
            hash: cast1.hash.clone(),
        });

        let cast2 =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast 2", None, None);
        let target2 = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast2.data.as_ref().unwrap().fid,
            hash: cast2.hash.clone(),
        });

        let reaction_add = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target1.clone(),
            None,
            None,
        );
        merge_message_success(&store, &db, &reaction_add);

        // Should return empty for different target
        let result =
            ReactionStore::get_reactions_by_target(&store, &target2, 0, &PageOptions::default())
                .unwrap();
        assert_eq!(result.messages, Vec::<message::Message>::new());
        assert!(result.next_page_token.is_none());
    }

    // getReactionsByTarget with type - returns empty array if no reactions exist
    #[test]
    fn test_get_reactions_by_target_with_type_empty_if_none() {
        let (store, _db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let result = ReactionStore::get_reactions_by_target(
            &store,
            &target,
            ReactionType::Like as i32,
            &PageOptions::default(),
        )
        .unwrap();
        assert_eq!(result.messages, Vec::<message::Message>::new());
        assert!(result.next_page_token.is_none());
    }

    // getReactionsByTarget with type - returns empty array if reactions exist for the target with different type
    #[test]
    fn test_get_reactions_by_target_with_type_different_type() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let reaction_add_recast = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Recast,
            target.clone(),
            None,
            None,
        );
        merge_message_success(&store, &db, &reaction_add_recast);

        // Should return empty for different type
        let result = ReactionStore::get_reactions_by_target(
            &store,
            &target,
            ReactionType::Like as i32,
            &PageOptions::default(),
        )
        .unwrap();
        assert_eq!(result.messages, Vec::<message::Message>::new());
        assert!(result.next_page_token.is_none());
    }

    // getReactionsByTarget with type - returns empty array if reactions exist for the type with different target
    #[test]
    fn test_get_reactions_by_target_with_type_different_target() {
        let (store, db, _temp_dir) = create_test_store();

        let cast1 =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast 1", None, None);
        let target1 = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast1.data.as_ref().unwrap().fid,
            hash: cast1.hash.clone(),
        });

        let cast2 =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast 2", None, None);
        let target2 = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast2.data.as_ref().unwrap().fid,
            hash: cast2.hash.clone(),
        });

        let reaction_add = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target1.clone(),
            None,
            None,
        );
        merge_message_success(&store, &db, &reaction_add);

        // Should return empty for different target even with correct type
        let result = ReactionStore::get_reactions_by_target(
            &store,
            &target2,
            ReactionType::Like as i32,
            &PageOptions::default(),
        )
        .unwrap();
        assert_eq!(result.messages, Vec::<message::Message>::new());
        assert!(result.next_page_token.is_none());
    }

    // getReactionsByTarget with type - returns reactions if they exist for the target and type
    #[test]
    fn test_get_reactions_by_target_with_type_returns_if_exists() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        // Create different users to avoid conflicts (same target, different users)
        let reaction_like1 = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            Some(1000),
            None,
        );
        let reaction_like2 = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST + 1, // Different user
            ReactionType::Like,
            target.clone(),
            Some(1002),
            None,
        );
        let reaction_recast = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Recast,
            target.clone(),
            Some(1001),
            None,
        );

        merge_message_success(&store, &db, &reaction_like2);
        merge_message_success(&store, &db, &reaction_like1);
        merge_message_success(&store, &db, &reaction_recast);

        // Should return only Like reactions
        let result_like = ReactionStore::get_reactions_by_target(
            &store,
            &target,
            ReactionType::Like as i32,
            &PageOptions::default(),
        )
        .unwrap();
        assert_eq!(
            result_like.messages,
            vec![reaction_like1.clone(), reaction_like2.clone()]
        );
        assert!(result_like.next_page_token.is_none());

        // Test reverse pagination for Like reactions
        let page_options_reverse_1 = PageOptions {
            page_size: Some(1),
            page_token: None,
            reverse: true,
        };
        let page_reverse_1 = ReactionStore::get_reactions_by_target(
            &store,
            &target,
            ReactionType::Like as i32,
            &page_options_reverse_1,
        )
        .unwrap();
        assert_eq!(page_reverse_1.messages, vec![reaction_like2.clone()]);
        assert!(page_reverse_1.next_page_token.is_some());

        let page_options_reverse_2 = PageOptions {
            page_size: None,
            page_token: page_reverse_1.next_page_token,
            reverse: true,
        };
        let page_reverse_2 = ReactionStore::get_reactions_by_target(
            &store,
            &target,
            ReactionType::Like as i32,
            &page_options_reverse_2,
        )
        .unwrap();
        assert_eq!(page_reverse_2.messages, vec![reaction_like1.clone()]);
        assert!(page_reverse_2.next_page_token.is_none());

        // Should return only Recast reactions
        let result_recast = ReactionStore::get_reactions_by_target(
            &store,
            &target,
            ReactionType::Recast as i32,
            &PageOptions::default(),
        )
        .unwrap();
        assert_eq!(result_recast.messages, vec![reaction_recast]);
        assert!(result_recast.next_page_token.is_none());
    }

    // merge - fails with invalid message type
    #[test]
    fn test_merge_fails_with_invalid_message_type() {
        let (store, _db, _temp_dir) = create_test_store();

        let cast_message =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);

        merge_message_failure(
            &store,
            &cast_message,
            "bad_request.validation_failure",
            "invalid message type",
        );
    }

    // merge ReactionAdd - succeeds
    #[test]
    fn test_merge_reaction_add_succeeds() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let reaction_add = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            None,
            None,
        );

        merge_message_success(&store, &db, &reaction_add);

        // Should be able to retrieve the message
        let result = ReactionStore::get_reaction_add(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target.clone()),
        )
        .unwrap()
        .unwrap();
        assert_eq!(result, reaction_add);

        // Should appear in target query
        let target_result =
            ReactionStore::get_reactions_by_target(&store, &target, 0, &PageOptions::default())
                .unwrap();
        assert_eq!(target_result.messages, vec![reaction_add]);

        // Should NOT be able to get remove
        let remove_result = ReactionStore::get_reaction_remove(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target),
        );
        assert!(remove_result.unwrap().is_none());
    }

    // merge ReactionAdd - fails if merged twice
    #[test]
    fn test_merge_reaction_add_fails_if_merged_twice() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let reaction_add = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            None,
            None,
        );

        merge_message_success(&store, &db, &reaction_add);

        // Second merge should fail
        merge_message_failure(
            &store,
            &reaction_add,
            "bad_request.duplicate",
            "message has already been merged",
        );
    }

    // merge ReactionAdd - succeeds with a later timestamp
    #[test]
    fn test_merge_reaction_add_succeeds_with_later_timestamp() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let reaction_add_early = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            Some(1000),
            None,
        );
        let reaction_add_later = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            Some(1001),
            None,
        );

        merge_message_success(&store, &db, &reaction_add_early);
        merge_message_with_conflicts(
            &store,
            &db,
            &reaction_add_later,
            vec![reaction_add_early.clone()],
        );

        // Later message should win
        let result = ReactionStore::get_reaction_add(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target),
        )
        .unwrap()
        .unwrap();
        assert_eq!(result, reaction_add_later);
    }

    // merge ReactionAdd - fails with an earlier timestamp
    #[test]
    fn test_merge_reaction_add_fails_with_earlier_timestamp() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let reaction_add_later = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            Some(1001),
            None,
        );
        let reaction_add_early = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            Some(1000),
            None,
        );

        merge_message_success(&store, &db, &reaction_add_later);

        // Earlier message should fail
        merge_message_failure(
            &store,
            &reaction_add_early,
            "bad_request.conflict",
            "message conflicts with a more recent add",
        );

        // Later message should still be there
        let result = ReactionStore::get_reaction_add(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target),
        )
        .unwrap()
        .unwrap();
        assert_eq!(result, reaction_add_later);
    }

    // merge ReactionAdd - succeeds with a higher hash
    #[test]
    fn test_merge_reaction_add_succeeds_with_higher_hash() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let reaction_add = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            Some(1000),
            None,
        );
        let mut reaction_add_higher = reaction_add.clone();
        reaction_add_higher.hash = increment_vec_u8(&reaction_add.hash);

        merge_message_success(&store, &db, &reaction_add);
        merge_message_with_conflicts(
            &store,
            &db,
            &reaction_add_higher,
            vec![reaction_add.clone()],
        );

        // Higher hash message should win
        let result = ReactionStore::get_reaction_add(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target),
        )
        .unwrap()
        .unwrap();
        assert_eq!(result, reaction_add_higher);
    }

    // merge ReactionAdd - fails with a lower hash
    #[test]
    fn test_merge_reaction_add_fails_with_lower_hash() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let reaction_add_higher = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            Some(1000),
            None,
        );
        let mut reaction_add_lower = reaction_add_higher.clone();
        reaction_add_lower.hash = decrement_vec_u8(&reaction_add_higher.hash);

        merge_message_success(&store, &db, &reaction_add_higher);

        // Lower hash message should fail
        merge_message_failure(
            &store,
            &reaction_add_lower,
            "bad_request.conflict",
            "message conflicts with a more recent add",
        );

        // Higher hash message should still be there
        let result = ReactionStore::get_reaction_add(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target),
        )
        .unwrap()
        .unwrap();
        assert_eq!(result, reaction_add_higher);
    }

    // merge ReactionRemove - succeeds
    #[test]
    fn test_merge_reaction_remove_succeeds() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let reaction_remove = messages_factory::reactions::create_reaction_remove(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            None,
            None,
        );

        merge_message_success(&store, &db, &reaction_remove);

        // Should be able to retrieve the remove message
        let result = ReactionStore::get_reaction_remove(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target.clone()),
        )
        .unwrap()
        .unwrap();
        assert_eq!(result, reaction_remove);

        // Should NOT appear in target query (removes don't show in target queries)
        let target_result =
            ReactionStore::get_reactions_by_target(&store, &target, 0, &PageOptions::default())
                .unwrap();
        assert_eq!(target_result.messages, Vec::<message::Message>::new());

        // Should NOT be able to get add
        let add_result = ReactionStore::get_reaction_add(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target),
        );
        assert!(add_result.unwrap().is_none());
    }

    // merge ReactionRemove - fails if merged twice
    #[test]
    fn test_merge_reaction_remove_fails_if_merged_twice() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let reaction_remove = messages_factory::reactions::create_reaction_remove(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            None,
            None,
        );

        merge_message_success(&store, &db, &reaction_remove);

        // Second merge should fail
        merge_message_failure(
            &store,
            &reaction_remove,
            "bad_request.duplicate",
            "message has already been merged",
        );
    }

    // merge ReactionRemove - succeeds with a later timestamp
    #[test]
    fn test_merge_reaction_remove_succeeds_with_later_timestamp() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let reaction_remove_early = messages_factory::reactions::create_reaction_remove(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            Some(1000),
            None,
        );
        let reaction_remove_later = messages_factory::reactions::create_reaction_remove(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            Some(1001),
            None,
        );

        merge_message_success(&store, &db, &reaction_remove_early);
        merge_message_with_conflicts(
            &store,
            &db,
            &reaction_remove_later,
            vec![reaction_remove_early.clone()],
        );

        // Later message should win
        let result = ReactionStore::get_reaction_remove(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target),
        )
        .unwrap()
        .unwrap();
        assert_eq!(result, reaction_remove_later);
    }

    // merge ReactionRemove - fails with an earlier timestamp
    #[test]
    fn test_merge_reaction_remove_fails_with_earlier_timestamp() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let reaction_remove_later = messages_factory::reactions::create_reaction_remove(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            Some(1001),
            None,
        );
        let reaction_remove_early = messages_factory::reactions::create_reaction_remove(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            Some(1000),
            None,
        );

        merge_message_success(&store, &db, &reaction_remove_later);

        // Earlier message should fail
        merge_message_failure(
            &store,
            &reaction_remove_early,
            "bad_request.conflict",
            "message conflicts with a more recent remove",
        );

        // Later message should still be there
        let result = ReactionStore::get_reaction_remove(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target),
        )
        .unwrap()
        .unwrap();
        assert_eq!(result, reaction_remove_later);
    }

    // merge ReactionRemove - succeeds with a higher hash
    #[test]
    fn test_merge_reaction_remove_succeeds_with_higher_hash() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let reaction_remove = messages_factory::reactions::create_reaction_remove(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            Some(1000),
            None,
        );
        let mut reaction_remove_higher = reaction_remove.clone();
        reaction_remove_higher.hash = increment_vec_u8(&reaction_remove.hash);

        merge_message_success(&store, &db, &reaction_remove);
        merge_message_with_conflicts(
            &store,
            &db,
            &reaction_remove_higher,
            vec![reaction_remove.clone()],
        );

        // Higher hash message should win
        let result = ReactionStore::get_reaction_remove(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target),
        )
        .unwrap()
        .unwrap();
        assert_eq!(result, reaction_remove_higher);
    }

    // merge ReactionRemove - fails with a lower hash
    #[test]
    fn test_merge_reaction_remove_fails_with_lower_hash() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let reaction_remove_higher = messages_factory::reactions::create_reaction_remove(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            Some(1000),
            None,
        );
        let mut reaction_remove_lower = reaction_remove_higher.clone();
        reaction_remove_lower.hash = decrement_vec_u8(&reaction_remove_lower.hash);

        merge_message_success(&store, &db, &reaction_remove_higher);

        // Lower hash message should fail
        merge_message_failure(
            &store,
            &reaction_remove_lower,
            "bad_request.conflict",
            "message conflicts with a more recent remove",
        );

        // Higher hash message should still be there
        let result = ReactionStore::get_reaction_remove(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target),
        )
        .unwrap()
        .unwrap();
        assert_eq!(result, reaction_remove_higher);
    }

    // merge ReactionAdd - succeeds with a later timestamp (vs ReactionRemove)
    #[test]
    fn test_merge_reaction_add_succeeds_with_later_timestamp_vs_remove() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let reaction_remove_early = messages_factory::reactions::create_reaction_remove(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            Some(1000),
            None,
        );
        let reaction_add_later = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            Some(1001),
            None,
        );

        merge_message_success(&store, &db, &reaction_remove_early);
        merge_message_with_conflicts(
            &store,
            &db,
            &reaction_add_later,
            vec![reaction_remove_early.clone()],
        );

        // ReactionAdd should win over earlier ReactionRemove
        let add_result = ReactionStore::get_reaction_add(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target.clone()),
        )
        .unwrap()
        .unwrap();
        assert_eq!(add_result, reaction_add_later);

        // Should NOT be able to get remove
        let remove_result = ReactionStore::get_reaction_remove(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target.clone()),
        );
        assert!(remove_result.unwrap().is_none());

        // Should appear in target query
        let target_result =
            ReactionStore::get_reactions_by_target(&store, &target, 0, &PageOptions::default())
                .unwrap();
        assert_eq!(target_result.messages, vec![reaction_add_later]);
    }

    // merge ReactionAdd - fails with an earlier timestamp (vs ReactionRemove)
    #[test]
    fn test_merge_reaction_add_fails_with_earlier_timestamp_vs_remove() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let reaction_remove_later = messages_factory::reactions::create_reaction_remove(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            Some(1001),
            None,
        );
        let reaction_add_early = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            Some(1000),
            None,
        );

        merge_message_success(&store, &db, &reaction_remove_later);

        // Earlier ReactionAdd should fail
        merge_message_failure(
            &store,
            &reaction_add_early,
            "bad_request.conflict",
            "message conflicts with a more recent remove",
        );

        // ReactionRemove should still be there
        let remove_result = ReactionStore::get_reaction_remove(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target.clone()),
        )
        .unwrap()
        .unwrap();
        assert_eq!(remove_result, reaction_remove_later);

        // Should NOT appear in target query
        let target_result =
            ReactionStore::get_reactions_by_target(&store, &target, 0, &PageOptions::default())
                .unwrap();
        assert_eq!(target_result.messages, Vec::<message::Message>::new());
    }

    // merge ReactionRemove - succeeds with a later timestamp (vs ReactionAdd)
    #[test]
    fn test_merge_reaction_remove_succeeds_with_later_timestamp_vs_add() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let reaction_add_early = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            Some(1000),
            None,
        );
        let reaction_remove_later = messages_factory::reactions::create_reaction_remove(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            Some(1001),
            None,
        );

        merge_message_success(&store, &db, &reaction_add_early);
        merge_message_with_conflicts(
            &store,
            &db,
            &reaction_remove_later,
            vec![reaction_add_early.clone()],
        );

        // ReactionRemove should win over ReactionAdd
        let remove_result = ReactionStore::get_reaction_remove(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target.clone()),
        )
        .unwrap()
        .unwrap();
        assert_eq!(remove_result, reaction_remove_later);

        // Should NOT be able to get add
        let add_result = ReactionStore::get_reaction_add(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target.clone()),
        );
        assert!(add_result.unwrap().is_none());

        // Should NOT appear in target query
        let target_result =
            ReactionStore::get_reactions_by_target(&store, &target, 0, &PageOptions::default())
                .unwrap();
        assert_eq!(target_result.messages, Vec::<message::Message>::new());
    }

    // merge ReactionRemove - fails with an earlier timestamp (vs ReactionAdd)
    #[test]
    fn test_merge_reaction_remove_fails_with_earlier_timestamp_vs_add() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let reaction_add_later = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            Some(1001),
            None,
        );
        let reaction_remove_early = messages_factory::reactions::create_reaction_remove(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            Some(1000),
            None,
        );

        merge_message_success(&store, &db, &reaction_add_later);

        // Earlier ReactionRemove should fail
        merge_message_failure(
            &store,
            &reaction_remove_early,
            "bad_request.conflict",
            "message conflicts with a more recent add",
        );

        // ReactionAdd should still be there
        let add_result = ReactionStore::get_reaction_add(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target.clone()),
        )
        .unwrap()
        .unwrap();
        assert_eq!(add_result, reaction_add_later);

        // Should appear in target query
        let target_result =
            ReactionStore::get_reactions_by_target(&store, &target, 0, &PageOptions::default())
                .unwrap();
        assert_eq!(target_result.messages, vec![reaction_add_later]);
    }

    // merge ReactionAdd - fails if remove has a higher hash
    #[test]
    fn test_merge_reaction_add_fails_if_remove_has_higher_hash() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let reaction_remove = messages_factory::reactions::create_reaction_remove(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            Some(1000),
            None,
        );
        let mut reaction_add_lower = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            Some(1000), // Same timestamp
            None,
        );

        // Ensure ReactionAdd has lower hash by decrementing from remove hash
        reaction_add_lower.hash = decrement_vec_u8(&reaction_remove.hash.clone());

        merge_message_success(&store, &db, &reaction_remove);

        // ReactionAdd with lower hash should fail
        merge_message_failure(
            &store,
            &reaction_add_lower,
            "bad_request.conflict",
            "message conflicts with a more recent remove",
        );

        // ReactionRemove should still be there
        let remove_result = ReactionStore::get_reaction_remove(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target.clone()),
        )
        .unwrap()
        .unwrap();
        assert_eq!(remove_result, reaction_remove);

        // Should NOT appear in target query
        let target_result =
            ReactionStore::get_reactions_by_target(&store, &target, 0, &PageOptions::default())
                .unwrap();
        assert_eq!(target_result.messages, Vec::<message::Message>::new());
    }

    // merge ReactionAdd - fails if remove has a lower hash
    #[test]
    fn test_merge_reaction_add_fails_if_remove_has_lower_hash() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let reaction_remove_lower = messages_factory::reactions::create_reaction_remove(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            Some(1000),
            None,
        );
        let mut reaction_add_higher = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            Some(1000), // Same timestamp
            None,
        );

        // Ensure ReactionAdd has higher hash by incrementing from remove hash
        reaction_add_higher.hash = increment_vec_u8(&reaction_remove_lower.hash);

        merge_message_success(&store, &db, &reaction_remove_lower);

        // ReactionAdd with higher hash should still fail (removes always win over adds with same timestamp)
        merge_message_failure(
            &store,
            &reaction_add_higher,
            "bad_request.conflict",
            "message conflicts with a more recent remove",
        );

        // ReactionRemove should still be there
        let remove_result = ReactionStore::get_reaction_remove(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target.clone()),
        )
        .unwrap()
        .unwrap();
        assert_eq!(remove_result, reaction_remove_lower);

        // Should NOT appear in target query
        let target_result =
            ReactionStore::get_reactions_by_target(&store, &target, 0, &PageOptions::default())
                .unwrap();
        assert_eq!(target_result.messages, Vec::<message::Message>::new());
    }

    // merge ReactionRemove - succeeds with a lower hash (vs ReactionAdd)
    #[test]
    fn test_merge_reaction_remove_succeeds_with_lower_hash_vs_add() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let reaction_add_higher = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            Some(1000),
            None,
        );
        let mut reaction_remove_lower = messages_factory::reactions::create_reaction_remove(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            Some(1000), // Same timestamp
            None,
        );

        // Ensure ReactionRemove has lower hash by decrementing from add hash
        reaction_remove_lower.hash = decrement_vec_u8(&reaction_add_higher.hash);

        merge_message_success(&store, &db, &reaction_add_higher);
        merge_message_with_conflicts(
            &store,
            &db,
            &reaction_remove_lower,
            vec![reaction_add_higher.clone()],
        );

        // ReactionRemove should win even with lower hash (removes always win over adds with same timestamp)
        let remove_result = ReactionStore::get_reaction_remove(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target.clone()),
        )
        .unwrap()
        .unwrap();
        assert_eq!(remove_result, reaction_remove_lower);

        // Should NOT be able to get add
        let add_result = ReactionStore::get_reaction_add(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target.clone()),
        );
        assert!(add_result.unwrap().is_none());

        // Should NOT appear in target query
        let target_result =
            ReactionStore::get_reactions_by_target(&store, &target, 0, &PageOptions::default())
                .unwrap();
        assert_eq!(target_result.messages, Vec::<message::Message>::new());
    }

    // merge ReactionRemove - succeeds with a higher hash (vs ReactionAdd)
    #[test]
    fn test_merge_reaction_remove_succeeds_with_higher_hash_vs_add() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let reaction_add_lower = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            Some(1000),
            None,
        );
        let mut reaction_remove_higher = messages_factory::reactions::create_reaction_remove(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            Some(1000), // Same timestamp
            None,
        );

        // Ensure ReactionRemove has higher hash by incrementing from add hash
        reaction_remove_higher.hash = increment_vec_u8(&reaction_add_lower.hash);

        merge_message_success(&store, &db, &reaction_add_lower);
        merge_message_with_conflicts(
            &store,
            &db,
            &reaction_remove_higher,
            vec![reaction_add_lower.clone()],
        );

        // ReactionRemove should win with higher hash
        let remove_result = ReactionStore::get_reaction_remove(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target.clone()),
        )
        .unwrap()
        .unwrap();
        assert_eq!(remove_result, reaction_remove_higher);

        // Should NOT be able to get add
        let add_result = ReactionStore::get_reaction_add(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target.clone()),
        );
        assert!(add_result.unwrap().is_none());

        // Should NOT appear in target query
        let target_result =
            ReactionStore::get_reactions_by_target(&store, &target, 0, &PageOptions::default())
                .unwrap();
        assert_eq!(target_result.messages, Vec::<message::Message>::new());
    }

    // revoke - fails with invalid message type
    #[test]
    fn test_revoke_fails_with_invalid_message_type() {
        let (store, _db, _temp_dir) = create_test_store();

        let cast_add =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);

        revoke_message_failure(
            &store,
            &cast_add,
            "bad_request.invalid_param",
            "invalid message type",
        );
    }

    // revoke - deletes all keys relating to the reaction
    #[test]
    fn test_revoke_deletes_all_keys_relating_to_reaction() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let reaction_add = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            None,
            None,
        );

        // Merge the reaction first
        merge_message_success(&store, &db, &reaction_add);

        // Verify it exists
        let result = ReactionStore::get_reaction_add(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target.clone()),
        )
        .unwrap()
        .unwrap();
        assert_eq!(result, reaction_add);

        // Count keys before revoke (simple check - just verify some exist)
        let target_result_before =
            ReactionStore::get_reactions_by_target(&store, &target, 0, &PageOptions::default())
                .unwrap();
        assert_eq!(target_result_before.messages, vec![reaction_add.clone()]);

        // Revoke the reaction
        revoke_message_success(&store, &db, &reaction_add);

        // Verify it no longer exists
        let result = ReactionStore::get_reaction_add(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target.clone()),
        );
        assert!(result.unwrap().is_none());

        // Verify it doesn't appear in target queries
        let target_result_after =
            ReactionStore::get_reactions_by_target(&store, &target, 0, &PageOptions::default())
                .unwrap();
        assert_eq!(target_result_after.messages, Vec::<message::Message>::new());
    }

    // revoke - deletes all keys relating to the cast with parent url
    #[test]
    fn test_revoke_deletes_all_keys_relating_to_cast_with_parent_url() {
        let (store, db, _temp_dir) = create_test_store();

        let url_target =
            message::reaction_body::Target::TargetUrl("https://example.com".to_string());

        let reaction_add = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            url_target.clone(),
            None,
            None,
        );

        // Merge the reaction first
        merge_message_success(&store, &db, &reaction_add);

        // Verify it exists
        let result = ReactionStore::get_reaction_add(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(url_target.clone()),
        )
        .unwrap()
        .unwrap();
        assert_eq!(result, reaction_add);

        // Verify it appears in target queries
        let target_result_before =
            ReactionStore::get_reactions_by_target(&store, &url_target, 0, &PageOptions::default())
                .unwrap();
        assert_eq!(target_result_before.messages, vec![reaction_add.clone()]);

        // Revoke the reaction
        revoke_message_success(&store, &db, &reaction_add);

        // Verify it no longer exists
        let result = ReactionStore::get_reaction_add(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(url_target.clone()),
        );
        assert!(result.unwrap().is_none());

        // Verify it doesn't appear in target queries
        let target_result_after =
            ReactionStore::get_reactions_by_target(&store, &url_target, 0, &PageOptions::default())
                .unwrap();
        assert_eq!(target_result_after.messages, Vec::<message::Message>::new());
    }

    // revoke - succeeds with ReactionAdd
    #[test]
    fn test_revoke_succeeds_with_reaction_add() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let reaction_add = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            None,
            None,
        );

        // Merge the reaction first
        merge_message_success(&store, &db, &reaction_add);

        // Verify it exists
        let result = ReactionStore::get_reaction_add(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target.clone()),
        )
        .unwrap()
        .unwrap();
        assert_eq!(result, reaction_add);

        // Revoke the reaction
        revoke_message_success(&store, &db, &reaction_add);

        // Verify it no longer exists
        let result = ReactionStore::get_reaction_add(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target),
        );
        assert!(result.unwrap().is_none());
    }

    // revoke - succeeds with ReactionRemove
    #[test]
    fn test_revoke_succeeds_with_reaction_remove() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let reaction_remove = messages_factory::reactions::create_reaction_remove(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            None,
            None,
        );

        // Merge the reaction first
        merge_message_success(&store, &db, &reaction_remove);

        // Verify it exists
        let result = ReactionStore::get_reaction_remove(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target.clone()),
        )
        .unwrap()
        .unwrap();
        assert_eq!(result, reaction_remove);

        // Revoke the reaction
        revoke_message_success(&store, &db, &reaction_remove);

        // Verify it no longer exists
        let result = ReactionStore::get_reaction_remove(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target),
        );
        assert!(result.unwrap().is_none());
    }

    // revoke - succeeds with unmerged message
    #[test]
    fn test_revoke_succeeds_with_unmerged_message() {
        let (store, db, _temp_dir) = create_test_store();

        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        let target = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast.data.as_ref().unwrap().fid,
            hash: cast.hash.clone(),
        });

        let reaction_add = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target.clone(),
            None,
            None,
        );

        // Don't merge the message first, just revoke it directly
        revoke_message_success(&store, &db, &reaction_add);

        // Verify it doesn't exist (was never merged)
        let result = ReactionStore::get_reaction_add(
            &store,
            FID_FOR_TEST,
            ReactionType::Like as i32,
            Some(target),
        );
        assert!(result.unwrap().is_none());
    }

    // pruneMessages - no-ops when no messages have been merged
    #[test]
    fn test_prune_messages_no_ops_when_no_messages_merged() {
        let (store, db, _temp_dir) = create_test_store();

        let mut txn = RocksDbTransactionBatch::new();
        let result = store.prune_messages(FID_FOR_TEST, 0, 3, &mut txn).unwrap();
        db.commit(txn).unwrap();

        assert_eq!(result.len(), 0);
    }

    // pruneMessages - prunes earliest messages (mixed types)
    #[test]
    fn test_prune_messages_prunes_earliest_messages() {
        let (store, db, _temp_dir) = create_test_store();

        let cast1 =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast 1", None, None);
        let cast2 =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast 2", None, None);
        let cast3 =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast 3", None, None);
        let cast4 =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast 4", None, None);
        let cast5 =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast 5", None, None);

        let target1 = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast1.data.as_ref().unwrap().fid,
            hash: cast1.hash.clone(),
        });
        let target2 = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast2.data.as_ref().unwrap().fid,
            hash: cast2.hash.clone(),
        });
        let target3 = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast3.data.as_ref().unwrap().fid,
            hash: cast3.hash.clone(),
        });
        let target4 = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast4.data.as_ref().unwrap().fid,
            hash: cast4.hash.clone(),
        });
        let target5 = message::reaction_body::Target::TargetCastId(message::CastId {
            fid: cast5.data.as_ref().unwrap().fid,
            hash: cast5.hash.clone(),
        });

        let add1 = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target1.clone(),
            Some(1),
            None,
        );
        let remove2 = messages_factory::reactions::create_reaction_remove(
            FID_FOR_TEST,
            ReactionType::Like,
            target2.clone(),
            Some(2),
            None,
        );
        let add3 = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target3.clone(),
            Some(3),
            None,
        );
        let remove4 = messages_factory::reactions::create_reaction_remove(
            FID_FOR_TEST,
            ReactionType::Like,
            target4.clone(),
            Some(4),
            None,
        );
        let add5 = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target5.clone(),
            Some(5),
            None,
        );

        // Merge messages
        merge_message_success(&store, &db, &add1);
        merge_message_success(&store, &db, &remove2);
        merge_message_success(&store, &db, &add3);
        merge_message_success(&store, &db, &remove4);
        merge_message_success(&store, &db, &add5);

        // Prune messages
        let mut txn = RocksDbTransactionBatch::new();
        let result = store.prune_messages(FID_FOR_TEST, 5, 3, &mut txn).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].r#type(), HubEventType::PruneMessage);
        if let Some(hub_event::Body::PruneMessageBody(body)) = &result[0].body {
            assert_eq!(*body.message.as_ref().unwrap(), add1);
        }
        if let Some(hub_event::Body::PruneMessageBody(body)) = &result[1].body {
            assert_eq!(*body.message.as_ref().unwrap(), remove2);
        }
        db.commit(txn).unwrap();
    }
}
