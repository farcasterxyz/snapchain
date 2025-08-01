#[cfg(test)]
mod tests {
    use super::super::super::test_helper::FID_FOR_TEST;
    use crate::proto::{self as message, hub_event, HubEventType, ReactionType};
    use crate::storage::db::{PageOptions, RocksDB, RocksDbTransactionBatch};
    use crate::storage::store::account::{
        Store, StoreEventHandler, VerificationStore, VerificationStoreDef,
    };
    use crate::utils::factory::{address, messages_factory};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_store() -> (Store<VerificationStoreDef>, Arc<RocksDB>, TempDir) {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = RocksDB::new(db_path.to_str().unwrap());
        db.open().unwrap();
        let db = Arc::new(db);

        let event_handler = StoreEventHandler::new();
        let store = VerificationStore::new(db.clone(), event_handler.clone(), 50);

        (store, db.clone(), temp_dir)
    }

    fn merge_message_success(
        store: &Store<VerificationStoreDef>,
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
        store: &Store<VerificationStoreDef>,
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
        store: &Store<VerificationStoreDef>,
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
        store: &Store<VerificationStoreDef>,
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
        store: &Store<VerificationStoreDef>,
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

    // getVerificationAdd tests

    #[test]
    fn test_get_verification_add_fails_if_missing() {
        let (store, _db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();

        let result = VerificationStore::get_verification_add(&store, FID_FOR_TEST, &address);
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_get_verification_add_returns_message() {
        let (store, db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();

        let verification_add = messages_factory::verifications::create_verification_add(
            FID_FOR_TEST,
            0, // verification_type
            address.clone(),
            vec![], // claim_signature
            vec![], // block_hash
            None,
            None,
        );

        merge_message_success(&store, &db, &verification_add);

        let retrieved = VerificationStore::get_verification_add(&store, FID_FOR_TEST, &address);
        assert_eq!(retrieved.unwrap().unwrap(), verification_add);
    }

    // getVerificationRemove tests

    #[test]
    fn test_get_verification_remove_fails_if_missing() {
        let (store, _db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();

        let result = VerificationStore::get_verification_remove(&store, FID_FOR_TEST, &address);
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_get_verification_remove_returns_message() {
        let (store, db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();

        let verification_remove = messages_factory::verifications::create_verification_remove(
            FID_FOR_TEST,
            address.clone(),
            None,
            None,
        );

        merge_message_success(&store, &db, &verification_remove);

        let retrieved = VerificationStore::get_verification_remove(&store, FID_FOR_TEST, &address);
        assert_eq!(retrieved.unwrap().unwrap(), verification_remove);
    }

    // getVerificationAddsByFid tests

    #[test]
    fn test_get_verification_adds_by_fid_returns_if_exists() {
        let (store, db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();

        let verification_add = messages_factory::verifications::create_verification_add(
            FID_FOR_TEST,
            0, // verification_type
            address.clone(),
            vec![], // claim_signature
            vec![], // block_hash
            None,
            None,
        );

        merge_message_success(&store, &db, &verification_add);

        let page_options = PageOptions::default();
        let result =
            VerificationStore::get_verification_adds_by_fid(&store, FID_FOR_TEST, &page_options);
        let page = result.unwrap();
        assert_eq!(page.messages, vec![verification_add]);
        assert!(page.next_page_token.is_none());
    }

    #[test]
    fn test_get_verification_adds_by_fid_returns_empty_without_messages() {
        let (store, _db, _temp_dir) = create_test_store();

        let page_options = PageOptions::default();
        let result =
            VerificationStore::get_verification_adds_by_fid(&store, FID_FOR_TEST, &page_options);
        let page = result.unwrap();
        assert_eq!(page.messages, Vec::<message::Message>::new());
        assert!(page.next_page_token.is_none());
    }

    // getVerificationRemovesByFid tests

    #[test]
    fn test_get_verification_removes_by_fid_returns_if_exists() {
        let (store, db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();

        let verification_remove = messages_factory::verifications::create_verification_remove(
            FID_FOR_TEST,
            address.clone(),
            None,
            None,
        );

        merge_message_success(&store, &db, &verification_remove);

        let page_options = PageOptions::default();
        let result =
            VerificationStore::get_verification_removes_by_fid(&store, FID_FOR_TEST, &page_options);
        let page = result.unwrap();
        assert_eq!(page.messages, vec![verification_remove]);
        assert!(page.next_page_token.is_none());
    }

    #[test]
    fn test_get_verification_removes_by_fid_returns_empty_without_messages() {
        let (store, _db, _temp_dir) = create_test_store();

        let page_options = PageOptions::default();
        let result =
            VerificationStore::get_verification_removes_by_fid(&store, FID_FOR_TEST, &page_options);
        let page = result.unwrap();
        assert_eq!(page.messages, Vec::<message::Message>::new());
        assert!(page.next_page_token.is_none());
    }

    // merge tests

    #[test]
    fn test_merge_fails_with_invalid_message_type() {
        let (store, _db, _temp_dir) = create_test_store();

        let reaction_add = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            message::reaction_body::Target::TargetUrl("http://example.com".to_string()),
            None,
            None,
        );

        merge_message_failure(
            &store,
            &reaction_add,
            "bad_request.validation_failure",
            "invalid message type",
        );
    }

    // VerificationAddEthAddress tests

    #[test]
    fn test_verification_add_succeeds() {
        let (store, db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();

        let verification_add = messages_factory::verifications::create_verification_add(
            FID_FOR_TEST,
            0, // verification_type
            address.clone(),
            vec![], // claim_signature
            vec![], // block_hash
            None,
            None,
        );

        merge_message_success(&store, &db, &verification_add);

        // Verify the message exists
        let retrieved = VerificationStore::get_verification_add(&store, FID_FOR_TEST, &address);
        assert_eq!(retrieved.unwrap().unwrap(), verification_add);
    }

    #[test]
    fn test_verification_add_fails_if_merged_twice() {
        let (store, db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();

        let verification_add = messages_factory::verifications::create_verification_add(
            FID_FOR_TEST,
            0, // verification_type
            address.clone(),
            vec![], // claim_signature
            vec![], // block_hash
            None,
            None,
        );

        merge_message_success(&store, &db, &verification_add);
        merge_message_failure(
            &store,
            &verification_add,
            "bad_request.duplicate",
            "message has already been merged",
        );

        // Verify the message still exists
        let retrieved = VerificationStore::get_verification_add(&store, FID_FOR_TEST, &address);
        assert_eq!(retrieved.unwrap().unwrap(), verification_add);
    }

    #[test]
    fn test_verification_add_succeeds_with_later_timestamp() {
        let (store, db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();

        let verification_add = messages_factory::verifications::create_verification_add(
            FID_FOR_TEST,
            0, // verification_type
            address.clone(),
            vec![], // claim_signature
            vec![], // block_hash
            Some(1),
            None,
        );

        let verification_add_later = messages_factory::verifications::create_verification_add(
            FID_FOR_TEST,
            0, // verification_type
            address.clone(),
            vec![], // claim_signature
            vec![], // block_hash
            Some(2),
            None,
        );

        merge_message_success(&store, &db, &verification_add);
        merge_message_with_conflicts(&store, &db, &verification_add_later, vec![verification_add]);

        // Verify the later message exists
        let retrieved = VerificationStore::get_verification_add(&store, FID_FOR_TEST, &address);
        assert_eq!(retrieved.unwrap().unwrap(), verification_add_later);
    }

    #[test]
    fn test_verification_add_fails_with_earlier_timestamp() {
        let (store, db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();

        let verification_add = messages_factory::verifications::create_verification_add(
            FID_FOR_TEST,
            0, // verification_type
            address.clone(),
            vec![], // claim_signature
            vec![], // block_hash
            Some(1),
            None,
        );

        let verification_add_later = messages_factory::verifications::create_verification_add(
            FID_FOR_TEST,
            0, // verification_type
            address.clone(),
            vec![], // claim_signature
            vec![], // block_hash
            Some(2),
            None,
        );

        merge_message_success(&store, &db, &verification_add_later);
        merge_message_failure(
            &store,
            &verification_add,
            "bad_request.conflict",
            "message conflicts with a more recent add",
        );

        // Verify the later message still exists
        let retrieved = VerificationStore::get_verification_add(&store, FID_FOR_TEST, &address);
        assert_eq!(retrieved.unwrap().unwrap(), verification_add_later);
    }

    #[test]
    fn test_verification_add_succeeds_with_higher_hash() {
        let (store, db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();

        let verification_add = messages_factory::verifications::create_verification_add(
            FID_FOR_TEST,
            0, // verification_type
            address.clone(),
            vec![], // claim_signature
            vec![], // block_hash
            Some(1),
            None,
        );

        let mut verification_add_later = verification_add.clone();
        // Increment hash to make it higher
        verification_add_later.hash[19] = verification_add_later.hash[19].wrapping_add(1);

        merge_message_success(&store, &db, &verification_add);
        merge_message_with_conflicts(&store, &db, &verification_add_later, vec![verification_add]);

        // Verify the higher hash message exists
        let retrieved = VerificationStore::get_verification_add(&store, FID_FOR_TEST, &address);
        assert_eq!(retrieved.unwrap().unwrap(), verification_add_later);
    }

    #[test]
    fn test_verification_add_fails_with_lower_hash() {
        let (store, db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();

        let verification_add = messages_factory::verifications::create_verification_add(
            FID_FOR_TEST,
            0, // verification_type
            address.clone(),
            vec![], // claim_signature
            vec![], // block_hash
            Some(1),
            None,
        );

        let mut verification_add_later = verification_add.clone();
        // Increment hash to make it higher
        verification_add_later.hash[19] = verification_add_later.hash[19].wrapping_add(1);

        merge_message_success(&store, &db, &verification_add_later);
        merge_message_failure(
            &store,
            &verification_add,
            "bad_request.conflict",
            "message conflicts with a more recent add",
        );

        // Verify the higher hash message still exists
        let retrieved = VerificationStore::get_verification_add(&store, FID_FOR_TEST, &address);
        assert_eq!(retrieved.unwrap().unwrap(), verification_add_later);
    }

    #[test]
    fn test_verification_add_succeeds_with_later_timestamp_vs_remove() {
        let (store, db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();

        let verification_remove_earlier =
            messages_factory::verifications::create_verification_remove(
                FID_FOR_TEST,
                address.clone(),
                Some(1),
                None,
            );

        let verification_add = messages_factory::verifications::create_verification_add(
            FID_FOR_TEST,
            0, // verification_type
            address.clone(),
            vec![], // claim_signature
            vec![], // block_hash
            Some(2),
            None,
        );

        merge_message_success(&store, &db, &verification_remove_earlier);
        merge_message_with_conflicts(
            &store,
            &db,
            &verification_add,
            vec![verification_remove_earlier],
        );

        // Verify the add message exists
        let retrieved = VerificationStore::get_verification_add(&store, FID_FOR_TEST, &address);
        assert_eq!(retrieved.unwrap().unwrap(), verification_add);

        // Verify the remove message is gone
        let retrieved_remove =
            VerificationStore::get_verification_remove(&store, FID_FOR_TEST, &address);
        assert!(retrieved_remove.unwrap().is_none());
    }

    #[test]
    fn test_verification_add_fails_with_earlier_timestamp_vs_remove() {
        let (store, db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();

        let verification_add = messages_factory::verifications::create_verification_add(
            FID_FOR_TEST,
            0, // verification_type
            address.clone(),
            vec![], // claim_signature
            vec![], // block_hash
            Some(1),
            None,
        );

        let verification_remove = messages_factory::verifications::create_verification_remove(
            FID_FOR_TEST,
            address.clone(),
            Some(2),
            None,
        );

        merge_message_success(&store, &db, &verification_remove);
        merge_message_failure(
            &store,
            &verification_add,
            "bad_request.conflict",
            "message conflicts with a more recent remove",
        );

        // Verify the remove message exists
        let retrieved = VerificationStore::get_verification_remove(&store, FID_FOR_TEST, &address);
        assert_eq!(retrieved.unwrap().unwrap(), verification_remove);
    }

    #[test]
    fn test_verification_add_fails_if_remove_has_higher_hash() {
        let (store, db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();

        let verification_add = messages_factory::verifications::create_verification_add(
            FID_FOR_TEST,
            0, // verification_type
            address.clone(),
            vec![], // claim_signature
            vec![], // block_hash
            Some(1),
            None,
        );

        let mut verification_remove_later =
            messages_factory::verifications::create_verification_remove(
                FID_FOR_TEST,
                address.clone(),
                Some(1), // same timestamp
                None,
            );
        // Increment hash to make it higher
        verification_remove_later.hash[19] = verification_remove_later.hash[19].wrapping_add(1);

        merge_message_success(&store, &db, &verification_remove_later);
        merge_message_failure(
            &store,
            &verification_add,
            "bad_request.conflict",
            "message conflicts with a more recent remove",
        );

        // Verify the remove message exists
        let retrieved = VerificationStore::get_verification_remove(&store, FID_FOR_TEST, &address);
        assert_eq!(retrieved.unwrap().unwrap(), verification_remove_later);
    }

    #[test]
    fn test_verification_add_fails_if_remove_has_lower_hash() {
        let (store, db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();

        let mut verification_add = messages_factory::verifications::create_verification_add(
            FID_FOR_TEST,
            0, // verification_type
            address.clone(),
            vec![], // claim_signature
            vec![], // block_hash
            Some(1),
            None,
        );
        // Increment hash to make it higher
        verification_add.hash[19] = verification_add.hash[19].wrapping_add(1);

        let verification_remove_earlier =
            messages_factory::verifications::create_verification_remove(
                FID_FOR_TEST,
                address.clone(),
                Some(1), // same timestamp
                None,
            );

        merge_message_success(&store, &db, &verification_remove_earlier);
        merge_message_failure(
            &store,
            &verification_add,
            "bad_request.conflict",
            "message conflicts with a more recent remove",
        );

        // Verify the remove message exists (remove always wins regardless of hash)
        let retrieved = VerificationStore::get_verification_remove(&store, FID_FOR_TEST, &address);
        assert_eq!(retrieved.unwrap().unwrap(), verification_remove_earlier);
    }

    // VerificationRemove tests

    #[test]
    fn test_verification_remove_succeeds() {
        let (store, db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();

        let verification_remove = messages_factory::verifications::create_verification_remove(
            FID_FOR_TEST,
            address.clone(),
            None,
            None,
        );

        merge_message_success(&store, &db, &verification_remove);

        // Verify the message exists
        let retrieved = VerificationStore::get_verification_remove(&store, FID_FOR_TEST, &address);
        assert_eq!(retrieved.unwrap().unwrap(), verification_remove);
    }

    #[test]
    fn test_verification_remove_fails_if_merged_twice() {
        let (store, db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();

        let verification_remove = messages_factory::verifications::create_verification_remove(
            FID_FOR_TEST,
            address.clone(),
            None,
            None,
        );

        merge_message_success(&store, &db, &verification_remove);
        merge_message_failure(
            &store,
            &verification_remove,
            "bad_request.duplicate",
            "message has already been merged",
        );

        // Verify the message still exists
        let retrieved = VerificationStore::get_verification_remove(&store, FID_FOR_TEST, &address);
        assert_eq!(retrieved.unwrap().unwrap(), verification_remove);
    }

    #[test]
    fn test_verification_remove_succeeds_with_later_timestamp() {
        let (store, db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();

        let verification_remove = messages_factory::verifications::create_verification_remove(
            FID_FOR_TEST,
            address.clone(),
            Some(1),
            None,
        );

        let verification_remove_later = messages_factory::verifications::create_verification_remove(
            FID_FOR_TEST,
            address.clone(),
            Some(2),
            None,
        );

        merge_message_success(&store, &db, &verification_remove);
        merge_message_with_conflicts(
            &store,
            &db,
            &verification_remove_later,
            vec![verification_remove],
        );

        // Verify the later message exists
        let retrieved = VerificationStore::get_verification_remove(&store, FID_FOR_TEST, &address);
        assert_eq!(retrieved.unwrap().unwrap(), verification_remove_later);
    }

    #[test]
    fn test_verification_remove_fails_with_earlier_timestamp() {
        let (store, db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();

        let verification_remove = messages_factory::verifications::create_verification_remove(
            FID_FOR_TEST,
            address.clone(),
            Some(1),
            None,
        );

        let verification_remove_later = messages_factory::verifications::create_verification_remove(
            FID_FOR_TEST,
            address.clone(),
            Some(2),
            None,
        );

        merge_message_success(&store, &db, &verification_remove_later);
        merge_message_failure(
            &store,
            &verification_remove,
            "bad_request.conflict",
            "message conflicts with a more recent remove",
        );

        // Verify the later message still exists
        let retrieved = VerificationStore::get_verification_remove(&store, FID_FOR_TEST, &address);
        assert_eq!(retrieved.unwrap().unwrap(), verification_remove_later);
    }

    #[test]
    fn test_verification_remove_succeeds_with_higher_hash() {
        let (store, db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();

        let verification_remove = messages_factory::verifications::create_verification_remove(
            FID_FOR_TEST,
            address.clone(),
            Some(1),
            None,
        );

        let mut verification_remove_later = verification_remove.clone();
        // Increment hash to make it higher
        verification_remove_later.hash[19] = verification_remove_later.hash[19].wrapping_add(1);

        merge_message_success(&store, &db, &verification_remove);
        merge_message_with_conflicts(
            &store,
            &db,
            &verification_remove_later,
            vec![verification_remove],
        );

        // Verify the higher hash message exists
        let retrieved = VerificationStore::get_verification_remove(&store, FID_FOR_TEST, &address);
        assert_eq!(retrieved.unwrap().unwrap(), verification_remove_later);
    }

    #[test]
    fn test_verification_remove_fails_with_lower_hash() {
        let (store, db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();

        let verification_remove = messages_factory::verifications::create_verification_remove(
            FID_FOR_TEST,
            address.clone(),
            Some(1),
            None,
        );

        let mut verification_remove_later = verification_remove.clone();
        // Increment hash to make it higher
        verification_remove_later.hash[19] = verification_remove_later.hash[19].wrapping_add(1);

        merge_message_success(&store, &db, &verification_remove_later);
        merge_message_failure(
            &store,
            &verification_remove,
            "bad_request.conflict",
            "message conflicts with a more recent remove",
        );

        // Verify the higher hash message still exists
        let retrieved = VerificationStore::get_verification_remove(&store, FID_FOR_TEST, &address);
        assert_eq!(retrieved.unwrap().unwrap(), verification_remove_later);
    }

    #[test]
    fn test_verification_remove_succeeds_with_later_timestamp_vs_add() {
        let (store, db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();

        let verification_add = messages_factory::verifications::create_verification_add(
            FID_FOR_TEST,
            0, // verification_type
            address.clone(),
            vec![], // claim_signature
            vec![], // block_hash
            Some(1),
            None,
        );

        let verification_remove = messages_factory::verifications::create_verification_remove(
            FID_FOR_TEST,
            address.clone(),
            Some(2),
            None,
        );

        merge_message_success(&store, &db, &verification_add);
        merge_message_with_conflicts(&store, &db, &verification_remove, vec![verification_add]);

        // Verify the remove message exists
        let retrieved = VerificationStore::get_verification_remove(&store, FID_FOR_TEST, &address);
        assert_eq!(retrieved.unwrap().unwrap(), verification_remove);

        // Verify the add message is gone
        let retrieved_add = VerificationStore::get_verification_add(&store, FID_FOR_TEST, &address);
        assert!(retrieved_add.unwrap().is_none());
    }

    #[test]
    fn test_verification_remove_fails_with_earlier_timestamp_vs_add() {
        let (store, db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();

        let verification_remove = messages_factory::verifications::create_verification_remove(
            FID_FOR_TEST,
            address.clone(),
            Some(1),
            None,
        );

        let verification_add_later = messages_factory::verifications::create_verification_add(
            FID_FOR_TEST,
            0, // verification_type
            address.clone(),
            vec![], // claim_signature
            vec![], // block_hash
            Some(2),
            None,
        );

        merge_message_success(&store, &db, &verification_add_later);
        merge_message_failure(
            &store,
            &verification_remove,
            "bad_request.conflict",
            "message conflicts with a more recent add",
        );

        // Verify the add message exists
        let retrieved = VerificationStore::get_verification_add(&store, FID_FOR_TEST, &address);
        assert_eq!(retrieved.unwrap().unwrap(), verification_add_later);
    }

    #[test]
    fn test_verification_remove_succeeds_regardless_of_add_message_hash() {
        let (store, db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();

        let mut verification_add_same_time =
            messages_factory::verifications::create_verification_add(
                FID_FOR_TEST,
                0, // verification_type
                address.clone(),
                vec![], // claim_signature
                vec![], // block_hash
                Some(1),
                None,
            );
        // Increment hash to make it higher
        verification_add_same_time.hash[19] = verification_add_same_time.hash[19].wrapping_add(1);

        let verification_remove = messages_factory::verifications::create_verification_remove(
            FID_FOR_TEST,
            address.clone(),
            Some(1), // same timestamp
            None,
        );

        merge_message_success(&store, &db, &verification_add_same_time);
        merge_message_with_conflicts(
            &store,
            &db,
            &verification_remove,
            vec![verification_add_same_time],
        );

        // Verify the remove message exists (remove always wins regardless of hash)
        let retrieved = VerificationStore::get_verification_remove(&store, FID_FOR_TEST, &address);
        assert_eq!(retrieved.unwrap().unwrap(), verification_remove);

        // Verify the add message is gone
        let retrieved_add = VerificationStore::get_verification_add(&store, FID_FOR_TEST, &address);
        assert!(retrieved_add.unwrap().is_none());
    }

    // revoke tests

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

    #[test]
    fn test_revoke_succeeds_with_verification_add_eth_address() {
        let (store, db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();

        let verification_add = messages_factory::verifications::create_verification_add(
            FID_FOR_TEST,
            0, // verification_type
            address.clone(),
            vec![], // claim_signature
            vec![], // block_hash
            None,
            None,
        );

        merge_message_success(&store, &db, &verification_add);
        revoke_message_success(&store, &db, &verification_add);

        // Verify the message is gone
        let retrieved = VerificationStore::get_verification_add(&store, FID_FOR_TEST, &address);
        assert!(retrieved.unwrap().is_none());
    }

    #[test]
    fn test_revoke_succeeds_with_verification_remove() {
        let (store, db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();

        let verification_remove = messages_factory::verifications::create_verification_remove(
            FID_FOR_TEST,
            address.clone(),
            None,
            None,
        );

        merge_message_success(&store, &db, &verification_remove);
        revoke_message_success(&store, &db, &verification_remove);

        // Verify the message is gone
        let retrieved = VerificationStore::get_verification_remove(&store, FID_FOR_TEST, &address);
        assert!(retrieved.unwrap().is_none());
    }

    #[test]
    fn test_revoke_succeeds_with_unmerged_message() {
        let (store, db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();

        let verification_add = messages_factory::verifications::create_verification_add(
            FID_FOR_TEST,
            0, // verification_type
            address.clone(),
            vec![], // claim_signature
            vec![], // block_hash
            None,
            None,
        );

        // Don't merge first, just revoke
        revoke_message_success(&store, &db, &verification_add);

        // Verify the message doesn't exist
        let retrieved = VerificationStore::get_verification_add(&store, FID_FOR_TEST, &address);
        assert!(retrieved.unwrap().is_none());
    }

    // pruneMessages tests

    #[test]
    fn test_prune_messages_no_ops_when_no_messages_merged() {
        let (store, db, _temp_dir) = create_test_store();

        let mut txn = RocksDbTransactionBatch::new();
        let result = store.prune_messages(FID_FOR_TEST, 0, 3, &mut txn).unwrap();
        db.commit(txn).unwrap();

        assert_eq!(result, Vec::<message::HubEvent>::new());
    }

    #[test]
    fn test_prune_messages_prunes_earliest_messages() {
        let (store, db, _temp_dir) = create_test_store();

        let addresses: Vec<_> = (0..5).map(|_| address::generate_random_address()).collect();

        let add1 = messages_factory::verifications::create_verification_add(
            FID_FOR_TEST,
            0,
            addresses[0].clone(),
            vec![],
            vec![],
            Some(1),
            None,
        );
        let remove2 = messages_factory::verifications::create_verification_remove(
            FID_FOR_TEST,
            addresses[1].clone(),
            Some(2),
            None,
        );
        let add3 = messages_factory::verifications::create_verification_add(
            FID_FOR_TEST,
            0,
            addresses[2].clone(),
            vec![],
            vec![],
            Some(3),
            None,
        );
        let remove4 = messages_factory::verifications::create_verification_remove(
            FID_FOR_TEST,
            addresses[3].clone(),
            Some(4),
            None,
        );
        let add5 = messages_factory::verifications::create_verification_add(
            FID_FOR_TEST,
            0,
            addresses[4].clone(),
            vec![],
            vec![],
            Some(5),
            None,
        );

        let messages = [&add1, &remove2, &add3, &remove4, &add5];
        for message in messages {
            merge_message_success(&store, &db, message);
        }

        let mut txn = RocksDbTransactionBatch::new();
        let result = store.prune_messages(FID_FOR_TEST, 5, 3, &mut txn).unwrap();
        db.commit(txn).unwrap();

        // Should prune 2 earliest messages (add1, remove2)
        assert_eq!(result.len(), 2);

        // Verify the pruned messages were the earliest ones in order
        assert_eq!(result[0].r#type(), HubEventType::PruneMessage);
        match &result[0].body {
            Some(hub_event::Body::PruneMessageBody(body)) => {
                assert_eq!(*body.message.as_ref().unwrap(), add1);
            }
            _ => panic!("Unexpected event"),
        }

        assert_eq!(result[1].r#type(), HubEventType::PruneMessage);
        match &result[1].body {
            Some(hub_event::Body::PruneMessageBody(body)) => {
                assert_eq!(*body.message.as_ref().unwrap(), remove2);
            }
            _ => panic!("Unexpected event"),
        }
    }
}
