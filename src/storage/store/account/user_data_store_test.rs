#[cfg(test)]
mod tests {
    use super::super::super::test_helper::FID_FOR_TEST;
    use crate::proto::{self as message, hub_event, HubEventType, UserDataType, UserNameType};
    use crate::storage::db::{PageOptions, RocksDB, RocksDbTransactionBatch};
    use crate::storage::store::account::{
        Store, StoreEventHandler, UserDataStore, UserDataStoreDef,
    };
    use crate::utils::factory::{messages_factory, username_factory};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_store() -> (Store<UserDataStoreDef>, Arc<RocksDB>, TempDir) {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = RocksDB::new(db_path.to_str().unwrap());
        db.open().unwrap();
        let db = Arc::new(db);

        let event_handler = StoreEventHandler::new();
        let store = UserDataStore::new(db.clone(), event_handler.clone(), 10);

        (store, db.clone(), temp_dir)
    }

    fn merge_message_success(
        store: &Store<UserDataStoreDef>,
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
        store: &Store<UserDataStoreDef>,
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
        store: &Store<UserDataStoreDef>,
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
        store: &Store<UserDataStoreDef>,
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
        store: &Store<UserDataStoreDef>,
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

    fn merge_username_proof_success(
        store: &Store<UserDataStoreDef>,
        db: &Arc<RocksDB>,
        proof: &message::UserNameProof,
    ) -> message::HubEvent {
        let mut txn = RocksDbTransactionBatch::new();
        let result = UserDataStore::merge_username_proof(&store, &proof, &mut txn).unwrap();
        assert_eq!(result.r#type(), HubEventType::MergeUsernameProof);
        match &result.body {
            Some(hub_event::Body::MergeUsernameProofBody(body)) => {
                assert_eq!(*body.username_proof.as_ref().unwrap(), *proof);
            }
            _ => {
                panic!("Unexpected event")
            }
        }
        db.commit(txn).unwrap();
        result
    }

    fn merge_username_proof_failure(
        store: &Store<UserDataStoreDef>,
        proof: &message::UserNameProof,
        err_code: &str,
        err_message: &str,
    ) {
        let mut txn = RocksDbTransactionBatch::new();
        let result = UserDataStore::merge_username_proof(&store, &proof, &mut txn);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.code, err_code);
        assert_eq!(error.message, err_message);
    }

    fn merge_username_proof_with_conflicts(
        store: &Store<UserDataStoreDef>,
        db: &Arc<RocksDB>,
        proof: &message::UserNameProof,
        deleted_proof: &message::UserNameProof,
    ) {
        let mut txn = RocksDbTransactionBatch::new();
        let result = UserDataStore::merge_username_proof(&store, &proof, &mut txn).unwrap();
        assert_eq!(result.r#type(), HubEventType::MergeUsernameProof);
        match &result.body {
            Some(hub_event::Body::MergeUsernameProofBody(body)) => {
                assert_eq!(*body.username_proof.as_ref().unwrap(), *proof);
                assert_eq!(
                    *body.deleted_username_proof.as_ref().unwrap(),
                    *deleted_proof
                );
            }
            _ => {
                panic!("Unexpected event")
            }
        }
        db.commit(txn).unwrap();
    }

    // getUserDataAdd - fails if missing
    #[test]
    fn test_get_user_data_add_fails_if_missing() {
        let (store, _db, _temp_dir) = create_test_store();

        let result =
            UserDataStore::get_user_data_add(&store, FID_FOR_TEST, UserDataType::Pfp as i32)
                .unwrap();
        assert!(result.is_none());

        let result =
            UserDataStore::get_user_data_add(&store, FID_FOR_TEST, UserDataType::Username as i32)
                .unwrap();
        assert!(result.is_none());
    }

    // getUserDataAdd - fails if the wrong fid or datatype is provided
    #[test]
    fn test_get_user_data_add_fails_wrong_fid_or_datatype() {
        let (store, db, _temp_dir) = create_test_store();

        let value = "http://example.com/pfp.jpg".to_string();
        let add_pfp = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST,
            UserDataType::Pfp,
            &value,
            None,
            None,
        );
        merge_message_success(&store, &db, &add_pfp);

        let unknown_fid = FID_FOR_TEST + 1;
        let result =
            UserDataStore::get_user_data_add(&store, unknown_fid, UserDataType::Pfp as i32)
                .unwrap();
        assert!(result.is_none());

        let result =
            UserDataStore::get_user_data_add(&store, FID_FOR_TEST, UserDataType::Bio as i32)
                .unwrap();
        assert!(result.is_none());
    }

    // getUserDataAdd - returns message
    #[test]
    fn test_get_user_data_add_returns_message() {
        let (store, db, _temp_dir) = create_test_store();

        let value = "http://example.com/pfp.jpg".to_string();
        let add_pfp = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST,
            UserDataType::Pfp,
            &value,
            None,
            None,
        );
        merge_message_success(&store, &db, &add_pfp);

        let result =
            UserDataStore::get_user_data_add(&store, FID_FOR_TEST, UserDataType::Pfp as i32)
                .unwrap()
                .unwrap();
        assert_eq!(result, add_pfp);
    }

    // mergeUserNameProof - succeeds
    #[test]
    fn test_merge_username_proof_succeeds() {
        let (store, db, _temp_dir) = create_test_store();

        let name = "testuser".to_string();
        let proof = username_factory::create_username_proof(
            FID_FOR_TEST,
            UserNameType::UsernameTypeFname,
            &name,
            None,
            None,
        );
        let event = merge_username_proof_success(&store, &db, &proof);

        // Verify we can retrieve the proof
        let retrieved = UserDataStore::get_username_proof_by_fid(&store, FID_FOR_TEST)
            .unwrap()
            .unwrap();
        assert_eq!(retrieved, proof);

        let retrieved_by_name = {
            let mut txn = RocksDbTransactionBatch::new();
            UserDataStore::get_username_proof(&store, &mut txn, &proof.name)
                .unwrap()
                .unwrap()
        };
        assert_eq!(retrieved_by_name, proof);

        // Verify event has correct structure (proof added, no deletion)
        match &event.body {
            Some(hub_event::Body::MergeUsernameProofBody(body)) => {
                assert_eq!(*body.username_proof.as_ref().unwrap(), proof);
                assert!(body.deleted_username_proof.is_none());
            }
            _ => panic!("Unexpected event"),
        }
    }

    // mergeUserNameProof - does not merge duplicates
    #[test]
    fn test_merge_username_proof_does_not_merge_duplicates() {
        let (store, db, _temp_dir) = create_test_store();

        let name = "testuser".to_string();
        let proof = username_factory::create_username_proof(
            FID_FOR_TEST,
            UserNameType::UsernameTypeFname,
            &name,
            None,
            None,
        );
        merge_username_proof_success(&store, &db, &proof);

        // Attempt to merge the same proof again should fail
        merge_username_proof_failure(
            &store,
            &proof,
            "bad_request.duplicate",
            "username proof already exists",
        );

        // Verify the proof is still there
        let retrieved = UserDataStore::get_username_proof_by_fid(&store, FID_FOR_TEST)
            .unwrap()
            .unwrap();
        assert_eq!(retrieved, proof);
    }

    // mergeUserNameProof - replaces existing proof with proof of greater timestamp
    #[test]
    fn test_merge_username_proof_replaces_with_greater_timestamp() {
        let (store, db, _temp_dir) = create_test_store();

        let name = "testuser".to_string();
        let existing_proof = username_factory::create_username_proof(
            FID_FOR_TEST,
            UserNameType::UsernameTypeFname,
            &name,
            None,
            None,
        );
        merge_username_proof_success(&store, &db, &existing_proof);

        let new_fid = FID_FOR_TEST + 1;
        let new_proof = username_factory::create_username_proof(
            new_fid,
            UserNameType::UsernameTypeFname,
            &name,
            Some(existing_proof.timestamp + 10),
            None,
        );

        merge_username_proof_with_conflicts(&store, &db, &new_proof, &existing_proof);

        // Verify new proof replaced the old one
        let retrieved_by_name = {
            let mut txn = RocksDbTransactionBatch::new();
            UserDataStore::get_username_proof(&store, &mut txn, &existing_proof.name)
                .unwrap()
                .unwrap()
        };
        assert_eq!(retrieved_by_name, new_proof);

        // Secondary index should be updated - old fid should return nothing, new fid should return new proof
        let old_fid_result =
            UserDataStore::get_username_proof_by_fid(&store, existing_proof.fid).unwrap();
        assert!(old_fid_result.is_none());

        let retrieved_by_fid = UserDataStore::get_username_proof_by_fid(&store, new_proof.fid)
            .unwrap()
            .unwrap();
        assert_eq!(retrieved_by_fid, new_proof);
    }

    // mergeUserNameProof - does not merge if existing timestamp is greater
    #[test]
    fn test_merge_username_proof_fails_with_earlier_timestamp() {
        let (store, db, _temp_dir) = create_test_store();

        let name = "testuser".to_string();
        let existing_proof = username_factory::create_username_proof(
            FID_FOR_TEST,
            UserNameType::UsernameTypeFname,
            &name,
            None,
            None,
        );
        merge_username_proof_success(&store, &db, &existing_proof);

        let new_fid = FID_FOR_TEST + 1;
        let new_proof = username_factory::create_username_proof(
            new_fid,
            UserNameType::UsernameTypeFname,
            &name,
            Some(existing_proof.timestamp - 10),
            None,
        );

        merge_username_proof_failure(
            &store,
            &new_proof,
            "bad_request.conflict",
            "event conflicts with a more recent UserNameProof",
        );

        // Verify existing proof is still there
        let retrieved_by_name = {
            let mut txn = RocksDbTransactionBatch::new();
            UserDataStore::get_username_proof(&store, &mut txn, &existing_proof.name)
                .unwrap()
                .unwrap()
        };
        assert_eq!(retrieved_by_name, existing_proof);
    }

    // mergeUserNameProof - deletes existing proof if fid is 0
    #[test]
    fn test_merge_username_proof_deletes_with_fid_zero() {
        let (store, db, _temp_dir) = create_test_store();

        let name = "testuser".to_string();
        let existing_proof = username_factory::create_username_proof(
            FID_FOR_TEST,
            UserNameType::UsernameTypeFname,
            &name,
            None,
            None,
        );
        merge_username_proof_success(&store, &db, &existing_proof);

        let deletion_proof = username_factory::create_username_proof(
            0,
            UserNameType::UsernameTypeFname,
            &name,
            Some(existing_proof.timestamp + 10),
            None,
        );

        merge_username_proof_with_conflicts(&store, &db, &deletion_proof, &existing_proof);

        // Verify proof is "deleted" - should return the deletion proof (fid=0)
        let mut txn = RocksDbTransactionBatch::new();
        let result = UserDataStore::get_username_proof(&store, &mut txn, &existing_proof.name);
        assert_eq!(result.unwrap().unwrap(), deletion_proof);

        // Old fid should return nothing
        let result = UserDataStore::get_username_proof_by_fid(&store, existing_proof.fid);
        assert!(result.unwrap().is_none());
    }

    // mergeUserNameProof - does not merge if deleted existing proof and timestamp is lesser
    #[test]
    fn test_merge_username_proof_does_not_merge_after_deletion() {
        let (store, db, _temp_dir) = create_test_store();

        let name = "testuser".to_string();
        let existing_proof = username_factory::create_username_proof(
            FID_FOR_TEST,
            UserNameType::UsernameTypeFname,
            &name,
            None,
            None,
        );
        merge_username_proof_success(&store, &db, &existing_proof);

        let deletion_proof = username_factory::create_username_proof(
            0,
            UserNameType::UsernameTypeFname,
            &name,
            Some(existing_proof.timestamp + 10),
            None,
        );

        merge_username_proof_with_conflicts(&store, &db, &deletion_proof, &existing_proof);

        // Try to merge the original proof again - should fail
        merge_username_proof_failure(
            &store,
            &existing_proof,
            "bad_request.conflict",
            "event conflicts with a more recent UserNameProof",
        );
    }

    // mergeUserNameProof - does not emit an event if there is no existing proof and new proof is to fid 0
    #[test]
    fn test_merge_username_proof_no_event_for_nonexistent_deletion() {
        let (store, _db, _temp_dir) = create_test_store();

        let name = "testuser".to_string();
        let deletion_proof = username_factory::create_username_proof(
            0,
            UserNameType::UsernameTypeFname,
            &name,
            None,
            None,
        );

        // Verify proof doesn't exist
        let mut txn = RocksDbTransactionBatch::new();
        let result = UserDataStore::get_username_proof(&store, &mut txn, &deletion_proof.name);
        assert!(result.unwrap().is_none());

        merge_username_proof_failure(
            &store,
            &deletion_proof,
            "bad_request.conflict",
            "proof does not exist",
        );

        let result = UserDataStore::get_username_proof_by_fid(&store, deletion_proof.fid);
        assert!(result.unwrap().is_none());
    }

    // mergeUserNameProof - does not delete existing proof if fid is 0 and timestamp is less than existing
    #[test]
    fn test_merge_username_proof_does_not_delete_with_earlier_timestamp() {
        let (store, db, _temp_dir) = create_test_store();

        let name = "testuser".to_string();
        let existing_proof = username_factory::create_username_proof(
            FID_FOR_TEST,
            UserNameType::UsernameTypeFname,
            &name,
            None,
            None,
        );
        merge_username_proof_success(&store, &db, &existing_proof);

        let deletion_proof = username_factory::create_username_proof(
            0,
            UserNameType::UsernameTypeFname,
            &name,
            Some(existing_proof.timestamp - 10),
            None,
        );

        merge_username_proof_failure(
            &store,
            &deletion_proof,
            "bad_request.conflict",
            "event conflicts with a more recent UserNameProof",
        );

        // Verify existing proof is still there
        let retrieved_by_name = {
            let mut txn = RocksDbTransactionBatch::new();
            UserDataStore::get_username_proof(&store, &mut txn, &existing_proof.name)
                .unwrap()
                .unwrap()
        };
        assert_eq!(retrieved_by_name, existing_proof);
    }

    // getUserDataAddsByFid - returns user data adds for an fid in chronological order
    #[test]
    fn test_get_user_data_adds_by_fid_chronological_order() {
        let (store, db, _temp_dir) = create_test_store();

        let pfp_value = "http://example.com/pfp.jpg".to_string();
        let add_pfp = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST,
            UserDataType::Pfp,
            &pfp_value,
            None,
            None,
        );
        merge_message_success(&store, &db, &add_pfp);

        let bio_value = "My bio".to_string();
        let add_bio = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST,
            UserDataType::Bio,
            &bio_value,
            Some(add_pfp.data.as_ref().unwrap().timestamp + 1),
            None,
        );
        merge_message_success(&store, &db, &add_bio);

        let results = UserDataStore::get_user_data_adds_by_fid(
            &store,
            FID_FOR_TEST,
            &PageOptions::default(),
            None,
            None,
        )
        .unwrap();
        assert_eq!(results.messages, vec![add_pfp, add_bio]);
    }

    // getUserDataAddsByFid - returns empty array if the wrong fid is provided
    #[test]
    fn test_get_user_data_adds_by_fid_wrong_fid() {
        let (store, db, _temp_dir) = create_test_store();

        let pfp_value = "http://example.com/pfp.jpg".to_string();
        let add_pfp = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST,
            UserDataType::Pfp,
            &pfp_value,
            None,
            None,
        );
        merge_message_success(&store, &db, &add_pfp);

        let unknown_fid = FID_FOR_TEST + 1;
        let results = UserDataStore::get_user_data_adds_by_fid(
            &store,
            unknown_fid,
            &PageOptions::default(),
            None,
            None,
        )
        .unwrap();
        assert_eq!(results.messages, Vec::<message::Message>::new());
        assert!(results.next_page_token.is_none());
    }

    // getUserDataAddsByFid - returns empty array without messages
    #[test]
    fn test_get_user_data_adds_by_fid_empty() {
        let (store, _db, _temp_dir) = create_test_store();

        let results = UserDataStore::get_user_data_adds_by_fid(
            &store,
            FID_FOR_TEST,
            &PageOptions::default(),
            None,
            None,
        )
        .unwrap();
        assert_eq!(results.messages, Vec::<message::Message>::new());
        assert!(results.next_page_token.is_none());
    }

    // merge - fails with invalid message type
    #[test]
    fn test_merge_fails_with_invalid_message_type() {
        let (store, _db, _temp_dir) = create_test_store();

        let cast_add =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "test cast", None, None);
        merge_message_failure(
            &store,
            &cast_add,
            "bad_request.validation_failure",
            "invalid message type",
        );
    }

    // merge UserDataAdd - succeeds
    #[test]
    fn test_merge_user_data_add_succeeds() {
        let (store, db, _temp_dir) = create_test_store();

        let pfp_value = "http://example.com/pfp.jpg".to_string();
        let add_pfp = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST,
            UserDataType::Pfp,
            &pfp_value,
            None,
            None,
        );
        merge_message_success(&store, &db, &add_pfp);

        // Verify message was stored
        let result =
            UserDataStore::get_user_data_add(&store, FID_FOR_TEST, UserDataType::Pfp as i32)
                .unwrap()
                .unwrap();
        assert_eq!(result, add_pfp);
    }

    // merge UserDataAdd - fails if merged twice
    #[test]
    fn test_merge_user_data_add_fails_if_merged_twice() {
        let (store, db, _temp_dir) = create_test_store();

        let pfp_value = "http://example.com/pfp.jpg".to_string();
        let add_pfp = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST,
            UserDataType::Pfp,
            &pfp_value,
            None,
            None,
        );
        merge_message_success(&store, &db, &add_pfp);

        merge_message_failure(
            &store,
            &add_pfp,
            "bad_request.duplicate",
            "message has already been merged",
        );

        // Verify original message is still there
        let result =
            UserDataStore::get_user_data_add(&store, FID_FOR_TEST, UserDataType::Pfp as i32)
                .unwrap()
                .unwrap();
        assert_eq!(result, add_pfp);
    }

    // merge UserDataAdd - does not conflict with UserDataAdd of different type
    #[test]
    fn test_merge_user_data_add_no_conflict_different_type() {
        let (store, db, _temp_dir) = create_test_store();

        let bio_value = "My bio".to_string();
        let add_bio = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST,
            UserDataType::Bio,
            &bio_value,
            None,
            None,
        );
        merge_message_success(&store, &db, &add_bio);

        let pfp_value = "http://example.com/pfp.jpg".to_string();
        let add_pfp = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST,
            UserDataType::Pfp,
            &pfp_value,
            None,
            None,
        );
        merge_message_success(&store, &db, &add_pfp);

        // Both should be retrievable
        let result_pfp =
            UserDataStore::get_user_data_add(&store, FID_FOR_TEST, UserDataType::Pfp as i32)
                .unwrap()
                .unwrap();
        assert_eq!(result_pfp, add_pfp);

        let result_bio =
            UserDataStore::get_user_data_add(&store, FID_FOR_TEST, UserDataType::Bio as i32)
                .unwrap()
                .unwrap();
        assert_eq!(result_bio, add_bio);
    }

    // merge UserDataAdd - succeeds with a later timestamp
    #[test]
    fn test_merge_user_data_add_succeeds_with_later_timestamp() {
        let (store, db, _temp_dir) = create_test_store();

        let pfp_value = "http://example.com/pfp.jpg".to_string();
        let add_pfp = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST,
            UserDataType::Pfp,
            &pfp_value,
            None,
            None,
        );
        merge_message_success(&store, &db, &add_pfp);

        let pfp_value_later = "http://example.com/pfp2.jpg".to_string();
        let add_pfp_later = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST,
            UserDataType::Pfp,
            &pfp_value_later,
            Some(add_pfp.data.as_ref().unwrap().timestamp + 1),
            None,
        );
        merge_message_with_conflicts(&store, &db, &add_pfp_later, vec![add_pfp.clone()]);

        // Later message should win
        let result =
            UserDataStore::get_user_data_add(&store, FID_FOR_TEST, UserDataType::Pfp as i32)
                .unwrap()
                .unwrap();
        assert_eq!(result, add_pfp_later);
    }

    // merge UserDataAdd - fails with an earlier timestamp
    #[test]
    fn test_merge_user_data_add_fails_with_earlier_timestamp() {
        let (store, db, _temp_dir) = create_test_store();

        let pfp_value_later = "http://example.com/pfp2.jpg".to_string();
        let add_pfp_later = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST,
            UserDataType::Pfp,
            &pfp_value_later,
            None,
            None,
        );
        merge_message_success(&store, &db, &add_pfp_later);

        let pfp_value = "http://example.com/pfp.jpg".to_string();
        let add_pfp = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST,
            UserDataType::Pfp,
            &pfp_value,
            Some(add_pfp_later.data.as_ref().unwrap().timestamp - 1),
            None,
        );
        merge_message_failure(
            &store,
            &add_pfp,
            "bad_request.conflict",
            "message conflicts with a more recent add",
        );

        // Later message should still be there
        let result =
            UserDataStore::get_user_data_add(&store, FID_FOR_TEST, UserDataType::Pfp as i32)
                .unwrap()
                .unwrap();
        assert_eq!(result, add_pfp_later);
    }

    // merge UserDataAdd - succeeds with a higher hash (identical timestamps)
    #[test]
    fn test_merge_user_data_add_succeeds_with_higher_hash() {
        let (store, db, _temp_dir) = create_test_store();

        let pfp_value = "http://example.com/pfp.jpg".to_string();
        let add_pfp = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST,
            UserDataType::Pfp,
            &pfp_value,
            None,
            None,
        );
        merge_message_success(&store, &db, &add_pfp);

        let pfp_value_higher = "http://example.com/pfp2.jpg".to_string();
        let mut add_pfp_higher = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST,
            UserDataType::Pfp,
            &pfp_value_higher,
            Some(add_pfp.data.as_ref().unwrap().timestamp), // Same timestamp
            None,
        );
        // Ensure higher hash by incrementing the last byte
        let mut hash = add_pfp.hash.clone();
        if let Some(last) = hash.last_mut() {
            *last = last.wrapping_add(1);
        }
        add_pfp_higher.hash = hash;

        merge_message_with_conflicts(&store, &db, &add_pfp_higher, vec![add_pfp.clone()]);

        // Higher hash message should win
        let result =
            UserDataStore::get_user_data_add(&store, FID_FOR_TEST, UserDataType::Pfp as i32)
                .unwrap()
                .unwrap();
        assert_eq!(result, add_pfp_higher);
    }

    // merge UserDataAdd - fails with a lower hash (identical timestamps)
    #[test]
    fn test_merge_user_data_add_fails_with_lower_hash() {
        let (store, db, _temp_dir) = create_test_store();

        let pfp_value_higher = "http://example.com/pfp2.jpg".to_string();
        let add_pfp_higher = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST,
            UserDataType::Pfp,
            &pfp_value_higher,
            None,
            None,
        );
        merge_message_success(&store, &db, &add_pfp_higher);

        let pfp_value = "http://example.com/pfp.jpg".to_string();
        let mut add_pfp = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST,
            UserDataType::Pfp,
            &pfp_value,
            Some(add_pfp_higher.data.as_ref().unwrap().timestamp), // Same timestamp
            None,
        );
        // Ensure lower hash by decrementing from the higher hash
        let mut lower_hash = add_pfp_higher.hash.clone();
        if let Some(last) = lower_hash.last_mut() {
            *last = last.wrapping_sub(1);
        }
        add_pfp.hash = lower_hash;

        merge_message_failure(
            &store,
            &add_pfp,
            "bad_request.conflict",
            "message conflicts with a more recent add",
        );

        // Higher hash message should still be there
        let result =
            UserDataStore::get_user_data_add(&store, FID_FOR_TEST, UserDataType::Pfp as i32)
                .unwrap()
                .unwrap();
        assert_eq!(result, add_pfp_higher);
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

    // revoke - succeeds with UserDataAdd
    #[test]
    fn test_revoke_succeeds_with_user_data_add() {
        let (store, db, _temp_dir) = create_test_store();

        let bio_value = "My bio".to_string();
        let add_bio = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST,
            UserDataType::Bio,
            &bio_value,
            None,
            None,
        );
        merge_message_success(&store, &db, &add_bio);

        // Verify message exists before revoke
        let result =
            UserDataStore::get_user_data_add(&store, FID_FOR_TEST, UserDataType::Bio as i32)
                .unwrap()
                .unwrap();
        assert_eq!(result, add_bio);

        // Revoke the message
        revoke_message_success(&store, &db, &add_bio);

        // Verify message no longer exists
        let result =
            UserDataStore::get_user_data_add(&store, FID_FOR_TEST, UserDataType::Bio as i32)
                .unwrap();
        assert!(result.is_none());
    }

    // revoke - succeeds with unmerged message
    #[test]
    fn test_revoke_succeeds_with_unmerged_message() {
        let (store, db, _temp_dir) = create_test_store();

        let pfp_value = "http://example.com/pfp.jpg".to_string();
        let add_pfp = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST,
            UserDataType::Pfp,
            &pfp_value,
            None,
            None,
        );

        // Don't merge the message first, just revoke it
        revoke_message_success(&store, &db, &add_pfp);

        // Verify message doesn't exist (was never merged)
        let result =
            UserDataStore::get_user_data_add(&store, FID_FOR_TEST, UserDataType::Pfp as i32)
                .unwrap();
        assert!(result.is_none());
    }

    // pruneMessages - no-ops when no messages have been merged
    #[test]
    fn test_prune_messages_no_ops_when_empty() {
        let (store, db, _temp_dir) = create_test_store();

        let mut txn = RocksDbTransactionBatch::new();
        let result = store.prune_messages(FID_FOR_TEST, 0, 2, &mut txn).unwrap();
        db.commit(txn).unwrap();
        assert_eq!(result, Vec::<message::HubEvent>::new());
    }

    // pruneMessages - prunes earliest add messages
    #[test]
    fn test_prune_messages_prunes_earliest() {
        let (store, db, _temp_dir) = create_test_store();

        let base_time = messages_factory::farcaster_time();

        // Create messages with different timestamps and types
        let add1 = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST,
            UserDataType::Pfp,
            &"http://example.com/pfp1.jpg".to_string(),
            Some(base_time + 1),
            None,
        );
        let add2 = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST,
            UserDataType::Display,
            &"Display Name".to_string(),
            Some(base_time + 2),
            None,
        );
        let add3 = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST,
            UserDataType::Bio,
            &"My bio".to_string(),
            Some(base_time + 3),
            None,
        );
        let add4 = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST,
            UserDataType::Url,
            &"https://example.com".to_string(),
            Some(base_time + 5),
            None,
        );

        // Merge all messages
        merge_message_success(&store, &db, &add1);
        merge_message_success(&store, &db, &add2);
        merge_message_success(&store, &db, &add3);
        merge_message_success(&store, &db, &add4);

        // Prune messages - should prune the 2 earliest (add1, add2)
        let mut txn = RocksDbTransactionBatch::new();
        let result = store.prune_messages(FID_FOR_TEST, 4, 2, &mut txn).unwrap();
        db.commit(txn).unwrap();

        // Verify that 2 messages were pruned and they are the earliest ones
        assert_eq!(result.len(), 2);

        // Extract the pruned messages from the HubEvents and verify they are add1 and add2
        let mut pruned_messages = Vec::new();
        for event in &result {
            if let Some(hub_event::Body::PruneMessageBody(prune_body)) = &event.body {
                pruned_messages.push(prune_body.message.as_ref().unwrap().clone());
            }
        }
        assert_eq!(pruned_messages, vec![add1.clone(), add2.clone()]);

        // Verify pruned messages no longer exist
        let result1 =
            UserDataStore::get_user_data_add(&store, FID_FOR_TEST, UserDataType::Pfp as i32)
                .unwrap();
        assert!(result1.is_none());

        let result2 =
            UserDataStore::get_user_data_add(&store, FID_FOR_TEST, UserDataType::Display as i32)
                .unwrap();
        assert!(result2.is_none());

        // Verify remaining messages still exist
        let result3 =
            UserDataStore::get_user_data_add(&store, FID_FOR_TEST, UserDataType::Bio as i32)
                .unwrap()
                .unwrap();
        assert_eq!(result3, add3);

        let result4 =
            UserDataStore::get_user_data_add(&store, FID_FOR_TEST, UserDataType::Url as i32)
                .unwrap()
                .unwrap();
        assert_eq!(result4, add4);
    }
}
