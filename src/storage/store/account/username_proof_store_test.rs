#[cfg(test)]
mod tests {
    use super::super::super::test_helper::FID_FOR_TEST;
    use crate::proto::{self as message, hub_event, HubEventType, UserNameProof, UserNameType};
    use crate::storage::db::{PageOptions, RocksDB, RocksDbTransactionBatch};
    use crate::storage::store::account::{
        Store, StoreEventHandler, UsernameProofStore, UsernameProofStoreDef,
    };
    use crate::utils::factory::{messages_factory, username_factory};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_store() -> (Store<UsernameProofStoreDef>, Arc<RocksDB>, TempDir) {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = RocksDB::new(db_path.to_str().unwrap());
        db.open().unwrap();
        let db = Arc::new(db);

        let event_handler = StoreEventHandler::new();
        let store = UsernameProofStore::new(db.clone(), event_handler.clone(), 10);

        (store, db.clone(), temp_dir)
    }

    fn merge_username_proof_success(
        store: &Store<UsernameProofStoreDef>,
        db: &Arc<RocksDB>,
        username_proof: &UserNameProof,
    ) {
        let message = messages_factory::username_proof::create_from_proof(&username_proof, None);
        let mut txn = RocksDbTransactionBatch::new();
        let result = store.merge(&message, &mut txn).unwrap();
        assert_eq!(result.r#type(), HubEventType::MergeUsernameProof);
        match &result.body {
            Some(hub_event::Body::MergeUsernameProofBody(body)) => {
                assert_eq!(*body.username_proof.as_ref().unwrap(), *username_proof);
                assert_eq!(body.deleted_username_proof, None);
            }
            _ => {
                panic!("Unexpected event")
            }
        }
        db.commit(txn).unwrap();
    }

    fn merge_username_proof_with_conflicts(
        store: &Store<UsernameProofStoreDef>,
        db: &Arc<RocksDB>,
        username_proof: &UserNameProof,
        deleted_proof: &UserNameProof,
    ) {
        let message = messages_factory::username_proof::create_from_proof(&username_proof, None);
        let mut txn = RocksDbTransactionBatch::new();
        let result = store.merge(&message, &mut txn).unwrap();
        assert_eq!(result.r#type(), HubEventType::MergeUsernameProof);
        match &result.body {
            Some(hub_event::Body::MergeUsernameProofBody(body)) => {
                assert_eq!(*body.username_proof.as_ref().unwrap(), *username_proof);
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

    fn merge_username_proof_failure(
        store: &Store<UsernameProofStoreDef>,
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

    #[test]
    fn test_should_merge_valid_proofs() {
        let (store, db, _temp_dir) = create_test_store();

        let name = "test".to_string();
        let username_proof = username_factory::create_username_proof(
            FID_FOR_TEST,
            UserNameType::UsernameTypeEnsL1,
            &name,
            None,
            None,
        );

        merge_username_proof_success(&store, &db, &username_proof);

        let retrieved = UsernameProofStore::get_username_proof(
            &store,
            &username_proof.name,
            &mut RocksDbTransactionBatch::new(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            retrieved,
            messages_factory::username_proof::create_from_proof(&username_proof, None)
        );
    }

    #[test]
    fn test_replaces_existing_proof_for_name_if_timestamp_is_newer() {
        let (store, db, _temp_dir) = create_test_store();

        let fname = "test".to_string();
        let username_proof1 = username_factory::create_username_proof(
            FID_FOR_TEST,
            UserNameType::UsernameTypeEnsL1,
            &fname,
            None,
            None,
        );

        merge_username_proof_success(&store, &db, &username_proof1);

        let username_proof2 = username_factory::create_username_proof(
            FID_FOR_TEST,
            UserNameType::UsernameTypeEnsL1,
            &fname,
            Some(username_proof1.timestamp + 10),
            None,
        );

        merge_username_proof_with_conflicts(&store, &db, &username_proof2, &username_proof1);

        let retrieved = UsernameProofStore::get_username_proof(
            &store,
            &username_proof2.name,
            &mut RocksDbTransactionBatch::new(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            retrieved,
            messages_factory::username_proof::create_from_proof(&username_proof2, None)
        );
    }

    #[test]
    fn test_replaces_existing_proof_for_name_even_if_fid_is_different_if_timestamp_is_newer() {
        let (store, db, _temp_dir) = create_test_store();

        let fname = "test2".to_string();
        let username_proof1 = username_factory::create_username_proof(
            FID_FOR_TEST,
            UserNameType::UsernameTypeEnsL1,
            &fname,
            None,
            None,
        );

        merge_username_proof_success(&store, &db, &username_proof1);

        let different_fid = FID_FOR_TEST + 1;
        let username_proof2 = username_factory::create_username_proof(
            different_fid,
            UserNameType::UsernameTypeEnsL1,
            &fname,
            Some(username_proof1.timestamp + 10),
            None,
        );

        merge_username_proof_with_conflicts(&store, &db, &username_proof2, &username_proof1);

        let retrieved = UsernameProofStore::get_username_proof(
            &store,
            &username_proof2.name,
            &mut RocksDbTransactionBatch::new(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            retrieved,
            messages_factory::username_proof::create_from_proof(&username_proof2, None)
        );

        let by_fid_result = UsernameProofStore::get_username_proof_by_fid_and_name(
            &store,
            &username_proof1.name,
            FID_FOR_TEST,
        )
        .unwrap();
        assert!(by_fid_result.is_none());
    }

    #[test]
    fn test_does_not_replace_existing_proof_for_name_if_timestamp_is_older() {
        let (store, db, _temp_dir) = create_test_store();

        let fname = "test3".to_string();
        let username_proof1 = username_factory::create_username_proof(
            FID_FOR_TEST,
            UserNameType::UsernameTypeEnsL1,
            &fname,
            None,
            None,
        );

        merge_username_proof_success(&store, &db, &username_proof1);

        let different_fid = FID_FOR_TEST + 1;
        let username_proof2 = username_factory::create_username_proof(
            different_fid,
            UserNameType::UsernameTypeEnsL1,
            &fname,
            Some(username_proof1.timestamp - 10),
            None,
        );

        let message2 = messages_factory::username_proof::create_from_proof(&username_proof2, None);
        merge_username_proof_failure(
            &store,
            &message2,
            "bad_request.conflict",
            "message conflicts with a more recent add",
        );

        let username_proof3 = username_factory::create_username_proof(
            FID_FOR_TEST,
            UserNameType::UsernameTypeEnsL1,
            &fname,
            Some(username_proof1.timestamp - 10),
            None,
        );

        let message3 = messages_factory::username_proof::create_from_proof(&username_proof3, None);
        merge_username_proof_failure(
            &store,
            &message3,
            "bad_request.conflict",
            "message conflicts with a more recent add",
        );

        let retrieved = UsernameProofStore::get_username_proof(
            &store,
            &username_proof1.name,
            &mut RocksDbTransactionBatch::new(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            retrieved,
            messages_factory::username_proof::create_from_proof(&username_proof1, None)
        );
    }

    #[test]
    fn test_does_not_merge_duplicates() {
        let (store, db, _temp_dir) = create_test_store();

        let name = "test4".to_string();
        let username_proof = username_factory::create_username_proof(
            FID_FOR_TEST,
            UserNameType::UsernameTypeEnsL1,
            &name,
            None,
            None,
        );

        merge_username_proof_success(&store, &db, &username_proof);

        let message = messages_factory::username_proof::create_from_proof(&username_proof, None);
        merge_username_proof_failure(
            &store,
            &message,
            "bad_request.duplicate",
            "message has already been merged",
        );
    }

    #[test]
    fn test_get_username_proof_fails_if_not_found() {
        let (store, _db, _temp_dir) = create_test_store();

        let name = "nonexistent".to_string();
        let result = UsernameProofStore::get_username_proof(
            &store,
            &name.as_bytes().to_vec(),
            &mut RocksDbTransactionBatch::new(),
        );
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().code, "not_found");
    }

    #[test]
    fn test_get_username_proofs_by_fid_should_return_empty_array_if_no_proofs() {
        let (store, _db, _temp_dir) = create_test_store();

        let result = UsernameProofStore::get_username_proofs_by_fid(
            &store,
            FID_FOR_TEST,
            &PageOptions::default(),
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap().messages.len(), 0);
    }

    #[test]
    fn test_get_username_proofs_by_fid_should_return_all_proofs_for_fid() {
        let (store, db, _temp_dir) = create_test_store();

        let another_fid = FID_FOR_TEST + 1;
        let proof1 = username_factory::create_username_proof(
            FID_FOR_TEST,
            UserNameType::UsernameTypeEnsL1,
            &"test5".to_string(),
            None,
            None,
        );
        let proof2 = username_factory::create_username_proof(
            FID_FOR_TEST,
            UserNameType::UsernameTypeEnsL1,
            &"test6".to_string(),
            None,
            None,
        );
        let proof_another_fid = username_factory::create_username_proof(
            another_fid,
            UserNameType::UsernameTypeEnsL1,
            &"test7".to_string(),
            None,
            None,
        );

        merge_username_proof_success(&store, &db, &proof1);
        merge_username_proof_success(&store, &db, &proof2);
        merge_username_proof_success(&store, &db, &proof_another_fid);

        let proofs_for_fid = UsernameProofStore::get_username_proofs_by_fid(
            &store,
            FID_FOR_TEST,
            &PageOptions::default(),
        );
        assert!(proofs_for_fid.is_ok());
        let proofs = proofs_for_fid.unwrap().messages;
        assert_eq!(proofs.len(), 2);
        let message1 = messages_factory::username_proof::create_from_proof(&proof1, None);
        let message2 = messages_factory::username_proof::create_from_proof(&proof2, None);
        assert!(proofs.contains(&message1));
        assert!(proofs.contains(&message2));

        let proofs_for_another_fid = UsernameProofStore::get_username_proofs_by_fid(
            &store,
            another_fid,
            &PageOptions::default(),
        );
        assert!(proofs_for_another_fid.is_ok());
        let another_proofs = proofs_for_another_fid.unwrap().messages;
        assert_eq!(another_proofs.len(), 1);
        let message_another =
            messages_factory::username_proof::create_from_proof(&proof_another_fid, None);
        assert!(another_proofs.contains(&message_another));
    }

    #[test]
    fn test_prune_messages_no_ops_when_no_messages_have_been_merged() {
        let (store, _db, _temp_dir) = create_test_store();

        let mut txn = RocksDbTransactionBatch::new();
        let result = store.prune_messages(FID_FOR_TEST, 0, 2, &mut txn);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[test]
    fn test_prune_messages_prunes_earliest_add_messages() {
        let (store, db, _temp_dir) = create_test_store();

        let current_time = 1609459200; // Jan 1, 2021 00:00:00 UTC

        let add1 = username_factory::create_username_proof(
            FID_FOR_TEST,
            UserNameType::UsernameTypeEnsL1,
            &"test8".to_string(),
            Some(current_time + 1),
            None,
        );
        let add2 = username_factory::create_username_proof(
            FID_FOR_TEST,
            UserNameType::UsernameTypeEnsL1,
            &"test9".to_string(),
            Some(current_time + 2),
            None,
        );
        let add3 = username_factory::create_username_proof(
            FID_FOR_TEST,
            UserNameType::UsernameTypeEnsL1,
            &"test10".to_string(),
            Some(current_time + 3),
            None,
        );
        let add4 = username_factory::create_username_proof(
            FID_FOR_TEST,
            UserNameType::UsernameTypeEnsL1,
            &"test11".to_string(),
            Some(current_time + 5),
            None,
        );

        merge_username_proof_success(&store, &db, &add1);
        merge_username_proof_success(&store, &db, &add2);
        merge_username_proof_success(&store, &db, &add3);
        merge_username_proof_success(&store, &db, &add4);

        let mut txn = RocksDbTransactionBatch::new();
        let result = store.prune_messages(FID_FOR_TEST, 4, 2, &mut txn);
        assert!(result.is_ok());
        let pruned_events = result.unwrap();
        assert_eq!(pruned_events.len(), 2);

        // Extract messages from pruned events
        let mut pruned_messages = Vec::new();
        for event in &pruned_events {
            if let Some(hub_event::Body::MergeUsernameProofBody(body)) = &event.body {
                pruned_messages.push(
                    body.deleted_username_proof_message
                        .as_ref()
                        .unwrap()
                        .clone(),
                );
            }
        }

        let message1 = messages_factory::username_proof::create_from_proof(&add1, None);
        let message2 = messages_factory::username_proof::create_from_proof(&add2, None);
        assert_eq!(pruned_messages, vec![message1.clone(), message2.clone()]);

        db.commit(txn).unwrap();

        let add1_name = &add1.name;
        let add2_name = &add2.name;

        let get_add1 = UsernameProofStore::get_username_proof(
            &store,
            add1_name,
            &mut RocksDbTransactionBatch::new(),
        );
        assert!(get_add1.is_err());
        assert_eq!(get_add1.err().unwrap().code, "not_found");

        let get_add2 = UsernameProofStore::get_username_proof(
            &store,
            add2_name,
            &mut RocksDbTransactionBatch::new(),
        );
        assert!(get_add2.is_err());
        assert_eq!(get_add2.err().unwrap().code, "not_found");
    }
}
