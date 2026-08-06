#[cfg(test)]
mod tests {
    use super::super::super::test_helper::{default_merge_ctx, FID_FOR_TEST};
    use crate::proto::{self as message, hub_event, HubEventType, ReactionType};
    use crate::storage::db::{PageOptions, RocksDB, RocksDbTransactionBatch};
    use crate::storage::store::account::{
        make_fid_key, make_ts_hash, select_verification_address_winner, Store, StoreEventHandler,
        VerificationStore, VerificationStoreDef, TS_HASH_LENGTH,
    };
    use crate::storage::util::{decrement_vec_u8, increment_vec_u8};
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
        let result = store
            .merge(&message, &mut txn, &default_merge_ctx())
            .unwrap();
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
        let result = store
            .merge(&message, &mut txn, &default_merge_ctx())
            .unwrap();
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
        let result = store.merge(&message, &mut txn, &default_merge_ctx());
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

        let result = VerificationStore::get_verification_add(&store, FID_FOR_TEST, &address, None);
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

        let retrieved =
            VerificationStore::get_verification_add(&store, FID_FOR_TEST, &address, None);
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

    #[test]
    fn test_get_verifications_by_address_returns_all_fid_entries() {
        let (store, db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();
        let fid1 = FID_FOR_TEST;
        let fid2 = FID_FOR_TEST + 1;

        let verification_add1 = messages_factory::verifications::create_verification_add(
            fid1,
            0,
            address.clone(),
            vec![],
            vec![],
            Some(1),
            None,
        );
        let verification_add2 = messages_factory::verifications::create_verification_add(
            fid2,
            0,
            address.clone(),
            vec![],
            vec![],
            Some(2),
            None,
        );

        merge_message_success(&store, &db, &verification_add1);
        merge_message_success(&store, &db, &verification_add2);

        let entries =
            VerificationStore::get_verifications_by_address(&store, &address, None).unwrap();
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().any(|(fid, ts_hash)| {
            *fid == fid1
                && *ts_hash
                    == make_ts_hash(
                        verification_add1.data.as_ref().unwrap().timestamp,
                        &verification_add1.hash,
                    )
                    .unwrap()
        }));
        assert!(entries.iter().any(|(fid, ts_hash)| {
            *fid == fid2
                && *ts_hash
                    == make_ts_hash(
                        verification_add2.data.as_ref().unwrap().timestamp,
                        &verification_add2.hash,
                    )
                    .unwrap()
        }));
    }

    #[test]
    fn test_verification_remove_deletes_only_own_by_address_entry() {
        let (store, db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();
        let fid1 = FID_FOR_TEST;
        let fid2 = FID_FOR_TEST + 1;

        let verification_add1 = messages_factory::verifications::create_verification_add(
            fid1,
            0,
            address.clone(),
            vec![],
            vec![],
            Some(1),
            None,
        );
        let verification_add2 = messages_factory::verifications::create_verification_add(
            fid2,
            0,
            address.clone(),
            vec![],
            vec![],
            Some(2),
            None,
        );
        let verification_remove1 = messages_factory::verifications::create_verification_remove(
            fid1,
            address.clone(),
            Some(3),
            None,
        );

        merge_message_success(&store, &db, &verification_add1);
        merge_message_success(&store, &db, &verification_add2);
        merge_message_with_conflicts(&store, &db, &verification_remove1, vec![verification_add1]);

        let entries =
            VerificationStore::get_verifications_by_address(&store, &address, None).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, fid2);
        assert_eq!(
            entries[0].1,
            make_ts_hash(
                verification_add2.data.as_ref().unwrap().timestamp,
                &verification_add2.hash
            )
            .unwrap()
        );
    }

    #[test]
    fn test_reverify_updates_own_by_address_ts_hash() {
        let (store, db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();

        let verification_add = messages_factory::verifications::create_verification_add(
            FID_FOR_TEST,
            0,
            address.clone(),
            vec![],
            vec![],
            Some(1),
            None,
        );
        let verification_add_later = messages_factory::verifications::create_verification_add(
            FID_FOR_TEST,
            0,
            address.clone(),
            vec![],
            vec![],
            Some(2),
            None,
        );

        merge_message_success(&store, &db, &verification_add);
        merge_message_with_conflicts(&store, &db, &verification_add_later, vec![verification_add]);

        let entries =
            VerificationStore::get_verifications_by_address(&store, &address, None).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, FID_FOR_TEST);
        assert_eq!(
            entries[0].1,
            make_ts_hash(
                verification_add_later.data.as_ref().unwrap().timestamp,
                &verification_add_later.hash
            )
            .unwrap()
        );
    }

    #[test]
    fn test_get_verifications_by_address_skips_legacy_slot() {
        let (store, db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();

        // Simulate the pre-migration on-disk state: a legacy address-only slot
        // (key == prefix, value == a bare 4-byte fid).
        let mut txn = RocksDbTransactionBatch::new();
        txn.put(
            VerificationStoreDef::make_verification_by_address_prefix(&address),
            make_fid_key(999),
        );
        db.commit(txn).unwrap();

        // The reader tolerates the transitional shape: it skips the legacy slot
        // rather than erroring the whole read.
        let entries =
            VerificationStore::get_verifications_by_address(&store, &address, None).unwrap();
        assert!(entries.is_empty());

        // A real (new-format) verification coexisting with the legacy slot is
        // still returned; the legacy slot stays skipped.
        let verification_add = messages_factory::verifications::create_verification_add(
            FID_FOR_TEST,
            0,
            address.clone(),
            vec![],
            vec![],
            Some(1),
            None,
        );
        merge_message_success(&store, &db, &verification_add);

        let entries =
            VerificationStore::get_verifications_by_address(&store, &address, None).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, FID_FOR_TEST);
    }

    #[test]
    fn test_get_verifications_by_address_isolates_addresses_of_different_lengths() {
        let (store, db, _temp_dir) = create_test_store();
        // A 20-byte ETH-length address and a 32-byte Solana-length address whose
        // prefix scans must not bleed into each other.
        let addr_eth = vec![0x11u8; 20];
        let addr_sol = vec![0x22u8; 32];
        let ts_eth = [0xAAu8; TS_HASH_LENGTH];
        let ts_sol = [0xBBu8; TS_HASH_LENGTH];

        let mut txn = RocksDbTransactionBatch::new();
        txn.put(
            VerificationStoreDef::make_verification_by_address_key(&addr_eth, 10),
            ts_eth.to_vec(),
        );
        txn.put(
            VerificationStoreDef::make_verification_by_address_key(&addr_sol, 20),
            ts_sol.to_vec(),
        );
        db.commit(txn).unwrap();

        let eth = VerificationStore::get_verifications_by_address(&store, &addr_eth, None).unwrap();
        assert_eq!(eth, vec![(10u64, ts_eth)]);
        let sol = VerificationStore::get_verifications_by_address(&store, &addr_sol, None).unwrap();
        assert_eq!(sol, vec![(20u64, ts_sol)]);
    }

    #[test]
    fn test_get_verifications_by_address_overlays_same_transaction_puts_and_deletes() {
        let (store, _db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();
        let add1 = messages_factory::verifications::create_verification_add(
            101,
            0,
            address.clone(),
            vec![],
            vec![],
            Some(1),
            None,
        );
        let remove1 = messages_factory::verifications::create_verification_remove(
            101,
            address.clone(),
            Some(2),
            None,
        );
        let add2 = messages_factory::verifications::create_verification_add(
            202,
            0,
            address.clone(),
            vec![],
            vec![],
            Some(3),
            None,
        );
        let mut txn = RocksDbTransactionBatch::new();
        store.merge(&add1, &mut txn, &default_merge_ctx()).unwrap();
        store
            .merge(&remove1, &mut txn, &default_merge_ctx())
            .unwrap();
        store.merge(&add2, &mut txn, &default_merge_ctx()).unwrap();

        assert!(
            VerificationStore::get_verifications_by_address(&store, &address, None)
                .unwrap()
                .is_empty()
        );
        let entries =
            VerificationStore::get_verifications_by_address(&store, &address, Some(&txn)).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, 202);
        assert_eq!(
            entries[0].1,
            make_ts_hash(add2.data.as_ref().unwrap().timestamp, &add2.hash).unwrap()
        );
    }

    #[test]
    fn test_select_verification_address_winner_uses_ts_hash_then_lower_fid() {
        let lower = [1u8; TS_HASH_LENGTH];
        let higher = [2u8; TS_HASH_LENGTH];
        assert_eq!(
            select_verification_address_winner(vec![(1, lower), (999, higher)]),
            Some(999)
        );
        assert_eq!(
            select_verification_address_winner(vec![(999, higher), (1, higher)]),
            Some(1)
        );
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
        let retrieved =
            VerificationStore::get_verification_add(&store, FID_FOR_TEST, &address, None);
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
        let retrieved =
            VerificationStore::get_verification_add(&store, FID_FOR_TEST, &address, None);
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
        let retrieved =
            VerificationStore::get_verification_add(&store, FID_FOR_TEST, &address, None);
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
        let retrieved =
            VerificationStore::get_verification_add(&store, FID_FOR_TEST, &address, None);
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
        verification_add_later.hash = increment_vec_u8(&verification_add.hash);

        merge_message_success(&store, &db, &verification_add);
        merge_message_with_conflicts(&store, &db, &verification_add_later, vec![verification_add]);

        // Verify the higher hash message exists
        let retrieved =
            VerificationStore::get_verification_add(&store, FID_FOR_TEST, &address, None);
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
        verification_add_later.hash = increment_vec_u8(&verification_add.hash);

        merge_message_success(&store, &db, &verification_add_later);
        merge_message_failure(
            &store,
            &verification_add,
            "bad_request.conflict",
            "message conflicts with a more recent add",
        );

        // Verify the higher hash message still exists
        let retrieved =
            VerificationStore::get_verification_add(&store, FID_FOR_TEST, &address, None);
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
        let retrieved =
            VerificationStore::get_verification_add(&store, FID_FOR_TEST, &address, None);
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
        verification_remove_later.hash = increment_vec_u8(&verification_add.hash);

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

        let verification_add = messages_factory::verifications::create_verification_add(
            FID_FOR_TEST,
            0, // verification_type
            address.clone(),
            vec![], // claim_signature
            vec![], // block_hash
            Some(1),
            None,
        );

        let mut verification_remove_earlier =
            messages_factory::verifications::create_verification_remove(
                FID_FOR_TEST,
                address.clone(),
                Some(1), // same timestamp
                None,
            );
        verification_remove_earlier.hash = decrement_vec_u8(&verification_add.hash);

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
        verification_remove_later.hash = increment_vec_u8(&verification_remove.hash);

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
        verification_remove_later.hash = increment_vec_u8(&verification_remove.hash);

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
        let retrieved_add =
            VerificationStore::get_verification_add(&store, FID_FOR_TEST, &address, None);
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
        let retrieved =
            VerificationStore::get_verification_add(&store, FID_FOR_TEST, &address, None);
        assert_eq!(retrieved.unwrap().unwrap(), verification_add_later);
    }

    #[test]
    fn test_verification_remove_succeeds_regardless_of_add_message_hash() {
        let (store, db, _temp_dir) = create_test_store();
        let address = address::generate_random_address();

        let verification_add_same_time = messages_factory::verifications::create_verification_add(
            FID_FOR_TEST,
            0, // verification_type
            address.clone(),
            vec![], // claim_signature
            vec![], // block_hash
            Some(1),
            None,
        );

        let mut verification_remove = messages_factory::verifications::create_verification_remove(
            FID_FOR_TEST,
            address.clone(),
            Some(1), // same timestamp
            None,
        );
        verification_remove.hash = decrement_vec_u8(&verification_add_same_time.hash);

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
        let retrieved_add =
            VerificationStore::get_verification_add(&store, FID_FOR_TEST, &address, None);
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
        let retrieved =
            VerificationStore::get_verification_add(&store, FID_FOR_TEST, &address, None);
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
        let retrieved =
            VerificationStore::get_verification_add(&store, FID_FOR_TEST, &address, None);
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
