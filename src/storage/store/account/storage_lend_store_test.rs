#[cfg(test)]
mod tests {
    use crate::proto::{self as message, hub_event, HubEventType, StorageUnitType};
    use crate::storage::db::{RocksDB, RocksDbTransactionBatch};
    use crate::storage::store::account::{
        StorageLendStore, StorageLendStoreDef, Store, StoreEventHandler,
    };
    use crate::storage::store::test_helper::FID_FOR_TEST;
    use crate::utils::factory::messages_factory;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_store() -> (Store<StorageLendStoreDef>, Arc<RocksDB>, TempDir) {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = RocksDB::new(db_path.to_str().unwrap());
        db.open().unwrap();
        let db = Arc::new(db);

        let event_handler = StoreEventHandler::new();
        let store = StorageLendStore::new(db.clone(), event_handler.clone(), 10);

        (store, db.clone(), temp_dir)
    }

    fn merge_message_success(
        store: &Store<StorageLendStoreDef>,
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
        store: &Store<StorageLendStoreDef>,
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
        store: &Store<StorageLendStoreDef>,
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

    // Basic Storage Lend Message Tests

    #[test]
    fn test_merge_storage_lend_succeeds() {
        let (store, db, _temp_dir) = create_test_store();

        let storage_lend = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST,
            FID_FOR_TEST + 1,
            100,
            StorageUnitType::UnitType2024,
            None,
            None,
        );
        merge_message_success(&store, &db, &storage_lend);

        // Verify the message was stored
        let result = store.get_add(&storage_lend, None).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), storage_lend);
    }

    #[test]
    fn test_merge_storage_lend_fails_if_merged_twice() {
        let (store, db, _temp_dir) = create_test_store();

        let storage_lend = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST,
            FID_FOR_TEST + 1,
            100,
            StorageUnitType::UnitType2024,
            None,
            None,
        );
        merge_message_success(&store, &db, &storage_lend);

        // Try to merge the same message again
        merge_message_failure(
            &store,
            &storage_lend,
            "bad_request.duplicate",
            "message has already been merged",
        );
    }

    #[test]
    fn test_merge_fails_with_invalid_message_type() {
        let (store, _db, _temp_dir) = create_test_store();

        let cast_add =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "Hello, world!", None, None);

        merge_message_failure(
            &store,
            &cast_add,
            "bad_request.validation_failure",
            "invalid message type",
        );
    }

    #[test]
    fn test_merge_storage_lend_fails_with_missing_body() {
        let (store, _db, _temp_dir) = create_test_store();

        // Create a message without a body
        let mut storage_lend = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST,
            FID_FOR_TEST + 1,
            100,
            StorageUnitType::UnitType2024,
            None,
            None,
        );

        // Remove the body
        if let Some(ref mut data) = storage_lend.data {
            data.body = None;
        }

        merge_message_failure(
            &store,
            &storage_lend,
            "bad_request.validation_failure",
            "invalid message type",
        );
    }

    // Primary Index Tests (get_lent_storage)

    #[test]
    fn test_get_lent_storage_empty_if_no_messages() {
        let (store, _db, _temp_dir) = create_test_store();

        let result = StorageLendStore::get_lent_storage(&store, FID_FOR_TEST).unwrap();
        assert_eq!(
            result.units_for(StorageUnitType::UnitTypeLegacy)
                + result.units_for(StorageUnitType::UnitType2024)
                + result.units_for(StorageUnitType::UnitType2025),
            0
        );
        assert!(!result.is_active());
    }

    #[test]
    fn test_get_lent_storage_returns_single_lend() {
        let (store, db, _temp_dir) = create_test_store();

        let storage_lend = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST,
            FID_FOR_TEST + 1,
            100,
            StorageUnitType::UnitType2024,
            None,
            None,
        );
        merge_message_success(&store, &db, &storage_lend);

        let result = StorageLendStore::get_lent_storage(&store, FID_FOR_TEST).unwrap();
        assert_eq!(result.units_for(StorageUnitType::UnitType2024), 100);
        assert_eq!(result.units_for(StorageUnitType::UnitTypeLegacy), 0);
        assert_eq!(result.units_for(StorageUnitType::UnitType2025), 0);
        assert!(result.is_active());
    }

    #[test]
    fn test_get_lent_storage_aggregates_multiple_lends() {
        let (store, db, _temp_dir) = create_test_store();

        // Lend to different recipients
        let storage_lend1 = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST,
            FID_FOR_TEST + 1,
            100,
            StorageUnitType::UnitType2024,
            None,
            None,
        );
        let storage_lend2 = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST,
            FID_FOR_TEST + 2,
            50,
            StorageUnitType::UnitType2024,
            None,
            None,
        );
        let storage_lend3 = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST,
            FID_FOR_TEST + 3,
            25,
            StorageUnitType::UnitTypeLegacy,
            None,
            None,
        );

        merge_message_success(&store, &db, &storage_lend1);
        merge_message_success(&store, &db, &storage_lend2);
        merge_message_success(&store, &db, &storage_lend3);

        let result = StorageLendStore::get_lent_storage(&store, FID_FOR_TEST).unwrap();
        assert_eq!(result.units_for(StorageUnitType::UnitType2024), 150); // 100 + 50
        assert_eq!(result.units_for(StorageUnitType::UnitTypeLegacy), 25);
        assert_eq!(result.units_for(StorageUnitType::UnitType2025), 0);
        assert!(result.is_active());
    }

    #[test]
    fn test_get_lent_storage_wrong_fid_returns_empty() {
        let (store, db, _temp_dir) = create_test_store();

        let storage_lend = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST,
            FID_FOR_TEST + 1,
            100,
            StorageUnitType::UnitType2024,
            None,
            None,
        );
        merge_message_success(&store, &db, &storage_lend);

        let result = StorageLendStore::get_lent_storage(&store, FID_FOR_TEST + 10).unwrap();
        assert_eq!(
            result.units_for(StorageUnitType::UnitTypeLegacy)
                + result.units_for(StorageUnitType::UnitType2024)
                + result.units_for(StorageUnitType::UnitType2025),
            0
        );
        assert!(!result.is_active());
    }

    // Secondary Index Tests (get_borrowed_storage)

    #[test]
    fn test_get_borrowed_storage_empty_if_no_messages() {
        let (store, _db, _temp_dir) = create_test_store();

        let result = StorageLendStore::get_borrowed_storage(&store, FID_FOR_TEST).unwrap();
        assert_eq!(
            result.units_for(StorageUnitType::UnitTypeLegacy)
                + result.units_for(StorageUnitType::UnitType2024)
                + result.units_for(StorageUnitType::UnitType2025),
            0
        );
        assert!(!result.is_active());
    }

    #[test]
    fn test_get_borrowed_storage_returns_single_borrow() {
        let (store, db, _temp_dir) = create_test_store();

        let storage_lend = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST + 1,
            FID_FOR_TEST, // This fid is the borrower
            100,
            StorageUnitType::UnitType2024,
            None,
            None,
        );
        merge_message_success(&store, &db, &storage_lend);

        let result = StorageLendStore::get_borrowed_storage(&store, FID_FOR_TEST).unwrap();
        assert_eq!(result.units_for(StorageUnitType::UnitType2024), 100);
        assert_eq!(result.units_for(StorageUnitType::UnitTypeLegacy), 0);
        assert_eq!(result.units_for(StorageUnitType::UnitType2025), 0);
        assert!(result.is_active());
    }

    #[test]
    fn test_get_borrowed_storage_aggregates_multiple_borrows() {
        let (store, db, _temp_dir) = create_test_store();

        // Multiple lenders lending to the same borrower (FID_FOR_TEST)
        let storage_lend1 = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST + 1,
            FID_FOR_TEST,
            100,
            StorageUnitType::UnitType2024,
            None,
            None,
        );
        let storage_lend2 = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST + 2,
            FID_FOR_TEST,
            50,
            StorageUnitType::UnitType2024,
            None,
            None,
        );
        let storage_lend3 = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST + 3,
            FID_FOR_TEST,
            25,
            StorageUnitType::UnitTypeLegacy,
            None,
            None,
        );

        merge_message_success(&store, &db, &storage_lend1);
        merge_message_success(&store, &db, &storage_lend2);
        merge_message_success(&store, &db, &storage_lend3);

        let result = StorageLendStore::get_borrowed_storage(&store, FID_FOR_TEST).unwrap();
        assert_eq!(result.units_for(StorageUnitType::UnitType2024), 150); // 100 + 50
        assert_eq!(result.units_for(StorageUnitType::UnitTypeLegacy), 25);
        assert_eq!(result.units_for(StorageUnitType::UnitType2025), 0);
        assert!(result.is_active());
    }

    #[test]
    fn test_get_borrowed_storage_wrong_fid_returns_empty() {
        let (store, db, _temp_dir) = create_test_store();

        let storage_lend = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST + 1,
            FID_FOR_TEST,
            100,
            StorageUnitType::UnitType2024,
            None,
            None,
        );
        merge_message_success(&store, &db, &storage_lend);

        let result = StorageLendStore::get_borrowed_storage(&store, FID_FOR_TEST + 10).unwrap();
        assert_eq!(
            result.units_for(StorageUnitType::UnitTypeLegacy)
                + result.units_for(StorageUnitType::UnitType2024)
                + result.units_for(StorageUnitType::UnitType2025),
            0
        );
        assert!(!result.is_active());
    }

    // Integration Tests

    #[test]
    fn test_lend_and_borrow_consistency() {
        let (store, db, _temp_dir) = create_test_store();

        let lender_fid = FID_FOR_TEST;
        let borrower_fid = FID_FOR_TEST + 1;

        let storage_lend = messages_factory::storage_lend::create_storage_lend(
            lender_fid,
            borrower_fid,
            100,
            StorageUnitType::UnitType2024,
            None,
            None,
        );
        merge_message_success(&store, &db, &storage_lend);

        // Check that lender's lent storage shows the lend
        let lent_result = StorageLendStore::get_lent_storage(&store, lender_fid).unwrap();
        assert_eq!(lent_result.units_for(StorageUnitType::UnitType2024), 100);
        assert!(lent_result.is_active());

        // Check that borrower's borrowed storage shows the lend
        let borrowed_result = StorageLendStore::get_borrowed_storage(&store, borrower_fid).unwrap();
        assert_eq!(
            borrowed_result.units_for(StorageUnitType::UnitType2024),
            100
        );
        assert!(borrowed_result.is_active());

        // Verify that the amounts match
        assert_eq!(
            lent_result.units_for(StorageUnitType::UnitType2024),
            borrowed_result.units_for(StorageUnitType::UnitType2024)
        );
    }

    #[test]
    fn test_multiple_fids_lend_borrow_isolation() {
        let (store, db, _temp_dir) = create_test_store();

        let fid1 = FID_FOR_TEST;
        let fid2 = FID_FOR_TEST + 1;
        let fid3 = FID_FOR_TEST + 2;

        // FID1 lends to FID2
        let storage_lend1 = messages_factory::storage_lend::create_storage_lend(
            fid1,
            fid2,
            100,
            StorageUnitType::UnitType2024,
            None,
            None,
        );
        // FID2 lends to FID3
        let storage_lend2 = messages_factory::storage_lend::create_storage_lend(
            fid2,
            fid3,
            50,
            StorageUnitType::UnitType2024,
            None,
            None,
        );

        merge_message_success(&store, &db, &storage_lend1);
        merge_message_success(&store, &db, &storage_lend2);

        // FID1 should have 100 lent, 0 borrowed
        let fid1_lent = StorageLendStore::get_lent_storage(&store, fid1).unwrap();
        let fid1_borrowed = StorageLendStore::get_borrowed_storage(&store, fid1).unwrap();
        assert_eq!(fid1_lent.units_for(StorageUnitType::UnitType2024), 100);
        assert_eq!(fid1_borrowed.units_for(StorageUnitType::UnitType2024), 0);
        assert!(fid1_lent.is_active());
        assert!(!fid1_borrowed.is_active());

        // FID2 should have 50 lent, 100 borrowed
        let fid2_lent = StorageLendStore::get_lent_storage(&store, fid2).unwrap();
        let fid2_borrowed = StorageLendStore::get_borrowed_storage(&store, fid2).unwrap();
        assert_eq!(fid2_lent.units_for(StorageUnitType::UnitType2024), 50);
        assert_eq!(fid2_borrowed.units_for(StorageUnitType::UnitType2024), 100);
        assert!(fid2_lent.is_active());
        assert!(fid2_borrowed.is_active());

        // FID3 should have 0 lent, 50 borrowed
        let fid3_lent = StorageLendStore::get_lent_storage(&store, fid3).unwrap();
        let fid3_borrowed = StorageLendStore::get_borrowed_storage(&store, fid3).unwrap();
        assert_eq!(fid3_lent.units_for(StorageUnitType::UnitType2024), 0);
        assert_eq!(fid3_borrowed.units_for(StorageUnitType::UnitType2024), 50);
        assert!(!fid3_lent.is_active());
        assert!(fid3_borrowed.is_active());
    }

    // Storage Slot Aggregation Tests

    #[test]
    fn test_storage_slot_merge_different_units() {
        let (store, db, _temp_dir) = create_test_store();

        let storage_lend1 = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST,
            FID_FOR_TEST + 1,
            100,
            StorageUnitType::UnitTypeLegacy,
            None,
            None,
        );
        let storage_lend2 = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST,
            FID_FOR_TEST + 2,
            200,
            StorageUnitType::UnitType2024,
            None,
            None,
        );
        let storage_lend3 = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST,
            FID_FOR_TEST + 3,
            50,
            StorageUnitType::UnitType2025,
            None,
            None,
        );

        merge_message_success(&store, &db, &storage_lend1);
        merge_message_success(&store, &db, &storage_lend2);
        merge_message_success(&store, &db, &storage_lend3);

        let result = StorageLendStore::get_lent_storage(&store, FID_FOR_TEST).unwrap();
        assert_eq!(result.units_for(StorageUnitType::UnitTypeLegacy), 100);
        assert_eq!(result.units_for(StorageUnitType::UnitType2024), 200);
        assert_eq!(result.units_for(StorageUnitType::UnitType2025), 50);
        assert_eq!(
            result.units_for(StorageUnitType::UnitTypeLegacy)
                + result.units_for(StorageUnitType::UnitType2024)
                + result.units_for(StorageUnitType::UnitType2025),
            350
        ); // Total units
        assert!(result.is_active());
    }

    // Conflict Resolution Tests

    #[test]
    fn test_storage_lend_replaces_with_later_timestamp() {
        let (store, db, _temp_dir) = create_test_store();

        let lender_fid = FID_FOR_TEST;
        let borrower_fid = FID_FOR_TEST + 1;
        let base_timestamp = 1000;

        // First storage lend
        let storage_lend1 = messages_factory::storage_lend::create_storage_lend(
            lender_fid,
            borrower_fid,
            100,
            StorageUnitType::UnitType2024,
            Some(base_timestamp),
            None,
        );
        merge_message_success(&store, &db, &storage_lend1);

        // Second storage lend with later timestamp should replace the first
        let storage_lend2 = messages_factory::storage_lend::create_storage_lend(
            lender_fid,
            borrower_fid,
            200,
            StorageUnitType::UnitType2024,
            Some(base_timestamp + 100),
            None,
        );
        merge_message_with_conflicts(&store, &db, &storage_lend2, vec![storage_lend1]);

        // Verify only the newer message is stored
        let lent_result = StorageLendStore::get_lent_storage(&store, lender_fid).unwrap();
        assert_eq!(lent_result.units_for(StorageUnitType::UnitType2024), 200);
        assert!(lent_result.is_active());

        let borrowed_result = StorageLendStore::get_borrowed_storage(&store, borrower_fid).unwrap();
        assert_eq!(
            borrowed_result.units_for(StorageUnitType::UnitType2024),
            200
        );
        assert!(borrowed_result.is_active());
    }

    #[test]
    fn test_storage_lend_fails_with_earlier_timestamp() {
        let (store, db, _temp_dir) = create_test_store();

        let lender_fid = FID_FOR_TEST;
        let borrower_fid = FID_FOR_TEST + 1;
        let base_timestamp = 1000;

        // First storage lend
        let storage_lend1 = messages_factory::storage_lend::create_storage_lend(
            lender_fid,
            borrower_fid,
            100,
            StorageUnitType::UnitType2024,
            Some(base_timestamp + 100),
            None,
        );
        merge_message_success(&store, &db, &storage_lend1);

        // Second storage lend with earlier timestamp should fail
        let storage_lend2 = messages_factory::storage_lend::create_storage_lend(
            lender_fid,
            borrower_fid,
            200,
            StorageUnitType::UnitType2024,
            Some(base_timestamp),
            None,
        );
        merge_message_failure(
            &store,
            &storage_lend2,
            "bad_request.conflict",
            "message conflicts with a more recent add",
        );

        // Verify original message is still stored
        let lent_result = StorageLendStore::get_lent_storage(&store, lender_fid).unwrap();
        assert_eq!(lent_result.units_for(StorageUnitType::UnitType2024), 100);
        assert!(lent_result.is_active());

        let borrowed_result = StorageLendStore::get_borrowed_storage(&store, borrower_fid).unwrap();
        assert_eq!(
            borrowed_result.units_for(StorageUnitType::UnitType2024),
            100
        );
        assert!(borrowed_result.is_active());
    }

    #[test]
    fn test_storage_lend_replaces_with_same_timestamp_higher_hash() {
        let (store, db, _temp_dir) = create_test_store();

        let lender_fid = FID_FOR_TEST;
        let borrower_fid = FID_FOR_TEST + 1;
        let timestamp = 1000;

        // Create two messages with same timestamp but different hashes
        let storage_lend1 = messages_factory::storage_lend::create_storage_lend(
            lender_fid,
            borrower_fid,
            100,
            StorageUnitType::UnitType2024,
            Some(timestamp),
            None,
        );

        let storage_lend2 = messages_factory::storage_lend::create_storage_lend(
            lender_fid,
            borrower_fid,
            200,
            StorageUnitType::UnitType2024,
            Some(timestamp),
            None,
        );

        // Determine which message has the higher hash
        let hash1 = &storage_lend1.hash;
        let hash2 = &storage_lend2.hash;

        let (lower_hash_message, higher_hash_message, higher_units) = if hash1 < hash2 {
            (&storage_lend1, &storage_lend2, 200)
        } else {
            (&storage_lend2, &storage_lend1, 100)
        };

        // First merge the message with the lower hash
        merge_message_success(&store, &db, lower_hash_message);

        // Then merge the message with the higher hash - it should replace the first
        merge_message_with_conflicts(
            &store,
            &db,
            higher_hash_message,
            vec![lower_hash_message.clone()],
        );

        // Verify the higher hash message won
        let lent_result = StorageLendStore::get_lent_storage(&store, lender_fid).unwrap();
        assert_eq!(
            lent_result.units_for(StorageUnitType::UnitType2024),
            higher_units
        );
        assert!(lent_result.is_active());

        let borrowed_result = StorageLendStore::get_borrowed_storage(&store, borrower_fid).unwrap();
        assert_eq!(
            borrowed_result.units_for(StorageUnitType::UnitType2024),
            higher_units
        );
        assert!(borrowed_result.is_active());
    }

    // get_messages_by_borrower_fid Tests
    #[test]
    fn test_get_messages_by_borrower_fid_does_not_return_lent_messages() {
        let (store, db, _temp_dir) = create_test_store();

        let lender_fid = FID_FOR_TEST;
        let borrower_fid = FID_FOR_TEST + 1;

        // FID_FOR_TEST lends to FID_FOR_TEST + 1
        let storage_lend = messages_factory::storage_lend::create_storage_lend(
            lender_fid,
            borrower_fid,
            100,
            StorageUnitType::UnitType2024,
            None,
            None,
        );
        merge_message_success(&store, &db, &storage_lend);

        // Query by lender FID should return empty (lender is not a borrower in this case)
        let result = StorageLendStore::get_messages_by_borrower_fid(&store, lender_fid).unwrap();
        assert_eq!(result.len(), 0);

        // Query by borrower FID should return the message
        let result = StorageLendStore::get_messages_by_borrower_fid(&store, borrower_fid).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], storage_lend);
    }

    #[test]
    fn test_get_messages_by_borrower_fid_after_replacement() {
        let (store, db, _temp_dir) = create_test_store();

        let lender_fid = FID_FOR_TEST;
        let borrower_fid = FID_FOR_TEST + 1;
        let base_timestamp = 1000;

        // First storage lend
        let storage_lend1 = messages_factory::storage_lend::create_storage_lend(
            lender_fid,
            borrower_fid,
            100,
            StorageUnitType::UnitType2024,
            Some(base_timestamp),
            None,
        );
        merge_message_success(&store, &db, &storage_lend1);

        // Second storage lend with later timestamp should replace the first
        let storage_lend2 = messages_factory::storage_lend::create_storage_lend(
            lender_fid,
            borrower_fid,
            200,
            StorageUnitType::UnitType2024,
            Some(base_timestamp + 100),
            None,
        );
        merge_message_with_conflicts(&store, &db, &storage_lend2, vec![storage_lend1]);

        // Query should return only the replacement message
        let result = StorageLendStore::get_messages_by_borrower_fid(&store, borrower_fid).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], storage_lend2);

        match &result[0].data.as_ref().unwrap().body {
            Some(message::message_data::Body::LendStorageBody(body)) => {
                assert_eq!(body.num_units, 200);
            }
            _ => panic!("Expected LendStorageBody"),
        }
    }

    #[test]
    fn test_get_messages_by_borrower_fid_isolation() {
        let (store, db, _temp_dir) = create_test_store();

        let fid1 = FID_FOR_TEST;
        let fid2 = FID_FOR_TEST + 1;
        let fid3 = FID_FOR_TEST + 2;

        // FID1 lends to FID2
        let storage_lend1 = messages_factory::storage_lend::create_storage_lend(
            fid1,
            fid2,
            100,
            StorageUnitType::UnitType2024,
            None,
            None,
        );
        // FID2 lends to FID3
        let storage_lend2 = messages_factory::storage_lend::create_storage_lend(
            fid2,
            fid3,
            50,
            StorageUnitType::UnitType2024,
            None,
            None,
        );
        // FID1 lends to FID3
        let storage_lend3 = messages_factory::storage_lend::create_storage_lend(
            fid1,
            fid3,
            75,
            StorageUnitType::UnitType2024,
            None,
            None,
        );

        merge_message_success(&store, &db, &storage_lend1);
        merge_message_success(&store, &db, &storage_lend2);
        merge_message_success(&store, &db, &storage_lend3);

        // FID1 should have no messages (not a borrower)
        let fid1_messages = StorageLendStore::get_messages_by_borrower_fid(&store, fid1).unwrap();
        assert_eq!(fid1_messages.len(), 0);

        // FID2 should have 1 message (from FID1)
        let fid2_messages = StorageLendStore::get_messages_by_borrower_fid(&store, fid2).unwrap();
        assert_eq!(fid2_messages.len(), 1);
        assert_eq!(fid2_messages[0], storage_lend1);

        // FID3 should have 2 messages (from FID1 and FID2)
        let fid3_messages = StorageLendStore::get_messages_by_borrower_fid(&store, fid3).unwrap();
        assert_eq!(fid3_messages.len(), 2);
        assert_eq!(fid3_messages, vec![storage_lend3, storage_lend2]);
    }
}
