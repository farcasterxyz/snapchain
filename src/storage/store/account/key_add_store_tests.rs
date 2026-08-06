#[cfg(test)]
mod tests {
    use crate::proto;
    use crate::storage::db;
    use crate::storage::db::PageOptions;
    use crate::storage::db::RocksDbTransactionBatch;
    use crate::storage::store::account::{
        decrement_gasless_key_count, delete_gasless_key_owner, delete_gasless_key_record,
        get_gasless_key_count, get_gasless_key_owner_fid, get_gasless_key_record,
        increment_gasless_key_count, list_gasless_keys_by_fid, make_gasless_key_by_fid_key,
        make_gasless_key_by_public_key_key, make_gasless_key_count_by_fid_key,
        put_gasless_key_owner, put_gasless_key_record, GaslessKeyRecord,
    };
    use std::sync::Arc;
    use tempfile::TempDir;

    fn open_db() -> (Arc<db::RocksDB>, TempDir) {
        let dir = tempfile::TempDir::new().unwrap();
        let db_path = dir.path().join("gasless_key.db");
        let db = db::RocksDB::new(db_path.to_str().unwrap());
        db.open().unwrap();
        (Arc::new(db), dir)
    }

    const KEY_A: [u8; 32] = [0xAA; 32];
    const KEY_B: [u8; 32] = [0xBB; 32];

    // Builds a minimal but structurally-valid GaslessKeyRecord. The embedded Message is just
    // a skeleton with the hash populated so get/put round-trips are meaningful — these tests
    // exercise the store layer, not the validation layer (the merge orchestration covers that).
    fn sample_record(request_fid: u64, message_hash: Vec<u8>) -> GaslessKeyRecord {
        GaslessKeyRecord {
            message: Some(proto::Message {
                data: None,
                hash: message_hash,
                hash_scheme: 0,
                signature: vec![],
                signature_scheme: 0,
                signer: vec![],
                data_bytes: None,
            }),
            request_fid,
        }
    }

    #[test]
    fn key_layout_varies_by_fid_and_public_key() {
        let ka_fid7 = make_gasless_key_by_fid_key(7, &KEY_A).unwrap();
        let ka_fid8 = make_gasless_key_by_fid_key(8, &KEY_A).unwrap();
        let kb_fid7 = make_gasless_key_by_fid_key(7, &KEY_B).unwrap();
        assert_ne!(ka_fid7, ka_fid8, "same key, different fid must differ");
        assert_ne!(ka_fid7, kb_fid7, "same fid, different key must differ");
    }

    #[test]
    fn make_key_rejects_wrong_length_public_key() {
        let err = make_gasless_key_by_fid_key(7, &[0xAA; 31]).unwrap_err();
        assert_eq!(err.code, "bad_request.validation_failure");
    }

    #[test]
    fn put_then_get_roundtrips_record() {
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();

        let expected = sample_record(5678, vec![0xDE, 0xAD, 0xBE, 0xEF]);
        put_gasless_key_record(&db, &mut txn, 7, &KEY_A, &expected).unwrap();

        let got = get_gasless_key_record(&db, &txn, 7, &KEY_A)
            .unwrap()
            .expect("record should be present after put");
        assert_eq!(got.request_fid, expected.request_fid);
        assert_eq!(
            got.message.as_ref().unwrap().hash,
            expected.message.as_ref().unwrap().hash
        );
    }

    #[test]
    fn get_returns_none_for_missing_record() {
        let (db, _dir) = open_db();
        let txn = RocksDbTransactionBatch::new();
        assert!(get_gasless_key_record(&db, &txn, 7, &KEY_A)
            .unwrap()
            .is_none());
    }

    #[test]
    fn delete_of_missing_record_is_noop() {
        // RocksDB tolerates deletes of absent keys, and `merge_key_remove` relies on that:
        // the merge path never pre-checks record existence before deleting.
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();
        delete_gasless_key_record(&db, &mut txn, 7, &KEY_A).unwrap();
        assert!(get_gasless_key_record(&db, &txn, 7, &KEY_A)
            .unwrap()
            .is_none());
    }

    #[test]
    fn committed_record_persists_across_txns() {
        let (db, _dir) = open_db();

        let mut txn1 = RocksDbTransactionBatch::new();
        let expected = sample_record(9999, vec![0xCA, 0xFE]);
        put_gasless_key_record(&db, &mut txn1, 7, &KEY_A, &expected).unwrap();
        db.commit(txn1).unwrap();

        let txn2 = RocksDbTransactionBatch::new();
        let got = get_gasless_key_record(&db, &txn2, 7, &KEY_A)
            .unwrap()
            .unwrap();
        assert_eq!(got.request_fid, 9999);
    }

    #[test]
    fn records_are_scoped_per_fid_and_per_key() {
        // Isolation check: two different FIDs each holding the same public key, and one FID
        // holding two different keys, must not leak across index positions.
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();

        put_gasless_key_record(&db, &mut txn, 7, &KEY_A, &sample_record(100, vec![1])).unwrap();
        put_gasless_key_record(&db, &mut txn, 8, &KEY_A, &sample_record(200, vec![2])).unwrap();
        put_gasless_key_record(&db, &mut txn, 7, &KEY_B, &sample_record(300, vec![3])).unwrap();

        assert_eq!(
            get_gasless_key_record(&db, &txn, 7, &KEY_A)
                .unwrap()
                .unwrap()
                .request_fid,
            100
        );
        assert_eq!(
            get_gasless_key_record(&db, &txn, 8, &KEY_A)
                .unwrap()
                .unwrap()
                .request_fid,
            200
        );
        assert_eq!(
            get_gasless_key_record(&db, &txn, 7, &KEY_B)
                .unwrap()
                .unwrap()
                .request_fid,
            300
        );
    }

    // ---- by-public-key owner index ---------------------------------------------------------

    #[test]
    fn owner_key_layout_varies_only_by_public_key() {
        let ka = make_gasless_key_by_public_key_key(&KEY_A).unwrap();
        let kb = make_gasless_key_by_public_key_key(&KEY_B).unwrap();
        assert_ne!(
            ka, kb,
            "different public keys must map to different index keys"
        );
        // Stable on repeat — the key material is all we encode, no FID mixing.
        assert_eq!(ka, make_gasless_key_by_public_key_key(&KEY_A).unwrap());
        // Size matches the compile-time constant: 1 root prefix + 1 postfix + 32 key.
        assert_eq!(ka.len(), 1 + 1 + 32);
    }

    #[test]
    fn owner_make_key_rejects_wrong_length_public_key() {
        let err = make_gasless_key_by_public_key_key(&[0xAA; 31]).unwrap_err();
        assert_eq!(err.code, "bad_request.validation_failure");
    }

    #[test]
    fn owner_get_returns_none_for_unclaimed_key() {
        let (db, _dir) = open_db();
        let txn = RocksDbTransactionBatch::new();
        assert!(get_gasless_key_owner_fid(&db, &txn, &KEY_A)
            .unwrap()
            .is_none());
    }

    #[test]
    fn owner_roundtrips_fid() {
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();

        put_gasless_key_owner(&db, &mut txn, &KEY_A, 42).unwrap();
        assert_eq!(
            get_gasless_key_owner_fid(&db, &txn, &KEY_A).unwrap(),
            Some(42)
        );

        // Different key stays unclaimed.
        assert!(get_gasless_key_owner_fid(&db, &txn, &KEY_B)
            .unwrap()
            .is_none());
    }

    #[test]
    fn owner_sees_uncommitted_writes_in_same_txn() {
        // The global-uniqueness check in `merge_key_add` runs against a txn_batch that may
        // already hold a staged KEY_ADD from earlier in the same commit. The read must see
        // that staged owner entry, otherwise two KEY_ADDs for the same key in one commit
        // could both pass the check.
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();

        put_gasless_key_owner(&db, &mut txn, &KEY_A, 7).unwrap();
        assert_eq!(
            get_gasless_key_owner_fid(&db, &txn, &KEY_A).unwrap(),
            Some(7)
        );
    }

    #[test]
    fn owner_persists_across_txns() {
        let (db, _dir) = open_db();

        let mut txn1 = RocksDbTransactionBatch::new();
        put_gasless_key_owner(&db, &mut txn1, &KEY_A, 9999).unwrap();
        db.commit(txn1).unwrap();

        let txn2 = RocksDbTransactionBatch::new();
        assert_eq!(
            get_gasless_key_owner_fid(&db, &txn2, &KEY_A).unwrap(),
            Some(9999)
        );
    }

    #[test]
    fn owner_overwrite_replaces_fid() {
        // Not a path the merge layer uses (KEY_REMOVE always clears before a re-add), but the
        // store must behave as a last-writer-wins map — the merge layer relies on delete +
        // add being idempotent regardless of ordering within a batch.
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();

        put_gasless_key_owner(&db, &mut txn, &KEY_A, 1).unwrap();
        put_gasless_key_owner(&db, &mut txn, &KEY_A, 2).unwrap();
        assert_eq!(
            get_gasless_key_owner_fid(&db, &txn, &KEY_A).unwrap(),
            Some(2)
        );
    }

    #[test]
    fn owner_delete_clears_claim() {
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();

        put_gasless_key_owner(&db, &mut txn, &KEY_A, 7).unwrap();
        delete_gasless_key_owner(&db, &mut txn, &KEY_A).unwrap();
        assert!(get_gasless_key_owner_fid(&db, &txn, &KEY_A)
            .unwrap()
            .is_none());
    }

    #[test]
    fn owner_delete_of_missing_entry_is_noop() {
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();
        delete_gasless_key_owner(&db, &mut txn, &KEY_A).unwrap();
        assert!(get_gasless_key_owner_fid(&db, &txn, &KEY_A)
            .unwrap()
            .is_none());
    }

    #[test]
    fn owner_get_on_corrupt_value_returns_internal_error() {
        // The owner value is a fixed-width 4-byte FID encoding. Anything else (legacy migration,
        // disk corruption) surfaces as an internal_error rather than being silently reinterpreted.
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();
        let key = make_gasless_key_by_public_key_key(&KEY_A).unwrap();
        txn.put(key, vec![0x01, 0x02]); // wrong width

        let err = get_gasless_key_owner_fid(&db, &txn, &KEY_A).unwrap_err();
        assert_eq!(err.code, "internal_error");
    }

    // ---- per-FID gasless-key counter (NEYN-10579) ----------------------------------------

    #[test]
    fn count_is_zero_when_no_entry() {
        let (db, _dir) = open_db();
        let txn = RocksDbTransactionBatch::new();
        assert_eq!(get_gasless_key_count(&db, &txn, 7).unwrap(), 0);
    }

    #[test]
    fn increment_then_get_returns_one() {
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();

        increment_gasless_key_count(&db, &mut txn, 7).unwrap();
        assert_eq!(get_gasless_key_count(&db, &txn, 7).unwrap(), 1);
    }

    #[test]
    fn multiple_increments_accumulate() {
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();

        for _ in 0..5 {
            increment_gasless_key_count(&db, &mut txn, 7).unwrap();
        }
        assert_eq!(get_gasless_key_count(&db, &txn, 7).unwrap(), 5);
    }

    #[test]
    fn decrement_saturates_at_zero() {
        // Decrementing an unset counter must not underflow. `saturating_sub` keeps us at 0.
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();

        decrement_gasless_key_count(&db, &mut txn, 7).unwrap();
        assert_eq!(get_gasless_key_count(&db, &txn, 7).unwrap(), 0);
    }

    #[test]
    fn decrement_to_zero_deletes_entry() {
        // Sparse-index invariant: a zero count is represented by absence, not by an entry holding
        // four 0x00 bytes. This keeps `get_gasless_key_count` for never-touched FIDs on the
        // absent-entry fast path.
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();

        increment_gasless_key_count(&db, &mut txn, 7).unwrap();
        decrement_gasless_key_count(&db, &mut txn, 7).unwrap();

        // Verify the entry is truly absent rather than stored as zero: the raw index key must not
        // round-trip through get_from_db_or_txn to a Some(_).
        let raw_key = make_gasless_key_count_by_fid_key(7);
        assert!(
            crate::storage::store::account::get_from_db_or_txn(&db, &txn, &raw_key)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn count_is_scoped_per_fid() {
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();

        increment_gasless_key_count(&db, &mut txn, 7).unwrap();
        increment_gasless_key_count(&db, &mut txn, 7).unwrap();
        increment_gasless_key_count(&db, &mut txn, 8).unwrap();

        assert_eq!(get_gasless_key_count(&db, &txn, 7).unwrap(), 2);
        assert_eq!(get_gasless_key_count(&db, &txn, 8).unwrap(), 1);
        assert_eq!(get_gasless_key_count(&db, &txn, 9).unwrap(), 0);
    }

    #[test]
    fn count_visible_in_same_txn_batch() {
        // The cap check in `merge_key_add` reads the counter through the in-flight txn batch;
        // this test locks in that a just-staged increment is visible to a subsequent read in the
        // same batch without commit.
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();

        increment_gasless_key_count(&db, &mut txn, 42).unwrap();
        increment_gasless_key_count(&db, &mut txn, 42).unwrap();
        assert_eq!(get_gasless_key_count(&db, &txn, 42).unwrap(), 2);
    }

    #[test]
    fn count_persists_across_commit() {
        let (db, _dir) = open_db();

        let mut txn1 = RocksDbTransactionBatch::new();
        increment_gasless_key_count(&db, &mut txn1, 7).unwrap();
        increment_gasless_key_count(&db, &mut txn1, 7).unwrap();
        increment_gasless_key_count(&db, &mut txn1, 7).unwrap();
        db.commit(txn1).unwrap();

        let txn2 = RocksDbTransactionBatch::new();
        assert_eq!(get_gasless_key_count(&db, &txn2, 7).unwrap(), 3);
    }

    #[test]
    fn corrupt_count_value_returns_internal_error() {
        // The count value is a fixed-width 4-byte u32 encoding. Any other width (disk corruption,
        // botched migration) surfaces as internal_error rather than being silently reinterpreted.
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();
        let key = make_gasless_key_count_by_fid_key(7);
        txn.put(key, vec![0x01, 0x02, 0x03]); // wrong width

        let err = get_gasless_key_count(&db, &txn, 7).unwrap_err();
        assert_eq!(err.code, "internal_error");
    }

    #[test]
    fn get_on_corrupt_value_returns_internal_error() {
        // If someone writes non-protobuf bytes into the record slot (e.g., a botched migration),
        // decode fails and we surface an internal_error rather than silently accepting a
        // default-constructed record.
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();
        let key = make_gasless_key_by_fid_key(7, &KEY_A).unwrap();
        txn.put(key, vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);

        let err = get_gasless_key_record(&db, &txn, 7, &KEY_A).unwrap_err();
        assert_eq!(err.code, "internal_error");
    }

    // ---- list_gasless_keys_by_fid (NEYN-10578) -------------------------------------------

    fn write_record(db: &Arc<db::RocksDB>, fid: u64, key: &[u8; 32], request_fid: u64) {
        let mut txn = RocksDbTransactionBatch::new();
        put_gasless_key_record(db, &mut txn, fid, key, &sample_record(request_fid, vec![1]))
            .unwrap();
        db.commit(txn).unwrap();
    }

    #[test]
    fn list_returns_empty_for_fid_with_no_records() {
        let (db, _dir) = open_db();
        let page = list_gasless_keys_by_fid(&db, 7, &PageOptions::default()).unwrap();
        assert!(page.records.is_empty());
        assert!(page.next_page_token.is_none());
    }

    #[test]
    fn list_returns_records_only_for_requested_fid() {
        let (db, _dir) = open_db();
        write_record(&db, 7, &KEY_A, 100);
        write_record(&db, 7, &KEY_B, 200);
        write_record(&db, 8, &KEY_A, 300);

        let page = list_gasless_keys_by_fid(&db, 7, &PageOptions::default()).unwrap();
        assert_eq!(page.records.len(), 2);
        let request_fids: Vec<u64> = page.records.iter().map(|(_, r)| r.request_fid).collect();
        assert!(request_fids.contains(&100));
        assert!(request_fids.contains(&200));
        assert!(page.next_page_token.is_none());
    }

    #[test]
    fn list_paginates_with_resumable_cursor() {
        let (db, _dir) = open_db();
        write_record(&db, 7, &KEY_A, 100);
        write_record(&db, 7, &KEY_B, 200);

        let first = list_gasless_keys_by_fid(
            &db,
            7,
            &PageOptions {
                page_size: Some(1),
                page_token: None,
                reverse: false,
            },
        )
        .unwrap();
        assert_eq!(first.records.len(), 1);
        assert!(first.next_page_token.is_some());

        let second = list_gasless_keys_by_fid(
            &db,
            7,
            &PageOptions {
                page_size: Some(1),
                page_token: first.next_page_token.clone(),
                reverse: false,
            },
        )
        .unwrap();
        assert_eq!(second.records.len(), 1);
        // The two pages should expose distinct keys.
        assert_ne!(first.records[0].0, second.records[0].0);
    }

    #[test]
    fn list_surfaces_public_key_in_row_tuple() {
        let (db, _dir) = open_db();
        write_record(&db, 7, &KEY_A, 100);
        let page = list_gasless_keys_by_fid(&db, 7, &PageOptions::default()).unwrap();
        assert_eq!(page.records[0].0, KEY_A.to_vec());
    }
}
