#[cfg(test)]
mod tests {
    use crate::proto;
    use crate::storage::db;
    use crate::storage::db::RocksDbTransactionBatch;
    use crate::storage::store::account::{
        delete_gasless_key_record, exists_gasless_key, get_gasless_key_record,
        make_gasless_key_by_fid_key, put_gasless_key_record, GaslessKeyRecord,
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
    fn exists_reflects_put_and_delete() {
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();

        assert!(!exists_gasless_key(&db, &txn, 7, &KEY_A).unwrap());

        put_gasless_key_record(&db, &mut txn, 7, &KEY_A, &sample_record(1, vec![1])).unwrap();
        assert!(exists_gasless_key(&db, &txn, 7, &KEY_A).unwrap());

        delete_gasless_key_record(&db, &mut txn, 7, &KEY_A).unwrap();
        assert!(!exists_gasless_key(&db, &txn, 7, &KEY_A).unwrap());
    }

    #[test]
    fn delete_of_missing_record_is_noop() {
        // Matches RocksDB's tolerance for deleting nonexistent keys — engine KEY_REMOVE path
        // shouldn't have to pre-check existence (conflict-resolution already owns that).
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();
        delete_gasless_key_record(&db, &mut txn, 7, &KEY_A).unwrap();
        assert!(!exists_gasless_key(&db, &txn, 7, &KEY_A).unwrap());
    }

    #[test]
    fn exists_sees_uncommitted_writes_in_same_txn() {
        // The conflict check in `merge_key_add` calls `exists_gasless_key` on the same txn_batch
        // that a prior KEY_ADD in the same shard commit might have staged — it must see that
        // staged write, otherwise two KEY_ADDs in one commit could both pass conflict resolution.
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();

        put_gasless_key_record(&db, &mut txn, 7, &KEY_A, &sample_record(1, vec![1])).unwrap();
        assert!(exists_gasless_key(&db, &txn, 7, &KEY_A).unwrap());
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
}
