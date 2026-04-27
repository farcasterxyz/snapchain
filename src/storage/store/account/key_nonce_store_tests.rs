#[cfg(test)]
mod tests {
    use crate::storage::db;
    use crate::storage::db::RocksDbTransactionBatch;
    use crate::storage::store::account::{
        check_and_set_app_nonce, check_and_set_user_nonce, get_app_nonce, get_user_nonce,
        make_app_nonce_key, make_user_nonce_key,
    };
    use std::sync::Arc;
    use tempfile::TempDir;

    fn open_db() -> (Arc<db::RocksDB>, TempDir) {
        let dir = tempfile::TempDir::new().unwrap();
        let db_path = dir.path().join("nonce.db");
        let db = db::RocksDB::new(db_path.to_str().unwrap());
        db.open().unwrap();
        (Arc::new(db), dir)
    }

    #[test]
    fn user_and_app_nonce_keys_are_distinct() {
        // The two counter namespaces share the same FID but different UserPostfix bytes, so the
        // keys must differ — otherwise a user-nonce update would corrupt the app counter.
        assert_ne!(make_user_nonce_key(42), make_app_nonce_key(42));
    }

    #[test]
    fn user_nonce_starts_unset_and_accepts_first_positive_value() {
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();

        assert_eq!(get_user_nonce(&db, &txn, 7).unwrap(), None);

        check_and_set_user_nonce(&db, &mut txn, 7, 1).unwrap();
        assert_eq!(get_user_nonce(&db, &txn, 7).unwrap(), Some(1));
    }

    #[test]
    fn user_nonce_rejects_zero_when_unset() {
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();

        // Missing key is treated as stored=0, so new_nonce=0 fails (0 > 0 is false).
        let err = check_and_set_user_nonce(&db, &mut txn, 7, 0).unwrap_err();
        assert_eq!(err.code, "bad_request.conflict");
    }

    #[test]
    fn user_nonce_rejects_equal_and_lower_values() {
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();

        check_and_set_user_nonce(&db, &mut txn, 7, 5).unwrap();

        let err = check_and_set_user_nonce(&db, &mut txn, 7, 5).unwrap_err();
        assert_eq!(err.code, "bad_request.conflict");

        let err = check_and_set_user_nonce(&db, &mut txn, 7, 4).unwrap_err();
        assert_eq!(err.code, "bad_request.conflict");

        assert_eq!(get_user_nonce(&db, &txn, 7).unwrap(), Some(5));
    }

    #[test]
    fn user_nonce_accepts_strictly_greater_values() {
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();

        check_and_set_user_nonce(&db, &mut txn, 7, 1).unwrap();
        check_and_set_user_nonce(&db, &mut txn, 7, 2).unwrap();
        check_and_set_user_nonce(&db, &mut txn, 7, 100).unwrap();

        assert_eq!(get_user_nonce(&db, &txn, 7).unwrap(), Some(100));
    }

    #[test]
    fn user_nonce_is_scoped_per_fid() {
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();

        check_and_set_user_nonce(&db, &mut txn, 7, 10).unwrap();

        // A different FID sees no stored value and accepts nonce=1.
        check_and_set_user_nonce(&db, &mut txn, 8, 1).unwrap();
        assert_eq!(get_user_nonce(&db, &txn, 8).unwrap(), Some(1));
        assert_eq!(get_user_nonce(&db, &txn, 7).unwrap(), Some(10));
    }

    #[test]
    fn user_and_app_counters_are_independent() {
        // Updates to the user counter must not affect the app counter for the same FID — they
        // live under different postfix bytes.
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();

        check_and_set_user_nonce(&db, &mut txn, 42, 50).unwrap();
        check_and_set_app_nonce(&db, &mut txn, 42, 1).unwrap();

        assert_eq!(get_user_nonce(&db, &txn, 42).unwrap(), Some(50));
        assert_eq!(get_app_nonce(&db, &txn, 42).unwrap(), Some(1));

        // The app counter should still reject values <= 1 even though the user counter is at 50.
        let err = check_and_set_app_nonce(&db, &mut txn, 42, 1).unwrap_err();
        assert_eq!(err.code, "bad_request.conflict");
    }

    // Reading from the same transaction that wrote must observe the staged value — this is what
    // `get_from_db_or_txn` buys us. Without it, two messages merged in the same txn would both
    // see the pre-txn value and both pass the check.
    #[test]
    fn nonce_reads_see_uncommitted_writes_in_same_txn() {
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();

        check_and_set_user_nonce(&db, &mut txn, 7, 5).unwrap();
        // Same-batch read must see 5, not None.
        assert_eq!(get_user_nonce(&db, &txn, 7).unwrap(), Some(5));

        // And a follow-up CAS in the same batch must reject <=5.
        let err = check_and_set_user_nonce(&db, &mut txn, 7, 5).unwrap_err();
        assert_eq!(err.code, "bad_request.conflict");
    }

    #[test]
    fn committed_nonce_persists_across_txns() {
        let (db, _dir) = open_db();

        let mut txn1 = RocksDbTransactionBatch::new();
        check_and_set_user_nonce(&db, &mut txn1, 7, 9).unwrap();
        db.commit(txn1).unwrap();

        let txn2 = RocksDbTransactionBatch::new();
        assert_eq!(get_user_nonce(&db, &txn2, 7).unwrap(), Some(9));
    }
}
