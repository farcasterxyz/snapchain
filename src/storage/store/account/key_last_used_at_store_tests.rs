#[cfg(test)]
mod tests {
    use crate::storage::db;
    use crate::storage::db::RocksDbTransactionBatch;
    use crate::storage::store::account::{
        check_and_bump_last_used_at, delete_last_used_at, get_last_used_at, init_last_used_at,
        make_last_used_at_key,
    };
    use std::sync::Arc;
    use tempfile::TempDir;

    fn open_db() -> (Arc<db::RocksDB>, TempDir) {
        let dir = tempfile::TempDir::new().unwrap();
        let db_path = dir.path().join("last_used_at.db");
        let db = db::RocksDB::new(db_path.to_str().unwrap());
        db.open().unwrap();
        (Arc::new(db), dir)
    }

    // Two 32-byte keys we can use for cross-key isolation tests.
    const KEY_A: [u8; 32] = [0xAA; 32];
    const KEY_B: [u8; 32] = [0xBB; 32];

    #[test]
    fn key_layout_varies_by_fid_and_public_key() {
        let ka_fid7 = make_last_used_at_key(7, &KEY_A).unwrap();
        let ka_fid8 = make_last_used_at_key(8, &KEY_A).unwrap();
        let kb_fid7 = make_last_used_at_key(7, &KEY_B).unwrap();
        assert_ne!(ka_fid7, ka_fid8, "same key, different fid must differ");
        assert_ne!(ka_fid7, kb_fid7, "same fid, different key must differ");
    }

    #[test]
    fn make_key_rejects_wrong_length_public_key() {
        let err = make_last_used_at_key(7, &[0xAA; 31]).unwrap_err();
        assert_eq!(err.code, "bad_request.validation_failure");
    }

    #[test]
    fn init_writes_timestamp_and_get_reads_it_back() {
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();

        assert_eq!(get_last_used_at(&db, &txn, 7, &KEY_A).unwrap(), None);

        init_last_used_at(&db, &mut txn, 7, &KEY_A, 1_000).unwrap();
        assert_eq!(get_last_used_at(&db, &txn, 7, &KEY_A).unwrap(), Some(1_000));
    }

    #[test]
    fn init_is_unconditional_overwrite() {
        // KEY_ADD should always establish a fresh baseline. If the engine permits a re-add after
        // a KEY_REMOVE (which first deletes this entry), a stale value must not survive; and more
        // defensively, init writes unconditionally regardless of any pre-existing entry.
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();

        init_last_used_at(&db, &mut txn, 7, &KEY_A, 500).unwrap();
        init_last_used_at(&db, &mut txn, 7, &KEY_A, 200).unwrap();
        assert_eq!(get_last_used_at(&db, &txn, 7, &KEY_A).unwrap(), Some(200));
    }

    #[test]
    fn delete_removes_entry() {
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();

        init_last_used_at(&db, &mut txn, 7, &KEY_A, 1_000).unwrap();
        delete_last_used_at(&db, &mut txn, 7, &KEY_A).unwrap();

        assert_eq!(get_last_used_at(&db, &txn, 7, &KEY_A).unwrap(), None);
    }

    #[test]
    fn delete_of_missing_entry_is_noop() {
        // Mirrors RocksDB's tolerance for deleting nonexistent keys; callers (engine KEY_REMOVE
        // path) shouldn't have to pre-check existence.
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();
        delete_last_used_at(&db, &mut txn, 7, &KEY_A).unwrap();
        assert_eq!(get_last_used_at(&db, &txn, 7, &KEY_A).unwrap(), None);
    }

    #[test]
    fn committed_value_persists_across_txns() {
        let (db, _dir) = open_db();

        let mut txn1 = RocksDbTransactionBatch::new();
        init_last_used_at(&db, &mut txn1, 7, &KEY_A, 1_234).unwrap();
        db.commit(txn1).unwrap();

        let txn2 = RocksDbTransactionBatch::new();
        assert_eq!(
            get_last_used_at(&db, &txn2, 7, &KEY_A).unwrap(),
            Some(1_234)
        );
    }

    // -- check_and_bump expiry/bump behavior ----------------------------------------------------
    //
    // These tests pin the contract the `TODO(human)` block implements. Keep them as-is; they
    // express the FIP semantics (`last_used_at + ttl >= current_block_timestamp`) and the bump
    // side-effect.

    #[test]
    fn check_and_bump_missing_entry_is_internal_error() {
        // A validation call before KEY_ADD initialized the counter is a programming error, not a
        // user-facing rejection — it must not silently accept the message.
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();
        let err =
            check_and_bump_last_used_at(&db, &mut txn, 7, &KEY_A, 3_600, 1_000, 1_500).unwrap_err();
        assert_eq!(err.code, "internal_error");
    }

    #[test]
    fn check_and_bump_accepts_inside_ttl_window_and_bumps_timestamp() {
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();
        init_last_used_at(&db, &mut txn, 7, &KEY_A, 1_000).unwrap();

        // stored=1000, ttl=3600, now=2000 → 1000+3600=4600 >= 2000, accept.
        check_and_bump_last_used_at(&db, &mut txn, 7, &KEY_A, 3_600, 1_800, 2_000).unwrap();
        assert_eq!(get_last_used_at(&db, &txn, 7, &KEY_A).unwrap(), Some(1_800));
    }

    #[test]
    fn check_and_bump_accepts_at_exact_expiry_boundary() {
        // `last_used_at + ttl >= current_block_timestamp` includes equality: stored + ttl == now
        // must still accept (not yet expired).
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();
        init_last_used_at(&db, &mut txn, 7, &KEY_A, 1_000).unwrap();

        check_and_bump_last_used_at(&db, &mut txn, 7, &KEY_A, 500, 1_400, 1_500).unwrap();
        assert_eq!(get_last_used_at(&db, &txn, 7, &KEY_A).unwrap(), Some(1_400));
    }

    #[test]
    fn check_and_bump_rejects_once_expired_and_does_not_bump() {
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();
        init_last_used_at(&db, &mut txn, 7, &KEY_A, 1_000).unwrap();

        // stored=1000, ttl=500, now=1501 → 1000+500=1500 < 1501, reject.
        let err =
            check_and_bump_last_used_at(&db, &mut txn, 7, &KEY_A, 500, 1_501, 1_501).unwrap_err();
        assert_eq!(err.code, "bad_request.validation_failure");

        // Rejection must leave the stored value untouched.
        assert_eq!(get_last_used_at(&db, &txn, 7, &KEY_A).unwrap(), Some(1_000));
    }

    // -- sliding-TTL throttle (NEYN-10579) ------------------------------------------------------
    //
    // The bump is suppressed when it would move `stored` forward by at most
    // SLIDING_TTL_THROTTLE_SECONDS. These tests pin the contract: acceptance still happens (no
    // error), but the stored timestamp is left unchanged.

    #[test]
    fn check_and_bump_within_throttle_window_does_not_write() {
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();
        init_last_used_at(&db, &mut txn, 7, &KEY_A, 1_000).unwrap();

        // stored=1000, message_ts=1100 → delta=100 < 300 throttle window → acceptance w/o write.
        check_and_bump_last_used_at(&db, &mut txn, 7, &KEY_A, 3_600, 1_100, 1_100).unwrap();
        assert_eq!(get_last_used_at(&db, &txn, 7, &KEY_A).unwrap(), Some(1_000));
    }

    #[test]
    fn check_and_bump_at_exact_throttle_boundary_does_not_write() {
        // delta == 300 is at-the-boundary-and-does-not-write (strict `>` on "past window").
        // Rationale: strict `>` makes the non-increasing-timestamp guard fall out for free — see
        // the comment on the predicate in `check_and_bump_last_used_at`.
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();
        init_last_used_at(&db, &mut txn, 7, &KEY_A, 1_000).unwrap();

        check_and_bump_last_used_at(&db, &mut txn, 7, &KEY_A, 3_600, 1_300, 1_300).unwrap();
        assert_eq!(get_last_used_at(&db, &txn, 7, &KEY_A).unwrap(), Some(1_000));
    }

    #[test]
    fn check_and_bump_one_past_throttle_boundary_writes() {
        // delta == 301 is the smallest delta that writes.
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();
        init_last_used_at(&db, &mut txn, 7, &KEY_A, 1_000).unwrap();

        check_and_bump_last_used_at(&db, &mut txn, 7, &KEY_A, 3_600, 1_301, 1_301).unwrap();
        assert_eq!(get_last_used_at(&db, &txn, 7, &KEY_A).unwrap(), Some(1_301));
    }

    #[test]
    fn check_and_bump_past_throttle_window_writes() {
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();
        init_last_used_at(&db, &mut txn, 7, &KEY_A, 1_000).unwrap();

        // delta=500 > 300, normal bump.
        check_and_bump_last_used_at(&db, &mut txn, 7, &KEY_A, 3_600, 1_500, 1_500).unwrap();
        assert_eq!(get_last_used_at(&db, &txn, 7, &KEY_A).unwrap(), Some(1_500));
    }

    #[test]
    fn check_and_bump_non_increasing_timestamp_does_not_write() {
        // Out-of-order message_ts (< stored) must never overwrite a higher stored value, even
        // when the key is still inside its TTL window.
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();
        init_last_used_at(&db, &mut txn, 7, &KEY_A, 2_000).unwrap();

        check_and_bump_last_used_at(&db, &mut txn, 7, &KEY_A, 3_600, 1_500, 2_000).unwrap();
        assert_eq!(get_last_used_at(&db, &txn, 7, &KEY_A).unwrap(), Some(2_000));
    }

    #[test]
    fn check_and_bump_equal_timestamp_does_not_write() {
        // message_ts == stored is trivially not-a-bump; also falls on the strict `>` guard.
        let (db, _dir) = open_db();
        let mut txn = RocksDbTransactionBatch::new();
        init_last_used_at(&db, &mut txn, 7, &KEY_A, 2_000).unwrap();

        check_and_bump_last_used_at(&db, &mut txn, 7, &KEY_A, 3_600, 2_000, 2_000).unwrap();
        assert_eq!(get_last_used_at(&db, &txn, 7, &KEY_A).unwrap(), Some(2_000));
    }
}
