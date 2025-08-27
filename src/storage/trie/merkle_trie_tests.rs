#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::storage::db::{RocksDB, RocksDbTransactionBatch};
    use crate::storage::store::account::IntoU8;
    use crate::storage::trie::merkle_trie::{Context, MerkleTrie, TrieKey};
    use crate::storage::trie::util::decode_trie_token;
    use crate::utils::factory::{events_factory, messages_factory};
    use rand::{thread_rng, Rng};

    fn random_hash() -> Vec<u8> {
        (0..32).map(|_| rand::random::<u8>()).collect()
    }

    #[test]
    fn test_reload_with_empty_root() {
        let ctx = &Context::new();

        let tmp_path = tempfile::tempdir()
            .unwrap()
            .path()
            .as_os_str()
            .to_string_lossy()
            .to_string();

        let db = &RocksDB::new(&tmp_path);
        db.open().unwrap();

        let mut trie = MerkleTrie::new(16).unwrap();
        trie.initialize(db).unwrap();

        let mut txn_batch = RocksDbTransactionBatch::new();
        let hash = random_hash();

        let res = trie
            .insert(ctx, db, &mut txn_batch, vec![&hash.clone()])
            .unwrap();
        assert_eq!(res, vec![true]);

        let res = trie.exists(ctx, db, &hash).unwrap();
        assert_eq!(res, true);

        let res = trie.reload(db);
        assert!(res.is_ok());

        // Does not exist after reload
        let res = trie.exists(ctx, db, &hash).unwrap();
        assert_eq!(res, false);
    }

    #[test]
    fn test_reload_with_existing_data() {
        let ctx = &Context::new();

        let tmp_path = tempfile::tempdir()
            .unwrap()
            .path()
            .as_os_str()
            .to_string_lossy()
            .to_string();

        let db = &RocksDB::new(&tmp_path);
        db.open().unwrap();

        let mut trie = MerkleTrie::new(16).unwrap();
        trie.initialize(db).unwrap();

        let mut first_txn = RocksDbTransactionBatch::new();
        let first_hash = random_hash();
        let second_hash = random_hash();

        trie.insert(ctx, db, &mut first_txn, vec![&first_hash.clone()])
            .unwrap();
        db.commit(first_txn).unwrap();
        trie.reload(db).unwrap();

        let res = trie.exists(ctx, db, &first_hash).unwrap();
        assert_eq!(res, true);

        let mut second_txn = RocksDbTransactionBatch::new();
        trie.insert(ctx, db, &mut second_txn, vec![&second_hash.clone()])
            .unwrap();

        trie.reload(db).unwrap();

        // First hash still exists, but not the second
        let res = trie.exists(ctx, db, &first_hash).unwrap();
        assert_eq!(res, true);
        let res = trie.exists(ctx, db, &second_hash).unwrap();
        assert_eq!(res, false);
    }

    #[test]
    fn test_trie_key() {
        let fid_key = TrieKey::for_fid(1234);
        assert_eq!(fid_key[0], 248); // Shard id for 1234
        assert_eq!(fid_key[1..5], (1234u32).to_be_bytes().to_vec());

        let message = messages_factory::casts::create_cast_add(1234, "test", None, None);
        let message_key = TrieKey::for_message(&message);
        assert_eq!(message_key[0], TrieKey::fid_shard(1234));
        assert_eq!(message_key[0..5], TrieKey::for_fid(1234));
        assert_eq!(message_key[5], message.msg_type().into_u8() << 3);
        assert_eq!(message_key[6..], message.hash);

        let delete_message =
            messages_factory::casts::create_cast_remove(321456, &message.hash, None, None);
        let delete_message_key = TrieKey::for_message(&delete_message);
        assert_eq!(delete_message_key[0], TrieKey::fid_shard(321456));
        assert_eq!(delete_message_key[0..5], TrieKey::for_fid(321456));
        assert_eq!(
            delete_message_key[5],
            delete_message.msg_type().into_u8() << 3
        );
        assert_eq!(delete_message_key[6..], delete_message.hash);

        let event = events_factory::create_onchain_event(1234);
        let event_key = TrieKey::for_onchain_event(&event);
        assert_eq!(event_key[0], TrieKey::fid_shard(1234));
        assert_eq!(event_key[0..5], TrieKey::for_fid(1234));
        assert_eq!(event_key[5], event.r#type as u8);
        assert_eq!(
            event_key[6..(6 + event.transaction_hash.len())],
            event.transaction_hash
        );
        assert_eq!(
            event_key[(6 + event.transaction_hash.len())..],
            event.log_index.to_be_bytes()
        );

        let username = "longishusername";
        let event_key = TrieKey::for_fname(1234, &username.to_string());
        assert_eq!(event_key[0], TrieKey::fid_shard(1234));
        assert_eq!(event_key[0..5], TrieKey::for_fid(1234));
        assert_eq!(event_key[5], 7);
        // Username is padded to length 20
        assert_eq!(
            event_key[6..],
            format!("{}{}", username, "\0\0\0\0\0")
                .bytes()
                .collect::<Vec<_>>()
        );
    }

    fn collect_all_paged_values_from_trie(
        trie: &mut MerkleTrie,
        ctx: &Context,
        db: &RocksDB,
        prefix: &[u8],
        page_size: usize,
    ) -> Vec<Vec<u8>> {
        let mut collected: Vec<Vec<u8>> = vec![];
        let mut page_token: Option<String> = None;
        loop {
            let mut page = vec![];
            let next = trie
                .get_paged_values_of_subtree(
                    ctx,
                    db,
                    prefix,
                    &mut page,
                    page_size,
                    page_token.clone(),
                )
                .unwrap();
            assert!(page.len() <= page_size);
            collected.extend(page);
            if next.is_none() {
                break;
            }
            page_token = next;
            // ensure token decodes
            let _ = decode_trie_token(page_token.as_ref().unwrap()).unwrap();
        }
        collected
    }

    #[test]
    fn test_get_paged_values_of_subtree_pagination() {
        let ctx = &Context::new();
        let tmp_path = tempfile::tempdir().unwrap();
        let db = &RocksDB::new(tmp_path.path().to_str().unwrap());
        db.open().unwrap();
        let mut trie = MerkleTrie::new(16).unwrap();
        trie.initialize(db).unwrap();
        let mut txn_batch = RocksDbTransactionBatch::new();

        // Collecting on an empty trie should work
        let collected = collect_all_paged_values_from_trie(&mut trie, ctx, db, &[], 3);
        assert_eq!(collected.len(), 0);

        // Insert 10 keys sharing same prefix '[0,1]'
        for i in 0u8..10u8 {
            // limited range
            let key = vec![0, 1, 2 + i, 3, 4, 5, 6, 7, 8, 9, 10, 11];
            trie.insert(ctx, db, &mut txn_batch, vec![&key]).unwrap();
        }

        let collected = collect_all_paged_values_from_trie(&mut trie, ctx, db, &[0, 1], 3);
        assert_eq!(collected.len(), 10);

        // Now try collecting [0,1,2], which should yield only 1 item.
        let collected = collect_all_paged_values_from_trie(&mut trie, ctx, db, &[0, 1, 2], 3);
        assert_eq!(collected.len(), 1);

        // Insert 10 more keys, under [0,2]
        for i in 0u8..10u8 {
            let key = vec![0, 2, 2 + i, 3, 4, 5, 6, 7, 8, 9, 10, 11];
            trie.insert(ctx, db, &mut txn_batch, vec![&key]).unwrap();
        }

        // Getting the keys for [0,1] and [0,2] should each yield 10 items
        let prefixes_to_test = [[0, 1], [0, 2]];
        for prefix_to_test in prefixes_to_test {
            let collected =
                collect_all_paged_values_from_trie(&mut trie, ctx, db, &prefix_to_test, 3);
            assert_eq!(collected.len(), 10);
        }

        // Collecting with a large page size should return all the values too
        for prefix_to_test in prefixes_to_test {
            let collected =
                collect_all_paged_values_from_trie(&mut trie, ctx, db, &prefix_to_test, 100);
            assert_eq!(collected.len(), 10);
        }

        // Collecting from a non-existant prefix should be empty
        let collected = collect_all_paged_values_from_trie(&mut trie, ctx, db, &[0, 3], 3);
        assert_eq!(collected.len(), 0);
    }

    #[test]
    fn test_get_paged_values_random() {
        let ctx = &Context::new();
        let tmp_path = tempfile::tempdir().unwrap();
        let db = &RocksDB::new(tmp_path.path().to_str().unwrap());
        db.open().unwrap();
        let mut trie = MerkleTrie::new(16).unwrap();
        trie.initialize(db).unwrap();
        let mut txn_batch = RocksDbTransactionBatch::new();

        // Generate 1000 random keys of random length between 6 and 20 bytes
        let mut rng = thread_rng();
        let mut original_keys = HashSet::with_capacity(1000);
        for _ in 0..1000 {
            let len = rng.gen_range(7..=20);
            let key: Vec<u8> = (0..len).map(|_| rng.gen()).collect();

            if original_keys.contains(&key) {
                continue;
            }

            trie.insert(ctx, db, &mut txn_batch, vec![&key]).unwrap();
            original_keys.insert(key);
        }

        // Collect all keys with page size 100
        let collected = collect_all_paged_values_from_trie(&mut trie, ctx, db, &[], 100);
        assert_eq!(collected.len(), 1000);

        // Make sure each collected key is present in the original set
        for key in &collected {
            assert!(
                original_keys.contains(key),
                "Collected key not found in original keys"
            );
        }

        // Make sure each original_key was collected
        for key in &original_keys {
            assert!(
                collected.contains(key),
                "Original key not found in collected keys"
            );
        }
    }
}
