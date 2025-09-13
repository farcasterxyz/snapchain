#[cfg(test)]
mod tests {
    use crate::storage::db::{RocksDB, RocksDbTransactionBatch};
    use crate::storage::store::account::IntoU8;
    use crate::storage::trie::merkle_trie::{Context, MerkleTrie, TrieKey};
    use crate::storage::trie::util::decode_trie_page_token;
    use crate::utils::factory::{events_factory, messages_factory};
    use rand::{seq::SliceRandom, thread_rng, Rng as _};
    use std::collections::HashSet;

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

        let mut trie = MerkleTrie::new().unwrap();
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

        let mut trie = MerkleTrie::new().unwrap();
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
        let message_key = TrieKey::for_message(&message)[0].clone();
        assert_eq!(message_key[0], TrieKey::fid_shard(1234));
        assert_eq!(message_key[0..5], TrieKey::for_fid(1234));
        assert_eq!(message_key[5], message.msg_type().into_u8() << 3);
        assert_eq!(message_key[6..], message.hash);

        let delete_message =
            messages_factory::casts::create_cast_remove(321456, &message.hash, None, None);
        let delete_message_key = TrieKey::for_message(&delete_message)[0].clone();
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
            let _ = decode_trie_page_token(page_token.as_ref().unwrap()).unwrap();
        }
        collected
    }

    #[test]
    fn test_get_paged_values_of_subtree_pagination() {
        let ctx = &Context::new();
        let tmp_path = tempfile::tempdir().unwrap();
        let db = &RocksDB::new(tmp_path.path().to_str().unwrap());
        db.open().unwrap();

        let mut trie = MerkleTrie::new().unwrap();
        trie.initialize(db).unwrap();

        let mut all_keys = vec![];
        // Make 10 keys with the prefix [1,2] like [1,2,0,0,0,0,0,1]
        for i in 0..10 {
            let key = vec![1, 2, 0, 0, 0, 0, 0, i];
            all_keys.push(key);
        }

        // 10 more keys with the prefix [1,3]
        for i in 0..10 {
            let key = vec![1, 3, 0, 0, 0, 0, 0, i];
            all_keys.push(key);
        }

        // And then 5 more individual keys like [1,4] [1,5]...
        for i in 0..5 {
            let key = vec![1, 4 + i, 0, 0, 0, 0, 0, 0];
            all_keys.push(key);
        }

        // Insert all keys
        let mut txn_batch = RocksDbTransactionBatch::new();
        trie.insert(
            ctx,
            db,
            &mut txn_batch,
            all_keys.iter().map(|k| k.as_slice()).collect(),
        )
        .unwrap();
        db.commit(txn_batch).unwrap();

        // Get all keys with prefix [1,2] should yield all 10
        let collected = collect_all_paged_values_from_trie(&mut trie, ctx, db, &[1, 2], 100);
        assert_eq!(collected.len(), 10);
        // assert that they are the same as the first 10 keys
        for (i, key) in collected.iter().enumerate() {
            assert_eq!(key, &all_keys[i]);
        }

        // Collecting them with page size 1 should also work
        let collected = collect_all_paged_values_from_trie(&mut trie, ctx, db, &[1, 2], 1);
        assert_eq!(collected.len(), 10);

        // with page size 10 should also work
        let collected = collect_all_paged_values_from_trie(&mut trie, ctx, db, &[1, 2], 10);
        assert_eq!(collected.len(), 10);

        // Collecting [1,3] should also yield 10 keys
        let collected = collect_all_paged_values_from_trie(&mut trie, ctx, db, &[1, 3], 4);
        assert_eq!(collected.len(), 10);

        // Collecting [1,4] should also yield 1 key
        let collected = collect_all_paged_values_from_trie(&mut trie, ctx, db, &[1, 4], 4);
        assert_eq!(collected.len(), 1);

        // Collecting [1] should yield all keys
        let collected = collect_all_paged_values_from_trie(&mut trie, ctx, db, &[1], 4);
        assert_eq!(collected.len(), all_keys.len());

        // Collecting [0] should yield no keys
        let collected = collect_all_paged_values_from_trie(&mut trie, ctx, db, &[0], 4);
        assert_eq!(collected.len(), 0);
    }

    #[test]
    fn test_get_paged_values_random() {
        let ctx = &Context::new();
        let tmp_path = tempfile::tempdir().unwrap();
        let db = &RocksDB::new(tmp_path.path().to_str().unwrap());
        db.open().unwrap();
        let mut trie = MerkleTrie::new().unwrap();
        trie.initialize(db).unwrap();
        let mut txn_batch = RocksDbTransactionBatch::new();

        // Generate 1000 random keys of random length between 6 and 20 bytes
        let mut rng = thread_rng();
        let mut original_keys = HashSet::with_capacity(1000);
        for _ in 0..1000 {
            let len = rng.gen_range(7..=20);
            let key: Vec<u8> = (0..len).map(|_| rng.gen::<u8>()).collect();

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

        // Collect with page size 1
        let collected = collect_all_paged_values_from_trie(&mut trie, ctx, db, &[], 1);
        assert_eq!(collected.len(), 1000);

        // with page size 99
        let collected = collect_all_paged_values_from_trie(&mut trie, ctx, db, &[], 99);
        assert_eq!(collected.len(), 1000);

        // with page size 1000
        let collected = collect_all_paged_values_from_trie(&mut trie, ctx, db, &[], 1000);
        assert_eq!(collected.len(), 1000);

        // with page size 1001
        let collected = collect_all_paged_values_from_trie(&mut trie, ctx, db, &[], 1001);
        assert_eq!(collected.len(), 1000);
    }

    #[test]
    fn test_bulk_insert_matches_serial_insert() {
        let ctx = &Context::new();

        let tmp_path1 = tempfile::tempdir().unwrap();
        let db1 = &RocksDB::new(tmp_path1.path().to_str().unwrap());
        db1.open().unwrap();

        let tmp_path2 = tempfile::tempdir().unwrap();
        let db2 = &RocksDB::new(tmp_path2.path().to_str().unwrap());
        db2.open().unwrap();

        let mut trie1 = MerkleTrie::new().unwrap();
        trie1.initialize(db1).unwrap();

        let mut trie2 = MerkleTrie::new().unwrap();
        trie2.initialize(db2).unwrap();

        // Generate 100 random keys of length 20
        let key_vecs: Vec<Vec<u8>> = (0..100)
            .map(|_| (0..20).map(|_| rand::random::<u8>()).collect())
            .collect();
        let keys: Vec<&[u8]> = key_vecs.iter().map(|v| v.as_slice()).collect();

        // Insert one by one into trie1
        for key in &keys {
            let mut txn_batch = RocksDbTransactionBatch::new();
            trie1.insert(ctx, db1, &mut txn_batch, vec![key]).unwrap();
            db1.commit(txn_batch).unwrap();
        }
        let root_hash1 = trie1.root_hash().unwrap();

        // Bulk insert into trie2
        let mut txn_batch = RocksDbTransactionBatch::new();
        trie2
            .insert(ctx, db2, &mut txn_batch, keys.clone())
            .unwrap();
        db2.commit(txn_batch).unwrap();
        let root_hash2 = trie2.root_hash().unwrap();
        assert_eq!(root_hash1, root_hash2);

        // Adding the same 100 keys again should result in no-op for the batch add
        let mut txn_batch = RocksDbTransactionBatch::new();
        let r = trie2.insert(ctx, db2, &mut txn_batch, keys.clone());
        db2.commit(txn_batch).unwrap();

        assert!(r.is_ok());
        let root_hash2 = trie2.root_hash().unwrap();
        assert_eq!(root_hash1, root_hash2);

        trie1.reload(db1).unwrap();
        trie2.reload(db2).unwrap();

        // Create a second set of 100 keys
        let key_vecs2: Vec<Vec<u8>> = (0..100)
            .map(|_| (0..20).map(|_| rand::random::<u8>()).collect())
            .collect();

        // Add them serially
        let mut txn_batch = RocksDbTransactionBatch::new();
        for key in &key_vecs2 {
            trie1
                .insert(ctx, db1, &mut txn_batch, vec![key.as_slice()])
                .unwrap();
        }
        db1.commit(txn_batch).unwrap();

        // Bulk add to trie 2
        let mut txn_batch = RocksDbTransactionBatch::new();
        trie2
            .insert(
                ctx,
                db2,
                &mut txn_batch,
                key_vecs2.iter().map(|v| v.as_slice()).collect(),
            )
            .unwrap();
        db2.commit(txn_batch).unwrap();

        // Root hashes should match again
        assert_eq!(trie1.root_hash().unwrap(), trie2.root_hash().unwrap());
    }

    #[test]
    fn test_bulk_insert_with_duplicates() {
        let ctx = &Context::new();

        let tmp_path1 = tempfile::tempdir().unwrap();
        let db1 = &RocksDB::new(tmp_path1.path().to_str().unwrap());
        db1.open().unwrap();

        let tmp_path2 = tempfile::tempdir().unwrap();
        let db2 = &RocksDB::new(tmp_path2.path().to_str().unwrap());
        db2.open().unwrap();

        let tmp_path3 = tempfile::tempdir().unwrap();
        let db3 = &RocksDB::new(tmp_path3.path().to_str().unwrap());
        db3.open().unwrap();

        let mut trie1 = MerkleTrie::new().unwrap();
        trie1.initialize(db1).unwrap();

        let mut trie2 = MerkleTrie::new().unwrap();
        trie2.initialize(db2).unwrap();

        let mut trie3 = MerkleTrie::new().unwrap();
        trie3.initialize(db3).unwrap();

        // Generate 3 random keys
        let key_vecs: Vec<Vec<u8>> = (0..3)
            .map(|_| (0..20).map(|_| rand::random::<u8>()).collect())
            .collect();

        // Create keys with duplicates: the 3 keys twice
        let mut keys: Vec<&[u8]> = vec![];
        for key in &key_vecs {
            keys.push(key.as_slice());
        }
        for key in &key_vecs {
            keys.push(key.as_slice());
        }

        // Bulk insert into trie1
        let mut txn_batch = RocksDbTransactionBatch::new();
        trie1
            .insert(ctx, db1, &mut txn_batch, keys.clone())
            .unwrap();
        db1.commit(txn_batch).unwrap();
        let root_hash1 = trie1.root_hash().unwrap();

        // Serial insert into trie2
        for key in &keys {
            let mut txn_batch = RocksDbTransactionBatch::new();
            trie2.insert(ctx, db2, &mut txn_batch, vec![*key]).unwrap();
            db2.commit(txn_batch).unwrap();
        }
        let root_hash2 = trie2.root_hash().unwrap();
        assert_eq!(root_hash1, root_hash2);

        // Bulk insert unique keys into trie3, then duplicate keys
        let keys_unique: Vec<&[u8]> = key_vecs.iter().map(|v| v.as_slice()).collect();

        // First bulk insert with unique keys
        let mut txn_batch = RocksDbTransactionBatch::new();
        trie3
            .insert(ctx, db3, &mut txn_batch, keys_unique.clone())
            .unwrap();
        db3.commit(txn_batch).unwrap();

        // Second bulk insert with the same keys (duplicates)
        let mut txn_batch = RocksDbTransactionBatch::new();
        trie3
            .insert(ctx, db3, &mut txn_batch, keys_unique.clone())
            .unwrap();
        db3.commit(txn_batch).unwrap();

        let root_hash3 = trie3.root_hash().unwrap();
        assert_eq!(root_hash1, root_hash3);
        assert_eq!(root_hash2, root_hash3);
    }

    #[test]
    fn test_bulk_insert_with_partial_duplicates() {
        let ctx = &Context::new();

        // Create 4 temporary databases
        let tmp_path1 = tempfile::tempdir().unwrap();
        let db1 = &RocksDB::new(tmp_path1.path().to_str().unwrap());
        db1.open().unwrap();

        let tmp_path2 = tempfile::tempdir().unwrap();
        let db2 = &RocksDB::new(tmp_path2.path().to_str().unwrap());
        db2.open().unwrap();

        let tmp_path3 = tempfile::tempdir().unwrap();
        let db3 = &RocksDB::new(tmp_path3.path().to_str().unwrap());
        db3.open().unwrap();

        let tmp_path4 = tempfile::tempdir().unwrap();
        let db4 = &RocksDB::new(tmp_path4.path().to_str().unwrap());
        db4.open().unwrap();

        let tmp_path5 = tempfile::tempdir().unwrap();
        let db5 = &RocksDB::new(tmp_path5.path().to_str().unwrap());
        db5.open().unwrap();

        let tmp_path6 = tempfile::tempdir().unwrap();
        let db6 = &RocksDB::new(tmp_path6.path().to_str().unwrap());
        db6.open().unwrap();

        // Initialize 4 tries
        let mut trie1 = MerkleTrie::new().unwrap();
        trie1.initialize(db1).unwrap();

        let mut trie2 = MerkleTrie::new().unwrap();
        trie2.initialize(db2).unwrap();

        let mut trie3 = MerkleTrie::new().unwrap();
        trie3.initialize(db3).unwrap();

        let mut trie4 = MerkleTrie::new().unwrap();
        trie4.initialize(db4).unwrap();

        let mut trie5 = MerkleTrie::new().unwrap();
        trie5.initialize(db5).unwrap();

        let mut trie6 = MerkleTrie::new().unwrap();
        trie6.initialize(db6).unwrap();

        // Generate 2000 random keys
        let mut rng = thread_rng();
        let key_vecs: Vec<Vec<u8>> = (0..2000)
            .map(|_| {
                let len = rng.gen_range(6..=20);
                (0..len).map(|_| rng.gen::<u8>()).collect()
            })
            .collect();

        // Create a list with 2000 keys + 1000 duplicates (first 1000 keys repeated)
        let mut all_keys: Vec<&[u8]> = key_vecs.iter().map(|v| v.as_slice()).collect();
        for i in 0..1000 {
            all_keys.push(key_vecs[i].as_slice()); // Add first 1000 keys again
        }
        // Now all_keys has 3000 elements: 2000 unique + 1000 duplicates

        // trie1 -> all 3000 in one go
        let mut txn_batch = RocksDbTransactionBatch::new();
        let results1 = trie1
            .insert(ctx, db1, &mut txn_batch, all_keys.clone())
            .unwrap();
        db1.commit(txn_batch).unwrap();
        let root_hash1 = trie1.root_hash().unwrap();

        // Assert first 2000 are true (unique keys) and last 1000 are false (duplicates)
        assert_eq!(results1.len(), 3000);
        for i in 0..2000 {
            assert_eq!(
                results1[i], true,
                "Position {} should be true (unique key)",
                i
            );
        }
        for i in 2000..3000 {
            assert_eq!(
                results1[i], false,
                "Position {} should be false (duplicate key)",
                i
            );
        }

        // trie2 -> 1500 + 1500
        let mut txn_batch = RocksDbTransactionBatch::new();
        let results2a = trie2
            .insert(ctx, db2, &mut txn_batch, all_keys[0..1500].to_vec())
            .unwrap();
        db2.commit(txn_batch).unwrap();

        let mut txn_batch = RocksDbTransactionBatch::new();
        let results2b = trie2
            .insert(ctx, db2, &mut txn_batch, all_keys[1500..3000].to_vec())
            .unwrap();
        db2.commit(txn_batch).unwrap();
        let root_hash2 = trie2.root_hash().unwrap();

        // Combine results for trie2
        let mut combined_results2 = results2a;
        combined_results2.extend(results2b);

        // Assert first 2000 are true (unique keys) and last 1000 are false (duplicates)
        assert_eq!(combined_results2.len(), 3000);
        for i in 0..2000 {
            assert_eq!(
                combined_results2[i], true,
                "Position {} should be true (unique key)",
                i
            );
        }
        for i in 2000..3000 {
            assert_eq!(
                combined_results2[i], false,
                "Position {} should be false (duplicate key)",
                i
            );
        }

        // trie3 -> 1000 + 1000 + 1000
        let mut txn_batch = RocksDbTransactionBatch::new();
        let results3a = trie3
            .insert(ctx, db3, &mut txn_batch, all_keys[0..1000].to_vec())
            .unwrap();
        db3.commit(txn_batch).unwrap();
        assert_eq!(trie3.items().unwrap(), 1000);
        trie3.reload(db3).unwrap();
        assert_eq!(trie3.items().unwrap(), 1000);

        let mut txn_batch = RocksDbTransactionBatch::new();
        let results3b = trie3
            .insert(ctx, db3, &mut txn_batch, all_keys[1000..2000].to_vec())
            .unwrap();
        assert_eq!(trie3.items().unwrap(), 2000);
        db3.commit(txn_batch).unwrap();
        trie3.reload(db3).unwrap();
        assert_eq!(trie3.items().unwrap(), 2000);

        let mut txn_batch = RocksDbTransactionBatch::new();
        let results3c = trie3
            .insert(ctx, db3, &mut txn_batch, all_keys[2000..3000].to_vec())
            .unwrap();
        // this txn_batch should be empty as all are duplicates
        assert!(txn_batch.batch.is_empty());
        assert_eq!(trie3.items().unwrap(), 2000);
        let root_hash3 = trie3.root_hash().unwrap();

        // Combine results for trie3
        let mut combined_results3 = results3a;
        combined_results3.extend(results3b);
        combined_results3.extend(results3c);

        // Assert first 2000 are true (unique keys) and last 1000 are false (duplicates)
        assert_eq!(combined_results3.len(), 3000);
        for i in 0..2000 {
            assert_eq!(
                combined_results3[i], true,
                "Position {} should be true (unique key)",
                i
            );
        }
        for i in 2000..3000 {
            assert_eq!(
                combined_results3[i], false,
                "Position {} should be false (duplicate key)",
                i
            );
        }

        // trie4 -> one at a time
        let mut results4: Vec<bool> = vec![];
        for key in &all_keys {
            let mut txn_batch = RocksDbTransactionBatch::new();
            let result = trie4.insert(ctx, db4, &mut txn_batch, vec![*key]).unwrap();
            results4.extend(result);
            db4.commit(txn_batch).unwrap();
        }
        let root_hash4 = trie4.root_hash().unwrap();

        // Assert first 2000 are true (unique keys) and last 1000 are false (duplicates)
        assert_eq!(results4.len(), 3000);
        for i in 0..2000 {
            assert_eq!(
                results4[i], true,
                "Position {} should be true (unique key)",
                i
            );
        }
        for i in 2000..3000 {
            assert_eq!(
                results4[i], false,
                "Position {} should be false (duplicate key)",
                i
            );
        }

        // trie5 -> only first 2000 unique keys, one at a time, each should return true
        for i in 0..2000 {
            let mut txn_batch = RocksDbTransactionBatch::new();
            let result = trie5
                .insert(ctx, db5, &mut txn_batch, vec![all_keys[i]])
                .unwrap();
            assert_eq!(result, vec![true]);
            db5.commit(txn_batch).unwrap();
        }
        let root_hash5 = trie5.root_hash().unwrap();

        // trie6 -> shuffle the 3000 keys, then bulk add them 100 at a time
        let mut shuffled_keys = all_keys.clone();
        shuffled_keys.shuffle(&mut thread_rng());

        let mut results6: Vec<bool> = vec![];
        for chunk in shuffled_keys.chunks(100) {
            let mut txn_batch = RocksDbTransactionBatch::new();
            let result = trie6
                .insert(ctx, db6, &mut txn_batch, chunk.to_vec())
                .unwrap();
            results6.extend(result);
            db6.commit(txn_batch).unwrap();
            trie6.reload(db6).unwrap();
        }

        let root_hash6 = trie6.root_hash().unwrap();

        let true_count = results6.iter().filter(|&&x| x).count();
        let false_count = results6.iter().filter(|&&x| !x).count();
        assert_eq!(true_count, 2000);
        assert_eq!(false_count, 1000);

        // Verify all 6 have the same hash
        assert_eq!(root_hash1, root_hash2);
        assert_eq!(root_hash2, root_hash3);
        assert_eq!(root_hash3, root_hash4);
        assert_eq!(root_hash4, root_hash5);
        assert_eq!(root_hash5, root_hash6);

        assert_eq!(trie1.items().unwrap(), 2000);
        assert_eq!(trie2.items().unwrap(), 2000);
        assert_eq!(trie3.items().unwrap(), 2000);
        assert_eq!(trie4.items().unwrap(), 2000);
        assert_eq!(trie5.items().unwrap(), 2000);
        assert_eq!(trie6.items().unwrap(), 2000);
    }
}
