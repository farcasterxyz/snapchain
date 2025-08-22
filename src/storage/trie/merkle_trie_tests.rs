#[cfg(test)]
mod tests {
    use crate::storage::db::{RocksDB, RocksDbTransactionBatch};
    use crate::storage::store::account::IntoU8;
    use crate::storage::trie::merkle_trie::{Context, MerkleTrie, TrieKey};
    use crate::storage::trie::util;
    use crate::utils::factory::{events_factory, messages_factory};

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

    #[test]
    fn test_attach_root() {
        let ctx = &Context::new();
        let tmp_path = tempfile::tempdir().unwrap();
        let db = &RocksDB::new(tmp_path.path().to_str().unwrap());
        db.open().unwrap();

        let branching_factor = 16;
        let key_conv = util::get_transform_functions(branching_factor).unwrap();

        let mut trie = MerkleTrie::new(branching_factor).unwrap();
        trie.initialize(db).unwrap();

        let key_vecs: Vec<Vec<u8>> = (0..4)
            .map(|_| (0..10).map(|_| rand::random::<u8>()).collect())
            .collect();
        let keys: Vec<&[u8]> = key_vecs.iter().map(|v| v.as_slice()).collect();

        let mut txn_batch = RocksDbTransactionBatch::new();
        trie.insert(ctx, db, &mut txn_batch, keys.clone()).unwrap();
        db.commit(txn_batch).unwrap();
        let root_hash = trie.root_hash().unwrap();

        // Just do recalculate hashes first, this should be a no-op
        let mut txn_batch = RocksDbTransactionBatch::new();
        trie.recalculate_hashes(ctx, db, &mut txn_batch, 6).unwrap();
        db.commit(txn_batch).unwrap();

        assert_eq!(trie.root_hash().unwrap(), root_hash);

        // Key for the node to mess up and then re attach
        let key = &key_vecs[0][0..6]; // try to attach this node
        let xkey = (key_conv.expand)(key); // with this expanded key
        let xprefix = &xkey[0..xkey.len() - 1]; // parent's prefix
        let child_char = xkey[xkey.len() - 1]; // child's character in the parent's children[]

        println!(
            "Key[0] is {:?}, xkey is {:?}",
            hex::encode(keys[0]),
            hex::encode(xkey.clone())
        );

        // Assert that the 0th key is in the DB and in the trie
        {
            let root = trie.get_root_node().unwrap();
            assert_eq!(root.items(), 4);

            let prefix_node = root.get_node_from_trie(ctx, db, &xprefix, 0).unwrap();

            // Now, intentionally remove this prefix->child from the trie, but keep it in the DB
            let mut children = prefix_node.children().clone();
            let removed = children.remove(&child_char);
            prefix_node.set_children(children);
            assert!(removed.is_some());

            let mut child_hashes = prefix_node.child_hashes().clone();
            let removed = child_hashes.remove(&child_char);
            prefix_node.set_child_hashes(child_hashes);
            assert!(removed.is_some());

            // Mess up the hashes all the way to the root for the prefix. This should be fixed up by the
            // attach_to_root operation
            for i in (0..=xprefix.len() - 1).rev() {
                let node = root.get_node_from_trie(ctx, db, &xprefix[..i], 0).unwrap();
                let child_char = xprefix[i];
                let mut child_hashes = node.child_hashes().clone();
                child_hashes.insert(child_char, vec![0; 32]);
                node.set_child_hashes(child_hashes);
            }
        }
        assert_ne!(
            hex::encode(trie.root_hash().unwrap()),
            hex::encode(root_hash.clone())
        );

        // Now, attach the prefix node to the trie
        let mut txn_batch = RocksDbTransactionBatch::new();
        let r1 = trie.attach_to_root(ctx, db, &mut txn_batch, &key);
        db.commit(txn_batch).unwrap();
        trie.reload(db).unwrap();
        assert!(r1.is_ok());

        let mut txn_batch = RocksDbTransactionBatch::new();
        let r = trie.recalculate_hashes(ctx, db, &mut txn_batch, 6);
        db.commit(txn_batch).unwrap();
        assert!(r.is_ok());

        let (is_attached, was_created) = r1.unwrap();
        assert!(is_attached && was_created); // Assert that it was attached

        // Now, the root hashes should match the original
        assert_eq!(
            hex::encode(trie.root_hash().unwrap()),
            hex::encode(root_hash.clone())
        );

        // Attaching it again should be a no-op
        let mut txn_batch = RocksDbTransactionBatch::new();
        let r1 = trie.attach_to_root(ctx, db, &mut txn_batch, &key);
        db.commit(txn_batch).unwrap();
        assert!(r1.is_ok());

        let mut txn_batch = RocksDbTransactionBatch::new();
        let r = trie.recalculate_hashes(ctx, db, &mut txn_batch, 6);
        db.commit(txn_batch).unwrap();
        assert!(r.is_ok());

        let (is_attached, was_created) = r1.unwrap();
        assert!(is_attached && !was_created); // Assert that it was not created again

        // root hashes should still match the original
        assert_eq!(
            hex::encode(trie.root_hash().unwrap()),
            hex::encode(root_hash)
        );

        // Attempting to attach a non-existing node should not be an error, but is_attached and was_created should
        // both be false
        let mut txn_batch = RocksDbTransactionBatch::new();
        let r = trie.attach_to_root(ctx, db, &mut txn_batch, &[0; 16]);
        db.commit(txn_batch).unwrap();

        assert!(r.is_ok());

        let (is_attached, was_created) = r.unwrap();
        assert!(!is_attached && !was_created);
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

        let branching_factor = 16;

        let mut trie1 = MerkleTrie::new(branching_factor).unwrap();
        trie1.initialize(db1).unwrap();

        let mut trie2 = MerkleTrie::new(branching_factor).unwrap();
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
}
