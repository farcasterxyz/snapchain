use super::super::{
    constants::RootPrefix,
    db::{RocksDB, RocksDbTransactionBatch},
    util::{blake3_20, bytes_compare},
};
use super::errors::TrieError;
use super::merkle_trie::TrieSnapshot;
use crate::proto::DbTrieNode;
use prost::Message as _;
use std::collections::hash_map::Iter;
use std::collections::HashMap;
use std::sync::atomic;

// TODO: remove or reduce this and/or rename (make sure it works under all branching factors)
pub const TIMESTAMP_LENGTH: usize = 10;

// This value is mirrored in [rpc/server.ts], make sure to change it in both places
const MAX_VALUES_RETURNED_PER_CALL: usize = 1024;

pub struct Context<'a> {
    db_read_count: atomic::AtomicU64,
    mem_read_count: atomic::AtomicU64,
    on_drop: Option<Box<dyn FnOnce((u64, u64)) + 'a>>,
}

impl<'a> Context<'a> {
    pub fn new() -> Self {
        Self {
            db_read_count: atomic::AtomicU64::new(0),
            mem_read_count: atomic::AtomicU64::new(0),
            on_drop: None,
        }
    }

    pub fn with_callback<F>(callback: F) -> Self
    where
        F: FnOnce((u64, u64)) + 'a,
    {
        Self {
            db_read_count: atomic::AtomicU64::new(0),
            mem_read_count: atomic::AtomicU64::new(0),
            on_drop: Some(Box::new(callback)),
        }
    }
}

impl<'a> Drop for Context<'a> {
    fn drop(&mut self) {
        if let Some(callback) = self.on_drop.take() {
            let db_read_count = self.db_read_count.load(atomic::Ordering::Relaxed);
            let mem_read_count = self.mem_read_count.load(atomic::Ordering::Relaxed);
            callback((db_read_count, mem_read_count));
        }
    }
}

/// Represents a node in a MerkleTrie. Automatically updates the hashes when items are added,
/// and keeps track of the number of items in the subtree.
#[derive(Default, Debug, Clone)]
pub struct TrieNode {
    hash: Vec<u8>,
    items: usize,
    children: HashMap<u8, TrieNodeType>,
    pub child_hashes: HashMap<u8, Vec<u8>>,
    key: Option<Vec<u8>>,
}

// An empty struct that represents a serialized trie node, which will need to be loaded from the db
#[derive(Debug, Clone)]
pub struct SerializedTrieNode {
    pub hash: Option<Vec<u8>>,
}

impl SerializedTrieNode {
    fn new(hash: Option<Vec<u8>>) -> Self {
        SerializedTrieNode { hash }
    }
}

// An enum that represents the different types of trie nodes
#[derive(Debug, Clone)]
pub enum TrieNodeType {
    Node(TrieNode),
    Serialized(SerializedTrieNode),
}

impl TrieNode {
    pub fn new() -> Self {
        TrieNode {
            hash: vec![],
            items: 0,
            children: HashMap::new(),
            child_hashes: HashMap::new(),
            key: None,
        }
    }

    pub fn key_ref(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    pub(crate) fn make_primary_key(prefix: &[u8], child_char: Option<u8>) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + prefix.len() + 1);
        key.push(RootPrefix::SyncMerkleTrieNode as u8);
        key.extend_from_slice(prefix);
        if let Some(char) = child_char {
            key.push(char);
        }

        key
    }

    pub fn serialize(node: &TrieNode) -> Vec<u8> {
        let db_trie_node = DbTrieNode {
            key: node.key.as_ref().unwrap_or(&vec![]).clone(),
            child_chars: node.children.keys().map(|c| *c as u32).collect(),
            items: node.items as u32,
            hash: node.hash.clone(),
            child_hashes: node
                .child_hashes
                .iter()
                .map(|(&k, v)| (k as u32, v.clone()))
                .collect(),
        };

        db_trie_node.encode_to_vec()
    }

    pub(crate) fn deserialize(serialized: &[u8]) -> Result<TrieNode, TrieError> {
        let db_trie_node = DbTrieNode::decode(serialized).map_err(TrieError::wrap_deserialize)?;

        let mut children = HashMap::new();
        for char in db_trie_node.child_chars {
            children.insert(
                char as u8,
                TrieNodeType::Serialized(SerializedTrieNode::new(None)),
            );
        }

        let child_hashes = db_trie_node
            .child_hashes
            .into_iter()
            .map(|(k, v)| (k as u8, v))
            .collect();

        Ok(TrieNode {
            hash: db_trie_node.hash,
            items: db_trie_node.items as usize,
            children,
            child_hashes,
            key: Some(db_trie_node.key),
        })
    }
}

impl TrieNode {
    pub fn is_leaf(&self) -> bool {
        self.children.is_empty()
    }

    pub fn items(&self) -> usize {
        self.items
    }

    pub fn hash(&self) -> Vec<u8> {
        self.hash.clone()
    }

    #[cfg(test)]
    pub fn value(&self) -> Option<Vec<u8>> {
        // Value is only defined for leaf nodes
        if self.is_leaf() {
            self.key.clone()
        } else {
            None
        }
    }

    pub fn children(&self) -> &HashMap<u8, TrieNodeType> {
        &self.children
    }

    pub fn get_node_from_trie(
        &mut self,
        ctx: &Context,
        db: &RocksDB,
        prefix: &[u8],
        current_index: usize,
    ) -> Option<&mut TrieNode> {
        if current_index == prefix.len() {
            return Some(self);
        }

        let char = prefix[current_index];
        if !self.children.contains_key(&char) {
            return None;
        }

        if let Ok(child) = self.get_or_load_child(ctx, db, &prefix[..current_index], char) {
            child.get_node_from_trie(ctx, db, prefix, current_index + 1)
        } else {
            None
        }
    }

    pub fn show_hashes(&mut self) {
        println!(
            "child_hashes = {}",
            child_hashes_to_string(self.child_hashes.iter())
        );

        let mut keys: Vec<u8> = self.child_hashes.keys().cloned().collect();
        keys.sort();

        for k in keys {
            let v = self.children.get(&k).unwrap();
            match v {
                TrieNodeType::Node(n) => {
                    let ch = self.child_hashes.get(&k).cloned().unwrap_or(vec![]);
                    println!("n {} {} {}", k, hex::encode(n.hash()), hex::encode(&ch));
                }
                TrieNodeType::Serialized(n) => {
                    panic!("s {} {}", k, hex::encode(n.hash.as_ref().unwrap()));
                }
            }
        }
    }

    /**
     * Inserts a value into the trie. Returns true if the value was inserted, false if it already existed.
     * Recursively traverses the trie by prefix and inserts the value at the end. Updates the hashes for
     * every node that was traversed.
     *
     * @param key - The key to insert
     * @param current_index - The index of the current character in the key (only used internally)
     * @returns true if the value was inserted, false if it already existed
     */
    pub fn insert(
        &mut self,
        ctx: &Context,
        child_hashes: &mut HashMap<u8, Vec<u8>>,
        db: &RocksDB,
        txn: &mut RocksDbTransactionBatch,
        mut keys: Vec<Vec<u8>>,
        current_index: usize,
    ) -> Result<Vec<bool>, TrieError> {
        if keys.len() == 0 {
            return Err(TrieError::NoKeysToInsert);
        }

        // Note that all the keys will have the same prefix, so we can get the [0]th one
        let prefix = keys[0][..current_index].to_vec();

        let mut results = keys.iter().map(|_| false).collect::<Vec<bool>>();

        // Vec to store the keys that were not inserted and their index
        let remaining_keys;

        // Do not compact the timestamp portion of the trie, since it is used to compare snapshots
        if current_index >= TIMESTAMP_LENGTH && self.is_leaf() {
            let mut inserted = false;
            if self.key.is_none() {
                let key = keys.pop().unwrap();
                // Reached a leaf node with no value, insert it
                self.key = Some(key);
                self.items += 1;

                self.update_hash(ctx, db, child_hashes, &prefix)?;
                self.put_to_txn(txn, &prefix);

                inserted = true;

                results[0] = true; // the first key was inserted successfully
            }

            if keys.is_empty() {
                return Ok(results);
            }

            // See if any of the keys already exists
            remaining_keys = keys
                .into_iter()
                .enumerate()
                .filter_map(|(i, key)| {
                    if bytes_compare(self.key.as_ref().unwrap_or(&vec![]), key.as_slice()) == 0 {
                        // Key already exists, do nothing
                        results[i] = false;
                        None
                    } else {
                        Some((i + if inserted { 1 } else { 0 }, key)) // If we already pop()ed the first key, the index is i + 1
                    }
                })
                .collect::<Vec<_>>();

            if remaining_keys.is_empty() {
                // Nothing new was inserted, return
                return Ok(results);
            }

            //  If the key is different, and a value exists, then split the node
            self.split_leaf_node(ctx, db, txn, current_index)?;
        } else {
            // If not a leaf, then we need to add all the keys
            remaining_keys = keys.into_iter().enumerate().collect::<Vec<_>>()
        };

        // Check if any of the remaining keys are invalid
        if remaining_keys
            .iter()
            .any(|(_, key)| current_index >= key.len())
        {
            return Err(TrieError::KeyLengthExceeded);
        }

        // For the remaining keys, group them by the key[current_index] and insert them in bulk
        // at the next level
        let mut grouped_keys = HashMap::new();
        for (i, key) in remaining_keys.into_iter() {
            let char = key[current_index];
            let entry = grouped_keys.entry(char).or_insert_with(|| (vec![], vec![]));

            entry.0.push(i);
            entry.1.push(key);
        }

        let mut successes = 0;
        for (char, (is, keys)) in grouped_keys.into_iter() {
            if !self.children.contains_key(&char) {
                self.children
                    .insert(char, TrieNodeType::Node(TrieNode::default()));
            }

            // Recurse into a non-leaf node and instruct it to insert the value.
            let child_results = {
                let mut child_hashes = std::mem::take(&mut self.child_hashes);
                let child = self.get_or_load_child(ctx, db, &prefix, char)?;
                let results =
                    child.insert(ctx, &mut child_hashes, db, txn, keys, current_index + 1)?;
                self.child_hashes = child_hashes;
                results
            };

            for (i, result) in is.into_iter().zip(child_results) {
                results[i] = result;
                if result {
                    successes += 1;
                }
            }
        }

        if successes > 0 {
            self.items += successes;

            self.update_hash(ctx, db, child_hashes, &prefix)?;
            self.put_to_txn(txn, &prefix);
        }

        Ok(results)
    }

    pub fn delete(
        &mut self,
        ctx: &Context,
        child_hashes: &mut HashMap<u8, Vec<u8>>,
        db: &RocksDB,
        txn: &mut RocksDbTransactionBatch,
        keys: Vec<Vec<u8>>,
        current_index: usize,
    ) -> Result<Vec<bool>, TrieError> {
        let prefix = keys[0][..current_index].to_vec();
        let mut results = keys.iter().map(|_| false).collect::<Vec<bool>>();

        if self.is_leaf() {
            // If any of the keys match, then we delete the key
            for (i, key) in keys.iter().enumerate() {
                if bytes_compare(self.key.as_ref().unwrap_or(&vec![]), key) == 0 {
                    self.key = None;
                    self.items -= 1;

                    self.delete_to_txn(txn, &prefix);
                    self.update_hash(ctx, db, child_hashes, &prefix)?;

                    results[i] = true;
                    break;
                }
            }

            return Ok(results);
        }

        // Check if any of the remaining keys are invalid
        if keys.iter().any(|key| current_index >= key.len()) {
            return Err(TrieError::KeyLengthExceeded);
        }

        // For the remaining keys, we group them by the key[current_index] and delete them in bulk
        // at the next level
        let mut grouped_keys = HashMap::new();
        for (i, key) in keys.into_iter().enumerate() {
            let char = key[current_index];
            grouped_keys
                .entry(char)
                .or_insert_with(|| vec![])
                .push((i, key));
        }

        let mut successes = 0;
        for (char, child_keys) in grouped_keys.into_iter() {
            if !self.children.contains_key(&char) {
                // There are no more nodes under this, so we can return false for all the keys
                for (i, _) in child_keys.into_iter() {
                    results[i] = false;
                }
                continue;
            }

            // Split the child_keys into the "i"s and the keys
            let mut is = vec![];
            let mut keys = vec![];
            for (i, key) in child_keys.into_iter() {
                is.push(i);
                keys.push(key);
            }

            let (child_results, child_items) = {
                let mut child_hashes = std::mem::take(&mut self.child_hashes);
                let child = self.get_or_load_child(ctx, db, &prefix, char)?;
                let child_results =
                    child.delete(ctx, &mut child_hashes, db, txn, keys, current_index + 1)?;
                let child_items = child.items();
                self.child_hashes = child_hashes;
                (child_results, child_items)
            };

            // Delete the child if it's empty. This is required to make sure the hash will be the same
            // as another trie that doesn't have this node in the first place.
            if child_items == 0 {
                self.children.remove(&char);
                self.child_hashes.remove(&char);
            }

            for (i, result) in is.into_iter().zip(child_results) {
                results[i] = result;
                if result {
                    successes += 1;
                }
            }
        }

        if successes > 0 {
            self.items -= successes;

            if self.items == 0 {
                // Delete this node
                self.delete_to_txn(txn, &prefix);
                self.update_hash(ctx, db, child_hashes, &prefix)?;
                return Ok(results);
            }

            if self.items == 1 && self.children.len() == 1 && current_index >= TIMESTAMP_LENGTH {
                // Compact the trie by removing the child and moving the key up
                let char = *self.children.keys().next().unwrap();
                let child = self.get_or_load_child(ctx, db, &prefix, char)?;

                if child.key.is_some() {
                    self.key = child.key.take();
                    self.children.remove(&char);

                    // Delete child
                    let child_prefix = Self::make_primary_key(&prefix, Some(char));
                    self.delete_to_txn(txn, &child_prefix);
                }
            }

            self.update_hash(ctx, db, child_hashes, &prefix)?;
            self.put_to_txn(txn, &prefix);
        }

        Ok(results)
    }

    pub fn exists(
        &mut self,
        ctx: &Context,
        db: &RocksDB,
        key: &[u8],
        current_index: usize,
    ) -> Result<bool, TrieError> {
        if self.is_leaf() {
            return Ok(bytes_compare(self.key.as_ref().unwrap_or(&vec![]), key) == 0);
        }

        if current_index >= key.len() {
            return Ok(false);
        }
        let char = key[current_index];

        if !self.children.contains_key(&char) {
            return Ok(false);
        }

        let child = self.get_or_load_child(ctx, db, &key[..current_index], char)?;
        child.exists(ctx, db, key, current_index + 1)
    }

    /**
     *  Splits a leaf node into a non-leaf node by clearing its key/value and adding a child for
     *  the next char in its key
     */
    pub fn split_leaf_node(
        &mut self,
        ctx: &Context,
        db: &RocksDB,
        txn: &mut RocksDbTransactionBatch,
        current_index: usize,
    ) -> Result<(), TrieError> {
        let key = self.key.take().unwrap();
        let prefix = key[..current_index].to_vec();

        let new_child_char = key[current_index];

        self.children
            .insert(new_child_char, TrieNodeType::Node(TrieNode::default()));

        if let Some(TrieNodeType::Node(new_child)) = self.children.get_mut(&new_child_char) {
            let mut child_hashes = std::mem::take(&mut self.child_hashes);
            new_child.insert(
                ctx,
                &mut child_hashes,
                db,
                txn,
                vec![key],
                current_index + 1,
            )?;
            self.child_hashes = child_hashes;
            self.child_hashes.insert(new_child_char, new_child.hash());
        }

        self.put_to_txn(txn, &prefix);

        Ok(())
    }

    fn get_or_load_child(
        &mut self,
        ctx: &Context,
        db: &RocksDB,
        prefix: &[u8],
        char: u8,
    ) -> Result<&mut TrieNode, TrieError> {
        use std::collections::hash_map::Entry;

        match self.children.entry(char) {
            Entry::Occupied(mut entry) => {
                match entry.get_mut() {
                    TrieNodeType::Serialized(_) => {
                        let child_prefix = Self::make_primary_key(prefix, Some(char));
                        let child_node = db
                            .get(&child_prefix)
                            .map_err(TrieError::wrap_database)?
                            .map(|b| TrieNode::deserialize(&b).unwrap())
                            .unwrap_or_default();

                        *entry.get_mut() = TrieNodeType::Node(child_node);

                        ctx.db_read_count.fetch_add(1, atomic::Ordering::Relaxed);
                    }
                    TrieNodeType::Node(_) => {
                        ctx.mem_read_count.fetch_add(1, atomic::Ordering::Relaxed);
                    }
                }
                match entry.into_mut() {
                    TrieNodeType::Node(node) => Ok(node),
                    _ => Err(TrieError::InvalidChildNode { child_char: char }),
                }
            }
            Entry::Vacant(_) => Err(TrieError::ChildNotFound {
                char,
                prefix: prefix.to_vec(),
            }),
        }
    }

    fn update_hash(
        &mut self,
        ctx: &Context,
        db: &RocksDB,
        child_hashes_: &mut HashMap<u8, Vec<u8>>,
        prefix: &[u8],
    ) -> Result<(), TrieError> {
        if self.is_leaf() {
            self.hash = blake3_20(&self.key.as_ref().unwrap_or(&vec![]));
            if prefix.len() > 0 {
                child_hashes_.insert(prefix[prefix.len() - 1], self.hash.clone());
            }
            return Ok(());
        }

        // Sort the children by their "char" value
        let child_hashes: Vec<(u8, Vec<u8>)> = {
            let mut sorted_children: Vec<_> = self.children.iter_mut().collect();
            sorted_children.sort_by_key(|(char, _)| *char);

            sorted_children
                .iter()
                .map(|(char, child)| match child {
                    TrieNodeType::Node(node) => (**char, node.hash.clone()),
                    TrieNodeType::Serialized(serialized) => {
                        if serialized.hash.is_some() {
                            (**char, serialized.hash.as_ref().unwrap().clone())
                        } else {
                            (**char, vec![])
                        }
                    }
                })
                .collect::<Vec<_>>()
        };

        // If any of the child hashes are none, we load the child from the db
        let mut concat_hashes = vec![];
        for (char, hash) in child_hashes.iter() {
            if hash.is_empty() {
                let child = self.get_or_load_child(ctx, db, prefix, *char)?;
                concat_hashes.extend_from_slice(child.hash.as_slice());
            } else {
                concat_hashes.extend_from_slice(hash.as_slice());
            }
        }

        self.hash = blake3_20(&concat_hashes);
        if prefix.len() > 0 {
            child_hashes_.insert(prefix[prefix.len() - 1], self.hash.clone());
        }
        Ok(())
    }

    fn excluded_hash(
        &mut self,
        ctx: &Context,
        db: &RocksDB,
        prefix: &[u8],
        prefix_char: u8,
    ) -> Result<(usize, String), TrieError> {
        let mut excluded_items = 0;
        let mut child_hashes = vec![];

        let mut sorted_children = self.children.keys().map(|c| *c).collect::<Vec<_>>();
        sorted_children.sort();

        for char in sorted_children {
            if char != prefix_char {
                let child_node = self.get_or_load_child(ctx, db, prefix, char)?;
                child_hashes.push(child_node.hash.clone());
                excluded_items += child_node.items;
            }
        }

        let hash = blake3_20(&child_hashes.concat());

        Ok((excluded_items, hex::encode(hash.as_slice())))
    }

    fn put_to_txn(&self, txn: &mut RocksDbTransactionBatch, prefix: &[u8]) {
        let key = Self::make_primary_key(prefix, None);
        let serialized = Self::serialize(self);
        txn.put(key, serialized);
    }

    fn delete_to_txn(&self, txn: &mut RocksDbTransactionBatch, prefix: &[u8]) {
        let key = Self::make_primary_key(prefix, None);
        txn.delete(key);
    }

    #[allow(dead_code)] // TODO
    pub fn unload_children(&mut self) {
        let mut serialized_children = HashMap::new();
        for (char, child) in self.children.iter_mut() {
            if let TrieNodeType::Node(child) = child {
                serialized_children.insert(
                    *char,
                    TrieNodeType::Serialized(SerializedTrieNode::new(Some(child.hash.clone()))),
                );
            }
        }
        self.children = serialized_children;
    }

    pub fn get_all_values(
        &mut self,
        ctx: &Context,
        db: &RocksDB,
        prefix: &[u8],
    ) -> Result<Vec<Vec<u8>>, TrieError> {
        if self.is_leaf() {
            return Ok(vec![self.key.clone().unwrap_or(vec![])]);
        }

        let mut values = vec![];
        let mut sorted_children = self.children.iter().map(|(c, _)| *c).collect::<Vec<_>>();
        sorted_children.sort();

        for char in sorted_children {
            let child_node = self.get_or_load_child(ctx, db, prefix, char)?;

            let mut child_prefix = prefix.to_vec();
            child_prefix.push(char);
            values.extend(child_node.get_all_values(ctx, db, &child_prefix)?);

            if values.len() > MAX_VALUES_RETURNED_PER_CALL {
                break;
            }
        }

        Ok(values)
    }

    pub fn get_snapshot(
        &mut self,
        ctx: &Context,
        db: &RocksDB,
        prefix: &[u8],
        current_index: usize,
    ) -> Result<TrieSnapshot, TrieError> {
        let mut excluded_hashes = vec![];
        let mut num_messages = 0;

        let mut current_node = self; // traverse from the current node
        for (i, char) in prefix.iter().enumerate().skip(current_index) {
            let current_prefix = prefix[0..i].to_vec();

            let (excluded_items, excluded_hash) =
                current_node.excluded_hash(ctx, db, &current_prefix, *char)?;

            excluded_hashes.push(excluded_hash);
            num_messages += excluded_items;

            if !current_node.children.contains_key(char) {
                return Ok(TrieSnapshot {
                    prefix: current_prefix,
                    excluded_hashes,
                    num_messages,
                });
            }

            current_node = current_node.get_or_load_child(ctx, db, &current_prefix, *char)?;
        }

        excluded_hashes.push(hex::encode(current_node.hash.as_slice()));

        Ok(TrieSnapshot {
            prefix: prefix.to_vec(),
            excluded_hashes,
            num_messages,
        })
    }

    pub(crate) fn print(&self, prefix: u8, depth: usize) -> Result<(), TrieError> {
        let indent = "  ".repeat(depth);
        let key = encode_with_spaces(self.key.as_ref().unwrap_or(&vec![]));

        let mut child_hashes: String = String::new();
        for (k, v) in self.child_hashes.iter() {
            child_hashes += format!("{k}:{} ", hex::encode(v)).as_str();
        }

        println!(
            "{}0x{} [{}]: {} {}",
            indent,
            hex::encode(vec![prefix]),
            key,
            hex::encode(self.hash.as_slice()),
            child_hashes
        );

        for (char, child) in self.children.iter() {
            match child {
                TrieNodeType::Node(child_node) => child_node.print(*char, depth + 1)?,
                TrieNodeType::Serialized(_) => {
                    println!("{}  {} (serialized):", indent, *char as char);
                }
            }
        }

        Ok(())
    }
}

fn child_hashes_to_string(p0: Iter<u8, Vec<u8>>) -> String {
    let mut child_hashes: String = String::new();
    for (k, v) in p0 {
        child_hashes += format!("{k}:{} ", hex::encode(v)).as_str();
    }
    child_hashes
}

fn encode_with_spaces(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{:02x}", byte)) // Convert each byte to a two-character hex string
        .collect::<Vec<_>>() // Collect as a vector of strings
        .join(" ") // Join them with spaces
}
