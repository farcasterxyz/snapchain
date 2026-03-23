use super::super::db::{RocksDB, RocksDbTransactionBatch};
use super::errors::TrieError;
use super::trie_node::{TrieNode, UNCOMPACTED_LENGTH};
use crate::mempool::routing::{MessageRouter, ShardRouter};
use crate::proto;
use crate::storage::store::account::{make_fid_key, read_fid_key, IntoU8, FID_BYTES};
use crate::storage::trie::util::{combine_nibbles, expand_nibbles};
use crate::storage::trie::{trie_node, util};
use std::collections::HashMap;
use std::str;
use tracing::info;
pub use trie_node::Context;

pub const TRIE_DBPATH_PREFIX: &str = "trieDb";
pub const USERNAME_MAX_LENGTH: u32 = 20;
pub const FNAME_MESSAGE_TYPE: u8 = 7;

pub const TRIE_SHARD_SIZE: u32 = 256; // So it fits into 1 byte

pub struct DecodedTrieKey {
    pub virtual_shard: u8,
    pub fid: u64,
    pub onchain_message_type: Option<u8>,
    pub message_type: Option<u8>,
    pub rest: Vec<u8>, // The rest of the (undecoded) key if any
}

pub struct TrieKey {}

impl TrieKey {
    #[inline]
    pub fn for_message(msg: &proto::Message) -> Vec<Vec<u8>> {
        let mut keys = vec![];
        let mut key = Self::for_message_type(msg.fid(), msg.msg_type().into_u8());
        key.extend_from_slice(&msg.hash);
        keys.push(key);

        // Storage lend messages are put into the trie in 2 places, one for the lender and one for the borrower. This produces the borrower key.
        match &msg.data.as_ref().unwrap().body {
            Some(proto::message_data::Body::LendStorageBody(lend_storage_body)) => {
                let mut key =
                    Self::for_message_type(lend_storage_body.to_fid, msg.msg_type().into_u8());
                key.extend_from_slice(&msg.hash);
                keys.push(key);
            }
            _ => {}
        }

        keys
    }

    #[inline]
    pub fn for_message_type(fid: u64, msg_type: u8) -> Vec<u8> {
        let fid_bytes = Self::for_fid(fid);
        let mut key = Vec::with_capacity(fid_bytes.len() + 1);
        key.extend_from_slice(&fid_bytes);
        // Left shift msg_ype by 3 bits so we don't collide with onchain event types.
        // Supports 8 reserved types (onchain events, fnames etc) and 32 message types
        key.push(msg_type << 3);
        key
    }

    #[inline]
    pub fn for_onchain_event(event: &proto::OnChainEvent) -> Vec<u8> {
        let mut key = Vec::new();
        key.extend_from_slice(&Self::for_fid(event.fid));
        key.push(event.r#type as u8);
        key.extend_from_slice(&event.transaction_hash);
        key.extend_from_slice(&event.log_index.to_be_bytes());
        key
    }

    #[inline]
    pub fn for_fname(fid: u64, name: &String) -> Vec<u8> {
        let fid_bytes = Self::for_fid(fid);
        let mut key = Vec::with_capacity(fid_bytes.len() + 1 + USERNAME_MAX_LENGTH as usize);
        key.extend_from_slice(&fid_bytes);
        key.push(FNAME_MESSAGE_TYPE); // 1-6 is for onchain events, use 7 for fnames, and everything else for messages

        // Pad the name with null bytes to ensure all names have the same length. The trie cannot handle entries that are substrings for another (e.g. "net" and "network")
        let name_bytes = name.as_bytes();
        key.extend_from_slice(name_bytes);
        if name_bytes.len() < USERNAME_MAX_LENGTH as usize {
            key.extend(std::iter::repeat(0).take(USERNAME_MAX_LENGTH as usize - name_bytes.len()));
        }
        key
    }

    #[inline]
    pub fn for_fid(fid: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + FID_BYTES);
        key.push(Self::fid_shard(fid));
        key.extend_from_slice(&make_fid_key(fid));
        key
    }

    // We divide fids into shards in the trie so in case we need to change the number of shards in the network,
    // we don't need to move the fids individually. This function maps an fid into a 1 byte shard number.
    // i.e. the trie behaves as if there are 256 virtual shards regardless of the actual number of shards in the network.
    pub fn fid_shard(fid: u64) -> u8 {
        static TRIE_ROUTER: ShardRouter = ShardRouter {};
        // route_fid adds 1 to avoid using shard 0 (the root shard). Subtract it to make it
        // 0-indexed, so it fits into 1 byte without overflow
        (TRIE_ROUTER.route_fid(fid, TRIE_SHARD_SIZE) - 1) as u8
    }

    // Decode a trie key into (virtual_shard_id, fid, onchain_message_type, message_type, hash OR fname OR tx_hash+log_index)
    pub fn decode(key: &[u8]) -> Result<DecodedTrieKey, TrieError> {
        if key.len() < UNCOMPACTED_LENGTH {
            return Err(TrieError::KeyLengthTooShort);
        }

        let message_type_pos = 1 + FID_BYTES;

        let virtual_shard = key[0];
        let fid = read_fid_key(&key, 1);

        let (onchain_message_type, message_type) = if key[message_type_pos] < (1 << 3) {
            // On-chain event
            (Some(key[message_type_pos]), None)
        } else {
            // Regular message
            (None, Some(key[message_type_pos] >> 3))
        };

        let rest = key[(message_type_pos + 1)..].to_vec();

        Ok(DecodedTrieKey {
            virtual_shard,
            fid,
            onchain_message_type,
            message_type,
            rest,
        })
    }

    // Compute the keys that need to be updated in the trie. Returns (inserts, deletes)
    pub fn for_hub_event(event: &proto::HubEvent) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
        let mut inserts = Vec::new();
        let mut deletes = Vec::new();

        match &event.body {
            Some(proto::hub_event::Body::MergeMessageBody(merge)) => {
                if let Some(msg) = &merge.message {
                    inserts.extend(TrieKey::for_message(&msg));
                }
                for deleted_message in &merge.deleted_messages {
                    deletes.extend(TrieKey::for_message(&deleted_message));
                }
            }
            Some(proto::hub_event::Body::MergeOnChainEventBody(merge)) => {
                if let Some(onchain_event) = &merge.on_chain_event {
                    inserts.push(TrieKey::for_onchain_event(&onchain_event));
                }
            }
            Some(proto::hub_event::Body::PruneMessageBody(prune)) => {
                if let Some(msg) = &prune.message {
                    deletes.extend(TrieKey::for_message(&msg));
                }
            }

            Some(proto::hub_event::Body::RevokeMessageBody(revoke)) => {
                if let Some(msg) = &revoke.message {
                    deletes.extend(TrieKey::for_message(&msg));
                }
            }
            Some(proto::hub_event::Body::MergeUsernameProofBody(merge)) => {
                if let Some(msg) = &merge.username_proof_message {
                    inserts.extend(TrieKey::for_message(&msg));
                }
                if let Some(msg) = &merge.deleted_username_proof_message {
                    deletes.extend(TrieKey::for_message(&msg));
                }
                if let Some(proof) = &merge.username_proof {
                    if proof.r#type == proto::UserNameType::UsernameTypeFname as i32
                        && proof.fid != 0
                    // Deletes should not be added to the trie
                    {
                        let name = str::from_utf8(&proof.name).unwrap().to_string();
                        inserts.push(TrieKey::for_fname(proof.fid, &name));
                    }
                }
                if let Some(proof) = &merge.deleted_username_proof {
                    if proof.r#type == proto::UserNameType::UsernameTypeFname as i32 {
                        let name = str::from_utf8(&proof.name).unwrap().to_string();
                        deletes.push(TrieKey::for_fname(proof.fid, &name));
                    }
                }
            }
            Some(proto::hub_event::Body::MergeFailure(_)) => {
                // Merge failures don't affect the trie. They are only for event subscribers
            }
            Some(proto::hub_event::Body::BlockConfirmedBody(_)) => {
                // BLOCK_CONFIRMED events don't affect the trie state.
            }
            &None => {
                // This should never happen
                panic!("No body in event");
            }
        };
        (inserts, deletes)
    }
}

#[derive(Debug)]
pub struct NodeMetadata {
    pub prefix: Vec<u8>,
    pub num_messages: usize,
    pub hash: String,
    pub children: HashMap<u8, NodeMetadata>,
}

#[derive(Clone)]
pub struct MerkleTrie {
    root: Option<TrieNode>,
}

impl MerkleTrie {
    pub fn new() -> Result<Self, TrieError> {
        Ok(MerkleTrie { root: None })
    }

    pub fn update_for_event(
        &mut self,
        ctx: &Context,
        db: &RocksDB,
        event: &proto::HubEvent,
        txn_batch: &mut RocksDbTransactionBatch,
    ) -> Result<(), TrieError> {
        let (inserts, deletes) = TrieKey::for_hub_event(event);

        for key in inserts {
            self.insert(ctx, db, txn_batch, vec![&key])?;
        }
        for key in deletes {
            self.delete(ctx, db, txn_batch, vec![&key])?;
        }

        Ok(())
    }

    fn create_empty_root(&mut self, txn_batch: &mut RocksDbTransactionBatch) {
        let root_key = TrieNode::make_primary_key(&[], None);
        let empty = TrieNode::new();
        let serialized = TrieNode::serialize(&empty);

        // Write the empty root node to the DB
        txn_batch.put(root_key, serialized);
        self.root.replace(empty);
    }

    pub fn initialize(&mut self, db: &RocksDB) -> Result<(), TrieError> {
        // db must be "open" by now

        let loaded = self.load_root(db)?;
        if let Some(root_node) = loaded {
            self.root.replace(root_node);
        } else {
            info!("Initializing empty merkle trie root");
            let mut txn_batch = RocksDbTransactionBatch::new();
            self.create_empty_root(&mut txn_batch);
            db.commit(txn_batch).map_err(TrieError::wrap_database)?;
        }

        Ok(())
    }

    fn load_root(&self, db: &RocksDB) -> Result<Option<TrieNode>, TrieError> {
        let root_key = TrieNode::make_primary_key(&[], None);

        if let Some(root_bytes) = db.get(&root_key).map_err(TrieError::wrap_database)? {
            let root_node = TrieNode::deserialize(&root_bytes.as_slice())?;
            Ok(Some(root_node))
        } else {
            Ok(None)
        }
    }

    pub fn reload(&mut self, db: &RocksDB) -> Result<(), TrieError> {
        // Load the root node using the provided database reference
        let loaded = self.load_root(db)?;

        match loaded {
            Some(replacement_root) => {
                // Replace the root node with the loaded node
                self.root.replace(replacement_root);
                Ok(())
            }
            None => Err(TrieError::UnableToReloadRoot),
        }
    }

    pub fn insert(
        &mut self,
        ctx: &Context,
        db: &RocksDB,
        txn_batch: &mut RocksDbTransactionBatch,
        keys: Vec<&[u8]>,
    ) -> Result<Vec<bool>, TrieError> {
        let keys: Vec<Vec<u8>> = keys.into_iter().map(expand_nibbles).collect();

        if keys.is_empty() {
            return Ok(Vec::new());
        }

        for key in keys.iter() {
            if key.len() < UNCOMPACTED_LENGTH {
                return Err(TrieError::KeyLengthTooShort);
            }
        }

        if let Some(root) = self.root.as_mut() {
            let mut txn = RocksDbTransactionBatch::new();
            // root_stub_map: the hash of a node is stored/cached in the parent of that node (in a hashmap)
            // the root doesn't have a parent, but still needs a hashmap here to satisfy the API. Note also
            // that the root's hash will NOT be populated anywhere in this map. Use root.hash() for that.
            let mut root_stub_map = HashMap::new();
            let results = root.insert(ctx, &mut root_stub_map, db, &mut txn, keys, 0)?;

            txn_batch.merge(txn);
            Ok(results)
        } else {
            Err(TrieError::TrieNotInitialized)
        }
    }

    pub fn delete(
        &mut self,
        ctx: &Context,
        db: &RocksDB,
        txn_batch: &mut RocksDbTransactionBatch,
        keys: Vec<&[u8]>,
    ) -> Result<Vec<bool>, TrieError> {
        let keys: Vec<Vec<u8>> = keys.into_iter().map(expand_nibbles).collect();

        if keys.is_empty() {
            return Ok(Vec::new());
        }

        for key in keys.iter() {
            if key.len() < UNCOMPACTED_LENGTH {
                return Err(TrieError::KeyLengthTooShort);
            }
        }

        if let Some(root) = self.root.as_mut() {
            let mut txn = RocksDbTransactionBatch::new();
            // root_stub_map: see comment in insert()
            let root_stub_map = &mut HashMap::new();
            let results = root.delete(ctx, root_stub_map, db, &mut txn, keys, 0)?;

            txn_batch.merge(txn);
            Ok(results)
        } else {
            Err(TrieError::TrieNotInitialized)
        }
    }

    pub fn exists(&mut self, ctx: &Context, db: &RocksDB, key: &[u8]) -> Result<bool, TrieError> {
        let key: Vec<u8> = expand_nibbles(key);

        if let Some(root) = self.root.as_mut() {
            root.exists(ctx, db, &key, 0)
        } else {
            Err(TrieError::TrieNotInitialized)
        }
    }

    pub fn items(&self) -> Result<usize, TrieError> {
        if let Some(root) = self.root.as_ref() {
            Ok(root.items())
        } else {
            Err(TrieError::TrieNotInitialized)
        }
    }

    fn get_node_from_txn_or_db(
        db: &RocksDB,
        txn_batch: &RocksDbTransactionBatch,
        node_key: &[u8],
    ) -> Option<TrieNode> {
        // First, attempt to get it from the DB cache
        if let Some(Some(node_bytes)) = txn_batch.batch.get(node_key) {
            if let Ok(node) = TrieNode::deserialize(&node_bytes) {
                return Some(node);
            }
        }

        // Else, get it directly from the DB
        if let Some(node_bytes) = db.get(node_key).ok().flatten() {
            if let Ok(node) = TrieNode::deserialize(&node_bytes) {
                return Some(node);
            }
        }

        None
    }

    fn get_node(
        &self,
        db: &RocksDB,
        txn_batch: &RocksDbTransactionBatch,
        prefix: &[u8],
    ) -> Option<TrieNode> {
        let prefix = expand_nibbles(prefix);
        let node_key = TrieNode::make_primary_key(&prefix, None);

        return Self::get_node_from_txn_or_db(db, txn_batch, &node_key);
    }

    pub fn get_hash(
        &self,
        db: &RocksDB,
        txn_batch: &RocksDbTransactionBatch,
        prefix: &[u8],
    ) -> Result<Vec<u8>, TrieError> {
        Ok(self
            .get_node(db, txn_batch, prefix)
            .map(|node| node.hash())
            .unwrap_or(vec![]))
    }

    pub fn get_count(
        &self,
        db: &RocksDB,
        txn_batch: &RocksDbTransactionBatch,
        prefix: &[u8],
    ) -> Result<u64, TrieError> {
        Ok(self
            .get_node(db, txn_batch, prefix)
            .map(|node| node.items() as u64)
            .unwrap_or(0))
    }

    pub fn root_hash(&self) -> Result<Vec<u8>, TrieError> {
        if let Some(root) = self.root.as_ref() {
            Ok(root.hash())
        } else {
            Err(TrieError::TrieNotInitialized)
        }
    }

    pub fn get_all_values(
        &mut self,
        ctx: &Context,
        db: &RocksDB,
        prefix: &[u8],
    ) -> Result<Vec<Vec<u8>>, TrieError> {
        let prefix = expand_nibbles(prefix);

        if let Some(root) = self.root.as_mut() {
            if let Some(node) = root.get_node_from_trie(ctx, db, &prefix, 0) {
                match node.get_all_values(ctx, db, &prefix) {
                    Ok(values) => Ok(values
                        .into_iter()
                        .map(|v| combine_nibbles(v.as_slice()))
                        .collect()),
                    Err(e) => Err(e),
                }
            } else {
                Ok(Vec::new())
            }
        } else {
            Err(TrieError::TrieNotInitialized)
        }
    }

    /// Get all the keys of the given subtree starting at `prefix`.
    /// The `leaf_keys` is filled with the keys found, upto `max_items`
    /// If there are more keys present, a `next_page_token` is returned, which should be sent back as the `page_token`
    /// the next time to retrieve the remaining keys
    pub fn get_paged_values_of_subtree(
        &mut self,
        ctx: &Context,
        db: &RocksDB,
        prefix: &[u8],
        leaf_keys: &mut Vec<Vec<u8>>,
        max_items: usize,
        page_token: Option<String>,
    ) -> Result<Option<String>, TrieError> {
        if self.root.is_none() {
            return Err(TrieError::TrieNotInitialized);
        }

        // Expand the input prefix using the branching factor transform to match the trie's internal key format
        let expanded_prefix = expand_nibbles(prefix);

        // Attempt to retrieve the subtree node starting from the root using the expanded prefix. We will visit all the leafs of this
        // subtree
        let subtree_node_opt =
            self.root
                .as_mut()
                .unwrap()
                .get_node_from_trie(ctx, db, &expanded_prefix, 0);

        if subtree_node_opt.is_none() {
            return Ok(None);
        }
        let subtree_node = subtree_node_opt.unwrap();

        // This tracks the full current path from the root of the trie (including the expanded_prefix) to the node being processed.
        // It is built by appending child chars as we traverse deeper and popping them when backtracking.
        let mut path = expanded_prefix.clone();

        // This tracks only the path segments added beyond the initial expanded_prefix (i.e., within the subtree). It mirrors the depth
        // traversed in the subtree and is used to construct the page token for resumption.
        let mut relative_path: Vec<u8> = vec![];

        // A stack of Vec<u8> where each inner Vec represents the remaining child chars to explore at that depth level. Chars are sorted
        // ascending and reversed so pop() yields the smallest (lex order). The stack size equals the current depth in the subtree plus
        // one (for the current level).
        let mut remaining_stack: Vec<Vec<u8>>;

        // Record the initial length of leaf_keys to track how many new items are added in this call
        let initial_len = leaf_keys.len();

        // If a page_token is provided, resume the iteration from the saved state
        let page_token = page_token
            .map(|token_str| util::decode_trie_page_token(&token_str))
            .transpose()?
            .flatten();

        if let Some(token) = page_token {
            // Validate that the token is not empty; if it is, return an error
            if token.is_empty() {
                return Err(TrieError::InvalidPageToken("<Empty token>".to_string()));
            }

            // Extract the relative_path from the first element of the token
            relative_path = token[0].clone();

            // Extract the remaining_stack from the rest of the token
            remaining_stack = token[1..].to_vec();

            // Enforce the invariant: relative_path.len() should be remaining_stack.len() - 1 (or 0 if stack has 1 level)
            if relative_path.len() != remaining_stack.len().saturating_sub(1) {
                return Err(TrieError::InvalidPageToken(hex::encode(&relative_path)));
            }
            // Extend the full path with the relative_path to resume at the correct position in the trie
            path.extend_from_slice(&relative_path);
        } else {
            // No page_token: start fresh from the subtree root
            // If the subtree root is a leaf, collect its key if present and return None (no more pages)
            if subtree_node.is_leaf() {
                // Check if the leaf has a valid key
                if let Some(key) = &subtree_node.value() {
                    // Ensure the key is not empty before processing
                    if key.is_empty() {
                        return Err(TrieError::KeyLengthTooShort);
                    }

                    // Combine the key back to its original form using the transform
                    let combined = combine_nibbles(key.as_slice());
                    // Append the combined key to the leaf_keys vector
                    leaf_keys.push(combined);
                }
                // Since it's a single leaf, iteration is complete
                return Ok(None);
            } else {
                // Subtree root is not a leaf; prepare to traverse its children
                // Collect and sort the child chars for ordered traversal
                let mut children: Vec<u8> = subtree_node.children().keys().cloned().collect();
                // If no children, the subtree is empty, return None
                if children.is_empty() {
                    return Err(TrieError::InvalidState(format!(
                        "Non-leaf node {:?} didn't have any children",
                        expanded_prefix
                    )));
                }

                // Sort children
                children.sort();
                // Reverse so that pop() yields the smallest char first
                children.reverse();

                // Initialize the remaining_stack with this level's children
                remaining_stack = vec![children];
            }
        }

        // Main iteration loop: performs iterative DFS traversal
        loop {
            // Check if we've collected the maximum number of items for this page
            if leaf_keys.len() - initial_len >= max_items {
                // Construct the page token: first element is relative_path, followed by remaining_stack levels
                let mut token: Vec<Vec<u8>> = vec![relative_path.clone()];
                token.extend(remaining_stack.into_iter());
                let encoded = crate::storage::trie::util::encode_trie_page_token(&Some(token));
                return Ok(Some(encoded));
            }

            // If the remaining_stack is empty, traversal is complete
            if remaining_stack.is_empty() {
                // No more items to process, return None
                return Ok(None);
            }

            // Get mutable reference to the top level's remaining children
            let remaining = remaining_stack.last_mut().unwrap();

            // If the current level has no more children, backtrack
            if remaining.is_empty() {
                // Remove the exhausted level from the stack
                remaining_stack.pop();
                // If relative_path is not empty, pop the last segment (backtrack in path)
                if !relative_path.is_empty() {
                    // Remove the last char from relative_path
                    relative_path.pop();
                    // Also remove from the full path
                    path.pop();
                }
                // Continue (i.e, process next set of remaining children at parent level)
                continue;
            }

            // Pop the next child char to explore (smallest remaining due to reverse sort)
            let next_char = remaining.pop().unwrap();

            // Append the char to the full path
            path.push(next_char);

            // Append to the relative_path (subtree-specific path)
            relative_path.push(next_char);

            // Retrieve the node at the current path
            let current_node_opt = self
                .root
                .as_mut()
                .unwrap()
                .get_node_from_trie(ctx, db, &path, 0);

            // The node should exist, error if not. This should never happen
            if current_node_opt.is_none() {
                return Err(TrieError::NodeNotFound {
                    prefix: path.clone(),
                });
            }
            let current_node = current_node_opt.unwrap();

            // If the current node is a leaf, collect its key
            if current_node.is_leaf() {
                // Check if the leaf has a valid key
                if let Some(key) = &current_node.value() {
                    // Ensure the key is not empty before processing
                    if key.is_empty() {
                        return Err(TrieError::KeyLengthTooShort);
                    }
                    // Combine the key back to original form
                    let combined = combine_nibbles(key.as_slice());
                    // Append to leaf_keys
                    leaf_keys.push(combined);
                }

                // Backtrack: remove the leaf's char from paths (no children to explore)
                path.pop();
                relative_path.pop();
            } else {
                // Current node is not a leaf; prepare to descend into its children
                // Collect and sort child chars
                let mut children: Vec<u8> = current_node.children().keys().cloned().collect();
                // Sort ascending
                children.sort();
                // Reverse for pop() to yield smallest first
                children.reverse();
                // Push this new level onto the remaining_stack
                remaining_stack.push(children);
            }
        }
    }

    pub fn get_trie_node_metadata(
        &self,
        db: &RocksDB,
        txn_batch: &mut RocksDbTransactionBatch,
        prefix: &[u8],
    ) -> Result<NodeMetadata, TrieError> {
        if let Some(node) = self.get_node(db, txn_batch, prefix) {
            let mut children = HashMap::new();

            for char in node.children().keys() {
                let mut child_prefix = expand_nibbles(prefix);
                child_prefix.push(*char);
                let node_key = TrieNode::make_primary_key(&child_prefix, None);

                let child_node = Self::get_node_from_txn_or_db(db, txn_batch, &node_key).ok_or(
                    TrieError::ChildNotFound {
                        char: *char,
                        prefix: prefix.to_vec(),
                    },
                )?;

                children.insert(
                    *char,
                    NodeMetadata {
                        prefix: child_prefix,
                        num_messages: child_node.items(),
                        hash: hex::encode(&child_node.hash()),
                        children: HashMap::new(),
                    },
                );
            }

            Ok(NodeMetadata {
                prefix: prefix.to_vec(),
                num_messages: node.items(),
                hash: hex::encode(&node.hash()),
                children,
            })
        } else {
            Err(TrieError::NodeNotFound {
                prefix: prefix.to_vec(),
            })
        }
    }

    pub fn evict_stale(&mut self, max_idle: std::time::Duration) {
        if let Some(root) = self.root.as_mut() {
            root.evict_stale_children(max_idle);
        }
    }

    #[cfg(test)]
    pub fn get_root_node(&mut self) -> Option<&mut TrieNode> {
        self.root.as_mut()
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::db::{RocksDB, RocksDbTransactionBatch};
    use crate::storage::trie::merkle_trie::{Context, MerkleTrie, TrieKey};
    use crate::storage::trie::trie_node::UNCOMPACTED_LENGTH;
    use crate::storage::trie::util::expand_nibbles;
    use std::collections::HashSet;

    #[test]
    fn test_merkle_trie_get_node() {
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

        let key1 = b"12345\x00".to_vec();
        trie.insert(ctx, db, &mut txn_batch, vec![&key1.clone()])
            .unwrap();

        let node = trie.get_node(db, &mut txn_batch, &key1).unwrap();
        assert_eq!(node.value().unwrap(), expand_nibbles(&key1));

        // Add another key
        let key2 = b"12345\x80".to_vec();
        trie.insert(ctx, db, &mut txn_batch, vec![&key2.clone()])
            .unwrap();

        // The get node should still work for both keys
        let node = trie.get_node(db, &mut txn_batch, &key1).unwrap();
        assert_eq!(node.value().unwrap(), expand_nibbles(&key1));
        let node = trie.get_node(db, &mut txn_batch, &key2).unwrap();
        assert_eq!(node.value().unwrap(), expand_nibbles(&key2));

        // Getting the node with first 5 bytes should return the node with key1
        let common_node = trie
            .get_node(db, &mut txn_batch, &key1[0..5].to_vec())
            .unwrap();

        assert_eq!(common_node.is_leaf(), false);
        assert_eq!(common_node.items(), 2);
        assert_eq!(common_node.children().len(), 2);
        let mut children_keys: Vec<_> = common_node.children().keys().collect();
        children_keys.sort();

        assert_eq!(*children_keys[0], expand_nibbles(&key1)[2 * 5]);
        assert_eq!(*children_keys[1], expand_nibbles(&key2)[2 * 5]);

        // Get the metadata for the root node
        let root_metadata = trie
            .get_trie_node_metadata(db, &mut txn_batch, &key1[0..1])
            .unwrap();
        assert_eq!(root_metadata.prefix, "1".bytes().collect::<Vec<_>>());
        assert_eq!(root_metadata.num_messages, 2);
        assert_eq!(root_metadata.children.len(), 1);

        let metadata = trie
            .get_trie_node_metadata(db, &mut txn_batch, &key1[0..5])
            .unwrap();

        // Get the children
        let mut children = metadata
            .children
            .into_iter()
            .map(|(k, v)| (k, v))
            .collect::<Vec<_>>();
        children.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(children[0].0, expand_nibbles(&key1)[2 * 5]);
        assert_eq!(children[0].1.prefix, expand_nibbles(&key1)[..11]);
        assert_eq!(children[0].1.num_messages, 1);

        assert_eq!(children[1].0, expand_nibbles(&key2)[2 * 5]);
        assert_eq!(children[1].1.prefix, expand_nibbles(&key2)[..11]);
        assert_eq!(children[1].1.num_messages, 1);

        db.close();

        // Clean up
        std::fs::remove_dir_all(&tmp_path).unwrap();
    }

    #[test]
    fn test_trie_keys_matches_uncompacted_length() {
        // The trie key for a message type should equal the uncompacted length in nibbles (due to the branching factor).
        // Compacting the keys before the message type is not supported and can cause weird bugs.
        assert_eq!(
            TrieKey::for_message_type(123, 1).len(),
            UNCOMPACTED_LENGTH / 2
        );
    }

    #[test]
    fn test_trie_shard_routing() {
        let mut map = HashSet::new();
        for i in 0..10000 {
            let shard = TrieKey::fid_shard(i);
            map.insert(shard);
        }
        // Ensure all 256 shards are used
        assert_eq!(map.len(), 256);

        // Verify the shard for a few fids, and ensure the hashing function doesn't change
        // IF THE SHARD ASSIGNMENT CHANGES, YOU WILL BREAK THE MERKLE TRIE
        assert_eq!(TrieKey::fid_shard(0), 152);
        assert_eq!(TrieKey::fid_shard(1), 168);
        assert_eq!(TrieKey::fid_shard(100), 89);
        assert_eq!(TrieKey::fid_shard(42), 141);
        assert_eq!(TrieKey::fid_shard(918648237462), 153);
    }
}
