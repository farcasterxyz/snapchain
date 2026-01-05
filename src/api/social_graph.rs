//! Social Graph Indexer for tracking follow relationships.
//!
//! Maintains efficient indexes for:
//! - Follower/following counts per FID
//! - Quick mutual follow detection
//! - Relationship queries

use crate::api::config::FeatureConfig;
use crate::api::events::IndexEvent;
use crate::api::indexer::{Indexer, IndexerError, IndexerStats};
use crate::proto::link_body::Target;
use crate::proto::message_data::Body;
use crate::proto::{Message, MessageType};
use crate::storage::db::{RocksDB, RocksDbTransactionBatch};
use async_trait::async_trait;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Key prefixes for social graph index data.
/// These are stored in the main RocksDB instance with a distinct prefix.
mod keys {
    /// Prefix for all social graph index keys.
    pub const SOCIAL_GRAPH_PREFIX: u8 = 0xE0;

    /// Follower count: <prefix><0x01><fid:8> -> count:8
    pub const FOLLOWER_COUNT: u8 = 0x01;

    /// Following count: <prefix><0x02><fid:8> -> count:8
    pub const FOLLOWING_COUNT: u8 = 0x02;

    /// Followers list: <prefix><0x03><fid:8><follower_fid:8> -> timestamp:4
    /// Allows iteration over all followers of a FID
    pub const FOLLOWERS_BY_FID: u8 = 0x03;

    /// Following list: <prefix><0x04><fid:8><following_fid:8> -> timestamp:4
    /// Allows iteration over all FIDs that this user follows
    pub const FOLLOWING_BY_FID: u8 = 0x04;

    /// Checkpoint: <prefix><0xFF> -> event_id:8
    pub const CHECKPOINT: u8 = 0xFF;
}

/// Social graph indexer that tracks follow relationships.
pub struct SocialGraphIndexer {
    config: FeatureConfig,
    db: Arc<RocksDB>,
    checkpoint: AtomicU64,
    items_indexed: AtomicU64,
}

impl SocialGraphIndexer {
    pub fn new(config: FeatureConfig, db: Arc<RocksDB>) -> Self {
        // Load checkpoint from DB
        let checkpoint = Self::load_checkpoint(&db).unwrap_or(0);

        Self {
            config,
            db,
            checkpoint: AtomicU64::new(checkpoint),
            items_indexed: AtomicU64::new(0),
        }
    }

    /// Get the number of followers for a FID.
    pub fn get_follower_count(&self, fid: u64) -> Result<u64, IndexerError> {
        let key = Self::make_follower_count_key(fid);
        match self.db.get(&key) {
            Ok(Some(value)) => {
                if value.len() == 8 {
                    Ok(u64::from_be_bytes(value.try_into().unwrap()))
                } else {
                    Ok(0)
                }
            }
            Ok(None) => Ok(0),
            Err(e) => Err(IndexerError::Storage(e.to_string())),
        }
    }

    /// Get the number of users this FID is following.
    pub fn get_following_count(&self, fid: u64) -> Result<u64, IndexerError> {
        let key = Self::make_following_count_key(fid);
        match self.db.get(&key) {
            Ok(Some(value)) => {
                if value.len() == 8 {
                    Ok(u64::from_be_bytes(value.try_into().unwrap()))
                } else {
                    Ok(0)
                }
            }
            Ok(None) => Ok(0),
            Err(e) => Err(IndexerError::Storage(e.to_string())),
        }
    }

    /// Check if two FIDs mutually follow each other.
    /// This is a convenience method that checks both directions.
    pub fn are_mutual_follows(&self, fid_a: u64, fid_b: u64) -> Result<bool, IndexerError> {
        // Check if fid_a follows fid_b
        let key_a_follows_b = Self::make_following_key(fid_a, fid_b);
        let a_follows_b = self
            .db
            .get(&key_a_follows_b)
            .map_err(|e| IndexerError::Storage(e.to_string()))?
            .is_some();

        if !a_follows_b {
            return Ok(false);
        }

        // Check if fid_b follows fid_a
        let key_b_follows_a = Self::make_following_key(fid_b, fid_a);
        let b_follows_a = self
            .db
            .get(&key_b_follows_a)
            .map_err(|e| IndexerError::Storage(e.to_string()))?
            .is_some();

        Ok(b_follows_a)
    }

    /// Get paginated list of followers for a FID.
    ///
    /// # Arguments
    /// * `fid` - The FID to get followers for
    /// * `cursor` - Optional cursor (follower FID) to start after for pagination
    /// * `limit` - Maximum number of results to return
    ///
    /// # Returns
    /// A tuple of (follower_fids, next_cursor) where next_cursor is Some if more results exist
    pub fn get_followers(
        &self,
        fid: u64,
        cursor: Option<u64>,
        limit: usize,
    ) -> Result<(Vec<u64>, Option<u64>), IndexerError> {
        let prefix = Self::make_follower_prefix(fid);
        let stop_prefix = Self::increment_prefix(&prefix);

        // Build start key based on cursor
        let start_key = if let Some(cursor_fid) = cursor {
            // Start after the cursor (exclusive)
            Self::make_follower_key(fid, cursor_fid + 1)
        } else {
            prefix.clone()
        };

        let mut results = Vec::with_capacity(limit + 1);

        let page_options = crate::storage::db::PageOptions {
            page_size: Some(limit + 1), // Get one extra to detect if there are more
            page_token: None,
            reverse: false,
        };

        self.db
            .for_each_iterator_by_prefix_paged(
                Some(start_key),
                Some(stop_prefix),
                &page_options,
                |key, _value| {
                    // Extract follower FID from key (last 8 bytes after prefix+type+target_fid)
                    if key.len() == 18 {
                        let follower_fid = u64::from_be_bytes(key[10..18].try_into().unwrap());
                        results.push(follower_fid);
                    }
                    // Continue until we have limit + 1
                    Ok(results.len() > limit)
                },
            )
            .map_err(|e| IndexerError::Storage(e.to_string()))?;

        // Determine cursor for next page
        let next_cursor = if results.len() > limit {
            results.pop(); // Remove the extra item
            results.last().copied()
        } else {
            None
        };

        Ok((results, next_cursor))
    }

    /// Get paginated list of users this FID is following.
    ///
    /// # Arguments
    /// * `fid` - The FID to get following for
    /// * `cursor` - Optional cursor (following FID) to start after for pagination
    /// * `limit` - Maximum number of results to return
    ///
    /// # Returns
    /// A tuple of (following_fids, next_cursor) where next_cursor is Some if more results exist
    pub fn get_following(
        &self,
        fid: u64,
        cursor: Option<u64>,
        limit: usize,
    ) -> Result<(Vec<u64>, Option<u64>), IndexerError> {
        let prefix = Self::make_following_prefix(fid);
        let stop_prefix = Self::increment_prefix(&prefix);

        // Build start key based on cursor
        let start_key = if let Some(cursor_fid) = cursor {
            // Start after the cursor (exclusive)
            Self::make_following_key(fid, cursor_fid + 1)
        } else {
            prefix.clone()
        };

        let mut results = Vec::with_capacity(limit + 1);

        let page_options = crate::storage::db::PageOptions {
            page_size: Some(limit + 1), // Get one extra to detect if there are more
            page_token: None,
            reverse: false,
        };

        self.db
            .for_each_iterator_by_prefix_paged(
                Some(start_key),
                Some(stop_prefix),
                &page_options,
                |key, _value| {
                    // Extract following FID from key (last 8 bytes after prefix+type+source_fid)
                    if key.len() == 18 {
                        let following_fid = u64::from_be_bytes(key[10..18].try_into().unwrap());
                        results.push(following_fid);
                    }
                    // Continue until we have limit + 1
                    Ok(results.len() > limit)
                },
            )
            .map_err(|e| IndexerError::Storage(e.to_string()))?;

        // Determine cursor for next page
        let next_cursor = if results.len() > limit {
            results.pop(); // Remove the extra item
            results.last().copied()
        } else {
            None
        };

        Ok((results, next_cursor))
    }

    /// Increment a prefix to create an exclusive upper bound.
    fn increment_prefix(prefix: &[u8]) -> Vec<u8> {
        let mut result = prefix.to_vec();
        // Add 1 to the last byte, handling overflow
        for i in (0..result.len()).rev() {
            if result[i] < 255 {
                result[i] += 1;
                break;
            } else {
                result[i] = 0;
            }
        }
        result
    }

    /// Process a link message (add or remove).
    fn process_link_message(
        &self,
        message: &Message,
        txn: &mut RocksDbTransactionBatch,
    ) -> Result<(), IndexerError> {
        let data = message
            .data
            .as_ref()
            .ok_or_else(|| IndexerError::InvalidData("message has no data".into()))?;

        let link_body = match &data.body {
            Some(Body::LinkBody(body)) => body,
            _ => return Ok(()), // Not a link message
        };

        // Only process "follow" type links
        if link_body.r#type != "follow" {
            return Ok(());
        }

        let target_fid = match &link_body.target {
            Some(Target::TargetFid(fid)) => *fid,
            None => return Ok(()), // No target
        };

        let source_fid = data.fid;
        let msg_type = MessageType::try_from(data.r#type)
            .map_err(|_| IndexerError::InvalidData("invalid message type".into()))?;

        match msg_type {
            MessageType::LinkAdd => {
                // Update counts
                self.increment_follower_count(target_fid, txn)?;
                self.increment_following_count(source_fid, txn)?;

                // Store the actual relationship
                // Follower entry: target_fid has follower source_fid
                let follower_key = Self::make_follower_key(target_fid, source_fid);
                let timestamp = data.timestamp.to_be_bytes().to_vec();
                txn.put(follower_key, timestamp.clone());

                // Following entry: source_fid follows target_fid
                let following_key = Self::make_following_key(source_fid, target_fid);
                txn.put(following_key, timestamp);

                self.items_indexed.fetch_add(1, Ordering::Relaxed);
            }
            MessageType::LinkRemove => {
                // Update counts
                self.decrement_follower_count(target_fid, txn)?;
                self.decrement_following_count(source_fid, txn)?;

                // Remove the relationship entries
                let follower_key = Self::make_follower_key(target_fid, source_fid);
                txn.delete(follower_key);

                let following_key = Self::make_following_key(source_fid, target_fid);
                txn.delete(following_key);
            }
            _ => {}
        }

        Ok(())
    }

    fn increment_follower_count(
        &self,
        fid: u64,
        txn: &mut RocksDbTransactionBatch,
    ) -> Result<(), IndexerError> {
        let key = Self::make_follower_count_key(fid);
        let current = self.get_count_from_txn_or_db(&key, txn)?;
        txn.put(key, (current + 1).to_be_bytes().to_vec());
        Ok(())
    }

    fn decrement_follower_count(
        &self,
        fid: u64,
        txn: &mut RocksDbTransactionBatch,
    ) -> Result<(), IndexerError> {
        let key = Self::make_follower_count_key(fid);
        let current = self.get_count_from_txn_or_db(&key, txn)?;
        if current > 0 {
            txn.put(key, (current - 1).to_be_bytes().to_vec());
        }
        Ok(())
    }

    fn increment_following_count(
        &self,
        fid: u64,
        txn: &mut RocksDbTransactionBatch,
    ) -> Result<(), IndexerError> {
        let key = Self::make_following_count_key(fid);
        let current = self.get_count_from_txn_or_db(&key, txn)?;
        txn.put(key, (current + 1).to_be_bytes().to_vec());
        Ok(())
    }

    fn decrement_following_count(
        &self,
        fid: u64,
        txn: &mut RocksDbTransactionBatch,
    ) -> Result<(), IndexerError> {
        let key = Self::make_following_count_key(fid);
        let current = self.get_count_from_txn_or_db(&key, txn)?;
        if current > 0 {
            txn.put(key, (current - 1).to_be_bytes().to_vec());
        }
        Ok(())
    }

    fn get_count_from_txn_or_db(
        &self,
        key: &[u8],
        txn: &RocksDbTransactionBatch,
    ) -> Result<u64, IndexerError> {
        // First check if there's a pending write in the transaction
        if let Some(Some(value)) = txn.batch.get(key) {
            if value.len() == 8 {
                return Ok(u64::from_be_bytes(value.as_slice().try_into().unwrap()));
            }
        }

        // Fall back to DB
        match self.db.get(key) {
            Ok(Some(value)) if value.len() == 8 => {
                Ok(u64::from_be_bytes(value.try_into().unwrap()))
            }
            Ok(_) => Ok(0),
            Err(e) => Err(IndexerError::Storage(e.to_string())),
        }
    }

    fn make_follower_count_key(fid: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(10);
        key.push(keys::SOCIAL_GRAPH_PREFIX);
        key.push(keys::FOLLOWER_COUNT);
        key.extend_from_slice(&fid.to_be_bytes());
        key
    }

    fn make_following_count_key(fid: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(10);
        key.push(keys::SOCIAL_GRAPH_PREFIX);
        key.push(keys::FOLLOWING_COUNT);
        key.extend_from_slice(&fid.to_be_bytes());
        key
    }

    /// Key for storing a follower relationship.
    /// Format: <prefix><0x03><target_fid:8><follower_fid:8>
    fn make_follower_key(target_fid: u64, follower_fid: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(18);
        key.push(keys::SOCIAL_GRAPH_PREFIX);
        key.push(keys::FOLLOWERS_BY_FID);
        key.extend_from_slice(&target_fid.to_be_bytes());
        key.extend_from_slice(&follower_fid.to_be_bytes());
        key
    }

    /// Key prefix for iterating all followers of a FID.
    fn make_follower_prefix(fid: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(10);
        key.push(keys::SOCIAL_GRAPH_PREFIX);
        key.push(keys::FOLLOWERS_BY_FID);
        key.extend_from_slice(&fid.to_be_bytes());
        key
    }

    /// Key for storing a following relationship.
    /// Format: <prefix><0x04><source_fid:8><target_fid:8>
    fn make_following_key(source_fid: u64, target_fid: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(18);
        key.push(keys::SOCIAL_GRAPH_PREFIX);
        key.push(keys::FOLLOWING_BY_FID);
        key.extend_from_slice(&source_fid.to_be_bytes());
        key.extend_from_slice(&target_fid.to_be_bytes());
        key
    }

    /// Key prefix for iterating all users a FID follows.
    fn make_following_prefix(fid: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(10);
        key.push(keys::SOCIAL_GRAPH_PREFIX);
        key.push(keys::FOLLOWING_BY_FID);
        key.extend_from_slice(&fid.to_be_bytes());
        key
    }

    fn make_checkpoint_key() -> Vec<u8> {
        vec![keys::SOCIAL_GRAPH_PREFIX, keys::CHECKPOINT]
    }

    fn load_checkpoint(db: &RocksDB) -> Result<u64, IndexerError> {
        let key = Self::make_checkpoint_key();
        match db.get(&key) {
            Ok(Some(value)) if value.len() == 8 => {
                Ok(u64::from_be_bytes(value.try_into().unwrap()))
            }
            Ok(_) => Ok(0),
            Err(e) => Err(IndexerError::Storage(e.to_string())),
        }
    }
}

#[async_trait]
impl Indexer for SocialGraphIndexer {
    fn name(&self) -> &'static str {
        "social_graph"
    }

    fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    async fn process_event(&self, event: &IndexEvent) -> Result<(), IndexerError> {
        match event {
            IndexEvent::MessageCommitted { message, .. } => {
                let mut txn = RocksDbTransactionBatch::new();
                self.process_link_message(message, &mut txn)?;
                if txn.len() > 0 {
                    self.db
                        .commit(txn)
                        .map_err(|e| IndexerError::Storage(e.to_string()))?;
                }
            }
            IndexEvent::MessagesCommitted { messages, .. } => {
                let mut txn = RocksDbTransactionBatch::new();
                for message in messages {
                    self.process_link_message(message, &mut txn)?;
                }
                if txn.len() > 0 {
                    self.db
                        .commit(txn)
                        .map_err(|e| IndexerError::Storage(e.to_string()))?;
                }
            }
            _ => {
                // Other event types not relevant for social graph
            }
        }
        Ok(())
    }

    async fn process_batch(&self, events: &[IndexEvent]) -> Result<(), IndexerError> {
        let mut txn = RocksDbTransactionBatch::new();

        for event in events {
            match event {
                IndexEvent::MessageCommitted { message, .. } => {
                    self.process_link_message(message, &mut txn)?;
                }
                IndexEvent::MessagesCommitted { messages, .. } => {
                    for message in messages {
                        self.process_link_message(message, &mut txn)?;
                    }
                }
                _ => {}
            }
        }

        if txn.len() > 0 {
            self.db
                .commit(txn)
                .map_err(|e| IndexerError::Storage(e.to_string()))?;
        }

        Ok(())
    }

    fn last_checkpoint(&self) -> u64 {
        self.checkpoint.load(Ordering::SeqCst)
    }

    async fn save_checkpoint(&self, event_id: u64) -> Result<(), IndexerError> {
        let key = Self::make_checkpoint_key();
        let mut txn = RocksDbTransactionBatch::new();
        txn.put(key, event_id.to_be_bytes().to_vec());
        self.db
            .commit(txn)
            .map_err(|e| IndexerError::Storage(e.to_string()))?;
        self.checkpoint.store(event_id, Ordering::SeqCst);
        Ok(())
    }

    fn stats(&self) -> IndexerStats {
        IndexerStats {
            items_indexed: self.items_indexed.load(Ordering::Relaxed),
            last_event_id: self.checkpoint.load(Ordering::SeqCst),
            last_block_height: 0, // TODO: track block height
            backfill_complete: self.checkpoint.load(Ordering::SeqCst) > 0,
            size_bytes: 0, // TODO: estimate size
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{LinkBody, MessageData};

    fn make_test_db() -> Arc<RocksDB> {
        let tmp_path = tempfile::tempdir()
            .unwrap()
            .path()
            .as_os_str()
            .to_str()
            .unwrap()
            .to_string();
        let db = RocksDB::new(&tmp_path);
        db.open().unwrap();
        Arc::new(db)
    }

    fn make_follow_message(from_fid: u64, to_fid: u64, is_add: bool) -> Message {
        Message {
            data: Some(MessageData {
                fid: from_fid,
                r#type: if is_add {
                    MessageType::LinkAdd as i32
                } else {
                    MessageType::LinkRemove as i32
                },
                body: Some(Body::LinkBody(LinkBody {
                    r#type: "follow".to_string(),
                    target: Some(Target::TargetFid(to_fid)),
                    ..Default::default()
                })),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_follow_increments_counts() {
        let db = make_test_db();
        let config = FeatureConfig {
            enabled: true,
            ..Default::default()
        };
        let indexer = SocialGraphIndexer::new(config, db);

        // User 1 follows User 2
        let msg = make_follow_message(1, 2, true);
        let event = IndexEvent::message(msg, 0, 1);
        indexer.process_event(&event).await.unwrap();

        assert_eq!(indexer.get_following_count(1).unwrap(), 1);
        assert_eq!(indexer.get_follower_count(2).unwrap(), 1);
        assert_eq!(indexer.get_following_count(2).unwrap(), 0);
        assert_eq!(indexer.get_follower_count(1).unwrap(), 0);
    }

    #[tokio::test]
    async fn test_unfollow_decrements_counts() {
        let db = make_test_db();
        let config = FeatureConfig {
            enabled: true,
            ..Default::default()
        };
        let indexer = SocialGraphIndexer::new(config, db);

        // User 1 follows User 2
        let follow = make_follow_message(1, 2, true);
        indexer
            .process_event(&IndexEvent::message(follow, 0, 1))
            .await
            .unwrap();

        // User 1 unfollows User 2
        let unfollow = make_follow_message(1, 2, false);
        indexer
            .process_event(&IndexEvent::message(unfollow, 0, 2))
            .await
            .unwrap();

        assert_eq!(indexer.get_following_count(1).unwrap(), 0);
        assert_eq!(indexer.get_follower_count(2).unwrap(), 0);
    }

    #[tokio::test]
    async fn test_multiple_follows() {
        let db = make_test_db();
        let config = FeatureConfig {
            enabled: true,
            ..Default::default()
        };
        let indexer = SocialGraphIndexer::new(config, db);

        // User 1 follows Users 2, 3, 4
        for target in [2, 3, 4] {
            let msg = make_follow_message(1, target, true);
            indexer
                .process_event(&IndexEvent::message(msg, 0, 1))
                .await
                .unwrap();
        }

        // Users 5, 6 follow User 1
        for source in [5, 6] {
            let msg = make_follow_message(source, 1, true);
            indexer
                .process_event(&IndexEvent::message(msg, 0, 1))
                .await
                .unwrap();
        }

        assert_eq!(indexer.get_following_count(1).unwrap(), 3);
        assert_eq!(indexer.get_follower_count(1).unwrap(), 2);
    }

    #[tokio::test]
    async fn test_batch_processing() {
        let db = make_test_db();
        let config = FeatureConfig {
            enabled: true,
            ..Default::default()
        };
        let indexer = SocialGraphIndexer::new(config, db);

        let events: Vec<IndexEvent> = vec![
            IndexEvent::message(make_follow_message(1, 2, true), 0, 1),
            IndexEvent::message(make_follow_message(1, 3, true), 0, 1),
            IndexEvent::message(make_follow_message(2, 1, true), 0, 1),
        ];

        indexer.process_batch(&events).await.unwrap();

        assert_eq!(indexer.get_following_count(1).unwrap(), 2);
        assert_eq!(indexer.get_follower_count(1).unwrap(), 1);
        assert_eq!(indexer.get_follower_count(2).unwrap(), 1);
    }

    #[tokio::test]
    async fn test_checkpoint_persistence() {
        let db = make_test_db();
        let config = FeatureConfig {
            enabled: true,
            ..Default::default()
        };

        {
            let indexer = SocialGraphIndexer::new(config.clone(), db.clone());
            indexer.save_checkpoint(12345).await.unwrap();
            assert_eq!(indexer.last_checkpoint(), 12345);
        }

        // Create new indexer, should load checkpoint from DB
        {
            let indexer = SocialGraphIndexer::new(config, db);
            assert_eq!(indexer.last_checkpoint(), 12345);
        }
    }

    #[test]
    fn test_disabled_indexer() {
        let db = make_test_db();
        let config = FeatureConfig {
            enabled: false,
            ..Default::default()
        };
        let indexer = SocialGraphIndexer::new(config, db);
        assert!(!indexer.is_enabled());
    }

    #[tokio::test]
    async fn test_get_followers_list() {
        let db = make_test_db();
        let config = FeatureConfig {
            enabled: true,
            ..Default::default()
        };
        let indexer = SocialGraphIndexer::new(config, db);

        // Users 10, 20, 30, 40 follow User 1
        for follower in [10, 20, 30, 40] {
            let msg = make_follow_message(follower, 1, true);
            indexer
                .process_event(&IndexEvent::message(msg, 0, 1))
                .await
                .unwrap();
        }

        // Get all followers
        let (followers, next_cursor) = indexer.get_followers(1, None, 10).unwrap();
        assert_eq!(followers, vec![10, 20, 30, 40]);
        assert!(next_cursor.is_none());

        // Get with pagination
        let (followers, next_cursor) = indexer.get_followers(1, None, 2).unwrap();
        assert_eq!(followers, vec![10, 20]);
        assert!(next_cursor.is_some());

        // Get next page
        let (followers, next_cursor) = indexer.get_followers(1, next_cursor, 2).unwrap();
        assert_eq!(followers, vec![30, 40]);
        assert!(next_cursor.is_none());
    }

    #[tokio::test]
    async fn test_get_following_list() {
        let db = make_test_db();
        let config = FeatureConfig {
            enabled: true,
            ..Default::default()
        };
        let indexer = SocialGraphIndexer::new(config, db);

        // User 1 follows Users 10, 20, 30, 40
        for target in [10, 20, 30, 40] {
            let msg = make_follow_message(1, target, true);
            indexer
                .process_event(&IndexEvent::message(msg, 0, 1))
                .await
                .unwrap();
        }

        // Get all following
        let (following, next_cursor) = indexer.get_following(1, None, 10).unwrap();
        assert_eq!(following, vec![10, 20, 30, 40]);
        assert!(next_cursor.is_none());

        // Get with pagination
        let (following, next_cursor) = indexer.get_following(1, None, 2).unwrap();
        assert_eq!(following, vec![10, 20]);
        assert!(next_cursor.is_some());

        // Get next page
        let (following, next_cursor) = indexer.get_following(1, next_cursor, 2).unwrap();
        assert_eq!(following, vec![30, 40]);
        assert!(next_cursor.is_none());
    }

    #[tokio::test]
    async fn test_mutual_follows() {
        let db = make_test_db();
        let config = FeatureConfig {
            enabled: true,
            ..Default::default()
        };
        let indexer = SocialGraphIndexer::new(config, db);

        // User 1 follows User 2
        let msg = make_follow_message(1, 2, true);
        indexer
            .process_event(&IndexEvent::message(msg, 0, 1))
            .await
            .unwrap();

        // Not mutual yet
        assert!(!indexer.are_mutual_follows(1, 2).unwrap());

        // User 2 follows User 1 back
        let msg = make_follow_message(2, 1, true);
        indexer
            .process_event(&IndexEvent::message(msg, 0, 2))
            .await
            .unwrap();

        // Now mutual
        assert!(indexer.are_mutual_follows(1, 2).unwrap());
        assert!(indexer.are_mutual_follows(2, 1).unwrap());
    }

    #[tokio::test]
    async fn test_unfollow_removes_from_list() {
        let db = make_test_db();
        let config = FeatureConfig {
            enabled: true,
            ..Default::default()
        };
        let indexer = SocialGraphIndexer::new(config, db);

        // User 10 and 20 follow User 1
        for follower in [10, 20] {
            let msg = make_follow_message(follower, 1, true);
            indexer
                .process_event(&IndexEvent::message(msg, 0, 1))
                .await
                .unwrap();
        }

        let (followers, _) = indexer.get_followers(1, None, 10).unwrap();
        assert_eq!(followers, vec![10, 20]);

        // User 10 unfollows User 1
        let msg = make_follow_message(10, 1, false);
        indexer
            .process_event(&IndexEvent::message(msg, 0, 2))
            .await
            .unwrap();

        let (followers, _) = indexer.get_followers(1, None, 10).unwrap();
        assert_eq!(followers, vec![20]);
    }
}
