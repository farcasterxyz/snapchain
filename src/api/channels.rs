//! Channels Indexer for tracking Farcaster channel activity.
//!
//! Maintains indexes for:
//! - Channel registry (known channels with URLs)
//! - Cast counts per channel
//! - Unique user counts per channel
//! - Channel member lists

use crate::api::config::FeatureConfig;
use crate::api::events::IndexEvent;
use crate::api::indexer::{Indexer, IndexerError, IndexerStats};
use crate::proto::cast_add_body::Parent;
use crate::proto::message_data::Body;
use crate::proto::{Message, MessageType};
use crate::storage::db::{PageOptions, RocksDB, RocksDbTransactionBatch};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Key prefixes for channel index data.
mod keys {
    /// Prefix for all channel index keys.
    pub const CHANNEL_PREFIX: u8 = 0xE1;

    /// Channel stats: <prefix><0x01><url_hash:32> -> ChannelStats (serialized)
    pub const CHANNEL_STATS: u8 = 0x01;

    /// Channel members: <prefix><0x02><url_hash:32><fid:8> -> timestamp:4
    pub const CHANNEL_MEMBERS: u8 = 0x02;

    /// FID channels: <prefix><0x03><fid:8><url_hash:32> -> 1 (for reverse lookup)
    pub const FID_CHANNELS: u8 = 0x03;

    /// Channel URL lookup: <prefix><0x04><url_hash:32> -> url (full URL string)
    pub const CHANNEL_URL: u8 = 0x04;

    /// Checkpoint: <prefix><0xFF> -> event_id:8
    pub const CHECKPOINT: u8 = 0xFF;
}

/// Statistics for a channel.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChannelStats {
    /// Total number of casts in this channel.
    pub cast_count: u64,
    /// Number of unique users who have cast in this channel.
    pub member_count: u64,
    /// Timestamp of last activity.
    pub last_activity: u32,
}

/// Channel info returned by queries.
#[derive(Debug, Clone, Serialize)]
pub struct ChannelInfo {
    /// Channel URL (parent_url).
    pub url: String,
    /// Channel statistics.
    pub stats: ChannelStats,
}

/// Channels indexer that tracks channel activity.
pub struct ChannelsIndexer {
    config: FeatureConfig,
    db: Arc<RocksDB>,
    checkpoint: AtomicU64,
    channels_indexed: AtomicU64,
}

impl ChannelsIndexer {
    pub fn new(config: FeatureConfig, db: Arc<RocksDB>) -> Self {
        let checkpoint = Self::load_checkpoint(&db).unwrap_or(0);

        Self {
            config,
            db,
            checkpoint: AtomicU64::new(checkpoint),
            channels_indexed: AtomicU64::new(0),
        }
    }

    /// Get channel info by URL.
    pub fn get_channel(&self, url: &str) -> Result<Option<ChannelInfo>, IndexerError> {
        let url_hash = Self::hash_url(url);
        let stats_key = Self::make_channel_stats_key(&url_hash);

        match self.db.get(&stats_key) {
            Ok(Some(value)) => {
                let stats: ChannelStats = serde_json::from_slice(&value)
                    .map_err(|e| IndexerError::InvalidData(e.to_string()))?;
                Ok(Some(ChannelInfo {
                    url: url.to_string(),
                    stats,
                }))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(IndexerError::Storage(e.to_string())),
        }
    }

    /// Get channel stats by URL hash (for internal use).
    fn get_channel_stats(&self, url_hash: &[u8; 32]) -> Result<ChannelStats, IndexerError> {
        let key = Self::make_channel_stats_key(url_hash);
        match self.db.get(&key) {
            Ok(Some(value)) => {
                serde_json::from_slice(&value).map_err(|e| IndexerError::InvalidData(e.to_string()))
            }
            Ok(None) => Ok(ChannelStats::default()),
            Err(e) => Err(IndexerError::Storage(e.to_string())),
        }
    }

    /// Get stats from pending transaction or DB.
    fn get_channel_stats_from_txn(
        &self,
        url_hash: &[u8; 32],
        txn: &RocksDbTransactionBatch,
    ) -> Result<ChannelStats, IndexerError> {
        let key = Self::make_channel_stats_key(url_hash);

        // Check pending writes first
        if let Some(Some(value)) = txn.batch.get(&key) {
            return serde_json::from_slice(value)
                .map_err(|e| IndexerError::InvalidData(e.to_string()));
        }

        // Fall back to DB
        self.get_channel_stats(url_hash)
    }

    /// Check if a user is already a member of a channel.
    fn is_member(
        &self,
        url_hash: &[u8; 32],
        fid: u64,
        txn: &RocksDbTransactionBatch,
    ) -> Result<bool, IndexerError> {
        let key = Self::make_channel_member_key(url_hash, fid);

        // Check pending writes
        if let Some(maybe_value) = txn.batch.get(&key) {
            return Ok(maybe_value.is_some());
        }

        // Check DB
        match self.db.get(&key) {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => Err(IndexerError::Storage(e.to_string())),
        }
    }

    /// Get paginated list of channels a user has posted to.
    pub fn get_user_channels(
        &self,
        fid: u64,
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<(Vec<String>, Option<Vec<u8>>), IndexerError> {
        let prefix = Self::make_fid_channels_prefix(fid);
        let stop_prefix = Self::increment_prefix(&prefix);

        let start_key = if let Some(ref cursor_bytes) = cursor {
            let mut key = prefix.clone();
            key.extend_from_slice(cursor_bytes);
            // Increment to start after cursor
            Self::increment_prefix(&key)
        } else {
            prefix.clone()
        };

        let mut results = Vec::with_capacity(limit + 1);
        let mut url_hashes = Vec::with_capacity(limit + 1);

        let page_options = PageOptions {
            page_size: Some(limit + 1),
            page_token: None,
            reverse: false,
        };

        self.db
            .for_each_iterator_by_prefix_paged(
                Some(start_key),
                Some(stop_prefix),
                &page_options,
                |key, _value| {
                    // Extract url_hash from key (last 32 bytes)
                    if key.len() >= 41 {
                        // prefix(1) + type(1) + fid(8) + url_hash(32) = 42, but we skip first byte
                        let url_hash: [u8; 32] = key[10..42].try_into().unwrap();
                        url_hashes.push(url_hash);
                    }
                    Ok(url_hashes.len() > limit)
                },
            )
            .map_err(|e| IndexerError::Storage(e.to_string()))?;

        // Resolve URL hashes to actual URLs
        for url_hash in &url_hashes {
            if results.len() >= limit {
                break;
            }
            if let Some(url) = self.get_channel_url(url_hash)? {
                results.push(url);
            }
        }

        // Determine cursor for next page
        let next_cursor = if url_hashes.len() > limit {
            url_hashes.get(limit - 1).map(|h| h.to_vec())
        } else {
            None
        };

        Ok((results, next_cursor))
    }

    /// Get paginated list of members for a channel.
    pub fn get_channel_members(
        &self,
        url: &str,
        cursor: Option<u64>,
        limit: usize,
    ) -> Result<(Vec<u64>, Option<u64>), IndexerError> {
        let url_hash = Self::hash_url(url);
        let prefix = Self::make_channel_members_prefix(&url_hash);
        let stop_prefix = Self::increment_prefix(&prefix);

        let start_key = if let Some(cursor_fid) = cursor {
            Self::make_channel_member_key(&url_hash, cursor_fid + 1)
        } else {
            prefix.clone()
        };

        let mut results = Vec::with_capacity(limit + 1);

        let page_options = PageOptions {
            page_size: Some(limit + 1),
            page_token: None,
            reverse: false,
        };

        self.db
            .for_each_iterator_by_prefix_paged(
                Some(start_key),
                Some(stop_prefix),
                &page_options,
                |key, _value| {
                    // Extract FID from key (last 8 bytes)
                    if key.len() == 42 {
                        let fid = u64::from_be_bytes(key[34..42].try_into().unwrap());
                        results.push(fid);
                    }
                    Ok(results.len() > limit)
                },
            )
            .map_err(|e| IndexerError::Storage(e.to_string()))?;

        let next_cursor = if results.len() > limit {
            results.pop();
            results.last().copied()
        } else {
            None
        };

        Ok((results, next_cursor))
    }

    /// Get the URL for a channel by its hash.
    fn get_channel_url(&self, url_hash: &[u8; 32]) -> Result<Option<String>, IndexerError> {
        let key = Self::make_channel_url_key(url_hash);
        match self.db.get(&key) {
            Ok(Some(value)) => String::from_utf8(value)
                .map(Some)
                .map_err(|e| IndexerError::InvalidData(e.to_string())),
            Ok(None) => Ok(None),
            Err(e) => Err(IndexerError::Storage(e.to_string())),
        }
    }

    /// Process a cast message to update channel indexes.
    fn process_cast_message(
        &self,
        message: &Message,
        txn: &mut RocksDbTransactionBatch,
    ) -> Result<(), IndexerError> {
        let data = message
            .data
            .as_ref()
            .ok_or_else(|| IndexerError::InvalidData("message has no data".into()))?;

        let cast_body = match &data.body {
            Some(Body::CastAddBody(body)) => body,
            _ => return Ok(()), // Not a cast message
        };

        let msg_type = MessageType::try_from(data.r#type)
            .map_err(|_| IndexerError::InvalidData("invalid message type".into()))?;

        // Only process CastAdd with parent_url (channel casts)
        if msg_type != MessageType::CastAdd {
            return Ok(());
        }

        let parent_url = match &cast_body.parent {
            Some(Parent::ParentUrl(url)) => url,
            _ => return Ok(()), // Not a channel cast
        };

        let fid = data.fid;
        let timestamp = data.timestamp;
        let url_hash = Self::hash_url(parent_url);

        // Store channel URL for reverse lookup
        let url_key = Self::make_channel_url_key(&url_hash);
        txn.put(url_key, parent_url.as_bytes().to_vec());

        // Check if user is already a member
        let was_member = self.is_member(&url_hash, fid, txn)?;

        // Get current stats
        let mut stats = self.get_channel_stats_from_txn(&url_hash, txn)?;

        // Update stats
        stats.cast_count += 1;
        if !was_member {
            stats.member_count += 1;
        }
        stats.last_activity = timestamp;

        // Store updated stats
        let stats_key = Self::make_channel_stats_key(&url_hash);
        let stats_value =
            serde_json::to_vec(&stats).map_err(|e| IndexerError::InvalidData(e.to_string()))?;
        txn.put(stats_key, stats_value);

        // Add member entry if new
        if !was_member {
            let member_key = Self::make_channel_member_key(&url_hash, fid);
            txn.put(member_key, timestamp.to_be_bytes().to_vec());

            // Add reverse lookup (fid -> channel)
            let fid_channel_key = Self::make_fid_channel_key(fid, &url_hash);
            txn.put(fid_channel_key, vec![1]);
        }

        self.channels_indexed.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Hash a URL to a fixed-size key component.
    fn hash_url(url: &str) -> [u8; 32] {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(url.as_bytes());
        hasher.finalize().into()
    }

    fn make_channel_stats_key(url_hash: &[u8; 32]) -> Vec<u8> {
        let mut key = Vec::with_capacity(34);
        key.push(keys::CHANNEL_PREFIX);
        key.push(keys::CHANNEL_STATS);
        key.extend_from_slice(url_hash);
        key
    }

    fn make_channel_url_key(url_hash: &[u8; 32]) -> Vec<u8> {
        let mut key = Vec::with_capacity(34);
        key.push(keys::CHANNEL_PREFIX);
        key.push(keys::CHANNEL_URL);
        key.extend_from_slice(url_hash);
        key
    }

    fn make_channel_member_key(url_hash: &[u8; 32], fid: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(42);
        key.push(keys::CHANNEL_PREFIX);
        key.push(keys::CHANNEL_MEMBERS);
        key.extend_from_slice(url_hash);
        key.extend_from_slice(&fid.to_be_bytes());
        key
    }

    fn make_channel_members_prefix(url_hash: &[u8; 32]) -> Vec<u8> {
        let mut key = Vec::with_capacity(34);
        key.push(keys::CHANNEL_PREFIX);
        key.push(keys::CHANNEL_MEMBERS);
        key.extend_from_slice(url_hash);
        key
    }

    fn make_fid_channel_key(fid: u64, url_hash: &[u8; 32]) -> Vec<u8> {
        let mut key = Vec::with_capacity(42);
        key.push(keys::CHANNEL_PREFIX);
        key.push(keys::FID_CHANNELS);
        key.extend_from_slice(&fid.to_be_bytes());
        key.extend_from_slice(url_hash);
        key
    }

    fn make_fid_channels_prefix(fid: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(10);
        key.push(keys::CHANNEL_PREFIX);
        key.push(keys::FID_CHANNELS);
        key.extend_from_slice(&fid.to_be_bytes());
        key
    }

    fn make_checkpoint_key() -> Vec<u8> {
        vec![keys::CHANNEL_PREFIX, keys::CHECKPOINT]
    }

    fn increment_prefix(prefix: &[u8]) -> Vec<u8> {
        let mut result = prefix.to_vec();
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
impl Indexer for ChannelsIndexer {
    fn name(&self) -> &'static str {
        "channels"
    }

    fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    async fn process_event(&self, event: &IndexEvent) -> Result<(), IndexerError> {
        match event {
            IndexEvent::MessageCommitted { message, .. } => {
                let mut txn = RocksDbTransactionBatch::new();
                self.process_cast_message(message, &mut txn)?;
                if txn.len() > 0 {
                    self.db
                        .commit(txn)
                        .map_err(|e| IndexerError::Storage(e.to_string()))?;
                }
            }
            IndexEvent::MessagesCommitted { messages, .. } => {
                let mut txn = RocksDbTransactionBatch::new();
                for message in messages {
                    self.process_cast_message(message, &mut txn)?;
                }
                if txn.len() > 0 {
                    self.db
                        .commit(txn)
                        .map_err(|e| IndexerError::Storage(e.to_string()))?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    async fn process_batch(&self, events: &[IndexEvent]) -> Result<(), IndexerError> {
        let mut txn = RocksDbTransactionBatch::new();

        for event in events {
            match event {
                IndexEvent::MessageCommitted { message, .. } => {
                    self.process_cast_message(message, &mut txn)?;
                }
                IndexEvent::MessagesCommitted { messages, .. } => {
                    for message in messages {
                        self.process_cast_message(message, &mut txn)?;
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
            items_indexed: self.channels_indexed.load(Ordering::Relaxed),
            last_event_id: self.checkpoint.load(Ordering::SeqCst),
            last_block_height: 0,
            backfill_complete: self.checkpoint.load(Ordering::SeqCst) > 0,
            size_bytes: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{CastAddBody, MessageData};

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

    fn make_channel_cast(fid: u64, channel_url: &str, timestamp: u32) -> Message {
        Message {
            data: Some(MessageData {
                fid,
                r#type: MessageType::CastAdd as i32,
                timestamp,
                body: Some(Body::CastAddBody(CastAddBody {
                    parent: Some(Parent::ParentUrl(channel_url.to_string())),
                    text: "Test cast".to_string(),
                    ..Default::default()
                })),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_channel_cast_creates_channel() {
        let db = make_test_db();
        let config = FeatureConfig {
            enabled: true,
            ..Default::default()
        };
        let indexer = ChannelsIndexer::new(config, db);

        let channel_url = "https://warpcast.com/~/channel/test";
        let msg = make_channel_cast(1, channel_url, 1000);
        let event = IndexEvent::message(msg, 0, 1);

        indexer.process_event(&event).await.unwrap();

        let info = indexer.get_channel(channel_url).unwrap();
        assert!(info.is_some());

        let info = info.unwrap();
        assert_eq!(info.url, channel_url);
        assert_eq!(info.stats.cast_count, 1);
        assert_eq!(info.stats.member_count, 1);
        assert_eq!(info.stats.last_activity, 1000);
    }

    #[tokio::test]
    async fn test_multiple_casts_increment_count() {
        let db = make_test_db();
        let config = FeatureConfig {
            enabled: true,
            ..Default::default()
        };
        let indexer = ChannelsIndexer::new(config, db);

        let channel_url = "https://warpcast.com/~/channel/test";

        // Same user casts twice
        for ts in [1000, 1001] {
            let msg = make_channel_cast(1, channel_url, ts);
            indexer
                .process_event(&IndexEvent::message(msg, 0, 1))
                .await
                .unwrap();
        }

        let info = indexer.get_channel(channel_url).unwrap().unwrap();
        assert_eq!(info.stats.cast_count, 2);
        assert_eq!(info.stats.member_count, 1); // Still only 1 unique member
    }

    #[tokio::test]
    async fn test_different_users_increment_members() {
        let db = make_test_db();
        let config = FeatureConfig {
            enabled: true,
            ..Default::default()
        };
        let indexer = ChannelsIndexer::new(config, db);

        let channel_url = "https://warpcast.com/~/channel/test";

        // Different users cast
        for fid in [1, 2, 3] {
            let msg = make_channel_cast(fid, channel_url, 1000);
            indexer
                .process_event(&IndexEvent::message(msg, 0, 1))
                .await
                .unwrap();
        }

        let info = indexer.get_channel(channel_url).unwrap().unwrap();
        assert_eq!(info.stats.cast_count, 3);
        assert_eq!(info.stats.member_count, 3);
    }

    #[tokio::test]
    async fn test_get_channel_members() {
        let db = make_test_db();
        let config = FeatureConfig {
            enabled: true,
            ..Default::default()
        };
        let indexer = ChannelsIndexer::new(config, db);

        let channel_url = "https://warpcast.com/~/channel/test";

        // Add members
        for fid in [10, 20, 30, 40] {
            let msg = make_channel_cast(fid, channel_url, 1000);
            indexer
                .process_event(&IndexEvent::message(msg, 0, 1))
                .await
                .unwrap();
        }

        let (members, next_cursor) = indexer.get_channel_members(channel_url, None, 10).unwrap();
        assert_eq!(members, vec![10, 20, 30, 40]);
        assert!(next_cursor.is_none());

        // Test pagination
        let (members, next_cursor) = indexer.get_channel_members(channel_url, None, 2).unwrap();
        assert_eq!(members, vec![10, 20]);
        assert!(next_cursor.is_some());
    }

    #[tokio::test]
    async fn test_get_user_channels() {
        let db = make_test_db();
        let config = FeatureConfig {
            enabled: true,
            ..Default::default()
        };
        let indexer = ChannelsIndexer::new(config, db);

        let fid = 1;
        let channels = [
            "https://warpcast.com/~/channel/alpha",
            "https://warpcast.com/~/channel/beta",
            "https://warpcast.com/~/channel/gamma",
        ];

        for channel in channels {
            let msg = make_channel_cast(fid, channel, 1000);
            indexer
                .process_event(&IndexEvent::message(msg, 0, 1))
                .await
                .unwrap();
        }

        let (user_channels, _) = indexer.get_user_channels(fid, None, 10).unwrap();
        assert_eq!(user_channels.len(), 3);
        // URLs may be in different order due to hashing, just check count
    }

    #[test]
    fn test_disabled_indexer() {
        let db = make_test_db();
        let config = FeatureConfig {
            enabled: false,
            ..Default::default()
        };
        let indexer = ChannelsIndexer::new(config, db);
        assert!(!indexer.is_enabled());
    }

    #[test]
    fn test_hash_url_consistency() {
        let url = "https://warpcast.com/~/channel/test";
        let hash1 = ChannelsIndexer::hash_url(url);
        let hash2 = ChannelsIndexer::hash_url(url);
        assert_eq!(hash1, hash2);

        let different_url = "https://warpcast.com/~/channel/other";
        let hash3 = ChannelsIndexer::hash_url(different_url);
        assert_ne!(hash1, hash3);
    }
}
