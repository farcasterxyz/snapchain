//! Engagement metrics indexer for cast reactions and replies.
//!
//! This module tracks:
//! - Like counts per cast
//! - Recast counts per cast
//! - Reply counts per cast
//!
//! # Storage Schema
//!
//! Keys are prefixed with METRICS_PREFIX (0xE2):
//! - `[prefix][0x01][fid:8][hash:20]` -> CastMetrics (likes, recasts, replies)
//! - `[prefix][0xFF]` -> checkpoint:8
//!
//! # Processing
//!
//! - ReactionAdd (type=Like): increment like count
//! - ReactionRemove (type=Like): decrement like count
//! - ReactionAdd (type=Recast): increment recast count
//! - ReactionRemove (type=Recast): decrement recast count
//! - CastAdd with parent_cast_id: increment reply count on parent

use crate::api::config::MetricsConfig;
use crate::api::events::IndexEvent;
use crate::api::indexer::{Indexer, IndexerError, IndexerStats};
use crate::proto::message_data::Body;
use crate::proto::{Message, MessageType, ReactionType};
use crate::storage::db::{RocksDB, RocksDbTransactionBatch};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Key prefixes for metrics data.
mod keys {
    /// Prefix for all metrics index keys.
    pub const METRICS_PREFIX: u8 = 0xE2;

    /// Cast metrics: <prefix><0x01><fid:8><hash_len:1><hash:N> -> CastMetrics
    pub const CAST_METRICS: u8 = 0x01;

    /// Checkpoint: <prefix><0xFF> -> event_id:8
    pub const CHECKPOINT: u8 = 0xFF;
}

/// Engagement metrics for a single cast.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CastMetrics {
    pub likes: u64,
    pub recasts: u64,
    pub replies: u64,
}

/// Metrics indexer for tracking engagement on casts.
pub struct MetricsIndexer {
    config: MetricsConfig,
    db: Arc<RocksDB>,
    checkpoint: AtomicU64,
    casts_tracked: AtomicU64,
}

impl MetricsIndexer {
    /// Create a new metrics indexer.
    pub fn new(config: MetricsConfig, db: Arc<RocksDB>) -> Self {
        let checkpoint = Self::load_checkpoint(&db).unwrap_or(0);

        Self {
            config,
            db,
            checkpoint: AtomicU64::new(checkpoint),
            casts_tracked: AtomicU64::new(0),
        }
    }

    /// Get metrics for a cast.
    pub fn get_cast_metrics(&self, fid: u64, hash: &[u8]) -> Result<CastMetrics, IndexerError> {
        let key = Self::make_cast_key(fid, hash);
        match self.db.get(&key) {
            Ok(Some(data)) => {
                let metrics: CastMetrics = serde_json::from_slice(&data).map_err(|e| {
                    IndexerError::Storage(format!("Failed to deserialize metrics: {}", e))
                })?;
                Ok(metrics)
            }
            Ok(None) => Ok(CastMetrics::default()),
            Err(e) => Err(IndexerError::Storage(format!("DB read error: {:?}", e))),
        }
    }

    /// Get trending casts sorted by engagement score.
    ///
    /// Returns a vector of (fid, hash, score) tuples sorted by score descending.
    pub fn get_trending_casts(
        &self,
        limit: usize,
    ) -> Result<Vec<(u64, Vec<u8>, f64)>, IndexerError> {
        use crate::storage::db::PageOptions;

        let prefix = vec![keys::METRICS_PREFIX, keys::CAST_METRICS];
        let page_options = PageOptions {
            page_size: None,
            page_token: None,
            reverse: false,
        };

        let mut results: Vec<(u64, Vec<u8>, f64)> = Vec::new();

        // Iterate all cast metrics
        self.db
            .for_each_iterator_by_prefix(Some(prefix.clone()), None, &page_options, |key, value| {
                // Parse key: [prefix:1][type:1][fid:8][hash_len:1][hash:N]
                if key.len() < 11 {
                    return Ok(false); // Continue iteration
                }

                // Check prefix matches
                if key[0] != keys::METRICS_PREFIX || key[1] != keys::CAST_METRICS {
                    return Ok(true); // Stop - outside our prefix
                }

                let fid = u64::from_be_bytes(key[2..10].try_into().unwrap());
                let hash_len = key[10] as usize;
                if key.len() < 11 + hash_len {
                    return Ok(false);
                }
                let hash = key[11..11 + hash_len].to_vec();

                // Parse metrics
                if let Ok(metrics) = serde_json::from_slice::<CastMetrics>(value) {
                    // Calculate score: likes + 2*recasts + 3*replies
                    let score = metrics.likes as f64
                        + (metrics.recasts as f64 * 2.0)
                        + (metrics.replies as f64 * 3.0);

                    if score > 0.0 {
                        results.push((fid, hash, score));
                    }
                }

                Ok(false) // Continue iteration
            })
            .map_err(|e| IndexerError::Storage(format!("Failed to iterate: {:?}", e)))?;

        // Sort by score descending
        results.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));

        // Return top N
        results.truncate(limit);

        Ok(results)
    }

    /// Get number of casts being tracked.
    pub fn casts_tracked(&self) -> u64 {
        self.casts_tracked.load(Ordering::Relaxed)
    }

    /// Process a single message.
    fn process_message(
        &self,
        message: &Message,
        txn: &mut RocksDbTransactionBatch,
    ) -> Result<(), IndexerError> {
        let data = match &message.data {
            Some(d) => d,
            None => return Ok(()),
        };

        let msg_type = MessageType::try_from(data.r#type).unwrap_or(MessageType::None);

        match msg_type {
            MessageType::ReactionAdd | MessageType::ReactionRemove => {
                self.process_reaction_message(data, msg_type == MessageType::ReactionAdd, txn)?;
            }
            MessageType::CastAdd => {
                self.process_cast_add(data, txn)?;
            }
            _ => {}
        }

        Ok(())
    }

    /// Process a reaction message (add or remove).
    fn process_reaction_message(
        &self,
        data: &crate::proto::MessageData,
        is_add: bool,
        txn: &mut RocksDbTransactionBatch,
    ) -> Result<(), IndexerError> {
        let reaction_body = match &data.body {
            Some(Body::ReactionBody(body)) => body,
            _ => return Ok(()),
        };

        // Get the target cast
        let target_cast = match &reaction_body.target {
            Some(crate::proto::reaction_body::Target::TargetCastId(cast_id)) => cast_id,
            _ => return Ok(()), // URL reactions not tracked
        };

        let reaction_type =
            ReactionType::try_from(reaction_body.r#type).unwrap_or(ReactionType::None);
        if reaction_type == ReactionType::None {
            return Ok(());
        }

        self.update_cast_metrics(
            target_cast.fid,
            &target_cast.hash,
            reaction_type,
            is_add,
            txn,
        )
    }

    /// Process a cast add message to track replies.
    fn process_cast_add(
        &self,
        data: &crate::proto::MessageData,
        txn: &mut RocksDbTransactionBatch,
    ) -> Result<(), IndexerError> {
        let cast_body = match &data.body {
            Some(Body::CastAddBody(body)) => body,
            _ => return Ok(()),
        };

        // Check if this is a reply (has a parent cast)
        let parent_cast = match &cast_body.parent {
            Some(crate::proto::cast_add_body::Parent::ParentCastId(cast_id)) => cast_id,
            _ => return Ok(()), // Not a reply
        };

        // Increment reply count on parent cast
        self.increment_reply_count(parent_cast.fid, &parent_cast.hash, txn)
    }

    /// Get metrics from pending transaction or DB.
    fn get_metrics_from_txn(
        &self,
        key: &[u8],
        txn: &RocksDbTransactionBatch,
    ) -> Result<Option<CastMetrics>, IndexerError> {
        // Check pending writes first
        if let Some(Some(value)) = txn.batch.get(key) {
            let metrics: CastMetrics = serde_json::from_slice(value)
                .map_err(|e| IndexerError::Storage(format!("Failed to deserialize: {}", e)))?;
            return Ok(Some(metrics));
        }

        // Fall back to DB
        match self.db.get(key) {
            Ok(Some(data)) => {
                let metrics: CastMetrics = serde_json::from_slice(&data)
                    .map_err(|e| IndexerError::Storage(format!("Failed to deserialize: {}", e)))?;
                Ok(Some(metrics))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(IndexerError::Storage(format!("DB error: {:?}", e))),
        }
    }

    /// Update metrics for a cast based on a reaction.
    fn update_cast_metrics(
        &self,
        fid: u64,
        hash: &[u8],
        reaction_type: ReactionType,
        is_add: bool,
        txn: &mut RocksDbTransactionBatch,
    ) -> Result<(), IndexerError> {
        let key = Self::make_cast_key(fid, hash);

        // Get existing metrics from txn or DB
        let mut metrics = match self.get_metrics_from_txn(&key, txn)? {
            Some(m) => m,
            None => {
                if is_add {
                    self.casts_tracked.fetch_add(1, Ordering::Relaxed);
                }
                CastMetrics::default()
            }
        };

        // Update the appropriate counter
        match reaction_type {
            ReactionType::Like => {
                if is_add {
                    metrics.likes = metrics.likes.saturating_add(1);
                } else {
                    metrics.likes = metrics.likes.saturating_sub(1);
                }
            }
            ReactionType::Recast => {
                if is_add {
                    metrics.recasts = metrics.recasts.saturating_add(1);
                } else {
                    metrics.recasts = metrics.recasts.saturating_sub(1);
                }
            }
            ReactionType::None => return Ok(()),
        }

        // Store updated metrics
        let value = serde_json::to_vec(&metrics)
            .map_err(|e| IndexerError::Storage(format!("Failed to serialize: {}", e)))?;
        txn.put(key, value);

        Ok(())
    }

    /// Increment reply count for a cast.
    fn increment_reply_count(
        &self,
        fid: u64,
        hash: &[u8],
        txn: &mut RocksDbTransactionBatch,
    ) -> Result<(), IndexerError> {
        let key = Self::make_cast_key(fid, hash);

        // Get existing metrics from txn or DB
        let mut metrics = match self.get_metrics_from_txn(&key, txn)? {
            Some(m) => m,
            None => {
                self.casts_tracked.fetch_add(1, Ordering::Relaxed);
                CastMetrics::default()
            }
        };

        metrics.replies = metrics.replies.saturating_add(1);

        // Store updated metrics
        let value = serde_json::to_vec(&metrics)
            .map_err(|e| IndexerError::Storage(format!("Failed to serialize: {}", e)))?;
        txn.put(key, value);

        Ok(())
    }

    /// Create a key for cast metrics.
    fn make_cast_key(fid: u64, hash: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(2 + 8 + 1 + hash.len());
        key.push(keys::METRICS_PREFIX);
        key.push(keys::CAST_METRICS);
        key.extend_from_slice(&fid.to_be_bytes());
        key.push(hash.len() as u8);
        key.extend_from_slice(hash);
        key
    }

    fn make_checkpoint_key() -> Vec<u8> {
        vec![keys::METRICS_PREFIX, keys::CHECKPOINT]
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
impl Indexer for MetricsIndexer {
    fn name(&self) -> &'static str {
        "metrics"
    }

    fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    async fn process_event(&self, event: &IndexEvent) -> Result<(), IndexerError> {
        match event {
            IndexEvent::MessageCommitted { message, .. } => {
                let mut txn = RocksDbTransactionBatch::new();
                self.process_message(message, &mut txn)?;
                if txn.len() > 0 {
                    self.db
                        .commit(txn)
                        .map_err(|e| IndexerError::Storage(e.to_string()))?;
                }
            }
            IndexEvent::MessagesCommitted { messages, .. } => {
                let mut txn = RocksDbTransactionBatch::new();
                for message in messages {
                    self.process_message(message, &mut txn)?;
                }
                if txn.len() > 0 {
                    self.db
                        .commit(txn)
                        .map_err(|e| IndexerError::Storage(e.to_string()))?;
                }
            }
            _ => {
                // Other event types not relevant for metrics
            }
        }
        Ok(())
    }

    async fn process_batch(&self, events: &[IndexEvent]) -> Result<(), IndexerError> {
        let mut txn = RocksDbTransactionBatch::new();

        for event in events {
            match event {
                IndexEvent::MessageCommitted { message, .. } => {
                    self.process_message(message, &mut txn)?;
                }
                IndexEvent::MessagesCommitted { messages, .. } => {
                    for message in messages {
                        self.process_message(message, &mut txn)?;
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
        let value = event_id.to_be_bytes().to_vec();

        let mut txn = RocksDbTransactionBatch::new();
        txn.put(key, value);
        self.db
            .commit(txn)
            .map_err(|e| IndexerError::Storage(e.to_string()))?;

        self.checkpoint.store(event_id, Ordering::SeqCst);
        Ok(())
    }

    fn stats(&self) -> IndexerStats {
        IndexerStats {
            items_indexed: self.casts_tracked.load(Ordering::Relaxed),
            last_event_id: self.checkpoint.load(Ordering::SeqCst),
            last_block_height: 0,
            backfill_complete: true,
            size_bytes: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::config::MetricsConfig;
    use crate::api::events::IndexEvent;
    use crate::proto::{CastAddBody, CastId, MessageData, ReactionBody};
    use crate::storage::db::RocksDB;

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

    fn create_indexer(db: Arc<RocksDB>) -> MetricsIndexer {
        let config = MetricsConfig {
            enabled: true,
            backfill_on_startup: false,
            backfill_batch_size: 100,
            ..Default::default()
        };
        MetricsIndexer::new(config, db)
    }

    fn create_reaction_message(
        fid: u64,
        target_fid: u64,
        target_hash: Vec<u8>,
        reaction_type: ReactionType,
        is_add: bool,
    ) -> Message {
        Message {
            data: Some(MessageData {
                fid,
                r#type: if is_add {
                    MessageType::ReactionAdd as i32
                } else {
                    MessageType::ReactionRemove as i32
                },
                timestamp: 1000,
                body: Some(crate::proto::message_data::Body::ReactionBody(
                    ReactionBody {
                        r#type: reaction_type as i32,
                        target: Some(crate::proto::reaction_body::Target::TargetCastId(CastId {
                            fid: target_fid,
                            hash: target_hash,
                        })),
                    },
                )),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn create_reply_message(fid: u64, parent_fid: u64, parent_hash: Vec<u8>) -> Message {
        Message {
            data: Some(MessageData {
                fid,
                r#type: MessageType::CastAdd as i32,
                timestamp: 1000,
                body: Some(crate::proto::message_data::Body::CastAddBody(CastAddBody {
                    parent: Some(crate::proto::cast_add_body::Parent::ParentCastId(CastId {
                        fid: parent_fid,
                        hash: parent_hash,
                    })),
                    text: "This is a reply".to_string(),
                    ..Default::default()
                })),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_like_reaction() {
        let db = make_test_db();
        let indexer = create_indexer(db);

        let target_fid = 123u64;
        let target_hash = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

        // Add a like
        let msg = create_reaction_message(
            456,
            target_fid,
            target_hash.clone(),
            ReactionType::Like,
            true,
        );
        let event = IndexEvent::message(msg, 1, 1);
        indexer.process_event(&event).await.unwrap();

        // Check metrics
        let metrics = indexer.get_cast_metrics(target_fid, &target_hash).unwrap();
        assert_eq!(metrics.likes, 1);
        assert_eq!(metrics.recasts, 0);
        assert_eq!(metrics.replies, 0);
    }

    #[tokio::test]
    async fn test_recast_reaction() {
        let db = make_test_db();
        let indexer = create_indexer(db);

        let target_fid = 123u64;
        let target_hash = vec![1, 2, 3, 4, 5, 6, 7, 8];

        // Add a recast
        let msg = create_reaction_message(
            456,
            target_fid,
            target_hash.clone(),
            ReactionType::Recast,
            true,
        );
        let event = IndexEvent::message(msg, 1, 1);
        indexer.process_event(&event).await.unwrap();

        // Check metrics
        let metrics = indexer.get_cast_metrics(target_fid, &target_hash).unwrap();
        assert_eq!(metrics.likes, 0);
        assert_eq!(metrics.recasts, 1);
        assert_eq!(metrics.replies, 0);
    }

    #[tokio::test]
    async fn test_reply_count() {
        let db = make_test_db();
        let indexer = create_indexer(db);

        let parent_fid = 123u64;
        let parent_hash = vec![1, 2, 3, 4, 5, 6, 7, 8];

        // Add a reply
        let msg = create_reply_message(456, parent_fid, parent_hash.clone());
        let event = IndexEvent::message(msg, 1, 1);
        indexer.process_event(&event).await.unwrap();

        // Check parent metrics
        let metrics = indexer.get_cast_metrics(parent_fid, &parent_hash).unwrap();
        assert_eq!(metrics.likes, 0);
        assert_eq!(metrics.recasts, 0);
        assert_eq!(metrics.replies, 1);
    }

    #[tokio::test]
    async fn test_multiple_reactions() {
        let db = make_test_db();
        let indexer = create_indexer(db);

        let target_fid = 123u64;
        let target_hash = vec![1, 2, 3, 4];

        // Add 3 likes from different users
        for user_fid in [100, 200, 300] {
            let msg = create_reaction_message(
                user_fid,
                target_fid,
                target_hash.clone(),
                ReactionType::Like,
                true,
            );
            let event = IndexEvent::message(msg, 1, 1);
            indexer.process_event(&event).await.unwrap();
        }

        // Add 2 recasts
        for user_fid in [400, 500] {
            let msg = create_reaction_message(
                user_fid,
                target_fid,
                target_hash.clone(),
                ReactionType::Recast,
                true,
            );
            let event = IndexEvent::message(msg, 1, 1);
            indexer.process_event(&event).await.unwrap();
        }

        // Check metrics
        let metrics = indexer.get_cast_metrics(target_fid, &target_hash).unwrap();
        assert_eq!(metrics.likes, 3);
        assert_eq!(metrics.recasts, 2);
        assert_eq!(metrics.replies, 0);
    }

    #[tokio::test]
    async fn test_reaction_remove() {
        let db = make_test_db();
        let indexer = create_indexer(db);

        let target_fid = 123u64;
        let target_hash = vec![1, 2, 3, 4];

        // Add 3 likes
        for user_fid in [100, 200, 300] {
            let msg = create_reaction_message(
                user_fid,
                target_fid,
                target_hash.clone(),
                ReactionType::Like,
                true,
            );
            let event = IndexEvent::message(msg, 1, 1);
            indexer.process_event(&event).await.unwrap();
        }

        // Remove 1 like
        let remove_msg = create_reaction_message(
            100,
            target_fid,
            target_hash.clone(),
            ReactionType::Like,
            false,
        );
        let event = IndexEvent::message(remove_msg, 1, 1);
        indexer.process_event(&event).await.unwrap();

        // Check metrics
        let metrics = indexer.get_cast_metrics(target_fid, &target_hash).unwrap();
        assert_eq!(metrics.likes, 2);
    }

    #[tokio::test]
    async fn test_combined_engagement() {
        let db = make_test_db();
        let indexer = create_indexer(db);

        let target_fid = 123u64;
        let target_hash = vec![1, 2, 3, 4, 5, 6, 7, 8];

        // Add likes
        for user_fid in [100, 200] {
            let msg = create_reaction_message(
                user_fid,
                target_fid,
                target_hash.clone(),
                ReactionType::Like,
                true,
            );
            let event = IndexEvent::message(msg, 1, 1);
            indexer.process_event(&event).await.unwrap();
        }

        // Add recasts
        for user_fid in [300, 400, 500] {
            let msg = create_reaction_message(
                user_fid,
                target_fid,
                target_hash.clone(),
                ReactionType::Recast,
                true,
            );
            let event = IndexEvent::message(msg, 1, 1);
            indexer.process_event(&event).await.unwrap();
        }

        // Add replies
        for user_fid in [600, 700, 800, 900] {
            let msg = create_reply_message(user_fid, target_fid, target_hash.clone());
            let event = IndexEvent::message(msg, 1, 1);
            indexer.process_event(&event).await.unwrap();
        }

        // Check final metrics
        let metrics = indexer.get_cast_metrics(target_fid, &target_hash).unwrap();
        assert_eq!(metrics.likes, 2);
        assert_eq!(metrics.recasts, 3);
        assert_eq!(metrics.replies, 4);
    }

    #[test]
    fn test_nonexistent_cast() {
        let db = make_test_db();
        let indexer = create_indexer(db);

        // Query metrics for a cast that doesn't exist
        let metrics = indexer.get_cast_metrics(999, &[1, 2, 3]).unwrap();
        assert_eq!(metrics.likes, 0);
        assert_eq!(metrics.recasts, 0);
        assert_eq!(metrics.replies, 0);
    }

    #[tokio::test]
    async fn test_batch_processing() {
        let db = make_test_db();
        let indexer = create_indexer(db);

        let target_fid = 123u64;
        let target_hash = vec![1, 2, 3, 4];

        // Create multiple messages
        let messages: Vec<Message> = (0..5)
            .map(|i| {
                create_reaction_message(
                    100 + i,
                    target_fid,
                    target_hash.clone(),
                    ReactionType::Like,
                    true,
                )
            })
            .collect();

        // Process as batch
        let event = IndexEvent::messages(messages, 1, 1);
        indexer.process_event(&event).await.unwrap();

        // Check metrics
        let metrics = indexer.get_cast_metrics(target_fid, &target_hash).unwrap();
        assert_eq!(metrics.likes, 5);
    }

    #[tokio::test]
    async fn test_checkpoint() {
        let db = make_test_db();
        let indexer = create_indexer(db.clone());

        assert_eq!(indexer.last_checkpoint(), 0);

        indexer.save_checkpoint(100).await.unwrap();
        assert_eq!(indexer.last_checkpoint(), 100);

        // Verify it persists
        let indexer2 = create_indexer(db);
        assert_eq!(indexer2.last_checkpoint(), 100);
    }

    #[test]
    fn test_indexer_name() {
        let db = make_test_db();
        let indexer = create_indexer(db);
        assert_eq!(indexer.name(), "metrics");
    }

    #[test]
    fn test_indexer_enabled() {
        let db = make_test_db();
        let indexer = create_indexer(db);
        assert!(indexer.is_enabled());
    }
}
