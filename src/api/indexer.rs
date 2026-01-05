//! Indexer trait and common types for Farcaster indexers.

use crate::api::events::IndexEvent;
use async_trait::async_trait;
use std::fmt;
use thiserror::Error;

/// Errors that can occur during indexing.
#[derive(Error, Debug)]
pub enum IndexerError {
    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Invalid data: {0}")]
    InvalidData(String),

    #[error("Not initialized")]
    NotInitialized,

    #[error("Backfill in progress")]
    BackfillInProgress,

    #[error("Feature disabled: {0}")]
    FeatureDisabled(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<prost::DecodeError> for IndexerError {
    fn from(e: prost::DecodeError) -> Self {
        Self::Serialization(e.to_string())
    }
}

/// Statistics about indexer state.
#[derive(Debug, Clone, Default)]
pub struct IndexerStats {
    /// Total items indexed.
    pub items_indexed: u64,

    /// Last event ID processed.
    pub last_event_id: u64,

    /// Last block height processed.
    pub last_block_height: u64,

    /// Whether backfill is complete.
    pub backfill_complete: bool,

    /// Estimated index size in bytes.
    pub size_bytes: u64,
}

/// Trait for Farcaster indexers.
///
/// Indexers process events from the block processing path and maintain
/// derived indexes for efficient querying.
#[async_trait]
pub trait Indexer: Send + Sync {
    /// Unique name for this indexer (e.g., "social_graph", "channels").
    fn name(&self) -> &'static str;

    /// Whether this indexer is currently enabled.
    fn is_enabled(&self) -> bool;

    /// Process a single index event (live path).
    ///
    /// Called from the worker pool for each event received.
    /// Should be fast - heavy processing should be batched.
    async fn process_event(&self, event: &IndexEvent) -> Result<(), IndexerError>;

    /// Process a batch of events (backfill path).
    ///
    /// More efficient for bulk processing during backfill.
    async fn process_batch(&self, events: &[IndexEvent]) -> Result<(), IndexerError> {
        for event in events {
            self.process_event(event).await?;
        }
        Ok(())
    }

    /// Get the last processed event ID for checkpointing.
    fn last_checkpoint(&self) -> u64;

    /// Save checkpoint (called periodically during backfill).
    async fn save_checkpoint(&self, event_id: u64) -> Result<(), IndexerError>;

    /// Get statistics about this indexer.
    fn stats(&self) -> IndexerStats;

    /// Check if the index is empty (needs full backfill).
    fn is_empty(&self) -> bool {
        self.stats().items_indexed == 0
    }

    /// Flush any pending writes to storage.
    async fn flush(&self) -> Result<(), IndexerError> {
        Ok(())
    }

    /// Called on shutdown to clean up resources.
    async fn shutdown(&self) -> Result<(), IndexerError> {
        self.flush().await
    }
}

/// Type alias for boxed indexer.
pub type BoxedIndexer = Box<dyn Indexer>;

/// Registry of available indexers.
pub struct IndexerRegistry {
    indexers: Vec<BoxedIndexer>,
}

impl IndexerRegistry {
    pub fn new() -> Self {
        Self {
            indexers: Vec::new(),
        }
    }

    pub fn register(&mut self, indexer: BoxedIndexer) {
        tracing::info!("Registering indexer: {}", indexer.name());
        self.indexers.push(indexer);
    }

    pub fn get(&self, name: &str) -> Option<&dyn Indexer> {
        self.indexers
            .iter()
            .find(|i| i.name() == name)
            .map(|i| i.as_ref())
    }

    pub fn enabled_indexers(&self) -> impl Iterator<Item = &dyn Indexer> {
        self.indexers
            .iter()
            .filter(|i| i.is_enabled())
            .map(|i| i.as_ref())
    }

    pub fn all_indexers(&self) -> impl Iterator<Item = &dyn Indexer> {
        self.indexers.iter().map(|i| i.as_ref())
    }

    pub fn into_vec(self) -> Vec<BoxedIndexer> {
        self.indexers
    }
}

impl Default for IndexerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for IndexerRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexerRegistry")
            .field(
                "indexers",
                &self.indexers.iter().map(|i| i.name()).collect::<Vec<_>>(),
            )
            .finish()
    }
}

/// A no-op indexer for testing or disabled features.
pub struct NoOpIndexer {
    name: &'static str,
}

impl NoOpIndexer {
    pub fn new(name: &'static str) -> Self {
        Self { name }
    }
}

#[async_trait]
impl Indexer for NoOpIndexer {
    fn name(&self) -> &'static str {
        self.name
    }

    fn is_enabled(&self) -> bool {
        false
    }

    async fn process_event(&self, _event: &IndexEvent) -> Result<(), IndexerError> {
        Ok(())
    }

    fn last_checkpoint(&self) -> u64 {
        0
    }

    async fn save_checkpoint(&self, _event_id: u64) -> Result<(), IndexerError> {
        Ok(())
    }

    fn stats(&self) -> IndexerStats {
        IndexerStats::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_noop_indexer() {
        let indexer = NoOpIndexer::new("test");
        assert_eq!(indexer.name(), "test");
        assert!(!indexer.is_enabled());
        assert!(indexer.is_empty());

        let event = IndexEvent::block_committed(1, 1, 0);
        assert!(indexer.process_event(&event).await.is_ok());
    }

    #[test]
    fn test_indexer_registry() {
        let mut registry = IndexerRegistry::new();
        registry.register(Box::new(NoOpIndexer::new("test1")));
        registry.register(Box::new(NoOpIndexer::new("test2")));

        assert!(registry.get("test1").is_some());
        assert!(registry.get("test2").is_some());
        assert!(registry.get("test3").is_none());

        // NoOpIndexer is disabled, so enabled_indexers should be empty
        assert_eq!(registry.enabled_indexers().count(), 0);
        assert_eq!(registry.all_indexers().count(), 2);
    }
}
