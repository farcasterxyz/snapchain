//! Backfill manager for catching up indexers on historical data.

use crate::api::config::ApiConfig;
use crate::api::events::IndexEvent;
use crate::api::indexer::{Indexer, IndexerError};
use crate::proto::{hub_event, HubEvent};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Statistics from a backfill operation.
#[derive(Debug, Clone, PartialEq)]
pub struct BackfillStats {
    /// Total events processed.
    pub events_processed: u64,

    /// Events that matched this indexer's criteria.
    pub events_matched: u64,

    /// Final event ID after backfill.
    pub final_event_id: u64,

    /// Duration of the backfill.
    pub duration: Duration,

    /// Events per second throughput.
    pub events_per_second: f64,
}

/// Status of a backfill operation.
#[derive(Debug, Clone, PartialEq)]
pub enum BackfillStatus {
    /// Not started.
    NotStarted,
    /// Currently running.
    InProgress {
        indexer_name: String,
        events_processed: u64,
        start_time: Instant,
    },
    /// Completed successfully.
    Completed(BackfillStats),
    /// Failed with error.
    Failed(String),
}

/// Trait for fetching hub events for backfill.
///
/// This abstracts the event source so we can use different implementations
/// (e.g., direct store access, gRPC client, etc.)
#[async_trait::async_trait]
pub trait EventSource: Send + Sync {
    /// Fetch events starting from the given ID.
    async fn get_events(
        &self,
        from_event_id: u64,
        limit: usize,
    ) -> Result<Vec<HubEvent>, IndexerError>;

    /// Get the latest event ID.
    async fn get_latest_event_id(&self) -> Result<u64, IndexerError>;
}

/// Manager for running backfill operations.
pub struct BackfillManager {
    config: ApiConfig,
    event_source: Arc<dyn EventSource>,
    status: RwLock<BackfillStatus>,
}

impl BackfillManager {
    pub fn new(config: ApiConfig, event_source: Arc<dyn EventSource>) -> Self {
        Self {
            config,
            event_source,
            status: RwLock::new(BackfillStatus::NotStarted),
        }
    }

    /// Get current backfill status.
    pub async fn status(&self) -> BackfillStatus {
        self.status.read().await.clone()
    }

    /// Run backfill for a specific indexer.
    ///
    /// Returns stats on completion, or error if backfill fails.
    pub async fn run_backfill(&self, indexer: &dyn Indexer) -> Result<BackfillStats, IndexerError> {
        let indexer_name = indexer.name().to_string();
        info!("Starting backfill for indexer: {}", indexer_name);

        if !indexer.is_enabled() {
            return Err(IndexerError::FeatureDisabled(indexer_name));
        }

        let start_time = Instant::now();
        let mut cursor = indexer.last_checkpoint();
        let mut events_processed: u64 = 0;
        let mut events_matched: u64 = 0;

        // Get batch size from config based on indexer type
        let batch_size = self.get_batch_size(&indexer_name);

        // Update status
        *self.status.write().await = BackfillStatus::InProgress {
            indexer_name: indexer_name.clone(),
            events_processed: 0,
            start_time,
        };

        let latest_event_id = self.event_source.get_latest_event_id().await?;
        info!(
            "Backfill from event {} to {} (approximately {} events)",
            cursor,
            latest_event_id,
            latest_event_id.saturating_sub(cursor)
        );

        loop {
            let events = self.event_source.get_events(cursor, batch_size).await?;

            if events.is_empty() {
                debug!("No more events to process, backfill complete");
                break;
            }

            let batch_len = events.len() as u64;

            // Convert hub events to index events
            let index_events: Vec<IndexEvent> = events
                .into_iter()
                .filter_map(|e| self.hub_event_to_index_event(e))
                .collect();

            let matched_count = index_events.len() as u64;

            // Process batch
            if !index_events.is_empty() {
                indexer.process_batch(&index_events).await?;
            }

            events_processed += batch_len;
            events_matched += matched_count;

            // Update cursor
            cursor += batch_len;

            // Checkpoint periodically (every 10 batches)
            if events_processed % (batch_size as u64 * 10) == 0 {
                indexer.save_checkpoint(cursor).await?;

                // Update status
                *self.status.write().await = BackfillStatus::InProgress {
                    indexer_name: indexer_name.clone(),
                    events_processed,
                    start_time,
                };

                info!(
                    "Backfill progress: {} events ({:.1}%)",
                    events_processed,
                    (cursor as f64 / latest_event_id as f64) * 100.0
                );
            }

            // Yield to other tasks periodically
            if events_processed % 1000 == 0 {
                tokio::task::yield_now().await;
            }
        }

        // Final checkpoint
        indexer.save_checkpoint(cursor).await?;
        indexer.flush().await?;

        let duration = start_time.elapsed();
        let events_per_second = events_processed as f64 / duration.as_secs_f64();

        let stats = BackfillStats {
            events_processed,
            events_matched,
            final_event_id: cursor,
            duration,
            events_per_second,
        };

        info!(
            "Backfill complete for {}: {} events in {:.1}s ({:.0} events/sec)",
            indexer_name,
            events_processed,
            duration.as_secs_f64(),
            events_per_second
        );

        *self.status.write().await = BackfillStatus::Completed(stats.clone());

        Ok(stats)
    }

    /// Convert a hub event to an index event.
    fn hub_event_to_index_event(&self, event: HubEvent) -> Option<IndexEvent> {
        let shard_id = event.shard_index;
        match event.body.as_ref()? {
            hub_event::Body::MergeMessageBody(body) => body
                .message
                .as_ref()
                .map(|m| IndexEvent::message(m.clone(), shard_id, 0)),
            hub_event::Body::MergeOnChainEventBody(body) => body
                .on_chain_event
                .as_ref()
                .map(|e| IndexEvent::onchain(e.clone(), shard_id, 0)),
            hub_event::Body::PruneMessageBody(body) => {
                // We might want to handle pruning differently
                body.message
                    .as_ref()
                    .map(|m| IndexEvent::message(m.clone(), shard_id, 0))
            }
            hub_event::Body::RevokeMessageBody(body) => body
                .message
                .as_ref()
                .map(|m| IndexEvent::message(m.clone(), shard_id, 0)),
            hub_event::Body::MergeUsernameProofBody(_) => {
                // Username proofs handled via hub event directly
                Some(IndexEvent::hub_event(event.clone(), shard_id))
            }
            hub_event::Body::MergeFailure(_) => {
                // Merge failures are not indexed
                None
            }
            hub_event::Body::BlockConfirmedBody(body) => Some(IndexEvent::block_committed(
                body.shard_index,
                body.block_number,
                body.total_events as usize,
            )),
        }
    }

    /// Get batch size for an indexer based on config.
    fn get_batch_size(&self, indexer_name: &str) -> usize {
        match indexer_name {
            "social_graph" => self.config.social_graph.backfill_batch_size,
            "channels" => self.config.channels.backfill_batch_size,
            "metrics" => self.config.metrics.backfill_batch_size,
            "search" => self.config.search.backfill_batch_size,
            _ => 1000, // Default batch size
        }
    }
}

/// Simple in-memory event source for testing.
#[cfg(test)]
pub struct MockEventSource {
    events: Vec<HubEvent>,
}

#[cfg(test)]
impl MockEventSource {
    pub fn new(events: Vec<HubEvent>) -> Self {
        Self { events }
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl EventSource for MockEventSource {
    async fn get_events(
        &self,
        from_event_id: u64,
        limit: usize,
    ) -> Result<Vec<HubEvent>, IndexerError> {
        let start = from_event_id as usize;
        if start >= self.events.len() {
            return Ok(vec![]);
        }
        let end = std::cmp::min(start + limit, self.events.len());
        Ok(self.events[start..end].to_vec())
    }

    async fn get_latest_event_id(&self) -> Result<u64, IndexerError> {
        Ok(self.events.len() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::indexer::{IndexerStats, NoOpIndexer};
    use std::sync::atomic::{AtomicU64, Ordering};

    struct CountingIndexer {
        count: AtomicU64,
        checkpoint: AtomicU64,
    }

    impl CountingIndexer {
        fn new() -> Self {
            Self {
                count: AtomicU64::new(0),
                checkpoint: AtomicU64::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl Indexer for CountingIndexer {
        fn name(&self) -> &'static str {
            "counting"
        }

        fn is_enabled(&self) -> bool {
            true
        }

        async fn process_event(&self, _event: &IndexEvent) -> Result<(), IndexerError> {
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn last_checkpoint(&self) -> u64 {
            self.checkpoint.load(Ordering::SeqCst)
        }

        async fn save_checkpoint(&self, event_id: u64) -> Result<(), IndexerError> {
            self.checkpoint.store(event_id, Ordering::SeqCst);
            Ok(())
        }

        fn stats(&self) -> IndexerStats {
            IndexerStats {
                items_indexed: self.count.load(Ordering::SeqCst),
                last_event_id: self.checkpoint.load(Ordering::SeqCst),
                ..Default::default()
            }
        }
    }

    #[tokio::test]
    async fn test_backfill_disabled_indexer() {
        let config = ApiConfig::default();
        let event_source = Arc::new(MockEventSource::new(vec![]));
        let manager = BackfillManager::new(config, event_source);

        let indexer = NoOpIndexer::new("test");
        let result = manager.run_backfill(&indexer).await;

        assert!(matches!(result, Err(IndexerError::FeatureDisabled(_))));
    }

    #[tokio::test]
    async fn test_backfill_empty_events() {
        let mut config = ApiConfig::default();
        config.enabled = true;

        let event_source = Arc::new(MockEventSource::new(vec![]));
        let manager = BackfillManager::new(config, event_source);

        let indexer = CountingIndexer::new();
        let stats = manager.run_backfill(&indexer).await.unwrap();

        assert_eq!(stats.events_processed, 0);
        assert_eq!(stats.events_matched, 0);
    }
}
