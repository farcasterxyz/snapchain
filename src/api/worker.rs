//! Index worker pool for processing events asynchronously.

use crate::api::config::ApiConfig;
use crate::api::events::{IndexEvent, IndexEventReceiver};
use crate::api::indexer::{BoxedIndexer, Indexer, IndexerRegistry};
use crate::storage::db::RocksDB;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

/// Worker pool that processes index events and dispatches to indexers.
pub struct IndexWorkerPool {
    config: ApiConfig,
    event_rx: IndexEventReceiver,
    db: Arc<RocksDB>,
    indexers: Vec<Arc<dyn Indexer>>,
    shutdown_tx: broadcast::Sender<()>,
    shutdown_rx: broadcast::Receiver<()>,
}

impl IndexWorkerPool {
    /// Create a new worker pool.
    pub fn new(config: ApiConfig, event_rx: IndexEventReceiver, db: Arc<RocksDB>) -> Self {
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        Self {
            config,
            event_rx,
            db,
            indexers: Vec::new(),
            shutdown_tx,
            shutdown_rx,
        }
    }

    /// Register an indexer with the worker pool.
    pub fn register_indexer<I: Indexer + 'static>(&mut self, indexer: I) {
        info!("Registering indexer: {}", indexer.name());
        self.indexers.push(Arc::new(indexer));
    }

    /// Register an Arc-wrapped indexer.
    pub fn register_arc(&mut self, indexer: Arc<dyn Indexer>) {
        info!("Registering indexer: {}", indexer.name());
        self.indexers.push(indexer);
    }

    /// Register a boxed indexer.
    pub fn register_boxed(&mut self, indexer: BoxedIndexer) {
        info!("Registering indexer: {}", indexer.name());
        // Convert Box<dyn Indexer> to Arc<dyn Indexer>
        self.indexers.push(Arc::from(indexer));
    }

    /// Register all indexers from a registry.
    pub fn register_all(&mut self, registry: IndexerRegistry) {
        for indexer in registry.into_vec() {
            self.register_boxed(indexer);
        }
    }

    /// Get a shutdown signal sender (for external shutdown triggers).
    pub fn shutdown_sender(&self) -> broadcast::Sender<()> {
        self.shutdown_tx.clone()
    }

    /// Get the list of registered indexers.
    pub fn indexers(&self) -> &[Arc<dyn Indexer>] {
        &self.indexers
    }

    /// Run the worker pool until shutdown.
    ///
    /// This should be spawned as a background task.
    pub async fn run(mut self) {
        if !self.config.enabled {
            info!("Farcaster indexing disabled, worker pool not starting");
            return;
        }

        info!(
            "Starting index worker pool with {} indexers",
            self.indexers.len()
        );

        let enabled_count = self.indexers.iter().filter(|i| i.is_enabled()).count();
        info!("{} indexers enabled", enabled_count);

        let mut events_processed: u64 = 0;
        let mut last_log_count: u64 = 0;

        loop {
            tokio::select! {
                // Biased toward shutdown for clean exit
                biased;

                _ = self.shutdown_rx.recv() => {
                    info!("Worker pool received shutdown signal");
                    break;
                }

                Some(event) = self.event_rx.recv() => {
                    self.dispatch_event(&event).await;
                    events_processed += 1;

                    // Log progress every 10k events
                    if events_processed - last_log_count >= 10_000 {
                        debug!("Processed {} index events", events_processed);
                        last_log_count = events_processed;
                    }
                }
            }
        }

        // Shutdown indexers gracefully
        info!("Shutting down indexers...");
        for indexer in &self.indexers {
            if let Err(e) = indexer.shutdown().await {
                error!("Error shutting down indexer {}: {:?}", indexer.name(), e);
            }
        }

        info!(
            "Worker pool stopped after processing {} events",
            events_processed
        );
    }

    /// Dispatch an event to all enabled indexers.
    async fn dispatch_event(&self, event: &IndexEvent) {
        for indexer in &self.indexers {
            if !indexer.is_enabled() {
                continue;
            }

            if let Err(e) = indexer.process_event(event).await {
                warn!(
                    "Indexer {} failed to process event: {:?}",
                    indexer.name(),
                    e
                );
                // Continue processing with other indexers - don't let one failure stop others
            }
        }
    }
}

/// Builder for IndexWorkerPool with fluent API.
pub struct IndexWorkerPoolBuilder {
    config: ApiConfig,
    event_rx: Option<IndexEventReceiver>,
    db: Option<Arc<RocksDB>>,
    indexers: Vec<BoxedIndexer>,
}

impl IndexWorkerPoolBuilder {
    pub fn new(config: ApiConfig) -> Self {
        Self {
            config,
            event_rx: None,
            db: None,
            indexers: Vec::new(),
        }
    }

    pub fn event_receiver(mut self, rx: IndexEventReceiver) -> Self {
        self.event_rx = Some(rx);
        self
    }

    pub fn database(mut self, db: Arc<RocksDB>) -> Self {
        self.db = Some(db);
        self
    }

    pub fn indexer<I: Indexer + 'static>(mut self, indexer: I) -> Self {
        self.indexers.push(Box::new(indexer));
        self
    }

    pub fn build(self) -> Result<IndexWorkerPool, &'static str> {
        let event_rx = self.event_rx.ok_or("event_rx is required")?;
        let db = self.db.ok_or("db is required")?;

        let mut pool = IndexWorkerPool::new(self.config, event_rx, db);
        for indexer in self.indexers {
            pool.register_boxed(indexer);
        }

        Ok(pool)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::indexer::NoOpIndexer;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_worker_pool_disabled() {
        let config = ApiConfig::default(); // disabled by default
        let (_tx, rx) = mpsc::channel(10);

        let db = Arc::new(RocksDB::new(":memory:"));

        let pool = IndexWorkerPool::new(config, rx, db);

        // Should exit immediately when disabled
        pool.run().await;
    }

    #[tokio::test]
    async fn test_worker_pool_processes_events() {
        use std::sync::atomic::{AtomicU64, Ordering};

        let mut config = ApiConfig::default();
        config.enabled = true;

        let (tx, rx) = mpsc::channel(10);
        let db = Arc::new(RocksDB::new(":memory:"));

        let mut pool = IndexWorkerPool::new(config, rx, db);

        // Create a simple counting indexer
        struct CountingIndexer {
            count: AtomicU64,
        }

        #[async_trait::async_trait]
        impl Indexer for CountingIndexer {
            fn name(&self) -> &'static str {
                "counting"
            }

            fn is_enabled(&self) -> bool {
                true
            }

            async fn process_event(
                &self,
                _event: &IndexEvent,
            ) -> Result<(), crate::api::indexer::IndexerError> {
                self.count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }

            fn last_checkpoint(&self) -> u64 {
                0
            }

            async fn save_checkpoint(
                &self,
                _event_id: u64,
            ) -> Result<(), crate::api::indexer::IndexerError> {
                Ok(())
            }

            fn stats(&self) -> crate::api::indexer::IndexerStats {
                crate::api::indexer::IndexerStats {
                    items_indexed: self.count.load(Ordering::SeqCst),
                    ..Default::default()
                }
            }
        }

        let indexer = Arc::new(CountingIndexer {
            count: AtomicU64::new(0),
        });
        let indexer_clone = indexer.clone();

        pool.indexers.push(indexer);

        let shutdown_tx = pool.shutdown_sender();

        // Spawn the pool
        let handle = tokio::spawn(async move {
            pool.run().await;
        });

        // Send some events
        tx.send(IndexEvent::block_committed(1, 1, 0)).await.unwrap();
        tx.send(IndexEvent::block_committed(1, 2, 0)).await.unwrap();
        tx.send(IndexEvent::block_committed(1, 3, 0)).await.unwrap();

        // Give it time to process
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Shutdown
        shutdown_tx.send(()).unwrap();
        handle.await.unwrap();

        // Verify events were processed
        assert_eq!(indexer_clone.count.load(Ordering::SeqCst), 3);
    }
}
