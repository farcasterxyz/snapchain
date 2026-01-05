//! Farcaster API compatibility layer for Snapchain.
//!
//! This module provides optional indexing infrastructure to support Farcaster v2 API endpoints.
//! All features are opt-in via configuration and have zero overhead when disabled.
//!
//! # Architecture
//!
//! ```text
//! ShardEngine ─→ HubEvent broadcast ─→ HubEventBridge ─→ IndexEventChannel ─→ IndexWorkerPool
//!                                                                                    ↓
//!                                                              [SocialGraph, Channels, Metrics, Search]
//! ```
//!
//! # Configuration
//!
//! ```toml
//! [api]
//! enabled = true
//!
//! [api.social_graph]
//! enabled = true
//! backfill_on_startup = true
//! ```

pub mod backfill;
pub mod bridge;
pub mod channels;
pub mod config;
pub mod conversations;
pub mod events;
pub mod feeds;
pub mod http;
pub mod indexer;
pub mod metrics;
pub mod search;
pub mod social_graph;
pub mod types;
pub mod worker;

pub use backfill::BackfillManager;
pub use bridge::HubEventBridge;
pub use channels::ChannelsIndexer;
pub use config::ApiConfig;
pub use conversations::ConversationService;
pub use events::{IndexEvent, IndexEventReceiver, IndexEventSender};
pub use feeds::{FeedHandler, FeedService};
pub use http::{ApiHttpHandler, ConversationHandler};
pub use indexer::{Indexer, IndexerError};
pub use metrics::MetricsIndexer;
pub use search::SearchIndexer;
pub use social_graph::SocialGraphIndexer;
pub use worker::IndexWorkerPool;

use crate::proto::HubEvent;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};

/// Default channel capacity for index events.
/// If indexers can't keep up, events are dropped and caught up via backfill.
pub const DEFAULT_CHANNEL_CAPACITY: usize = 10_000;

/// Create an index event channel pair.
pub fn create_index_channel(capacity: usize) -> (IndexEventSender, IndexEventReceiver) {
    mpsc::channel(capacity)
}

/// Handles for the Farcaster indexing system.
pub struct ApiSystem {
    /// Worker pool task handle.
    pub worker_handle: tokio::task::JoinHandle<()>,
    /// Bridge task handles (one per shard).
    pub bridge_handles: Vec<tokio::task::JoinHandle<()>>,
    /// Shutdown sender for worker pool.
    pub shutdown_tx: broadcast::Sender<()>,
    /// HTTP handler for api endpoints.
    pub http_handler: ApiHttpHandler,
}

impl ApiSystem {
    /// Shutdown the system gracefully.
    pub async fn shutdown(self) {
        tracing::info!("Shutting down Farcaster indexing system");
        let _ = self.shutdown_tx.send(());

        // Wait for worker pool
        if let Err(e) = self.worker_handle.await {
            tracing::error!("Worker pool task panicked: {:?}", e);
        }

        // Bridges will stop when the HubEvent channels close
        for handle in self.bridge_handles {
            let _ = handle.await;
        }
    }
}

/// Initialize the Farcaster indexing system.
///
/// Returns None if api is disabled in config.
///
/// # Arguments
/// * `config` - Farcaster configuration
/// * `db` - RocksDB instance for indexer storage
/// * `hub_event_senders` - HubEvent broadcast senders from each shard engine
pub fn initialize(
    config: &ApiConfig,
    db: Arc<crate::storage::db::RocksDB>,
    hub_event_senders: Vec<(u32, broadcast::Sender<HubEvent>)>,
) -> Option<ApiSystem> {
    if !config.enabled {
        tracing::info!("Farcaster indexing disabled");
        return None;
    }

    tracing::info!(
        "Initializing Farcaster indexing system with {} shards",
        hub_event_senders.len()
    );

    let (index_tx, index_rx) = create_index_channel(DEFAULT_CHANNEL_CAPACITY);

    // Create indexers
    let social_graph_indexer = if config.social_graph.enabled {
        tracing::info!("Social graph indexer enabled");
        Some(Arc::new(SocialGraphIndexer::new(
            config.social_graph.clone(),
            db.clone(),
        )))
    } else {
        None
    };

    let channels_indexer = if config.channels.enabled {
        tracing::info!("Channels indexer enabled");
        Some(Arc::new(ChannelsIndexer::new(
            config.channels.clone(),
            db.clone(),
        )))
    } else {
        None
    };

    let metrics_indexer = if config.metrics.enabled {
        tracing::info!("Metrics indexer enabled");
        Some(Arc::new(MetricsIndexer::new(
            config.metrics.clone(),
            db.clone(),
        )))
    } else {
        None
    };

    // Create worker pool and register indexers
    let mut worker_pool = IndexWorkerPool::new(config.clone(), index_rx, db);

    if let Some(ref indexer) = social_graph_indexer {
        worker_pool.register_arc(indexer.clone());
    }

    if let Some(ref indexer) = channels_indexer {
        worker_pool.register_arc(indexer.clone());
    }

    if let Some(ref indexer) = metrics_indexer {
        worker_pool.register_arc(indexer.clone());
    }

    let shutdown_tx = worker_pool.shutdown_sender();

    // Spawn worker pool
    let worker_handle = tokio::spawn(async move {
        worker_pool.run().await;
    });

    // Create bridges for each shard
    let mut bridge_handles = Vec::new();
    for (shard_id, hub_event_tx) in hub_event_senders {
        let bridge = HubEventBridge::from_sender(&hub_event_tx, index_tx.clone(), shard_id);
        let handle = tokio::spawn(async move {
            bridge.run().await;
        });
        bridge_handles.push(handle);
    }

    // Create HTTP handler
    let http_handler = ApiHttpHandler::new(social_graph_indexer, channels_indexer, metrics_indexer);

    Some(ApiSystem {
        worker_handle,
        bridge_handles,
        shutdown_tx,
        http_handler,
    })
}
