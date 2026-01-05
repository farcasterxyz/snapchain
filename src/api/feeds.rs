//! Feed aggregation for Farcaster API.
//!
//! This module provides feed generation without requiring a persistent feed index.
//! Feeds are computed on-demand using existing indexes (social graph, metrics) and stores.
//!
//! # Supported Feeds
//!
//! - **Following Feed**: Casts from users the requester follows, sorted by timestamp
//! - **Trending Feed**: Top casts by engagement score from the metrics index
//!
//! # Usage
//!
//! ```ignore
//! let feed_service = FeedService::new(config, social_graph, metrics, hub_service);
//! let feed = feed_service.get_following_feed(fid, cursor, limit).await?;
//! ```

use crate::api::config::FeedConfig;
use crate::api::metrics::MetricsIndexer;
use crate::api::social_graph::SocialGraphIndexer;
use crate::proto::{self, Message};
use async_trait::async_trait;
use serde::Serialize;
use std::collections::BinaryHeap;
use std::sync::Arc;
use thiserror::Error;
use tonic::Request;

/// Errors that can occur during feed generation.
#[derive(Error, Debug)]
pub enum FeedError {
    #[error("Feature disabled")]
    FeatureDisabled,

    #[error("Service error: {0}")]
    ServiceError(String),

    #[error("Invalid cursor")]
    InvalidCursor,

    #[error("User not found: {0}")]
    UserNotFound(u64),
}

/// A single item in a feed.
#[derive(Debug, Clone, Serialize)]
pub struct FeedItem {
    /// The cast message.
    pub cast: Message,
    /// Engagement metrics for the cast.
    pub likes: u64,
    pub recasts: u64,
    pub replies: u64,
    /// Engagement score (for ranking).
    pub score: f64,
}

/// A feed response with pagination.
#[derive(Debug, Clone, Serialize)]
pub struct FeedResponse {
    /// Feed items.
    pub items: Vec<FeedItem>,
    /// Cursor for next page (timestamp-based for following, score-based for trending).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    /// Total items available (if known).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<usize>,
}

/// Trait for feed handler to allow type erasure.
#[async_trait]
pub trait FeedHandler: Send + Sync {
    /// Check if feeds are enabled.
    fn is_enabled(&self) -> bool;

    /// Get the following feed for a user.
    async fn get_following_feed(
        &self,
        fid: u64,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<FeedResponse, FeedError>;

    /// Get the trending feed.
    async fn get_trending_feed(
        &self,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<FeedResponse, FeedError>;
}

/// Helper struct for merge-sorting casts by timestamp.
#[derive(Debug)]
struct TimestampedCast {
    timestamp: u32,
    fid: u64,
    hash: Vec<u8>,
    cast: Message,
}

impl PartialEq for TimestampedCast {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp && self.fid == other.fid && self.hash == other.hash
    }
}

impl Eq for TimestampedCast {}

impl Ord for TimestampedCast {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Natural order for max-heap (newest/highest timestamp first)
        self.timestamp
            .cmp(&other.timestamp)
            .then_with(|| self.fid.cmp(&other.fid))
    }
}

impl PartialOrd for TimestampedCast {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Feed service for generating user feeds.
pub struct FeedService<S> {
    config: FeedConfig,
    social_graph: Option<Arc<SocialGraphIndexer>>,
    metrics: Option<Arc<MetricsIndexer>>,
    hub_service: Arc<S>,
}

impl<S> FeedService<S>
where
    S: proto::hub_service_server::HubService + Send + Sync + 'static,
{
    /// Create a new feed service.
    pub fn new(
        config: FeedConfig,
        social_graph: Option<Arc<SocialGraphIndexer>>,
        metrics: Option<Arc<MetricsIndexer>>,
        hub_service: Arc<S>,
    ) -> Self {
        Self {
            config,
            social_graph,
            metrics,
            hub_service,
        }
    }

    /// Check if feeds are enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get the following feed for a user.
    ///
    /// Returns casts from users that the given FID follows, sorted by timestamp (newest first).
    pub async fn get_following_feed(
        &self,
        fid: u64,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<FeedResponse, FeedError> {
        if !self.config.enabled {
            return Err(FeedError::FeatureDisabled);
        }

        let social_graph = self.social_graph.as_ref().ok_or_else(|| {
            FeedError::ServiceError("Social graph indexer not available".to_string())
        })?;

        // Parse cursor (timestamp to start from)
        let start_timestamp: Option<u32> = cursor.and_then(|c| c.parse().ok());

        // Get users this FID follows
        let (following, _) = social_graph
            .get_following(fid, None, self.config.max_following_fetch)
            .map_err(|e| FeedError::ServiceError(format!("Failed to get following: {:?}", e)))?;

        if following.is_empty() {
            return Ok(FeedResponse {
                items: vec![],
                next_cursor: None,
                total: Some(0),
            });
        }

        // Fetch recent casts from each followed user and merge-sort
        let limit = limit.min(100); // Cap at 100
        let mut heap: BinaryHeap<TimestampedCast> = BinaryHeap::new();

        for followed_fid in following.iter().take(self.config.max_following_fetch) {
            let casts = self
                .fetch_user_casts(*followed_fid, start_timestamp, limit)
                .await?;
            for cast in casts {
                if let Some(data) = &cast.data {
                    heap.push(TimestampedCast {
                        timestamp: data.timestamp,
                        fid: data.fid,
                        hash: cast.hash.clone(),
                        cast,
                    });
                }
            }
        }

        // Extract top N items
        let mut items = Vec::with_capacity(limit);
        let mut last_timestamp = None;

        while let Some(tc) = heap.pop() {
            if items.len() >= limit {
                break;
            }

            // Get metrics for this cast
            let (likes, recasts, replies, score) = self.get_cast_metrics(tc.fid, &tc.hash);

            items.push(FeedItem {
                cast: tc.cast,
                likes,
                recasts,
                replies,
                score,
            });

            last_timestamp = Some(tc.timestamp);
        }

        // Generate next cursor if there are more items
        let next_cursor = if heap.is_empty() {
            None
        } else {
            last_timestamp.map(|ts| ts.to_string())
        };

        Ok(FeedResponse {
            items,
            next_cursor,
            total: None,
        })
    }

    /// Get the trending feed.
    ///
    /// Returns casts sorted by engagement score (highest first).
    pub async fn get_trending_feed(
        &self,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<FeedResponse, FeedError> {
        if !self.config.enabled {
            return Err(FeedError::FeatureDisabled);
        }

        let metrics = self
            .metrics
            .as_ref()
            .ok_or_else(|| FeedError::ServiceError("Metrics indexer not available".to_string()))?;

        // Parse cursor (offset)
        let offset: usize = cursor.and_then(|c| c.parse().ok()).unwrap_or(0);
        let limit = limit.min(100);

        // Get trending casts from metrics indexer
        let trending = metrics
            .get_trending_casts(limit + offset)
            .map_err(|e| FeedError::ServiceError(format!("Failed to get trending: {:?}", e)))?;

        // Skip to offset
        let trending: Vec<_> = trending.into_iter().skip(offset).take(limit).collect();

        // Fetch full cast data for each trending cast
        let mut items = Vec::with_capacity(trending.len());
        for (fid, hash, score) in &trending {
            match self.fetch_cast(*fid, hash).await {
                Ok(cast) => {
                    let metrics_data = metrics.get_cast_metrics(*fid, hash).unwrap_or_default();

                    items.push(FeedItem {
                        cast,
                        likes: metrics_data.likes,
                        recasts: metrics_data.recasts,
                        replies: metrics_data.replies,
                        score: *score,
                    });
                }
                Err(_) => {
                    // Cast may have been deleted, skip it
                    continue;
                }
            }
        }

        // Generate next cursor
        let next_cursor = if items.len() == limit {
            Some((offset + limit).to_string())
        } else {
            None
        };

        Ok(FeedResponse {
            items,
            next_cursor,
            total: None,
        })
    }

    /// Fetch casts from a user, optionally filtered by timestamp.
    async fn fetch_user_casts(
        &self,
        fid: u64,
        before_timestamp: Option<u32>,
        limit: usize,
    ) -> Result<Vec<Message>, FeedError> {
        let request = proto::FidRequest {
            fid,
            page_size: Some(limit as u32),
            page_token: None,
            reverse: Some(true), // Newest first
        };

        let response = self
            .hub_service
            .get_casts_by_fid(Request::new(request))
            .await
            .map_err(|e| FeedError::ServiceError(e.message().to_string()))?;

        let messages = response.into_inner().messages;

        // Filter by timestamp if cursor provided
        if let Some(ts) = before_timestamp {
            Ok(messages
                .into_iter()
                .filter(|m| m.data.as_ref().map(|d| d.timestamp < ts).unwrap_or(false))
                .collect())
        } else {
            Ok(messages)
        }
    }

    /// Fetch a single cast.
    async fn fetch_cast(&self, fid: u64, hash: &[u8]) -> Result<Message, FeedError> {
        let cast_id = proto::CastId {
            fid,
            hash: hash.to_vec(),
        };

        let response = self
            .hub_service
            .get_cast(Request::new(cast_id))
            .await
            .map_err(|e| FeedError::ServiceError(e.message().to_string()))?;

        Ok(response.into_inner())
    }

    /// Get metrics for a cast.
    fn get_cast_metrics(&self, fid: u64, hash: &[u8]) -> (u64, u64, u64, f64) {
        if let Some(metrics) = &self.metrics {
            if let Ok(m) = metrics.get_cast_metrics(fid, hash) {
                let score = m.likes as f64 + (m.recasts as f64 * 2.0) + (m.replies as f64 * 3.0);
                return (m.likes, m.recasts, m.replies, score);
            }
        }
        (0, 0, 0, 0.0)
    }
}

/// Implement FeedHandler trait for type-erased access in HTTP handler.
#[async_trait]
impl<S> FeedHandler for FeedService<S>
where
    S: proto::hub_service_server::HubService + Send + Sync + 'static,
{
    fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    async fn get_following_feed(
        &self,
        fid: u64,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<FeedResponse, FeedError> {
        FeedService::get_following_feed(self, fid, cursor, limit).await
    }

    async fn get_trending_feed(
        &self,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<FeedResponse, FeedError> {
        FeedService::get_trending_feed(self, cursor, limit).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamped_cast_ordering() {
        // Test that the heap orders by timestamp descending (newest first)
        let mut heap: BinaryHeap<TimestampedCast> = BinaryHeap::new();

        heap.push(TimestampedCast {
            timestamp: 100,
            fid: 1,
            hash: vec![1],
            cast: Message::default(),
        });
        heap.push(TimestampedCast {
            timestamp: 300,
            fid: 2,
            hash: vec![2],
            cast: Message::default(),
        });
        heap.push(TimestampedCast {
            timestamp: 200,
            fid: 3,
            hash: vec![3],
            cast: Message::default(),
        });

        // Should pop in order: 300, 200, 100 (newest first)
        assert_eq!(heap.pop().unwrap().timestamp, 300);
        assert_eq!(heap.pop().unwrap().timestamp, 200);
        assert_eq!(heap.pop().unwrap().timestamp, 100);
    }

    #[test]
    fn test_feed_response_serialization() {
        let response = FeedResponse {
            items: vec![],
            next_cursor: Some("12345".to_string()),
            total: Some(100),
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"next_cursor\":\"12345\""));
        assert!(json.contains("\"total\":100"));
    }

    #[test]
    fn test_feed_response_no_cursor() {
        let response = FeedResponse {
            items: vec![],
            next_cursor: None,
            total: None,
        };

        let json = serde_json::to_string(&response).unwrap();
        // Should not contain next_cursor or total when None
        assert!(!json.contains("next_cursor"));
        assert!(!json.contains("total"));
    }
}
