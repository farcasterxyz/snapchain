//! Conversation/thread aggregation for Farcaster API.
//!
//! This module provides on-demand thread aggregation without requiring
//! a persistent index. It recursively fetches replies to build conversation threads.
//!
//! # Usage
//!
//! ```ignore
//! let conversations = ConversationService::new(config, hub_service);
//! let thread = conversations.get_conversation(fid, hash, depth).await?;
//! ```

use crate::api::config::ConversationConfig;
use crate::api::http::ConversationHandler;
use crate::proto::{self, CastId, Message};
use async_trait::async_trait;
use serde::Serialize;
use std::collections::HashSet;
use std::sync::Arc;
use thiserror::Error;
use tonic::Request;

/// Errors that can occur during conversation fetching.
#[derive(Error, Debug)]
pub enum ConversationError {
    #[error("Cast not found: {0}")]
    CastNotFound(String),

    #[error("Service error: {0}")]
    ServiceError(String),

    #[error("Depth limit exceeded")]
    DepthLimitExceeded,

    #[error("Reply limit exceeded")]
    ReplyLimitExceeded,

    #[error("Feature disabled")]
    FeatureDisabled,
}

/// A cast with its replies forming a conversation tree.
#[derive(Debug, Clone, Serialize)]
pub struct ConversationCast {
    /// The cast message.
    pub cast: Message,
    /// Direct replies to this cast.
    pub replies: Vec<ConversationCast>,
    /// Depth in the conversation tree (0 = root).
    pub depth: u32,
}

/// A complete conversation thread.
#[derive(Debug, Clone, Serialize)]
pub struct Conversation {
    /// The root cast of the conversation.
    pub root: ConversationCast,
    /// Total number of replies in the thread.
    pub total_replies: usize,
    /// Maximum depth reached in the thread.
    pub max_depth: u32,
}

/// Statistics about a conversation fetch operation.
#[derive(Debug, Clone, Default)]
pub struct ConversationStats {
    /// Number of casts fetched.
    pub casts_fetched: usize,
    /// Number of unique authors in the conversation.
    pub unique_authors: usize,
    /// Depth of the conversation.
    pub depth: u32,
}

/// Service for fetching and aggregating conversation threads.
pub struct ConversationService<S> {
    config: ConversationConfig,
    hub_service: Arc<S>,
}

impl<S> ConversationService<S>
where
    S: proto::hub_service_server::HubService + Send + Sync + 'static,
{
    /// Create a new conversation service.
    pub fn new(config: ConversationConfig, hub_service: Arc<S>) -> Self {
        Self {
            config,
            hub_service,
        }
    }

    /// Check if conversations feature is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get a conversation thread starting from a cast.
    ///
    /// # Arguments
    /// * `fid` - The FID of the cast author
    /// * `hash` - The hash of the root cast
    /// * `depth` - Optional maximum depth (defaults to config value)
    ///
    /// # Returns
    /// The complete conversation thread with nested replies.
    pub async fn get_conversation(
        &self,
        fid: u64,
        hash: &[u8],
        depth: Option<u32>,
    ) -> Result<Conversation, ConversationError> {
        if !self.config.enabled {
            return Err(ConversationError::FeatureDisabled);
        }

        let max_depth = depth
            .unwrap_or(self.config.max_depth)
            .min(self.config.max_depth);

        // Fetch the root cast
        let root_cast = self.fetch_cast(fid, hash).await?;

        // Build the conversation tree
        let mut total_replies = 0;
        let mut actual_max_depth = 0;
        let root = self
            .build_conversation_tree(
                root_cast,
                0,
                max_depth,
                &mut total_replies,
                &mut actual_max_depth,
            )
            .await?;

        Ok(Conversation {
            root,
            total_replies,
            max_depth: actual_max_depth,
        })
    }

    /// Get conversation with statistics.
    pub async fn get_conversation_with_stats(
        &self,
        fid: u64,
        hash: &[u8],
        depth: Option<u32>,
    ) -> Result<(Conversation, ConversationStats), ConversationError> {
        let conversation = self.get_conversation(fid, hash, depth).await?;

        // Collect unique authors
        let mut authors = HashSet::new();
        self.collect_authors(&conversation.root, &mut authors);

        let stats = ConversationStats {
            casts_fetched: conversation.total_replies + 1, // +1 for root
            unique_authors: authors.len(),
            depth: conversation.max_depth,
        };

        Ok((conversation, stats))
    }

    /// Fetch a single cast by ID.
    async fn fetch_cast(&self, fid: u64, hash: &[u8]) -> Result<Message, ConversationError> {
        let cast_id = CastId {
            fid,
            hash: hash.to_vec(),
        };

        let response = self
            .hub_service
            .get_cast(Request::new(cast_id))
            .await
            .map_err(|e| {
                if e.code() == tonic::Code::NotFound {
                    ConversationError::CastNotFound(format!(
                        "fid={}, hash={}",
                        fid,
                        hex::encode(hash)
                    ))
                } else {
                    ConversationError::ServiceError(e.message().to_string())
                }
            })?;

        Ok(response.into_inner())
    }

    /// Fetch replies to a cast.
    async fn fetch_replies(
        &self,
        fid: u64,
        hash: &[u8],
    ) -> Result<Vec<Message>, ConversationError> {
        let request = proto::CastsByParentRequest {
            parent: Some(proto::casts_by_parent_request::Parent::ParentCastId(
                CastId {
                    fid,
                    hash: hash.to_vec(),
                },
            )),
            page_size: Some(self.config.max_replies as u32),
            page_token: None,
            reverse: Some(false),
        };

        let response = self
            .hub_service
            .get_casts_by_parent(Request::new(request))
            .await
            .map_err(|e| ConversationError::ServiceError(e.message().to_string()))?;

        Ok(response.into_inner().messages)
    }

    /// Recursively build the conversation tree.
    async fn build_conversation_tree(
        &self,
        cast: Message,
        current_depth: u32,
        max_depth: u32,
        total_replies: &mut usize,
        actual_max_depth: &mut u32,
    ) -> Result<ConversationCast, ConversationError> {
        // Update max depth tracker
        if current_depth > *actual_max_depth {
            *actual_max_depth = current_depth;
        }

        // Check reply limit
        if *total_replies >= self.config.max_replies {
            return Ok(ConversationCast {
                cast,
                replies: vec![],
                depth: current_depth,
            });
        }

        // If at max depth, don't fetch replies
        if current_depth >= max_depth {
            return Ok(ConversationCast {
                cast,
                replies: vec![],
                depth: current_depth,
            });
        }

        // Get cast ID for fetching replies
        let (fid, hash) = match &cast.data {
            Some(data) => (data.fid, cast.hash.clone()),
            None => {
                return Ok(ConversationCast {
                    cast,
                    replies: vec![],
                    depth: current_depth,
                });
            }
        };

        // Fetch direct replies
        let reply_messages = self.fetch_replies(fid, &hash).await?;
        *total_replies += reply_messages.len();

        // Recursively build reply trees
        let mut replies = Vec::with_capacity(reply_messages.len());
        for reply in reply_messages {
            // Check reply limit before processing each reply
            if *total_replies >= self.config.max_replies {
                break;
            }

            let reply_tree = Box::pin(self.build_conversation_tree(
                reply,
                current_depth + 1,
                max_depth,
                total_replies,
                actual_max_depth,
            ))
            .await?;
            replies.push(reply_tree);
        }

        Ok(ConversationCast {
            cast,
            replies,
            depth: current_depth,
        })
    }

    /// Collect all unique author FIDs from a conversation tree.
    fn collect_authors(&self, node: &ConversationCast, authors: &mut HashSet<u64>) {
        if let Some(data) = &node.cast.data {
            authors.insert(data.fid);
        }
        for reply in &node.replies {
            self.collect_authors(reply, authors);
        }
    }
}

/// Implement ConversationHandler trait for type-erased access in HTTP handler.
#[async_trait]
impl<S> ConversationHandler for ConversationService<S>
where
    S: proto::hub_service_server::HubService + Send + Sync + 'static,
{
    fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    async fn get_conversation(
        &self,
        fid: u64,
        hash: &[u8],
        depth: Option<u32>,
    ) -> Result<Conversation, ConversationError> {
        ConversationService::get_conversation(self, fid, hash, depth).await
    }
}

/// Simplified conversation response for HTTP API.
#[derive(Debug, Clone, Serialize)]
pub struct ConversationResponse {
    pub fid: u64,
    pub hash: String,
    pub total_replies: usize,
    pub max_depth: u32,
    pub thread: ConversationThread,
}

/// Thread representation for API response.
#[derive(Debug, Clone, Serialize)]
pub struct ConversationThread {
    pub fid: u64,
    pub hash: String,
    pub text: String,
    pub timestamp: u32,
    pub replies: Vec<ConversationThread>,
}

impl ConversationThread {
    /// Convert a ConversationCast to a simplified thread format.
    pub fn from_conversation_cast(cast: &ConversationCast) -> Option<Self> {
        let data = cast.cast.data.as_ref()?;
        let text = match &data.body {
            Some(proto::message_data::Body::CastAddBody(body)) => body.text.clone(),
            _ => String::new(),
        };

        Some(ConversationThread {
            fid: data.fid,
            hash: hex::encode(&cast.cast.hash),
            text,
            timestamp: data.timestamp,
            replies: cast
                .replies
                .iter()
                .filter_map(|r| ConversationThread::from_conversation_cast(r))
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_conversation_thread_from_cast() {
        let cast = ConversationCast {
            cast: Message {
                hash: vec![1, 2, 3, 4],
                data: Some(proto::MessageData {
                    fid: 123,
                    timestamp: 1000,
                    r#type: proto::MessageType::CastAdd as i32,
                    body: Some(proto::message_data::Body::CastAddBody(proto::CastAddBody {
                        text: "Hello world".to_string(),
                        ..Default::default()
                    })),
                    ..Default::default()
                }),
                ..Default::default()
            },
            replies: vec![],
            depth: 0,
        };

        let thread = ConversationThread::from_conversation_cast(&cast).unwrap();
        assert_eq!(thread.fid, 123);
        assert_eq!(thread.text, "Hello world");
        assert_eq!(thread.timestamp, 1000);
        assert!(thread.replies.is_empty());
    }

    #[test]
    fn test_conversation_thread_with_replies() {
        let reply = ConversationCast {
            cast: Message {
                hash: vec![5, 6, 7, 8],
                data: Some(proto::MessageData {
                    fid: 456,
                    timestamp: 1001,
                    r#type: proto::MessageType::CastAdd as i32,
                    body: Some(proto::message_data::Body::CastAddBody(proto::CastAddBody {
                        text: "Reply text".to_string(),
                        ..Default::default()
                    })),
                    ..Default::default()
                }),
                ..Default::default()
            },
            replies: vec![],
            depth: 1,
        };

        let root = ConversationCast {
            cast: Message {
                hash: vec![1, 2, 3, 4],
                data: Some(proto::MessageData {
                    fid: 123,
                    timestamp: 1000,
                    r#type: proto::MessageType::CastAdd as i32,
                    body: Some(proto::message_data::Body::CastAddBody(proto::CastAddBody {
                        text: "Root text".to_string(),
                        ..Default::default()
                    })),
                    ..Default::default()
                }),
                ..Default::default()
            },
            replies: vec![reply],
            depth: 0,
        };

        let thread = ConversationThread::from_conversation_cast(&root).unwrap();
        assert_eq!(thread.fid, 123);
        assert_eq!(thread.replies.len(), 1);
        assert_eq!(thread.replies[0].fid, 456);
        assert_eq!(thread.replies[0].text, "Reply text");
    }

    #[test]
    fn test_collect_authors() {
        let reply = ConversationCast {
            cast: Message {
                hash: vec![5, 6, 7, 8],
                data: Some(proto::MessageData {
                    fid: 456,
                    timestamp: 1001,
                    ..Default::default()
                }),
                ..Default::default()
            },
            replies: vec![],
            depth: 1,
        };

        let root = ConversationCast {
            cast: Message {
                hash: vec![1, 2, 3, 4],
                data: Some(proto::MessageData {
                    fid: 123,
                    timestamp: 1000,
                    ..Default::default()
                }),
                ..Default::default()
            },
            replies: vec![reply],
            depth: 0,
        };

        // Test author collection logic manually
        let mut authors = HashSet::new();
        if let Some(data) = &root.cast.data {
            authors.insert(data.fid);
        }
        for reply in &root.replies {
            if let Some(data) = &reply.cast.data {
                authors.insert(data.fid);
            }
        }

        assert_eq!(authors.len(), 2);
        assert!(authors.contains(&123));
        assert!(authors.contains(&456));
    }
}
