//! HTTP endpoints for Farcaster API compatibility.
//!
//! These endpoints match the Farcaster v2 API specification for compatibility
//! with existing Farcaster SDK clients.

use crate::api::channels::ChannelsIndexer;
use crate::api::conversations::{Conversation as ConversationData, ConversationError};
use crate::api::feeds::{FeedError, FeedHandler};
use crate::api::indexer::Indexer;
use crate::api::metrics::MetricsIndexer;
use crate::api::search::SearchIndexer;
use crate::api::social_graph::SocialGraphIndexer;
use crate::api::types::{
    Bio, Cast, CastReactions, CastReplies, CastWithReplies, CastsSearchResponse, CastsSearchResult,
    Channel, ChannelMember, ChannelMemberListResponse, ChannelResponse, Conversation,
    ConversationResponse, ErrorResponse, FeedResponse, Follower, FollowersResponse, NextCursor,
    User, UserProfile, VerifiedAddresses,
};
use async_trait::async_trait;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::{Method, Request, Response, StatusCode};
use serde::Serialize;
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;

/// Trait for conversation handler to allow type erasure.
/// This allows the HTTP handler to use any HubService implementation.
#[async_trait]
pub trait ConversationHandler: Send + Sync {
    /// Check if conversations are enabled.
    fn is_enabled(&self) -> bool;

    /// Get a conversation thread.
    async fn get_conversation(
        &self,
        fid: u64,
        hash: &[u8],
        depth: Option<u32>,
    ) -> Result<ConversationData, ConversationError>;
}

/// Trait for hydrating user data from the hub.
#[async_trait]
pub trait UserHydrator: Send + Sync {
    /// Hydrate a user by FID.
    async fn hydrate_user(&self, fid: u64) -> Option<User>;

    /// Hydrate multiple users by FID.
    async fn hydrate_users(&self, fids: &[u64]) -> Vec<User>;
}

/// Farcaster HTTP handler for v2 API endpoints.
#[derive(Clone)]
pub struct ApiHttpHandler {
    social_graph: Option<Arc<SocialGraphIndexer>>,
    channels: Option<Arc<ChannelsIndexer>>,
    metrics: Option<Arc<MetricsIndexer>>,
    conversations: Option<Arc<dyn ConversationHandler>>,
    feeds: Option<Arc<dyn FeedHandler>>,
    search: Option<Arc<SearchIndexer>>,
    user_hydrator: Option<Arc<dyn UserHydrator>>,
}

impl ApiHttpHandler {
    /// Create a new handler with optional indexers.
    pub fn new(
        social_graph: Option<Arc<SocialGraphIndexer>>,
        channels: Option<Arc<ChannelsIndexer>>,
        metrics: Option<Arc<MetricsIndexer>>,
    ) -> Self {
        Self {
            social_graph,
            channels,
            metrics,
            conversations: None,
            feeds: None,
            search: None,
            user_hydrator: None,
        }
    }

    /// Set the conversation handler.
    pub fn with_conversations(mut self, handler: Arc<dyn ConversationHandler>) -> Self {
        self.conversations = Some(handler);
        self
    }

    /// Set the feed handler.
    pub fn with_feeds(mut self, handler: Arc<dyn FeedHandler>) -> Self {
        self.feeds = Some(handler);
        self
    }

    /// Set the search indexer.
    pub fn with_search(mut self, indexer: Arc<SearchIndexer>) -> Self {
        self.search = Some(indexer);
        self
    }

    /// Set the user hydrator.
    pub fn with_user_hydrator(mut self, hydrator: Arc<dyn UserHydrator>) -> Self {
        self.user_hydrator = Some(hydrator);
        self
    }

    /// Check if this handler can handle the given request.
    pub fn can_handle(&self, method: &Method, path: &str) -> bool {
        if method != Method::GET {
            return false;
        }

        // All Farcaster v2 endpoints start with /v2/farcaster/
        if !path.starts_with("/v2/farcaster/") {
            return false;
        }

        // Followers/following endpoints
        if path == "/v2/farcaster/followers" || path == "/v2/farcaster/followers/" {
            return true;
        }
        if path == "/v2/farcaster/following" || path == "/v2/farcaster/following/" {
            return true;
        }

        // Channel endpoints
        if path == "/v2/farcaster/channel" || path == "/v2/farcaster/channel/" {
            return true;
        }
        if path == "/v2/farcaster/channel/member/list"
            || path == "/v2/farcaster/channel/member/list/"
        {
            return true;
        }

        // Cast endpoints
        if path == "/v2/farcaster/cast/search" || path == "/v2/farcaster/cast/search/" {
            return true;
        }
        if path == "/v2/farcaster/cast/conversation" || path == "/v2/farcaster/cast/conversation/" {
            return true;
        }

        // Feed endpoints
        if path == "/v2/farcaster/feed/following" || path == "/v2/farcaster/feed/following/" {
            return true;
        }
        if path == "/v2/farcaster/feed/trending" || path == "/v2/farcaster/feed/trending/" {
            return true;
        }

        false
    }

    /// Handle a request.
    pub async fn handle(
        &self,
        req: Request<hyper::body::Incoming>,
    ) -> Result<Response<BoxBody<Bytes, Infallible>>, Infallible> {
        let path = req.uri().path().trim_end_matches('/');
        let query = req.uri().query().unwrap_or("");

        // Parse query parameters
        let params = Self::parse_query_params(query);

        // Common parameters
        let limit: usize = params
            .get("limit")
            .and_then(|s| s.parse().ok())
            .unwrap_or(25)
            .min(100);
        let cursor = params.get("cursor").cloned();
        let _viewer_fid: Option<u64> = params.get("viewer_fid").and_then(|s| s.parse().ok());

        // Route to appropriate handler
        match path {
            "/v2/farcaster/followers" => {
                let fid = match params.get("fid").and_then(|s| s.parse().ok()) {
                    Some(fid) => fid,
                    None => {
                        return Ok(Self::error_response(
                            StatusCode::BAD_REQUEST,
                            "Missing required parameter: fid",
                        ))
                    }
                };
                self.handle_followers(fid, cursor.as_deref(), limit).await
            }

            "/v2/farcaster/following" => {
                let fid = match params.get("fid").and_then(|s| s.parse().ok()) {
                    Some(fid) => fid,
                    None => {
                        return Ok(Self::error_response(
                            StatusCode::BAD_REQUEST,
                            "Missing required parameter: fid",
                        ))
                    }
                };
                self.handle_following(fid, cursor.as_deref(), limit).await
            }

            "/v2/farcaster/channel" => {
                let id = match params.get("id") {
                    Some(id) => id.clone(),
                    None => {
                        return Ok(Self::error_response(
                            StatusCode::BAD_REQUEST,
                            "Missing required parameter: id",
                        ))
                    }
                };
                let id_type = params.get("type").map(|s| s.as_str()).unwrap_or("id");
                self.handle_channel(&id, id_type).await
            }

            "/v2/farcaster/channel/member/list" => {
                let channel_id = match params.get("channel_id") {
                    Some(id) => id.clone(),
                    None => {
                        return Ok(Self::error_response(
                            StatusCode::BAD_REQUEST,
                            "Missing required parameter: channel_id",
                        ))
                    }
                };
                self.handle_channel_members(&channel_id, cursor.as_deref(), limit)
                    .await
            }

            "/v2/farcaster/cast/search" => {
                let q = match params.get("q") {
                    Some(q) if !q.is_empty() => q.clone(),
                    _ => {
                        return Ok(Self::error_response(
                            StatusCode::BAD_REQUEST,
                            "Missing required parameter: q",
                        ))
                    }
                };
                self.handle_cast_search(&q, cursor.as_deref(), limit).await
            }

            "/v2/farcaster/cast/conversation" => {
                let identifier = match params.get("identifier") {
                    Some(id) => id.clone(),
                    None => {
                        return Ok(Self::error_response(
                            StatusCode::BAD_REQUEST,
                            "Missing required parameter: identifier",
                        ))
                    }
                };
                let id_type = match params.get("type") {
                    Some(t) => t.clone(),
                    None => {
                        return Ok(Self::error_response(
                            StatusCode::BAD_REQUEST,
                            "Missing required parameter: type",
                        ))
                    }
                };
                let reply_depth: u32 = params
                    .get("reply_depth")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(2)
                    .min(5);
                self.handle_conversation(&identifier, &id_type, reply_depth)
                    .await
            }

            "/v2/farcaster/feed/following" => {
                let fid = match params.get("fid").and_then(|s| s.parse().ok()) {
                    Some(fid) => fid,
                    None => {
                        return Ok(Self::error_response(
                            StatusCode::BAD_REQUEST,
                            "Missing required parameter: fid",
                        ))
                    }
                };
                self.handle_following_feed(fid, cursor.as_deref(), limit)
                    .await
            }

            "/v2/farcaster/feed/trending" => {
                let _time_window = params
                    .get("time_window")
                    .map(|s| s.as_str())
                    .unwrap_or("24h");
                self.handle_trending_feed(cursor.as_deref(), limit).await
            }

            _ => Ok(Self::error_response(
                StatusCode::NOT_FOUND,
                "Endpoint not found",
            )),
        }
    }

    /// Parse query string into key-value pairs.
    fn parse_query_params(query: &str) -> HashMap<String, String> {
        query
            .split('&')
            .filter_map(|pair| {
                let mut parts = pair.splitn(2, '=');
                match (parts.next(), parts.next()) {
                    (Some(key), Some(value)) if !key.is_empty() => {
                        // URL decode the value
                        let decoded = urlencoding::decode(value).unwrap_or_else(|_| value.into());
                        Some((key.to_string(), decoded.to_string()))
                    }
                    _ => None,
                }
            })
            .collect()
    }

    /// Create a stub user for an FID (when hydration is not available).
    fn stub_user(fid: u64) -> User {
        User {
            object: "user".to_string(),
            fid,
            username: format!("fid:{}", fid),
            display_name: None,
            custody_address: String::new(),
            pfp_url: None,
            profile: UserProfile {
                bio: Bio {
                    text: String::new(),
                },
            },
            follower_count: 0,
            following_count: 0,
            verifications: Vec::new(),
            verified_addresses: VerifiedAddresses::default(),
            viewer_context: None,
        }
    }

    /// Hydrate a user or return stub.
    async fn get_user(&self, fid: u64) -> User {
        if let Some(hydrator) = &self.user_hydrator {
            if let Some(user) = hydrator.hydrate_user(fid).await {
                return user;
            }
        }
        Self::stub_user(fid)
    }

    /// Hydrate multiple users or return stubs.
    async fn get_users(&self, fids: &[u64]) -> Vec<User> {
        if let Some(hydrator) = &self.user_hydrator {
            let users = hydrator.hydrate_users(fids).await;
            if !users.is_empty() {
                return users;
            }
        }
        fids.iter().map(|&fid| Self::stub_user(fid)).collect()
    }

    /// Handle GET /v2/farcaster/followers/?fid=X
    async fn handle_followers(
        &self,
        fid: u64,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Response<BoxBody<Bytes, Infallible>>, Infallible> {
        let Some(indexer) = &self.social_graph else {
            return Ok(Self::error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "Social graph indexing not enabled",
            ));
        };

        let cursor_u64: Option<u64> = cursor.and_then(|s| s.parse().ok());

        match indexer.get_followers(fid, cursor_u64, limit) {
            Ok((follower_fids, next_cursor)) => {
                let users = self.get_users(&follower_fids).await;
                let followers: Vec<Follower> = users
                    .into_iter()
                    .map(|user| Follower {
                        object: "follower".to_string(),
                        user,
                    })
                    .collect();

                let response = FollowersResponse {
                    users: followers,
                    next: NextCursor {
                        cursor: next_cursor.map(|c| c.to_string()),
                    },
                };
                Ok(Self::json_response(StatusCode::OK, &response))
            }
            Err(e) => Ok(Self::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("Failed to get followers: {:?}", e),
            )),
        }
    }

    /// Handle GET /v2/farcaster/following/?fid=X
    async fn handle_following(
        &self,
        fid: u64,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Response<BoxBody<Bytes, Infallible>>, Infallible> {
        let Some(indexer) = &self.social_graph else {
            return Ok(Self::error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "Social graph indexing not enabled",
            ));
        };

        let cursor_u64: Option<u64> = cursor.and_then(|s| s.parse().ok());

        match indexer.get_following(fid, cursor_u64, limit) {
            Ok((following_fids, next_cursor)) => {
                let users = self.get_users(&following_fids).await;
                let followers: Vec<Follower> = users
                    .into_iter()
                    .map(|user| Follower {
                        object: "follower".to_string(),
                        user,
                    })
                    .collect();

                let response = FollowersResponse {
                    users: followers,
                    next: NextCursor {
                        cursor: next_cursor.map(|c| c.to_string()),
                    },
                };
                Ok(Self::json_response(StatusCode::OK, &response))
            }
            Err(e) => Ok(Self::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("Failed to get following: {:?}", e),
            )),
        }
    }

    /// Handle GET /v2/farcaster/channel/?id=X&type=id|parent_url
    async fn handle_channel(
        &self,
        id: &str,
        id_type: &str,
    ) -> Result<Response<BoxBody<Bytes, Infallible>>, Infallible> {
        let Some(indexer) = &self.channels else {
            return Ok(Self::error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "Channels indexing not enabled",
            ));
        };

        // If type is parent_url, use the id as the URL directly
        // Otherwise, we'd need a channel registry to look up by id
        let channel_url = if id_type == "parent_url" {
            id.to_string()
        } else {
            // For now, treat id as the URL if we don't have a registry
            id.to_string()
        };

        match indexer.get_channel(&channel_url) {
            Ok(Some(info)) => {
                let channel = Channel {
                    object: "channel".to_string(),
                    id: id.to_string(),
                    name: id.to_string(), // Would need registry for real name
                    image_url: None,
                    parent_url: Some(info.url),
                    description: None,
                    follower_count: None,
                    member_count: Some(info.stats.member_count),
                };
                let response = ChannelResponse { channel };
                Ok(Self::json_response(StatusCode::OK, &response))
            }
            Ok(None) => Ok(Self::error_response(
                StatusCode::NOT_FOUND,
                "Channel not found",
            )),
            Err(e) => Ok(Self::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("Failed to get channel: {:?}", e),
            )),
        }
    }

    /// Handle GET /v2/farcaster/channel/member/list/?channel_id=X
    async fn handle_channel_members(
        &self,
        channel_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Response<BoxBody<Bytes, Infallible>>, Infallible> {
        let Some(indexer) = &self.channels else {
            return Ok(Self::error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "Channels indexing not enabled",
            ));
        };

        let cursor_u64: Option<u64> = cursor.and_then(|s| s.parse().ok());

        match indexer.get_channel_members(channel_id, cursor_u64, limit) {
            Ok((member_fids, next_cursor)) => {
                let users = self.get_users(&member_fids).await;
                let members: Vec<ChannelMember> = users
                    .into_iter()
                    .map(|user| ChannelMember {
                        object: "channel_member".to_string(),
                        user,
                        role: "member".to_string(),
                    })
                    .collect();

                let response = ChannelMemberListResponse {
                    members,
                    next: NextCursor {
                        cursor: next_cursor.map(|c| c.to_string()),
                    },
                };
                Ok(Self::json_response(StatusCode::OK, &response))
            }
            Err(e) => Ok(Self::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("Failed to get channel members: {:?}", e),
            )),
        }
    }

    /// Handle GET /v2/farcaster/cast/search/?q=X
    async fn handle_cast_search(
        &self,
        query: &str,
        _cursor: Option<&str>,
        limit: usize,
    ) -> Result<Response<BoxBody<Bytes, Infallible>>, Infallible> {
        let Some(indexer) = &self.search else {
            return Ok(Self::error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "Search indexing not available",
            ));
        };

        if !indexer.is_enabled() {
            return Ok(Self::error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "Search feature disabled",
            ));
        }

        match indexer.search_casts(query, limit) {
            Ok(results) => {
                let mut casts = Vec::with_capacity(results.len());
                for result in results {
                    let author = self.get_user(result.fid).await;
                    casts.push(Cast {
                        object: "cast".to_string(),
                        hash: result.hash,
                        parent_hash: None,
                        parent_url: None,
                        root_parent_url: None,
                        author,
                        text: result.text,
                        timestamp: format_timestamp(result.timestamp),
                        embeds: Vec::new(),
                        reactions: CastReactions::default(),
                        replies: CastReplies::default(),
                        thread_hash: None,
                        channel: None,
                        viewer_context: None,
                    });
                }

                let response = CastsSearchResponse {
                    result: CastsSearchResult {
                        casts,
                        next: NextCursor { cursor: None },
                    },
                };
                Ok(Self::json_response(StatusCode::OK, &response))
            }
            Err(e) => Ok(Self::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("Search failed: {:?}", e),
            )),
        }
    }

    /// Handle GET /v2/farcaster/cast/conversation/?identifier=X&type=hash|url
    async fn handle_conversation(
        &self,
        identifier: &str,
        id_type: &str,
        reply_depth: u32,
    ) -> Result<Response<BoxBody<Bytes, Infallible>>, Infallible> {
        let Some(handler) = &self.conversations else {
            return Ok(Self::error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "Conversation service not available",
            ));
        };

        if !handler.is_enabled() {
            return Ok(Self::error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "Conversation feature disabled",
            ));
        }

        // Parse identifier based on type
        let (fid, hash) = if id_type == "hash" {
            // identifier format: "0x..." or just hex hash
            // We need the FID to look up the cast - for now require it in identifier
            // Format: "fid:hash" or just "hash" (lookup needed)
            let hash_str = identifier.trim_start_matches("0x");
            let hash = match hex::decode(hash_str) {
                Ok(h) => h,
                Err(_) => {
                    return Ok(Self::error_response(
                        StatusCode::BAD_REQUEST,
                        "Invalid hash format",
                    ))
                }
            };
            // We need FID - for now, return error if not provided
            // In full implementation, would look up cast by hash
            (0u64, hash)
        } else {
            // URL format - would need to parse Warpcast URL
            return Ok(Self::error_response(
                StatusCode::BAD_REQUEST,
                "URL identifier type not yet supported",
            ));
        };

        match handler
            .get_conversation(fid, &hash, Some(reply_depth))
            .await
        {
            Ok(conversation) => {
                let root_cast = self.conversation_cast_to_cast(&conversation.root).await;
                let cast_with_replies = CastWithReplies {
                    cast: root_cast,
                    direct_replies: Vec::new(), // Would need recursive conversion
                };

                let response = ConversationResponse {
                    conversation: Conversation {
                        cast: cast_with_replies,
                    },
                };
                Ok(Self::json_response(StatusCode::OK, &response))
            }
            Err(ConversationError::CastNotFound(msg)) => {
                Ok(Self::error_response(StatusCode::NOT_FOUND, &msg))
            }
            Err(ConversationError::FeatureDisabled) => Ok(Self::error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "Conversation feature disabled",
            )),
            Err(e) => Ok(Self::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("Failed to get conversation: {:?}", e),
            )),
        }
    }

    /// Convert internal conversation cast to Farcaster Cast format.
    async fn conversation_cast_to_cast(
        &self,
        conv_cast: &crate::api::conversations::ConversationCast,
    ) -> Cast {
        // Extract data from the Message
        let (fid, hash, text, timestamp, parent_hash, parent_url) =
            if let Some(data) = &conv_cast.cast.data {
                let text = match &data.body {
                    Some(crate::proto::message_data::Body::CastAddBody(body)) => body.text.clone(),
                    _ => String::new(),
                };
                let (parent_hash, parent_url) = match &data.body {
                    Some(crate::proto::message_data::Body::CastAddBody(body)) => {
                        let ph = match &body.parent {
                            Some(crate::proto::cast_add_body::Parent::ParentCastId(id)) => {
                                Some(hex::encode(&id.hash))
                            }
                            _ => None,
                        };
                        let pu = match &body.parent {
                            Some(crate::proto::cast_add_body::Parent::ParentUrl(url)) => {
                                Some(url.clone())
                            }
                            _ => None,
                        };
                        (ph, pu)
                    }
                    _ => (None, None),
                };
                (
                    data.fid,
                    conv_cast.cast.hash.clone(),
                    text,
                    data.timestamp,
                    parent_hash,
                    parent_url,
                )
            } else {
                (0, conv_cast.cast.hash.clone(), String::new(), 0, None, None)
            };

        let author = self.get_user(fid).await;
        Cast {
            object: "cast".to_string(),
            hash: hex::encode(&hash),
            parent_hash,
            parent_url,
            root_parent_url: None,
            author,
            text,
            timestamp: format_timestamp(timestamp),
            embeds: Vec::new(),
            reactions: CastReactions::default(),
            replies: CastReplies {
                count: conv_cast.replies.len() as u64,
            },
            thread_hash: None,
            channel: None,
            viewer_context: None,
        }
    }

    /// Handle GET /v2/farcaster/feed/following/?fid=X
    async fn handle_following_feed(
        &self,
        fid: u64,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Response<BoxBody<Bytes, Infallible>>, Infallible> {
        let Some(handler) = &self.feeds else {
            return Ok(Self::error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "Feed service not available",
            ));
        };

        if !handler.is_enabled() {
            return Ok(Self::error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "Feeds feature disabled",
            ));
        }

        match handler.get_following_feed(fid, cursor, limit).await {
            Ok(feed_response) => {
                // Convert internal feed response to Farcaster format
                let casts = self.convert_feed_items(&feed_response.items).await;
                let response = FeedResponse {
                    casts,
                    next: NextCursor {
                        cursor: feed_response.next_cursor,
                    },
                };
                Ok(Self::json_response(StatusCode::OK, &response))
            }
            Err(FeedError::FeatureDisabled) => Ok(Self::error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "Feeds feature disabled",
            )),
            Err(FeedError::UserNotFound(fid)) => Ok(Self::error_response(
                StatusCode::NOT_FOUND,
                &format!("User not found: {}", fid),
            )),
            Err(e) => Ok(Self::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("Failed to get following feed: {:?}", e),
            )),
        }
    }

    /// Handle GET /v2/farcaster/feed/trending/
    async fn handle_trending_feed(
        &self,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Response<BoxBody<Bytes, Infallible>>, Infallible> {
        let Some(handler) = &self.feeds else {
            return Ok(Self::error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "Feed service not available",
            ));
        };

        if !handler.is_enabled() {
            return Ok(Self::error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "Feeds feature disabled",
            ));
        }

        match handler.get_trending_feed(cursor, limit).await {
            Ok(feed_response) => {
                let casts = self.convert_feed_items(&feed_response.items).await;
                let response = FeedResponse {
                    casts,
                    next: NextCursor {
                        cursor: feed_response.next_cursor,
                    },
                };
                Ok(Self::json_response(StatusCode::OK, &response))
            }
            Err(FeedError::FeatureDisabled) => Ok(Self::error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "Feeds feature disabled",
            )),
            Err(e) => Ok(Self::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("Failed to get trending feed: {:?}", e),
            )),
        }
    }

    /// Convert internal feed items to Farcaster Cast format.
    async fn convert_feed_items(&self, feed_items: &[crate::api::feeds::FeedItem]) -> Vec<Cast> {
        let mut casts = Vec::with_capacity(feed_items.len());
        for item in feed_items {
            // Extract data from the Message
            let (fid, hash, text, timestamp, parent_hash, parent_url) = if let Some(data) =
                &item.cast.data
            {
                let text = match &data.body {
                    Some(crate::proto::message_data::Body::CastAddBody(body)) => body.text.clone(),
                    _ => String::new(),
                };
                let (parent_hash, parent_url) = match &data.body {
                    Some(crate::proto::message_data::Body::CastAddBody(body)) => {
                        let ph = match &body.parent {
                            Some(crate::proto::cast_add_body::Parent::ParentCastId(id)) => {
                                Some(hex::encode(&id.hash))
                            }
                            _ => None,
                        };
                        let pu = match &body.parent {
                            Some(crate::proto::cast_add_body::Parent::ParentUrl(url)) => {
                                Some(url.clone())
                            }
                            _ => None,
                        };
                        (ph, pu)
                    }
                    _ => (None, None),
                };
                (
                    data.fid,
                    item.cast.hash.clone(),
                    text,
                    data.timestamp,
                    parent_hash,
                    parent_url,
                )
            } else {
                (0, item.cast.hash.clone(), String::new(), 0, None, None)
            };

            let author = self.get_user(fid).await;
            casts.push(Cast {
                object: "cast".to_string(),
                hash: hex::encode(&hash),
                parent_hash,
                parent_url,
                root_parent_url: None,
                author,
                text,
                timestamp: format_timestamp(timestamp),
                embeds: Vec::new(),
                reactions: CastReactions {
                    likes_count: item.likes,
                    recasts_count: item.recasts,
                    likes: Vec::new(),
                    recasts: Vec::new(),
                },
                replies: CastReplies {
                    count: item.replies,
                },
                thread_hash: None,
                channel: None,
                viewer_context: None,
            });
        }
        casts
    }

    /// Create a JSON response.
    fn json_response<T: Serialize>(
        status: StatusCode,
        body: &T,
    ) -> Response<BoxBody<Bytes, Infallible>> {
        let json = serde_json::to_string(body).unwrap_or_else(|_| "{}".to_string());
        Response::builder()
            .status(status)
            .header("Content-Type", "application/json")
            .body(BoxBody::new(
                Full::new(Bytes::from(json)).map_err(|_| unreachable!()),
            ))
            .unwrap()
    }

    /// Create an error response.
    fn error_response(status: StatusCode, message: &str) -> Response<BoxBody<Bytes, Infallible>> {
        let error = ErrorResponse {
            message: message.to_string(),
            code: None,
        };
        Self::json_response(status, &error)
    }
}

/// Format a Farcaster timestamp to ISO 8601.
fn format_timestamp(ts: u32) -> String {
    // Farcaster timestamps are seconds since Farcaster epoch (2021-01-01)
    let farcaster_epoch = 1609459200u64; // 2021-01-01 00:00:00 UTC
    let unix_ts = farcaster_epoch + ts as u64;
    chrono::DateTime::from_timestamp(unix_ts as i64, 0)
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
        .unwrap_or_else(|| ts.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_can_handle() {
        let handler = ApiHttpHandler::new(None, None, None);

        // Followers/following endpoints
        assert!(handler.can_handle(&Method::GET, "/v2/farcaster/followers"));
        assert!(handler.can_handle(&Method::GET, "/v2/farcaster/followers/"));
        assert!(handler.can_handle(&Method::GET, "/v2/farcaster/following"));
        assert!(handler.can_handle(&Method::GET, "/v2/farcaster/following/"));

        // Channel endpoints
        assert!(handler.can_handle(&Method::GET, "/v2/farcaster/channel"));
        assert!(handler.can_handle(&Method::GET, "/v2/farcaster/channel/"));
        assert!(handler.can_handle(&Method::GET, "/v2/farcaster/channel/member/list"));
        assert!(handler.can_handle(&Method::GET, "/v2/farcaster/channel/member/list/"));

        // Cast endpoints
        assert!(handler.can_handle(&Method::GET, "/v2/farcaster/cast/search"));
        assert!(handler.can_handle(&Method::GET, "/v2/farcaster/cast/search/"));
        assert!(handler.can_handle(&Method::GET, "/v2/farcaster/cast/conversation"));
        assert!(handler.can_handle(&Method::GET, "/v2/farcaster/cast/conversation/"));

        // Feed endpoints
        assert!(handler.can_handle(&Method::GET, "/v2/farcaster/feed/following"));
        assert!(handler.can_handle(&Method::GET, "/v2/farcaster/feed/following/"));
        assert!(handler.can_handle(&Method::GET, "/v2/farcaster/feed/trending"));
        assert!(handler.can_handle(&Method::GET, "/v2/farcaster/feed/trending/"));

        // Invalid - wrong method
        assert!(!handler.can_handle(&Method::POST, "/v2/farcaster/followers"));

        // Invalid - wrong prefix
        assert!(!handler.can_handle(&Method::GET, "/v2/user/123/followers"));
        assert!(!handler.can_handle(&Method::GET, "/v1/castById"));
    }

    #[test]
    fn test_parse_query_params() {
        let params = ApiHttpHandler::parse_query_params("fid=123&limit=50&cursor=abc");
        assert_eq!(params.get("fid"), Some(&"123".to_string()));
        assert_eq!(params.get("limit"), Some(&"50".to_string()));
        assert_eq!(params.get("cursor"), Some(&"abc".to_string()));

        let empty = ApiHttpHandler::parse_query_params("");
        assert!(empty.is_empty());
    }

    #[test]
    fn test_stub_user() {
        let user = ApiHttpHandler::stub_user(123);
        assert_eq!(user.fid, 123);
        assert_eq!(user.object, "user");
        assert_eq!(user.username, "fid:123");
    }

    #[test]
    fn test_format_timestamp() {
        // Test with a known timestamp
        let ts = format_timestamp(0);
        assert!(ts.starts_with("2021-01-01"));
    }

    #[test]
    fn test_json_response() {
        let response = ApiHttpHandler::json_response(
            StatusCode::OK,
            &ErrorResponse {
                message: "test".to_string(),
                code: None,
            },
        );
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn test_error_response() {
        let response = ApiHttpHandler::error_response(StatusCode::NOT_FOUND, "Not found");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
