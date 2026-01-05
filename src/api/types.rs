//! Farcaster API compatible response types.
//!
//! These types match the Farcaster v2 API response schemas for compatibility
//! with existing Farcaster SDK clients.

use serde::{Deserialize, Serialize};

/// User object matching Farcaster API schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub object: String,
    pub fid: u64,
    pub username: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    pub custody_address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pfp_url: Option<String>,
    pub profile: UserProfile,
    pub follower_count: u64,
    pub following_count: u64,
    pub verifications: Vec<String>,
    pub verified_addresses: VerifiedAddresses,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub viewer_context: Option<ViewerContext>,
}

impl Default for User {
    fn default() -> Self {
        Self {
            object: "user".to_string(),
            fid: 0,
            username: String::new(),
            display_name: None,
            custody_address: String::new(),
            pfp_url: None,
            profile: UserProfile::default(),
            follower_count: 0,
            following_count: 0,
            verifications: Vec::new(),
            verified_addresses: VerifiedAddresses::default(),
            viewer_context: None,
        }
    }
}

/// User profile with bio.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UserProfile {
    pub bio: Bio,
}

/// User bio.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Bio {
    pub text: String,
}

/// Verified addresses for a user.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VerifiedAddresses {
    pub eth_addresses: Vec<String>,
    pub sol_addresses: Vec<String>,
}

/// Viewer context showing relationship to the viewer.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ViewerContext {
    pub following: bool,
    pub followed_by: bool,
}

/// Cast viewer context.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CastViewerContext {
    pub liked: bool,
    pub recasted: bool,
}

/// Follower object wrapping a user.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Follower {
    pub object: String,
    pub user: User,
}

/// Cast object matching Farcaster API schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cast {
    pub object: String,
    pub hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_parent_url: Option<String>,
    pub author: User,
    pub text: String,
    pub timestamp: String,
    pub embeds: Vec<Embed>,
    pub reactions: CastReactions,
    pub replies: CastReplies,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<Channel>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub viewer_context: Option<CastViewerContext>,
}

impl Default for Cast {
    fn default() -> Self {
        Self {
            object: "cast".to_string(),
            hash: String::new(),
            parent_hash: None,
            parent_url: None,
            root_parent_url: None,
            author: User::default(),
            text: String::new(),
            timestamp: String::new(),
            embeds: Vec::new(),
            reactions: CastReactions::default(),
            replies: CastReplies::default(),
            thread_hash: None,
            channel: None,
            viewer_context: None,
        }
    }
}

/// Embed in a cast.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Embed {
    Url { url: String },
    Cast { cast_id: CastId },
}

/// Cast ID reference.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CastId {
    pub fid: u64,
    pub hash: String,
}

/// Reaction counts for a cast.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CastReactions {
    pub likes_count: u64,
    pub recasts_count: u64,
    #[serde(default)]
    pub likes: Vec<ReactionUser>,
    #[serde(default)]
    pub recasts: Vec<ReactionUser>,
}

/// User who reacted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReactionUser {
    pub fid: u64,
    pub fname: String,
}

/// Reply count for a cast.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CastReplies {
    pub count: u64,
}

/// Channel information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Channel {
    pub object: String,
    pub id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub follower_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub member_count: Option<u64>,
}

impl Default for Channel {
    fn default() -> Self {
        Self {
            object: "channel".to_string(),
            id: String::new(),
            name: String::new(),
            image_url: None,
            parent_url: None,
            description: None,
            follower_count: None,
            member_count: None,
        }
    }
}

/// Pagination cursor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NextCursor {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

// === Response Types ===

/// Response for followers/following endpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FollowersResponse {
    pub users: Vec<Follower>,
    pub next: NextCursor,
}

/// Response for feed endpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeedResponse {
    pub casts: Vec<Cast>,
    pub next: NextCursor,
}

/// Response for cast search.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CastsSearchResponse {
    pub result: CastsSearchResult,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CastsSearchResult {
    pub casts: Vec<Cast>,
    pub next: NextCursor,
}

/// Response for single channel lookup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelResponse {
    pub channel: Channel,
}

/// Response for channel member list.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelMemberListResponse {
    pub members: Vec<ChannelMember>,
    pub next: NextCursor,
}

/// Channel member.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelMember {
    pub object: String,
    pub user: User,
    pub role: String,
}

/// Response for conversation endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversationResponse {
    pub conversation: Conversation,
}

/// Conversation with cast and replies.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Conversation {
    pub cast: CastWithReplies,
}

/// Cast with nested replies.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CastWithReplies {
    #[serde(flatten)]
    pub cast: Cast,
    pub direct_replies: Vec<CastWithReplies>,
}

/// Error response matching Farcaster format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_serialization() {
        let user = User::default();
        let json = serde_json::to_string(&user).unwrap();
        assert!(json.contains("\"object\":\"user\""));
    }

    #[test]
    fn test_cast_serialization() {
        let cast = Cast::default();
        let json = serde_json::to_string(&cast).unwrap();
        assert!(json.contains("\"object\":\"cast\""));
    }

    #[test]
    fn test_followers_response_serialization() {
        let response = FollowersResponse {
            users: vec![],
            next: NextCursor {
                cursor: Some("abc".to_string()),
            },
        };
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"cursor\":\"abc\""));
    }
}
