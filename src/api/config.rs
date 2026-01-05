//! Configuration for Farcaster API features.

use serde::{Deserialize, Serialize};

/// Master configuration for all Farcaster API features.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiConfig {
    /// Master switch for all Farcaster features. When false, no indexing overhead.
    #[serde(default)]
    pub enabled: bool,

    /// Social graph indexing (followers/following).
    #[serde(default)]
    pub social_graph: FeatureConfig,

    /// Channel indexing (registry, membership, activity).
    #[serde(default)]
    pub channels: FeatureConfig,

    /// Engagement metrics (likes, recasts, replies, trending).
    #[serde(default)]
    pub metrics: MetricsConfig,

    /// Full-text search indexing.
    #[serde(default)]
    pub search: SearchConfig,

    /// Feed generation settings.
    #[serde(default)]
    pub feeds: FeedConfig,

    /// Conversation/thread aggregation.
    #[serde(default)]
    pub conversations: ConversationConfig,

    /// AI-powered features (summaries).
    #[serde(default)]
    pub ai: AiConfig,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            social_graph: FeatureConfig::default(),
            channels: FeatureConfig::default(),
            metrics: MetricsConfig::default(),
            search: SearchConfig::default(),
            feeds: FeedConfig::default(),
            conversations: ConversationConfig::default(),
            ai: AiConfig::default(),
        }
    }
}

/// Common configuration for indexer features.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FeatureConfig {
    /// Whether this feature is enabled.
    #[serde(default)]
    pub enabled: bool,

    /// Whether to run backfill on startup.
    #[serde(default)]
    pub backfill_on_startup: bool,

    /// Batch size for backfill operations.
    #[serde(default = "default_backfill_batch_size")]
    pub backfill_batch_size: usize,

    /// Whether to allow degraded mode (slow fallback) when index unavailable.
    #[serde(default)]
    pub allow_degraded: bool,
}

impl Default for FeatureConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            backfill_on_startup: false,
            backfill_batch_size: default_backfill_batch_size(),
            allow_degraded: false,
        }
    }
}

fn default_backfill_batch_size() -> usize {
    10_000
}

/// Configuration for engagement metrics indexing.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MetricsConfig {
    /// Whether metrics indexing is enabled.
    #[serde(default)]
    pub enabled: bool,

    /// Whether to run backfill on startup.
    #[serde(default)]
    pub backfill_on_startup: bool,

    /// Batch size for backfill operations.
    #[serde(default = "default_backfill_batch_size")]
    pub backfill_batch_size: usize,

    /// Time window for trending calculations (in hours).
    #[serde(default = "default_trending_window_hours")]
    pub trending_window_hours: u32,

    /// Weights for engagement score calculation.
    #[serde(default)]
    pub score_weights: ScoreWeights,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            backfill_on_startup: false,
            backfill_batch_size: default_backfill_batch_size(),
            trending_window_hours: default_trending_window_hours(),
            score_weights: ScoreWeights::default(),
        }
    }
}

fn default_trending_window_hours() -> u32 {
    24
}

/// Weights for calculating engagement scores.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ScoreWeights {
    pub like: f64,
    pub recast: f64,
    pub reply: f64,
}

impl Default for ScoreWeights {
    fn default() -> Self {
        Self {
            like: 1.0,
            recast: 2.0,
            reply: 3.0,
        }
    }
}

/// Configuration for full-text search.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SearchConfig {
    /// Whether search indexing is enabled.
    #[serde(default)]
    pub enabled: bool,

    /// Whether to run backfill on startup (not recommended for search).
    #[serde(default)]
    pub backfill_on_startup: bool,

    /// Batch size for backfill operations.
    #[serde(default = "default_search_batch_size")]
    pub backfill_batch_size: usize,

    /// Search engine to use.
    #[serde(default)]
    pub engine: SearchEngine,

    /// Path for Tantivy index files.
    #[serde(default = "default_index_path")]
    pub index_path: String,

    /// Memory budget for Tantivy (in MB).
    #[serde(default = "default_memory_budget_mb")]
    pub memory_budget_mb: usize,
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            backfill_on_startup: false,
            backfill_batch_size: default_search_batch_size(),
            engine: SearchEngine::default(),
            index_path: default_index_path(),
            memory_budget_mb: default_memory_budget_mb(),
        }
    }
}

fn default_search_batch_size() -> usize {
    1_000
}

fn default_index_path() -> String {
    "./data/search_index".to_string()
}

fn default_memory_budget_mb() -> usize {
    256
}

/// Search engine backend.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SearchEngine {
    /// Tantivy (embedded, Rust-native).
    #[default]
    Tantivy,
    /// Meilisearch (external service).
    Meilisearch,
}

/// Configuration for feed generation.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FeedConfig {
    /// Whether feed generation is enabled.
    #[serde(default)]
    pub enabled: bool,

    /// Maximum number of users to fetch casts from for following feed.
    #[serde(default = "default_max_following_fetch")]
    pub max_following_fetch: usize,

    /// Default page size for feed responses.
    #[serde(default = "default_feed_page_size")]
    pub default_page_size: usize,
}

impl Default for FeedConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_following_fetch: default_max_following_fetch(),
            default_page_size: default_feed_page_size(),
        }
    }
}

fn default_max_following_fetch() -> usize {
    1000
}

fn default_feed_page_size() -> usize {
    25
}

/// Configuration for conversation/thread features.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ConversationConfig {
    /// Whether conversation features are enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Maximum depth for recursive reply fetching.
    #[serde(default = "default_max_depth")]
    pub max_depth: u32,

    /// Maximum total replies to return.
    #[serde(default = "default_max_replies")]
    pub max_replies: usize,
}

impl Default for ConversationConfig {
    fn default() -> Self {
        Self {
            enabled: true, // No index required, enable by default
            max_depth: default_max_depth(),
            max_replies: default_max_replies(),
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_max_depth() -> u32 {
    100
}

fn default_max_replies() -> usize {
    500
}

/// Configuration for AI-powered features.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AiConfig {
    /// Whether AI features are enabled.
    #[serde(default)]
    pub enabled: bool,

    /// LLM provider to use.
    #[serde(default)]
    pub provider: AiProvider,

    /// Environment variable name for API key.
    #[serde(default = "default_api_key_env")]
    pub api_key_env: String,

    /// Model to use for summaries.
    #[serde(default = "default_model")]
    pub model: String,

    /// Maximum tokens for summary responses.
    #[serde(default = "default_max_tokens")]
    pub max_tokens: u32,
}

impl Default for AiConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: AiProvider::default(),
            api_key_env: default_api_key_env(),
            model: default_model(),
            max_tokens: default_max_tokens(),
        }
    }
}

fn default_api_key_env() -> String {
    "FARCASTER_LLM_API_KEY".to_string()
}

fn default_model() -> String {
    "gpt-4o-mini".to_string()
}

fn default_max_tokens() -> u32 {
    500
}

/// AI/LLM provider.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum AiProvider {
    #[default]
    OpenAi,
    Anthropic,
    Local,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ApiConfig::default();
        assert!(!config.enabled);
        assert!(!config.social_graph.enabled);
        assert!(!config.search.enabled);
        assert!(config.conversations.enabled); // Enabled by default (no index needed)
    }

    #[test]
    fn test_deserialize_minimal() {
        let toml = r#"
            enabled = true
        "#;
        let config: ApiConfig = toml::from_str(toml).unwrap();
        assert!(config.enabled);
        assert!(!config.social_graph.enabled);
    }

    #[test]
    fn test_deserialize_full() {
        let toml = r#"
            enabled = true

            [social_graph]
            enabled = true
            backfill_on_startup = true
            backfill_batch_size = 5000

            [channels]
            enabled = true

            [search]
            enabled = true
            engine = "tantivy"
            index_path = "/custom/path"

            [ai]
            enabled = true
            provider = "anthropic"
        "#;
        let config: ApiConfig = toml::from_str(toml).unwrap();
        assert!(config.enabled);
        assert!(config.social_graph.enabled);
        assert!(config.social_graph.backfill_on_startup);
        assert_eq!(config.social_graph.backfill_batch_size, 5000);
        assert!(config.channels.enabled);
        assert!(config.search.enabled);
        assert_eq!(config.search.engine, SearchEngine::Tantivy);
        assert_eq!(config.search.index_path, "/custom/path");
        assert!(config.ai.enabled);
        assert_eq!(config.ai.provider, AiProvider::Anthropic);
    }
}
