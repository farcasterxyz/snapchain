//! Operator-facing configuration for the mesh diagnostics endpoints.

use crate::network::mesh::nodes::KnownNode;
use serde::{Deserialize, Serialize};

/// `[mesh]` config section.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    /// TTL, in seconds, for cached mesh view / topology responses. `0` disables
    /// caching entirely (every request recomputes). Default 5.
    pub cache_ttl_secs: u64,

    /// Extends / overrides the compiled-in known-node table used to attach
    /// human-readable names to nodes in the JSON output. TOML-only: figment
    /// cannot build a struct array from a flat env var, so
    /// `SNAPCHAIN_MESH__NODES` is not supported — use `[[mesh.nodes]]` tables.
    /// Each entry is matched to a builtin by `consensus_public_key` (preferred)
    /// or `peer_id`; a match replaces the whole builtin entry, otherwise the
    /// entry is appended.
    #[serde(default)]
    pub nodes: Vec<KnownNode>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            cache_ttl_secs: 5,
            nodes: Vec::new(),
        }
    }
}
