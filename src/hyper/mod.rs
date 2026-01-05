use crate::proto;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Execution context for state mutations.
///
/// `Legacy` follows the current pruning rules while `Hyper` keeps
/// every message/transaction unbounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StateContext {
    Legacy,
    Hyper,
}

impl StateContext {
    /// Prefix that can be used when namespacing storage keys.
    pub const fn namespace_prefix(self) -> &'static [u8] {
        match self {
            StateContext::Legacy => b"legacy",
            StateContext::Hyper => b"hyper",
        }
    }

    /// Whether this context allows the store to prune data.
    pub const fn allows_pruning(self) -> bool {
        matches!(self, StateContext::Legacy)
    }

    pub const fn is_hyper(self) -> bool {
        matches!(self, StateContext::Hyper)
    }
}

/// Capability advertised during peer handshakes to signal that
/// additional hyper envelopes may follow legacy block messages.
pub const CAPABILITY_HYPER: &str = "hyper:v1";

/// Configuration toggles for the hyper pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HyperConfig {
    pub enabled: bool,
    /// Optional retention hint for operators who want alerts when
    /// storage grows past a defined message count.
    pub retention_soft_cap: Option<u64>,
    /// Interval for emitting hyper metrics/diff comparisons.
    #[serde(with = "humantime_serde")]
    pub metrics_interval: Duration,
}

impl Default for HyperConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            retention_soft_cap: None,
            metrics_interval: Duration::from_secs(60),
        }
    }
}

impl HyperConfig {
    pub fn can_start_pipeline(&self) -> bool {
        self.enabled
    }

    pub fn retention_soft_cap(&self) -> Option<u64> {
        self.retention_soft_cap
    }
}

/// Metadata describing the hyper block that mirrors a canonical block.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HyperBlockMetadata {
    pub canonical_block_id: u64,
    pub parent_hash: Vec<u8>,
    pub hyper_state_root: Vec<u8>,
    pub extra_rules_version: u32,
    pub retained_message_count: u64,
}

/// Envelope that is only shared with peers that advertise
/// [`CAPABILITY_HYPER`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HyperEnvelope {
    pub metadata: HyperBlockMetadata,
    /// Hyper-only payload that may include new message types or
    /// rule-specific annotations.
    pub payload: Vec<u8>,
}

/// Summary emitted by diff tooling when comparing legacy and hyper
/// stores for a particular block.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HyperDiffReport {
    pub block_id: u64,
    pub legacy_state_root: Option<Vec<u8>>,
    pub hyper_state_root: Vec<u8>,
    pub retained_message_delta: i64,
    pub notes: Vec<String>,
}

impl HyperDiffReport {
    pub fn diverged(&self) -> bool {
        self.legacy_state_root
            .as_ref()
            .map(|legacy| legacy != &self.hyper_state_root)
            .unwrap_or(false)
            || self.retained_message_delta != 0
    }
}

impl From<HyperBlockMetadata> for proto::HyperBlockMetadata {
    fn from(value: HyperBlockMetadata) -> Self {
        proto::HyperBlockMetadata {
            canonical_block_id: value.canonical_block_id,
            parent_hash: value.parent_hash,
            hyper_state_root: value.hyper_state_root,
            extra_rules_version: value.extra_rules_version,
            retained_message_count: value.retained_message_count,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_context_helpers() {
        assert_eq!(StateContext::Legacy.namespace_prefix(), b"legacy");
        assert!(StateContext::Legacy.allows_pruning());
        assert!(!StateContext::Legacy.is_hyper());

        assert_eq!(StateContext::Hyper.namespace_prefix(), b"hyper");
        assert!(!StateContext::Hyper.allows_pruning());
        assert!(StateContext::Hyper.is_hyper());
    }

    #[test]
    fn hyper_config_defaults_and_accessors() {
        let cfg = HyperConfig::default();
        assert!(!cfg.can_start_pipeline());
        assert_eq!(cfg.retention_soft_cap(), None);

        let cfg = HyperConfig {
            enabled: true,
            retention_soft_cap: Some(42),
            metrics_interval: Duration::from_secs(10),
        };
        assert!(cfg.can_start_pipeline());
        assert_eq!(cfg.retention_soft_cap(), Some(42));
        assert_eq!(cfg.metrics_interval, Duration::from_secs(10));
    }

    #[test]
    fn hyper_envelope_round_trips_to_proto() {
        let metadata = HyperBlockMetadata {
            canonical_block_id: 99,
            parent_hash: vec![0xaa, 0xbb],
            hyper_state_root: vec![0x01, 0x02],
            extra_rules_version: 3,
            retained_message_count: 7,
        };
        let envelope = HyperEnvelope {
            metadata: metadata.clone(),
            payload: vec![0x10, 0x20, 0x30],
        };

        let proto_envelope: proto::HyperEnvelope = envelope.clone().into();
        assert_eq!(proto_envelope.payload, envelope.payload);
        assert_eq!(
            proto_envelope.metadata.as_ref().unwrap().canonical_block_id,
            metadata.canonical_block_id
        );
        assert_eq!(
            proto_envelope
                .metadata
                .as_ref()
                .unwrap()
                .retained_message_count,
            metadata.retained_message_count
        );
    }
}

impl From<HyperEnvelope> for proto::HyperEnvelope {
    fn from(value: HyperEnvelope) -> Self {
        proto::HyperEnvelope {
            metadata: Some(value.metadata.into()),
            payload: value.payload,
        }
    }
}

pub fn build_envelope_for_block(block: &proto::Block, hyper_state_root: Vec<u8>) -> HyperEnvelope {
    let canonical_block_id = block
        .header
        .as_ref()
        .and_then(|header| header.height.clone())
        .map(|height| height.block_number)
        .unwrap_or_default();
    let parent_hash = block
        .header
        .as_ref()
        .map(|header| header.parent_hash.clone())
        .unwrap_or_default();

    HyperEnvelope {
        metadata: HyperBlockMetadata {
            canonical_block_id,
            parent_hash,
            hyper_state_root,
            extra_rules_version: 0,
            retained_message_count: block.transactions.len() as u64,
        },
        payload: Vec::new(),
    }
}
