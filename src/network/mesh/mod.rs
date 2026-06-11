//! Mesh health & topology: per-peer gossip metrics, a local mesh view, and a
//! recursive crawl of the validator network over the gossip port.
//!
//! `metrics` holds Prometheus-client cumulative counters for per-peer/per-topic
//! gossip volume, from which rates are derived.

pub mod crawl;
pub mod diagnostics;
pub mod metrics;
pub mod render;
pub mod view;
