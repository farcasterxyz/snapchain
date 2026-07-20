//! Mesh health & topology: per-peer gossip metrics, a local mesh view, and a
//! recursive crawl of the validator network over the gossip port.
//!
//! `metrics` holds Prometheus-client cumulative counters for per-peer/per-topic
//! gossip volume, from which rates are derived.

pub mod cache;
pub mod config;
pub mod crawl;
pub mod diagnostics;
pub mod metrics;
pub mod nodes;
pub mod render;
pub mod view;
