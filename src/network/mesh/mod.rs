//! Mesh health & topology: per-peer gossip metrics, a local mesh view, and
//! (later) a recursive crawl of the validator network over the gossip port.
//!
//! Milestone 0 lives here: `metrics` holds Prometheus-client cumulative
//! counters for per-peer/per-topic gossip volume, from which rates are derived.

pub mod metrics;
