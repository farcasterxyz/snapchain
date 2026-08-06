//! Per-peer, per-topic gossip metrics, modeled on the Prometheus client data
//! model: cumulative labeled counters are the store of record, and rates are
//! *derived* from two counter snapshots at sample time rather than stored.
//!
//! The counters register into the same in-process `SharedRegistry` Malachite
//! uses (`prometheus_client`), so a future `/metrics` text endpoint exposes
//! them alongside consensus metrics. A lightweight sampler additionally derives
//! human-readable rates for the on-demand mesh view.
//!
//! Cardinality is bounded to *currently connected* peers: a peer's series and
//! sampler state are evicted on disconnect (`remove_peer`).

use informalsystems_malachitebft_metrics::SharedRegistry;
use libp2p::PeerId;
use parking_lot::Mutex;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

/// Label set for the per-peer, per-topic gossip counters.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct GossipLabels {
    pub peer: String,
    pub topic: String,
}

impl GossipLabels {
    fn new(peer: &PeerId, topic: &str) -> Self {
        Self {
            peer: peer.to_string(),
            topic: topic.to_string(),
        }
    }
}

/// Derived per-(peer, topic) gossip rate plus the cumulative totals it was
/// derived from. This is what the mesh view surfaces.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct GossipRate {
    pub msgs_per_sec: f64,
    pub bytes_per_sec: f64,
    pub total_msgs: u64,
    pub total_bytes: u64,
}

#[derive(Clone, Copy, Debug)]
struct Snapshot {
    msgs: u64,
    bytes: u64,
    at: Instant,
}

/// Cumulative gossip counters (the store of record) plus a sampler that derives
/// rates for the on-demand mesh view.
///
/// Cheap to clone: the `Family` values are `Arc`-backed and share storage, and
/// the sampler state sits behind a single `Arc<Mutex<_>>`. A clone is a handle
/// to the same underlying metrics — used so `main` can register the counters
/// after the gossip task has been spawned.
#[derive(Clone, Debug, Default)]
pub struct GossipMetrics {
    messages_total: Family<GossipLabels, Counter>,
    bytes_total: Family<GossipLabels, Counter>,
    /// Last counter snapshot per (peer, topic); the basis for rate derivation.
    /// Bounded to connected peers (evicted on disconnect).
    samples: Arc<Mutex<HashMap<GossipLabels, Snapshot>>>,
    /// Most recently derived rate per (peer, topic), read by the mesh view.
    rates: Arc<Mutex<HashMap<GossipLabels, GossipRate>>>,
}

impl GossipMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register the cumulative counters into the shared Prometheus registry.
    /// Call once (production); the counters function whether or not they are
    /// registered — registration only affects text-format export.
    pub fn register(&self, registry: &SharedRegistry) {
        registry.with_prefix("snapchain_gossip", |reg| {
            reg.register(
                "messages_received",
                "Gossip messages received, per peer and topic",
                self.messages_total.clone(),
            );
            reg.register(
                "bytes_received",
                "Gossip bytes received, per peer and topic",
                self.bytes_total.clone(),
            );
        });
    }

    /// Record one received gossip message from `peer` on `topic`.
    pub fn record_message(&self, peer: &PeerId, topic: &str, bytes: u64) {
        let labels = GossipLabels::new(peer, topic);
        self.messages_total.get_or_create(&labels).inc();
        self.bytes_total.get_or_create(&labels).inc_by(bytes);
    }

    /// Recompute rates for `peers` across `topics` from the cumulative
    /// counters, deriving `(cur - prev) / dt` and refreshing the stored sample.
    /// Intended to be called on the periodic gossip tick.
    pub fn refresh_rates<'a>(&self, peers: impl IntoIterator<Item = &'a PeerId>, topics: &[&str]) {
        let now = Instant::now();
        let mut samples = self.samples.lock();
        let mut rates = self.rates.lock();
        for peer in peers {
            for topic in topics {
                let labels = GossipLabels::new(peer, topic);
                let cur_msgs = self.messages_total.get_or_create(&labels).get();
                let cur_bytes = self.bytes_total.get_or_create(&labels).get();
                let mut rate = GossipRate {
                    total_msgs: cur_msgs,
                    total_bytes: cur_bytes,
                    ..Default::default()
                };
                if let Some(prev) = samples.get(&labels) {
                    let dt = now.duration_since(prev.at).as_secs_f64();
                    if dt > 0.0 {
                        rate.msgs_per_sec = cur_msgs.saturating_sub(prev.msgs) as f64 / dt;
                        rate.bytes_per_sec = cur_bytes.saturating_sub(prev.bytes) as f64 / dt;
                    }
                }
                samples.insert(
                    labels.clone(),
                    Snapshot {
                        msgs: cur_msgs,
                        bytes: cur_bytes,
                        at: now,
                    },
                );
                rates.insert(labels, rate);
            }
        }
    }

    /// Current derived rates for `peer` across `topics`, for the mesh view.
    /// Only returns entries that have a derived sample.
    pub fn rates_for(&self, peer: &PeerId, topics: &[&str]) -> Vec<(String, GossipRate)> {
        let rates = self.rates.lock();
        topics
            .iter()
            .filter_map(|topic| {
                let labels = GossipLabels::new(peer, topic);
                rates.get(&labels).map(|r| (topic.to_string(), *r))
            })
            .collect()
    }

    /// Evict all series and sampler state for a disconnected peer, bounding
    /// cardinality to currently connected peers.
    pub fn remove_peer(&self, peer: &PeerId, topics: &[&str]) {
        let mut samples = self.samples.lock();
        let mut rates = self.rates.lock();
        for topic in topics {
            let labels = GossipLabels::new(peer, topic);
            self.messages_total.remove(&labels);
            self.bytes_total.remove(&labels);
            samples.remove(&labels);
            rates.remove(&labels);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer() -> PeerId {
        PeerId::random()
    }

    #[test]
    fn records_cumulative_totals() {
        let m = GossipMetrics::new();
        let p = peer();
        m.record_message(&p, "consensus", 100);
        m.record_message(&p, "consensus", 50);
        m.refresh_rates([&p], &["consensus"]);
        let rates = m.rates_for(&p, &["consensus"]);
        assert_eq!(rates.len(), 1);
        let (topic, rate) = &rates[0];
        assert_eq!(topic, "consensus");
        assert_eq!(rate.total_msgs, 2);
        assert_eq!(rate.total_bytes, 150);
    }

    #[test]
    fn derives_rate_between_samples() {
        let m = GossipMetrics::new();
        let p = peer();
        // First sample establishes a baseline (no prior → rate 0).
        m.refresh_rates([&p], &["mempool"]);
        m.record_message(&p, "mempool", 10);
        m.record_message(&p, "mempool", 10);
        // Second sample sees a positive delta over a positive dt.
        std::thread::sleep(std::time::Duration::from_millis(20));
        m.refresh_rates([&p], &["mempool"]);
        let rate = m.rates_for(&p, &["mempool"])[0].1;
        assert!(rate.msgs_per_sec > 0.0, "expected positive msgs/s");
        assert!(rate.bytes_per_sec > 0.0, "expected positive bytes/s");
        assert_eq!(rate.total_msgs, 2);
    }

    #[test]
    fn remove_peer_evicts_series() {
        let m = GossipMetrics::new();
        let p = peer();
        m.record_message(&p, "consensus", 100);
        m.refresh_rates([&p], &["consensus"]);
        assert_eq!(m.rates_for(&p, &["consensus"]).len(), 1);
        m.remove_peer(&p, &["consensus"]);
        assert_eq!(m.rates_for(&p, &["consensus"]).len(), 0);
    }
}
