//! ASCII rendering of a [`proto::MeshView`]: a peer table plus a simple
//! adjacency view of the local node's consensus mesh. Pure and unit-testable.

use crate::proto;
use libp2p::PeerId;
use std::fmt::Write;

const CONSENSUS_TOPIC: &str = "consensus";

/// Render a mesh view as an ASCII table + consensus-mesh graph.
pub fn render_mesh_view(view: &proto::MeshView) -> String {
    let mut out = String::new();
    let local = view.local.as_ref();

    let self_id = local
        .map(|l| short_peer(&l.peer_id))
        .unwrap_or_else(|| "?".to_string());
    let role = match local {
        Some(l) if l.is_validator => "validator",
        Some(_) => "non-validator",
        None => "?",
    };
    let height = local.map(|l| l.current_height).unwrap_or(0);
    let net = local.map(|l| network_name(l.network)).unwrap_or("?");
    let mesh_size = local.map(|l| l.consensus_mesh_size).unwrap_or(0);
    let validators = view
        .peers
        .iter()
        .filter(|p| p.node_type == proto::MeshNodeType::Validator as i32)
        .count();

    let _ = writeln!(
        out,
        "MESH VIEW  self={self_id}  {role}  height {height}  net={net}  consensus-mesh {mesh_size}"
    );
    let _ = writeln!(
        out,
        "PEERS ({} shown, {} validators)",
        view.peers.len(),
        validators
    );
    let _ = writeln!(
        out,
        "{:<12} {:<13} {:<4} {:<7} {:<10} {}",
        "PEER", "TYPE", "DIR", "C-MESH", "CONTACT", "msgs/s(consensus)"
    );
    for p in &view.peers {
        let rate = consensus_rate(p)
            .map(|r| format!("{:.1}", r))
            .unwrap_or_else(|| "—".to_string());
        let _ = writeln!(
            out,
            "{:<12} {:<13} {:<4} {:<7} {:<10} {}",
            short_peer(&p.peer_id),
            node_type_label(p.node_type),
            if p.direct_peer { "yes" } else { "no" },
            if in_consensus_mesh(p) { "yes" } else { "NO" },
            contact_source_label(p.contact_source),
            rate,
        );
    }

    let _ = writeln!(out, "GRAPH (consensus mesh)");
    let mut any_validator = false;
    for p in &view.peers {
        if p.node_type != proto::MeshNodeType::Validator as i32 {
            continue;
        }
        any_validator = true;
        let peer = short_peer(&p.peer_id);
        if in_consensus_mesh(p) {
            let _ = writeln!(out, "  {self_id} ── {peer}");
        } else {
            let _ = writeln!(
                out,
                "  {self_id} ╳  {peer}   (connected, not meshed) ← consensus-partition risk"
            );
        }
    }
    if !any_validator {
        let _ = writeln!(out, "  (no validator peers)");
    }

    out
}

/// Clean JSON representation of a mesh view: peer ids as base58 strings,
/// public keys as hex, enums as labels (instead of proto's raw bytes/ints).
pub fn mesh_view_json(view: &proto::MeshView) -> serde_json::Value {
    use serde_json::json;
    let local = view.local.as_ref().map(|l| {
        json!({
            "peer_id": peer_str(&l.peer_id),
            "consensus_public_key": hex_or_null(&l.consensus_public_key),
            "is_validator": l.is_validator,
            "gossip_address": l.gossip_address,
            "rpc_address": l.rpc_address,
            "snapchain_version": l.snapchain_version,
            "network": network_name(l.network),
            "subscribed_topics": l.subscribed_topics,
            "consensus_mesh_size": l.consensus_mesh_size,
            "current_height": l.current_height,
        })
    });
    let peers: Vec<_> = view
        .peers
        .iter()
        .map(|p| {
            json!({
                "peer_id": peer_str(&p.peer_id),
                "node_type": node_type_label(p.node_type),
                "consensus_public_key": p.consensus_public_key.as_ref().map(hex::encode),
                "connected": p.connected,
                "direct_peer": p.direct_peer,
                "contact_source": contact_source_label(p.contact_source),
                "observed_address": p.observed_address,
                "contact_info": p.contact_info.as_ref().map(|c| json!({
                    "gossip_address": c.gossip_address,
                    "announce_rpc_address": c.announce_rpc_address,
                    "snapchain_version": c.snapchain_version,
                    "network": network_name(c.network),
                    "timestamp": c.timestamp,
                })),
                "topics": p.topics.iter().map(|t| json!({
                    "topic": t.topic, "subscribed": t.subscribed, "in_mesh": t.in_mesh
                })).collect::<Vec<_>>(),
                "gossip_rates": p.gossip_rates.iter().map(|r| json!({
                    "topic": r.topic,
                    "msgs_per_sec": r.msgs_per_sec,
                    "bytes_per_sec": r.bytes_per_sec,
                    "total_msgs": r.total_msgs,
                    "total_bytes": r.total_bytes,
                })).collect::<Vec<_>>(),
            })
        })
        .collect();
    json!({ "self": local, "peers": peers, "generated_at": view.generated_at })
}

fn peer_str(peer_id: &[u8]) -> String {
    PeerId::from_bytes(peer_id)
        .map(|p| p.to_string())
        .unwrap_or_else(|_| hex::encode(peer_id))
}

fn hex_or_null(bytes: &[u8]) -> Option<String> {
    if bytes.is_empty() {
        None
    } else {
        Some(hex::encode(bytes))
    }
}

fn short_peer(peer_id: &[u8]) -> String {
    let full = PeerId::from_bytes(peer_id)
        .map(|p| p.to_base58())
        .unwrap_or_else(|_| hex::encode(peer_id));
    let tail: String = full
        .chars()
        .rev()
        .take(7)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("…{tail}")
}

fn node_type_label(node_type: i32) -> &'static str {
    if node_type == proto::MeshNodeType::Validator as i32 {
        "validator"
    } else if node_type == proto::MeshNodeType::NonValidator as i32 {
        "non-val"
    } else {
        "unknown"
    }
}

fn contact_source_label(source: i32) -> &'static str {
    if source == proto::ContactSource::Collected as i32 {
        "collected"
    } else if source == proto::ContactSource::Derived as i32 {
        "derived"
    } else {
        "unknown"
    }
}

fn network_name(network: i32) -> &'static str {
    match proto::FarcasterNetwork::try_from(network) {
        Ok(proto::FarcasterNetwork::Mainnet) => "MAINNET",
        Ok(proto::FarcasterNetwork::Testnet) => "TESTNET",
        Ok(proto::FarcasterNetwork::Devnet) => "DEVNET",
        _ => "?",
    }
}

fn in_consensus_mesh(peer: &proto::MeshPeer) -> bool {
    peer.topics
        .iter()
        .any(|t| t.topic == CONSENSUS_TOPIC && t.in_mesh)
}

fn consensus_rate(peer: &proto::MeshPeer) -> Option<f64> {
    peer.gossip_rates
        .iter()
        .find(|r| r.topic == CONSENSUS_TOPIC)
        .map(|r| r.msgs_per_sec)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_table_and_partition_flag() {
        let a = PeerId::random();
        let b = PeerId::random();
        let c = PeerId::random();
        let view = proto::MeshView {
            local: Some(proto::MeshSelf {
                peer_id: a.to_bytes(),
                is_validator: true,
                current_height: 1234567,
                network: proto::FarcasterNetwork::Mainnet as i32,
                consensus_mesh_size: 1,
                ..Default::default()
            }),
            peers: vec![
                // Meshed validator with a rate.
                proto::MeshPeer {
                    peer_id: b.to_bytes(),
                    node_type: proto::MeshNodeType::Validator as i32,
                    direct_peer: true,
                    contact_source: proto::ContactSource::Collected as i32,
                    topics: vec![proto::TopicMembership {
                        topic: CONSENSUS_TOPIC.to_string(),
                        subscribed: true,
                        in_mesh: true,
                    }],
                    gossip_rates: vec![proto::GossipRate {
                        topic: CONSENSUS_TOPIC.to_string(),
                        msgs_per_sec: 12.4,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                // Connected validator NOT meshed, no collected contact info.
                proto::MeshPeer {
                    peer_id: c.to_bytes(),
                    node_type: proto::MeshNodeType::Validator as i32,
                    contact_source: proto::ContactSource::Derived as i32,
                    topics: vec![proto::TopicMembership {
                        topic: CONSENSUS_TOPIC.to_string(),
                        subscribed: true,
                        in_mesh: false,
                    }],
                    ..Default::default()
                },
            ],
            generated_at: 0,
        };

        let out = render_mesh_view(&view);
        assert!(out.contains("validator"));
        assert!(out.contains("height 1234567"));
        assert!(out.contains("MAINNET"));
        assert!(out.contains("collected"));
        assert!(out.contains("derived"));
        assert!(out.contains("12.4"));
        // The unmeshed validator must surface the partition risk.
        assert!(out.contains("consensus-partition risk"));
        // Both validators counted.
        assert!(out.contains("2 validators"));
    }
}
