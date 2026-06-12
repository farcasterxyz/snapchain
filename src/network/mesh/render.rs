//! ASCII rendering of a [`proto::MeshView`]: a peer table with per-topic
//! gossip-mesh membership plus a consensus-mesh graph. Pure and unit-testable.
//!
//! The set of topics surfaced in the ASCII tables/matrices is caller-controlled
//! (the `?topics=` URL param); the data layer always carries every topic, so
//! this only narrows the *display*. See [`DEFAULT_TOPICS`].

use crate::network::gossip::{ALL_TOPICS, CONSENSUS_TOPIC, MEMPOOL_TOPIC};
use crate::proto;
use libp2p::PeerId;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write;

/// Topics shown in the ASCII view when `?topics=` is omitted. Derived from the
/// canonical topic constants — consensus is the consensus-partition signal,
/// mempool the next most operationally important (transaction propagation).
pub const DEFAULT_TOPICS: [&str; 2] = [CONSENSUS_TOPIC, MEMPOOL_TOPIC];

/// Parse a comma-separated `?topics=` value into a topic list. The sentinel
/// `all` selects every known topic. Otherwise entries are trimmed and validated
/// against the canonical [`ALL_TOPICS`]; unknown names are dropped. Falls back
/// to [`DEFAULT_TOPICS`] when nothing valid remains.
pub fn parse_topics(param: Option<&str>) -> Vec<String> {
    let raw = param.unwrap_or("");
    if raw.split(',').any(|t| t.trim().eq_ignore_ascii_case("all")) {
        return ALL_TOPICS.iter().map(|t| t.to_string()).collect();
    }
    let topics: Vec<String> = raw
        .split(',')
        .map(|t| t.trim())
        .filter(|t| ALL_TOPICS.contains(t))
        .map(|t| t.to_string())
        .collect();
    if topics.is_empty() {
        DEFAULT_TOPICS.iter().map(|t| t.to_string()).collect()
    } else {
        topics
    }
}

/// Render a mesh view as an ASCII table (per-topic gossip-mesh membership) plus
/// a consensus-mesh graph. `topics` controls which topics appear as columns.
pub fn render_mesh_view(view: &proto::MeshView, topics: &[String]) -> String {
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
    let validators = view
        .peers
        .iter()
        .filter(|p| p.node_type == proto::MeshNodeType::Validator as i32)
        .count();
    // Effective consensus connectivity counts emergent mesh AND explicit/direct
    // peers; direct peers are excluded from gossipsub's `mesh_peers()` by design.
    let consensus_linked = view
        .peers
        .iter()
        .filter(|p| p.node_type == proto::MeshNodeType::Validator as i32 && has_consensus_link(p))
        .count();

    let _ = writeln!(
        out,
        "MESH VIEW  self={self_id}  {role}  height {height}  net={net}  consensus-mesh {consensus_linked}/{validators}"
    );
    let _ = writeln!(
        out,
        "PEERS ({} shown, {} validators)   MESH per topic: ● in-mesh  ◆ direct/explicit  ○ sub-only  · none",
        view.peers.len(),
        validators
    );

    // Header: fixed columns + one column per topic + contact + rate.
    let rate_topic = topics
        .first()
        .cloned()
        .unwrap_or_else(|| CONSENSUS_TOPIC.to_string());
    let mut header = format!("{:<12} {:<13} {:<4} ", "PEER", "TYPE", "DIR");
    for t in topics {
        header.push_str(&format!("{:<11}", t));
    }
    header.push_str(&format!("{:<10} msgs/s({})", "CONTACT", rate_topic));
    let _ = writeln!(out, "{header}");

    for p in &view.peers {
        let mut row = format!(
            "{:<12} {:<13} {:<4} ",
            short_peer(&p.peer_id),
            node_type_label(p.node_type),
            if p.direct_peer { "yes" } else { "no" },
        );
        for t in topics {
            row.push_str(&format!("{:<11}", mesh_cell(link_state(p, t))));
        }
        let rate = rate_for_topic(p, &rate_topic)
            .map(|r| format!("{:.1}", r))
            .unwrap_or_else(|| "—".to_string());
        row.push_str(&format!(
            "{:<10} {}",
            contact_source_label(p.contact_source),
            rate
        ));
        let _ = writeln!(out, "{row}");
    }

    // Consensus partition graph — only when consensus is among the shown topics
    // (it is the partition-critical signal).
    if topics.iter().any(|t| t == CONSENSUS_TOPIC) {
        let _ = writeln!(out, "GRAPH (consensus mesh)");
        let mut any_validator = false;
        for p in &view.peers {
            if p.node_type != proto::MeshNodeType::Validator as i32 {
                continue;
            }
            any_validator = true;
            let peer = short_peer(&p.peer_id);
            match link_state(p, CONSENSUS_TOPIC) {
                LinkState::Mesh => {
                    let _ = writeln!(out, "  {self_id} ── {peer}");
                }
                LinkState::Direct => {
                    let _ = writeln!(out, "  {self_id} ─◆ {peer}   (direct/explicit peer)");
                }
                LinkState::SubOnly | LinkState::None => {
                    let _ = writeln!(
                        out,
                        "  {self_id} ╳  {peer}   (connected, not meshed) ← consensus-partition risk"
                    );
                }
            }
        }
        if !any_validator {
            let _ = writeln!(out, "  (no validator peers)");
        }
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

/// Effective per-topic link state between us and a peer.
///
/// libp2p gossipsub forwards every published message directly to **explicit
/// peers** (`add_explicit_peer`, used for snapchain `direct_peers`) but
/// deliberately keeps them OUT of `mesh_peers()`. So a perfectly healthy direct
/// peer reports `in_mesh = false`. We treat a subscribed direct peer as a
/// first-class link (`Direct`) so it is never mistaken for a partition or for a
/// merely-subscribed peer.
#[derive(Clone, Copy, PartialEq, Eq)]
enum LinkState {
    Mesh,    // in our emergent gossipsub mesh
    Direct,  // explicit/direct peer, subscribed (excluded from the mesh by design)
    SubOnly, // subscribed but neither meshed nor an explicit peer
    None,    // not subscribed and not meshed
}

fn link_state(peer: &proto::MeshPeer, topic: &str) -> LinkState {
    let (subscribed, in_mesh) = topic_state(peer, topic);
    if in_mesh {
        LinkState::Mesh
    } else if peer.direct_peer && subscribed {
        LinkState::Direct
    } else if subscribed {
        LinkState::SubOnly
    } else {
        LinkState::None
    }
}

/// An effective gossip link — either an emergent mesh edge or an explicit/direct
/// peering. The two are equivalent for message delivery; only `Direct` is hidden
/// from `mesh_peers()`.
fn has_link(state: LinkState) -> bool {
    matches!(state, LinkState::Mesh | LinkState::Direct)
}

/// Does the peer have an effective consensus link (meshed or explicit/direct)?
fn has_consensus_link(peer: &proto::MeshPeer) -> bool {
    has_link(link_state(peer, CONSENSUS_TOPIC))
}

/// `(subscribed, in_mesh)` for a peer on a given topic. Absent membership entry
/// means the peer neither subscribes to nor meshes on the topic.
fn topic_state(peer: &proto::MeshPeer, topic: &str) -> (bool, bool) {
    peer.topics
        .iter()
        .find(|t| t.topic == topic)
        .map(|t| (t.subscribed, t.in_mesh))
        .unwrap_or((false, false))
}

/// Single-cell gossip indicator: meshed / explicit-direct / subscribed-only / none.
fn mesh_cell(state: LinkState) -> &'static str {
    match state {
        LinkState::Mesh => "●",
        LinkState::Direct => "◆",
        LinkState::SubOnly => "○",
        LinkState::None => "·",
    }
}

fn rate_for_topic(peer: &proto::MeshPeer, topic: &str) -> Option<f64> {
    peer.gossip_rates
        .iter()
        .find(|r| r.topic == topic)
        .map(|r| r.msgs_per_sec)
}

/// Render an aggregated [`proto::MeshTopology`] (the crawl result) as ASCII:
/// one N×N adjacency matrix per `topic`, a per-node summary table with per-topic
/// mesh sizes, an unreachable list, and — when non-validator peers are present
/// (`validators_only=false`) — a reader-spoke section.
pub fn render_topology(topo: &proto::MeshTopology, topics: &[String]) -> String {
    let mut out = String::new();

    // Nodes that reported a `local` block, in stable order.
    let entries: Vec<(&proto::MeshView, &proto::MeshSelf)> = topo
        .nodes
        .iter()
        .filter_map(|n| n.local.as_ref().map(|l| (n, l)))
        .collect();
    let net = entries
        .first()
        .map(|(_, l)| network_name(l.network))
        .unwrap_or("?");
    let ids: Vec<&Vec<u8>> = entries.iter().map(|(_, l)| &l.peer_id).collect();

    let _ = writeln!(
        out,
        "MESH TOPOLOGY  nodes={}  unreachable={}  net={}",
        entries.len(),
        topo.unreachable.len(),
        net
    );

    // Per-topic, per-node maps of peer id -> effective link state. A node's link
    // to a peer is either an emergent mesh edge or an explicit/direct peering;
    // `link_maps[topic_idx][node_idx]`.
    let link_maps: Vec<Vec<HashMap<Vec<u8>, LinkState>>> = topics
        .iter()
        .map(|topic| {
            entries
                .iter()
                .map(|(n, _)| {
                    n.peers
                        .iter()
                        .map(|p| (p.peer_id.clone(), link_state(p, topic)))
                        .collect()
                })
                .collect()
        })
        .collect();

    // Effective link from node `i` to peer id `id`: meshed or explicit/direct.
    let linked = |t: usize, i: usize, id: &Vec<u8>| -> Option<LinkState> {
        link_maps[t][i].get(id).copied().filter(|s| has_link(*s))
    };

    // One adjacency matrix per topic.
    for (t, topic) in topics.iter().enumerate() {
        let _ = writeln!(
            out,
            "{} LINKS (row->col:  ● mesh  ◆ direct/explicit  > row->col only  < col->row only  · none)",
            topic.to_uppercase()
        );
        let mut header = format!("{:<16}", "");
        for id in &ids {
            header.push_str(&format!("{:<6}", col_tag(id)));
        }
        let _ = writeln!(out, "{}", header);
        for (i, (_, li)) in entries.iter().enumerate() {
            let role = if li.is_validator { "val" } else { "non" };
            let label = format!("{} ({})", short_peer(&li.peer_id), role);
            let mut row = format!("{:<16}", label);
            for (j, id_j) in ids.iter().enumerate() {
                let cell = if i == j {
                    "—"
                } else {
                    let a = linked(t, i, id_j); // row i -> col j
                    let b = linked(t, j, ids[i]); // col j -> row i
                    match (a, b) {
                        // Both linked: ● only when both are emergent mesh edges,
                        // ◆ when either side is an explicit/direct peering.
                        (Some(LinkState::Mesh), Some(LinkState::Mesh)) => "●",
                        (Some(_), Some(_)) => "◆",
                        (Some(_), None) => ">",
                        (None, Some(_)) => "<",
                        (None, None) => "·",
                    }
                };
                row.push_str(&format!("{:<6}", cell));
            }
            let _ = writeln!(out, "{}", row);
        }
    }

    // Per-node summary: effective link count per topic (mesh + direct).
    let _ = writeln!(out, "NODES (links per topic: mesh + direct)");
    let mut nhdr = format!("{:<12} {:<11} ", "PEER", "ROLE");
    for topic in topics {
        nhdr.push_str(&format!("{:<6}", topic_tag(topic)));
    }
    nhdr.push_str("VERSION");
    let _ = writeln!(out, "{nhdr}");
    for (i, (_, l)) in entries.iter().enumerate() {
        let mut row = format!(
            "{:<12} {:<11} ",
            short_peer(&l.peer_id),
            if l.is_validator {
                "validator"
            } else {
                "non-val"
            },
        );
        for t in 0..topics.len() {
            let n_links = link_maps[t][i].values().filter(|s| has_link(**s)).count();
            row.push_str(&format!("{:<6}", n_links));
        }
        row.push_str(&l.snapchain_version);
        let _ = writeln!(out, "{row}");
    }

    // Unreachable validators.
    if topo.unreachable.is_empty() {
        let _ = writeln!(out, "unreachable: (none)");
    } else {
        let _ = writeln!(out, "UNREACHABLE ({})", topo.unreachable.len());
        for u in &topo.unreachable {
            let _ = writeln!(out, "  {:<12} {}", short_peer(&u.peer_id), u.reason);
        }
    }

    // Reader spokes — present only when non-validator peers were kept
    // (validators_only=false). Dedup readers by peer_id; list reporting validators.
    let mut readers: BTreeMap<Vec<u8>, Vec<Vec<u8>>> = BTreeMap::new();
    for (n, l) in &entries {
        for p in &n.peers {
            if p.node_type == proto::MeshNodeType::NonValidator as i32 {
                readers
                    .entry(p.peer_id.clone())
                    .or_default()
                    .push(l.peer_id.clone());
            }
        }
    }
    if !readers.is_empty() {
        let _ = writeln!(out, "READER SPOKES ({})", readers.len());
        for (reader, reporters) in &readers {
            let reps: Vec<String> = reporters.iter().map(|r| short_peer(r)).collect();
            let _ = writeln!(out, "  {}  ← {}", short_peer(reader), reps.join(", "));
        }
    }

    out
}

/// Clean JSON for an aggregated topology: each node via [`mesh_view_json`], plus
/// the unreachable list and generation time.
pub fn topology_json(topo: &proto::MeshTopology) -> serde_json::Value {
    use serde_json::json;
    json!({
        "nodes": topo.nodes.iter().map(mesh_view_json).collect::<Vec<_>>(),
        "unreachable": topo.unreachable.iter().map(|u| json!({
            "peer_id": peer_str(&u.peer_id),
            "consensus_public_key": hex_or_null(&u.consensus_public_key),
            "reason": u.reason,
        })).collect::<Vec<_>>(),
        "generated_at": topo.generated_at,
    })
}

/// Short fixed-width header tag for a topic in the per-node summary table.
fn topic_tag(topic: &str) -> String {
    topic.chars().take(5).collect()
}

/// Short trailing tag of a peer id for compact matrix column headers.
fn col_tag(peer_id: &[u8]) -> String {
    let full = PeerId::from_bytes(peer_id)
        .map(|p| p.to_base58())
        .unwrap_or_else(|_| hex::encode(peer_id));
    full.chars()
        .rev()
        .take(5)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_topics() -> Vec<String> {
        DEFAULT_TOPICS.iter().map(|t| t.to_string()).collect()
    }

    #[test]
    fn parse_topics_validates_against_canonical_list() {
        // Unknown topics are dropped; valid ones (in any order) are kept.
        assert_eq!(
            parse_topics(Some("consensus,bogus,contact-info")),
            vec!["consensus".to_string(), "contact-info".to_string()]
        );
        // All-invalid or empty falls back to the default.
        assert_eq!(parse_topics(Some("nope,also-nope")), default_topics());
        assert_eq!(parse_topics(Some("")), default_topics());
        assert_eq!(parse_topics(None), default_topics());
        // `all` selects every canonical topic.
        let all: Vec<String> = ALL_TOPICS.iter().map(|t| t.to_string()).collect();
        assert_eq!(parse_topics(Some("all")), all);
        assert_eq!(parse_topics(Some("consensus,all")), all);
    }

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

        let out = render_mesh_view(&view, &default_topics());
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

    // Build a topology node: `self_id` (validator), reporting `consensus` mesh
    // membership for each (peer_id, in_mesh) and node type.
    fn node(
        self_id: &PeerId,
        is_validator: bool,
        peers: &[(&PeerId, bool, proto::MeshNodeType)],
    ) -> proto::MeshView {
        proto::MeshView {
            local: Some(proto::MeshSelf {
                peer_id: self_id.to_bytes(),
                is_validator,
                snapchain_version: "13".to_string(),
                network: proto::FarcasterNetwork::Devnet as i32,
                ..Default::default()
            }),
            peers: peers
                .iter()
                .map(|(pid, in_mesh, node_type)| proto::MeshPeer {
                    peer_id: pid.to_bytes(),
                    node_type: *node_type as i32,
                    topics: vec![proto::TopicMembership {
                        topic: CONSENSUS_TOPIC.to_string(),
                        subscribed: true,
                        in_mesh: *in_mesh,
                    }],
                    ..Default::default()
                })
                .collect(),
            generated_at: 0,
        }
    }

    #[test]
    fn topology_matrix_shows_directional_asymmetry() {
        let val = proto::MeshNodeType::Validator;
        let a = PeerId::random();
        let b = PeerId::random();
        let c = PeerId::random();

        // A reports meshing with B and C. B reports A and C. C reports only B —
        // so A->C is a one-way link (A reports C, C does not report A).
        let topo = proto::MeshTopology {
            nodes: vec![
                node(&a, true, &[(&b, true, val), (&c, true, val)]),
                node(&b, true, &[(&a, true, val), (&c, true, val)]),
                node(&c, true, &[(&b, true, val), (&a, false, val)]),
            ],
            unreachable: vec![],
            generated_at: 0,
        };

        let out = render_topology(&topo, &default_topics());
        // Symmetric A<->B edge.
        assert!(out.contains("●"));
        // One-way A->C surfaces as both a `>` (A's row) and a `<` (C's row).
        assert!(out.contains('>'), "expected a row->col-only marker:\n{out}");
        assert!(out.contains('<'), "expected a col->row-only marker:\n{out}");
        assert!(out.contains("nodes=3"));
        assert!(out.contains("unreachable: (none)"));
    }

    #[test]
    fn topology_lists_unreachable_and_reader_spokes() {
        let val = proto::MeshNodeType::Validator;
        let non = proto::MeshNodeType::NonValidator;
        let a = PeerId::random();
        let b = PeerId::random();
        let reader = PeerId::random();
        let down = PeerId::random();

        let topo = proto::MeshTopology {
            nodes: vec![
                node(&a, true, &[(&b, true, val), (&reader, false, non)]),
                node(&b, true, &[(&a, true, val), (&reader, false, non)]),
            ],
            unreachable: vec![proto::UnreachableNode {
                peer_id: down.to_bytes(),
                consensus_public_key: vec![],
                reason: "not_connected".to_string(),
            }],
            generated_at: 7,
        };

        let out = render_topology(&topo, &default_topics());
        assert!(out.contains("UNREACHABLE (1)"));
        assert!(out.contains("not_connected"));
        // The reader is reported by both validators but deduped into one spoke row.
        assert!(out.contains("READER SPOKES (1)"));

        // JSON carries nodes, unreachable, and the generation time.
        let json = topology_json(&topo);
        assert_eq!(json["generated_at"], 7);
        assert_eq!(json["nodes"].as_array().unwrap().len(), 2);
        assert_eq!(json["unreachable"][0]["reason"], "not_connected");
    }

    #[test]
    fn direct_peers_render_as_links_not_partitions() {
        // An explicit/direct peer is held out of gossipsub's mesh (`in_mesh=false`)
        // but is still an effective consensus link — it must read as a direct link,
        // not a partition and not merely-subscribed.
        let a = PeerId::random();
        let b = PeerId::random();
        let view = proto::MeshView {
            local: Some(proto::MeshSelf {
                peer_id: a.to_bytes(),
                is_validator: true,
                network: proto::FarcasterNetwork::Testnet as i32,
                ..Default::default()
            }),
            peers: vec![proto::MeshPeer {
                peer_id: b.to_bytes(),
                node_type: proto::MeshNodeType::Validator as i32,
                direct_peer: true,
                topics: vec![proto::TopicMembership {
                    topic: CONSENSUS_TOPIC.to_string(),
                    subscribed: true,
                    in_mesh: false,
                }],
                ..Default::default()
            }],
            generated_at: 0,
        };

        let out = render_mesh_view(&view, &default_topics());
        // Rendered as a direct/explicit link, never a partition.
        assert!(out.contains("◆"), "expected a direct-link glyph:\n{out}");
        assert!(
            !out.contains("consensus-partition risk"),
            "a subscribed direct peer must not read as a partition:\n{out}"
        );
        // Counted toward effective consensus connectivity (1 of 1 validators).
        assert!(out.contains("consensus-mesh 1/1"), "{out}");
    }

    #[test]
    fn topology_matrix_shows_reciprocal_direct_links() {
        // Two validators that peer each other as direct/explicit peers (mesh-excluded)
        // must render a reciprocal ◆, not a false `·` partition.
        let a = PeerId::random();
        let b = PeerId::random();
        let direct = |self_id: &PeerId, peer: &PeerId| proto::MeshView {
            local: Some(proto::MeshSelf {
                peer_id: self_id.to_bytes(),
                is_validator: true,
                network: proto::FarcasterNetwork::Testnet as i32,
                ..Default::default()
            }),
            peers: vec![proto::MeshPeer {
                peer_id: peer.to_bytes(),
                node_type: proto::MeshNodeType::Validator as i32,
                direct_peer: true,
                topics: vec![proto::TopicMembership {
                    topic: CONSENSUS_TOPIC.to_string(),
                    subscribed: true,
                    in_mesh: false,
                }],
                ..Default::default()
            }],
            generated_at: 0,
        };
        let topo = proto::MeshTopology {
            nodes: vec![direct(&a, &b), direct(&b, &a)],
            unreachable: vec![],
            generated_at: 0,
        };
        let out = render_topology(&topo, &default_topics());
        assert!(
            out.contains("◆"),
            "expected reciprocal direct-link glyph in matrix:\n{out}"
        );
    }

    // A peer carrying explicit per-topic membership.
    fn peer_with_topics(pid: &PeerId, topics: &[(&str, bool)]) -> proto::MeshPeer {
        proto::MeshPeer {
            peer_id: pid.to_bytes(),
            node_type: proto::MeshNodeType::Validator as i32,
            topics: topics
                .iter()
                .map(|(t, in_mesh)| proto::TopicMembership {
                    topic: t.to_string(),
                    subscribed: true,
                    in_mesh: *in_mesh,
                })
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn single_node_shows_per_topic_columns() {
        let a = PeerId::random();
        let b = PeerId::random();
        let view = proto::MeshView {
            local: Some(proto::MeshSelf {
                peer_id: a.to_bytes(),
                is_validator: true,
                network: proto::FarcasterNetwork::Devnet as i32,
                ..Default::default()
            }),
            // Meshed on consensus, subscribed-but-not-meshed on mempool.
            peers: vec![peer_with_topics(
                &b,
                &[("consensus", true), ("mempool", false)],
            )],
            generated_at: 0,
        };

        let out = render_mesh_view(&view, &default_topics());
        // Both topics appear as columns.
        assert!(out.contains("consensus"));
        assert!(out.contains("mempool"));
        // The mempool drop is visible: in-mesh (●) and sub-only (○) both present.
        assert!(out.contains("●"));
        assert!(out.contains("○"));
    }

    #[test]
    fn topology_renders_a_matrix_per_topic() {
        let a = PeerId::random();
        let b = PeerId::random();

        let mk = |self_id: &PeerId, peer: proto::MeshPeer| proto::MeshView {
            local: Some(proto::MeshSelf {
                peer_id: self_id.to_bytes(),
                is_validator: true,
                snapchain_version: "13".to_string(),
                network: proto::FarcasterNetwork::Devnet as i32,
                ..Default::default()
            }),
            peers: vec![peer],
            generated_at: 0,
        };

        // Both mesh on consensus; only A reports B on mempool (one-way mempool).
        let topo = proto::MeshTopology {
            nodes: vec![
                mk(
                    &a,
                    peer_with_topics(&b, &[("consensus", true), ("mempool", true)]),
                ),
                mk(
                    &b,
                    peer_with_topics(&a, &[("consensus", true), ("mempool", false)]),
                ),
            ],
            unreachable: vec![],
            generated_at: 0,
        };

        let topics = vec!["consensus".to_string(), "mempool".to_string()];
        let out = render_topology(&topo, &topics);
        // One matrix per topic.
        assert!(out.contains("CONSENSUS LINKS"));
        assert!(out.contains("MEMPOOL LINKS"));
        // The mempool matrix surfaces the one-way link as a directional marker.
        let mempool = out.split("MEMPOOL LINKS").nth(1).unwrap();
        assert!(
            mempool.contains('>') || mempool.contains('<'),
            "expected mempool asymmetry:\n{out}"
        );
    }
}
