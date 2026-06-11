//! Centralized mesh crawl: assemble a network-wide topology by querying each
//! connected validator for its local mesh view over the diagnostics
//! request-response behaviour, then aggregating the responses.
//!
//! **Connected-only.** The serving node queries validators it already has a live
//! connection to (in the hub-and-spoke core mesh, that is the whole validator
//! set from any validator's vantage). Querying a validator it is not connected
//! to would require dialing, and the only dialable address is the contact info
//! validators never publish (#911 / NEYN-11921) — so unconnected validators are
//! reported as `not_connected` rather than dialed. Dial-and-query is deferred.
//!
//! **Traversal is always validators-only.** The `validators_only` flag controls
//! only the OUTPUT (which peers appear in each returned view); the crawl never
//! sends a request to a read node. So with `validators_only=false` each
//! validator additionally *reports* its reader spokes, but readers are never
//! *queried*.

use crate::core::types::SnapchainValidatorContext;
use crate::network::gossip::GossipEvent;
use crate::network::mesh::view::{classify_mesh_view, ValidatorPeerIds};
use crate::proto;
use libp2p::PeerId;
use std::collections::HashSet;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

/// Per-peer deadline for a diagnostics request. Requests are issued up front and
/// awaited concurrently, so this bounds overall wall-clock, not the sum.
const CRAWL_REQUEST_TIMEOUT: Duration = Duration::from_secs(3);
/// Deadline for retrieving this node's own local view from the gossip task.
const LOCAL_VIEW_TIMEOUT: Duration = Duration::from_millis(500);
/// Safety cap on validators queried in one crawl (the validator set is small;
/// this only guards against a pathological config).
const MAX_VALIDATORS_QUERIED: usize = 256;

type GossipSender = mpsc::Sender<GossipEvent<SnapchainValidatorContext>>;

/// Crawl the connected validator set and return the aggregated topology.
pub async fn crawl_mesh(
    gossip_tx: &GossipSender,
    validators: &ValidatorPeerIds,
    current_height: u64,
    validators_only: bool,
    generated_at: u64,
) -> Result<proto::MeshTopology, String> {
    // 1. This node's own (classified) view — the crawl's root node.
    let local = classify_mesh_view(
        fetch_local_view(gossip_tx).await?,
        validators,
        current_height,
        validators_only,
    );
    let self_pid = local
        .local
        .as_ref()
        .and_then(|l| PeerId::from_bytes(&l.peer_id).ok());

    // 2. Seed = connected validator peers. Filter on node_type so the traversal
    //    set is validators-only regardless of the `validators_only` output flag,
    //    and never includes self.
    let seed: Vec<PeerId> = local
        .peers
        .iter()
        .filter(|p| p.node_type == proto::MeshNodeType::Validator as i32)
        .filter_map(|p| PeerId::from_bytes(&p.peer_id).ok())
        .filter(|pid| Some(*pid) != self_pid)
        .take(MAX_VALIDATORS_QUERIED)
        .collect();
    let seed_set: HashSet<PeerId> = seed.iter().copied().collect();

    // 3. Issue all requests up front (in-flight concurrently), then await each.
    let mut receivers = Vec::with_capacity(seed.len());
    for pid in &seed {
        let (tx, rx) = oneshot::channel();
        let request = proto::GetMeshViewRequest {
            validators_only,
            ttl: 0,
            visited_peer_ids: vec![],
        };
        if gossip_tx
            .send(GossipEvent::SendMeshViewRequest(*pid, request, tx))
            .await
            .is_ok()
        {
            receivers.push((*pid, rx));
        }
    }

    let responses = futures::future::join_all(
        receivers
            .into_iter()
            .map(|(pid, rx)| async move { (pid, timeout(CRAWL_REQUEST_TIMEOUT, rx).await) }),
    )
    .await;

    // 4. Aggregate. Responding validators become `nodes`; queried-but-silent
    //    ones become `unreachable`.
    let mut nodes = vec![local];
    let mut unreachable = Vec::new();
    for (pid, result) in responses {
        match result {
            Ok(Ok(Ok(view))) => {
                nodes.push(classify_mesh_view(
                    view,
                    validators,
                    current_height,
                    validators_only,
                ));
            }
            Ok(Ok(Err(err))) => {
                unreachable.push(unreachable_node(&pid, validators, format!("error: {err}")))
            }
            Ok(Err(_)) => unreachable.push(unreachable_node(
                &pid,
                validators,
                "error: response channel closed".to_string(),
            )),
            Err(_) => unreachable.push(unreachable_node(&pid, validators, "timeout".to_string())),
        }
    }

    // 5. Validators in the set we never queried (no live connection) — surface
    //    them as `not_connected` rather than silently dropping.
    for (pid, pubkey) in validators {
        if Some(*pid) == self_pid || seed_set.contains(pid) {
            continue;
        }
        unreachable.push(proto::UnreachableNode {
            peer_id: pid.to_bytes(),
            consensus_public_key: pubkey.clone(),
            reason: "not_connected".to_string(),
        });
    }

    Ok(proto::MeshTopology {
        nodes,
        unreachable,
        generated_at,
    })
}

async fn fetch_local_view(gossip_tx: &GossipSender) -> Result<proto::MeshView, String> {
    let (tx, rx) = oneshot::channel();
    gossip_tx
        .send(GossipEvent::GetMeshView(tx))
        .await
        .map_err(|e| e.to_string())?;
    timeout(LOCAL_VIEW_TIMEOUT, rx)
        .await
        .map_err(|_| "timeout retrieving local mesh view".to_string())?
        .map_err(|e| e.to_string())
}

fn unreachable_node(
    pid: &PeerId,
    validators: &ValidatorPeerIds,
    reason: String,
) -> proto::UnreachableNode {
    proto::UnreachableNode {
        peer_id: pid.to_bytes(),
        consensus_public_key: validators.get(pid).cloned().unwrap_or_default(),
        reason,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::mesh::view::{build_validator_peer_ids, peer_id_from_validator_hex};
    use std::sync::{Arc, Mutex};

    // Generate an Ed25519 key and its derived PeerId.
    fn gen() -> (String, PeerId) {
        let kp = libp2p::identity::ed25519::Keypair::generate();
        let hex_key = hex::encode(kp.public().to_bytes());
        let (pid, _) = peer_id_from_validator_hex(&hex_key).unwrap();
        (hex_key, pid)
    }

    // Raw (unclassified) local view: `self_id` connected to each peer on the
    // consensus topic. node_type is Unknown, as the gossip layer emits it.
    fn raw_view(self_id: &PeerId, peers: &[&PeerId]) -> proto::MeshView {
        proto::MeshView {
            local: Some(proto::MeshSelf {
                peer_id: self_id.to_bytes(),
                ..Default::default()
            }),
            peers: peers
                .iter()
                .map(|pid| proto::MeshPeer {
                    peer_id: pid.to_bytes(),
                    node_type: proto::MeshNodeType::Unknown as i32,
                    topics: vec![proto::TopicMembership {
                        topic: "consensus".to_string(),
                        subscribed: true,
                        in_mesh: true,
                    }],
                    ..Default::default()
                })
                .collect(),
            generated_at: 0,
        }
    }

    #[tokio::test]
    async fn crawl_queries_only_validators_and_reports_not_connected() {
        let (a_hex, a_pid) = gen(); // self
        let (b_hex, b_pid) = gen(); // connected validator
        let (c_hex, c_pid) = gen(); // connected validator
        let (d_hex, d_pid) = gen(); // validator in set, NOT connected
        let (_r_hex, r_pid) = gen(); // read node (not in the validator set)
        let validators = build_validator_peer_ids([&a_hex, &b_hex, &c_hex, &d_hex]);

        let (tx, mut rx) = mpsc::channel::<GossipEvent<SnapchainValidatorContext>>(32);
        // A is connected to validators B, C and reader R.
        let local = raw_view(&a_pid, &[&b_pid, &c_pid, &r_pid]);

        let queried = Arc::new(Mutex::new(Vec::new()));
        let q = queried.clone();
        let mock = tokio::spawn(async move {
            while let Some(ev) = rx.recv().await {
                match ev {
                    GossipEvent::GetMeshView(reply) => {
                        let _ = reply.send(local.clone());
                    }
                    GossipEvent::SendMeshViewRequest(pid, _req, reply) => {
                        q.lock().unwrap().push(pid);
                        let _ = reply.send(Ok(raw_view(&pid, &[])));
                    }
                    _ => {}
                }
            }
        });

        let topo = crawl_mesh(&tx, &validators, 100, true, 0).await.unwrap();
        drop(tx);
        mock.await.unwrap();

        let queried = queried.lock().unwrap();
        // Only the two connected validators are queried — the reader never is.
        assert_eq!(queried.len(), 2);
        assert!(queried.contains(&b_pid) && queried.contains(&c_pid));
        assert!(!queried.contains(&r_pid));

        // nodes = self + the two responders.
        assert_eq!(topo.nodes.len(), 3);
        // The in-set-but-unconnected validator is surfaced, not dropped.
        assert!(topo
            .unreachable
            .iter()
            .any(|u| u.peer_id == d_pid.to_bytes() && u.reason == "not_connected"));
        // The reader is never reported as an unreachable validator.
        assert!(!topo
            .unreachable
            .iter()
            .any(|u| u.peer_id == r_pid.to_bytes()));
    }
}
