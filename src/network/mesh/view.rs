//! Validator classification + enrichment for the mesh view.
//!
//! Validator identity is cryptographic, not heuristic: a node's libp2p keypair
//! *is* its validator signing key, and a validator's `Address` is its 32-byte
//! Ed25519 public key. So a validator's `PeerId` derives deterministically from
//! its public key, and we identify validators by mapping connected peer IDs to
//! the set of validator-derived PeerIds — no gossipsub topic heuristic.

use crate::proto;
use libp2p::PeerId;
use std::collections::HashMap;

/// Map of validator `PeerId` -> consensus public key bytes, derived from the
/// validator set's hex public keys.
pub type ValidatorPeerIds = HashMap<PeerId, Vec<u8>>;

/// Derive the libp2p `PeerId` (and raw public key bytes) for a validator's
/// hex-encoded Ed25519 public key. Returns `None` for malformed keys.
pub fn peer_id_from_validator_hex(hex_key: &str) -> Option<(PeerId, Vec<u8>)> {
    let bytes = hex::decode(hex_key).ok()?;
    let ed = libp2p::identity::ed25519::PublicKey::try_from_bytes(&bytes).ok()?;
    let public: libp2p::identity::PublicKey = ed.into();
    Some((public.to_peer_id(), bytes))
}

/// Build the validator `PeerId` index from the configured validator public
/// keys (hex). Malformed keys are skipped.
pub fn build_validator_peer_ids<'a, I>(hex_keys: I) -> ValidatorPeerIds
where
    I: IntoIterator<Item = &'a String>,
{
    hex_keys
        .into_iter()
        .filter_map(|k| peer_id_from_validator_hex(k))
        .collect()
}

/// Enrich a raw mesh view (peer facts from the gossip layer) with validator
/// classification, fill in `self` fields the gossip layer can't know, and
/// optionally drop non-validator peers.
pub fn classify_mesh_view(
    mut view: proto::MeshView,
    validators: &ValidatorPeerIds,
    current_height: u64,
    validators_only: bool,
) -> proto::MeshView {
    if let Some(local) = view.local.as_mut() {
        local.current_height = current_height;
        if let Ok(pid) = PeerId::from_bytes(&local.peer_id) {
            if let Some(pubkey) = validators.get(&pid) {
                local.is_validator = true;
                local.consensus_public_key = pubkey.clone();
            }
        }
    }

    view.peers = view
        .peers
        .into_iter()
        .filter_map(|mut peer| {
            let matched = PeerId::from_bytes(&peer.peer_id)
                .ok()
                .and_then(|pid| validators.get(&pid).cloned());
            match matched {
                Some(pubkey) => {
                    peer.node_type = proto::MeshNodeType::Validator as i32;
                    peer.consensus_public_key = Some(pubkey);
                }
                None => {
                    peer.node_type = proto::MeshNodeType::NonValidator as i32;
                    peer.consensus_public_key = None;
                }
            }
            let is_validator = peer.node_type == proto::MeshNodeType::Validator as i32;
            if validators_only && !is_validator {
                None
            } else {
                Some(peer)
            }
        })
        .collect();

    view
}

#[cfg(test)]
mod tests {
    use super::*;

    // A known devnet validator key (hex Ed25519 public key) and the PeerId it
    // derives to are computed at runtime so the test is self-consistent.
    fn validator_key() -> (String, PeerId) {
        let kp = libp2p::identity::ed25519::Keypair::generate();
        let hex_key = hex::encode(kp.public().to_bytes());
        let (pid, _) = peer_id_from_validator_hex(&hex_key).unwrap();
        (hex_key, pid)
    }

    #[test]
    fn derives_peer_id_matching_libp2p_identity() {
        let kp = libp2p::identity::ed25519::Keypair::generate();
        let expected: libp2p::identity::PublicKey = kp.public().into();
        let expected_pid = expected.to_peer_id();
        let hex_key = hex::encode(kp.public().to_bytes());
        let (derived, pubkey) = peer_id_from_validator_hex(&hex_key).unwrap();
        assert_eq!(derived, expected_pid);
        assert_eq!(pubkey, kp.public().to_bytes());
    }

    #[test]
    fn classifies_and_filters() {
        let (hex_key, validator_pid) = validator_key();
        let validators = build_validator_peer_ids([&hex_key]);

        let non_validator = PeerId::random();
        let view = proto::MeshView {
            local: Some(proto::MeshSelf {
                peer_id: validator_pid.to_bytes(),
                ..Default::default()
            }),
            peers: vec![
                proto::MeshPeer {
                    peer_id: validator_pid.to_bytes(),
                    node_type: proto::MeshNodeType::Unknown as i32,
                    ..Default::default()
                },
                proto::MeshPeer {
                    peer_id: non_validator.to_bytes(),
                    node_type: proto::MeshNodeType::Unknown as i32,
                    ..Default::default()
                },
            ],
            generated_at: 0,
        };

        // With everyone shown: validator classified, non-validator classified.
        let all = classify_mesh_view(view.clone(), &validators, 42, false);
        assert_eq!(all.local.as_ref().unwrap().current_height, 42);
        assert!(all.local.as_ref().unwrap().is_validator);
        assert_eq!(all.peers.len(), 2);
        assert_eq!(
            all.peers[0].node_type,
            proto::MeshNodeType::Validator as i32
        );
        assert_eq!(
            all.peers[1].node_type,
            proto::MeshNodeType::NonValidator as i32
        );

        // validators_only drops the non-validator.
        let only = classify_mesh_view(view, &validators, 42, true);
        assert_eq!(only.peers.len(), 1);
        assert_eq!(only.peers[0].peer_id, validator_pid.to_bytes());
    }
}
