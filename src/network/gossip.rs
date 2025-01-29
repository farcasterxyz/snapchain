use crate::consensus::consensus::{ConsensusMsg, MalachiteEventShard, SystemMessage};
use crate::consensus::malachite::network_connector::MalachiteNetworkEvent;
use crate::core::types::{
    proto, ShardId, SnapchainContext, SnapchainShard, SnapchainValidator, SnapchainValidatorContext,
};
use crate::storage::store::engine::MempoolMessage;
use bytes::Bytes;
use futures::StreamExt;
use informalsystems_malachitebft_core_types::{SignedProposal, SignedVote};
use informalsystems_malachitebft_network::PeerId as MalachitePeerId;
use informalsystems_malachitebft_network::{Channel, PeerIdExt};
use libp2p::identity::ed25519::Keypair;
use libp2p::swarm::dial_opts::DialOpts;
use libp2p::{
    gossipsub, noise, swarm::NetworkBehaviour, swarm::SwarmEvent, tcp, yamux, PeerId, Swarm,
};
use prost::Message;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Duration;
use tokio::io;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tracing::{debug, info, warn};

const DEFAULT_GOSSIP_PORT: u16 = 3382;
const DEFAULT_GOSSIP_HOST: &str = "127.0.0.1";
const MAX_GOSSIP_MESSAGE_SIZE: usize = 1024 * 1024 * 10; // 10 mb

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub address: String,
    pub bootstrap_peers: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            address: format!(
                "/ip4/{}/udp/{}/quic-v1",
                DEFAULT_GOSSIP_HOST, DEFAULT_GOSSIP_PORT
            ),
            bootstrap_peers: "".to_string(),
        }
    }
}

impl Config {
    pub fn new(address: String, bootstrap_peers: String) -> Self {
        Config {
            address,
            bootstrap_peers,
        }
    }

    pub fn bootstrap_addrs(&self) -> Vec<String> {
        self.bootstrap_peers
            .split(',')
            .map(|s| s.trim().to_string())
            .collect()
    }
}

pub enum GossipEvent<Ctx: SnapchainContext> {
    BroadcastSignedVote(SignedVote<Ctx>),
    BroadcastSignedProposal(SignedProposal<Ctx>),
    BroadcastFullProposal(proto::FullProposal),
    RegisterValidator(proto::RegisterValidator),
    BroadcastMempoolMessage(MempoolMessage),
}

pub enum GossipTopic {
    Consensus,
    Mempool,
}

#[derive(NetworkBehaviour)]
pub struct SnapchainBehavior {
    pub gossipsub: gossipsub::Behaviour,
}

pub struct SnapchainGossip {
    pub swarm: Swarm<SnapchainBehavior>,
    pub tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
    rx: mpsc::Receiver<GossipEvent<SnapchainValidatorContext>>,
    system_tx: Sender<SystemMessage>,
}

impl SnapchainGossip {
    pub fn create(
        keypair: Keypair,
        config: Config,
        system_tx: Sender<SystemMessage>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut swarm = libp2p::SwarmBuilder::with_existing_identity(keypair.clone().into())
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                yamux::Config::default,
            )?
            .with_quic()
            .with_behaviour(|key| {
                // To content-address message, we can take the hash of message and use it as an ID.
                let message_id_fn = |message: &gossipsub::Message| {
                    let mut s = DefaultHasher::new();
                    message.data.hash(&mut s);
                    gossipsub::MessageId::from(s.finish().to_string())
                };

                // Set a custom gossipsub configuration
                let gossipsub_config = gossipsub::ConfigBuilder::default()
                    .heartbeat_interval(Duration::from_secs(10)) // This is set to aid debugging by not cluttering the log space
                    .validation_mode(gossipsub::ValidationMode::Strict) // This sets the kind of message validation. The default is Strict (enforce message signing)
                    .message_id_fn(message_id_fn) // content-address messages. No two messages of the same content will be propagated.
                    .max_transmit_size(MAX_GOSSIP_MESSAGE_SIZE) // maximum message size that can be transmitted
                    .build()
                    .map_err(|msg| io::Error::new(io::ErrorKind::Other, msg))?; // Temporary hack because `build` does not return a proper `std::error::Error`.

                // build a gossipsub network behaviour
                let gossipsub = gossipsub::Behaviour::new(
                    gossipsub::MessageAuthenticity::Signed(key.clone()),
                    gossipsub_config,
                )?;

                Ok(SnapchainBehavior { gossipsub })
            })?
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
            .build();

        for addr in config.bootstrap_addrs() {
            info!("Processing bootstrap peer: {:?}", addr);
            let parsed_addr: libp2p::Multiaddr = addr.parse()?;
            let opts = DialOpts::unknown_peer_id()
                .address(parsed_addr.clone())
                .build();
            info!("Dialing bootstrap peer: {:?} ({:?})", parsed_addr, addr);
            let res = swarm.dial(opts);
            if let Err(e) = res {
                warn!(
                    "Failed to dial bootstrap peer {:?}: {:?}",
                    parsed_addr.clone(),
                    e
                );
            }
        }

        // Create a Gossipsub topic
        let topic = gossipsub::IdentTopic::new("test-net");
        // subscribes to our topic
        let result = swarm.behaviour_mut().gossipsub.subscribe(&topic);
        if let Err(e) = result {
            warn!("Failed to subscribe to topic: {:?}", e);
            return Err(Box::new(e));
        }

        let topic = gossipsub::IdentTopic::new("test-net-mempool");
        let result = swarm.behaviour_mut().gossipsub.subscribe(&topic);
        if let Err(e) = result {
            warn!("Failed to subscribe to topic: {:?}", e);
            return Err(Box::new(e));
        }

        // Listen on all assigned port for this id
        swarm.listen_on(config.address.parse()?)?;

        let (tx, rx) = mpsc::channel(100);
        Ok(SnapchainGossip {
            swarm,
            tx,
            rx,
            system_tx,
        })
    }

    pub async fn start(self: &mut Self) {
        loop {
            tokio::select! {
                gossip_event = self.swarm.select_next_some() => {
                    match gossip_event {
                        SwarmEvent::ConnectionEstablished {peer_id, ..} => {
                            info!("Connection established with peer: {peer_id}");
                            let event = MalachiteNetworkEvent::PeerConnected(MalachitePeerId::from_libp2p(&peer_id));
                            let res = self.system_tx.send(SystemMessage::MalachiteNetwork(MalachiteEventShard::None, event)).await;
                            if let Err(e) = res {
                                warn!("Failed to send connection established message: {:?}", e);
                            }
                        },
                        SwarmEvent::ConnectionClosed {peer_id, cause, ..} => {
                            info!("Connection closed with peer: {:?} due to: {:?}", peer_id, cause);
                            let event = MalachiteNetworkEvent::PeerDisconnected(MalachitePeerId::from_libp2p(&peer_id));
                            let res = self.system_tx.send(SystemMessage::MalachiteNetwork(MalachiteEventShard::None, event)).await;
                            if let Err(e) = res {
                                warn!("Failed to send connection closed message: {:?}", e);
                            }
                        },
                        SwarmEvent::Behaviour(SnapchainBehaviorEvent::Gossipsub(gossipsub::Event::Subscribed { peer_id, topic })) =>
                            info!("Peer: {peer_id} subscribed to topic: {topic}"),
                        SwarmEvent::Behaviour(SnapchainBehaviorEvent::Gossipsub(gossipsub::Event::Unsubscribed { peer_id, topic })) =>
                            info!("Peer: {peer_id} unsubscribed to topic: {topic}"),
                        SwarmEvent::NewListenAddr { address, .. } => {
                            info!(address = address.to_string(), "Local node is listening");
                            let res = self.system_tx.send(SystemMessage::MalachiteNetwork(MalachiteEventShard::None, MalachiteNetworkEvent::Listening(address))).await;
                            if let Err(e) = res {
                                warn!("Failed to send Listening message: {:?}", e);
                            }
                        },
                        SwarmEvent::Behaviour(SnapchainBehaviorEvent::Gossipsub(gossipsub::Event::Message {
                            propagation_source: peer_id,
                            message_id: _id,
                            message,
                        })) => {
                            if let Some(system_message) = Self::map_gossip_bytes_to_system_message(peer_id, message.data) {
                                let res = self.system_tx.send(system_message).await;
                                if let Err(e) = res {
                                    warn!("Failed to send system block message: {:?}", e);
                                }
                            }
                        },
                        _ => {}
                    }
                }
                event = self.rx.recv() => {
                    if let Some((gossip_topic, encoded_message)) = Self::map_gossip_event_to_bytes(event) {
                        match gossip_topic {
                            GossipTopic::Consensus => self.publish(encoded_message),
                            GossipTopic::Mempool => self.publish_mempool(encoded_message),
                        }
                    }
                }
            }
        }
    }

    fn publish(&mut self, message: Vec<u8>) {
        let topic = gossipsub::IdentTopic::new("test-net");
        if let Err(e) = self.swarm.behaviour_mut().gossipsub.publish(topic, message) {
            warn!("Failed to publish gossip message: {:?}", e);
        }
    }

    fn publish_mempool(&mut self, message: Vec<u8>) {
        let topic = gossipsub::IdentTopic::new("test-net-mempool");
        if let Err(e) = self.swarm.behaviour_mut().gossipsub.publish(topic, message) {
            warn!("Failed to publish gossip message: {:?}", e);
        }
    }

    pub fn map_gossip_bytes_to_system_message(
        peer_id: PeerId,
        gossip_message: Vec<u8>,
    ) -> Option<SystemMessage> {
        match proto::GossipMessage::decode(gossip_message.as_slice()) {
            Ok(gossip_message) => match gossip_message.gossip_message {
                Some(proto::gossip_message::GossipMessage::FullProposal(full_proposal)) => {
                    let height = full_proposal.height();
                    debug!(
                        "Received block with height {} from peer: {}",
                        height, peer_id
                    );
                    let malachite_peer_id = MalachitePeerId::from_libp2p(&peer_id);
                    let bytes = Bytes::from(full_proposal.encode_to_vec());
                    let event = MalachiteNetworkEvent::Message(
                        Channel::ProposalParts,
                        malachite_peer_id,
                        bytes,
                    );
                    let shard_result = full_proposal.shard_id();
                    if shard_result.is_err() {
                        warn!("Failed to get shard id from consensus message");
                        return None;
                    }
                    let shard = MalachiteEventShard::Shard(shard_result.unwrap());
                    Some(SystemMessage::MalachiteNetwork(shard, event))
                }
                Some(proto::gossip_message::GossipMessage::Validator(validator)) => {
                    debug!("Received validator registration from peer: {}", peer_id);
                    if let Some(validator) = validator.validator {
                        let public_key =
                            libp2p::identity::ed25519::PublicKey::try_from_bytes(&validator.signer);
                        if public_key.is_err() {
                            warn!("Failed to decode public key from peer: {}", peer_id);
                            return None;
                        }
                        let rpc_address = validator.rpc_address;
                        let shard_index = validator.shard_index;
                        let validator = SnapchainValidator::new(
                            SnapchainShard::new(shard_index),
                            public_key.unwrap(),
                            Some(rpc_address),
                            validator.current_height,
                        );
                        let consensus_message = ConsensusMsg::RegisterValidator(validator);
                        return Some(SystemMessage::Consensus(consensus_message));
                    }
                    None
                }
                Some(proto::gossip_message::GossipMessage::Consensus(signed_consensus_msg)) => {
                    let malachite_peer_id = MalachitePeerId::from_libp2p(&peer_id);
                    let bytes = Bytes::from(signed_consensus_msg.encode_to_vec());
                    let event = MalachiteNetworkEvent::Message(
                        Channel::Consensus,
                        malachite_peer_id,
                        bytes,
                    );
                    let shard_result = signed_consensus_msg.shard_id();
                    if shard_result.is_err() {
                        warn!("Failed to get shard id from consensus message");
                        return None;
                    }
                    let shard = MalachiteEventShard::Shard(shard_result.unwrap());
                    Some(SystemMessage::MalachiteNetwork(shard, event))
                }
                Some(proto::gossip_message::GossipMessage::MempoolMessage(message)) => {
                    if let Some(mempool_message_proto) = message.mempool_message {
                        let mempool_message = match mempool_message_proto {
                            proto::mempool_message::MempoolMessage::UserMessage(message) => {
                                MempoolMessage::UserMessage(message)
                            }
                        };
                        Some(SystemMessage::Mempool(mempool_message))
                    } else {
                        warn!("Unknown mempool message from peer: {}", peer_id);
                        None
                    }
                }
                None => {
                    warn!("Empty message from peer: {}", peer_id);
                    None
                }
            },
            Err(e) => {
                warn!("Failed to decode gossip message: {}", e);
                None
            }
        }
    }

    pub fn map_gossip_event_to_bytes(
        event: Option<GossipEvent<SnapchainValidatorContext>>,
    ) -> Option<(GossipTopic, Vec<u8>)> {
        match event {
            Some(GossipEvent::BroadcastSignedVote(vote)) => {
                let vote_proto = vote.to_proto();
                let gossip_message = proto::GossipMessage {
                    gossip_message: Some(proto::gossip_message::GossipMessage::Consensus(
                        proto::ConsensusMessage {
                            signature: vote.signature.0,
                            consensus_message: Some(
                                proto::consensus_message::ConsensusMessage::Vote(vote_proto),
                            ),
                        },
                    )),
                };
                Some((GossipTopic::Consensus, gossip_message.encode_to_vec()))
            }
            Some(GossipEvent::BroadcastSignedProposal(proposal)) => {
                let proposal_proto = proposal.to_proto();
                let gossip_message = proto::GossipMessage {
                    gossip_message: Some(proto::gossip_message::GossipMessage::Consensus(
                        proto::ConsensusMessage {
                            signature: proposal.signature.0,
                            consensus_message: Some(
                                proto::consensus_message::ConsensusMessage::Proposal(
                                    proposal_proto,
                                ),
                            ),
                        },
                    )),
                };
                Some((GossipTopic::Consensus, gossip_message.encode_to_vec()))
            }
            Some(GossipEvent::BroadcastFullProposal(full_proposal)) => {
                let gossip_message = proto::GossipMessage {
                    gossip_message: Some(proto::gossip_message::GossipMessage::FullProposal(
                        full_proposal,
                    )),
                };
                Some((GossipTopic::Consensus, gossip_message.encode_to_vec()))
            }
            Some(GossipEvent::RegisterValidator(register_validator)) => {
                debug!("Broadcasting validator registration");
                let gossip_message = proto::GossipMessage {
                    gossip_message: Some(proto::gossip_message::GossipMessage::Validator(
                        register_validator,
                    )),
                };
                Some((GossipTopic::Consensus, gossip_message.encode_to_vec()))
            }
            Some(GossipEvent::BroadcastMempoolMessage(message)) => {
                let proto_message = message.to_proto();
                let gossip_message = proto::GossipMessage {
                    gossip_message: Some(proto::gossip_message::GossipMessage::MempoolMessage(
                        proto_message,
                    )),
                };
                Some((GossipTopic::Mempool, gossip_message.encode_to_vec()))
            }
            None => None,
        }
    }
}
