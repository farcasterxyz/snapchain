use crate::consensus::consensus::{MalachiteEventShard, SystemMessage};
use crate::consensus::malachite::network_connector::MalachiteNetworkEvent;
use crate::consensus::malachite::snapchain_codec::SnapchainCodec;
use crate::core::types::{proto, SnapchainContext, SnapchainValidatorContext};
use crate::storage::store::engine::MempoolMessage;
use bytes::Bytes;
use futures::StreamExt;
use informalsystems_malachitebft_codec::Codec;
use informalsystems_malachitebft_core_types::{SignedProposal, SignedVote};
use informalsystems_malachitebft_network::PeerId as MalachitePeerId;
use informalsystems_malachitebft_network::{Channel, PeerIdExt};
use informalsystems_malachitebft_sync as sync;
use libp2p::identity::ed25519::Keypair;
use libp2p::request_response::{InboundRequestId, OutboundRequestId};
use libp2p::swarm::dial_opts::DialOpts;
use libp2p::{
    gossipsub, noise, swarm::NetworkBehaviour, swarm::SwarmEvent, tcp, yamux, PeerId, Swarm,
};
use prost::Message;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::Duration;
use tokio::io;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot};
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
    BroadcastMempoolMessage(MempoolMessage),
    BroadcastStatus(sync::Status<SnapchainValidatorContext>),
    SyncRequest(
        MalachitePeerId,
        sync::Request<SnapchainValidatorContext>,
        oneshot::Sender<OutboundRequestId>,
    ),
    SyncReply(InboundRequestId, sync::Response<SnapchainValidatorContext>),
}

pub enum GossipTopic {
    Consensus,
    Mempool,
    SyncRequest(MalachitePeerId, oneshot::Sender<OutboundRequestId>),
    SyncReply(InboundRequestId),
}

#[derive(NetworkBehaviour)]
pub struct SnapchainBehavior {
    pub gossipsub: gossipsub::Behaviour,
    pub rpc: sync::Behaviour,
}

pub struct SnapchainGossip {
    pub swarm: Swarm<SnapchainBehavior>,
    pub tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
    rx: mpsc::Receiver<GossipEvent<SnapchainValidatorContext>>,
    system_tx: Sender<SystemMessage>,
    sync_channels: HashMap<InboundRequestId, sync::ResponseChannel>,
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

                let rpc = sync::Behaviour::new(sync::Config::default());

                Ok(SnapchainBehavior { gossipsub, rpc })
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
            sync_channels: HashMap::new(),
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
                        SwarmEvent::Behaviour(SnapchainBehaviorEvent::Rpc(sync_event)) => {
                            match sync_event {
                                sync::Event::Message {peer, message} => {
                                    match message {
                                        libp2p::request_response::Message::Request {
                                            request_id,
                                            request,
                                            channel,
                                        } => {
                                            self.sync_channels.insert(request_id, channel);
                                            let request = sync::RawMessage::Request {
                                                request_id,
                                                peer: MalachitePeerId::from_libp2p(&peer),
                                                body: request.0,
                                            };
                                            let event = Self::map_sync_message_to_system_message(request);
                                            if let Some(event) = event {
                                                let res = self.system_tx.send(event).await;
                                                if let Err(e) = res {
                                                    warn!("Failed to send RPC request message: {:?}", e);
                                                }
                                            }
                                        },
                                       libp2p::request_response::Message::Response {
                                            request_id,
                                            response,
                                        } => {
                                            let event = sync::RawMessage::Response {
                                                request_id,
                                                peer: MalachitePeerId::from_libp2p(&peer),
                                                body: response.0,
                                            };
                                            let event = Self::map_sync_message_to_system_message(event);
                                            if let Some(event) = event {
                                                let res = self.system_tx.send(event).await;
                                                if let Err(e) = res {
                                                    warn!("Failed to send RPC request message: {:?}", e);
                                                }
                                            }
                                        },
                                    }
                                },
                                _ => {}
                            }
                        }
                        _ => {}
                    }
                }
                event = self.rx.recv() => {
                    if let Some((gossip_topic, encoded_message)) = Self::map_gossip_event_to_bytes(event) {
                        match gossip_topic {
                            GossipTopic::Consensus => self.publish(encoded_message),
                            GossipTopic::Mempool => self.publish_mempool(encoded_message),
                            GossipTopic::SyncRequest(peer_id, reply_tx) => {
                                let peer = peer_id.to_libp2p();
                                let request_id = self.swarm.behaviour_mut().rpc.send_request(peer, Bytes::from(encoded_message));
                                if let Err(e) = reply_tx.send(request_id) {
                                    warn!("Failed to send RPC request: {:?}", e);
                                }
                            },
                            GossipTopic::SyncReply(request_id) => {
                                let Some(channel) = self.sync_channels.remove(&request_id) else {
                                    warn!(%request_id, "Received Sync reply for unknown request ID");
                                    continue;
                                };

                                let result = self.swarm.behaviour_mut().rpc.send_response(channel, Bytes::from(encoded_message));
                                if let Err(e) = result {
                                    warn!("Failed to send RPC response: {:?}", e);
                                }
                            },
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
                Some(proto::gossip_message::GossipMessage::Status(status)) => {
                    let encoded = status.encode_to_vec();
                    let Some(height) = status.height else {
                        warn!(
                            "Received status message without height from peer: {}",
                            peer_id
                        );
                        return None;
                    };
                    let shard = MalachiteEventShard::Shard(height.shard_index);
                    let malachite_peer_id = MalachitePeerId::from_libp2p(&peer_id);
                    let event = MalachiteNetworkEvent::Message(
                        Channel::Sync,
                        malachite_peer_id,
                        Bytes::from(encoded),
                    );
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

    pub fn map_sync_message_to_system_message(message: sync::RawMessage) -> Option<SystemMessage> {
        let snapchain_codec = SnapchainCodec {};
        match &message {
            sync::RawMessage::Request {
                request_id: _,
                peer: _,
                body,
            } => {
                let event = MalachiteNetworkEvent::Sync(message.clone());
                let request: sync::Request<SnapchainValidatorContext> =
                    match snapchain_codec.decode(body.clone()) {
                        Ok(request) => request,
                        Err(e) => {
                            warn!("Failed to decode sync request: {:?}", e);
                            return None;
                        }
                    };
                let shard_index = match request {
                    sync::Request::ValueRequest(request) => request.height.shard_index,
                    sync::Request::VoteSetRequest(request) => request.height.shard_index,
                };
                let shard = MalachiteEventShard::Shard(shard_index);
                Some(SystemMessage::MalachiteNetwork(shard, event))
            }
            sync::RawMessage::Response {
                request_id: _,
                peer: _,
                body,
            } => {
                let event = MalachiteNetworkEvent::Sync(message.clone());
                let response: sync::Response<SnapchainValidatorContext> =
                    match snapchain_codec.decode(body.clone()) {
                        Ok(response) => response,
                        Err(e) => {
                            warn!("Failed to decode sync response: {:?}", e);
                            return None;
                        }
                    };
                let shard_index = match response {
                    sync::Response::ValueResponse(response) => response.height.shard_index,
                    sync::Response::VoteSetResponse(response) => response.height.shard_index,
                };
                let shard = MalachiteEventShard::Shard(shard_index);
                Some(SystemMessage::MalachiteNetwork(shard, event))
            }
        }
    }

    pub fn map_gossip_event_to_bytes(
        event: Option<GossipEvent<SnapchainValidatorContext>>,
    ) -> Option<(GossipTopic, Vec<u8>)> {
        let snapchain_codec = SnapchainCodec {};
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
            Some(GossipEvent::BroadcastMempoolMessage(message)) => {
                let proto_message = message.to_proto();
                let gossip_message = proto::GossipMessage {
                    gossip_message: Some(proto::gossip_message::GossipMessage::MempoolMessage(
                        proto_message,
                    )),
                };
                Some((GossipTopic::Mempool, gossip_message.encode_to_vec()))
            }
            Some(GossipEvent::SyncRequest(peer_id, request, reply_tx)) => {
                let encoded = snapchain_codec.encode(&request);
                match encoded {
                    Ok(encoded) => {
                        let topic = GossipTopic::SyncRequest(peer_id, reply_tx);
                        Some((topic, encoded.to_vec()))
                    }
                    Err(e) => {
                        warn!("Failed to encode sync request: {:?}", e);
                        None
                    }
                }
            }
            Some(GossipEvent::SyncReply(request_id, response)) => {
                let encoded = snapchain_codec.encode(&response);
                match encoded {
                    Ok(encoded) => {
                        let topic = GossipTopic::SyncReply(request_id);
                        Some((topic, encoded.to_vec()))
                    }
                    Err(e) => {
                        warn!("Failed to encode sync reply: {:?}", e);
                        None
                    }
                }
            }
            Some(GossipEvent::BroadcastStatus(status)) => {
                let encoded = snapchain_codec.encode(&status);
                match encoded {
                    Ok(encoded) => {
                        let gossip_message = proto::GossipMessage {
                            gossip_message: Some(proto::gossip_message::GossipMessage::Status(
                                proto::StatusMessage::decode(encoded).unwrap(),
                            )),
                        };
                        // Should probably use a separate topic for status messages, but these are infrequent
                        Some((GossipTopic::Consensus, gossip_message.encode_to_vec()))
                    }
                    Err(e) => {
                        warn!("Failed to encode status message: {:?}", e);
                        None
                    }
                }
            }
            None => None,
        }
    }
}
