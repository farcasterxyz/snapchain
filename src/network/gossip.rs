use crate::consensus::consensus::{MalachiteEventShard, SystemMessage};
use crate::consensus::malachite::network_connector::MalachiteNetworkEvent;
use crate::consensus::malachite::snapchain_codec::SnapchainCodec;
use crate::consensus::proposer::PROTOCOL_VERSION;
use crate::core::types::{proto, SnapchainContext, SnapchainValidatorContext};
use crate::mempool::mempool::{MempoolRequest, MempoolSource};
use crate::proto::{
    gossip_message, read_node_message, ContactInfo, ContactInfoBody, FarcasterNetwork,
    GossipMessage,
};
use crate::storage::store::engine::MempoolMessage;
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use bytes::Bytes;
use futures::StreamExt;
use informalsystems_malachitebft_codec::Codec;
use informalsystems_malachitebft_core_types::{SignedProposal, SignedVote};
use informalsystems_malachitebft_network::{Channel, PeerIdExt};
use informalsystems_malachitebft_network::{MessageId, PeerId as MalachitePeerId};
use informalsystems_malachitebft_sync::{self as sync};
use libp2p::identity::ed25519::Keypair;
use libp2p::request_response::{InboundRequestId, OutboundRequestId};
use libp2p::swarm::dial_opts::DialOpts;
use libp2p::{
    gossipsub, noise, swarm::NetworkBehaviour, swarm::SwarmEvent, tcp, yamux, PeerId, Swarm,
};
use libp2p_connection_limits::ConnectionLimits;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::Duration;
use tokio::io;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

const DEFAULT_GOSSIP_PORT: u16 = 3382;
const DEFAULT_GOSSIP_HOST: &str = "127.0.0.1";
const MAX_GOSSIP_MESSAGE_SIZE: usize = 1024 * 1024 * 10; // 10 mb

const CONSENSUS_TOPIC: &str = "consensus";
const MEMPOOL_TOPIC: &str = "mempool";
const DECIDED_VALUES: &str = "decided-values";
const READ_NODE_PEER_STATUSES: &str = "read-node-peers";
const CONTACT_INFO: &str = "contact-info";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub address: String,
    pub announce_address: String,
    pub bootstrap_peers: String,
    pub contact_info_interval: Duration,
    pub bootstrap_reconnect_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        let address = format!(
            "/ip4/{}/udp/{}/quic-v1",
            DEFAULT_GOSSIP_HOST, DEFAULT_GOSSIP_PORT
        );
        Config {
            address: address.clone(),
            announce_address: address,
            bootstrap_peers: "".to_string(),
            contact_info_interval: Duration::from_secs(300),
            bootstrap_reconnect_interval: Duration::from_secs(30),
        }
    }
}

impl Config {
    pub fn new(address: String, bootstrap_peers: String) -> Self {
        Config::default()
            .with_address(address)
            .with_bootstrap_peers(bootstrap_peers)
    }

    fn with_address(self, address: String) -> Self {
        Config { address, ..self }
    }

    fn with_bootstrap_peers(self, bootstrap_peers: String) -> Self {
        Config {
            bootstrap_peers,
            ..self
        }
    }

    pub fn with_contact_info_interval(self, contact_info_interval: Duration) -> Self {
        Config {
            contact_info_interval,
            ..self
        }
    }

    pub fn with_bootstrap_reconnect_interval(self, bootstrap_reconnect_interval: Duration) -> Self {
        Config {
            bootstrap_reconnect_interval,
            ..self
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
    BroadcastDecidedValue(proto::DecidedValue),
    SubscribeToDecidedValuesTopic(),
}

pub enum GossipTopic {
    Consensus,
    DecidedValues,
    ReadNodePeerStatuses,
    Mempool,
    SyncRequest(MalachitePeerId, oneshot::Sender<OutboundRequestId>),
    SyncReply(InboundRequestId),
}

#[derive(NetworkBehaviour)]
pub struct SnapchainBehavior {
    pub gossipsub: gossipsub::Behaviour,
    pub rpc: sync::Behaviour,
    pub connection_limits: libp2p_connection_limits::Behaviour,
}

pub struct SnapchainGossip {
    pub swarm: Swarm<SnapchainBehavior>,
    pub tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
    rx: mpsc::Receiver<GossipEvent<SnapchainValidatorContext>>,
    system_tx: Sender<SystemMessage>,
    sync_channels: HashMap<InboundRequestId, sync::ResponseChannel>,
    read_node: bool,
    bootstrap_addrs: HashSet<String>,
    connected_bootstrap_addrs: HashSet<String>,
    announce_address: String,
    fc_network: FarcasterNetwork,
    contact_info_interval: Duration,
    bootstrap_reconnect_interval: Duration,
    statsd_client: StatsdClientWrapper,
}

impl SnapchainGossip {
    pub fn create(
        keypair: Keypair,
        config: &Config,
        system_tx: Sender<SystemMessage>,
        read_node: bool,
        fc_network: FarcasterNetwork,
        statsd_client: StatsdClientWrapper,
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
                let message_id_fn = |message: &gossipsub::Message| {
                    // This is the default implementation inside libp2p
                    let default_message_id_fn = |message: &gossipsub::Message| {
                        let mut source_string = if let Some(peer_id) = message.source.as_ref() {
                            peer_id.to_base58()
                        } else {
                            PeerId::from_bytes(&[0, 1, 0])
                                .expect("Valid peer id")
                                .to_base58()
                        };
                        source_string
                            .push_str(&message.sequence_number.unwrap_or_default().to_string());
                        MessageId::from(source_string)
                    };

                    match message.topic.as_str() {
                        MEMPOOL_TOPIC | CONTACT_INFO => {
                            let mut s = DefaultHasher::new();
                            message.data.hash(&mut s);
                            gossipsub::MessageId::from(s.finish().to_string())
                        }
                        _ => default_message_id_fn(message),
                    }
                };

                // Set a custom gossipsub configuration
                let gossipsub_config = gossipsub::ConfigBuilder::default()
                    .heartbeat_interval(Duration::from_secs(10)) // This is set to aid debugging by not cluttering the log space
                    .validation_mode(gossipsub::ValidationMode::Strict) // This sets the kind of message validation. The default is Strict (enforce message signing)
                    .message_id_fn(message_id_fn) // content-address mempool messages
                    .max_transmit_size(MAX_GOSSIP_MESSAGE_SIZE) // maximum message size that can be transmitted
                    .build()
                    .map_err(|msg| io::Error::new(io::ErrorKind::Other, msg))?; // Temporary hack because `build` does not return a proper `std::error::Error`.

                // build a gossipsub network behaviour
                let gossipsub = gossipsub::Behaviour::new(
                    gossipsub::MessageAuthenticity::Signed(key.clone()),
                    gossipsub_config,
                )?;

                let rpc = sync::Behaviour::new(sync::Config::default());

                let connection_limits = libp2p_connection_limits::Behaviour::new(
                    ConnectionLimits::default()
                        .with_max_established_incoming(Some(15))
                        .with_max_established_outgoing(Some(15))
                        .with_max_pending_incoming(Some(5))
                        .with_max_pending_outgoing(Some(5)),
                );

                Ok(SnapchainBehavior {
                    gossipsub,
                    rpc,
                    connection_limits,
                })
            })?
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
            .build();

        for addr in config.bootstrap_addrs() {
            let _ = Self::dial(&mut swarm, &addr);
        }

        if read_node {
            let topic = gossipsub::IdentTopic::new(READ_NODE_PEER_STATUSES);
            let result = swarm.behaviour_mut().gossipsub.subscribe(&topic);
            if let Err(e) = result {
                warn!("Failed to subscribe to topic: {:?}", e);
                return Err(Box::new(e));
            }
        } else {
            // Create a Gossipsub topic
            let topic = gossipsub::IdentTopic::new(CONSENSUS_TOPIC);
            // subscribes to our topic
            let result = swarm.behaviour_mut().gossipsub.subscribe(&topic);
            if let Err(e) = result {
                warn!("Failed to subscribe to topic: {:?}", e);
                return Err(Box::new(e));
            }

            let topic = gossipsub::IdentTopic::new(MEMPOOL_TOPIC);
            let result = swarm.behaviour_mut().gossipsub.subscribe(&topic);
            if let Err(e) = result {
                warn!("Failed to subscribe to topic: {:?}", e);
                return Err(Box::new(e));
            }
        }

        let topic = gossipsub::IdentTopic::new(CONTACT_INFO);
        let result = swarm.behaviour_mut().gossipsub.subscribe(&topic);
        if let Err(e) = result {
            warn!("Failed to subscribe to topic: {:?}", e);
            return Err(Box::new(e));
        }

        // Listen on all assigned port for this id
        swarm.listen_on(config.address.parse()?)?;

        // ~5 seconds of buffer (assuming 1K msgs/pec)
        let (tx, rx) = mpsc::channel(5000);
        Ok(SnapchainGossip {
            swarm,
            tx,
            rx,
            system_tx,
            sync_channels: HashMap::new(),
            read_node,
            bootstrap_addrs: config.bootstrap_addrs().into_iter().collect(),
            announce_address: config.announce_address.clone(),
            fc_network,
            contact_info_interval: config.contact_info_interval,
            bootstrap_reconnect_interval: config.bootstrap_reconnect_interval,
            statsd_client,
            connected_bootstrap_addrs: HashSet::new(),
        })
    }

    fn dial(
        swarm: &mut Swarm<SnapchainBehavior>,
        addr: &String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let parsed_addr: libp2p::Multiaddr = addr.parse()?;
        let opts = DialOpts::unknown_peer_id()
            .address(parsed_addr.clone())
            .build();
        info!("Dialing peer: {:?} ({:?})", parsed_addr, addr);
        let res = swarm.dial(opts);
        if let Err(e) = res {
            warn!("Failed to dial peer {:?}: {:?}", parsed_addr.clone(), e);
            return Err(Box::new(e));
        }
        Ok(())
    }

    pub async fn check_and_reconnect_to_bootstrap_peers(&mut self) {
        let connected_peers_count = self.swarm.connected_peers().count();
        // Validators should stay connected to all bootstrap peers. Read nodes should only try to connect if they're not connected to anybody else.
        if !self.read_node || (self.read_node && connected_peers_count == 0) {
            for addr in &self.bootstrap_addrs {
                if !self.connected_bootstrap_addrs.contains(addr) {
                    warn!("Attempting to reconnect to bootstrap peer: {}", addr);
                    let _ = Self::dial(&mut self.swarm, &addr);
                }
            }
        }
    }

    pub fn publish_contact_info(&mut self) {
        let contact_info = ContactInfo {
            body: Some(ContactInfoBody {
                peer_id: self.swarm.local_peer_id().to_bytes(),
                gossip_address: self.announce_address.clone(),
                network: self.fc_network as i32,
                snapchain_version: PROTOCOL_VERSION.to_string(),
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
            }),
        };

        let gossip_message = GossipMessage {
            gossip_message: Some(gossip_message::GossipMessage::ContactInfoMessage(
                contact_info,
            )),
        };
        self.publish(gossip_message.encode_to_vec(), CONTACT_INFO);
    }

    pub async fn start(self: &mut Self) {
        let mut reconnect_timer = tokio::time::interval(self.bootstrap_reconnect_interval);

        let mut publish_contact_info_timer = tokio::time::interval(self.contact_info_interval);

        loop {
            tokio::select! {
                _ = reconnect_timer.tick() => {
                    self.check_and_reconnect_to_bootstrap_peers().await;
                    self.statsd_client.gauge("gossip.connected_peers", self.swarm.connected_peers().count() as u64);
                },
                _ = publish_contact_info_timer.tick() => {
                    if self.read_node {
                        info!("Publishing contact info");
                        self.publish_contact_info()
                    }
                }
                gossip_event = self.swarm.select_next_some() => {
                    match gossip_event {
                        SwarmEvent::ConnectionEstablished {peer_id, endpoint, ..} => {
                            info!(total_peers = self.swarm.connected_peers().count(), "Connection established with peer: {peer_id}");
                            let event = MalachiteNetworkEvent::PeerConnected(MalachitePeerId::from_libp2p(&peer_id));
                            let res = self.system_tx.send(SystemMessage::MalachiteNetwork(MalachiteEventShard::None, event)).await;
                            if let Err(e) = res {
                                warn!("Failed to send connection established message: {}", e);
                            };
                            match endpoint {
                                libp2p::core::ConnectedPoint::Dialer { address, ..} => {
                                    if self.bootstrap_addrs.contains(&address.to_string()) {
                                        self.connected_bootstrap_addrs.insert(address.to_string());
                                    }

                                },
                                libp2p::core::ConnectedPoint::Listener { .. } => {},
                            };
                        },
                        SwarmEvent::ConnectionClosed {peer_id, cause, endpoint, ..} => {
                            info!("Connection closed with peer: {:?} due to: {:?}", peer_id, cause);
                            let event = MalachiteNetworkEvent::PeerDisconnected(MalachitePeerId::from_libp2p(&peer_id));
                            let res = self.system_tx.send(SystemMessage::MalachiteNetwork(MalachiteEventShard::None, event)).await;
                            if let Err(e) = res {
                                warn!("Failed to send connection closed message: {}", e);
                            }
                            match endpoint {
                                libp2p::core::ConnectedPoint::Dialer { address, ..} => {
                                    self.connected_bootstrap_addrs.remove(&address.to_string());
                                },
                                libp2p::core::ConnectedPoint::Listener { .. } => {},
                            };
                        },
                        SwarmEvent::Behaviour(SnapchainBehaviorEvent::Gossipsub(gossipsub::Event::Subscribed { peer_id, topic })) =>
                            info!("Peer: {peer_id} subscribed to topic: {topic}"),
                        SwarmEvent::Behaviour(SnapchainBehaviorEvent::Gossipsub(gossipsub::Event::Unsubscribed { peer_id, topic })) =>
                            info!("Peer: {peer_id} unsubscribed to topic: {topic}"),
                        SwarmEvent::NewListenAddr { address, .. } => {
                            info!(address = address.to_string(), "Local node is listening");
                            let res = self.system_tx.send(SystemMessage::MalachiteNetwork(MalachiteEventShard::None, MalachiteNetworkEvent::Listening(address))).await;
                            if let Err(e) = res {
                                warn!("Failed to send Listening message: {}", e);
                            }
                        },
                        SwarmEvent::Behaviour(SnapchainBehaviorEvent::Gossipsub(gossipsub::Event::Message {
                            propagation_source: peer_id,
                            message_id: _id,
                            message,
                        })) => {
                            if let Some(system_message) = self.map_gossip_bytes_to_system_message(peer_id, message.data) {
                                let res = self.system_tx.send(system_message).await;
                                if let Err(e) = res {
                                    warn!("Failed to send system block message: {}", e);
                                }
                            }
                        },
                        SwarmEvent::Behaviour(SnapchainBehaviorEvent::Rpc(sync_event)) => {
                            match sync_event {
                                sync::Event::Message {peer, message, connection_id: _} => {
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
                                                    warn!("Failed to send RPC request message: {}", e);
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
                                                    warn!("Failed to send RPC request message: {}", e);
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
                    if let Some((gossip_topics, encoded_message)) = self.process_gossip_event(event) {
                        for gossip_topic in gossip_topics {
                            match gossip_topic {
                                GossipTopic::Consensus => self.publish(encoded_message.clone(), CONSENSUS_TOPIC),
                                GossipTopic::DecidedValues=> self.publish(encoded_message.clone(), DECIDED_VALUES),
                                GossipTopic::ReadNodePeerStatuses => self.publish(encoded_message.clone(), READ_NODE_PEER_STATUSES),
                                GossipTopic::Mempool => self.publish(encoded_message.clone(), MEMPOOL_TOPIC),
                                GossipTopic::SyncRequest(peer_id, reply_tx) => {
                                    let peer = peer_id.to_libp2p();
                                    let request_id = self.swarm.behaviour_mut().rpc.send_request(peer, Bytes::from(encoded_message.clone()));
                                    if let Err(e) = reply_tx.send(request_id) {
                                        warn!("Failed to send RPC request: {}", e);
                                    }
                                },
                                GossipTopic::SyncReply(request_id) => {
                                    let Some(channel) = self.sync_channels.remove(&request_id) else {
                                        warn!(%request_id, "Received Sync reply for unknown request ID");
                                        continue;
                                    };

                                    let result = self.swarm.behaviour_mut().rpc.send_response(channel, Bytes::from(encoded_message.clone()));
                                    if let Err(e) = result {
                                        warn!("Failed to send RPC response: {}", e);
                                    }
                                },
                            }
                        }
                    }
                }
            }
        }
    }

    fn publish(&mut self, message: Vec<u8>, topic: &str) {
        let publish_topic = gossipsub::IdentTopic::new(topic);
        if let Err(e) = self
            .swarm
            .behaviour_mut()
            .gossipsub
            .publish(publish_topic, message)
        {
            warn!("Failed to publish gossip message: {} ({:?})", e, topic);
        }
    }

    pub fn handle_contact_info(&mut self, contact_info: ContactInfo) {
        // TODO(aditi): We might want to persist peers and reconnect to them on restart
        let contact_info_body = contact_info.body.unwrap();
        let contact_peer_id = PeerId::from_bytes(&contact_info_body.peer_id).unwrap();
        if let Some(peer_id) = self
            .swarm
            .connected_peers()
            .find(|peer_id| contact_peer_id == **peer_id)
        {
            info!(
                peer_id = peer_id.to_string(),
                "Already connected to peer, so not dialing"
            );
            return;
        }

        if contact_info_body.network() != self.fc_network {
            info!(
                peer_id = contact_peer_id.to_string(),
                "Peer running on different network"
            );
            return;
        }

        if contact_info_body.snapchain_version != PROTOCOL_VERSION.to_string() {
            info!(
                peer_id = contact_peer_id.to_string(),
                "Peer running a different protocol version"
            );
            return;
        }

        let _ = Self::dial(&mut self.swarm, &contact_info_body.gossip_address);
    }

    pub fn map_gossip_bytes_to_system_message(
        &mut self,
        peer_id: PeerId,
        gossip_message: Vec<u8>,
    ) -> Option<SystemMessage> {
        match proto::GossipMessage::decode(gossip_message.as_slice()) {
            Ok(gossip_message) => match gossip_message.gossip_message {
                Some(gossip_message::GossipMessage::ContactInfoMessage(contact_info)) => {
                    info!(
                        peer_id = peer_id.to_string(),
                        "Received contact info from peer"
                    );
                    // Validators should just dial the bootstrap set since the validator set is fixed.
                    if self.read_node {
                        self.handle_contact_info(contact_info);
                    }
                    None
                }

                Some(proto::gossip_message::GossipMessage::ReadNodeMessage(read_node_message)) => {
                    let read_node_message = read_node_message.read_node_message;
                    match read_node_message {
                        None => None,
                        Some(read_node_message) => match read_node_message {
                            read_node_message::ReadNodeMessage::DecidedValue(decided_value) => {
                                Some(SystemMessage::DecidedValueForReadNode(decided_value))
                            }
                        },
                    }
                }

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
                        Some(SystemMessage::Mempool(MempoolRequest::AddMessage(
                            mempool_message,
                            MempoolSource::Gossip,
                        )))
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
                            warn!("Failed to decode sync request: {}", e);
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
                            warn!("Failed to decode sync response: {}", e);
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

    // Return the bytes for the associated network message if there's one
    pub fn process_gossip_event(
        &mut self,
        event: Option<GossipEvent<SnapchainValidatorContext>>,
    ) -> Option<(Vec<GossipTopic>, Vec<u8>)> {
        let snapchain_codec = SnapchainCodec {};
        match event {
            Some(GossipEvent::SubscribeToDecidedValuesTopic()) => {
                let topic = gossipsub::IdentTopic::new(DECIDED_VALUES);
                let result = self.swarm.behaviour_mut().gossipsub.subscribe(&topic);
                if let Err(e) = result {
                    error!("Failed to subscribe to {} topic: {}", DECIDED_VALUES, e);
                }
                None
            }
            Some(GossipEvent::BroadcastDecidedValue(decided_value)) => {
                let gossip_message = proto::GossipMessage {
                    gossip_message: Some(proto::gossip_message::GossipMessage::ReadNodeMessage(
                        proto::ReadNodeMessage {
                            read_node_message: Some(
                                proto::read_node_message::ReadNodeMessage::DecidedValue(
                                    decided_value,
                                ),
                            ),
                        },
                    )),
                };
                Some((
                    vec![GossipTopic::DecidedValues],
                    gossip_message.encode_to_vec(),
                ))
            }
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
                Some((vec![GossipTopic::Consensus], gossip_message.encode_to_vec()))
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
                Some((vec![GossipTopic::Consensus], gossip_message.encode_to_vec()))
            }
            Some(GossipEvent::BroadcastFullProposal(full_proposal)) => {
                let gossip_message = proto::GossipMessage {
                    gossip_message: Some(proto::gossip_message::GossipMessage::FullProposal(
                        full_proposal,
                    )),
                };
                Some((vec![GossipTopic::Consensus], gossip_message.encode_to_vec()))
            }
            Some(GossipEvent::BroadcastMempoolMessage(message)) => {
                let proto_message = message.to_proto();
                let gossip_message = proto::GossipMessage {
                    gossip_message: Some(proto::gossip_message::GossipMessage::MempoolMessage(
                        proto_message,
                    )),
                };
                Some((vec![GossipTopic::Mempool], gossip_message.encode_to_vec()))
            }
            Some(GossipEvent::SyncRequest(peer_id, request, reply_tx)) => {
                let encoded = snapchain_codec.encode(&request);
                match encoded {
                    Ok(encoded) => {
                        let topic = GossipTopic::SyncRequest(peer_id, reply_tx);
                        Some((vec![topic], encoded.to_vec()))
                    }
                    Err(e) => {
                        warn!("Failed to encode sync request: {}", e);
                        None
                    }
                }
            }
            Some(GossipEvent::SyncReply(request_id, response)) => {
                let encoded = snapchain_codec.encode(&response);
                match encoded {
                    Ok(encoded) => {
                        let topic = GossipTopic::SyncReply(request_id);
                        Some((vec![topic], encoded.to_vec()))
                    }
                    Err(e) => {
                        warn!("Failed to encode sync reply: {}", e);
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

                        let topics = if self.read_node {
                            vec![GossipTopic::ReadNodePeerStatuses]
                        } else {
                            vec![GossipTopic::Consensus, GossipTopic::ReadNodePeerStatuses]
                        };

                        // Should probably use a separate topic for status messages, but these are infrequent
                        Some((topics, gossip_message.encode_to_vec()))
                    }
                    Err(e) => {
                        warn!("Failed to encode status message: {}", e);
                        None
                    }
                }
            }
            None => None,
        }
    }
}
