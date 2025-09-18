use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::StreamExt;
use libp2p::identity::ed25519::Keypair;
use libp2p::swarm::SwarmEvent;
use libp2p::PeerId;
use prost::Message as _;
use tracing::{info, warn};

use crate::bootstrap::replication::rpc_client::RpcClientsManager;
use crate::network::gossip::{Config as GossipConfig, SnapchainGossip};
use crate::proto::{self, ContactInfoBody};
use crate::utils::statsd_wrapper::StatsdClientWrapper;

const INCOMPATIBLE_PEER_COOLDOWN: Duration = Duration::from_secs(30 * 60); // 30 minutes

// PeerId -> time we marked it incompatible
type CooldownMap = Arc<Mutex<HashMap<PeerId, Instant>>>;

pub struct PeerDiscoverer {
    gossip: SnapchainGossip,
    rpc_manager: Arc<RpcClientsManager>,
    target_height: u64,
    incompatible_peers: CooldownMap,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
}

impl PeerDiscoverer {
    pub async fn new(
        gossip_config: &GossipConfig,
        rpc_manager: Arc<RpcClientsManager>,
        target_height: u64,
        fc_network: crate::proto::FarcasterNetwork,
        statsd_client: StatsdClientWrapper,
        shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // Ephemeral keypair for discovery session
        let keypair = Keypair::generate();
        let gossip = SnapchainGossip::create(
            keypair,
            gossip_config,
            None, // no system channel
            true, // behave like read node (subscribe to contact info / statuses)
            fc_network,
            statsd_client,
        )
        .await?;

        Ok(Self {
            gossip,
            rpc_manager,
            target_height,
            incompatible_peers: Arc::new(Mutex::new(HashMap::new())),
            shutdown_rx,
        })
    }

    pub async fn run(mut self) {
        info!(target_height = self.target_height, "PeerDiscoverer started");
        let mut reconnect_timer = tokio::time::interval(Duration::from_secs(300)); // every 5 minutes

        loop {
            tokio::select! {
                _ = &mut self.shutdown_rx => {
                    info!("PeerDiscoverer received shutdown signal; exiting");
                    break;
                }
                 _ = reconnect_timer.tick() => {
                    self.gossip.check_and_reconnect_to_bootstrap_peers().await;
                },
                event = self.gossip.swarm.select_next_some() => {
                    if let SwarmEvent::Behaviour(crate::network::gossip::SnapchainBehaviorEvent::Gossipsub(
                        libp2p::gossipsub::Event::Message { message, .. }
                    )) = event {
                        if let Ok(gossip_msg) = proto::GossipMessage::decode(message.data.as_slice()) {
                            if let Some(proto::gossip_message::GossipMessage::ContactInfoMessage(ci)) = gossip_msg.gossip_message {
                                if let Some(body) = ci.body { self.handle_contact_info(body); }
                            }
                        }
                    }
                }
            }
        }
    }

    fn gossip_addr_to_http_addr(&self, gossip_addr: &str) -> Option<String> {
        let parts: Vec<&str> = gossip_addr.split('/').collect();
        if parts.len() >= 3 && parts[1] == "ip4" {
            let ip = parts[2];
            Some(format!("https://{}:3383", ip))
        } else {
            None
        }
    }

    fn handle_contact_info(&self, contact_info: ContactInfoBody) {
        let peer_id = match PeerId::from_bytes(&contact_info.peer_id) {
            Ok(p) => p,
            Err(_) => {
                warn!("Invalid peer id in contact info");
                return;
            }
        };

        let http_addr = match self.gossip_addr_to_http_addr(&contact_info.gossip_address) {
            Some(addr) => addr,
            None => {
                warn!("Invalid gossip address format");
                return;
            }
        };

        // Cooldown check
        if let Some(ts) = self.incompatible_peers.lock().unwrap().get(&peer_id) {
            if ts.elapsed() < INCOMPATIBLE_PEER_COOLDOWN {
                return;
            }
        }

        // Already known
        if self.rpc_manager.knows_peer(&http_addr) {
            return;
        }

        // Spawn validation (metadata height match) via RpcClientsManager::add_new_peer
        let rpc_manager = self.rpc_manager.clone();
        let incompatible = self.incompatible_peers.clone();
        info!(peer = %peer_id, addr = %http_addr, "Discovered potential replication peer");
        tokio::spawn(async move {
            let handle = rpc_manager.add_new_peer(http_addr.clone());
            match handle.await {
                Ok(Ok(true)) => {
                    info!(peer = %peer_id, addr = %http_addr, "Added new replication peer")
                }
                _ => {
                    warn!(peer = %peer_id, addr = %http_addr, "Peer incompatible or validation failed; cooling down");
                    incompatible.lock().unwrap().insert(peer_id, Instant::now());
                }
            }
        });
    }
}
