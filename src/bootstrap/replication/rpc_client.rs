use crate::bootstrap::replication::error::BootstrapError;
use crate::bootstrap::replication::service::ReplicatorBootstrapConfig;
use crate::proto::{
    self, replication_service_client::ReplicationServiceClient, GetShardSnapshotMetadataResponse,
    GetShardTransactionsResponse, ShardSnapshotMetadata,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, Mutex};
use tokio::time::{sleep, Instant};
use tonic::transport::Channel;
use tracing::{info, warn};

// Comma-separated list of initial peers to bootstrap from if none are configured.
pub const REPLICATION_INITIAL_PEERS: &str = "https://rho.farcaster.xyz:3381";

// A peer owns its own client and its stats, guarded by a Mutex for concurrent updates.
#[derive(Debug)]
struct Peer {
    client: ReplicationServiceClient<Channel>,
    stats: Mutex<PeerStats>,
}

#[derive(Debug, Clone, Default)]
struct PeerStats {
    errors: u64,
    total_latency: Duration,
    calls: u64,
}

// This manager handles peer selection, connection, and affinity.
pub(crate) struct PeerManager {
    // We use the address as a key and store the Peer in an Arc for shared ownership.
    peers: HashMap<String, Arc<Peer>>,
    pub peer_addresses: Vec<String>,
    // Affinity stores the prefered peer to use for each vts. We want to request all the pages for a
    // virtual trie shard (vts) from the same peer to take advantage of server-side caching.
    pub vts_peer_affinity: HashMap<u8, String>,
}

impl PeerManager {
    fn new(peer_addrs: Vec<String>) -> Self {
        Self {
            peers: HashMap::new(),
            peer_addresses: peer_addrs,
            vts_peer_affinity: HashMap::new(),
        }
    }

    // This is the single entry point to get a managed peer connection.
    async fn get_peer_for_vts(&mut self, vts: u8) -> Result<Arc<Peer>, BootstrapError> {
        // Check if we already have an established affinity for this vts.
        if let Some(peer_address) = self.vts_peer_affinity.get(&vts) {
            // If so, return the existing peer connection.
            if let Some(peer) = self.peers.get(peer_address) {
                return Ok(peer.clone());
            }
        }

        // If no affinity or peer doesn't exist, select and connect
        // Simple round-robin selection logic.
        let peer_address = self.peer_addresses[(vts as usize) % self.peer_addresses.len()].clone();

        // If we've already connected to this peer, return it.
        if let Some(peer) = self.peers.get(&peer_address) {
            // Establish affinity for next time.
            self.vts_peer_affinity.insert(vts, peer_address);
            return Ok(peer.clone());
        }

        //  If it's a new peer, connect and store it
        let client = ReplicationServiceClient::connect(peer_address.clone()).await?;
        let new_peer = Arc::new(Peer {
            client,
            stats: Mutex::new(PeerStats::default()),
        });

        self.peers.insert(peer_address.clone(), new_peer.clone());
        self.vts_peer_affinity.insert(vts, peer_address);

        Ok(new_peer)
    }
}

pub(crate) struct ReplicationServiceRpcData {
    pub peer_manager: PeerManager,
    // The cache holds (vts, next_page_token) -> oneshot::Receiver which will yield the response when ready.
    vts_next_page_cache: HashMap<
        (u8, Option<String>),
        oneshot::Receiver<Result<GetShardTransactionsResponse, BootstrapError>>,
    >,
}

pub struct RpcClientsManager {
    shard_id: u32,
    height: u64,
    metadata: ShardSnapshotMetadata,
    config: ReplicatorBootstrapConfig,
    inner: Arc<Mutex<ReplicationServiceRpcData>>,
}

impl RpcClientsManager {
    pub fn shard_id(&self) -> u32 {
        self.shard_id
    }

    pub async fn new(
        peer_addr: String,
        shard_id: u32,
        config: ReplicatorBootstrapConfig,
    ) -> Result<RpcClientsManager, BootstrapError> {
        // We'll create a new RPC client, seeded by the metadata in the peer.

        // First, get the metadata from the peer
        let snapshots = Self::get_shard_metadata(peer_addr.clone(), shard_id).await?;

        // Go over all the snapshots and find the one with the highest height
        let highest_snapshot = snapshots.snapshots.iter().max_by_key(|s| s.height).ok_or({
            BootstrapError::MetadataFetchError(format!(
                "No snapshots found for shard_id {} on peer {}",
                shard_id, peer_addr
            ))
        })?;

        // Create the PeerManager with the initial peer
        let peer_manager = PeerManager::new(vec![peer_addr.clone()]);
        let height = highest_snapshot.height;

        info!(
            "Initialized RpcClientsManager for shard_id {} at height {} with peer address {}",
            shard_id, height, peer_addr
        );

        Ok(Self {
            shard_id,
            height,
            metadata: highest_snapshot.clone(),
            config,
            inner: Arc::new(Mutex::new(ReplicationServiceRpcData {
                peer_manager,
                vts_next_page_cache: HashMap::new(),
            })),
        })
    }

    pub async fn add_new_peer(&self, peer_address: String) -> Result<bool, BootstrapError> {
        let shard_id = self.shard_id;
        let height = self.height;

        {
            // Lock the inner to see if we already know this peer
            let data = self.inner.lock().await;

            // Check if we already know this peer
            if data
                .peer_manager
                .peer_addresses
                .iter()
                .any(|addr| *addr == peer_address)
            {
                // Already known
                return Ok(true);
            }
        }

        match Self::get_shard_metadata(peer_address.clone(), shard_id).await {
            Ok(snapshots) => {
                if snapshots.snapshots.iter().any(|s| s.height == height) {
                    let mut data = self.inner.lock().await;
                    data.peer_manager.peer_addresses.push(peer_address);

                    Ok(true)
                } else {
                    warn!("peer {} doesn't have the required metadata", peer_address);
                    Ok(false)
                }
            }
            Err(e) => Err(e),
        }
    }

    pub fn get_metadata(&self) -> ShardSnapshotMetadata {
        self.metadata.clone()
    }

    // Allow tests to access the inner data for inspection.
    #[cfg(test)]
    pub(crate) fn inner(&self) -> Arc<Mutex<ReplicationServiceRpcData>> {
        Arc::clone(&self.inner)
    }

    pub async fn close(&self) {
        let mut data = self.inner.lock().await;
        data.peer_manager.peers.clear();
        data.peer_manager.peer_addresses.clear();
        data.peer_manager.vts_peer_affinity.clear();
        data.vts_next_page_cache.clear();
    }

    // This function takes a shared reference to the peer and locks it internally.
    async fn call_get_shard_transactions_with_retry(
        peer: Arc<Peer>,
        request: proto::GetShardTransactionsRequest,
        config: &ReplicatorBootstrapConfig,
    ) -> Result<GetShardTransactionsResponse, tonic::Status> {
        let start = Instant::now();
        let mut last_error = None;

        for _attempt in 1..=config.max_rpc_retries {
            let mut req = tonic::Request::new(request.clone());
            req.set_timeout(Duration::from_secs(15));
            // Clone the client from within the peer. This is a cheap operation.
            let mut client = peer.client.clone();

            let response = client.get_shard_transactions(req).await;

            // Lock the peer to update stats.
            {
                let mut s = peer.stats.lock().await;
                match response {
                    Ok(response) => {
                        let latency = start.elapsed();

                        s.calls += 1;
                        s.total_latency += latency;
                        return Ok(response.into_inner());
                    }
                    Err(e) => {
                        s.errors += 1;
                        last_error = Some(e);
                    }
                }
            }

            if last_error.is_some() {
                sleep(config.rpc_retry_delay).await;
            }
        }
        Err(last_error.unwrap())
    }

    // This helper handles the response and spawns the next page pre-fetch task.
    async fn handle_response_and_spawn_next(
        &self,
        vts: u8,
        next_page_token: Option<String>,
    ) -> Result<(), BootstrapError> {
        // treat empty string as None (no token)
        if let Some(next_page_token) = next_page_token.filter(|s| !s.is_empty()) {
            let (sender, receiver) = oneshot::channel();

            // insert the receiver into the cache while holding the lock
            {
                let mut data = self.inner.lock().await;
                data.vts_next_page_cache
                    .insert((vts, Some(next_page_token.clone())), receiver);
            }

            // capture what we need for the spawned task
            let inner = self.inner.clone();
            let shard_id = self.shard_id;
            let height = self.height;
            let next_page_token = next_page_token.clone();
            let config = self.config.clone();

            // spawn the background prefetch — do peer resolution and network calls inside the task
            tokio::spawn(async move {
                // obtain a peer while inside the spawned task (locks inner only inside this task)
                let peer_res = {
                    let mut data = inner.lock().await;
                    data.peer_manager.get_peer_for_vts(vts).await
                };

                match peer_res {
                    Ok(peer) => {
                        let response = Self::call_get_shard_transactions_with_retry(
                            peer,
                            proto::GetShardTransactionsRequest {
                                shard_id,
                                height,
                                trie_virtual_shard: vts as u32,
                                page_token: Some(next_page_token),
                            },
                            &config,
                        )
                        .await;
                        let _ = sender.send(response.map_err(BootstrapError::from));
                    }
                    Err(e) => {
                        // ensure the receiver gets an error instead of being left forever
                        let _ = sender.send(Err(e));
                    }
                }
            });
        }
        Ok(())
    }

    async fn get_shard_metadata(
        peer_address: String,
        shard_id: u32,
    ) -> Result<GetShardSnapshotMetadataResponse, BootstrapError> {
        let mut client = ReplicationServiceClient::connect(peer_address).await?;

        let mut req = tonic::Request::new(proto::GetShardSnapshotMetadataRequest { shard_id });
        req.set_timeout(Duration::from_secs(15));
        let response = client.get_shard_snapshot_metadata(req).await?;

        Ok(response.into_inner())
    }

    pub async fn get_shard_transactions(
        &self,
        vts: u8,
        page_token: Option<String>,
    ) -> Result<GetShardTransactionsResponse, BootstrapError> {
        let normalized_key = page_token
            .clone()
            .and_then(|s| if s.is_empty() { None } else { Some(s) });
        let page_key = (vts, normalized_key);

        //  Check Cache
        let receiver = {
            let mut data = self.inner.lock().await;
            data.vts_next_page_cache.remove(&page_key)
        }; // Lock is released here.

        if let Some(receiver) = receiver {
            // Await the pre-fetched result.
            let response = receiver.await??;
            // Handle spawning the *next* pre-fetch.
            self.handle_response_and_spawn_next(vts, response.next_page_token.clone())
                .await?;
            return Ok(response);
        }

        //  Cache miss, Make a new request
        let peer = {
            let mut data = self.inner.lock().await;
            data.peer_manager.get_peer_for_vts(vts).await?
        }; // Lock is released here.

        let request = proto::GetShardTransactionsRequest {
            shard_id: self.shard_id,
            height: self.height,
            trie_virtual_shard: vts as u32,
            page_token,
        };

        let response =
            Self::call_get_shard_transactions_with_retry(peer, request, &self.config).await?;

        // Handle spawning the next pre-fetch.
        self.handle_response_and_spawn_next(vts, response.next_page_token.clone())
            .await?;

        Ok(response)
    }
}
