use governor::clock::QuantaClock;
use governor::state::{InMemoryState, NotKeyed};
use moka::policy::EvictionPolicy;
use serde::{Deserialize, Serialize};
use std::cmp::PartialEq;
use std::sync::Arc;
use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::core::error::HubError;
use crate::core::util::FarcasterTime;
use crate::proto::OnChainEventType;
use crate::{
    core::types::SnapchainValidatorContext,
    network::gossip::GossipEvent,
    proto::{self, ShardChunk},
    storage::{
        db::RocksDbTransactionBatch,
        store::{
            account::{
                get_message_by_key, make_message_primary_key, make_ts_hash, type_to_set_postfix,
                UserDataStore,
            },
            engine::MempoolMessage,
            stores::Stores,
        },
    },
    utils::statsd_wrapper::StatsdClientWrapper,
};

use super::routing::{MessageRouter, ShardRouter};
use governor::{Quota, RateLimiter};
use moka::sync::{Cache, CacheBuilder};
use std::num::NonZeroU32;
use tracing::{error, warn};

type DirectRateLimiter = RateLimiter<NotKeyed, InMemoryState, QuantaClock>;

pub struct RateLimitsConfig {
    pub time_to_idle: Duration,
    pub max_capacity: u64,
}

impl Default for RateLimitsConfig {
    fn default() -> Self {
        Self {
            time_to_idle: Duration::from_secs(60 * 2 * 2),
            // Make time to idle 2x the rate limit window so it's ok if out of sync
            max_capacity: 1_000_000,
        }
    }
}

pub struct RateLimits {
    shard_stores: HashMap<u32, Stores>,
    rate_limits_by_fid: Cache<u64, Arc<DirectRateLimiter>>,
    statsd_client: StatsdClientWrapper,
}

impl RateLimits {
    pub fn new(
        shard_stores: HashMap<u32, Stores>,
        config: RateLimitsConfig,
        statsd_client: StatsdClientWrapper,
    ) -> Self {
        RateLimits {
            shard_stores,
            statsd_client,
            rate_limits_by_fid: CacheBuilder::new(config.max_capacity)
                .time_to_idle(config.time_to_idle)
                .eviction_policy(EvictionPolicy::lru())
                .build(),
        }
    }

    fn invalidate_rate_limiter_for_fid(&mut self, fid: u64) {
        self.rate_limits_by_fid.invalidate(&fid);
    }

    fn get_rate_limiter_for_fid(
        &mut self,
        shard_id: u32,
        fid: u64,
    ) -> Option<Arc<DirectRateLimiter>> {
        self.rate_limits_by_fid.optionally_get_with(fid, || {
            let stores = self.shard_stores.get(&shard_id).unwrap();
            let storage_limits = stores.get_storage_limits(fid).unwrap();
            let storage_allowance: u32 = storage_limits
                .limits
                .iter()
                .map(|limits| limits.limit as u32)
                .sum();
            if storage_allowance == 0 {
                None
            } else {
                // If we update the quota, we should update [time_to_idle] accordingly
                Some(Arc::new(RateLimiter::direct(Quota::per_hour(
                    NonZeroU32::new(100.max(storage_allowance / 10)).unwrap(),
                ))))
            }
        })
    }

    pub fn consume_for_fid(&mut self, shard_id: u32, fid: u64) -> bool {
        let rate_limiter = self.get_rate_limiter_for_fid(shard_id, fid);
        self.statsd_client.gauge(
            "mempool.rate_limiter_entries",
            self.rate_limits_by_fid.entry_count(),
        );
        match rate_limiter {
            Some(rate_limiter) => rate_limiter.check().is_ok(),
            None => false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub queue_size: u32,
    pub allow_unlimited_mempool_size: bool,
    pub capacity_per_shard: u64,
    pub rx_poll_interval: Duration,
    pub enable_rate_limits: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            queue_size: 5000,
            allow_unlimited_mempool_size: false,
            capacity_per_shard: 1_000_000,
            rx_poll_interval: Duration::from_millis(1),
            enable_rate_limits: false,
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum MempoolMessageKind {
    ValidatorMessage = 1,
    UserMessage = 2,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct MempoolKey {
    message_kind: MempoolMessageKind,
    timestamp: u64, // in unix seconds
    identity: String,
}

impl MempoolKey {
    pub fn new(message_kind: MempoolMessageKind, timestamp: u64, identity: String) -> Self {
        MempoolKey {
            message_kind,
            timestamp,
            identity,
        }
    }

    pub fn identity(self) -> String {
        self.identity
    }
}

impl proto::Message {
    pub fn mempool_key(&self) -> MempoolKey {
        if let Some(data) = &self.data {
            // TODO: Consider revisiting choice of timestamp here as backdated messages currently are prioritized.
            return MempoolKey::new(
                MempoolMessageKind::UserMessage,
                FarcasterTime::new(data.timestamp as u64).to_unix_seconds(),
                self.hex_hash(),
            );
        }
        todo!();
    }
}

impl proto::ValidatorMessage {
    pub fn mempool_key(&self) -> MempoolKey {
        if let Some(fname) = &self.fname_transfer {
            if let Some(proof) = &fname.proof {
                return MempoolKey::new(
                    MempoolMessageKind::ValidatorMessage,
                    proof.timestamp,
                    fname.id.to_string(),
                );
            }
        }
        if let Some(event) = &self.on_chain_event {
            return MempoolKey::new(
                MempoolMessageKind::ValidatorMessage,
                event.block_timestamp,
                hex::encode(&event.transaction_hash) + &event.log_index.to_string(),
            );
        }
        todo!();
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MempoolSource {
    Gossip,
    RPC,
    Local,
}

#[derive(Debug)]
pub enum MempoolRequest {
    AddMessage(
        MempoolMessage,
        MempoolSource,
        Option<oneshot::Sender<Result<(), HubError>>>,
    ),
    GetSize(oneshot::Sender<HashMap<u32, u64>>),
}

impl MempoolMessage {
    pub fn mempool_key(&self) -> MempoolKey {
        match self {
            MempoolMessage::UserMessage(msg) => msg.mempool_key(),
            MempoolMessage::ValidatorMessage(msg) => msg.mempool_key(),
        }
    }
}

pub struct MempoolMessagesRequest {
    pub shard_id: u32,
    pub message_tx: oneshot::Sender<Vec<MempoolMessage>>,
    pub max_messages_per_block: u32,
}

pub struct ReadNodeMempool {
    shard_stores: HashMap<u32, Stores>,
    message_router: Box<dyn MessageRouter>,
    num_shards: u32,
    mempool_rx: mpsc::Receiver<MempoolRequest>,
    gossip_tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
    statsd_client: StatsdClientWrapper,
}

impl ReadNodeMempool {
    pub fn new(
        mempool_rx: mpsc::Receiver<MempoolRequest>,
        num_shards: u32,
        shard_stores: HashMap<u32, Stores>,
        gossip_tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
        statsd_client: StatsdClientWrapper,
    ) -> Self {
        ReadNodeMempool {
            shard_stores,
            num_shards,
            mempool_rx,
            message_router: Box::new(ShardRouter {}),
            gossip_tx,
            statsd_client,
        }
    }

    fn message_already_exists(&self, shard: u32, message: &MempoolMessage) -> bool {
        let fid = message.fid();

        let stores = self.shard_stores.get(&shard);
        // Default to false in the error paths
        match stores {
            None => {
                error!("Error finding store for shard: {}", shard);
                false
            }
            Some(stores) => match message {
                MempoolMessage::UserMessage(message) => match &message.data {
                    None => false,
                    Some(message_data) => {
                        let ts_hash = make_ts_hash(message_data.timestamp, &message.hash).unwrap();
                        match type_to_set_postfix(message_data.r#type()) {
                            Err(_) => {
                                // We hit this for link compact messages and it's expected. Return [false] so the message isn't filtered out
                                false
                            }
                            Ok(set_postfix) => {
                                let primary_key = make_message_primary_key(
                                    fid,
                                    set_postfix as u8,
                                    Some(&ts_hash),
                                );
                                let existing_message = get_message_by_key(
                                    &stores.db,
                                    &mut RocksDbTransactionBatch::new(),
                                    &primary_key,
                                );
                                match existing_message {
                                    Ok(Some(_)) => true,
                                    Err(_) | Ok(None) => false,
                                }
                            }
                        }
                    }
                },
                MempoolMessage::ValidatorMessage(message) => {
                    if let Some(onchain_event) = &message.on_chain_event {
                        match stores.onchain_event_store.exists(&onchain_event) {
                            Err(_) => return false,
                            Ok(exists) => return exists,
                        }
                    }

                    if let Some(fname_transfer) = &message.fname_transfer {
                        match &fname_transfer.proof {
                            None => return false,
                            Some(proof) => {
                                let username_proof = UserDataStore::get_username_proof(
                                    &stores.user_data_store,
                                    &mut RocksDbTransactionBatch::new(),
                                    &proof.name,
                                );
                                match username_proof {
                                    Err(_) | Ok(None) => return false,
                                    Ok(Some(_)) => return true,
                                }
                            }
                        }
                    }
                    return false;
                }
            },
        }
    }

    async fn gossip_message(&self, message: MempoolMessage, source: MempoolSource) {
        match message {
            MempoolMessage::UserMessage(_) => {
                // Don't re-broadcast messages that were received from gossip, other nodes will already have them.
                if source != MempoolSource::Gossip {
                    let result = self
                        .gossip_tx
                        .send(GossipEvent::BroadcastMempoolMessage(message))
                        .await;

                    if let Err(e) = result {
                        warn!("Failed to gossip message {:?}", e);
                    }
                }
            }
            _ => {}
        }
    }

    fn message_is_valid(&mut self, message: &MempoolMessage) -> Result<(), HubError> {
        let fid = message.fid();
        let shard = self.message_router.route_fid(fid, self.num_shards);
        if self.message_already_exists(shard, message) {
            return Err(HubError::duplicate("message has already been merged"));
        }
        Ok(())
    }

    pub async fn run(&mut self) {
        while let Some(message_request) = self.mempool_rx.recv().await {
            match message_request {
                MempoolRequest::AddMessage(message, source, reply_to) => {
                    self.statsd_client
                        .count("read_mempool.messages_received", 1);
                    let result = self.message_is_valid(&message);
                    if result.is_ok() {
                        self.gossip_message(message, source).await;
                        self.statsd_client
                            .count("read_mempool.messages_published", 1);
                    }
                    if let Some(sender) = reply_to {
                        if let Err(_) = sender.send(result) {
                            error!("Unable to reply to add message request from mempool");
                        }
                    }
                }
                MempoolRequest::GetSize(reply_to) => {
                    // Read nodes don't have a local mempool, so the size is always 0
                    if let Err(_) = reply_to.send(HashMap::new()) {
                        error!("Unable to reply to message size request from mempool");
                    }
                }
            }
        }
        panic!("Mempool has exited");
    }
}

pub struct Mempool {
    config: Config,
    messages_request_rx: mpsc::Receiver<MempoolMessagesRequest>,
    messages: HashMap<u32, BTreeMap<MempoolKey, MempoolMessage>>,
    shard_decision_rx: broadcast::Receiver<ShardChunk>,
    statsd_client: StatsdClientWrapper,
    read_node_mempool: ReadNodeMempool,
    rate_limits: Option<RateLimits>,
}

impl Mempool {
    pub fn new(
        config: Config,
        mempool_rx: mpsc::Receiver<MempoolRequest>,
        messages_request_rx: mpsc::Receiver<MempoolMessagesRequest>,
        num_shards: u32,
        shard_stores: HashMap<u32, Stores>,
        gossip_tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
        shard_decision_rx: broadcast::Receiver<ShardChunk>,
        statsd_client: StatsdClientWrapper,
    ) -> Self {
        Mempool {
            messages: HashMap::new(),
            messages_request_rx,
            shard_decision_rx,
            rate_limits: if config.enable_rate_limits {
                Some(RateLimits::new(
                    shard_stores.clone(),
                    RateLimitsConfig::default(),
                    statsd_client.clone(),
                ))
            } else {
                None
            },
            config,
            read_node_mempool: ReadNodeMempool::new(
                mempool_rx,
                num_shards,
                shard_stores,
                gossip_tx,
                statsd_client.clone(),
            ),
            statsd_client,
        }
    }

    fn message_exceeds_rate_limits(&mut self, shard_id: u32, message: &MempoolMessage) -> bool {
        match message {
            MempoolMessage::UserMessage(message) => {
                if let Some(rate_limits) = &mut self.rate_limits {
                    !rate_limits.consume_for_fid(shard_id, message.fid())
                } else {
                    false
                }
            }
            MempoolMessage::ValidatorMessage(_) => false,
        }
    }

    fn message_already_exists(&mut self, shard: u32, message: &MempoolMessage) -> bool {
        self.read_node_mempool
            .message_already_exists(shard, message)
    }

    async fn pull_messages(&mut self, request: MempoolMessagesRequest) {
        let mut messages = vec![];
        while messages.len() < request.max_messages_per_block as usize {
            let shard_messages = self.messages.get_mut(&request.shard_id);
            match shard_messages {
                None => break,
                Some(shard_messages) => {
                    match shard_messages.pop_first() {
                        None => break,
                        Some((_, next_message)) => {
                            let result = self.message_is_valid(&next_message);
                            if result.is_ok() {
                                messages.push(next_message);
                            }
                        }
                    };
                }
            }
        }

        if let Err(_) = request.message_tx.send(messages) {
            error!("Unable to send message from mempool");
        }
    }

    pub fn message_is_valid(&mut self, message: &MempoolMessage) -> Result<(), HubError> {
        let shard = self
            .read_node_mempool
            .message_router
            .route_fid(message.fid(), self.read_node_mempool.num_shards);

        if self.message_already_exists(shard, message) {
            return Err(HubError::duplicate("message has already been merged"));
        }
        if self.message_exceeds_rate_limits(shard, message) {
            self.statsd_client
                .count_with_shard(shard, "mempool.rate_limit_hit", 1);
            return Err(HubError::rate_limited("Rate limit exceeded"));
        }
        Ok(())
    }

    async fn insert(
        &mut self,
        message: MempoolMessage,
        source: MempoolSource,
    ) -> Result<(), HubError> {
        let fid = message.fid();
        let shard_id = self
            .read_node_mempool
            .message_router
            .route_fid(fid, self.read_node_mempool.num_shards);

        match self.messages.get_mut(&shard_id) {
            Some(shard_messages) => {
                if shard_messages.contains_key(&message.mempool_key()) {
                    // Exit early if the message already exists in the mempool
                    return Err(HubError::duplicate("message already in the mempool"));
                }
            }
            None => {}
        }

        // TODO(aditi): Maybe we don't need to run validations here?
        let result = self.message_is_valid(&message);
        if result.is_ok() {
            match self.messages.get_mut(&shard_id) {
                None => {
                    let mut messages = BTreeMap::new();
                    messages.insert(message.mempool_key(), message.clone());
                    self.messages.insert(shard_id, messages);
                    self.statsd_client
                        .gauge_with_shard(shard_id, "mempool.size", 1);
                }
                Some(messages) => {
                    messages.insert(message.mempool_key(), message.clone());
                    self.statsd_client.gauge_with_shard(
                        shard_id,
                        "mempool.size",
                        messages.len() as u64,
                    );
                }
            }

            self.statsd_client
                .count_with_shard(shard_id, "mempool.insert.success", 1);

            self.read_node_mempool.gossip_message(message, source).await;
        } else {
            self.statsd_client.count("mempool.insert.failure", 1);
        }

        result
    }

    pub async fn run(&mut self) {
        let mut poll_interval = tokio::time::interval(self.config.rx_poll_interval);
        loop {
            tokio::select! {
                biased;

                message_request = self.messages_request_rx.recv() => {
                    if let Some(messages_request) = message_request {
                        self.pull_messages(messages_request).await
                    }
                }
                chunk = self.shard_decision_rx.recv() => {
                    match chunk {
                        Ok(chunk) => {
                            let header = chunk.header.expect("Expects chunk to have a header");
                            let height = header.height.expect("Expects header to have a height");
                            if let Some(mempool) = self.messages.get_mut(&height.shard_index) {
                                for transaction in chunk.transactions {
                                    for user_message in transaction.user_messages {
                                        mempool.remove(&user_message.mempool_key());
                                        self.statsd_client.count_with_shard(height.shard_index, "mempool.remove.success", 1);
                                    }
                                    for system_message in transaction.system_messages {
                                        mempool.remove(&system_message.mempool_key());
                                        if let Some(onchain_event) = system_message.on_chain_event
                                        {
                                            if onchain_event.r#type() == OnChainEventType::EventTypeStorageRent{
                                                // If the user buys more storage, we should bump their rate limit
                                                if let Some(rate_limits) = &mut self.rate_limits {
                                                    rate_limits.invalidate_rate_limiter_for_fid(onchain_event.fid);
                                                }

                                            }
                                        }
                                       self.statsd_client.count_with_shard(height.shard_index, "mempool.remove.success", 1);
                                    }
                                }
                            }
                        },
                        Err(broadcast::error::RecvError::Closed) => {
                            panic!("Shard decision tx is closed.");
                        },
                        Err(broadcast::error::RecvError::Lagged(count)) => {
                            error!(lag = count, "Shard decision rx is lagged");
                        }
                    }
                }
                _ = poll_interval.tick() => {
                    // We want to pull in multiple messages per poll so that throughput is not blocked on the polling frequency. The number of messages we pull should be fixed and relatively small so that the mempool isn't always stuck here.
                    for _ in 0..256 {
                        if self.config.allow_unlimited_mempool_size || (self.messages.len() as u64) < self.config.capacity_per_shard {
                            match self.read_node_mempool.mempool_rx.try_recv() {
                                Ok(MempoolRequest::AddMessage(message, source, reply_to)) => {
                                    let result = self.insert(message, source).await;
                                    if let Some(sender) = reply_to {
                                        if let Err(_) = sender.send(result) {
                                            error!("Unable to reply to add message request from mempool");
                                        }
                                    }
                                }
                                Ok(MempoolRequest::GetSize(reply_to)) => {
                                    let mut sizes = HashMap::new();
                                    for (shard_id, messages) in &self.messages {
                                        sizes.insert(*shard_id, messages.len() as u64);
                                    }
                                    if let Err(_) = reply_to.send(sizes) {
                                        error!("Unable to reply to message size request from mempool");
                                    }
                                }
                                Err(mpsc::error::TryRecvError::Disconnected) => {
                                    panic!("Mempool tx is disconnected")
                                }
                                Err(mpsc::error::TryRecvError::Empty) => {
                                    break;
                                },

                            }
                        } else {
                            break;
                        }
                    }
                }
            }
        }
    }
}
