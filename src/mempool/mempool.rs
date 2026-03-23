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
use tokio::time::Instant;

use crate::core::error::HubError;
use crate::core::util::FarcasterTime;
use crate::mempool::routing;
use crate::proto::{Block, FarcasterNetwork, Height, OnChainEventType, Transaction};
use crate::storage::store::block_engine::BlockStores;
use crate::{
    core::types::SnapchainValidatorContext,
    network::gossip::GossipEvent,
    proto::{self, ShardChunk},
    storage::{
        db::RocksDbTransactionBatch,
        store::{
            account::{
                get_message_by_key, make_message_primary_key, make_ts_hash, type_to_set_postfix,
            },
            mempool_poller::MempoolMessage,
            stores::Stores,
        },
    },
    utils::statsd_wrapper::StatsdClientWrapper,
};

use super::routing::{MessageRouter, ShardRouter};
use crate::version::version::{EngineVersion, ProtocolFeature};
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
    message_router: Box<dyn MessageRouter>,
    num_shards: u32,
}

impl RateLimits {
    pub fn new(
        shard_stores: HashMap<u32, Stores>,
        config: RateLimitsConfig,
        statsd_client: StatsdClientWrapper,
        num_shards: u32,
    ) -> Self {
        RateLimits {
            shard_stores,
            statsd_client,
            rate_limits_by_fid: CacheBuilder::new(config.max_capacity)
                .time_to_idle(config.time_to_idle)
                .eviction_policy(EvictionPolicy::lru())
                .build(),
            message_router: Box::new(ShardRouter {}),
            num_shards,
        }
    }

    fn invalidate_rate_limiter_for_fid(&mut self, fid: u64) {
        self.rate_limits_by_fid.invalidate(&fid);
    }

    fn get_rate_limiter_for_fid(&mut self, fid: u64) -> Option<Arc<DirectRateLimiter>> {
        self.rate_limits_by_fid.optionally_get_with(fid, || {
            let shard_id = self.message_router.route_fid(fid, self.num_shards);
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

    pub fn consume_for_fid(&mut self, fid: u64) -> bool {
        let rate_limiter = self.get_rate_limiter_for_fid(fid);
        self.statsd_client.gauge(
            "mempool.rate_limiter_entries",
            self.rate_limits_by_fid.entry_count(),
            vec![],
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

// Extension traits for proto types that depend on mempool types
pub trait MessageMempoolExt {
    fn mempool_key(&self) -> MempoolKey;
}

impl MessageMempoolExt for proto::Message {
    fn mempool_key(&self) -> MempoolKey {
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

pub trait ValidatorMessageMempoolExt {
    fn mempool_key(&self) -> MempoolKey;
}

impl ValidatorMessageMempoolExt for proto::ValidatorMessage {
    fn mempool_key(&self) -> MempoolKey {
        if let Some(onchain_event) = &self.on_chain_event {
            MempoolKey::new(
                MempoolMessageKind::ValidatorMessage,
                onchain_event.block_timestamp,
                hex::encode(&onchain_event.transaction_hash) + &onchain_event.log_index.to_string(),
            )
        } else if let Some(fname_transfer) = &self.fname_transfer {
            MempoolKey::new(
                MempoolMessageKind::ValidatorMessage,
                fname_transfer.proof.as_ref().unwrap().timestamp,
                fname_transfer.id.to_string(),
            )
        } else if let Some(block_event) = &self.block_event {
            MempoolKey::new(
                MempoolMessageKind::ValidatorMessage,
                block_event.block_timestamp(),
                block_event.seqnum().to_string(),
            )
        } else {
            MempoolKey::new(
                MempoolMessageKind::ValidatorMessage,
                0,
                "unknown".to_string(),
            )
        }
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
            MempoolMessage::OnchainEvent(event)
            | MempoolMessage::OnchainEventForMigration(event) => {
                let validator_message = proto::ValidatorMessage {
                    on_chain_event: Some(event.clone()),
                    fname_transfer: None,
                    block_event: None,
                };
                validator_message.mempool_key()
            }
            MempoolMessage::FnameTransfer(fname) => {
                let validator_message = proto::ValidatorMessage {
                    on_chain_event: None,
                    fname_transfer: Some(fname.clone()),
                    block_event: None,
                };
                validator_message.mempool_key()
            }
            MempoolMessage::BlockEvent {
                for_shard: _,
                message: block_event,
            } => {
                let validator_message = proto::ValidatorMessage {
                    on_chain_event: None,
                    fname_transfer: None,
                    block_event: Some(block_event.clone()),
                };
                validator_message.mempool_key()
            }
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
    block_stores: BlockStores,
    message_router: Box<dyn MessageRouter>,
    num_shards: u32,
    mempool_rx: mpsc::Receiver<MempoolRequest>,
    gossip_tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
    statsd_client: StatsdClientWrapper,
    network: FarcasterNetwork,
}

impl ReadNodeMempool {
    pub fn new(
        mempool_rx: mpsc::Receiver<MempoolRequest>,
        num_shards: u32,
        shard_stores: HashMap<u32, Stores>,
        block_stores: BlockStores,
        gossip_tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
        statsd_client: StatsdClientWrapper,
        network: FarcasterNetwork,
    ) -> Self {
        ReadNodeMempool {
            shard_stores,
            block_stores,
            num_shards,
            mempool_rx,
            message_router: Box::new(ShardRouter {}),
            gossip_tx,
            statsd_client,
            network,
        }
    }

    fn message_already_exists(&self, shard: u32, message: &MempoolMessage) -> bool {
        let fid = message.fid();

        let db = if shard == 0 {
            Some(&self.block_stores.db)
        } else {
            self.shard_stores.get(&shard).map(|stores| &stores.db)
        };

        // Default to false in the error paths
        match db {
            None => {
                error!("Error finding store for shard: {}", shard);
                false
            }
            Some(db) => match message {
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
                                    db,
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
                MempoolMessage::OnchainEvent(_)
                | MempoolMessage::OnchainEventForMigration(_)
                | MempoolMessage::FnameTransfer(_)
                | MempoolMessage::BlockEvent { .. } => {
                    // Don't do duplicate checks for validator messages. They are infrequent, and engine can handle duplicates.
                    false
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

    fn route_mempool_message(&self, message: &MempoolMessage) -> Vec<u32> {
        let fid_shard = self
            .message_router
            .route_fid(message.fid(), self.num_shards);

        // Fname transfers are mirrored to both the sender and receiver shard.
        match message {
            MempoolMessage::FnameTransfer(_fname_transfer) => {
                let version = EngineVersion::current(self.network);
                // Send the username transfer to all other shards, transfers from a->b->c are
                // correctly tracked. Due to current limitations of the engine, if we transfer from
                // shard 1 to shard 2, and then transfer within shard 2, we will keep the transfer
                // around forever on shard 1. See test_fname_transfer for an example.
                if version.is_enabled(ProtocolFeature::UsernameShardRoutingFix) {
                    (1..self.num_shards + 1).collect()
                } else {
                    vec![fid_shard]
                }
            }
            MempoolMessage::OnchainEventForMigration(_) => {
                // TODO(aditi): Remove this codepath after migrating onchain events to shard 0
                vec![0]
            }
            MempoolMessage::OnchainEvent(_) => {
                // Onchain events need to get to shard 0 so that we can support other messages (lend storage) in shard 0.
                vec![0, fid_shard]
            }
            MempoolMessage::BlockEvent {
                for_shard,
                message: _,
            } => {
                vec![*for_shard]
            }
            MempoolMessage::UserMessage(message) => {
                vec![routing::route_message(
                    &self.message_router,
                    message,
                    self.num_shards,
                )]
            }
        }
    }

    fn message_is_valid(&mut self, shard: u32, message: &MempoolMessage) -> Result<(), HubError> {
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
                        .count("read_mempool.messages_received", 1, vec![]);
                    let results: Vec<Result<(), HubError>> = self
                        .route_mempool_message(&message)
                        .iter()
                        .map(|shard| self.message_is_valid(*shard, &message))
                        .collect();
                    // If the message is valid on any shard then keep it. Else all shards return errors and just take the first error.
                    let result = if results.iter().any(|res| res.is_ok()) {
                        Ok(())
                    } else {
                        results[0].clone()
                    };
                    if result.is_ok() {
                        self.gossip_message(message, source).await;
                        self.statsd_client
                            .count("read_mempool.messages_published", 1, vec![]);
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
    block_decision_rx: broadcast::Receiver<Block>,
    statsd_client: StatsdClientWrapper,
    read_node_mempool: ReadNodeMempool,
    rate_limits: Option<RateLimits>,
}

impl Mempool {
    pub fn new(
        config: Config,
        network: FarcasterNetwork,
        mempool_rx: mpsc::Receiver<MempoolRequest>,
        messages_request_rx: mpsc::Receiver<MempoolMessagesRequest>,
        num_shards: u32,
        shard_stores: HashMap<u32, Stores>,
        block_stores: BlockStores,
        gossip_tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
        shard_decision_rx: broadcast::Receiver<ShardChunk>,
        block_decision_rx: broadcast::Receiver<Block>,
        statsd_client: StatsdClientWrapper,
    ) -> Self {
        Mempool {
            messages: HashMap::new(),
            messages_request_rx,
            shard_decision_rx,
            block_decision_rx,
            rate_limits: if config.enable_rate_limits {
                Some(RateLimits::new(
                    shard_stores.clone(),
                    RateLimitsConfig::default(),
                    statsd_client.clone(),
                    num_shards,
                ))
            } else {
                None
            },
            config,
            read_node_mempool: ReadNodeMempool::new(
                mempool_rx,
                num_shards,
                shard_stores,
                block_stores,
                gossip_tx,
                statsd_client.clone(),
                network,
            ),
            statsd_client,
        }
    }

    fn message_exceeds_rate_limits(&mut self, message: &MempoolMessage) -> bool {
        match message {
            MempoolMessage::UserMessage(message) => {
                if let Some(rate_limits) = &mut self.rate_limits {
                    !rate_limits.consume_for_fid(message.fid())
                } else {
                    false
                }
            }
            MempoolMessage::OnchainEvent(_)
            | MempoolMessage::OnchainEventForMigration(_)
            | MempoolMessage::FnameTransfer(_)
            | MempoolMessage::BlockEvent { .. } => false,
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
                            let result = self.message_is_valid(request.shard_id, &next_message);
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

    pub fn message_is_valid(
        &mut self,
        shard: u32,
        message: &MempoolMessage,
    ) -> Result<(), HubError> {
        // Check for block events that have already been merged
        if let MempoolMessage::BlockEvent { message, for_shard } = message {
            if *for_shard == 0 {
                return Err(HubError::invalid_internal_state(
                    "block event cannot be submitted to shard 0",
                ));
            }
            let stores = self.read_node_mempool.shard_stores.get(&for_shard).unwrap();
            if let Ok(max_seqnum) = stores.block_event_store.max_seqnum() {
                if message.seqnum() <= max_seqnum {
                    return Err(HubError::duplicate("block event has already been merged"));
                }
            }
        }

        if self.message_already_exists(shard, message) {
            return Err(HubError::duplicate("message has already been merged"));
        }
        if self.message_exceeds_rate_limits(message) {
            self.statsd_client
                .count_with_shard(shard, "mempool.rate_limit_hit", 1, vec![]);
            return Err(HubError::rate_limited(&format!(
                "rate limit exceeded for FID {}",
                message.fid()
            )));
        }
        Ok(())
    }

    async fn insert(
        &mut self,
        message: MempoolMessage,
        source: MempoolSource,
    ) -> Result<(), HubError> {
        let shard_ids = self.read_node_mempool.route_mempool_message(&message);

        let mut errors = vec![];
        for shard_id in shard_ids {
            if let Err(err) = self
                .insert_into_shard(shard_id, message.clone(), source.clone())
                .await
            {
                error!(
                    shard_id = shard_id.to_string(),
                    "Unable to insert message into mempool for shard: {}",
                    err.to_string()
                );
                errors.push(err)
            }
        }

        if !errors.is_empty() {
            // Just pick the first error because we need to return some error
            return Err(errors[0].clone());
        }

        Ok(())
    }

    async fn insert_into_shard(
        &mut self,
        shard_id: u32,
        message: MempoolMessage,
        source: MempoolSource,
    ) -> Result<(), HubError> {
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
        let result = self.message_is_valid(shard_id, &message);
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
                .count_with_shard(shard_id, "mempool.insert.success", 1, vec![]);

            self.read_node_mempool.gossip_message(message, source).await;
        } else {
            self.statsd_client
                .count("mempool.insert.failure", 1, vec![]);
        }

        result
    }

    fn remove_committed_txns(&mut self, height: Height, transactions: &Vec<Transaction>) {
        if let Some(mempool) = self.messages.get_mut(&height.shard_index) {
            for transaction in transactions {
                for user_message in &transaction.user_messages {
                    mempool.remove(&user_message.mempool_key());
                    self.statsd_client.count_with_shard(
                        height.shard_index,
                        "mempool.remove.success",
                        1,
                        vec![],
                    );
                }
                for system_message in &transaction.system_messages {
                    mempool.remove(&system_message.mempool_key());
                    if let Some(onchain_event) = &system_message.on_chain_event {
                        if onchain_event.r#type() == OnChainEventType::EventTypeStorageRent {
                            // If the user buys more storage, we should bump their rate limit
                            if let Some(rate_limits) = &mut self.rate_limits {
                                rate_limits.invalidate_rate_limiter_for_fid(onchain_event.fid);
                            }
                        }
                    }
                    self.statsd_client.count_with_shard(
                        height.shard_index,
                        "mempool.remove.success",
                        1,
                        vec![],
                    );
                }
            }
        }
    }

    pub async fn run(&mut self) {
        let mut poll_interval = tokio::time::interval(self.config.rx_poll_interval);
        let mut last_inbound_message_poll_time = Instant::now();
        loop {
            tokio::select! {
                biased;

                message_request = self.messages_request_rx.recv() => {
                    if let Some(messages_request) = message_request {
                        self.pull_messages(messages_request).await
                    }
                }
                block = self.block_decision_rx.recv() => {
                    match block  {
                        Ok(block) => {
                            let header = block.header.expect("Expects block to have a header");
                            let height = header.height.expect("Expects header to have a height");
                            self.remove_committed_txns(height, &block.transactions)

                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            panic!("Block decision tx is closed.");
                        },
                        Err(broadcast::error::RecvError::Lagged(count)) => {
                            error!(lag = count, "Block decision rx is lagged");
                        }
                    }
                }
                chunk = self.shard_decision_rx.recv() => {
                    match chunk {
                        Ok(chunk) => {
                            let header = chunk.header.expect("Expects chunk to have a header");
                            let height = header.height.expect("Expects header to have a height");
                            self.remove_committed_txns(height, &chunk.transactions)
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
                    let now = Instant::now();
                    self.statsd_client.gauge("mempool.queue_size", self.read_node_mempool.mempool_rx.len() as u64, vec![]);
                    self.statsd_client.time("mempool.inbound_message_poll_interval_ms", now.duration_since(last_inbound_message_poll_time).as_millis() as u64);
                    last_inbound_message_poll_time = now;
                    // We want to pull in multiple messages per poll so that throughput is not blocked on the polling frequency. The number of messages we pull should be fixed and relatively small so that the mempool isn't always stuck here.
                    for _ in 0..256 {
                        if self.config.allow_unlimited_mempool_size || (self.messages.values().map(|m| m.len()).sum::<usize>() as u64) < self.config.capacity_per_shard {
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
