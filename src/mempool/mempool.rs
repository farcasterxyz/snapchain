use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc, oneshot};

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
use tracing::{error, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub queue_size: u32,
    pub allow_unlimited_mempool_size: bool,
    pub capacity_per_shard: u64,
    pub rx_poll_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            queue_size: 500,
            allow_unlimited_mempool_size: false,
            capacity_per_shard: 1024,
            rx_poll_interval: Duration::from_millis(1),
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct MempoolKey {
    timestamp: u64,
    identity: String,
}

impl MempoolKey {
    pub fn new(timestamp: u64, identity: String) -> Self {
        MempoolKey {
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
            return MempoolKey::new(data.timestamp as u64, self.hex_hash());
        }
        todo!();
    }
}

impl proto::ValidatorMessage {
    pub fn mempool_key(&self) -> MempoolKey {
        if let Some(fname) = &self.fname_transfer {
            if let Some(proof) = &fname.proof {
                return MempoolKey::new(proof.timestamp, fname.id.to_string());
            }
        }
        if let Some(event) = &self.on_chain_event {
            return MempoolKey::new(
                event.block_timestamp,
                hex::encode(&event.transaction_hash) + &event.log_index.to_string(),
            );
        }
        todo!();
    }
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

pub struct Mempool {
    config: Config,
    shard_stores: HashMap<u32, Stores>,
    message_router: Box<dyn MessageRouter>,
    num_shards: u32,
    mempool_rx: mpsc::Receiver<MempoolMessage>,
    messages_request_rx: mpsc::Receiver<MempoolMessagesRequest>,
    messages: HashMap<u32, BTreeMap<MempoolKey, MempoolMessage>>,
    gossip_tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
    shard_decision_rx: broadcast::Receiver<ShardChunk>,
    statsd_client: StatsdClientWrapper,
}

impl Mempool {
    pub fn new(
        config: Config,
        mempool_rx: mpsc::Receiver<MempoolMessage>,
        messages_request_rx: mpsc::Receiver<MempoolMessagesRequest>,
        num_shards: u32,
        shard_stores: HashMap<u32, Stores>,
        gossip_tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
        shard_decision_rx: broadcast::Receiver<ShardChunk>,
        statsd_client: StatsdClientWrapper,
    ) -> Self {
        Mempool {
            config,
            shard_stores,
            num_shards,
            mempool_rx,
            message_router: Box::new(ShardRouter {}),
            messages: HashMap::new(),
            messages_request_rx,
            gossip_tx,
            shard_decision_rx,
            statsd_client,
        }
    }

    fn message_already_exists(&mut self, message: &MempoolMessage) -> bool {
        let fid = message.fid();
        let shard = self.message_router.route_message(fid, self.num_shards);
        let stores = self.shard_stores.get_mut(&shard);
        // Default to false in the orror paths
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
                            Err(err) => {
                                error!("Error retrieving set postfix: {}", err.to_string());
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
                            if self.message_is_valid(&next_message) {
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

    pub fn message_is_valid(&mut self, message: &MempoolMessage) -> bool {
        if self.message_already_exists(message) {
            return false;
        }

        return true;
    }

    async fn insert(&mut self, message: MempoolMessage) {
        // TODO(aditi): Maybe we don't need to run validations here?
        if self.message_is_valid(&message) {
            let fid = message.fid();
            let shard_id = self.message_router.route_message(fid, self.num_shards);
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

            match message {
                MempoolMessage::UserMessage(_) => {
                    let result = self
                        .gossip_tx
                        .send(GossipEvent::BroadcastMempoolMessage(message))
                        .await;

                    if let Err(e) = result {
                        warn!("Failed to gossip message {:?}", e);
                    }
                }
                _ => {}
            }
        } else {
            self.statsd_client.count("mempool.insert.failure", 1);
        }
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
                            match self.mempool_rx.try_recv() {
                                Ok(message) => {
                                    self.insert(message).await;
                                }, Err(mpsc::error::TryRecvError::Disconnected) => {
                                    panic!("Mempool tx is disconnected")
                                },
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
