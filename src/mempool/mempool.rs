use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::{core::types::SnapchainValidatorContext, network::gossip::GossipEvent, proto::ShardChunk, storage::{
    db::RocksDbTransactionBatch,
    store::{
        account::{
            get_message_by_key, make_message_primary_key, make_ts_hash, type_to_set_postfix,
            UserDataStore,
        },
        engine::MempoolMessage,
        stores::Stores,
    },
}};

use super::routing::{MessageRouter, ShardRouter};
use tracing::{error, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub queue_size: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self { queue_size: 500 }
    }
}

pub struct MempoolMessagesRequest {
    pub shard_id: u32,
    pub message_tx: oneshot::Sender<Vec<MempoolMessage>>,
    pub max_messages_per_block: u32,
}

pub struct Mempool {
    capacity_per_shard: usize,
    shard_stores: HashMap<u32, Stores>,
    message_router: Box<dyn MessageRouter>,
    num_shards: u32,
    mempool_rx: mpsc::Receiver<MempoolMessage>,
    messages_request_rx: mpsc::Receiver<MempoolMessagesRequest>,
    messages: HashMap<u32, BTreeMap<String, MempoolMessage>>,
    gossip_tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
    shard_decision_rx: broadcast::Receiver<ShardChunk>,
}

impl Mempool {
    pub fn new(
        capacity_per_shard: usize,
        mempool_rx: mpsc::Receiver<MempoolMessage>,
        messages_request_rx: mpsc::Receiver<MempoolMessagesRequest>,
        num_shards: u32,
        shard_stores: HashMap<u32, Stores>,
        gossip_tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
        shard_decision_rx: broadcast::Receiver<ShardChunk>,
    ) -> Self {
        Mempool {
            capacity_per_shard,
            shard_stores,
            num_shards,
            mempool_rx,
            message_router: Box::new(ShardRouter {}),
            messages: HashMap::new(),
            messages_request_rx,
            gossip_tx,
            shard_decision_rx,
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
                        let set_postfix = type_to_set_postfix(message_data.r#type());
                        let primary_key =
                            make_message_primary_key(fid, set_postfix as u8, Some(&ts_hash));
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
            // TODO(aditi): We need a size limit on the mempool and we need to figure out what to do if it's exceeded
            match self.messages.get_mut(&shard_id) {
                None => {
                    let mut messages = BTreeMap::new();
                    messages.insert(message.hex_hash(), message.clone());
                    self.messages.insert(shard_id, messages);
                }
                Some(messages) => {
                    // For now, mempool messages are dropped here if the mempool is full.
                    if messages.len() < self.capacity_per_shard {
                        messages.insert(message.hex_hash(), message.clone());
                    }
                }
            }

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
        }
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                biased;

                message_request = self.messages_request_rx.recv() => {
                    if let Some(messages_request) = message_request {
                        self.pull_messages(messages_request).await
                    }
                }
                chunk = self.shard_decision_rx.recv() => {
                    if let Ok(chunk) = chunk {
                        let header = chunk.header.expect("Expects chunk to have a header");
                        let height = header.height.expect("Expects header to have a height");
                        if let Some(mempool) = self.messages.get_mut(&height.shard_index) {
                            for transaction in chunk.transactions {
                                for user_message in transaction.user_messages {
                                    mempool.remove(&user_message.hex_hash());
                                }
                                for system_message in transaction.system_messages {
                                    mempool.remove(&system_message.hex_hash());
                                }
                            }
                        }
                    }
                }
                message = self.mempool_rx.recv() => {
                    if let Some(message) = message {
                        self.insert(message).await;
                    }
                }
            }
        }
    }
}
