use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};

use crate::storage::{
    store::{engine::MempoolMessage, stores::Stores},
    trie::merkle_trie::{self, TrieKey},
};

use super::routing::{MessageRouter, ShardRouter};
use tracing::error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub queue_size: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self { queue_size: 500 }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct MempoolKey {
    inserted_at: Instant,
}

pub struct MempoolMessagesRequest {
    pub shard_id: u32,
    pub message_tx: oneshot::Sender<Vec<MempoolMessage>>,
    pub max_messages_per_block: u32,
}

pub struct Mempool {
    shard_stores: HashMap<u32, Stores>,
    message_router: Box<dyn MessageRouter>,
    num_shards: u32,
    mempool_rx: mpsc::Receiver<MempoolMessage>,
    messages_request_rx: mpsc::Receiver<MempoolMessagesRequest>,
    messages: HashMap<u32, BTreeMap<MempoolKey, MempoolMessage>>,
}

impl Mempool {
    pub fn new(
        mempool_rx: mpsc::Receiver<MempoolMessage>,
        messages_request_rx: mpsc::Receiver<MempoolMessagesRequest>,
        num_shards: u32,
        shard_stores: HashMap<u32, Stores>,
    ) -> Self {
        Mempool {
            shard_stores,
            num_shards,
            mempool_rx,
            message_router: Box::new(ShardRouter {}),
            messages: HashMap::new(),
            messages_request_rx,
        }
    }

    fn message_exists_in_trie(&mut self, fid: u64, trie_key: Vec<u8>) -> bool {
        let shard = self.message_router.route_message(fid, self.num_shards);
        let stores = self.shard_stores.get_mut(&shard);
        match stores {
            None => {
                error!("Error finding store for shard: {}", shard);
                false
            }
            Some(stores) => {
                // TODO(aditi): The engine reloads its ref to the trie on commit but we maintain a separate ref to the trie here.
                stores.trie.reload(&stores.db).unwrap();
                match stores.trie.exists(
                    &merkle_trie::Context::new(),
                    &stores.db,
                    trie_key.as_ref(),
                ) {
                    Err(err) => {
                        error!("Error finding key in trie: {}", err);
                        false
                    }
                    Ok(exists) => exists,
                }
            }
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

    fn get_trie_key(message: &MempoolMessage) -> Option<Vec<u8>> {
        match message {
            MempoolMessage::UserMessage(message) => return Some(TrieKey::for_message(message)),
            MempoolMessage::ValidatorMessage(validator_message) => {
                if let Some(onchain_event) = &validator_message.on_chain_event {
                    return Some(TrieKey::for_onchain_event(&onchain_event));
                }

                if let Some(fname_transfer) = &validator_message.fname_transfer {
                    if let Some(proof) = &fname_transfer.proof {
                        let name = String::from_utf8(proof.name.clone()).unwrap();
                        return Some(TrieKey::for_fname(fname_transfer.id, &name));
                    }
                }

                return None;
            }
        }
    }

    fn is_message_already_merged(&mut self, message: &MempoolMessage) -> bool {
        let fid = message.fid();
        let trie_key = Self::get_trie_key(&message);
        match trie_key {
            Some(trie_key) => self.message_exists_in_trie(fid, trie_key),
            None => false,
        }
    }

    pub fn message_is_valid(&mut self, message: &MempoolMessage) -> bool {
        if self.is_message_already_merged(message) {
            return false;
        }

        return true;
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
                message = self.mempool_rx.recv() => {
                    if let Some(message) = message {
                        // TODO(aditi): Maybe we don't need to run validations here?
                        if self.message_is_valid(&message) {
                            let fid = message.fid();
                            let shard_id = self.message_router.route_message(fid, self.num_shards);
                            // TODO(aditi): We need a size limit on the mempool and we need to figure out what to do if it's exceeded
                            match self.messages.get_mut(&shard_id) {
                                None => {
                                    let mut messages = BTreeMap::new();
                                    messages.insert(MempoolKey { inserted_at: Instant::now()}, message.clone());
                                    self.messages.insert(shard_id, messages);
                                }
                                Some(messages) => {
                                    messages.insert(MempoolKey { inserted_at: Instant::now()}, message.clone());
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
