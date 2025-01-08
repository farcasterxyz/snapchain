use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::storage::{
    store::{
        engine::{MempoolMessage, Senders},
        stores::Stores,
    },
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

pub struct Mempool {
    shard_senders: HashMap<u32, Senders>,
    shard_stores: HashMap<u32, Stores>,
    message_router: Box<dyn MessageRouter>,
    num_shards: u32,
    mempool_rx: mpsc::Receiver<MempoolMessage>,
}

impl Mempool {
    pub fn new(
        mempool_rx: mpsc::Receiver<MempoolMessage>,
        num_shards: u32,
        shard_senders: HashMap<u32, Senders>,
        shard_stores: HashMap<u32, Stores>,
    ) -> Self {
        Mempool {
            shard_senders,
            shard_stores,
            num_shards,
            mempool_rx,
            message_router: Box::new(ShardRouter {}),
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

    fn is_message_already_merged(&mut self, message: &MempoolMessage) -> bool {
        let fid = message.fid();
        match message {
            MempoolMessage::UserMessage(message) => {
                self.message_exists_in_trie(fid, TrieKey::for_message(message))
            }
            MempoolMessage::ValidatorMessage(validator_message) => {
                if let Some(onchain_event) = &validator_message.on_chain_event {
                    return self
                        .message_exists_in_trie(fid, TrieKey::for_onchain_event(&onchain_event));
                }

                if let Some(fname_transfer) = &validator_message.fname_transfer {
                    if let Some(proof) = &fname_transfer.proof {
                        let name = String::from_utf8(proof.name.clone()).unwrap();
                        return self.message_exists_in_trie(
                            fid,
                            TrieKey::for_fname(fname_transfer.id, &name),
                        );
                    }
                }
                false
            }
        }
    }

    pub fn message_is_valid(&mut self, message: &MempoolMessage) -> bool {
        if self.is_message_already_merged(message) {
            return false;
        }

        return true;
    }

    pub async fn run(&mut self) {
        while let Some(message) = self.mempool_rx.recv().await {
            if self.message_is_valid(&message) {
                let fid = message.fid();
                let shard = self.message_router.route_message(fid, self.num_shards);
                let senders = self.shard_senders.get(&shard);
                match senders {
                    None => {
                        error!("Unable to find shard to send message to")
                    }
                    Some(senders) => {
                        if let Err(err) = senders.messages_tx.send(message).await {
                            error!("Unable to send message to engine: {}", err.to_string())
                        }
                    }
                }
            }
        }
    }
}
