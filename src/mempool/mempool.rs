use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::storage::store::engine::{MempoolMessage, Senders};

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
    message_router: Box<dyn MessageRouter>,
    num_shards: u32,
    mempool_rx: mpsc::Receiver<MempoolMessage>,
}

impl Mempool {
    pub fn new(
        mempool_rx: mpsc::Receiver<MempoolMessage>,
        num_shards: u32,
        shard_senders: HashMap<u32, Senders>,
    ) -> Self {
        Mempool {
            shard_senders,
            num_shards,
            mempool_rx,
            message_router: Box::new(ShardRouter {}),
        }
    }

    pub async fn run(&mut self) {
        while let Some(message) = self.mempool_rx.recv().await {
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
