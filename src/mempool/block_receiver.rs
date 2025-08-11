use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::consensus::consensus::SystemMessage;
use crate::mempool::mempool::{MempoolRequest, MempoolSource};
use crate::proto::{Block, ValidatorMessage};
use crate::storage::store::{mempool_poller::MempoolMessage, stores::Stores};

// Maintain one per shard, shards move independently
pub struct BlockReceiver {
    pub shard_id: u32,
    pub block_rx: broadcast::Receiver<Block>,
    pub mempool_tx: mpsc::Sender<MempoolRequest>,
    pub system_tx: mpsc::Sender<SystemMessage>,
    pub stores: Stores,
}

impl BlockReceiver {
    fn validate_block(&self, _block: &Block) -> bool {
        // TODO(aditi): Fill this in
        true
    }

    async fn submit_block(&mut self, block: &Block) {
        if self.validate_block(&block) {
            for event in block.events.iter() {
                self.mempool_tx
                    .send(MempoolRequest::AddMessage(
                        MempoolMessage::ValidatorMessage {
                            for_shard: Some(self.shard_id),
                            message: ValidatorMessage {
                                on_chain_event: None,
                                fname_transfer: None,
                                block_event: Some(event.clone()),
                            },
                        },
                        MempoolSource::Local,
                        None,
                    ))
                    .await
                    .unwrap();
            }
        }
    }

    async fn sync_missing_blocks(&mut self, start_block_number: u64, stop_block_number: u64) {
        let mut poll_interval = tokio::time::interval(Duration::from_secs(1));
        let mut current_block_number = start_block_number;
        while current_block_number <= stop_block_number {
            let (block_tx, block_rx) = oneshot::channel::<Option<Block>>();
            self.system_tx
                .send(SystemMessage::BlockRequest {
                    block_number: current_block_number,
                    block_tx,
                })
                .await
                .unwrap();
            let block = block_rx.await.unwrap().unwrap();
            if block.events.len() == 0 {
                current_block_number += 1;
                continue;
            }
            self.submit_block(&block).await;
            poll_interval.tick().await;
            let last_stored_seqnum = self.stores.block_event_store.max_seqnum().unwrap();
            let last_seqnum_on_block = block.events.last().as_ref().unwrap().seqnum;
            if last_stored_seqnum >= last_seqnum_on_block {
                current_block_number += 1;
            }
        }
    }

    pub async fn run(&mut self) {
        let mut poll_interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            let block = self.block_rx.recv().await.unwrap();
            if block.events.len() == 0 {
                continue;
            }
            let last_stored_event = self
                .stores
                .block_event_store
                .get_last_block_event()
                .unwrap();
            let last_stored_event_seqnum = match &last_stored_event {
                None => 0,
                Some(last_event) => last_event.seqnum,
            };
            let first_event_in_block = block.events.first().unwrap();
            let last_event_in_block = block.events.last().unwrap();
            // TODO(aditi): There are some details to handle correctly here. If the seqnum is lower we should drop the block. We should also handle the case where some events in the block have been committed but not all.
            if last_event_in_block.seqnum < last_stored_event_seqnum {
                continue;
            }
            if first_event_in_block.seqnum > last_stored_event_seqnum + 1 {
                let last_block_number = match &last_stored_event {
                    None => 0,
                    Some(last_event) => last_event.block_number,
                };
                self.sync_missing_blocks(last_block_number, first_event_in_block.block_number - 1)
                    .await;
            };
            self.submit_block(&block).await;
            poll_interval.tick().await;
        }
    }
}
