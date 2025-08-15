use std::time::Duration;
use tokio::select;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::Instant;
use tracing::info;

use crate::consensus::consensus::SystemMessage;
use crate::mempool::mempool::{MempoolRequest, MempoolSource};
use crate::proto::{hub_event, Block, HubEvent, ValidatorMessage};
use crate::storage::store::{mempool_poller::MempoolMessage, stores::Stores};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum BlockReceiverError {
    #[error("timed out waiting for confirmation")]
    ConfirmationTimedOut,
}
// Maintain one per shard, shards move independently
pub struct BlockReceiver {
    pub shard_id: u32,
    pub block_rx: broadcast::Receiver<Block>,
    pub mempool_tx: mpsc::Sender<MempoolRequest>,
    pub system_tx: mpsc::Sender<SystemMessage>,
    pub event_rx: broadcast::Receiver<HubEvent>,
    pub stores: Stores,
}

impl BlockReceiver {
    fn validate_block(&self, _block: &Block) -> bool {
        // TODO(aditi): Fill this in
        true
    }

    async fn wait_for_confirmation(
        &mut self,
        seqnum: u64,
        timeout: Duration,
    ) -> Result<(), BlockReceiverError> {
        let deadline = Instant::now() + timeout;
        loop {
            let timeout = tokio::time::sleep_until(deadline);
            select! {
                event = self.event_rx.recv() => {
                    if let Ok(event) = event {
                        if let Some(hub_event::Body::BlockConfirmedBody(body)) = event.body {
                            if body.max_block_event_seqnum >= seqnum {
                                return Ok(())
                            }
                        }
                    }
                }
                _ = timeout => {
                    return Err(BlockReceiverError::ConfirmationTimedOut)
                }

            }
        }
    }

    async fn submit_block(&mut self, block: &Block) {
        if self.validate_block(&block) {
            for event in block.events.iter() {
                info!(
                    shard = self.shard_id.to_string(),
                    seqnum = event.seqnum.to_string(),
                    "Submitting block event to mempool"
                );
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
        }
    }

    pub async fn run(&mut self) {
        info!(shard = self.shard_id.to_string(), "Running block receiver");
        loop {
            let block = self.block_rx.recv().await.unwrap();
            info!(
                shard = self.shard_id.to_string(),
                num_events = block.events.len(),
                height = block.header.as_ref().unwrap().height.unwrap().block_number,
                "Received block"
            );
            if block.events.len() == 0 {
                continue;
            }
            // The db is the source of truth, it's possible to read this out of the events_rx channel but delivery over that channel is not reliable (it's a broadcast channel) we may not have the most up to date state.
            let last_stored_event_seqnum = self.stores.block_event_store.max_seqnum().unwrap();
            let last_event_in_block = block.events.last().unwrap();
            if last_event_in_block.seqnum < last_stored_event_seqnum {
                continue;
            }

            let first_event_in_block = block.events.first().unwrap();
            if first_event_in_block.seqnum > last_stored_event_seqnum + 1 {
                let last_stored_event = self
                    .stores
                    .block_event_store
                    .get_last_block_event()
                    .unwrap();
                let last_block_number = match &last_stored_event {
                    None => 0,
                    Some(last_event) => last_event.block_number,
                };
                self.sync_missing_blocks(
                    last_block_number,
                    block.header.as_ref().unwrap().height.unwrap().block_number - 1,
                )
                .await;
                if let Err(BlockReceiverError::ConfirmationTimedOut) = self
                    .wait_for_confirmation(first_event_in_block.seqnum - 1, Duration::from_secs(10))
                    .await
                {
                    // TODO(aditi): Right now, we will just wait for the next block with events and try again. In the future we may want better retry logic
                    continue;
                }
            };
            self.submit_block(&block).await;
            // If confirmation fails, we'll try move onto the next block and retry this block if needed.
            let _ = self
                .wait_for_confirmation(last_event_in_block.seqnum, Duration::from_secs(1))
                .await;
        }
    }
}
