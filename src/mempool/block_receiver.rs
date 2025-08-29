use prost::Message;
use std::time::Duration;
use tokio::select;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::Instant;
use tracing::{info, warn};

use crate::consensus::consensus::SystemMessage;
use crate::consensus::validator::StoredValidatorSets;
use crate::core::util::verify_signatures;
use crate::mempool::mempool::{MempoolRequest, MempoolSource};
use crate::proto::{hub_event, Block, HubEvent};
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
    pub validator_sets: StoredValidatorSets,
}

impl BlockReceiver {
    fn validate_block_events(&self, block: &Block) -> bool {
        if block.events.len() == 0 {
            return true;
        }

        let mut events_hasher = blake3::Hasher::new();
        for event in &block.events {
            if event.data.is_none() {
                return false;
            }

            if event.hash
                != blake3::hash(&event.data.as_ref().unwrap().encode_to_vec())
                    .as_bytes()
                    .to_vec()
            {
                return false;
            }

            events_hasher.update(&event.hash);
        }

        if block.header.as_ref().unwrap().events_hash
            != events_hasher.finalize().as_bytes().to_vec()
        {
            return false;
        }

        let commits = block.commits.as_ref().unwrap();

        return verify_signatures(&commits, &self.validator_sets);
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
        if self.validate_block_events(&block) {
            for event in block.events.iter() {
                info!(
                    shard = self.shard_id.to_string(),
                    seqnum = event.data.as_ref().unwrap().seqnum.to_string(),
                    "Submitting block event to mempool"
                );
                self.mempool_tx
                    .send(MempoolRequest::AddMessage(
                        MempoolMessage::BlockEvent {
                            for_shard: self.shard_id,
                            message: event.clone(),
                        },
                        MempoolSource::Local,
                        None,
                    ))
                    .await
                    .unwrap();
            }
        }
    }

    async fn sync_missing_block_events(&mut self, start_seqnum: u64, stop_seqnum: u64) {
        info!(start_seqnum, stop_seqnum, "Syncing missing blocks",);
        let mut currrent_seqnum = start_seqnum;
        while currrent_seqnum <= stop_seqnum {
            let (block_tx, block_rx) = oneshot::channel::<Option<Block>>();
            self.system_tx
                .send(SystemMessage::BlockRequest {
                    block_event_seqnum: currrent_seqnum,
                    block_tx,
                })
                .await
                .unwrap();
            let block = block_rx.await.unwrap().unwrap();
            self.submit_block(&block).await;
            if let Some(last_event) = block.events.last() {
                currrent_seqnum = last_event.seqnum() + 1;
            }
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
            if last_event_in_block.seqnum() < last_stored_event_seqnum {
                continue;
            }

            let first_event_in_block = block.events.first().unwrap();
            if first_event_in_block.seqnum() > last_stored_event_seqnum + 1 {
                self.sync_missing_block_events(
                    last_stored_event_seqnum + 1,
                    first_event_in_block.seqnum() - 1,
                )
                .await;
                if let Err(BlockReceiverError::ConfirmationTimedOut) = self
                    .wait_for_confirmation(
                        first_event_in_block.seqnum() - 1,
                        Duration::from_secs(10),
                    )
                    .await
                {
                    // TODO(aditi): Right now, we will just wait for the next block with events and try again. In the future we may want better retry logic
                    warn!(
                        seqnum = first_event_in_block.seqnum() - 1,
                        "Timed out waiting for confirmation",
                    );
                    continue;
                }
            };
            self.submit_block(&block).await;
            // If confirmation fails, we'll try move onto the next block and retry this block if needed.
            if let Err(BlockReceiverError::ConfirmationTimedOut) = self
                .wait_for_confirmation(last_event_in_block.seqnum(), Duration::from_secs(1))
                .await
            {
                warn!(
                    seqnum = last_event_in_block.seqnum(),
                    "Timed out waiting for confirmation",
                );
            };
        }
    }
}
