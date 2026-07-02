use prost::Message;
use std::time::Duration;
use tokio::select;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::Instant;
use tracing::{error, info, warn};

use crate::consensus::consensus::SystemMessage;
use crate::consensus::validator::StoredValidatorSets;
use crate::core::util::verify_signatures;
use crate::mempool::mempool::{MempoolRequest, MempoolSource};
use crate::proto::{hub_event, Block, HubEvent};
use crate::storage::store::{mempool_poller::MempoolMessage, stores::Stores};
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// While waiting for a `BlockConfirmedBody` notification, also poll the durable
/// store this often. The broadcast `event_rx` is best-effort (a lagged or dropped
/// message is indistinguishable from a real miss), so the store is the tiebreaker.
const STORE_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// How many times to re-drive a block's events into the durable store after a
/// genuine confirmation timeout before giving up and alerting. This is preventive:
/// it keeps the store ahead of consensus so a later dependent event never wedges the
/// shard. It cannot recover an already-wedged shard (commit needs consensus, which is
/// frozen) — that remains the job of the manual `reconcile_heartbeat_event` override.
const MAX_CONFIRMATION_RETRIES: u32 = 3;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
    #[serde(with = "humantime_serde")]
    pub single_block_confirmation_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub sync_confirmation_timeout: Duration,
    pub sync_batch_size: u64,
    pub enabled: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            single_block_confirmation_timeout: Duration::from_secs(5),
            sync_confirmation_timeout: Duration::from_secs(10),
            sync_batch_size: 500,
            enabled: false,
        }
    }
}

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
    pub config: Config,
    pub statsd: StatsdClientWrapper,
}

impl BlockReceiver {
    fn validate_block_events(&self, block: &Block) -> bool {
        if block.events.is_empty() {
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

    /// True once the durable block-event store has persisted `seqnum`.
    ///
    /// The store is the acknowledged source of truth; the `BlockConfirmedBody`
    /// notifications on `event_rx` are only a best-effort fast path. Consulting the
    /// store directly is what prevents a lagged or dropped broadcast message from
    /// being mistaken for a real failure — the bug that silently dropped a seqnum and
    /// later wedged the shard.
    fn confirmed_in_store(&self, seqnum: u64) -> bool {
        match self.stores.block_event_store.max_seqnum() {
            Ok(max_seqnum) => max_seqnum >= seqnum,
            Err(err) => {
                warn!(
                    shard = self.shard_id,
                    seqnum,
                    "Failed to read max block event seqnum from store while confirming: {}",
                    err
                );
                false
            }
        }
    }

    pub(crate) async fn wait_for_confirmation(
        &mut self,
        seqnum: u64,
        timeout: Duration,
    ) -> Result<(), BlockReceiverError> {
        let deadline = Instant::now() + timeout;
        loop {
            // The store is the source of truth. Check it before waiting (the event may
            // already be committed) and after every wakeup, so a missed broadcast
            // notification can never turn a durably-committed seqnum into a timeout.
            if self.confirmed_in_store(seqnum) {
                return Ok(());
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(BlockReceiverError::ConfirmationTimedOut);
            }
            // Wake on a confirmation event or a short poll tick (bounded by the
            // deadline), so the store re-check above still runs if no broadcast arrives.
            let poll = tokio::time::sleep(STORE_POLL_INTERVAL.min(deadline - now));
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
                _ = poll => {}
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

    async fn sync_missing_block_events(
        &mut self,
        start_seqnum: u64,
        stop_seqnum: u64,
    ) -> Result<(), BlockReceiverError> {
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
                let num_events_processed = last_event.seqnum() - start_seqnum;
                // If we've completed a batch or completed the full sync, wait for confirmation
                if (num_events_processed > 0
                    && num_events_processed % self.config.sync_batch_size == 0)
                    || last_event.seqnum() >= stop_seqnum
                {
                    if let Err(BlockReceiverError::ConfirmationTimedOut) = self
                        .wait_for_confirmation(
                            last_event.seqnum(),
                            self.config.sync_confirmation_timeout,
                        )
                        .await
                    {
                        return Err(BlockReceiverError::ConfirmationTimedOut);
                    }
                }
                currrent_seqnum = last_event.seqnum() + 1;
            }
        }

        Ok(())
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
            if block.events.is_empty() {
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
                if let Err(BlockReceiverError::ConfirmationTimedOut) = self
                    .sync_missing_block_events(
                        last_stored_event_seqnum + 1,
                        first_event_in_block.seqnum() - 1,
                    )
                    .await
                {
                    // TODO(aditi): Right now, we will just wait for the next block with events and try again. In the future we may want better retry logic
                    warn!("Timed out waiting for confirmation. Sync ended early");
                    continue;
                }
            };

            self.submit_block(&block).await;
            let target_seqnum = last_event_in_block.seqnum();
            // Make sure the events we just submitted actually reach the durable store
            // before advancing. On a genuine timeout (store still behind — not just a
            // missed broadcast, which wait_for_confirmation now filters out) re-drive
            // the missing range instead of silently moving on: an unpersisted seqnum
            // here is the latent gap that later wedges the shard when a dependent event
            // is rejected as "not next".
            if let Err(BlockReceiverError::ConfirmationTimedOut) = self
                .wait_for_confirmation(target_seqnum, self.config.single_block_confirmation_timeout)
                .await
            {
                self.ensure_events_confirmed(target_seqnum).await;
            };
        }
    }

    /// Re-drive block events up to `target_seqnum` into the durable store after a
    /// genuine confirmation timeout, retrying a bounded number of times via the same
    /// `sync_missing_block_events` path used for forward gaps. Unlike the previous
    /// warn-and-continue behavior, this refuses to let the receiver advance past an
    /// unpersisted seqnum.
    ///
    /// This is *preventive* — it keeps the store ahead of consensus so a later
    /// dependent event never wedges the shard. It cannot un-wedge a shard whose
    /// consensus is already frozen on the dependent block (commit needs consensus);
    /// recovering that state remains the job of the manual `reconcile_heartbeat_event`
    /// override.
    async fn ensure_events_confirmed(&mut self, target_seqnum: u64) {
        for attempt in 1..=MAX_CONFIRMATION_RETRIES {
            let from = match self.stores.block_event_store.max_seqnum() {
                Ok(max_seqnum) if max_seqnum >= target_seqnum => return,
                Ok(max_seqnum) => max_seqnum + 1,
                Err(err) => {
                    warn!(
                        shard = self.shard_id,
                        target_seqnum,
                        "Failed to read max block event seqnum during confirmation retry: {}",
                        err
                    );
                    return;
                }
            };
            warn!(
                shard = self.shard_id,
                target_seqnum,
                from,
                attempt,
                "Block event confirmation timed out; re-syncing missing range"
            );
            if self
                .sync_missing_block_events(from, target_seqnum)
                .await
                .is_ok()
                && self.confirmed_in_store(target_seqnum)
            {
                return;
            }
        }
        if self.confirmed_in_store(target_seqnum) {
            return;
        }
        // The gap survived every retry. Surface it loudly and as a metric so it is
        // alertable *before* a dependent event wedges the shard — this incident was
        // invisible until a human noticed the halt.
        error!(
            shard = self.shard_id,
            seqnum = target_seqnum,
            retries = MAX_CONFIRMATION_RETRIES,
            "Block event confirmation unresolved after retries; shard may wedge if a dependent event arrives"
        );
        self.statsd
            .count_with_shard(self.shard_id, "block_receiver.gap_unresolved", 1, vec![]);
    }
}
