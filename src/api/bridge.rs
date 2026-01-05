//! Bridge between HubEvent broadcast and IndexEvent channel.
//!
//! This module provides a subscriber that listens to HubEvents from the
//! ShardEngine's broadcast channel and forwards them to the indexing system.
//! This approach keeps the integration non-invasive - no changes to the engine.

use crate::api::events::{IndexEvent, IndexEventSender, IndexEventSenderExt};
use crate::proto::{hub_event, HubEvent, HubEventType};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

/// Bridge that forwards HubEvents to the indexing system.
///
/// Subscribes to the engine's HubEvent broadcast channel and converts
/// events to IndexEvents for processing by indexers.
pub struct HubEventBridge {
    event_rx: broadcast::Receiver<HubEvent>,
    index_tx: IndexEventSender,
    shard_id: u32,
}

impl HubEventBridge {
    /// Create a new bridge.
    ///
    /// # Arguments
    /// * `event_rx` - Receiver for HubEvents from the engine's broadcast channel
    /// * `index_tx` - Sender to forward IndexEvents to the worker pool
    /// * `shard_id` - The shard ID this bridge is handling
    pub fn new(
        event_rx: broadcast::Receiver<HubEvent>,
        index_tx: IndexEventSender,
        shard_id: u32,
    ) -> Self {
        Self {
            event_rx,
            index_tx,
            shard_id,
        }
    }

    /// Create a bridge from a broadcast sender (subscribes automatically).
    pub fn from_sender(
        event_tx: &broadcast::Sender<HubEvent>,
        index_tx: IndexEventSender,
        shard_id: u32,
    ) -> Self {
        Self::new(event_tx.subscribe(), index_tx, shard_id)
    }

    /// Run the bridge, forwarding events until the channel is closed or shutdown.
    ///
    /// This should be spawned as a background task.
    pub async fn run(mut self) {
        info!(shard_id = self.shard_id, "Starting HubEvent bridge");

        let mut events_forwarded: u64 = 0;
        let mut events_dropped: u64 = 0;

        loop {
            match self.event_rx.recv().await {
                Ok(hub_event) => {
                    if let Some(index_event) = self.convert_event(&hub_event) {
                        if self.index_tx.try_send_event(index_event) {
                            events_forwarded += 1;
                        } else {
                            events_dropped += 1;
                            if events_dropped % 1000 == 1 {
                                warn!(
                                    shard_id = self.shard_id,
                                    events_dropped, "Index channel full, events being dropped"
                                );
                            }
                        }
                    }
                }
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    warn!(
                        shard_id = self.shard_id,
                        lagged = count,
                        "HubEvent bridge lagged behind, missed events (will backfill)"
                    );
                }
                Err(broadcast::error::RecvError::Closed) => {
                    info!(
                        shard_id = self.shard_id,
                        events_forwarded,
                        events_dropped,
                        "HubEvent channel closed, bridge stopping"
                    );
                    break;
                }
            }
        }
    }

    /// Convert a HubEvent to an IndexEvent.
    fn convert_event(&self, hub_event: &HubEvent) -> Option<IndexEvent> {
        let event_type = HubEventType::try_from(hub_event.r#type).ok()?;

        match event_type {
            HubEventType::MergeMessage => {
                if let Some(hub_event::Body::MergeMessageBody(body)) = &hub_event.body {
                    if let Some(message) = &body.message {
                        return Some(IndexEvent::message(
                            message.clone(),
                            self.shard_id,
                            self.extract_block_height(hub_event),
                        ));
                    }
                }
                None
            }
            HubEventType::PruneMessage => {
                // Prune events may be relevant for some indexers
                if let Some(hub_event::Body::PruneMessageBody(body)) = &hub_event.body {
                    if let Some(message) = &body.message {
                        return Some(IndexEvent::message(
                            message.clone(),
                            self.shard_id,
                            self.extract_block_height(hub_event),
                        ));
                    }
                }
                None
            }
            HubEventType::RevokeMessage => {
                // Revoke events are important for maintaining consistency
                if let Some(hub_event::Body::RevokeMessageBody(body)) = &hub_event.body {
                    if let Some(message) = &body.message {
                        return Some(IndexEvent::message(
                            message.clone(),
                            self.shard_id,
                            self.extract_block_height(hub_event),
                        ));
                    }
                }
                None
            }
            HubEventType::MergeOnChainEvent => {
                if let Some(hub_event::Body::MergeOnChainEventBody(body)) = &hub_event.body {
                    if let Some(event) = &body.on_chain_event {
                        return Some(IndexEvent::onchain(
                            event.clone(),
                            self.shard_id,
                            self.extract_block_height(hub_event),
                        ));
                    }
                }
                None
            }
            HubEventType::MergeUsernameProof => {
                // Forward username proof events as hub events
                Some(IndexEvent::hub_event(hub_event.clone(), self.shard_id))
            }
            HubEventType::BlockConfirmed => {
                // Extract block info and create block committed event
                if let Some(hub_event::Body::BlockConfirmedBody(body)) = &hub_event.body {
                    return Some(IndexEvent::block_committed(
                        body.shard_index,
                        body.block_number,
                        body.total_events as usize,
                    ));
                }
                None
            }
            _ => {
                // Other event types (None, MergeIdRegistryEvent, etc.)
                // Forward as generic hub event for indexers that care
                debug!(event_type = ?event_type, "Forwarding unhandled event type");
                Some(IndexEvent::hub_event(hub_event.clone(), self.shard_id))
            }
        }
    }

    /// Extract block height from hub event ID.
    /// Event IDs encode the block height in the upper bits.
    fn extract_block_height(&self, hub_event: &HubEvent) -> u64 {
        // Event ID format: (block_number << 24) | sequence
        // See: src/storage/store/event_handler.rs
        hub_event.id >> 24
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{self, Message, MessageData, MessageType};
    use tokio::sync::mpsc;

    fn make_test_message(fid: u64) -> Message {
        Message {
            data: Some(MessageData {
                fid,
                r#type: MessageType::CastAdd as i32,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn make_merge_event(message: Message) -> HubEvent {
        HubEvent {
            r#type: HubEventType::MergeMessage as i32,
            id: 1,
            body: Some(hub_event::Body::MergeMessageBody(proto::MergeMessageBody {
                message: Some(message),
                deleted_messages: vec![],
            })),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_bridge_forwards_events() {
        let (hub_tx, _) = broadcast::channel::<HubEvent>(10);
        let (index_tx, mut index_rx) = mpsc::channel::<IndexEvent>(10);

        let bridge = HubEventBridge::from_sender(&hub_tx, index_tx, 1);

        // Spawn the bridge
        let handle = tokio::spawn(async move {
            bridge.run().await;
        });

        // Send a hub event
        let message = make_test_message(123);
        let hub_event = make_merge_event(message.clone());
        hub_tx.send(hub_event).unwrap();

        // Should receive index event
        let index_event =
            tokio::time::timeout(std::time::Duration::from_millis(100), index_rx.recv())
                .await
                .unwrap()
                .unwrap();

        match index_event {
            IndexEvent::MessageCommitted {
                message: m,
                shard_id,
                ..
            } => {
                assert_eq!(m.data.as_ref().unwrap().fid, 123);
                assert_eq!(shard_id, 1);
            }
            _ => panic!("Expected MessageCommitted event"),
        }

        // Drop sender to close bridge
        drop(hub_tx);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_bridge_handles_block_confirmed() {
        let (hub_tx, _) = broadcast::channel::<HubEvent>(10);
        let (index_tx, mut index_rx) = mpsc::channel::<IndexEvent>(10);

        let bridge = HubEventBridge::from_sender(&hub_tx, index_tx, 1);

        let handle = tokio::spawn(async move {
            bridge.run().await;
        });

        // Send block confirmed event
        let hub_event = HubEvent {
            r#type: HubEventType::BlockConfirmed as i32,
            id: 1,
            body: Some(hub_event::Body::BlockConfirmedBody(
                proto::BlockConfirmedBody {
                    block_number: 100,
                    shard_index: 1,
                    total_events: 50,
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        hub_tx.send(hub_event).unwrap();

        let index_event =
            tokio::time::timeout(std::time::Duration::from_millis(100), index_rx.recv())
                .await
                .unwrap()
                .unwrap();

        match index_event {
            IndexEvent::BlockCommitted {
                shard_id,
                block_height,
                message_count,
            } => {
                assert_eq!(shard_id, 1);
                assert_eq!(block_height, 100);
                assert_eq!(message_count, 50);
            }
            _ => panic!("Expected BlockCommitted event"),
        }

        drop(hub_tx);
        handle.await.unwrap();
    }
}
