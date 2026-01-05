//! Index event types and channels.
//!
//! Events are sent from the block processing path to indexers via a bounded channel.
//! If the channel is full, events are dropped (non-blocking) and will be caught up
//! via backfill.

use crate::proto::{HubEvent, Message, OnChainEvent};
use tokio::sync::mpsc;

/// Channel sender for index events.
pub type IndexEventSender = mpsc::Sender<IndexEvent>;

/// Channel receiver for index events.
pub type IndexEventReceiver = mpsc::Receiver<IndexEvent>;

/// Events sent to indexers from the block processing path.
#[derive(Debug, Clone)]
pub enum IndexEvent {
    /// A single message was committed.
    MessageCommitted {
        message: Message,
        shard_id: u32,
        block_height: u64,
    },

    /// Multiple messages were committed in a batch.
    MessagesCommitted {
        messages: Vec<Message>,
        shard_id: u32,
        block_height: u64,
    },

    /// An on-chain event was processed.
    OnChainEventProcessed {
        event: OnChainEvent,
        shard_id: u32,
        block_height: u64,
    },

    /// A hub event was emitted (wraps message/onchain events with additional metadata).
    HubEventEmitted { event: HubEvent, shard_id: u32 },

    /// Signals that a block has been fully committed.
    /// Useful for indexers that need to batch updates per block.
    BlockCommitted {
        shard_id: u32,
        block_height: u64,
        message_count: usize,
    },
}

impl IndexEvent {
    /// Create event for a single committed message.
    pub fn message(message: Message, shard_id: u32, block_height: u64) -> Self {
        Self::MessageCommitted {
            message,
            shard_id,
            block_height,
        }
    }

    /// Create event for a batch of committed messages.
    pub fn messages(messages: Vec<Message>, shard_id: u32, block_height: u64) -> Self {
        Self::MessagesCommitted {
            messages,
            shard_id,
            block_height,
        }
    }

    /// Create event for an on-chain event.
    pub fn onchain(event: OnChainEvent, shard_id: u32, block_height: u64) -> Self {
        Self::OnChainEventProcessed {
            event,
            shard_id,
            block_height,
        }
    }

    /// Create event for a hub event.
    pub fn hub_event(event: HubEvent, shard_id: u32) -> Self {
        Self::HubEventEmitted { event, shard_id }
    }

    /// Create event for block commit.
    pub fn block_committed(shard_id: u32, block_height: u64, message_count: usize) -> Self {
        Self::BlockCommitted {
            shard_id,
            block_height,
            message_count,
        }
    }

    /// Get the shard ID for this event.
    pub fn shard_id(&self) -> u32 {
        match self {
            Self::MessageCommitted { shard_id, .. } => *shard_id,
            Self::MessagesCommitted { shard_id, .. } => *shard_id,
            Self::OnChainEventProcessed { shard_id, .. } => *shard_id,
            Self::HubEventEmitted { shard_id, .. } => *shard_id,
            Self::BlockCommitted { shard_id, .. } => *shard_id,
        }
    }

    /// Get the block height for this event, if applicable.
    pub fn block_height(&self) -> Option<u64> {
        match self {
            Self::MessageCommitted { block_height, .. } => Some(*block_height),
            Self::MessagesCommitted { block_height, .. } => Some(*block_height),
            Self::OnChainEventProcessed { block_height, .. } => Some(*block_height),
            Self::HubEventEmitted { .. } => None,
            Self::BlockCommitted { block_height, .. } => Some(*block_height),
        }
    }
}

/// Extension trait for non-blocking sends to index channel.
pub trait IndexEventSenderExt {
    /// Try to send an event without blocking.
    /// Returns true if sent, false if channel is full (event dropped).
    fn try_send_event(&self, event: IndexEvent) -> bool;

    /// Send messages committed event, non-blocking.
    fn notify_messages(&self, messages: Vec<Message>, shard_id: u32, block_height: u64) -> bool;

    /// Send block committed event, non-blocking.
    fn notify_block_committed(
        &self,
        shard_id: u32,
        block_height: u64,
        message_count: usize,
    ) -> bool;
}

impl IndexEventSenderExt for IndexEventSender {
    fn try_send_event(&self, event: IndexEvent) -> bool {
        match self.try_send(event) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!("Index event channel full, event dropped (will backfill)");
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::debug!("Index event channel closed");
                false
            }
        }
    }

    fn notify_messages(&self, messages: Vec<Message>, shard_id: u32, block_height: u64) -> bool {
        if messages.is_empty() {
            return true;
        }
        self.try_send_event(IndexEvent::messages(messages, shard_id, block_height))
    }

    fn notify_block_committed(
        &self,
        shard_id: u32,
        block_height: u64,
        message_count: usize,
    ) -> bool {
        self.try_send_event(IndexEvent::block_committed(
            shard_id,
            block_height,
            message_count,
        ))
    }
}

/// Optional sender that does nothing when None.
/// Useful for optionally sending events when indexing may be disabled.
pub trait OptionalIndexEventSender {
    fn try_notify(&self, event: IndexEvent);
}

impl OptionalIndexEventSender for Option<IndexEventSender> {
    fn try_notify(&self, event: IndexEvent) {
        if let Some(sender) = self {
            sender.try_send_event(event);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_channel_capacity() {
        let (tx, mut rx) = mpsc::channel::<IndexEvent>(2);

        // Fill the channel
        assert!(tx.try_send_event(IndexEvent::block_committed(1, 1, 0)));
        assert!(tx.try_send_event(IndexEvent::block_committed(1, 2, 0)));

        // Channel full, should return false
        assert!(!tx.try_send_event(IndexEvent::block_committed(1, 3, 0)));

        // Drain one
        rx.recv().await;

        // Should work again
        assert!(tx.try_send_event(IndexEvent::block_committed(1, 4, 0)));
    }

    #[test]
    fn test_event_accessors() {
        let event = IndexEvent::messages(vec![], 5, 100);
        assert_eq!(event.shard_id(), 5);
        assert_eq!(event.block_height(), Some(100));
    }
}
