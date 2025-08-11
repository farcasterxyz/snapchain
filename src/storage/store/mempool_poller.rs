use crate::mempool::mempool::MempoolMessagesRequest;
use crate::proto::{self, FarcasterNetwork, OnChainEvent, Transaction};
use crate::storage::store::account::{OnchainEventStorageError, OnchainEventStore};
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use crate::version::version::{EngineVersion, ProtocolFeature};
use itertools::Itertools;
use std::string::ToString;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use tracing::{error, info};

#[derive(Error, Debug)]
pub enum MempoolPollerError {
    #[error(transparent)]
    MessageReceiveError(#[from] oneshot::error::RecvError),

    #[error(transparent)]
    OnchainEventError(#[from] OnchainEventStorageError),
}

pub struct MempoolPoller {
    pub messages_request_tx: Option<mpsc::Sender<MempoolMessagesRequest>>,
    pub max_messages_per_block: u32,
    pub onchain_event_store: OnchainEventStore,
    pub network: FarcasterNetwork,
    pub shard_id: u32,
    pub statsd_client: StatsdClientWrapper,
}

#[derive(Clone, Debug)]
pub enum MempoolMessage {
    UserMessage(proto::Message),
    ValidatorMessage {
        for_shard: Option<u32>,
        message: proto::ValidatorMessage,
    },
}

impl MempoolMessage {
    pub fn fid(&self) -> u64 {
        match self {
            MempoolMessage::UserMessage(msg) => msg.fid(),
            MempoolMessage::ValidatorMessage {
                for_shard: _,
                message,
            } => message.fid(),
        }
    }

    pub fn to_proto(&self) -> proto::MempoolMessage {
        let msg = match self {
            MempoolMessage::UserMessage(msg) => {
                proto::mempool_message::MempoolMessage::UserMessage(msg.clone())
            }
            _ => todo!(),
        };
        proto::MempoolMessage {
            mempool_message: Some(msg),
        }
    }
}

impl MempoolPoller {
    pub fn for_read_node(&self) -> bool {
        self.messages_request_tx.is_none()
    }
    fn time_with_shard(&self, key: &str, value: u64) {
        let key = format!("mempool_poller.{}", key);
        self.statsd_client
            .time_with_shard(self.shard_id, key.as_str(), value);
    }

    pub(crate) async fn pull_messages(
        &mut self,
        max_wait: Duration,
    ) -> Result<Vec<MempoolMessage>, MempoolPollerError> {
        if let Some(messages_request_tx) = &self.messages_request_tx {
            let now = std::time::Instant::now();
            let (message_tx, message_rx) = oneshot::channel();

            if let Err(err) = messages_request_tx
                .send(MempoolMessagesRequest {
                    shard_id: self.shard_id,
                    message_tx,
                    max_messages_per_block: self.max_messages_per_block,
                })
                .await
            {
                error!(
                    "Could not send request for messages to mempool {}",
                    err.to_string()
                )
            }

            match timeout(max_wait, message_rx).await {
                Ok(response) => match response {
                    Ok(new_messages) => {
                        let elapsed = now.elapsed();
                        self.time_with_shard("pull_messages", elapsed.as_millis() as u64);
                        Ok(new_messages)
                    }
                    Err(err) => Err(err)?,
                },
                Err(_) => {
                    error!("Did not receive messages from mempool in time");
                    // Just proceed with no messages
                    Ok(vec![])
                }
            }
        } else {
            Ok(vec![])
        }
    }

    // Groups messages by fid and creates a transaction for each fid
    pub fn create_transactions_from_mempool(
        &mut self,
        messages: Vec<MempoolMessage>,
    ) -> Result<Vec<Transaction>, MempoolPollerError> {
        let mut transactions = vec![];

        let grouped_messages = messages.iter().into_group_map_by(|msg| msg.fid());
        let unique_fids = grouped_messages.keys().len();
        for (fid, messages) in grouped_messages {
            let mut transaction = Transaction {
                fid: fid as u64,
                account_root: vec![], // Starts empty, will be updated after replay
                system_messages: vec![],
                user_messages: vec![],
            };

            // First pass: collect all system_messages for this FID
            for msg in &messages {
                if let MempoolMessage::ValidatorMessage {
                    for_shard: _,
                    message,
                } = msg
                {
                    transaction.system_messages.push(message.clone());
                }
            }

            // Extract just the OnChainEvents from the collected system messages
            let pending_onchain_events: Vec<OnChainEvent> = transaction
                .system_messages
                .iter()
                .filter_map(|vm| vm.on_chain_event.clone())
                .collect();

            let version = EngineVersion::current(self.network);
            let maybe_onchainevents =
                if version.is_enabled(ProtocolFeature::DependentMessagesInBulkSubmit) {
                    Some(pending_onchain_events.as_slice())
                } else {
                    None
                };

            let storage_slot = self.onchain_event_store.get_storage_slot_for_fid(
                fid,
                self.network,
                maybe_onchainevents,
            )?;

            // Second pass: filter user_messages based on the look-ahead storage check
            for msg in messages {
                if let MempoolMessage::UserMessage(user_msg) = msg {
                    // Only include messages for users that have storage
                    if storage_slot.is_active() {
                        transaction.user_messages.push(user_msg.clone());
                    }
                }
            }

            if !transaction.user_messages.is_empty() || !transaction.system_messages.is_empty() {
                transactions.push(transaction);
            }
        }
        info!(
            transactions = transactions.len(),
            messages = messages.len(),
            fids = unique_fids,
            "Created transactions from mempool"
        );
        Ok(transactions)
    }
}
