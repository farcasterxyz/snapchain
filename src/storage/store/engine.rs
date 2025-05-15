use super::account::UsernameProofStore;
use super::account::{IntoU8, OnchainEventStorageError, UserDataStore};
use crate::core::error::HubError;
use crate::core::types::Height;
use crate::core::validations;
use crate::core::validations::verification;
use crate::mempool::mempool::MempoolMessagesRequest;
use crate::proto::UserNameProof;
use crate::proto::UserNameType;
use crate::proto::{self, Block, MessageType, ShardChunk, Transaction};
use crate::proto::{FarcasterNetwork, HubEvent};
use crate::proto::{OnChainEvent, OnChainEventType};
use crate::storage::db::{PageOptions, RocksDB, RocksDbTransactionBatch};
use crate::storage::store::account::{CastStore, MessagesPage, OnchainEventStore};
use crate::storage::store::stores::{StoreLimits, Stores};
use crate::storage::store::BlockStore;
use crate::storage::trie;
use crate::storage::trie::merkle_trie;
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use informalsystems_malachitebft_core_types::Round;
use itertools::Itertools;
use merkle_trie::TrieKey;
use std::cmp::PartialEq;
use std::collections::HashSet;
use std::str;
use std::string::ToString;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::oneshot;
use tokio::sync::{broadcast, mpsc};
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

#[derive(Error, Debug)]
pub enum EngineError {
    #[error(transparent)]
    TrieError(#[from] trie::errors::TrieError),

    #[error(transparent)]
    StoreError(#[from] HubError),

    #[error("unsupported message type")]
    UnsupportedMessageType(MessageType),

    #[error("merkle trie root hash mismatch")]
    HashMismatch,

    #[error("Unable to get usage count")]
    UsageCountError,

    #[error("message receive error")]
    MessageReceiveError(#[from] oneshot::error::RecvError),

    #[error(transparent)]
    MergeOnchainEventError(#[from] OnchainEventStorageError),

    #[error(transparent)]
    EngineMessageValidationError(#[from] MessageValidationError),
}

#[derive(Clone, Debug, PartialEq, strum_macros::Display)]
pub enum ProposalSource {
    Simulate,
    Propose,
    Validate,
    Commit,
}

#[derive(Error, Debug, Clone)]
pub enum MessageValidationError {
    #[error("message has no data")]
    NoMessageData,

    #[error("unknown fid")]
    MissingFid,

    #[error("invalid signer")]
    MissingSigner,

    #[error(transparent)]
    MessageValidationError(#[from] validations::error::ValidationError),

    #[error("invalid message type")]
    InvalidMessageType(i32),

    #[error(transparent)]
    StoreError(#[from] HubError),

    #[error("fname is not registered for fid")]
    MissingFname,
}

#[derive(Clone, Debug)]
pub enum MempoolMessage {
    UserMessage(proto::Message),
    ValidatorMessage(proto::ValidatorMessage),
}

impl MempoolMessage {
    pub fn fid(&self) -> u64 {
        match self {
            MempoolMessage::UserMessage(msg) => msg.fid(),
            MempoolMessage::ValidatorMessage(msg) => msg.fid(),
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

// Shard state root and the transactions
#[derive(Clone)]
pub struct ShardStateChange {
    pub shard_id: u32,
    pub new_state_root: Vec<u8>,
    pub transactions: Vec<Transaction>,
    pub events: Vec<HubEvent>,
}

#[derive(Clone)]
pub struct Senders {
    pub events_tx: broadcast::Sender<HubEvent>,
}

impl Senders {
    pub fn new() -> Senders {
        let (events_tx, _events_rx) = broadcast::channel::<HubEvent>(10_000); // About 10s of events with full blocks
        Senders { events_tx }
    }
}

struct TransactionCounts {
    transactions: u64,
    user_messages: u64,
    system_messages: u64,
}

#[derive(Clone)]
struct CachedTransaction {
    shard_root: Vec<u8>,
    events: Vec<HubEvent>,
    txn: RocksDbTransactionBatch,
}

pub struct ShardEngine {
    shard_id: u32,
    network: FarcasterNetwork,
    pub db: Arc<RocksDB>,
    senders: Senders,
    stores: Stores,
    statsd_client: StatsdClientWrapper,
    max_messages_per_block: u32,
    messages_request_tx: Option<mpsc::Sender<MempoolMessagesRequest>>,
    pending_txn: Option<CachedTransaction>,
}

impl ShardEngine {
    pub fn new(
        db: Arc<RocksDB>,
        network: proto::FarcasterNetwork,
        trie: merkle_trie::MerkleTrie,
        shard_id: u32,
        store_limits: StoreLimits,
        statsd_client: StatsdClientWrapper,
        max_messages_per_block: u32,
        messages_request_tx: Option<mpsc::Sender<MempoolMessagesRequest>>,
    ) -> ShardEngine {
        // TODO: adding the trie here introduces many calls that want to return errors. Rethink unwrap strategy.
        ShardEngine {
            shard_id,
            network,
            stores: Stores::new(
                db.clone(),
                shard_id,
                trie,
                store_limits,
                statsd_client.clone(),
            ),
            senders: Senders::new(),
            db,
            statsd_client,
            max_messages_per_block,
            messages_request_tx,
            pending_txn: None,
        }
    }

    pub fn shard_id(&self) -> u32 {
        self.shard_id
    }

    // statsd
    fn count(&self, key: &str, count: u64) {
        let key = format!("engine.{}", key);
        self.statsd_client
            .count_with_shard(self.shard_id, key.as_str(), count);
    }

    // statsd
    fn gauge(&self, key: &str, value: u64) {
        let key = format!("engine.{}", key);
        self.statsd_client
            .gauge_with_shard(self.shard_id, key.as_str(), value);
    }

    fn time_with_shard(&self, key: &str, value: u64) {
        let key = format!("engine.{}", key);
        self.statsd_client
            .time_with_shard(self.shard_id, key.as_str(), value);
    }

    pub fn get_stores(&self) -> Stores {
        self.stores.clone()
    }

    pub fn get_senders(&self) -> Senders {
        self.senders.clone()
    }

    pub(crate) fn trie_root_hash(&self) -> Vec<u8> {
        self.stores.trie.root_hash().unwrap()
    }

    pub(crate) async fn pull_messages(
        &mut self,
        max_wait: Duration,
    ) -> Result<Vec<MempoolMessage>, EngineError> {
        let now = std::time::Instant::now();
        if let Some(messages_request_tx) = &self.messages_request_tx {
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
                    Err(err) => Err(EngineError::from(err)),
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

    fn prepare_proposal(
        &mut self,
        trie_ctx: &merkle_trie::Context,
        txn_batch: &mut RocksDbTransactionBatch,
        shard_id: u32,
        messages: Vec<MempoolMessage>,
    ) -> Result<ShardStateChange, EngineError> {
        self.count("prepare_proposal.recv_messages", messages.len() as u64);

        let mut snapchain_txns = self.create_transactions_from_mempool(messages)?;
        let mut events = vec![];
        let mut validation_error_count = 0;
        for snapchain_txn in &mut snapchain_txns {
            let (account_root, txn_events, validation_errors) = self.replay_snapchain_txn(
                trie_ctx,
                &snapchain_txn,
                txn_batch,
                ProposalSource::Propose,
            )?;
            snapchain_txn.account_root = account_root;
            events.extend(txn_events);
            validation_error_count += validation_errors.len();
        }

        let count = Self::txn_counts(&snapchain_txns);

        self.count("prepare_proposal.transactions", count.transactions);
        self.count("prepare_proposal.user_messages", count.user_messages);
        self.count("prepare_proposal.system_messages", count.system_messages);
        self.count(
            "prepare_proposal.validation_errors",
            validation_error_count as u64,
        );

        let new_root_hash = self.stores.trie.root_hash()?;
        let result = ShardStateChange {
            shard_id,
            new_state_root: new_root_hash.clone(),
            transactions: snapchain_txns,
            events,
        };

        Ok(result)
    }

    // Groups messages by fid and creates a transaction for each fid
    fn create_transactions_from_mempool(
        &mut self,
        messages: Vec<MempoolMessage>,
    ) -> Result<Vec<Transaction>, EngineError> {
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
            let storage_slot = self
                .stores
                .onchain_event_store
                .get_storage_slot_for_fid(fid)?;
            for msg in messages {
                match msg {
                    MempoolMessage::ValidatorMessage(msg) => {
                        transaction.system_messages.push(msg.clone());
                    }
                    MempoolMessage::UserMessage(msg) => {
                        // Only include messages for users that have storage
                        if storage_slot.is_active() {
                            transaction.user_messages.push(msg.clone());
                        }
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

    pub fn start_round(&mut self, height: Height, _round: Round) {
        self.pending_txn = None;
        self.stores
            .event_handler
            .set_current_height(height.block_number);
    }

    pub fn propose_state_change(
        &mut self,
        shard: u32,
        messages: Vec<MempoolMessage>,
    ) -> ShardStateChange {
        let now = std::time::Instant::now();
        let mut txn = RocksDbTransactionBatch::new();

        let count_fn = Self::make_count_fn(self.statsd_client.clone(), self.shard_id);
        let count_callback = move |read_count: (u64, u64)| {
            count_fn("trie.db_get_count.total", read_count.0);
            count_fn("trie.db_get_count.for_propose", read_count.0);
            count_fn("trie.mem_get_count.total", read_count.1);
            count_fn("trie.mem_get_count.for_propose", read_count.1);
        };
        let result = self
            .prepare_proposal(
                &merkle_trie::Context::with_callback(count_callback),
                &mut txn,
                shard,
                messages,
            )
            .unwrap(); //TODO: don't unwrap()

        // TODO: this should probably operate automatically via drop trait
        self.stores.trie.reload(&self.db).unwrap();
        self.pending_txn = Some(CachedTransaction {
            shard_root: result.new_state_root.clone(),
            events: result.events.clone(),
            txn,
        });

        let proposal_duration = now.elapsed();
        self.time_with_shard("propose_time", proposal_duration.as_millis() as u64);

        self.count("propose.invoked", 1);
        result
    }

    fn txn_summary(txn: &Transaction) -> String {
        let mut summary = String::new();
        let fid = format!("fid: {}\n", txn.fid);
        summary += fid.as_str();
        for message in &txn.user_messages {
            let msg_type = message.msg_type().as_str_name();
            let message_summary = format!(
                "message_type: {}, msg_hash: {}\n",
                hex::encode(&message.hash),
                msg_type
            );
            summary += message_summary.as_str()
        }

        for message in &txn.system_messages {
            let onchain_event_summary = match &message.on_chain_event {
                Some(onchain_event) => {
                    format!(
                        "message_type: onchain_event, type: {}, tx_hash: {}, log_index: {}\n",
                        onchain_event.r#type().as_str_name(),
                        hex::encode(&onchain_event.transaction_hash),
                        onchain_event.log_index
                    )
                }
                None => "".to_string(),
            };
            summary += onchain_event_summary.as_str();

            let fname_transfer_summary =
                match &message.fname_transfer {
                    Some(fname_transfer) => {
                        format!(
                        "message_type: fname_transfer, from_fid: {}, to_fid: {}, username: {:#?}",
                        fname_transfer.from_fid,
                        fname_transfer.id,
                        fname_transfer.proof.as_ref().map(|proof| proof.name.clone())
                    )
                    }
                    None => "".to_string(),
                };
            summary += fname_transfer_summary.as_str()
        }
        summary
    }

    fn txns_summary(transactions: &[Transaction]) -> String {
        let mut summary = String::new();
        for snapchain_txn in transactions {
            summary += Self::txn_summary(snapchain_txn).as_str()
        }
        summary
    }

    fn replay_proposal(
        &mut self,
        trie_ctx: &merkle_trie::Context,
        txn_batch: &mut RocksDbTransactionBatch,
        transactions: &[Transaction],
        shard_root: &[u8],
        source: ProposalSource,
    ) -> Result<Vec<HubEvent>, EngineError> {
        let now = std::time::Instant::now();
        let mut events = vec![];
        // Validate that the trie is in a good place to start with
        match self.get_last_shard_chunk() {
            None => { // There are places where it's hard to provide a parent hash-- e.g. tests so make this an option and skip validation if not present
            }
            Some(shard_chunk) => match self.stores.trie.root_hash() {
                Err(err) => {
                    warn!(
                        source = source.to_string(),
                        "Unable to compute trie root hash {:#?}", err
                    )
                }
                Ok(root_hash) => {
                    let parent_shard_root = shard_chunk.header.unwrap().shard_root;
                    if root_hash != parent_shard_root {
                        warn!(
                            shard_id = self.shard_id,
                            our_shard_root = hex::encode(&root_hash),
                            parent_shard_root = hex::encode(parent_shard_root),
                            source = source.to_string(),
                            "Parent shard root mismatch"
                        );
                    }
                }
            },
        }

        for snapchain_txn in transactions {
            let (account_root, txn_events, _) =
                self.replay_snapchain_txn(trie_ctx, snapchain_txn, txn_batch, source.clone())?;
            // Reject early if account roots fail to match (shard roots will definitely fail)
            if &account_root != &snapchain_txn.account_root {
                warn!(
                    fid = snapchain_txn.fid,
                    new_account_root = hex::encode(&account_root),
                    tx_account_root = hex::encode(&snapchain_txn.account_root),
                    source = source.to_string(),
                    summary = Self::txn_summary(snapchain_txn),
                    num_system_messages = snapchain_txn.system_messages.len(),
                    num_user_messages = snapchain_txn.user_messages.len(),
                    "Account root mismatch"
                );
                return Err(EngineError::HashMismatch);
            }
            events.extend(txn_events);
        }

        let root1 = self.stores.trie.root_hash()?;
        if &root1 != shard_root {
            warn!(
                shard_id = self.shard_id,
                new_shard_root = hex::encode(&root1),
                tx_shard_root = hex::encode(shard_root),
                source = source.to_string(),
                summary = Self::txns_summary(transactions),
                num_txns = transactions.len(),
                "Shard root mismatch"
            );
            return Err(EngineError::HashMismatch);
        }

        let elapsed = now.elapsed();
        self.time_with_shard("replay_proposal_time", elapsed.as_millis() as u64);
        Ok(events)
    }

    fn replay_snapchain_txn(
        &mut self,
        trie_ctx: &merkle_trie::Context,
        snapchain_txn: &Transaction,
        txn_batch: &mut RocksDbTransactionBatch,
        source: ProposalSource,
    ) -> Result<(Vec<u8>, Vec<HubEvent>, Vec<MessageValidationError>), EngineError> {
        let now = std::time::Instant::now();
        let total_user_messages = snapchain_txn.user_messages.len();
        let total_system_messages = snapchain_txn.system_messages.len();
        let mut user_messages_count = 0;
        let mut system_messages_count = 0;
        let mut onchain_events_count = 0;
        let mut merged_fnames_count = 0;
        let mut merged_messages_count = 0;
        let mut pruned_messages_count = 0;
        let mut revoked_messages_count = 0;
        let mut events = vec![];
        let mut message_types = HashSet::new();
        let mut revoked_signers = HashSet::new();

        let mut validation_errors = vec![];

        // System messages first, then user messages and finally prunes
        for msg in &snapchain_txn.system_messages {
            if let Some(onchain_event) = &msg.on_chain_event {
                let event = self
                    .stores
                    .onchain_event_store
                    .merge_onchain_event(onchain_event.clone(), txn_batch);

                match event {
                    Ok(hub_event) => {
                        onchain_events_count += 1;
                        self.update_trie(trie_ctx, &hub_event, txn_batch)?;
                        events.push(hub_event.clone());
                        system_messages_count += 1;
                        match &onchain_event.body {
                            Some(proto::on_chain_event::Body::SignerEventBody(signer_event)) => {
                                if signer_event.event_type == proto::SignerEventType::Remove as i32
                                    && OnchainEventStore::is_signer_key(&signer_event)
                                {
                                    revoked_signers.insert(signer_event.key.clone());
                                }
                            }
                            _ => {}
                        }
                    }
                    Err(err) => {
                        if source != ProposalSource::Simulate {
                            warn!("Error merging onchain event: {:?}", err);
                        }
                    }
                }
            }
            if let Some(fname_transfer) = &msg.fname_transfer {
                match &fname_transfer.proof {
                    None => {
                        if source != ProposalSource::Simulate {
                            warn!(
                                fid = snapchain_txn.fid,
                                id = fname_transfer.id,
                                "Fname transfer has no proof"
                            );
                        }
                    }
                    Some(proof) => {
                        match verification::validate_fname_transfer(fname_transfer) {
                            Ok(_) => {
                                let event = UserDataStore::merge_username_proof(
                                    &self.stores.user_data_store,
                                    proof,
                                    txn_batch,
                                );
                                match event {
                                    Ok(hub_event) => {
                                        merged_fnames_count += 1;
                                        self.update_trie(
                                            &merkle_trie::Context::new(),
                                            &hub_event,
                                            txn_batch,
                                        )?;
                                        events.push(hub_event.clone());
                                        system_messages_count += 1;
                                    }
                                    Err(err) => {
                                        if source != ProposalSource::Simulate {
                                            warn!("Error merging fname transfer: {:?}", err);
                                        }
                                    }
                                }
                                // If the name was transfered from an existing fid, we need to make sure to revoke any existing username UserDataAdd
                                if fname_transfer.from_fid > 0 {
                                    let existing_username = self.get_user_data_by_fid_and_type(
                                        fname_transfer.from_fid,
                                        proto::UserDataType::Username,
                                    );
                                    if existing_username.is_ok() {
                                        let existing_username = existing_username.unwrap();
                                        let event = self
                                            .stores
                                            .user_data_store
                                            .revoke(&existing_username, txn_batch);
                                        match event {
                                            Ok(hub_event) => {
                                                revoked_messages_count += 1;
                                                self.update_trie(
                                                    &merkle_trie::Context::new(),
                                                    &hub_event,
                                                    txn_batch,
                                                )?;
                                                events.push(hub_event.clone());
                                            }
                                            Err(err) => {
                                                if source != ProposalSource::Simulate {
                                                    warn!(
                                                        "Error revoking existing username: {:?}",
                                                        err
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            Err(err) => {
                                if source != ProposalSource::Simulate {
                                    warn!("Error validating fname transfer: {:?}", err);
                                }
                            }
                        }
                    }
                }
            }
        }

        for key in revoked_signers {
            let result = self
                .stores
                .revoke_messages(snapchain_txn.fid, &key, txn_batch);
            match result {
                Ok(revoke_events) => {
                    for event in revoke_events {
                        revoked_messages_count += 1;
                        self.update_trie(trie_ctx, &event, txn_batch)?;
                        events.push(event.clone());
                    }
                }
                Err(err) => {
                    if source != ProposalSource::Simulate {
                        warn!(
                            fid = snapchain_txn.fid,
                            key = hex::encode(key),
                            "Error revoking signer: {:?}",
                            err
                        );
                    }
                }
            }
        }

        for msg in &snapchain_txn.user_messages {
            // Errors are validated based on the shard root
            match self.validate_user_message(msg, txn_batch) {
                Ok(()) => {
                    let result = self.merge_message(msg, txn_batch);
                    match result {
                        Ok(event) => {
                            merged_messages_count += 1;
                            self.update_trie(trie_ctx, &event, txn_batch)?;
                            events.push(event.clone());
                            user_messages_count += 1;
                            message_types.insert(msg.msg_type());
                        }
                        Err(err) => {
                            if source != ProposalSource::Simulate {
                                warn!(
                                    fid = msg.fid(),
                                    hash = msg.hex_hash(),
                                    "Error merging message: {:?}",
                                    err
                                );
                            }
                            validation_errors.push(err.clone());
                            events.push(HubEvent::from_validation_error(err, msg));
                        }
                    }
                }
                Err(err) => {
                    if source != ProposalSource::Simulate {
                        warn!(
                            fid = msg.fid(),
                            hash = msg.hex_hash(),
                            "Error validating user message: {:?}",
                            err
                        );
                    }
                    validation_errors.push(err.clone());
                    events.push(HubEvent::from_validation_error(err, msg));
                }
            }
        }

        for msg_type in message_types {
            let fid = snapchain_txn.fid;
            let result = self.prune_messages(fid, msg_type, txn_batch);
            match result {
                Ok(pruned_events) => {
                    for event in pruned_events {
                        pruned_messages_count += 1;
                        self.update_trie(trie_ctx, &event, txn_batch)?;
                        events.push(event.clone());
                    }
                }
                Err(err) => {
                    if source != ProposalSource::Simulate {
                        warn!(
                            fid = fid,
                            msg_type = msg_type.into_u8(),
                            "Error pruning messages: {:?}",
                            err
                        );
                    }
                }
            }
        }

        let account_root =
            self.stores
                .trie
                .get_hash(&self.db, txn_batch, &TrieKey::for_fid(snapchain_txn.fid));
        debug!(
            fid = snapchain_txn.fid,
            num_user_messages = total_user_messages,
            num_system_messages = total_system_messages,
            user_messages_merged = user_messages_count,
            system_messages_merged = system_messages_count,
            onchain_events_merged = onchain_events_count,
            fnames_merged = merged_fnames_count,
            messages_merged = merged_messages_count,
            messages_pruned = pruned_messages_count,
            messages_revoked = revoked_messages_count,
            validation_errors = validation_errors.len(),
            new_account_root = hex::encode(&account_root),
            tx_account_root = hex::encode(&snapchain_txn.account_root),
            source = source.to_string(),
            "Replayed transaction"
        );
        let elapsed = now.elapsed();
        self.time_with_shard("replay_txn_time_us", elapsed.as_micros() as u64);

        // Return the new account root hash
        Ok((account_root, events, validation_errors))
    }

    fn merge_message(
        &mut self,
        msg: &proto::Message,
        txn_batch: &mut RocksDbTransactionBatch,
    ) -> Result<proto::HubEvent, MessageValidationError> {
        let now = std::time::Instant::now();
        let data = msg
            .data
            .as_ref()
            .ok_or(MessageValidationError::NoMessageData)?;
        let mt = MessageType::try_from(data.r#type)
            .or(Err(MessageValidationError::InvalidMessageType(data.r#type)))?;

        let event = match mt {
            MessageType::CastAdd | MessageType::CastRemove => self
                .stores
                .cast_store
                .merge(msg, txn_batch)
                .map_err(|e| MessageValidationError::StoreError(e)),
            MessageType::LinkAdd | MessageType::LinkRemove | MessageType::LinkCompactState => self
                .stores
                .link_store
                .merge(msg, txn_batch)
                .map_err(|e| MessageValidationError::StoreError(e)),
            MessageType::ReactionAdd | MessageType::ReactionRemove => self
                .stores
                .reaction_store
                .merge(msg, txn_batch)
                .map_err(|e| MessageValidationError::StoreError(e)),
            MessageType::UserDataAdd => self
                .stores
                .user_data_store
                .merge(msg, txn_batch)
                .map_err(|e| MessageValidationError::StoreError(e)),
            MessageType::VerificationAddEthAddress | MessageType::VerificationRemove => self
                .stores
                .verification_store
                .merge(msg, txn_batch)
                .map_err(|e| MessageValidationError::StoreError(e)),
            MessageType::UsernameProof => {
                let store = &self.stores.username_proof_store;
                let result = store.merge(msg, txn_batch);
                result.map_err(|e| MessageValidationError::StoreError(e))
            }
            unhandled_type => {
                return Err(MessageValidationError::InvalidMessageType(
                    unhandled_type as i32,
                ));
            }
        }?;
        let elapsed = now.elapsed();
        self.time_with_shard("merge_message_time_us", elapsed.as_micros() as u64);
        Ok(event)
    }

    fn prune_messages(
        &mut self,
        fid: u64,
        msg_type: MessageType,
        txn_batch: &mut RocksDbTransactionBatch,
    ) -> Result<Vec<HubEvent>, EngineError> {
        let (current_count, max_count) = self
            .stores
            .get_usage(fid, msg_type, txn_batch)
            .map_err(|_| EngineError::UsageCountError)?;

        let events = match msg_type {
            MessageType::CastAdd | MessageType::CastRemove => self
                .stores
                .cast_store
                .prune_messages(fid, current_count, max_count, txn_batch)
                .map_err(|e| EngineError::StoreError(e)),
            MessageType::LinkAdd | MessageType::LinkRemove | MessageType::LinkCompactState => self
                .stores
                .link_store
                .prune_messages(fid, current_count, max_count, txn_batch)
                .map_err(|e| EngineError::StoreError(e)),
            MessageType::ReactionAdd | MessageType::ReactionRemove => self
                .stores
                .reaction_store
                .prune_messages(fid, current_count, max_count, txn_batch)
                .map_err(|e| EngineError::StoreError(e)),
            MessageType::UserDataAdd => self
                .stores
                .user_data_store
                .prune_messages(fid, current_count, max_count, txn_batch)
                .map_err(|e| EngineError::StoreError(e)),
            MessageType::VerificationAddEthAddress | MessageType::VerificationRemove => self
                .stores
                .verification_store
                .prune_messages(fid, current_count, max_count, txn_batch)
                .map_err(|e| EngineError::StoreError(e)),
            unhandled_type => {
                return Err(EngineError::UnsupportedMessageType(unhandled_type));
            }
        }?;

        if !events.is_empty() {
            info!(
                fid = fid,
                msg_type = msg_type.into_u8(),
                count = events.len(),
                "Pruned messages"
            );
        }
        Ok(events)
    }

    fn update_trie(
        &mut self,
        ctx: &merkle_trie::Context,
        event: &proto::HubEvent,
        txn_batch: &mut RocksDbTransactionBatch,
    ) -> Result<(), EngineError> {
        let now = std::time::Instant::now();
        match &event.body {
            Some(proto::hub_event::Body::MergeMessageBody(merge)) => {
                if let Some(msg) = &merge.message {
                    self.stores.trie.insert(
                        ctx,
                        &self.db,
                        txn_batch,
                        vec![&TrieKey::for_message(&msg)],
                    )?;
                }
                for deleted_message in &merge.deleted_messages {
                    self.stores.trie.delete(
                        ctx,
                        &self.db,
                        txn_batch,
                        vec![&TrieKey::for_message(&deleted_message)],
                    )?;
                }
            }
            Some(proto::hub_event::Body::MergeOnChainEventBody(merge)) => {
                if let Some(onchain_event) = &merge.on_chain_event {
                    self.stores.trie.insert(
                        ctx,
                        &self.db,
                        txn_batch,
                        vec![&TrieKey::for_onchain_event(&onchain_event)],
                    )?;
                }
            }
            Some(proto::hub_event::Body::PruneMessageBody(prune)) => {
                if let Some(msg) = &prune.message {
                    self.stores.trie.delete(
                        ctx,
                        &self.db,
                        txn_batch,
                        vec![&TrieKey::for_message(&msg)],
                    )?;
                }
            }
            Some(proto::hub_event::Body::RevokeMessageBody(revoke)) => {
                if let Some(msg) = &revoke.message {
                    self.stores.trie.delete(
                        ctx,
                        &self.db,
                        txn_batch,
                        vec![&TrieKey::for_message(&msg)],
                    )?;
                }
            }
            Some(proto::hub_event::Body::MergeUsernameProofBody(merge)) => {
                if let Some(msg) = &merge.username_proof_message {
                    self.stores.trie.insert(
                        ctx,
                        &self.db,
                        txn_batch,
                        vec![&TrieKey::for_message(&msg)],
                    )?;
                }
                if let Some(msg) = &merge.deleted_username_proof_message {
                    self.stores.trie.delete(
                        ctx,
                        &self.db,
                        txn_batch,
                        vec![&TrieKey::for_message(&msg)],
                    )?;
                }
                if let Some(proof) = &merge.username_proof {
                    if proof.r#type == proto::UserNameType::UsernameTypeFname as i32
                        && proof.fid != 0
                    // Deletes should not be added to the trie
                    {
                        let name = str::from_utf8(&proof.name).unwrap().to_string();
                        self.stores.trie.insert(
                            ctx,
                            &self.db,
                            txn_batch,
                            vec![&TrieKey::for_fname(proof.fid, &name)],
                        )?;
                    }
                }
                if let Some(proof) = &merge.deleted_username_proof {
                    if proof.r#type == proto::UserNameType::UsernameTypeFname as i32 {
                        let name = str::from_utf8(&proof.name).unwrap().to_string();
                        self.stores.trie.delete(
                            ctx,
                            &self.db,
                            txn_batch,
                            vec![&TrieKey::for_fname(proof.fid, &name)],
                        )?;
                    }
                }
            }
            Some(proto::hub_event::Body::MergeFailure(_)) => {
                // Merge failures don't affect the trie. They are only for event subscribers
            }
            &None => {
                // This should never happen
                panic!("No body in event");
            }
        }
        let elapsed = now.elapsed();
        self.time_with_shard("update_trie_time_us", elapsed.as_micros() as u64);
        Ok(())
    }

    pub(crate) fn validate_user_message(
        &self,
        message: &proto::Message,
        txn_batch: &mut RocksDbTransactionBatch,
    ) -> Result<(), MessageValidationError> {
        let now = std::time::Instant::now();
        // Ensure message data is present
        let message_data = message
            .data
            .as_ref()
            .ok_or(MessageValidationError::NoMessageData)?;

        validations::message::validate_message(message, self.network)?;

        // Check that the user has a custody address
        self.stores
            .onchain_event_store
            .get_id_register_event_by_fid(message_data.fid)
            .map_err(|_| MessageValidationError::MissingFid)?
            .ok_or(MessageValidationError::MissingFid)?;

        // Check that signer is valid
        self.stores
            .onchain_event_store
            .get_active_signer(message_data.fid, message.signer.clone())
            .map_err(|_| MessageValidationError::MissingSigner)?
            .ok_or(MessageValidationError::MissingSigner)?;

        // State-dependent verifications:
        match &message_data.body {
            Some(proto::message_data::Body::UserDataBody(user_data)) => {
                if user_data.r#type == proto::UserDataType::Username as i32 {
                    self.validate_username(message_data.fid, &user_data.value, txn_batch)?;
                }
            }
            _ => {}
        }

        let elapsed = now.elapsed();
        self.time_with_shard("validate_user_message_time_us", elapsed.as_micros() as u64);
        Ok(())
    }

    fn get_username_proof(
        &self,
        name: String,
        txn: &mut RocksDbTransactionBatch,
    ) -> Result<Option<UserNameProof>, MessageValidationError> {
        // TODO(aditi): The fnames proofs should live in the username proof store.
        if name.ends_with(".eth") {
            let proof_message = UsernameProofStore::get_username_proof(
                &self.stores.username_proof_store,
                &name.as_bytes().to_vec(),
                UserNameType::UsernameTypeEnsL1 as u8,
            )
            .map_err(|e| MessageValidationError::StoreError(e))?;
            match proof_message {
                Some(message) => match message.data {
                    None => Ok(None),
                    Some(message_data) => match message_data.body {
                        Some(body) => match body {
                            proto::message_data::Body::UsernameProofBody(user_name_proof) => {
                                Ok(Some(user_name_proof))
                            }
                            _ => Ok(None),
                        },
                        None => Ok(None),
                    },
                },
                None => Ok(None),
            }
        } else {
            UserDataStore::get_username_proof(&self.stores.user_data_store, txn, name.as_bytes())
                .map_err(|e| MessageValidationError::StoreError(e))
        }
    }

    fn validate_username(
        &self,
        fid: u64,
        name: &str,
        txn: &mut RocksDbTransactionBatch,
    ) -> Result<(), MessageValidationError> {
        if name.is_empty() {
            // Setting an empty username is allowed, no need to validate the proof
            return Ok(());
        }
        let name = name.to_string();
        // TODO: validate fname string

        let proof = self.get_username_proof(name.clone(), txn)?;
        match proof {
            Some(proof) => {
                if proof.fid != fid {
                    return Err(MessageValidationError::MissingFname);
                }

                if name.ends_with(".eth") {
                    // TODO: Validate ens names
                } else {
                }
            }
            None => {
                return Err(MessageValidationError::MissingFname);
            }
        }
        Ok(())
    }

    pub fn validate_state_change(&mut self, shard_state_change: &ShardStateChange) -> bool {
        let mut txn = RocksDbTransactionBatch::new();

        let now = std::time::Instant::now();
        let transactions = &shard_state_change.transactions;
        let shard_root = &shard_state_change.new_state_root;
        // We ignore the events here, we don't know what they are yet. If state roots match, the events should match

        let mut result = true;

        let count_fn = Self::make_count_fn(self.statsd_client.clone(), self.shard_id);
        let count_callback = move |read_count: (u64, u64)| {
            count_fn("trie.db_get_count.total", read_count.0);
            count_fn("trie.db_get_count.for_validate", read_count.0);
            count_fn("trie.mem_get_count.total", read_count.1);
            count_fn("trie.mem_get_count.for_validate", read_count.1);
        };

        let proposal_result = self.replay_proposal(
            &merkle_trie::Context::with_callback(count_callback),
            &mut txn,
            transactions,
            shard_root,
            ProposalSource::Validate,
        );

        match proposal_result {
            Err(err) => {
                error!("State change validation failed: {}", err);
                result = false;
            }
            Ok(events) => {
                self.pending_txn = Some(CachedTransaction {
                    shard_root: shard_root.clone(),
                    txn,
                    events,
                });
            }
        }

        self.stores.trie.reload(&self.db).unwrap();
        let elapsed = now.elapsed();
        self.time_with_shard("validate_time", elapsed.as_millis() as u64);

        if result {
            self.count("validate.true", 1);
            self.count("validate.false", 0);
        } else {
            self.count("validate.false", 1);
            self.count("validate.true", 0);
        }

        result
    }

    pub fn commit_and_emit_events(
        &mut self,
        shard_chunk: &ShardChunk,
        events: Vec<HubEvent>,
        txn: RocksDbTransactionBatch,
    ) {
        let now = std::time::Instant::now();
        self.db.commit(txn).unwrap();
        for mut event in events {
            event.timestamp = shard_chunk.header.as_ref().unwrap().timestamp;
            // An error here just means there are no active receivers, which is fine and will happen if there are no active subscribe rpcs
            let _ = self.senders.events_tx.send(event);
        }
        self.stores.trie.reload(&self.db).unwrap();

        _ = self.emit_commit_metrics(&shard_chunk);

        match self.stores.shard_store.put_shard_chunk(shard_chunk) {
            Err(err) => {
                error!("Unable to write shard chunk to store {}", err)
            }
            Ok(()) => {}
        }
        let elapsed = now.elapsed();
        self.time_with_shard("commit_time", elapsed.as_millis() as u64);
    }

    fn emit_commit_metrics(&mut self, shard_chunk: &&ShardChunk) -> Result<(), EngineError> {
        self.count("commit.invoked", 1);

        let block_number = &shard_chunk
            .header
            .as_ref()
            .unwrap()
            .height
            .unwrap()
            .block_number;

        self.gauge("block_height", *block_number);

        let trie_size = self.stores.trie.items()?;
        self.gauge("trie.num_items", trie_size as u64);

        let counts = Self::txn_counts(&shard_chunk.transactions);

        self.count("commit.transactions", counts.transactions);
        self.count("commit.user_messages", counts.user_messages);
        self.count("commit.system_messages", counts.system_messages);

        // useful to see on perf test dashboards
        self.gauge(
            "trie.branching_factor",
            self.stores.trie.branching_factor() as u64,
        );
        self.gauge("max_messages_per_block", self.max_messages_per_block as u64);

        Ok(())
    }

    pub fn make_count_fn(statsd_client: StatsdClientWrapper, shard_id: u32) -> impl Fn(&str, u64) {
        move |key: &str, count: u64| {
            let key = format!("engine.{}", key);
            statsd_client.count_with_shard(shard_id, &key, count);
        }
    }

    pub fn commit_shard_chunk(&mut self, shard_chunk: &ShardChunk) {
        let mut txn = RocksDbTransactionBatch::new();

        let shard_root = &shard_chunk.header.as_ref().unwrap().shard_root;
        let transactions = &shard_chunk.transactions;

        let count_fn = Self::make_count_fn(self.statsd_client.clone(), self.shard_id);
        let count_callback = move |read_count: (u64, u64)| {
            count_fn("trie.db_get_count.total", read_count.0);
            count_fn("trie.db_get_count.for_commit", read_count.0);
            count_fn("trie.mem_get_count.total", read_count.1);
            count_fn("trie.mem_get_count.for_commit", read_count.1);
        };
        let trie_ctx = &merkle_trie::Context::with_callback(count_callback);

        let mut applied_cached_txn = false;
        if let Some(cached_txn) = self.pending_txn.clone() {
            if &cached_txn.shard_root == shard_root {
                applied_cached_txn = true;
                self.commit_and_emit_events(shard_chunk, cached_txn.events, cached_txn.txn);
            } else {
                error!(
                    shard_id = self.shard_id,
                    cached_shard_root = hex::encode(&cached_txn.shard_root),
                    commit_shard_root = hex::encode(shard_root),
                    "Cached shard root mismatch"
                );
            }
        }

        if !applied_cached_txn {
            warn!(
                shard_id = self.shard_id,
                shard_root = hex::encode(shard_root),
                "No valid cached transaction to apply. Replaying proposal"
            );
            // If we need to replay, reset the sequence number on the event id generator, just in case
            let block_number = &shard_chunk
                .header
                .as_ref()
                .unwrap()
                .height
                .unwrap()
                .block_number;
            self.stores.event_handler.set_current_height(*block_number);
            match self.replay_proposal(
                trie_ctx,
                &mut txn,
                transactions,
                shard_root,
                ProposalSource::Commit,
            ) {
                Err(err) => {
                    error!("State change commit failed: {}", err);
                    panic!("State change commit failed: {}", err);
                }
                Ok(events) => {
                    self.commit_and_emit_events(shard_chunk, events, txn);
                }
            }
        }
    }

    pub fn simulate_message(
        &mut self,
        message: &proto::Message,
    ) -> Result<(), MessageValidationError> {
        let mut txn = RocksDbTransactionBatch::new();
        let snapchain_txn = Transaction {
            fid: message.fid() as u64,
            account_root: vec![],
            system_messages: vec![],
            user_messages: vec![message.clone()],
        };
        let result = self.replay_snapchain_txn(
            &merkle_trie::Context::new(),
            &snapchain_txn,
            &mut txn,
            ProposalSource::Simulate,
        );

        match result {
            Ok((_, _, errors)) => {
                self.stores.trie.reload(&self.db).map_err(|e| {
                    MessageValidationError::StoreError(HubError::invalid_internal_state(
                        &*e.to_string(),
                    ))
                })?;
                if !errors.is_empty() {
                    return Err(errors[0].clone());
                } else {
                    Ok(())
                }
            }
            Err(err) => {
                error!("Error simulating message: {:?}", err);
                Err(MessageValidationError::StoreError(
                    HubError::invalid_internal_state(&*err.to_string()),
                ))
            }
        }
    }

    pub fn trie_key_exists(&mut self, ctx: &merkle_trie::Context, sync_id: &Vec<u8>) -> bool {
        self.stores
            .trie
            .exists(ctx, &self.db, sync_id.as_ref())
            .unwrap_or_else(|err| {
                error!("Error checking if sync id exists: {:?}", err);
                false
            })
    }

    pub fn get_min_height(&self) -> Height {
        match self.stores.shard_store.min_block_number() {
            Ok(block_num) => Height::new(self.shard_id, block_num),
            // In case of no blocks, return height 1
            Err(_) => Height::new(self.shard_id, 1),
        }
    }

    pub fn get_confirmed_height(&self) -> Height {
        match self.stores.shard_store.max_block_number() {
            Ok(block_num) => Height::new(self.shard_id, block_num),
            Err(_) => Height::new(self.shard_id, 0),
        }
    }

    pub fn get_last_shard_chunk(&self) -> Option<ShardChunk> {
        match self.stores.shard_store.get_last_shard_chunk() {
            Ok(shard_chunk) => shard_chunk,
            Err(err) => {
                error!("Unable to obtain last shard chunk {:#?}", err);
                None
            }
        }
    }

    pub fn get_shard_chunk_by_height(&self, height: Height) -> Option<ShardChunk> {
        if self.shard_id != height.shard_index {
            error!(
                shard_id = self.shard_id,
                requested_shard_id = height.shard_index,
                "Requested shard chunk from incorrect shard"
            );
            return None;
        }
        self.stores
            .shard_store
            .get_chunk_by_height(height.block_number)
            .unwrap_or_else(|err| {
                error!("No shard chunk at height {:#?}", err);
                None
            })
    }

    pub fn get_casts_by_fid(&self, fid: u64) -> Result<MessagesPage, HubError> {
        CastStore::get_cast_adds_by_fid(&self.stores.cast_store, fid, &PageOptions::default())
    }

    pub fn get_links_by_fid(&self, fid: u64) -> Result<MessagesPage, HubError> {
        self.stores
            .link_store
            .get_adds_by_fid::<fn(&proto::Message) -> bool>(fid, &PageOptions::default(), None)
    }

    pub fn get_link_compact_state_messages_by_fid(
        &self,
        fid: u64,
    ) -> Result<MessagesPage, HubError> {
        self.stores
            .link_store
            .get_compact_state_messages_by_fid(fid, &PageOptions::default())
    }

    pub fn get_reactions_by_fid(&self, fid: u64) -> Result<MessagesPage, HubError> {
        self.stores
            .reaction_store
            .get_adds_by_fid::<fn(&proto::Message) -> bool>(fid, &PageOptions::default(), None)
    }

    pub fn get_user_data_by_fid(&self, fid: u64) -> Result<MessagesPage, HubError> {
        self.stores
            .user_data_store
            .get_adds_by_fid::<fn(&proto::Message) -> bool>(fid, &PageOptions::default(), None)
    }

    pub fn get_user_data_by_fid_and_type(
        &self,
        fid: u64,
        user_data_type: proto::UserDataType,
    ) -> Result<proto::Message, HubError> {
        UserDataStore::get_user_data_by_fid_and_type(
            &self.stores.user_data_store,
            fid,
            user_data_type,
        )
    }

    pub fn get_verifications_by_fid(&self, fid: u64) -> Result<MessagesPage, HubError> {
        self.stores
            .verification_store
            .get_adds_by_fid::<fn(&proto::Message) -> bool>(fid, &PageOptions::default(), None)
    }

    pub fn get_username_proofs_by_fid(&self, fid: u64) -> Result<MessagesPage, HubError> {
        self.stores
            .username_proof_store
            .get_adds_by_fid::<fn(&proto::Message) -> bool>(fid, &PageOptions::default(), None)
    }

    pub fn get_fname_proof(&self, name: &String) -> Result<Option<UserNameProof>, HubError> {
        UserDataStore::get_username_proof(
            &self.stores.user_data_store,
            &mut RocksDbTransactionBatch::new(),
            name.as_bytes(),
        )
    }

    pub fn get_onchain_events(
        &self,
        event_type: OnChainEventType,
        fid: u64,
    ) -> Result<Vec<OnChainEvent>, OnchainEventStorageError> {
        self.stores
            .onchain_event_store
            .get_onchain_events(event_type, Some(fid))
    }

    fn txn_counts(txns: &[Transaction]) -> TransactionCounts {
        let (user_count, system_count) =
            txns.iter().fold((0, 0), |(user_count, system_count), tx| {
                (
                    user_count + tx.user_messages.len(),
                    system_count + tx.system_messages.len(),
                )
            });

        TransactionCounts {
            transactions: txns.len() as u64,
            user_messages: user_count as u64,
            system_messages: system_count as u64,
        }
    }

    pub fn trie_num_items(&mut self) -> usize {
        self.stores.trie.items().unwrap()
    }
}

pub struct BlockEngine {
    block_store: BlockStore,
    statsd_client: StatsdClientWrapper,
}

impl BlockEngine {
    pub fn new(block_store: BlockStore, statsd_client: StatsdClientWrapper) -> Self {
        BlockEngine {
            block_store,
            statsd_client,
        }
    }

    // statsd
    fn count(&self, key: &str, count: u64) {
        let key = format!("engine.{}", key);
        self.statsd_client.count_with_shard(0, key.as_str(), count);
    }

    // statsd
    fn gauge(&self, key: &str, value: u64) {
        let key = format!("engine.{}", key);
        self.statsd_client.gauge_with_shard(0, key.as_str(), value);
    }

    pub fn commit_block(&mut self, block: &Block) {
        self.gauge(
            "block_height",
            block
                .header
                .as_ref()
                .unwrap()
                .height
                .as_ref()
                .unwrap()
                .block_number,
        );
        self.count(
            "block_shards",
            block
                .shard_witness
                .as_ref()
                .unwrap()
                .shard_chunk_witnesses
                .len() as u64,
        );

        let result = self.block_store.put_block(block);
        if result.is_err() {
            error!("Failed to store block: {:?}", result.err());
        }
    }

    pub fn get_last_block(&self) -> Option<Block> {
        match self.block_store.get_last_block() {
            Ok(block) => block,
            Err(err) => {
                error!("Unable to obtain last block {:#?}", err);
                None
            }
        }
    }

    pub fn get_block_by_height(&self, height: Height) -> Option<Block> {
        if height.shard_index != 0 {
            error!(
                shard_id = 0,
                requested_shard_id = height.shard_index,
                "Requested shard chunk from incorrect shard"
            );

            return None;
        }
        match self.block_store.get_block_by_height(height.block_number) {
            Ok(block) => block,
            Err(err) => {
                error!("No block at height {:#?}", err);
                None
            }
        }
    }

    pub fn get_confirmed_height(&self) -> Height {
        let shard_index = 0;
        match self.block_store.max_block_number() {
            Ok(block_num) => Height::new(shard_index, block_num),
            Err(_) => Height::new(shard_index, 0),
        }
    }

    pub fn get_min_height(&self) -> Height {
        let shard_index = 0;
        match self.block_store.min_block_number() {
            Ok(block_num) => Height::new(shard_index, block_num),
            // In case of no blocks, return height 1
            Err(_) => Height::new(shard_index, 1),
        }
    }
}
