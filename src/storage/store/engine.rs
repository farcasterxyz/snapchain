use super::account::{IntoU8, OnchainEventStorageError, UserDataStore, UsernameProofStore};
use crate::consensus::proposer::ProposalSource;
use crate::core::{
    error::HubError, types::Height, util::FarcasterTime, validations, validations::verification,
};
use crate::mempool::mempool::MempoolMessagesRequest;
use crate::proto::message_data::Body;
use crate::proto::shard_trie_entry_with_message::TrieMessage;
use crate::proto::BlockEvent;
use crate::proto::{
    self, hub_event, FarcasterNetwork, HubEvent, HubEventType, MessageType, OnChainEvent,
    OnChainEventType, Protocol, ShardChunk, ShardTrieEntryWithMessage, Transaction, UserDataType,
    UserNameProof,
};
use crate::storage::db::{PageOptions, RocksDB, RocksDbTransactionBatch};
use crate::storage::store::account::{
    BlockEventStorageError, CastStore, MessagesPage, StorageLendStore, StoreOptions,
    VerificationStore,
};
use crate::storage::store::engine_metrics::Metrics;
use crate::storage::store::mempool_poller::{MempoolMessage, MempoolPoller, MempoolPollerError};
use crate::storage::store::migrations::{MigrationContext, MigrationRunner};
use crate::storage::store::stores::{StoreLimits, Stores};
use crate::storage::trie::{self, merkle_trie};
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use crate::version::version::{EngineVersion, ProtocolFeature};
use alloy_primitives::hex::FromHex;
use alloy_primitives::Address;
use informalsystems_malachitebft_core_types::Round;
use itertools::Itertools;
use merkle_trie::TrieKey;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::str;
use std::string::ToString;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{broadcast, mpsc, oneshot};
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

    #[error(transparent)]
    MergeOnchainEventError(#[from] OnchainEventStorageError),

    #[error(transparent)]
    EngineMessageValidationError(#[from] MessageValidationError),

    #[error(transparent)]
    MempoolPollerError(#[from] MempoolPollerError),

    #[error(transparent)]
    BlockEventStoreError(#[from] BlockEventStorageError),

    #[error("Replicator merge error: {0}")]
    ReplicatorError(String),
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

    #[error("invalid ethereum address")]
    InvalidEthereumAddress,

    #[error("invalid solana address")]
    InvalidSolanaAddress,

    #[error("address is not part of any verification")]
    AddressNotPartOfVerification,

    #[error("invalid seqnum for block event")]
    InvalidSeqnumForBlockEvent,
}

pub struct MergedReplicatorMessage {
    pub fid: u64,
    pub trie_key: Vec<u8>,
    pub hub_event: HubEvent,
}

// Shard state root and the transactions
#[derive(Clone)]
pub struct ShardStateChange {
    pub shard_id: u32,
    pub timestamp: FarcasterTime,
    pub new_state_root: Vec<u8>,
    pub transactions: Vec<Transaction>,
    pub events: Vec<HubEvent>,
    pub version: EngineVersion,
    pub max_block_event_seqnum: u64,
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

#[derive(Debug)]
pub struct PostCommitMessage {
    pub shard_id: u32,
    pub header: proto::ShardHeader,
    pub channel: oneshot::Sender<bool>,
}

#[derive(Clone)]
struct CachedTransaction {
    shard_root: Vec<u8>,
    events: Vec<HubEvent>,
    max_block_event_seqnum: u64,
    txn: RocksDbTransactionBatch,
}

pub struct ShardEngine {
    shard_id: u32,
    pub network: FarcasterNetwork,
    pub db: Arc<RocksDB>,
    senders: Senders,
    stores: Stores,
    metrics: Metrics,
    pending_txn: Option<CachedTransaction>,
    fname_signer_address: Option<alloy_primitives::Address>,
    post_commit_tx: Option<mpsc::Sender<PostCommitMessage>>,
    pub mempool_poller: MempoolPoller,
}

impl ShardEngine {
    pub async fn new(
        db: Arc<RocksDB>,
        network: proto::FarcasterNetwork,
        trie: merkle_trie::MerkleTrie,
        shard_id: u32,
        store_limits: StoreLimits,
        statsd_client: StatsdClientWrapper,
        max_messages_per_block: u32,
        messages_request_tx: Option<mpsc::Sender<MempoolMessagesRequest>>,
        fname_signer_address: Option<alloy_primitives::Address>,
        post_commit_tx: Option<mpsc::Sender<PostCommitMessage>>,
    ) -> Result<ShardEngine, HubError> {
        Self::new_with_opts(
            db,
            network,
            trie,
            shard_id,
            store_limits,
            statsd_client,
            max_messages_per_block,
            messages_request_tx,
            fname_signer_address,
            post_commit_tx,
            StoreOptions::default(),
        )
        .await
    }

    pub async fn new_with_opts(
        db: Arc<RocksDB>,
        network: proto::FarcasterNetwork,
        trie: merkle_trie::MerkleTrie,
        shard_id: u32,
        store_limits: StoreLimits,
        statsd_client: StatsdClientWrapper,
        max_messages_per_block: u32,
        messages_request_tx: Option<mpsc::Sender<MempoolMessagesRequest>>,
        fname_signer_address: Option<alloy_primitives::Address>,
        post_commit_tx: Option<mpsc::Sender<PostCommitMessage>>,
        store_opts: StoreOptions,
    ) -> Result<ShardEngine, HubError> {
        let stores = Stores::new_with_opts(
            db.clone(),
            shard_id,
            trie,
            store_limits,
            network,
            statsd_client.clone(),
            store_opts,
        );

        // No migrations on devnet (during tests)
        if network != proto::FarcasterNetwork::Devnet {
            let migration_context = MigrationContext {
                db: db.clone(),
                stores: stores.clone(),
            };
            let runner = MigrationRunner::new(migration_context);
            let migration_handle = runner.run_pending_migrations().await?;

            // If there are background migrations, we can optionally store the handle
            // for later checking or cancellation if needed
            if let Some(_background_handle) = migration_handle {
                // Background migrations are running, but we don't block startup
                info!(shard_id, "Background migrations started");
            }
        }

        Ok(ShardEngine {
            shard_id,
            network,
            stores,
            mempool_poller: MempoolPoller {
                shard_id,
                max_messages_per_block,
                messages_request_tx,
                network,
                statsd_client: statsd_client.clone(),
            },
            senders: Senders::new(),
            db,
            metrics: Metrics {
                statsd_client,
                shard_id,
            },
            pending_txn: None,
            fname_signer_address,
            post_commit_tx,
        })
    }

    pub fn shard_id(&self) -> u32 {
        self.shard_id
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

    pub fn is_read_only(&self) -> bool {
        // For a read-only node, we will never pull from the mempool, so Engines are constructed without a
        // messages_request_tx for read-only nodes
        self.mempool_poller.for_read_node()
    }

    fn prepare_proposal(
        &mut self,
        trie_ctx: &merkle_trie::Context,
        txn_batch: &mut RocksDbTransactionBatch,
        shard_id: u32,
        messages: Vec<MempoolMessage>,
        timestamp: &FarcasterTime,
    ) -> Result<ShardStateChange, EngineError> {
        self.metrics.count(
            "prepare_proposal.recv_messages",
            messages.len() as u64,
            vec![],
        );

        let version = self.version_for(timestamp);
        let mut snapchain_txns = MempoolPoller::create_transactions_from_mempool(messages)?
            .into_iter()
            .filter_map(|mut transaction| {
                // TODO(aditi): In the future, this seems like it should just be a part of the validation logic in replay_snapchain_txn.
                let pending_onchain_events: Vec<OnChainEvent> = transaction
                    .system_messages
                    .iter()
                    .filter_map(|vm| vm.on_chain_event.clone())
                    .collect();

                let maybe_onchainevents =
                    if version.is_enabled(ProtocolFeature::DependentMessagesInBulkSubmit) {
                        pending_onchain_events.as_slice()
                    } else {
                        &[]
                    };

                let storage_slot = self
                    .stores
                    .get_storage_slot_for_fid(transaction.fid, maybe_onchainevents)
                    .ok()?;

                // Drop events if storage slot is inactive
                if !storage_slot.is_active() {
                    transaction.user_messages = vec![];
                }

                if transaction.system_messages.is_empty() && transaction.user_messages.is_empty() {
                    return None;
                } else {
                    return Some(transaction);
                }
            })
            .collect_vec();

        let mut events = vec![];
        let mut validation_error_count = 0;
        let mut max_block_event_seqnum = self.stores.block_event_store.max_seqnum()?;
        for snapchain_txn in &mut snapchain_txns {
            let (account_root, txn_events, validation_errors, last_block_event_seqnum) = self
                .replay_snapchain_txn(
                    trie_ctx,
                    &snapchain_txn,
                    txn_batch,
                    ProposalSource::Propose,
                    version,
                    timestamp,
                    max_block_event_seqnum,
                )?;
            snapchain_txn.account_root = account_root;
            events.extend(txn_events);
            validation_error_count += validation_errors.len();
            max_block_event_seqnum = last_block_event_seqnum;
        }

        self.metrics
            .publish_transaction_counts(&snapchain_txns, ProposalSource::Propose);

        self.metrics.count(
            "validation_errors",
            validation_error_count as u64,
            Metrics::proposal_source_tags(ProposalSource::Propose),
        );

        let new_root_hash = self.stores.trie.root_hash()?;
        let result = ShardStateChange {
            shard_id,
            timestamp: timestamp.clone(),
            version,
            new_state_root: new_root_hash.clone(),
            transactions: snapchain_txns,
            events,
            max_block_event_seqnum,
        };

        Ok(result)
    }

    pub fn version_for(&self, timestamp: &FarcasterTime) -> EngineVersion {
        EngineVersion::version_for(timestamp, self.network)
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
        timestamp: Option<FarcasterTime>,
    ) -> ShardStateChange {
        let now = std::time::Instant::now();
        let mut txn = RocksDbTransactionBatch::new();

        let count_fn = self.metrics.make_count_fn();
        let count_callback = move |read_count: (u64, u64)| {
            count_fn("trie.db_get_count.total", read_count.0, vec![]);
            count_fn("trie.db_get_count.for_propose", read_count.0, vec![]);
            count_fn("trie.mem_get_count.total", read_count.1, vec![]);
            count_fn("trie.mem_get_count.for_propose", read_count.1, vec![]);
        };
        let timestamp = timestamp.unwrap_or_else(FarcasterTime::current);
        let result = self
            .prepare_proposal(
                &merkle_trie::Context::with_callback(count_callback),
                &mut txn,
                shard,
                messages,
                &timestamp,
            )
            .unwrap(); //TODO: don't unwrap()

        // TODO: this should probably operate automatically via drop trait
        self.stores.trie.reload(&self.db).unwrap();
        self.pending_txn = Some(CachedTransaction {
            shard_root: result.new_state_root.clone(),
            events: result.events.clone(),
            txn,
            max_block_event_seqnum: result.max_block_event_seqnum,
        });

        let proposal_duration = now.elapsed();
        self.metrics
            .time_with_shard("propose_time", proposal_duration.as_millis() as u64);

        self.metrics.count("propose.invoked", 1, vec![]);
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
        version: EngineVersion,
        timestamp: &FarcasterTime,
    ) -> Result<(Vec<HubEvent>, u64), EngineError> {
        let now = std::time::Instant::now();
        let mut events = vec![];
        let mut max_block_event_seqnum = self.stores.block_event_store.max_seqnum()?;
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
            let (account_root, txn_events, _, last_block_event_seqnum) = self
                .replay_snapchain_txn(
                    trie_ctx,
                    snapchain_txn,
                    txn_batch,
                    source.clone(),
                    version,
                    timestamp,
                    max_block_event_seqnum,
                )?;
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
            max_block_event_seqnum = last_block_event_seqnum;
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
        self.metrics
            .time_with_shard("replay_proposal_time", elapsed.as_millis() as u64);
        Ok((events, max_block_event_seqnum))
    }

    pub(crate) fn replay_replicator_message(
        &mut self,
        txn_batch: &mut RocksDbTransactionBatch,
        trie_message: &ShardTrieEntryWithMessage,
    ) -> Result<MergedReplicatorMessage, EngineError> {
        let (hub_event, fid) = match &trie_message.trie_message {
            Some(TrieMessage::UserMessage(m)) => {
                let event = self.merge_message(m, txn_batch)?;
                (event, m.fid())
            }
            Some(TrieMessage::OnChainEvent(oe)) => {
                let event = self
                    .stores
                    .onchain_event_store
                    .merge_onchain_event(oe.clone(), txn_batch)?;

                (event, oe.fid)
            }
            Some(TrieMessage::FnameTransfer(f)) => match &f.proof {
                None => {
                    return Err(EngineError::ReplicatorError(format!(
                        "FnameTransfer proof is missing for fname message {:?}",
                        f
                    )));
                }
                Some(p) => {
                    let event = UserDataStore::merge_username_proof(
                        &self.stores.user_data_store,
                        p,
                        txn_batch,
                    )?;
                    (event, p.fid)
                }
            },
            _ => {
                return Err(EngineError::ReplicatorError(format!(
                    "Unknown trie_message type {:?}",
                    trie_message
                )))
            }
        };

        let (inserts, deletes) = TrieKey::for_hub_event(&hub_event);

        if inserts.len() != 1 {
            return Err(EngineError::ReplicatorError(format!(
                "Message generated incorrect number of inserts. Message:{:?}\nHubEvent {:?}",
                trie_message, hub_event
            )));
        }

        if !deletes.is_empty() {
            warn!(
                "Message generated deletes, ignoring them. Message {:?}\nHubEvent {:?}",
                trie_message, hub_event
            );
        }

        Ok(MergedReplicatorMessage {
            fid,
            trie_key: inserts[0].clone(),
            hub_event,
        })
    }

    // Reset the event id generator to 0. This is used when doing replication, when we're merging in events from another node
    // and not from blocks (so no blockheight to use)
    pub(crate) fn reset_event_id(&mut self) {
        self.stores.event_handler.set_current_height(0);
    }

    pub(crate) fn replay_snapchain_txn(
        &mut self,
        trie_ctx: &merkle_trie::Context,
        snapchain_txn: &Transaction,
        txn_batch: &mut RocksDbTransactionBatch,
        source: ProposalSource,
        version: EngineVersion,
        timestamp: &FarcasterTime,
        mut last_block_event_seqnum: u64,
    ) -> Result<(Vec<u8>, Vec<HubEvent>, Vec<MessageValidationError>, u64), EngineError> {
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

        // Sort system_messages to process OnChainEvents first in their canonical order,
        // followed by FnameTransfers sorted by timestamp.
        let mut sorted_system_messages = snapchain_txn.system_messages.clone();
        sorted_system_messages.sort_by(|a, b| {
            match (&a.on_chain_event, &b.on_chain_event) {
                (Some(event_a), Some(event_b)) => {
                    // Both are OnChainEvents, sort by block_number then log_index.
                    (event_a.block_number, event_a.log_index)
                        .cmp(&(event_b.block_number, event_b.log_index))
                }
                (Some(_), None) => Ordering::Less, // OnChainEvents come before FnameTransfers.
                (None, Some(_)) => Ordering::Greater, // FnameTransfers come after OnChainEvents.
                (None, None) => Ordering::Equal,   // Both are FnameTransfers, sort equal
            }
        });
        for msg in sorted_system_messages {
            if let Some(onchain_event) = &msg.on_chain_event {
                if onchain_event.r#type() == OnChainEventType::EventTypeTierPurchase
                    && !version.is_enabled(ProtocolFeature::FarcasterPro)
                {
                    warn!("Saw tier purchase while feature isn't active");
                    continue;
                }

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
                                if Self::should_revoke_signer(&signer_event, version) {
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
                        match verification::validate_fname_transfer(
                            fname_transfer,
                            self.network,
                            self.fname_signer_address,
                        ) {
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

            if let Some(block_event) = &msg.block_event {
                // TODO(aditi): Validate hash and insert into any other relevant stores and trie if the message is relevant
                if version.is_enabled(ProtocolFeature::ReadDataFromShardZero)
                    && block_event.seqnum() == last_block_event_seqnum + 1
                {
                    if let Err(err) = self
                        .stores
                        .block_event_store
                        .put_block_event(block_event, txn_batch)
                    {
                        error!(
                            seqnum = block_event.seqnum().to_string(),
                            "Error merging block event: {}",
                            err.to_string()
                        );
                    }

                    if version.is_enabled(ProtocolFeature::StorageLending) {
                        // Process storage lend messages from block events
                        match &block_event.data.as_ref().unwrap().body {
                            Some(proto::block_event_data::Body::MergeMessageEventBody(
                                merge_message_event,
                            )) => {
                                if let Some(message) = &merge_message_event.message {
                                    match self.merge_message(&message, txn_batch) {
                                        Ok(hub_event) => {
                                            merged_messages_count += 1;
                                            self.update_trie(trie_ctx, &hub_event, txn_batch)?;
                                            events.push(hub_event);
                                            message_types.insert(message.msg_type());
                                        }
                                        Err(err) => {
                                            if source != ProposalSource::Simulate {
                                                warn!(
                                                    seqnum = block_event.seqnum(),
                                                    "Error merging message from block event: {}",
                                                    err.to_string()
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                            Some(proto::block_event_data::Body::RevokeMessageEventBody(
                                revoke_message_event,
                            )) => {
                                if let Some(message) = &revoke_message_event.message {
                                    match self.stores.revoke_message(&message, txn_batch) {
                                        Ok(hub_event) => {
                                            revoked_messages_count += 1;
                                            self.update_trie(trie_ctx, &hub_event, txn_batch)?;
                                            events.push(hub_event);
                                            message_types.insert(message.msg_type());
                                        }
                                        Err(err) => {
                                            if source != ProposalSource::Simulate {
                                                warn!(
                                                    seqnum = block_event.seqnum(),
                                                    "Error revoking message from block event: {}",
                                                    err.to_string()
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }

                        last_block_event_seqnum += 1;
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

        // Sort the user messages so that all dependent messages are processed in order
        let mut sorted_user_messages = snapchain_txn.user_messages.clone();
        sorted_user_messages.sort_by(|a, b| {
            fn get_message_priority(msg: &proto::Message) -> u8 {
                let msg_type = msg.msg_type();
                match msg_type {
                    MessageType::VerificationAddEthAddress => 1,
                    MessageType::UsernameProof => 2,
                    MessageType::UserDataAdd => {
                        if let Some(Body::UserDataBody(body)) = &msg.data.as_ref().unwrap().body {
                            let user_data_type = body.r#type();
                            if user_data_type == UserDataType::Username
                                || user_data_type == UserDataType::UserDataPrimaryAddressEthereum
                                || user_data_type == UserDataType::UserDataPrimaryAddressSolana
                            {
                                return 3; // Dependent UserData types
                            }
                        }
                        4 // Other UserDataAdd types
                    }
                    _ => 5, // All other message types
                }
            }

            let priority_a = get_message_priority(a);
            let priority_b = get_message_priority(b);

            // Message priority first, then timestamp
            priority_a.cmp(&priority_b).then_with(|| {
                a.data
                    .as_ref()
                    .unwrap()
                    .timestamp
                    .cmp(&b.data.as_ref().unwrap().timestamp)
            })
        });

        for msg in &sorted_user_messages {
            // Errors are validated based on the shard root
            match self.validate_user_message(msg, timestamp, version, txn_batch) {
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
                            let mut merge_failure = HubEvent::from_validation_error(err, msg);
                            let _ = self
                                .stores
                                .event_handler
                                .commit_transaction(txn_batch, &mut merge_failure);
                            events.push(merge_failure);
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
                    let mut merge_failure = HubEvent::from_validation_error(err, msg);
                    let _ = self
                        .stores
                        .event_handler
                        .commit_transaction(txn_batch, &mut merge_failure);
                    events.push(merge_failure);
                }
            }
        }

        for msg_type in message_types {
            let fid = snapchain_txn.fid;
            let result = self.prune_messages(fid, msg_type, txn_batch, &version);
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

        let result = self.handle_delete_side_effects(&version, &events, txn_batch);
        match result {
            Ok(revoke_events) => {
                for event in revoke_events {
                    revoked_messages_count += 1;
                    self.update_trie(trie_ctx, &event, txn_batch)?;
                    events.push(event);
                }
            }
            Err(err) => {
                if source != ProposalSource::Simulate {
                    warn!("Error handling delete side effects: {:?}", err);
                }
            }
        }

        let account_root =
            self.stores
                .trie
                .get_hash(&self.db, txn_batch, &TrieKey::for_fid(snapchain_txn.fid))?;

        debug!(
            shard_id = self.shard_id,
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
            last_block_event_seqnum,
            "Replayed transaction"
        );
        let elapsed = now.elapsed();
        self.metrics
            .time_with_shard("replay_txn_time_us", elapsed.as_micros() as u64);

        // Return the new account root hash
        Ok((
            account_root,
            events,
            validation_errors,
            last_block_event_seqnum,
        ))
    }

    /// Checks if a removed verification was set as the user's primary address and revokes it if necessary.
    ///
    /// When a verification is removed from a user's account, this function ensures that if the
    /// removed address was also set as their primary address (for either Ethereum or Solana),
    /// the primary address setting is also revoked to maintain consistency.
    ///
    fn check_and_revoke_primary_address(
        &mut self,
        fid: u64,
        deleted_verification: &proto::VerificationAddAddressBody,
        txn_batch: &mut RocksDbTransactionBatch,
    ) -> Result<Vec<HubEvent>, MessageValidationError> {
        // Determine protocol, parse address, and get user data type in one match
        let (parsed_address, user_data_type) = match deleted_verification.protocol {
            x if x == proto::Protocol::Ethereum as i32 => (
                Address::from_slice(&deleted_verification.address).to_checksum(None),
                UserDataType::UserDataPrimaryAddressEthereum,
            ),
            x if x == proto::Protocol::Solana as i32 => (
                bs58::encode(&deleted_verification.address).into_string(),
                UserDataType::UserDataPrimaryAddressSolana,
            ),
            _ => return Err(MessageValidationError::AddressNotPartOfVerification),
        };

        // Check if this address is set as the primary address
        let user_data_result = UserDataStore::get_user_data_by_fid_and_type(
            &self.stores.user_data_store,
            fid,
            user_data_type,
        );

        if let Ok(user_data) = user_data_result {
            if let Some(data_ref) = &user_data.data {
                if let Some(Body::UserDataBody(body)) = &data_ref.body {
                    if body.value == parsed_address {
                        let revoke_hub_event =
                            self.stores.user_data_store.revoke(&user_data, txn_batch)?;
                        return Ok(vec![revoke_hub_event]);
                    }
                }
            }
        }
        Ok(vec![])
    }

    /// Takes a list of merge events of type VerificationAddAddress and revokes the user's primary address
    /// if it was deleted as part of a verification remove message.
    fn handle_delete_side_effects(
        &mut self,
        version: &EngineVersion,
        events: &[HubEvent],
        txn_batch: &mut RocksDbTransactionBatch,
    ) -> Result<Vec<HubEvent>, EngineError> {
        if !version.is_enabled(ProtocolFeature::PrimaryAddresses) {
            return Ok(vec![]);
        }

        // Process verification removal hooks
        // Look for any events with deleted verification messages. We use this as a trigger to revoke
        // the user's primary address if it was deleted as part of a verification remove message.
        let mut revoke_events = Vec::new();
        for event in events {
            if let Some(hub_event::Body::MergeMessageBody(merge_body)) = &event.body {
                for deleted_message in &merge_body.deleted_messages {
                    let data = deleted_message
                        .data
                        .as_ref()
                        .ok_or(MessageValidationError::NoMessageData)?;
                    match &data.body {
                        Some(Body::VerificationAddAddressBody(body)) => {
                            let new_revoke_events = self.check_and_revoke_primary_address(
                                deleted_message.fid(),
                                body,
                                txn_batch,
                            )?;
                            revoke_events.extend(new_revoke_events);
                        }
                        _ => continue,
                    }
                }
            }
        }
        Ok(revoke_events)
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
            MessageType::LendStorage => {
                let store = &self.stores.storage_lend_store;
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
        self.metrics
            .time_with_shard("merge_message_time_us", elapsed.as_micros() as u64);
        Ok(event)
    }

    fn prune_messages(
        &mut self,
        fid: u64,
        msg_type: MessageType,
        txn_batch: &mut RocksDbTransactionBatch,
        version: &EngineVersion,
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
            MessageType::UsernameProof => {
                // Usernameproof pruning was missing before this version
                if version.is_enabled(ProtocolFeature::EnsValidation) {
                    let store = &self.stores.username_proof_store;
                    store
                        .prune_messages(fid, current_count, max_count, txn_batch)
                        .map_err(|e| EngineError::StoreError(e))
                } else {
                    Ok(vec![])
                }
            }
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

        self.stores
            .trie
            .update_for_event(ctx, &self.db, event, txn_batch)?;

        let elapsed = now.elapsed();
        self.metrics
            .time_with_shard("update_trie_time_us", elapsed.as_micros() as u64);
        Ok(())
    }

    pub(crate) fn validate_user_message(
        &self,
        message: &proto::Message,
        timestamp: &FarcasterTime,
        version: EngineVersion,
        txn_batch: &RocksDbTransactionBatch,
    ) -> Result<(), MessageValidationError> {
        let now = std::time::Instant::now();

        let txn_batch = if version.is_enabled(ProtocolFeature::DependentMessagesInBulkSubmit) {
            txn_batch
        } else {
            &RocksDbTransactionBatch::new()
        };

        // Ensure message data is present
        let message_data = message
            .data
            .as_ref()
            .ok_or(MessageValidationError::NoMessageData)?;

        let is_pro_user = self
            .stores
            .is_pro_user(message.fid(), timestamp)
            .map_err(|err| HubError::internal_db_error(&err.to_string()))?;
        validations::message::validate_message(
            message,
            self.network,
            is_pro_user,
            timestamp,
            version,
        )?;

        // 1. Check that the user has a custody address
        // If not in the temp cache, fall back to the DB
        self.stores
            .onchain_event_store
            .get_id_register_event_by_fid(message_data.fid, Some(txn_batch))
            .map_err(|_| MessageValidationError::MissingFid)?
            .ok_or(MessageValidationError::MissingFid)?;

        // 2. Check that the user has a valid signer
        self.stores
            .onchain_event_store
            .get_active_signer(message_data.fid, message.signer.clone(), Some(txn_batch))
            .map_err(|_| MessageValidationError::MissingSigner)?
            .ok_or(MessageValidationError::MissingSigner)?;

        // Don't allow storage lends to be merged directly without going through shard 0
        if message_data.r#type() == MessageType::LendStorage {
            return Err(MessageValidationError::InvalidMessageType(
                message_data.r#type,
            ));
        }

        // State-dependent verifications:
        match &message_data.body {
            Some(proto::message_data::Body::UserDataBody(user_data)) => {
                if user_data.r#type == proto::UserDataType::Username as i32 {
                    self.validate_username(message_data.fid, &user_data.value, txn_batch)?;
                }
                if user_data.r#type == proto::UserDataType::UserDataPrimaryAddressEthereum as i32 {
                    self.validate_ethereum_address_ownership(
                        message_data.fid,
                        &user_data.value,
                        txn_batch,
                    )?;
                }
                if user_data.r#type == proto::UserDataType::UserDataPrimaryAddressSolana as i32 {
                    self.validate_solana_address_ownership(
                        message_data.fid,
                        &user_data.value,
                        txn_batch,
                    )?;
                }
            }
            _ => {}
        }

        let elapsed = now.elapsed();
        self.metrics
            .time_with_shard("validate_user_message_time_us", elapsed.as_micros() as u64);
        Ok(())
    }

    fn get_username_proof(
        &self,
        name: String,
        txn: &RocksDbTransactionBatch,
    ) -> Result<Option<UserNameProof>, MessageValidationError> {
        // TODO(aditi): The fnames proofs should live in the username proof store.
        if name.ends_with(".eth") {
            let version = EngineVersion::current(self.network);
            let batch_txn = if version.is_enabled(ProtocolFeature::DependentMessagesInBulkSubmit) {
                txn
            } else {
                &RocksDbTransactionBatch::new()
            };
            let proof_message = UsernameProofStore::get_username_proof(
                &self.stores.username_proof_store,
                &name.as_bytes().to_vec(),
                batch_txn,
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

    fn validate_ethereum_address_ownership(
        &self,
        fid: u64,
        address: &str,
        txn_batch: &RocksDbTransactionBatch,
    ) -> Result<(), MessageValidationError> {
        if address.is_empty() {
            return Ok(());
        }
        // Decode Ethereum address into bytes
        let address_instance: Address = Address::from_hex(address)
            .map_err(|_| MessageValidationError::InvalidEthereumAddress)?;
        self.verify_fid_owns_address(
            fid,
            Protocol::Ethereum,
            address_instance.as_ref(),
            txn_batch,
        )?;
        Ok(())
    }

    fn validate_solana_address_ownership(
        &self,
        fid: u64,
        address: &str,
        txn_batch: &RocksDbTransactionBatch,
    ) -> Result<(), MessageValidationError> {
        if address.is_empty() {
            return Ok(());
        }
        // Decode Solana address from base58 to bytes
        let address_bytes = bs58::decode(address)
            .into_vec()
            .map_err(|_| MessageValidationError::InvalidSolanaAddress)?;
        if address_bytes.len() != 32 {
            return Err(MessageValidationError::InvalidSolanaAddress);
        }
        self.verify_fid_owns_address(fid, Protocol::Solana, &address_bytes, txn_batch)?;
        Ok(())
    }

    fn validate_username(
        &self,
        fid: u64,
        name: &str,
        txn: &RocksDbTransactionBatch,
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

        let count_fn = self.metrics.make_count_fn();
        let count_callback = move |read_count: (u64, u64)| {
            count_fn("trie.db_get_count.total", read_count.0, vec![]);
            count_fn("trie.db_get_count.for_validate", read_count.0, vec![]);
            count_fn("trie.mem_get_count.total", read_count.1, vec![]);
            count_fn("trie.mem_get_count.for_validate", read_count.1, vec![]);
        };

        let proposal_result = self.replay_proposal(
            &merkle_trie::Context::with_callback(count_callback),
            &mut txn,
            transactions,
            shard_root,
            ProposalSource::Validate,
            shard_state_change.version,
            &shard_state_change.timestamp,
        );

        match proposal_result {
            Err(err) => {
                error!("State change validation failed: {}", err);
                result = false;
            }
            Ok((events, max_block_event_seqnum)) => {
                self.pending_txn = Some(CachedTransaction {
                    shard_root: shard_root.clone(),
                    txn,
                    events,
                    max_block_event_seqnum,
                });
            }
        }

        self.stores.trie.reload(&self.db).unwrap();
        let elapsed = now.elapsed();
        self.metrics
            .time_with_shard("validate_time", elapsed.as_millis() as u64);

        if result {
            self.metrics.count("validate.true", 1, vec![]);
            self.metrics.count("validate.false", 0, vec![]);
        } else {
            self.metrics.count("validate.false", 1, vec![]);
            self.metrics.count("validate.true", 0, vec![]);
        }

        result
    }

    fn compute_event_counts_by_type(events: &Vec<HubEvent>) -> HashMap<i32, u64> {
        let mut counts_by_type = HashMap::new();
        for event in events {
            let count = counts_by_type.get(&event.r#type).unwrap_or(&0);
            counts_by_type.insert(event.r#type, count + 1);
        }
        counts_by_type
    }
    pub async fn commit_and_emit_events(
        &mut self,
        shard_chunk: &ShardChunk,
        mut events: Vec<HubEvent>,
        max_block_event_seqnum: u64,
        mut txn: RocksDbTransactionBatch,
    ) {
        let header = shard_chunk.header.as_ref().unwrap();
        let height = header.height.as_ref().unwrap();
        let mut event_counts_by_type = Self::compute_event_counts_by_type(&events);
        event_counts_by_type.insert(HubEventType::BlockConfirmed as i32, 1);
        let mut block_confirmed = HubEvent::from(
            proto::HubEventType::BlockConfirmed,
            proto::hub_event::Body::BlockConfirmedBody(proto::BlockConfirmedBody {
                block_number: height.block_number,
                shard_index: height.shard_index,
                timestamp: header.timestamp,
                block_hash: shard_chunk.hash.clone(),
                total_events: (events.len() + 1) as u64, // +1 for BLOCK_CONFIRMED itself
                event_counts_by_type,
                max_block_event_seqnum,
            }),
        );
        let _block_confirmed_id = self
            .stores
            .event_handler
            .commit_transaction(&mut txn, &mut block_confirmed)
            .unwrap();
        events.insert(0, block_confirmed);

        self.metrics
            .gauge("block_event_seqnum", max_block_event_seqnum);

        _ = self.emit_commit_metrics(&shard_chunk, &events);

        let now = std::time::Instant::now();
        self.db.commit(txn).unwrap();
        for mut event in events {
            event.timestamp = header.timestamp;
            let _ = self.senders.events_tx.send(event);
        }
        self.stores.trie.reload(&self.db).unwrap();

        match self.stores.shard_store.put_shard_chunk(shard_chunk) {
            Err(err) => {
                error!("Unable to write shard chunk to store {}", err)
            }
            Ok(()) => {}
        }
        let elapsed = now.elapsed();
        self.metrics
            .time_with_shard("commit_time", elapsed.as_millis() as u64);
        self.post_commit(shard_chunk.header.as_ref().unwrap().clone())
            .await;
    }

    fn emit_commit_metrics(
        &mut self,
        shard_chunk: &&ShardChunk,
        events: &Vec<HubEvent>,
    ) -> Result<(), EngineError> {
        self.metrics.count("commit.invoked", 1, vec![]);

        let block_number = &shard_chunk
            .header
            .as_ref()
            .unwrap()
            .height
            .unwrap()
            .block_number;

        self.metrics.gauge("block_height", *block_number);
        let block_timestamp = shard_chunk.header.as_ref().unwrap().timestamp;
        self.metrics.gauge(
            "block_delay_seconds",
            FarcasterTime::current().to_u64() - block_timestamp,
        );

        let trie_size = self.stores.trie.items()?;
        self.metrics.gauge("trie.num_items", trie_size as u64);

        self.metrics
            .publish_transaction_counts(&shard_chunk.transactions, ProposalSource::Commit);

        for event in events {
            self.metrics.count(
                "commit.emitted_event",
                1,
                vec![("event_type", &event.r#type.to_string())],
            );
            if event.r#type() == HubEventType::MergeMessage {
                match &event.body {
                    Some(hub_event::Body::MergeMessageBody(body)) => {
                        if let Some(message) = &body.message {
                            self.metrics.count(
                                "commit.merged_message",
                                1,
                                vec![("message_type", &(message.msg_type() as i32).to_string())],
                            )
                        }
                    }
                    _ => {}
                }
            }
        }

        Ok(())
    }

    async fn post_commit(&mut self, header: proto::ShardHeader) {
        match &mut self.post_commit_tx {
            None => return,
            Some(tx) => {
                let now = std::time::Instant::now();

                // Attempt to send the post commit message and wait for a response,
                // but don't block indefinitely in case the receiver is not ready, unavailable,
                // or too slow.

                let (oneshot_tx, oneshot_rx) = oneshot::channel();
                let send_future = tx.send(PostCommitMessage {
                    shard_id: self.shard_id,
                    header: header,
                    channel: oneshot_tx,
                });

                if let Err(err) =
                    tokio::time::timeout(Duration::from_millis(200), send_future).await
                {
                    error!("Post commit hook send failed: {}", err);
                    return;
                }

                // Attempt to wait for the receiver to process the message, but don't block
                // indefinitely in case the receiver is not ready or available.
                match tokio::time::timeout(Duration::from_millis(200), oneshot_rx).await {
                    Ok(Ok(_)) => {} // success
                    Ok(Err(err)) => error!("Post commit hook failed: {}", err),
                    Err(err) => error!("Post commit hook receive failed: {}", err),
                }

                let elapsed = now.elapsed();
                self.metrics
                    .time_with_shard("post_commit_time", elapsed.as_millis() as u64);
            }
        }
    }

    pub async fn commit_shard_chunk(&mut self, shard_chunk: &ShardChunk) {
        let mut txn = RocksDbTransactionBatch::new();

        let shard_root = &shard_chunk.header.as_ref().unwrap().shard_root;
        let transactions = &shard_chunk.transactions;

        let mut applied_cached_txn = false;
        if let Some(cached_txn) = self.pending_txn.clone() {
            if &cached_txn.shard_root == shard_root {
                applied_cached_txn = true;
                self.commit_and_emit_events(
                    shard_chunk,
                    cached_txn.events,
                    cached_txn.max_block_event_seqnum,
                    cached_txn.txn,
                )
                .await;
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
            if !self.is_read_only() {
                warn!(
                    shard_id = self.shard_id,
                    shard_root = hex::encode(shard_root),
                    "No valid cached transaction to apply. Replaying proposal"
                );
            }
            let header = &shard_chunk.header.as_ref().unwrap();
            let block_number = header.height.unwrap().block_number;
            self.stores.event_handler.set_current_height(block_number);
            let version = self.version_for(&FarcasterTime::new(header.timestamp));

            let count_fn = self.metrics.make_count_fn();
            let count_callback = move |read_count: (u64, u64)| {
                count_fn("trie.db_get_count.total", read_count.0, vec![]);
                count_fn("trie.db_get_count.for_commit", read_count.0, vec![]);
                count_fn("trie.mem_get_count.total", read_count.1, vec![]);
                count_fn("trie.mem_get_count.for_commit", read_count.1, vec![]);
            };
            let trie_ctx = &merkle_trie::Context::with_callback(count_callback);

            match self.replay_proposal(
                trie_ctx,
                &mut txn,
                transactions,
                shard_root,
                ProposalSource::Commit,
                version,
                &FarcasterTime::new(header.timestamp),
            ) {
                Err(err) => {
                    error!("State change commit failed: {}", err);
                    panic!("State change commit failed: {}", err);
                }
                Ok((events, max_block_event_seqnum)) => {
                    self.commit_and_emit_events(shard_chunk, events, max_block_event_seqnum, txn)
                        .await;
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
        let version = self.version_for(&FarcasterTime::current());
        let max_block_event_seqnum = self.stores.block_event_store.max_seqnum().unwrap_or(0);
        let result = self.replay_snapchain_txn(
            &merkle_trie::Context::new(),
            &snapchain_txn,
            &mut txn,
            ProposalSource::Simulate,
            version,
            &FarcasterTime::current(),
            max_block_event_seqnum,
        );

        match result {
            Ok((_, _, errors, _)) => {
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

    pub fn simulate_bulk_messages(
        &mut self,
        messages: &[proto::Message],
    ) -> Vec<Result<(), MessageValidationError>> {
        use std::collections::HashMap;

        let mut txn_batch = RocksDbTransactionBatch::new();
        let trie_ctx = merkle_trie::Context::new();
        let version = self.version_for(&FarcasterTime::current());
        let timestamp = FarcasterTime::current();

        // Group messages by FID and track their original positions
        let mut messages_by_fid: HashMap<u64, Vec<(usize, &proto::Message)>> = HashMap::new();
        for (index, message) in messages.iter().enumerate() {
            let fid = message.fid() as u64;
            messages_by_fid
                .entry(fid)
                .or_insert_with(Vec::new)
                .push((index, message));
        }

        // Initialize results vector with placeholder values
        let mut results: Vec<Result<(), MessageValidationError>> = vec![Ok(()); messages.len()];
        let mut max_block_event_seqnum = self.stores.block_event_store.max_seqnum().unwrap_or(0);

        // Process each FID group
        for (fid, fid_messages) in messages_by_fid {
            // Create a single transaction for all messages from this FID
            let user_messages: Vec<proto::Message> =
                fid_messages.iter().map(|(_, msg)| (*msg).clone()).collect();
            let snapchain_txn = Transaction {
                fid,
                account_root: vec![], // Not used for simulation
                system_messages: vec![],
                user_messages,
            };

            // Process the transaction for this FID
            match self.replay_snapchain_txn(
                &trie_ctx,
                &snapchain_txn,
                &mut txn_batch,
                ProposalSource::Simulate,
                version,
                &timestamp,
                max_block_event_seqnum,
            ) {
                Ok((_, _, validation_errors, latest_block_event_seqnum)) => {
                    // If there are any validation errors, assign them to the results
                    if !validation_errors.is_empty() {
                        let error = validation_errors[0].clone();
                        for (original_index, _) in fid_messages {
                            results[original_index] = Err(error.clone());
                        }
                    }
                    max_block_event_seqnum = latest_block_event_seqnum;
                }
                Err(err) => {
                    // If replay_snapchain_txn fails, all messages for this FID fail
                    let error = MessageValidationError::StoreError(
                        HubError::invalid_internal_state(&*err.to_string()),
                    );
                    for (original_index, _) in fid_messages {
                        results[original_index] = Err(error.clone());
                    }
                }
            }
        }

        // The simulation was successful. We must now discard all in-memory changes
        // to the trie and the transaction batch, as we are not committing state here.
        match self.stores.trie.reload(&self.db) {
            Ok(()) => results,
            Err(e) => {
                let trie_error = MessageValidationError::StoreError(
                    HubError::invalid_internal_state(&*e.to_string()),
                );
                // If trie reload fails, return the trie error for all messages
                vec![Err(trie_error); messages.len()]
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

    pub fn get_block_event(
        &self,
        seqnum: u64,
    ) -> Result<Option<BlockEvent>, BlockEventStorageError> {
        self.stores
            .block_event_store
            .get_block_event_by_seqnum(seqnum)
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

    /// Verifies that a FID has an active verification for a specific address and protocol.
    ///
    /// This function checks if a user has previously verified ownership of an address by looking for
    /// existing VerificationAddAddress messages in their verification store. It's used to ensure that
    /// users can only set addresses as primary addresses if they have already proven ownership through
    /// the verification process.
    ///
    /// # Note
    /// Only checks for add messages since CRDT rules ensure that verification remove messages
    /// automatically drop the corresponding add messages from the active state.
    pub fn verify_fid_owns_address(
        &self,
        fid: u64,
        protocol: Protocol,
        address: &[u8],
        txn_batch: &RocksDbTransactionBatch,
    ) -> Result<(), MessageValidationError> {
        let page_result = VerificationStore::get_verification_add(
            &self.stores.verification_store,
            fid,
            address,
            Some(txn_batch),
        )
        .map_err(|_| {
            MessageValidationError::StoreError(HubError::invalid_internal_state(
                "Unable to get verifications by fid",
            ))
        })?;
        match page_result {
            Some(message) => {
                let verification = match &message.data.as_ref().unwrap().body.as_ref().unwrap() {
                    Body::VerificationAddAddressBody(body) => body,
                    _ => unreachable!(),
                };

                if verification.protocol == protocol as i32 {
                    Ok(())
                } else {
                    Err(MessageValidationError::AddressNotPartOfVerification)
                }
            }
            None => {
                return Err(MessageValidationError::AddressNotPartOfVerification);
            }
        }
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

    pub fn trie_num_items(&mut self) -> usize {
        self.stores.trie.items().unwrap()
    }

    fn should_revoke_signer(signer_event: &proto::SignerEventBody, version: EngineVersion) -> bool {
        // When this bug was active, we did not revoke any signers, so, always return false
        if version.is_enabled(ProtocolFeature::SignerRevokeBug) {
            return false;
        }
        signer_event.event_type == proto::SignerEventType::Remove as i32
    }
}
