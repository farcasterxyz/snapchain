use tokio::select;
use tracing::error;

use crate::{
    core::util,
    proto,
    replication::{error::ReplicationError, replication_stores::ReplicationStores},
    storage::{
        db::PageOptions,
        store::{
            account::{self, FIDIterator, Store, StoreDef, UserDataStore, UsernameProofStore},
            engine::PostCommitMessage,
            stores::Stores,
        },
    },
};
use std::{sync::Arc, time::Duration};

pub async fn run(
    replicator: Arc<Replicator>,
    mut receive: tokio::sync::mpsc::Receiver<PostCommitMessage>,
) {
    loop {
        select! {
            Some(msg) = receive.recv() => {
                if let Err(e) = replicator.handle_post_commit_message(&msg) {
                    error!("Failed to handle post commit message: {}", e);
                }

                if let Err(e) = msg.channel.send(true) {
                    error!("Failed to send post commit response: {}", e);
                }
            }
            else => break, // Exit loop if the channel is closed
        }
    }
}

#[derive(Clone)]
pub struct ReplicatorSnapshotOptions {
    pub interval: u64,     // Interval in blocks to take snapshots
    pub max_age: Duration, // Maximum age of snapshots to keep
}

impl Default for ReplicatorSnapshotOptions {
    fn default() -> Self {
        ReplicatorSnapshotOptions {
            interval: 1_000, // Default to taking a snapshot every 1000 blocks
            max_age: Duration::from_secs(60 * 60 * 24 * 7),
        }
    }
}

#[derive(Clone)]
pub struct Replicator {
    stores: Arc<ReplicationStores>,
    snapshot_options: ReplicatorSnapshotOptions,
}

impl Replicator {
    pub fn new(stores: Arc<ReplicationStores>) -> Self {
        Self::new_with_options(stores, ReplicatorSnapshotOptions::default())
    }

    pub fn new_with_options(
        stores: Arc<ReplicationStores>,
        snapshot_options: ReplicatorSnapshotOptions,
    ) -> Self {
        if snapshot_options.interval == 0 {
            panic!("Snapshot interval cannot be zero");
        }

        Replicator {
            stores,
            snapshot_options,
        }
    }

    fn transactions_for_fid(
        &self,
        height: u64,
        shard: u32,
        fid: u64,
    ) -> Result<Option<proto::Transaction>, ReplicationError> {
        let stores = match self.stores.get(shard, height) {
            Some(stores) => stores,
            None => {
                return Err(ReplicationError::StoreNotFound(
                    shard,
                    height,
                    "No stores found for the given height and shard".to_string(),
                ));
            }
        };

        // Use an arbitrarily large limit to ensure we fetch all messages for the fid
        let mut cursor = Cursor::new_for_fid(fid, 1_000_000_000);
        build_transaction_for_fid(&stores, &mut cursor)
    }

    pub fn latest_transactions_for_fid(
        &self,
        shard: u32,
        fid: u64,
    ) -> Result<Option<proto::Transaction>, ReplicationError> {
        let height = self.stores.max_height_for_shard(shard).ok_or_else(|| {
            ReplicationError::StoreNotFound(
                shard,
                0,
                "No stores found for the given shard".to_string(),
            )
        })?;

        self.transactions_for_fid(height, shard, fid)
    }

    pub fn transactions_for_shard_and_height(
        &self,
        shard: u32,
        height: u64,
        page_token: Option<Vec<u8>>,
        message_limit: usize,
    ) -> Result<(Vec<proto::Transaction>, Option<Vec<u8>>), ReplicationError> {
        let stores = match self.stores.get(shard, height) {
            Some(stores) => stores,
            None => {
                return Err(ReplicationError::StoreNotFound(
                    shard,
                    height,
                    "No stores found for the given height and shard".to_string(),
                ));
            }
        };

        let mut cursor = match page_token {
            Some(token) => Cursor::new(Token::new_raw(token), message_limit),
            None => Cursor::new_for_fid(0, message_limit),
        };

        let iterator_fid = cursor.token.fid().saturating_sub(1);
        let fid_iterator = FIDIterator::new(stores.db.clone(), iterator_fid);
        let mut transactions = vec![];

        for fid in fid_iterator.into_iter() {
            if cursor.token.fid() != fid {
                cursor.token = Token::new_for_fid(fid);
            }

            let transaction = build_transaction_for_fid(&stores, &mut cursor)?;
            if let Some(tx) = transaction {
                transactions.push(tx);
            }

            if cursor.limit == 0 {
                return Ok((transactions, Some(cursor.token.into())));
            }
        }

        Ok((transactions, None))
    }

    pub fn get_snapshot_metadata(&self, shard: u32) -> Result<Vec<(u64, u64)>, ReplicationError> {
        self.stores.get_metadata(shard)
    }

    // Calculates the oldest timestamp that is still valid for snapshots.
    fn oldest_valid_timestamp(&self) -> Result<u64, ReplicationError> {
        let current_time = match util::get_farcaster_time() {
            Ok(time) => time,
            Err(e) => {
                return Err(ReplicationError::InternalError(format!(
                    "Failed to get current Farcaster time: {}",
                    e
                )));
            }
        };

        let oldest_timestamp = current_time.saturating_sub(self.snapshot_options.max_age.as_secs());
        Ok(oldest_timestamp)
    }

    pub fn handle_post_commit_message(
        &self,
        msg: &PostCommitMessage,
    ) -> Result<(), ReplicationError> {
        let block_number = match msg.header.height {
            Some(height) => height.block_number,
            None => {
                return Err(ReplicationError::InvalidMessage(
                    "PostCommitMessage must contain a block number".to_string(),
                ));
            }
        };

        let timestamp = msg.header.timestamp;
        let oldest_valid_timestamp = self.oldest_valid_timestamp()?;

        // Clean up old snapshots
        self.stores
            .close_aged_snapshots(msg.shard_id, oldest_valid_timestamp);

        // Check if we can take a snapshot of this block
        if block_number > 0 && block_number % self.snapshot_options.interval != 0 {
            return Ok(());
        }

        // Check if the timestamp is expired
        if timestamp < oldest_valid_timestamp {
            return Ok(());
        }

        // Open a snapshot
        self.stores
            .open_snapshot(msg.shard_id, block_number, timestamp)
    }
}

#[derive(PartialEq)]
enum MessageType {
    OnchainEventsSigner = 1,
    OnchainEventsSignerMigrated = 2,
    OnchainEventsIdRegister = 3,
    OnchainEventsStorageRent = 4,
    OnchainEventsTierPurchase = 5,
    UsernameProofs = 6,
    UsernameProof = 7,
    CastMessages = 8,
    LinkCompactState = 9,
    LinkMessages = 10,
    ReactionMessages = 11,
    UserDataMessages = 12,
    VerificationMessages = 13,
    UsernameProofsMessages = 14,
}

impl TryFrom<u8> for MessageType {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, String> {
        match value {
            1 => Ok(MessageType::OnchainEventsSigner),
            2 => Ok(MessageType::OnchainEventsSignerMigrated),
            3 => Ok(MessageType::OnchainEventsIdRegister),
            4 => Ok(MessageType::OnchainEventsStorageRent),
            5 => Ok(MessageType::OnchainEventsTierPurchase),
            6 => Ok(MessageType::UsernameProofs),
            7 => Ok(MessageType::UsernameProof),
            8 => Ok(MessageType::CastMessages),
            9 => Ok(MessageType::LinkCompactState),
            10 => Ok(MessageType::LinkMessages),
            11 => Ok(MessageType::ReactionMessages),
            12 => Ok(MessageType::UserDataMessages),
            13 => Ok(MessageType::VerificationMessages),
            14 => Ok(MessageType::UsernameProofsMessages),
            _ => Err(format!("Unknown message type: {}", value)),
        }
    }
}

struct Token {
    // Cursor token format [fid, message_type, cursor]
    // 8 bytes for fid
    // 1 byte for message type
    // ... variable length for cursor
    inner: Vec<u8>,
}

impl Token {
    fn new_for_fid(fid: u64) -> Self {
        let mut token = Vec::with_capacity(8);
        token.extend_from_slice(&fid.to_be_bytes());
        Token { inner: token }
    }

    fn new_with_message_type(fid: u64, message_type: MessageType) -> Self {
        let mut token = Vec::with_capacity(8 + 1);
        token.extend_from_slice(&fid.to_be_bytes());
        token.push(message_type as u8);
        Token { inner: token }
    }

    fn new(fid: u64, message_type: MessageType, cursor: Vec<u8>) -> Self {
        let mut token = Vec::with_capacity(8 + 1 + cursor.len());
        token.extend_from_slice(&fid.to_be_bytes());
        token.push(message_type as u8);
        token.extend_from_slice(&cursor);
        Token { inner: token }
    }

    fn new_raw(inner: Vec<u8>) -> Self {
        Token { inner }
    }

    fn fid(&self) -> u64 {
        let mut buf = [0; 8];
        buf.copy_from_slice(&self.inner[..8]);
        u64::from_be_bytes(buf)
    }

    fn message_type(&self) -> Option<MessageType> {
        if self.inner.len() < 9 {
            return None;
        }

        let message_type_byte = self.inner[8];
        match MessageType::try_from(message_type_byte) {
            Ok(message_type) => Some(message_type),
            Err(_) => None,
        }
    }

    fn page_token(&self) -> Option<Vec<u8>> {
        if self.inner.len() <= 9 {
            return None;
        }

        Some(self.inner[9..].to_vec())
    }
}

impl From<Token> for Vec<u8> {
    fn from(token: Token) -> Self {
        token.inner
    }
}

struct Cursor {
    token: Token,
    limit: usize,
}

impl Cursor {
    fn new_for_fid(fid: u64, limit: usize) -> Self {
        Self::new(Token::new_for_fid(fid), limit)
    }

    fn new(token: Token, limit: usize) -> Self {
        Cursor { token, limit }
    }
}

// collect_messages_with_cursor is a generic function that is utilized to support the pagination
// and collection of messages (generic type T) from a store using a cursor. It handles all the
// pagination logic, including checking the cursor token, fetching results, and updating the
// cursor for all the various types of messages that need to be queried as part of the replication
// process.
//
// The function takes a closure `f` that is responsible for fetching the messages from the store.
// It is expected to return a tuple containing the fetched messages and an optional next page
// token.
fn collect_messages_with_cursor<
    T,
    E: std::fmt::Display,
    F: Fn(&PageOptions, &mut Cursor) -> Result<(Vec<T>, Option<Vec<u8>>), E>, // (results, next_page_token)
>(
    cursor: &mut Cursor,
    message_type: MessageType,
    f: F,
) -> Result<Vec<T>, ReplicationError> {
    const PAGE_SIZE: usize = 1_000;

    let mut page_options = PageOptions::default();

    // Handle the cursor/token
    match cursor.token.message_type() {
        // When it is none, we're landing here for the first time and should
        // return results for the given message type.
        None => {
            // If the cursor limit is 0, update the token to reflect the message type
            // so that we can continue fetching results from here on a subsequent call.
            if cursor.limit == 0 {
                cursor.token = Token::new_with_message_type(cursor.token.fid(), message_type);
                return Ok(vec![]);
            }
        }

        // When the cursor token is for this message type, set the page token to continue
        // fetching results from where we left off.
        Some(cursor_message_type) if message_type == cursor_message_type => {
            // This shouldn't happen in practice, but if the limit is 0, we
            // can return an empty vector immediately as the cursor is already set to
            // return here on a subsequent call.
            if cursor.limit == 0 {
                return Ok(vec![]);
            }

            page_options.page_token = cursor.token.page_token();
        }

        // When the cursor token is for a different message type, we should
        // exit early and not fetch any results
        Some(_other) => {
            return Ok(vec![]);
        }
    }

    let mut messages = vec![];

    loop {
        // Only attempt to fetch up to the remaining limit or PAGE_SIZE, whichever is smaller
        page_options.page_size = Some(std::cmp::min(cursor.limit, PAGE_SIZE));

        let (results, next_page_token) = match f(&page_options, cursor) {
            Ok(r) => r,
            Err(e) => {
                return Err(ReplicationError::InternalError(format!(
                    "Failed to fetch data: {}",
                    e
                )));
            }
        };

        if results.is_empty() {
            // All results have been fetched, clear out the token
            cursor.token = Token::new_for_fid(cursor.token.fid());
            break;
        }

        // Decrement the cursor limit by the number of results fetched
        cursor.limit = cursor.limit.saturating_sub(results.len());
        messages.extend(results);

        if next_page_token.is_none() {
            // All results have been fetched, clear out the token
            cursor.token = Token::new_for_fid(cursor.token.fid());
            break;
        }

        // If we have reached the limit, set the token so that we can continue
        // fetching results from here on a subsequent call.
        if cursor.limit <= 0 {
            cursor.token = Token::new(
                cursor.token.fid(),
                message_type,
                next_page_token.clone().unwrap_or_default(),
            );
            break;
        }

        page_options.page_token = next_page_token;
    }

    Ok(messages)
}

fn collect_messages<T: StoreDef + Clone>(
    store: &Store<T>,
    cursor: &mut Cursor,
    message_type: MessageType,
) -> Result<Vec<proto::Message>, ReplicationError> {
    collect_messages_with_cursor(cursor, message_type, |page_options, cursor| {
        match store.get_all_messages_by_fid(cursor.token.fid(), None, None, page_options) {
            Ok(results) => Ok((results.messages, results.next_page_token)),
            Err(e) => Err(ReplicationError::InternalError(format!(
                "Failed to fetch messages: {}",
                e
            ))),
        }
    })
}

fn collect_compact_state<T: StoreDef + Clone>(
    store: &Store<T>,
    cursor: &mut Cursor,
    message_type: MessageType,
) -> Result<Vec<proto::Message>, ReplicationError> {
    collect_messages_with_cursor(cursor, message_type, |page_options, cursor| {
        match store.get_compact_state_messages_by_fid(cursor.token.fid(), page_options) {
            Ok(results) => Ok((results.messages, results.next_page_token)),
            Err(e) => Err(ReplicationError::InternalError(format!(
                "Failed to fetch compact state messages: {}",
                e
            ))),
        }
    })
}

fn collect_onchain_events_with_cursor(
    stores: &Stores,
    cursor: &mut Cursor,
    message_type: MessageType,
    event_type: proto::OnChainEventType,
) -> Result<Vec<proto::ValidatorMessage>, ReplicationError> {
    collect_messages_with_cursor(cursor, message_type, |page_options, cursor| {
        match account::get_onchain_events(
            &stores.db,
            page_options,
            event_type,
            Some(cursor.token.fid()),
        ) {
            Ok(results) => {
                let messages = results
                    .onchain_events
                    .into_iter()
                    .map(|event| proto::ValidatorMessage {
                        on_chain_event: Some(event),
                        ..Default::default()
                    })
                    .collect();

                Ok((messages, results.next_page_token))
            }
            Err(e) => Err(e),
        }
    })
}

fn build_username_proof_events(
    stores: &Stores,
    cursor: &mut Cursor,
) -> Result<Vec<proto::ValidatorMessage>, ReplicationError> {
    collect_messages_with_cursor(
        cursor,
        MessageType::UsernameProofs,
        |page_options, cursor| {
            let results = UsernameProofStore::get_username_proofs_by_fid(
                &stores.username_proof_store,
                cursor.token.fid(),
                page_options,
            );

            match results {
                Ok(r) => {
                    let messages = r
                        .messages
                        .into_iter()
                        .filter_map(|msg| {
                            match msg.data.unwrap().body.unwrap() {
                                proto::message_data::Body::UsernameProofBody(username_proof) => {
                                    Some(proto::ValidatorMessage {
                                        fname_transfer: Some(proto::FnameTransfer {
                                            proof: Some(username_proof.clone()),
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    })
                                }
                                _ => None, // Skip if not a UsernameProof
                            }
                        })
                        .collect();

                    Ok((messages, r.next_page_token))
                }
                Err(e) => Err(ReplicationError::InternalError(format!(
                    "Failed to fetch username proofs: {}",
                    e
                ))),
            }
        },
    )
}

fn build_username_proof_event(
    stores: &Stores,
    cursor: &mut Cursor,
) -> Result<Vec<proto::ValidatorMessage>, ReplicationError> {
    collect_messages_with_cursor(
        cursor,
        MessageType::UsernameProof,
        |_page_options, cursor| {
            // Fetch the username proof by fid from the user data store
            match UserDataStore::get_username_proof_by_fid(
                &stores.user_data_store,
                cursor.token.fid(),
            ) {
                Ok(None) => {
                    // If no proof is found, return an empty vector
                    Ok((vec![], None))
                }
                Ok(Some(proof)) => {
                    let fname_transfer = proto::FnameTransfer {
                        proof: Some(proof.clone()),
                        ..Default::default()
                    };

                    let results = vec![proto::ValidatorMessage {
                        fname_transfer: Some(fname_transfer),
                        ..Default::default()
                    }];

                    Ok((results, None))
                }
                Err(e) => Err(ReplicationError::InternalError(format!(
                    "Failed to get username proof by fid: {}",
                    e
                ))),
            }
        },
    )
}

fn build_validator_messages(
    stores: &Stores,
    cursor: &mut Cursor,
) -> Result<Vec<proto::ValidatorMessage>, ReplicationError> {
    let mut messages = vec![];

    // IMPORTANT: changing the order of these calls will affect the cursor token, and
    // be a backwards-incompatible change!

    // onchain events

    let fetch_pairs = vec![
        (
            MessageType::OnchainEventsSigner,
            proto::OnChainEventType::EventTypeSigner,
        ),
        (
            MessageType::OnchainEventsSignerMigrated,
            proto::OnChainEventType::EventTypeSignerMigrated,
        ),
        (
            MessageType::OnchainEventsIdRegister,
            proto::OnChainEventType::EventTypeIdRegister,
        ),
        (
            MessageType::OnchainEventsStorageRent,
            proto::OnChainEventType::EventTypeStorageRent,
        ),
        (
            MessageType::OnchainEventsTierPurchase,
            proto::OnChainEventType::EventTypeTierPurchase,
        ),
    ];

    for (message_type, event_type) in fetch_pairs {
        let result = collect_onchain_events_with_cursor(stores, cursor, message_type, event_type);
        match result {
            Ok(mut msgs) => messages.append(&mut msgs),
            Err(e) => {
                return Err(ReplicationError::InternalError(format!(
                    "Failed to collect on-chain events for {:?}: {}",
                    event_type, e
                )))
            }
        }
    }

    // username proofs

    match build_username_proof_events(stores, cursor) {
        Ok(mut msgs) => messages.append(&mut msgs),
        Err(e) => {
            return Err(ReplicationError::InternalError(format!(
                "Failed to collect username proofs: {}",
                e
            )))
        }
    }

    match build_username_proof_event(stores, cursor) {
        Ok(mut msgs) => messages.append(&mut msgs),
        Err(e) => {
            return Err(ReplicationError::InternalError(format!(
                "Failed to collect username proof event: {}",
                e
            )));
        }
    }

    Ok(messages)
}

fn build_user_messages_for_fid(
    stores: &Stores,
    cursor: &mut Cursor,
) -> Result<Vec<proto::Message>, ReplicationError> {
    let mut messages = vec![];

    // IMPORTANT: changing the order of these calls will affect the cursor token, and
    // be a backwards-incompatible change!

    // casts

    match collect_messages(&stores.cast_store, cursor, MessageType::CastMessages) {
        Ok(mut msgs) => messages.append(&mut msgs),
        Err(e) => {
            return Err(ReplicationError::InternalError(format!(
                "Failed to collect cast messages: {}",
                e
            )));
        }
    }

    // links

    match collect_compact_state(&stores.link_store, cursor, MessageType::LinkCompactState) {
        Ok(mut msgs) => messages.append(&mut msgs),
        Err(e) => {
            return Err(ReplicationError::InternalError(format!(
                "Failed to collect link compact state: {}",
                e
            )));
        }
    }

    match collect_messages(&stores.link_store, cursor, MessageType::LinkMessages) {
        Ok(mut msgs) => messages.append(&mut msgs),
        Err(e) => {
            return Err(ReplicationError::InternalError(format!(
                "Failed to collect link messages: {}",
                e
            )));
        }
    }

    // reactions

    match collect_messages(
        &stores.reaction_store,
        cursor,
        MessageType::ReactionMessages,
    ) {
        Ok(mut msgs) => messages.append(&mut msgs),
        Err(e) => {
            return Err(ReplicationError::InternalError(format!(
                "Failed to collect reaction messages: {}",
                e
            )));
        }
    }

    // user data

    match collect_messages(
        &stores.user_data_store,
        cursor,
        MessageType::UserDataMessages,
    ) {
        Ok(mut msgs) => messages.append(&mut msgs),
        Err(e) => {
            return Err(ReplicationError::InternalError(format!(
                "Failed to collect user data messages: {}",
                e
            )));
        }
    }

    // verifications

    match collect_messages(
        &stores.verification_store,
        cursor,
        MessageType::VerificationMessages,
    ) {
        Ok(mut msgs) => messages.append(&mut msgs),
        Err(e) => {
            return Err(ReplicationError::InternalError(format!(
                "Failed to collect verification messages: {}",
                e
            )));
        }
    }

    // username proofs

    match collect_messages(
        &stores.username_proof_store,
        cursor,
        MessageType::UsernameProofsMessages,
    ) {
        Ok(mut msgs) => messages.append(&mut msgs),
        Err(e) => {
            return Err(ReplicationError::InternalError(format!(
                "Failed to collect username proofs messages: {}",
                e
            )));
        }
    }

    Ok(messages)
}

fn build_transaction_for_fid(
    stores: &Stores,
    cursor: &mut Cursor,
) -> Result<Option<proto::Transaction>, ReplicationError> {
    // IMPORTANT: changing the order of these calls will affect the cursor token, and
    // be a backwards-incompatible change!

    let system_messages = match build_validator_messages(stores, cursor) {
        Ok(messages) => messages,
        Err(e) => {
            return Err(ReplicationError::InternalError(format!(
                "Failed to build validator messages: {}",
                e
            )));
        }
    };

    let user_messages = match build_user_messages_for_fid(stores, cursor) {
        Ok(messages) => messages,
        Err(e) => {
            return Err(ReplicationError::InternalError(format!(
                "Failed to build user messages: {}",
                e
            )));
        }
    };

    if system_messages.is_empty() && user_messages.is_empty() {
        return Ok(None);
    }

    Ok(Some(proto::Transaction {
        fid: cursor.token.fid(),
        system_messages,
        user_messages,
        ..Default::default()
    }))
}
