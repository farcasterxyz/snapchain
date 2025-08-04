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

#[derive(PartialEq, Debug)]
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

// Note: if you receive a compile error here, you will need to add the event type into the
// implementation below for fetching on-chain events (build_validator_messages).
impl Into<MessageType> for proto::OnChainEventType {
    fn into(self) -> MessageType {
        match self {
            proto::OnChainEventType::EventTypeSigner => MessageType::OnchainEventsSigner,
            proto::OnChainEventType::EventTypeSignerMigrated => {
                MessageType::OnchainEventsSignerMigrated
            }
            proto::OnChainEventType::EventTypeIdRegister => MessageType::OnchainEventsIdRegister,
            proto::OnChainEventType::EventTypeStorageRent => MessageType::OnchainEventsStorageRent,
            proto::OnChainEventType::EventTypeTierPurchase => {
                MessageType::OnchainEventsTierPurchase
            }
            proto::OnChainEventType::EventTypeNone => {
                panic!("EventTypeNone is not a replicated on-chain event type")
            }
        }
    }
}

// Note: if you receive a compile error here, you will need to add the event type into the
// implementation below for fetching messages (build_user_messages_for_fid).
impl Into<MessageType> for proto::MessageType {
    fn into(self) -> MessageType {
        match self {
            proto::MessageType::CastAdd => MessageType::CastMessages,
            proto::MessageType::CastRemove => panic!("CastRemove is not a replicated message type"),
            proto::MessageType::ReactionAdd => MessageType::ReactionMessages,
            proto::MessageType::ReactionRemove => {
                panic!("ReactionRemove is not a replicated message type")
            }
            proto::MessageType::LinkAdd => MessageType::LinkMessages,
            proto::MessageType::LinkCompactState => MessageType::LinkCompactState,
            proto::MessageType::LinkRemove => panic!("LinkRemove is not a replicated message type"),
            proto::MessageType::VerificationAddEthAddress => MessageType::VerificationMessages,
            proto::MessageType::VerificationRemove => {
                panic!("VerificationRemove is not a replicated message type")
            }
            proto::MessageType::UserDataAdd => MessageType::UserDataMessages,
            proto::MessageType::UsernameProof => MessageType::UsernameProofsMessages,
            proto::MessageType::FrameAction => {
                panic!("FrameAction is not a replicated message type")
            }
            proto::MessageType::None => {
                panic!("None is not a replicated message type")
            }
        }
    }
}

struct Token {
    // Cursor token format [fid, message_type, cursor]
    // 4 bytes for fid
    // 1 byte for message type
    // ... variable length for cursor
    inner: Vec<u8>,
}

impl Token {
    const MESSAGE_TYPE_BYTES: usize = 1;

    fn new_for_fid(fid: u64) -> Self {
        let mut token = Vec::with_capacity(account::FID_BYTES);
        token.extend_from_slice(&account::make_fid_key(fid));
        Token { inner: token }
    }

    fn new_with_message_type(fid: u64, message_type: MessageType) -> Self {
        let mut token = Vec::with_capacity(account::FID_BYTES + Self::MESSAGE_TYPE_BYTES);
        token.extend_from_slice(&account::make_fid_key(fid));
        token.push(message_type as u8);
        Token { inner: token }
    }

    fn new(fid: u64, message_type: MessageType, cursor: Vec<u8>) -> Self {
        let mut token =
            Vec::with_capacity(account::FID_BYTES + Self::MESSAGE_TYPE_BYTES + cursor.len());
        token.extend_from_slice(&account::make_fid_key(fid));
        token.push(message_type as u8);
        token.extend_from_slice(&cursor);
        Token { inner: token }
    }

    fn new_raw(inner: Vec<u8>) -> Self {
        Token { inner }
    }

    fn fid(&self) -> u64 {
        account::read_fid_key(&self.inner, 0)
    }

    fn message_type(&self) -> Option<MessageType> {
        if self.inner.len() < (account::FID_BYTES + Self::MESSAGE_TYPE_BYTES) {
            return None;
        }

        let message_type_byte = self.inner[account::FID_BYTES];
        match MessageType::try_from(message_type_byte) {
            Ok(message_type) => Some(message_type),
            Err(_) => None,
        }
    }

    fn page_token(&self) -> Option<Vec<u8>> {
        if self.inner.len() <= (account::FID_BYTES + Self::MESSAGE_TYPE_BYTES) {
            return None;
        }

        Some(self.inner[(account::FID_BYTES + Self::MESSAGE_TYPE_BYTES)..].to_vec())
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
        // Only attempt to fetch up to the remaining limit
        page_options.page_size = Some(cursor.limit);

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
        if cursor.limit == 0 {
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

    let event_types = vec![
        proto::OnChainEventType::EventTypeSigner,
        proto::OnChainEventType::EventTypeSignerMigrated,
        proto::OnChainEventType::EventTypeIdRegister,
        proto::OnChainEventType::EventTypeStorageRent,
        proto::OnChainEventType::EventTypeTierPurchase,
    ];

    for event_type in event_types {
        let message_type: MessageType = event_type.into();
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
    // IMPORTANT: changing the order of these calls will affect the cursor token, and
    // be a backwards-incompatible change!

    let message_types = vec![
        proto::MessageType::CastAdd,
        proto::MessageType::LinkCompactState,
        proto::MessageType::LinkAdd,
        proto::MessageType::ReactionAdd,
        proto::MessageType::UserDataAdd,
        proto::MessageType::VerificationAddEthAddress,
        proto::MessageType::UsernameProof,
    ];

    let mut messages = vec![];

    for message_type in message_types {
        let result = match message_type {
            proto::MessageType::CastAdd => {
                collect_messages(&stores.cast_store, cursor, message_type.into())
            }
            proto::MessageType::LinkCompactState => {
                collect_compact_state(&stores.link_store, cursor, message_type.into())
            }
            proto::MessageType::LinkAdd => {
                collect_messages(&stores.link_store, cursor, message_type.into())
            }
            proto::MessageType::ReactionAdd => {
                collect_messages(&stores.reaction_store, cursor, message_type.into())
            }
            proto::MessageType::UserDataAdd => {
                collect_messages(&stores.user_data_store, cursor, message_type.into())
            }
            proto::MessageType::VerificationAddEthAddress => {
                collect_messages(&stores.verification_store, cursor, message_type.into())
            }
            proto::MessageType::UsernameProof => {
                collect_messages(&stores.username_proof_store, cursor, message_type.into())
            }
            _ => {
                return Err(ReplicationError::InternalError(format!(
                    "Unsupported message type for user messages: {:?}",
                    message_type
                )));
            }
        };

        match result {
            Ok(mut msgs) => messages.append(&mut msgs),
            Err(e) => {
                return Err(ReplicationError::InternalError(format!(
                    "Failed to collect messages for {:?}: {}",
                    message_type, e
                )));
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn fetch_from_array<T>(
        data: Vec<T>,
        page_options: &PageOptions,
    ) -> Result<(Vec<T>, Option<Vec<u8>>), String>
    where
        T: Clone,
    {
        let start = match page_options.page_token {
            Some(ref token) if !token.is_empty() => token[0] as usize,
            _ => 0,
        };

        if start >= data.len() {
            return Ok((vec![], None));
        }

        let page_size = page_options.page_size.unwrap_or(10);
        let end = std::cmp::min(start + page_size, data.len());

        let results = data[start..end].to_vec();

        let next_page_token = if end < data.len() {
            Some(vec![end as u8])
        } else {
            None
        };

        Ok((results, next_page_token))
    }

    #[tokio::test]
    async fn test_collect_messages_with_cursor() {
        let limit = 10;
        let mut cursor = Cursor::new_for_fid(1, limit);

        let test_data = (1..=20).collect::<Vec<_>>();

        let results = collect_messages_with_cursor(
            &mut cursor,
            MessageType::CastMessages,
            |page_options, _| fetch_from_array(test_data.clone(), page_options),
        );

        assert!(results.is_ok());

        let messages = results.unwrap();

        assert_eq!(messages, test_data[0..limit].to_vec());
        assert_eq!(cursor.token.fid(), 1);
        assert_eq!(cursor.token.message_type(), Some(MessageType::CastMessages));
        assert_eq!(cursor.token.page_token(), Some(vec![10]));
        assert_eq!(cursor.limit, 0);

        // Fetch remaining

        cursor.limit = limit; // Reset limit to fetch next page

        let results = collect_messages_with_cursor(
            &mut cursor,
            MessageType::CastMessages,
            |page_options, _| fetch_from_array(test_data.clone(), page_options),
        );

        assert!(results.is_ok());

        let messages = results.unwrap();

        assert_eq!(messages, test_data[10..20].to_vec());
        assert_eq!(cursor.token.fid(), 1);
        assert_eq!(cursor.token.message_type(), None);
        assert_eq!(cursor.token.page_token(), None);
        assert_eq!(cursor.limit, 0);
    }

    #[tokio::test]
    async fn test_collect_messages_with_cursor_multiple_stores() {
        let limit = 30;
        let mut cursor = Cursor::new_for_fid(1, limit);

        let test_data1 = (1..=20).collect::<Vec<_>>();
        let test_data2 = (21..=40).collect::<Vec<_>>();

        let mut all_results = vec![];

        // First pass of fetching

        let results = collect_messages_with_cursor(
            &mut cursor,
            MessageType::CastMessages,
            |page_options, _| fetch_from_array(test_data1.clone(), page_options),
        );
        assert!(results.is_ok());
        all_results.extend(results.unwrap());

        let results = collect_messages_with_cursor(
            &mut cursor,
            MessageType::LinkMessages,
            |page_options, _| fetch_from_array(test_data2.clone(), page_options),
        );
        assert!(results.is_ok());
        all_results.extend(results.unwrap());

        // Assert results

        let expected_results = test_data1[0..20]
            .to_vec()
            .into_iter()
            .chain(test_data2[0..10].to_vec())
            .collect::<Vec<_>>();

        assert_eq!(all_results, expected_results);
        assert_eq!(cursor.token.fid(), 1);
        assert_eq!(cursor.token.message_type(), Some(MessageType::LinkMessages));
        assert_eq!(cursor.token.page_token(), Some(vec![10]));
        assert_eq!(cursor.limit, 0);

        // Fetch remaining on second pass

        cursor.limit = limit;

        let mut all_results = vec![];
        let results = collect_messages_with_cursor(
            &mut cursor,
            MessageType::CastMessages,
            |page_options, _| fetch_from_array(test_data1.clone(), page_options),
        );
        assert!(results.is_ok());
        all_results.extend(results.unwrap());

        let results = collect_messages_with_cursor(
            &mut cursor,
            MessageType::LinkMessages,
            |page_options, _| fetch_from_array(test_data2.clone(), page_options),
        );
        assert!(results.is_ok());
        all_results.extend(results.unwrap());

        // Assert final results

        let expected_results = test_data2[10..20].to_vec();

        assert_eq!(all_results, expected_results);
        assert_eq!(cursor.token.fid(), 1);
        assert_eq!(cursor.token.message_type(), None);
        assert_eq!(cursor.token.page_token(), None);
        assert_eq!(cursor.limit, 20); // Remaining limit after fetching the last 10 in store 2
    }

    // Similar to test_collect_messages_with_cursor_multiple_stores, but this test
    // exercises the scenario where the limit is exhausted at the end of the first store.
    // In this case, we want to ensure that the cursor is updated correctly to reflect The
    // next store's messsage type.
    #[tokio::test]
    async fn test_collect_messages_with_cursor_multiple_stores_cursor_edge() {
        let limit = 20;
        let mut cursor = Cursor::new_for_fid(1, limit);

        let test_data1 = (1..=20).collect::<Vec<_>>();
        let test_data2 = (21..=40).collect::<Vec<_>>();

        let mut all_results = vec![];

        // First pass of fetching

        let results = collect_messages_with_cursor(
            &mut cursor,
            MessageType::CastMessages,
            |page_options, _| fetch_from_array(test_data1.clone(), page_options),
        );
        assert!(results.is_ok());
        all_results.extend(results.unwrap());

        let results = collect_messages_with_cursor(
            &mut cursor,
            MessageType::LinkMessages,
            |page_options, _| fetch_from_array(test_data2.clone(), page_options),
        );
        assert!(results.is_ok());
        all_results.extend(results.unwrap());

        // Assert results

        let expected_results = test_data1.clone();

        assert_eq!(all_results, expected_results);
        assert_eq!(cursor.token.fid(), 1);
        assert_eq!(cursor.token.message_type(), Some(MessageType::LinkMessages));
        assert_eq!(cursor.token.page_token(), None); // None because we haven't fetching anything
                                                     // here yet
        assert_eq!(cursor.limit, 0);

        // Fetch remaining on second pass

        cursor.limit = limit;

        let mut all_results = vec![];
        let results = collect_messages_with_cursor(
            &mut cursor,
            MessageType::CastMessages,
            |page_options, _| fetch_from_array(test_data1.clone(), page_options),
        );
        assert!(results.is_ok());
        all_results.extend(results.unwrap());

        let results = collect_messages_with_cursor(
            &mut cursor,
            MessageType::LinkMessages,
            |page_options, _| fetch_from_array(test_data2.clone(), page_options),
        );
        assert!(results.is_ok());
        all_results.extend(results.unwrap());

        // Assert final results

        let expected_results = test_data2.clone();

        assert_eq!(all_results, expected_results);
        assert_eq!(cursor.token.fid(), 1);
        assert_eq!(cursor.token.message_type(), None);
        assert_eq!(cursor.token.page_token(), None);
        assert_eq!(cursor.limit, 0);
    }
}
