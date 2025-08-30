use crate::{
    core::util,
    proto::{
        self, shard_trie_entry_with_message::TrieMessage, GetShardTransactionsResponse,
        MessageType, OnChainEventType,
    },
    replication::{error::ReplicationError, replication_stores::ReplicationStores},
    storage::{
        db::{PageOptions, RocksDbTransactionBatch},
        store::{
            account::{LinkStore, UserDataStore, UsernameProofStore, VerificationStore, FID_BYTES},
            engine::PostCommitMessage,
            stores::Stores,
        },
        trie::merkle_trie::{self, TrieKey},
    },
};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::select;
use tracing::{error, info};

#[derive(Clone)]
pub enum CacheEntry {
    UserMessage(proto::Message),
    OnChainEvent(proto::OnChainEvent),
}

/// Cache all messages for (fid, (user/onchain)message_type) by hash, so we can easily get to a message from its hash. This is needed
/// because the Trie only has the hash, but the Messages are stored in the DB by ts_hash, and the trie doesn't have a timestamp (the "ts" part)
/// So, there's no way to read a message from the DB from just the (fid, hash). Hence this cache.
/// Note that the cache itself is an Arc, so this object can be cloned without a memory penalty.
#[derive(Clone)]
pub struct FidMessageTypeCache {
    pub fid: u64,
    pub user_message_type: Option<proto::MessageType>,
    pub onchain_event_type: Option<proto::OnChainEventType>,
    pub cache: Arc<HashMap<Vec<u8>, CacheEntry>>,
}

impl FidMessageTypeCache {
    pub fn new_user_message_type(
        fid: u64,
        user_message_type: proto::MessageType,
        cache: Arc<HashMap<Vec<u8>, CacheEntry>>,
    ) -> Self {
        Self {
            fid,
            user_message_type: Some(user_message_type),
            onchain_event_type: None,
            cache,
        }
    }

    pub fn new_onchain_message_type(
        fid: u64,
        onchain_event_type: proto::OnChainEventType,
        cache: Arc<HashMap<Vec<u8>, CacheEntry>>,
    ) -> Self {
        Self {
            fid,
            user_message_type: None,
            onchain_event_type: Some(onchain_event_type),
            cache,
        }
    }
}

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
    fid_message_type_cache: Arc<RwLock<Option<FidMessageTypeCache>>>,
}

impl Replicator {
    const MESSAGE_LIMIT: usize = 1_000; // Maximum number of messages to fetch per page

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
            fid_message_type_cache: Arc::new(RwLock::new(None)),
        }
    }

    fn build_fid_onchain_message_type_cache(
        &self,
        stores: &Stores,
        fid: u64,
        onchain_event_type: proto::OnChainEventType,
    ) -> Result<Arc<HashMap<Vec<u8>, CacheEntry>>, ReplicationError> {
        {
            // First see if this cache is already present
            let rw = self.fid_message_type_cache.read()?;

            // See if cache hit, we can use the cached value
            if let Some(fmc) = rw.as_ref() {
                if fmc.fid == fid
                    && fmc.onchain_event_type.is_some()
                    && fmc.onchain_event_type.unwrap() == onchain_event_type
                {
                    return Ok(fmc.cache.clone());
                }
            }
        }

        let onchain_events = stores
            .onchain_event_store
            .get_onchain_events(onchain_event_type, Some(fid))
            .map_err(|e| {
                ReplicationError::InternalError(format!(
                    "Failed to get onchain events for fid {} of type {:?}: {}",
                    fid, onchain_event_type, e
                ))
            })?;

        // Build a hashmap of hash -> onchain_event and put it in the cache
        let message_hash_map = Arc::new(
            onchain_events
                .into_iter()
                .map(|m| {
                    // tx_hash + log_index is the key
                    let hash = TrieKey::for_onchain_event(&m)[(1 + FID_BYTES + 1)..].to_vec();

                    (hash, CacheEntry::OnChainEvent(m))
                })
                .collect::<HashMap<_, _>>(),
        );

        {
            let mut rw = self.fid_message_type_cache.write()?;

            *rw = Some(FidMessageTypeCache::new_onchain_message_type(
                fid,
                onchain_event_type,
                message_hash_map.clone(),
            ));
        }

        Ok(message_hash_map)
    }

    fn build_fid_user_message_type_cache(
        &self,
        stores: &Stores,
        fid: u64,
        user_message_type: proto::MessageType,
    ) -> Result<Arc<HashMap<Vec<u8>, CacheEntry>>, ReplicationError> {
        {
            // First see if this cache is already present
            let rw = self.fid_message_type_cache.read()?;

            // See if cache hit, we can use the cached value
            if let Some(fmc) = rw.as_ref() {
                if fmc.fid == fid
                    && fmc.user_message_type.is_some()
                    && fmc.user_message_type.unwrap() == user_message_type
                {
                    return Ok(fmc.cache.clone());
                }
            }
        }

        let mut page_options = PageOptions::default();
        // Get all messages in one go, no need for paging through an individual FIDs messages by message_type
        page_options.page_size = Some(usize::MAX);

        // No filter, we want all messages of fid + message_type
        let filter = Option::<fn(&proto::Message) -> bool>::None;

        // Handle each store type directly
        let messages = match user_message_type {
            proto::MessageType::FrameAction | proto::MessageType::None => {
                return Err(ReplicationError::InvalidMessage(format!(
                    "Invalid message type for FID {}: {:?}",
                    fid, user_message_type
                )));
            }
            proto::MessageType::CastAdd => {
                stores
                    .cast_store
                    .get_adds_by_fid(fid, &page_options, filter)?
                    .messages
            }
            proto::MessageType::CastRemove => {
                stores
                    .cast_store
                    .get_removes_by_fid(fid, &page_options, filter)?
                    .messages
            }
            proto::MessageType::ReactionAdd => {
                stores
                    .reaction_store
                    .get_adds_by_fid(fid, &page_options, filter)?
                    .messages
            }
            proto::MessageType::ReactionRemove => {
                stores
                    .reaction_store
                    .get_removes_by_fid(fid, &page_options, filter)?
                    .messages
            }
            proto::MessageType::LinkAdd => {
                stores
                    .link_store
                    .get_adds_by_fid(fid, &page_options, filter)?
                    .messages
            }
            proto::MessageType::LinkRemove => {
                stores
                    .link_store
                    .get_removes_by_fid(fid, &page_options, filter)?
                    .messages
            }
            proto::MessageType::LinkCompactState => {
                LinkStore::get_link_compact_state_message_by_fid(
                    &stores.link_store,
                    fid,
                    &page_options,
                )?
                .messages
            }
            proto::MessageType::UserDataAdd => {
                stores
                    .user_data_store
                    .get_adds_by_fid(fid, &page_options, filter)?
                    .messages
            }
            proto::MessageType::VerificationAddEthAddress => {
                VerificationStore::get_verification_adds_by_fid(
                    &stores.verification_store,
                    fid,
                    &page_options,
                )?
                .messages
            }
            proto::MessageType::VerificationRemove => {
                VerificationStore::get_verification_removes_by_fid(
                    &stores.verification_store,
                    fid,
                    &page_options,
                )?
                .messages
            }
            proto::MessageType::UsernameProof => {
                UsernameProofStore::get_username_proofs_by_fid(
                    &stores.username_proof_store,
                    fid,
                    &page_options,
                )?
                .messages
            }
        };

        // Build a hashmap of message_hash -> message and put it in the cache
        let message_hash_map = Arc::new(
            messages
                .into_iter()
                .map(|m| (m.hash.clone(), CacheEntry::UserMessage(m)))
                .collect::<HashMap<_, _>>(),
        );

        {
            let mut rw = self.fid_message_type_cache.write()?;

            *rw = Some(FidMessageTypeCache::new_user_message_type(
                fid,
                user_message_type,
                message_hash_map.clone(),
            ));
        }

        Ok(message_hash_map)
    }

    pub fn messages_for_trie_prefix(
        &self,
        shard_id: u32,
        height: u64,
        trie_virtual_shard: u8,
        page_token: Option<String>,
    ) -> Result<GetShardTransactionsResponse, ReplicationError> {
        // Get the stores for this shard_id and height
        let stores = match self.stores.get(shard_id, height) {
            Some(stores) => stores,
            None => {
                return Err(ReplicationError::StoreNotFound(
                    shard_id,
                    height,
                    "No stores found for the given height and shard".to_string(),
                ));
            }
        };

        let mut trie = stores.trie.clone();

        // First, collect MAX_SIZE trie elements starting at the given page_token and prefix
        let mut trie_keys = vec![];

        let next_page_token = trie.get_paged_values_of_subtree(
            &merkle_trie::Context::new(),
            &stores.db,
            &[trie_virtual_shard],
            &mut trie_keys,
            Self::MESSAGE_LIMIT,
            page_token,
        )?;

        let mut fids_in_page = HashSet::new();
        let mut trie_messages = vec![];

        // For each trie key, fetch the associated message.
        for trie_key in trie_keys {
            let (key_virtual_shard, fid, onchain_message_type, message_type, rest) =
                TrieKey::decode(&trie_key)?;
            if key_virtual_shard != trie_virtual_shard {
                return Err(ReplicationError::InternalError(format!(
                    "Virtual shard ID mismatch: expected {}, got {}",
                    trie_virtual_shard, key_virtual_shard
                )));
            }

            fids_in_page.insert(fid);

            match (onchain_message_type, message_type) {
                (Some(onchain_event_type), None) => {
                    // On-chain event
                    if onchain_event_type == 7 {
                        // fname. Get directly from the DB
                        let fname_bytes =
                            rest.into_iter().take_while(|&b| b != 0).collect::<Vec<_>>();

                        let fname_proof = UserDataStore::get_username_proof(
                            &stores.user_data_store,
                            &RocksDbTransactionBatch::new(),
                            &fname_bytes,
                        )?;

                        if fname_proof.is_none() {
                            return Err(ReplicationError::InternalError(format!(
                                "Failed to retrieve fname proof for FID {} with fname_bytes {:?} ({})",
                                fid, fname_bytes, String::from_utf8_lossy(&fname_bytes)
                            )));
                        }

                        let fname_transfer = proto::FnameTransfer {
                            proof: fname_proof,
                            ..Default::default()
                        };

                        trie_messages.push(proto::ShardTrieEntryWithMessage {
                            trie_key,
                            trie_message: Some(
                                proto::shard_trie_entry_with_message::TrieMessage::FnameTransfer(
                                    fname_transfer,
                                ),
                            ),
                        });
                    } else {
                        // onchain event

                        // The "rest" vec contains type at byte 0, transaction_hash in bytes 1-33 and log_index in byte 34+
                        let onchain_event_type =
                            OnChainEventType::try_from(onchain_event_type as i32).map_err(|e| {
                                ReplicationError::InvalidMessage(format!(
                                    "Invalid on-chain event type: {}. Error: {}",
                                    rest[0], e
                                ))
                            })?;

                        // `rest` is tx_hash + log_index.to_be_bytes(), which is the "hash" part of the trie key
                        let hash = rest.to_vec();

                        let cache = self.build_fid_onchain_message_type_cache(
                            &stores,
                            fid,
                            onchain_event_type,
                        )?;

                        let cache_entry = cache.get(&hash).cloned();
                        if cache_entry.is_none() {
                            return Err(ReplicationError::InternalError(format!(
                                "On-chain event not found in cache for FID {} and onchain_event_type {:?}: {:?}",
                                fid, onchain_event_type, hash
                            )));
                        }

                        let onchain_event = match cache_entry.unwrap() {
                            CacheEntry::OnChainEvent(event) => event,
                            CacheEntry::UserMessage(_) => {
                                return Err(ReplicationError::InternalError(
                                    "Expected OnChainEvent but found UserMessage in cache"
                                        .to_string(),
                                ));
                            }
                        };

                        trie_messages.push(proto::ShardTrieEntryWithMessage {
                            trie_key,
                            trie_message: Some(TrieMessage::OnChainEvent(onchain_event)),
                        });
                    }
                }
                (None, Some(message_type)) => {
                    // User message
                    let hash = rest;
                    let cache = self.build_fid_user_message_type_cache(
                        &stores,
                        fid,
                        MessageType::try_from(message_type as i32).map_err(|e| {
                            ReplicationError::InvalidMessage(format!(
                                "Invalid message type: {}. Error: {}",
                                message_type, e
                            ))
                        })?,
                    )?;

                    let cache_entry = cache.get(&hash).cloned();
                    if cache_entry.is_none() {
                        return Err(ReplicationError::InternalError(format!(
                            "User message not found in cache for FID {} and message_type {}: {:?}",
                            fid, message_type, hash
                        )));
                    }

                    let message = match cache_entry.unwrap() {
                        CacheEntry::UserMessage(msg) => msg,
                        CacheEntry::OnChainEvent(_) => {
                            return Err(ReplicationError::InternalError(
                                "Expected UserMessage but found OnChainEvent in cache".to_string(),
                            ));
                        }
                    };

                    trie_messages.push(proto::ShardTrieEntryWithMessage {
                        trie_key,
                        trie_message: Some(TrieMessage::UserMessage(message)),
                    });
                }
                _ => {
                    return Err(ReplicationError::InternalError(format!(
                        "Both onchain_message_type and message_type were set. Invalid trie key: {:?}",
                        trie_key
                    )));
                }
            }
        }

        // For each FID in the page, get its account root hash
        let mut fid_account_roots = vec![];
        for fid in fids_in_page {
            let account_root_hash = trie.get_hash(
                &stores.db,
                &mut RocksDbTransactionBatch::new(),
                &TrieKey::for_fid(fid),
            )?;
            fid_account_roots.push(proto::FidAccountRootHash {
                fid,
                account_root_hash,
            });
        }

        trie.reload(&stores.db)?;

        let response = proto::GetShardTransactionsResponse {
            trie_messages,
            fid_account_roots,
            next_page_token,
        };
        Ok(response)
    }

    pub fn get_snapshot_metadata(&self, shard: u32) -> Result<Vec<(u64, u64)>, ReplicationError> {
        self.stores.get_metadata(shard)
    }

    pub fn get_shard_chunk_by_height(
        &self,
        shard_id: u32,
        height: u64,
    ) -> Result<Option<proto::ShardChunk>, ReplicationError> {
        let stores = match self.stores.get(shard_id, height) {
            Some(stores) => stores,
            None => {
                // This case is valid, it just means no snapshot exists for this height.
                return Ok(None);
            }
        };

        stores
            .shard_store
            .get_chunk_by_height(height)
            .map_err(|e| ReplicationError::InternalError(e.to_string()))
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
        let oldest_valid_timestamp = 0; // self.oldest_valid_timestamp()?;

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

        // Check if the number of existing snapshots exceeds 1
        if let Ok(metadata) = self.stores.get_metadata(msg.shard_id) {
            if metadata.len() > 1 {
                return Ok(());
            }
        }

        // Open a snapshot
        let open_result = self
            .stores
            .open_snapshot(msg.shard_id, block_number, timestamp);
        if let Ok(_) = open_result {
            info!(
                "Opened replicator snapshot for shard {} at height {} with timestamp {}",
                msg.shard_id, block_number, timestamp
            );
        } else {
            error!(
                "Failed to open replicator snapshot for shard {} at height {} with timestamp {}: {:?}",
                msg.shard_id, block_number, timestamp, open_result
            );
        }

        open_result
    }
}
