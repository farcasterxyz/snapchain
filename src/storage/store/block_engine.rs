use crate::consensus::proposer::ProposalSource;
use crate::core::error::HubError;
use crate::core::message::HubEventExt;
use crate::core::validations;
use crate::core::{types::Height, util::FarcasterTime};
use crate::mempool::mempool::MempoolMessagesRequest;
use crate::proto::{
    self, block_event_data, Block, BlockEvent, BlockEventData, BlockEventType, FarcasterNetwork,
    HeartbeatEventBody, HubEvent, MergeMessageBody, MessageType, OnChainEvent, ShardChunkWitness,
    StoreType, Transaction,
};
use crate::storage::db::{RocksDB, RocksDbTransactionBatch};
use crate::storage::store::account::{
    BlockEventStore, ChannelMemberStore, ChannelMemberStoreDef, ChannelModerateStore,
    ChannelModerateStoreDef, ChannelPinStore, ChannelPinStoreDef, ChannelUpdateStore,
    ChannelUpdateStoreDef, IntoU8, MergeContext, OnchainEventStorageError, OnchainEventStore,
    StorageLendStore, StorageLendStoreDef, StorageSlot, Store, StoreEventHandler,
    VerificationStore, VerificationStoreDef,
};
use crate::storage::store::engine_metrics::Metrics;
use crate::storage::store::mempool_poller::{MempoolMessage, MempoolPoller, MempoolPollerError};
use crate::storage::store::stores::{Limits, StoreLimits};
use crate::storage::store::BlockStore;
use crate::storage::trie::merkle_trie::{self, MerkleTrie, TrieKey};
use crate::storage::trie::{self};
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use crate::version::version::{EngineVersion, ProtocolFeature};
use itertools::Itertools;
use prost::Message;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::sync::Arc;
use std::u32;
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::{error, warn};

#[derive(Error, Debug)]
pub enum BlockEngineError {
    #[error(transparent)]
    TrieError(#[from] trie::errors::TrieError),

    #[error(transparent)]
    MempoolPollerError(#[from] MempoolPollerError),

    #[error("merkle trie root hash mismatch")]
    HashMismatch,

    #[error("events hash mismatch")]
    EventsHashMismatch,

    #[error(transparent)]
    OnchainEventError(#[from] OnchainEventStorageError),

    #[error(transparent)]
    HubError(#[from] HubError),
}
#[derive(Error, Debug, Clone)]
pub enum MessageValidationError {
    #[error("message has no data")]
    NoMessageData,

    #[error("unknown fid")]
    MissingFid,

    #[error("invalid signer")]
    MissingSigner,

    #[error("message type {msg_type} is not in the scope of the gasless signer")]
    GaslessKeyOutOfScope { msg_type: i32 },

    #[error("invalid message type")]
    InvalidMessageType,

    #[error("verification timestamp predates shard-zero activation")]
    VerificationTimestampBeforeActivation,

    #[error("insufficient storage")]
    InsufficientStorage,

    #[error(transparent)]
    HubError(#[from] HubError),

    #[error(transparent)]
    MessageValidationError(#[from] validations::error::ValidationError),
}

// WHY SHARD-0 VERIFICATION TOMBSTONES ARE PERMANENT, AND THEREFORE WHY THEY NEED A CAP.
//
// Unlike the data shard's pruned store, shard 0 never reclaims a tombstone. Do not "clean this
// up" by aging, pruning, or reclaiming them: dropping a tombstone empties the address's logical
// key, which lets anyone re-gossip the owner's old post-V20-signed add. Shard 0 merges it (LWW
// against nothing), fans it out, and force-override replay re-imposes it on the data shard
// unconditionally — resurrecting a verification its owner deliberately removed. Pre-V20 pruning
// had the same hole bounded by plain LWW; force-override amplifies it. Permanent-but-bounded is
// the design, and the bound is what keeps "permanent" affordable.
//
// Legacy verification limits ran into the hundreds, so this floor lets a fid shed a large
// pre-V20 verification set. Larger storage allocations scale the bound through `max_messages`.
// The zero-storage case deliberately remains zero so storage-free fids cannot mint permanent
// shard-0 rows.
const VERIFICATION_TOMBSTONE_CAP_FLOOR: u32 = 256;

/// How many permanent tombstone rows a fid may mint, given its live-add allowance.
fn verification_tombstone_cap(max_messages: u32) -> u32 {
    if max_messages == 0 {
        0
    } else {
        max_messages.max(VERIFICATION_TOMBSTONE_CAP_FLOOR)
    }
}

/// The hard bound on a fid's TOTAL permanent shard-0 verification rows (adds + tombstones).
/// Because rows are never reclaimed, this is a lifetime bound on distinct addresses, not a
/// concurrent one: a fid that reaches it can still replace rows it already has, but can never
/// admit a new address again.
fn verification_row_cap(max_messages: u32) -> u32 {
    max_messages.saturating_add(verification_tombstone_cap(max_messages))
}

#[derive(Clone, Copy, Default)]
struct VerificationMessageCounts {
    live_adds: u32,
    tombstones: u32,
}

impl VerificationMessageCounts {
    /// The counter a message of `msg_type` belongs to, or `None` if it is not a verification type.
    /// The single place that maps type -> counter: callers supply their own error for `None`, so
    /// this mapping cannot drift between the trie read and the in-transaction delta.
    fn counter_mut(&mut self, msg_type: MessageType) -> Option<&mut u32> {
        match msg_type {
            MessageType::VerificationAddEthAddress => Some(&mut self.live_adds),
            MessageType::VerificationRemove => Some(&mut self.tombstones),
            _ => None,
        }
    }
}

// `merge_key_add` / `merge_key_remove` in `account::gasless_key_merge` return the
// ShardEngine-flavored `engine::MessageValidationError` (their historical home). Translate
// into the block-engine variant so callers here can use `?` on them. The merge helpers only
// ever produce the subset of variants mapped explicitly below; anything else falls through
// to `HubError` with the source's formatted message so the information isn't lost.
impl From<crate::storage::store::engine::MessageValidationError> for MessageValidationError {
    fn from(err: crate::storage::store::engine::MessageValidationError) -> Self {
        use crate::storage::store::engine::MessageValidationError as E;
        match err {
            E::NoMessageData => Self::NoMessageData,
            E::MissingFid => Self::MissingFid,
            E::MissingSigner => Self::MissingSigner,
            E::GaslessKeyOutOfScope { msg_type } => Self::GaslessKeyOutOfScope { msg_type },
            E::MessageValidationError(v) => Self::MessageValidationError(v),
            E::InvalidMessageType(_) => Self::InvalidMessageType,
            E::StoreError(h) => Self::HubError(h),
            other => Self::HubError(HubError::internal_db_error(&other.to_string())),
        }
    }
}

#[derive(Clone)]
pub struct BlockStores {
    pub block_store: BlockStore,
    pub block_event_store: BlockEventStore,
    pub onchain_event_store: OnchainEventStore,
    pub storage_lend_store: Store<StorageLendStoreDef>,
    pub verification_store: Store<VerificationStoreDef>,
    pub channel_update_store: Store<ChannelUpdateStoreDef>,
    pub channel_member_store: Store<ChannelMemberStoreDef>,
    pub channel_pin_store: Store<ChannelPinStoreDef>,
    pub channel_moderate_store: Store<ChannelModerateStoreDef>,
    pub network: FarcasterNetwork,
    pub db: Arc<RocksDB>,
    pub trie: MerkleTrie,
    pub event_handler: Arc<StoreEventHandler>,
}

impl BlockStores {
    pub fn new(db: Arc<RocksDB>, trie: MerkleTrie, network: FarcasterNetwork) -> Self {
        let store_event_handler = StoreEventHandler::new();
        BlockStores {
            block_store: BlockStore::new(db.clone()),
            block_event_store: BlockEventStore { db: db.clone() },
            onchain_event_store: OnchainEventStore::new(db.clone(), store_event_handler.clone()),
            storage_lend_store: StorageLendStore::new(db.clone(), store_event_handler.clone(), 100),
            verification_store: VerificationStore::new(
                db.clone(),
                store_event_handler.clone(),
                100,
            ),
            channel_update_store: ChannelUpdateStore::new(
                db.clone(),
                store_event_handler.clone(),
                100,
            ),
            channel_member_store: ChannelMemberStore::new(
                db.clone(),
                store_event_handler.clone(),
                100,
            ),
            channel_pin_store: ChannelPinStore::new(db.clone(), store_event_handler.clone(), 100),
            channel_moderate_store: ChannelModerateStore::new(
                db.clone(),
                store_event_handler.clone(),
                100,
            ),
            network,
            db: db.clone(),
            trie,
            event_handler: store_event_handler,
        }
    }
    pub fn get_block_by_event_seqnum(&self, seqnum: u64) -> Option<Block> {
        let block_event = self
            .block_event_store
            .get_block_event_by_seqnum(seqnum)
            .ok()??;
        self.block_store
            .get_block_by_height(block_event.block_number())
            .ok()?
    }

    pub fn get_storage_slot_for_fid(
        &self,
        fid: u64,
        engine_version: EngineVersion,
        pending_onchain_events: &Vec<OnChainEvent>,
        count_lent_storage: bool,
        count_borrowed_storage: bool,
    ) -> Option<StorageSlot> {
        let lent_storage = if count_lent_storage {
            StorageLendStore::get_lent_storage(&self.storage_lend_store, fid).ok()?
        } else {
            StorageSlot::new(0, 0, 0, u32::MAX)
        };

        let borrowed_storage = if count_borrowed_storage {
            StorageLendStore::get_borrowed_storage(&self.storage_lend_store, fid).ok()?
        } else {
            StorageSlot::new(0, 0, 0, u32::MAX)
        };

        self.onchain_event_store
            .get_storage_slot_for_fid(
                fid,
                self.network,
                engine_version,
                pending_onchain_events.as_slice(),
                &lent_storage,
                &borrowed_storage,
            )
            .ok()
    }
}

pub struct BlockEngine {
    stores: BlockStores,
    pub network: FarcasterNetwork,
    pub mempool_poller: MempoolPoller,
    shard_id: u64,
    db: Arc<RocksDB>,
    metrics: Metrics,
}

// Shard state root and the transactions
#[derive(Clone, Debug)]
pub struct BlockStateChange {
    pub timestamp: FarcasterTime,
    pub new_state_root: Vec<u8>,
    pub events_hash: Vec<u8>,
    pub transactions: Vec<Transaction>,
    pub events: Vec<BlockEvent>,
}

pub(crate) fn block_engine_system_messages_for_replay<'a>(
    system_messages: &'a [proto::ValidatorMessage],
    version: EngineVersion,
) -> Cow<'a, [proto::ValidatorMessage]> {
    if !version.is_enabled(ProtocolFeature::SortedBlockEngineEvents) {
        return Cow::Borrowed(system_messages);
    }

    let mut sorted_system_messages = system_messages.to_vec();
    // CONSENSUS-CRITICAL: the ordering semantics here must stay identical to ShardEngine's
    // comparator in engine.rs (`replay_snapchain_txn`, its `sorted_system_messages.sort_by`
    // block). Both engines replay the same onchain events and must canonicalize their order
    // the same way; if the two diverge, block-shard and data-shard replay fold shard-0 state
    // differently. Change both together, or extract a shared comparator.
    sorted_system_messages.sort_by(|a, b| {
        match (&a.on_chain_event, &b.on_chain_event) {
            (Some(event_a), Some(event_b)) => {
                // Both are OnChainEvents: order by block_number then log_index.
                (event_a.block_number, event_a.log_index)
                    .cmp(&(event_b.block_number, event_b.log_index))
            }
            (Some(_), None) => Ordering::Less, // OnChainEvents sort before other system messages.
            (None, Some(_)) => Ordering::Greater, // Other system messages sort after OnChainEvents.
            (None, None) => Ordering::Equal, // Neither is an OnChainEvent; keep input order (stable sort).
        }
    });

    Cow::Owned(sorted_system_messages)
}

impl BlockEngine {
    pub fn new(
        mut trie: MerkleTrie,
        statsd_client: StatsdClientWrapper,
        db: Arc<RocksDB>,
        max_messages_per_block: u32,
        messages_request_tx: Option<mpsc::Sender<MempoolMessagesRequest>>,
        network: FarcasterNetwork,
    ) -> Self {
        trie.initialize(&db).unwrap();
        BlockEngine {
            stores: BlockStores::new(db.clone(), trie, network),
            shard_id: 0,
            mempool_poller: MempoolPoller {
                max_messages_per_block,
                messages_request_tx,
                network,
                shard_id: 0,
                statsd_client: statsd_client.clone(),
            },
            db,
            metrics: Metrics {
                statsd_client,
                shard_id: 0,
            },
            network,
        }
    }

    pub fn stores(&self) -> BlockStores {
        self.stores.clone()
    }

    pub fn trie_root_hash(&self) -> Vec<u8> {
        self.stores.trie.root_hash().unwrap()
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

    fn set_height(&self, version: &EngineVersion, height: Height) {
        if version.is_enabled(ProtocolFeature::EventIdBugFix) {
            self.stores
                .event_handler
                .set_current_height(height.block_number);
        }
    }

    /// Single-message convenience wrapper. TEST-ONLY BY CONSTRUCTION: it derives the verification
    /// quota counters by reading the trie, which is correct only at the START of a transaction.
    /// Block-engine trie updates are staged after the user-message loop, so a mid-loop caller
    /// would silently read a pre-transaction count and admit past the cap. Production must go
    /// through `replay_snapchain_txn`, which threads the transaction-local count instead. Keeping
    /// this `cfg(test)` stops a future caller from reaching for the shorter name.
    #[cfg(test)]
    pub fn validate_user_message(
        &self,
        message: &proto::Message,
        storage_slot: &StorageSlot,
        timestamp: &FarcasterTime,
        version: EngineVersion,
        txn_batch: &mut RocksDbTransactionBatch,
    ) -> Result<(), MessageValidationError> {
        let verification_counts = self.verification_message_counts(message.fid(), txn_batch)?;
        self.validate_user_message_with_verification_counts(
            message,
            storage_slot,
            timestamp,
            version,
            txn_batch,
            verification_counts,
        )
    }

    /// Counts the fid's live adds and tombstones separately from the trie. The message types come
    /// from the same store-type mapping used by data-shard accounting, while keeping the shard-0
    /// quota semantics explicit per type.
    fn verification_message_counts(
        &self,
        fid: u64,
        txn_batch: &RocksDbTransactionBatch,
    ) -> Result<VerificationMessageCounts, HubError> {
        let mut counts = VerificationMessageCounts::default();
        for msg_type in Limits::store_type_to_message_types(StoreType::Verifications) {
            let count = self
                .stores
                .trie
                .get_count(
                    &self.stores.db,
                    txn_batch,
                    &TrieKey::for_message_type(fid, msg_type.into_u8()),
                )
                .map_err(|err| {
                    HubError::internal_db_error(&format!(
                        "unable to count shard-0 verifications: {err}"
                    ))
                })?;
            let count = u32::try_from(count)
                .map_err(|_| HubError::internal_db_error("verification count exceeds u32"))?;
            let target = counts.counter_mut(msg_type).ok_or_else(|| {
                HubError::internal_db_error(
                    "verification store mapping contains a non-verification type",
                )
            })?;
            *target = target
                .checked_add(count)
                .ok_or_else(|| HubError::internal_db_error("verification count overflow"))?;
        }
        Ok(counts)
    }

    /// The type of the fid's existing primary record for `address`, if any. Callers must branch on
    /// the TYPE, not merely on presence: which counter an incoming message grows depends on what it
    /// replaces (see the transition match in `validate_user_message_with_verification_counts`).
    fn verification_logical_key_type(
        &self,
        fid: u64,
        address: &[u8],
        txn_batch: &RocksDbTransactionBatch,
    ) -> Result<Option<MessageType>, MessageValidationError> {
        let add_exists = VerificationStore::get_verification_add(
            &self.stores.verification_store,
            fid,
            address,
            Some(txn_batch),
        )
        .map_err(MessageValidationError::HubError)?
        .is_some();
        if add_exists {
            return Ok(Some(MessageType::VerificationAddEthAddress));
        }
        Ok(VerificationStore::get_verification_remove_with_txn(
            &self.stores.verification_store,
            fid,
            address,
            Some(txn_batch),
        )
        .map_err(MessageValidationError::HubError)?
        .map(|_| MessageType::VerificationRemove))
    }

    fn validate_user_message_with_verification_counts(
        &self,
        message: &proto::Message,
        storage_slot: &StorageSlot,
        timestamp: &FarcasterTime,
        version: EngineVersion,
        txn_batch: &mut RocksDbTransactionBatch,
        verification_counts: VerificationMessageCounts,
    ) -> Result<(), MessageValidationError> {
        // Ensure message data is present
        let message_data = message
            .data
            .as_ref()
            .ok_or(MessageValidationError::NoMessageData)?;

        let is_pro_user = self
            .stores
            .onchain_event_store
            .is_tier_subscription_active_at(proto::TierType::Pro, message.fid(), timestamp)
            .map_err(|err| HubError::internal_db_error(&err.to_string()))?;

        validations::message::validate_message(
            message,
            self.network,
            is_pro_user,
            timestamp,
            version,
        )?;

        // 1. Check that the user has a custody address
        self.stores
            .onchain_event_store
            .get_id_register_event_by_fid(message_data.fid, Some(txn_batch))
            .map_err(|_| MessageValidationError::MissingFid)?
            .ok_or(MessageValidationError::MissingFid)?;

        // 2. Check that the user has a valid signer.
        //
        // KEY_ADD / KEY_REMOVE are special: they authenticate via a custody EIP-712 signature
        // (and, for self-revocation KEY_REMOVE, via the Ed25519 key being revoked signing
        // itself). The outer `message.signer` on these messages is the Ed25519 key being
        // added or removed — by definition either not yet in the signer store (KEY_ADD) or
        // about to leave it (KEY_REMOVE). Their real authentication happens in the merge
        // path (see `merge_key_add` / `merge_key_remove`), so skip the active-signer check
        // here. This bypass also subsumes the self-revocation `KEY_REMOVE` scope carve-out
        // (see matching comment in `ShardEngine::validate_user_message`).
        let msg_type = MessageType::try_from(message_data.r#type).unwrap_or(MessageType::None);
        let mut gasless_ttl_for_bump: Option<u32> = None;
        let is_key_message = msg_type == MessageType::KeyAdd || msg_type == MessageType::KeyRemove;
        if is_key_message && !version.is_enabled(ProtocolFeature::GaslessSigners) {
            return Err(MessageValidationError::InvalidMessageType);
        }
        if !is_key_message {
            let active_key = crate::storage::store::account::get_active_key(
                &self.stores.onchain_event_store,
                &self.stores.db,
                txn_batch,
                message_data.fid,
                &message.signer,
            )
            .map_err(|_| MessageValidationError::MissingSigner)?
            .ok_or(MessageValidationError::MissingSigner)?;

            if !active_key.admits(msg_type) {
                return Err(MessageValidationError::GaslessKeyOutOfScope {
                    msg_type: message_data.r#type,
                });
            }

            // Capture gasless ttl for the sliding-expiry bump below. Matches
            // `ShardEngine::validate_user_message` — the `ttl > 0` guard is defense in depth
            // since `validate_key_add_body` rejects `ttl == 0` for gasless keys.
            if let crate::storage::store::account::ActiveKey::Gasless { ttl_seconds, .. } =
                active_key
            {
                if ttl_seconds > 0 {
                    gasless_ttl_for_bump = Some(ttl_seconds);
                }
            }
        }

        match message_data
            .body
            .as_ref()
            .ok_or(MessageValidationError::NoMessageData)?
        {
            crate::proto::message_data::Body::LendStorageBody(lend_storage) => {
                let total_storage_purchased = self
                    .stores
                    .get_storage_slot_for_fid(message_data.fid, version, &vec![], false, false)
                    .ok_or(MessageValidationError::InsufficientStorage)?;

                // Restricts who can lend storage to some reasonable set of users. Don't enforce this limit in devnet and testnet so we can test with fewer storage units.
                if self.network == FarcasterNetwork::Mainnet
                    && total_storage_purchased.units_for(lend_storage.unit_type()) < 1000
                {
                    return Err(MessageValidationError::InsufficientStorage);
                }

                let num_units_available = storage_slot.units_for(lend_storage.unit_type());
                let num_units_required =
                    if version.is_enabled(ProtocolFeature::StorageLendingLimitFix) {
                        // Retain 1 unit for the lender so the lender is able to revoke lent storage. There are a couple places that fail if the user has no active storage. Maintaining 1 storage unit is easier and safer than bypassing these validations for storage lends.
                        lend_storage.num_units + 1
                    } else {
                        lend_storage.num_units
                    };
                if num_units_available < num_units_required as u32 {
                    return Err(MessageValidationError::InsufficientStorage);
                }
            }
            // KEY_ADD / KEY_REMOVE have no per-body pre-merge validation to do at the block-
            // engine level: static body validation (key length, scopes, ttl bound, etc.) ran
            // upstream in `validate_message`, and state-dependent checks (nonce CAS, custody
            // recovery, conflict resolution) live in the merge helpers themselves.
            crate::proto::message_data::Body::KeyAddBody(_)
            | crate::proto::message_data::Body::KeyRemoveBody(_)
                if version.is_enabled(ProtocolFeature::GaslessSigners) => {}
            body @ (crate::proto::message_data::Body::VerificationAddAddressBody(_)
            | crate::proto::message_data::Body::VerificationRemoveBody(_))
                if version.is_enabled(ProtocolFeature::VerificationsOnShardZero) =>
            {
                // A message's `r#type` and its body are independent on the wire; routing and the
                // merge path dispatch on `r#type` while this match dispatches on the body. Without
                // an agreement check, a KEY_ADD-typed message carrying a verification body routes
                // to shard 0 and would be admitted here — accepted and gossiped by submit_message
                // even though merge (dispatching on `r#type`) can never merge it. Require
                // agreement so such a message stays rejected exactly as before these arms existed.
                let (expected_type, is_add, address) = match body {
                    crate::proto::message_data::Body::VerificationAddAddressBody(body) => (
                        MessageType::VerificationAddEthAddress,
                        true,
                        body.address.as_slice(),
                    ),
                    crate::proto::message_data::Body::VerificationRemoveBody(body) => (
                        MessageType::VerificationRemove,
                        false,
                        body.address.as_slice(),
                    ),
                    // Unreachable: the arm's pattern binds only the two bodies above.
                    _ => return Err(MessageValidationError::InvalidMessageType),
                };
                if msg_type != expected_type {
                    return Err(MessageValidationError::InvalidMessageType);
                }
                let embedded_version = EngineVersion::version_for(
                    &FarcasterTime::new(message_data.timestamp as u64),
                    self.network,
                );
                if !embedded_version.is_enabled(ProtocolFeature::VerificationsOnShardZero) {
                    // Reject a verification minted before this feature existed, even inside a
                    // block that has it. Such a message already lives on the fid shard, and this
                    // replica starts empty at activation, so it holds no history to judge the
                    // message against.
                    //
                    // The hazard is for machinery that does not exist yet, which is why this
                    // rejects rather than merges: if a later change fans shard-0 verification
                    // merges out to fid shards, an add admitted here could carry an older
                    // `ts_hash` than a remove the fid shard has already applied. Should that
                    // replay overwrite local state instead of re-running the CRDT compare in
                    // `Store::merge_add`, it would resurrect the add and undo the remove.
                    // Refusing old-regime messages here means such a replay can never encounter
                    // one. Dropping this check is a one-line revert if that fan-out lands with
                    // conflict-free replay semantics instead.
                    return Err(MessageValidationError::VerificationTimestampBeforeActivation);
                }

                // Production always constructs `Stores`/`BlockStores` with `StoreLimits::default()`,
                // so the live-add limit matches the data shard's limit exactly. If store limits
                // ever become configurable, this must read the same injected value the data-shard
                // prune uses or the two sides will enforce different caps on the same message.
                let max_messages =
                    StoreLimits::default().max_messages(storage_slot, StoreType::Verifications);
                // With no active storage, adds are never admitted, including replacements. This
                // is the spam gate for shard 0's otherwise gasless verification write path.
                if max_messages == 0 && is_add {
                    return Err(MessageValidationError::InsufficientStorage);
                }
                // Admission is decided per TRANSITION, against the quantity each one actually
                // grows. Two rules a reader may be tempted to collapse back into one, both of
                // which are unsound — each was a live hole caught in review:
                //
                //   1. A type-blind "any existing row supersedes, so admit" carve-out lets an add
                //      land on a tombstone, skipping the live-add cap while refunding a tombstone
                //      slot. `remove addr; add addr` then cycles forever.
                //   2. Gating only the counter a transition grows is still not enough, because
                //      rows are PERMANENT while the counters are not. `add addr_i; remove addr_i`
                //      returns `live_adds` to 0 every cycle and leaves a tombstone behind, so any
                //      rule written purely over current counter state never engages.
                //
                // Rows appear ONLY in the two `None` arms, and both check `total_rows`. That makes
                // the bound locally provable here: no fid can ever exceed
                // `max_messages + tombstone_cap` permanent replica rows, for any message sequence.
                let tombstone_cap = verification_tombstone_cap(max_messages);
                let row_cap = verification_row_cap(max_messages);
                let total_rows = verification_counts
                    .live_adds
                    .saturating_add(verification_counts.tombstones);
                let existing =
                    self.verification_logical_key_type(message_data.fid, address, txn_batch)?;
                match (is_add, existing) {
                    // Replacing an add with an add: net-zero on both counters, always admitted.
                    (true, Some(MessageType::VerificationAddEthAddress)) => {}
                    // Re-adding a tombstoned address. Row-neutral, but it grows `live_adds`.
                    (true, Some(_)) => {
                        if verification_counts.live_adds >= max_messages {
                            return Err(MessageValidationError::InsufficientStorage);
                        }
                    }
                    // Mints a new add row.
                    (true, None) => {
                        if verification_counts.live_adds >= max_messages || total_rows >= row_cap {
                            return Err(MessageValidationError::InsufficientStorage);
                        }
                    }
                    // A remove over any existing row turns an add row into a tombstone row, or
                    // replaces a tombstone with a newer one. Row-neutral either way, and
                    // deliberately admitted even past `tombstone_cap`: a fid at cap, or one whose
                    // storage lapsed, must always be able to shed live state.
                    (false, Some(_)) => {}
                    // Mints a new tombstone row. This is the pre-V20-address remove case: the
                    // replica has never seen the address, so the row is new.
                    (false, None) => {
                        if verification_counts.tombstones >= tombstone_cap || total_rows >= row_cap
                        {
                            return Err(MessageValidationError::InsufficientStorage);
                        }
                    }
                }
            }
            _ => return Err(MessageValidationError::InvalidMessageType),
        }

        // 3. Sliding-TTL enforcement for gasless keys (NEYN-10576). Mirrors
        // `ShardEngine::validate_user_message`. See the long-form comment there for the
        // `current_block_timestamp` unit / error-handling rationale.
        if let Some(ttl) = gasless_ttl_for_bump {
            let current_block_timestamp = timestamp.to_u64();
            crate::storage::store::account::check_and_bump_last_used_at(
                &self.stores.db,
                txn_batch,
                message_data.fid,
                &message.signer,
                ttl,
                message_data.timestamp,
                current_block_timestamp,
            )
            .map_err(MessageValidationError::HubError)?;
        }

        Ok(())
    }

    fn merge_message(
        &self,
        message: &proto::Message,
        txn_batch: &mut RocksDbTransactionBatch,
        block_version: EngineVersion,
    ) -> Result<Vec<proto::HubEvent>, MessageValidationError> {
        let msg_type = message.msg_type();
        let gasless_enabled = if matches!(msg_type, MessageType::KeyAdd | MessageType::KeyRemove) {
            let ts = message
                .data
                .as_ref()
                .ok_or(MessageValidationError::NoMessageData)?
                .timestamp;
            let version = EngineVersion::version_for(&FarcasterTime::new(ts as u64), self.network);
            version.is_enabled(ProtocolFeature::GaslessSigners)
        } else {
            false
        };
        match msg_type {
            MessageType::LendStorage => {
                let ts = message
                    .data
                    .as_ref()
                    .ok_or(MessageValidationError::NoMessageData)?
                    .timestamp;
                let version =
                    EngineVersion::version_for(&FarcasterTime::new(ts as u64), self.network);
                let ctx = MergeContext { version };
                Ok(StorageLendStore::merge(
                    &self.stores.storage_lend_store,
                    message,
                    txn_batch,
                    &ctx,
                )?)
            }
            MessageType::KeyAdd if gasless_enabled => {
                // BlockEngine is the admission path — full validation, including the
                // request_fid IdRegister + custody match.
                Ok(vec![crate::storage::store::account::merge_key_add(
                    &self.stores.db,
                    &self.stores.onchain_event_store,
                    message,
                    txn_batch,
                    false,
                )?])
            }
            MessageType::KeyRemove if gasless_enabled => {
                Ok(vec![crate::storage::store::account::merge_key_remove(
                    &self.stores.db,
                    &self.stores.onchain_event_store,
                    message,
                    txn_batch,
                )?])
            }
            MessageType::VerificationAddEthAddress | MessageType::VerificationRemove
                if block_version.is_enabled(ProtocolFeature::VerificationsOnShardZero) =>
            {
                // Two different version notions meet here, deliberately. The arm's gate uses the
                // *block* version, because whether shard 0 may host verifications at all is a
                // property of the block being replayed. The `MergeContext` carries the message's
                // *embedded* version, because merge semantics are a property of the message.
                //
                // Be precise about what that context does today: nothing. `Store::merge` forwards
                // `ctx` only to `merge_compact_state`, and `VerificationStoreDef` has no compact
                // state, so verification adds/removes never read it. It is derived this way so it
                // agrees by construction with how `ShardEngine::merge_message` builds the context
                // for this same store, *if* `VerificationStoreDef` ever gates a merge decision on
                // version (as `LinkStoreDef` already does). Do not read this as a live
                // constraint, and do not "simplify" it to the block version — that would
                // silently diverge from the fid shard the moment it starts mattering.
                //
                // Only the context half mirrors `ShardEngine`: it has no block-version notion at
                // all, because a data shard needs no feature gate to host its own verifications.
                // Do not go looking there for the gate above.
                let ts = message
                    .data
                    .as_ref()
                    .ok_or(MessageValidationError::NoMessageData)?
                    .timestamp;
                // Named apart from `block_version` on purpose: both are `EngineVersion`, so only
                // the names keep the gate and the merge context from being swapped by a refactor.
                let embedded_version =
                    EngineVersion::version_for(&FarcasterTime::new(ts as u64), self.network);
                let ctx = MergeContext {
                    version: embedded_version,
                };
                Ok(vec![self
                    .stores
                    .verification_store
                    .merge(message, txn_batch, &ctx)?])
            }
            _ => return Err(MessageValidationError::InvalidMessageType),
        }
    }

    /// Applies one successful merge to the transaction-local verification counters. Block-engine
    /// trie updates are staged only after the user-message loop, so re-reading the trie mid-loop
    /// would miss earlier merges. Applying every deleted message before the merged message makes
    /// the event itself enforce all add/remove replacement transitions.
    fn apply_verification_count_delta(
        mut counts: VerificationMessageCounts,
        merge_message_body: &MergeMessageBody,
    ) -> Result<VerificationMessageCounts, BlockEngineError> {
        for deleted in &merge_message_body.deleted_messages {
            let target = counts.counter_mut(deleted.msg_type()).ok_or_else(|| {
                BlockEngineError::HubError(HubError::internal_db_error(
                    "verification merge deleted a non-verification row",
                ))
            })?;
            *target = target.checked_sub(1).ok_or_else(|| {
                BlockEngineError::HubError(HubError::internal_db_error(
                    "verification merge count underflow",
                ))
            })?;
        }

        if let Some(message) = &merge_message_body.message {
            let target = counts.counter_mut(message.msg_type()).ok_or_else(|| {
                BlockEngineError::HubError(HubError::internal_db_error(
                    "verification merge added a non-verification row",
                ))
            })?;
            *target = target.checked_add(1).ok_or_else(|| {
                BlockEngineError::HubError(HubError::internal_db_error(
                    "verification merge count overflow",
                ))
            })?;
        }

        Ok(counts)
    }

    /// Mirrors `HubEventExt::from_validation_error`, which cannot be reused directly because it is
    /// typed on `engine::MessageValidationError` rather than this module's error enum.
    fn merge_failure_event(message: &proto::Message, err: &MessageValidationError) -> HubEvent {
        let merge_error = match err {
            MessageValidationError::HubError(hub_error) => hub_error.clone(),
            _ => HubError::validation_failure(&err.to_string()),
        };
        HubEvent::new_event(
            proto::HubEventType::MergeFailure,
            proto::hub_event::Body::MergeFailure(proto::MergeFailureBody {
                message: Some(message.clone()),
                code: merge_error.code,
                reason: merge_error.message,
            }),
        )
    }

    fn on_merge_message(
        &mut self,
        storage_slot: &mut StorageSlot,
        merge_message_body: &MergeMessageBody,
    ) -> Result<(), BlockEngineError> {
        if let Some(added_message) = &merge_message_body.message {
            match added_message.data.as_ref().unwrap().body.as_ref().unwrap() {
                proto::message_data::Body::LendStorageBody(lend_storage_body) => {
                    storage_slot.sub(&StorageSlot::from_storage_lend(&lend_storage_body));
                }
                _ => {}
            }
        }

        for deleted_message in &merge_message_body.deleted_messages {
            match deleted_message
                .data
                .as_ref()
                .unwrap()
                .body
                .as_ref()
                .unwrap()
            {
                proto::message_data::Body::LendStorageBody(lend_storage_body) => {
                    storage_slot.merge(&StorageSlot::from_storage_lend(&lend_storage_body));
                }
                _ => {}
            }
        }

        Ok(())
    }

    pub(crate) fn replay_snapchain_txn(
        &mut self,
        trie_ctx: &merkle_trie::Context,
        snapchain_txn: &Transaction,
        txn_batch: &mut RocksDbTransactionBatch,
        timestamp: &FarcasterTime,
        version: EngineVersion,
    ) -> Result<(Vec<u8>, Vec<HubEvent>, Vec<MessageValidationError>), BlockEngineError> {
        let mut hub_events = vec![];
        let mut validation_errors = vec![];
        let system_messages =
            block_engine_system_messages_for_replay(&snapchain_txn.system_messages, version);
        for message in system_messages.as_ref() {
            if let Some(ref onchain_event) = message.on_chain_event {
                if onchain_event.r#type() == proto::OnChainEventType::EventTypeChannelRegister
                    && !version.is_enabled(ProtocolFeature::ChannelRegistrations)
                {
                    warn!(
                        block_number = onchain_event.block_number,
                        log_index = onchain_event.log_index,
                        chain_id = onchain_event.chain_id,
                        "Saw channel register event while feature isn't active"
                    );
                    continue;
                }
                match self
                    .stores
                    .onchain_event_store
                    .merge_onchain_event(onchain_event.clone(), txn_batch)
                {
                    Ok(event) => {
                        hub_events.push(event);
                    }
                    Err(err) => {
                        // Duplicate error is expected
                        warn!("Unable to merge onchain event: {:#?}", err.to_string())
                    }
                }
            }
        }

        let mut storage_slot = self
            .storage_slot_for_transaction(snapchain_txn, version, true, false)
            .unwrap();
        // Lending validation must exclude borrowed units, but verification quota must include
        // them exactly as data-shard StoreLimits does. Keep two views and apply any successful
        // lend in this transaction to both so later verification admission sees the same net
        // slot on proposal, validation, and commit replay.
        //
        // Both are derived up front rather than lazily at the first verification, because the
        // slot must be snapshotted before any in-transaction lend mutates it via
        // `on_merge_message`. Shard 0 carries every key rotation and lend, so the prescan keeps
        // verification-free transactions off the extra storage-rent scan and trie reads.
        let has_verification = version.is_enabled(ProtocolFeature::VerificationsOnShardZero)
            && snapchain_txn.user_messages.iter().any(|message| {
                matches!(
                    message.msg_type(),
                    MessageType::VerificationAddEthAddress | MessageType::VerificationRemove
                )
            });
        let mut verification_storage_slot = if has_verification {
            // Deliberately not `.unwrap()` like the slot above: that call passes
            // count_borrowed_storage=false and never reads the lend store, while this one does.
            // `get_storage_slot_for_fid` collapses a RocksDB error into `None`, so unwrapping
            // would turn a transient local read failure into a panic on the propose/validate/
            // commit paths. Surface it as an error instead, matching the data shard, whose
            // equivalent read maps to `EngineError::UsageCountError` rather than aborting.
            self.storage_slot_for_transaction(snapchain_txn, version, true, true)
                .ok_or_else(|| {
                    BlockEngineError::HubError(HubError::internal_db_error(
                        "unable to read storage slot for shard-0 verification quota",
                    ))
                })?
        } else {
            storage_slot.clone()
        };
        let mut verification_counts = if has_verification {
            self.verification_message_counts(snapchain_txn.fid, txn_batch)
                .map_err(BlockEngineError::HubError)?
        } else {
            VerificationMessageCounts::default()
        };

        for message in &snapchain_txn.user_messages {
            let validation_storage_slot = match message.msg_type() {
                MessageType::VerificationAddEthAddress | MessageType::VerificationRemove => {
                    &verification_storage_slot
                }
                _ => &storage_slot,
            };
            match self.validate_user_message_with_verification_counts(
                message,
                validation_storage_slot,
                timestamp,
                version,
                txn_batch,
                verification_counts,
            ) {
                Ok(()) => match message.msg_type() {
                    MessageType::LendStorage => {
                        if version.is_enabled(ProtocolFeature::StorageLending) {
                            if let Ok(events) = self.merge_message(message, txn_batch, version) {
                                for event in &events {
                                    if let Some(proto::hub_event::Body::MergeMessageBody(body)) =
                                        event.body.as_ref()
                                    {
                                        // Both views must see the same lend, or verification
                                        // admission and lend validation drift apart.
                                        self.on_merge_message(&mut storage_slot, body)?;
                                        self.on_merge_message(
                                            &mut verification_storage_slot,
                                            body,
                                        )?;
                                    }
                                }
                                hub_events.extend(events);
                            }
                        }
                    }
                    MessageType::KeyAdd | MessageType::KeyRemove => {
                        // No storage-slot accounting needed — gasless keys don't consume
                        // storage units. Emitted MergeMessageBody propagates to shards via
                        // BlockEvent so their local DBs can replay the same merge.
                        if version.is_enabled(ProtocolFeature::GaslessSigners) {
                            if let Ok(events) = self.merge_message(message, txn_batch, version) {
                                hub_events.extend(events);
                            }
                        }
                    }
                    // THE live path for verifications once V20 is enabled: routing sends them
                    // here, admission has already applied the timestamp floor and the replica
                    // quota, and successful merges fan out as BlockEvents for force-override
                    // replay onto the fid's data shard. Below V20 this arm is unreachable —
                    // `validate_user_message` rejects verification bodies outright, so the merge
                    // is never attempted and nothing here can perturb pre-V20 streams.
                    MessageType::VerificationAddEthAddress | MessageType::VerificationRemove => {
                        if version.is_enabled(ProtocolFeature::VerificationsOnShardZero) {
                            match self.merge_message(message, txn_batch, version) {
                                Ok(events) => {
                                    for event in &events {
                                        if let Some(proto::hub_event::Body::MergeMessageBody(
                                            body,
                                        )) = event.body.as_ref()
                                        {
                                            verification_counts =
                                                Self::apply_verification_count_delta(
                                                    verification_counts,
                                                    body,
                                                )?;
                                        }
                                    }
                                    hub_events.extend(events)
                                }
                                Err(err) => {
                                    // Surfaced rather than swallowed, unlike the arms above, and
                                    // the asymmetry is deliberate. For those types the routine
                                    // rejections are caught in `validate_user_message`, so a merge
                                    // error is near-unreachable. A verification's rejections --
                                    // duplicate, add superseded by a newer remove, stale add after
                                    // a remove -- are all detected *here*, because the validation
                                    // arm above checks only signatures and the version floor. Drop
                                    // them and `simulate_message`, which reports success when
                                    // `validation_errors` is empty, would tell a submitter their
                                    // verification was accepted while it was never stored.
                                    //
                                    // Consensus is unaffected either way: both consensus callers
                                    // of `replay_snapchain_txn` discard `validation_errors`, and
                                    // neither the trie nor block events are touched when no merge
                                    // event is produced. Only `simulate_message` reads this.
                                    warn!(
                                        fid = message.fid(),
                                        hash = message.hex_hash(),
                                        "Error merging shard-0 verification: {:?}",
                                        err
                                    );
                                    let mut merge_failure =
                                        Self::merge_failure_event(message, &err);
                                    // Event-id assignment is a pure function of block content, so
                                    // a failure here is identical on every node — the event drops
                                    // from the stream network-wide rather than diverging.
                                    // MergeFailure is trie-inert and never becomes a BlockEvent,
                                    // so neither the state root nor events_hash is affected.
                                    if let Err(event_err) = self
                                        .stores
                                        .event_handler
                                        .commit_transaction(txn_batch, &mut merge_failure)
                                    {
                                        error!(
                                            fid = message.fid(),
                                            hash = message.hex_hash(),
                                            "Failed to persist shard-0 verification merge failure event: {:?}",
                                            event_err
                                        );
                                    }
                                    hub_events.push(merge_failure);
                                    validation_errors.push(err);
                                }
                            }
                        }
                    }
                    _ => {}
                },
                Err(err) => {
                    warn!(
                        fid = snapchain_txn.fid,
                        "Error merging message {}",
                        err.to_string()
                    );
                    validation_errors.push(err);
                }
            }
        }

        for event in &hub_events {
            self.stores
                .trie
                .update_for_event(trie_ctx, &self.db, &event, txn_batch)?;
        }

        let account_root =
            self.stores
                .trie
                .get_hash(&self.db, txn_batch, &TrieKey::for_fid(snapchain_txn.fid))?;

        Ok((account_root, hub_events, validation_errors))
    }

    fn heartbeat_block_interval(&self) -> u64 {
        match self.network {
            FarcasterNetwork::Devnet => 5,
            FarcasterNetwork::None | FarcasterNetwork::Testnet | FarcasterNetwork::Mainnet => 100,
        }
    }

    fn build_block_event(data: BlockEventData) -> BlockEvent {
        let hash = blake3::hash(data.encode_to_vec().as_slice())
            .as_bytes()
            .to_vec();
        BlockEvent {
            hash,
            data: Some(data),
        }
    }

    fn generate_block_events(
        &self,
        height: Height,
        timestamp: &FarcasterTime,
        hub_events: Vec<HubEvent>,
        txn: &mut RocksDbTransactionBatch,
    ) -> (Vec<BlockEvent>, Vec<u8>) {
        let version = EngineVersion::version_for(timestamp, self.network);
        let gasless_enabled = version.is_enabled(ProtocolFeature::GaslessSigners);
        let mut events = vec![];
        let mut max_block_event_seqnum = self.stores.block_event_store.max_seqnum().unwrap();
        for hub_event in hub_events {
            match hub_event.body.unwrap() {
                proto::hub_event::Body::MergeMessageBody(merge_message_body) => {
                    if let Some(message) = merge_message_body.message {
                        let msg_type = message.msg_type();
                        let is_key =
                            matches!(msg_type, MessageType::KeyAdd | MessageType::KeyRemove);
                        if is_key && !gasless_enabled {
                            continue;
                        }
                        match msg_type {
                            MessageType::LendStorage
                            | MessageType::KeyAdd
                            | MessageType::KeyRemove
                            | MessageType::VerificationAddEthAddress
                            | MessageType::VerificationRemove => {
                                // All shard-0-hosted user messages propagate the same way:
                                // wrap the original message in a MergeMessageEvent so shards
                                // 1..N can replay the merge into their local DBs via
                                // ShardEngine::handle_block_event. Upstream merge gates decide
                                // whether a feature's messages can reach this allowlist.
                                max_block_event_seqnum += 1;
                                let data = BlockEventData {
                                    seqnum: max_block_event_seqnum,
                                    r#type: BlockEventType::MergeMessage as i32,
                                    block_number: height.block_number,
                                    event_index: events.len() as u64,
                                    block_timestamp: timestamp.to_u64(),
                                    body: Some(block_event_data::Body::MergeMessageEventBody(
                                        proto::MergeMessageEventBody {
                                            message: Some(message),
                                        },
                                    )),
                                };
                                let event = Self::build_block_event(data);
                                events.push(event);
                            }
                            _ => {}
                        }
                    }
                }
                proto::hub_event::Body::MergeOnChainEventBody(merge_on_chain_event_body) => {
                    // Shard 0 fans channel-register onchain events to every data shard so
                    // their replica folds can rebuild the ownership indexes and emit hints
                    // (ShardEngine::handle_block_event). Carry the whole original event,
                    // mirroring the MergeMessage template above. Only channel registers fan
                    // out, and only once the feature is active; every other onchain event is
                    // skipped silently, mirroring the pre-feature gasless-key gate.
                    let Some(on_chain_event) = merge_on_chain_event_body.on_chain_event else {
                        continue;
                    };
                    if on_chain_event.r#type() != proto::OnChainEventType::EventTypeChannelRegister
                        || !version.is_enabled(ProtocolFeature::ChannelOwnershipEvents)
                    {
                        continue;
                    }
                    max_block_event_seqnum += 1;
                    let data = BlockEventData {
                        seqnum: max_block_event_seqnum,
                        r#type: BlockEventType::MergeOnChainEvent as i32,
                        block_number: height.block_number,
                        event_index: events.len() as u64,
                        block_timestamp: timestamp.to_u64(),
                        body: Some(block_event_data::Body::MergeOnChainEventEventBody(
                            proto::MergeOnChainEventEventBody {
                                on_chain_event: Some(on_chain_event),
                            },
                        )),
                    };
                    let event = Self::build_block_event(data);
                    events.push(event);
                }
                _ => {}
            }
        }

        if height.block_number % self.heartbeat_block_interval() == 0 {
            max_block_event_seqnum += 1;
            let data = BlockEventData {
                seqnum: max_block_event_seqnum,
                r#type: BlockEventType::Heartbeat as i32,
                block_number: height.block_number,
                event_index: events.len() as u64,
                block_timestamp: timestamp.to_u64(),
                body: Some(block_event_data::Body::HeartbeatEventBody(
                    HeartbeatEventBody {},
                )),
            };
            let event = Self::build_block_event(data);
            // Store these events so
            // (1) It's possible to figuure out the max seqnum easily
            // (2) It's possible to query over them in an rpc and see what has been produced.
            events.push(event);
        }

        for event in events.iter() {
            self.stores
                .block_event_store
                .put_block_event(&event, txn)
                .unwrap();
        }

        let events_hash = if events.is_empty() {
            vec![]
        } else {
            let mut events_hasher = blake3::Hasher::new();
            for event in events.iter() {
                events_hasher.update(&event.hash);
            }
            events_hasher.finalize().as_bytes().to_vec()
        };

        (events, events_hash)
    }

    fn storage_slot_for_transaction(
        &self,
        snapchain_txn: &Transaction,
        engine_version: EngineVersion,
        count_lent_storage: bool,
        count_borrowed_storage: bool,
    ) -> Option<StorageSlot> {
        let pending_onchain_events: Vec<OnChainEvent> = snapchain_txn
            .system_messages
            .iter()
            .filter_map(|vm| vm.on_chain_event.clone())
            .collect();

        self.stores.get_storage_slot_for_fid(
            snapchain_txn.fid,
            engine_version,
            &pending_onchain_events,
            count_lent_storage,
            count_borrowed_storage,
        )
    }

    fn prepare_proposal(
        &mut self,
        txn_batch: &mut RocksDbTransactionBatch,
        messages: Vec<MempoolMessage>,
        height: Height,
        timestamp: &FarcasterTime,
        version: EngineVersion,
    ) -> Result<BlockStateChange, BlockEngineError> {
        self.metrics.count(
            "recv_messages",
            messages.len() as u64,
            Metrics::proposal_source_tags(ProposalSource::Propose),
        );

        let mut snapchain_txns = MempoolPoller::create_transactions_from_mempool(messages)?
            .into_iter()
            .filter_map(|mut transaction| {
                // TODO(aditi): We could share this code with the shard engine but there may be other things we want to add here. For example, it may make sense to exclude validator messages and user messages that aren't intended for shard 0 here so a bug in the mempool won't impact the protocol in a significant way.
                let storage_slot =
                    self.storage_slot_for_transaction(&transaction, version, true, true)?;

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

        self.set_height(&version, height);

        let mut all_hub_events = vec![];
        for snapchain_txn in &mut snapchain_txns {
            let (account_root, hub_events, _) = self.replay_snapchain_txn(
                &merkle_trie::Context::new(),
                &snapchain_txn,
                txn_batch,
                timestamp,
                version,
            )?;
            snapchain_txn.account_root = account_root;
            all_hub_events.extend_from_slice(&hub_events);
        }

        let (events, events_hash) =
            self.generate_block_events(height, timestamp, all_hub_events, txn_batch);

        self.metrics
            .publish_transaction_counts(&snapchain_txns, ProposalSource::Propose);

        let new_root_hash = self.stores.trie.root_hash()?;

        let result = BlockStateChange {
            timestamp: timestamp.clone(),
            new_state_root: new_root_hash.clone(),
            transactions: snapchain_txns,
            events_hash,
            events,
        };

        Ok(result)
    }

    pub fn propose_state_change(
        &mut self,
        messages: Vec<MempoolMessage>,
        height: Height,
        timestamp: Option<FarcasterTime>,
    ) -> BlockStateChange {
        let now = std::time::Instant::now();
        let mut txn = RocksDbTransactionBatch::new();

        let timestamp = timestamp.unwrap_or(FarcasterTime::current());
        let version = EngineVersion::version_for(&timestamp, self.network);
        let state_change = if version.is_enabled(ProtocolFeature::WriteDataToShardZero) {
            let result = self
                .prepare_proposal(&mut txn, messages, height, &timestamp, version)
                .unwrap();

            self.stores.trie.reload(&self.db).unwrap();
            result
        } else {
            BlockStateChange {
                events: vec![],
                new_state_root: vec![],
                timestamp: FarcasterTime::current(),
                events_hash: vec![],
                transactions: vec![],
            }
        };

        let proposal_duration = now.elapsed();
        self.metrics
            .time_with_shard("propose_time", proposal_duration.as_millis() as u64);

        self.metrics.count("propose.invoked", 1, vec![]);
        state_change
    }

    fn replay_proposal(
        &mut self,
        txn_batch: &mut RocksDbTransactionBatch,
        transactions: &[Transaction],
        shard_root: &[u8],
        events_hash: &Vec<u8>,
        source: ProposalSource,
        height: Height,
        timestamp: &FarcasterTime,
        version: EngineVersion,
    ) -> Result<(), BlockEngineError> {
        let now = std::time::Instant::now();
        // TODO(aditi): We probably only want to check this if we're in a test env (maybe only if the network is Devnet)
        // Validate that the trie is in a good place to start with
        match self.get_last_block() {
            None => { // There are places where it's hard to provide a parent hash-- e.g. tests so make this an option and skip validation if not present
            }
            Some(block) => match self.stores.trie.root_hash() {
                Err(err) => {
                    warn!(
                        source = source.to_string(),
                        "Unable to compute trie root hash {:#?}", err
                    )
                }
                Ok(root_hash) => {
                    let parent_shard_root = block.header.unwrap().state_root;
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

        self.set_height(&version, height);

        let mut all_hub_events = vec![];
        for snapchain_txn in transactions {
            let (account_root, hub_events, _) = self.replay_snapchain_txn(
                &merkle_trie::Context::new(),
                snapchain_txn,
                txn_batch,
                timestamp,
                version,
            )?;
            // Reject early if account roots fail to match (shard roots will definitely fail)
            if &account_root != &snapchain_txn.account_root {
                warn!(
                    fid = snapchain_txn.fid,
                    new_account_root = hex::encode(&account_root),
                    tx_account_root = hex::encode(&snapchain_txn.account_root),
                    source = source.to_string(),
                    num_system_messages = snapchain_txn.system_messages.len(),
                    num_user_messages = snapchain_txn.user_messages.len(),
                    "Account root mismatch"
                );
                return Err(BlockEngineError::HashMismatch);
            }

            all_hub_events.extend_from_slice(&hub_events);
        }

        let (_block_events, computed_events_hash) =
            self.generate_block_events(height, timestamp, all_hub_events, txn_batch);

        if computed_events_hash != *events_hash {
            warn!(
                shard_id = self.shard_id,
                expected_events_hash = hex::encode(events_hash),
                actual_events_hash = hex::encode(computed_events_hash),
                "Events hash mismatch"
            );
            return Err(BlockEngineError::EventsHashMismatch);
        }

        let root1 = self.stores.trie.root_hash()?;
        if &root1 != shard_root {
            warn!(
                shard_id = self.shard_id,
                new_shard_root = hex::encode(&root1),
                tx_shard_root = hex::encode(shard_root),
                source = source.to_string(),
                num_txns = transactions.len(),
                "Shard root mismatch"
            );
            return Err(BlockEngineError::HashMismatch);
        }

        let elapsed = now.elapsed();
        self.metrics
            .time_with_shard("replay_proposal_time", elapsed.as_millis() as u64);

        Ok(())
    }

    pub fn validate_state_change(
        &mut self,
        shard_state_change: &BlockStateChange,
        height: Height,
    ) -> bool {
        let version = EngineVersion::version_for(&shard_state_change.timestamp, self.network);
        if !version.is_enabled(ProtocolFeature::WriteDataToShardZero) {
            return true;
        }
        let mut txn = RocksDbTransactionBatch::new();

        let now = std::time::Instant::now();
        let transactions = &shard_state_change.transactions;
        let shard_root = &shard_state_change.new_state_root;

        let proposal_result = self.replay_proposal(
            &mut txn,
            transactions,
            shard_root,
            &shard_state_change.events_hash,
            ProposalSource::Validate,
            height,
            &shard_state_change.timestamp,
            version,
        );

        if let Err(ref err) = proposal_result {
            error!("State change validation failed: {}", err);
        }

        self.stores.trie.reload(&self.db).unwrap();
        let elapsed = now.elapsed();
        self.metrics
            .time_with_shard("validate_time", elapsed.as_millis() as u64);

        if proposal_result.is_ok() {
            self.metrics.count("validate.true", 1, vec![]);
        } else {
            self.metrics.count("validate.false", 1, vec![]);
        }

        proposal_result.is_ok()
    }

    pub fn commit_block(&mut self, block: &Block) {
        let height = block.header.as_ref().unwrap().height.unwrap();
        self.metrics.gauge("block_height", height.block_number);
        let block_timestamp = block.header.as_ref().unwrap().timestamp;
        // If block timestamp is ahead of current (only in tests), don't overflow
        self.metrics.gauge(
            "block_delay_seconds",
            FarcasterTime::current().to_u64().max(block_timestamp) - block_timestamp,
        );
        self.metrics.count(
            "block_shards",
            block
                .shard_witness
                .as_ref()
                .unwrap()
                .shard_chunk_witnesses
                .len() as u64,
            vec![],
        );

        let version =
            EngineVersion::version_for(&FarcasterTime::new(block_timestamp), self.network);
        if version.is_enabled(ProtocolFeature::WriteDataToShardZero) {
            let mut txn = RocksDbTransactionBatch::new();
            match self.replay_proposal(
                &mut txn,
                &block.transactions,
                &block.header.as_ref().unwrap().state_root,
                &block.header.as_ref().unwrap().events_hash,
                ProposalSource::Commit,
                height,
                &FarcasterTime::new(block_timestamp),
                version,
            ) {
                Err(err) => {
                    error!("State change commit failed: {}", err);
                    panic!("State change commit failed: {}", err);
                }
                Ok(()) => {
                    self.db.commit(txn).unwrap();
                    let result = self.stores.block_store.put_block(block);
                    if result.is_err() {
                        error!("Failed to store block: {:?}", result.err());
                    }
                    self.stores.trie.reload(&self.db).unwrap();
                    self.metrics
                        .publish_transaction_counts(&block.transactions, ProposalSource::Commit);
                    self.metrics.count(
                        "block_events",
                        block.events.len() as u64,
                        Metrics::proposal_source_tags(ProposalSource::Commit),
                    );
                    let max_block_event_seqnum =
                        self.stores.block_event_store.max_seqnum().unwrap();
                    self.metrics
                        .gauge("block_event_seqnum", max_block_event_seqnum);
                    // TODO(aditi): We need to add the post-commit hooks for replication for shard 0.
                }
            }
        } else {
            let result = self.stores.block_store.put_block(block);
            if result.is_err() {
                error!("Failed to store block: {:?}", result.err());
            }
        }
    }

    pub fn get_last_block(&self) -> Option<Block> {
        match self.stores.block_store.get_last_block() {
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
        match self
            .stores
            .block_store
            .get_block_by_height(height.block_number)
        {
            Ok(block) => block,
            Err(err) => {
                error!("No block at height {:#?}", err);
                None
            }
        }
    }

    pub fn get_confirmed_height(&self) -> Height {
        let shard_index = 0;
        match self.stores.block_store.max_block_number() {
            Ok(block_num) => Height::new(shard_index, block_num),
            Err(_) => Height::new(shard_index, 0),
        }
    }

    pub fn get_min_height(&self) -> Height {
        let shard_index = 0;
        match self.stores.block_store.min_block_number() {
            Ok(block_num) => Height::new(shard_index, block_num),
            // In case of no blocks, return height 1
            Err(_) => Height::new(shard_index, 1),
        }
    }

    pub fn get_last_shard_witness(
        &self,
        height: Height,
        shard_id: u32,
    ) -> Option<ShardChunkWitness> {
        let previous_height = height.decrement()?;
        let previous_block = self.get_block_by_height(previous_height)?;
        let previous_shard_witness = previous_block.shard_witness?;
        previous_shard_witness
            .shard_chunk_witnesses
            .iter()
            .find(|witness| witness.height.unwrap().shard_index == shard_id)
            .cloned()
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
        let version = EngineVersion::current(self.network);
        let (_, _, errors) = self
            .replay_snapchain_txn(
                &merkle_trie::Context::new(),
                &snapchain_txn,
                &mut txn,
                &FarcasterTime::current(),
                version,
            )
            .map_err(|err| {
                MessageValidationError::HubError(HubError::invalid_internal_state(&err.to_string()))
            })?;

        self.stores.trie.reload(&self.db).map_err(|e| {
            MessageValidationError::HubError(HubError::invalid_internal_state(&e.to_string()))
        })?;

        if !errors.is_empty() {
            return Err(errors[0].clone());
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod verification_cap_tests {
    use super::{verification_row_cap, verification_tombstone_cap};

    // Pinned as pure functions because the interesting input -- an allowance ABOVE the 256 floor,
    // which takes ~52 storage units -- is impractical to reach through the commit path.
    #[test]
    fn tombstone_cap_floors_at_the_constant_and_then_scales() {
        // No storage: storage-free fids must not mint permanent rows at all.
        assert_eq!(verification_tombstone_cap(0), 0);
        assert_eq!(verification_row_cap(0), 0);

        // Below the floor (one 2025 unit is 5), the floor dominates so a whale shedding a large
        // pre-V20 set is not blocked by its own small live allowance.
        assert_eq!(verification_tombstone_cap(5), 256);
        assert_eq!(verification_row_cap(5), 261);

        // Above the floor, the allowance scales instead -- this is the `.max(max_messages)` term.
        assert_eq!(verification_tombstone_cap(300), 300);
        assert_eq!(verification_row_cap(300), 600);
    }

    #[test]
    fn row_cap_saturates_rather_than_overflowing() {
        assert_eq!(verification_row_cap(u32::MAX), u32::MAX);
    }
}

#[cfg(test)]
mod ordering_tests {
    use super::block_engine_system_messages_for_replay;
    use crate::proto::{OnChainEvent, ValidatorMessage};
    use crate::version::version::EngineVersion;

    fn validator_message(block_number: u32, log_index: u32) -> ValidatorMessage {
        ValidatorMessage {
            on_chain_event: Some(OnChainEvent {
                block_number,
                log_index,
                ..Default::default()
            }),
            fname_transfer: None,
            block_event: None,
        }
    }

    fn log_indexes(messages: &[ValidatorMessage]) -> Vec<u32> {
        messages
            .iter()
            .map(|message| message.on_chain_event.as_ref().unwrap().log_index)
            .collect()
    }

    #[test]
    fn sorted_block_engine_events_gate_controls_system_message_order() {
        let messages = vec![
            validator_message(7, 20),
            validator_message(7, 10),
            validator_message(6, 30),
        ];

        let off = block_engine_system_messages_for_replay(&messages, EngineVersion::V19);
        assert_eq!(log_indexes(off.as_ref()), vec![20, 10, 30]);

        let on = block_engine_system_messages_for_replay(&messages, EngineVersion::V20);
        assert_eq!(log_indexes(on.as_ref()), vec![30, 10, 20]);
    }
}

#[cfg(test)]
mod error_conversion_tests {
    //! Lock down `From<engine::MessageValidationError> for block_engine::MessageValidationError`.
    //!
    //! The merge helpers in `account::gasless_key_merge` return the ShardEngine-flavored error
    //! (their historical home). Callers in `BlockEngine::merge_message` use `?` against the
    //! block-engine-flavored error, which relies on this conversion. These tests enumerate every
    //! variant the helpers can realistically produce so the mapping doesn't silently drop
    //! information if future merge-helper changes start emitting new variants.

    use super::MessageValidationError;
    use crate::core::error::HubError;
    use crate::core::validations::error::ValidationError;
    use crate::storage::store::engine::MessageValidationError as EngineErr;

    #[test]
    fn maps_no_message_data() {
        let out: MessageValidationError = EngineErr::NoMessageData.into();
        assert!(matches!(out, MessageValidationError::NoMessageData));
    }

    #[test]
    fn maps_missing_fid() {
        let out: MessageValidationError = EngineErr::MissingFid.into();
        assert!(matches!(out, MessageValidationError::MissingFid));
    }

    #[test]
    fn maps_missing_signer() {
        let out: MessageValidationError = EngineErr::MissingSigner.into();
        assert!(matches!(out, MessageValidationError::MissingSigner));
    }

    #[test]
    fn maps_validation_error_transparently() {
        // ValidationError passes through unchanged. Pick a specific variant the merge helpers
        // actually emit (`KeyNotRegistered` from merge_key_remove) so a future widening of the
        // `ValidationError` enum is caught here.
        let out: MessageValidationError =
            EngineErr::MessageValidationError(ValidationError::KeyNotRegistered).into();
        assert!(matches!(
            out,
            MessageValidationError::MessageValidationError(ValidationError::KeyNotRegistered)
        ));
    }

    #[test]
    fn maps_invalid_message_type_drops_payload() {
        // The engine variant carries an `i32` discriminant; block-engine's variant has no
        // payload. We intentionally drop the i32 — block-engine callers don't need to
        // distinguish which message type got rejected because they're the originator.
        let out: MessageValidationError = EngineErr::InvalidMessageType(42).into();
        assert!(matches!(out, MessageValidationError::InvalidMessageType));
    }

    #[test]
    fn maps_store_error_to_hub_error() {
        // engine::StoreError wraps a HubError. The mapping should preserve the HubError
        // payload so logs/telemetry still carry the original code + message.
        let source = HubError::internal_db_error("corrupt record");
        let out: MessageValidationError = EngineErr::StoreError(source.clone()).into();
        match out {
            MessageValidationError::HubError(h) => {
                assert_eq!(h.code, source.code);
                assert_eq!(h.message, source.message);
            }
            other => panic!("expected HubError, got {other:?}"),
        }
    }

    #[test]
    fn maps_gasless_key_out_of_scope_preserves_msg_type() {
        // Scope violations surface from the shared validation path (called from either engine).
        // Preserving the `msg_type` field across the conversion keeps the telemetry signal intact
        // — callers that format this variant want the numeric type for a later lookup, not the
        // flattened Display string.
        let out: MessageValidationError = EngineErr::GaslessKeyOutOfScope { msg_type: 3 }.into();
        match out {
            MessageValidationError::GaslessKeyOutOfScope { msg_type } => assert_eq!(msg_type, 3),
            other => panic!("expected GaslessKeyOutOfScope, got {other:?}"),
        }
    }

    #[test]
    fn unmapped_variants_fall_through_to_hub_error_with_source_message() {
        // Variants the merge helpers don't emit today (e.g., MissingFname, InvalidEthereumAddress)
        // fall through to HubError. We format the source `Display` into the HubError message so
        // the telemetry trail isn't lost. Regression guard: if the explicit mapping is ever
        // expanded to cover these, the assertion below will fail loudly and prompt updating.
        let out: MessageValidationError = EngineErr::MissingFname.into();
        match out {
            MessageValidationError::HubError(h) => {
                assert!(
                    h.message.contains("fname"),
                    "expected source message to carry 'fname', got: {}",
                    h.message
                );
            }
            other => panic!("expected HubError fallthrough, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod channel_message_inertness_tests {
    use super::MessageValidationError;
    use crate::core::util::FarcasterTime;
    use crate::core::validations::error::ValidationError;
    use crate::storage::db::RocksDbTransactionBatch;
    use crate::storage::store::account::StorageSlot;
    use crate::storage::store::block_engine_test_helpers;
    use crate::utils::factory::messages_factory;
    use crate::version::version::EngineVersion;

    /// Today every channel body dies at `validations::message::validate_message`, which
    /// `validate_user_message` calls before the custody lookup — so the second arm below is what
    /// actually fires, and the fid/signer registration is never reached. Both are deliberate.
    /// The registration keeps this test honest if a later increment installs real body validation:
    /// execution would then reach BlockEngine's own per-body allowlist and fail there (arm one)
    /// rather than dying at `MissingFid`, which would be a setup artifact rather than a real pin.
    /// The disjunction therefore asserts the property that matters — BlockEngine rejects channel
    /// messages — without pinning which of the two independent layers does it.
    #[test]
    fn channel_messages_are_rejected_by_block_engine_validation() {
        let (mut engine, _tmpdir) = block_engine_test_helpers::setup();
        let fid = 1234;
        block_engine_test_helpers::register_user(
            fid,
            block_engine_test_helpers::default_signer(),
            block_engine_test_helpers::default_custody_address(),
            1,
            &mut engine,
        );

        for (message_type, body) in messages_factory::channels::all_message_bodies() {
            let message =
                messages_factory::create_message_with_data(fid, message_type, body, None, None);
            let timestamp = FarcasterTime::new(message.data.as_ref().unwrap().timestamp as u64);
            let result = engine.validate_user_message(
                &message,
                &StorageSlot::new(0, 0, 1, u32::MAX),
                &timestamp,
                EngineVersion::V20,
                &mut RocksDbTransactionBatch::new(),
            );
            assert!(matches!(
                result,
                Err(MessageValidationError::InvalidMessageType)
                    | Err(MessageValidationError::MessageValidationError(
                        ValidationError::InvalidMessageType
                    ))
            ));
        }
    }
}
