use super::{
    get_many_messages, make_cast_id_key, make_fid_key, make_message_primary_key, make_user_key,
    read_fid_key, read_ts_hash, require_page_token_in_prefix,
    store::{FollowIndexGate, Store, StoreDef},
    MessagesPage, StoreEventHandler, PAGE_SIZE_MAX, TS_HASH_LENGTH,
};
use crate::core::channel_uri::{channel_id_for_follow_target, ChannelRegistrar};
use crate::storage::store::account::ChannelPage;
use crate::storage::store::account::CHANNEL_ID_LENGTH;
use crate::{core::error::HubError, proto::SignatureScheme, storage::store::account::StoreOptions};
use crate::{proto::message_data::Body, storage::db::PageOptions};
use crate::{
    proto::MessageData,
    storage::constants::{RootPrefix, UserPostfix},
};
use crate::{
    proto::{reaction_body::Target, ReactionBody, ReactionType},
    storage::util::increment_vec_u8,
};
use crate::{
    proto::{Message, MessageType},
    storage::db::{RocksDB, RocksDbTransactionBatch},
};
use std::{borrow::Borrow, sync::Arc};

/// Sub-index byte under [`RootPrefix::ChannelFollow`].
#[repr(u8)]
#[derive(Clone, Copy)]
enum ChannelFollowIndex {
    ByFid = 1,
    ByChannel = 2,
    // 3 was FollowerCount, a per-channel stored counter. Removed rather than
    // fixed: see `insert_follow`. Do not reuse the discriminant — a node that
    // bootstrapped while it existed may still hold rows under it.
}

/// Both directional follow keys are `[prefix][sub-index][32-byte id][4-byte id]`,
/// just with the two ids swapped, so one length covers both.
const FOLLOW_KEY_LENGTH: usize = 2 + CHANNEL_ID_LENGTH + 4;

/// One row of the channel-follow index.
///
/// Only the field the caller did not already supply carries new information —
/// `followers_by_channel` fills `fid`, `follows_by_fid` fills `channel_id` — but
/// both are present so a page maps to the wire without threading the query back
/// through.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelFollowEntry {
    pub fid: u64,
    /// Fixed-width, matching every other channel_id in the feature. Do not widen
    /// this to `Vec<u8>`: the key builders take `[u8; CHANNEL_ID_LENGTH]`
    /// precisely so the width is a type guarantee rather than a runtime check.
    pub channel_id: [u8; CHANNEL_ID_LENGTH],
    /// Farcaster timestamp of the reaction that expressed the follow.
    pub followed_at: u32,
}

#[derive(Clone)]
pub struct ReactionStoreDef {
    prune_size_limit: u32,
    /// The registrar whose ERC-721 tokens are followable, or `None` on a network
    /// with no registrar deployed. Resolved once at construction from
    /// [`channel_registrar_for_network`] — a per-network constant, never node
    /// config, because it decides the contents of a replicated derived index.
    registrar: Option<ChannelRegistrar>,
}

impl StoreDef for ReactionStoreDef {
    #[inline]
    fn postfix(&self) -> u8 {
        UserPostfix::ReactionMessage.as_u8()
    }

    #[inline]
    fn add_message_type(&self) -> u8 {
        MessageType::ReactionAdd as u8
    }

    #[inline]
    fn remove_message_type(&self) -> u8 {
        MessageType::ReactionRemove as u8
    }

    #[inline]
    fn is_add_type(&self, message: &Message) -> bool {
        if message.data.is_none() {
            return false;
        }
        let data = message.data.as_ref().unwrap();
        message.signature_scheme == SignatureScheme::Ed25519 as i32
            && data.r#type == MessageType::ReactionAdd as i32
            && data.body.is_some()
    }

    #[inline]
    fn is_remove_type(&self, message: &Message) -> bool {
        if message.data.is_none() {
            return false;
        }
        let data = message.data.as_ref().unwrap();
        message.signature_scheme == SignatureScheme::Ed25519 as i32
            && data.r#type == MessageType::ReactionRemove as i32
            && data.body.is_some()
    }

    #[inline]
    fn compact_state_message_type(&self) -> u8 {
        MessageType::None as u8
    }

    #[inline]
    fn is_compact_state_type(&self, _message: &Message) -> bool {
        false
    }

    #[inline]
    fn build_secondary_indices(
        &self,
        txn: &mut RocksDbTransactionBatch,
        ts_hash: &[u8; TS_HASH_LENGTH],
        message: &Message,
    ) -> Result<(), HubError> {
        let (by_target_key, rtype) = self.secondary_index_key(ts_hash, message)?;

        txn.put(by_target_key, vec![rtype]);

        Ok(())
    }

    #[inline]
    fn delete_secondary_indices(
        &self,
        txn: &mut RocksDbTransactionBatch,
        ts_hash: &[u8; TS_HASH_LENGTH],
        message: &Message,
    ) -> Result<(), HubError> {
        let (by_target_key, _) = self.secondary_index_key(ts_hash, message)?;

        txn.delete(by_target_key);

        Ok(())
    }

    #[inline]
    fn build_follow_index(
        &self,
        _db: &RocksDB,
        txn: &mut RocksDbTransactionBatch,
        message: &Message,
        gate: FollowIndexGate,
    ) -> Result<(), HubError> {
        if !gate.writes_follow_index() {
            return Ok(());
        }
        let Some(channel_id) = self.follow_channel_id(message) else {
            return Ok(());
        };
        let data = message.data.as_ref().unwrap();
        ReactionStoreDef::insert_follow(txn, data.fid, &channel_id, data.timestamp);
        Ok(())
    }

    #[inline]
    fn delete_follow_index(
        &self,
        _db: &RocksDB,
        txn: &mut RocksDbTransactionBatch,
        message: &Message,
    ) -> Result<(), HubError> {
        let Some(channel_id) = self.follow_channel_id(message) else {
            return Ok(());
        };
        ReactionStoreDef::remove_follow(txn, message.data.as_ref().unwrap().fid, &channel_id);
        Ok(())
    }

    #[inline]
    fn make_add_key(&self, message: &Message) -> Result<Vec<u8>, HubError> {
        let reaction_body = match message.data.as_ref().unwrap().body.as_ref().unwrap() {
            Body::ReactionBody(reaction_body) => reaction_body,
            _ => {
                return Err(HubError {
                    code: "bad_request.validation_failure".to_string(),
                    message: "Invalid reaction body".to_string(),
                })
            }
        };

        Self::make_reaction_adds_key(
            message.data.as_ref().unwrap().fid,
            reaction_body.r#type,
            reaction_body.target.as_ref(),
        )
    }

    #[inline]
    fn make_remove_key(&self, message: &Message) -> Result<Vec<u8>, HubError> {
        let reaction_body = match message.data.as_ref().unwrap().body.as_ref().unwrap() {
            Body::ReactionBody(reaction_body) => reaction_body,
            _ => {
                return Err(HubError {
                    code: "bad_request.validation_failure".to_string(),
                    message: "Invalid reaction body".to_string(),
                })
            }
        };

        Self::make_reaction_removes_key(
            message.data.as_ref().unwrap().fid,
            reaction_body.r#type,
            reaction_body.target.as_ref(),
        )
    }

    #[inline]
    fn make_compact_state_add_key(&self, _message: &Message) -> Result<Vec<u8>, HubError> {
        Err(HubError {
            code: "bad_request.invalid_param".to_string(),
            message: "Reaction Store doesn't support compact state".to_string(),
        })
    }

    #[inline]
    fn make_compact_state_prefix(&self, _fid: u64) -> Result<Vec<u8>, HubError> {
        Err(HubError {
            code: "bad_request.invalid_param".to_string(),
            message: "Reaction Store doesn't support compact state".to_string(),
        })
    }

    #[inline]
    fn get_prune_size_limit(&self) -> u32 {
        self.prune_size_limit
    }
}

impl ReactionStoreDef {
    /// Whether `message` expresses a channel follow, and which channel.
    ///
    /// THE SINGLE CLASSIFIER. Both `build_follow_index` and `delete_follow_index`
    /// go through this, for the same reason `secondary_index_key` below is shared
    /// by its own build/delete pair: if the two sides could ever disagree about
    /// what a message means, teardown would miss rows that the build wrote.
    ///
    /// The `ReactionType::Like` check is load-bearing, not decoration. A RECAST of
    /// the same channel URI produces a *different* reaction with the *same*
    /// `(fid, channel_id)`; without the type filter, removing that recast would
    /// delete the like's follow row and the fid would silently stop following.
    fn follow_channel_id(&self, message: &Message) -> Option<[u8; CHANNEL_ID_LENGTH]> {
        let registrar = self.registrar.as_ref()?;
        let body = match message.data.as_ref()?.body.as_ref()? {
            Body::ReactionBody(body) => body,
            _ => return None,
        };
        if body.r#type != ReactionType::Like as i32 {
            return None;
        }
        match body.target.as_ref()? {
            Target::TargetUrl(url) => channel_id_for_follow_target(url, registrar),
            Target::TargetCastId(_) => None,
        }
    }

    /// `[ChannelFollow][ByFid][fid][channel_id]` — "channels followed by fid".
    ///
    /// `channel_id` is `[u8; CHANNEL_ID_LENGTH]` by construction (it comes from
    /// `ChannelAssetId::parse`, which returns `U256::to_be_bytes()`), so the
    /// fixed-width prefix-freeness the channel slot keys have to check at runtime
    /// is a type-level guarantee here. Do not loosen these signatures to `&[u8]`.
    pub fn make_follow_by_fid_key(fid: u64, channel_id: &[u8; CHANNEL_ID_LENGTH]) -> Vec<u8> {
        let mut key = Self::follow_by_fid_prefix(fid);
        key.extend_from_slice(channel_id);
        key
    }

    pub fn follow_by_fid_prefix(fid: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(2 + 4 + CHANNEL_ID_LENGTH);
        key.push(RootPrefix::ChannelFollow as u8);
        key.push(ChannelFollowIndex::ByFid as u8);
        // Truncating u32 cast, as everywhere else fids are put on disk — see
        // `make_fid_key`. Erroring here instead would make an ordinary reaction
        // unmergeable for the sake of an index row.
        key.extend_from_slice(&make_fid_key(fid));
        key
    }

    /// `[ChannelFollow][ByChannel][channel_id][fid]` — "followers of channel", on
    /// this shard only. A global follower set is the union across every shard; see
    /// the fan-out reads in `server.rs`.
    pub fn make_follow_by_channel_key(channel_id: &[u8; CHANNEL_ID_LENGTH], fid: u64) -> Vec<u8> {
        let mut key = Self::follow_by_channel_prefix(channel_id);
        key.extend_from_slice(&make_fid_key(fid));
        key
    }

    pub fn follow_by_channel_prefix(channel_id: &[u8; CHANNEL_ID_LENGTH]) -> Vec<u8> {
        let mut key = Vec::with_capacity(2 + CHANNEL_ID_LENGTH + 4);
        key.push(RootPrefix::ChannelFollow as u8);
        key.push(ChannelFollowIndex::ByChannel as u8);
        key.extend_from_slice(channel_id);
        key
    }

    /// Records `fid` as following `channel_id`.
    ///
    /// INFALLIBLE BY CONSTRUCTION, and that is load-bearing rather than
    /// incidental. An earlier version maintained a per-channel follower counter
    /// here, which made this a read-modify-write with two ways to fail. Both were
    /// worse than they appeared:
    ///
    /// * Replication bootstrap runs `num_parallel_vts` concurrent tasks, each
    ///   with its own `RocksDbTransactionBatch`. Virtual trie shards partition by
    ///   fid, so the two directional rows are disjoint across tasks — but a
    ///   counter key is per-channel and therefore shared. Two fids in different
    ///   tasks following one channel would both read N and both write N+1, and
    ///   the batch-local read that makes a same-batch supersede net zero cannot
    ///   see across batches.
    /// * That undercount later underflowed on unfollow, and an `Err` from this
    ///   function reaches `merge_message`, which then skips `update_trie`. A
    ///   deliberately non-authoritative, off-trie index could therefore make a
    ///   node reject a message its peers accepted — the exact consensus exposure
    ///   `FollowIndexGate` argues these rows do not have.
    ///
    /// `follower_count` derives the count by scanning instead. Both rows are
    /// written unconditionally, so a re-merge refreshes `followed_at` and a
    /// supersede is idempotent.
    fn insert_follow(
        txn: &mut RocksDbTransactionBatch,
        fid: u64,
        channel_id: &[u8; CHANNEL_ID_LENGTH],
        followed_at: u32,
    ) {
        let followed_at = followed_at.to_be_bytes().to_vec();
        txn.put(
            Self::make_follow_by_fid_key(fid, channel_id),
            followed_at.clone(),
        );
        txn.put(
            Self::make_follow_by_channel_key(channel_id, fid),
            followed_at,
        );
    }

    /// Reverts what `insert_follow` wrote, if anything.
    ///
    /// Also infallible, and needs no presence check: deleting an absent key is a
    /// no-op. That is exactly right for a reaction that was never indexed — one
    /// merged before `ChannelFollows` activated, or one whose URI names another
    /// contract — and it is what lets teardown be ungated. See `FollowIndexGate`.
    ///
    /// The two rows are written and deleted together and never independently, so
    /// they cannot drift apart.
    fn remove_follow(
        txn: &mut RocksDbTransactionBatch,
        fid: u64,
        channel_id: &[u8; CHANNEL_ID_LENGTH],
    ) {
        txn.delete(Self::make_follow_by_fid_key(fid, channel_id));
        txn.delete(Self::make_follow_by_channel_key(channel_id, fid));
    }

    fn secondary_index_key(
        &self,
        ts_hash: &[u8; TS_HASH_LENGTH],
        message: &Message,
    ) -> Result<(Vec<u8>, u8), HubError> {
        // Make sure at least one of targetCastId or targetUrl is set
        let reaction_body = match message.data.as_ref().unwrap().body.as_ref().unwrap() {
            Body::ReactionBody(reaction_body) => reaction_body,
            _ => Err(HubError {
                code: "bad_request.validation_failure".to_string(),
                message: "Invalid reaction body".to_string(),
            })?,
        };
        let target = reaction_body.target.as_ref().ok_or(HubError {
            code: "bad_request.validation_failure".to_string(),
            message: "Invalid reaction body".to_string(),
        })?;

        let by_target_key = ReactionStoreDef::make_reactions_by_target_key(
            target,
            message.data.as_ref().unwrap().fid,
            Some(ts_hash),
        );

        Ok((by_target_key, reaction_body.r#type as u8))
    }

    pub fn make_reactions_by_target_key(
        target: &Target,
        fid: u64,
        ts_hash: Option<&[u8; TS_HASH_LENGTH]>,
    ) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 28 + 24 + 4);

        key.push(RootPrefix::ReactionsByTarget as u8); // ReactionsByTarget prefix, 1 byte
        key.extend_from_slice(&Self::make_target_key(target));
        if ts_hash.is_some() && ts_hash.unwrap().len() == TS_HASH_LENGTH {
            key.extend_from_slice(ts_hash.unwrap());
        }
        if fid > 0 {
            key.extend_from_slice(&make_fid_key(fid));
        }

        key
    }

    pub fn make_target_key(target: &Target) -> Vec<u8> {
        match target {
            Target::TargetUrl(url) => url.as_bytes().to_vec(),
            Target::TargetCastId(cast_id) => make_cast_id_key(cast_id),
        }
    }

    pub fn make_reaction_adds_key(
        fid: u64,
        r#type: i32,
        target: Option<&Target>,
    ) -> Result<Vec<u8>, HubError> {
        if target.is_some() && r#type == 0 {
            return Err(HubError {
                code: "bad_request.validation_failure".to_string(),
                message: "targetId provided without type".to_string(),
            });
        }
        let mut key = Vec::with_capacity(33 + 1 + 1 + 28);

        key.extend_from_slice(&make_user_key(fid));
        key.push(UserPostfix::ReactionAdds as u8); // ReactionAdds postfix, 1 byte
        if r#type > 0 {
            key.push(r#type as u8); // type, 1 byte
        }
        if target.is_some() {
            // target, 28 bytes
            key.extend_from_slice(&Self::make_target_key(target.unwrap()));
        }

        Ok(key)
    }

    pub fn make_reaction_removes_key(
        fid: u64,
        r#type: i32,
        target: Option<&Target>,
    ) -> Result<Vec<u8>, HubError> {
        if target.is_some() && r#type == 0 {
            return Err(HubError {
                code: "bad_request.validation_failure".to_string(),
                message: "targetId provided without type".to_string(),
            });
        }
        let mut key = Vec::with_capacity(33 + 1 + 1 + 28);

        key.extend_from_slice(&make_user_key(fid));
        key.push(UserPostfix::ReactionRemoves as u8); // ReactionRemoves postfix, 1 byte
        if r#type > 0 {
            key.push(r#type as u8); // type, 1 byte
        }
        if target.is_some() {
            key.extend_from_slice(&Self::make_target_key(target.unwrap()));
            // target, 28 bytes
        }

        Ok(key)
    }
}

pub struct ReactionStore {}

impl ReactionStore {
    /// `registrar` is an explicit argument rather than something the store looks
    /// up, so every caller — production and test alike — has to say which network's
    /// registrar it means. `None` disables follow indexing entirely.
    pub fn new(
        db: Arc<RocksDB>,
        store_event_handler: Arc<StoreEventHandler>,
        prune_size_limit: u32,
        registrar: Option<ChannelRegistrar>,
    ) -> Store<ReactionStoreDef> {
        Store::new_with_store_def(
            db,
            store_event_handler,
            ReactionStoreDef {
                prune_size_limit,
                registrar,
            },
        )
    }

    pub fn new_with_opts(
        db: Arc<RocksDB>,
        store_event_handler: Arc<StoreEventHandler>,
        prune_size_limit: u32,
        store_opts: StoreOptions,
        registrar: Option<ChannelRegistrar>,
    ) -> Store<ReactionStoreDef> {
        Store::new_with_store_def_opts(
            db,
            store_event_handler,
            ReactionStoreDef {
                prune_size_limit,
                registrar,
            },
            store_opts,
        )
    }

    /// Followers of `channel_id` on this shard, ascending by fid.
    pub fn followers_by_channel(
        store: &Store<ReactionStoreDef>,
        channel_id: &[u8; CHANNEL_ID_LENGTH],
        page_options: &PageOptions,
    ) -> Result<ChannelPage<ChannelFollowEntry>, HubError> {
        let prefix = ReactionStoreDef::follow_by_channel_prefix(channel_id);
        Self::follow_page(store, prefix, page_options, |suffix, followed_at| {
            Ok(ChannelFollowEntry {
                fid: read_fid_key(suffix, 0),
                channel_id: *channel_id,
                followed_at,
            })
        })
    }

    /// Channels followed by `fid`, from that fid's own shard.
    pub fn follows_by_fid(
        store: &Store<ReactionStoreDef>,
        fid: u64,
        page_options: &PageOptions,
    ) -> Result<ChannelPage<ChannelFollowEntry>, HubError> {
        let prefix = ReactionStoreDef::follow_by_fid_prefix(fid);
        Self::follow_page(store, prefix, page_options, move |suffix, followed_at| {
            Ok(ChannelFollowEntry {
                fid,
                // `follow_page` has already rejected any key whose suffix is not
                // exactly CHANNEL_ID_LENGTH, so this cannot fail — but it is
                // surfaced rather than defaulted, because a silent zero
                // channel_id would be indistinguishable from a real row.
                channel_id: suffix.try_into().map_err(|_| {
                    HubError::invalid_internal_state("channel follow key has invalid suffix")
                })?,
                followed_at,
            })
        })
    }

    /// Shared scan for the two directional indexes. Both have fixed-width keys
    /// and a 4-byte `followed_at` value, so a row of any other shape is this
    /// node's own derived state gone wrong — it fails loudly rather than being
    /// skipped, which would silently under-report a follower list.
    fn follow_page(
        store: &Store<ReactionStoreDef>,
        prefix: Vec<u8>,
        page_options: &PageOptions,
        mut entry: impl FnMut(&[u8], u32) -> Result<ChannelFollowEntry, HubError>,
    ) -> Result<ChannelPage<ChannelFollowEntry>, HubError> {
        require_page_token_in_prefix(&prefix, page_options)?;
        let suffix_length = FOLLOW_KEY_LENGTH - prefix.len();
        let page_size = page_options.page_size.unwrap_or(PAGE_SIZE_MAX);
        let mut entries = Vec::new();
        let mut last_key = None;
        let all_done = store.db().for_each_iterator_by_prefix(
            Some(prefix.clone()),
            Some(increment_vec_u8(&prefix)),
            page_options,
            |key, value| {
                if key.len() != prefix.len() + suffix_length {
                    return Err(HubError::invalid_internal_state(
                        "channel follow key has invalid length",
                    ));
                }
                let followed_at: [u8; 4] = value.try_into().map_err(|_| {
                    HubError::invalid_internal_state("channel follow row has invalid length")
                })?;
                entries.push(entry(
                    &key[prefix.len()..],
                    u32::from_be_bytes(followed_at),
                )?);
                last_key = Some(key.to_vec());
                Ok(entries.len() >= page_size)
            },
        )?;
        Ok(ChannelPage {
            entries,
            next_page_token: (!all_done).then_some(last_key).flatten(),
        })
    }

    /// This shard's share of `channel_id`'s follower count. A global count is the
    /// sum across every shard — reactions live on their author's shard, so no one
    /// shard holds the whole set.
    ///
    /// Counted by scanning the by-channel prefix rather than read from a stored
    /// counter; `insert_follow` explains why the counter was removed. Two
    /// consequences worth knowing:
    ///
    /// * The cost is linear in the channel's followers ON THIS SHARD, not a point
    ///   lookup. Callers on a hot path should prefer paging `followers_by_channel`
    ///   over polling this.
    /// * It cannot disagree with `followers_by_channel`, because it counts the
    ///   same rows. A stored counter could, and had no way to notice.
    pub fn follower_count(
        store: &Store<ReactionStoreDef>,
        channel_id: &[u8; CHANNEL_ID_LENGTH],
    ) -> Result<u64, HubError> {
        let prefix = ReactionStoreDef::follow_by_channel_prefix(channel_id);
        let mut count = 0u64;
        store.db().for_each_iterator_by_prefix(
            Some(prefix.clone()),
            Some(increment_vec_u8(&prefix)),
            &PageOptions::default(),
            |_, _| {
                count += 1;
                // Never stop early: this is a count, not a page.
                Ok(false)
            },
        )?;
        Ok(count)
    }

    /// The `followed_at` of `fid`'s follow of `channel_id`, or `None`. Answerable
    /// from `fid`'s own shard alone.
    pub fn is_following(
        store: &Store<ReactionStoreDef>,
        fid: u64,
        channel_id: &[u8; CHANNEL_ID_LENGTH],
    ) -> Result<Option<u32>, HubError> {
        match store
            .db()
            .get(&ReactionStoreDef::make_follow_by_fid_key(fid, channel_id))?
        {
            None => Ok(None),
            Some(value) => {
                let followed_at: [u8; 4] = value.try_into().map_err(|_| {
                    HubError::invalid_internal_state("channel follow row has invalid length")
                })?;
                Ok(Some(u32::from_be_bytes(followed_at)))
            }
        }
    }

    pub fn get_reaction_add(
        store: &Store<ReactionStoreDef>,
        fid: u64,
        r#type: i32,
        target: Option<Target>,
    ) -> Result<Option<Message>, HubError> {
        let partial_message = Message {
            data: Some(MessageData {
                fid,
                r#type: MessageType::ReactionAdd.into(),
                body: Some(Body::ReactionBody(ReactionBody {
                    r#type,
                    target: target.clone(),
                })),
                ..Default::default()
            }),
            ..Default::default()
        };

        store.get_add(&partial_message, None)
    }

    pub fn get_reaction_remove(
        store: &Store<ReactionStoreDef>,
        fid: u64,
        r#type: i32,
        target: Option<Target>,
    ) -> Result<Option<Message>, HubError> {
        let partial_message = Message {
            data: Some(MessageData {
                fid,
                r#type: MessageType::ReactionRemove.into(),
                body: Some(Body::ReactionBody(ReactionBody {
                    r#type,
                    target: target.clone(),
                })),
                ..Default::default()
            }),
            ..Default::default()
        };

        let r = store.get_remove(&partial_message, None);
        // println!("got reaction remove: {:?}", r);

        r
    }

    pub fn get_reaction_adds_by_fid(
        store: &Store<ReactionStoreDef>,
        fid: u64,
        reaction_type: i32,
        page_options: &PageOptions,
    ) -> Result<MessagesPage, HubError> {
        store.get_adds_by_fid(
            fid,
            page_options,
            Some(|message: &Message| {
                if let Some(reaction_body) = &message.data.as_ref().unwrap().body {
                    if let Body::ReactionBody(reaction_body) = reaction_body {
                        if reaction_type == 0 || reaction_body.r#type == reaction_type {
                            return true;
                        }
                    }
                }

                false
            }),
        )
    }

    pub fn get_reaction_removes_by_fid(
        store: &Store<ReactionStoreDef>,
        fid: u64,
        reaction_type: i32,
        page_options: &PageOptions,
    ) -> Result<MessagesPage, HubError> {
        store.get_removes_by_fid(
            fid,
            page_options,
            Some(|message: &Message| {
                if let Some(reaction_body) = &message.data.as_ref().unwrap().body {
                    if let Body::ReactionBody(reaction_body) = reaction_body {
                        if reaction_type == 0 || reaction_body.r#type == reaction_type {
                            return true;
                        }
                    }
                }

                false
            }),
        )
    }

    pub fn get_reactions_by_target(
        store: &Store<ReactionStoreDef>,
        target: &Target,
        reaction_type: i32,
        page_options: &PageOptions,
    ) -> Result<MessagesPage, HubError> {
        let start_prefix = ReactionStoreDef::make_reactions_by_target_key(target, 0, None);

        let mut message_keys = vec![];
        let mut last_key = vec![];

        store.db().for_each_iterator_by_prefix(
            Some(start_prefix.to_vec()),
            Some(increment_vec_u8(&start_prefix)),
            page_options,
            |key, value| {
                if reaction_type == ReactionType::None as i32
                    || (value.len() == 1 && value[0] == reaction_type as u8)
                {
                    let ts_hash_offset = start_prefix.len();
                    let fid_offset = ts_hash_offset + TS_HASH_LENGTH;

                    let fid = read_fid_key(key, fid_offset);
                    let ts_hash = read_ts_hash(key, ts_hash_offset);
                    let message_primary_key =
                        make_message_primary_key(fid, store.postfix(), Some(&ts_hash));

                    message_keys.push(message_primary_key.to_vec());
                    if message_keys.len() >= page_options.page_size.unwrap_or(PAGE_SIZE_MAX) {
                        last_key = key.to_vec();
                        return Ok(true); // Stop iterating
                    }
                }

                Ok(false) // Continue iterating
            },
        )?;

        let messages = get_many_messages(store.db().borrow(), message_keys)?;
        let next_page_token = if !last_key.is_empty() {
            Some(last_key.to_vec())
        } else {
            None
        };

        Ok(MessagesPage {
            messages,
            next_page_token,
        })
    }
}
