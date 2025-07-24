use tokio::select;
use tracing::error;

use crate::{
    core::util,
    proto,
    replication::{error::ReplicationError, replication_stores::ReplicationStores},
    storage::{
        db::{PageOptions, RocksDbTransactionBatch},
        store::{
            account::{MessagesPage, Store, StoreDef, UserDataStore, UsernameProofStore},
            engine::PostCommitMessage,
            stores::Stores,
        },
        trie::merkle_trie::TrieKey,
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

type ReplicationTransactions = (Vec<proto::Transaction>, Vec<proto::Transaction>);
type ReplicationTransaction = (Option<proto::Transaction>, Option<proto::Transaction>);

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
    ) -> Result<ReplicationTransaction, ReplicationError> {
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

        // Build system and user transactions for the given fid
        let sys_tx = build_system_transaction(&stores, fid);
        let user_tx = build_user_transaction(&stores, fid);

        Ok((sys_tx, user_tx))
    }

    pub fn latest_transactions_for_fid(
        &self,
        shard: u32,
        fid: u64,
    ) -> Result<ReplicationTransaction, ReplicationError> {
        let height = self.stores.max_height_for_shard(shard).ok_or_else(|| {
            ReplicationError::StoreNotFound(
                shard,
                0,
                "No stores found for the given shard".to_string(),
            )
        })?;

        self.transactions_for_fid(height, shard, fid)
    }

    // Fetches a set of system and user transactions that represent the current
    // state of the given fid range. Start and end are inclusive, i.e. [start, end].
    pub fn transactions_for_fid_range(
        &self,
        height: u64,
        shard: u32,
        start_fid: u64,
        end_fid: u64,
    ) -> Result<ReplicationTransactions, ReplicationError> {
        let mut sys = vec![];
        let mut user = vec![];

        for fid in start_fid..=end_fid {
            let (sys_tx, user_tx) = self.transactions_for_fid(height, shard, fid)?;

            if let Some(sys_tx) = sys_tx {
                sys.push(sys_tx);
            }

            if let Some(user_tx) = user_tx {
                user.push(user_tx);
            }
        }

        Ok((sys, user))
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

fn collect_messages_with_page_options<
    T: Fn(&PageOptions) -> Result<MessagesPage, crate::core::error::HubError>,
>(
    f: T,
) -> Vec<proto::Message> {
    let mut page_options = PageOptions::default();
    let mut messages = vec![];

    loop {
        let results = f(&page_options).unwrap();

        if results.messages.is_empty() {
            break;
        }

        messages.extend(results.messages);

        if results.next_page_token.is_none() {
            break;
        }

        page_options.page_token = results.next_page_token;
    }

    messages
}

fn collect_messages<T: StoreDef + Clone>(store: &Store<T>, fid: &u64) -> Vec<proto::Message> {
    collect_messages_with_page_options(|page_options| {
        store.get_all_messages_by_fid(*fid, None, None, page_options)
    })
}

fn collect_compact_state<T: StoreDef + Clone>(store: &Store<T>, fid: &u64) -> Vec<proto::Message> {
    collect_messages_with_page_options(|page_options| {
        store.get_compact_state_messages_by_fid(*fid, page_options)
    })
}

fn build_user_messages_for_fid(stores: &Stores, fid: u64) -> Vec<proto::Message> {
    let mut messages = vec![];

    // Casts
    messages.append(&mut collect_messages(&stores.cast_store, &fid));

    // Links
    messages.append(&mut collect_compact_state(&stores.link_store, &fid));
    messages.append(&mut collect_messages(&stores.link_store, &fid));

    // Reactions
    messages.append(&mut collect_messages(&stores.reaction_store, &fid));

    // User Data
    messages.append(&mut collect_messages(&stores.user_data_store, &fid));

    // Verifications
    messages.append(&mut collect_messages(&stores.verification_store, &fid));

    // Username Proofs
    messages.append(&mut collect_messages(&stores.username_proof_store, &fid));

    messages
}

fn account_root_for_fid(stores: &Stores, fid: u64) -> Vec<u8> {
    stores.trie.get_hash(
        &stores.db,
        &mut RocksDbTransactionBatch::new(),
        &TrieKey::for_fid(fid),
    )
}

fn build_user_transaction(stores: &Stores, fid: u64) -> Option<proto::Transaction> {
    let user_messages = build_user_messages_for_fid(stores, fid);
    let account_root = account_root_for_fid(stores, fid);

    if user_messages.is_empty() && account_root.is_empty() {
        return None;
    }

    Some(proto::Transaction {
        fid,
        user_messages: build_user_messages_for_fid(stores, fid),
        account_root: account_root_for_fid(stores, fid),
        ..Default::default()
    })
}

fn build_on_chain_event_validator_messages_for_fid(
    stores: &Stores,
    fid: u64,
) -> Vec<proto::ValidatorMessage> {
    let mut validator_messages = vec![];
    vec![
        proto::OnChainEventType::EventTypeSigner,
        proto::OnChainEventType::EventTypeSignerMigrated,
        proto::OnChainEventType::EventTypeIdRegister,
        proto::OnChainEventType::EventTypeStorageRent,
        proto::OnChainEventType::EventTypeTierPurchase,
    ]
    .iter()
    .for_each(|event_type| {
        let events = stores
            .onchain_event_store
            .get_onchain_events(*event_type, Some(fid))
            .unwrap()
            .into_iter()
            .map(|event| proto::ValidatorMessage {
                on_chain_event: Some(event),
                ..Default::default()
            });

        validator_messages.extend(events);
    });
    validator_messages
}

fn build_username_proof_validator_messages_for_fid(
    stores: &Stores,
    fid: u64,
) -> Vec<proto::ValidatorMessage> {
    let mut validator_messages = vec![];

    let mut page_options = PageOptions::default();
    let mut messages = vec![];
    loop {
        let results = UsernameProofStore::get_username_proofs_by_fid(
            &stores.username_proof_store,
            fid,
            &page_options,
        )
        .unwrap();

        if results.messages.is_empty() {
            break;
        }

        messages.extend(results.messages);

        if results.next_page_token.is_none() {
            break;
        }

        page_options.page_token = results.next_page_token;
    }

    for message in messages {
        let username_proof = match message.data.unwrap().body.unwrap() {
            proto::message_data::Body::UsernameProofBody(username_proof) => username_proof,
            _ => continue, // Skip if not a UsernameProof
        };

        let fname_transfer = proto::FnameTransfer {
            proof: Some(username_proof),
            ..Default::default()
        };
        validator_messages.push(proto::ValidatorMessage {
            fname_transfer: Some(fname_transfer),
            ..Default::default()
        });
    }

    match UserDataStore::get_username_proof_by_fid(&stores.user_data_store, fid) {
        Ok(proof) => {
            let fname_transfer = proto::FnameTransfer {
                proof: proof.clone(),
                ..Default::default()
            };

            validator_messages.push(proto::ValidatorMessage {
                fname_transfer: Some(fname_transfer),
                ..Default::default()
            });
        }
        _ => todo!("Handle error case for getting username proof by fid"),
    }

    validator_messages
}

fn build_system_messages_for_fid(stores: &Stores, fid: u64) -> Vec<proto::ValidatorMessage> {
    let mut messages = vec![];
    messages.append(&mut build_on_chain_event_validator_messages_for_fid(
        stores, fid,
    ));
    messages.append(&mut build_username_proof_validator_messages_for_fid(
        stores, fid,
    ));
    messages
}

fn build_system_transaction(stores: &Stores, fid: u64) -> Option<proto::Transaction> {
    let messages = build_system_messages_for_fid(stores, fid);
    if messages.is_empty() {
        return None;
    }

    Some(proto::Transaction {
        fid,
        system_messages: build_system_messages_for_fid(stores, fid),
        ..Default::default()
    })
}
