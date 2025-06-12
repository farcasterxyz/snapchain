use std::collections::HashMap;

use crate::{
    proto::{self, sync_service_server::SyncService, OnChainEventType, Transaction},
    storage::{
        db::{PageOptions, RocksDbTransactionBatch},
        store::{
            account::{Store, StoreDef},
            stores::Stores,
        },
        trie::merkle_trie::TrieKey,
    },
};

fn collect_messages<T: StoreDef + Clone>(store: Store<T>, fid: &u64) -> Vec<proto::Message> {
    let mut page_options = PageOptions::default();
    let mut messages = vec![];
    loop {
        let results = store
            .get_all_messages_by_fid(*fid, None, None, &PageOptions::default())
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

    messages
}

pub struct SyncServer {
    shard_stores: HashMap<u32, Stores>,
}

impl SyncServer {
    pub fn new(shard_stores: HashMap<u32, Stores>) -> Self {
        Self { shard_stores }
    }

    fn build_user_messages_for_fid(&self, fid: u64, stores: &Stores) -> Vec<proto::Message> {
        let mut messages = vec![];
        messages.append(&mut collect_messages(stores.cast_store.clone(), &fid));
        messages.append(&mut collect_messages(stores.link_store.clone(), &fid));
        messages.append(&mut collect_messages(stores.reaction_store.clone(), &fid));
        messages.append(&mut collect_messages(stores.user_data_store.clone(), &fid));
        messages.append(&mut collect_messages(
            stores.verification_store.clone(),
            &fid,
        ));
        messages.append(&mut collect_messages(
            stores.username_proof_store.clone(),
            &fid,
        ));

        messages
    }

    fn build_system_messages_for_fid(
        &self,
        fid: u64,
        stores: &Stores,
    ) -> Vec<proto::ValidatorMessage> {
        let mut validator_messages = vec![];
        vec![
            OnChainEventType::EventTypeSigner,
            OnChainEventType::EventTypeSignerMigrated,
            OnChainEventType::EventTypeIdRegister,
            OnChainEventType::EventTypeStorageRent,
            OnChainEventType::EventTypeTierPurchase,
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

    fn account_root_for_fid(&self, fid: u64, stores: &Stores) -> Vec<u8> {
        stores.trie.get_hash(
            &stores.db,
            &mut RocksDbTransactionBatch::new(),
            &TrieKey::for_fid(fid),
        )
    }

    fn build_fid_transactions(
        &self,
        fid: u64,
        stores: &Stores,
    ) -> (proto::Transaction, proto::Transaction) {
        (
            proto::Transaction {
                fid,
                system_messages: self.build_system_messages_for_fid(fid, stores),
                ..Default::default()
            },
            proto::Transaction {
                fid,
                user_messages: self.build_user_messages_for_fid(fid, stores),
                account_root: self.account_root_for_fid(fid, stores),
                ..Default::default()
            },
        )
    }
}

#[tonic::async_trait]
impl SyncService for SyncServer {
    async fn get_sync_transactions(
        &self,
        request: tonic::Request<proto::GetSyncTransactionsRequest>,
    ) -> Result<tonic::Response<proto::GetSyncTransactionsResponse>, tonic::Status> {
        let request = request.into_inner();
        let stores = self.shard_stores.get(&request.shard_id).unwrap();
        let mut transactions = vec![];

        for fid in request.fids.iter() {
            let (system_tx, user_tx) = self.build_fid_transactions(*fid, stores);
            transactions.push(system_tx);
            transactions.push(user_tx);
        }

        let response = proto::GetSyncTransactionsResponse {
            transactions: transactions,
        };

        Ok(tonic::Response::new(response))
    }
}
