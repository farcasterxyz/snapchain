use crate::{
    core::util::FarcasterTime,
    proto,
    storage::{
        db::{PageOptions, RocksDbTransactionBatch},
        store::{
            account::{MessagesPage, Store, StoreDef, UserDataStore, UsernameProofStore},
            engine::{ProposalSource, ShardEngine},
        },
        trie::merkle_trie::{self, TrieKey},
    },
    version::version::EngineVersion,
};

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

fn collect_messages<T: StoreDef + Clone>(store: Store<T>, fid: &u64) -> Vec<proto::Message> {
    collect_messages_with_page_options(|page_options| {
        store.get_all_messages_by_fid(*fid, None, None, page_options)
    })
}

fn collect_compact_state<T: StoreDef + Clone>(store: Store<T>, fid: &u64) -> Vec<proto::Message> {
    collect_messages_with_page_options(|page_options| {
        store.get_compact_state_messages_by_fid(*fid, page_options)
    })
}

/*
 *
 * This extension to the ShardEngine provides the ability to replicate FID state between
 * two nodes for a shard.
 *
 */
impl ShardEngine {
    fn build_user_messages_for_fid(&self, fid: u64) -> Vec<proto::Message> {
        let stores = self.get_stores();
        let mut messages = vec![];

        // Casts
        messages.append(&mut collect_messages(stores.cast_store.clone(), &fid));

        // Links
        messages.append(&mut collect_compact_state(stores.link_store.clone(), &fid));
        messages.append(&mut collect_messages(stores.link_store.clone(), &fid));

        // Reactions
        messages.append(&mut collect_messages(stores.reaction_store.clone(), &fid));

        // User Data
        messages.append(&mut collect_messages(stores.user_data_store.clone(), &fid));

        // Verifications
        messages.append(&mut collect_messages(
            stores.verification_store.clone(),
            &fid,
        ));

        // Username Proofs
        messages.append(&mut collect_messages(
            stores.username_proof_store.clone(),
            &fid,
        ));

        messages
    }

    fn build_on_chain_event_validator_messages_for_fid(
        &self,
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
            let events = self
                .get_stores()
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
        &self,
        fid: u64,
    ) -> Vec<proto::ValidatorMessage> {
        let mut validator_messages = vec![];

        let mut page_options = PageOptions::default();
        let mut messages = vec![];
        loop {
            let results = UsernameProofStore::get_username_proofs_by_fid(
                &self.get_stores().username_proof_store,
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

        // TODO: this appears to always be empty in the tests - how can we test the case
        // where this is present?
        println!("Found {} username proofs for fid {}", messages.len(), fid);

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

        match UserDataStore::get_username_proof_by_fid(&self.get_stores().user_data_store, fid) {
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

    fn build_system_messages_for_fid(&self, fid: u64) -> Vec<proto::ValidatorMessage> {
        let mut validator_messages = vec![];
        validator_messages.append(&mut self.build_on_chain_event_validator_messages_for_fid(fid));
        validator_messages.append(&mut self.build_username_proof_validator_messages_for_fid(fid));
        validator_messages
    }

    pub fn account_root_for_fid(&self, fid: u64) -> Vec<u8> {
        let stores = self.get_stores();
        stores.trie.get_hash(
            &stores.db,
            &mut RocksDbTransactionBatch::new(),
            &TrieKey::for_fid(fid),
        )
    }

    fn build_system_transaction(&self, fid: u64) -> proto::Transaction {
        proto::Transaction {
            fid,
            system_messages: self.build_system_messages_for_fid(fid),
            ..Default::default()
        }
    }

    fn build_user_transaction(&self, fid: u64) -> proto::Transaction {
        proto::Transaction {
            fid,
            user_messages: self.build_user_messages_for_fid(fid),
            account_root: self.account_root_for_fid(fid),
            ..Default::default()
        }
    }

    pub fn transactions_for_fids(
        &self,
        fids: &[u64],
    ) -> (Vec<proto::Transaction>, Vec<proto::Transaction>) {
        let mut sys = vec![];
        let mut user = vec![];
        for fid in fids {
            let sys_tx = self.build_system_transaction(*fid);
            let user_tx = self.build_user_transaction(*fid);
            sys.push(sys_tx);
            user.push(user_tx);
        }
        (sys, user)
    }

    fn replay_transactions(
        &mut self,
        transactions: Vec<proto::Transaction>,
    ) -> Result<(), crate::core::error::HubError> {
        let db = self.get_stores().db.clone();
        let mut tx_batch = RocksDbTransactionBatch::new();
        let ctx = merkle_trie::Context::new();

        for tx in transactions {
            let (_, _, validation_errors) = self
                .replay_snapchain_txn(
                    &ctx,
                    &tx,
                    &mut tx_batch,
                    ProposalSource::Commit,
                    EngineVersion::V5,
                    &FarcasterTime::current(),
                )
                .unwrap();

            if !validation_errors.is_empty() {
                return Err(crate::core::error::HubError {
                    code: "internal_error".to_string(),
                    message: format!("Validation errors found: {:?}", validation_errors),
                });
            }
        }

        db.commit(tx_batch).unwrap();
        Ok(())
    }

    pub fn replay_fid_transactions(
        &mut self,
        sys: Vec<proto::Transaction>,
        user: Vec<proto::Transaction>,
    ) -> Result<(), crate::core::error::HubError> {
        // TODO: should we validate before committing?
        self.replay_transactions(sys)?;
        self.replay_transactions(user)
    }
}
