//! End-to-end merge tests for KEY_ADD, focused on the resubmission semantics added by
//! NEYN-10624: a same-FID KEY_ADD from the same originating app should upsert scope/TTL on an
//! existing gasless record rather than being rejected. Tests build fully-signed KEY_ADD
//! messages (SignedKeyRequest metadata + EIP-712 custody signature) so they exercise the real
//! crypto validation path alongside the state transitions.

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use alloy_signer_local::PrivateKeySigner;
    use ed25519_dalek::SigningKey;
    use tempfile::TempDir;

    use crate::core::validations::error::ValidationError;
    use crate::proto::{self, message_data::Body, IdRegisterEventType, MessageType};
    use crate::storage::db::{self, RocksDbTransactionBatch};
    use crate::storage::store::account::{
        get_gasless_key_owner_fid, get_gasless_key_record, get_last_used_at, merge_key_add,
        OnchainEventStore, StoreEventHandler,
    };
    use crate::storage::store::engine::MessageValidationError;
    use crate::utils::factory::{events_factory, messages_factory, signers};

    /// Returns the 20-byte Ethereum address for a `PrivateKeySigner`, encoded as the custody
    /// address form expected by `IdRegisterEventBody.to`.
    fn address_bytes(signer: &PrivateKeySigner) -> Vec<u8> {
        signer.address().as_slice().to_vec()
    }

    /// Test scratch-pad: every ingredient `merge_key_add` inspects, in one mutable place. Callers
    /// tweak fields (scopes, ttl, nonce) and call [`Fixture::build`], which delegates to
    /// `messages_factory::keys::create_key_add` so the EIP-712 + envelope signing logic lives in
    /// exactly one place.
    struct Fixture {
        fid: u64,
        fid_custody: PrivateKeySigner,
        request_fid: u64,
        app_custody: PrivateKeySigner,
        envelope_signer: SigningKey,
        envelope_signer_pubkey: [u8; 32],
        scopes: Vec<i32>,
        ttl: u32,
        nonce: u32,
        deadline: u32,
        timestamp: u32,
    }

    impl Fixture {
        fn build(&self) -> proto::Message {
            let scopes: Vec<MessageType> = self
                .scopes
                .iter()
                .map(|s| MessageType::try_from(*s).unwrap())
                .collect();
            messages_factory::keys::create_key_add(
                self.fid,
                &self.fid_custody,
                self.request_fid,
                &self.app_custody,
                &self.envelope_signer,
                scopes,
                self.ttl,
                self.nonce,
                self.deadline,
                Some(self.timestamp),
            )
        }
    }

    struct World {
        db: Arc<db::RocksDB>,
        store: OnchainEventStore,
        _dir: TempDir,
    }

    fn new_world() -> World {
        let dir = tempfile::TempDir::new().unwrap();
        let db_path = dir.path().join("gasless_key_merge.db");
        let rocks = db::RocksDB::new(db_path.to_str().unwrap());
        rocks.open().unwrap();
        let rocks = Arc::new(rocks);
        let store = OnchainEventStore::new(rocks.clone(), StoreEventHandler::new());
        World {
            db: rocks,
            store,
            _dir: dir,
        }
    }

    /// Registers the custody address for `fid` so `get_id_register_event_by_fid` resolves it
    /// during `merge_key_add`.
    fn register_custody(world: &World, fid: u64, custody: &PrivateKeySigner) {
        let event = events_factory::create_id_register_event(
            fid,
            IdRegisterEventType::Register,
            address_bytes(custody),
            None,
        );
        let mut txn = RocksDbTransactionBatch::new();
        world.store.merge_onchain_event(event, &mut txn).unwrap();
        world.db.commit(txn).unwrap();
    }

    fn fresh_fixture(fid: u64, request_fid: u64) -> Fixture {
        let envelope = signers::generate_signer();
        let mut pubkey = [0u8; 32];
        pubkey.copy_from_slice(envelope.verifying_key().as_bytes());
        Fixture {
            fid,
            fid_custody: PrivateKeySigner::random(),
            request_fid,
            app_custody: PrivateKeySigner::random(),
            envelope_signer: envelope,
            envelope_signer_pubkey: pubkey,
            scopes: vec![MessageType::CastAdd as i32],
            ttl: 3600,
            nonce: 1,
            deadline: 1_700_000_000,
            timestamp: 1_000_000_000,
        }
    }

    fn commit_merge(world: &World, msg: &proto::Message) {
        let mut txn = RocksDbTransactionBatch::new();
        merge_key_add(&world.db, &world.store, msg, &mut txn, false).unwrap();
        world.db.commit(txn).unwrap();
    }

    #[test]
    fn first_time_key_add_succeeds_and_persists_record() {
        let world = new_world();
        let f = fresh_fixture(7, 99);
        register_custody(&world, f.fid, &f.fid_custody);
        register_custody(&world, f.request_fid, &f.app_custody);

        commit_merge(&world, &f.build());

        let txn = RocksDbTransactionBatch::new();
        assert_eq!(
            get_gasless_key_owner_fid(&world.db, &txn, &f.envelope_signer_pubkey).unwrap(),
            Some(f.fid),
        );
        let stored = get_gasless_key_record(&world.db, &txn, f.fid, &f.envelope_signer_pubkey)
            .unwrap()
            .expect("record should exist");
        assert_eq!(stored.request_fid, f.request_fid);
    }

    #[test]
    fn resubmit_with_modified_scope_upserts_record() {
        let world = new_world();
        let mut f = fresh_fixture(7, 99);
        register_custody(&world, f.fid, &f.fid_custody);
        register_custody(&world, f.request_fid, &f.app_custody);

        // First KEY_ADD: scope = [CastAdd], ttl = 3600, nonce = 1.
        commit_merge(&world, &f.build());

        // Second KEY_ADD: broaden scope, bump TTL and timestamp, advance nonce. Same app
        // (same request_fid + same app_custody) and same envelope key as before.
        f.scopes = vec![MessageType::CastAdd as i32, MessageType::ReactionAdd as i32];
        f.ttl = 7200;
        f.nonce = 2;
        f.timestamp = 1_000_000_500;
        commit_merge(&world, &f.build());

        // The stored record's embedded message reflects the new scope/TTL — `active_key.rs`
        // re-derives the scope mask and TTL from `record.message` on every read, so updating
        // the embedded message is sufficient to change admission behavior.
        let txn = RocksDbTransactionBatch::new();
        let stored = get_gasless_key_record(&world.db, &txn, f.fid, &f.envelope_signer_pubkey)
            .unwrap()
            .unwrap();
        let body = match stored
            .message
            .as_ref()
            .unwrap()
            .data
            .as_ref()
            .unwrap()
            .body
            .as_ref()
            .unwrap()
        {
            Body::KeyAddBody(b) => b,
            _ => panic!("expected KeyAddBody"),
        };
        assert_eq!(
            body.scopes,
            vec![MessageType::CastAdd as i32, MessageType::ReactionAdd as i32],
        );
        assert_eq!(body.ttl, 7200);

        // last_used_at resets to the resubmission's timestamp — the sliding-TTL window
        // restarts on each accepted KEY_ADD (NEYN-10624 policy).
        let last_used = get_last_used_at(&world.db, &txn, f.fid, &f.envelope_signer_pubkey)
            .unwrap()
            .unwrap();
        assert_eq!(last_used, 1_000_000_500);
    }

    #[test]
    fn resubmit_returns_prior_message_in_deleted_messages() {
        let world = new_world();
        let mut f = fresh_fixture(7, 99);
        register_custody(&world, f.fid, &f.fid_custody);
        register_custody(&world, f.request_fid, &f.app_custody);

        let first = f.build();
        commit_merge(&world, &first);

        f.scopes = vec![MessageType::LinkAdd as i32];
        f.nonce = 2;
        f.timestamp = 1_000_000_500;
        let second = f.build();

        let mut txn = RocksDbTransactionBatch::new();
        let event = merge_key_add(&world.db, &world.store, &second, &mut txn, false).unwrap();

        let body = match event.body.as_ref().unwrap() {
            proto::hub_event::Body::MergeMessageBody(b) => b,
            _ => panic!("expected MergeMessageBody"),
        };
        assert_eq!(
            body.deleted_messages.len(),
            1,
            "upsert should surface prior KEY_ADD in deleted_messages",
        );
        // Cheap identity check: the prior embedded message carries the first nonce; the new
        // top-level `message` carries the second.
        let prior_body = match body.deleted_messages[0]
            .data
            .as_ref()
            .unwrap()
            .body
            .as_ref()
            .unwrap()
        {
            Body::KeyAddBody(b) => b,
            _ => panic!(),
        };
        assert_eq!(prior_body.nonce, 1);
        let new_body = match body
            .message
            .as_ref()
            .unwrap()
            .data
            .as_ref()
            .unwrap()
            .body
            .as_ref()
            .unwrap()
        {
            Body::KeyAddBody(b) => b,
            _ => panic!(),
        };
        assert_eq!(new_body.nonce, 2);
    }

    #[test]
    fn resubmit_with_stale_nonce_rejected() {
        let world = new_world();
        let mut f = fresh_fixture(7, 99);
        register_custody(&world, f.fid, &f.fid_custody);
        register_custody(&world, f.request_fid, &f.app_custody);

        f.nonce = 5;
        commit_merge(&world, &f.build());

        // Same nonce again: `check_and_set_user_nonce` rejects `new_nonce <= stored`.
        f.scopes = vec![MessageType::LinkAdd as i32];
        f.timestamp = 1_000_000_500;
        let mut txn = RocksDbTransactionBatch::new();
        let err = merge_key_add(&world.db, &world.store, &f.build(), &mut txn, false).unwrap_err();
        assert!(
            matches!(err, MessageValidationError::StoreError(_)),
            "expected StoreError from nonce CAS, got {err:?}",
        );
    }

    #[test]
    fn resubmit_by_different_app_rejected() {
        let world = new_world();
        let mut f = fresh_fixture(7, 99);
        register_custody(&world, f.fid, &f.fid_custody);
        register_custody(&world, f.request_fid, &f.app_custody);

        commit_merge(&world, &f.build());

        // Same fid + envelope key, but a different app custody signs a metadata for a
        // different request_fid. Register that app so the metadata verification passes, then
        // expect `merge_key_add` to reject because the stored record's `request_fid` differs.
        let different_app = PrivateKeySigner::random();
        let different_request_fid = 123;
        register_custody(&world, different_request_fid, &different_app);

        f.request_fid = different_request_fid;
        f.app_custody = different_app;
        f.nonce = 2;
        f.timestamp = 1_000_000_500;
        let mut txn = RocksDbTransactionBatch::new();
        let err = merge_key_add(&world.db, &world.store, &f.build(), &mut txn, false).unwrap_err();
        match err {
            MessageValidationError::MessageValidationError(
                ValidationError::KeyRegisteredByDifferentRequestingFid,
            ) => {}
            other => panic!("expected KeyRegisteredByDifferentRequestingFid, got {other:?}"),
        }
    }

    #[test]
    fn cross_user_collision_rejected() {
        // Two different FIDs can't both hold a gasless registration for the same Ed25519 key.
        // The first KEY_ADD establishes the owner; the second (different fid, otherwise valid)
        // hits the owner-mismatch branch and is rejected.
        let world = new_world();
        let f = fresh_fixture(7, 99);
        register_custody(&world, f.fid, &f.fid_custody);
        register_custody(&world, f.request_fid, &f.app_custody);
        commit_merge(&world, &f.build());

        // Second FID, different custody, same app, same envelope key.
        let fid_b = 8;
        let fid_b_custody = PrivateKeySigner::random();
        register_custody(&world, fid_b, &fid_b_custody);

        // Reuse the same envelope key + app from `f` so this attempt targets the existing
        // owner's key and passes metadata verification.
        let mut f_b = fresh_fixture(fid_b, f.request_fid);
        f_b.fid = fid_b;
        f_b.fid_custody = fid_b_custody;
        f_b.nonce = 1;
        f_b.timestamp = 1_000_000_500;
        f_b.envelope_signer = f.envelope_signer.clone();
        f_b.envelope_signer_pubkey = f.envelope_signer_pubkey;
        f_b.app_custody = f.app_custody.clone();

        let mut txn = RocksDbTransactionBatch::new();
        let err = merge_key_add(&world.db, &world.store, &f_b.build(), &mut txn, false).unwrap_err();
        match err {
            MessageValidationError::MessageValidationError(
                ValidationError::KeyClaimedByDifferentFid,
            ) => {}
            other => panic!("expected KeyClaimedByDifferentFid, got {other:?}"),
        }
    }
}
