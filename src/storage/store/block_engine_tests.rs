#[cfg(test)]
mod tests {
    use crate::core::util::FarcasterTime;
    use crate::core::validations::error::ValidationError;
    use crate::proto::{
        block_event_data, Block, BlockEvent, BlockEventType, ChannelMemberAction,
        ChannelRegisterEventType, FarcasterNetwork, HubEvent, MembershipMode, Message, MessageType,
        OnChainEvent, StorageUnitType, StoreType,
    };
    use crate::storage::db::RocksDbTransactionBatch;
    use crate::storage::store::account::{
        make_ts_hash, ChannelMemberState, ChannelMemberStore, ChannelUpdateStore,
        HubEventStorageExt, IntoU8, MergeContext, StorageSlot, VerificationStore,
    };
    use crate::storage::store::block_engine::{
        channel_member_authority, BlockStateChange, ChannelAuthorRole, ChannelAuthorityDecision,
        IsSelf, MessageValidationError, TargetIsOwner,
    };
    use crate::storage::store::block_engine_test_helpers::*;
    use crate::storage::store::mempool_poller::MempoolMessage;
    use crate::storage::store::stores::Limits;
    use crate::storage::store::test_helper::{trie_ctx, FID_FOR_TEST};
    use crate::storage::trie::merkle_trie::TrieKey;
    use crate::utils::factory::{events_factory, messages_factory, signers};
    use crate::version::version::EngineVersion;
    use alloy_primitives::keccak256;

    fn channel_label(channel_key: &str) -> Vec<u8> {
        keccak256(channel_key.as_bytes()).to_vec()
    }

    fn channel_owner(byte: u8) -> Vec<u8> {
        vec![byte; 20]
    }

    fn inverted_same_block_channel_events(channel_key: &str) -> (OnChainEvent, OnChainEvent) {
        let label = channel_label(channel_key);
        let block_number = 42;
        let mut register = events_factory::create_channel_register_event(
            channel_key,
            label.clone(),
            channel_owner(0xAA),
            1_000,
            ChannelRegisterEventType::Register,
            block_number,
            7,
        );
        register.transaction_hash = vec![0x11; 32];

        let mut transfer = events_factory::create_channel_register_event(
            "",
            label,
            channel_owner(0xBB),
            0,
            ChannelRegisterEventType::Transfer,
            block_number,
            9,
        );
        transfer.transaction_hash = vec![0x22; 32];

        (transfer, register)
    }

    // The claim signature below is bound to a (fid, address, network, block hash, protocol)
    // tuple, so the fid is load-bearing: changing it fails with `InvalidClaimSignature` rather
    // than anything that names a fid. Same fixture the data-shard verification tests use.
    //
    // The `network` in that tuple is the *message's* own field, which `messages_factory`
    // hardcodes to Mainnet -- not the engine's. `validate_message` reads the claim's network from
    // `message_data.network` and only requires the two to agree when the engine is Mainnet, which
    // is why this one fixture validates against both the devnet `setup()` engine and the mainnet
    // engine used by the pre-activation test.
    const VERIFICATION_FID: u64 = 2;
    const VERIFICATION_ADDRESS_HEX: &str = "91031dcfdea024b4d51e775486111d2b2a715871";
    const VERIFICATION_CLAIM_SIGNATURE_HEX: &str = "b72c63d61f075b36fb66a9a867b50836cef19d653a3c09005628738677bcb25f25b6b6e6d2e1d69cd725327b3c020deef9e2575a22dc8ed08f88bc75718ce1cb1c";
    const VERIFICATION_BLOCK_HASH_HEX: &str =
        "d74860c4bbf574d5ad60f03a478a30f990e05ac723e138a5c860cdb3095f4296";

    fn verification_address() -> Vec<u8> {
        hex::decode(VERIFICATION_ADDRESS_HEX).unwrap()
    }

    fn verification_add(
        timestamp: u32,
        private_key: Option<&ed25519_dalek::SigningKey>,
    ) -> Message {
        messages_factory::verifications::create_verification_add(
            VERIFICATION_FID,
            0,
            verification_address(),
            hex::decode(VERIFICATION_CLAIM_SIGNATURE_HEX).unwrap(),
            hex::decode(VERIFICATION_BLOCK_HASH_HEX).unwrap(),
            Some(timestamp),
            private_key,
        )
    }

    fn verification_remove(address: Vec<u8>, timestamp: u32) -> Message {
        messages_factory::verifications::create_verification_remove(
            VERIFICATION_FID,
            address,
            Some(timestamp),
            None,
        )
    }

    fn quota_test_address(index: u16) -> Vec<u8> {
        let mut address = vec![0xA5; 20];
        address[..2].copy_from_slice(&index.to_be_bytes());
        address
    }

    fn verification_contract_add(address: Vec<u8>, timestamp: u32) -> Message {
        verification_contract_add_for_fid(VERIFICATION_FID, address, timestamp)
    }

    fn verification_contract_add_for_fid(fid: u64, address: Vec<u8>, timestamp: u32) -> Message {
        messages_factory::verifications::create_verification_add(
            fid,
            1,
            address,
            vec![],
            vec![0xB6; 32],
            Some(timestamp),
            None,
        )
    }

    fn channel_update_message(
        fid: u64,
        channel_id: Vec<u8>,
        name: &str,
        membership_mode: Option<MembershipMode>,
        timestamp: u32,
    ) -> Message {
        messages_factory::create_message_with_data(
            fid,
            MessageType::ChannelUpdate,
            crate::proto::message_data::Body::ChannelUpdateBody(crate::proto::ChannelUpdateBody {
                channel_id,
                name: Some(name.to_string()),
                membership_mode: membership_mode.map(|mode| mode as i32),
                ..Default::default()
            }),
            Some(timestamp),
            None,
        )
    }

    fn channel_member_message(
        author_fid: u64,
        channel_id: Vec<u8>,
        target_fid: u64,
        action: ChannelMemberAction,
        timestamp: u32,
    ) -> Message {
        messages_factory::create_message_with_data(
            author_fid,
            MessageType::ChannelMember,
            crate::proto::message_data::Body::ChannelMemberBody(crate::proto::ChannelMemberBody {
                channel_id,
                fid: target_fid,
                action: action as i32,
            }),
            Some(timestamp),
            None,
        )
    }

    fn validate_channel_for_test(
        engine: &crate::storage::store::block_engine::BlockEngine,
        message: &Message,
        txn: &mut RocksDbTransactionBatch,
    ) -> Result<(), MessageValidationError> {
        let timestamp = FarcasterTime::new(message.data.as_ref().unwrap().timestamp as u64);
        engine.validate_user_message(
            message,
            &StorageSlot::new(0, 0, 1, u32::MAX),
            &timestamp,
            EngineVersion::V20,
            txn,
        )
    }

    fn verification_replica_counts(
        engine: &crate::storage::store::block_engine::BlockEngine,
    ) -> (u64, u64) {
        let stores = engine.stores();
        let txn_batch = RocksDbTransactionBatch::new();
        let mut live_adds = 0;
        let mut tombstones = 0;
        for msg_type in Limits::store_type_to_message_types(StoreType::Verifications) {
            let count = stores
                .trie
                .get_count(
                    &stores.db,
                    &txn_batch,
                    &TrieKey::for_message_type(VERIFICATION_FID, msg_type.into_u8()),
                )
                .unwrap();
            match msg_type {
                MessageType::VerificationAddEthAddress => live_adds += count,
                MessageType::VerificationRemove => tombstones += count,
                other => panic!("unexpected verification store message type: {other:?}"),
            }
        }
        (live_adds, tombstones)
    }

    fn assert_verification_index(
        engine: &crate::storage::store::block_engine::BlockEngine,
        expected: Option<&Message>,
    ) {
        let stores = engine.stores();
        let entries = VerificationStore::get_verifications_by_address(
            &stores.verification_store,
            &verification_address(),
            None,
        )
        .unwrap();
        match expected {
            Some(message) => {
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].0, VERIFICATION_FID);
                assert_eq!(
                    entries[0].1,
                    make_ts_hash(message.data.as_ref().unwrap().timestamp, &message.hash).unwrap()
                );
            }
            None => assert!(entries.is_empty()),
        }
    }

    #[test]
    fn channel_member_authority_exhaustively_matches_t1() {
        use crate::storage::store::account::ChannelMemberState;

        let actions = [
            ChannelMemberAction::AddMember,
            ChannelMemberAction::RemoveMember,
            ChannelMemberAction::AddModerator,
            ChannelMemberAction::RemoveModerator,
            ChannelMemberAction::Ban,
            ChannelMemberAction::Unban,
        ];
        let roles = [
            ChannelAuthorRole::Owner,
            ChannelAuthorRole::Moderator,
            ChannelAuthorRole::Member,
            ChannelAuthorRole::Other,
        ];
        let states = [
            None,
            Some(ChannelMemberState::Removed),
            Some(ChannelMemberState::Member),
            Some(ChannelMemberState::Moderator),
            Some(ChannelMemberState::Banned),
        ];

        let mut cells = 0;
        for action in actions {
            for role in roles {
                for state in states {
                    for is_self in [false, true] {
                        cells += 1;
                        // This matcher is a direct transcription of T1's allowed cells. The
                        // production helper is total and returns a reason for every rejected
                        // cell; this matrix deliberately compares only admission so changing
                        // a rejection category cannot hide an authority-table drift.
                        let expected = match action {
                            ChannelMemberAction::AddMember => {
                                matches!(state, None | Some(ChannelMemberState::Removed))
                                    && (matches!(
                                        role,
                                        ChannelAuthorRole::Owner | ChannelAuthorRole::Moderator
                                    ) || is_self)
                            }
                            ChannelMemberAction::RemoveMember => match state {
                                Some(ChannelMemberState::Member) => {
                                    matches!(
                                        role,
                                        ChannelAuthorRole::Owner | ChannelAuthorRole::Moderator
                                    ) || is_self
                                }
                                Some(ChannelMemberState::Moderator) => is_self,
                                _ => false,
                            },
                            ChannelMemberAction::AddModerator => {
                                role == ChannelAuthorRole::Owner
                                    && matches!(
                                        state,
                                        None | Some(ChannelMemberState::Removed)
                                            | Some(ChannelMemberState::Member)
                                    )
                            }
                            ChannelMemberAction::RemoveModerator => {
                                role == ChannelAuthorRole::Owner
                                    && state == Some(ChannelMemberState::Moderator)
                            }
                            ChannelMemberAction::Ban => match state {
                                None
                                | Some(ChannelMemberState::Removed)
                                | Some(ChannelMemberState::Member) => matches!(
                                    role,
                                    ChannelAuthorRole::Owner | ChannelAuthorRole::Moderator
                                ),
                                Some(ChannelMemberState::Moderator) => {
                                    role == ChannelAuthorRole::Owner
                                }
                                Some(ChannelMemberState::Banned) => false,
                            },
                            ChannelMemberAction::Unban => {
                                state == Some(ChannelMemberState::Banned)
                                    && matches!(
                                        role,
                                        ChannelAuthorRole::Owner | ChannelAuthorRole::Moderator
                                    )
                            }
                            ChannelMemberAction::None => unreachable!(),
                        };
                        let actual = channel_member_authority(
                            action,
                            role,
                            state,
                            IsSelf(is_self),
                            MembershipMode::Open,
                            TargetIsOwner(false),
                        ) == ChannelAuthorityDecision::Allowed;
                        assert_eq!(
                            actual, expected,
                            "T1 drift at action={action:?}, role={role:?}, state={state:?}, self={is_self}"
                        );
                    }
                }
            }
        }
        assert_eq!(cells, 6 * 4 * 5 * 2);

        // APPROVAL and NONE both close the self-add exception. Owner/moderator authority remains
        // intact; an otherwise unauthorized joining fid is rejected in both restrictive modes.
        for mode in [MembershipMode::Approval, MembershipMode::None] {
            for state in [None, Some(ChannelMemberState::Removed)] {
                assert_eq!(
                    channel_member_authority(
                        ChannelMemberAction::AddMember,
                        ChannelAuthorRole::Other,
                        state,
                        IsSelf(true),
                        mode,
                        TargetIsOwner(false),
                    ),
                    ChannelAuthorityDecision::Unauthorized
                );
            }
        }

        // The owner is unbannable by EVERY author role, from every target state, whether or not
        // the ban is a self-ban. The non-self rows are the ones that matter: they are the only
        // place the `target_is_owner` input is observed independently of `is_self`.
        for state in states {
            for role in roles {
                for is_self in [false, true] {
                    assert_eq!(
                        channel_member_authority(
                            ChannelMemberAction::Ban,
                            role,
                            state,
                            IsSelf(is_self),
                            MembershipMode::Open,
                            TargetIsOwner(true),
                        ),
                        ChannelAuthorityDecision::OwnerUnbannable,
                        "owner must be unbannable by role={role:?}, state={state:?}, self={is_self}"
                    );
                }
            }
        }
    }

    /// T1's three non-member rows (ChannelUpdate = owner only; ChannelPin / ChannelModerate =
    /// owner or moderator) are enforced inline in `validate_channel_message`, NOT by
    /// `channel_member_authority`, so the exhaustive matrix above does not reach them. This
    /// drives every (row x author role) cell through the real admission path, which also makes
    /// it the only coverage of `channel_author_role`'s store-lookup branch: the pipeline tests
    /// all author as the owner and short-circuit on the registry check before reading a slot.
    #[test]
    fn channel_pin_update_and_moderate_rows_enforce_t1_author_roles() {
        use crate::proto::message_data::Body;

        let (mut engine, _tmpdir) = setup();
        let owner_fid = 61;
        let moderator_fid = 62;
        let member_fid = 63;
        let stranger_fid = 64;
        let owner_address = vec![0x61; 20];
        let channel_key = "role-channel";
        let channel_id = channel_label(channel_key);

        for fid in [owner_fid, moderator_fid, member_fid, stranger_fid] {
            register_user(
                fid,
                default_signer(),
                default_custody_address(),
                1,
                &mut engine,
            );
        }
        commit_event(
            &mut engine,
            &events_factory::create_channel_register_event(
                channel_key,
                channel_id.clone(),
                owner_address.clone(),
                1_000,
                ChannelRegisterEventType::Register,
                61,
                1,
            ),
        );
        let timestamp = messages_factory::farcaster_time();
        commit_message(
            &mut engine,
            &verification_contract_add_for_fid(owner_fid, owner_address, timestamp),
            Validity::Valid,
        );

        // Seed a real moderator row and a real member row so author roles resolve from the store
        // rather than from the owner short-circuit.
        let stores = engine.stores();
        let mut txn = RocksDbTransactionBatch::new();
        for (target, action) in [
            (moderator_fid, ChannelMemberAction::AddModerator),
            (member_fid, ChannelMemberAction::AddMember),
        ] {
            let grant = channel_member_message(
                owner_fid,
                channel_id.clone(),
                target,
                action,
                timestamp + 1,
            );
            assert!(
                validate_channel_for_test(&engine, &grant, &mut txn).is_ok(),
                "owner must be able to seed {action:?}"
            );
            ChannelMemberStore::merge(&stores.channel_member_store, &grant, &mut txn).unwrap();
        }

        let pin_body = |channel_id: Vec<u8>| {
            Body::ChannelPinBody(crate::proto::ChannelPinBody {
                channel_id,
                cast_hash: vec![0x77; 20],
            })
        };
        let moderate_body = |channel_id: Vec<u8>| {
            Body::ChannelModerateBody(crate::proto::ChannelModerateBody {
                channel_id,
                cast_hash: vec![0x88; 20],
                action: crate::proto::ChannelModerateAction::Hide as i32,
            })
        };

        // (message type, body builder, whether a moderator is authorized for this row)
        let rows: Vec<(MessageType, Box<dyn Fn(Vec<u8>) -> Body>, bool)> = vec![
            (
                MessageType::ChannelUpdate,
                Box::new(|channel_id| {
                    Body::ChannelUpdateBody(crate::proto::ChannelUpdateBody {
                        channel_id,
                        name: Some("role".to_string()),
                        ..Default::default()
                    })
                }),
                false,
            ),
            (MessageType::ChannelPin, Box::new(pin_body), true),
            (MessageType::ChannelModerate, Box::new(moderate_body), true),
        ];

        for (index, (message_type, body, moderator_allowed)) in rows.iter().enumerate() {
            for (author, is_owner_or_allowed_mod) in [
                (owner_fid, true),
                (moderator_fid, *moderator_allowed),
                (member_fid, false),
                (stranger_fid, false),
            ] {
                let message = messages_factory::create_message_with_data(
                    author,
                    *message_type,
                    body(channel_id.clone()),
                    Some(timestamp + 10 + index as u32),
                    None,
                );
                let result = validate_channel_for_test(&engine, &message, &mut txn);
                if is_owner_or_allowed_mod {
                    assert!(
                        result.is_ok(),
                        "{message_type:?} must admit author {author}: {result:?}"
                    );
                } else {
                    let error =
                        result.expect_err(&format!("{message_type:?} must reject author {author}"));
                    assert!(
                        matches!(
                            error,
                            MessageValidationError::HubError(ref hub_error)
                                if hub_error.message == "unauthorized channel action"
                        ),
                        "{message_type:?} author {author} rejected for the wrong reason: {error:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn channel_validation_checks_type_and_widths_before_registry_state() {
        use crate::proto::message_data::Body;

        let (mut engine, _tmpdir) = setup();
        let fid = 1234;
        register_user(
            fid,
            default_signer(),
            default_custody_address(),
            1,
            &mut engine,
        );
        let timestamp = messages_factory::farcaster_time();
        let block_timestamp = FarcasterTime::new(timestamp as u64);
        let storage_slot = StorageSlot::new(0, 0, 1, u32::MAX);

        let (_, update_body) = messages_factory::channels::all_message_bodies()
            .into_iter()
            .next()
            .unwrap();
        let mismatch = messages_factory::create_message_with_data(
            fid,
            MessageType::KeyAdd,
            update_body,
            Some(timestamp),
            None,
        );
        assert!(matches!(
            engine.validate_user_message(
                &mismatch,
                &storage_slot,
                &block_timestamp,
                EngineVersion::V20,
                &mut RocksDbTransactionBatch::new(),
            ),
            Err(MessageValidationError::InvalidMessageType)
        ));

        let wrong_channel_width = messages_factory::create_message_with_data(
            fid,
            MessageType::ChannelUpdate,
            Body::ChannelUpdateBody(crate::proto::ChannelUpdateBody {
                channel_id: vec![0x11; 31],
                ..Default::default()
            }),
            Some(timestamp),
            None,
        );
        let error = engine
            .validate_user_message(
                &wrong_channel_width,
                &storage_slot,
                &block_timestamp,
                EngineVersion::V20,
                &mut RocksDbTransactionBatch::new(),
            )
            .unwrap_err();
        assert!(matches!(
            error,
            MessageValidationError::HubError(ref hub_error)
                if hub_error.message == "channel id must be 32 bytes"
        ));

        let wrong_cast_width = messages_factory::create_message_with_data(
            fid,
            MessageType::ChannelModerate,
            Body::ChannelModerateBody(crate::proto::ChannelModerateBody {
                channel_id: vec![0x11; 32],
                cast_hash: vec![0x22; 19],
                action: crate::proto::ChannelModerateAction::Hide as i32,
            }),
            Some(timestamp),
            None,
        );
        let error = engine
            .validate_user_message(
                &wrong_cast_width,
                &storage_slot,
                &block_timestamp,
                EngineVersion::V20,
                &mut RocksDbTransactionBatch::new(),
            )
            .unwrap_err();
        assert!(matches!(
            error,
            MessageValidationError::HubError(ref hub_error)
                if hub_error.message == "channel moderate cast hash must be 20 bytes"
        ));

        let unknown_channel = messages_factory::create_message_with_data(
            fid,
            MessageType::ChannelUpdate,
            Body::ChannelUpdateBody(crate::proto::ChannelUpdateBody {
                channel_id: vec![0x11; 32],
                ..Default::default()
            }),
            Some(timestamp),
            None,
        );
        let error = engine
            .validate_user_message(
                &unknown_channel,
                &storage_slot,
                &block_timestamp,
                EngineVersion::V20,
                &mut RocksDbTransactionBatch::new(),
            )
            .unwrap_err();
        assert!(matches!(
            error,
            MessageValidationError::HubError(ref hub_error)
                if hub_error.message == "unknown channel"
        ));
    }

    #[test]
    fn same_block_verification_then_channel_action_commits_and_fans_out_on_devnet() {
        let (mut engine, _tmpdir) = setup();
        let fid = 44;
        let owner_address = vec![0x44; 20];
        let channel_key = "same-block-channel";
        let channel_id = channel_label(channel_key);
        register_user(
            fid,
            default_signer(),
            default_custody_address(),
            1,
            &mut engine,
        );
        commit_event(
            &mut engine,
            &events_factory::create_channel_register_event(
                channel_key,
                channel_id.clone(),
                owner_address.clone(),
                1_000,
                ChannelRegisterEventType::Register,
                50,
                1,
            ),
        );

        let timestamp = messages_factory::farcaster_time();
        let verification = verification_contract_add_for_fid(fid, owner_address, timestamp);
        let update = channel_update_message(
            fid,
            channel_id.clone(),
            "same block",
            Some(MembershipMode::Open),
            timestamp + 1,
        );
        let height = engine.get_confirmed_height().increment();
        let state_change = engine.propose_state_change(
            vec![
                MempoolMessage::UserMessage(verification),
                MempoolMessage::UserMessage(update.clone()),
            ],
            height,
            Some(FarcasterTime::new((timestamp + 1) as u64)),
        );
        assert_eq!(state_change.transactions.len(), 1);
        assert_eq!(state_change.transactions[0].user_messages.len(), 2);
        validate_and_commit_state_change(&mut engine, &state_change);

        let state = ChannelUpdateStore::get_channel_update(
            &engine.stores().channel_update_store,
            &channel_id,
            None,
        )
        .unwrap()
        .expect("channel update must merge in the full propose/validate/commit pipeline");
        assert_eq!(state.body.name.as_deref(), Some("same block"));
        assert_eq!(state.membership_mode, MembershipMode::Open);
        let channel_events = state_change
            .events
            .iter()
            .filter_map(|event| {
                let block_event_data::Body::MergeMessageEventBody(body) =
                    event.data.as_ref()?.body.as_ref()?
                else {
                    return None;
                };
                let message = body.message.as_ref()?;
                (message.msg_type() == MessageType::ChannelUpdate).then_some(message)
            })
            .collect::<Vec<_>>();
        assert_eq!(channel_events, vec![&update]);
    }

    #[test]
    fn channel_owner_transfer_moves_authority_after_verification() {
        let (mut engine, _tmpdir) = setup();
        let old_fid = 51;
        let new_fid = 52;
        let old_address = vec![0x51; 20];
        let new_address = vec![0x52; 20];
        let channel_key = "transferred-channel";
        let channel_id = channel_label(channel_key);
        for fid in [old_fid, new_fid] {
            register_user(
                fid,
                default_signer(),
                default_custody_address(),
                1,
                &mut engine,
            );
        }
        commit_event(
            &mut engine,
            &events_factory::create_channel_register_event(
                channel_key,
                channel_id.clone(),
                old_address.clone(),
                1_000,
                ChannelRegisterEventType::Register,
                60,
                1,
            ),
        );
        let timestamp = messages_factory::farcaster_time();
        commit_message(
            &mut engine,
            &verification_contract_add_for_fid(old_fid, old_address, timestamp),
            Validity::Valid,
        );

        let old_update = channel_update_message(
            old_fid,
            channel_id.clone(),
            "old owner",
            Some(MembershipMode::Approval),
            timestamp + 1,
        );
        assert!(engine.simulate_message(&old_update).is_ok());
        commit_message(&mut engine, &old_update, Validity::Valid);

        let owner_ban = channel_member_message(
            old_fid,
            channel_id.clone(),
            old_fid,
            ChannelMemberAction::Ban,
            timestamp + 2,
        );
        let error = engine.simulate_message(&owner_ban).unwrap_err();
        assert!(matches!(
            error,
            MessageValidationError::HubError(ref hub_error)
                if hub_error.message == "channel owner cannot be banned"
        ));

        commit_event(
            &mut engine,
            &events_factory::create_channel_register_event(
                "",
                channel_id.clone(),
                new_address.clone(),
                0,
                ChannelRegisterEventType::Transfer,
                61,
                1,
            ),
        );
        let stale_owner_update = channel_update_message(
            old_fid,
            channel_id.clone(),
            "stale owner",
            Some(MembershipMode::Approval),
            timestamp + 3,
        );
        // Pin the reason, not just the failure: a bare `is_err()` here would also pass if the
        // transfer had broken owner resolution outright (unknown channel, duplicate hash), which
        // is exactly the bug this test is named for. The transfer moved ownership to an address
        // with no shard-0 verification, so the channel is parked and the freeze rejects everyone
        // — including the stale owner — until the new address verifies.
        let stale_error = engine.simulate_message(&stale_owner_update).unwrap_err();
        assert!(
            matches!(
                stale_error,
                MessageValidationError::HubError(ref hub_error)
                    if hub_error.message == "channel is parked"
            ),
            "old owner must lose authority via the parked freeze, got {stale_error:?}"
        );

        let new_owner_update = channel_update_message(
            new_fid,
            channel_id.clone(),
            "new owner",
            Some(MembershipMode::Open),
            timestamp + 4,
        );
        let unverified_error = engine.simulate_message(&new_owner_update).unwrap_err();
        assert!(
            matches!(
                unverified_error,
                MessageValidationError::HubError(ref hub_error)
                    if hub_error.message == "channel is parked"
            ),
            "new owner must stay frozen until the new address is verified on shard 0, got {unverified_error:?}"
        );
        commit_message(
            &mut engine,
            &verification_contract_add_for_fid(new_fid, new_address, timestamp + 3),
            Validity::Valid,
        );
        assert!(engine.simulate_message(&new_owner_update).is_ok());
        commit_message(&mut engine, &new_owner_update, Validity::Valid);
    }

    #[test]
    fn parked_channel_freezes_all_actions_except_self_leave() {
        let (mut engine, _tmpdir) = setup();
        let owner_fid = 71;
        let mod_fid = 72;
        let member_fid = 73;
        let outsider_fid = 74;
        let owner_address = vec![0x71; 20];
        let channel_key = "parked-channel";
        let channel_id = channel_label(channel_key);
        for fid in [owner_fid, mod_fid, member_fid, outsider_fid] {
            register_user(
                fid,
                default_signer(),
                default_custody_address(),
                1,
                &mut engine,
            );
        }
        commit_event(
            &mut engine,
            &events_factory::create_channel_register_event(
                channel_key,
                channel_id.clone(),
                owner_address.clone(),
                1_000,
                ChannelRegisterEventType::Register,
                70,
                1,
            ),
        );
        let timestamp = messages_factory::farcaster_time();
        let assert_parked = |engine: &mut crate::storage::store::block_engine::BlockEngine,
                             message: &Message,
                             what: &str| {
            let error = engine.simulate_message(message).unwrap_err();
            assert!(
                matches!(
                    error,
                    MessageValidationError::HubError(ref hub_error)
                        if hub_error.message == "channel is parked"
                ),
                "{what} must be rejected as parked, got {error:?}"
            );
        };

        // Day-one shape: registered channel, owner address never verified on shard 0.
        assert_parked(
            &mut engine,
            &channel_update_message(
                owner_fid,
                channel_id.clone(),
                "day one",
                Some(MembershipMode::Open),
                timestamp,
            ),
            "the owner's first action before any verification",
        );

        // Verify the owner, open the channel, and seed a moderator and a member.
        commit_message(
            &mut engine,
            &verification_contract_add_for_fid(owner_fid, owner_address.clone(), timestamp),
            Validity::Valid,
        );
        commit_message(
            &mut engine,
            &channel_update_message(
                owner_fid,
                channel_id.clone(),
                "open",
                Some(MembershipMode::Open),
                timestamp + 1,
            ),
            Validity::Valid,
        );
        commit_message(
            &mut engine,
            &channel_member_message(
                owner_fid,
                channel_id.clone(),
                mod_fid,
                ChannelMemberAction::AddModerator,
                timestamp + 2,
            ),
            Validity::Valid,
        );
        commit_message(
            &mut engine,
            &channel_member_message(
                owner_fid,
                channel_id.clone(),
                member_fid,
                ChannelMemberAction::AddMember,
                timestamp + 3,
            ),
            Validity::Valid,
        );

        // Park the channel: the owner's verification is removed, so the owner address no longer
        // resolves to a fid.
        commit_message(
            &mut engine,
            &messages_factory::verifications::create_verification_remove(
                owner_fid,
                owner_address.clone(),
                Some(timestamp + 4),
                None,
            ),
            Validity::Valid,
        );

        // Every authority-bearing action is frozen — including the true owner's own, the
        // moderator's still-seeded powers, and the previously-admitted moderator ban of the
        // unresolvable owner (the fail-open this freeze closes).
        assert_parked(
            &mut engine,
            &channel_update_message(
                owner_fid,
                channel_id.clone(),
                "parked owner",
                None,
                timestamp + 5,
            ),
            "the unresolvable owner's update",
        );
        assert_parked(
            &mut engine,
            &messages_factory::create_message_with_data(
                mod_fid,
                MessageType::ChannelPin,
                crate::proto::message_data::Body::ChannelPinBody(crate::proto::ChannelPinBody {
                    channel_id: channel_id.clone(),
                    cast_hash: vec![0x77; 20],
                }),
                Some(timestamp + 6),
                None,
            ),
            "a moderator pin while parked (the availability cost, pinned deliberately)",
        );
        assert_parked(
            &mut engine,
            &channel_member_message(
                mod_fid,
                channel_id.clone(),
                member_fid,
                ChannelMemberAction::Ban,
                timestamp + 7,
            ),
            "a moderator ban of a member while parked",
        );
        assert_parked(
            &mut engine,
            &channel_member_message(
                mod_fid,
                channel_id.clone(),
                owner_fid,
                ChannelMemberAction::Ban,
                timestamp + 8,
            ),
            "a moderator ban of the unresolvable owner (fail-open closure)",
        );
        assert_parked(
            &mut engine,
            &channel_member_message(
                outsider_fid,
                channel_id.clone(),
                outsider_fid,
                ChannelMemberAction::AddMember,
                timestamp + 9,
            ),
            "an OPEN self-add while parked (joins mint slots nobody can police)",
        );

        // Self-leave is the sole exception: its authority is self-contained, so a member and a
        // moderator can still remove themselves. A never-seen fid's self-leave falls through to
        // the authority table and is rejected on target state — the carve-out mints nothing.
        let outsider_leave = channel_member_message(
            outsider_fid,
            channel_id.clone(),
            outsider_fid,
            ChannelMemberAction::RemoveMember,
            timestamp + 10,
        );
        let outsider_error = engine.simulate_message(&outsider_leave).unwrap_err();
        assert!(
            matches!(
                outsider_error,
                MessageValidationError::HubError(ref hub_error)
                    if hub_error.message == "invalid channel target state"
            ),
            "a never-seen fid's self-leave must fail on target state, not mint a row, got {outsider_error:?}"
        );
        assert!(engine
            .simulate_message(&channel_member_message(
                mod_fid,
                channel_id.clone(),
                mod_fid,
                ChannelMemberAction::RemoveMember,
                timestamp + 11,
            ))
            .is_ok());
        commit_message(
            &mut engine,
            &channel_member_message(
                member_fid,
                channel_id.clone(),
                member_fid,
                ChannelMemberAction::RemoveMember,
                timestamp + 12,
            ),
            Validity::Valid,
        );

        // Unparking is the owner re-verifying; full authority returns.
        commit_message(
            &mut engine,
            &verification_contract_add_for_fid(owner_fid, owner_address, timestamp + 13),
            Validity::Valid,
        );
        commit_message(
            &mut engine,
            &channel_update_message(
                owner_fid,
                channel_id.clone(),
                "unparked",
                Some(MembershipMode::Approval),
                timestamp + 14,
            ),
            Validity::Valid,
        );
    }

    #[test]
    fn parked_freeze_transitions_within_a_single_block_are_deterministic() {
        // The freeze resolves the owner THROUGH the transaction batch, so a verification remove or
        // add earlier in a block parks or unparks the channel for a later message in the SAME
        // block. Both interleavings are now reachable states. Keep the verification and the channel
        // action under one fid so they share a per-fid transaction whose `user_messages` replay in
        // input order in propose AND validate; `commit_messages` asserts both produced identical
        // state roots, so a mid-block transition is deterministic, not a propose/validate split.
        let (mut engine, _tmpdir) = setup();
        let owner_fid = 81;
        let owner_address = vec![0x81; 20];
        let channel_key = "midblock-channel";
        let channel_id = channel_label(channel_key);
        register_user(
            owner_fid,
            default_signer(),
            default_custody_address(),
            1,
            &mut engine,
        );
        commit_event(
            &mut engine,
            &events_factory::create_channel_register_event(
                channel_key,
                channel_id.clone(),
                owner_address.clone(),
                1_000,
                ChannelRegisterEventType::Register,
                80,
                1,
            ),
        );
        let timestamp = messages_factory::farcaster_time();
        // Resolve the owner first: the channel starts UNparked and the owner is authorized.
        commit_message(
            &mut engine,
            &verification_contract_add_for_fid(owner_fid, owner_address.clone(), timestamp),
            Validity::Valid,
        );

        // PARK MID-BLOCK. The identical owner update is authorized against the current unparked
        // state, so the only thing that can freeze it is the remove landing ahead of it.
        let frozen_update =
            channel_update_message(owner_fid, channel_id.clone(), "frozen", None, timestamp + 2);
        assert!(
            engine.simulate_message(&frozen_update).is_ok(),
            "owner update must be valid while the channel is unparked",
        );
        let park_remove = messages_factory::verifications::create_verification_remove(
            owner_fid,
            owner_address.clone(),
            Some(timestamp + 1),
            None,
        );
        // One block: the remove parks the channel, then the owner's own update in the same block
        // resolves to `None` and is frozen — rejected, never written to the trie — even though the
        // same author committed successfully one block earlier.
        commit_messages(
            &mut engine,
            vec![
                (&park_remove, Validity::Valid),
                (&frozen_update, Validity::Invalid),
            ],
        );

        // UNPARK MID-BLOCK. One block: the owner re-verifies, then the owner's update lands behind
        // it — the add resolves the owner and the update is admitted in the same block that thawed
        // the channel.
        let unpark_add =
            verification_contract_add_for_fid(owner_fid, owner_address.clone(), timestamp + 3);
        let thawed_update = channel_update_message(
            owner_fid,
            channel_id.clone(),
            "thawed",
            Some(MembershipMode::Approval),
            timestamp + 4,
        );
        commit_messages(
            &mut engine,
            vec![
                (&unpark_add, Validity::Valid),
                (&thawed_update, Validity::Valid),
            ],
        );
    }

    #[test]
    fn channel_moderator_cap_is_enforced_at_ten_through_the_txn() {
        let (mut engine, _tmpdir) = setup();
        let owner_fid = 61;
        let owner_address = vec![0x61; 20];
        let channel_key = "moderator-cap-channel";
        let channel_id = channel_label(channel_key);
        register_user(
            owner_fid,
            default_signer(),
            default_custody_address(),
            1,
            &mut engine,
        );
        commit_event(
            &mut engine,
            &events_factory::create_channel_register_event(
                channel_key,
                channel_id.clone(),
                owner_address.clone(),
                1_000,
                ChannelRegisterEventType::Register,
                70,
                1,
            ),
        );
        let timestamp = messages_factory::farcaster_time();
        commit_message(
            &mut engine,
            &verification_contract_add_for_fid(owner_fid, owner_address, timestamp),
            Validity::Valid,
        );

        let stores = engine.stores();
        let mut txn = RocksDbTransactionBatch::new();
        for index in 0..9 {
            ChannelMemberStore::merge(
                &stores.channel_member_store,
                &channel_member_message(
                    owner_fid,
                    channel_id.clone(),
                    1_000 + index,
                    ChannelMemberAction::AddModerator,
                    timestamp + 1 + index as u32,
                ),
                &mut txn,
            )
            .unwrap();
        }
        let tenth = channel_member_message(
            owner_fid,
            channel_id.clone(),
            2_000,
            ChannelMemberAction::AddModerator,
            timestamp + 20,
        );
        assert!(validate_channel_for_test(&engine, &tenth, &mut txn).is_ok());
        ChannelMemberStore::merge(&stores.channel_member_store, &tenth, &mut txn).unwrap();
        assert_eq!(
            ChannelMemberStore::live_moderator_count(
                &stores.channel_member_store,
                &channel_id,
                Some(&txn),
            )
            .unwrap(),
            10
        );

        let eleventh = channel_member_message(
            owner_fid,
            channel_id,
            2_001,
            ChannelMemberAction::AddModerator,
            timestamp + 21,
        );
        let error = validate_channel_for_test(&engine, &eleventh, &mut txn).unwrap_err();
        assert!(matches!(
            error,
            MessageValidationError::HubError(ref hub_error)
                if hub_error.message == "channel moderator cap reached"
        ));
    }

    #[test]
    fn channel_self_add_follows_folded_modes_and_bans() {
        let (mut engine, _tmpdir) = setup();
        let joiner_fid = 71;
        let channel_key = "self-add-channel";
        let channel_id = channel_label(channel_key);
        register_user(
            joiner_fid,
            default_signer(),
            default_custody_address(),
            1,
            &mut engine,
        );
        commit_event(
            &mut engine,
            &events_factory::create_channel_register_event(
                channel_key,
                channel_id.clone(),
                vec![0x72; 20],
                1_000,
                ChannelRegisterEventType::Register,
                80,
                1,
            ),
        );
        // The owner address must resolve, or the parked freeze rejects everything before the
        // fold/ban logic under test is ever reached.
        let owner_fid = 72;
        register_user(
            owner_fid,
            default_signer(),
            default_custody_address(),
            1,
            &mut engine,
        );
        commit_message(
            &mut engine,
            &verification_contract_add_for_fid(
                owner_fid,
                vec![0x72; 20],
                messages_factory::farcaster_time(),
            ),
            Validity::Valid,
        );

        let stores = engine.stores();
        let timestamp = messages_factory::farcaster_time();
        let mut txn = RocksDbTransactionBatch::new();
        for (mode, admitted) in [
            (MembershipMode::Open, true),
            (MembershipMode::Approval, false),
            (MembershipMode::None, false),
        ] {
            ChannelUpdateStore::merge(
                &stores.channel_update_store,
                &channel_update_message(
                    joiner_fid,
                    channel_id.clone(),
                    "mode",
                    Some(mode),
                    timestamp + mode as u32 + 1,
                ),
                &mut txn,
            )
            .unwrap();
            let self_add = channel_member_message(
                joiner_fid,
                channel_id.clone(),
                joiner_fid,
                ChannelMemberAction::AddMember,
                timestamp + 10 + mode as u32,
            );
            assert_eq!(
                validate_channel_for_test(&engine, &self_add, &mut txn).is_ok(),
                admitted,
                "unexpected self-add result for folded mode {mode:?}"
            );
        }

        ChannelUpdateStore::merge(
            &stores.channel_update_store,
            &channel_update_message(
                joiner_fid,
                channel_id.clone(),
                "open again",
                Some(MembershipMode::Open),
                timestamp + 30,
            ),
            &mut txn,
        )
        .unwrap();
        ChannelMemberStore::merge(
            &stores.channel_member_store,
            &channel_member_message(
                999,
                channel_id.clone(),
                joiner_fid,
                ChannelMemberAction::Ban,
                timestamp + 31,
            ),
            &mut txn,
        )
        .unwrap();
        assert_eq!(
            ChannelMemberStore::member_state(
                &stores.channel_member_store,
                &channel_id,
                joiner_fid,
                Some(&txn),
            )
            .unwrap(),
            Some(ChannelMemberState::Banned)
        );
        let self_add = channel_member_message(
            joiner_fid,
            channel_id,
            joiner_fid,
            ChannelMemberAction::AddMember,
            timestamp + 32,
        );
        let error = validate_channel_for_test(&engine, &self_add, &mut txn).unwrap_err();
        assert!(matches!(
            error,
            MessageValidationError::HubError(ref hub_error)
                if hub_error.message == "channel member is banned"
        ));
    }

    #[test]
    fn channel_owner_fid_resolution_sees_same_transaction_verification_changes() {
        let (engine, _tmpdir) = setup();
        let stores = engine.stores();
        let owner_address = vec![0xAB; 20];
        let older = messages_factory::verifications::create_verification_add(
            10,
            1,
            owner_address.clone(),
            vec![],
            vec![0x11; 32],
            Some(100),
            None,
        );
        let newer = messages_factory::verifications::create_verification_add(
            20,
            1,
            owner_address.clone(),
            vec![],
            vec![0x22; 32],
            Some(200),
            None,
        );
        let remove_newer = messages_factory::verifications::create_verification_remove(
            20,
            owner_address.clone(),
            Some(300),
            None,
        );
        let mut txn = RocksDbTransactionBatch::new();
        let ctx = MergeContext {
            version: EngineVersion::V20,
        };
        stores
            .verification_store
            .merge(&older, &mut txn, &ctx)
            .unwrap();
        stores
            .verification_store
            .merge(&newer, &mut txn, &ctx)
            .unwrap();
        assert_eq!(
            stores
                .resolve_channel_owner_fid(&owner_address, Some(&txn))
                .unwrap(),
            Some(20)
        );
        stores
            .verification_store
            .merge(&remove_newer, &mut txn, &ctx)
            .unwrap();
        assert_eq!(
            stores
                .resolve_channel_owner_fid(&owner_address, Some(&txn))
                .unwrap(),
            Some(10)
        );
    }

    fn verification_block_event_messages(block: &Block) -> Vec<&Message> {
        block
            .events
            .iter()
            .filter_map(|event| {
                let block_event_data::Body::MergeMessageEventBody(body) =
                    event.data.as_ref()?.body.as_ref()?
                else {
                    return None;
                };
                let message = body.message.as_ref()?;
                matches!(
                    message.msg_type(),
                    crate::proto::MessageType::VerificationAddEthAddress
                        | crate::proto::MessageType::VerificationRemove
                )
                .then_some(message)
            })
            .collect()
    }

    /// Use only when no verification merged. Verification types are allowlisted for fan-out, so
    /// absence here proves the block produced no verification MergeMessage HubEvent; it is not an
    /// assertion that the message types are excluded. Heartbeats may legitimately share the block.
    fn assert_no_verification_block_events(block: &Block) {
        assert!(
            verification_block_event_messages(block).is_empty(),
            "block without a verification merge emitted a verification BlockEvent"
        );
    }

    fn assert_one_verification_block_event(block: &Block, expected: &Message) {
        assert_eq!(
            verification_block_event_messages(block),
            vec![expected],
            "a verification merge must emit exactly one BlockEvent carrying the original message"
        );
    }

    #[tokio::test]
    async fn test_trie_updated_only_on_commit() {
        let (mut block_engine, _temp_dir) = setup();
        // Rolling grant date; a fixed one ages out against wall-clock `is_active`.
        // See `test_helper::default_storage_event`.
        let onchain_event = events_factory::create_rent_event_with_timestamp(
            FID_FOR_TEST,
            1,
            crate::utils::factory::time::current_timestamp(),
        );
        let height = block_engine.get_confirmed_height().increment();
        let state_change = block_engine.propose_state_change(
            vec![MempoolMessage::OnchainEvent(onchain_event.clone())],
            height,
            None,
        );
        assert!(!state_change.new_state_root.is_empty());
        assert!(block_engine.trie_root_hash().is_empty());

        block_engine.validate_state_change(&state_change, height);
        assert!(block_engine.trie_root_hash().is_empty());

        block_engine.commit_block(&state_change_to_block(height.block_number, &state_change));
        assert_eq!(block_engine.trie_root_hash(), state_change.new_state_root);
    }

    #[tokio::test]
    async fn test_empty_block() {
        let (mut block_engine, _temp_dir) = setup();
        let height = block_engine.get_confirmed_height().increment();
        let state_change = block_engine.propose_state_change(vec![], height, None);

        assert_eq!(state_change.transactions.len(), 0);
        assert!(state_change.events.is_empty());
        assert!(state_change.new_state_root.is_empty());
        assert!(state_change.events_hash.is_empty());

        validate_and_commit_state_change(&mut block_engine, &state_change);
        assert_eq!(block_engine.get_confirmed_height(), height);
    }

    #[tokio::test]
    async fn test_mainnet_propose_validate_commit() {
        // Test that the pipeline works while new features are not active on mainnet
        let (mut block_engine, _temp_dir) = setup_with_options(BlockEngineOptions {
            network: FarcasterNetwork::Mainnet,
            ..BlockEngineOptions::default()
        });
        let height = block_engine.get_confirmed_height().increment();
        let state_change = block_engine.propose_state_change(vec![], height, None);

        assert_eq!(state_change.transactions.len(), 0);
        assert!(state_change.events.is_empty());
        assert!(state_change.new_state_root.is_empty());
        assert!(state_change.events_hash.is_empty());

        validate_and_commit_state_change(&mut block_engine, &state_change);
        assert_eq!(block_engine.get_confirmed_height(), height);
    }

    #[test]
    fn test_shard_zero_verification_validation_gate_and_signatures() {
        let (mut block_engine, _temp_dir) = setup();
        register_user(
            VERIFICATION_FID,
            default_signer(),
            default_custody_address(),
            1,
            &mut block_engine,
        );
        let timestamp = messages_factory::farcaster_time();
        let block_timestamp = FarcasterTime::new(timestamp as u64);
        let storage_slot = StorageSlot::new(0, 0, 1, u32::MAX);

        let valid = verification_add(timestamp, None);
        assert!(block_engine
            .validate_user_message(
                &valid,
                &storage_slot,
                &block_timestamp,
                EngineVersion::V20,
                &mut RocksDbTransactionBatch::new(),
            )
            .is_ok());

        assert!(matches!(
            block_engine.validate_user_message(
                &valid,
                &storage_slot,
                &block_timestamp,
                EngineVersion::V19,
                &mut RocksDbTransactionBatch::new(),
            ),
            Err(MessageValidationError::InvalidMessageType)
        ));

        // The arm admits `VerificationRemoveBody` as well as adds, so gate both -- a gate that
        // covered only adds would leave removes admissible pre-activation.
        let remove = messages_factory::verifications::create_verification_remove(
            VERIFICATION_FID,
            verification_address(),
            Some(timestamp),
            None,
        );
        assert!(block_engine
            .validate_user_message(
                &remove,
                &storage_slot,
                &block_timestamp,
                EngineVersion::V20,
                &mut RocksDbTransactionBatch::new(),
            )
            .is_ok());
        assert!(matches!(
            block_engine.validate_user_message(
                &remove,
                &storage_slot,
                &block_timestamp,
                EngineVersion::V19,
                &mut RocksDbTransactionBatch::new(),
            ),
            Err(MessageValidationError::InvalidMessageType)
        ));

        let mut bad_claim_signature = hex::decode(VERIFICATION_CLAIM_SIGNATURE_HEX).unwrap();
        bad_claim_signature[0] ^= 0xff;
        let invalid_signature = messages_factory::verifications::create_verification_add(
            VERIFICATION_FID,
            0,
            verification_address(),
            bad_claim_signature,
            hex::decode(VERIFICATION_BLOCK_HASH_HEX).unwrap(),
            Some(timestamp),
            None,
        );
        assert!(matches!(
            block_engine.validate_user_message(
                &invalid_signature,
                &storage_slot,
                &block_timestamp,
                EngineVersion::V20,
                &mut RocksDbTransactionBatch::new(),
            ),
            Err(MessageValidationError::MessageValidationError(
                ValidationError::InvalidClaimSignature
            ))
        ));

        let inactive_signer = signers::generate_signer();
        let invalid_signer = verification_add(timestamp, Some(&inactive_signer));
        assert!(matches!(
            block_engine.validate_user_message(
                &invalid_signer,
                &storage_slot,
                &block_timestamp,
                EngineVersion::V20,
                &mut RocksDbTransactionBatch::new(),
            ),
            Err(MessageValidationError::MissingSigner)
        ));
    }

    #[test]
    fn test_shard_zero_verification_quota_boundary() {
        let timestamp = messages_factory::farcaster_time();

        // No storage is an unconditional spam gate for adds, even before any replica rows exist.
        let (mut no_storage_engine, _temp_dir) = setup();
        register_user(
            VERIFICATION_FID,
            default_signer(),
            default_custody_address(),
            1,
            &mut no_storage_engine,
        );
        assert!(matches!(
            no_storage_engine.validate_user_message(
                &verification_add(timestamp, None),
                &StorageSlot::new(0, 0, 0, u32::MAX),
                &FarcasterTime::new(timestamp as u64),
                EngineVersion::V20,
                &mut RocksDbTransactionBatch::new(),
            ),
            Err(MessageValidationError::InsufficientStorage)
        ));

        // One 2025 unit permits five live verification adds. Put six in one transaction so the
        // sixth pins the in-transaction live-add count maintained between successful merges.
        let (mut engine, _temp_dir) = setup();
        register_user(
            VERIFICATION_FID,
            default_signer(),
            default_custody_address(),
            1,
            &mut engine,
        );
        let addresses = (1u16..=6).map(quota_test_address).collect::<Vec<_>>();
        let initial_adds = addresses
            .iter()
            .map(|address| verification_contract_add(address.clone(), timestamp))
            .collect::<Vec<_>>();
        let mut initial_results = initial_adds
            .iter()
            .take(5)
            .map(|message| (message, Validity::Valid))
            .collect::<Vec<_>>();
        initial_results.push((&initial_adds[5], Validity::Invalid));
        commit_messages(&mut engine, initial_results);

        // A remove for an existing address is never quota-blocked: it supersedes the old row and
        // lowers the live count, allowing an at-cap user to shed live state.
        let superseding_remove = verification_remove(addresses[0].clone(), timestamp + 1);
        commit_message(&mut engine, &superseding_remove, Validity::Valid);

        // Superseding adds are live-count-neutral too. Build a second at-cap replica, then replace
        // one address with a newer add.
        let (mut replacement_engine, _temp_dir) = setup();
        register_user(
            VERIFICATION_FID,
            default_signer(),
            default_custody_address(),
            1,
            &mut replacement_engine,
        );
        let at_cap_adds = (11u16..=15)
            .map(|index| verification_contract_add(quota_test_address(index), timestamp))
            .collect::<Vec<_>>();
        commit_messages(
            &mut replacement_engine,
            at_cap_adds
                .iter()
                .map(|message| (message, Validity::Valid))
                .collect(),
        );
        let replacement_add = verification_contract_add(quota_test_address(11), timestamp + 1);
        commit_message(&mut replacement_engine, &replacement_add, Validity::Valid);
    }

    #[test]
    fn test_shard_zero_verification_remove_then_add_cycles_cannot_launder_the_add_cap() {
        let timestamp = messages_factory::farcaster_time();
        let (mut engine, _temp_dir) = setup();
        register_user(
            VERIFICATION_FID,
            default_signer(),
            default_custody_address(),
            1,
            &mut engine,
        );

        // A type-blind supersede carve-out would let `remove addr; add addr` cycles mint unbounded
        // permanent rows on one storage unit: the add lands on a tombstone, skipping the live-add
        // cap, and refunds the tombstone slot for the next cycle. The live-add cap must bind on an
        // add over a tombstone exactly as it does on a net-new add.
        let mut live_index = 100u16;
        for _ in 0..5 {
            let address = quota_test_address(live_index);
            commit_message(
                &mut engine,
                &verification_remove(address.clone(), timestamp),
                Validity::Valid,
            );
            commit_message(
                &mut engine,
                &verification_contract_add(address, timestamp + 1),
                Validity::Valid,
            );
            live_index += 1;
        }
        assert_eq!(verification_replica_counts(&engine), (5, 0));

        // The sixth cycle mints its tombstone but cannot convert it: live adds are at cap.
        let address = quota_test_address(live_index);
        commit_message(
            &mut engine,
            &verification_remove(address.clone(), timestamp),
            Validity::Valid,
        );
        commit_message(
            &mut engine,
            &verification_contract_add(address, timestamp + 1),
            Validity::Invalid,
        );
        assert_eq!(verification_replica_counts(&engine), (5, 1));
    }

    #[test]
    fn test_shard_zero_verification_add_then_remove_cycles_cannot_mint_unbounded_rows() {
        let timestamp = messages_factory::farcaster_time();
        let (mut engine, _temp_dir) = setup();
        register_user(
            VERIFICATION_FID,
            default_signer(),
            default_custody_address(),
            1,
            &mut engine,
        );

        // Shard-0 rows are permanent, so the bound has to hold against sequences that return the
        // COUNTERS to their starting point while leaving a row behind. `add addr_i; remove addr_i`
        // is that sequence: the remove is row-neutral and ungated, and it resets `live_adds` to 0,
        // so the live-add cap alone never engages and rows would grow forever.
        //
        // Drive the replica to the row cap with live_adds well BELOW max_messages, so only the
        // total-row gate can reject the next mint.
        let removes = (2000u16..2000 + 256)
            .map(|index| verification_remove(quota_test_address(index), timestamp))
            .collect::<Vec<_>>();
        commit_messages(
            &mut engine,
            removes
                .iter()
                .map(|message| (message, Validity::Valid))
                .collect(),
        );
        let adds = (3000u16..3005)
            .map(|index| verification_contract_add(quota_test_address(index), timestamp))
            .collect::<Vec<_>>();
        commit_messages(
            &mut engine,
            adds.iter()
                .map(|message| (message, Validity::Valid))
                .collect(),
        );
        assert_eq!(verification_replica_counts(&engine), (5, 256));

        // Shed all five live adds. Row-neutral and always admitted, but each mints a permanent
        // tombstone, pushing tombstones past their own cap and rows to the row cap.
        for index in 3000u16..3005 {
            commit_message(
                &mut engine,
                &verification_remove(quota_test_address(index), timestamp + 1),
                Validity::Valid,
            );
        }
        assert_eq!(verification_replica_counts(&engine), (0, 261));

        // live_adds is 0, so the live-add cap would happily admit this. The row cap must not.
        let over_row_cap = verification_contract_add(quota_test_address(4000), timestamp + 2);
        commit_message(&mut engine, &over_row_cap, Validity::Invalid);
        assert_eq!(verification_replica_counts(&engine), (0, 261));

        // Row-neutral traffic still works at the row cap: re-adding a tombstoned address.
        let resurrect = verification_contract_add(quota_test_address(3000), timestamp + 3);
        commit_message(&mut engine, &resurrect, Validity::Valid);
        assert_eq!(verification_replica_counts(&engine), (1, 260));
    }

    #[test]
    fn test_shard_zero_verification_add_then_remove_cycles_are_row_bounded() {
        let timestamp = messages_factory::farcaster_time();
        let (mut engine, _temp_dir) = setup();
        register_user(
            VERIFICATION_FID,
            default_signer(),
            default_custody_address(),
            1,
            &mut engine,
        );

        // The attack the row cap exists to stop, in its literal form: each cycle takes a FRESH
        // address, adds it (admitted -- `live_adds` is 0 at the top of every cycle, so the
        // live-add cap never engages), then removes it (row-neutral, so the tombstone cap is
        // never consulted either). Every cycle leaves one permanent row behind for two gasless
        // messages. Only a bound on TOTAL ROWS terminates this.
        const ROW_CAP: u16 = 261; // max_messages(5) + tombstone_cap(256)
        for index in 0..ROW_CAP {
            let address = quota_test_address(2000 + index);
            commit_message(
                &mut engine,
                &verification_contract_add(address.clone(), timestamp),
                Validity::Valid,
            );
            commit_message(
                &mut engine,
                &verification_remove(address, timestamp + 1),
                Validity::Valid,
            );
        }
        assert_eq!(verification_replica_counts(&engine), (0, ROW_CAP as u64));

        // The next cycle cannot start: rows are at cap even though `live_adds` is 0.
        commit_message(
            &mut engine,
            &verification_contract_add(quota_test_address(2999), timestamp),
            Validity::Invalid,
        );
        assert_eq!(verification_replica_counts(&engine), (0, ROW_CAP as u64));
    }

    #[test]
    fn test_shard_zero_verification_tombstones_do_not_lock_out_live_adds() {
        let timestamp = messages_factory::farcaster_time();
        let (mut engine, _temp_dir) = setup();
        register_user(
            VERIFICATION_FID,
            default_signer(),
            default_custody_address(),
            1,
            &mut engine,
        );

        // B2 regression: five net-new tombstones used to consume the entire add quota and make
        // this fid terminal despite having no live verifications.
        let removes = (20u16..25)
            .map(|index| verification_remove(quota_test_address(index), timestamp))
            .collect::<Vec<_>>();
        commit_messages(
            &mut engine,
            removes
                .iter()
                .map(|message| (message, Validity::Valid))
                .collect(),
        );
        assert_eq!(verification_replica_counts(&engine), (0, 5));

        let add = verification_contract_add(quota_test_address(25), timestamp + 1);
        commit_message(&mut engine, &add, Validity::Valid);
        assert_eq!(verification_replica_counts(&engine), (1, 5));
    }

    #[test]
    fn test_shard_zero_verification_shed_state_allows_new_add() {
        let timestamp = messages_factory::farcaster_time();
        let (mut engine, _temp_dir) = setup();
        register_user(
            VERIFICATION_FID,
            default_signer(),
            default_custody_address(),
            1,
            &mut engine,
        );

        let adds = (30u16..35)
            .map(|index| verification_contract_add(quota_test_address(index), timestamp))
            .collect::<Vec<_>>();
        commit_messages(
            &mut engine,
            adds.iter()
                .map(|message| (message, Validity::Valid))
                .collect(),
        );
        assert_eq!(verification_replica_counts(&engine), (5, 0));

        let remove = verification_remove(quota_test_address(30), timestamp + 1);
        commit_message(&mut engine, &remove, Validity::Valid);
        assert_eq!(verification_replica_counts(&engine), (4, 1));

        let add = verification_contract_add(quota_test_address(35), timestamp + 2);
        commit_message(&mut engine, &add, Validity::Valid);
        assert_eq!(verification_replica_counts(&engine), (5, 1));
    }

    #[test]
    fn test_shard_zero_verification_net_new_remove_admitted_at_add_cap() {
        let timestamp = messages_factory::farcaster_time();
        let (mut engine, _temp_dir) = setup();
        register_user(
            VERIFICATION_FID,
            default_signer(),
            default_custody_address(),
            1,
            &mut engine,
        );

        let adds = (40u16..45)
            .map(|index| verification_contract_add(quota_test_address(index), timestamp))
            .collect::<Vec<_>>();
        commit_messages(
            &mut engine,
            adds.iter()
                .map(|message| (message, Validity::Valid))
                .collect(),
        );

        // This address can predate the shard-0 replica; its tombstone uses the independent bound.
        let remove = verification_remove(quota_test_address(45), timestamp + 1);
        commit_message(&mut engine, &remove, Validity::Valid);
        assert_eq!(verification_replica_counts(&engine), (5, 1));
    }

    #[test]
    fn test_shard_zero_verification_tombstone_cap_boundary() {
        const TOMBSTONE_CAP: u16 = 256;

        let timestamp = messages_factory::farcaster_time();
        let (mut engine, _temp_dir) = setup();
        register_user(
            VERIFICATION_FID,
            default_signer(),
            default_custody_address(),
            1,
            &mut engine,
        );

        let removes = (1000u16..1000 + TOMBSTONE_CAP)
            .map(|index| verification_remove(quota_test_address(index), timestamp))
            .collect::<Vec<_>>();
        commit_messages(
            &mut engine,
            removes
                .iter()
                .map(|message| (message, Validity::Valid))
                .collect(),
        );
        assert_eq!(verification_replica_counts(&engine), (0, 256));

        let over_cap = verification_remove(quota_test_address(1000 + TOMBSTONE_CAP), timestamp + 1);
        commit_message(&mut engine, &over_cap, Validity::Invalid);

        let superseding = verification_remove(quota_test_address(1000), timestamp + 1);
        commit_message(&mut engine, &superseding, Validity::Valid);
        assert_eq!(verification_replica_counts(&engine), (0, 256));
    }

    #[test]
    fn test_shard_zero_verification_add_remove_add_keeps_counters_and_row_count_stable() {
        let timestamp = messages_factory::farcaster_time();
        let (mut engine, _temp_dir) = setup();
        register_user(
            VERIFICATION_FID,
            default_signer(),
            default_custody_address(),
            1,
            &mut engine,
        );
        let address = quota_test_address(50);

        let add = verification_contract_add(address.clone(), timestamp);
        commit_message(&mut engine, &add, Validity::Valid);
        let counts = verification_replica_counts(&engine);
        assert_eq!(counts, (1, 0));
        assert_eq!(counts.0 + counts.1, 1);

        let remove = verification_remove(address.clone(), timestamp + 1);
        commit_message(&mut engine, &remove, Validity::Valid);
        let counts = verification_replica_counts(&engine);
        assert_eq!(counts, (0, 1));
        assert_eq!(counts.0 + counts.1, 1);

        let replacement_add = verification_contract_add(address, timestamp + 2);
        commit_message(&mut engine, &replacement_add, Validity::Valid);
        let counts = verification_replica_counts(&engine);
        assert_eq!(counts, (1, 0));
        assert_eq!(counts.0 + counts.1, 1);
    }

    // The `max_messages == 0 && is_add` gate is invisible to the boundary test above: with no
    // storage, a NON-superseding add is already rejected because `live_adds >= max_messages`.
    // The gate only carries independent meaning for a SUPERSEDING add, which short-circuits the
    // live-add count check — i.e. a fid whose storage lapsed replacing a verification it already
    // has. Without this case, deleting the gate leaves the whole suite green.
    #[test]
    fn test_shard_zero_superseding_add_rejected_without_storage() {
        let timestamp = messages_factory::farcaster_time();
        let (mut engine, _temp_dir) = setup();
        register_user(
            VERIFICATION_FID,
            default_signer(),
            default_custody_address(),
            1,
            &mut engine,
        );

        // Seed a record for the address so a later add supersedes rather than grows.
        let existing_add = verification_add(timestamp, None);
        commit_message(&mut engine, &existing_add, Validity::Valid);

        // A superseding add for that same address, judged against an empty storage slot, must
        // still be refused: shard 0's verification write path is otherwise gasless.
        let superseding_add = verification_add(timestamp + 1, None);
        assert!(matches!(
            engine.validate_user_message(
                &superseding_add,
                &StorageSlot::new(0, 0, 0, u32::MAX),
                &FarcasterTime::new((timestamp + 1) as u64),
                EngineVersion::V20,
                &mut RocksDbTransactionBatch::new(),
            ),
            Err(MessageValidationError::InsufficientStorage)
        ));

        // The mirror case is admitted by design, so an at-cap fid can always shed state.
        let superseding_remove = verification_remove(verification_address(), timestamp + 2);
        assert!(engine
            .validate_user_message(
                &superseding_remove,
                &StorageSlot::new(0, 0, 0, u32::MAX),
                &FarcasterTime::new((timestamp + 2) as u64),
                EngineVersion::V20,
                &mut RocksDbTransactionBatch::new(),
            )
            .is_ok());

        // A storage-free fid may supersede a replica-known row but may not mint a permanent
        // tombstone for an address shard 0 has never seen.
        let net_new_remove = verification_remove(quota_test_address(60), timestamp + 2);
        assert!(matches!(
            engine.validate_user_message(
                &net_new_remove,
                &StorageSlot::new(0, 0, 0, u32::MAX),
                &FarcasterTime::new((timestamp + 2) as u64),
                EngineVersion::V20,
                &mut RocksDbTransactionBatch::new(),
            ),
            Err(MessageValidationError::InsufficientStorage)
        ));
    }

    #[test]
    fn test_shard_zero_verification_quota_includes_borrowed_storage() {
        const LENDER_FID: u64 = 100;
        let (mut engine, _temp_dir) = setup();
        register_user(
            LENDER_FID,
            default_signer(),
            default_custody_address(),
            2,
            &mut engine,
        );
        register_user(
            VERIFICATION_FID,
            default_signer(),
            default_custody_address(),
            0,
            &mut engine,
        );
        let timestamp = messages_factory::farcaster_time();
        let lend = messages_factory::storage_lend::create_storage_lend(
            LENDER_FID,
            VERIFICATION_FID,
            1,
            StorageUnitType::UnitType2025,
            Some(timestamp),
            None,
        );
        commit_message(&mut engine, &lend, Validity::Valid);

        // The borrower owns no storage units. Admission succeeds only if shard 0 computes the
        // verification limit from the net slot (purchased - lent + borrowed), as data shards do.
        let add = verification_add(timestamp + 1, None);
        commit_message(&mut engine, &add, Validity::Valid);
    }

    #[test]
    fn test_shard_zero_verification_rejects_pre_activation_embedded_timestamp() {
        let (mut block_engine, _temp_dir) = setup_with_options(BlockEngineOptions {
            network: FarcasterNetwork::Mainnet,
            ..BlockEngineOptions::default()
        });
        register_user(
            VERIFICATION_FID,
            default_signer(),
            default_custody_address(),
            1,
            &mut block_engine,
        );
        // Be precise about what this pins, because it is weaker than the name suggests. No
        // network can currently straddle the activation boundary: V20 is unscheduled on mainnet
        // and testnet (so *every* timestamp there resolves pre-V20), and devnet activates V20 at
        // timestamp 0 (so no pre-activation timestamp exists at all). This test therefore proves
        // the floor is present and rejects -- deleting the floor turns it red -- but it cannot
        // yet prove the floor *discriminates* pre- from post-activation timestamps. Add that
        // boundary test when V20 is scheduled; until then the discrimination is untestable.
        //
        // Pinned to the Farcaster epoch rather than `now` so the test keeps meaning the same
        // thing afterwards: once V20 is scheduled, `now` would resolve to V20, the floor would
        // rightly stop rejecting, and a `now`-based test would go red during the very rollout it
        // exists to protect. Only the future bound in `validate_timestamp` constrains this value,
        // so an epoch timestamp reaches the floor instead of dying earlier on an unrelated error.
        let pre_activation_timestamp = 0;
        let message = verification_add(pre_activation_timestamp, None);

        let error = block_engine
            .validate_user_message(
                &message,
                &StorageSlot::new(0, 0, 1, u32::MAX),
                &FarcasterTime::new(pre_activation_timestamp as u64),
                EngineVersion::V20,
                &mut RocksDbTransactionBatch::new(),
            )
            .unwrap_err();
        assert!(matches!(
            &error,
            MessageValidationError::VerificationTimestampBeforeActivation
        ));
        assert_eq!(
            error.to_string(),
            "verification timestamp predates shard-zero activation"
        );
    }

    /// A message's `r#type` and its body are independent on the wire, and nothing in
    /// `validate_message` requires them to agree. `route_message` and the merge arm dispatch on
    /// `r#type`, while the validation arm matches on the body -- so a KEY_ADD-typed message
    /// carrying a verification body routes to shard 0 and reaches the verification validation arm.
    /// Admitting it would let `submit_message` accept and gossip a message shard 0 can never merge
    /// (`merge_key_add` rejects the body), burning mempool and block space, and would be a devnet
    /// behavior change out of an increment that must be inert. Such a message must stay rejected
    /// exactly as it was before verification arms existed.
    #[test]
    fn test_shard_zero_verification_body_with_mismatched_type_is_rejected() {
        let (mut block_engine, _temp_dir) = setup();
        register_user(
            VERIFICATION_FID,
            default_signer(),
            default_custody_address(),
            1,
            &mut block_engine,
        );
        let timestamp = messages_factory::farcaster_time();

        for spoofed_type in [
            crate::proto::MessageType::KeyAdd,
            crate::proto::MessageType::KeyRemove,
            crate::proto::MessageType::LendStorage,
        ] {
            let body = crate::proto::VerificationAddAddressBody {
                address: verification_address(),
                claim_signature: hex::decode(VERIFICATION_CLAIM_SIGNATURE_HEX).unwrap(),
                block_hash: hex::decode(VERIFICATION_BLOCK_HASH_HEX).unwrap(),
                verification_type: 0,
                chain_id: 0,
                protocol: 0,
            };
            let spoofed = messages_factory::create_message_with_data(
                VERIFICATION_FID,
                spoofed_type,
                crate::proto::message_data::Body::VerificationAddAddressBody(body),
                Some(timestamp),
                None,
            );

            assert!(
                matches!(
                    block_engine.validate_user_message(
                        &spoofed,
                        &StorageSlot::new(0, 0, 1, u32::MAX),
                        &FarcasterTime::new(timestamp as u64),
                        EngineVersion::V20,
                        &mut RocksDbTransactionBatch::new(),
                    ),
                    Err(MessageValidationError::InvalidMessageType)
                ),
                "{spoofed_type:?}-typed message with a verification body must be rejected"
            );
        }
    }

    /// End-to-end inertness on a network where the feature is dormant. The unit tests above drive
    /// `validate_user_message` directly; this one drives a whole block through propose/commit, so
    /// it pins all three gates together -- the validation arm, the replay gate, and the merge
    /// arm's own guard -- plus the things a consensus reader actually cares about: no trie key,
    /// no stored verification, no fan-out.
    #[test]
    fn test_shard_zero_verification_inert_in_pre_activation_block() {
        let (mut block_engine, _temp_dir) = setup_with_options(BlockEngineOptions {
            network: FarcasterNetwork::Mainnet,
            ..BlockEngineOptions::default()
        });
        register_user(
            VERIFICATION_FID,
            default_signer(),
            default_custody_address(),
            1,
            &mut block_engine,
        );

        let add = verification_add(messages_factory::farcaster_time(), None);
        let block = commit_message(&mut block_engine, &add, Validity::Invalid);

        assert_no_verification_block_events(&block);
        assert_verification_index(&block_engine, None);
        assert_eq!(
            VerificationStore::get_verification_add(
                &block_engine.stores().verification_store,
                VERIFICATION_FID,
                &verification_address(),
                None,
            )
            .unwrap(),
            None,
            "a dormant-feature block must not merge a verification into the replica"
        );
    }

    #[test]
    fn test_shard_zero_verification_merge_lww_index_trie_event_and_fanout() {
        let (mut block_engine, _temp_dir) = setup();
        register_user(
            VERIFICATION_FID,
            default_signer(),
            default_custody_address(),
            1,
            &mut block_engine,
        );
        let timestamp = messages_factory::farcaster_time();

        let add = verification_add(timestamp, None);
        let add_block = commit_message(&mut block_engine, &add, Validity::Valid);
        assert_one_verification_block_event(&add_block, &add);
        assert_verification_index(&block_engine, Some(&add));
        assert_eq!(
            VerificationStore::get_verification_add(
                &block_engine.stores().verification_store,
                VERIFICATION_FID,
                &verification_address(),
                None,
            )
            .unwrap(),
            Some(add.clone())
        );
        let add_event_id =
            crate::storage::store::account::HubEventIdGenerator::make_event_id_for_block_number(
                add_block
                    .header
                    .as_ref()
                    .unwrap()
                    .height
                    .as_ref()
                    .unwrap()
                    .block_number,
            ) + 1;
        let add_event = HubEvent::get_event(block_engine.stores().db, add_event_id).unwrap();
        match add_event.body {
            Some(crate::proto::hub_event::Body::MergeMessageBody(body)) => {
                assert_eq!(body.message, Some(add.clone()));
            }
            other => panic!("expected merge-message hub event, got {other:?}"),
        }

        // `commit_message` already asserts the committed message's own trie presence, so each
        // assertion here names the *other* message -- the LWW eviction is the claim.
        let replacement = verification_add(timestamp + 1, None);
        let replacement_block = commit_message(&mut block_engine, &replacement, Validity::Valid);
        assert_one_verification_block_event(&replacement_block, &replacement);
        assert_verification_index(&block_engine, Some(&replacement));
        assert!(!message_exists_in_trie(&mut block_engine, &add));

        let remove = messages_factory::verifications::create_verification_remove(
            VERIFICATION_FID,
            verification_address(),
            Some(timestamp + 2),
            None,
        );
        let remove_block = commit_message(&mut block_engine, &remove, Validity::Valid);
        assert_one_verification_block_event(&remove_block, &remove);
        assert_verification_index(&block_engine, None);
        assert!(!message_exists_in_trie(&mut block_engine, &replacement));
        assert_eq!(
            VerificationStore::get_verification_remove(
                &block_engine.stores().verification_store,
                VERIFICATION_FID,
                &verification_address(),
            )
            .unwrap(),
            Some(remove.clone())
        );

        // The stale add is rejected by the CRDT in `merge`, not by `validate_user_message` -- so
        // the rejection is only visible if the merge error is surfaced rather than swallowed.
        // `simulate_message` is what submitMessage consults once routing reaches shard 0; it
        // must not report success for a message that was never stored.
        assert!(
            block_engine.simulate_message(&add).is_err(),
            "stale add's merge error must reach simulate_message, not be swallowed"
        );

        let old_add_block = commit_message(&mut block_engine, &add, Validity::Invalid);
        assert_no_verification_block_events(&old_add_block);
        assert_verification_index(&block_engine, None);
        assert!(message_exists_in_trie(&mut block_engine, &remove));
        let merge_failure_id =
            crate::storage::store::account::HubEventIdGenerator::make_event_id_for_block_number(
                old_add_block
                    .header
                    .as_ref()
                    .unwrap()
                    .height
                    .unwrap()
                    .block_number,
            ) + 1;
        let merge_failure =
            HubEvent::get_event(block_engine.stores().db, merge_failure_id).unwrap();
        assert!(matches!(
            merge_failure.body,
            Some(crate::proto::hub_event::Body::MergeFailure(body))
                if body.message.as_ref() == Some(&add)
                    && body.code == "bad_request.conflict"
                    && !body.reason.is_empty()
        ));
    }

    #[tokio::test]
    async fn test_validate_and_commit_old_blocks() {
        // Test that validate and commit will work for old blocks even after features are active
        let (mut block_engine, _temp_dir) = setup_with_options(BlockEngineOptions {
            network: FarcasterNetwork::Mainnet,
            ..BlockEngineOptions::default()
        });

        let height = block_engine.get_confirmed_height().increment();
        validate_and_commit_state_change(
            &mut block_engine,
            &BlockStateChange {
                timestamp: FarcasterTime::from_unix_seconds(1752685200),
                new_state_root: vec![],
                events_hash: vec![],
                transactions: vec![],
                events: vec![],
            },
        );
        assert_eq!(block_engine.get_confirmed_height(), height);
    }

    #[tokio::test]
    async fn test_user_messages_dropped_if_no_storage() {
        let (mut block_engine, _temp_dir) = setup();
        let height = block_engine.get_confirmed_height().increment();
        // These messages are included in the transaction list but not included in the state root.
        let messages = vec![MempoolMessage::UserMessage(
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "hi", None, None),
        )];
        let state_change = block_engine.propose_state_change(messages, height, None);
        assert_eq!(state_change.transactions.len(), 0);
        assert!(state_change.events.is_empty());
        assert!(state_change.new_state_root.is_empty());
        assert!(state_change.events_hash.is_empty());

        validate_and_commit_state_change(&mut block_engine, &state_change);
        assert_eq!(block_engine.get_confirmed_height(), height);
    }

    #[tokio::test]
    async fn test_user_messages_put_in_block_if_storage_purchased() {
        let (mut block_engine, _temp_dir) = setup();
        // Rolling grant date; a fixed one ages out against wall-clock `is_active`.
        // See `test_helper::default_storage_event`.
        let onchain_event = events_factory::create_rent_event_with_timestamp(
            FID_FOR_TEST,
            1,
            crate::utils::factory::time::current_timestamp(),
        );
        commit_event(&mut block_engine, &onchain_event);

        let height = block_engine.get_confirmed_height().increment();
        let initial_state_root = block_engine.trie_root_hash();
        let messages = vec![MempoolMessage::UserMessage(
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "hi", None, None),
        )];

        // The message is included in the block but doesn't impact trie state.
        let state_change = block_engine.propose_state_change(messages, height, None);
        assert_eq!(state_change.transactions.len(), 1);
        assert_eq!(state_change.new_state_root, initial_state_root);

        validate_and_commit_state_change(&mut block_engine, &state_change);
        assert_eq!(block_engine.get_confirmed_height(), height);
    }

    #[tokio::test]
    #[should_panic(expected = "State change commit failed: merkle trie root hash mismatch")]
    async fn test_invalid_state_root() {
        let (mut block_engine, _temp_dir) = setup();
        let height = block_engine.get_confirmed_height().increment();
        let invalid_hash = hex::decode("ffffffffffffffffffffffffffffffffffffffff").unwrap();

        let mut state_change = block_engine.propose_state_change(vec![], height, None);

        let valid = block_engine.validate_state_change(&state_change, height);
        assert!(valid);

        state_change.new_state_root = invalid_hash;
        let valid = block_engine.validate_state_change(&state_change, height);
        assert!(!valid);

        block_engine.commit_block(&state_change_to_block(height.block_number, &state_change));
    }

    #[tokio::test]
    #[should_panic(expected = "State change commit failed: events hash mismatch")]
    async fn test_invalid_events_hash() {
        let (mut block_engine, _temp_dir) = setup();
        let height = block_engine.get_confirmed_height().increment();
        let invalid_hash = hex::decode("ffffffffffffffffffffffffffffffffffffffff").unwrap();

        let mut state_change = block_engine.propose_state_change(vec![], height, None);

        let valid = block_engine.validate_state_change(&state_change, height);
        assert!(valid);

        state_change.events_hash = invalid_hash;
        let valid = block_engine.validate_state_change(&state_change, height);
        assert!(!valid);

        block_engine.commit_block(&state_change_to_block(height.block_number, &state_change));
    }

    #[tokio::test]
    async fn test_merge_onchain_event() {
        let (mut block_engine, _temp_dir) = setup();
        // Rolling grant date; a fixed one ages out against wall-clock `is_active`.
        // See `test_helper::default_storage_event`.
        let onchain_event = events_factory::create_rent_event_with_timestamp(
            FID_FOR_TEST,
            1,
            crate::utils::factory::time::current_timestamp(),
        );
        let block = commit_event(&mut block_engine, &onchain_event);
        // Don't generate any block events for onchain events
        assert!(block.events.is_empty());
        assert!(
            block_engine.trie_key_exists(trie_ctx(), &TrieKey::for_onchain_event(&onchain_event))
        );
        assert_eq!(
            block.header.as_ref().unwrap().state_root,
            block_engine.trie_root_hash()
        );
        let storage_slot = block_engine
            .stores()
            .get_storage_slot_for_fid(
                FID_FOR_TEST,
                crate::version::version::EngineVersion::latest(),
                &vec![],
                true,
                true,
            )
            .unwrap();
        assert_eq!(storage_slot.units_for(StorageUnitType::UnitType2025), 1);
    }

    #[tokio::test]
    async fn test_block_engine_sorts_channel_events_before_replay_when_enabled() {
        let (mut block_engine, _temp_dir) = setup();
        let channel_key = "ordered";
        let (transfer, register) = inverted_same_block_channel_events(channel_key);
        let height = block_engine.get_confirmed_height().increment();

        let state_change = block_engine.propose_state_change(
            vec![
                MempoolMessage::OnchainEvent(transfer),
                MempoolMessage::OnchainEvent(register),
            ],
            height,
            None,
        );

        validate_and_commit_state_change(&mut block_engine, &state_change);
        let stores = block_engine.stores();
        let owner_record = stores
            .onchain_event_store
            .get_channel_owner(channel_key, None)
            .unwrap()
            .unwrap();
        assert_eq!(owner_record.owner_address, channel_owner(0xBB));
    }

    #[tokio::test]
    async fn test_block_engine_pre_v20_drops_channel_events_before_ordering_matters() {
        let (mut block_engine, _temp_dir) = setup_with_options(BlockEngineOptions {
            network: FarcasterNetwork::Mainnet,
            ..BlockEngineOptions::default()
        });
        let channel_key = "pre-v20";
        let (transfer, register) = inverted_same_block_channel_events(channel_key);
        let height = block_engine.get_confirmed_height().increment();
        let timestamp = FarcasterTime::from_unix_seconds(4102444800);

        let state_change = block_engine.propose_state_change(
            vec![
                MempoolMessage::OnchainEvent(transfer),
                MempoolMessage::OnchainEvent(register),
            ],
            height,
            Some(timestamp),
        );

        validate_and_commit_state_change(&mut block_engine, &state_change);
        let stores = block_engine.stores();
        assert!(stores
            .onchain_event_store
            .get_channel_owner(channel_key, None)
            .unwrap()
            .is_none());
    }

    // --- Shard-0 fan-out of channel-register events -------------------------------------------

    fn single_channel_register(channel_key: &str, owner_byte: u8) -> OnChainEvent {
        events_factory::create_channel_register_event(
            channel_key,
            channel_label(channel_key),
            channel_owner(owner_byte),
            1_900_000_000,
            ChannelRegisterEventType::Register,
            100,
            0,
        )
    }

    fn merge_on_chain_fan_out_events(block: &Block) -> Vec<&BlockEvent> {
        block
            .events
            .iter()
            .filter(|event| {
                event.data.as_ref().unwrap().r#type() == BlockEventType::MergeOnChainEvent
            })
            .collect()
    }

    fn fanned_out_onchain_event(block_event: &BlockEvent) -> &OnChainEvent {
        match block_event.data.as_ref().unwrap().body.as_ref().unwrap() {
            block_event_data::Body::MergeOnChainEventEventBody(body) => {
                body.on_chain_event.as_ref().unwrap()
            }
            other => panic!("expected MergeOnChainEventEventBody, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_channel_register_fans_out_one_merge_on_chain_event() {
        // V20 devnet: a merged channel-register event fans out exactly one
        // MergeOnChainEvent BlockEvent carrying the whole original event, seqnum-chained
        // and persisted, with events_hash covering it.
        let (mut block_engine, _temp_dir) = setup();
        let register = single_channel_register("pets", 0xAA);

        let block = commit_event(&mut block_engine, &register);

        let fan_out = merge_on_chain_fan_out_events(&block);
        assert_eq!(
            fan_out.len(),
            1,
            "exactly one fan-out event per channel register"
        );
        let data = fan_out[0].data.as_ref().unwrap();
        assert_eq!(data.seqnum, 1, "first block event gets seqnum 1");
        assert_eq!(data.event_index, 0);
        assert_eq!(
            data.block_number,
            block.header.as_ref().unwrap().height.unwrap().block_number
        );
        // Carries the whole original event.
        let carried = fanned_out_onchain_event(fan_out[0]);
        assert_eq!(carried.transaction_hash, register.transaction_hash);
        assert_eq!(carried.r#type, register.r#type);
        // events_hash covers the fan-out event's hash.
        let mut hasher = blake3::Hasher::new();
        for event in &block.events {
            hasher.update(&event.hash);
        }
        assert_eq!(
            block.header.as_ref().unwrap().events_hash,
            hasher.finalize().as_bytes().to_vec()
        );
        // Persisted in the block-event store.
        let stored = block_engine
            .stores()
            .block_event_store
            .get_block_event_by_seqnum(1)
            .unwrap()
            .unwrap();
        assert_eq!(&stored, fan_out[0]);
    }

    #[tokio::test]
    async fn test_channel_register_fan_out_seqnum_chains_across_blocks() {
        // Two registers in consecutive blocks get consecutive seqnums (1, then 2), proving the
        // fan-out reads the persisted max seqnum rather than resetting per block.
        let (mut block_engine, _temp_dir) = setup();

        // Distinct log indices so the two registers have distinct onchain primary keys.
        let register1 = events_factory::create_channel_register_event(
            "pets",
            channel_label("pets"),
            channel_owner(0xAA),
            1_900_000_000,
            ChannelRegisterEventType::Register,
            100,
            0,
        );
        let register2 = events_factory::create_channel_register_event(
            "casts",
            channel_label("casts"),
            channel_owner(0xBB),
            1_900_000_000,
            ChannelRegisterEventType::Register,
            100,
            1,
        );
        let block1 = commit_event(&mut block_engine, &register1);
        let block2 = commit_event(&mut block_engine, &register2);

        assert_eq!(
            merge_on_chain_fan_out_events(&block1)[0]
                .data
                .as_ref()
                .unwrap()
                .seqnum,
            1
        );
        assert_eq!(
            merge_on_chain_fan_out_events(&block2)[0]
                .data
                .as_ref()
                .unwrap()
                .seqnum,
            2
        );
    }

    #[tokio::test]
    async fn test_pre_feature_channel_register_does_not_fan_out() {
        // Mainnet pre-V20: ChannelRegistrations (and, co-activated, ChannelOwnershipEvents) are
        // off, so the channel event is dropped before it can merge — no fan-out, and events_hash
        // stays empty, byte-identical to the pre-increment behavior.
        let (mut block_engine, _temp_dir) = setup_with_options(BlockEngineOptions {
            network: FarcasterNetwork::Mainnet,
            ..BlockEngineOptions::default()
        });
        let height = block_engine.get_confirmed_height().increment();
        let state_change = block_engine.propose_state_change(
            vec![MempoolMessage::OnchainEvent(single_channel_register(
                "pets", 0xAA,
            ))],
            height,
            Some(FarcasterTime::from_unix_seconds(4102444800)),
        );

        assert!(
            state_change.events.is_empty(),
            "no fan-out before the feature is active"
        );
        assert!(state_change.events_hash.is_empty());
    }

    #[tokio::test]
    async fn test_non_channel_onchain_event_does_not_fan_out() {
        // A non-channel onchain event (storage rent) still produces a MergeOnChainEventBody hub
        // event, but the fan-out arm only carries channel registers — so nothing is emitted.
        let (mut block_engine, _temp_dir) = setup();
        let rent = events_factory::create_rent_event(
            FID_FOR_TEST,
            1,
            StorageUnitType::UnitType2025,
            false,
            FarcasterNetwork::Devnet,
        );

        let block = commit_event(&mut block_engine, &rent);

        assert!(
            merge_on_chain_fan_out_events(&block).is_empty(),
            "only channel-register events fan out"
        );
    }

    #[tokio::test]
    async fn test_channel_register_fan_out_is_deterministic() {
        // Two fresh engines replaying the same channel register produce byte-identical event
        // lists — the fan-out is a pure function of the block's merged events.
        let register = single_channel_register("pets", 0xAA);

        let (mut engine_a, _dir_a) = setup();
        let (mut engine_b, _dir_b) = setup();
        let block_a = commit_event(&mut engine_a, &register);
        let block_b = commit_event(&mut engine_b, &register);

        assert_eq!(block_a.events, block_b.events);
    }

    #[tokio::test]
    async fn test_block_engine_onchain_input_order_does_not_change_committed_state() {
        // Two properties from one setup: two fresh V20 engines see the same same-eth-block
        // REGISTER + TRANSFER pair in opposite mempool arrival orders.
        //
        // 1. state_root convergence (consensus agreement): both orders must commit the SAME
        //    non-empty state_root — nodes that pulled these events in different mempool orders
        //    still agree on hashed state. NB this holds by the content-keyed trie and does not by
        //    itself exercise the sort (it would pass with the sort disabled too). Whether the sort
        //    *itself* perturbs state_root is not reachable through the engine: channel events are
        //    only accepted once ChannelRegistrations is on, and SortedBlockEngineEvents
        //    co-activates with it, so there is no "accepted-but-unsorted" state to compare
        //    against. That neutrality is structural — the channel-owner index is a RocksDB
        //    secondary index, never a trie leaf (see block_engine.rs / onchain_event_store.rs).
        // 2. owner resolution (the sort's actual signal): engine_a's INVERTED input resolves the
        //    post-TRANSFER owner 0xBB only because the canonical sort reorders REGISTER before
        //    TRANSFER. With the sort disabled engine_a drops the TRANSFER and resolves 0xAA, so
        //    the owner assertion below is what fails if the sort regresses.
        let channel_key = "order-independent";

        // Engine A: inverted mempool order — TRANSFER (log_index 9) before REGISTER (log_index 7).
        let (mut engine_a, _dir_a) = setup();
        let (transfer_a, register_a) = inverted_same_block_channel_events(channel_key);
        let height_a = engine_a.get_confirmed_height().increment();
        let change_a = engine_a.propose_state_change(
            vec![
                MempoolMessage::OnchainEvent(transfer_a),
                MempoolMessage::OnchainEvent(register_a),
            ],
            height_a,
            None,
        );
        validate_and_commit_state_change(&mut engine_a, &change_a);

        // Engine B: canonical order — REGISTER before TRANSFER.
        let (mut engine_b, _dir_b) = setup();
        let (transfer_b, register_b) = inverted_same_block_channel_events(channel_key);
        let height_b = engine_b.get_confirmed_height().increment();
        let change_b = engine_b.propose_state_change(
            vec![
                MempoolMessage::OnchainEvent(register_b),
                MempoolMessage::OnchainEvent(transfer_b),
            ],
            height_b,
            None,
        );
        validate_and_commit_state_change(&mut engine_b, &change_b);

        // Hashed consensus state is identical (and non-empty) regardless of input order.
        assert!(!change_a.new_state_root.is_empty());
        assert_eq!(change_a.new_state_root, change_b.new_state_root);

        // ...and both resolve the same post-TRANSFER owner (0xBB). This is the assertion that
        // fails if the canonical sort is ever disabled for engine_a's inverted input.
        for engine in [&engine_a, &engine_b] {
            let owner = engine
                .stores()
                .onchain_event_store
                .get_channel_owner(channel_key, None)
                .unwrap()
                .unwrap();
            assert_eq!(owner.owner_address, channel_owner(0xBB));
        }
    }

    #[tokio::test]
    async fn test_heartbeat_generated_on_interval() {
        let (mut block_engine, _temp_dir) = setup();
        // The heartbeat interval is 5 blocks, generate the first 4 where there will be no events
        for _ in 0..4 {
            let height = block_engine.get_confirmed_height().increment();
            let state_change = block_engine.propose_state_change(vec![], height, None);
            assert!(state_change.events.is_empty());
            validate_and_commit_state_change(&mut block_engine, &state_change);
        }

        let height = block_engine.get_confirmed_height().increment();
        let state_change = block_engine.propose_state_change(vec![], height, None);
        assert_eq!(state_change.events.len(), 1);
        assert_eq!(state_change.events[0].data.as_ref().unwrap().seqnum, 1);
        assert_eq!(
            state_change.events[0].data.as_ref().unwrap().block_number,
            height.block_number
        );
        assert_eq!(state_change.events[0].data.as_ref().unwrap().event_index, 0);
        validate_and_commit_state_change(&mut block_engine, &state_change);

        // The heartbeat interval is 5 blocks, generate the next 4 where there will be no events
        for _ in 0..4 {
            let height = block_engine.get_confirmed_height().increment();
            let state_change = block_engine.propose_state_change(vec![], height, None);
            assert!(state_change.events.is_empty());
            validate_and_commit_state_change(&mut block_engine, &state_change);
        }

        // Check that seqnum is incremented properly
        let height = block_engine.get_confirmed_height().increment();
        let state_change = block_engine.propose_state_change(vec![], height, None);
        assert_eq!(state_change.events.len(), 1);
        assert_eq!(state_change.events[0].data.as_ref().unwrap().seqnum, 2);
        assert_eq!(
            state_change.events[0].data.as_ref().unwrap().block_number,
            height.block_number
        );
        assert_eq!(state_change.events[0].data.as_ref().unwrap().event_index, 0);
        validate_and_commit_state_change(&mut block_engine, &state_change);
    }

    #[tokio::test]
    async fn test_storage_lend_message_merged() {
        let (mut block_engine, _temp_dir) = setup();

        // Register user with storage
        register_user(
            FID_FOR_TEST,
            default_signer(),
            default_custody_address(),
            1000,
            &mut block_engine,
        );

        let lend_message = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST,
            FID_FOR_TEST + 1,
            100,
            StorageUnitType::UnitType2025,
            None,
            None,
        );

        let block = commit_message(&mut block_engine, &lend_message, Validity::Valid);

        // Should generate one block event for the storage lend
        assert_eq!(block.events.len(), 1);
        assert_merge_message_event(&block.events[0], &lend_message);
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST,
            StorageUnitType::UnitType2025,
            900,
        );
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST + 1,
            StorageUnitType::UnitType2025,
            100,
        );
    }

    #[tokio::test]
    async fn test_multiple_storage_lends_in_same_transaction() {
        let (mut block_engine, _temp_dir) = setup();

        // Register user with only 250 units of storage - not enough for all lends
        register_user(
            FID_FOR_TEST,
            default_signer(),
            default_custody_address(),
            400, // Only 300 units - insufficient for all lends (100 + 200 + 150 = 450)
            &mut block_engine,
        );

        // Create multiple lend messages from same FID to different recipients
        let lend_message1 = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST,
            FID_FOR_TEST + 1,
            100, // This should succeed (400 - 100 = 300 remaining)
            StorageUnitType::UnitType2025,
            None,
            None,
        );
        let lend_message2 = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST,
            FID_FOR_TEST + 2,
            200, // This should succeed (300 - 200 = 100 remaining)
            StorageUnitType::UnitType2025,
            None,
            None,
        );
        let lend_message3 = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST,
            FID_FOR_TEST + 3,
            150, // This should fail (still insufficient storage)
            StorageUnitType::UnitType2025,
            None,
            None,
        );

        let block = commit_messages(
            &mut block_engine,
            vec![
                (&lend_message1, Validity::Valid),
                (&lend_message2, Validity::Valid),
                (&lend_message3, Validity::Invalid),
            ],
        );

        // Should only generate one block event for the successful lend (the first one)
        // The other two should fail during merge due to insufficient storage
        assert_eq!(block.events.len(), 2);
        assert_eq!(block.events[1].seqnum(), block.events[0].seqnum() + 1);
        assert_merge_message_event(&block.events[0], &lend_message1);
        assert_merge_message_event(&block.events[1], &lend_message2);
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST,
            StorageUnitType::UnitType2025,
            100,
        );
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST + 1,
            StorageUnitType::UnitType2025,
            100,
        );
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST + 2,
            StorageUnitType::UnitType2025,
            200,
        );
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST + 3,
            StorageUnitType::UnitType2025,
            0,
        );
    }

    #[tokio::test]
    async fn test_borrowed_storage_cannot_be_lent() {
        let (mut block_engine, _temp_dir) = setup();

        // Register FID_FOR_TEST + 1 with some storage to lend to FID_FOR_TEST + 2
        register_user(
            FID_FOR_TEST + 1,
            default_signer(),
            default_custody_address(),
            500,
            &mut block_engine,
        );

        // Register FID_FOR_TEST + 2 so they can receive lent storage
        register_user(
            FID_FOR_TEST + 2,
            default_signer(),
            default_custody_address(),
            0, // No initial storage
            &mut block_engine,
        );

        // FID_FOR_TEST + 1 lends storage to FID_FOR_TEST + 2
        let lend_message1 = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST + 1,
            FID_FOR_TEST + 2,
            300,
            StorageUnitType::UnitType2025,
            None,
            None,
        );
        let block = commit_message(&mut block_engine, &lend_message1, Validity::Valid);
        assert_merge_message_event(&block.events[0], &lend_message1);
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST + 1,
            StorageUnitType::UnitType2025,
            200,
        );
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST + 2,
            StorageUnitType::UnitType2025,
            300,
        );

        // Now FID_FOR_TEST + 2 tries to lend storage they don't own
        // They have 300 borrowed units, but 0 owned units
        let lend_message2 = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST + 2, // Borrower trying to lend
            FID_FOR_TEST,     // Different recipient
            100,              // Amount they don't actually own
            StorageUnitType::UnitType2025,
            None,
            None,
        );

        let block = commit_message(&mut block_engine, &lend_message2, Validity::Invalid);

        // No block events should be generated for failed storage lend
        assert_eq!(block.events.len(), 0);
    }

    #[tokio::test]
    async fn test_lender_can_take_back_storage_by_setting_to_zero() {
        let (mut block_engine, _temp_dir) = setup();

        // Register lender with storage
        register_user(
            FID_FOR_TEST,
            default_signer(),
            default_custody_address(),
            500,
            &mut block_engine,
        );

        // Make sure to retain 1 unit for the lender so the lender can revoke.
        let invalid_lend_message = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST,
            FID_FOR_TEST + 1,
            500,
            StorageUnitType::UnitType2025,
            None,
            None,
        );
        commit_message(&mut block_engine, &invalid_lend_message, Validity::Invalid);

        // Lender lends 300 units to borrower
        let lend_message1 = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST,
            FID_FOR_TEST + 1,
            499,
            StorageUnitType::UnitType2025,
            None,
            None,
        );
        let block1 = commit_message(&mut block_engine, &lend_message1, Validity::Valid);
        assert_eq!(
            block1
                .events
                .iter()
                .filter(|event| event.data.as_ref().unwrap().r#type() != BlockEventType::Heartbeat)
                .count(),
            1
        );
        assert_merge_message_event(&block1.events[0], &lend_message1);

        // Verify initial balances after lending
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST,
            StorageUnitType::UnitType2025,
            1,
        ); // 500 - 499
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST + 1,
            StorageUnitType::UnitType2025,
            499,
        ); // borrowed

        // Lender takes back storage by setting lend to 0
        let lend_message2 = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST,
            FID_FOR_TEST + 1,
            0, // Setting to 0 takes back the storage
            StorageUnitType::UnitType2025,
            Some(lend_message1.data.as_ref().unwrap().timestamp + 1),
            None,
        );
        let block2 = commit_message(&mut block_engine, &lend_message2, Validity::Invalid); // Mark as invalid because we don't expect this message to be in the trie
        assert_merge_message_event(&block2.events[0], &lend_message2);

        // Verify balances after taking back storage
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST,
            StorageUnitType::UnitType2025,
            500,
        ); // Back to original
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST + 1,
            StorageUnitType::UnitType2025,
            0,
        ); // No more borrowed storage

        // The old message shouldn't get merged again if it's far enough in the past
        commit_message_at(
            &mut block_engine,
            &lend_message1,
            FarcasterTime::new(lend_message1.data.as_ref().unwrap().timestamp as u64 + (60 * 11)),
            Validity::Invalid,
        );
    }

    #[tokio::test]
    async fn test_user_with_low_total_storage_cannot_lend() {
        let (mut block_engine, _temp_dir) = setup_with_options(BlockEngineOptions {
            network: FarcasterNetwork::Mainnet,
            ..Default::default()
        });

        register_user(
            FID_FOR_TEST,
            default_signer(),
            default_custody_address(),
            500, // Only 500 units - below the 1000 unit minimum for lending
            &mut block_engine,
        );

        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST,
            StorageUnitType::UnitType2025,
            500, // Original amount
        );
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST + 1,
            StorageUnitType::UnitType2025,
            0, // No borrowed storage
        );

        let future_time = FarcasterTime::from_unix_seconds(1761019200);

        // Attempt to create a storage lend message
        let lend_message = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST,
            FID_FOR_TEST + 1,
            10, // Try to lend 10 units
            StorageUnitType::UnitType2025,
            Some(future_time.to_u64() as u32 - 1),
            None,
        );

        // The message should be invalid due to insufficient total storage
        let block = commit_message_at(
            &mut block_engine,
            &lend_message,
            future_time.clone(),
            Validity::Invalid,
        );

        // No block events should be generated for failed storage lend
        assert_eq!(block.events.len(), 0);

        // Storage balances should remain unchanged
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST,
            StorageUnitType::UnitType2025,
            500, // Original amount
        );
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST + 1,
            StorageUnitType::UnitType2025,
            0, // No borrowed storage
        );

        register_user(
            FID_FOR_TEST,
            default_signer(),
            default_custody_address(),
            500,
            &mut block_engine,
        );

        // Goes through if the user gets 1000 units
        let lend_message = messages_factory::storage_lend::create_storage_lend(
            FID_FOR_TEST,
            FID_FOR_TEST + 1,
            10, // Try to lend 10 units
            StorageUnitType::UnitType2025,
            Some(future_time.to_u64() as u32 - 1),
            None,
        );

        commit_message_at(
            &mut block_engine,
            &lend_message,
            future_time,
            Validity::Valid,
        );

        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST,
            StorageUnitType::UnitType2025,
            990,
        );
        assert_storage_balance(
            &block_engine,
            FID_FOR_TEST + 1,
            StorageUnitType::UnitType2025,
            10,
        );
    }

    // ----------------------------------------------------------------------------------------
    // KEY_ADD / KEY_REMOVE engine integration tests (NEYN-10618)
    //
    // Devnet routes V16 unconditionally (per NEYN-10625), so feature gating doesn't trip up the
    // default `setup()` path. Tests pin custody addresses to real `PrivateKeySigner`s so the
    // EIP-712 recovery checks inside `merge_key_add` / `merge_key_remove` get exercised end-to-
    // end. Failures land as silent merge errors (BlockEngine swallows them via `if let Ok(...)`
    // in `replay_snapchain_txn`), so failure tests assert on absence-of-event + trie-omission
    // rather than a specific error variant; specific-variant coverage lives at the unit level
    // in `gasless_key_merge_test.rs`.
    // ----------------------------------------------------------------------------------------
    mod key_add_remove_tests {
        use super::*;
        use crate::core::util::calculate_message_hash;
        use crate::proto::{self, message_data::Body, MessageType};
        use crate::storage::store::account::{
            get_active_key, get_gasless_key_owner_fid, get_gasless_key_record, get_last_used_at,
            get_user_nonce, ActiveKey,
        };
        use crate::storage::store::block_engine::BlockEngine;
        use alloy_signer_local::PrivateKeySigner;
        use ed25519_dalek::{Signer, SigningKey};
        use prost::Message;

        const REQUEST_FID: u64 = FID_FOR_TEST + 100;
        const STORAGE_UNITS: u32 = 1000;

        fn address_bytes(signer: &PrivateKeySigner) -> Vec<u8> {
            signer.address().as_slice().to_vec()
        }

        /// Re-signs the envelope after the body has been mutated post-factory. Keeps the
        /// envelope hash + Ed25519 signature consistent so static validation passes and we can
        /// observe merge-time rejection.
        fn re_sign_envelope(mut msg: proto::Message, signer: &SigningKey) -> proto::Message {
            let data = msg.data.as_ref().expect("message has data").clone();
            let bytes = data.encode_to_vec();
            let hash = calculate_message_hash(&bytes);
            msg.signature = signer.sign(&hash).to_bytes().to_vec();
            msg.hash = hash;
            msg
        }

        /// Convenience wrapper that registers an FID with a real Ethereum custody address (for
        /// EIP-712 recovery) plus the supplied Ed25519 signer (used for active-key validation
        /// of non-gasless messages, e.g. casts later in this suite).
        fn register_user_eth(
            fid: u64,
            custody: &PrivateKeySigner,
            signer: SigningKey,
            engine: &mut BlockEngine,
        ) {
            register_user(fid, signer, address_bytes(custody), STORAGE_UNITS, engine);
        }

        /// Drops an on-chain `SIGNER_ADD` event for `(fid, gasless_key)` so a subsequent gasless
        /// KEY_ADD trips the on-chain-collision branch in `merge_key_add`.
        fn add_onchain_signer(engine: &mut BlockEngine, fid: u64, signer: &SigningKey) {
            let event = events_factory::create_signer_event(
                fid,
                signer.clone(),
                proto::SignerEventType::Add,
                None,
                None,
            );
            commit_event(engine, &event);
        }

        fn build_key_add(
            fid_custody: &PrivateKeySigner,
            app_custody: &PrivateKeySigner,
            envelope: &SigningKey,
            scopes: Vec<MessageType>,
            ttl: u32,
            nonce: u32,
        ) -> proto::Message {
            let now = messages_factory::farcaster_time();
            messages_factory::keys::create_key_add(
                FID_FOR_TEST,
                fid_custody,
                REQUEST_FID,
                app_custody,
                envelope,
                scopes,
                ttl,
                nonce,
                now + 1_000_000, // deadline well past block timestamp
                Some(now),
            )
        }

        // -- Happy path ------------------------------------------------------------------------

        #[tokio::test]
        async fn test_key_add_message_merged() {
            let (mut block_engine, _temp_dir) = setup();
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let envelope = signers::generate_signer();
            let envelope_pubkey = envelope.verifying_key().to_bytes();

            register_user_eth(
                FID_FOR_TEST,
                &fid_custody,
                default_signer(),
                &mut block_engine,
            );
            register_user_eth(
                REQUEST_FID,
                &app_custody,
                signers::generate_signer(),
                &mut block_engine,
            );

            let key_add = build_key_add(
                &fid_custody,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd],
                3600,
                1,
            );
            let block = commit_message(&mut block_engine, &key_add, Validity::Valid);

            assert_eq!(block.events.len(), 1);
            assert_merge_message_event(&block.events[0], &key_add);

            // State assertions hit the same store readers used by the active-key validation path.
            let txn = crate::storage::db::RocksDbTransactionBatch::new();
            let db = block_engine.stores().db.clone();
            let record = get_gasless_key_record(&db, &txn, FID_FOR_TEST, &envelope_pubkey)
                .unwrap()
                .expect("gasless key record persisted");
            assert_eq!(record.request_fid, REQUEST_FID);
            assert_eq!(
                get_gasless_key_owner_fid(&db, &txn, &envelope_pubkey).unwrap(),
                Some(FID_FOR_TEST),
            );
            assert_eq!(
                get_last_used_at(&db, &txn, FID_FOR_TEST, &envelope_pubkey).unwrap(),
                Some(key_add.data.as_ref().unwrap().timestamp),
            );
            assert_eq!(get_user_nonce(&db, &txn, FID_FOR_TEST).unwrap(), Some(1));
        }

        // -- Failure paths ---------------------------------------------------------------------

        #[tokio::test]
        async fn test_key_add_rejects_expired_deadline() {
            let (mut block_engine, _temp_dir) = setup();
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let envelope = signers::generate_signer();

            register_user_eth(
                FID_FOR_TEST,
                &fid_custody,
                default_signer(),
                &mut block_engine,
            );
            register_user_eth(
                REQUEST_FID,
                &app_custody,
                signers::generate_signer(),
                &mut block_engine,
            );

            // deadline strictly less than message timestamp → SignedKeyRequestExpired at merge.
            let now = messages_factory::farcaster_time();
            let key_add = messages_factory::keys::create_key_add(
                FID_FOR_TEST,
                &fid_custody,
                REQUEST_FID,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd],
                3600,
                1,
                now - 1,
                Some(now),
            );
            let block = commit_message(&mut block_engine, &key_add, Validity::Invalid);
            assert!(
                block.events.iter().all(|e| !matches!(
                    e.data.as_ref().unwrap().r#type(),
                    proto::BlockEventType::MergeMessage
                )),
                "expired KEY_ADD must not emit a MergeMessage event",
            );
        }

        #[tokio::test]
        async fn test_key_add_rejects_malformed_metadata() {
            let (mut block_engine, _temp_dir) = setup();
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let envelope = signers::generate_signer();

            register_user_eth(
                FID_FOR_TEST,
                &fid_custody,
                default_signer(),
                &mut block_engine,
            );
            register_user_eth(
                REQUEST_FID,
                &app_custody,
                signers::generate_signer(),
                &mut block_engine,
            );

            let mut key_add = build_key_add(
                &fid_custody,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd],
                3600,
                1,
            );
            // Truncate metadata so abi_decode fails at verify_signed_key_request_metadata.
            if let Some(Body::KeyAddBody(body)) = key_add.data.as_mut().unwrap().body.as_mut() {
                body.metadata = vec![0xde, 0xad];
            }
            let key_add = re_sign_envelope(key_add, &envelope);

            let block = commit_message(&mut block_engine, &key_add, Validity::Invalid);
            assert!(block.events.iter().all(|e| !matches!(
                e.data.as_ref().unwrap().r#type(),
                proto::BlockEventType::MergeMessage
            )));
        }

        #[tokio::test]
        async fn test_key_add_rejects_unregistered_request_fid() {
            let (mut block_engine, _temp_dir) = setup();
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let envelope = signers::generate_signer();

            register_user_eth(
                FID_FOR_TEST,
                &fid_custody,
                default_signer(),
                &mut block_engine,
            );
            // Intentionally do NOT register REQUEST_FID — get_id_register_event_by_fid returns
            // None inside merge_key_add → InvalidSignedKeyRequest.

            let key_add = build_key_add(
                &fid_custody,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd],
                3600,
                1,
            );
            let block = commit_message(&mut block_engine, &key_add, Validity::Invalid);
            assert!(block.events.iter().all(|e| !matches!(
                e.data.as_ref().unwrap().r#type(),
                proto::BlockEventType::MergeMessage
            )));
        }

        #[tokio::test]
        async fn test_key_add_rejects_request_signer_custody_mismatch() {
            let (mut block_engine, _temp_dir) = setup();
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let other_custody = PrivateKeySigner::random();
            let envelope = signers::generate_signer();

            register_user_eth(
                FID_FOR_TEST,
                &fid_custody,
                default_signer(),
                &mut block_engine,
            );
            // Register REQUEST_FID with a custody DIFFERENT from app_custody so the
            // request_fid_custody.to vs verified.request_signer comparison fails.
            register_user_eth(
                REQUEST_FID,
                &other_custody,
                signers::generate_signer(),
                &mut block_engine,
            );

            let key_add = build_key_add(
                &fid_custody,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd],
                3600,
                1,
            );
            let block = commit_message(&mut block_engine, &key_add, Validity::Invalid);
            assert!(block.events.iter().all(|e| !matches!(
                e.data.as_ref().unwrap().r#type(),
                proto::BlockEventType::MergeMessage
            )));
        }

        #[tokio::test]
        async fn test_key_add_rejects_invalid_custody_signature() {
            let (mut block_engine, _temp_dir) = setup();
            let real_custody = PrivateKeySigner::random();
            let wrong_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let envelope = signers::generate_signer();

            // FID_FOR_TEST is registered with `real_custody`'s address, but the inner KeyAdd
            // EIP-712 signature is produced by `wrong_custody`. Custody recovery in
            // merge_key_add returns wrong_custody.address(), which mismatches real_custody.address().
            register_user_eth(
                FID_FOR_TEST,
                &real_custody,
                default_signer(),
                &mut block_engine,
            );
            register_user_eth(
                REQUEST_FID,
                &app_custody,
                signers::generate_signer(),
                &mut block_engine,
            );

            let key_add = build_key_add(
                &wrong_custody,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd],
                3600,
                1,
            );
            let block = commit_message(&mut block_engine, &key_add, Validity::Invalid);
            assert!(block.events.iter().all(|e| !matches!(
                e.data.as_ref().unwrap().r#type(),
                proto::BlockEventType::MergeMessage
            )));
        }

        #[tokio::test]
        async fn test_key_add_rejects_replay_same_nonce() {
            let (mut block_engine, _temp_dir) = setup();
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let envelope = signers::generate_signer();

            register_user_eth(
                FID_FOR_TEST,
                &fid_custody,
                default_signer(),
                &mut block_engine,
            );
            register_user_eth(
                REQUEST_FID,
                &app_custody,
                signers::generate_signer(),
                &mut block_engine,
            );

            // Pin distinct timestamps so the two messages have distinct envelope hashes (and
            // therefore distinct trie keys). Both still claim nonce=1, so the second is
            // rejected at merge time by check_and_set_user_nonce.
            let now = messages_factory::farcaster_time();
            let first = messages_factory::keys::create_key_add(
                FID_FOR_TEST,
                &fid_custody,
                REQUEST_FID,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd],
                3600,
                1,
                now + 1_000_000,
                Some(now),
            );
            commit_message(&mut block_engine, &first, Validity::Valid);

            let replay = messages_factory::keys::create_key_add(
                FID_FOR_TEST,
                &fid_custody,
                REQUEST_FID,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd],
                3600,
                1,
                now + 1_000_000,
                Some(now + 1),
            );
            let block = commit_message(&mut block_engine, &replay, Validity::Invalid);
            // No second MergeMessage event for the replay.
            let merge_events: Vec<_> = block
                .events
                .iter()
                .filter(|e| {
                    matches!(
                        e.data.as_ref().unwrap().r#type(),
                        proto::BlockEventType::MergeMessage
                    )
                })
                .collect();
            assert!(merge_events.is_empty());
        }

        #[tokio::test]
        async fn test_key_add_rejects_lower_nonce() {
            let (mut block_engine, _temp_dir) = setup();
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let envelope = signers::generate_signer();

            register_user_eth(
                FID_FOR_TEST,
                &fid_custody,
                default_signer(),
                &mut block_engine,
            );
            register_user_eth(
                REQUEST_FID,
                &app_custody,
                signers::generate_signer(),
                &mut block_engine,
            );

            let first = build_key_add(
                &fid_custody,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd],
                3600,
                5,
            );
            commit_message(&mut block_engine, &first, Validity::Valid);

            // Different envelope key but same FID — the user-nonce store is per-fid, not per-key,
            // so a fresh KEY_ADD on the same fid with nonce <= 5 must be rejected.
            let envelope2 = signers::generate_signer();
            let backwards = build_key_add(
                &fid_custody,
                &app_custody,
                &envelope2,
                vec![MessageType::CastAdd],
                3600,
                3,
            );
            let block = commit_message(&mut block_engine, &backwards, Validity::Invalid);
            assert!(block.events.iter().all(|e| !matches!(
                e.data.as_ref().unwrap().r#type(),
                proto::BlockEventType::MergeMessage
            )));
        }

        #[tokio::test]
        async fn test_key_add_rejects_already_registered_onchain() {
            let (mut block_engine, _temp_dir) = setup();
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let envelope = signers::generate_signer();

            register_user_eth(
                FID_FOR_TEST,
                &fid_custody,
                default_signer(),
                &mut block_engine,
            );
            register_user_eth(
                REQUEST_FID,
                &app_custody,
                signers::generate_signer(),
                &mut block_engine,
            );
            // Pre-merge an on-chain SIGNER_ADD for the same (fid, key) the gasless KEY_ADD will
            // target. merge_key_add's get_active_signer check fires before any state writes.
            add_onchain_signer(&mut block_engine, FID_FOR_TEST, &envelope);

            let key_add = build_key_add(
                &fid_custody,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd],
                3600,
                1,
            );
            let block = commit_message(&mut block_engine, &key_add, Validity::Invalid);
            assert!(block.events.iter().all(|e| !matches!(
                e.data.as_ref().unwrap().r#type(),
                proto::BlockEventType::MergeMessage
            )));
        }

        // -- Concurrency-within-commit ---------------------------------------------------------

        #[tokio::test]
        async fn test_key_add_two_in_same_commit_same_fid_nonce_cas() {
            let (mut block_engine, _temp_dir) = setup();
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let env_a = signers::generate_signer();
            let env_b = signers::generate_signer();

            register_user_eth(
                FID_FOR_TEST,
                &fid_custody,
                default_signer(),
                &mut block_engine,
            );
            register_user_eth(
                REQUEST_FID,
                &app_custody,
                signers::generate_signer(),
                &mut block_engine,
            );

            // Both messages claim nonce=1 on the same FID. Within one batch,
            // check_and_set_user_nonce sees the staged write from `a` when validating `b`.
            let a = build_key_add(
                &fid_custody,
                &app_custody,
                &env_a,
                vec![MessageType::CastAdd],
                3600,
                1,
            );
            let b = build_key_add(
                &fid_custody,
                &app_custody,
                &env_b,
                vec![MessageType::CastAdd],
                3600,
                1,
            );
            let block = commit_messages(
                &mut block_engine,
                vec![(&a, Validity::Valid), (&b, Validity::Invalid)],
            );
            // Exactly one MergeMessage event (for `a`), no event for `b`.
            let merge_events: Vec<_> = block
                .events
                .iter()
                .filter(|e| {
                    matches!(
                        e.data.as_ref().unwrap().r#type(),
                        proto::BlockEventType::MergeMessage
                    )
                })
                .collect();
            assert_eq!(merge_events.len(), 1);
            assert_merge_message_event(merge_events[0], &a);
        }

        #[tokio::test]
        async fn test_key_add_two_in_same_commit_same_key_resubmission() {
            let (mut block_engine, _temp_dir) = setup();
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let envelope = signers::generate_signer();

            register_user_eth(
                FID_FOR_TEST,
                &fid_custody,
                default_signer(),
                &mut block_engine,
            );
            register_user_eth(
                REQUEST_FID,
                &app_custody,
                signers::generate_signer(),
                &mut block_engine,
            );

            // Same FID, same envelope key, same app — second is a same-app resubmission and
            // must hit the upsert path (gasless_key_merge.rs:180-199), with deleted_messages
            // carrying the first message.
            let first = build_key_add(
                &fid_custody,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd],
                3600,
                1,
            );
            let second = build_key_add(
                &fid_custody,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd, MessageType::ReactionAdd],
                7200,
                2,
            );
            // The upsert deletes `first`'s trie key when `second` merges, so `first` ends up
            // not-in-trie post-batch even though it merged successfully. The helper's per-msg
            // `in_trie` check therefore needs `Validity::Invalid` for `first`.
            let block = commit_messages(
                &mut block_engine,
                vec![(&first, Validity::Invalid), (&second, Validity::Valid)],
            );
            // Two MergeMessage events: first for `first`, second carrying `first` in
            // deleted_messages.
            let merge_events: Vec<_> = block
                .events
                .iter()
                .filter(|e| {
                    matches!(
                        e.data.as_ref().unwrap().r#type(),
                        proto::BlockEventType::MergeMessage
                    )
                })
                .collect();
            assert_eq!(merge_events.len(), 2);
            assert_merge_message_event(merge_events[0], &first);
            assert_merge_message_event(merge_events[1], &second);

            let txn = crate::storage::db::RocksDbTransactionBatch::new();
            let db = block_engine.stores().db.clone();
            let envelope_pubkey = envelope.verifying_key().to_bytes();
            let record = get_gasless_key_record(&db, &txn, FID_FOR_TEST, &envelope_pubkey)
                .unwrap()
                .expect("upsert leaves a single record under the latest message");
            // Stored record reflects the resubmission's broader scopes / longer ttl.
            let body = match record
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
            assert_eq!(body.ttl, 7200);
            assert_eq!(
                body.scopes,
                vec![MessageType::CastAdd as i32, MessageType::ReactionAdd as i32],
            );
        }

        // -- KEY_REMOVE custody path -----------------------------------------------------------

        #[tokio::test]
        async fn test_key_remove_custody_signature_merged() {
            let (mut block_engine, _temp_dir) = setup();
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let envelope = signers::generate_signer();
            let envelope_pubkey: [u8; 32] = envelope.verifying_key().to_bytes();

            register_user_eth(
                FID_FOR_TEST,
                &fid_custody,
                default_signer(),
                &mut block_engine,
            );
            register_user_eth(
                REQUEST_FID,
                &app_custody,
                signers::generate_signer(),
                &mut block_engine,
            );

            let key_add = build_key_add(
                &fid_custody,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd],
                3600,
                1,
            );
            commit_message(&mut block_engine, &key_add, Validity::Valid);

            // KEY_REMOVE custody. Envelope can be any active key on the FID — use the FID's
            // registered Ed25519 default signer (skips active-signer lookup via the bypass).
            let now = messages_factory::farcaster_time();
            let remove = messages_factory::keys::create_key_remove_custody(
                FID_FOR_TEST,
                &fid_custody,
                &default_signer(),
                &envelope_pubkey,
                2,
                now + 1_000_000,
                Some(now + 1),
            );
            let block = commit_message(&mut block_engine, &remove, Validity::Valid);
            let merge_events: Vec<_> = block
                .events
                .iter()
                .filter(|e| {
                    matches!(
                        e.data.as_ref().unwrap().r#type(),
                        proto::BlockEventType::MergeMessage
                    )
                })
                .collect();
            assert_eq!(merge_events.len(), 1);
            assert_merge_message_event(merge_events[0], &remove);

            // State assertions: record + owner + last_used_at all cleared.
            let txn = crate::storage::db::RocksDbTransactionBatch::new();
            let db = block_engine.stores().db.clone();
            assert!(
                get_gasless_key_record(&db, &txn, FID_FOR_TEST, &envelope_pubkey)
                    .unwrap()
                    .is_none()
            );
            assert!(get_gasless_key_owner_fid(&db, &txn, &envelope_pubkey)
                .unwrap()
                .is_none());
            assert_eq!(
                get_last_used_at(&db, &txn, FID_FOR_TEST, &envelope_pubkey).unwrap(),
                None,
            );
            // User nonce advanced to 2.
            assert_eq!(get_user_nonce(&db, &txn, FID_FOR_TEST).unwrap(), Some(2));
        }

        #[tokio::test]
        async fn test_key_remove_custody_rejects_invalid_signature() {
            let (mut block_engine, _temp_dir) = setup();
            let fid_custody = PrivateKeySigner::random();
            let wrong_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let envelope = signers::generate_signer();
            let envelope_pubkey: [u8; 32] = envelope.verifying_key().to_bytes();

            register_user_eth(
                FID_FOR_TEST,
                &fid_custody,
                default_signer(),
                &mut block_engine,
            );
            register_user_eth(
                REQUEST_FID,
                &app_custody,
                signers::generate_signer(),
                &mut block_engine,
            );
            let key_add = build_key_add(
                &fid_custody,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd],
                3600,
                1,
            );
            commit_message(&mut block_engine, &key_add, Validity::Valid);

            // Sign with wrong_custody so the EIP-712 recovery returns a different address than
            // FID_FOR_TEST's registered custody.
            let now = messages_factory::farcaster_time();
            let remove = messages_factory::keys::create_key_remove_custody(
                FID_FOR_TEST,
                &wrong_custody,
                &default_signer(),
                &envelope_pubkey,
                2,
                now + 1_000_000,
                Some(now + 1),
            );
            let block = commit_message(&mut block_engine, &remove, Validity::Invalid);
            // The KEY_ADD's MergeMessage already lives in a prior block; this block should have
            // no new MergeMessage event for the rejected KEY_REMOVE.
            let merge_events: Vec<_> = block
                .events
                .iter()
                .filter(|e| {
                    matches!(
                        e.data.as_ref().unwrap().r#type(),
                        proto::BlockEventType::MergeMessage
                    )
                })
                .collect();
            assert!(merge_events.is_empty());

            // Record still present.
            let txn = crate::storage::db::RocksDbTransactionBatch::new();
            let db = block_engine.stores().db.clone();
            assert!(
                get_gasless_key_record(&db, &txn, FID_FOR_TEST, &envelope_pubkey)
                    .unwrap()
                    .is_some()
            );
        }

        #[tokio::test]
        async fn test_key_remove_rejects_unknown_key() {
            let (mut block_engine, _temp_dir) = setup();
            let fid_custody = PrivateKeySigner::random();

            register_user_eth(
                FID_FOR_TEST,
                &fid_custody,
                default_signer(),
                &mut block_engine,
            );

            // No KEY_ADD landed — the gasless record for `unknown_key` does not exist.
            let unknown_key: [u8; 32] = signers::generate_signer().verifying_key().to_bytes();
            let now = messages_factory::farcaster_time();
            let remove = messages_factory::keys::create_key_remove_custody(
                FID_FOR_TEST,
                &fid_custody,
                &default_signer(),
                &unknown_key,
                1,
                now + 1_000_000,
                Some(now),
            );
            let block = commit_message(&mut block_engine, &remove, Validity::Invalid);
            assert!(block.events.iter().all(|e| !matches!(
                e.data.as_ref().unwrap().r#type(),
                proto::BlockEventType::MergeMessage
            )));
        }

        // -- KEY_REMOVE self-revoke path -------------------------------------------------------

        #[tokio::test]
        async fn test_key_remove_self_revoke_merged() {
            let (mut block_engine, _temp_dir) = setup();
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let envelope = signers::generate_signer();
            let envelope_pubkey: [u8; 32] = envelope.verifying_key().to_bytes();

            register_user_eth(
                FID_FOR_TEST,
                &fid_custody,
                default_signer(),
                &mut block_engine,
            );
            register_user_eth(
                REQUEST_FID,
                &app_custody,
                signers::generate_signer(),
                &mut block_engine,
            );
            let key_add = build_key_add(
                &fid_custody,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd],
                3600,
                1,
            );
            commit_message(&mut block_engine, &key_add, Validity::Valid);

            // Self-revoke: envelope IS the key being revoked. Body.signature is empty; envelope
            // Ed25519 sig + body.key match are sufficient. Nonce lives in the app namespace
            // for the verified request_fid (REQUEST_FID).
            let now = messages_factory::farcaster_time();
            let remove = messages_factory::keys::create_key_remove_self_revoke(
                FID_FOR_TEST,
                &envelope,
                1,
                now + 1_000_000,
                Some(now + 1),
            );
            let block = commit_message(&mut block_engine, &remove, Validity::Valid);
            let merge_events: Vec<_> = block
                .events
                .iter()
                .filter(|e| {
                    matches!(
                        e.data.as_ref().unwrap().r#type(),
                        proto::BlockEventType::MergeMessage
                    )
                })
                .collect();
            assert_eq!(merge_events.len(), 1);
            assert_merge_message_event(merge_events[0], &remove);

            let txn = crate::storage::db::RocksDbTransactionBatch::new();
            let db = block_engine.stores().db.clone();
            assert!(
                get_gasless_key_record(&db, &txn, FID_FOR_TEST, &envelope_pubkey)
                    .unwrap()
                    .is_none()
            );
        }

        #[tokio::test]
        async fn test_key_remove_self_revoke_rejects_envelope_signer_mismatch() {
            let (mut block_engine, _temp_dir) = setup();
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let envelope = signers::generate_signer();
            let envelope_pubkey: [u8; 32] = envelope.verifying_key().to_bytes();

            register_user_eth(
                FID_FOR_TEST,
                &fid_custody,
                default_signer(),
                &mut block_engine,
            );
            register_user_eth(
                REQUEST_FID,
                &app_custody,
                signers::generate_signer(),
                &mut block_engine,
            );
            let key_add = build_key_add(
                &fid_custody,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd],
                3600,
                1,
            );
            commit_message(&mut block_engine, &key_add, Validity::Valid);

            // Self-revoke targeting the gasless key, but the envelope is signed by a *different*
            // Ed25519 key (the FID's default signer). msg.signer != body.key → InvalidSignature.
            let now = messages_factory::farcaster_time();
            let other_signer = default_signer();
            let mut remove = messages_factory::keys::create_key_remove_self_revoke(
                FID_FOR_TEST,
                &other_signer,
                1,
                now + 1_000_000,
                Some(now + 1),
            );
            // Override body.key to point at the gasless key, then re-sign envelope.
            if let Some(Body::KeyRemoveBody(body)) = remove.data.as_mut().unwrap().body.as_mut() {
                body.key = envelope_pubkey.to_vec();
            }
            let remove = re_sign_envelope(remove, &other_signer);

            let block = commit_message(&mut block_engine, &remove, Validity::Invalid);
            assert!(block.events.iter().all(|e| !matches!(
                e.data.as_ref().unwrap().r#type(),
                proto::BlockEventType::MergeMessage
            )));

            // Gasless record must still be in place.
            let txn = crate::storage::db::RocksDbTransactionBatch::new();
            let db = block_engine.stores().db.clone();
            assert!(
                get_gasless_key_record(&db, &txn, FID_FOR_TEST, &envelope_pubkey)
                    .unwrap()
                    .is_some()
            );
        }

        // -- Active-key lookup smoke test (precondition for downstream cast tests) -------------

        #[tokio::test]
        async fn test_active_key_lookup_returns_gasless_record() {
            // Once a KEY_ADD merges, get_active_key (used by validate_user_message for non-key
            // messages) must surface the gasless registration. This is the precondition for the
            // ShardEngine cast-by-gasless-key tests; if it ever broke, the whole feature would
            // be dead code.
            let (mut block_engine, _temp_dir) = setup();
            let fid_custody = PrivateKeySigner::random();
            let app_custody = PrivateKeySigner::random();
            let envelope = signers::generate_signer();
            let envelope_pubkey = envelope.verifying_key().to_bytes();

            register_user_eth(
                FID_FOR_TEST,
                &fid_custody,
                default_signer(),
                &mut block_engine,
            );
            register_user_eth(
                REQUEST_FID,
                &app_custody,
                signers::generate_signer(),
                &mut block_engine,
            );
            let key_add = build_key_add(
                &fid_custody,
                &app_custody,
                &envelope,
                vec![MessageType::CastAdd],
                3600,
                1,
            );
            commit_message(&mut block_engine, &key_add, Validity::Valid);

            let txn = crate::storage::db::RocksDbTransactionBatch::new();
            let db = block_engine.stores().db.clone();
            let active = get_active_key(
                &block_engine.stores().onchain_event_store,
                &db,
                &txn,
                FID_FOR_TEST,
                &envelope_pubkey,
            )
            .unwrap()
            .expect("active key lookup must surface the gasless record");
            match active {
                ActiveKey::Gasless { ttl_seconds, .. } => assert_eq!(ttl_seconds, 3600),
                ActiveKey::OnChain => {
                    panic!("expected ActiveKey::Gasless, got OnChain")
                }
            }
        }
    }
}
