#[cfg(test)]
mod tests {
    use crate::proto::{
        hub_event, message_data::Body, CastingMode, ChannelMemberAction, ChannelMemberBody,
        ChannelModerateAction, ChannelModerateBody, ChannelPinBody, ChannelUpdateBody, HubEvent,
        MembershipMode, Message, MessageType,
    };
    use crate::storage::constants::RootPrefix;
    use crate::storage::db::{RocksDB, RocksDbTransactionBatch};
    use crate::storage::store::account::{
        ChannelMemberState, ChannelMemberStore, ChannelMemberStoreDef, ChannelModerateStore,
        ChannelModerateStoreDef, ChannelModerationState, ChannelPinStore, ChannelPinStoreDef,
        ChannelUpdateStore, ChannelUpdateStoreDef, Store, StoreEventHandler,
        CHANNEL_MEMBER_SLOT_CAP, CHANNEL_MODERATE_SLOT_CAP,
    };
    use crate::storage::trie::merkle_trie::{Context, MerkleTrie, TrieKey};
    use crate::utils::factory::messages_factory;
    use std::sync::Arc;
    use tempfile::TempDir;

    struct TestStores {
        update: Store<ChannelUpdateStoreDef>,
        member: Store<ChannelMemberStoreDef>,
        pin: Store<ChannelPinStoreDef>,
        moderate: Store<ChannelModerateStoreDef>,
        db: Arc<RocksDB>,
        _temp_dir: TempDir,
    }

    fn test_stores() -> TestStores {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db = Arc::new(RocksDB::new(
            temp_dir.path().join("channel.db").to_str().unwrap(),
        ));
        db.open().unwrap();
        let event_handler = StoreEventHandler::new();
        TestStores {
            update: ChannelUpdateStore::new(db.clone(), event_handler.clone(), 100),
            member: ChannelMemberStore::new(db.clone(), event_handler.clone(), 100),
            pin: ChannelPinStore::new(db.clone(), event_handler.clone(), 100),
            moderate: ChannelModerateStore::new(db.clone(), event_handler, 100),
            db,
            _temp_dir: temp_dir,
        }
    }

    fn channel_id(byte: u8) -> Vec<u8> {
        vec![byte; 32]
    }

    fn cast_hash(index: u32) -> Vec<u8> {
        let mut hash = vec![0xCC; 20];
        hash[..4].copy_from_slice(&index.to_be_bytes());
        hash
    }

    fn update_message(
        author: u64,
        channel_id: Vec<u8>,
        timestamp: u32,
        name: Option<&str>,
        description: Option<&str>,
        casting_mode: Option<CastingMode>,
        membership_mode: Option<MembershipMode>,
    ) -> Message {
        messages_factory::create_message_with_data(
            author,
            MessageType::ChannelUpdate,
            Body::ChannelUpdateBody(ChannelUpdateBody {
                channel_id,
                name: name.map(str::to_string),
                description: description.map(str::to_string),
                casting_mode: casting_mode.map(i32::from),
                membership_mode: membership_mode.map(i32::from),
                ..Default::default()
            }),
            Some(timestamp),
            None,
        )
    }

    fn member_message(
        author: u64,
        channel_id: Vec<u8>,
        target_fid: u64,
        action: ChannelMemberAction,
        timestamp: u32,
    ) -> Message {
        messages_factory::create_message_with_data(
            author,
            MessageType::ChannelMember,
            Body::ChannelMemberBody(ChannelMemberBody {
                channel_id,
                fid: target_fid,
                action: action as i32,
            }),
            Some(timestamp),
            None,
        )
    }

    fn pin_message(
        author: u64,
        channel_id: Vec<u8>,
        cast_hash: Vec<u8>,
        timestamp: u32,
    ) -> Message {
        messages_factory::create_message_with_data(
            author,
            MessageType::ChannelPin,
            Body::ChannelPinBody(ChannelPinBody {
                channel_id,
                cast_hash,
            }),
            Some(timestamp),
            None,
        )
    }

    fn moderate_message(
        author: u64,
        channel_id: Vec<u8>,
        cast_hash: Vec<u8>,
        action: ChannelModerateAction,
        timestamp: u32,
    ) -> Message {
        messages_factory::create_message_with_data(
            author,
            MessageType::ChannelModerate,
            Body::ChannelModerateBody(ChannelModerateBody {
                channel_id,
                cast_hash,
                action: action as i32,
            }),
            Some(timestamp),
            None,
        )
    }

    fn deleted_messages(event: &HubEvent) -> &[Message] {
        match event.body.as_ref().unwrap() {
            hub_event::Body::MergeMessageBody(body) => &body.deleted_messages,
            other => panic!("expected MergeMessage, got {other:?}"),
        }
    }

    fn expected_member_state(action: ChannelMemberAction) -> ChannelMemberState {
        match action {
            ChannelMemberAction::AddMember | ChannelMemberAction::RemoveModerator => {
                ChannelMemberState::Member
            }
            ChannelMemberAction::AddModerator => ChannelMemberState::Moderator,
            ChannelMemberAction::RemoveMember | ChannelMemberAction::Unban => {
                ChannelMemberState::Removed
            }
            ChannelMemberAction::Ban => ChannelMemberState::Banned,
            ChannelMemberAction::None => panic!("none is not a state transition"),
        }
    }

    #[test]
    fn channel_slot_key_layouts_are_pinned() {
        let channel = channel_id(0x44);
        let moderated_cast = cast_hash(7);
        let mut update = vec![RootPrefix::Channel as u8, 1];
        update.extend_from_slice(&channel);
        assert_eq!(ChannelUpdateStore::slot_key(&channel), update);

        let mut member = vec![RootPrefix::Channel as u8, 2];
        member.extend_from_slice(&channel);
        member.extend_from_slice(&123u32.to_be_bytes());
        assert_eq!(ChannelMemberStore::slot_key(&channel, 123).unwrap(), member);

        let mut pin = vec![RootPrefix::Channel as u8, 3];
        pin.extend_from_slice(&channel);
        assert_eq!(ChannelPinStore::slot_key(&channel), pin);

        let mut moderate = vec![RootPrefix::Channel as u8, 4];
        moderate.extend_from_slice(&channel);
        moderate.extend_from_slice(&moderated_cast);
        assert_eq!(
            ChannelModerateStore::slot_key(&channel, &moderated_cast),
            moderate
        );
    }

    #[test]
    fn cross_author_same_block_supersede_keeps_store_index_trie_and_event_in_agreement() {
        let stores = test_stores();
        let mut trie = MerkleTrie::new().unwrap();
        trie.initialize(&stores.db).unwrap();
        let ctx = Context::new();
        let channel = channel_id(1);
        let incumbent = update_message(
            100,
            channel.clone(),
            500,
            Some("incumbent"),
            None,
            None,
            None,
        );
        let replacement = update_message(
            200,
            channel.clone(),
            100,
            Some("replacement"),
            None,
            None,
            None,
        );

        let mut txn = RocksDbTransactionBatch::new();
        let incumbent_event =
            ChannelUpdateStore::merge(&stores.update, &incumbent, &mut txn).unwrap();
        trie.update_for_event(&ctx, &stores.db, &incumbent_event, &mut txn)
            .unwrap();
        let replacement_event =
            ChannelUpdateStore::merge(&stores.update, &replacement, &mut txn).unwrap();
        trie.update_for_event(&ctx, &stores.db, &replacement_event, &mut txn)
            .unwrap();
        assert_eq!(deleted_messages(&replacement_event), &[incumbent.clone()]);
        stores.db.commit(txn).unwrap();

        assert!(stores.update.get_add(&incumbent, None).unwrap().is_none());
        assert_eq!(
            stores.update.get_add(&replacement, None).unwrap(),
            Some(replacement.clone())
        );
        let slot_pointer = stores
            .db
            .get(&ChannelUpdateStore::slot_key(&channel))
            .unwrap()
            .unwrap();
        assert_eq!(slot_pointer.len(), 28);
        assert_eq!(
            &slot_pointer[..4],
            &(replacement.fid() as u32).to_be_bytes()
        );
        assert_eq!(
            ChannelUpdateStore::get_channel_update(&stores.update, &channel, None)
                .unwrap()
                .unwrap()
                .body
                .name,
            Some("replacement".to_string())
        );

        let incumbent_key = TrieKey::for_message(&incumbent).pop().unwrap();
        let replacement_key = TrieKey::for_message(&replacement).pop().unwrap();
        assert!(!trie.exists(&ctx, &stores.db, &incumbent_key).unwrap());
        assert!(trie.exists(&ctx, &stores.db, &replacement_key).unwrap());
    }

    #[test]
    fn every_slot_type_uses_merge_order_not_timestamp_and_supersedes_both_authors() {
        let stores = test_stores();
        let channel = channel_id(2);
        let mut txn = RocksDbTransactionBatch::new();

        let update_a = update_message(1, channel.clone(), 900, Some("a"), None, None, None);
        let update_b = update_message(2, channel.clone(), 100, Some("b"), None, None, None);
        let update_a_again =
            update_message(1, channel.clone(), 50, Some("a-again"), None, None, None);
        ChannelUpdateStore::merge(&stores.update, &update_a, &mut txn).unwrap();
        assert_eq!(
            deleted_messages(
                &ChannelUpdateStore::merge(&stores.update, &update_b, &mut txn).unwrap()
            ),
            &[update_a]
        );
        assert_eq!(
            deleted_messages(
                &ChannelUpdateStore::merge(&stores.update, &update_a_again, &mut txn).unwrap()
            ),
            &[update_b]
        );

        let member_a = member_message(1, channel.clone(), 99, ChannelMemberAction::AddMember, 900);
        let member_b = member_message(2, channel.clone(), 99, ChannelMemberAction::Ban, 100);
        let member_a_again = member_message(1, channel.clone(), 99, ChannelMemberAction::Unban, 50);
        ChannelMemberStore::merge(&stores.member, &member_a, &mut txn).unwrap();
        assert_eq!(
            deleted_messages(
                &ChannelMemberStore::merge(&stores.member, &member_b, &mut txn).unwrap()
            ),
            &[member_a]
        );
        assert_eq!(
            deleted_messages(
                &ChannelMemberStore::merge(&stores.member, &member_a_again, &mut txn).unwrap()
            ),
            &[member_b]
        );

        let pin_a = pin_message(1, channel.clone(), cast_hash(1), 900);
        let pin_b = pin_message(2, channel.clone(), cast_hash(2), 100);
        let pin_a_again = pin_message(1, channel.clone(), Vec::new(), 50);
        ChannelPinStore::merge(&stores.pin, &pin_a, &mut txn).unwrap();
        assert_eq!(
            deleted_messages(&ChannelPinStore::merge(&stores.pin, &pin_b, &mut txn).unwrap()),
            &[pin_a]
        );
        assert_eq!(
            deleted_messages(&ChannelPinStore::merge(&stores.pin, &pin_a_again, &mut txn).unwrap()),
            &[pin_b]
        );

        let moderated_cast = cast_hash(3);
        let moderate_a = moderate_message(
            1,
            channel.clone(),
            moderated_cast.clone(),
            ChannelModerateAction::Hide,
            900,
        );
        let moderate_b = moderate_message(
            2,
            channel.clone(),
            moderated_cast.clone(),
            ChannelModerateAction::Unhide,
            100,
        );
        let moderate_a_again =
            moderate_message(1, channel, moderated_cast, ChannelModerateAction::Hide, 50);
        ChannelModerateStore::merge(&stores.moderate, &moderate_a, &mut txn).unwrap();
        assert_eq!(
            deleted_messages(
                &ChannelModerateStore::merge(&stores.moderate, &moderate_b, &mut txn).unwrap()
            ),
            &[moderate_a]
        );
        assert_eq!(
            deleted_messages(
                &ChannelModerateStore::merge(&stores.moderate, &moderate_a_again, &mut txn)
                    .unwrap()
            ),
            &[moderate_b]
        );
    }

    #[test]
    fn channel_update_is_whole_replace_and_unset_modes_fold_restrictively() {
        let stores = test_stores();
        let channel = channel_id(3);
        let mut txn = RocksDbTransactionBatch::new();
        ChannelUpdateStore::merge(
            &stores.update,
            &update_message(
                1,
                channel.clone(),
                1,
                Some("first"),
                Some("must not survive"),
                Some(CastingMode::Everyone),
                Some(MembershipMode::Open),
            ),
            &mut txn,
        )
        .unwrap();
        ChannelUpdateStore::merge(
            &stores.update,
            &update_message(1, channel.clone(), 2, Some("second"), None, None, None),
            &mut txn,
        )
        .unwrap();

        let state = ChannelUpdateStore::get_channel_update(&stores.update, &channel, Some(&txn))
            .unwrap()
            .unwrap();
        assert_eq!(state.body.name, Some("second".to_string()));
        assert_eq!(state.body.description, None);
        assert_eq!(state.body.casting_mode, None);
        assert_eq!(state.body.membership_mode, None);
        assert_eq!(state.casting_mode, CastingMode::MembersOnly);
        assert_eq!(state.membership_mode, MembershipMode::Approval);
    }

    #[test]
    fn member_state_machine_applies_all_six_actions_from_every_prior_state() {
        let stores = test_stores();
        let channel = channel_id(4);
        let actions = [
            ChannelMemberAction::AddMember,
            ChannelMemberAction::AddModerator,
            ChannelMemberAction::RemoveModerator,
            ChannelMemberAction::RemoveMember,
            ChannelMemberAction::Ban,
            ChannelMemberAction::Unban,
        ];
        let prior_actions = [
            None,
            Some(ChannelMemberAction::AddMember),
            Some(ChannelMemberAction::AddModerator),
            Some(ChannelMemberAction::RemoveMember),
            Some(ChannelMemberAction::Ban),
        ];
        let mut txn = RocksDbTransactionBatch::new();
        let mut target_fid = 1_000;
        let mut timestamp = 1;
        for prior_action in prior_actions {
            for action in actions {
                if let Some(prior_action) = prior_action {
                    ChannelMemberStore::merge(
                        &stores.member,
                        &member_message(1, channel.clone(), target_fid, prior_action, timestamp),
                        &mut txn,
                    )
                    .unwrap();
                    timestamp += 1;
                }
                ChannelMemberStore::merge(
                    &stores.member,
                    &member_message(1, channel.clone(), target_fid, action, timestamp),
                    &mut txn,
                )
                .unwrap();
                timestamp += 1;
                assert_eq!(
                    ChannelMemberStore::member_state(
                        &stores.member,
                        &channel,
                        target_fid,
                        Some(&txn),
                    )
                    .unwrap(),
                    Some(expected_member_state(action)),
                    "prior={prior_action:?}, action={action:?}"
                );
                target_fid += 1;
            }
        }
    }

    #[test]
    fn member_churn_is_row_neutral_and_live_moderator_count_tracks_transitions() {
        let stores = test_stores();
        let channel = channel_id(5);
        let target_fid = 55;
        let mut txn = RocksDbTransactionBatch::new();
        let sequence = [
            (ChannelMemberAction::AddMember, 0),
            (ChannelMemberAction::AddModerator, 1),
            (ChannelMemberAction::RemoveModerator, 0),
            (ChannelMemberAction::AddModerator, 1),
            (ChannelMemberAction::RemoveMember, 0),
            (ChannelMemberAction::AddMember, 0),
            (ChannelMemberAction::Ban, 0),
            (ChannelMemberAction::Unban, 0),
            (ChannelMemberAction::AddModerator, 1),
        ];
        for (index, (action, expected_moderators)) in sequence.into_iter().enumerate() {
            ChannelMemberStore::merge(
                &stores.member,
                &member_message(1, channel.clone(), target_fid, action, index as u32 + 1),
                &mut txn,
            )
            .unwrap();
            assert_eq!(
                ChannelMemberStore::slot_count(&stores.member, &channel, Some(&txn)).unwrap(),
                1
            );
            assert_eq!(
                ChannelMemberStore::live_moderator_count(&stores.member, &channel, Some(&txn),)
                    .unwrap(),
                expected_moderators
            );
        }
    }

    #[test]
    fn pin_unpin_and_moderate_hide_unhide_fold_in_place() {
        let stores = test_stores();
        let channel = channel_id(6);
        let moderated_cast = cast_hash(9);
        let mut txn = RocksDbTransactionBatch::new();
        ChannelPinStore::merge(
            &stores.pin,
            &pin_message(1, channel.clone(), cast_hash(8), 1),
            &mut txn,
        )
        .unwrap();
        ChannelPinStore::merge(
            &stores.pin,
            &pin_message(1, channel.clone(), Vec::new(), 2),
            &mut txn,
        )
        .unwrap();
        assert_eq!(
            ChannelPinStore::get_channel_pin(&stores.pin, &channel, Some(&txn))
                .unwrap()
                .unwrap()
                .cast_hash,
            Vec::<u8>::new()
        );

        ChannelModerateStore::merge(
            &stores.moderate,
            &moderate_message(
                1,
                channel.clone(),
                moderated_cast.clone(),
                ChannelModerateAction::Hide,
                3,
            ),
            &mut txn,
        )
        .unwrap();
        assert_eq!(
            ChannelModerateStore::moderation_state(
                &stores.moderate,
                &channel,
                &moderated_cast,
                Some(&txn),
            )
            .unwrap(),
            Some(ChannelModerationState::Hidden)
        );
        ChannelModerateStore::merge(
            &stores.moderate,
            &moderate_message(
                1,
                channel.clone(),
                moderated_cast.clone(),
                ChannelModerateAction::Unhide,
                4,
            ),
            &mut txn,
        )
        .unwrap();
        assert_eq!(
            ChannelModerateStore::moderation_state(
                &stores.moderate,
                &channel,
                &moderated_cast,
                Some(&txn),
            )
            .unwrap(),
            Some(ChannelModerationState::Visible)
        );
        assert_eq!(
            ChannelModerateStore::slot_count(&stores.moderate, &channel, Some(&txn)).unwrap(),
            1
        );
    }

    #[test]
    fn member_cap_boundary_checks_every_action_on_mint_and_overwrite() {
        let stores = test_stores();
        let channel = channel_id(7);
        let actions = [
            ChannelMemberAction::AddMember,
            ChannelMemberAction::AddModerator,
            ChannelMemberAction::RemoveModerator,
            ChannelMemberAction::RemoveMember,
            ChannelMemberAction::Ban,
            ChannelMemberAction::Unban,
        ];
        let mut txn = RocksDbTransactionBatch::new();
        for target_fid in 1..=u64::from(CHANNEL_MEMBER_SLOT_CAP) {
            ChannelMemberStore::merge(
                &stores.member,
                &member_message(
                    1,
                    channel.clone(),
                    target_fid,
                    ChannelMemberAction::AddMember,
                    target_fid as u32,
                ),
                &mut txn,
            )
            .unwrap();
        }
        assert_eq!(
            ChannelMemberStore::slot_count(&stores.member, &channel, Some(&txn)).unwrap(),
            CHANNEL_MEMBER_SLOT_CAP
        );

        for (index, action) in actions.into_iter().enumerate() {
            let before = txn.batch.clone();
            let mint = member_message(
                1,
                channel.clone(),
                u64::from(CHANNEL_MEMBER_SLOT_CAP) + index as u64 + 1,
                action,
                CHANNEL_MEMBER_SLOT_CAP + index as u32 + 1,
            );
            let error = ChannelMemberStore::merge(&stores.member, &mint, &mut txn).unwrap_err();
            assert_eq!(error.message, "channel slot cap exceeded");
            assert_eq!(txn.batch, before, "failed {action:?} mint changed txn");
        }

        for (index, action) in actions.into_iter().enumerate() {
            ChannelMemberStore::merge(
                &stores.member,
                &member_message(
                    2,
                    channel.clone(),
                    1,
                    action,
                    CHANNEL_MEMBER_SLOT_CAP + 100 + index as u32,
                ),
                &mut txn,
            )
            .unwrap();
            assert_eq!(
                ChannelMemberStore::slot_count(&stores.member, &channel, Some(&txn)).unwrap(),
                CHANNEL_MEMBER_SLOT_CAP
            );
        }
    }

    #[test]
    fn moderate_cap_boundary_checks_both_actions_on_mint_and_overwrite() {
        let stores = test_stores();
        let channel = channel_id(8);
        let actions = [ChannelModerateAction::Hide, ChannelModerateAction::Unhide];
        let mut txn = RocksDbTransactionBatch::new();
        for index in 0..CHANNEL_MODERATE_SLOT_CAP {
            if index > 0 && index % 8_000 == 0 {
                stores
                    .moderate
                    .event_handler()
                    .set_current_height(u64::from(index / 8_000));
            }
            ChannelModerateStore::merge(
                &stores.moderate,
                &moderate_message(
                    1,
                    channel.clone(),
                    cast_hash(index),
                    ChannelModerateAction::Hide,
                    index + 1,
                ),
                &mut txn,
            )
            .unwrap();
        }
        assert_eq!(
            ChannelModerateStore::slot_count(&stores.moderate, &channel, Some(&txn)).unwrap(),
            CHANNEL_MODERATE_SLOT_CAP
        );

        for (index, action) in actions.into_iter().enumerate() {
            let before = txn.batch.clone();
            let mint = moderate_message(
                1,
                channel.clone(),
                cast_hash(CHANNEL_MODERATE_SLOT_CAP + index as u32),
                action,
                CHANNEL_MODERATE_SLOT_CAP + index as u32 + 1,
            );
            let error = ChannelModerateStore::merge(&stores.moderate, &mint, &mut txn).unwrap_err();
            assert_eq!(error.message, "channel slot cap exceeded");
            assert_eq!(txn.batch, before, "failed {action:?} mint changed txn");
        }

        for (index, action) in actions.into_iter().enumerate() {
            ChannelModerateStore::merge(
                &stores.moderate,
                &moderate_message(
                    2,
                    channel.clone(),
                    cast_hash(0),
                    action,
                    CHANNEL_MODERATE_SLOT_CAP + 100 + index as u32,
                ),
                &mut txn,
            )
            .unwrap();
            assert_eq!(
                ChannelModerateStore::slot_count(&stores.moderate, &channel, Some(&txn)).unwrap(),
                CHANNEL_MODERATE_SLOT_CAP
            );
        }
    }

    #[test]
    fn generic_timestamp_lww_merge_is_rejected_for_channel_slots() {
        let stores = test_stores();
        let message = update_message(1, channel_id(9), 1, Some("x"), None, None, None);
        let error = stores
            .update
            .merge(
                &message,
                &mut RocksDbTransactionBatch::new(),
                &crate::storage::store::test_helper::default_merge_ctx(),
            )
            .unwrap_err();
        assert_eq!(error.message, "slot store requires consensus-order merge");
    }
}
