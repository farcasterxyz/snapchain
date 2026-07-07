#[cfg(test)]
mod tests {
    use crate::core::util::FarcasterTime;
    use crate::proto::{
        ChannelRegisterEventType, FarcasterNetwork, IdRegisterEventType, SignerEventType,
        StorageUnitType, TierType,
    };
    use crate::storage::db::RocksDbTransactionBatch;
    use crate::storage::db::{self, PageOptions};
    use crate::storage::store::account::{
        block_event_store, delete_channel_key_by_owner_address, get_channel_keys_by_owner_address,
        put_channel_key_by_owner_address, OnchainEventStore, StorageSlot, StoreEventHandler,
    };
    use crate::storage::store::test_helper::default_custody_address;
    use crate::utils::factory::{self, events_factory, signers};
    use crate::version::version::EngineVersion;
    use alloy_primitives::keccak256;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn store() -> (OnchainEventStore, TempDir) {
        let dir = tempfile::TempDir::new().unwrap();
        let db_path = dir.path().join("a.db");

        let db = db::RocksDB::new(db_path.to_str().unwrap());
        db.open().unwrap();

        (
            OnchainEventStore::new(Arc::new(db), StoreEventHandler::new()),
            dir,
        )
    }

    fn channel_label(channel_key: &str) -> Vec<u8> {
        keccak256(channel_key.as_bytes()).to_vec()
    }

    fn owner(byte: u8) -> Vec<u8> {
        vec![byte; 20]
    }

    fn merge_channel_events(store: &OnchainEventStore, events: Vec<crate::proto::OnChainEvent>) {
        let mut txn = RocksDbTransactionBatch::new();
        for event in events {
            store.merge_onchain_event(event, &mut txn).unwrap();
        }
        store.db.commit(txn).unwrap();
    }

    // This test deliberately pins historical grant dates, because a unit's cohort (legacy / 2024 /
    // 2025) is decided by its grant timestamp -- a `now`-dated grant can only ever be the newest
    // cohort. That makes `is_active()` assertions unusable here: it compares `invalidate_at` against
    // wall-clock `SystemTime::now()`, so any "still active" assertion on a pinned grant rots once
    // real time passes grant + validity. Assert `invalidate_at` instead, which pins the same
    // behaviour deterministically. The one `is_active()` below asserting *false* is safe: that grant
    // is backdated far enough to be expired under every multiplier, now and forever.
    #[test]
    fn test_storage_slot_from_rent_event() {
        let one_year_in_seconds = 365 * 24 * 60 * 60;

        // Legacy units: 3 years pre-extension (V17), 4 years post-extension (V18).
        let expired_legacy_rent_event = factory::events_factory::create_rent_event(
            10,
            1,
            StorageUnitType::UnitTypeLegacy,
            true,
            FarcasterNetwork::Mainnet,
        );
        let slot = StorageSlot::from_event(
            &expired_legacy_rent_event,
            FarcasterNetwork::Mainnet,
            EngineVersion::V17,
        )
        .unwrap();
        assert_eq!(slot.is_active(), false);
        assert_eq!(slot.units_for(StorageUnitType::UnitTypeLegacy), 1);
        assert_eq!(slot.units_for(StorageUnitType::UnitType2024), 0);
        assert_eq!(slot.units_for(StorageUnitType::UnitType2025), 0);
        assert_eq!(
            slot.invalidate_at,
            expired_legacy_rent_event.block_timestamp as u32 + one_year_in_seconds * 3
        );
        let slot = StorageSlot::from_event(
            &expired_legacy_rent_event,
            FarcasterNetwork::Mainnet,
            EngineVersion::V18,
        )
        .unwrap();
        assert_eq!(slot.units_for(StorageUnitType::UnitTypeLegacy), 1);
        assert_eq!(
            slot.invalidate_at,
            expired_legacy_rent_event.block_timestamp as u32 + one_year_in_seconds * 4
        );

        let valid_legacy_rent_event = factory::events_factory::create_rent_event(
            10,
            5,
            StorageUnitType::UnitTypeLegacy,
            false,
            FarcasterNetwork::Mainnet,
        );
        let slot = StorageSlot::from_event(
            &valid_legacy_rent_event,
            FarcasterNetwork::Mainnet,
            EngineVersion::V17,
        )
        .unwrap();
        assert_eq!(slot.units_for(StorageUnitType::UnitTypeLegacy), 5);
        assert_eq!(
            slot.invalidate_at,
            valid_legacy_rent_event.block_timestamp as u32 + one_year_in_seconds * 3
        );
        let slot = StorageSlot::from_event(
            &valid_legacy_rent_event,
            FarcasterNetwork::Mainnet,
            EngineVersion::V18,
        )
        .unwrap();
        assert_eq!(slot.units_for(StorageUnitType::UnitTypeLegacy), 5);
        assert_eq!(
            slot.invalidate_at,
            valid_legacy_rent_event.block_timestamp as u32 + one_year_in_seconds * 4
        );

        // 2024 units: 2 years pre-extension, 3 years post-extension.
        let valid_2024_rent_event = factory::events_factory::create_rent_event(
            10,
            9,
            StorageUnitType::UnitType2024,
            false,
            FarcasterNetwork::Mainnet,
        );
        let slot = StorageSlot::from_event(
            &valid_2024_rent_event,
            FarcasterNetwork::Mainnet,
            EngineVersion::V17,
        )
        .unwrap();
        assert_eq!(slot.units_for(StorageUnitType::UnitType2024), 9);
        assert_eq!(
            slot.invalidate_at,
            valid_2024_rent_event.block_timestamp as u32 + one_year_in_seconds * 2
        );
        let slot = StorageSlot::from_event(
            &valid_2024_rent_event,
            FarcasterNetwork::Mainnet,
            EngineVersion::V18,
        )
        .unwrap();
        assert_eq!(slot.units_for(StorageUnitType::UnitType2024), 9);
        assert_eq!(
            slot.invalidate_at,
            valid_2024_rent_event.block_timestamp as u32 + one_year_in_seconds * 3
        );

        // 2025 cohort (rented before the 2025 cutoff): 1 year pre-extension, 2 years post-extension.
        let september_first_2025_timestamp = 1756710000;
        let valid_2025_rent_event = factory::events_factory::create_rent_event_with_timestamp(
            11,
            3,
            september_first_2025_timestamp,
        );
        let slot = StorageSlot::from_event(
            &valid_2025_rent_event,
            FarcasterNetwork::Mainnet,
            EngineVersion::V17,
        )
        .unwrap();
        // No `is_active()` assertion here: it compares against wall-clock `SystemTime::now()`, and
        // this grant is deliberately pinned to a historical date so it classifies as 2025-cohort
        // (a `now`-dated grant would fall in the new-rental branch instead). Under V17 the unit only
        // gets 1 year, so `is_active()` here goes false one year after the pinned date and rots the
        // test. `invalidate_at` below covers the same ground deterministically, and the V18 block
        // that follows already asserts only that.
        assert_eq!(slot.units_for(StorageUnitType::UnitType2025), 3);
        assert_eq!(
            slot.invalidate_at,
            valid_2025_rent_event.block_timestamp as u32 + one_year_in_seconds
        );
        let slot = StorageSlot::from_event(
            &valid_2025_rent_event,
            FarcasterNetwork::Mainnet,
            EngineVersion::V18,
        )
        .unwrap();
        assert_eq!(slot.units_for(StorageUnitType::UnitType2025), 3);
        assert_eq!(
            slot.invalidate_at,
            valid_2025_rent_event.block_timestamp as u32 + one_year_in_seconds * 2
        );

        // New rentals (rented at/after the 2025 cutoff) keep the standard 1-year validity, even
        // after the extension activates at V18.
        let new_rental_timestamp =
            StorageSlot::unit_type_2025_cutoff(FarcasterNetwork::Mainnet) + 1;
        let new_rental_rent_event =
            factory::events_factory::create_rent_event_with_timestamp(13, 4, new_rental_timestamp);
        let slot = StorageSlot::from_event(
            &new_rental_rent_event,
            FarcasterNetwork::Mainnet,
            EngineVersion::V17,
        )
        .unwrap();
        assert_eq!(slot.units_for(StorageUnitType::UnitType2025), 4);
        assert_eq!(
            slot.invalidate_at,
            new_rental_rent_event.block_timestamp as u32 + one_year_in_seconds
        );
        let slot = StorageSlot::from_event(
            &new_rental_rent_event,
            FarcasterNetwork::Mainnet,
            EngineVersion::V18,
        )
        .unwrap();
        assert_eq!(slot.units_for(StorageUnitType::UnitType2025), 4);
        assert_eq!(
            slot.invalidate_at,
            new_rental_rent_event.block_timestamp as u32 + one_year_in_seconds
        );
    }

    // The 2025-cohort cutoff must line up exactly with the StorageExpiryExtension2026 (V18)
    // activation timestamp for each network: the existing cohort is everything rented before the
    // extension goes live. These constants live in two different modules, so this test guards
    // against them silently drifting apart.
    #[test]
    fn test_unit_type_2025_cutoff_matches_v18_activation() {
        for network in [FarcasterNetwork::Mainnet, FarcasterNetwork::Testnet] {
            let cutoff = StorageSlot::unit_type_2025_cutoff(network) as u64;

            // V18 (and thus the extension) is not yet active one second before the cutoff...
            assert_eq!(
                EngineVersion::version_for(&FarcasterTime::from_unix_seconds(cutoff - 1), network,),
                EngineVersion::V17,
                "{:?}: extension should be inactive just before the 2025 cutoff",
                network,
            );
            // ...and is active exactly at the cutoff.
            assert_eq!(
                EngineVersion::version_for(&FarcasterTime::from_unix_seconds(cutoff), network),
                EngineVersion::V18,
                "{:?}: extension should activate exactly at the 2025 cutoff",
                network,
            );
        }
    }

    #[test]
    fn test_storage_slot_merge() {
        let current_time = factory::time::current_timestamp();
        // When merging two active slots, the units should be added together
        let active_slot = StorageSlot::new(1, 2, 0, current_time + 1);
        let mut active_slot2 = StorageSlot::new(2, 1, 0, current_time + 10);

        assert_eq!(active_slot.is_active(), true);
        assert_eq!(active_slot2.is_active(), true);

        assert_eq!(active_slot2.merge(&active_slot), true);

        assert_eq!(active_slot2.units_for(StorageUnitType::UnitTypeLegacy), 3);
        assert_eq!(active_slot2.units_for(StorageUnitType::UnitType2024), 3);
        assert_eq!(active_slot2.units_for(StorageUnitType::UnitType2025), 0);
        assert_eq!(active_slot2.invalidate_at, current_time + 1); // min of both timestamps
        assert_eq!(active_slot2.is_active(), true);

        // When merging an active slot with an inactive slot, the inactive slot should be ignored
        let inactive_slot = StorageSlot::new(1, 2, 0, current_time - 10);
        let mut active_slot3 = StorageSlot::new(2, 1, 0, current_time + 10);

        assert_eq!(inactive_slot.is_active(), false);

        let mut inactive_slot_merged = inactive_slot.clone();

        // When merging an active slot into inactive slot, the inactive slot is replaced
        assert_eq!(inactive_slot_merged.merge(&active_slot3), true);
        assert_eq!(
            inactive_slot_merged.units_for(StorageUnitType::UnitTypeLegacy),
            2
        );
        assert_eq!(
            inactive_slot_merged.units_for(StorageUnitType::UnitType2024),
            1
        );
        assert_eq!(
            inactive_slot_merged.units_for(StorageUnitType::UnitType2025),
            0
        );
        assert_eq!(inactive_slot_merged.invalidate_at, current_time + 10);
        assert_eq!(inactive_slot_merged.is_active(), true);

        // When merging an inactive slot into active slot, the active slot is unchanged
        assert_eq!(active_slot3.merge(&inactive_slot), false);
        assert_eq!(active_slot3.units_for(StorageUnitType::UnitTypeLegacy), 2);
        assert_eq!(active_slot3.units_for(StorageUnitType::UnitType2024), 1);
        assert_eq!(active_slot3.units_for(StorageUnitType::UnitType2025), 0);
        assert_eq!(active_slot3.invalidate_at, current_time + 10);
        assert_eq!(active_slot3.is_active(), true);
    }

    #[test]
    fn test_storage_slot_when_no_units() {
        let (store, _dir) = store();

        let storage_slot = store
            .get_storage_slot_for_fid(
                10,
                FarcasterNetwork::Mainnet,
                EngineVersion::V17,
                &[],
                &StorageSlot::new(0, 0, 0, 0),
                &StorageSlot::new(0, 0, 0, 0),
            )
            .unwrap();
        assert_eq!(storage_slot.is_active(), false);
        assert_eq!(storage_slot.units_for(StorageUnitType::UnitTypeLegacy), 0);
        assert_eq!(storage_slot.units_for(StorageUnitType::UnitType2024), 0);
        assert_eq!(storage_slot.units_for(StorageUnitType::UnitType2025), 0);
        assert_eq!(storage_slot.invalidate_at, 0);
    }

    #[test]
    fn test_storage_slot_with_mix_of_units() {
        let (store, _dir) = store();

        let expired_legacy_rent_event = factory::events_factory::create_rent_event(
            10,
            1,
            StorageUnitType::UnitTypeLegacy,
            true,
            FarcasterNetwork::Mainnet,
        );

        // NOTE: `another_*` events below share their fid, type and timestamp with the event above
        // them, so `create_rent_event` derives the same block number base for both. The only thing
        // separating their primary keys (type + fid + block_number + log_index) is the factory's
        // `rand % 1000` block-number jitter, which collides 1-in-1000 and fails the merge below with
        // `DuplicateOnchainEvent`. Pin distinct log indices so the keys differ deterministically.
        let mut valid_legacy_rent_event = factory::events_factory::create_rent_event(
            10,
            5,
            StorageUnitType::UnitTypeLegacy,
            false,
            FarcasterNetwork::Mainnet,
        );
        valid_legacy_rent_event.log_index = 0;
        let mut another_valid_legacy_rent_event = factory::events_factory::create_rent_event(
            10,
            7,
            StorageUnitType::UnitTypeLegacy,
            false,
            FarcasterNetwork::Mainnet,
        );
        another_valid_legacy_rent_event.log_index = 1;
        let mut valid_2024_rent_event = factory::events_factory::create_rent_event(
            10,
            9,
            StorageUnitType::UnitType2024,
            false,
            FarcasterNetwork::Mainnet,
        );
        valid_2024_rent_event.log_index = 0;
        let mut another_valid_2024_rent_event = factory::events_factory::create_rent_event(
            10,
            11,
            StorageUnitType::UnitType2024,
            false,
            FarcasterNetwork::Mainnet,
        );
        another_valid_2024_rent_event.log_index = 1;

        let valid_rent_event_different_fid = factory::events_factory::create_rent_event(
            11,
            13,
            StorageUnitType::UnitType2024,
            false,
            FarcasterNetwork::Mainnet,
        );

        // Rolling grant date, not a fixed one. This slot only has to be *active* here (the merge
        // below skips expired slots outright, so an aged-out grant would zero the unit assertions
        // further down, not just the `is_active()` one). A `now`-dated grant still classifies as
        // UnitType2025, which is all this test needs; the cohort-vs-new-rental boundary is covered
        // deterministically by `test_storage_slot_from_rent_event`.
        let valid_2025_rent_event = factory::events_factory::create_rent_event_with_timestamp(
            12,
            1,
            factory::time::current_timestamp(),
        );

        let mut txn = RocksDbTransactionBatch::new();
        for event in vec![
            expired_legacy_rent_event,
            valid_legacy_rent_event,
            another_valid_legacy_rent_event,
            valid_2024_rent_event,
            another_valid_2024_rent_event,
            valid_rent_event_different_fid,
            valid_2025_rent_event,
        ] {
            store.merge_onchain_event(event, &mut txn).unwrap();
        }
        store.db.commit(txn).unwrap();

        let storage_slot_different_fid = store
            .get_storage_slot_for_fid(
                11,
                FarcasterNetwork::Mainnet,
                EngineVersion::V17,
                &[],
                &StorageSlot::new(0, 0, 0, 0),
                &StorageSlot::new(0, 0, 0, 0),
            )
            .unwrap();
        assert_eq!(storage_slot_different_fid.is_active(), true);
        assert_eq!(
            storage_slot_different_fid.units_for(StorageUnitType::UnitTypeLegacy),
            0
        );
        assert_eq!(
            storage_slot_different_fid.units_for(StorageUnitType::UnitType2024),
            13
        );
        assert_eq!(
            storage_slot_different_fid.units_for(StorageUnitType::UnitType2025),
            0
        );

        let storage_slot = store
            .get_storage_slot_for_fid(
                10,
                FarcasterNetwork::Mainnet,
                EngineVersion::V17,
                &[],
                &StorageSlot::new(0, 0, 0, 0),
                &StorageSlot::new(0, 0, 0, 0),
            )
            .unwrap();
        assert_eq!(storage_slot.is_active(), true);
        assert_eq!(storage_slot.units_for(StorageUnitType::UnitTypeLegacy), 12); // 5 + 7
        assert_eq!(storage_slot.units_for(StorageUnitType::UnitType2024), 20); // 9 + 11

        let storage_slot_2025 = store
            .get_storage_slot_for_fid(
                12,
                FarcasterNetwork::Mainnet,
                EngineVersion::V17,
                &[],
                &StorageSlot::new(0, 0, 0, 0),
                &StorageSlot::new(0, 0, 0, 0),
            )
            .unwrap();
        assert_eq!(storage_slot_2025.is_active(), true);
        assert_eq!(
            storage_slot_2025.units_for(StorageUnitType::UnitTypeLegacy),
            0
        );
        assert_eq!(
            storage_slot_2025.units_for(StorageUnitType::UnitType2024),
            0
        );
        assert_eq!(
            storage_slot_2025.units_for(StorageUnitType::UnitType2025),
            1
        );
    }

    #[test]
    fn test_pro_user_expiration() {
        let (store, _dir) = store();
        let day_in_secs = 24 * 60 * 60;
        let start_time = FarcasterTime::new(100);

        let pro_user_event1 = factory::events_factory::create_pro_user_event(
            10,
            1,
            Some(start_time.to_unix_seconds() as u32),
        );
        let pro_user_event2 = factory::events_factory::create_pro_user_event(
            10,
            1,
            Some((pro_user_event1.block_timestamp + day_in_secs - 10) as u32),
        );
        let pro_user_event3 = factory::events_factory::create_pro_user_event(
            10,
            1,
            Some((pro_user_event1.block_timestamp + (2 * day_in_secs) + 10) as u32),
        );

        let mut txn = RocksDbTransactionBatch::new();
        for event in [
            pro_user_event1.clone(),
            pro_user_event2.clone(),
            pro_user_event3.clone(),
        ] {
            store.merge_onchain_event(event, &mut txn).unwrap();
        }
        store.db.commit(txn).unwrap();

        assert!(!store
            .is_tier_subscription_active_at(TierType::Pro, 10, &start_time.decr_by(1))
            .unwrap());
        assert!(store
            .is_tier_subscription_active_at(TierType::Pro, 10, &start_time)
            .unwrap());
        assert!(store
            .is_tier_subscription_active_at(TierType::Pro, 10, &start_time.incr_by(2 * day_in_secs))
            .unwrap());
        assert!(!store
            .is_tier_subscription_active_at(
                TierType::Pro,
                10,
                &start_time.incr_by((2 * day_in_secs) + 1)
            )
            .unwrap());
        assert!(!store
            .is_tier_subscription_active_at(
                TierType::Pro,
                10,
                &FarcasterTime::from_unix_seconds(pro_user_event3.block_timestamp).decr_by(1)
            )
            .unwrap());
        assert!(store
            .is_tier_subscription_active_at(
                TierType::Pro,
                10,
                &FarcasterTime::from_unix_seconds(pro_user_event3.block_timestamp)
            )
            .unwrap());
        assert!(store
            .is_tier_subscription_active_at(
                TierType::Pro,
                10,
                &FarcasterTime::from_unix_seconds(pro_user_event3.block_timestamp)
                    .incr_by(day_in_secs)
            )
            .unwrap());
        assert!(!store
            .is_tier_subscription_active_at(
                TierType::Pro,
                10,
                &FarcasterTime::from_unix_seconds(pro_user_event3.block_timestamp)
                    .incr_by(day_in_secs + 1)
            )
            .unwrap());
    }

    #[test]
    fn test_get_all_onchain_events() {
        let (store, _dir) = store();

        // Create different types of onchain events
        let id_register_event1 = factory::events_factory::create_id_register_event(
            1,
            IdRegisterEventType::Register,
            default_custody_address(),
            None,
        );
        let id_register_event2 = factory::events_factory::create_id_register_event(
            2,
            IdRegisterEventType::Register,
            default_custody_address(),
            None,
        );
        let signer_event1 = factory::events_factory::create_signer_event(
            1,
            signers::generate_signer(),
            SignerEventType::Add,
            None,
            None,
        );
        let signer_event2 = factory::events_factory::create_signer_event(
            3,
            signers::generate_signer(),
            SignerEventType::Add,
            None,
            None,
        );
        let rent_event1 = factory::events_factory::create_rent_event(
            1,
            5,
            StorageUnitType::UnitTypeLegacy,
            false,
            FarcasterNetwork::Mainnet,
        );
        let rent_event2 = factory::events_factory::create_rent_event(
            2,
            10,
            StorageUnitType::UnitType2024,
            false,
            FarcasterNetwork::Mainnet,
        );

        // Merge all events into the store
        let mut txn = RocksDbTransactionBatch::new();
        let events = vec![
            id_register_event1.clone(),
            id_register_event2.clone(),
            signer_event1.clone(),
            signer_event2.clone(),
            rent_event1.clone(),
            rent_event2.clone(),
        ];
        for event in &events {
            store.merge_onchain_event(event.clone(), &mut txn).unwrap();
        }
        // Put in some data with a prefix higher than the onchain event store prefix and make sure it's not included.
        block_event_store::put_block_event(&events_factory::create_heartbeat_event(1), &mut txn)
            .unwrap();
        store.db.commit(txn).unwrap();

        // Test pagination with page size limit
        let page_options = PageOptions {
            page_size: Some(3),
            page_token: None,
            reverse: false,
        };
        let page = store.get_all_onchain_events(&page_options).unwrap();
        assert!(page.next_page_token.is_some());
        assert_eq!(
            page.onchain_events,
            vec![
                signer_event1.clone(),
                signer_event2.clone(),
                id_register_event1.clone(),
            ]
        );

        // Get second page
        let page_options = PageOptions {
            page_size: Some(4),
            page_token: page.next_page_token,
            reverse: false,
        };
        let page = store.get_all_onchain_events(&page_options).unwrap();
        assert!(page.next_page_token.is_none());
        assert_eq!(
            page.onchain_events,
            vec![
                id_register_event2.clone(),
                rent_event1.clone(),
                rent_event2.clone()
            ]
        );

        // With exactly 6 events and page size 3, we should have exactly 2 pages
        // Test without page size limit (get all events)
        let page_options = PageOptions {
            page_size: None,
            page_token: None,
            reverse: false,
        };
        let page = store.get_all_onchain_events(&page_options).unwrap();
        assert!(page.next_page_token.is_none());
        assert_eq!(
            page.onchain_events,
            vec![
                signer_event1.clone(),
                signer_event2.clone(),
                id_register_event1.clone(),
                id_register_event2.clone(),
                rent_event1.clone(),
                rent_event2.clone(),
            ]
        );
    }

    #[test]
    fn test_channel_renew_overwrites_expiry() {
        let (store, _dir) = store();
        let label = channel_label("pets");
        let owner = owner(10);
        merge_channel_events(
            &store,
            vec![
                events_factory::create_channel_register_event(
                    "pets",
                    label.clone(),
                    owner.clone(),
                    100,
                    ChannelRegisterEventType::Register,
                    1,
                    1,
                ),
                events_factory::create_channel_register_event(
                    "pets",
                    label,
                    vec![],
                    250,
                    ChannelRegisterEventType::Renew,
                    2,
                    1,
                ),
            ],
        );

        let channel_owner = store.get_channel_owner("pets", None).unwrap().unwrap();
        assert_eq!(channel_owner.channel_key, "pets");
        assert_eq!(channel_owner.owner_address, owner);
        assert_eq!(channel_owner.expiry, 250);
    }

    #[test]
    fn test_channel_transfer_rebinds_owner_address() {
        let (store, _dir) = store();
        let label = channel_label("casts");
        let first_owner = owner(11);
        let next_owner = owner(12);
        merge_channel_events(
            &store,
            vec![
                events_factory::create_channel_register_event(
                    "casts",
                    label.clone(),
                    first_owner,
                    100,
                    ChannelRegisterEventType::Register,
                    1,
                    1,
                ),
                events_factory::create_channel_register_event(
                    "",
                    label,
                    next_owner.clone(),
                    0,
                    ChannelRegisterEventType::Transfer,
                    2,
                    1,
                ),
            ],
        );

        let channel_owner = store.get_channel_owner("casts", None).unwrap().unwrap();
        assert_eq!(channel_owner.channel_key, "casts");
        assert_eq!(channel_owner.owner_address, next_owner);
        assert_eq!(channel_owner.expiry, 100);
    }

    #[test]
    fn test_channel_reregistration_supersedes_prior_record() {
        let (store, _dir) = store();
        let label = channel_label("music");
        let old_owner = owner(13);
        let new_owner = owner(14);
        merge_channel_events(
            &store,
            vec![
                events_factory::create_channel_register_event(
                    "music",
                    label.clone(),
                    old_owner,
                    100,
                    ChannelRegisterEventType::Register,
                    1,
                    1,
                ),
                events_factory::create_channel_register_event(
                    "music",
                    label,
                    new_owner.clone(),
                    500,
                    ChannelRegisterEventType::Register,
                    10,
                    1,
                ),
            ],
        );

        let channel_owner = store.get_channel_owner("music", None).unwrap().unwrap();
        assert_eq!(channel_owner.channel_key, "music");
        assert_eq!(channel_owner.owner_address, new_owner);
        assert_eq!(channel_owner.expiry, 500);
    }

    #[test]
    fn test_channel_mint_transfer_does_not_clobber_register() {
        let (store, _dir) = store();
        let label = channel_label("frames");
        let owner = owner(15);
        merge_channel_events(
            &store,
            vec![
                events_factory::create_channel_register_event(
                    "frames",
                    label.clone(),
                    owner.clone(),
                    900,
                    ChannelRegisterEventType::Register,
                    1,
                    1,
                ),
                events_factory::create_channel_register_event(
                    "",
                    label,
                    owner.clone(),
                    0,
                    ChannelRegisterEventType::Transfer,
                    1,
                    2,
                ),
            ],
        );

        let channel_owner = store.get_channel_owner("frames", None).unwrap().unwrap();
        assert_eq!(channel_owner.owner_address, owner);
        assert_eq!(channel_owner.expiry, 900);
    }

    #[test]
    fn test_channel_mint_transfer_before_register_is_skipped_then_register_applies() {
        let (store, _dir) = store();
        let label = channel_label("early");
        let owner = owner(18);
        merge_channel_events(
            &store,
            vec![
                events_factory::create_channel_register_event(
                    "",
                    label.clone(),
                    owner.clone(),
                    0,
                    ChannelRegisterEventType::Transfer,
                    1,
                    1,
                ),
                events_factory::create_channel_register_event(
                    "early",
                    label,
                    owner.clone(),
                    700,
                    ChannelRegisterEventType::Register,
                    1,
                    2,
                ),
            ],
        );

        let channel_owner = store.get_channel_owner("early", None).unwrap().unwrap();
        assert_eq!(channel_owner.owner_address, owner);
        assert_eq!(channel_owner.expiry, 700);
    }

    #[test]
    fn test_channel_transfer_unknown_label_skips_index_update() {
        let (store, _dir) = store();
        merge_channel_events(
            &store,
            vec![events_factory::create_channel_register_event(
                "",
                channel_label("unknown"),
                owner(16),
                0,
                ChannelRegisterEventType::Transfer,
                1,
                1,
            )],
        );

        assert!(store.get_channel_owner("unknown", None).unwrap().is_none());
    }

    #[test]
    fn test_channel_register_with_mismatched_label_skips_index_update() {
        let (store, _dir) = store();
        merge_channel_events(
            &store,
            vec![events_factory::create_channel_register_event(
                "bad-label",
                channel_label("different"),
                owner(19),
                100,
                ChannelRegisterEventType::Register,
                1,
                1,
            )],
        );

        assert!(store
            .get_channel_owner("bad-label", None)
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_channel_transfer_with_invalid_owner_address_skips_index_update() {
        let (store, _dir) = store();
        let label = channel_label("invalid-owner");
        let original_owner = owner(20);
        merge_channel_events(
            &store,
            vec![
                events_factory::create_channel_register_event(
                    "invalid-owner",
                    label.clone(),
                    original_owner.clone(),
                    100,
                    ChannelRegisterEventType::Register,
                    1,
                    1,
                ),
                events_factory::create_channel_register_event(
                    "",
                    label,
                    vec![1, 2, 3],
                    0,
                    ChannelRegisterEventType::Transfer,
                    2,
                    1,
                ),
            ],
        );

        let channel_owner = store
            .get_channel_owner("invalid-owner", None)
            .unwrap()
            .unwrap();
        assert_eq!(channel_owner.owner_address, original_owner);
        assert_eq!(channel_owner.expiry, 100);
    }

    #[test]
    fn test_channel_by_owner_address_moves_on_transfer_and_reregistration() {
        let (store, _dir) = store();
        let page_options = PageOptions {
            page_size: None,
            page_token: None,
            reverse: false,
        };
        let first_owner = owner(21);
        let second_owner = owner(22);
        let third_owner = owner(23);
        let label = channel_label("moves");

        merge_channel_events(
            &store,
            vec![
                events_factory::create_channel_register_event(
                    "moves",
                    label.clone(),
                    first_owner.clone(),
                    100,
                    ChannelRegisterEventType::Register,
                    1,
                    1,
                ),
                events_factory::create_channel_register_event(
                    "",
                    label.clone(),
                    second_owner.clone(),
                    0,
                    ChannelRegisterEventType::Transfer,
                    2,
                    1,
                ),
            ],
        );

        let (channel_keys, _) =
            get_channel_keys_by_owner_address(&store.db, &first_owner, &page_options).unwrap();
        assert!(channel_keys.is_empty());
        let (channel_keys, _) =
            get_channel_keys_by_owner_address(&store.db, &second_owner, &page_options).unwrap();
        assert_eq!(channel_keys, vec!["moves".to_string()]);

        merge_channel_events(
            &store,
            vec![events_factory::create_channel_register_event(
                "moves",
                label,
                third_owner.clone(),
                500,
                ChannelRegisterEventType::Register,
                10,
                1,
            )],
        );

        let (channel_keys, _) =
            get_channel_keys_by_owner_address(&store.db, &second_owner, &page_options).unwrap();
        assert!(channel_keys.is_empty());
        let (channel_keys, _) =
            get_channel_keys_by_owner_address(&store.db, &third_owner, &page_options).unwrap();
        assert_eq!(channel_keys, vec!["moves".to_string()]);
    }

    #[test]
    fn test_channel_by_owner_address_helpers_write_read_delete() {
        let (store, _dir) = store();
        let address = owner(17);
        let page_options = PageOptions {
            page_size: None,
            page_token: None,
            reverse: false,
        };

        let mut txn = RocksDbTransactionBatch::new();
        put_channel_key_by_owner_address(&mut txn, &address, "alpha").unwrap();
        put_channel_key_by_owner_address(&mut txn, &address, "beta").unwrap();
        store.db.commit(txn).unwrap();

        let (channel_keys, next_page_token) =
            get_channel_keys_by_owner_address(&store.db, &address, &page_options).unwrap();
        assert_eq!(channel_keys, vec!["alpha".to_string(), "beta".to_string()]);
        assert!(next_page_token.is_none());

        let mut txn = RocksDbTransactionBatch::new();
        delete_channel_key_by_owner_address(&mut txn, &address, "alpha").unwrap();
        store.db.commit(txn).unwrap();

        let (channel_keys, _) =
            get_channel_keys_by_owner_address(&store.db, &address, &page_options).unwrap();
        assert_eq!(channel_keys, vec!["beta".to_string()]);
    }

    #[test]
    fn test_channel_by_owner_address_helpers_reject_non_evm_addresses() {
        let (store, _dir) = store();
        let short_address = vec![1, 2, 3];
        let page_options = PageOptions {
            page_size: None,
            page_token: None,
            reverse: false,
        };

        let mut txn = RocksDbTransactionBatch::new();
        let err = put_channel_key_by_owner_address(&mut txn, &short_address, "alpha")
            .err()
            .unwrap();
        assert_eq!(
            err.to_string(),
            "bad_request.validation_failure/expected 20-byte EVM address, got 3"
        );
        assert!(
            get_channel_keys_by_owner_address(&store.db, &short_address, &page_options).is_err()
        );
    }
}
