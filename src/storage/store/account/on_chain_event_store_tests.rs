#[cfg(test)]
mod tests {
    use crate::core::util::FarcasterTime;
    use crate::proto::{StorageUnitType, TierType};
    use crate::storage::db;
    use crate::storage::db::RocksDbTransactionBatch;
    use crate::storage::store::account::{OnchainEventStore, StorageSlot, StoreEventHandler};
    use crate::utils::factory::{self};
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

    #[test]
    fn test_storage_slot_from_rent_event() {
        let one_year_in_seconds = 365 * 24 * 60 * 60;

        let expired_legacy_rent_event =
            factory::events_factory::create_rent_event(10, Some(1), None, true);
        let slot = StorageSlot::from_event(&expired_legacy_rent_event).unwrap();
        assert_eq!(slot.is_active(), false);
        assert_eq!(slot.units_for(StorageUnitType::UnitTypeLegacy), 1);
        assert_eq!(slot.units_for(StorageUnitType::UnitType2024), 0);
        assert_eq!(slot.units_for(StorageUnitType::UnitType2025), 0);
        assert_eq!(
            slot.invalidate_at,
            expired_legacy_rent_event.block_timestamp as u32 + one_year_in_seconds * 3
        );

        let valid_legacy_rent_event =
            factory::events_factory::create_rent_event(10, Some(5), None, false);
        let slot = StorageSlot::from_event(&valid_legacy_rent_event).unwrap();
        assert_eq!(slot.is_active(), true);
        assert_eq!(slot.units_for(StorageUnitType::UnitTypeLegacy), 5);
        assert_eq!(slot.units_for(StorageUnitType::UnitType2024), 0);
        assert_eq!(slot.units_for(StorageUnitType::UnitType2025), 0);
        assert_eq!(
            slot.invalidate_at,
            valid_legacy_rent_event.block_timestamp as u32 + one_year_in_seconds * 3
        );

        let valid_2024_rent_event =
            factory::events_factory::create_rent_event(10, None, Some(9), false);
        let slot = StorageSlot::from_event(&valid_2024_rent_event).unwrap();
        assert_eq!(slot.is_active(), true);
        assert_eq!(slot.units_for(StorageUnitType::UnitTypeLegacy), 0);
        assert_eq!(slot.units_for(StorageUnitType::UnitType2024), 9);
        assert_eq!(slot.units_for(StorageUnitType::UnitType2025), 0);
        assert_eq!(
            slot.invalidate_at,
            valid_2024_rent_event.block_timestamp as u32 + one_year_in_seconds * 2
        );

        let sep_1_2025 = 1756710000;
        let valid_2025_rent_event =
            factory::events_factory::create_rent_event_with_timestamp(11, 3, sep_1_2025);
        let slot = StorageSlot::from_event(&valid_2025_rent_event).unwrap();
        assert_eq!(slot.is_active(), true);
        assert_eq!(slot.units_for(StorageUnitType::UnitTypeLegacy), 0);
        assert_eq!(slot.units_for(StorageUnitType::UnitType2024), 0);
        assert_eq!(slot.units_for(StorageUnitType::UnitType2025), 3);
        assert_eq!(
            slot.invalidate_at,
            valid_2025_rent_event.block_timestamp as u32 + one_year_in_seconds
        );
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

        let storage_slot = store.get_storage_slot_for_fid(10).unwrap();
        assert_eq!(storage_slot.is_active(), false);
        assert_eq!(storage_slot.units_for(StorageUnitType::UnitTypeLegacy), 0);
        assert_eq!(storage_slot.units_for(StorageUnitType::UnitType2024), 0);
        assert_eq!(storage_slot.units_for(StorageUnitType::UnitType2025), 0);
        assert_eq!(storage_slot.invalidate_at, 0);
    }

    #[test]
    fn test_storage_slot_with_mix_of_units() {
        let (store, _dir) = store();

        let expired_legacy_rent_event =
            factory::events_factory::create_rent_event(10, Some(1), None, true);

        let valid_legacy_rent_event =
            factory::events_factory::create_rent_event(10, Some(5), None, false);
        let another_valid_legacy_rent_event =
            factory::events_factory::create_rent_event(10, Some(7), None, false);
        let valid_2024_rent_event =
            factory::events_factory::create_rent_event(10, None, Some(9), false);
        let another_valid_2024_rent_event =
            factory::events_factory::create_rent_event(10, None, Some(11), false);

        let valid_rent_event_different_fid =
            factory::events_factory::create_rent_event(11, None, Some(13), false);

        // Get timestamp for a date in Aug 2025
        let sep_1_2025 = 1756710000;
        let valid_2025_rent_event =
            factory::events_factory::create_rent_event_with_timestamp(12, 1, sep_1_2025);

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

        let storage_slot_different_fid = store.get_storage_slot_for_fid(11).unwrap();
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

        let storage_slot = store.get_storage_slot_for_fid(10).unwrap();
        assert_eq!(storage_slot.is_active(), true);
        assert_eq!(storage_slot.units_for(StorageUnitType::UnitTypeLegacy), 12); // 5 + 7
        assert_eq!(storage_slot.units_for(StorageUnitType::UnitType2024), 20); // 9 + 11

        let storage_slot_2025 = store.get_storage_slot_for_fid(12).unwrap();
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
}
