use super::{AsyncMigration, MigrationContext, MigrationError};
use crate::storage::constants::{RootPrefix, UserPostfix};
use crate::storage::db::{PageOptions, RocksDbTransactionBatch};
use crate::storage::store::account::{
    make_user_key, FIDIterator, VerificationStoreDef, FID_BYTES, TS_HASH_LENGTH,
};
use crate::storage::util::increment_vec_u8;
use async_trait::async_trait;
use tracing::{info, warn};

pub struct M2VerificationByAddressIndex;

/// Max legacy-slot deletes per committed transaction, so no single write batch
/// grows unbounded during the cleanup sweep. This bounds the RocksDB batch, not
/// process memory: the collection phase still holds every legacy key in
/// `old_keys` before deleting — O(legacy keys), acceptable for a one-shot
/// background migration.
const DELETE_CHUNK_SIZE: usize = 10_000;

/// A legacy `VerificationByAddress` slot stores a bare `make_fid_key` value
/// (`FID_BYTES` long); the new per-verifier entries store a `TS_HASH_LENGTH`
/// (24) byte ts_hash. Discriminating on the value length is
/// address-length-agnostic, so it needs no enumeration of the valid address
/// sizes.
///
/// Correct only because `FID_BYTES != TS_HASH_LENGTH` and the sweep runs *after*
/// the backfill has written the new entries into the same keyspace: this length
/// check is what stops the sweep from deleting those fresh entries. If the two
/// lengths ever converged, the sweep would misclassify new entries as legacy and
/// delete them — a data-loss migration replayed by every node.
fn is_old_verification_by_address_value(value: &[u8]) -> bool {
    value.len() == FID_BYTES
}

#[async_trait]
impl AsyncMigration for M2VerificationByAddressIndex {
    fn to_db_version(&self) -> u32 {
        2
    }

    fn description(&self) -> &str {
        "Backfills verification by-address secondary index entries per verifier and removes legacy address-only slots."
    }

    async fn run(&self, context: MigrationContext) -> Result<(), MigrationError> {
        let fid_iterator = FIDIterator::new(context.stores.db.clone(), 0);
        let shard_id = context.stores.shard_id;

        info!(
            shard_id,
            "Starting verification by-address index migration."
        );

        for fid in fid_iterator {
            if fid % 1000 == 0 {
                info!(
                    fid,
                    shard_id = context.stores.shard_id,
                    "Processing FID for verification by-address index migration."
                );
            }

            let mut txn = RocksDbTransactionBatch::new();
            let mut fixed_count = 0;
            let mut prefix = make_user_key(fid);
            prefix.push(UserPostfix::VerificationAdds as u8);
            let stop_prefix = increment_vec_u8(&prefix);

            context
                .stores
                .db
                .for_each_iterator_by_prefix(
                    Some(prefix.clone()),
                    Some(stop_prefix),
                    &PageOptions::default(),
                    |key, value| {
                        if value.len() != TS_HASH_LENGTH {
                            // A primary VerificationAdds value is always a
                            // 24-byte ts_hash; a malformed one means local DB
                            // corruption for this entry. Skip and log it (M1's
                            // stance) rather than aborting the whole migration —
                            // aborting would leave the schema version un-bumped
                            // and re-run the sweep from fid 0 on every restart.
                            warn!(
                                fid,
                                shard_id,
                                value_len = value.len(),
                                "Skipping verification add with unexpected ts_hash length during migration."
                            );
                            return Ok(false);
                        }

                        let address = &key[prefix.len()..];
                        if address.is_empty() {
                            return Ok(false);
                        }

                        let by_address_key =
                            VerificationStoreDef::make_verification_by_address_key(address, fid);
                        txn.put(by_address_key, value.to_vec());
                        fixed_count += 1;

                        Ok(false)
                    },
                )
                .map_err(|e| MigrationError::InternalError(e.to_string()))?;

            if fixed_count > 0 {
                context
                    .stores
                    .db
                    .commit(txn)
                    .map_err(MigrationError::DbError)?;
                info!(
                    shard_id = context.stores.shard_id,
                    fid,
                    count = fixed_count,
                    "Committed verification by-address index entries."
                );
            }
        }

        let root_prefix = vec![RootPrefix::VerificationByAddress as u8];
        let stop_prefix = increment_vec_u8(&root_prefix);
        let mut old_keys = Vec::new();
        context
            .stores
            .db
            .for_each_iterator_by_prefix(
                Some(root_prefix),
                Some(stop_prefix),
                &PageOptions::default(),
                |key, value| {
                    if is_old_verification_by_address_value(value) {
                        old_keys.push(key.to_vec());
                    }

                    Ok(false)
                },
            )
            .map_err(|e| MigrationError::InternalError(e.to_string()))?;

        if !old_keys.is_empty() {
            let old_key_count = old_keys.len();
            let mut txn = RocksDbTransactionBatch::new();
            for key in old_keys {
                txn.delete(key);
                if txn.len() >= DELETE_CHUNK_SIZE {
                    context
                        .stores
                        .db
                        .commit(txn)
                        .map_err(MigrationError::DbError)?;
                    txn = RocksDbTransactionBatch::new();
                }
            }
            if txn.len() > 0 {
                context
                    .stores
                    .db
                    .commit(txn)
                    .map_err(MigrationError::DbError)?;
            }
            info!(
                shard_id = context.stores.shard_id,
                count = old_key_count,
                "Deleted legacy verification by-address slots."
            );
        }

        info!(
            shard_id = context.stores.shard_id,
            "Finished verification by-address index migration."
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{IdRegisterEventType, Message};
    use crate::storage::store::account::{make_fid_key, make_ts_hash, VerificationStore};
    use crate::storage::store::migrations::MigrationContext;
    use crate::storage::store::stores::Stores;
    use crate::storage::store::test_helper::{self, default_custody_address};
    use crate::utils::factory::{address, events_factory, messages_factory};
    use tempfile::TempDir;

    async fn setup_migration_test(
        fid1: u64,
        fid2: u64,
        verification_address: Vec<u8>,
    ) -> (Stores, TempDir, Message, Message) {
        let (engine, tmpdir) = test_helper::new_engine().await;
        let stores = engine.get_stores();

        let id_register1 = events_factory::create_id_register_event(
            fid1,
            IdRegisterEventType::Register,
            default_custody_address(),
            None,
        );
        let id_register2 = events_factory::create_id_register_event(
            fid2,
            IdRegisterEventType::Register,
            default_custody_address(),
            None,
        );

        let add1 = messages_factory::verifications::create_verification_add(
            fid1,
            0,
            verification_address.clone(),
            vec![],
            vec![],
            Some(1),
            None,
        );
        let add2 = messages_factory::verifications::create_verification_add(
            fid2,
            0,
            verification_address.clone(),
            vec![],
            vec![],
            Some(2),
            None,
        );

        let mut txn = RocksDbTransactionBatch::new();
        stores
            .onchain_event_store
            .merge_onchain_event(id_register1, &mut txn)
            .unwrap();
        stores
            .onchain_event_store
            .merge_onchain_event(id_register2, &mut txn)
            .unwrap();
        txn.put(
            VerificationStoreDef::make_verification_adds_key(fid1, &verification_address),
            make_ts_hash(add1.data.as_ref().unwrap().timestamp, &add1.hash)
                .unwrap()
                .to_vec(),
        );
        txn.put(
            VerificationStoreDef::make_verification_adds_key(fid2, &verification_address),
            make_ts_hash(add2.data.as_ref().unwrap().timestamp, &add2.hash)
                .unwrap()
                .to_vec(),
        );
        txn.put(
            VerificationStoreDef::make_verification_by_address_prefix(&verification_address),
            make_fid_key(fid2),
        );
        stores.db.commit(txn).unwrap();

        (stores, tmpdir, add1, add2)
    }

    async fn run_migration(stores: Stores) {
        let migration_context = MigrationContext {
            db: stores.db.clone(),
            stores,
        };
        M2VerificationByAddressIndex
            .run(migration_context)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_m2_verification_by_address_index_migrates_entries_and_deletes_old_slots() {
        let fid1 = 111;
        let fid2 = 222;
        let verification_address = address::generate_random_address();
        let (stores, _tmpdir, add1, add2) =
            setup_migration_test(fid1, fid2, verification_address.clone()).await;

        run_migration(stores.clone()).await;

        let entries = VerificationStore::get_verifications_by_address(
            &stores.verification_store,
            &verification_address,
        )
        .unwrap();
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().any(|(fid, ts_hash)| {
            *fid == fid1
                && *ts_hash
                    == make_ts_hash(add1.data.as_ref().unwrap().timestamp, &add1.hash).unwrap()
        }));
        assert!(entries.iter().any(|(fid, ts_hash)| {
            *fid == fid2
                && *ts_hash
                    == make_ts_hash(add2.data.as_ref().unwrap().timestamp, &add2.hash).unwrap()
        }));
        assert!(stores
            .db
            .get(&VerificationStoreDef::make_verification_by_address_prefix(
                &verification_address
            ))
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn test_m2_verification_by_address_index_is_idempotent() {
        let fid1 = 333;
        let fid2 = 444;
        let verification_address = address::generate_random_address();
        let (stores, _tmpdir, _add1, _add2) =
            setup_migration_test(fid1, fid2, verification_address.clone()).await;

        run_migration(stores.clone()).await;
        run_migration(stores.clone()).await;

        let entries = VerificationStore::get_verifications_by_address(
            &stores.verification_store,
            &verification_address,
        )
        .unwrap();
        assert_eq!(entries.len(), 2);
        assert!(stores
            .db
            .get(&VerificationStoreDef::make_verification_by_address_prefix(
                &verification_address
            ))
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn test_m2_deletes_all_legacy_slots_across_chunk_boundary() {
        let (engine, _tmpdir) = test_helper::new_engine().await;
        let stores = engine.get_stores();

        // Seed more legacy slots than a single delete chunk holds, so the sweep
        // exercises the mid-loop commit path (commit + fresh batch) and not just
        // the final flush. Each slot is a legacy address-only key (value == a
        // bare 4-byte fid).
        let slot_count = DELETE_CHUNK_SIZE + 1;
        let mut txn = RocksDbTransactionBatch::new();
        for i in 0..slot_count {
            let mut address = vec![0u8; 20];
            address[..8].copy_from_slice(&(i as u64).to_be_bytes());
            txn.put(
                VerificationStoreDef::make_verification_by_address_prefix(&address),
                make_fid_key(i as u64),
            );
        }
        stores.db.commit(txn).unwrap();

        run_migration(stores.clone()).await;

        // Every legacy slot is swept. No FIDs are registered, so phase 1 writes
        // nothing and the root prefix must be completely empty afterward.
        let root_prefix = vec![RootPrefix::VerificationByAddress as u8];
        let stop_prefix = increment_vec_u8(&root_prefix);
        let mut remaining = 0usize;
        stores
            .db
            .for_each_iterator_by_prefix(
                Some(root_prefix),
                Some(stop_prefix),
                &PageOptions::default(),
                |_key, _value| {
                    remaining += 1;
                    Ok(false)
                },
            )
            .unwrap();
        assert_eq!(remaining, 0);
    }
}
