use super::{AsyncMigration, MigrationContext, MigrationError};
use crate::storage::db::RocksDbTransactionBatch;
use crate::storage::store::account::{
    make_fname_username_proof_by_fid_key, make_fname_username_proof_key, FIDIterator, UserDataStore,
};
use crate::storage::trie::merkle_trie::TrieKey;
use async_trait::async_trait;
use tracing::{error, info};

pub struct M1FixFnameSecondaryIndex;

#[async_trait]
impl AsyncMigration for M1FixFnameSecondaryIndex {
    fn version(&self) -> u32 {
        1
    }

    fn description(&self) -> &str {
        "Fixes the secondary index for Farcaster name (fname) proofs by ensuring all proofs have a corresponding by-FID index entry."
    }

    fn blocks_startup(&self) -> bool {
        false // This migration can run in the background
    }

    async fn run(&self, mut context: MigrationContext) -> Result<(), MigrationError> {
        let fid_iterator = FIDIterator::new(context.stores.db.clone(), 0);

        info!(
            shard_id = context.stores.shard_id,
            "Starting fname secondary index fix."
        );

        for fid in fid_iterator {
            // The original logic was in a function called `fix_fname_secondary_index_for_fid`
            // We will move that logic here.
            let mut txn = RocksDbTransactionBatch::new();
            let mut fixed_count = 0;

            let trie_keys = context
                .stores
                .trie
                .get_all_values(
                    &crate::storage::trie::merkle_trie::Context::new(),
                    &context.stores.db,
                    &TrieKey::for_fid(fid),
                )
                .map_err(|e| MigrationError::InternalError(format!("Trie error: {}", e)))?;

            for trie_key in trie_keys {
                // Check if this is a fname proof (type_byte == 7)
                if trie_key.len() > 5 && trie_key[5] == 7 {
                    let name_bytes = &trie_key[6..];
                    let name_end = name_bytes
                        .iter()
                        .position(|&b| b == 0)
                        .unwrap_or(name_bytes.len());
                    let name = &name_bytes[..name_end];

                    if name.is_empty() {
                        continue;
                    }
                    let name_str = String::from_utf8_lossy(name).to_string();

                    match UserDataStore::get_username_proof(
                        &context.stores.user_data_store,
                        &txn,
                        &name.to_vec(),
                    ) {
                        Ok(Some(message)) => {
                            let secondary_key = make_fname_username_proof_by_fid_key(message.fid);
                            if context.stores.db.get(&secondary_key)?.is_none() {
                                info!(fid, name_str, "Fixing missing secondary index for fname.");
                                let primary_key = make_fname_username_proof_key(name);
                                txn.put(secondary_key, primary_key);
                                fixed_count += 1;
                            }
                        }
                        Ok(None) => {
                            error!(fid, name_str, "Proof in trie but not in store, skipping.");
                        }
                        Err(e) => {
                            error!(fid, name_str, "Failed to get proof: {}", e);
                        }
                    }
                }
            }

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
                    "Committed fname index fixes."
                );
            }
        }
        info!(
            shard_id = context.stores.shard_id,
            "Finished fname secondary index fix."
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::db::RocksDbTransactionBatch;
    use crate::storage::store::account::{make_fname_username_proof_by_fid_key, UserDataStore};
    use crate::storage::store::migrations::MigrationContext;
    use crate::storage::store::test_helper::{self, default_custody_address, key_exists_in_trie};
    use crate::utils::factory::{self, username_factory};

    #[tokio::test]
    async fn test_m1_fix_fname_secondary_index() {
        let fid = 111;
        let fname_signer = alloy_signer_local::PrivateKeySigner::random();

        let timestamp = factory::time::farcaster_time();

        let (mut engine, _tmpdir) = test_helper::new_engine().await;
        test_helper::register_user(
            fid,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;

        let fname = "username".to_string();
        let fname_register = username_factory::create_transfer(
            fid,
            &fname,
            Some(timestamp),
            Some(0),
            Some(default_custody_address()),
            fname_signer.clone(),
        );
        test_helper::commit_fname_transfer(&mut engine, &fname_register).await;
        assert!(key_exists_in_trie(
            &mut engine,
            &TrieKey::for_fname(fid, &fname)
        ));

        let result =
            UserDataStore::get_username_proof_by_fid(&engine.get_stores().user_data_store, fid);
        assert!(result.is_ok());
        assert!(String::from_utf8(result.unwrap().unwrap().name).unwrap() == fname);

        // Manually delete the secondary key from the DB
        let db = engine.db.clone();
        let mut txn = RocksDbTransactionBatch::new();
        let secondary_key = make_fname_username_proof_by_fid_key(fid);
        txn.delete(secondary_key);
        db.commit(txn).unwrap();

        // Now we can't get the get_username_proof_by_fid
        let result =
            UserDataStore::get_username_proof_by_fid(&engine.get_stores().user_data_store, fid);
        assert!(result.unwrap().is_none());

        // setup is now complete. do the migration

        // 1. Construct the migration context
        let stores = engine.get_stores();
        let migration_context = MigrationContext {
            db: engine.db.clone(),
            stores: stores.clone(),
        };

        // 2. Run the migration and await the handle
        let result = M1FixFnameSecondaryIndex.run(migration_context).await;

        // 3. Assert that the migration succeeded
        assert!(result.is_ok(), "Migration failed: {:?}", result.err());

        // 4. Test that get_username_proof_by_fid() now works again
        let result =
            UserDataStore::get_username_proof_by_fid(&engine.get_stores().user_data_store, fid);
        assert!(
            String::from_utf8(result.unwrap().unwrap().name).unwrap() == fname,
            "Username proof by fid should be restored after migration"
        );
    }
}
