use crate::storage::db::{RocksDB, RocksdbError};
use crate::storage::store::migrations::m1_fix_fname_index::M1FixFnameSecondaryIndex;
use crate::storage::store::migrations::m2_verification_by_address_index::M2VerificationByAddressIndex;
use crate::storage::store::stores::Stores;
use crate::{core::error::HubError, storage::constants::RootPrefix};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;
use tracing::{error, info, warn};

mod m1_fix_fname_index;
mod m2_verification_by_address_index;

/// The latest DB schema version supported by this version of the code.
pub const LATEST_SCHEMA_VERSION: u32 = 2;

#[derive(Error, Debug)]
pub enum MigrationError {
    #[error("Database error during migration: {0}")]
    DbError(#[from] crate::storage::db::RocksdbError),

    #[error("Internal migration error: {0}")]
    InternalError(String),
}

impl From<MigrationError> for HubError {
    fn from(err: MigrationError) -> Self {
        HubError::internal_db_error(&err.to_string())
    }
}

/// A context object to pass necessary dependencies to migrations.
#[derive(Clone)]
pub struct MigrationContext {
    pub db: Arc<RocksDB>,
    pub stores: Stores,
}

/// Trait that all migration implementations must adhere to. Note that these are all non-blocking migrations
/// i.e. they don't block engine startup.
///
/// IDEMPOTENCY CONTRACT — every migration MUST be safe to (a) run to completion
/// more than once and (b) re-run from the start after a partial, interrupted
/// run. The runner re-runs a migration after any handled error, and — because a
/// process can crash or be restarted mid-migration, leaving the in-progress flag
/// set with the schema version un-bumped — it also re-runs a migration whose
/// flag was left set by such an interrupted run (see `run_pending_migrations`).
/// A migration that is not idempotent can therefore corrupt data. Achieve
/// idempotency by making each write derive its value so that a repeat is a no-op
/// (e.g. read-under-lock / last-write-wins) and by making any destructive
/// cleanup discriminate the new state from the old before deleting (e.g. M2's
/// value-length check), never by assuming it runs exactly once.
#[async_trait]
pub trait AsyncMigration: Send + Sync {
    /// Returns the schema version this migration upgrades the DB to.
    fn to_db_version(&self) -> u32;

    /// A brief description of what the migration does.
    fn description(&self) -> &str;

    /// The core logic of the migration.
    async fn run(&self, context: MigrationContext) -> Result<(), MigrationError>;
}

pub struct MigrationRunner {
    context: MigrationContext,
    all_migrations: Vec<Box<dyn AsyncMigration>>,
}

impl MigrationRunner {
    pub fn new(context: MigrationContext) -> Self {
        let all_migrations: Vec<Box<dyn AsyncMigration>> = vec![
            Box::new(M1FixFnameSecondaryIndex),
            Box::new(M2VerificationByAddressIndex),
            // Add future migrations here, e.g., Box::new(M3DoSomethingElse)
        ];

        Self {
            context,
            all_migrations,
        }
    }

    #[cfg(test)]
    pub fn new_with_list(
        context: MigrationContext,
        migrations: Vec<Box<dyn AsyncMigration>>,
    ) -> Self {
        Self {
            context,
            all_migrations: migrations,
        }
    }

    fn make_migration_version_key(migration_version: u32) -> Vec<u8> {
        vec![RootPrefix::DBSchemaVersion as u8, migration_version as u8]
    }

    fn set_migration_running(
        context: &MigrationContext,
        migration_version: u32,
        running: bool,
    ) -> Result<(), RocksdbError> {
        // Write the migration running state to the database
        context.stores.db.put(
            &Self::make_migration_version_key(migration_version),
            &[if running { 1u8 } else { 0u8 }],
        )
    }

    fn get_migration_running(
        context: &MigrationContext,
        migration_version: u32,
    ) -> Result<bool, RocksdbError> {
        // Check if the migration is already running
        match context
            .stores
            .db
            .get(&Self::make_migration_version_key(migration_version))?
        {
            Some(v) => Ok(v == [1u8]),
            None => Ok(false),
        }
    }

    /// Checks the database schema version and runs all pending migrations.
    /// Returns a handle to the background task for the migrations.
    pub async fn run_pending_migrations(
        self,
    ) -> Result<Option<tokio::task::JoinHandle<Result<(), MigrationError>>>, MigrationError> {
        let db_version = self.context.stores.get_schema_version()?;
        let shard_id = self.context.stores.shard_id;

        // Publish the current schema version unconditionally so a node that
        // needs no migration still reports where it is. A migration has finished
        // rolling across the fleet once every shard's gauge reads
        // LATEST_SCHEMA_VERSION.
        self.context.stores.statsd.gauge_with_shard(
            shard_id,
            "migration.schema_version",
            db_version as u64,
        );

        if db_version >= LATEST_SCHEMA_VERSION {
            info!(
                shard_id,
                db_version,
                code_version = LATEST_SCHEMA_VERSION,
                "DB schema is up to date; no migrations to run."
            );
            return Ok(None);
        }

        for (i, migration) in self.all_migrations.iter().enumerate() {
            if migration.to_db_version() as usize != i + 1 {
                return Err(MigrationError::InternalError(format!(
                    "Migration version mismatch for '{}': expected {}, found {}",
                    migration.description(),
                    i + 1,
                    migration.to_db_version()
                )));
            }
        }

        let start_migrations_at = db_version as usize;
        if start_migrations_at >= self.all_migrations.len() {
            return Err(MigrationError::InternalError(
                "Migration list and DB Schema mismatch!".to_string(),
            ));
        }

        // Guard against running the same migration twice concurrently. In
        // practice the DB is opened by a single process (RocksDB's TransactionDB
        // holds an exclusive directory lock) and this runs once per shard at
        // startup, so a still-set flag here does NOT mean a migration is actively
        // running. The flag is cleared on both success and handled failure, and
        // db_version is known to be < LATEST_SCHEMA_VERSION at this point, so a
        // set flag means a prior run started this migration and never finished —
        // the process crashed or was restarted mid-migration. Re-run it rather
        // than skipping: leaving it set would strand this shard at the old schema
        // version until an operator manually cleared the flag. This recovery is
        // safe ONLY because migrations honor the idempotency contract documented
        // on AsyncMigration (re-runnable from scratch after a partial run).
        let context = self.context.clone();
        if Self::get_migration_running(&context, start_migrations_at as u32)? {
            warn!(
                shard_id,
                start_migrations_at,
                db_version,
                "Detected an interrupted migration (in-progress flag still set with an \
                 un-bumped schema version); re-running it from the start. Migrations must \
                 be idempotent for this recovery to be safe."
            );
            context.stores.statsd.count_with_shard(
                shard_id,
                "migration.recovered_interrupted",
                1,
                vec![],
            );
        }
        Self::set_migration_running(&context, start_migrations_at as u32, true)?;

        info!(
            shard_id,
            db_version,
            code_version = LATEST_SCHEMA_VERSION,
            start_migrations_at,
            pending = self.all_migrations.len() - start_migrations_at,
            "DB needs migrations. Running pending DB migrations..."
        );

        // Collect all the migrations to run
        let migrations_to_run = self
            .all_migrations
            .into_iter()
            .skip(start_migrations_at)
            .collect::<Vec<_>>();

        // Kick them off and return, not waiting for them to finish i.e., they will run in the background

        let handle = tokio::spawn(async move {
            for migration in migrations_to_run {
                let version = migration.to_db_version();
                let statsd = &context.stores.statsd;

                statsd.count_with_shard(shard_id, "migration.started", 1, vec![]);
                info!(
                    shard_id,
                    version,
                    description = migration.description(),
                    "Starting background migration..."
                );

                // We will await the background migration, but we're inside a tokio::spawn, so not blocking engine startup
                // This is done so that only one background migration runs at a time, and the SCHEMA_VERSION is updated correctly
                let started_at = Instant::now();
                if let Err(e) = migration.run(context.clone()).await {
                    // The JoinHandle is dropped by the caller, so this is the only
                    // place a background-migration failure surfaces — log it loudly
                    // rather than letting it vanish into a detached task.
                    error!(
                        shard_id,
                        version,
                        description = migration.description(),
                        elapsed_ms = started_at.elapsed().as_millis() as u64,
                        error = %e,
                        "Background migration failed."
                    );
                    context
                        .stores
                        .statsd
                        .count_with_shard(shard_id, "migration.failed", 1, vec![]);
                    // If a migration fails, we'll write to DB that the migration is no longer running
                    Self::set_migration_running(&context, start_migrations_at as u32, false)?;
                    return Err(e);
                }

                // Update the schema version in the DB transactionally with the migration
                context.stores.set_schema_version(version)?;

                let statsd = &context.stores.statsd;
                statsd.count_with_shard(shard_id, "migration.completed", 1, vec![]);
                statsd.gauge_with_shard(shard_id, "migration.schema_version", version as u64);
                info!(
                    shard_id,
                    version,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    "Background migration completed successfully."
                );
            }

            Self::set_migration_running(&context, start_migrations_at as u32, false)?;
            info!(
                shard_id,
                db_version = LATEST_SCHEMA_VERSION,
                "All pending background migrations complete; DB is now at the latest schema version."
            );
            Ok(())
        });
        Ok(Some(handle))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::store::test_helper;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    /// A mock migration for testing purposes. It tracks which migrations have been run.
    struct TestMigration {
        version: u32,
        run_tracker: Arc<Mutex<Vec<u32>>>,
    }

    #[async_trait]
    impl AsyncMigration for TestMigration {
        fn to_db_version(&self) -> u32 {
            self.version
        }

        fn description(&self) -> &str {
            "A test migration"
        }

        async fn run(&self, _context: MigrationContext) -> Result<(), MigrationError> {
            let mut tracker = self.run_tracker.lock().await;
            tracker.push(self.version);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_runner_calls_single_migration() {
        let (engine, _tmpdir) = test_helper::new_engine().await;
        let stores = engine.get_stores();
        let context = MigrationContext {
            db: engine.db.clone(),
            stores: stores.clone(),
        };

        // Start with DB version 0
        assert_eq!(stores.get_schema_version().unwrap(), 0);

        let run_tracker = Arc::new(Mutex::new(Vec::new()));
        let migrations: Vec<Box<dyn AsyncMigration>> = vec![Box::new(TestMigration {
            version: 1,
            run_tracker: run_tracker.clone(),
        })];

        // Run the migration
        let runner = MigrationRunner::new_with_list(context.clone(), migrations);
        let handle = runner.run_pending_migrations().await.unwrap();
        handle.unwrap().await.unwrap().unwrap();

        // Assert that the migration ran and the DB version was updated
        assert_eq!(*run_tracker.lock().await, vec![1]);
        assert_eq!(stores.get_schema_version().unwrap(), 1);
    }

    #[tokio::test]
    async fn test_runner_reruns_interrupted_migration_instead_of_wedging() {
        let (engine, _tmpdir) = test_helper::new_engine().await;
        let stores = engine.get_stores();
        let context = MigrationContext {
            db: engine.db.clone(),
            stores: stores.clone(),
        };
        assert_eq!(stores.get_schema_version().unwrap(), 0);

        // Simulate a prior run that set the in-progress flag but crashed before
        // bumping the schema version — the classic wedge state.
        MigrationRunner::set_migration_running(&context, 0, true).unwrap();

        let run_tracker = Arc::new(Mutex::new(Vec::new()));
        let migrations: Vec<Box<dyn AsyncMigration>> = vec![Box::new(TestMigration {
            version: 1,
            run_tracker: run_tracker.clone(),
        })];

        let runner = MigrationRunner::new_with_list(context.clone(), migrations);
        // A wedge would skip and return Ok(None); recovery returns a handle that
        // re-runs the interrupted migration.
        let handle = runner
            .run_pending_migrations()
            .await
            .unwrap()
            .expect("interrupted migration should re-run, not be skipped");
        handle.await.unwrap().unwrap();

        // The migration re-ran and the schema advanced past the wedge.
        assert_eq!(*run_tracker.lock().await, vec![1]);
        assert_eq!(stores.get_schema_version().unwrap(), 1);
    }

    #[tokio::test]
    async fn test_runner_runs_multiple_migrations_in_order() {
        let (engine, _tmpdir) = test_helper::new_engine().await;
        let stores = engine.get_stores();
        let context = MigrationContext {
            db: engine.db.clone(),
            stores: stores.clone(),
        };
        assert_eq!(stores.get_schema_version().unwrap(), 0);

        let run_tracker = Arc::new(Mutex::new(Vec::new()));
        let migrations: Vec<Box<dyn AsyncMigration>> = vec![
            Box::new(TestMigration {
                version: 1,
                run_tracker: run_tracker.clone(),
            }),
            Box::new(TestMigration {
                version: 2,
                run_tracker: run_tracker.clone(),
            }),
            Box::new(TestMigration {
                version: 3,
                run_tracker: run_tracker.clone(),
            }),
        ];

        let runner = MigrationRunner::new_with_list(context.clone(), migrations);
        // Run the migrations
        let handle = runner.run_pending_migrations().await.unwrap();
        handle.unwrap().await.unwrap().unwrap();

        // Assert that all migrations ran in the correct order
        assert_eq!(*run_tracker.lock().await, vec![1, 2, 3]);
        // Assert that the DB version was updated to the latest version
        assert_eq!(stores.get_schema_version().unwrap(), 3);
    }
}
