use crate::storage::db::{RocksDB, RocksdbError};
use crate::storage::store::migrations::m1_fix_fname_index::M1FixFnameSecondaryIndex;
use crate::storage::store::stores::Stores;
use crate::{core::error::HubError, storage::constants::RootPrefix};
use async_trait::async_trait;
use std::sync::Arc;
use thiserror::Error;
use tracing::info;

mod m1_fix_fname_index;

/// The latest DB schema version supported by this version of the code.
pub const LATEST_SCHEMA_VERSION: u32 = 1;

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
            // Add future migrations here, e.g., Box::new(M2DoSomethingElse)
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

        if db_version >= LATEST_SCHEMA_VERSION {
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

        // Don't run more than 1 migration at a time
        let context = self.context.clone();
        if Self::get_migration_running(&context, start_migrations_at as u32)? {
            return Ok(None);
        }
        Self::set_migration_running(&context, start_migrations_at as u32, true)?;

        info!(
            shard_id = self.context.stores.shard_id,
            db_version,
            code_version = LATEST_SCHEMA_VERSION,
            start_migrations_at,
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
                info!(
                    shard_id = context.stores.shard_id,
                    version = migration.to_db_version(),
                    description = migration.description(),
                    "Starting background migration..."
                );

                // We will await the background migration, but we're inside a tokio::spawn, so not blocking engine startup
                // This is done so that only one background migration runs at a time, and the SCHEMA_VERSION is updated correctly
                if let Err(e) = migration.run(context.clone()).await {
                    // If a migration fails, we'll write to DB that the migration is no longer running
                    Self::set_migration_running(&context, start_migrations_at as u32, false)?;
                    return Err(e);
                }

                // Update the schema version in the DB transactionally with the migration
                context
                    .stores
                    .set_schema_version(migration.to_db_version())?;

                info!(
                    shard_id = context.stores.shard_id,
                    version = migration.to_db_version(),
                    "Background migration completed successfully."
                );
            }

            Self::set_migration_running(&context, start_migrations_at as u32, false)?;
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
