use crate::core::error::HubError;
use crate::storage::db::{RocksDB, RocksDbTransactionBatch};
use crate::storage::store::migrations::m1_fix_fname_index::M1FixFnameSecondaryIndex;
use crate::storage::store::stores::Stores;
use async_trait::async_trait;
use std::sync::Arc;
use thiserror::Error;
use tracing::{error, info};

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

/// Trait that all migration implementations must adhere to.
#[async_trait]
pub trait AsyncMigration: Send + Sync {
    /// Returns the schema version this migration upgrades the DB to.
    fn version(&self) -> u32;

    /// A brief description of what the migration does.
    fn description(&self) -> &str;

    /// Whether this migration should block node startup until completion.
    /// If true, the migration will be awaited during node initialization.
    /// If false, the migration will run in the background without blocking startup.
    fn blocks_startup(&self) -> bool;

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

    /// Checks the database schema version and runs all pending migrations.
    /// Returns a handle to the background task for non-blocking migrations.
    pub async fn run_pending_migrations(
        self,
    ) -> Result<Option<tokio::task::JoinHandle<Result<(), MigrationError>>>, MigrationError> {
        let db_version = self.context.stores.get_schema_version()?;
        info!(
            shard_id = self.context.stores.shard_id,
            db_version,
            code_version = LATEST_SCHEMA_VERSION,
            "Checking for pending DB migrations."
        );

        if db_version >= LATEST_SCHEMA_VERSION {
            info!(
                shard_id = self.context.stores.shard_id,
                "DB schema is up to date."
            );
            return Ok(None);
        }

        for (i, migration) in self.all_migrations.iter().enumerate() {
            if migration.version() as usize != i + 1 {
                return Err(MigrationError::InternalError(format!(
                    "Migration version mismatch for '{}': expected {}, found {}",
                    migration.description(),
                    i + 1,
                    migration.version()
                )));
            }
        }

        let start_migrations_at = db_version as usize;
        if start_migrations_at >= self.all_migrations.len() {
            return Err(MigrationError::InternalError(
                "Migration list and DB Schema mismatch!".to_string(),
            ));
        }

        // Collect all the same type of migrations to run (Don't mix blocking and non-blocking migrations)
        let is_blocking_migrations = self.all_migrations[start_migrations_at].blocks_startup();
        let migrations_to_run = self
            .all_migrations
            .into_iter()
            .skip(start_migrations_at)
            .take_while(|m| m.blocks_startup() == is_blocking_migrations)
            .collect::<Vec<_>>();

        if is_blocking_migrations {
            // Blocking migrations are run one by one, waiting for each to finish
            for migration in migrations_to_run {
                info!(
                    shard_id = self.context.stores.shard_id,
                    version = migration.version(),
                    description = migration.description(),
                    "Running blocking migration..."
                );

                // Directly await, blocking engine startup
                migration.run(self.context.clone()).await?;

                // Update the schema version in the DB transactionally with the migration
                let mut txn = RocksDbTransactionBatch::new();
                self.context
                    .stores
                    .set_schema_version(migration.version(), &mut txn)?;
                self.context.db.commit(txn)?;

                info!(
                    shard_id = self.context.stores.shard_id,
                    version = migration.version(),
                    "Blocking migration completed successfully."
                );
            }
            Ok(None)
        } else {
            // Non blocking migrations. Kick them off in parallel and return, not waiting for them to finish
            // i.e., they will run in the background
            let context = self.context.clone();

            let handle = tokio::spawn(async move {
                for migration in migrations_to_run {
                    info!(
                        shard_id = context.stores.shard_id,
                        version = migration.version(),
                        description = migration.description(),
                        "Starting background migration..."
                    );

                    // We will await the background migration, but we're inside a tokio::spawn, so not blocking engine startup
                    // This is done so that only one background migration runs at a time, and the SCHEMA_VERSION is updated correctly
                    migration.run(context.clone()).await?;

                    // Update the schema version in the DB transactionally with the migration
                    let mut txn = RocksDbTransactionBatch::new();
                    context
                        .stores
                        .set_schema_version(migration.version(), &mut txn)?;
                    context.db.commit(txn)?;

                    info!(
                        shard_id = context.stores.shard_id,
                        version = migration.version(),
                        "Background migration completed successfully."
                    );
                }
                Ok(())
            });
            Ok(Some(handle))
        }
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
        blocks: bool,
        run_tracker: Arc<Mutex<Vec<u32>>>,
    }

    #[async_trait]
    impl AsyncMigration for TestMigration {
        fn version(&self) -> u32 {
            self.version
        }

        fn description(&self) -> &str {
            "A test migration"
        }

        fn blocks_startup(&self) -> bool {
            self.blocks
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
            blocks: true,
            run_tracker: run_tracker.clone(),
        })];

        // Run the migration
        let runner = MigrationRunner::new_with_list(context.clone(), migrations);
        let handle = runner.run_pending_migrations().await.unwrap();
        assert!(
            handle.is_none(),
            "Blocking migration should not return a handle"
        );

        // Assert that the migration ran and the DB version was updated
        assert_eq!(*run_tracker.lock().await, vec![1]);
        assert_eq!(stores.get_schema_version().unwrap(), 1);
    }

    #[tokio::test]
    async fn test_runner_runs_multiple_blocking_migrations_in_order() {
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
                blocks: true,
                run_tracker: run_tracker.clone(),
            }),
            Box::new(TestMigration {
                version: 2,
                blocks: true,
                run_tracker: run_tracker.clone(),
            }),
            Box::new(TestMigration {
                version: 3,
                blocks: true,
                run_tracker: run_tracker.clone(),
            }),
        ];

        let runner = MigrationRunner::new_with_list(context.clone(), migrations);
        // Run the migrations
        let handle = runner.run_pending_migrations().await.unwrap();
        assert!(handle.is_none());

        // Assert that all migrations ran in the correct order
        assert_eq!(*run_tracker.lock().await, vec![1, 2, 3]);
        // Assert that the DB version was updated to the latest version
        assert_eq!(stores.get_schema_version().unwrap(), 3);
    }

    #[tokio::test]
    async fn test_runner_stops_at_first_non_blocking_migration() {
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
                blocks: true,
                run_tracker: run_tracker.clone(),
            }),
            Box::new(TestMigration {
                version: 2,
                blocks: true,
                run_tracker: run_tracker.clone(),
            }),
            Box::new(TestMigration {
                version: 3,
                blocks: false,
                run_tracker: run_tracker.clone(),
            }),
            Box::new(TestMigration {
                version: 4,
                blocks: false,
                run_tracker: run_tracker.clone(),
            }),
            Box::new(TestMigration {
                version: 5,
                blocks: true,
                run_tracker: run_tracker.clone(),
            }),
        ];

        let runner = MigrationRunner::new_with_list(context.clone(), migrations);
        let handle = runner.run_pending_migrations().await.unwrap();
        assert!(handle.is_none());

        // Assert that only the consecutive blocking migrations ran
        assert_eq!(*run_tracker.lock().await, vec![1, 2]);
        // Assert that the non-blocking migrations did NOT run
        assert!(!run_tracker.lock().await.contains(&3));
        assert!(!run_tracker.lock().await.contains(&4));
        assert!(!run_tracker.lock().await.contains(&5));

        // Assert that the DB version was updated only to the last completed blocking migration
        assert_eq!(stores.get_schema_version().unwrap(), 2);
    }
}
