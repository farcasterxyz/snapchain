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
}

impl MigrationRunner {
    pub fn new(context: MigrationContext) -> Self {
        Self { context }
    }

    /// Checks the database schema version and runs all pending migrations.
    /// Returns a handle to the background task for non-blocking migrations.
    pub async fn run_pending_migrations(
        self,
    ) -> Result<Option<tokio::task::JoinHandle<Result<(), MigrationError>>>, MigrationError> {
        let all_migrations: Vec<Box<dyn AsyncMigration>> = vec![
            Box::new(M1FixFnameSecondaryIndex),
            // Add future migrations here, e.g., Box::new(M2DoSomethingElse)
        ];

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

        let start_migrations_at = db_version as usize + 1;
        if start_migrations_at >= all_migrations.len() {
            return Err(MigrationError::InternalError(
                "Migration list and DB Schema mismatch!".to_string(),
            ));
        }

        // Collect all the same type of migrations to run (Don't mix blocking and non-blocking migrations)
        let is_blocking_migrations = all_migrations[start_migrations_at].blocks_startup();
        let migrations_to_run = all_migrations
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
