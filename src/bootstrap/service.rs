use crate::{bootstrap::BootstrapError, cfg::Config};
use std::sync::{atomic::AtomicBool, Arc};
use tracing::info;

pub enum ReplicationBootstrapState {
    // Successfully finished, ready to continue with startup
    Finished,

    // User manually stopped it (CTRL+C). Will resume after snapchain is restarted
    Stopped,

    // Bootstrap was finished, but there were some errors. Should continue with startup
    PartiallyComplete,
}

pub struct ReplicatorBootstrap {
    shutdown: Arc<AtomicBool>,
}

impl ReplicatorBootstrap {
    pub fn new() -> Self {
        Self {
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Bootstrap a node from replication instead of snapshot download
    pub async fn bootstrap_using_replication(
        &self,
        app_config: &Config,
    ) -> Result<ReplicationBootstrapState, BootstrapError> {
        info!("Starting replication-based bootstrap");

        Ok(ReplicationBootstrapState::Stopped)
    }
}
