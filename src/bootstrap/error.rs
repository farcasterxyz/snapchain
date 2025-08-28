use thiserror::Error;

#[derive(Error, Debug)]
pub enum BootstrapError {
    #[error("Failed to connect to peer: {0}")]
    PeerConnectionError(String),

    #[error("Failed to initialize gossip: {0}")]
    GossipInitError(String),

    #[error("Failed to fetch snapshot metadata: {0}")]
    MetadataFetchError(String),

    #[error("Failed to replay transactions: {0}")]
    TransactionReplayError(String),

    #[error("PostProcessing failed: {0}")]
    PostProcessError(String),

    #[error(
        "State root verification failed for shard {shard_id}: expected {expected}, got {actual}"
    )]
    StateRootMismatch {
        shard_id: u32,
        expected: String,
        actual: String,
    },

    #[error("Database operation failed: {0}")]
    DatabaseError(String),

    #[error("RPC error: {0}")]
    RpcError(#[from] tonic::Status),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Account root mismatch: {0}")]
    AccountRootMismatch(String),
}
