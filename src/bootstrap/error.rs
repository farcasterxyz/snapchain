use futures::channel::oneshot;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;

use crate::storage::{db::RocksdbError, store::engine::EngineError, trie::errors::TrieError};

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

    #[error("Trie error: {0}")]
    TrieError(#[from] TrieError),

    #[error("RPC error: {0}")]
    RpcError(#[from] tonic::Status),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Account root mismatch: {0}")]
    AccountRootMismatch(String),

    #[error("Parse error: {0}")]
    ParseError(#[from] std::num::ParseIntError),

    #[error("{0}")]
    GenericError(String),
}

impl From<&str> for BootstrapError {
    fn from(s: &str) -> Self {
        BootstrapError::GenericError(s.to_string())
    }
}

impl From<String> for BootstrapError {
    fn from(s: String) -> Self {
        BootstrapError::GenericError(s)
    }
}

impl From<RocksdbError> for BootstrapError {
    fn from(err: RocksdbError) -> Self {
        BootstrapError::DatabaseError(err.to_string())
    }
}

impl From<tonic::transport::Error> for BootstrapError {
    fn from(err: tonic::transport::Error) -> Self {
        BootstrapError::PeerConnectionError(err.to_string())
    }
}

impl From<EngineError> for BootstrapError {
    fn from(err: EngineError) -> Self {
        BootstrapError::TransactionReplayError(err.to_string())
    }
}

impl<T> From<SendError<T>> for BootstrapError {
    fn from(err: SendError<T>) -> Self {
        BootstrapError::GenericError(err.to_string())
    }
}

impl From<oneshot::Canceled> for BootstrapError {
    fn from(_: oneshot::Canceled) -> Self {
        BootstrapError::GenericError("DB writer response channel cancelled".to_string())
    }
}
