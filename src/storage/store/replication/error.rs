use std::fmt::Display;

pub enum ReplicationError {
    ShardStoreNotFound(u32),
    StoreNotFound(u64, u32, String),
    InternalError(String),
    InvalidMessage(String),
}

impl From<ReplicationError> for tonic::Status {
    fn from(err: ReplicationError) -> Self {
        match err {
            ReplicationError::ShardStoreNotFound(shard) => {
                tonic::Status::internal(format!("Shard store not found for shard {}", shard))
            }
            ReplicationError::StoreNotFound(height, shard, msg) => {
                tonic::Status::internal(format!(
                    "Store not found for height {} and shard {}: {}",
                    height, shard, msg
                ))
            }
            ReplicationError::InternalError(msg) => tonic::Status::internal(msg),
            ReplicationError::InvalidMessage(msg) => {
                tonic::Status::invalid_argument(format!("Invalid message: {}", msg))
            }
        }
    }
}

impl Display for ReplicationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplicationError::ShardStoreNotFound(shard) => {
                write!(f, "Shard store not found for shard {}", shard)
            }
            ReplicationError::StoreNotFound(height, shard, msg) => {
                write!(
                    f,
                    "Store not found for height {} and shard {}: {}",
                    height, shard, msg
                )
            }
            ReplicationError::InternalError(msg) => write!(f, "Internal error: {}", msg),
            ReplicationError::InvalidMessage(msg) => write!(f, "Invalid message: {}", msg),
        }
    }
}
