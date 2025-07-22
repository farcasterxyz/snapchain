use std::fmt::Display;

pub enum ReplicationError {
    ShardStoreNotFound(u32),         // shard
    StoreNotFound(u32, u64, String), // shard, height, message
    InternalError(String),           // message
    InvalidMessage(String),          // message
    TimestampTooOld(u32, u64, u64),  // shard, height, timestamp
}

impl From<ReplicationError> for tonic::Status {
    fn from(err: ReplicationError) -> Self {
        match err {
            ReplicationError::ShardStoreNotFound(shard) => {
                tonic::Status::internal(format!("Shard store not found for shard {}", shard))
            }
            ReplicationError::StoreNotFound(shard, height, msg) => {
                tonic::Status::internal(format!(
                    "Store not found for shard {} and height {}: {}",
                    shard, height, msg
                ))
            }
            ReplicationError::InternalError(msg) => tonic::Status::internal(msg),
            ReplicationError::InvalidMessage(msg) => {
                tonic::Status::invalid_argument(format!("Invalid message: {}", msg))
            }
            ReplicationError::TimestampTooOld(shard, height, timestamp) => {
                tonic::Status::failed_precondition(format!(
                    "Timestamp too old for shard {}, height {}, timestamp {}",
                    shard, height, timestamp
                ))
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
            ReplicationError::StoreNotFound(shard, height, msg) => {
                write!(
                    f,
                    "Store not found for shard {} and height {}: {}",
                    shard, height, msg
                )
            }
            ReplicationError::InternalError(msg) => write!(f, "Internal error: {}", msg),
            ReplicationError::InvalidMessage(msg) => write!(f, "Invalid message: {}", msg),
            ReplicationError::TimestampTooOld(shard, height, timestamp) => {
                write!(
                    f,
                    "Timestamp too old for shard {}, height {}, timestamp {}",
                    shard, height, timestamp
                )
            }
        }
    }
}
