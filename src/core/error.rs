use crate::storage::db;
use std::error::Error;
use std::fmt::Display;

#[derive(Debug, PartialEq, Clone)]
pub struct NodeError {
    pub code: String,
    pub message: String,
}

impl NodeError {
    pub fn validation_failure(error_message: &str) -> NodeError {
        NodeError {
            code: "bad_request.validation_failure".to_string(),
            message: error_message.to_string(),
        }
    }

    pub fn invalid_parameter(error_message: &str) -> NodeError {
        NodeError {
            code: "bad_request.invalid_param".to_string(),
            message: error_message.to_string(),
        }
    }

    pub fn internal_db_error(error_message: &str) -> NodeError {
        NodeError {
            code: "db.internal_error".to_string(),
            message: error_message.to_string(),
        }
    }

    pub fn not_found(error_message: &str) -> NodeError {
        NodeError {
            code: "not_found".to_string(),
            message: error_message.to_string(),
        }
    }

    pub fn invalid_internal_state(error_message: &str) -> NodeError {
        NodeError {
            code: "invalid_internal_state".to_string(),
            message: error_message.to_string(),
        }
    }
}

impl Error for NodeError {}

impl Display for NodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

// Convert RocksDB errors
impl From<db::RocksdbError> for NodeError {
    fn from(e: db::RocksdbError) -> NodeError {
        NodeError {
            code: "db.internal_error".to_string(),
            message: e.to_string(),
        }
    }
}

impl From<prost::DecodeError> for NodeError {
    fn from(e: prost::DecodeError) -> NodeError {
        NodeError {
            code: "decode.error".to_string(),
            message: e.to_string(),
        }
    }
}
