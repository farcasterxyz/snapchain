pub mod error;
pub mod message;
pub mod types;
pub mod util;
pub mod validations;

// Re-export extension traits for convenience
pub use message::HubEventExt;
pub use types::{CommitsExt, FullProposalExt};
