pub mod cfg;
pub mod connectors;
pub mod consensus;
pub mod core;
pub mod mempool;
pub mod network;
pub mod node;
pub mod perf;
pub mod storage;
pub mod utils;

mod tests;

pub mod proto {
    tonic::include_proto!("_");
}

pub mod time {
    use crate::core::types::FARCASTER_EPOCH;

    pub fn current_time() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - (FARCASTER_EPOCH / 1000)
    }
}

pub mod background_jobs;
