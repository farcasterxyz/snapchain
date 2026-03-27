#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] =
    b"background_thread:true,dirty_decay_ms:5000,muzzy_decay_ms:10000,narenas:16\0";

pub mod bootstrap;
pub mod cfg;
pub mod connectors;
pub mod consensus;
pub mod core;
pub mod jobs;
pub mod mempool;
pub mod network;
pub mod node;
pub mod perf;
pub mod storage;
pub mod utils;
pub mod version;

mod tests;

pub use snapchain_proto::proto;
