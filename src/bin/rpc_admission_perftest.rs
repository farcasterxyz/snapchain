use snapchain::perf::rpc_admission_perftest;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    rpc_admission_perftest::run().await
}
