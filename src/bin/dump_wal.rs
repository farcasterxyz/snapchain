use std::error::Error;

use informalsystems_malachitebft_engine::wal::log_entries;
use informalsystems_malachitebft_wal as wal;
use snapchain::consensus::malachite::snapchain_codec::SnapchainCodec;

fn main() -> Result<(), Box<dyn Error>> {
    let Some(wal_file) = std::env::args().nth(1) else {
        eprintln!("Usage: dump_wal <wal_file>");
        std::process::exit(1);
    };

    let mut log = wal::Log::open(&wal_file)?;
    let len = log.len();

    println!("WAL Dump");
    println!("- Entries: {len}");
    println!("- Size:    {} bytes", log.size_bytes().unwrap_or(0));
    println!("Entries:");

    let mut count = 0;

    for (idx, entry) in log_entries(&mut log, &SnapchainCodec)?.enumerate() {
        count += 1;

        match entry {
            Ok(entry) => {
                println!("- #{idx}: {entry:?}");
            }
            Err(e) => {
                println!("- #{idx}: Error decoding WAL entry: {e}");
            }
        }
    }

    if count != len {
        println!("Expected {len} entries, but found {count} entries");
    }

    Ok(())
}
