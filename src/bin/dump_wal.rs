use std::error::Error;
use std::marker::PhantomData;

use informalsystems_malachitebft_core_types::Context;
use informalsystems_malachitebft_engine::wal::{WalCodec, WalEntry};
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

pub fn log_entries<'a, Ctx, Codec>(
    log: &'a mut wal::Log,
    codec: &'a Codec,
) -> Result<WalIter<'a, Ctx, Codec>, Box<dyn Error>>
where
    Ctx: Context,
    Codec: WalCodec<Ctx>,
{
    Ok(WalIter {
        iter: log.iter()?,
        codec,
        _marker: PhantomData,
    })
}

pub struct WalIter<'a, Ctx, Codec> {
    iter: wal::LogIter<'a>,
    codec: &'a Codec,
    _marker: PhantomData<Ctx>,
}

impl<Ctx, Codec> Iterator for WalIter<'_, Ctx, Codec>
where
    Ctx: Context,
    Codec: WalCodec<Ctx>,
{
    type Item = Result<WalEntry<Ctx>, Box<dyn Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        let entry = self.iter.next()?;
        match entry {
            Ok(bytes) => {
                let buf = std::io::Cursor::new(bytes);
                let entry = WalEntry::decode(self.codec, buf);
                Some(entry.map_err(Into::into))
            }
            Err(e) => Some(Err(e.into())),
        }
    }
}
