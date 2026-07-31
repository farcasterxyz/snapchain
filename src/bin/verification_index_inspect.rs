//! Offline inspector for the verification by-address secondary index and the DB schema
//! version, for exercising the `M2VerificationByAddressIndex` migration on a local node.
//!
//! Why this exists: M2 runs inside `ShardEngine`, i.e. on the *data* shards, but the only
//! production reader of the by-address index is `BlockEngine::resolve_channel_owner_fid`,
//! which reads shard 0. There is therefore no RPC that observes what M2 did, and no way to
//! tell a successful backfill from a no-op without reading the keyspace directly.
//!
//! Key layouts this tool decodes (see `storage::store::account::verification_store`):
//!   primary add : [RootPrefix::User(3)] ++ fid_be32 ++ [UserPostfix::VerificationAdds(93)]
//!                 ++ address           -> 24-byte ts_hash
//!   new index   : [RootPrefix::VerificationByAddress(14)] ++ address ++ fid_be32
//!                                      -> 24-byte ts_hash
//!   legacy slot : [RootPrefix::VerificationByAddress(14)] ++ address
//!                                      -> 4-byte fid            (what M2 sweeps)
//!   schema ver  : [RootPrefix::DBSchemaVersion(13)]             -> 4-byte BE u32
//!   in-progress : [RootPrefix::DBSchemaVersion(13), migration_version]  -> [0] or [1]
//!
//! Read subcommands open a read-only snapshot and do not take the DB lock, so they work
//! against a running node — but a node mid-migration is a moving target, so make hard
//! assertions with it stopped. `arm-wedge` and `inject-legacy` write, and take the exclusive
//! lock: they fail loudly if the node is up, which is the behavior you want.

use clap::{Parser, Subcommand};
use snapchain::storage::constants::{RootPrefix, UserPostfix};
use snapchain::storage::db::RocksDB;
use snapchain::storage::store::account::{make_fid_key, read_fid_key, FID_BYTES, TS_HASH_LENGTH};
use std::collections::{BTreeMap, BTreeSet};
use std::process::ExitCode;

/// `[3] ++ fid_be32 ++ [93]` — everything before the address in a primary adds key.
const PRIMARY_ADDS_HEADER_LEN: usize = 1 + FID_BYTES + 1;

#[derive(Parser)]
#[command(
    name = "verification_index_inspect",
    about = "Inspect the verification by-address index and DB schema version of a shard DB",
    long_about = "Offline inspector for the M2 verification by-address migration. Read \
                  subcommands use a read-only snapshot; arm-wedge and inject-legacy take the \
                  exclusive DB lock and require the node to be stopped."
)]
struct Cli {
    /// RocksDB directory holding the per-shard databases, e.g. `nodes/1/.rocks`. The shard
    /// subdirectory (`shard-<n>`) is appended automatically.
    #[arg(long)]
    db: String,

    /// Shard to inspect. Migrations only ever run on data shards (1..); shard 0 is useful as
    /// a control, since it is never migrated and must never hold legacy rows.
    #[arg(long)]
    shard: u32,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Print the schema version and every migration in-progress flag.
    Status,
    /// Summarize the by-address keyspace and the primary verification adds.
    Counts,
    /// Show the primary adds and index entries for one address, side by side.
    Address(AddressArgs),
    /// Check the index against the primary adds and exit non-zero on a real divergence.
    Verify,
    /// Write a schema version and set a migration's in-progress flag, reproducing the state
    /// a process killed mid-migration leaves behind. Requires the node to be stopped.
    ArmWedge(ArmWedgeArgs),
    /// Write a single legacy-format (address-only) index row. Requires the node to be stopped.
    InjectLegacy(InjectLegacyArgs),
}

#[derive(clap::Args)]
struct AddressArgs {
    /// Hex address, with or without a 0x prefix.
    #[arg(long)]
    address: String,
}

#[derive(clap::Args)]
struct ArmWedgeArgs {
    /// Schema version to write. The runner resumes at `all_migrations[schema_version..]`.
    #[arg(long)]
    schema_version: u32,

    /// Migration whose in-progress flag to set. The runner keys this by the schema version
    /// it started from, not the version it targets — so to simulate a crash during the
    /// migration that upgrades 1 -> 2, pass `--schema-version 1 --in-progress 1`.
    #[arg(long)]
    in_progress: u32,

    /// Required. This mutates the database in place.
    #[arg(long)]
    yes: bool,
}

#[derive(clap::Args)]
struct InjectLegacyArgs {
    /// Hex address, with or without a 0x prefix.
    #[arg(long)]
    address: String,

    /// FID to store in the slot value, as the pre-#958 format did.
    #[arg(long)]
    fid: u64,

    /// Required. This mutates the database in place.
    #[arg(long)]
    yes: bool,
}

// ---------- key helpers ----------

fn schema_version_key() -> Vec<u8> {
    vec![RootPrefix::DBSchemaVersion as u8]
}

fn migration_flag_key(version: u32) -> Vec<u8> {
    vec![RootPrefix::DBSchemaVersion as u8, version as u8]
}

fn by_address_prefix(address: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + address.len());
    key.push(RootPrefix::VerificationByAddress as u8);
    key.extend_from_slice(address);
    key
}

fn parse_hex(s: &str) -> Result<Vec<u8>, String> {
    hex::decode(s.trim().trim_start_matches("0x")).map_err(|e| format!("bad hex: {}", e))
}

// ---------- DB access ----------

fn shard_path(db_dir: &str, shard: u32) -> String {
    format!("{}/shard-{}", db_dir.trim_end_matches('/'), shard)
}

/// Open a read-only snapshot.
///
/// Deliberately not `RocksDB::open_read_only`, which passes `error_if_log_file_exist = true`
/// and so refuses any database with a WAL — that is every database a node has ever written.
fn open_read_only(db_dir: &str, shard: u32) -> Result<rocksdb::DB, String> {
    let path = shard_path(db_dir, shard);
    if std::path::Path::new(&path).join("LOCK").exists() && lock_is_held(&path) {
        eprintln!(
            "warning: {} appears to be open by a running node; this is a stale read-only \
             snapshot. Stop the node before making assertions.",
            path
        );
    }
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(false);
    // Must match how the node opened it (`RocksDB::open`), or reads fail to decompress.
    opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    rocksdb::DB::open_for_read_only(&opts, &path, false)
        .map_err(|e| format!("failed to open {} read-only: {}", path, e))
}

/// A LOCK file always exists once a DB has been opened; only an exclusive open can tell us
/// whether anyone currently holds it. Probe by trying one, and treat success as "not held".
fn lock_is_held(path: &str) -> bool {
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(false);
    opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    match rocksdb::DB::open(&opts, path) {
        Ok(db) => {
            drop(db);
            false
        }
        Err(_) => true,
    }
}

// ---------- scanning ----------

/// One verification, keyed the same way in both the primary store and the index so the two
/// can be compared as sets.
type Entry = (u64, Vec<u8>, Vec<u8>);

#[derive(Default)]
struct IndexScan {
    /// `[14] ++ address ++ fid` -> ts_hash. The post-#958 format.
    new_entries: Vec<Entry>,
    /// `[14] ++ address` -> fid. The pre-#958 format M2 sweeps.
    legacy_slots: Vec<(Vec<u8>, u64)>,
    /// Anything under the prefix matching neither shape. Should always be empty.
    malformed: Vec<Vec<u8>>,
}

fn scan_index(db: &rocksdb::DB) -> IndexScan {
    let mut scan = IndexScan::default();
    let prefix = [RootPrefix::VerificationByAddress as u8];

    for item in db.prefix_iterator(prefix) {
        let Ok((key, value)) = item else { continue };
        if key.first() != Some(&(RootPrefix::VerificationByAddress as u8)) {
            // `prefix_iterator` runs past the prefix once it is exhausted.
            break;
        }

        // Discriminate on the value length, exactly as the migration's sweep does. It is
        // address-length agnostic, which matters because addresses are 20 bytes for Ethereum
        // and 32 for Solana.
        if value.len() == FID_BYTES && key.len() > 1 {
            let address = key[1..].to_vec();
            scan.legacy_slots.push((address, read_fid_key(&value, 0)));
        } else if value.len() == TS_HASH_LENGTH && key.len() > 1 + FID_BYTES {
            let fid_offset = key.len() - FID_BYTES;
            let address = key[1..fid_offset].to_vec();
            scan.new_entries
                .push((read_fid_key(&key, fid_offset), address, value.to_vec()));
        } else {
            scan.malformed.push(key.to_vec());
        }
    }
    scan
}

fn scan_primary_adds(db: &rocksdb::DB) -> Vec<Entry> {
    let mut adds = Vec::new();
    let prefix = [RootPrefix::User as u8];

    for item in db.prefix_iterator(prefix) {
        let Ok((key, value)) = item else { continue };
        if key.first() != Some(&(RootPrefix::User as u8)) {
            break;
        }
        if key.len() <= PRIMARY_ADDS_HEADER_LEN
            || key[1 + FID_BYTES] != UserPostfix::VerificationAdds as u8
        {
            continue;
        }
        adds.push((
            read_fid_key(&key, 1),
            key[PRIMARY_ADDS_HEADER_LEN..].to_vec(),
            value.to_vec(),
        ));
    }
    adds
}

fn hex_short(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

// ---------- subcommands ----------

fn cmd_status(db: &rocksdb::DB) -> Result<(), String> {
    let version = db
        .get(schema_version_key())
        .map_err(|e| e.to_string())?
        .map(|bytes| {
            let mut buf = [0u8; 4];
            // Mirrors `Stores::get_schema_version`, which also tolerates a wrong-sized value.
            if bytes.len() == 4 {
                buf.copy_from_slice(&bytes);
            }
            u32::from_be_bytes(buf)
        });

    match version {
        Some(v) => println!("schema_version: {}", v),
        None => println!("schema_version: 0 (key absent — never migrated)"),
    }

    // The runner only ever keys flags by a schema version, so a small scan covers every one
    // that could exist without hard-coding LATEST_SCHEMA_VERSION.
    let mut any = false;
    for v in 0..16u32 {
        if let Some(bytes) = db.get(migration_flag_key(v)).map_err(|e| e.to_string())? {
            let state = match bytes.first() {
                Some(1) => "SET (a migration started from this version and did not finish)",
                Some(0) => "cleared",
                _ => "unrecognized",
            };
            println!("migration in-progress flag [{}]: {}", v, state);
            any = true;
        }
    }
    if !any {
        println!("migration in-progress flags: none present");
    }
    Ok(())
}

fn cmd_counts(db: &rocksdb::DB) -> Result<(), String> {
    let scan = scan_index(db);
    let adds = scan_primary_adds(db);

    let mut verifiers_per_address: BTreeMap<Vec<u8>, usize> = BTreeMap::new();
    for (_, address, _) in &scan.new_entries {
        *verifiers_per_address.entry(address.clone()).or_default() += 1;
    }
    let distinct_index_addresses = verifiers_per_address.len() + scan.legacy_slots.len();
    let max_verifiers = verifiers_per_address.values().copied().max().unwrap_or(0);
    let distinct_primary_addresses: BTreeSet<_> =
        adds.iter().map(|(_, address, _)| address).collect();

    println!("legacy_slots:                 {}", scan.legacy_slots.len());
    println!("new_entries:                  {}", scan.new_entries.len());
    println!("malformed:                    {}", scan.malformed.len());
    println!("primary_adds:                 {}", adds.len());
    println!("distinct_addresses (index):   {}", distinct_index_addresses);
    println!(
        "distinct_addresses (primary): {}",
        distinct_primary_addresses.len()
    );
    println!("max_verifiers_for_one_address: {}", max_verifiers);
    Ok(())
}

fn cmd_address(db: &rocksdb::DB, address: &[u8]) -> Result<(), String> {
    println!("address: {}", hex_short(address));

    println!("\nprimary VerificationAdds:");
    let adds: Vec<_> = scan_primary_adds(db)
        .into_iter()
        .filter(|(_, addr, _)| addr == address)
        .collect();
    if adds.is_empty() {
        println!("  (none)");
    }
    for (fid, _, ts_hash) in &adds {
        println!("  fid {:<12} ts_hash {}", fid, hex_short(ts_hash));
    }

    println!("\nby-address index:");
    let mut found = false;
    for item in db.prefix_iterator(by_address_prefix(address)) {
        let Ok((key, value)) = item else { continue };
        if !key.starts_with(&by_address_prefix(address)) {
            break;
        }
        found = true;
        if value.len() == FID_BYTES {
            println!(
                "  LEGACY slot          -> fid {}   (M2 has not swept this)",
                read_fid_key(&value, 0)
            );
        } else if value.len() == TS_HASH_LENGTH && key.len() > 1 + FID_BYTES {
            println!(
                "  fid {:<12} ts_hash {}",
                read_fid_key(&key, key.len() - FID_BYTES),
                hex_short(&value)
            );
        } else {
            println!(
                "  MALFORMED key {} value {}",
                hex_short(&key),
                hex_short(&value)
            );
        }
    }
    if !found {
        println!("  (none)");
    }
    Ok(())
}

/// Compare the index against the primary adds.
///
/// The two directions are not symmetric, deliberately:
///   * an add with no index entry (`A \ B`) is always a bug — a lost write,
///   * an index entry with no add (`B \ A`) is tolerated. `get_verifications_by_address`
///     documents that a remove racing the backfill can leave a stale entry behind, which
///     self-heals the next time that (fid, address) is touched. Reported separately so it can
///     be investigated rather than silently failing a run.
fn cmd_verify(db: &rocksdb::DB) -> Result<bool, String> {
    let scan = scan_index(db);
    let adds: BTreeSet<Entry> = scan_primary_adds(db).into_iter().collect();
    let index: BTreeSet<Entry> = scan.new_entries.iter().cloned().collect();

    let missing: Vec<_> = adds.difference(&index).collect();
    let orphaned: Vec<_> = index.difference(&adds).collect();

    let mut ok = true;

    if missing.is_empty() {
        println!("missing index entries (primary \\ index): 0");
    } else {
        ok = false;
        println!(
            "missing index entries (primary \\ index): {}",
            missing.len()
        );
        for (fid, address, ts_hash) in missing.iter().take(20) {
            println!(
                "  fid {:<12} address {} ts_hash {}",
                fid,
                hex_short(address),
                hex_short(ts_hash)
            );
        }
        if missing.len() > 20 {
            println!("  ... {} more", missing.len() - 20);
        }
    }

    println!(
        "stale index entries (index \\ primary): {}  [tolerated; must self-heal on next touch]",
        orphaned.len()
    );
    for (fid, address, _) in orphaned.iter().take(20) {
        println!("  fid {:<12} address {}", fid, hex_short(address));
    }
    if orphaned.len() > 20 {
        println!("  ... {} more", orphaned.len() - 20);
    }

    if scan.legacy_slots.is_empty() {
        println!("legacy slots: 0");
    } else {
        ok = false;
        println!(
            "legacy slots: {}  [migration has not swept these]",
            scan.legacy_slots.len()
        );
    }

    if scan.malformed.is_empty() {
        println!("malformed rows: 0");
    } else {
        ok = false;
        println!("malformed rows: {}", scan.malformed.len());
        for key in scan.malformed.iter().take(20) {
            println!("  {}", hex_short(key));
        }
    }

    println!("\n{}", if ok { "OK" } else { "FAILED" });
    Ok(ok)
}

fn cmd_arm_wedge(db_dir: &str, shard: u32, args: &ArmWedgeArgs) -> Result<(), String> {
    if !args.yes {
        return Err("refusing to write without --yes".to_string());
    }
    let db = RocksDB::open_shard_db(db_dir, shard);
    db.put(&schema_version_key(), &args.schema_version.to_be_bytes())
        .map_err(|e| e.to_string())?;
    db.put(&migration_flag_key(args.in_progress), &[1u8])
        .map_err(|e| e.to_string())?;
    println!(
        "wrote schema_version = {} and set the in-progress flag for migration {}",
        args.schema_version, args.in_progress
    );
    println!(
        "on next startup the runner should log \"Detected an interrupted migration\" and \
         re-run from migration index {}",
        args.schema_version
    );
    Ok(())
}

fn cmd_inject_legacy(
    db_dir: &str,
    shard: u32,
    address: &[u8],
    fid: u64,
    yes: bool,
) -> Result<(), String> {
    if !yes {
        return Err("refusing to write without --yes".to_string());
    }
    if address.is_empty() {
        return Err("address must not be empty".to_string());
    }
    let db = RocksDB::open_shard_db(db_dir, shard);
    db.put(&by_address_prefix(address), &make_fid_key(fid))
        .map_err(|e| e.to_string())?;
    println!(
        "wrote legacy slot {} -> fid {}",
        hex_short(&by_address_prefix(address)),
        fid
    );
    Ok(())
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    let result = match &cli.cmd {
        Cmd::ArmWedge(args) => cmd_arm_wedge(&cli.db, cli.shard, args).map(|_| true),
        Cmd::InjectLegacy(args) => parse_hex(&args.address).and_then(|address| {
            cmd_inject_legacy(&cli.db, cli.shard, &address, args.fid, args.yes).map(|_| true)
        }),
        read_cmd => open_read_only(&cli.db, cli.shard).and_then(|db| match read_cmd {
            Cmd::Status => cmd_status(&db).map(|_| true),
            Cmd::Counts => cmd_counts(&db).map(|_| true),
            Cmd::Address(args) => {
                let address = parse_hex(&args.address)?;
                cmd_address(&db, &address).map(|_| true)
            }
            Cmd::Verify => cmd_verify(&db),
            Cmd::ArmWedge(_) | Cmd::InjectLegacy(_) => unreachable!("handled above"),
        }),
    };

    match result {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(err) => {
            eprintln!("error: {}", err);
            ExitCode::FAILURE
        }
    }
}
