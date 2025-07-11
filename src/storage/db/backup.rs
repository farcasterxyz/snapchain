use crate::storage::db::multi_chunk_writer::MultiChunkWriter;
use crate::storage::db::{DBProvider, RocksDB, RocksdbError};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};
use tracing::{info, warn};

fn create_tar_gzip(
    input_dir: &str,
    output_dir: &str,
    shard_id: u32,
) -> Result<String, RocksdbError> {
    let base_name = Path::new(input_dir)
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap();

    let chunked_output_dir = Path::new(output_dir)
        .join(format!("{}-{}.tar.gz", base_name, shard_id.to_string()))
        .as_os_str()
        .to_str()
        .unwrap()
        .to_string();

    let start = std::time::SystemTime::now();
    info!(
        output_file_path = &chunked_output_dir,
        base_name = &base_name,
        "Creating chunked tar.gz snapshot for directory: {}",
        input_dir
    );

    let mut multi_chunk_writer = MultiChunkWriter::new(
        PathBuf::from(chunked_output_dir.clone()),
        100 * 1024 * 1024, // 100MB, this is the max size recommended for the S3 [put_object] API
    );

    let mut tar = tar::Builder::new(&mut multi_chunk_writer);
    tar.append_dir_all(base_name, input_dir)?;
    tar.finish()?;
    drop(tar); // Needed so we can call multi_chunk_writer.finish() next
    multi_chunk_writer.finish()?;

    let metadata = fs::metadata(&chunked_output_dir)?;
    let time_taken = start.elapsed().expect("Time went backwards");
    info!(
            "Created chunked tar.gz archive for snapshot: path = {}, size = {} bytes, time taken = {:?}",
            chunked_output_dir,
            metadata.len(),
            time_taken
        );

    Ok(chunked_output_dir)
}

pub fn backup_db(
    db: Arc<RocksDB>,
    backup_dir: &str,
    shard_id: u32,
    timestamp_ms: i64,
) -> Result<String, RocksdbError> {
    let now = chrono::DateTime::from_timestamp_millis(timestamp_ms)
        .unwrap()
        .naive_utc();
    let backup_path = Path::new(backup_dir).join(format!("shard-{}", shard_id));
    let span = tracing::span!(
        tracing::Level::INFO,
        "backup_db",
        path = backup_path.to_str().unwrap()
    );
    let _enter = span.enter();
    info!("Backing up db to {:?}", backup_path);
    if backup_path.exists() {
        warn!("Backup path already exists, removing it");
        fs::remove_dir_all(&backup_path).map_err(|e| RocksdbError::BackupError(e))?;
    }
    let backup_path = backup_path.into_os_string().into_string().unwrap();

    let mut backup_db_options = rocksdb::Options::default();
    backup_db_options.set_compression_type(rocksdb::DBCompressionType::Lz4);
    backup_db_options.create_if_missing(true);

    let backup_db = rocksdb::DB::open(&backup_db_options, &backup_path)
        .map_err(|e| RocksdbError::InternalError(e))?;
    let mut write_options = rocksdb::WriteOptions::default();
    write_options.disable_wal(true); // Significantly faster, WAL doesn't provide benefits for backups
    let mut write_batch = rocksdb::WriteBatch::default();

    let backup_thread = std::thread::spawn(move || {
        let provider = db.db();
        let main_db_snapshot = match provider.as_ref() {
            Some(DBProvider::Transaction(main_db)) => main_db.snapshot(),
            Some(DBProvider::ReadOnly(_)) => {
                warn!("Main database is read-only, cannot create snapshot");
                return;
            }
            None => {
                warn!("Main database is not open, cannot create snapshot");
                return;
            }
        };

        let iterator = main_db_snapshot.iterator(rocksdb::IteratorMode::Start);
        let mut count = 0;
        for item in iterator {
            let (key, value) = item.unwrap();
            write_batch.put(key, value);
            if write_batch.len() >= 10_000 {
                backup_db.write_opt(write_batch, &write_options).unwrap();
                write_batch = rocksdb::WriteBatch::default();
            }

            count += 1;
            if count % 1_000_000 == 0 {
                backup_db.flush().unwrap();
                info!("Snapshot backup progress: {}M keys", count / 1_000_000);
            }
        }

        // write any leftover keys
        backup_db.write_opt(write_batch, &write_options).unwrap();

        info!("Backup completed: {}", count);
        drop(main_db_snapshot);
        drop(backup_db);
    });

    backup_thread.join().unwrap();
    info!(
        "Backup completed, time taken = {:?}",
        chrono::Utc::now().naive_utc() - now
    );
    let output_file = create_tar_gzip(&backup_path, backup_dir, shard_id)?;
    fs::remove_dir_all(&backup_path).map_err(|e| RocksdbError::BackupError(e))?;

    Ok(output_file)
}
