use crate::core::error::HubError;
use crate::storage::db::multi_chunk_writer::MultiChunkWriter;
use crate::storage::util::increment_vec_u8;
use rocksdb::{Options, TransactionDB, DB};
use std::collections::HashMap;
use std::fs::{self};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, RwLockReadGuard};
use thiserror::Error;
use tracing::{info, warn};
use walkdir::WalkDir;

#[derive(Error, Debug)]
pub enum RocksdbError {
    #[error(transparent)]
    InternalError(#[from] rocksdb::Error),

    #[error("Unable to decode message")]
    DecodeError,

    #[error("DB is not open")]
    DbNotOpen,

    #[error(transparent)]
    BackupError(#[from] std::io::Error),
}

/** Hold a transaction. List of key/value pairs that will be committed together */
pub struct RocksDbTransactionBatch {
    pub batch: HashMap<Vec<u8>, Option<Vec<u8>>>,
}

impl RocksDbTransactionBatch {
    pub fn new() -> RocksDbTransactionBatch {
        RocksDbTransactionBatch {
            batch: HashMap::new(),
        }
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.batch.insert(key, Some(value));
    }

    pub fn delete(&mut self, key: Vec<u8>) {
        self.batch.insert(key, None);
    }

    pub fn merge(&mut self, other: RocksDbTransactionBatch) {
        for (key, value) in other.batch {
            self.batch.insert(key, value);
        }
    }

    pub fn len(&self) -> usize {
        self.batch.len()
    }
}

pub struct IteratorOptions {
    pub opts: rocksdb::ReadOptions,
    pub reverse: bool,
}

#[derive(Default)]
pub struct RocksDB {
    pub db: RwLock<Option<rocksdb::TransactionDB>>,
    pub path: String,
}

#[derive(Debug, Default)]
pub struct PageOptions {
    pub page_size: Option<usize>,
    pub page_token: Option<Vec<u8>>,
    pub reverse: bool,
}

impl std::fmt::Debug for RocksDB {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksDB").field("path", &self.path).finish()
    }
}

impl RocksDB {
    pub fn new(path: &str) -> RocksDB {
        info!({ path }, "Creating new RocksDB");

        RocksDB {
            db: RwLock::new(None),
            path: path.to_string(),
        }
    }

    pub fn open_shard_db(db_dir: &str, shard_id: u32) -> Arc<RocksDB> {
        let db = RocksDB::new(format!("{}/shard-{}", db_dir, shard_id).as_str());
        db.open().unwrap();
        Arc::new(db)
    }

    pub fn open_block_db(db_dir: &str) -> Arc<RocksDB> {
        let db = RocksDB::new(format!("{}/block", db_dir).as_str());
        db.open().unwrap();
        Arc::new(db)
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
        let backup_path =
            Path::new(backup_dir).join(format!("backup-{}", now.format("%Y-%m-%d-%s")));
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

        let mut backup_db_options = Options::default();
        backup_db_options.set_compression_type(rocksdb::DBCompressionType::Lz4);

        let backup_db = DB::open(&backup_db_options, &backup_path)
            .map_err(|e| RocksdbError::InternalError(e))?;
        let mut write_options = rocksdb::WriteOptions::default();
        write_options.disable_wal(true); // Significantly faster, WAL doesn't provide benefits for backups
        let mut write_batch = rocksdb::WriteBatch::default();

        let backup_thread = std::thread::spawn(move || {
            let main_db = db.db();
            let main_db_snapshot = main_db.as_ref().unwrap().snapshot();

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
        let output_file = Self::create_tar_gzip(&backup_path, backup_dir, shard_id)?;
        fs::remove_dir_all(&backup_path).map_err(|e| RocksdbError::BackupError(e))?;

        Ok(output_file)
    }

    pub fn create_tar_gzip(
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
            4 * 1024 * 1024 * 1024, // 4GB
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

    pub fn open(&self) -> Result<(), RocksdbError> {
        let mut db_lock = self.db.write().unwrap();

        // Create RocksDB options
        let mut opts = Options::default();
        opts.create_if_missing(true); // Creates a database if it does not exist
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);

        let mut tx_db_opts = rocksdb::TransactionDBOptions::default();
        tx_db_opts.set_default_lock_timeout(5000); // 5 seconds

        // Open the database with multi-threaded support
        let db = rocksdb::TransactionDB::open(&opts, &tx_db_opts, &self.path)?;
        *db_lock = Some(db);

        // We put the db in a RwLock to make the compiler happy, but it is strictly not required.
        // We can use unsafe to replace the value directly, and this will work fine, and shave off
        // 100ns per db read/write operation.
        // eg:
        // unsafe {
        //     let db_ptr = &self.db as *const Option<TransactionDB> as *mut Option<TransactionDB>;
        //     std::ptr::replace(db_ptr, Some(db));
        // }

        Ok(())
    }

    pub fn location(&self) -> String {
        self.path.clone()
    }

    pub fn close(&self) {
        let mut db_lock = self.db.write().unwrap();
        if db_lock.is_some() {
            let db = db_lock.take().unwrap();
            drop(db);
        }

        // See the comment in open(). We strictly don't need to use the RwLock here, but we do it
        // to make the compiler happy. We could use unsafe to replace the value directly, like this:
        // if self.db.is_some() {
        //     let db = unsafe {
        //         let db_ptr = &self.db as *const Option<TransactionDB> as *mut Option<TransactionDB>;
        //         std::ptr::replace(db_ptr, None)
        //     };

        //     // Strictly not needed, but writing so its clear we are dropping the DB here
        //     db.map(|db| drop(db));
        // }
    }

    pub fn destroy(&self) -> Result<(), RocksdbError> {
        self.close();
        let path = Path::new(&self.path);

        let result = rocksdb::DB::destroy(&rocksdb::Options::default(), path);

        // Also rm -rf the directory, ignore any errors
        let _ = fs::remove_dir_all(path);

        result?;

        Ok(())
    }

    pub fn db(&self) -> RwLockReadGuard<'_, Option<TransactionDB>> {
        self.db.read().unwrap()
    }

    pub fn keys_exist(&self, keys: &Vec<Vec<u8>>) -> Vec<bool> {
        let db = self.db();
        let db = db.as_ref().unwrap();

        db.multi_get(keys)
            .into_iter()
            .map(|r| match r {
                Ok(Some(_)) => true,
                Ok(None) => false,
                Err(_) => false,
            })
            .collect::<Vec<_>>()
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RocksdbError> {
        self.db()
            .as_ref()
            .unwrap()
            .get(key)
            .map_err(|e| RocksdbError::InternalError(e))
    }

    pub fn get_many(&self, keys: &Vec<Vec<u8>>) -> Result<Vec<Vec<u8>>, RocksdbError> {
        let results = self.db().as_ref().unwrap().multi_get(keys);

        // If any of the results are Errors, return an error
        let results = results.into_iter().collect::<Result<Vec<_>, _>>()?;
        let results = results
            .into_iter()
            .map(|r| r.unwrap_or(vec![]))
            .collect::<Vec<_>>();

        Ok(results)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), RocksdbError> {
        self.db()
            .as_ref()
            .unwrap()
            .put(key, value)
            .map_err(|e| RocksdbError::InternalError(e))
    }

    pub fn del(&self, key: &[u8]) -> Result<(), RocksdbError> {
        self.db()
            .as_ref()
            .unwrap()
            .delete(key)
            .map_err(|e| RocksdbError::InternalError(e))
    }

    pub fn txn(&self) -> RocksDbTransactionBatch {
        RocksDbTransactionBatch::new()
    }

    pub fn commit(&self, batch: RocksDbTransactionBatch) -> Result<(), RocksdbError> {
        let db = self.db();
        if db.is_none() {
            return Err(RocksdbError::DbNotOpen);
        }

        let txn = db.as_ref().unwrap().transaction();
        for (key, value) in batch.batch {
            if value.is_none() {
                txn.delete(key)?;
            } else {
                txn.put(key, value.unwrap())?;
            }
        }

        txn.commit().map_err(|e| RocksdbError::InternalError(e))
    }

    fn get_iterator_options(
        start_prefix: Option<Vec<u8>>,
        stop_prefix: Option<Vec<u8>>,
        page_options: &PageOptions,
    ) -> IteratorOptions {
        let start_iterator_prefix = match start_prefix {
            None => vec![],
            Some(prefix) => prefix,
        };

        let stop_iterator_prefix = match stop_prefix {
            None => vec![255u8; 32],
            Some(prefix) => prefix,
        };

        let upper_bound = if page_options.reverse {
            if let Some(page_token) = &page_options.page_token {
                page_token.clone()
            } else {
                stop_iterator_prefix.clone()
            }
        } else {
            stop_iterator_prefix.clone()
        };

        let lower_bound = if page_options.reverse {
            start_iterator_prefix
        } else {
            if let Some(page_token) = &page_options.page_token {
                increment_vec_u8(&page_token)
            } else {
                start_iterator_prefix
            }
        };

        let mut opts = rocksdb::ReadOptions::default();
        opts.set_iterate_lower_bound(lower_bound);
        opts.set_iterate_upper_bound(upper_bound);

        IteratorOptions {
            opts,
            reverse: page_options.reverse,
        }
    }

    /**
     * Iterate over all keys with a given prefix.
     * The callback function should return true to stop the iteration, or false to continue.
     */
    pub fn for_each_iterator_by_prefix_paged<F>(
        &self,
        start_prefix: Option<Vec<u8>>,
        stop_prefix: Option<Vec<u8>>,
        page_options: &PageOptions,
        mut f: F,
    ) -> Result<bool, HubError>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool, HubError>,
    {
        let iter_opts = RocksDB::get_iterator_options(start_prefix, stop_prefix, page_options);

        let db = self.db();
        let mut iter = db.as_ref().unwrap().raw_iterator_opt(iter_opts.opts);

        if iter_opts.reverse {
            iter.seek_to_last();
        } else {
            iter.seek_to_first();
        }

        let mut all_done = true;
        let mut count = 0;

        while iter.valid() {
            if let Some((key, value)) = iter.item() {
                if f(&key, &value)? {
                    all_done = false;
                    break;
                }
                if page_options.page_size.is_some() {
                    count += 1;
                    if count >= page_options.page_size.unwrap() {
                        all_done = true;
                        break;
                    }
                }
            }

            if iter_opts.reverse {
                iter.prev();
            } else {
                iter.next();
            }
        }

        Ok(all_done)
    }

    // Same as for_each_iterator_by_prefix above, but does not limit by page size. To be used in
    // cases where higher level callers are doing custom filtering
    pub fn for_each_iterator_by_prefix<F>(
        &self,
        start_prefix: Option<Vec<u8>>,
        stop_prefix: Option<Vec<u8>>,
        page_options: &PageOptions,
        f: F,
    ) -> Result<bool, HubError>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool, HubError>,
    {
        let unbounded_page_options = PageOptions {
            page_size: None,
            page_token: page_options.page_token.clone(),
            reverse: page_options.reverse,
        };

        let all_done = self.for_each_iterator_by_prefix_paged(
            start_prefix,
            stop_prefix,
            &unbounded_page_options,
            f,
        )?;
        Ok(all_done)
    }

    pub fn clear(&self) -> Result<u32, RocksdbError> {
        let mut deleted;

        loop {
            // reset deleted count
            deleted = 0;

            // Iterate over all keys and delete them
            let mut txn = self.txn();
            let db = self.db();

            for item in db.as_ref().unwrap().iterator(rocksdb::IteratorMode::Start) {
                if let Ok((key, _)) = item {
                    txn.delete(key.to_vec());
                    deleted += 1;
                }
            }

            self.commit(txn)?;

            // Check if we deleted anything
            if deleted == 0 {
                break;
            }
        }

        Ok(deleted)
    }

    pub fn approximate_size(&self) -> u64 {
        WalkDir::new(self.location())
            .into_iter()
            .filter_map(Result::ok) // Filter out any Errs and unwrap the Ok values.
            .filter_map(|entry| fs::metadata(entry.path()).ok()) // Attempt to get metadata, filter out Errs.
            .filter(|metadata| metadata.is_file()) // Ensure we only consider files.
            .map(|metadata| metadata.len()) // Extract the file size.
            .sum() // Sum the sizes.
    }

    /**
     * Count the number of keys with a given prefix.
     */
    pub fn count_keys_at_prefix(&self, prefix: Vec<u8>) -> Result<u32, HubError> {
        let iter_opts = RocksDB::get_iterator_options(
            Some(prefix.clone()),
            Some(increment_vec_u8(&prefix.to_vec())),
            &PageOptions::default(),
        );

        let db = self.db();
        let mut iter = db.as_ref().unwrap().raw_iterator_opt(iter_opts.opts);

        let mut count = 0;
        iter.seek_to_first();
        while iter.valid() {
            count += 1;

            iter.next();
        }

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::db::{RocksDB, RocksDbTransactionBatch};

    #[test]
    fn test_merge_rocksdb_transaction() {
        let mut txn1 = RocksDbTransactionBatch::new();

        let mut txn2 = RocksDbTransactionBatch::new();

        // Add some txns to txn2
        txn2.put(b"key1".to_vec(), b"value1".to_vec());
        txn2.put(b"key2".to_vec(), b"value2".to_vec());
        txn2.delete(b"key3".to_vec());

        // Merge txn2 into txn1
        txn1.merge(txn2);

        // Check that txn1 has all the keys from txn2
        assert_eq!(txn1.batch.len(), 3);
        assert_eq!(
            txn1.batch
                .get(&b"key1".to_vec())
                .unwrap()
                .as_ref()
                .unwrap()
                .clone(),
            b"value1".to_vec()
        );
        assert_eq!(
            txn1.batch
                .get(&b"key2".to_vec())
                .unwrap()
                .as_ref()
                .unwrap()
                .clone(),
            b"value2".to_vec()
        );
        assert_eq!(txn1.batch.get(&b"key3".to_vec()).unwrap().is_none(), true);

        // Add some more txns to txn3
        let mut txn3 = RocksDbTransactionBatch::new();
        txn3.put(b"key4".to_vec(), b"value4".to_vec());
        txn3.put(b"key5".to_vec(), b"value5".to_vec());

        // Merge txn3 into txn1
        txn1.merge(txn3);

        // Check that txn1 has all the keys from txn2 and txn3
        assert_eq!(txn1.batch.len(), 5);
        assert_eq!(
            txn1.batch
                .get(&b"key1".to_vec())
                .unwrap()
                .as_ref()
                .unwrap()
                .clone(),
            b"value1".to_vec()
        );
        assert_eq!(
            txn1.batch
                .get(&b"key4".to_vec())
                .unwrap()
                .as_ref()
                .unwrap()
                .clone(),
            b"value4".to_vec()
        );

        // Add some more txns to txn4 that overwrite existing keys
        let mut txn4 = RocksDbTransactionBatch::new();

        txn4.put(b"key1".to_vec(), b"value1_new".to_vec());
        txn4.put(b"key4".to_vec(), b"value4_new".to_vec());
        txn4.delete(b"key5".to_vec());

        // Merge txn4 into txn1
        txn1.merge(txn4);

        // Check that txn1 has all the keys from txn2 and txn3, and the overwritten keys from txn4
        assert_eq!(txn1.batch.len(), 5);
        assert_eq!(
            txn1.batch
                .get(&b"key1".to_vec())
                .unwrap()
                .as_ref()
                .unwrap()
                .clone(),
            b"value1_new".to_vec()
        );
        assert_eq!(
            txn1.batch
                .get(&b"key2".to_vec())
                .unwrap()
                .as_ref()
                .unwrap()
                .clone(),
            b"value2".to_vec()
        );
        assert_eq!(
            txn1.batch
                .get(&b"key4".to_vec())
                .unwrap()
                .as_ref()
                .unwrap()
                .clone(),
            b"value4_new".to_vec()
        );
        assert_eq!(txn1.batch.get(&b"key5".to_vec()).unwrap().is_none(), true);
    }

    #[test]
    fn test_count_keys_at_prefix() {
        let tmp_path = tempfile::tempdir()
            .unwrap()
            .path()
            .as_os_str()
            .to_string_lossy()
            .to_string();
        let db = RocksDB::new(&tmp_path);
        db.open().unwrap();

        // Add some keys
        db.put(b"key100", b"value1").unwrap();
        db.put(b"key101", b"value3").unwrap();
        db.put(b"key104", b"value4").unwrap();
        db.put(b"key200", b"value2").unwrap();

        // Count all keys
        let count = db.count_keys_at_prefix(b"key".to_vec());
        assert_eq!(count.unwrap(), 4);

        // Count keys at prefix
        let count = db.count_keys_at_prefix(b"key1".to_vec());
        assert_eq!(count.unwrap(), 3);

        // Count keys at prefix with a specific prefix that doesn't exist
        let count = db.count_keys_at_prefix(b"key11".to_vec());
        assert_eq!(count.unwrap(), 0);

        // Count keys at prefix with a specific sub prefix
        let count = db.count_keys_at_prefix(b"key10".to_vec());
        assert_eq!(count.unwrap(), 3);

        // Count keys at prefix with a specific prefix
        let count = db.count_keys_at_prefix(b"key200".to_vec());
        assert_eq!(count.unwrap(), 1);

        // Count keys at prefix with a specific prefix that doesn't exist
        let count = db.count_keys_at_prefix(b"key201".to_vec());
        assert_eq!(count.unwrap(), 0);

        // Cleanup
        db.destroy().unwrap();
    }

    #[test]
    fn test_keys_exist_in_db() {
        let tmp_path = tempfile::tempdir()
            .unwrap()
            .path()
            .as_os_str()
            .to_string_lossy()
            .to_string();
        let db = crate::storage::db::RocksDB::new(&tmp_path);
        db.open().unwrap();

        // Add some keys
        db.put(b"key100", b"value1").unwrap();
        db.put(b"key101", b"value3").unwrap();
        db.put(b"key104", b"value4").unwrap();
        db.put(b"key200", b"value2").unwrap();

        // Check if keys exist
        let exists = db.keys_exist(&vec![b"key100".to_vec(), b"key101".to_vec()]);
        assert_eq!(exists, vec![true, true]);

        // Check if keys exist with a key that doesn't exist
        let exists = db.keys_exist(&vec![
            b"key100".to_vec(),
            b"key101".to_vec(),
            b"key102".to_vec(),
        ]);
        assert_eq!(exists, vec![true, true, false]);

        // Check if keys exist with a key that doesn't exist
        let exists = db.keys_exist(&vec![
            b"key100".to_vec(),
            b"key101".to_vec(),
            b"key102".to_vec(),
            b"key200".to_vec(),
        ]);
        assert_eq!(exists, vec![true, true, false, true]);

        // No keys should return an empty array
        let exists = db.keys_exist(&vec![]);
        assert_eq!(exists.len(), 0);

        // Cleanup
        db.destroy().unwrap();
    }
}
