use crate::core::error::HubError;
use crate::storage::util::increment_vec_u8;
use rocksdb::{Options, TransactionDB, WriteOptions, DB};
use std::collections::HashMap;
use std::fs::{self};
use std::path::Path;
use std::sync::{Arc, RwLock, RwLockReadGuard};
use thiserror::Error;
use tokio::time::Duration;
use tracing::info;
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

    #[error("DB is read only")]
    ReadOnly,
}

/** Hold a transaction. List of key/value pairs that will be committed together */
#[derive(Clone)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SnapchainDbOptimizationType {
    #[default]
    Default, // Default read-write DB
    BulkWriteOptimized, // Optimized for very large bulk writes
}

pub enum DBProvider {
    Transaction(TransactionDB),
    ReadOnly(DB),
}

#[derive(Default)]
pub struct RocksDB {
    inner: RwLock<Option<DBProvider>>,

    pub path: String,
    pub db_options_type: SnapchainDbOptimizationType,
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
        info!({ path }, "Opening RocksDB database");

        RocksDB {
            inner: RwLock::new(None),
            path: path.to_string(),
            db_options_type: SnapchainDbOptimizationType::Default,
        }
    }

    pub fn new_with_options(path: &str, db_options_type: SnapchainDbOptimizationType) -> RocksDB {
        info!({ path }, "Opening RocksDB database");

        RocksDB {
            inner: RwLock::new(None),
            path: path.to_string(),
            db_options_type,
        }
    }

    pub fn open_shard_db(db_dir: &str, shard_id: u32) -> Arc<RocksDB> {
        let db = RocksDB::new(format!("{}/shard-{}", db_dir, shard_id).as_str());
        db.open().unwrap();
        Arc::new(db)
    }

    pub fn open_global_db(db_dir: &str) -> Arc<RocksDB> {
        let db = RocksDB::new(format!("{}/global", db_dir).as_str());
        db.open().unwrap();
        Arc::new(db)
    }

    pub fn open(&self) -> Result<(), RocksdbError> {
        let mut inner = self.inner.write().unwrap();

        // Create RocksDB options
        let mut opts = Options::default();

        if self.db_options_type == SnapchainDbOptimizationType::BulkWriteOptimized {
            // 1. Increase the write buffer size. This is the size of a single in-memory memtable.
            // For large, sustained writes, a larger buffer (e.g., 128MB) reduces how often
            // the database flushes to disk
            opts.set_write_buffer_size(512 * 1024 * 1024);

            // 2. Increase the number of background threads for flushes and compactions.
            opts.increase_parallelism(8);

            // 3. Set the maximum number of write buffers. This allows RocksDB to continue
            // accepting writes into a new memtable while old ones are being flushed.
            opts.set_max_write_buffer_number(4);

            // 4. Set the minimum number of write buffers to merge before flushing. 2
            // allows us to "double-buffer" writes, allowing for more efficient flush-to-disk.
            opts.set_min_write_buffer_number_to_merge(2);
        }

        opts.create_if_missing(true); // Creates a database if it does not exist
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);

        let mut tx_db_opts = rocksdb::TransactionDBOptions::default();
        tx_db_opts.set_default_lock_timeout(5000); // 5 seconds

        // Open the database with multi-threaded support
        let db = rocksdb::TransactionDB::open(&opts, &tx_db_opts, &self.path)?;
        *inner = Some(DBProvider::Transaction(db));

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

    pub fn open_read_only(&self) -> Result<Self, RocksdbError> {
        let _wl = self.inner.write().unwrap();

        let mut opts = Options::default();
        opts.create_if_missing(false);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);

        match rocksdb::DB::open_for_read_only(&opts, self.path.clone(), true) {
            Ok(db) => {
                let provider = DBProvider::ReadOnly(db);
                let rdb = RocksDB {
                    inner: RwLock::new(Some(provider)),
                    path: self.path.clone(),
                    db_options_type: SnapchainDbOptimizationType::default(),
                };
                Ok(rdb)
            }
            Err(e) => Err(RocksdbError::InternalError(e)),
        }
    }

    pub fn close(&self) {
        let mut inner = self.inner.write().unwrap();
        if inner.is_some() {
            let db = inner.take().unwrap();
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

    pub fn db(&self) -> RwLockReadGuard<'_, Option<DBProvider>> {
        self.inner.read().unwrap()
    }

    pub fn location(&self) -> String {
        self.path.clone()
    }

    pub fn keys_exist(&self, keys: &Vec<Vec<u8>>) -> Vec<bool> {
        match self.db().as_ref() {
            Some(DBProvider::Transaction(db)) => db.multi_get(keys),
            Some(DBProvider::ReadOnly(db)) => db.multi_get(keys),
            None => vec![],
        }
        .into_iter()
        .map(|result| match result {
            Ok(Some(_)) => true,
            Ok(None) => false,
            Err(_) => false,
        })
        .collect()
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RocksdbError> {
        match self.db().as_ref() {
            Some(DBProvider::Transaction(db)) => db.get(key),
            Some(DBProvider::ReadOnly(db)) => db.get(key),
            None => return Err(RocksdbError::DbNotOpen),
        }
        .map_err(|e| RocksdbError::InternalError(e))
    }

    pub fn get_many(&self, keys: &Vec<Vec<u8>>) -> Result<Vec<Vec<u8>>, RocksdbError> {
        let results = match self.db().as_ref() {
            Some(DBProvider::Transaction(db)) => db.multi_get(keys),
            Some(DBProvider::ReadOnly(db)) => db.multi_get(keys),
            None => return Err(RocksdbError::DbNotOpen),
        };

        // If any of the results are Errors, return an error
        let results = results.into_iter().collect::<Result<Vec<_>, _>>()?;
        let results = results
            .into_iter()
            .map(|r| r.unwrap_or(vec![]))
            .collect::<Vec<_>>();

        Ok(results)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), RocksdbError> {
        match self.db().as_ref() {
            Some(DBProvider::Transaction(db)) => db.put(key, value),
            Some(DBProvider::ReadOnly(_)) => return Err(RocksdbError::ReadOnly),
            None => return Err(RocksdbError::DbNotOpen),
        }
        .map_err(|e| RocksdbError::InternalError(e))
    }

    pub fn del(&self, key: &[u8]) -> Result<(), RocksdbError> {
        match self.db().as_ref() {
            Some(DBProvider::Transaction(db)) => db.delete(key),
            Some(DBProvider::ReadOnly(_)) => return Err(RocksdbError::ReadOnly),
            None => return Err(RocksdbError::DbNotOpen),
        }
        .map_err(|e| RocksdbError::InternalError(e))
    }

    pub fn txn(&self) -> RocksDbTransactionBatch {
        RocksDbTransactionBatch::new()
    }

    pub fn commit(&self, batch: RocksDbTransactionBatch) -> Result<(), RocksdbError> {
        match self.db().as_ref() {
            Some(DBProvider::Transaction(db)) => {
                let txn = if self.db_options_type == SnapchainDbOptimizationType::BulkWriteOptimized
                {
                    // Create a WriteOptions object for this specific transaction.
                    let mut write_opts = WriteOptions::default();

                    // Disabling the WAL (Write Ahead Log) provides a significant performance boost
                    // for bulk writes. If you have BulkWriteOptimized enabled, this can greatly improve
                    // write performance, but a crash means recent data will be lost and need to be re-written.
                    write_opts.disable_wal(true);

                    // Use the custom write options to create the transaction.
                    db.transaction_opt(&write_opts, &Default::default())
                } else {
                    db.transaction()
                };

                for (key, value) in batch.batch {
                    if value.is_none() {
                        txn.delete(key)?;
                    } else {
                        txn.put(key, value.unwrap())?;
                    }
                }

                txn.commit().map_err(|e| RocksdbError::InternalError(e))
            }
            Some(DBProvider::ReadOnly(_)) => Err(RocksdbError::ReadOnly),
            None => Err(RocksdbError::DbNotOpen),
        }
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
                // In RocksDB, the lexicographically next key after [1,2,3] is [1,2,3,0] (and NOT [1,2,4])
                // This is because RocksDB uses a prefix-based storage engine, and the next key is determined by the last byte
                // with smaller keys sorting BEFORE longer keys with the same prefix
                [page_token.clone(), vec![0u8]].concat()
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

        // TODO: maybe write a generic function to handle both branches
        match self.db().as_ref() {
            Some(DBProvider::Transaction(db)) => {
                let mut iter = db.raw_iterator_opt(iter_opts.opts);

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
            Some(DBProvider::ReadOnly(db)) => {
                let mut iter = db.raw_iterator_opt(iter_opts.opts);

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
            None => return Err(RocksdbError::DbNotOpen.into()),
        }
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

            match self.db().as_ref() {
                Some(DBProvider::Transaction(db)) => {
                    for item in db.iterator(rocksdb::IteratorMode::Start) {
                        if let Ok((key, _)) = item {
                            txn.delete(key.to_vec());
                            deleted += 1;
                        }
                    }
                }
                Some(DBProvider::ReadOnly(db)) => {
                    for item in db.iterator(rocksdb::IteratorMode::Start) {
                        if let Ok((key, _)) = item {
                            txn.delete(key.to_vec());
                            deleted += 1;
                        }
                    }
                }
                None => return Err(RocksdbError::DbNotOpen),
            };

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

        let mut count = 0;

        match self.db().as_ref() {
            Some(DBProvider::Transaction(db)) => {
                let mut iter = db.raw_iterator_opt(iter_opts.opts);
                iter.seek_to_first();
                while iter.valid() {
                    count += 1;
                    iter.next();
                }
            }
            Some(DBProvider::ReadOnly(db)) => {
                let mut iter = db.raw_iterator_opt(iter_opts.opts);
                iter.seek_to_first();
                while iter.valid() {
                    count += 1;
                    iter.next();
                }
            }
            None => return Err(RocksdbError::DbNotOpen.into()),
        };

        Ok(count)
    }

    // Returns finds the first key with the prefix index_prefix after start,
    // using the value to index into the db, returning the value.
    pub fn get_next_by_index(
        &self,
        index_prefix: Vec<u8>,
        start: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, HubError> {
        let page_options = PageOptions {
            page_size: Some(1),
            ..PageOptions::default()
        };
        let mut primary_key: Option<Vec<u8>> = None;
        self.for_each_iterator_by_prefix(
            Some(start),
            Some(increment_vec_u8(&index_prefix)), // avoid overflowing to another index
            &page_options,
            |_, index| {
                primary_key = Some(index.to_vec());
                Ok(true) // Stop iterating after the first key
            },
        )?;
        primary_key
            .map(|primary_key| {
                self.get(&primary_key)
                    .map_err(|e| HubError::from(e))?
                    .ok_or(HubError::not_found("No value found for the given key"))
            })
            .transpose()
    }

    // Deletes keys in the given prefix range, respecting page_options.
    // Returns the number of keys deleted.
    pub fn delete_page(
        &self,
        start_prefix: Option<Vec<u8>>,
        stop_prefix: Option<Vec<u8>>,
        page_options: &PageOptions,
    ) -> Result<u32, HubError> {
        let mut txn = self.txn();
        self.for_each_iterator_by_prefix_paged(
            start_prefix,
            stop_prefix,
            page_options,
            |key, _| {
                txn.delete(key.to_vec());
                Ok(false) // Continue iterating
            },
        )?;

        let count = txn.len();
        self.commit(txn)?;
        Ok(count as u32)
    }

    // Deletes keys in the given prefix range, respecting page_options.
    // This function will keep deleting pages until there are no more keys to delete,
    // or until shutdown is requested via the shutdown_rx channel.
    // The progress_callback function can be used to report progress.
    // The throttle parameter can be used to control the rate of deletion.
    // Returns the total number of keys deleted.
    pub async fn delete_paginated(
        &self,
        start_prefix: Option<Vec<u8>>,
        stop_prefix: Option<Vec<u8>>,
        page_options: &PageOptions,
        throttle: Duration,
        progress_callback: Option<impl Fn(u32) + Send>,
    ) -> Result<u32, HubError> {
        let mut total_deleted = 0;
        loop {
            match self.delete_page(start_prefix.clone(), stop_prefix.clone(), page_options)? {
                0 => break, // No more keys to delete
                count => total_deleted += count,
            }

            if let Some(callback) = &progress_callback {
                callback(total_deleted);
            }
            tokio::time::sleep(throttle).await;
        }

        Ok(total_deleted)
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::{
        db::{PageOptions, RocksDB, RocksDbTransactionBatch},
        util::increment_vec_u8,
    };

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

    #[test]
    fn test_get_next_by_index() {
        let tmp_path = tempfile::tempdir()
            .unwrap()
            .path()
            .as_os_str()
            .to_string_lossy()
            .to_string();
        let db = crate::storage::db::RocksDB::new(&tmp_path);
        db.open().unwrap();

        let key = b"key100";
        let value = b"value1";

        let index_prefix = b"index";
        let index = b"index100";

        db.put(key, value).expect("Failed to put key");
        db.put(index, key).expect("Failed to put index");

        // Get by exact index
        let got = db
            .get_next_by_index(index_prefix.to_vec(), index.to_vec())
            .expect("Failed to get next by index")
            .expect("No value found for the given key");
        assert_eq!(got, value.to_vec());

        // Get next index
        let query = b"index099";
        let got = db
            .get_next_by_index(index_prefix.to_vec(), query.to_vec())
            .expect("Failed to get next by index")
            .expect("No value found for the given key");
        assert_eq!(got, value.to_vec());

        // Ensure lookup does not overflow to another index
        let other_index = increment_vec_u8(&index_prefix.to_vec()); // "indey"
        db.put(other_index.as_slice(), key)
            .expect("Failed to put other index");

        let query = b"index101";
        let got = db
            .get_next_by_index(index_prefix.to_vec(), query.to_vec())
            .expect("Failed to get next by index");
        assert!(got.is_none());

        // Cleanup
        db.destroy().unwrap();
        db.open().unwrap();
    }

    // Helper function to test pagination through keys
    fn test_pagination_helper(
        db: &RocksDB,
        expected_total_keys: usize,
        page_size: usize,
        start_prefix: Option<Vec<u8>>,
        stop_prefix: Option<Vec<u8>>,
    ) {
        let mut last_key = None;
        let mut processed_keys = 0;

        loop {
            let page_options = PageOptions {
                page_size: Some(page_size),
                page_token: last_key.clone(),
                reverse: false,
            };

            let mut processed_this_pass = 0;
            let all_done = db
                .for_each_iterator_by_prefix_paged(
                    start_prefix.clone(),
                    stop_prefix.clone(),
                    &page_options,
                    |key, _value| {
                        processed_this_pass += 1;

                        if processed_this_pass == page_size {
                            last_key = Some(key.to_vec());
                            return Ok(true); // stop
                        }

                        return Ok(false); // continue
                    },
                )
                .unwrap();
            processed_keys += processed_this_pass;

            if all_done {
                assert_eq!(processed_this_pass, expected_total_keys % page_size);
                break;
            } else {
                assert_eq!(processed_this_pass, page_size);
            }
        }

        assert_eq!(processed_keys, expected_total_keys);
    }

    #[test]
    fn test_iteration_pages_through_nested_keys() {
        let tmp_path = tempfile::tempdir()
            .unwrap()
            .path()
            .as_os_str()
            .to_string_lossy()
            .to_string();
        let db = crate::storage::db::RocksDB::new(&tmp_path);
        db.open().unwrap();

        // Add 10 nested keys: [0], [0,1], [0,1,2], ..., [0,1,2,3,4,5,6,7,8,9]
        for i in 0..10 {
            let key: Vec<u8> = (0..=i).collect();
            let value = format!("value{}", i).into_bytes();
            db.put(&key, &value).unwrap();
        }

        test_pagination_helper(&db, 10, 3, Some(vec![0u8]), Some(vec![1u8]));
    }

    #[test]
    fn test_iteration_pages_through_serial_keys() {
        let tmp_path = tempfile::tempdir()
            .unwrap()
            .path()
            .as_os_str()
            .to_string_lossy()
            .to_string();
        let db = crate::storage::db::RocksDB::new(&tmp_path);
        db.open().unwrap();

        // Add 10 serial keys: [0,0,1], [0,0,2], [0,0,3], ..., [0,0,10]
        for i in 1..=10 {
            let key = vec![0u8, 0u8, i as u8];
            let value = format!("value{}", i).into_bytes();
            db.put(&key, &value).unwrap();
        }

        test_pagination_helper(&db, 10, 3, Some(vec![0u8]), Some(vec![1u8]));
    }
}
