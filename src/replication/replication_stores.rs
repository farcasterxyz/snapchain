use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use tracing::{error, warn};

use crate::{
    proto,
    replication::error::ReplicationError,
    storage::{
        db::RocksDB,
        store::stores::{StoreLimits, Stores},
        trie::merkle_trie,
    },
    utils::statsd_wrapper::StatsdClientWrapper,
};

type TimestampedStore = (u64, Stores); // (farcaster timestamp, Stores)

pub struct ReplicationStores {
    shard_stores: HashMap<u32, Stores>,
    // Shard -> Height -> TimestampedStore
    read_only_stores: RwLock<HashMap<u32, HashMap<u64, TimestampedStore>>>,
    trie_branching_factor: u32,
    statsd_client: StatsdClientWrapper,
    network: proto::FarcasterNetwork,
}

impl ReplicationStores {
    const SNAPSHOT_METRIC_NAME: &'static str = "replication.snapshot.count";

    pub fn new(
        shard_stores: HashMap<u32, Stores>,
        trie_branching_factor: u32,
        statsd_client: StatsdClientWrapper,
        network: proto::FarcasterNetwork,
    ) -> Self {
        ReplicationStores {
            shard_stores,
            trie_branching_factor,
            statsd_client: statsd_client,
            read_only_stores: RwLock::new(HashMap::new()),
            network,
        }
    }

    pub fn get(&self, shard: u32, height: u64) -> Option<Stores> {
        match self.read_only_stores.read() {
            Ok(stores) => match stores.get(&shard) {
                Some(stores) => stores.get(&height).map(|(_, store)| store.clone()),
                None => None,
            },
            Err(_) => {
                error!("Failed to acquire read lock on read_only_stores");
                None
            }
        }
    }

    pub fn max_height_for_shard(&self, shard_id: u32) -> Option<u64> {
        match self.read_only_stores.read() {
            Ok(stores) => stores
                .get(&shard_id)
                .and_then(|shard_stores| shard_stores.keys().max().cloned()),
            Err(_) => {
                error!("Failed to acquire read lock on read_only_stores");
                None
            }
        }
    }

    fn new_timestamped_store(
        &self,
        shard: u32,
        timestamp: u64,
        read_only_db: RocksDB,
    ) -> TimestampedStore {
        let trie = merkle_trie::MerkleTrie::new(self.trie_branching_factor).unwrap();
        let store = Stores::new(
            Arc::new(read_only_db),
            shard,
            trie,
            StoreLimits::default(),
            self.network.clone(),
            self.statsd_client.clone(),
        );
        (timestamp, store)
    }

    fn insert_snapshot(
        &self,
        shard: u32,
        height: u64,
        timestamp: u64,
        read_only_db: RocksDB,
    ) -> Result<(), ReplicationError> {
        let stores = self.read_only_stores.write();
        if stores.is_err() {
            return Err(ReplicationError::InternalError(
                "Failed to acquire write lock on read_only_stores".to_string(),
            ));
        }
        let mut stores = stores.unwrap();

        if !stores.contains_key(&shard) {
            stores.insert(shard, HashMap::new());
        }

        if stores[&shard].contains_key(&height) {
            return Ok(());
        }

        let timestamped_store = self.new_timestamped_store(shard, timestamp, read_only_db);
        stores
            .get_mut(&shard)
            .unwrap()
            .insert(height, timestamped_store);

        self.capture_snapshot_metrics(&stores);

        Ok(())
    }

    pub fn open_snapshot(
        &self,
        shard: u32,
        height: u64,
        timestamp: u64,
    ) -> Result<(), ReplicationError> {
        let stores = self.shard_stores.get(&shard);
        if stores.is_none() {
            return Err(ReplicationError::ShardStoreNotFound(shard));
        }

        match stores.unwrap().db.open_read_only() {
            Ok(read_only_db) => self.insert_snapshot(shard, height, timestamp, read_only_db),
            Err(e) => Err(ReplicationError::InternalError(format!(
                "Failed to open read-only database for shard {}: {}",
                shard, e
            ))),
        }
    }

    pub fn close_aged_snapshots(&self, shard: u32, min_timestamp: u64) {
        let stores = self.read_only_stores.write();
        if stores.is_err() {
            error!("Failed to acquire write lock on read_only_stores");
            return;
        }
        let mut stores = stores.unwrap();

        match stores.get_mut(&shard) {
            Some(shard_stores) => {
                shard_stores.retain(|&_, &mut (timestamp, _)| timestamp >= min_timestamp)
            }
            None => warn!("Shard {} not found in read_only_stores", shard),
        }

        self.capture_snapshot_metrics(&stores);
    }

    fn close_all_snapshots(&mut self) {
        let stores = self.read_only_stores.write();
        if stores.is_err() {
            error!("Failed to acquire write lock on read_only_stores");
            return;
        }
        let mut stores = stores.unwrap();
        stores.clear();
    }

    fn capture_snapshot_metrics(&self, stores: &HashMap<u32, HashMap<u64, TimestampedStore>>) {
        for (shard, heights) in stores.iter() {
            self.statsd_client.gauge_with_shard(
                shard.clone(),
                Self::SNAPSHOT_METRIC_NAME,
                heights.len() as u64,
            );
        }
    }
}

impl Drop for ReplicationStores {
    fn drop(&mut self) {
        self.close_all_snapshots();
    }
}
