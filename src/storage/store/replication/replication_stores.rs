use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::{
    storage::{
        store::replication::error::ReplicationError,
        store::stores::{StoreLimits, Stores},
        trie::merkle_trie,
    },
    utils::statsd_wrapper::StatsdClientWrapper,
};

pub struct ReplicationStores {
    shard_stores: HashMap<u32, Stores>,
    read_only_stores: RwLock<HashMap<u64, HashMap<u32, Stores>>>,
    trie_branching_factor: u32,
    statsd_client: StatsdClientWrapper,
}

impl ReplicationStores {
    pub fn new(
        shard_stores: HashMap<u32, Stores>,
        trie_branching_factor: u32,
        statsd_client: StatsdClientWrapper,
    ) -> Self {
        ReplicationStores {
            shard_stores,
            trie_branching_factor,
            statsd_client: statsd_client,
            read_only_stores: RwLock::new(HashMap::new()),
        }
    }

    pub fn get(&self, height: u64, shard: u32) -> Option<Stores> {
        self.read_only_stores
            .read()
            .unwrap()
            .get(&height)
            .and_then(|stores| stores.get(&shard).cloned())
    }

    pub fn open_snapshot(&self, height: u64, shard: u32) -> Result<(), ReplicationError> {
        match self.shard_stores.get(&shard) {
            Some(stores) => match stores.db.open_read_only() {
                Ok(read_only_db) => {
                    let mut stores = self.read_only_stores.write().unwrap();

                    if !stores.contains_key(&height) {
                        stores.insert(height, HashMap::new());
                    }

                    if stores[&height].contains_key(&shard) {
                        return Ok(());
                    }

                    let trie = merkle_trie::MerkleTrie::new(self.trie_branching_factor).unwrap();
                    let store = Stores::new(
                        Arc::new(read_only_db),
                        shard,
                        trie,
                        StoreLimits::default(),
                        self.statsd_client.clone(),
                    );

                    stores.get_mut(&height).unwrap().insert(shard, store);

                    Ok(())
                }
                Err(e) => Err(ReplicationError::InternalError(format!(
                    "Failed to open read-only database for shard {}: {}",
                    shard, e
                ))),
            },
            None => Err(ReplicationError::ShardStoreNotFound(shard)),
        }
    }

    pub fn close_snapshot(&mut self, height: u64) {
        self.read_only_stores.write().unwrap().remove(&height);
    }

    fn close_all_snapshots(&mut self) {
        self.read_only_stores.write().unwrap().clear();
    }
}

impl Drop for ReplicationStores {
    fn drop(&mut self) {
        self.close_all_snapshots();
    }
}
