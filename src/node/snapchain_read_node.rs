use crate::consensus::consensus::{Config, MalachiteEventShard, SystemMessage};
use crate::consensus::malachite::network_connector::MalachiteNetworkEvent;
use crate::consensus::malachite::spawn_read_node::MalachiteReadNodeActors;
use crate::consensus::read_validator::Engine;
use crate::core::types::{Address, ShardId, SnapchainShard, SnapchainValidatorContext};
use crate::mempool::mempool::MempoolMessagesRequest;
use crate::network::gossip::GossipEvent;
use crate::proto;
use crate::storage::db::RocksDB;
use crate::storage::store::engine::{BlockEngine, Senders, ShardEngine};
use crate::storage::store::stores::StoreLimits;
use crate::storage::store::stores::Stores;
use crate::storage::store::BlockStore;
use crate::storage::trie::merkle_trie;
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use informalsystems_malachitebft_metrics::SharedRegistry;
use libp2p::identity::ed25519::Keypair;
use libp2p::PeerId;
use std::collections::{BTreeMap, HashMap};
use tokio::sync::mpsc;
use tracing::warn;

const MAX_SHARDS: u32 = 64;

#[derive(Clone)]
pub struct SnapchainReadNode {
    pub consensus_actors: BTreeMap<u32, MalachiteReadNodeActors>,
    pub shard_stores: HashMap<u32, Stores>,
    pub shard_senders: HashMap<u32, Senders>,
    pub address: Address,
}

impl SnapchainReadNode {
    pub async fn create(
        keypair: Keypair,
        config: Config,
        local_peer_id: PeerId,
        gossip_tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
        system_tx: mpsc::Sender<SystemMessage>,
        messages_request_tx: mpsc::Sender<MempoolMessagesRequest>,
        block_store: BlockStore,
        rocksdb_dir: String,
        statsd_client: StatsdClientWrapper,
        trie_branching_factor: u32,
        registry: &SharedRegistry,
    ) -> Self {
        let validator_address = Address(keypair.public().to_bytes());

        let mut consensus_actors = BTreeMap::new();

        let mut shard_senders: HashMap<u32, Senders> = HashMap::new();
        let mut shard_stores: HashMap<u32, Stores> = HashMap::new();

        // Create the shard validators
        for shard_id in config.shard_ids.clone() {
            if shard_id == 0 {
                panic!("Shard ID 0 is reserved for the block shard, created automaticaly");
            } else if shard_id > MAX_SHARDS {
                panic!("Shard ID must be between 1 and {}", MAX_SHARDS);
            }

            let ctx = SnapchainValidatorContext::new(keypair.clone());

            let db = RocksDB::open_shard_db(rocksdb_dir.clone().as_str(), shard_id);
            let trie = merkle_trie::MerkleTrie::new(trie_branching_factor).unwrap(); //TODO: don't unwrap()
            let engine = ShardEngine::new(
                db.clone(),
                trie,
                shard_id,
                StoreLimits::default(),
                statsd_client.clone(),
                config.max_messages_per_block,
                Some(messages_request_tx.clone()),
            );

            shard_senders.insert(shard_id, engine.get_senders());
            shard_stores.insert(shard_id, engine.get_stores());

            let consensus_actor = MalachiteReadNodeActors::create_and_start(
                ctx,
                Engine::ShardEngine(engine),
                local_peer_id,
                gossip_tx.clone(),
                system_tx.clone(),
                registry,
                shard_id,
                statsd_client.clone(),
            )
            .await;

            if consensus_actor.is_err() {
                panic!("Failed to create consensus actor for shard {}", shard_id);
            }

            consensus_actors.insert(shard_id, consensus_actor.unwrap());
        }

        // Now create the block validator
        let block_shard = SnapchainShard::new(0);

        // We might want to use different keys for the block shard so signatures are different and cannot be accidentally used in the wrong shard
        let engine = BlockEngine::new(block_store.clone(), statsd_client.clone());
        let ctx = SnapchainValidatorContext::new(keypair.clone());
        let block_actor = MalachiteReadNodeActors::create_and_start(
            ctx,
            Engine::BlockEngine(engine),
            local_peer_id,
            gossip_tx.clone(),
            system_tx.clone(),
            registry,
            block_shard.shard_id(),
            statsd_client.clone(),
        )
        .await;
        if block_actor.is_err() {
            panic!("Failed to create consensus actor for block shard");
        }
        consensus_actors.insert(0, block_actor.unwrap());

        Self {
            consensus_actors,
            address: validator_address,
            shard_senders,
            shard_stores,
        }
    }

    pub fn id(&self) -> String {
        self.address.prefix()
    }

    pub fn stop(&self) {
        // Stop all actors
        for (_, actor) in self.consensus_actors.iter() {
            actor.stop();
        }
    }
    pub fn dispatch_decided_value(&self, decided_value: proto::DecidedValue) {
        let shard_id = match decided_value.value.as_ref().unwrap() {
            proto::decided_value::Value::Shard(shard_chunk) => {
                shard_chunk
                    .header
                    .as_ref()
                    .unwrap()
                    .height
                    .unwrap()
                    .shard_index
            }
            proto::decided_value::Value::Block(block) => {
                block.header.as_ref().unwrap().height.unwrap().shard_index
            }
        };
        let actors = self.consensus_actors.get(&shard_id).unwrap();
        actors.cast_decided_value(decided_value).unwrap();
    }

    pub fn dispatch_network_event(&self, shard: MalachiteEventShard, event: MalachiteNetworkEvent) {
        match shard {
            MalachiteEventShard::None => {
                for (shard_index, actor) in self.consensus_actors.iter() {
                    let result = actor.cast_network_event(event.clone());
                    if let Err(e) = result {
                        warn!(
                            "Failed to forward message to actor: {:?} at shard: {:?}",
                            e, shard_index
                        );
                    }
                }
            }
            MalachiteEventShard::Shard(shard_index) => {
                if let Some(actor) = self.consensus_actors.get(&shard_index) {
                    let result = actor.cast_network_event(event);
                    if let Err(e) = result {
                        warn!("Failed to forward message to actor: {:?}", e);
                    }
                } else {
                    warn!("No actor found for shard, could not forward message");
                }
            }
        }
    }
}
