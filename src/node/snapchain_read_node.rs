use crate::consensus::consensus::{Config, MalachiteEventShard, SystemMessage};
use crate::consensus::malachite::network_connector::MalachiteNetworkEvent;
use crate::consensus::malachite::spawn_read_node::MalachiteReadNodeActors;
use crate::consensus::read_validator::Engine;
use crate::core::types::{Address, ShardId, SnapchainShard, SnapchainValidatorContext};
use crate::network::gossip::GossipEvent;
use crate::proto;
use crate::storage::db::RocksDB;
use crate::storage::store::block_engine::{BlockEngine, BlockStores};
use crate::storage::store::engine::{PostCommitMessage, Senders, ShardEngine};
use crate::storage::store::stores::{StoreLimits, Stores};
use crate::storage::trie::merkle_trie::{self, MerkleTrie};
use crate::utils::block_event_fix::reconcile_heartbeat_events;
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use informalsystems_malachitebft_metrics::SharedRegistry;
use libp2p::identity::ed25519::Keypair;
use libp2p::PeerId;
use std::collections::{BTreeMap, HashMap};
use tokio::sync::mpsc;
use tracing::{info, warn};

const MAX_SHARDS: u32 = 64;

#[derive(Clone)]
pub struct SnapchainReadNode {
    pub consensus_actors: BTreeMap<u32, MalachiteReadNodeActors>,
    pub shard_stores: HashMap<u32, Stores>,
    pub shard_senders: HashMap<u32, Senders>,
    pub block_stores: BlockStores,
    pub address: Address,
}

impl SnapchainReadNode {
    pub async fn create(
        keypair: Keypair,
        config: Config,
        local_peer_id: PeerId,
        gossip_tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
        system_tx: mpsc::Sender<SystemMessage>,
        rocksdb_dir: String,
        statsd_client: StatsdClientWrapper,
        farcaster_network: proto::FarcasterNetwork,
        registry: &SharedRegistry,
        engine_post_commit_tx: Option<mpsc::Sender<PostCommitMessage>>,
    ) -> Self {
        let validator_address = Address(keypair.public().to_bytes());

        // Now create the block validator data structures
        let block_shard = SnapchainShard::new(0);

        // We might want to use different keys for the block shard so signatures are different and cannot be accidentally used in the wrong shard
        let trie = MerkleTrie::new().unwrap();
        let block_db = RocksDB::open_shard_db(rocksdb_dir.as_str(), 0);
        let engine = BlockEngine::new(
            trie,
            statsd_client.clone(),
            block_db,
            config.max_messages_per_block,
            None,
            farcaster_network,
        );
        let block_stores = engine.stores();
        info!(
            "Block db height {}",
            block_stores.block_store.max_block_number().unwrap()
        );

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
            let trie = merkle_trie::MerkleTrie::new().unwrap(); //TODO: don't unwrap()
            let engine = match ShardEngine::new(
                db.clone(),
                farcaster_network,
                trie,
                shard_id,
                StoreLimits::default(),
                statsd_client.clone(),
                config.max_messages_per_block,
                None, // For a read-only node, we will never pull from the mempool
                None,
                engine_post_commit_tx.clone(),
            )
            .await
            {
                Ok(engine) => engine,
                Err(err) => {
                    panic!(
                        "Failed to create shard engine for shard {}: {}",
                        shard_id, err
                    );
                }
            };

            shard_senders.insert(shard_id, engine.get_senders());
            shard_stores.insert(shard_id, engine.get_stores());

            if config.reconcile_heartbeat_event != 0 {
                if let Err(err) = reconcile_heartbeat_events(
                    block_stores.clone(),
                    engine.get_stores(),
                    config.reconcile_heartbeat_event,
                )
                .await
                {
                    warn!(
                        "Unable to reconcile heartbeat events {:#?}",
                        err.to_string(),
                    )
                }
            }

            let consensus_actor = MalachiteReadNodeActors::create_and_start(
                ctx,
                Engine::ShardEngine(engine),
                local_peer_id,
                gossip_tx.clone(),
                system_tx.clone(),
                registry,
                shard_id,
                statsd_client.clone(),
                config.clone(),
            )
            .await;

            if consensus_actor.is_err() {
                panic!("Failed to create consensus actor for shard {}", shard_id);
            }

            consensus_actors.insert(shard_id, consensus_actor.unwrap());
        }

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
            config.clone(),
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
            block_stores,
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
