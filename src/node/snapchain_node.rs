use crate::consensus::consensus::{Config, MalachiteEventShard};
use crate::consensus::malachite::network_connector::MalachiteNetworkEvent;
use crate::consensus::malachite::spawn::MalachiteConsensusActors;
use crate::consensus::proposer::{BlockProposer, ShardProposer};
use crate::consensus::validator::ShardValidator;
use crate::core::types::{Address, ShardId, SnapchainShard, SnapchainValidatorContext};
use crate::mempool::mempool::MempoolMessagesRequest;
use crate::network::gossip::GossipEvent;
use crate::proto::{Block, FarcasterNetwork, ShardChunk};
use crate::storage::db::RocksDB;
use crate::storage::store::engine::{BlockEngine, Senders, ShardEngine};
use crate::storage::store::node_local_state::LocalStateStore;
use crate::storage::store::stores::StoreLimits;
use crate::storage::store::stores::Stores;
use crate::storage::store::BlockStore;
use crate::storage::trie::merkle_trie;
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use informalsystems_malachitebft_metrics::SharedRegistry;
use libp2p::identity::ed25519::Keypair;
use libp2p::PeerId;
use std::collections::{BTreeMap, HashMap};
use tokio::sync::{broadcast, mpsc};
use tracing::warn;

const MAX_SHARDS: u32 = 64;

#[derive(Clone)]
pub struct SnapchainNode {
    pub consensus_actors: BTreeMap<u32, MalachiteConsensusActors>,
    pub shard_stores: HashMap<u32, Stores>,
    pub shard_senders: HashMap<u32, Senders>,
    pub address: Address,
}

impl SnapchainNode {
    pub async fn create(
        keypair: Keypair,
        config: Config,
        local_peer_id: PeerId,
        gossip_tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
        shard_decision_tx: broadcast::Sender<ShardChunk>,
        block_tx: Option<mpsc::Sender<Block>>,
        messages_request_tx: mpsc::Sender<MempoolMessagesRequest>,
        block_store: BlockStore,
        local_state_store: LocalStateStore,
        rocksdb_dir: String,
        statsd_client: StatsdClientWrapper,
        trie_branching_factor: u32,
        network: FarcasterNetwork,
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

            let shard = SnapchainShard::new(shard_id);
            let ctx = SnapchainValidatorContext::new(keypair.clone());

            let db = RocksDB::open_shard_db(rocksdb_dir.clone().as_str(), shard_id);
            let trie = merkle_trie::MerkleTrie::new(trie_branching_factor).unwrap(); //TODO: don't unwrap()
            let engine = ShardEngine::new(
                db.clone(),
                network,
                trie,
                shard_id,
                StoreLimits::default(),
                statsd_client.clone(),
                config.max_messages_per_block,
                Some(messages_request_tx.clone()),
                None,
            );

            shard_senders.insert(shard_id, engine.get_senders());
            shard_stores.insert(shard_id, engine.get_stores());

            let shard_proposer = ShardProposer::new(
                validator_address.clone(),
                shard.clone(),
                engine,
                statsd_client.clone(),
                shard_decision_tx.clone(),
            );

            let validator_sets = config.get_validator_set_config(shard_id);
            let shard_validator = ShardValidator::new(
                validator_address.clone(),
                shard.clone(),
                validator_sets,
                None,
                Some(shard_proposer),
                local_state_store.clone(),
                statsd_client.clone(),
            );
            let consensus_actor = MalachiteConsensusActors::create_and_start(
                ctx,
                shard_validator,
                local_peer_id,
                rocksdb_dir.clone(),
                gossip_tx.clone(),
                registry,
                config.clone(),
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
        let block_proposer = BlockProposer::new(
            validator_address.clone(),
            block_shard.clone(),
            shard_stores.clone(),
            config.num_shards,
            network,
            block_tx,
            engine,
            statsd_client.clone(),
        );
        let validator_sets = config.get_validator_set_config(0);
        let block_validator = ShardValidator::new(
            validator_address.clone(),
            block_shard.clone(),
            validator_sets,
            Some(block_proposer),
            None,
            local_state_store,
            statsd_client.clone(),
        );
        let ctx = SnapchainValidatorContext::new(keypair.clone());
        let block_consensus_actor = MalachiteConsensusActors::create_and_start(
            ctx,
            block_validator,
            local_peer_id,
            rocksdb_dir.clone(),
            gossip_tx.clone(),
            registry,
            config,
            statsd_client.clone(),
        )
        .await;
        if block_consensus_actor.is_err() {
            panic!("Failed to create consensus actor for block shard");
        }
        consensus_actors.insert(0, block_consensus_actor.unwrap());

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

    pub fn dispatch(&self, shard: MalachiteEventShard, event: MalachiteNetworkEvent) {
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
