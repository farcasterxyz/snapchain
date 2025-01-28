use crate::consensus::consensus::{
    Config, Consensus, ConsensusMsg, ConsensusParams, MalachiteEventShard,
};
use crate::consensus::malachite::network_connector::MalachiteNetworkEvent;
use crate::consensus::malachite::spawn::MalachiteConsensusActors;
use crate::consensus::proposer::{BlockProposer, ShardProposer};
use crate::consensus::validator::ShardValidator;
use crate::core::types::{
    Address, Height, ShardId, SnapchainShard, SnapchainValidator, SnapchainValidatorConfig,
    SnapchainValidatorContext, SnapchainValidatorSet,
};
use crate::mempool::mempool::MempoolMessagesRequest;
use crate::network::gossip::GossipEvent;
use crate::proto::{Block, ShardChunk};
use crate::storage::db::RocksDB;
use crate::storage::store::engine::{BlockEngine, Senders, ShardEngine};
use crate::storage::store::stores::StoreLimits;
use crate::storage::store::stores::Stores;
use crate::storage::store::BlockStore;
use crate::storage::trie::merkle_trie;
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use informalsystems_malachitebft_config::TimeoutConfig;
use informalsystems_malachitebft_core_consensus::ValuePayload;
use informalsystems_malachitebft_metrics::{Metrics, SharedRegistry};
use libp2p::identity::ed25519::Keypair;
use ractor::ActorRef;
use std::collections::{BTreeMap, HashMap};
use tokio::sync::{broadcast, mpsc};
use tracing::warn;

const MAX_SHARDS: u32 = 64;

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
        rpc_address: Option<String>,
        gossip_tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
        shard_decision_tx: broadcast::Sender<ShardChunk>,
        block_tx: Option<mpsc::Sender<Block>>,
        messages_request_tx: mpsc::Sender<MempoolMessagesRequest>,
        block_store: BlockStore,
        rocksdb_dir: String,
        statsd_client: StatsdClientWrapper,
        trie_branching_factor: u32,
        registry: &SharedRegistry,
        validator_config: &SnapchainValidatorConfig,
    ) -> Self {
        let validator_address = Address(keypair.public().to_bytes());

        let mut consensus_actors = BTreeMap::new();

        let mut shard_senders: HashMap<u32, Senders> = HashMap::new();
        let mut shard_stores: HashMap<u32, Stores> = HashMap::new();

        // Create the shard validators
        for shard_id in config.shard_ids {
            if shard_id == 0 {
                panic!("Shard ID 0 is reserved for the block shard, created automaticaly");
            } else if shard_id > MAX_SHARDS {
                panic!("Shard ID must be between 1 and {}", MAX_SHARDS);
            }

            let shard = SnapchainShard::new(shard_id);
            let shard_validator_set = validator_config.get_validator_set(shard_id).unwrap();
            let ctx = SnapchainValidatorContext::new(keypair.clone());

            let db = RocksDB::open_shard_db(rocksdb_dir.clone().as_str(), shard_id);
            let trie = merkle_trie::MerkleTrie::new(trie_branching_factor).unwrap(); //TODO: don't unwrap()
            let engine = ShardEngine::new(
                db,
                trie,
                shard_id,
                StoreLimits::default(),
                statsd_client.clone(),
                config.max_messages_per_block,
                Some(messages_request_tx.clone()),
            );

            shard_senders.insert(shard_id, engine.get_senders());
            shard_stores.insert(shard_id, engine.get_stores());

            let shard_proposer = ShardProposer::new(
                validator_address.clone(),
                shard.clone(),
                engine,
                statsd_client.clone(),
                shard_decision_tx.clone(),
                config.propose_value_delay,
            );

            let shard_validator = ShardValidator::new(
                validator_address.clone(),
                shard.clone(),
                shard_validator_set.clone(),
                None,
                Some(shard_proposer),
            );
            let consensus_actor = MalachiteConsensusActors::create_and_start(
                ctx,
                shard_validator,
                rocksdb_dir.clone(),
                gossip_tx.clone(),
                registry,
            )
            .await;

            if consensus_actor.is_err() {
                panic!("Failed to create consensus actor for shard {}", shard_id);
            }

            consensus_actors.insert(shard_id, consensus_actor.unwrap());
        }

        // Now create the block validator
        let block_shard = SnapchainShard::new(0);

        let current_height = match block_store.max_block_number() {
            Err(_) => 0,
            Ok(height) => height,
        };
        // We might want to use different keys for the block shard so signatures are different and cannot be accidentally used in the wrong shard
        let block_validator_set = validator_config.get_validator_set(0).unwrap();
        let engine = BlockEngine::new(block_store.clone());
        let shard_decision_rx = shard_decision_tx.subscribe();
        let block_proposer = BlockProposer::new(
            validator_address.clone(),
            block_shard.clone(),
            shard_decision_rx,
            config.num_shards,
            block_tx,
            engine,
            statsd_client.clone(),
        );
        let block_validator = ShardValidator::new(
            validator_address.clone(),
            block_shard.clone(),
            block_validator_set.clone(),
            Some(block_proposer),
            None,
        );
        let ctx = SnapchainValidatorContext::new(keypair.clone());
        let block_consensus_actor = MalachiteConsensusActors::create_and_start(
            ctx,
            block_validator,
            rocksdb_dir.clone(),
            gossip_tx.clone(),
            registry,
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

    // pub fn start_height(&self, block_number: u64) {
    //     for (shard, actor) in self.consensus_actors.iter() {
    //         let result = actor.cast(ConsensusMsg::StartHeight(Height::new(*shard, block_number)));
    //         if let Err(e) = result {
    //             warn!("Failed to start height: {:?}", e);
    //         }
    //     }
    // }
    //
    pub fn dispatch(&self, shard: MalachiteEventShard, event: MalachiteNetworkEvent) {
        match shard {
            MalachiteEventShard::None => {
                // Dispatch to all shards
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
