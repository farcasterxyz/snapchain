use crate::consensus::malachite::network_connector::MalachiteNetworkEvent;
use crate::mempool::mempool::MempoolMessageWithSource;
use crate::proto;
pub use informalsystems_malachitebft_core_consensus::Params as ConsensusParams;
pub use informalsystems_malachitebft_core_consensus::State as ConsensusState;
use libp2p::identity::ed25519::{Keypair, SecretKey};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Debug)]
pub enum MalachiteEventShard {
    None,
    Shard(u32),
}

#[derive(Debug)]
pub enum SystemMessage {
    MalachiteNetwork(MalachiteEventShard, MalachiteNetworkEvent), // Shard Id and the malachite network event
    Mempool(MempoolMessageWithSource),

    DecidedValueForReadNode(proto::DecidedValue),

    ReadNodeFinishedInitialSync { shard_id: u32 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorSetConfig {
    pub effective_at: u64,
    pub validator_public_keys: Vec<String>,
    pub shard_ids: Vec<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub private_key: String,
    pub num_shards: u32,
    pub shard_ids: Vec<u32>,

    #[serde(with = "humantime_serde")]
    pub block_time: Duration,

    pub max_messages_per_block: u32,
    validator_sets: Option<Vec<ValidatorSetConfig>>,
    validator_addresses: Option<Vec<String>>, // Deprecated

    // Number of seconds to wait before kicking off start height
    pub consensus_start_delay: u32,
}

impl Config {
    pub fn keypair(&self) -> Keypair {
        let bytes = hex::decode(&self.private_key).unwrap();
        let secret_key = SecretKey::try_from_bytes(bytes);
        Keypair::from(secret_key.unwrap())
    }

    pub fn with(&self, shard_ids: Vec<u32>, validator_sets: Vec<ValidatorSetConfig>) -> Self {
        Self {
            private_key: self.private_key.clone(),
            num_shards: shard_ids.len() as u32,
            shard_ids,
            block_time: self.block_time,
            max_messages_per_block: self.max_messages_per_block,
            validator_addresses: None,
            validator_sets: Some(validator_sets.clone()),
            consensus_start_delay: self.consensus_start_delay,
        }
    }

    pub fn get_validator_set_config(&self, shard_id: u32) -> Vec<ValidatorSetConfig> {
        if let Some(sets) = &self.validator_sets {
            assert!(sets.len() > 0);
            return sets.to_vec();
        }

        if let Some(addresses) = &self.validator_addresses {
            assert!(addresses.len() > 0);
            return vec![ValidatorSetConfig {
                effective_at: 0,
                validator_public_keys: addresses.clone(),
                shard_ids: vec![shard_id],
            }];
        }

        panic!("No validator configuration provided")
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            private_key: hex::encode(SecretKey::generate()),
            shard_ids: vec![1],
            num_shards: 1,
            block_time: Duration::from_millis(250),
            max_messages_per_block: 500,
            validator_addresses: None,
            validator_sets: Some(vec![]),
            consensus_start_delay: 2,
        }
    }
}
