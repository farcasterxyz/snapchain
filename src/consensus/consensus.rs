use crate::consensus::malachite::network_connector::MalachiteNetworkEvent;
use crate::core::types::{ShardId, SnapchainShard, SnapchainValidator, SnapchainValidatorSet};
use crate::mempool::mempool::MempoolMessageWithSource;
use crate::proto;
pub use informalsystems_malachitebft_core_consensus::Params as ConsensusParams;
pub use informalsystems_malachitebft_core_consensus::State as ConsensusState;
use libp2p::identity::ed25519::{Keypair, PublicKey, SecretKey};
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
pub struct Config {
    pub private_key: String,
    pub num_shards: u32,
    pub shard_ids: Vec<u32>,

    #[serde(with = "humantime_serde")]
    pub block_time: Duration,

    pub max_messages_per_block: u32,
    pub validator_addresses: Vec<String>,

    // Number of seconds to wait before kicking off start height
    pub consensus_start_delay: u32,
}

impl Config {
    pub fn keypair(&self) -> Keypair {
        let bytes = hex::decode(&self.private_key).unwrap();
        let secret_key = SecretKey::try_from_bytes(bytes);
        Keypair::from(secret_key.unwrap())
    }

    pub fn with(&self, shard_ids: Vec<u32>, validator_addresses: Vec<String>) -> Self {
        Self {
            private_key: self.private_key.clone(),
            num_shards: shard_ids.len() as u32,
            shard_ids,
            block_time: self.block_time,
            max_messages_per_block: self.max_messages_per_block,
            validator_addresses: validator_addresses.clone(),
            consensus_start_delay: self.consensus_start_delay,
        }
    }

    pub fn validator_set_for(&self, shard_id: u32) -> SnapchainValidatorSet {
        let mut validators = SnapchainValidatorSet::new(vec![]);
        for address in &self.validator_addresses {
            let validator = SnapchainValidator::new(
                SnapchainShard::new(shard_id),
                PublicKey::try_from_bytes(&hex::decode(address).unwrap()).unwrap(),
                None,
                0,
            );
            validators.add(validator);
        }
        validators
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
            validator_addresses: vec![],
            consensus_start_delay: 2,
        }
    }
}
