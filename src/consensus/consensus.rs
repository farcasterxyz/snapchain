use crate::consensus::malachite::network_connector::MalachiteNetworkEvent;
use crate::consensus::validator::StoredValidatorSet;
use crate::consensus::validator::StoredValidatorSets;
use crate::core::types::ShardId;
use crate::mempool::mempool::MempoolRequest;
use crate::proto;
use crate::proto::Block;
pub use informalsystems_malachitebft_core_consensus::Params as ConsensusParams;
pub use informalsystems_malachitebft_core_consensus::State as ConsensusState;
use libp2p::identity::ed25519::{Keypair, SecretKey};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::oneshot;

#[derive(Clone, Debug)]
pub enum MalachiteEventShard {
    None,
    Shard(u32),
}

#[derive(Debug)]
pub enum SystemMessage {
    MalachiteNetwork(MalachiteEventShard, MalachiteNetworkEvent), // Shard Id and the malachite network event
    Mempool(MempoolRequest),

    DecidedValueForReadNode(proto::DecidedValue),

    BlockRequest {
        block_event_seqnum: u64,
        block_tx: oneshot::Sender<Option<Block>>,
    },

    ReadNodeFinishedInitialSync {
        shard_id: u32,
    },
    ExitWithError(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorSetConfig {
    pub effective_at: u64,
    pub validator_public_keys: Vec<String>,
    pub shard_ids: Vec<u32>,
}

/// Wrapper for loading validator sets from a separate file.
/// The file should contain a TOML array of `[[validator_sets]]` entries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorSetsFile {
    pub validator_sets: Vec<ValidatorSetConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub private_key: String,
    pub num_shards: u32,
    pub shard_ids: Vec<u32>,

    #[serde(with = "humantime_serde")]
    pub step_delta: Duration, // Timeout delta between steps
    #[serde(with = "humantime_serde")]
    pub propose_time: Duration, // Timeout for each propose/prevote/precommit step
    #[serde(with = "humantime_serde")]
    pub prevote_time: Duration, // Timeout for each propose/prevote/precommit step
    #[serde(with = "humantime_serde")]
    pub precommit_time: Duration, // Timeout for each propose/prevote/precommit step
    #[serde(with = "humantime_serde")]
    pub block_time: Duration,

    pub max_messages_per_block: u32,
    validator_sets: Option<Vec<ValidatorSetConfig>>,
    validator_addresses: Option<Vec<String>>, // Deprecated

    /// Path to an external TOML file containing validator sets.
    /// When set, validator sets are loaded from this file instead of being
    /// specified inline. This avoids duplicating the validator list across
    /// multiple node configs. (See issue #437)
    pub validator_sets_file: Option<String>,

    // Number of seconds to wait before kicking off start height
    pub consensus_start_delay: u32,
    pub sync_request_timeout: Duration,
    pub sync_status_update_interval: Duration,
    pub reconcile_heartbeat_event: u64,
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
            propose_time: self.propose_time,
            prevote_time: self.prevote_time,
            precommit_time: self.precommit_time,
            step_delta: self.step_delta,
            max_messages_per_block: self.max_messages_per_block,
            validator_addresses: None,
            validator_sets: Some(validator_sets.clone()),
            validator_sets_file: None,
            consensus_start_delay: self.consensus_start_delay,
            sync_request_timeout: self.sync_request_timeout,
            sync_status_update_interval: self.sync_status_update_interval,
            reconcile_heartbeat_event: self.reconcile_heartbeat_event,
        }
    }

    pub fn get_validator_set_config(&self, shard_id: u32) -> Vec<ValidatorSetConfig> {
        // Priority: inline validator_sets > validator_sets_file > validator_addresses (deprecated)
        if let Some(sets) = &self.validator_sets {
            assert!(sets.len() > 0);
            return sets.to_vec();
        }

        // Load from external file if specified (fixes #437)
        if let Some(path) = &self.validator_sets_file {
            let contents = std::fs::read_to_string(path)
                .unwrap_or_else(|e| panic!("Failed to read validator sets file '{}': {}", path, e));
            let file: ValidatorSetsFile = toml::from_str(&contents)
                .unwrap_or_else(|e| panic!("Failed to parse validator sets file '{}': {}", path, e));
            assert!(file.validator_sets.len() > 0, "Validator sets file '{}' is empty", path);
            return file.validator_sets;
        }

        if let Some(addresses) = &self.validator_addresses {
            assert!(addresses.len() > 0);
            return vec![ValidatorSetConfig {
                effective_at: 0,
                validator_public_keys: addresses.clone(),
                shard_ids: vec![shard_id],
            }];
        }

        panic!("No validator configuration provided. Set validator_sets, validator_sets_file, or validator_addresses in the consensus config.")
    }

    pub fn to_stored_validator_sets(&self, shard_id: u32) -> StoredValidatorSets {
        let validator_set_config = self.get_validator_set_config(shard_id);
        let validator_sets = validator_set_config
            .iter()
            .map(|config| StoredValidatorSet::new(ShardId::new(shard_id), &config))
            .collect();

        StoredValidatorSets::new(shard_id, validator_sets)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            private_key: hex::encode(SecretKey::generate()),
            shard_ids: vec![1],
            num_shards: 1,
            propose_time: Duration::from_millis(1000),
            prevote_time: Duration::from_millis(500),
            precommit_time: Duration::from_millis(500),
            step_delta: Duration::from_millis(500),
            block_time: Duration::from_millis(1000),
            max_messages_per_block: 1000,
            validator_addresses: None,
            validator_sets: None,
            validator_sets_file: None,
            consensus_start_delay: 2,
            sync_request_timeout: Duration::from_secs(2),
            sync_status_update_interval: Duration::from_secs(10),
            reconcile_heartbeat_event: 0,
        }
    }
}
