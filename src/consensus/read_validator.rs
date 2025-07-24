use std::collections::BTreeMap;

use super::validator::StoredValidatorSets;
use crate::consensus::consensus::SystemMessage;
use crate::core::types::{SnapchainValidatorContext, Vote};
use crate::core::util::FarcasterTime;
use crate::proto::{self, DecidedValue, FarcasterNetwork, Height};
use crate::storage::store::engine::{BlockEngine, ShardEngine};
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use crate::version::version::EngineVersion;
use bytes::Bytes;
use informalsystems_malachitebft_core_types::{NilOrVal, ThresholdParams};
use informalsystems_malachitebft_sync::RawDecidedValue;
use itertools::Itertools;
use libp2p::identity::ed25519::PublicKey;
use prost::Message;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

pub enum Engine {
    ShardEngine(ShardEngine),
    BlockEngine(BlockEngine),
}
pub struct ReadValidator {
    pub engine: Engine,
    pub shard_id: u32,
    pub last_height: Height,
    pub max_num_buffered_blocks: u32,
    pub buffered_blocks: BTreeMap<Height, proto::DecidedValue>,
    pub statsd_client: StatsdClientWrapper,
    pub validator_sets: StoredValidatorSets,
    pub system_tx: mpsc::Sender<SystemMessage>,
}

impl ReadValidator {
    pub fn initialize_height(&mut self) {
        let height = match &self.engine {
            Engine::BlockEngine(engine) => engine.get_confirmed_height(),
            Engine::ShardEngine(engine) => engine.get_confirmed_height(),
        };
        self.last_height = height;
    }

    pub fn get_min_height(&self) -> Height {
        match &self.engine {
            Engine::BlockEngine(engine) => engine.get_min_height(),
            Engine::ShardEngine(engine) => engine.get_min_height(),
        }
    }

    async fn commit_decided_value(&mut self, value: &DecidedValue, height: Height) {
        match &mut self.engine {
            Engine::ShardEngine(shard_engine) => match &value.value {
                Some(proto::decided_value::Value::Shard(shard_chunk)) => {
                    shard_engine.commit_shard_chunk(&shard_chunk).await;
                    info!(
                        %height,
                        hash = hex::encode(&shard_chunk.hash),
                        "Processed decided shard chunk"
                    );
                }
                _ => {
                    panic!("Invalid decided value")
                }
            },
            Engine::BlockEngine(block_engine) => match &value.value {
                Some(proto::decided_value::Value::Block(block)) => {
                    block_engine.commit_block(&block);
                    info!(
                        %height,
                        hash = hex::encode(&block.hash),
                        "Processed decided block"
                    );
                }
                _ => {
                    panic!("Invalid decided value")
                }
            },
        };
        self.last_height = height;
    }

    async fn process_buffered_blocks(&mut self) -> u64 {
        let mut num_blocks_processed = 0;
        // This works only because [buffered_blocks] is ordered by height. It's important to maintain this property
        while let Some((height, value)) = self.buffered_blocks.pop_first() {
            if height == self.last_height.increment() {
                self.commit_decided_value(&value, height).await;
                num_blocks_processed += 1;
            } else if height > self.last_height.increment() {
                self.buffered_blocks.insert(height, value);
                break;
            }
        }

        num_blocks_processed
    }

    fn get_decided_value_height(value: &proto::DecidedValue) -> Height {
        match value.value.as_ref().unwrap() {
            proto::decided_value::Value::Shard(shard_chunk) => {
                shard_chunk.header.as_ref().unwrap().height.unwrap()
            }

            proto::decided_value::Value::Block(block) => {
                block.header.as_ref().unwrap().height.unwrap()
            }
        }
    }

    fn verify_signatures(&self, value: &proto::DecidedValue) -> bool {
        let certificate = match value.value.as_ref().unwrap() {
            proto::decided_value::Value::Shard(shard_chunk) => shard_chunk
                .commits
                .as_ref()
                .unwrap()
                .to_commit_certificate(),

            proto::decided_value::Value::Block(block) => {
                block.commits.as_ref().unwrap().to_commit_certificate()
            }
        };

        let validator_set = self
            .validator_sets
            .get_validator_set(certificate.height.as_u64());

        let mut expected_pubkeys = validator_set
            .validators
            .iter()
            .map(|validator| validator.public_key.to_bytes());

        if !ThresholdParams::default().quorum.is_met(
            certificate.aggregated_signature.signatures.len() as u64,
            expected_pubkeys.len() as u64,
        ) {
            error!(%certificate.height, last_height = %self.last_height, "Block did not have quorum");
            return false;
        }

        for signature in certificate.aggregated_signature.signatures {
            let address_bytes = &signature.address.0;
            if !expected_pubkeys.contains(address_bytes) {
                error!(%certificate.height, last_height = %self.last_height, "Block contained signatures from unexpected signers");
                return false;
            }

            let vote = Vote::new_precommit(
                certificate.height,
                certificate.round,
                NilOrVal::Val(certificate.value_id.clone()),
                signature.address.clone(),
            );

            let public_key = PublicKey::try_from_bytes(address_bytes).unwrap();
            if !public_key.verify(&vote.to_sign_bytes(), &signature.signature.0) {
                error!(%certificate.height, last_height = %self.last_height, "Block contained invalid signatures");
                return false;
            }
        }

        true
    }

    pub fn validate_protocol_version(&self, value: &DecidedValue) -> bool {
        match &value.value {
            Some(proto::decided_value::Value::Block(block)) => {
                let header = block.header.as_ref().unwrap();
                let network = FarcasterNetwork::try_from(header.chain_id).unwrap();
                let timestamp = FarcasterTime::new(header.timestamp);
                let expected_version =
                    EngineVersion::version_for(&timestamp, network).protocol_version();

                if header.version != expected_version {
                    let error_message = format!(
                        "Invalid protocol version in decided block at height {}: expected {}, got {}. Does your node need an upgrade?",
                        header.height.unwrap().block_number,
                        expected_version, header.version
                    );
                    error!(%self.last_height, error_message);
                    self.system_tx
                        .try_send(SystemMessage::ExitWithError(error_message))
                        .unwrap_or_else(|e| {
                            error!(%self.last_height, "Failed to send system message: {}", e);
                        });
                    return false;
                }
            }
            _ => {
                // no-op. Only blocks have protocol version
            }
        }
        true
    }

    pub async fn process_decided_value(&mut self, value: DecidedValue) -> u64 {
        let height = Self::get_decided_value_height(&value);
        let verified = self.verify_signatures(&value);
        if !verified {
            error!(%height, last_height = %self.last_height, "Dropping decided block because its signatures are invalid");
            return 0;
        }

        // Only validate the protocol version after verifying signatures so we know it's a valid block
        if !self.validate_protocol_version(&value) {
            error!(%height, last_height = %self.last_height, "Dropping decided block because its protocol version is invalid");
            return 0;
        }

        let num_committed_values = if height > self.last_height.increment() {
            if (self.buffered_blocks.len() as u32) < self.max_num_buffered_blocks {
                self.buffered_blocks.insert(height, value);
                0
            } else {
                warn!(%height, last_height = %self.last_height, "Dropping decided block because buffered block space is full");
                0
            }
        } else if height == self.last_height.increment() {
            self.commit_decided_value(&value, height).await;
            let num_buffered_blocks_processed = self.process_buffered_blocks().await;
            num_buffered_blocks_processed + 1
        } else {
            debug!(%height, last_height = %self.last_height, "Dropping decided block because height is too low");
            0
        };
        self.statsd_client.gauge_with_shard(
            self.shard_id,
            "read_validator.num_buffered_blocks",
            self.buffered_blocks.len() as u64,
        );
        self.statsd_client.count_with_shard(
            self.shard_id,
            "read_validator.num_commited_values",
            num_committed_values,
            vec![],
        );
        num_committed_values
    }

    pub fn get_decided_value(
        &mut self,
        height: Height,
    ) -> Option<RawDecidedValue<SnapchainValidatorContext>> {
        match &self.engine {
            Engine::ShardEngine(shard_engine) => {
                let shard_chunk = shard_engine.get_shard_chunk_by_height(height);
                match shard_chunk {
                    Some(chunk) => {
                        let commits = chunk.commits.clone().unwrap();
                        Some(RawDecidedValue {
                            certificate: commits.to_commit_certificate(),
                            value_bytes: Bytes::from(chunk.encode_to_vec()),
                        })
                    }
                    None => None,
                }
            }
            Engine::BlockEngine(block_engine) => {
                let block = block_engine.get_block_by_height(height);
                match block {
                    Some(block) => {
                        let commits = block.commits.clone().unwrap();
                        Some(RawDecidedValue {
                            certificate: commits.to_commit_certificate(),
                            value_bytes: Bytes::from(block.encode_to_vec()),
                        })
                    }
                    None => None,
                }
            }
        }
    }
}
