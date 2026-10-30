use std::collections::BTreeMap;

use super::validator::StoredValidatorSets;
use crate::consensus::consensus::SystemMessage;
use crate::core::types::{CommitsExt, SnapchainValidatorContext};
use crate::core::util::{
    block_events_match_hash, block_hash, shard_chunk_hash, shard_witness_hash, verify_signatures,
    FarcasterTime,
};
use crate::proto::{self, DecidedValue, FarcasterNetwork, Height};
use crate::storage::store::block_engine::BlockEngine;
use crate::storage::store::engine::ShardEngine;
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use crate::version::version::EngineVersion;
use bytes::Bytes;
use informalsystems_malachitebft_sync::RawDecidedValue;
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

    /// Replays and commits `value`. Returns false, leaving state untouched, if its transactions
    /// do not reproduce the state root in its header.
    async fn commit_decided_value(&mut self, value: &DecidedValue, height: Height) -> bool {
        match (&mut self.engine, &value.value) {
            (
                Engine::ShardEngine(shard_engine),
                Some(proto::decided_value::Value::Shard(shard_chunk)),
            ) => {
                // The certificate signs only the header hash, so the transactions are bound to it
                // solely through the state root. Check that without panicking before committing,
                // since `commit_shard_chunk` treats a mismatch as fatal. This caches the replay, so
                // the commit does not replay again.
                if !shard_engine.validate_shard_chunk(shard_chunk) {
                    error!(%height, hash = hex::encode(&shard_chunk.hash), "Dropping decided shard chunk whose transactions do not match its header");
                    return false;
                }
                shard_engine.commit_shard_chunk(&shard_chunk).await;
                info!(
                    %height,
                    hash = hex::encode(&shard_chunk.hash),
                    "Processed decided shard chunk"
                );
            }
            (
                Engine::BlockEngine(block_engine),
                Some(proto::decided_value::Value::Block(block)),
            ) => {
                if !block_engine.validate_block(block) {
                    error!(%height, hash = hex::encode(&block.hash), "Dropping decided block whose transactions do not match its header");
                    return false;
                }
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
        };
        self.last_height = height;
        true
    }

    async fn process_buffered_blocks(&mut self) -> u64 {
        let mut num_blocks_processed = 0;
        // This works only because [buffered_blocks] is ordered by height. It's important to maintain this property
        while let Some((height, value)) = self.buffered_blocks.pop_first() {
            if height == self.last_height.increment() {
                if !self.commit_decided_value(&value, height).await {
                    break;
                }
                num_blocks_processed += 1;
            } else if height > self.last_height.increment() {
                self.buffered_blocks.insert(height, value);
                break;
            }
        }

        num_blocks_processed
    }

    /// Returns the height in a decided value's header, or `None` if the value, header or height
    /// is missing.
    pub fn decided_value_height(value: &proto::DecidedValue) -> Option<Height> {
        match value.value.as_ref()? {
            proto::decided_value::Value::Shard(shard_chunk) => shard_chunk.header.as_ref()?.height,
            proto::decided_value::Value::Block(block) => block.header.as_ref()?.height,
        }
    }

    fn matches_engine(&self, value: &proto::DecidedValue) -> bool {
        matches!(
            (&self.engine, &value.value),
            (
                Engine::ShardEngine(_),
                Some(proto::decided_value::Value::Shard(_))
            ) | (
                Engine::BlockEngine(_),
                Some(proto::decided_value::Value::Block(_))
            )
        )
    }

    /// Checks that the value carries a valid commit certificate for exactly this value. The
    /// certificate signs `(height, hash)`, so the hash must be the header's hash and the
    /// certificate's height and value must match the header; otherwise a peer could attach a
    /// genuine certificate to a forged header.
    fn verify_certificate(&self, value: &proto::DecidedValue) -> bool {
        let (height, hash, expected_hash, body_matches_header, commits) = match value.value.as_ref()
        {
            Some(proto::decided_value::Value::Shard(shard_chunk)) => {
                let Some(header) = shard_chunk.header.as_ref() else {
                    return false;
                };
                (
                    header.height,
                    &shard_chunk.hash,
                    shard_chunk_hash(header),
                    true,
                    shard_chunk.commits.as_ref(),
                )
            }
            Some(proto::decided_value::Value::Block(block)) => {
                let Some(header) = block.header.as_ref() else {
                    return false;
                };
                // Witnesses and events are stored and served but not replayed, so bind them
                // to the header too. Blocks from before witnesses were introduced have no hash.
                let witness_matches = header.shard_witnesses_hash.is_empty()
                    || block.shard_witness.as_ref().is_some_and(|witness| {
                        shard_witness_hash(witness) == header.shard_witnesses_hash
                    });
                let events_match = block_events_match_hash(&block.events, &header.events_hash);
                (
                    header.height,
                    &block.hash,
                    block_hash(header),
                    witness_matches && events_match,
                    block.commits.as_ref(),
                )
            }
            None => return false,
        };
        let (Some(height), Some(commits)) = (height, commits) else {
            return false;
        };

        if *hash != expected_hash {
            error!(%height, "Decided value hash does not match its header");
            return false;
        }
        if !body_matches_header {
            error!(%height, "Decided block witness or events do not match its header");
            return false;
        }
        let expected_value = proto::ShardHash {
            shard_index: height.shard_index,
            hash: hash.clone(),
        };
        if commits.height != Some(height) || commits.value.as_ref() != Some(&expected_value) {
            error!(%height, "Commit certificate is for a different height or value");
            return false;
        }

        verify_signatures(commits, &self.validator_sets)
    }

    pub fn validate_protocol_version(&self, value: &DecidedValue) -> bool {
        match &value.value {
            Some(proto::decided_value::Value::Block(block)) => {
                let Some(header) = block.header.as_ref() else {
                    return false;
                };
                let Ok(network) = FarcasterNetwork::try_from(header.chain_id) else {
                    error!(%self.last_height, chain_id = header.chain_id, "Invalid chain id in decided block");
                    return false;
                };
                let timestamp = FarcasterTime::new(header.timestamp);
                let expected_version =
                    EngineVersion::version_for(&timestamp, network).protocol_version();

                if header.version != expected_version {
                    let error_message = format!(
                        "Invalid protocol version in decided block at height {}: expected {}, got {}. Does your node need an upgrade?",
                        header.height.unwrap_or_default().block_number,
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
        let Some(height) = Self::decided_value_height(&value) else {
            warn!(last_height = %self.last_height, "Dropping decided value with missing header or height");
            return 0;
        };
        if height.shard_index != self.shard_id || !self.matches_engine(&value) {
            warn!(%height, last_height = %self.last_height, "Dropping decided value for a different shard");
            return 0;
        }
        if !self.verify_certificate(&value) {
            error!(%height, last_height = %self.last_height, "Dropping decided block because its commit certificate is invalid");
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
            if self.commit_decided_value(&value, height).await {
                let num_buffered_blocks_processed = self.process_buffered_blocks().await;
                num_buffered_blocks_processed + 1
            } else {
                0
            }
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
