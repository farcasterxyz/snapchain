use std::collections::BTreeMap;

use crate::core::types::SnapchainValidatorContext;
use crate::proto::{self, DecidedValue, Height};
use crate::storage::store::engine::{BlockEngine, ShardEngine};
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use bytes::Bytes;
use informalsystems_malachitebft_sync::RawDecidedValue;
use prost::Message;
use tracing::{debug, info, warn};

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
        // Always return the genesis block, until we implement pruning
        Height::new(self.shard_id, 1)
    }

    fn commit_decided_value(&mut self, value: &DecidedValue, height: Height) {
        match &mut self.engine {
            Engine::ShardEngine(shard_engine) => match &value.value {
                Some(proto::decided_value::Value::Shard(shard_chunk)) => {
                    shard_engine.commit_shard_chunk(&shard_chunk);
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

    fn process_buffered_blocks(&mut self) -> u64 {
        let mut num_blocks_processed = 0;
        // This works only because [buffered_blocks] is ordered by height. It's important to maintain this property
        while let Some((height, value)) = self.buffered_blocks.pop_first() {
            if height == self.last_height.increment() {
                self.commit_decided_value(&value, height);
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

    pub fn process_decided_value(&mut self, value: DecidedValue) -> u64 {
        let height = Self::get_decided_value_height(&value);
        let num_committed_values = if height > self.last_height.increment() {
            if (self.buffered_blocks.len() as u32) < self.max_num_buffered_blocks {
                self.buffered_blocks.insert(height, value);
                0
            } else {
                warn!(%height, last_height = %self.last_height, "Dropping decided block because buffered block space is full");
                0
            }
        } else if height == self.last_height.increment() {
            self.commit_decided_value(&value, height);
            let num_buffered_blocks_processed = self.process_buffered_blocks();
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
