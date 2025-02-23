//! Implementation of a host actor for bridiging consensus and the application via a set of channels.

use std::collections::BTreeMap;

use crate::consensus::malachite::read_sync;
use crate::core::types::SnapchainValidatorContext;
use crate::proto::{self, Height};
use crate::storage::store::engine::{BlockEngine, ShardEngine};
use bytes::Bytes;
use informalsystems_malachitebft_sync::RawDecidedValue;
use prost::Message;
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef, RpcReplyPort, SpawnErr};
use tracing::{error, info};

use super::read_sync::ReadSyncRef;

/// Messages that need to be handled by the host actor.
#[derive(Debug)]
pub enum ReadHostMsg {
    /// Request the earliest block height in the block store
    GetHistoryMinHeight { reply_to: RpcReplyPort<Height> },

    // Consensus has decided on a value
    ProcessDecidedValue {
        value: proto::DecidedValue,
        sync: ReadSyncRef,
    },

    // Retrieve decided block from the block store
    GetDecidedValue {
        height: Height,
        reply_to: RpcReplyPort<Option<RawDecidedValue<SnapchainValidatorContext>>>,
    },
}

pub type ReadHostRef = ActorRef<ReadHostMsg>;

pub struct ReadHost {}

pub enum Engine {
    ShardEngine(ShardEngine),
    BlockEngine(BlockEngine),
}

pub struct ReadHostState {
    pub engine: Engine,
    pub shard_id: u32,
    pub last_height: Height,
    pub max_num_buffered_blocks: u32,
    pub buffered_blocks: BTreeMap<Height, proto::DecidedValue>,
}

impl ReadHost {
    pub fn new() -> Self {
        ReadHost {}
    }

    pub async fn spawn(state: ReadHostState) -> Result<ActorRef<ReadHostMsg>, SpawnErr> {
        let (actor_ref, _) = Actor::spawn(None, Self::new(), state).await?;
        Ok(actor_ref)
    }
}

impl ReadHost {
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

    fn process_buffered_blocks(state: &mut ReadHostState, sync: &ReadSyncRef) {
        while let Some((height, value)) = state.buffered_blocks.pop_first() {
            if height == state.last_height.increment() {
                Self::process_decided_value(state, &value, height, sync);
            } else if height > state.last_height.increment() {
                state.buffered_blocks.insert(height, value);
                break;
            }
        }
    }

    fn process_decided_value(
        state: &mut ReadHostState,
        value: &proto::DecidedValue,
        height: Height,
        sync: &ReadSyncRef,
    ) {
        match &mut state.engine {
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
        state.last_height = height;
        sync.cast(read_sync::Msg::Decided(height)).unwrap();
    }

    async fn handle_msg(
        &self,
        _myself: ActorRef<ReadHostMsg>,
        msg: ReadHostMsg,
        state: &mut ReadHostState,
    ) -> Result<(), ActorProcessingErr> {
        match msg {
            ReadHostMsg::GetHistoryMinHeight { reply_to } => {
                reply_to.send(crate::proto::Height::new(state.shard_id, 1))?;
            }

            ReadHostMsg::ProcessDecidedValue { value, sync } => {
                let height = Self::get_decided_value_height(&value);
                if height > state.last_height.increment() {
                    if (state.buffered_blocks.len() as u32) < state.max_num_buffered_blocks {
                        state.buffered_blocks.insert(height, value);
                    } else {
                        info!(%height, last_height = %state.last_height, "Dropping decided block because buffered block space is full")
                    }
                } else if height == state.last_height.increment() {
                    Self::process_decided_value(state, &value, height, &sync);
                    Self::process_buffered_blocks(state, &sync);
                } else {
                    info!(%height, last_height = %state.last_height, "Dropping decided block because height is too low")
                }
            }

            ReadHostMsg::GetDecidedValue { height, reply_to } => {
                let decided_value = match &state.engine {
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
                };
                reply_to.send(decided_value)?;
            }
        };

        Ok(())
    }
}

#[async_trait]
impl Actor for ReadHost {
    type Msg = ReadHostMsg;
    type State = ReadHostState;
    type Arguments = ReadHostState;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(args)
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        msg: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        if let Err(e) = self.handle_msg(myself, msg, state).await {
            error!("Error processing message: {e}");
        }
        Ok(())
    }
}
