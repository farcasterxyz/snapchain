//! Implementation of a host actor for bridiging consensus and the application via a set of channels.

use crate::consensus::read_validator::ReadValidator;
use crate::core::types::SnapchainValidatorContext;
use crate::proto::{self, Height};
use informalsystems_malachitebft_sync::RawDecidedValue;
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef, RpcReplyPort, SpawnErr};
use tracing::error;

use super::read_sync::{self, ReadSyncRef};

/// Messages that need to be handled by the host actor.
#[derive(Debug)]
pub enum ReadHostMsg {
    Started {
        sync: ReadSyncRef,
    },
    /// Request the earliest block height in the block store
    GetHistoryMinHeight {
        reply_to: RpcReplyPort<Height>,
    },

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

pub struct ReadHostState {
    pub validator: ReadValidator,
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
    async fn handle_msg(
        &self,
        _myself: ActorRef<ReadHostMsg>,
        msg: ReadHostMsg,
        state: &mut ReadHostState,
    ) -> Result<(), ActorProcessingErr> {
        match msg {
            ReadHostMsg::Started { sync } => {
                state.validator.initialize_height();
                sync.cast(read_sync::Msg::Decided(state.validator.last_height))?
            }

            ReadHostMsg::GetHistoryMinHeight { reply_to } => {
                // For now until we implement pruning
                reply_to.send(state.validator.get_min_height())?;
            }

            ReadHostMsg::ProcessDecidedValue { value, sync } => {
                let num_values_processed = state.validator.process_decided_value(value);
                if num_values_processed > 0 {
                    sync.cast(read_sync::Msg::Decided(state.validator.last_height))?
                }
            }

            ReadHostMsg::GetDecidedValue { height, reply_to } => {
                let decided_value = state.validator.get_decided_value(height);
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
