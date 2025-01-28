//! Implementation of a host actor for bridiging consensus and the application via a set of channels.

use informalsystems_malachitebft_core_consensus::{PeerId, ProposedValue};
use informalsystems_malachitebft_core_types::{CommitCertificate, Round, ValueId};
use informalsystems_malachitebft_engine::consensus::ConsensusMsg;
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef, SpawnErr};
use tokio::sync::mpsc;

use crate::consensus::validator::ShardValidator;
use crate::core::types::SnapchainValidatorContext;
use crate::network::gossip::GossipEvent;
use informalsystems_malachitebft_engine::host::{HostMsg, LocallyProposedValue};
use informalsystems_malachitebft_engine::network::{NetworkMsg, NetworkRef};
use informalsystems_malachitebft_engine::util::streaming::{StreamContent, StreamMessage};
use tracing::{debug, error, info, warn};

/// Actor for bridging consensus and the application via a set of channels.
///
/// This actor is responsible for forwarding messages from the
/// consensus actor to the application over a channel, and vice-versa.
pub struct Host {}

pub struct HostState {
    pub shard_validator: ShardValidator,
    pub gossip_tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
    pub network: NetworkRef<SnapchainValidatorContext>,
}

impl Host {
    pub fn new() -> Self {
        Host {}
    }

    pub async fn spawn(
        state: HostState,
    ) -> Result<ActorRef<HostMsg<SnapchainValidatorContext>>, SpawnErr> {
        let (actor_ref, _) = Actor::spawn(None, Self::new(), state).await?;
        Ok(actor_ref)
    }
}

impl Host {
    async fn handle_msg(
        &self,
        _myself: ActorRef<HostMsg<SnapchainValidatorContext>>,
        msg: HostMsg<SnapchainValidatorContext>,
        state: &mut HostState,
    ) -> Result<(), ActorProcessingErr> {
        match msg {
            HostMsg::ConsensusReady(consensus_ref) => {
                // Start height
                state.shard_validator.start(); // Call each time?
                let validator_set = state.shard_validator.get_validator_set();
                let height = state.shard_validator.get_current_height().increment();
                info!(
                    height = height.to_string(),
                    validators = validator_set.validators.len(),
                    "Consensus ready. Starting Height"
                );
                consensus_ref.cast(ConsensusMsg::StartHeight(height, validator_set))?;
            }

            HostMsg::StartedRound {
                height,
                round,
                proposer,
            } => {
                state.shard_validator.start_round(height, round, proposer);
                // Replay undecided values?
            }

            HostMsg::GetValue {
                height,
                round,
                timeout,
                reply_to,
            } => {
                let value = state
                    .shard_validator
                    .propose_value(height, round, timeout)
                    .await;
                let shard_hash = value.shard_hash().clone();
                let locally_proposed_value =
                    LocallyProposedValue::new(height, round, shard_hash, None);
                reply_to.send(locally_proposed_value)?;

                // Next, broadcast the value to the network
                let stream_message = StreamMessage::new(0, 0, StreamContent::Data(value));
                state
                    .network
                    .cast(NetworkMsg::PublishProposalPart(stream_message))?;
            }

            HostMsg::RestreamValue {
                height,
                round,
                valid_round: _,
                address: _,
                value_id: _,
            } => {
                // This is only called for pol_rounds which we're not using?
                warn!("Restream requested for value at height: {height}, round: {round}");
            }

            HostMsg::GetHistoryMinHeight { reply_to } => {
                reply_to.send(state.shard_validator.get_min_height())?;
            }

            HostMsg::ReceivedProposalPart {
                from,
                part,
                reply_to,
            } => {
                // store proposal part
                let data = part.content.as_data();
                match data {
                    Some(proposal) => {
                        let proposed_value = state.shard_validator.add_proposed_value(proposal);
                        reply_to.send(proposed_value)?;
                    }
                    None => {
                        error!("Received invalid proposal part from {from}");
                    }
                }
            }

            HostMsg::GetValidatorSet { height, reply_to } => {
                reply_to.send(state.shard_validator.get_validator_set())?;
            }

            HostMsg::Decided {
                certificate,
                consensus: consensus_ref,
            } => {
                //commit
                state
                    .shard_validator
                    .decide(certificate.height, certificate.round, certificate.value_id)
                    .await;

                // Start next height
                let next_height = certificate.height.increment();
                let validator_set = state.shard_validator.get_validator_set();
                consensus_ref.cast(ConsensusMsg::StartHeight(next_height, validator_set))?;
            }

            HostMsg::GetDecidedValue { height, reply_to } => {
                // TODO: Get previously decided value for sync
            }

            HostMsg::ProcessSyncedValue {
                height,
                round,
                validator_address,
                value_bytes,
                reply_to,
            } => {
                // TODO: Convert bytes to Proposal (sync)
            }
        };

        Ok(())
    }
}

#[async_trait]
impl Actor for Host {
    type Msg = HostMsg<SnapchainValidatorContext>;
    type State = HostState;
    type Arguments = HostState;

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
