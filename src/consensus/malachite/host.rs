//! Implementation of a host actor for bridiging consensus and the application via a set of channels.

use crate::consensus::validator::ShardValidator;
use crate::core::types::SnapchainValidatorContext;
use crate::proto::{full_proposal, Block, Commits, FullProposal, ShardChunk};
use bytes::Bytes;
use informalsystems_malachitebft_engine::consensus::ConsensusMsg;
use informalsystems_malachitebft_engine::host::{HostMsg, LocallyProposedValue};
use informalsystems_malachitebft_engine::network::{NetworkMsg, NetworkRef};
use informalsystems_malachitebft_engine::util::streaming::{
    StreamContent, StreamId, StreamMessage,
};
use informalsystems_malachitebft_sync::RawDecidedValue;
use prost::Message;
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef, SpawnErr};
use tracing::{error, info, warn};

/// Actor for bridging consensus and the application via a set of channels.
///
/// This actor is responsible for forwarding messages from the
/// consensus actor to the application over a channel, and vice-versa.
pub struct Host {}

pub struct HostState {
    pub shard_validator: ShardValidator,
    pub network: NetworkRef<SnapchainValidatorContext>,
    pub consensus_start_delay: u32,
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
                // Wait a few seconds before starting
                tokio::time::sleep(tokio::time::Duration::from_secs(
                    state.consensus_start_delay as u64,
                ))
                .await;
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
                let locally_proposed_value = LocallyProposedValue::new(height, round, shard_hash);
                reply_to.send(locally_proposed_value)?;

                // Next, broadcast the value to the network
                let mut bytes = Vec::new();
                bytes.extend_from_slice(&height.as_u64().to_be_bytes());
                bytes.extend_from_slice(&round.as_i64().to_be_bytes());
                let stream_id = StreamId::new(bytes.into());
                let stream_message = StreamMessage::new(stream_id, 0, StreamContent::Data(value));
                state
                    .network
                    .cast(NetworkMsg::PublishProposalPart(stream_message))?;
            }

            HostMsg::RestreamValue {
                height,
                round,
                valid_round,
                address,
                value_id,
            } => {
                // This is only called for pol_rounds which we're not using?
                warn!("RestreamValue at height: {height}, round: {round}, valid_round: {valid_round}, value_id.hash: {:#?}, value_id.shard_index: {:#?}", hex::encode(&value_id.hash), value_id.shard_index);
                let full_proposal = state.shard_validator.get_proposed_value(&value_id);
                match full_proposal {
                    None => {
                        error!(
                            "Could not find previously proposed value for RestreamValue: {}",
                            hex::encode(&value_id.hash)
                        );
                    }
                    Some(full_proposal) => {
                        if full_proposal.height() != height
                            || full_proposal.round() != round
                            || full_proposal.proposer != address.to_vec()
                        {
                            info!(request_height = height.as_u64(), proposal_height = full_proposal.height().as_u64(), request_round = round.as_i64(), proposal_round = full_proposal.round().as_i64(), request_address= hex::encode(address.to_vec()), proposal_address = hex::encode(&full_proposal.proposer), "Proposal published in RestreamValue does not match height/round/proposer in the request")
                        }
                        let mut bytes = Vec::new();
                        bytes.extend_from_slice(&height.as_u64().to_be_bytes());
                        bytes.extend_from_slice(&round.as_i64().to_be_bytes());
                        let stream_id = StreamId::new(bytes.into());
                        let stream_message =
                            StreamMessage::new(stream_id, 0, StreamContent::Data(full_proposal));
                        state
                            .network
                            .cast(NetworkMsg::PublishProposalPart(stream_message))?;
                    }
                }
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

            HostMsg::GetValidatorSet {
                height: _,
                reply_to,
            } => {
                reply_to.send(state.shard_validator.get_validator_set())?;
            }

            HostMsg::Decided {
                certificate,
                consensus: consensus_ref,
                extensions: _,
            } => {
                //commit
                state
                    .shard_validator
                    .decide(Commits::from_commit_certificate(&certificate))
                    .await;

                // Start next height
                let next_height = certificate.height.increment();
                let validator_set = state.shard_validator.get_validator_set();
                consensus_ref.cast(ConsensusMsg::StartHeight(next_height, validator_set))?;
            }

            HostMsg::GetDecidedValue { height, reply_to } => {
                let proposal = state.shard_validator.get_decided_value(height).await;
                let decided_value = match proposal {
                    Some((commits, proposal)) => match proposal {
                        full_proposal::ProposedValue::Block(block) => Some(RawDecidedValue {
                            certificate: commits.to_commit_certificate(),
                            value_bytes: Bytes::from(block.encode_to_vec()),
                        }),
                        full_proposal::ProposedValue::Shard(shard_chunk) => Some(RawDecidedValue {
                            certificate: commits.to_commit_certificate(),
                            value_bytes: Bytes::from(shard_chunk.encode_to_vec()),
                        }),
                    },
                    None => None,
                };
                reply_to.send(decided_value)?;
            }

            HostMsg::ProcessSyncedValue {
                height,
                round,
                validator_address,
                value_bytes,
                reply_to,
            } => {
                let proposal = if height.shard_index == 0 {
                    let decoded_block = Block::decode(value_bytes.as_ref()).unwrap();
                    FullProposal {
                        height: Some(height),
                        round: round.as_i64(),
                        proposer: validator_address.to_vec(),
                        proposed_value: Some(full_proposal::ProposedValue::Block(decoded_block)),
                    }
                } else {
                    let chunk = ShardChunk::decode(value_bytes.as_ref()).unwrap();
                    FullProposal {
                        height: Some(height),
                        round: round.as_i64(),
                        proposer: validator_address.to_vec(),
                        proposed_value: Some(full_proposal::ProposedValue::Shard(chunk)),
                    }
                };
                let proposed_value = state.shard_validator.add_proposed_value(&proposal);
                info!(
                    height = height.to_string(),
                    "Processed value via sync: {}", proposed_value.value
                );
                reply_to.send(proposed_value)?;
            }

            // We don't use vote extensions, and don't care about peers joining or leaving here
            HostMsg::ExtendVote {
                height: _,
                round: _,
                value_id: _,
                reply_to,
            } => {
                reply_to.send(None)?;
            }
            HostMsg::VerifyVoteExtension {
                height: _,
                round: _,
                value_id: _,
                extension: _,
                reply_to,
            } => reply_to.send(Ok(()))?,
            HostMsg::PeerJoined { .. } => {}
            HostMsg::PeerLeft { .. } => {}
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
