//! Implementation of a host actor for bridiging consensus and the application via a set of channels.

use bytes::Bytes;
use informalsystems_malachitebft_core_consensus::{PeerId, ProposedValue};
use informalsystems_malachitebft_core_types::{CommitCertificate, Round, ValueId};
use informalsystems_malachitebft_engine::consensus::ConsensusMsg;
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef, SpawnErr};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::core::types::{SnapchainContext, SnapchainValidatorContext};
use informalsystems_malachitebft_engine::host::{HostMsg, LocallyProposedValue};
use informalsystems_malachitebft_engine::util::streaming::StreamMessage;
use informalsystems_malachitebft_sync::DecidedValue;

pub type Reply<T> = oneshot::Sender<T>;

enum AppMsg<Ctx: SnapchainContext> {
    /// Notifies the application that consensus is ready.
    ///
    /// The application MAY reply with a message to instruct
    /// consensus to start at a given height.
    ConsensusReady {
        /// Channel for sending a [`ConsensusMsg::StartHeight`] message back to consensus
        reply: Reply<ConsensusMsg<Ctx>>,
    },

    /// Notifies the application that a new consensus round has begun.
    StartedRound {
        /// Current consensus height
        height: Ctx::Height,
        /// Round that was just started
        round: Round,
        /// Proposer for that round
        proposer: Ctx::Address,
    },

    /// Requests the application to build a value for consensus to run on.
    ///
    /// The application MUST reply to this message with the requested value
    /// within the specified timeout duration.
    GetValue {
        /// Height which consensus is at
        height: Ctx::Height,
        /// Round which consensus is at
        round: Round,
        /// Maximum time allowed for the application to respond
        timeout: Duration,
        /// Channel for sending back the value just built to consensus
        reply: Reply<LocallyProposedValue<Ctx>>,
    },

    /// Requests the application to re-stream a proposal that it has already seen.
    ///
    /// The application MUST re-publish again all the proposal parts pertaining
    /// to that value by sending [`NetworkMsg::PublishProposalPart`] messages through
    /// the [`Channels::network`] channel.
    RestreamProposal {
        /// Height of the proposal
        height: Ctx::Height,
        /// Round of the proposal
        round: Round,
        /// Rround at which the proposal was locked on
        valid_round: Round,
        /// Address of the original proposer
        address: Ctx::Address,
        /// Unique identifier of the proposed value
        value_id: ValueId<Ctx>,
    },

    /// Requests the earliest height available in the history maintained by the application.
    ///
    /// The application MUST respond with its earliest available height.
    GetHistoryMinHeight { reply: Reply<Ctx::Height> },

    /// Notifies the application that consensus has received a proposal part over the network.
    ///
    /// If this part completes the full proposal, the application MUST respond
    /// with the complete proposed value. Otherwise, it MUST respond with `None`.
    ReceivedProposalPart {
        /// Peer whom the proposal part was received from
        from: PeerId,
        /// Received proposal part, together with its stream metadata
        part: StreamMessage<Ctx::ProposalPart>,
        /// Channel for returning the complete value if the proposal is now complete
        reply: Reply<Option<ProposedValue<Ctx>>>,
    },

    /// Requests the validator set for a specific height
    GetValidatorSet {
        /// Height of the validator set to retrieve
        height: Ctx::Height,
        /// Channel for sending back the validator set
        reply: Reply<Ctx::ValidatorSet>,
    },

    /// Notifies the application that consensus has decided on a value.
    ///
    /// This message includes a commit certificate containing the ID of
    /// the value that was decided on, the height and round at which it was decided,
    /// and the aggregated signatures of the validators that committed to it.
    ///
    /// In response to this message, the application MAY send a [`ConsensusMsg::StartHeight`]
    /// message back to consensus, instructing it to start the next height.
    Decided {
        /// The certificate for the decided value
        certificate: CommitCertificate<Ctx>,
        /// Channel for instructing consensus to start the next height, if desired
        reply: Reply<ConsensusMsg<Ctx>>,
    },

    /// Requests a previously decided value from the application's storage.
    ///
    /// The application MUST respond with that value if available, or `None` otherwise.
    GetDecidedValue {
        /// Height of the decided value to retrieve
        height: Ctx::Height,
        /// Channel for sending back the decided value
        reply: Reply<Option<DecidedValue<Ctx>>>,
    },

    /// Notifies the application that a value has been synced from the network.
    /// This may happen when the node is catching up with the network.
    ///
    /// If a value can be decoded from the bytes provided, then the application MUST reply
    /// to this message with the decoded value.
    ProcessSyncedValue {
        /// Height of the synced value
        height: Ctx::Height,
        /// Round of the synced value
        round: Round,
        /// Address of the original proposer
        proposer: Ctx::Address,
        /// Raw encoded value data
        value_bytes: Bytes,
        /// Channel for sending back the proposed value, if successfully decoded
        reply: Reply<ProposedValue<Ctx>>,
    },
}

/// Actor for bridging consensus and the application via a set of channels.
///
/// This actor is responsible for forwarding messages from the
/// consensus actor to the application over a channel, and vice-versa.
pub struct Connector {
    sender: mpsc::Sender<AppMsg<SnapchainValidatorContext>>,
}

impl Connector {
    pub fn new(sender: mpsc::Sender<AppMsg<SnapchainValidatorContext>>) -> Self {
        Connector { sender }
    }

    pub async fn spawn(
        sender: mpsc::Sender<AppMsg<SnapchainValidatorContext>>,
    ) -> Result<ActorRef<HostMsg<SnapchainValidatorContext>>, SpawnErr> {
        let (actor_ref, _) = Actor::spawn(None, Self::new(sender), ()).await?;
        Ok(actor_ref)
    }
}

impl Connector {
    async fn handle_msg(
        &self,
        _myself: ActorRef<HostMsg<SnapchainValidatorContext>>,
        msg: HostMsg<SnapchainValidatorContext>,
        _state: &mut (),
    ) -> Result<(), ActorProcessingErr> {
        match msg {
            HostMsg::ConsensusReady(consensus_ref) => {
                let (reply, rx) = oneshot::channel();

                self.sender.send(AppMsg::ConsensusReady { reply }).await?;

                consensus_ref.cast(rx.await?.into())?;
            }

            HostMsg::StartedRound {
                height,
                round,
                proposer,
            } => {
                self.sender
                    .send(AppMsg::StartedRound {
                        height,
                        round,
                        proposer,
                    })
                    .await?
            }

            HostMsg::GetValue {
                height,
                round,
                timeout,
                reply_to,
            } => {
                let (reply, rx) = oneshot::channel();

                self.sender
                    .send(AppMsg::GetValue {
                        height,
                        round,
                        timeout,
                        reply,
                    })
                    .await?;

                reply_to.send(rx.await?)?;
            }

            HostMsg::RestreamValue {
                height,
                round,
                valid_round,
                address,
                value_id,
            } => {
                self.sender
                    .send(AppMsg::RestreamProposal {
                        height,
                        round,
                        valid_round,
                        address,
                        value_id,
                    })
                    .await?
            }

            HostMsg::GetHistoryMinHeight { reply_to } => {
                let (reply, rx) = oneshot::channel();

                self.sender
                    .send(AppMsg::GetHistoryMinHeight { reply })
                    .await?;

                reply_to.send(rx.await?)?;
            }

            HostMsg::ReceivedProposalPart {
                from,
                part,
                reply_to,
            } => {
                let (reply, rx) = oneshot::channel();

                self.sender
                    .send(AppMsg::ReceivedProposalPart { from, part, reply })
                    .await?;

                if let Some(value) = rx.await? {
                    reply_to.send(value)?;
                }
            }

            HostMsg::GetValidatorSet { height, reply_to } => {
                let (reply, rx) = oneshot::channel();

                self.sender
                    .send(AppMsg::GetValidatorSet { height, reply })
                    .await?;

                reply_to.send(rx.await?)?;
            }

            HostMsg::Decided {
                certificate,
                consensus: consensus_ref,
            } => {
                let (reply, rx) = oneshot::channel();

                self.sender
                    .send(AppMsg::Decided { certificate, reply })
                    .await?;

                consensus_ref.cast(rx.await?.into())?;
            }

            HostMsg::GetDecidedValue { height, reply_to } => {
                let (reply, rx) = oneshot::channel();

                self.sender
                    .send(AppMsg::GetDecidedValue { height, reply })
                    .await?;

                reply_to.send(rx.await?)?;
            }

            HostMsg::ProcessSyncedValue {
                height,
                round,
                validator_address,
                value_bytes,
                reply_to,
            } => {
                let (reply, rx) = oneshot::channel();

                self.sender
                    .send(AppMsg::ProcessSyncedValue {
                        height,
                        round,
                        proposer: validator_address,
                        value_bytes,
                        reply,
                    })
                    .await?;

                reply_to.send(rx.await?)?;
            }
        };

        Ok(())
    }
}

#[async_trait]
impl Actor for Connector {
    type Msg = HostMsg<SnapchainValidatorContext>;
    type State = ();
    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(())
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        msg: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        if let Err(e) = self.handle_msg(myself, msg, state).await {
            tracing::error!("Error processing message: {e}");
        }

        Ok(())
    }
}
