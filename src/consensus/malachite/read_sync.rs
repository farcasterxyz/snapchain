use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;

use prost::Message;
use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};
use rand::SeedableRng;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use informalsystems_malachitebft_core_consensus::PeerId;
use informalsystems_malachitebft_sync::{
    self as sync, InboundRequestId, OutboundRequestId, Response,
};
use informalsystems_malachitebft_sync::{RawDecidedValue, Request};

use informalsystems_malachitebft_engine::network::{NetworkEvent, NetworkMsg, NetworkRef, Status};
use informalsystems_malachitebft_engine::util::ticker::ticker;
use informalsystems_malachitebft_engine::util::timers::{TimeoutElapsed, TimerScheduler};

use crate::core::types::SnapchainValidatorContext;
use crate::proto::{self, Height};

use super::read_host::{ReadHostMsg, ReadHostRef};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Timeout {
    Request(OutboundRequestId),
}

type Timers = TimerScheduler<Timeout>;

pub type SyncRef = ActorRef<Msg>;

#[derive(Clone, Debug)]
pub struct InflightRequest {
    pub peer_id: PeerId,
    pub request: Request<SnapchainValidatorContext>,
}

pub type InflightRequests = HashMap<OutboundRequestId, InflightRequest>;

#[derive(Debug)]
pub enum Msg {
    /// Internal tick
    Tick { reply_to: Option<RpcReplyPort<()>> },

    /// Receive an even from gossip layer
    NetworkEvent(NetworkEvent<SnapchainValidatorContext>),

    /// Consensus has decided on a value at the given height
    Decided(Height),

    /// Host has a response for the blocks request
    GotDecidedBlock(
        InboundRequestId,
        Height,
        Option<RawDecidedValue<SnapchainValidatorContext>>,
    ),

    /// A timeout has elapsed
    TimeoutElapsed(TimeoutElapsed<Timeout>),
}

pub type ReadSyncRef = ActorRef<Msg>;

impl From<NetworkEvent<SnapchainValidatorContext>> for Msg {
    fn from(event: NetworkEvent<SnapchainValidatorContext>) -> Self {
        Msg::NetworkEvent(event)
    }
}

impl From<TimeoutElapsed<Timeout>> for Msg {
    fn from(elapsed: TimeoutElapsed<Timeout>) -> Self {
        Msg::TimeoutElapsed(elapsed)
    }
}

#[derive(Debug)]
pub struct ReadParams {
    pub status_update_interval: Duration,
    pub request_timeout: Duration,
}

impl Default for ReadParams {
    fn default() -> Self {
        Self {
            status_update_interval: Duration::from_secs(5),
            request_timeout: Duration::from_secs(10),
        }
    }
}

pub struct State {
    /// The state of the sync state machine
    sync: sync::State<SnapchainValidatorContext>,

    /// Scheduler for timers
    timers: Timers,

    /// In-flight requests
    inflight: InflightRequests,

    /// Task for sending status updates
    ticker: JoinHandle<()>,

    initial_sync_completed: bool,
}

impl State {
    fn initial_sync_first_completed(&mut self) -> bool {
        if self.initial_sync_completed {
            return false;
        }

        if self.sync.peers.len() == 0 {
            return false;
        }

        let sync_height = self.sync.sync_height;
        let peer_ahead_by_threshold = self
            .sync
            .random_peer_with_value(sync_height.increment_by(10));

        self.initial_sync_completed = peer_ahead_by_threshold.is_none();
        return self.initial_sync_completed;
    }
}

#[allow(dead_code)]
pub struct ReadSync {
    ctx: SnapchainValidatorContext,
    gossip: NetworkRef<SnapchainValidatorContext>,
    host: ReadHostRef,
    params: ReadParams,
    metrics: sync::Metrics,
    span: tracing::Span,
}

impl ReadSync {
    pub fn new(
        ctx: SnapchainValidatorContext,
        gossip: NetworkRef<SnapchainValidatorContext>,
        host: ReadHostRef,
        params: ReadParams,
        metrics: sync::Metrics,
        span: tracing::Span,
    ) -> Self {
        Self {
            ctx,
            gossip,
            host,
            params,
            metrics,
            span,
        }
    }

    pub async fn spawn(
        ctx: SnapchainValidatorContext,
        gossip: NetworkRef<SnapchainValidatorContext>,
        host: ReadHostRef,
        params: ReadParams,
        metrics: sync::Metrics,
        span: tracing::Span,
    ) -> Result<SyncRef, ractor::SpawnErr> {
        let actor = Self::new(ctx, gossip, host, params, metrics, span);
        let (actor_ref, _) = Actor::spawn(None, actor, ()).await?;
        Ok(actor_ref)
    }

    async fn process_input(
        &self,
        myself: &ActorRef<Msg>,
        state: &mut State,
        input: sync::Input<SnapchainValidatorContext>,
    ) -> Result<(), ActorProcessingErr> {
        informalsystems_malachitebft_sync::process!(
            input: input,
            state: &mut state.sync,
            metrics: &self.metrics,
            with: effect => {
                self.handle_effect(myself, &mut state.timers, &mut state.inflight, effect).await
            }
        )
    }

    async fn get_history_min_height(&self) -> Result<Height, ActorProcessingErr> {
        ractor::call!(self.host, |reply_to| ReadHostMsg::GetHistoryMinHeight {
            reply_to
        })
        .map_err(|e| e.into())
    }

    async fn handle_effect(
        &self,
        myself: &ActorRef<Msg>,
        timers: &mut Timers,
        inflight: &mut InflightRequests,
        effect: sync::Effect<SnapchainValidatorContext>,
    ) -> Result<sync::Resume<SnapchainValidatorContext>, ActorProcessingErr> {
        use sync::Effect;

        match effect {
            Effect::BroadcastStatus(height) => {
                let history_min_height = self.get_history_min_height().await?;

                self.gossip.cast(NetworkMsg::BroadcastStatus(Status::new(
                    height,
                    history_min_height,
                )))?;
            }

            Effect::SendValueRequest(peer_id, value_request) => {
                debug!(
                    height = %value_request.height, peer = %peer_id,
                    "Send the value request to peer"
                );

                let request = Request::ValueRequest(value_request);
                let result = ractor::call!(self.gossip, |reply_to| {
                    NetworkMsg::OutgoingRequest(peer_id, request.clone(), reply_to)
                });

                match result {
                    Ok(request_id) => {
                        let request_id = OutboundRequestId::new(request_id);

                        timers.start_timer(
                            Timeout::Request(request_id.clone()),
                            self.params.request_timeout,
                        );

                        inflight.insert(request_id.clone(), InflightRequest { peer_id, request });
                    }
                    Err(e) => {
                        error!("Failed to send request to gossip layer: {e}");
                    }
                }
            }

            Effect::SendValueResponse(request_id, value_response) => {
                debug!(
                    height = %value_response.height, request = %request_id,
                    "Sending the value response"
                );

                let response = Response::ValueResponse(value_response);
                self.gossip
                    .cast(NetworkMsg::OutgoingResponse(request_id, response))?;
            }

            Effect::GetDecidedValue(request_id, height) => {
                self.host.call_and_forward(
                    |reply_to| ReadHostMsg::GetDecidedValue { height, reply_to },
                    myself,
                    move |synced_value| Msg::GotDecidedBlock(request_id, height, synced_value),
                    None,
                )?;
            }
            Effect::SendVoteSetRequest(peer_id, vote_set_request) => {
                error!(
                    height = %vote_set_request.height, round = %vote_set_request.round, peer = %peer_id,
                    "Read node requesting vote set"
                );
            }
        }

        Ok(sync::Resume::default())
    }

    async fn handle_msg(
        &self,
        myself: ActorRef<Msg>,
        msg: Msg,
        state: &mut State,
    ) -> Result<(), ActorProcessingErr> {
        match msg {
            Msg::Tick { reply_to } => {
                self.process_input(&myself, state, sync::Input::Tick)
                    .await?;

                if let Some(reply_to) = reply_to {
                    reply_to.send(()).unwrap();
                }
            }

            Msg::NetworkEvent(NetworkEvent::PeerDisconnected(peer_id)) => {
                info!(%peer_id, "Disconnected from peer");

                if state.sync.peers.remove(&peer_id).is_some() {
                    debug!(%peer_id, "Removed disconnected peer");
                }
            }

            Msg::NetworkEvent(NetworkEvent::Status(peer_id, status)) => {
                debug!(%peer_id, height = %status.height, "Received peer status");
                let status = sync::Status {
                    peer_id,
                    height: status.height,
                    history_min_height: status.history_min_height,
                };

                self.process_input(&myself, state, sync::Input::Status(status))
                    .await?;
            }

            Msg::NetworkEvent(NetworkEvent::Request(request_id, from, request)) => {
                match request {
                    Request::ValueRequest(value_request) => {
                        self.process_input(
                            &myself,
                            state,
                            sync::Input::ValueRequest(request_id, from, value_request),
                        )
                        .await?;
                    }
                    Request::VoteSetRequest(vote_set_request) => {
                        error!(height = %vote_set_request.height, round = %vote_set_request.round, peer = %from,
                            "Read node received vote set request");
                    }
                };
            }

            Msg::NetworkEvent(NetworkEvent::Response(request_id, peer, response)) => {
                // Cancel the timer associated with the request for which we just received a response
                state.timers.cancel(&Timeout::Request(request_id.clone()));

                match response {
                    Response::ValueResponse(value_response) => {
                        let decided_value = value_response.value.as_ref().unwrap();
                        let value_bytes =
                            value_response.value.as_ref().unwrap().value_bytes.as_ref();
                        let value = if decided_value.certificate.value_id.shard_index == 0 {
                            proto::decided_value::Value::Block(
                                proto::Block::decode(value_bytes).unwrap(),
                            )
                        } else {
                            proto::decided_value::Value::Shard(
                                proto::ShardChunk::decode(value_bytes).unwrap(),
                            )
                        };
                        debug!(peer_id = %peer, height = %decided_value.certificate.height, "Received sync value response");
                        self.host.cast(ReadHostMsg::ProcessDecidedValue {
                            value: proto::DecidedValue { value: Some(value) },
                            sync: myself.clone(),
                        })?;
                        self.process_input(
                            &myself,
                            state,
                            sync::Input::ValueResponse(request_id, peer, value_response),
                        )
                        .await?;

                        if state.initial_sync_first_completed() {
                            self.host.cast(ReadHostMsg::InitialSyncCompleted)?;
                        }
                    }
                    Response::VoteSetResponse(vote_set_response) => {
                        error!(height = %vote_set_response.height, round = %vote_set_response.round, %peer ,
                            "Read node sending vote set response");
                    }
                }
            }

            Msg::NetworkEvent(_) => {
                // Ignore other gossip events
            }

            Msg::Decided(height) => {
                self.process_input(&myself, state, sync::Input::UpdateHeight(height))
                    .await?;
                self.process_input(&myself, state, sync::Input::StartHeight(height.increment()))
                    .await?;
            }

            Msg::GotDecidedBlock(request_id, height, block) => {
                self.process_input(
                    &myself,
                    state,
                    sync::Input::GotDecidedValue(request_id, height, block),
                )
                .await?;
            }

            Msg::TimeoutElapsed(elapsed) => {
                let Some(timeout) = state.timers.intercept_timer_msg(elapsed) else {
                    // Timer was cancelled or already processed, ignore
                    return Ok(());
                };

                warn!(?timeout, "Timeout elapsed");

                match timeout {
                    Timeout::Request(request_id) => {
                        if let Some(inflight) = state.inflight.remove(&request_id) {
                            self.process_input(
                                &myself,
                                state,
                                sync::Input::SyncRequestTimedOut(
                                    inflight.peer_id,
                                    inflight.request,
                                ),
                            )
                            .await?;
                        } else {
                            debug!(%request_id, "Timeout for unknown request");
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Actor for ReadSync {
    type Msg = Msg;
    type State = State;
    type Arguments = ();

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        self.gossip
            .cast(NetworkMsg::Subscribe(Box::new(myself.clone())))?;

        let ticker = tokio::spawn(ticker(
            self.params.status_update_interval,
            myself.clone(),
            || Msg::Tick { reply_to: None },
        ));

        let rng = Box::new(rand::rngs::StdRng::from_entropy());

        Ok(State {
            sync: sync::State::new(rng),
            timers: Timers::new(Box::new(myself.clone())),
            inflight: HashMap::new(),
            ticker,
            initial_sync_completed: false,
        })
    }

    #[tracing::instrument(
        name = "sync",
        parent = &self.span,
        skip_all,
        fields(
            height.tip = %state.sync.tip_height,
            height.sync = %state.sync.sync_height,
        ),
    )]
    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        msg: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        if let Err(e) = self.handle_msg(myself, msg, state).await {
            error!("Error handling message: {e:?}");
        }

        Ok(())
    }

    async fn post_stop(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        state.ticker.abort();
        Ok(())
    }
}
