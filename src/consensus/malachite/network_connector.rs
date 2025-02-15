use crate::core::types::SnapchainValidatorContext;
use crate::network::gossip::GossipEvent;
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use async_trait::async_trait;
use informalsystems_malachitebft_core_consensus::SignedConsensusMsg;
use informalsystems_malachitebft_engine::consensus::ConsensusCodec;
use informalsystems_malachitebft_engine::network::{NetworkEvent, NetworkMsg as Msg, Status};
use informalsystems_malachitebft_engine::sync::SyncCodec;
use informalsystems_malachitebft_engine::util::streaming::StreamMessage;
use informalsystems_malachitebft_network::PeerId as MalachitePeerId;
use informalsystems_malachitebft_network::{Channel, Event};
use informalsystems_malachitebft_sync::{self as sync, RawMessage};
use libp2p::request_response;
use ractor::{Actor, ActorProcessingErr, ActorRef, OutputPort};
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, trace};

pub type MalachiteNetworkActorMsg = Msg<SnapchainValidatorContext>;
pub type MalachiteNetworkEvent = Event;

pub struct MalachiteNetworkConnector<Codec> {
    pub codec: Codec,
}

pub struct NetworkConnectorState {
    peer_id: MalachitePeerId,
    output_port: OutputPort<NetworkEvent<SnapchainValidatorContext>>,
    inbound_requests: HashMap<sync::InboundRequestId, request_response::InboundRequestId>,
    gossip_tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
    statsd_client: StatsdClientWrapper,
    shard_id: u32,
}

fn count(
    statsd_client: &StatsdClientWrapper,
    shard_id: u32,
    key: &str,
    value: u64,
    tags: Vec<(String, String)>,
) {
    statsd_client.count_with_shard(
        shard_id,
        format!("network_connector.{}", key).as_str(),
        value,
        tags,
    );
}

fn gauge(
    statsd_client: &StatsdClientWrapper,
    shard_id: u32,
    key: &str,
    value: u64,
    tags: Vec<(String, String)>,
) {
    statsd_client.gauge_with_shard(
        shard_id,
        format!("network_connector.{}", key).as_str(),
        value,
        tags,
    );
}

pub struct NetworkConnectorArgs {
    pub gossip_tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
    pub peer_id: MalachitePeerId,
    pub statsd_client: StatsdClientWrapper,
    pub shard_id: u32,
}

impl<Codec> MalachiteNetworkConnector<Codec>
where
    Codec: ConsensusCodec<SnapchainValidatorContext> + SyncCodec<SnapchainValidatorContext>,
{
    pub fn new(codec: Codec) -> Self {
        Self { codec }
    }

    pub async fn spawn(
        codec: Codec,
        args: NetworkConnectorArgs,
    ) -> Result<ActorRef<Msg<SnapchainValidatorContext>>, ractor::SpawnErr> {
        let (actor_ref, _) = Actor::spawn(None, Self::new(codec), args).await?;
        Ok(actor_ref)
    }
}

#[async_trait]
impl<Codec> Actor for MalachiteNetworkConnector<Codec>
where
    Codec: ConsensusCodec<SnapchainValidatorContext> + SyncCodec<SnapchainValidatorContext>,
{
    type Msg = Msg<SnapchainValidatorContext>;
    type State = NetworkConnectorState;
    type Arguments = NetworkConnectorArgs;

    async fn pre_start(
        &self,
        _myself: ActorRef<Msg<SnapchainValidatorContext>>,
        args: NetworkConnectorArgs,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(NetworkConnectorState {
            output_port: OutputPort::default(),
            gossip_tx: args.gossip_tx.clone(),
            inbound_requests: HashMap::new(),
            peer_id: args.peer_id,
            statsd_client: args.statsd_client,
            shard_id: args.shard_id,
        })
    }

    async fn post_start(
        &self,
        _myself: ActorRef<Msg<SnapchainValidatorContext>>,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        Ok(())
    }

    async fn handle(
        &self,
        _myself: ActorRef<Msg<SnapchainValidatorContext>>,
        msg: Msg<SnapchainValidatorContext>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        let NetworkConnectorState {
            output_port,
            gossip_tx,
            inbound_requests,
            peer_id: _,
            statsd_client,
            shard_id,
        } = state;

        match msg {
            Msg::Subscribe(subscriber) => {
                subscriber.subscribe_to_port(output_port);
            }

            Msg::Publish(msg) => match msg {
                SignedConsensusMsg::Vote(vote) => {
                    count(statsd_client, *shard_id, "send_vote", 1, vec![]);
                    gauge(
                        statsd_client,
                        *shard_id,
                        "send_vote_height",
                        vote.height.block_number,
                        vec![],
                    );
                    gossip_tx
                        .send(GossipEvent::BroadcastSignedVote(vote))
                        .await?;
                }
                SignedConsensusMsg::Proposal(proposal) => {
                    count(statsd_client, *shard_id, "send_proposal", 1, vec![]);
                    gauge(
                        statsd_client,
                        *shard_id,
                        "send_proposal_height",
                        proposal.height.block_number,
                        vec![],
                    );
                    gossip_tx
                        .send(GossipEvent::BroadcastSignedProposal(proposal))
                        .await?;
                }
            },

            Msg::PublishProposalPart(msg) => {
                if let Some(full_proposal) = msg.content.as_data() {
                    count(statsd_client, *shard_id, "send_proposal_part", 1, vec![]);
                    gauge(
                        statsd_client,
                        *shard_id,
                        "send_proposal_part_height",
                        full_proposal.height().block_number,
                        vec![],
                    );
                    gossip_tx
                        .send(GossipEvent::BroadcastFullProposal(full_proposal.clone()))
                        .await?;
                } else {
                    error!("Could not map proposal part to full proposal for gossip");
                }
            }

            Msg::BroadcastStatus(status) => {
                let height = status.height;
                let status = sync::Status {
                    peer_id: state.peer_id,
                    height,
                    history_min_height: status.history_min_height,
                };
                count(statsd_client, *shard_id, "send_status", 1, vec![]);
                gauge(
                    statsd_client,
                    *shard_id,
                    "broadcast_status_height",
                    height.block_number,
                    vec![],
                );
                gossip_tx.send(GossipEvent::BroadcastStatus(status)).await?
            }

            Msg::OutgoingRequest(peer_id, request, reply_to) => {
                let (tx, rx) = oneshot::channel();
                count(
                    statsd_client,
                    *shard_id,
                    "send_sync_request",
                    1,
                    vec![("peer_id".to_string(), peer_id.to_string())],
                );
                gossip_tx
                    .send(GossipEvent::SyncRequest(peer_id, request, tx))
                    .await?;
                let request_id = rx.await?;
                reply_to.send(sync::OutboundRequestId::new(request_id))?;
            }

            Msg::OutgoingResponse(request_id, response) => {
                let request_id = inbound_requests.remove(&request_id);
                if let Some(request_id) = request_id {
                    count(statsd_client, *shard_id, "send_sync_reply", 1, vec![]);
                    gossip_tx
                        .send(GossipEvent::SyncReply(request_id, response))
                        .await?;
                } else {
                    error!("Unknown request ID in response: {:?}", request_id);
                }
            }

            Msg::NewEvent(Event::Listening(addr)) => {
                output_port.send(NetworkEvent::Listening(addr));
            }

            Msg::NewEvent(Event::PeerConnected(peer_id)) => {
                // peers.insert(peer_id);
                count(
                    statsd_client,
                    *shard_id,
                    "recv_peer_connected",
                    1,
                    vec![("peer_id".to_string(), peer_id.to_string())],
                );
                info!(peer_id = peer_id.to_string(), shard_id, "Peer connected");
                output_port.send(NetworkEvent::PeerConnected(peer_id));
            }

            Msg::NewEvent(Event::PeerDisconnected(peer_id)) => {
                // peers.remove(&peer_id);
                count(
                    statsd_client,
                    *shard_id,
                    "recv_peer_disconnected",
                    1,
                    vec![("peer_id".to_string(), peer_id.to_string())],
                );
                info!(peer_id = peer_id.to_string(), shard_id, "Peer disconnected");
                output_port.send(NetworkEvent::PeerDisconnected(peer_id));
            }

            Msg::NewEvent(Event::Message(Channel::Consensus, from, data)) => {
                let msg = match self.codec.decode(data) {
                    Ok(msg) => msg,
                    Err(e) => {
                        error!(%from, "Failed to decode gossip message: {e:?}");
                        return Ok(());
                    }
                };

                let event = match msg {
                    SignedConsensusMsg::Vote(vote) => {
                        count(
                            statsd_client,
                            *shard_id,
                            "recv_vote",
                            1,
                            vec![("peer_id".to_string(), from.to_string())],
                        );
                        gauge(
                            statsd_client,
                            *shard_id,
                            "recv_vote_height",
                            vote.height.block_number,
                            vec![("peer_id".to_string(), from.to_string())],
                        );
                        NetworkEvent::Vote(from, vote)
                    }

                    SignedConsensusMsg::Proposal(proposal) => {
                        count(
                            statsd_client,
                            *shard_id,
                            "recv_proposal",
                            1,
                            vec![("peer_id".to_string(), from.to_string())],
                        );
                        gauge(
                            statsd_client,
                            *shard_id,
                            "recv_proposal_height",
                            proposal.height.block_number,
                            vec![("peer_id".to_string(), from.to_string())],
                        );
                        debug!("Received proposal from network");
                        NetworkEvent::Proposal(from, proposal)
                    }
                };

                output_port.send(event);
            }

            Msg::NewEvent(Event::Message(Channel::ProposalParts, from, data)) => {
                debug!("Received proposal parts from network");
                count(statsd_client, *shard_id, "recv_proposal_parts", 1, vec![]);
                let msg: StreamMessage<<SnapchainValidatorContext as informalsystems_malachitebft_core_types::Context>::ProposalPart> = match self.codec.decode(data) {
                    Ok(stream_msg) => stream_msg,
                    Err(e) => {
                        error!(%from, "Failed to decode stream message: {e:?}");
                        return Ok(());
                    }
                };

                trace!(
                    %from,
                    stream_id = %msg.stream_id,
                    sequence = %msg.sequence,
                    "Received proposal part"
                );

                output_port.send(NetworkEvent::ProposalPart(from, msg));
            }

            Msg::NewEvent(Event::Message(Channel::Sync, from, data)) => {
                let status: sync::Status<SnapchainValidatorContext> = match self.codec.decode(data)
                {
                    Ok(status) => status,
                    Err(e) => {
                        error!(%from, "Failed to decode status message: {e:?}");
                        return Ok(());
                    }
                };
                count(
                    statsd_client,
                    *shard_id,
                    "recv_peer_status",
                    1,
                    vec![("peer_id".to_string(), from.to_string())],
                );
                gauge(
                    statsd_client,
                    *shard_id,
                    "peer_height",
                    status.height.block_number,
                    vec![("peer_id".to_string(), status.peer_id.to_string())],
                );

                // We don't need this check because we're using gossip and not broadcast
                // if from != status.peer_id {
                //     error!(%from, %status.peer_id, "Mismatched peer ID in status message");
                //     return Ok(());
                // }

                trace!(%from, height = %status.height, "Received status");

                output_port.send(NetworkEvent::Status(
                    status.peer_id,
                    Status::new(status.height, status.history_min_height),
                ));
            }

            Msg::NewEvent(Event::Sync(raw_msg)) => match raw_msg {
                RawMessage::Request {
                    request_id,
                    peer,
                    body,
                } => {
                    let request: sync::Request<SnapchainValidatorContext> =
                        match self.codec.decode(body) {
                            Ok(request) => request,
                            Err(e) => {
                                error!(%peer, "Failed to decode sync request: {e:?}");
                                return Ok(());
                            }
                        };
                    count(
                        statsd_client,
                        *shard_id,
                        "recv_sync_request",
                        1,
                        vec![("peer_id".to_string(), peer.to_string())],
                    );

                    inbound_requests.insert(sync::InboundRequestId::new(request_id), request_id);

                    output_port.send(NetworkEvent::Request(
                        sync::InboundRequestId::new(request_id),
                        peer,
                        request,
                    ));
                }

                RawMessage::Response {
                    request_id,
                    peer,
                    body,
                } => {
                    let response: sync::Response<SnapchainValidatorContext> =
                        match self.codec.decode(body) {
                            Ok(response) => response,
                            Err(e) => {
                                error!(%peer, "Failed to decode sync response: {e:?}");
                                return Ok(());
                            }
                        };
                    count(
                        statsd_client,
                        *shard_id,
                        "recv_sync_response",
                        1,
                        vec![("peer_id".to_string(), peer.to_string())],
                    );

                    output_port.send(NetworkEvent::Response(
                        sync::OutboundRequestId::new(request_id),
                        peer,
                        response,
                    ));
                }
            },

            Msg::GetState { reply: _ } => {
                // Unused

                // let number_peers = match state {
                //     State::Stopped => 0,
                //     State::Running { peers, .. } => peers.len(),
                // };
                // reply.send(number_peers)?;
            }
        }

        Ok(())
    }

    async fn post_stop(
        &self,
        _myself: ActorRef<Msg<SnapchainValidatorContext>>,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        Ok(())
    }
}
