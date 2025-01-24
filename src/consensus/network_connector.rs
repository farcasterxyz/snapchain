use crate::core::types::{SnapchainContext, SnapchainValidatorContext};
use async_trait::async_trait;
use informalsystems_malachitebft_engine::network::NetworkMsg as Msg;
use informalsystems_malachitebft_engine::sync;
use informalsystems_malachitebft_network::{Channel, Event};
use informalsystems_malachitebft_sync::RawMessage;
use ractor::{Actor, ActorProcessingErr, ActorRef};
use std::collections::{BTreeSet, HashMap};
use tracing::{error, trace};

pub struct MalachiteNetworkConnector {}

pub struct NetworkConnectorState {}

#[async_trait]
impl Actor for MalachiteNetworkConnector {
    type Msg = Msg<SnapchainValidatorContext>;
    type State = NetworkConnectorState;
    type Arguments = ();

    async fn pre_start(
        &self,
        myself: ActorRef<Msg<SnapchainValidatorContext>>,
        args: (),
    ) -> Result<Self::State, ActorProcessingErr> {
        // let handle = malachitebft_network::spawn(args.keypair, args.config, args.metrics).await?;
        //
        // let (mut recv_handle, ctrl_handle) = handle.split();
        //
        // let recv_task = tokio::spawn(async move {
        //     while let Some(event) = recv_handle.recv().await {
        //         if let Err(e) = myself.cast(Msg::NewEvent(event)) {
        //             error!("Actor has died, stopping network: {e:?}");
        //             break;
        //         }
        //     }
        // });
        //
        // Ok(State::Running {
        //     peers: BTreeSet::new(),
        //     output_port: OutputPort::default(),
        //     ctrl_handle,
        //     recv_task,
        //     inbound_requests: HashMap::new(),
        // })

        Ok(NetworkConnectorState {})
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
        // let State::Running {
        //     peers,
        //     output_port,
        //     ctrl_handle,
        //     inbound_requests,
        //     ..
        // } = state
        // else {
        //     return Ok(());
        // };

        match msg {
            Msg::Subscribe(subscriber) => {}

            Msg::Publish(msg) => {}

            Msg::PublishProposalPart(msg) => {}

            Msg::BroadcastStatus(status) => {
                // let status = sync::Status {
                //     peer_id: ctrl_handle.peer_id(),
                //     height: status.height,
                //     history_min_height: status.history_min_height,
                // };
                //
                // let data = self.codec.encode(&status);
                // match data {
                //     Ok(data) => ctrl_handle.broadcast(Channel::Sync, data).await?,
                //     Err(e) => error!("Failed to encode status message: {e:?}"),
                // }
            }

            Msg::OutgoingRequest(peer_id, request, reply_to) => {
                // let request = self.codec.encode(&request);
                //
                // match request {
                //     Ok(data) => {
                //         let p2p_request_id = ctrl_handle.sync_request(peer_id, data).await?;
                //         reply_to.send(OutboundRequestId::new(p2p_request_id))?;
                //     }
                //     Err(e) => error!("Failed to encode request message: {e:?}"),
                // }
            }

            Msg::OutgoingResponse(request_id, response) => {
                // let response = self.codec.encode(&response);
                //
                // match response {
                //     Ok(data) => {
                //         let request_id = inbound_requests
                //             .remove(&request_id)
                //             .ok_or_else(|| eyre!("Unknown inbound request ID: {request_id}"))?;
                //
                //         ctrl_handle.sync_reply(request_id, data).await?
                //     }
                //     Err(e) => {
                //         error!(%request_id, "Failed to encode response message: {e:?}");
                //         return Ok(());
                //     }
                // };
            }

            Msg::NewEvent(Event::Listening(addr)) => {
                // output_port.send(NetworkEvent::Listening(addr));
            }

            Msg::NewEvent(Event::PeerConnected(peer_id)) => {
                // peers.insert(peer_id);
                // output_port.send(NetworkEvent::PeerConnected(peer_id));
            }

            Msg::NewEvent(Event::PeerDisconnected(peer_id)) => {
                // peers.remove(&peer_id);
                // output_port.send(NetworkEvent::PeerDisconnected(peer_id));
            }

            Msg::NewEvent(Event::Message(Channel::Consensus, from, data)) => {
                // let msg = match self.codec.decode(data) {
                //     Ok(msg) => msg,
                //     Err(e) => {
                //         error!(%from, "Failed to decode gossip message: {e:?}");
                //         return Ok(());
                //     }
                // };
                //
                // let event = match msg {
                //     SignedConsensusMsg::Vote(vote) => NetworkEvent::Vote(from, vote),
                //     SignedConsensusMsg::Proposal(proposal) => {
                //         NetworkEvent::Proposal(from, proposal)
                //     }
                // };
                //
                // output_port.send(event);
            }

            Msg::NewEvent(Event::Message(Channel::ProposalParts, from, data)) => {
                // let msg: StreamMessage<Ctx::ProposalPart> = match self.codec.decode(data) {
                //     Ok(stream_msg) => stream_msg,
                //     Err(e) => {
                //         error!(%from, "Failed to decode stream message: {e:?}");
                //         return Ok(());
                //     }
                // };
                //
                // trace!(
                //     %from,
                //     stream_id = %msg.stream_id,
                //     sequence = %msg.sequence,
                //     "Received proposal part"
                // );
                //
                // output_port.send(NetworkEvent::ProposalPart(from, msg));
            }

            Msg::NewEvent(Event::Message(Channel::Sync, from, data)) => {
                // let status: sync::Status<Ctx> = match self.codec.decode(data) {
                //     Ok(status) => status,
                //     Err(e) => {
                //         error!(%from, "Failed to decode status message: {e:?}");
                //         return Ok(());
                //     }
                // };
                //
                // if from != status.peer_id {
                //     error!(%from, %status.peer_id, "Mismatched peer ID in status message");
                //     return Ok(());
                // }
                //
                // trace!(%from, height = %status.height, "Received status");
                //
                // output_port.send(NetworkEvent::Status(
                //     status.peer_id,
                //     Status::new(status.height, status.history_min_height),
                // ));
            }

            Msg::NewEvent(Event::Sync(raw_msg)) => match raw_msg {
                RawMessage::Request {
                    request_id,
                    peer,
                    body,
                } => {
                    // let request: sync::Request<SnapchainValidatorContext> = match self.codec.decode(body) {
                    //     Ok(request) => request,
                    //     Err(e) => {
                    //         error!(%peer, "Failed to decode sync request: {e:?}");
                    //         return Ok(());
                    //     }
                    // };
                    //
                    // inbound_requests.insert(InboundRequestId::new(request_id), request_id);
                    //
                    // output_port.send(NetworkEvent::Request(
                    //     InboundRequestId::new(request_id),
                    //     peer,
                    //     request,
                    // ));
                }

                RawMessage::Response {
                    request_id,
                    peer,
                    body,
                } => {
                    // let response: sync::Response<Ctx> = match self.codec.decode(body) {
                    //     Ok(response) => response,
                    //     Err(e) => {
                    //         error!(%peer, "Failed to decode sync response: {e:?}");
                    //         return Ok(());
                    //     }
                    // };
                    //
                    // output_port.send(NetworkEvent::Response(
                    //     OutboundRequestId::new(request_id),
                    //     peer,
                    //     response,
                    // ));
                }
            },

            Msg::GetState { reply } => {
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
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        // let state = std::mem::replace(state, State::Stopped);
        //
        // if let State::Running {
        //     ctrl_handle,
        //     recv_task,
        //     ..
        // } = state
        // {
        //     ctrl_handle.wait_shutdown().await?;
        //     recv_task.await?;
        // }

        Ok(())
    }
}
