use crate::core::error::HubError;
use crate::core::types::{Shardable, SnapchainValidatorContext};
use crate::proto;
use crate::proto::HubEvent;
use crate::storage::store::engine::MessageValidationError;

// impl proto::Message, impl proto::FullProposal::shard_id, impl ConsensusMessage::shard_id
// are now in the snapchain-proto crate

// Extension trait for HubEvent that depends on main crate types (HubError, MessageValidationError)
pub trait HubEventExt {
    fn new_event(event_type: proto::HubEventType, body: proto::hub_event::Body) -> HubEvent;
    fn from_validation_error(err: MessageValidationError, message: &proto::Message) -> HubEvent;
}

impl HubEventExt for HubEvent {
    fn new_event(event_type: proto::HubEventType, body: proto::hub_event::Body) -> HubEvent {
        proto::HubEvent {
            r#type: event_type as i32,
            body: Some(body),

            // These are populated later
            block_number: 0,
            id: 0,
            shard_index: 0,
            timestamp: 0,
        }
    }

    fn from_validation_error(err: MessageValidationError, message: &proto::Message) -> HubEvent {
        let merge_error = match err.clone() {
            MessageValidationError::StoreError(hub_err) => hub_err,
            _ => HubError::validation_failure(err.to_string().as_str()),
        };
        HubEvent::new_event(
            proto::HubEventType::MergeFailure,
            proto::hub_event::Body::MergeFailure(proto::MergeFailureBody {
                message: Some(message.clone()),
                code: merge_error.code,
                reason: merge_error.message,
            }),
        )
    }
}

impl Shardable for informalsystems_malachitebft_sync::Request<SnapchainValidatorContext> {
    fn shard_id(&self) -> u32 {
        match self {
            informalsystems_malachitebft_sync::Request::ValueRequest(req) => req.height.shard_index,
            informalsystems_malachitebft_sync::Request::VoteSetRequest(req) => {
                req.height.shard_index
            }
        }
    }
}

impl Shardable for informalsystems_malachitebft_sync::Response<SnapchainValidatorContext> {
    fn shard_id(&self) -> u32 {
        match self {
            informalsystems_malachitebft_sync::Response::ValueResponse(resp) => {
                resp.height.shard_index
            }
            informalsystems_malachitebft_sync::Response::VoteSetResponse(resp) => {
                resp.height.shard_index
            }
        }
    }
}
