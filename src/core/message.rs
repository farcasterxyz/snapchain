use crate::core::error::HubError;
use crate::core::types::{Shardable, SnapchainValidatorContext};
use crate::proto;
use crate::proto::{consensus_message, ConsensusMessage, HubEvent, MessageType};
use crate::storage::store::engine::MessageValidationError;

impl proto::Message {
    pub fn is_type(&self, message_type: proto::MessageType) -> bool {
        self.data.is_some() && self.data.as_ref().unwrap().r#type == message_type as i32
    }

    pub fn fid(&self) -> u64 {
        if self.data.is_some() {
            self.data.as_ref().unwrap().fid
        } else {
            0
        }
    }

    pub fn msg_type(&self) -> MessageType {
        if self.data.is_some() {
            MessageType::try_from(self.data.as_ref().unwrap().r#type).unwrap_or(MessageType::None)
        } else {
            MessageType::None
        }
    }

    pub fn hex_hash(&self) -> String {
        hex::encode(&self.hash)
    }
}

impl proto::FullProposal {
    pub fn shard_id(&self) -> Result<u32, String> {
        if let Some(height) = &self.height {
            Ok(height.shard_index)
        } else {
            Err("No height in FullProposal".to_string())
        }
    }
}

impl ConsensusMessage {
    pub fn shard_id(&self) -> Result<u32, String> {
        if let Some(msg) = &self.consensus_message {
            match msg {
                consensus_message::ConsensusMessage::Vote(vote) => {
                    if let Some(height) = &vote.height {
                        return Ok(height.shard_index);
                    }
                }
                consensus_message::ConsensusMessage::Proposal(vote) => {
                    if let Some(height) = &vote.height {
                        return Ok(height.shard_index);
                    }
                }
            }
        }
        Err("Could not determine shard id for ConsensusMessage".to_string())
    }
}

impl proto::HubEvent {
    pub fn from(event_type: proto::HubEventType, body: proto::hub_event::Body) -> Self {
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

    pub fn from_validation_error(err: MessageValidationError, message: &proto::Message) -> Self {
        let merge_error = match err.clone() {
            MessageValidationError::StoreError(hub_err) => hub_err,
            _ => HubError::validation_failure(err.to_string().as_str()),
        };
        HubEvent::from(
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
