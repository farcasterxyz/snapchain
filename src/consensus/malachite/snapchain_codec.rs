use crate::core::types::{Proposal, Signature, SnapchainValidatorContext, Vote};
use crate::proto;
use crate::proto::sync_request::SyncRequest;
use crate::proto::{consensus_message, ConsensusMessage, FullProposal, StatusMessage};
use bytes::Bytes;
use informalsystems_malachitebft_codec::Codec;
use informalsystems_malachitebft_core_consensus::SignedConsensusMsg;
use informalsystems_malachitebft_core_types::{Round, SignedProposal, SignedVote, VoteSet};
use informalsystems_malachitebft_engine::util::streaming::{StreamContent, StreamMessage};
use informalsystems_malachitebft_network::PeerId as MalachitePeerId;
use informalsystems_malachitebft_sync as sync;
use informalsystems_malachitebft_sync::{DecidedValue, Request, Status};
use prost::{DecodeError, EncodeError, Message};
use thiserror::Error;

pub struct SnapchainCodec;

#[derive(Debug, Error)]
pub enum SnapchainCodecError {
    #[error("Failed to decode message")]
    Decode(#[from] DecodeError),
    #[error("Failed to encode message")]
    Encode(#[from] EncodeError),

    #[error("Invalid field: {0}")]
    InvalidField(String),
}

impl Codec<SignedConsensusMsg<SnapchainValidatorContext>> for SnapchainCodec {
    type Error = SnapchainCodecError;

    fn decode(
        &self,
        bytes: Bytes,
    ) -> Result<SignedConsensusMsg<SnapchainValidatorContext>, Self::Error> {
        let message = ConsensusMessage::decode(bytes)?;
        match message.consensus_message {
            Some(consensus_message::ConsensusMessage::Vote(vote)) => {
                Ok(SignedConsensusMsg::Vote(SignedVote {
                    message: Vote::from_proto(vote),
                    signature: Signature(message.signature),
                }))
            }
            Some(consensus_message::ConsensusMessage::Proposal(proposal)) => {
                Ok(SignedConsensusMsg::Proposal(SignedProposal {
                    message: Proposal::from_proto(proposal),
                    signature: Signature(message.signature),
                }))
            }
            None => Err(SnapchainCodecError::Decode(DecodeError::new(
                "No consensus message",
            ))),
        }
    }

    fn encode(
        &self,
        msg: &SignedConsensusMsg<SnapchainValidatorContext>,
    ) -> Result<Bytes, Self::Error> {
        match msg {
            SignedConsensusMsg::Vote(vote) => {
                let vote = vote.message.to_proto();
                let signature = msg.signature().0.clone();
                let consensus_message = consensus_message::ConsensusMessage::Vote(vote);
                let message = ConsensusMessage {
                    consensus_message: Some(consensus_message),
                    signature,
                };
                Ok(Bytes::from(message.encode_to_vec()))
            }
            SignedConsensusMsg::Proposal(proposal) => {
                let proposal = proposal.message.to_proto();
                let signature = msg.signature().0.clone();
                let consensus_message = consensus_message::ConsensusMessage::Proposal(proposal);
                let message = ConsensusMessage {
                    consensus_message: Some(consensus_message),
                    signature,
                };
                Ok(Bytes::from(message.encode_to_vec()))
            }
        }
    }
}

impl Codec<FullProposal> for SnapchainCodec {
    type Error = SnapchainCodecError;

    fn decode(&self, bytes: Bytes) -> Result<FullProposal, Self::Error> {
        FullProposal::decode(bytes).map_err(SnapchainCodecError::Decode)
    }

    fn encode(&self, msg: &FullProposal) -> Result<Bytes, Self::Error> {
        Ok(Bytes::from(msg.encode_to_vec()))
    }
}

// Since we're always sending full proposals, just encode that directly instead of using StreamMessage
impl Codec<StreamMessage<FullProposal>> for SnapchainCodec {
    type Error = SnapchainCodecError;

    fn decode(&self, bytes: Bytes) -> Result<StreamMessage<FullProposal>, Self::Error> {
        let proposal = Self.decode(bytes)?;
        Ok(StreamMessage::new(0, 0, StreamContent::Data(proposal)))
    }

    fn encode(&self, msg: &StreamMessage<FullProposal>) -> Result<Bytes, Self::Error> {
        msg.content.as_data().map_or(
            Err(SnapchainCodecError::InvalidField(
                "StreamMessage content could not be encoded to FullProposal".to_string(),
            )),
            |proposal| self.encode(proposal),
        )
    }
}

// Sync codecs

impl Codec<sync::Status<SnapchainValidatorContext>> for SnapchainCodec {
    type Error = SnapchainCodecError;

    fn decode(&self, bytes: Bytes) -> Result<Status<SnapchainValidatorContext>, Self::Error> {
        let status = StatusMessage::decode(bytes)?;
        let peer_id = MalachitePeerId::from_bytes(&status.peer_id).map_err(|_| {
            SnapchainCodecError::InvalidField("Invalid peer ID for status".to_string())
        })?;
        let height = status.height.ok_or_else(|| {
            SnapchainCodecError::InvalidField("Missing height for status".to_string())
        })?;
        let history_min_height = status.min_height.ok_or_else(|| {
            SnapchainCodecError::InvalidField("Missing min_height for status".to_string())
        })?;
        Ok(Status {
            peer_id,
            height,
            history_min_height,
        })
    }

    fn encode(&self, msg: &Status<SnapchainValidatorContext>) -> Result<Bytes, Self::Error> {
        let status = StatusMessage {
            peer_id: msg.peer_id.to_bytes(),
            height: Some(msg.height),
            min_height: Some(msg.history_min_height),
        };
        Ok(Bytes::from(status.encode_to_vec()))
    }
}

impl Codec<sync::Request<SnapchainValidatorContext>> for SnapchainCodec {
    type Error = SnapchainCodecError;

    fn decode(
        &self,
        bytes: Bytes,
    ) -> Result<sync::Request<SnapchainValidatorContext>, Self::Error> {
        let decoded = proto::SyncRequest::decode(bytes).map_err(SnapchainCodecError::Decode)?;
        let decoded_request = decoded
            .sync_request
            .ok_or_else(|| SnapchainCodecError::InvalidField("Missing sync_request".to_string()))?;
        match decoded_request {
            SyncRequest::Value(value) => Ok(sync::Request::ValueRequest(sync::ValueRequest {
                height: value.height.ok_or_else(|| {
                    SnapchainCodecError::InvalidField("Missing height in ValueRequest".to_string())
                })?,
            })),
            SyncRequest::VoteSet(vote_set) => {
                Ok(sync::Request::VoteSetRequest(sync::VoteSetRequest {
                    height: vote_set.height.ok_or_else(|| {
                        SnapchainCodecError::InvalidField(
                            "Missing height in VoteSetRequest".to_string(),
                        )
                    })?,
                    round: Round::from(vote_set.round),
                }))
            }
        }
    }

    fn encode(
        &self,
        request: &sync::Request<SnapchainValidatorContext>,
    ) -> Result<Bytes, Self::Error> {
        let proto_request = match request {
            Request::ValueRequest(value) => {
                proto::sync_request::SyncRequest::Value(proto::SyncValueRequest {
                    height: Some(value.height),
                })
            }
            Request::VoteSetRequest(vote_set) => {
                proto::sync_request::SyncRequest::VoteSet(proto::SyncVoteSetRequest {
                    height: Some(vote_set.height),
                    round: vote_set.round.as_i64(),
                })
            }
        };
        let sync_message = proto::SyncRequest {
            sync_request: Some(proto_request),
        };
        Ok(Bytes::from(sync_message.encode_to_vec()))
    }
}

impl Codec<sync::Response<SnapchainValidatorContext>> for SnapchainCodec {
    type Error = SnapchainCodecError;

    fn decode(
        &self,
        bytes: Bytes,
    ) -> Result<sync::Response<SnapchainValidatorContext>, Self::Error> {
        let decoded = proto::SyncResponse::decode(bytes).map_err(SnapchainCodecError::Decode)?;
        let decoded_response = decoded.sync_response.ok_or_else(|| {
            SnapchainCodecError::InvalidField("Missing sync_response".to_string())
        })?;
        match decoded_response {
            proto::sync_response::SyncResponse::Value(value) => {
                let commits = value.commits.ok_or_else(|| {
                    SnapchainCodecError::InvalidField(
                        "Missing commits in ValueResponse".to_string(),
                    )
                })?;
                let commit_certificate = commits.to_commit_certificate();
                Ok(sync::Response::ValueResponse(sync::ValueResponse {
                    height: value.height.ok_or_else(|| {
                        SnapchainCodecError::InvalidField(
                            "Missing height in ValueResponse".to_string(),
                        )
                    })?,
                    value: Some(DecidedValue {
                        value_bytes: Bytes::from(value.full_value),
                        certificate: commit_certificate,
                    }),
                }))
            }
            proto::sync_response::SyncResponse::VoteSet(vote_set) => {
                let signed_votes = vote_set
                    .votes
                    .into_iter()
                    .zip(vote_set.signatures)
                    .map(|(vote, signature)| SignedVote {
                        message: Vote::from_proto(vote),
                        signature: Signature(signature),
                    })
                    .collect();
                Ok(sync::Response::VoteSetResponse(sync::VoteSetResponse {
                    height: vote_set.height.ok_or_else(|| {
                        SnapchainCodecError::InvalidField(
                            "Missing height in VoteSetResponse".to_string(),
                        )
                    })?,
                    round: Round::from(vote_set.round),
                    vote_set: VoteSet::new(signed_votes),
                }))
            }
        }
    }

    fn encode(
        &self,
        response: &sync::Response<SnapchainValidatorContext>,
    ) -> Result<Bytes, Self::Error> {
        let proto_reply = match response {
            sync::Response::ValueResponse(value) => match &value.value {
                Some(decided_value) => {
                    proto::sync_response::SyncResponse::Value(proto::SyncValueResponse {
                        height: Some(value.height),
                        full_value: decided_value.value_bytes.to_vec(),
                        commits: Some(proto::Commits::from_commit_certificate(
                            &decided_value.certificate,
                        )),
                    })
                }
                None => {
                    return Err(SnapchainCodecError::InvalidField(
                        "Missing value in ValueResponse".to_string(),
                    ));
                }
            },
            sync::Response::VoteSetResponse(vote_set) => {
                let votes = vote_set
                    .vote_set
                    .votes
                    .iter()
                    .map(|vote| vote.to_proto())
                    .collect();
                let signatures: Vec<Vec<u8>> = vote_set
                    .vote_set
                    .votes
                    .iter()
                    .map(|vote| vote.signature.0.clone())
                    .collect();
                proto::sync_response::SyncResponse::VoteSet(proto::SyncVoteSetResponse {
                    height: Some(vote_set.height),
                    round: vote_set.round.as_i64(),
                    votes,
                    signatures,
                })
            }
        };
        let sync_message = proto::SyncResponse {
            sync_response: Some(proto_reply),
        };
        Ok(Bytes::from(sync_message.encode_to_vec()))
    }
}
