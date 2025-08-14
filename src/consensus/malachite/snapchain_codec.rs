use crate::core::types::{Address, Proposal, Signature, SnapchainValidatorContext, Vote};
use crate::proto::sync_request::SyncRequest;
use crate::proto::{self};
use crate::proto::{consensus_message, ConsensusMessage, FullProposal, StatusMessage};
use bytes::Bytes;
use informalsystems_malachitebft_codec::Codec;
use informalsystems_malachitebft_core_consensus::{LivenessMsg, ProposedValue, SignedConsensusMsg};
use informalsystems_malachitebft_core_types::{
    NilOrVal, PolkaCertificate, PolkaSignature, Round, RoundCertificate, RoundCertificateType,
    RoundSignature, SignedProposal, SignedVote, Validity, VoteType,
};
use informalsystems_malachitebft_engine::util::streaming::{
    StreamContent, StreamId, StreamMessage,
};
use informalsystems_malachitebft_network::PeerId as MalachitePeerId;
use informalsystems_malachitebft_sync as sync;
use informalsystems_malachitebft_sync::{RawDecidedValue, Request, Status};
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
        let proposal: FullProposal = Self.decode(bytes)?;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&proposal.height.unwrap().as_u64().to_be_bytes());
        bytes.extend_from_slice(&proposal.round.to_be_bytes());
        let stream_id = StreamId::new(bytes.into());

        Ok(StreamMessage::new(
            stream_id,
            0,
            StreamContent::Data(proposal),
        ))
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
        let tip_height = status.tip_height.ok_or_else(|| {
            SnapchainCodecError::InvalidField("Missing height for status".to_string())
        })?;
        let history_min_height = status.min_height.ok_or_else(|| {
            SnapchainCodecError::InvalidField("Missing min_height for status".to_string())
        })?;
        Ok(Status {
            peer_id,
            tip_height,
            history_min_height,
        })
    }

    fn encode(&self, msg: &Status<SnapchainValidatorContext>) -> Result<Bytes, Self::Error> {
        let status = StatusMessage {
            peer_id: msg.peer_id.to_bytes(),
            tip_height: Some(msg.tip_height),
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
                    value: Some(RawDecidedValue {
                        value_bytes: Bytes::from(value.full_value),
                        certificate: commit_certificate,
                    }),
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
        };
        let sync_message = proto::SyncResponse {
            sync_response: Some(proto_reply),
        };
        Ok(Bytes::from(sync_message.encode_to_vec()))
    }
}

impl Codec<LivenessMsg<SnapchainValidatorContext>> for SnapchainCodec {
    type Error = SnapchainCodecError;

    fn decode(&self, bytes: Bytes) -> Result<LivenessMsg<SnapchainValidatorContext>, Self::Error> {
        let msg = proto::LivenessMessage::decode(bytes.as_ref())?;
        match msg.message {
            Some(proto::liveness_message::Message::Vote(vote)) => match vote.consensus_message {
                Some(consensus_message::ConsensusMessage::Vote(vote_msg)) => {
                    Ok(LivenessMsg::Vote(SignedVote {
                        message: Vote::from_proto(vote_msg),
                        signature: Signature(vote.signature),
                    }))
                }
                _ => {
                    return Err(SnapchainCodecError::InvalidField(
                        "Expected a vote message".to_string(),
                    ));
                }
            },
            Some(proto::liveness_message::Message::PolkaCertificate(cert)) => {
                Ok(LivenessMsg::PolkaCertificate(PolkaCertificate {
                    height: cert.height.ok_or_else(|| {
                        SnapchainCodecError::InvalidField(
                            "Missing height in PolkaCertificate".to_string(),
                        )
                    })?,
                    round: Round::from(cert.round),
                    value_id: cert.value_id.ok_or_else(|| {
                        SnapchainCodecError::InvalidField(
                            "Missing value_id in PolkaCertificate".to_string(),
                        )
                    })?,
                    polka_signatures: cert
                        .signatures
                        .into_iter()
                        .map(|sig| {
                            Ok(PolkaSignature {
                                address: Address::from_vec(sig.validator_address),
                                signature: Signature(sig.signature),
                            })
                        })
                        .collect::<Result<Vec<_>, SnapchainCodecError>>()?,
                }))
            }
            Some(proto::liveness_message::Message::RoundCertificate(cert)) => {
                let cert_type = match proto::RoundCertificateType::try_from(cert.cert_type) {
                    Ok(proto::RoundCertificateType::RoundCertPrecommit) => {
                        RoundCertificateType::Precommit
                    }
                    Ok(proto::RoundCertificateType::RoundCertSkip) => RoundCertificateType::Skip,
                    _ => {
                        return Err(SnapchainCodecError::InvalidField(
                            "Invalid cert_type in RoundCertificate".to_string(),
                        ));
                    }
                };
                Ok(LivenessMsg::SkipRoundCertificate(RoundCertificate {
                    height: cert.height.ok_or_else(|| {
                        SnapchainCodecError::InvalidField(
                            "Missing height in RoundCertificate".to_string(),
                        )
                    })?,
                    round: Round::from(cert.round),
                    cert_type,
                    round_signatures: cert
                        .signatures
                        .into_iter()
                        .map(|sig| {
                            let vote_type = match proto::VoteType::try_from(sig.vote_type) {
                                Ok(proto::VoteType::Prevote) => VoteType::Prevote,
                                Ok(proto::VoteType::Precommit) => VoteType::Precommit,
                                _ => {
                                    return Err(SnapchainCodecError::InvalidField(
                                        "Invalid vote_type in RoundCertificate".to_string(),
                                    ));
                                }
                            };
                            Ok(RoundSignature {
                                value_id: match sig.value_id {
                                    Some(id) => NilOrVal::Val(id),
                                    None => NilOrVal::Nil,
                                },
                                vote_type,
                                address: Address::from_vec(sig.validator_address),
                                signature: Signature(sig.signature),
                            })
                        })
                        .collect::<Result<Vec<_>, SnapchainCodecError>>()?,
                }))
            }
            None => Err(SnapchainCodecError::InvalidField("message".to_string())),
        }
    }

    fn encode(&self, msg: &LivenessMsg<SnapchainValidatorContext>) -> Result<Bytes, Self::Error> {
        match msg {
            LivenessMsg::Vote(vote) => {
                let signature = vote.signature.0.clone();
                let message = vote.message.to_proto();
                Ok(Bytes::from(
                    proto::LivenessMessage {
                        message: Some(proto::liveness_message::Message::Vote(ConsensusMessage {
                            consensus_message: Some(consensus_message::ConsensusMessage::Vote(
                                message,
                            )),
                            signature,
                        })),
                    }
                    .encode_to_vec(),
                ))
            }
            LivenessMsg::PolkaCertificate(cert) => {
                let shard_hash = cert.value_id.clone();
                Ok(Bytes::from(
                    proto::LivenessMessage {
                        message: Some(proto::liveness_message::Message::PolkaCertificate(
                            proto::PolkaCertificate {
                                height: Some(cert.height),
                                round: cert.round.as_i64(),
                                value_id: Some(shard_hash),
                                signatures: cert
                                    .polka_signatures
                                    .iter()
                                    .map(|sig| proto::PolkaSignature {
                                        validator_address: sig.address.to_vec(),
                                        signature: sig.signature.0.clone(),
                                    })
                                    .collect(),
                            },
                        )),
                    }
                    .encode_to_vec(),
                ))
            }
            LivenessMsg::SkipRoundCertificate(cert) => Ok(Bytes::from(
                proto::LivenessMessage {
                    message: Some(proto::liveness_message::Message::RoundCertificate(
                        proto::RoundCertificate {
                            height: Some(cert.height),
                            round: cert.round.as_i64(),
                            cert_type: match cert.cert_type {
                                RoundCertificateType::Precommit => {
                                    proto::RoundCertificateType::RoundCertPrecommit
                                }
                                RoundCertificateType::Skip => {
                                    proto::RoundCertificateType::RoundCertSkip
                                }
                            } as i32,
                            signatures: cert
                                .round_signatures
                                .iter()
                                .map(|sig| proto::RoundSignature {
                                    value_id: match &sig.value_id {
                                        NilOrVal::Val(id) => Some(id.clone()),
                                        NilOrVal::Nil => None,
                                    },
                                    vote_type: match sig.vote_type {
                                        VoteType::Prevote => proto::VoteType::Prevote,
                                        VoteType::Precommit => proto::VoteType::Precommit,
                                    } as i32,
                                    validator_address: sig.address.to_vec(),
                                    signature: sig.signature.0.clone(),
                                })
                                .collect(),
                        },
                    )),
                }
                .encode_to_vec(),
            )),
        }
    }
}

impl Codec<ProposedValue<SnapchainValidatorContext>> for SnapchainCodec {
    type Error = SnapchainCodecError;

    fn decode(
        &self,
        bytes: Bytes,
    ) -> Result<ProposedValue<SnapchainValidatorContext>, Self::Error> {
        let proposed_value = proto::Proposal::decode(bytes)?;
        let validity = proposed_value.validity.ok_or_else(|| {
            SnapchainCodecError::InvalidField("Missing validity in ProposedValue".to_string())
        })?;
        Ok(ProposedValue {
            height: proposed_value.height.ok_or_else(|| {
                SnapchainCodecError::InvalidField("Missing height in ProposedValue".to_string())
            })?,
            round: Round::from(proposed_value.round),
            valid_round: Round::from(proposed_value.pol_round),
            proposer: Address::from_vec(proposed_value.proposer),
            value: proposed_value.value.ok_or_else(|| {
                SnapchainCodecError::InvalidField("Missing value in ProposedValue".to_string())
            })?,
            validity: match validity {
                true => Validity::Valid,
                false => Validity::Invalid,
            },
        })
    }

    fn encode(&self, msg: &ProposedValue<SnapchainValidatorContext>) -> Result<Bytes, Self::Error> {
        let proposed_value = proto::Proposal {
            height: Some(msg.height),
            round: msg.round.as_i64(),
            pol_round: msg.valid_round.as_i64(),
            proposer: msg.proposer.to_vec(),
            value: Some(msg.value.clone()),
            validity: match msg.validity {
                Validity::Valid => Some(true),
                Validity::Invalid => Some(false),
            },
        };
        Ok(Bytes::from(proposed_value.encode_to_vec()))
    }
}
