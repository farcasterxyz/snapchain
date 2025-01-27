use crate::core::types::{Proposal, Signature, SnapchainValidatorContext, Vote};
use crate::proto::{consensus_message, ConsensusMessage, FullProposal};
use bytes::Bytes;
use informalsystems_malachitebft_codec::Codec;
use informalsystems_malachitebft_core_consensus::SignedConsensusMsg;
use informalsystems_malachitebft_core_types::{SignedProposal, SignedVote};
use informalsystems_malachitebft_engine::util::streaming::{StreamContent, StreamMessage};
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
