#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use informalsystems_malachitebft_codec::Codec;
    use informalsystems_malachitebft_core_consensus::SignedConsensusMsg;
    use informalsystems_malachitebft_engine::util::streaming::StreamMessage;
    use informalsystems_malachitebft_sync as sync;
    use prost::Message;

    use crate::consensus::malachite::snapchain_codec::SnapchainCodec;
    use crate::core::types::{SnapchainValidatorContext, WellFormedProposal};
    use crate::proto::{
        self, consensus_message, full_proposal::ProposedValue, CommitSignature, Commits,
        ConsensusMessage, FullProposal, SyncResponse, SyncValueResponse, SyncVoteSetRequest,
        SyncVoteSetResponse,
    };
    use crate::utils::factory::shard_chunk_factory;

    const ROUND_OVERFLOW: i64 = u32::MAX as i64 + 1;

    fn height() -> proto::Height {
        proto::Height {
            shard_index: 1,
            block_number: 1,
        }
    }

    fn addr32() -> Vec<u8> {
        vec![0x11u8; 32]
    }

    fn shard_hash() -> proto::ShardHash {
        proto::ShardHash {
            shard_index: 1,
            hash: vec![0u8; 20],
        }
    }

    fn assert_rejected<T, E: std::fmt::Display>(result: Result<T, E>, expected: &str) {
        match result {
            Ok(_) => panic!("expected decode to fail with {expected:?}"),
            Err(err) => assert!(
                err.to_string().contains(expected),
                "expected error containing {expected:?}, got {err}"
            ),
        }
    }

    fn decode_consensus(
        message: consensus_message::ConsensusMessage,
    ) -> Result<SignedConsensusMsg<SnapchainValidatorContext>, impl std::fmt::Display> {
        let message = ConsensusMessage {
            consensus_message: Some(message),
            signature: vec![0u8; 64],
        };
        Codec::<SignedConsensusMsg<SnapchainValidatorContext>>::decode(
            &SnapchainCodec,
            Bytes::from(message.encode_to_vec()),
        )
    }

    fn decode_vote(
        vote: proto::Vote,
    ) -> Result<SignedConsensusMsg<SnapchainValidatorContext>, impl std::fmt::Display> {
        decode_consensus(consensus_message::ConsensusMessage::Vote(vote))
    }

    fn decode_proposal(
        proposal: proto::Proposal,
    ) -> Result<SignedConsensusMsg<SnapchainValidatorContext>, impl std::fmt::Display> {
        decode_consensus(consensus_message::ConsensusMessage::Proposal(proposal))
    }

    fn decode_sync_response(
        response: proto::sync_response::SyncResponse,
    ) -> Result<sync::Response<SnapchainValidatorContext>, impl std::fmt::Display> {
        let response = SyncResponse {
            sync_response: Some(response),
        };
        Codec::<sync::Response<SnapchainValidatorContext>>::decode(
            &SnapchainCodec,
            Bytes::from(response.encode_to_vec()),
        )
    }

    fn decode_value_response(
        commits: Commits,
    ) -> Result<sync::Response<SnapchainValidatorContext>, impl std::fmt::Display> {
        decode_sync_response(proto::sync_response::SyncResponse::Value(
            SyncValueResponse {
                height: Some(height()),
                full_value: vec![0u8; 8],
                commits: Some(commits),
            },
        ))
    }

    fn well_formed_proposal() -> FullProposal {
        FullProposal {
            height: Some(height()),
            round: 0,
            proposer: addr32(),
            proposed_value: Some(ProposedValue::Shard(
                shard_chunk_factory::create_shard_chunk(1, Some(1), Some(0)),
            )),
        }
    }

    /// Decodes through both proposal codecs that peers reach, and checks they agree.
    fn assert_full_proposal_rejected(proposal: FullProposal, expected: &str) {
        let bytes = Bytes::from(proposal.encode_to_vec());
        assert_rejected(
            Codec::<FullProposal>::decode(&SnapchainCodec, bytes.clone()),
            expected,
        );
        assert_rejected(
            Codec::<StreamMessage<FullProposal>>::decode(&SnapchainCodec, bytes),
            expected,
        );
    }

    #[test]
    fn rejects_vote_with_short_voter() {
        assert_rejected(
            decode_vote(proto::Vote {
                height: Some(height()),
                voter: vec![0u8; 16],
                ..Default::default()
            }),
            "Invalid address length: 16",
        );
    }

    #[test]
    fn rejects_vote_with_unknown_type() {
        assert_rejected(
            decode_vote(proto::Vote {
                height: Some(height()),
                voter: addr32(),
                r#type: 2,
                ..Default::default()
            }),
            "Invalid vote type: 2",
        );
    }

    #[test]
    fn rejects_vote_with_round_above_u32() {
        assert_rejected(
            decode_vote(proto::Vote {
                height: Some(height()),
                voter: addr32(),
                round: ROUND_OVERFLOW,
                ..Default::default()
            }),
            "Round out of range",
        );
    }

    #[test]
    fn rejects_proposal_with_short_proposer() {
        assert_rejected(
            decode_proposal(proto::Proposal {
                height: Some(height()),
                value: Some(shard_hash()),
                proposer: vec![0u8; 8],
                ..Default::default()
            }),
            "Invalid address length: 8",
        );
    }

    #[test]
    fn rejects_proposal_without_value() {
        assert_rejected(
            decode_proposal(proto::Proposal {
                height: Some(height()),
                proposer: addr32(),
                ..Default::default()
            }),
            "Missing proposal value",
        );
    }

    #[test]
    fn rejects_proposal_with_round_above_u32() {
        assert_rejected(
            decode_proposal(proto::Proposal {
                height: Some(height()),
                value: Some(shard_hash()),
                proposer: addr32(),
                round: ROUND_OVERFLOW,
                ..Default::default()
            }),
            "Round out of range",
        );
    }

    #[test]
    fn rejects_proposal_with_pol_round_above_u32() {
        assert_rejected(
            decode_proposal(proto::Proposal {
                height: Some(height()),
                value: Some(shard_hash()),
                proposer: addr32(),
                pol_round: ROUND_OVERFLOW,
                ..Default::default()
            }),
            "Round out of range",
        );
    }

    #[test]
    fn rejects_synced_commits_with_short_signer() {
        assert_rejected(
            decode_value_response(Commits {
                height: Some(height()),
                value: Some(shard_hash()),
                signatures: vec![CommitSignature {
                    signer: vec![0u8; 16],
                    signature: vec![0u8; 64],
                }],
                ..Default::default()
            }),
            "Invalid address length: 16",
        );
    }

    #[test]
    fn rejects_synced_commits_without_height() {
        assert_rejected(
            decode_value_response(Commits {
                value: Some(shard_hash()),
                ..Default::default()
            }),
            "Missing commits height",
        );
    }

    #[test]
    fn rejects_synced_commits_without_value() {
        assert_rejected(
            decode_value_response(Commits {
                height: Some(height()),
                ..Default::default()
            }),
            "Missing commits value",
        );
    }

    #[test]
    fn rejects_synced_commits_with_round_above_u32() {
        assert_rejected(
            decode_value_response(Commits {
                height: Some(height()),
                value: Some(shard_hash()),
                round: ROUND_OVERFLOW,
                ..Default::default()
            }),
            "Round out of range",
        );
    }

    #[test]
    fn rejects_vote_set_request_with_round_above_u32() {
        let request = proto::SyncRequest {
            sync_request: Some(proto::sync_request::SyncRequest::VoteSet(
                SyncVoteSetRequest {
                    height: Some(height()),
                    round: ROUND_OVERFLOW,
                },
            )),
        };
        assert_rejected(
            Codec::<sync::Request<SnapchainValidatorContext>>::decode(
                &SnapchainCodec,
                Bytes::from(request.encode_to_vec()),
            ),
            "Round out of range",
        );
    }

    #[test]
    fn rejects_vote_set_response_with_malformed_vote() {
        assert_rejected(
            decode_sync_response(proto::sync_response::SyncResponse::VoteSet(
                SyncVoteSetResponse {
                    height: Some(height()),
                    votes: vec![proto::Vote {
                        voter: addr32(),
                        ..Default::default()
                    }],
                    signatures: vec![vec![0u8; 64]],
                    ..Default::default()
                },
            )),
            "Missing vote height",
        );
    }

    #[test]
    fn rejects_vote_set_response_with_round_above_u32() {
        assert_rejected(
            decode_sync_response(proto::sync_response::SyncResponse::VoteSet(
                SyncVoteSetResponse {
                    height: Some(height()),
                    round: ROUND_OVERFLOW,
                    votes: vec![proto::Vote {
                        height: Some(height()),
                        voter: addr32(),
                        ..Default::default()
                    }],
                    signatures: vec![vec![0u8; 64]],
                },
            )),
            "Round out of range",
        );
    }

    #[test]
    fn accepts_well_formed_full_proposal() {
        let proposal = well_formed_proposal();
        let bytes = Bytes::from(proposal.encode_to_vec());
        let decoded = Codec::<FullProposal>::decode(&SnapchainCodec, bytes.clone()).unwrap();
        assert_eq!(decoded, proposal);
        let streamed =
            Codec::<StreamMessage<FullProposal>>::decode(&SnapchainCodec, bytes).unwrap();
        assert_eq!(streamed.content.as_data(), Some(&proposal));
    }

    #[test]
    fn rejects_full_proposal_without_height() {
        assert_full_proposal_rejected(
            FullProposal {
                height: None,
                ..well_formed_proposal()
            },
            "Missing height",
        );
    }

    #[test]
    fn rejects_full_proposal_without_value() {
        assert_full_proposal_rejected(
            FullProposal {
                proposed_value: None,
                ..well_formed_proposal()
            },
            "Missing proposed value",
        );
    }

    #[test]
    fn rejects_full_proposal_with_negative_round() {
        assert_full_proposal_rejected(
            FullProposal {
                round: -1,
                ..well_formed_proposal()
            },
            "Invalid round: -1",
        );
    }

    #[test]
    fn rejects_full_proposal_with_round_above_u32() {
        assert_full_proposal_rejected(
            FullProposal {
                round: ROUND_OVERFLOW,
                ..well_formed_proposal()
            },
            "Invalid round: 4294967296",
        );
    }

    #[test]
    fn rejects_full_proposal_with_short_proposer() {
        assert_full_proposal_rejected(
            FullProposal {
                proposer: vec![0u8; 8],
                ..well_formed_proposal()
            },
            "Invalid address length: 8",
        );
    }

    #[test]
    fn rejects_full_proposal_with_block_on_shard_chunk_shard() {
        assert_full_proposal_rejected(
            FullProposal {
                proposed_value: Some(ProposedValue::Block(proto::Block::default())),
                ..well_formed_proposal()
            },
            "Proposed value type does not match shard",
        );
    }

    #[test]
    fn rejects_full_proposal_whose_header_height_differs() {
        assert_full_proposal_rejected(
            FullProposal {
                proposed_value: Some(ProposedValue::Shard(
                    shard_chunk_factory::create_shard_chunk(1, Some(2), Some(0)),
                )),
                ..well_formed_proposal()
            },
            "Header height does not match proposal height",
        );
    }

    #[test]
    fn well_formed_proposal_exposes_checked_fields() {
        let mut proposal = well_formed_proposal();
        if let Some(ProposedValue::Shard(chunk)) = proposal.proposed_value.as_mut() {
            chunk.hash = vec![7u8; 32];
        }
        proposal.round = 3;
        let well_formed = WellFormedProposal::try_from(proposal).unwrap();
        assert_eq!(well_formed.height(), height());
        assert_eq!(well_formed.round().as_i64(), 3);
        assert_eq!(
            well_formed.shard_hash(),
            &proto::ShardHash {
                shard_index: 1,
                hash: vec![7u8; 32],
            }
        );
        assert_eq!(well_formed.proposer_address().to_vec(), addr32());
    }
}
