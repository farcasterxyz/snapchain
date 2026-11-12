#[cfg(test)]
mod tests {
    use crate::consensus::proposer::{BlockProposer, Proposer, ShardProposer};
    use crate::core::types::{Address, Height, ShardId, SnapchainShard, WellFormedProposal};
    use crate::core::util::{
        block_hash, expected_transactions_root, shard_chunk_hash, shard_witness_hash,
    };
    use crate::proto::{
        full_proposal, Block, BlockHeader, Commits, FarcasterNetwork, FullProposal, ShardChunk,
        ShardChunkWitness, ShardHeader, ShardWitness, Transaction,
    };
    use crate::storage::store::{block_engine_test_helpers, test_helper};
    use crate::version::version::EngineVersion;
    use informalsystems_malachitebft_core_types::Validity;
    use std::collections::HashMap;
    use tokio::sync::broadcast;

    /// Returns a shard proposer and a valid proposal for its next height.
    async fn new_shard_proposer() -> (ShardProposer, WellFormedProposal, tempfile::TempDir) {
        let (mut engine, dir) = test_helper::new_engine().await;
        let state_change = engine.propose_state_change(1, vec![], None);
        let header = ShardHeader {
            parent_hash: vec![],
            timestamp: state_change.timestamp.into(),
            height: Some(Height::new(1, 1)),
            shard_root: state_change.new_state_root.clone(),
            transactions_root: expected_transactions_root(
                &state_change.transactions,
                state_change.version,
            ),
        };
        let proposal = FullProposal {
            height: header.height,
            round: 0,
            proposer: Address([0u8; 32]).to_vec(),
            proposed_value: Some(full_proposal::ProposedValue::Shard(ShardChunk {
                hash: shard_chunk_hash(&header),
                header: Some(header),
                transactions: state_change.transactions,
                commits: None,
            })),
        };
        let proposal = WellFormedProposal::try_from(proposal).unwrap();

        let (tx_decision, _) = broadcast::channel(10);
        let proposer = ShardProposer::new(
            Address([0u8; 32]),
            SnapchainShard::new(1),
            engine,
            test_helper::statsd_client(),
            tx_decision,
        );
        (proposer, proposal, dir)
    }

    #[tokio::test]
    async fn test_second_body_for_same_hash_does_not_replace_first() {
        let (mut proposer, first, _dir) = new_shard_proposer().await;
        let mut second = first.proposal().clone();
        let Some(full_proposal::ProposedValue::Shard(chunk)) = &mut second.proposed_value else {
            panic!("expected shard chunk");
        };
        chunk.transactions.push(Transaction {
            fid: 1234,
            ..Default::default()
        });
        let second = WellFormedProposal::try_from(second).unwrap();

        assert_eq!(proposer.add_proposed_value(&first), Validity::Valid);
        assert_eq!(proposer.add_proposed_value(&second), Validity::Invalid);
        assert_eq!(
            proposer.get_proposed_value(first.shard_hash()),
            Some(first.clone())
        );

        // The same body again is still accepted.
        assert_eq!(proposer.add_proposed_value(&first), Validity::Valid);
    }

    #[tokio::test]
    async fn test_synced_copy_with_commits_matches_cached_body() {
        let (mut proposer, consensus_copy, _dir) = new_shard_proposer().await;
        let mut synced_copy = consensus_copy.proposal().clone();
        let Some(full_proposal::ProposedValue::Shard(chunk)) = &mut synced_copy.proposed_value
        else {
            panic!("expected shard chunk");
        };
        chunk.commits = Some(Commits::default());
        let synced_copy = WellFormedProposal::try_from(synced_copy).unwrap();

        assert_eq!(
            proposer.add_proposed_value(&consensus_copy),
            Validity::Valid
        );
        assert_eq!(proposer.add_proposed_value(&synced_copy), Validity::Valid);
    }

    #[tokio::test]
    async fn test_chunk_whose_transactions_do_not_match_header_is_not_cached() {
        let (mut proposer, proposal, _dir) = new_shard_proposer().await;
        let mut mismatched = proposal.proposal().clone();
        let Some(full_proposal::ProposedValue::Shard(chunk)) = &mut mismatched.proposed_value
        else {
            panic!("expected shard chunk");
        };
        chunk.transactions.push(Transaction {
            fid: 1234,
            ..Default::default()
        });
        let mismatched = WellFormedProposal::try_from(mismatched).unwrap();

        assert_eq!(proposer.add_proposed_value(&mismatched), Validity::Invalid);
        assert_eq!(proposer.get_proposed_value(proposal.shard_hash()), None);
        assert_eq!(proposer.add_proposed_value(&proposal), Validity::Valid);
    }

    fn new_block_proposer() -> (BlockProposer, WellFormedProposal, tempfile::TempDir) {
        let (mut engine, dir) = block_engine_test_helpers::setup();
        let height = Height::new(0, 1);
        let state_change = engine.propose_state_change(vec![], height, None);
        let version = EngineVersion::version_for(&state_change.timestamp, FarcasterNetwork::Devnet);
        let shard_witness = ShardWitness {
            shard_chunk_witnesses: vec![
                ShardChunkWitness {
                    height: Some(Height::new(1, 1)),
                    shard_hash: vec![1; 32],
                    shard_root: vec![2; 32],
                },
                ShardChunkWitness {
                    height: Some(Height::new(2, 1)),
                    shard_hash: vec![3; 32],
                    shard_root: vec![4; 32],
                },
            ],
        };
        let header = BlockHeader {
            parent_hash: vec![0; 32],
            chain_id: FarcasterNetwork::Devnet as i32,
            version: version.protocol_version(),
            timestamp: state_change.timestamp.to_u64(),
            height: Some(height),
            shard_witnesses_hash: shard_witness_hash(&shard_witness),
            state_root: state_change.new_state_root.clone(),
            events_hash: state_change.events_hash.clone(),
            transactions_root: expected_transactions_root(&state_change.transactions, version),
        };
        let block = Block {
            hash: block_hash(&header),
            header: Some(header),
            shard_witness: Some(shard_witness),
            commits: None,
            transactions: state_change.transactions,
            events: state_change.events,
        };
        let proposal = WellFormedProposal::try_from(FullProposal {
            height: Some(height),
            round: 0,
            proposer: Address([0u8; 32]).to_vec(),
            proposed_value: Some(full_proposal::ProposedValue::Block(block)),
        })
        .unwrap();
        let proposer = BlockProposer::new(
            Address([0u8; 32]),
            SnapchainShard::new(0),
            HashMap::new(),
            2,
            FarcasterNetwork::Devnet,
            None,
            engine,
            test_helper::statsd_client(),
        );
        (proposer, proposal, dir)
    }

    fn with_witness_heights(
        base: &WellFormedProposal,
        heights: [Option<Height>; 2],
    ) -> WellFormedProposal {
        let mut proposal = base.proposal().clone();
        let Some(full_proposal::ProposedValue::Block(block)) = &mut proposal.proposed_value else {
            panic!("expected block");
        };
        let shard_witness = block.shard_witness.as_mut().unwrap();
        for (witness, height) in shard_witness.shard_chunk_witnesses.iter_mut().zip(heights) {
            witness.height = height;
        }
        let header = block.header.as_mut().unwrap();
        header.shard_witnesses_hash = shard_witness_hash(shard_witness);
        block.hash = block_hash(header);
        WellFormedProposal::try_from(proposal).unwrap()
    }

    #[tokio::test]
    async fn test_block_with_unset_witness_height_is_rejected() {
        let (mut proposer, valid, _dir) = new_block_proposer();

        let all_unset = with_witness_heights(&valid, [None, None]);
        assert_eq!(proposer.add_proposed_value(&all_unset), Validity::Invalid);
        assert_eq!(proposer.get_proposed_value(all_unset.shard_hash()), None);

        let one_unset = with_witness_heights(&valid, [Some(Height::new(1, 1)), None]);
        assert_eq!(proposer.add_proposed_value(&one_unset), Validity::Invalid);
        assert_eq!(proposer.get_proposed_value(one_unset.shard_hash()), None);

        assert_eq!(proposer.add_proposed_value(&valid), Validity::Valid);
        assert_eq!(
            proposer.get_proposed_value(valid.shard_hash()),
            Some(valid.clone())
        );
        proposer
            .decide(Commits {
                height: Some(valid.height()),
                round: 0,
                value: Some(valid.shard_hash().clone()),
                signatures: vec![],
            })
            .await;
        assert_eq!(proposer.get_confirmed_height(), Height::new(0, 1));
    }

    #[test]
    fn test_last_shard_witness_skips_unset_height() {
        let (mut engine, _dir) = block_engine_test_helpers::setup();
        let state_change = engine.propose_state_change(vec![], Height::new(0, 1), None);
        let mut block = block_engine_test_helpers::state_change_to_block(1, &state_change);
        block.shard_witness = Some(ShardWitness {
            shard_chunk_witnesses: vec![
                ShardChunkWitness {
                    height: None,
                    shard_hash: vec![1; 32],
                    shard_root: vec![2; 32],
                },
                ShardChunkWitness {
                    height: Some(Height::new(2, 5)),
                    shard_hash: vec![3; 32],
                    shard_root: vec![4; 32],
                },
            ],
        });
        engine.commit_block(&block);

        assert_eq!(engine.get_last_shard_witness(Height::new(0, 2), 1), None);
        assert_eq!(
            engine
                .get_last_shard_witness(Height::new(0, 2), 2)
                .and_then(|witness| witness.height),
            Some(Height::new(2, 5))
        );
    }
}
