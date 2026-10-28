#[cfg(test)]
mod tests {
    use crate::consensus::proposer::{Proposer, ShardProposer};
    use crate::core::types::{Address, Height, ShardId, SnapchainShard, WellFormedProposal};
    use crate::core::util::{expected_transactions_root, shard_chunk_hash};
    use crate::proto::{
        full_proposal, Commits, FullProposal, ShardChunk, ShardHeader, Transaction,
    };
    use crate::storage::store::test_helper;
    use informalsystems_malachitebft_core_types::Validity;
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
}
