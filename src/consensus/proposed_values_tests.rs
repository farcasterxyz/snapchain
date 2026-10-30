#[cfg(test)]
mod tests {
    use crate::{
        consensus::proposer::ProposedValues,
        core::types::WellFormedProposal,
        proto::{full_proposal::ProposedValue, FullProposal, Height, Transaction},
        utils::factory::shard_chunk_factory,
    };

    fn make_proposal(shard_index: u32, block_number: u64, round: i64) -> WellFormedProposal {
        let mut chunk =
            shard_chunk_factory::create_shard_chunk(shard_index, Some(block_number), Some(0));
        chunk.hash = make_hash(format!("{},{}", block_number, round));
        WellFormedProposal::try_from(FullProposal {
            height: Some(Height {
                shard_index,
                block_number,
            }),
            round,
            proposer: vec![0; 32],
            proposed_value: Some(ProposedValue::Shard(chunk)),
        })
        .unwrap()
    }

    fn make_hash(s: String) -> Vec<u8> {
        blake3::hash(s.as_bytes()).as_bytes().to_vec()
    }

    fn add_proposed_value(proposed_values: &mut ProposedValues, proposal: &WellFormedProposal) {
        proposed_values.add_proposed_value(proposal.clone());
        assert_eq!(
            proposed_values
                .get_by_shard_hash(proposal.shard_hash())
                .unwrap()
                .clone(),
            proposal.clone()
        );
    }

    #[test]
    fn test_basic() {
        let mut proposed_values = ProposedValues::new();
        let proposal1 = make_proposal(1, 1, 0);
        add_proposed_value(&mut proposed_values, &proposal1);
        assert_eq!(proposed_values.count(), 1);
        proposed_values.decide(Height {
            shard_index: 1,
            block_number: 1,
        });
        assert!(proposed_values
            .get_by_shard_hash(proposal1.shard_hash())
            .is_none());
        assert_eq!(proposed_values.count(), 0);
    }

    #[test]
    fn test_same_hash_keeps_first_value() {
        let mut proposed_values = ProposedValues::new();
        let first = make_proposal(1, 1, 0);
        let mut second = first.proposal().clone();
        let Some(ProposedValue::Shard(chunk)) = &mut second.proposed_value else {
            panic!("expected shard chunk");
        };
        chunk.transactions.push(Transaction::default());

        add_proposed_value(&mut proposed_values, &first);
        proposed_values.add_proposed_value(WellFormedProposal::try_from(second).unwrap());

        assert_eq!(
            proposed_values
                .get_by_shard_hash(first.shard_hash())
                .unwrap()
                .clone(),
            first
        );
        assert_eq!(proposed_values.count(), 1);
    }

    #[test]
    fn test_clean_up_old_proposed_values() {
        let mut proposed_values = ProposedValues::new();
        let proposal1 = make_proposal(1, 1, 0);
        let proposal2 = make_proposal(1, 2, 0);
        let proposal3 = make_proposal(1, 2, 1);
        let proposal4 = make_proposal(1, 3, 0);
        add_proposed_value(&mut proposed_values, &proposal1);
        add_proposed_value(&mut proposed_values, &proposal2);
        add_proposed_value(&mut proposed_values, &proposal3);
        add_proposed_value(&mut proposed_values, &proposal4);
        assert_eq!(proposed_values.count(), 4);
        proposed_values.decide(Height {
            shard_index: 1,
            block_number: 2,
        });

        assert!(proposed_values
            .get_by_shard_hash(proposal1.shard_hash())
            .is_none());
        assert!(proposed_values
            .get_by_shard_hash(proposal2.shard_hash())
            .is_none());
        assert!(proposed_values
            .get_by_shard_hash(proposal3.shard_hash())
            .is_none());
        assert_eq!(
            proposed_values
                .get_by_shard_hash(proposal4.shard_hash())
                .unwrap()
                .clone(),
            proposal4
        );
        assert_eq!(proposed_values.count(), 1);
    }
}
