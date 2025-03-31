#[cfg(test)]
mod tests {
    use crate::{
        consensus::proposer::ProposedValues,
        proto::{full_proposal::ProposedValue, Block, FullProposal, Height},
    };

    fn make_proposal(shard_index: u32, block_number: u64, round: i64) -> FullProposal {
        FullProposal {
            height: Some(Height {
                shard_index,
                block_number,
            }),
            round,
            proposer: vec![],
            proposed_value: Some(ProposedValue::Block(Block {
                hash: make_hash(format!("{},{}", block_number, round)),
                header: None,
                shard_witness: None,
                commits: None,
            })),
        }
    }

    fn make_hash(s: String) -> Vec<u8> {
        blake3::hash(s.as_bytes()).as_bytes().to_vec()
    }

    fn add_proposed_value(proposed_values: &mut ProposedValues, proposal: &FullProposal) {
        proposed_values.add_proposed_value(proposal.clone());
        assert_eq!(
            proposed_values
                .get_by_shard_hash(&proposal.shard_hash())
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
            .get_by_shard_hash(&proposal1.shard_hash())
            .is_none());
        assert_eq!(proposed_values.count(), 0);
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
            .get_by_shard_hash(&proposal1.shard_hash())
            .is_none());
        assert!(proposed_values
            .get_by_shard_hash(&proposal2.shard_hash())
            .is_none());
        assert!(proposed_values
            .get_by_shard_hash(&proposal3.shard_hash())
            .is_none());
        assert_eq!(
            proposed_values
                .get_by_shard_hash(&proposal4.shard_hash())
                .unwrap()
                .clone(),
            proposal4
        );
        assert_eq!(proposed_values.count(), 1);
    }
}
