#[cfg(test)]
mod tests {
    use crate::core::types::WellFormedProposal;
    use crate::proto::{full_proposal::ProposedValue, FullProposal, Height};
    use crate::storage::db;
    use crate::storage::store::node_local_state::LocalStateStore;
    use crate::utils::factory::shard_chunk_factory;
    use std::sync::Arc;

    fn store() -> LocalStateStore {
        let dir = tempfile::TempDir::new().unwrap();
        let db_path = dir.path().join("a.db");

        let db = db::RocksDB::new(db_path.to_str().unwrap());
        db.open().unwrap();

        LocalStateStore::new(Arc::new(db))
    }

    fn make_proposal(shard_index: u32, block_number: u64, round: i64) -> WellFormedProposal {
        let chunk =
            shard_chunk_factory::create_shard_chunk(shard_index, Some(block_number), Some(0));
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

    fn assert_proposal_exists(store: &LocalStateStore, proposal: &WellFormedProposal) {
        let stored = store
            .get_proposal(
                proposal.height().shard_index,
                proposal.height(),
                proposal.round(),
            )
            .unwrap()
            .unwrap();

        assert_eq!(&stored, proposal.proposal());
    }

    fn assert_proposal_not_exists(store: &LocalStateStore, proposal: &WellFormedProposal) {
        let proposal = store
            .get_proposal(
                proposal.height().shard_index,
                proposal.height(),
                proposal.round(),
            )
            .unwrap();

        assert!(proposal.is_none());
    }

    #[test]
    fn test_proposals() {
        let store = store();
        let proposal1 = make_proposal(1, 2, 0);
        let proposal2 = make_proposal(1, 2, 1);
        let proposal3 = make_proposal(1, 3, 1);
        let proposal4 = make_proposal(2, 3, 1);

        store.put_proposal(&proposal1).unwrap();
        store.put_proposal(&proposal2).unwrap();
        store.put_proposal(&proposal3).unwrap();
        store.put_proposal(&proposal4).unwrap();

        assert_proposal_exists(&store, &proposal1);
        assert_proposal_exists(&store, &proposal2);
        assert_proposal_exists(&store, &proposal3);
        assert_proposal_exists(&store, &proposal4);

        store
            .delete_proposals(proposal1.height().shard_index, proposal1.height())
            .unwrap();

        assert_proposal_not_exists(&store, &proposal1);
        assert_proposal_not_exists(&store, &proposal2);
        assert_proposal_exists(&store, &proposal3);
        assert_proposal_exists(&store, &proposal4);
    }
}
