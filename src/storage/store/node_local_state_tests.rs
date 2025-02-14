#[cfg(test)]
mod tests {
    use crate::proto::{FullProposal, Height};
    use crate::storage::db;
    use crate::storage::store::node_local_state::LocalStateStore;
    use std::sync::Arc;

    fn store() -> LocalStateStore {
        let dir = tempfile::TempDir::new().unwrap();
        let db_path = dir.path().join("a.db");

        let db = db::RocksDB::new(db_path.to_str().unwrap());
        db.open().unwrap();

        LocalStateStore::new(Arc::new(db))
    }

    fn make_proposal(shard_index: u32, block_number: u64, round: i64) -> FullProposal {
        FullProposal {
            height: Some(Height {
                shard_index,
                block_number,
            }),
            round,
            proposer: vec![],
            proposed_value: None,
        }
    }

    fn assert_proposal_exists(store: &LocalStateStore, proposal: &FullProposal) {
        let proposal = store
            .get_proposal(
                proposal.shard_id().unwrap(),
                proposal.height(),
                proposal.round(),
            )
            .unwrap()
            .unwrap();

        assert_eq!(proposal, proposal);
    }

    fn assert_proposal_not_exists(store: &LocalStateStore, proposal: &FullProposal) {
        let proposal = store
            .get_proposal(
                proposal.shard_id().unwrap(),
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

        store.put_proposal(proposal1.clone()).unwrap();
        store.put_proposal(proposal2.clone()).unwrap();
        store.put_proposal(proposal3.clone()).unwrap();
        store.put_proposal(proposal4.clone()).unwrap();

        assert_proposal_exists(&store, &proposal1);
        assert_proposal_exists(&store, &proposal2);
        assert_proposal_exists(&store, &proposal3);
        assert_proposal_exists(&store, &proposal4);

        store
            .delete_proposals(proposal1.shard_id().unwrap(), proposal1.height())
            .unwrap();

        assert_proposal_not_exists(&store, &proposal1);
        assert_proposal_not_exists(&store, &proposal2);
        assert_proposal_exists(&store, &proposal3);
        assert_proposal_exists(&store, &proposal4);
    }
}
