#[cfg(test)]
mod tests {
    use crate::consensus::consensus::ValidatorSetConfig;
    use crate::consensus::validator::ShardValidator;
    use crate::core::types::{Address, Height, ShardId, SnapchainShard, WellFormedProposal};
    use crate::proto::{full_proposal, FullProposal};
    use crate::storage::db::RocksDB;
    use crate::storage::store::node_local_state::LocalStateStore;
    use crate::storage::store::test_helper;
    use crate::utils::factory::shard_chunk_factory;
    use libp2p::identity::ed25519::Keypair;
    use std::sync::Arc;

    fn new_validator(keys: &[&Keypair], dir: &tempfile::TempDir) -> ShardValidator {
        let db = RocksDB::new(dir.path().join("local.db").to_str().unwrap());
        db.open().unwrap();
        ShardValidator::new(
            Address([0u8; 32]),
            SnapchainShard::new(1),
            vec![ValidatorSetConfig {
                effective_at: 0,
                validator_public_keys: keys
                    .iter()
                    .map(|key| hex::encode(key.public().to_bytes()))
                    .collect(),
                shard_ids: vec![1],
            }],
            None,
            None,
            LocalStateStore::new(Arc::new(db)),
            test_helper::statsd_client(),
        )
    }

    fn proposal(proposer: &Keypair, round: i64) -> WellFormedProposal {
        WellFormedProposal::try_from(FullProposal {
            height: Some(Height::new(1, 5)),
            round,
            proposer: proposer.public().to_bytes().to_vec(),
            proposed_value: Some(full_proposal::ProposedValue::Shard(
                shard_chunk_factory::create_shard_chunk(1, Some(5), Some(0)),
            )),
        })
        .unwrap()
    }

    #[test]
    fn test_only_the_selected_proposer_is_expected() {
        let dir = tempfile::TempDir::new().unwrap();
        let a = Keypair::generate();
        let b = Keypair::generate();
        let outsider = Keypair::generate();
        let validator = new_validator(&[&a, &b], &dir);

        let expected_for_round = |round: i64| -> Vec<Vec<u8>> {
            [&a, &b]
                .into_iter()
                .filter(|key| validator.is_expected_proposer(&proposal(key, round)))
                .map(|key| key.public().to_bytes().to_vec())
                .collect()
        };
        let round_0 = expected_for_round(0);
        let round_1 = expected_for_round(1);
        assert_eq!(round_0.len(), 1);
        assert_eq!(round_1.len(), 1);
        assert_ne!(round_0, round_1, "proposer should rotate with the round");

        assert!(!validator.is_expected_proposer(&proposal(&outsider, 0)));
        assert!(!validator.is_expected_proposer(&proposal(&outsider, 1)));
    }
}
