#[cfg(test)]
mod tests {

    use std::collections::BTreeMap;

    use crate::consensus::consensus::{SystemMessage, ValidatorSetConfig};
    use crate::consensus::read_validator::{Engine, ReadValidator};
    use crate::consensus::validator::{StoredValidatorSet, StoredValidatorSets};
    use crate::core::types::{Address, ShardId, Vote};
    use crate::core::util::{block_hash, expected_transactions_root, shard_witness_hash};
    use crate::proto::{
        self, BlockHeader, CommitSignature, Commits, FarcasterNetwork, Height, ShardChunk,
        ShardChunkWitness, ShardHash, ShardWitness,
    };
    use crate::storage::store::block_engine::BlockStateChange;
    use crate::storage::store::block_engine_test_helpers;
    use crate::storage::store::engine::ShardEngine;
    use crate::storage::store::test_helper::{
        self, commit_event, default_storage_event, new_engine_with_options, sign_chunk,
        EngineOptions, FID_FOR_TEST,
    };
    use crate::version::version::EngineVersion;
    use informalsystems_malachitebft_core_types::{NilOrVal, Round};
    use libp2p::identity::ed25519::Keypair;
    use tokio::sync::mpsc;

    async fn setup(
        num_already_decided_blocks: u64,
    ) -> (
        ShardEngine,
        ShardEngine,
        ReadValidator,
        Keypair,
        mpsc::Receiver<SystemMessage>,
    ) {
        let proposer_keypair = Keypair::generate();
        let (mut proposer_engine, _) = test_helper::new_engine().await;
        let (mut read_node_engine, _) = test_helper::new_engine().await;
        for _ in 0..num_already_decided_blocks {
            let shard_chunk = commit_shard_chunk(&mut proposer_engine, &proposer_keypair).await;
            read_node_engine.commit_shard_chunk(&shard_chunk).await;
        }

        let (read_node_engine_clone, _) = new_engine_with_options(EngineOptions {
            db: Some(read_node_engine.db.clone()),
            ..Default::default()
        })
        .await;

        let proposer_address = Address(proposer_keypair.public().to_bytes());

        let validator_set_config = ValidatorSetConfig {
            effective_at: 0,
            validator_public_keys: vec![proposer_address.to_hex()],
            shard_ids: vec![read_node_engine.shard_id()],
        };

        let validator_sets = vec![StoredValidatorSet::new(
            ShardId::new(read_node_engine.shard_id()),
            &validator_set_config,
        )];

        let (system_tx, system_rx) = mpsc::channel(100);

        let read_validator = ReadValidator {
            shard_id: read_node_engine.shard_id(),
            last_height: Height {
                shard_index: read_node_engine.shard_id(),
                block_number: 0,
            },
            engine: Engine::ShardEngine(read_node_engine_clone),
            max_num_buffered_blocks: 1,
            buffered_blocks: BTreeMap::new(),
            statsd_client: test_helper::statsd_client(),
            validator_sets: StoredValidatorSets::new(read_node_engine.shard_id(), validator_sets),
            system_tx,
        };

        (
            proposer_engine,
            read_node_engine,
            read_validator,
            proposer_keypair,
            system_rx,
        )
    }

    async fn commit_shard_chunk(
        engine: &mut ShardEngine,
        proposer_keypair: &Keypair,
    ) -> ShardChunk {
        let shard_chunk = commit_event(engine, &default_storage_event(FID_FOR_TEST)).await;
        sign_chunk(proposer_keypair, shard_chunk).await
    }

    async fn process_decided_value(
        read_validator: &mut ReadValidator,
        shard_chunk: &ShardChunk,
    ) -> u64 {
        let decided_value = proto::DecidedValue {
            value: Some(proto::decided_value::Value::Shard(shard_chunk.clone())),
        };
        read_validator.process_decided_value(decided_value).await
    }

    #[tokio::test]
    async fn test_get_decided_value() {
        let (mut proposer_engine, read_node_engine, mut read_validator, proposer_keypair, _) =
            setup(0).await;
        let shard_chunk = commit_shard_chunk(&mut proposer_engine, &proposer_keypair).await;
        let num_processed = process_decided_value(&mut read_validator, &shard_chunk).await;
        assert_eq!(num_processed, 1);
        assert_eq!(
            read_validator.last_height,
            shard_chunk.header.as_ref().unwrap().height.unwrap()
        );
        assert_eq!(
            read_node_engine.get_confirmed_height(),
            shard_chunk.header.as_ref().unwrap().height.unwrap()
        );
        let decided_value = read_validator
            .get_decided_value(shard_chunk.header.as_ref().unwrap().height.unwrap())
            .unwrap();
        assert_eq!(shard_chunk.hash, decided_value.certificate.value_id.hash);
    }

    #[tokio::test]
    async fn test_validate_protocol_version() {
        let (_, _, read_validator, _, mut system_rx) = setup(0).await;
        let valid_value = proto::DecidedValue {
            value: Some(proto::decided_value::Value::Block(proto::Block {
                header: Some(proto::BlockHeader {
                    height: Some(Height {
                        shard_index: read_validator.shard_id,
                        block_number: 1,
                    }),
                    version: 1,
                    timestamp: 1,
                    chain_id: 1, // Mainnet
                    ..Default::default()
                }),
                ..Default::default()
            })),
        };
        assert_eq!(read_validator.validate_protocol_version(&valid_value), true);
        assert_eq!(system_rx.try_recv().is_err(), true); // No system message should be sent

        let invalid_value = proto::DecidedValue {
            value: Some(proto::decided_value::Value::Block(proto::Block {
                header: Some(proto::BlockHeader {
                    height: Some(Height {
                        shard_index: read_validator.shard_id,
                        block_number: 1,
                    }),
                    version: 2, // Invalid version
                    timestamp: 1,
                    chain_id: 1, // Mainnet
                    ..Default::default()
                }),
                ..Default::default()
            })),
        };
        assert_eq!(
            read_validator.validate_protocol_version(&invalid_value),
            false
        );
        let system_message = system_rx.try_recv();
        assert_eq!(system_message.is_ok(), true);
        match system_message {
            Ok(SystemMessage::ExitWithError { .. }) => (),
            _ => panic!("Expected ExitWithError system message"),
        }
    }

    #[tokio::test]
    async fn test_buffered_values() {
        let (mut proposer_engine, read_node_engine, mut read_validator, proposer_keypair, _) =
            setup(0).await;
        let shard_chunk1 = commit_shard_chunk(&mut proposer_engine, &proposer_keypair).await;
        let shard_chunk2 = commit_shard_chunk(&mut proposer_engine, &proposer_keypair).await;
        let shard_chunk3 = commit_shard_chunk(&mut proposer_engine, &proposer_keypair).await;
        // Drop the new block if the buffer map is full
        let num_processed = process_decided_value(&mut read_validator, &shard_chunk2).await;
        assert_eq!(num_processed, 0);
        let num_processed = process_decided_value(&mut read_validator, &shard_chunk3).await;
        assert_eq!(num_processed, 0);
        assert_eq!(read_validator.buffered_blocks.len(), 1);
        assert!(read_validator
            .buffered_blocks
            .contains_key(&shard_chunk2.header.as_ref().unwrap().height.unwrap()));
        assert!(!read_validator
            .buffered_blocks
            .contains_key(&shard_chunk3.header.unwrap().height.unwrap()));
        assert_eq!(
            read_node_engine.get_confirmed_height(),
            Height {
                shard_index: read_node_engine.shard_id(),
                block_number: 0
            }
        );

        // Buffer should clear once the unblocking value shows up
        let num_processed = process_decided_value(&mut read_validator, &shard_chunk1).await;
        assert_eq!(num_processed, 2);
        assert_eq!(
            read_validator.last_height,
            shard_chunk2.header.as_ref().unwrap().height.unwrap()
        );
        assert_eq!(
            read_node_engine.get_confirmed_height(),
            shard_chunk2.header.as_ref().unwrap().height.unwrap()
        );
        assert_eq!(read_validator.buffered_blocks.len(), 0);
    }

    #[tokio::test]
    async fn test_initialize() {
        let (_proposer_engine, read_node_engine, mut read_validator, _proposer_keypair, _) =
            setup(3).await;

        read_validator.initialize_height();
        assert_eq!(
            read_validator.last_height,
            Height {
                shard_index: read_node_engine.shard_id(),
                block_number: 3
            }
        );
    }

    #[tokio::test]
    async fn test_ignore_no_signatures() {
        let (mut proposer_engine, _read_node_engine, mut read_validator, proposer_keypair, _) =
            setup(0).await;
        let mut shard_chunk = commit_shard_chunk(&mut proposer_engine, &proposer_keypair).await;
        // Remove signatures from chunk
        shard_chunk.commits = Some(Commits {
            height: shard_chunk.header.as_ref().unwrap().height,
            round: 0,
            value: Some(ShardHash {
                shard_index: shard_chunk
                    .header
                    .as_ref()
                    .unwrap()
                    .height
                    .unwrap()
                    .shard_index,
                hash: shard_chunk.hash.clone(),
            }),
            signatures: vec![],
        });
        let num_processed = process_decided_value(&mut read_validator, &shard_chunk).await;
        assert_eq!(num_processed, 0);
    }

    #[tokio::test]
    async fn test_ignore_invalid_signatures() {
        let (mut proposer_engine, _read_node_engine, mut read_validator, proposer_keypair, _) =
            setup(0).await;
        let invalid_keypair = Keypair::generate();
        let mut shard_chunk = commit_shard_chunk(&mut proposer_engine, &invalid_keypair).await;

        // We sign with invalid_keypair but then replace the signer with the expected signer
        // This causes the signature to be invalid
        let signature = shard_chunk.commits.unwrap().signatures[0].signature.clone();
        let signer = proposer_keypair.public().to_bytes().to_vec();
        shard_chunk.commits = Some(Commits {
            height: shard_chunk.header.as_ref().unwrap().height,
            round: 0,
            value: Some(ShardHash {
                shard_index: shard_chunk
                    .header
                    .as_ref()
                    .unwrap()
                    .height
                    .unwrap()
                    .shard_index,
                hash: shard_chunk.hash.clone(),
            }),
            signatures: vec![CommitSignature { signature, signer }],
        });
        let num_processed = process_decided_value(&mut read_validator, &shard_chunk).await;
        assert_eq!(num_processed, 0);
    }

    async fn assert_dropped(
        read_validator: &mut ReadValidator,
        decided_value: proto::DecidedValue,
    ) {
        let last_height = read_validator.last_height;
        assert_eq!(read_validator.process_decided_value(decided_value).await, 0);
        assert_eq!(read_validator.last_height, last_height);
        assert!(read_validator.buffered_blocks.is_empty());
    }

    fn shard_value(shard_chunk: ShardChunk) -> proto::DecidedValue {
        proto::DecidedValue {
            value: Some(proto::decided_value::Value::Shard(shard_chunk)),
        }
    }

    #[tokio::test]
    async fn test_ignore_missing_value() {
        let (_, _, mut read_validator, _, _) = setup(0).await;
        assert_dropped(&mut read_validator, proto::DecidedValue { value: None }).await;
    }

    #[tokio::test]
    async fn test_ignore_missing_header_or_height() {
        let (mut proposer_engine, _, mut read_validator, proposer_keypair, _) = setup(0).await;
        let shard_chunk = commit_shard_chunk(&mut proposer_engine, &proposer_keypair).await;

        let mut no_header = shard_chunk.clone();
        no_header.header = None;
        assert_dropped(&mut read_validator, shard_value(no_header)).await;

        let mut no_height = shard_chunk.clone();
        no_height.header.as_mut().unwrap().height = None;
        assert_dropped(&mut read_validator, shard_value(no_height)).await;

        let no_block_header = proto::DecidedValue {
            value: Some(proto::decided_value::Value::Block(proto::Block::default())),
        };
        assert_dropped(&mut read_validator, no_block_header).await;
    }

    #[tokio::test]
    async fn test_ignore_malformed_commits() {
        let (mut proposer_engine, _, mut read_validator, proposer_keypair, _) = setup(0).await;
        let shard_chunk = commit_shard_chunk(&mut proposer_engine, &proposer_keypair).await;

        let mut no_commits = shard_chunk.clone();
        no_commits.commits = None;
        assert_dropped(&mut read_validator, shard_value(no_commits)).await;

        let mut no_commit_height = shard_chunk.clone();
        no_commit_height.commits.as_mut().unwrap().height = None;
        assert_dropped(&mut read_validator, shard_value(no_commit_height)).await;

        let mut no_commit_value = shard_chunk.clone();
        no_commit_value.commits.as_mut().unwrap().value = None;
        assert_dropped(&mut read_validator, shard_value(no_commit_value)).await;

        let mut short_signer = shard_chunk.clone();
        short_signer.commits.as_mut().unwrap().signatures[0].signer = vec![1, 2, 3];
        assert_dropped(&mut read_validator, shard_value(short_signer)).await;
    }

    #[tokio::test]
    async fn test_ignore_value_for_wrong_engine() {
        let (mut proposer_engine, _, mut read_validator, proposer_keypair, mut system_rx) =
            setup(0).await;
        let shard_chunk = commit_shard_chunk(&mut proposer_engine, &proposer_keypair).await;

        // Validly signed commits wrapped in a Block, sent to a shard engine
        let block = proto::Block {
            header: Some(proto::BlockHeader {
                height: shard_chunk.header.as_ref().unwrap().height,
                ..Default::default()
            }),
            commits: shard_chunk.commits.clone(),
            ..Default::default()
        };
        let decided_value = proto::DecidedValue {
            value: Some(proto::decided_value::Value::Block(block)),
        };
        assert_dropped(&mut read_validator, decided_value).await;
        assert!(system_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_ignore_value_for_other_shard() {
        let (mut proposer_engine, _, mut read_validator, proposer_keypair, _) = setup(0).await;
        let mut shard_chunk = commit_shard_chunk(&mut proposer_engine, &proposer_keypair).await;
        shard_chunk
            .header
            .as_mut()
            .unwrap()
            .height
            .as_mut()
            .unwrap()
            .shard_index += 1;
        assert_dropped(&mut read_validator, shard_value(shard_chunk)).await;
    }

    #[test]
    fn test_decided_value_height() {
        assert_eq!(
            ReadValidator::decided_value_height(&proto::DecidedValue { value: None }),
            None
        );
        let height = Height {
            shard_index: 3,
            block_number: 7,
        };
        let shard_chunk = ShardChunk {
            header: Some(proto::ShardHeader {
                height: Some(height),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            ReadValidator::decided_value_height(&shard_value(shard_chunk)),
            Some(height)
        );
        assert_eq!(
            ReadValidator::decided_value_height(&shard_value(ShardChunk::default())),
            None
        );
    }

    fn signed_decided_block(
        proposer_keypair: &Keypair,
        state_change: &BlockStateChange,
        witnesses: Vec<ShardChunkWitness>,
    ) -> proto::DecidedValue {
        let height = Height::new(0, 1);
        let version = EngineVersion::version_for(&state_change.timestamp, FarcasterNetwork::Devnet);
        let shard_witness = ShardWitness {
            shard_chunk_witnesses: witnesses,
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
        let hash = block_hash(&header);
        let value = ShardHash {
            shard_index: 0,
            hash: hash.clone(),
        };
        let signer = proposer_keypair.public().to_bytes().to_vec();
        let vote = Vote::new_precommit(
            height,
            Round::from(0u32),
            NilOrVal::Val(value.clone()),
            Address::from_vec(signer.clone()),
        );
        let signature = proposer_keypair.sign(&vote.to_sign_bytes());
        proto::DecidedValue {
            value: Some(proto::decided_value::Value::Block(proto::Block {
                header: Some(header),
                hash,
                shard_witness: Some(shard_witness),
                commits: Some(Commits {
                    height: Some(height),
                    round: 0,
                    value: Some(value),
                    signatures: vec![CommitSignature { signature, signer }],
                }),
                transactions: state_change.transactions.clone(),
                events: state_change.events.clone(),
            })),
        }
    }

    #[tokio::test]
    async fn test_ignore_block_with_unset_witness_height() {
        let proposer_keypair = Keypair::generate();
        let proposer_address = Address(proposer_keypair.public().to_bytes());
        let (mut proposer_engine, _proposer_dir) = block_engine_test_helpers::setup();
        let (read_engine, _read_dir) = block_engine_test_helpers::setup();
        let validator_sets = vec![StoredValidatorSet::new(
            ShardId::new(0),
            &ValidatorSetConfig {
                effective_at: 0,
                validator_public_keys: vec![proposer_address.to_hex()],
                shard_ids: vec![0],
            },
        )];
        let (system_tx, _system_rx) = mpsc::channel(100);
        let mut read_validator = ReadValidator {
            shard_id: 0,
            last_height: Height::new(0, 0),
            engine: Engine::BlockEngine(read_engine),
            max_num_buffered_blocks: 1,
            buffered_blocks: BTreeMap::new(),
            statsd_client: test_helper::statsd_client(),
            validator_sets: StoredValidatorSets::new(0, validator_sets),
            system_tx,
        };
        let state_change = proposer_engine.propose_state_change(vec![], Height::new(0, 1), None);

        let unset_height = signed_decided_block(
            &proposer_keypair,
            &state_change,
            vec![ShardChunkWitness {
                height: None,
                shard_hash: vec![1; 32],
                shard_root: vec![2; 32],
            }],
        );
        assert_dropped(&mut read_validator, unset_height).await;

        let valid = signed_decided_block(
            &proposer_keypair,
            &state_change,
            vec![ShardChunkWitness {
                height: Some(Height::new(1, 1)),
                shard_hash: vec![1; 32],
                shard_root: vec![2; 32],
            }],
        );
        assert_eq!(read_validator.process_decided_value(valid).await, 1);
        assert_eq!(read_validator.last_height, Height::new(0, 1));
    }
}
