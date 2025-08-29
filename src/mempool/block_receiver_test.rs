#[cfg(test)]
mod tests {
    use crate::consensus::consensus::SystemMessage;
    use crate::consensus::validator::{StoredValidatorSet, StoredValidatorSets};
    use crate::core::types::{
        Address, ShardId, SnapchainShard, SnapchainValidator, SnapchainValidatorSet, Vote,
    };
    use crate::mempool::block_receiver::BlockReceiver;
    use crate::mempool::mempool::{MempoolRequest, MempoolSource};
    use crate::proto::{Block, CommitSignature, Commits, ShardHash};
    use crate::storage::db::RocksDB;
    use crate::storage::store::block_engine::BlockEngine;
    use crate::storage::store::block_engine_test_helpers;
    use crate::storage::store::engine::ShardEngine;
    use crate::storage::store::mempool_poller::MempoolMessage;
    use crate::storage::store::test_helper::{self, new_engine_with_options, EngineOptions};
    use informalsystems_malachitebft_core_types::{NilOrVal, Round};
    use libp2p::identity::ed25519::{Keypair, PublicKey};
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::sync::{broadcast, mpsc};

    struct TestSetup {
        block_receiver: BlockReceiver,
        block_tx: broadcast::Sender<Block>,
        mempool_rx: mpsc::Receiver<MempoolRequest>,
        system_rx: mpsc::Receiver<SystemMessage>,
        shard_engine: ShardEngine,
        block_engine: BlockEngine,
        validator_keypair: Keypair,
        _temp_dir: TempDir,
    }

    async fn setup_test() -> TestSetup {
        let shard_id = 1;
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = RocksDB::new(db_path.to_str().unwrap());
        db.open().unwrap();
        let db = Arc::new(db);

        // Create block engine
        let (block_engine, _temp_dir_block) = block_engine_test_helpers::setup(None);

        // Create shard engine
        let (shard_engine, _temp_dir2) = new_engine_with_options(EngineOptions {
            db: Some(db),
            shard_id,
            ..Default::default()
        })
        .await;

        // Create channels for BlockReceiver
        let (block_tx, block_rx) = broadcast::channel(100);
        let (mempool_tx, mempool_rx) = mpsc::channel(100);
        let (system_tx, system_rx) = mpsc::channel(100);

        // Create a test keypair and validator set
        let validator_keypair = Keypair::generate();
        let public_key = PublicKey::try_from_bytes(&validator_keypair.public().to_bytes()).unwrap();

        // Create validator set with the test keypair
        let shard = SnapchainShard::new(shard_id);
        let validator = SnapchainValidator::new(shard.clone(), public_key, None, 0);
        let mut validator_set = SnapchainValidatorSet::new(vec![]);
        validator_set.add(validator);

        let stored_validator_set = StoredValidatorSet {
            effective_at: 0,
            validators: validator_set,
            shard_ids: vec![shard_id],
        };
        let validator_sets = StoredValidatorSets::new(shard_id, vec![stored_validator_set]);

        let block_receiver = BlockReceiver {
            shard_id,
            block_rx,
            mempool_tx,
            system_tx,
            event_rx: shard_engine.get_senders().events_tx.subscribe(),
            stores: shard_engine.get_stores(),
            validator_sets,
        };

        TestSetup {
            block_receiver,
            block_tx,
            mempool_rx,
            system_rx,
            shard_engine,
            block_engine,
            validator_keypair,
            _temp_dir: temp_dir,
        }
    }

    fn add_commits_to_block(block: &mut Block, keypair: &Keypair) {
        if !block.events.is_empty() {
            let height = block
                .header
                .as_ref()
                .unwrap()
                .height
                .as_ref()
                .unwrap()
                .clone();
            let hash = ShardHash {
                shard_index: height.shard_index,
                hash: block.header.as_ref().unwrap().state_root.clone(),
            };
            let round = Round::from(0u32);
            let signer = keypair.public().to_bytes().to_vec();
            let address = Address::from_vec(signer.clone());
            let vote =
                Vote::new_precommit(height.clone(), round, NilOrVal::Val(hash.clone()), address);
            let signature = keypair.sign(&vote.to_sign_bytes());
            block.commits = Some(Commits {
                height: Some(height),
                round: round.as_i64(),
                value: Some(hash),
                signatures: vec![CommitSignature { signature, signer }],
            });
        }
    }

    fn generate_heartbeats(
        block_engine: &mut BlockEngine,
        block_tx: Option<&broadcast::Sender<Block>>,
        keypair: &Keypair,
        num_heartbeats: u32,
    ) -> Vec<Block> {
        // Generate 5 empty blocks to trigger heartbeat on the 5th block
        let mut blocks = vec![];
        for _ in 0..(num_heartbeats * 5) {
            let height = block_engine.get_confirmed_height().increment();
            let state_change = block_engine.propose_state_change(vec![], height);
            let mut block = block_engine_test_helpers::validate_and_commit_state_change(
                block_engine,
                &state_change,
            );

            // Add commits if the block has events
            add_commits_to_block(&mut block, keypair);

            if let Some(block_tx) = block_tx {
                block_tx.send(block.clone()).unwrap();
            }
            blocks.push(block.clone())
        }
        blocks
    }

    async fn process_heartbeats(
        shard_engine: &mut ShardEngine,
        mempool_rx: &mut mpsc::Receiver<MempoolRequest>,
        num_heartbeats: u32,
    ) {
        assert_eq!(mempool_rx.len() as u32, num_heartbeats);
        // Verify heartbeat event was processed - should receive mempool request
        for _ in 0..num_heartbeats {
            let mempool_request = mempool_rx.try_recv();
            assert!(mempool_request.is_ok());

            if let MempoolRequest::AddMessage(msg, source, _) = mempool_request.unwrap() {
                assert!(matches!(msg, MempoolMessage::BlockEvent { .. }));
                assert!(matches!(source, MempoolSource::Local));
                let state_change = shard_engine.propose_state_change(1, vec![msg], None);
                test_helper::validate_and_commit_state_change(shard_engine, &state_change).await;
            }
        }
    }

    async fn sync_block_events(
        block_engine: &BlockEngine,
        system_rx: &mut mpsc::Receiver<SystemMessage>,
        keypair: &Keypair,
        num_events: u32,
    ) {
        for _ in 0..num_events {
            if let SystemMessage::BlockRequest {
                block_event_seqnum,
                block_tx,
            } = system_rx.recv().await.unwrap()
            {
                let block_stores = block_engine.stores();
                let mut block = block_stores
                    .get_block_by_event_seqnum(block_event_seqnum)
                    .unwrap();

                // Add commits to the retrieved block if it has events
                add_commits_to_block(&mut block, keypair);

                block_tx.send(Some(block)).unwrap();
            }
        }
    }

    #[tokio::test]
    async fn test_block_receiver_processes_heartbeat_events() {
        let mut setup = setup_test().await;
        let handle = tokio::spawn(async move { setup.block_receiver.run().await });

        // Create a block with heartbeat event, each heartbeat block is separated by 4 empty blocks.
        generate_heartbeats(
            &mut setup.block_engine,
            Some(&setup.block_tx),
            &setup.validator_keypair,
            2,
        );

        tokio::time::sleep(Duration::from_millis(100)).await;

        process_heartbeats(&mut setup.shard_engine, &mut setup.mempool_rx, 1).await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Now the second heartbeat is dispatched because the previous one has been committed.
        process_heartbeats(&mut setup.shard_engine, &mut setup.mempool_rx, 1).await;

        handle.abort();
    }

    #[tokio::test]
    async fn test_sync_dropped_blocks() {
        let mut setup = setup_test().await;
        let handle = tokio::spawn(async move { setup.block_receiver.run().await });

        generate_heartbeats(
            &mut setup.block_engine,
            Some(&setup.block_tx),
            &setup.validator_keypair,
            1,
        );

        tokio::time::sleep(Duration::from_millis(100)).await;

        process_heartbeats(&mut setup.shard_engine, &mut setup.mempool_rx, 1).await;

        generate_heartbeats(&mut setup.block_engine, None, &setup.validator_keypair, 2);
        assert_eq!(setup.mempool_rx.len(), 0);

        generate_heartbeats(
            &mut setup.block_engine,
            Some(&setup.block_tx),
            &setup.validator_keypair,
            1,
        );
        assert_eq!(setup.mempool_rx.len(), 0);

        tokio::time::sleep(Duration::from_millis(100)).await;

        sync_block_events(
            &setup.block_engine,
            &mut setup.system_rx,
            &setup.validator_keypair,
            2,
        )
        .await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        // The last heartbeat won't be processed til all synced block events are confirmed
        process_heartbeats(&mut setup.shard_engine, &mut setup.mempool_rx, 2).await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        process_heartbeats(&mut setup.shard_engine, &mut setup.mempool_rx, 1).await;

        handle.abort();
    }

    #[tokio::test]
    async fn test_sync_confirmation_timeout_blocks() {
        let mut setup = setup_test().await;
        let handle = tokio::spawn(async move { setup.block_receiver.run().await });

        generate_heartbeats(
            &mut setup.block_engine,
            Some(&setup.block_tx),
            &setup.validator_keypair,
            2,
        );

        // We time out waiting for confirmation on the first event. We sync it again.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(setup.mempool_rx.len(), 1);
        // Just drain the first block event from the mempool.
        setup.mempool_rx.recv().await.unwrap();

        sync_block_events(
            &setup.block_engine,
            &mut setup.system_rx,
            &setup.validator_keypair,
            1,
        )
        .await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Wait for synced block to get confirmed
        process_heartbeats(&mut setup.shard_engine, &mut setup.mempool_rx, 1).await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Then the other block is enqueued
        process_heartbeats(&mut setup.shard_engine, &mut setup.mempool_rx, 1).await;

        handle.abort();
    }

    #[tokio::test]
    async fn test_block_with_invalid_signature_rejected() {
        let mut setup = setup_test().await;
        let handle = tokio::spawn(async move { setup.block_receiver.run().await });

        // Generate a different keypair that's not in the validator set
        let invalid_keypair = Keypair::generate();

        // Generate heartbeats but sign them with the invalid keypair
        generate_heartbeats(
            &mut setup.block_engine,
            Some(&setup.block_tx),
            &invalid_keypair,
            1,
        );

        tokio::time::sleep(Duration::from_millis(100)).await;

        // No events should be processed since the signature is invalid
        assert_eq!(setup.mempool_rx.len(), 0);

        handle.abort();
    }
}
