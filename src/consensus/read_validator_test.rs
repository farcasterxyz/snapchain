#[cfg(test)]
mod tests {

    use std::collections::BTreeMap;

    use crate::consensus::malachite::read_sync::ReadSyncRef;
    use crate::consensus::malachite::spawn_read_node::MalachiteReadNodeActors;
    use crate::consensus::read_validator::{Engine, ReadValidator};
    use crate::core::types::SnapchainValidatorContext;
    use crate::network::gossip::GossipEvent;
    use crate::proto::{self, Commits, Height, ShardChunk, ShardHash};
    use crate::storage::store::engine::ShardEngine;
    use crate::storage::store::test_helper::{
        self, commit_event, default_storage_event, FID_FOR_TEST,
    };
    use informalsystems_malachitebft_metrics::SharedRegistry;
    use libp2p::identity::ed25519::Keypair;
    use tokio::sync::mpsc;

    async fn setup(
        num_already_decided_blocks: u64,
    ) -> (
        ShardEngine,
        ShardEngine,
        ReadValidator,
        ReadSyncRef,
        mpsc::Receiver<GossipEvent<SnapchainValidatorContext>>,
    ) {
        let (mut proposer_engine, _) = test_helper::new_engine();
        let (mut read_node_engine, _) = test_helper::new_engine();
        for _ in 0..num_already_decided_blocks {
            let shard_chunk = commit_shard_chunk(&mut proposer_engine).await;
            read_node_engine.commit_shard_chunk(&shard_chunk);
        }

        let shard_id = read_node_engine.shard_id();
        let read_validator = ReadValidator {
            shard_id: read_node_engine.shard_id(),
            last_height: Height {
                shard_index: read_node_engine.shard_id(),
                block_number: 0,
            },
            engine: Engine::ShardEngine(read_node_engine.clone()),
            max_num_buffered_blocks: 1,
            buffered_blocks: BTreeMap::new(),
        };

        let keypair = Keypair::generate();
        let (gossip_tx, gossip_rx) = mpsc::channel(100);

        let read_node_actors = MalachiteReadNodeActors::create_and_start(
            SnapchainValidatorContext::new(keypair),
            Engine::ShardEngine(read_node_engine.clone()),
            libp2p::PeerId::random(),
            gossip_tx,
            SharedRegistry::global(),
            shard_id,
        )
        .await
        .unwrap();

        (
            proposer_engine,
            read_node_engine,
            read_validator,
            read_node_actors.sync_actor,
            gossip_rx,
        )
    }

    async fn commit_shard_chunk(engine: &mut ShardEngine) -> ShardChunk {
        let mut shard_chunk = commit_event(engine, &default_storage_event(FID_FOR_TEST)).await;
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
        shard_chunk
    }

    async fn process_decided_value(
        read_validator: &mut ReadValidator,
        shard_chunk: &ShardChunk,
        sync: &ReadSyncRef,
    ) {
        let decided_value = proto::DecidedValue {
            value: Some(proto::decided_value::Value::Shard(shard_chunk.clone())),
        };
        read_validator.process_decided_value(decided_value, sync);
    }

    #[tokio::test]
    async fn test_get_decided_value() {
        let (mut proposer_engine, read_node_engine, mut read_validator, sync, _gossip_rx) =
            setup(0).await;
        let shard_chunk = commit_shard_chunk(&mut proposer_engine).await;
        process_decided_value(&mut read_validator, &shard_chunk, &sync).await;
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
    async fn test_buffered_values() {
        let (mut proposer_engine, read_node_engine, mut read_validator, sync, _gossip_rx) =
            setup(0).await;
        let shard_chunk1 = commit_shard_chunk(&mut proposer_engine).await;
        let shard_chunk2 = commit_shard_chunk(&mut proposer_engine).await;
        let shard_chunk3 = commit_shard_chunk(&mut proposer_engine).await;
        // Drop the new block if the buffer map is full
        process_decided_value(&mut read_validator, &shard_chunk2, &sync).await;
        process_decided_value(&mut read_validator, &shard_chunk3, &sync).await;
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
        process_decided_value(&mut read_validator, &shard_chunk1, &sync).await;
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
        let (_proposer_engine, read_node_engine, mut read_validator, sync, _gossip_rx) =
            setup(3).await;

        read_validator.initialize_height(&sync);
        assert_eq!(
            read_validator.last_height,
            Height {
                shard_index: read_node_engine.shard_id(),
                block_number: 3
            }
        );
    }
}
