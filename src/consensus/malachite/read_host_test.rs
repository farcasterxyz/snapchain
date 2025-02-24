#[cfg(test)]
mod tests {

    use std::time::Duration;

    use crate::consensus::malachite::read_host::{Engine, ReadHostMsg};
    use crate::consensus::malachite::read_sync::{self};
    use crate::consensus::malachite::spawn::MalachiteReadNodeActors;
    use crate::core::types::SnapchainValidatorContext;
    use crate::network::gossip::GossipEvent;
    use crate::proto::{self, Height, ShardChunk};
    use crate::storage::store::engine::ShardEngine;
    use crate::storage::store::test_helper::{
        self, commit_event, default_storage_event, FID_FOR_TEST,
    };
    use informalsystems_malachitebft_metrics::SharedRegistry;
    use libp2p::identity::ed25519::Keypair;
    use libp2p::PeerId;
    use tokio::sync::mpsc;

    async fn setup(
        num_already_decided_blocks: u64,
    ) -> (
        ShardEngine,
        ShardEngine,
        mpsc::Receiver<GossipEvent<SnapchainValidatorContext>>,
        MalachiteReadNodeActors,
    ) {
        let (mut proposer_engine, _) = test_helper::new_engine();
        let (mut read_node_engine, _) = test_helper::new_engine();
        for _ in 0..num_already_decided_blocks {
            let shard_chunk = commit_shard_chunk(&mut proposer_engine).await;
            read_node_engine.commit_shard_chunk(&shard_chunk);
        }
        let keypair = Keypair::generate();
        let (gossip_tx, gossip_rx) = mpsc::channel(100);
        let shard_id = read_node_engine.shard_id();

        let read_node_actors = MalachiteReadNodeActors::create_and_start(
            SnapchainValidatorContext::new(keypair),
            Engine::ShardEngine(read_node_engine.clone()),
            PeerId::random(),
            gossip_tx,
            SharedRegistry::global(),
            shard_id,
        )
        .await
        .unwrap();
        (
            proposer_engine,
            read_node_engine,
            gossip_rx,
            read_node_actors,
        )
    }

    async fn broadcast_status_and_assert_height(
        actors: &MalachiteReadNodeActors,
        gossip_rx: &mut mpsc::Receiver<GossipEvent<SnapchainValidatorContext>>,
        expected_height: Height,
    ) {
        actors
            .sync_actor
            .call(
                |reply_to| read_sync::Msg::Tick {
                    reply_to: Some(reply_to),
                },
                None,
            )
            .await
            .unwrap();

        let gossip_msg = gossip_rx.recv().await.unwrap();
        match gossip_msg {
            GossipEvent::BroadcastStatus(status) => {
                assert_eq!(status.height, expected_height);
            }
            _ => panic!("Unexpected gossip message"),
        }
    }

    async fn process_shard_chunk(actors: &MalachiteReadNodeActors, shard_chunk: &ShardChunk) {
        let decided_value = proto::DecidedValue {
            value: Some(proto::decided_value::Value::Shard(shard_chunk.clone())),
        };

        actors
            .host_actor
            .call(
                |reply_to| ReadHostMsg::ProcessDecidedValue {
                    value: decided_value,
                    sync: actors.sync_actor.clone(),
                    reply_to: Some(reply_to),
                },
                None,
            )
            .await
            .unwrap();
    }

    async fn commit_shard_chunk(engine: &mut ShardEngine) -> ShardChunk {
        commit_event(engine, &default_storage_event(FID_FOR_TEST)).await
    }

    #[tokio::test]
    async fn test_process_decided_value() {
        let (mut proposer_engine, read_node_engine, mut gossip_rx, actors) = setup(0).await;

        let shard_chunk = commit_shard_chunk(&mut proposer_engine).await;
        process_shard_chunk(&actors, &shard_chunk).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(
            read_node_engine.get_confirmed_height(),
            shard_chunk.header.as_ref().unwrap().height.unwrap()
        );

        broadcast_status_and_assert_height(
            &actors,
            &mut gossip_rx,
            shard_chunk.header.as_ref().unwrap().height.unwrap(),
        )
        .await;
    }

    #[tokio::test]
    async fn test_out_of_order_decided_values() {
        let (mut proposer_engine, read_node_engine, _gossip_rx, actors) = setup(0).await;

        let shard_chunk1 = commit_shard_chunk(&mut proposer_engine).await;
        let shard_chunk2 = commit_shard_chunk(&mut proposer_engine).await;
        let shard_chunk3 = commit_shard_chunk(&mut proposer_engine).await;

        process_shard_chunk(&actors, &shard_chunk3).await;
        assert_eq!(
            read_node_engine.get_confirmed_height(),
            Height {
                shard_index: read_node_engine.shard_id(),
                block_number: 0
            }
        );

        process_shard_chunk(&actors, &shard_chunk2).await;
        assert_eq!(
            read_node_engine.get_confirmed_height(),
            Height {
                shard_index: read_node_engine.shard_id(),
                block_number: 0
            }
        );

        process_shard_chunk(&actors, &shard_chunk1).await;
        assert_eq!(
            read_node_engine.get_confirmed_height(),
            shard_chunk3.header.as_ref().unwrap().height.unwrap()
        );
    }

    #[tokio::test]
    async fn test_startup_with_already_decided_blocks() {
        let num_initial_blocks = 3;
        let (mut proposer_engine, read_node_engine, _gossip_rx, actors) =
            setup(num_initial_blocks).await;

        let start_height = Height {
            shard_index: read_node_engine.shard_id(),
            block_number: num_initial_blocks,
        };

        assert_eq!(read_node_engine.get_confirmed_height(), start_height);

        let shard_chunk4 = commit_shard_chunk(&mut proposer_engine).await;
        process_shard_chunk(&actors, &shard_chunk4).await;
        assert_eq!(
            read_node_engine.get_confirmed_height(),
            start_height.increment()
        );
    }

    #[tokio::test]
    async fn test_block_with_height_too_low() {
        let (mut proposer_engine, read_node_engine, _gossip_rx, actors) = setup(0).await;

        let shard_chunk1 = commit_shard_chunk(&mut proposer_engine).await;
        process_shard_chunk(&actors, &shard_chunk1).await;
        assert_eq!(
            read_node_engine.get_confirmed_height(),
            shard_chunk1.header.as_ref().unwrap().height.unwrap()
        );

        let shard_chunk2 = commit_shard_chunk(&mut proposer_engine).await;
        process_shard_chunk(&actors, &shard_chunk2).await;
        assert_eq!(
            read_node_engine.get_confirmed_height(),
            shard_chunk2.header.as_ref().unwrap().height.unwrap()
        );

        process_shard_chunk(&actors, &shard_chunk1).await;
        assert_eq!(
            read_node_engine.get_confirmed_height(),
            shard_chunk2.header.as_ref().unwrap().height.unwrap()
        );

        let shard_chunk3 = commit_shard_chunk(&mut proposer_engine).await;
        process_shard_chunk(&actors, &shard_chunk3).await;
        assert_eq!(
            read_node_engine.get_confirmed_height(),
            shard_chunk3.header.as_ref().unwrap().height.unwrap()
        );
    }
}
