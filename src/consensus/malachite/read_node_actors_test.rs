#[cfg(test)]
mod tests {

    use std::time::Duration;

    use crate::consensus::malachite::network_connector::MalachiteNetworkEvent;
    use crate::consensus::malachite::spawn_read_node::MalachiteReadNodeActors;
    use crate::consensus::read_validator::Engine;
    use crate::core::types::SnapchainValidatorContext;
    use crate::network::gossip::GossipEvent;
    use crate::proto::{self, Height, ShardChunk, StatusMessage};
    use crate::storage::store::engine::ShardEngine;
    use crate::storage::store::test_helper::{
        self, commit_event, default_storage_event, new_engine_with_options, EngineOptions,
        FID_FOR_TEST,
    };
    use bytes::Bytes;
    use informalsystems_malachitebft_metrics::SharedRegistry;
    use informalsystems_malachitebft_network::{Channel, PeerIdExt};
    use informalsystems_malachitebft_sync::PeerId;
    use libp2p::identity::ed25519::Keypair;
    use prost::Message;
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

        let (read_node_engine_clone, _) = new_engine_with_options(EngineOptions {
            limits: None,
            db: Some(read_node_engine.db.clone()),
            messages_request_tx: None,
        });
        let read_node_actors = MalachiteReadNodeActors::create_and_start(
            SnapchainValidatorContext::new(keypair),
            Engine::ShardEngine(read_node_engine_clone),
            libp2p::PeerId::random(),
            gossip_tx,
            SharedRegistry::global(),
            shard_id,
            test_helper::statsd_client(),
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

    async fn assert_height_on_status(
        gossip_rx: &mut mpsc::Receiver<GossipEvent<SnapchainValidatorContext>>,
        expected_height: Height,
    ) {
        wait_for_status().await;
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

        actors.cast_decided_value(decided_value).unwrap();
        wait_for_block().await;
    }

    async fn commit_shard_chunk(engine: &mut ShardEngine) -> ShardChunk {
        commit_event(engine, &default_storage_event(FID_FOR_TEST)).await
    }

    async fn wait_for_block() {
        tokio::time::sleep(Duration::from_millis(10)).await
    }

    async fn wait_for_status() {
        tokio::time::sleep(Duration::from_secs(6)).await
    }

    #[tokio::test]
    async fn test_process_decided_value() {
        let (mut proposer_engine, read_node_engine, mut gossip_rx, actors) = setup(0).await;

        let shard_chunk = commit_shard_chunk(&mut proposer_engine).await;
        process_shard_chunk(&actors, &shard_chunk).await;
        assert_eq!(
            read_node_engine.get_confirmed_height(),
            shard_chunk.header.as_ref().unwrap().height.unwrap()
        );

        assert_height_on_status(
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
        let (mut proposer_engine, read_node_engine, mut gossip_rx, actors) =
            setup(num_initial_blocks).await;

        let start_height = Height {
            shard_index: read_node_engine.shard_id(),
            block_number: num_initial_blocks,
        };

        assert_eq!(read_node_engine.get_confirmed_height(), start_height);
        assert_height_on_status(&mut gossip_rx, start_height).await;

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

        // This shard chunk is just dropped
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

    #[tokio::test]
    async fn test_send_sync_request() {
        let (_proposer_engine, read_node_engine, mut gossip_rx, actors) = setup(0).await;

        let peer = PeerId::from_libp2p(&libp2p::PeerId::random());
        actors
            .cast_network_event(MalachiteNetworkEvent::Message(
                Channel::Sync,
                peer,
                Bytes::from(
                    (StatusMessage {
                        peer_id: peer.to_libp2p().to_bytes(),
                        height: Some(Height {
                            shard_index: read_node_engine.shard_id(),
                            block_number: 10,
                        }),
                        min_height: Some(Height {
                            shard_index: read_node_engine.shard_id(),
                            block_number: 1,
                        }),
                    })
                    .encode_to_vec(),
                ),
            ))
            .unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        // Show that we send a sync request when we're behind a peer
        match gossip_rx.recv().await.unwrap() {
            GossipEvent::SyncRequest(peer_id, request, _sender) => {
                assert_eq!(peer_id, peer);
                match request {
                    informalsystems_malachitebft_sync::Request::ValueRequest(value_request) => {
                        assert_eq!(
                            value_request.height,
                            read_node_engine.get_confirmed_height().increment()
                        );
                    }
                    _ => panic!("Expected value request"),
                }
            }
            _ => {
                panic!("Expected sync request")
            }
        };
    }
}
