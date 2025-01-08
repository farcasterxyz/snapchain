#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tokio::sync::mpsc;

    use crate::{
        mempool::mempool::Mempool,
        storage::store::{
            engine::{MempoolMessage, ShardEngine},
            test_helper,
        },
        utils::factory::messages_factory,
    };

    use self::test_helper::{default_custody_address, default_signer};

    fn setup() -> (ShardEngine, Mempool) {
        let (_mempool_tx, mempool_rx) = mpsc::channel(100);
        let (engine, _) = test_helper::new_engine();
        let mut shard_senders = HashMap::new();
        shard_senders.insert(1, engine.get_senders());
        let mut shard_stores = HashMap::new();
        shard_stores.insert(1, engine.get_stores());
        let mempool = Mempool::new(mempool_rx, 1, shard_senders, shard_stores);
        (engine, mempool)
    }

    #[tokio::test]
    async fn test_duplicate_message_is_invalid() {
        let (mut engine, mut mempool) = setup();
        test_helper::register_user(
            1234,
            default_signer(),
            default_custody_address(),
            &mut engine,
        )
        .await;
        let cast = messages_factory::casts::create_cast_add(1234, "hello", None, None);
        let valid = mempool.message_is_valid(&MempoolMessage::UserMessage(cast.clone()));
        assert!(valid);
        test_helper::commit_message(&mut engine, &cast).await;
        let valid = mempool.message_is_valid(&MempoolMessage::UserMessage(cast.clone()));
        assert!(!valid)
    }
}
