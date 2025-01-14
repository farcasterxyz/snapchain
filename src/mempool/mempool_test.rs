#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tokio::sync::mpsc;

    use crate::{
        mempool::mempool::Mempool,
        proto::{FnameTransfer, UserNameProof, ValidatorMessage},
        storage::store::{
            engine::{MempoolMessage, ShardEngine},
            test_helper,
        },
        utils::factory::{events_factory, messages_factory},
    };

    use self::test_helper::{default_custody_address, default_signer};

    fn setup() -> (ShardEngine, Mempool) {
        let (_mempool_tx, mempool_rx) = mpsc::channel(100);
        let (_mempool_tx, messages_request_rx) = mpsc::channel(100);
        let (engine, _) = test_helper::new_engine();
        let mut shard_senders = HashMap::new();
        shard_senders.insert(1, engine.get_senders());
        let mut shard_stores = HashMap::new();
        shard_stores.insert(1, engine.get_stores());
        let mempool = Mempool::new(mempool_rx, messages_request_rx, 1, shard_stores);
        (engine, mempool)
    }

    #[tokio::test]
    async fn test_duplicate_user_message_is_invalid() {
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

    #[tokio::test]
    async fn test_duplicate_onchain_event_is_invalid() {
        let (mut engine, mut mempool) = setup();
        let onchain_event = events_factory::create_rent_event(1234, Some(10), None, false);
        let valid = mempool.message_is_valid(&MempoolMessage::ValidatorMessage(ValidatorMessage {
            on_chain_event: Some(onchain_event.clone()),
            fname_transfer: None,
        }));
        assert!(valid);
        test_helper::commit_event(&mut engine, &onchain_event).await;
        let valid = mempool.message_is_valid(&MempoolMessage::ValidatorMessage(ValidatorMessage {
            on_chain_event: Some(onchain_event.clone()),
            fname_transfer: None,
        }));
        assert!(!valid)
    }

    #[tokio::test]
    async fn test_duplicate_fname_transfer_is_invalid() {
        let (mut engine, mut mempool) = setup();
        let valid = mempool.message_is_valid(&MempoolMessage::ValidatorMessage(ValidatorMessage {
            on_chain_event: None,
            fname_transfer: Some(FnameTransfer {
                id: 1234,
                from_fid: 0,
                proof,
            }),
        }));
        assert!(valid);
        test_helper::commit_event(&mut engine, &onchain_event).await;
        let valid = mempool.message_is_valid(&MempoolMessage::ValidatorMessage(ValidatorMessage {
            on_chain_event: Some(onchain_event.clone()),
            fname_transfer: None,
        }));
        assert!(!valid)
    }
}
