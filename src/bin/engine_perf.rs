use snapchain::proto::snapchain::{Height, ShardChunk, ShardHeader};
use snapchain::storage::db;
use snapchain::storage::store::engine::{MempoolMessage, ShardEngine, ShardStateChange};
use snapchain::storage::store::stores::{Limits, StoreLimits};
use snapchain::storage::trie::merkle_trie;
use snapchain::utils::cli::compose_message;
use snapchain::utils::factory::events_factory;
use snapchain::utils::statsd_wrapper::StatsdClientWrapper;
use std::error::Error;
use std::sync::Arc;
use tempfile;

//TODO: lots of copy and pasted code from engine_test.rs and other places

fn new_engine() -> (ShardEngine, tempfile::TempDir) {
    let statsd_client = StatsdClientWrapper::new(
        cadence::StatsdClient::builder("", cadence::NopMetricSink {}).build(),
        true,
    );
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("a.db");

    let db = db::RocksDB::new(db_path.to_str().unwrap());
    db.open().unwrap();

    let limits = Limits {
        casts: u32::MAX,
        links: u32::MAX,
        reactions: u32::MAX,
        user_data: u32::MAX,
        user_name_proofs: u32::MAX,
        verifications: u32::MAX,
    };

    let test_limits = StoreLimits {
        limits: limits.clone(),
        legacy_limits: limits.clone(),
    };

    let trie = merkle_trie::MerkleTrie::new(16).unwrap(); //TODO: don't hardcode; don't unwrap()
    (
        ShardEngine::new(Arc::new(db), trie, 1, test_limits, statsd_client),
        dir,
    )
}

const FID_FOR_TEST: u32 = 1234;

fn state_change_to_shard_chunk(
    shard_index: u32,
    block_number: u64,
    change: &ShardStateChange,
) -> ShardChunk {
    ShardChunk {
        header: Some(ShardHeader {
            shard_root: change.new_state_root.clone(),
            height: Some(Height {
                shard_index,
                block_number,
            }),
            timestamp: 0,
            parent_hash: vec![], // TODO
        }),
        transactions: change.transactions.clone(),
        hash: vec![],
        votes: None,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (mut engine, _tmpdir) = new_engine();

    let fid = FID_FOR_TEST;
    let mut i = 0;
    let messages_tx = engine.messages_tx();

    {
        let storage_event = events_factory::create_rent_event(fid, None, Some(1), false);
        // commit_event(&mut engine, &storage_event).await;

        messages_tx
            .send(MempoolMessage::ValidatorMessage(
                snapchain::proto::snapchain::ValidatorMessage {
                    on_chain_event: Some(storage_event.clone()),
                    fname_transfer: None,
                },
            ))
            .await
            .unwrap();
        let state_change = engine.propose_state_change(1);
        let chunk = state_change_to_shard_chunk(1, 1, &state_change);

        engine.commit_shard_chunk(&chunk);
    }

    loop {
        for _ in 0..100 {
            let text = format!("For benchmarking {}", i);
            let msg = compose_message(fid, text.as_str(), None, None);

            messages_tx
                .send(MempoolMessage::UserMessage(msg.clone()))
                .await
                .unwrap();
            i += 1;
        }

        let state_change = engine.propose_state_change(1);

        let valid = engine.validate_state_change(&state_change);
        assert!(valid);

        // TODO: need block height below
        let chunk = state_change_to_shard_chunk(1, 1, &state_change);
        engine.commit_shard_chunk(&chunk);

        println!("{}", engine.trie_num_items());
    }
}
