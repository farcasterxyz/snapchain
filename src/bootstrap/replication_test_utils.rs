#[cfg(test)]
pub mod replication_test_utils {
    use crate::{
        proto,
        replication::{
            replication_stores::ReplicationStores,
            replicator::{Replicator, ReplicatorSnapshotOptions},
            ReplicationServer,
        },
        storage::{
            db::{RocksDB, RocksdbError},
            store::{
                account::UserDataStore,
                engine::{PostCommitMessage, ShardEngine},
                mempool_poller::MempoolMessage,
                test_helper::{self, EngineOptions},
                BlockStore,
            },
            trie::merkle_trie::TrieKey,
        },
        utils::factory::{self, messages_factory, username_factory},
    };
    use std::{collections::HashMap, sync::Arc, time::Duration};

    pub fn opendb(path: &str) -> Result<Arc<RocksDB>, RocksdbError> {
        let milliseconds_timestamp: u128 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let path = format!("{}/{}", path, milliseconds_timestamp);
        let db = RocksDB::new(&path);
        db.open()?;
        Ok(Arc::new(db))
    }

    pub async fn new_engine_with_fname_signer(
        tmp: &tempfile::TempDir,
        post_commit_tx: Option<tokio::sync::mpsc::Sender<PostCommitMessage>>,
    ) -> (alloy_signer_local::PrivateKeySigner, ShardEngine) {
        let signer = alloy_signer_local::PrivateKeySigner::random();

        let db = opendb(tmp.path().to_str().unwrap()).expect("Failed to open RocksDB");

        let (engine, _tmpdir) = test_helper::new_engine_with_options(EngineOptions {
            db: Some(db),
            fname_signer_address: Some(signer.address()),
            post_commit_tx,
            ..EngineOptions::default()
        })
        .await;
        (signer, engine)
    }

    pub async fn commit_message(engine: &mut ShardEngine, message: &proto::Message) {
        let state_change = engine.propose_state_change(
            1,
            vec![MempoolMessage::UserMessage(message.clone())],
            None,
        );

        if state_change.transactions.is_empty() {
            panic!("Failed to propose message");
        }

        let chunk = test_helper::validate_and_commit_state_change(engine, &state_change).await;

        assert_eq!(
            state_change.new_state_root,
            chunk.header.as_ref().unwrap().shard_root
        );

        assert!(TrieKey::for_message(message)
            .iter()
            .all(|trie_key| engine.trie_key_exists(test_helper::trie_ctx(), trie_key)));
    }

    pub async fn register_fid(engine: &mut ShardEngine, fid: u64) -> ed25519_dalek::SigningKey {
        let signer = factory::signers::generate_signer();
        let address = factory::address::generate_random_address();
        test_helper::register_user(fid, signer.clone(), address, engine).await;
        signer
    }

    pub fn has_fname(engine: &mut ShardEngine, fid: u64, fname: Option<&String>) -> bool {
        let result =
            UserDataStore::get_username_proof_by_fid(&engine.get_stores().user_data_store, fid);
        assert!(result.is_ok());

        if let Some(fname) = fname {
            result
                .unwrap()
                .is_some_and(|proof| proof.name == fname.as_bytes().to_vec())
                && test_helper::key_exists_in_trie(engine, &TrieKey::for_fname(fid, fname))
        } else {
            result.unwrap().is_none()
        }
    }

    pub async fn register_fname(
        engine: &mut ShardEngine,
        fname_signer: &alloy_signer_local::PrivateKeySigner,
        fid: u64,
        signing_key: Option<&ed25519_dalek::SigningKey>,
        fname: &String,
        timestamp: Option<u32>,
    ) {
        let fname_transfer = username_factory::create_transfer(
            fid,
            fname,
            timestamp,
            None,
            Some(test_helper::default_custody_address()),
            fname_signer.clone(),
        );

        test_helper::commit_fname_transfer(engine, &fname_transfer).await;

        assert!(has_fname(engine, fid, Some(fname)));

        let username_message = messages_factory::user_data::create_user_data_add(
            fid,
            proto::UserDataType::Username,
            &fname,
            timestamp,
            signing_key,
        );
        commit_message(engine, &username_message).await;
    }

    pub async fn transfer_fname(
        engine: &mut ShardEngine,
        fname_signer: &alloy_signer_local::PrivateKeySigner,
        fid: u64,
        _signing_key: Option<&ed25519_dalek::SigningKey>,
        new_fid: u64,
        fname: &String,
        timestamp: Option<u32>,
    ) {
        assert!(has_fname(engine, fid, Some(fname)));
        assert!(has_fname(engine, new_fid, None));

        let fname_transfer = username_factory::create_transfer(
            new_fid,
            fname,
            timestamp,
            Some(fid),
            Some(test_helper::default_custody_address()),
            fname_signer.clone(),
        );

        test_helper::commit_fname_transfer(engine, &fname_transfer).await;

        assert!(has_fname(engine, fid, None));
        assert!(has_fname(engine, new_fid, Some(fname)));
    }

    pub async fn send_cast(
        engine: &mut ShardEngine,
        fid: u64,
        signer: Option<&ed25519_dalek::SigningKey>,
        content: &str,
        timestamp: Option<u32>,
    ) -> proto::Message {
        let cast = messages_factory::casts::create_cast_add(fid, content, timestamp, signer);
        commit_message(engine, &cast).await;
        cast
    }

    pub async fn like_cast(
        engine: &mut ShardEngine,
        fid: u64,
        signer: Option<&ed25519_dalek::SigningKey>,
        cast: &proto::Message,
        timestamp: Option<u32>,
    ) -> proto::Message {
        let reaction_body = proto::ReactionBody {
            r#type: proto::ReactionType::Like as i32,
            target: Some(proto::reaction_body::Target::TargetCastId(proto::CastId {
                fid: cast.fid(),
                hash: cast.hash.clone(),
            })),
        };
        let like = messages_factory::create_message_with_data(
            fid,
            proto::MessageType::ReactionAdd,
            proto::message_data::Body::ReactionBody(reaction_body),
            timestamp,
            signer,
        );
        commit_message(engine, &like).await;
        like
    }

    pub async fn set_bio(
        engine: &mut ShardEngine,
        fid: u64,
        signer: Option<&ed25519_dalek::SigningKey>,
        bio: &String,
        timestamp: Option<u32>,
    ) -> proto::Message {
        let user_data_add = messages_factory::user_data::create_user_data_add(
            fid,
            proto::UserDataType::Bio,
            bio,
            timestamp,
            signer,
        );
        commit_message(engine, &user_data_add).await;
        user_data_add
    }

    pub async fn create_link(
        engine: &mut ShardEngine,
        fid: u64,
        signer: Option<&ed25519_dalek::SigningKey>,
        target_fid: u64,
        timestamp: Option<u32>,
    ) -> proto::Message {
        let link =
            messages_factory::links::create_link_add(fid, "follow", target_fid, timestamp, signer);
        commit_message(engine, &link).await;
        link
    }

    pub async fn create_compact_link(
        engine: &mut ShardEngine,
        fid: u64,
        signer: Option<&ed25519_dalek::SigningKey>,
        target_fids: Vec<u64>,
        timestamp: Option<u32>,
    ) -> proto::Message {
        let link = messages_factory::links::create_link_compact_state(
            fid,
            "follow",
            target_fids,
            timestamp,
            signer,
        );
        commit_message(engine, &link).await;
        link
    }

    pub fn setup_replicator(engine: &mut ShardEngine) -> (Arc<Replicator>, ReplicationServer) {
        use crate::storage::store::block_engine_test_helpers::default_block;

        let statsd_client = crate::utils::statsd_wrapper::StatsdClientWrapper::new(
            cadence::StatsdClient::builder("", cadence::NopMetricSink {}).build(),
            true,
        );

        let mut shard_stores = HashMap::new();
        shard_stores.insert(engine.shard_id(), engine.get_stores().clone());

        let replication_stores = Arc::new(ReplicationStores::new(
            shard_stores.clone(),
            statsd_client.clone(),
            engine.network.clone(),
        ));

        let replicator = Arc::new(Replicator::new_with_options(
            replication_stores.clone(),
            statsd_client,
            ReplicatorSnapshotOptions {
                interval: 1,
                max_age: Duration::from_secs(10),
            },
        ));

        let block_store = BlockStore::new(engine.db.clone());
        let block = default_block();
        block_store.put_block(&block).unwrap();

        // Set up the replication server with the given replicator
        let replication_server = ReplicationServer::new(replicator.clone(), block_store);

        (replicator, replication_server)
    }
}
