#[cfg(test)]
mod tests {
    use crate::{
        proto,
        replication::{
            replication_stores::ReplicationStores,
            replicator::{self, Replicator, ReplicatorSnapshotOptions},
        },
        storage::{
            db::{RocksDB, RocksdbError},
            store::{
                account::UserDataStore,
                engine::{MempoolMessage, PostCommitMessage, ShardEngine},
                test_helper::{self, EngineOptions},
            },
            trie::merkle_trie::TrieKey,
        },
        utils::factory::{self, messages_factory, username_factory},
    };
    use std::{collections::HashMap, sync::Arc, time::Duration};

    fn opendb(path: &str) -> Result<Arc<RocksDB>, RocksdbError> {
        let milliseconds_timestamp: u128 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let path = format!("{}/{}", path, milliseconds_timestamp);
        let db = RocksDB::new(&path);
        db.open()?;
        Ok(Arc::new(db))
    }

    fn new_engine_with_fname_signer(
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
        });
        (signer, engine)
    }

    async fn commit_message(engine: &mut ShardEngine, message: &proto::Message) {
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
        assert!(engine.trie_key_exists(test_helper::trie_ctx(), &TrieKey::for_message(message)));
    }

    async fn register_fid(engine: &mut ShardEngine, fid: u64) -> ed25519_dalek::SigningKey {
        let signer = factory::signers::generate_signer();
        let address = factory::address::generate_random_address();
        test_helper::register_user(fid, signer.clone(), address, engine).await;
        signer
    }

    fn has_fname(engine: &mut ShardEngine, fid: u64, fname: Option<&String>) -> bool {
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

    async fn register_fname(
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

    async fn transfer_fname(
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

    async fn send_cast(
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

    async fn like_cast(
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

    async fn set_bio(
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

    async fn create_link(
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

    async fn create_compact_link(
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

    fn setup_replicator(engine: &mut ShardEngine) -> Arc<Replicator> {
        let statsd_client = crate::utils::statsd_wrapper::StatsdClientWrapper::new(
            cadence::StatsdClient::builder("", cadence::NopMetricSink {}).build(),
            true,
        );

        let mut shard_stores = HashMap::new();
        shard_stores.insert(engine.shard_id(), engine.get_stores().clone());

        let replication_stores = Arc::new(ReplicationStores::new(
            shard_stores.clone(),
            16,
            statsd_client.clone(),
            engine.network.clone(),
        ));

        let replicator = Arc::new(Replicator::new_with_options(
            replication_stores.clone(),
            ReplicatorSnapshotOptions {
                interval: 1,
                max_age: Duration::from_secs(10),
            },
        ));

        replicator
    }

    fn replicate_fids(
        source_engine: &ShardEngine,
        dest_engine: &mut ShardEngine,
        replicator: Arc<Replicator>,
        fids_to_sync: &Vec<u64>,
    ) {
        let height = source_engine.get_confirmed_height().block_number;
        let min = *fids_to_sync.iter().min().unwrap();
        let max = *fids_to_sync.iter().max().unwrap();

        let (sys, user) =
            match replicator.transactions_for_fid_range(height, source_engine.shard_id(), min, max)
            {
                Ok(transactions) => transactions,
                Err(e) => {
                    panic!("Error fetching transactions for fid range: {}", e);
                }
            };

        dest_engine
            .replay_fid_transactions(sys, user)
            .expect("Failed to replay transactions for fid");

        for fid in fids_to_sync {
            let root1 = source_engine.account_root_for_fid(*fid);
            let root2 = dest_engine.account_root_for_fid(*fid);

            println!(
                "Account root for fid {}: {} vs {}",
                fid,
                hex::encode(&root1),
                hex::encode(&root2)
            );

            assert_eq!(root1, root2, "Account roots do not match for fid {}", fid);
        }
    }

    #[tokio::test]
    async fn test_replication() {
        // open tmp dir for database
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let (post_commit_tx, post_commit_rx) = tokio::sync::mpsc::channel::<PostCommitMessage>(1);

        let (signer, mut engine) = new_engine_with_fname_signer(&tmp_dir, Some(post_commit_tx)); // source engine
        let (_, mut new_engine) = new_engine_with_fname_signer(&tmp_dir, None); // engine to replicate to

        let replicator = setup_replicator(&mut engine);
        let spawned_replicator = replicator.clone();
        tokio::spawn(async move {
            replicator::run(spawned_replicator, post_commit_rx).await;
        });

        // Note: we're using FID3_FOR_TEST here because the address verification message contains
        // that FID.
        let fid = test_helper::FID3_FOR_TEST;
        let fid_signer = register_fid(&mut engine, fid).await;

        let fid2 = 2000;
        let fid2_signer = register_fid(&mut engine, fid2).await;

        // Running timestamp
        let mut timestamp = factory::time::farcaster_time();

        timestamp += 1;

        let fname = &"replica-test".to_string();

        register_fname(
            &mut engine,
            &signer,
            fid,
            Some(&fid_signer),
            fname,
            Some(timestamp),
        )
        .await;

        set_bio(
            &mut engine,
            fid2,
            Some(&fid2_signer),
            &"hello".to_string(),
            Some(timestamp),
        )
        .await;

        timestamp += 1;

        transfer_fname(
            &mut engine,
            &signer,
            fid,
            Some(&fid_signer),
            fid2,
            fname,
            Some(timestamp),
        )
        .await;

        timestamp += 1;

        let cast = send_cast(
            &mut engine,
            fid,
            Some(&fid_signer),
            "hello world",
            Some(timestamp),
        )
        .await;

        timestamp += 1;

        create_link(&mut engine, fid, Some(&fid_signer), fid2, Some(timestamp)).await;
        create_compact_link(
            &mut engine,
            fid2,
            Some(&fid2_signer),
            vec![fid],
            Some(timestamp),
        )
        .await;
        like_cast(
            &mut engine,
            fid2,
            Some(&fid2_signer),
            &cast,
            Some(timestamp),
        )
        .await;

        timestamp += 1;

        // Note: has to use FID3_FOR_TEST
        let address_verification_add = messages_factory::verifications::create_verification_add(
            fid,
            0,
            hex::decode("91031dcfdea024b4d51e775486111d2b2a715871").unwrap(),
            hex::decode("b72c63d61f075b36fb66a9a867b50836cef19d653a3c09005628738677bcb25f25b6b6e6d2e1d69cd725327b3c020deef9e2575a22dc8ed08f88bc75718ce1cb1c").unwrap(),
            hex::decode("d74860c4bbf574d5ad60f03a478a30f990e05ac723e138a5c860cdb3095f4296").unwrap(),
            Some(timestamp),
            Some(&fid_signer),
        );

        commit_message(&mut engine, &address_verification_add).await;

        timestamp += 1;

        let username_proof_add = messages_factory::username_proof::create_username_proof(
            fid,
            proto::UserNameType::UsernameTypeEnsL1,
            "username.eth".to_string().clone(),
            hex::decode("91031dcfdea024b4d51e775486111d2b2a715871").unwrap(),
            "signature".to_string(),
            timestamp as u64,
            Some(&fid_signer),
        );

        commit_message(&mut engine, &username_proof_add).await;

        replicate_fids(&engine, &mut new_engine, replicator, &vec![fid, fid2]);
    }
}
