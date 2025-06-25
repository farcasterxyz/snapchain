#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use base64::Engine;
    use foundry_common::ens::EnsError;
    use prost::Message;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tokio::time::{sleep, timeout};

    use crate::connectors::onchain_events::{Chain, ChainAPI, ChainClients};
    use crate::core::validations::{self, verification::VerificationAddressClaim};
    use crate::mempool::mempool::{self, Mempool};
    use crate::mempool::routing;
    use crate::mempool::routing::MessageRouter;
    use crate::network::server::MyHubService;
    use crate::proto::hub_service_server::HubService;
    use crate::proto::{
        self, EventRequest, EventsRequest, HubEvent, HubEventType, OnChainEventType, ShardChunk,
        UserDataType, UserNameProof, UserNameType, UsernameProofRequest,
        VerificationAddAddressBody,
    };
    use crate::proto::{FidRequest, SubscribeRequest};
    use crate::storage::db::{self, RocksDB, RocksDbTransactionBatch};
    use crate::storage::store::account::{HubEventIdGenerator, SEQUENCE_BITS};
    use crate::storage::store::engine::{Senders, ShardEngine};
    use crate::storage::store::stores::Stores;
    use crate::storage::store::test_helper::{commit_event, generate_signer, register_user};
    use crate::storage::store::{test_helper, BlockStore};
    use crate::storage::trie::merkle_trie;
    use crate::utils::factory::{events_factory, messages_factory};
    use crate::utils::statsd_wrapper::StatsdClientWrapper;
    use futures::future;
    use futures::StreamExt;
    use tempfile;
    use tokio::sync::{broadcast, mpsc};
    use tonic::Request;

    const SHARD1_FID: u64 = test_helper::SHARD1_FID;
    const SHARD2_FID: u64 = test_helper::SHARD2_FID;

    const USER_NAME: &str = "user";
    const PASSWORD: &str = "password";

    impl FidRequest {
        fn for_fid(fid: u64) -> Request<Self> {
            Request::new(FidRequest {
                fid,
                page_size: None,
                page_token: None,
                reverse: None,
            })
        }
    }

    struct MockL1Client {}

    #[async_trait]
    impl ChainAPI for MockL1Client {
        async fn resolve_ens_name(
            &self,
            name: String,
        ) -> Result<alloy_primitives::Address, EnsError> {
            let address_str = match name.as_str() {
                "username.eth" => "91031dcfdea024b4d51e775486111d2b2a715871",
                "username.base.eth" => "849151d7D0bF1F34b70d5caD5149D28CC2308bf1",
                _ => return Err(EnsError::ResolverNotFound(name)),
            };
            let addr = alloy_primitives::Address::from_slice(&hex::decode(address_str).unwrap());
            future::ready(Ok(addr)).await
        }

        async fn verify_contract_signature(
            &self,
            _claim: VerificationAddressClaim,
            _body: &VerificationAddAddressBody,
        ) -> Result<(), validations::error::ValidationError> {
            future::ready(Ok(())).await
        }
    }

    async fn subscribe_and_listen(
        service: &MyHubService,
        shard_id: u32,
        from_id: Option<u64>,
        num_events_expected: u64,
        event_types: Vec<i32>,
    ) -> tokio::task::JoinHandle<()> {
        let request = Request::new(SubscribeRequest {
            event_types,
            from_id,
            shard_index: Some(shard_id),
        });
        let mut listener = service.subscribe(request).await.unwrap();

        let mut num_events_seen = 0;

        return tokio::spawn(async move {
            loop {
                let event = timeout(Duration::from_millis(100), listener.get_mut().next()).await;
                if let Ok(Some(Ok(hub_event))) = event {
                    let block_number = hub_event.block_number;
                    assert!(block_number > 0);
                    assert!(hub_event.shard_index > 0);
                    num_events_seen += 1;
                    if num_events_seen == num_events_expected {
                        break;
                    }
                } else {
                    if num_events_seen == num_events_expected {
                        break;
                    }
                }
            }
            assert_eq!(num_events_seen, num_events_expected);
        });
    }

    async fn send_events(events_tx: broadcast::Sender<HubEvent>, num_events: u64) {
        for i in 0..num_events {
            events_tx
                .send(HubEvent {
                    r#type: HubEventType::MergeMessage as i32,
                    id: i,
                    body: None,
                    block_number: 1,
                    shard_index: 1,
                    timestamp: 0,
                })
                .unwrap();
        }
    }

    async fn write_events_to_db(db: Arc<RocksDB>, num_events: u64) {
        let mut txn = RocksDbTransactionBatch::new();
        for i in 0..num_events {
            HubEvent::put_event_transaction(
                &mut txn,
                &HubEvent {
                    r#type: HubEventType::MergeMessage as i32,
                    id: i,
                    body: None,
                    block_number: 1,
                    shard_index: 1,
                    timestamp: 0,
                },
            )
            .unwrap();
        }
        db.commit(txn).unwrap();
    }

    fn make_db(dir: &tempfile::TempDir, filename: &str) -> Arc<RocksDB> {
        let db_path = dir.path().join(filename);

        let db = Arc::new(db::RocksDB::new(db_path.to_str().unwrap()));
        db.open().unwrap();
        db
    }

    fn add_auth_header<T>(request: &mut Request<T>, username: &str, password: &str) {
        let auth = format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(format!("{}:{}", username, password))
        );
        request
            .metadata_mut()
            .insert("authorization", auth.parse().unwrap());
    }

    async fn submit_message(
        service: &MyHubService,
        message: proto::Message,
    ) -> Result<tonic::Response<proto::Message>, tonic::Status> {
        let mut request = Request::new(message);
        add_auth_header(&mut request, USER_NAME, PASSWORD);
        service.submit_message(request).await
    }

    async fn make_server(
        rpc_auth: Option<String>,
    ) -> (
        HashMap<u32, Stores>,
        HashMap<u32, Senders>,
        [ShardEngine; 2],
        MyHubService,
    ) {
        let statsd_client = StatsdClientWrapper::new(
            cadence::StatsdClient::builder("", cadence::NopMetricSink {}).build(),
            true,
        );

        let limits = test_helper::limits::test_store_limits();
        let (engine1, _) = test_helper::new_engine_with_options(test_helper::EngineOptions {
            limits: Some(limits.clone()),
            ..Default::default()
        });
        let (engine2, _) = test_helper::new_engine_with_options(test_helper::EngineOptions {
            limits: Some(limits.clone()),
            ..Default::default()
        });
        let db1 = engine1.db.clone();
        let db2 = engine2.db.clone();

        let (_msgs_request_tx, msgs_request_rx) = mpsc::channel(100);

        let shard1_stores = Stores::new(
            db1,
            1,
            merkle_trie::MerkleTrie::new(16).unwrap(),
            limits.clone(),
            test_helper::statsd_client(),
        );
        let shard1_senders = engine1.get_senders();

        let shard2_stores = Stores::new(
            db2,
            2,
            merkle_trie::MerkleTrie::new(16).unwrap(),
            limits.clone(),
            test_helper::statsd_client(),
        );
        let shard2_senders = engine2.get_senders();
        let stores = HashMap::from([(1, shard1_stores), (2, shard2_stores)]);
        let senders = HashMap::from([(1, shard1_senders), (2, shard2_senders)]);
        let num_shards = senders.len() as u32;

        let auth = rpc_auth.unwrap_or_else(|| format!("{}:{}", USER_NAME, PASSWORD));
        let blocks_dir = tempfile::TempDir::new().unwrap();
        let blocks_store = BlockStore::new(make_db(&blocks_dir, "blocks.db"));

        let message_router = Box::new(routing::EvenOddRouterForTest {});
        assert_eq!(message_router.route_fid(SHARD1_FID, 2), 1);
        assert_eq!(message_router.route_fid(SHARD2_FID, 2), 2);

        let (mempool_tx, mempool_rx) = mpsc::channel(1000);
        let (gossip_tx, _gossip_rx) = mpsc::channel(1000);
        let (_shard_decision_tx, shard_decision_rx) = broadcast::channel(1000);
        let mut mempool = Mempool::new(
            mempool::Config::default(),
            engine1.network,
            mempool_rx,
            msgs_request_rx,
            num_shards,
            stores.clone(),
            gossip_tx,
            shard_decision_rx,
            statsd_client.clone(),
        );
        tokio::spawn(async move { mempool.run().await });

        let mut chain_clients = ChainClients {
            chain_api_map: HashMap::new(),
        };
        chain_clients.chain_api_map.insert(
            Chain::EthMainnet,
            Box::new(MockL1Client {}) as Box<dyn ChainAPI>,
        );
        chain_clients.chain_api_map.insert(
            Chain::BaseMainnet,
            Box::new(MockL1Client {}) as Box<dyn ChainAPI>,
        );
        (
            stores.clone(),
            senders.clone(),
            [engine1, engine2],
            MyHubService::new(
                auth,
                blocks_store,
                stores,
                senders,
                statsd_client,
                num_shards,
                proto::FarcasterNetwork::Devnet,
                message_router,
                mempool_tx.clone(),
                chain_clients,
                "0.1.2".to_string(),
                "asddef".to_string(),
            ),
        )
    }

    #[tokio::test]
    async fn test_subscribe_rpc() {
        let (stores, senders, _, service) = make_server(None).await;

        let num_shard1_pre_existing_events = 10;
        let num_shard2_pre_existing_events = 20;

        write_events_to_db(
            stores.get(&1u32).unwrap().shard_store.db.clone(),
            num_shard1_pre_existing_events,
        )
        .await;
        write_events_to_db(
            stores.get(&2u32).unwrap().shard_store.db.clone(),
            num_shard2_pre_existing_events,
        )
        .await;

        let num_shard1_events = 5;
        let num_shard2_events = 10;
        let shard1_subscriber = subscribe_and_listen(
            &service,
            1,
            Some(0),
            num_shard1_events + num_shard1_pre_existing_events,
            vec![HubEventType::MergeMessage as i32],
        )
        .await;
        let shard2_subscriber = subscribe_and_listen(
            &service,
            2,
            Some(0),
            num_shard2_events + num_shard2_pre_existing_events,
            vec![HubEventType::MergeMessage as i32],
        )
        .await;

        // Allow time for rpc handler to subscribe to event rx channels
        tokio::time::sleep(Duration::from_secs(1)).await;

        send_events(
            senders.get(&1u32).unwrap().events_tx.clone(),
            num_shard1_events,
        )
        .await;
        send_events(
            senders.get(&2u32).unwrap().events_tx.clone(),
            num_shard2_events,
        )
        .await;

        let _ = shard1_subscriber.await;
        let _ = shard2_subscriber.await;
    }

    #[tokio::test]
    async fn test_subscribe_with_filter_rpc() {
        let (stores, senders, _, service) = make_server(None).await;

        let num_shard1_pre_existing_events = 10;
        let num_shard2_pre_existing_events = 20;

        write_events_to_db(
            stores.get(&1u32).unwrap().shard_store.db.clone(),
            num_shard1_pre_existing_events,
        )
        .await;
        write_events_to_db(
            stores.get(&2u32).unwrap().shard_store.db.clone(),
            num_shard2_pre_existing_events,
        )
        .await;

        let num_shard1_events = 5;
        let shard1_subscriber = subscribe_and_listen(
            &service,
            1,
            Some(0),
            num_shard1_events + num_shard1_pre_existing_events,
            vec![HubEventType::MergeMessage as i32],
        )
        .await;
        let shard2_subscriber = subscribe_and_listen(
            &service,
            2,
            Some(0),
            0,
            vec![HubEventType::PruneMessage as i32],
        )
        .await;

        // Allow time for rpc handler to subscribe to event rx channels
        tokio::time::sleep(Duration::from_secs(1)).await;

        send_events(
            senders.get(&1u32).unwrap().events_tx.clone(),
            num_shard1_events,
        )
        .await;
        send_events(senders.get(&2u32).unwrap().events_tx.clone(), 0).await;

        let _ = shard1_subscriber.await;
        let _ = shard2_subscriber.await;
    }

    #[tokio::test]
    async fn test_get_event_success() {
        let (stores, _, _, service) = make_server(None).await;
        let event_id = 12345;
        let hub_event = HubEvent {
            r#type: HubEventType::MergeMessage as i32,
            id: event_id,
            body: None,
            block_number: 0,
            shard_index: 0,
            timestamp: 0,
        };

        let db = stores.get(&1u32).unwrap().shard_store.db.clone();
        let mut txn = RocksDbTransactionBatch::new();
        HubEvent::put_event_transaction(&mut txn, &hub_event).unwrap();
        db.commit(txn).unwrap();

        let request = Request::new(proto::EventRequest {
            id: event_id,
            shard_index: 1,
        });
        let response = service.get_event(request).await.unwrap();

        let hub_event_response = response.into_inner();
        assert_eq!(hub_event_response.block_number, event_id >> SEQUENCE_BITS);
        assert_eq!(hub_event_response.shard_index, 1);
        assert_eq!(hub_event_response.r#type, hub_event.r#type);
        assert_eq!(hub_event_response.id, event_id);
    }

    #[tokio::test]
    async fn test_get_event_not_found() {
        let (stores, _, _, service) = make_server(None).await;

        write_events_to_db(stores.get(&1u32).unwrap().shard_store.db.clone(), 1).await;

        let request = Request::new(proto::EventRequest {
            id: 99999, // Junk event ID
            shard_index: 1,
        });
        let response = service.get_event(request).await;

        assert!(response.is_err());
        let status = response.unwrap_err();
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), "not_found/Event not found");
    }

    #[tokio::test]
    async fn test_get_event_invalid_shard() {
        let (stores, _, _, service) = make_server(None).await;

        let event_id = 12345;
        write_events_to_db(stores.get(&1u32).unwrap().shard_store.db.clone(), 1).await;

        let request = Request::new(proto::EventRequest {
            id: event_id,
            shard_index: 999, // junk shard
        });
        let response = service.get_event(request).await;

        // Validate the response
        assert!(response.is_err());
        let status = response.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(status.message(), "no shard store for fid");
    }

    #[tokio::test]
    async fn test_get_event_missing_shard_index() {
        let (stores, _, _, service) = make_server(None).await;

        let event_id = 12345;
        write_events_to_db(stores.get(&1u32).unwrap().shard_store.db.clone(), 1).await;

        let request = Request::new(proto::EventRequest {
            id: event_id,
            shard_index: 0,
        });
        let response = service.get_event(request).await;

        assert!(response.is_err());
        let status = response.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(status.message(), "no shard store for fid");
    }

    #[tokio::test]
    async fn test_get_events() {
        let (stores, _, _, service) = make_server(None).await;

        // Write some test events to the DB
        write_events_to_db(stores.get(&1u32).unwrap().shard_store.db.clone(), 10).await;

        // Test getting first page
        let request = Request::new(proto::EventsRequest {
            start_id: 0,
            shard_index: None,
            stop_id: None,
            page_size: Some(3),
            page_token: None,
            reverse: None,
        });
        let response = service.get_events(request).await.unwrap();
        let events = response.get_ref().events.clone();
        assert_eq!(events.len(), 3);
        let next_page_token = response.get_ref().next_page_token.clone();

        // Test getting second page
        let request = Request::new(proto::EventsRequest {
            start_id: 0,
            shard_index: None,
            stop_id: None,
            page_size: Some(3),
            page_token: next_page_token,
            reverse: None,
        });
        let response = service.get_events(request).await.unwrap();
        let events = response.get_ref().events.clone();
        assert_eq!(events.len(), 3);

        // Test getting from only one shard
        let request = Request::new(proto::EventsRequest {
            start_id: 0,
            shard_index: Some(2),
            stop_id: None,
            page_size: Some(3),
            page_token: None,
            reverse: None,
        });
        let response = service.get_events(request).await.unwrap();
        let events = response.get_ref().events.clone();
        assert_eq!(events.len(), 0); // No events in shard 2

        // Test with start_id and stop_id with reverse pagination, on shard 1
        let request = Request::new(proto::EventsRequest {
            start_id: 2,
            shard_index: Some(1),
            stop_id: Some(8),
            page_size: Some(7),
            page_token: None,
            reverse: Some(true),
        });
        let response = service.get_events(request).await.unwrap();
        let events = response.get_ref().events.clone();
        assert_eq!(events.len(), 6);
        assert_eq!(events[0].id, 7);
        assert_eq!(events[events.len() - 1].id, 2);
        assert!(events[0].shard_index > 0);
    }

    #[tokio::test]
    async fn test_submit_message_fails_with_error_for_invalid_messages() {
        let (_stores, _senders, [mut engine1, _], service) = make_server(None).await;

        // Message with no fid registration
        let invalid_message = messages_factory::casts::create_cast_add(123, "test", None, None);

        let response = submit_message(&service, invalid_message).await.unwrap_err();

        assert_eq!(response.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            response.message(),
            "bad_request.validation_failure/unknown fid"
        );
        assert_eq!(
            response
                .metadata()
                .get("x-err-code")
                .unwrap()
                .to_str()
                .unwrap(),
            "bad_request.validation_failure"
        );

        register_user(
            SHARD1_FID,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine1,
        )
        .await;
        let valid_message =
            messages_factory::casts::create_cast_add(SHARD1_FID, "test", None, None);
        test_helper::commit_message(&mut engine1, &valid_message).await;

        // Submitting a duplicate message should return an error
        let response = submit_message(&service, valid_message).await.unwrap_err();
        assert_eq!(response.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            response.message(),
            "bad_request.duplicate/message has already been merged"
        );
        assert_eq!(
            response
                .metadata()
                .get("x-err-code")
                .unwrap()
                .to_str()
                .unwrap(),
            "bad_request.duplicate"
        );
    }

    #[tokio::test]
    async fn test_authentication() {
        let (_stores, _senders, _, service) =
            make_server(Some("user1:pass1,user2:pass2".to_string())).await;
        let message = messages_factory::casts::create_cast_add(123, "test", None, None);

        let no_auth_request = Request::new(message.clone());
        // Providing no auth fails
        let response = service.submit_message(no_auth_request).await.unwrap_err();
        assert_eq!(response.code(), tonic::Code::Unauthenticated);
        assert_eq!(response.message(), "missing authorization header");

        let mut invalid_creds_request = Request::new(message.clone());
        add_auth_header(&mut invalid_creds_request, "user3", "pass1");
        let response = service
            .submit_message(invalid_creds_request)
            .await
            .unwrap_err();
        assert_eq!(response.code(), tonic::Code::Unauthenticated);
        assert_eq!(response.message(), "invalid username or password");

        let mut valid_creds_request = Request::new(message.clone());
        add_auth_header(&mut valid_creds_request, "user2", "pass2");
        let response = service
            .submit_message(valid_creds_request)
            .await
            .unwrap_err();
        // Authenticated but no fid registration
        assert_eq!(response.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            response.message(),
            "bad_request.validation_failure/unknown fid"
        );
    }

    #[tokio::test]
    async fn test_event_timestamp() {
        let (_stores, _senders, [mut engine1, _], service) = make_server(None).await;

        test_helper::register_user(
            SHARD1_FID,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine1,
        )
        .await;
        let cast_add = messages_factory::casts::create_cast_add(SHARD1_FID, "test", None, None);
        let cast_add2 = messages_factory::casts::create_cast_add(SHARD1_FID, "test2", None, None);
        let mut shard_chunks = vec![];

        shard_chunks
            .push(test_helper::commit_messages(&mut engine1, vec![cast_add, cast_add2]).await);

        sleep(Duration::from_secs(1)).await;

        let cast_add3 = messages_factory::casts::create_cast_add(SHARD1_FID, "test3", None, None);

        shard_chunks.push(test_helper::commit_message(&mut engine1, &cast_add3).await);

        let request = Request::new(SubscribeRequest {
            event_types: vec![HubEventType::MergeMessage as i32],
            from_id: Some(0),
            shard_index: Some(1),
        });
        let mut listener = service.subscribe(request).await.unwrap();

        let mut events = vec![];
        let start_time = Instant::now();
        loop {
            if start_time.elapsed() > Duration::from_secs(2) {
                break;
            }

            let event = timeout(Duration::from_millis(100), listener.get_mut().next()).await;
            if let Ok(Some(Ok(hub_event))) = event {
                assert_ne!(hub_event.timestamp, 0);
                events.push(hub_event);
            }
        }

        let cast_add4 = messages_factory::casts::create_cast_add(SHARD1_FID, "test4", None, None);

        shard_chunks.push(test_helper::commit_message(&mut engine1, &cast_add4).await);

        let event = timeout(Duration::from_millis(100), listener.get_mut().next()).await;
        if let Ok(Some(Ok(hub_event))) = event {
            assert_ne!(hub_event.timestamp, 0);
            events.push(hub_event);
        }

        let assert_events = |events: &Vec<HubEvent>, shard_chunks: &Vec<ShardChunk>| {
            assert_eq!(events.len(), 4);
            assert_eq!(shard_chunks.len(), 3);
            assert_eq!(
                events[0].timestamp,
                shard_chunks[0].header.as_ref().unwrap().timestamp
            );
            assert_eq!(
                events[1].timestamp,
                shard_chunks[0].header.as_ref().unwrap().timestamp
            );
            assert_eq!(
                events[2].timestamp,
                shard_chunks[1].header.as_ref().unwrap().timestamp
            );
            assert_eq!(
                events[3].timestamp,
                shard_chunks[2].header.as_ref().unwrap().timestamp
            );
        };
        assert_events(&events, &shard_chunks);

        let req = Request::new(EventsRequest {
            start_id: events[0].id,
            stop_id: None,
            shard_index: Some(1),
            page_size: None,
            page_token: None,
            reverse: None,
        });
        let res = service.get_events(req).await.unwrap();
        let inner_res = res.into_inner();
        let filtered_events: Vec<HubEvent> = inner_res
            .events
            .into_iter()
            .filter(|event| event.r#type == HubEventType::MergeMessage as i32)
            .collect();
        assert_eq!(filtered_events.len(), 4);
        assert_events(&filtered_events, &shard_chunks);

        let req = Request::new(EventRequest {
            shard_index: 1,
            id: HubEventIdGenerator::make_event_id_for_block_number(
                shard_chunks[2]
                    .header
                    .as_ref()
                    .unwrap()
                    .height
                    .unwrap()
                    .block_number,
            ),
        });
        let res = service.get_event(req).await.unwrap();
        assert_eq!(
            res.into_inner().timestamp,
            shard_chunks[2].header.as_ref().unwrap().timestamp
        );
    }

    #[tokio::test]
    async fn test_good_ens_proof() {
        let (_stores, _senders, [mut engine1, mut _engine2], service) = make_server(None).await;
        let signer = test_helper::default_signer();
        let owner = hex::decode("91031dcfdea024b4d51e775486111d2b2a715871").unwrap();
        let fid = SHARD1_FID;

        test_helper::register_user(fid, signer.clone(), owner.clone(), &mut engine1).await;

        let username_proof = UserNameProof {
            timestamp: messages_factory::farcaster_time() as u64,
            name: b"username.eth".to_vec(),
            owner,
            signature: "signature".to_string().encode_to_vec(),
            fid,
            r#type: UserNameType::UsernameTypeEnsL1 as i32,
        };

        let result = service
            .validate_ens_username_proof(fid, &username_proof)
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_basename_proof() {
        let (_stores, _senders, [mut engine1, mut _engine2], service) = make_server(None).await;
        let signer = test_helper::default_signer();
        let owner = hex::decode("849151d7D0bF1F34b70d5caD5149D28CC2308bf1").unwrap();
        let fid = SHARD1_FID;

        test_helper::register_user(fid, signer.clone(), owner.clone(), &mut engine1).await;

        let username_proof = UserNameProof {
            timestamp: messages_factory::farcaster_time() as u64,
            name: b"username.base.eth".to_vec(),
            owner,
            signature: "signature".to_string().encode_to_vec(),
            fid,
            r#type: UserNameType::UsernameTypeBasename as i32,
        };

        let result = service
            .validate_ens_username_proof(fid, &username_proof)
            .await;
        assert!(result.is_ok());

        let user_data_add = messages_factory::user_data::create_user_data_add(
            fid,
            UserDataType::Username,
            &"username.base.eth".to_string(),
            None,
            None,
        );

        // User data add fails because the proof is not committed yet
        let result = submit_message(&service, user_data_add.clone()).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);

        let proof_message =
            messages_factory::username_proof::create_from_proof(&username_proof, None);
        test_helper::commit_message(&mut engine1, &proof_message).await;

        // Now the user data add should succeed
        let result = submit_message(&service, user_data_add).await;

        let error = result.unwrap_err();
        // Ensure that it's not a validation error, if it got as far as adding to the mempool, validation passed
        // TODO: We should fix the test setup so adding to mempool does not fail
        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert_eq!(error.message(), "unavailable/Error adding to mempool");
    }

    #[tokio::test]
    async fn test_ens_proof_with_bad_owner() {
        let (_stores, _senders, [mut engine1, mut _engine2], service) = make_server(None).await;
        let signer = test_helper::default_signer();
        let owner = test_helper::default_custody_address();
        let fid = SHARD1_FID;

        test_helper::register_user(fid, signer.clone(), owner.clone(), &mut engine1).await;

        let username_proof = UserNameProof {
            timestamp: messages_factory::farcaster_time() as u64,
            name: b"username.eth".to_vec(),
            owner: "100000000000000000".to_string().encode_to_vec(),
            signature: "signature".to_string().encode_to_vec(),
            fid,
            r#type: UserNameType::UsernameTypeEnsL1 as i32,
        };

        // Proof owner does not match owner of ens name
        let result = service
            .validate_ens_username_proof(fid, &username_proof)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_ens_proof_with_bad_custody_address() {
        let (_stores, _senders, [mut engine1, mut _engine2], service) = make_server(None).await;
        let signer = test_helper::default_signer();
        let owner = test_helper::default_custody_address();
        let fid = SHARD1_FID;

        test_helper::register_user(
            fid,
            signer.clone(),
            "100000000000000000".to_string().encode_to_vec(),
            &mut engine1,
        )
        .await;

        let username_proof = UserNameProof {
            timestamp: messages_factory::farcaster_time() as u64,
            name: b"username.eth".to_vec(),
            owner,
            signature: "signature".to_string().encode_to_vec(),
            fid,
            r#type: UserNameType::UsernameTypeEnsL1 as i32,
        };

        let result = service
            .validate_ens_username_proof(fid, &username_proof)
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_ens_proof_with_verified_address() {
        let (_stores, _senders, [mut _engine1, mut engine2], service) = make_server(None).await;
        let signer = test_helper::default_signer();
        let fid = 2;
        let owner = test_helper::default_custody_address();
        let signature = "signature".to_string();

        test_helper::register_user(fid, signer.clone(), owner.clone(), &mut engine2).await;

        let verification_add = messages_factory::verifications::create_verification_add(
            fid,
            0,
            hex::decode("91031dcfdea024b4d51e775486111d2b2a715871").unwrap(),
            hex::decode("b72c63d61f075b36fb66a9a867b50836cef19d653a3c09005628738677bcb25f25b6b6e6d2e1d69cd725327b3c020deef9e2575a22dc8ed08f88bc75718ce1cb1c").unwrap(),
            hex::decode("d74860c4bbf574d5ad60f03a478a30f990e05ac723e138a5c860cdb3095f4296").unwrap(),
            None,
            None,
        );

        test_helper::commit_message(&mut engine2, &verification_add).await;

        let username_proof = UserNameProof {
            timestamp: messages_factory::farcaster_time() as u64,
            name: b"username.eth".to_vec(),
            owner: hex::decode("91031dcfdea024b4d51e775486111d2b2a715871").unwrap(),
            signature: signature.encode_to_vec(),
            fid,
            r#type: UserNameType::UsernameTypeEnsL1 as i32,
        };

        let result = service
            .validate_ens_username_proof(fid, &username_proof)
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_cast_apis() {
        let (_, _, [mut engine1, mut engine2], service) = make_server(None).await;
        let engine1 = &mut engine1;
        let engine2 = &mut engine2;
        test_helper::register_user(
            SHARD1_FID,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            engine1,
        )
        .await;
        test_helper::register_user(
            SHARD2_FID,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            engine2,
        )
        .await;
        let cast_add = messages_factory::casts::create_cast_add(SHARD1_FID, "test", None, None);
        let cast_add2 = messages_factory::casts::create_cast_add(SHARD1_FID, "test2", None, None);
        let cast_remove = messages_factory::casts::create_cast_remove(
            SHARD1_FID,
            &cast_add.hash,
            Some(cast_add.data.as_ref().unwrap().timestamp + 10),
            None,
        );

        let another_shard_cast =
            messages_factory::casts::create_cast_add(SHARD2_FID, "another fid", None, None);

        test_helper::commit_message(engine1, &cast_add).await;
        test_helper::commit_message(engine1, &cast_add2).await;
        test_helper::commit_message(engine1, &cast_remove).await;
        test_helper::commit_message(engine2, &another_shard_cast).await;

        let response = service
            .get_cast(Request::new(proto::CastId {
                fid: SHARD1_FID,
                hash: cast_add2.hash.clone(),
            }))
            .await
            .unwrap();
        assert_eq!(response.get_ref().hash, cast_add2.hash);

        // Fetching a removed cast fails
        let response = service
            .get_cast(Request::new(proto::CastId {
                fid: SHARD1_FID,
                hash: cast_add.hash.clone(),
            }))
            .await
            .unwrap_err();
        assert_eq!(response.code(), tonic::Code::NotFound);

        // Fetching across shards works
        let response = service
            .get_cast(Request::new(proto::CastId {
                fid: SHARD2_FID,
                hash: another_shard_cast.hash.clone(),
            }))
            .await
            .unwrap();
        assert_eq!(response.get_ref().hash, another_shard_cast.hash);

        // Fetching on the wrong shard fails
        let response = service
            .get_cast(Request::new(proto::CastId {
                fid: SHARD1_FID,
                hash: another_shard_cast.hash.clone(),
            }))
            .await
            .unwrap_err();
        assert_eq!(response.code(), tonic::Code::NotFound);

        // Returns all active casts
        let all_casts_request = proto::FidRequest {
            fid: SHARD1_FID,
            page_size: None,
            page_token: None,
            reverse: None,
        };
        let response = service
            .get_casts_by_fid(Request::new(all_casts_request))
            .await;
        test_helper::assert_contains_all_messages(&response, &[&cast_add2]);

        // Pagination works
        let all_casts_request = proto::FidTimestampRequest {
            fid: SHARD1_FID,
            page_size: Some(1),
            page_token: None,
            reverse: None,
            start_timestamp: None,
            stop_timestamp: None,
        };
        let response = service
            .get_all_cast_messages_by_fid(Request::new(all_casts_request))
            .await;
        test_helper::assert_contains_all_messages(&response, &[&cast_add2]);

        let second_page_request = proto::FidTimestampRequest {
            fid: SHARD1_FID,
            page_size: Some(1),
            page_token: response.as_ref().unwrap().get_ref().next_page_token.clone(),
            reverse: None,
            start_timestamp: None,
            stop_timestamp: None,
        };
        let response = service
            .get_all_cast_messages_by_fid(Request::new(second_page_request))
            .await;
        test_helper::assert_contains_all_messages(&response, &[&cast_remove]);

        let reverse_request = proto::FidTimestampRequest {
            fid: SHARD1_FID,
            page_size: Some(1),
            page_token: None,
            reverse: Some(true),
            start_timestamp: None,
            stop_timestamp: None,
        };
        let response = service
            .get_all_cast_messages_by_fid(Request::new(reverse_request))
            .await;
        test_helper::assert_contains_all_messages(&response, &[&cast_remove]);

        // Returns all casts
        let bulk_casts_request = proto::FidTimestampRequest {
            fid: SHARD1_FID,
            page_size: None,
            page_token: None,
            reverse: None,
            start_timestamp: None,
            stop_timestamp: None,
        };
        let response = service
            .get_all_cast_messages_by_fid(Request::new(bulk_casts_request))
            .await;
        test_helper::assert_contains_all_messages(&response, &[&cast_add2, &cast_remove]);

        // Returns casts even if page token is empty
        let empty_page_token_request = proto::FidTimestampRequest {
            fid: SHARD1_FID,
            page_size: None,
            page_token: Some(vec![]),
            reverse: None,
            start_timestamp: None,
            stop_timestamp: None,
        };
        let response = service
            .get_all_cast_messages_by_fid(Request::new(empty_page_token_request))
            .await;
        test_helper::assert_contains_all_messages(&response, &[&cast_add2, &cast_remove]);
    }

    #[tokio::test]
    async fn test_get_casts_by_parent_hash() {
        let (_, _, [mut engine1, mut engine2], service) = make_server(None).await;
        let engine1 = &mut engine1;
        let engine2 = &mut engine2;
        test_helper::register_user(
            SHARD1_FID,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            engine1,
        )
        .await;
        test_helper::register_user(
            SHARD2_FID,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            engine2,
        )
        .await;
        let original_cast =
            messages_factory::casts::create_cast_add(SHARD1_FID, "test", None, None);
        let timestamp = original_cast.data.as_ref().unwrap().timestamp;
        let reply_1 = messages_factory::casts::create_cast_with_parent(
            SHARD1_FID,
            "reply 1",
            SHARD1_FID,
            &original_cast.hash,
            Some(timestamp + 1),
            None,
        );
        let reply_2 = messages_factory::casts::create_cast_with_parent(
            SHARD1_FID,
            "reply 2",
            SHARD1_FID,
            &original_cast.hash,
            Some(timestamp + 2),
            None,
        );
        let reply_3_another_shard = messages_factory::casts::create_cast_with_parent(
            SHARD2_FID,
            "reply 3",
            SHARD1_FID,
            &original_cast.hash,
            Some(timestamp + 3),
            None,
        );
        let reply_4_another_shard = messages_factory::casts::create_cast_with_parent(
            SHARD2_FID,
            "reply 4",
            SHARD1_FID,
            &original_cast.hash,
            Some(timestamp + 4),
            None,
        );

        test_helper::commit_message(engine1, &original_cast).await;
        test_helper::commit_message(engine1, &reply_1).await;
        test_helper::commit_message(engine1, &reply_2).await;
        test_helper::commit_message(engine2, &reply_3_another_shard).await;
        test_helper::commit_message(engine2, &reply_4_another_shard).await;

        let response = service
            .get_casts_by_parent(Request::new(proto::CastsByParentRequest {
                parent: Some(proto::casts_by_parent_request::Parent::ParentCastId(
                    proto::CastId {
                        fid: SHARD1_FID,
                        hash: original_cast.hash.clone(),
                    },
                )),
                page_size: Some(1),
                page_token: None,
                reverse: None,
            }))
            .await
            .unwrap();
        test_helper::assert_contains_all_messages(&response, &[&reply_1, &reply_3_another_shard]);

        let page_token = response.get_ref().next_page_token.clone();
        let response = service
            .get_casts_by_parent(Request::new(proto::CastsByParentRequest {
                parent: Some(proto::casts_by_parent_request::Parent::ParentCastId(
                    proto::CastId {
                        fid: SHARD1_FID,
                        hash: original_cast.hash.clone(),
                    },
                )),
                page_size: Some(2),
                page_token: page_token,
                reverse: None,
            }))
            .await
            .unwrap();
        test_helper::assert_contains_all_messages(&response, &[&reply_2, &reply_4_another_shard]);

        // Test reverse pagination
        let response = service
            .get_casts_by_parent(Request::new(proto::CastsByParentRequest {
                parent: Some(proto::casts_by_parent_request::Parent::ParentCastId(
                    proto::CastId {
                        fid: SHARD1_FID,
                        hash: original_cast.hash.clone(),
                    },
                )),
                page_size: Some(1),
                page_token: None,
                reverse: Some(true),
            }))
            .await
            .unwrap();

        let page_token = response.get_ref().next_page_token.clone();
        let response = service
            .get_casts_by_parent(Request::new(proto::CastsByParentRequest {
                parent: Some(proto::casts_by_parent_request::Parent::ParentCastId(
                    proto::CastId {
                        fid: SHARD1_FID,
                        hash: original_cast.hash.clone(),
                    },
                )),
                page_size: Some(2),
                page_token: page_token.clone(),
                reverse: Some(true),
            }))
            .await
            .unwrap();
        test_helper::assert_contains_all_messages(&response, &[&reply_1, &reply_3_another_shard]);
    }

    #[tokio::test]
    async fn test_storage_limits() {
        // Works with no storage
        let (_, _, [mut engine1, _], service) = make_server(None).await;

        let response = service
            .get_current_storage_limits_by_fid(FidRequest::for_fid(SHARD1_FID))
            .await
            .unwrap();
        assert_eq!(response.get_ref().units, 0);
        assert_eq!(response.get_ref().limits.len(), 6);
        for limit in response.get_ref().limits.iter() {
            assert_eq!(limit.limit, 0);
            assert_eq!(limit.used, 0);
        }
        assert_eq!(response.get_ref().unit_details.len(), 2);
        assert_eq!(response.get_ref().unit_details[0].unit_size, 0);
        assert_eq!(response.get_ref().unit_details[1].unit_size, 0);

        test_helper::register_user(
            SHARD1_FID,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine1,
        )
        .await;
        // register_user will give the user a single unit of storage, let add one more legacy unit and a 2024 unit for a total of 1 legacy and 2 2024 units
        test_helper::commit_event(
            &mut engine1,
            &events_factory::create_rent_event(SHARD1_FID, Some(1), None, false),
        )
        .await;
        test_helper::commit_event(
            &mut engine1,
            &events_factory::create_rent_event(SHARD1_FID, None, Some(1), false),
        )
        .await;
        let cast_add = &messages_factory::casts::create_cast_add(SHARD1_FID, "test", None, None);
        test_helper::commit_message(&mut engine1, cast_add).await;
        test_helper::commit_message(
            &mut engine1,
            &messages_factory::casts::create_cast_add(SHARD1_FID, "test2", None, None),
        )
        .await;
        test_helper::commit_message(
            &mut engine1,
            &messages_factory::casts::create_cast_remove(
                SHARD1_FID,
                &cast_add.hash,
                Some(cast_add.data.as_ref().unwrap().timestamp + 10),
                None,
            ),
        )
        .await;
        test_helper::commit_message(
            &mut engine1,
            &messages_factory::links::create_link_add(SHARD1_FID, "follow", SHARD2_FID, None, None),
        )
        .await;

        let response = service
            .get_current_storage_limits_by_fid(FidRequest::for_fid(SHARD1_FID))
            .await
            .unwrap();
        assert_eq!(response.get_ref().units, 3);
        assert_eq!(response.get_ref().unit_details.len(), 2);
        assert_eq!(response.get_ref().unit_details[0].unit_size, 1);
        assert_eq!(
            response.get_ref().unit_details[0].unit_type,
            proto::StorageUnitType::UnitTypeLegacy as i32
        );
        assert_eq!(
            response.get_ref().unit_details[1].unit_type,
            proto::StorageUnitType::UnitType2024 as i32
        );
        assert_eq!(response.get_ref().unit_details[1].unit_size, 2);

        let casts_limit = response
            .get_ref()
            .limits
            .iter()
            .filter(|limit| limit.store_type() == proto::StoreType::Casts)
            .collect::<Vec<_>>()[0];
        let configured_limits = engine1.get_stores().store_limits;
        assert_eq!(
            casts_limit.limit as u32,
            (configured_limits.limits.casts * 2) + (configured_limits.legacy_limits.casts)
        );
        assert_eq!(casts_limit.used, 2); // Cast remove counts as 1
        assert_eq!(casts_limit.name, "CASTS");

        let links_limit = response
            .get_ref()
            .limits
            .iter()
            .filter(|limit| limit.store_type() == proto::StoreType::Links)
            .collect::<Vec<_>>()[0];
        assert_eq!(links_limit.used, 1);
    }

    #[tokio::test]
    async fn test_get_info() {
        let (_, _, [mut engine1, _], service) = make_server(None).await;

        test_helper::register_user(
            SHARD1_FID,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine1,
        )
        .await;
        test_helper::commit_message(
            &mut engine1,
            &messages_factory::casts::create_cast_add(SHARD1_FID, "test", None, None),
        )
        .await;

        let response = service
            .get_info(Request::new(proto::GetInfoRequest {}))
            .await
            .unwrap();
        let info = response.get_ref();
        assert_eq!(info.num_shards, 2);
        assert_eq!(info.shard_infos.len(), 3); // +1 for the block shard
        assert_eq!(info.peer_id, "asddef");
        assert_eq!(info.version, "0.1.2");

        let block_info = info
            .shard_infos
            .iter()
            .find(|info| info.shard_id == 0)
            .unwrap();
        assert_eq!(block_info.shard_id, 0);
        assert_eq!(block_info.num_fid_registrations, 0);
        assert_eq!(block_info.num_messages, 0);
        assert_eq!(block_info.max_height, 0);
        assert_eq!(block_info.mempool_size, 0);

        let shard1_info = info
            .shard_infos
            .iter()
            .find(|info| info.shard_id == 1)
            .unwrap();
        assert_eq!(shard1_info.shard_id, 1);
        assert_eq!(shard1_info.num_fid_registrations, 1);
        assert_eq!(shard1_info.num_messages, 4); // 3 onchain events for registration + 1 cast add
        assert_eq!(shard1_info.max_height, 4); // Each message above was commited in a separate block
        assert_eq!(block_info.mempool_size, 0);

        let shard2_info = info
            .shard_infos
            .iter()
            .find(|info| info.shard_id == 2)
            .unwrap();
        assert_eq!(shard2_info.shard_id, 2);
        assert_eq!(shard2_info.num_fid_registrations, 0);
        assert_eq!(shard2_info.num_messages, 0);
        assert_eq!(shard2_info.max_height, 0);
        assert_eq!(block_info.mempool_size, 0);
    }

    #[tokio::test]
    async fn test_get_username_proof_ens() {
        let (_, _, [mut engine1, _], service) = make_server(None).await;
        let fid = SHARD1_FID;
        let signer = test_helper::default_signer();
        let owner = hex::decode("91031dcfdea024b4d51e775486111d2b2a715871").unwrap();

        // Register the user
        test_helper::register_user(fid, signer.clone(), owner.clone(), &mut engine1).await;

        // Create an ENS username proof
        let ens_username = "test.eth";

        // Create a username proof message and store it
        let proof_message = messages_factory::username_proof::create_username_proof(
            fid,
            UserNameType::UsernameTypeEnsL1,
            ens_username.to_string(),
            owner.clone(),
            "signature".to_string(),
            messages_factory::farcaster_time() as u64,
            None,
        );

        // Commit the message to engine1
        test_helper::commit_message(&mut engine1, &proof_message).await;

        // Test get_username_proof for ENS name
        let request = Request::new(UsernameProofRequest {
            name: ens_username.as_bytes().to_vec(),
        });

        let response = service.get_username_proof(request).await;
        assert!(
            response.is_ok(),
            "Failed to get ENS username proof: {:?}",
            response.err()
        );

        let proof = response.unwrap().into_inner();
        assert_eq!(proof.fid, fid);
        assert_eq!(proof.name, ens_username.as_bytes().to_vec());
        assert_eq!(proof.r#type, UserNameType::UsernameTypeEnsL1 as i32);
    }

    #[tokio::test]
    async fn test_get_fids() {
        let (_, _, [mut engine1, mut engine2], service) = make_server(None).await;

        test_helper::register_user(
            SHARD1_FID,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine1,
        )
        .await;
        test_helper::register_user(
            SHARD1_FID + 2, // another fid for shard 1
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine1,
        )
        .await;
        test_helper::register_user(
            SHARD2_FID,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine2,
        )
        .await;

        let shard1_response = service
            .get_fids(Request::new(proto::FidsRequest {
                shard_id: 1,
                page_size: Some(1),
                page_token: None,
                reverse: None,
            }))
            .await
            .unwrap();
        let res = shard1_response.into_inner();
        assert_eq!(res.fids, vec![SHARD1_FID]);
        assert!(res.next_page_token.is_some());

        let shard1_response = service
            .get_fids(Request::new(proto::FidsRequest {
                shard_id: 1,
                page_size: None,
                page_token: res.next_page_token.clone(),
                reverse: None,
            }))
            .await
            .unwrap();
        assert_eq!(shard1_response.into_inner().fids, vec![SHARD1_FID + 2]);

        let shard2_response = service
            .get_fids(Request::new(proto::FidsRequest {
                shard_id: 2,
                page_size: None,
                page_token: None,
                reverse: None,
            }))
            .await
            .unwrap();
        let shard2_ref = shard2_response.get_ref();
        let shard2_fids = &shard2_ref.fids;
        assert_eq!(*shard2_fids, vec![SHARD2_FID]);
        assert_eq!(shard2_ref.next_page_token, None);
    }

    #[tokio::test]
    async fn test_get_id_registry_event_by_address() {
        let (_stores, _senders, [mut engine1, _], service) = make_server(None).await;
        let owner = test_helper::default_custody_address();
        let fid = SHARD1_FID;
        // Should we write a bunch of users to test the iteration or is this sufficient?
        test_helper::register_user(
            fid,
            test_helper::default_signer(),
            owner.clone(),
            &mut engine1,
        )
        .await;

        let request = Request::new(proto::IdRegistryEventByAddressRequest {
            address: owner.clone(),
        });
        let response = service
            .get_id_registry_on_chain_event_by_address(request)
            .await
            .unwrap();
        let event = response.into_inner();
        if let Some(proto::on_chain_event::Body::IdRegisterEventBody(body)) = event.body {
            assert_eq!(body.to, owner);
        } else {
            panic!("Expected IdRegisterEventBody");
        }
    }

    #[tokio::test]
    async fn test_get_fid_address_type() {
        let (_stores, _senders, [mut engine1, _], service) = make_server(None).await;
        let fid = SHARD1_FID;
        let signer = test_helper::default_signer();
        let custody_address = test_helper::default_custody_address();
        let auth_signer = generate_signer(); // Generate a signing key for auth
        let auth_key = auth_signer.verifying_key().as_bytes().to_vec(); // Auth key with keyType=2

        // Register user with custody address
        test_helper::register_user(fid, signer.clone(), custody_address.clone(), &mut engine1)
            .await;

        // Add an auth key (keyType=2)
        let auth_signer_event = events_factory::create_signer_event(
            fid,
            auth_signer,
            proto::SignerEventType::Add,
            None,
            Some(2), // keyType=2 for auth
        );
        commit_event(&mut engine1, &auth_signer_event).await;

        // Test custody address
        let request = Request::new(proto::FidAddressTypeRequest {
            fid,
            address: custody_address.clone(),
        });
        let response = service.get_fid_address_type(request).await.unwrap();
        let result = response.get_ref();
        assert!(result.is_custody);
        assert!(!result.is_auth);
        assert!(!result.is_verified);

        // Test auth address
        let request = Request::new(proto::FidAddressTypeRequest {
            fid,
            address: auth_key.clone(),
        });
        let response = service.get_fid_address_type(request).await.unwrap();
        let result = response.get_ref();
        assert!(!result.is_custody);
        assert!(result.is_auth);
        assert!(!result.is_verified);

        // Test unknown address
        let unknown_address = hex::decode("1234567890abcdef1234567890abcdef12345678").unwrap();
        let request = Request::new(proto::FidAddressTypeRequest {
            fid,
            address: unknown_address,
        });
        let response = service.get_fid_address_type(request).await.unwrap();
        let result = response.get_ref();
        assert!(!result.is_custody);
        assert!(!result.is_auth);
        assert!(!result.is_verified);
    }

    #[tokio::test]
    async fn test_get_on_chain_signers_by_fid() {
        let (_stores, _senders, [mut engine1, _], service) = make_server(None).await;
        let fid = SHARD1_FID;
        let signer = test_helper::default_signer();
        let owner = test_helper::default_custody_address();

        // Register user to create signer event
        test_helper::register_user(fid, signer.clone(), owner.clone(), &mut engine1).await;

        let signer_event = events_factory::create_signer_event(
            fid,
            generate_signer(),
            proto::SignerEventType::Add,
            None,
            None,
        );
        commit_event(&mut engine1, &signer_event).await;

        // Non-signer key
        let signer_event = events_factory::create_signer_event(
            fid,
            generate_signer(),
            proto::SignerEventType::Add,
            None,
            Some(2),
        );
        commit_event(&mut engine1, &signer_event).await;

        // Test normal request
        let request = Request::new(FidRequest {
            fid,
            page_size: Some(1),
            page_token: None,
            reverse: None,
        });
        let response = service.get_on_chain_signers_by_fid(request).await.unwrap();
        let events = response.get_ref().events.clone();
        assert_eq!(events.len(), 1);
        assert!(events
            .iter()
            .all(|event| event.r#type() == OnChainEventType::EventTypeSigner));

        // Test pagination
        let request = Request::new(FidRequest {
            fid,
            page_size: None,
            page_token: response.get_ref().next_page_token.clone(),
            reverse: None,
        });
        let response = service.get_on_chain_signers_by_fid(request).await.unwrap();
        let events = response.get_ref().events.clone();
        // only 2 keys total, non-signer key is not returned
        assert_eq!(events.len(), 1);
        assert!(events
            .iter()
            .all(|event| event.r#type() == OnChainEventType::EventTypeSigner));
    }
}
