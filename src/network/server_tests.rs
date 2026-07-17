#[cfg(test)]
mod tests {
    use crate::connectors::onchain_events::ens::EnsError;
    use alloy_primitives::keccak256;
    use async_trait::async_trait;
    use base64::Engine;
    use prost::Message;
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::sync::Arc;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
    use tokio::time::{sleep, timeout};

    use crate::connectors::fname::{FetchError, FnameTransferLookup};
    use crate::connectors::onchain_events::{Chain, ChainAPI, ChainClients};
    use crate::core::validations::{self, verification::VerificationAddressClaim};
    use crate::mempool::mempool::{self, Mempool};
    use crate::mempool::routing;
    use crate::mempool::routing::MessageRouter;
    use crate::network::server::MyHubService;
    use crate::proto::hub_service_server::HubService;
    use crate::proto::{
        self, Block, ChannelMemberRequest, ChannelMembersRequest, ChannelMembershipsByFidRequest,
        ChannelModerationsRequest, ChannelOwnerRequest, ChannelOwnerResponse,
        ChannelRegisterEventType, ChannelRequest, ChannelsByAddressRequest, ChannelsByFidRequest,
        EventRequest, EventsRequest, FarcasterNetwork, FnameTransfer, HubEvent, HubEventType,
        OnChainEventType, ShardChunk, StorageUnitType, SubmitBulkMessagesRequest,
        SubmitBulkMessagesResponse, UserDataType, UserNameProof, UserNameType,
        UsernameProofRequest, VerificationAddAddressBody,
    };
    use crate::proto::{FidRequest, SignersByFidRequest, SubscribeRequest};
    use crate::storage::constants::RootPrefix;
    use crate::storage::db::{PageOptions, RocksDB, RocksDbTransactionBatch};
    use crate::storage::store::account::{
        make_message_primary_key, make_ts_hash, ChannelMemberStore, ChannelModerateStore,
        ChannelPinStore, ChannelUpdateStore, HubEventIdGenerator, HubEventStorageExt,
        VerificationStoreDef, SEQUENCE_BITS,
    };
    use crate::storage::store::block_engine::BlockEngine;
    use crate::storage::store::block_engine_test_helpers::{BlockEngineOptions, Validity};
    use crate::storage::store::engine::{Senders, ShardEngine};
    use crate::storage::store::stores::Stores;
    use crate::storage::store::test_helper::{commit_event, register_user};
    use crate::storage::store::{block_engine_test_helpers, test_helper};
    use crate::storage::trie::merkle_trie;
    use crate::storage::util::increment_vec_u8;
    use crate::utils::factory::signers::generate_signer;
    use crate::utils::factory::{events_factory, messages_factory};
    use crate::utils::statsd_wrapper::StatsdClientWrapper;
    use futures::future;
    use futures::StreamExt;
    use tokio::sync::{broadcast, mpsc};
    use tonic::Request;

    const SHARD1_FID: u64 = test_helper::SHARD1_FID;
    const SHARD2_FID: u64 = test_helper::SHARD2_FID;

    const USER_NAME: &str = "user";
    const PASSWORD: &str = "password";

    fn fid_request(fid: u64) -> Request<FidRequest> {
        Request::new(FidRequest {
            fid,
            page_size: None,
            page_token: None,
            reverse: None,
        })
    }

    fn now_unix_seconds() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn channel_label(channel_key: &str) -> Vec<u8> {
        keccak256(channel_key.as_bytes()).to_vec()
    }

    fn owner_address(byte: u8) -> Vec<u8> {
        vec![byte; 20]
    }

    fn merge_channel_registration(
        block_engine: &BlockEngine,
        channel_key: &str,
        owner_address: Vec<u8>,
        expiry: u64,
    ) {
        merge_channel_event(
            block_engine,
            events_factory::create_channel_register_event(
                channel_key,
                channel_label(channel_key),
                owner_address,
                expiry,
                ChannelRegisterEventType::Register,
                1,
                1,
            ),
        );
    }

    fn merge_channel_transfer(
        block_engine: &BlockEngine,
        channel_key: &str,
        owner_address: Vec<u8>,
        expiry: u64,
    ) {
        merge_channel_event(
            block_engine,
            events_factory::create_channel_register_event(
                channel_key,
                channel_label(channel_key),
                owner_address,
                expiry,
                ChannelRegisterEventType::Transfer,
                2,
                1,
            ),
        );
    }

    fn merge_channel_event(block_engine: &BlockEngine, event: proto::OnChainEvent) {
        let block_stores = block_engine.stores();
        let mut txn = RocksDbTransactionBatch::new();
        block_stores
            .onchain_event_store
            .merge_onchain_event(event, &mut txn)
            .unwrap();
        block_stores.onchain_event_store.db.commit(txn).unwrap();
    }

    fn merge_channel_message(block_engine: &BlockEngine, message: &proto::Message) {
        let block_stores = block_engine.stores();
        let mut txn = RocksDbTransactionBatch::new();
        match message.msg_type() {
            proto::MessageType::ChannelUpdate => {
                ChannelUpdateStore::merge(&block_stores.channel_update_store, message, &mut txn)
                    .unwrap();
            }
            proto::MessageType::ChannelMember => {
                ChannelMemberStore::merge_with_gated_by_fid_index(
                    &block_stores.channel_member_store,
                    message,
                    &mut txn,
                    true,
                )
                .unwrap();
            }
            proto::MessageType::ChannelPin => {
                ChannelPinStore::merge(&block_stores.channel_pin_store, message, &mut txn).unwrap();
            }
            proto::MessageType::ChannelModerate => {
                ChannelModerateStore::merge(
                    &block_stores.channel_moderate_store,
                    message,
                    &mut txn,
                )
                .unwrap();
            }
            other => panic!("unexpected channel message type: {other:?}"),
        }
        block_stores.db.commit(txn).unwrap();
    }

    async fn get_channel_owner_response(
        service: &MyHubService,
        channel_key: &str,
    ) -> ChannelOwnerResponse {
        service
            .get_channel_owner(Request::new(ChannelOwnerRequest {
                channel_key: channel_key.to_string(),
            }))
            .await
            .unwrap()
            .into_inner()
    }

    async fn get_channels_by_fid_keys(service: &MyHubService, fid: u64) -> Vec<String> {
        service
            .get_channels_by_fid(Request::new(ChannelsByFidRequest {
                fid,
                page_size: None,
                page_token: None,
            }))
            .await
            .unwrap()
            .into_inner()
            .channels
            .into_iter()
            .map(|channel| channel.channel_key)
            .collect()
    }

    async fn assert_channel_owner_fid_invariant(
        service: &MyHubService,
        channel_key: &str,
        winner_fid: u64,
        loser_fid: Option<u64>,
    ) {
        let owner = get_channel_owner_response(service, channel_key).await;
        assert_eq!(owner.fid, winner_fid);

        if winner_fid != 0 {
            assert!(get_channels_by_fid_keys(service, winner_fid)
                .await
                .contains(&channel_key.to_string()));
        }
        if let Some(loser_fid) = loser_fid {
            assert!(!get_channels_by_fid_keys(service, loser_fid)
                .await
                .contains(&channel_key.to_string()));
        }
    }

    fn verification_add(fid: u64, address: Vec<u8>, timestamp: u32) -> proto::Message {
        messages_factory::verifications::create_verification_add(
            fid,
            0,
            address,
            vec![],
            vec![],
            Some(timestamp),
            None,
        )
    }

    fn verification_remove(fid: u64, address: Vec<u8>, timestamp: u32) -> proto::Message {
        messages_factory::verifications::create_verification_remove(
            fid,
            address,
            Some(timestamp),
            None,
        )
    }

    fn channels_by_address_request(address: Vec<u8>) -> Request<ChannelsByAddressRequest> {
        Request::new(ChannelsByAddressRequest {
            owner_address: address,
            page_size: None,
            page_token: None,
            reverse: None,
        })
    }

    fn channels_by_fid_request(fid: u64, page_size: Option<u32>) -> Request<ChannelsByFidRequest> {
        Request::new(ChannelsByFidRequest {
            fid,
            page_size,
            page_token: None,
        })
    }

    fn merge_verification(stores: &Stores, verification: &proto::Message) {
        let mut txn = RocksDbTransactionBatch::new();
        stores
            .verification_store
            .merge(verification, &mut txn, &test_helper::default_merge_ctx())
            .unwrap();
        stores.db.commit(txn).unwrap();
    }

    fn merge_shard_zero_verification(block_engine: &BlockEngine, verification: &proto::Message) {
        let stores = block_engine.stores();
        let mut txn = RocksDbTransactionBatch::new();
        stores
            .verification_store
            .merge(verification, &mut txn, &test_helper::default_merge_ctx())
            .unwrap();
        stores.db.commit(txn).unwrap();
    }

    fn merge_channel_owner_verification(
        stores: &Stores,
        block_engine: &BlockEngine,
        verification: &proto::Message,
    ) {
        merge_verification(stores, verification);
        merge_shard_zero_verification(block_engine, verification);
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

    async fn submit_bulk_messages(
        service: &MyHubService,
        messages: Vec<proto::Message>,
    ) -> Result<tonic::Response<SubmitBulkMessagesResponse>, tonic::Status> {
        let mut request = Request::new(SubmitBulkMessagesRequest { messages });
        add_auth_header(&mut request, USER_NAME, PASSWORD);
        service.submit_bulk_messages(request).await
    }

    async fn make_server(
        rpc_auth: Option<String>,
        admin_rpc_auth: Option<String>,
    ) -> (
        HashMap<u32, Stores>,
        HashMap<u32, Senders>,
        [ShardEngine; 2],
        BlockEngine,
        MyHubService,
        broadcast::Sender<ShardChunk>,
        broadcast::Sender<Block>,
    ) {
        let (msgs_request_tx, msgs_request_rx) = mpsc::channel(100);

        let statsd_client = StatsdClientWrapper::new(
            cadence::StatsdClient::builder("", cadence::NopMetricSink {}).build(),
            true,
        );

        let limits = test_helper::limits::test_store_limits();
        let (engine1, _) = test_helper::new_engine_with_options(test_helper::EngineOptions {
            limits: Some(limits.clone()),
            messages_request_tx: Some(msgs_request_tx.clone()),
            ..Default::default()
        })
        .await;
        let (engine2, _) = test_helper::new_engine_with_options(test_helper::EngineOptions {
            limits: Some(limits.clone()),
            messages_request_tx: Some(msgs_request_tx.clone()),
            shard_id: 2,
            ..Default::default()
        })
        .await;
        let db1 = engine1.db.clone();
        let db2 = engine2.db.clone();

        let shard1_stores = Stores::new(
            db1,
            1,
            merkle_trie::MerkleTrie::new().unwrap(),
            limits.clone(),
            proto::FarcasterNetwork::Devnet,
            test_helper::statsd_client(),
        );
        let shard1_senders = engine1.get_senders();

        let shard2_stores = Stores::new(
            db2,
            2,
            merkle_trie::MerkleTrie::new().unwrap(),
            limits.clone(),
            proto::FarcasterNetwork::Devnet,
            test_helper::statsd_client(),
        );
        let shard2_senders = engine2.get_senders();
        let stores = HashMap::from([(1, shard1_stores), (2, shard2_stores)]);
        let senders = HashMap::from([(1, shard1_senders), (2, shard2_senders)]);
        let num_shards = senders.len() as u32;

        let auth = rpc_auth.unwrap_or_else(|| format!("{}:{}", USER_NAME, PASSWORD));

        let message_router = Box::new(routing::EvenOddRouterForTest {});
        assert_eq!(message_router.route_fid(SHARD1_FID, 2), 1);
        assert_eq!(message_router.route_fid(SHARD2_FID, 2), 2);

        let (mempool_tx, mempool_rx) = mpsc::channel(1000);
        let (gossip_tx, _gossip_rx) = mpsc::channel(1000);
        let (shard_decision_tx, shard_decision_rx) = broadcast::channel(1000);
        let (block_decision_tx, block_decision_rx) = broadcast::channel(1000);
        let (block_engine, _) = block_engine_test_helpers::setup_with_options(BlockEngineOptions {
            messages_request_tx: Some(msgs_request_tx),
            ..BlockEngineOptions::default()
        });
        let block_stores = block_engine.stores();

        let mut mempool = Mempool::new(
            mempool::Config::default(),
            engine1.network,
            mempool_rx,
            msgs_request_rx,
            num_shards,
            stores.clone(),
            block_stores.clone(),
            gossip_tx.clone(),
            shard_decision_rx,
            block_decision_rx,
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
            block_engine,
            MyHubService::new(
                auth,
                admin_rpc_auth.unwrap_or_default(),
                vec![],
                block_stores,
                stores,
                senders,
                statsd_client,
                num_shards,
                proto::FarcasterNetwork::Devnet,
                message_router,
                mempool_tx.clone(),
                gossip_tx.clone(),
                chain_clients,
                "0.1.2".to_string(),
                "asddef".to_string(),
                None,
                Default::default(),
            ),
            shard_decision_tx,
            block_decision_tx,
        )
    }

    #[tokio::test]
    async fn test_get_channel_owner_unknown_channel_key_returns_not_found() {
        let (
            _stores,
            _senders,
            _engines,
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

        let err = service
            .get_channel_owner(Request::new(ChannelOwnerRequest {
                channel_key: "missing".to_string(),
            }))
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::NotFound);
        assert_eq!(err.message(), "channel not registered");
    }

    // The registry emits no onchain event when a lapsed registration's grace
    // period ends, so the endpoint reports the last known registration as-is
    // (owner still resolvable, expiry visibly in the past) and callers
    // interpret expiry themselves.
    #[tokio::test]
    async fn test_get_channel_owner_lapsed_registration_returns_record() {
        let (
            stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let address = owner_address(1);
        let expiry = now_unix_seconds() - 1;
        merge_channel_registration(&block_engine, "lapsed", address.clone(), expiry);
        let verification = messages_factory::verifications::create_verification_add(
            SHARD1_FID,
            0,
            address.clone(),
            vec![],
            vec![],
            Some(10),
            None,
        );
        merge_verification(stores.get(&1).unwrap(), &verification);
        merge_shard_zero_verification(&block_engine, &verification);

        let response = service
            .get_channel_owner(Request::new(ChannelOwnerRequest {
                channel_key: "lapsed".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.fid, SHARD1_FID);
        assert_eq!(response.owner_address, address);
        assert_eq!(response.expiry, expiry);
    }

    #[tokio::test]
    async fn test_get_channel_owner_registered_without_verification_returns_parked() {
        let (
            _stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let address = owner_address(2);
        let expiry = now_unix_seconds() + 3600;
        merge_channel_registration(&block_engine, "parked", address.clone(), expiry);

        let response = service
            .get_channel_owner(Request::new(ChannelOwnerRequest {
                channel_key: "parked".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.fid, 0);
        assert_eq!(response.owner_address, address);
        assert_eq!(response.expiry, expiry);
    }

    #[tokio::test]
    async fn test_get_channel_owner_ignores_pre_v20_data_shard_verification() {
        let (
            stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let address = owner_address(3);
        let expiry = now_unix_seconds() + 3600;
        merge_channel_registration(&block_engine, "verified", address.clone(), expiry);
        let verification_add = messages_factory::verifications::create_verification_add(
            SHARD1_FID,
            0,
            address.clone(),
            vec![],
            vec![],
            Some(1),
            None,
        );
        merge_verification(stores.get(&1).unwrap(), &verification_add);

        assert!(
            crate::storage::store::account::VerificationStore::get_verification_add(
                &stores.get(&1).unwrap().verification_store,
                SHARD1_FID,
                &address,
                None,
            )
            .unwrap()
            .is_some()
        );
        assert!(
            crate::storage::store::account::VerificationStore::get_verification_add(
                &block_engine.stores().verification_store,
                SHARD1_FID,
                &address,
                None,
            )
            .unwrap()
            .is_none()
        );

        let response = service
            .get_channel_owner(Request::new(ChannelOwnerRequest {
                channel_key: "verified".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.fid, 0);
        assert_eq!(response.owner_address, address);
        assert_eq!(response.expiry, expiry);
    }

    #[tokio::test]
    async fn test_get_channel_owner_lww_in_shard_zero_replica() {
        let (
            _stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let address = owner_address(4);
        merge_channel_registration(
            &block_engine,
            "lww",
            address.clone(),
            now_unix_seconds() + 3600,
        );
        let later = messages_factory::verifications::create_verification_add(
            SHARD2_FID,
            0,
            address.clone(),
            vec![],
            vec![],
            Some(20),
            None,
        );
        let earlier = messages_factory::verifications::create_verification_add(
            SHARD1_FID,
            0,
            address.clone(),
            vec![],
            vec![],
            Some(10),
            None,
        );
        merge_shard_zero_verification(&block_engine, &later);
        merge_shard_zero_verification(&block_engine, &earlier);

        let response = service
            .get_channel_owner(Request::new(ChannelOwnerRequest {
                channel_key: "lww".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.fid, SHARD2_FID);
    }

    // Mirror of the test above with the later verification on the OTHER shard.
    // `shard_stores` is a `HashMap` iterated in an unspecified order, so running
    // both directions pins "latest ts_hash wins" rather than an accidental
    // "last shard iterated wins" — one direction alone could pass by luck of the
    // hash order.
    #[tokio::test]
    async fn test_get_channel_owner_lww_in_shard_zero_replica_reversed() {
        let (
            _stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let address = owner_address(6);
        merge_channel_registration(
            &block_engine,
            "lww_reversed",
            address.clone(),
            now_unix_seconds() + 3600,
        );
        let later = messages_factory::verifications::create_verification_add(
            SHARD1_FID,
            0,
            address.clone(),
            vec![],
            vec![],
            Some(20),
            None,
        );
        let earlier = messages_factory::verifications::create_verification_add(
            SHARD2_FID,
            0,
            address.clone(),
            vec![],
            vec![],
            Some(10),
            None,
        );
        merge_shard_zero_verification(&block_engine, &later);
        merge_shard_zero_verification(&block_engine, &earlier);

        let response = service
            .get_channel_owner(Request::new(ChannelOwnerRequest {
                channel_key: "lww_reversed".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.fid, SHARD1_FID);
    }

    // A verification remove deletes both the primary add and its by-address index
    // entry, so a channel that resolved to an fid must fall back to parked once
    // the owner removes their verification. This exercises the real
    // add-then-remove path end to end, distinct from the artificial orphan seed.
    #[tokio::test]
    async fn test_get_channel_owner_reverts_to_parked_after_verification_remove() {
        let (
            _stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let address = owner_address(7);
        let expiry = now_unix_seconds() + 3600;
        merge_channel_registration(&block_engine, "removed", address.clone(), expiry);
        let verification_add = messages_factory::verifications::create_verification_add(
            SHARD1_FID,
            0,
            address.clone(),
            vec![],
            vec![],
            Some(10),
            None,
        );
        merge_shard_zero_verification(&block_engine, &verification_add);

        // Sanity: resolves to the verifier before the remove.
        let resolved = service
            .get_channel_owner(Request::new(ChannelOwnerRequest {
                channel_key: "removed".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resolved.fid, SHARD1_FID);

        // Later remove wins the CRDT and clears both the add and the index entry.
        let verification_remove = messages_factory::verifications::create_verification_remove(
            SHARD1_FID,
            address.clone(),
            Some(20),
            None,
        );
        merge_shard_zero_verification(&block_engine, &verification_remove);

        let response = service
            .get_channel_owner(Request::new(ChannelOwnerRequest {
                channel_key: "removed".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.fid, 0);
        assert_eq!(response.owner_address, address);
        assert_eq!(response.expiry, expiry);
    }

    #[tokio::test]
    async fn test_get_channel_owner_drops_orphan_index_candidate() {
        let (
            _stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let address = owner_address(5);
        let expiry = now_unix_seconds() + 3600;
        merge_channel_registration(&block_engine, "orphan", address.clone(), expiry);

        let mut txn = RocksDbTransactionBatch::new();
        let orphan_hash = vec![9; 20];
        let orphan_ts_hash = make_ts_hash(100, &orphan_hash).unwrap();
        txn.put(
            VerificationStoreDef::make_verification_by_address_key(&address, SHARD1_FID),
            orphan_ts_hash.to_vec(),
        );
        block_engine.stores().db.commit(txn).unwrap();

        let response = service
            .get_channel_owner(Request::new(ChannelOwnerRequest {
                channel_key: "orphan".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.fid, 0);
        assert_eq!(response.owner_address, address);
        assert_eq!(response.expiry, expiry);
    }

    #[tokio::test]
    async fn test_channel_reads_delayed_flip_from_parked_to_verified() {
        let (
            stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let address = owner_address(8);
        let expiry = now_unix_seconds() + 3600;
        merge_channel_registration(&block_engine, "delayed_flip", address.clone(), expiry);

        let parked = get_channel_owner_response(&service, "delayed_flip").await;
        assert_eq!(parked.fid, 0);
        assert_eq!(parked.owner_address, address);
        assert_eq!(
            get_channels_by_fid_keys(&service, SHARD1_FID).await,
            Vec::<String>::new()
        );
        let by_address_parked = service
            .get_channels_by_address(channels_by_address_request(address.clone()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(by_address_parked.channels.len(), 1);
        assert_eq!(by_address_parked.channels[0].fid, 0);
        assert_eq!(by_address_parked.channels[0].channel_key, "delayed_flip");

        merge_channel_owner_verification(
            stores.get(&1).unwrap(),
            &block_engine,
            &verification_add(SHARD1_FID, address.clone(), 10),
        );

        assert_channel_owner_fid_invariant(&service, "delayed_flip", SHARD1_FID, None).await;
        let by_address_resolved = service
            .get_channels_by_address(channels_by_address_request(address))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(by_address_resolved.channels.len(), 1);
        assert_eq!(by_address_resolved.channels[0].fid, SHARD1_FID);
        assert_eq!(by_address_resolved.next_page_token, None);
    }

    // Lapsed registrations stay listed (with their past expiry) for the same
    // reason GetChannelOwner returns them: release state is not computable
    // from chain events, and a renewal prompt needs the owner to still see
    // the channel.
    #[tokio::test]
    async fn test_channel_lists_include_lapsed_registrations() {
        let (
            stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let address = owner_address(9);
        let expiry = now_unix_seconds() - 1;
        merge_channel_registration(&block_engine, "lapsed_list", address.clone(), expiry);
        merge_channel_owner_verification(
            stores.get(&1).unwrap(),
            &block_engine,
            &verification_add(SHARD1_FID, address.clone(), 10),
        );

        let by_address = service
            .get_channels_by_address(channels_by_address_request(address.clone()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(by_address.channels.len(), 1);
        assert_eq!(by_address.channels[0].channel_key, "lapsed_list");
        assert_eq!(by_address.channels[0].fid, SHARD1_FID);
        assert_eq!(by_address.channels[0].expiry, expiry);

        assert_eq!(
            get_channels_by_fid_keys(&service, SHARD1_FID).await,
            vec!["lapsed_list".to_string()]
        );
    }

    #[tokio::test]
    async fn test_channel_reads_last_verifier_wins_regardless_of_merge_order() {
        let (
            stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let address = owner_address(9);
        merge_channel_registration(
            &block_engine,
            "last_verifier_wins",
            address.clone(),
            now_unix_seconds() + 3600,
        );

        let later = verification_add(SHARD2_FID, address.clone(), 20);
        let earlier = verification_add(SHARD1_FID, address, 10);
        merge_channel_owner_verification(stores.get(&2).unwrap(), &block_engine, &later);
        merge_channel_owner_verification(stores.get(&1).unwrap(), &block_engine, &earlier);

        assert_channel_owner_fid_invariant(
            &service,
            "last_verifier_wins",
            SHARD2_FID,
            Some(SHARD1_FID),
        )
        .await;
    }

    #[tokio::test]
    async fn test_channel_reads_remove_fallback_then_parked() {
        let (
            stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let address = owner_address(10);
        merge_channel_registration(
            &block_engine,
            "remove_fallback",
            address.clone(),
            now_unix_seconds() + 3600,
        );
        merge_channel_owner_verification(
            stores.get(&1).unwrap(),
            &block_engine,
            &verification_add(SHARD1_FID, address.clone(), 10),
        );
        merge_channel_owner_verification(
            stores.get(&2).unwrap(),
            &block_engine,
            &verification_add(SHARD2_FID, address.clone(), 20),
        );
        assert_channel_owner_fid_invariant(
            &service,
            "remove_fallback",
            SHARD2_FID,
            Some(SHARD1_FID),
        )
        .await;

        merge_channel_owner_verification(
            stores.get(&2).unwrap(),
            &block_engine,
            &verification_remove(SHARD2_FID, address.clone(), 30),
        );
        assert_channel_owner_fid_invariant(
            &service,
            "remove_fallback",
            SHARD1_FID,
            Some(SHARD2_FID),
        )
        .await;

        merge_channel_owner_verification(
            stores.get(&1).unwrap(),
            &block_engine,
            &verification_remove(SHARD1_FID, address, 40),
        );
        let parked = get_channel_owner_response(&service, "remove_fallback").await;
        assert_eq!(parked.fid, 0);
        assert!(!get_channels_by_fid_keys(&service, SHARD1_FID)
            .await
            .contains(&"remove_fallback".to_string()));
        assert!(!get_channels_by_fid_keys(&service, SHARD2_FID)
            .await
            .contains(&"remove_fallback".to_string()));
    }

    #[tokio::test]
    async fn test_channel_reads_cold_wallet_round_trip() {
        let (
            stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let address_a = owner_address(11);
        let address_b = owner_address(12);
        let expiry = now_unix_seconds() + 3600;
        merge_channel_registration(&block_engine, "cold_wallet", address_a.clone(), expiry);

        let parked = get_channel_owner_response(&service, "cold_wallet").await;
        assert_eq!(parked.fid, 0);
        assert_eq!(parked.owner_address, address_a);
        assert_eq!(parked.expiry, expiry);

        merge_channel_owner_verification(
            stores.get(&1).unwrap(),
            &block_engine,
            &verification_add(SHARD1_FID, address_a, 10),
        );
        assert_channel_owner_fid_invariant(&service, "cold_wallet", SHARD1_FID, None).await;

        merge_channel_transfer(
            &block_engine,
            "cold_wallet",
            address_b.clone(),
            expiry + 999,
        );
        let transferred = get_channel_owner_response(&service, "cold_wallet").await;
        assert_eq!(transferred.fid, 0);
        assert_eq!(transferred.owner_address, address_b);
        assert_eq!(transferred.expiry, expiry);

        merge_channel_owner_verification(
            stores.get(&2).unwrap(),
            &block_engine,
            &verification_add(SHARD2_FID, address_b, 20),
        );
        assert_channel_owner_fid_invariant(&service, "cold_wallet", SHARD2_FID, Some(SHARD1_FID))
            .await;
    }

    #[tokio::test]
    async fn test_channel_reads_verify_with_no_channels_noop() {
        let (
            stores,
            _senders,
            _engines,
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let address = owner_address(13);
        merge_verification(
            stores.get(&1).unwrap(),
            &verification_add(SHARD1_FID, address.clone(), 10),
        );

        let by_fid = service
            .get_channels_by_fid(channels_by_fid_request(SHARD1_FID, None))
            .await
            .unwrap()
            .into_inner();
        assert!(by_fid.channels.is_empty());
        assert_eq!(by_fid.next_page_token, None);

        let by_address = service
            .get_channels_by_address(channels_by_address_request(address))
            .await
            .unwrap()
            .into_inner();
        assert!(by_address.channels.is_empty());
        assert_eq!(by_address.next_page_token, None);
    }

    #[tokio::test]
    async fn test_channel_message_read_surface_uses_shard_zero_folds_and_indexes() {
        let (
            _stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None).await;
        let channel_key = "message_reads";
        let channel_id = channel_label(channel_key);
        merge_channel_registration(
            &block_engine,
            channel_key,
            owner_address(55),
            now_unix_seconds() + 3600,
        );

        let empty_member = service
            .get_channel_member(Request::new(ChannelMemberRequest {
                channel_id: channel_id.clone(),
                fid: 101,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(empty_member.state, proto::ChannelMemberState::None as i32);
        assert_eq!(empty_member.last_action_ts, None);

        let update = messages_factory::create_message_with_data(
            500,
            proto::MessageType::ChannelUpdate,
            proto::message_data::Body::ChannelUpdateBody(proto::ChannelUpdateBody {
                channel_id: channel_id.clone(),
                name: Some("Channel name".to_string()),
                image_url: Some("https://example.com/image.png".to_string()),
                ..Default::default()
            }),
            Some(9),
            None,
        );
        let moderator = messages_factory::create_message_with_data(
            500,
            proto::MessageType::ChannelMember,
            proto::message_data::Body::ChannelMemberBody(proto::ChannelMemberBody {
                channel_id: channel_id.clone(),
                fid: 101,
                action: proto::ChannelMemberAction::AddModerator as i32,
            }),
            Some(10),
            None,
        );
        let banned = messages_factory::create_message_with_data(
            500,
            proto::MessageType::ChannelMember,
            proto::message_data::Body::ChannelMemberBody(proto::ChannelMemberBody {
                channel_id: channel_id.clone(),
                fid: 102,
                action: proto::ChannelMemberAction::Ban as i32,
            }),
            Some(11),
            None,
        );
        let pin_hash = vec![0xAB; 20];
        let pin = messages_factory::create_message_with_data(
            501,
            proto::MessageType::ChannelPin,
            proto::message_data::Body::ChannelPinBody(proto::ChannelPinBody {
                channel_id: channel_id.clone(),
                cast_hash: pin_hash.clone(),
            }),
            Some(12),
            None,
        );
        let moderated_hash = vec![0xCD; 20];
        let moderation = messages_factory::create_message_with_data(
            502,
            proto::MessageType::ChannelModerate,
            proto::message_data::Body::ChannelModerateBody(proto::ChannelModerateBody {
                channel_id: channel_id.clone(),
                cast_hash: moderated_hash.clone(),
                action: proto::ChannelModerateAction::Hide as i32,
            }),
            Some(13),
            None,
        );
        for message in [&update, &moderator, &banned, &pin, &moderation] {
            merge_channel_message(&block_engine, message);
        }

        let member = service
            .get_channel_member(Request::new(ChannelMemberRequest {
                channel_id: channel_id.clone(),
                fid: 101,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(member.state, proto::ChannelMemberState::Moderator as i32);
        assert_eq!(member.last_action_ts, Some(10));

        let members = service
            .get_channel_members(Request::new(ChannelMembersRequest {
                channel_id: channel_id.clone(),
                state_filter: None,
                page_size: Some(1),
                page_token: None,
                reverse: Some(false),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(members.members.len(), 1);
        assert!(members.next_page_token.is_some());

        let moderators = service
            .get_channel_members(Request::new(ChannelMembersRequest {
                channel_id: channel_id.clone(),
                state_filter: Some(proto::ChannelMemberState::Moderator as i32),
                page_size: None,
                page_token: None,
                reverse: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(moderators.members.len(), 1);
        assert_eq!(moderators.members[0].fid, 101);

        let memberships = service
            .get_channel_memberships_by_fid(Request::new(ChannelMembershipsByFidRequest {
                fid: 101,
                page_size: None,
                page_token: None,
                reverse: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(memberships.memberships.len(), 1);
        assert_eq!(memberships.memberships[0].channel_id, channel_id);
        assert_eq!(
            memberships.memberships[0].state,
            proto::ChannelMemberState::Moderator as i32
        );

        let pin_response = service
            .get_channel_pin(Request::new(ChannelRequest {
                channel_id: channel_label(channel_key),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(pin_response.cast_hash, Some(pin_hash));
        assert_eq!(pin_response.author_fid, Some(501));

        let moderations = service
            .get_channel_moderations(Request::new(ChannelModerationsRequest {
                channel_id: channel_label(channel_key),
                page_size: None,
                page_token: None,
                reverse: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(moderations.moderations.len(), 1);
        assert_eq!(moderations.moderations[0].cast_hash, moderated_hash);
        assert_eq!(moderations.moderations[0].author_fid, 502);

        let metadata = service
            .get_channel_metadata(Request::new(ChannelRequest {
                channel_id: channel_label(channel_key),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(metadata.name.as_deref(), Some("Channel name"));
        assert_eq!(
            metadata.casting_mode,
            proto::CastingMode::MembersOnly as i32
        );
        assert_eq!(
            metadata.membership_mode,
            proto::MembershipMode::Approval as i32
        );

        let malformed = service
            .get_channel_pin(Request::new(ChannelRequest {
                channel_id: vec![1; 31],
            }))
            .await
            .unwrap_err();
        assert_eq!(malformed.code(), tonic::Code::InvalidArgument);
        let unregistered = service
            .get_channel_pin(Request::new(ChannelRequest {
                channel_id: vec![9; 32],
            }))
            .await
            .unwrap_err();
        assert_eq!(unregistered.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn channel_fanout_end_to_end_keeps_shard_zero_replicas_and_reads_in_sync() {
        let (
            _stores,
            _senders,
            mut engines,
            mut block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None).await;
        const OWNER_FID: u64 = 7101;
        const MODERATOR_FID: u64 = 7102;
        const TARGET_FID: u64 = 7103;

        for fid in [OWNER_FID, MODERATOR_FID] {
            block_engine_test_helpers::register_user(
                fid,
                block_engine_test_helpers::default_signer(),
                block_engine_test_helpers::default_custody_address(),
                1,
                &mut block_engine,
            );
        }
        let channel_key = "fanout-e2e";
        let channel_id = channel_label(channel_key);
        let owner = owner_address(0x71);
        merge_channel_registration(
            &block_engine,
            channel_key,
            owner.clone(),
            now_unix_seconds() + 3600,
        );
        merge_shard_zero_verification(
            &block_engine,
            &verification_add(OWNER_FID, owner, messages_factory::farcaster_time()),
        );

        let timestamp = messages_factory::farcaster_time() + 10;
        let update = messages_factory::create_message_with_data(
            OWNER_FID,
            proto::MessageType::ChannelUpdate,
            proto::message_data::Body::ChannelUpdateBody(proto::ChannelUpdateBody {
                channel_id: channel_id.clone(),
                name: Some("fanout".to_string()),
                ..Default::default()
            }),
            Some(timestamp),
            None,
        );
        let add_moderator = messages_factory::create_message_with_data(
            OWNER_FID,
            proto::MessageType::ChannelMember,
            proto::message_data::Body::ChannelMemberBody(proto::ChannelMemberBody {
                channel_id: channel_id.clone(),
                fid: MODERATOR_FID,
                action: proto::ChannelMemberAction::AddModerator as i32,
            }),
            Some(timestamp + 1),
            None,
        );
        let add_target = messages_factory::create_message_with_data(
            OWNER_FID,
            proto::MessageType::ChannelMember,
            proto::message_data::Body::ChannelMemberBody(proto::ChannelMemberBody {
                channel_id: channel_id.clone(),
                fid: TARGET_FID,
                action: proto::ChannelMemberAction::AddMember as i32,
            }),
            Some(timestamp + 2),
            None,
        );
        let pin_hash = vec![0x72; 20];
        let pin = messages_factory::create_message_with_data(
            MODERATOR_FID,
            proto::MessageType::ChannelPin,
            proto::message_data::Body::ChannelPinBody(proto::ChannelPinBody {
                channel_id: channel_id.clone(),
                cast_hash: pin_hash.clone(),
            }),
            Some(timestamp + 3),
            None,
        );
        let moderated_hash = vec![0x73; 20];
        let moderation = messages_factory::create_message_with_data(
            MODERATOR_FID,
            proto::MessageType::ChannelModerate,
            proto::message_data::Body::ChannelModerateBody(proto::ChannelModerateBody {
                channel_id: channel_id.clone(),
                cast_hash: moderated_hash.clone(),
                action: proto::ChannelModerateAction::Hide as i32,
            }),
            Some(timestamp + 4),
            None,
        );
        // Cross-author supersede: the moderator's later consensus action replaces the owner's
        // incumbent target slot even though its embedded timestamp is deliberately older.
        let ban_target = messages_factory::create_message_with_data(
            MODERATOR_FID,
            proto::MessageType::ChannelMember,
            proto::message_data::Body::ChannelMemberBody(proto::ChannelMemberBody {
                channel_id: channel_id.clone(),
                fid: TARGET_FID,
                action: proto::ChannelMemberAction::Ban as i32,
            }),
            Some(timestamp - 1),
            None,
        );
        let workload = vec![
            update.clone(),
            add_moderator.clone(),
            add_target.clone(),
            pin.clone(),
            moderation.clone(),
            ban_target.clone(),
        ];

        let [replica_a, replica_b] = &mut engines;
        // Bring both consumers to the exact BlockEvent cursor produced while registering the
        // test authors. Those earlier blocks contribute heartbeat events; replaying them is what
        // makes the subsequent channel event's strict seqnum provenance real rather than
        // manufacturing a fresh sequence for the test.
        let initial_block_events = block_engine.stores().block_event_store;
        for seqnum in 1..=initial_block_events.max_seqnum().unwrap() {
            let event = initial_block_events
                .get_block_event_by_seqnum(seqnum)
                .unwrap()
                .unwrap();
            let replay_a = replica_a.propose_state_change(
                replica_a.shard_id(),
                vec![
                    crate::storage::store::mempool_poller::MempoolMessage::BlockEvent {
                        for_shard: replica_a.shard_id(),
                        message: event.clone(),
                    },
                ],
                None,
            );
            let replay_b = replica_b.propose_state_change(
                replica_b.shard_id(),
                vec![
                    crate::storage::store::mempool_poller::MempoolMessage::BlockEvent {
                        for_shard: replica_b.shard_id(),
                        message: event,
                    },
                ],
                None,
            );
            test_helper::validate_and_commit_state_change(replica_a, &replay_a).await;
            test_helper::validate_and_commit_state_change(replica_b, &replay_b).await;
        }
        for message in &workload {
            let height = block_engine.get_confirmed_height().increment();
            let state_change = block_engine.propose_state_change(
                vec![
                    crate::storage::store::mempool_poller::MempoolMessage::UserMessage(
                        message.clone(),
                    ),
                ],
                height,
                Some(crate::core::util::FarcasterTime::new(
                    message.data.as_ref().unwrap().timestamp as u64,
                )),
            );
            let block = block_engine_test_helpers::validate_and_commit_state_change(
                &mut block_engine,
                &state_change,
            );
            assert_eq!(
                block
                    .events
                    .iter()
                    .filter(|event| {
                        matches!(
                            event.data.as_ref().and_then(|data| data.body.as_ref()),
                            Some(proto::block_event_data::Body::MergeMessageEventBody(body))
                                if body.message.as_ref() == Some(message)
                        )
                    })
                    .count(),
                1,
                "one channel fan-out event per admitted merge"
            );

            let mut fanout_events = block.events.clone();
            fanout_events.sort_by_key(|event| event.seqnum());
            for event in fanout_events {
                let replay_a = replica_a.propose_state_change(
                    replica_a.shard_id(),
                    vec![
                        crate::storage::store::mempool_poller::MempoolMessage::BlockEvent {
                            for_shard: replica_a.shard_id(),
                            message: event.clone(),
                        },
                    ],
                    None,
                );
                let replay_b = replica_b.propose_state_change(
                    replica_b.shard_id(),
                    vec![
                        crate::storage::store::mempool_poller::MempoolMessage::BlockEvent {
                            for_shard: replica_b.shard_id(),
                            message: event,
                        },
                    ],
                    None,
                );
                test_helper::validate_and_commit_state_change(replica_a, &replay_a).await;
                test_helper::validate_and_commit_state_change(replica_b, &replay_b).await;
            }
        }

        assert_eq!(replica_a.trie_root_hash(), replica_b.trie_root_hash());
        let block_stores = block_engine.stores();
        let replica_a_stores = replica_a.get_stores();
        let replica_b_stores = replica_b.get_stores();

        let channel_rows = |db: &RocksDB| {
            let prefix = vec![RootPrefix::Channel as u8];
            let mut rows = BTreeMap::new();
            db.for_each_iterator_by_prefix(
                Some(prefix.clone()),
                Some(increment_vec_u8(&prefix)),
                &PageOptions::default(),
                |key, value| {
                    rows.insert(key.to_vec(), value.to_vec());
                    Ok(false)
                },
            )
            .unwrap();
            rows
        };
        let shard_zero_channel_rows = channel_rows(&block_stores.db);
        assert_eq!(channel_rows(&replica_a.db), shard_zero_channel_rows);
        assert_eq!(channel_rows(&replica_b.db), shard_zero_channel_rows);

        for message in [&update, &add_moderator, &pin, &moderation, &ban_target] {
            let postfix = match message.msg_type() {
                proto::MessageType::ChannelUpdate => block_stores.channel_update_store.postfix(),
                proto::MessageType::ChannelMember => block_stores.channel_member_store.postfix(),
                proto::MessageType::ChannelPin => block_stores.channel_pin_store.postfix(),
                proto::MessageType::ChannelModerate => {
                    block_stores.channel_moderate_store.postfix()
                }
                other => panic!("unexpected type {other:?}"),
            };
            let data = message.data.as_ref().unwrap();
            let ts_hash = make_ts_hash(data.timestamp, &message.hash).unwrap();
            let key = make_message_primary_key(message.fid(), postfix, Some(&ts_hash));
            let expected = block_stores.db.get(&key).unwrap();
            assert!(expected.is_some());
            assert_eq!(replica_a.db.get(&key).unwrap(), expected);
            assert_eq!(replica_b.db.get(&key).unwrap(), expected);
            assert!(test_helper::message_exists_in_trie(replica_a, message));
            assert!(test_helper::message_exists_in_trie(replica_b, message));
            assert!(
                crate::storage::trie::merkle_trie::TrieKey::for_message(message)
                    .iter()
                    .all(|key| block_engine.trie_key_exists(test_helper::trie_ctx(), key))
            );
        }
        assert!(!test_helper::message_exists_in_trie(replica_a, &add_target));
        assert!(!test_helper::message_exists_in_trie(replica_b, &add_target));

        let merge_bodies = |engine: &ShardEngine| {
            HubEvent::get_events(engine.db.clone(), 0, None, None)
                .unwrap()
                .events
                .into_iter()
                .filter_map(|event| match event.body {
                    Some(proto::hub_event::Body::MergeMessageBody(body)) => Some(body),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        let replica_a_bodies = merge_bodies(replica_a);
        let replica_b_bodies = merge_bodies(replica_b);
        assert_eq!(replica_a_bodies, replica_b_bodies);
        let ban_event = replica_a_bodies
            .iter()
            .find(|body| body.message.as_ref() == Some(&ban_target))
            .unwrap();
        assert_eq!(ban_event.deleted_messages, vec![add_target]);

        assert_eq!(
            ChannelUpdateStore::get_channel_update(
                &replica_a_stores.channel_update_store,
                &channel_id,
                None,
            )
            .unwrap(),
            ChannelUpdateStore::get_channel_update(
                &replica_b_stores.channel_update_store,
                &channel_id,
                None,
            )
            .unwrap()
        );
        assert_eq!(
            ChannelMemberStore::member_state(
                &replica_a_stores.channel_member_store,
                &channel_id,
                TARGET_FID,
                None,
            )
            .unwrap(),
            Some(crate::storage::store::account::ChannelMemberState::Banned)
        );

        let member = service
            .get_channel_member(Request::new(ChannelMemberRequest {
                channel_id: channel_id.clone(),
                fid: TARGET_FID,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(member.state, proto::ChannelMemberState::Banned as i32);
        let metadata = service
            .get_channel_metadata(Request::new(ChannelRequest {
                channel_id: channel_id.clone(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(metadata.name.as_deref(), Some("fanout"));
        let memberships = service
            .get_channel_memberships_by_fid(Request::new(ChannelMembershipsByFidRequest {
                fid: TARGET_FID,
                page_size: None,
                page_token: None,
                reverse: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(memberships.memberships.len(), 1);
        assert_eq!(memberships.memberships[0].channel_id, channel_id);

        let members_zero = service
            .get_channel_members(Request::new(ChannelMembersRequest {
                channel_id: channel_id.clone(),
                state_filter: None,
                page_size: Some(0),
                page_token: None,
                reverse: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(members_zero.members.is_empty());
        assert_eq!(members_zero.next_page_token, None);

        let moderations_zero = service
            .get_channel_moderations(Request::new(ChannelModerationsRequest {
                channel_id: channel_id.clone(),
                page_size: Some(0),
                page_token: None,
                reverse: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(moderations_zero.moderations.is_empty());
        assert_eq!(moderations_zero.next_page_token, None);

        let memberships_zero = service
            .get_channel_memberships_by_fid(Request::new(ChannelMembershipsByFidRequest {
                fid: TARGET_FID,
                page_size: Some(0),
                page_token: None,
                reverse: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(memberships_zero.memberships.is_empty());
        assert_eq!(memberships_zero.next_page_token, None);
    }

    // Registers `channel_key` to `owner_address` at a distinct chain position so
    // several channels can coexist under one or more owner addresses.
    fn register_channel_at(
        block_engine: &BlockEngine,
        channel_key: &str,
        owner_address: Vec<u8>,
        expiry: u64,
        log_index: u32,
    ) {
        merge_channel_event(
            block_engine,
            events_factory::create_channel_register_event(
                channel_key,
                channel_label(channel_key),
                owner_address,
                expiry,
                ChannelRegisterEventType::Register,
                1,
                log_index,
            ),
        );
    }

    async fn channels_by_fid_page(
        service: &MyHubService,
        fid: u64,
        page_size: Option<u32>,
        page_token: Option<Vec<u8>>,
    ) -> proto::ChannelsResponse {
        service
            .get_channels_by_fid(Request::new(ChannelsByFidRequest {
                fid,
                page_size,
                page_token,
            }))
            .await
            .unwrap()
            .into_inner()
    }

    // Scenario (g): a fid with two verified addresses owning four channels total,
    // walked with page_size smaller than the total. Asserts the paged union equals
    // the unpaginated result with no duplicates, that a page crosses an address
    // boundary, and that the final page's token is unset (== enumeration complete).
    #[tokio::test]
    async fn test_channel_reads_by_fid_cursor_round_trip() {
        let (
            stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        // owner_address(20) < owner_address(21) lexicographically, so the composite
        // cursor visits address_a's channels before address_b's.
        let address_a = owner_address(20);
        let address_b = owner_address(21);
        let expiry = now_unix_seconds() + 3600;
        register_channel_at(&block_engine, "rt_a1", address_a.clone(), expiry, 1);
        register_channel_at(&block_engine, "rt_a2", address_a.clone(), expiry, 2);
        register_channel_at(&block_engine, "rt_b1", address_b.clone(), expiry, 3);
        register_channel_at(&block_engine, "rt_b2", address_b.clone(), expiry, 4);
        merge_channel_owner_verification(
            stores.get(&1).unwrap(),
            &block_engine,
            &verification_add(SHARD1_FID, address_a.clone(), 10),
        );
        merge_channel_owner_verification(
            stores.get(&1).unwrap(),
            &block_engine,
            &verification_add(SHARD1_FID, address_b.clone(), 20),
        );

        // Unpaginated baseline: all four channels, no token.
        let full = channels_by_fid_page(&service, SHARD1_FID, None, None).await;
        let full_keys: Vec<String> = full
            .channels
            .iter()
            .map(|c| c.channel_key.clone())
            .collect();
        assert_eq!(full_keys, vec!["rt_a1", "rt_a2", "rt_b1", "rt_b2"]);
        assert_eq!(full.next_page_token, None);

        // Page 1 of 2 (page_size 3) must cross the address_a -> address_b boundary.
        let page1 = channels_by_fid_page(&service, SHARD1_FID, Some(3), None).await;
        assert_eq!(page1.channels.len(), 3);
        assert!(page1.next_page_token.is_some());
        let page1_owners: HashSet<Vec<u8>> = page1
            .channels
            .iter()
            .map(|c| c.owner_address.clone())
            .collect();
        assert_eq!(page1_owners.len(), 2, "page 1 should span both addresses");

        // Page 2 resumes strictly after the token and completes.
        let page2 =
            channels_by_fid_page(&service, SHARD1_FID, Some(3), page1.next_page_token.clone())
                .await;
        assert_eq!(page2.next_page_token, None);

        let mut walked: Vec<String> = page1
            .channels
            .iter()
            .chain(page2.channels.iter())
            .map(|c| c.channel_key.clone())
            .collect();
        let deduped: HashSet<String> = walked.iter().cloned().collect();
        assert_eq!(deduped.len(), walked.len(), "pages must not duplicate");
        walked.sort();
        assert_eq!(walked, vec!["rt_a1", "rt_a2", "rt_b1", "rt_b2"]);
        for channel in page1.channels.iter().chain(page2.channels.iter()) {
            assert_eq!(channel.fid, SHARD1_FID);
        }
    }

    // The composite cursor round-trips at the single-address level too, and the
    // page-size clamp caps an omitted/oversized request without dropping results.
    #[tokio::test]
    async fn test_channel_reads_by_address_pagination_round_trip() {
        let (
            stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let address = owner_address(22);
        let expiry = now_unix_seconds() + 3600;
        register_channel_at(&block_engine, "pa_1", address.clone(), expiry, 1);
        register_channel_at(&block_engine, "pa_2", address.clone(), expiry, 2);
        register_channel_at(&block_engine, "pa_3", address.clone(), expiry, 3);
        merge_channel_owner_verification(
            stores.get(&1).unwrap(),
            &block_engine,
            &verification_add(SHARD1_FID, address.clone(), 10),
        );

        let mut walked = Vec::new();
        let mut page_token = None;
        loop {
            let response = service
                .get_channels_by_address(Request::new(ChannelsByAddressRequest {
                    owner_address: address.clone(),
                    page_size: Some(2),
                    page_token: page_token.clone(),
                    reverse: None,
                }))
                .await
                .unwrap()
                .into_inner();
            for channel in &response.channels {
                assert_eq!(channel.fid, SHARD1_FID);
                walked.push(channel.channel_key.clone());
            }
            match response.next_page_token {
                Some(token) => page_token = Some(token),
                None => break,
            }
        }

        assert_eq!(walked, vec!["pa_1", "pa_2", "pa_3"]);
    }

    // Pins the page-size contract shared with GetChannelsByFid: 0 is an empty
    // page with no token, not "at least one row".
    #[tokio::test]
    async fn test_channel_reads_by_address_zero_page_size_returns_empty_page() {
        let (
            _stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let address = owner_address(24);
        merge_channel_registration(
            &block_engine,
            "zero_page",
            address.clone(),
            now_unix_seconds() + 3600,
        );

        let response = service
            .get_channels_by_address(Request::new(ChannelsByAddressRequest {
                owner_address: address,
                page_size: Some(0),
                page_token: None,
                reverse: None,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(response.channels.is_empty());
        assert_eq!(response.next_page_token, None);
    }

    // Non-EVM (Solana) and malformed verifications must never contribute an
    // owner address to the EVM-only channel resolution.
    #[tokio::test]
    async fn test_channel_reads_by_fid_excludes_non_ethereum() {
        let (
            stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let evm_address = owner_address(23);
        let solana_address = vec![7u8; 32];
        let expiry = now_unix_seconds() + 3600;
        register_channel_at(&block_engine, "evm_owned", evm_address.clone(), expiry, 1);

        merge_channel_owner_verification(
            stores.get(&1).unwrap(),
            &block_engine,
            &verification_add(SHARD1_FID, evm_address, 10),
        );
        // The Solana verification's 32-byte address must be filtered out before
        // resolution. Without the protocol/length filter it would be treated as an
        // owner address and fail the 20-byte EVM validation, erroring the request.
        let solana_verification = messages_factory::verifications::create_verification_add(
            SHARD1_FID,
            2, // Protocol::Solana
            solana_address,
            vec![],
            vec![],
            Some(20),
            None,
        );
        merge_verification(stores.get(&1).unwrap(), &solana_verification);

        let keys = get_channels_by_fid_keys(&service, SHARD1_FID).await;
        assert_eq!(keys, vec!["evm_owned"]);
    }

    // A non-20-byte address is a client error, mapped to INVALID_ARGUMENT rather
    // than surfacing as an opaque internal error.
    #[tokio::test]
    async fn test_channel_reads_by_address_rejects_malformed_address() {
        let (
            _stores,
            _senders,
            _engines,
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

        let status = service
            .get_channels_by_address(channels_by_address_request(vec![0u8; 4]))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    // Pins the accepted last-key cursor contract (shared with by-address): when a
    // page fills exactly on the final entry, a token is still returned and the
    // next request returns an empty final page with the token unset. Callers page
    // until the token is unset; a present token does not guarantee more results.
    #[tokio::test]
    async fn test_channel_reads_by_fid_exact_boundary_yields_final_empty_page() {
        let (
            stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let address = owner_address(25);
        let expiry = now_unix_seconds() + 3600;
        register_channel_at(&block_engine, "eb_1", address.clone(), expiry, 1);
        register_channel_at(&block_engine, "eb_2", address.clone(), expiry, 2);
        merge_channel_owner_verification(
            stores.get(&1).unwrap(),
            &block_engine,
            &verification_add(SHARD1_FID, address, 10),
        );

        // Two channels, page_size 2: the page fills exactly on the last entry.
        let page1 = channels_by_fid_page(&service, SHARD1_FID, Some(2), None).await;
        assert_eq!(page1.channels.len(), 2);
        assert!(
            page1.next_page_token.is_some(),
            "boundary-aligned page still returns a token"
        );

        // Resuming yields the final empty page with no token.
        let page2 =
            channels_by_fid_page(&service, SHARD1_FID, Some(2), page1.next_page_token.clone())
                .await;
        assert!(page2.channels.is_empty());
        assert_eq!(page2.next_page_token, None);
    }

    #[tokio::test]
    async fn test_subscribe_rpc() {
        let (
            stores,
            senders,
            _engines,
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

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
        let (
            stores,
            senders,
            _engines,
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

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
        let (
            stores,
            _senders,
            _engines,
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
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
        let (
            stores,
            _senders,
            _engines,
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

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
        let (
            stores,
            _senders,
            _engines,
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

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
        let (
            stores,
            _senders,
            _engines,
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

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
        let (
            stores,
            _senders,
            _engines,
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

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
        let (
            _stores,
            _senders,
            [mut engine1, _engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

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
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let user1_pass = format!("pw1-{now}");
        let user2_pass = format!("pw2-{now}");
        let auth_config = format!("user1:{user1_pass},user2:{user2_pass}");

        let (
            _stores,
            _senders,
            _engines,
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(Some(auth_config), None).await;
        let message = messages_factory::casts::create_cast_add(123, "test", None, None);

        let no_auth_request = Request::new(message.clone());
        // Providing no auth fails
        let response = service.submit_message(no_auth_request).await.unwrap_err();
        assert_eq!(response.code(), tonic::Code::Unauthenticated);
        assert_eq!(response.message(), "missing authorization header");

        let mut invalid_creds_request = Request::new(message.clone());
        add_auth_header(&mut invalid_creds_request, "user3", &user1_pass);
        let response = service
            .submit_message(invalid_creds_request)
            .await
            .unwrap_err();
        assert_eq!(response.code(), tonic::Code::Unauthenticated);
        assert_eq!(response.message(), "invalid username or password");

        let mut valid_creds_request = Request::new(message.clone());
        add_auth_header(&mut valid_creds_request, "user2", &user2_pass);
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

    // The mesh diagnostic endpoints are admin-gated, and that gate must run
    // BEFORE the response cache — a cached view/topology must never reach an
    // unauthenticated caller. We can't easily prime the cache with a success in
    // this harness (the gossip receiver is dropped), but asserting that an
    // unauthenticated call returns Unauthenticated (rather than a cached value
    // or an internal gossip error) proves the auth check is ordered first.
    #[tokio::test]
    async fn test_mesh_endpoints_authenticate_before_cache() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let admin = format!("admin:secret-{now}");
        let (
            _stores,
            _senders,
            _engines,
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, Some(admin)).await;

        let mesh_req = || {
            Request::new(proto::GetMeshViewRequest {
                validators_only: true,
                ttl: 0,
                visited_peer_ids: vec![],
            })
        };

        // No credentials -> Unauthenticated (not a cached value, not internal).
        assert_eq!(
            service.get_mesh_view(mesh_req()).await.unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
        assert_eq!(
            service
                .get_mesh_topology(mesh_req())
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );

        // Wrong credentials -> still Unauthenticated.
        let mut bad = mesh_req();
        add_auth_header(&mut bad, "admin", "wrong");
        assert_eq!(
            service.get_mesh_view(bad).await.unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
    }

    // Tests for submit_bulk_messages RPC endpoint

    #[tokio::test]
    async fn test_submit_bulk_messages_empty() {
        let (
            _stores,
            _senders,
            _engines,
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

        // Test submitting 0 messages
        let response = submit_bulk_messages(&service, vec![]).await.unwrap();
        let messages = response.into_inner().messages;
        assert_eq!(messages.len(), 0);
    }

    #[tokio::test]
    async fn test_submit_bulk_messages_valid() {
        let (
            _stores,
            _senders,
            [mut engine1, _engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

        // Register a user
        register_user(
            SHARD1_FID,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine1,
        )
        .await;

        // Create 2 valid messages with different text to avoid duplicates
        let message1 = messages_factory::casts::create_cast_add(SHARD1_FID, "test1", None, None);
        let message2 = messages_factory::casts::create_cast_add(SHARD1_FID, "test2", None, None);
        let messages = vec![message1, message2];

        let response = submit_bulk_messages(&service, messages).await.unwrap();
        let responses = response.into_inner().messages;

        assert_eq!(responses.len(), 2);

        // We're expecting both messages to succeed (upto adding to the mempool, which isn't setup in this test here)
        // So, we assert for "unavailable - Error adding to mempool"
        for (i, response) in responses.iter().enumerate() {
            assert!(response.response.is_some());
            match response.response.as_ref().unwrap() {
                proto::bulk_message_response::Response::Message(_) => {
                    // Succeeds
                }
                proto::bulk_message_response::Response::MessageError(err) => {
                    // We expect mempool error for valid messages in this test setup
                    assert_eq!(err.err_code, "unavailable");
                    assert!(
                        err.message.contains("Error adding to mempool"),
                        "Message {} should fail with mempool error, got: {}",
                        i,
                        err.message
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn test_submit_bulk_messages_mixed() {
        let (
            _stores,
            _senders,
            [mut engine1, _engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

        // Register a user for the valid message
        register_user(
            SHARD1_FID,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine1,
        )
        .await;

        // Create 1 valid message and 1 invalid message (unknown fid)
        let valid_message =
            messages_factory::casts::create_cast_add(SHARD1_FID, "test", None, None);
        let invalid_message = messages_factory::casts::create_cast_add(123, "test", None, None);
        let messages = vec![valid_message, invalid_message];

        let response = submit_bulk_messages(&service, messages).await.unwrap();
        let responses = response.into_inner().messages;

        assert_eq!(responses.len(), 2);

        // First message should fail with mempool error (but passes validation)
        assert!(responses[0].response.is_some());
        match &responses[0].response {
            Some(proto::bulk_message_response::Response::Message(_)) => {
                // Succeeds
            }
            Some(proto::bulk_message_response::Response::MessageError(error)) => {
                // Should fail with mempool error, not validation error
                assert_eq!(error.err_code, "unavailable");
                assert!(error.message.contains("Error adding to mempool"));
            }
            None => panic!("Response should not be None"),
        }

        // Second message should fail
        assert!(responses[1].response.is_some());
        match &responses[1].response {
            Some(proto::bulk_message_response::Response::MessageError(error)) => {
                assert_eq!(error.err_code, "bad_request.validation_failure");
                assert_eq!(error.message, "unknown fid");
            }
            Some(proto::bulk_message_response::Response::Message(_)) => {
                panic!("Expected second message to fail");
            }
            None => panic!("Response should not be None"),
        }
    }

    #[tokio::test]
    async fn test_event_timestamp() {
        let (
            _stores,
            _senders,
            [mut engine1, _engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

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
        let (
            _stores,
            _senders,
            [mut engine1, mut _engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
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
        let (
            _stores,
            _senders,
            [mut engine1, mut _engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
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
        let result = submit_message(&service, user_data_add.clone()).await;
        assert_eq!(result.unwrap().into_inner(), user_data_add);
    }

    // Mock fname registry that simulates the side-effect of a real registry
    // poll: the lookup itself commits the transfer to the engine, mirroring the
    // consensus path (mempool -> block -> engine merge) that the unit test
    // fixture doesn't run. The recovery path under test still pushes the
    // transfer through the mempool; the commit-on-lookup is what ensures the
    // poll loop eventually sees the proof in the store.
    struct MockFnameLookup {
        engine: Arc<tokio::sync::Mutex<ShardEngine>>,
        transfer: FnameTransfer,
    }

    #[async_trait]
    impl FnameTransferLookup for MockFnameLookup {
        async fn lookup_fname(&self, _fname: &str) -> Result<Vec<FnameTransfer>, FetchError> {
            let mut engine = self.engine.lock().await;
            test_helper::commit_fname_transfer(&mut *engine, &self.transfer).await;
            Ok(vec![self.transfer.clone()])
        }
    }

    #[tokio::test]
    async fn test_user_data_add_username_recovers_when_fname_transfer_pending() {
        let (
            _stores,
            _senders,
            [_engine1, engine2],
            _block_engine,
            mut service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

        // FID_FOR_TEST is 1234 — even, so EvenOddRouterForTest routes it to shard 2,
        // which is backed by engine2's RocksDB.
        let fid = test_helper::FID_FOR_TEST;
        let fname = "acp".to_string();
        let owner = hex::decode("711aa8ec273dae42e51732fe1be2b15ee53b00a4").unwrap();

        // Hardcoded fname transfer signed by the default fname signer, lifted
        // from test_merge_fname so we don't have to spin up a custom signer.
        let target_transfer = FnameTransfer {
            id: 1234,
            from_fid: 0,
            proof: Some(UserNameProof {
                timestamp: 1660233642,
                name: fname.as_bytes().to_vec(),
                owner: owner.clone(),
                signature: hex::decode("ebd1b040a4961c5ea751e8ec867d4af6fdbf80ade6775d33dad94ab1c0423dc64a2f684d0e48b89f2958a2385b91743647161ade04e6628a166b5bd1579d86ff1b").unwrap(),
                fid,
                r#type: UserNameType::UsernameTypeFname as i32,
            }),
        };

        let engine2 = Arc::new(tokio::sync::Mutex::new(engine2));
        {
            let mut engine = engine2.lock().await;
            test_helper::register_user(
                fid,
                test_helper::default_signer(),
                owner.clone(),
                &mut *engine,
            )
            .await;
        }

        let user_data_add = messages_factory::user_data::create_user_data_add(
            fid,
            UserDataType::Username,
            &fname,
            None,
            None,
        );

        // Without recovery wired, submit_message fails because the fname proof
        // hasn't been ingested yet — the race condition this fix targets.
        let err = submit_message(&service, user_data_add.clone())
            .await
            .expect_err("expected MissingFname error");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message().contains("fname is not registered"),
            "unexpected error message: {}",
            err.message()
        );

        // Wire up the mock lookup. The next submit_message should detect
        // MissingFname, drive the lookup (which lands the proof), and succeed
        // on retry within the recovery budget.
        service.set_fname_lookup_for_test(Arc::new(MockFnameLookup {
            engine: engine2.clone(),
            transfer: target_transfer.clone(),
        }));

        let response = submit_message(&service, user_data_add.clone())
            .await
            .expect("submit_message should succeed after MissingFname recovery");
        assert_eq!(response.into_inner(), user_data_add);
    }

    #[tokio::test]
    async fn test_ens_proof_with_bad_owner() {
        let (
            _stores,
            _senders,
            [mut engine1, mut _engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
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
        let (
            _stores,
            _senders,
            [mut engine1, mut _engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
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
        let (
            _stores,
            _senders,
            [mut _engine1, mut engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
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

        // This read-path test needs a historical data-shard verification row, not a new direct
        // submission. V20 rejects the latter by design, so seed the store as pre-activation state.
        merge_verification(&engine2.get_stores(), &verification_add);

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
        let (
            _stores,
            _senders,
            [mut engine1, mut engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
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
        let (
            _stores,
            _senders,
            [mut engine1, mut engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
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
        let (
            _stores,
            _senders,
            [mut engine1, _engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

        let response = service
            .get_current_storage_limits_by_fid(fid_request(SHARD1_FID))
            .await
            .unwrap();
        assert_eq!(response.get_ref().units, 0);
        assert_eq!(response.get_ref().limits.len(), 7);
        for limit in response.get_ref().limits.iter() {
            assert_eq!(limit.limit, 0);
            assert_eq!(limit.used, 0);
        }
        assert_eq!(response.get_ref().unit_details.len(), 3);
        assert_eq!(response.get_ref().unit_details[0].unit_size, 0);
        assert_eq!(response.get_ref().unit_details[1].unit_size, 0);
        assert_eq!(response.get_ref().unit_details[2].unit_size, 0);

        test_helper::register_user(
            SHARD1_FID,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine1,
        )
        .await;
        // register_user will give the user a single unit of 2025 storage, let add one more legacy unit and a 2024 unit for 1 of each.
        test_helper::commit_event(
            &mut engine1,
            &events_factory::create_rent_event(
                SHARD1_FID,
                1,
                StorageUnitType::UnitTypeLegacy,
                false,
                FarcasterNetwork::Devnet,
            ),
        )
        .await;
        test_helper::commit_event(
            &mut engine1,
            &events_factory::create_rent_event(
                SHARD1_FID,
                1,
                StorageUnitType::UnitType2024,
                false,
                FarcasterNetwork::Devnet,
            ),
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
            .get_current_storage_limits_by_fid(fid_request(SHARD1_FID))
            .await
            .unwrap();
        assert_eq!(response.get_ref().units, 3);
        assert_eq!(response.get_ref().unit_details.len(), 3);
        assert_eq!(response.get_ref().unit_details[0].unit_size, 1);
        assert_eq!(
            response.get_ref().unit_details[0].unit_type,
            proto::StorageUnitType::UnitTypeLegacy as i32
        );
        assert_eq!(
            response.get_ref().unit_details[1].unit_type,
            proto::StorageUnitType::UnitType2024 as i32
        );
        assert_eq!(response.get_ref().unit_details[1].unit_size, 1);
        assert_eq!(
            response.get_ref().unit_details[2].unit_type,
            proto::StorageUnitType::UnitType2025 as i32
        );
        assert_eq!(response.get_ref().unit_details[2].unit_size, 1);

        let casts_limit = response
            .get_ref()
            .limits
            .iter()
            .filter(|limit| limit.store_type() == proto::StoreType::Casts)
            .collect::<Vec<_>>()[0];
        let configured_limits = engine1.get_stores().store_limits;
        assert_eq!(
            casts_limit.limit as u32,
            (configured_limits
                .for_type(proto::StorageUnitType::UnitType2024)
                .casts
                * 2)
                + (configured_limits
                    .for_type(proto::StorageUnitType::UnitTypeLegacy)
                    .casts)
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

        let storage_lends_limit = response
            .get_ref()
            .limits
            .iter()
            .filter(|limit| limit.store_type() == proto::StoreType::StorageLends)
            .collect::<Vec<_>>()[0];
        assert_eq!(storage_lends_limit.limit, 3);
        assert_eq!(storage_lends_limit.used, 0);
    }

    #[tokio::test]
    async fn test_get_info() {
        let (
            _stores,
            _senders,
            [mut engine1, _engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

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
        let (
            _stores,
            _senders,
            [mut engine1, _engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
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
        let (
            _stores,
            _senders,
            [mut engine1, mut engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

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
        let (
            _stores,
            _senders,
            [mut engine1, _engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
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
        let (
            _stores,
            _senders,
            [mut engine1, _engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
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
        let (
            _stores,
            _senders,
            [mut engine1, _engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let fid = SHARD1_FID;
        let signer = test_helper::default_signer();
        let owner = test_helper::default_custody_address();

        // Register user to create signer event
        test_helper::register_user(fid, signer.clone(), owner.clone(), &mut engine1).await;

        let removed_signer = generate_signer();
        let add_signer_event = events_factory::create_signer_event(
            fid,
            removed_signer.clone(),
            proto::SignerEventType::Add,
            None,
            None,
        );
        commit_event(&mut engine1, &add_signer_event).await;

        // Remove 1 signer
        let mut remove_signer_event = events_factory::create_signer_event(
            fid,
            removed_signer,
            proto::SignerEventType::Remove,
            None,
            None,
        );
        remove_signer_event.block_number = add_signer_event.block_number + 1;
        remove_signer_event.block_timestamp = add_signer_event.block_timestamp + 1;
        commit_event(&mut engine1, &remove_signer_event).await;

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
        let paginated_response = service.get_on_chain_signers_by_fid(request).await.unwrap();
        let events = paginated_response.get_ref().events.clone();
        // only 2 keys total, non-signer key is not returned, removed key is not returned
        if events.len() != 1 {
            // Re-query without pagination to distinguish "store missing the second add" from
            // "pagination dropped it" — this test has flaked here historically.
            let all_response = service
                .get_on_chain_signers_by_fid(Request::new(FidRequest {
                    fid,
                    page_size: None,
                    page_token: None,
                    reverse: None,
                }))
                .await
                .unwrap();
            let all_events = &all_response.get_ref().events;
            panic!(
                "expected 1 paginated event for fid={fid}, got {}. \
                 Paginated types={:?}, block_numbers={:?}. \
                 Full set has {} events: types={:?}, block_numbers={:?}",
                events.len(),
                events.iter().map(|e| e.r#type()).collect::<Vec<_>>(),
                events.iter().map(|e| e.block_number).collect::<Vec<_>>(),
                all_events.len(),
                all_events.iter().map(|e| e.r#type()).collect::<Vec<_>>(),
                all_events
                    .iter()
                    .map(|e| e.block_number)
                    .collect::<Vec<_>>(),
            );
        }
        assert!(events
            .iter()
            .all(|event| event.r#type() == OnChainEventType::EventTypeSigner));
    }

    // NEYN-10578 — unified GetSigner / GetSignersByFid surface.
    //
    // The gasless-key paths are exercised by writing records directly to the shard's
    // DB via `put_gasless_key_record` + `put_gasless_key_owner`, mirroring the seam
    // used by `key_add_store_tests`. This bypasses `merge_key_add` (which requires
    // a fully-signed EIP-712 KEY_ADD); the merge path itself is covered separately
    // by `gasless_key_merge_tests`. What we want here is to assert the RPC surface
    // joins the two stores correctly and shapes the response per the proto.
    #[tokio::test]
    async fn test_get_signer_returns_onchain_record() {
        let (
            stores,
            _senders,
            [mut engine1, _engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let fid = SHARD1_FID;
        let signer_key = test_helper::default_signer();
        let signer_pubkey = signer_key.verifying_key().as_bytes().to_vec();
        register_user(
            fid,
            signer_key,
            test_helper::default_custody_address(),
            &mut engine1,
        )
        .await;
        let _ = stores;

        let request = Request::new(proto::SignerRequest {
            fid,
            signer: signer_pubkey.clone(),
        });
        let response = service.get_signer(request).await.unwrap();
        let signer = response.into_inner().signer.expect("signer present");
        assert_eq!(signer.source, proto::SignerSource::Onchain as i32);
        assert_eq!(signer.key, signer_pubkey);
        assert_eq!(signer.fid, fid);
        assert!(signer.added_at.is_some(), "added_at should be populated");
        assert!(signer.last_used_at.is_none());
        assert!(signer.scopes.is_empty());
        assert!(signer.onchain_event.is_some());
    }

    #[tokio::test]
    async fn test_get_signer_returns_gasless_record() {
        use crate::proto::{message_data::Body, KeyAddBody, MessageData, MessageType};
        use crate::storage::store::account::{
            put_gasless_key_owner, put_gasless_key_record, GaslessKeyRecord,
        };

        let (
            stores,
            _senders,
            _engines,
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let fid = SHARD1_FID;
        let envelope = generate_signer();
        let pubkey: Vec<u8> = envelope.verifying_key().as_bytes().to_vec();

        let key_add = KeyAddBody {
            key: pubkey.clone(),
            key_type: 1,
            custody_signature: vec![0u8; 65],
            deadline: 1_700_000_000,
            nonce: 4,
            metadata: vec![],
            metadata_type: 1,
            registration_tx_hash: vec![],
            scopes: vec![MessageType::CastAdd as i32, MessageType::ReactionAdd as i32],
            ttl: 86_400,
        };
        let message = proto::Message {
            data: Some(MessageData {
                r#type: MessageType::KeyAdd as i32,
                fid,
                timestamp: 100_000,
                network: proto::FarcasterNetwork::Devnet as i32,
                body: Some(Body::KeyAddBody(key_add)),
            }),
            hash: vec![0xAB; 20],
            hash_scheme: 1,
            signature: vec![0u8; 64],
            signature_scheme: 2,
            signer: pubkey.clone(),
            data_bytes: None,
        };
        let record = GaslessKeyRecord {
            message: Some(message),
            request_fid: 9152,
        };

        let shard_stores = stores.get(&1).expect("shard 1 stores");
        let mut txn = RocksDbTransactionBatch::new();
        put_gasless_key_record(&shard_stores.db, &mut txn, fid, &pubkey, &record).unwrap();
        put_gasless_key_owner(&shard_stores.db, &mut txn, &pubkey, fid).unwrap();
        shard_stores.db.commit(txn).unwrap();

        let response = service
            .get_signer(Request::new(proto::SignerRequest {
                fid,
                signer: pubkey.clone(),
            }))
            .await
            .unwrap();
        let signer = response.into_inner().signer.expect("signer present");
        assert_eq!(signer.source, proto::SignerSource::Offchain as i32);
        assert_eq!(signer.key, pubkey);
        assert_eq!(signer.ttl, Some(86_400));
        assert_eq!(signer.nonce, Some(4));
        assert_eq!(signer.request_fid, Some(9152));
        // `added_at` is reported as Unix epoch seconds. The KEY_ADD message was
        // stamped at Farcaster-time second 100_000, so the unified Signer
        // surfaces FARCASTER_EPOCH/1000 + 100_000.
        assert_eq!(
            signer.added_at,
            Some(100_000 + crate::core::types::FARCASTER_EPOCH / 1000)
        );
        assert_eq!(
            signer.scopes,
            vec![MessageType::CastAdd as i32, MessageType::ReactionAdd as i32,]
        );
        // No CAST has been merged through this key, so last_used_at and the
        // computed expires_at should both be absent.
        assert!(signer.last_used_at.is_none());
        assert!(signer.expires_at.is_none());
        assert!(signer.onchain_event.is_none());
    }

    #[tokio::test]
    async fn test_get_signers_by_fid_unions_onchain_and_gasless() {
        use crate::proto::{message_data::Body, KeyAddBody, MessageData, MessageType};
        use crate::storage::store::account::{
            increment_gasless_key_count, put_gasless_key_owner, put_gasless_key_record,
            GaslessKeyRecord,
        };

        let (
            stores,
            _senders,
            [mut engine1, _engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let fid = SHARD1_FID;

        let onchain_key = test_helper::default_signer();
        let onchain_pubkey = onchain_key.verifying_key().as_bytes().to_vec();
        register_user(
            fid,
            onchain_key,
            test_helper::default_custody_address(),
            &mut engine1,
        )
        .await;

        let gasless_envelope = generate_signer();
        let gasless_pubkey: Vec<u8> = gasless_envelope.verifying_key().as_bytes().to_vec();
        let key_add = KeyAddBody {
            key: gasless_pubkey.clone(),
            key_type: 1,
            custody_signature: vec![0u8; 65],
            deadline: 1_700_000_000,
            nonce: 1,
            metadata: vec![],
            metadata_type: 1,
            registration_tx_hash: vec![],
            scopes: vec![MessageType::CastAdd as i32],
            ttl: 3_600,
        };
        let record = GaslessKeyRecord {
            message: Some(proto::Message {
                data: Some(MessageData {
                    r#type: MessageType::KeyAdd as i32,
                    fid,
                    timestamp: 50_000,
                    network: proto::FarcasterNetwork::Devnet as i32,
                    body: Some(Body::KeyAddBody(key_add)),
                }),
                hash: vec![0xCD; 20],
                hash_scheme: 1,
                signature: vec![0u8; 64],
                signature_scheme: 2,
                signer: gasless_pubkey.clone(),
                data_bytes: None,
            }),
            request_fid: 7777,
        };
        let shard_stores = stores.get(&1).expect("shard 1 stores");
        let mut txn = RocksDbTransactionBatch::new();
        put_gasless_key_record(&shard_stores.db, &mut txn, fid, &gasless_pubkey, &record).unwrap();
        put_gasless_key_owner(&shard_stores.db, &mut txn, &gasless_pubkey, fid).unwrap();
        // Bump the per-FID gasless counter to mirror what `merge_key_add` would do —
        // the RPC reads it directly and exposes it as `gasless_signer_count`.
        increment_gasless_key_count(&shard_stores.db, &mut txn, fid).unwrap();
        shard_stores.db.commit(txn).unwrap();

        let response = service
            .get_signers_by_fid(Request::new(SignersByFidRequest {
                fid,
                page_size: None,
                page_token: None,
                reverse: None,
                requester_fids: vec![],
            }))
            .await
            .unwrap();
        let body = response.into_inner();
        let signers = &body.signers;
        assert_eq!(body.gasless_signer_count, 1);
        assert_eq!(
            body.gasless_signer_limit,
            crate::core::validations::key::MAX_GASLESS_KEYS_PER_FID
        );

        // Expect at least the on-chain signer + the gasless key. Other on-chain
        // events written by `register_user` (e.g. storage rent) are filtered out
        // upstream by `get_signers`, so the on-chain side carries exactly one
        // entry — the registered signer.
        assert!(signers
            .iter()
            .any(|s| s.source == proto::SignerSource::Onchain as i32 && s.key == onchain_pubkey));
        let gasless = signers
            .iter()
            .find(|s| s.source == proto::SignerSource::Offchain as i32 && s.key == gasless_pubkey)
            .expect("gasless signer in unified response");
        assert_eq!(gasless.ttl, Some(3_600));
        assert_eq!(gasless.scopes, vec![MessageType::CastAdd as i32]);
        assert_eq!(gasless.request_fid, Some(7_777));

        // Counter store wasn't touched by the direct put above, so both
        // namespaces are absent and the API surfaces 0 / empty.
        assert_eq!(body.current_user_nonce, 0);
        assert!(body.requester_fid_nonces.is_empty());
    }

    #[tokio::test]
    async fn test_get_signers_by_fid_surfaces_nonces() {
        use crate::storage::store::account::{check_and_set_app_nonce, check_and_set_user_nonce};

        let (
            stores,
            _senders,
            _engines,
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let fid = SHARD1_FID;
        let requester_fid: u64 = 7_777;
        let shard_stores = stores.get(&1).expect("shard 1 stores");

        // No counter activity yet — `current_user_nonce` defaults to 0 and
        // `requester_fid_nonces` stays empty unless the request opts in.
        let response = service
            .get_signers_by_fid(Request::new(SignersByFidRequest {
                fid,
                page_size: None,
                page_token: None,
                reverse: None,
                requester_fids: vec![],
            }))
            .await
            .unwrap();
        let body = response.into_inner();
        assert_eq!(body.current_user_nonce, 0);
        assert!(body.requester_fid_nonces.is_empty());

        // Opting in with a requester_fid still returns 0 when the app-nonce
        // counter has no entry for that requester. The response carries one
        // entry naming the requested fid.
        let response = service
            .get_signers_by_fid(Request::new(SignersByFidRequest {
                fid,
                page_size: None,
                page_token: None,
                reverse: None,
                requester_fids: vec![requester_fid],
            }))
            .await
            .unwrap();
        let body = response.into_inner();
        assert_eq!(body.current_user_nonce, 0);
        assert_eq!(body.requester_fid_nonces.len(), 1);
        assert_eq!(body.requester_fid_nonces.get(&requester_fid), Some(&0));

        // Advance both counters to simulate prior KEY_ADD / self-revoke
        // activity. Reading them back through the RPC should reflect the
        // exact stored values, including across a subsequent revocation
        // (the counter persists even when the per-key record is gone).
        let mut txn = RocksDbTransactionBatch::new();
        check_and_set_user_nonce(&shard_stores.db, &mut txn, fid, 3).unwrap();
        check_and_set_app_nonce(&shard_stores.db, &mut txn, requester_fid, 9).unwrap();
        shard_stores.db.commit(txn).unwrap();

        let response = service
            .get_signers_by_fid(Request::new(SignersByFidRequest {
                fid,
                page_size: None,
                page_token: None,
                reverse: None,
                requester_fids: vec![requester_fid],
            }))
            .await
            .unwrap();
        let body = response.into_inner();
        assert_eq!(body.current_user_nonce, 3);
        assert_eq!(body.requester_fid_nonces.len(), 1);
        assert_eq!(body.requester_fid_nonces.get(&requester_fid), Some(&9));

        // Omitting `requester_fids` on the next call still surfaces the
        // user-nonce, but suppresses the per-requester list.
        let response = service
            .get_signers_by_fid(Request::new(SignersByFidRequest {
                fid,
                page_size: None,
                page_token: None,
                reverse: None,
                requester_fids: vec![],
            }))
            .await
            .unwrap();
        let body = response.into_inner();
        assert_eq!(body.current_user_nonce, 3);
        assert!(body.requester_fid_nonces.is_empty());

        // Batched lookup: request multiple requester FIDs in one call. The
        // response carries one entry per supplied FID in the same order, with
        // unknown counters reported as 0.
        let other_requester: u64 = 8_888;
        let response = service
            .get_signers_by_fid(Request::new(SignersByFidRequest {
                fid,
                page_size: None,
                page_token: None,
                reverse: None,
                requester_fids: vec![requester_fid, other_requester],
            }))
            .await
            .unwrap();
        let body = response.into_inner();
        assert_eq!(body.requester_fid_nonces.len(), 2);
        assert_eq!(body.requester_fid_nonces.get(&requester_fid), Some(&9));
        assert_eq!(body.requester_fid_nonces.get(&other_requester), Some(&0));
    }

    #[tokio::test]
    async fn test_submit_storage_lending_message() {
        let (
            _stores,
            _senders,
            _engines,
            mut block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

        block_engine_test_helpers::register_user(
            SHARD1_FID,
            block_engine_test_helpers::default_signer(),
            block_engine_test_helpers::default_custody_address(),
            2,
            &mut block_engine,
        );

        // Create a storage lend message from SHARD1_FID to SHARD2_FID
        let storage_lend_message = messages_factory::storage_lend::create_storage_lend(
            SHARD1_FID,
            SHARD2_FID,
            1, // units
            proto::StorageUnitType::UnitType2025,
            None,
            None,
        );

        let response = submit_message(&service, storage_lend_message.clone()).await;
        assert_eq!(response.unwrap().into_inner(), storage_lend_message);

        // Confirm that the mempool processes the message correctly and that the message can be pulled out
        let messages = block_engine
            .mempool_poller
            .pull_messages(Duration::from_millis(100))
            .await
            .unwrap();

        let state_change = block_engine.propose_state_change(
            messages,
            block_engine.get_confirmed_height().increment(),
            None,
        );

        assert_eq!(
            state_change.transactions[0].user_messages[0],
            storage_lend_message
        )
    }

    #[tokio::test]
    async fn test_verification_activation_pipeline_preserves_primary_address_ownership() {
        let (
            _stores,
            _senders,
            [_engine1, mut data_engine],
            mut block_engine,
            service,
            shard_decision_tx,
            block_decision_tx,
        ) = make_server(None, None).await;
        let fid = 2u64; // EvenOddRouterForTest routes this FID's data to shard 2.
        let signer = test_helper::default_signer();
        let custody = test_helper::default_custody_address();
        block_engine_test_helpers::register_user(
            fid,
            signer.clone(),
            custody.clone(),
            1,
            &mut block_engine,
        );
        test_helper::register_user(fid, signer, custody, &mut data_engine).await;

        let timestamp = messages_factory::farcaster_time();
        let address = hex::decode("91031dcfdea024b4d51e775486111d2b2a715871").unwrap();
        let verification_add = messages_factory::verifications::create_verification_add(
            fid,
            0,
            address.clone(),
            hex::decode("b72c63d61f075b36fb66a9a867b50836cef19d653a3c09005628738677bcb25f25b6b6e6d2e1d69cd725327b3c020deef9e2575a22dc8ed08f88bc75718ce1cb1c").unwrap(),
            hex::decode("d74860c4bbf574d5ad60f03a478a30f990e05ac723e138a5c860cdb3095f4296").unwrap(),
            Some(timestamp),
            None,
        );

        // Submit through the real RPC routing/simulation path, then prove the message was queued
        // on shard 0 (where floor + quota admission run) rather than on the FID shard.
        assert_eq!(
            submit_message(&service, verification_add.clone())
                .await
                .unwrap()
                .into_inner(),
            verification_add
        );
        let block_messages = block_engine
            .mempool_poller
            .pull_messages(Duration::from_millis(100))
            .await
            .unwrap();
        assert!(matches!(
            block_messages.as_slice(),
            [crate::storage::store::mempool_poller::MempoolMessage::UserMessage(message)]
                if message == &verification_add
        ));

        let add_height = block_engine.get_confirmed_height().increment();
        let add_state = block_engine.propose_state_change(block_messages, add_height, None);
        let add_block = block_engine_test_helpers::validate_and_commit_state_change(
            &mut block_engine,
            &add_state,
        );
        let _ = block_decision_tx.send(add_block.clone());
        let verification_events = add_block
            .events
            .iter()
            .filter(|event| {
                matches!(
                    event.data.as_ref().and_then(|data| data.body.as_ref()),
                    Some(proto::block_event_data::Body::MergeMessageEventBody(body))
                        if body.message.as_ref() == Some(&verification_add)
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(verification_events.len(), 1);

        // Fan the shard-0 block events into the FID shard through its public replay path.
        test_helper::commit_block_events(&mut data_engine, add_block.events.iter().collect()).await;
        assert_eq!(
            data_engine.get_verifications_by_fid(fid).unwrap().messages,
            vec![verification_add.clone()]
        );

        // The primary-address write is admitted only because replay populated the FID shard's
        // local verification store.
        let checksummed = alloy_primitives::Address::from_slice(&address).to_checksum(None);
        let primary_address = messages_factory::user_data::create_user_data_add(
            fid,
            UserDataType::UserDataPrimaryAddressEthereum,
            &checksummed,
            Some(timestamp + 1),
            None,
        );
        submit_message(&service, primary_address.clone())
            .await
            .unwrap();
        let data_messages = data_engine
            .mempool_poller
            .pull_messages(Duration::from_millis(100))
            .await
            .unwrap();
        let primary_state =
            data_engine.propose_state_change(data_engine.shard_id(), data_messages, None);
        let primary_chunk =
            test_helper::validate_and_commit_state_change(&mut data_engine, &primary_state).await;
        let _ = shard_decision_tx.send(primary_chunk);
        assert!(test_helper::message_exists_in_trie(
            &mut data_engine,
            &primary_address,
        ));

        let verification_remove = messages_factory::verifications::create_verification_remove(
            fid,
            address,
            Some(timestamp + 2),
            None,
        );
        submit_message(&service, verification_remove.clone())
            .await
            .unwrap();
        let remove_messages = block_engine
            .mempool_poller
            .pull_messages(Duration::from_millis(100))
            .await
            .unwrap();
        let remove_height = block_engine.get_confirmed_height().increment();
        let remove_state = block_engine.propose_state_change(remove_messages, remove_height, None);
        let remove_block = block_engine_test_helpers::validate_and_commit_state_change(
            &mut block_engine,
            &remove_state,
        );
        let _ = block_decision_tx.send(remove_block.clone());
        test_helper::commit_block_events(&mut data_engine, remove_block.events.iter().collect())
            .await;

        assert!(data_engine
            .get_user_data_by_fid_and_type(fid, UserDataType::UserDataPrimaryAddressEthereum,)
            .is_err());
        assert!(!test_helper::message_exists_in_trie(
            &mut data_engine,
            &primary_address,
        ));
    }

    #[tokio::test]
    async fn test_get_all_lend_storage_messages_by_fid() {
        let (
            _stores,
            _senders,
            [_engine1, _engine2],
            mut block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        block_engine_test_helpers::register_user(
            SHARD1_FID,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            5,
            &mut block_engine,
        );

        // Create storage lend messages
        let lend1 = messages_factory::storage_lend::create_storage_lend(
            SHARD1_FID,
            SHARD1_FID + 1,
            1,
            proto::StorageUnitType::UnitType2025,
            None,
            None,
        );
        let lend2 = messages_factory::storage_lend::create_storage_lend(
            SHARD1_FID,
            SHARD2_FID + 2,
            2,
            proto::StorageUnitType::UnitType2025,
            Some(lend1.data.as_ref().unwrap().timestamp + 10),
            None,
        );

        block_engine_test_helpers::commit_message(&mut block_engine, &lend1, Validity::Valid);
        block_engine_test_helpers::commit_message(&mut block_engine, &lend2, Validity::Valid);

        // Test pagination
        let request = proto::FidTimestampRequest {
            fid: SHARD1_FID,
            page_size: Some(1),
            page_token: None,
            reverse: None,
            start_timestamp: None,
            stop_timestamp: None,
        };
        let response = service
            .get_all_lend_storage_messages_by_fid(Request::new(request))
            .await;
        test_helper::assert_contains_all_messages(&response, &[&lend1]);

        // Test getting all messages
        let request = proto::FidTimestampRequest {
            fid: SHARD1_FID,
            page_size: None,
            page_token: None,
            reverse: None,
            start_timestamp: None,
            stop_timestamp: None,
        };
        let response = service
            .get_all_lend_storage_messages_by_fid(Request::new(request))
            .await;
        test_helper::assert_contains_all_messages(&response, &[&lend1, &lend2]);

        // Test reverse order
        let request = proto::FidTimestampRequest {
            fid: SHARD1_FID,
            page_size: Some(1),
            page_token: None,
            reverse: Some(true),
            start_timestamp: None,
            stop_timestamp: None,
        };
        let response = service
            .get_all_lend_storage_messages_by_fid(Request::new(request))
            .await;
        test_helper::assert_contains_all_messages(&response, &[&lend2]);
    }
}
