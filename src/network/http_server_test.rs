#[cfg(test)]
pub mod tests {
    use async_trait::async_trait;
    use std::{collections::HashMap, sync::Arc};
    use tokio::sync::Mutex;
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::{Request, Response, Status};

    use crate::{
        core::types::FARCASTER_EPOCH,
        network::http_server::{
            AddressLookupRequest as HttpAddressLookupRequest, HubHttpService, HubHttpServiceImpl,
            NameLookupRequest as HttpNameLookupRequest,
        },
        proto::{hub_service_server::HubService, *},
    };

    #[derive(Clone)]
    pub struct MockHubService {
        current_peers: Option<GetConnectedPeersResponse>,
        pub call_counts: Arc<Mutex<HashMap<String, usize>>>,
        pub fid_lookup_response: FidResponse,
        pub name_to_address_response: NameToAddressResponse,
        pub address_lookup_response: AddressToFidResponse,
    }

    impl MockHubService {
        pub fn new() -> Self {
            Self {
                current_peers: None,
                call_counts: Arc::new(Mutex::new(HashMap::new())),
                fid_lookup_response: FidResponse { fid: 0 },
                name_to_address_response: NameToAddressResponse {
                    fid: 0,
                    custody_address: None,
                    connected_addresses: vec![],
                },
                address_lookup_response: AddressToFidResponse { matches: vec![] },
            }
        }
    }

    #[async_trait]
    impl HubService for MockHubService {
        async fn submit_message(
            &self,
            _request: Request<Message>,
        ) -> Result<Response<Message>, Status> {
            let message = Message::default();
            Ok(Response::new(message))
        }

        async fn submit_bulk_messages(
            &self,
            _request: Request<SubmitBulkMessagesRequest>,
        ) -> Result<Response<SubmitBulkMessagesResponse>, Status> {
            let response = SubmitBulkMessagesResponse::default();
            Ok(Response::new(response))
        }

        async fn validate_message(
            &self,
            _request: Request<Message>,
        ) -> Result<Response<ValidationResponse>, Status> {
            let response = ValidationResponse::default();
            Ok(Response::new(response))
        }

        type GetBlocksStream = ReceiverStream<Result<Block, Status>>;
        async fn get_blocks(
            &self,
            _request: Request<BlocksRequest>,
        ) -> Result<Response<Self::GetBlocksStream>, Status> {
            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            self.call_counts
                .lock()
                .await
                .entry("get_blocks".to_string())
                .and_modify(|count| *count += 1)
                .or_insert(1);
            Ok(Response::new(ReceiverStream::new(rx)))
        }

        async fn get_shard_chunks(
            &self,
            _request: Request<ShardChunksRequest>,
        ) -> Result<Response<ShardChunksResponse>, Status> {
            let response = ShardChunksResponse::default();
            Ok(Response::new(response))
        }

        async fn get_info(
            &self,
            _request: Request<GetInfoRequest>,
        ) -> Result<Response<GetInfoResponse>, Status> {
            let response = GetInfoResponse::default();
            Ok(Response::new(response))
        }

        async fn get_fids(
            &self,
            _request: Request<FidsRequest>,
        ) -> Result<Response<FidsResponse>, Status> {
            let response = FidsResponse::default();
            Ok(Response::new(response))
        }

        async fn get_connected_peers(
            &self,
            _request: Request<GetConnectedPeersRequest>,
        ) -> Result<Response<GetConnectedPeersResponse>, Status> {
            let response = self
                .current_peers
                .clone()
                .unwrap_or(GetConnectedPeersResponse::default());
            Ok(Response::new(response))
        }

        type SubscribeStream = ReceiverStream<Result<HubEvent, Status>>;
        async fn subscribe(
            &self,
            _request: Request<SubscribeRequest>,
        ) -> Result<Response<Self::SubscribeStream>, Status> {
            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            Ok(Response::new(ReceiverStream::new(rx)))
        }

        async fn get_event(
            &self,
            _request: Request<EventRequest>,
        ) -> Result<Response<HubEvent>, Status> {
            let event = HubEvent::default();
            Ok(Response::new(event))
        }

        async fn get_events(
            &self,
            _request: Request<EventsRequest>,
        ) -> Result<Response<EventsResponse>, Status> {
            let response = EventsResponse::default();
            Ok(Response::new(response))
        }

        async fn get_cast(&self, _request: Request<CastId>) -> Result<Response<Message>, Status> {
            let message = Message::default();
            Ok(Response::new(message))
        }

        async fn get_casts_by_fid(
            &self,
            _request: Request<FidRequest>,
        ) -> Result<Response<MessagesResponse>, Status> {
            let response = MessagesResponse::default();
            Ok(Response::new(response))
        }

        async fn get_casts_by_parent(
            &self,
            _request: Request<CastsByParentRequest>,
        ) -> Result<Response<MessagesResponse>, Status> {
            let response = MessagesResponse::default();
            Ok(Response::new(response))
        }

        async fn get_casts_by_mention(
            &self,
            _request: Request<FidRequest>,
        ) -> Result<Response<MessagesResponse>, Status> {
            let response = MessagesResponse::default();
            Ok(Response::new(response))
        }

        async fn get_reaction(
            &self,
            _request: Request<ReactionRequest>,
        ) -> Result<Response<Message>, Status> {
            let message = Message::default();
            Ok(Response::new(message))
        }

        async fn get_reactions_by_fid(
            &self,
            _request: Request<ReactionsByFidRequest>,
        ) -> Result<Response<MessagesResponse>, Status> {
            let response = MessagesResponse::default();
            Ok(Response::new(response))
        }

        async fn get_reactions_by_cast(
            &self,
            _request: Request<ReactionsByTargetRequest>,
        ) -> Result<Response<MessagesResponse>, Status> {
            let response = MessagesResponse::default();
            Ok(Response::new(response))
        }

        async fn get_reactions_by_target(
            &self,
            _request: Request<ReactionsByTargetRequest>,
        ) -> Result<Response<MessagesResponse>, Status> {
            let response = MessagesResponse::default();
            Ok(Response::new(response))
        }

        async fn get_user_data(
            &self,
            _request: Request<UserDataRequest>,
        ) -> Result<Response<Message>, Status> {
            let message = Message::default();
            Ok(Response::new(message))
        }

        async fn get_user_data_by_fid(
            &self,
            _request: Request<FidRequest>,
        ) -> Result<Response<MessagesResponse>, Status> {
            let response = MessagesResponse::default();
            Ok(Response::new(response))
        }

        async fn get_username_proof(
            &self,
            _request: Request<UsernameProofRequest>,
        ) -> Result<Response<UserNameProof>, Status> {
            let proof = UserNameProof::default();
            Ok(Response::new(proof))
        }

        async fn get_user_name_proofs_by_fid(
            &self,
            _request: Request<FidRequest>,
        ) -> Result<Response<UsernameProofsResponse>, Status> {
            let response = UsernameProofsResponse::default();
            Ok(Response::new(response))
        }

        async fn get_fid_by_name(
            &self,
            _request: Request<NameLookupRequest>,
        ) -> Result<Response<FidResponse>, Status> {
            Ok(Response::new(self.fid_lookup_response.clone()))
        }

        async fn get_addresses_by_name(
            &self,
            _request: Request<NameLookupRequest>,
        ) -> Result<Response<NameToAddressResponse>, Status> {
            Ok(Response::new(self.name_to_address_response.clone()))
        }

        async fn get_fid_by_address(
            &self,
            _request: Request<AddressLookupRequest>,
        ) -> Result<Response<AddressToFidResponse>, Status> {
            Ok(Response::new(self.address_lookup_response.clone()))
        }

        async fn get_verification(
            &self,
            _request: Request<VerificationRequest>,
        ) -> Result<Response<Message>, Status> {
            let message = Message::default();
            Ok(Response::new(message))
        }

        async fn get_verifications_by_fid(
            &self,
            _request: Request<FidRequest>,
        ) -> Result<Response<MessagesResponse>, Status> {
            let response = MessagesResponse::default();
            Ok(Response::new(response))
        }

        async fn get_on_chain_signer(
            &self,
            _request: Request<SignerRequest>,
        ) -> Result<Response<OnChainEvent>, Status> {
            let event = OnChainEvent::default();
            Ok(Response::new(event))
        }

        async fn get_on_chain_signers_by_fid(
            &self,
            _request: Request<FidRequest>,
        ) -> Result<Response<OnChainEventResponse>, Status> {
            let response = OnChainEventResponse::default();
            Ok(Response::new(response))
        }

        async fn get_on_chain_events(
            &self,
            _request: Request<OnChainEventRequest>,
        ) -> Result<Response<OnChainEventResponse>, Status> {
            let response = OnChainEventResponse::default();
            Ok(Response::new(response))
        }

        async fn get_id_registry_on_chain_event(
            &self,
            _request: Request<FidRequest>,
        ) -> Result<Response<OnChainEvent>, Status> {
            let event = OnChainEvent::default();
            Ok(Response::new(event))
        }

        async fn get_id_registry_on_chain_event_by_address(
            &self,
            _request: Request<IdRegistryEventByAddressRequest>,
        ) -> Result<Response<OnChainEvent>, Status> {
            let event = OnChainEvent::default();
            Ok(Response::new(event))
        }

        async fn get_current_storage_limits_by_fid(
            &self,
            _request: Request<FidRequest>,
        ) -> Result<Response<StorageLimitsResponse>, Status> {
            let response = StorageLimitsResponse::default();
            Ok(Response::new(response))
        }

        async fn get_fid_address_type(
            &self,
            _request: Request<FidAddressTypeRequest>,
        ) -> Result<Response<FidAddressTypeResponse>, Status> {
            let response = FidAddressTypeResponse::default();
            Ok(Response::new(response))
        }

        async fn get_link(
            &self,
            _request: Request<LinkRequest>,
        ) -> Result<Response<Message>, Status> {
            let message = Message::default();
            Ok(Response::new(message))
        }

        async fn get_links_by_fid(
            &self,
            _request: Request<LinksByFidRequest>,
        ) -> Result<Response<MessagesResponse>, Status> {
            let response = MessagesResponse::default();
            Ok(Response::new(response))
        }

        async fn get_links_by_target(
            &self,
            _request: Request<LinksByTargetRequest>,
        ) -> Result<Response<MessagesResponse>, Status> {
            let response = MessagesResponse::default();
            Ok(Response::new(response))
        }

        async fn get_link_compact_state_message_by_fid(
            &self,
            _request: Request<FidRequest>,
        ) -> Result<Response<MessagesResponse>, Status> {
            let response = MessagesResponse::default();
            Ok(Response::new(response))
        }

        async fn get_all_cast_messages_by_fid(
            &self,
            _request: Request<FidTimestampRequest>,
        ) -> Result<Response<MessagesResponse>, Status> {
            let response = MessagesResponse::default();
            Ok(Response::new(response))
        }

        async fn get_all_reaction_messages_by_fid(
            &self,
            _request: Request<FidTimestampRequest>,
        ) -> Result<Response<MessagesResponse>, Status> {
            let response = MessagesResponse::default();
            Ok(Response::new(response))
        }

        async fn get_all_verification_messages_by_fid(
            &self,
            _request: Request<FidTimestampRequest>,
        ) -> Result<Response<MessagesResponse>, Status> {
            let response = MessagesResponse::default();
            Ok(Response::new(response))
        }

        async fn get_all_user_data_messages_by_fid(
            &self,
            _request: Request<FidTimestampRequest>,
        ) -> Result<Response<MessagesResponse>, Status> {
            let response = MessagesResponse::default();
            Ok(Response::new(response))
        }

        async fn get_all_link_messages_by_fid(
            &self,
            _request: Request<FidTimestampRequest>,
        ) -> Result<Response<MessagesResponse>, Status> {
            let response = MessagesResponse::default();
            Ok(Response::new(response))
        }

        async fn get_all_lend_storage_messages_by_fid(
            &self,
            _request: Request<FidTimestampRequest>,
        ) -> Result<Response<MessagesResponse>, Status> {
            let response = MessagesResponse::default();
            Ok(Response::new(response))
        }

        async fn get_trie_metadata_by_prefix(
            &self,
            _request: Request<TrieNodeMetadataRequest>,
        ) -> Result<Response<TrieNodeMetadataResponse>, Status> {
            let response = TrieNodeMetadataResponse::default();
            Ok(Response::new(response))
        }
    }

    #[tokio::test]
    async fn test_current_peers() {
        let mut mock_hub_service = MockHubService::new();
        mock_hub_service.current_peers = Some(GetConnectedPeersResponse {
            contacts: vec![ContactInfoBody {
                gossip_address: "127.0.0.1:3382".to_string(),
                announce_rpc_address: "http://127.0.0.1:3381".to_string(),
                network: FarcasterNetwork::Mainnet as i32,
                peer_id: vec![
                    0, 36, 8, 1, 18, 32, 113, 33, 69, 101, 159, 234, 6, 137, 235, 52, 28, 108, 100,
                    242, 16, 180, 130, 238, 153, 64, 79, 138, 80, 251, 13, 157, 24, 101, 103, 73,
                    168, 19,
                ],
                snapchain_version: "0.2.1".to_string(),
                timestamp: FARCASTER_EPOCH,
                capabilities: vec![],
            }],
        });
        let http_service = HubHttpServiceImpl {
            service: Arc::new(mock_hub_service),
        };
        let response = http_service
            .get_connected_peers(GetConnectedPeersRequest {})
            .await;

        assert!(response.is_ok());
        insta::assert_json_snapshot!(response.unwrap(), @r#"
        {
          "contacts": [
            {
              "gossip_address": "127.0.0.1:3382",
              "announce_rpc_address": "http://127.0.0.1:3381",
              "peer_id": "12D3KooWHRyfTBKcjkqjNk5UZarJhzT7rXZYfr4DmaCWJgen62Xk",
              "snapchain_version": "0.2.1",
              "network": "Mainnet",
              "timestamp": 1609459200000,
              "capabilities": []
            }
          ]
        }
        "#);
    }

    #[tokio::test]
    async fn test_get_fid_by_name_http_handler() {
        let mut mock_hub_service = MockHubService::new();
        mock_hub_service.fid_lookup_response = FidResponse { fid: 1234 };

        let http_service = HubHttpServiceImpl {
            service: Arc::new(mock_hub_service),
        };

        let response = http_service
            .get_fid_by_name(HttpNameLookupRequest {
                name: "alice".to_string(),
                name_type: Some("fname".to_string()),
            })
            .await
            .unwrap();

        assert_eq!(response.fid, 1234);
    }

    #[tokio::test]
    async fn test_get_addresses_by_name_http_handler() {
        let mut mock_hub_service = MockHubService::new();
        mock_hub_service.name_to_address_response = NameToAddressResponse {
            fid: 9,
            custody_address: Some(vec![0x10, 0x11]),
            connected_addresses: vec![vec![0xaa]],
        };

        let http_service = HubHttpServiceImpl {
            service: Arc::new(mock_hub_service),
        };

        let response = http_service
            .get_addresses_by_name(HttpNameLookupRequest {
                name: "bob".to_string(),
                name_type: Some("fname".to_string()),
            })
            .await
            .unwrap();

        assert_eq!(response.fid, 9);
        assert_eq!(response.custody_address.as_deref(), Some("0x1011"));
        assert_eq!(response.connected_addresses, vec!["0xaa".to_string()]);
    }

    #[tokio::test]
    async fn test_get_fid_by_address_http_handler() {
        let mut mock_hub_service = MockHubService::new();
        mock_hub_service.address_lookup_response = AddressToFidResponse {
            matches: vec![
                AddressMatch {
                    fid: 77,
                    is_custody: true,
                    is_verified: false,
                },
                AddressMatch {
                    fid: 88,
                    is_custody: false,
                    is_verified: true,
                },
            ],
        };

        let http_service = HubHttpServiceImpl {
            service: Arc::new(mock_hub_service),
        };

        let response = http_service
            .get_fid_by_address(HttpAddressLookupRequest {
                address: vec![0xde, 0xad],
            })
            .await
            .unwrap();

        assert_eq!(response.matches.len(), 2);
        assert!(response.matches[0].is_custody);
        assert!(response.matches[1].is_verified);
    }
}
