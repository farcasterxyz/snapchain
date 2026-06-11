#[cfg(test)]
pub mod tests {
    use async_trait::async_trait;
    use std::{collections::HashMap, sync::Arc};
    use tokio::sync::Mutex;
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::{Request, Response, Status};

    use crate::{
        core::types::FARCASTER_EPOCH,
        network::http_server::{HubHttpService, HubHttpServiceImpl},
        proto::{hub_service_server::HubService, *},
    };

    #[derive(Clone)]
    pub struct MockHubService {
        current_peers: Option<GetConnectedPeersResponse>,
        pub call_counts: Arc<Mutex<HashMap<String, usize>>>,
    }

    impl MockHubService {
        pub fn new() -> Self {
            Self {
                current_peers: None,
                call_counts: Arc::new(Mutex::new(HashMap::new())),
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

        async fn get_mesh_view(
            &self,
            _request: Request<GetMeshViewRequest>,
        ) -> Result<Response<MeshView>, Status> {
            Ok(Response::new(MeshView::default()))
        }

        async fn get_mesh_topology(
            &self,
            _request: Request<GetMeshViewRequest>,
        ) -> Result<Response<MeshTopology>, Status> {
            Ok(Response::new(MeshTopology::default()))
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

        async fn get_signer(
            &self,
            _request: Request<SignerRequest>,
        ) -> Result<Response<SignerResponse>, Status> {
            Ok(Response::new(SignerResponse::default()))
        }

        async fn get_signers_by_fid(
            &self,
            _request: Request<SignersByFidRequest>,
        ) -> Result<Response<SignersByFidResponse>, Status> {
            Ok(Response::new(SignersByFidResponse::default()))
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

    /// Pins the JSON-on-the-wire shape for `SignersByFidResponse`. The proto
    /// uses `map<uint64, uint32>` for `requester_fid_nonces`; this test
    /// verifies the HTTP layer serializes that as a JSON object keyed by the
    /// stringified FID (not an array of `{fid, nonce}` pairs), so clients can
    /// look up a requester's nonce directly from the parsed body.
    #[test]
    fn signers_by_fid_response_json_shape() {
        use crate::network::http_server::{Signer, SignersByFidResponse};
        use std::collections::HashMap;

        let mut requester_fid_nonces = HashMap::new();
        requester_fid_nonces.insert(7_777u64, 9u32);
        requester_fid_nonces.insert(8_888u64, 0u32);

        let response = SignersByFidResponse {
            signers: Vec::<Signer>::new(),
            next_page_token: None,
            gasless_signer_count: 0,
            gasless_signer_limit: 1000,
            current_user_nonce: 3,
            requester_fid_nonces,
        };

        let json = serde_json::to_value(&response).expect("serialize");
        assert_eq!(json["currentUserNonce"], 3);
        let nonces = json
            .get("requesterFidNonces")
            .expect("requesterFidNonces present");
        // Map shape: a JSON object whose keys are stringified FIDs. If this
        // ever serialized as an array, `as_object()` would return None.
        let nonces = nonces.as_object().expect("requesterFidNonces is a map");
        assert_eq!(nonces.len(), 2);
        assert_eq!(nonces.get("7777"), Some(&serde_json::json!(9)));
        assert_eq!(nonces.get("8888"), Some(&serde_json::json!(0)));
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
            }],
            peers: vec![],
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
              "timestamp": 1609459200000
            }
          ],
          "peers": []
        }
        "#);
    }
}
