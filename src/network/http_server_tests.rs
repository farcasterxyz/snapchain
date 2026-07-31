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

        async fn get_channel_owner(
            &self,
            _request: Request<ChannelOwnerRequest>,
        ) -> Result<Response<ChannelOwnerResponse>, Status> {
            let response = ChannelOwnerResponse::default();
            Ok(Response::new(response))
        }

        async fn get_channels_by_address(
            &self,
            _request: Request<ChannelsByAddressRequest>,
        ) -> Result<Response<ChannelsResponse>, Status> {
            let response = ChannelsResponse::default();
            Ok(Response::new(response))
        }

        async fn get_channels_by_fid(
            &self,
            _request: Request<ChannelsByFidRequest>,
        ) -> Result<Response<ChannelsResponse>, Status> {
            let response = ChannelsResponse::default();
            Ok(Response::new(response))
        }

        async fn get_channel_member(
            &self,
            _request: Request<ChannelMemberRequest>,
        ) -> Result<Response<ChannelMemberResponse>, Status> {
            Ok(Response::new(ChannelMemberResponse::default()))
        }

        async fn get_channel_members(
            &self,
            _request: Request<ChannelMembersRequest>,
        ) -> Result<Response<ChannelMembersResponse>, Status> {
            Ok(Response::new(ChannelMembersResponse::default()))
        }

        async fn get_channel_pin(
            &self,
            _request: Request<ChannelRequest>,
        ) -> Result<Response<ChannelPinResponse>, Status> {
            Ok(Response::new(ChannelPinResponse::default()))
        }

        async fn get_channel_moderations(
            &self,
            _request: Request<ChannelModerationsRequest>,
        ) -> Result<Response<ChannelModerationsResponse>, Status> {
            Ok(Response::new(ChannelModerationsResponse::default()))
        }

        async fn get_channel_metadata(
            &self,
            _request: Request<ChannelRequest>,
        ) -> Result<Response<ChannelMetadataResponse>, Status> {
            Ok(Response::new(ChannelMetadataResponse::default()))
        }

        async fn get_channel_memberships_by_fid(
            &self,
            _request: Request<ChannelMembershipsByFidRequest>,
        ) -> Result<Response<ChannelMembershipsResponse>, Status> {
            Ok(Response::new(ChannelMembershipsResponse::default()))
        }

        async fn get_channel_followers(
            &self,
            _request: Request<crate::proto::ChannelFollowersRequest>,
        ) -> Result<Response<crate::proto::ChannelFollowersResponse>, Status> {
            Ok(Response::new(
                crate::proto::ChannelFollowersResponse::default(),
            ))
        }

        async fn get_channel_follower_count(
            &self,
            _request: Request<crate::proto::ChannelFollowerCountRequest>,
        ) -> Result<Response<crate::proto::ChannelFollowerCountResponse>, Status> {
            Ok(Response::new(
                crate::proto::ChannelFollowerCountResponse::default(),
            ))
        }

        async fn get_channel_follows(
            &self,
            _request: Request<crate::proto::ChannelFollowsRequest>,
        ) -> Result<Response<crate::proto::ChannelFollowsResponse>, Status> {
            Ok(Response::new(
                crate::proto::ChannelFollowsResponse::default(),
            ))
        }

        async fn is_following_channel(
            &self,
            _request: Request<crate::proto::IsFollowingChannelRequest>,
        ) -> Result<Response<crate::proto::IsFollowingChannelResponse>, Status> {
            Ok(Response::new(
                crate::proto::IsFollowingChannelResponse::default(),
            ))
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

    /// Pins the JSON-on-the-wire shape for `ChannelOwnerResponse`. The address
    /// field must serialize as a "0x"-prefixed lowercase hex string under the
    /// camelCase key `ownerAddress` (via `serdehex` + `rename`); `fid` and
    /// `expiry` are plain numbers. The stub-based HTTP test elsewhere only
    /// exercises the default (all-zero) response, so this pins the encoding of a
    /// real 20-byte address.
    #[test]
    fn channel_owner_response_json_shape() {
        use crate::network::http_server::ChannelOwnerResponse;

        let response = ChannelOwnerResponse {
            fid: 1234,
            owner_address: vec![0x11u8; 20],
            expiry: 1_700_000_000,
        };

        let json = serde_json::to_value(&response).expect("serialize");
        assert_eq!(json["fid"], 1234);
        assert_eq!(json["expiry"], 1_700_000_000u64);
        assert_eq!(
            json["ownerAddress"],
            serde_json::json!(format!("0x{}", "11".repeat(20)))
        );
        // The raw snake_case key must not leak onto the wire.
        assert!(json.get("owner_address").is_none());
    }

    #[test]
    fn channel_message_read_response_json_shapes() {
        use crate::network::http_server::{
            CastingMode, ChannelMemberResponse, ChannelMemberState, ChannelMetadataResponse,
            ChannelPin, ChannelPinResponse, MembershipMode,
        };

        let member = serde_json::to_value(ChannelMemberResponse {
            state: ChannelMemberState::CHANNEL_MEMBER_STATE_MODERATOR,
            last_action_ts: Some(42),
        })
        .unwrap();
        assert_eq!(member["state"], "CHANNEL_MEMBER_STATE_MODERATOR");
        assert_eq!(member["lastActionTs"], 42);

        let pin = serde_json::to_value(ChannelPinResponse {
            pin: Some(ChannelPin {
                cast_hash: vec![0xAB; 20],
                author_fid: 123,
            }),
        })
        .unwrap();
        assert_eq!(pin["pin"]["castHash"], format!("0x{}", "ab".repeat(20)));
        assert_eq!(pin["pin"]["authorFid"], 123);

        // No pin is one absent object, not two independently absent fields — a
        // castHash without an authorFid is unrepresentable.
        let unpinned = serde_json::to_value(ChannelPinResponse { pin: None }).unwrap();
        assert_eq!(unpinned["pin"], serde_json::Value::Null);

        let metadata = serde_json::to_value(ChannelMetadataResponse {
            name: None,
            description: None,
            image_url: None,
            header: None,
            rules: None,
            casting_mode: CastingMode::CASTING_MODE_MEMBERS_ONLY,
            membership_mode: MembershipMode::MEMBERSHIP_MODE_APPROVAL,
        })
        .unwrap();
        assert_eq!(metadata["castingMode"], "CASTING_MODE_MEMBERS_ONLY");
        assert_eq!(metadata["membershipMode"], "MEMBERSHIP_MODE_APPROVAL");
        // ChannelUpdate is a whole-replace fold, so an absent field means CLEARED.
        // These must serialize as explicit null rather than being omitted, or a
        // consumer merging the response into a stored model cannot tell a cleared
        // description from one that never existed and keeps deleted metadata.
        for field in ["name", "description", "imageUrl", "header", "rules"] {
            assert_eq!(
                metadata.get(field),
                Some(&serde_json::Value::Null),
                "{field} must serialize as null when cleared, not be omitted"
            );
        }

        let populated = serde_json::to_value(ChannelMetadataResponse {
            name: Some("Channel name".to_string()),
            description: None,
            image_url: None,
            header: None,
            rules: None,
            casting_mode: CastingMode::CASTING_MODE_EVERYONE,
            membership_mode: MembershipMode::MEMBERSHIP_MODE_OPEN,
        })
        .unwrap();
        assert_eq!(populated["name"], "Channel name");
        assert_eq!(populated["description"], serde_json::Value::Null);
    }

    #[test]
    fn channel_page_tokens_survive_the_http_base64_round_trip() {
        use crate::network::http_server::{ChannelMembersRequest, ChannelMembershipsByFidRequest};
        use base64::prelude::*;

        // A channel page token is a raw RocksDB key that reaches the client as base64
        // and comes back through query-string deserialization. The store now REJECTS a
        // token that does not start with the requested index's prefix, so a byte
        // mangled in that round trip stops being "a slightly wrong page" and becomes a
        // hard invalid_argument. Only the encode side had any coverage.
        let channel_id_hex = format!("0x{}", "11".repeat(32));

        // Percent-encode the base64 characters that are reserved in a query string.
        let query_escape = |value: &str| {
            value
                .replace('%', "%25")
                .replace('+', "%2B")
                .replace('/', "%2F")
                .replace('=', "%3D")
        };

        // A realistic member-slot key: RootPrefix::Channel, MemberSlot, 32-byte channel
        // id, 4-byte fid, with byte values chosen so the base64 uses the `+` and `/`
        // alphabet and needs padding.
        let mut token = vec![24u8, 2];
        token.extend_from_slice(&[0xFBu8; 32]);
        token.extend_from_slice(&909u32.to_be_bytes());
        let encoded = BASE64_STANDARD.encode(&token);
        assert!(
            encoded.contains('+') && encoded.contains('/') && encoded.ends_with('='),
            "fixture must exercise the +, / and padding cases: {encoded}"
        );

        // Both spellings are accepted and folded together by `to_proto`, so both have
        // to decode to the same bytes.
        for field in ["page_token", "pageToken"] {
            let parsed: ChannelMembersRequest = serde_qs::from_str(&format!(
                "channelId={channel_id_hex}&{field}={}",
                query_escape(&encoded)
            ))
            .unwrap();
            assert_eq!(
                parsed.page_token.or(parsed.pageToken).as_deref(),
                Some(token.as_slice()),
                "{field} must round-trip to the exact key bytes"
            );
        }

        // A raw `+` in a query string arrives as a space, which is why the decoder
        // restores it. Without that, every token containing `+` would come back
        // corrupted — and now rejected outright by the prefix guard.
        let space_mangled: ChannelMembersRequest = serde_qs::from_str(&format!(
            "channelId={channel_id_hex}&pageToken={}",
            query_escape(&encoded.replace('+', " "))
        ))
        .unwrap();
        assert_eq!(
            space_mangled
                .page_token
                .or(space_mangled.pageToken)
                .as_deref(),
            Some(token.as_slice())
        );

        // Empty and absent both mean "first page". They must not become Some(vec![]),
        // which the store treats as out-of-prefix and rejects.
        for query in [
            format!("channelId={channel_id_hex}&pageToken="),
            format!("channelId={channel_id_hex}"),
        ] {
            let parsed: ChannelMembersRequest = serde_qs::from_str(&query).unwrap();
            assert_eq!(parsed.page_token.or(parsed.pageToken), None, "{query}");
        }

        // Undecodable input fails deserialization rather than silently becoming None,
        // which would page from the start of the index instead of reporting the error.
        assert!(serde_qs::from_str::<ChannelMembersRequest>(&format!(
            "channelId={channel_id_hex}&pageToken=%21%21not-base64%21%21"
        ))
        .is_err());

        // Same contract on the fid-keyed read, whose tokens key a different index.
        let memberships: ChannelMembershipsByFidRequest =
            serde_qs::from_str(&format!("fid=909&pageToken={}", query_escape(&encoded))).unwrap();
        assert_eq!(
            memberships.page_token.or(memberships.pageToken).as_deref(),
            Some(token.as_slice())
        );
    }

    #[test]
    fn channel_read_errors_separate_server_faults_from_caller_input() {
        use crate::network::http_server::ErrorResponse;
        use hyper::StatusCode;

        // The channel reads sit on a brand-new, integrity-sensitive index, and
        // `handle_request` maps every handler error to 400 unless told otherwise.
        // Without this split an operator watching 4xx/5xx during a replica-corruption
        // incident sees "channel slot points to a missing message" as client traffic.
        for code in [
            tonic::Code::Internal,
            tonic::Code::DataLoss,
            tonic::Code::Unknown,
        ] {
            let err = ErrorResponse::from_status(
                &Status::new(code, "channel slot points to a missing message"),
                "Failed to get channel members",
            );
            assert_eq!(
                err.status,
                Some(StatusCode::INTERNAL_SERVER_ERROR),
                "{code:?} is a server fault"
            );
            assert_eq!(err.error, "Failed to get channel members");
        }

        // Caller-supplied input keeps 400: a foreign page token, a malformed
        // channel_id, an unregistered channel.
        for code in [
            tonic::Code::InvalidArgument,
            tonic::Code::NotFound,
            tonic::Code::PermissionDenied,
        ] {
            let err = ErrorResponse::from_status(
                &Status::new(code, "page token does not belong to this channel"),
                "Failed to get channel members",
            );
            assert_eq!(err.status, None, "{code:?} is caller input");
        }

        // The status is transport-only and must never leak into the JSON body.
        let body = serde_json::to_value(ErrorResponse::from_status(
            &Status::internal("boom"),
            "Failed to get channel pin",
        ))
        .unwrap();
        assert!(body.get("status").is_none());
        assert_eq!(body["error"], "Failed to get channel pin");
    }

    #[test]
    fn channel_members_request_accepts_camel_and_snake_case_state_filter() {
        use crate::network::http_server::{ChannelMemberState, ChannelMembersRequest};

        let channel_id = format!("0x{}", "11".repeat(32));
        let camel: ChannelMembersRequest = serde_qs::from_str(&format!(
            "channelId={channel_id}&stateFilter=CHANNEL_MEMBER_STATE_MODERATOR"
        ))
        .unwrap();
        assert!(matches!(
            camel.state_filter,
            Some(ChannelMemberState::CHANNEL_MEMBER_STATE_MODERATOR)
        ));

        let snake: ChannelMembersRequest = serde_qs::from_str(&format!(
            "channelId={channel_id}&state_filter=CHANNEL_MEMBER_STATE_BANNED"
        ))
        .unwrap();
        assert!(matches!(
            snake.state_filter,
            Some(ChannelMemberState::CHANNEL_MEMBER_STATE_BANNED)
        ));
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

    #[test]
    fn channel_follow_request_shapes_deserialize() {
        use crate::network::http_server::{
            ChannelFollowerCountRequest, ChannelFollowersRequest, ChannelFollowsRequest,
            IsFollowingChannelRequest,
        };
        use base64::prelude::*;

        let channel_id_hex = format!("0x{}", "a1".repeat(32));
        let query_escape = |value: &str| {
            value
                .replace('%', "%25")
                .replace('+', "%2B")
                .replace('/', "%2F")
                .replace('=', "%3D")
        };

        // The fan-out cursor is base64 of a JSON envelope rather than a raw
        // RocksDB key, which changes the hazard profile from the channel-member
        // token this mirrors. Base64 of JSON is alphanumeric-dominated — a search
        // over realistic cursors found none producing both `+` and `/` — so that
        // part of the query-string mangling is structurally unlikely here. The
        // padding case is not: `=` is reserved in a query string and appears
        // whenever the payload length is not a multiple of 3. And the failure is
        // harsher than a raw key's: one mangled byte makes the JSON unparseable,
        // so the client gets a hard 400 mid-pagination rather than a wrong page.
        let cursor = br#"[{"shard_id":1,"scan":{"Resume":[25,2,161,161]}},{"shard_id":2,"scan":"Exhausted"}]"#;
        let encoded = BASE64_STANDARD.encode(cursor);
        assert!(
            encoded.ends_with('='),
            "fixture must exercise base64 padding: {encoded}"
        );

        let parsed: ChannelFollowersRequest = serde_qs::from_str(&format!(
            "channelId={channel_id_hex}&pageSize=25&pageToken={}",
            query_escape(&encoded)
        ))
        .unwrap();
        assert_eq!(parsed.channel_id, vec![0xa1u8; 32]);
        assert_eq!(parsed.page_size.or(parsed.pageSize), Some(25));
        assert_eq!(
            parsed.page_token.or(parsed.pageToken).as_deref(),
            Some(&cursor[..]),
            "cursor must survive the base64 + query-string round trip byte for byte"
        );

        // Both spellings of the paging fields must reach the proto — they are folded
        // with `.or()`, so a regression that drops one is invisible from the other.
        let snake: ChannelFollowersRequest = serde_qs::from_str(&format!(
            "channelId={channel_id_hex}&page_size=7&page_token={}",
            query_escape(&encoded)
        ))
        .unwrap();
        assert_eq!(snake.page_size.or(snake.pageSize), Some(7));
        assert_eq!(
            snake.page_token.or(snake.pageToken).as_deref(),
            Some(&cursor[..])
        );

        // An absent token must be None, never Some(vec![]) — an empty token is
        // out-of-prefix by construction and the store rejects it.
        let bare: ChannelFollowersRequest =
            serde_qs::from_str(&format!("channelId={channel_id_hex}")).unwrap();
        assert_eq!(bare.page_token.or(bare.pageToken), None);

        // channelId is accepted with and without the 0x prefix.
        let unprefixed: ChannelFollowerCountRequest =
            serde_qs::from_str(&format!("channelId={}", "a1".repeat(32))).unwrap();
        assert_eq!(unprefixed.channel_id, vec![0xa1u8; 32]);

        let follows: ChannelFollowsRequest = serde_qs::from_str("fid=1234&pageSize=3").unwrap();
        assert_eq!(follows.fid, 1234);
        assert_eq!(follows.page_size.or(follows.pageSize), Some(3));

        let is_following: IsFollowingChannelRequest =
            serde_qs::from_str(&format!("fid=1234&channelId={channel_id_hex}")).unwrap();
        assert_eq!(is_following.fid, 1234);
        assert_eq!(is_following.channel_id, vec![0xa1u8; 32]);
    }

    #[test]
    fn channel_follow_response_shapes_serialize() {
        use crate::network::http_server::{
            ChannelFollow, ChannelFollower, ChannelFollowersResponse, ChannelFollowsResponse,
            IsFollowingChannelResponse,
        };

        let followers = ChannelFollowersResponse {
            followers: vec![ChannelFollower {
                fid: 1234,
                followed_at: 99,
            }],
            next_page_token: None,
        };
        let json = serde_json::to_value(&followers).unwrap();
        assert_eq!(json["followers"][0]["followedAt"], 99);
        assert!(
            json.get("nextPageToken").is_none(),
            "an absent token must be omitted, not serialized as null"
        );

        // channel_id goes out as hex on HTTP, unlike the gRPC bytes.
        let follows = ChannelFollowsResponse {
            follows: vec![ChannelFollow {
                channel_id: vec![0xa1u8; 32],
                followed_at: 7,
            }],
            next_page_token: Some("dG9rZW4=".to_string()),
        };
        let json = serde_json::to_value(&follows).unwrap();
        assert_eq!(
            json["follows"][0]["channelId"],
            format!("0x{}", "a1".repeat(32))
        );
        assert_eq!(json["nextPageToken"], "dG9rZW4=");

        // The documented contract: followedAt is present exactly when following.
        let yes = serde_json::to_value(&IsFollowingChannelResponse {
            following: true,
            followed_at: Some(42),
        })
        .unwrap();
        assert_eq!(yes["following"], true);
        assert_eq!(yes["followedAt"], 42);

        let no = serde_json::to_value(&IsFollowingChannelResponse {
            following: false,
            followed_at: None,
        })
        .unwrap();
        assert_eq!(no["following"], false);
        assert!(no.get("followedAt").is_none());
    }
}
