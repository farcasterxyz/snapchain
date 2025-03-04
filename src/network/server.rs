use super::rpc_extensions::{AsMessagesResponse, AsSingleMessageResponse};
use crate::connectors::onchain_events::L1Client;
use crate::core::error::HubError;
use crate::core::util::get_farcaster_time;
use crate::core::validations;
use crate::core::validations::verification::VerificationAddressClaim;
use crate::mempool::mempool::{MempoolMessageWithSource, MempoolSource};
use crate::mempool::routing;
use crate::proto;
use crate::proto::cast_add_body;
use crate::proto::casts_by_parent_request;
use crate::proto::hub_service_server::HubService;
use crate::proto::link_body;
use crate::proto::links_by_target_request;
use crate::proto::message_data;
use crate::proto::on_chain_event::Body;
use crate::proto::reaction_body;
use crate::proto::reactions_by_target_request;
use crate::proto::CastsByParentRequest;
use crate::proto::FidsRequest;
use crate::proto::FidsResponse;
use crate::proto::GetInfoResponse;
use crate::proto::HubEvent;
use crate::proto::IdRegistryEventByAddressRequest;
use crate::proto::LinksByTargetRequest;
use crate::proto::MessageType;
use crate::proto::OnChainEvent;
use crate::proto::OnChainEventRequest;
use crate::proto::OnChainEventResponse;
use crate::proto::ReactionsByTargetRequest;
use crate::proto::SignerRequest;
use crate::proto::TrieNodeMetadataRequest;
use crate::proto::TrieNodeMetadataResponse;
use crate::proto::UserNameProof;
use crate::proto::UserNameType;
use crate::proto::UsernameProofRequest;
use crate::proto::UsernameProofsResponse;
use crate::proto::VerificationAddAddressBody;
use crate::proto::{Block, CastId, DbStats};
use crate::proto::{BlocksRequest, ShardChunksRequest, ShardChunksResponse, SubscribeRequest};
use crate::proto::{FidRequest, FidTimestampRequest};
use crate::proto::{GetInfoRequest, StorageLimitsResponse};
use crate::proto::{
    LinkRequest, LinksByFidRequest, Message, MessagesResponse, ReactionRequest,
    ReactionsByFidRequest, UserDataRequest, VerificationRequest,
};
use crate::storage::constants::OnChainEventPostfix;
use crate::storage::constants::RootPrefix;
use crate::storage::db::PageOptions;
use crate::storage::db::RocksDbTransactionBatch;
use crate::storage::store::account::message_bytes_decode;
use crate::storage::store::account::MessagesPage;
use crate::storage::store::account::UsernameProofStore;
use crate::storage::store::account::{
    CastStore, LinkStore, ReactionStore, UserDataStore, VerificationStore,
};
use crate::storage::store::engine::{MempoolMessage, Senders, ShardEngine};
use crate::storage::store::stores::Stores;
use crate::storage::store::BlockStore;
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use hex::ToHex;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info};

pub struct MyHubService {
    block_store: BlockStore,
    shard_stores: HashMap<u32, Stores>,
    shard_senders: HashMap<u32, Senders>,
    num_shards: u32,
    message_router: Box<dyn routing::MessageRouter>,
    statsd_client: StatsdClientWrapper,
    l1_client: Option<Box<dyn L1Client>>,
    mempool_tx: mpsc::Sender<MempoolMessageWithSource>,
}

impl MyHubService {
    pub fn new(
        block_store: BlockStore,
        shard_stores: HashMap<u32, Stores>,
        shard_senders: HashMap<u32, Senders>,
        statsd_client: StatsdClientWrapper,
        num_shards: u32,
        message_router: Box<dyn routing::MessageRouter>,
        mempool_tx: mpsc::Sender<MempoolMessageWithSource>,
        l1_client: Option<Box<dyn L1Client>>,
    ) -> Self {
        Self {
            block_store,
            shard_senders,
            shard_stores,
            statsd_client,
            message_router,
            num_shards,
            l1_client,
            mempool_tx,
        }
    }

    async fn submit_message_internal(
        &self,
        message: proto::Message,
        bypass_validation: bool,
    ) -> Result<proto::Message, Status> {
        let fid = message.fid();
        if fid == 0 {
            return Err(Status::invalid_argument(
                "no fid or invalid fid".to_string(),
            ));
        }

        let dst_shard = self.message_router.route_message(fid, self.num_shards);

        let stores = match self.shard_stores.get(&dst_shard) {
            Some(store) => store,
            None => {
                return Err(Status::invalid_argument(
                    "no shard store for fid".to_string(),
                ))
            }
        };

        if !bypass_validation {
            // TODO: This is a hack to get around the fact that self cannot be made mutable
            let mut readonly_engine = ShardEngine::new(
                stores.db.clone(),
                stores.trie.clone(),
                1,
                stores.store_limits.clone(),
                self.statsd_client.clone(),
                100,
                None,
            );
            let result = readonly_engine.simulate_message(&message);

            if let Err(err) = result {
                return Err(Status::invalid_argument(format!(
                    "Invalid message: {}",
                    err.to_string()
                )));
            }

            // We're doing the ens and address validations here for now because we don't want L1 interactions to be on the consensus critical path. Eventually this will move to the fname server.
            if let Some(message_data) = &message.data {
                match &message_data.body {
                    Some(proto::message_data::Body::UserDataBody(user_data)) => {
                        if user_data.r#type() == proto::UserDataType::Username {
                            if user_data.value.ends_with(".eth") {
                                self.validate_ens_username(fid, user_data.value.to_string())
                                    .await?;
                            }
                        };
                    }
                    Some(proto::message_data::Body::UsernameProofBody(proof)) => {
                        if proof.r#type() == UserNameType::UsernameTypeEnsL1 {
                            self.validate_ens_username_proof(fid, &proof).await?;
                        }
                    }
                    Some(proto::message_data::Body::VerificationAddAddressBody(body)) => {
                        if body.verification_type == 1 {
                            // todo: thread through network
                            let claim_result =
                                validations::verification::make_verification_address_claim(
                                    message_data.fid,
                                    &body.address,
                                    proto::FarcasterNetwork::Mainnet,
                                    &body.block_hash,
                                    proto::Protocol::Ethereum,
                                );
                            match claim_result {
                                Ok(claim) => {
                                    self.validate_contract_signature(claim, body).await?;
                                }
                                Err(err) => {
                                    return Err(Status::invalid_argument(format!(
                                        "Invalid message: {}",
                                        err.to_string()
                                    )))
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        match self.mempool_tx.try_send((
            MempoolMessage::UserMessage(message.clone()),
            MempoolSource::RPC,
        )) {
            Ok(_) => {
                self.statsd_client.count("rpc.submit_message.success", 1);
                debug!("successfully submitted message");
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.statsd_client
                    .count("rpc.submit_message.channel_full", 1);
                return Err(Status::resource_exhausted("channel is full"));
            }
            Err(e) => {
                self.statsd_client.count("rpc.submit_message.failure", 1);
                println!("error sending: {:?}", e.to_string());
                return Err(Status::internal("failed to submit message"));
            }
        }

        Ok(message)
    }

    fn get_stores_for_shard(&self, shard_id: u32) -> Result<&Stores, Status> {
        match self.shard_stores.get(&shard_id) {
            Some(store) => Ok(store),
            None => Err(Status::invalid_argument(
                "no shard store for fid".to_string(),
            )),
        }
    }

    fn get_stores_for(&self, fid: u64) -> Result<&Stores, Status> {
        let shard_id = self.message_router.route_message(fid, self.num_shards);
        self.get_stores_for_shard(shard_id)
    }

    pub async fn validate_contract_signature(
        &self,
        claim: VerificationAddressClaim,
        body: &VerificationAddAddressBody,
    ) -> Result<(), Status> {
        match &self.l1_client {
            None => {
                // Fail validation, can be fixed with config change
                Err(Status::invalid_argument(
                    "unable to validate contract signature because there's no l1 client",
                ))
            }
            Some(l1_client) => l1_client
                .verify_contract_signature(claim, body)
                .await
                .or_else(|_| {
                    Err(Status::invalid_argument(
                        "could not verify contract signature",
                    ))
                }),
        }
    }

    pub async fn validate_ens_username_proof(
        &self,
        fid: u64,
        proof: &UserNameProof,
    ) -> Result<(), Status> {
        match &self.l1_client {
            None => {
                // Fail validation, can be fixed with config change
                Err(Status::invalid_argument(
                    "unable to validate ens name because there's no l1 client",
                ))
            }
            Some(l1_client) => {
                let name = std::str::from_utf8(&proof.name)
                    .map_err(|err| Status::from_error(Box::new(err)))?;

                if !name.ends_with(".eth") {
                    return Err(Status::invalid_argument(
                        "invalid ens name, doesn't end with .eth",
                    ));
                }

                let resolved_ens_address = l1_client
                    .resolve_ens_name(name.to_string())
                    .await
                    .map_err(|err| Status::from_error(Box::new(err)))?
                    .to_vec();

                if resolved_ens_address != proof.owner {
                    return Err(Status::invalid_argument(
                        "invalid ens name, resolved address doesn't match proof owner address",
                    ));
                }

                let stores = self
                    .get_stores_for(fid)
                    .map_err(|err| Status::from_error(Box::new(err)))?;

                let id_register = stores
                    .onchain_event_store
                    .get_id_register_event_by_fid(fid)
                    .map_err(|err| Status::from_error(Box::new(err)))?;

                match id_register {
                    None => return Err(Status::invalid_argument("missing id registration")),
                    Some(id_register) => {
                        match id_register.body {
                            Some(Body::IdRegisterEventBody(id_register)) => {
                                // Check verified addresses if the resolved address doesn't match the custody address
                                if id_register.to != resolved_ens_address {
                                    let verification = VerificationStore::get_verification_add(
                                        &stores.verification_store,
                                        fid,
                                        &resolved_ens_address,
                                    )
                                    .map_err(|err| Status::from_error(Box::new(err)))?;

                                    match verification {
                                    None => Err(Status::invalid_argument(
                                        "invalid ens proof, no matching custody address or verified addresses",
                                    )),
                                    Some(_) => Ok(()),
                                }
                                } else {
                                    Ok(())
                                }
                            }
                            _ => return Err(Status::invalid_argument("missing id registration")),
                        }
                    }
                }
            }
        }
    }

    async fn validate_ens_username(&self, fid: u64, name: String) -> Result<(), Status> {
        let stores = self
            .get_stores_for(fid)
            .map_err(|err| Status::from_error(Box::new(err)))?;
        let proof_message = UsernameProofStore::get_username_proof(
            &stores.username_proof_store,
            &name.as_bytes().to_vec(),
            UserNameType::UsernameTypeEnsL1 as u8,
        )
        .map_err(|err| Status::from_error(Box::new(err)))?;
        match proof_message {
            Some(message) => match message.data {
                None => Err(Status::invalid_argument("username proof missing data")),
                Some(message_data) => match message_data.body {
                    Some(body) => match body {
                        proto::message_data::Body::UsernameProofBody(proof) => {
                            self.validate_ens_username_proof(fid, &proof).await
                        }
                        _ => Err(Status::invalid_argument("username proof has wrong type")),
                    },
                    None => Err(Status::invalid_argument("username proof missing body")),
                },
            },
            None => Err(Status::invalid_argument(
                "missing username proof for username",
            )),
        }
    }

    fn rewrite_hub_event(mut hub_event: HubEvent) -> HubEvent {
        match &mut hub_event.body {
            Some(body) => {
                match body {
                    proto::hub_event::Body::MergeMessageBody(merge_message_body) => {
                        match &merge_message_body.message {
                            None => {}
                            Some(message) => {
                                if message.msg_type() == MessageType::LinkCompactState {
                                    // In the case of merging compact state, we omit the deleted messages as this would
                                    // result in an unbounded message size:
                                    merge_message_body.deleted_messages = vec![]
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            None => {}
        };
        hub_event
    }
}

#[tonic::async_trait]
impl HubService for MyHubService {
    async fn submit_message_with_options(
        &self,
        request: Request<proto::SubmitMessageRequest>,
    ) -> Result<Response<proto::SubmitMessageResponse>, Status> {
        let start_time = std::time::Instant::now();

        let hash = request
            .get_ref()
            .message
            .as_ref()
            .map(|msg| msg.hash.encode_hex::<String>())
            .unwrap_or_default();
        debug!(%hash, "Received call to [submit_message_with_options] RPC");

        let proto::SubmitMessageRequest {
            message,
            bypass_validation,
        } = request.into_inner();

        let message = match message {
            Some(msg) => msg,
            None => return Err(Status::invalid_argument("Message is required")),
        };

        let response_message = self
            .submit_message_internal(message, bypass_validation.unwrap_or(false))
            .await?;

        let response = proto::SubmitMessageResponse {
            message: Some(response_message),
        };

        self.statsd_client.time(
            "rpc.submit_message_with_options.duration",
            start_time.elapsed().as_millis() as u64,
        );

        Ok(Response::new(response))
    }

    async fn submit_message(
        &self,
        request: Request<proto::Message>,
    ) -> Result<Response<proto::Message>, Status> {
        let start_time = std::time::Instant::now();

        let hash = request.get_ref().hash.encode_hex::<String>();
        debug!(hash, "Received call to [submit_message] RPC");

        let mut message = request.into_inner();
        message_bytes_decode(&mut message);
        let response_message = self.submit_message_internal(message, false).await?;

        self.statsd_client.time(
            "rpc.submit_message.duration",
            start_time.elapsed().as_millis() as u64,
        );

        Ok(Response::new(response_message))
    }

    type GetBlocksStream = ReceiverStream<Result<Block, Status>>;

    async fn get_blocks(
        &self,
        request: Request<BlocksRequest>,
    ) -> Result<Response<Self::GetBlocksStream>, Status> {
        let start_block_number = request.get_ref().start_block_number;
        let stop_block_number = request.get_ref().stop_block_number;
        // TODO(aditi): Rethink the channel size
        let (server_tx, client_rx) = mpsc::channel::<Result<Block, Status>>(100);

        info!( {start_block_number, stop_block_number}, "Received call to [get_blocks] RPC");

        let block_store = self.block_store.clone();

        tokio::spawn(async move {
            let mut next_page_token = None;
            loop {
                match block_store.get_blocks(
                    start_block_number,
                    stop_block_number,
                    &PageOptions {
                        page_size: Some(100),
                        page_token: next_page_token,
                        reverse: false,
                    },
                ) {
                    Err(err) => {
                        _ = server_tx.send(Err(Status::from_error(Box::new(err)))).await;
                        break;
                    }
                    Ok(block_page) => {
                        for block in block_page.blocks {
                            if let Err(_) = server_tx.send(Ok(block)).await {
                                break;
                            }
                        }

                        if block_page.next_page_token.is_none() {
                            break;
                        } else {
                            next_page_token = block_page.next_page_token;
                        }
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(client_rx)))
    }

    async fn get_shard_chunks(
        &self,
        request: Request<ShardChunksRequest>,
    ) -> Result<Response<ShardChunksResponse>, Status> {
        // TODO(aditi): Write unit tests for these functions.
        let shard_index = request.get_ref().shard_id;
        let start_block_number = request.get_ref().start_block_number;
        let stop_block_number = request.get_ref().stop_block_number;

        info!( {shard_index, start_block_number, stop_block_number},
            "Received call to [get_shard_chunks] RPC");

        let stores = self.shard_stores.get(&shard_index);
        match stores {
            None => Err(Status::from_error(Box::new(
                HubError::invalid_internal_state("Missing shard store"),
            ))),
            Some(stores) => {
                match stores
                    .shard_store
                    .get_shard_chunks(start_block_number, stop_block_number)
                {
                    Err(err) => Err(Status::from_error(Box::new(err))),
                    Ok(shard_chunks) => {
                        let response = Response::new(ShardChunksResponse { shard_chunks });
                        Ok(response)
                    }
                }
            }
        }
    }

    async fn get_info(
        &self,
        _request: Request<GetInfoRequest>,
    ) -> Result<Response<GetInfoResponse>, Status> {
        let mut total_fid_registrations = 0;
        let mut total_approx_size = 0;
        let mut total_num_messages = 0;
        let mut shard_infos = Vec::new();

        let current_time = get_farcaster_time().unwrap_or(0);
        let block_info = proto::ShardInfo {
            shard_id: 0,
            max_height: self.block_store.max_block_number().unwrap_or(0),
            num_messages: 0,
            num_fid_registrations: 0,
            approx_size: self.block_store.db.approximate_size(),
            block_delay: current_time - self.block_store.max_block_timestamp().unwrap_or(0),
        };
        shard_infos.push(block_info);

        for (shard_index, shard_store) in self.shard_stores.iter() {
            let shard_approx_size = shard_store.db.approximate_size();
            let shard_num_messages = shard_store.trie.get_count(
                &shard_store.db,
                &mut RocksDbTransactionBatch::new(),
                &[],
            );
            let shard_fid_registrations = shard_store
                .db
                .count_keys_at_prefix(vec![
                    RootPrefix::OnChainEvent as u8,
                    OnChainEventPostfix::IdRegisterByFid as u8,
                ])
                .map_err(|err| Status::from_error(Box::new(err)))?
                as u64;

            let max_block_time = shard_store.shard_store.max_block_timestamp().unwrap_or(0);

            let info = proto::ShardInfo {
                shard_id: *shard_index,
                max_height: shard_store.shard_store.max_block_number().unwrap_or(0),
                num_messages: shard_num_messages,
                num_fid_registrations: shard_fid_registrations,
                approx_size: shard_approx_size,
                block_delay: current_time - max_block_time,
            };
            shard_infos.push(info);
            total_num_messages += shard_num_messages;
            total_fid_registrations += shard_fid_registrations;
            total_approx_size += shard_approx_size;
        }

        Ok(Response::new(GetInfoResponse {
            db_stats: Some(DbStats {
                num_fid_registrations: total_fid_registrations,
                num_messages: total_num_messages,
                approx_size: total_approx_size,
            }),
            shard_infos,
            num_shards: self.num_shards,
        }))
    }

    type SubscribeStream = ReceiverStream<Result<HubEvent, Status>>;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        // TODO(aditi): Incorporate event types
        info!("Received call to [subscribe] RPC");
        // TODO(aditi): Rethink the channel size
        let (server_tx, client_rx) = mpsc::channel::<Result<HubEvent, Status>>(100);
        let events_txs = match request.get_ref().shard_index {
            Some(shard_id) => match self.shard_senders.get(&(shard_id)) {
                None => {
                    return Err(Status::from_error(Box::new(
                        HubError::invalid_internal_state("Missing shard event tx"),
                    )))
                }
                Some(senders) => vec![senders.events_tx.clone()],
            },
            None => self
                .shard_senders
                .values()
                .map(|senders| senders.events_tx.clone())
                .collect(),
        };

        let shard_stores = match request.get_ref().shard_index {
            Some(shard_id) => {
                vec![self.shard_stores.get(&shard_id).cloned().unwrap()]
            }
            None => self.shard_stores.values().cloned().collect(),
        };

        let start_id = request.get_ref().from_id.unwrap_or(0);

        tokio::spawn(async move {
            let mut page_token = None;
            for store in shard_stores {
                loop {
                    // TODO(aditi): We should stop pulling the raw db out of the shard store and create a new store type for events to house the db.
                    let old_events = HubEvent::get_events(
                        store.shard_store.db.clone(),
                        start_id,
                        None,
                        Some(PageOptions {
                            page_token: page_token.clone(),
                            page_size: None,
                            reverse: false,
                        }),
                    )
                    .unwrap();

                    for event in old_events.events {
                        let event = Self::rewrite_hub_event(event);
                        if let Err(_) = server_tx.send(Ok(event)).await {
                            return;
                        }
                    }

                    page_token = old_events.next_page_token;
                    if page_token.is_none() {
                        break;
                    }
                }
            }

            // TODO(aditi): It's possible that events show up between when we finish reading from the db and the subscription starts. We don't handle this case in the current hub code, but we may want to down the line.
            for event_tx in events_txs {
                let tx = server_tx.clone();
                tokio::spawn(async move {
                    let mut event_rx = event_tx.subscribe();
                    loop {
                        match event_rx.recv().await {
                            Ok(hub_event) => {
                                let hub_event = Self::rewrite_hub_event(hub_event);
                                match tx.send(Ok(hub_event)).await {
                                    Ok(_) => {}
                                    Err(_) => {
                                        // This means the client hung up
                                        break;
                                    }
                                }
                            }
                            Err(err) => {
                                error!(
                                    { err = err.to_string() },
                                    "[subscribe] error receiving from event stream"
                                )
                            }
                        }
                    }
                });
            }
        });

        Ok(Response::new(ReceiverStream::new(client_rx)))
    }

    async fn get_cast(&self, request: Request<CastId>) -> Result<Response<proto::Message>, Status> {
        let cast_id = request.into_inner();
        let stores = self.get_stores_for(cast_id.fid)?;
        CastStore::get_cast_add(&stores.cast_store, cast_id.fid, cast_id.hash).as_response()
    }

    async fn get_casts_by_fid(
        &self,
        request: Request<FidRequest>,
    ) -> Result<Response<proto::MessagesResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let options = request.page_options();
        CastStore::get_cast_adds_by_fid(&stores.cast_store, request.fid, &options).as_response()
    }

    async fn get_all_cast_messages_by_fid(
        &self,
        request: Request<FidTimestampRequest>,
    ) -> Result<Response<proto::MessagesResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let (start_ts, stop_ts) = request.timestamps();
        stores
            .cast_store
            .get_all_messages_by_fid(request.fid, start_ts, stop_ts, &request.page_options())
            .as_response()
    }

    async fn get_reaction(
        &self,
        request: Request<ReactionRequest>,
    ) -> Result<Response<Message>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let target = match request.target {
            Some(proto::reaction_request::Target::TargetCastId(cast_id)) => {
                Some(proto::reaction_body::Target::TargetCastId(cast_id))
            }
            Some(proto::reaction_request::Target::TargetUrl(url)) => {
                Some(proto::reaction_body::Target::TargetUrl(url))
            }
            None => None,
        };
        ReactionStore::get_reaction_add(
            &stores.reaction_store,
            request.fid,
            request.reaction_type,
            target,
        )
        .as_response()
    }

    async fn get_reactions_by_fid(
        &self,
        request: Request<ReactionsByFidRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let options = request.page_options();
        ReactionStore::get_reaction_adds_by_fid(
            &stores.reaction_store,
            request.fid,
            request.reaction_type.unwrap_or(0),
            &options,
        )
        .as_response()
    }

    async fn get_all_reaction_messages_by_fid(
        &self,
        request: Request<FidTimestampRequest>,
    ) -> Result<Response<proto::MessagesResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let (start_ts, stop_ts) = request.timestamps();
        stores
            .reaction_store
            .get_all_messages_by_fid(request.fid, start_ts, stop_ts, &request.page_options())
            .as_response()
    }

    async fn get_link(&self, request: Request<LinkRequest>) -> Result<Response<Message>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let target = match request.target {
            Some(proto::link_request::Target::TargetFid(fid)) => {
                Some(proto::link_body::Target::TargetFid(fid))
            }
            None => None,
        };
        LinkStore::get_link_add(&stores.link_store, request.fid, request.link_type, target)
            .as_response()
    }

    async fn get_links_by_fid(
        &self,
        request: Request<LinksByFidRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let options = request.page_options();
        LinkStore::get_link_adds_by_fid(
            &stores.link_store,
            request.fid,
            request.link_type.unwrap_or("".to_string()),
            &options,
        )
        .as_response()
    }

    async fn get_all_link_messages_by_fid(
        &self,
        request: Request<FidTimestampRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let (start_ts, stop_ts) = request.timestamps();
        stores
            .link_store
            .get_all_messages_by_fid(request.fid, start_ts, stop_ts, &request.page_options())
            .as_response()
    }

    async fn get_user_data(
        &self,
        request: Request<UserDataRequest>,
    ) -> Result<Response<Message>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let user_data_type = proto::UserDataType::try_from(request.user_data_type)
            .map_err(|_| Status::invalid_argument("Invalid user data type"))?;
        UserDataStore::get_user_data_by_fid_and_type(
            &stores.user_data_store,
            request.fid,
            user_data_type,
        )
        .as_response()
    }

    async fn get_user_data_by_fid(
        &self,
        request: Request<FidRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let options = request.page_options();
        UserDataStore::get_user_data_adds_by_fid(
            &stores.user_data_store,
            request.fid,
            &options,
            None,
            None,
        )
        .as_response()
    }

    async fn get_all_user_data_messages_by_fid(
        &self,
        request: Request<FidTimestampRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let (start_ts, stop_ts) = request.timestamps();
        stores
            .user_data_store
            .get_all_messages_by_fid(request.fid, start_ts, stop_ts, &request.page_options())
            .as_response()
    }

    async fn get_verification(
        &self,
        request: Request<VerificationRequest>,
    ) -> Result<Response<Message>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        VerificationStore::get_verification_add(
            &stores.verification_store,
            request.fid,
            &request.address,
        )
        .as_response()
    }

    async fn get_verifications_by_fid(
        &self,
        request: Request<FidRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let options = request.page_options();
        VerificationStore::get_verification_adds_by_fid(
            &stores.verification_store,
            request.fid,
            &options,
        )
        .as_response()
    }

    async fn get_all_verification_messages_by_fid(
        &self,
        request: Request<FidTimestampRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let (start_ts, stop_ts) = request.timestamps();
        stores
            .verification_store
            .get_all_messages_by_fid(request.fid, start_ts, stop_ts, &request.page_options())
            .as_response()
    }

    async fn get_link_compact_state_message_by_fid(
        &self,
        request: Request<FidRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let options = request.page_options();
        LinkStore::get_link_compact_state_message_by_fid(&stores.link_store, request.fid, &options)
            .as_response()
    }

    async fn get_current_storage_limits_by_fid(
        &self,
        request: Request<FidRequest>,
    ) -> Result<Response<StorageLimitsResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let limits = stores
            .get_storage_limits(request.fid)
            .map_err(|err| Status::internal(err.to_string()))?;
        Ok(Response::new(limits))
    }

    async fn get_casts_by_parent(
        &self,
        request: Request<CastsByParentRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let req = request.into_inner();
        let parent = match req.parent {
            Some(casts_by_parent_request::Parent::ParentCastId(cast_id)) => {
                cast_add_body::Parent::ParentCastId(cast_id)
            }
            Some(casts_by_parent_request::Parent::ParentUrl(url)) => {
                cast_add_body::Parent::ParentUrl(url)
            }
            None => return Err(Status::not_found("Parent not specified".to_string())),
        };
        let num_shards = self.shard_stores.len();
        let per_shard_tokens: Vec<Option<Vec<u8>>> = if let Some(token_bytes) = req.page_token {
            serde_json::from_slice(&token_bytes)
                .map_err(|e| Status::invalid_argument(format!("Invalid page token: {}", e)))?
        } else {
            vec![None; num_shards]
        };
        if per_shard_tokens.len() != num_shards {
            return Err(Status::invalid_argument(
                "Page token does not match number of shards".to_string(),
            ));
        }
        let pages: Vec<MessagesPage> = self
            .shard_stores
            .iter()
            .zip(per_shard_tokens.into_iter())
            .map(|(shard_entry, shard_token)| {
                let page_options = PageOptions {
                    page_size: req.page_size.map(|s| s as usize),
                    page_token: shard_token,
                    reverse: req.reverse.unwrap_or(false),
                };
                let cast_store = &shard_entry.1.cast_store;
                return CastStore::get_casts_by_parent(cast_store, &parent, &page_options)
                    .unwrap_or(MessagesPage {
                        messages: vec![],
                        next_page_token: None,
                    });
            })
            .collect();
        let combined_messages: Vec<Message> = pages
            .iter()
            .flat_map(|page| page.messages.clone())
            .collect();
        let next_page_tokens: Vec<Option<Vec<u8>>> =
            pages.into_iter().map(|page| page.next_page_token).collect();
        let new_page_token = serde_json::to_vec(&next_page_tokens)
            .map_err(|e| Status::internal(format!("Failed to serialize next_page_token: {}", e)))?;
        let response = MessagesResponse {
            messages: combined_messages,
            next_page_token: Some(new_page_token),
        };

        Ok(Response::new(response))
    }

    async fn get_casts_by_mention(
        &self,
        request: Request<FidRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let req = request.into_inner();
        let mention = req.fid;

        let num_shards = self.shard_stores.len();

        let per_shard_tokens: Vec<Option<Vec<u8>>> = if let Some(token_bytes) = req.page_token {
            serde_json::from_slice(&token_bytes)
                .map_err(|e| Status::invalid_argument(format!("Invalid page token: {}", e)))?
        } else {
            vec![None; num_shards]
        };

        if per_shard_tokens.len() != num_shards {
            return Err(Status::invalid_argument(
                "Page token does not match number of shards".to_string(),
            ));
        }

        let pages: Vec<MessagesPage> =
            self.shard_stores
                .iter()
                .zip(per_shard_tokens.into_iter())
                .map(|(shard_entry, shard_token)| {
                    let page_options = PageOptions {
                        page_size: req.page_size.map(|s| s as usize),
                        page_token: shard_token,
                        reverse: req.reverse.unwrap_or(false),
                    };

                    let store = &shard_entry.1.cast_store;
                    return CastStore::get_casts_by_mention(store, mention, &page_options)
                        .unwrap_or(MessagesPage {
                            messages: vec![],
                            next_page_token: None,
                        });
                })
                .collect();

        let combined_messages: Vec<Message> = pages
            .iter()
            .flat_map(|page| page.messages.clone())
            .collect();

        let next_page_tokens: Vec<Option<Vec<u8>>> =
            pages.into_iter().map(|page| page.next_page_token).collect();

        let new_page_token = serde_json::to_vec(&next_page_tokens)
            .map_err(|e| Status::internal(format!("Failed to serialize next_page_token: {}", e)))?;

        let response = MessagesResponse {
            messages: combined_messages,
            next_page_token: Some(new_page_token),
        };

        Ok(Response::new(response))
    }

    async fn get_reactions_by_cast(
        &self,
        request: Request<ReactionsByTargetRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let req = request.into_inner();

        let reaction_type = req
            .reaction_type
            .ok_or_else(|| Status::invalid_argument("reaction_type is required".to_string()))?;

        let target = match req.target {
            Some(reactions_by_target_request::Target::TargetCastId(cast_id)) => {
                reaction_body::Target::TargetCastId(cast_id)
            }
            // Enforce compatibility, disallow url target
            _ => return Err(Status::not_found("Target not specified".to_string())),
        };

        let num_shards = self.shard_stores.len();

        let per_shard_tokens: Vec<Option<Vec<u8>>> = if let Some(token_bytes) = req.page_token {
            serde_json::from_slice(&token_bytes)
                .map_err(|e| Status::invalid_argument(format!("Invalid page token: {}", e)))?
        } else {
            vec![None; num_shards]
        };

        if per_shard_tokens.len() != num_shards {
            return Err(Status::invalid_argument(
                "Page token does not match number of shards".to_string(),
            ));
        }

        let pages: Vec<MessagesPage> = self
            .shard_stores
            .iter()
            .zip(per_shard_tokens.into_iter())
            .map(|(shard_entry, shard_token)| {
                let page_options = PageOptions {
                    page_size: req.page_size.map(|s| s as usize),
                    page_token: shard_token,
                    reverse: req.reverse.unwrap_or(false),
                };

                let store = &shard_entry.1.reaction_store;

                return ReactionStore::get_reactions_by_target(
                    store,
                    &target,
                    reaction_type,
                    &page_options,
                )
                .unwrap_or(MessagesPage {
                    messages: vec![],
                    next_page_token: None,
                });
            })
            .collect();

        let combined_messages: Vec<Message> = pages
            .iter()
            .flat_map(|page| page.messages.clone())
            .collect();

        let next_page_tokens: Vec<Option<Vec<u8>>> =
            pages.into_iter().map(|page| page.next_page_token).collect();

        let new_page_token = serde_json::to_vec(&next_page_tokens)
            .map_err(|e| Status::internal(format!("Failed to serialize next_page_token: {}", e)))?;

        let response = MessagesResponse {
            messages: combined_messages,
            next_page_token: Some(new_page_token),
        };

        Ok(Response::new(response))
    }

    async fn get_reactions_by_target(
        &self,
        request: Request<ReactionsByTargetRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let req = request.into_inner();

        let reaction_type = req
            .reaction_type
            .ok_or_else(|| Status::invalid_argument("reaction_type is required".to_string()))?;

        let target = match req.target {
            Some(reactions_by_target_request::Target::TargetCastId(cast_id)) => {
                reaction_body::Target::TargetCastId(cast_id)
            }
            Some(reactions_by_target_request::Target::TargetUrl(url)) => {
                reaction_body::Target::TargetUrl(url)
            }
            None => return Err(Status::not_found("Target not specified".to_string())),
        };

        let num_shards = self.shard_stores.len();

        let per_shard_tokens: Vec<Option<Vec<u8>>> = if let Some(token_bytes) = req.page_token {
            serde_json::from_slice(&token_bytes)
                .map_err(|e| Status::invalid_argument(format!("Invalid page token: {}", e)))?
        } else {
            vec![None; num_shards]
        };

        if per_shard_tokens.len() != num_shards {
            return Err(Status::invalid_argument(
                "Page token does not match number of shards".to_string(),
            ));
        }

        let pages: Vec<MessagesPage> = self
            .shard_stores
            .iter()
            .zip(per_shard_tokens.into_iter())
            .map(|(shard_entry, shard_token)| {
                let page_options = PageOptions {
                    page_size: req.page_size.map(|s| s as usize),
                    page_token: shard_token,
                    reverse: req.reverse.unwrap_or(false),
                };

                let store = &shard_entry.1.reaction_store;

                return ReactionStore::get_reactions_by_target(
                    store,
                    &target,
                    reaction_type,
                    &page_options,
                )
                .unwrap_or(MessagesPage {
                    messages: vec![],
                    next_page_token: None,
                });
            })
            .collect();

        let combined_messages: Vec<Message> = pages
            .iter()
            .flat_map(|page| page.messages.clone())
            .collect();

        let next_page_tokens: Vec<Option<Vec<u8>>> =
            pages.into_iter().map(|page| page.next_page_token).collect();

        let new_page_token = serde_json::to_vec(&next_page_tokens)
            .map_err(|e| Status::internal(format!("Failed to serialize next_page_token: {}", e)))?;

        let response = MessagesResponse {
            messages: combined_messages,
            next_page_token: Some(new_page_token),
        };

        Ok(Response::new(response))
    }

    async fn get_username_proof(
        &self,
        request: Request<UsernameProofRequest>,
    ) -> Result<Response<UserNameProof>, Status> {
        let req = request.into_inner();
        let name = req.name;
        let user_name_type = UserNameType::UsernameTypeEnsL1 as u8;

        let proof_opt = self.shard_stores.iter().find_map(|(_shard_entry, stores)| {
            match UsernameProofStore::get_username_proof(
                &stores.username_proof_store,
                &name,
                user_name_type,
            ) {
                Ok(Some(message)) => message.data.and_then(|data| {
                    if let Some(message_data::Body::UsernameProofBody(user_name_proof)) = data.body
                    {
                        Some(user_name_proof)
                    } else {
                        None
                    }
                }),
                _ => None,
            }
        });

        if let Some(proof_message) = proof_opt {
            Ok(Response::new(proof_message))
        } else {
            Err(Status::not_found("Username proof not found".to_string()))
        }
    }

    async fn get_user_name_proofs_by_fid(
        &self,
        request: Request<FidRequest>,
    ) -> Result<Response<UsernameProofsResponse>, Status> {
        let req = request.into_inner();
        let fid = req.fid;

        let shard_results: Vec<Result<Vec<UserNameProof>, Status>> = self
            .shard_stores
            .iter()
            .map(|(_shard_id, stores)| {
                let mut all_proofs = Vec::new();
                let mut token: Option<Vec<u8>> = None;

                loop {
                    let page_options = PageOptions {
                        page_size: None,
                        page_token: token.clone(),
                        reverse: false,
                    };

                    let page = UsernameProofStore::get_username_proofs_by_fid(
                        &stores.username_proof_store,
                        fid,
                        &page_options,
                    )
                    .map_err(|e| Status::internal(format!("Store error: {:?}", e)))?;

                    all_proofs.extend(page.messages.into_iter().filter_map(|message| {
                        message.data.and_then(|data| {
                            if let Some(message_data::Body::UsernameProofBody(user_name_proof)) =
                                data.body
                            {
                                Some(user_name_proof)
                            } else {
                                None
                            }
                        })
                    }));

                    if page.next_page_token.is_none() {
                        break;
                    }

                    token = page.next_page_token;
                }

                Ok(all_proofs)
            })
            .collect();

        let mut combined_proofs = Vec::new();
        for shard_result in shard_results {
            let proofs = shard_result?;
            combined_proofs.extend(proofs);
        }

        let response = UsernameProofsResponse {
            proofs: combined_proofs,
        };

        Ok(Response::new(response))
    }

    async fn get_on_chain_signer(
        &self,
        request: Request<SignerRequest>,
    ) -> Result<Response<OnChainEvent>, Status> {
        let req = request.into_inner();
        let fid = req.fid;
        let signer = req.signer;

        let maybe_event = self.shard_stores.iter().find_map(|(_shard_id, stores)| {
            match stores
                .onchain_event_store
                .get_active_signer(fid, signer.clone())
            {
                Ok(Some(event)) => Some(Ok(event)),
                Ok(None) => None,
                Err(e) => Some(Err(Status::internal(format!("Store error: {:?}", e)))),
            }
        });

        let event = match maybe_event {
            Some(Ok(event)) => event,
            Some(Err(e)) => return Err(e),
            None => return Err(Status::not_found("Active signer not found".to_string())),
        };

        Ok(Response::new(event))
    }

    async fn get_on_chain_signers_by_fid(
        &self,
        request: Request<FidRequest>,
    ) -> Result<Response<OnChainEventResponse>, Status> {
        let req = request.into_inner();
        let fid = req.fid;
        let event_type = proto::OnChainEventType::EventTypeSigner;

        let mut combined_events = Vec::new();
        for (_shard_id, stores) in &self.shard_stores {
            let events = stores
                .onchain_event_store
                .get_onchain_events(event_type, fid)
                .map_err(|e| Status::internal(format!("Store error: {:?}", e)))?;
            combined_events.extend(events);
        }

        let response = OnChainEventResponse {
            events: combined_events,
            next_page_token: None,
        };
        Ok(Response::new(response))
    }

    async fn get_on_chain_events(
        &self,
        request: Request<OnChainEventRequest>,
    ) -> Result<Response<OnChainEventResponse>, Status> {
        let req = request.into_inner();
        let fid = req.fid;

        let event_type = proto::OnChainEventType::try_from(req.event_type)
            .map_err(|_| Status::invalid_argument("Invalid event type"))?;

        let mut combined_events = Vec::new();
        for (_shard_id, stores) in &self.shard_stores {
            let events = stores
                .onchain_event_store
                .get_onchain_events(event_type, fid)
                .map_err(|e| Status::internal(format!("Store error: {:?}", e)))?;
            combined_events.extend(events);
        }

        let response = OnChainEventResponse {
            events: combined_events,
            next_page_token: None,
        };
        Ok(Response::new(response))
    }

    async fn get_id_registry_on_chain_event(
        &self,
        request: Request<FidRequest>,
    ) -> Result<Response<OnChainEvent>, Status> {
        let req = request.into_inner();
        let fid = req.fid;

        let maybe_event = self.shard_stores.iter().find_map(|(_shard_id, stores)| {
            match stores.onchain_event_store.get_id_register_event_by_fid(fid) {
                Ok(Some(event)) => Some(Ok(event)),
                Ok(None) => None,
                Err(e) => Some(Err(Status::internal(format!("Store error: {:?}", e)))),
            }
        });

        let event = match maybe_event {
            Some(Ok(event)) => event,
            Some(Err(e)) => return Err(e),
            None => return Err(Status::not_found("ID registry event not found".to_string())),
        };

        Ok(Response::new(event))
    }

    async fn get_id_registry_on_chain_event_by_address(
        &self,
        _: Request<IdRegistryEventByAddressRequest>,
    ) -> Result<Response<OnChainEvent>, Status> {
        Err(Status::internal("method not supported".to_string()))
    }

    async fn get_fids(&self, _: Request<FidsRequest>) -> Result<Response<FidsResponse>, Status> {
        Err(Status::internal("method not supported".to_string()))
    }

    async fn get_links_by_target(
        &self,
        request: Request<LinksByTargetRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let req = request.into_inner();

        if req.link_type.clone().is_none() {
            return Err(Status::invalid_argument(
                "link_type is required".to_string(),
            ));
        }

        let target = match req.target {
            Some(links_by_target_request::Target::TargetFid(fid)) => {
                link_body::Target::TargetFid(fid)
            }
            None => return Err(Status::not_found("Target not specified".to_string())),
        };

        let num_shards = self.shard_stores.len();

        let per_shard_tokens: Vec<Option<Vec<u8>>> = if let Some(token_bytes) = req.page_token {
            serde_json::from_slice(&token_bytes)
                .map_err(|e| Status::invalid_argument(format!("Invalid page token: {}", e)))?
        } else {
            vec![None; num_shards]
        };

        if per_shard_tokens.len() != num_shards {
            return Err(Status::invalid_argument(
                "Page token does not match number of shards".to_string(),
            ));
        }

        let pages: Vec<MessagesPage> = self
            .shard_stores
            .iter()
            .zip(per_shard_tokens.into_iter())
            .map(|(shard_entry, shard_token)| {
                let page_options = PageOptions {
                    page_size: req.page_size.map(|s| s as usize),
                    page_token: shard_token,
                    reverse: req.reverse.unwrap_or(false),
                };

                let store = &shard_entry.1.link_store;
                LinkStore::get_links_by_target(
                    store,
                    &target,
                    req.link_type.clone().unwrap(),
                    &page_options,
                )
                .unwrap_or(MessagesPage {
                    messages: vec![],
                    next_page_token: None,
                })
            })
            .collect();

        let combined_messages: Vec<Message> = pages
            .iter()
            .flat_map(|page| page.messages.clone())
            .collect();

        let next_page_tokens: Vec<Option<Vec<u8>>> =
            pages.into_iter().map(|page| page.next_page_token).collect();

        let new_page_token = serde_json::to_vec(&next_page_tokens)
            .map_err(|e| Status::internal(format!("Failed to serialize next_page_token: {}", e)))?;

        let response = MessagesResponse {
            messages: combined_messages,
            next_page_token: Some(new_page_token),
        };

        Ok(Response::new(response))
    }

    async fn get_trie_metadata_by_prefix(
        &self,
        request: Request<TrieNodeMetadataRequest>,
    ) -> Result<Response<TrieNodeMetadataResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for_shard(request.shard_id)?;
        let trie_node = stores
            .trie
            .get_trie_node_metadata(
                &stores.db,
                &mut RocksDbTransactionBatch::new(),
                &request.prefix,
            )
            .map_err(|err| Status::internal(err.to_string()))?;
        let children = trie_node
            .children
            .values()
            .map(|child_node| TrieNodeMetadataResponse {
                prefix: child_node.prefix.clone(),
                num_messages: child_node.num_messages as u64,
                hash: child_node.hash.clone(),
                children: vec![],
            })
            .collect();
        Ok(Response::new(TrieNodeMetadataResponse {
            prefix: trie_node.prefix,
            num_messages: trie_node.num_messages as u64,
            hash: trie_node.hash,
            children,
        }))
    }
}
