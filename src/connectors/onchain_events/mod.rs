use crate::cfg::Config as AppConfig;
use crate::proto::{ChannelRegisterBody, ChannelRegisterEventType, TierPurchaseBody};
use crate::storage::store::node_local_state;
use alloy_primitives::U256;
use alloy_primitives::{address, ruint::FromUintError, Address, FixedBytes};
use alloy_provider::{Provider, RootProvider};
use alloy_rpc_types::{Filter, Log};
use alloy_sol_types::{sol, SolEvent, SolType};
use async_trait::async_trait;
use ens::EnsResolver::EnsResolverInstance;
use ens::{namehash, EnsError, EnsRegistry};
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::{broadcast, mpsc};
use tracing::{error, info, warn};

use crate::core::error::HubError;
use crate::mempool::mempool::{MempoolRequest, MempoolSource};
use crate::{
    core::validations::{
        self,
        verification::{validate_verification_contract_signature, VerificationAddressClaim},
    },
    proto::{
        on_chain_event, IdRegisterEventBody, IdRegisterEventType, OnChainEvent, OnChainEventType,
        SignerEventBody, SignerEventType, SignerMigratedEventBody, StorageRentEventBody,
        VerificationAddAddressBody,
    },
    storage::store::mempool_poller::MempoolMessage,
    storage::store::node_local_state::LocalStateStore,
    utils::statsd_wrapper::StatsdClientWrapper,
};

pub(crate) mod ens;

sol!(
    #[allow(missing_docs)]
    #[sol(rpc)]
    StorageRegistryAbi,
    "src/connectors/onchain_events/storage_registry_abi.json"
);

sol!(
    #[allow(missing_docs)]
    #[sol(rpc)]
    IdRegistryAbi,
    "src/connectors/onchain_events/id_registry_abi.json"
);

sol!(
    #[allow(missing_docs)]
    #[sol(rpc)]
    KeyRegistryAbi,
    "src/connectors/onchain_events/key_registry_abi.json"
);

sol!(
    #[allow(missing_docs)]
    #[sol(rpc)]
    TierRegistryAbi,
    "src/connectors/onchain_events/tier_registry_abi.json"
);

sol!(
    /// Farcaster ChannelRegistrar on Base. Channel names are registered as
    /// ERC-721 NFTs whose tokenId is `uint256(keccak256(channel name))`. The
    /// connector consumes three of its events — `NameRegistered` (initial
    /// registration), `NameRenewed` (expiry extension), and the ERC-721
    /// `Transfer` (ownership change) — and emits each as an
    /// `EVENT_TYPE_CHANNEL_REGISTER` onchain event. The mainnet address is
    /// added as a constant once the contract deploys; until then it is only
    /// watched when `override_channel_registrar_address` is set (see
    /// `contracts()`).
    #[allow(missing_docs)]
    #[sol(rpc)]
    ChannelRegistrarAbi,
    "src/connectors/onchain_events/channel_registrar_abi.json"
);

sol! {
    /// SignedKeyRequest metadata structure as defined in the Farcaster contracts.
    /// See: https://github.com/farcasterxyz/contracts/blob/main/src/validators/SignedKeyRequestValidator.sol
    // Also declared in `src/core/validations/key.rs` for off-chain KEY_ADD validation (NEYN-10570).
    // TODO: extract a shared module that both sides depend on so this lives in exactly one place.
    struct SignedKeyRequestMetadata {
        uint256 requestFid;
        address requestSigner;
        bytes signature;
        uint256 deadline;
    }
}

// Note these are the registry addresses, not the resolver addresses. We look up the resolver from the registry.
static ETH_L1_ENS_REGISTRY: Address = address!("00000000000C2E074eC69A0dFb2997BA6C7d2e1e");
static BASE_MAINNET_ENS_REGISTRY: Address = address!("0xB94704422c2a1E396835A571837Aa5AE53285a95");

// For reference, in case it needs to be specified manually
const OP_MAINNET_FIRST_BLOCK: u64 = 108864739;
static OP_MAINNET_CHAIN_ID: u32 = 10; // OP mainnet
const BASE_MAINNET_FIRST_BLOCK: u64 = 31180908;
static BASE_MAINNET_CHAIN_ID: u32 = 8453; // Base mainnet
const RENT_EXPIRY_IN_SECONDS: u64 = 365 * 24 * 60 * 60; // One year

const RETRY_TIMEOUT_SECONDS: u64 = 10;

const BASE_BLOCK_PAGE_SIZE: u64 = 8000; // Alchemy max is 10K

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub rpc_url: String,
    pub start_block_number: Option<u64>,
    pub stop_block_number: Option<u64>,
    pub override_tier_registry_address: Option<String>, // For testing
    pub override_channel_registrar_address: Option<String>, // For testing
}

impl Default for Config {
    fn default() -> Config {
        return Config {
            rpc_url: String::new(),
            start_block_number: None,
            stop_block_number: None,
            override_tier_registry_address: None,
            override_channel_registrar_address: None,
        };
    }
}

#[derive(Clone)]
pub enum OnchainEventsRequest {
    RetryFid(u64),
    RetryBlockRange {
        start_block_number: u64,
        stop_block_number: u64,
    },
}

#[derive(Error, Debug)]
pub enum SubscribeError {
    #[error(transparent)]
    UnableToSubscribe(#[from] alloy_transport::TransportError),

    #[error(transparent)]
    UnableToParseUrl(#[from] url::ParseError),

    #[error(transparent)]
    UnableToParseLog(#[from] alloy_sol_types::Error),

    #[error(transparent)]
    UnableToConvertToU64(#[from] FromUintError<u64>),

    #[error(transparent)]
    UnableToConvertToU32(#[from] FromUintError<u32>),

    #[error(transparent)]
    UnableToConvertToI32(#[from] FromUintError<i32>),

    #[error("Empty rpc url")]
    EmptyRpcUrl,

    #[error("Invalid override contract address: {0}")]
    InvalidOverrideAddress(String),

    #[error("Log missing block hash")]
    LogMissingBlockHash,

    #[error("Log missing log index")]
    LogMissingLogIndex,

    #[error("Log missing block number")]
    LogMissingBlockNumber,

    #[error("Log missing tx index")]
    LogMissingTxIndex,

    #[error("Log missing tx hash")]
    LogMissingTransactionHash,

    #[error("Unable to find block by hash")]
    UnableToFindBlockByHash,
}

/// Extracts the requestFid (app FID) from a SignerEventBody's metadata field.
///
/// For metadata_type = 1 (SignedKeyRequest), the metadata contains an ABI-encoded
/// SignedKeyRequestMetadata struct that includes the FID of the application that
/// requested the signer to be added.
pub fn get_request_fid_from_signer_event(signer_event_body: &SignerEventBody) -> Option<u64> {
    // Only metadata_type 1 is SignedKeyRequest which contains requestFid
    if signer_event_body.metadata_type != 1 {
        return None;
    }

    if signer_event_body.metadata.is_empty() {
        return None;
    }

    match SignedKeyRequestMetadata::abi_decode(&signer_event_body.metadata) {
        Ok(decoded) => {
            // Convert U256 to u64, returning None if it doesn't fit
            decoded.requestFid.try_into().ok()
        }
        Err(_) => None,
    }
}

/// Maps a signer FID to a human-readable name for metrics to reduce cardinality of the tag.
pub fn map_signer_fid_to_name(fid: u64) -> &'static str {
    match fid {
        9152 => "farcaster",
        309857 => "base",
        _ => "unknown",
    }
}

#[async_trait]
pub trait ChainAPI: Send + Sync {
    async fn resolve_ens_name(&self, name: String) -> Result<Address, EnsError>;
    async fn verify_contract_signature(
        &self,
        claim: VerificationAddressClaim,
        body: &VerificationAddAddressBody,
    ) -> Result<(), validations::error::ValidationError>;
}

#[derive(Eq, Hash, PartialEq, Debug)]
pub enum Chain {
    EthMainnet,
    BaseMainnet,
    OptimismMainnet,
}

impl Chain {
    pub fn from_chain_id(chain_id: u32) -> Option<Self> {
        match chain_id {
            1 => Some(Chain::EthMainnet),
            10 => Some(Chain::OptimismMainnet),
            8453 => Some(Chain::BaseMainnet),
            _ => None,
        }
    }
}

pub struct ChainClients {
    pub chain_api_map: HashMap<Chain, Box<dyn ChainAPI>>,
}

impl ChainClients {
    pub fn new(app_config: &AppConfig) -> Self {
        let mut chain_api_map = HashMap::new();
        if !app_config.l1_rpc_url.is_empty() {
            let client: Box<dyn ChainAPI> = Box::new(
                RealL1Client::new(app_config.l1_rpc_url.clone(), Some(ETH_L1_ENS_REGISTRY))
                    .unwrap(),
            );
            chain_api_map.insert(Chain::EthMainnet, client);
        }
        if !app_config.base_onchain_events.rpc_url.is_empty() {
            let client: Box<dyn ChainAPI> = Box::new(
                RealL1Client::new(
                    app_config.base_onchain_events.rpc_url.clone(),
                    Some(BASE_MAINNET_ENS_REGISTRY),
                )
                .unwrap(),
            );
            chain_api_map.insert(Chain::BaseMainnet, client);
        }
        if !app_config.onchain_events.rpc_url.is_empty() {
            let client: Box<dyn ChainAPI> = Box::new(
                RealL1Client::new(app_config.onchain_events.rpc_url.clone(), None).unwrap(),
            );
            chain_api_map.insert(Chain::OptimismMainnet, client);
        }

        ChainClients { chain_api_map }
    }

    pub fn for_chain(&self, chain: Chain) -> Result<&Box<dyn ChainAPI>, HubError> {
        match self.chain_api_map.get(&chain) {
            Some(client) => Ok(client),
            None => Err(HubError::invalid_internal_state(
                format!("No client configured for chain: {:?}", chain).as_str(),
            )),
        }
    }
}

pub struct RealL1Client {
    provider: RootProvider,
    ens_resolver_address: Option<Address>,
}

impl RealL1Client {
    pub fn new(
        rpc_url: String,
        ens_resolver_address: Option<Address>,
    ) -> Result<RealL1Client, SubscribeError> {
        if rpc_url.is_empty() {
            return Err(SubscribeError::EmptyRpcUrl);
        }
        let url = rpc_url.parse()?;
        let provider = RootProvider::new_http(url);
        Ok(RealL1Client {
            provider,
            ens_resolver_address,
        })
    }
}

#[async_trait]
impl ChainAPI for RealL1Client {
    async fn resolve_ens_name(&self, name: String) -> Result<Address, EnsError> {
        // Adapted from the ens module (originally foundry_common::ens) so we can support
        // both ETH and Base mainnet
        let node = namehash(name.as_str());
        let ens_resolver_address = self.ens_resolver_address.ok_or(EnsError::ResolverNotFound(
            "no resolver address configured for chain".to_string(),
        ))?;
        let registry = EnsRegistry::new(ens_resolver_address, self.provider.clone());
        let address = registry
            .resolver(node)
            .call()
            .await
            .map_err(EnsError::Resolver)?;
        if address == Address::ZERO {
            return Err(EnsError::ResolverNotFound(name.to_string()));
        }
        let resolver = EnsResolverInstance::new(address, self.provider.clone());
        let addr = resolver
            .addr(node)
            .call()
            .await
            .map_err(EnsError::Resolve)
            .inspect_err(|e| {
                warn!("Failed to resolve ens name {name}: {}", e);
            })?;
        Ok(addr)
    }

    async fn verify_contract_signature(
        &self,
        claim: VerificationAddressClaim,
        body: &VerificationAddAddressBody,
    ) -> Result<(), validations::error::ValidationError> {
        validate_verification_contract_signature(&self.provider, claim, body).await
    }
}

#[derive(Clone)]
pub enum ContractKind {
    TierRegistry,
    StorageRegistry,
    KeyRegistry,
    IdRegistry,
    ChannelRegistrar,
}
#[derive(Clone)]
pub struct Contract {
    address: Address,
    kind: ContractKind,
}

impl Contract {
    pub fn storage_registry() -> Self {
        Contract {
            address: address!("00000000fcce7f938e7ae6d3c335bd6a1a7c593d"),
            kind: ContractKind::StorageRegistry,
        }
    }

    pub fn key_registry() -> Self {
        Contract {
            address: address!("00000000Fc1237824fb747aBDE0FF18990E59b7e"),
            kind: ContractKind::KeyRegistry,
        }
    }

    pub fn id_registry() -> Self {
        Contract {
            address: address!("00000000Fc6c5F01Fc30151999387Bb99A9f489b"),
            kind: ContractKind::IdRegistry,
        }
    }

    pub fn tier_registry() -> Self {
        Contract {
            address: address!("0x00000000fc84484d585C3cF48d213424DFDE43FD"),
            kind: ContractKind::TierRegistry,
        }
    }

    pub fn event_kind(&self) -> &str {
        match self.kind {
            ContractKind::TierRegistry => "tier",
            ContractKind::StorageRegistry => "storage",
            ContractKind::KeyRegistry => "key",
            ContractKind::IdRegistry => "id",
            ContractKind::ChannelRegistrar => "channel",
        }
    }

    pub fn retry_filters(&self, fid: u64, start_block: u64) -> Vec<Filter> {
        match self.kind {
            ContractKind::TierRegistry => {
                vec![Filter::new()
                    .address(vec![self.address])
                    .from_block(start_block)
                    .events(vec!["PurchasedTier(uint256,uint256,uint256,address)"])
                    .topic1(U256::from(fid))]
            }
            ContractKind::StorageRegistry => {
                vec![Filter::new()
                    .address(vec![self.address])
                    .from_block(start_block)
                    .events(vec!["Rent(address,uint256,uint256)"])
                    .topic2(U256::from(fid))]
            }
            ContractKind::KeyRegistry => {
                vec![Filter::new()
                    .address(vec![self.address])
                    .from_block(start_block)
                    .events(vec![
                        "Add(uint256,uint32,bytes,bytes,uint8,bytes)",
                        "Remove(uint256,bytes,bytes)",
                    ])
                    .topic1(U256::from(fid))]
            }
            ContractKind::IdRegistry => {
                vec![
                    Filter::new()
                        .address(vec![self.address])
                        .from_block(start_block)
                        .events(vec!["Register(address,uint256,address)"])
                        .topic2(U256::from(fid)),
                    Filter::new()
                        .address(vec![self.address])
                        .from_block(start_block)
                        .events(vec!["Transfer(address,address,uint256)"])
                        .topic3(U256::from(fid)),
                ]
            }
            // Channel registrar events are keyed by label/owner address onchain, not
            // by fid (the connector always emits fid = 0 and resolution happens at
            // read time, via GetChannelOwner), so there is no per-fid retry filter.
            // Retry by block range.
            ContractKind::ChannelRegistrar => vec![],
        }
    }
}

pub struct Subscriber {
    provider: RootProvider,
    mempool_tx: mpsc::Sender<MempoolRequest>,
    start_block_number: Option<u64>,
    stop_block_number: Option<u64>,
    statsd_client: StatsdClientWrapper,
    local_state_store: LocalStateStore,
    onchain_events_request_rx: broadcast::Receiver<OnchainEventsRequest>,
    chain: node_local_state::Chain,
    override_tier_registry_address: Option<Address>,
    override_channel_registrar_address: Option<Address>,
}

/// Parses an override contract address from config once, at construction, so a
/// malformed value surfaces as a structured startup error instead of a panic
/// when the contract list is first built.
fn parse_override_address(address: &str) -> Result<Address, SubscribeError> {
    Address::from_str(address)
        .map_err(|err| SubscribeError::InvalidOverrideAddress(format!("{}: {}", address, err)))
}

// TODO(aditi): Wait for 1 confirmation before "committing" an onchain event.
impl Subscriber {
    pub fn new(
        config: &Config,
        chain: node_local_state::Chain,
        mempool_tx: mpsc::Sender<MempoolRequest>,
        statsd_client: StatsdClientWrapper,
        local_state_store: LocalStateStore,
        onchain_events_request_rx: broadcast::Receiver<OnchainEventsRequest>,
    ) -> Result<Subscriber, SubscribeError> {
        if config.rpc_url.is_empty() {
            return Err(SubscribeError::EmptyRpcUrl);
        }
        let url = config.rpc_url.parse()?;
        let provider = RootProvider::new_http(url);
        Ok(Subscriber {
            local_state_store,
            provider,
            mempool_tx,
            start_block_number: config
                .start_block_number
                .map(|start_block| start_block.max(Self::first_block(chain))),
            stop_block_number: config.stop_block_number,
            statsd_client,
            onchain_events_request_rx,
            chain,
            override_tier_registry_address: config
                .override_tier_registry_address
                .as_deref()
                .map(parse_override_address)
                .transpose()?,
            override_channel_registrar_address: config
                .override_channel_registrar_address
                .as_deref()
                .map(parse_override_address)
                .transpose()?,
        })
    }

    fn contracts(&self) -> Vec<Contract> {
        match self.chain {
            node_local_state::Chain::Optimism => vec![
                Contract::storage_registry(),
                Contract::key_registry(),
                Contract::id_registry(),
            ],
            node_local_state::Chain::Base => {
                let mut contracts = vec![match self.override_tier_registry_address {
                    None => Contract::tier_registry(),
                    Some(address) => Contract {
                        address,
                        kind: ContractKind::TierRegistry,
                    },
                }];
                // The mainnet ChannelRegistrar address is added as a constant once the
                // contract deploys. Until then the contract is only watched when the
                // override is configured (tests + the testnet acceptance run), so the
                // connector is a no-op on mainnet even after this code ships.
                if let Some(address) = self.override_channel_registrar_address {
                    contracts.push(Contract {
                        address,
                        kind: ContractKind::ChannelRegistrar,
                    });
                }
                contracts
            }
        }
    }

    fn first_block(chain: node_local_state::Chain) -> u64 {
        match chain {
            node_local_state::Chain::Optimism => OP_MAINNET_FIRST_BLOCK,
            node_local_state::Chain::Base => BASE_MAINNET_FIRST_BLOCK,
        }
    }

    fn chain_id(chain: node_local_state::Chain) -> u32 {
        match chain {
            node_local_state::Chain::Optimism => OP_MAINNET_CHAIN_ID,
            node_local_state::Chain::Base => BASE_MAINNET_CHAIN_ID,
        }
    }

    fn count(&self, key: &str, value: i64, extra_tags: Vec<(&str, &str)>) {
        self.statsd_client.count(
            format!("onchain_events.{}", key).as_str(),
            value,
            extra_tags,
        );
    }

    fn gauge(&self, key: &str, value: u64, extra_tags: Vec<(&str, &str)>) {
        self.statsd_client.gauge(
            format!("onchain_events.{}", key).as_str(),
            value,
            extra_tags,
        );
    }

    async fn add_onchain_event(
        &mut self,
        fid: u64,
        block_number: u32,
        block_hash: FixedBytes<32>,
        block_timestamp: u64,
        log_index: u32,
        tx_index: u32,
        transaction_hash: FixedBytes<32>,
        event_type: OnChainEventType,
        event_body: on_chain_event::Body,
    ) {
        let event = OnChainEvent {
            fid,
            block_number,
            block_hash: block_hash.to_vec(),
            block_timestamp,
            log_index,
            tx_index,
            r#type: event_type as i32,
            chain_id: Self::chain_id(self.chain),
            version: 0,
            body: Some(event_body),
            transaction_hash: transaction_hash.to_vec(),
        };
        info!(
            fid,
            event_type = event_type.as_str_name(),
            block_number = event.block_number,
            block_timestamp = event.block_timestamp,
            tx_hash = hex::encode(&event.transaction_hash),
            log_index = event.log_index,
            chain = self.chain.to_string(),
            "Processed onchain event"
        );
        match event_type {
            OnChainEventType::EventTypeNone => {}
            OnChainEventType::EventTypeSigner => {
                // Try to extract request_fid from the signer event metadata
                if let Some(on_chain_event::Body::SignerEventBody(signer_body)) = &event.body {
                    if let Some(request_fid) = get_request_fid_from_signer_event(signer_body) {
                        let signer_name = map_signer_fid_to_name(request_fid);
                        self.count("num_signer_events", 1, vec![("signer_app", signer_name)]);
                    } else {
                        self.count("num_signer_events", 1, vec![]);
                    }
                } else {
                    self.count("num_signer_events", 1, vec![]);
                }
            }
            OnChainEventType::EventTypeSignerMigrated => {
                self.count("num_signer_migrated_events", 1, vec![]);
            }
            OnChainEventType::EventTypeIdRegister => {
                self.count("num_id_register_events", 1, vec![]);
            }
            OnChainEventType::EventTypeStorageRent => {
                self.count("num_storage_events", 1, vec![]);
            }
            OnChainEventType::EventTypeTierPurchase => {
                self.count("num_tier_purchase_events", 1, vec![]);
            }
            OnChainEventType::EventTypeChannelRegister => {
                self.count("num_channel_register_events", 1, vec![]);
            }
        };
        match &event.body {
            Some(on_chain_event::Body::IdRegisterEventBody(id_register_event_body)) => {
                if id_register_event_body.event_type() == IdRegisterEventType::Register {
                    self.gauge("latest_fid_registered", fid, vec![]);
                }
            }
            _ => {}
        }
        self.gauge(
            "latest_block_number",
            block_number as u64,
            vec![("chain", &self.chain.to_string())],
        );
        let delay = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            - (event.block_timestamp * 1000);
        self.gauge(
            "on_chain_to_ingest_delay",
            delay,
            vec![("chain", &self.chain.to_string())],
        );
        if let Err(err) = self
            .mempool_tx
            .send(MempoolRequest::AddMessage(
                MempoolMessage::OnchainEvent(event.clone()),
                MempoolSource::Local,
                None,
            ))
            .await
        {
            error!(
                block_number = event.block_number,
                tx_hash = hex::encode(&event.transaction_hash),
                log_index = event.log_index,
                err = err.to_string(),
                chain = self.chain.to_string(),
                "Unable to send onchain event to mempool"
            )
        }
    }

    fn record_block_number(&self, block_number: u64) {
        let latest_block_in_db = self.latest_block_in_db();
        if block_number as u64 > latest_block_in_db {
            match self
                .local_state_store
                .set_latest_block_number(self.chain.clone(), block_number)
            {
                Err(err) => {
                    error!(
                        block_number,
                        err = err.to_string(),
                        chain = self.chain.to_string(),
                        "Unable to store last block number",
                    );
                }
                _ => {}
            }
        };
    }

    async fn get_block_timestamp(&self, block_hash: FixedBytes<32>) -> Result<u64, SubscribeError> {
        let mut retry_count = 0;
        loop {
            match self.provider.get_block_by_hash(block_hash).await {
                Ok(Some(block)) => {
                    return Ok(block.header.timestamp);
                }
                Ok(None) => {
                    return Err(SubscribeError::UnableToFindBlockByHash);
                }
                Err(err) => {
                    retry_count += 1;

                    if retry_count > 5 {
                        return Err(err.into());
                    }

                    error!(
                        chain = self.chain.to_string(),
                        "Error getting block timestamp for hash {}: {}. Retry {} in {} seconds",
                        hex::encode(block_hash),
                        err,
                        retry_count,
                        RETRY_TIMEOUT_SECONDS
                    );

                    tokio::time::sleep(tokio::time::Duration::from_secs(RETRY_TIMEOUT_SECONDS))
                        .await;
                }
            }
        }
    }

    async fn process_log(&mut self, event: &Log) -> Result<(), SubscribeError> {
        let block_hash = event
            .block_hash
            .ok_or(SubscribeError::LogMissingBlockHash)?;
        let log_index = event.log_index.ok_or(SubscribeError::LogMissingLogIndex)?;
        let block_number = event
            .block_number
            .ok_or(SubscribeError::LogMissingBlockNumber)?;
        let tx_index = event
            .transaction_index
            .ok_or(SubscribeError::LogMissingTxIndex)?;
        let transaction_hash = event
            .transaction_hash
            .ok_or(SubscribeError::LogMissingTransactionHash)?;
        // TODO(aditi): Cache these queries for timestamp to optimize rpc calls.
        // [block_timestamp] exists on [Log], however it's never populated in practice.
        let block_timestamp = self.get_block_timestamp(block_hash).await?;
        // Cloned up front so the channel-registrar arms can emit a warn + metric on a
        // dropped log: the `add_event` closure below borrows `self` mutably for the whole
        // match, which would otherwise conflict with `self.count`/`self.chain` in an arm.
        let statsd_client = self.statsd_client.clone();
        let chain = self.chain.clone();
        let chain_name = chain.to_string();
        let add_event = |fid, event_type, event_body| async move {
            self.add_onchain_event(
                fid,
                block_number as u32,
                block_hash,
                block_timestamp,
                log_index as u32,
                tx_index as u32,
                transaction_hash,
                event_type,
                event_body,
            )
            .await;
        };
        match event.topic0() {
            Some(&StorageRegistryAbi::Rent::SIGNATURE_HASH) => {
                let StorageRegistryAbi::Rent { payer, fid, units } = event.log_decode()?.inner.data;
                let fid = fid.try_into()?;
                add_event(
                    fid,
                    OnChainEventType::EventTypeStorageRent,
                    on_chain_event::Body::StorageRentEventBody(StorageRentEventBody {
                        payer: payer.to_vec(),
                        units: units.try_into()?,
                        expiry: (block_timestamp + RENT_EXPIRY_IN_SECONDS) as u32,
                    }),
                )
                .await;
                Ok(())
            }
            Some(&IdRegistryAbi::Register::SIGNATURE_HASH) => {
                let IdRegistryAbi::Register { to, id, recovery } = event.log_decode()?.inner.data;
                let fid = id.try_into()?;
                add_event(
                    fid,
                    OnChainEventType::EventTypeIdRegister,
                    on_chain_event::Body::IdRegisterEventBody(IdRegisterEventBody {
                        event_type: IdRegisterEventType::Register as i32,
                        to: to.to_vec(),
                        recovery_address: recovery.to_vec(),
                        from: vec![],
                    }),
                )
                .await;
                Ok(())
            }
            Some(&IdRegistryAbi::Transfer::SIGNATURE_HASH) => {
                // Transfer(address,address,uint256) is emitted by BOTH the OP IdRegistry
                // (an fid custody transfer) and the Base channel registrar (an ERC-721
                // channel NFT transfer); the two share a topic0. The subscriber is
                // per-chain and these contracts are chain-disjoint (no Base contract we
                // watch otherwise emits Transfer), so we dispatch on the chain. Revisit if
                // a second Transfer-emitting contract is ever watched on the same chain.
                match chain {
                    node_local_state::Chain::Base => {
                        // ERC-721 Transfer carries only the tokenId; the store learns the
                        // tokenId -> channel_key mapping from the REGISTER event. tokenId
                        // == uint256(label), so the tokenId's big-endian bytes reproduce
                        // `label` exactly. `to` is the receiving address, resolved to an
                        // fid at read time (see GetChannelOwner); fid is always 0 here.
                        let ChannelRegistrarAbi::Transfer { from: _, to, id } =
                            event.log_decode()?.inner.data;
                        add_event(
                            0,
                            OnChainEventType::EventTypeChannelRegister,
                            on_chain_event::Body::ChannelRegisterEventBody(ChannelRegisterBody {
                                channel_key: String::new(),
                                expiry: 0,
                                owner_address: to.to_vec(),
                                event_type: ChannelRegisterEventType::Transfer as i32,
                                label: id.to_be_bytes::<32>().to_vec(),
                            }),
                        )
                        .await;
                    }
                    node_local_state::Chain::Optimism => {
                        let IdRegistryAbi::Transfer { from, to, id } =
                            event.log_decode()?.inner.data;
                        let fid = id.try_into()?;
                        add_event(
                            fid,
                            OnChainEventType::EventTypeIdRegister,
                            on_chain_event::Body::IdRegisterEventBody(IdRegisterEventBody {
                                event_type: IdRegisterEventType::Transfer as i32,
                                to: to.to_vec(),
                                from: from.to_vec(),
                                recovery_address: vec![],
                            }),
                        )
                        .await;
                    }
                }
                Ok(())
            }
            Some(&IdRegistryAbi::ChangeRecoveryAddress::SIGNATURE_HASH) => {
                let IdRegistryAbi::ChangeRecoveryAddress { id, recovery } =
                    event.log_decode()?.inner.data;
                let fid = id.try_into()?;
                add_event(
                    fid,
                    OnChainEventType::EventTypeIdRegister,
                    on_chain_event::Body::IdRegisterEventBody(IdRegisterEventBody {
                        event_type: IdRegisterEventType::ChangeRecovery as i32,
                        to: vec![],
                        from: vec![],
                        recovery_address: recovery.to_vec(),
                    }),
                )
                .await;
                Ok(())
            }
            Some(&KeyRegistryAbi::Add::SIGNATURE_HASH) => {
                let KeyRegistryAbi::Add {
                    fid,
                    key: _,
                    keytype,
                    keyBytes,
                    metadatatype,
                    metadata,
                } = event.log_decode()?.inner.data;
                let fid = fid.try_into()?;
                add_event(
                    fid,
                    OnChainEventType::EventTypeSigner,
                    on_chain_event::Body::SignerEventBody(SignerEventBody {
                        key: keyBytes.to_vec(),
                        key_type: keytype,
                        event_type: SignerEventType::Add as i32,
                        metadata: metadata.to_vec(),
                        metadata_type: metadatatype as u32,
                    }),
                )
                .await;
                Ok(())
            }
            Some(&KeyRegistryAbi::Remove::SIGNATURE_HASH) => {
                let KeyRegistryAbi::Remove {
                    fid,
                    key: _,
                    keyBytes,
                } = event.log_decode()?.inner.data;
                let fid = fid.try_into()?;
                add_event(
                    fid,
                    OnChainEventType::EventTypeSigner,
                    on_chain_event::Body::SignerEventBody(SignerEventBody {
                        key: keyBytes.to_vec(),
                        key_type: 0,
                        event_type: SignerEventType::Remove as i32,
                        metadata: vec![],
                        metadata_type: 0,
                    }),
                )
                .await;
                Ok(())
            }
            Some(&KeyRegistryAbi::AdminReset::SIGNATURE_HASH) => {
                let KeyRegistryAbi::AdminReset {
                    fid,
                    key: _,
                    keyBytes,
                } = event.log_decode()?.inner.data;
                let fid = fid.try_into()?;
                add_event(
                    fid,
                    OnChainEventType::EventTypeSigner,
                    on_chain_event::Body::SignerEventBody(SignerEventBody {
                        key: keyBytes.to_vec(),
                        key_type: 0,
                        event_type: SignerEventType::AdminReset as i32,
                        metadata: vec![],
                        metadata_type: 0,
                    }),
                )
                .await;
                Ok(())
            }
            Some(&KeyRegistryAbi::Migrated::SIGNATURE_HASH) => {
                let KeyRegistryAbi::Migrated { keysMigratedAt } = event.log_decode()?.inner.data;
                let migrated_at = keysMigratedAt.try_into()?;
                add_event(
                    0,
                    OnChainEventType::EventTypeSignerMigrated,
                    on_chain_event::Body::SignerMigratedEventBody(SignerMigratedEventBody {
                        migrated_at,
                    }),
                )
                .await;
                Ok(())
            }
            Some(&TierRegistryAbi::PurchasedTier::SIGNATURE_HASH) => {
                let TierRegistryAbi::PurchasedTier {
                    fid,
                    tier,
                    forDays: for_days,
                    payer,
                } = event.log_decode()?.inner.data;
                add_event(
                    fid.try_into()?,
                    OnChainEventType::EventTypeTierPurchase,
                    on_chain_event::Body::TierPurchaseEventBody(TierPurchaseBody {
                        tier_type: tier.try_into()?,
                        for_days: for_days.try_into()?,
                        payer: payer.to_vec(),
                    }),
                )
                .await;
                Ok(())
            }
            Some(&ChannelRegistrarAbi::NameRegistered::SIGNATURE_HASH) => {
                // A non-UTF-8 name is registrable onchain (the registry validates length
                // only) but cannot be represented in the proto3 `string` channel_key, so
                // ABI decode fails. Drop just this log with a warn + metric — one hostile
                // registration must never stall ingestion of subsequent events. (The
                // BaseRegistrar's same-named `NameRegistered(uint256,...)` has a different
                // topic0 and falls through to the silent `_` arm.) fid is always 0; the
                // owner address is resolved to an fid at read time (see GetChannelOwner).
                //
                // NOTE: we decode with validate = true (unlike `event.log_decode()`, which
                // passes false and would lossily replace non-UTF-8 bytes with U+FFFD and
                // silently mint a corrupted channel_key). validate = true routes an invalid
                // UTF-8 name to the Err arm below so it is dropped, honoring the proto's
                // "non-UTF-8 names are never minted as events" invariant.
                match ChannelRegistrarAbi::NameRegistered::decode_log(&event.inner, true) {
                    Ok(decoded) => {
                        let ChannelRegistrarAbi::NameRegistered {
                            name,
                            label,
                            owner,
                            expires,
                        } = decoded.data;
                        add_event(
                            0,
                            OnChainEventType::EventTypeChannelRegister,
                            on_chain_event::Body::ChannelRegisterEventBody(ChannelRegisterBody {
                                channel_key: name,
                                expiry: expires.try_into()?,
                                owner_address: owner.to_vec(),
                                event_type: ChannelRegisterEventType::Register as i32,
                                label: label.to_vec(),
                            }),
                        )
                        .await;
                    }
                    Err(err) => {
                        warn!(
                            chain = chain_name.as_str(),
                            tx_hash = hex::encode(transaction_hash),
                            log_index,
                            "Skipping channel NameRegistered log with undecodable (likely non-UTF-8) name: {}",
                            err
                        );
                        statsd_client.count(
                            "onchain_events.channel_register_decode_failures",
                            1,
                            vec![("event", "name_registered")],
                        );
                    }
                }
                Ok(())
            }
            Some(&ChannelRegistrarAbi::NameRenewed::SIGNATURE_HASH) => {
                // Same non-UTF-8 drop rule as NameRegistered (validate = true; see there).
                // NameRenewed carries no owner (`expires` is absolute — the store
                // overwrites, never adds duration).
                match ChannelRegistrarAbi::NameRenewed::decode_log(&event.inner, true) {
                    Ok(decoded) => {
                        let ChannelRegistrarAbi::NameRenewed {
                            name,
                            label,
                            expires,
                        } = decoded.data;
                        add_event(
                            0,
                            OnChainEventType::EventTypeChannelRegister,
                            on_chain_event::Body::ChannelRegisterEventBody(ChannelRegisterBody {
                                channel_key: name,
                                expiry: expires.try_into()?,
                                owner_address: vec![],
                                event_type: ChannelRegisterEventType::Renew as i32,
                                label: label.to_vec(),
                            }),
                        )
                        .await;
                    }
                    Err(err) => {
                        warn!(
                            chain = chain_name.as_str(),
                            tx_hash = hex::encode(transaction_hash),
                            log_index,
                            "Skipping channel NameRenewed log with undecodable (likely non-UTF-8) name: {}",
                            err
                        );
                        statsd_client.count(
                            "onchain_events.channel_register_decode_failures",
                            1,
                            vec![("event", "name_renewed")],
                        );
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    async fn get_logs(&mut self, filter: &Filter, event_kind: &str) -> Result<(), SubscribeError> {
        let events = self.provider.get_logs(filter).await?;
        for event in events {
            let result = self.process_log(&event).await;
            match result {
                Err(err) => {
                    error!(
                        chain = self.chain.to_string(),
                        event_kind,
                        "Error processing onchain event. Error: {:#?}. Event: {:#?}",
                        err,
                        event,
                    )
                }
                Ok(()) => {}
            }
        }
        Ok(())
    }

    async fn get_logs_with_retry(
        &mut self,
        filter: Filter,
        event_kind: &str,
    ) -> Result<(), SubscribeError> {
        let mut retry_count = 0;
        loop {
            match self.get_logs(&filter, event_kind).await {
                Ok(_) => return Ok(()),
                Err(err) => {
                    retry_count += 1;

                    if retry_count > 5 {
                        return Err(err);
                    }

                    error!(
                        chain = self.chain.to_string(),
                        "Error getting logs for {} event kind(s): {}. Retry {} in {} seconds",
                        event_kind,
                        err,
                        retry_count,
                        RETRY_TIMEOUT_SECONDS
                    );

                    tokio::time::sleep(tokio::time::Duration::from_secs(RETRY_TIMEOUT_SECONDS))
                        .await;
                }
            }
        }
    }

    pub async fn sync_historical_events(
        &mut self,
        initial_start_block: u64,
        final_stop_block: u64,
    ) -> Result<(), SubscribeError> {
        info!(
            start_block_number = initial_start_block,
            stop_block_number = final_stop_block,
            chain = self.chain.to_string(),
            "Starting historical sync"
        );
        let batch_size = 1000;
        let mut start_block = initial_start_block;
        loop {
            let stop_block = final_stop_block.min(start_block + batch_size);

            for contract in self.contracts() {
                let filter = Filter::new()
                    .address(contract.address)
                    .from_block(start_block)
                    .to_block(stop_block);
                self.get_logs_with_retry(filter, contract.event_kind())
                    .await?;
            }

            self.record_block_number(stop_block);
            start_block += batch_size;

            if start_block > final_stop_block {
                info!(
                    start_block,
                    stop_block = final_stop_block,
                    chain = self.chain.to_string(),
                    "Stopping historical sync"
                );
                return Ok(());
            }
        }
    }

    fn latest_block_in_db(&self) -> u64 {
        match self
            .local_state_store
            .get_latest_block_number(self.chain.clone())
        {
            Ok(number) => number.unwrap_or(0),
            Err(err) => {
                error!(
                    err = err.to_string(),
                    chain = self.chain.to_string(),
                    "Unable to retrieve last block number",
                );
                0
            }
        }
    }

    // We're running into issues using getFilterChanges for this, possibly because the events are
    // so rare. Or perhaps due to an alchemy issue. We weren't getting any events. So swtich to raw
    // polling. We're only seeing a few events per day, so this should be fine. Used for all Base
    // contracts (tier registry, channel registrar); OP contracts still stream.
    async fn poll_registry_events(
        &mut self,
        contract: &Contract,
        from_block: &mut u64,
    ) -> Result<(), SubscribeError> {
        // Get the current block number
        let current_block = self.latest_block_on_chain().await?;

        // If there are new blocks to process
        while *from_block <= current_block {
            let mut to_block = match self.stop_block_number {
                Some(stop_block) => stop_block.min(current_block),
                None => current_block,
            };

            // Paginate through blocks in batches
            to_block = to_block.min(*from_block + BASE_BLOCK_PAGE_SIZE);

            // Create filter for this contract's events
            let filter = Filter::new()
                .address(contract.address)
                .from_block(*from_block)
                .to_block(to_block);

            info!(
                from_block = *from_block,
                to_block,
                chain = self.chain.to_string(),
                event_kind = contract.event_kind(),
                "Polling registry events"
            );

            // Get and process logs
            match self.get_logs(&filter, contract.event_kind()).await {
                Ok(_) => {
                    // Advance only this contract's in-memory cursor. The stored
                    // block number is chain-wide, so it is persisted by the caller
                    // as the minimum across all polled contracts' cursors — see
                    // the poll loop in sync_live_events.
                    *from_block = to_block + 1;
                }
                Err(err) => {
                    error!(
                        chain = self.chain.to_string(),
                        event_kind = contract.event_kind(),
                        "Error getting registry logs: {}",
                        err
                    );
                    return Err(err);
                }
            }
        }

        Ok(())
    }

    async fn latest_block_on_chain(&mut self) -> Result<u64, SubscribeError> {
        let mut retry_count = 0;
        loop {
            match self
                .provider
                .get_block_by_number(alloy_rpc_types::BlockNumberOrTag::Latest)
                .await
            {
                Ok(block) => {
                    return Ok(block
                        .ok_or(SubscribeError::LogMissingBlockNumber)?
                        .header
                        .number);
                }
                Err(err) => {
                    retry_count += 1;
                    if retry_count > 5 {
                        return Err(err.into());
                    }

                    error!(
                        chain = self.chain.to_string(),
                        "Error getting latest block on chain: {}. Retry {} in {} seconds",
                        err,
                        retry_count,
                        RETRY_TIMEOUT_SECONDS
                    );

                    tokio::time::sleep(tokio::time::Duration::from_secs(RETRY_TIMEOUT_SECONDS))
                        .await;
                }
            }
        }
    }

    async fn sync_live_events(&mut self, start_block_number: u64) -> Result<(), SubscribeError> {
        info!(
            start_block_number,
            chain = self.chain.to_string(),
            "Starting live sync"
        );

        // Base contracts (tier registry, channel registrar) are polled; OP contracts are
        // streamed. See poll_registry_events for why Base uses raw polling.
        let mut polled_contracts: Vec<Contract> = Vec::new();
        let mut streamed_contracts: Vec<Contract> = Vec::new();

        for contract in self.contracts() {
            match contract.kind {
                ContractKind::TierRegistry | ContractKind::ChannelRegistrar => {
                    polled_contracts.push(contract);
                }
                _ => {
                    streamed_contracts.push(contract);
                }
            }
        }

        // Set up streaming for streamed contracts if any exist
        let mut stream = if !streamed_contracts.is_empty() {
            let contract_addresses: Vec<Address> = streamed_contracts
                .iter()
                .map(|contract| contract.address)
                .collect();
            let filter = Filter::new()
                .address(contract_addresses)
                .from_block(start_block_number);

            let filter = match self.stop_block_number {
                None => filter,
                Some(stop_block) => filter.to_block(stop_block),
            };

            let subscription = self.provider.watch_logs(&filter).await?;
            Some(subscription.into_stream())
        } else {
            None
        };

        // Set up polling for polled contracts if any exist
        let mut poll_interval = if !polled_contracts.is_empty() {
            Some(tokio::time::interval(tokio::time::Duration::from_secs(30)))
        } else {
            None
        };

        // Track the last block polled per polled contract (parallel to polled_contracts)
        let mut polled_last_blocks: Vec<u64> = vec![start_block_number; polled_contracts.len()];

        loop {
            tokio::select! {
                 biased;

                 request = self.onchain_events_request_rx.recv() => {
                    match request {
                        Err(_) => {
                            // Ignore, this can happen if we don't run an admin server
                        }, Ok(request) => {
                            match request {
                                OnchainEventsRequest::RetryFid(retry_fid) =>  {
                                    if let Err(err) = self.retry_fid(retry_fid).await {
                                        error!(fid = retry_fid, chain = self.chain.to_string(),
                                             "Unable to retry fid: {}", err.to_string())
                                    }
                                },
                                OnchainEventsRequest::RetryBlockRange{start_block_number, stop_block_number} => {
                                    if let Err(err) = self.retry_block_range(start_block_number, stop_block_number).await {
                                        error!(start_block_number, stop_block_number, chain = self.chain.to_string(),
                                            "Unable to retry block range: {}", err.to_string())
                                    }


                                }
                            }
                        }
                    }
                 }
                 _ = async {
                     if let Some(ref mut interval) = poll_interval {
                         interval.tick().await;
                     } else {
                         // If no polled contracts, wait forever
                         futures_util::future::pending::<()>().await;
                     }
                 } => {
                     // Poll each Base contract in turn, advancing its own block cursor.
                     for (contract, last_block) in polled_contracts.iter().zip(polled_last_blocks.iter_mut()) {
                         if let Err(err) = self.poll_registry_events(contract, last_block).await {
                             error!(
                                 chain = self.chain.to_string(),
                                 event_kind = contract.event_kind(),
                                 "Error polling registry events: {}", err
                             );
                         }
                     }
                     // Persist the lowest cursor across polled contracts. The stored
                     // block number is chain-wide, so recording any single contract's
                     // progress would make a restart resume every contract from the
                     // most-advanced one and silently skip a lagging contract's gap.
                     // Resuming from the minimum re-polls blocks the faster contracts
                     // already processed, which is safe (onchain event merges are
                     // idempotent); skipped blocks are not recoverable.
                     if let Some(min_next_block) = polled_last_blocks.iter().min() {
                         if *min_next_block > 0 {
                             self.record_block_number(min_next_block - 1);
                         }
                     }
                 }
                 events = async {
                     if let Some(ref mut s) = stream {
                         s.next().await
                     } else {
                         // If no stream, wait forever
                         futures_util::future::pending().await
                     }
                 } => {
                     match events {
                         None => {
                            // We want to trigger a retry here
                             break;
                         },
                         Some(events) => {
                             for event in events {
                                 let result = self.process_log(&event).await;
                                 match result {
                                     Err(err) => {
                                         error!(
                                             "Error processing onchain event. Error: {:#?}. Event: {:#?}",
                                             err, event,
                                         )
                                     }
                                     Ok(()) => match event.block_number {
                                         None => {}
                                         Some(block_number) => {
                                             self.record_block_number(block_number);
                                         }
                                     },
                                 }
                             }
                         }
                     }
                 }
            }
        }
        Ok(())
    }

    pub async fn retry_fid(&mut self, fid: u64) -> Result<(), SubscribeError> {
        info!(
            fid,
            chain = self.chain.to_string(),
            "Retrying onchain events for fid"
        );
        for contract in self.contracts() {
            for retry_filter in contract.retry_filters(fid, Self::first_block(self.chain)) {
                self.get_logs_with_retry(retry_filter, contract.event_kind())
                    .await?;
            }
        }

        Ok(())
    }

    pub async fn retry_block_range(
        &mut self,
        start_block_number: u64,
        stop_block_number: u64,
    ) -> Result<(), SubscribeError> {
        info!(
            start_block_number,
            stop_block_number,
            chain = self.chain.to_string(),
            "Retrying onchain events in range"
        );
        let filter = Filter::new()
            .address(
                self.contracts()
                    .iter()
                    .map(|contract| contract.address)
                    .collect::<Vec<Address>>(),
            )
            .from_block(start_block_number)
            .to_block(stop_block_number);
        self.get_logs_with_retry(filter, "all").await?;
        Ok(())
    }

    pub async fn run(&mut self) -> Result<(), SubscribeError> {
        let latest_block_on_chain = self.latest_block_on_chain().await?;
        let latest_block_in_db = self.latest_block_in_db();
        info!(
            start_block_number = self.start_block_number,
            stop_block_numer = self.stop_block_number,
            latest_block_on_chain,
            latest_block_in_db,
            chain = self.chain.to_string(),
            "Starting l2 events subscription"
        );
        let live_sync_block;
        match self.start_block_number {
            None => {
                // By default, start from the first block or the latest block in the db. Whichever is higher
                live_sync_block = Some(Self::first_block(self.chain).max(latest_block_in_db));
            }
            Some(start_block_number) => {
                let historical_sync_start_block = latest_block_in_db.max(start_block_number);
                let historical_sync_stop_block = latest_block_on_chain
                    .min(self.stop_block_number.unwrap_or(latest_block_on_chain));

                // If we have a specific start block, sync historical events first
                self.sync_historical_events(
                    historical_sync_start_block,
                    historical_sync_stop_block,
                )
                .await?;

                live_sync_block = match self.stop_block_number {
                    // No specificed stop block, so live sync should resume from where historical sync ended
                    None => Some(historical_sync_stop_block),
                    Some(stop_block) => {
                        // stop block is in the future, so start live sync
                        if stop_block > historical_sync_stop_block {
                            Some(historical_sync_stop_block)
                        } else {
                            // stop block is in the past, so no need to live sync
                            None
                        }
                    }
                };
            }
        }

        if live_sync_block.is_none() {
            info!(
                chain = self.chain.to_string(),
                "Historical sync complete. Not subscribing to live events"
            );
            return Ok(());
        }

        loop {
            match self.sync_live_events(live_sync_block.unwrap()).await {
                Err(e) => {
                    error!(
                        chain = self.chain.to_string(),
                        "Live sync ended with error: {e}. Retrying in 10 seconds",
                    );
                }
                _ => {
                    error!(
                        chain = self.chain.to_string(),
                        "Live sync ended unexpectedly. Retrying in 10 seconds",
                    );
                }
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(RETRY_TIMEOUT_SECONDS)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::connectors::onchain_events;

    use super::*;

    #[tokio::test]
    #[ignore = "Requires a valid Alchemy API key"]
    async fn test_chain_clients() {
        // Test with a valid API key for Alchemy
        let api_key = "<KEY>";
        let app_config = AppConfig {
            l1_rpc_url: format!("https://eth-mainnet.g.alchemy.com/v2/{}", api_key).to_string(),
            base_onchain_events: onchain_events::Config {
                rpc_url: format!("https://base-mainnet.g.alchemy.com/v2/{}", api_key).to_string(),
                start_block_number: None,
                stop_block_number: None,
                override_tier_registry_address: None,
                override_channel_registrar_address: None,
            },
            ..Default::default()
        };
        let chain_clients = ChainClients::new(&app_config);
        assert!(chain_clients.for_chain(Chain::EthMainnet).is_ok());
        assert!(chain_clients.for_chain(Chain::BaseMainnet).is_ok());

        let address = chain_clients
            .for_chain(Chain::EthMainnet)
            .unwrap()
            .resolve_ens_name("vitalik.eth".to_string())
            .await
            .unwrap();
        assert_eq!(
            address,
            address!("0xD8dA6BF26964aF9D7eEd9e03E53415D37aA96045")
        );

        let address = chain_clients
            .for_chain(Chain::BaseMainnet)
            .unwrap()
            .resolve_ens_name("jesse.base.eth".to_string())
            .await
            .unwrap();
        assert_eq!(
            address,
            address!("0x849151d7D0bF1F34b70d5caD5149D28CC2308bf1")
        );
    }

    #[test]
    fn test_get_request_fid() {
        // Real metadata from an actual onchain signer event
        let metadata_hex = "000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000023c000000000000000000000000002ef790dd7993a35fd847c053eddae940d05559600000000000000000000000000000000000000000000000000000000000000800000000000000000000000000000000000000000000000000000000065420dd50000000000000000000000000000000000000000000000000000000000000041bd0677376b4740f956a6d591e863d948bc2771d5ac109bfa57bd24127c35ca4b3cd00d56593750802eff94cd28e65b32ab06012a4940f0a5b8c25ca1e54050761b00000000000000000000000000000000000000000000000000000000000000";
        let metadata_bytes = hex::decode(metadata_hex).expect("Invalid hex string");

        let signer_event = SignerEventBody {
            key: vec![],
            key_type: 1,
            event_type: SignerEventType::Add as i32,
            metadata: metadata_bytes,
            metadata_type: 1,
        };

        let result = get_request_fid_from_signer_event(&signer_event);

        assert_eq!(result, Some(9152));
    }

    #[test]
    fn test_channel_name_registered_round_trip() {
        // Verifies the ABI json shape (indexed flags + types) decodes as expected and that
        // tokenId == uint256(label) — the join key the store uses to tie a Transfer
        // (tokenId only) back to a channel_key.
        let label = FixedBytes::<32>::from([0xAB; 32]);
        let owner = address!("0x849151d7D0bF1F34b70d5caD5149D28CC2308bf1");
        let expires = U256::from(1_800_000_000u64);
        let encoded = ChannelRegistrarAbi::NameRegistered {
            name: "pets".to_string(),
            label,
            owner,
            expires,
        }
        .encode_log_data();

        let decoded = ChannelRegistrarAbi::NameRegistered::decode_log_data(&encoded, true).unwrap();
        assert_eq!(decoded.name, "pets");
        assert_eq!(decoded.label, label);
        assert_eq!(decoded.owner, owner);
        assert_eq!(decoded.expires, expires);

        // The connector stores label == the tokenId's big-endian bytes.
        let token_id = U256::from_be_bytes(label.0);
        assert_eq!(token_id.to_be_bytes::<32>().to_vec(), label.to_vec());
    }

    #[test]
    fn test_channel_transfer_round_trip() {
        // ERC-721 Transfer is all-indexed; the connector reads `to` (receiving address)
        // and reproduces `label` from the tokenId.
        let from = Address::ZERO; // mint fires from 0x0
        let to = address!("0x849151d7D0bF1F34b70d5caD5149D28CC2308bf1");
        let id = U256::from_be_bytes([0x11u8; 32]);
        let encoded = ChannelRegistrarAbi::Transfer { from, to, id }.encode_log_data();

        let decoded = ChannelRegistrarAbi::Transfer::decode_log_data(&encoded, true).unwrap();
        assert_eq!(decoded.to, to);
        assert_eq!(decoded.id, id);
        assert_eq!(decoded.id.to_be_bytes::<32>().to_vec(), vec![0x11u8; 32]);
    }

    #[test]
    fn test_transfer_topic0_collision_is_intentional() {
        // The Base channel-registrar ERC-721 Transfer and the OP IdRegistry Transfer share
        // an identical signature, hence the same topic0. process_log cannot tell them apart
        // by topic0 and dispatches on the chain instead; this is the regression guard for
        // that collision (if it ever stops holding, the chain-dispatch can be revisited).
        assert_eq!(
            ChannelRegistrarAbi::Transfer::SIGNATURE_HASH,
            IdRegistryAbi::Transfer::SIGNATURE_HASH
        );
        // NameRegistered(string,...) does not collide with the IdRegistry Register event.
        assert_ne!(
            ChannelRegistrarAbi::NameRegistered::SIGNATURE_HASH,
            IdRegistryAbi::Register::SIGNATURE_HASH
        );
    }

    #[test]
    fn test_channel_name_non_utf8_dropped_only_with_validation() {
        use alloy_primitives::{Bytes, LogData};

        // A non-UTF-8 name is registrable onchain (length-only contract check) but cannot
        // be represented in the proto3 string channel_key. Hand-build a NameRegistered log
        // whose non-indexed data tuple (string name, uint256 expires) holds a single 0xFF
        // byte as the name.
        let label = FixedBytes::<32>::from([0x11; 32]);
        let owner = address!("0x849151d7D0bF1F34b70d5caD5149D28CC2308bf1");
        let mut data = Vec::new();
        data.extend_from_slice(&U256::from(0x40).to_be_bytes::<32>()); // offset to string
        data.extend_from_slice(&U256::ZERO.to_be_bytes::<32>()); // expires = 0
        data.extend_from_slice(&U256::from(1).to_be_bytes::<32>()); // string length = 1
        let mut name_word = [0u8; 32];
        name_word[0] = 0xFF; // invalid UTF-8
        data.extend_from_slice(&name_word);

        let topics = vec![
            ChannelRegistrarAbi::NameRegistered::SIGNATURE_HASH,
            label,
            owner.into_word(),
        ];
        let log_data = LogData::new_unchecked(topics, Bytes::from(data));

        // validate = false is what `Log::log_decode` uses. It lossily replaces the bad byte
        // with U+FFFD and succeeds — precisely why the connector must NOT use that path: it
        // would mint a corrupted channel_key whose keccak no longer equals `label`.
        let lossy = ChannelRegistrarAbi::NameRegistered::decode_log_data(&log_data, false).unwrap();
        assert!(lossy.name.contains('\u{FFFD}'));

        // validate = true is the connector's path: the invalid name is rejected, so the log
        // is dropped (warn + metric) rather than minted — honoring the proto invariant.
        assert!(ChannelRegistrarAbi::NameRegistered::decode_log_data(&log_data, true).is_err());
    }
}
