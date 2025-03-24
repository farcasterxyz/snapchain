use alloy_primitives::{address, ruint::FromUintError, Address, FixedBytes};
use alloy_provider::{Provider, ProviderBuilder, RootProvider};
use alloy_rpc_types::{Filter, Log};
use alloy_sol_types::{sol, SolEvent};
use alloy_transport_http::{Client, Http};
use async_trait::async_trait;
use foundry_common::ens::EnsError;
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::mempool::mempool::{MempoolRequest, MempoolSource};
use crate::{
    core::validations::{
        self,
        verification::{validate_verification_contract_signature, VerificationAddressClaim},
    },
    proto::{
        on_chain_event, IdRegisterEventBody, IdRegisterEventType, OnChainEvent, OnChainEventType,
        SignerEventBody, SignerEventType, SignerMigratedEventBody, StorageRentEventBody,
        ValidatorMessage, VerificationAddAddressBody,
    },
    storage::store::{engine::MempoolMessage, node_local_state::LocalStateStore},
    utils::statsd_wrapper::StatsdClientWrapper,
};

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

static STORAGE_REGISTRY: Address = address!("00000000fcce7f938e7ae6d3c335bd6a1a7c593d");

static KEY_REGISTRY: Address = address!("00000000Fc1237824fb747aBDE0FF18990E59b7e");

static ID_REGISTRY: Address = address!("00000000Fc6c5F01Fc30151999387Bb99A9f489b");

// For reference, in case it needs to be specified manually
const FIRST_BLOCK: u64 = 108864739;
static CHAIN_ID: u32 = 10; // OP mainnet
const RENT_EXPIRY_IN_SECONDS: u64 = 365 * 24 * 60 * 60; // One year

const RETRY_TIMEOUT_SECONDS: u64 = 10;

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub rpc_url: String,
    pub start_block_number: Option<u64>,
    pub stop_block_number: Option<u64>,
}

impl Default for Config {
    fn default() -> Config {
        return Config {
            rpc_url: String::new(),
            start_block_number: None,
            stop_block_number: None,
        };
    }
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

    #[error("Empty rpc url")]
    EmptyRpcUrl,

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

#[async_trait]
pub trait L1Client: Send + Sync {
    async fn resolve_ens_name(&self, name: String) -> Result<Address, EnsError>;
    async fn verify_contract_signature(
        &self,
        claim: VerificationAddressClaim,
        body: &VerificationAddAddressBody,
    ) -> Result<(), validations::error::ValidationError>;
}

pub struct RealL1Client {
    provider: RootProvider<Http<Client>>,
}

impl RealL1Client {
    pub fn new(rpc_url: String) -> Result<RealL1Client, SubscribeError> {
        if rpc_url.is_empty() {
            return Err(SubscribeError::EmptyRpcUrl);
        }
        let url = rpc_url.parse()?;
        let provider = ProviderBuilder::new().on_http(url);
        Ok(RealL1Client { provider })
    }
}

#[async_trait]
impl L1Client for RealL1Client {
    async fn resolve_ens_name(&self, name: String) -> Result<Address, EnsError> {
        foundry_common::ens::NameOrAddress::Name(name)
            .resolve(&self.provider)
            .await
    }

    async fn verify_contract_signature(
        &self,
        claim: VerificationAddressClaim,
        body: &VerificationAddAddressBody,
    ) -> Result<(), validations::error::ValidationError> {
        validate_verification_contract_signature(&self.provider, claim, body).await
    }
}

pub struct Subscriber {
    provider: RootProvider<Http<Client>>,
    mempool_tx: mpsc::Sender<MempoolRequest>,
    start_block_number: Option<u64>,
    stop_block_number: Option<u64>,
    statsd_client: StatsdClientWrapper,
    local_state_store: LocalStateStore,
}

// TODO(aditi): Wait for 1 confirmation before "committing" an onchain event.
impl Subscriber {
    pub fn new(
        config: &Config,
        mempool_tx: mpsc::Sender<MempoolRequest>,
        statsd_client: StatsdClientWrapper,
        local_state_store: LocalStateStore,
    ) -> Result<Subscriber, SubscribeError> {
        if config.rpc_url.is_empty() {
            return Err(SubscribeError::EmptyRpcUrl);
        }
        let url = config.rpc_url.parse()?;
        let provider = ProviderBuilder::new().on_http(url);
        Ok(Subscriber {
            local_state_store,
            provider,
            mempool_tx,
            start_block_number: config
                .start_block_number
                .map(|start_block| start_block.max(FIRST_BLOCK)),
            stop_block_number: config.stop_block_number,
            statsd_client,
        })
    }

    fn count(&self, key: &str, value: u64) {
        self.statsd_client
            .count(format!("onchain_events.{}", key).as_str(), value);
    }

    fn gauge(&self, key: &str, value: u64) {
        self.statsd_client
            .gauge(format!("onchain_events.{}", key).as_str(), value);
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
            chain_id: CHAIN_ID,
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
            "Processed onchain event"
        );
        match event_type {
            OnChainEventType::EventTypeNone => {}
            OnChainEventType::EventTypeSigner => {
                self.count("num_signer_events", 1);
            }
            OnChainEventType::EventTypeSignerMigrated => {
                self.count("num_signer_migrated_events", 1);
            }
            OnChainEventType::EventTypeIdRegister => {
                self.count("num_id_register_events", 1);
            }
            OnChainEventType::EventTypeStorageRent => {
                self.count("num_storage_events", 1);
            }
        };
        match &event.body {
            Some(on_chain_event::Body::IdRegisterEventBody(id_register_event_body)) => {
                if id_register_event_body.event_type() == IdRegisterEventType::Register {
                    self.gauge("latest_fid_registered", fid);
                }
            }
            _ => {}
        }
        self.gauge("latest_block_number", block_number as u64);
        if let Err(err) = self
            .mempool_tx
            .send(MempoolRequest::AddMessage(
                MempoolMessage::ValidatorMessage(ValidatorMessage {
                    on_chain_event: Some(event.clone()),
                    fname_transfer: None,
                }),
                MempoolSource::Local,
            ))
            .await
        {
            error!(
                block_number = event.block_number,
                tx_hash = hex::encode(&event.transaction_hash),
                log_index = event.log_index,
                err = err.to_string(),
                "Unable to send onchain event to mempool"
            )
        }
    }

    fn record_block_number(&self, block_number: u64) {
        if block_number as u64 > self.latest_block_in_db() {
            match self.local_state_store.set_latest_block_number(block_number) {
                Err(err) => {
                    error!(
                        block_number,
                        err = err.to_string(),
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
            match self
                .provider
                .get_block_by_hash(block_hash, alloy_rpc_types::BlockTransactionsKind::Hashes)
                .await
            {
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
                let IdRegistryAbi::Transfer { from, to, id } = event.log_decode()?.inner.data;
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
            _ => Ok(()),
        }
    }

    async fn get_logs(
        &mut self,
        address: Address,
        start_block: u64,
        stop_block: u64,
    ) -> Result<(), SubscribeError> {
        let filter = Filter::new()
            .address(address)
            .from_block(start_block)
            .to_block(stop_block);
        let event_kind = if address == STORAGE_REGISTRY {
            "storage"
        } else if address == ID_REGISTRY {
            "id"
        } else if address == KEY_REGISTRY {
            "key"
        } else {
            panic!("Invalid registry")
        };
        info!(
            event_kind,
            start_block, stop_block, "Syncing historical events in range"
        );
        let events = self.provider.get_logs(&filter).await?;
        for event in events {
            let result = self.process_log(&event).await;
            match result {
                Err(err) => {
                    error!(
                        "Error processing onchain event. Error: {:#?}. Event: {:#?}",
                        err, event,
                    )
                }
                Ok(()) => {}
            }
        }
        Ok(())
    }

    async fn get_logs_with_retry(
        &mut self,
        address: Address,
        start_block: u64,
        stop_block: u64,
    ) -> Result<(), SubscribeError> {
        let mut retry_count = 0;
        loop {
            match self.get_logs(address, start_block, stop_block).await {
                Ok(_) => return Ok(()),
                Err(err) => {
                    retry_count += 1;
                    let registry_name = if address == STORAGE_REGISTRY {
                        "Storage Registry"
                    } else if address == ID_REGISTRY {
                        "ID Registry"
                    } else if address == KEY_REGISTRY {
                        "Key Registry"
                    } else {
                        "Unknown Registry"
                    };

                    if retry_count > 5 {
                        return Err(err);
                    }

                    error!(
                        "Error getting logs for {}, blocks {}-{}: {}. Retry {} in {} seconds",
                        registry_name,
                        start_block,
                        stop_block,
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
        let batch_size = 1000;
        let mut start_block = initial_start_block;
        loop {
            let stop_block = final_stop_block.min(start_block + batch_size);

            self.get_logs_with_retry(STORAGE_REGISTRY, start_block, stop_block)
                .await?;
            self.get_logs_with_retry(ID_REGISTRY, start_block, stop_block)
                .await?;
            self.get_logs_with_retry(KEY_REGISTRY, start_block, stop_block)
                .await?;

            self.record_block_number(stop_block);
            start_block += batch_size;

            if start_block > final_stop_block {
                info!(
                    start_block,
                    stop_block = final_stop_block,
                    "Stopping onchain events sync"
                );
                return Ok(());
            }
        }
    }

    fn latest_block_in_db(&self) -> u64 {
        match self.local_state_store.get_latest_block_number() {
            Ok(number) => number.unwrap_or(0),
            Err(err) => {
                error!(
                    err = err.to_string(),
                    "Unable to retrieve last block number",
                );
                0
            }
        }
    }

    async fn latest_block_on_chain(&mut self) -> Result<u64, SubscribeError> {
        let mut retry_count = 0;
        loop {
            match self
                .provider
                .get_block_by_number(
                    alloy_rpc_types::BlockNumberOrTag::Latest,
                    alloy_rpc_types::BlockTransactionsKind::Hashes,
                )
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
                        "Error getting latest block on chain: {}. Retry {} in {} seconds",
                        err, retry_count, RETRY_TIMEOUT_SECONDS
                    );

                    tokio::time::sleep(tokio::time::Duration::from_secs(RETRY_TIMEOUT_SECONDS))
                        .await;
                }
            }
        }
    }

    async fn sync_live_events(&mut self, start_block_number: u64) -> Result<(), SubscribeError> {
        // Subscribe to new events starting from now.
        let filter = Filter::new()
            .address(vec![STORAGE_REGISTRY, KEY_REGISTRY, ID_REGISTRY])
            .from_block(start_block_number);

        let filter = match self.stop_block_number {
            None => filter,
            Some(stop_block) => filter.to_block(stop_block),
        };

        let subscription = self.provider.watch_logs(&filter).await?;
        let mut stream = subscription.into_stream();
        while let Some(events) = stream.next().await {
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
            "Starting l2 events subscription"
        );
        let live_sync_block;
        match self.start_block_number {
            None => {
                // By default, start from the first block or the latest block in the db. Whichever is higher
                live_sync_block = Some(FIRST_BLOCK.max(latest_block_in_db));
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
            info!("Historical sync complete. Not subscribing to live events");
            return Ok(());
        }

        loop {
            match self.sync_live_events(live_sync_block.unwrap()).await {
                Err(e) => {
                    error!("Live sync ended with error: {e}. Retrying in 10 seconds",);
                }
                _ => {
                    error!("Live sync ended unexpectedly. Retrying in 10 seconds",);
                }
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(RETRY_TIMEOUT_SECONDS)).await;
        }
    }
}
