use std::collections::HashMap;

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
    storage::store::{
        engine::MempoolMessage,
        node_local_state::{LocalStateStore, Registry},
    },
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

static CHAIN_ID: u32 = 10; // OP mainnet
const RENT_EXPIRY_IN_SECONDS: u64 = 365 * 24 * 60 * 60; // One year
const FIRST_BLOCK: u64 = 108864739;

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub rpc_url: String,
    pub start_block_number: u64,
    pub stop_block_number: Option<u64>,
}

impl Default for Config {
    fn default() -> Config {
        return Config {
            rpc_url: String::new(),
            start_block_number: FIRST_BLOCK,
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
    onchain_events_by_block: HashMap<u32, Vec<OnChainEvent>>,
    mempool_tx: mpsc::Sender<MempoolMessage>,
    start_block_number: u64,
    stop_block_number: Option<u64>,
    statsd_client: StatsdClientWrapper,
    local_state_store: LocalStateStore,
}

// TODO(aditi): Wait for 1 confirmation before "committing" an onchain event.
impl Subscriber {
    pub fn new(
        config: Config,
        mempool_tx: mpsc::Sender<MempoolMessage>,
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
            onchain_events_by_block: HashMap::new(),
            mempool_tx,
            start_block_number: config.start_block_number,
            stop_block_number: config.stop_block_number,
            statsd_client,
        })
    }

    fn count(&self, key: &str, value: u64) {
        self.statsd_client
            .count(format!("onchain_events.{}", key).as_str(), value);
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
        let events = self.onchain_events_by_block.get_mut(&block_number);
        match events {
            None => {
                self.onchain_events_by_block
                    .insert(block_number, vec![event.clone()]);
            }
            Some(events) => events.push(event.clone()),
        }
        if let Err(err) = self
            .mempool_tx
            .send(MempoolMessage::ValidatorMessage(ValidatorMessage {
                on_chain_event: Some(event.clone()),
                fname_transfer: None,
            }))
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

    fn record_block_number(&self, event: &Log) {
        match event.block_number {
            Some(block_number) => {
                match Self::get_registry(event) {
                    Some(registry) => {
                        if block_number as u64 > self.latest_block_in_db(registry) {
                            match self
                                .local_state_store
                                .set_latest_block_number(block_number, registry)
                            {
                                Err(err) => {
                                    error!(
                                        block_number,
                                        err = err.to_string(),
                                        "Unable to store last block number",
                                    );
                                }
                                _ => {}
                            }
                        }
                    }
                    None => {}
                };
            }
            None => {}
        }
    }

    async fn get_block_timestamp(&self, block_hash: FixedBytes<32>) -> Result<u64, SubscribeError> {
        let block = self
            .provider
            .get_block_by_hash(block_hash, alloy_rpc_types::BlockTransactionsKind::Hashes)
            .await?
            .ok_or(SubscribeError::UnableToFindBlockByHash)?;
        Ok(block.header.timestamp)
    }

    fn get_registry(event: &Log) -> Option<Registry> {
        match event.topic0()? {
            &StorageRegistryAbi::Rent::SIGNATURE_HASH => Some(Registry::StorageRegistry),
            &IdRegistryAbi::Register::SIGNATURE_HASH => Some(Registry::IdRegistry),
            &IdRegistryAbi::Transfer::SIGNATURE_HASH => Some(Registry::IdRegistry),
            &IdRegistryAbi::ChangeRecoveryAddress::SIGNATURE_HASH => Some(Registry::IdRegistry),
            &KeyRegistryAbi::Add::SIGNATURE_HASH => Some(Registry::KeyRegistry),
            &KeyRegistryAbi::Remove::SIGNATURE_HASH => Some(Registry::KeyRegistry),
            &KeyRegistryAbi::AdminReset::SIGNATURE_HASH => Some(Registry::KeyRegistry),
            &KeyRegistryAbi::Migrated::SIGNATURE_HASH => Some(Registry::KeyRegistry),
            _ => None,
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

    pub async fn sync_historical_events(
        &mut self,
        address: Address,
        final_stop_block: u64,
    ) -> Result<(), SubscribeError> {
        let registry = if address == STORAGE_REGISTRY {
            Registry::StorageRegistry
        } else if address == ID_REGISTRY {
            Registry::IdRegistry
        } else if address == KEY_REGISTRY {
            Registry::KeyRegistry
        } else {
            panic!("Invalid address for contract")
        };
        let batch_size = 1000;
        let mut start_block = self
            .start_block_number
            .max(self.latest_block_in_db(registry));
        loop {
            let stop_block = final_stop_block.min(start_block + batch_size);
            let filter = Filter::new()
                .address(address)
                .from_block(start_block)
                .to_block(stop_block);
            info!(
                start_block,
                stop_block, "Syncing historical events in range"
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
                    Ok(()) => {
                        // TODO(aditi): Consider recording the block number only after the last event in the batch. Right now, batches are pretty large so we wouldn't be recording frequently enough
                        self.record_block_number(&event);
                    }
                }
            }
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

    fn latest_block_in_db(&self, registry: Registry) -> u64 {
        match self.local_state_store.get_latest_block_number(registry) {
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
        let block = self
            .provider
            .get_block_by_number(
                alloy_rpc_types::BlockNumberOrTag::Latest,
                alloy_rpc_types::BlockTransactionsKind::Hashes,
            )
            .await?;
        Ok(block
            .ok_or(SubscribeError::LogMissingBlockNumber)?
            .header
            .number)
    }

    pub async fn run(&mut self) -> Result<(), SubscribeError> {
        let latest_block_on_chain = self.latest_block_on_chain().await?;
        info!(
            start_block_number = self.start_block_number,
            stop_block_numer = self.stop_block_number,
            latest_block_on_chain,
            "Starting l2 events subscription"
        );
        let historical_sync_stop_block =
            latest_block_on_chain.min(self.stop_block_number.unwrap_or(latest_block_on_chain));
        self.sync_historical_events(STORAGE_REGISTRY, historical_sync_stop_block)
            .await?;
        self.sync_historical_events(ID_REGISTRY, historical_sync_stop_block)
            .await?;
        self.sync_historical_events(KEY_REGISTRY, historical_sync_stop_block)
            .await?;

        let should_start_live_sync = match self.stop_block_number {
            None => true,
            Some(stop_block) => stop_block > historical_sync_stop_block,
        };

        if should_start_live_sync {
            // Subscribe to new events starting from now.
            let filter = Filter::new()
                .address(vec![STORAGE_REGISTRY, KEY_REGISTRY, ID_REGISTRY])
                .from_block(historical_sync_stop_block);
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
                        Ok(()) => {
                            self.record_block_number(&event);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
