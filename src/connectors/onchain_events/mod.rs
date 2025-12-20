use crate::cfg::Config as AppConfig;
use crate::proto::TierPurchaseBody;
use crate::storage::store::node_local_state;
use alloy_primitives::U256;
use alloy_primitives::{address, ruint::FromUintError, Address, FixedBytes};
use alloy_provider::{Provider, ProviderBuilder, RootProvider};
use alloy_rpc_types::{Filter, Log};
use alloy_sol_types::{sol, SolEvent, SolType};
use alloy_transport_http::{Client, Http};
use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use foundry_common::ens::EnsResolver::EnsResolverInstance;
use foundry_common::ens::{namehash, EnsError, EnsRegistry};
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::{
    sync::{broadcast, mpsc},
    task::spawn_blocking,
};
use tracing::{debug, error, info, warn};

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

sol! {
    /// SignedKeyRequest metadata structure as defined in the Farcaster contracts.
    /// See: https://github.com/farcasterxyz/contracts/blob/main/src/validators/SignedKeyRequestValidator.sol
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
const NAME_SERVICE_PROGRAM_ID: &str = "namesLPneVptA9Z5rqUDD9tMTWEJwofgaYwp8cawRkX";
const SOL_TLD_LABEL: &str = "sol";

// Solana Name Service derivation constants
const HASH_PREFIX: &str = "SPL Name Service";
const ROOT_DOMAIN_ACCOUNT: &str = "58PwtjSDuFHuUkYjH9BYnnQKHfwo9reZhC2zMJv9JPkx";

mod derivation {
    use super::*;
    use ed25519_dalek::VerifyingKey;

    /// Derive the domain key for a Solana Name Service domain.
    /// This is a minimal implementation based on the Bonfida SNS SDK and SPL Name Service.
    pub fn get_domain_key(domain: &str) -> Result<[u8; 32], String> {
        // Hash the domain name with the SNS prefix
        let hashed_name = hash_domain_name(domain);

        // Decode the root domain account
        let root_domain = bs58::decode(ROOT_DOMAIN_ACCOUNT)
            .into_vec()
            .map_err(|e| format!("Failed to decode root domain: {}", e))?;

        if root_domain.len() != 32 {
            return Err("Invalid root domain length".to_string());
        }
        let mut root_array = [0u8; 32];
        root_array.copy_from_slice(&root_domain);

        // Decode the name service program ID
        let program_id = bs58::decode(NAME_SERVICE_PROGRAM_ID)
            .into_vec()
            .map_err(|e| format!("Failed to decode program ID: {}", e))?;

        if program_id.len() != 32 {
            return Err("Invalid program ID length".to_string());
        }
        let mut program_array = [0u8; 32];
        program_array.copy_from_slice(&program_id);

        // Derive the PDA (Program Derived Address) using the SPL Name Service algorithm
        find_program_address(&program_array, &hashed_name, None, Some(&root_array))
    }

    fn hash_domain_name(domain: &str) -> Vec<u8> {
        let prefixed = format!("{}{}", HASH_PREFIX, domain);
        Sha256::digest(prefixed.as_bytes()).to_vec()
    }

    /// Find a program-derived address for the given seeds and program ID.
    /// Based on Solana's find_program_address implementation.
    fn find_program_address(
        program_id: &[u8; 32],
        hashed_name: &[u8],
        name_class: Option<&[u8; 32]>,
        parent: Option<&[u8; 32]>,
    ) -> Result<[u8; 32], String> {
        // Build the seeds vector according to SPL Name Service algorithm
        let mut seeds_vec = Vec::new();
        seeds_vec.extend_from_slice(hashed_name);

        // Add name_class (or zeros if None)
        if let Some(class) = name_class {
            seeds_vec.extend_from_slice(class);
        } else {
            seeds_vec.extend_from_slice(&[0u8; 32]);
        }

        // Add parent (or zeros if None)
        if let Some(parent_addr) = parent {
            seeds_vec.extend_from_slice(parent_addr);
        } else {
            seeds_vec.extend_from_slice(&[0u8; 32]);
        }

        // Split into 32-byte chunks as required by Solana PDA derivation
        let seed_chunks: Vec<&[u8]> = seeds_vec.chunks(32).collect();

        // Try bumps from 255 down to 0
        for bump in (0..=255u8).rev() {
            if let Ok(pda) = create_program_address(&seed_chunks, bump, program_id) {
                return Ok(pda);
            }
        }

        Err("Could not find valid PDA".to_string())
    }

    /// Create a program address from seeds and program ID.
    /// Based on Solana's create_program_address implementation.
    fn create_program_address(
        seeds: &[&[u8]],
        bump: u8,
        program_id: &[u8; 32],
    ) -> Result<[u8; 32], String> {
        // Hash: seeds || [bump] || program_id || "ProgramDerivedAddress"
        let mut hasher = Sha256::new();
        for seed in seeds {
            hasher.update(seed);
        }
        hasher.update(&[bump]);
        hasher.update(program_id);
        hasher.update(b"ProgramDerivedAddress");
        let hash = hasher.finalize();

        // Check if the resulting address is NOT on the Ed25519 curve
        if is_on_curve(&hash) {
            return Err("Address is on curve".to_string());
        }

        let mut result = [0u8; 32];
        result.copy_from_slice(&hash);
        Ok(result)
    }

    fn is_on_curve(pubkey: &[u8]) -> bool {
        if pubkey.len() != 32 {
            return false;
        }
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(pubkey);

        // Try to create a verifying key from the bytes
        // If it succeeds, the point is on the curve
        VerifyingKey::from_bytes(&bytes).is_ok()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub rpc_url: String,
    pub start_block_number: Option<u64>,
    pub stop_block_number: Option<u64>,
    pub override_tier_registry_address: Option<String>, // For testing
}

impl Default for Config {
    fn default() -> Config {
        return Config {
            rpc_url: String::new(),
            start_block_number: None,
            stop_block_number: None,
            override_tier_registry_address: None,
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

    match SignedKeyRequestMetadata::abi_decode(&signer_event_body.metadata, true) {
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

#[derive(Error, Debug)]
pub enum SolanaNameServiceError {
    #[error("solana name must end with .sol")]
    InvalidSuffix,
    #[error("solana name missing label")]
    MissingLabel,
    #[error("solana account owner mismatch")]
    InvalidAccountOwner,
    #[error("solana rpc error: {0}")]
    Rpc(String),
    #[error("solana account not found")]
    AccountNotFound,
    #[error("solana account data invalid: {0}")]
    AccountData(String),
    #[error("unable to decode solana account data: {0}")]
    Base64(String),
    #[error("failed to derive solana name address: {0}")]
    AddressDerivation(String),
}

#[async_trait]
pub trait SolanaNameResolver: Send + Sync {
    async fn resolve(&self, name: String) -> Result<Vec<u8>, SolanaNameServiceError>;
}

pub struct SolanaNameService {
    agent: Arc<ureq::Agent>,
    rpc_url: String,
}

impl SolanaNameService {
    pub fn new(rpc_url: String) -> Result<Self, SolanaNameServiceError> {
        let agent = Arc::new(ureq::Agent::new_with_defaults());
        Ok(Self { agent, rpc_url })
    }

    async fn fetch_account_owner(
        &self,
        account_pubkey: &[u8; 32],
    ) -> Result<Vec<u8>, SolanaNameServiceError> {
        let account_key = bs58::encode(account_pubkey).into_string();
        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": [account_key, { "encoding": "base64" }]
        });
        let agent = Arc::clone(&self.agent);
        let rpc_url = self.rpc_url.clone();
        let payload_string = payload.to_string();
        let rpc_response: RpcResponse<AccountInfo> = spawn_blocking(
            move || -> Result<RpcResponse<AccountInfo>, SolanaNameServiceError> {
                let mut response = agent
                    .post(&rpc_url)
                    .header("Content-Type", "application/json")
                    .send(payload_string.into_bytes())
                    .map_err(|err| SolanaNameServiceError::Rpc(err.to_string()))?;
                let body = response
                    .body_mut()
                    .read_to_string()
                    .map_err(|err| SolanaNameServiceError::Rpc(err.to_string()))?;
                serde_json::from_str::<RpcResponse<AccountInfo>>(&body)
                    .map_err(|err| SolanaNameServiceError::Rpc(err.to_string()))
            },
        )
        .await
        .map_err(|err| SolanaNameServiceError::Rpc(err.to_string()))??;

        if let Some(error) = rpc_response.error {
            return Err(SolanaNameServiceError::Rpc(error.message));
        }

        let account = rpc_response
            .result
            .and_then(|result| result.value)
            .ok_or(SolanaNameServiceError::AccountNotFound)?;

        if account.owner != NAME_SERVICE_PROGRAM_ID {
            return Err(SolanaNameServiceError::InvalidAccountOwner);
        }

        let data = BASE64_STANDARD
            .decode(account.data.0)
            .map_err(|err| SolanaNameServiceError::Base64(err.to_string()))?;

        if data.len() < 64 {
            return Err(SolanaNameServiceError::AccountData(
                "account data too short".to_string(),
            ));
        }

        Ok(data[32..64].to_vec())
    }

    async fn resolve_inner(&self, name: String) -> Result<Vec<u8>, SolanaNameServiceError> {
        let normalized = name.trim().to_lowercase();
        if !normalized.ends_with(".sol") {
            return Err(SolanaNameServiceError::InvalidSuffix);
        }
        let label = normalized.trim_end_matches(".sol");
        if label.is_empty() {
            return Err(SolanaNameServiceError::MissingLabel);
        }

        let name_account = Self::derive_domain_account(label)?;
        self.fetch_account_owner(&name_account).await
    }

    fn derive_domain_account(label: &str) -> Result<[u8; 32], SolanaNameServiceError> {
        derivation::get_domain_key(label)
            .map_err(|err| SolanaNameServiceError::AddressDerivation(err))
    }
}

#[derive(Deserialize)]
struct RpcResponse<T> {
    result: Option<RpcResult<T>>,
    error: Option<RpcError>,
}

#[derive(Deserialize)]
struct RpcResult<T> {
    value: Option<T>,
}

#[derive(Deserialize)]
struct RpcError {
    message: String,
}

#[derive(Deserialize)]
struct AccountInfo {
    data: (String, String),
    owner: String,
}

#[async_trait]
impl SolanaNameResolver for SolanaNameService {
    async fn resolve(&self, name: String) -> Result<Vec<u8>, SolanaNameServiceError> {
        self.resolve_inner(name).await
    }
}

// Quilibrium Name Service (QNS) support

#[derive(Error, Debug)]
pub enum QnsError {
    #[error("qns name must end with .q")]
    InvalidSuffix,
    #[error("qns name missing label")]
    MissingLabel,
    #[error("qns name not found: {0}")]
    NameNotFound(String),
    #[error("qns error: {0}")]
    Sdk(String),
    #[error("qns client not configured")]
    NotConfigured,
    #[error("qns invalid signature: {0}")]
    InvalidSignature(String),
}

/// Verify an ed448 signature for a QNS username proof.
/// Message format: "FARCASTER_QNS:" + name + ":" + big_endian_u64(fid)
pub fn verify_qns_signature(
    name: &str,
    fid: u64,
    public_key: &[u8],
    signature: &[u8],
) -> Result<(), QnsError> {
    use ed448_goldilocks_plus::{Signature, VerifyingKey};

    // Construct the message: "FARCASTER_QNS:" + name + ":" + big_endian_u64(fid)
    let mut message = Vec::new();
    message.extend_from_slice(b"FARCASTER_QNS:");
    message.extend_from_slice(name.as_bytes());
    message.extend_from_slice(b":");
    message.extend_from_slice(&fid.to_be_bytes());

    // Parse the public key (ed448 public keys are 57 bytes)
    let pubkey_bytes: [u8; 57] = public_key
        .try_into()
        .map_err(|_| QnsError::InvalidSignature("public key must be 57 bytes".to_string()))?;
    let pubkey = VerifyingKey::from_bytes(&pubkey_bytes)
        .map_err(|e| QnsError::InvalidSignature(format!("invalid public key: {:?}", e)))?;

    // Parse the signature (ed448 signatures are 114 bytes)
    let sig_bytes: [u8; 114] = signature
        .try_into()
        .map_err(|_| QnsError::InvalidSignature("signature must be 114 bytes".to_string()))?;
    let sig = Signature::from_bytes(&sig_bytes)
        .map_err(|e| QnsError::InvalidSignature(format!("invalid signature: {:?}", e)))?;

    // Verify the signature using Ed448 "pure" mode (no context)
    pubkey
        .verify_raw(&sig, &message)
        .map_err(|e| QnsError::InvalidSignature(format!("signature verification failed: {:?}", e)))
}

#[async_trait]
pub trait QnsResolver: Send + Sync {
    async fn resolve(&self, name: String) -> Result<Vec<u8>, QnsError>;
}

pub struct QuilibriumNameService {
    client: quilibrium_names_sdk::blocking::QnsClient,
}

impl QuilibriumNameService {
    pub fn new(rpc_url: String) -> Result<Self, QnsError> {
        let client = if rpc_url.is_empty() {
            quilibrium_names_sdk::blocking::QnsClient::default()
        } else {
            quilibrium_names_sdk::blocking::QnsClient::with_timeout(
                &rpc_url,
                std::time::Duration::from_secs(30),
            )
        };
        Ok(Self { client })
    }

    fn resolve_inner(&self, name: String) -> Result<Vec<u8>, QnsError> {
        let normalized = name.trim().to_lowercase();
        if !normalized.ends_with(".q") {
            return Err(QnsError::InvalidSuffix);
        }
        let label = normalized.trim_end_matches(".q");
        if label.is_empty() {
            return Err(QnsError::MissingLabel);
        }

        let record = self.client.resolve(&normalized).map_err(|e| match e {
            quilibrium_names_sdk::QnsError::NameNotFound(n) => QnsError::NameNotFound(n),
            other => QnsError::Sdk(other.to_string()),
        })?;

        // The authority_key is the owner's ed448 public key (hex string)
        // Convert it to bytes
        let owner_hex = record.header.authority_key.trim_start_matches("0x");
        let owner_bytes = hex::decode(owner_hex)
            .map_err(|e| QnsError::Sdk(format!("invalid owner hex: {}", e)))?;

        Ok(owner_bytes)
    }
}

#[async_trait]
impl QnsResolver for QuilibriumNameService {
    async fn resolve(&self, name: String) -> Result<Vec<u8>, QnsError> {
        // The SDK uses blocking calls, so we use resolve_inner which does sync I/O
        // This is acceptable because the SDK is designed for blocking use
        self.resolve_inner(name)
    }
}

#[cfg(test)]
mod sol_resolver_tests {
    use super::*;

    #[test]
    fn bonfida_sol_derives_expected_account() {
        let derived = SolanaNameService::derive_domain_account("bonfida").expect("derive bonfida");
        let expected_bytes = bs58::decode("Crf8hzfthWGbGbLTVCiqRqV5MVnbpHB1L9KQMd6gsinb")
            .into_vec()
            .expect("decode bonfida.sol");
        let expected: [u8; 32] = expected_bytes.try_into().expect("pubkey length");
        assert_eq!(derived, expected);
    }

    #[test]
    fn derivation_module_derives_bonfida_correctly() {
        // Test the derivation module directly
        let derived = derivation::get_domain_key("bonfida").expect("derive bonfida");
        let expected_bytes = bs58::decode("Crf8hzfthWGbGbLTVCiqRqV5MVnbpHB1L9KQMd6gsinb")
            .into_vec()
            .expect("decode bonfida.sol");
        let expected: [u8; 32] = expected_bytes.try_into().expect("pubkey length");

        assert_eq!(
            derived, expected,
            "Derived address for 'bonfida' should match expected Solana Name Service address"
        );

        // Verify the derived address as base58
        let derived_base58 = bs58::encode(&derived).into_string();
        assert_eq!(
            derived_base58, "Crf8hzfthWGbGbLTVCiqRqV5MVnbpHB1L9KQMd6gsinb",
            "Derived address should encode to expected base58 string"
        );
    }

    #[test]
    fn derivation_produces_off_curve_addresses() {
        // Test that derivation produces valid PDAs (off-curve addresses)
        use ed25519_dalek::VerifyingKey;

        let derived = derivation::get_domain_key("bonfida").expect("derive bonfida");

        // A valid PDA should NOT be a valid Ed25519 point
        assert!(
            VerifyingKey::from_bytes(&derived).is_err(),
            "Derived PDA should not be on the Ed25519 curve"
        );
    }

    #[test]
    fn derivation_is_deterministic() {
        // Test that derivation produces the same result every time
        let derived1 = derivation::get_domain_key("bonfida").expect("derive bonfida 1");
        let derived2 = derivation::get_domain_key("bonfida").expect("derive bonfida 2");
        let derived3 = derivation::get_domain_key("bonfida").expect("derive bonfida 3");

        assert_eq!(derived1, derived2, "Derivation should be deterministic");
        assert_eq!(derived2, derived3, "Derivation should be deterministic");
    }

    #[test]
    fn different_domains_produce_different_addresses() {
        // Test that different domain names produce different addresses
        let bonfida = derivation::get_domain_key("bonfida").expect("derive bonfida");
        let solana = derivation::get_domain_key("solana").expect("derive solana");

        assert_ne!(
            bonfida, solana,
            "Different domain names should produce different addresses"
        );
    }
}

#[cfg(test)]
mod qns_signature_tests {
    use super::*;

    #[test]
    fn test_verify_qns_signature_rejects_invalid_pubkey() {
        let name = "test.q";
        let fid: u64 = 12345;
        let invalid_pubkey = vec![0u8; 10]; // Too short
        let signature = vec![0u8; 114];

        let result = verify_qns_signature(name, fid, &invalid_pubkey, &signature);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("public key"),
            "Expected error about public key, got: {}",
            err_msg
        );
    }

    #[test]
    fn test_verify_qns_signature_rejects_invalid_signature() {
        let name = "test.q";
        let fid: u64 = 12345;
        // Valid length pubkey but random bytes
        let pubkey = vec![0u8; 57];
        let invalid_signature = vec![0u8; 10]; // Too short

        let result = verify_qns_signature(name, fid, &pubkey, &invalid_signature);
        assert!(result.is_err());
    }

    #[test]
    fn test_verify_qns_signature_constructs_correct_message() {
        // This test verifies the message format is correct by checking that
        // a valid keypair can sign and verify
        use ed448_goldilocks_plus::SigningKey;

        let name = "cassie.q";
        let fid: u64 = 12345;

        // Generate a test keypair
        let signing_key = SigningKey::generate(&mut rand::thread_rng());
        let verifying_key = signing_key.verifying_key();

        // Construct the expected message
        let mut message = Vec::new();
        message.extend_from_slice(b"FARCASTER_QNS:");
        message.extend_from_slice(name.as_bytes());
        message.extend_from_slice(b":");
        message.extend_from_slice(&fid.to_be_bytes());

        // Sign the message using raw mode (no context)
        let signature = signing_key.sign_raw(&message);

        // Verify using our function
        let pubkey_bytes = verifying_key.to_bytes();
        let sig_bytes = signature.to_bytes();
        let result = verify_qns_signature(name, fid, &pubkey_bytes, &sig_bytes);
        assert!(
            result.is_ok(),
            "Valid signature should verify: {:?}",
            result
        );
    }

    #[test]
    fn test_verify_qns_signature_rejects_wrong_fid() {
        use ed448_goldilocks_plus::SigningKey;

        let name = "cassie.q";
        let fid: u64 = 12345;
        let wrong_fid: u64 = 99999;

        // Generate a test keypair
        let signing_key = SigningKey::generate(&mut rand::thread_rng());
        let verifying_key = signing_key.verifying_key();

        // Construct message with correct fid
        let mut message = Vec::new();
        message.extend_from_slice(b"FARCASTER_QNS:");
        message.extend_from_slice(name.as_bytes());
        message.extend_from_slice(b":");
        message.extend_from_slice(&fid.to_be_bytes());

        // Sign the message
        let signature = signing_key.sign_raw(&message);

        // Get pubkey bytes
        let pubkey_bytes = verifying_key.to_bytes();
        let sig_bytes = signature.to_bytes();

        // Verify with wrong fid should fail
        let result = verify_qns_signature(name, wrong_fid, &pubkey_bytes, &sig_bytes);
        assert!(
            result.is_err(),
            "Signature with wrong fid should not verify"
        );
    }

    #[test]
    fn test_verify_qns_signature_rejects_wrong_name() {
        use ed448_goldilocks_plus::SigningKey;

        let name = "cassie.q";
        let wrong_name = "alice.q";
        let fid: u64 = 12345;

        // Generate a test keypair
        let signing_key = SigningKey::generate(&mut rand::thread_rng());
        let verifying_key = signing_key.verifying_key();

        // Construct message with correct name
        let mut message = Vec::new();
        message.extend_from_slice(b"FARCASTER_QNS:");
        message.extend_from_slice(name.as_bytes());
        message.extend_from_slice(b":");
        message.extend_from_slice(&fid.to_be_bytes());

        // Sign the message
        let signature = signing_key.sign_raw(&message);

        // Get pubkey bytes
        let pubkey_bytes = verifying_key.to_bytes();
        let sig_bytes = signature.to_bytes();

        // Verify with wrong name should fail
        let result = verify_qns_signature(wrong_name, fid, &pubkey_bytes, &sig_bytes);
        assert!(
            result.is_err(),
            "Signature with wrong name should not verify"
        );
    }
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
    pub solana_name_service: Option<Box<dyn SolanaNameResolver>>,
    pub qns_service: Option<Box<dyn QnsResolver>>,
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

        let solana_name_service: Option<Box<dyn SolanaNameResolver>> =
            if !app_config.solana_rpc_url.is_empty() {
                match SolanaNameService::new(app_config.solana_rpc_url.clone()) {
                    Ok(client) => Some(Box::new(client)),
                    Err(err) => {
                        warn!("Failed to initialize Solana name service: {}", err);
                        None
                    }
                }
            } else {
                None
            };

        let qns_service: Option<Box<dyn QnsResolver>> =
            match QuilibriumNameService::new(app_config.quilibrium_rpc_url.clone()) {
                Ok(client) => Some(Box::new(client)),
                Err(err) => {
                    warn!("Failed to initialize Quilibrium name service: {}", err);
                    None
                }
            };

        ChainClients {
            chain_api_map,
            solana_name_service,
            qns_service,
        }
    }

    pub fn for_chain(&self, chain: Chain) -> Result<&Box<dyn ChainAPI>, HubError> {
        match self.chain_api_map.get(&chain) {
            Some(client) => Ok(client),
            None => Err(HubError::invalid_internal_state(
                format!("No client configured for chain: {:?}", chain).as_str(),
            )),
        }
    }

    pub async fn resolve_sol_name(&self, name: String) -> Result<Vec<u8>, HubError> {
        match &self.solana_name_service {
            Some(client) => client
                .resolve(name)
                .await
                .map_err(|err| HubError::validation_failure(&err.to_string())),
            None => Err(HubError::invalid_internal_state(
                "Solana RPC client not configured",
            )),
        }
    }

    pub async fn resolve_qns_name(&self, name: String) -> Result<Vec<u8>, HubError> {
        match &self.qns_service {
            Some(client) => client
                .resolve(name)
                .await
                .map_err(|err| HubError::validation_failure(&err.to_string())),
            None => Err(HubError::invalid_internal_state(
                "Quilibrium name service client not configured",
            )),
        }
    }
}

pub struct RealL1Client {
    provider: RootProvider<Http<Client>>,
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
        let provider = ProviderBuilder::new().on_http(url);
        Ok(RealL1Client {
            provider,
            ens_resolver_address,
        })
    }
}

#[async_trait]
impl ChainAPI for RealL1Client {
    async fn resolve_ens_name(&self, name: String) -> Result<Address, EnsError> {
        // Copied from foundry_common::ens so we can support both ETH and Base mainnet
        let node = namehash(name.as_str());
        let ens_resolver_address = self.ens_resolver_address.ok_or(EnsError::ResolverNotFound(
            "no resolver address configured for chain".to_string(),
        ))?;
        let registry = EnsRegistry::new(ens_resolver_address, self.provider.clone());
        let address = registry
            .resolver(node)
            .call()
            .await
            .map_err(EnsError::Resolver)?
            ._0;
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
            })?
            ._0;
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
        }
    }
}

pub struct Subscriber {
    provider: RootProvider<Http<Client>>,
    mempool_tx: mpsc::Sender<MempoolRequest>,
    start_block_number: Option<u64>,
    stop_block_number: Option<u64>,
    statsd_client: StatsdClientWrapper,
    local_state_store: LocalStateStore,
    onchain_events_request_rx: broadcast::Receiver<OnchainEventsRequest>,
    chain: node_local_state::Chain,
    override_tier_registry_address: Option<String>,
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
        let provider = ProviderBuilder::new().on_http(url);
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
            override_tier_registry_address: config.override_tier_registry_address.clone(),
        })
    }

    fn contracts(&self) -> Vec<Contract> {
        match self.chain {
            node_local_state::Chain::Optimism => vec![
                Contract::storage_registry(),
                Contract::key_registry(),
                Contract::id_registry(),
            ],
            node_local_state::Chain::Base => vec![match &self.override_tier_registry_address {
                None => Contract::tier_registry(),
                Some(tier_registry_address) => Contract {
                    address: Address::from_str(&tier_registry_address).unwrap(),
                    kind: ContractKind::TierRegistry,
                },
            }],
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
    // polling. We're only seeing a few events per day, so this should be fine.
    async fn poll_tier_registry_events(
        &mut self,
        tier_registry: &Contract,
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

            // Create filter for tier_registry events
            let filter = Filter::new()
                .address(tier_registry.address)
                .from_block(*from_block)
                .to_block(to_block);

            info!(
                from_block = *from_block,
                to_block,
                chain = self.chain.to_string(),
                "Polling tier_registry events"
            );

            // Get and process logs
            match self.get_logs(&filter, tier_registry.event_kind()).await {
                Ok(_) => {
                    // Update the last processed block
                    *from_block = to_block + 1;
                    self.record_block_number(to_block);
                }
                Err(err) => {
                    error!(
                        chain = self.chain.to_string(),
                        "Error getting tier_registry logs: {}", err
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

        // Separate tier_registry from other contracts
        let mut tier_registry_contract: Option<Contract> = None;
        let mut other_contracts: Vec<Contract> = Vec::new();

        for contract in self.contracts() {
            match contract.kind {
                ContractKind::TierRegistry => {
                    tier_registry_contract = Some(contract);
                }
                _ => {
                    other_contracts.push(contract);
                }
            }
        }

        // Set up streaming for non-tier_registry contracts if any exist
        let mut stream = if !other_contracts.is_empty() {
            let contract_addresses: Vec<Address> = other_contracts
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

        // Set up polling for tier_registry if it exists
        let mut tier_registry_poll_interval = if tier_registry_contract.is_some() {
            Some(tokio::time::interval(tokio::time::Duration::from_secs(30)))
        } else {
            None
        };

        // Track the last block polled for tier_registry
        let mut tier_registry_last_block = start_block_number;

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
                     if let Some(ref mut interval) = tier_registry_poll_interval {
                         interval.tick().await;
                     } else {
                         // If no tier_registry, wait forever
                         futures_util::future::pending::<()>().await;
                     }
                 } => {
                     if let Some(ref tier_registry) = tier_registry_contract {
                         // Poll tier_registry events
                         if let Err(err) = self.poll_tier_registry_events(tier_registry, &mut tier_registry_last_block).await {
                             error!(
                                 chain = self.chain.to_string(),
                                 "Error polling tier_registry events: {}", err
                             );
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
}
