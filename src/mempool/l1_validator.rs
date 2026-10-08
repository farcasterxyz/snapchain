//! L1/Base-backed admission checks for the user messages whose validity
//! consensus cannot decide: ENS username proofs, `.eth` usernames and ERC-1271
//! (`verification_type = 1`) address verifications.
//!
//! Consensus only checks that these messages are well-formed, because the
//! answers depend on chain RPCs that validators may see differently. The checks
//! here therefore have to run before a message enters a validator's mempool, on
//! every ingress path: the gRPC submit path and the mempool gossip gate
//! (`l1_gate`).

use std::collections::HashMap;
use std::time::Duration;

use moka::policy::EvictionPolicy;
use moka::sync::{Cache, CacheBuilder};
use sha2::{Digest, Sha256};

use crate::connectors::onchain_events::{Chain, ChainClients, EnsError};
use crate::core::error::HubError;
use crate::core::validations;
use crate::mempool::routing::MessageRouter;
use crate::proto::{
    self, on_chain_event, FarcasterNetwork, UserNameProof, UserNameType, VerificationAddAddressBody,
};
use crate::storage::db::RocksDbTransactionBatch;
use crate::storage::store::account::{UsernameProofStore, VerificationStore};
use crate::storage::store::stores::Stores;

/// Why an L1 check did not admit a message.
#[derive(Debug, Clone)]
pub enum L1ValidationError {
    /// The message fails the check: the name resolves elsewhere, the contract
    /// rejects the signature, or local state does not back the claim.
    Rejected(HubError),
    /// The check could not run: no client is configured for the chain, or the
    /// RPC failed. Callers must treat this as a rejection (fail closed).
    Unavailable(HubError),
}

impl From<L1ValidationError> for HubError {
    fn from(err: L1ValidationError) -> Self {
        match err {
            L1ValidationError::Rejected(err) | L1ValidationError::Unavailable(err) => err,
        }
    }
}

impl From<HubError> for L1ValidationError {
    fn from(err: HubError) -> Self {
        L1ValidationError::Rejected(err)
    }
}

fn rejected(message: &str) -> L1ValidationError {
    L1ValidationError::Rejected(HubError::validation_failure(message))
}

/// Tuning for [`L1Validator`]'s result cache.
#[derive(Debug, Clone)]
pub struct L1CacheConfig {
    pub ttl: Duration,
    pub max_capacity: u64,
}

impl Default for L1CacheConfig {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(60),
            max_capacity: 100_000,
        }
    }
}

/// Runs the L1-backed checks against the configured chain clients and this
/// node's shard stores.
///
/// Successful chain answers are cached for a short TTL, keyed by
/// `(username type, name)` for ENS resolution and by a digest of the full claim
/// and signature for contract signatures, so replays of a valid message cost no
/// further RPC calls. Failures are not cached: they are bounded by the gate's
/// per-fid limit and concurrency cap instead, and a transient RPC error must not
/// pin a genuine message out for the TTL.
pub struct L1Validator {
    chain_clients: ChainClients,
    shard_stores: HashMap<u32, Stores>,
    message_router: Box<dyn MessageRouter>,
    num_shards: u32,
    network: FarcasterNetwork,
    ens_resolutions: Cache<(i32, String), Vec<u8>>,
    valid_contract_signatures: Cache<[u8; 32], ()>,
}

impl L1Validator {
    pub fn new(
        chain_clients: ChainClients,
        shard_stores: HashMap<u32, Stores>,
        message_router: Box<dyn MessageRouter>,
        num_shards: u32,
        network: FarcasterNetwork,
        cache_config: L1CacheConfig,
    ) -> Self {
        Self {
            chain_clients,
            shard_stores,
            message_router,
            num_shards,
            network,
            ens_resolutions: build_cache(&cache_config),
            valid_contract_signatures: build_cache(&cache_config),
        }
    }

    /// Whether `message` needs an L1 check before admission.
    pub fn requires_l1_validation(message: &proto::Message) -> bool {
        match message.data.as_ref().and_then(|data| data.body.as_ref()) {
            Some(proto::message_data::Body::UserDataBody(user_data)) => {
                user_data.r#type() == proto::UserDataType::Username
                    && user_data.value.ends_with(".eth")
            }
            Some(proto::message_data::Body::UsernameProofBody(_)) => true,
            Some(proto::message_data::Body::VerificationAddAddressBody(body)) => {
                body.verification_type == 1
            }
            _ => false,
        }
    }

    /// Whether a client is configured for `chain`. Without one, every check
    /// that needs it fails closed.
    pub fn has_client(&self, chain: &Chain) -> bool {
        self.chain_clients.chain_api_map.contains_key(chain)
    }

    /// The stores for `fid`'s data shard, if this node hosts it.
    pub fn stores_for(&self, fid: u64) -> Option<&Stores> {
        let shard_id = self.message_router.route_fid(fid, self.num_shards);
        self.shard_stores.get(&shard_id)
    }

    /// Runs the L1 check `message` needs, if any.
    pub async fn validate_message(
        &self,
        message: &proto::Message,
    ) -> Result<(), L1ValidationError> {
        let Some(message_data) = &message.data else {
            return Ok(());
        };
        let fid = message_data.fid;
        match &message_data.body {
            Some(proto::message_data::Body::UserDataBody(user_data)) => {
                if user_data.r#type() == proto::UserDataType::Username
                    && user_data.value.ends_with(".eth")
                {
                    self.validate_ens_username(fid, &user_data.value).await?;
                }
                Ok(())
            }
            Some(proto::message_data::Body::UsernameProofBody(proof)) => {
                self.validate_ens_username_proof(fid, proof).await
            }
            Some(proto::message_data::Body::VerificationAddAddressBody(body)) => {
                if body.verification_type == 1 {
                    let claim = validations::verification::make_verification_address_claim(
                        fid,
                        &body.address,
                        self.network,
                        &body.block_hash,
                        proto::Protocol::Ethereum,
                    )
                    .map_err(|err| {
                        rejected(&format!(
                            "could not create verification address claim: {}",
                            err
                        ))
                    })?;
                    self.validate_contract_signature(fid, claim, body).await?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    pub async fn validate_contract_signature(
        &self,
        fid: u64,
        claim: validations::verification::VerificationAddressClaim,
        body: &VerificationAddAddressBody,
    ) -> Result<(), L1ValidationError> {
        let chain = Chain::from_chain_id(body.chain_id).ok_or(rejected("invalid chain id"))?;
        let client = self
            .chain_clients
            .for_chain(chain)
            .map_err(L1ValidationError::Unavailable)?;

        let cache_key = contract_signature_cache_key(fid, self.network, body);
        if self.valid_contract_signatures.contains_key(&cache_key) {
            return Ok(());
        }

        // `verify_contract_signature` folds RPC failures into
        // `InvalidClaimSignature`, so they surface here as rejections.
        client
            .verify_contract_signature(claim, body)
            .await
            .map_err(|e| rejected(&format!("could not verify contract signature: {}", e)))?;
        self.valid_contract_signatures.insert(cache_key, ());
        Ok(())
    }

    pub async fn validate_ens_username_proof(
        &self,
        fid: u64,
        proof: &UserNameProof,
    ) -> Result<(), L1ValidationError> {
        let resolved_ens_address = self.resolve_ens_address(proof).await?;
        if resolved_ens_address != proof.owner {
            return Err(rejected(
                "invalid ens name, resolved address doesn't match proof owner address",
            ));
        }

        let stores = self
            .stores_for(fid)
            .ok_or_else(|| HubError::internal_db_error("stores not found for fid"))?;

        let id_register = stores
            .onchain_event_store
            .get_id_register_event_by_fid(fid, None)
            .map_err(|_| HubError::internal_db_error("Could not fetch id registration"))?;

        let custody_address = match id_register.and_then(|event| event.body) {
            Some(on_chain_event::Body::IdRegisterEventBody(id_register)) => id_register.to,
            _ => return Err(rejected("missing fid registration")),
        };

        // Check verified addresses if the resolved address doesn't match the custody address
        if custody_address != resolved_ens_address {
            let verification = VerificationStore::get_verification_add(
                &stores.verification_store,
                fid,
                &resolved_ens_address,
                None,
            )?;
            if verification.is_none() {
                return Err(rejected(
                    "invalid ens proof, no matching custody address or verified addresses",
                ));
            }
        }
        Ok(())
    }

    async fn resolve_ens_address(
        &self,
        proof: &UserNameProof,
    ) -> Result<Vec<u8>, L1ValidationError> {
        let name =
            std::str::from_utf8(&proof.name).map_err(|_| rejected("ENS name is not utf8"))?;

        let chain = match UserNameType::try_from(proof.r#type) {
            Ok(UserNameType::UsernameTypeEnsL1) => {
                if !name.ends_with(".eth") {
                    return Err(rejected("ENS name does not end with .eth"));
                }
                Chain::EthMainnet
            }
            Ok(UserNameType::UsernameTypeBasename) => {
                if !name.ends_with(".base.eth") {
                    return Err(rejected("Basename does not end with base.eth"));
                }
                Chain::BaseMainnet
            }
            _ => {
                return Err(rejected(&format!(
                    "unsupported username type: {} for name: {}",
                    proof.r#type, name,
                )))
            }
        };
        let chain_api = self
            .chain_clients
            .for_chain(chain)
            .map_err(L1ValidationError::Unavailable)?;

        let cache_key = (proof.r#type, name.to_string());
        if let Some(address) = self.ens_resolutions.get(&cache_key) {
            return Ok(address);
        }

        let address = chain_api
            .resolve_ens_name(name.to_string())
            .await
            .map_err(|err| {
                let hub_error =
                    HubError::validation_failure(&format!("ENS resolution error: {}", err));
                if is_rpc_failure(&err) {
                    L1ValidationError::Unavailable(hub_error)
                } else {
                    L1ValidationError::Rejected(hub_error)
                }
            })?
            .to_vec();
        self.ens_resolutions.insert(cache_key, address.clone());
        Ok(address)
    }

    pub async fn validate_ens_username(
        &self,
        fid: u64,
        name: &str,
    ) -> Result<(), L1ValidationError> {
        let stores = self
            .stores_for(fid)
            .ok_or_else(|| HubError::invalid_parameter("stores not found for fid"))?;
        let proof_message = UsernameProofStore::get_username_proof(
            &stores.username_proof_store,
            &name.as_bytes().to_vec(),
            &mut RocksDbTransactionBatch::new(),
        )?
        .ok_or(rejected("username proof missing proof"))?;
        let message_data = proof_message
            .data
            .ok_or(rejected("username proof missing data"))?;
        match message_data.body {
            Some(proto::message_data::Body::UsernameProofBody(proof)) => {
                self.validate_ens_username_proof(fid, &proof).await
            }
            Some(_) => Err(rejected("username proof has wrong type")),
            None => Err(rejected("username proof missing body")),
        }
    }
}

fn build_cache<K, V>(config: &L1CacheConfig) -> Cache<K, V>
where
    K: std::hash::Hash + Eq + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    CacheBuilder::new(config.max_capacity)
        .time_to_live(config.ttl)
        .eviction_policy(EvictionPolicy::lru())
        .build()
}

/// True for a transport failure with no revert data: the chain gave no answer,
/// as opposed to answering that the name has no resolver or address.
fn is_rpc_failure(err: &EnsError) -> bool {
    match err {
        EnsError::Resolver(e) | EnsError::Resolve(e) => {
            matches!(e, alloy_contract::Error::TransportError(_)) && e.as_revert_data().is_none()
        }
        EnsError::ResolverNotFound(_) => false,
    }
}

/// Covers every input `verify_contract_signature` sees, including the claim
/// signature, so a cached verdict for one signature never admits another.
fn contract_signature_cache_key(
    fid: u64,
    network: FarcasterNetwork,
    body: &VerificationAddAddressBody,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(fid.to_be_bytes());
    hasher.update((network as i32).to_be_bytes());
    hasher.update(body.chain_id.to_be_bytes());
    for field in [&body.address, &body.block_hash, &body.claim_signature] {
        hasher.update((field.len() as u64).to_be_bytes());
        hasher.update(field);
    }
    hasher.finalize().into()
}
