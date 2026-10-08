use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use ed25519_dalek::SigningKey;
use parking_lot::Mutex;
use tokio::sync::mpsc;

use crate::connectors::onchain_events::{Chain, ChainAPI, ChainClients, EnsError};
use crate::core::validations::error::ValidationError;
use crate::core::validations::verification::VerificationAddressClaim;
use crate::mempool::l1_gate::{Config, GossipL1Gate};
use crate::mempool::l1_validator::L1Validator;
use crate::mempool::mempool::{MempoolRequest, MempoolSource};
use crate::mempool::routing::ShardRouter;
use crate::proto::{self, FarcasterNetwork, MessageType, UserDataType, UserNameType};
use crate::storage::store::engine::ShardEngine;
use crate::storage::store::mempool_poller::MempoolMessage;
use crate::storage::store::test_helper;
use crate::utils::factory::{messages_factory, signers, time};

const FID: u64 = 1234;
const NAME: &str = "username.eth";
/// Generous, because a debug build is slow; only failures wait this long.
const FORWARD_WAIT: Duration = Duration::from_secs(5);
const NO_FORWARD_WAIT: Duration = Duration::from_secs(1);

fn custody_address() -> Vec<u8> {
    hex::decode("91031dcfdea024b4d51e775486111d2b2a715871").unwrap()
}

#[derive(Default)]
struct MockChain {
    ens_names: Mutex<HashMap<String, Vec<u8>>>,
    contract_signatures: Mutex<HashSet<Vec<u8>>>,
    delay: Mutex<Duration>,
    calls: AtomicUsize,
}

impl MockChain {
    async fn call(&self) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let delay = *self.delay.lock();
        tokio::time::sleep(delay).await;
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

struct MockChainClient(Arc<MockChain>);

#[async_trait]
impl ChainAPI for MockChainClient {
    async fn resolve_ens_name(&self, name: String) -> Result<alloy_primitives::Address, EnsError> {
        self.0.call().await;
        match self.0.ens_names.lock().get(&name) {
            Some(owner) => Ok(alloy_primitives::Address::from_slice(owner)),
            None => Err(EnsError::ResolverNotFound(name)),
        }
    }

    async fn verify_contract_signature(
        &self,
        _claim: VerificationAddressClaim,
        body: &proto::VerificationAddAddressBody,
    ) -> Result<(), ValidationError> {
        self.0.call().await;
        if self
            .0
            .contract_signatures
            .lock()
            .contains(&body.claim_signature)
        {
            Ok(())
        } else {
            Err(ValidationError::InvalidClaimSignature)
        }
    }
}

struct Harness {
    gate: GossipL1Gate,
    mempool_rx: mpsc::Receiver<MempoolRequest>,
    l1_rejections: Arc<AtomicU64>,
    chain: Arc<MockChain>,
    signer: SigningKey,
    _engine: ShardEngine,
    _dir: tempfile::TempDir,
}

impl Harness {
    async fn new(config: Config) -> Self {
        Self::with_chain_clients(config, true).await
    }

    async fn with_chain_clients(config: Config, configure_clients: bool) -> Self {
        let (mut engine, dir) = test_helper::new_engine().await;
        let signer = test_helper::default_signer();
        test_helper::register_user(FID, signer.clone(), custody_address(), &mut engine).await;

        let chain = Arc::new(MockChain::default());
        chain
            .ens_names
            .lock()
            .insert(NAME.to_string(), custody_address());
        let mut chain_clients = ChainClients {
            chain_api_map: HashMap::new(),
        };
        if configure_clients {
            for c in [Chain::EthMainnet, Chain::BaseMainnet] {
                chain_clients
                    .chain_api_map
                    .insert(c, Box::new(MockChainClient(chain.clone())));
            }
        }

        let mut shard_stores = HashMap::new();
        shard_stores.insert(1, engine.get_stores());
        let validator = Arc::new(L1Validator::new(
            chain_clients,
            shard_stores,
            Box::new(ShardRouter {}),
            1,
            FarcasterNetwork::Devnet,
            config.cache_config(),
        ));
        let (mempool_tx, mempool_rx) = mpsc::channel(100);
        let (gate, worker) = GossipL1Gate::new(
            config,
            validator,
            FarcasterNetwork::Devnet,
            mempool_tx,
            test_helper::statsd_client(),
        );
        tokio::spawn(worker.run());

        Self {
            l1_rejections: gate.l1_rejection_count_handle(),
            gate,
            mempool_rx,
            chain,
            signer,
            _engine: engine,
            _dir: dir,
        }
    }

    fn gossip(&self, message: &proto::Message) {
        self.gate.admit(MempoolRequest::AddMessage(
            MempoolMessage::UserMessage(message.clone()),
            MempoolSource::Gossip,
            None,
        ));
    }

    /// The next message forwarded to the mempool, or `None` if nothing arrives
    /// within `wait`.
    async fn next_forwarded(&mut self, wait: Duration) -> Option<proto::Message> {
        match tokio::time::timeout(wait, self.mempool_rx.recv()).await {
            Ok(Some(MempoolRequest::AddMessage(MempoolMessage::UserMessage(message), _, _))) => {
                Some(message)
            }
            Ok(Some(_)) => panic!("unexpected mempool request"),
            Ok(None) | Err(_) => None,
        }
    }

    async fn assert_forwarded(&mut self, expected: &proto::Message) {
        assert_eq!(
            self.next_forwarded(FORWARD_WAIT).await.as_ref(),
            Some(expected)
        );
    }

    async fn assert_nothing_forwarded(&mut self) {
        assert_eq!(self.next_forwarded(NO_FORWARD_WAIT).await, None);
    }

    fn l1_rejections(&self) -> u64 {
        self.l1_rejections.load(Ordering::Relaxed)
    }

    fn ens_proof(&self, owner: Vec<u8>, timestamp_offset: u32) -> proto::Message {
        ens_proof_signed_by(&self.signer, owner, timestamp_offset)
    }

    fn contract_verification(&self, claim_signature: Vec<u8>) -> proto::Message {
        messages_factory::create_message_with_data(
            FID,
            MessageType::VerificationAddEthAddress,
            proto::message_data::Body::VerificationAddAddressBody(
                proto::VerificationAddAddressBody {
                    address: vec![0x22; 20],
                    claim_signature,
                    block_hash: vec![0x11; 32],
                    verification_type: 1,
                    chain_id: 1,
                    protocol: proto::Protocol::Ethereum as i32,
                },
            ),
            None,
            Some(&self.signer),
        )
    }
}

fn ens_proof_signed_by(
    signer: &SigningKey,
    owner: Vec<u8>,
    timestamp_offset: u32,
) -> proto::Message {
    messages_factory::username_proof::create_username_proof(
        FID,
        UserNameType::UsernameTypeEnsL1,
        NAME.to_string(),
        owner,
        "signature".to_string(),
        (time::farcaster_time() - timestamp_offset) as u64,
        Some(signer),
    )
}

#[tokio::test]
async fn forwards_messages_that_need_no_l1_check() {
    let mut harness = Harness::new(Config::default()).await;
    let cast = messages_factory::casts::create_cast_add(FID, "hello", None, Some(&harness.signer));

    harness.gossip(&cast);

    harness.assert_forwarded(&cast).await;
    assert_eq!(harness.chain.calls(), 0);
}

#[tokio::test]
async fn admits_ens_proof_owned_by_custody_address() {
    let mut harness = Harness::new(Config::default()).await;
    let proof = harness.ens_proof(custody_address(), 0);

    harness.gossip(&proof);

    harness.assert_forwarded(&proof).await;
}

#[tokio::test]
async fn rejects_ens_proof_for_name_resolving_elsewhere() {
    let mut harness = Harness::new(Config::default()).await;
    // The publisher claims the name for an address ENS does not resolve it to.
    let forged = harness.ens_proof(vec![0x33; 20], 0);

    harness.gossip(&forged);

    harness.assert_nothing_forwarded().await;
    assert_eq!(harness.l1_rejections(), 1);
}

#[tokio::test]
async fn rejects_ens_proof_for_unresolvable_name() {
    let mut harness = Harness::new(Config::default()).await;
    harness.chain.ens_names.lock().clear();

    harness.gossip(&harness.ens_proof(custody_address(), 0));

    harness.assert_nothing_forwarded().await;
}

#[tokio::test]
async fn rejects_eth_username_without_stored_proof() {
    let mut harness = Harness::new(Config::default()).await;
    let user_data = messages_factory::user_data::create_user_data_add(
        FID,
        UserDataType::Username,
        &NAME.to_string(),
        None,
        Some(&harness.signer),
    );

    harness.gossip(&user_data);

    harness.assert_nothing_forwarded().await;
}

#[tokio::test]
async fn checks_contract_verification_signature() {
    let mut harness = Harness::new(Config::default()).await;
    harness
        .chain
        .contract_signatures
        .lock()
        .insert(vec![0xab; 65]);
    let genuine = harness.contract_verification(vec![0xab; 65]);
    let forged = harness.contract_verification(vec![0xcd; 65]);

    harness.gossip(&forged);
    harness.assert_nothing_forwarded().await;
    assert_eq!(harness.l1_rejections(), 1);

    harness.gossip(&genuine);
    harness.assert_forwarded(&genuine).await;
}

#[tokio::test]
async fn fails_closed_without_chain_client() {
    let mut harness = Harness::with_chain_clients(Config::default(), false).await;

    harness.gossip(&harness.ens_proof(custody_address(), 0));

    harness.assert_nothing_forwarded().await;
}

#[tokio::test]
async fn fails_closed_when_l1_check_times_out() {
    let mut harness = Harness::new(Config {
        check_timeout: Duration::from_millis(50),
        ..Config::default()
    })
    .await;
    *harness.chain.delay.lock() = Duration::from_millis(200);

    harness.gossip(&harness.ens_proof(custody_address(), 0));

    harness.assert_nothing_forwarded().await;
    assert_eq!(harness.l1_rejections(), 0);
}

#[tokio::test]
async fn rejects_inactive_signer_without_calling_l1() {
    let mut harness = Harness::new(Config::default()).await;
    // Signed by a key the fid never registered: the fid is spoofed.
    let spoofed = ens_proof_signed_by(&signers::generate_signer(), custody_address(), 0);

    harness.gossip(&spoofed);

    harness.assert_nothing_forwarded().await;
    assert_eq!(harness.chain.calls(), 0);
    assert_eq!(harness.l1_rejections(), 0);
}

#[tokio::test]
async fn per_fid_limit_drops_excess_messages() {
    let mut harness = Harness::new(Config {
        per_fid_checks_per_minute: 2,
        ..Config::default()
    })
    .await;

    for offset in 0..4 {
        harness.gossip(&harness.ens_proof(custody_address(), offset));
    }

    let mut forwarded = 0;
    while harness.next_forwarded(NO_FORWARD_WAIT).await.is_some() {
        forwarded += 1;
    }
    assert_eq!(forwarded, 2);
}

#[tokio::test]
async fn drops_messages_beyond_concurrency_cap() {
    let mut harness = Harness::new(Config {
        max_concurrent_checks: 1,
        ..Config::default()
    })
    .await;
    *harness.chain.delay.lock() = Duration::from_millis(100);
    let first = harness.ens_proof(custody_address(), 0);

    harness.gossip(&first);
    harness.gossip(&harness.ens_proof(custody_address(), 1));

    harness.assert_forwarded(&first).await;
    harness.assert_nothing_forwarded().await;
}

#[tokio::test]
async fn caches_ens_resolution_across_messages() {
    let mut harness = Harness::new(Config::default()).await;
    let first = harness.ens_proof(custody_address(), 0);
    let second = harness.ens_proof(custody_address(), 1);

    harness.gossip(&first);
    harness.assert_forwarded(&first).await;
    harness.gossip(&second);
    harness.assert_forwarded(&second).await;

    assert_eq!(harness.chain.calls(), 1);
}

#[test]
fn requires_l1_validation_only_for_affected_types() {
    let signer = test_helper::default_signer();
    let username = |value: &str| {
        messages_factory::user_data::create_user_data_add(
            FID,
            UserDataType::Username,
            &value.to_string(),
            None,
            Some(&signer),
        )
    };
    let verification = |verification_type: u32| {
        messages_factory::verifications::create_verification_add(
            FID,
            verification_type,
            vec![0x22; 20],
            vec![],
            vec![0x11; 32],
            None,
            Some(&signer),
        )
    };

    assert!(L1Validator::requires_l1_validation(&ens_proof_signed_by(
        &signer,
        custody_address(),
        0
    )));
    assert!(L1Validator::requires_l1_validation(&username("name.eth")));
    assert!(L1Validator::requires_l1_validation(&username(
        "name.base.eth"
    )));
    assert!(L1Validator::requires_l1_validation(&verification(1)));

    assert!(!L1Validator::requires_l1_validation(&username("fname")));
    assert!(!L1Validator::requires_l1_validation(&verification(0)));
    assert!(!L1Validator::requires_l1_validation(
        &messages_factory::casts::create_cast_add(FID, "x.eth", None, Some(&signer))
    ));
}
