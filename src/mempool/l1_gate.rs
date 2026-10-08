//! Admission gate for mempool gossip on validators.
//!
//! ENS username proofs, `.eth` usernames and ERC-1271 verifications need an L1
//! check that consensus does not run (see `l1_validator`). The gRPC submit path
//! runs it; this gate runs the same check on messages that arrive over mempool
//! gossip, before they reach the block-producing mempool.
//!
//! The gate is local admission policy, not a consensus rule: validators that get
//! different L1 answers only differ in what they propose.
//!
//! Every other gossiped message is forwarded untouched. Messages that need a
//! check go to a worker task so L1 latency never stalls the caller or the
//! mempool loop. Since any peer can make every validator issue L1 calls by
//! gossiping these messages, the worker bounds that work:
//!
//! 1. Cheap local checks first: the message's hash and Ed25519 signature, and
//!    that its signer is an active key for its fid. This binds the fid, so the
//!    per-fid limit below cannot be charged to someone else's fid.
//! 2. A per-fid limit on these message types.
//! 3. A cap on L1 checks in flight. Messages beyond the cap, or beyond the
//!    worker's queue, are dropped rather than queued. A genuine message dropped
//!    this way is still proposed by the validator it was submitted to.
//!
//! The gate fails closed: a missing chain client, an RPC error or a timeout all
//! drop the message.

use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use governor::clock::QuantaClock;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter};
use moka::policy::EvictionPolicy;
use moka::sync::{Cache, CacheBuilder};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, Semaphore};
use tracing::{debug, error, warn};

use crate::connectors::onchain_events::Chain;
use crate::core::error::HubError;
use crate::core::util::FarcasterTime;
use crate::core::validations;
use crate::mempool::l1_validator::{L1CacheConfig, L1ValidationError, L1Validator};
use crate::mempool::mempool::{MempoolRequest, MempoolSource};
use crate::proto::{self, FarcasterNetwork};
use crate::storage::db::RocksDbTransactionBatch;
use crate::storage::store::account::get_active_key;
use crate::storage::store::mempool_poller::MempoolMessage;
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use crate::version::version::EngineVersion;

type DirectRateLimiter = RateLimiter<NotKeyed, InMemoryState, QuantaClock>;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    /// Gossiped messages waiting for the worker's local checks. Arrivals beyond
    /// this are dropped.
    pub queue_size: usize,
    /// L1 checks in flight. Messages that would exceed this are dropped.
    pub max_concurrent_checks: usize,
    /// Gossiped messages needing an L1 check that one fid may submit per minute.
    pub per_fid_checks_per_minute: u32,
    /// Budget for a single L1 check; exceeding it drops the message.
    #[serde(with = "humantime_serde")]
    pub check_timeout: Duration,
    /// How long a successful chain answer is reused, on both the gossip and
    /// gRPC paths.
    #[serde(with = "humantime_serde")]
    pub cache_ttl: Duration,
    pub cache_max_capacity: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            queue_size: 1024,
            max_concurrent_checks: 32,
            per_fid_checks_per_minute: 10,
            check_timeout: Duration::from_secs(10),
            cache_ttl: Duration::from_secs(60),
            cache_max_capacity: 100_000,
        }
    }
}

impl Config {
    /// The [`L1Validator`] cache settings carried in this config section.
    pub fn cache_config(&self) -> L1CacheConfig {
        L1CacheConfig {
            ttl: self.cache_ttl,
            max_capacity: self.cache_max_capacity,
        }
    }
}

/// Why the gate refused a message, as reported in metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Refusal {
    /// The message failed a local or L1 check.
    Rejected(&'static str),
    /// The gate was over capacity or the fid over its limit.
    Dropped(&'static str),
    /// The L1 check could not run.
    L1Error(&'static str),
}

/// Entry point for mempool requests that arrived over gossip. Cheap to call
/// from an event loop: it never awaits.
pub struct GossipL1Gate {
    checks_tx: mpsc::Sender<proto::Message>,
    mempool_tx: mpsc::Sender<MempoolRequest>,
    statsd_client: StatsdClientWrapper,
    l1_rejection_count: Arc<AtomicU64>,
}

impl GossipL1Gate {
    /// Builds the gate and the worker that runs its checks. The caller spawns
    /// [`GossipL1GateWorker::run`].
    pub fn new(
        config: Config,
        validator: Arc<L1Validator>,
        network: FarcasterNetwork,
        mempool_tx: mpsc::Sender<MempoolRequest>,
        statsd_client: StatsdClientWrapper,
    ) -> (Self, GossipL1GateWorker) {
        for chain in [Chain::EthMainnet, Chain::BaseMainnet] {
            if !validator.has_client(&chain) {
                warn!(
                    ?chain,
                    "No chain client configured; gossiped messages that need it will be rejected"
                );
            }
        }

        let (checks_tx, checks_rx) = mpsc::channel(config.queue_size);
        let l1_rejection_count = Arc::new(AtomicU64::new(0));
        let worker = GossipL1GateWorker {
            checks_rx,
            validator,
            network,
            mempool_tx: mempool_tx.clone(),
            in_flight: Arc::new(Semaphore::new(config.max_concurrent_checks)),
            rate_limits_by_fid: CacheBuilder::new(100_000)
                // 2x the one-minute quota window, so an idle entry is not evicted
                // and refilled while its window is still running.
                .time_to_idle(Duration::from_secs(120))
                .eviction_policy(EvictionPolicy::lru())
                .build(),
            config,
            statsd_client: statsd_client.clone(),
            l1_rejection_count: l1_rejection_count.clone(),
        };
        (
            Self {
                checks_tx,
                mempool_tx,
                statsd_client,
                l1_rejection_count,
            },
            worker,
        )
    }

    /// Lifetime count of gossiped messages rejected by their L1 check: a
    /// forged ENS proof, a `.eth` username without a valid proof, or a
    /// contract verification the contract refused. Mirrors
    /// `l1_gate.rejected{reason=l1_check_failed}` so tests can observe
    /// rejections without a statsd recorder. Messages refused before the L1
    /// check, dropped at capacity, or failing on RPC errors are not counted.
    pub fn l1_rejection_count_handle(&self) -> Arc<AtomicU64> {
        self.l1_rejection_count.clone()
    }

    /// Forwards `request` to the mempool, unless it is a gossiped message that
    /// needs an L1 check, in which case it is handed to the worker.
    pub fn admit(&self, request: MempoolRequest) {
        let request = match request {
            MempoolRequest::AddMessage(
                MempoolMessage::UserMessage(message),
                MempoolSource::Gossip,
                _,
            ) if L1Validator::requires_l1_validation(&message) => {
                match self.checks_tx.try_send(message) {
                    Ok(()) => {}
                    Err(TrySendError::Full(_)) => {
                        record(&self.statsd_client, Refusal::Dropped("queue_full"));
                    }
                    Err(TrySendError::Closed(_)) => {
                        error!("L1 gate worker has stopped; dropping gossiped message");
                        record(&self.statsd_client, Refusal::Dropped("worker_stopped"));
                    }
                }
                return;
            }
            request => request,
        };
        if let Err(e) = self.mempool_tx.try_send(request) {
            warn!("Failed to add to local mempool: {:?}", e);
        }
    }
}

pub struct GossipL1GateWorker {
    checks_rx: mpsc::Receiver<proto::Message>,
    validator: Arc<L1Validator>,
    network: FarcasterNetwork,
    mempool_tx: mpsc::Sender<MempoolRequest>,
    in_flight: Arc<Semaphore>,
    rate_limits_by_fid: Cache<u64, Arc<DirectRateLimiter>>,
    config: Config,
    statsd_client: StatsdClientWrapper,
    l1_rejection_count: Arc<AtomicU64>,
}

impl GossipL1GateWorker {
    pub async fn run(mut self) {
        while let Some(message) = self.checks_rx.recv().await {
            if let Err(refusal) = self.precheck(&message) {
                debug!(
                    fid = message.fid(),
                    ?refusal,
                    "Gossiped message refused before L1 check"
                );
                record(&self.statsd_client, refusal);
                continue;
            }
            let Ok(permit) = self.in_flight.clone().try_acquire_owned() else {
                record(&self.statsd_client, Refusal::Dropped("at_capacity"));
                continue;
            };

            let validator = self.validator.clone();
            let mempool_tx = self.mempool_tx.clone();
            let statsd_client = self.statsd_client.clone();
            let l1_rejection_count = self.l1_rejection_count.clone();
            let check_timeout = self.config.check_timeout;
            tokio::spawn(async move {
                let outcome = check_l1(&validator, &message, check_timeout).await;
                drop(permit);
                match outcome {
                    L1Outcome::Admitted => {
                        statsd_client.count("l1_gate.accepted", 1, vec![]);
                        if let Err(e) = mempool_tx.try_send(MempoolRequest::AddMessage(
                            MempoolMessage::UserMessage(message),
                            MempoolSource::Gossip,
                            None,
                        )) {
                            warn!("Failed to add to local mempool: {:?}", e);
                        }
                    }
                    // Logged at warn because only messages that passed the
                    // signer check and per-fid limit get here, so an attacker
                    // cannot flood the log without burning registered fids.
                    L1Outcome::Rejected(err) => {
                        warn!(
                            fid = message.fid(),
                            message_type = ?message.msg_type(),
                            hash = hex::encode(&message.hash),
                            error = %err,
                            "Rejected gossiped message that failed its L1 check"
                        );
                        record(&statsd_client, Refusal::Rejected("l1_check_failed"));
                        l1_rejection_count.fetch_add(1, Ordering::Relaxed);
                    }
                    L1Outcome::Unavailable(err) => {
                        debug!(fid = message.fid(), error = %err, "L1 check unavailable");
                        record(&statsd_client, Refusal::L1Error("unavailable"));
                    }
                    L1Outcome::TimedOut => {
                        debug!(fid = message.fid(), "L1 check timed out");
                        record(&statsd_client, Refusal::L1Error("timeout"));
                    }
                }
            });
        }
    }

    fn precheck(&self, message: &proto::Message) -> Result<(), Refusal> {
        // Consensus decides pro status. Claiming it here only relaxes limits,
        // so the gate never refuses what consensus would accept.
        validations::message::validate_message(
            message,
            self.network,
            true,
            &FarcasterTime::current(),
            EngineVersion::current(self.network),
        )
        .map_err(|_| Refusal::Rejected("invalid_message"))?;

        let fid = message.fid();
        let stores = self
            .validator
            .stores_for(fid)
            .ok_or(Refusal::Rejected("shard_not_hosted"))?;
        let active_key = get_active_key(
            &stores.onchain_event_store,
            &stores.db,
            &RocksDbTransactionBatch::new(),
            fid,
            &message.signer,
        )
        .map_err(|_| Refusal::Rejected("signer_lookup_failed"))?;
        if !active_key.is_some_and(|key| key.admits(message.msg_type())) {
            return Err(Refusal::Rejected("inactive_signer"));
        }

        if !self.consume_for_fid(fid) {
            return Err(Refusal::Dropped("rate_limited"));
        }
        Ok(())
    }

    fn consume_for_fid(&self, fid: u64) -> bool {
        let Some(quota) = NonZeroU32::new(self.config.per_fid_checks_per_minute) else {
            return false;
        };
        self.rate_limits_by_fid
            .get_with(fid, || {
                Arc::new(RateLimiter::direct(Quota::per_minute(quota)))
            })
            .check()
            .is_ok()
    }
}

/// How one L1 check ended, with the gate's timeout folded in.
enum L1Outcome {
    Admitted,
    Rejected(HubError),
    Unavailable(HubError),
    TimedOut,
}

async fn check_l1(
    validator: &L1Validator,
    message: &proto::Message,
    check_timeout: Duration,
) -> L1Outcome {
    match tokio::time::timeout(check_timeout, validator.validate_message(message)).await {
        Ok(Ok(())) => L1Outcome::Admitted,
        Ok(Err(L1ValidationError::Rejected(err))) => L1Outcome::Rejected(err),
        Ok(Err(L1ValidationError::Unavailable(err))) => L1Outcome::Unavailable(err),
        Err(_) => L1Outcome::TimedOut,
    }
}

fn record(statsd_client: &StatsdClientWrapper, refusal: Refusal) {
    let (metric, reason) = match refusal {
        Refusal::Rejected(reason) => ("l1_gate.rejected", reason),
        Refusal::Dropped(reason) => ("l1_gate.dropped", reason),
        Refusal::L1Error(reason) => ("l1_gate.l1_error", reason),
    };
    statsd_client.count(metric, 1, vec![("reason", reason)]);
}
