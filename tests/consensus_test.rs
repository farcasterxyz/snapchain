use libp2p::identity::ed25519::Keypair;
use malachite_config::TimeoutConfig;
use malachite_consensus::Params as ConsensusParams;
use malachite_metrics::{Metrics, SharedRegistry};
use ractor::{Actor, ActorRef};
use tokio::{select, time};
use tokio::sync::mpsc;

use snapchain::{
    consensus::consensus::{Consensus, ConsensusMsg},
    core::types::{
        Address, Height, ShardId, SnapchainShard, SnapchainValidator, SnapchainValidatorContext,
        SnapchainValidatorSet,
    },
    network::gossip::GossipEvent,
};
use snapchain::consensus::consensus::{Decision, TxDecision};

struct NodeForTest {
    shard: SnapchainShard,
    keypair: Keypair,
    validator_set: SnapchainValidatorSet,
    gossip_rx: mpsc::Receiver<GossipEvent<SnapchainValidatorContext>>,
    gossip_tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
    consensus_actor: ActorRef<ConsensusMsg<SnapchainValidatorContext>>,
    decision_rx: mpsc::Receiver<Decision<SnapchainValidatorContext>>,
}

impl NodeForTest {
    pub async fn create(shard: SnapchainShard, keypair: Keypair, validator_set: SnapchainValidatorSet, metrics: Metrics) -> Self {
        let address = Address(keypair.public().to_bytes());
        let consensus_params = ConsensusParams {
            start_height: Height::new(shard.shard_id(), 1),
            initial_validator_set: validator_set.clone(),
            address: address.clone(),
            threshold_params: Default::default(),
        };

        // Create validator context
        let ctx = SnapchainValidatorContext::new(keypair.clone());

        let (gossip_tx, gossip_rx) = mpsc::channel::<GossipEvent<SnapchainValidatorContext>>(100);
        let (decision_tx, decision_rx) = mpsc::channel::<Decision<SnapchainValidatorContext>>(100);

        // Spawn consensus actor
        let consensus_actor = Consensus::spawn(
            ctx,
            shard.clone(),
            consensus_params,
            TimeoutConfig::default(),
            metrics.clone(),
            Some(decision_tx),
            gossip_tx.clone(),
        ).await.unwrap();
        Self {
            shard,
            keypair,
            validator_set,
            consensus_actor,
            gossip_rx,
            gossip_tx,
            decision_rx,
        }
    }

    pub fn cast(&self, msg: ConsensusMsg<SnapchainValidatorContext>) {
        self.consensus_actor.cast(msg).unwrap()
    }

    pub fn stop(&self) {
        self.consensus_actor.stop(None);
    }
}

#[tokio::test]
async fn test_basic_consensus() {
    // Set up test environment
    let registry = SharedRegistry::global();
    let metrics = Metrics::register(registry);

    // Create validator keys
    let keypair1 = Keypair::generate();
    let keypair2 = Keypair::generate();
    let keypair3 = Keypair::generate();

    let validator1_address = Address(keypair1.public().to_bytes());

    // Set up shard and validators
    let shard = SnapchainShard::new(0);
    let validator1 = SnapchainValidator::new(shard.clone(), keypair1.public().clone());
    let validator2 = SnapchainValidator::new(shard.clone(), keypair2.public().clone());
    let validator3 = SnapchainValidator::new(shard.clone(), keypair3.public().clone());

    // Create validator set with all validators
    let validator_set = SnapchainValidatorSet::new(vec![
        validator1.clone(),
        validator2.clone(),
        validator3.clone(),
    ]);

    let mut node1 = NodeForTest::create(shard.clone(), keypair1.clone(), validator_set.clone(), metrics.clone()).await;
    let mut node2 = NodeForTest::create(shard.clone(), keypair2.clone(), validator_set.clone(), metrics.clone()).await;
    let mut node3 = NodeForTest::create(shard.clone(), keypair3.clone(), validator_set.clone(), metrics.clone()).await;

    // Register validators
    for validator in validator_set.validators {
        node1.cast(ConsensusMsg::RegisterValidator(validator.clone()));
        node2.cast(ConsensusMsg::RegisterValidator(validator.clone()));
        node3.cast(ConsensusMsg::RegisterValidator(validator.clone()));
    }

    // Kick off consensus
    node1.cast(ConsensusMsg::StartHeight(Height::new(shard.shard_id(), 1)));

    let mut blocks_count = 0;

    // Wait for gossip messages with a timeout
    let timeout = tokio::time::Duration::from_secs(10);
    let start = tokio::time::Instant::now();
    let mut timer = time::interval(tokio::time::Duration::from_secs(1));

    while blocks_count < 3 {
        select! {
            Some(decision) = node1.decision_rx.recv() => {
                match decision {
                    (height, round, value) => {
                        println!("Node 1: Decided block at height {}, round {}, value: {}", height, round, value);
                        blocks_count += 1;
                    }
                }
            }

            // Wire up the gossip messages to the other nodes
            // TODO: dedup
            Some(gossip_event) = node1.gossip_rx.recv() => {
                match gossip_event {
                    GossipEvent::BroadcastSignedProposal(proposal) => {
                        node2.cast(ConsensusMsg::ReceivedSignedProposal(proposal.clone()));
                        node3.cast(ConsensusMsg::ReceivedSignedProposal(proposal.clone()));
                    },
                    GossipEvent::BroadcastSignedVote(vote) => {
                        node2.cast(ConsensusMsg::ReceivedSignedVote(vote.clone()));
                        node3.cast(ConsensusMsg::ReceivedSignedVote(vote.clone()));
                    }
                    _ => {}}
            }
            Some(gossip_event) = node2.gossip_rx.recv() => {
                match gossip_event {
                    GossipEvent::BroadcastSignedProposal(proposal) => {
                        node1.cast(ConsensusMsg::ReceivedSignedProposal(proposal.clone()));
                        node3.cast(ConsensusMsg::ReceivedSignedProposal(proposal.clone()));
                    },
                    GossipEvent::BroadcastSignedVote(vote) => {
                        node1.cast(ConsensusMsg::ReceivedSignedVote(vote.clone()));
                        node3.cast(ConsensusMsg::ReceivedSignedVote(vote.clone()));
                    }
                    _ => {}}
            }
            Some(gossip_event) = node3.gossip_rx.recv() => {
                match gossip_event {
                    GossipEvent::BroadcastSignedProposal(proposal) => {
                        node1.cast(ConsensusMsg::ReceivedSignedProposal(proposal.clone()));
                        node2.cast(ConsensusMsg::ReceivedSignedProposal(proposal.clone()));
                    },
                    GossipEvent::BroadcastSignedVote(vote) => {
                        node1.cast(ConsensusMsg::ReceivedSignedVote(vote.clone()));
                        node2.cast(ConsensusMsg::ReceivedSignedVote(vote.clone()));
                    }
                    _ => {}}
            }

            _ = timer.tick() => {
                if start.elapsed() > timeout {
                    panic!("Test timed out waiting for blocks");
                }
            }
        }
    }

    assert!(blocks_count > 0, "Should have confirmed blocks");

    // Clean up
    node1.stop();
    node2.stop();
    node3.stop();
}