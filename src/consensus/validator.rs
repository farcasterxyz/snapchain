use crate::consensus::proposer::{BlockProposer, Proposer, ShardProposer};
use crate::core::types::{
    Address, Height, ShardId, SnapchainShard, SnapchainValidator, SnapchainValidatorContext,
    SnapchainValidatorSet,
};
use crate::proto::{full_proposal, Commits, FullProposal, ShardHash};
use crate::storage::store::node_local_state::LocalStateStore;
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use informalsystems_malachitebft_core_consensus::ProposedValue;
use informalsystems_malachitebft_core_types::{Round, ValidatorSet};
use std::time::Duration;
use tracing::{error, warn};

pub struct ShardValidator {
    pub(crate) shard_id: SnapchainShard,

    #[allow(dead_code)] // TODO
    address: Address,

    validator_set: SnapchainValidatorSet,
    current_round: Round,
    current_height: Option<Height>,
    current_proposer: Option<Address>,
    height_started_at: Option<std::time::Instant>,
    proposed_at: Option<std::time::Instant>,
    // This should be proposer: Box<dyn Proposer> but that doesn't implement Send which is required for the actor system.
    // TODO: Fix once we remove the actor system
    block_proposer: Option<BlockProposer>,
    shard_proposer: Option<ShardProposer>,
    pub started: bool,
    local_state_store: LocalStateStore,
    pub statsd: StatsdClientWrapper,
}

impl ShardValidator {
    pub fn new(
        address: Address,
        shard: SnapchainShard,
        initial_validator_set: SnapchainValidatorSet,
        block_proposer: Option<BlockProposer>,
        shard_proposer: Option<ShardProposer>,
        local_state_store: LocalStateStore,
        statsd: StatsdClientWrapper,
    ) -> ShardValidator {
        ShardValidator {
            shard_id: shard.clone(),
            address: address.clone(),
            validator_set: initial_validator_set,
            current_height: None,
            current_round: Round::new(0),
            height_started_at: None,
            proposed_at: None,
            current_proposer: None,
            block_proposer,
            shard_proposer,
            started: false,
            local_state_store,
            statsd,
        }
    }

    pub fn get_validator_set(&self) -> SnapchainValidatorSet {
        self.validator_set.clone()
    }

    pub fn validator_count(&self) -> usize {
        self.validator_set.count()
    }

    pub fn get_address(&self) -> Address {
        self.address.clone()
    }

    pub fn get_current_height(&self) -> Height {
        if let Some(p) = &self.block_proposer {
            return p.get_confirmed_height();
        } else if let Some(p) = &self.shard_proposer {
            return p.get_confirmed_height();
        }
        panic!("No proposer set on validator");
    }

    pub fn get_min_height(&self) -> Height {
        if let Some(p) = &self.block_proposer {
            return p.get_min_height();
        } else if let Some(p) = &self.shard_proposer {
            return p.get_min_height();
        }
        panic!("No proposer set on validator");
    }

    pub fn add_validator(&mut self, validator: SnapchainValidator) -> bool {
        self.validator_set.add(validator)
    }

    pub fn start(&mut self) {
        self.started = true;
    }

    pub fn start_round(&mut self, height: Height, round: Round, proposer: Address) {
        if self.current_height.is_none() || self.current_height.unwrap() != height {
            self.height_started_at = Some(std::time::Instant::now());
        }
        self.current_height = Some(height);
        self.current_round = round;
        self.current_proposer = Some(proposer);
        self.proposed_at = None;
    }

    pub fn next_height_delay(&self, target_block_time: u64) -> Duration {
        if self.height_started_at.is_none() {
            return Duration::from_secs(0);
        }
        let last_height_start_time = self.height_started_at.unwrap();
        let current_time = std::time::Instant::now();
        let duration = current_time.duration_since(last_height_start_time);
        let target_duration = Duration::from_millis(target_block_time);
        if duration < target_duration {
            // If we're ahead of schedule, wait until the target duration
            target_duration - duration
        } else {
            // If we're behind schedule, don't wait to start next block
            Duration::from_secs(0)
        }
    }

    pub async fn decide(&mut self, commits: Commits) {
        if self.proposed_at.is_some() {
            let duration = self.proposed_at.unwrap().elapsed();
            // Time spent in achieving consensus (just after the app proposes to just before it commits)
            self.statsd.time_with_shard(
                self.shard_id.shard_id(),
                "consensus_time",
                duration.as_millis() as u64,
            );
        }
        let received_shard_id = commits.height.unwrap().shard_index;
        if self.shard_id.shard_id() != received_shard_id {
            warn!(
                "Received commits for shard {} on shard {}",
                received_shard_id,
                self.shard_id.shard_id()
            );
            panic!("Received commits for wrong shard");
        }
        if let Some(block_proposer) = &mut self.block_proposer {
            block_proposer.decide(commits.clone()).await;
        } else if let Some(shard_proposer) = &mut self.shard_proposer {
            shard_proposer.decide(commits.clone()).await;
        } else {
            panic!("No proposer set");
        }
        // Delete all proposals for this height, this node might have proposed for earlier rounds
        if let Err(err) = self
            .local_state_store
            .delete_proposals(self.shard_id.shard_id(), commits.height.unwrap())
        {
            error!("Error deleting proposal {}", err.to_string())
        }
        self.current_height = commits.height;
        self.current_round = Round::Nil;
        if let Some(height_started_at) = self.height_started_at {
            let duration = height_started_at.elapsed();
            self.statsd.time_with_shard(
                self.shard_id.shard_id(),
                "block_time",
                duration.as_millis() as u64,
            );
        }
        self.statsd.gauge_with_shard(
            self.shard_id.shard_id(),
            "round",
            commits.round.unsigned_abs(),
        );
    }

    pub async fn get_decided_value(
        &self,
        height: Height,
    ) -> Option<(Commits, full_proposal::ProposedValue)> {
        if let Some(block_proposer) = &self.block_proposer {
            return block_proposer.get_decided_value(height).await;
        } else if let Some(shard_proposer) = &self.shard_proposer {
            return shard_proposer.get_decided_value(height).await;
        }
        panic!("No proposer set");
    }

    pub fn add_proposed_value(
        &mut self,
        full_proposal: &FullProposal,
    ) -> ProposedValue<SnapchainValidatorContext> {
        let value = full_proposal.shard_hash();
        if self.shard_id.shard_id() != full_proposal.shard_id().unwrap() {
            warn!(
                "Received proposal for shard {} on shard {}",
                full_proposal.shard_id().unwrap(),
                self.shard_id.shard_id()
            );
            panic!("Received proposal for wrong shard");
        }
        let validity = if let Some(block_proposer) = &mut self.block_proposer {
            block_proposer.add_proposed_value(full_proposal)
        } else if let Some(shard_proposer) = &mut self.shard_proposer {
            shard_proposer.add_proposed_value(full_proposal)
        } else {
            panic!("No proposer set");
        };

        ProposedValue {
            height: full_proposal.height(),
            round: full_proposal.round(),
            valid_round: Round::Nil,
            proposer: full_proposal.proposer_address(),
            value,
            validity,
        }
    }

    pub async fn propose_value(
        &mut self,
        height: Height,
        round: Round,
        timeout: Duration,
    ) -> FullProposal {
        // TODO(aditi): As an optimization, we should only look inside the db on the first height after a restart. We do not need to look here in the steady state.
        match self
            .local_state_store
            .get_proposal(self.shard_id.shard_id(), height, round)
        {
            Ok(Some(proposal)) => return proposal,
            Ok(None) => {}
            Err(err) => {
                error!("Unable to retrieve proposal from db {}", err.to_string())
            }
        }
        let proposal = if let Some(block_proposer) = &mut self.block_proposer {
            block_proposer.propose_value(height, round, timeout).await
        } else if let Some(shard_proposer) = &mut self.shard_proposer {
            shard_proposer.propose_value(height, round, timeout).await
        } else {
            panic!("No proposer set");
        };

        if let Err(err) = self.local_state_store.put_proposal(proposal.clone()) {
            error!("Unable to store proposal {}", err.to_string());
        };

        self.proposed_at = Some(std::time::Instant::now());
        proposal
    }

    pub fn get_proposed_value(&mut self, shard_hash: &ShardHash) -> Option<FullProposal> {
        // TODO(aditi): In the future, we may want to look the value up in the db
        if let Some(block_proposer) = &mut self.block_proposer {
            block_proposer.get_proposed_value(shard_hash)
        } else if let Some(shard_proposer) = &mut self.shard_proposer {
            shard_proposer.get_proposed_value(shard_hash)
        } else {
            panic!("No proposer set");
        }
    }
}
