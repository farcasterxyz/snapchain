use crate::consensus::proposer::{BlockProposer, Proposer, ShardProposer};
use crate::core::types::{
    Address, Height, ShardId, SnapchainShard, SnapchainValidator, SnapchainValidatorContext,
    SnapchainValidatorSet,
};
use crate::proto::{full_proposal, Commits, FullProposal, ShardHash};
use crate::storage::store::node_local_state::LocalStateStore;
use informalsystems_malachitebft_core_consensus::ProposedValue;
use informalsystems_malachitebft_core_types::{Round, ValidatorSet};
use std::collections::HashSet;
use std::time::Duration;
use tracing::{error, warn};

pub struct ShardValidator {
    pub(crate) shard_id: SnapchainShard,

    #[allow(dead_code)] // TODO
    address: Address,

    validator_set: SnapchainValidatorSet,
    confirmed_height: Option<Height>,
    current_round: Round,
    current_height: Option<Height>,
    current_proposer: Option<Address>,
    // This should be proposer: Box<dyn Proposer> but that doesn't implement Send which is required for the actor system.
    // TODO: Fix once we remove the actor system
    block_proposer: Option<BlockProposer>,
    shard_proposer: Option<ShardProposer>,
    pub started: bool,
    pub saw_proposal_from_validator: HashSet<Address>,
    local_state_store: LocalStateStore,
}

impl ShardValidator {
    pub fn new(
        address: Address,
        shard: SnapchainShard,
        initial_validator_set: SnapchainValidatorSet,
        block_proposer: Option<BlockProposer>,
        shard_proposer: Option<ShardProposer>,
        local_state_store: LocalStateStore,
    ) -> ShardValidator {
        ShardValidator {
            shard_id: shard.clone(),
            address: address.clone(),
            validator_set: initial_validator_set,
            confirmed_height: None,
            current_round: Round::new(0),
            current_height: None,
            current_proposer: None,
            block_proposer,
            shard_proposer,
            started: false,
            saw_proposal_from_validator: HashSet::new(),
            local_state_store,
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

    pub fn saw_proposal_from_validator(&self, address: Address) -> bool {
        self.saw_proposal_from_validator.contains(&address)
    }

    pub async fn sync_against_validator(&mut self, validator: &SnapchainValidator) {
        if let Some(p) = &mut self.block_proposer {
            match p.sync_against_validator(&validator).await {
                Ok(()) => {}
                Err(err) => error!("Error registering validator {:#?}", err),
            };
        } else if let Some(p) = &mut self.shard_proposer {
            match p.sync_against_validator(&validator).await {
                Ok(()) => {}
                Err(err) => error!("Error registering validator {:#?}", err),
            }
        }
    }

    pub fn start_round(&mut self, height: Height, round: Round, proposer: Address) {
        self.current_height = Some(height);
        self.current_round = round;
        self.current_proposer = Some(proposer);
    }

    pub async fn decide(&mut self, commits: Commits) {
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
        self.confirmed_height = commits.height;
        self.current_round = Round::Nil;
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

        self.saw_proposal_from_validator
            .insert(full_proposal.proposer_address());
        ProposedValue {
            height: full_proposal.height(),
            round: full_proposal.round(),
            valid_round: Round::Nil,
            proposer: full_proposal.proposer_address(),
            value,
            validity,
            extension: None,
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
