use crate::core::types::{
    proto, Address, Height, ShardHash, ShardId, SnapchainShard, FARCASTER_EPOCH,
};
use crate::proto::{
    full_proposal, Block, BlockHeader, Commits, FullProposal, ShardChunk, ShardChunkWitness,
    ShardHeader, ShardWitness,
};
use crate::storage::store::engine::{BlockEngine, ShardEngine, ShardStateChange};
use crate::storage::store::stores::Stores;
use crate::storage::store::BlockStorageError;
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use informalsystems_malachitebft_core_types::{Round, Validity};
use prost::Message;
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{broadcast, mpsc};
use tokio::time::Instant;
use tokio::{select, time};
use tracing::{error, warn};

const PROTOCOL_VERSION: u32 = 1;
pub const GENESIS_MESSAGE: &str =
    "It occurs to me that our survival may depend upon our talking to one another.";

pub fn current_time() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - (FARCASTER_EPOCH / 1000)
}

#[allow(async_fn_in_trait)] // TODO
pub trait Proposer {
    // Create a new block/shard chunk for the given height that will be proposed for confirmation to the other validators
    async fn propose_value(
        &mut self,
        height: Height,
        round: Round,
        timeout: Duration,
    ) -> FullProposal;
    // Receive a block/shard chunk proposed by another validator and return whether it is valid
    fn add_proposed_value(&mut self, full_proposal: &FullProposal) -> Validity;

    // Consensus has confirmed the block/shard_chunk, apply it to the local state
    async fn decide(&mut self, commits: Commits);

    async fn get_decided_value(
        &self,
        height: Height,
    ) -> Option<(Commits, full_proposal::ProposedValue)>;

    fn get_confirmed_height(&self) -> Height;

    fn get_min_height(&self) -> Height;

    fn get_proposed_value(&self, shard_hash: &ShardHash) -> Option<FullProposal>;
}

pub struct ProposedValues {
    values_by_height: BTreeMap<Height, Vec<ShardHash>>,
    values: BTreeMap<ShardHash, FullProposal>,
}

impl ProposedValues {
    pub fn new() -> Self {
        ProposedValues {
            values_by_height: BTreeMap::new(),
            values: BTreeMap::new(),
        }
    }

    pub fn add_proposed_value(&mut self, value: FullProposal) {
        let height = value.height();
        let shard_hash = value.shard_hash();
        self.values.insert(shard_hash.clone(), value);
        match self.values_by_height.get_mut(&height) {
            Some(hashes) => {
                hashes.push(shard_hash);
            }
            None => {
                self.values_by_height.insert(height, vec![shard_hash]);
            }
        }
    }

    pub fn get_by_shard_hash(&self, shard_hash: &ShardHash) -> Option<&FullProposal> {
        self.values.get(&shard_hash)
    }

    pub fn decide(&mut self, height: Height) {
        let mut heights_to_remove = vec![];
        for (entry_height, _) in &self.values_by_height {
            if *entry_height <= height {
                heights_to_remove.push(*entry_height);
            } else {
                break;
            }
        }

        for height in heights_to_remove {
            if let Some(shard_hashes) = self.values_by_height.remove(&height) {
                for shard_hash in shard_hashes {
                    self.values.remove(&shard_hash);
                }
            }
        }
    }

    pub fn count(&self) -> usize {
        self.values.len()
    }
}

pub struct ShardProposer {
    shard_id: SnapchainShard,
    address: Address,
    proposed_chunks: ProposedValues,
    tx_decision: broadcast::Sender<ShardChunk>,
    engine: ShardEngine,
    statsd_client: StatsdClientWrapper,
}

impl ShardProposer {
    pub fn new(
        address: Address,
        shard_id: SnapchainShard,
        engine: ShardEngine,
        statsd_client: StatsdClientWrapper,
        tx_decision: broadcast::Sender<ShardChunk>,
    ) -> ShardProposer {
        ShardProposer {
            shard_id,
            address,
            proposed_chunks: ProposedValues::new(),
            tx_decision,
            engine,
            statsd_client,
        }
    }

    async fn publish_new_shard_chunk(&self, shard_chunk: &ShardChunk) {
        let _ = &self.tx_decision.send(shard_chunk.clone());
    }

    pub fn start_round(&mut self, height: Height, round: Round) {
        self.engine.start_round(height, round);
    }
}

impl Proposer for ShardProposer {
    async fn propose_value(
        &mut self,
        height: Height,
        round: Round,
        _timeout: Duration,
    ) -> FullProposal {
        // TODO: perhaps not the best place to get our messages, but this is (currently) the
        // last place we're still in an async function
        let mempool_timeout = Duration::from_millis(200);
        let messages = self.engine.pull_messages(mempool_timeout).await.unwrap(); // TODO: don't unwrap

        let previous_chunk = self.engine.get_last_shard_chunk();
        let parent_hash = match previous_chunk {
            Some(chunk) => chunk.hash.clone(),
            None => vec![0, 32],
        };

        let state_change = self
            .engine
            .propose_state_change(self.shard_id.shard_id(), messages);
        let shard_header = ShardHeader {
            parent_hash,
            timestamp: current_time(),
            height: Some(height.clone()),
            shard_root: state_change.new_state_root.clone(),
        };
        let hash = blake3::hash(&shard_header.encode_to_vec())
            .as_bytes()
            .to_vec();

        let chunk = ShardChunk {
            header: Some(shard_header),
            hash: hash.clone(),
            transactions: state_change.transactions.clone(),
            commits: None,
        };

        let proposal = FullProposal {
            height: Some(height.clone()),
            round: round.as_i64(),
            proposed_value: Some(proto::full_proposal::ProposedValue::Shard(chunk)),
            proposer: self.address.to_vec(),
        };
        self.proposed_chunks.add_proposed_value(proposal.clone());
        proposal
    }

    fn add_proposed_value(&mut self, full_proposal: &FullProposal) -> Validity {
        if let Some(proto::full_proposal::ProposedValue::Shard(chunk)) =
            full_proposal.proposed_value.clone()
        {
            let header = chunk.header.as_ref().unwrap();
            let height = header.height.unwrap();
            self.proposed_chunks
                .add_proposed_value(full_proposal.clone());
            let receive_delay = current_time().saturating_sub(header.timestamp);
            self.statsd_client.gauge_with_shard(
                self.shard_id.shard_id(),
                "proposer.receive_delay",
                receive_delay,
            );

            let confirmed_height = self.get_confirmed_height();
            if height != confirmed_height.increment() {
                warn!(
                    shard = height.shard_index,
                    our_height = confirmed_height.block_number,
                    proposal_height = height.block_number,
                    "Cannot validate height, not the next height"
                );
                return Validity::Invalid;
            }

            let state = ShardStateChange {
                shard_id: height.shard_index,
                new_state_root: header.shard_root.clone(),
                transactions: chunk.transactions.clone(),
                events: vec![],
            };
            return if self.engine.validate_state_change(&state) {
                Validity::Valid
            } else {
                error!(
                    shard = state.shard_id,
                    height = height.block_number,
                    "Invalid state change"
                );
                Validity::Invalid
            };
        }
        error!("Invalid proposed value: {:?}", full_proposal.proposed_value);
        Validity::Invalid // TODO: Validate proposer signature?
    }

    fn get_proposed_value(&self, shard_hash: &ShardHash) -> Option<FullProposal> {
        self.proposed_chunks.get_by_shard_hash(&shard_hash).cloned()
    }

    async fn decide(&mut self, commits: Commits) {
        let value = commits.value.clone().unwrap();
        let height = commits.height.unwrap();
        if let Some(proposal) = self.proposed_chunks.get_by_shard_hash(&value) {
            let chunk = proposal.shard_chunk(commits).unwrap();
            self.publish_new_shard_chunk(&chunk.clone()).await;
            self.engine.commit_shard_chunk(&chunk);
            self.proposed_chunks.decide(height);
        } else {
            panic!(
                "Unable to find proposal for decided value. height {}, round {}, shard_hash {}",
                height.to_string(),
                commits.round,
                hex::encode(value.hash),
            )
        }
        self.statsd_client.gauge_with_shard(
            self.shard_id.shard_id(),
            "proposer.pending_blocks",
            self.proposed_chunks.count() as u64,
        );
    }

    async fn get_decided_value(
        &self,
        height: Height,
    ) -> Option<(Commits, full_proposal::ProposedValue)> {
        let shard_chunk = self.engine.get_shard_chunk_by_height(height);
        match shard_chunk {
            Some(chunk) => {
                let commits = chunk.commits.clone().unwrap();
                Some((commits, full_proposal::ProposedValue::Shard(chunk)))
            }
            _ => None,
        }
    }

    fn get_confirmed_height(&self) -> Height {
        self.engine.get_confirmed_height()
    }

    fn get_min_height(&self) -> Height {
        // Always return the genesis block, until we implement pruning
        Height::new(self.shard_id.shard_id(), 1)
    }
}

#[derive(Error, Debug)]
pub enum BlockProposerError {
    #[error("Block missing header")]
    BlockMissingHeader,

    #[error("Block missing height")]
    BlockMissingHeight,

    #[error("No peers")]
    NoPeers,

    #[error(transparent)]
    RpcTransportError(#[from] tonic::transport::Error),

    #[error(transparent)]
    RpcResponseError(#[from] tonic::Status),

    #[error(transparent)]
    BlockStorageError(#[from] BlockStorageError),
}

pub struct BlockProposer {
    #[allow(dead_code)] // TODO
    shard_id: SnapchainShard,
    address: Address,
    proposed_blocks: ProposedValues,
    shard_stores: HashMap<u32, Stores>,
    num_shards: u32,
    network: proto::FarcasterNetwork,
    block_tx: Option<mpsc::Sender<Block>>,
    engine: BlockEngine,
    statsd_client: StatsdClientWrapper,
}

impl BlockProposer {
    pub fn new(
        address: Address,
        shard_id: SnapchainShard,
        shard_stores: HashMap<u32, Stores>,
        num_shards: u32,
        network: proto::FarcasterNetwork,
        block_tx: Option<mpsc::Sender<Block>>,
        engine: BlockEngine,
        statsd_client: StatsdClientWrapper,
    ) -> BlockProposer {
        BlockProposer {
            shard_id,
            address,
            proposed_blocks: ProposedValues::new(),
            shard_stores,
            num_shards,
            network,
            block_tx,
            engine,
            statsd_client,
        }
    }

    async fn collect_confirmed_shard_witnesses(
        &mut self,
        height: Height,
        timeout: Duration,
    ) -> Vec<ShardChunkWitness> {
        let requested_height = height.block_number;

        let mut poll_interval = time::interval(Duration::from_millis(100));
        let mut chunks = BTreeMap::new();

        // convert to deadline
        let deadline = Instant::now() + timeout;
        loop {
            let timeout = time::sleep_until(deadline);
            select! {
                _ = poll_interval.tick() => {
                    for (shard_id, store) in self.shard_stores.iter() {
                        if chunks.contains_key(shard_id) {
                            continue;
                        }
                        let result = store.shard_store.get_chunk_by_height(requested_height);
                        match result {
                            Ok(Some(chunk)) => {
                                let header = chunk.header.as_ref().unwrap();
                                let shard_witness = ShardChunkWitness {
                                    height: header.height,
                                    shard_hash: chunk.hash.clone(),
                                    shard_root: chunk.header.unwrap().shard_root,
                                };
                                chunks.insert(*shard_id, shard_witness);
                            }
                            Ok(None) => {}
                            Err(err) => {
                                error!(height=height.block_number, shard_id=shard_id, "Error getting confirmed shard chunk: {:?}", err);
                            }
                        }
                    }
                    if chunks.len() == self.num_shards as usize {
                        break;
                    }
                }
                _ = timeout => {
                    break;
                }
            }
        }

        if chunks.values().len() == self.num_shards as usize {
            chunks.values().cloned().collect()
        } else {
            warn!(
                "Block validator did not receive all shard chunks for height: {:?}",
                requested_height
            );
            vec![]
        }
    }

    async fn publish_new_block(&self, block: Block) {
        if let Some(block_tx) = &self.block_tx {
            match block_tx.send(block.clone()).await {
                Err(err) => {
                    error!("Error publishing new block {:?}", err.to_string());
                }
                Ok(_) => {}
            }
        }
    }
}

impl Proposer for BlockProposer {
    async fn propose_value(
        &mut self,
        height: Height,
        round: Round,
        timeout: Duration,
    ) -> FullProposal {
        let shard_witnesses = self
            .collect_confirmed_shard_witnesses(height, timeout)
            .await;
        let shard_witness = ShardWitness {
            shard_chunk_witnesses: shard_witnesses,
        };
        let previous_block = self.engine.get_last_block();
        let genesis_height = Height::new(self.shard_id.shard_id(), 1);
        let parent_hash = if height == genesis_height {
            if previous_block.is_some() {
                error!(
                    height = height.as_u64(),
                    round = round.as_i64(),
                    parent_hash = hex::encode(previous_block.unwrap().hash.clone()),
                    "Block unexpectedly has parent. Replacing parent hash with genesis message."
                );
            }
            GENESIS_MESSAGE.as_bytes().to_vec()
        } else {
            match previous_block {
                Some(block) => block.hash.clone(),
                None => {
                    // This should only be the case for the genesis block
                    error!(
                        height = height.as_u64(),
                        round = round.as_i64(),
                        "Block unexpectedly missing parent"
                    );
                    vec![0; 32]
                }
            }
        };
        let witness_hash = blake3::hash(&shard_witness.encode_to_vec())
            .as_bytes()
            .to_vec();
        let block_header = BlockHeader {
            parent_hash,
            chain_id: self.network as i32,
            version: PROTOCOL_VERSION,
            timestamp: current_time(),
            height: Some(height.clone()),
            shard_witnesses_hash: witness_hash,
        };
        let hash = blake3::hash(&block_header.encode_to_vec())
            .as_bytes()
            .to_vec();

        let block = Block {
            header: Some(block_header),
            hash: hash.clone(),
            shard_witness: Some(shard_witness),
            commits: None,
        };

        let proposal = FullProposal {
            height: Some(height.clone()),
            round: round.as_i64(),
            proposed_value: Some(proto::full_proposal::ProposedValue::Block(block)),
            proposer: self.address.to_vec(),
        };

        self.proposed_blocks.add_proposed_value(proposal.clone());
        proposal
    }

    fn add_proposed_value(&mut self, full_proposal: &FullProposal) -> Validity {
        if let Some(proto::full_proposal::ProposedValue::Block(block)) =
            &full_proposal.proposed_value
        {
            let header = block.header.as_ref().unwrap();
            let height = header.height.unwrap();

            if height != self.get_confirmed_height().increment() {
                warn!(
                    shard = height.shard_index,
                    our_height = height.block_number,
                    proposal_height = height.block_number,
                    "Cannot validate height, not the next height"
                );
                return Validity::Invalid;
            }

            if header.chain_id != (self.network as i32) {
                error!("Received block with wrong chain_id: {}", header.chain_id);
                return Validity::Invalid;
            }
            if header.version != PROTOCOL_VERSION {
                error!(
                    "Received block with wrong protocol version: {}",
                    header.version
                );
                return Validity::Invalid;
            }
            if header.height.is_none() {
                error!("Received block with missing height");
                return Validity::Invalid;
            }
            if header.shard_witnesses_hash.is_empty() {
                error!("Received block with missing shard witnesses hash");
                return Validity::Invalid;
            }
            if block.shard_witness.is_none() {
                error!("Received block with missing shard witnesses");
                return Validity::Invalid;
            }
            let witness = block.shard_witness.as_ref().unwrap();
            if witness.shard_chunk_witnesses.len() != self.num_shards as usize {
                error!(
                    "Received block with wrong number of shard witnesses: {}",
                    witness.shard_chunk_witnesses.len()
                );
                return Validity::Invalid;
            }
            let witness_hash = blake3::hash(&witness.encode_to_vec()).as_bytes().to_vec();
            if witness_hash != header.shard_witnesses_hash {
                error!("Received block with invalid shard witnesses hash");
                return Validity::Invalid;
            }
            self.proposed_blocks
                .add_proposed_value(full_proposal.clone());
        }
        Validity::Valid // TODO: Validate proposer signature?
    }

    fn get_proposed_value(&self, shard_hash: &ShardHash) -> Option<FullProposal> {
        self.proposed_blocks.get_by_shard_hash(&shard_hash).cloned()
    }

    async fn decide(&mut self, commits: Commits) {
        let value = commits.value.clone().unwrap();
        let height = commits.height.unwrap();
        if let Some(proposal) = self.proposed_blocks.get_by_shard_hash(&value) {
            let block = proposal.block(commits).unwrap();
            self.publish_new_block(block.clone()).await;
            self.engine.commit_block(&block);
            self.proposed_blocks.decide(height);
        } else {
            panic!(
                "Unable to find proposal for decided value. height {}, round {}, shard_hash {}",
                height.to_string(),
                commits.round,
                hex::encode(value.hash),
            )
        }

        // TODO: We might need to prune proposed blocks (and similarly in shard proposer)
        self.statsd_client.gauge_with_shard(
            self.shard_id.shard_id(),
            "proposer.pending_blocks",
            self.proposed_blocks.count() as u64,
        );
    }

    async fn get_decided_value(
        &self,
        height: Height,
    ) -> Option<(Commits, full_proposal::ProposedValue)> {
        let maybe_block = self.engine.get_block_by_height(height);
        match maybe_block {
            Some(block) => {
                let commits = block.commits.clone().unwrap();
                Some((commits, full_proposal::ProposedValue::Block(block)))
            }
            _ => None,
        }
    }

    fn get_confirmed_height(&self) -> Height {
        self.engine.get_confirmed_height()
    }

    fn get_min_height(&self) -> Height {
        // Always return the genesis block, until we implement pruning
        Height::new(self.shard_id.shard_id(), 1)
    }
}
