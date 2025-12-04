use core::fmt;

pub mod proto {
    tonic::include_proto!("_");
}

pub use proto::*;

// Basic impls for proto types that don't depend on main crate

impl informalsystems_malachitebft_core_types::Height for proto::Height {
    const ZERO: Self = Self::new(0, 0);
    const INITIAL: Self = Self::new(0, 1);

    fn increment(&self) -> Self {
        self.increment()
    }

    fn as_u64(&self) -> u64 {
        self.block_number
    }

    fn increment_by(&self, n: u64) -> Self {
        self.increment_by(n)
    }

    fn decrement_by(&self, n: u64) -> Option<Self> {
        self.decrement_by(n)
    }
}

impl informalsystems_malachitebft_core_types::Value for proto::ShardHash {
    type Id = proto::ShardHash;

    fn id(&self) -> Self::Id {
        self.clone()
    }
}

impl proto::Height {
    pub const fn new(shard_index: u32, block_number: u64) -> Self {
        Self {
            shard_index,
            block_number,
        }
    }

    pub const fn as_u64(&self) -> u64 {
        self.block_number
    }

    pub const fn increment(&self) -> Self {
        self.increment_by(1)
    }

    pub const fn increment_by(&self, n: u64) -> Self {
        Self {
            shard_index: self.shard_index,
            block_number: self.block_number + n,
        }
    }

    pub fn decrement(&self) -> Option<Self> {
        self.block_number.checked_sub(1).map(|block_number| Self {
            shard_index: self.shard_index,
            block_number,
        })
    }

    pub fn decrement_by(&self, n: u64) -> Option<Self> {
        self.block_number.checked_sub(n).map(|block_number| Self {
            shard_index: self.shard_index,
            block_number,
        })
    }
}

impl fmt::Display for proto::Height {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.shard_index, self.block_number)
    }
}

impl fmt::Display for proto::ShardHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {:?}", self.shard_index, hex::encode(&self.hash))
    }
}

impl proto::BlockEvent {
    pub fn seqnum(&self) -> u64 {
        self.data.as_ref().unwrap().seqnum
    }

    pub fn block_number(&self) -> u64 {
        self.data.as_ref().unwrap().block_number
    }

    pub fn block_timestamp(&self) -> u64 {
        self.data.as_ref().unwrap().block_timestamp
    }

    pub fn event_index(&self) -> u64 {
        self.data.as_ref().unwrap().event_index
    }
}

impl proto::Message {
    pub fn is_type(&self, message_type: proto::MessageType) -> bool {
        self.data.is_some() && self.data.as_ref().unwrap().r#type == message_type as i32
    }

    pub fn fid(&self) -> u64 {
        if self.data.is_some() {
            self.data.as_ref().unwrap().fid
        } else {
            0
        }
    }

    pub fn msg_type(&self) -> proto::MessageType {
        if self.data.is_some() {
            proto::MessageType::try_from(self.data.as_ref().unwrap().r#type)
                .unwrap_or(proto::MessageType::None)
        } else {
            proto::MessageType::None
        }
    }

    pub fn hex_hash(&self) -> String {
        hex::encode(&self.hash)
    }
}

// Make malachite happy. Prost already implements PartialEq, should be safe to mark as Eq.
impl Eq for proto::FullProposal {}

impl proto::FullProposal {
    pub fn shard_id(&self) -> Result<u32, String> {
        if let Some(height) = &self.height {
            Ok(height.shard_index)
        } else {
            Err("No height in FullProposal".to_string())
        }
    }

    pub fn shard_hash(&self) -> proto::ShardHash {
        match &self.proposed_value {
            Some(proto::full_proposal::ProposedValue::Block(block)) => proto::ShardHash {
                shard_index: self.height().shard_index as u32,
                hash: block.hash.clone(),
            },
            Some(proto::full_proposal::ProposedValue::Shard(shard_chunk)) => proto::ShardHash {
                shard_index: self.height().shard_index as u32,
                hash: shard_chunk.hash.clone(),
            },
            _ => {
                panic!("Invalid proposal type");
            }
        }
    }

    pub fn block(&self, commits: proto::Commits) -> Option<proto::Block> {
        match &self.proposed_value {
            Some(proto::full_proposal::ProposedValue::Block(block)) => {
                let mut block = block.clone();
                block.commits = Some(commits);
                Some(block)
            }
            _ => None,
        }
    }

    pub fn shard_chunk(&self, commits: proto::Commits) -> Option<proto::ShardChunk> {
        match &self.proposed_value {
            Some(proto::full_proposal::ProposedValue::Shard(chunk)) => {
                let mut chunk = chunk.clone();
                chunk.commits = Some(commits);
                Some(chunk)
            }
            _ => None,
        }
    }

    pub fn height(&self) -> proto::Height {
        self.height.clone().unwrap()
    }

    pub fn round(&self) -> informalsystems_malachitebft_core_types::Round {
        informalsystems_malachitebft_core_types::Round::new(self.round.try_into().unwrap())
    }

    pub fn to_sign_bytes(&self) -> Vec<u8> {
        use prost::Message;
        self.encode_to_vec()
    }
}

impl proto::ConsensusMessage {
    pub fn shard_id(&self) -> Result<u32, String> {
        if let Some(msg) = &self.consensus_message {
            match msg {
                proto::consensus_message::ConsensusMessage::Vote(vote) => {
                    if let Some(height) = &vote.height {
                        return Ok(height.shard_index);
                    }
                }
                proto::consensus_message::ConsensusMessage::Proposal(vote) => {
                    if let Some(height) = &vote.height {
                        return Ok(height.shard_index);
                    }
                }
            }
        }
        Err("Could not determine shard id for ConsensusMessage".to_string())
    }
}
