use crate::proto;
use crate::proto::{consensus_message, ConsensusMessage, MessageType};

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

    pub fn msg_type(&self) -> MessageType {
        if self.data.is_some() {
            MessageType::try_from(self.data.as_ref().unwrap().r#type).unwrap_or(MessageType::None)
        } else {
            MessageType::None
        }
    }

    pub fn hex_hash(&self) -> String {
        hex::encode(&self.hash)
    }
}

impl proto::ValidatorMessage {
    pub fn fid(&self) -> u64 {
        if let Some(fname) = &self.fname_transfer {
            if let Some(proof) = &fname.proof {
                return proof.fid;
            }
        }
        if let Some(event) = &self.on_chain_event {
            return event.fid;
        }
        0
    }
}

impl proto::FullProposal {
    pub fn shard_id(&self) -> Result<u32, String> {
        if let Some(height) = &self.height {
            Ok(height.shard_index)
        } else {
            Err("No height in FullProposal".to_string())
        }
    }
}

impl ConsensusMessage {
    pub fn shard_id(&self) -> Result<u32, String> {
        if let Some(msg) = &self.consensus_message {
            match msg {
                consensus_message::ConsensusMessage::Vote(vote) => {
                    if let Some(height) = &vote.height {
                        return Ok(height.shard_index);
                    }
                }
                consensus_message::ConsensusMessage::Proposal(vote) => {
                    if let Some(height) = &vote.height {
                        return Ok(height.shard_index);
                    }
                }
            }
        }
        Err("Could not determine shard id for ConsensusMessage".to_string())
    }
}
