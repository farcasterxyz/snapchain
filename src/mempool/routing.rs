use crate::core::types::FidOnDisk;
use crate::proto::{self, MessageType};
use sha2::{Digest, Sha256};

pub trait MessageRouter: Send + Sync {
    fn route_fid(&self, fid: u64, num_shards: u32) -> u32;
}

pub struct ShardRouter {}

impl MessageRouter for ShardRouter {
    fn route_fid(&self, fid: u64, num_shards: u32) -> u32 {
        // DO NOT CHANGE THE HASHING FUNCTION
        // This is being used to determine the merkle trie key for messages. Changing this will
        // break merkle trie hashes
        let hash = Sha256::digest((fid as FidOnDisk).to_be_bytes());
        let hash_u32 = u32::from_be_bytes(hash[..4].try_into().unwrap());
        (hash_u32 % num_shards) + 1
    }
}

// Meant only for tests
pub struct EvenOddRouterForTest {}
impl MessageRouter for EvenOddRouterForTest {
    fn route_fid(&self, fid: u64, num_shards: u32) -> u32 {
        if num_shards > 2 {
            panic!("EvenOddRouterForTest only supports 2 shards");
        }
        // Event fids go to the even shard (2), and odd fids go to the odd shard (1)
        if fid % 2 == 0 {
            2
        } else {
            1
        }
    }
}

pub fn route_message(
    router: &Box<dyn MessageRouter>,
    message: &proto::Message,
    num_shards: u32,
) -> u32 {
    // Shard 0 hosts state that must be coherent across shards before other messages validate:
    // storage lends (accounting), and gasless keys (active-signer set consulted by every shard
    // during user-message validation). Per-shard state (casts, reactions, links, etc.) routes
    // by FID hash.
    match message.msg_type() {
        MessageType::LendStorage | MessageType::KeyAdd | MessageType::KeyRemove => 0,
        _ => router.route_fid(message.fid(), num_shards),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{MessageData, MessageType};

    fn msg(msg_type: MessageType, fid: u64) -> proto::Message {
        // The minimal shape `route_message` inspects: msg_type + fid via the `data` accessor.
        // Values outside those two fields are irrelevant to routing.
        proto::Message {
            data: Some(MessageData {
                r#type: msg_type as i32,
                fid,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn key_add_and_key_remove_route_to_shard_zero_regardless_of_fid() {
        let router: Box<dyn MessageRouter> = Box::new(ShardRouter {});

        // FID 1 is arbitrary — the point is that KEY_ADD/KEY_REMOVE bypass the FID hash
        // entirely. Try a few FIDs to confirm.
        for fid in [1u64, 42, 1_000_000, u64::MAX] {
            assert_eq!(route_message(&router, &msg(MessageType::KeyAdd, fid), 2), 0);
            assert_eq!(
                route_message(&router, &msg(MessageType::KeyRemove, fid), 2),
                0
            );
        }
    }

    #[test]
    fn lend_storage_still_routes_to_shard_zero() {
        // Regression guard — the existing LendStorage rule shouldn't have shifted when the
        // match arm changed from an if-expression to a match.
        let router: Box<dyn MessageRouter> = Box::new(ShardRouter {});
        assert_eq!(
            route_message(&router, &msg(MessageType::LendStorage, 99), 2),
            0
        );
    }

    #[test]
    fn cast_add_still_routes_by_fid_hash() {
        // Ensure unrelated message types go through the FID router, not shard 0.
        let router: Box<dyn MessageRouter> = Box::new(ShardRouter {});
        let shard = route_message(&router, &msg(MessageType::CastAdd, 12345), 2);
        assert!(
            shard == 1 || shard == 2,
            "expected shard 1 or 2, got {shard}"
        );
    }
}
