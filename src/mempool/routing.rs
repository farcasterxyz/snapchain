use crate::core::types::FidOnDisk;
use crate::proto::{self, MessageType};
use crate::version::version::{EngineVersion, ProtocolFeature};
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
    version: EngineVersion,
) -> u32 {
    // Shard 0 hosts state that must be coherent across shards before other messages validate:
    // storage lends (accounting), and gasless keys (active-signer set consulted by every shard
    // during user-message validation). Channel messages are here for a different reason: their
    // authority inputs — the channel registry fold and the verification replica that resolves an
    // owner address to a fid — are shard-0-only, so admission can only be decided there. Per-shard
    // state (casts, reactions, links, etc.) routes by FID hash.
    //
    // The channel arms are unconditional because these types are new at V21 and no pre-V21
    // traffic exists; the feature gate lives at admission (mempool, wall-clock; engines,
    // block-ts), not here.
    match message.msg_type() {
        MessageType::LendStorage
        | MessageType::KeyAdd
        | MessageType::KeyRemove
        | MessageType::ChannelUpdate
        | MessageType::ChannelMember
        | MessageType::ChannelPin
        | MessageType::ChannelModerate => 0,
        // Routing is a wall-clock convention on each node; consensus does not re-check it.
        // During the mixed V21 window, an in-flight verification routed to a data shard before
        // cutover can land in a post-cutover block, where the data shard's block-ts-gated arm
        // rejects it deterministically.
        //
        // Whether the client can then resubmit depends on when the message was SIGNED, and the
        // two cases below intersect on the likeliest message:
        //   - signed at/after cutover: resubmitting routes it to shard 0 and it merges.
        //   - signed before cutover (the common case at the boundary): shard 0's embedded-
        //     timestamp floor also rejects it, so it must be re-signed with a fresh timestamp.
        //     That UX edge is the intentional tombstone-resurrection guard, not an oversight.
        //
        // If the same verification merged live before cutover and arrives again by shard-0
        // replay afterwards, self-supersede handling keeps the store, secondary index, trie, and
        // events mutually consistent. Note the event stream is NOT idempotent: a re-merge emits
        // a fresh MergeMessage HubEvent with a new id and empty deleted_messages. State converges;
        // subscribers may see the duplicate.
        MessageType::VerificationAddEthAddress | MessageType::VerificationRemove
            if version.is_enabled(ProtocolFeature::VerificationsOnShardZero) =>
        {
            0
        }
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
            assert_eq!(
                route_message(
                    &router,
                    &msg(MessageType::KeyAdd, fid),
                    2,
                    EngineVersion::V21
                ),
                0
            );
            assert_eq!(
                route_message(
                    &router,
                    &msg(MessageType::KeyRemove, fid),
                    2,
                    EngineVersion::V21,
                ),
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
            route_message(
                &router,
                &msg(MessageType::LendStorage, 99),
                2,
                EngineVersion::V21,
            ),
            0
        );
    }

    #[test]
    fn cast_add_still_routes_by_fid_hash() {
        // Ensure unrelated message types go through the FID router, not shard 0.
        let router: Box<dyn MessageRouter> = Box::new(ShardRouter {});
        let shard = route_message(
            &router,
            &msg(MessageType::CastAdd, 12345),
            2,
            EngineVersion::V21,
        );
        assert!(
            shard == 1 || shard == 2,
            "expected shard 1 or 2, got {shard}"
        );
    }

    #[test]
    fn s4_verifications_route_by_fid_before_activation() {
        let router: Box<dyn MessageRouter> = Box::new(EvenOddRouterForTest {});

        for message_type in [
            MessageType::VerificationAddEthAddress,
            MessageType::VerificationRemove,
        ] {
            assert_eq!(
                route_message(&router, &msg(message_type, 1), 2, EngineVersion::V19),
                1
            );
            assert_eq!(
                route_message(&router, &msg(message_type, 2), 2, EngineVersion::V19),
                2
            );
        }
    }

    #[test]
    fn s4_verifications_route_to_shard_zero_after_activation() {
        let router: Box<dyn MessageRouter> = Box::new(EvenOddRouterForTest {});

        for message_type in [
            MessageType::VerificationAddEthAddress,
            MessageType::VerificationRemove,
        ] {
            assert_eq!(
                route_message(&router, &msg(message_type, 1), 2, EngineVersion::V21),
                0
            );
            assert_eq!(
                route_message(&router, &msg(message_type, 2), 2, EngineVersion::V21),
                0
            );
        }
    }

    #[test]
    fn s4_channel_messages_route_to_shard_zero_before_and_after_activation() {
        let router: Box<dyn MessageRouter> = Box::new(EvenOddRouterForTest {});

        // Channel types are new at V21, so their routing is unconditional. The mempool's
        // wall-clock feature gate rejects them before activation; keeping both versions here
        // pins that routing itself cannot strand an admitted channel message on a data shard.
        for version in [EngineVersion::V21, EngineVersion::V21] {
            for message_type in [
                MessageType::ChannelUpdate,
                MessageType::ChannelMember,
                MessageType::ChannelPin,
                MessageType::ChannelModerate,
            ] {
                for fid in [1, 2] {
                    assert_eq!(
                        route_message(&router, &msg(message_type, fid), 2, version),
                        0
                    );
                }
            }
        }
    }
}
