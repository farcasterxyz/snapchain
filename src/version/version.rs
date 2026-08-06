use crate::core::util::FarcasterTime;
use crate::proto::FarcasterNetwork;
use strum::IntoEnumIterator;
use strum_macros::EnumIter;

const LATEST_PROTOCOL_VERSION: u32 = 13;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, EnumIter)]
pub enum EngineVersion {
    V0 = 0,
    V1 = 1,
    V2 = 2,
    V3 = 3,
    V4 = 4,
    V5 = 5,
    V6 = 6,
    V7 = 7,
    V8 = 8,
    V9 = 9,
    V10 = 10,
    V11 = 11,
    V12 = 12,
    V13 = 13,
    V14 = 14,
    V15 = 15,
    V16 = 16,
    V17 = 17,
    V18 = 18,
    V19 = 19,
    V20 = 20,
    V21 = 21,
}

pub enum ProtocolFeature {
    SignerRevokeBug,
    FarcasterPro,
    Basenames,
    EnsValidation, // Before this version, ENS validation was not enforced
    MessageLengthCheckFix,
    PrimaryAddresses,
    UsernameShardRoutingFix,
    FutureTimestampValidation,
    DependentMessagesInBulkSubmit,
    DecoupleShardZeroBlockProduction,
    WriteDataToShardZero,
    ReadDataFromShardZero,
    UserProfileToken,
    StorageLending,
    EventIdBugFix,
    StorageLendingLimitFix,
    StopRevokingExistingMessages,
    IncreaseUsernameProofSizeLimit,
    GaslessSigners,
    LiveAt,
    StorageExpiryExtension2026,
    BlockLinks,
    ChannelRegistrations,
    SortedBlockEngineEvents,
    ChannelOwnershipEvents,
    ChannelMessages,
    VerificationsOnShardZero,
    ChannelFollows,
    IncreaseEmbedLimitForAllUsers,
}

pub struct VersionSchedule {
    pub active_at: u64, // Unix timestamp in seconds
    pub version: EngineVersion,
}

const ENGINE_VERSION_SCHEDULE_MAINNET: &[VersionSchedule] = [
    VersionSchedule {
        active_at: 0,
        version: EngineVersion::V0,
    },
    VersionSchedule {
        active_at: 1747333800, // Signer revoke bug deployed
        version: EngineVersion::V1,
    },
    VersionSchedule {
        active_at: 1747352400, // Signer revoke bug reverted
        version: EngineVersion::V2,
    },
    VersionSchedule {
        active_at: 1747356000, // Signer revoke bug redeployed
        version: EngineVersion::V3,
    },
    VersionSchedule {
        active_at: 1747417200, // Signer revoke bug fixed
        version: EngineVersion::V4,
    },
    VersionSchedule {
        active_at: 1750093200, // 2025-06-16 5PM UTC
        version: EngineVersion::V5,
    },
    VersionSchedule {
        active_at: 1752685200, // 2025-07-16 5PM UTC
        version: EngineVersion::V6,
    },
    VersionSchedule {
        active_at: 1756141200, // 2025-08-25 5PM UTC
        version: EngineVersion::V7,
    },
    VersionSchedule {
        active_at: 1756918800, // 2025-09-03 5PM UTC
        version: EngineVersion::V8,
    },
    VersionSchedule {
        active_at: 1757523600, // 2025-09-10 5PM UTC
        version: EngineVersion::V9,
    },
    VersionSchedule {
        active_at: 1758733200, // 2025-09-24 5PM UTC
        version: EngineVersion::V10,
    },
    VersionSchedule {
        active_at: 1759942800, // 2025-10-08 5PM UTC
        version: EngineVersion::V11,
    },
    VersionSchedule {
        active_at: 1759942800, // 2025-10-08 5PM UTC, fixes testnet issue
        version: EngineVersion::V12,
    },
    VersionSchedule {
        active_at: 1759942800, // 2025-10-08 5PM UTC, fixes testnet issue
        version: EngineVersion::V13,
    },
    VersionSchedule {
        active_at: 1761757200, // 2025-10-29 5PM UTC
        version: EngineVersion::V14,
    },
    VersionSchedule {
        active_at: 1765386000, // 2025-12-10 5PM UTC
        version: EngineVersion::V15,
    },
    VersionSchedule {
        active_at: 1778173200, // 2026-05-07 5PM UTC (12:00 PM CDT)
        version: EngineVersion::V16,
    },
    VersionSchedule {
        active_at: 1780592400, // 2026-06-04 5PM UTC (12:00 PM CDT)
        version: EngineVersion::V17,
    },
    VersionSchedule {
        active_at: 1782147600, // 2026-06-22 5PM UTC (12:00 PM CDT)
        version: EngineVersion::V18,
    },
    VersionSchedule {
        active_at: 1785160800, // 2026-07-27 2PM UTC (9:00 AM CDT)
        version: EngineVersion::V19,
    },
    VersionSchedule {
        active_at: 1786640400, // 2026-08-13 5PM UTC (12:00 PM CDT)
        version: EngineVersion::V20,
    },
]
.as_slice();

const ENGINE_VERSION_SCHEDULE_TESTNET: &[VersionSchedule] = [
    VersionSchedule {
        active_at: 0,
        version: EngineVersion::V0,
    },
    VersionSchedule {
        active_at: 1748970000, // 2025-06-3 5PM UTC
        version: EngineVersion::V5,
    },
    VersionSchedule {
        active_at: 1752099060, // 2025-07-09 ~10PM UTC
        version: EngineVersion::V6,
    },
    VersionSchedule {
        active_at: 1755291600, // 2025-08-15 9PM UTC
        version: EngineVersion::V7,
    },
    VersionSchedule {
        active_at: 1755709200, // 2025-08-20 5PM UTC
        version: EngineVersion::V8,
    },
    VersionSchedule {
        active_at: 1756918800, // 2025-09-03 5PM UTC
        version: EngineVersion::V9,
    },
    VersionSchedule {
        active_at: 1757523600, // 2025-09-10 5PM UTC
        version: EngineVersion::V10,
    },
    VersionSchedule {
        active_at: 1758733200, // 2025-09-24 5PM UTC
        version: EngineVersion::V11,
    },
    VersionSchedule {
        active_at: 1758763200, // 2025-09-25 1:20AM UTC, block engine event id fix
        version: EngineVersion::V12,
    },
    VersionSchedule {
        active_at: 1758906000, // 2025-09-26 5PM UTC, storage lending allowance limit fix
        version: EngineVersion::V13,
    },
    VersionSchedule {
        active_at: 1761152400, // 2025-10-22 5PM UTC
        version: EngineVersion::V14,
    },
    VersionSchedule {
        active_at: 1764783794, // 2025-12-03 5:43PM UTC -- need to forward date because the change wasn't rolled to testnet when it went active.
        version: EngineVersion::V15,
    },
    VersionSchedule {
        active_at: 1777406400, // 2026-04-28 8PM UTC (3:00 PM CDT)
        version: EngineVersion::V16,
    },
    VersionSchedule {
        active_at: 1779382800, // 2026-05-21 5PM UTC (12:00 PM CDT)
        version: EngineVersion::V17,
    },
    VersionSchedule {
        active_at: 1781283600, // 2026-06-12 5PM UTC
        version: EngineVersion::V18,
    },
    VersionSchedule {
        active_at: 1784124000, // 2026-07-15 2PM UTC (9:00 AM CDT)
        version: EngineVersion::V19,
    },
    VersionSchedule {
        active_at: 1786035600, // 2026-08-06 5PM UTC (12:00 PM CDT)
        version: EngineVersion::V20,
    },
]
.as_slice();

const ENGINE_VERSION_SCHEDULE_DEVNET: &[VersionSchedule] = [VersionSchedule {
    active_at: 0,
    version: EngineVersion::V21,
}]
.as_slice();

impl EngineVersion {
    pub fn version_for(time: &FarcasterTime, network: FarcasterNetwork) -> EngineVersion {
        let schedule = match network {
            FarcasterNetwork::Mainnet => &ENGINE_VERSION_SCHEDULE_MAINNET,
            FarcasterNetwork::Testnet => &ENGINE_VERSION_SCHEDULE_TESTNET,
            _ => &ENGINE_VERSION_SCHEDULE_DEVNET,
        };
        let version = schedule
            .iter()
            .filter(|schedule| schedule.active_at <= time.to_unix_seconds())
            .last();
        match version {
            Some(schedule) => schedule.version,
            None => panic!(
                "No version schedule found for time: {}",
                time.to_unix_seconds()
            ),
        }
    }

    pub fn current(network: FarcasterNetwork) -> Self {
        Self::version_for(&FarcasterTime::current(), network)
    }

    /// Whether a replication snapshot taken at `snapshot_timestamp` (Farcaster
    /// seconds) may carry channel rows.
    ///
    /// Both sides of replication have to answer this identically or a snapshot
    /// straddling the boundary is served under one rule and replayed under
    /// another: the server gates which rows it emits (`replicator.rs`, from
    /// `ReplicationStores::get_timestamp`) and the bootstrap client gates whether
    /// it will merge them (`bootstrap/replication/service.rs`, from
    /// `ShardSnapshotMetadata.timestamp`). Those two timestamps share an origin —
    /// the replicator's own snapshot metadata is what populates the wire field —
    /// so keeping the derivation in one function is what makes the pair safe.
    /// Do not re-inline it at either call site.
    pub fn channel_messages_enabled_for_snapshot(
        snapshot_timestamp: u64,
        network: FarcasterNetwork,
    ) -> bool {
        Self::version_for(&FarcasterTime::new(snapshot_timestamp), network)
            .is_enabled(ProtocolFeature::ChannelMessages)
    }

    pub fn is_enabled(&self, feature: ProtocolFeature) -> bool {
        match feature {
            ProtocolFeature::SignerRevokeBug => {
                // This was a bug that was only active for a short time
                self == &EngineVersion::V1 || self == &EngineVersion::V3
            }
            ProtocolFeature::FarcasterPro
            | ProtocolFeature::Basenames
            | ProtocolFeature::EnsValidation
            | ProtocolFeature::MessageLengthCheckFix
            | ProtocolFeature::UsernameShardRoutingFix
            | ProtocolFeature::PrimaryAddresses => self >= &EngineVersion::V5,
            ProtocolFeature::FutureTimestampValidation => self >= &EngineVersion::V6,
            ProtocolFeature::DependentMessagesInBulkSubmit => self >= &EngineVersion::V7,
            ProtocolFeature::DecoupleShardZeroBlockProduction => self >= &EngineVersion::V8,
            ProtocolFeature::WriteDataToShardZero => self >= &EngineVersion::V9,
            ProtocolFeature::ReadDataFromShardZero | ProtocolFeature::UserProfileToken => {
                self >= &EngineVersion::V10
            }
            ProtocolFeature::StorageLending => self >= &EngineVersion::V11,
            ProtocolFeature::EventIdBugFix => self >= &EngineVersion::V12,
            ProtocolFeature::StorageLendingLimitFix => self >= &EngineVersion::V13,
            ProtocolFeature::StopRevokingExistingMessages => self >= &EngineVersion::V14,
            ProtocolFeature::IncreaseUsernameProofSizeLimit => self >= &EngineVersion::V15,
            ProtocolFeature::GaslessSigners => self >= &EngineVersion::V16,
            ProtocolFeature::LiveAt => self >= &EngineVersion::V17,
            ProtocolFeature::StorageExpiryExtension2026 => self >= &EngineVersion::V18,
            ProtocolFeature::BlockLinks => self >= &EngineVersion::V19,
            // Raises the cast embed cap to 4 for every fid, not just Pro subscribers. A pure
            // loosening: nothing that validated before this boundary stops validating after it,
            // so replay of pre-V20 history is untouched. It is still versioned because the
            // rolling-upgrade window is not symmetric — an upgraded proposer would include a
            // 4-embed non-Pro cast that an un-upgraded validator rejects.
            //
            // This sits BELOW the channel block on purpose. V20 was originally the channel
            // rollout; that work is blocked on the mainnet registrar deployment (see SEQUENCING
            // below), and because every gate here is `self >= VN`, scheduling anything above the
            // channel version would drag the channel features live with it. Taking the lower slot
            // is what lets this ship on its own.
            ProtocolFeature::IncreaseEmbedLimitForAllUsers => self >= &EngineVersion::V20,
            // Distinct features, but their activation boundaries MUST stay identical.
            // ChannelRegistrations gates *acceptance* of channel-register events (which build the
            // order-dependent shard-0 channel-owner index); SortedBlockEngineEvents gates the
            // *canonical ordering* of shard-0 system messages. If SortedBlockEngineEvents ever
            // lagged ChannelRegistrations, BlockEngine would accept channel-register events but
            // replay them unsorted, reintroducing the same-eth-block owner divergence this fix
            // closes. ChannelOwnershipEvents gates the shard-0 -> data-shard fan-out of those
            // registrations (and the ownership-change hints derived from them); it shares the same
            // boundary so no fanned history is ever missing (registrations cannot exist before the
            // gate opens, and it opens at the same instant), making backfill unnecessary.
            // ChannelMessages is consensus-coupled to registrations because channel-message
            // validation depends on the registration state established at that boundary. All four
            // are kept in one arm so they share the V21 boundary; the lock-step invariant is
            // enforced by `test_channel_features_activate_together`.
            //
            // VerificationsOnShardZero shares the V21 boundary because channel authority consumes
            // the shard-0 verification set. Post-V21 verification admission routes to shard 0;
            // accepted rows enter its trie, drive channel-owner resolution, and fan out in shard-0
            // consensus order to every data shard. The admission timestamp floor deliberately
            // excludes pre-V21 verification history, so authority begins from post-activation
            // state rather than a backfill.
            //
            // That makes RE-VERIFICATION A REQUIRED MIGRATION STEP, not an edge case: a channel
            // owner who verified their address before V21 has no shard-0 row, so their channel
            // resolves to owner fid 0 and rejects every permissioned write until they submit a
            // fresh verification of the same address. It is documented on GetChannelOwner and
            // ChannelOwnerResponse.fid because clients have to surface it. Anything that would
            // change this — a backfill, or relaxing the floor — has to reckon with the
            // tombstone-resurrection guard the floor exists to provide.
            //
            // ChannelMessages has consumers on both sides of the fan-out. BlockEngine validates
            // authority and merges the four slot stores on shard 0, then emits gated MergeMessage
            // BlockEvents. Every data shard replays those events through the same StoreDefs using
            // consensus-order slot replacement, updates its trie, emits its locally derived
            // HubEvents, and exposes the rows to state-root-verified replication. Direct channel
            // admission on a data shard remains rejected; replay and replication are its only
            // writers.
            //
            // Keep every future widening of this topology gated in the same change that makes it
            // reachable. The allowlist, replay dispatch, and replication cache each require an
            // explicit ChannelMessages check; catch-all match arms and StoreType::None provide no
            // compiler-enforced reminder. A type or index that mutates before its gate creates
            // permanent replay divergence. Derived index writes are the one case that IS
            // compiler-enforced: `DerivedIndexGate` is a required argument on all four channel
            // merges, so a store that gains a gated index cannot silently skip it.
            ProtocolFeature::ChannelRegistrations
            | ProtocolFeature::SortedBlockEngineEvents
            | ProtocolFeature::ChannelOwnershipEvents
            | ProtocolFeature::ChannelMessages
            | ProtocolFeature::VerificationsOnShardZero => self >= &EngineVersion::V21,
            // Ships in the V21 rollout, but deliberately kept in its own arm rather than
            // appended to the block above. Sharing that arm would assert a lock-step
            // obligation that does not exist here: those five MUST co-activate or
            // consensus breaks, whereas channel follows are not consensus-coupled to the
            // shard-0 channel set at all — a follow is an ordinary ReactionAdd on the
            // author's own shard, and the feature gates only whether the derived follow
            // index is written. A future version could move this boundary alone without
            // touching the invariant those five encode. Keeping the arms separate also
            // keeps `test_channel_features_activate_together` honest: it names its five
            // features explicitly, so a sixth sharing the arm would look covered by it
            // while being asserted nowhere.
            //
            // This gate is read from `MergeContext.version`, i.e. the version for the
            // *message's own embedded timestamp*, and never from a block or snapshot
            // clock. That is the only clock both write paths share: live merge and
            // bootstrap replay both reach `ShardEngine::merge_message`, but bootstrap
            // has only a snapshot timestamp, so gating on that would make a
            // post-activation snapshot index pre-activation reactions that a node
            // living through the boundary would have skipped. On the message clock the
            // index is a pure function of the merged reaction set: same reactions, same
            // index, however they arrived.
            //
            // The teardown side is deliberately ungated — see `FollowIndexGate` in
            // `store.rs`. Prune and revoke reach `delete_add_transaction` with no
            // version in hand, so removal is driven by row presence instead.
            //
            // SEQUENCING: a mainnet `active_at` for V21 may not be scheduled while
            // `channel_registrar_for_network(Mainnet)` is still `None` — the registrar
            // contract is not deployed. Flipping that constant from `None` to `Some`
            // after activation would itself be an unversioned change to derived state,
            // so the constant and the schedule entry land together. This is a stricter
            // precondition than the rest of V21 carries, and it now blocks the whole
            // version rather than one feature within it.
            // `channel_follows_requires_a_registrar_wherever_it_is_scheduled` pins this.
            //
            // This precondition attaches to V21 ONLY. It is why the channel features live
            // here rather than at V20: V20 is scheduled on mainnet and testnet and carries
            // no channel behavior, so it is not gated on the registrar. Do not read a
            // scheduled V20 as a violation of this rule.
            ProtocolFeature::ChannelFollows => self >= &EngineVersion::V21,
        }
    }

    pub fn protocol_version(&self) -> u32 {
        match self {
            EngineVersion::V0
            | EngineVersion::V1
            | EngineVersion::V2
            | EngineVersion::V3
            | EngineVersion::V4 => 1,
            EngineVersion::V5 => 2,
            EngineVersion::V6 => 3,
            EngineVersion::V7 => 4,
            EngineVersion::V8 => 5,
            EngineVersion::V9 => 6,
            EngineVersion::V10 => 7,
            EngineVersion::V11 | EngineVersion::V12 | EngineVersion::V13 => 8,
            EngineVersion::V14 => 9,
            EngineVersion::V15 => 10,
            EngineVersion::V16 => 11,
            EngineVersion::V17 => 12,
            EngineVersion::V18 | EngineVersion::V19 | EngineVersion::V20 | EngineVersion::V21 => {
                LATEST_PROTOCOL_VERSION
            }
        }
    }

    pub fn latest() -> Self {
        EngineVersion::iter()
            .max()
            .expect("Version list can't be empty")
    }

    pub fn next_version_timestamp_for(
        time: &FarcasterTime,
        network: FarcasterNetwork,
    ) -> Option<u64> {
        let schedule = match network {
            FarcasterNetwork::Mainnet => &ENGINE_VERSION_SCHEDULE_MAINNET,
            FarcasterNetwork::Testnet => &ENGINE_VERSION_SCHEDULE_TESTNET,
            _ => &ENGINE_VERSION_SCHEDULE_DEVNET,
        };

        schedule
            .iter()
            .find(|schedule_entry| schedule_entry.active_at > time.to_unix_seconds())
            .map(|schedule_entry| schedule_entry.active_at)
    }
}

#[cfg(test)]
mod version_test {
    use super::*;

    #[test]
    fn test_engine_version_values() {
        assert_eq!(EngineVersion::V0 as u8, 0);
        assert_eq!(EngineVersion::V1 as u8, 1);
        assert_eq!(EngineVersion::V2 as u8, 2);
    }

    #[test]
    fn test_engine_version_ordering() {
        assert!(EngineVersion::V0 < EngineVersion::V1);
        assert!(EngineVersion::V1 < EngineVersion::V2);
        assert!(EngineVersion::V0 < EngineVersion::V2);

        assert!(EngineVersion::V2 > EngineVersion::V1);
        assert!(EngineVersion::V1 > EngineVersion::V0);

        assert_eq!(EngineVersion::V0, EngineVersion::V0);
        assert_eq!(EngineVersion::V1, EngineVersion::V1);
        assert_eq!(EngineVersion::V2, EngineVersion::V2);
    }

    #[test]
    fn test_latest_progression() {
        for i in 1..ENGINE_VERSION_SCHEDULE_MAINNET.len() {
            let previous_version = &ENGINE_VERSION_SCHEDULE_MAINNET[i - 1];
            let current_version = &ENGINE_VERSION_SCHEDULE_MAINNET[i];

            assert!(
                current_version.version > previous_version.version,
                "Version {:?} should be greater than {:?}",
                current_version.version,
                previous_version.version
            );
            assert!(
                current_version.active_at >= previous_version.active_at,
                "Active time {:?} should be greater than {:?}",
                current_version.active_at,
                previous_version.active_at
            );
            assert!(
                current_version.version.protocol_version()
                    >= previous_version.version.protocol_version(),
                "Protocol version for {:?} should be greater than or equal to {:?}",
                current_version.version,
                previous_version.version
            );
        }
    }

    #[test]
    fn test_version_for_mainnet_with_current_schedule() {
        let time = FarcasterTime::new(0);
        assert_eq!(
            EngineVersion::version_for(&time, FarcasterNetwork::Mainnet),
            EngineVersion::V0
        );

        let time = FarcasterTime::from_unix_seconds(1747352401);
        assert_eq!(
            EngineVersion::version_for(&time, FarcasterNetwork::Mainnet),
            EngineVersion::V2
        );

        let time = FarcasterTime::from_unix_seconds(1748970000);
        assert_eq!(
            EngineVersion::version_for(&time, FarcasterNetwork::Mainnet),
            EngineVersion::V4
        );
    }

    #[test]
    fn test_version_for_testnet_with_current_schedule() {
        let time = FarcasterTime::new(0);
        assert_eq!(
            EngineVersion::version_for(&time, FarcasterNetwork::Testnet),
            EngineVersion::V0
        );

        let time = FarcasterTime::from_unix_seconds(1748970000);
        assert_eq!(
            EngineVersion::version_for(&time, FarcasterNetwork::Testnet),
            EngineVersion::V5
        );
    }

    #[test]
    fn test_latest_version_for_mainnet_matches_testnet() {
        let latest_mainnet_version = ENGINE_VERSION_SCHEDULE_MAINNET.last();
        let latest_testnet_version = ENGINE_VERSION_SCHEDULE_TESTNET.last();
        assert_eq!(
            latest_mainnet_version.map(|v| v.version),
            latest_testnet_version.map(|v| v.version)
        );
    }

    #[test]
    fn test_version_for_devnet_with_current_schedule() {
        // Devnet always has the latest version
        assert_eq!(ENGINE_VERSION_SCHEDULE_DEVNET.len(), 1);

        let time = FarcasterTime::new(0);
        assert_eq!(
            EngineVersion::version_for(&time, FarcasterNetwork::Devnet),
            EngineVersion::latest()
        );

        let time = FarcasterTime::current();
        assert_eq!(
            EngineVersion::version_for(&time, FarcasterNetwork::Devnet),
            EngineVersion::latest()
        );
    }

    #[test]
    fn test_gasless_signers_feature_gate() {
        // Gate closed below V16, open at V16+.
        assert_eq!(
            EngineVersion::V15.is_enabled(ProtocolFeature::GaslessSigners),
            false
        );
        assert_eq!(
            EngineVersion::V16.is_enabled(ProtocolFeature::GaslessSigners),
            true
        );
        assert_eq!(
            EngineVersion::latest().is_enabled(ProtocolFeature::GaslessSigners),
            true
        );
    }

    #[test]
    fn test_live_at_feature_gate() {
        assert_eq!(
            EngineVersion::V16.is_enabled(ProtocolFeature::LiveAt),
            false
        );
        assert_eq!(EngineVersion::V17.is_enabled(ProtocolFeature::LiveAt), true);
        assert_eq!(
            EngineVersion::latest().is_enabled(ProtocolFeature::LiveAt),
            true
        );
    }

    #[test]
    fn test_gasless_signers_activation_schedule() {
        // Testnet: V16 at 2026-04-28 20:00 UTC (3:00 PM CDT); pre-activation returns V15.
        let testnet_active = 1777406400;
        assert_eq!(
            EngineVersion::version_for(
                &FarcasterTime::from_unix_seconds(testnet_active - 1),
                FarcasterNetwork::Testnet,
            ),
            EngineVersion::V15
        );
        assert_eq!(
            EngineVersion::version_for(
                &FarcasterTime::from_unix_seconds(testnet_active),
                FarcasterNetwork::Testnet,
            ),
            EngineVersion::V16
        );

        // Mainnet: V16 at 2026-05-07 17:00 UTC (12:00 PM CDT); pre-activation returns V15.
        let mainnet_active = 1778173200;
        assert_eq!(
            EngineVersion::version_for(
                &FarcasterTime::from_unix_seconds(mainnet_active - 1),
                FarcasterNetwork::Mainnet,
            ),
            EngineVersion::V15
        );
        assert_eq!(
            EngineVersion::version_for(
                &FarcasterTime::from_unix_seconds(mainnet_active),
                FarcasterNetwork::Mainnet,
            ),
            EngineVersion::V16
        );
    }

    #[test]
    fn test_live_at_activation_schedule() {
        let testnet_active = 1779382800;
        assert_eq!(
            EngineVersion::version_for(
                &FarcasterTime::from_unix_seconds(testnet_active - 1),
                FarcasterNetwork::Testnet,
            ),
            EngineVersion::V16
        );
        assert_eq!(
            EngineVersion::version_for(
                &FarcasterTime::from_unix_seconds(testnet_active),
                FarcasterNetwork::Testnet,
            ),
            EngineVersion::V17
        );

        let mainnet_active = 1780592400;
        assert_eq!(
            EngineVersion::version_for(
                &FarcasterTime::from_unix_seconds(mainnet_active - 1),
                FarcasterNetwork::Mainnet,
            ),
            EngineVersion::V16
        );
        assert_eq!(
            EngineVersion::version_for(
                &FarcasterTime::from_unix_seconds(mainnet_active),
                FarcasterNetwork::Mainnet,
            ),
            EngineVersion::V17
        );
    }

    #[test]
    fn test_storage_expiry_extension_feature_gate() {
        // Gate closed below V18, open at V18+.
        assert_eq!(
            EngineVersion::V17.is_enabled(ProtocolFeature::StorageExpiryExtension2026),
            false
        );
        assert_eq!(
            EngineVersion::V18.is_enabled(ProtocolFeature::StorageExpiryExtension2026),
            true
        );
        assert_eq!(
            EngineVersion::latest().is_enabled(ProtocolFeature::StorageExpiryExtension2026),
            true
        );
    }

    #[test]
    fn test_storage_expiry_extension_activation_schedule() {
        // Testnet: V18 at 2026-06-12 17:00 UTC; pre-activation returns V17.
        let testnet_active = 1781283600;
        assert_eq!(
            EngineVersion::version_for(
                &FarcasterTime::from_unix_seconds(testnet_active - 1),
                FarcasterNetwork::Testnet,
            ),
            EngineVersion::V17
        );
        assert_eq!(
            EngineVersion::version_for(
                &FarcasterTime::from_unix_seconds(testnet_active),
                FarcasterNetwork::Testnet,
            ),
            EngineVersion::V18
        );

        // Mainnet: V18 at 2026-06-22 17:00 UTC; pre-activation returns V17.
        let mainnet_active = 1782147600;
        assert_eq!(
            EngineVersion::version_for(
                &FarcasterTime::from_unix_seconds(mainnet_active - 1),
                FarcasterNetwork::Mainnet,
            ),
            EngineVersion::V17
        );
        assert_eq!(
            EngineVersion::version_for(
                &FarcasterTime::from_unix_seconds(mainnet_active),
                FarcasterNetwork::Mainnet,
            ),
            EngineVersion::V18
        );
    }

    #[test]
    fn test_block_links_feature_gate() {
        // Gate closed below V19, open at V19+. This boundary is the source of truth for
        // pre-V19 replay safety, so pin it explicitly.
        assert_eq!(
            EngineVersion::V18.is_enabled(ProtocolFeature::BlockLinks),
            false
        );
        assert_eq!(
            EngineVersion::V19.is_enabled(ProtocolFeature::BlockLinks),
            true
        );
        assert_eq!(
            EngineVersion::latest().is_enabled(ProtocolFeature::BlockLinks),
            true
        );
    }

    #[test]
    fn test_block_links_activation_schedule() {
        // Testnet: V19 at 2026-07-15 14:00 UTC (9:00 AM CDT); pre-activation returns V18.
        let testnet_active = 1784124000;
        assert_eq!(
            EngineVersion::version_for(
                &FarcasterTime::from_unix_seconds(testnet_active - 1),
                FarcasterNetwork::Testnet,
            ),
            EngineVersion::V18
        );
        assert_eq!(
            EngineVersion::version_for(
                &FarcasterTime::from_unix_seconds(testnet_active),
                FarcasterNetwork::Testnet,
            ),
            EngineVersion::V19
        );

        // Mainnet: V19 at 2026-07-27 14:00 UTC (9:00 AM CDT); pre-activation returns V18.
        let mainnet_active = 1785160800;
        assert_eq!(
            EngineVersion::version_for(
                &FarcasterTime::from_unix_seconds(mainnet_active - 1),
                FarcasterNetwork::Mainnet,
            ),
            EngineVersion::V18
        );
        assert_eq!(
            EngineVersion::version_for(
                &FarcasterTime::from_unix_seconds(mainnet_active),
                FarcasterNetwork::Mainnet,
            ),
            EngineVersion::V19
        );

        // Devnet: V19 from genesis. Devnet runs the latest version (V21+ on this branch), so
        // assert the feature rather than version equality.
        assert!(
            EngineVersion::version_for(&FarcasterTime::new(0), FarcasterNetwork::Devnet)
                .is_enabled(ProtocolFeature::BlockLinks)
        );
    }

    #[test]
    fn test_increase_embed_limit_feature_gate() {
        // Gate closed below V20, open at V20+. Below the boundary the cap is 4 for Pro and 2 for
        // everyone else; at and above it, 4 for everyone. Pin it explicitly: this decides whether
        // a replayed non-Pro cast with 3 or 4 embeds validates, so moving it silently would fork
        // every node's view of history.
        assert_eq!(
            EngineVersion::V19.is_enabled(ProtocolFeature::IncreaseEmbedLimitForAllUsers),
            false
        );
        assert_eq!(
            EngineVersion::V20.is_enabled(ProtocolFeature::IncreaseEmbedLimitForAllUsers),
            true
        );
        assert_eq!(
            EngineVersion::latest().is_enabled(ProtocolFeature::IncreaseEmbedLimitForAllUsers),
            true
        );
    }

    #[test]
    fn test_increase_embed_limit_activation_schedule() {
        // Testnet: V20 at 2026-08-06 17:00 UTC (12:00 PM CDT); pre-activation returns V19.
        let testnet_active = 1786035600;
        assert_eq!(
            EngineVersion::version_for(
                &FarcasterTime::from_unix_seconds(testnet_active - 1),
                FarcasterNetwork::Testnet,
            ),
            EngineVersion::V19
        );
        assert_eq!(
            EngineVersion::version_for(
                &FarcasterTime::from_unix_seconds(testnet_active),
                FarcasterNetwork::Testnet,
            ),
            EngineVersion::V20
        );

        // Mainnet: V20 at 2026-08-13 17:00 UTC (12:00 PM CDT); pre-activation returns V19.
        let mainnet_active = 1786640400;
        assert_eq!(
            EngineVersion::version_for(
                &FarcasterTime::from_unix_seconds(mainnet_active - 1),
                FarcasterNetwork::Mainnet,
            ),
            EngineVersion::V19
        );
        assert_eq!(
            EngineVersion::version_for(
                &FarcasterTime::from_unix_seconds(mainnet_active),
                FarcasterNetwork::Mainnet,
            ),
            EngineVersion::V20
        );

        // Testnet leads mainnet, so a regression that swapped the two constants is caught here
        // rather than at the cutover.
        assert!(testnet_active < mainnet_active);

        // Devnet runs the latest version (V21+), so assert the feature rather than version
        // equality.
        assert!(
            EngineVersion::version_for(&FarcasterTime::new(0), FarcasterNetwork::Devnet)
                .is_enabled(ProtocolFeature::IncreaseEmbedLimitForAllUsers)
        );
    }

    #[test]
    fn test_embed_limit_activates_without_the_channel_rollout() {
        // The whole point of putting embeds at V20 and channels at V21: scheduling the embed
        // change must NOT drag the channel features live. Both networks now schedule V20, so
        // every channel gate has to stay shut at their latest scheduled version.
        let far_future = FarcasterTime::from_unix_seconds(4102444800); // 2100-01-01 UTC
        for network in [FarcasterNetwork::Mainnet, FarcasterNetwork::Testnet] {
            let version = EngineVersion::version_for(&far_future, network);
            assert!(version.is_enabled(ProtocolFeature::IncreaseEmbedLimitForAllUsers));
            assert!(!version.is_enabled(ProtocolFeature::ChannelRegistrations));
            assert!(!version.is_enabled(ProtocolFeature::ChannelMessages));
            assert!(!version.is_enabled(ProtocolFeature::ChannelFollows));
            assert!(!version.is_enabled(ProtocolFeature::VerificationsOnShardZero));
        }
    }

    #[test]
    fn test_channel_registrations_feature_gate() {
        // Gate closed below V21, open at V21+. The engine's admission gate for
        // channel-register events consults this boundary at replay, so pin it explicitly —
        // an accidental change would alter pre-V21 replay behavior.
        assert_eq!(
            EngineVersion::V20.is_enabled(ProtocolFeature::ChannelRegistrations),
            false
        );
        assert_eq!(
            EngineVersion::V21.is_enabled(ProtocolFeature::ChannelRegistrations),
            true
        );
        assert_eq!(
            EngineVersion::latest().is_enabled(ProtocolFeature::ChannelRegistrations),
            true
        );
    }

    #[test]
    fn test_channel_registrations_activation_schedule() {
        // V21 is unscheduled on mainnet/testnet: the feature must stay dormant there even
        // far in the future, and active on devnet (which always runs the latest version).
        // Asserted via is_enabled rather than a pinned version so this only breaks when
        // V21 (or a later version) is scheduled, not when unrelated earlier versions are.
        let far_future = FarcasterTime::from_unix_seconds(4102444800); // 2100-01-01 UTC
        for network in [FarcasterNetwork::Mainnet, FarcasterNetwork::Testnet] {
            assert!(!EngineVersion::version_for(&far_future, network)
                .is_enabled(ProtocolFeature::ChannelRegistrations));
        }
        assert!(
            EngineVersion::version_for(&FarcasterTime::new(0), FarcasterNetwork::Devnet)
                .is_enabled(ProtocolFeature::ChannelRegistrations)
        );
    }

    #[test]
    fn test_sorted_block_engine_events_feature_gate() {
        // Gate closed below V21, open at V21+. BlockEngine replay consults this
        // boundary before canonicalizing shard-0 onchain-event order.
        assert_eq!(
            EngineVersion::V20.is_enabled(ProtocolFeature::SortedBlockEngineEvents),
            false
        );
        assert_eq!(
            EngineVersion::V21.is_enabled(ProtocolFeature::SortedBlockEngineEvents),
            true
        );
        assert_eq!(
            EngineVersion::latest().is_enabled(ProtocolFeature::SortedBlockEngineEvents),
            true
        );
    }

    #[test]
    fn test_sorted_block_engine_events_activation_schedule() {
        let far_future = FarcasterTime::from_unix_seconds(4102444800); // 2100-01-01 UTC
        for network in [FarcasterNetwork::Mainnet, FarcasterNetwork::Testnet] {
            assert!(!EngineVersion::version_for(&far_future, network)
                .is_enabled(ProtocolFeature::SortedBlockEngineEvents));
        }
        assert!(
            EngineVersion::version_for(&FarcasterTime::new(0), FarcasterNetwork::Devnet)
                .is_enabled(ProtocolFeature::SortedBlockEngineEvents)
        );
    }

    #[test]
    fn test_channel_ownership_events_feature_gate() {
        // Gate closed below V21, open at V21+. handle_block_event consults this boundary before
        // admitting a MergeOnChainEvent BlockEvent, so pin it explicitly.
        assert_eq!(
            EngineVersion::V20.is_enabled(ProtocolFeature::ChannelOwnershipEvents),
            false
        );
        assert_eq!(
            EngineVersion::V21.is_enabled(ProtocolFeature::ChannelOwnershipEvents),
            true
        );
        assert_eq!(
            EngineVersion::latest().is_enabled(ProtocolFeature::ChannelOwnershipEvents),
            true
        );
    }

    #[test]
    fn test_channel_ownership_events_activation_schedule() {
        // V21 is unscheduled on mainnet/testnet: the feature must stay dormant there even far in
        // the future, and active on devnet (which always runs the latest version). Asserted via
        // is_enabled rather than a pinned version so this only breaks when V21 (or a later
        // version) is scheduled, not when unrelated earlier versions are.
        let far_future = FarcasterTime::from_unix_seconds(4102444800); // 2100-01-01 UTC
        for network in [FarcasterNetwork::Mainnet, FarcasterNetwork::Testnet] {
            assert!(!EngineVersion::version_for(&far_future, network)
                .is_enabled(ProtocolFeature::ChannelOwnershipEvents));
        }
        assert!(
            EngineVersion::version_for(&FarcasterTime::new(0), FarcasterNetwork::Devnet)
                .is_enabled(ProtocolFeature::ChannelOwnershipEvents)
        );
    }

    #[test]
    fn test_channel_messages_feature_gate() {
        assert!(!EngineVersion::V20.is_enabled(ProtocolFeature::ChannelMessages));
        assert!(EngineVersion::V21.is_enabled(ProtocolFeature::ChannelMessages));
        assert!(EngineVersion::latest().is_enabled(ProtocolFeature::ChannelMessages));
    }

    #[test]
    fn test_channel_messages_activation_schedule() {
        let far_future = FarcasterTime::from_unix_seconds(4102444800); // 2100-01-01 UTC
        for network in [FarcasterNetwork::Mainnet, FarcasterNetwork::Testnet] {
            assert!(!EngineVersion::version_for(&far_future, network)
                .is_enabled(ProtocolFeature::ChannelMessages));
        }
        assert!(
            EngineVersion::version_for(&FarcasterTime::new(0), FarcasterNetwork::Devnet)
                .is_enabled(ProtocolFeature::ChannelMessages)
        );
    }

    #[test]
    fn test_verifications_on_shard_zero_feature_gate() {
        assert!(!EngineVersion::V20.is_enabled(ProtocolFeature::VerificationsOnShardZero));
        assert!(EngineVersion::V21.is_enabled(ProtocolFeature::VerificationsOnShardZero));
        assert!(EngineVersion::latest().is_enabled(ProtocolFeature::VerificationsOnShardZero));

        let far_future = FarcasterTime::from_unix_seconds(4102444800); // 2100-01-01 UTC
        for network in [FarcasterNetwork::Mainnet, FarcasterNetwork::Testnet] {
            assert!(!EngineVersion::version_for(&far_future, network)
                .is_enabled(ProtocolFeature::VerificationsOnShardZero));
        }
        assert!(
            EngineVersion::version_for(&FarcasterTime::new(0), FarcasterNetwork::Devnet)
                .is_enabled(ProtocolFeature::VerificationsOnShardZero)
        );
    }

    #[test]
    fn test_channel_follows_feature_gate() {
        assert!(!EngineVersion::V20.is_enabled(ProtocolFeature::ChannelFollows));
        assert!(EngineVersion::V21.is_enabled(ProtocolFeature::ChannelFollows));
        assert!(EngineVersion::latest().is_enabled(ProtocolFeature::ChannelFollows));
    }

    #[test]
    fn test_channel_follows_activation_schedule() {
        let far_future = FarcasterTime::from_unix_seconds(4102444800); // 2100-01-01 UTC
        for network in [FarcasterNetwork::Mainnet, FarcasterNetwork::Testnet] {
            assert!(!EngineVersion::version_for(&far_future, network)
                .is_enabled(ProtocolFeature::ChannelFollows));
        }
        assert!(
            EngineVersion::version_for(&FarcasterTime::new(0), FarcasterNetwork::Devnet)
                .is_enabled(ProtocolFeature::ChannelFollows)
        );
    }

    #[test]
    fn test_channel_features_activate_together() {
        // CONSENSUS INVARIANT: the four channel features must be enabled at the exact same
        // versions. VerificationsOnShardZero is pinned alongside them for a weaker reason, spelled
        // out at the end of this comment — do not read it as consensus-coupled.
        // If SortedBlockEngineEvents ever lagged ChannelRegistrations, BlockEngine would accept
        // channel-register events but replay them unsorted, reintroducing the same-eth-block
        // channel-owner divergence this fix closes. ChannelOwnershipEvents gates the fan-out of
        // those registrations; if it lagged, a registration could be admitted with no shard ever
        // fanning it out (or, mixed across binaries, diverge on which blocks fan out).
        // ChannelMessages validates against the registration state, so it must share the same
        // boundary. VerificationsOnShardZero has no present coupling — it is pinned here so the
        // shard-0 replica shares the one V21 rollout boundary rather than drifting into its own;
        // see the comment on the matching arm in `is_enabled`. This test fails CI if a future
        // change splits any activation boundary.
        //
        // ChannelFollows also activates at V21 but is deliberately NOT asserted here: it ships in
        // the same rollout without being coupled to it, so a later change may move its boundary
        // alone. `test_channel_follows_feature_gate` is what pins it.
        use strum::IntoEnumIterator;
        for version in EngineVersion::iter() {
            let channel_registrations = version.is_enabled(ProtocolFeature::ChannelRegistrations);
            assert_eq!(
                channel_registrations,
                version.is_enabled(ProtocolFeature::SortedBlockEngineEvents),
                "ChannelRegistrations and SortedBlockEngineEvents must co-activate; they differ at {:?}",
                version
            );
            assert_eq!(
                channel_registrations,
                version.is_enabled(ProtocolFeature::ChannelOwnershipEvents),
                "ChannelRegistrations and ChannelOwnershipEvents must co-activate; they differ at {:?}",
                version
            );
            assert_eq!(
                channel_registrations,
                version.is_enabled(ProtocolFeature::ChannelMessages),
                "ChannelRegistrations and ChannelMessages must co-activate; they differ at {:?}",
                version
            );
            assert_eq!(
                channel_registrations,
                version.is_enabled(ProtocolFeature::VerificationsOnShardZero),
                "ChannelRegistrations and VerificationsOnShardZero must co-activate; they differ at {:?}",
                version
            );
        }
    }

    #[test]
    fn snapshot_channel_gate_tracks_the_activation_boundary_on_every_network() {
        // Both sides of replication derive "may this snapshot carry channel rows?"
        // from a snapshot timestamp — the server to decide which rows it emits, the
        // bootstrap client to decide whether it will merge them. They now share this
        // function, so this is the one place that behavior is pinned. Hardcoding
        // either call site to a literal used to leave every test green while a
        // post-activation bootstrap died on its first channel row.
        for network in [
            FarcasterNetwork::Mainnet,
            FarcasterNetwork::Testnet,
            FarcasterNetwork::Devnet,
        ] {
            for timestamp in [0u64, 1, 1_000_000, u32::MAX as u64] {
                assert_eq!(
                    EngineVersion::channel_messages_enabled_for_snapshot(timestamp, network),
                    EngineVersion::version_for(&FarcasterTime::new(timestamp), network)
                        .is_enabled(ProtocolFeature::ChannelMessages),
                    "snapshot gate must equal the schedule's answer at {timestamp} on {network:?}"
                );
            }
        }

        // Devnet activates channel messages at genesis, so a devnet snapshot always
        // carries them — the case the bootstrap tests actually exercise.
        assert!(EngineVersion::channel_messages_enabled_for_snapshot(
            0,
            FarcasterNetwork::Devnet
        ));
        // Mainnet at timestamp 0 predates every activation, so a snapshot from before
        // the boundary must not be treated as channel-bearing.
        assert!(!EngineVersion::channel_messages_enabled_for_snapshot(
            0,
            FarcasterNetwork::Mainnet
        ));
    }

    #[test]
    fn test_is_enabled_signer_revoke_bug() {
        assert_eq!(
            EngineVersion::V0.is_enabled(ProtocolFeature::SignerRevokeBug),
            false
        );
        assert_eq!(
            EngineVersion::V1.is_enabled(ProtocolFeature::SignerRevokeBug),
            true
        );
        assert_eq!(
            EngineVersion::V2.is_enabled(ProtocolFeature::SignerRevokeBug),
            false
        );
        assert_eq!(
            EngineVersion::V3.is_enabled(ProtocolFeature::SignerRevokeBug),
            true
        );
        assert_eq!(
            EngineVersion::V4.is_enabled(ProtocolFeature::SignerRevokeBug),
            false
        );
    }

    #[test]
    fn test_latest() {
        assert_eq!(EngineVersion::latest(), EngineVersion::V21);
        assert_eq!(
            EngineVersion::version_for(&FarcasterTime::current(), FarcasterNetwork::Devnet),
            EngineVersion::latest()
        );
        assert_eq!(
            EngineVersion::latest().protocol_version(),
            LATEST_PROTOCOL_VERSION
        );
    }

    #[test]
    fn test_next_version_timestamp_for() {
        let time = FarcasterTime::from_unix_seconds(1747333000);
        assert_eq!(
            EngineVersion::next_version_timestamp_for(&time, FarcasterNetwork::Mainnet),
            Some(1747333800)
        );

        let time = FarcasterTime::from_unix_seconds(1747333800);
        assert_eq!(
            EngineVersion::next_version_timestamp_for(&time, FarcasterNetwork::Mainnet),
            Some(1747352400)
        );

        let time = FarcasterTime::from_unix_seconds(1765386000);
        assert_eq!(
            EngineVersion::next_version_timestamp_for(&time, FarcasterNetwork::Mainnet),
            Some(1778173200)
        );

        let time = FarcasterTime::from_unix_seconds(1778173200);
        assert_eq!(
            EngineVersion::next_version_timestamp_for(&time, FarcasterNetwork::Mainnet),
            Some(1780592400)
        );

        let time = FarcasterTime::from_unix_seconds(1780592400);
        assert_eq!(
            EngineVersion::next_version_timestamp_for(&time, FarcasterNetwork::Mainnet),
            Some(1782147600)
        );

        let time = FarcasterTime::from_unix_seconds(1782147600);
        assert_eq!(
            EngineVersion::next_version_timestamp_for(&time, FarcasterNetwork::Mainnet),
            Some(1785160800)
        );

        let time = FarcasterTime::from_unix_seconds(1785160800);
        assert_eq!(
            EngineVersion::next_version_timestamp_for(&time, FarcasterNetwork::Mainnet),
            Some(1786640400)
        );

        // V20 is the last scheduled mainnet version; V21 (channels) has no entry yet.
        let time = FarcasterTime::from_unix_seconds(1786640400);
        assert_eq!(
            EngineVersion::next_version_timestamp_for(&time, FarcasterNetwork::Mainnet),
            None
        );

        let time = FarcasterTime::from_unix_seconds(1640995200); // January 1, 2022 UTC
        assert_eq!(
            EngineVersion::next_version_timestamp_for(&time, FarcasterNetwork::Testnet),
            Some(1748970000)
        );

        let time = FarcasterTime::from_unix_seconds(1640995200); // January 1, 2022 UTC
        assert_eq!(
            EngineVersion::next_version_timestamp_for(&time, FarcasterNetwork::Devnet),
            None
        );
    }
}
