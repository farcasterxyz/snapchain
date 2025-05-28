use crate::core::util::FarcasterTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd)]
pub enum EngineVersion {
    V0 = 0,
    V1 = 1,
    V2 = 2,
    V3 = 3,
    V4 = 4,
}

pub enum ProtocolFeature {
    SignerRevokeBug,
}

pub struct VersionSchedule {
    pub active_at: u64, // Unix timestamp in seconds
    pub version: EngineVersion,
}

const ENGINE_VERSION_SCHEDULE: [VersionSchedule; 5] = [
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
];

impl EngineVersion {
    pub fn version_for(time: &FarcasterTime) -> EngineVersion {
        let version = ENGINE_VERSION_SCHEDULE
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

    pub fn latest() -> EngineVersion {
        ENGINE_VERSION_SCHEDULE.last().unwrap().version
    }

    pub fn is_enabled(&self, feature: ProtocolFeature) -> bool {
        match feature {
            ProtocolFeature::SignerRevokeBug => {
                // This was a bug that was only active for a short time
                self == &EngineVersion::V1 || self == &EngineVersion::V3
            }
        }
    }
}

#[cfg(test)]
mod version_test {
    use super::*;

    #[test]
    fn test_protocol_version_values() {
        assert_eq!(EngineVersion::V0 as u8, 0);
        assert_eq!(EngineVersion::V1 as u8, 1);
        assert_eq!(EngineVersion::V2 as u8, 2);
    }

    #[test]
    fn test_protocol_version_ordering() {
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
        for i in 1..ENGINE_VERSION_SCHEDULE.len() {
            let previous_version = &ENGINE_VERSION_SCHEDULE[i - 1];
            let current_version = &ENGINE_VERSION_SCHEDULE[i];

            assert!(
                current_version.version > previous_version.version,
                "Version {:?} should be greater than {:?}",
                current_version.version,
                previous_version.version
            );
            assert!(
                current_version.active_at > previous_version.active_at,
                "Active time {:?} should be greater than {:?}",
                current_version.active_at,
                previous_version.active_at
            );
        }
    }

    #[test]
    fn test_version_for_with_current_schedule() {
        let time = FarcasterTime::new(0);
        assert_eq!(EngineVersion::version_for(&time), EngineVersion::V0);

        let time = FarcasterTime::from_unix_seconds(1747352401);
        assert_eq!(EngineVersion::version_for(&time), EngineVersion::V2);

        let time = FarcasterTime::current();
        assert_eq!(EngineVersion::version_for(&time), EngineVersion::V4);
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
}
