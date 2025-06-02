#[cfg(test)]
mod tests {
    use std::{collections::HashMap, time::Duration};

    use ractor::concurrency::sleep;

    use crate::{
        mempool::mempool::{RateLimits, RateLimitsConfig},
        storage::store::{
            engine::ShardEngine,
            stores::{Limits, StoreLimits, Stores},
            test_helper::{
                self, limits, register_user, statsd_client, EngineOptions, FID_FOR_TEST,
            },
        },
    };

    fn setup(limits: Limits) -> (ShardEngine, HashMap<u32, Stores>) {
        let (engine, _tmpdir) = test_helper::new_engine_with_options(EngineOptions {
            limits: Some(StoreLimits {
                limits,
                legacy_limits: Limits {
                    casts: 0,
                    links: 0,
                    reactions: 0,
                    user_data: 0,
                    user_name_proofs: 0,
                    verifications: 0,
                },
            }),
            db: None,
            messages_request_tx: None,
            network: None,
        });
        let mut shard_stores = HashMap::new();
        shard_stores.insert(engine.shard_id(), engine.get_stores());
        return (engine, shard_stores);
    }

    #[tokio::test]
    async fn test_basic_rate_limits() {
        // Make limits high so refresh rate is high enough that we can sent a message after the rate limit is hit in test.
        let (mut engine, shard_stores) = setup(Limits {
            casts: 10000,
            links: 10000,
            reactions: 10000,
            user_data: 10000,
            user_name_proofs: 10000,
            verifications: 10000,
        });

        register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;

        let mut rate_limits = RateLimits::new(
            shard_stores,
            RateLimitsConfig {
                time_to_idle: Duration::from_secs(2),
                max_capacity: 10,
            },
            statsd_client(),
        );

        for _ in 0..6000 {
            assert!(rate_limits.consume_for_fid(engine.shard_id(), FID_FOR_TEST))
        }

        assert!(!rate_limits.consume_for_fid(engine.shard_id(), FID_FOR_TEST));

        sleep(Duration::from_millis(1500)).await;

        assert!(rate_limits.consume_for_fid(engine.shard_id(), FID_FOR_TEST));
    }

    #[tokio::test]
    async fn test_exceed_time_to_idle() {
        let (mut engine, shard_stores) = setup(Limits {
            casts: 1000,
            links: 1000,
            reactions: 1000,
            user_data: 1000,
            user_name_proofs: 1000,
            verifications: 1000,
        });

        register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;

        let mut rate_limits = RateLimits::new(
            shard_stores,
            RateLimitsConfig {
                time_to_idle: Duration::from_millis(10), // Time to idle is lower than the refresh rate on the rate limiter here since the limits are fairly small
                max_capacity: 10,
            },
            statsd_client(),
        );

        for _ in 0..600 {
            assert!(rate_limits.consume_for_fid(engine.shard_id(), FID_FOR_TEST))
        }

        assert!(!rate_limits.consume_for_fid(engine.shard_id(), FID_FOR_TEST));

        sleep(Duration::from_millis(20)).await;

        // The rate limiter for this fid is evicted because the tti is 10ms and the rate limits are freed up again.
        assert!(rate_limits.consume_for_fid(engine.shard_id(), FID_FOR_TEST));
    }

    #[tokio::test]
    async fn test_exceed_cache_size_limit() {
        let (mut engine, shard_stores) = setup(Limits {
            casts: 1000,
            links: 1000,
            reactions: 1000,
            user_data: 1000,
            user_name_proofs: 1000,
            verifications: 1000,
        });

        for fid in FID_FOR_TEST..FID_FOR_TEST + 11 {
            register_user(
                fid,
                test_helper::default_signer(),
                test_helper::default_custody_address(),
                &mut engine,
            )
            .await;
        }

        let mut rate_limits = RateLimits::new(
            shard_stores,
            RateLimitsConfig {
                time_to_idle: Duration::from_secs(1),
                max_capacity: 10,
            },
            statsd_client(),
        );

        for fid in FID_FOR_TEST..FID_FOR_TEST + 11 {
            for _ in 0..600 {
                assert!(rate_limits.consume_for_fid(engine.shard_id(), fid))
            }
        }

        // FID_FOR_TEST was LRU so it should be removed
        assert!(rate_limits.consume_for_fid(engine.shard_id(), FID_FOR_TEST));
        for fid in FID_FOR_TEST + 1..FID_FOR_TEST + 11 {
            assert!(!rate_limits.consume_for_fid(engine.shard_id(), fid));
        }
    }

    #[tokio::test]
    async fn test_small_limits() {
        let (mut engine, shard_stores) = setup(Limits {
            casts: 1,
            links: 1,
            reactions: 1,
            user_data: 1,
            user_name_proofs: 1,
            verifications: 1,
        });

        register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;

        let mut rate_limits = RateLimits::new(
            shard_stores,
            RateLimitsConfig {
                time_to_idle: Duration::from_secs(1),
                max_capacity: 10,
            },
            statsd_client(),
        );

        // Min allowance is 100
        for _ in 0..100 {
            assert!(rate_limits.consume_for_fid(engine.shard_id(), FID_FOR_TEST))
        }
    }

    #[tokio::test]
    async fn test_zero_storage_allowance() {
        let (mut engine, shard_stores) = setup(limits::zero());

        register_user(
            FID_FOR_TEST,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;

        let mut rate_limits = RateLimits::new(
            shard_stores,
            RateLimitsConfig {
                time_to_idle: Duration::from_secs(1),
                max_capacity: 10,
            },
            statsd_client(),
        );

        // If allowance is 0, don't allow any messages. This more realistically happens when the user has no storage.
        assert!(!rate_limits.consume_for_fid(engine.shard_id(), FID_FOR_TEST))
    }
}
