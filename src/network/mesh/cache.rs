//! Short-TTL cache in front of the mesh view / topology endpoints.
//!
//! The topology crawl fans a libp2p request out to every connected validator
//! with a 3s per-peer timeout ([`crate::network::mesh::crawl`]); the local view
//! snapshots gossipsub state. Neither is free, and the `/v1/mesh` HTTP endpoint
//! (plus the browser UI polling it) can produce many concurrent requests. This
//! cache collapses them: within the TTL, repeated requests return the cached
//! proto, and — crucially — concurrent *misses* for the same key share a single
//! in-flight computation (single-flight) rather than each launching a crawl.
//!
//! Single-flight is provided by moka's [`try_get_with`], which guarantees
//! exactly one init future runs per key while other callers await it. Errors are
//! not cached, so a transient crawl failure isn't pinned for the whole TTL.
//!
//! **The cache never gates access.** Admin authentication happens in the gRPC
//! handler *before* the cache is consulted, so a cached value is only ever
//! returned to a caller that has already passed the auth check.
//!
//! [`try_get_with`]: moka::future::Cache::try_get_with

use crate::proto;
use moka::future::Cache;
use std::future::Future;
use std::time::Duration;
use tonic::Status;

/// Caches mesh view and topology proto responses, keyed on `validators_only`.
///
/// Cheap to `clone` (both inner caches are `Arc`-backed). A TTL of
/// [`Duration::ZERO`] disables caching entirely — every call runs its init.
#[derive(Clone)]
pub struct MeshCache {
    views: Option<Cache<bool, proto::MeshView>>,
    topologies: Option<Cache<bool, proto::MeshTopology>>,
}

impl MeshCache {
    pub fn new(ttl: Duration) -> Self {
        if ttl.is_zero() {
            return Self::disabled();
        }
        Self {
            // Key space is a single bool, so a tiny capacity is plenty.
            views: Some(Cache::builder().max_capacity(4).time_to_live(ttl).build()),
            topologies: Some(Cache::builder().max_capacity(4).time_to_live(ttl).build()),
        }
    }

    /// A cache that always runs its init (no caching, no single-flight).
    pub fn disabled() -> Self {
        Self {
            views: None,
            topologies: None,
        }
    }

    /// Return the cached local mesh view for `validators_only`, or run `init` on
    /// a miss. Concurrent misses for the same key share one `init`.
    pub async fn view<F, Fut>(
        &self,
        validators_only: bool,
        init: F,
    ) -> Result<proto::MeshView, Status>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<proto::MeshView, Status>>,
    {
        match &self.views {
            None => init().await,
            Some(cache) => cache
                .try_get_with(validators_only, init())
                .await
                .map_err(status_from_arc),
        }
    }

    /// Return the cached topology for `validators_only`, or run `init` on a miss.
    /// Concurrent misses for the same key share one `init` — so a burst of
    /// requests triggers a single crawl.
    pub async fn topology<F, Fut>(
        &self,
        validators_only: bool,
        init: F,
    ) -> Result<proto::MeshTopology, Status>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<proto::MeshTopology, Status>>,
    {
        match &self.topologies {
            None => init().await,
            Some(cache) => cache
                .try_get_with(validators_only, init())
                .await
                .map_err(status_from_arc),
        }
    }
}

/// `try_get_with` hands back `Arc<Status>` (the error is shared with any other
/// waiters). `Status` isn't `Clone`, so rebuild an equivalent one.
fn status_from_arc(err: std::sync::Arc<Status>) -> Status {
    Status::new(err.code(), err.message().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::Notify;

    fn empty_view() -> proto::MeshView {
        proto::MeshView {
            local: None,
            peers: vec![],
            generated_at: 0,
        }
    }

    #[tokio::test]
    async fn caches_within_ttl() {
        let cache = MeshCache::new(Duration::from_secs(60));
        let calls = Arc::new(AtomicUsize::new(0));

        for _ in 0..3 {
            let calls = calls.clone();
            let v = cache
                .view(true, || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(empty_view())
                })
                .await
                .unwrap();
            assert_eq!(v.generated_at, 0);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn distinct_keys_do_not_share() {
        let cache = MeshCache::new(Duration::from_secs(60));
        let calls = Arc::new(AtomicUsize::new(0));

        for validators_only in [true, false, true, false] {
            let calls = calls.clone();
            cache
                .view(validators_only, || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(empty_view())
                })
                .await
                .unwrap();
        }
        // One init per distinct key.
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn expires_after_ttl() {
        let cache = MeshCache::new(Duration::from_millis(50));
        let calls = Arc::new(AtomicUsize::new(0));

        let run = || {
            let calls = calls.clone();
            let cache = cache.clone();
            async move {
                cache
                    .view(true, || async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(empty_view())
                    })
                    .await
                    .unwrap();
            }
        };

        run().await;
        tokio::time::sleep(Duration::from_millis(120)).await;
        run().await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn single_flight_collapses_concurrent_misses() {
        let cache = MeshCache::new(Duration::from_secs(60));
        let calls = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Notify::new());

        let mut handles = Vec::new();
        for _ in 0..10 {
            let cache = cache.clone();
            let calls = calls.clone();
            let gate = gate.clone();
            handles.push(tokio::spawn(async move {
                cache
                    .view(true, || async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        // Hold the single in-flight init open until released, so
                        // all 10 callers are guaranteed to be waiting on it.
                        gate.notified().await;
                        Ok(empty_view())
                    })
                    .await
                    .unwrap();
            }));
        }

        // Give the tasks a moment to converge on the one init, then release it.
        tokio::time::sleep(Duration::from_millis(50)).await;
        gate.notify_waiters();
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn errors_are_not_cached() {
        let cache = MeshCache::new(Duration::from_secs(60));
        let calls = Arc::new(AtomicUsize::new(0));

        let first = cache
            .view(true, || {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err(Status::internal("boom"))
                }
            })
            .await;
        assert!(first.is_err());

        let second = cache
            .view(true, || {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(empty_view())
                }
            })
            .await;
        assert!(second.is_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn zero_ttl_disables_cache() {
        let cache = MeshCache::new(Duration::ZERO);
        let calls = Arc::new(AtomicUsize::new(0));

        for _ in 0..2 {
            let calls = calls.clone();
            cache
                .view(true, || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(empty_view())
                })
                .await
                .unwrap();
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
