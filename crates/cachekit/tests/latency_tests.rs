//! L1 hit latency: which calls a client times into its backend's report
//! (spec/saas-api.md, "L1 hit latency"). Each test names the requirement it
//! pins (API-NN). The wire side — headers, the switch, health checks and
//! retries — is tested against a stub server in `src/backend/cachekitio.rs`.
//!
//! Run with:
//!   cargo test --features macros --test latency_tests

#![cfg(all(feature = "l1", not(target_arch = "wasm32"), not(feature = "unsync")))]

mod common;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::common::MockBackend;
use cachekit::backend::{Backend, HealthStatus};
use cachekit::error::BackendError;
use cachekit::metrics::L1Latency;
use cachekit::{CacheKit, CachekitError, SwrRead};

/// A [`MockBackend`] that hands clients a report accumulator timing every
/// call (N = 1), as the cachekit.io backend does with telemetry on.
#[derive(Clone)]
struct ReportingBackend {
    store: MockBackend,
    latency: Arc<L1Latency>,
}

impl ReportingBackend {
    fn new() -> Self {
        Self {
            store: MockBackend::default(),
            latency: Arc::new(L1Latency::__new(1)),
        }
    }

    /// Timed hits in the report since the last call, which takes it.
    fn timed(&self) -> u64 {
        self.latency.__take().map_or(0, |[buckets, _, _]| {
            buckets
                .split(',')
                .map(|count| count.parse::<u64>().expect("bucket count"))
                .sum()
        })
    }

    fn client(&self) -> CacheKit {
        CacheKit::builder()
            .backend(Arc::new(self.clone()))
            .no_encryption()
            .build()
            .expect("client builds")
    }
}

#[async_trait]
impl Backend for ReportingBackend {
    fn l1_latency(&self) -> Option<Arc<L1Latency>> {
        Some(self.latency.clone())
    }
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        self.store.get(key).await
    }
    async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        self.store.set(key, value, ttl).await
    }
    async fn delete(&self, key: &str) -> Result<bool, BackendError> {
        self.store.delete(key).await
    }
    async fn exists(&self, key: &str) -> Result<bool, BackendError> {
        self.store.exists(key).await
    }
    async fn health(&self) -> Result<HealthStatus, BackendError> {
        self.store.health().await
    }
}

/// API-79: a read counted as an L1 hit that returns a value is timed; an L2
/// hit and a miss are not.
#[tokio::test]
async fn api_79_a_plain_read_is_timed_only_as_an_l1_hit() {
    let backend = ReportingBackend::new();
    let cache = backend.client();
    cache.set("k", &7u32).await.expect("set");

    assert_eq!(cache.get::<u32>("k").await.expect("get"), Some(7));
    assert_eq!(backend.timed(), 1, "an L1 hit");

    let cold = backend.client();
    assert_eq!(cold.get::<u32>("k").await.expect("get"), Some(7));
    assert_eq!(backend.timed(), 0, "an L2 hit");

    assert_eq!(cache.get::<u32>("absent").await.expect("get"), None);
    assert_eq!(backend.timed(), 0, "a miss");
    assert_eq!(cache.stats().l1_hits, 1);
}

/// API-79: every value read on the public API is timed as an L1 hit.
#[tokio::test]
async fn api_79_every_value_read_is_timed() {
    let backend = ReportingBackend::new();
    let cache = backend.client();
    cache.set("k", &"v".to_owned()).await.expect("set");

    let _: Option<String> = cache.interop_get("k").await.expect("interop_get");
    assert_eq!(backend.timed(), 1, "interop_get");
    let swr: SwrRead<String> = cache.interop_get_swr("k").await.expect("interop_get_swr");
    assert!(matches!(swr, SwrRead::Fresh(_)));
    assert_eq!(backend.timed(), 1, "interop_get_swr");
}

/// API-79: a read that L1 serves but that returns no value — its entry does
/// not decode as the type asked for — adds nothing, though it counts.
#[tokio::test]
async fn api_79_a_read_that_returns_no_value_is_not_timed() {
    let backend = ReportingBackend::new();
    let cache = backend.client();
    cache.set("k", &"text".to_owned()).await.expect("set");

    let err = cache.get::<u32>("k").await.expect_err("does not decode");
    assert!(matches!(err, CachekitError::Serialization(_)), "{err}");
    assert_eq!(backend.timed(), 0);
    assert_eq!(cache.stats().l1_hits, 1, "the read still counts");
}

/// API-79: an L1 stale serve is an L1 hit and is timed.
#[tokio::test]
async fn api_79_a_stale_serve_is_timed() {
    let backend = ReportingBackend::new();
    let cache = CacheKit::builder()
        .backend(Arc::new(backend.clone()))
        .no_encryption()
        // Stale from 0.25 × 4 s (±10%), hard expiry at 4 s.
        .swr_threshold_ratio(0.25)
        .build()
        .expect("client builds");
    cache
        .set_with_ttl("k", &1u32, Duration::from_secs(4))
        .await
        .expect("set");
    tokio::time::sleep(Duration::from_millis(1400)).await;

    let read: SwrRead<u32> = cache.interop_get_swr("k").await.expect("read");
    assert!(matches!(read, SwrRead::Stale(1, _)), "a stale serve");
    assert_eq!(backend.timed(), 1);
}

/// API-80: an existence check is not timed.
#[tokio::test]
async fn api_80_an_existence_check_is_not_timed() {
    let backend = ReportingBackend::new();
    let cache = backend.client();
    cache.set("k", &1u32).await.expect("set");

    assert!(cache.exists("k").await.expect("exists"));
    assert_eq!(backend.timed(), 0);
}

/// A client without an L1 has no L1 hit to time, and takes no report.
#[tokio::test]
async fn a_client_without_l1_times_nothing() {
    let backend = ReportingBackend::new();
    let cache = CacheKit::builder()
        .backend(Arc::new(backend.clone()))
        .no_encryption()
        .no_l1()
        .build()
        .expect("client builds");
    cache.set("k", &1u32).await.expect("set");
    for _ in 0..10 {
        assert_eq!(cache.get::<u32>("k").await.expect("get"), Some(1));
    }
    assert_eq!(backend.timed(), 0);
}

#[cfg(feature = "encryption")]
mod encrypted {
    use super::*;

    const KEY: [u8; 32] = [7; 32];

    fn client(backend: &ReportingBackend) -> CacheKit {
        CacheKit::builder()
            .backend(Arc::new(backend.clone()))
            .encryption_from_bytes(&KEY, "tenant")
            .expect("key")
            .build()
            .expect("client builds")
    }

    /// API-79: an encrypted L1 hit is timed through decryption, on the
    /// client's own reads and the `SecureCache` handle's, once per call.
    #[tokio::test]
    async fn api_79_encrypted_reads_are_timed_once() {
        let backend = ReportingBackend::new();
        let cache = client(&backend);
        cache.set("k", &"v".to_owned()).await.expect("set");

        let _: Option<String> = cache.get("k").await.expect("get");
        assert_eq!(backend.timed(), 1, "an encrypting client's get");
        let _: Option<String> = cache.interop_get("k").await.expect("interop_get");
        assert_eq!(backend.timed(), 1, "an encrypting client's interop_get");

        let secure = cache.secure_cache().expect("secure");
        let _: Option<String> = secure.get("k").await.expect("get");
        assert_eq!(backend.timed(), 1, "SecureCache::get");
        let _: Option<String> = secure.interop_get("k").await.expect("interop_get");
        assert_eq!(backend.timed(), 1, "SecureCache::interop_get");
        let _: SwrRead<String> = secure.interop_get_swr("k").await.expect("swr");
        assert_eq!(backend.timed(), 1, "SecureCache::interop_get_swr");
    }
}

#[cfg(feature = "macros")]
mod wrapped {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;
    use cachekit::cachekit;

    static RUNS: AtomicU32 = AtomicU32::new(0);

    #[cachekit(client = cache, ttl = 60, interop = "latency_op", namespace = "latency")]
    async fn op(cache: &CacheKit, id: u64) -> Result<u64, CachekitError> {
        RUNS.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(id * 2)
    }

    /// API-76 / API-79 / API-80: a wrapped call that runs the function is
    /// not timed; one served from L1 is timed once, its own reads adding
    /// nothing; one that joined another call's fill for the same key counts
    /// an L1 hit but is not timed.
    #[tokio::test]
    async fn api_80_wrapped_calls_time_l1_hits_but_never_a_joined_call() {
        let backend = ReportingBackend::new();
        let cache = Arc::new(backend.client());

        // Two concurrent cold calls: one runs the function, the other joins
        // its in-process fill and is served from L1 when it lands.
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let tasks: Vec<_> = (0..2)
            .map(|_| {
                let (cache, barrier) = (cache.clone(), barrier.clone());
                tokio::spawn(async move {
                    barrier.wait().await;
                    op(&cache, 21).await
                })
            })
            .collect();
        for task in tasks {
            assert_eq!(task.await.expect("task").expect("call"), 42);
        }
        assert_eq!(RUNS.load(Ordering::SeqCst), 1, "one call ran the function");
        let stats = cache.stats();
        assert_eq!(
            (stats.misses, stats.l1_hits),
            (1, 1),
            "the joined call counts an L1 hit"
        );
        assert_eq!(
            backend.timed(),
            0,
            "neither the miss nor the joined call is timed"
        );

        assert_eq!(op(&cache, 21).await.expect("call"), 42);
        assert_eq!(backend.timed(), 1, "an L1 hit, timed once");
    }
}
