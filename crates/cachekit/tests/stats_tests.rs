//! Live read statistics (LAB-521): `CacheKit::stats`, `l1_entry_count`, and
//! the telemetry provider the builder attaches to the backend.
//!
//! Run with:
//!   cargo test --test stats_tests

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use crate::common::{FailingBackend, MockBackend};
use cachekit::backend::{Backend, HealthStatus};
use cachekit::client::SharedBackend;
use cachekit::error::BackendError;
use cachekit::metrics::MetricsProvider;
use cachekit::{CacheKit, L1Stats};

// ── RecordingBackend ─────────────────────────────────────────────────────────

/// A `MockBackend` that also captures the provider the client attaches.
#[derive(Default, Clone)]
struct RecordingBackend {
    store: MockBackend,
    attached: Arc<Mutex<Option<MetricsProvider>>>,
}

impl RecordingBackend {
    fn new_with_handle() -> (SharedBackend, Self) {
        let backend = Self::default();
        let handle = backend.clone();
        #[cfg(not(feature = "unsync"))]
        let shared: SharedBackend = Arc::new(backend);
        #[cfg(feature = "unsync")]
        let shared: SharedBackend = std::rc::Rc::new(backend);
        (shared, handle)
    }

    fn attached(&self) -> MetricsProvider {
        self.attached
            .lock()
            .expect("attached lock")
            .clone()
            .expect("the client attached a provider at build time")
    }
}

#[cfg_attr(not(feature = "unsync"), async_trait)]
#[cfg_attr(feature = "unsync", async_trait(?Send))]
impl Backend for RecordingBackend {
    fn attach_metrics(&self, provider: MetricsProvider) {
        *self.attached.lock().expect("attached lock") = Some(provider);
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
        Ok(HealthStatus {
            is_healthy: true,
            latency_ms: 0.0,
            backend_type: "recording".to_owned(),
            details: HashMap::new(),
        })
    }
}

fn reads(stats: L1Stats) -> u64 {
    stats.l1_hits + stats.l2_hits + stats.misses
}

// ── stats() ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn stats_count_every_read_by_the_layer_that_served_it() {
    let (shared, _store) = MockBackend::new_with_handle();
    // The writer has no L1, so the reader's first hit is a genuine L2 hit.
    let writer = CacheKit::builder()
        .backend(shared.clone())
        .no_l1()
        .build()
        .expect("writer builds");
    let reader = CacheKit::builder()
        .backend(shared)
        .default_ttl(Duration::from_secs(60))
        .build()
        .expect("reader builds");

    assert_eq!(
        reader.stats(),
        L1Stats {
            l1_enabled: cfg!(feature = "l1"),
            ..L1Stats::default()
        },
        "fresh client: zero everywhere"
    );

    let miss: Option<u32> = reader.get("k").await.expect("get");
    assert_eq!(miss, None);
    writer.set("k", &7u32).await.expect("set");
    let hit: Option<u32> = reader.get("k").await.expect("get");
    assert_eq!(hit, Some(7));
    let hit: Option<u32> = reader.get("k").await.expect("get");
    assert_eq!(hit, Some(7));

    let stats = reader.stats();
    assert_eq!(stats.misses, 1);
    assert_eq!(reads(stats), 3);
    if cfg!(feature = "l1") {
        // miss → L2 hit (backfills L1) → L1 hit
        assert_eq!((stats.l2_hits, stats.l1_hits), (1, 1));
        assert_eq!(reader.l1_entry_count(), Some(1));
    } else {
        assert_eq!((stats.l2_hits, stats.l1_hits), (2, 0));
        assert_eq!(reader.l1_entry_count(), None);
    }
    assert_eq!(reads(writer.stats()), 0, "the writer never read");
    assert!(!writer.stats().l1_enabled);
    assert_eq!(writer.l1_entry_count(), None);
}

#[tokio::test]
async fn clones_share_one_set_of_counters() {
    let client = CacheKit::builder()
        .backend(MockBackend::shared())
        .build()
        .expect("client builds");
    let clone = client.clone();

    let _: Option<u32> = clone.get("k").await.expect("get");

    assert_eq!(client.stats().misses, 1);
    assert_eq!(client.stats(), clone.stats());
}

#[tokio::test]
async fn exists_is_not_a_read_but_a_served_read_that_fails_to_deserialize_is() {
    let client = CacheKit::builder()
        .backend(MockBackend::shared())
        .build()
        .expect("client builds");

    client.set("k", &1u32).await.expect("set");
    assert!(client.exists("k").await.expect("exists"));
    assert_eq!(reads(client.stats()), 0, "exists() is not a value read");

    // Wrong type: the cache served the bytes; deserialization failed after.
    client
        .get::<String>("k")
        .await
        .expect_err("u32 payload is not a String");
    assert_eq!(reads(client.stats()), 1, "{:?}", client.stats());
}

#[tokio::test]
async fn reads_that_fail_at_the_backend_are_not_counted() {
    let client = CacheKit::builder()
        .backend(FailingBackend::shared())
        .build()
        .expect("client builds");

    client.get::<u32>("k").await.expect_err("backend is down");

    assert_eq!(reads(client.stats()), 0, "{:?}", client.stats());
}

// ── attach_metrics wiring ────────────────────────────────────────────────────

#[tokio::test]
async fn builder_attaches_a_live_provider_over_the_client_counters() {
    let (shared, handle) = RecordingBackend::new_with_handle();
    let client = CacheKit::builder()
        .backend(shared)
        .build()
        .expect("client builds");
    let provider = handle.attached();

    assert_eq!(provider(), Some(client.stats()), "zero reads yet");

    let _: Option<u32> = client.get("k").await.expect("get");
    client.set("k", &1u32).await.expect("set");
    let _: Option<u32> = client.get("k").await.expect("get");

    let seen = provider().expect("provider reports a snapshot");
    assert_eq!(
        seen,
        client.stats(),
        "the provider is a live view, not a copy"
    );
    assert_eq!(seen.misses, 1);
    assert_eq!(reads(seen), 2);
    assert_eq!(seen.l1_enabled, cfg!(feature = "l1"));

    // The backend can outlive the client: once every clone is gone the
    // provider reports nothing, not the dead client's frozen numbers.
    let clone = client.clone();
    drop(client);
    assert!(
        provider().is_some(),
        "a live clone keeps the counters alive"
    );
    drop(clone);
    assert_eq!(provider(), None);
}

#[tokio::test]
async fn provider_reports_l1_disabled_for_a_no_l1_client() {
    let (shared, handle) = RecordingBackend::new_with_handle();
    let _client = CacheKit::builder()
        .backend(shared)
        .no_l1()
        .build()
        .expect("client builds");

    let snapshot = handle.attached()().expect("provider reports a snapshot");
    assert!(!snapshot.l1_enabled);
}

/// The reliability decorator does not forward `attach_metrics`; the builder
/// attaches to the raw backend before wrapping, so the SaaS backend inside a
/// `production`/`io` preset still gets the counters.
#[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
#[tokio::test]
async fn provider_reaches_the_raw_backend_under_the_reliability_stack() {
    let (shared, handle) = RecordingBackend::new_with_handle();
    let client = CacheKit::builder()
        .backend(shared)
        .reliability(cachekit::ReliabilityConfig::default())
        .build()
        .expect("client builds");

    let _: Option<u32> = client.get("k").await.expect("get");

    let seen = handle.attached()().expect("provider reports a snapshot");
    assert_eq!(seen.misses, 1);
    assert_eq!(seen, client.stats());
}
