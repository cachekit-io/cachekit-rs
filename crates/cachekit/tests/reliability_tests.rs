//! Integration tests for the reliability tier (LAB-518): retry, circuit
//! breaker, and single-flight distributed-lock wiring.
//!
//! Run with:
//!   cargo test --test reliability_tests --features reliability

#![cfg(all(feature = "reliability", not(target_arch = "wasm32")))]

mod common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::common::MockBackend;
use cachekit::backend::{Backend, HealthStatus, LockableBackend};
use cachekit::client::SharedBackend;
use cachekit::error::{BackendError, BackendErrorKind};
use cachekit::reliability::{
    BackpressureConfig, CircuitBreakerConfig, ReliabilityConfig, RetryConfig,
};
use cachekit::{CacheKit, CachekitError};

// ── ScriptedBackend ──────────────────────────────────────────────────────────

/// Backend whose `get` fails `failures_before_success` times with a chosen
/// error kind, then returns a hit. Counts every backend call.
#[derive(Debug)]
struct ScriptedInner {
    calls: AtomicU32,
    failures_before_success: u32,
    kind: BackendErrorKind,
}

#[derive(Debug, Clone)]
struct ScriptedBackend {
    inner: Arc<ScriptedInner>,
}

impl ScriptedBackend {
    fn new_with_handle(
        failures_before_success: u32,
        kind: BackendErrorKind,
    ) -> (SharedBackend, Self) {
        let backend = Self {
            inner: Arc::new(ScriptedInner {
                calls: AtomicU32::new(0),
                failures_before_success,
                kind,
            }),
        };
        let handle = backend.clone();
        #[cfg(not(feature = "unsync"))]
        let shared: SharedBackend = Arc::new(backend);
        #[cfg(feature = "unsync")]
        let shared: SharedBackend = std::rc::Rc::new(backend);
        (shared, handle)
    }

    fn calls(&self) -> u32 {
        self.inner.calls.load(Ordering::SeqCst)
    }

    fn fail(&self) -> Result<Option<Vec<u8>>, BackendError> {
        match self.inner.kind {
            BackendErrorKind::Transient => Err(BackendError::transient("scripted failure")),
            BackendErrorKind::Timeout => Err(BackendError::timeout("scripted timeout")),
            BackendErrorKind::Authentication => Err(BackendError::auth("scripted auth")),
            _ => Err(BackendError::permanent("scripted failure")),
        }
    }
}

#[cfg_attr(not(feature = "unsync"), async_trait)]
#[cfg_attr(feature = "unsync", async_trait(?Send))]
impl Backend for ScriptedBackend {
    async fn get(&self, _key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        let n = self.inner.calls.fetch_add(1, Ordering::SeqCst);
        if n < self.inner.failures_before_success {
            self.fail()
        } else {
            // Any valid MessagePack payload: the msgpack encoding of 7u32.
            Ok(Some(rmp_encoded_seven()))
        }
    }

    async fn set(
        &self,
        _key: &str,
        _value: Vec<u8>,
        _ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        Ok(())
    }

    async fn delete(&self, _key: &str) -> Result<bool, BackendError> {
        Ok(false)
    }

    async fn exists(&self, _key: &str) -> Result<bool, BackendError> {
        Ok(false)
    }

    async fn health(&self) -> Result<HealthStatus, BackendError> {
        Ok(HealthStatus {
            is_healthy: true,
            latency_ms: 0.0,
            backend_type: "scripted".to_owned(),
            details: HashMap::new(),
        })
    }
}

/// MessagePack for the integer 7 — a decodable `u32` payload.
fn rmp_encoded_seven() -> Vec<u8> {
    vec![0x07]
}

// ── Config helpers ───────────────────────────────────────────────────────────

fn fast_retry(max_attempts: u32) -> RetryConfig {
    RetryConfig {
        max_attempts,
        base_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(5),
        jitter: false,
    }
}

fn client_with(backend: SharedBackend, config: ReliabilityConfig) -> CacheKit {
    CacheKit::builder()
        .backend(backend)
        .reliability(config)
        .no_l1()
        .build()
        .expect("client builds")
}

fn backend_kind(err: &CachekitError) -> Option<&BackendErrorKind> {
    match err {
        CachekitError::Backend(b) => Some(&b.kind),
        _ => None,
    }
}

// ── Retry ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn retry_recovers_from_transient_failures() {
    let (shared, handle) = ScriptedBackend::new_with_handle(2, BackendErrorKind::Transient);
    let client = client_with(
        shared,
        ReliabilityConfig {
            retry: Some(fast_retry(3)),
            circuit_breaker: None,
            backpressure: None,
        },
    );

    let value: u32 = client
        .get("k")
        .await
        .expect("third attempt succeeds")
        .expect("value present");
    assert_eq!(value, 7);
    assert_eq!(handle.calls(), 3, "two transient failures then a success");
}

#[tokio::test]
async fn retry_recovers_from_timeouts() {
    let (shared, handle) = ScriptedBackend::new_with_handle(1, BackendErrorKind::Timeout);
    let client = client_with(
        shared,
        ReliabilityConfig {
            retry: Some(fast_retry(3)),
            circuit_breaker: None,
            backpressure: None,
        },
    );

    let value: Option<u32> = client.get("k").await.expect("retry covers the timeout");
    assert_eq!(value, Some(7));
    assert_eq!(handle.calls(), 2);
}

#[tokio::test]
async fn retry_does_not_touch_permanent_errors() {
    let (shared, handle) = ScriptedBackend::new_with_handle(u32::MAX, BackendErrorKind::Permanent);
    let client = client_with(
        shared,
        ReliabilityConfig {
            retry: Some(fast_retry(3)),
            circuit_breaker: None,
            backpressure: None,
        },
    );

    let err = client
        .get::<u32>("k")
        .await
        .expect_err("permanent error propagates");
    assert_eq!(backend_kind(&err), Some(&BackendErrorKind::Permanent));
    assert_eq!(handle.calls(), 1, "permanent errors are never retried");
}

#[tokio::test]
async fn retry_exhausts_attempts_then_propagates() {
    let (shared, handle) = ScriptedBackend::new_with_handle(u32::MAX, BackendErrorKind::Transient);
    let client = client_with(
        shared,
        ReliabilityConfig {
            retry: Some(fast_retry(3)),
            circuit_breaker: None,
            backpressure: None,
        },
    );

    let err = client.get::<u32>("k").await.expect_err("all attempts fail");
    assert_eq!(backend_kind(&err), Some(&BackendErrorKind::Transient));
    assert_eq!(handle.calls(), 3, "exactly max_attempts calls");
}

// ── Circuit breaker ──────────────────────────────────────────────────────────

#[tokio::test]
async fn breaker_opens_and_fails_fast_without_touching_backend() {
    let (shared, handle) = ScriptedBackend::new_with_handle(u32::MAX, BackendErrorKind::Transient);
    let client = client_with(
        shared,
        ReliabilityConfig {
            retry: None,
            circuit_breaker: Some(CircuitBreakerConfig {
                failure_threshold: 2,
                open_timeout: Duration::from_secs(60),
                ..CircuitBreakerConfig::default()
            }),
            backpressure: None,
        },
    );

    for _ in 0..2 {
        let err = client.get::<u32>("k").await.expect_err("backend failing");
        assert_eq!(backend_kind(&err), Some(&BackendErrorKind::Transient));
    }
    assert_eq!(handle.calls(), 2);

    let err = client.get::<u32>("k").await.expect_err("circuit is open");
    assert_eq!(backend_kind(&err), Some(&BackendErrorKind::CircuitOpen));
    assert_eq!(handle.calls(), 2, "open circuit never reaches the backend");
}

#[tokio::test]
async fn breaker_half_open_probe_recovers() {
    // One failure opens the circuit; after open_timeout a probe succeeds and
    // (success_threshold: 1) closes it again.
    let (shared, handle) = ScriptedBackend::new_with_handle(1, BackendErrorKind::Transient);
    let client = client_with(
        shared,
        ReliabilityConfig {
            retry: None,
            circuit_breaker: Some(CircuitBreakerConfig {
                failure_threshold: 1,
                success_threshold: 1,
                open_timeout: Duration::from_millis(50),
                half_open_max_calls: 3,
                rolling_window: Duration::from_secs(60),
            }),
            backpressure: None,
        },
    );

    client
        .get::<u32>("k")
        .await
        .expect_err("first call fails and opens");
    let err = client.get::<u32>("k").await.expect_err("open: fails fast");
    assert_eq!(backend_kind(&err), Some(&BackendErrorKind::CircuitOpen));
    assert_eq!(handle.calls(), 1);

    tokio::time::sleep(Duration::from_millis(60)).await;

    let value: Option<u32> = client.get("k").await.expect("half-open probe passes");
    assert_eq!(value, Some(7));
    let value: Option<u32> = client.get("k").await.expect("circuit closed again");
    assert_eq!(value, Some(7));
    assert_eq!(handle.calls(), 3);
}

#[tokio::test]
async fn breaker_counts_one_failure_per_exhausted_retry_sequence() {
    // retry(2) inside breaker(threshold 2): two client calls = 4 backend
    // attempts but only 2 breaker failures — the third call fails fast.
    let (shared, handle) = ScriptedBackend::new_with_handle(u32::MAX, BackendErrorKind::Transient);
    let client = client_with(
        shared,
        ReliabilityConfig {
            retry: Some(fast_retry(2)),
            circuit_breaker: Some(CircuitBreakerConfig {
                failure_threshold: 2,
                open_timeout: Duration::from_secs(60),
                ..CircuitBreakerConfig::default()
            }),
            backpressure: None,
        },
    );

    for _ in 0..2 {
        client.get::<u32>("k").await.expect_err("backend failing");
    }
    assert_eq!(handle.calls(), 4, "2 calls x 2 attempts");

    let err = client.get::<u32>("k").await.expect_err("circuit open");
    assert_eq!(backend_kind(&err), Some(&BackendErrorKind::CircuitOpen));
    assert_eq!(handle.calls(), 4);
}

// ── Single-flight: in-process ────────────────────────────────────────────────

#[tokio::test]
async fn single_flight_leader_never_waits() {
    let client = CacheKit::builder()
        .backend(MockBackend::shared())
        .no_l1()
        .build()
        .expect("client builds");

    let mut flight = client.single_flight("cold").await;
    assert!(
        !flight.wait_for_fill().await,
        "an uncontested leader computes immediately — a re-check would be a second billable miss"
    );
    flight.release().await;
}

#[tokio::test]
async fn single_flight_follower_rechecks_once() {
    let client = Arc::new(
        CacheKit::builder()
            .backend(MockBackend::shared())
            .no_l1()
            .build()
            .expect("client builds"),
    );

    let leader = client.single_flight("cold").await;

    let follower_client = Arc::clone(&client);
    let follower = tokio::spawn(async move {
        let mut flight = follower_client.single_flight("cold").await;
        let mut rechecks = 0;
        while flight.wait_for_fill().await {
            rechecks += 1;
        }
        flight.release().await;
        rechecks
    });

    // The follower must be parked behind the leader's in-process lock.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!follower.is_finished(), "follower waits for the leader");

    leader.release().await;
    let rechecks = follower.await.expect("follower task completes");
    assert_eq!(
        rechecks, 1,
        "a queued follower re-checks the cache exactly once"
    );
}

// ── Single-flight: distributed lock wiring ───────────────────────────────────

/// Lock-capable mock: records acquire/release calls; `grant` controls whether
/// the distributed lock is granted or contested.
#[derive(Debug)]
struct LockingInner {
    store: tokio::sync::Mutex<HashMap<String, Vec<u8>>>,
    grant: bool,
    acquires: AtomicU32,
    releases: AtomicU32,
}

#[derive(Debug, Clone)]
struct LockingBackend {
    inner: Arc<LockingInner>,
}

impl LockingBackend {
    fn new_with_handle(grant: bool) -> (SharedBackend, Self) {
        let backend = Self {
            inner: Arc::new(LockingInner {
                store: tokio::sync::Mutex::new(HashMap::new()),
                grant,
                acquires: AtomicU32::new(0),
                releases: AtomicU32::new(0),
            }),
        };
        let handle = backend.clone();
        #[cfg(not(feature = "unsync"))]
        let shared: SharedBackend = Arc::new(backend);
        #[cfg(feature = "unsync")]
        let shared: SharedBackend = std::rc::Rc::new(backend);
        (shared, handle)
    }
}

#[cfg_attr(not(feature = "unsync"), async_trait)]
#[cfg_attr(feature = "unsync", async_trait(?Send))]
impl Backend for LockingBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        Ok(self.inner.store.lock().await.get(key).cloned())
    }

    async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        _ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        self.inner.store.lock().await.insert(key.to_owned(), value);
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<bool, BackendError> {
        Ok(self.inner.store.lock().await.remove(key).is_some())
    }

    async fn exists(&self, key: &str) -> Result<bool, BackendError> {
        Ok(self.inner.store.lock().await.contains_key(key))
    }

    async fn health(&self) -> Result<HealthStatus, BackendError> {
        Ok(HealthStatus {
            is_healthy: true,
            latency_ms: 0.0,
            backend_type: "locking-mock".to_owned(),
            details: HashMap::new(),
        })
    }

    fn as_lockable(&self) -> Option<&dyn LockableBackend> {
        Some(self)
    }
}

#[cfg_attr(not(feature = "unsync"), async_trait)]
#[cfg_attr(feature = "unsync", async_trait(?Send))]
impl LockableBackend for LockingBackend {
    async fn acquire_lock(
        &self,
        _key: &str,
        _timeout_ms: u64,
    ) -> Result<Option<String>, BackendError> {
        self.inner.acquires.fetch_add(1, Ordering::SeqCst);
        Ok(self.inner.grant.then(|| "lock-1".to_owned()))
    }

    async fn release_lock(&self, _key: &str, lock_id: &str) -> Result<bool, BackendError> {
        assert_eq!(lock_id, "lock-1");
        self.inner.releases.fetch_add(1, Ordering::SeqCst);
        Ok(true)
    }
}

#[tokio::test]
async fn single_flight_leader_takes_and_releases_distributed_lock() {
    let (shared, handle) = LockingBackend::new_with_handle(true);
    let client = CacheKit::builder()
        .backend(shared)
        .no_l1()
        .build()
        .expect("client builds");

    let mut flight = client.single_flight("cold").await;
    assert!(!flight.wait_for_fill().await, "lock granted → leader");
    assert_eq!(handle.inner.acquires.load(Ordering::SeqCst), 1);

    flight.release().await;
    assert_eq!(
        handle.inner.releases.load(Ordering::SeqCst),
        1,
        "release() frees the distributed fill lock"
    );
}

#[tokio::test]
async fn single_flight_contested_lock_polls_and_finds_remote_fill() {
    let (shared, handle) = LockingBackend::new_with_handle(false);
    let client = CacheKit::builder()
        .backend(shared)
        .no_l1()
        .build()
        .expect("client builds");

    // Simulate another process completing its fill while we wait.
    handle
        .inner
        .store
        .lock()
        .await
        .insert("cold".to_owned(), rmp_encoded_seven());

    let mut flight = client.single_flight("cold").await;
    assert!(
        flight.wait_for_fill().await,
        "contested lock → poll for the other process's fill"
    );
    let value: Option<u32> = client.get("cold").await.expect("get succeeds");
    assert_eq!(value, Some(7), "remote fill is visible on re-check");
    flight.release().await;
    assert_eq!(
        handle.inner.releases.load(Ordering::SeqCst),
        0,
        "no lock held, nothing to release"
    );
}

// ── Cancel-safety (panel CRIT: probe slot must survive cancellation) ─────────

/// Scripted per call index: fail (opens the breaker), hang (the probe that
/// gets cancelled), then succeed.
#[derive(Debug, Clone, Default)]
struct HangOnceBackend {
    calls: Arc<AtomicU32>,
}

impl HangOnceBackend {
    fn shared() -> SharedBackend {
        #[cfg(not(feature = "unsync"))]
        {
            Arc::new(Self::default())
        }
        #[cfg(feature = "unsync")]
        {
            std::rc::Rc::new(Self::default())
        }
    }
}

#[cfg_attr(not(feature = "unsync"), async_trait)]
#[cfg_attr(feature = "unsync", async_trait(?Send))]
impl Backend for HangOnceBackend {
    async fn get(&self, _key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        match self.calls.fetch_add(1, Ordering::SeqCst) {
            0 => Err(BackendError::transient("opening failure")),
            1 => std::future::pending().await,
            _ => Ok(Some(rmp_encoded_seven())),
        }
    }

    async fn set(
        &self,
        _key: &str,
        _value: Vec<u8>,
        _ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        Ok(())
    }

    async fn delete(&self, _key: &str) -> Result<bool, BackendError> {
        Ok(false)
    }

    async fn exists(&self, _key: &str) -> Result<bool, BackendError> {
        Ok(false)
    }

    async fn health(&self) -> Result<HealthStatus, BackendError> {
        Ok(HealthStatus {
            is_healthy: true,
            latency_ms: 0.0,
            backend_type: "hang-once".to_owned(),
            details: HashMap::new(),
        })
    }
}

#[tokio::test]
async fn cancelled_probe_does_not_wedge_the_breaker() {
    // One probe slot, one success to close. The half-open probe hangs and the
    // caller times out — cancelling the guarded future mid-await. Without the
    // RAII permit releasing the slot on drop, that single slot leaks and the
    // breaker fast-fails CircuitOpen forever, even after the backend recovers.
    let client = client_with(
        HangOnceBackend::shared(),
        ReliabilityConfig {
            retry: None,
            circuit_breaker: Some(CircuitBreakerConfig {
                failure_threshold: 1,
                success_threshold: 1,
                open_timeout: Duration::from_millis(5),
                half_open_max_calls: 1,
                rolling_window: Duration::from_secs(60),
            }),
            backpressure: None,
        },
    );

    client
        .get::<u32>("k")
        .await
        .expect_err("first call opens the breaker");
    tokio::time::sleep(Duration::from_millis(10)).await; // → half-open

    let cancelled = tokio::time::timeout(Duration::from_millis(20), client.get::<u32>("k")).await;
    assert!(
        cancelled.is_err(),
        "the hanging probe must be cancelled by the timeout"
    );

    let value: Option<u32> = client
        .get("k")
        .await
        .expect("a fresh probe is admitted after the cancelled one released its slot");
    assert_eq!(
        value,
        Some(7),
        "breaker recovered instead of wedging half-open"
    );
}

// ── Backpressure (LAB-729) ───────────────────────────────────────────────────

fn bp(max_concurrent: usize, max_queue: usize, acquire_timeout: Duration) -> BackpressureConfig {
    BackpressureConfig {
        max_concurrent,
        max_queue,
        acquire_timeout,
    }
}

/// Backend that records how many `get`s are in flight at once.
#[derive(Debug, Default)]
struct ProbeInner {
    current: AtomicU32,
    max_seen: AtomicU32,
    calls: AtomicU32,
}

#[derive(Debug, Clone, Default)]
struct ConcurrencyProbeBackend {
    inner: Arc<ProbeInner>,
}

impl ConcurrencyProbeBackend {
    fn new_with_handle() -> (SharedBackend, Self) {
        let backend = Self::default();
        let handle = backend.clone();
        #[cfg(not(feature = "unsync"))]
        let shared: SharedBackend = Arc::new(backend);
        #[cfg(feature = "unsync")]
        let shared: SharedBackend = std::rc::Rc::new(backend);
        (shared, handle)
    }
}

#[cfg_attr(not(feature = "unsync"), async_trait)]
#[cfg_attr(feature = "unsync", async_trait(?Send))]
impl Backend for ConcurrencyProbeBackend {
    async fn get(&self, _key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        let now = self.inner.current.fetch_add(1, Ordering::SeqCst) + 1;
        self.inner.max_seen.fetch_max(now, Ordering::SeqCst);
        self.inner.calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(20)).await;
        self.inner.current.fetch_sub(1, Ordering::SeqCst);
        Ok(Some(rmp_encoded_seven()))
    }

    async fn set(
        &self,
        _key: &str,
        _value: Vec<u8>,
        _ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        Ok(())
    }

    async fn delete(&self, _key: &str) -> Result<bool, BackendError> {
        Ok(false)
    }

    async fn exists(&self, _key: &str) -> Result<bool, BackendError> {
        Ok(false)
    }

    async fn health(&self) -> Result<HealthStatus, BackendError> {
        Ok(HealthStatus {
            is_healthy: true,
            latency_ms: 0.0,
            backend_type: "concurrency-probe".to_owned(),
            details: HashMap::new(),
        })
    }
}

/// Backend whose `get` blocks until the test releases it (one release = one
/// completed call). Counts calls that actually reach the backend.
#[derive(Debug)]
struct GateInner {
    gate: tokio::sync::Semaphore,
    calls: AtomicU32,
}

#[derive(Debug, Clone)]
struct GateBackend {
    inner: Arc<GateInner>,
}

impl GateBackend {
    fn new_with_handle() -> (SharedBackend, Self) {
        let backend = Self {
            inner: Arc::new(GateInner {
                gate: tokio::sync::Semaphore::new(0),
                calls: AtomicU32::new(0),
            }),
        };
        let handle = backend.clone();
        #[cfg(not(feature = "unsync"))]
        let shared: SharedBackend = Arc::new(backend);
        #[cfg(feature = "unsync")]
        let shared: SharedBackend = std::rc::Rc::new(backend);
        (shared, handle)
    }

    fn release(&self, n: usize) {
        self.inner.gate.add_permits(n);
    }

    fn calls(&self) -> u32 {
        self.inner.calls.load(Ordering::SeqCst)
    }
}

#[cfg_attr(not(feature = "unsync"), async_trait)]
#[cfg_attr(feature = "unsync", async_trait(?Send))]
impl Backend for GateBackend {
    async fn get(&self, _key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        self.inner.calls.fetch_add(1, Ordering::SeqCst);
        match self.inner.gate.acquire().await {
            Ok(permit) => {
                permit.forget();
                Ok(Some(rmp_encoded_seven()))
            }
            Err(_) => Err(BackendError::permanent("gate closed")),
        }
    }

    async fn set(
        &self,
        _key: &str,
        _value: Vec<u8>,
        _ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        Ok(())
    }

    async fn delete(&self, _key: &str) -> Result<bool, BackendError> {
        Ok(false)
    }

    async fn exists(&self, _key: &str) -> Result<bool, BackendError> {
        Ok(false)
    }

    async fn health(&self) -> Result<HealthStatus, BackendError> {
        Ok(HealthStatus {
            is_healthy: true,
            latency_ms: 0.0,
            backend_type: "gate".to_owned(),
            details: HashMap::new(),
        })
    }
}

#[tokio::test]
async fn backpressure_caps_concurrent_backend_ops() {
    // AC (LAB-729): with the cap at K, no more than K backend ops are ever in
    // flight under a burst of ≫K concurrent callers — and nobody is shed as
    // long as the waiting queue and timeout absorb the burst.
    let (shared, handle) = ConcurrencyProbeBackend::new_with_handle();
    let client = Arc::new(client_with(
        shared,
        ReliabilityConfig {
            retry: None,
            circuit_breaker: None,
            backpressure: Some(bp(4, 1000, Duration::from_secs(5))),
        },
    ));

    let tasks: Vec<_> = (0..32)
        .map(|_| {
            let client = Arc::clone(&client);
            tokio::spawn(async move { client.get::<u32>("k").await })
        })
        .collect();
    for task in tasks {
        let value = task.await.expect("task").expect("get succeeds");
        assert_eq!(value, Some(7));
    }

    assert_eq!(handle.inner.calls.load(Ordering::SeqCst), 32);
    let max_seen = handle.inner.max_seen.load(Ordering::SeqCst);
    assert!(max_seen <= 4, "cap 4 exceeded: {max_seen} ops in flight");
}

#[tokio::test]
async fn backpressure_sheds_immediately_when_queue_disabled() {
    // max_queue: 0 → a saturated limiter sheds on the spot with the typed
    // Backpressure error, without waiting out acquire_timeout and without
    // the call ever reaching the backend. Also the regression test for the
    // builder gating: a backpressure-ONLY config must still wrap the backend.
    let (shared, handle) = GateBackend::new_with_handle();
    let client = Arc::new(client_with(
        shared,
        ReliabilityConfig {
            retry: None,
            circuit_breaker: None,
            backpressure: Some(bp(1, 0, Duration::from_secs(5))),
        },
    ));

    let holder = {
        let client = Arc::clone(&client);
        tokio::spawn(async move { client.get::<u32>("k").await })
    };
    // Let the holder reach the backend and park on the gate.
    while handle.calls() == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let start = std::time::Instant::now();
    let err = client
        .get::<u32>("k")
        .await
        .expect_err("saturated limiter with no queue sheds");
    assert_eq!(backend_kind(&err), Some(&BackendErrorKind::Backpressure));
    assert!(
        start.elapsed() < Duration::from_millis(500),
        "queue-full sheds immediately, not after the 5 s acquire_timeout"
    );
    assert_eq!(handle.calls(), 1, "the shed call never reached the backend");

    handle.release(1);
    let value = holder.await.expect("task").expect("holder completes");
    assert_eq!(value, Some(7));
}

#[tokio::test]
async fn backpressure_times_out_waiting_then_sheds() {
    // A queued caller waits acquire_timeout for a permit, then is shed with
    // the typed Backpressure error (bounded wait, never an unbounded queue).
    let (shared, handle) = GateBackend::new_with_handle();
    let client = Arc::new(client_with(
        shared,
        ReliabilityConfig {
            retry: None,
            circuit_breaker: None,
            backpressure: Some(bp(1, 10, Duration::from_millis(50))),
        },
    ));

    let holder = {
        let client = Arc::clone(&client);
        tokio::spawn(async move { client.get::<u32>("k").await })
    };
    while handle.calls() == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let start = std::time::Instant::now();
    let err = client
        .get::<u32>("k")
        .await
        .expect_err("no permit frees up within acquire_timeout");
    assert_eq!(backend_kind(&err), Some(&BackendErrorKind::Backpressure));
    assert!(
        start.elapsed() >= Duration::from_millis(40),
        "the caller joins the queue and waits out acquire_timeout"
    );
    assert_eq!(handle.calls(), 1, "the shed call never reached the backend");

    handle.release(1);
    holder.await.expect("task").expect("holder completes");
}

#[tokio::test]
async fn backpressure_rejections_do_not_open_the_breaker() {
    // Shed calls are caller-side overload, not backend-health signals: with
    // failure_threshold: 1, a single counted failure would open the circuit —
    // so the success after the sheds proves they never touched the breaker.
    let (shared, handle) = GateBackend::new_with_handle();
    let client = Arc::new(client_with(
        shared,
        ReliabilityConfig {
            retry: None,
            circuit_breaker: Some(CircuitBreakerConfig {
                failure_threshold: 1,
                open_timeout: Duration::from_secs(60),
                ..CircuitBreakerConfig::default()
            }),
            backpressure: Some(bp(1, 0, Duration::from_secs(5))),
        },
    ));

    let holder = {
        let client = Arc::clone(&client);
        tokio::spawn(async move { client.get::<u32>("k").await })
    };
    while handle.calls() == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    for _ in 0..3 {
        let err = client
            .get::<u32>("k")
            .await
            .expect_err("shed while saturated");
        assert_eq!(backend_kind(&err), Some(&BackendErrorKind::Backpressure));
    }

    handle.release(1);
    holder.await.expect("task").expect("holder completes");

    // Were sheds counted as breaker failures, this would be CircuitOpen.
    handle.release(1);
    let value = client
        .get::<u32>("k")
        .await
        .expect("breaker stayed closed through the sheds");
    assert_eq!(value, Some(7));
    assert_eq!(handle.calls(), 2, "sheds never reached the backend");
}

#[tokio::test]
async fn backpressure_composes_with_retry_without_deadlock() {
    // The permit is held across the whole retry sequence (backoff included).
    // With the cap at 1 and two concurrent callers, the second caller queues
    // behind the first's full retry sequence and both complete — permits
    // held across backoff cannot deadlock because a permit holder never
    // re-enters the limiter.
    let (shared, handle) = ScriptedBackend::new_with_handle(2, BackendErrorKind::Transient);
    let client = Arc::new(client_with(
        shared,
        ReliabilityConfig {
            retry: Some(fast_retry(3)),
            circuit_breaker: None,
            backpressure: Some(bp(1, 10, Duration::from_secs(5))),
        },
    ));

    let a = {
        let client = Arc::clone(&client);
        tokio::spawn(async move { client.get::<u32>("k").await })
    };
    let b = {
        let client = Arc::clone(&client);
        tokio::spawn(async move { client.get::<u32>("k").await })
    };

    let a = a.await.expect("task a").expect("a succeeds");
    let b = b.await.expect("task b").expect("b succeeds");
    assert_eq!(a, Some(7));
    assert_eq!(b, Some(7));
    assert_eq!(
        handle.calls(),
        4,
        "one caller retries through 2 failures (3 attempts), the other hits once"
    );
}

#[tokio::test]
async fn backpressure_permit_released_after_error() {
    // A failed operation must release its permit: with the cap at 1, the
    // second sequential call would otherwise be shed instead of reaching the
    // backend for its own (transient) failure.
    let (shared, handle) = ScriptedBackend::new_with_handle(u32::MAX, BackendErrorKind::Transient);
    let client = client_with(
        shared,
        ReliabilityConfig {
            retry: None,
            circuit_breaker: None,
            backpressure: Some(bp(1, 0, Duration::from_millis(50))),
        },
    );

    for _ in 0..2 {
        let err = client.get::<u32>("k").await.expect_err("backend failing");
        assert_eq!(
            backend_kind(&err),
            Some(&BackendErrorKind::Transient),
            "a Backpressure kind here would mean the first call leaked its permit"
        );
    }
    assert_eq!(handle.calls(), 2);
}
