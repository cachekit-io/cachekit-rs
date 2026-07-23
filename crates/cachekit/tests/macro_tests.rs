//! Integration tests for the #[cachekit] proc-macro.
//!
//! Run with:
//!   cargo test --test macro_tests --features macros,l1

#![cfg(feature = "macros")]

mod common;

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use cachekit::backend::{Backend, HealthStatus};
use cachekit::client::SharedBackend;
use cachekit::error::BackendError;
use cachekit::interop::{interop_key, InteropValue};
use cachekit::{cachekit, CacheKit, CachekitError};

// ── CountingBackend ──────────────────────────────────────────────────────────

/// Shared state for CountingBackend (Clone shares the same underlying data).
#[derive(Debug, Default)]
struct CountingInner {
    store: Mutex<HashMap<String, Vec<u8>>>,
    set_count: std::sync::atomic::AtomicU32,
}

/// In-memory backend that also counts how many get/set calls it receives.
#[derive(Debug, Default, Clone)]
struct CountingBackend {
    inner: std::sync::Arc<CountingInner>,
}

impl CountingBackend {
    fn new_with_handle() -> (SharedBackend, Self) {
        let backend = Self {
            inner: std::sync::Arc::new(CountingInner::default()),
        };
        let handle = backend.clone();
        #[cfg(not(any(target_arch = "wasm32", feature = "unsync")))]
        let shared: SharedBackend = std::sync::Arc::new(backend);
        #[cfg(any(target_arch = "wasm32", feature = "unsync"))]
        let shared: SharedBackend = std::rc::Rc::new(backend);
        (shared, handle)
    }

    fn sets(&self) -> u32 {
        self.inner
            .set_count
            .load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg_attr(not(any(target_arch = "wasm32", feature = "unsync")), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", feature = "unsync"), async_trait(?Send))]
impl Backend for CountingBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        Ok(self.inner.store.lock().await.get(key).cloned())
    }

    async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        _ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        self.inner
            .set_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
            backend_type: "mock".to_owned(),
            details: HashMap::new(),
        })
    }
}

// ── Test types ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct User {
    name: String,
}

// ── Decorated functions ──────────────────────────────────────────────────────

#[cachekit(client = cache, ttl = 60, interop = "get_user", namespace = "users")]
async fn get_user(cache: &CacheKit, id: u64) -> Result<User, CachekitError> {
    Ok(User {
        name: format!("User {id}"),
    })
}

#[cachekit(client = cache, ttl = 120, interop = "users.fetch_by_id", namespace = "ns")]
async fn get_user_namespaced(cache: &CacheKit, id: u64) -> Result<User, CachekitError> {
    Ok(User {
        name: format!("Namespaced {id}"),
    })
}

#[cachekit(client = cache, ttl = 60, interop = "get_user_multi_args", namespace = "orgs")]
async fn get_user_multi_args(
    cache: &CacheKit,
    org: String,
    id: u64,
) -> Result<User, CachekitError> {
    Ok(User {
        name: format!("{org}/{id}"),
    })
}

#[cachekit(client = cache, ttl = 60, interop = "get_no_extra_args", namespace = "consts")]
async fn get_no_extra_args(cache: &CacheKit) -> Result<String, CachekitError> {
    Ok("constant".to_owned())
}

// ── Tests ────────────────────────────────────────────────────────────────────

/// Build a client with a CountingBackend and return both.
fn mock_client_counting() -> (CacheKit, CountingBackend) {
    let (shared, handle) = CountingBackend::new_with_handle();
    let client = CacheKit::builder()
        .backend(shared)
        .default_ttl(Duration::from_secs(300))
        .no_l1()
        .build()
        .expect("mock client builds");
    (client, handle)
}

#[tokio::test]
async fn macro_caches_result() {
    let (cache, backend) = mock_client_counting();

    let user1 = get_user(&cache, 42).await.unwrap();
    assert_eq!(user1.name, "User 42");
    assert_eq!(backend.sets(), 1, "first call should write to cache");

    let user2 = get_user(&cache, 42).await.unwrap();
    assert_eq!(user2, user1, "second call should return cached value");
    assert_eq!(backend.sets(), 1, "cache hit should NOT write again");
}

#[tokio::test]
async fn macro_different_args_different_keys() {
    let (cache, backend) = mock_client_counting();

    let u1 = get_user(&cache, 1).await.unwrap();
    let u2 = get_user(&cache, 2).await.unwrap();

    assert_ne!(
        u1, u2,
        "different args should produce different cache entries"
    );
    assert_eq!(
        backend.sets(),
        2,
        "each distinct arg set should write to cache"
    );
}

#[tokio::test]
async fn macro_multi_args() {
    let (cache, backend) = mock_client_counting();

    let u1 = get_user_multi_args(&cache, "acme".to_owned(), 1)
        .await
        .unwrap();
    assert_eq!(u1.name, "acme/1");

    // Same args -> cache hit
    let u2 = get_user_multi_args(&cache, "acme".to_owned(), 1)
        .await
        .unwrap();
    assert_eq!(u2, u1);
    assert_eq!(backend.sets(), 1, "same args should hit cache");

    // Different args -> cache miss
    let u3 = get_user_multi_args(&cache, "acme".to_owned(), 2)
        .await
        .unwrap();
    assert_eq!(u3.name, "acme/2");
    assert_eq!(backend.sets(), 2, "different args should miss cache");
}

#[tokio::test]
async fn macro_no_extra_args() {
    let (cache, backend) = mock_client_counting();

    let v1 = get_no_extra_args(&cache).await.unwrap();
    assert_eq!(v1, "constant");

    let v2 = get_no_extra_args(&cache).await.unwrap();
    assert_eq!(v2, "constant");
    assert_eq!(backend.sets(), 1, "no-args function should still cache");
}

#[tokio::test]
async fn macro_key_pinned_end_to_end() {
    // Byte-stability guard for the full key pipeline the macro emits.
    // Changing it invalidates every #[cachekit] user's cache (billed as
    // misses) AND breaks cross-SDK key identity — do not update this constant
    // without an explicit migration decision.
    let (cache, backend) = mock_client_counting();
    get_user_namespaced(&cache, 42).await.unwrap();

    // Key: operation comes from the `interop` attr ("users.fetch_by_id"),
    // NOT the fn name — this constant also trips a regression to fn-ident
    // keying. Independently verified (Python): canonical args msgpack [42]
    // = 0x912a; blake2b-256(0x912a) = 6159...8875.
    let key =
        "ns:users.fetch_by_id:61598716255080080f6456eb065c2e51badfaa4320b0efe97469c29cffee8875"; // pragma: allowlist secret
    let store = backend.inner.store.lock().await;
    let keys: Vec<&String> = store.keys().collect();
    assert_eq!(keys, vec![key]);

    // Value: plain MessagePack map, no envelope — the interop value format
    // other SDKs read. Independently verified (Python):
    // msgpack.packb({"name": "Namespaced 42"}).
    assert_eq!(
        store.get(key).unwrap(),
        &hex_bytes("81a46e616d65ad4e616d65737061636564203432") // pragma: allowlist secret
    );
}

fn hex_bytes(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect()
}

#[tokio::test]
async fn macro_self_heals_undecodable_entry() {
    // A stored entry that cannot decode as the return type (poisoned,
    // foreign-shaped, or a Python-internal CK frame) must be treated as a
    // miss and OVERWRITTEN — not brick the function until TTL expiry.
    let (cache, backend) = mock_client_counting();
    let key =
        "ns:users.fetch_by_id:61598716255080080f6456eb065c2e51badfaa4320b0efe97469c29cffee8875"; // pragma: allowlist secret
    backend
        .inner
        .store
        .lock()
        .await
        .insert(key.to_owned(), b"CK\x03garbage".to_vec());

    let user = get_user_namespaced(&cache, 42).await.unwrap();
    assert_eq!(user.name, "Namespaced 42");
    assert_eq!(
        backend.sets(),
        1,
        "fresh result must overwrite the poisoned entry"
    );

    let healed = get_user_namespaced(&cache, 42).await.unwrap();
    assert_eq!(healed, user);
    assert_eq!(backend.sets(), 1, "second call hits the healed entry");
}

#[tokio::test]
async fn macro_key_delegates_to_interop_key() {
    // The macro must mint EXACTLY interop_key(namespace, operation, args),
    // operation being the `interop` attribute — this delegation is what
    // makes the 48 protocol interop vectors
    // (interop_vector_tests.rs) transitively cover #[cachekit] keys, and
    // what makes the same entry addressable from the Python/TS SDKs.
    let (cache, backend) = mock_client_counting();
    get_user_multi_args(&cache, "acme".to_owned(), 7)
        .await
        .unwrap();

    let expected = interop_key(
        "orgs",
        "get_user_multi_args",
        &[InteropValue::from("acme"), InteropValue::from(7u64)],
    )
    .unwrap();
    let keys: Vec<String> = backend.inner.store.lock().await.keys().cloned().collect();
    assert_eq!(keys, vec![expected]);
}

// ── Reliability behaviour (LAB-518) ──────────────────────────────────────────

/// Backend where every data operation fails with a transient error.
#[derive(Debug, Default, Clone)]
struct DownBackend;

impl DownBackend {
    fn shared() -> SharedBackend {
        #[cfg(not(any(target_arch = "wasm32", feature = "unsync")))]
        {
            std::sync::Arc::new(Self)
        }
        #[cfg(any(target_arch = "wasm32", feature = "unsync"))]
        {
            std::rc::Rc::new(Self)
        }
    }
}

#[cfg_attr(not(any(target_arch = "wasm32", feature = "unsync")), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", feature = "unsync"), async_trait(?Send))]
impl Backend for DownBackend {
    async fn get(&self, _key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        Err(BackendError::transient("backend down"))
    }

    async fn set(
        &self,
        _key: &str,
        _value: Vec<u8>,
        _ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        Err(BackendError::transient("backend down"))
    }

    async fn delete(&self, _key: &str) -> Result<bool, BackendError> {
        Err(BackendError::transient("backend down"))
    }

    async fn exists(&self, _key: &str) -> Result<bool, BackendError> {
        Err(BackendError::transient("backend down"))
    }

    async fn health(&self) -> Result<HealthStatus, BackendError> {
        Err(BackendError::transient("backend down"))
    }
}

static FAIL_OPEN_RUNS: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

#[cachekit(client = cache, ttl = 60, interop = "fail_open_op", namespace = "reliab")]
async fn fail_open_op(cache: &CacheKit, id: u64) -> Result<User, CachekitError> {
    FAIL_OPEN_RUNS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    Ok(User {
        name: format!("degraded {id}"),
    })
}

#[tokio::test]
async fn macro_fails_open_when_backend_down() {
    let cache = CacheKit::builder()
        .backend(DownBackend::shared())
        .no_l1()
        .build()
        .expect("client builds");

    // Every call runs the function uncached — graceful degradation, matching
    // cachekit-py's BackendError → execute-without-caching posture.
    let user = fail_open_op(&cache, 1)
        .await
        .expect("fail-open: function runs uncached");
    assert_eq!(user.name, "degraded 1");
    let _ = fail_open_op(&cache, 1).await.expect("still degrading");
    assert_eq!(
        FAIL_OPEN_RUNS.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "nothing was cached while the backend was down"
    );
}

#[cfg(feature = "encryption")]
static SECURE_RUNS: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

#[cfg(feature = "encryption")]
#[cachekit(client = cache, ttl = 60, interop = "secure_op", namespace = "reliab", secure)]
async fn secure_op(cache: &CacheKit, id: u64) -> Result<User, CachekitError> {
    SECURE_RUNS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    Ok(User {
        name: format!("secret {id}"),
    })
}

#[cfg(feature = "encryption")]
#[tokio::test]
async fn macro_secure_fails_closed_when_backend_down() {
    let cache = CacheKit::builder()
        .backend(DownBackend::shared())
        .encryption_from_bytes(&[7u8; 32], "default")
        .expect("encryption configures")
        .no_l1()
        .build()
        .expect("client builds");

    // Encrypted paths never silently degrade: the backend error reaches the
    // caller and the wrapped function does NOT run.
    let err = secure_op(&cache, 1)
        .await
        .expect_err("fail-closed: error propagates");
    assert!(matches!(err, CachekitError::Backend(_)), "got: {err:?}");
    assert_eq!(
        SECURE_RUNS.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "secure path must not fail open into uncached execution"
    );
}

static SLOW_OP_RUNS: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

#[cachekit(client = cache, ttl = 60, interop = "slow_op", namespace = "flight")]
async fn slow_op(cache: &CacheKit, id: u64) -> Result<User, CachekitError> {
    SLOW_OP_RUNS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok(User {
        name: format!("slow {id}"),
    })
}

#[tokio::test]
async fn macro_single_flight_collapses_concurrent_misses() {
    let (cache, backend) = mock_client_counting();
    let cache = std::sync::Arc::new(cache);

    // Barrier-align the tasks so their initial cache checks all miss before
    // the leader finishes computing.
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(5));
    let tasks: Vec<_> = (0..5)
        .map(|_| {
            let cache = std::sync::Arc::clone(&cache);
            let barrier = std::sync::Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                slow_op(&cache, 9).await
            })
        })
        .collect();

    for task in tasks {
        let user = task.await.expect("task completes").expect("call succeeds");
        assert_eq!(user.name, "slow 9");
    }

    assert_eq!(
        SLOW_OP_RUNS.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "five concurrent misses must execute the function exactly once"
    );
    assert_eq!(backend.sets(), 1, "and write the cache exactly once");
}
