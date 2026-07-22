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

#[cachekit(client = cache, ttl = 120, interop = "get_user_namespaced", namespace = "ns")]
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
async fn macro_with_namespace() {
    let (cache, backend) = mock_client_counting();

    let user = get_user_namespaced(&cache, 7).await.unwrap();
    assert_eq!(user.name, "Namespaced 7");

    let user2 = get_user_namespaced(&cache, 7).await.unwrap();
    assert_eq!(user2, user);
    assert_eq!(backend.sets(), 1, "second call should be cached");
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

    let keys: Vec<String> = backend.inner.store.lock().await.keys().cloned().collect();
    // Independently verified (Python): canonical args msgpack [42] = 0x912a;
    // blake2b-256(0x912a) = 6159...8875.
    assert_eq!(
        keys,
        vec![
            "ns:get_user_namespaced:61598716255080080f6456eb065c2e51badfaa4320b0efe97469c29cffee8875"
                .to_owned()
        ]
    );
}

#[tokio::test]
async fn macro_key_delegates_to_interop_key() {
    // The macro must mint EXACTLY interop_key(namespace, fn_name, args) —
    // this delegation is what makes the 48 protocol interop vectors
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
