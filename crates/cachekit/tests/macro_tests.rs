//! Integration tests for the #[cachekit] proc-macro.
//!
//! Run with:
//!   cargo test --test macro_tests --features macros,l1

#![cfg(feature = "macros")]

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use cachekit::backend::{Backend, HealthStatus};
use cachekit::error::BackendError;
use cachekit::{cachekit, CacheKit, CachekitError};

// ── CountingBackend ──────────────────────────────────────────────────────────

/// In-memory backend that also counts how many get/set calls it receives.
#[derive(Debug, Default)]
struct CountingBackend {
    store: Mutex<HashMap<String, Vec<u8>>>,
    set_count: std::sync::atomic::AtomicU32,
}

impl CountingBackend {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn sets(&self) -> u32 {
        self.set_count.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[async_trait]
impl Backend for CountingBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        Ok(self.store.lock().await.get(key).cloned())
    }

    async fn set(&self, key: &str, value: Vec<u8>, _ttl: Option<Duration>) -> Result<(), BackendError> {
        self.set_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.store.lock().await.insert(key.to_owned(), value);
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<bool, BackendError> {
        Ok(self.store.lock().await.remove(key).is_some())
    }

    async fn exists(&self, key: &str) -> Result<bool, BackendError> {
        Ok(self.store.lock().await.contains_key(key))
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

#[cachekit(client = cache, ttl = 60)]
async fn get_user(cache: &CacheKit, id: u64) -> Result<User, CachekitError> {
    Ok(User {
        name: format!("User {id}"),
    })
}

#[cachekit(client = cache, ttl = 120, namespace = "ns")]
async fn get_user_namespaced(cache: &CacheKit, id: u64) -> Result<User, CachekitError> {
    Ok(User {
        name: format!("Namespaced {id}"),
    })
}

#[cachekit(client = cache, ttl = 60)]
async fn get_user_multi_args(cache: &CacheKit, org: String, id: u64) -> Result<User, CachekitError> {
    Ok(User {
        name: format!("{org}/{id}"),
    })
}

#[cachekit(client = cache, ttl = 60)]
async fn get_no_extra_args(cache: &CacheKit) -> Result<String, CachekitError> {
    Ok("constant".to_owned())
}

// ── Tests ────────────────────────────────────────────────────────────────────

/// Build a client with a CountingBackend and return both.
fn mock_client_counting() -> (CacheKit, Arc<CountingBackend>) {
    let backend = CountingBackend::new();
    let client = CacheKit::builder()
        .backend(backend.clone())
        .default_ttl(Duration::from_secs(300))
        .no_l1()
        .build()
        .expect("mock client builds");
    (client, backend)
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

    assert_ne!(u1, u2, "different args should produce different cache entries");
    assert_eq!(backend.sets(), 2, "each distinct arg set should write to cache");
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

    let u1 = get_user_multi_args(&cache, "acme".to_owned(), 1).await.unwrap();
    assert_eq!(u1.name, "acme/1");

    // Same args -> cache hit
    let u2 = get_user_multi_args(&cache, "acme".to_owned(), 1).await.unwrap();
    assert_eq!(u2, u1);
    assert_eq!(backend.sets(), 1, "same args should hit cache");

    // Different args -> cache miss
    let u3 = get_user_multi_args(&cache, "acme".to_owned(), 2).await.unwrap();
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
