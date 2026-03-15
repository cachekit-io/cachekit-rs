//! Integration tests for the CacheKit client.
//!
//! Run with:
//!   cargo test --test client_tests --features cachekitio,l1

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use cachekit::backend::{Backend, HealthStatus};
use cachekit::error::BackendError;
use cachekit::{CacheKit, CachekitError};

// ── MockBackend ───────────────────────────────────────────────────────────────

/// In-memory mock backend backed by a `Mutex<HashMap>` for use in tests.
#[derive(Debug, Default)]
struct MockBackend {
    store: Mutex<HashMap<String, Vec<u8>>>,
}

impl MockBackend {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

#[async_trait]
impl Backend for MockBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        Ok(self.store.lock().await.get(key).cloned())
    }

    async fn set(&self, key: &str, value: Vec<u8>, _ttl: Option<Duration>) -> Result<(), BackendError> {
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
            backend_type: "mock".to_owned(),
            details: HashMap::new(),
        })
    }
}

// ── Test fixtures ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct User {
    id: u32,
    name: String,
}

fn mock_client() -> CacheKit {
    CacheKit::builder()
        .backend(MockBackend::new())
        .default_ttl(Duration::from_secs(60))
        .no_l1()
        .build()
        .expect("mock client builds without error")
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn client_set_and_get() {
    let client = mock_client();
    let user = User { id: 1, name: "Alice".to_owned() };

    client.set("user:1", &user).await.expect("set should succeed");

    let retrieved: User = client
        .get("user:1")
        .await
        .expect("get should succeed")
        .expect("value should be present");

    assert_eq!(retrieved, user);
}

#[tokio::test]
async fn client_get_missing() {
    let client = mock_client();

    let result: Option<User> = client.get("nonexistent").await.expect("get should succeed");

    assert!(result.is_none(), "missing key should return None");
}

#[tokio::test]
async fn client_delete() {
    let client = mock_client();
    let user = User { id: 2, name: "Bob".to_owned() };

    client.set("user:2", &user).await.expect("set should succeed");

    let existed = client.delete("user:2").await.expect("first delete should succeed");
    assert!(existed, "delete should return true when key existed");

    let already_gone = client.delete("user:2").await.expect("second delete should succeed");
    assert!(!already_gone, "delete should return false when key was already absent");
}

#[tokio::test]
async fn client_payload_too_large() {
    let client = CacheKit::builder()
        .backend(MockBackend::new())
        .max_payload_bytes(10)
        .no_l1()
        .build()
        .expect("client builds");

    // A long string will serialise to well over 10 bytes.
    let big_value = "x".repeat(100);

    let err = client
        .set("big", &big_value)
        .await
        .expect_err("set should fail for oversized payload");

    assert!(
        matches!(err, CachekitError::PayloadTooLarge { .. }),
        "expected PayloadTooLarge, got: {err:?}"
    );
}

#[tokio::test]
async fn client_key_validation() {
    let client = mock_client();

    // Empty key
    let err = client.get::<String>("").await.expect_err("empty key should be rejected");
    assert!(matches!(err, CachekitError::InvalidKey(_)), "empty key: {err:?}");

    // Control character (newline = 0x0A)
    let err = client
        .get::<String>("bad\nkey")
        .await
        .expect_err("control char key should be rejected");
    assert!(matches!(err, CachekitError::InvalidKey(_)), "control char: {err:?}");

    // DEL character (0x7F)
    let err = client
        .get::<String>("bad\x7Fkey")
        .await
        .expect_err("DEL char key should be rejected");
    assert!(matches!(err, CachekitError::InvalidKey(_)), "DEL char: {err:?}");

    // Key that is exactly 1025 bytes (one over the limit)
    let too_long = "a".repeat(1025);
    let err = client
        .get::<String>(&too_long)
        .await
        .expect_err("over-length key should be rejected");
    assert!(matches!(err, CachekitError::InvalidKey(_)), "too long: {err:?}");

    // Boundary case: exactly 1024 bytes should be accepted
    let client2 = CacheKit::builder()
        .backend(MockBackend::new())
        .no_l1()
        .build()
        .expect("client builds");
    let at_limit = "a".repeat(1024);
    let result = client2.get::<String>(&at_limit).await;
    assert!(result.is_ok(), "1024-byte key should be accepted: {result:?}");
}
