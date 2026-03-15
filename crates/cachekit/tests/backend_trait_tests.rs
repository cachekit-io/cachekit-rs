use async_trait::async_trait;
use cachekit::backend::{Backend, HealthStatus};
use cachekit::error::{BackendError, CachekitError};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

// ── MockBackend ───────────────────────────────────────────────────────────────

#[derive(Clone, Default)]
struct MockBackend {
    store: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

#[async_trait]
impl Backend for MockBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        Ok(self.store.lock().unwrap().get(key).cloned())
    }

    async fn set(&self, key: &str, value: Vec<u8>, _ttl: Option<Duration>) -> Result<(), BackendError> {
        self.store.lock().unwrap().insert(key.to_owned(), value);
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<bool, BackendError> {
        Ok(self.store.lock().unwrap().remove(key).is_some())
    }

    async fn exists(&self, key: &str) -> Result<bool, BackendError> {
        Ok(self.store.lock().unwrap().contains_key(key))
    }

    async fn health(&self) -> Result<HealthStatus, BackendError> {
        Ok(HealthStatus { backend_type: "mock".to_owned(), details: HashMap::new() })
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn roundtrip_set_and_get() {
    let backend = MockBackend::default();
    backend.set("hello", b"world".to_vec(), None).await.unwrap();
    let result = backend.get("hello").await.unwrap();
    assert_eq!(result, Some(b"world".to_vec()));
}

#[tokio::test]
async fn get_missing_returns_none() {
    let backend = MockBackend::default();
    let result = backend.get("nonexistent").await.unwrap();
    assert_eq!(result, None);
}

#[tokio::test]
async fn delete_existing_key() {
    let backend = MockBackend::default();
    backend.set("foo", b"bar".to_vec(), None).await.unwrap();
    let deleted = backend.delete("foo").await.unwrap();
    assert!(deleted);
    assert_eq!(backend.get("foo").await.unwrap(), None);
}

#[tokio::test]
async fn delete_missing_key_returns_false() {
    let backend = MockBackend::default();
    let deleted = backend.delete("ghost").await.unwrap();
    assert!(!deleted);
}

#[tokio::test]
async fn exists_after_set() {
    let backend = MockBackend::default();
    assert!(!backend.exists("k").await.unwrap());
    backend.set("k", b"v".to_vec(), None).await.unwrap();
    assert!(backend.exists("k").await.unwrap());
}

#[tokio::test]
async fn health_returns_backend_type() {
    let backend = MockBackend::default();
    let status = backend.health().await.unwrap();
    assert_eq!(status.backend_type, "mock");
}

#[tokio::test]
async fn backend_error_converts_to_cachekit_error() {
    let err = BackendError::transient("whoops");
    let cachekit_err: CachekitError = err.into();
    assert!(matches!(cachekit_err, CachekitError::Backend(_)));
}
