mod common;

use cachekit::error::{BackendError, CachekitError};

use crate::common::MockBackend;
use cachekit::backend::Backend;

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
    assert!(status.is_healthy);
}

#[tokio::test]
async fn backend_error_converts_to_cachekit_error() {
    let err = BackendError::transient("whoops");
    let cachekit_err: CachekitError = err.into();
    assert!(matches!(cachekit_err, CachekitError::Backend(_)));
}
