//! Integration tests for the zero-knowledge encryption layer.
//!
//! Run with:
//!   cargo test --test encryption_tests --features cachekitio,encryption,l1

mod common;

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::common::MockBackend;
use cachekit::{CacheKit, CachekitError};

// ── Test fixtures ─────────────────────────────────────────────────────────────

/// 32-byte master key for tests. NOT for production use.
const TEST_MASTER_KEY: &[u8] = b"test_master_key_32_bytes_long!!!";

/// Hex-encoded version of the test master key.
fn test_master_key_hex() -> String {
    hex::encode(TEST_MASTER_KEY)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Secret {
    api_key: String,
    user_id: u64,
}

fn make_encrypted_client(backend: Arc<MockBackend>) -> CacheKit {
    CacheKit::builder()
        .backend(backend)
        .default_ttl(Duration::from_secs(60))
        .no_l1()
        .encryption_from_bytes(TEST_MASTER_KEY, "test-tenant")
        .expect("encryption setup")
        .build()
        .expect("client builds")
}

fn make_encrypted_client_with_l1(backend: Arc<MockBackend>) -> CacheKit {
    CacheKit::builder()
        .backend(backend)
        .default_ttl(Duration::from_secs(60))
        .l1_capacity(100)
        .encryption_from_bytes(TEST_MASTER_KEY, "test-tenant")
        .expect("encryption setup")
        .build()
        .expect("client builds")
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn secure_set_and_get() {
    let backend = MockBackend::new();
    let client = make_encrypted_client(backend);

    let secret = Secret {
        api_key: "sk-live-abc123".to_owned(),
        user_id: 42,
    };

    let secure = client
        .secure()
        .expect("secure() should work with encryption configured");
    secure.set("secret:42", &secret).await.expect("secure set");

    let retrieved: Secret = secure
        .get("secret:42")
        .await
        .expect("secure get")
        .expect("value should exist");

    assert_eq!(retrieved, secret);
}

#[tokio::test]
async fn secure_data_is_encrypted_in_backend() {
    let backend = MockBackend::new();
    let client = make_encrypted_client(backend.clone());

    let secret = Secret {
        api_key: "sk-live-SUPERSECRET".to_owned(),
        user_id: 999,
    };

    let secure = client.secure().unwrap();
    secure.set("secret:999", &secret).await.unwrap();

    // Read raw bytes from the backend
    let raw_bytes = backend
        .store
        .lock()
        .await
        .get("secret:999")
        .cloned()
        .expect("key should exist in backend");

    // The stored bytes must NOT contain the plaintext API key
    let raw_str = String::from_utf8_lossy(&raw_bytes);
    assert!(
        !raw_str.contains("SUPERSECRET"),
        "backend must store ciphertext, not plaintext; got: {raw_str}"
    );

    // Ciphertext must include the 12-byte nonce prefix + at least 16-byte auth tag
    assert!(
        raw_bytes.len() >= 28,
        "ciphertext too short: {} bytes (expected nonce + tag overhead)",
        raw_bytes.len()
    );
}

#[tokio::test]
async fn secure_without_master_key_fails() {
    let client = CacheKit::builder()
        .backend(MockBackend::new())
        .no_l1()
        .build()
        .expect("client builds without encryption");

    let result = client.secure();
    assert!(result.is_err(), "secure() without encryption should fail");

    let err = result.unwrap_err();
    assert!(
        matches!(err, CachekitError::Config(_)),
        "expected Config error, got: {err:?}"
    );
    assert!(
        err.to_string().contains("CACHEKIT_MASTER_KEY"),
        "error should mention CACHEKIT_MASTER_KEY: {err}"
    );
}

#[tokio::test]
async fn secure_get_missing_returns_none() {
    let client = make_encrypted_client(MockBackend::new());
    let secure = client.secure().unwrap();

    let result: Option<String> = secure.get("nonexistent").await.expect("get should succeed");
    assert!(result.is_none());
}

#[tokio::test]
async fn secure_delete() {
    let client = make_encrypted_client(MockBackend::new());
    let secure = client.secure().unwrap();

    secure.set("to-delete", &"temporary").await.unwrap();
    assert!(secure.exists("to-delete").await.unwrap());

    let deleted = secure.delete("to-delete").await.unwrap();
    assert!(deleted);

    let gone: Option<String> = secure.get("to-delete").await.unwrap();
    assert!(gone.is_none());
}

#[tokio::test]
async fn secure_wrong_key_fails_decryption() {
    let backend = MockBackend::new();
    let client = make_encrypted_client(backend.clone());

    let secure = client.secure().unwrap();
    secure.set("key-a", &"secret data").await.unwrap();

    // Manually swap the value to a different key in the backend
    let stored = backend.store.lock().await.get("key-a").cloned().unwrap();
    backend
        .store
        .lock()
        .await
        .insert("key-b".to_owned(), stored);

    // Decrypting with a different cache key should fail (AAD mismatch)
    let result: Result<Option<String>, _> = secure.get("key-b").await;
    assert!(
        result.is_err(),
        "decryption with wrong cache key AAD must fail"
    );
}

#[tokio::test]
async fn secure_different_tenants_cant_decrypt() {
    let backend = MockBackend::new();

    let client_a = CacheKit::builder()
        .backend(backend.clone())
        .no_l1()
        .encryption_from_bytes(TEST_MASTER_KEY, "tenant-a")
        .unwrap()
        .build()
        .unwrap();

    let client_b = CacheKit::builder()
        .backend(backend)
        .no_l1()
        .encryption_from_bytes(TEST_MASTER_KEY, "tenant-b")
        .unwrap()
        .build()
        .unwrap();

    client_a
        .secure()
        .unwrap()
        .set("shared-key", &"tenant-a-secret")
        .await
        .unwrap();

    // Tenant B should fail to decrypt tenant A's data
    let result: Result<Option<String>, _> = client_b.secure().unwrap().get("shared-key").await;
    assert!(
        result.is_err(),
        "cross-tenant decryption must fail (different derived keys)"
    );
}

#[tokio::test]
async fn secure_hex_builder() {
    let client = CacheKit::builder()
        .backend(MockBackend::new())
        .no_l1()
        .encryption(&test_master_key_hex(), "hex-tenant")
        .expect("hex encryption setup")
        .build()
        .unwrap();

    let secure = client.secure().unwrap();
    secure.set("hex-test", &42u64).await.unwrap();

    let val: u64 = secure.get("hex-test").await.unwrap().unwrap();
    assert_eq!(val, 42);
}

#[tokio::test]
async fn secure_with_l1_roundtrip() {
    let backend = MockBackend::new();
    let client = make_encrypted_client_with_l1(backend.clone());

    let secure = client.secure().unwrap();
    secure.set("l1-test", &"encrypted in L1").await.unwrap();

    // First get populates L1 (already done by set write-through)
    let val: String = secure.get("l1-test").await.unwrap().unwrap();
    assert_eq!(val, "encrypted in L1");

    // Remove from backend to prove L1 is serving ciphertext
    backend.store.lock().await.remove("l1-test");

    // Should still get the value from L1 (decrypted from ciphertext)
    let val2: String = secure.get("l1-test").await.unwrap().unwrap();
    assert_eq!(val2, "encrypted in L1");
}

#[tokio::test]
async fn secure_l1_stores_ciphertext_not_plaintext() {
    let backend = MockBackend::new();
    let client = make_encrypted_client_with_l1(backend.clone());

    let secure = client.secure().unwrap();
    secure.set("l1-cipher", &"PLAINTEXT_VALUE").await.unwrap();

    // The backend should have ciphertext, not the msgpack encoding of "PLAINTEXT_VALUE".
    let store = backend.store.lock().await;
    let (_key, raw_bytes) = store.iter().next().expect("backend should have one entry");
    let plaintext_msgpack = rmp_serde::to_vec_named(&"PLAINTEXT_VALUE").unwrap();
    assert_ne!(
        raw_bytes, &plaintext_msgpack,
        "backend should store ciphertext, not plaintext msgpack"
    );
    // Ciphertext has AAD prefix (0x03 version byte) and is longer than plaintext
    assert!(
        raw_bytes.len() > plaintext_msgpack.len(),
        "ciphertext should be larger than plaintext due to AAD + GCM tag"
    );
}

#[tokio::test]
async fn secure_with_namespace() {
    let backend = MockBackend::new();
    let client = CacheKit::builder()
        .backend(backend.clone())
        .namespace("ns")
        .no_l1()
        .encryption_from_bytes(TEST_MASTER_KEY, "test-tenant")
        .unwrap()
        .build()
        .unwrap();

    let secure = client.secure().unwrap();
    secure.set("namespaced", &"value").await.unwrap();

    // Backend should have the namespaced key
    let keys: Vec<String> = backend.store.lock().await.keys().cloned().collect();
    assert!(
        keys.contains(&"ns:namespaced".to_owned()),
        "expected namespaced key, got: {keys:?}"
    );

    // Round-trip should work
    let val: String = secure.get("namespaced").await.unwrap().unwrap();
    assert_eq!(val, "value");
}
