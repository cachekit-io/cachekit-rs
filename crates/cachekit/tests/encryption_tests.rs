//! Integration tests for the zero-knowledge encryption layer.
//!
//! Run with:
//!   cargo test --test encryption_tests --features cachekitio,encryption,l1

mod common;

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::common::MockBackend;
use cachekit::client::SharedBackend;
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

fn make_encrypted_client(backend: SharedBackend) -> CacheKit {
    CacheKit::builder()
        .backend(backend)
        .default_ttl(Duration::from_secs(60))
        .no_l1()
        .encryption_from_bytes(TEST_MASTER_KEY, "test-tenant")
        .expect("encryption setup")
        .build()
        .expect("client builds")
}

fn make_encrypted_client_with_l1(backend: SharedBackend) -> CacheKit {
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
    let backend = MockBackend::shared();
    let client = make_encrypted_client(backend);

    let secret = Secret {
        api_key: "sk-live-abc123".to_owned(), // pragma: allowlist secret
        user_id: 42,
    };

    let secure = client
        .secure_cache()
        .expect("secure_cache() should work with encryption configured");
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
    let (shared, backend) = MockBackend::new_with_handle();
    let client = make_encrypted_client(shared);

    let secret = Secret {
        api_key: "sk-live-SUPERSECRET".to_owned(), // pragma: allowlist secret
        user_id: 999,
    };

    let secure = client.secure_cache().unwrap();
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
        .backend(MockBackend::shared())
        .no_l1()
        .build()
        .expect("client builds without encryption");

    let result = client.secure_cache();
    assert!(
        result.is_err(),
        "secure_cache() without encryption should fail"
    );

    let err = result.unwrap_err();
    assert!(
        matches!(err, CachekitError::Config(_)),
        "expected Config error, got: {err:?}"
    );
    let msg = err.to_string();
    assert!(
        msg.contains("CacheKit::secure(url, master_key_hex)")
            && msg.contains("CacheKit::secure_from_env(url)")
            && msg.contains("CACHEKIT_MASTER_KEY"),
        "error should name both secure preset forms and CACHEKIT_MASTER_KEY: {msg}"
    );
}

#[tokio::test]
async fn secure_get_missing_returns_none() {
    let client = make_encrypted_client(MockBackend::shared());
    let secure = client.secure_cache().unwrap();

    let result: Option<String> = secure.get("nonexistent").await.expect("get should succeed");
    assert!(result.is_none());
}

#[tokio::test]
async fn secure_delete() {
    let client = make_encrypted_client(MockBackend::shared());
    let secure = client.secure_cache().unwrap();

    secure.set("to-delete", &"temporary").await.unwrap();
    assert!(secure.exists("to-delete").await.unwrap());

    let deleted = secure.delete("to-delete").await.unwrap();
    assert!(deleted);

    let gone: Option<String> = secure.get("to-delete").await.unwrap();
    assert!(gone.is_none());
}

#[tokio::test]
async fn secure_wrong_key_fails_decryption() {
    let (shared, backend) = MockBackend::new_with_handle();
    let client = make_encrypted_client(shared);

    let secure = client.secure_cache().unwrap();
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
    let (shared_a, _backend) = MockBackend::new_with_handle();
    let shared_b = shared_a.clone();

    let client_a = CacheKit::builder()
        .backend(shared_a)
        .no_l1()
        .encryption_from_bytes(TEST_MASTER_KEY, "tenant-a")
        .unwrap()
        .build()
        .unwrap();

    let client_b = CacheKit::builder()
        .backend(shared_b)
        .no_l1()
        .encryption_from_bytes(TEST_MASTER_KEY, "tenant-b")
        .unwrap()
        .build()
        .unwrap();

    client_a
        .secure_cache()
        .unwrap()
        .set("shared-key", &"tenant-a-secret")
        .await
        .unwrap();

    // Tenant B should fail to decrypt tenant A's data
    let result: Result<Option<String>, _> =
        client_b.secure_cache().unwrap().get("shared-key").await;
    assert!(
        result.is_err(),
        "cross-tenant decryption must fail (different derived keys)"
    );
}

#[tokio::test]
async fn secure_hex_builder() {
    let client = CacheKit::builder()
        .backend(MockBackend::shared())
        .no_l1()
        .encryption(&test_master_key_hex(), "hex-tenant")
        .expect("hex encryption setup")
        .build()
        .unwrap();

    let secure = client.secure_cache().unwrap();
    secure.set("hex-test", &42u64).await.unwrap();

    let val: u64 = secure.get("hex-test").await.unwrap().unwrap();
    assert_eq!(val, 42);
}

#[test]
fn hex_builder_error_does_not_quote_the_key() {
    // hex's own error Display names the offending character — key material
    // in an error string (CWE-532).
    let key = format!("{}Q9", "a1".repeat(31));
    let err = CacheKit::builder()
        .backend(MockBackend::shared())
        .encryption(&key, "hex-tenant")
        .err()
        .expect("non-hex key must be rejected");
    assert!(matches!(err, CachekitError::Config(_)), "got {err:?}");
    assert!(
        !err.to_string().contains('Q'),
        "error quotes key material: {err}"
    );
}

#[tokio::test]
async fn secure_with_l1_roundtrip() {
    let (shared, backend) = MockBackend::new_with_handle();
    let client = make_encrypted_client_with_l1(shared);

    let secure = client.secure_cache().unwrap();
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
    let (shared, backend) = MockBackend::new_with_handle();
    let client = make_encrypted_client_with_l1(shared);

    let secure = client.secure_cache().unwrap();
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

/// Write a real ciphertext for `key` through an L1-less client, then flip its
/// last byte in the backend so it fails AES-GCM authentication. Returns the
/// planted bytes.
async fn plant_tampered(shared: SharedBackend, backend: &MockBackend, key: &str) -> Vec<u8> {
    let writer = make_encrypted_client(shared);
    writer
        .secure_cache()
        .unwrap()
        .set(key, &"authentic")
        .await
        .unwrap();
    let mut store = backend.store.lock().await;
    let bytes = store.get_mut(key).expect("writer stored the entry");
    *bytes.last_mut().unwrap() ^= 0x01;
    bytes.clone()
}

#[tokio::test]
async fn secure_get_evicts_l1_after_decrypt_failure() {
    let (shared, backend) = MockBackend::new_with_handle();
    let client = make_encrypted_client_with_l1(shared.clone());
    let secure = client.secure_cache().unwrap();
    let planted = plant_tampered(shared, &backend, "poisoned").await;

    let err = secure.get::<String>("poisoned").await.unwrap_err();
    assert!(matches!(err, CachekitError::Encryption(_)), "got: {err:?}");
    // The failed read must not remove or rewrite the backend entry.
    assert_eq!(backend.store.lock().await.get("poisoned"), Some(&planted));

    // Remove the entry behind the client's back: a surviving L1 copy would
    // keep failing; an evicted one yields a miss.
    backend.store.lock().await.remove("poisoned");
    assert_eq!(secure.get::<String>("poisoned").await.unwrap(), None);
}

#[tokio::test]
async fn secure_interop_get_swr_evicts_l1_after_decrypt_failure() {
    let (shared, backend) = MockBackend::new_with_handle();
    let client = make_encrypted_client_with_l1(shared.clone());
    let secure = client.secure_cache().unwrap();
    let planted = plant_tampered(shared, &backend, "poisoned-swr").await;

    let err = secure
        .interop_get_swr::<String>("poisoned-swr")
        .await
        .unwrap_err();
    assert!(matches!(err, CachekitError::Encryption(_)), "got: {err:?}");
    assert_eq!(
        backend.store.lock().await.get("poisoned-swr"),
        Some(&planted)
    );

    backend.store.lock().await.remove("poisoned-swr");
    assert!(matches!(
        secure.interop_get_swr::<String>("poisoned-swr").await,
        Ok(cachekit::SwrRead::Miss)
    ));
}

#[tokio::test]
async fn secure_with_namespace() {
    let (shared, backend) = MockBackend::new_with_handle();
    let client = CacheKit::builder()
        .backend(shared)
        .namespace("ns")
        .no_l1()
        .encryption_from_bytes(TEST_MASTER_KEY, "test-tenant")
        .unwrap()
        .build()
        .unwrap();

    let secure = client.secure_cache().unwrap();
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

#[tokio::test]
async fn secure_set_rejects_payload_whose_ciphertext_exceeds_limit() {
    // The get paths size-check the STORED ciphertext (plaintext + 28 bytes of
    // nonce + tag). If set only checked the plaintext, a value within 28 bytes
    // of the limit would write successfully and then fail every read with
    // PayloadTooLarge. set must therefore check the ciphertext length.
    let client = CacheKit::builder()
        .backend(MockBackend::shared())
        .no_l1()
        .max_payload_bytes(64)
        .encryption_from_bytes(TEST_MASTER_KEY, "test-tenant")
        .expect("encryption setup")
        .build()
        .expect("client builds");
    let secure = client.secure_cache().expect("secure handle");

    // 50 serialized bytes: under the 64-byte limit as plaintext, over it as
    // ciphertext (50 + 28 = 78). Must fail at write time, not become
    // unreadable after a successful write.
    let value = "x".repeat(48); // msgpack str8: 2 header bytes + 48
    let err = secure
        .set("boundary:key", &value)
        .await
        .expect_err("ciphertext over limit must fail at set time");
    assert!(
        matches!(err, CachekitError::PayloadTooLarge { .. }),
        "expected PayloadTooLarge, got: {err:?}"
    );

    // Well under the limit still round-trips.
    secure.set("small:key", &"ok").await.expect("small set");
    let got: Option<String> = secure.get("small:key").await.expect("small get");
    assert_eq!(got.as_deref(), Some("ok"));
}

// ── Key rotation (keyring) ────────────────────────────────────────────────────

/// End-to-end rotation round-trip:
/// value written under k1 → k2 promoted with k1 decrypt-only → read succeeds
/// without re-encryption → k1 dropped → read fails as an error.
#[tokio::test]
async fn rotation_round_trip_without_reencryption() {
    const K1: &[u8] = &[0x11; 32];
    const K2: &[u8] = &[0x22; 32];

    let (backend, store) = common::MockBackend::new_with_handle();

    // Phase 1: pre-rotation client writes under k1.
    let writer = CacheKit::builder()
        .backend(backend.clone())
        .default_ttl(Duration::from_secs(60))
        .no_l1()
        .encryption_from_bytes(K1, "test-tenant")
        .expect("encryption setup")
        .build()
        .expect("client builds");
    let secret = Secret {
        api_key: "sk-live-rotate-me".to_owned(), // pragma: allowlist secret
        user_id: 7,
    };
    writer
        .secure_cache()
        .expect("secure_cache()")
        .set("secret:7", &secret)
        .await
        .expect("secure set under k1");

    let ciphertext_before = store.store.lock().await.get("secret:7").cloned().unwrap();

    // Phase 2: k2 promoted to current, k1 retained decrypt-only.
    let rotated = CacheKit::builder()
        .backend(backend.clone())
        .default_ttl(Duration::from_secs(60))
        .no_l1()
        .encryption_from_bytes_with_previous(K2, &[K1], "test-tenant")
        .expect("keyring setup")
        .build()
        .expect("client builds");
    let read_back: Secret = rotated
        .secure_cache()
        .expect("secure_cache()")
        .get("secret:7")
        .await
        .expect("secure get after rotation")
        .expect("value should exist");
    assert_eq!(read_back, secret);

    // The read must not have re-encrypted: stored bytes are untouched.
    let ciphertext_after = store.store.lock().await.get("secret:7").cloned().unwrap();
    assert_eq!(
        ciphertext_before, ciphertext_after,
        "read-through rotation must not rewrite the entry"
    );

    // Phase 3: k1 dropped — hard cut-over, the k1-era entry is unreadable.
    let cut_over = CacheKit::builder()
        .backend(backend)
        .default_ttl(Duration::from_secs(60))
        .no_l1()
        .encryption_from_bytes_with_previous(K2, &[], "test-tenant")
        .expect("keyring setup")
        .build()
        .expect("client builds");
    let result: Result<Option<Secret>, _> = cut_over
        .secure_cache()
        .expect("secure_cache()")
        .get("secret:7")
        .await;
    assert!(
        matches!(result, Err(CachekitError::Encryption(_))),
        "dropped-key read must surface as an encryption error, got {result:?}"
    );
}

/// Rotation drain signal: the builder wires the counters into the
/// user-held secure handle, so a read served by the retiring key is visible
/// there. Index-0 silence is owned and tested at the layer (`encryption.rs`).
#[tokio::test]
async fn rotation_drain_signal_is_visible_on_secure_cache() {
    const K1: &[u8] = &[0x11; 32];
    const K2: &[u8] = &[0x22; 32];

    let backend = common::MockBackend::shared();

    let writer = CacheKit::builder()
        .backend(backend.clone())
        .default_ttl(Duration::from_secs(60))
        .no_l1()
        .encryption_from_bytes(K1, "test-tenant")
        .expect("encryption setup")
        .build()
        .expect("client builds");
    writer
        .secure_cache()
        .expect("secure_cache()")
        .set("drain:old", &"written under k1")
        .await
        .expect("secure set under k1");

    let rotated = CacheKit::builder()
        .backend(backend)
        .default_ttl(Duration::from_secs(60))
        .no_l1()
        .encryption_from_bytes_with_previous(K2, &[K1], "test-tenant")
        .expect("keyring setup")
        .build()
        .expect("client builds");
    let secure = rotated.secure_cache().expect("secure_cache()");
    assert_eq!(secure.previous_key_hits(), vec![0], "nothing read yet");

    // The k1-era entry is served by previous[0]: the grace window is still live.
    let _: Option<String> = secure.get("drain:old").await.expect("secure get");
    assert_eq!(
        secure.previous_key_hits(),
        vec![1],
        "previous-key hit is counted"
    );
}

// ── Plain methods on an encrypted client ──────────────────────────────────────
//
// protocol spec/intent-presets.md § Encryption Activation, rule 1: an explicit
// encryption option encrypts every operation on the client, not only the
// `secure_cache()` handle.

type Configure = fn(cachekit::CacheKitBuilder) -> Result<cachekit::CacheKitBuilder, CachekitError>;

/// Every builder encryption spelling, each with the same key and tenant, so
/// one independently built layer decrypts whatever any of them stores.
const SPELLINGS: [(&str, Configure); 3] = [
    ("encryption", |b| {
        b.encryption(&test_master_key_hex(), "test-tenant")
    }),
    ("encryption_from_bytes", |b| {
        b.encryption_from_bytes(TEST_MASTER_KEY, "test-tenant")
    }),
    ("encryption_from_bytes_with_previous", |b| {
        b.encryption_from_bytes_with_previous(TEST_MASTER_KEY, &[&[0x11; 32]], "test-tenant")
    }),
];

fn plain_client(configure: Configure, backend: SharedBackend, l1: bool) -> CacheKit {
    let builder = CacheKit::builder()
        .backend(backend)
        .default_ttl(Duration::from_secs(60));
    let builder = if l1 {
        builder.l1_capacity(100)
    } else {
        builder.no_l1()
    };
    configure(builder)
        .expect("encryption setup")
        .build()
        .expect("client builds")
}

fn plain_secret() -> Secret {
    Secret {
        api_key: "sk-live-PLAINPATH".to_owned(), // pragma: allowlist secret
        user_id: 7,
    }
}

/// The bytes the backend holds for `key` are the AES-GCM ciphertext of
/// `value`'s MessagePack: never the MessagePack itself, and decryptable by a
/// layer built apart from the client.
async fn assert_backend_holds_ciphertext_of<T: Serialize>(
    backend: &MockBackend,
    key: &str,
    value: &T,
    how: &str,
) {
    let stored = backend
        .store
        .lock()
        .await
        .get(key)
        .cloned()
        .unwrap_or_else(|| panic!("{how}: {key} never reached the backend"));
    let plaintext = cachekit::serializer::serialize(value).unwrap();
    assert_ne!(stored, plaintext, "{how}: {key} stored as plaintext");
    let decrypted = cachekit::EncryptionLayer::new(TEST_MASTER_KEY, "test-tenant")
        .unwrap()
        .decrypt(&stored, key)
        .unwrap_or_else(|e| panic!("{how}: {key} is not ciphertext under the key: {e}"));
    assert_eq!(decrypted, plaintext, "{how}: {key}");
}

#[tokio::test]
async fn plain_set_and_set_with_ttl_store_ciphertext() {
    let secret = plain_secret();
    for (how, configure) in SPELLINGS {
        let (shared, backend) = MockBackend::new_with_handle();
        let client = plain_client(configure, shared, false);
        client.set("plain:set", &secret).await.unwrap();
        client
            .set_with_ttl("plain:ttl", &secret, Duration::from_secs(30))
            .await
            .unwrap();
        assert_backend_holds_ciphertext_of(&backend, "plain:set", &secret, how).await;
        assert_backend_holds_ciphertext_of(&backend, "plain:ttl", &secret, how).await;
    }
}

#[tokio::test]
async fn plain_reads_decrypt_what_plain_writes_store() {
    let secret = plain_secret();
    for (how, configure) in SPELLINGS {
        let client = plain_client(configure, MockBackend::shared(), false);
        client.set("plain:rt", &secret).await.unwrap();
        assert_eq!(
            client.get::<Secret>("plain:rt").await.unwrap(),
            Some(secret.clone()),
            "{how}: get"
        );
        assert_eq!(
            client.interop_get::<Secret>("plain:rt").await.unwrap(),
            Some(secret.clone()),
            "{how}: interop_get"
        );
        assert_eq!(
            client.interop_get_swr::<Secret>("plain:rt").await.unwrap(),
            cachekit::SwrRead::Fresh(secret.clone()),
            "{how}: interop_get_swr"
        );

        // One format: the secure_cache() handle and the plain methods read
        // each other's entries.
        let secure = client.secure_cache().unwrap();
        assert_eq!(
            secure.get::<Secret>("plain:rt").await.unwrap(),
            Some(secret.clone()),
            "{how}: handle reads a plain write"
        );
        secure.set("secure:rt", &secret).await.unwrap();
        assert_eq!(
            client.get::<Secret>("secure:rt").await.unwrap(),
            Some(secret.clone()),
            "{how}: plain get reads a handle write"
        );
    }
}

/// L1 is not visible from here, so read it through the handle, which always
/// decrypts: with the backend entry gone, an L1 hit that decrypts proves L1
/// holds ciphertext (the crate-internal `secure_tests` compare the bytes).
#[tokio::test]
async fn plain_writes_keep_ciphertext_in_l1() {
    let secret = plain_secret();
    for (how, configure) in SPELLINGS {
        let (shared, backend) = MockBackend::new_with_handle();
        let client = plain_client(configure, shared, true);
        client.set("plain:l1", &secret).await.unwrap();
        backend.store.lock().await.clear();

        let secure = client.secure_cache().unwrap();
        assert_eq!(
            secure.get::<Secret>("plain:l1").await.unwrap(),
            Some(secret.clone()),
            "{how}"
        );
        assert_eq!(client.stats().l1_hits, 1, "{how}: served from L1");
    }
}

/// An entry written in the clear (by an unencrypted client, or by plain
/// `set` on an encrypted client before plain writes encrypted) fails closed
/// on an encrypted client's plain reads, until it is overwritten or expires.
#[tokio::test]
async fn plain_reads_reject_a_plaintext_entry() {
    let (shared, _backend) = MockBackend::new_with_handle();
    CacheKit::builder()
        .backend(shared.clone())
        .no_l1()
        .build()
        .unwrap()
        .set("legacy", &"in the clear")
        .await
        .unwrap();

    let client = make_encrypted_client(shared);
    let err = client.get::<String>("legacy").await.unwrap_err();
    assert!(matches!(err, CachekitError::Encryption(_)), "got: {err:?}");
    let err = client.interop_get::<String>("legacy").await.unwrap_err();
    assert!(matches!(err, CachekitError::Encryption(_)), "got: {err:?}");

    client.set("legacy", &"sealed").await.unwrap();
    assert_eq!(
        client.get::<String>("legacy").await.unwrap().as_deref(),
        Some("sealed")
    );
}

/// A decode failure after a successful decrypt names only the target type:
/// serde's own message quotes the decrypted value (CWE-532) — here
/// `invalid type: string "sk-live-…", expected struct Secret`. The error stays
/// `Serialization`, which `#[cachekit]` reads as a miss.
#[tokio::test]
async fn decode_error_after_decrypt_does_not_quote_the_value() {
    let client = make_encrypted_client(MockBackend::shared());
    client.set("typed", &"sk-live-REDACTME").await.unwrap(); // pragma: allowlist secret
    let secure = client.secure_cache().unwrap();
    for (how, result) in [
        ("get", client.get::<Secret>("typed").await.map(drop)),
        (
            "interop_get",
            client.interop_get::<Secret>("typed").await.map(drop),
        ),
        (
            "interop_get_swr",
            client.interop_get_swr::<Secret>("typed").await.map(drop),
        ),
        ("handle get", secure.get::<Secret>("typed").await.map(drop)),
    ] {
        match result {
            Err(CachekitError::Serialization(msg)) => {
                assert!(!msg.contains("REDACTME"), "{how} quotes the value: {msg}");
                assert!(
                    msg.contains("Secret"),
                    "{how} must name the target type: {msg}"
                );
            }
            other => panic!("{how}: expected a Serialization error, got {other:?}"),
        }
    }
}

// ── Master-key length (spec/intent-presets.md § Master Key Input) ─────────────

fn assert_builder_config_err(result: Result<cachekit::CacheKitBuilder, CachekitError>, what: &str) {
    match result {
        Err(CachekitError::Config(_)) => {}
        Err(e) => panic!("{what}: expected Config error, got {e:?}"),
        Ok(_) => panic!("{what}: expected Config error, got Ok"),
    }
}

/// Rule 4: a raw-bytes entry point takes exactly 32 bytes.
#[test]
fn encryption_from_bytes_requires_exactly_32_bytes() {
    for len in [31, 33] {
        assert_builder_config_err(
            CacheKit::builder().encryption_from_bytes(&vec![7u8; len], "t"),
            &format!("{len}-byte key"),
        );
    }
    assert!(CacheKit::builder()
        .encryption_from_bytes(&[7u8; 32], "t")
        .is_ok());
}

/// The ASCII bytes of a 64-char hex string pass a `>= 32` check and derive a
/// silently different key — they must be rejected, not accepted.
#[test]
fn encryption_from_bytes_rejects_ascii_hex() {
    let hex = "ab".repeat(32);
    assert_builder_config_err(
        CacheKit::builder().encryption_from_bytes(hex.as_bytes(), "t"),
        "ASCII hex as raw bytes",
    );
}

#[test]
fn encryption_from_bytes_with_previous_requires_exactly_32_bytes() {
    const K1: &[u8] = &[0x11; 32];
    const K2: &[u8] = &[0x22; 32];
    for len in [31, 33] {
        let bad = vec![7u8; len];
        assert_builder_config_err(
            CacheKit::builder().encryption_from_bytes_with_previous(&bad, &[K1], "t"),
            &format!("{len}-byte current key"),
        );
        assert_builder_config_err(
            CacheKit::builder().encryption_from_bytes_with_previous(K2, &[&bad], "t"),
            &format!("{len}-byte previous key"),
        );
    }
    assert!(CacheKit::builder()
        .encryption_from_bytes_with_previous(K2, &[K1], "t")
        .is_ok());
}

/// Rule 3: the hex path accepts any key decoding to >= 32 bytes.
#[test]
fn encryption_hex_accepts_32_and_48_byte_keys() {
    for bytes in [32, 48] {
        assert!(
            CacheKit::builder()
                .encryption(&"ab".repeat(bytes), "t")
                .is_ok(),
            "{bytes}-byte hex key must be accepted"
        );
    }
}

#[cfg(feature = "cachekitio")]
#[test]
#[serial_test::serial]
fn from_env_accepts_32_and_48_byte_hex_keys() {
    for bytes in [32, 48] {
        let (current, previous) = ("22".repeat(bytes), "11".repeat(bytes));
        let _env = common::EnvGuard::set(&[
            ("CACHEKIT_API_KEY", Some("test-key")),
            ("CACHEKIT_API_URL", None),
            ("CACHEKIT_MASTER_KEY", Some(current.as_str())),
            ("CACHEKIT_PREVIOUS_MASTER_KEYS", Some(previous.as_str())),
        ]);
        let builder = CacheKit::from_env()
            .unwrap_or_else(|e| panic!("{bytes}-byte hex keys must be accepted: {e}"));
        assert!(builder.build().unwrap().secure_cache().is_ok());
    }
}
