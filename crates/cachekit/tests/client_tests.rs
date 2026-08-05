//! Integration tests for the CacheKit client.
//!
//! Run with:
//!   cargo test --test client_tests --features cachekitio,l1

mod common;

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::common::MockBackend;
use cachekit::{CacheKit, CachekitError};

// ── Test fixtures ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct User {
    id: u32,
    name: String,
}

fn mock_client() -> CacheKit {
    CacheKit::builder()
        .backend(MockBackend::shared())
        .default_ttl(Duration::from_secs(60))
        .no_l1()
        .build()
        .expect("mock client builds without error")
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn client_set_and_get() {
    let client = mock_client();
    let user = User {
        id: 1,
        name: "Alice".to_owned(),
    };

    client
        .set("user:1", &user)
        .await
        .expect("set should succeed");

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
    let user = User {
        id: 2,
        name: "Bob".to_owned(),
    };

    client
        .set("user:2", &user)
        .await
        .expect("set should succeed");

    let existed = client
        .delete("user:2")
        .await
        .expect("first delete should succeed");
    assert!(existed, "delete should return true when key existed");

    let already_gone = client
        .delete("user:2")
        .await
        .expect("second delete should succeed");
    assert!(
        !already_gone,
        "delete should return false when key was already absent"
    );
}

#[tokio::test]
async fn client_payload_too_large() {
    let client = CacheKit::builder()
        .backend(MockBackend::shared())
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
    let err = client
        .get::<String>("")
        .await
        .expect_err("empty key should be rejected");
    assert!(
        matches!(err, CachekitError::InvalidKey(_)),
        "empty key: {err:?}"
    );

    // Control character (newline = 0x0A)
    let err = client
        .get::<String>("bad\nkey")
        .await
        .expect_err("control char key should be rejected");
    assert!(
        matches!(err, CachekitError::InvalidKey(_)),
        "control char: {err:?}"
    );

    // DEL character (0x7F)
    let err = client
        .get::<String>("bad\x7Fkey")
        .await
        .expect_err("DEL char key should be rejected");
    assert!(
        matches!(err, CachekitError::InvalidKey(_)),
        "DEL char: {err:?}"
    );

    // Key that is exactly 1025 bytes (one over the limit)
    let too_long = "a".repeat(1025);
    let err = client
        .get::<String>(&too_long)
        .await
        .expect_err("over-length key should be rejected");
    assert!(
        matches!(err, CachekitError::InvalidKey(_)),
        "too long: {err:?}"
    );

    // Boundary case: exactly 1024 bytes should be accepted
    let client2 = CacheKit::builder()
        .backend(MockBackend::shared())
        .no_l1()
        .build()
        .expect("client builds");
    let at_limit = "a".repeat(1024);
    let result = client2.get::<String>(&at_limit).await;
    assert!(
        result.is_ok(),
        "1024-byte key should be accepted: {result:?}"
    );
}

// ── Interop mode ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn interop_get_round_trips_plain_msgpack() {
    let client = mock_client();
    let key = cachekit::interop::interop_key(
        "users",
        "get_user",
        &[cachekit::interop::InteropValue::from(42i64)],
    )
    .expect("valid interop key");

    let user = User {
        id: 42,
        name: "Alice".to_owned(),
    };
    // Regular set writes plain MessagePack — already the interop value format.
    client.set(&key, &user).await.expect("set succeeds");

    let fetched: Option<User> = client
        .interop_get(&key)
        .await
        .expect("interop_get succeeds");
    assert_eq!(fetched, Some(user));
}

#[tokio::test]
async fn interop_get_rejects_trailing_bytes_that_get_accepts() {
    let (backend, handle) = MockBackend::new_with_handle();
    let client = CacheKit::builder()
        .backend(backend)
        .no_l1()
        .build()
        .expect("client builds");

    // Simulate a corrupt/foreign entry: a valid document plus trailing bytes.
    let mut bytes = rmp_serde::to_vec(&7u8).expect("encode");
    bytes.push(0x00);
    handle
        .store
        .lock()
        .await
        .insert("ns:op:deadbeef".to_owned(), bytes);

    // The lenient auto-mode reader accepts it...
    let lenient: Option<u8> = client.get("ns:op:deadbeef").await.expect("lenient get");
    assert_eq!(lenient, Some(7));

    // ...the interop reader must reject it (spec MUST: exactly one document).
    let err = client
        .interop_get::<u8>("ns:op:deadbeef")
        .await
        .expect_err("interop read must reject trailing bytes");
    assert!(
        err.to_string().contains("trailing"),
        "expected trailing-bytes rejection: {err}"
    );
}

#[tokio::test]
async fn interop_get_fails_closed_on_namespaced_client() {
    // A client namespace prefix would rewrite interop storage keys to
    // {prefix}:{interop_key}, which no other SDK computes — every cross-SDK
    // read would silently miss. interop_get must error instead.
    let client = CacheKit::builder()
        .backend(MockBackend::shared())
        .namespace("app1")
        .no_l1()
        .build()
        .expect("client builds");

    let err = client
        .interop_get::<User>("users:get_user:0000")
        .await
        .expect_err("namespaced client must fail closed for interop reads");
    assert!(
        matches!(err, CachekitError::Config(_)),
        "expected Config error, got: {err:?}"
    );
}
