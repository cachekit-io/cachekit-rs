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

// ── Write TTL bounds (spec/saas-api.md, TTL Validation Rules) ────────────────

/// A positive sub-second TTL is accepted (the wire ceils it to 1 s); zero is
/// still a `Config` error.
#[tokio::test]
async fn set_with_ttl_accepts_sub_second_and_rejects_zero() {
    let client = mock_client();

    client
        .set_with_ttl("k", &1u8, Duration::from_millis(500))
        .await
        .expect("a 500 ms TTL must be accepted");

    let err = client
        .set_with_ttl("k", &1u8, Duration::ZERO)
        .await
        .expect_err("a zero TTL must be rejected");
    assert!(
        matches!(err, CachekitError::Config(_)),
        "expected Config error, got: {err:?}"
    );
}

// ── L1 backfill honours the server's freshness (spec/saas-api.md) ─────────────

/// A stale label or `Fresh-For: 0` forbids the L1 backfill; a present
/// `Fresh-For` bounds it. Every client here runs the default reliability stack,
/// so the reads cross `ReliableBackend` exactly as the presets' reads do.
#[cfg(all(
    feature = "l1",
    feature = "reliability",
    not(feature = "unsync"),
    not(target_arch = "wasm32")
))]
mod freshness {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;

    use crate::common::MockBackend;
    use cachekit::backend::{Backend, Freshness, HealthStatus};
    use cachekit::error::BackendError;
    use cachekit::reliability::ReliabilityConfig;
    use cachekit::{CacheKit, SharedBackend, SwrRead};

    /// L2 double that labels every hit with a scripted [`Freshness`] and
    /// counts the reads that reach it.
    #[derive(Default)]
    struct LabelledBackend {
        store: MockBackend,
        freshness: Mutex<Freshness>,
        reads: AtomicUsize,
    }

    impl LabelledBackend {
        fn labelled(freshness: Freshness) -> Arc<Self> {
            Arc::new(Self {
                freshness: Mutex::new(freshness),
                ..Self::default()
            })
        }

        fn reads(&self) -> usize {
            self.reads.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl Backend for LabelledBackend {
        async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.store.get(key).await
        }

        async fn get_with_freshness(
            &self,
            key: &str,
        ) -> Result<Option<(Vec<u8>, Freshness)>, BackendError> {
            let freshness = *self.freshness.lock().unwrap();
            Ok(self.get(key).await?.map(|bytes| (bytes, freshness)))
        }

        async fn set(
            &self,
            key: &str,
            value: Vec<u8>,
            ttl: Option<Duration>,
        ) -> Result<(), BackendError> {
            self.store.set(key, value, ttl).await
        }

        async fn delete(&self, key: &str) -> Result<bool, BackendError> {
            self.store.delete(key).await
        }

        async fn exists(&self, key: &str) -> Result<bool, BackendError> {
            self.store.exists(key).await
        }

        async fn health(&self) -> Result<HealthStatus, BackendError> {
            Ok(HealthStatus {
                is_healthy: true,
                latency_ms: 0.0,
                backend_type: "labelled".to_owned(),
                details: HashMap::new(),
            })
        }
    }

    /// A 300 s default TTL, so only the 30 s cap or `Fresh-For` can bound the
    /// backfill. The value is seeded through an L1-less client, so the reader's
    /// L1 starts empty and its first read is an L2 hit.
    async fn reader_over(backend: &Arc<LabelledBackend>) -> CacheKit {
        let shared: SharedBackend = backend.clone();
        let writer = CacheKit::builder()
            .backend(shared.clone())
            .no_l1()
            .build()
            .expect("writer builds");
        writer.set("k", &"v".to_owned()).await.expect("seed L2");

        let reader = CacheKit::builder()
            .backend(shared)
            .default_ttl(Duration::from_secs(300))
            .reliability(ReliabilityConfig::default())
            .build()
            .expect("reader builds");
        assert!(
            reader.circuit_state().is_some(),
            "reads must cross the ReliableBackend"
        );
        reader
    }

    async fn read(cache: &CacheKit) -> Option<String> {
        cache.get::<String>("k").await.expect("read succeeds")
    }

    /// (a) The caller still gets stale bytes, but the L1 keeps no copy, even
    /// when a buggy server pairs the stale label with a positive bound.
    #[tokio::test]
    async fn stale_read_is_served_but_not_backfilled() {
        let backend = LabelledBackend::labelled(Freshness {
            is_stale: true,
            fresh_for: Some(Duration::from_secs(60)),
        });
        let cache = reader_over(&backend).await;

        assert_eq!(read(&cache).await.as_deref(), Some("v"));
        assert_eq!(cache.l1_entry_count(), Some(0), "stale read backfilled L1");
        assert_eq!(read(&cache).await.as_deref(), Some("v"));
        assert_eq!(backend.reads(), 2, "the next read must reach L2");
    }

    /// (b) `Fresh-For: 0` on a fresh label forbids the backfill too.
    #[tokio::test]
    async fn zero_fresh_for_is_not_backfilled() {
        let backend = LabelledBackend::labelled(Freshness {
            is_stale: false,
            fresh_for: Some(Duration::ZERO),
        });
        let cache = reader_over(&backend).await;

        assert_eq!(read(&cache).await.as_deref(), Some("v"));
        assert_eq!(
            cache.l1_entry_count(),
            Some(0),
            "Fresh-For: 0 backfilled L1"
        );
        assert_eq!(read(&cache).await.as_deref(), Some("v"));
        assert_eq!(backend.reads(), 2, "the next read must reach L2");
    }

    /// (c) `Fresh-For: 1` bounds the backfill to 1 s although the cap is 30 s,
    /// and the SWR path cannot serve the copy once that bound has passed.
    /// Real time: moka's clock is `std::time::Instant`, which tokio cannot
    /// pause.
    #[tokio::test]
    async fn fresh_for_bounds_the_backfill_and_its_swr_service() {
        let backend = LabelledBackend::labelled(Freshness {
            is_stale: false,
            fresh_for: Some(Duration::from_secs(1)),
        });
        let cache = reader_over(&backend).await;

        assert_eq!(read(&cache).await.as_deref(), Some("v"));
        assert_eq!(read(&cache).await.as_deref(), Some("v"));
        assert_eq!(
            backend.reads(),
            1,
            "a bounded backfill still serves from L1"
        );

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(
            cache.l1_entry_count(),
            Some(0),
            "L1 kept the copy past Fresh-For"
        );
        let swr = cache
            .interop_get_swr::<String>("k")
            .await
            .expect("read succeeds");
        assert_eq!(
            swr,
            SwrRead::Fresh("v".to_owned()),
            "served from L2, not L1"
        );
        assert_eq!(backend.reads(), 2, "the SWR read must reach L2");
    }

    /// The writer's own L1 copy lives no longer than the TTL the write sent:
    /// 1.5 s goes on the wire as 1 s, so by 1.2 s the next read reaches L2.
    /// Real time, as above.
    #[tokio::test]
    async fn write_through_is_bounded_by_the_wire_ttl() {
        let backend = LabelledBackend::labelled(Freshness::default());
        let shared: SharedBackend = backend.clone();
        let cache = CacheKit::builder()
            .backend(shared)
            .build()
            .expect("client builds");

        cache
            .set_with_ttl("k", &"v".to_owned(), Duration::from_millis(1500))
            .await
            .expect("write succeeds");
        assert_eq!(read(&cache).await.as_deref(), Some("v"));
        assert_eq!(backend.reads(), 0, "a fresh write-through serves from L1");

        tokio::time::sleep(Duration::from_millis(1200)).await;
        assert_eq!(read(&cache).await.as_deref(), Some("v"));
        assert_eq!(backend.reads(), 1, "L1 kept the copy past the wire TTL");
    }

    /// The other half of `min(local_ttl, wire)`: a 500 ms write goes on the
    /// wire as 1 s, but its L1 copy keeps 500 ms, so by 700 ms the next read
    /// reaches L2. Real time, as above.
    #[tokio::test]
    async fn sub_second_write_through_keeps_the_local_ttl() {
        let backend = LabelledBackend::labelled(Freshness::default());
        let shared: SharedBackend = backend.clone();
        let cache = CacheKit::builder()
            .backend(shared)
            .build()
            .expect("client builds");

        cache
            .set_with_ttl("k", &"v".to_owned(), Duration::from_millis(500))
            .await
            .expect("write succeeds");
        assert_eq!(read(&cache).await.as_deref(), Some("v"));
        assert_eq!(backend.reads(), 0, "a fresh write-through serves from L1");

        tokio::time::sleep(Duration::from_millis(700)).await;
        assert_eq!(read(&cache).await.as_deref(), Some("v"));
        assert_eq!(backend.reads(), 1, "L1 kept the copy past the local TTL");
    }

    /// (e) No header leaves today's behaviour: the hit is backfilled (the 30 s
    /// cap itself is pinned by `client::backfill_ttl_tests`).
    #[tokio::test]
    async fn absent_header_still_backfills() {
        let backend = LabelledBackend::labelled(Freshness::default());
        let cache = reader_over(&backend).await;

        assert_eq!(read(&cache).await.as_deref(), Some("v"));
        assert_eq!(read(&cache).await.as_deref(), Some("v"));
        assert_eq!(backend.reads(), 1, "the second read must be an L1 hit");
    }
}
