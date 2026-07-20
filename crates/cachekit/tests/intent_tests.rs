//! Tests for intent-based cache presets.
//!
//! The async Redis intents (minimal, production, encrypted) connect eagerly,
//! so their success paths need a live Redis and are not tested here. Their
//! error paths (invalid URL, invalid key) are deterministic and local — those
//! are exercised through the public factories directly. The sync io() intent
//! can be tested end-to-end.
//!
//! Run with:
//!   cargo test --test intent_tests --features redis,encryption,cachekitio,l1

mod common;

use std::time::Duration;

// ── minimal / production (factory URL validation, no network) ────────────────

#[cfg(feature = "redis")]
mod redis_intents {
    use cachekit::error::CachekitError;
    use cachekit::CacheKit;

    #[tokio::test]
    async fn minimal_rejects_empty_url() {
        assert!(
            matches!(CacheKit::minimal("").await, Err(CachekitError::Config(_))),
            "empty URL must be a config error, raised before any connection attempt"
        );
    }

    #[tokio::test]
    async fn minimal_rejects_invalid_url() {
        assert!(matches!(
            CacheKit::minimal("not-a-redis-url").await,
            Err(CachekitError::Config(_))
        ));
    }

    #[tokio::test]
    async fn production_rejects_empty_url() {
        assert!(matches!(
            CacheKit::production("").await,
            Err(CachekitError::Config(_))
        ));
    }

    #[tokio::test]
    async fn production_rejects_invalid_url() {
        assert!(matches!(
            CacheKit::production("not-a-redis-url").await,
            Err(CachekitError::Config(_))
        ));
    }

    // Positive URL parsing stays at the builder level: the factories connect
    // eagerly, so a valid-URL factory test would require a live Redis.
    #[test]
    fn builder_accepts_valid_url() {
        let backend = cachekit::backend::redis::RedisBackend::builder()
            .url("redis://localhost:6379")
            .build();
        assert!(backend.is_ok());
    }

    #[test]
    fn builder_accepts_url_with_password() {
        let backend = cachekit::backend::redis::RedisBackend::builder()
            .url("redis://:secret@host:6379/0")
            .build();
        assert!(backend.is_ok());
    }
}

// ── encrypted (factory key validation, no network) ───────────────────────────

#[cfg(all(feature = "redis", feature = "encryption"))]
mod encrypted_intent {
    use crate::common::MockBackend;
    use cachekit::error::CachekitError;
    use cachekit::CacheKit;

    #[tokio::test]
    async fn rejects_short_master_key_before_connecting() {
        // The URL points at an unreachable Redis on purpose: key validation
        // must fire first, so we get the deterministic Encryption error —
        // never a Backend (connection) error.
        let result = CacheKit::encrypted("redis://127.0.0.1:1", b"too_short").await;
        assert!(
            matches!(result, Err(CachekitError::Encryption(_))),
            "short master key must be rejected before any Redis I/O"
        );
    }

    #[test]
    fn accepts_valid_master_key() {
        let result = cachekit::CacheKitBuilder::default()
            .backend(MockBackend::shared())
            .encryption_from_bytes(b"test_master_key_32_bytes_long!!!", "tenant");
        assert!(result.is_ok());
    }
}

// ── io (full end-to-end, no network needed) ──────────────────────────────────

#[cfg(all(feature = "cachekitio", not(target_arch = "wasm32")))]
mod io_intent {
    use super::*;
    use cachekit::CacheKit;

    #[test]
    fn builds_with_valid_key() {
        let builder = CacheKit::io("ck_live_test123");
        assert!(builder.is_ok());
        assert!(builder.unwrap().build().is_ok());
    }

    #[test]
    fn rejects_empty_key() {
        assert!(CacheKit::io("").is_err());
    }

    #[test]
    fn allows_overrides() {
        let cache = CacheKit::io("ck_live_test123")
            .unwrap()
            .default_ttl(Duration::from_secs(60))
            .namespace("test")
            .no_l1()
            .build();
        assert!(cache.is_ok());
    }
}
