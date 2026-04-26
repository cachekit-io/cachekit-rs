//! Tests for intent-based cache presets.
//!
//! The async intents (minimal, production, encrypted) require a Redis
//! connection, so we test parameter validation through the underlying
//! builders. The sync io() intent can be tested end-to-end.
//!
//! Run with:
//!   cargo test --test intent_tests --features redis,encryption,cachekitio,l1

mod common;

use std::time::Duration;

// ── minimal / production (Redis URL validation) ──────────────────────────────

#[cfg(feature = "redis")]
mod redis_intents {
    #[test]
    fn accepts_valid_url() {
        let backend = cachekit::backend::redis::RedisBackend::builder()
            .url("redis://localhost:6379")
            .build();
        assert!(backend.is_ok());
    }

    #[test]
    fn accepts_url_with_password() {
        let backend = cachekit::backend::redis::RedisBackend::builder()
            .url("redis://:secret@host:6379/0")
            .build();
        assert!(backend.is_ok());
    }

    #[test]
    fn rejects_empty_url() {
        let backend = cachekit::backend::redis::RedisBackend::builder()
            .url("")
            .build();
        assert!(backend.is_err());
    }
}

// ── encrypted (encryption key validation) ────────────────────────────────────

#[cfg(all(feature = "redis", feature = "encryption"))]
mod encrypted_intent {
    use crate::common::MockBackend;

    #[test]
    fn rejects_short_master_key() {
        let result = cachekit::CacheKitBuilder::default()
            .backend(MockBackend::shared())
            .encryption_from_bytes(b"too_short", "tenant");
        assert!(
            result.is_err(),
            "master key under 16 bytes must be rejected"
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
