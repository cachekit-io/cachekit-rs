//! Intent-based cache presets.
//!
//! Pre-configured factory methods that build a [`CacheKit`] client from a
//! single declarative call. Each intent sets sensible defaults for a specific
//! use case and returns a [`CacheKitBuilder`] so callers can override any
//! setting before building.
//!
//! | Intent | Backend | L1 | Encryption | Default TTL |
//! |------------|-----------|------|------------|-------------|
//! | `minimal` | Redis | Off | No | 300 s |
//! | `production` | Redis | On | No | 600 s |
//! | `encrypted` | Redis | On | AES-256-GCM | 600 s |
//! | `io` | cachekit.io | On | No | 3 600 s |

use std::time::Duration;

use crate::client::{CacheKit, CacheKitBuilder, SharedBackend};
use crate::error::CachekitError;

// ── SharedBackend wrapping ───────────────────────────────────────────────────

#[cfg(all(
    feature = "redis",
    not(any(target_arch = "wasm32", feature = "unsync"))
))]
fn wrap_redis(b: crate::backend::redis::RedisBackend) -> SharedBackend {
    std::sync::Arc::new(b)
}

#[cfg(all(feature = "redis", any(target_arch = "wasm32", feature = "unsync")))]
fn wrap_redis(b: crate::backend::redis::RedisBackend) -> SharedBackend {
    std::rc::Rc::new(b)
}

#[cfg(all(
    feature = "cachekitio",
    not(target_arch = "wasm32"),
    not(feature = "unsync")
))]
fn wrap_cachekitio(b: crate::backend::cachekitio::CachekitIO) -> SharedBackend {
    std::sync::Arc::new(b)
}

#[cfg(all(
    feature = "cachekitio",
    not(target_arch = "wasm32"),
    feature = "unsync"
))]
fn wrap_cachekitio(b: crate::backend::cachekitio::CachekitIO) -> SharedBackend {
    std::rc::Rc::new(b)
}

// ── Intent presets ───────────────────────────────────────────────────────────

impl CacheKit {
    /// **Minimal** — speed-first Redis cache, no extras.
    ///
    /// * Backend: Redis (connects eagerly)
    /// * L1 cache: **off**
    /// * Encryption: **no**
    /// * Default TTL: **300 s**
    ///
    /// Good for: product catalogs, public data, development.
    ///
    /// # Errors
    ///
    /// Returns [`CachekitError`] if the URL is invalid or Redis is unreachable.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), cachekit::CachekitError> {
    /// let cache = cachekit::CacheKit::minimal("redis://localhost:6379").await?
    ///     .namespace("myapp")
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "redis")]
    pub async fn minimal(redis_url: &str) -> Result<CacheKitBuilder, CachekitError> {
        let backend = crate::backend::redis::RedisBackend::builder()
            .url(redis_url)
            .build()?;
        drop(backend.connect().await?);

        Ok(CacheKitBuilder::default()
            .backend(wrap_redis(backend))
            .default_ttl(Duration::from_secs(300))
            .no_l1())
    }

    /// **Production** — reliability-first Redis cache with L1.
    ///
    /// * Backend: Redis (connects eagerly)
    /// * L1 cache: **on** (1 000 entries)
    /// * Encryption: **no**
    /// * Default TTL: **600 s**
    ///
    /// Good for: user sessions, API responses, production services.
    ///
    /// # Errors
    ///
    /// Returns [`CachekitError`] if the URL is invalid or Redis is unreachable.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), cachekit::CachekitError> {
    /// let cache = cachekit::CacheKit::production("redis://localhost:6379").await?
    ///     .namespace("api")
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "redis")]
    pub async fn production(redis_url: &str) -> Result<CacheKitBuilder, CachekitError> {
        let backend = crate::backend::redis::RedisBackend::builder()
            .url(redis_url)
            .build()?;
        drop(backend.connect().await?);

        Ok(CacheKitBuilder::default()
            .backend(wrap_redis(backend))
            .default_ttl(Duration::from_secs(600))
            .l1_capacity(1000))
    }

    /// **Encrypted** — zero-knowledge encrypted Redis cache.
    ///
    /// * Backend: Redis (connects eagerly)
    /// * L1 cache: **on** (1 000 entries, stores ciphertext)
    /// * Encryption: **AES-256-GCM** with HKDF-SHA256
    /// * Default TTL: **600 s**
    /// * Tenant ID: `"default"` (override via
    ///   [`.encryption_from_bytes()`](CacheKitBuilder::encryption_from_bytes))
    ///
    /// Good for: PII, payments, GDPR/HIPAA-sensitive data.
    ///
    /// `master_key` must be at least 16 raw bytes (32 recommended).
    ///
    /// # Errors
    ///
    /// Returns [`CachekitError`] if the URL is invalid, Redis is unreachable,
    /// or the master key is too short.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), cachekit::CachekitError> {
    /// let key = b"my_32_byte_production_key_here!!";
    /// let cache = cachekit::CacheKit::encrypted("redis://localhost:6379", key).await?
    ///     .build()?;
    /// let encrypted = cache.secure()?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(all(feature = "redis", feature = "encryption"))]
    pub async fn encrypted(
        redis_url: &str,
        master_key: &[u8],
    ) -> Result<CacheKitBuilder, CachekitError> {
        let backend = crate::backend::redis::RedisBackend::builder()
            .url(redis_url)
            .build()?;
        drop(backend.connect().await?);

        CacheKitBuilder::default()
            .backend(wrap_redis(backend))
            .default_ttl(Duration::from_secs(600))
            .l1_capacity(1000)
            .encryption_from_bytes(master_key, "default")
    }

    /// **CachekitIO** — managed SaaS cache, zero infrastructure.
    ///
    /// * Backend: [cachekit.io](https://cachekit.io) HTTP API
    /// * L1 cache: **on** (1 000 entries)
    /// * Encryption: **no** (add via
    ///   [`.encryption()`](CacheKitBuilder::encryption))
    /// * Default TTL: **3 600 s**
    ///
    /// Good for: serverless, edge compute, managed caching without Redis.
    ///
    /// # Errors
    ///
    /// Returns [`CachekitError`] if `api_key` is empty.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # fn example() -> Result<(), cachekit::CachekitError> {
    /// let cache = cachekit::CacheKit::io("ck_live_abc123")?
    ///     .namespace("edge")
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(all(feature = "cachekitio", not(target_arch = "wasm32")))]
    pub fn io(api_key: &str) -> Result<CacheKitBuilder, CachekitError> {
        let backend = crate::backend::cachekitio::CachekitIO::builder()
            .api_key(api_key)
            .build()?;

        Ok(CacheKitBuilder::default()
            .backend(wrap_cachekitio(backend))
            .default_ttl(Duration::from_secs(3600))
            .l1_capacity(1000))
    }
}
