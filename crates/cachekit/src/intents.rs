//! Intent-based cache presets.
//!
//! Pre-configured factory methods that build a [`CacheKit`] client from a
//! single declarative call. Each intent sets sensible defaults for a specific
//! use case and returns a [`CacheKitBuilder`] so callers can override any
//! setting before building.
//!
//! The preset matrix and per-intent resilience contract live in the
//! crate-level docs (`lib.rs`) — the single rustdoc-rendered copy. This
//! module is private, so docs here reach source readers only; the per-method
//! docs below are what docs.rs renders.
//!
//! Reliability defaults come from
//! [`ReliabilityConfig::default()`](crate::reliability::ReliabilityConfig)
//! (requires the `reliability` feature, on by default). Override via
//! [`CacheKitBuilder::reliability`];
//! [`ReliabilityConfig::disabled()`](crate::reliability::ReliabilityConfig::disabled)
//! turns the stack off entirely.

use std::time::Duration;

use crate::client::{CacheKit, CacheKitBuilder, SharedBackend};
use crate::error::CachekitError;

// ── SharedBackend wrapping ───────────────────────────────────────────────────

#[cfg(not(any(target_arch = "wasm32", feature = "unsync")))]
fn wrap(b: impl crate::backend::Backend + 'static) -> SharedBackend {
    std::sync::Arc::new(b)
}

#[cfg(any(target_arch = "wasm32", feature = "unsync"))]
fn wrap(b: impl crate::backend::Backend + 'static) -> SharedBackend {
    std::rc::Rc::new(b)
}

// ── Intent presets ───────────────────────────────────────────────────────────

/// `minimal` builder defaults, split from the eager Redis connect so the
/// L1-on / SWR-off contract (`protocol/spec/intent-presets.md` § L1 Posture)
/// is unit-testable without a live server.
#[cfg(feature = "redis")]
fn minimal_defaults(backend: SharedBackend) -> CacheKitBuilder {
    let builder = CacheKitBuilder::default()
        .backend(backend)
        .default_ttl(Duration::from_secs(300))
        .l1_capacity(1000);
    // Builder default is SWR-on; spec § L1 Posture says minimal MUST NOT.
    #[cfg(all(feature = "l1", not(feature = "unsync"), not(target_arch = "wasm32")))]
    let builder = builder.swr_enabled(false);
    builder
}

/// `secure` builder defaults and key resolution, split from the eager Redis
/// connect so the key path is unit-testable without a live server. `source`
/// names the key's origin in error messages.
#[cfg(all(feature = "redis", feature = "encryption"))]
fn secure_defaults(master_key_hex: &str, source: &str) -> Result<CacheKitBuilder, CachekitError> {
    let master_key = crate::config::decode_master_key_hex(master_key_hex, source)?;
    let builder = CacheKitBuilder::default()
        .default_ttl(Duration::from_secs(600))
        .l1_capacity(1000)
        .encryption_from_bytes(&master_key, "default")?;
    #[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
    let builder = builder.reliability(crate::reliability::ReliabilityConfig::default());
    Ok(builder)
}

/// `CACHEKIT_MASTER_KEY` for [`CacheKit::secure_from_env`]; unset and empty
/// are both an error, never plaintext.
#[cfg(all(feature = "redis", feature = "encryption"))]
fn master_key_hex_from_env() -> Result<zeroize::Zeroizing<String>, CachekitError> {
    std::env::var("CACHEKIT_MASTER_KEY")
        .ok()
        .map(zeroize::Zeroizing::new)
        .filter(|k| !k.is_empty())
        .ok_or_else(|| {
            CachekitError::Config(
                "CACHEKIT_MASTER_KEY is unset or empty: set it to a 64-hex-char key or pass \
                 the key to CacheKit::secure(url, master_key_hex)"
                    .to_owned(),
            )
        })
}

/// Attach an eagerly connected, auto-reconnecting Redis backend to a
/// `secure` builder. Takes the builder, not the key, so key validation has
/// already happened: a bad key must not be masked by (or pay for) Redis I/O.
#[cfg(all(feature = "redis", feature = "encryption"))]
async fn connect_secure(
    redis_url: &str,
    builder: CacheKitBuilder,
) -> Result<CacheKitBuilder, CachekitError> {
    let backend = crate::backend::redis::RedisBackend::builder()
        .url(redis_url)
        .auto_reconnect()
        .build()?;
    drop(backend.connect().await?);
    Ok(builder.backend(wrap(backend)))
}

impl CacheKit {
    /// **Minimal** — speed-first Redis cache, no extras.
    ///
    /// * Backend: Redis (connects eagerly; **fails fast** — a dropped
    ///   connection is not re-established)
    /// * L1 cache: **on** (1 000 entries, **no SWR / invalidation**) — an L1
    ///   hit is served as-is until it expires, so a read may return an entry
    ///   up to its TTL (300 s by default) after another process changed it
    ///   in Redis. Chain
    ///   [`.no_l1()`](CacheKitBuilder::no_l1) to read through every time.
    /// * Encryption: **no**
    /// * Reliability: **off** — no retry, no circuit breaker, no
    ///   backpressure; every backend error propagates on first failure and
    ///   backend concurrency is unbounded
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

        Ok(minimal_defaults(wrap(backend)))
    }

    /// **Production** — reliability-first Redis cache with L1.
    ///
    /// * Backend: Redis (connects eagerly, failing fast if unreachable;
    ///   **auto-reconnects** after a dropped connection with exponential
    ///   backoff, 100 ms → 30 s, retrying indefinitely)
    /// * L1 cache: **on** (1 000 entries)
    /// * Encryption: **no**
    /// * Reliability: **on** — retry with backoff + jitter, circuit
    ///   breaker, backpressure (max 100 concurrent backend ops)
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
            .auto_reconnect()
            .build()?;
        drop(backend.connect().await?);

        let builder = CacheKitBuilder::default()
            .backend(wrap(backend))
            .default_ttl(Duration::from_secs(600))
            .l1_capacity(1000);
        #[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
        let builder = builder.reliability(crate::reliability::ReliabilityConfig::default());
        Ok(builder)
    }

    /// **Secure** — zero-knowledge encrypted Redis cache.
    ///
    /// * Backend: Redis (connects eagerly, failing fast if unreachable;
    ///   **auto-reconnects** after a dropped connection with exponential
    ///   backoff, 100 ms → 30 s, retrying indefinitely)
    /// * L1 cache: **on** (1 000 entries, stores ciphertext)
    /// * Encryption: **AES-256-GCM** with HKDF-SHA256
    /// * Reliability: **on** — retry with backoff + jitter, circuit
    ///   breaker, backpressure (max 100 concurrent backend ops)
    /// * Default TTL: **600 s**
    /// * Tenant ID: `"default"` for both key derivation and AAD (override via
    ///   [`.encryption()`](CacheKitBuilder::encryption))
    ///
    /// Good for: PII, payments, GDPR/HIPAA-sensitive data.
    ///
    /// `master_key_hex` is the master key as a **hex string** — the same
    /// string `CACHEKIT_MASTER_KEY` holds, decoded to the same key bytes by
    /// every CacheKit SDK. This is the cross-SDK-portable key path. Use
    /// exactly **32 bytes (64 hex chars)**: shorter is rejected, and longer
    /// is accepted here but not by every SDK. Generate one with
    /// `openssl rand -hex 32`.
    ///
    /// Use [`CacheKit::secure_from_env`] to read the key from
    /// `CACHEKIT_MASTER_KEY`. There is no raw-bytes preset: decoded key bytes
    /// go to [`CacheKitBuilder::encryption_from_bytes`], and the ASCII bytes
    /// of a hex string must never go there — they derive a key no other SDK
    /// derives.
    ///
    /// The key is validated **before** any Redis connection is attempted — a
    /// bad key is a deterministic local error, never masked by (or paying
    /// for) network I/O.
    ///
    /// # Errors
    ///
    /// Returns [`CachekitError::Config`] if `master_key_hex` is not hex or
    /// decodes to fewer than 32 bytes, or if the URL is invalid; another
    /// [`CachekitError`] if Redis is unreachable.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// // 64 hex chars (32 bytes) from your secret store — `openssl rand -hex 32`.
    /// let master_key_hex = std::env::var("APP_CACHE_MASTER_KEY")?;
    /// let cache = cachekit::CacheKit::secure("redis://localhost:6379", &master_key_hex)
    ///     .await?
    ///     .build()?;
    /// let secure = cache.secure_cache()?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(all(feature = "redis", feature = "encryption"))]
    pub async fn secure(
        redis_url: &str,
        master_key_hex: &str,
    ) -> Result<CacheKitBuilder, CachekitError> {
        connect_secure(redis_url, secure_defaults(master_key_hex, "master_key")?).await
    }

    /// **Secure**, master key from the environment — [`CacheKit::secure`]
    /// with the hex key read from `CACHEKIT_MASTER_KEY`.
    ///
    /// Identical preset to [`secure`](CacheKit::secure), same hex decoding,
    /// so the same `CACHEKIT_MASTER_KEY` value derives the same key bytes as
    /// every other CacheKit SDK. Never falls back to plaintext.
    ///
    /// # Errors
    ///
    /// Returns [`CachekitError::Config`] when `CACHEKIT_MASTER_KEY` is unset,
    /// empty, not hex, or shorter than 32 bytes — all before any Redis I/O.
    /// Otherwise as [`secure`](CacheKit::secure).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), cachekit::CachekitError> {
    /// let cache = cachekit::CacheKit::secure_from_env("redis://localhost:6379")
    ///     .await?
    ///     .build()?;
    /// let secure = cache.secure_cache()?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(all(feature = "redis", feature = "encryption"))]
    pub async fn secure_from_env(redis_url: &str) -> Result<CacheKitBuilder, CachekitError> {
        let master_key_hex = master_key_hex_from_env()?;
        connect_secure(
            redis_url,
            secure_defaults(&master_key_hex, "CACHEKIT_MASTER_KEY")?,
        )
        .await
    }

    /// **CachekitIO** — managed SaaS cache, zero infrastructure.
    ///
    /// * Backend: [cachekit.io](https://cachekit.io) HTTP API
    /// * L1 cache: **on** (1 000 entries)
    /// * Encryption: **no** (add via
    ///   [`.encryption()`](CacheKitBuilder::encryption))
    /// * Reliability: **on** — retry with backoff + jitter, circuit
    ///   breaker, backpressure (max 100 concurrent backend ops)
    /// * Default TTL: **3 600 s**
    ///
    /// Good for: serverless, edge compute, managed caching without Redis.
    ///
    /// Use [`CacheKit::io_from_env`] to read the key from `CACHEKIT_API_KEY`.
    ///
    /// # Errors
    ///
    /// Returns [`CachekitError::Config`] if `api_key` is empty.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let api_key = std::env::var("CACHEKIT_API_KEY")?;
    /// let cache = cachekit::CacheKit::io(&api_key)?
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

        let builder = CacheKitBuilder::default()
            .backend(wrap(backend))
            .default_ttl(Duration::from_secs(3600))
            .l1_capacity(1000);
        #[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
        let builder = builder.reliability(crate::reliability::ReliabilityConfig::default());
        Ok(builder)
    }

    /// **CachekitIO**, API key from the environment — [`CacheKit::io`] with
    /// the key read from `CACHEKIT_API_KEY`.
    ///
    /// Identical preset to [`io`](CacheKit::io). Reads **only**
    /// `CACHEKIT_API_KEY` — unlike [`CacheKit::from_env`], never
    /// `CACHEKIT_MASTER_KEY`, so no encryption is activated.
    ///
    /// # Errors
    ///
    /// Returns [`CachekitError::Config`] when `CACHEKIT_API_KEY` is unset or
    /// empty.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # fn example() -> Result<(), cachekit::CachekitError> {
    /// let cache = cachekit::CacheKit::io_from_env()?
    ///     .namespace("edge")
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(all(feature = "cachekitio", not(target_arch = "wasm32")))]
    pub fn io_from_env() -> Result<CacheKitBuilder, CachekitError> {
        let api_key = std::env::var("CACHEKIT_API_KEY")
            .ok()
            .map(zeroize::Zeroizing::new)
            .filter(|k| !k.is_empty())
            .ok_or_else(|| {
                CachekitError::Config(
                    "CACHEKIT_API_KEY is unset or empty: set it or pass the key to CacheKit::io(api_key)"
                        .to_owned(),
                )
            })?;
        Self::io(&api_key)
    }
}

#[cfg(all(test, feature = "redis", feature = "l1"))]
#[allow(clippy::expect_used)] // test-only: a failing build here should panic loudly
mod tests {
    /// `RedisBackend::build()` does not connect, so the preset's defaults
    /// are observable without a live server.
    #[test]
    fn minimal_enables_l1_without_swr() {
        let backend = crate::backend::redis::RedisBackend::builder()
            .url("redis://localhost:6379")
            .build()
            .expect("valid URL");
        let cache = super::minimal_defaults(super::wrap(backend))
            .build()
            .expect("minimal defaults must build");

        assert!(
            cache.l1.is_some(),
            "spec/intent-presets.md § L1 Posture: minimal MUST enable L1"
        );
        #[cfg(all(not(feature = "unsync"), not(target_arch = "wasm32")))]
        assert!(
            !cache.swr_enabled,
            "spec/intent-presets.md § L1 Posture: minimal MUST NOT enable SWR"
        );
    }
}

#[cfg(all(test, feature = "redis", feature = "encryption"))]
#[allow(clippy::expect_used)] // test-only: a failing build here should panic loudly
mod secure_tests {
    // protocol test-vectors/encryption.json v1.2.0, `default_tenant.vectors[0]`
    // (`default_tenant_interop`): spec/intent-presets.md § Master Key Input
    // rule 5 — with no tenant configured, the preset must derive and bind AAD
    // under the literal "default", byte-for-byte with every other SDK.
    const MASTER_KEY_HEX: &str = "6161616161616161616161616161616161616161616161616161616161616161";
    const CACHE_KEY: &str =
        "users:get_user:61598716255080080f6456eb065c2e51badfaa4320b0efe97469c29cffee8875";
    const CIPHERTEXT_HEX: &str =
        "0d0e0f1011121314151617183c29af318238925ee76d081934adce133c0b4a7c5eb5704102b04582dcbf278ffd"; // pragma: allowlist secret
    const PLAINTEXT_HEX: &str = "82a36167651ea46e616d65a5616c696365"; // pragma: allowlist secret

    fn assert_decrypts_default_tenant_vector(builder: crate::CacheKitBuilder) {
        let layer = builder
            .encryption
            .expect("secure preset must configure encryption");
        assert_eq!(layer.tenant_id(), "default");
        let ciphertext = hex::decode(CIPHERTEXT_HEX).expect("vector hex");
        let plaintext = layer
            .decrypt(&ciphertext, CACHE_KEY)
            .expect("default-tenant vector must decrypt");
        assert_eq!(hex::encode(plaintext), PLAINTEXT_HEX);
    }

    #[test]
    fn hex_path_decrypts_default_tenant_vector() {
        assert_decrypts_default_tenant_vector(
            super::secure_defaults(MASTER_KEY_HEX, "master_key").expect("valid key"),
        );
    }

    #[test]
    #[serial_test::serial]
    fn env_path_decrypts_default_tenant_vector() {
        let saved = std::env::var_os("CACHEKIT_MASTER_KEY");
        std::env::set_var("CACHEKIT_MASTER_KEY", MASTER_KEY_HEX);
        let resolved = super::master_key_hex_from_env();
        // Restore before anything can panic.
        match saved {
            Some(v) => std::env::set_var("CACHEKIT_MASTER_KEY", v),
            None => std::env::remove_var("CACHEKIT_MASTER_KEY"),
        }
        let master_key_hex = resolved.expect("CACHEKIT_MASTER_KEY is set");
        assert_decrypts_default_tenant_vector(
            super::secure_defaults(&master_key_hex, "CACHEKIT_MASTER_KEY").expect("valid key"),
        );
    }
}
