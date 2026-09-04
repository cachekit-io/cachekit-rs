use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;

use crate::error::BackendError;

// ── HealthStatus ─────────────────────────────────────────────────────────────

/// Describes the health of a backend at a point in time.
#[derive(Debug, Clone)]
pub struct HealthStatus {
    /// Whether the backend is considered healthy.
    pub is_healthy: bool,
    /// Round-trip latency of the health check in milliseconds.
    pub latency_ms: f64,
    /// Human-readable name for this backend implementation.
    pub backend_type: String,
    /// Optional key-value details (pool size, version, etc.).
    pub details: HashMap<String, String>,
}

// ── Backend trait ─────────────────────────────────────────────────────────────

/// Async cache backend abstraction.
///
/// Implementors must be `Send + Sync` on native targets (unless the `unsync`
/// feature is enabled). On `wasm32` targets or with `unsync`, `Send` is relaxed
/// (`?Send`) because the runtime is single-threaded.
#[cfg(not(any(target_arch = "wasm32", feature = "unsync")))]
#[async_trait]
pub trait Backend: Send + Sync {
    /// Retrieve the raw bytes stored under `key`, or `None` if absent.
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError>;

    /// Store `value` under `key`, optionally expiring after `ttl`.
    async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), BackendError>;

    /// Remove `key` and return `true` if it existed.
    async fn delete(&self, key: &str) -> Result<bool, BackendError>;

    /// Return `true` if `key` exists without fetching the value.
    async fn exists(&self, key: &str) -> Result<bool, BackendError>;

    /// Return health/status information for this backend.
    async fn health(&self) -> Result<HealthStatus, BackendError>;

    /// Expose this backend's [`LockableBackend`] capability, if it has one.
    ///
    /// Trait objects (`dyn Backend`) cannot be cross-cast to a sibling trait,
    /// so backends that support distributed locking opt in by overriding this
    /// to return `Some(self)`. Used by the client's cold-miss single-flight
    /// for cross-process fill suppression. Default: `None`.
    fn as_lockable(&self) -> Option<&dyn LockableBackend> {
        None
    }
}

/// Async cache backend abstraction (`?Send` variant).
///
/// Active when compiling for `wasm32` or with the `unsync` feature.
/// Identical API to the `Send + Sync` variant but without thread-safety bounds.
#[cfg(any(target_arch = "wasm32", feature = "unsync"))]
#[async_trait(?Send)]
pub trait Backend {
    /// Retrieve the raw bytes stored under `key`, or `None` if absent.
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError>;

    /// Store `value` under `key`, optionally expiring after `ttl`.
    async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), BackendError>;

    /// Remove `key` and return `true` if it existed.
    async fn delete(&self, key: &str) -> Result<bool, BackendError>;

    /// Return `true` if `key` exists without fetching the value.
    async fn exists(&self, key: &str) -> Result<bool, BackendError>;

    /// Return health/status information for this backend.
    async fn health(&self) -> Result<HealthStatus, BackendError>;

    /// Expose this backend's [`LockableBackend`] capability, if it has one.
    ///
    /// Trait objects (`dyn Backend`) cannot be cross-cast to a sibling trait,
    /// so backends that support distributed locking opt in by overriding this
    /// to return `Some(self)`. Used by the client's cold-miss single-flight
    /// for cross-process fill suppression. Default: `None`.
    fn as_lockable(&self) -> Option<&dyn LockableBackend> {
        None
    }
}

// ── TtlInspectable ───────────────────────────────────────────────────────────

/// Optional extension for backends that can report the remaining TTL of a key.
#[cfg(not(any(target_arch = "wasm32", feature = "unsync")))]
#[async_trait]
pub trait TtlInspectable: Backend {
    /// Return the remaining TTL for `key`, or `None` if the key does not exist
    /// or has no expiry.
    async fn ttl(&self, key: &str) -> Result<Option<Duration>, BackendError>;

    /// Refresh the TTL on an existing key. Default: not supported.
    async fn refresh_ttl(&self, _key: &str, _ttl: Duration) -> Result<bool, BackendError> {
        Err(BackendError::permanent(
            "refresh_ttl not supported by this backend",
        ))
    }
}

/// Optional extension for backends that can report the remaining TTL of a key (`?Send` variant).
#[cfg(any(target_arch = "wasm32", feature = "unsync"))]
#[async_trait(?Send)]
pub trait TtlInspectable: Backend {
    /// Return the remaining TTL for `key`, or `None` if the key does not exist
    /// or has no expiry.
    async fn ttl(&self, key: &str) -> Result<Option<Duration>, BackendError>;

    /// Refresh the TTL on an existing key. Default: not supported.
    async fn refresh_ttl(&self, _key: &str, _ttl: Duration) -> Result<bool, BackendError> {
        Err(BackendError::permanent(
            "refresh_ttl not supported by this backend",
        ))
    }
}

// ── LockableBackend ─────────────────────────────────────────────────────────

/// Optional extension for backends that support distributed locking.
#[cfg(not(any(target_arch = "wasm32", feature = "unsync")))]
#[async_trait]
pub trait LockableBackend: Backend {
    /// Acquire a distributed lock. Returns lock_id if acquired, None if contested.
    async fn acquire_lock(
        &self,
        key: &str,
        timeout_ms: u64,
    ) -> Result<Option<String>, BackendError>;
    /// Release a distributed lock. Returns true if released.
    async fn release_lock(&self, key: &str, lock_id: &str) -> Result<bool, BackendError>;
}

/// Optional extension for backends that support distributed locking (`?Send` variant).
#[cfg(any(target_arch = "wasm32", feature = "unsync"))]
#[async_trait(?Send)]
pub trait LockableBackend: Backend {
    /// Acquire a distributed lock. Returns lock_id if acquired, None if contested.
    async fn acquire_lock(
        &self,
        key: &str,
        timeout_ms: u64,
    ) -> Result<Option<String>, BackendError>;
    /// Release a distributed lock. Returns true if released.
    async fn release_lock(&self, key: &str, lock_id: &str) -> Result<bool, BackendError>;
}

// ── Blocking-pool bridge (file + memcached backends) ─────────────────────────

/// Run sync I/O on tokio's blocking pool so the async executor never stalls.
#[cfg(all(any(feature = "file", feature = "memcached"), not(feature = "unsync")))]
pub(crate) async fn run_blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, crate::error::BackendError> + Send + 'static,
) -> Result<T, crate::error::BackendError> {
    tokio::task::spawn_blocking(f).await.map_err(|e| {
        crate::error::BackendError::permanent(format!("backend blocking task failed: {e}"))
    })?
}

/// `unsync` opts into single-threaded runtimes and drops `Send` from
/// `BackendError`, so results cannot cross `spawn_blocking`. Run the I/O
/// inline instead — the same sync-in-async trade-off cachekit-py documents.
#[cfg(all(any(feature = "file", feature = "memcached"), feature = "unsync"))]
pub(crate) async fn run_blocking<T>(
    f: impl FnOnce() -> Result<T, crate::error::BackendError>,
) -> Result<T, crate::error::BackendError> {
    f()
}

// ── Cache-key path encoding (CWE-22) ─────────────────────────────────────────

/// Percent-encode a cache key for the `/v1/cache/{key}` path segment, shared by
/// the native `cachekitio` and wasm `workers` backends (DRY: every CachekitIO
/// path is built through this one fallible chokepoint).
///
/// Almost every key is just [`urlencoding::encode`]. The exception is a key
/// that is exactly `.` or `..`, which is **rejected** with a permanent
/// [`BackendError`] rather than encoded — because there is no encoding of it
/// that survives the client's URL parser.
///
/// A dot is RFC-3986 *unreserved*, so `urlencoding::encode("..") == ".."`
/// unchanged, and `reqwest`'s WHATWG URL parser (rust-url) then removes that
/// dot-segment **before the request leaves the process**: `/v1/cache/..` →
/// `/v1/`, `…/../ttl` → `…/ttl`, escaping the `/v1/cache/` prefix the SaaS
/// `cache-key-validator` guards and carrying the app's bearer token to a route
/// it never vetted (CWE-22). Percent-encoding does **not** help here: WHATWG
/// treats `%2e` / `%2e%2e` (case-insensitive) as dot-segments too, and rust-url
/// `%2E%2E` → `/v1/` and `%2E` → `/v1/cache/` are both verified to collapse
/// (see the `repro_raw_dot_key_escapes_the_cache_prefix` test). Since every representation that
/// `decodeURIComponent`s once back to `.`/`..` is a WHATWG dot-segment, no
/// encoding can both reach the wire intact **and** round-trip — so the only safe
/// action is to refuse to build the request at all.
///
/// This diverges deliberately from cachekit-py's `_encode_key`
/// (`src/cachekit/backends/cachekitio/backend.py:247-250` @ `f000ba3`), which
/// rewrites to `%2E`: Python's HTTP client applies RFC-3986 `remove_dot_segments`
/// (which does **not** decode `%2e`), so `%2E%2E` survives there and the SaaS
/// rejects the decoded `..`. rust-url is stricter. `.`/`..` is never a canonical
/// CacheKit key (those always contain `:`), so refusing it breaks nothing
/// legitimate. For every **other** key the output is byte-identical to
/// `urlencoding::encode`, preserving cross-SDK wire parity.
#[cfg(any(feature = "cachekitio", feature = "workers", test))]
pub(crate) fn encode_key(key: &str) -> Result<std::borrow::Cow<'_, str>, BackendError> {
    let encoded = urlencoding::encode(key);
    if matches!(encoded.as_ref(), "." | "..") {
        return Err(BackendError::permanent(
            "cache key must not be `.` or `..`: the client URL parser strips an \
             all-dot path segment before the request is sent (CWE-22), so it \
             cannot be addressed on the wire",
        ));
    }
    Ok(encoded)
}

// ── Feature-gated backend modules ─────────────────────────────────────────────

/// JSON wire bodies for the SaaS lock/TTL endpoints. Compiled under `test`
/// unconditionally so the wire-contract round-trip tests always run in CI,
/// even though only the `workers` backend consumes the structs at runtime.
#[cfg(any(feature = "workers", test))]
mod saas_wire;

/// HTTP backend for the cachekit.io SaaS API.
#[cfg(feature = "cachekitio")]
pub mod cachekitio;
#[cfg(feature = "cachekitio")]
mod cachekitio_lock;
#[cfg(feature = "cachekitio")]
mod cachekitio_ttl;

/// Redis backend via the [`fred`](https://crates.io/crates/fred) client.
#[cfg(feature = "redis")]
pub mod redis;

/// Memcached backend via the [`rust-memcache`](https://crates.io/crates/memcache) client.
#[cfg(feature = "memcached")]
pub mod memcached;

/// Local filesystem backend, byte-compatible with cachekit-py's File backend.
#[cfg(feature = "file")]
pub mod file;

/// Cloudflare Workers backend using `worker::Fetch`.
#[cfg(feature = "workers")]
pub mod workers;

// ── encode_key unit tests (CWE-22) ───────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::expect_used)] // test-only: an encoding failure on a safe key should panic loudly
mod encode_key_tests {
    use super::encode_key;

    /// Non-`.`/`..` vectors: canonical key, near-misses (`..` embedded, not a
    /// whole segment), reserved chars, spaces, empty.
    const SAFE_VECTORS: &[&str] = &[
        "a:..",
        "..a",
        "a..",
        ".hidden",
        "default:../../admin",
        "ns:default:func:m.f:args:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef:",
        "a b",
        "k?x=1#f",
        "",
    ];

    #[test]
    fn all_dot_keys_are_rejected() {
        // No encoding of `.`/`..` survives rust-url/WHATWG dot-segment removal
        // (see `cachekitio::path_encoding_tests::dot_keys_are_rejected_by_every_builder`),
        // so the guard refuses to build a request rather than emit one whose path
        // was rewritten (CWE-22).
        assert!(encode_key(".").is_err());
        assert!(encode_key("..").is_err());
    }

    #[test]
    fn safe_keys_are_byte_identical_to_urlencoding() {
        // AC-1: nothing but an exact `.`/`..` segment may change encoding, or the
        // SDKs diverge on the wire.
        for k in SAFE_VECTORS {
            let enc = encode_key(k).expect("safe key must encode");
            assert_eq!(
                enc,
                urlencoding::encode(k),
                "encode_key diverged from urlencoding for {k:?}"
            );
        }
    }

    #[test]
    fn safe_keys_decode_once_back_to_the_original() {
        // AC-3: the SaaS validator does a single `decodeURIComponent`; every safe
        // vector must survive that exact round-trip untouched.
        for k in SAFE_VECTORS {
            let enc = encode_key(k).expect("safe key must encode");
            let decoded = urlencoding::decode(&enc).expect("single decode succeeds");
            assert_eq!(decoded, *k, "decode-once round-trip changed {k:?}");
        }
    }
}
