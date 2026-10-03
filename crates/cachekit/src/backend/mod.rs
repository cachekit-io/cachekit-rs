use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;

use crate::error::BackendError;
use crate::metrics::MetricsProvider;

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

// ── Freshness ────────────────────────────────────────────────────────────────

/// The server's freshness for one read: the protocol's `X-CacheKit-Freshness`
/// label and `X-CacheKit-Fresh-For` bound (`spec/saas-api.md` § Remaining
/// Freshness). The client reads it to bound the L1 backfill.
///
/// The default is a fresh read with no server bound, which leaves the client's
/// configured local TTL unchanged.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Freshness {
    /// The read was labelled stale, or with a token the client does not know.
    /// The L1 must not backfill it.
    pub is_stale: bool,
    /// Remaining server freshness, or `None` when the header was absent. Zero
    /// forbids the backfill.
    pub fresh_for: Option<Duration>,
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

    /// Retrieve `key` together with the server's [`Freshness`] for it.
    ///
    /// Client plumbing for the L1 backfill, not part of the documented surface.
    /// The default wraps [`Self::get`] as a fresh read with no bound, so a
    /// backend without a freshness signal keeps today's behaviour. A backend
    /// that wraps another must forward this method, or the signal is lost.
    #[doc(hidden)]
    async fn get_with_freshness(
        &self,
        key: &str,
    ) -> Result<Option<(Vec<u8>, Freshness)>, BackendError> {
        Ok(self
            .get(key)
            .await?
            .map(|bytes| (bytes, Freshness::default())))
    }

    /// Store `value` under `key`, optionally expiring after `ttl`.
    async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), BackendError>;

    /// Remove `key` and return `true` if it existed.
    ///
    /// `CachekitIO` and `WorkersCachekitIO` return `true` on every successful
    /// delete, whether or not the key existed: the server does not report
    /// existence on `DELETE`.
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

    /// Receive the client's live hit/miss statistics.
    ///
    /// `CacheKitBuilder::build` calls this once, on the raw backend before any
    /// reliability decorator, so a backend that reports telemetry — the
    /// cachekit.io backends' `X-CacheKit-*` headers — carries real numbers
    /// with no user plumbing. A provider the user set on the backend's own
    /// builder must take precedence; implementations keep the first value
    /// they receive — so one backend instance reports one client, the first
    /// built over it (build one backend per client, as the presets do, for
    /// per-client attribution). The provider holds the client's counters
    /// weakly and reports `None` once that client is gone. Backends with
    /// nothing to report keep this default no-op.
    fn attach_metrics(&self, _provider: MetricsProvider) {}
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

    /// Retrieve `key` together with the server's [`Freshness`] for it.
    ///
    /// Client plumbing for the L1 backfill, not part of the documented surface.
    /// The default wraps [`Self::get`] as a fresh read with no bound, so a
    /// backend without a freshness signal keeps today's behaviour. A backend
    /// that wraps another must forward this method, or the signal is lost.
    #[doc(hidden)]
    async fn get_with_freshness(
        &self,
        key: &str,
    ) -> Result<Option<(Vec<u8>, Freshness)>, BackendError> {
        Ok(self
            .get(key)
            .await?
            .map(|bytes| (bytes, Freshness::default())))
    }

    /// Store `value` under `key`, optionally expiring after `ttl`.
    async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), BackendError>;

    /// Remove `key` and return `true` if it existed.
    ///
    /// `CachekitIO` and `WorkersCachekitIO` return `true` on every successful
    /// delete, whether or not the key existed: the server does not report
    /// existence on `DELETE`.
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

    /// Receive the client's live hit/miss statistics.
    ///
    /// `CacheKitBuilder::build` calls this once, on the raw backend before any
    /// reliability decorator, so a backend that reports telemetry — the
    /// cachekit.io backends' `X-CacheKit-*` headers — carries real numbers
    /// with no user plumbing. A provider the user set on the backend's own
    /// builder must take precedence; implementations keep the first value
    /// they receive — so one backend instance reports one client, the first
    /// built over it (build one backend per client, as the presets do, for
    /// per-client attribution). The provider holds the client's counters
    /// weakly and reports `None` once that client is gone. Backends with
    /// nothing to report keep this default no-op.
    fn attach_metrics(&self, _provider: MetricsProvider) {}
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
/// Almost every key is just [`urlencoding::encode`]. The exceptions are the
/// empty key and a key whose encoded form is one of the **five reserved path
/// segments** — `.`, `..`, `health`, `ttl`, `lock` — which are **rejected** with
/// a permanent [`BackendError`] rather than sent. This is the client's half of
/// the protocol `spec/saas-api.md` § Cache-Key Path Encoding, rule 2.
///
/// Three distinct hazards, each taking the request off the key's own
/// `/v1/cache/{key}` path and sending the app's bearer token somewhere the SaaS
/// `cache-key-validator` never vets (CWE-22):
///
/// - **The empty key.** It encodes to an empty segment, so `/v1/cache/{key}`
///   becomes `/v1/cache/` and `/v1/cache/{key}/ttl` becomes `/v1/cache//ttl`,
///   neither of which addresses a stored entry.
/// - **Dot segments (`.`, `..`).** A dot is RFC-3986 *unreserved*, so
///   `urlencoding::encode("..") == ".."` unchanged, and `reqwest`'s WHATWG URL
///   parser (rust-url) removes that dot-segment **before the request leaves the
///   process**: `/v1/cache/..` → `/v1/`, `…/../ttl` → `…/ttl`. Percent-encoding
///   does not help: WHATWG treats `%2e` / `%2e%2e` (case-insensitive) as
///   dot-segments too, so `%2E%2E` → `/v1/` and `%2E` → `/v1/cache/` collapse
///   just the same (verified in `repro_raw_dot_key_escapes_the_cache_prefix`).
///   Since every representation that `decodeURIComponent`s once back to `.`/`..`
///   is a WHATWG dot-segment, no encoding both reaches the wire intact and
///   round-trips — the only safe action is to refuse to build the request.
/// - **Route tokens (`health`, `ttl`, `lock`).** These are live path tokens at
///   this level: `/v1/cache/health` IS the health endpoint (see `health_url`),
///   and a trailing `ttl` / `lock` segment selects a sub-resource. A key of
///   exactly one of those words routes elsewhere or reads as an empty key, so
///   the spec reserves them client-side too.
///
/// Only an *entirely*-reserved segment is caught: `a:..`, `..a`, `x..y` are
/// inert and sent per rule 1 with their dots raw. Canonical and interop keys
/// always contain `:` and never meet this rule, so for every non-reserved key
/// the output is byte-identical to `urlencoding::encode` — the same bytes as
/// cachekit-py; cachekit-ts may leave `! * ' ( )` raw, which decodes to the same
/// key (spec rule 4). rust-url's uniform rejection matches the cachekit-ts twin;
/// it diverges from cachekit-py's older `%2E` rewrite
/// (`src/cachekit/backends/cachekitio/backend.py:247-250` @ `f000ba3`), whose
/// RFC-3986 client kept `%2E%2E` on the wire — the spec now mandates uniform
/// client-side rejection on every stack.
#[cfg(any(feature = "cachekitio", feature = "workers", test))]
pub(crate) fn encode_key(key: &str) -> Result<std::borrow::Cow<'_, str>, BackendError> {
    let encoded = urlencoding::encode(key);
    // spec/saas-api.md § Cache-Key Path Encoding rule 2: reject the empty key
    // and a key whose encoded form is exactly one of the five reserved segments.
    if matches!(
        encoded.as_ref(),
        "" | "." | ".." | "health" | "ttl" | "lock"
    ) {
        return Err(BackendError::permanent(
            "cache key must not be empty or a reserved path segment (`.`, `..`, \
             `health`, `ttl`, `lock`): the request path would leave \
             `/v1/cache/{key}` (CWE-22), so it cannot be addressed on the wire",
        ));
    }
    Ok(encoded)
}

/// Whether a `DELETE /v1/cache/{key}` status is a success. The server answers
/// `200` whether or not the key existed and never `404` (`spec/saas-api.md`),
/// so success carries no existence signal and every other status, `404`
/// included, takes the caller's error path. Shared by the native and Workers
/// backends so their mappings cannot drift.
#[cfg(any(feature = "cachekitio", feature = "workers", test))]
pub(crate) fn delete_succeeded(status: u16) -> bool {
    matches!(status, 200 | 204)
}

/// The `PUT /v1/cache/{key}` header that carries a write's TTL in whole
/// seconds. `spec/saas-api.md` says SDKs MUST send `X-CacheKit-TTL` only: the
/// legacy `X-TTL` goes away in protocol 2.0, and a write that sends only it
/// would then be stored with no expiry. Shared by the native and Workers
/// backends so their wire forms cannot drift.
#[cfg(any(feature = "cachekitio", feature = "workers", test))]
pub(crate) fn ttl_header(ttl: Duration) -> (&'static str, String) {
    ("X-CacheKit-TTL", ttl.as_secs().to_string())
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

// ── Path-encoding protocol vectors (test-only) ───────────────────────────────

/// Loader for `tests/vectors/path-encoding.json`, vendored verbatim from
/// cachekit-io/protocol `test-vectors/path-encoding.json` 1.1.0 (merge commit
/// `774281b09892a064feee6049ee29beb62f068804`). Do not edit the JSON here;
/// change it upstream and re-vendor, then update `SHA256`.
///
/// Every path-encoding conformance assertion reads its keys from this file, so
/// a row added upstream reaches the rs suite with no test edit. The only
/// rs-local list is `RS_NEAR_MISSES`, which can add acceptances but never hide
/// a reject row.
#[cfg(test)]
#[allow(clippy::expect_used)] // test-only: a malformed vendored fixture should panic loudly
pub(crate) mod path_encoding_vectors {
    use serde::Deserialize;

    const JSON: &str = include_str!("../../tests/vectors/path-encoding.json");

    /// sha256 of the vendored file, pinned so a local edit cannot drift from the
    /// protocol copy unnoticed.
    const SHA256: &str = "8f6fd4be5440da9cf4bbb1a112cb89c410c4e46734c7d8a9c023eaa034727ee3"; // pragma: allowlist secret

    /// One fixture row. `deny_unknown_fields` makes a new row field upstream
    /// fail loudly here rather than be silently ignored by the tests.
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct Vector {
        pub(crate) key: String,
        pub(crate) encoded: Option<String>,
        #[serde(default)]
        pub(crate) reject: bool,
        // Declared only so `deny_unknown_fields` accepts them. `decoded` and the
        // `encodeURIComponent` alternates are verified by the protocol's own CI.
        #[serde(rename = "decoded")]
        _decoded: Option<String>,
        #[serde(rename = "encoded_alternates", default)]
        _encoded_alternates: Vec<String>,
        #[serde(rename = "note")]
        _note: String,
    }

    #[derive(Deserialize)]
    struct Fixture {
        vectors: Vec<Vector>,
    }

    fn all() -> Vec<Vector> {
        serde_json::from_str::<Fixture>(JSON)
            .expect("vendored path-encoding.json must match the fixture schema")
            .vectors
    }

    /// Keys spec rule 2 reserves: a conformant client refuses to build a URL.
    pub(crate) fn reject_keys() -> Vec<String> {
        all()
            .into_iter()
            .filter(|v| v.reject)
            .map(|v| v.key)
            .collect()
    }

    /// Rows a conformant client sends.
    pub(crate) fn transmittable() -> Vec<Vector> {
        all().into_iter().filter(|v| !v.reject).collect()
    }

    /// rs-local, acceptance-only regressions that are NOT fixture rows: route-token
    /// near-misses that a prefix, case-insensitive or `contains` guard would
    /// wrongly reject. The fixture has none of those; its `x/../../health` row
    /// covers the suffix case, and `a:..` / `..a` cover the dot cases.
    pub(crate) const RS_NEAR_MISSES: &[&str] = &["healthy", "HEALTH", "ttls", "unlock"];

    #[test]
    fn vendored_fixture_matches_the_pinned_sha256() {
        use sha2::{Digest, Sha256};
        assert_eq!(
            hex::encode(Sha256::digest(JSON.as_bytes())),
            SHA256,
            "tests/vectors/path-encoding.json differs from the pinned protocol copy: \
             re-vendor it from protocol and update SHA256"
        );
    }
}

// ── encode_key unit tests (CWE-22) ───────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::expect_used)] // test-only: an encoding failure on a safe key should panic loudly
mod encode_key_tests {
    use super::encode_key;
    use super::path_encoding_vectors::{reject_keys, transmittable, RS_NEAR_MISSES};

    #[test]
    fn fixture_reject_rows_are_rejected() {
        // spec/saas-api.md rule 2: the empty key, `.`/`..` and the route tokens
        // are not addressable on `/v1/cache/{key}` (builder-level coverage:
        // `cachekitio::path_encoding_tests::reserved_segments_rejected_by_every_builder`).
        let keys = reject_keys();
        assert!(!keys.is_empty(), "fixture has no reject rows");
        for k in &keys {
            assert!(
                encode_key(k).is_err(),
                "reserved key {k:?} must be rejected"
            );
        }
    }

    #[test]
    fn fixture_transmittable_rows_encode_to_the_reference_form() {
        // The fixture accepts `[encoded] + encoded_alternates`; rs holds itself to
        // `encoded`, the reference form, so its wire bytes stay identical to
        // cachekit-py's (the alternates cover cachekit-ts's raw `! * ' ( )`).
        let rows = transmittable();
        assert!(!rows.is_empty(), "fixture has no transmittable rows");
        for row in &rows {
            let enc = encode_key(&row.key).expect("transmittable fixture key must encode");
            assert_eq!(
                Some(enc.as_ref()),
                row.encoded.as_deref(),
                "encode_key({:?}) is not the fixture's reference form",
                row.key
            );
        }
    }

    #[test]
    fn near_misses_are_not_rejected() {
        for k in RS_NEAR_MISSES {
            assert!(encode_key(k).is_ok(), "near-miss key {k:?} must encode");
        }
    }
}

// ── delete_succeeded unit tests ──────────────────────────────────────────────

#[cfg(test)]
mod delete_status_tests {
    use super::delete_succeeded;

    #[test]
    fn delete_success_is_200_or_204() {
        assert!(delete_succeeded(200));
        assert!(delete_succeeded(204));
    }

    #[test]
    fn delete_404_is_not_a_miss() {
        // The server never answers DELETE with 404, so it is an error, not `Ok(false)`.
        assert!(!delete_succeeded(404));
        for status in [201, 400, 401, 403, 429, 500, 503] {
            assert!(!delete_succeeded(status), "{status}");
        }
    }
}

// ── ttl_header unit tests ────────────────────────────────────────────────────

#[cfg(test)]
mod ttl_header_tests {
    use std::time::Duration;

    use super::ttl_header;

    #[test]
    fn ttl_header_is_the_canonical_name_in_whole_seconds() {
        assert_eq!(
            ttl_header(Duration::from_millis(60_900)),
            ("X-CacheKit-TTL", "60".to_owned())
        );
    }
}
