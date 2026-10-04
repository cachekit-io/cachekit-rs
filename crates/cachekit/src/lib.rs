//! CacheKit — caching for Rust.
//!
//! Supports cachekit.io SaaS, Redis, Memcached, local File, and Cloudflare
//! Workers backends. Zero-knowledge encryption via AES-256-GCM with HKDF key
//! derivation.
//!
//! # Getting started: intent presets
//!
//! The intent presets are the primary entry point — one call that names your
//! use case and returns a pre-configured [`CacheKitBuilder`] you can still
//! override before [`build()`](CacheKitBuilder::build):
//!
//! | Intent | Backend | L1 | Encryption | Auto-reconnect² | Reliability¹ | Default TTL |
//! |------------|-----------|------|------------|-----------------|--------------|-------------|
//! | `CacheKit::minimal`³ | Redis | On (no SWR) | No | No | Off | 300 s |
//! | `CacheKit::production`³ | Redis | On | No | Yes | On | 600 s |
//! | `CacheKit::secure`³ ⁵ | Redis | On | AES-256-GCM | Yes | On | 600 s |
//! | [`io`](CacheKit::io)⁴ | cachekit.io | On | No | n/a (HTTP) | On | 3 600 s |
//!
//! ¹ Retry with backoff + jitter, a circuit breaker, and backpressure around
//! backend ops — see [`reliability`]. Requires the default-on `reliability`
//! cargo feature.
//! ² `production`/`secure` re-establish a dropped Redis connection with
//! exponential backoff (100 ms → 30 s, retrying indefinitely); `minimal` is
//! fail-fast (a dropped connection stays dead). **Initial** connections fail
//! fast for every Redis preset — `io` opens no connection at construction: an
//! empty API key fails at construction, while an invalid key or unreachable
//! endpoint surfaces at the first request.
//! Auto-reconnect is connection-level repair, distinct from the per-operation
//! reliability stack.
//! ³ Requires the `redis` cargo feature; `secure` also needs the
//! default-on `encryption` feature.
//! ⁴ API key by argument, or from `CACHEKIT_API_KEY` via
//! [`io_from_env`](CacheKit::io_from_env). Neither reads
//! `CACHEKIT_MASTER_KEY`; unlike [`from_env`](CacheKit::from_env), the `io`
//! preset never activates encryption from the environment.
//! ⁵ Master key as a hex string, `secure(url, master_key_hex)`, or from
//! `CACHEKIT_MASTER_KEY` via `secure_from_env(url)` — the same hex decoding
//! either way, so one key value derives the same key bytes in every CacheKit
//! SDK. Use exactly 32 bytes (64 hex chars), the only length every SDK
//! accepts. `secure_from_env` also reads decrypt-only rotation keys from
//! `CACHEKIT_PREVIOUS_MASTER_KEYS`, validated as `from_env` validates them.
//! A missing, non-hex or short key fails at construction, before any Redis
//! I/O; `secure` never falls back to plaintext.
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let cache = cachekit::CacheKit::io_from_env()?
//!     .namespace("myapp")
//!     .build()?;
//!
//! cache.set("greeting", &"Hello, world!").await?;
//! let val: Option<String> = cache.get("greeting").await?;
//! # Ok(())
//! # }
//! ```
//!
//! For full control, drop down to [`CacheKit::builder`] or
//! [`CacheKit::from_env`].
//!
//! # Observability
//!
//! Every client counts its reads: [`CacheKit::stats`] answers "is my cache
//! hitting?" (L1 hits / L2 hits / misses), [`CacheKit::l1_entry_count`]
//! reports L1 occupancy, and with `reliability` on,
//! [`CacheKit::circuit_state`] reports the breaker. The same counters feed the
//! cachekit.io backends' `X-CacheKit-*` telemetry headers automatically. Enable
//! the `tracing` cargo feature for a `debug` event per operation on the
//! `cachekit` target (`op`, `outcome`, `key_hash` — never the key) and
//! breaker transitions on `cachekit::reliability` — see [`metrics`].

// Production code lints — these only fire in src/, not tests/
#![warn(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#![warn(missing_docs)]

// Mutually exclusive feature guards
#[cfg(all(feature = "workers", feature = "redis"))]
compile_error!(
    "features `workers` and `redis` are mutually exclusive — Workers runtime cannot use fred"
);

#[cfg(all(feature = "workers", feature = "l1"))]
compile_error!("features `workers` and `l1` are mutually exclusive — moka requires std threads unavailable in wasm32");

#[cfg(all(feature = "workers", feature = "reliability"))]
compile_error!("features `workers` and `reliability` are mutually exclusive — retry/breaker timers need tokio `time`, unavailable in wasm32");

#[cfg(all(feature = "workers", feature = "memcached"))]
compile_error!("features `workers` and `memcached` are mutually exclusive — Workers runtime has no TCP sockets");

#[cfg(all(feature = "workers", feature = "file"))]
compile_error!(
    "features `workers` and `file` are mutually exclusive — Workers runtime has no filesystem"
);

// Target guard: moka reads `Instant::now()` for its clock origin when the cache is constructed.
#[cfg(all(feature = "l1", target_arch = "wasm32", target_os = "unknown"))]
compile_error!("feature `l1` is not supported on wasm32-unknown-unknown — std::time::Instant has no clock there, so building the client (and its L1 cache) would panic; set `default-features = false, features = [\"encryption\"]` plus your backend feature, leaving out `l1`");

/// Pluggable cache backend trait and implementations (CachekitIO, Redis,
/// Memcached, File, Workers).
pub mod backend;
/// High-level cache client with dual-layer (L1/L2) support.
pub mod client;
/// Configuration types and environment variable parsing.
pub mod config;
/// Error types for cache operations and backend communication.
pub mod error;
/// Cold-miss single-flight: dedup concurrent fills of the same key.
pub mod flight;
/// Interop mode (interop/v1): cross-SDK cache keys and plain-MessagePack values.
pub mod interop;
/// Live hit/miss counters, the SaaS telemetry headers built from them, and
/// (feature `tracing`) structured events for cache operations.
pub mod metrics;
/// Serialization and deserialization of cached values via MessagePack.
pub mod serializer;
/// SDK session tracking (session ID and start timestamp).
pub mod session;
/// SSRF-safe URL validation for CachekitIO endpoints.
pub mod url_validator;

/// Intent-based cache presets (`CacheKit::minimal`, `::production`, `::secure`, `::io`).
mod intents;

/// Client-side AES-256-GCM encryption with HKDF key derivation.
#[cfg(feature = "encryption")]
pub mod encryption;

/// In-process L1 cache backed by [`moka`] with per-entry TTL.
#[cfg(feature = "l1")]
pub mod l1;

/// Reliability tier: retry with backoff + jitter, circuit breaker.
#[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
pub mod reliability;

// Re-exports
pub use client::{CacheKit, CacheKitBuilder, SharedBackend, SwrRead, SwrToken};
pub use config::CachekitConfig;
pub use error::{BackendError, BackendErrorKind, CachekitError};
pub use metrics::L1Stats;

#[cfg(feature = "encryption")]
pub use client::SecureCache;
#[cfg(feature = "encryption")]
pub use encryption::EncryptionLayer;

#[cfg(feature = "macros")]
pub use cachekit_macros::cachekit;

pub use flight::SingleFlight;

#[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
pub use reliability::{
    BackpressureConfig, CircuitBreakerConfig, CircuitState, ReliabilityConfig, RetryConfig,
};

// ── Shared jitter source ─────────────────────────────────────────────────────

/// Uniform random in `[0, 1)`. Used by retry backoff (`reliability`) and the
/// L1 SWR freshness threshold at entry insertion (`l1`). Jitter needs
/// decorrelation across clients, not crypto quality, so this is a per-thread
/// SplitMix64 seeded from std's `RandomState` instead of a getrandom syscall
/// per call; 53 bits is plenty. Native only: wasm32 has no process ids
/// (`std::process::id()` panics there), and `RandomState` carries no entropy
/// on wasm32-unknown-unknown.
#[cfg(all(
    any(feature = "l1", feature = "reliability"),
    not(target_arch = "wasm32")
))]
pub(crate) fn random_unit() -> f64 {
    thread_local! {
        static STATE: JitterState = const { std::cell::Cell::new(None) };
    }
    let z = STATE.with(|state| next_jitter(state, std::process::id()));
    ((z >> 11) as f64) / ((1u64 << 53) as f64)
}

/// wasm32 variant (`l1` without `workers`, e.g. on WASI): one host-RNG draw per
/// call through UUID v4. Nothing forks there, so the per-call draw is safe.
#[cfg(all(feature = "l1", target_arch = "wasm32"))]
pub(crate) fn random_unit() -> f64 {
    let bits = uuid::Uuid::new_v4().as_u128() & ((1u128 << 53) - 1);
    (bits as f64) / ((1u64 << 53) as f64)
}

/// The owning process id and SplitMix64 state behind [`random_unit`].
#[cfg(all(
    any(feature = "l1", feature = "reliability"),
    not(target_arch = "wasm32")
))]
type JitterState = std::cell::Cell<Option<(u32, u64)>>;

/// One SplitMix64 step. The state reseeds whenever `pid` differs from the one
/// it was seeded under: a forked child inherits its parent's thread-local
/// state, and without the reseed every prefork worker would replay the
/// parent's sequence and synchronise refreshes and backoffs. The pid is hashed
/// into the seed because siblings forked from one parent also inherit the same
/// `RandomState` keys.
#[cfg(all(
    any(feature = "l1", feature = "reliability"),
    not(target_arch = "wasm32")
))]
fn next_jitter(state: &JitterState, pid: u32) -> u64 {
    use std::hash::{BuildHasher, Hasher};

    let current = match state.get() {
        Some((owner, current)) if owner == pid => current,
        _ => {
            let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
            hasher.write_u32(pid);
            hasher.finish()
        }
    };
    let next = current.wrapping_add(0x9E37_79B9_7F4A_7C15);
    state.set(Some((pid, next)));
    let mut z = next;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

// ── SWR background-refresh spawn (macro plumbing) ───────────────────────────

/// Spawn a stale-while-revalidate background refresh onto the ambient tokio
/// runtime. Macro plumbing for `#[cachekit]` — not public API.
///
/// Without a tokio runtime on the current thread the refresh is skipped: the
/// caller has already been served the stale value, and a later stale read
/// simply retries. Panicking here would turn a cache optimisation into an
/// availability bug on non-tokio executors.
#[cfg(all(feature = "l1", not(feature = "unsync"), not(target_arch = "wasm32")))]
#[doc(hidden)]
pub fn __swr_spawn<F>(fut: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        drop(handle.spawn(fut));
    }
}

/// No-op variant: under `unsync`, on wasm32, or without the `l1` feature the
/// client never classifies a hit as stale (`SwrRead::Stale` is unreachable),
/// so the refresh future handed here is dead code by construction. The stub
/// exists so `#[cachekit]`-generated code compiles under every configuration.
#[cfg(not(all(feature = "l1", not(feature = "unsync"), not(target_arch = "wasm32"))))]
#[doc(hidden)]
pub fn __swr_spawn<F>(_fut: F)
where
    F: std::future::Future<Output = ()> + 'static,
{
}

/// Convenient glob import for the most common types.
pub mod prelude {
    pub use crate::{
        BackendError, BackendErrorKind, CacheKit, CacheKitBuilder, CachekitConfig, CachekitError,
        SwrRead, SwrToken,
    };

    #[cfg(feature = "encryption")]
    pub use crate::{EncryptionLayer, SecureCache};

    #[cfg(feature = "macros")]
    pub use crate::cachekit;
}

#[cfg(all(
    test,
    any(feature = "l1", feature = "reliability"),
    not(target_arch = "wasm32")
))]
mod random_unit_tests {
    use super::{next_jitter, random_unit, JitterState};

    /// A forked child inherits the parent's thread-local state (simulated by
    /// copying the cell). Under its own pid it must not replay the parent's
    /// next draw, and two siblings must not match each other either.
    #[test]
    fn a_new_pid_reseeds_instead_of_replaying_the_parent() {
        let parent = JitterState::new(None);
        next_jitter(&parent, 100);
        let child_a = JitterState::new(parent.get());
        let child_b = JitterState::new(parent.get());
        let from_parent = next_jitter(&parent, 100);
        let from_a = next_jitter(&child_a, 101);
        let from_b = next_jitter(&child_b, 102);
        assert_ne!(from_parent, from_a);
        assert_ne!(from_parent, from_b);
        assert_ne!(from_a, from_b);
        // Same pid: the sequence continues rather than reseeding each draw.
        let replay = JitterState::new(child_a.get());
        assert_eq!(next_jitter(&replay, 101), next_jitter(&child_a, 101));
    }

    #[test]
    fn stays_in_unit_interval_and_spreads() {
        let draws: Vec<f64> = (0..10_000).map(|_| random_unit()).collect();
        assert!(draws.iter().all(|x| (0.0..1.0).contains(x)));
        let mean = draws.iter().sum::<f64>() / draws.len() as f64;
        assert!((mean - 0.5).abs() < 0.02, "mean {mean}");
        let low = draws.iter().filter(|x| **x < 0.1).count();
        let high = draws.iter().filter(|x| **x >= 0.9).count();
        assert!(low > 800 && high > 800, "tails {low} / {high}");
    }

    /// Each thread seeds from its own `RandomState` key, so two threads (and
    /// two processes) do not replay one sequence.
    #[test]
    #[allow(clippy::expect_used)]
    fn threads_draw_different_sequences() {
        let draw = || (0..4).map(|_| random_unit().to_bits()).collect::<Vec<_>>();
        let a = std::thread::spawn(draw).join().expect("thread a");
        let b = std::thread::spawn(draw).join().expect("thread b");
        assert_ne!(a, b);
    }
}
