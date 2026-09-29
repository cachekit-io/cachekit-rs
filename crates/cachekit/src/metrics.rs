//! Telemetry: live hit/miss counters, the `X-CacheKit-*` SaaS telemetry
//! headers built from them, and — behind the `tracing` feature — structured
//! events for cache operations.
//!
//! Counters are owned by a [`CacheKit`](crate::CacheKit) and shared by its
//! clones; read a snapshot with [`CacheKit::stats`](crate::CacheKit::stats).
//! At build time the client hands a
//! [`MetricsProvider`](crate::metrics::MetricsProvider) over those counters to
//! the backend ([`Backend::attach_metrics`](crate::backend::Backend::attach_metrics)),
//! so the cachekit.io backends' telemetry headers report real numbers with no
//! user plumbing. A provider given to the backend's own builder takes
//! precedence.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Snapshot of cache hit statistics for one [`CacheKit`](crate::CacheKit)
/// and all of its clones.
///
/// Counts cover the value read path — `get`, `interop_get`, their SWR
/// variants, and the `SecureCache` equivalents. `exists` and reads that end
/// in a backend error are not counted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct L1Stats {
    /// Number of reads served from the L1 in-process cache.
    pub l1_hits: u64,
    /// Number of reads served from the L2 backend.
    pub l2_hits: u64,
    /// Number of reads that found nothing in either layer.
    pub misses: u64,
    /// Whether the L1 cache is enabled on this client.
    pub l1_enabled: bool,
}

impl L1Stats {
    /// Fraction of reads served from L1, in `[0.0, 1.0]` (`0.0` before any read).
    pub fn l1_hit_rate(&self) -> f64 {
        let total = self.l1_hits + self.l2_hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.l1_hits as f64 / total as f64
        }
    }
}

/// Thread-safe closure that produces an optional [`L1Stats`] snapshot.
pub type MetricsProvider = Arc<dyn Fn() -> Option<L1Stats> + Send + Sync>;

/// Build `X-CacheKit-*` HTTP headers from the current L1 stats provider.
pub fn metrics_headers(provider: Option<&MetricsProvider>) -> Vec<(&'static str, String)> {
    let disabled = vec![("X-CacheKit-L1-Status", "disabled".to_string())];

    let provider = match provider {
        Some(p) => p,
        None => return disabled,
    };

    let stats = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (provider)())) {
        Ok(Some(s)) => s,
        _ => return disabled,
    };

    if !stats.l1_enabled {
        return disabled;
    }

    vec![
        // The server accepts exactly `hit`, `miss`, or `disabled` and answers
        // 400 to an SDK key sending anything else (spec/saas-api.md, "Optional
        // Metrics Headers"). Like the Python and TypeScript SDKs we report the
        // aggregate `miss` whenever L1 is on — per-request classification is
        // not something an L2 request can know about itself.
        ("X-CacheKit-L1-Status", "miss".to_string()),
        ("X-CacheKit-L1-Hits", stats.l1_hits.to_string()),
        ("X-CacheKit-L2-Hits", stats.l2_hits.to_string()),
        ("X-CacheKit-Misses", stats.misses.to_string()),
        (
            "X-CacheKit-L1-Hit-Rate",
            format!("{:.3}", stats.l1_hit_rate()),
        ),
    ]
}

// ── Key hashing ──────────────────────────────────────────────────────────────

/// Blake2b-128 digest of a storage key as 32 lowercase hex characters.
///
/// Cache keys routinely embed user identifiers, so `tracing` events carry this
/// digest instead of the key (CWE-532). It is a **correlator, not a
/// redaction**: it is unkeyed and deterministic, so the same key hashes
/// identically in every process and matches the File backend's on-disk
/// filename (and cachekit-py's `hashlib.blake2b(key, digest_size=16)`) — and
/// for that same reason a low-entropy key such as `user:42` can be recovered
/// from its digest by enumeration. Treat `cachekit=debug` output with the
/// care you give the keys themselves.
pub fn key_hash(key: &str) -> String {
    use blake2::{digest::consts::U16, Blake2b, Digest};

    let mut hasher = Blake2b::<U16>::new();
    hasher.update(key.as_bytes());
    hex::encode(hasher.finalize())
}

// ── Live counters ────────────────────────────────────────────────────────────

/// How a value read was served. Counted by [`CacheCounters`] and, with the
/// `tracing` feature, emitted as the `outcome` field of the read event.
///
/// The L1 variants are only constructed by the L1 read paths (`L1Stale` by
/// the native SWR path), so builds without them would flag dead variants.
#[cfg_attr(
    not(all(feature = "l1", not(feature = "unsync"), not(target_arch = "wasm32"))),
    allow(dead_code)
)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadOutcome {
    /// Served from the in-process L1 cache, within its freshness window.
    L1Hit,
    /// Served from L1 past the SWR freshness threshold: still an L1 hit for
    /// the counters, but the caller is about to refresh it in the background.
    L1Stale,
    /// Served from the L2 backend (and backfilled into L1 when enabled).
    L2Hit,
    /// Neither layer had the key.
    Miss,
}

#[cfg(feature = "tracing")]
impl ReadOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::L1Hit => "l1_hit",
            Self::L1Stale => "l1_stale",
            Self::L2Hit => "l2_hit",
            Self::Miss => "miss",
        }
    }
}

/// Live counters behind [`L1Stats`], owned by a client and shared with its
/// clones and its backend's [`MetricsProvider`].
///
/// Relaxed atomics: each counter is an independent monotonic tally and a
/// snapshot may observe one counter a step ahead of another — fine for
/// telemetry, and the cost of a stronger ordering on the read hot path is not.
#[derive(Debug)]
pub(crate) struct CacheCounters {
    l1_hits: AtomicU64,
    l2_hits: AtomicU64,
    misses: AtomicU64,
    /// Fixed at build time: whether the owning client has an L1.
    l1_enabled: bool,
}

impl CacheCounters {
    pub(crate) fn new(l1_enabled: bool) -> Self {
        Self {
            l1_hits: AtomicU64::new(0),
            l2_hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            l1_enabled,
        }
    }

    /// Count one read outcome and emit its `tracing` event (feature `tracing`).
    pub(crate) fn record(&self, outcome: ReadOutcome, full_key: &str) {
        let counter = match outcome {
            ReadOutcome::L1Hit | ReadOutcome::L1Stale => &self.l1_hits,
            ReadOutcome::L2Hit => &self.l2_hits,
            ReadOutcome::Miss => &self.misses,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        trace_read(full_key, outcome);
    }

    /// Current totals.
    pub(crate) fn snapshot(&self) -> L1Stats {
        L1Stats {
            l1_hits: self.l1_hits.load(Ordering::Relaxed),
            l2_hits: self.l2_hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            l1_enabled: self.l1_enabled,
        }
    }

    /// A [`MetricsProvider`] over these counters, for
    /// [`Backend::attach_metrics`](crate::backend::Backend::attach_metrics).
    ///
    /// Holds the counters weakly: a backend can outlive the client that was
    /// built over it, and once every clone of that client is gone the provider
    /// reports `None` — the headers fall back to `disabled` — rather than the
    /// frozen numbers of a dead client.
    pub(crate) fn provider(self: &Arc<Self>) -> MetricsProvider {
        let counters = Arc::downgrade(self);
        Arc::new(move || counters.upgrade().map(|c| c.snapshot()))
    }
}

// ── tracing events ───────────────────────────────────────────────────────────
//
// One `debug` event per completed cache operation on the `cachekit` target
// (`RUST_LOG=cachekit=debug`). Fields: `op` (`get` | `set` | `delete`),
// `key_hash` (see `key_hash`), and per op `outcome` / `ttl_secs` / `existed`.
// Field expressions only run when a subscriber is enabled for the callsite,
// so the hash is never computed for an uninterested process. Without the
// feature these are empty functions the optimizer removes.

/// Target for cache-operation events.
#[cfg(feature = "tracing")]
const TARGET: &str = "cachekit";

fn trace_read(full_key: &str, outcome: ReadOutcome) {
    #[cfg(feature = "tracing")]
    tracing::debug!(
        target: TARGET,
        op = "get",
        key_hash = %key_hash(full_key),
        outcome = outcome.as_str(),
    );
    #[cfg(not(feature = "tracing"))]
    let _ = (full_key, outcome);
}

/// Emit the event for a completed write (`set` and SWR refresh commits).
pub(crate) fn trace_write(full_key: &str, ttl: Duration) {
    #[cfg(feature = "tracing")]
    tracing::debug!(
        target: TARGET,
        op = "set",
        key_hash = %key_hash(full_key),
        ttl_secs = ttl.as_secs(),
    );
    #[cfg(not(feature = "tracing"))]
    let _ = (full_key, ttl);
}

/// Emit the event for a completed delete.
pub(crate) fn trace_delete(full_key: &str, existed: bool) {
    #[cfg(feature = "tracing")]
    tracing::debug!(
        target: TARGET,
        op = "delete",
        key_hash = %key_hash(full_key),
        existed,
    );
    #[cfg(not(feature = "tracing"))]
    let _ = (full_key, existed);
}

#[cfg(test)]
#[allow(clippy::panic)] // test-only: a missing header on a fixture should panic loudly
mod tests {
    use super::*;

    fn header<'a>(headers: &'a [(&str, String)], name: &str) -> &'a str {
        headers
            .iter()
            .find(|h| h.0 == name)
            .map(|h| h.1.as_str())
            .unwrap_or_else(|| panic!("header {name} missing from {headers:?}"))
    }

    fn provider(l1_hits: u64, l2_hits: u64, misses: u64, l1_enabled: bool) -> MetricsProvider {
        Arc::new(move || {
            Some(L1Stats {
                l1_hits,
                l2_hits,
                misses,
                l1_enabled,
            })
        })
    }

    #[test]
    fn disabled_when_no_provider() {
        let headers = metrics_headers(None);
        assert_eq!(headers.len(), 1);
        assert_eq!(headers[0], ("X-CacheKit-L1-Status", "disabled".to_string()));
    }

    #[test]
    fn disabled_when_l1_not_enabled() {
        let headers = metrics_headers(Some(&provider(0, 0, 0, false)));
        assert_eq!(headers[0].1, "disabled");
    }

    /// The server rejects any status outside `hit|miss|disabled` with a 400
    /// for SDK keys — this pins the value the SDK sends when L1 is on.
    #[test]
    fn status_is_a_server_accepted_value_when_l1_enabled() {
        let headers = metrics_headers(Some(&provider(3, 2, 5, true)));
        let status = header(&headers, "X-CacheKit-L1-Status");
        assert_eq!(status, "miss");
        assert!(["hit", "miss", "disabled"].contains(&status));
    }

    #[test]
    fn correct_hit_rate_calculation() {
        let headers = metrics_headers(Some(&provider(3, 2, 5, true)));
        assert_eq!(header(&headers, "X-CacheKit-L1-Hit-Rate"), "0.300"); // 3 / (3+2+5)
        assert_eq!(header(&headers, "X-CacheKit-L1-Hits"), "3");
        assert_eq!(header(&headers, "X-CacheKit-L2-Hits"), "2");
        assert_eq!(header(&headers, "X-CacheKit-Misses"), "5");
    }

    #[test]
    fn zero_division_guard() {
        let headers = metrics_headers(Some(&provider(0, 0, 0, true)));
        assert_eq!(header(&headers, "X-CacheKit-L1-Hit-Rate"), "0.000");
        assert_eq!(L1Stats::default().l1_hit_rate(), 0.0);
    }

    #[test]
    fn disabled_when_provider_panics() {
        #[allow(clippy::panic)]
        let provider: MetricsProvider = Arc::new(|| panic!("boom"));
        let headers = metrics_headers(Some(&provider));
        assert_eq!(headers[0].1, "disabled");
    }

    #[test]
    fn counters_record_and_snapshot() {
        let counters = Arc::new(CacheCounters::new(true));
        counters.record(ReadOutcome::L1Hit, "k");
        counters.record(ReadOutcome::L1Stale, "k");
        counters.record(ReadOutcome::L2Hit, "k");
        counters.record(ReadOutcome::Miss, "k");
        assert_eq!(
            counters.snapshot(),
            L1Stats {
                l1_hits: 2,
                l2_hits: 1,
                misses: 1,
                l1_enabled: true,
            }
        );
        // The provider is a live view, not a copy...
        let provider = counters.provider();
        counters.record(ReadOutcome::Miss, "k");
        assert_eq!(provider().map(|s| s.misses), Some(2));
        // ...and a weak one: a dead client reports nothing, not stale numbers.
        drop(counters);
        assert_eq!(provider(), None);
        assert_eq!(metrics_headers(Some(&provider))[0].1, "disabled");
    }

    /// Same digest as the File backend filename and cachekit-py's
    /// `hashlib.blake2b(key, digest_size=16).hexdigest()`.
    #[test]
    fn key_hash_is_py_blake2b16_hex() {
        assert_eq!(
            key_hash("ns:app:func:m.f:args:abc:v1"),
            "bcb35ae6f64fa65b2770ab3af631b1ce" // pragma: allowlist secret
        );
        assert_eq!(key_hash("").len(), 32);
    }
}
