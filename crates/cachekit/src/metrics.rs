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

/// Names of the `X-CacheKit-*` telemetry headers, in the order
/// [`metrics_headers`] emits them: L1 status, L1 hits, L2 hits, misses, L1 hit
/// rate.
pub(crate) const METRICS_HEADER_NAMES: [&str; 5] = [
    "X-CacheKit-L1-Status",
    "X-CacheKit-L1-Hits",
    "X-CacheKit-L2-Hits",
    "X-CacheKit-Misses",
    "X-CacheKit-L1-Hit-Rate",
];

/// Build `X-CacheKit-*` HTTP headers from the current L1 stats provider.
pub fn metrics_headers(provider: Option<&MetricsProvider>) -> Vec<(&'static str, String)> {
    metrics_headers_named(provider, &METRICS_HEADER_NAMES, true)
}

/// [`metrics_headers`] with the names supplied in [`METRICS_HEADER_NAMES`]
/// order, so the native CachekitIO backend can pass prebuilt `HeaderName`s
/// instead of having reqwest parse each name on every request. With
/// `telemetry` off only `X-CacheKit-L1-Status` is built: the server requires
/// it on every SDK-key request, and it describes the request, not the session
/// (spec/saas-api.md, "Telemetry switch").
pub(crate) fn metrics_headers_named<N: Clone>(
    provider: Option<&MetricsProvider>,
    names: &[N; 5],
    telemetry: bool,
) -> Vec<(N, String)> {
    let [status, l1_hits, l2_hits, misses, hit_rate] = names.clone();
    let disabled = vec![(status.clone(), "disabled".to_string())];

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

    // The server accepts exactly `hit`, `miss`, or `disabled` and answers
    // 400 to an SDK key sending anything else (spec/saas-api.md, "Optional
    // Metrics Headers"). Like the Python and TypeScript SDKs we report the
    // aggregate `miss` whenever L1 is on — per-request classification is
    // not something an L2 request can know about itself.
    let status = (status, "miss".to_string());
    if !telemetry {
        return vec![status];
    }
    vec![
        status,
        (l1_hits, stats.l1_hits.to_string()),
        (l2_hits, stats.l2_hits.to_string()),
        (misses, stats.misses.to_string()),
        (hit_rate, format!("{:.3}", stats.l1_hit_rate())),
    ]
}

// ── Telemetry switch ─────────────────────────────────────────────────────────

/// The environment variable that turns a CachekitIO backend's telemetry off
/// (spec/saas-api.md, "Telemetry switch").
#[cfg(any(feature = "cachekitio", feature = "workers"))]
pub(crate) const TELEMETRY_ENV: &str = "CACHEKIT_TELEMETRY";

/// One telemetry control's reading: the `telemetry` builder setting, or
/// [`TELEMETRY_ENV`].
#[cfg(any(feature = "cachekitio", feature = "workers"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Switch {
    /// Not set: the other control, or the default, decides.
    Unset,
    On,
    Off,
}

#[cfg(any(feature = "cachekitio", feature = "workers"))]
impl Switch {
    /// Read a `CACHEKIT_TELEMETRY` value by the spec's rule, never by a
    /// loader's own boolean parsing: case-insensitive after trimming white
    /// space, `false`/`0`/`off`/`no` off, `true`/`1`/`on`/`yes` on, unset or
    /// empty unset. Any other value is off, the safe reading of a variable
    /// someone set to change something, and warns once. `None` is an unset
    /// variable; `Some(Err(()))` one whose value is not Unicode.
    pub(crate) fn from_env_value(raw: Option<Result<&str, ()>>) -> Self {
        let value = match raw {
            None => return Self::Unset,
            Some(Ok(value)) => value.trim(),
            Some(Err(())) => {
                warn_unrecognised_telemetry_env();
                return Self::Off;
            }
        };
        let is = |words: [&str; 4]| words.iter().any(|w| value.eq_ignore_ascii_case(w));
        if value.is_empty() {
            Self::Unset
        } else if is(["false", "0", "off", "no"]) {
            Self::Off
        } else if is(["true", "1", "on", "yes"]) {
            Self::On
        } else {
            warn_unrecognised_telemetry_env();
            Self::Off
        }
    }

    /// Two readings of the same control from different sources (on Workers,
    /// the process environment and the bindings): off from either wins.
    #[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
    pub(crate) fn either(self, other: Self) -> Self {
        match (self, other) {
            (Self::Off, _) | (_, Self::Off) => Self::Off,
            (Self::On, _) | (_, Self::On) => Self::On,
            (Self::Unset, Self::Unset) => Self::Unset,
        }
    }
}

/// Resolve the `telemetry` setting and the environment variable's reading by
/// the spec's truth table: off from either control wins, and with neither off
/// telemetry is on (the closed-beta default).
#[cfg(any(feature = "cachekitio", feature = "workers"))]
pub(crate) fn telemetry_on(setting: Option<bool>, env: Switch) -> bool {
    !matches!((setting, env), (Some(false), _) | (_, Switch::Off))
}

/// `CACHEKIT_TELEMETRY` from the process environment. Read once per backend,
/// when it is built.
#[cfg(all(feature = "cachekitio", not(target_arch = "wasm32")))]
pub(crate) fn telemetry_env() -> Switch {
    let raw = std::env::var_os(TELEMETRY_ENV);
    Switch::from_env_value(raw.as_deref().map(|v| v.to_str().ok_or(())))
}

/// Warn, once per process, that `CACHEKIT_TELEMETRY` held a value the rule
/// does not know and was read as off. Goes through `tracing` (feature
/// `tracing`) natively and to the console on Workers.
#[cfg(any(feature = "cachekitio", feature = "workers"))]
fn warn_unrecognised_telemetry_env() {
    use std::sync::atomic::AtomicBool;
    static WARNED: AtomicBool = AtomicBool::new(false);
    if WARNED.swap(true, Ordering::Relaxed) {
        return;
    }
    const MESSAGE: &str = "CACHEKIT_TELEMETRY is set to a value other than \
                           true/false/1/0/on/off/yes/no: telemetry is off";
    #[cfg(feature = "tracing")]
    tracing::warn!(target: "cachekit", "{MESSAGE}");
    #[cfg(all(feature = "workers", target_arch = "wasm32"))]
    worker::console_warn!("{MESSAGE}");
    #[cfg(not(any(feature = "tracing", all(feature = "workers", target_arch = "wasm32"))))]
    let _ = MESSAGE;
}

// ── L1 hit latency ───────────────────────────────────────────────────────────
//
// spec/saas-api.md, "L1 hit latency". A sample of the calls counted as L1
// hits is timed over the whole call, into a per-report histogram that the
// next CachekitIO request carries and resets. Only a backend that reports to
// cachekit.io has an accumulator, so every other client pays one `None`
// check per read.

/// Names of the latency headers, in the order [`L1Latency`] reports them.
#[cfg(all(feature = "cachekitio", not(target_arch = "wasm32")))]
pub(crate) const LATENCY_HEADER_NAMES: [&str; 3] = [
    "X-CacheKit-L1-Latency-Buckets",
    "X-CacheKit-L1-Latency-Sum-Ns",
    "X-CacheKit-L1-Latency-One-In",
];

/// Upper bound of each latency bucket, in nanoseconds: doubling from 128 ns,
/// with the last bucket closed at 60 s. A change to the bounds needs a new
/// header name.
#[cfg(all(feature = "l1", not(target_arch = "wasm32")))]
const BUCKET_BOUNDS_NS: [u64; 16] = [
    128,
    256,
    512,
    1_024,
    2_048,
    4_096,
    8_192,
    16_384,
    32_768,
    65_536,
    131_072,
    262_144,
    524_288,
    1_048_576,
    2_097_152,
    60_000_000_000,
];

/// The largest integer on the wire: 2^53 − 1, exact in a JavaScript number.
#[cfg(all(feature = "l1", not(target_arch = "wasm32")))]
const MAX_WIRE_INT: u64 = (1 << 53) - 1;

/// The sampling denominator every CachekitIO backend uses: one call in 128
/// is timed. Two clock reads and a locked add on every L1 hit would cost a
/// visible share of a sub-microsecond hit; one in 128 costs about one
/// instruction per hit and still times dozens of hits a second at a few
/// thousand hits a second.
#[cfg(all(feature = "cachekitio", not(target_arch = "wasm32")))]
pub(crate) const DEFAULT_ONE_IN: u32 = 128;

/// Index of the bucket that holds a duration of `ns` nanoseconds (at most
/// 60 s): bucket `i` holds `(bound[i-1], bound[i]]`, and bucket 0 everything
/// up to 128 ns, 0 included.
#[cfg(all(feature = "l1", not(target_arch = "wasm32")))]
fn bucket_index(ns: u64) -> usize {
    // Bits needed for `ns - 1`, less the 7 that bucket 0 spans: 129..=256
    // needs 8 bits, so lands in 1.
    let bits = 64 - ns.saturating_sub(1).leading_zeros();
    (bits.saturating_sub(7) as usize).min(BUCKET_BOUNDS_NS.len() - 1)
}

/// The hits timed since the previous report, and the process they were timed
/// in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Report {
    buckets: [u64; 16],
    sum_ns: u64,
    pid: u32,
}

impl Report {
    fn empty(pid: u32) -> Self {
        Self {
            buckets: [0; 16],
            sum_ns: 0,
            pid,
        }
    }
}

/// One CachekitIO backend's L1 hit-latency report accumulator.
///
/// A backend that reports to cachekit.io with telemetry on hands this to the
/// client built over it
/// ([`Backend::l1_latency`](crate::backend::Backend::l1_latency)); the client
/// times a sample of its L1 hits into it, and the backend's next request (any
/// but the health check) takes the report and carries it as the
/// `X-CacheKit-L1-Latency-*` headers. It has no public constructor: a backend
/// that wraps another forwards the inner one's.
#[derive(Debug)]
pub struct L1Latency {
    one_in: u32,
    /// A draw at or below this is timed: `u64::MAX / one_in`, so the chance
    /// is exactly `1 / one_in` when that is a power of two.
    #[cfg_attr(
        not(all(feature = "l1", not(target_arch = "wasm32"))),
        allow(dead_code)
    )]
    threshold: u64,
    /// Buckets, sum and owning process change together, under one lock, so a
    /// report's buckets and sum always describe the same hits.
    report: std::sync::Mutex<Report>,
}

impl L1Latency {
    /// An empty accumulator timing one call in `one_in` (clamped to 1 to
    /// 1,000,000).
    pub(crate) fn new(one_in: u32) -> Self {
        let one_in = one_in.clamp(1, 1_000_000);
        Self {
            one_in,
            threshold: u64::MAX / u64::from(one_in),
            report: std::sync::Mutex::new(Report::empty(current_pid())),
        }
    }

    /// An accumulator for tests outside this crate, which have no CachekitIO
    /// stub to read one from. Not public API.
    #[doc(hidden)]
    pub fn __new(one_in: u32) -> Self {
        Self::new(one_in)
    }

    /// The sampling denominator N sent as `X-CacheKit-L1-Latency-One-In`.
    pub fn one_in(&self) -> u32 {
        self.one_in
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Report> {
        self.report
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Count one timed L1 hit of `elapsed`. A hit over 60 s is discarded, as
    /// the spec requires, and so is one that would take the sum or its
    /// bucket past 2^53 − 1, so the report stays one the server accepts.
    #[cfg(all(feature = "l1", not(target_arch = "wasm32")))]
    pub(crate) fn record(&self, elapsed: Duration) {
        let Ok(ns) = u64::try_from(elapsed.as_nanos()) else {
            return;
        };
        if ns > BUCKET_BOUNDS_NS[15] {
            return;
        }
        let i = bucket_index(ns);
        let mut report = self.lock();
        let sum = report.sum_ns + ns;
        if sum > MAX_WIRE_INT || report.buckets[i] == MAX_WIRE_INT {
            return;
        }
        report.buckets[i] += 1;
        report.sum_ns = sum;
    }

    /// Take the report and reset the accumulator in one step, as the three
    /// header values in [`LATENCY_HEADER_NAMES`] order, or `None` when there
    /// is nothing to send: no timed hit, or only hits that read 0 ns, which
    /// stay for a later request. A report built before this process forked
    /// is discarded, never sent twice.
    pub(crate) fn take(&self) -> Option<[String; 3]> {
        self.take_in(current_pid())
    }

    fn take_in(&self, pid: u32) -> Option<[String; 3]> {
        let taken = {
            let mut report = self.lock();
            if report.pid != pid {
                *report = Report::empty(pid);
                return None;
            }
            if report.sum_ns == 0 {
                return None;
            }
            std::mem::replace(&mut *report, Report::empty(pid))
        };
        let buckets = taken.buckets.map(|count| count.to_string()).join(",");
        Some([buckets, taken.sum_ns.to_string(), self.one_in.to_string()])
    }

    /// [`Self::take`] for tests outside this crate. Not public API.
    #[doc(hidden)]
    pub fn __take(&self) -> Option<[String; 3]> {
        self.take()
    }

    /// Add the duration of a timed call that started at `start` and has just
    /// produced its value, if `by` says L1 served it: the one outcome timed.
    #[cfg(all(feature = "l1", not(target_arch = "wasm32")))]
    #[cold]
    #[inline(never)]
    pub(crate) fn record_hit(&self, start: std::time::Instant, by: ReadOutcome) {
        if matches!(by, ReadOutcome::L1Hit | ReadOutcome::L1Stale) {
            self.record(start.elapsed());
        }
    }

    /// Decide whether to time a call, by an independent draw with chance
    /// `1 / one_in`, made before anything about the call is known.
    #[cfg(all(feature = "l1", not(target_arch = "wasm32")))]
    #[inline]
    pub(crate) fn draw(&self) -> bool {
        next_random() <= self.threshold
    }
}

/// The process a report is built in. `std::process::id` panics on
/// wasm32-unknown-unknown, where nothing forks, so wasm32 reports 0.
fn current_pid() -> u32 {
    #[cfg(not(target_arch = "wasm32"))]
    {
        std::process::id()
    }
    #[cfg(target_arch = "wasm32")]
    {
        0
    }
}

/// The next draw from this thread's wyrand generator, seeded on first use
/// from the standard library's per-process random hash keys. A draw is an
/// add, a multiply and two xors, and each thread draws independently.
#[cfg(all(feature = "l1", not(target_arch = "wasm32")))]
#[inline]
fn next_random() -> u64 {
    use std::cell::Cell;
    thread_local! {
        static STATE: Cell<u64> = const { Cell::new(0) };
    }
    STATE.with(|state| {
        let mut s = state.get();
        if s == 0 {
            s = seed();
        }
        s = s.wrapping_add(0xa076_1d64_78bd_642f);
        state.set(s);
        let t = u128::from(s) * u128::from(s ^ 0xe703_7ed1_a0b4_28db);
        #[allow(clippy::cast_possible_truncation)] // the two halves, by design
        let r = (t >> 64) as u64 ^ t as u64;
        r
    })
}

/// A random, non-zero generator seed.
#[cfg(all(feature = "l1", not(target_arch = "wasm32")))]
#[cold]
fn seed() -> u64 {
    use std::hash::BuildHasher;
    std::collections::hash_map::RandomState::new().hash_one(0u8) | 1
}

/// A cache call's latency clock: when it started if the call was drawn for
/// timing, nothing otherwise. Carried through the call so the clock stops
/// where the call produces its value. Public only for `#[cachekit]` — not
/// public API.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct L1Sample {
    #[cfg(all(feature = "l1", not(target_arch = "wasm32")))]
    start: Option<std::time::Instant>,
}

impl L1Sample {
    /// A call not drawn for timing.
    pub(crate) const NONE: Self = Self {
        #[cfg(all(feature = "l1", not(target_arch = "wasm32")))]
        start: None,
    };

    /// Start a drawn call's clock.
    #[cfg(all(feature = "l1", not(target_arch = "wasm32")))]
    #[inline]
    pub(crate) fn start() -> Self {
        Self {
            start: Some(std::time::Instant::now()),
        }
    }

    /// Stop the clock as the call produces its value, and add the duration to
    /// `latency` if the call was drawn and L1 served it (`by`).
    #[cfg(all(feature = "l1", not(target_arch = "wasm32")))]
    #[inline]
    pub(crate) fn stop(self, by: ReadOutcome, latency: Option<&L1Latency>) {
        if let (Some(start), Some(latency)) = (self.start, latency) {
            latency.record_hit(start, by);
        }
    }
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
/// Public only so `#[cachekit]` can carry a call's outcome from its reads to
/// [`CacheKit::__count`](crate::CacheKit::__count) — not public API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
#[non_exhaustive]
pub enum ReadOutcome {
    /// Served from the in-process L1 cache, within its freshness window.
    L1Hit,
    /// Served from L1 past the SWR freshness threshold: still an L1 hit for
    /// the counters, but the caller is about to refresh it in the background.
    L1Stale,
    /// Served from the L2 backend (and backfilled into L1 when enabled, unless
    /// the server marked the read stale or its freshness exhausted).
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
// (`RUST_LOG=cachekit=debug`), one per failed fill-lock call, and one per
// `#[cachekit]` result the macro could not store. Fields: `op`
// (`get` | `set` | `delete` | `lock` | `unlock`), `key_hash` (see `key_hash`),
// and per op `outcome` / `ttl_secs` / `existed` / `error_kind`.
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
        ttl_secs = crate::backend::ttl_wire_secs(ttl),
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

/// Emit the event for a failed fill-lock call (`op` is `lock` or `unlock`).
/// The call bypasses the circuit breaker, so this is the only signal of a
/// failing lock endpoint: refreshes stand down, cold misses fill without the
/// lease, and an unreleased lease is left to expire. It carries the error's
/// kind, never its message: a transport error names the request URL, and the
/// URL holds the key (CWE-532).
#[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
pub(crate) fn trace_lock_error(full_key: &str, op: &'static str, err: &crate::BackendError) {
    #[cfg(feature = "tracing")]
    tracing::debug!(
        target: TARGET,
        op,
        key_hash = %key_hash(full_key),
        error_kind = %err.kind,
    );
    #[cfg(not(feature = "tracing"))]
    let _ = (full_key, op, err);
}

/// Emit the event for a `#[cachekit]` result the macro computed but could not
/// store: a cold-miss fill, or an SWR refresh whose commit failed. Macro
/// plumbing. A cold-miss call still returns its result; a failed refresh
/// commit is dropped and the stale value keeps being served. Either way this
/// is the only signal of a failing store — otherwise it shows only as repeat
/// misses or refreshes.
/// The macro stores only on an un-namespaced client, so `key` is the storage
/// key. Like the lock-error event, it carries the error's kind, never
/// its message: a transport error names the request URL, and an invalid-key
/// error the key (CWE-532).
#[doc(hidden)]
pub fn __trace_store_error(key: &str, err: &crate::CachekitError) {
    #[cfg(feature = "tracing")]
    {
        use crate::CachekitError as E;
        let error_kind: &dyn std::fmt::Display = match err {
            E::Backend(be) => &be.kind,
            E::Serialization(_) => &"serialization",
            E::Encryption(_) => &"encryption",
            E::Config(_) => &"config",
            E::PayloadTooLarge { .. } => &"payload-too-large",
            E::InvalidKey(_) => &"invalid-key",
        };
        tracing::debug!(
            target: TARGET,
            op = "set",
            key_hash = %key_hash(key),
            error_kind = %error_kind,
        );
    }
    #[cfg(not(feature = "tracing"))]
    let _ = (key, err);
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
