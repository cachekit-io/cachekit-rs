//! Integration tests for L1 stale-while-revalidate (LAB-728).
//!
//! Run with:
//!   cargo test --test swr_tests --features macros,l1
//!
//! SWR is native-only (no `unsync`, no wasm32): the background refresh needs
//! a spawnable `Send` future. These tests use real time — moka's clock is not
//! tokio-mockable — so windows are generous: threshold ~1 s (±10% jitter),
//! hard expiry 4 s, origin delay 400 ms, and every latency assertion leaves
//! two orders of magnitude of slack over an L1 read.

#![cfg(all(
    feature = "macros",
    feature = "l1",
    not(feature = "unsync"),
    not(target_arch = "wasm32")
))]

mod common;

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use common::MockBackend;

use cachekit::interop::{interop_key, InteropValue};
use cachekit::{cachekit, CacheKit, CachekitError};
use tokio::sync::Notify;

/// Origin latency: long enough that a blocking call is unmistakable next to
/// a served-from-L1 stale read.
const ORIGIN_DELAY: Duration = Duration::from_millis(400);

fn client(backend: cachekit::SharedBackend) -> CacheKit {
    CacheKit::builder()
        .backend(backend)
        // threshold = 0.25 × 4 s = 1 s (±10%): stale from ≤1.1 s, hard
        // expiry at 4 s — a comfortable mid-window probe point at 1.4 s.
        .swr_threshold_ratio(0.25)
        .build()
        .expect("client builds")
}

fn key(operation: &str, id: u64) -> String {
    interop_key("swrtest", operation, &[InteropValue::from(id)]).expect("test key is valid")
}

// ── serve stale + exactly-one refresh ────────────────────────────────────────

static SWR_CALLS: AtomicU32 = AtomicU32::new(0);

#[cachekit(client = cache, ttl = 4, interop = "swr_probe", namespace = "swrtest")]
async fn swr_probe(cache: &CacheKit, id: u64) -> Result<String, CachekitError> {
    let n = SWR_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
    tokio::time::sleep(ORIGIN_DELAY).await;
    Ok(format!("u{id}-c{n}"))
}

/// The core LAB-728 contract in one scenario: a read past the SWR threshold
/// (but before hard expiry) returns the stale value without blocking on the
/// origin; N concurrent stale readers trigger exactly ONE background
/// re-execution (single-flight dedup); the next read sees the refreshed value
/// without recomputing.
#[tokio::test]
async fn stale_reads_serve_immediately_and_refresh_exactly_once() {
    let cache = client(MockBackend::shared());

    // Warm: one blocking origin call.
    assert_eq!(swr_probe(&cache, 7).await.unwrap(), "u7-c1");
    assert_eq!(SWR_CALLS.load(Ordering::SeqCst), 1);

    // Age into the stale window (threshold ≤1.1 s, hard expiry 4 s).
    tokio::time::sleep(Duration::from_millis(1400)).await;

    // 8 concurrent stale readers, each on its own client clone (clones share
    // L1 and single-flight state, so this also exercises cross-clone dedup).
    let started = Instant::now();
    let mut set = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let clone = cache.clone();
        set.spawn(async move { swr_probe(&clone, 7).await });
    }
    let mut reads = Vec::new();
    while let Some(joined) = set.join_next().await {
        reads.push(joined.expect("reader task panicked"));
    }
    let elapsed = started.elapsed();

    // Every reader got the stale value, and none of them awaited the origin
    // (which takes 400 ms): the whole batch is a set of L1 reads.
    assert_eq!(reads.len(), 8);
    for read in reads {
        assert_eq!(read.unwrap(), "u7-c1", "stale value is served as-is");
    }
    assert!(
        elapsed < Duration::from_millis(300),
        "stale reads must not block on the origin: took {elapsed:?}"
    );

    // Let the single background refresh finish.
    tokio::time::sleep(ORIGIN_DELAY + Duration::from_millis(800)).await;
    assert_eq!(
        SWR_CALLS.load(Ordering::SeqCst),
        2,
        "8 concurrent stale readers must trigger exactly one refresh"
    );

    // The refreshed value is now served fresh — no further origin calls.
    assert_eq!(swr_probe(&cache, 7).await.unwrap(), "u7-c2");
    assert_eq!(SWR_CALLS.load(Ordering::SeqCst), 2);
}

// ── version-guarded refresh commit ───────────────────────────────────────────

static DELETE_RACE_CALLS: AtomicU32 = AtomicU32::new(0);
static DELETE_REFRESH_STARTED: Notify = Notify::const_new();
static DELETE_REFRESH_RELEASE: Notify = Notify::const_new();

#[cachekit(
    client = cache,
    ttl = 4,
    interop = "swr_delete_race",
    namespace = "swrtest"
)]
async fn swr_delete_race(cache: &CacheKit, id: u64) -> Result<String, CachekitError> {
    let n = DELETE_RACE_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
    if n == 2 {
        DELETE_REFRESH_STARTED.notify_one();
        DELETE_REFRESH_RELEASE.notified().await;
    }
    Ok(format!("d{id}-c{n}"))
}

/// A delete that lands while the origin is recomputing invalidates the stale
/// token. The refresh must be discarded in both L1 and L2 — never resurrected.
#[tokio::test]
async fn concurrent_delete_wins_over_an_older_refresh() {
    let cache = client(MockBackend::shared());
    let storage_key = key("swr_delete_race", 11);

    assert_eq!(swr_delete_race(&cache, 11).await.unwrap(), "d11-c1");
    tokio::time::sleep(Duration::from_millis(1400)).await;

    // Serve stale and wait until the background origin is definitely in
    // flight before deleting the same key.
    assert_eq!(swr_delete_race(&cache, 11).await.unwrap(), "d11-c1");
    tokio::time::timeout(Duration::from_secs(2), DELETE_REFRESH_STARTED.notified())
        .await
        .expect("refresh origin started");
    assert!(cache.delete(&storage_key).await.unwrap());

    DELETE_REFRESH_RELEASE.notify_one();
    let flight = cache.single_flight(&storage_key).await;
    flight.release().await;

    let value: Option<String> = cache.interop_get(&storage_key).await.unwrap();
    assert_eq!(value, None, "completed refresh must not resurrect a delete");
    assert_eq!(DELETE_RACE_CALLS.load(Ordering::SeqCst), 2);
}

#[cfg(feature = "encryption")]
static SECURE_DELETE_RACE_CALLS: AtomicU32 = AtomicU32::new(0);
#[cfg(feature = "encryption")]
static SECURE_DELETE_REFRESH_STARTED: Notify = Notify::const_new();
#[cfg(feature = "encryption")]
static SECURE_DELETE_REFRESH_RELEASE: Notify = Notify::const_new();

#[cfg(feature = "encryption")]
#[cachekit(
    client = cache,
    ttl = 4,
    interop = "swr_secure_delete_race",
    namespace = "swrtest",
    secure
)]
async fn swr_secure_delete_race(cache: &CacheKit, id: u64) -> Result<String, CachekitError> {
    let n = SECURE_DELETE_RACE_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
    if n == 2 {
        SECURE_DELETE_REFRESH_STARTED.notify_one();
        SECURE_DELETE_REFRESH_RELEASE.notified().await;
    }
    Ok(format!("sd{id}-c{n}"))
}

/// The encrypted macro path uses the same version check before it persists a
/// freshly encrypted value, so ciphertext cannot resurrect a deleted entry.
#[cfg(feature = "encryption")]
#[tokio::test]
async fn concurrent_delete_wins_over_an_older_secure_refresh() {
    let cache = CacheKit::builder()
        .backend(MockBackend::shared())
        .swr_threshold_ratio(0.25)
        .encryption_from_bytes(&[7_u8; 32], "swr-race-tenant")
        .expect("encryption configures")
        .build()
        .expect("client builds");
    let storage_key = key("swr_secure_delete_race", 13);

    assert_eq!(swr_secure_delete_race(&cache, 13).await.unwrap(), "sd13-c1");
    tokio::time::sleep(Duration::from_millis(1400)).await;

    assert_eq!(swr_secure_delete_race(&cache, 13).await.unwrap(), "sd13-c1");
    tokio::time::timeout(
        Duration::from_secs(2),
        SECURE_DELETE_REFRESH_STARTED.notified(),
    )
    .await
    .expect("secure refresh origin started");
    assert!(cache.secure().unwrap().delete(&storage_key).await.unwrap());

    SECURE_DELETE_REFRESH_RELEASE.notify_one();
    let flight = cache.single_flight(&storage_key).await;
    flight.release().await;

    let value: Option<String> = cache
        .secure()
        .unwrap()
        .interop_get(&storage_key)
        .await
        .unwrap();
    assert_eq!(value, None, "secure refresh must not resurrect a delete");
    assert_eq!(SECURE_DELETE_RACE_CALLS.load(Ordering::SeqCst), 2);
}

static SET_RACE_CALLS: AtomicU32 = AtomicU32::new(0);
static SET_REFRESH_STARTED: Notify = Notify::const_new();
static SET_REFRESH_RELEASE: Notify = Notify::const_new();

#[cachekit(
    client = cache,
    ttl = 4,
    interop = "swr_set_race",
    namespace = "swrtest"
)]
async fn swr_set_race(cache: &CacheKit, id: u64) -> Result<String, CachekitError> {
    let n = SET_RACE_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
    if n == 2 {
        SET_REFRESH_STARTED.notify_one();
        SET_REFRESH_RELEASE.notified().await;
    }
    Ok(format!("s{id}-c{n}"))
}

/// A newer explicit set wins over an origin result computed from the stale
/// snapshot, even when that refresh completes later.
#[tokio::test]
async fn concurrent_set_wins_over_an_older_refresh() {
    let cache = client(MockBackend::shared());
    let storage_key = key("swr_set_race", 12);

    assert_eq!(swr_set_race(&cache, 12).await.unwrap(), "s12-c1");
    tokio::time::sleep(Duration::from_millis(1400)).await;

    assert_eq!(swr_set_race(&cache, 12).await.unwrap(), "s12-c1");
    tokio::time::timeout(Duration::from_secs(2), SET_REFRESH_STARTED.notified())
        .await
        .expect("refresh origin started");
    cache
        .clone()
        .set_with_ttl(
            &storage_key,
            &"manual-write".to_owned(),
            Duration::from_secs(4),
        )
        .await
        .unwrap();

    SET_REFRESH_RELEASE.notify_one();
    let flight = cache.single_flight(&storage_key).await;
    flight.release().await;

    let value: Option<String> = cache.interop_get(&storage_key).await.unwrap();
    assert_eq!(value.as_deref(), Some("manual-write"));
    assert_eq!(SET_RACE_CALLS.load(Ordering::SeqCst), 2);
}

static RENEW_CALLS: AtomicU32 = AtomicU32::new(0);
static RENEW_REFRESH_STARTED: Notify = Notify::const_new();

#[cachekit(
    client = cache,
    ttl = 2,
    interop = "swr_expiry_renewal",
    namespace = "swrtest"
)]
async fn swr_expiry_renewal(cache: &CacheKit, id: u64) -> Result<String, CachekitError> {
    let n = RENEW_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
    if n == 2 {
        RENEW_REFRESH_STARTED.notify_one();
    }
    Ok(format!("r{id}-c{n}"))
}

/// A successful refresh renews moka's hard-expiry deadline from the commit,
/// rather than retaining the original entry's create deadline.
#[tokio::test]
async fn refresh_renews_the_full_l1_ttl() {
    let (backend, handle) = MockBackend::new_with_handle();
    let cache = client(backend);
    let storage_key = key("swr_expiry_renewal", 14);

    assert_eq!(swr_expiry_renewal(&cache, 14).await.unwrap(), "r14-c1");
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(swr_expiry_renewal(&cache, 14).await.unwrap(), "r14-c1");
    tokio::time::timeout(Duration::from_secs(2), RENEW_REFRESH_STARTED.notified())
        .await
        .expect("refresh origin started");

    // Join the refresh flight, then remove L2 so the later assertion can
    // succeed only through the renewed L1 entry.
    let flight = cache.single_flight(&storage_key).await;
    flight.release().await;
    assert_eq!(RENEW_CALLS.load(Ordering::SeqCst), 2);
    handle.store.lock().await.clear();

    // Past the original t+2s deadline, but comfortably before the refresh's
    // t+2s deadline (the refresh committed near t+700ms).
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let value: Option<String> = cache.interop_get(&storage_key).await.unwrap();
    assert_eq!(value.as_deref(), Some("r14-c2"));
}

static SLOW_RENEW_CALLS: AtomicU32 = AtomicU32::new(0);
static SLOW_RENEW_STARTED: Notify = Notify::const_new();
static SLOW_RENEW_RELEASE: Notify = Notify::const_new();

#[cachekit(
    client = cache,
    ttl = 2,
    interop = "swr_slow_expiry_renewal",
    namespace = "swrtest"
)]
async fn swr_slow_expiry_renewal(cache: &CacheKit, id: u64) -> Result<String, CachekitError> {
    let n = SLOW_RENEW_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
    if n == 2 {
        SLOW_RENEW_STARTED.notify_one();
        SLOW_RENEW_RELEASE.notified().await;
    }
    Ok(format!("sr{id}-c{n}"))
}

/// Hard expiry controls serving, not mutation identity. A valid origin result
/// that finishes after the stale entry expires must still repopulate L1/L2.
#[tokio::test]
async fn slow_refresh_can_commit_after_the_original_entry_expires() {
    let (backend, handle) = MockBackend::new_with_handle();
    let cache = client(backend);
    let storage_key = key("swr_slow_expiry_renewal", 15);

    assert_eq!(
        swr_slow_expiry_renewal(&cache, 15).await.unwrap(),
        "sr15-c1"
    );
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(
        swr_slow_expiry_renewal(&cache, 15).await.unwrap(),
        "sr15-c1"
    );
    tokio::time::timeout(Duration::from_secs(2), SLOW_RENEW_STARTED.notified())
        .await
        .expect("slow refresh origin started");

    // Cross the original t+2s hard-expiry deadline while origin work remains
    // in flight, then allow its still-current token to commit.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    SLOW_RENEW_RELEASE.notify_one();
    let flight = cache.single_flight(&storage_key).await;
    flight.release().await;
    assert_eq!(SLOW_RENEW_CALLS.load(Ordering::SeqCst), 2);

    // Remove L2: the refreshed value must now be coming from the renewed L1.
    handle.store.lock().await.clear();
    let value: Option<String> = cache.interop_get(&storage_key).await.unwrap();
    assert_eq!(value.as_deref(), Some("sr15-c2"));
}

// ── hard expiry falls through to a blocking miss ─────────────────────────────

static EXPIRY_CALLS: AtomicU32 = AtomicU32::new(0);

#[cachekit(client = cache, ttl = 2, interop = "swr_expiry", namespace = "swrtest")]
async fn swr_expiry_probe(cache: &CacheKit, id: u64) -> Result<String, CachekitError> {
    let n = EXPIRY_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
    tokio::time::sleep(ORIGIN_DELAY).await;
    Ok(format!("e{id}-c{n}"))
}

/// SWR never serves past hard expiry: once the entry's TTL elapses, the read
/// is a normal blocking miss + fill, and the caller waits for the origin.
#[tokio::test]
async fn hard_expired_entry_takes_the_blocking_miss_path() {
    let (backend, handle) = MockBackend::new_with_handle();
    let cache = client(backend);

    assert_eq!(swr_expiry_probe(&cache, 3).await.unwrap(), "e3-c1");

    // Cross hard expiry (ttl = 2 s). The mock L2 has no TTL support, so
    // clear it manually — this test is about the L1 expiry contract.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    handle.store.lock().await.clear();

    let started = Instant::now();
    let value = swr_expiry_probe(&cache, 3).await.unwrap();
    let elapsed = started.elapsed();

    assert_eq!(value, "e3-c2", "hard-expired read must recompute");
    assert!(
        elapsed >= ORIGIN_DELAY,
        "hard-expired read must block on the origin: took {elapsed:?}"
    );
    assert_eq!(EXPIRY_CALLS.load(Ordering::SeqCst), 2);
}

// ── fresh reads schedule nothing ─────────────────────────────────────────────

static FRESH_CALLS: AtomicU32 = AtomicU32::new(0);

#[cachekit(client = cache, ttl = 60, interop = "swr_fresh", namespace = "swrtest")]
async fn swr_fresh_probe(cache: &CacheKit, id: u64) -> Result<String, CachekitError> {
    let n = FRESH_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
    Ok(format!("f{id}-c{n}"))
}

/// A hit inside the freshness window is a plain hit: no background refresh
/// is scheduled, ever.
#[tokio::test]
async fn fresh_read_does_not_schedule_a_refresh() {
    let cache = client(MockBackend::shared());

    assert_eq!(swr_fresh_probe(&cache, 1).await.unwrap(), "f1-c1");
    assert_eq!(swr_fresh_probe(&cache, 1).await.unwrap(), "f1-c1");

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        FRESH_CALLS.load(Ordering::SeqCst),
        1,
        "a fresh hit must not spawn background work"
    );
}

// ── the off switch ───────────────────────────────────────────────────────────

static OFF_CALLS: AtomicU32 = AtomicU32::new(0);

#[cachekit(client = cache, ttl = 4, interop = "swr_off", namespace = "swrtest")]
async fn swr_off_probe(cache: &CacheKit, id: u64) -> Result<String, CachekitError> {
    let n = OFF_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
    Ok(format!("o{id}-c{n}"))
}

/// `.swr_enabled(false)` restores the pre-SWR contract exactly: entries are
/// plain hits until hard expiry, and no background refresh ever runs.
#[tokio::test]
async fn disabled_swr_serves_until_hard_expiry_without_refreshing() {
    let cache = CacheKit::builder()
        .backend(MockBackend::shared())
        .swr_enabled(false)
        // Would put the stale window at ~1 s of the 4 s TTL if SWR were on —
        // the probe below lands deep inside it, 2.5 s clear of hard expiry.
        .swr_threshold_ratio(0.25)
        .build()
        .unwrap();

    assert_eq!(swr_off_probe(&cache, 5).await.unwrap(), "o5-c1");

    // Deep in what would be the stale window (threshold would be ~1 s).
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(swr_off_probe(&cache, 5).await.unwrap(), "o5-c1");

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        OFF_CALLS.load(Ordering::SeqCst),
        1,
        "SWR off must mean zero background refreshes"
    );
}

// ── reference-typed arguments survive the 'static refresh capture ────────────

static STR_CALLS: AtomicU32 = AtomicU32::new(0);

#[cachekit(client = cache, ttl = 4, interop = "swr_str", namespace = "swrtest")]
async fn swr_str_probe(cache: &CacheKit, name: &str) -> Result<String, CachekitError> {
    let n = STR_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
    Ok(format!("{name}-c{n}"))
}

/// A `&str` argument is re-materialised as an owned `String` for the
/// `'static` refresh task and rebound as `&str` — the background refresh
/// runs the unchanged function body with an identical-typed argument.
#[tokio::test]
async fn str_argument_refreshes_in_the_background() {
    let cache = client(MockBackend::shared());

    assert_eq!(swr_str_probe(&cache, "ada").await.unwrap(), "ada-c1");

    tokio::time::sleep(Duration::from_millis(1400)).await;
    assert_eq!(swr_str_probe(&cache, "ada").await.unwrap(), "ada-c1");

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(STR_CALLS.load(Ordering::SeqCst), 2, "one refresh ran");
    assert_eq!(swr_str_probe(&cache, "ada").await.unwrap(), "ada-c2");
}

// ── config validation ────────────────────────────────────────────────────────

#[tokio::test]
async fn threshold_ratio_is_validated_at_build() {
    for bad in [0.0, -0.5, 1.5, f64::NAN] {
        let Err(err) = CacheKit::builder()
            .backend(MockBackend::shared())
            .swr_threshold_ratio(bad)
            .build()
        else {
            panic!("out-of-range ratio {bad} must fail at build");
        };
        assert!(matches!(err, CachekitError::Config(_)), "got {err:?}");
    }
    // Boundary: 1.0 is legal (stale only in the jitter margin before expiry).
    assert!(
        CacheKit::builder()
            .backend(MockBackend::shared())
            .swr_threshold_ratio(1.0)
            .build()
            .is_ok(),
        "ratio 1.0 is legal"
    );
}

// ── client clones share cache state ──────────────────────────────────────────

/// `CacheKit` clones share the L1 store (and single-flight map) — the clone
/// handed to a background refresh writes back into the same cache the
/// original reads from.
#[tokio::test]
async fn clones_share_l1_state() {
    let (backend, handle) = MockBackend::new_with_handle();
    let cache = CacheKit::builder().backend(backend).build().unwrap();
    let clone = cache.clone();

    cache.set("shared", &"value".to_owned()).await.unwrap();

    // Remove the L2 copy: a hit through the clone can only come from the
    // shared L1.
    handle.store.lock().await.clear();

    let via_clone: Option<String> = clone.get("shared").await.unwrap();
    assert_eq!(via_clone.as_deref(), Some("value"));
}
