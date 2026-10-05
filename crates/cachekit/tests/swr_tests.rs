//! Integration tests for L1 stale-while-revalidate.
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

/// The core SWR contract in one scenario: a read past the SWR threshold
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
    assert!(cache
        .secure_cache()
        .unwrap()
        .delete(&storage_key)
        .await
        .unwrap());

    SECURE_DELETE_REFRESH_RELEASE.notify_one();
    let flight = cache.single_flight(&storage_key).await;
    flight.release().await;

    let value: Option<String> = cache
        .secure_cache()
        .unwrap()
        .interop_get(&storage_key)
        .await
        .unwrap();
    assert_eq!(value, None, "secure refresh must not resurrect a delete");
    assert_eq!(SECURE_DELETE_RACE_CALLS.load(Ordering::SeqCst), 2);
}

#[cfg(feature = "encryption")]
static PLAIN_ENCRYPTED_CALLS: AtomicU32 = AtomicU32::new(0);

#[cfg(feature = "encryption")]
#[cachekit(
    client = cache,
    ttl = 4,
    interop = "swr_plain_encrypted",
    namespace = "swrtest"
)]
async fn swr_plain_encrypted(cache: &CacheKit, id: u64) -> Result<String, CachekitError> {
    let n = PLAIN_ENCRYPTED_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
    Ok(format!("pe{id}-c{n}"))
}

/// A plain (non-`secure`) function's background refresh on a client with
/// encryption configured commits ciphertext, like every other write on it.
#[cfg(feature = "encryption")]
#[tokio::test]
async fn plain_refresh_on_encrypted_client_commits_ciphertext() {
    const TENANT: &str = "swr-plain-tenant";
    let (shared, backend) = MockBackend::new_with_handle();
    let cache = CacheKit::builder()
        .backend(shared)
        .swr_threshold_ratio(0.25)
        .encryption_from_bytes(&[7_u8; 32], TENANT)
        .expect("encryption configures")
        .build()
        .expect("client builds");
    let storage_key = key("swr_plain_encrypted", 17);

    assert_eq!(swr_plain_encrypted(&cache, 17).await.unwrap(), "pe17-c1");
    tokio::time::sleep(Duration::from_millis(1400)).await;
    // Stale: served at once while one refresh runs in the background.
    assert_eq!(swr_plain_encrypted(&cache, 17).await.unwrap(), "pe17-c1");

    let layer = cachekit::EncryptionLayer::new(&[7_u8; 32], TENANT).unwrap();
    let refreshed = cachekit::serializer::serialize(&"pe17-c2").unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let stored = backend.store.lock().await.get(&storage_key).cloned();
        if stored.is_some_and(|s| layer.decrypt(&s, &storage_key).ok() == Some(refreshed.clone())) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the refresh never committed ciphertext of the new value"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
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

// ── a refresh never waits and never records a miss ───────────────────────────
//
// A refresh runs while the stale copy is still being served, so a worker that
// already holds this key's flight (in-process) or fill lock (cross-process)
// means the refresh has nothing to do: it stands down at once, as it does when
// the lock call fails. It must not
// poll for the other side's fill, re-read the cache (a re-read that misses is
// a billed miss in `X-CacheKit-Misses`), or recompute and PUT without the
// lease (`spec/saas-api.md` API-62/63).

/// Poll until `done` holds, failing after 2 s.
async fn eventually(what: &str, done: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[cfg(feature = "reliability")]
const LOCK_GRANT: u8 = 0;
#[cfg(feature = "reliability")]
const LOCK_CONTESTED: u8 = 1;
#[cfg(feature = "reliability")]
const LOCK_FAILING: u8 = 2;

/// Lock-capable mock. `acquire_lock` grants the fill lock in `LOCK_GRANT`
/// mode. In the other modes it first takes a permit from `gate` (no permits
/// until a test adds them, so a test can hold a caller inside the lock call),
/// then reports the lock held by another process or fails.
#[cfg(feature = "reliability")]
#[derive(Clone)]
struct ContestedBackend {
    mock: MockBackend,
    mode: std::sync::Arc<std::sync::atomic::AtomicU8>,
    /// Every `acquire_lock` call, counted on entry.
    acquires: std::sync::Arc<AtomicU32>,
    sets: std::sync::Arc<AtomicU32>,
    gate: std::sync::Arc<tokio::sync::Semaphore>,
}

#[cfg(feature = "reliability")]
impl ContestedBackend {
    fn new_with_handle() -> (cachekit::SharedBackend, Self) {
        let backend = Self {
            mock: MockBackend::default(),
            mode: std::sync::Arc::default(),
            acquires: std::sync::Arc::default(),
            sets: std::sync::Arc::default(),
            gate: std::sync::Arc::new(tokio::sync::Semaphore::new(0)),
        };
        let handle = backend.clone();
        (std::sync::Arc::new(backend), handle)
    }

    fn set_mode(&self, mode: u8) {
        self.mode.store(mode, Ordering::SeqCst);
    }

    fn acquires(&self) -> u32 {
        self.acquires.load(Ordering::SeqCst)
    }

    fn sets(&self) -> u32 {
        self.sets.load(Ordering::SeqCst)
    }
}

#[cfg(feature = "reliability")]
#[async_trait::async_trait]
impl cachekit::backend::Backend for ContestedBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, cachekit::BackendError> {
        self.mock.get(key).await
    }

    async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), cachekit::BackendError> {
        self.sets.fetch_add(1, Ordering::SeqCst);
        self.mock.set(key, value, ttl).await
    }

    async fn delete(&self, key: &str) -> Result<bool, cachekit::BackendError> {
        self.mock.delete(key).await
    }

    async fn exists(&self, key: &str) -> Result<bool, cachekit::BackendError> {
        self.mock.exists(key).await
    }

    async fn health(&self) -> Result<cachekit::backend::HealthStatus, cachekit::BackendError> {
        self.mock.health().await
    }

    fn as_lockable(&self) -> Option<&dyn cachekit::backend::LockableBackend> {
        Some(self)
    }
}

#[cfg(feature = "reliability")]
#[async_trait::async_trait]
impl cachekit::backend::LockableBackend for ContestedBackend {
    async fn acquire_lock(
        &self,
        _key: &str,
        _timeout_ms: u64,
    ) -> Result<Option<String>, cachekit::BackendError> {
        self.acquires.fetch_add(1, Ordering::SeqCst);
        if self.mode.load(Ordering::SeqCst) == LOCK_GRANT {
            return Ok(Some("lock-1".to_owned()));
        }
        self.gate.acquire().await.expect("gate open").forget();
        match self.mode.load(Ordering::SeqCst) {
            LOCK_FAILING => Err(cachekit::BackendError::transient("lock endpoint down")),
            _ => Ok(None),
        }
    }

    async fn release_lock(
        &self,
        _key: &str,
        _lock_id: &str,
    ) -> Result<bool, cachekit::BackendError> {
        Ok(true)
    }
}

/// Run `stale_read`, let its refresh's lock call through, and check that the
/// refresh stood down at once: it let go of the key's flight (a 50 ms probe
/// takes it), and made one lock call, no read, no PUT and no
/// origin run.
#[cfg(feature = "reliability")]
async fn assert_refresh_stands_down<F: std::future::Future<Output = String>>(
    cache: &CacheKit,
    mock: &ContestedBackend,
    flight_key: &str,
    calls: &AtomicU32,
    stale_read: F,
) {
    let (acquires, sets, origin_runs) =
        (mock.acquires(), mock.sets(), calls.load(Ordering::SeqCst));
    assert_eq!(stale_read.await, "u1-c1", "stale is served");
    let after_stale_read = cache.stats();
    eventually("the refresh's lock call", || {
        mock.acquires() == acquires + 1
    })
    .await;
    mock.gate.add_permits(2); // the refresh's lock call, then the probe's

    let probe = tokio::time::timeout(Duration::from_millis(50), cache.single_flight(flight_key))
        .await
        .expect("the refresh must stand down, not hold the flight to poll or compute");
    drop(probe);

    // The probe calls the lock only if it led; it queues as a follower when
    // it arrived before the refresh let go.
    assert!(
        mock.acquires() <= acquires + 2,
        "the refresh made one lock call, and no retry"
    );
    assert_eq!(
        cache.stats().misses,
        after_stale_read.misses,
        "the refresh counted no miss"
    );
    assert_eq!(cache.stats(), after_stale_read, "the refresh read nothing");
    assert_eq!(mock.sets(), sets, "the refresh sent no PUT");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        origin_runs,
        "origin not re-run"
    );
}

#[cfg(feature = "reliability")]
static CONTESTED_CALLS: AtomicU32 = AtomicU32::new(0);

#[cfg(feature = "reliability")]
#[cachekit(client = cache, ttl = 4, interop = "swr_contested", namespace = "swrtest")]
async fn swr_contested(cache: &CacheKit, id: u64) -> Result<String, CachekitError> {
    let n = CONTESTED_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
    Ok(format!("u{id}-c{n}"))
}

/// Stale copy still in L1, fill lock held by another process: the refresh
/// stands down at once. Before the fix it slept one 100 ms poll, then
/// re-read its own stale copy.
#[cfg(feature = "reliability")]
#[tokio::test]
async fn contested_refresh_stands_down_without_polling() {
    let (backend, mock) = ContestedBackend::new_with_handle();
    let cache = client(backend);
    assert_eq!(swr_contested(&cache, 1).await.unwrap(), "u1-c1");
    mock.set_mode(LOCK_CONTESTED);
    tokio::time::sleep(Duration::from_millis(1400)).await;

    assert_refresh_stands_down(
        &cache,
        &mock,
        &key("swr_contested", 1),
        &CONTESTED_CALLS,
        async { swr_contested(&cache, 1).await.unwrap() },
    )
    .await;
}

#[cfg(feature = "reliability")]
static LOCK_ERROR_CALLS: AtomicU32 = AtomicU32::new(0);

#[cfg(feature = "reliability")]
#[cachekit(client = cache, ttl = 4, interop = "swr_lock_error", namespace = "swrtest")]
async fn swr_lock_error(cache: &CacheKit, id: u64) -> Result<String, CachekitError> {
    let n = LOCK_ERROR_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
    Ok(format!("u{id}-c{n}"))
}

/// The lock call itself fails (a 429 or 5xx from the lock endpoint): the
/// refresh abandons instead of failing open. Unlike a cold miss it already
/// has a value to serve, and recomputing and PUTting without the lease from
/// every process would add load to a backend that is shedding it
/// (`spec/saas-api.md` § Revalidation flow, step 2).
#[cfg(feature = "reliability")]
#[tokio::test]
async fn refresh_abandons_when_the_lock_call_fails() {
    let (backend, mock) = ContestedBackend::new_with_handle();
    let cache = client(backend);
    assert_eq!(swr_lock_error(&cache, 1).await.unwrap(), "u1-c1");
    mock.set_mode(LOCK_FAILING);
    tokio::time::sleep(Duration::from_millis(1400)).await;

    assert_refresh_stands_down(
        &cache,
        &mock,
        &key("swr_lock_error", 1),
        &LOCK_ERROR_CALLS,
        async { swr_lock_error(&cache, 1).await.unwrap() },
    )
    .await;
}

#[cfg(feature = "reliability")]
static CONTESTED_EXPIRY_CALLS: AtomicU32 = AtomicU32::new(0);

#[cfg(feature = "reliability")]
#[cachekit(client = cache, ttl = 2, interop = "swr_contested_expiry", namespace = "swrtest")]
async fn swr_contested_expiry(cache: &CacheKit, id: u64) -> Result<String, CachekitError> {
    let n = CONTESTED_EXPIRY_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
    Ok(format!("u{id}-c{n}"))
}

/// Fill lock held by another process, and by the time the refresh hears so
/// its L1 copy has hard-expired and L2 misses. Before the fix the refresh
/// polled ~5 s, counting a miss on every re-read, then recomputed and PUT
/// without the lease — the stampede the lock exists to prevent.
#[cfg(feature = "reliability")]
#[tokio::test]
async fn contested_refresh_after_l1_expiry_neither_polls_nor_puts() {
    let (backend, mock) = ContestedBackend::new_with_handle();
    let cache = client(backend); // ttl 2 s → threshold 0.5 s (±10%)
    let warmed = Instant::now();
    assert_eq!(swr_contested_expiry(&cache, 1).await.unwrap(), "u1-c1");
    mock.set_mode(LOCK_CONTESTED);
    tokio::time::sleep(Duration::from_millis(800)).await;

    // The refresh waits inside the contested lock call while the L1 copy
    // hard-expires and L2 empties; then its lock call is answered.
    assert_refresh_stands_down(
        &cache,
        &mock,
        &key("swr_contested_expiry", 1),
        &CONTESTED_EXPIRY_CALLS,
        async {
            let stale = swr_contested_expiry(&cache, 1).await.unwrap();
            mock.mock.store.lock().await.clear();
            tokio::time::sleep(
                (warmed + Duration::from_millis(2300)).saturating_duration_since(Instant::now()),
            )
            .await;
            stale
        },
    )
    .await;
}

#[cfg(feature = "reliability")]
static BEHIND_REFRESH_CALLS: AtomicU32 = AtomicU32::new(0);

#[cfg(feature = "reliability")]
#[cachekit(client = cache, ttl = 2, interop = "swr_behind_refresh", namespace = "swrtest")]
async fn swr_behind_refresh(cache: &CacheKit, id: u64) -> Result<String, CachekitError> {
    let n = BEHIND_REFRESH_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
    Ok(format!("u{id}-c{n}"))
}

/// A cold miss that queued behind a refresh, while the refresh waited on a
/// contested lock, must not take the refresh's stand-down for a finished
/// fill. It contests the lease itself and picks up the holder's fill, instead
/// of running the origin and PUTting without the lease.
#[cfg(feature = "reliability")]
#[tokio::test]
async fn cold_miss_queued_behind_a_contested_refresh_contests_the_lease() {
    let (backend, mock) = ContestedBackend::new_with_handle();
    let cache = client(backend); // ttl 2 s → threshold 0.5 s (±10%)
    let warmed = Instant::now();
    assert_eq!(swr_behind_refresh(&cache, 1).await.unwrap(), "u1-c1");
    mock.set_mode(LOCK_CONTESTED);
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Stale read; its refresh holds the key's flight inside the lock call.
    assert_eq!(swr_behind_refresh(&cache, 1).await.unwrap(), "u1-c1");
    eventually("the refresh's lock call", || mock.acquires() == 2).await;

    // The L1 copy hard-expires and L2 is empty: the next call is a cold miss,
    // and it queues behind the refresh.
    mock.mock.store.lock().await.clear();
    tokio::time::sleep(
        (warmed + Duration::from_millis(2300)).saturating_duration_since(Instant::now()),
    )
    .await;
    let cold = tokio::spawn({
        let cache = cache.clone();
        async move { swr_behind_refresh(&cache, 1).await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Answer the refresh's lock call. The refresh stands down, and the queued
    // cold miss must contest the lease rather than compute.
    mock.gate.add_permits(1);
    eventually("the cold miss's lock call", || mock.acquires() == 3).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        BEHIND_REFRESH_CALLS.load(Ordering::SeqCst),
        1,
        "no origin run while another process holds the lease"
    );

    // The holder fills, and the cold miss's poll picks that fill up.
    let holder = CacheKit::builder()
        .backend(std::sync::Arc::new(mock.clone()))
        .no_l1()
        .build()
        .expect("holder client builds");
    holder
        .set(&key("swr_behind_refresh", 1), &"u1-holder".to_owned())
        .await
        .unwrap();
    let sets = mock.sets();
    mock.gate.add_permits(1);
    let value = tokio::time::timeout(Duration::from_secs(2), cold)
        .await
        .expect("the cold miss finds the holder's fill")
        .expect("cold-miss task panicked")
        .unwrap();
    assert_eq!(value, "u1-holder");
    assert_eq!(
        BEHIND_REFRESH_CALLS.load(Ordering::SeqCst),
        1,
        "origin not re-run"
    );
    assert_eq!(mock.sets(), sets, "no PUT without the lease");
}

static QUEUED_CALLS: AtomicU32 = AtomicU32::new(0);

#[cachekit(client = cache, ttl = 2, interop = "swr_queued", namespace = "swrtest")]
async fn swr_queued(cache: &CacheKit, id: u64) -> Result<String, CachekitError> {
    let n = QUEUED_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
    Ok(format!("u{id}-c{n}"))
}

/// Another in-process worker holds the key's flight when the refresh starts.
/// The refresh stands down instead of queueing behind it. Before the fix it
/// queued, then re-read the cache once the holder let go: with the L1 copy
/// expired and L2 empty, that re-read counted a miss and the refresh ran the
/// origin and PUT.
#[tokio::test]
async fn refresh_behind_a_held_flight_stands_down_without_a_miss() {
    let (backend, mock) = MockBackend::new_with_handle();
    let cache = client(backend); // ttl 2 s → threshold 0.5 s (±10%)
    let warmed = Instant::now();
    assert_eq!(swr_queued(&cache, 1).await.unwrap(), "u1-c1");
    tokio::time::sleep(Duration::from_millis(800)).await;

    let held = cache.single_flight(&key("swr_queued", 1)).await;
    assert_eq!(
        swr_queued(&cache, 1).await.unwrap(),
        "u1-c1",
        "stale is served"
    );
    let after_stale_read = cache.stats();

    mock.store.lock().await.clear();
    tokio::time::sleep(
        (warmed + Duration::from_millis(2300)).saturating_duration_since(Instant::now()),
    )
    .await;
    held.release().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(
        cache.stats().misses,
        after_stale_read.misses,
        "the refresh counted no miss"
    );
    assert_eq!(cache.stats(), after_stale_read, "the refresh read nothing");
    assert!(
        mock.store.lock().await.is_empty(),
        "the refresh sent no PUT"
    );
    assert_eq!(QUEUED_CALLS.load(Ordering::SeqCst), 1, "origin not re-run");
}
