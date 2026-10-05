//! Paused-clock call-shape tests for the `#[cachekit]` cold-miss fill lock:
//! which backend round trips a cold miss waits for before it returns, and
//! how long a contested follower polls. Virtual time makes every figure
//! exact.
//!
//! Run under each fill-lock build:
//!   cargo test --test fill_lock_timing_tests --features macros
//!   cargo test --test fill_lock_timing_tests --no-default-features --features reliability,macros
//!   cargo test --test fill_lock_timing_tests --no-default-features --features reliability,macros,unsync
//!
//! The first build releases a stored fill's lock without waiting for the
//! unlock; the other two keep the unlock on the caller's path.

#![cfg(all(
    feature = "macros",
    feature = "reliability",
    not(target_arch = "wasm32")
))]
// The measured figures are the report: `--nocapture` shows them.
#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::time::Instant;

use cachekit::backend::{Backend, HealthStatus, LockableBackend};
use cachekit::client::SharedBackend;
use cachekit::error::BackendError;
use cachekit::interop::{interop_key, serialize_value, InteropValue};
use cachekit::{cachekit, CacheKit, CachekitError};

/// The build releases a stored fill's lock without waiting for the unlock.
const DETACHED: bool = cfg!(all(feature = "l1", not(feature = "unsync")));

/// US p50 legs from the perf sweep (ms).
const GET: u64 = 202;
const LOCK: u64 = 179;
const ORIGIN: u64 = 50;
const SET: u64 = 190;
const UNLOCK: u64 = 112;

/// The contested-poll deadline: the 5 s fill lock timeout.
const FILL_LOCK_TIMEOUT: u64 = 5_000;
const POLL_INTERVAL: u64 = 100;

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

// ── Fixed-latency lockable backend ───────────────────────────────────────────

#[derive(Default)]
struct State {
    store: HashMap<String, Vec<u8>>,
    lock_held: bool,
    /// Every completed backend call, with when it completed.
    ops: Vec<(&'static str, Instant)>,
    gets: u32,
}

#[derive(Clone)]
struct Timed {
    state: Arc<Mutex<State>>,
    unlock_ms: u64,
    set_fails: bool,
    /// GETs after this many hang for 30 s.
    hang_after_gets: Option<u32>,
}

impl Timed {
    fn new() -> Self {
        Self {
            state: Arc::default(),
            unlock_ms: UNLOCK,
            set_fails: false,
            hang_after_gets: None,
        }
    }

    fn client(&self) -> CacheKit {
        #[cfg(not(feature = "unsync"))]
        let shared: SharedBackend = Arc::new(self.clone());
        #[cfg(feature = "unsync")]
        let shared: SharedBackend = std::rc::Rc::new(self.clone());
        CacheKit::builder()
            .backend(shared)
            .build()
            .expect("client builds")
    }

    fn st(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap()
    }

    async fn leg(&self, op: &'static str, d: u64) {
        tokio::time::sleep(ms(d)).await;
        self.st().ops.push((op, Instant::now()));
    }

    /// Backend calls that completed by `t`, in order.
    fn ops_by(&self, t: Instant) -> Vec<&'static str> {
        self.ops_between(Instant::now() - Duration::from_secs(86_400), t)
    }

    /// Backend calls that completed after `from` and by `to`, in order.
    fn ops_between(&self, from: Instant, to: Instant) -> Vec<&'static str> {
        self.st()
            .ops
            .iter()
            .filter(|(_, at)| from < *at && *at <= to)
            .map(|(op, _)| *op)
            .collect()
    }

    fn gets(&self) -> u32 {
        self.st().gets
    }

    fn unlocks(&self) -> usize {
        self.st()
            .ops
            .iter()
            .filter(|(op, _)| *op == "unlock")
            .count()
    }
}

#[cfg_attr(not(feature = "unsync"), async_trait)]
#[cfg_attr(feature = "unsync", async_trait(?Send))]
impl Backend for Timed {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        let n = {
            let mut st = self.st();
            st.gets += 1;
            st.gets
        };
        if self.hang_after_gets.is_some_and(|after| n > after) {
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
        self.leg("get", GET).await;
        Ok(self.st().store.get(key).cloned())
    }

    async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        _ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        self.leg("set", SET).await;
        if self.set_fails {
            return Err(BackendError::permanent("set rejected"));
        }
        self.st().store.insert(key.to_owned(), value);
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<bool, BackendError> {
        Ok(self.st().store.remove(key).is_some())
    }

    async fn exists(&self, key: &str) -> Result<bool, BackendError> {
        Ok(self.st().store.contains_key(key))
    }

    async fn health(&self) -> Result<HealthStatus, BackendError> {
        Ok(HealthStatus {
            is_healthy: true,
            latency_ms: 0.0,
            backend_type: "timed-lock-mock".to_owned(),
            details: HashMap::new(),
        })
    }

    fn as_lockable(&self) -> Option<&dyn LockableBackend> {
        Some(self)
    }
}

#[cfg_attr(not(feature = "unsync"), async_trait)]
#[cfg_attr(feature = "unsync", async_trait(?Send))]
impl LockableBackend for Timed {
    async fn acquire_lock(
        &self,
        _key: &str,
        _timeout_ms: u64,
    ) -> Result<Option<String>, BackendError> {
        self.leg("lock", LOCK).await;
        let mut st = self.st();
        if st.lock_held {
            return Ok(None);
        }
        st.lock_held = true;
        Ok(Some("lease".to_owned()))
    }

    async fn release_lock(&self, _key: &str, _lock_id: &str) -> Result<bool, BackendError> {
        self.leg("unlock", self.unlock_ms).await;
        self.st().lock_held = false;
        Ok(true)
    }
}

// ── Wrapped functions ────────────────────────────────────────────────────────

thread_local! {
    /// When the wrapped body last started running on this test's thread.
    static ORIGIN_STARTED: std::cell::Cell<Option<Instant>> = const { std::cell::Cell::new(None) };
}

fn origin_started() -> Instant {
    ORIGIN_STARTED.get().expect("origin ran")
}

#[cachekit(client = cache, ttl = 600, interop = "fill", namespace = "timing")]
async fn fill(cache: &CacheKit, id: u64) -> Result<u64, CachekitError> {
    ORIGIN_STARTED.set(Some(Instant::now()));
    tokio::time::sleep(ms(ORIGIN)).await;
    Ok(id * 10)
}

#[cachekit(client = cache, ttl = 600, interop = "fail", namespace = "timing")]
async fn fail(cache: &CacheKit, id: u64) -> Result<u64, CachekitError> {
    tokio::time::sleep(ms(ORIGIN)).await;
    Err(CachekitError::Config(format!("origin {id} failed")))
}

fn fill_key(id: u64) -> String {
    interop_key("timing", "fill", &[InteropValue::from(id)]).expect("key")
}

fn since(t0: Instant) -> u64 {
    u64::try_from(t0.elapsed().as_millis()).expect("fits")
}

// ── Leader and local follower: the unlock leaves the caller's path ───────────

#[tokio::test(start_paused = true)]
async fn cold_miss_leader_returns_without_waiting_for_the_unlock() {
    let backend = Timed::new();
    let cache = backend.client();
    let t0 = Instant::now();

    assert_eq!(fill(&cache, 1).await.unwrap(), 10);
    let returned = Instant::now();

    let inline = GET + LOCK + ORIGIN + SET + UNLOCK; // 733
    let (want_ms, want_ops) = if DETACHED {
        (inline - UNLOCK, vec!["get", "lock", "set"])
    } else {
        (inline, vec!["get", "lock", "set", "unlock"])
    };
    println!(
        "leader cold miss: {} ms, awaited {:?}",
        since(t0),
        backend.ops_by(returned)
    );
    assert_eq!(since(t0), want_ms);
    assert_eq!(backend.ops_by(returned), want_ops);

    // The unlock is still sent. (+1: a timer at the same instant may wake
    // this task before the release task records its call.)
    tokio::time::sleep(ms(UNLOCK + 1)).await;
    assert_eq!(backend.unlocks(), 1);
}

#[tokio::test(start_paused = true)]
async fn same_key_local_follower_returns_without_waiting_for_the_unlock() {
    let backend = Timed::new();
    let cache = backend.client();
    let t0 = Instant::now();

    let follower = async {
        // Start just behind the leader so it queues on the in-process flight.
        tokio::task::yield_now().await;
        let v = fill(&cache, 2).await.unwrap();
        (v, Instant::now())
    };
    let ((leader, _), (follower, follower_done)) = tokio::join!(
        async { (fill(&cache, 2).await.unwrap(), Instant::now()) },
        follower
    );
    assert_eq!((leader, follower), (20, 20));

    // The follower re-checks once the leader frees the flight: from L1 when
    // it is on, otherwise with one more GET.
    let leader_done = GET + LOCK + ORIGIN + SET + if DETACHED { 0 } else { UNLOCK };
    let recheck = if cfg!(feature = "l1") { 0 } else { GET };
    println!(
        "local follower: {} ms, unlock done before return: {}",
        u64::try_from((follower_done - t0).as_millis()).unwrap(),
        backend.ops_by(follower_done).contains(&"unlock")
    );
    assert_eq!(follower_done - t0, ms(leader_done + recheck));
    assert_eq!(backend.ops_by(follower_done).contains(&"unlock"), !DETACHED);
}

// ── Inline unlock after a failed store or an Err ─────────────────────────────

#[tokio::test(start_paused = true)]
async fn failed_set_keeps_the_unlock_inline() {
    let mut backend = Timed::new();
    backend.set_fails = true;
    let cache = backend.client();
    let t0 = Instant::now();

    assert_eq!(fill(&cache, 3).await.unwrap(), 30);
    let returned = Instant::now();
    assert_eq!(since(t0), GET + LOCK + ORIGIN + SET + UNLOCK);
    assert_eq!(backend.ops_by(returned), ["get", "lock", "set", "unlock"]);
}

#[tokio::test(start_paused = true)]
async fn err_result_keeps_the_unlock_inline() {
    let backend = Timed::new();
    let cache = backend.client();
    let t0 = Instant::now();

    assert!(fail(&cache, 4).await.is_err());
    let returned = Instant::now();
    assert_eq!(since(t0), GET + LOCK + ORIGIN + UNLOCK);
    assert_eq!(backend.ops_by(returned), ["get", "lock", "unlock"]);
}

// ── Re-miss inside the unlock window ─────────────────────────────────────────

/// A same-key re-call that misses while this process's unlock is still in
/// flight must wait for that unlock, then lead: never contest its own lease
/// and poll an empty cache until the deadline.
#[tokio::test(start_paused = true)]
async fn re_miss_inside_the_unlock_window_leads_instead_of_polling() {
    let mut backend = Timed::new();
    backend.unlock_ms = 1_000; // far longer than the re-call's path to its lock call
    let cache = backend.client();

    assert_eq!(fill(&cache, 5).await.unwrap(), 50);
    cache.delete(&fill_key(5)).await.unwrap();
    let gets_before = backend.gets();

    let t1 = Instant::now();
    assert_eq!(fill(&cache, 5).await.unwrap(), 50);
    let took = since(t1);
    let ops = backend.ops_between(t1, Instant::now());

    // One GET (the miss), no poll GETs, and the lease was granted, not contested.
    assert_eq!(backend.gets() - gets_before, 1, "no poll GETs");
    // Detached: the leader's unlock is still in flight, so the re-call sends
    // it itself before its own lock call, which is granted. Inline: the
    // leader returned only after its unlock.
    let (want_ms, want_ops) = if DETACHED {
        (
            GET + 1_000 + LOCK + ORIGIN + SET,
            vec!["get", "unlock", "lock", "set"],
        )
    } else {
        (
            GET + LOCK + ORIGIN + SET + 1_000,
            vec!["get", "lock", "set", "unlock"],
        )
    };
    println!(
        "re-miss inside the unlock window: {took} ms (5,000+ if it had contested its own lease)"
    );
    assert_eq!(ops, want_ops);
    assert_eq!(took, want_ms);
}

// ── Contested follower: bounded by the lock timeout ──────────────────────────

/// Polls before this change: a fixed 50, each 100 ms plus a GET.
fn head_poll_wait(get_ms: u64) -> u64 {
    50 * (POLL_INTERVAL + get_ms)
}

#[tokio::test(start_paused = true)]
async fn contested_follower_whose_leader_never_fills_computes_at_the_deadline() {
    let backend = Timed::new();
    backend.st().lock_held = true; // another process leads and never fills
    let cache = backend.client();
    let t0 = Instant::now();

    assert_eq!(fill(&cache, 6).await.unwrap(), 60);
    let computed = origin_started() - t0;

    // The lock attempt starts after the first GET.
    let deadline = GET + FILL_LOCK_TIMEOUT;
    let gets = backend.gets();
    println!(
        "contested, no fill: computes at {} ms with {gets} GETs; before this change \
         {} ms with 51 GETs",
        computed.as_millis(),
        GET + LOCK + head_poll_wait(GET)
    );
    assert_eq!(computed, ms(deadline));
    // 1 miss + 16 polls: the 16th is cut off at the deadline.
    assert_eq!(gets, 17);
}

#[tokio::test(start_paused = true)]
async fn contested_follower_whose_leader_errs_computes_at_the_deadline() {
    let backend = Timed::new();
    backend.st().lock_held = true;
    let cache = backend.client();
    let t0 = Instant::now();

    // The remote leader fails at t = 1,000 ms: it unlocks and stores nothing.
    let leader_err = async {
        tokio::time::sleep(ms(1_000)).await;
        backend.st().lock_held = false;
    };
    let (value, ()) = tokio::join!(fill(&cache, 7), leader_err);
    assert_eq!(value.unwrap(), 70);
    let computed = origin_started() - t0;
    println!(
        "contested, leader Err: computes at {} ms with {} GETs",
        computed.as_millis(),
        backend.gets()
    );
    assert_eq!(computed, ms(GET + FILL_LOCK_TIMEOUT));
}

#[tokio::test(start_paused = true)]
async fn deadline_cuts_off_a_hung_poll_get() {
    let mut backend = Timed::new();
    backend.hang_after_gets = Some(1); // every poll GET hangs for 30 s
    backend.st().lock_held = true;
    let cache = backend.client();
    let t0 = Instant::now();

    assert_eq!(fill(&cache, 8).await.unwrap(), 80);
    let computed = origin_started() - t0;
    println!(
        "contested, poll GET hangs: computes at {} ms with {} GETs",
        computed.as_millis(),
        backend.gets()
    );
    assert_eq!(computed, ms(GET + FILL_LOCK_TIMEOUT));
    assert_eq!(
        backend.gets(),
        2,
        "the first poll GET hangs until the deadline"
    );
}

#[tokio::test(start_paused = true)]
async fn a_remote_fill_is_still_picked_up_within_one_poll() {
    let backend = Timed::new();
    backend.st().lock_held = true;
    let cache = backend.client();
    let t0 = Instant::now();

    // Polling starts once the contested lock call returns; the remote fill
    // lands 300 ms after that.
    let polling_from = GET + LOCK;
    let lands = polling_from + 300;
    let remote_fill = async {
        tokio::time::sleep(ms(lands)).await;
        let bytes = serialize_value(&InteropValue::from(90_u64)).unwrap();
        backend.st().store.insert(fill_key(9), bytes);
    };
    let (value, ()) = tokio::join!(fill(&cache, 9), remote_fill);
    assert_eq!(value.unwrap(), 90);
    let picked_up = since(t0);
    println!("remote fill landing at {lands} ms picked up at {picked_up} ms");
    // The first poll's GET (+100 to +302) reads after the fill lands at +300.
    assert_eq!(picked_up, polling_from + POLL_INTERVAL + GET);
}

// ── The leader's runtime idles or goes away before the unlock is sent ───────

fn current_thread() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime builds")
}

/// One client shared by two current_thread runtimes. The leader's runtime
/// sits idle after its call, so its spawned unlock never runs. A re-miss on
/// the other runtime must not wait for that task: it sends the unlock itself
/// and leads. Real clock, so the bound is loose.
#[test]
fn re_miss_does_not_wait_on_an_unlock_whose_runtime_idles() {
    let backend = Timed::new();
    let cache = backend.client();
    let idle = current_thread();
    idle.block_on(async { assert_eq!(fill(&cache, 10).await.unwrap(), 100) });
    current_thread().block_on(async {
        cache.delete(&fill_key(10)).await.unwrap();
        let gets = backend.gets();
        let t = Instant::now();
        let value = tokio::time::timeout(Duration::from_secs(4), fill(&cache, 10))
            .await
            .expect("the re-miss must not wait on an idle runtime's task");
        assert_eq!(value.unwrap(), 100);
        println!(
            "re-miss beside an idle runtime: {} ms, {} GETs",
            t.elapsed().as_millis(),
            backend.gets() - gets
        );
        assert_eq!(backend.gets() - gets, 1, "led without polling");
    });
    drop(idle);
}

/// The leader's runtime is dropped right after its call (a runtime per
/// call), cancelling the spawned unlock before it is sent. A re-miss must
/// still find the unsent unlock, send it, and lead.
#[test]
fn re_miss_after_the_leaders_runtime_is_dropped_sends_the_unlock_and_leads() {
    let backend = Timed::new();
    let cache = backend.client();
    {
        let per_call = tokio::runtime::Runtime::new().expect("runtime builds");
        per_call.block_on(async { assert_eq!(fill(&cache, 11).await.unwrap(), 110) });
    }
    current_thread().block_on(async {
        cache.delete(&fill_key(11)).await.unwrap();
        let gets = backend.gets();
        let t = Instant::now();
        assert_eq!(fill(&cache, 11).await.unwrap(), 110);
        println!(
            "re-miss after the runtime drop: {} ms, {} GETs, {} unlocks",
            t.elapsed().as_millis(),
            backend.gets() - gets,
            backend.unlocks()
        );
        assert_eq!(backend.gets() - gets, 1, "led without polling");
    });
}
