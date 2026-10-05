//! Cold-miss single-flight: dedup concurrent fills of the same key.
//!
//! Under metered-misses pricing a stampede is literally billable — N tasks
//! missing the same key at once means N backend misses and N executions of
//! the wrapped function. [`CacheKit::single_flight`](crate::CacheKit::single_flight)
//! collapses that to one:
//!
//! - **In-process** (always available): a per-key async mutex. The first
//!   task through becomes the *leader* and computes; concurrent tasks queue
//!   behind it and re-check the cache once the leader finishes.
//! - **Cross-process** (`reliability` feature, native, backend implements
//!   `LockableBackend` — CachekitIO and Redis do): the leader additionally
//!   takes a distributed fill lock. If another process already holds it,
//!   this process polls the cache for the other side's fill instead of
//!   recomputing, and computes anyway once the poll budget is exhausted
//!   (fail-open — a stampede beats unavailability).
//!
//! The `#[cachekit]` macro wires this in automatically around its miss path.
//! Its stale-while-revalidate refresh takes the same locks but never waits
//! for another worker's fill: if another worker holds either one, or the lock
//! call fails, the refresh stands down without polling or re-reading the
//! cache, and the stale copy keeps being served. A cold miss queued behind a
//! holder that found the distributed lock contested and let go without a
//! fill (a refresh that stood down, a cancelled or failed poll) contests that
//! lock itself before computing, since another process may be filling.
//! Manual usage follows the same shape:
//!
//! ```no_run
//! # async fn example(cache: &cachekit::CacheKit) -> Result<(), cachekit::CachekitError> {
//! if let Some(_v) = cache.get::<String>("expensive").await? {
//!     return Ok(());
//! }
//! let mut flight = cache.single_flight("expensive").await;
//! while flight.wait_for_fill().await {
//!     if let Some(_v) = cache.get::<String>("expensive").await? {
//!         flight.release().await; // another worker filled it
//!         return Ok(());
//!     }
//! }
//! let value = "computed".to_owned(); // expensive work — runs once
//! cache.set("expensive", &value).await?;
//! flight.release().await;
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError, Weak};

#[cfg(all(feature = "l1", not(feature = "unsync"), not(target_arch = "wasm32")))]
use std::sync::atomic::{AtomicU64, Ordering};

use crate::client::SharedBackend;

/// Above this many live entries, dead map slots are swept opportunistically.
const SWEEP_THRESHOLD: usize = 128;

/// How long a distributed fill lock is held server-side before auto-expiry.
///
/// Also the cross-process suppression ceiling: a fill that runs longer than
/// this loses its lock mid-compute and another process may recompute
/// concurrently (fail-open by design — release is owner-checked, so an
/// expired lock is never wrongfully deleted). Workloads whose fills
/// routinely approach 5 s keep in-process dedup but should not rely on
/// cross-process suppression.
/// ponytail: fixed TTL, no heartbeat — add lock renewal if slow fills matter.
#[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
const FILL_LOCK_TIMEOUT_MS: u64 = 5_000;

/// Poll cadence while waiting for another process's fill.
#[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
const FILL_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

/// Poll budget: 50 × 100 ms ≈ the fill lock timeout.
#[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
const FILL_POLL_BUDGET: u32 = 50;

// ── FlightMap ────────────────────────────────────────────────────────────────

/// Per-key async mutexes for in-process fill dedup. Weak entries let finished
/// flights drop their state without an explicit removal protocol.
///
/// Each mutex guards a stand-down note: `true` when the last holder to try
/// the distributed lock found it contested, so it may have filled nothing (a
/// refresh stands down; a cold miss polls and may be cancelled or fail), and
/// a queued cold miss whose re-check misses must contest that lock before
/// computing. A holder that leads clears the note: on acquire, or when its
/// own lease contest makes it a leader. A follower leaves it as it found it,
/// whether it finds a fill, errors out or is dropped, so the next follower
/// still sees it. A stale `true` costs at most one lock call, by a follower
/// that was about to compute anyway.
#[derive(Default)]
pub(crate) struct FlightMap {
    entries: Mutex<HashMap<String, Weak<tokio::sync::Mutex<bool>>>>,
}

impl FlightMap {
    fn handle(&self, key: &str) -> Arc<tokio::sync::Mutex<bool>> {
        let mut map = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        // ponytail: O(n) sweep once the map grows; a doubly-indexed structure
        // is not worth it until someone caches millions of distinct cold keys.
        if map.len() > SWEEP_THRESHOLD {
            map.retain(|_, w| w.strong_count() > 0);
        }
        if let Some(existing) = map.get(key).and_then(Weak::upgrade) {
            return existing;
        }
        let fresh = Arc::new(tokio::sync::Mutex::new(false));
        map.insert(key.to_owned(), Arc::downgrade(&fresh));
        fresh
    }
}

// ── SWR mutation ordering ────────────────────────────────────────────────────

/// State shared by a stale-read token and same-key mutations while that token
/// is alive. The weak map can discard idle keys without losing correctness:
/// an outstanding token itself keeps this state alive.
#[cfg(all(feature = "l1", not(feature = "unsync"), not(target_arch = "wasm32")))]
pub(crate) struct MutationState {
    lock: Arc<tokio::sync::Mutex<()>>,
    version: AtomicU64,
}

#[cfg(all(feature = "l1", not(feature = "unsync"), not(target_arch = "wasm32")))]
impl MutationState {
    fn new() -> Self {
        Self {
            lock: Arc::new(tokio::sync::Mutex::new(())),
            version: AtomicU64::new(0),
        }
    }

    pub(crate) fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }
}

/// Per-key mutation versions for conditional SWR commits. Cloned clients
/// share this map. Each entry is weak so unrelated keys do not accumulate.
#[cfg(all(feature = "l1", not(feature = "unsync"), not(target_arch = "wasm32")))]
#[derive(Default)]
pub(crate) struct MutationMap {
    entries: Mutex<HashMap<String, Weak<MutationState>>>,
}

#[cfg(all(feature = "l1", not(feature = "unsync"), not(target_arch = "wasm32")))]
impl MutationMap {
    pub(crate) fn state(&self, key: &str) -> Arc<MutationState> {
        let mut map = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        if map.len() > SWEEP_THRESHOLD {
            map.retain(|_, weak| weak.strong_count() > 0);
        }
        if let Some(existing) = map.get(key).and_then(Weak::upgrade) {
            return existing;
        }
        let fresh = Arc::new(MutationState::new());
        map.insert(key.to_owned(), Arc::downgrade(&fresh));
        fresh
    }

    pub(crate) async fn lock(&self, key: &str) -> MutationGuard {
        let state = self.state(key);
        let lock = Arc::clone(&state.lock).lock_owned().await;
        MutationGuard { state, _lock: lock }
    }
}

/// Holds same-key ordering across an L2 mutation and its L1 update.
#[cfg(all(feature = "l1", not(feature = "unsync"), not(target_arch = "wasm32")))]
pub(crate) struct MutationGuard {
    state: Arc<MutationState>,
    _lock: tokio::sync::OwnedMutexGuard<()>,
}

#[cfg(all(feature = "l1", not(feature = "unsync"), not(target_arch = "wasm32")))]
impl MutationGuard {
    pub(crate) fn snapshot(&self) -> (Arc<MutationState>, u64) {
        (Arc::clone(&self.state), self.state.version())
    }

    pub(crate) fn is_current(&self, state: &Arc<MutationState>, version: u64) -> bool {
        Arc::ptr_eq(&self.state, state) && self.state.version() == version
    }

    pub(crate) fn advance(&self) {
        self.state.version.fetch_add(1, Ordering::Release);
    }
}

// ── SingleFlight guard ───────────────────────────────────────────────────────

enum Role {
    /// First worker in: compute without re-checking (a re-check would be a
    /// second billable miss under metered-misses pricing).
    Leader,
    /// Queued behind a local holder that has since finished: re-check the
    /// cache once — a leader's fill is in L1 — then compute if it missed.
    /// `contest_lease` is the stand-down note this follower found (see
    /// [`FlightMap`]): another process may be filling, so a missed re-check
    /// contests the lease before computing.
    LocalFollower {
        rechecked: bool,
        #[cfg_attr(
            not(all(feature = "reliability", not(target_arch = "wasm32"))),
            allow(dead_code)
        )]
        contest_lease: bool,
    },
    /// Another *process* holds the distributed fill lock: poll the cache for
    /// its fill, then compute anyway when the budget runs out (fail-open).
    #[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
    RemoteContested { polls_left: u32 },
}

/// Outcome of one distributed fill-lock attempt. The cold-miss and refresh
/// paths apply different policies to it.
#[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
enum LockAttempt {
    /// The lease was granted; carries its id.
    Granted(String),
    /// The backend has no distributed lock.
    Unlockable,
    /// Another process holds the lease.
    Contested,
    /// The lock call itself failed.
    Failed,
}

#[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
impl LockAttempt {
    async fn run(backend: &SharedBackend, full_key: &str) -> Self {
        let Some(lockable) = backend.as_lockable() else {
            return Self::Unlockable;
        };
        match lockable.acquire_lock(full_key, FILL_LOCK_TIMEOUT_MS).await {
            Ok(Some(lock_id)) => Self::Granted(lock_id),
            Ok(None) => Self::Contested,
            Err(err) => {
                crate::metrics::trace_lock_error(full_key, "lock", &err);
                Self::Failed
            }
        }
    }

    /// Cold-miss policy: poll a contested lease; a lock-infrastructure error
    /// fails open to a plain leader — suppression is an optimisation, never
    /// an availability dependency.
    fn cold_role(self) -> (Role, Option<String>) {
        match self {
            Self::Granted(lock_id) => (Role::Leader, Some(lock_id)),
            Self::Unlockable | Self::Failed => (Role::Leader, None),
            Self::Contested => (
                Role::RemoteContested {
                    polls_left: FILL_POLL_BUDGET,
                },
                None,
            ),
        }
    }
}

/// Guard for a single-flight fill, returned by
/// [`CacheKit::single_flight`](crate::CacheKit::single_flight).
///
/// Holds the per-key in-process lock for its whole lifetime, and the
/// distributed fill lock (if one was acquired) until [`Self::release`].
/// Dropping without `release` is safe: the in-process lock frees immediately
/// and a distributed lock expires server-side after its timeout.
pub struct SingleFlight {
    /// The per-key lock. Its value is the stand-down note (see
    /// [`FlightMap`]).
    local: tokio::sync::OwnedMutexGuard<bool>,
    role: Role,
    #[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
    backend: SharedBackend,
    #[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
    full_key: String,
    /// Id of the distributed fill lock this flight holds, if any.
    #[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
    lock_id: Option<String>,
}

impl SingleFlight {
    /// `true` while another worker may still be filling this key — re-check
    /// the cache after every `true` before computing yourself:
    ///
    /// - Leader: immediately `false` (compute, don't re-read your own miss).
    /// - Queued behind a local holder: `true` once, for the re-check, then
    ///   `false`. If the holder found the distributed lock contested and let
    ///   go without a fill, a missed re-check first contests that lock and
    ///   then behaves as a leader or a contested flight.
    /// - Contested cross-process: sleeps one poll interval per call, `true`
    ///   until the poll budget is spent.
    pub async fn wait_for_fill(&mut self) -> bool {
        #[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
        if let Role::LocalFollower {
            rechecked: true,
            contest_lease: true,
        } = self.role
        {
            (self.role, self.lock_id) = LockAttempt::run(&self.backend, &self.full_key)
                .await
                .cold_role();
            self.note_role();
        }
        match &mut self.role {
            Role::Leader => false,
            Role::LocalFollower { rechecked, .. } => {
                let first = !*rechecked;
                *rechecked = true;
                first
            }
            #[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
            Role::RemoteContested { polls_left } => {
                if *polls_left == 0 {
                    return false;
                }
                *polls_left -= 1;
                tokio::time::sleep(FILL_POLL_INTERVAL).await;
                true
            }
        }
    }

    /// Release the flight. Best-effort: frees the distributed fill lock (if
    /// held) so other processes stop waiting early. A failed release is
    /// traced (feature `tracing`) and otherwise ignored: the lock expires
    /// server-side regardless. Leaves the stand-down note as it is (see
    /// [`FlightMap`]), since a caller may release after an error, with no fill.
    pub async fn release(self) {
        #[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
        if let (Some(lock_id), Some(lockable)) = (&self.lock_id, self.backend.as_lockable()) {
            if let Err(err) = lockable.release_lock(&self.full_key, lock_id).await {
                crate::metrics::trace_lock_error(&self.full_key, "unlock", &err);
            }
        }
    }

    /// Record this flight's role in the stand-down note (see [`FlightMap`]):
    /// a leader clears it, a contested flight sets it, since it fills nothing
    /// unless its poll runs out, and a follower leaves it as found.
    fn note_role(&mut self) {
        match self.role {
            Role::Leader => *self.local = false,
            Role::LocalFollower { .. } => {}
            #[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
            Role::RemoteContested { .. } => *self.local = true,
        }
    }

    /// A flight over a freshly acquired per-key lock, its role recorded in
    /// the stand-down note.
    fn new(
        local: tokio::sync::OwnedMutexGuard<bool>,
        role: Role,
        backend: &SharedBackend,
        full_key: &str,
    ) -> Self {
        #[cfg(not(all(feature = "reliability", not(target_arch = "wasm32"))))]
        let _ = (backend, full_key);
        let mut flight = Self {
            local,
            role,
            #[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
            backend: backend.clone(),
            #[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
            full_key: full_key.to_owned(),
            #[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
            lock_id: None,
        };
        flight.note_role();
        flight
    }

    /// Cold-miss entry: lead (attempting cross-process suppression via the
    /// backend's distributed lock, when available), or queue behind a local
    /// holder.
    pub(crate) async fn acquire(map: &FlightMap, backend: &SharedBackend, full_key: &str) -> Self {
        let handle = map.handle(full_key);
        let Ok(local) = Arc::clone(&handle).try_lock_owned() else {
            // Contended: a local holder is filling. Queue behind it.
            let local = handle.lock_owned().await;
            let role = Role::LocalFollower {
                rechecked: false,
                contest_lease: *local,
            };
            return Self::new(local, role, backend, full_key);
        };
        #[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
        {
            let (role, lock_id) = LockAttempt::run(backend, full_key).await.cold_role();
            let mut flight = Self::new(local, role, backend, full_key);
            flight.lock_id = lock_id;
            flight
        }
        #[cfg(not(all(feature = "reliability", not(target_arch = "wasm32"))))]
        Self::new(local, Role::Leader, backend, full_key)
    }

    /// Refresh-ahead entry: lead the fill, or `None` to stand down at once.
    ///
    /// A refresh runs only while a stale copy is still being served, so it
    /// never waits for another worker's fill and never re-reads the cache
    /// (`spec/saas-api.md` API-62/63). It leads only with the lease, or on a
    /// backend without one. A held in-process flight, a contested lease or a
    /// failed lock call all mean stand down: the stale copy keeps being
    /// served, and once it hard-expires the cold-miss path takes over.
    pub(crate) async fn try_lead(
        map: &FlightMap,
        backend: &SharedBackend,
        full_key: &str,
    ) -> Option<Self> {
        let local = map.handle(full_key).try_lock_owned().ok()?;
        #[cfg_attr(
            not(all(feature = "reliability", not(target_arch = "wasm32"))),
            allow(unused_mut)
        )]
        let mut flight = Self::new(local, Role::Leader, backend, full_key);
        #[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
        match LockAttempt::run(backend, full_key).await {
            LockAttempt::Granted(lock_id) => flight.lock_id = Some(lock_id),
            LockAttempt::Unlockable => {}
            LockAttempt::Contested => {
                // Leave a note: a cold miss queued behind this refresh must
                // not take the stand-down for a finished fill.
                *flight.local = true;
                return None;
            }
            LockAttempt::Failed => return None,
        }
        Some(flight)
    }
}
