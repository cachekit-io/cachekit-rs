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
//! Manual usage follows the same shape:
//!
//! ```no_run
//! # async fn example(cache: &cachekit::CacheKit) -> Result<(), cachekit::CachekitError> {
//! if let Some(_v) = cache.get::<String>("expensive").await? {
//!     return Ok(());
//! }
//! let mut flight = cache.single_flight("expensive").await;
//! while flight.awaiting_fill().await {
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

use crate::client::SharedBackend;

/// Above this many live entries, dead map slots are swept opportunistically.
const SWEEP_THRESHOLD: usize = 128;

/// How long a distributed fill lock is held server-side before auto-expiry.
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
#[derive(Default)]
pub(crate) struct FlightMap {
    entries: Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>,
}

impl FlightMap {
    fn handle(&self, key: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut map = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        // ponytail: O(n) sweep once the map grows; a doubly-indexed structure
        // is not worth it until someone caches millions of distinct cold keys.
        if map.len() > SWEEP_THRESHOLD {
            map.retain(|_, w| w.strong_count() > 0);
        }
        if let Some(existing) = map.get(key).and_then(Weak::upgrade) {
            return existing;
        }
        let fresh = Arc::new(tokio::sync::Mutex::new(()));
        map.insert(key.to_owned(), Arc::downgrade(&fresh));
        fresh
    }
}

// ── SingleFlight guard ───────────────────────────────────────────────────────

enum Role {
    /// First worker in: compute without re-checking (a re-check would be a
    /// second billable miss under metered-misses pricing).
    Leader,
    /// Queued behind a local leader that has since finished: re-check the
    /// cache once — the leader's fill is in L1 — then compute if it missed.
    LocalFollower { rechecked: bool },
    /// Another *process* holds the distributed fill lock: poll the cache for
    /// its fill, then compute anyway when the budget runs out (fail-open).
    #[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
    RemoteContested { polls_left: u32 },
}

/// Guard for a single-flight fill, returned by
/// [`CacheKit::single_flight`](crate::CacheKit::single_flight).
///
/// Holds the per-key in-process lock for its whole lifetime, and the
/// distributed fill lock (if one was acquired) until [`Self::release`].
/// Dropping without `release` is safe: the in-process lock frees immediately
/// and a distributed lock expires server-side after its timeout.
pub struct SingleFlight {
    _local: tokio::sync::OwnedMutexGuard<()>,
    role: Role,
    #[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
    dist: Option<DistLock>,
}

#[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
struct DistLock {
    backend: SharedBackend,
    full_key: String,
    lock_id: String,
}

impl SingleFlight {
    /// `true` while another worker may still be filling this key — re-check
    /// the cache after every `true` before computing yourself:
    ///
    /// - Leader: immediately `false` (compute, don't re-read your own miss).
    /// - Queued behind a local leader: `true` exactly once.
    /// - Contested cross-process: sleeps one poll interval per call, `true`
    ///   until the poll budget is spent.
    pub async fn awaiting_fill(&mut self) -> bool {
        match &mut self.role {
            Role::Leader => false,
            Role::LocalFollower { rechecked } => {
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
    /// held) so other processes stop waiting early; errors are ignored — the
    /// lock expires server-side regardless.
    pub async fn release(self) {
        #[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
        if let Some(dist) = self.dist {
            if let Some(lockable) = dist.backend.as_lockable() {
                let _ = lockable.release_lock(&dist.full_key, &dist.lock_id).await;
            }
        }
    }

    pub(crate) async fn acquire(map: &FlightMap, backend: &SharedBackend, full_key: &str) -> Self {
        let handle = map.handle(full_key);
        match Arc::clone(&handle).try_lock_owned() {
            Ok(local) => Self::lead(local, backend, full_key).await,
            Err(_) => {
                // Contended: a local leader is filling. Queue behind it.
                let local = handle.lock_owned().await;
                Self {
                    _local: local,
                    role: Role::LocalFollower { rechecked: false },
                    #[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
                    dist: None,
                }
            }
        }
    }

    /// Local leader: attempt cross-process suppression via the backend's
    /// distributed lock, when available. Lock-infrastructure errors fail
    /// open to a plain leader — suppression is an optimisation, never an
    /// availability dependency.
    #[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
    async fn lead(
        local: tokio::sync::OwnedMutexGuard<()>,
        backend: &SharedBackend,
        full_key: &str,
    ) -> Self {
        let (role, dist) = match backend.as_lockable() {
            Some(lockable) => match lockable.acquire_lock(full_key, FILL_LOCK_TIMEOUT_MS).await {
                Ok(Some(lock_id)) => (
                    Role::Leader,
                    Some(DistLock {
                        backend: backend.clone(),
                        full_key: full_key.to_owned(),
                        lock_id,
                    }),
                ),
                Ok(None) => (
                    Role::RemoteContested {
                        polls_left: FILL_POLL_BUDGET,
                    },
                    None,
                ),
                Err(_) => (Role::Leader, None),
            },
            None => (Role::Leader, None),
        };
        Self {
            _local: local,
            role,
            dist,
        }
    }

    #[cfg(not(all(feature = "reliability", not(target_arch = "wasm32"))))]
    async fn lead(
        local: tokio::sync::OwnedMutexGuard<()>,
        _backend: &SharedBackend,
        _full_key: &str,
    ) -> Self {
        Self {
            _local: local,
            role: Role::Leader,
        }
    }
}
