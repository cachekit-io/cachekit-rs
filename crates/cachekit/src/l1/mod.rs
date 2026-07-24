use moka::sync::Cache;
use moka::Expiry;
use std::time::{Duration, Instant};

#[derive(Clone)]
struct L1Entry {
    data: Vec<u8>,
    ttl: Duration,
    created_at: Instant,
}

struct L1Expiry;

impl Expiry<String, L1Entry> for L1Expiry {
    fn expire_after_create(
        &self,
        _key: &String,
        value: &L1Entry,
        _created_at: std::time::Instant,
    ) -> Option<Duration> {
        Some(value.ttl)
    }
}

/// Outcome of an SWR-aware L1 read — see [`L1Cache::get_with_swr`].
pub enum L1SwrRead {
    /// Entry present and within its freshness window: serve as-is.
    Fresh(Vec<u8>),
    /// Entry present but past the freshness threshold (and before hard
    /// expiry): serve it, but the caller should schedule a background
    /// refresh.
    Stale(Vec<u8>),
    /// Entry absent or hard-expired: a normal (blocking) miss.
    Miss,
}

/// In-process LRU cache with per-entry TTL, backed by [`moka`].
///
/// Used as the L1 layer in the dual-layer cache architecture. `Clone` is
/// cheap and shares the underlying store (moka is internally referenced).
#[derive(Clone)]
pub struct L1Cache {
    store: Cache<String, L1Entry>,
}

impl L1Cache {
    /// Create a new L1 cache with the given maximum entry capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            store: Cache::builder()
                .max_capacity(u64::try_from(capacity).unwrap_or(u64::MAX))
                .expire_after(L1Expiry)
                .build(),
        }
    }

    /// Retrieve cached bytes by key, or `None` if absent or expired.
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.store.get(key).map(|entry| entry.data.clone())
    }

    /// Retrieve cached bytes with stale-while-revalidate classification.
    ///
    /// An entry is *fresh* until it has lived `threshold_ratio` of its own
    /// TTL (±10% jitter, so refreshes de-synchronise across processes —
    /// same jitter as the Python and TypeScript SDKs), *stale* from then
    /// until hard expiry, and a *miss* after that. The freshness window
    /// derives from the TTL the entry was **inserted** with: a direct write
    /// carries the caller's full TTL, an L2 backfill carries the capped
    /// backfill TTL — see `CacheKit`'s L1 documentation.
    ///
    /// Semantics mirror cachekit-py's `swr_threshold_ratio` (elapsed
    /// lifetime > ratio × TTL ⇒ stale). Hard expiry is enforced by moka:
    /// an expired entry is never returned, so SWR can never serve past it.
    ///
    /// This is a pure read — it does not track refresh state. Callers own
    /// refresh scheduling and deduplication (the `#[cachekit]` macro uses
    /// `CacheKit::single_flight`).
    pub fn get_with_swr(&self, key: &str, threshold_ratio: f64) -> L1SwrRead {
        let Some(entry) = self.store.get(key) else {
            return L1SwrRead::Miss;
        };
        let jitter = 0.9 + crate::random_unit() * 0.2;
        let threshold = entry.ttl.as_secs_f64() * threshold_ratio * jitter;
        if entry.created_at.elapsed().as_secs_f64() > threshold {
            L1SwrRead::Stale(entry.data.clone())
        } else {
            L1SwrRead::Fresh(entry.data.clone())
        }
    }

    /// Insert or overwrite an entry with the given TTL.
    pub fn set(&self, key: &str, value: &[u8], ttl: Duration) {
        self.store.insert(
            key.to_string(),
            L1Entry {
                data: value.to_vec(),
                ttl,
                created_at: Instant::now(),
            },
        );
    }

    /// Remove an entry by key.
    pub fn delete(&self, key: &str) {
        self.store.invalidate(key);
    }

    /// Drive moka's internal eviction machinery. Useful in tests to force
    /// pending invalidations and expiry checks to complete synchronously.
    pub fn run_pending_tasks(&self) {
        self.store.run_pending_tasks();
    }
}
