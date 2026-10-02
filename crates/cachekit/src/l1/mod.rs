use bytes::Bytes;
use moka::sync::Cache;
use moka::Expiry;
use std::time::{Duration, Instant};

#[derive(Clone)]
struct L1Entry {
    // Bytes, not Vec: moka's `get` clones the entry, and a refcount bump is
    // cheaper than a deep copy of a payload of up to the 5 MiB cap.
    data: Bytes,
    ttl: Duration,
    created_at: Instant,
    freshness_jitter: f64,
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

    fn expire_after_update(
        &self,
        _key: &String,
        value: &L1Entry,
        _updated_at: std::time::Instant,
        _duration_until_expiry: Option<Duration>,
    ) -> Option<Duration> {
        // A successful SWR refresh is an update of the existing moka entry.
        // Returning the replacement value's TTL renews hard expiry from the
        // refresh commit instead of retaining the original create deadline.
        Some(value.ttl)
    }
}

/// Outcome of an SWR-aware L1 read — see [`L1Cache::get_with_swr`].
#[derive(Debug, Clone, PartialEq)]
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

/// [`L1SwrRead`] over a shared buffer: the client's copy-free read path.
pub(crate) enum SharedSwrRead {
    Fresh(Bytes),
    Stale(Bytes),
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
        self.get_shared(key).map(|data| data.to_vec())
    }

    /// [`Self::get`] without the copy: the stored buffer, by refcount.
    pub(crate) fn get_shared(&self, key: &str) -> Option<Bytes> {
        self.store.get(key).map(|entry| entry.data)
    }

    /// Whether a live (unexpired) entry exists, without reading its value.
    ///
    /// Unlike [`Self::get`], this does not count as an access for moka's
    /// eviction policy.
    pub(crate) fn contains(&self, key: &str) -> bool {
        self.store.contains_key(key)
    }

    /// Retrieve cached bytes with stale-while-revalidate classification.
    ///
    /// An entry is *fresh* until it has lived `threshold_ratio` of its own
    /// TTL (±10% jitter, drawn once when the entry is inserted so a hot read
    /// never touches the entropy source), *stale* from then until hard
    /// expiry, and a *miss* after that. The freshness window
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
        match self.get_with_swr_shared(key, threshold_ratio) {
            SharedSwrRead::Fresh(data) => L1SwrRead::Fresh(data.to_vec()),
            SharedSwrRead::Stale(data) => L1SwrRead::Stale(data.to_vec()),
            SharedSwrRead::Miss => L1SwrRead::Miss,
        }
    }

    /// [`Self::get_with_swr`] without the copy: the stored buffer, by refcount.
    pub(crate) fn get_with_swr_shared(&self, key: &str, threshold_ratio: f64) -> SharedSwrRead {
        let Some(entry) = self.store.get(key) else {
            return SharedSwrRead::Miss;
        };
        let threshold = entry.ttl.as_secs_f64() * threshold_ratio * entry.freshness_jitter;
        if entry.created_at.elapsed().as_secs_f64() > threshold {
            SharedSwrRead::Stale(entry.data)
        } else {
            SharedSwrRead::Fresh(entry.data)
        }
    }

    /// Insert or overwrite an entry with the given TTL.
    pub fn set(&self, key: &str, value: &[u8], ttl: Duration) {
        self.set_shared(key, Bytes::copy_from_slice(value), ttl);
    }

    /// [`Self::set`] without the copy: L1 keeps `value`'s buffer.
    pub(crate) fn set_shared(&self, key: &str, value: Bytes, ttl: Duration) {
        self.store.insert(
            key.to_string(),
            L1Entry {
                data: value,
                ttl,
                created_at: Instant::now(),
                freshness_jitter: 0.9 + crate::random_unit() * 0.2,
            },
        );
    }

    /// Remove an entry by key.
    pub fn delete(&self, key: &str) {
        self.store.invalidate(key);
    }

    /// Number of entries currently held.
    ///
    /// Runs moka's pending housekeeping first, so expired and evicted entries
    /// are already gone from the count — the exact figure at the time of the
    /// call, not moka's eventually-consistent estimate.
    pub fn entry_count(&self) -> u64 {
        self.store.run_pending_tasks();
        self.store.entry_count()
    }

    /// Drive moka's internal eviction machinery. Useful in tests to force
    /// pending invalidations and expiry checks to complete synchronously.
    pub fn run_pending_tasks(&self) {
        self.store.run_pending_tasks();
    }
}

#[cfg(test)]
mod tests {
    use super::L1Cache;
    use std::time::Duration;

    /// `contains` backs `CacheKit::exists`; it must honour per-entry TTL the
    /// way `get` does, or a warm-key check would report an expired entry.
    #[test]
    fn contains_is_false_after_ttl_expiry() {
        let cache = L1Cache::new(16);
        cache.set("k", b"v", Duration::from_millis(50));
        assert!(cache.contains("k"));
        std::thread::sleep(Duration::from_millis(100));
        assert!(!cache.contains("k"));
    }
}
