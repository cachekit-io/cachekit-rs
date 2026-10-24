use bytes::Bytes;
use quick_cache::sync::{Cache, DefaultLifecycle};
use quick_cache::{OptionsBuilder, UnitWeighter};
use std::hash::RandomState;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone)]
struct L1Entry {
    // Bytes, not Vec: quick_cache's `get` clones the entry, and a refcount
    // bump is cheaper than a deep copy of a payload of up to the 5 MiB cap.
    data: Bytes,
    ttl: Duration,
    created_at: Instant,
    /// `created_at + ttl`, so a read compares one `Instant` instead of
    /// subtracting two. `None` when the sum overflows `Instant`: that entry
    /// never expires.
    expires_at: Option<Instant>,
    freshness_jitter: f64,
}

impl L1Entry {
    fn is_live(&self, now: Instant) -> bool {
        self.expires_at.is_none_or(|deadline| now < deadline)
    }
}

/// SipHash-1-3 keyed from the OS RNG (std's `RandomState`), pinned as the
/// hasher type parameter. quick_cache's default hasher is foldhash, which
/// makes no claim to resist hash flooding. Its `custom-hasher` feature only
/// changes `DefaultHashBuilder`, which this type never names, so no feature
/// enabled anywhere in a build can swap the hasher.
type Store = Cache<String, L1Entry, UnitWeighter, RandomState, DefaultLifecycle<String, L1Entry>>;

/// Fewest entries a shard gets before L1 adds another. A full shard evicts
/// even while other shards have room, so small shards lose hits on a working
/// set near capacity. At 128, filling a 1,000-entry L1 with distinct keys
/// loses about 2% of them to that, where quick_cache's own default of up to
/// 32 shards loses about 7%, and the 4 shards still beat a single lock.
const MIN_SHARD_ENTRIES: usize = 128;

/// Most shards L1 uses: past this, another lock buys little.
const MAX_SHARDS: usize = 64;

/// The largest power of two at or below `capacity / MIN_SHARD_ENTRIES`,
/// within `1..=MAX_SHARDS`. It depends on capacity alone, so eviction is the
/// same on every host; quick_cache's own default scales with the core count.
fn shard_count(capacity: usize) -> usize {
    1 << (capacity / MIN_SHARD_ENTRIES).clamp(1, MAX_SHARDS).ilog2()
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

/// In-process cache with per-entry TTL, backed by [`quick_cache`].
///
/// Used as the L1 layer in the dual-layer cache architecture. `Clone` is
/// cheap and shares the underlying store.
///
/// It never holds more than `capacity` entries. quick_cache evicts with a
/// scan-resistant policy (a CLOCK-Pro variant close to S3-FIFO): a run of
/// keys written once and never read does not flush the entries being read,
/// as it would under LRU. Capacity is split into shards, `capacity / 128`
/// rounded down to a power of two, from 1 to 64 (4 at the default 1,000),
/// and a full shard evicts even while others have room.
///
/// No read returns an expired entry. An expired entry leaves memory when a
/// read finds it, when eviction reaches it, or when [`Self::entry_count`] or
/// [`Self::run_pending_tasks`] sweeps; until then it counts against capacity.
#[derive(Clone)]
pub struct L1Cache {
    store: Arc<Store>,
}

impl L1Cache {
    /// Create a new L1 cache that holds at most `capacity` entries.
    pub fn new(capacity: usize) -> Self {
        let shards = shard_count(capacity);
        // Floor per shard, where quick_cache would round up, so the shards
        // never hold more than `capacity` between them.
        let weight = u64::try_from(capacity / shards * shards).unwrap_or(u64::MAX);
        let options = OptionsBuilder::new()
            .shards(shards)
            .estimated_items_capacity(capacity)
            .weight_capacity(weight)
            .build();
        let store = match options {
            Ok(options) => Store::with_options(
                options,
                UnitWeighter,
                RandomState::new(),
                DefaultLifecycle::default(),
            ),
            // `build` fails only when a capacity is unset, and both are set
            // above. Fall back to quick_cache's own shard count rather than
            // panic while a client is being built.
            Err(_) => Store::with(
                capacity,
                weight,
                UnitWeighter,
                RandomState::new(),
                DefaultLifecycle::default(),
            ),
        };
        Self {
            store: Arc::new(store),
        }
    }

    /// Retrieve cached bytes by key, or `None` if absent or expired.
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.get_shared(key).map(|data| data.to_vec())
    }

    /// [`Self::get`] without the copy: the stored buffer, by refcount.
    pub(crate) fn get_shared(&self, key: &str) -> Option<Bytes> {
        let entry = self.store.get(key)?;
        if entry.is_live(Instant::now()) {
            return Some(entry.data);
        }
        self.remove_expired(key, &entry);
        None
    }

    /// Whether a live (unexpired) entry exists, without reading its value.
    ///
    /// Unlike [`Self::get`], this does not count as an access for eviction.
    pub(crate) fn contains(&self, key: &str) -> bool {
        let Some(entry) = self.store.peek(key) else {
            return false;
        };
        if entry.is_live(Instant::now()) {
            return true;
        }
        self.remove_expired(key, &entry);
        false
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
    /// lifetime > ratio × TTL ⇒ stale). Hard expiry is checked on every
    /// read: an expired entry is never returned, so SWR can never serve past
    /// it.
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
        let now = Instant::now();
        if !entry.is_live(now) {
            self.remove_expired(key, &entry);
            return SharedSwrRead::Miss;
        }
        let threshold = entry.ttl.as_secs_f64() * threshold_ratio * entry.freshness_jitter;
        let age = now.saturating_duration_since(entry.created_at);
        if age.as_secs_f64() > threshold {
            SharedSwrRead::Stale(entry.data)
        } else {
            SharedSwrRead::Fresh(entry.data)
        }
    }

    /// Insert or overwrite an entry with the given TTL.
    ///
    /// An overwrite is a new entry: its TTL and freshness window start now.
    pub fn set(&self, key: &str, value: &[u8], ttl: Duration) {
        self.set_shared(key, Bytes::copy_from_slice(value), ttl);
    }

    /// [`Self::set`] without the copy: L1 keeps `value`'s buffer.
    pub(crate) fn set_shared(&self, key: &str, value: Bytes, ttl: Duration) {
        let created_at = Instant::now();
        self.store.insert(
            key.to_owned(),
            L1Entry {
                data: value,
                ttl,
                created_at,
                expires_at: created_at.checked_add(ttl),
                freshness_jitter: 0.9 + crate::random_unit() * 0.2,
            },
        );
    }

    /// Remove an entry by key.
    pub fn delete(&self, key: &str) {
        self.store.remove(key);
    }

    /// Number of entries currently held.
    ///
    /// Sweeps expired entries first (see [`Self::run_pending_tasks`]), so the
    /// figure is exact at the time of the call. The sweep visits every entry:
    /// poll this, don't put it on a hot path.
    pub fn entry_count(&self) -> u64 {
        self.run_pending_tasks();
        u64::try_from(self.store.len()).unwrap_or(u64::MAX)
    }

    /// Remove every expired entry now, one shard at a time.
    ///
    /// Reads never return an expired entry, so this only frees the memory
    /// and the capacity they hold. It visits every entry, holding each
    /// shard's write lock while it walks that shard.
    pub fn run_pending_tasks(&self) {
        let now = Instant::now();
        self.store.retain(|_, entry| entry.is_live(now));
    }

    /// Remove `seen`, an expired entry a read just found under `key`, unless
    /// a write replaced it in the meantime. A replacement was created later,
    /// and its buffer cannot sit at `seen`'s address while `seen` holds that
    /// buffer alive; either difference spares it.
    fn remove_expired(&self, key: &str, seen: &L1Entry) {
        self.store.remove_if(key, |current| {
            current.created_at == seen.created_at && current.data.as_ptr() == seen.data.as_ptr()
        });
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)] // test-only: a failed thread or fixture should panic loudly
mod tests {
    use super::{shard_count, L1Cache, L1SwrRead};
    use std::time::Duration;

    const LONG: Duration = Duration::from_secs(60);

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

    #[test]
    fn shard_count_follows_capacity_alone() {
        for (capacity, shards) in [
            (0, 1),
            (1, 1),
            (255, 1),
            (256, 2),
            (1000, 4),
            (1023, 4),
            (1024, 8),
            (8191, 32),
            (8192, 64),
            (usize::MAX, 64),
        ] {
            assert_eq!(shard_count(capacity), shards, "capacity {capacity}");
        }
        assert_eq!(L1Cache::new(1000).store.num_shards(), 4);
    }

    /// Per-shard capacity rounds down, so no capacity ever lets L1 hold more
    /// entries than it was given, under any amount of churn.
    #[test]
    fn never_holds_more_than_its_capacity() {
        for capacity in [0usize, 1, 2, 16, 127, 128, 255, 256, 1000, 1001, 4100] {
            let cache = L1Cache::new(capacity);
            assert!(
                cache.store.capacity() <= capacity as u64,
                "capacity {capacity}"
            );
            for i in 0..capacity * 3 + 10 {
                cache.set(&format!("key-{i}"), b"v", LONG);
                assert!(
                    cache.store.len() <= capacity,
                    "capacity {capacity} after {i}"
                );
            }
        }
    }

    /// Capacity is a ceiling, not a reservation: quick_cache allocates its
    /// tables as entries arrive, so a large capacity costs nothing up front.
    #[test]
    fn a_large_capacity_reserves_nothing_up_front() {
        let cache = L1Cache::new(5_000_000);
        assert_eq!(cache.store.memory_used().total(), 0);
        cache.set("k", b"v", LONG);
        assert!(cache.store.memory_used().total() > 0);
    }

    #[test]
    fn zero_capacity_holds_nothing() {
        let cache = L1Cache::new(0);
        cache.set("k", b"v", LONG);
        assert_eq!(cache.get("k"), None);
        assert_eq!(cache.entry_count(), 0);
    }

    /// Every read that finds an expired entry removes it, so its memory is
    /// freed without waiting for a sweep.
    #[test]
    fn a_read_that_finds_an_expired_entry_frees_it() {
        let cache = L1Cache::new(16);
        for key in ["get", "swr", "exists"] {
            cache.set(key, b"v", Duration::from_millis(1));
        }
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(cache.store.len(), 3);
        assert_eq!(cache.get("get"), None);
        assert_eq!(cache.get_with_swr("swr", 0.5), L1SwrRead::Miss);
        assert!(!cache.contains("exists"));
        assert_eq!(cache.store.len(), 0);
    }

    /// The removal after an expired read must not take a newer value that a
    /// concurrent write put under the same key in the meantime.
    #[test]
    fn expired_removal_spares_a_newer_write() {
        let cache = L1Cache::new(16);
        cache.set("k", b"old", Duration::from_millis(1));
        let seen = cache.store.get("k").expect("just written");
        std::thread::sleep(Duration::from_millis(20));
        cache.set("k", b"new", LONG);
        cache.remove_expired("k", &seen);
        assert_eq!(cache.get("k").as_deref(), Some(&b"new"[..]));
    }

    #[test]
    fn entry_count_sweeps_expired_entries() {
        let cache = L1Cache::new(64);
        for i in 0..5 {
            cache.set(&format!("short-{i}"), b"v", Duration::from_millis(1));
        }
        for i in 0..3 {
            cache.set(&format!("long-{i}"), b"v", LONG);
        }
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(cache.store.len(), 8, "nothing has read or swept yet");
        assert_eq!(cache.entry_count(), 3);
        assert_eq!(cache.store.len(), 3);
    }

    #[test]
    fn swr_goes_fresh_then_stale_then_miss() {
        let cache = L1Cache::new(16);
        // Ratio 0.5 with ±10% jitter: stale from 180-220 ms, a miss from 400 ms.
        cache.set("k", b"v", Duration::from_millis(400));
        assert_eq!(
            cache.get_with_swr("k", 0.5),
            L1SwrRead::Fresh(b"v".to_vec())
        );
        std::thread::sleep(Duration::from_millis(300));
        assert_eq!(
            cache.get_with_swr("k", 0.5),
            L1SwrRead::Stale(b"v".to_vec())
        );
        std::thread::sleep(Duration::from_millis(150));
        assert_eq!(cache.get_with_swr("k", 0.5), L1SwrRead::Miss);
    }

    /// `exists` must not protect an entry from eviction the way a read does.
    /// At capacity 2 the second insert goes to the probationary queue, and
    /// the next insert evicts it unless it was read in between.
    #[test]
    fn exists_does_not_count_as_a_read() {
        let peeked = L1Cache::new(2);
        peeked.set("a", b"1", LONG);
        peeked.set("b", b"2", LONG);
        assert!(peeked.contains("b"));
        peeked.set("c", b"3", LONG);
        assert!(peeked.store.peek("b").is_none(), "exists kept b alive");

        let read = L1Cache::new(2);
        read.set("a", b"1", LONG);
        read.set("b", b"2", LONG);
        assert!(read.get("b").is_some());
        read.set("c", b"3", LONG);
        assert!(
            read.store.peek("b").is_some(),
            "a read did not keep b alive"
        );
    }

    /// One pass over keys that are never read again must not evict entries
    /// that were read, which an LRU would do.
    #[test]
    fn a_scan_does_not_flush_entries_that_were_read() {
        let cache = L1Cache::new(100);
        for i in 0..50 {
            cache.set(&format!("hot-{i}"), b"v", LONG);
        }
        for _ in 0..2 {
            for i in 0..50 {
                assert!(cache.get(&format!("hot-{i}")).is_some());
            }
        }
        for i in 0..1000 {
            cache.set(&format!("scan-{i}"), b"v", LONG);
        }
        let kept = (0..50)
            .filter(|i| cache.store.peek(&format!("hot-{i}")).is_some())
            .count();
        assert_eq!(kept, 50);
    }

    /// Many threads mixing every operation on shared keys, with TTLs short
    /// enough to expire mid-run and more keys than capacity: no read returns
    /// another key's value, occupancy never passes capacity, and once every
    /// TTL has passed the count is exact at zero.
    #[test]
    fn concurrent_mixed_operations_keep_the_invariants() {
        const THREADS: u64 = 8;
        const OPS: u64 = 20_000;
        const KEYS: u64 = 64;
        const CAPACITY: usize = 32;

        fn next(state: &mut u64) -> u64 {
            *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = *state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }

        let cache = L1Cache::new(CAPACITY);
        let workers: Vec<_> = (0..THREADS)
            .map(|t| {
                let cache = cache.clone();
                std::thread::spawn(move || {
                    let mut rng = t;
                    for i in 0..OPS {
                        let key = format!("k{}", next(&mut rng) % KEYS);
                        let owned_by_key = |value: &[u8]| {
                            assert!(
                                value.starts_with(format!("{key}:").as_bytes()),
                                "{key} returned {}",
                                String::from_utf8_lossy(value)
                            );
                        };
                        match next(&mut rng) % 6 {
                            0 | 1 => {
                                let ttl = Duration::from_millis(1 + next(&mut rng) % 20);
                                cache.set(&key, format!("{key}:{t}:{i}").as_bytes(), ttl);
                            }
                            2 => {
                                if let Some(value) = cache.get(&key) {
                                    owned_by_key(&value);
                                }
                            }
                            3 => match cache.get_with_swr(&key, 0.5) {
                                L1SwrRead::Fresh(value) | L1SwrRead::Stale(value) => {
                                    owned_by_key(&value);
                                }
                                L1SwrRead::Miss => {}
                            },
                            4 => {
                                cache.contains(&key);
                            }
                            _ => cache.delete(&key),
                        }
                        assert!(cache.store.len() <= CAPACITY);
                    }
                })
            })
            .collect();
        for worker in workers {
            worker.join().expect("a worker thread panicked");
        }
        assert!(cache.entry_count() <= CAPACITY as u64);
        std::thread::sleep(Duration::from_millis(30));
        assert_eq!(cache.entry_count(), 0);
    }
}
