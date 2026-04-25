use moka::sync::Cache;
use moka::Expiry;
use std::time::Duration;

#[derive(Clone)]
struct L1Entry {
    data: Vec<u8>,
    ttl: Duration,
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

/// In-process LRU cache with per-entry TTL, backed by [`moka`].
///
/// Used as the L1 layer in the dual-layer cache architecture.
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

    /// Insert or overwrite an entry with the given TTL.
    pub fn set(&self, key: &str, value: &[u8], ttl: Duration) {
        self.store.insert(
            key.to_string(),
            L1Entry {
                data: value.to_vec(),
                ttl,
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
