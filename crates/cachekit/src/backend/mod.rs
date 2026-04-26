use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;

use crate::error::BackendError;

// ── HealthStatus ─────────────────────────────────────────────────────────────

/// Describes the health of a backend at a point in time.
#[derive(Debug, Clone)]
pub struct HealthStatus {
    /// Whether the backend is considered healthy.
    pub is_healthy: bool,
    /// Round-trip latency of the health check in milliseconds.
    pub latency_ms: f64,
    /// Human-readable name for this backend implementation.
    pub backend_type: String,
    /// Optional key-value details (pool size, version, etc.).
    pub details: HashMap<String, String>,
}

// ── Backend trait ─────────────────────────────────────────────────────────────

/// Async cache backend abstraction.
///
/// Implementors must be `Send + Sync` on native targets.
/// On `wasm32` targets `Send` is relaxed (`?Send`) because the Workers runtime
/// is single-threaded and `reqwest`/`worker::Fetch` futures are `!Send`.
#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
pub trait Backend: Send + Sync {
    /// Retrieve the raw bytes stored under `key`, or `None` if absent.
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError>;

    /// Store `value` under `key`, optionally expiring after `ttl`.
    async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), BackendError>;

    /// Remove `key` and return `true` if it existed.
    async fn delete(&self, key: &str) -> Result<bool, BackendError>;

    /// Return `true` if `key` exists without fetching the value.
    async fn exists(&self, key: &str) -> Result<bool, BackendError>;

    /// Return health/status information for this backend.
    async fn health(&self) -> Result<HealthStatus, BackendError>;
}

#[cfg(target_arch = "wasm32")]
#[async_trait(?Send)]
pub trait Backend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError>;
    async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), BackendError>;
    async fn delete(&self, key: &str) -> Result<bool, BackendError>;
    async fn exists(&self, key: &str) -> Result<bool, BackendError>;
    async fn health(&self) -> Result<HealthStatus, BackendError>;
}

// ── TtlInspectable ───────────────────────────────────────────────────────────

/// Optional extension for backends that can report the remaining TTL of a key.
#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
pub trait TtlInspectable: Backend {
    /// Return the remaining TTL for `key`, or `None` if the key does not exist
    /// or has no expiry.
    async fn ttl(&self, key: &str) -> Result<Option<Duration>, BackendError>;

    /// Refresh the TTL on an existing key. Default: not supported.
    async fn refresh_ttl(&self, _key: &str, _ttl: Duration) -> Result<bool, BackendError> {
        Err(BackendError::permanent(
            "refresh_ttl not supported by this backend",
        ))
    }
}

#[cfg(target_arch = "wasm32")]
#[async_trait(?Send)]
pub trait TtlInspectable: Backend {
    async fn ttl(&self, key: &str) -> Result<Option<Duration>, BackendError>;

    async fn refresh_ttl(&self, _key: &str, _ttl: Duration) -> Result<bool, BackendError> {
        Err(BackendError::permanent(
            "refresh_ttl not supported by this backend",
        ))
    }
}

// ── LockableBackend ─────────────────────────────────────────────────────────

/// Optional extension for backends that support distributed locking.
#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
pub trait LockableBackend: Backend {
    /// Acquire a distributed lock. Returns lock_id if acquired, None if contested.
    async fn acquire_lock(
        &self,
        key: &str,
        timeout_ms: u64,
    ) -> Result<Option<String>, BackendError>;
    /// Release a distributed lock. Returns true if released.
    async fn release_lock(&self, key: &str, lock_id: &str) -> Result<bool, BackendError>;
}

#[cfg(target_arch = "wasm32")]
#[async_trait(?Send)]
pub trait LockableBackend: Backend {
    async fn acquire_lock(
        &self,
        key: &str,
        timeout_ms: u64,
    ) -> Result<Option<String>, BackendError>;
    async fn release_lock(&self, key: &str, lock_id: &str) -> Result<bool, BackendError>;
}

// ── Feature-gated backend modules ─────────────────────────────────────────────

/// HTTP backend for the cachekit.io SaaS API.
#[cfg(feature = "cachekitio")]
pub mod cachekitio;
#[cfg(feature = "cachekitio")]
mod cachekitio_lock;
#[cfg(feature = "cachekitio")]
mod cachekitio_ttl;

/// Redis backend via the [`fred`](https://crates.io/crates/fred) client.
#[cfg(feature = "redis")]
pub mod redis;

/// Cloudflare Workers backend using `worker::Fetch`.
#[cfg(feature = "workers")]
pub mod workers;
