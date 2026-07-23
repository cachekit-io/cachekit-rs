//! Memcached backend via Shopify's [`async-memcached`](https://crates.io/crates/async-memcached) client.
//!
//! Implements the base [`Backend`] contract only — deliberately no
//! [`TtlInspectable`](crate::backend::TtlInspectable) and no
//! [`LockableBackend`](crate::backend::LockableBackend). This mirrors
//! cachekit-py's Memcached backend, which is base-only by design: the classic
//! memcached text protocol has no command to *read* a key's remaining TTL, so
//! Python cannot implement `get_ttl` and ships without the capability. Rust
//! *could* read TTLs via the meta protocol (`mg <key> t`, memcached >= 1.6),
//! but shipping a capability here that Python cannot match would make
//! `refresh_ttl_on_get`-style behaviour diverge between SDKs on the same
//! backend. Revisit only when cachekit-py gains meta-protocol support.

use std::collections::HashMap;
use std::time::Duration;

use async_memcached::{AsciiProtocol, Client, ErrorKind as McErrorKind, Status as McStatus};
use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::backend::{Backend, HealthStatus};
use crate::error::{BackendError, BackendErrorKind};

/// Memcached rejects TTLs above 30 days *as durations* — anything larger is
/// silently reinterpreted as an absolute unix timestamp, expiring the entry
/// in the past. Clamp instead, matching cachekit-py's `MAX_MEMCACHED_TTL`.
const MAX_MEMCACHED_TTL_SECS: u64 = 30 * 24 * 60 * 60;

/// Memcached's default server-side item-size limit (`-I` flag).
const DEFAULT_MAX_ITEM_SIZE_BYTES: usize = 1024 * 1024;

// ── Error mapping ─────────────────────────────────────────────────────────────

fn memcached_err(e: async_memcached::Error) -> BackendError {
    use async_memcached::Error as McError;

    let kind = match &e {
        McError::Connect(io) | McError::Io(io) => match io.kind() {
            std::io::ErrorKind::TimedOut => BackendErrorKind::Timeout,
            _ => BackendErrorKind::Transient,
        },
        // SERVER_ERROR is retryable (matches py's MemcacheServerError →
        // TRANSIENT); CLIENT_ERROR / bad input / protocol violations are not.
        McError::Protocol(McStatus::Error(McErrorKind::Server(_))) => BackendErrorKind::Transient,
        McError::Protocol(_) | McError::ParseError(_) => BackendErrorKind::Permanent,
    };

    BackendError {
        kind,
        message: e.to_string(),
        source: Some(Box::new(e)),
    }
}

// ── MemcachedBackend ──────────────────────────────────────────────────────────

/// Memcached backend powered by [`async-memcached`](https://crates.io/crates/async-memcached).
///
/// Build with [`builder`](MemcachedBackend::builder); the terminal
/// [`connect`](MemcachedBackendBuilder::connect) is async because the
/// underlying client connects eagerly (unlike [`RedisBackend`]'s lazy
/// build-then-connect split).
///
/// [`RedisBackend`]: crate::backend::redis::RedisBackend
pub struct MemcachedBackend {
    // ponytail: one connection serialized behind an async Mutex — swap for a
    // connection pool (e.g. deadpool) if per-backend throughput matters.
    client: Mutex<Client>,
    max_item_size_bytes: usize,
}

impl std::fmt::Debug for MemcachedBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemcachedBackend").finish_non_exhaustive()
    }
}

impl MemcachedBackend {
    /// Start building a [`MemcachedBackend`].
    pub fn builder() -> MemcachedBackendBuilder {
        MemcachedBackendBuilder::default()
    }
}

// ── Backend impl ──────────────────────────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
#[cfg_attr(not(feature = "unsync"), async_trait)]
#[cfg_attr(feature = "unsync", async_trait(?Send))]
impl Backend for MemcachedBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        let mut client = self.client.lock().await;
        let value = client.get(key).await.map_err(memcached_err)?;
        Ok(value.and_then(|v| v.data))
    }

    async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        // Fail loudly on oversized items instead of letting the server reject
        // them: callers should compress, shard, or switch backends (mirrors
        // cachekit-py's client-side guard).
        if self.max_item_size_bytes > 0 && value.len() > self.max_item_size_bytes {
            return Err(BackendError::permanent(format!(
                "value is {} bytes, exceeding the memcached max item size of {} bytes; \
                 enable compression, use a larger-payload backend (Redis/SaaS/File), or \
                 raise both the server's -I limit and the builder's max_item_size_bytes",
                value.len(),
                self.max_item_size_bytes,
            )));
        }

        // 0 = never expires on the wire; sub-second TTLs round up to 1s and
        // anything above 30 days clamps (see MAX_MEMCACHED_TTL_SECS).
        let expire = ttl.map(|d| {
            let secs = d.as_secs().clamp(1, MAX_MEMCACHED_TTL_SECS);
            i64::try_from(secs).unwrap_or(i64::MAX)
        });

        let mut client = self.client.lock().await;
        client
            .set(key, value.as_slice(), expire, None)
            .await
            .map_err(memcached_err)
    }

    async fn delete(&self, key: &str) -> Result<bool, BackendError> {
        let mut client = self.client.lock().await;
        match client.delete(key).await {
            Ok(()) => Ok(true),
            Err(async_memcached::Error::Protocol(McStatus::NotFound)) => Ok(false),
            Err(e) => Err(memcached_err(e)),
        }
    }

    async fn exists(&self, key: &str) -> Result<bool, BackendError> {
        // Memcached has no EXISTS command; a get is the probe (same trade-off
        // as cachekit-py — the value crosses the wire and is discarded).
        let mut client = self.client.lock().await;
        Ok(client.get(key).await.map_err(memcached_err)?.is_some())
    }

    async fn health(&self) -> Result<HealthStatus, BackendError> {
        let start = std::time::Instant::now();
        let version = {
            let mut client = self.client.lock().await;
            client.version().await.map_err(memcached_err)?
        };
        let latency = start.elapsed();

        let mut details = HashMap::new();
        details.insert("version".to_string(), version);
        Ok(HealthStatus {
            is_healthy: true,
            latency_ms: latency.as_secs_f64() * 1000.0,
            backend_type: "memcached".to_string(),
            details,
        })
    }
}

// ── Builder ───────────────────────────────────────────────────────────────────

/// Builder for [`MemcachedBackend`].
#[derive(Default)]
#[must_use]
pub struct MemcachedBackendBuilder {
    url: Option<String>,
    max_item_size_bytes: Option<usize>,
}

impl MemcachedBackendBuilder {
    /// Set the memcached server DSN (required).
    ///
    /// Accepts `tcp://host:port`, bare `host:port`, or `unix://path`.
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    /// Reject values larger than this before sending to memcached
    /// (default 1 MiB — the server's own `-I` default; `0` disables the
    /// check). Raise this only if the server's `-I` limit is raised too.
    pub fn max_item_size_bytes(mut self, bytes: usize) -> Self {
        self.max_item_size_bytes = Some(bytes);
        self
    }

    /// Connect to memcached and construct a [`MemcachedBackend`].
    ///
    /// # Errors
    ///
    /// Returns a config error if `url` was not provided, or a backend error
    /// if the connection cannot be established.
    pub async fn connect(self) -> Result<MemcachedBackend, crate::error::CachekitError> {
        use crate::error::CachekitError;

        let url = self
            .url
            .filter(|u| !u.is_empty())
            .ok_or_else(|| CachekitError::Config("url is required".to_string()))?;

        let client = Client::new(&url).await.map_err(memcached_err)?;
        Ok(MemcachedBackend {
            client: Mutex::new(client),
            max_item_size_bytes: self
                .max_item_size_bytes
                .unwrap_or(DEFAULT_MAX_ITEM_SIZE_BYTES),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_mapping_classifies_by_retryability() {
        let io = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        assert_eq!(
            memcached_err(async_memcached::Error::Connect(io)).kind,
            BackendErrorKind::Transient
        );

        let timeout = std::io::Error::new(std::io::ErrorKind::TimedOut, "slow");
        assert_eq!(
            memcached_err(async_memcached::Error::Io(timeout)).kind,
            BackendErrorKind::Timeout
        );

        assert_eq!(
            memcached_err(async_memcached::Error::Protocol(McStatus::Error(
                McErrorKind::Server("busy".into())
            )))
            .kind,
            BackendErrorKind::Transient
        );

        assert_eq!(
            memcached_err(async_memcached::Error::Protocol(McStatus::Error(
                McErrorKind::Client("bad".into())
            )))
            .kind,
            BackendErrorKind::Permanent
        );

        assert_eq!(
            memcached_err(async_memcached::Error::Protocol(McStatus::Error(
                McErrorKind::KeyTooLong
            )))
            .kind,
            BackendErrorKind::Permanent
        );
    }
}
