use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use fred::prelude::*;
use fred::types::ConnectHandle;

use crate::backend::{Backend, HealthStatus, TtlInspectable};
use crate::error::{BackendError, BackendErrorKind};

// ── Error mapping ─────────────────────────────────────────────────────────────

fn redis_err(e: RedisError) -> BackendError {
    let kind = match e.kind() {
        RedisErrorKind::Auth => BackendErrorKind::Authentication,
        RedisErrorKind::IO => BackendErrorKind::Transient,
        RedisErrorKind::Timeout => BackendErrorKind::Timeout,
        RedisErrorKind::Canceled => BackendErrorKind::Transient,
        _ => BackendErrorKind::Permanent,
    };
    BackendError { kind, message: e.to_string(), source: Some(Box::new(e)) }
}

// ── RedisBackend ──────────────────────────────────────────────────────────────

/// Redis backend powered by the `fred` client.
///
/// Call [`connect`](RedisBackend::connect) before issuing any cache operations.
pub struct RedisBackend {
    client: RedisClient,
}

impl std::fmt::Debug for RedisBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisBackend").finish_non_exhaustive()
    }
}

impl RedisBackend {
    /// Start building a [`RedisBackend`].
    pub fn builder() -> RedisBackendBuilder {
        RedisBackendBuilder::default()
    }

    /// Connect to Redis and wait until the connection is established.
    ///
    /// This must be called before any cache operations. The returned
    /// `ConnectHandle` drives the connection task — hold onto it for the
    /// lifetime of the backend (or join it on shutdown).
    pub async fn connect(&self) -> Result<ConnectHandle, BackendError> {
        self.client.init().await.map_err(redis_err)
    }
}

// ── Backend impl ──────────────────────────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl Backend for RedisBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        let result: Option<bytes::Bytes> = self.client.get(key).await.map_err(redis_err)?;
        Ok(result.map(|b| b.to_vec()))
    }

    async fn set(&self, key: &str, value: Vec<u8>, ttl: Option<Duration>) -> Result<(), BackendError> {
        let expiration = ttl.map(|d| Expiration::EX(d.as_secs().max(1) as i64));
        self.client
            .set::<(), _, _>(key, value.as_slice(), expiration, None, false)
            .await
            .map_err(redis_err)
    }

    async fn delete(&self, key: &str) -> Result<bool, BackendError> {
        let removed: i64 = self.client.del(key).await.map_err(redis_err)?;
        Ok(removed > 0)
    }

    async fn exists(&self, key: &str) -> Result<bool, BackendError> {
        let count: i64 = self.client.exists(key).await.map_err(redis_err)?;
        Ok(count > 0)
    }

    async fn health(&self) -> Result<HealthStatus, BackendError> {
        let start = std::time::Instant::now();
        let _pong: String = self.client.ping().await.map_err(redis_err)?;
        let latency = start.elapsed();

        let mut details = HashMap::new();
        details.insert("latency_ms".to_string(), latency.as_millis().to_string());
        Ok(HealthStatus {
            is_healthy: true,
            latency_ms: latency.as_secs_f64() * 1000.0,
            backend_type: "redis".to_string(),
            details,
        })
    }
}

// ── TtlInspectable impl ───────────────────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl TtlInspectable for RedisBackend {
    async fn ttl(&self, key: &str) -> Result<Option<Duration>, BackendError> {
        // Redis TTL return values:
        //   -2 → key does not exist
        //   -1 → key exists but has no TTL
        //   N  → remaining seconds
        let secs: i64 = self.client.ttl(key).await.map_err(redis_err)?;
        match secs {
            -2 | -1 => Ok(None),
            n => Ok(Some(Duration::from_secs(n as u64))),
        }
    }
}

// ── Builder ───────────────────────────────────────────────────────────────────

/// Builder for [`RedisBackend`].
#[derive(Default)]
pub struct RedisBackendBuilder {
    url: Option<String>,
}

impl RedisBackendBuilder {
    /// Set the Redis connection URL (required).
    ///
    /// Accepts standard Redis URL formats, e.g. `redis://localhost:6379` or
    /// `redis://:password@host:6379/0`.
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    /// Consume the builder and construct a [`RedisBackend`].
    ///
    /// # Errors
    ///
    /// Returns an error if `url` was not provided or is not a valid Redis URL.
    /// The connection itself is lazy — call [`RedisBackend::connect`] to establish it.
    pub fn build(self) -> Result<RedisBackend, crate::error::CachekitError> {
        use crate::error::CachekitError;

        let url = self.url.filter(|u| !u.is_empty()).ok_or_else(|| CachekitError::Config("url is required".to_string()))?;

        let config = RedisConfig::from_url(&url).map_err(|e| CachekitError::Config(format!("invalid Redis URL: {e}")))?;

        let client = RedisClient::new(config, None, None, None);
        Ok(RedisBackend { client })
    }
}
