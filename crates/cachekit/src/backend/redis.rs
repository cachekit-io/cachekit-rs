use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use fred::interfaces::LuaInterface;
use fred::prelude::*;
use fred::types::ConnectHandle;

use crate::backend::{nonzero_timeout, Backend, HealthStatus, LockableBackend, TtlInspectable};
use crate::error::{BackendError, BackendErrorKind};

// ── Error mapping ─────────────────────────────────────────────────────────────

/// Sanitize Redis error messages to avoid leaking connection URL passwords.
fn sanitize_redis_message(msg: &str) -> String {
    // Redis URLs may contain passwords: redis://:password@host:6379
    // Strip anything between :// and @ to remove embedded credentials.
    if let Some(proto_end) = msg.find("://") {
        if let Some(at_pos) = msg[proto_end..].find('@') {
            let mut sanitized = String::with_capacity(msg.len());
            sanitized.push_str(&msg[..proto_end + 3]);
            sanitized.push_str("[REDACTED]");
            sanitized.push_str(&msg[proto_end + at_pos..]);
            return sanitized;
        }
    }
    msg.to_string()
}

fn redis_err(e: RedisError) -> BackendError {
    let kind = match e.kind() {
        RedisErrorKind::Auth => BackendErrorKind::Authentication,
        RedisErrorKind::IO => BackendErrorKind::Transient,
        RedisErrorKind::Timeout => BackendErrorKind::Timeout,
        RedisErrorKind::Canceled => BackendErrorKind::Transient,
        _ => BackendErrorKind::Permanent,
    };
    BackendError {
        kind,
        message: sanitize_redis_message(&e.to_string()),
        source: Some(Box::new(e)),
    }
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

    /// The fred client, so tests can read the options `build` gave it.
    #[cfg(test)]
    pub(crate) fn client(&self) -> &RedisClient {
        &self.client
    }
}

// ── Backend impl ──────────────────────────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
#[cfg_attr(not(feature = "unsync"), async_trait)]
#[cfg_attr(feature = "unsync", async_trait(?Send))]
impl Backend for RedisBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        let result: Option<bytes::Bytes> = self.client.get(key).await.map_err(redis_err)?;
        Ok(result.map(|b| b.to_vec()))
    }

    async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        let expiration = ttl.map(|d| {
            let secs = i64::try_from(d.as_secs().max(1)).unwrap_or(i64::MAX);
            Expiration::EX(secs)
        });
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

    fn as_lockable(&self) -> Option<&dyn LockableBackend> {
        Some(self)
    }
}

// ── TtlInspectable impl ───────────────────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
#[cfg_attr(not(feature = "unsync"), async_trait)]
#[cfg_attr(feature = "unsync", async_trait(?Send))]
impl TtlInspectable for RedisBackend {
    async fn ttl(&self, key: &str) -> Result<Option<Duration>, BackendError> {
        // Redis TTL return values:
        //   -2 → key does not exist
        //   -1 → key exists but has no TTL
        //   N  → remaining seconds
        let secs: i64 = self.client.ttl(key).await.map_err(redis_err)?;
        match secs {
            ..0 => Ok(None),
            n => Ok(Some(Duration::from_secs(n.unsigned_abs()))),
        }
    }
}

// ── LockableBackend impl ──────────────────────────────────────────────────────

/// Callers pass the bare cache key; the backend derives the lock key on the
/// wire. `<key>:lock` matches cachekit-py's Redis lock namespace so py and rs
/// workloads contend on the same lock.
fn lock_key(key: &str) -> String {
    format!("{key}:lock")
}

/// Compare-and-delete: only the holder of the lock_id token may release.
/// Atomic via Lua — a GET-then-DEL from the client would race with expiry
/// and delete a lock some other client has since acquired.
const RELEASE_LOCK_SCRIPT: &str = r#"if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) else return 0 end"#;

#[cfg(not(target_arch = "wasm32"))]
#[cfg_attr(not(feature = "unsync"), async_trait)]
#[cfg_attr(feature = "unsync", async_trait(?Send))]
impl LockableBackend for RedisBackend {
    async fn acquire_lock(
        &self,
        key: &str,
        timeout_ms: u64,
    ) -> Result<Option<String>, BackendError> {
        let lock_id = uuid::Uuid::new_v4().to_string();
        // PX must be >= 1 (Redis rejects 0); clamp like Backend::set does for EX.
        let px = i64::try_from(timeout_ms.max(1)).unwrap_or(i64::MAX);

        // SET NX PX: returns OK when the lock was free, nil when contested.
        let result: Option<String> = self
            .client
            .set(
                lock_key(key),
                lock_id.as_str(),
                Some(Expiration::PX(px)),
                Some(SetOptions::NX),
                false,
            )
            .await
            .map_err(redis_err)?;

        Ok(result.map(|_| lock_id))
    }

    async fn release_lock(&self, key: &str, lock_id: &str) -> Result<bool, BackendError> {
        let deleted: i64 = self
            .client
            .eval(RELEASE_LOCK_SCRIPT, vec![lock_key(key)], vec![lock_id])
            .await
            .map_err(redis_err)?;
        Ok(deleted == 1)
    }
}

// ── Builder ───────────────────────────────────────────────────────────────────

/// Default per-command deadline ([`RedisBackendBuilder::command_timeout`]).
/// fred's default is 0 (no timeout), so a stalled or black-holed server would
/// hold every op until TCP gives up and the circuit breaker could never open.
/// 5 s matches cachekit-py and cachekit-ts.
pub(crate) const COMMAND_TIMEOUT: Duration = Duration::from_secs(5);

/// fred client options for [`RedisBackendBuilder::build`].
///
/// The unresponsive-connection check is enabled only with a reconnect policy:
/// fred closes a connection it deems unresponsive, and without a policy it is
/// never re-established, so one stall past the threshold would leave the
/// client failing permanently. Its threshold is the command timeout, so a
/// connection that cannot answer within the budget is replaced.
pub(crate) fn fred_options(
    reconnects: bool,
    command_timeout: Duration,
) -> (PerformanceConfig, ConnectionConfig) {
    let perf = PerformanceConfig {
        default_command_timeout: command_timeout,
        ..Default::default()
    };
    let mut connection = ConnectionConfig::default();
    if reconnects {
        connection.unresponsive.max_timeout = Some(command_timeout);
    }
    (perf, connection)
}

/// Builder for [`RedisBackend`].
#[derive(Default)]
#[must_use]
pub struct RedisBackendBuilder {
    url: Option<String>,
    reconnect: Option<ReconnectPolicy>,
    command_timeout: Option<Duration>,
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

    /// Automatically re-establish dropped connections with exponential
    /// backoff (100 ms → 30 s, retrying indefinitely). Off by default.
    ///
    /// Also turns on fred's unresponsive-connection check: a connection with
    /// a reply pending for longer than the
    /// [command timeout](Self::command_timeout) is closed and re-established
    /// (fred checks every 2 s). Without a reconnect policy that check stays
    /// off, because a closed connection would never come back.
    ///
    /// Initial connections still fail fast — fred's `fail_fast` default stays
    /// on, so the policy only governs reconnecting a connection that was
    /// previously established. This is the policy `CacheKit::production` and
    /// `CacheKit::secure` use. Call it when you build a backend yourself, for
    /// example one backend shared by several clients through
    /// [`CacheKitBuilder::backend`](crate::CacheKitBuilder::backend), so it
    /// recovers after a Redis restart instead of failing every operation.
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), cachekit::CachekitError> {
    /// use cachekit::backend::redis::RedisBackend;
    ///
    /// let backend = RedisBackend::builder()
    ///     .url("redis://localhost:6379")
    ///     .auto_reconnect()
    ///     .build()?;
    /// let _connection = backend.connect().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn auto_reconnect(mut self) -> Self {
        self.reconnect = Some(ReconnectPolicy::new_exponential(0, 100, 30_000, 2));
        self
    }

    /// Deadline for each Redis command (default 5 s). A command past it fails
    /// with `BackendErrorKind::Timeout`, and may still run on the server.
    ///
    /// The budget is per attempt. When the reliability stack wraps the
    /// backend (`CacheKit::production`, `CacheKit::secure`, or
    /// `CacheKitBuilder::reliability`),
    /// each retry gets a fresh budget, so with the default 3 attempts one
    /// operation can take about three budgets plus backoff before it errors.
    /// The stack sets no overall deadline for Redis. Each operation that ends
    /// in a timeout counts as one failure toward opening the circuit breaker,
    /// which a `tokio::time::timeout` around the call does not: a cancelled
    /// call records no outcome. With [`auto_reconnect`](Self::auto_reconnect)
    /// the budget is also the unresponsive-connection threshold.
    ///
    /// `build` rejects `Duration::ZERO` with `CachekitError::Config`; fred
    /// reads zero as "no timeout".
    pub fn command_timeout(mut self, timeout: Duration) -> Self {
        self.command_timeout = Some(timeout);
        self
    }

    /// Consume the builder and construct a [`RedisBackend`].
    ///
    /// # Errors
    ///
    /// Returns an error if `url` was not provided or is not a valid Redis URL,
    /// or if the command timeout is zero. The connection itself is lazy — call [`RedisBackend::connect`] to establish it.
    pub fn build(self) -> Result<RedisBackend, crate::error::CachekitError> {
        use crate::error::CachekitError;

        let url = self
            .url
            .filter(|u| !u.is_empty())
            .ok_or_else(|| CachekitError::Config("url is required".to_string()))?;

        let config = RedisConfig::from_url(&url).map_err(|e| {
            CachekitError::Config(format!(
                "invalid Redis URL: {}",
                sanitize_redis_message(&e.to_string())
            ))
        })?;

        let command_timeout =
            nonzero_timeout("command_timeout", self.command_timeout, COMMAND_TIMEOUT)?;

        let (perf, connection) = fred_options(self.reconnect.is_some(), command_timeout);
        let client = RedisClient::new(config, Some(perf), Some(connection), self.reconnect);
        Ok(RedisBackend { client })
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)] // test-only: a builder failure should panic loudly
mod tests {
    use super::*;

    /// Compile-time proof that RedisBackend implements LockableBackend.
    fn _assert_lockable(_b: &dyn LockableBackend) {}

    #[test]
    fn redis_is_lockable() {
        fn _check(backend: &RedisBackend) {
            _assert_lockable(backend);
        }
    }

    #[test]
    fn lock_key_derives_py_compatible_namespace() {
        // Bare-key contract: callers never build ":lock" themselves; the
        // backend appends it, byte-identical to cachekit-py's Redis lock.
        assert_eq!(
            lock_key("ns:app:func:m.f:args:abc:v1"),
            "ns:app:func:m.f:args:abc:v1:lock"
        );
    }

    #[test]
    fn command_timeout_is_five_seconds_on_every_build() {
        assert_eq!(COMMAND_TIMEOUT, Duration::from_secs(5));
        for reconnects in [false, true] {
            let (perf, _) = fred_options(reconnects, COMMAND_TIMEOUT);
            assert_eq!(perf.default_command_timeout, COMMAND_TIMEOUT);
        }
    }

    #[test]
    fn unresponsive_recycling_only_with_reconnect_policy() {
        let (_, connection) = fred_options(false, COMMAND_TIMEOUT);
        assert_eq!(connection.unresponsive.max_timeout, None);
        let (_, connection) = fred_options(true, COMMAND_TIMEOUT);
        assert_eq!(connection.unresponsive.max_timeout, Some(COMMAND_TIMEOUT));
    }

    const URL: &str = "redis://localhost:6379";

    #[test]
    fn auto_reconnect_is_off_by_default() {
        let backend = RedisBackend::builder().url(URL).build().expect("builds");
        assert_eq!(backend.client().client_reconnect_policy(), None);
        assert_eq!(
            backend
                .client()
                .connection_config()
                .unresponsive
                .max_timeout,
            None
        );
    }

    #[test]
    fn auto_reconnect_sets_the_exponential_policy_and_unresponsive_check() {
        let backend = RedisBackend::builder()
            .url(URL)
            .auto_reconnect()
            .build()
            .expect("builds");
        assert_eq!(
            backend.client().client_reconnect_policy(),
            Some(ReconnectPolicy::new_exponential(0, 100, 30_000, 2))
        );
        assert_eq!(
            backend
                .client()
                .connection_config()
                .unresponsive
                .max_timeout,
            Some(COMMAND_TIMEOUT)
        );
    }

    #[test]
    fn configured_command_timeout_reaches_fred() {
        let budget = Duration::from_millis(250);
        let backend = RedisBackend::builder()
            .url(URL)
            .command_timeout(budget)
            .build()
            .expect("builds");
        assert_eq!(
            backend.client().perf_config().default_command_timeout,
            budget
        );
        assert_eq!(
            backend
                .client()
                .connection_config()
                .unresponsive
                .max_timeout,
            None
        );

        let backend = RedisBackend::builder()
            .url(URL)
            .command_timeout(budget)
            .auto_reconnect()
            .build()
            .expect("builds");
        assert_eq!(
            backend.client().perf_config().default_command_timeout,
            budget
        );
        assert_eq!(
            backend
                .client()
                .connection_config()
                .unresponsive
                .max_timeout,
            Some(budget)
        );
    }

    #[test]
    fn default_command_timeout_reaches_fred() {
        let backend = RedisBackend::builder().url(URL).build().expect("builds");
        assert_eq!(
            backend.client().perf_config().default_command_timeout,
            COMMAND_TIMEOUT
        );
    }

    #[test]
    fn zero_command_timeout_is_a_config_error() {
        let err = RedisBackend::builder()
            .url(URL)
            .command_timeout(Duration::ZERO)
            .build()
            .expect_err("zero is rejected");
        assert!(
            matches!(&err, crate::error::CachekitError::Config(m) if m.contains("command_timeout")),
            "{err:?}"
        );
    }
}
