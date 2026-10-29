use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::Poll;
use std::time::Duration;

use async_trait::async_trait;
use fred::clients::WithOptions;
use fred::interfaces::{EventInterface, LuaInterface};
use fred::prelude::*;
use fred::types::{ConnectHandle, Options};
use tokio::sync::broadcast::{self, error::RecvError};
use tokio::task::{AbortHandle, JoinHandle};

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
    /// Commands go through these options, which carry fred's `fail_fast`
    /// when the offline queue is disabled. EVAL does not: fred implements
    /// `LuaInterface` only on the bare client.
    client: WithOptions<RedisClient>,
    /// `Some` when built with
    /// [`disable_offline_queue`](RedisBackendBuilder::disable_offline_queue).
    offline: Option<OfflineGuard>,
}

/// What [`RedisBackend`] keeps to refuse commands while the connection is down.
#[derive(Default)]
struct OfflineGuard {
    /// Set by fred's error events, cleared by its reconnect events.
    down: Arc<AtomicBool>,
    /// Set by the first call to `connect`.
    connect_called: AtomicBool,
    /// The task following fred's connection events. fred's reconnect task
    /// keeps the event channels open after the backend is dropped, so the
    /// task is aborted with the backend instead of waiting for them to close.
    watcher: OnceLock<AbortHandle>,
}

impl Drop for OfflineGuard {
    fn drop(&mut self) {
        if let Some(watcher) = self.watcher.get() {
            watcher.abort();
        }
    }
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
    /// This must be called before any cache operations.
    ///
    /// The returned `ConnectHandle` is fred's handle to the spawned task that
    /// drives the connection (a tokio `JoinHandle`). Dropping it detaches the
    /// task without stopping it: the connection, and any auto-reconnect,
    /// keeps running for as long as the backend is alive. The intent presets
    /// drop it for that reason. Keep it only to observe how the connection
    /// ended: the task resolves when the connection closes for good, with the
    /// error that closed it, if any. Without a reconnect policy that is the
    /// first command to reach the dropped connection; with
    /// [`disable_offline_queue`](RedisBackendBuilder::disable_offline_queue)
    /// none may reach it, so the task may never resolve and failed commands
    /// are the signal. Do not `abort` it while the backend is still in use.
    ///
    /// With `disable_offline_queue`, call it once per backend: a second call
    /// returns `BackendErrorKind::Permanent`, even after a first call that
    /// failed or was cancelled. Another attempt would replace the connection
    /// fred is still making or holding, and fred can report that one's close
    /// after the new one is up. To try again, build a new backend.
    pub async fn connect(&self) -> Result<ConnectHandle, BackendError> {
        let Some(guard) = &self.offline else {
            return self.client.init().await.map_err(redis_err);
        };
        if guard.connect_called.swap(true, Ordering::AcqRel) {
            return Err(BackendError::permanent(
                "connect was already called on this backend; build a new one to connect again",
            ));
        }
        // Before `init`, so no event is missed, and so the watcher runs even if
        // this future is dropped while fred's connection task carries on.
        let watcher = watch_connection(&self.client, Arc::clone(&guard.down));
        // `connect_called` lets only the first call get here.
        let _ = guard.watcher.set(watcher.abort_handle());
        self.client.init().await.map_err(redis_err)
    }

    /// The client to send a command through, or the error a command returns
    /// at once while the connection is down and the offline queue disabled.
    fn redis(&self) -> Result<&WithOptions<RedisClient>, BackendError> {
        match &self.offline {
            Some(guard) if guard.down.load(Ordering::Acquire) => Err(BackendError::transient(
                "Redis connection is down; the offline queue is disabled",
            )),
            _ => Ok(&self.client),
        }
    }

    /// The fred client, so tests can read the options `build` gave it.
    #[cfg(test)]
    pub(crate) fn client(&self) -> &WithOptions<RedisClient> {
        &self.client
    }
}

/// Keep `down` in step with fred's connection events.
///
/// fred's `fail_fast` reads the client state, which lags the connection: after
/// a drop it stays `Connected` through the first reconnect backoff (100 to
/// 150 ms), and without a reconnect policy it never changes. On a single
/// server or sentinel connection, fred broadcasts an error when a connection
/// ends or a reconnect attempt fails, and a reconnect event each time one is
/// established. (Redis Cluster is excluded at `build`: there an error can come
/// from one node while the others serve.)
fn watch_connection(client: &RedisClient, down: Arc<AtomicBool>) -> JoinHandle<()> {
    let errors = client.error_rx();
    let reconnects = client.reconnect_rx();
    let probe = client.client_reconnect_policy().is_some().then(|| {
        let client = client.clone();
        move || {
            let client = client.clone();
            async move { answered(client.ping::<()>().await) }
        }
    });
    tokio::spawn(async move { follow_connection(errors, reconnects, &down, probe).await })
}

/// Whether the server replied, an error reply included: a user without
/// `+ping` still answers.
fn answered(ping: RedisResult<()>) -> bool {
    !matches!(
        ping,
        Err(e) if matches!(e.kind(), RedisErrorKind::IO | RedisErrorKind::Canceled | RedisErrorKind::Timeout)
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionEvent {
    Dropped,
    Reconnected,
    /// The watcher fell behind and the channel dropped events.
    Missed,
    /// The probe in flight finished: whether the server answered.
    Probed(bool),
    /// The last probe got no answer and no event has come since.
    Unsettled,
}

/// The pause between probes while the last one got no answer.
const REPROBE_DELAY: Duration = Duration::from_secs(1);

/// The watcher's state machine, apart from fred so it can be tested.
///
/// The two channels carry no common order, so a watcher that falls behind
/// cannot tell an old reconnect from a new one. A drop therefore always marks
/// the connection down, and nothing marks it up on an event's word alone.
/// With a reconnect policy, `probe` (a PING through the bare client, which
/// waits for a reconnect in progress instead of failing at once) decides after
/// each reconnect and after missed events. The probe runs alongside the event
/// channels, so a drop is handled at once even while a probe waits. A drop
/// replaces the probe in flight with a fresh one: the drop may be older than
/// the reconnect that started the probe, and an answer from before the drop
/// proves nothing. A probe started by a reconnect or missed events that gets
/// no answer is repeated [`REPROBE_DELAY`] after it ends, one at a time, until
/// one is answered or a drop arrives: a server that stalled past the command
/// timeout and then recovered sends no further event. Without a policy
/// (`probe` is `None`) the only reconnect event is the first connect and a drop
/// is final, so reconnect events are ignored and nothing is probed.
async fn follow_connection<P, F>(
    mut errors: broadcast::Receiver<RedisError>,
    mut reconnects: broadcast::Receiver<Server>,
    down: &AtomicBool,
    probe: Option<P>,
) where
    P: Fn() -> F,
    F: Future<Output = bool>,
{
    let (mut errors_open, mut reconnects_open) = (true, true);
    let mut probing: Option<Pin<Box<F>>> = None;
    // Whether the probe in flight is repeated if it gets no answer.
    let mut repeat = false;
    let mut unsettled = false;
    loop {
        let retry = unsettled && probing.is_none() && errors_open && reconnects_open;
        let event = tokio::select! {
            biased;
            event = errors.recv(), if errors_open => match event {
                Ok(_) => Some(ConnectionEvent::Dropped),
                Err(RecvError::Lagged(_)) => Some(ConnectionEvent::Missed),
                Err(RecvError::Closed) => {
                    errors_open = false;
                    None
                }
            },
            event = reconnects.recv(), if reconnects_open => match event {
                Ok(_) => Some(ConnectionEvent::Reconnected),
                Err(RecvError::Lagged(_)) => Some(ConnectionEvent::Missed),
                Err(RecvError::Closed) => {
                    reconnects_open = false;
                    None
                }
            },
            answered = in_flight(&mut probing), if probing.is_some() => {
                Some(ConnectionEvent::Probed(answered))
            }
            () = tokio::time::sleep(REPROBE_DELAY), if retry => Some(ConnectionEvent::Unsettled),
            // The client is gone and no probe is in flight: nothing to guard.
            else => return,
        };
        let Some(event) = event else { continue };
        let Some(probe) = &probe else {
            if event != ConnectionEvent::Reconnected {
                down.store(true, Ordering::Release);
            }
            continue;
        };
        match event {
            ConnectionEvent::Dropped => {
                down.store(true, Ordering::Release);
                unsettled = false;
                // This drop may be older than the reconnect that started the
                // probe in flight, whose answer could then predate the drop.
                // Only a PING sent after the drop tells. If it gets no answer
                // it is not repeated: a real drop brings a reconnect event.
                if probing.is_some() {
                    probing = Some(Box::pin(probe()));
                    repeat = false;
                }
            }
            // A probe already in flight began after the last drop the watcher
            // read, so its answer still counts.
            ConnectionEvent::Reconnected if probing.is_some() => repeat = true,
            ConnectionEvent::Reconnected | ConnectionEvent::Unsettled => {
                probing = Some(Box::pin(probe()));
                repeat = true;
            }
            ConnectionEvent::Missed => {
                // What is still buffered has lost its order, and may hide a
                // drop after the probe in flight began: start afresh.
                errors = errors.resubscribe();
                reconnects = reconnects.resubscribe();
                down.store(true, Ordering::Release);
                probing = Some(Box::pin(probe()));
                repeat = true;
            }
            ConnectionEvent::Probed(answered) => {
                probing = None;
                unsettled = !answered && repeat;
                if answered {
                    down.store(false, Ordering::Release);
                }
            }
        }
    }
}

/// The answer of the probe in flight; pending while there is none.
fn in_flight<F: Future<Output = bool>>(
    probing: &mut Option<Pin<Box<F>>>,
) -> impl Future<Output = bool> + '_ {
    std::future::poll_fn(move |cx| match probing.as_mut() {
        Some(probe) => probe.as_mut().poll(cx),
        None => Poll::Pending,
    })
}

// ── Backend impl ──────────────────────────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
#[cfg_attr(not(feature = "unsync"), async_trait)]
#[cfg_attr(feature = "unsync", async_trait(?Send))]
impl Backend for RedisBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        let result: Option<bytes::Bytes> = self.redis()?.get(key).await.map_err(redis_err)?;
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
        self.redis()?
            .set::<(), _, _>(key, value.as_slice(), expiration, None, false)
            .await
            .map_err(redis_err)
    }

    async fn delete(&self, key: &str) -> Result<bool, BackendError> {
        let removed: i64 = self.redis()?.del(key).await.map_err(redis_err)?;
        Ok(removed > 0)
    }

    async fn exists(&self, key: &str) -> Result<bool, BackendError> {
        let count: i64 = self.redis()?.exists(key).await.map_err(redis_err)?;
        Ok(count > 0)
    }

    async fn health(&self) -> Result<HealthStatus, BackendError> {
        let start = std::time::Instant::now();
        let _pong: String = self.redis()?.ping().await.map_err(redis_err)?;
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
        let secs: i64 = self.redis()?.ttl(key).await.map_err(redis_err)?;
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
            .redis()?
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
            .redis()?
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
    disable_offline_queue: bool,
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
    /// A `tokio::time::timeout` shorter than the budget ends the caller's wait
    /// but not the command. While the connection is down fred keeps such a
    /// command queued, value included: with `auto_reconnect` it is sent after
    /// the reconnect, and without it, it stays queued until the backend is
    /// dropped. [`disable_offline_queue`](Self::disable_offline_queue) fails
    /// commands at once instead of queueing them.
    ///
    /// `build` rejects `Duration::ZERO` with `CachekitError::Config`; fred
    /// reads zero as "no timeout".
    pub fn command_timeout(mut self, timeout: Duration) -> Self {
        self.command_timeout = Some(timeout);
        self
    }

    /// Fail each command at once while the connection is down, instead of
    /// queueing it until a reconnect. Off by default.
    ///
    /// By default fred queues every command sent while it has no connection,
    /// with no limit. With [`auto_reconnect`](Self::auto_reconnect) the queue
    /// is sent once the connection is back, including commands whose caller
    /// has given up; without it, nothing reads the queue again, so it grows
    /// until the backend is dropped. With this option the backend queues
    /// none of its commands from the moment fred reports the drop until the
    /// connection is back, and, without `auto_reconnect`, none after it.
    /// "Back" means the server answered a PING after fred reported the
    /// reconnect. While the backend waits for that answer, its PING sits in
    /// fred's queue. A PING can wait up to the command timeout; one second
    /// after an unanswered one ends, the backend sends another, until one is
    /// answered. A drop fred reports meanwhile marks the connection down at
    /// once, and the backend sends one fresh PING, because the drop may
    /// predate the reconnect; the earlier PING stays queued until the next
    /// reconnect.
    /// A command already written to the connection when fred notices the drop
    /// fails then, instead of being sent again after the reconnect; it may
    /// have run on the server. (Lock release is the exception: a release
    /// already written when the connection drops can be sent again after the
    /// reconnect, which is harmless because it deletes the lock only while
    /// the lock still holds the caller's token. fred does not promise that
    /// resend: it skips a release whose caller waited out the command
    /// timeout, and can drop one during a long outage, and the lock then
    /// stays until its TTL. A release called while the connection is down
    /// is refused like any other command.)
    ///
    /// A refused command returns `BackendErrorKind::Transient`. Under the
    /// reliability stack it is retried with backoff and counts toward
    /// opening the circuit breaker.
    ///
    /// Not covered: commands sent before fred reports the drop. fred notices
    /// a closed connection at once. It notices one that stays open but stops
    /// answering only through the unresponsive check of `auto_reconnect`, up
    /// to 4 s after the command timeout; until then commands are written to
    /// it. Most commands sent in that window fail when fred notices the drop,
    /// but some can still be sent after the reconnect.
    ///
    /// The backend learns of drops and reconnects from fred's connection
    /// events, so [`RedisBackend::connect`] may be called only once, and
    /// `build` rejects the option on a Redis Cluster URL, where one node's
    /// connection can drop while the others serve.
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), cachekit::CachekitError> {
    /// use cachekit::backend::redis::RedisBackend;
    ///
    /// let backend = RedisBackend::builder()
    ///     .url("redis://localhost:6379")
    ///     .auto_reconnect()
    ///     .disable_offline_queue()
    ///     .build()?;
    /// let _connection = backend.connect().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn disable_offline_queue(mut self) -> Self {
        self.disable_offline_queue = true;
        self
    }

    /// Consume the builder and construct a [`RedisBackend`].
    ///
    /// # Errors
    ///
    /// Returns an error if `url` was not provided or is not a valid Redis URL,
    /// if the command timeout is zero, or if
    /// [`disable_offline_queue`](Self::disable_offline_queue) is set on a
    /// Redis Cluster URL. The connection itself is lazy — call [`RedisBackend::connect`] to establish it.
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

        if self.disable_offline_queue && config.server.is_clustered() {
            return Err(CachekitError::Config(
                "disable_offline_queue does not support Redis Cluster".to_string(),
            ));
        }

        let (perf, connection) = fred_options(self.reconnect.is_some(), command_timeout);
        // fred's per-command `fail_fast` refuses a command while its client
        // state is not `Connected` and fails one in flight when the connection
        // drops; `OfflineGuard` covers the time that state lags.
        let options = Options {
            fail_fast: self.disable_offline_queue,
            ..Default::default()
        };
        let client = RedisClient::new(config, Some(perf), Some(connection), self.reconnect)
            .with_options(&options);
        let offline = self.disable_offline_queue.then(OfflineGuard::default);
        Ok(RedisBackend { client, offline })
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

    #[test]
    fn offline_queue_is_on_by_default() {
        for builder in [
            RedisBackend::builder().url(URL),
            RedisBackend::builder().url(URL).auto_reconnect(),
        ] {
            let backend = builder.build().expect("builds");
            assert!(!backend.client().options().fail_fast);
            assert!(backend.offline.is_none());
        }
    }

    #[test]
    fn disable_offline_queue_fails_fast_without_bounding_the_command_channel() {
        for reconnects in [false, true] {
            let mut builder = RedisBackend::builder().url(URL).disable_offline_queue();
            if reconnects {
                builder = builder.auto_reconnect();
            }
            let backend = builder.build().expect("builds");
            assert!(backend.client().options().fail_fast);
            // fred sends its own reconnect request through the command
            // channel; a length bound on that channel could refuse it.
            assert_eq!(
                backend.client().connection_config().max_command_buffer_len,
                0
            );
            assert!(backend.offline.is_some());
        }
    }

    #[test]
    fn disable_offline_queue_rejects_a_cluster_url() {
        let cluster = "redis-cluster://localhost:30001";
        RedisBackend::builder()
            .url(cluster)
            .build()
            .expect("a cluster URL builds without the option");
        let err = RedisBackend::builder()
            .url(cluster)
            .disable_offline_queue()
            .build()
            .expect_err("cluster is rejected");
        assert!(
            matches!(&err, crate::error::CachekitError::Config(m) if m.contains("Cluster")),
            "{err:?}"
        );
    }

    // ── follow_connection ────────────────────────────────────────────────────

    use ConnectionEvent::{Dropped, Reconnected};

    /// Run the watcher over `events`, all sent before it runs (the case of a
    /// watcher that fell behind), then close both channels. Returns the final
    /// `down` and the number of probes. `probe` is the probe's answer, or
    /// `None` for a backend without a reconnect policy.
    async fn follow(
        events: &[ConnectionEvent],
        capacity: usize,
        probe: Option<bool>,
    ) -> (bool, usize) {
        let (errors, errors_rx) = broadcast::channel(capacity);
        let (reconnects, reconnects_rx) = broadcast::channel(capacity);
        for event in events {
            // `send` fails only once every receiver is gone.
            let sent = match event {
                Dropped => errors
                    .send(RedisError::new(RedisErrorKind::IO, "closed"))
                    .is_ok(),
                _ => reconnects.send(Server::new("localhost", 6379)).is_ok(),
            };
            assert!(sent);
        }
        drop((errors, reconnects));

        let probes = std::sync::atomic::AtomicUsize::new(0);
        let probe = probe.map(|answer| {
            let probes = &probes;
            move || {
                probes.fetch_add(1, Ordering::Relaxed);
                std::future::ready(answer)
            }
        });
        let down = AtomicBool::new(false);
        follow_connection(errors_rx, reconnects_rx, &down, probe).await;
        (down.load(Ordering::Acquire), probes.load(Ordering::Relaxed))
    }

    #[tokio::test]
    async fn a_drop_after_the_first_connect_is_final_without_a_policy() {
        // The first connect's reconnect event and the drop are both pending;
        // the drop is processed first, and the older reconnect must not undo it.
        assert_eq!(follow(&[Reconnected, Dropped], 32, None).await, (true, 0));
        assert_eq!(follow(&[Dropped, Reconnected], 32, None).await, (true, 0));
        assert_eq!(follow(&[Reconnected], 32, None).await, (false, 0));
    }

    #[tokio::test]
    async fn a_reconnect_clears_a_drop_only_once_the_server_answers() {
        // An older reconnect read after a newer drop: the probe gets no answer.
        assert_eq!(
            follow(&[Reconnected, Dropped], 32, Some(false)).await,
            (true, 1)
        );
        // A drop then a reconnect: the probe gets an answer.
        assert_eq!(
            follow(&[Dropped, Reconnected], 32, Some(true)).await,
            (false, 1)
        );
        assert_eq!(follow(&[Dropped], 32, Some(true)).await, (true, 0));
    }

    #[tokio::test]
    async fn missed_events_are_settled_by_a_probe() {
        // Five drops overflow a channel of two, so the reconnect after them
        // is dropped with the backlog: only the probe can clear `down`.
        let overflow = [Dropped, Dropped, Dropped, Dropped, Dropped, Reconnected];
        assert_eq!(follow(&overflow, 2, Some(true)).await, (false, 1));
        assert_eq!(follow(&overflow, 2, Some(false)).await, (true, 1));
        assert_eq!(follow(&overflow, 2, None).await, (true, 0));
    }

    /// A watcher on open channels, probing with `answers` in turn (then `false`).
    /// Returns the senders that keep it alive, `down`, the probe count and the task.
    #[allow(clippy::type_complexity)]
    fn live_watcher(
        answers: Vec<bool>,
    ) -> (
        (broadcast::Sender<RedisError>, broadcast::Sender<Server>),
        Arc<AtomicBool>,
        Arc<std::sync::atomic::AtomicUsize>,
        tokio::task::JoinHandle<()>,
    ) {
        let (errors, errors_rx) = broadcast::channel(32);
        let (reconnects, reconnects_rx) = broadcast::channel(32);
        let down = Arc::new(AtomicBool::new(false));
        let probes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let probe = {
            let probes = Arc::clone(&probes);
            move || {
                let n = probes.fetch_add(1, Ordering::Relaxed);
                std::future::ready(answers.get(n).copied().unwrap_or(false))
            }
        };
        let task = tokio::spawn({
            let down = Arc::clone(&down);
            async move { follow_connection(errors_rx, reconnects_rx, &down, Some(probe)).await }
        });
        ((errors, reconnects), down, probes, task)
    }

    fn drop_event() -> RedisError {
        RedisError::new(RedisErrorKind::IO, "closed")
    }

    #[tokio::test(start_paused = true)]
    async fn an_unanswered_probe_is_repeated_until_one_is_answered() {
        // A server that stalled past the command timeout after a reconnect, then
        // recovered: no further event comes, so only a repeat can clear `down`.
        let ((errors, reconnects), down, probes, task) = live_watcher(vec![false, false, true]);
        errors.send(drop_event()).expect("watcher listening");
        reconnects
            .send(Server::new("localhost", 6379))
            .expect("watcher listening");
        tokio::time::timeout(Duration::from_secs(60), async {
            while probes.load(Ordering::Relaxed) < 3 || down.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("down never cleared");
        assert_eq!(probes.load(Ordering::Relaxed), 3);
        drop((errors, reconnects));
        task.await.expect("watcher ends with the channels");
    }
    /// A watcher on open channels whose probes follow `script` in turn: `None`
    /// waits forever (a PING with no answer yet), `Some(answer)` answers at
    /// once. Probes past the script wait forever.
    #[allow(clippy::type_complexity)]
    fn scripted_watcher(
        script: Vec<Option<bool>>,
    ) -> (
        (broadcast::Sender<RedisError>, broadcast::Sender<Server>),
        Arc<AtomicBool>,
        Arc<std::sync::atomic::AtomicUsize>,
        tokio::task::JoinHandle<()>,
    ) {
        let (errors, errors_rx) = broadcast::channel(32);
        let (reconnects, reconnects_rx) = broadcast::channel(32);
        let down = Arc::new(AtomicBool::new(false));
        let probes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let probe = {
            let probes = Arc::clone(&probes);
            move || -> Pin<Box<dyn Future<Output = bool> + Send>> {
                let n = probes.fetch_add(1, Ordering::Relaxed);
                match script.get(n).copied().flatten() {
                    Some(answer) => Box::pin(std::future::ready(answer)),
                    None => Box::pin(std::future::pending()),
                }
            }
        };
        let task = tokio::spawn({
            let down = Arc::clone(&down);
            async move { follow_connection(errors_rx, reconnects_rx, &down, Some(probe)).await }
        });
        ((errors, reconnects), down, probes, task)
    }

    fn reconnect_event() -> Server {
        Server::new("localhost", 6379)
    }

    /// Yield to the watcher until `condition` holds. Counted in yields, not
    /// time, so it also ends under a paused clock.
    async fn until(condition: impl Fn() -> bool) {
        for _ in 0..10_000 {
            if condition() {
                return;
            }
            tokio::task::yield_now().await;
        }
        assert!(condition(), "condition never held");
    }

    #[tokio::test]
    async fn a_drop_is_handled_while_a_probe_waits() {
        // The first connect's probe still waits for its PING when the
        // connection closes: the drop must not wait for the probe.
        let ((errors, reconnects), down, probes, task) = scripted_watcher(vec![]);
        reconnects
            .send(reconnect_event())
            .expect("watcher listening");
        until(|| probes.load(Ordering::Relaxed) == 1).await;
        // A second reconnect leaves the probe in flight alone.
        reconnects
            .send(reconnect_event())
            .expect("watcher listening");
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(probes.load(Ordering::Relaxed), 1);

        errors.send(drop_event()).expect("watcher listening");
        until(|| down.load(Ordering::Acquire)).await;
        // The drop replaced the probe in flight with a fresh one.
        until(|| probes.load(Ordering::Relaxed) == 2).await;
        task.abort();
    }

    #[tokio::test]
    async fn a_drop_older_than_the_reconnect_is_settled_by_a_fresh_probe() {
        // The channels share no order: the watcher can read a reconnect, start
        // its probe, and only then read a drop from before that reconnect. On a
        // healthy connection no further event comes, so the fresh probe the drop
        // starts must clear `down`.
        let ((errors, reconnects), down, probes, task) = scripted_watcher(vec![None, Some(true)]);
        reconnects
            .send(reconnect_event())
            .expect("watcher listening");
        until(|| probes.load(Ordering::Relaxed) == 1).await;
        errors.send(drop_event()).expect("watcher listening");
        until(|| probes.load(Ordering::Relaxed) == 2).await;
        until(|| !down.load(Ordering::Acquire)).await;
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn a_fresh_probe_after_a_drop_is_not_repeated() {
        // A real drop brings a reconnect event, so an unanswered probe the drop
        // started is left at that: no PING piles up in fred during the outage.
        let ((errors, reconnects), down, probes, task) = scripted_watcher(vec![None, Some(false)]);
        reconnects
            .send(reconnect_event())
            .expect("watcher listening");
        until(|| probes.load(Ordering::Relaxed) == 1).await;
        errors.send(drop_event()).expect("watcher listening");
        until(|| probes.load(Ordering::Relaxed) == 2).await;
        tokio::time::sleep(REPROBE_DELAY * 10).await;
        assert_eq!(probes.load(Ordering::Relaxed), 2);
        assert!(down.load(Ordering::Acquire));
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn a_drop_stops_the_repeats() {
        let ((errors, reconnects), down, probes, task) = live_watcher(vec![]);
        errors.send(drop_event()).expect("watcher listening");
        reconnects
            .send(Server::new("localhost", 6379))
            .expect("watcher listening");
        while probes.load(Ordering::Relaxed) < 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        errors.send(drop_event()).expect("watcher listening");
        tokio::time::sleep(REPROBE_DELAY * 10).await;
        assert_eq!(probes.load(Ordering::Relaxed), 1);
        assert!(down.load(Ordering::Acquire));
        drop((errors, reconnects));
        task.await.expect("watcher ends with the channels");
    }

    #[test]
    fn any_reply_counts_as_an_answer() {
        assert!(answered(Ok(())));
        assert!(answered(Err(RedisError::new(
            RedisErrorKind::Unknown,
            "NOPERM this user has no permissions to run the 'ping' command"
        ))));
        for kind in [
            RedisErrorKind::IO,
            RedisErrorKind::Canceled,
            RedisErrorKind::Timeout,
        ] {
            assert!(!answered(Err(RedisError::new(kind, "no reply"))));
        }
    }
}
