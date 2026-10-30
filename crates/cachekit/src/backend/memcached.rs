//! Memcached backend via [`rust-memcache`](https://crates.io/crates/memcache)
//! (ASCII protocol, per-socket read/write timeouts, backend-owned connection
//! checkout).
//!
//! ## Why the ASCII protocol
//!
//! The connection URL pins `protocol=ascii` (the same protocol cachekit-py's
//! pymemcache speaks) deliberately: rust-memcache's binary-protocol parser
//! does not correlate responses to requests (opcode/opaque unchecked), so a
//! connection desynced by a timed-out operation could serve a *late* response
//! as the answer to the next command — worst case a wrong value for a
//! different key. The ASCII `get` validates the echoed key. Every operation
//! additionally runs under a hard async time budget (see
//! [`crate::backend::memcached::MemcachedBackendBuilder::timeout`]) so a wedged server surfaces as a
//! `Timeout` error instead of hanging callers.
//!
//! ## The desync guard: discard on error
//!
//! A connection goes back to the idle list only after an operation whose
//! complete response parsed and returned `Ok`. Any error drops it, so a late
//! or partial response is never read as the answer to a later request. The
//! decision runs on the blocking thread that ran the operation: an operation
//! the async budget abandoned keeps its connection until it ends, and returns
//! it only if it then ends cleanly.
//!
//! rust-memcache's own pool cannot enforce this. Its `has_broken` always
//! returns `false`, so an errored connection goes back into the pool, and its
//! only guard is r2d2's checkout test: a `version` round trip before every
//! operation. The backend instead keeps its own idle list of
//! single-connection clients with that test off, which halves the round trips
//! per operation. The cost: each client's one-connection r2d2 pool runs its
//! own 3 worker threads, so a backend holds up to `3 × pool_size` parked
//! threads (30 at the default `pool_size` of 10). The inner pools run no
//! reaper, so a discarded client's threads exit when it is dropped; the
//! backend closes connections idle for over 10 minutes itself (r2d2's
//! default).
//!
//! A connection that died while idle (server restart, NAT idle drop) now fails
//! the one operation that finds it, with a retryable error, where the checkout
//! test used to replace it silently. A lost connection also drops every idle
//! connection, so a server restart costs about one failed operation.
//!
//! ## TTL capability — the honest parity picture
//!
//! cachekit-py's Memcached backend implements `refresh_ttl` (wrapping the
//! memcached `touch` command) but **not** `get_ttl` — the classic memcached
//! protocol has no command to read a key's remaining TTL. That makes py's
//! Memcached *not* a `TTLInspectableBackend`; its `refresh_ttl` is a bare,
//! directly-callable method outside the protocol.
//!
//! This backend mirrors that exactly: no [`TtlInspectable`] impl (the trait
//! requires the unreadable `ttl()`), and a bare inherent
//! [`refresh_ttl`](crate::backend::memcached::MemcachedBackend::refresh_ttl) wrapping `touch`. Rust
//! *could* read TTLs via the meta protocol (`mg <key> t`, memcached >= 1.6),
//! but shipping a capability py cannot match would make TTL-driven behaviour
//! diverge between SDKs on the same cluster. Revisit only when cachekit-py
//! gains meta-protocol support.
//!
//! [`TtlInspectable`]: crate::backend::TtlInspectable
//!
//! ## Why rust-memcache
//!
//! The previously used `async-memcached 0.6` ships `toxiproxy_rust` (a fault
//! injection test proxy) as a non-optional *runtime* dependency, dragging
//! openssl/native-tls/hyper-0.14 into a rustls-only SDK — so it was
//! rejected. rust-memcache with `default-features = false` has a
//! lean tree, a connection pool (one hung connection doesn't wedge the
//! backend), per-socket timeouts, and `touch`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use memcache::{
    Client, ClientError, CommandError, ConnectionManager, MemcacheError, ServerError, Url,
};
use tokio::sync::Semaphore;

use async_trait::async_trait;

use crate::backend::{run_blocking, Backend, HealthStatus};
use crate::error::{BackendError, BackendErrorKind};

/// Memcached rejects TTLs above 30 days *as durations* — anything larger is
/// silently reinterpreted as an absolute unix timestamp, expiring the entry
/// in the past. Clamp instead, matching cachekit-py's `MAX_MEMCACHED_TTL`.
const MAX_MEMCACHED_TTL_SECS: u64 = 30 * 24 * 60 * 60;

/// Memcached's default server-side item-size limit (`-I` flag).
const DEFAULT_MAX_ITEM_SIZE_BYTES: usize = 1024 * 1024;

/// Memcached's hard key-length limit (bytes).
const MAX_KEY_LEN: usize = 250;

// py-parity defaults (cachekit-py MemcachedBackendConfig).
const DEFAULT_OP_TIMEOUT: Duration = Duration::from_secs(1);
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_POOL_SIZE: u32 = 10;

// ── Key validation (CWE-93) ───────────────────────────────────────────────────

/// Reject keys the memcached ASCII protocol cannot represent safely.
///
/// The ASCII protocol is `\r\n`-delimited and space-separated: a key
/// containing control bytes or spaces injects protocol commands (CWE-93 —
/// `flush_all`, cross-key `set`/`delete`). This backend speaks ASCII, so the
/// guard is load-bearing. Non-ASCII bytes (> 0x7e) are rejected too:
/// cachekit-py's pymemcache refuses them (`allow_unicode_keys=False`
/// default), so allowing them here would create keys py cannot address —
/// breaking cross-SDK key identity.
fn validate_key(key: &str) -> Result<(), BackendError> {
    if key.is_empty() {
        return Err(BackendError::permanent("memcached key must not be empty"));
    }
    if key.len() > MAX_KEY_LEN {
        return Err(BackendError::permanent(format!(
            "memcached key is {} bytes (limit {MAX_KEY_LEN})",
            key.len()
        )));
    }
    if key.bytes().any(|b| b <= 0x20 || b >= 0x7f) {
        return Err(BackendError::permanent(
            "memcached key contains whitespace, control, or non-ASCII bytes — not \
             representable in the memcached ASCII protocol (and rejected by cachekit-py, \
             breaking cross-SDK key identity)",
        ));
    }
    Ok(())
}

// ── Error mapping ─────────────────────────────────────────────────────────────

/// SO_RCVTIMEO/SO_SNDTIMEO expiry surfaces as `WouldBlock` on unix.
fn is_timeout(kind: std::io::ErrorKind) -> bool {
    matches!(
        kind,
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
    )
}

/// rust-memcache 0.19's ASCII reader reports a peer that closed the socket
/// (a read of 0 bytes) as this `ClientError`, not as an I/O error.
const ASCII_EOF: &str = "Ascii protocol no line found";

/// memcached's reply when it is at `-c` (maxconns).
const CONN_LIMIT: &str = "ERROR Too many open connections";

/// The connection is gone, not merely slow: reset, broken pipe or EOF. That
/// usually means a server restart, so every idle connection is suspect too.
fn connection_lost(e: &MemcacheError) -> bool {
    match e {
        MemcacheError::IOError(io) => !is_timeout(io.kind()),
        MemcacheError::ClientError(ClientError::Error(msg)) => msg == ASCII_EOF,
        _ => false,
    }
}

fn memcached_err(e: MemcacheError) -> BackendError {
    let kind = match &e {
        MemcacheError::IOError(io) if is_timeout(io.kind()) => BackendErrorKind::Timeout,
        MemcacheError::IOError(_) => BackendErrorKind::Transient,
        // EOF: the server closed the connection. The retry runs on a fresh one.
        e if connection_lost(e) => BackendErrorKind::Transient,
        // Pool exhaustion / connection acquisition — retryable.
        MemcacheError::PoolError(_) => BackendErrorKind::Transient,
        // Oversized items must NOT be retried — the same payload fails
        // forever (Transient here means infinite retry
        // loops upstream). The ASCII protocol reports it as a SERVER_ERROR
        // string ("SERVER_ERROR object too large for cache", verified live);
        // the binary protocol as CommandError::ValueTooLarge.
        MemcacheError::ServerError(ServerError::Error(msg)) if msg.contains("object too large") => {
            BackendErrorKind::Permanent
        }
        // Other SERVER_ERROR strings are retryable (py maps
        // MemcacheServerError → TRANSIENT); malformed responses are protocol
        // violations — not.
        MemcacheError::ServerError(ServerError::Error(_)) => BackendErrorKind::Transient,
        // A server at its connection limit answers a new connection's first
        // command with this line, then closes it. rust-memcache passes it
        // through as a malformed response, but it is capacity, not protocol.
        MemcacheError::ServerError(ServerError::BadResponse(msg))
            if msg.starts_with(CONN_LIMIT) =>
        {
            BackendErrorKind::Transient
        }
        MemcacheError::ServerError(_) => BackendErrorKind::Permanent,
        MemcacheError::CommandError(CommandError::ValueTooLarge) => BackendErrorKind::Permanent,
        MemcacheError::ClientError(_)
        | MemcacheError::CommandError(_)
        | MemcacheError::ParseError(_)
        | MemcacheError::BadURL(_) => BackendErrorKind::Permanent,
    };

    BackendError {
        kind,
        message: e.to_string(),
        source: Some(Box::new(e)),
    }
}

/// Wire-level expiration: 0 = never expires; sub-second TTLs round up to 1s
/// and anything above 30 days clamps (see [`MAX_MEMCACHED_TTL_SECS`]).
fn expiration_secs(ttl: Option<Duration>) -> u32 {
    match ttl {
        None => 0,
        Some(d) => {
            let secs = d.as_secs().clamp(1, MAX_MEMCACHED_TTL_SECS);
            // 30 days fits u32 by construction.
            u32::try_from(secs).unwrap_or(u32::MAX)
        }
    }
}

// ── Connection checkout ───────────────────────────────────────────────────────

/// An idle connection older than this is closed instead of reused: r2d2's
/// default `idle_timeout`, which the inner pools turn off (see `open`).
const IDLE_TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// Idle single-connection clients, and the permits that cap how many exist.
///
/// This is the desync guard the module docs describe. Each client wraps a
/// one-connection r2d2 pool with the checkout test off, because
/// rust-memcache's `Client` has no public constructor over a bare connection.
struct Connections {
    url: Url,
    connect_timeout: Duration,
    /// LIFO, like r2d2: the most recently used connection is reused first,
    /// so the list is ordered by when each client went idle, oldest first.
    idle: Mutex<Vec<(Client, Instant)>>,
    /// One permit per client in use, held until the blocking call ends, so
    /// idle plus in-use clients never exceed `pool_size`.
    permits: Arc<Semaphore>,
}

impl Connections {
    fn new(url: Url, connect_timeout: Duration, pool_size: u32) -> Self {
        Self {
            url,
            connect_timeout,
            idle: Mutex::new(Vec::new()),
            permits: Arc::new(Semaphore::new(
                usize::try_from(pool_size)
                    .map_or(Semaphore::MAX_PERMITS, |n| n.min(Semaphore::MAX_PERMITS)),
            )),
        }
    }

    fn open(&self) -> Result<Client, MemcacheError> {
        let pool = memcache::Pool::builder()
            .max_size(1)
            // build() opens nothing; the op's first checkout connects, bounded
            // by connection_timeout.
            .min_idle(Some(0))
            .test_on_check_out(false)
            // No reaper. With either lifetime set, r2d2 schedules a reaper on
            // the pool's 3 worker threads, and a dropped pool's threads stay
            // alive until that job is due (30 s): every discarded client would
            // leave 3 threads behind. Idle expiry is done in `checkout`.
            .max_lifetime(None)
            .idle_timeout(None)
            .connection_timeout(self.connect_timeout)
            .build(ConnectionManager::new(self.url.clone()))?;
        Client::with_pool(pool)
    }

    /// The most recently idled client, or a new one. When it is past
    /// [`IDLE_TIMEOUT`], so is every client below it: all are closed.
    fn checkout(&self) -> Result<Client, MemcacheError> {
        let mut idle = self.lock_idle();
        match idle.pop() {
            Some((client, since)) if since.elapsed() < IDLE_TIMEOUT => return Ok(client),
            Some(stale) => {
                let older = std::mem::take(&mut *idle);
                drop(idle);
                // Dropped outside the lock: each drop stops 3 worker threads.
                drop((stale, older));
            }
            None => drop(idle),
        }
        self.open()
    }

    /// Run `op` on an idle client, or a new one, on the calling thread.
    ///
    /// The client goes back to the idle list only if `op` returned `Ok`, so
    /// its whole response was parsed. Any error drops it, and a lost
    /// connection drops every idle one too. The one exception is a failed
    /// checkout (`PoolError`): the client never held a connection, so there
    /// is nothing to desync, and keeping it stops a dead server from costing
    /// 3 new threads per attempt.
    fn run<T>(
        &self,
        op: impl FnOnce(&Client) -> Result<T, MemcacheError>,
    ) -> Result<T, BackendError> {
        let client = self.checkout().map_err(memcached_err)?;
        match op(&client) {
            Ok(value) => {
                self.lock_idle().push((client, Instant::now()));
                Ok(value)
            }
            Err(e @ MemcacheError::PoolError(_)) => {
                self.lock_idle().push((client, Instant::now()));
                Err(memcached_err(e))
            }
            Err(e) => {
                drop(client);
                if connection_lost(&e) {
                    let stale = std::mem::take(&mut *self.lock_idle());
                    drop(stale);
                }
                Err(memcached_err(e))
            }
        }
    }

    /// The list is only pushed, popped and taken, so a poisoned lock still
    /// guards a consistent `Vec`.
    fn lock_idle(&self) -> MutexGuard<'_, Vec<(Client, Instant)>> {
        self.idle.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

// ── MemcachedBackend ──────────────────────────────────────────────────────────

/// Memcached backend powered by [`rust-memcache`](https://crates.io/crates/memcache).
///
/// Build with [`builder`](MemcachedBackend::builder); the terminal
/// [`connect`](MemcachedBackendBuilder::connect) is async and verifies the
/// server is reachable (unlike [`RedisBackend`]'s lazy build-then-connect
/// split).
///
/// [`RedisBackend`]: crate::backend::redis::RedisBackend
pub struct MemcachedBackend {
    // ponytail: single server — py's HashClient shards across servers; add
    // multi-server + consistent hashing when needed.
    conns: Arc<Connections>,
    max_item_size_bytes: usize,
    /// Hard async ceiling per operation, including the wait for a free
    /// connection. Socket timeouts bound each I/O call, not the whole
    /// operation — this budget bounds the caller.
    op_budget: Duration,
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

    /// Wait for a free connection, then run `op` on the blocking pool, all
    /// under the op budget.
    ///
    /// A server that trickles its response, or a connect or a busy pool, can
    /// outlast any single socket timeout; the budget converts that into an
    /// immediate `Timeout` error for the caller. The abandoned blocking task
    /// keeps its permit and connection until its own I/O ends, then returns
    /// the connection only on `Ok` (see [`Connections::run`]). Under `unsync`
    /// the call runs inline (no budget — there is no executor to time it out
    /// against; the socket timeouts still bound each I/O call).
    #[cfg(not(feature = "unsync"))]
    async fn run_op<T: Send + 'static>(
        &self,
        op: impl FnOnce(&Client) -> Result<T, MemcacheError> + Send + 'static,
    ) -> Result<T, BackendError> {
        let conns = Arc::clone(&self.conns);
        let checkout = async move {
            let permit = Arc::clone(&conns.permits)
                .acquire_owned()
                .await
                .map_err(|_| BackendError::permanent("memcached connection permits closed"))?;
            run_blocking(move || {
                let _permit = permit;
                conns.run(op)
            })
            .await
        };
        match tokio::time::timeout(self.op_budget, checkout).await {
            Ok(result) => result,
            Err(_) => Err(BackendError::timeout(format!(
                "memcached operation exceeded its {}ms budget (server hung or every \
                 connection busy); the in-flight call was abandoned",
                self.op_budget.as_millis()
            ))),
        }
    }

    /// `unsync` variant — see above.
    #[cfg(feature = "unsync")]
    async fn run_op<T>(
        &self,
        op: impl FnOnce(&Client) -> Result<T, MemcacheError>,
    ) -> Result<T, BackendError> {
        let _permit = self
            .conns
            .permits
            .acquire()
            .await
            .map_err(|_| BackendError::permanent("memcached connection permits closed"))?;
        run_blocking(|| self.conns.run(op)).await
    }

    /// Refresh a key's TTL via the memcached `touch` command.
    ///
    /// Mirrors cachekit-py's `refresh_ttl`: a bare method, **not** part of
    /// [`TtlInspectable`](crate::backend::TtlInspectable) — memcached cannot
    /// *read* TTLs, so the trait (which requires `ttl()`) cannot be
    /// implemented, and TTL-refresh-on-read features will not auto-engage on
    /// this backend in any SDK. `None` makes the entry permanent (py's
    /// `refresh_ttl(key, 0)`); TTLs clamp to the 30-day ceiling like
    /// [`set`](Backend::set).
    ///
    /// Returns `true` if the key existed and its expiry was updated.
    pub async fn refresh_ttl(
        &self,
        key: &str,
        ttl: Option<Duration>,
    ) -> Result<bool, BackendError> {
        validate_key(key)?;
        let key = key.to_owned();
        let expire = expiration_secs(ttl);
        self.run_op(move |client| client.touch(&key, expire)).await
    }
}

// ── Backend impl ──────────────────────────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
#[cfg_attr(not(feature = "unsync"), async_trait)]
#[cfg_attr(feature = "unsync", async_trait(?Send))]
impl Backend for MemcachedBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        validate_key(key)?;
        let key = key.to_owned();
        self.run_op(move |client| client.get::<Vec<u8>>(&key)).await
    }

    async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        validate_key(key)?;
        // Fail loudly on oversized items instead of waiting for the server
        // to reject them: callers should compress, shard, or switch backends
        // (mirrors cachekit-py's client-side guard). The server-side
        // rejection maps to a Permanent error either way (see memcached_err).
        if self.max_item_size_bytes > 0 && value.len() > self.max_item_size_bytes {
            return Err(BackendError::permanent(format!(
                "value is {} bytes, exceeding the memcached max item size of {} bytes; \
                 enable compression, use a larger-payload backend (Redis/SaaS/File), or \
                 raise both the server's -I limit and the builder's max_item_size_bytes",
                value.len(),
                self.max_item_size_bytes,
            )));
        }

        let key = key.to_owned();
        let expire = expiration_secs(ttl);
        self.run_op(move |client| client.set(&key, value.as_slice(), expire))
            .await
    }

    async fn delete(&self, key: &str) -> Result<bool, BackendError> {
        validate_key(key)?;
        let key = key.to_owned();
        self.run_op(move |client| client.delete(&key)).await
    }

    async fn exists(&self, key: &str) -> Result<bool, BackendError> {
        // Memcached has no EXISTS command; a get is the probe (same trade-off
        // as cachekit-py — the value crosses the wire and is discarded).
        validate_key(key)?;
        let key = key.to_owned();
        self.run_op(move |client| Ok(client.get::<Vec<u8>>(&key)?.is_some()))
            .await
    }

    async fn health(&self) -> Result<HealthStatus, BackendError> {
        let start = std::time::Instant::now();
        let versions = self.run_op(|client| client.version()).await?;
        let latency = start.elapsed();

        let mut details = HashMap::new();
        if let Some((_, version)) = versions.first() {
            details.insert("version".to_string(), version.clone());
        }
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
///
/// Timeout and pool defaults mirror cachekit-py's `MemcachedBackendConfig`
/// (1s op timeout, 2s connect timeout, pool of 10).
#[derive(Default)]
#[must_use]
pub struct MemcachedBackendBuilder {
    url: Option<String>,
    max_item_size_bytes: Option<usize>,
    timeout: Option<Duration>,
    connect_timeout: Option<Duration>,
    pool_size: Option<u32>,
}

impl MemcachedBackendBuilder {
    /// Set the memcached server address (required).
    ///
    /// Accepts `memcache://host:port`, `tcp://host:port`, or bare
    /// `host:port`.
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

    /// Socket read/write timeout per operation (default 1s). A hung server
    /// errors that operation instead of wedging the backend.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// TCP connect timeout (default 2s).
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Maximum open connections, and so concurrent operations (default 10).
    ///
    /// Each connection also runs 3 parked r2d2 worker threads (see the module
    /// docs), so the default costs up to 30 threads.
    pub fn pool_size(mut self, size: u32) -> Self {
        self.pool_size = Some(size.max(1));
        self
    }

    /// Normalize accepted URL spellings to rust-memcache's `memcache://`.
    fn normalized_url(url: &str) -> String {
        if let Some(rest) = url.strip_prefix("tcp://") {
            return format!("memcache://{rest}");
        }
        if url.contains("://") {
            return url.to_string();
        }
        format!("memcache://{url}")
    }

    /// Pin the connection parameters rust-memcache reads from the URL.
    ///
    /// - `protocol=ascii`: see the module docs — the binary parser does not
    ///   correlate responses to requests, so a timed-out op could desync a
    ///   pooled connection into serving wrong values.
    /// - `timeout=<secs>`: the only spelling that applies socket read/write
    ///   timeouts to **every** connection at creation.
    ///   `ClientBuilder::with_read_timeout` reaches exactly one checked-out
    ///   connection and none of the replacements r2d2 creates later (a hung
    ///   server wedged the backend despite configured timeouts).
    ///
    /// Explicit user-supplied `protocol=`/`timeout=` params win.
    fn with_connection_params(url: &str, timeout: Duration) -> String {
        let mut url = url.to_string();
        let mut sep = if url.contains('?') { '&' } else { '?' };
        // Match whole param names: a bare `contains("timeout=")` would also
        // match a user-supplied `connect_timeout=` and silently skip pinning
        // the load-bearing socket timeout.
        let has_param = |u: &str, name: &str| {
            u.contains(&format!("?{name}=")) || u.contains(&format!("&{name}="))
        };
        if !has_param(&url, "protocol") {
            url.push(sep);
            url.push_str("protocol=ascii");
            sep = '&';
        }
        if !has_param(&url, "timeout") {
            url.push(sep);
            url.push_str(&format!("timeout={}", timeout.as_secs_f64()));
        }
        url
    }

    /// Connect to memcached and construct a [`MemcachedBackend`].
    ///
    /// Opens one connection and pings the server (`version`), so a bad
    /// address fails here rather than on the first cache operation. Later
    /// operations send no ping.
    ///
    /// # Errors
    ///
    /// Returns a config error if `url` was not provided or is invalid, or a
    /// backend error if the connection cannot be established.
    pub async fn connect(self) -> Result<MemcachedBackend, crate::error::CachekitError> {
        use crate::error::CachekitError;

        let url = self
            .url
            .filter(|u| !u.is_empty())
            .ok_or_else(|| CachekitError::Config("url is required".to_string()))?;

        let timeout = self.timeout.unwrap_or(DEFAULT_OP_TIMEOUT);
        let connect_timeout = self.connect_timeout.unwrap_or(DEFAULT_CONNECT_TIMEOUT);
        let pool_size = self.pool_size.unwrap_or(DEFAULT_POOL_SIZE);
        let url = Self::with_connection_params(&Self::normalized_url(&url), timeout);
        let url =
            Url::parse(&url).map_err(|e| memcached_err(MemcacheError::BadURL(e.to_string())))?;
        // rust-memcache's ClientBuilder check, kept so a wrong scheme fails
        // here instead of as a connect timeout on the first checkout.
        if !matches!(url.scheme(), "memcache" | "memcache+tls" | "memcache+udp") {
            return Err(memcached_err(MemcacheError::BadURL(format!(
                "Unsupported protocol: {}",
                url.scheme()
            )))
            .into());
        }

        // Budget: a connect plus a few socket timeouts' worth of I/O covers
        // the healthy path with slack while still failing a wedged server
        // fast. It also bounds the wait for a free connection.
        let op_budget = timeout * 3 + connect_timeout;

        let backend = MemcachedBackend {
            conns: Arc::new(Connections::new(url, connect_timeout, pool_size)),
            max_item_size_bytes: self
                .max_item_size_bytes
                .unwrap_or(DEFAULT_MAX_ITEM_SIZE_BYTES),
            op_budget,
        };
        // Eager reachability check — connect() promises a live server. The
        // socket timeouts (via the URL) and the op budget bound it even
        // against a wedged-but-accepting server. The connection it opens is
        // the first idle one.
        backend.run_op(|client| client.version()).await?;
        Ok(backend)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)] // test-only: failures should panic loudly
mod tests {
    use memcache::ClientError;

    use super::*;

    #[test]
    fn error_mapping_classifies_by_retryability() {
        let refused = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        assert_eq!(
            memcached_err(MemcacheError::IOError(refused)).kind,
            BackendErrorKind::Transient
        );

        let timed_out = std::io::Error::new(std::io::ErrorKind::TimedOut, "slow");
        assert_eq!(
            memcached_err(MemcacheError::IOError(timed_out)).kind,
            BackendErrorKind::Timeout
        );

        // SO_RCVTIMEO expiry surfaces as WouldBlock on unix sockets.
        let would_block = std::io::Error::new(std::io::ErrorKind::WouldBlock, "recv timeout");
        assert_eq!(
            memcached_err(MemcacheError::IOError(would_block)).kind,
            BackendErrorKind::Timeout
        );

        assert_eq!(
            memcached_err(MemcacheError::ServerError(ServerError::Error(
                "SERVER_ERROR out of memory".into()
            )))
            .kind,
            BackendErrorKind::Transient
        );

        // ASCII-protocol oversize rejection (exact live server string) must
        // be Permanent — the same payload can never succeed on retry.
        assert_eq!(
            memcached_err(MemcacheError::ServerError(ServerError::Error(
                "SERVER_ERROR object too large for cache".into()
            )))
            .kind,
            BackendErrorKind::Permanent
        );

        // Oversized items must be Permanent — Transient would mean upstream
        // retry loops resending a payload that can never succeed.
        assert_eq!(
            memcached_err(MemcacheError::CommandError(CommandError::ValueTooLarge)).kind,
            BackendErrorKind::Permanent
        );

        assert_eq!(
            memcached_err(MemcacheError::ClientError(ClientError::KeyTooLong)).kind,
            BackendErrorKind::Permanent
        );

        // EOF (server closed the connection) is retryable on a fresh one, and
        // drops the idle list; a timeout is neither.
        let eof = MemcacheError::ClientError(ClientError::Error(ASCII_EOF.into()));
        assert!(connection_lost(&eof));
        assert_eq!(memcached_err(eof).kind, BackendErrorKind::Transient);
        let reset = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset");
        assert!(connection_lost(&MemcacheError::IOError(reset)));
        let slow = std::io::Error::new(std::io::ErrorKind::WouldBlock, "recv timeout");
        assert!(!connection_lost(&MemcacheError::IOError(slow)));

        // A server at maxconns is capacity, not a protocol violation.
        let full = ServerError::BadResponse(format!("{CONN_LIMIT}\r\n").into());
        assert_eq!(
            memcached_err(MemcacheError::ServerError(full)).kind,
            BackendErrorKind::Transient
        );
        let garbage = ServerError::BadResponse("key doesn't match in the response".into());
        assert_eq!(
            memcached_err(MemcacheError::ServerError(garbage)).kind,
            BackendErrorKind::Permanent
        );
    }

    #[test]
    fn key_validation_rejects_protocol_metacharacters() {
        // CWE-93: CRLF and space are ASCII-protocol framing bytes.
        assert!(validate_key("evil\r\nflush_all\r\n").is_err());
        assert!(validate_key("evil\nkey").is_err());
        assert!(validate_key("evil key").is_err());
        assert!(validate_key("evil\tkey").is_err());
        assert!(validate_key("evil\x00key").is_err());
        assert!(validate_key("evil\x7fkey").is_err());
        assert!(validate_key("").is_err());
        assert!(validate_key(&"k".repeat(251)).is_err());

        // Non-ASCII: pymemcache rejects it (allow_unicode_keys=False), so a
        // key writable from rs would be unaddressable from py.
        assert!(validate_key("café").is_err());
        assert!(validate_key("键").is_err());

        // The canonical cache-key format passes.
        assert!(validate_key("ns:app:func:m.f:args:abc123:v1").is_ok());
        assert!(validate_key(&"k".repeat(250)).is_ok());
    }

    #[test]
    fn expiration_clamps_to_memcached_ceiling() {
        assert_eq!(expiration_secs(None), 0);
        assert_eq!(expiration_secs(Some(Duration::from_millis(10))), 1);
        assert_eq!(expiration_secs(Some(Duration::from_secs(60))), 60);
        // > 30 days would be read as an absolute unix timestamp — clamp.
        let clamped = expiration_secs(Some(Duration::from_secs(MAX_MEMCACHED_TTL_SECS + 1)));
        assert_eq!(u64::from(clamped), MAX_MEMCACHED_TTL_SECS);
    }

    #[test]
    fn url_normalization() {
        assert_eq!(
            MemcachedBackendBuilder::normalized_url("tcp://h:11211"),
            "memcache://h:11211"
        );
        assert_eq!(
            MemcachedBackendBuilder::normalized_url("h:11211"),
            "memcache://h:11211"
        );
        assert_eq!(
            MemcachedBackendBuilder::normalized_url("memcache://h:11211?protocol=ascii"),
            "memcache://h:11211?protocol=ascii"
        );
    }

    #[test]
    fn connection_params_pin_ascii_and_per_connection_timeout() {
        // `timeout=` on the URL is the only spelling that reaches EVERY
        // pooled connection; `protocol=ascii` is deliberate.
        assert_eq!(
            MemcachedBackendBuilder::with_connection_params(
                "memcache://h:11211",
                Duration::from_secs(1)
            ),
            "memcache://h:11211?protocol=ascii&timeout=1"
        );
        // Explicit user params win — nothing is duplicated.
        assert_eq!(
            MemcachedBackendBuilder::with_connection_params(
                "memcache://h:11211?protocol=binary&timeout=5",
                Duration::from_secs(1)
            ),
            "memcache://h:11211?protocol=binary&timeout=5"
        );
        assert_eq!(
            MemcachedBackendBuilder::with_connection_params(
                "memcache://h:11211?tcp_nodelay=true",
                Duration::from_millis(1500)
            ),
            "memcache://h:11211?tcp_nodelay=true&protocol=ascii&timeout=1.5"
        );
        // `connect_timeout=` must NOT be mistaken for `timeout=` — the
        // load-bearing socket timeout still gets pinned.
        assert_eq!(
            MemcachedBackendBuilder::with_connection_params(
                "memcache://h:11211?connect_timeout=5",
                Duration::from_secs(1)
            ),
            "memcache://h:11211?connect_timeout=5&protocol=ascii&timeout=1"
        );
    }
}
