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
//! ## Transports and socket timeouts
//!
//! [`url`](crate::backend::memcached::MemcachedBackendBuilder::url) accepts
//! TCP, UDP (`memcache+udp://`, or `?udp=true`) and Unix-socket
//! (`memcache:///path`) URLs, plus `memcache+tls://` when rust-memcache's
//! `tls` feature is enabled in the build (cachekit does not enable it).
//! rust-memcache applies the URL's `timeout=` to TCP and TLS sockets only,
//! so the backend sets the configured timeout on every connection itself
//! before its first operation. Without it a lost UDP reply or a stalled
//! Unix-socket server would block a connection forever, and with it a
//! permit, until every operation failed.
//!
//! Over UDP every `get` fails with a retryable error: rust-memcache 0.19
//! never sends an ASCII `get` there. Writes work.
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
//! A connection that died while idle now fails the one operation that finds
//! it, with a retryable error, where the checkout test used to replace it
//! silently. When that connection was reset or closed (EOF), as a server
//! restart does, or a NAT that resets an expired mapping, every other idle
//! connection is dropped too, so a restart costs about one failed operation.
//! A new connection lost the same way drops nothing: a server at its
//! connection limit closes new connections while the idle ones still work. A
//! NAT or firewall that drops an idle connection silently sends no reset, so
//! the operation fails with a read timeout and no other idle connection is
//! dropped: each dead idle connection costs one operation, after
//! [`timeout`](crate::backend::memcached::MemcachedBackendBuilder::timeout).
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
#[cfg(not(feature = "unsync"))]
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

/// The connection is gone, not merely slow: reset, broken pipe or EOF. On a
/// connection that was idle, that usually means a server restart, so every
/// idle connection is suspect too.
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

/// Remove and return the entries idle for at least [`IDLE_TIMEOUT`] at `now`.
///
/// Entries are only pushed (stamped `now`) and popped at the end, so the list
/// is sorted oldest first and the expired entries are a prefix.
fn split_expired<T>(idle: &mut Vec<(T, Instant)>, now: Instant) -> Vec<(T, Instant)> {
    let cut =
        idle.partition_point(|(_, since)| now.saturating_duration_since(*since) >= IDLE_TIMEOUT);
    idle.drain(..cut).collect()
}

/// A single-connection client, and whether [`Connections::prepare`] has set
/// up its connection (socket timeouts, then authentication).
struct Conn {
    client: Client,
    ready: bool,
}

/// Idle single-connection clients, and the permits that cap how many exist.
///
/// This is the desync guard the module docs describe. Each client wraps a
/// one-connection r2d2 pool with the checkout test off, because
/// rust-memcache's `Client` has no public constructor over a bare connection.
struct Connections {
    url: Url,
    connect_timeout: Duration,
    /// Socket read/write timeout for every connection. rust-memcache applies
    /// the URL's `timeout=` to TCP (and TLS) streams only, so a UDP or
    /// Unix-socket connection would block forever on a lost reply, holding
    /// its permit. [`Connections::run`] sets it on each connection itself.
    io_timeout: Duration,
    /// `<user> <password>` for the ASCII `set auth` command, sent once the
    /// timeouts are on. rust-memcache would authenticate inside the pool's
    /// connect instead, before a UDP or Unix socket has any timeout: a lost
    /// reply there blocks that connect forever, and the pool, which allows
    /// one connect at a time, never connects again.
    auth: Option<String>,
    /// LIFO, like r2d2: the most recently used connection is reused first,
    /// so the list is ordered by when each client went idle, oldest first.
    idle: Mutex<Vec<(Conn, Instant)>>,
    /// One permit per client in use, held until the blocking call ends, so
    /// idle plus in-use clients never exceed `pool_size`. Under `unsync` an
    /// operation runs inline, so only one client is ever in use.
    #[cfg(not(feature = "unsync"))]
    permits: Arc<Semaphore>,
}

impl Connections {
    // Under `unsync` no permits cap the clients (see `run_op`).
    #[cfg_attr(feature = "unsync", allow(unused_variables))]
    fn new(
        url: Url,
        connect_timeout: Duration,
        io_timeout: Duration,
        auth: Option<String>,
        pool_size: u32,
    ) -> Self {
        Self {
            url,
            connect_timeout,
            io_timeout,
            auth,
            idle: Mutex::new(Vec::new()),
            #[cfg(not(feature = "unsync"))]
            permits: Arc::new(Semaphore::new(
                usize::try_from(pool_size)
                    .map_or(Semaphore::MAX_PERMITS, |n| n.min(Semaphore::MAX_PERMITS)),
            )),
        }
    }

    fn open(&self) -> Result<Conn, MemcacheError> {
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
        Ok(Conn {
            client: Client::with_pool(pool)?,
            ready: false,
        })
    }

    /// Set the socket timeouts on `conn`'s connection, then authenticate it,
    /// once. A new client connects here, bounded by `connect_timeout`.
    ///
    /// The timeouts then hold for the client's life: its pool never replaces
    /// its one connection (no checkout test, no reaper, and rust-memcache's
    /// `has_broken` is always `false`).
    fn prepare(&self, conn: &mut Conn) -> Result<(), MemcacheError> {
        if !conn.ready {
            conn.client.set_read_timeout(Some(self.io_timeout))?;
            conn.client.set_write_timeout(Some(self.io_timeout))?;
            if let Some(auth) = &self.auth {
                // rust-memcache's own ASCII auth sends exactly this `set`.
                conn.client.set("auth", auth.as_str(), 0)?;
            }
            conn.ready = true;
        }
        Ok(())
    }

    /// The most recently idled client, or a new one. Every checkout first
    /// closes the clients idle for longer than [`IDLE_TIMEOUT`], so a hot top
    /// entry cannot keep older idle connections (and their threads) alive.
    fn checkout(&self) -> Result<Conn, MemcacheError> {
        let mut idle = self.lock_idle();
        let expired = split_expired(&mut idle, Instant::now());
        let fresh = idle.pop();
        drop(idle);
        // Dropped outside the lock: each drop stops 3 worker threads.
        drop(expired);
        match fresh {
            Some((conn, _)) => Ok(conn),
            None => self.open(),
        }
    }

    /// Run `op` on an idle client, or a new one, on the calling thread, after
    /// [`prepare`](Self::prepare) on a client not yet prepared.
    ///
    /// The client goes back to the idle list only if `op` returned `Ok`, so
    /// its whole response was parsed. Any error drops it. The one exception
    /// is a failed checkout (`PoolError`): the client never held a
    /// connection, so there is nothing to desync, and keeping it stops a dead
    /// server from costing 3 new threads per attempt.
    ///
    /// A lost connection that had been idle drops every idle one too. A new
    /// one drops nothing: it was lost before it went idle, so it says nothing
    /// about the others.
    fn run<T>(
        &self,
        op: impl FnOnce(&Client) -> Result<T, MemcacheError>,
    ) -> Result<T, BackendError> {
        let mut conn = self.checkout().map_err(memcached_err)?;
        // Only a client that came back from the idle list is prepared: a new
        // one, or one kept after a failed connect, is not.
        let was_idle = conn.ready;
        let result = self.prepare(&mut conn).and_then(|()| op(&conn.client));
        match &result {
            Ok(_) | Err(MemcacheError::PoolError(_)) => {
                self.lock_idle().push((conn, Instant::now()));
            }
            Err(e) => {
                drop(conn);
                if was_idle && connection_lost(e) {
                    let stale = std::mem::take(&mut *self.lock_idle());
                    drop(stale);
                }
            }
        }
        result.map_err(memcached_err)
    }

    /// The list is only pushed, popped and taken, so a poisoned lock still
    /// guards a consistent `Vec`.
    fn lock_idle(&self) -> MutexGuard<'_, Vec<(Conn, Instant)>> {
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
    /// operation — this budget bounds the caller. Unused under `unsync`.
    #[cfg_attr(feature = "unsync", allow(dead_code))]
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
    /// against; the socket timeouts still bound each I/O call), and needs no
    /// permit: it holds its client across no `.await`, so no other operation
    /// can run meanwhile.
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
    /// Accepts:
    ///
    /// - TCP: `memcache://host:port`, `tcp://host:port`, or bare `host:port`;
    /// - UDP: `memcache+udp://host:port`, or `memcache://host:port?udp=true`.
    ///   rust-memcache 0.19 never sends an ASCII `get` over UDP, so every
    ///   `get` and `exists` fails with a retryable error; writes work;
    /// - a Unix socket: `memcache:///path/to/socket` (no host, unix only);
    /// - TLS: `memcache+tls://host:port`, only when rust-memcache's `tls`
    ///   feature is enabled in the build. cachekit does not enable it, so
    ///   otherwise the URL fails to connect.
    ///
    /// Every connection carries the [`timeout`](Self::timeout), whatever
    /// the transport. Credentials in the URL (a `user:pass@` prefix on the host)
    /// are sent with memcached's ASCII `set auth` after the timeout is set, so
    /// a lost reply fails one operation. With an explicit `protocol=binary`,
    /// rust-memcache sends them itself, before any timeout exists on a UDP or
    /// Unix socket, so [`connect`](Self::connect) rejects credentials with
    /// `protocol=binary` on those transports (a config error). Binary over
    /// TCP or TLS is unaffected.
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

    /// Socket read/write timeout on every connection, for every URL form
    /// [`url`](Self::url) accepts (default 1s). A hung server, or a lost UDP
    /// reply, errors that operation and discards its connection instead of
    /// wedging the backend. A `timeout=<secs>` param in the URL overrides it.
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
    /// - `timeout=<secs>`: rust-memcache's `ConnectionManager` applies it on
    ///   every TCP (and TLS) connect. It ignores it for UDP and Unix sockets,
    ///   so the backend sets the same value on every connection itself (see
    ///   [`Connections::prepare`]).
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

    /// The URL's `timeout=` (pinned by `with_connection_params` unless the
    /// caller set one), parsed as rust-memcache parses it for TCP, so every
    /// transport gets the value TCP gets. `default` if it does not parse.
    fn socket_timeout(url: &Url, default: Duration) -> Duration {
        url.query_pairs()
            .find(|(name, _)| name == "timeout")
            .and_then(|(_, secs)| secs.parse::<f64>().ok())
            .and_then(|secs| Duration::try_from_secs_f64(secs).ok())
            .unwrap_or(default)
    }

    /// Move an ASCII-protocol URL's credentials out of the URL, so the
    /// backend authenticates after the socket timeouts are on (see
    /// `Connections::auth`). rust-memcache authenticates when the username
    /// is non-empty and a password is present, with both as written in the
    /// URL; this takes the same pair under the same condition.
    ///
    /// A binary-protocol URL keeps its credentials: its SASL handshake is
    /// not reachable through rust-memcache's public API. `connect` rejects
    /// that combination where it would run untimed (see
    /// [`authenticates_untimed`](Self::authenticates_untimed)).
    fn take_ascii_credentials(url: Url) -> (Url, Option<String>) {
        let is_ascii = url
            .query_pairs()
            .any(|(name, value)| name == "protocol" && value == "ascii");
        let Some(password) = url
            .password()
            .filter(|_| is_ascii && !url.username().is_empty())
        else {
            return (url, None);
        };
        let auth = format!("{} {password}", url.username());
        let mut bare = url.clone();
        if bare.set_password(None).is_err() || bare.set_username("").is_err() {
            return (url, None);
        }
        (bare, Some(auth))
    }

    /// Whether rust-memcache would authenticate on a socket that has no
    /// timeout yet: the URL still carries credentials (a binary-protocol URL,
    /// after [`take_ascii_credentials`](Self::take_ascii_credentials)) and
    /// its transport is UDP or a Unix socket, which rust-memcache opens
    /// without the URL's `timeout=`. The transport rules are rust-memcache's
    /// own, for the schemes `connect` allows.
    fn authenticates_untimed(url: &Url) -> bool {
        let has_credentials = !url.username().is_empty() && url.password().is_some();
        let udp = url.scheme() == "memcache+udp"
            || (url.scheme() == "memcache"
                && url
                    .query_pairs()
                    .any(|(name, value)| name == "udp" && value == "true"));
        let unix = cfg!(unix)
            && url.scheme() == "memcache"
            && url.host().is_none()
            && url.port().is_none();
        has_credentials && (udp || unix)
    }

    /// Connect to memcached and construct a [`MemcachedBackend`].
    ///
    /// Opens one connection and pings the server (`version`), so a bad
    /// address fails here rather than on the first cache operation. Later
    /// operations send no ping.
    ///
    /// # Errors
    ///
    /// Returns a config error if `url` was not provided or is invalid, or
    /// carries credentials with `protocol=binary` over UDP or a Unix socket
    /// (see [`url`](Self::url)), or a backend error if the connection cannot
    /// be established.
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

        let io_timeout = Self::socket_timeout(&url, timeout);
        let (url, auth) = Self::take_ascii_credentials(url);
        if Self::authenticates_untimed(&url) {
            return Err(CachekitError::Config(
                "memcached: a URL with credentials and protocol=binary is not supported \
                 over UDP or a Unix socket: rust-memcache authenticates before any socket \
                 timeout is set there, so one lost reply would wedge the connection pool. \
                 Use protocol=ascii (the default) or TCP"
                    .to_string(),
            ));
        }

        // Budget: a connect plus a few socket timeouts' worth of I/O covers
        // the healthy path with slack while still failing a wedged server
        // fast. It also bounds the wait for a free connection.
        let op_budget = timeout * 3 + connect_timeout;

        let backend = MemcachedBackend {
            conns: Arc::new(Connections::new(
                url,
                connect_timeout,
                io_timeout,
                auth,
                pool_size,
            )),
            max_item_size_bytes: self
                .max_item_size_bytes
                .unwrap_or(DEFAULT_MAX_ITEM_SIZE_BYTES),
            op_budget,
        };
        // Eager reachability check — connect() promises a live server. The
        // socket timeouts and the op budget bound it even against a
        // wedged-but-accepting server. The connection it opens is the first
        // idle one.
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
    fn idle_expiry_splits_the_old_prefix() {
        let now = Instant::now();
        let ago = |secs| now.checked_sub(Duration::from_secs(secs)).expect("uptime");
        let mut idle = vec![(1, ago(900)), (2, ago(601)), (3, ago(600)), (4, ago(5))];
        let expired = split_expired(&mut idle, now);
        assert_eq!(expired.iter().map(|e| e.0).collect::<Vec<_>>(), [1, 2, 3]);
        assert_eq!(idle.iter().map(|e| e.0).collect::<Vec<_>>(), [4]);
        assert!(split_expired(&mut idle, now).is_empty());
    }

    #[test]
    fn hot_client_does_not_keep_cold_clients_alive() {
        // A ten-connection burst, then sequential traffic: the top client is
        // fresh on every checkout, while the nine below it went idle 601 s
        // ago. Checkout must close those nine, not only look at the top one.
        // Building a Pool with min_idle 0 opens no socket, so no server runs.
        let url = Url::parse("memcache://127.0.0.1:1?protocol=ascii&timeout=1").expect("url");
        let conns = Connections::new(
            url,
            Duration::from_millis(100),
            Duration::from_secs(1),
            None,
            10,
        );
        let cold = Instant::now()
            .checked_sub(Duration::from_secs(601))
            .expect("uptime");
        for _ in 0..9 {
            let client = conns.open().expect("client");
            conns.lock_idle().push((client, cold));
        }
        let hot = conns.open().expect("client");
        conns.lock_idle().push((hot, Instant::now()));

        let client = conns.checkout().expect("checkout");
        assert!(
            conns.lock_idle().is_empty(),
            "the nine cold clients must be closed"
        );
        conns.lock_idle().push((client, Instant::now()));
        drop(conns.checkout().expect("checkout"));
        assert!(conns.lock_idle().is_empty());
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
    fn socket_timeout_is_the_urls_timeout_param() {
        let at = |url: &str| {
            let url = Url::parse(url).expect("url");
            MemcachedBackendBuilder::socket_timeout(&url, Duration::from_secs(1))
        };
        assert_eq!(
            at("memcache+udp://h:1?protocol=ascii&timeout=0.25"),
            Duration::from_millis(250)
        );
        assert_eq!(
            at("memcache:///tmp/m.sock?timeout=5"),
            Duration::from_secs(5)
        );
        // No param, or one that does not parse: the builder's value.
        assert_eq!(
            at("memcache://h:1?connect_timeout=5"),
            Duration::from_secs(1)
        );
        assert_eq!(at("memcache://h:1?timeout=soon"), Duration::from_secs(1));
        assert_eq!(at("memcache://h:1?timeout=-1"), Duration::from_secs(1));
    }

    #[test]
    fn ascii_credentials_move_out_of_the_url() {
        let take = |url: &str| {
            let (url, auth) =
                MemcachedBackendBuilder::take_ascii_credentials(Url::parse(url).expect("url"));
            (url.to_string(), auth)
        };
        assert_eq!(
            take("memcache+udp://u:p%20w@h:1?protocol=ascii&timeout=1"), // pragma: allowlist secret
            (
                "memcache+udp://h:1?protocol=ascii&timeout=1".to_string(),
                Some("u p%20w".to_string())
            )
        );
        // rust-memcache's condition: a non-empty username and a password.
        assert_eq!(take("memcache://u@h:1?protocol=ascii").1, None);
        assert_eq!(take("memcache://:p@h:1?protocol=ascii").1, None);
        // Binary: rust-memcache's SASL handshake keeps them.
        assert_eq!(
            take("memcache://u:p@h:1?protocol=binary"),
            ("memcache://u:p@h:1?protocol=binary".to_string(), None)
        );
    }

    #[test]
    fn untimed_authentication_is_binary_credentials_on_udp_or_unix() {
        let untimed = |url: &str| {
            MemcachedBackendBuilder::authenticates_untimed(&Url::parse(url).expect("url"))
        };
        assert!(untimed("memcache+udp://u:p@h:1?protocol=binary")); // pragma: allowlist secret
        assert!(untimed("memcache://u:p@h:1?protocol=binary&udp=true")); // pragma: allowlist secret
                                                                         // TCP and TLS sockets carry the URL's timeout from the connect on.
        assert!(!untimed("memcache://u:p@h:1?protocol=binary")); // pragma: allowlist secret
        assert!(!untimed("memcache+tls://u:p@h:1?protocol=binary")); // pragma: allowlist secret
                                                                     // No credentials (or ASCII ones, already taken out): nothing untimed.
        assert!(!untimed("memcache+udp://h:1?protocol=binary"));
        assert!(!untimed("memcache+udp://u@h:1?protocol=binary"));
        assert!(!untimed("memcache:///tmp/m.sock?protocol=binary"));
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
