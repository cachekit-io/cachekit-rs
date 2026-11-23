# cachekit-rs

[![Crates.io](https://img.shields.io/crates/v/cachekit-rs.svg)](https://crates.io/crates/cachekit-rs) [![docs.rs](https://docs.rs/cachekit-rs/badge.svg)](https://docs.rs/cachekit-rs) [![MSRV][msrv-badge]][msrv-release] [![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

<!-- The MSRV badge is reference-style so the badge line, which names cachekit-rs, carries no version number: crates/cachekit/tests/readme_pin_tests.rs fails a line that names cachekit-rs beside a version outside a release-please block. -->
[msrv-badge]: https://img.shields.io/badge/MSRV-1.91-blue.svg
[msrv-release]: https://blog.rust-lang.org/2025/10/30/Rust-1.91.0/

Caching for Rust — dual-layer L1/L2, zero-knowledge encryption, multi-backend.

> [!NOTE]
> **Status: beta** — CacheKit is in closed beta ahead of 1.0. APIs are stabilising; minor breaking changes may still occur between 0.x releases.

## Requirements

New releases support the latest stable Rust. This release requires **Rust 1.91** or later: `rust-version` in `Cargo.toml` is the oldest toolchain CI tests, and any release may raise it when a dependency needs a newer one.

## Installation

Install with `cargo add cachekit-rs` (defaults), `cargo add cachekit-rs --features redis`, or, for Cloudflare Workers, `cargo add cachekit-rs --no-default-features --features workers,encryption`. The equivalent `Cargo.toml` entries:

<!-- x-release-please-start-version -->
```toml
# Defaults: CacheKit Cloud (`cachekitio`) + encryption + L1 + reliability
[dependencies]
cachekit-rs = "0.14.0"

# With Redis backend
[dependencies]
cachekit-rs = { version = "0.14.0", features = ["redis"] }

# For Cloudflare Workers (no L1, no Redis)
[dependencies]
cachekit-rs = { version = "0.14.0", default-features = false, features = ["workers", "encryption"] }
```
<!-- x-release-please-end -->

### Feature flags

| Feature | Default | Description |
|:--------|:-------:|:------------|
| `cachekitio` | ✅ | CacheKit Cloud HTTP backend ([api.cachekit.io](https://api.cachekit.io)) via [reqwest](https://crates.io/crates/reqwest) + rustls |
| `encryption` | ✅ | Zero-knowledge AES-256-GCM via [cachekit-core](https://crates.io/crates/cachekit-core). Without it, every builder encryption call (`.encryption()`, `.encryption_with_previous()`, `.encryption_from_env()`, `.encryption_from_bytes()`, `.encryption_from_bytes_with_previous()`) returns a config error |
| `l1` | ✅ | In-process L1 cache via [quick_cache](https://crates.io/crates/quick_cache), with per-entry TTL and stale-while-revalidate (native). Not supported on `wasm32-unknown-unknown` (compile error: no clock there) |
| `reliability` | ✅ | Retry with backoff + jitter, circuit breaker, backpressure, distributed fill locks (native only) |
| `redis` | ❌ | Redis backend via [fred](https://crates.io/crates/fred) (native only) |
| `memcached` | ❌ | Memcached backend via [rust-memcache](https://crates.io/crates/memcache) (native only) |
| `file` | ❌ | Local filesystem backend, byte-compatible with cachekit-py's File backend (native only) |
| `workers` | ❌ | Cloudflare Workers backend via [worker](https://crates.io/crates/worker) |
| `macros` | ❌ | `#[cachekit]` proc-macro decorator (mints [interop/v1](#cross-sdk-interop-mode) keys) |
| `tracing` | ❌ | [`tracing`](https://crates.io/crates/tracing) events per cache operation and breaker transition — see [Observability](#observability) |

> [!WARNING]
> **Mutually exclusive features:**
> - `workers` + `redis` — Workers runtime cannot use fred
> - `workers` + `l1` — L1 reads `std::time::Instant`, which has no clock on wasm32-unknown-unknown, the Workers target
> - `workers` + `reliability` — retry/breaker timers need tokio `time`, unavailable in wasm32
> - `workers` + `memcached` — Workers runtime has no TCP sockets
> - `workers` + `file` — Workers runtime has no filesystem
>
> `l1` is also a compile error on `wasm32-unknown-unknown` with any feature set: `std::time::Instant` has no clock there, so the client's first L1 read or write would panic. Build for that target with `default-features = false, features = ["encryption"]` plus your backend feature (`workers` for CacheKit Cloud); `wasm32-wasip1` keeps L1.

## Quick start

The `production` [intent preset](#intent-presets) over Redis:

```rust
use cachekit::prelude::*;

#[tokio::main]
async fn main() -> Result<(), CachekitError> {
    // Needs the `redis` feature.
    let cache = CacheKit::production("redis://localhost:6379").await?
        .namespace("api")
        .build()?;

    cache.set("greeting", &"Hello, world!").await?;
    let val: Option<String> = cache.get("greeting").await?;
    println!("{val:?}");

    Ok(())
}
```

### From environment variables

```rust
use cachekit::prelude::*;

#[tokio::main]
async fn main() -> Result<(), CachekitError> {
    let cache = CacheKit::from_env()?.build()?;

    cache.set("greeting", &"Hello, world!").await?;
    let val: String = cache.get("greeting").await?.unwrap();
    println!("{val}");

    Ok(())
}
```

`from_env()` reads the [Environment Variables](#environment-variables) below. Its default TTL is 300 s, and no variable changes it: chain `.default_ttl(…)` before `.build()`, or pass a TTL per call with `set_with_ttl`.

With `CACHEKIT_MASTER_KEY` set, chain `.encryption_from_env()?` or `.no_encryption()` before `.build()`; see [Zero-Knowledge Encryption](#encryption).

### Builder API

```rust
use std::sync::Arc;
use std::time::Duration;
use cachekit::prelude::*;
use cachekit::backend::cachekitio::CachekitIO;

let backend = CachekitIO::builder()
    .api_key("ck_live_...")
    .build()?;

let cache = CacheKit::builder()
    .backend(Arc::new(backend))
    .default_ttl(Duration::from_secs(600))
    .namespace("myapp")
    .l1_capacity(5000)
    .build()?;
```

> [!IMPORTANT]
> Never hardcode API keys or master keys. Use environment variables or a secrets manager.

Each CacheKit Cloud request attempt times out after 5 s, the protocol's
`CACHEKIT_TIMEOUT` default; a write (`set`, `delete`) gets 10 s. The timeout
covers connect, TLS and the whole response body, and surfaces as
`BackendErrorKind::Timeout`. The same budgets apply on native targets and on
the [Workers backend](#cloudflare-workers). On native targets,
`CachekitIO::builder().read_timeout(..)` and `.write_timeout(..)` change them
(zero is rejected at `build()`); connecting still gives up after 5 s. Under the
[reliability stack](#reliability) the budget is per attempt.

On native targets every request carries the User-Agent `cachekit-rs/<version>`,
and an idle pooled connection is kept for 390 s (reqwest's default is 90 s).
Cloudflare closes an idle client connection after 400 s, so a request after a
90-390 s pause reuses the open connection instead of paying for a new DNS
lookup, TCP connect and TLS handshake. reqwest's 15 s TCP keepalive keeps NAT
mappings alive while the connection is idle. On Linux and macOS it also drops
a silently dead connection within about 60 s; Windows keeps its own fixed
probe count, so detection there takes about 165 s. On wasm32 the platform's
`fetch` decides pooling and the User-Agent.

Neither CacheKit Cloud client (`CachekitIO`, `WorkersCachekitIO`) follows a redirect: a 3xx from the API is a
permanent error. On wasm32, use the [Workers backend](#cloudflare-workers).

### Values and binary payloads

`get` and `set` take any serde type and store it as MessagePack. serde treats a plain `Vec<u8>` as a sequence, so it is stored as a MessagePack array of integers: up to twice the size, with a per-byte encode and decode. For bodies, blobs and other large byte payloads, mark the field with [`serde_bytes`](https://crates.io/crates/serde_bytes) (a crate you add yourself) so it is stored as one MessagePack `bin` value:

```rust
#[derive(serde::Serialize, serde::Deserialize)]
struct Response {
    status: u16,
    #[serde(with = "serde_bytes")]
    body: Vec<u8>,
}
```

For a bare byte value, store a `serde_bytes::ByteBuf`. Either form decodes into either type, so switching a field to `serde_bytes` still reads entries written before the switch.

`SecureCache` borrows the `CacheKit` it came from, so keep the client and call `secure_cache()` where you need the handle; it is cheap enough to call per operation.

## Features

`cachekit-rs` is the Rust SDK for [cachekit.io](https://cachekit.io). Pick an [intent preset](#intent-presets) — `minimal`, `production`, `secure`, or `io` — and get a pre-configured cache in one call, from bare Redis speed to dual-layer with client-side encryption. Pick `secure` and values never leave your process in plaintext; cache keys travel in clear.

| Component | What it does |
|:----------|:-------------|
| **CacheKit** | `get` / `set` / `delete` / `exists` with automatic L1 → L2 layering; with a master key configured, every value is AES-256-GCM encrypted before storage (zero-knowledge) |
| **SecureCache** | The same encryption as a handle (`cache.secure_cache()`) that refuses a client without a key |
| **Backend** | Pluggable trait — CacheKit Cloud, Redis, Memcached, local File, Cloudflare Workers |
| **L1 Cache** | In-process cache ([quick_cache](https://crates.io/crates/quick_cache)) with per-entry TTL, write-through + backfill |

> [!TIP]
> For the Python SDK with decorators, see [`cachekit`](https://github.com/cachekit-io/cachekit-py).
> For the low-level compression/encryption primitives, see [`cachekit-core`](https://crates.io/crates/cachekit-core).

## Intent presets

One call that names your use case. Each preset returns a pre-configured builder you can still override before `.build()`:

| Preset | When to use | Backend | L1 | Encryption | Reliability¹ | Auto-reconnect² | Default TTL |
|:-------|:------------|:--------|:--:|:----------:|:------------:|:---------------:|:-----------:|
| `CacheKit::minimal(url)` | Development, public data, product catalogs — speed first, no extras | Redis³ | ✅ (no SWR) | ❌ | ❌ | ❌ | 300 s |
| `CacheKit::production(url)` | User sessions, API responses, production services | Redis³ | ✅ | ❌ | ✅ | ✅ | 600 s |
| `CacheKit::secure(url, master_key_hex)`⁵ | PII, payments, GDPR/HIPAA-sensitive data — zero-knowledge AES-256-GCM | Redis³ | ✅ | ✅ | ✅ | ✅ | 600 s |
| `CacheKit::io(api_key)`⁴ | Serverless, edge compute, managed caching without running Redis | CacheKit Cloud | ✅ | ❌ | ✅ | n/a (HTTP) | 3 600 s |

¹ Retry with backoff + jitter, circuit breaker, backpressure — the [reliability stack](#reliability). Requires the default-on `reliability` feature.
² See the resilience contract below.
³ Requires the `redis` feature flag; `secure` also needs the default-on `encryption` feature.
⁴ Or `CacheKit::io_from_env()` to read `CACHEKIT_API_KEY`.
⁵ Or `CacheKit::secure_from_env(url)` to read `CACHEKIT_MASTER_KEY`, plus the decrypt-only rotation keys in `CACHEKIT_PREVIOUS_MASTER_KEYS` (see [Key Rotation](#key-rotation)). Both take the key as a hex string and decode it the same way every CacheKit SDK does, verified against the protocol's shared [`encryption.json`](crates/cachekit/tests/vectors/encryption.json) master key input vectors (vendored). Use exactly 32 bytes (64 hex chars, `openssl rand -hex 32`) — the only length every SDK accepts. Every value read and write on the client is encrypted — plain `get` / `set` included — and L1 holds ciphertext.

**Resilience contract** — connection failures, at construction and mid-run:

- `production` / `secure` **auto-reconnect**: a dropped connection is re-established with exponential backoff (100 ms → 30 s cap), retrying indefinitely. To get the same policy on a backend you build yourself, for example one `RedisBackend` shared by several clients through `CacheKitBuilder::backend`, call `RedisBackendBuilder::auto_reconnect()` (see [Redis](#redis)). A backend built without it does not reconnect, like `minimal`. While a connection is down, operations queue until the reconnect; `RedisBackendBuilder::disable_offline_queue()` fails them at once instead.
- `minimal` does **not reconnect**: a dropped connection is not re-established, and every later operation that reaches Redis errors until you rebuild the client: the first at once, each later one after the 5 s command timeout (see [Redis](#redis)). Reads served from a warm L1 entry still return without contacting Redis.
- **Initial** connections fail fast for every Redis preset: a bad URL or unreachable Redis errors immediately at construction, never enters a retry loop. `io` opens no connection at construction: an empty API key fails at construction, while an invalid key or unreachable endpoint surfaces at the first request.
- `secure` / `secure_from_env` validate the master key (and `secure_from_env` any previous keys) **before** any Redis connection is attempted — a missing, non-hex or short key, or a malformed `CACHEKIT_PREVIOUS_MASTER_KEYS`, is a deterministic local error, never masked by (or paying for) network I/O, and never a fallback to plaintext.
- Auto-reconnect is connection-level repair, distinct from the per-operation [reliability stack](#reliability) (retry, circuit breaker, backpressure) that `production` / `secure` / `io` also enable. `minimal` has neither — every failure is yours to handle.
- Every Redis command has a **5 s timeout** (matching cachekit-py and cachekit-ts), so a stalled or unreachable server returns `BackendErrorKind::Timeout` instead of hanging. `minimal` fails after ~5 s. `production` / `secure` retry a timeout up to 3 attempts, so one op can take ~15 s before it errors, and those timeouts count toward opening the circuit breaker. Under `production` / `secure`, a connection that stops answering is also closed and re-established, typically 6-8 s after the stalled command was sent (fred checks every 2 s for a pending reply older than 5 s). On a backend you build yourself, `RedisBackendBuilder::command_timeout` sets a different budget; it is per attempt, and it is also the stalled-reply threshold when `auto_reconnect()` is on.

## Configuration

### Environment variables

| Variable | Required | Description |
|:---------|:--------:|:------------|
| `CACHEKIT_API_KEY` | ✅ | API key for CacheKit Cloud (`from_env()` and `CacheKit::io_from_env()`) |
| `CACHEKIT_API_URL` | ❌ | Override API endpoint (default: `https://api.cachekit.io`) |
| `CACHEKIT_MASTER_KEY` | ❌ | Hex-encoded master key for encryption (`CacheKit::secure_from_env()` and the builder's `.encryption_from_env()`); use exactly 32 bytes (64 hex chars) — shorter, non-hex or non-UTF-8 values are rejected, never treated as unset. A key source, never a switch: `from_env()` validates it but does not encrypt, and while it is set a `from_env()` builder needs `.encryption_from_env()` or `.no_encryption()` |
| `CACHEKIT_TELEMETRY` | ❌ | `false` / `0` / `off` / `no` turns off the optional telemetry headers a CacheKit Cloud backend sends; read once when the backend is built. See [Telemetry sent to CacheKit Cloud](#telemetry-sent-to-cachekit-cloud) |
| `CACHEKIT_PREVIOUS_MASTER_KEYS` | ❌ | Comma-separated hex-encoded decrypt-only previous master keys for key rotation (`CacheKit::secure_from_env()` and `.encryption_from_env()`; max 3; a blank value is treated as unset). `from_env()` validates it and fails on a malformed value, even for a `.no_encryption()` client |

> [!CAUTION]
> `CACHEKIT_API_URL` must use HTTPS, must not carry credentials, a query or a
> fragment, and must not point to a private IP address. All of these are
> enforced at configuration time.

## Backends

### CacheKit Cloud (default feature)

HTTP backend targeting [api.cachekit.io](https://api.cachekit.io) with SSRF-safe URL validation, distributed locking, TTL inspection, and [telemetry](#telemetry-sent-to-cachekit-cloud) you can turn off.

```rust
use cachekit::backend::cachekitio::CachekitIO;

let backend = CachekitIO::builder()
    .api_key("ck_live_...")
    .api_url("https://api.cachekit.io")  // optional, this is the default
    .telemetry(false)                    // optional; on by default during the beta
    .build()?;
```

How fresh a CacheKit Cloud read is, and how long a deleted project's data stays readable: [Consistency and Deletion](https://docs.cachekit.io/concepts/consistency/).

### Redis

Native Redis via [fred](https://crates.io/crates/fred) with cluster support, TTL inspection, and distributed locking (`SET NX PX` acquire, atomic Lua compare-and-delete release, `<key>:lock` namespace shared with cachekit-py). With the `reliability` feature (on by default), on a single node (not a `redis-cluster://` URL), a `#[cachekit]` cold miss reads the key and takes its fill lock in one Lua script: `GET`, and only if the key is absent the same `SET <key>:lock <token> NX PX`, so a miss reaches the function after one round trip instead of two. The script is never sent twice, since a lost reply may mean it already took the lock: after a connection error or a timeout the cold miss re-reads the key with a plain `GET`, retried like any read, serves a value it finds, and on a miss runs the function without the lock. A cluster keeps the separate `GET` and `SET NX PX`, since one script cannot touch both keys there.

Known issue ([#220](https://github.com/cachekit-io/cachekit-rs/issues/220)): behind a proxy that routes `EVAL` by its first key (Envoy's Redis proxy, twemproxy), the script creates `<key>:lock` on the server that owns `<key>`. A cachekit-py process on the same key then takes its lock elsewhere, so the two SDKs stop excluding each other, and this SDK's unlock misses the lock, which lives until its 5 s expiry. A re-miss of the key inside those 5 s waits 5 s for a fill that never comes. The stall repeats: a waiter does not retry the lock while it polls, so a workload that invalidates then reads a key, or caches with a TTL of 5 s or less, alternates a fast fill with a 5 s stall. Workaround: `#[cachekit(distributed_lock = false)]` on the affected functions, which sends no lock call and keeps in-process dedup. Each command times out after 5 s with `BackendErrorKind::Timeout`; `.command_timeout(Duration)` changes that (zero is rejected at `build()`). A timed-out command may still run on the server: harmless for `get`/`set`/`delete`, and a timed-out lock acquire leaves the lock to expire on its own TTL. Requires the `redis` feature flag.

<!-- x-release-please-start-version -->
```toml
cachekit-rs = { version = "0.14.0", features = ["redis"] }
```
<!-- x-release-please-end -->

```rust
use std::time::Duration;
use cachekit::backend::redis::RedisBackend;

let backend = RedisBackend::builder()
    .url("redis://localhost:6379")
    .auto_reconnect()                            // optional: the production/secure policy
    .command_timeout(Duration::from_millis(500)) // optional: default 5 s
    .disable_offline_queue()                     // optional: fail at once while disconnected
    .build()?;
backend.connect().await?;  // explicit connect required
```

Without `.auto_reconnect()` a dropped connection is not re-established: after Redis restarts, every operation errors until you rebuild the backend.

**Commands sent while the connection is down.** By default fred queues them, with no limit and value included, including ones whose caller has given up: a `tokio::time::timeout` shorter than the command timeout ends the wait, not the command. With `.auto_reconnect()` the queue is sent after the reconnect. Without it, only the first operation after the drop errors at once; each later one waits out the command timeout and stays queued until the backend is dropped. `.disable_offline_queue()` fails each command at once instead, with `BackendErrorKind::Transient`, which the reliability stack retries and counts toward the circuit breaker.

With it, the backend queues none of its commands from the moment fred reports the drop until the connection is back, or, without `.auto_reconnect()`, ever after. "Back" means the server answered a PING the backend sent after the last drop fred reported. These PINGs pass the same guard as any command, so while fred is reconnecting they fail at once instead of queueing. While the connection is down, the backend sends one when fred reports a reconnect, and another one second after each unanswered one ends, until one is answered. A drop reported meanwhile marks the connection down at once, and the answer to a PING sent before it no longer counts. A command already written to the connection when fred notices the drop fails instead of being sent again after the reconnect; it may have run on the server. Lock release is the exception: a release already written when the connection drops can be sent again after the reconnect, which is harmless because it deletes the lock only while the lock still holds the caller's token. fred does not promise that resend: it skips a release whose caller waited out the command timeout, and can drop one during a long outage, and the lock then stays until its TTL. A release called while the connection is down is refused like any other command. Not covered: commands sent before fred reports the drop. A closed connection is noticed at once. One that stays open but stops answering is noticed only by `.auto_reconnect()`'s unresponsive check, up to 4 s after the command timeout. Most commands sent in that window fail when fred notices, but some can still be sent after the reconnect.

The backend follows fred's connection events, so with the option `connect` may be called once per backend (a second call returns `BackendErrorKind::Permanent`, even after a first call that failed; build a new backend to try again), and `build()` rejects the option on a Redis Cluster URL. The option is off by default and in every preset.

`connect` returns fred's `ConnectHandle`, the task driving the connection. Discarding it, as above and in the presets, leaves the connection and any auto-reconnect running; keep it only to await the error, if any, that finally closed the connection. On a backend with `.disable_offline_queue()` and no `.auto_reconnect()` it may never resolve, because no command reaches the closed connection; failed commands are the signal there.

### Memcached

Memcached via [rust-memcache](https://crates.io/crates/memcache) (single server, connection-pooled, per-socket timeouts — a hung server errors one operation instead of wedging the backend). Each operation is one round trip: a connection is reused only after a fully parsed response, and any error discards it, so a late reply can never answer a later request. A connection that died while idle fails the one operation that finds it, with a retryable error. Only when it was reset or closed (a server restart, or a NAT that resets an expired mapping) are the other idle connections dropped too. A NAT or firewall that drops an idle connection silently surfaces as a read timeout and drops no other idle connection, so each dead idle connection fails one operation after `timeout()`. Each pooled connection runs 3 parked r2d2 worker threads, up to 30 at the default `pool_size(10)`. Keys are validated against protocol metacharacters (whitespace/control bytes) before anything reaches the wire, keeping the key space identical to cachekit-py's.

**URL forms.** `url()` accepts TCP (`memcache://host:port`, `tcp://host:port` or bare `host:port`), UDP (`memcache+udp://host:port` or `memcache://host:port?udp=true`) and a Unix socket (`memcache:///path/to/socket`, no host). Every connection carries the `timeout()` socket read/write timeout, whatever the transport, so a lost UDP reply or a stalled Unix-socket server fails one operation instead of holding a connection forever. Credentials in the URL (`user:pass@`) are sent after that timeout is set; with an explicit `protocol=binary` rust-memcache would send them itself, before a UDP or Unix socket has a timeout, so `connect()` rejects credentials with `protocol=binary` on those two transports (a `CachekitError::Config`). Over UDP, rust-memcache 0.19 never sends an ASCII `get`, so every `get` and `exists` fails with a retryable error; writes work. `memcache+tls://host:port` connects only when rust-memcache's `tls` feature is enabled in the build; cachekit does not enable it.

**TTL capability, precisely:** memcached's protocol cannot *read* a key's remaining TTL, so this backend does not implement `TtlInspectable` — matching cachekit-py, where Memcached is likewise not TTL-inspectable. Both SDKs do ship a bare `refresh_ttl` (wrapping the memcached `touch` command) callable directly on the backend, outside the capability trait — so TTL-refresh works, but TTL-*driven* features that need to read TTLs never engage on memcached in any SDK.

TTLs above memcached's 30-day ceiling are clamped (larger values would be misread as absolute timestamps); values above the item-size limit (default 1 MiB) fail loudly client-side, and a server-side "object too large" classifies as permanent (never retried). Requires the `memcached` feature flag.

<!-- x-release-please-start-version -->
```toml
cachekit-rs = { version = "0.14.0", features = ["memcached"] }
```
<!-- x-release-please-end -->

```rust
use cachekit::backend::memcached::MemcachedBackend;

let backend = MemcachedBackend::builder()
    .url("tcp://localhost:11211")
    .connect()          // eager: verifies the server is reachable
    .await?;
```

### File (local filesystem)

Local disk cache, **byte-compatible with cachekit-py's File backend** — a py and an rs process pointed at the same directory read each other's entries (Blake2b-128 hashed filenames, shared 14-byte header, atomic write-then-rename, lazy expiry). An entry whose header carries a nonzero reserved byte or nonzero flags (a transform this reader does not implement) reads as a miss on every path and is never deleted or rewritten, even once its expiry has passed, as the protocol requires; an entry with an expiry is expired from that second on. Both header read paths (the one behind `get`, and the header-only probe behind `exists` and `ttl`) and the writer are verified against the protocol's shared [`file-backend.json`](crates/cachekit/tests/vectors/file-backend.json) vectors (vendored). Implements `TtlInspectable` (TTL read off the on-disk header, in-place refresh). Same-process operations on one key serialize on a striped per-key lock (64 stripes), so a read waits on a write to an unrelated key only when the two share a stripe (about 1 in 64); py serializes all operations on one `RLock`. Cross-process behaviour matches py: on unix, reads and in-place TTL rewrites take advisory `flock` while writes stay lock-free via atomic rename; and expired-entry unlinks are inode-validated so a stale read decision doesn't delete a concurrent writer's fresh entry. On unix the cache directory must be owned by you and not group/other-writable. Not yet ported from py: LRU eviction and size caps — the directory grows until entries expire or you clear it. Requires the `file` feature flag and a tokio runtime (I/O runs via `spawn_blocking`).

<!-- x-release-please-start-version -->
```toml
cachekit-rs = { version = "0.14.0", features = ["file"] }
```
<!-- x-release-please-end -->

```rust
use cachekit::backend::file::FileBackend;

let backend = FileBackend::builder()
    .cache_dir("/var/cache/myapp")  // default: <system temp dir>/cachekit
    .build()?;
```

### Cloudflare Workers

`wasm32-unknown-unknown` backend using `worker::Fetch`, with distributed locking and TTL inspection against the CacheKit Cloud lock/TTL endpoints. It is the `CachekitIO` backend on wasm32. Requires the `workers` feature with default features disabled.

The Workers runtime's `fetch` has no timeout of its own and can hold a request for tens of seconds, so the backend bounds each request itself: 5 s for a read and 10 s for a write (`set`, `delete`), from send through the last byte of the response body. Past that the request is aborted and the call fails with `BackendErrorKind::Timeout`. The `reliability` feature is native-only, so a Workers call makes one attempt and does not retry.

`WorkersCachekitIO::builder(&env)` takes the Worker's bindings (the `env` your fetch handler receives): the backend reads a `CACHEKIT_TELEMETRY` variable or secret there, and in `process.env` (with the `nodejs_compat` flag), to turn [telemetry](#telemetry-sent-to-cachekit-cloud) off.

<!-- x-release-please-start-version -->
```toml
cachekit-rs = { version = "0.14.0", default-features = false, features = ["workers", "encryption"] }
```
<!-- x-release-please-end -->

<details>
<summary><strong>Custom Backend</strong></summary>

Implement the `Backend` trait to plug in any storage:

```rust
use async_trait::async_trait;
use cachekit::backend::{Backend, HealthStatus};
use cachekit::error::BackendError;
use std::time::Duration;

struct MyBackend;

#[async_trait]
impl Backend for MyBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError> { todo!() }
    async fn set(&self, key: &str, value: Vec<u8>, ttl: Option<Duration>) -> Result<(), BackendError> { todo!() }
    async fn delete(&self, key: &str) -> Result<bool, BackendError> { todo!() }
    async fn exists(&self, key: &str) -> Result<bool, BackendError> { todo!() }
    async fn health(&self) -> Result<HealthStatus, BackendError> { todo!() }
}
```

Optional extension traits: `TtlInspectable` (TTL queries), `LockableBackend` (distributed locking). A backend that wraps the `CachekitIO` backend and passes its requests through should forward `attach_metrics` and `l1_latency` to it, so its [telemetry](#telemetry-sent-to-cachekit-cloud) keeps working.

The client writes through `async fn set_bytes(&self, key: &str, value: bytes::Bytes, ttl: Option<Duration>)`, so the L1 and every retry attempt share one buffer. Its default turns the buffer into a `Vec<u8>` (a copy while another handle shares it) and calls `set`. Override it (with `bytes = "1"` as a dependency) if your backend can store a `Bytes` without copying, as the `CachekitIO` backend does with its request body. An override must store exactly what `set` would.

</details>

## Dual-layer caching

When the `l1` feature is enabled (default), CacheKit keeps an in-process cache in front of the backend. [quick_cache](https://crates.io/crates/quick_cache) stores the entries and chooses what to evict; CacheKit adds per-entry TTL and stale-while-revalidate on top:

```
┌─────────────────────────────────────────────────────────┐
│                     CacheKit Client                     │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  GET path:                                              │
│  L1 fresh hit ──► return immediately                    │
│  L1 stale hit ──► return + background refresh (SWR)     │
│  L1 miss ──► L2 backend ──► backfill L1 if fresh (≤30s) │
│                                                         │
│  SET path:                                              │
│  write to L2 backend ──► write-through to L1            │
│                                                         │
│  DELETE path:                                           │
│  invalidate L1 first ──► delete from L2 backend         │
│                                                         │
├─────────────┬───────────────────────────────────────────┤
│  L1         │  L2 (CacheKit Cloud / Redis / Workers)    │
│  in-process │  one backend round trip                   │
└─────────────┴───────────────────────────────────────────┘
```

| Behavior | Detail |
|:---------|:-------|
| **Write-through** | `set()` writes to L2 first, then to L1 once L2 accepted the write. The L1 copy lives no longer than the write's TTL. On CacheKit Cloud, an `X-CacheKit-Fresh-For` on the `PUT` response (the store applied a shorter TTL, as a plan maximum does) bounds it further. Both bounds run from when the `PUT` response arrives, not from when the copy enters L1, so a delay in between (a slow backend wrapper) shortens the copy; a malformed value or `0` keeps no L1 copy at all, so the next read goes to the server. A `refresh_ttl()` (`PATCH /ttl`) never lengthens an L1 copy |
| **Backfill on miss** | L2 hits populate L1 for at most 30 s. On CacheKit Cloud a shorter `X-CacheKit-Fresh-For` bounds it further, and a hit labelled stale or with `Fresh-For: 0` is not backfilled at all, so the next read goes back to the server |
| **Invalidate-first** | `delete()` evicts L1 before touching L2 |
| **Encrypted L1** | A client with encryption configured stores ciphertext in L1 (never plaintext) |
| **Evict on decrypt failure** | A read on an encrypted client that fails decryption drops the key's L1 copy and returns the error; the backend entry is kept, so the next read reaches the backend: a hit once the entry is replaced with ciphertext the client can decrypt, a miss once it expires or is deleted |
| **Capacity** | 1,000 entries by default (`.l1_capacity()` sets it); L1 never holds more. It is split into shards: capacity ÷ 128 rounded down to a power of two, from 1 to 64 (4 at the default). A full shard evicts even while others have room |
| **Eviction** | Scan-resistant (quick_cache's CLOCK-Pro variant, close to S3-FIFO): a run of keys written once and never read does not flush the entries being read, as it would under LRU. `exists()` does not count as a read |
| **Expiry** | An entry is never served past its TTL. An expired entry leaves memory when a read finds it, when eviction reaches it, or when `l1_entry_count()` sweeps; until then it holds its place, so an idle L1 can keep up to its capacity in expired entries |
| **Live counters** | `cache.stats()` reports L1 hits / L2 hits / misses; `cache.l1_entry_count()` the current occupancy — see [Observability](#observability) |
| **Stale-while-revalidate** | On by default (native; `minimal` turns it off): `#[cachekit]` serves an L1 hit past `swr_threshold_ratio` × entry TTL (default 0.5, ±10% jitter) immediately and refreshes it in the background — see below |

### Stale-while-revalidate (SWR)

With SWR (default when `l1` is on, native targets), an L1 entry has two phases
before it disappears: *fresh* until `swr_threshold_ratio` of its TTL has
elapsed, then *stale* until hard expiry. A `#[cachekit]`-wrapped call that
hits a stale entry returns it **immediately** — no caller ever blocks on a
merely-stale value — while at most one background task re-executes the
function. If the same-key mutation token is still current, the task rewrites
both cache layers and renews L1 hard expiry with the full write-path TTL; if a
newer `set()` or `delete()` landed through the same client (or one of its
clones) while the origin ran, that explicit mutation wins and the older
refresh result is discarded before it can touch L2. Entry expiry and capacity
eviction do not invalidate the token, so a valid slow refresh can still
repopulate both layers. Refresh dedup takes the same locks as the cold-miss single-flight
(in-process, plus distributed fill locks on lock-capable backends), so N
concurrent stale readers cost one origin execution — misses are billable;
stampedes are not acceptable. Unlike a cold miss, a refresh never waits for
another worker's fill: if another worker or process already holds the key's
lock, or the lock call fails, the refresh stands down at once without polling or
re-reading the cache, records no miss, and the stale value keeps being served
until hard expiry. A hard-expired entry always takes the normal blocking miss
path: SWR never serves past hard expiry.

```rust,ignore
let cache = CacheKit::builder()
    .backend(backend)
    .swr_threshold_ratio(0.25) // stale after 25% of entry TTL (default 0.5)
    // .swr_enabled(false)     // restore strict expire-or-serve behaviour
    .build()?;
```

Semantics mirror cachekit-py (`swr_threshold_ratio` = elapsed-lifetime
fraction; enabled by default) and cachekit-ts (`getWithSwr`). Worth knowing:

- **The freshness window derives from each entry's own TTL.** A backfilled
  entry (L2 hit → L1, 30 s cap) goes stale at ~`ratio × 30 s` — the cap still
  bounds staleness of L2-derived data, but SWR replaces its expiry cliff with
  a background refresh that restores the full write-path TTL. The configured
  ratio is never silently clamped; the window follows the entry.
- **The server's freshness bound is a hard limit.** When CacheKit Cloud sends a
  shorter `X-CacheKit-Fresh-For`, the backfilled entry's TTL is that bound, so
  SWR goes stale at ~`ratio × Fresh-For` and never serves the copy past it.
  The same holds for a write: a `PUT` response's `Fresh-For` bounds the
  writer's own L1 copy.
  Stale-labelled reads are never backfilled, so there is nothing local to
  serve; the server serves its own stale window.
- **Refresh completion is version-guarded and same-key ordered.** Each L1
  stale read receives a mutation token. A concurrent explicit write or delete
  through that client or a clone invalidates the token, so an older origin
  result cannot clobber the new value or resurrect the deletion in either
  layer. The guard is intentionally process-local; cross-instance invalidation
  is outside SWR's serving-policy scope.
- **Jitter is fixed per entry.** The ±10% threshold jitter is drawn when an
  entry is inserted, not on every hit, from a per-thread generator seeded once
  from OS entropy, so neither hot L1 reads nor inserts make an entropy syscall,
  and the entry's freshness boundary stays stable for its lifetime.
- **The background refresh needs a tokio runtime** (`Handle::try_current`).
  On other executors the stale value is still served and the refresh is
  skipped — behaviourally SWR-off, never a panic.
- **Refresh failures are absorbed**: the stale value keeps serving, a later
  stale read retries, and once the entry hard-expires the blocking path
  surfaces errors normally.
- **Native only**: on wasm32 (`l1` is refused on `wasm32-unknown-unknown`) and under `unsync`
  there is no SWR; the builder knobs don't exist there, so misuse is a
  compile error rather than a silent no-op. A sync function under
  `#[cachekit]` is likewise a clear compile-time error.

## Encryption

Configure a master key — the `secure` preset, or `.encryption()` / `.encryption_with_previous()` / `.encryption_from_env()` / `.encryption_from_bytes()` / `.encryption_from_bytes_with_previous()` on any builder — and every value the client reads or writes is encrypted client-side with AES-256-GCM before it reaches any cache layer. `get`, `set`, `set_with_ttl`, `interop_get`, `interop_get_swr` and `#[cachekit]` functions all encrypt; the backend and L1 only ever see ciphertext. `delete` and `exists` carry no value.

```rust
// Env: CACHEKIT_MASTER_KEY=<64 hex chars>
let cache = CacheKit::secure_from_env("redis://localhost:6379").await?.build()?;

// Encrypt → store (backend and L1 see only ciphertext)
cache.set("user:42:ssn", &"123-45-6789").await?;

// Retrieve → decrypt (transparent to caller)
let ssn: String = cache.get("user:42:ssn").await?.unwrap();

// Same encryption as a handle that errors on a client without a key — for
// code that must never run unencrypted.
let secure = cache.secure_cache()?;
```

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Your Code   │────>│   CacheKit   │────>│   Backend    │
│              │     │  AES-256-GCM │     │  (CacheKit   │
│  plaintext   │     │  encrypt /   │     │  Cloud or    │
│              │     │              │     │  Redis)      │
│              │<────│  decrypt     │<────│              │
└──────────────┘     └──────────────┘     └──────────────┘
                      L1 stores ciphertext
                      (zero-knowledge preserved)
```

`CACHEKIT_MASTER_KEY` is a key source, never a switch: no constructor encrypts because it is set. Chain `.encryption_from_env()?` to encrypt with it (and with `CACHEKIT_PREVIOUS_MASTER_KEYS`), for tenant `"default"`, or `.no_encryption()` to state a plaintext client.

> [!WARNING]
> Earlier releases encrypted on `CacheKit::from_env()` whenever `CACHEKIT_MASTER_KEY` was set. A `from_env()` builder that sees the key now fails at `build()` until you state the intent, so the upgrade cannot silently start writing plaintext. To keep encrypting, change `CacheKit::from_env()?` to `CacheKit::from_env()?.encryption_from_env()?`: the keys and tenant are unchanged, so existing entries stay readable. To stop encrypting, use `.no_encryption()`, and delete the old entries first: a client that does not encrypt cannot read them. `get` returns an error, a `#[cachekit]` function recomputes the value and overwrites the entry in plaintext, and a rare entry decodes into a wrong value. This check is transitional and goes away in a later release, after which a `from_env()` builder that states neither builds a plaintext client.

> [!WARNING]
> Earlier releases encrypted only through `secure_cache()`: plain `get` / `set` on a client with a key configured read and wrote plaintext. Reading such an entry now returns an `Encryption` error until it is overwritten, deleted or expires, so delete (or let expire) whatever those calls wrote before you upgrade.

> [!WARNING]
> Earlier releases left the `.namespace()` prefix out of the AAD. On a client with both encryption and `.namespace()` configured, reading an entry an earlier release wrote returns an `Encryption` error, never a miss, on every read until it is overwritten, deleted or expires. Before you upgrade, delete those entries or move to a fresh namespace; waiting out the TTL is not a migration. There is no fallback to the old AAD, because one would keep accepting ciphertext moved between namespaces. Clients without a namespace are unaffected.

<details>
<summary><strong>Security Properties</strong></summary>

| Property | Implementation |
|:---------|:---------------|
| **Encryption** | AES-256-GCM (AEAD) via [cachekit-core](https://crates.io/crates/cachekit-core) (`ring` on native, `aes-gcm` on wasm32) |
| **Key Derivation** | HKDF-SHA256 — per-tenant cryptographic isolation |
| **AAD Binding** | The key the client passes to its backend, `.namespace()` prefix included, bound to ciphertext (prevents substitution between keys and namespaces) |
| **Memory Safety** | [zeroize](https://crates.io/crates/zeroize) on drop for all key material |
| **L1 Guarantee** | L1 stores ciphertext, never plaintext |
| **Cache-key path encoding (CWE-22)** | Keys are percent-encoded into the `CachekitIO` request path; the empty key, and a key encoding to a reserved segment (`.`, `..`, `health`, `ttl`, `lock`), are **rejected** rather than sent |

**Cache-key path encoding (CWE-22):** `CachekitIO` keys are percent-encoded (`urlencoding::encode`) so a key can only ever address `/v1/cache/{key}`. The empty key, and a key whose encoded form is one of the five reserved path segments — `.`, `..`, `health`, `ttl`, `lock` — are **rejected** with a permanent error rather than sent (protocol `spec/saas-api.md` § Cache-Key Path Encoding, rule 2). The empty key encodes to an empty segment, so `/v1/cache/{key}` becomes `/v1/cache/` and `/v1/cache/{key}/ttl` becomes `/v1/cache//ttl`, neither of which addresses a stored entry. `.`/`..` are dot segments that `reqwest`'s WHATWG URL parser (rust-url) strips *before the request leaves the process* (`/v1/cache/..` → `/v1/`); `health`/`ttl`/`lock` are live route tokens (`/v1/cache/health` is the health endpoint, a trailing `ttl`/`lock` selects a sub-resource). Encoding can't neutralise either — WHATWG collapses `%2E%2E` too — so the SDK refuses rather than emit a request whose path was rewritten. This matches the cachekit-ts twin and is stricter than cachekit-py's older `%2E` rewrite; none of these is ever a canonical CacheKit key (those are non-empty and contain `:`), so nothing legitimate is affected. Every other key encodes to the same bytes as cachekit-py; cachekit-ts may leave `! * ' ( )` raw, and every conformant form decodes once to the same key (spec rule 4).

**AAD v0x03 wire format:**

```text
[version(0x03)][len(4)][tenant_id][len(4)][cache_key][len(4)][format][len(4)][compressed]
```

Each field is length-prefixed with a 4-byte big-endian u32 to prevent boundary-confusion attacks.
Cross-SDK compatible — ciphertext produced by the Python SDK decrypts with the Rust SDK and vice versa.

</details>

**Is AES hardware-accelerated on this host?** `cache.secure_cache()?.hardware_acceleration_enabled()` (also on `EncryptionLayer`) forwards cachekit-core's detection. Informational only: `ring`/`aes-gcm` pick their implementation independently, so use it to explain secure-cache latency, not to change behaviour. The per-architecture semantics are core's: a runtime AES-NI probe on x86/x86_64, a runtime probe for the Armv8 Crypto Extension's AES instructions on aarch64 (a Raspberry Pi 4, a Cortex-A72 without the Crypto Extension, reports `false`), and `false` on wasm32.

### Key rotation

Rotate the master key without invalidating existing entries: promote the new key to current and keep the old one as a decrypt-only previous key during a grace window (max 3, per the [protocol keyring spec](https://github.com/cachekit-io/protocol/blob/main/spec/encryption.md)). Writes always use the current key; reads attempt the current key first, then each previous key in order. Old entries age out via TTL or re-encrypt on the next write — no bulk re-encryption.

```rust
// Env: CACHEKIT_MASTER_KEY=<k2-hex> CACHEKIT_PREVIOUS_MASTER_KEYS=<k1-hex>
let cache = CacheKit::from_env()?.encryption_from_env()?.build()?;

// The Redis `secure` preset reads the same two variables:
let cache = CacheKit::secure_from_env("redis://localhost:6379").await?.build()?;

// Or explicitly on the client builder, for any tenant, with hex keys
// (each decodes to at least 32 bytes, as for `.encryption()`):
let cache = CacheKit::builder()
    .backend(backend.clone())
    .encryption_with_previous(&k2_hex, &[&k1_hex], "tenant")?
    .build()?;

// Or with exactly 32 raw bytes per key
// (decoded, never the ASCII of a hex string; any other length is an error):
let cache = CacheKit::builder()
    .backend(backend)
    .encryption_from_bytes_with_previous(&k2_bytes, &[&k1_bytes], "tenant")?
    .build()?;
```

Use exactly 32-byte keys (64 hex chars), the only length every SDK accepts. A longer key that `.encryption(hex, tenant)` accepted can still be retired: list it as a previous key in `.encryption_with_previous()` for the same tenant. The env variables always derive for tenant `"default"`, and the raw-bytes method takes only 32-byte keys.

Rotation is forward-only: a retired key is never re-promoted (re-promoting would resume a used AES-GCM nonce budget), and a config listing the current key among the previous keys is rejected at load. For the three-phase zero-miss rollout and compromise response, see the [key rotation runbook](https://docs.cachekit.io/concepts/key-rotation/).

**Knowing when to drop the old key.** Every read served by a previous key is counted against that key's position; `cache.secure_cache()?.previous_key_hits()` returns the counts (`hits[i]` for `previous_keys[i]`, current-key reads not counted, no key material). The signal confirms a grace window has drained; it does not shorten one. Follow the protocol's [scheduled-rotation runbook](https://github.com/cachekit-io/protocol/blob/main/decisions/key-rotation.md#runbooks-normative-for-docs): audit for non-expiring entries, add the incoming key as decrypt-only fleet-wide, then promote it. The clock starts only when the promotion deploy has completed on every instance — a lagging instance still writes under the retiring key and reads it silently as *its* current key. From then, wait at least the longest TTL in use (including any explicit `set_with_ttl` values), aggregating counts across every instance (they are per process and reset on restart). Once the retiring key's count has stayed flat over that whole window, every live entry has aged out or been re-encrypted on write, and the key can be dropped from `CACHEKIT_PREVIOUS_MASTER_KEYS` without a hard cut-over.

## Cross-SDK interop mode

Interop mode ([interop/v1](https://github.com/cachekit-io/protocol/blob/main/spec/interop-mode.md)) lets the Python, TypeScript, and Rust SDKs share cache entries: keys are `{namespace}:{operation}:{args_hash}` with an explicit operation name (no language-specific function path), and values are plain MessagePack — no envelope, readable by any MessagePack library.

The cross-SDK rules — the opt-in for each SDK, what composes with interop, and the shared-entry contract — are in the [Using Interop Mode](https://docs.cachekit.io/concepts/using-interop-mode/) guide on docs.cachekit.io, with the [interop/v1 spec](https://github.com/cachekit-io/protocol/blob/main/spec/interop-mode.md) as the normative reference. Both win over this README on any conflict.

```rust
use cachekit::interop::{interop_key, InteropValue};

// Every SDK computes this exact key for get_user(42)
let key = interop_key("users", "get_user", &[InteropValue::from(42i64)])?;

cache.set_with_ttl(&key, &user, ttl).await?;          // plain MessagePack — already interop
let user: Option<User> = cache.interop_get(&key).await?; // strict read: exactly one document
```

`ns` and `nsapi` are reserved as namespaces — the CacheKit Cloud server parses a key starting `ns:` or `nsapi:` as namespace-prefixed — so `interop_key` rejects them with `InvalidKey` and `#[cachekit(namespace = ...)]` with a compile error; operations are unaffected. Neither segment may contain `..` (`a..b` is rejected the same two ways; a lone `.`, as in `app.v1`, is fine), because the server rejects `..` anywhere in a key. On a `#[cachekit]` function, a parameter whose name starts with `__ck` or `__CK` is a compile error too: that prefix is reserved for the macro's generated locals, which would otherwise shadow the argument.

Argument hashing is byte-identical across SDKs (canonical MessagePack + Blake2b-256), verified against the shared [protocol](https://github.com/cachekit-io/protocol) test vectors ([`interop-mode.json`](crates/cachekit/tests/vectors/interop-mode.json), vendored) in this repo's test suite. `get` and `interop_get` (also on `SecureCache`) both read exactly one MessagePack document and reject trailing bytes, so neither silently misreads a Python-internal CK frame as the integer 67; `interop_get` names the CK frame in its error. On a client without encryption, `get`, `interop_get` and `interop_get_swr` refuse every CK frame in the protocol's shared [`python-frame.json`](crates/cachekit/tests/vectors/python-frame.json) vectors (vendored); an encrypted client refuses them at decrypt. A bare ByteStorage envelope (another SDK's auto-mode container) is one well-formed MessagePack document, so no structural check can refuse it: on a client without encryption, a read of one fails only when its shape does not fit the requested type, and rmp-serde decodes by position, so a type whose four elements line up with the envelope's (bytes, 8 integers, an integer, a string) does read one. An encrypting client fails at decrypt, whatever the type. Every decode of backend-supplied bytes (`get` and `interop_get` alike) first passes a header-only structural walk that rejects, before anything is decoded, a document nested deeper than `serializer::MAX_DECODE_DEPTH` (100 levels, matching the TypeScript SDK; every collection header counts, empty ones included) or declaring more elements or bytes than the input can back, verified against the protocol's shared [`decode-bounds.json`](crates/cachekit/tests/vectors/decode-bounds.json) vectors (vendored) — a forged nested-header entry is a bounded `Serialization` error, not a memory blow-up or a stack overflow. A client without encryption walks the bytes when a backend read or its own write admits them, and a later L1 hit on bytes that passed decodes without walking again. An encrypting client's L1 holds ciphertext: every read decrypts it, and the first L1 hit whose decrypted plaintext passes the walk marks the entry, so later hits on that ciphertext decode without walking again. A rewrite, delete or eviction drops the mark with the entry, and a hostile plaintext under a valid tag is never marked. Encryption works unchanged — interop keys are identical across SDKs, so the AAD verifies cross-SDK. On a client with encryption configured, `set_with_ttl` stores the AES-GCM ciphertext of that MessagePack and `interop_get` decrypts it.

> [!WARNING]
> Every service that binds one `(namespace, operation)` must agree on encryption: on in all of them or off in all of them, with the same master key and `tenant_id` (decrypt-only previous master keys may differ). If an unencrypted service shares the operation with an encrypted one, values end up **stored unencrypted** at the shared key: the unencrypted service writes in the clear whenever it fills the entry, and a `#[cachekit]` function on a client without encryption also replaces ciphertext it cannot decode. See [Shared entries are a contract](https://docs.cachekit.io/concepts/using-interop-mode/#shared-entries-are-a-contract).

> [!IMPORTANT]
> Use interop keys on a client **without** `.namespace()` — a client prefix would rewrite the storage key to `{prefix}:{interop_key}`, which no other SDK computes. `interop_get` fails closed with a config error rather than silently missing; interop keys already carry their own namespace segment. `set` / `set_with_ttl` do **not** fail on a namespaced client: they return `Ok` and write the entry under `{prefix}:{interop_key}`, a key no other SDK reads.

Interop mode in production: [Skyline](https://github.com/cachekit-io/bluesky-thinking) — the canonical example project — runs this SDK on `wasm32` (Cloudflare Workers) deriving interop keys and verifying payload integrity for the namespace the Python and TypeScript SDKs share (public aggregate reads stay on the TypeScript edge — see the [example page](https://docs.cachekit.io/examples/skyline/)).

## Reliability

With the `reliability` feature (default, native only), the `production`, `secure`, and `io` presets wrap every backend operation in a reliability stack; `minimal` stays bare for maximum throughput:

| Layer | What it does | Defaults |
|:------|:-------------|:---------|
| **Retry** | Truncated exponential backoff + jitter on transient/timeout errors (`BackendErrorKind::is_retryable`); permanent and auth errors propagate immediately. Under `io`, all attempts of one op share a deadline: an attempt still running at it is cancelled with `BackendErrorKind::Timeout`, and no retry starts past it, so a CacheKit Cloud request that stops answering costs one attempt, not three. Other presets and custom backends get no deadline. A CacheKit Cloud `429` for a spent quota or balance (`X-CacheKit-Deny-Reason`) is not retried; it stays `Transient` and counts toward the breaker, so a plain `#[cachekit]` function fails open while a direct client call returns the error and `#[cachekit(secure)]` fails closed | 3 attempts, 100 ms base, 5 s cap, jitter ×[0.5, 1.5); `io` deadline 5 s per read, 10 s per write |
| **Circuit breaker** | closed → open after N retryable failures in a rolling window; fails fast (`BackendErrorKind::CircuitOpen`) while open; half-open probes recovery | threshold 5, window 60 s, open 5 s, 3 probes, close after 3 successes |
| **Backpressure** | Bounds concurrent backend data ops with a semaphore + bounded waiting queue; over-limit calls are shed with `BackendErrorKind::Backpressure` before reaching the backend — a slow backend can't exhaust the caller's connection pool or memory | 100 concurrent, 1 000 queued, 100 ms wait (Python SDK parity) |
| **Graceful degradation** | On outage-class backend failure (transient, timeout, open breaker, backpressure shed), `#[cachekit]`-wrapped functions run uncached (fail-open); permanent/auth errors propagate — a wrong API key fails loudly. `#[cachekit(secure)]` paths fail **closed** on everything — encrypted workloads never silently degrade | built into the macro |
| **Single-flight** | Concurrent misses of one key collapse to a single execution: per-key in-process lock, plus a distributed fill lock across processes on lock-capable backends (CacheKit Cloud, Redis). On single-node Redis the cold miss's first read carries the lock attempt (see [Redis](#redis)); under `reliability` that combined call passes the backpressure limiter and circuit breaker like a read but is never retried, since a retry after a lost reply would find its own lock and report it contested. A combined call that times out or loses its connection counts no breaker failure: the cold miss re-reads the key with a plain, retried `GET`, which is the breaker's sample, serves a value it finds, and on a miss leads without the lock. One the server refuses (a full or read-only server, scripting denied) falls back to the separate read and lock call. One the stack sheds or fails fast fails the read as a plain read would, and the lock call follows. `#[cachekit(..., distributed_lock = false)]` drops that lock for one function's cold misses: no lock or unlock round trip, in-process dedup kept, and processes that miss one key together may each run the function. Its SWR background refresh still sends the lock and unlock | in-process always on; cross-process on by default (`distributed_lock = true`); cross-process 5 s lock, 100 ms polls, and a contested waiter computes once 5 s have passed since its own lock attempt; with `l1` on a tokio runtime (never under `unsync`), a stored fill's unlock is sent without the caller waiting for it; the client's next lock attempt on that key re-sends it if the backend has not answered, and if the caller's runtime is dropped or idle first, the lease can stay held for up to its 5 s timeout |
| **Stale-while-revalidate** | Stale-but-unexpired L1 hits are served immediately while one single-flight-deduplicated background task re-executes the function ([details](#stale-while-revalidate-swr)) | on by default with `l1` (native); threshold 0.5 × entry TTL ±10% jitter |

Retry sits *inside* the breaker (one exhausted retry sequence = one breaker failure) and backpressure sits *outside* both — one permit per logical operation, held across the whole retry sequence, so retry amplification is bounded and shed calls never skew breaker state. Degradation, single-flight, and SWR sit in the `#[cachekit]` macro around the read path — the same composition as the TypeScript SDK's `ReliabilityExecutor` and the Python decorator.

To give operations a tighter budget than the 5 s default, set it on the backend builder (`RedisBackendBuilder::command_timeout`, `CachekitIOBuilder::read_timeout` / `write_timeout`), not with `tokio::time::timeout` around each call. The backend's timeout is an error the stack sees: it is retried, each attempt gets the full budget (one op can take about three budgets plus backoff), and an op that ends in a timeout counts as one breaker failure. A call cancelled from outside records nothing, so a backend slower than your budget would never open the breaker and every op would wait out your full timeout.

```rust,ignore
use std::time::Duration;
use cachekit::{CacheKit, ReliabilityConfig, RetryConfig};

// Presets enable it — override or disable per client:
let cache = CacheKit::production("redis://localhost:6379").await?
    .reliability(ReliabilityConfig {
        retry: Some(RetryConfig { max_attempts: 5, ..RetryConfig::default() }),
        ..ReliabilityConfig::default()
    })
    .build()?;

// Opt a preset out: a disabled config applies no wrapping.
let bare = CacheKit::production("redis://localhost:6379").await?
    .reliability(ReliabilityConfig::disabled())
    .build()?;
```

Requires a tokio runtime for backoff timers (the `redis` and `cachekitio` backends already do).

## Observability

Every client counts its reads, with no configuration:

```rust,ignore
let stats = cache.stats();                 // cachekit::L1Stats — live, shared by all clones
println!(
    "L1 {} / L2 {} / miss {} — L1 hit rate {:.1}%",
    stats.l1_hits, stats.l2_hits, stats.misses, stats.l1_hit_rate() * 100.0,
);
let occupancy = cache.l1_entry_count();    // Option<u64>: None when L1 is off
let breaker = cache.circuit_state();       // Option<CircuitState>: Closed / Open / HalfOpen
```

| Surface | What you get |
|:--------|:-------------|
| `CacheKit::stats()` | `L1Stats { l1_hits, l2_hits, misses, l1_enabled }`. Each value read (`get`, `interop_get`, SWR and `SecureCache` variants) counts one outcome: a hit by the layer that served it, or a miss. Each `#[cachekit]` call counts one outcome too: a hit by the layer that served its value, or one miss when it runs the function; its own reads, single-flight re-polls included, count nothing. `exists`, and reads that fail at the backend or on an oversized payload, are not counted; a plain read whose served entry then fails to decode or decrypt counts the layer that served it. |
| `CacheKit::l1_entry_count()` | Exact L1 occupancy. Sweeps expired entries out first, a pass over every entry, so poll it; don't put it on a hot path. |
| `CacheKit::circuit_state()` | Live breaker state (`reliability` feature); `None` when the client has no breaker. |
| CacheKit Cloud telemetry headers | The CacheKit Cloud backends send `X-CacheKit-L1-Hits` / `L2-Hits` / `Misses` / `L1-Hit-Rate` from the **same counters**, wired automatically by `CacheKitBuilder::build()`, and a sampled L1 hit-latency histogram: see [Telemetry sent to CacheKit Cloud](#telemetry-sent-to-cachekit-cloud). A `.metrics_provider(..)` set on the backend builder still takes precedence. One backend instance reports one client's counters — the first built over it; once that client is gone the counters fall back to `disabled`. |
| `tracing` feature | One `debug` event per completed operation (a `#[cachekit]` call's reads emit one `get` event, with the call's outcome), per failed fill-lock call, and per `#[cachekit]` result the macro could not store, on the `cachekit` target, and breaker transitions on `cachekit::reliability` (`warn` on open, `info` for half-open / closed). |

With the `tracing` feature, point your subscriber at the crate:

```bash
RUST_LOG=cachekit=debug cargo run
```

```text
DEBUG cachekit: op=get key_hash=bcb35ae6f64fa65b2770ab3af631b1ce outcome=miss
DEBUG cachekit: op=set key_hash=bcb35ae6f64fa65b2770ab3af631b1ce ttl_secs=3600
DEBUG cachekit: op=get key_hash=bcb35ae6f64fa65b2770ab3af631b1ce outcome=l1_hit
 WARN cachekit::reliability: circuit breaker opened breaker=1 seq=1 from=Closed to=Open
```

Fields: `op` (`get` | `set` | `delete` | `lock` | `unlock`), `outcome` (`l1_hit` | `l1_stale` | `l2_hit` | `miss`), `ttl_secs`, `existed`, and `error_kind` (`transient` | `timeout` | …) on a failed `lock` or `unlock`: a fill-lock call skips the circuit breaker, so this event is the only signal of a failing lock endpoint. A `#[cachekit]` result the macro cannot store is not an error: a cold-miss fill is still returned, and a failed SWR refresh commit is dropped while the stale value keeps being served. An `op=set` event with `error_kind` (a backend kind, or `serialization` | `encryption` | `config` | `payload-too-large` | `invalid-key`) is the only record, since otherwise the failure shows only as repeat misses or refreshes. Breaker events carry `breaker`, `seq`, `from`, `to`, and **`(breaker, seq)` is the ordering key**: `breaker` is a process-unique id assigned when the breaker is built (stable for its lifetime, not a key or secret), `seq` counts that breaker's transitions and is assigned under the breaker lock. Events are emitted after the lock is released (so a subscriber may call `circuit_state()` safely), which means two transitions can arrive out of order under contention, and several clients in one process each restart `seq` at 1 — group by `breaker`, order by `seq`, never by arrival. For fleet-wide correlation combine the pair with the host/process fields your subscriber adds. Events carry `key_hash` — Blake2b-128 of the namespaced storage key (`cachekit::metrics::key_hash`) — **never the key itself**: keys routinely embed user identifiers (CWE-532). The digest is a correlator, not a redaction: it is unkeyed and deterministic, so it matches the [File backend](#file-local-filesystem)'s on-disk filename (a log line names the cache file it touched), and for the same reason a low-entropy key like `user:42` can be recovered from it by enumeration. Treat `cachekit=debug` output with the care you give the keys themselves.

Prometheus exposition and OpenTelemetry spans are deliberately not built in: Rust services bring their own registry and bridge `tracing` themselves.

### Telemetry sent to CacheKit Cloud

**Unreleased.** The latency headers and the telemetry switch are on `main` and in no published release yet. The current release sends the session and counter headers with no way to turn them off, and its `WorkersCachekitIO::builder()` takes no bindings.

Only the CacheKit Cloud backends, `CachekitIO` and `WorkersCachekitIO`, send telemetry, and only to the API they are built with; they never follow a redirect. A Redis, Memcached or File backend sends none anywhere. While telemetry is on, each request carries these optional headers ([protocol spec](https://github.com/cachekit-io/protocol/blob/main/spec/saas-api.md#optional-metrics-headers)):

| Header | Value |
|:-------|:------|
| `X-CacheKit-Session-ID` | A random UUID for the process |
| `X-CacheKit-Session-Start` | When the process started its session, in milliseconds since the epoch |
| `X-CacheKit-L1-Hits`, `X-CacheKit-L2-Hits`, `X-CacheKit-Misses` | The client's read counters, as `stats()` returns them, since the session started |
| `X-CacheKit-L1-Hit-Rate` | `L1-Hits / (L1-Hits + L2-Hits + Misses)`, three decimals, `0.000` before any read |
| `X-CacheKit-L1-Latency-Buckets` | The L1 hits timed since the previous report, counted into 16 duration buckets: 15 that double from 128 ns to 2.1 ms, and a last one up to 60 s |
| `X-CacheKit-L1-Latency-Sum-Ns` | The total duration of those hits, in nanoseconds |
| `X-CacheKit-L1-Latency-One-In` | N: each call was timed with probability 1/N (128) |

One value read in 128, drawn at random before the read starts, is timed, from the call's first step until it has its value: resolving the key, the L1 lookup, decryption and decoding. Only a read that L1 served and that returns a value counts; a `#[cachekit]` call that waited for another call's fill of the same key, an existence check, and the reads inside a `#[cachekit]` call are never timed. The report rides the backend's next request, except the health check, and is never sent again, even when that request fails. Hits timed after the last request are never sent. `WorkersCachekitIO` sends no latency headers: the Workers clock advances only on I/O, so it cannot time an in-memory hit.

Telemetry is on by default during the closed beta, and becomes opt-in when CacheKit Cloud reaches general availability. Two controls turn it off, and off from either wins:

- `.telemetry(false)` on `CachekitIO::builder()` or `WorkersCachekitIO::builder()`. The setting is unset by default.
- `CACHEKIT_TELEMETRY=false` in the environment. `false`, `0`, `off` and `no` turn telemetry off and `true`, `1`, `on` and `yes` turn it on, in any case and ignoring surrounding white space. Unset or empty leaves the default. Any other value also turns telemetry off and warns once: through `tracing` natively (with the `tracing` feature), and to the console on Workers. On Workers the variable is read from the bindings passed to `WorkersCachekitIO::builder(&env)` and from `process.env`.

Both controls are read once, when the backend is built. With telemetry off the backend sends none of the headers above and times no read. It still sends `X-CacheKit-L1-Status`, which the service requires on every request and which describes that request only, and, natively, `User-Agent: cachekit-rs/<version>`.

## Performance

What one call costs in the client: the public `CacheKit` API over an in-memory backend, so no figure includes network time. The client comes from `CacheKit::builder()` with no reliability stack, like `minimal`. The `production`, `secure` and `io` presets send every backend call through the [reliability stack](#reliability), so their L2 hits and sets cost more than the table shows; L1 hits never reach it. One **Bench** workflow run times each cell in five processes of its own, each on a single-threaded tokio runtime, and reports their median wall time per call. Measured on GitHub-hosted `ubuntu-latest` runners (AMD EPYC 7763, 4 vCPUs) with Rust 1.99.1.

| Operation | 64 B | 1 KiB |
|:----------|-----:|------:|
| `get`, L1 hit | 187 ns | 234 ns |
| `get`, L2 hit | 1.4 µs | 2.2 µs |
| `set_with_ttl` | 752 ns | 793 ns |
| `secure_cache()` `get`, L1 hit | 592 ns | 809 ns |
| `secure_cache()` `get`, L2 hit | 1.9 µs | 2.9 µs |
| `secure_cache()` `set_with_ttl` | 1.2 µs | 1.5 µs |

- **L1 hit**: the value comes from the in-process cache; the backend is not asked.
- **L2 hit**: L1 does not hold the key, so `get` reads the backend, decodes the MessagePack and backfills L1. A real backend adds its round trip, which for Redis or CacheKit Cloud is a network call.
- **Encrypted** (`secure_cache()`): L1 holds the same ciphertext as the backend, so every hit decrypts (AES-256-GCM) and checks the AAD.
- **Larger values**: at 64 KiB most of the time is copying and allocating. It ran to tens of microseconds for L2 hits and encrypted calls, but repeat runs disagreed by up to 3.5x, so the table stops at 1 KiB.

Each cell in the table is the median of seven such runs of one build on this CPU model. Across the seven, L1 hits varied by up to 10% and L2 hits by up to 29%. Hosted runners also come with other CPU models: on an AMD EPYC 9V45, three runs of the same build took 62-93% as long, cell for cell. Other machines give other figures. To print this table for your own machine, run `cargo run --release --example bench_hot_path -- readme`, or run the **Bench** workflow from the Actions tab. To compare two builds, use the instruction counts in [Measuring performance](CONTRIBUTING.md#measuring-performance).

## Architecture

```
cachekit-rs/
├── crates/
│   ├── cachekit/              # Main SDK crate
│   │   └── src/
│   │       ├── lib.rs         # Public API + prelude
│   │       ├── client.rs      # CacheKit, SecureCache, CacheKitBuilder
│   │       ├── config.rs      # CachekitConfig + from_env()
│   │       ├── encryption.rs  # AES-256-GCM + AAD v0x03
│   │       ├── error.rs       # CachekitError, BackendError
│   │       ├── interop.rs     # interop/v1 cross-SDK keys + strict reads
│   │       ├── metrics.rs     # Live counters, CacheKit Cloud telemetry headers, tracing events
│   │       ├── session.rs     # SDK session tracking
│   │       ├── url_validator.rs # SSRF-safe URL validation
│   │       ├── serializer/    # MessagePack serialization
│   │       ├── l1/            # L1 cache: quick_cache plus TTL and SWR (feature = "l1")
│   │       └── backend/
│   │           ├── mod.rs     # Backend + TtlInspectable + LockableBackend traits
│   │           ├── cachekitio.rs      # CacheKit Cloud HTTP backend
│   │           ├── cachekitio_lock.rs # Distributed locking
│   │           ├── cachekitio_ttl.rs  # TTL inspection
│   │           ├── saas_wire.rs       # CacheKit Cloud lock/TTL JSON wire bodies
│   │           ├── redis.rs           # Redis backend (feature = "redis")
│   │           └── workers.rs         # Workers backend (feature = "workers")
│   │
│   └── cachekit-macros/       # Proc-macro crate
│       └── src/lib.rs         # #[cachekit] decorator
│
├── Cargo.toml                 # Workspace root
└── Makefile                   # Development commands
```

## Documentation

- [Rust Quickstart](https://docs.cachekit.io/rust/quickstart/)
- [Backends Reference](https://docs.cachekit.io/rust/reference/backends/)
- [Builder Reference](https://docs.cachekit.io/rust/reference/builder/)
- [Encryption Reference](https://docs.cachekit.io/rust/reference/encryption/)
- [Macros Reference](https://docs.cachekit.io/rust/reference/macros/)
- [Observability Reference](https://docs.cachekit.io/rust/reference/observability/)
- [API reference on docs.rs](https://docs.rs/cachekit-rs)
- [Protocol spec](https://github.com/cachekit-io/protocol): the cross-SDK cache-key, wire, encryption and CacheKit Cloud API formats, with test vectors

## Contributing

Building from source, running the tests and benchmarks, the CI gates and the release mechanics are in [CONTRIBUTING.md](CONTRIBUTING.md).

User-facing docs in this repository follow CacheKit's shared rule on what belongs in them:
[What belongs in these docs](https://docs.cachekit.io/contributing/#what-belongs-in-these-docs).

## License

MIT — see [LICENSE](LICENSE) for details.
