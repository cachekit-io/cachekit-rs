# cachekit-rs

<div align="center">

**Production-ready caching for Rust — dual-layer L1/L2, zero-knowledge encryption, multi-backend.**

[![Crates.io](https://img.shields.io/crates/v/cachekit-rs.svg)](https://crates.io/crates/cachekit-rs)
[![docs.rs](https://docs.rs/cachekit-rs/badge.svg)](https://docs.rs/cachekit-rs)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![MSRV](https://img.shields.io/badge/MSRV-1.85-blue.svg)](https://blog.rust-lang.org/2025/02/20/Rust-1.85.0.html)

[Features](#features) · [Quick Start](#quick-start) · [Encryption](#zero-knowledge-encryption) · [Backends](#backends) · [Architecture](#architecture)

</div>

---

## Overview

`cachekit-rs` is the Rust SDK for [cachekit.io](https://cachekit.io). Plug in a backend, get dual-layer caching with optional client-side encryption. Bytes never leave your process unencrypted unless you say so.

| Component | What it does |
|:----------|:-------------|
| **CacheKit** | `get` / `set` / `delete` / `exists` with automatic L1 → L2 layering |
| **SecureCache** | Transparent AES-256-GCM encryption before storage (zero-knowledge) |
| **Backend** | Pluggable trait — cachekit.io SaaS, Redis, Cloudflare Workers |
| **L1 Cache** | In-process [moka](https://crates.io/crates/moka) cache with write-through + backfill |

> [!TIP]
> For the Python SDK with decorators, see [`cachekit`](https://github.com/cachekit-io/cachekit).
> For the low-level compression/encryption primitives, see [`cachekit-core`](https://crates.io/crates/cachekit-core).

---

## Features

| Feature | Default | Description |
|:--------|:-------:|:------------|
| `cachekitio` | ✅ | HTTP backend for [api.cachekit.io](https://api.cachekit.io) via [reqwest](https://crates.io/crates/reqwest) + rustls |
| `encryption` | ✅ | Zero-knowledge AES-256-GCM via [cachekit-core](https://crates.io/crates/cachekit-core) |
| `l1` | ✅ | In-process L1 cache via [moka](https://crates.io/crates/moka) |
| `redis` | ❌ | Redis backend via [fred](https://crates.io/crates/fred) (native only) |
| `workers` | ❌ | Cloudflare Workers backend via [worker](https://crates.io/crates/worker) |
| `macros` | ❌ | `#[cachekit]` proc-macro decorator |

```toml
# Defaults: SaaS + encryption + L1
[dependencies]
cachekit-rs = "0.2"

# With Redis backend
[dependencies]
cachekit-rs = { version = "0.2", features = ["redis"] }

# For Cloudflare Workers (no L1, no Redis)
[dependencies]
cachekit-rs = { version = "0.2", default-features = false, features = ["workers", "encryption"] }
```

> [!WARNING]
> **Mutually exclusive features:**
> - `workers` + `redis` — Workers runtime cannot use fred
> - `workers` + `l1` — moka requires std threads unavailable in wasm32

---

## Quick Start

### From Environment Variables

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

---

## Zero-Knowledge Encryption

Call `.secure()` to get an encrypted cache handle. All values are encrypted client-side with AES-256-GCM before hitting any backend. The backend only ever sees ciphertext.

```rust
let cache = CacheKit::from_env()?.build()?;
let secure = cache.secure()?;

// Encrypt → store (backend sees only ciphertext)
secure.set("user:42:ssn", &"123-45-6789").await?;

// Retrieve → decrypt (transparent to caller)
let ssn: String = secure.get("user:42:ssn").await?.unwrap();
```

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Your Code   │────>│  SecureCache  │────>│   Backend    │
│              │     │  AES-256-GCM  │     │  (cachekit.io│
│  plaintext   │     │  encrypt /    │     │   or Redis)  │
│              │<────│  decrypt      │<────│              │
└──────────────┘     └──────────────┘     └──────────────┘
                      L1 stores ciphertext
                      (zero-knowledge preserved)
```

<details>
<summary><strong>Security Properties</strong></summary>

| Property | Implementation |
|:---------|:---------------|
| **Encryption** | AES-256-GCM (AEAD) via [cachekit-core](https://crates.io/crates/cachekit-core) (`ring` on native, `aes-gcm` on wasm32) |
| **Key Derivation** | HKDF-SHA256 — per-tenant cryptographic isolation |
| **AAD Binding** | Cache key bound to ciphertext (prevents substitution attacks) |
| **Memory Safety** | [zeroize](https://crates.io/crates/zeroize) on drop for all key material |
| **L1 Guarantee** | L1 stores ciphertext, never plaintext |

**AAD v0x03 wire format:**

```text
[version(0x03)][len(4)][tenant_id][len(4)][cache_key][len(4)][format][len(4)][compressed]
```

Each field is length-prefixed with a 4-byte big-endian u32 to prevent boundary-confusion attacks.
Cross-SDK compatible — ciphertext produced by the Python SDK decrypts with the Rust SDK and vice versa.

</details>

---

## Backends

### cachekit.io SaaS (default)

HTTP backend targeting [api.cachekit.io](https://api.cachekit.io) with session tracking, L1 metrics headers, SSRF-safe URL validation, distributed locking, and TTL inspection.

```rust
use cachekit::backend::cachekitio::CachekitIO;

let backend = CachekitIO::builder()
    .api_key("ck_live_...")
    .api_url("https://api.cachekit.io")  // optional, this is the default
    .build()?;
```

### Redis

Native Redis via [fred](https://crates.io/crates/fred) with cluster support. Requires the `redis` feature flag.

```toml
cachekit-rs = { version = "0.2", features = ["redis"] }
```

```rust
use cachekit::backend::redis::RedisBackend;

let backend = RedisBackend::builder()
    .url("redis://localhost:6379")
    .build()?;
backend.connect().await?;  // explicit connect required
```

### Cloudflare Workers

`wasm32-unknown-unknown` backend using `worker::Fetch`. Requires the `workers` feature with default features disabled.

```toml
cachekit-rs = { version = "0.2", default-features = false, features = ["workers", "encryption"] }
```

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

Optional extension traits: `TtlInspectable` (TTL queries), `LockableBackend` (distributed locking).

</details>

---

## Dual-Layer Caching

When the `l1` feature is enabled (default), CacheKit maintains an in-process [moka](https://crates.io/crates/moka) cache in front of the backend:

```
┌─────────────────────────────────────────────────────────┐
│                     CacheKit Client                     │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  GET path:                                              │
│  L1 hit (~50ns) ──► return immediately                  │
│  L1 miss ──► L2 backend ──► backfill L1 (30s cap)      │
│                                                         │
│  SET path:                                              │
│  write to L2 backend ──► write-through to L1            │
│                                                         │
│  DELETE path:                                           │
│  invalidate L1 first ──► delete from L2 backend         │
│                                                         │
├─────────────┬───────────────────────────────────────────┤
│  L1 (moka)  │  L2 (cachekit.io / Redis / Workers)      │
│  ~50ns      │  ~2–50ms                                  │
└─────────────┴───────────────────────────────────────────┘
```

| Behavior | Detail |
|:---------|:-------|
| **Write-through** | `set()` writes to L2 first, then L1 |
| **Backfill on miss** | L2 hits populate L1 with a capped 30s TTL |
| **Invalidate-first** | `delete()` evicts L1 before touching L2 |
| **Encrypted L1** | `SecureCache` stores ciphertext in L1 (never plaintext) |
| **Default capacity** | 1,000 entries (configurable via `.l1_capacity()`) |

---

## Environment Variables

| Variable | Required | Description |
|:---------|:--------:|:------------|
| `CACHEKIT_API_KEY` | ✅ | API key for cachekit.io |
| `CACHEKIT_API_URL` | ❌ | Override API endpoint (default: `https://api.cachekit.io`) |
| `CACHEKIT_MASTER_KEY` | ❌ | Hex-encoded master key (min 32 bytes) for encryption |
| `CACHEKIT_DEFAULT_TTL` | ❌ | Default TTL in seconds (min 1, default: 300) |

> [!CAUTION]
> `CACHEKIT_API_URL` must use HTTPS and must not point to a private IP address.
> Both constraints are enforced at configuration time.

---

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
│   │       ├── key.rs         # Blake2b-256 cache key generation
│   │       ├── metrics.rs     # L1 hit-rate metrics headers
│   │       ├── session.rs     # SDK session tracking
│   │       ├── url_validator.rs # SSRF-safe URL validation
│   │       ├── serializer/    # MessagePack serialization
│   │       ├── l1/            # moka-based L1 cache (feature = "l1")
│   │       └── backend/
│   │           ├── mod.rs     # Backend + TtlInspectable + LockableBackend traits
│   │           ├── cachekitio.rs      # cachekit.io HTTP backend
│   │           ├── cachekitio_lock.rs # Distributed locking
│   │           ├── cachekitio_ttl.rs  # TTL inspection
│   │           ├── redis.rs           # Redis backend (feature = "redis")
│   │           └── workers.rs         # Workers backend (feature = "workers")
│   │
│   └── cachekit-macros/       # Proc-macro crate
│       └── src/lib.rs         # #[cachekit] decorator
│
├── Cargo.toml                 # Workspace root
└── Makefile                   # Development commands
```

---

## Development

```bash
make quick-check   # fmt + clippy + test (run before every commit)
make test          # cargo test --all-features
make build         # cargo build --release
make build-wasm    # wasm32-unknown-unknown (workers feature)
```

## Minimum Supported Rust Version

**Rust 1.85** or later (Edition 2021).

## License

MIT — see [LICENSE](LICENSE) for details.

---

<div align="center">

**[Documentation](https://docs.rs/cachekit-rs)** · **[cachekit.io](https://cachekit.io)** · **[GitHub](https://github.com/cachekit-io/cachekit-rs)**

</div>
