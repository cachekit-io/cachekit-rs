# cachekit-rs

Rust SDK for [cachekit.io](https://cachekit.io) — production-ready caching with zero-knowledge encryption.

## Crates

| Crate | Path | Purpose |
|-------|------|---------|
| `cachekit` | `crates/cachekit/` | Main SDK |
| `cachekit-macros` | `crates/cachekit-macros/` | Proc-macro decorators |

## Features

| Feature | Default | Description |
|---------|---------|-------------|
| `cachekitio` | yes | HTTP backend for api.cachekit.io (reqwest + rustls) |
| `encryption` | yes | Zero-knowledge AES-256-GCM via cachekit-core |
| `l1` | yes | In-process L1 cache via moka |
| `redis` | no | Redis backend via fred (native only) |
| `workers` | no | Cloudflare Workers backend via worker::Fetch |
| `macros` | no | `#[cache]` proc-macro decorator |

## Mutually exclusive

- `workers` + `redis` — Workers runtime cannot use fred
- `workers` + `l1` — moka requires std threads unavailable in wasm32

## Quick start

```rust
use cachekit::prelude::*;

#[tokio::main]
async fn main() -> Result<(), CachekitError> {
    let config = CachekitConfig::from_env()?;
    let cache = CacheKit::new(config).await?;

    cache.set("greeting", &"Hello, world!", None).await?;
    let val: String = cache.get("greeting").await?.unwrap();
    println!("{val}");
    Ok(())
}
```

## Development

```bash
make quick-check   # fmt + clippy + test
make test          # cargo test --all-features
make build         # cargo build --release
make build-wasm    # wasm32 target (workers feature)
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `CACHEKIT_API_KEY` | API key for cachekit.io |
| `CACHEKIT_API_URL` | Override API endpoint (default: https://api.cachekit.io) |
| `CACHEKIT_MASTER_KEY` | Hex-encoded master key (min 32 bytes) for encryption |
| `CACHEKIT_DEFAULT_TTL` | Default TTL in seconds (min 1) |
