# cachekit-macros

Proc-macro companion crate for [`cachekit-rs`](https://crates.io/crates/cachekit-rs).

Provides the `#[cachekit]` attribute macro for declarative function-level caching
under cross-SDK [interop/v1](https://github.com/cachekit-io/protocol/blob/main/spec/interop-mode.md)
cache keys (`{namespace}:{operation}:{args_hash}` — the same entry is addressable
from the Python and TypeScript SDKs).

```rust
use cachekit::prelude::*;

#[cachekit(client = cache, ttl = 60, interop = "get_user", namespace = "users")]
async fn get_user(cache: &CacheKit, id: u64) -> Result<User, CachekitError> {
    // Expensive database lookup — automatically cached for 60s
    db.find_user(id).await
}
```

This crate is not intended to be used directly. Add it via the `macros` feature on `cachekit-rs`:

```toml
[dependencies]
cachekit-rs = { version = "0.2", features = ["macros"] }
```

See the [cachekit-rs documentation](https://docs.rs/cachekit-rs) for full usage details.
