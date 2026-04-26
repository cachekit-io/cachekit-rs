# cachekit-macros

Proc-macro companion crate for [`cachekit-rs`](https://crates.io/crates/cachekit-rs).

Provides the `#[cachekit]` attribute macro for declarative function-level caching.

```rust
use cachekit::prelude::*;

#[cachekit(client = cache, ttl = 60)]
async fn get_user(id: u64) -> Result<User, CachekitError> {
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
