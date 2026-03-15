use std::time::Duration;

use serde::{de::DeserializeOwned, Serialize};

use crate::backend::Backend;
use crate::error::CachekitError;
use crate::serializer;

// ── SharedBackend type alias ──────────────────────────────────────────────────

/// Thread-safe reference to a heap-allocated backend.
///
/// On native targets we require `Send + Sync` for use across threads.
/// On `wasm32` the Workers runtime is single-threaded so `Rc` is sufficient.
#[cfg(not(target_arch = "wasm32"))]
pub type SharedBackend = std::sync::Arc<dyn Backend>;

#[cfg(target_arch = "wasm32")]
pub type SharedBackend = std::rc::Rc<dyn Backend>;

// ── Key validation ────────────────────────────────────────────────────────────

const MAX_KEY_BYTES: usize = 1024;

fn validate_key(key: &str) -> Result<(), CachekitError> {
    if key.is_empty() {
        return Err(CachekitError::InvalidKey("key must not be empty".to_owned()));
    }
    if key.len() > MAX_KEY_BYTES {
        return Err(CachekitError::InvalidKey(format!(
            "key is {} bytes (limit: {MAX_KEY_BYTES})",
            key.len()
        )));
    }
    for b in key.bytes() {
        if b < 0x20 || b == 0x7F {
            return Err(CachekitError::InvalidKey(format!(
                "key contains illegal control character 0x{b:02X}"
            )));
        }
    }
    Ok(())
}

// ── CacheKit ─────────────────────────────────────────────────────────────────

/// Production-ready cache client with optional L1 in-process cache layer.
pub struct CacheKit {
    backend: SharedBackend,
    default_ttl: Duration,
    namespace: Option<String>,
    max_payload_bytes: usize,

    #[cfg(feature = "l1")]
    l1: Option<crate::l1::L1Cache>,
}

impl CacheKit {
    /// Create a new builder.
    pub fn builder() -> CacheKitBuilder {
        CacheKitBuilder::default()
    }

    /// Build from environment variables via [`crate::config::CachekitConfig::from_env`].
    ///
    /// Creates a [`crate::backend::cachekitio::CachekitIO`] backend from the
    /// config. Requires the `cachekitio` feature.
    #[cfg(feature = "cachekitio")]
    pub fn from_env() -> Result<CacheKitBuilder, CachekitError> {
        use crate::backend::cachekitio::CachekitIO;
        use crate::config::CachekitConfig;

        let config = CachekitConfig::from_env()?;

        let api_key = config
            .api_key
            .as_deref()
            .ok_or_else(|| CachekitError::Config("CACHEKIT_API_KEY is required".to_owned()))?
            .to_owned();

        let backend = CachekitIO::builder()
            .api_key(api_key)
            .api_url(config.api_url)
            .build()
            .map_err(|e| CachekitError::Config(e.to_string()))?;

        let mut builder = CacheKitBuilder::default()
            .backend(std::sync::Arc::new(backend))
            .default_ttl(config.default_ttl)
            .max_payload_bytes(config.max_payload_bytes)
            .l1_capacity(config.l1_capacity);

        if let Some(ns) = config.namespace {
            builder = builder.namespace(ns);
        }

        // TODO: Encryption wiring (Chunk 7)
        // if let Some(ref master_key) = config.master_key {
        //     let namespace = config.namespace.as_deref().unwrap_or("default");
        //     builder = builder.encryption_from_bytes(master_key, namespace)?;
        // }

        Ok(builder)
    }

    // ── Namespacing ───────────────────────────────────────────────────────────

    fn namespaced_key(&self, key: &str) -> String {
        match &self.namespace {
            Some(ns) => format!("{ns}:{key}"),
            None => key.to_owned(),
        }
    }

    // ── Public operations ─────────────────────────────────────────────────────

    /// Retrieve and deserialize a value stored under `key`.
    ///
    /// Returns `None` if the key does not exist.
    /// Checks L1 cache before hitting the backend.
    pub async fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, CachekitError> {
        validate_key(key)?;
        let full_key = self.namespaced_key(key);

        // L1 hit
        #[cfg(feature = "l1")]
        if let Some(ref l1) = self.l1 {
            if let Some(bytes) = l1.get(&full_key) {
                self.check_payload_size(bytes.len())?;
                return Ok(Some(serializer::deserialize(&bytes)?));
            }
        }

        // L2 backend
        let bytes = match self.backend.get(&full_key).await? {
            Some(b) => b,
            None => return Ok(None),
        };

        self.check_payload_size(bytes.len())?;

        // Populate L1 on L2 hit
        #[cfg(feature = "l1")]
        if let Some(ref l1) = self.l1 {
            l1.set(&full_key, &bytes, self.default_ttl);
        }

        Ok(Some(serializer::deserialize(&bytes)?))
    }

    /// Serialize and store `value` under `key` using the client's default TTL.
    pub async fn set<T: Serialize>(&self, key: &str, value: &T) -> Result<(), CachekitError> {
        self.set_with_ttl(key, value, self.default_ttl).await
    }

    /// Serialize and store `value` under `key` with an explicit `ttl`.
    ///
    /// Returns [`CachekitError::Config`] if `ttl` is less than 1 second.
    pub async fn set_with_ttl<T: Serialize>(
        &self,
        key: &str,
        value: &T,
        ttl: Duration,
    ) -> Result<(), CachekitError> {
        validate_key(key)?;

        if ttl < Duration::from_secs(1) {
            return Err(CachekitError::Config(format!(
                "TTL must be at least 1 second; got {ttl:?}"
            )));
        }

        let bytes = serializer::serialize(value)?;
        self.check_payload_size(bytes.len())?;

        let full_key = self.namespaced_key(key);
        self.backend.set(&full_key, bytes.clone(), Some(ttl)).await?;

        // Write-through to L1
        #[cfg(feature = "l1")]
        if let Some(ref l1) = self.l1 {
            l1.set(&full_key, &bytes, ttl);
        }

        Ok(())
    }

    /// Delete `key` and return `true` if it existed.
    ///
    /// Invalidates the L1 entry regardless of the backend result.
    pub async fn delete(&self, key: &str) -> Result<bool, CachekitError> {
        validate_key(key)?;
        let full_key = self.namespaced_key(key);

        // Invalidate L1 first so callers never read a stale value even if the
        // backend delete fails partway through.
        #[cfg(feature = "l1")]
        if let Some(ref l1) = self.l1 {
            l1.delete(&full_key);
        }

        Ok(self.backend.delete(&full_key).await?)
    }

    /// Return `true` if `key` exists without fetching the value.
    pub async fn exists(&self, key: &str) -> Result<bool, CachekitError> {
        validate_key(key)?;

        // Check L1 first — avoids a network round-trip for warm entries.
        #[cfg(feature = "l1")]
        if let Some(ref l1) = self.l1 {
            if l1.get(&self.namespaced_key(key)).is_some() {
                return Ok(true);
            }
        }

        Ok(self.backend.exists(&self.namespaced_key(key)).await?)
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    fn check_payload_size(&self, size: usize) -> Result<(), CachekitError> {
        if size > self.max_payload_bytes {
            return Err(CachekitError::PayloadTooLarge {
                size,
                limit: self.max_payload_bytes,
            });
        }
        Ok(())
    }
}

// ── CacheKitBuilder ───────────────────────────────────────────────────────────

/// Fluent builder for [`CacheKit`].
#[derive(Default)]
pub struct CacheKitBuilder {
    backend: Option<SharedBackend>,
    default_ttl: Option<Duration>,
    namespace: Option<String>,
    max_payload_bytes: Option<usize>,

    #[cfg(feature = "l1")]
    l1_capacity: Option<usize>,

    #[cfg(feature = "l1")]
    no_l1: bool,
}

impl CacheKitBuilder {
    /// Set the storage backend.
    pub fn backend(mut self, backend: SharedBackend) -> Self {
        self.backend = Some(backend);
        self
    }

    /// Override the default TTL (used when no per-call TTL is specified).
    pub fn default_ttl(mut self, ttl: Duration) -> Self {
        self.default_ttl = Some(ttl);
        self
    }

    /// Set a namespace prefix. All keys will be stored as `{namespace}:{key}`.
    pub fn namespace(mut self, ns: impl Into<String>) -> Self {
        self.namespace = Some(ns.into());
        self
    }

    /// Set the maximum accepted payload size in bytes.
    pub fn max_payload_bytes(mut self, limit: usize) -> Self {
        self.max_payload_bytes = Some(limit);
        self
    }

    /// Set the L1 cache capacity (max entries).
    #[cfg(feature = "l1")]
    pub fn l1_capacity(mut self, capacity: usize) -> Self {
        self.l1_capacity = Some(capacity);
        self
    }

    /// Disable the L1 cache entirely.
    #[cfg(feature = "l1")]
    pub fn no_l1(mut self) -> Self {
        self.no_l1 = true;
        self
    }

    // Stubs for when the l1 feature is disabled — still compile cleanly.
    #[cfg(not(feature = "l1"))]
    pub fn l1_capacity(self, _capacity: usize) -> Self {
        self
    }

    #[cfg(not(feature = "l1"))]
    pub fn no_l1(self) -> Self {
        self
    }

    /// Finalise and build the [`CacheKit`] client.
    ///
    /// Returns an error if no backend was provided.
    pub fn build(self) -> Result<CacheKit, CachekitError> {
        let backend = self
            .backend
            .ok_or_else(|| CachekitError::Config("a backend must be provided via .backend()".to_owned()))?;

        #[cfg(feature = "l1")]
        let l1 = if self.no_l1 {
            None
        } else {
            let capacity = self.l1_capacity.unwrap_or(1000);
            Some(crate::l1::L1Cache::new(capacity))
        };

        Ok(CacheKit {
            backend,
            default_ttl: self.default_ttl.unwrap_or(Duration::from_secs(300)),
            namespace: self.namespace,
            max_payload_bytes: self.max_payload_bytes.unwrap_or(5 * 1024 * 1024),

            #[cfg(feature = "l1")]
            l1,
        })
    }
}
