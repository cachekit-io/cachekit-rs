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

// ── SharedEncryption type alias ──────────────────────────────────────────────

/// Thread-safe reference to the encryption layer.
///
/// On native targets `Arc` is used (requires `Sync`).
/// On `wasm32` the Workers runtime is single-threaded so `Rc` is sufficient
/// and avoids the `!Sync` problem caused by `Cell<u64>` inside cachekit-core's
/// nonce counter.
#[cfg(all(feature = "encryption", not(target_arch = "wasm32")))]
type SharedEncryption = std::sync::Arc<crate::encryption::EncryptionLayer>;

#[cfg(all(feature = "encryption", target_arch = "wasm32"))]
type SharedEncryption = std::rc::Rc<crate::encryption::EncryptionLayer>;

// ── Key validation ────────────────────────────────────────────────────────────

const MAX_KEY_BYTES: usize = 1024;

/// Maximum TTL for L1 entries populated from L2 cache hits.
/// Uses a short ceiling to limit staleness when the original TTL is unknown.
const L1_BACKFILL_TTL_SECS: u64 = 30;

fn validate_key(key: &str) -> Result<(), CachekitError> {
    if key.is_empty() {
        return Err(CachekitError::InvalidKey(
            "key must not be empty".to_owned(),
        ));
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

    #[cfg(feature = "encryption")]
    encryption: Option<SharedEncryption>,
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
    #[cfg(all(feature = "cachekitio", not(target_arch = "wasm32")))]
    pub fn from_env() -> Result<CacheKitBuilder, CachekitError> {
        use crate::backend::cachekitio::CachekitIO;
        use crate::config::CachekitConfig;

        let config = CachekitConfig::from_env()?;

        let api_key_z = config
            .api_key
            .ok_or_else(|| CachekitError::Config("CACHEKIT_API_KEY is required".to_owned()))?;

        let backend = CachekitIO::builder()
            .api_key(api_key_z.as_str())
            .api_url(config.api_url)
            .build()
            .map_err(|e| CachekitError::Config(e.to_string()))?;

        let mut builder = CacheKitBuilder::default()
            .backend(std::sync::Arc::new(backend))
            .default_ttl(config.default_ttl)
            .max_payload_bytes(config.max_payload_bytes)
            .l1_capacity(config.l1_capacity);

        if let Some(ns) = config.namespace.clone() {
            builder = builder.namespace(ns);
        }

        // Wire up encryption if master key is configured
        #[cfg(feature = "encryption")]
        if let Some(ref master_key) = config.master_key {
            let namespace = config.namespace.as_deref().unwrap_or("default");
            builder = builder.encryption_from_bytes(master_key, namespace)?;
        }

        Ok(builder)
    }

    // ── Namespacing ───────────────────────────────────────────────────────────

    fn namespaced_key(&self, key: &str) -> String {
        match &self.namespace {
            Some(ns) => format!("{ns}:{key}"),
            None => key.to_owned(),
        }
    }

    /// Validate key and return the namespaced version.
    fn resolve_key(&self, key: &str) -> Result<String, CachekitError> {
        validate_key(key)?;
        Ok(self.namespaced_key(key))
    }

    // ── L1 helpers ───────────────────────────────────────────────────────────

    /// Try L1 cache first. Returns Some(bytes) on hit.
    #[cfg(feature = "l1")]
    fn l1_get(&self, full_key: &str) -> Option<Vec<u8>> {
        self.l1.as_ref().and_then(|l1| l1.get(full_key))
    }

    /// Populate L1 from an L2 hit with capped TTL to limit staleness.
    #[cfg(feature = "l1")]
    fn l1_backfill(&self, full_key: &str, bytes: &[u8]) {
        if let Some(ref l1) = self.l1 {
            let l1_ttl = std::cmp::min(self.default_ttl, Duration::from_secs(L1_BACKFILL_TTL_SECS));
            l1.set(full_key, bytes, l1_ttl);
        }
    }

    /// Write-through to L1.
    #[cfg(feature = "l1")]
    fn l1_set(&self, full_key: &str, bytes: &[u8], ttl: Duration) {
        if let Some(ref l1) = self.l1 {
            l1.set(full_key, bytes, ttl);
        }
    }

    /// Invalidate L1 entry.
    #[cfg(feature = "l1")]
    fn l1_delete(&self, full_key: &str) {
        if let Some(ref l1) = self.l1 {
            l1.delete(full_key);
        }
    }

    /// Validate TTL is at least 1 second.
    fn validate_ttl(ttl: Duration) -> Result<(), CachekitError> {
        if ttl < Duration::from_secs(1) {
            return Err(CachekitError::Config(format!(
                "TTL must be at least 1 second; got {ttl:?}"
            )));
        }
        Ok(())
    }

    // ── Public operations ─────────────────────────────────────────────────────

    /// Retrieve and deserialize a value stored under `key`.
    ///
    /// Returns `None` if the key does not exist.
    /// Checks L1 cache before hitting the backend.
    pub async fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, CachekitError> {
        let full_key = self.resolve_key(key)?;

        // L1 hit
        #[cfg(feature = "l1")]
        if let Some(bytes) = self.l1_get(&full_key) {
            self.check_payload_size(bytes.len())?;
            return Ok(Some(serializer::deserialize(&bytes)?));
        }

        // L2 backend
        let bytes = match self.backend.get(&full_key).await? {
            Some(b) => b,
            None => return Ok(None),
        };

        self.check_payload_size(bytes.len())?;

        // Populate L1 on L2 hit (capped TTL to limit staleness)
        #[cfg(feature = "l1")]
        self.l1_backfill(&full_key, &bytes);

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
        Self::validate_ttl(ttl)?;

        let bytes = serializer::serialize(value)?;
        self.check_payload_size(bytes.len())?;

        let full_key = self.resolve_key(key)?;

        // Only clone bytes when L1 needs a copy after the backend consumes them.
        #[cfg(feature = "l1")]
        {
            let l1_bytes = bytes.clone();
            self.backend.set(&full_key, bytes, Some(ttl)).await?;
            self.l1_set(&full_key, &l1_bytes, ttl);
        }
        #[cfg(not(feature = "l1"))]
        {
            self.backend.set(&full_key, bytes, Some(ttl)).await?;
        }

        Ok(())
    }

    /// Delete `key` and return `true` if it existed.
    ///
    /// Invalidates the L1 entry regardless of the backend result.
    pub async fn delete(&self, key: &str) -> Result<bool, CachekitError> {
        let full_key = self.resolve_key(key)?;

        // Invalidate L1 first so callers never read a stale value even if the
        // backend delete fails partway through.
        #[cfg(feature = "l1")]
        self.l1_delete(&full_key);

        Ok(self.backend.delete(&full_key).await?)
    }

    /// Return `true` if `key` exists without fetching the value.
    pub async fn exists(&self, key: &str) -> Result<bool, CachekitError> {
        let full_key = self.resolve_key(key)?;

        // Check L1 first — avoids a network round-trip for warm entries.
        #[cfg(feature = "l1")]
        if self.l1_get(&full_key).is_some() {
            return Ok(true);
        }

        Ok(self.backend.exists(&full_key).await?)
    }

    // ── Secure cache ─────────────────────────────────────────────────────────

    /// Return a [`SecureCache`] handle that encrypts all values before storage.
    ///
    /// L1 stores **ciphertext** (not plaintext) to preserve the zero-knowledge
    /// property across all cache layers.
    ///
    /// # Errors
    /// Returns `CachekitError::Config` if no encryption layer is configured.
    /// Configure encryption via [`CacheKitBuilder::encryption`] or
    /// [`CacheKitBuilder::encryption_from_bytes`].
    #[cfg(feature = "encryption")]
    pub fn secure(&self) -> Result<SecureCache<'_>, CachekitError> {
        let enc = self.encryption.as_ref().ok_or_else(|| {
            CachekitError::Config(
                "encryption requires CACHEKIT_MASTER_KEY or .encryption() on builder".to_owned(),
            )
        })?;
        Ok(SecureCache {
            client: self,
            encryption: enc,
        })
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

// ── SecureCache ──────────────────────────────────────────────────────────────

/// Encrypted cache handle returned by [`CacheKit::secure()`].
///
/// All values are serialized, then encrypted with AES-256-GCM before storage.
/// L1 stores ciphertext to maintain zero-knowledge guarantees.
#[cfg(feature = "encryption")]
pub struct SecureCache<'a> {
    client: &'a CacheKit,
    encryption: &'a crate::encryption::EncryptionLayer,
}

#[cfg(feature = "encryption")]
impl std::fmt::Debug for SecureCache<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecureCache")
            .field("tenant_id", &self.encryption.tenant_id())
            .finish()
    }
}

#[cfg(feature = "encryption")]
impl SecureCache<'_> {
    /// Encrypt and store `value` under `key` using the client's default TTL.
    pub async fn set<T: Serialize>(&self, key: &str, value: &T) -> Result<(), CachekitError> {
        self.set_with_ttl(key, value, self.client.default_ttl).await
    }

    /// Encrypt and store `value` under `key` with an explicit `ttl`.
    pub async fn set_with_ttl<T: Serialize>(
        &self,
        key: &str,
        value: &T,
        ttl: Duration,
    ) -> Result<(), CachekitError> {
        CacheKit::validate_ttl(ttl)?;

        // Serialize then encrypt
        let plaintext = serializer::serialize(value)?;
        self.client.check_payload_size(plaintext.len())?;
        let ciphertext = self.encryption.encrypt(&plaintext, key)?;

        let full_key = self.client.resolve_key(key)?;

        // Only clone when L1 needs a copy after the backend consumes the data.
        #[cfg(feature = "l1")]
        {
            let l1_bytes = ciphertext.clone();
            self.client
                .backend
                .set(&full_key, ciphertext, Some(ttl))
                .await?;
            self.client.l1_set(&full_key, &l1_bytes, ttl);
        }
        #[cfg(not(feature = "l1"))]
        {
            self.client
                .backend
                .set(&full_key, ciphertext, Some(ttl))
                .await?;
        }

        Ok(())
    }

    /// Retrieve, decrypt, and deserialize a value stored under `key`.
    ///
    /// Checks L1 (which holds ciphertext) before the backend.
    pub async fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, CachekitError> {
        let full_key = self.client.resolve_key(key)?;

        // L1 hit (ciphertext)
        #[cfg(feature = "l1")]
        if let Some(ciphertext) = self.client.l1_get(&full_key) {
            self.client.check_payload_size(ciphertext.len())?;
            let plaintext = self.encryption.decrypt(&ciphertext, key)?;
            return Ok(Some(serializer::deserialize(&plaintext)?));
        }

        // L2 backend
        let ciphertext = match self.client.backend.get(&full_key).await? {
            Some(b) => b,
            None => return Ok(None),
        };

        self.client.check_payload_size(ciphertext.len())?;

        // Populate L1 with ciphertext on L2 hit (capped TTL to limit staleness)
        #[cfg(feature = "l1")]
        self.client.l1_backfill(&full_key, &ciphertext);

        let plaintext = self.encryption.decrypt(&ciphertext, key)?;
        Ok(Some(serializer::deserialize(&plaintext)?))
    }

    /// Delete an encrypted key. Behaves identically to [`CacheKit::delete`].
    pub async fn delete(&self, key: &str) -> Result<bool, CachekitError> {
        self.client.delete(key).await
    }

    /// Check if an encrypted key exists. Behaves identically to [`CacheKit::exists`].
    pub async fn exists(&self, key: &str) -> Result<bool, CachekitError> {
        self.client.exists(key).await
    }
}

// ── CacheKitBuilder ───────────────────────────────────────────────────────────

/// Fluent builder for [`CacheKit`].
#[derive(Default)]
#[must_use]
pub struct CacheKitBuilder {
    backend: Option<SharedBackend>,
    default_ttl: Option<Duration>,
    namespace: Option<String>,
    max_payload_bytes: Option<usize>,

    #[cfg(feature = "l1")]
    l1_capacity: Option<usize>,

    #[cfg(feature = "l1")]
    no_l1: bool,

    #[cfg(feature = "encryption")]
    encryption: Option<SharedEncryption>,
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

    /// Configure encryption from raw master key bytes and tenant ID.
    ///
    /// The master key must be at least 16 bytes (32 recommended).
    /// Keys are derived per-tenant via HKDF-SHA256.
    #[cfg(feature = "encryption")]
    pub fn encryption_from_bytes(
        mut self,
        master_key: &[u8],
        tenant_id: &str,
    ) -> Result<Self, CachekitError> {
        let layer = crate::encryption::EncryptionLayer::new(master_key, tenant_id)?;
        self.encryption = Some(SharedEncryption::new(layer));
        Ok(self)
    }

    /// Configure encryption from a hex-encoded master key string.
    ///
    /// Convenience wrapper that hex-decodes then delegates to
    /// [`Self::encryption_from_bytes`].
    #[cfg(feature = "encryption")]
    pub fn encryption(self, hex_key: &str, tenant_id: &str) -> Result<Self, CachekitError> {
        let bytes = hex::decode(hex_key)
            .map_err(|e| CachekitError::Config(format!("master key is not valid hex: {e}")))?;
        self.encryption_from_bytes(&bytes, tenant_id)
    }

    // Stub for when encryption feature is disabled.
    #[cfg(not(feature = "encryption"))]
    pub fn encryption_from_bytes(
        self,
        _master_key: &[u8],
        _tenant_id: &str,
    ) -> Result<Self, CachekitError> {
        Ok(self)
    }

    #[cfg(not(feature = "encryption"))]
    pub fn encryption(self, _hex_key: &str, _tenant_id: &str) -> Result<Self, CachekitError> {
        Ok(self)
    }

    /// Finalise and build the [`CacheKit`] client.
    ///
    /// Returns an error if no backend was provided.
    pub fn build(self) -> Result<CacheKit, CachekitError> {
        let backend = self.backend.ok_or_else(|| {
            CachekitError::Config("a backend must be provided via .backend()".to_owned())
        })?;

        // Validate namespace if provided
        if let Some(ref ns) = self.namespace {
            if ns.is_empty() {
                return Err(CachekitError::Config("namespace cannot be empty".into()));
            }
            if ns.len() > 255 {
                return Err(CachekitError::Config("namespace exceeds 255 bytes".into()));
            }
            if !ns.bytes().all(|b| (0x20..=0x7E).contains(&b)) {
                return Err(CachekitError::Config(
                    "namespace must be ASCII printable".into(),
                ));
            }
        }

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

            #[cfg(feature = "encryption")]
            encryption: self.encryption,
        })
    }
}
