use std::time::Duration;

use serde::{de::DeserializeOwned, Serialize};

use crate::backend::Backend;
use crate::error::CachekitError;
use crate::serializer;

// ── SharedBackend type alias ──────────────────────────────────────────────────

/// Reference-counted pointer to a heap-allocated backend.
///
/// On native targets (without `unsync`) we require `Send + Sync` via `Arc`.
/// On `wasm32` or with the `unsync` feature, `Rc` is used instead — the runtime
/// is single-threaded so `Send` bounds are unnecessary.
#[cfg(not(any(target_arch = "wasm32", feature = "unsync")))]
pub type SharedBackend = std::sync::Arc<dyn Backend>;

/// Reference-counted pointer to a heap-allocated backend (`?Send` variant).
#[cfg(any(target_arch = "wasm32", feature = "unsync"))]
pub type SharedBackend = std::rc::Rc<dyn Backend>;

// ── SharedFlight type alias ──────────────────────────────────────────────────

/// Reference-counted pointer to the single-flight map, so client clones share
/// fill-dedup state (two clones racing a cold miss must collapse to one fill).
#[cfg(not(any(target_arch = "wasm32", feature = "unsync")))]
type SharedFlight = std::sync::Arc<crate::flight::FlightMap>;

#[cfg(any(target_arch = "wasm32", feature = "unsync"))]
type SharedFlight = std::rc::Rc<crate::flight::FlightMap>;

// ── SharedEncryption type alias ──────────────────────────────────────────────

/// Reference-counted pointer to the encryption layer.
///
/// On native targets (without `unsync`) `Arc` is used (requires `Sync`).
/// On `wasm32` or with `unsync`, `Rc` is used — avoids the `!Sync` problem
/// caused by `Cell<u64>` inside cachekit-core's nonce counter.
#[cfg(all(
    feature = "encryption",
    not(any(target_arch = "wasm32", feature = "unsync"))
))]
type SharedEncryption = std::sync::Arc<crate::encryption::EncryptionLayer>;

#[cfg(all(
    feature = "encryption",
    any(target_arch = "wasm32", feature = "unsync")
))]
type SharedEncryption = std::rc::Rc<crate::encryption::EncryptionLayer>;

// ── Key validation ────────────────────────────────────────────────────────────

const MAX_KEY_BYTES: usize = 1024;

/// Maximum TTL for L1 entries populated from L2 cache hits.
/// Uses a short ceiling to limit staleness when the original TTL is unknown.
///
/// Reconciliation with stale-while-revalidate: a backfilled entry's SWR
/// freshness window derives from this capped TTL (window = ratio × entry
/// TTL), **not** from the write-path TTL — the cap is the staleness bound
/// for L2-derived data, deliberately kept. SWR removes the cap's expiry
/// cliff instead: past ~ratio × 30 s the entry is served stale while one
/// background refresh re-executes the origin, and that refresh writes both
/// layers with the caller's full TTL. Without SWR the entry simply
/// hard-expires at the cap and the next read blocks on L2, as before.
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

// ── Stale-while-revalidate ───────────────────────────────────────────────────
//
// SWR needs an L1 to age entries in and a spawnable (`Send`) runtime for the
// background refresh — native, non-`unsync` targets with the `l1` feature.
// Everywhere else the SWR read path degrades to the plain read path and
// `SwrRead::Stale` is never produced.

/// Outcome of an SWR-aware typed read — see [`CacheKit::interop_get_swr`].
pub enum SwrRead<T> {
    /// Cache hit within the freshness window (or an L2 hit): use directly.
    Fresh(T),
    /// L1 hit past the freshness threshold but before hard expiry: use the
    /// value now, and schedule a background refresh (the `#[cachekit]` macro
    /// does this via [`CacheKit::single_flight`] + re-execution).
    Stale(T),
    /// No usable entry: fall through to a normal blocking miss + fill.
    Miss,
}

// ── CacheKit ─────────────────────────────────────────────────────────────────

/// Production-ready cache client with optional L1 in-process cache layer.
///
/// `Clone` is cheap and shares everything: backend, L1 cache, single-flight
/// state, and encryption layer. Clones exist so `'static` background work
/// (e.g. the SWR refresh spawned by `#[cachekit]`) can hold the client
/// without borrowing it.
#[derive(Clone)]
pub struct CacheKit {
    backend: SharedBackend,
    default_ttl: Duration,
    namespace: Option<String>,
    max_payload_bytes: usize,
    flight: SharedFlight,

    #[cfg(feature = "l1")]
    l1: Option<crate::l1::L1Cache>,

    #[cfg(all(feature = "l1", not(feature = "unsync"), not(target_arch = "wasm32")))]
    swr_enabled: bool,

    #[cfg(all(feature = "l1", not(feature = "unsync"), not(target_arch = "wasm32")))]
    swr_threshold_ratio: f64,

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

        #[cfg(not(feature = "unsync"))]
        let shared: SharedBackend = std::sync::Arc::new(backend);
        #[cfg(feature = "unsync")]
        let shared: SharedBackend = std::rc::Rc::new(backend);

        let mut builder = CacheKitBuilder::default()
            .backend(shared)
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
        match self.get_bytes(key).await? {
            Some(bytes) => Ok(Some(serializer::deserialize(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Retrieve and deserialize an interop-mode value stored under `key`.
    ///
    /// Identical to [`Self::get`] except the payload is decoded with
    /// [`crate::interop::deserialize`], which consumes exactly one MessagePack
    /// document and rejects trailing bytes (interop/v1 spec MUST). A
    /// Python-SDK-internal CK frame is rejected with a specific diagnostic
    /// instead of silently decoding as the integer 67.
    ///
    /// Use with keys from [`crate::interop::interop_key`] on a client
    /// **without** a namespace prefix. There is no interop-specific write
    /// method: [`Self::set`] already writes plain MessagePack (no ByteStorage
    /// envelope), which is the interop value format.
    ///
    /// # Errors
    ///
    /// Returns [`CachekitError::Config`] if the client was built with
    /// [`CacheKitBuilder::namespace`] (or `CACHEKIT_NAMESPACE`): the prefix
    /// would rewrite the storage key to `{prefix}:{interop_key}`, which no
    /// other SDK computes — every cross-SDK entry would silently miss. Interop
    /// keys carry their own namespace segment; failing loudly here beats a
    /// 100% miss rate that looks like a cold cache.
    pub async fn interop_get<T: DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<T>, CachekitError> {
        self.reject_namespaced_interop()?;
        match self.get_bytes(key).await? {
            Some(bytes) => Ok(Some(crate::interop::deserialize(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Interop keys must reach the backend verbatim; a client namespace prefix
    /// would silently produce storage keys no other SDK computes.
    fn reject_namespaced_interop(&self) -> Result<(), CachekitError> {
        match self.namespace {
            None => Ok(()),
            Some(_) => Err(CachekitError::Config(
                "interop reads require a client without a namespace prefix: .namespace() / \
                 CACHEKIT_NAMESPACE would store interop entries under {prefix}:{interop_key}, \
                 which other SDKs never compute (interop keys already carry a namespace \
                 segment) — use a dedicated non-namespaced client for interop entries"
                    .to_owned(),
            )),
        }
    }

    /// Retrieve and deserialize an interop-mode value with SWR classification.
    ///
    /// Identical to [`Self::interop_get`] except an L1 hit is classified
    /// against the client's stale-while-revalidate freshness window:
    ///
    /// - [`SwrRead::Fresh`] — L1 hit within `swr_threshold_ratio` of the
    ///   entry's TTL (±10% jitter), or any L2 hit. Use directly.
    /// - [`SwrRead::Stale`] — L1 hit past the threshold but **before hard
    ///   expiry**: the value is returned without touching the backend or
    ///   origin, and the caller should schedule exactly one background
    ///   refresh (dedup via [`Self::single_flight`] — this is what the
    ///   `#[cachekit]` macro generates).
    /// - [`SwrRead::Miss`] — nothing usable anywhere: normal blocking miss.
    ///
    /// A hard-expired L1 entry is a [`SwrRead::Miss`], never `Stale` — moka
    /// drops entries at their TTL, so SWR cannot serve past hard expiry.
    ///
    /// With SWR disabled ([`CacheKitBuilder::swr_enabled`]`(false)`), without
    /// the `l1` feature, on wasm32, or under `unsync`, this behaves exactly
    /// like [`Self::interop_get`]: hits are `Fresh`, `Stale` is never
    /// produced.
    ///
    /// # Errors
    ///
    /// Same as [`Self::interop_get`] (including the namespaced-client
    /// rejection).
    pub async fn interop_get_swr<T: DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<SwrRead<T>, CachekitError> {
        self.reject_namespaced_interop()?;
        match self.get_bytes_swr(key).await? {
            SwrRead::Fresh(b) => Ok(SwrRead::Fresh(crate::interop::deserialize(&b)?)),
            SwrRead::Stale(b) => Ok(SwrRead::Stale(crate::interop::deserialize(&b)?)),
            SwrRead::Miss => Ok(SwrRead::Miss),
        }
    }

    /// Fetch raw payload bytes with SWR classification: an L1 hit is split
    /// into fresh vs stale against the configured freshness window; on L1
    /// miss this defers to [`Self::get_bytes`] (L2 + backfill), whose hit is
    /// always fresh.
    async fn get_bytes_swr(&self, key: &str) -> Result<SwrRead<Vec<u8>>, CachekitError> {
        #[cfg(all(feature = "l1", not(feature = "unsync"), not(target_arch = "wasm32")))]
        if self.swr_enabled {
            if let Some(ref l1) = self.l1 {
                let full_key = self.resolve_key(key)?;
                match l1.get_with_swr(&full_key, self.swr_threshold_ratio) {
                    crate::l1::L1SwrRead::Fresh(bytes) => {
                        self.check_payload_size(bytes.len())?;
                        return Ok(SwrRead::Fresh(bytes));
                    }
                    crate::l1::L1SwrRead::Stale(bytes) => {
                        self.check_payload_size(bytes.len())?;
                        return Ok(SwrRead::Stale(bytes));
                    }
                    // Absent or hard-expired: fall through to the normal
                    // read path (the redundant L1 re-check there is a cheap
                    // in-process miss).
                    crate::l1::L1SwrRead::Miss => {}
                }
            }
        }

        Ok(match self.get_bytes(key).await? {
            Some(bytes) => SwrRead::Fresh(bytes),
            None => SwrRead::Miss,
        })
    }

    /// Fetch raw payload bytes for `key` (L1, then L2 with L1 backfill).
    async fn get_bytes(&self, key: &str) -> Result<Option<Vec<u8>>, CachekitError> {
        let full_key = self.resolve_key(key)?;

        // L1 hit
        #[cfg(feature = "l1")]
        if let Some(bytes) = self.l1_get(&full_key) {
            self.check_payload_size(bytes.len())?;
            return Ok(Some(bytes));
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

        Ok(Some(bytes))
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

    // ── Single-flight ─────────────────────────────────────────────────────────

    /// Begin a cold-miss single-flight for `key` (see [`crate::flight`]).
    ///
    /// Call after a cache miss, before computing the value. Concurrent
    /// in-process fills of the same key are collapsed to one; with the
    /// `reliability` feature and a lock-capable backend (CachekitIO, Redis),
    /// fills are also suppressed across processes via a distributed fill
    /// lock. The `#[cachekit]` macro does this automatically.
    ///
    /// The key is namespaced like every cache operation but not validated —
    /// this call is infallible; an invalid key simply fails later at the
    /// actual cache operation.
    pub async fn single_flight(&self, key: &str) -> crate::flight::SingleFlight {
        let full_key = self.namespaced_key(key);
        crate::flight::SingleFlight::acquire(&self.flight, &self.backend, &full_key).await
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
        let ciphertext = self.encryption.encrypt(&plaintext, key)?;
        // Size-check what is actually persisted (nonce + ciphertext + tag).
        // The get paths check the stored ciphertext length, so checking the
        // plaintext here would let a value within 28 bytes of the limit write
        // successfully and then fail EVERY subsequent read with PayloadTooLarge.
        self.client.check_payload_size(ciphertext.len())?;

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
        match self.get_plaintext(key).await? {
            Some(plaintext) => Ok(Some(serializer::deserialize(&plaintext)?)),
            None => Ok(None),
        }
    }

    /// Retrieve, decrypt, and deserialize an interop-mode value stored under `key`.
    ///
    /// Identical to [`Self::get`] except the decrypted plaintext is decoded
    /// with [`crate::interop::deserialize`] — exactly one MessagePack document,
    /// trailing bytes rejected (interop/v1 spec MUST). In interop mode the
    /// AES-GCM plaintext is the plain MessagePack value bytes, so the AAD
    /// (v0x03, `format="msgpack"`, `compressed="False"`) verifies cross-SDK
    /// unchanged.
    ///
    /// # Errors
    ///
    /// Returns [`CachekitError::Config`] on a namespace-prefixed client — see
    /// [`CacheKit::interop_get`].
    pub async fn interop_get<T: DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<T>, CachekitError> {
        self.client.reject_namespaced_interop()?;
        match self.get_plaintext(key).await? {
            Some(plaintext) => Ok(Some(crate::interop::deserialize(&plaintext)?)),
            None => Ok(None),
        }
    }

    /// Retrieve, decrypt, and deserialize an interop-mode value with SWR
    /// classification. The secure twin of [`CacheKit::interop_get_swr`]:
    /// staleness is judged on the L1 **ciphertext** entry (zero-knowledge is
    /// preserved — freshness metadata never exposes plaintext), then the
    /// value is decrypted and decoded per [`Self::interop_get`].
    ///
    /// # Errors
    ///
    /// Same as [`Self::interop_get`] — the secure path fails closed on every
    /// backend and decryption error.
    pub async fn interop_get_swr<T: DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<SwrRead<T>, CachekitError> {
        self.client.reject_namespaced_interop()?;
        match self.client.get_bytes_swr(key).await? {
            SwrRead::Fresh(ct) => Ok(SwrRead::Fresh(crate::interop::deserialize(
                &self.encryption.decrypt(&ct, key)?,
            )?)),
            SwrRead::Stale(ct) => Ok(SwrRead::Stale(crate::interop::deserialize(
                &self.encryption.decrypt(&ct, key)?,
            )?)),
            SwrRead::Miss => Ok(SwrRead::Miss),
        }
    }

    /// Fetch ciphertext (L1, then L2 with L1 backfill) and decrypt it.
    ///
    /// Ciphertext retrieval delegates to [`CacheKit::get_bytes`], which returns
    /// the stored bytes untransformed — for a secure cache exactly the AES-GCM
    /// ciphertext, so decrypt receives the same bytes the backend holds.
    async fn get_plaintext(&self, key: &str) -> Result<Option<Vec<u8>>, CachekitError> {
        match self.client.get_bytes(key).await? {
            Some(ciphertext) => Ok(Some(self.encryption.decrypt(&ciphertext, key)?)),
            None => Ok(None),
        }
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

    #[cfg(all(feature = "l1", not(feature = "unsync"), not(target_arch = "wasm32")))]
    swr_enabled: Option<bool>,

    #[cfg(all(feature = "l1", not(feature = "unsync"), not(target_arch = "wasm32")))]
    swr_threshold_ratio: Option<f64>,

    #[cfg(feature = "encryption")]
    encryption: Option<SharedEncryption>,

    #[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
    reliability: Option<crate::reliability::ReliabilityConfig>,
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

    /// Enable or disable L1 stale-while-revalidate (default: **enabled**,
    /// matching the Python and TypeScript SDKs).
    ///
    /// With SWR on, an L1 hit older than `swr_threshold_ratio` of its TTL is
    /// still served immediately, and the `#[cachekit]` macro schedules
    /// exactly one background refresh (deduplicated through
    /// [`CacheKit::single_flight`], in-process and — on lock-capable
    /// backends — across processes). A hard-expired entry is never served:
    /// it falls through to a normal blocking miss.
    ///
    /// Native targets only: this knob does not exist on wasm32, under the
    /// `unsync` feature, or without `l1` — calling it there is a compile
    /// error rather than a silent no-op.
    #[cfg(all(feature = "l1", not(feature = "unsync"), not(target_arch = "wasm32")))]
    pub fn swr_enabled(mut self, enabled: bool) -> Self {
        self.swr_enabled = Some(enabled);
        self
    }

    /// Set the SWR freshness threshold as a fraction of each L1 entry's TTL
    /// (default: **0.5**, matching the Python and TypeScript SDKs).
    ///
    /// An entry is *fresh* until it has lived `ratio × TTL` (±10% jitter to
    /// de-synchronise refreshes across processes), then *stale* — served
    /// immediately with a background refresh — until hard expiry. Mirrors
    /// cachekit-py's `swr_threshold_ratio` semantics (elapsed-lifetime
    /// fraction). Must be in `(0.0, 1.0]`; validated at [`Self::build`].
    #[cfg(all(feature = "l1", not(feature = "unsync"), not(target_arch = "wasm32")))]
    pub fn swr_threshold_ratio(mut self, ratio: f64) -> Self {
        self.swr_threshold_ratio = Some(ratio);
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

    /// Wrap the backend in the reliability stack (retry with exponential
    /// backoff + jitter, circuit breaker) — see [`crate::reliability`].
    ///
    /// Enabled by default with production settings by the `production`,
    /// `encrypted`, and `io` intent presets; off for `minimal` and for
    /// manually-built clients. To opt a preset out, pass a config with both
    /// layers `None` — an empty config applies no wrapping at all.
    #[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
    pub fn reliability(mut self, config: crate::reliability::ReliabilityConfig) -> Self {
        self.reliability = Some(config);
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

        // SWR defaults mirror the sibling SDKs: enabled, threshold ratio 0.5,
        // ratio validated in (0.0, 1.0] exactly like py's L1CacheConfig.
        #[cfg(all(feature = "l1", not(feature = "unsync"), not(target_arch = "wasm32")))]
        let swr_threshold_ratio = {
            let ratio = self.swr_threshold_ratio.unwrap_or(0.5);
            if !(ratio > 0.0 && ratio <= 1.0) {
                return Err(CachekitError::Config(format!(
                    "swr_threshold_ratio must be in (0.0, 1.0]; got {ratio}"
                )));
            }
            ratio
        };

        // Apply the reliability stack last so it decorates the final backend.
        // A config with neither layer set is the documented opt-out: skip the
        // (no-op) decorator entirely.
        #[cfg(all(feature = "reliability", not(target_arch = "wasm32")))]
        let backend = match self.reliability {
            Some(config) if config.retry.is_some() || config.circuit_breaker.is_some() => {
                crate::reliability::wrap_reliable(backend, config)
            }
            _ => backend,
        };

        Ok(CacheKit {
            backend,
            default_ttl: self.default_ttl.unwrap_or(Duration::from_secs(300)),
            namespace: self.namespace,
            max_payload_bytes: self.max_payload_bytes.unwrap_or(5 * 1024 * 1024),
            flight: SharedFlight::default(),

            #[cfg(feature = "l1")]
            l1,

            #[cfg(all(feature = "l1", not(feature = "unsync"), not(target_arch = "wasm32")))]
            swr_enabled: self.swr_enabled.unwrap_or(true),

            #[cfg(all(feature = "l1", not(feature = "unsync"), not(target_arch = "wasm32")))]
            swr_threshold_ratio,

            #[cfg(feature = "encryption")]
            encryption: self.encryption,
        })
    }
}
