use std::time::Duration;

use zeroize::Zeroizing;

use crate::error::CachekitError;

/// Maximum number of decrypt-only previous master keys.
///
/// Mirrors `cachekit_core::MAX_DECRYPT_ONLY_KEYS` (spec/encryption.md → "Key
/// Rotation (Keyring)"), which is feature-gated behind `encryption` and so
/// cannot be referenced here unconditionally. A drift-guard test in
/// `config_tests.rs` asserts the two stay equal.
pub const MAX_PREVIOUS_MASTER_KEYS: usize = 3;

// ── CachekitConfig ────────────────────────────────────────────────────────────

/// Runtime configuration for a [`crate::client::CacheKit`] instance.
pub struct CachekitConfig {
    /// API key for cachekit.io authentication.
    pub api_key: Option<Zeroizing<String>>,
    /// Base URL of the cachekit.io API.
    pub api_url: String,
    /// Master key used for zero-knowledge encryption (AES-256-GCM).
    pub master_key: Option<Zeroizing<Vec<u8>>>,
    /// Decrypt-only previous master keys retained during a rotation grace
    /// window, in attempt order. Writes always use `master_key`; reads
    /// attempt it first, then these, sequentially. At most
    /// [`MAX_PREVIOUS_MASTER_KEYS`] entries.
    pub previous_master_keys: Vec<Zeroizing<Vec<u8>>>,
    /// Default TTL for cache entries when none is specified at call site.
    pub default_ttl: Duration,
    /// Optional namespace prefix applied to all cache keys.
    pub namespace: Option<String>,
    /// Maximum number of entries in the L1 in-process cache.
    pub l1_capacity: usize,
    /// Maximum allowed payload size in bytes. Larger payloads are rejected.
    pub max_payload_bytes: usize,
}

impl std::fmt::Debug for CachekitConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let api_key_repr = if self.api_key.is_some() {
            "[REDACTED]"
        } else {
            "None"
        };
        let master_key_repr = if self.master_key.is_some() {
            "[REDACTED]"
        } else {
            "None"
        };

        f.debug_struct("CachekitConfig")
            .field("api_key", &api_key_repr)
            .field("api_url", &self.api_url)
            .field("master_key", &master_key_repr)
            .field(
                "previous_master_keys",
                &format_args!("[REDACTED; {}]", self.previous_master_keys.len()),
            )
            .field("default_ttl", &self.default_ttl)
            .field("namespace", &self.namespace)
            .field("l1_capacity", &self.l1_capacity)
            .field("max_payload_bytes", &self.max_payload_bytes)
            .finish()
    }
}

impl Default for CachekitConfig {
    fn default() -> Self {
        Self {
            api_key: None,
            api_url: "https://api.cachekit.io".to_owned(),
            master_key: None,
            previous_master_keys: Vec::new(),
            default_ttl: Duration::from_secs(300),
            namespace: None,
            l1_capacity: 1000,
            max_payload_bytes: 5 * 1024 * 1024, // 5 MiB
        }
    }
}

impl CachekitConfig {
    /// Build configuration from environment variables.
    ///
    /// | Variable | Description |
    /// |---|---|
    /// | `CACHEKIT_API_KEY` | API key for cachekit.io |
    /// | `CACHEKIT_API_URL` | Override API base URL (must be HTTPS) |
    /// | `CACHEKIT_MASTER_KEY` | Hex-encoded master key (min 32 bytes) |
    /// | `CACHEKIT_PREVIOUS_MASTER_KEYS` | Comma-separated hex-encoded decrypt-only previous master keys (max 3) |
    /// | `CACHEKIT_DEFAULT_TTL` | Default TTL in seconds (min 1) |
    pub fn from_env() -> Result<Self, CachekitError> {
        let mut config = Self::default();

        // API key
        if let Ok(val) = std::env::var("CACHEKIT_API_KEY") {
            config.api_key = Some(Zeroizing::new(val));
        }

        // API URL — must be HTTPS
        if let Ok(val) = std::env::var("CACHEKIT_API_URL") {
            validate_https(&val)?;
            config.api_url = val;
        }

        // Master key — hex-decode and validate length >= 32 bytes
        if let Ok(val) = std::env::var("CACHEKIT_MASTER_KEY") {
            let bytes = decode_master_key_hex(&val, "CACHEKIT_MASTER_KEY")?;
            config.master_key = Some(Zeroizing::new(bytes));
        }

        // Previous master keys — comma-separated hex, decrypt-only, max 3.
        if let Ok(val) = std::env::var("CACHEKIT_PREVIOUS_MASTER_KEYS") {
            let mut previous = Vec::new();
            for entry in val.split(',') {
                let entry = entry.trim();
                if entry.is_empty() {
                    return Err(CachekitError::Config(
                        "CACHEKIT_PREVIOUS_MASTER_KEYS contains an empty entry".to_owned(),
                    ));
                }
                previous.push(Zeroizing::new(decode_master_key_hex(
                    entry,
                    "CACHEKIT_PREVIOUS_MASTER_KEYS entry",
                )?));
            }
            // Previous keys without a current key is a broken rotation
            // deploy: nothing would ever consume them, and the operator
            // would only find out at the first secure() call. Fail at load.
            if config.master_key.is_none() {
                return Err(CachekitError::Config(
                    "CACHEKIT_PREVIOUS_MASTER_KEYS requires CACHEKIT_MASTER_KEY to be set"
                        .to_owned(),
                ));
            }
            validate_previous_master_keys(
                config.master_key.as_deref().map(Vec::as_slice),
                &previous,
            )?;
            config.previous_master_keys = previous;
        }

        // Default TTL — minimum 1 second
        if let Ok(val) = std::env::var("CACHEKIT_DEFAULT_TTL") {
            let secs: u64 = val.parse().map_err(|e| {
                CachekitError::Config(format!("CACHEKIT_DEFAULT_TTL must be an integer: {e}"))
            })?;
            if secs < 1 {
                return Err(CachekitError::Config(
                    "CACHEKIT_DEFAULT_TTL must be at least 1 second".to_owned(),
                ));
            }
            config.default_ttl = Duration::from_secs(secs);
        }

        Ok(config)
    }
}

// ── CachekitConfigBuilder ─────────────────────────────────────────────────────

/// Fluent builder for [`CachekitConfig`].
#[derive(Default)]
#[must_use]
pub struct CachekitConfigBuilder {
    inner: CachekitConfig,
}

impl CachekitConfigBuilder {
    /// Create a new builder with defaults.
    pub fn new() -> Self {
        Self {
            inner: CachekitConfig::default(),
        }
    }

    /// Set the API key.
    pub fn api_key(mut self, key: impl Into<String>) -> Self {
        self.inner.api_key = Some(Zeroizing::new(key.into()));
        self
    }

    /// Set the API base URL. Must use HTTPS.
    pub fn api_url(mut self, url: impl Into<String>) -> Result<Self, CachekitError> {
        let url = url.into();
        validate_https(&url)?;
        self.inner.api_url = url;
        Ok(self)
    }

    /// Set the master key from a hex string. Must decode to at least 32 bytes.
    pub fn master_key(mut self, hex_key: &str) -> Result<Self, CachekitError> {
        let bytes = decode_master_key_hex(hex_key, "master_key")?;
        validate_previous_master_keys(Some(bytes.as_slice()), &self.inner.previous_master_keys)?;
        self.inner.master_key = Some(Zeroizing::new(bytes));
        Ok(self)
    }

    /// Set decrypt-only previous master keys from hex strings, in attempt
    /// order. Retained during a key-rotation grace window: reads attempt the
    /// current master key first, then each of these sequentially.
    ///
    /// Validation is identical to [`Self::master_key`] per entry (valid hex,
    /// at least 32 bytes). At most [`MAX_PREVIOUS_MASTER_KEYS`] entries —
    /// more is a [`CachekitError::Config`], never truncated. The current
    /// master key must not reappear here (forward-only rotation: a retired
    /// key is never re-promoted).
    ///
    /// # Examples
    ///
    /// ```
    /// use cachekit::config::CachekitConfigBuilder;
    ///
    /// // k2 is current after rotation; k1 stays readable during the grace window.
    /// let k1 = "11".repeat(32);
    /// let k2 = "22".repeat(32);
    ///
    /// let config = CachekitConfigBuilder::new()
    ///     .master_key(&k2)?
    ///     .previous_master_keys(&[k1.as_str()])?
    ///     .build();
    ///
    /// assert_eq!(config.previous_master_keys.len(), 1);
    /// # Ok::<(), cachekit::CachekitError>(())
    /// ```
    pub fn previous_master_keys(mut self, hex_keys: &[&str]) -> Result<Self, CachekitError> {
        let mut previous = Vec::with_capacity(hex_keys.len());
        for hex_key in hex_keys {
            previous.push(Zeroizing::new(decode_master_key_hex(
                hex_key,
                "previous_master_keys entry",
            )?));
        }
        validate_previous_master_keys(
            self.inner.master_key.as_deref().map(Vec::as_slice),
            &previous,
        )?;
        self.inner.previous_master_keys = previous;
        Ok(self)
    }

    /// Set the default TTL. Must be at least 1 second.
    pub fn default_ttl(mut self, ttl: Duration) -> Result<Self, CachekitError> {
        if ttl < Duration::from_secs(1) {
            return Err(CachekitError::Config(
                "default_ttl must be at least 1 second".to_owned(),
            ));
        }
        self.inner.default_ttl = ttl;
        Ok(self)
    }

    /// Set the namespace prefix.
    pub fn namespace(mut self, ns: impl Into<String>) -> Self {
        self.inner.namespace = Some(ns.into());
        self
    }

    /// Set the L1 cache capacity (max entries).
    pub fn l1_capacity(mut self, capacity: usize) -> Self {
        self.inner.l1_capacity = capacity;
        self
    }

    /// Finalise and return the [`CachekitConfig`].
    pub fn build(self) -> CachekitConfig {
        self.inner
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Hex-decode a master key and require at least 32 bytes. Shared by the
/// current-key and previous-key paths so validation cannot drift.
fn decode_master_key_hex(hex_key: &str, what: &str) -> Result<Vec<u8>, CachekitError> {
    let bytes = hex::decode(hex_key)
        .map_err(|e| CachekitError::Config(format!("{what} is not valid hex: {e}")))?;
    if bytes.len() < 32 {
        return Err(CachekitError::Config(format!(
            "{what} must be at least 32 bytes (64 hex chars); got {} bytes",
            bytes.len()
        )));
    }
    Ok(bytes)
}

/// Enforce the keyring config invariants: at most [`MAX_PREVIOUS_MASTER_KEYS`]
/// previous keys (rejected, never truncated), and the current master key must
/// not also appear in the previous list (the detectable subset of the
/// forward-only rotation rule — re-promoting a retired key would resume a
/// used, unknowable AES-GCM nonce budget).
///
/// Fail-fast mirror of the checks `cachekit_core::Keyring::new` repeats at
/// client build time; plain equality is fine — both operands are
/// operator-supplied configuration, not secrets under timing attack.
fn validate_previous_master_keys(
    master_key: Option<&[u8]>,
    previous: &[Zeroizing<Vec<u8>>],
) -> Result<(), CachekitError> {
    if previous.len() > MAX_PREVIOUS_MASTER_KEYS {
        return Err(CachekitError::Config(format!(
            "previous_master_keys accepts at most {MAX_PREVIOUS_MASTER_KEYS} entries; got {}",
            previous.len()
        )));
    }
    if let Some(master) = master_key {
        if previous.iter().any(|key| key.as_slice() == master) {
            return Err(CachekitError::Config(
                "the current master key must not appear in previous_master_keys \
                 (rotation is forward-only; retired keys are never re-promoted)"
                    .to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_https(url: &str) -> Result<(), CachekitError> {
    if !url.starts_with("https://") {
        return Err(CachekitError::Config(format!(
            "API URL must use HTTPS; got: {url}"
        )));
    }
    Ok(())
}
