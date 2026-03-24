use std::time::Duration;

use zeroize::Zeroizing;

use crate::error::CachekitError;

// ── CachekitConfig ────────────────────────────────────────────────────────────

/// Runtime configuration for a [`crate::client::CacheKit`] instance.
pub struct CachekitConfig {
    /// API key for cachekit.io authentication.
    pub api_key: Option<Zeroizing<String>>,
    /// Base URL of the cachekit.io API.
    pub api_url: String,
    /// Master key used for zero-knowledge encryption (AES-256-GCM).
    pub master_key: Option<Zeroizing<Vec<u8>>>,
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
            let bytes = hex::decode(&val).map_err(|e| {
                CachekitError::Config(format!("CACHEKIT_MASTER_KEY is not valid hex: {e}"))
            })?;
            if bytes.len() < 32 {
                return Err(CachekitError::Config(format!(
                    "CACHEKIT_MASTER_KEY must be at least 32 bytes ({} hex chars); got {} bytes",
                    64,
                    bytes.len()
                )));
            }
            config.master_key = Some(Zeroizing::new(bytes));
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
        let bytes = hex::decode(hex_key)
            .map_err(|e| CachekitError::Config(format!("master_key is not valid hex: {e}")))?;
        if bytes.len() < 32 {
            return Err(CachekitError::Config(format!(
                "master_key must be at least 32 bytes; got {}",
                bytes.len()
            )));
        }
        self.inner.master_key = Some(Zeroizing::new(bytes));
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

fn validate_https(url: &str) -> Result<(), CachekitError> {
    if !url.starts_with("https://") {
        return Err(CachekitError::Config(format!(
            "API URL must use HTTPS; got: {url}"
        )));
    }
    Ok(())
}
