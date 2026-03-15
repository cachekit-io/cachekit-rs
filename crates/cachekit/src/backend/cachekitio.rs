use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use zeroize::Zeroizing;

use crate::backend::{Backend, HealthStatus};
use crate::error::{BackendError, BackendErrorKind};

// ── CachekitIO ────────────────────────────────────────────────────────────────

/// HTTP backend that talks to the cachekit.io SaaS API.
pub struct CachekitIO {
    client: reqwest::Client,
    api_key: Zeroizing<String>,
    api_url: String,
}

/// Redact `api_key` from debug output.
impl std::fmt::Debug for CachekitIO {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachekitIO")
            .field("api_url", &self.api_url)
            .field("api_key", &"<redacted>")
            .finish()
    }
}

impl CachekitIO {
    /// Start building a [`CachekitIO`] instance.
    pub fn builder() -> CachekitIOBuilder {
        CachekitIOBuilder::default()
    }

    /// Return the configured API URL (useful in tests / introspection).
    pub fn api_url(&self) -> &str {
        &self.api_url
    }

    /// Build the full URL for a cache key path segment.
    ///
    /// Keys are percent-encoded so that slashes or special characters in the
    /// cache key do not break the URL structure.
    fn url(&self, key: &str) -> String {
        let encoded = urlencoding::encode(key);
        format!("{}/v1/cache/{}", self.api_url.trim_end_matches('/'), encoded)
    }

    /// Build the health-check URL.
    fn health_url(&self) -> String {
        format!("{}/v1/health", self.api_url.trim_end_matches('/'))
    }
}

// ── Error helpers ────────────────────────────────────────────────────────────

/// Convert a reqwest error into a BackendError, preserving the source.
fn reqwest_err(e: reqwest::Error) -> BackendError {
    let kind = if e.is_timeout() {
        BackendErrorKind::Timeout
    } else {
        BackendErrorKind::Transient
    };
    BackendError { kind, message: e.to_string(), source: Some(Box::new(e)) }
}

// ── Backend impl ──────────────────────────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl Backend for CachekitIO {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        let resp = self
            .client
            .get(self.url(key))
            .bearer_auth(self.api_key.as_str())
            .send()
            .await
            .map_err(reqwest_err)?;

        match resp.status().as_u16() {
            200 => {
                let bytes = resp.bytes().await.map_err(reqwest_err)?;
                Ok(Some(bytes.to_vec()))
            }
            404 => Ok(None),
            status => {
                let body = resp.bytes().await.unwrap_or_default();
                Err(BackendError::from_http_status(status, &body))
            }
        }
    }

    async fn set(&self, key: &str, value: Vec<u8>, ttl: Option<Duration>) -> Result<(), BackendError> {
        let mut req = self
            .client
            .put(self.url(key))
            .bearer_auth(self.api_key.as_str())
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .body(value);

        if let Some(ttl) = ttl {
            req = req.header("X-Cache-TTL", ttl.as_secs().to_string());
        }

        let resp = req.send().await.map_err(reqwest_err)?;

        let status = resp.status().as_u16();
        if (200..300).contains(&status) {
            Ok(())
        } else {
            let body = resp.bytes().await.unwrap_or_default();
            Err(BackendError::from_http_status(status, &body))
        }
    }

    async fn delete(&self, key: &str) -> Result<bool, BackendError> {
        let resp = self
            .client
            .delete(self.url(key))
            .bearer_auth(self.api_key.as_str())
            .send()
            .await
            .map_err(reqwest_err)?;

        match resp.status().as_u16() {
            200 | 204 => Ok(true),
            404 => Ok(false),
            status => {
                let body = resp.bytes().await.unwrap_or_default();
                Err(BackendError::from_http_status(status, &body))
            }
        }
    }

    async fn exists(&self, key: &str) -> Result<bool, BackendError> {
        let resp = self
            .client
            .head(self.url(key))
            .bearer_auth(self.api_key.as_str())
            .send()
            .await
            .map_err(reqwest_err)?;

        match resp.status().as_u16() {
            200 => Ok(true),
            404 => Ok(false),
            status => {
                Err(BackendError::from_http_status(status, &[]))
            }
        }
    }

    async fn health(&self) -> Result<HealthStatus, BackendError> {
        let start = std::time::Instant::now();

        let resp = self
            .client
            .get(self.health_url())
            .bearer_auth(self.api_key.as_str())
            .send()
            .await
            .map_err(reqwest_err)?;

        let latency = start.elapsed();
        let status = resp.status().as_u16();

        if (200..300).contains(&status) {
            let mut details = HashMap::new();
            details.insert("http_status".to_string(), status.to_string());
            Ok(HealthStatus {
                is_healthy: true,
                latency_ms: latency.as_secs_f64() * 1000.0,
                backend_type: "cachekitio".to_string(),
                details,
            })
        } else {
            let body = resp.bytes().await.unwrap_or_default();
            Err(BackendError::from_http_status(status, &body))
        }
    }
}

// ── Builder ───────────────────────────────────────────────────────────────────

/// Builder for [`CachekitIO`].
#[derive(Default)]
pub struct CachekitIOBuilder {
    api_key: Option<Zeroizing<String>>,
    api_url: Option<String>,
}

impl CachekitIOBuilder {
    /// Set the API key (required).
    pub fn api_key(mut self, key: impl Into<String>) -> Self {
        self.api_key = Some(Zeroizing::new(key.into()));
        self
    }

    /// Override the API base URL (default: `https://api.cachekit.io`).
    pub fn api_url(mut self, url: impl Into<String>) -> Self {
        self.api_url = Some(url.into());
        self
    }

    /// Consume the builder and construct a [`CachekitIO`].
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `api_key` was not set.
    /// - the resolved URL scheme is not `https`.
    pub fn build(self) -> Result<CachekitIO, crate::error::CachekitError> {
        use crate::error::CachekitError;

        let api_key = self
            .api_key
            .filter(|k| !k.is_empty())
            .ok_or_else(|| CachekitError::Config("api_key is required".to_string()))?;
        // api_key is already Zeroizing<String> from the builder — no re-wrapping needed.

        let api_url = self.api_url.unwrap_or_else(|| "https://api.cachekit.io".to_string());

        // Enforce HTTPS — plaintext HTTP must never transmit API keys or cached data.
        if !api_url.starts_with("https://") {
            return Err(CachekitError::Config(format!(
                "api_url must use HTTPS, got: {}",
                api_url
            )));
        }

        let client = reqwest::Client::builder()
            .use_rustls_tls()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| CachekitError::Config(format!("failed to build HTTP client: {e}")))?;

        Ok(CachekitIO { client, api_key, api_url })
    }
}
