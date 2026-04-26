use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use zeroize::Zeroizing;

use crate::backend::{Backend, HealthStatus};
use crate::error::{BackendError, BackendErrorKind};
use crate::metrics::{metrics_headers, MetricsProvider};
use crate::session::session_headers;
use crate::url_validator::validate_cachekitio_url;

// ── CachekitIO ────────────────────────────────────────────────────────────────

/// HTTP backend that talks to the cachekit.io SaaS API.
pub struct CachekitIO {
    client: reqwest::Client,
    api_key: Zeroizing<String>,
    api_url: String,
    metrics_provider: Option<MetricsProvider>,
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

    /// Return a reference to the underlying HTTP client.
    pub(crate) fn client(&self) -> &reqwest::Client {
        &self.client
    }

    /// Return the API key as a string slice (for bearer auth in sibling modules).
    pub(crate) fn api_key_str(&self) -> &str {
        self.api_key.as_str()
    }

    /// Return a reference to the optional metrics provider (for testing/introspection).
    #[allow(dead_code)]
    pub(crate) fn metrics_provider(&self) -> Option<&MetricsProvider> {
        self.metrics_provider.as_ref()
    }

    /// Build the full URL for a cache key path segment.
    ///
    /// Keys are percent-encoded so that slashes or special characters in the
    /// cache key do not break the URL structure.
    fn url(&self, key: &str) -> String {
        let encoded = urlencoding::encode(key);
        format!("{}/v1/cache/{}", self.api_url, encoded)
    }

    /// Build the health-check URL.
    fn health_url(&self) -> String {
        format!("{}/v1/cache/health", self.api_url)
    }

    /// Apply standard session and metrics headers to a request builder.
    pub(crate) fn with_standard_headers(
        &self,
        mut req: reqwest::RequestBuilder,
    ) -> reqwest::RequestBuilder {
        for (name, value) in session_headers() {
            req = req.header(name, value);
        }
        for (name, value) in metrics_headers(self.metrics_provider.as_ref()) {
            req = req.header(name, value);
        }
        req
    }

    /// Read error body from response and build a sanitized BackendError.
    pub(crate) async fn error_from_response(&self, resp: reqwest::Response) -> BackendError {
        let status = resp.status().as_u16();
        let body = resp.bytes().await.unwrap_or_default();
        from_http_status_sanitized(status, &body, self.api_key.as_str())
    }
}

// ── Error helpers ────────────────────────────────────────────────────────────

/// Convert a reqwest error into a BackendError, sanitizing the API key from the message.
pub(crate) fn reqwest_err_sanitized(e: reqwest::Error, api_key: &str) -> BackendError {
    let kind = if e.is_timeout() {
        BackendErrorKind::Timeout
    } else {
        BackendErrorKind::Transient
    };
    BackendError {
        kind,
        message: BackendError::sanitize_message(&e.to_string(), api_key),
        source: Some(Box::new(e)),
    }
}

/// Build a [`BackendError`] from an HTTP status + body, sanitizing the API key from output.
pub(crate) fn from_http_status_sanitized(status: u16, body: &[u8], api_key: &str) -> BackendError {
    let sanitized =
        BackendError::sanitize_message(std::str::from_utf8(body).unwrap_or(""), api_key);
    BackendError::from_http_status(status, sanitized.as_bytes())
}

// ── Backend impl ──────────────────────────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
#[cfg_attr(not(feature = "unsync"), async_trait)]
#[cfg_attr(feature = "unsync", async_trait(?Send))]
impl Backend for CachekitIO {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        let req = self.with_standard_headers(
            self.client
                .get(self.url(key))
                .bearer_auth(self.api_key.as_str()),
        );

        let resp = req
            .send()
            .await
            .map_err(|e| reqwest_err_sanitized(e, self.api_key.as_str()))?;

        match resp.status().as_u16() {
            200 => {
                let bytes = resp
                    .bytes()
                    .await
                    .map_err(|e| reqwest_err_sanitized(e, self.api_key.as_str()))?;
                Ok(Some(bytes.to_vec()))
            }
            404 => Ok(None),
            _ => Err(self.error_from_response(resp).await),
        }
    }

    async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        let mut req = self
            .client
            .put(self.url(key))
            .bearer_auth(self.api_key.as_str())
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .body(value);

        if let Some(ttl) = ttl {
            req = req.header("X-TTL", ttl.as_secs().to_string());
        }

        let req = self.with_standard_headers(req);

        let resp = req
            .send()
            .await
            .map_err(|e| reqwest_err_sanitized(e, self.api_key.as_str()))?;

        let status = resp.status().as_u16();
        if (200..300).contains(&status) {
            Ok(())
        } else {
            Err(self.error_from_response(resp).await)
        }
    }

    async fn delete(&self, key: &str) -> Result<bool, BackendError> {
        let req = self.with_standard_headers(
            self.client
                .delete(self.url(key))
                .bearer_auth(self.api_key.as_str()),
        );

        let resp = req
            .send()
            .await
            .map_err(|e| reqwest_err_sanitized(e, self.api_key.as_str()))?;

        match resp.status().as_u16() {
            200 | 204 => Ok(true),
            404 => Ok(false),
            _ => Err(self.error_from_response(resp).await),
        }
    }

    async fn exists(&self, key: &str) -> Result<bool, BackendError> {
        let req = self.with_standard_headers(
            self.client
                .head(self.url(key))
                .bearer_auth(self.api_key.as_str()),
        );

        let resp = req
            .send()
            .await
            .map_err(|e| reqwest_err_sanitized(e, self.api_key.as_str()))?;

        match resp.status().as_u16() {
            200 => Ok(true),
            404 => Ok(false),
            status => Err(BackendError::from_http_status(status, &[])),
        }
    }

    async fn health(&self) -> Result<HealthStatus, BackendError> {
        let start = std::time::Instant::now();

        let req = self.with_standard_headers(
            self.client
                .get(self.health_url())
                .bearer_auth(self.api_key.as_str()),
        );

        let resp = req
            .send()
            .await
            .map_err(|e| reqwest_err_sanitized(e, self.api_key.as_str()))?;

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
            Err(self.error_from_response(resp).await)
        }
    }
}

// ── Builder ───────────────────────────────────────────────────────────────────

/// Builder for [`CachekitIO`].
#[derive(Default)]
#[must_use]
pub struct CachekitIOBuilder {
    api_key: Option<Zeroizing<String>>,
    api_url: Option<String>,
    allow_custom_host: bool,
    metrics_provider: Option<MetricsProvider>,
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

    /// Allow non-standard hostnames (e.g. custom proxies). Private IPs are still blocked.
    pub fn allow_custom_host(mut self, allow: bool) -> Self {
        self.allow_custom_host = allow;
        self
    }

    /// Provide L1 cache metrics for request telemetry headers.
    pub fn metrics_provider(mut self, provider: MetricsProvider) -> Self {
        self.metrics_provider = Some(provider);
        self
    }

    /// Consume the builder and construct a [`CachekitIO`].
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `api_key` was not set.
    /// - the resolved URL scheme is not `https`.
    /// - the URL hostname is not permitted (see [`validate_cachekitio_url`]).
    pub fn build(self) -> Result<CachekitIO, crate::error::CachekitError> {
        use crate::error::CachekitError;

        let api_key = self
            .api_key
            .filter(|k| !k.is_empty())
            .ok_or_else(|| CachekitError::Config("api_key is required".to_string()))?;

        let api_url = self
            .api_url
            .unwrap_or_else(|| "https://api.cachekit.io".to_string());

        // Validate URL: HTTPS, allowed host, no private IPs.
        validate_cachekitio_url(&api_url, self.allow_custom_host)?;

        // Trim trailing slash once so url()/health_url() don't repeat it per-request.
        let api_url = api_url.trim_end_matches('/').to_string();

        let client = reqwest::Client::builder();

        #[cfg(not(target_arch = "wasm32"))]
        let client = client
            .use_rustls_tls()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10));

        let client = client
            .build()
            .map_err(|e| CachekitError::Config(format!("failed to build HTTP client: {e}")))?;

        Ok(CachekitIO {
            client,
            api_key,
            api_url,
            metrics_provider: self.metrics_provider,
        })
    }
}
