//! Cloudflare Workers backend using the `worker::Fetch` API.
//!
//! This module is only compiled for `wasm32` targets (`--features workers`).
//! It uses `#[async_trait(?Send)]` because the Workers runtime is
//! single-threaded and `worker::Fetch` futures are `!Send`.

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use zeroize::Zeroizing;

use crate::backend::{Backend, HealthStatus};
use crate::error::BackendError;

// ── WorkersCachekitIO ────────────────────────────────────────────────────────

/// HTTP backend for cachekit.io that uses `worker::Fetch` instead of `reqwest`.
///
/// Designed for use inside Cloudflare Workers where the standard networking
/// stack is unavailable and `worker::Fetch` is the only HTTP primitive.
pub struct WorkersCachekitIO {
    api_key: Zeroizing<String>,
    api_url: String,
}

/// Redact `api_key` from debug output.
impl std::fmt::Debug for WorkersCachekitIO {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkersCachekitIO")
            .field("api_url", &self.api_url)
            .field("api_key", &"<redacted>")
            .finish()
    }
}

impl WorkersCachekitIO {
    /// Start building a [`WorkersCachekitIO`] instance.
    pub fn builder() -> WorkersCachekitIOBuilder {
        WorkersCachekitIOBuilder::default()
    }

    /// Return the configured API URL.
    pub fn api_url(&self) -> &str {
        &self.api_url
    }

    /// Build the full URL for a cache key path segment.
    fn url(&self, key: &str) -> String {
        let encoded = urlencoding::encode(key);
        format!("{}/v1/cache/{}", self.api_url.trim_end_matches('/'), encoded)
    }

    /// Build the health-check URL.
    fn health_url(&self) -> String {
        format!("{}/v1/health", self.api_url.trim_end_matches('/'))
    }

    /// Execute a fetch request with the given method, URL, optional body, and extra headers.
    async fn fetch(
        &self,
        method: &str,
        url: &str,
        body: Option<Vec<u8>>,
        extra_headers: Vec<(&str, String)>,
    ) -> Result<worker::Response, BackendError> {
        let mut headers = worker::Headers::new();
        headers
            .set("Authorization", &format!("Bearer {}", self.api_key.as_str()))
            .map_err(|e| BackendError::permanent(format!("failed to set auth header: {e}")))?;

        for (name, value) in extra_headers {
            headers
                .set(name, &value)
                .map_err(|e| BackendError::permanent(format!("failed to set header {name}: {e}")))?;
        }

        let mut init = worker::RequestInit::new();
        init.with_method(match method {
            "GET" => worker::Method::Get,
            "PUT" => worker::Method::Put,
            "DELETE" => worker::Method::Delete,
            "HEAD" => worker::Method::Head,
            _ => worker::Method::Get,
        });
        init.with_headers(headers);

        if let Some(bytes) = body {
            let js_array = js_sys::Uint8Array::from(bytes.as_slice());
            init.with_body(Some(js_array.into()));
        }

        let request = worker::Request::new_with_init(url, &init)
            .map_err(|e| BackendError::transient(format!("failed to build request: {e}")))?;

        worker::Fetch::Request(request)
            .send()
            .await
            .map_err(|e| BackendError::transient(format!("fetch failed: {e}")))
    }
}

// ── Backend impl (wasm32 only) ───────────────────────────────────────────────

#[async_trait(?Send)]
impl Backend for WorkersCachekitIO {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        let mut resp = self.fetch("GET", &self.url(key), None, vec![]).await?;

        match resp.status_code() {
            200 => {
                let bytes = resp
                    .bytes()
                    .await
                    .map_err(|e| BackendError::transient(format!("failed to read body: {e}")))?;
                Ok(Some(bytes))
            }
            404 => Ok(None),
            status => {
                let body = resp.bytes().await.unwrap_or_default();
                Err(BackendError::from_http_status(status, &body))
            }
        }
    }

    async fn set(&self, key: &str, value: Vec<u8>, ttl: Option<Duration>) -> Result<(), BackendError> {
        let mut headers = vec![("Content-Type", "application/octet-stream".to_owned())];
        if let Some(ttl) = ttl {
            headers.push(("X-Cache-TTL", ttl.as_secs().to_string()));
        }

        let mut resp = self.fetch("PUT", &self.url(key), Some(value), headers).await?;
        let status = resp.status_code();

        if (200..300).contains(&status) {
            Ok(())
        } else {
            let body = resp.bytes().await.unwrap_or_default();
            Err(BackendError::from_http_status(status, &body))
        }
    }

    async fn delete(&self, key: &str) -> Result<bool, BackendError> {
        let mut resp = self.fetch("DELETE", &self.url(key), None, vec![]).await?;

        match resp.status_code() {
            200 | 204 => Ok(true),
            404 => Ok(false),
            status => {
                let body = resp.bytes().await.unwrap_or_default();
                Err(BackendError::from_http_status(status, &body))
            }
        }
    }

    async fn exists(&self, key: &str) -> Result<bool, BackendError> {
        let resp = self.fetch("HEAD", &self.url(key), None, vec![]).await?;

        match resp.status_code() {
            200 => Ok(true),
            404 => Ok(false),
            status => Err(BackendError::from_http_status(status, &[])),
        }
    }

    async fn health(&self) -> Result<HealthStatus, BackendError> {
        let mut resp = self.fetch("GET", &self.health_url(), None, vec![]).await?;
        let status = resp.status_code();

        if (200..300).contains(&status) {
            let mut details = HashMap::new();
            details.insert("http_status".to_string(), status.to_string());
            Ok(HealthStatus {
                is_healthy: true,
                latency_ms: 0.0,
                backend_type: "workers-cachekitio".to_string(),
                details,
            })
        } else {
            let body = resp.bytes().await.unwrap_or_default();
            Err(BackendError::from_http_status(status, &body))
        }
    }
}

// ── Builder ──────────────────────────────────────────────────────────────────

/// Builder for [`WorkersCachekitIO`].
#[derive(Default)]
pub struct WorkersCachekitIOBuilder {
    api_key: Option<String>,
    api_url: Option<String>,
}

impl WorkersCachekitIOBuilder {
    /// Set the API key (required).
    pub fn api_key(mut self, key: impl Into<String>) -> Self {
        self.api_key = Some(key.into());
        self
    }

    /// Override the API base URL (default: `https://api.cachekit.io`).
    pub fn api_url(mut self, url: impl Into<String>) -> Self {
        self.api_url = Some(url.into());
        self
    }

    /// Consume the builder and construct a [`WorkersCachekitIO`].
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `api_key` was not set or is empty.
    /// - the resolved URL scheme is not `https`.
    pub fn build(self) -> Result<WorkersCachekitIO, crate::error::CachekitError> {
        use crate::error::CachekitError;

        let api_key = self
            .api_key
            .filter(|k| !k.is_empty())
            .ok_or_else(|| CachekitError::Config("api_key is required".to_string()))?;

        let api_url = self
            .api_url
            .unwrap_or_else(|| "https://api.cachekit.io".to_string());

        // Enforce HTTPS — plaintext HTTP must never transmit API keys.
        if !api_url.starts_with("https://") {
            return Err(CachekitError::Config(format!(
                "api_url must use HTTPS, got: {}",
                api_url
            )));
        }

        Ok(WorkersCachekitIO {
            api_key: Zeroizing::new(api_key),
            api_url,
        })
    }
}
