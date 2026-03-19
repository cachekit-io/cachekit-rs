//! [`TtlInspectable`] implementation for the cachekit.io HTTP backend.

use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::cachekitio::{from_http_status_sanitized, reqwest_err_sanitized, CachekitIO};
use super::TtlInspectable;
use crate::error::BackendError;

#[derive(Deserialize)]
struct TtlResponse {
    ttl: Option<u64>,
}

#[derive(Serialize)]
struct RefreshTtlRequest {
    ttl: u64,
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl TtlInspectable for CachekitIO {
    async fn ttl(&self, key: &str) -> Result<Option<Duration>, BackendError> {
        let url = format!(
            "{}/v1/cache/{}/ttl",
            self.api_url().trim_end_matches('/'),
            urlencoding::encode(key)
        );

        let mut req = self.client().get(&url).bearer_auth(self.api_key_str());

        for (name, value) in crate::session::session_headers() {
            req = req.header(name, value);
        }
        for (name, value) in crate::metrics::metrics_headers(self.metrics_provider()) {
            req = req.header(name, value);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| reqwest_err_sanitized(e, self.api_key_str()))?;

        match resp.status().as_u16() {
            200 => {
                let body: TtlResponse = resp.json().await.map_err(|e| {
                    BackendError::transient(format!("failed to parse TTL response: {e}"))
                })?;
                Ok(body.ttl.map(Duration::from_secs))
            }
            404 => Ok(None),
            status => {
                let body = resp.bytes().await.unwrap_or_default();
                Err(from_http_status_sanitized(
                    status,
                    &body,
                    self.api_key_str(),
                ))
            }
        }
    }

    async fn refresh_ttl(&self, key: &str, ttl: Duration) -> Result<bool, BackendError> {
        let url = format!(
            "{}/v1/cache/{}/ttl",
            self.api_url().trim_end_matches('/'),
            urlencoding::encode(key)
        );

        let body = serde_json::to_vec(&RefreshTtlRequest { ttl: ttl.as_secs() }).map_err(|e| {
            BackendError::permanent(format!("failed to serialize refresh_ttl request: {e}"))
        })?;

        let mut req = self
            .client()
            .patch(&url)
            .bearer_auth(self.api_key_str())
            .header("Content-Type", "application/json")
            .body(body);

        for (name, value) in crate::session::session_headers() {
            req = req.header(name, value);
        }
        for (name, value) in crate::metrics::metrics_headers(self.metrics_provider()) {
            req = req.header(name, value);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| reqwest_err_sanitized(e, self.api_key_str()))?;

        match resp.status().as_u16() {
            200 | 204 => Ok(true),
            404 => Ok(false),
            status => {
                let body_bytes = resp.bytes().await.unwrap_or_default();
                Err(from_http_status_sanitized(
                    status,
                    &body_bytes,
                    self.api_key_str(),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Compile-time proof that CachekitIO implements TtlInspectable.
    fn _assert_ttl_inspectable(_b: &dyn TtlInspectable) {}

    #[test]
    fn cachekitio_is_ttl_inspectable() {
        fn _check(backend: &CachekitIO) {
            _assert_ttl_inspectable(backend);
        }
    }
}
