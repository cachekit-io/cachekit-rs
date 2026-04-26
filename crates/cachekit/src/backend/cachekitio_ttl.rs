//! [`TtlInspectable`] implementation for the cachekit.io HTTP backend.

use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::cachekitio::{reqwest_err_sanitized, CachekitIO};
use super::TtlInspectable;
use crate::error::BackendError;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TtlResponse {
    ttl: Option<u64>,
}

#[derive(Serialize)]
struct RefreshTtlRequest {
    ttl: u64,
}

#[cfg(not(target_arch = "wasm32"))]
#[cfg_attr(not(feature = "unsync"), async_trait)]
#[cfg_attr(feature = "unsync", async_trait(?Send))]
impl TtlInspectable for CachekitIO {
    async fn ttl(&self, key: &str) -> Result<Option<Duration>, BackendError> {
        let url = format!(
            "{}/v1/cache/{}/ttl",
            self.api_url(),
            urlencoding::encode(key)
        );

        let req =
            self.with_standard_headers(self.client().get(&url).bearer_auth(self.api_key_str()));

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
            _ => Err(self.error_from_response(resp).await),
        }
    }

    async fn refresh_ttl(&self, key: &str, ttl: Duration) -> Result<bool, BackendError> {
        let secs = ttl.as_secs();
        if secs == 0 {
            return Err(BackendError::permanent(
                "refresh_ttl requires at least 1 second".to_string(),
            ));
        }

        let url = format!(
            "{}/v1/cache/{}/ttl",
            self.api_url(),
            urlencoding::encode(key)
        );

        let body = serde_json::to_vec(&RefreshTtlRequest { ttl: secs }).map_err(|e| {
            BackendError::permanent(format!("failed to serialize refresh_ttl request: {e}"))
        })?;

        let req = self.with_standard_headers(
            self.client()
                .patch(&url)
                .bearer_auth(self.api_key_str())
                .header("Content-Type", "application/json")
                .body(body),
        );

        let resp = req
            .send()
            .await
            .map_err(|e| reqwest_err_sanitized(e, self.api_key_str()))?;

        match resp.status().as_u16() {
            200 | 204 => Ok(true),
            404 => Ok(false),
            _ => Err(self.error_from_response(resp).await),
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
