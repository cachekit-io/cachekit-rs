//! [`LockableBackend`] implementation for the cachekit.io HTTP backend.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::cachekitio::{from_http_status_sanitized, reqwest_err_sanitized, CachekitIO};
use super::LockableBackend;
use crate::error::BackendError;

#[derive(Serialize)]
struct LockAcquireRequest {
    timeout_ms: u64,
}

#[derive(Deserialize)]
struct LockAcquireResponse {
    lock_id: Option<String>,
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl LockableBackend for CachekitIO {
    async fn acquire_lock(
        &self,
        key: &str,
        timeout_ms: u64,
    ) -> Result<Option<String>, BackendError> {
        let url = format!(
            "{}/v1/cache/{}/lock",
            self.api_url().trim_end_matches('/'),
            urlencoding::encode(key)
        );

        let body = serde_json::to_vec(&LockAcquireRequest { timeout_ms }).map_err(|e| {
            BackendError::permanent(format!("failed to serialize lock request: {e}"))
        })?;

        let mut req = self
            .client()
            .post(&url)
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

        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = resp.bytes().await.unwrap_or_default();
            return Err(from_http_status_sanitized(
                status,
                &body,
                self.api_key_str(),
            ));
        }

        let response: LockAcquireResponse = resp
            .json()
            .await
            .map_err(|e| BackendError::transient(format!("failed to parse lock response: {e}")))?;

        Ok(response.lock_id)
    }

    async fn release_lock(&self, key: &str, lock_id: &str) -> Result<bool, BackendError> {
        let url = format!(
            "{}/v1/cache/{}/lock?lock_id={}",
            self.api_url().trim_end_matches('/'),
            urlencoding::encode(key),
            urlencoding::encode(lock_id),
        );

        let mut req = self
            .client()
            .delete(&url)
            .bearer_auth(self.api_key_str());

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
                let body = resp.bytes().await.unwrap_or_default();
                Err(from_http_status_sanitized(
                    status,
                    &body,
                    self.api_key_str(),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Compile-time proof that CachekitIO implements LockableBackend.
    fn _assert_lockable(_b: &dyn LockableBackend) {}

    #[test]
    fn cachekitio_is_lockable() {
        fn _check(backend: &CachekitIO) {
            _assert_lockable(backend);
        }
    }
}
