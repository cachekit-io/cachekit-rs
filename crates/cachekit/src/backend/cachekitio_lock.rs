//! [`LockableBackend`] implementation for the cachekit.io HTTP backend.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::cachekitio::{reqwest_err_sanitized, CachekitIO};
use super::LockableBackend;
use crate::error::BackendError;

/// Lock capability token travels in this request header, never the query string
/// (CWE-532): a `?lock_id=` query leaks the token into access/proxy logs and
/// OpenTelemetry `http.url` spans. SaaS dual-reads header + legacy query during
/// rollout, preferring the header. See protocol `spec/saas-api.md`.
const LOCK_ID_HEADER: &str = "X-CacheKit-Lock-Id";

impl CachekitIO {
    /// Build the unlock request. Extracted so tests can assert the lock_id rides the
    /// `X-CacheKit-Lock-Id` header and never appears in the URL (CWE-532).
    fn release_request(&self, key: &str, lock_id: &str) -> reqwest::RequestBuilder {
        let url = format!(
            "{}/v1/cache/{}/lock",
            self.api_url(),
            urlencoding::encode(key)
        );
        self.with_standard_headers(
            self.client()
                .delete(&url)
                .bearer_auth(self.api_key_str())
                .header(LOCK_ID_HEADER, lock_id),
        )
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LockAcquireRequest {
    timeout_ms: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LockAcquireResponse {
    lock_id: Option<String>,
}

#[cfg(not(target_arch = "wasm32"))]
#[cfg_attr(not(feature = "unsync"), async_trait)]
#[cfg_attr(feature = "unsync", async_trait(?Send))]
impl LockableBackend for CachekitIO {
    async fn acquire_lock(
        &self,
        key: &str,
        timeout_ms: u64,
    ) -> Result<Option<String>, BackendError> {
        let url = format!(
            "{}/v1/cache/{}/lock",
            self.api_url(),
            urlencoding::encode(key)
        );

        let body = serde_json::to_vec(&LockAcquireRequest { timeout_ms }).map_err(|e| {
            BackendError::permanent(format!("failed to serialize lock request: {e}"))
        })?;

        let req = self.with_standard_headers(
            self.client()
                .post(&url)
                .bearer_auth(self.api_key_str())
                .header("Content-Type", "application/json")
                .body(body),
        );

        let resp = req
            .send()
            .await
            .map_err(|e| reqwest_err_sanitized(e, self.api_key_str()))?;

        if !resp.status().is_success() {
            return Err(self.error_from_response(resp).await);
        }

        let response: LockAcquireResponse = resp
            .json()
            .await
            .map_err(|e| BackendError::transient(format!("failed to parse lock response: {e}")))?;

        Ok(response.lock_id)
    }

    async fn release_lock(&self, key: &str, lock_id: &str) -> Result<bool, BackendError> {
        // lock_id is a capability token → X-CacheKit-Lock-Id header, not the query string
        // (CWE-532). See `release_request`.
        let resp = self
            .release_request(key, lock_id)
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

    /// Compile-time proof that CachekitIO implements LockableBackend.
    fn _assert_lockable(_b: &dyn LockableBackend) {}

    #[test]
    fn cachekitio_is_lockable() {
        fn _check(backend: &CachekitIO) {
            _assert_lockable(backend);
        }
    }

    #[test]
    #[allow(clippy::expect_used)] // test-only: a failed build/missing header should panic loudly
    fn release_lock_sends_token_in_header_not_url() {
        let backend = CachekitIO::builder()
            .api_url("https://api.cachekit.io")
            .api_key("ck_test_key")
            .build()
            .expect("builder should succeed for the canonical host");

        let req = backend
            .release_request("my-key", "lock-secret-123")
            .build()
            .expect("request should build");

        // CWE-532: the capability token must never appear in the URL.
        let url = req.url();
        assert!(url.query().is_none(), "unexpected query string: {url}");
        assert!(
            !url.as_str().contains("lock_id"),
            "lock_id leaked into URL: {url}"
        );
        assert!(
            !url.as_str().contains("lock-secret-123"),
            "token leaked into URL: {url}"
        );

        // ...it rides the X-CacheKit-Lock-Id header under the exact wire name.
        let header = req
            .headers()
            .get("X-CacheKit-Lock-Id")
            .expect("X-CacheKit-Lock-Id header must be set");
        assert_eq!(header, "lock-secret-123");
    }
}
