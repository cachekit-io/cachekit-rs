use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use reqwest::header::{HeaderName, HeaderValue, InvalidHeaderValue};
use zeroize::Zeroizing;

use crate::backend::{
    delete_succeeded, encode_key, ttl_header, Backend, Freshness, HealthStatus, LockableBackend,
};
use crate::error::{BackendError, BackendErrorKind};
use crate::metrics::{metrics_headers_named, MetricsProvider, METRICS_HEADER_NAMES};
use crate::session::session_headers;
use crate::url_validator::validate_cachekitio_url;

// ── CachekitIO ────────────────────────────────────────────────────────────────

/// HTTP backend that talks to the cachekit.io SaaS API.
pub struct CachekitIO {
    client: reqwest::Client,
    api_key: Zeroizing<String>,
    api_url: String,
    /// `Authorization` and the session headers, built once so no request
    /// re-formats or re-parses them. The bearer value is marked sensitive, so
    /// a request's `Debug` output redacts it, and its buffer is wiped when the
    /// last clone drops.
    static_headers: [(HeaderName, HeaderValue); 3],
    /// The telemetry header names, parsed once, in [`METRICS_HEADER_NAMES`] order.
    metrics_header_names: [HeaderName; 5],
    /// Source of the `X-CacheKit-*` telemetry headers. Set once: by the
    /// builder when the user supplies one, otherwise by the client at build
    /// time via [`Backend::attach_metrics`] (first writer wins).
    metrics_provider: OnceLock<MetricsProvider>,
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

    /// Return the API key as a string slice (for error sanitizing in sibling modules).
    pub(crate) fn api_key_str(&self) -> &str {
        self.api_key.as_str()
    }

    /// Build the full URL for a cache key path segment.
    ///
    /// Keys are percent-encoded via [`encode_key`](crate::backend::encode_key) so
    /// slashes or special characters do not break the URL structure. Fallible: it
    /// rejects exactly the keys `encode_key` rejects (CWE-22, spec rule 2).
    fn url(&self, key: &str) -> Result<String, BackendError> {
        Ok(format!("{}/v1/cache/{}", self.api_url, encode_key(key)?))
    }

    /// Build the TTL URL for a cache key (`…/ttl`). Composes on [`Self::url`], so
    /// it inherits the same [`encode_key`](crate::backend::encode_key) guard and
    /// the `/v1/cache/` prefix lives in one place (matching the wasm `workers`
    /// backend). `pub(crate)` so the [`TtlInspectable`](super::TtlInspectable)
    /// impl in the sibling `cachekitio_ttl` module builds its path through it.
    pub(crate) fn ttl_url(&self, key: &str) -> Result<String, BackendError> {
        Ok(format!("{}/ttl", self.url(key)?))
    }

    /// Build the lock URL for a cache key (`…/lock`). Composes on [`Self::url`]
    /// (same guard, same single prefix). `pub(crate)` so the
    /// [`LockableBackend`](super::LockableBackend) impl in the sibling
    /// `cachekitio_lock` module builds its path through it.
    pub(crate) fn lock_url(&self, key: &str) -> Result<String, BackendError> {
        Ok(format!("{}/lock", self.url(key)?))
    }

    /// Build the health-check URL.
    fn health_url(&self) -> String {
        format!("{}/v1/cache/health", self.api_url)
    }

    /// Apply the bearer token, session and metrics headers to a request builder.
    pub(crate) fn with_standard_headers(
        &self,
        mut req: reqwest::RequestBuilder,
    ) -> reqwest::RequestBuilder {
        for (name, value) in &self.static_headers {
            req = req.header(name.clone(), value.clone());
        }
        for (name, value) in
            metrics_headers_named(self.metrics_provider.get(), &self.metrics_header_names)
        {
            req = req.header(name, value);
        }
        req
    }

    /// Build the `PUT` for [`Backend::set`] without sending it, so tests can
    /// pin its headers.
    fn put_request(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<reqwest::RequestBuilder, BackendError> {
        let mut req = self
            .client
            .put(self.url(key)?)
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .body(value);

        if let Some(ttl) = ttl {
            let (name, value) = ttl_header(ttl);
            req = req.header(name, value);
        }

        Ok(self.with_standard_headers(req))
    }

    /// Read error body from response and build a sanitized BackendError.
    pub(crate) async fn error_from_response(&self, resp: reqwest::Response) -> BackendError {
        let status = resp.status().as_u16();
        let quota_denied = status == 429 && resp.headers().contains_key(DENY_REASON_HEADER);
        let body = resp.bytes().await.unwrap_or_default();
        let err = from_http_status_sanitized(status, &body, self.api_key.as_str());
        if quota_denied {
            err.with_quota_denied()
        } else {
            err
        }
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

// ── Freshness headers ────────────────────────────────────────────────────────

/// Read a `GET 200`'s [`Freshness`] from its headers (`spec/saas-api.md`
/// § Stale-While-Revalidate, § Remaining Freshness).
///
/// An absent `X-CacheKit-Freshness` means fresh (a pre-SWR server). Any value
/// other than exactly `fresh` means stale, because revalidating is the safe
/// reading of a token the client does not know; every copy of a repeated
/// header must say `fresh`. An absent `X-CacheKit-Fresh-For` means no server
/// bound. A repeated one has no single bound, so it counts as `0`.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn freshness_from_headers(headers: &reqwest::header::HeaderMap) -> Freshness {
    let is_stale = headers
        .get_all("x-cachekit-freshness")
        .iter()
        .any(|value| value != "fresh");
    let mut fresh_for = headers.get_all("x-cachekit-fresh-for").iter();
    let fresh_for = match (fresh_for.next(), fresh_for.next()) {
        (None, _) => None,
        (Some(value), None) => Some(parse_fresh_for(value.as_bytes())),
        (Some(_), Some(_)) => Some(Duration::ZERO),
    };
    Freshness {
        is_stale,
        fresh_for,
    }
}

/// `X-CacheKit-Fresh-For` must be 1–7 ASCII digits and at most 2,592,000 (the
/// 30-day TTL cap); anything else counts as `0`. The length check runs first,
/// so the range check never sees a value that a fixed-width parse could wrap
/// back into range (`4297559296` as a `u32` is `2592000`).
#[cfg(not(target_arch = "wasm32"))]
fn parse_fresh_for(value: &[u8]) -> Duration {
    const MAX_FRESH_FOR_SECS: u64 = 2_592_000;
    if !(1..=7).contains(&value.len()) || !value.iter().all(u8::is_ascii_digit) {
        return Duration::ZERO;
    }
    match std::str::from_utf8(value).map(str::parse::<u64>) {
        Ok(Ok(secs)) if secs <= MAX_FRESH_FOR_SECS => Duration::from_secs(secs),
        _ => Duration::ZERO,
    }
}

// ── Backend impl ──────────────────────────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
#[cfg_attr(not(feature = "unsync"), async_trait)]
#[cfg_attr(feature = "unsync", async_trait(?Send))]
impl Backend for CachekitIO {
    fn attach_metrics(&self, provider: MetricsProvider) {
        // A provider supplied on the builder is already set and wins.
        self.metrics_provider.get_or_init(|| provider);
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        Ok(self.get_with_freshness(key).await?.map(|(bytes, _)| bytes))
    }

    async fn get_with_freshness(
        &self,
        key: &str,
    ) -> Result<Option<(Vec<u8>, Freshness)>, BackendError> {
        let req = self.with_standard_headers(self.client.get(self.url(key)?));

        let resp = req
            .send()
            .await
            .map_err(|e| reqwest_err_sanitized(e, self.api_key.as_str()))?;

        match resp.status().as_u16() {
            200 => {
                let freshness = freshness_from_headers(resp.headers());
                let bytes = resp
                    .bytes()
                    .await
                    .map_err(|e| reqwest_err_sanitized(e, self.api_key.as_str()))?;
                Ok(Some((Vec::from(bytes), freshness)))
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
        let resp = self
            .put_request(key, value, ttl)?
            .timeout(WRITE_TIMEOUT)
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

    /// `true` on every successful delete, whether or not the key existed: the
    /// server does not report existence on `DELETE`.
    async fn delete(&self, key: &str) -> Result<bool, BackendError> {
        let req = self.with_standard_headers(self.client.delete(self.url(key)?));

        let resp = req
            .timeout(WRITE_TIMEOUT)
            .send()
            .await
            .map_err(|e| reqwest_err_sanitized(e, self.api_key.as_str()))?;

        if delete_succeeded(resp.status().as_u16()) {
            Ok(true)
        } else {
            Err(self.error_from_response(resp).await)
        }
    }

    async fn exists(&self, key: &str) -> Result<bool, BackendError> {
        let req = self.with_standard_headers(self.client.head(self.url(key)?));

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

        let req = self.with_standard_headers(self.client.get(self.health_url()));

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

    fn as_lockable(&self) -> Option<&dyn LockableBackend> {
        Some(self)
    }
}

// ── Prebuilt headers ──────────────────────────────────────────────────────────

/// Build the per-client constant headers: `Authorization: Bearer <key>`,
/// marked sensitive as `bearer_auth` would, and the process's session headers.
/// The bearer outlives every request and each request clones it, so its buffer
/// is a zeroising owner those clones share: the last drop wipes the key.
///
/// # Errors
///
/// [`CachekitError::Config`](crate::error::CachekitError::Config) when the key
/// holds bytes an HTTP header cannot carry. The message never echoes the key.
fn static_headers(
    api_key: &str,
) -> Result<[(HeaderName, HeaderValue); 3], crate::error::CachekitError> {
    use crate::error::CachekitError;

    let mut bearer = Zeroizing::new(Vec::with_capacity("Bearer ".len() + api_key.len()));
    bearer.extend_from_slice(b"Bearer ");
    bearer.extend_from_slice(api_key.as_bytes());
    let mut auth = header_over(bearer).map_err(|_| {
        CachekitError::Config("api_key holds characters an HTTP header cannot carry".to_string())
    })?;
    auth.set_sensitive(true);

    let [(id_name, id), (start_name, start)] = session_headers();
    let header = |name: &str, value: &str| {
        Ok::<_, CachekitError>((
            HeaderName::from_bytes(name.as_bytes())
                .map_err(|e| CachekitError::Config(format!("header name {name}: {e}")))?,
            HeaderValue::from_str(value)
                .map_err(|e| CachekitError::Config(format!("header {name}: {e}")))?,
        ))
    };
    Ok([
        (reqwest::header::AUTHORIZATION, auth),
        header(id_name, id)?,
        header(start_name, start)?,
    ])
}

/// A header value over `owner`'s buffer, not a copy of it: clones share the
/// buffer, and `owner` drops with the last of them.
fn header_over<T: AsRef<[u8]> + Send + 'static>(
    owner: T,
) -> Result<HeaderValue, InvalidHeaderValue> {
    HeaderValue::from_maybe_shared(Bytes::from_owner(owner))
}

/// Parse [`METRICS_HEADER_NAMES`] once per client.
fn metrics_header_names() -> Result<[HeaderName; 5], crate::error::CachekitError> {
    let [a, b, c, d, e] = METRICS_HEADER_NAMES.map(|name| {
        HeaderName::from_bytes(name.as_bytes())
            .map_err(|e| crate::error::CachekitError::Config(format!("header name {name}: {e}")))
    });
    Ok([a?, b?, c?, d?, e?])
}

// ── Builder ───────────────────────────────────────────────────────────────────

/// `User-Agent` on every native CachekitIO request, so the service and its edge
/// can tell this SDK and version apart from other HTTP clients.
#[cfg(not(target_arch = "wasm32"))]
const USER_AGENT: &str = concat!("cachekit-rs/", env!("CARGO_PKG_VERSION"));

/// How long an idle pooled connection is kept for reuse. Cloudflare closes an
/// idle client HTTP/1.1 connection after 400 s, not configurable; reqwest's
/// 90 s default dropped connections the edge would still serve, so a request
/// after a 90-390 s gap paid DNS, TCP and TLS again. 390 s leaves a 10 s
/// margin, and reqwest's 15 s TCP keepalive keeps NAT mappings alive meanwhile.
/// Per-attempt budget for every request but a write, connect and TLS
/// included: the protocol's `CACHEKIT_TIMEOUT` default (`spec/saas-api.md`),
/// as in cachekit-py and cachekit-ts. It was 30 s, so under the retrying
/// presets a request the service accepted and never answered held a call for
/// ~90 s before it failed open.
#[cfg(not(target_arch = "wasm32"))]
const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Per-attempt budget for a write (`PUT`, `DELETE`): twice the read budget,
/// because a write carries the payload and the service commits it before it
/// answers, so a slow but healthy write must not time out and be re-sent.
#[cfg(not(target_arch = "wasm32"))]
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);

/// Header on a `429` that denies a spent quota or balance rather than a rate
/// limit; such an error is never retried within the call (see
/// [`crate::error::QuotaDenied`]).
#[cfg_attr(target_arch = "wasm32", allow(dead_code))] // wasm32 uses the Workers backend
const DENY_REASON_HEADER: &str = "x-cachekit-deny-reason";

#[cfg(not(target_arch = "wasm32"))]
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(390);

/// The settings behind every native [`CachekitIO`] client, unbuilt so tests can
/// add `.no_proxy()` and reach a loopback stub whatever the proxy env says.
#[cfg(not(target_arch = "wasm32"))]
fn http_client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .use_rustls_tls()
        .user_agent(USER_AGENT)
        .redirect(reqwest::redirect::Policy::none())
        .timeout(READ_TIMEOUT)
        .connect_timeout(READ_TIMEOUT)
        .pool_idle_timeout(POOL_IDLE_TIMEOUT)
}

/// The HTTP client every native [`CachekitIO`] uses.
#[cfg(not(target_arch = "wasm32"))]
fn http_client() -> reqwest::Result<reqwest::Client> {
    http_client_builder().build()
}

/// wasm32: the platform's `fetch` owns pooling and timeouts; no User-Agent is set.
#[cfg(target_arch = "wasm32")]
fn http_client() -> reqwest::Result<reqwest::Client> {
    reqwest::Client::builder().build()
}

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

    /// Override the source of the `X-CacheKit-*` telemetry headers.
    ///
    /// Not needed for normal use: `CacheKitBuilder::build` attaches the
    /// client's own live counters to any backend without one. Set this only
    /// to report numbers from somewhere else; it takes precedence.
    pub fn metrics_provider(mut self, provider: MetricsProvider) -> Self {
        self.metrics_provider = Some(provider);
        self
    }

    /// Consume the builder and construct a [`CachekitIO`].
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `api_key` was not set, or holds bytes an HTTP header cannot carry
    ///   (a control character such as a newline).
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

        let client = http_client()
            .map_err(|e| CachekitError::Config(format!("failed to build HTTP client: {e}")))?;

        let static_headers = static_headers(&api_key)?;
        let metrics_header_names = metrics_header_names()?;

        Ok(CachekitIO {
            client,
            api_key,
            api_url,
            static_headers,
            metrics_header_names,
            metrics_provider: self
                .metrics_provider
                .map_or_else(OnceLock::new, OnceLock::from),
        })
    }
}

// ── Freshness-header parsing tests ───────────────────────────────────────────

#[cfg(all(test, not(target_arch = "wasm32")))]
#[allow(clippy::expect_used)] // test-only: a malformed fixture header should panic loudly
mod freshness_header_tests {
    use std::time::Duration;

    use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

    use super::freshness_from_headers;
    use crate::backend::Freshness;

    /// Build a header map with the wire's mixed-case names; `append` keeps
    /// repeats, as a proxy that duplicates a header would.
    fn headers(pairs: &[(&str, &[u8])]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.append(
                HeaderName::from_bytes(name.as_bytes()).expect("valid header name"),
                HeaderValue::from_bytes(value).expect("valid header value"),
            );
        }
        map
    }

    fn label(values: &[&[u8]]) -> bool {
        let pairs: Vec<_> = values
            .iter()
            .map(|v| ("X-CacheKit-Freshness", *v))
            .collect();
        freshness_from_headers(&headers(&pairs)).is_stale
    }

    fn fresh_for(values: &[&[u8]]) -> Option<Duration> {
        let pairs: Vec<_> = values
            .iter()
            .map(|v| ("X-CacheKit-Fresh-For", *v))
            .collect();
        freshness_from_headers(&headers(&pairs)).fresh_for
    }

    #[test]
    fn absent_headers_are_a_fresh_unbounded_read() {
        assert_eq!(
            freshness_from_headers(&HeaderMap::new()),
            Freshness::default()
        );
    }

    #[test]
    fn only_an_exact_fresh_label_is_fresh() {
        assert!(!label(&[b"fresh"]));
        assert!(!label(&[b"fresh", b"fresh"]));
        // (d) `stale`, an unknown token, a case variant, an empty value, or any
        // non-`fresh` copy of a repeated header all read as stale.
        for stale in [&b"stale"[..], b"revalidating", b"Fresh", b"", b"fresh "] {
            assert!(
                label(&[stale]),
                "{:?} must read as stale",
                String::from_utf8_lossy(stale)
            );
        }
        assert!(label(&[b"fresh", b"stale"]));
    }

    #[test]
    fn fresh_for_in_grammar_parses() {
        for (value, secs) in [
            (&b"0"[..], 0),
            (b"1", 1),
            (b"0000005", 5),
            (b"2592000", 2_592_000),
        ] {
            assert_eq!(fresh_for(&[value]), Some(Duration::from_secs(secs)));
        }
    }

    /// (d) Outside 1–7 ASCII digits, or over 2,592,000, is `0`. `4297559296` is
    /// the value a wrapping `u32` parse would turn into `2592000`.
    #[test]
    fn fresh_for_outside_grammar_is_zero() {
        let invalid: [&[u8]; 11] = [
            b"",
            b"-1",
            b"+5",
            b" 5",
            b"5s",
            b"1.5",
            "\u{0663}".as_bytes(), // ARABIC-INDIC DIGIT THREE: a digit, not ASCII
            b"12345678",
            b"2592001",
            b"9999999",
            b"4297559296",
        ];
        for value in invalid {
            assert_eq!(
                fresh_for(&[value]),
                Some(Duration::ZERO),
                "{:?} must count as 0",
                String::from_utf8_lossy(value)
            );
        }
        assert_eq!(
            fresh_for(&[b"10", b"10"]),
            Some(Duration::ZERO),
            "a repeated bound counts as 0"
        );
    }
}

// ── Telemetry-header wiring tests ─────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::expect_used)] // test-only: a builder failure on a fixture should panic loudly
mod metrics_wiring_tests {
    use std::sync::Arc;

    use super::CachekitIO;
    use crate::backend::Backend;
    use crate::metrics::{metrics_headers, L1Stats, MetricsProvider};

    fn provider(l1_hits: u64) -> MetricsProvider {
        Arc::new(move || {
            Some(L1Stats {
                l1_hits,
                l2_hits: 0,
                misses: 0,
                l1_enabled: true,
            })
        })
    }

    fn l1_hits_header(backend: &CachekitIO) -> Option<String> {
        metrics_headers(backend.metrics_provider.get())
            .into_iter()
            .find(|h| h.0 == "X-CacheKit-L1-Hits")
            .map(|h| h.1)
    }

    #[test]
    fn attach_fills_an_unset_provider() {
        let backend = CachekitIO::builder()
            .api_key("ck_test_key")
            .build()
            .expect("builder succeeds");
        assert_eq!(l1_hits_header(&backend), None, "nothing attached yet");

        backend.attach_metrics(provider(7));
        assert_eq!(l1_hits_header(&backend).as_deref(), Some("7"));
    }

    #[test]
    fn builder_provider_is_retained_over_attach() {
        let backend = CachekitIO::builder()
            .api_key("ck_test_key")
            .metrics_provider(provider(1))
            .build()
            .expect("builder succeeds");

        backend.attach_metrics(provider(99));
        assert_eq!(
            l1_hits_header(&backend).as_deref(),
            Some("1"),
            "user-supplied provider wins"
        );
    }
}

// ── Cache-key path encoding tests (CWE-22) ────────────────────────────────────

#[cfg(test)]
#[allow(clippy::expect_used)] // test-only: a builder/parse failure on a fixture should panic loudly
mod path_encoding_tests {
    use super::CachekitIO;
    use crate::backend::path_encoding_vectors::{reject_keys, transmittable, RS_NEAR_MISSES};
    use url::Url;

    const API: &str = "https://api.cachekit.io";

    fn backend() -> CachekitIO {
        CachekitIO::builder()
            .api_url(API)
            .api_key("ck_test_key")
            .build()
            .expect("builder should succeed for the canonical host")
    }

    /// Repro. Before any guard, a raw-encoded `.`/`..` key collapses in
    /// rust-url (the parser `reqwest` uses) *before* the request leaves the
    /// process: the segment is stripped and the path escapes `/v1/cache/`.
    /// `%2E%2E` collapses identically, which is why the fix rejects rather than
    /// re-encodes (WHATWG treats `%2e%2e` as a dot-segment too).
    #[test]
    fn repro_raw_dot_key_escapes_the_cache_prefix() {
        let cases = [
            ("..", "", "/v1/"),
            ("..", "/ttl", "/v1/ttl"),
            ("..", "/lock", "/v1/lock"),
            (".", "", "/v1/cache/"),
        ];
        for (key, suffix, escaped) in cases {
            let raw = format!("{API}/v1/cache/{}{suffix}", urlencoding::encode(key));
            let parsed = Url::parse(&raw).expect("parses");
            assert_eq!(
                parsed.path(),
                escaped,
                "raw {key:?}{suffix} should collapse to {escaped}"
            );
            // The %2E form the Python SDK emits collapses just the same in rust-url.
            let pct = key.replace('.', "%2E");
            let enc = format!("{API}/v1/cache/{pct}{suffix}");
            assert_eq!(
                Url::parse(&enc).expect("parses").path(),
                escaped,
                "%2E-encoded {key:?}{suffix} also collapses — encoding cannot fix this in rust-url"
            );
        }
    }

    /// Spec rule 2 — every fixture reject row (the empty key, `.`, `..`,
    /// `health`, `ttl`, `lock`) is rejected by every builder (base, ttl, lock):
    /// no URL is produced, so no rewritten or mis-routed request can be sent.
    #[test]
    fn reserved_segments_rejected_by_every_builder() {
        let b = backend();
        let keys = reject_keys();
        assert!(!keys.is_empty(), "fixture has no reject rows");
        for key in &keys {
            assert!(b.url(key).is_err(), "url({key:?}) must be rejected");
            assert!(b.ttl_url(key).is_err(), "ttl_url({key:?}) must be rejected");
            assert!(
                b.lock_url(key).is_err(),
                "lock_url({key:?}) must be rejected"
            );
        }
    }

    /// Every transmittable fixture row, plus the rs near-misses, builds a URL
    /// whose *parsed* path (the real wire path, post-normalisation) stays inside
    /// `/v1/cache/`. Asserting on the unparsed `format!` output would pass while
    /// still shipping a traversal, so we parse with the same `url` crate
    /// `reqwest` uses.
    #[test]
    fn safe_keys_never_escape_the_cache_prefix() {
        let b = backend();
        let fixture_keys: Vec<String> = transmittable().into_iter().map(|v| v.key).collect();
        let vectors = fixture_keys
            .iter()
            .map(String::as_str)
            .chain(RS_NEAR_MISSES.iter().copied());
        for key in vectors {
            let base = Url::parse(&b.url(key).expect("url")).expect("parse base");
            let ttl = Url::parse(&b.ttl_url(key).expect("ttl_url")).expect("parse ttl");
            let lock = Url::parse(&b.lock_url(key).expect("lock_url")).expect("parse lock");

            assert!(
                base.path().starts_with("/v1/cache/") && base.path().len() > "/v1/cache/".len(),
                "base path {} escaped prefix for {key:?}",
                base.path()
            );
            assert_eq!(
                base.path(),
                format!("/v1/cache/{}", urlencoding::encode(key)),
                "base wire path mismatch for {key:?}"
            );
            assert!(
                ttl.path().starts_with("/v1/cache/") && ttl.path().ends_with("/ttl"),
                "ttl path {} escaped prefix for {key:?}",
                ttl.path()
            );
            assert!(
                lock.path().starts_with("/v1/cache/") && lock.path().ends_with("/lock"),
                "lock path {} escaped prefix for {key:?}",
                lock.path()
            );
        }
    }
}

// ── PUT TTL header tests ──────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::expect_used)] // test-only: a builder failure on a fixture should panic loudly
mod put_ttl_header_tests {
    use std::time::Duration;

    use super::CachekitIO;

    fn put(ttl: Option<Duration>) -> reqwest::Request {
        CachekitIO::builder()
            .api_key("ck_test_key")
            .build()
            .expect("builder succeeds")
            .put_request("k", b"v".to_vec(), ttl)
            .expect("key encodes")
            .build()
            .expect("request builds")
    }

    #[test]
    fn put_sends_x_cachekit_ttl_and_no_legacy_x_ttl() {
        let req = put(Some(Duration::from_secs(300)));
        assert_eq!(
            req.headers()
                .get("X-CacheKit-TTL")
                .and_then(|v| v.to_str().ok()),
            Some("300")
        );
        assert!(req.headers().get("X-TTL").is_none(), "legacy header sent");
    }

    #[test]
    fn put_without_ttl_sends_no_ttl_header() {
        let req = put(None);
        assert!(req.headers().get("X-CacheKit-TTL").is_none());
        assert!(req.headers().get("X-TTL").is_none());
    }
}

// ── Prebuilt header tests ─────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::expect_used)] // test-only: a builder failure on a fixture should panic loudly
mod prebuilt_header_tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use super::{header_over, CachekitIO};
    use crate::metrics::{metrics_headers, L1Stats, MetricsProvider};
    use crate::session::session_headers;

    const KEY: &str = "ck_live_prebuilt_secret";

    fn put(provider: Option<MetricsProvider>) -> reqwest::Request {
        let mut builder = CachekitIO::builder().api_key(KEY);
        if let Some(provider) = provider {
            builder = builder.metrics_provider(provider);
        }
        builder
            .build()
            .expect("builder succeeds")
            .put_request("k", b"v".to_vec(), None)
            .expect("key encodes")
            .build()
            .expect("request builds")
    }

    #[test]
    fn bearer_is_sent_and_redacted_from_debug() {
        let req = put(None);
        let auth = req
            .headers()
            .get(reqwest::header::AUTHORIZATION)
            .expect("authorization header");
        assert_eq!(auth.as_bytes(), format!("Bearer {KEY}").as_bytes());
        assert!(auth.is_sensitive(), "prebuilt bearer must be sensitive");
        let debug = format!("{req:?}");
        assert!(!debug.contains(KEY), "api key leaked into Debug: {debug}");
    }

    /// The bearer's zeroising owner is the header's buffer, not a copy of it:
    /// clones share it, and it drops (wiping the key) only with the last one.
    #[test]
    fn header_clones_share_the_owner_until_the_last_drops() {
        struct Owner(Vec<u8>, Arc<AtomicBool>);
        impl AsRef<[u8]> for Owner {
            fn as_ref(&self) -> &[u8] {
                &self.0
            }
        }
        impl Drop for Owner {
            fn drop(&mut self) {
                self.1.store(true, Ordering::SeqCst);
            }
        }

        let dropped = Arc::new(AtomicBool::new(false));
        let header = header_over(Owner(b"Bearer k".to_vec(), Arc::clone(&dropped))).expect("valid");
        let clone = header.clone();
        drop(header);
        assert!(
            !dropped.load(Ordering::SeqCst),
            "owner dropped while a clone lives"
        );
        assert_eq!(clone.as_bytes(), b"Bearer k");
        drop(clone);
        assert!(
            dropped.load(Ordering::SeqCst),
            "owner outlived its last clone"
        );
    }

    #[test]
    fn a_key_no_header_can_carry_fails_build_without_echoing_it() {
        let err = CachekitIO::builder()
            .api_key("ck_bad\nkey")
            .build()
            .expect_err("newline in key");
        assert!(!err.to_string().contains("ck_bad"), "{err}");
    }

    /// The prebuilt headers carry the same names and values the string
    /// builders produce (header names compare case-insensitively).
    #[test]
    fn session_and_metrics_headers_match_their_string_builders() {
        let provider: MetricsProvider = Arc::new(|| {
            Some(L1Stats {
                l1_hits: 3,
                l2_hits: 2,
                misses: 5,
                l1_enabled: true,
            })
        });
        let req = put(Some(provider.clone()));
        let expected = session_headers()
            .into_iter()
            .map(|(n, v)| (n, v.to_string()))
            .chain(metrics_headers(Some(&provider)));
        let mut count = 0;
        for (name, value) in expected {
            let got = req.headers().get_all(name).iter().collect::<Vec<_>>();
            assert_eq!(got.len(), 1, "{name} sent {} times", got.len());
            assert_eq!(got[0].as_bytes(), value.as_bytes(), "{name}");
            count += 1;
        }
        assert_eq!(count, 7);
    }

    #[test]
    fn disabled_metrics_send_only_the_status_header() {
        let req = put(None);
        assert_eq!(
            req.headers()
                .get("X-CacheKit-L1-Status")
                .map(|v| v.as_bytes()),
            Some(&b"disabled"[..])
        );
        assert!(req.headers().get("X-CacheKit-L1-Hits").is_none());
    }
}

// ── HTTP client settings tests ────────────────────────────────────────────────

#[cfg(all(test, not(target_arch = "wasm32")))]
#[allow(clippy::expect_used)] // test-only: a stub-server failure should panic loudly
mod http_client_tests {
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::{http_client_builder, CachekitIO, READ_TIMEOUT, WRITE_TIMEOUT};
    use crate::backend::Backend;
    use crate::error::BackendErrorKind;

    /// The production client minus the system proxy: an `HTTP_PROXY` or
    /// `ALL_PROXY` that does not exempt loopback would otherwise take the
    /// stub's requests.
    fn client() -> reqwest::Client {
        http_client_builder()
            .no_proxy()
            .build()
            .expect("client builds")
    }

    /// A keep-alive HTTP/1.1 stub on loopback: answers every request `200`,
    /// records each request's header lines, and counts accepted connections.
    struct Stub {
        url: String,
        connections: Arc<Mutex<usize>>,
        requests: Arc<Mutex<Vec<Vec<String>>>>,
    }

    const OK: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n";

    fn stub() -> Stub {
        stub_with(|_| Some(OK))
    }

    /// [`stub`], but `answer(n)` gives the reply to the stub's `n`th request
    /// (from 0); `None` reads the request and never answers it.
    fn stub_with(answer: fn(usize) -> Option<&'static [u8]>) -> Stub {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind stub");
        let url = format!("http://{}/", listener.local_addr().expect("addr"));
        let connections = Arc::new(Mutex::new(0));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let (conns, reqs) = (connections.clone(), requests.clone());
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                *conns.lock().expect("lock") += 1;
                let reqs = reqs.clone();
                std::thread::spawn(move || {
                    let mut reader = BufReader::new(stream.try_clone().expect("clone"));
                    loop {
                        let mut head = Vec::new();
                        loop {
                            let mut line = String::new();
                            if reader.read_line(&mut line).unwrap_or(0) == 0 {
                                return;
                            }
                            if line == "\r\n" {
                                break;
                            }
                            head.push(line.trim_end().to_owned());
                        }
                        let n = {
                            let mut reqs = reqs.lock().expect("lock");
                            reqs.push(head);
                            reqs.len() - 1
                        };
                        let Some(reply) = answer(n) else {
                            // Hold the connection open, unanswered, for good.
                            loop {
                                std::thread::park();
                            }
                        };
                        if stream.write_all(reply).is_err() {
                            return;
                        }
                    }
                });
            }
        });
        Stub {
            url,
            connections,
            requests,
        }
    }

    /// Connections the stub accepted for two GETs `idle` apart, on the
    /// tokio clock reqwest's pool reads.
    async fn connections_after_idle(idle: Duration) -> usize {
        let stub = stub();
        let client = client();
        client.get(&stub.url).send().await.expect("first GET");
        tokio::time::pause();
        tokio::time::advance(idle).await;
        tokio::time::resume();
        client.get(&stub.url).send().await.expect("second GET");
        let n = *stub.connections.lock().expect("lock");
        n
    }

    #[tokio::test]
    async fn sends_cachekit_rs_user_agent() {
        let stub = stub();
        client().get(&stub.url).send().await.expect("GET");
        let want = format!("user-agent: cachekit-rs/{}", env!("CARGO_PKG_VERSION"));
        let requests = stub.requests.lock().expect("lock");
        assert!(
            requests[0].iter().any(|h| h.eq_ignore_ascii_case(&want)),
            "no `{want}` in {:?}",
            requests[0]
        );
    }

    #[tokio::test]
    async fn reuses_a_connection_idle_389_s() {
        assert_eq!(connections_after_idle(Duration::from_secs(389)).await, 1);
    }

    #[tokio::test]
    async fn redials_a_connection_idle_past_390_s() {
        assert_eq!(connections_after_idle(Duration::from_secs(391)).await, 2);
    }

    /// A production [`CachekitIO`] aimed at `stub` over plain HTTP: the
    /// builder only accepts `https`, so the test swaps the client and URL.
    fn backend_at(stub: &Stub) -> CachekitIO {
        let mut backend = CachekitIO::builder()
            .api_key("ck_test_key")
            .build()
            .expect("builder succeeds for the default host");
        backend.client = client();
        backend.api_url = stub.url.trim_end_matches('/').to_owned();
        backend
    }

    /// How long `op` takes on the tokio clock, paused once the stub's first
    /// request has opened the pooled connection: an unanswered request then
    /// advances virtual time straight to whichever timer fires first.
    async fn paused_elapsed<T>(op: impl std::future::Future<Output = T>) -> (T, Duration) {
        tokio::time::pause();
        let start = tokio::time::Instant::now();
        let out = op.await;
        let elapsed = start.elapsed();
        tokio::time::resume();
        (out, elapsed)
    }

    /// `elapsed` is `budget`, give or take the tokio timer's 1 ms resolution.
    fn assert_budget(elapsed: Duration, budget: Duration) {
        assert!(
            elapsed >= budget && elapsed <= budget + Duration::from_millis(2),
            "took {elapsed:?}, budget {budget:?}"
        );
    }

    /// Answers the first request (to pool a connection), stalls every later one.
    fn first_only(n: usize) -> Option<&'static [u8]> {
        (n == 0).then_some(OK)
    }

    #[tokio::test]
    async fn an_unanswered_read_times_out_after_5_s() {
        let stub = stub_with(first_only);
        let backend = backend_at(&stub);
        backend
            .exists("warm")
            .await
            .expect("first request pools a connection");

        let (result, elapsed) = paused_elapsed(backend.get("k")).await;

        let err = result.expect_err("an unanswered GET times out");
        assert_eq!(err.kind, BackendErrorKind::Timeout);
        assert_budget(elapsed, READ_TIMEOUT);
    }

    #[tokio::test]
    async fn an_unanswered_write_times_out_after_10_s() {
        let stub = stub_with(first_only);
        let backend = backend_at(&stub);
        backend
            .exists("warm")
            .await
            .expect("first request pools a connection");

        let (result, elapsed) = paused_elapsed(backend.set("k", b"v".to_vec(), None)).await;

        let err = result.expect_err("an unanswered PUT times out");
        assert_eq!(err.kind, BackendErrorKind::Timeout);
        assert_budget(elapsed, WRITE_TIMEOUT);
    }

    #[cfg(feature = "reliability")]
    mod through_the_reliability_stack {
        use super::*;
        use crate::client::SharedBackend;
        use crate::reliability::{wrap_reliable, ReliabilityConfig, RetryConfig};

        /// The default stack with jitter off, so virtual time is exact.
        fn reliable(backend: CachekitIO) -> SharedBackend {
            reliable_with_breaker(backend).0
        }

        fn reliable_with_breaker(
            backend: CachekitIO,
        ) -> (
            SharedBackend,
            Option<Arc<crate::reliability::CircuitBreaker>>,
        ) {
            #[cfg(not(feature = "unsync"))]
            let shared: SharedBackend = Arc::new(backend);
            #[cfg(feature = "unsync")]
            let shared: SharedBackend = std::rc::Rc::new(backend);
            let config = ReliabilityConfig {
                retry: Some(RetryConfig {
                    jitter: false,
                    ..RetryConfig::default()
                }),
                ..ReliabilityConfig::default()
            };
            wrap_reliable(shared, config)
        }

        const QUOTA_DENY: &[u8] = b"HTTP/1.1 429 Too Many Requests\r\n\
            x-cachekit-deny-reason: quota\r\ncontent-length: 0\r\n\r\n";
        const RATE_LIMITED: &[u8] = b"HTTP/1.1 429 Too Many Requests\r\n\
            retry-after: 1\r\ncontent-length: 0\r\n\r\n";

        /// Was 3 x 30 s + backoff = 90.3 s and 3 requests.
        #[tokio::test]
        async fn an_unanswered_get_costs_one_attempt_and_5_s() {
            let stub = stub_with(first_only);
            let backend = reliable(backend_at(&stub));
            backend
                .exists("warm")
                .await
                .expect("first request pools a connection");

            let (result, elapsed) = paused_elapsed(backend.get("k")).await;

            assert_eq!(result.expect_err("stalled").kind, BackendErrorKind::Timeout);
            assert_budget(elapsed, Duration::from_secs(5));
            assert_eq!(
                stub.requests.lock().expect("lock").len(),
                2,
                "warm-up + one GET"
            );
        }

        #[tokio::test]
        async fn a_quota_deny_is_sent_once_and_stays_transient() {
            let stub = stub_with(|_| Some(QUOTA_DENY));
            let backend = reliable(backend_at(&stub));

            let err = backend.get("k").await.expect_err("denied");

            assert_eq!(
                err.kind,
                BackendErrorKind::Transient,
                "fails open, counts toward the breaker"
            );
            assert!(err.is_quota_denied());
            assert_eq!(
                stub.requests.lock().expect("lock").len(),
                1,
                "no in-call retry"
            );
        }

        #[tokio::test]
        async fn quota_denies_still_open_the_breaker() {
            let stub = stub_with(|_| Some(QUOTA_DENY));
            let (backend, breaker) = reliable_with_breaker(backend_at(&stub));

            for _ in 0..5 {
                backend.get("k").await.expect_err("denied");
            }

            let breaker = breaker.expect("default stack has a breaker");
            assert_eq!(breaker.state(), crate::reliability::CircuitState::Open);
            assert_eq!(
                stub.requests.lock().expect("lock").len(),
                5,
                "one request per op"
            );
        }

        #[tokio::test]
        async fn a_rate_limit_429_still_retries() {
            let stub = stub_with(|_| Some(RATE_LIMITED));
            let backend = reliable(backend_at(&stub));

            let err = backend.get("k").await.expect_err("rate limited");

            assert_eq!(err.kind, BackendErrorKind::Transient);
            assert!(!err.is_quota_denied());
            assert_eq!(
                stub.requests.lock().expect("lock").len(),
                3,
                "every attempt is sent"
            );
        }
    }
}
