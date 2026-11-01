//! Cloudflare Workers backend using the `worker::Fetch` API.
//!
//! This module is only compiled for `wasm32` targets (`--features workers`).
//! It uses `#[async_trait(?Send)]` because the Workers runtime is
//! single-threaded and `worker::Fetch` futures are `!Send`.

use std::collections::HashMap;
use std::future::{poll_fn, Future};
use std::pin::pin;
use std::sync::OnceLock;
use std::task::Poll;
use std::time::Duration;

use async_trait::async_trait;
use js_sys::{Function, Promise, Reflect};
use worker::wasm_bindgen::{JsCast, JsValue};
use worker::wasm_bindgen_futures::JsFuture;
use zeroize::Zeroizing;

use crate::backend::saas_wire::{
    LockAcquireRequest, LockAcquireResponse, RefreshTtlRequest, TtlResponse,
};
use crate::backend::{
    delete_succeeded, encode_key, ttl_header, ttl_wire_secs, Backend, HealthStatus,
    LockableBackend, TtlInspectable, READ_TIMEOUT, WRITE_TIMEOUT,
};
use crate::error::BackendError;
use crate::metrics::{
    metrics_headers_named, telemetry_on, MetricsProvider, Switch, METRICS_HEADER_NAMES,
    TELEMETRY_ENV,
};
use crate::session::session_headers;
use crate::url_validator::cachekitio_base_url;

// ── WorkersCachekitIO ────────────────────────────────────────────────────────

/// A response read in full within its attempt's budget.
struct Fetched {
    status: u16,
    body: Vec<u8>,
}

/// A timer on the global `setTimeout`, cleared when dropped. `worker::Delay`
/// is not used because it panics unless `setTimeout` returns a number, which
/// holds on workerd but not on Node.
struct Timer {
    handle: JsValue,
    /// Settles the timer's promise. `JsFuture` frees its callbacks only when
    /// the promise settles, so a cleared timer must still resolve it, or every
    /// request that beats its deadline leaks them.
    resolve: Function,
}

impl Timer {
    /// Arm a timer for `after`; the returned future resolves when it fires.
    fn arm(after: Duration) -> Result<(Self, JsFuture), BackendError> {
        let global = js_sys::global();
        let set_timeout: Function = Reflect::get(&global, &"setTimeout".into())
            .ok()
            .and_then(|f| f.dyn_into().ok())
            .ok_or_else(|| BackendError::permanent("no global setTimeout"))?;
        let mut armed = Err(JsValue::UNDEFINED);
        let mut settle = None;
        let fired = Promise::new(&mut |resolve, _reject| {
            armed = set_timeout.call2(&global, &resolve, &(after.as_millis() as f64).into());
            settle = Some(resolve);
        });
        let handle = armed.map_err(|e| {
            BackendError::permanent(format!("failed to arm the request timeout: {e:?}"))
        })?;
        let resolve = settle
            .ok_or_else(|| BackendError::permanent("Promise::new did not run its executor"))?;
        Ok((Self { handle, resolve }, JsFuture::from(fired)))
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        let global = js_sys::global();
        if let Ok(clear) = Reflect::get(&global, &"clearTimeout".into()) {
            if let Ok(clear) = clear.dyn_into::<Function>() {
                let _ = clear.call1(&global, &self.handle);
            }
        }
        let _ = self.resolve.call0(&JsValue::UNDEFINED);
    }
}

/// Aborts its request when dropped unless disarmed, so a request is cancelled
/// both when its budget runs out and when the caller drops the call mid-flight.
struct AbortOnDrop(Option<worker::AbortController>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(controller) = self.0.take() {
            controller.abort();
        }
    }
}

/// HTTP backend for cachekit.io that uses `worker::Fetch` instead of `reqwest`.
///
/// Designed for use inside Cloudflare Workers where the standard networking
/// stack is unavailable and `worker::Fetch` is the only HTTP primitive.
pub struct WorkersCachekitIO {
    api_key: Zeroizing<String>,
    api_url: String,
    /// Source of the `X-CacheKit-*` telemetry headers. Set once: by the
    /// builder when the user supplies one, otherwise by the client at build
    /// time via [`Backend::attach_metrics`] (first writer wins).
    metrics_provider: OnceLock<MetricsProvider>,
    /// Whether this backend sends the optional metrics headers
    /// ([`WorkersCachekitIOBuilder::telemetry`]), resolved once at build. It
    /// never sends the latency headers: the Workers clock advances only on
    /// I/O, so it cannot time an in-memory hit.
    telemetry: bool,
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
    ///
    /// Keys are percent-encoded via [`encode_key`](crate::backend::encode_key).
    /// Fallible: it rejects exactly the keys `encode_key` rejects (CWE-22, spec
    /// rule 2). `ttl_url`/`lock_url` build on this, so all three wasm paths
    /// inherit the guard.
    fn url(&self, key: &str) -> Result<String, BackendError> {
        Ok(format!("{}/v1/cache/{}", self.api_url, encode_key(key)?))
    }

    /// Build the health-check URL.
    fn health_url(&self) -> String {
        format!("{}/v1/cache/health", self.api_url)
    }

    /// Build the lock URL for a cache key. Callers pass the bare cache key;
    /// the SaaS lock endpoint owns the lock namespace server-side.
    fn lock_url(&self, key: &str) -> Result<String, BackendError> {
        Ok(format!("{}/lock", self.url(key)?))
    }

    /// Build the TTL URL for a cache key.
    fn ttl_url(&self, key: &str) -> Result<String, BackendError> {
        Ok(format!("{}/ttl", self.url(key)?))
    }

    /// Convert a non-success response into a classified, sanitized error.
    fn error_from(&self, resp: &Fetched) -> BackendError {
        let sanitized = BackendError::sanitize_message(
            std::str::from_utf8(&resp.body).unwrap_or(""),
            self.api_key.as_str(),
        );
        BackendError::from_http_status(resp.status, sanitized.as_bytes())
    }

    /// Send a request and read its whole response within `budget`, the same
    /// per-attempt budget the native backend gives reqwest. The Workers
    /// runtime's `fetch` has no timeout of its own and can hold a request for
    /// tens of seconds, so past the budget the request is aborted and the
    /// call fails with [`BackendErrorKind::Timeout`](crate::BackendErrorKind::Timeout).
    ///
    /// A success body is read in full and a failure to read it is an error.
    /// An error body is read best-effort: its status alone classifies it.
    async fn fetch(
        &self,
        method: &str,
        url: &str,
        body: Option<Vec<u8>>,
        extra_headers: Vec<(&str, String)>,
        budget: Duration,
    ) -> Result<Fetched, BackendError> {
        let mut headers = worker::Headers::new();
        headers
            .set(
                "Authorization",
                &format!("Bearer {}", self.api_key.as_str()),
            )
            .map_err(|e| {
                BackendError::permanent(BackendError::sanitize_message(
                    &format!("failed to set auth header: {e}"),
                    self.api_key.as_str(),
                ))
            })?;

        for (name, value) in extra_headers {
            headers.set(name, &value).map_err(|e| {
                BackendError::permanent(BackendError::sanitize_message(
                    &format!("failed to set header {name}: {e}"),
                    self.api_key.as_str(),
                ))
            })?;
        }

        // Inject session headers
        if self.telemetry {
            for (name, value) in session_headers() {
                headers.set(name, value).map_err(|e| {
                    BackendError::permanent(format!("failed to set session header {name}: {e}"))
                })?;
            }
        }

        // Inject metrics headers: with telemetry off, only L1-Status
        for (name, value) in metrics_headers_named(
            self.metrics_provider.get(),
            &METRICS_HEADER_NAMES,
            self.telemetry,
        ) {
            headers.set(name, &value).map_err(|e| {
                BackendError::permanent(format!("failed to set metrics header {name}: {e}"))
            })?;
        }

        let mut init = worker::RequestInit::new();
        init.with_method(match method {
            "GET" => worker::Method::Get,
            "PUT" => worker::Method::Put,
            "POST" => worker::Method::Post,
            "PATCH" => worker::Method::Patch,
            "DELETE" => worker::Method::Delete,
            "HEAD" => worker::Method::Head,
            _ => {
                return Err(BackendError::permanent(format!(
                    "unsupported HTTP method: {method}"
                )))
            }
        });
        init.with_headers(headers);
        // The API never redirects, and this request carries credentials, so it
        // goes only to the configured URL: `Manual` returns a 3xx as the
        // response, which every caller treats as an error. Same policy as the
        // native client's `Policy::none()`.
        init.with_redirect(worker::RequestRedirect::Manual);

        if let Some(bytes) = body {
            let js_array = js_sys::Uint8Array::from(bytes.as_slice());
            init.with_body(Some(js_array.into()));
        }

        let request = worker::Request::new_with_init(url, &init).map_err(|e| {
            BackendError::transient(BackendError::sanitize_message(
                &format!("failed to build request: {e}"),
                self.api_key.as_str(),
            ))
        })?;

        let controller = worker::AbortController::default();
        let signal = controller.signal();
        let mut abort = AbortOnDrop(Some(controller));
        let attempt = async {
            let mut resp = worker::Fetch::Request(request)
                .send_with_signal(&signal)
                .await
                .map_err(|e| {
                    BackendError::transient(BackendError::sanitize_message(
                        &format!("fetch failed: {e}"),
                        self.api_key.as_str(),
                    ))
                })?;
            let status = resp.status_code();
            let body = if (200..300).contains(&status) {
                resp.bytes().await.map_err(|e| {
                    BackendError::transient(BackendError::sanitize_message(
                        &format!("failed to read body: {e}"),
                        self.api_key.as_str(),
                    ))
                })?
            } else {
                resp.bytes().await.unwrap_or_default()
            };
            Ok(Fetched { status, body })
        };

        let (_timer, deadline) = Timer::arm(budget)?;
        let mut attempt = pin!(attempt);
        let mut deadline = pin!(deadline);
        let finished = poll_fn(|cx| match attempt.as_mut().poll(cx) {
            Poll::Ready(result) => Poll::Ready(Some(result)),
            Poll::Pending => deadline.as_mut().poll(cx).map(|_| None),
        })
        .await;

        match finished {
            Some(result) => {
                abort.0 = None;
                result
            }
            // `abort` drops on return, cancelling the request and any body
            // still streaming in.
            None => Err(BackendError::timeout(format!(
                "{method} request timed out after {} s",
                budget.as_secs()
            ))),
        }
    }
}

// ── Backend impl (wasm32 only) ───────────────────────────────────────────────

#[async_trait(?Send)]
impl Backend for WorkersCachekitIO {
    fn attach_metrics(&self, provider: MetricsProvider) {
        // A provider supplied on the builder is already set and wins.
        self.metrics_provider.get_or_init(|| provider);
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        let resp = self
            .fetch("GET", &self.url(key)?, None, vec![], READ_TIMEOUT)
            .await?;

        match resp.status {
            200 => Ok(Some(resp.body)),
            404 => Ok(None),
            _ => Err(self.error_from(&resp)),
        }
    }

    async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        let mut headers = vec![("Content-Type", "application/octet-stream".to_owned())];
        if let Some(ttl) = ttl {
            headers.push(ttl_header(ttl));
        }

        let resp = self
            .fetch("PUT", &self.url(key)?, Some(value), headers, WRITE_TIMEOUT)
            .await?;

        if (200..300).contains(&resp.status) {
            Ok(())
        } else {
            Err(self.error_from(&resp))
        }
    }

    /// `true` on every successful delete, whether or not the key existed: the
    /// server does not report existence on `DELETE`.
    async fn delete(&self, key: &str) -> Result<bool, BackendError> {
        let resp = self
            .fetch("DELETE", &self.url(key)?, None, vec![], WRITE_TIMEOUT)
            .await?;

        if delete_succeeded(resp.status) {
            Ok(true)
        } else {
            Err(self.error_from(&resp))
        }
    }

    async fn exists(&self, key: &str) -> Result<bool, BackendError> {
        let resp = self
            .fetch("HEAD", &self.url(key)?, None, vec![], READ_TIMEOUT)
            .await?;

        match resp.status {
            200 => Ok(true),
            404 => Ok(false),
            status => Err(BackendError::from_http_status(status, &[])),
        }
    }

    async fn health(&self) -> Result<HealthStatus, BackendError> {
        let resp = self
            .fetch("GET", &self.health_url(), None, vec![], READ_TIMEOUT)
            .await?;
        let status = resp.status;

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
            Err(self.error_from(&resp))
        }
    }
}

// ── LockableBackend impl (wasm32 only) ───────────────────────────────────────

/// Lock capability token travels in this request header, never the query
/// string (CWE-532): a `?lock_id=` query leaks the token into access/proxy
/// logs and OpenTelemetry `http.url` spans. Same contract as the native
/// `CachekitIO` impl. See protocol `spec/saas-api.md`.
const LOCK_ID_HEADER: &str = "X-CacheKit-Lock-Id";

#[async_trait(?Send)]
impl LockableBackend for WorkersCachekitIO {
    async fn acquire_lock(
        &self,
        key: &str,
        timeout_ms: u64,
    ) -> Result<Option<String>, BackendError> {
        let body = serde_json::to_vec(&LockAcquireRequest { timeout_ms }).map_err(|e| {
            BackendError::permanent(format!("failed to serialize lock request: {e}"))
        })?;

        let resp = self
            .fetch(
                "POST",
                &self.lock_url(key)?,
                Some(body),
                vec![("Content-Type", "application/json".to_owned())],
                READ_TIMEOUT,
            )
            .await?;

        if !(200..300).contains(&resp.status) {
            return Err(self.error_from(&resp));
        }

        // Contested acquire is `200 {"lock_id": null}` — branch on the body,
        // never on a 409 status (protocol#22).
        let response: LockAcquireResponse = serde_json::from_slice(&resp.body)
            .map_err(|e| BackendError::transient(format!("failed to parse lock response: {e}")))?;

        Ok(response.lock_id)
    }

    async fn release_lock(&self, key: &str, lock_id: &str) -> Result<bool, BackendError> {
        // lock_id is a capability token → X-CacheKit-Lock-Id header, not the
        // query string (CWE-532).
        let resp = self
            .fetch(
                "DELETE",
                &self.lock_url(key)?,
                None,
                vec![(LOCK_ID_HEADER, lock_id.to_owned())],
                READ_TIMEOUT,
            )
            .await?;

        match resp.status {
            200 | 204 => Ok(true),
            404 => Ok(false),
            _ => Err(self.error_from(&resp)),
        }
    }
}

// ── TtlInspectable impl (wasm32 only) ────────────────────────────────────────

#[async_trait(?Send)]
impl TtlInspectable for WorkersCachekitIO {
    async fn ttl(&self, key: &str) -> Result<Option<Duration>, BackendError> {
        let resp = self
            .fetch("GET", &self.ttl_url(key)?, None, vec![], READ_TIMEOUT)
            .await?;

        match resp.status {
            200 => {
                let body: TtlResponse = serde_json::from_slice(&resp.body).map_err(|e| {
                    BackendError::transient(format!("failed to parse TTL response: {e}"))
                })?;
                Ok(body.ttl.map(Duration::from_secs))
            }
            404 => Ok(None),
            _ => Err(self.error_from(&resp)),
        }
    }

    async fn refresh_ttl(&self, key: &str, ttl: Duration) -> Result<bool, BackendError> {
        // PATCH /ttl takes the X-CacheKit-TTL rules (spec/saas-api.md).
        let secs = ttl_wire_secs(ttl);
        if secs == 0 {
            return Err(BackendError::permanent(
                "refresh_ttl requires a non-zero TTL".to_string(),
            ));
        }

        let body = serde_json::to_vec(&RefreshTtlRequest { ttl: secs }).map_err(|e| {
            BackendError::permanent(format!("failed to serialize refresh_ttl request: {e}"))
        })?;

        let resp = self
            .fetch(
                "PATCH",
                &self.ttl_url(key)?,
                Some(body),
                vec![("Content-Type", "application/json".to_owned())],
                READ_TIMEOUT,
            )
            .await?;

        match resp.status {
            200 | 204 => Ok(true),
            404 => Ok(false),
            _ => Err(self.error_from(&resp)),
        }
    }
}

// ── Builder ──────────────────────────────────────────────────────────────────

/// Builder for [`WorkersCachekitIO`].
#[derive(Default)]
#[must_use]
pub struct WorkersCachekitIOBuilder {
    api_key: Option<Zeroizing<String>>,
    api_url: Option<String>,
    allow_custom_host: bool,
    metrics_provider: Option<MetricsProvider>,
    telemetry: Option<bool>,
    bindings: Option<worker::Env>,
}

impl WorkersCachekitIOBuilder {
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

    /// Turn this backend's telemetry on or off. Unset by default.
    ///
    /// Telemetry is the optional `X-CacheKit-*` headers the backend sends to
    /// cachekit.io, and nowhere else: the session ID and start time, and the
    /// client's L1 hit, L2 hit and miss counts with its L1 hit rate. With
    /// telemetry off none of them is sent; `X-CacheKit-L1-Status`, which the
    /// service requires on every request, still is. This backend never sends
    /// L1 hit latency: the Workers clock advances only on I/O.
    ///
    /// The variable `CACHEKIT_TELEMETRY` is the other control, read from
    /// `process.env` (present with the `nodejs_compat` flag) and from the
    /// bindings passed to [`bindings`](Self::bindings): `false`, `0`, `off` or
    /// `no` (any case, surrounding white space ignored) turns telemetry off,
    /// `true`, `1`, `on` or `yes` on, and any other non-empty value off. Off
    /// from any source wins; with none set telemetry is on during the closed
    /// beta. `build` reads them once.
    ///
    /// Without [`bindings`](Self::bindings), the SDK cannot see a
    /// `CACHEKIT_TELEMETRY` binding, so telemetry is off unless this setting
    /// turns it on.
    pub fn telemetry(mut self, on: bool) -> Self {
        self.telemetry = Some(on);
        self
    }

    /// The Worker's bindings (the `env` your handler receives), read once at
    /// `build` for a `CACHEKIT_TELEMETRY` variable or secret
    /// ([`telemetry`](Self::telemetry)). Without them telemetry is off unless
    /// `.telemetry(true)` turns it on, since a `CACHEKIT_TELEMETRY` binding
    /// could not be seen.
    pub fn bindings(mut self, env: &worker::Env) -> Self {
        self.bindings = Some(env.clone());
        self
    }

    /// Consume the builder and construct a [`WorkersCachekitIO`].
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `api_key` was not set or is empty.
    /// - the resolved URL scheme is not `https`, or the URL carries a query or
    ///   a fragment.
    /// - the URL carries credentials, or its hostname is not permitted (see
    ///   [`validate_cachekitio_url`](crate::url_validator::validate_cachekitio_url)).
    pub fn build(self) -> Result<WorkersCachekitIO, crate::error::CachekitError> {
        use crate::error::CachekitError;

        let api_key = self
            .api_key
            .filter(|k| !k.is_empty())
            .ok_or_else(|| CachekitError::Config("api_key is required".to_string()))?;

        let api_url = self
            .api_url
            .unwrap_or_else(|| "https://api.cachekit.io".to_string());

        let api_url = cachekitio_base_url(&api_url, self.allow_custom_host)?;

        let process_env = Reflect::get(&js_sys::global(), &"process".into())
            .ok()
            .and_then(|process| Reflect::get(&process, &"env".into()).ok());
        let bindings = match &self.bindings {
            Some(env) => telemetry_var(Some(env.as_ref())),
            // Bindings the SDK was not given may hold an off it cannot see,
            // and a missed off would leak what the user turned off: they
            // read as off unless the code turned telemetry on itself.
            None if self.telemetry == Some(true) => Switch::Unset,
            None => {
                warn_no_bindings();
                Switch::Off
            }
        };
        let env = telemetry_var(process_env.as_ref()).either(bindings);

        Ok(WorkersCachekitIO {
            api_key,
            api_url,
            metrics_provider: self
                .metrics_provider
                .map_or_else(OnceLock::new, OnceLock::from),
            telemetry: telemetry_on(self.telemetry, env),
        })
    }
}

/// Warn, once per isolate, that a backend built without its Worker's bindings
/// has telemetry off.
fn warn_no_bindings() {
    use std::sync::atomic::{AtomicBool, Ordering};
    static WARNED: AtomicBool = AtomicBool::new(false);
    if !WARNED.swap(true, Ordering::Relaxed) {
        worker::console_warn!(
            "cachekit: WorkersCachekitIO was built without .bindings(&env), so it cannot read \
             CACHEKIT_TELEMETRY: telemetry is off. Pass the bindings, or .telemetry(true)."
        );
    }
}

/// `CACHEKIT_TELEMETRY` on a JS object (`process.env` or the bindings), read
/// by the spec's rule. A value that is not a string reads as its JSON text, so
/// a `[vars]` boolean or number means what it says.
fn telemetry_var(source: Option<&JsValue>) -> Switch {
    let Some(source) = source.filter(|s| s.is_object()) else {
        return Switch::Unset;
    };
    let value = Reflect::get(source, &TELEMETRY_ENV.into()).unwrap_or(JsValue::UNDEFINED);
    if value.is_undefined() || value.is_null() {
        return Switch::Unset;
    }
    let text = value.as_string().or_else(|| {
        js_sys::JSON::stringify(&value)
            .ok()
            .and_then(|json| json.as_string())
    });
    Switch::from_env_value(Some(text.as_deref().ok_or(())))
}
