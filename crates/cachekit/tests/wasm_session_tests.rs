//! wasm32 runtime regression tests for the session clock and the Workers
//! backend's request path.
//!
//! `SystemTime::now()` / `Instant::now()` panic on `wasm32-unknown-unknown`,
//! and the compile-only wasm CI check shipped that trap in five releases —
//! these tests *execute* the affected paths on the real wasm32 target. If a
//! target gate is ever reverted to a bare std clock, the affected test traps
//! with `RuntimeError: unreachable` and the run goes red.
//!
//! Runs under `wasm-bindgen-test-runner` (Node) — see the `wasm` job in
//! `.github/workflows/ci.yml`.

#![cfg(all(target_arch = "wasm32", feature = "workers"))]

use cachekit::session::session_headers;
use wasm_bindgen_test::wasm_bindgen_test;

/// The exact panic site: building session headers on wasm32.
/// Reaching the asserts at all proves the clock (and uuid's js entropy) did
/// not trap; the value asserts check non-zero, plausible epoch millis (the
/// bounds mirror the native tests in src/session.rs, keep them in lockstep).
#[wasm_bindgen_test]
fn session_headers_valid_on_wasm32() {
    let headers = session_headers();
    assert_eq!(headers[0].0, "X-CacheKit-Session-ID");
    assert_eq!(headers[1].0, "X-CacheKit-Session-Start");

    let id = uuid::Uuid::parse_str(headers[0].1).expect("session ID should be a valid UUID");
    assert_eq!(id.get_version_num(), 4, "should be UUID v4");

    let start: u64 = headers[1]
        .1
        .parse()
        .expect("session start should be numeric");
    assert!(
        start > 1_704_067_200_000,
        "start {start} should be after 2024"
    );
    assert!(
        start < 4_102_444_800_000,
        "start {start} should be before 2100"
    );
}

/// Exercise the `WorkersCachekitIO` request path up to the fetch boundary:
/// `Backend::get` routes through `fetch()`, which injects `session_headers()`
/// (workers.rs) before any network I/O — the exact path that trapped in
/// production. `.invalid` is reserved (RFC 2606) and never resolves, so under
/// Node this fails fast with a transient network error and no live traffic.
/// A trap in session_headers() would abort the whole test instead.
#[wasm_bindgen_test]
async fn workers_backend_get_reaches_fetch_without_trapping() {
    use cachekit::backend::workers::WorkersCachekitIO;
    use cachekit::backend::Backend;

    let backend = WorkersCachekitIO::builder()
        .api_key("test-key-never-sent")
        .api_url("https://cachekit-wasm-clock.invalid")
        .allow_custom_host(true)
        .build()
        .expect("builder should accept a syntactically valid https URL");

    // Err(transient network failure) is the expected outcome; the regression
    // this guards is a wasm trap *before* the request is even built.
    let result = backend.get("wasm-clock-regression-key").await;
    assert!(
        result.is_err(),
        ".invalid must not resolve — expected a network error, got {result:?}"
    );
}

/// Starts a local HTTP server and points `globalThis.fetch` at it, keeping
/// each request's method, headers, body and redirect mode, so redirects are
/// handled by Node's real `fetch`. `redirect-{3xx}` answers that status with
/// `Location: /v1/cache/redirected`; `redirected` answers 200 to any method
/// and counts its hits in `globalThis.__ckRedirect.targetHits`.
const REDIRECT_SERVER_JS: &str = r"
    const http = process.getBuiltinModule('node:http');
    const state = (globalThis.__ckRedirect = { targetHits: 0 });
    const server = http.createServer((req, res) => {
        req.resume();
        const status = /^\/v1\/cache\/redirect-(3\d\d)$/.exec(req.url)?.[1];
        if (status) return res.writeHead(Number(status), { location: '/v1/cache/redirected' }).end();
        if (req.url === '/v1/cache/redirected') {
            state.targetHits++;
            return res.writeHead(200).end('followed');
        }
        res.writeHead(404).end();
    });
    server.unref();
    return new Promise((resolve) => server.listen(0, '127.0.0.1', () => {
        const origin = `http://127.0.0.1:${server.address().port}`;
        const realFetch = globalThis.fetch;
        globalThis.fetch = async (input, init) => {
            const req = new Request(input, init);
            const body = ['GET', 'HEAD'].includes(req.method) ? undefined : await req.arrayBuffer();
            return realFetch(origin + new URL(req.url).pathname, {
                method: req.method, headers: req.headers, body, redirect: req.redirect,
            });
        };
        state.close = () => {
            globalThis.fetch = realFetch;
            server.closeAllConnections();
            server.close();
        };
        resolve();
    }));
";

/// The API never redirects, so a 3xx from it is an error and the backend
/// sends nothing to the `Location` it names. The control shows the harness
/// does follow when a request asks it to, so the zero is not vacuous. Every
/// request runs before any assertion, so a failure never leaves `fetch`
/// patched for the other tests.
#[wasm_bindgen_test]
async fn workers_backend_does_not_follow_redirects() {
    use cachekit::backend::workers::WorkersCachekitIO;
    use cachekit::backend::Backend;
    use cachekit::BackendErrorKind;
    use worker::js_sys::{global, Function, Promise, Reflect};
    use worker::wasm_bindgen::{JsCast, JsValue};
    use worker::wasm_bindgen_futures::JsFuture;

    let backend = WorkersCachekitIO::builder()
        .api_key("test-key-never-sent")
        .build()
        .expect("default URL is valid");

    let start: Promise = Function::new_no_args(REDIRECT_SERVER_JS)
        .call0(&JsValue::NULL)
        .expect("start redirect server")
        .unchecked_into();
    JsFuture::from(start)
        .await
        .expect("redirect server listening");
    let state = Reflect::get(&global(), &"__ckRedirect".into()).expect("server state");
    let target_hits = || {
        Reflect::get(&state, &"targetHits".into())
            .ok()
            .and_then(|v| v.as_f64())
    };

    // Control: a request in follow mode reaches the redirect target.
    let mut init = worker::RequestInit::new();
    init.with_redirect(worker::RequestRedirect::Follow);
    let control = match worker::Request::new_with_init(
        "https://api.cachekit.io/v1/cache/redirect-302",
        &init,
    ) {
        Ok(request) => worker::Fetch::Request(request)
            .send()
            .await
            .map(|resp| resp.status_code()),
        Err(e) => Err(e),
    };
    let control_hits = target_hits();
    let _ = Reflect::set(&state, &"targetHits".into(), &0.into());

    let mut results = Vec::new();
    for status in [301, 302, 303, 307, 308] {
        let key = format!("redirect-{status}");
        results.push(("get", status, backend.get(&key).await.err()));
        results.push(("set", status, backend.set(&key, vec![1], None).await.err()));
        results.push(("delete", status, backend.delete(&key).await.err()));
        results.push(("exists", status, backend.exists(&key).await.err()));
    }
    let hits = target_hits();

    if let Ok(close) = Reflect::get(&state, &"close".into()) {
        let _ = close.unchecked_into::<Function>().call0(&JsValue::NULL);
    }

    assert_eq!(control.ok(), Some(200), "control: the harness follows");
    assert_eq!(
        control_hits,
        Some(1.0),
        "control: the target saw the follow"
    );
    for (op, status, err) in results {
        let err = err.unwrap_or_else(|| panic!("{op} on HTTP {status} must be an error"));
        assert_eq!(
            err.kind,
            BackendErrorKind::Permanent,
            "{op} on HTTP {status}: {err}"
        );
    }
    assert_eq!(hits, Some(0.0), "a redirect was followed");
}

/// The Workers backend sends requests to the URL it validated, as the parser
/// serialized it, never the raw input: workerd's URL parser can differ from
/// the validator's.
#[wasm_bindgen_test]
fn workers_backend_uses_the_validated_url_as_serialized() {
    use cachekit::backend::workers::WorkersCachekitIO;

    let backend = WorkersCachekitIO::builder()
        .api_key("test-key-never-sent")
        .api_url("https://api.cachekit.io\\@evil.example")
        .build()
        .expect("allowlisted host");
    assert_eq!(backend.api_url(), "https://api.cachekit.io/@evil.example");
}
