//! wasm32 runtime regression tests for the session clock, the Workers
//! backend's request path and, in `tests/wasm/encryption.rs`, the
//! `EncryptionLayer` seal, open and rejects.
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

// In this binary, not a second one: `make test-wasm` runs one test binary.
#[cfg(feature = "encryption")]
#[path = "wasm/encryption.rs"]
mod encryption;

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

    let backend = WorkersCachekitIO::builder(&no_bindings())
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

    let backend = WorkersCachekitIO::builder(&no_bindings())
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

    let backend = WorkersCachekitIO::builder(&no_bindings())
        .api_key("test-key-never-sent")
        .api_url("https://api.cachekit.io\\@evil.example")
        .build()
        .expect("allowlisted host");
    assert_eq!(backend.api_url(), "https://api.cachekit.io/@evil.example");
}

/// Points `globalThis.fetch` at two stalls: a `hang` path whose promise never
/// settles, and a `stall-body` path that answers `200` with a body that never
/// ends. Every request's `AbortSignal` lands in `globalThis.__ckStall.signals`.
const STALL_FETCH_JS: &str = r"
    const state = (globalThis.__ckStall = { signals: [] });
    const realFetch = globalThis.fetch;
    globalThis.fetch = (input, init) => {
        state.signals.push(init?.signal);
        const path = new URL(new Request(input).url).pathname;
        if (path === '/v1/cache/stall-body') {
            return Promise.resolve(new Response(new ReadableStream({}), { status: 200 }));
        }
        return new Promise(() => {});
    };
    state.close = () => { globalThis.fetch = realFetch; };
";

/// Run two futures to completion together; each output carries the
/// `Date.now()` millis at which it finished.
async fn join_timed<A: std::future::Future, B: std::future::Future>(
    a: A,
    b: B,
) -> ((A::Output, f64), (B::Output, f64)) {
    use std::task::Poll;
    use worker::js_sys::Date;

    let (mut a, mut b) = (std::pin::pin!(a), std::pin::pin!(b));
    let (mut done_a, mut done_b) = (None, None);
    std::future::poll_fn(|cx| {
        if done_a.is_none() {
            if let Poll::Ready(out) = a.as_mut().poll(cx) {
                done_a = Some((out, Date::now()));
            }
        }
        if done_b.is_none() {
            if let Poll::Ready(out) = b.as_mut().poll(cx) {
                done_b = Some((out, Date::now()));
            }
        }
        if done_a.is_some() && done_b.is_some() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    (done_a.expect("a finished"), done_b.expect("b finished"))
}

/// The Workers runtime's `fetch` sets no timeout, so the backend bounds each
/// attempt itself at the native budgets: 5 s for a read and 10 s for a write,
/// body included. Each stalled call fails with `Timeout` within its budget plus
/// 1 s and aborts its request. The three calls run concurrently, so the test
/// takes ~10 s rather than 20 s.
#[wasm_bindgen_test]
async fn workers_backend_times_out_a_stalled_request() {
    use cachekit::backend::workers::WorkersCachekitIO;
    use cachekit::backend::Backend;
    use cachekit::BackendErrorKind;
    use worker::js_sys::{global, Array, Date, Function, Reflect};
    use worker::wasm_bindgen::{JsCast, JsValue};

    let backend = WorkersCachekitIO::builder(&no_bindings())
        .api_key("test-key-never-sent")
        .build()
        .expect("default URL is valid");

    Function::new_no_args(STALL_FETCH_JS)
        .call0(&JsValue::NULL)
        .expect("patch fetch");
    let state = Reflect::get(&global(), &"__ckStall".into()).expect("stall state");

    let start = Date::now();
    let ((get_hang, get_hang_at), (((get_body, get_body_at), (set_hang, set_hang_at)), _)) =
        join_timed(
            backend.get("hang"),
            join_timed(
                backend.get("stall-body"),
                backend.set("hang", vec![1], None),
            ),
        )
        .await;

    let signals: Array = Reflect::get(&state, &"signals".into())
        .expect("signals")
        .unchecked_into();
    let aborted: Vec<bool> = signals
        .iter()
        .map(|s| {
            Reflect::get(&s, &"aborted".into())
                .ok()
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
        })
        .collect();
    if let Ok(close) = Reflect::get(&state, &"close".into()) {
        let _ = close.unchecked_into::<Function>().call0(&JsValue::NULL);
    }

    let calls = [
        ("get, no response", get_hang.err(), get_hang_at, 5_000.0),
        ("get, stalled body", get_body.err(), get_body_at, 5_000.0),
        ("set, no response", set_hang.err(), set_hang_at, 10_000.0),
    ];
    for (call, err, at, budget) in calls {
        let err = err.unwrap_or_else(|| panic!("{call}: must fail"));
        assert_eq!(err.kind, BackendErrorKind::Timeout, "{call}: {err}");
        let elapsed = at - start;
        assert!(
            elapsed >= budget - 50.0 && elapsed <= budget + 1_000.0,
            "{call}: gave up after {elapsed} ms, budget {budget} ms"
        );
    }
    assert_eq!(
        aborted,
        [true, true, true],
        "every stalled request is aborted"
    );
}

/// Await a JS `setTimeout` of `ms`, letting pending promise callbacks run.
async fn sleep_ms(ms: u32) {
    use worker::js_sys::{Function, Promise};
    use worker::wasm_bindgen::{JsCast, JsValue};
    use worker::wasm_bindgen_futures::JsFuture;

    let wait: Promise =
        Function::new_with_args("ms", "return new Promise(r => setTimeout(r, ms));")
            .call1(&JsValue::NULL, &ms.into())
            .expect("sleep")
            .unchecked_into();
    JsFuture::from(wait).await.expect("sleep resolves");
}

/// A request that answers before its budget leaves nothing behind: its
/// deadline timer and that timer's wasm callbacks are freed, so a long-lived
/// isolate does not grow with traffic.
#[wasm_bindgen_test]
async fn workers_backend_frees_the_deadline_of_a_completed_request() {
    use cachekit::backend::workers::WorkersCachekitIO;
    use cachekit::backend::Backend;
    use worker::js_sys::Function;
    use worker::wasm_bindgen::{externref_heap_live_count, JsCast, JsValue};

    let restore: Function = Function::new_no_args(
        "const old = globalThis.fetch;
         globalThis.fetch = () => Promise.resolve(new Response('ok'));
         return () => { globalThis.fetch = old; };",
    )
    .call0(&JsValue::NULL)
    .expect("patch fetch")
    .unchecked_into();
    let backend = WorkersCachekitIO::builder(&no_bindings())
        .api_key("test-key-never-sent")
        .build()
        .expect("default URL is valid");

    let warmup = backend.get("warmup").await;
    sleep_ms(0).await;
    let before = externref_heap_live_count();
    let mut results = Vec::new();
    for _ in 0..200 {
        results.push(backend.get("ok").await);
    }
    sleep_ms(0).await;
    let retained = externref_heap_live_count() - before;
    let _ = restore.call0(&JsValue::NULL);

    assert_eq!(warmup.ok(), Some(Some(b"ok".to_vec())));
    assert!(results
        .iter()
        .all(|r| r.as_ref().ok() == Some(&Some(b"ok".to_vec()))));
    assert!(
        retained < 10,
        "200 completed requests retained {retained} live JS references"
    );
}

/// A caller that drops a call mid-flight (say, racing it against a fallback)
/// aborts its request rather than leaving it outstanding with no deadline.
#[wasm_bindgen_test]
async fn workers_backend_aborts_a_dropped_request() {
    use cachekit::backend::workers::WorkersCachekitIO;
    use cachekit::backend::Backend;
    use std::task::Poll;
    use worker::js_sys::{global, Function, Reflect};
    use worker::wasm_bindgen::{JsCast, JsValue};

    let restore: Function = Function::new_no_args(
        "const old = globalThis.fetch;
         globalThis.fetch = (_input, init) => {
             globalThis.__ckDropped = init.signal;
             return new Promise((_resolve, reject) =>
                 init.signal.addEventListener('abort', () => reject(new Error('aborted'))));
         };
         return () => { globalThis.fetch = old; };",
    )
    .call0(&JsValue::NULL)
    .expect("patch fetch")
    .unchecked_into();
    let backend = WorkersCachekitIO::builder(&no_bindings())
        .api_key("test-key-never-sent")
        .build()
        .expect("default URL is valid");

    let mut call = backend.get("dropped");
    let pending = std::future::poll_fn(|cx| Poll::Ready(call.as_mut().poll(cx).is_pending())).await;
    drop(call);
    sleep_ms(0).await;
    let aborted = Reflect::get(&global(), &"__ckDropped".into())
        .and_then(|signal| Reflect::get(&signal, &"aborted".into()))
        .ok()
        .and_then(|v| v.as_bool());
    let _ = restore.call0(&JsValue::NULL);

    assert!(pending, "the call was in flight when dropped");
    assert_eq!(aborted, Some(true), "dropping the call aborts its request");
}

// ── Telemetry on Workers ─────────────────────────────────────────────────────
//
// spec/saas-api.md, "Optional Metrics Headers" and "Telemetry switch"; each
// test names the requirement it pins (API-NN).

/// Points `globalThis.fetch` at a stub that answers `404` and records each
/// request's headers, lower-cased, in `globalThis.__ckHeaders.seen`.
const HEADERS_FETCH_JS: &str = r"
    const state = (globalThis.__ckHeaders = { seen: [] });
    const realFetch = globalThis.fetch;
    globalThis.fetch = (input, init) => {
        const req = new Request(input, init);
        state.seen.push(Object.fromEntries(req.headers.entries()));
        return Promise.resolve(new Response(null, { status: 404 }));
    };
    state.close = () => { globalThis.fetch = realFetch; };
";

const OPTIONAL_HEADERS: [&str; 9] = [
    "x-cachekit-session-id",
    "x-cachekit-session-start",
    "x-cachekit-l1-hits",
    "x-cachekit-l2-hits",
    "x-cachekit-misses",
    "x-cachekit-l1-hit-rate",
    "x-cachekit-l1-latency-buckets",
    "x-cachekit-l1-latency-sum-ns",
    "x-cachekit-l1-latency-one-in",
];

/// The headers of the one request `backend.get` sends, by name.
async fn sent_headers(
    backend: &cachekit::backend::workers::WorkersCachekitIO,
) -> std::collections::HashMap<String, String> {
    use cachekit::backend::Backend;
    use worker::js_sys::{global, Array, Function, Object, Reflect};
    use worker::wasm_bindgen::{JsCast, JsValue};

    Function::new_no_args(HEADERS_FETCH_JS)
        .call0(&JsValue::NULL)
        .expect("patch fetch");
    let state = Reflect::get(&global(), &"__ckHeaders".into()).expect("state");
    let result = backend.get("k").await;
    let seen: Array = Reflect::get(&state, &"seen".into())
        .expect("seen")
        .unchecked_into();
    if let Ok(close) = Reflect::get(&state, &"close".into()) {
        let _ = close.unchecked_into::<Function>().call0(&JsValue::NULL);
    }
    assert_eq!(result.ok(), Some(None), "the stub answers 404");
    assert_eq!(seen.length(), 1);
    let entries = Object::entries(&seen.get(0).unchecked_into());
    entries
        .iter()
        .map(|pair| {
            let pair: Array = pair.unchecked_into();
            (
                pair.get(0).as_string().expect("name"),
                pair.get(1).as_string().expect("value"),
            )
        })
        .collect()
}

/// Live counters with an L1, as a client attaches them.
fn l1_counters() -> cachekit::metrics::MetricsProvider {
    std::sync::Arc::new(|| {
        Some(cachekit::L1Stats {
            l1_hits: 3,
            l2_hits: 2,
            misses: 5,
            l1_enabled: true,
        })
    })
}

/// Set (or, with `None`, delete) `globalThis.process.env.CACHEKIT_TELEMETRY`.
fn set_process_env(value: Option<&str>) {
    use worker::js_sys::{global, Reflect};
    let env = Reflect::get(&global(), &"process".into())
        .and_then(|process| Reflect::get(&process, &"env".into()))
        .expect("Node has process.env");
    match value {
        Some(v) => {
            Reflect::set(&env, &"CACHEKIT_TELEMETRY".into(), &v.into()).expect("set");
        }
        None => {
            Reflect::delete_property(&env.into(), &"CACHEKIT_TELEMETRY".into()).expect("delete");
        }
    }
}

/// A `worker::Env` with no bindings.
fn no_bindings() -> worker::Env {
    use worker::wasm_bindgen::JsCast;
    worker::js_sys::Object::new().unchecked_into()
}

/// A `worker::Env` holding one `CACHEKIT_TELEMETRY` binding.
fn bindings(value: &worker::wasm_bindgen::JsValue) -> worker::Env {
    use worker::js_sys::{Object, Reflect};
    use worker::wasm_bindgen::JsCast;
    let env = Object::new();
    Reflect::set(&env, &"CACHEKIT_TELEMETRY".into(), value).expect("set");
    env.unchecked_into()
}

/// API-82 / API-94: on by default, the Workers backend sends the session
/// and counter headers, and never the latency headers: the Workers clock
/// cannot time an in-memory hit, so it sends nothing rather than zeros.
#[wasm_bindgen_test]
async fn workers_api_82_telemetry_on_sends_no_latency_headers() {
    use cachekit::backend::workers::WorkersCachekitIO;
    use cachekit::backend::Backend;

    set_process_env(None);
    let backend = WorkersCachekitIO::builder(&no_bindings())
        .api_key("test-key-never-sent")
        .build()
        .expect("default URL is valid");
    backend.attach_metrics(l1_counters());
    assert!(
        backend.l1_latency().is_none(),
        "no report accumulates on Workers"
    );

    let headers = sent_headers(&backend).await;
    for name in &OPTIONAL_HEADERS[..6] {
        assert!(headers.contains_key(*name), "{name} missing: {headers:?}");
    }
    for name in &OPTIONAL_HEADERS[6..] {
        assert!(!headers.contains_key(*name), "{name} sent on Workers");
    }
    assert_eq!(
        headers.get("x-cachekit-l1-status").map(String::as_str),
        Some("miss")
    );
}

/// API-96: with telemetry off, no optional metrics header is sent;
/// `X-CacheKit-L1-Status` still is.
#[wasm_bindgen_test]
async fn workers_api_96_telemetry_off_sends_only_the_status() {
    use cachekit::backend::workers::WorkersCachekitIO;
    use cachekit::backend::Backend;

    set_process_env(None);
    let backend = WorkersCachekitIO::builder(&no_bindings())
        .api_key("test-key-never-sent")
        .telemetry(false)
        .build()
        .expect("default URL is valid");
    backend.attach_metrics(l1_counters());

    let headers = sent_headers(&backend).await;
    for name in OPTIONAL_HEADERS {
        assert!(
            !headers.contains_key(name),
            "{name} sent with telemetry off"
        );
    }
    assert_eq!(
        headers.get("x-cachekit-l1-status").map(String::as_str),
        Some("miss")
    );
}

/// API-94 / API-95: on Workers `CACHEKIT_TELEMETRY` is read from
/// `process.env` and from the bindings the backend is built with, once, at
/// build; off from either wins, and a non-string binding reads as its JSON
/// text.
#[wasm_bindgen_test]
async fn workers_api_95_reads_process_env_and_bindings_once() {
    use cachekit::backend::workers::WorkersCachekitIO;
    use cachekit::backend::Backend;
    use worker::wasm_bindgen::JsValue;

    let session_sent = |builder: cachekit::backend::workers::WorkersCachekitIOBuilder| async move {
        let backend = builder
            .api_key("test-key-never-sent")
            .build()
            .expect("default URL is valid");
        backend.attach_metrics(l1_counters());
        sent_headers(&backend)
            .await
            .contains_key("x-cachekit-session-id")
    };

    set_process_env(Some(" Off "));
    let off_in_env = session_sent(WorkersCachekitIO::builder(&no_bindings()).telemetry(true)).await;
    set_process_env(None);

    let off_in_bindings = session_sent(WorkersCachekitIO::builder(&bindings(&JsValue::from_bool(
        false,
    ))))
    .await;

    set_process_env(Some("on"));
    let off_in_one = session_sent(WorkersCachekitIO::builder(&bindings(&JsValue::from_str(
        "no",
    ))))
    .await;
    let on_in_both = session_sent(WorkersCachekitIO::builder(&bindings(&JsValue::from_f64(
        1.0,
    ))))
    .await;

    set_process_env(None);
    let built = WorkersCachekitIO::builder(&no_bindings())
        .api_key("test-key-never-sent")
        .build()
        .expect("default URL is valid");
    built.attach_metrics(l1_counters());
    set_process_env(Some("false"));
    let read_once = sent_headers(&built)
        .await
        .contains_key("x-cachekit-session-id");
    set_process_env(None);

    assert!(!off_in_env, "process.env off wins over the setting on");
    assert!(!off_in_bindings, "a binding of false is off");
    assert!(!off_in_one, "off from either source wins");
    assert!(on_in_both, "on in both is on");
    assert!(read_once, "the environment is read once, at build");
}

/// API-95: the bindings the backend is built with take part in the truth
/// table like any other control: with nothing set telemetry is on, an on in
/// `process.env` is on, and a binding off wins over the setting on.
#[wasm_bindgen_test]
async fn workers_api_95_bindings_join_the_truth_table() {
    use cachekit::backend::workers::WorkersCachekitIO;
    use cachekit::backend::Backend;
    use worker::wasm_bindgen::JsValue;

    let sends = |builder: cachekit::backend::workers::WorkersCachekitIOBuilder| async move {
        let backend = builder
            .api_key("test-key-never-sent")
            .build()
            .expect("default URL is valid");
        backend.attach_metrics(l1_counters());
        sent_headers(&backend)
            .await
            .contains_key("x-cachekit-session-id")
    };

    set_process_env(None);
    let nothing_set = sends(WorkersCachekitIO::builder(&no_bindings())).await;
    set_process_env(Some("1"));
    let env_on = sends(WorkersCachekitIO::builder(&no_bindings())).await;
    set_process_env(None);
    let binding_off_setting_on =
        sends(WorkersCachekitIO::builder(&bindings(&JsValue::from_str("off"))).telemetry(true))
            .await;

    assert!(nothing_set, "unset everywhere: on during the closed beta");
    assert!(env_on, "process.env on: on");
    assert!(
        !binding_off_setting_on,
        "a binding off wins over .telemetry(true)"
    );
}
