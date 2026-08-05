//! wasm32 runtime regression tests for the session clock (LAB-1079).
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

/// The exact panic site of LAB-1079: building session headers on wasm32.
/// Reaching the asserts at all proves the clock (and uuid's js entropy) did
/// not trap; the value asserts are AC-2 (non-zero, plausible epoch millis —
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
        start < 2_051_222_400_000,
        "start {start} should be before 2035"
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
        .api_url("https://cachekit-lab-1079.invalid")
        .allow_custom_host(true)
        .build()
        .expect("builder should accept a syntactically valid https URL");

    // Err(transient network failure) is the expected outcome; the regression
    // this guards is a wasm trap *before* the request is even built.
    let result = backend.get("lab-1079-regression-key").await;
    assert!(
        result.is_err(),
        ".invalid must not resolve — expected a network error, got {result:?}"
    );
}
