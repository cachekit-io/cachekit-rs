//! wasm32 runtime regression tests for the session clock (LAB-1079).
//!
//! `SystemTime::now()` panics on `wasm32-unknown-unknown`, and the compile-only
//! wasm CI check shipped that trap in five releases — these tests *execute*
//! `session_headers()` (the code `WorkersCachekitIO::fetch` injects into every
//! request) on the real wasm32 target. If the target-gated clock in
//! `session.rs` is ever reverted to bare `SystemTime`, every test here traps
//! with `RuntimeError: unreachable` and the run goes red.
//!
//! Runs under `wasm-bindgen-test-runner` (Node) — see the `wasm` job in
//! `.github/workflows/ci.yml`.

#![cfg(all(target_arch = "wasm32", feature = "workers"))]

use cachekit::session::session_headers;
use wasm_bindgen_test::wasm_bindgen_test;

/// 2024-01-01T00:00:00Z — any honest clock reads after this.
const MS_2024: u64 = 1_704_067_200_000;
/// 2035-01-01T00:00:00Z — and before this.
const MS_2035: u64 = 2_051_222_400_000;

/// The exact panic site of LAB-1079: building session headers on wasm32.
/// Reaching the asserts at all proves the clock (and uuid's js entropy) did
/// not trap.
#[wasm_bindgen_test]
fn session_headers_do_not_trap_on_wasm32() {
    let headers = session_headers();
    assert_eq!(headers[0].0, "X-CacheKit-Session-ID");
    assert_eq!(headers[1].0, "X-CacheKit-Session-Start");
}

#[wasm_bindgen_test]
fn session_id_is_uuid_v4_on_wasm32() {
    let id = session_headers()[0].1;
    let parsed = uuid::Uuid::parse_str(id).expect("session ID should be a valid UUID");
    assert_eq!(parsed.get_version_num(), 4, "should be UUID v4");
}

/// AC-2: X-CacheKit-Session-Start carries a non-zero, plausible epoch-millis
/// value on wasm32 — asserted, not eyeballed.
#[wasm_bindgen_test]
fn session_start_is_plausible_epoch_millis_on_wasm32() {
    let start: u64 = session_headers()[1]
        .1
        .parse()
        .expect("session start should be numeric");
    assert!(start > MS_2024, "start {start} should be after 2024");
    assert!(start < MS_2035, "start {start} should be before 2035");
}

/// The `OnceLock` contract holds on wasm32: one session per isolate.
#[wasm_bindgen_test]
fn session_is_stable_across_calls_on_wasm32() {
    let h1 = session_headers();
    let h2 = session_headers();
    assert_eq!(h1[0].1, h2[0].1, "session ID should be stable");
    assert_eq!(h1[1].1, h2[1].1, "session start should be stable");
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
