//! `CACHEKIT_TELEMETRY` read from the real process environment
//! (spec/saas-api.md, "Telemetry switch"). One test in its own binary, so
//! setting the variable cannot race another test that builds a backend.
//!
//! Run with:
//!   cargo test --test telemetry_env_tests

#![cfg(all(feature = "cachekitio", not(target_arch = "wasm32")))]

use cachekit::backend::cachekitio::CachekitIO;
use cachekit::backend::Backend;

fn reports(setting: Option<bool>) -> bool {
    let mut builder = CachekitIO::builder().api_key("ck_test_key");
    if let Some(on) = setting {
        builder = builder.telemetry(on);
    }
    builder.build().expect("builds").l1_latency().is_some()
}

/// API-94 / API-95: both controls apply; the variable is read once, when the
/// backend is built, parsed by the spec's rule, and off from either wins.
#[test]
fn api_95_the_variable_is_read_once_at_build() {
    std::env::remove_var("CACHEKIT_TELEMETRY");
    assert!(reports(None), "unset: on during the closed beta");
    let built_on = CachekitIO::builder()
        .api_key("ck_test_key")
        .build()
        .expect("builds");

    for off in ["false", " OFF ", "0", "no", "anything else"] {
        std::env::set_var("CACHEKIT_TELEMETRY", off);
        assert!(!reports(None), "{off:?} is off");
        assert!(!reports(Some(true)), "{off:?} wins over the setting on");
    }
    assert!(
        built_on.l1_latency().is_some(),
        "a built backend keeps what it read at build"
    );

    std::env::set_var("CACHEKIT_TELEMETRY", "yes");
    assert!(reports(None));
    assert!(
        !reports(Some(false)),
        "the setting off wins over the variable on"
    );

    std::env::set_var("CACHEKIT_TELEMETRY", "");
    assert!(reports(None), "empty is unset");
    std::env::remove_var("CACHEKIT_TELEMETRY");
}
