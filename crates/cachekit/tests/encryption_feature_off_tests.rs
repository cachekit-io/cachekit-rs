//! A build without the `encryption` cargo feature rejects every way of
//! configuring a key — the builder encryption calls, and `from_env()` with
//! `CACHEKIT_MASTER_KEY` set: accepting a key and building a plaintext client
//! is the silent downgrade protocol `spec/intent-presets.md` § Encryption
//! Activation, rule 1 forbids.
//!
//! The CI test job builds with `encryption`, which compiles this file to
//! nothing. Run it with:
//!   cargo test -p cachekit-rs --no-default-features --features cachekitio --test encryption_feature_off_tests

#![cfg(not(feature = "encryption"))]

use cachekit::{CacheKit, CacheKitBuilder, CachekitError};

fn assert_feature_missing(result: Result<CacheKitBuilder, CachekitError>, call: &str) {
    match result {
        Err(CachekitError::Config(msg)) => assert!(
            msg.contains(call) && msg.contains("`encryption` cargo feature"),
            "{call}: error must name the call and the missing feature, got: {msg}"
        ),
        Err(e) => panic!("{call}: expected a Config error, got {e:?}"),
        Ok(_) => panic!("{call}: accepted a key in a build that cannot encrypt"),
    }
}

#[test]
fn every_builder_encryption_method_is_a_config_error() {
    let key = [7u8; 32];
    assert_feature_missing(
        CacheKit::builder().encryption(&"07".repeat(32), "t"),
        ".encryption()",
    );
    assert_feature_missing(
        CacheKit::builder().encryption_from_bytes(&key, "t"),
        ".encryption_from_bytes()",
    );
    assert_feature_missing(
        CacheKit::builder().encryption_from_bytes_with_previous(&key, &[&[8u8; 32]], "t"),
        ".encryption_from_bytes_with_previous()",
    );
}

#[cfg(all(feature = "cachekitio", not(target_arch = "wasm32")))]
mod common;

#[cfg(all(feature = "cachekitio", not(target_arch = "wasm32")))]
#[test]
#[serial_test::serial]
fn from_env_with_a_master_key_is_a_config_error() {
    let key = "07".repeat(32);
    let _env = common::EnvGuard::set(&[
        ("CACHEKIT_API_KEY", Some("test-key")),
        ("CACHEKIT_API_URL", None),
        ("CACHEKIT_MASTER_KEY", Some(key.as_str())),
        ("CACHEKIT_PREVIOUS_MASTER_KEYS", None),
        ("CACHEKIT_DEFAULT_TTL", None),
    ]);
    assert_feature_missing(CacheKit::from_env(), "CACHEKIT_MASTER_KEY");

    // Control: the same environment without the key builds, so the error
    // above is the key's and not some other misconfiguration.
    let _no_key = common::EnvGuard::set(&[("CACHEKIT_MASTER_KEY", None)]);
    assert!(
        CacheKit::from_env().is_ok(),
        "from_env without a key must build"
    );
}
