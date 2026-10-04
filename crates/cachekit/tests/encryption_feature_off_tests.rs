//! A build without the `encryption` cargo feature rejects every builder
//! encryption call: accepting a key and building a plaintext client is the
//! silent downgrade protocol `spec/intent-presets.md` § Encryption
//! Activation, rule 1 forbids.
//!
//! The CI test job builds with `encryption`, which compiles this file to
//! nothing. Run it with:
//!   cargo test -p cachekit-rs --no-default-features --test encryption_feature_off_tests

#![cfg(not(feature = "encryption"))]

use cachekit::{CacheKit, CacheKitBuilder, CachekitError};

fn assert_feature_missing(result: Result<CacheKitBuilder, CachekitError>, method: &str) {
    match result {
        Err(CachekitError::Config(msg)) => assert!(
            msg.contains(&format!(".{method}()")) && msg.contains("`encryption` cargo feature"),
            "{method}: error must name the call and the missing feature, got: {msg}"
        ),
        Err(e) => panic!("{method}: expected a Config error, got {e:?}"),
        Ok(_) => panic!("{method}: accepted a key in a build that cannot encrypt"),
    }
}

#[test]
fn every_builder_encryption_method_is_a_config_error() {
    let key = [7u8; 32];
    assert_feature_missing(
        CacheKit::builder().encryption(&"07".repeat(32), "t"),
        "encryption",
    );
    assert_feature_missing(
        CacheKit::builder().encryption_from_bytes(&key, "t"),
        "encryption_from_bytes",
    );
    assert_feature_missing(
        CacheKit::builder().encryption_from_bytes_with_previous(&key, &[&[8u8; 32]], "t"),
        "encryption_from_bytes_with_previous",
    );
}
