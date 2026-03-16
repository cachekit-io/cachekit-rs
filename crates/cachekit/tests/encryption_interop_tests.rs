//! Cross-SDK encryption interoperability tests.
//!
//! Uses REAL Python-generated test vectors from the TypeScript test suite.
//! The Rust SDK must decrypt ciphertext that was encrypted by the Python SDK
//! using identical master key, tenant ID, and AAD construction.
//!
//! Run with:
//!   cargo test --test encryption_interop_tests --features encryption

#![cfg(feature = "encryption")]

use cachekit::EncryptionLayer;

// ── Python test fixtures ────────────────────────────────────────────────────

/// Master key used by Python when generating the test vectors.
const MASTER_KEY_HEX: &str = "6161616161616161616161616161616161616161616161616161616161616161";

/// Tenant ID used by Python when generating the test vectors.
const TENANT_ID: &str = "cross-sdk-test";

struct TestVector {
    name: &'static str,
    plaintext_hex: &'static str,
    cache_key: &'static str,
    aad_hex: &'static str,
    ciphertext_hex: &'static str,
}

const VECTORS: &[TestVector] = &[
    TestVector {
        name: "basic_bytes",
        plaintext_hex: "0102030405060708",
        cache_key: "test:vector:1",
        aad_hex: "030000000e63726f73732d73646b2d746573740000000d\
                  746573743a766563746f723a31000000076d73677061636b\
                  0000000546616c7365",
        ciphertext_hex: "f8f69c70000000000000000087d9ec09f2c2347a\
                         4b37d5330ccf50e3c31e69642801bef5",
    },
    TestVector {
        name: "special_cache_key",
        plaintext_hex: "deadbeef",
        cache_key: "user@example.com:profile",
        aad_hex: "030000000e63726f73732d73646b2d74657374\
                  0000001875736572406578616d706c652e636f6d3a70726f66696c65\
                  000000076d73677061636b0000000546616c7365",
        ciphertext_hex: "f8f69c7000000000000000012bf77e0cfc353ab0\
                         4d0b941b22aa2ce39167ee63",
    },
];

// ── Helpers ─────────────────────────────────────────────────────────────────

fn make_layer() -> EncryptionLayer {
    let master_key = hex::decode(MASTER_KEY_HEX).unwrap();
    EncryptionLayer::new(&master_key, TENANT_ID).unwrap()
}

fn make_layer_with_tenant(tenant: &str) -> EncryptionLayer {
    let master_key = hex::decode(MASTER_KEY_HEX).unwrap();
    EncryptionLayer::new(&master_key, tenant).unwrap()
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[test]
fn aad_matches_python() {
    let layer = make_layer();

    for v in VECTORS {
        let rust_aad = layer.build_aad(v.cache_key, false);
        let rust_hex = hex::encode(&rust_aad);

        // Normalize: remove any whitespace from the expected hex (const may have line breaks)
        let expected: String = v.aad_hex.chars().filter(|c| !c.is_whitespace()).collect();

        assert_eq!(
            rust_hex, expected,
            "[{}] AAD mismatch.\n  expected: {expected}\n  actual:   {rust_hex}",
            v.name
        );
    }
}

#[test]
fn decrypt_python_ciphertext_basic() {
    let layer = make_layer();
    let v = &VECTORS[0];
    assert_eq!(v.name, "basic_bytes");

    let ciphertext_hex: String = v
        .ciphertext_hex
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    let ciphertext = hex::decode(&ciphertext_hex).unwrap();
    let expected_plaintext = hex::decode(v.plaintext_hex).unwrap();

    let decrypted = layer
        .decrypt(&ciphertext, v.cache_key)
        .expect("decrypting Python-generated ciphertext must succeed");

    assert_eq!(
        decrypted, expected_plaintext,
        "decrypted output does not match Python plaintext for vector '{}'",
        v.name
    );
}

#[test]
fn decrypt_python_ciphertext_special_key() {
    let layer = make_layer();
    let v = &VECTORS[1];
    assert_eq!(v.name, "special_cache_key");

    let ciphertext_hex: String = v
        .ciphertext_hex
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    let ciphertext = hex::decode(&ciphertext_hex).unwrap();
    let expected_plaintext = hex::decode(v.plaintext_hex).unwrap();

    let decrypted = layer
        .decrypt(&ciphertext, v.cache_key)
        .expect("decrypting Python-generated ciphertext must succeed");

    assert_eq!(
        decrypted, expected_plaintext,
        "decrypted output does not match Python plaintext for vector '{}'",
        v.name
    );
}

#[test]
fn wrong_aad_fails_decryption() {
    let layer = make_layer();
    let v = &VECTORS[0];

    let ciphertext_hex: String = v
        .ciphertext_hex
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    let ciphertext = hex::decode(&ciphertext_hex).unwrap();

    // Use the wrong cache key — AAD will differ, GCM authentication must fail
    let result = layer.decrypt(&ciphertext, "wrong:cache:key");
    assert!(
        result.is_err(),
        "decryption with wrong cache key (wrong AAD) must fail"
    );
}

#[test]
fn wrong_tenant_fails_decryption() {
    let v = &VECTORS[0];

    let ciphertext_hex: String = v
        .ciphertext_hex
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    let ciphertext = hex::decode(&ciphertext_hex).unwrap();

    // Different tenant → different derived key → GCM authentication must fail
    let wrong_layer = make_layer_with_tenant("wrong-tenant-id");
    let result = wrong_layer.decrypt(&ciphertext, v.cache_key);
    assert!(
        result.is_err(),
        "decryption with wrong tenant (different derived key) must fail"
    );
}
