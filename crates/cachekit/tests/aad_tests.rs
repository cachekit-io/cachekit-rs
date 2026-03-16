//! AAD v0x03 format protocol compliance tests.
//!
//! Verifies that the AAD (Additional Authenticated Data) format matches the
//! Python and TypeScript SDKs exactly. Cross-SDK AAD compatibility is critical:
//! a mismatch means ciphertext encrypted by one SDK cannot be decrypted by another.
//!
//! Run with:
//!   cargo test --test aad_tests --features encryption

#![cfg(feature = "encryption")]

use cachekit::EncryptionLayer;

/// 32-byte master key for AAD tests (content irrelevant — AAD does not depend on key material).
fn test_layer(tenant: &str) -> EncryptionLayer {
    let master_key =
        hex::decode("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f").unwrap();
    EncryptionLayer::new(&master_key, tenant).unwrap()
}

/// Python test vector for AAD v0x03 with tenant="test", key="mykey",
/// format="msgpack", compressed=false.
///
/// Expected hex (spaces removed):
///   03 00000004 74657374 00000005 6d796b6579 00000007 6d73677061636b 00000005 46616c7365
const PYTHON_AAD_HEX: &str = "0300000004746573740000000\
     56d796b657900000007\
     6d73677061636b00000005\
     46616c7365";

#[test]
fn aad_v03_basic_format() {
    let layer = test_layer("test");
    let aad = layer.build_aad("mykey", false);
    let actual_hex = hex::encode(&aad);

    assert_eq!(
        actual_hex, PYTHON_AAD_HEX,
        "AAD does not match Python test vector.\n  expected: {PYTHON_AAD_HEX}\n  actual:   {actual_hex}"
    );
}

#[test]
fn aad_v03_version_byte() {
    let layer = test_layer("test");
    let aad = layer.build_aad("mykey", false);

    assert_eq!(aad[0], 0x03, "first byte must be AAD version 0x03");
}

#[test]
fn aad_v03_compressed_true() {
    let layer = test_layer("test");
    let aad = layer.build_aad("mykey", true);

    // Python format: compressed flag is "True" (capital T, Python-style bool repr)
    assert!(
        aad.ends_with(b"True"),
        "compressed=true AAD must end with b\"True\"; got: {:?}",
        &aad[aad.len().saturating_sub(10)..]
    );

    // Verify the length prefix before "True" is correct (4 bytes)
    let true_len_offset = aad.len() - 4 - 4; // 4 for "True", 4 for length prefix
    let len_bytes: [u8; 4] = aad[true_len_offset..true_len_offset + 4]
        .try_into()
        .unwrap();
    let len = u32::from_be_bytes(len_bytes);
    assert_eq!(len, 4, "length prefix for \"True\" must be 4");
}

#[test]
fn aad_v03_different_keys_different_aad() {
    let layer = test_layer("test");
    let aad_a = layer.build_aad("key:alpha", false);
    let aad_b = layer.build_aad("key:beta", false);

    assert_ne!(
        aad_a, aad_b,
        "different cache keys must produce different AAD"
    );
}

#[test]
fn aad_v03_different_tenants_different_aad() {
    let layer_a = test_layer("tenant-alpha");
    let layer_b = test_layer("tenant-beta");

    let aad_a = layer_a.build_aad("same-key", false);
    let aad_b = layer_b.build_aad("same-key", false);

    assert_ne!(aad_a, aad_b, "different tenants must produce different AAD");
}
