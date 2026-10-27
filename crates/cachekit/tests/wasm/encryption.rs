//! wasm32 runtime tests for [`EncryptionLayer`]: its seal, its open and its
//! rejects, against the shared protocol vectors (`tests/vectors/encryption.json`)
//! and an independent AES-256-GCM implementation.
//!
//! On wasm32 the layer seals and opens with RustCrypto `aes-gcm`, not ring, so
//! the native vector tests never run this code. A seal-then-open round trip
//! would not catch a fault here either: both directions share the same AES-GCM
//! core, and the protocol vectors stop at 59 bytes. So each seal is compared
//! byte for byte with the host's WebCrypto (`crypto.subtle`, OpenSSL under
//! Node) over the nonce the layer emitted, at lengths from 0 to 64 KiB.
//!
//! WebCrypto needs the derived key, which the layer keeps private, so the test
//! derives it independently: the protocol's HKDF salt construction
//! (`spec/encryption.md`, "Key Derivation") ported here, HKDF-SHA256 itself
//! from WebCrypto, checked against the file's `derived_key_fingerprint_hex`.
//!
//! Rows are selected as the native `encryption_vector_tests.rs` selects them:
//! a row binds this reader when its `aad_hex` is the AAD the layer builds for
//! the row's `cache_key`.

use cachekit::{CachekitError, EncryptionLayer};
use serde_json::Value as Json;
use sha2::{Digest, Sha256};
use wasm_bindgen_test::wasm_bindgen_test;
use worker::js_sys::{Array, Function, Promise, Uint8Array};
use worker::wasm_bindgen::{JsCast, JsValue};
use worker::wasm_bindgen_futures::JsFuture;

const VECTORS_JSON: &str = include_str!("../vectors/encryption.json");

/// The error every failed authentication reaches a reader as; the native
/// tests assert the same text (`tests/common/mod.rs`).
const AUTH_FAILURE: &str = "decrypt failed: Authentication verification failed";

/// Plaintext lengths for the seal comparison: empty, one byte, either side of
/// and on the 16-byte AES block, 1 KiB and 64 KiB.
const SEAL_LENGTHS: [usize; 7] = [0, 1, 15, 16, 17, 1024, 65_536];

const NONCE_LEN: usize = 12;
const TAG_LEN: usize = 16;

/// HKDF-SHA256 from WebCrypto: `(master, salt, info)` to 32 bytes.
const HKDF_JS: &str = "return crypto.subtle.importKey('raw', master, 'HKDF', false, ['deriveBits'])
    .then(k => crypto.subtle.deriveBits({ name: 'HKDF', hash: 'SHA-256', salt, info }, k, 256));";

/// AES-256-GCM seal from WebCrypto: `ciphertext || tag`.
const SEAL_JS: &str = "return crypto.subtle.importKey('raw', key, 'AES-GCM', false, ['encrypt'])
    .then(k => crypto.subtle.encrypt({ name: 'AES-GCM', iv, additionalData: aad, tagLength: 128 }, k, pt));";

/// AES-256-GCM open from WebCrypto; rejects when authentication fails.
const OPEN_JS: &str = "return crypto.subtle.importKey('raw', key, 'AES-GCM', false, ['decrypt'])
    .then(k => crypto.subtle.decrypt({ name: 'AES-GCM', iv, additionalData: aad, tagLength: 128 }, k, ct));";

fn vectors() -> Json {
    serde_json::from_str(VECTORS_JSON).expect("vendored vector file must be valid JSON")
}

fn field<'a>(row: &'a Json, k: &str) -> &'a str {
    row[k]
        .as_str()
        .unwrap_or_else(|| panic!("row {} lacks {k}", row["name"]))
}

fn hex_field(row: &Json, k: &str) -> Vec<u8> {
    hex::decode(field(row, k)).unwrap_or_else(|e| panic!("row {} {k}: {e}", row["name"]))
}

fn group<'a>(doc: &'a Json, k: &str) -> &'a [Json] {
    doc[k]
        .as_array()
        .unwrap_or_else(|| panic!("vector file lacks {k}"))
}

/// The main master key and tenant every row here is sealed under.
fn layer(doc: &Json) -> EncryptionLayer {
    EncryptionLayer::new(&hex_field(doc, "master_key_hex"), field(doc, "tenant_id"))
        .expect("main vector key")
}

/// The rows of `rows` whose `aad_hex` is the AAD this reader builds for the
/// row's `cache_key` (the native `binding_rows`).
fn binding_rows<'a>(doc: &Json, rows: &'a [Json]) -> Vec<&'a Json> {
    let layer = layer(doc);
    rows.iter()
        .filter(|row| layer.build_aad(field(row, "cache_key"), false) == hex_field(row, "aad_hex"))
        .collect()
}

fn names<'a>(rows: &[&'a Json]) -> Vec<&'a str> {
    rows.iter().map(|row| field(row, "name")).collect()
}

/// Call the JS function `body` of `params`, which returns a promise of an
/// `ArrayBuffer`, with each input as a `Uint8Array`, and return the bytes.
async fn webcrypto(params: &str, body: &str, inputs: &[&[u8]]) -> Result<Vec<u8>, JsValue> {
    let args: Array = inputs
        .iter()
        .map(|bytes| JsValue::from(Uint8Array::from(*bytes)))
        .collect();
    let promise: Promise = Function::new_with_args(params, body)
        .apply(&JsValue::NULL, &args)?
        .unchecked_into();
    let buffer = JsFuture::from(promise).await?;
    Ok(Uint8Array::new(&buffer).to_vec())
}

/// The encryption-domain key for the file's master key and tenant, derived
/// without cachekit, and checked against the file's fingerprint
/// (`SHA-256("key_fingerprint_v1" || key)[0..16]`).
async fn derived_key(doc: &Json) -> Vec<u8> {
    const DOMAIN: &[u8] = b"encryption";
    let tenant = field(doc, "tenant_id").as_bytes();
    // salt = "cachekit_v1_" || u8 domain length || domain
    //        || u16 big-endian tenant length || tenant; info = domain.
    let mut salt = b"cachekit_v1_".to_vec();
    salt.push(u8::try_from(DOMAIN.len()).expect("domain fits a u8"));
    salt.extend_from_slice(DOMAIN);
    salt.extend_from_slice(
        &u16::try_from(tenant.len())
            .expect("tenant fits a u16")
            .to_be_bytes(),
    );
    salt.extend_from_slice(tenant);

    let key = webcrypto(
        "master, salt, info",
        HKDF_JS,
        &[&hex_field(doc, "master_key_hex"), &salt, DOMAIN],
    )
    .await
    .expect("WebCrypto HKDF");
    let fingerprint = Sha256::new()
        .chain_update(b"key_fingerprint_v1")
        .chain_update(&key)
        .finalize();
    assert_eq!(
        hex::encode(&fingerprint[..16]),
        field(doc, "derived_key_fingerprint_hex"),
        "independently derived key matches the vector file's fingerprint"
    );
    key
}

fn assert_auth_failure(result: Result<Vec<u8>, CachekitError>, what: &str) {
    match result {
        Err(CachekitError::Encryption(msg)) if msg == AUTH_FAILURE => {}
        other => panic!("{what}: expected Encryption({AUTH_FAILURE:?}), got {other:?}"),
    }
}

/// Every seal equals WebCrypto's seal of the same plaintext under the
/// independently derived key, the nonce the layer emitted and the row's
/// `aad_hex`, byte for byte; and the layer opens it again.
#[wasm_bindgen_test]
async fn encryption_layer_seal_matches_webcrypto_byte_for_byte() {
    let doc = vectors();
    let layer = layer(&doc);
    let key = derived_key(&doc).await;
    let rows = binding_rows(&doc, group(&doc, "vectors"));
    assert!(!rows.is_empty(), "no vectors row binds this reader");

    for row in rows {
        let (name, cache_key, aad) = (
            field(row, "name"),
            field(row, "cache_key"),
            hex_field(row, "aad_hex"),
        );
        for len in SEAL_LENGTHS {
            // Not block-periodic, so a misplaced block cannot match by accident.
            let plaintext: Vec<u8> = (0..251u8).cycle().take(len).collect();
            let sealed = layer
                .encrypt(&plaintext, cache_key)
                .unwrap_or_else(|e| panic!("{name}/{len}: encrypt: {e}"));
            assert_eq!(
                sealed.len(),
                NONCE_LEN + len + TAG_LEN,
                "{name}/{len}: sealed length"
            );

            let nonce = &sealed[..NONCE_LEN];
            let expected = webcrypto(
                "key, iv, aad, pt",
                SEAL_JS,
                &[&key, nonce, &aad, &plaintext],
            )
            .await
            .unwrap_or_else(|e| panic!("{name}/{len}: WebCrypto seal: {e:?}"));
            assert!(
                sealed[NONCE_LEN..] == expected[..],
                "{name}/{len}: EncryptionLayer's ciphertext || tag differs from WebCrypto's"
            );

            let opened = layer
                .decrypt(&sealed, cache_key)
                .unwrap_or_else(|e| panic!("{name}/{len}: decrypt: {e}"));
            assert!(opened == plaintext, "{name}/{len}: opens to its plaintext");
        }
    }
}

/// The `vectors` rows this reader builds the AAD of decrypt to their
/// plaintext.
#[wasm_bindgen_test]
fn encryption_layer_decrypts_the_vectors_it_binds() {
    let doc = vectors();
    let layer = layer(&doc);
    let rows = binding_rows(&doc, group(&doc, "vectors"));
    assert_eq!(
        names(&rows),
        ["basic_bytes", "special_cache_key"],
        "vectors rows whose AAD this reader builds"
    );
    for row in rows {
        let name = field(row, "name");
        let plaintext = layer
            .decrypt(&hex_field(row, "ciphertext_hex"), field(row, "cache_key"))
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(plaintext, hex_field(row, "plaintext_hex"), "{name}");
    }
}

/// The `aad_reject_vectors` rows this reader builds the AAD of fail
/// authentication. Each is first shown to open, under WebCrypto, with the
/// `aad_hex` of the row it was sealed as, so a corrupt vector cannot pass as a
/// conforming reject.
#[wasm_bindgen_test]
async fn encryption_layer_rejects_the_aad_reject_rows_it_binds() {
    let doc = vectors();
    let layer = layer(&doc);
    let key = derived_key(&doc).await;
    let rows = binding_rows(&doc, group(&doc, "aad_reject_vectors"));
    assert_eq!(
        names(&rows),
        [
            "aad_compressed_false_sealed_true",
            "aad_without_original_type_sealed_with",
            "aad_key_with_prefix_sealed_without",
        ],
        "aad_reject_vectors rows whose AAD this reader builds"
    );

    for row in rows {
        let name = field(row, "name");
        let sealed_as = field(row, "sealed_as");
        let sealed = group(&doc, "vectors")
            .iter()
            .find(|v| v["name"] == sealed_as)
            .unwrap_or_else(|| panic!("{name}: sealed_as {sealed_as} is not in vectors"));
        let ciphertext = hex_field(row, "ciphertext_hex");
        let (nonce, body) = ciphertext.split_at(NONCE_LEN);
        let opened = webcrypto(
            "key, iv, aad, ct",
            OPEN_JS,
            &[&key, nonce, &hex_field(sealed, "aad_hex"), body],
        )
        .await
        .unwrap_or_else(|e| panic!("{name}: does not authenticate as {sealed_as}: {e:?}"));
        assert_eq!(
            opened,
            hex_field(sealed, "plaintext_hex"),
            "{name}: decrypts to {sealed_as}'s plaintext"
        );

        assert_auth_failure(layer.decrypt(&ciphertext, field(row, "cache_key")), name);
    }
}

/// A bound vector's ciphertext is rejected under another cache key, under
/// another tenant, and with one tag byte flipped.
#[wasm_bindgen_test]
fn encryption_layer_rejects_wrong_key_wrong_tenant_and_tampered_tag() {
    let doc = vectors();
    let layer = layer(&doc);
    let tenant = field(&doc, "tenant_id");
    let other_tenant = EncryptionLayer::new(
        &hex_field(&doc, "master_key_hex"),
        &format!("{tenant}-other"),
    )
    .expect("other tenant");

    for row in binding_rows(&doc, group(&doc, "vectors")) {
        let (name, cache_key) = (field(row, "name"), field(row, "cache_key"));
        let ciphertext = hex_field(row, "ciphertext_hex");
        // Positive control: the untouched read succeeds.
        layer
            .decrypt(&ciphertext, cache_key)
            .unwrap_or_else(|e| panic!("{name}: control read: {e}"));

        assert_auth_failure(
            layer.decrypt(&ciphertext, &format!("{cache_key}:other")),
            &format!("{name}: wrong cache key"),
        );
        assert_auth_failure(
            other_tenant.decrypt(&ciphertext, cache_key),
            &format!("{name}: wrong tenant"),
        );

        let mut tampered = ciphertext.clone();
        *tampered.last_mut().expect("non-empty ciphertext") ^= 0x01;
        assert_auth_failure(
            layer.decrypt(&tampered, cache_key),
            &format!("{name}: tampered tag"),
        );
    }
}
