//! `aad_reject_vectors` and `decrypted_container` rows from the shared
//! protocol vectors (`spec/encryption.md` § AAD v0x03 Format, ENC-2 and
//! ENC-3), driven through an encrypting client over a backend that holds each
//! row's ciphertext.
//!
//! Vectors: `tests/vectors/encryption.json`, the copy
//! `tests/master_key_input_tests.rs` pins by sha256. Do not edit the JSON
//! here; regenerate upstream and re-vendor.
//!
//! This reader builds one AAD shape: four components, `format` `msgpack`,
//! `compressed` `False`. A row binds it when its `aad_hex` is that AAD for the
//! row's `cache_key`, and the tests select rows that way, so a row added
//! upstream that binds this reader fails the exact name list until it is
//! driven here.
//!
//! An AAD reject row decrypts under some other AAD to a plaintext that no
//! value decode tells apart from a miss, so each read asserts the
//! authentication failure itself: exactly [`AUTH_FAILURE`], never another
//! decrypt error (a malformed ciphertext), a `Serialization` error or a miss.
//! Each row's ciphertext is first shown to authenticate under the AAD it was
//! sealed with, so a corrupt vector cannot pass as a conforming reject.
//!
//! Run with:
//!   cargo test --test encryption_vector_tests --features encryption

#![cfg(feature = "encryption")]

mod common;

use std::collections::BTreeMap;

use cachekit::{CacheKit, CachekitError, EncryptionLayer};
use cachekit_core::encryption::key_derivation::derive_tenant_keys;
use cachekit_core::ZeroKnowledgeEncryptor;
use common::{interop_reads, runtime, EnvelopedValue, MockBackend};
use serde::de::IgnoredAny;
use serde_json::Value as Json;

const VECTORS_JSON: &str = include_str!("vectors/encryption.json");
const INTEROP_JSON: &str = include_str!("vectors/interop-mode.json");

/// The namespace a client needs for `aad_key_with_prefix_sealed_without`:
/// its `cache_key` is the sealed key with `app:` in front.
const PREFIX_NAMESPACE: &str = "app";

/// The one error an AES-GCM tag mismatch reaches a reader as.
const AUTH_FAILURE: &str = "decrypt failed: Authentication verification failed";

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

/// The main master key and tenant every row here is sealed under.
fn layer() -> EncryptionLayer {
    let doc = vectors();
    EncryptionLayer::new(&hex_field(&doc, "master_key_hex"), field(&doc, "tenant_id"))
        .expect("main vector key")
}

/// The rows of `group` whose `aad_hex` is the AAD this reader builds for the
/// row's `cache_key`.
fn binding_rows(group: &[Json]) -> Vec<Json> {
    let layer = layer();
    group
        .iter()
        .filter(|row| layer.build_aad(field(row, "cache_key"), false) == hex_field(row, "aad_hex"))
        .cloned()
        .collect()
}

fn names(rows: &[Json]) -> Vec<&str> {
    rows.iter().map(|row| field(row, "name")).collect()
}

/// An encrypting client under the main key and tenant, L1 off so every read
/// reaches the backend, holding `ciphertext` at the backend key `stored_key`.
fn client_holding(namespace: Option<&str>, stored_key: &str, ciphertext: Vec<u8>) -> CacheKit {
    let doc = vectors();
    let (backend, handle) = MockBackend::new_with_handle();
    let mut builder = CacheKit::builder()
        .backend(backend)
        .no_l1()
        .encryption(field(&doc, "master_key_hex"), field(&doc, "tenant_id"))
        .expect("main vector key");
    if let Some(ns) = namespace {
        builder = builder.namespace(ns);
    }
    handle
        .store
        .try_lock()
        .expect("fresh store")
        .insert(stored_key.to_owned(), ciphertext);
    builder.build().expect("client builds")
}

// ── aad_reject_vectors ───────────────────────────────────────────────────────

/// Positive control for an AAD reject row: its ciphertext decrypts, under the
/// main key and tenant and the `aad_hex` of the `vectors` row it names in
/// `sealed_as`, to that row's plaintext. Decrypted through cachekit-core
/// directly, because no cachekit reader presents another SDK's AAD.
fn assert_authenticates_as_sealed(doc: &Json, row: &Json) {
    let name = field(row, "name");
    let sealed_as = field(row, "sealed_as");
    let sealed = doc["vectors"]
        .as_array()
        .expect("vectors")
        .iter()
        .find(|v| v["name"] == sealed_as)
        .unwrap_or_else(|| panic!("{name}: sealed_as {sealed_as} is not in vectors"));
    let key = derive_tenant_keys(&hex_field(doc, "master_key_hex"), field(doc, "tenant_id"))
        .expect("main vector key")
        .encryption_key;
    let plaintext = ZeroKnowledgeEncryptor::new()
        .expect("encryptor")
        .decrypt_aes_gcm(
            &hex_field(row, "ciphertext_hex"),
            &key,
            &hex_field(sealed, "aad_hex"),
        )
        .unwrap_or_else(|e| panic!("{name}: does not authenticate as {sealed_as}: {e:?}"));
    assert_eq!(
        plaintext,
        hex_field(sealed, "plaintext_hex"),
        "{name}: decrypts to {sealed_as}'s plaintext"
    );
}

#[test]
fn aad_reject_rows_this_reader_builds_fail_authentication() {
    let doc = vectors();
    let rows = binding_rows(
        doc["aad_reject_vectors"]
            .as_array()
            .expect("aad_reject_vectors"),
    );
    assert_eq!(
        names(&rows),
        [
            "aad_compressed_false_sealed_true",
            "aad_without_original_type_sealed_with",
            "aad_key_with_prefix_sealed_without",
        ],
        "aad_reject_vectors rows whose AAD this reader builds"
    );

    let rt = runtime();
    for row in &rows {
        assert_authenticates_as_sealed(&doc, row);
        let name = field(row, "name");
        let cache_key = field(row, "cache_key");
        let ciphertext = hex_field(row, "ciphertext_hex");
        // The prefix row's client is asked for the key the row was sealed
        // under; its namespace puts the presented key in the backend and in
        // the AAD.
        let namespace = (name == "aad_key_with_prefix_sealed_without").then_some(PREFIX_NAMESPACE);
        let key = match namespace {
            Some(ns) => cache_key
                .strip_prefix(&format!("{ns}:"))
                .expect("the prefix row's cache_key carries the namespace"),
            None => cache_key,
        };
        let client = client_holding(namespace, cache_key, ciphertext);

        let mut reads = vec![("CacheKit::get", rt.block_on(client.get::<IgnoredAny>(key)))];
        // Interop reads refuse a namespaced client before any decrypt.
        if namespace.is_none() {
            reads.extend(rt.block_on(interop_reads::<IgnoredAny>(&client, key)));
        }
        for (path, result) in reads {
            match result {
                Err(CachekitError::Encryption(msg)) if msg == AUTH_FAILURE => {}
                other => panic!("{name}: {path} must fail authentication, got {other:?}"),
            }
        }
    }
}

// ── decrypted_container ──────────────────────────────────────────────────────

fn container_rows() -> Vec<Json> {
    let doc = vectors();
    let rows = binding_rows(
        doc["decrypted_container"]["vectors"]
            .as_array()
            .expect("decrypted_container.vectors"),
    );
    assert_eq!(
        names(&rows),
        [
            "container_envelope_to_plain_reader",
            "container_trailing_byte_to_interop_reader",
            "container_incomplete_tail_to_interop_reader",
        ],
        "decrypted_container rows whose AAD this reader builds"
    );
    for row in &rows {
        // A wrong key or tenant would fail at decrypt and pass the error
        // assertions below for the wrong reason.
        assert_eq!(
            layer()
                .decrypt(&hex_field(row, "ciphertext_hex"), field(row, "cache_key"))
                .unwrap_or_else(|e| panic!("{}: does not decrypt: {e:?}", row["name"])),
            hex_field(row, "plaintext_hex"),
            "{}",
            row["name"]
        );
    }
    rows
}

fn container_row(name: &str) -> Json {
    container_rows()
        .into_iter()
        .find(|row| row["name"] == name)
        .unwrap_or_else(|| panic!("{name}"))
}

/// `container_envelope_to_plain_reader`, outcome `not_unwrapped`: both
/// plain-MessagePack readers, auto mode (`get`) and interop mode
/// (`interop_get`, `interop_get_swr`), decode the plaintext as one MessagePack
/// document, the envelope's 4-element array, and never return the map inside
/// it.
#[test]
fn envelope_plaintext_is_not_unwrapped() {
    let row = container_row("container_envelope_to_plain_reader");
    assert_eq!(row["reader"], "plain_msgpack");
    assert_eq!(row["outcome"], "not_unwrapped");
    let key = field(&row, "cache_key");
    let client = client_holding(None, key, hex_field(&row, "ciphertext_hex"));
    let rt = runtime();

    let (values, maps, documents) = rt.block_on(async {
        let mut values = vec![(
            "CacheKit::get",
            client.get::<EnvelopedValue>(key).await.map(drop),
        )];
        let mut maps = vec![(
            "CacheKit::get",
            client
                .get::<BTreeMap<String, IgnoredAny>>(key)
                .await
                .map(drop),
        )];
        let mut documents = vec![(
            "CacheKit::get",
            client
                .get::<Vec<IgnoredAny>>(key)
                .await
                .map(|v| v.map(|v| v.len())),
        )];
        values.extend(
            interop_reads::<EnvelopedValue>(&client, key)
                .await
                .map(|(path, r)| (path, r.map(drop))),
        );
        maps.extend(
            interop_reads::<BTreeMap<String, IgnoredAny>>(&client, key)
                .await
                .map(|(path, r)| (path, r.map(drop))),
        );
        documents.extend(
            interop_reads::<Vec<IgnoredAny>>(&client, key)
                .await
                .map(|(path, r)| (path, r.map(|v| v.map(|v| v.len())))),
        );
        (values, maps, documents)
    });
    for (path, result) in values {
        assert!(
            matches!(result, Err(CachekitError::Serialization(_))),
            "{path}: the envelope's inner value must not decode, got {result:?}"
        );
    }
    for (path, result) in maps {
        assert!(
            matches!(result, Err(CachekitError::Serialization(_))),
            "{path}: no map must decode from the envelope, got {result:?}"
        );
    }
    for (path, result) in documents {
        assert_eq!(
            result.unwrap_or_else(|e| panic!("{path}: one MessagePack document: {e:?}")),
            Some(4),
            "{path}: the plaintext decodes as the envelope's 4-element array"
        );
    }
}

/// `container_trailing_byte_to_interop_reader` and
/// `container_incomplete_tail_to_interop_reader`, outcome `error`: an interop
/// read of the `interop-mode.json` key each is sealed under consumes exactly
/// one document after a decrypt, so a trailing byte or an incomplete second
/// document is a `Serialization` error.
#[test]
fn interop_plaintext_with_trailing_bytes_is_refused() {
    let interop: Json = serde_json::from_str(INTEROP_JSON).expect("interop-mode.json");
    let expected_key = |name: &str| {
        interop["key_vectors"]
            .as_array()
            .expect("key_vectors")
            .iter()
            .find(|v| v["name"] == name)
            .and_then(|v| v["expected_key"].as_str())
            .unwrap_or_else(|| panic!("interop-mode.json key {name}"))
            .to_owned()
    };
    let rt = runtime();
    for (name, interop_key) in [
        ("container_trailing_byte_to_interop_reader", "bool_null"),
        (
            "container_incomplete_tail_to_interop_reader",
            "issue_example_mixed",
        ),
    ] {
        let row = container_row(name);
        assert_eq!(row["reader"], "interop");
        assert_eq!(row["outcome"], "error");
        let key = field(&row, "cache_key");
        assert_eq!(key, expected_key(interop_key), "{name}'s interop key");
        let client = client_holding(None, key, hex_field(&row, "ciphertext_hex"));
        for (path, result) in rt.block_on(interop_reads::<IgnoredAny>(&client, key)) {
            assert!(
                matches!(result, Err(CachekitError::Serialization(_))),
                "{name}: {path} must refuse the bytes after the first document, got {result:?}"
            );
        }
    }
}
