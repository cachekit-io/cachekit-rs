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
//! authentication failure itself: a `CachekitError::Encryption` naming the
//! failed decrypt, never a `Serialization` error or a miss.
//!
//! Run with:
//!   cargo test --test encryption_vector_tests --features encryption

#![cfg(feature = "encryption")]

mod common;

use std::collections::BTreeMap;

use cachekit::{CacheKit, CachekitError, EncryptionLayer, SwrRead};
use common::MockBackend;
use serde::de::IgnoredAny;
use serde::Deserialize;
use serde_json::Value as Json;

const VECTORS_JSON: &str = include_str!("vectors/encryption.json");
const INTEROP_JSON: &str = include_str!("vectors/interop-mode.json");

/// The namespace a client needs for `aad_key_with_prefix_sealed_without`:
/// its `cache_key` is the sealed key with `app:` in front.
const PREFIX_NAMESPACE: &str = "app";

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

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
}

/// Every interop read of `key`: `interop_get` and `interop_get_swr`, with the
/// SWR hit flattened to the value.
async fn interop_reads(
    client: &CacheKit,
    key: &str,
) -> Vec<(&'static str, Result<Option<IgnoredAny>, CachekitError>)> {
    vec![
        (
            "CacheKit::interop_get",
            client.interop_get::<IgnoredAny>(key).await,
        ),
        (
            "CacheKit::interop_get_swr",
            client
                .interop_get_swr::<IgnoredAny>(key)
                .await
                .map(|read| match read {
                    SwrRead::Fresh(v) => Some(v),
                    SwrRead::Miss => None,
                    SwrRead::Stale(..) => panic!("with L1 off no read is stale"),
                }),
        ),
    ]
}

// ── aad_reject_vectors ───────────────────────────────────────────────────────

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
            reads.extend(rt.block_on(interop_reads(&client, key)));
        }
        for (path, result) in reads {
            match result {
                Err(CachekitError::Encryption(msg)) if msg.starts_with("decrypt failed") => {}
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
                .expect("container rows decrypt"),
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

/// The map inside the envelope (`python-frame.json`'s default write payload).
#[derive(Debug, Deserialize)]
#[allow(dead_code)] // decoded only to prove it is not
struct EnvelopedValue {
    user_id: u64,
    name: String,
    active: bool,
}

/// `container_envelope_to_plain_reader`, outcome `not_unwrapped`: this
/// reader decodes the plaintext as one MessagePack document, the envelope's
/// 4-element array, and never returns the map inside it.
#[test]
fn envelope_plaintext_is_not_unwrapped() {
    let row = container_row("container_envelope_to_plain_reader");
    assert_eq!(row["reader"], "plain_msgpack");
    assert_eq!(row["outcome"], "not_unwrapped");
    let key = field(&row, "cache_key");
    let client = client_holding(None, key, hex_field(&row, "ciphertext_hex"));
    let rt = runtime();

    let as_value = rt.block_on(client.get::<EnvelopedValue>(key));
    assert!(
        matches!(as_value, Err(CachekitError::Serialization(_))),
        "the envelope's inner value must not decode, got {as_value:?}"
    );
    let as_map = rt.block_on(client.get::<BTreeMap<String, IgnoredAny>>(key));
    assert!(
        matches!(as_map, Err(CachekitError::Serialization(_))),
        "no map must decode from the envelope, got {as_map:?}"
    );
    let as_document = rt.block_on(client.get::<Vec<IgnoredAny>>(key));
    assert_eq!(
        as_document
            .expect("one MessagePack document")
            .map(|v| v.len()),
        Some(4),
        "the plaintext decodes as the envelope's 4-element array"
    );
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
        for (path, result) in rt.block_on(interop_reads(&client, key)) {
            assert!(
                matches!(result, Err(CachekitError::Serialization(_))),
                "{name}: {path} must refuse the bytes after the first document, got {result:?}"
            );
        }
    }
}
