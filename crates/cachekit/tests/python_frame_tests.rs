//! Foreign auto-mode containers (`spec/wire-format.md` → SDK Storage
//! Containers, WIRE-21: an SDK MUST NOT decode another SDK's auto-mode
//! container), driven through every read path of this SDK's plain-MessagePack
//! value reader on a client without encryption. An encrypting client refuses
//! every vector in the file earlier, at decrypt:
//! `every_vector_is_refused_at_decrypt_by_an_encrypting_client`.
//!
//! Vectors: `tests/vectors/python-frame.json`, vendored verbatim from
//! cachekit-io/protocol `test-vectors/python-frame.json` at `main@b4ae567a`
//! (the file carries no `version` field;
//! sha256 `b677d5f14de4a3cd1fa5307d4b46eae0545a20e51e9163f96495ee7ad15600d0`).
//! Do not edit the JSON here; regenerate upstream and re-vendor.
//!
//! cachekit-rs stores plain MessagePack with no envelope, so the frame-parse
//! vectors bind cachekit-py only. What binds this reader is the CK frame: its
//! first byte `0x43` is a complete one-byte document, so auto mode refuses the
//! frame by its trailing bytes, and interop mode names its `CK` prefix. Every
//! CK-prefixed vector in the file is driven through both decoders and every
//! client read, with `IgnoredAny` as the target, so an outcome depends on the
//! bytes, never on a caller's type.
//!
//! `bare_envelope_fed_to_frame_reader` is a bare ByteStorage envelope, which is
//! one well-formed MessagePack document: no structural check can refuse it, and
//! this reader decodes it. `bare_envelope_decodes_but_not_as_its_value` records
//! that outcome, pins the refusal callers do get (a typed read of the
//! envelope's own value shape fails), and pins the positional match: a target
//! whose elements line up with the envelope's reads it.
//! `plain_msgpack_fed_to_frame_reader` is this
//! SDK's own format, so it is not driven through the plain reader.

mod common;

use cachekit::CachekitError;
use serde::de::IgnoredAny;
use serde_json::Value as Json;

use crate::common::{read_every_path, EnvelopedValue};

const VECTORS_JSON: &str = include_str!("vectors/python-frame.json");

/// sha256 of the vendored file, pinned so a local edit cannot drift from the
/// protocol copy unnoticed.
const VECTORS_SHA256: &str = "b677d5f14de4a3cd1fa5307d4b46eae0545a20e51e9163f96495ee7ad15600d0"; // pragma: allowlist secret

fn vectors() -> Json {
    serde_json::from_str(VECTORS_JSON).expect("vendored vector file must be valid JSON")
}

/// The refusal each path must give a CK frame: auto mode sees one document
/// plus trailing bytes, interop mode names the frame.
fn ck_frame_reason(path: &str) -> &'static str {
    match path {
        "serializer::deserialize" | "CacheKit::get" => "trailing",
        _ => "CK frame",
    }
}

#[test]
fn vendored_fixture_matches_the_pinned_sha256() {
    use sha2::{Digest, Sha256};
    assert_eq!(
        hex::encode(Sha256::digest(VECTORS_JSON.as_bytes())),
        VECTORS_SHA256,
        "tests/vectors/python-frame.json differs from the pinned protocol copy: \
         re-vendor it from protocol and update VECTORS_SHA256"
    );
}

#[test]
fn every_ck_frame_in_the_file_is_refused_on_every_path() {
    let doc = vectors();
    let mut names = Vec::new();
    for group in ["frame_vectors", "error_vectors", "encrypted_read_vectors"] {
        for v in doc[group].as_array().expect("vector group") {
            let hex = v["frame_hex"].as_str().expect("frame_hex");
            if !hex.starts_with("434b") {
                continue;
            }
            let name = v["name"].as_str().expect("name");
            let bytes = hex::decode(hex).expect("frame_hex is hex");
            for (path, result) in read_every_path::<IgnoredAny>(&bytes) {
                let want = ck_frame_reason(path);
                match result {
                    Err(CachekitError::Serialization(msg)) => assert!(
                        msg.contains(want),
                        "{name}: {path} must refuse a CK frame naming {want:?}, got: {msg}"
                    ),
                    other => panic!("{name}: {path} decoded a CK frame (WIRE-21), got {other:?}"),
                }
            }
            names.push(name);
        }
    }
    assert_eq!(names.len(), 29, "CK-prefixed vectors in python-frame.json");
    let paths: Vec<_> = read_every_path::<IgnoredAny>(b"\x00")
        .into_iter()
        .map(|(path, _)| path)
        .collect();
    assert_eq!(
        paths,
        [
            "serializer::deserialize",
            "interop::deserialize",
            "CacheKit::get",
            "CacheKit::interop_get",
            "CacheKit::interop_get_swr",
        ],
        "the sweep must cover every read path"
    );
    assert!(
        names.contains(&"ck_frame_fed_to_interop_reader"),
        "the WIRE-21 vector must be among the frames swept"
    );
}

#[test]
fn bare_envelope_decodes_but_not_as_its_value() {
    let doc = vectors();
    let hex = doc["error_vectors"]
        .as_array()
        .expect("error_vectors")
        .iter()
        .find(|v| v["name"] == "bare_envelope_fed_to_frame_reader")
        .and_then(|v| v["frame_hex"].as_str())
        .expect("bare_envelope_fed_to_frame_reader");
    let bytes = hex::decode(hex).expect("frame_hex is hex");

    for (path, result) in read_every_path::<IgnoredAny>(&bytes) {
        assert!(
            result.is_ok(),
            "{path}: a bare envelope is one well-formed MessagePack document, which this \
             reader decodes; if it now refuses it, update this test and the WIRE-21 record \
             in protocol's sdk-feature-matrix.md: {result:?}"
        );
    }
    for (path, result) in read_every_path::<EnvelopedValue>(&bytes) {
        assert!(
            matches!(result, Err(CachekitError::Serialization(_))),
            "{path}: a bare envelope must not decode as the value it wraps, got {result:?}"
        );
    }
    // rmp-serde decodes by position, so a target whose four elements line up
    // with the envelope's (bytes, 8 integers, an integer, a string) reads it.
    type EnvelopeShaped = (IgnoredAny, Vec<u8>, u64, String);
    for (path, result) in read_every_path::<EnvelopeShaped>(&bytes) {
        assert!(
            result.is_ok(),
            "{path}: an envelope-shaped target reads a bare envelope: {result:?}"
        );
    }
}

/// Every vector in the file, stored under the `cache_key` of the file's
/// `encrypted_reader` and read by an encrypting client with that reader's
/// master key and tenant. This reader parses no CK header: it hands the
/// stored bytes whole to AES-GCM as its own ciphertext, so each vector is
/// refused there, before a header field (`encrypted`, `compressed`, the
/// serializer name) or a decode is reached. That is where every
/// `encrypted_read_vectors` row meets its `fail_closed` here.
///
/// Each refusal is pinned to its exact message: vectors shorter than a nonce
/// and a tag (28 bytes) fail as malformed ciphertext (`SHORT_CIPHERTEXT`),
/// the rest fail authentication (`AUTH_FAILURE`). A reader that parsed the
/// CK header, or refused CK magic, before AES-GCM would fail some row here.
#[cfg(feature = "encryption")]
#[test]
fn every_vector_is_refused_at_decrypt_by_an_encrypting_client() {
    use crate::common::{
        encrypting_client_holding, every_read, runtime, AUTH_FAILURE, SHORT_CIPHERTEXT,
    };

    let doc = vectors();
    let reader = |k: &str| {
        doc["encrypted_reader"][k]
            .as_str()
            .unwrap_or_else(|| panic!("encrypted_reader lacks {k}"))
    };
    let key = reader("cache_key");
    let rt = runtime();
    let mut count = 0;
    let mut short = 0;
    for group in ["frame_vectors", "error_vectors", "encrypted_read_vectors"] {
        for v in doc[group].as_array().expect("vector group") {
            let name = v["name"].as_str().expect("name");
            if group == "encrypted_read_vectors" {
                assert_eq!(v["outcome"], "fail_closed", "{name}");
            }
            let bytes =
                hex::decode(v["frame_hex"].as_str().expect("frame_hex")).expect("frame_hex is hex");
            let expected = if bytes.len() < 12 + 16 {
                short += 1;
                SHORT_CIPHERTEXT
            } else {
                AUTH_FAILURE
            };
            let client = encrypting_client_holding(
                reader("master_key_hex"),
                reader("tenant_id"),
                None,
                key,
                bytes,
            );
            for (path, result) in rt.block_on(every_read::<IgnoredAny>(&client, key)) {
                match result {
                    Err(CachekitError::Encryption(msg)) if msg == expected => {}
                    other => {
                        panic!("{name}: {path} must be refused with {expected:?}, got {other:?}")
                    }
                }
            }
            count += 1;
        }
    }
    assert_eq!(count, 31, "vectors in python-frame.json");
    assert_eq!(short, 4, "vectors under 28 bytes");
}
