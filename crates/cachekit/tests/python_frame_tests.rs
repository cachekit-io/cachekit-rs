//! Foreign auto-mode containers (`spec/wire-format.md` → SDK Storage
//! Containers, WIRE-21: an SDK MUST NOT decode another SDK's auto-mode
//! container), driven through every read path of this SDK's plain-MessagePack
//! value reader.
//!
//! Vectors: `tests/vectors/python-frame.json`, vendored verbatim from
//! cachekit-io/protocol `test-vectors/python-frame.json`
//! (sha256 `1210a2cdf00ef420e59d4d1c75f4979385ad1cb023181b39f46d7f994761770a`).
//! Do not edit the JSON here; regenerate upstream and re-vendor.
//!
//! cachekit-rs stores plain MessagePack with no envelope, so the frame-parse
//! vectors bind cachekit-py only. What binds this reader is the CK frame: its
//! first byte `0x43` is a complete one-byte document, so a reader refuses the
//! frame only by rejecting the bytes after it. Every CK-prefixed vector in the
//! file is driven through both decoders and both client reads. The decoder
//! target is `IgnoredAny`, so an outcome depends on the bytes, never on a
//! caller's type.
//!
//! `bare_envelope_fed_to_frame_reader` is a bare ByteStorage envelope, which is
//! one well-formed MessagePack document: no structural check can refuse it, and
//! this reader decodes it. `bare_envelope_is_one_document_and_decodes` records
//! that outcome. A caller's typed read fails only if the envelope's shape does
//! not fit the caller's type. `plain_msgpack_fed_to_frame_reader` is this SDK's
//! own format, so it is not driven here.

mod common;

use cachekit::{interop, serializer, CacheKit, CachekitError};
use serde::de::IgnoredAny;
use serde_json::Value as Json;

use crate::common::MockBackend;

const VECTORS_JSON: &str = include_str!("vectors/python-frame.json");

/// sha256 of the vendored file, pinned so a local edit cannot drift from the
/// protocol copy unnoticed.
const VECTORS_SHA256: &str = "1210a2cdf00ef420e59d4d1c75f4979385ad1cb023181b39f46d7f994761770a"; // pragma: allowlist secret

fn vectors() -> Json {
    serde_json::from_str(VECTORS_JSON).expect("vendored vector file must be valid JSON")
}

/// `frame_hex` of the named vector, from any group.
fn frame(name: &str) -> Vec<u8> {
    let doc = vectors();
    let hex = ["frame_vectors", "error_vectors", "encrypted_read_vectors"]
        .iter()
        .flat_map(|group| doc[group].as_array().expect("vector group"))
        .find(|v| v["name"] == name)
        .and_then(|v| v["frame_hex"].as_str())
        .unwrap_or_else(|| panic!("vector {name} missing from python-frame.json"))
        .to_owned();
    hex::decode(hex).expect("frame_hex is hex")
}

/// Both decoders, then `get` and `interop_get` of a backend entry holding
/// exactly `bytes`. L1 is off so each client read reaches its decoder.
fn read_every_path(bytes: &[u8]) -> Vec<(&'static str, Result<(), CachekitError>)> {
    const KEY: &str = "python:frame:forged";
    let mut reads = vec![
        (
            "serializer::deserialize",
            serializer::deserialize::<IgnoredAny>(bytes).map(drop),
        ),
        (
            "interop::deserialize",
            interop::deserialize::<IgnoredAny>(bytes).map(drop),
        ),
    ];
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let (backend, handle) = MockBackend::new_with_handle();
    let client = CacheKit::builder()
        .backend(backend)
        .no_l1()
        .build()
        .expect("client builds");
    let found = |r: Result<Option<IgnoredAny>, CachekitError>| {
        r.map(|v| {
            v.expect("the forged entry must be found, not missed");
        })
    };
    runtime.block_on(async {
        handle
            .store
            .lock()
            .await
            .insert(KEY.to_owned(), bytes.to_vec());
        reads.push(("CacheKit::get", found(client.get(KEY).await)));
        reads.push((
            "CacheKit::interop_get",
            found(client.interop_get(KEY).await),
        ));
    });
    reads
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
fn ck_frame_fed_to_interop_reader_is_refused_on_every_path() {
    for (path, result) in read_every_path(&frame("ck_frame_fed_to_interop_reader")) {
        match result {
            // Auto mode names the trailing bytes; interop mode names the CK frame.
            Err(CachekitError::Serialization(msg)) => assert!(
                msg.contains("trailing") || msg.contains("CK frame"),
                "{path}: expected a trailing-bytes or CK-frame refusal, got: {msg}"
            ),
            other => panic!("{path}: decoded a CK frame (WIRE-21), got {other:?}"),
        }
    }
}

#[test]
fn every_ck_frame_in_the_file_is_refused_on_every_path() {
    let doc = vectors();
    let mut frames = 0;
    for group in ["frame_vectors", "error_vectors", "encrypted_read_vectors"] {
        for v in doc[group].as_array().expect("vector group") {
            let hex = v["frame_hex"].as_str().expect("frame_hex");
            if !hex.starts_with("434b") {
                continue;
            }
            let name = v["name"].as_str().expect("name");
            for (path, result) in read_every_path(&hex::decode(hex).expect("hex")) {
                assert!(
                    matches!(result, Err(CachekitError::Serialization(_))),
                    "{name}: {path} decoded a CK frame (WIRE-21), got {result:?}"
                );
            }
            frames += 1;
        }
    }
    assert_eq!(frames, 16, "CK-prefixed vectors in python-frame.json");
}

#[test]
fn bare_envelope_is_one_document_and_decodes() {
    for (path, result) in read_every_path(&frame("bare_envelope_fed_to_frame_reader")) {
        assert!(
            result.is_ok(),
            "{path}: a bare envelope is one well-formed MessagePack document, which this \
             reader decodes; if it now refuses it, update this test and the WIRE-21 record \
             in protocol's sdk-feature-matrix.md: {result:?}"
        );
    }
}
