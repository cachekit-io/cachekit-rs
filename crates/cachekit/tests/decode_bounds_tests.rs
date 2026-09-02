//! Untrusted-decode bounds (LAB-2503): the shared protocol `decode-bounds.json`
//! vectors run against BOTH decode entry points, plus the depth-bound boundary.
//!
//! Vectors: `tests/vectors/decode-bounds.json`, vendored verbatim from
//! `cachekit-io/protocol` `test-vectors/decode-bounds.json`
//! (sha256 `864b7126986e9a2bd0dd50358018eda34fe2f70bca06ae9763e8ce6321f34b0a`).
//! Do not edit the JSON here; regenerate upstream and re-vendor.
//!
//! Why this exists: `rmp-serde` never pre-allocates from a header, so the only
//! amplification axis is recursion depth — and its 1024 default is NOT a safe
//! bound: a debug build overflows a 2 MiB thread stack (uncatchable abort)
//! somewhere between 512 and 768 nested arrays, i.e. a ~700-byte forged entry.
//! `serializer::MAX_DECODE_DEPTH` (100) makes the bound ours, and this file
//! fails if a dependency bump (or a new decode path bypassing
//! `bounded_deserializer`) re-opens it.

use cachekit::interop;
use cachekit::serializer::{self, MAX_DECODE_DEPTH};
use cachekit::CachekitError;
use serde_json::Value as Json;

const VECTORS_JSON: &str = include_str!("vectors/decode-bounds.json");

fn vectors() -> Json {
    serde_json::from_str(VECTORS_JSON).expect("vendored vector file must be valid JSON")
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("vector input_hex must be hex"))
        .collect()
}

/// Both untrusted decode entry points: auto-mode `get` and interop `interop_get`.
fn decode_both(bytes: &[u8]) -> [(&'static str, Result<Json, CachekitError>); 2] {
    [
        (
            "serializer::deserialize",
            serializer::deserialize::<Json>(bytes),
        ),
        ("interop::deserialize", interop::deserialize::<Json>(bytes)),
    ]
}

/// Rejecting a document nested N deep unwinds N recursion frames. Run each case on
/// a thread with the DEFAULT Rust thread stack (2 MiB) so the test also proves the
/// bound is reachable without overflowing an ordinary tokio worker stack.
fn on_default_stack<F: FnOnce() + Send + 'static>(f: F) {
    std::thread::Builder::new()
        .stack_size(2 << 20)
        .spawn(f)
        .expect("spawn")
        .join()
        .expect("decode must not panic or overflow the stack");
}

fn nested_fixarray(depth: usize) -> Vec<u8> {
    let mut v = vec![0x91u8; depth];
    v.push(0xc0);
    v
}

#[test]
fn vector_file_shape_is_the_vendored_one() {
    let v = vectors();
    assert_eq!(v["reject_vectors"].as_array().map(Vec::len), Some(10));
    assert_eq!(v["accept_vectors"].as_array().map(Vec::len), Some(2));
    assert_eq!(v["spec"], "spec/interop-mode.md#decode-bounds");
}

#[test]
fn every_reject_vector_is_rejected_by_both_decoders() {
    on_default_stack(|| {
        for vector in vectors()["reject_vectors"].as_array().unwrap() {
            let name = vector["name"].as_str().unwrap();
            let bytes = unhex(vector["input_hex"].as_str().unwrap());
            assert_eq!(
                bytes.len() as u64,
                vector["input_len"].as_u64().unwrap(),
                "{name}: input_len"
            );
            for (path, result) in decode_both(&bytes) {
                let err = result
                    .err()
                    .unwrap_or_else(|| panic!("{name}: {path} decoded a reject vector"));
                assert!(
                    matches!(err, CachekitError::Serialization(_)),
                    "{name}: {path} must fail as a catchable Serialization error, got {err:?}"
                );
            }
        }
    });
}

#[test]
fn every_accept_vector_decodes_on_both_paths() {
    on_default_stack(|| {
        for vector in vectors()["accept_vectors"].as_array().unwrap() {
            let name = vector["name"].as_str().unwrap();
            let bytes = unhex(vector["input_hex"].as_str().unwrap());
            for (path, result) in decode_both(&bytes) {
                let value = result
                    .unwrap_or_else(|e| panic!("{name}: {path} rejected an accept vector: {e}"));
                let depth = vector["nesting_depth"].as_u64().unwrap();
                assert_eq!(
                    nesting_depth(&value) as u64,
                    depth,
                    "{name}: {path} decoded to the wrong shape"
                );
            }
        }
    });
}

fn nesting_depth(v: &Json) -> usize {
    match v {
        Json::Array(items) => 1 + items.iter().map(nesting_depth).max().unwrap_or(0),
        Json::Object(map) => 1 + map.values().map(nesting_depth).max().unwrap_or(0),
        _ => 0,
    }
}

#[test]
fn depth_bound_is_owned_and_matches_the_typescript_sdk() {
    // The protocol requires 32 <= bound <= 1024; 100 matches cachekit-ts and stays
    // far below the stack-overflow region measured for debug builds (512..768).
    assert_eq!(MAX_DECODE_DEPTH, 100);
    on_default_stack(|| {
        for (path, result) in decode_both(&nested_fixarray(MAX_DECODE_DEPTH)) {
            assert_eq!(
                nesting_depth(&result.unwrap()),
                MAX_DECODE_DEPTH,
                "{path}: depth == bound must decode"
            );
        }
        for (path, result) in decode_both(&nested_fixarray(MAX_DECODE_DEPTH + 1)) {
            let msg = result
                .err()
                .unwrap_or_else(|| panic!("{path}: depth bound+1 decoded"))
                .to_string();
            assert!(
                msg.contains("depth limit exceeded"),
                "{path}: expected the depth-limit error, got {msg}"
            );
        }
    });
}
