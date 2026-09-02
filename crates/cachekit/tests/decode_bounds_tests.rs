//! Untrusted-decode bounds (LAB-2503): the shared protocol `decode-bounds.json`
//! vectors run against BOTH decode entry points, plus the depth-bound boundary.
//!
//! Vectors: `tests/vectors/decode-bounds.json`, vendored verbatim from
//! `cachekit-io/protocol` `test-vectors/decode-bounds.json`
//! (sha256 `fa8bc750a4911fe3663b9ab68f13438a3e924b6bc742e9f6763ca35ad2407476`).
//! Do not edit the JSON here; regenerate upstream and re-vendor.
//!
//! Why 100 and not rmp-serde's 1024, and why a header walk is needed at all:
//! see the rustdoc on `serializer::MAX_DECODE_DEPTH` and `check_structure`.
//! This file fails if a dependency bump (or a new decode path bypassing
//! `bounded_deserializer`) re-opens either bound.

use cachekit::interop;
use cachekit::serializer::{self, MAX_DECODE_DEPTH};
use cachekit::CachekitError;
use serde::Deserialize;
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

/// A recursive `Vec`-bearing target: serde's `Vec<T>` visitor pre-allocates
/// `min(declared, 1 MiB)` per level from the header, so WITHOUT the structural walk
/// 50 nested `array32(0xFFFFFFFF)` headers (250 bytes) cost 50 MiB before EOF.
/// `serde_json::Value` happens to allocate nothing here, which is why the vector
/// tests alone cannot catch a walk regression — this one can.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum Tree {
    Leaf(u8),
    Node(Vec<Tree>),
}

#[test]
fn vec_target_amplifier_is_rejected_before_any_allocation() {
    let mut bomb = Vec::new();
    for _ in 0..50 {
        bomb.extend_from_slice(&[0xdd, 0xff, 0xff, 0xff, 0xff]);
    }
    for (path, result) in [
        ("serializer", serializer::deserialize::<Tree>(&bomb)),
        ("interop", interop::deserialize::<Tree>(&bomb)),
    ] {
        let msg = result
            .err()
            .unwrap_or_else(|| panic!("{path}: decoded the amplifier"))
            .to_string();
        assert!(
            msg.contains("more elements than the input can back"),
            "{path}: {msg}"
        );
    }
    // …and a legitimately backed Vec-tree still decodes on both paths.
    let legit = [0x92, 0x92, 0x01, 0x02, 0x91, 0x03]; // [[1, 2], [3]]
    for tree in [
        serializer::deserialize::<Tree>(&legit).unwrap(),
        interop::deserialize::<Tree>(&legit).unwrap(),
    ] {
        let Tree::Node(children) = tree else {
            panic!("root must be a node")
        };
        let leaves: Vec<Vec<u8>> = children
            .iter()
            .map(|c| match c {
                Tree::Node(n) => n
                    .iter()
                    .map(|l| match l {
                        Tree::Leaf(v) => *v,
                        Tree::Node(_) => u8::MAX,
                    })
                    .collect(),
                Tree::Leaf(v) => vec![*v],
            })
            .collect();
        assert_eq!(leaves, vec![vec![1, 2], vec![3]]);
    }
}
