//! Untrusted-decode bounds: the shared protocol `decode-bounds.json` vectors run
//! through every untrusted read path — both decode entry points, and every
//! client read (`CacheKit::get`, `CacheKit::interop_get`,
//! `CacheKit::interop_get_swr`) against a backend entry forged with the
//! vector's bytes — plus the depth-bound boundary.
//!
//! Vectors: `tests/vectors/decode-bounds.json`, vendored verbatim from
//! `cachekit-io/protocol` `test-vectors/decode-bounds.json` v1.1.0
//! (sha256 `907b025d2b270a0f60abd9296a8a1c864e69057c553ac7a70206b44256558916`).
//! Do not edit the JSON here; regenerate upstream and re-vendor.
//!
//! Every reject vector must fail with the structural guard's own error
//! (`decode bound:` from `serializer::check_structure`), not merely fail: a
//! stock decoder rejects the same bytes mid-decode, after it has materialised
//! part of the document, so "it errored" cannot tell a guarded reader from an
//! unguarded one. The client reads sit below the `#[cachekit]` macro's
//! conversion of a `Serialization` error into a cache miss, so they still see
//! the error itself. Why 100 and not rmp-serde's 1024, and why a header walk is
//! needed at all: see the rustdoc on `serializer::MAX_DECODE_DEPTH` and
//! `check_structure`. This file fails if a dependency bump (or a new decode
//! path bypassing `bounded_deserializer`) re-opens either bound.

mod common;

use cachekit::interop;
use cachekit::serializer::{self, MAX_DECODE_DEPTH};
use cachekit::{CacheKit, CachekitError, SwrRead};
use serde::de::IgnoredAny;
use serde::Deserialize;
use serde_json::Value as Json;

use crate::common::MockBackend;

const VECTORS_JSON: &str = include_str!("vectors/decode-bounds.json");

/// sha256 of the vendored file, pinned so a local edit cannot drift from the
/// protocol copy unnoticed.
const VECTORS_SHA256: &str = "907b025d2b270a0f60abd9296a8a1c864e69057c553ac7a70206b44256558916"; // pragma: allowlist secret

fn vectors() -> Json {
    serde_json::from_str(VECTORS_JSON).expect("vendored vector file must be valid JSON")
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("vector input_hex must be hex"))
        .collect()
}

type Read = (&'static str, Result<Json, CachekitError>);

/// Both untrusted decode entry points: auto-mode `get` and interop `interop_get`.
fn decode_both(bytes: &[u8]) -> [Read; 2] {
    [
        (
            "serializer::deserialize",
            serializer::deserialize::<Json>(bytes),
        ),
        ("interop::deserialize", interop::deserialize::<Json>(bytes)),
    ]
}

/// Every untrusted read path for `bytes`: both decoders directly, then every
/// client read of a backend entry holding exactly `bytes`. `get`
/// stores plain MessagePack (no envelope), so the forged entry is the vector
/// itself. L1 is off so each read reaches the backend and its decoder.
fn read_every_path(bytes: &[u8]) -> Vec<Read> {
    const KEY: &str = "decode:bounds:forged";
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
    let found = |r: Result<Option<Json>, CachekitError>| {
        r.map(|v| v.expect("the forged entry must be found, not missed"))
    };
    let mut reads = Vec::from(decode_both(bytes));
    runtime.block_on(async {
        handle
            .store
            .lock()
            .await
            .insert(KEY.to_owned(), bytes.to_vec());
        reads.push(("CacheKit::get", found(client.get::<Json>(KEY).await)));
        reads.push((
            "CacheKit::interop_get",
            found(client.interop_get::<Json>(KEY).await),
        ));
        reads.push((
            "CacheKit::interop_get_swr",
            client
                .interop_get_swr::<Json>(KEY)
                .await
                .map(|read| match read {
                    SwrRead::Fresh(v) => v,
                    _ => panic!("with L1 off the forged entry must be a fresh hit"),
                }),
        ));
    });
    reads
}

/// The guard's rejection, and nothing else: a `Serialization` error whose
/// message comes from `check_structure`.
fn assert_guard_rejected(what: &str, path: &str, result: Result<Json, CachekitError>) {
    let e = result
        .err()
        .unwrap_or_else(|| panic!("{what}: {path} decoded a document it must reject"));
    assert!(
        matches!(e, CachekitError::Serialization(ref m) if m.starts_with("decode bound:")),
        "{what}: {path} must fail in the structural guard, got {e:?}"
    );
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

/// `level` repeated `depth` times around `null`; `level` is one collection
/// header of one child (plus the key, for a map).
fn nest(level: &[u8], depth: usize) -> Vec<u8> {
    let mut v = level.repeat(depth);
    v.push(0xc0);
    v
}

/// `[[...[null]...]]`, `depth` levels.
fn nested_fixarray(depth: usize) -> Vec<u8> {
    nest(&[0x91], depth)
}

/// One level of every collection header width, each backed by exactly one child,
/// so the overclaim check never fires and only depth can reject.
const LEVELS: [(&str, &[u8]); 6] = [
    ("fixarray", &[0x91]),
    ("array16", &[0xdc, 0x00, 0x01]),
    ("array32", &[0xdd, 0x00, 0x00, 0x00, 0x01]),
    ("fixmap", &[0x81, 0xa0]),
    ("map16", &[0xde, 0x00, 0x01, 0xa0]),
    ("map32", &[0xdf, 0x00, 0x00, 0x00, 0x01, 0xa0]),
];

/// `[[...[]...]]`, `depth` levels: the innermost level is an EMPTY array, which
/// still counts as a level.
fn nested_fixarray_empty_leaf(depth: usize) -> Vec<u8> {
    let mut v = vec![0x91u8; depth - 1];
    v.push(0x90);
    v
}

#[test]
fn vector_file_shape_is_the_vendored_one() {
    let v = vectors();
    assert_eq!(v["version"], "1.1.0");
    assert_eq!(v["reject_vectors"].as_array().map(Vec::len), Some(17));
    assert_eq!(v["accept_vectors"].as_array().map(Vec::len), Some(3));
    assert_eq!(v["spec"], "spec/interop-mode.md#decode-bounds");
}

#[test]
fn vendored_fixture_matches_the_pinned_sha256() {
    use sha2::{Digest, Sha256};
    assert_eq!(
        hex::encode(Sha256::digest(VECTORS_JSON.as_bytes())),
        VECTORS_SHA256,
        "tests/vectors/decode-bounds.json differs from the pinned protocol copy: \
         re-vendor it from protocol and update VECTORS_SHA256"
    );
}

#[test]
fn every_reject_vector_is_rejected_by_the_guard_on_every_read_path() {
    on_default_stack(|| {
        for vector in vectors()["reject_vectors"].as_array().unwrap() {
            let name = vector["name"].as_str().unwrap();
            let bytes = unhex(vector["input_hex"].as_str().unwrap());
            for (path, result) in read_every_path(&bytes) {
                assert_guard_rejected(name, path, result);
            }
        }
    });
}

#[test]
fn every_accept_vector_decodes_on_every_read_path() {
    on_default_stack(|| {
        for vector in vectors()["accept_vectors"].as_array().unwrap() {
            let name = vector["name"].as_str().unwrap();
            let bytes = unhex(vector["input_hex"].as_str().unwrap());
            let depth = vector["nesting_depth"].as_u64().unwrap();
            for (path, result) in read_every_path(&bytes) {
                let value = result
                    .unwrap_or_else(|e| panic!("{name}: {path} rejected an accept vector: {e}"));
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
    let at_depth = |depth: usize| {
        let mut shapes: Vec<(String, Vec<u8>)> = LEVELS
            .iter()
            .map(|(shape, level)| (shape.to_string(), nest(level, depth)))
            .collect();
        // The innermost level is empty and must still count.
        shapes.push((
            "empty-leaf fixarray".into(),
            nested_fixarray_empty_leaf(depth),
        ));
        shapes
    };
    on_default_stack(move || {
        for (shape, bytes) in at_depth(MAX_DECODE_DEPTH) {
            for (path, result) in read_every_path(&bytes) {
                let value = result
                    .unwrap_or_else(|e| panic!("{shape}: {path}: depth == bound must decode: {e}"));
                assert_eq!(
                    nesting_depth(&value),
                    MAX_DECODE_DEPTH,
                    "{shape}: {path}: decoded to the wrong shape"
                );
            }
        }
        for (shape, bytes) in at_depth(MAX_DECODE_DEPTH + 1) {
            for (path, result) in read_every_path(&bytes) {
                assert_guard_rejected(&format!("{shape} at depth bound+1"), path, result);
            }
        }
    });
}

/// An ext value is a leaf, not a level: `rmp-serde` counts it as one, so its
/// backstop must sit high enough that an ext at the bound still decodes, while
/// one level more is still the walk's rejection.
#[test]
fn ext_leaf_at_the_bound_decodes() {
    const FIXEXT1: [u8; 3] = [0xd4, 0x01, 0x02];
    let ext_at = |depth: usize| {
        let mut v = vec![0x91u8; depth];
        v.extend_from_slice(&FIXEXT1);
        v
    };
    on_default_stack(move || {
        let bytes = ext_at(MAX_DECODE_DEPTH);
        for (path, result) in [
            (
                "serializer::deserialize",
                serializer::deserialize::<IgnoredAny>(&bytes),
            ),
            (
                "interop::deserialize",
                interop::deserialize::<IgnoredAny>(&bytes),
            ),
        ] {
            result
                .unwrap_or_else(|e| panic!("{path}: ext leaf at depth == bound must decode: {e}"));
        }
        let bytes = ext_at(MAX_DECODE_DEPTH + 1);
        for (path, result) in [
            (
                "serializer::deserialize",
                serializer::deserialize::<IgnoredAny>(&bytes),
            ),
            (
                "interop::deserialize",
                interop::deserialize::<IgnoredAny>(&bytes),
            ),
        ] {
            let e = result
                .err()
                .unwrap_or_else(|| panic!("{path}: ext leaf at depth bound+1 decoded"));
            assert!(
                matches!(e, CachekitError::Serialization(ref m) if m.starts_with("decode bound:")),
                "{path}: must fail in the structural guard, got {e:?}"
            );
        }
    });
}

/// Depth is the deepest path, not the number of collections: a closed empty
/// collection must release its level, so many shallow siblings stay legal.
#[test]
fn closed_collections_release_their_depth() {
    // [[], [], ... x 200] at depth 2, then [[[...]]] siblings at the bound.
    let mut wide = vec![0xdc, 0x00, 0xc8];
    wide.extend(std::iter::repeat_n(0x90, 200));
    let mut siblings = vec![0x92];
    siblings.extend(nested_fixarray(MAX_DECODE_DEPTH - 1));
    siblings.extend(nested_fixarray_empty_leaf(MAX_DECODE_DEPTH - 1));
    on_default_stack(move || {
        for (shape, bytes, depth) in [("wide", wide, 2), ("siblings", siblings, MAX_DECODE_DEPTH)] {
            for (path, result) in decode_both(&bytes) {
                let value = result.unwrap_or_else(|e| panic!("{shape}: {path}: {e}"));
                assert_eq!(nesting_depth(&value), depth, "{shape}: {path}");
            }
        }
    });
}

/// A recursive `Vec`-bearing target. `#[serde(untagged)]` decodes through serde's
/// `Content` buffer, which pre-allocates from `size_hint` like `Vec<T>` (1 MiB per
/// level; see `check_structure`), so WITHOUT the structural walk 50 nested
/// `array32(0xFFFFFFFF)` headers (250 bytes) request 50 MiB before EOF.
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
