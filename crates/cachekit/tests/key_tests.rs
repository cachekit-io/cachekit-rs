//! Tests for the internal `#[cachekit]` key derivation (`__private` plumbing).
//!
//! The pinned vector below is a byte-stability guard: `#[cachekit]` users'
//! cache keys must not change across releases without a deliberate,
//! reviewed migration (changing them invalidates every existing entry).

use cachekit::__private::generate_cache_key;

#[test]
fn key_pinned_vector() {
    // Blake2b-256 over msgpack tuple ("my_func", b"args"), "myns:" prefix.
    // If this fails, the key algorithm changed — that is a cache-invalidating
    // breaking change for every #[cachekit] user. Do not update this constant
    // without an explicit migration decision.
    // Independently verified (Python): blake2b-256 over msgpack
    // ("my_func", [97,114,103,115]) — note rmp_serde encodes &[u8] as an
    // int array, not msgpack bin.
    let key = generate_cache_key("myns", "my_func", b"args").unwrap();
    assert_eq!(
        key,
        "myns:e5c51a5beab59124858aec56f6811cc96fccfe69e9eab44fb11808ac54dbfbce"
    );
}

#[test]
fn key_deterministic() {
    let a = generate_cache_key("ns", "func", b"args").unwrap();
    let b = generate_cache_key("ns", "func", b"args").unwrap();
    assert_eq!(a, b);
}

#[test]
fn key_different_args() {
    let a = generate_cache_key("ns", "func", b"args1").unwrap();
    let b = generate_cache_key("ns", "func", b"args2").unwrap();
    assert_ne!(a, b);
}

#[test]
fn key_different_functions() {
    let a = generate_cache_key("ns", "func_a", b"args").unwrap();
    let b = generate_cache_key("ns", "func_b", b"args").unwrap();
    assert_ne!(a, b);
}

#[test]
fn key_without_namespace() {
    let key = generate_cache_key("", "func", b"args").unwrap();
    // No colon prefix — just the 64-char hex hash
    assert_eq!(key.len(), 64, "Without namespace, key is just the hash");
    assert!(key.chars().all(|c| c.is_ascii_hexdigit()), "Must be hex");
    assert!(!key.contains(':'), "No colon when namespace is empty");
}
