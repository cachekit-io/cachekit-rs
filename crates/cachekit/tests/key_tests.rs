use cachekit::key::generate_cache_key;

#[test]
fn key_with_namespace() {
    let key = generate_cache_key("myns", "my_func", b"args").unwrap();
    // Format: {namespace}:{64-hex-char hash}
    let parts: Vec<&str> = key.splitn(2, ':').collect();
    assert_eq!(parts.len(), 2, "Expected namespace:hash format");
    assert_eq!(parts[0], "myns");
    assert_eq!(parts[1].len(), 64, "Hash must be 64 hex chars (Blake2b-256)");
    assert!(
        parts[1].chars().all(|c| c.is_ascii_hexdigit()),
        "Hash must be hex"
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
    assert!(
        key.chars().all(|c| c.is_ascii_hexdigit()),
        "Must be hex"
    );
    assert!(!key.contains(':'), "No colon when namespace is empty");
}
