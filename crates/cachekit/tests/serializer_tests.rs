use cachekit::serializer::{deserialize, serialize};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Point {
    x: i32,
    y: i32,
}

#[test]
fn roundtrip_struct() {
    let original = Point { x: 42, y: -7 };
    let bytes = serialize(&original).expect("serialize failed");
    let recovered: Point = deserialize(&bytes).expect("deserialize failed");
    assert_eq!(original, recovered);
}

#[test]
fn roundtrip_string() {
    let original = "hello, cachekit!".to_owned();
    let bytes = serialize(&original).expect("serialize failed");
    let recovered: String = deserialize(&bytes).expect("deserialize failed");
    assert_eq!(original, recovered);
}

#[test]
fn roundtrip_vec_of_ints() {
    let original: Vec<u64> = vec![1, 2, 3, 1_000_000];
    let bytes = serialize(&original).expect("serialize failed");
    let recovered: Vec<u64> = deserialize(&bytes).expect("deserialize failed");
    assert_eq!(original, recovered);
}

#[test]
fn wrong_type_fails_gracefully() {
    // Serialize an integer, try to deserialize as a struct — must return Err.
    let bytes = serialize(&42u32).expect("serialize failed");
    let result: Result<Point, _> = deserialize(&bytes);
    assert!(
        result.is_err(),
        "expected deserialization to fail for wrong type"
    );
}

#[test]
fn empty_bytes_fails() {
    let result: Result<Point, _> = deserialize(&[]);
    assert!(result.is_err(), "expected error for empty bytes");
}
