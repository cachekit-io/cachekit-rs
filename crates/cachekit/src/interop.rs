//! Interop mode (interop/v1): language-neutral cache keys and plain-MessagePack values.
//!
//! Implements the cross-SDK key and value formats from
//! `protocol/spec/interop-mode.md`, byte-verified against
//! `protocol/test-vectors/interop-mode.json` (see `tests/interop_vector_tests.rs`).
//!
//! # Key format
//!
//! ```text
//! {namespace}:{operation}:{args_hash}
//! ```
//!
//! `namespace` and `operation` are user-supplied, validated against
//! `^[a-z0-9][a-z0-9._-]{0,63}$` (full-string). `args_hash` is the Blake2b-256
//! digest (lowercase hex) of the canonical MessagePack encoding of the flat
//! argument array. Unlike auto mode, there is no `func:` segment — the
//! operation identity is explicit, so every SDK computes the same key for the
//! same logical call.
//!
//! # Why a hand-rolled encoder
//!
//! The spec makes the encoding *normative*: shortest-form integer/str/bin/array/map
//! headers, float64-only floats, map keys sorted by Unicode code point, sets
//! sorted by encoded bytes, and integral-float collapse. `rmp-serde` happens to
//! emit shortest forms but provides no sorting, no set semantics, and no number
//! canonicalization — hashing whatever serde produces would make key equality an
//! implementation accident. The closed [`InteropValue`] model plus an explicit
//! encoder is the only way to guarantee byte-identical hashes across SDKs.
//!
//! # Values
//!
//! Interop values are plain MessagePack documents — no ByteStorage envelope, no
//! LZ4, no checksum. `cachekit-rs` already writes plain MessagePack natively
//! (via [`crate::serializer`]), so regular [`crate::CacheKit::set`] output is
//! interop-readable as-is. Reads are the sharp edge: interop readers MUST
//! consume exactly one MessagePack document and reject trailing bytes — see
//! [`deserialize`].
//!
//! # Example
//!
//! ```
//! use cachekit::interop::{interop_key, InteropValue};
//!
//! let key = interop_key("users", "get_user", &[InteropValue::from(42i64)]).unwrap();
//! assert_eq!(
//!     key,
//!     "users:get_user:61598716255080080f6456eb065c2e51badfaa4320b0efe97469c29cffee8875"
//! );
//! ```

use std::collections::BTreeMap;

use blake2::{digest::consts::U32, Blake2b, Digest};
use serde::de::DeserializeOwned;

use crate::error::CachekitError;

type Blake2b256 = Blake2b<U32>;

/// Inclusive lower bound of the interop integer range: -2^63.
const INT_MIN: i128 = i64::MIN as i128;
/// Inclusive upper bound of the interop integer range: 2^64 - 1.
const INT_MAX: i128 = u64::MAX as i128;

/// -2^63 as float64 (exact; the collapse range lower bound, inclusive).
const F64_COLLAPSE_MIN: f64 = -9_223_372_036_854_775_808.0;
/// 2^64 as float64 (exact; the collapse range upper bound, EXCLUSIVE).
///
/// Spec: do NOT write 2^64-1 here — that literal rounds up to 2^64 as float64,
/// so the comparison must be strict-less-than against 2^64 itself.
const F64_COLLAPSE_MAX: f64 = 18_446_744_073_709_551_616.0;

// ── Data model ───────────────────────────────────────────────────────────────

/// A value in the closed interop/v1 argument data model.
///
/// Anything outside this model is unrepresentable by construction; values that
/// are representable but out of spec range (non-finite floats, integers outside
/// `[-2^63, 2^64-1]`) are rejected with an error at encode time — never
/// silently coerced.
#[derive(Debug, Clone, PartialEq)]
pub enum InteropValue {
    /// msgpack nil.
    Null,
    /// msgpack bool.
    Bool(bool),
    /// Integer. Must be within `[-2^63, 2^64-1]` (checked at encode time);
    /// `i128` storage lets a single variant span the full signed+unsigned range.
    Int(i128),
    /// Float. Must be finite (NaN / ±Inf rejected at encode time). In the
    /// argument profile, integral floats within `[-2^63, 2^64)` collapse to the
    /// integer encoding, so `2.0` and `2` hash identically (spec: number
    /// canonicalization).
    Float(f64),
    /// UTF-8 string, encoded as the msgpack str family. No Unicode
    /// normalization is applied. Rust strings are well-formed Unicode scalar
    /// sequences by construction, which the spec requires.
    Str(String),
    /// Byte string, encoded as the msgpack bin family (never str).
    Bytes(Vec<u8>),
    /// Ordered sequence; elements normalized recursively.
    Array(Vec<InteropValue>),
    /// String-keyed map. `BTreeMap`'s byte-wise key order **is** the spec's
    /// Unicode-code-point order (UTF-8 byte order ≡ code point order), applied
    /// at every nesting level.
    Map(BTreeMap<String, InteropValue>),
    /// Set: elements are normalized, encoded, sorted by their encoded bytes
    /// (unsigned lexicographic), and deduplicated post-normalization.
    Set(Vec<InteropValue>),
    /// Timezone-aware instant as microseconds since the Unix epoch.
    ///
    /// Sub-microsecond precision must be floored toward negative infinity
    /// before construction. Encoding performs the spec's single IEEE 754
    /// float64 division by 10^6, so hashes are bit-deterministic across SDKs.
    /// Naive (offset-less) datetimes are unrepresentable in this API.
    DateTime {
        /// Microseconds since `1970-01-01T00:00:00Z` (negative = pre-epoch).
        unix_micros: i64,
    },
    /// UUID, encoded as its lowercase hyphenated string form.
    Uuid(uuid::Uuid),
}

impl InteropValue {
    /// Build a byte string (msgpack bin).
    #[must_use]
    pub fn bytes(bytes: impl Into<Vec<u8>>) -> Self {
        Self::Bytes(bytes.into())
    }

    /// Build a timezone-aware datetime from microseconds since the Unix epoch.
    #[must_use]
    pub fn datetime_from_unix_micros(unix_micros: i64) -> Self {
        Self::DateTime { unix_micros }
    }
}

impl From<bool> for InteropValue {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}

impl From<i32> for InteropValue {
    fn from(v: i32) -> Self {
        Self::Int(i128::from(v))
    }
}

impl From<i64> for InteropValue {
    fn from(v: i64) -> Self {
        Self::Int(i128::from(v))
    }
}

impl From<u32> for InteropValue {
    fn from(v: u32) -> Self {
        Self::Int(i128::from(v))
    }
}

impl From<u64> for InteropValue {
    fn from(v: u64) -> Self {
        Self::Int(i128::from(v))
    }
}

impl From<i128> for InteropValue {
    /// Range is checked at encode time, not construction time.
    fn from(v: i128) -> Self {
        Self::Int(v)
    }
}

impl From<f64> for InteropValue {
    /// Finiteness is checked at encode time, not construction time.
    fn from(v: f64) -> Self {
        Self::Float(v)
    }
}

impl From<&str> for InteropValue {
    fn from(v: &str) -> Self {
        Self::Str(v.to_owned())
    }
}

impl From<String> for InteropValue {
    fn from(v: String) -> Self {
        Self::Str(v)
    }
}

// Deliberately NO `From<Vec<u8>>`: serde_json maps `Vec<u8>` to an array of
// numbers, so an implicit conversion to msgpack bin here would silently flip
// bin/array semantics for anyone carrying serde_json muscle memory — and a
// silently different encoding is a silently different cache key. Use the
// explicit `InteropValue::bytes()` constructor.

impl From<Vec<InteropValue>> for InteropValue {
    fn from(v: Vec<InteropValue>) -> Self {
        Self::Array(v)
    }
}

impl From<BTreeMap<String, InteropValue>> for InteropValue {
    fn from(v: BTreeMap<String, InteropValue>) -> Self {
        Self::Map(v)
    }
}

impl From<uuid::Uuid> for InteropValue {
    fn from(v: uuid::Uuid) -> Self {
        Self::Uuid(v)
    }
}

impl TryFrom<std::time::SystemTime> for InteropValue {
    type Error = CachekitError;

    /// Convert an instant to interop datetime micros, flooring sub-microsecond
    /// precision toward negative infinity (spec rule — truncation toward zero
    /// would differ for pre-epoch instants).
    fn try_from(t: std::time::SystemTime) -> Result<Self, CachekitError> {
        let out_of_range =
            || CachekitError::Serialization("datetime out of interop range".to_owned());
        match t.duration_since(std::time::UNIX_EPOCH) {
            Ok(after) => {
                let micros = i64::try_from(after.as_micros()).map_err(|_| out_of_range())?;
                Ok(Self::DateTime {
                    unix_micros: micros,
                })
            }
            Err(before) => {
                // Pre-epoch: flooring toward -inf rounds the magnitude UP.
                let micros_up = before.duration().as_nanos().div_ceil(1000);
                let micros = i64::try_from(micros_up).map_err(|_| out_of_range())?;
                Ok(Self::DateTime {
                    unix_micros: -micros,
                })
            }
        }
    }
}

// ── Segment validation ───────────────────────────────────────────────────────

/// Validate a key segment against `^[a-z0-9][a-z0-9._-]{0,63}$` as a
/// full-string match.
///
/// Byte-wise iteration over the whole string makes this a full match by
/// construction — a trailing `\n` (which Python's `re.match` + `$` would
/// accept) fails here, as the `reject_trailing_newline` vector requires.
fn validate_segment(kind: &str, segment: &str) -> Result<(), CachekitError> {
    let bytes = segment.as_bytes();
    let valid = matches!(bytes.first(), Some(b) if b.is_ascii_lowercase() || b.is_ascii_digit())
        && bytes.len() <= 64
        && bytes[1..].iter().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'-')
        });
    if valid {
        Ok(())
    } else {
        Err(CachekitError::InvalidKey(format!(
            "interop {kind} {segment:?} must match ^[a-z0-9][a-z0-9._-]{{0,63}}$ \
             (lowercase ASCII letters, digits, '.', '_', '-'; 1-64 chars)"
        )))
    }
}

// ── Public API ───────────────────────────────────────────────────────────────

/// Generate an interop/v1 cache key: `{namespace}:{operation}:{args_hash}`.
///
/// `args_hash` is Blake2b-256 (32-byte digest, unkeyed, lowercase hex) over
/// [`canonical_args`] of the flat argument array. The maximum possible key
/// length is 194 characters, so the auto-mode truncation rule never applies.
///
/// # Errors
///
/// - [`CachekitError::InvalidKey`] if `namespace` or `operation` fails the
///   segment grammar (rejected, never normalized).
/// - [`CachekitError::Serialization`] if any argument is outside the interop
///   data model's ranges (non-finite float, integer outside `[-2^63, 2^64-1]`).
pub fn interop_key(
    namespace: &str,
    operation: &str,
    args: &[InteropValue],
) -> Result<String, CachekitError> {
    validate_segment("namespace", namespace)?;
    validate_segment("operation", operation)?;

    let encoded = canonical_args(args)?;
    let mut hasher = Blake2b256::new();
    hasher.update(&encoded);
    let args_hash = hex::encode(hasher.finalize());

    Ok(format!("{namespace}:{operation}:{args_hash}"))
}

/// Encode the flat argument array to canonical MessagePack (argument profile:
/// number canonicalization applies, so integral floats collapse to ints).
///
/// This is the exact byte string hashed by [`interop_key`]; exposed so the
/// canonical bytes themselves can be verified against the protocol vectors.
///
/// # Errors
///
/// [`CachekitError::Serialization`] for out-of-model values (see [`interop_key`]).
pub fn canonical_args(args: &[InteropValue]) -> Result<Vec<u8>, CachekitError> {
    let mut buf = Vec::new();
    encode_array_header(&mut buf, args.len())?;
    for arg in args {
        encode(&mut buf, arg, Profile::Args)?;
    }
    Ok(buf)
}

/// Encode a single value to canonical MessagePack (value profile: floats are
/// **not** collapsed — a float value `2.0` stays float64 so it round-trips as
/// a float).
///
/// Interop values are plain MessagePack, so any serde-serialized document is
/// also a valid interop value; use this function when byte-canonical output
/// (sorted map keys, shortest forms) is required, e.g. to match the published
/// value vectors.
///
/// # Errors
///
/// [`CachekitError::Serialization`] for out-of-model values, and for
/// [`InteropValue::DateTime`] — temporal *values* use the wire-format sentinel
/// map (`{"__datetime__": true, "value": "<ISO-8601>"}`), not the
/// argument-hashing Unix-timestamp encoding; build the sentinel map explicitly.
pub fn serialize_value(value: &InteropValue) -> Result<Vec<u8>, CachekitError> {
    let mut buf = Vec::new();
    encode(&mut buf, value, Profile::Value)?;
    Ok(buf)
}

/// Deserialize an interop-mode MessagePack document, consuming **exactly one**
/// document and rejecting trailing bytes (spec MUST).
///
/// `rmp_serde::from_slice` silently ignores trailing bytes. That leniency is
/// dangerous here: a Python-SDK-internal CK frame begins `0x43` (`'C'`), which
/// is a *complete* one-byte MessagePack document (positive fixint 67) — a
/// lenient reader would silently decode an entire CK frame as the integer 67.
///
/// A payload with the `0x43 0x4B` (`"CK"`) prefix gets a specific diagnostic
/// naming the Python auto-mode frame instead of a generic trailing-bytes error.
///
/// # Errors
///
/// [`CachekitError::Serialization`] if the payload is a CK frame, is not valid
/// MessagePack, or has trailing bytes after the first document.
pub fn deserialize<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, CachekitError> {
    if bytes.starts_with(b"CK") {
        return Err(CachekitError::Serialization(
            "payload starts with 0x43 0x4B (\"CK\"): this is a Python-SDK-internal auto-mode \
             entry (CK frame), not an interop-mode value — it cannot be read cross-SDK"
                .to_owned(),
        ));
    }

    // `Read for &[u8]` advances the slice, so `remaining` ends up holding
    // whatever the decoder did not consume.
    let mut remaining: &[u8] = bytes;
    let mut de = rmp_serde::Deserializer::new(&mut remaining);
    let value = T::deserialize(&mut de)
        .map_err(|e| CachekitError::Serialization(format!("interop decode: {e}")))?;

    if !remaining.is_empty() {
        return Err(CachekitError::Serialization(format!(
            "interop payload has {} trailing byte(s) after the MessagePack document — \
             interop readers must consume exactly one document",
            remaining.len()
        )));
    }

    Ok(value)
}

// ── Canonical encoder ────────────────────────────────────────────────────────

/// Which canonicalization profile to encode with. The profiles differ in
/// exactly one rule: the argument profile collapses integral floats to ints
/// (keys need hash equality); the value profile does not (values need
/// round-trip fidelity).
#[derive(Clone, Copy)]
enum Profile {
    Args,
    Value,
}

fn encode(buf: &mut Vec<u8>, value: &InteropValue, profile: Profile) -> Result<(), CachekitError> {
    match value {
        InteropValue::Null => buf.push(0xc0),
        InteropValue::Bool(false) => buf.push(0xc2),
        InteropValue::Bool(true) => buf.push(0xc3),
        InteropValue::Int(i) => encode_int(buf, *i)?,
        InteropValue::Float(f) => encode_float(buf, *f, profile)?,
        InteropValue::Str(s) => encode_str(buf, s)?,
        InteropValue::Bytes(b) => encode_bin(buf, b)?,
        InteropValue::Array(items) => {
            encode_array_header(buf, items.len())?;
            for item in items {
                encode(buf, item, profile)?;
            }
        }
        InteropValue::Map(map) => {
            // BTreeMap iterates in ascending byte order of the UTF-8 keys,
            // which is exactly the spec's Unicode-code-point sort.
            encode_map_header(buf, map.len())?;
            for (key, val) in map {
                encode_str(buf, key)?;
                encode(buf, val, profile)?;
            }
        }
        InteropValue::Set(items) => {
            let mut encoded: Vec<Vec<u8>> = items
                .iter()
                .map(|item| {
                    let mut b = Vec::new();
                    encode(&mut b, item, profile)?;
                    Ok(b)
                })
                .collect::<Result<_, CachekitError>>()?;
            // Vec<u8> Ord is unsigned lexicographic — the spec's total order.
            // Sorting first makes dedup() remove ALL post-normalization
            // duplicates ({2, 2.0} collapses to the same bytes -> one element).
            encoded.sort();
            encoded.dedup();
            encode_array_header(buf, encoded.len())?;
            for bytes in &encoded {
                buf.extend_from_slice(bytes);
            }
        }
        InteropValue::DateTime { unix_micros } => {
            // The Unix-float encoding is an ARGUMENT rule (keys need byte-equal
            // hashes). Temporal VALUES use the wire-format sentinel-map
            // convention instead (round-trip fidelity) — a bare float64 value
            // would be off-spec bytes no other SDK revives as a datetime.
            if matches!(profile, Profile::Value) {
                return Err(CachekitError::Serialization(
                    "datetime interop VALUES use the wire-format sentinel map \
                     {\"__datetime__\": true, \"value\": \"<ISO-8601>\"} — build that map \
                     explicitly; the Unix-timestamp encoding is argument-hashing only"
                        .to_owned(),
                ));
            }
            // Spec: floor to integer micros, then ONE IEEE 754 float64
            // division by 10^6 — bit-deterministic in every language.
            #[allow(clippy::cast_precision_loss)] // the i64->f64 conversion IS the spec rule
            let ts = (*unix_micros as f64) / 1_000_000.0;
            encode_float(buf, ts, profile)?;
        }
        InteropValue::Uuid(u) => {
            // `Hyphenated` formats lowercase; 36 chars -> str8 header.
            encode_str(buf, &u.hyphenated().to_string())?;
        }
    }
    Ok(())
}

fn encode_int(buf: &mut Vec<u8>, i: i128) -> Result<(), CachekitError> {
    if !(INT_MIN..=INT_MAX).contains(&i) {
        return Err(CachekitError::Serialization(format!(
            "integer {i} is outside the interop range [-2^63, 2^64-1]"
        )));
    }
    // Range-checked above: non-negative fits u64, negative fits i64.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    if i >= 0 {
        encode_uint(buf, i as u64);
    } else {
        encode_negative_int(buf, i as i64);
    }
    Ok(())
}

/// Shortest-form unsigned encoding (spec: non-negative integers always use the
/// positive fixint / uint family).
#[allow(clippy::cast_possible_truncation)] // every arm is range-guarded by its branch
fn encode_uint(buf: &mut Vec<u8>, n: u64) {
    if n <= 0x7f {
        buf.push(n as u8); // positive fixint
    } else if n <= 0xff {
        buf.push(0xcc);
        buf.push(n as u8);
    } else if n <= 0xffff {
        buf.push(0xcd);
        buf.extend_from_slice(&(n as u16).to_be_bytes());
    } else if n <= 0xffff_ffff {
        buf.push(0xce);
        buf.extend_from_slice(&(n as u32).to_be_bytes());
    } else {
        buf.push(0xcf);
        buf.extend_from_slice(&n.to_be_bytes());
    }
}

/// Shortest-form signed encoding for strictly negative integers.
#[allow(clippy::cast_possible_truncation)] // every arm is range-guarded by its branch
fn encode_negative_int(buf: &mut Vec<u8>, n: i64) {
    debug_assert!(n < 0);
    if n >= -32 {
        buf.push((n as i8).to_be_bytes()[0]); // negative fixint (0xe0..0xff)
    } else if n >= i64::from(i8::MIN) {
        buf.push(0xd0);
        buf.push((n as i8).to_be_bytes()[0]);
    } else if n >= i64::from(i16::MIN) {
        buf.push(0xd1);
        buf.extend_from_slice(&(n as i16).to_be_bytes());
    } else if n >= i64::from(i32::MIN) {
        buf.push(0xd2);
        buf.extend_from_slice(&(n as i32).to_be_bytes());
    } else {
        buf.push(0xd3);
        buf.extend_from_slice(&n.to_be_bytes());
    }
}

fn encode_float(buf: &mut Vec<u8>, f: f64, profile: Profile) -> Result<(), CachekitError> {
    if !f.is_finite() {
        return Err(CachekitError::Serialization(format!(
            "{f} is not allowed in interop values (NaN and infinities are rejected, \
             never silently encoded)"
        )));
    }
    if matches!(profile, Profile::Args)
        && f.trunc() == f
        && (F64_COLLAPSE_MIN..F64_COLLAPSE_MAX).contains(&f)
    {
        // Integral and in range: collapse to the int encoding. The casts are
        // exact — f is integral, non-negative values are < 2^64, negative
        // values are >= -2^63. -0.0 compares >= 0.0 and casts to 0.
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        if f >= 0.0 {
            encode_uint(buf, f as u64);
        } else {
            encode_negative_int(buf, f as i64);
        }
    } else {
        // float64 (0xcb) only — float32 is forbidden by the spec.
        buf.push(0xcb);
        buf.extend_from_slice(&f.to_be_bytes());
    }
    Ok(())
}

fn encode_str(buf: &mut Vec<u8>, s: &str) -> Result<(), CachekitError> {
    let len = checked_len(s.len())?;
    #[allow(clippy::cast_possible_truncation)] // arms are range-guarded
    if len <= 31 {
        buf.push(0xa0 | (len as u8)); // fixstr
    } else if len <= 0xff {
        buf.push(0xd9);
        buf.push(len as u8);
    } else if len <= 0xffff {
        buf.push(0xda);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(0xdb);
        buf.extend_from_slice(&len.to_be_bytes());
    }
    buf.extend_from_slice(s.as_bytes());
    Ok(())
}

fn encode_bin(buf: &mut Vec<u8>, b: &[u8]) -> Result<(), CachekitError> {
    let len = checked_len(b.len())?;
    #[allow(clippy::cast_possible_truncation)] // arms are range-guarded
    if len <= 0xff {
        buf.push(0xc4);
        buf.push(len as u8);
    } else if len <= 0xffff {
        buf.push(0xc5);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(0xc6);
        buf.extend_from_slice(&len.to_be_bytes());
    }
    buf.extend_from_slice(b);
    Ok(())
}

fn encode_array_header(buf: &mut Vec<u8>, len: usize) -> Result<(), CachekitError> {
    let len = checked_len(len)?;
    #[allow(clippy::cast_possible_truncation)] // arms are range-guarded
    if len <= 15 {
        buf.push(0x90 | (len as u8)); // fixarray
    } else if len <= 0xffff {
        buf.push(0xdc);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(0xdd);
        buf.extend_from_slice(&len.to_be_bytes());
    }
    Ok(())
}

fn encode_map_header(buf: &mut Vec<u8>, len: usize) -> Result<(), CachekitError> {
    let len = checked_len(len)?;
    #[allow(clippy::cast_possible_truncation)] // arms are range-guarded
    if len <= 15 {
        buf.push(0x80 | (len as u8)); // fixmap
    } else if len <= 0xffff {
        buf.push(0xde);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(0xdf);
        buf.extend_from_slice(&len.to_be_bytes());
    }
    Ok(())
}

/// MessagePack length fields are at most u32.
fn checked_len(len: usize) -> Result<u32, CachekitError> {
    u32::try_from(len).map_err(|_| {
        CachekitError::Serialization(format!("length {len} exceeds the MessagePack u32 maximum"))
    })
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // Byte-level vector conformance lives in tests/interop_vector_tests.rs;
    // these unit tests pin the read guard and API-level behaviors that the
    // shared vectors cannot express.

    #[test]
    fn deserialize_single_document() {
        let value: i64 = deserialize(&[0x2a]).unwrap();
        assert_eq!(value, 42);
    }

    #[test]
    fn deserialize_rejects_trailing_bytes() {
        // fixint 42 followed by one stray byte — from_slice would accept this.
        let err = deserialize::<i64>(&[0x2a, 0x00]).unwrap_err();
        assert!(
            err.to_string().contains("trailing byte"),
            "expected trailing-bytes error, got: {err}"
        );
    }

    #[test]
    fn deserialize_rejects_ck_frame_with_diagnostic() {
        // A CK frame prefix: 0x43 alone is a complete msgpack document
        // (fixint 67), so a lenient reader decodes the frame as the int 67.
        let ck_frame = b"CK\x03\x00\x00\x00\x02{}payload";
        let err = deserialize::<i64>(ck_frame).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Python-SDK-internal auto-mode entry"),
            "expected the CK-frame diagnostic, got: {msg}"
        );
    }

    #[test]
    fn deserialize_matches_lenient_reader_on_clean_input() {
        let bytes = rmp_serde::to_vec(&("hello", 7u8)).unwrap();
        let strict: (String, u8) = deserialize(&bytes).unwrap();
        let lenient: (String, u8) = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(strict, lenient);
    }

    #[test]
    fn segment_rejects_trailing_newline() {
        assert!(interop_key("users\n", "get_user", &[]).is_err());
    }

    #[test]
    fn segment_rejects_empty_and_too_long() {
        assert!(interop_key("", "op", &[]).is_err());
        assert!(interop_key(&"a".repeat(65), "op", &[]).is_err());
        assert!(interop_key(&"a".repeat(64), "op", &[]).is_ok());
    }

    #[test]
    fn segment_rejects_leading_punctuation() {
        assert!(interop_key(".users", "op", &[]).is_err());
        assert!(interop_key("-users", "op", &[]).is_err());
        assert!(interop_key("users.v2_x-y", "op", &[]).is_ok());
    }

    #[test]
    fn systemtime_pre_epoch_floors_toward_negative_infinity() {
        use std::time::{Duration, SystemTime, UNIX_EPOCH};
        fn micros(t: SystemTime) -> Option<i64> {
            match InteropValue::try_from(t) {
                Ok(InteropValue::DateTime { unix_micros }) => Some(unix_micros),
                _ => None,
            }
        }
        // 1.5 us before the epoch: floor(-1.5us) = -2us, not -1us.
        assert_eq!(micros(UNIX_EPOCH - Duration::from_nanos(1500)), Some(-2));
        // Exactly on a microsecond boundary: no rounding.
        assert_eq!(micros(UNIX_EPOCH - Duration::from_micros(3)), Some(-3));
    }

    #[test]
    fn int_range_enforced_at_encode_time() {
        // 2^64 and -2^63-1 are constructible (i128) but must fail to encode.
        assert!(canonical_args(&[InteropValue::Int(INT_MAX + 1)]).is_err());
        assert!(canonical_args(&[InteropValue::Int(INT_MIN - 1)]).is_err());
        assert!(canonical_args(&[InteropValue::Int(INT_MAX)]).is_ok());
        assert!(canonical_args(&[InteropValue::Int(INT_MIN)]).is_ok());
    }

    #[test]
    fn value_profile_rejects_datetime() {
        // Temporal VALUES must use the sentinel-map convention; the
        // Unix-timestamp encoding is argument-hashing only.
        let dt = InteropValue::datetime_from_unix_micros(1_704_067_200_000_000);
        let err = serialize_value(&dt).unwrap_err();
        assert!(err.to_string().contains("__datetime__"), "got: {err}");
        // ...while the args profile accepts it (datetime_whole_second vector).
        assert!(canonical_args(&[dt]).is_ok());
    }

    #[test]
    fn value_profile_preserves_floats() {
        // 2.0 stays float64 in the value profile...
        let bytes = serialize_value(&InteropValue::from(2.0f64)).unwrap();
        assert_eq!(bytes, hex::decode("cb4000000000000000").unwrap());
        // ...but collapses to int 2 in the args profile.
        let bytes = canonical_args(&[InteropValue::from(2.0f64)]).unwrap();
        assert_eq!(bytes, hex::decode("9102").unwrap());
    }
}
