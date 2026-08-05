//! interop/v1 protocol vector cross-check — cachekit-rs's first
//! byte-verification of the shared cross-SDK vectors.
//!
//! Vectors: `tests/vectors/interop-mode.json`, vendored verbatim from
//! `cachekit-io/protocol` `test-vectors/interop-mode.json` at commit
//! `ef3e6d4d` (sha256 `a1f24b61e4957e9500a01ce7ed9fbb3ec601847514b481bf54813d9e470226df`).
//! Do not edit the JSON here; regenerate upstream and re-vendor.
//!
//! Vector inputs use the tagged-JSON convention documented in the file header
//! (`{"$set": ...}`, `{"$float": "..."}`, `{"$int": "..."}`,
//! `{"$datetime": "..."}`, `{"$uuid": "..."}`, `{"$bytes": "<hex>"}`) because
//! JSON alone cannot express sets, bytes, float-vs-int, or full 64-bit ints.
//!
//! Group counts are asserted exactly so a silently skipped vector fails loudly.
//!
//! Run with:
//!   cargo test --test interop_vector_tests --features encryption

use std::collections::BTreeMap;

use cachekit::interop::{canonical_args, interop_key, serialize_value, InteropValue};
use serde_json::Value as Json;

const VECTORS_JSON: &str = include_str!("vectors/interop-mode.json");

fn vectors() -> Json {
    serde_json::from_str(VECTORS_JSON).expect("vendored vector file must be valid JSON")
}

// ── Tagged-JSON input parsing ────────────────────────────────────────────────

/// Convert a tagged-JSON vector input into an `InteropValue`.
///
/// Returns `Err` for inputs the harness itself cannot represent (naive
/// datetime — unrepresentable in the SDK's micros-based API — and integers
/// beyond i128, which cannot occur here). Range/model violations that ARE
/// representable (2^64, NaN, ±Inf) parse fine and are rejected by the SDK at
/// encode time, which is what the error vectors verify.
fn parse_input(v: &Json) -> Result<InteropValue, String> {
    match v {
        Json::Null => Ok(InteropValue::Null),
        Json::Bool(b) => Ok(InteropValue::from(*b)),
        Json::Number(n) => {
            // Per the vector file header, plain JSON numbers are always integers.
            n.as_i64()
                .map(InteropValue::from)
                .or_else(|| n.as_u64().map(InteropValue::from))
                .ok_or_else(|| format!("unexpected non-integer JSON number: {n}"))
        }
        Json::String(s) => Ok(InteropValue::from(s.as_str())),
        Json::Array(items) => Ok(InteropValue::Array(
            items.iter().map(parse_input).collect::<Result<_, _>>()?,
        )),
        Json::Object(obj) => {
            if obj.len() == 1 {
                let (tag, val) = obj.iter().next().ok_or("empty object")?;
                if let Some(parsed) = parse_tagged(tag, val)? {
                    return Ok(parsed);
                }
            }
            // Plain JSON object -> interop map (string keys by construction).
            let map: BTreeMap<String, InteropValue> = obj
                .iter()
                .map(|(k, v)| Ok((k.clone(), parse_input(v)?)))
                .collect::<Result<_, String>>()?;
            Ok(InteropValue::Map(map))
        }
    }
}

/// Parse a `$`-tagged single-key object. Returns `Ok(None)` if the key is not
/// a recognized tag (then it is an ordinary one-entry map).
fn parse_tagged(tag: &str, val: &Json) -> Result<Option<InteropValue>, String> {
    let as_str = || {
        val.as_str()
            .ok_or_else(|| format!("{tag} payload must be a string"))
    };
    match tag {
        "$int" => {
            // i128 spans well beyond [-2^63, 2^64-1]; the SDK enforces the
            // interop range at encode time (reject_int_overflow/underflow).
            let i: i128 = as_str()?
                .parse()
                .map_err(|e| format!("bad $int literal: {e}"))?;
            Ok(Some(InteropValue::from(i)))
        }
        "$float" => {
            // "nan" parses to NaN and "1e999" overflows to +inf in Rust, as
            // the vector file header requires; the SDK rejects both at encode.
            let f: f64 = as_str()?
                .parse()
                .map_err(|e| format!("bad $float literal: {e}"))?;
            Ok(Some(InteropValue::from(f)))
        }
        "$bytes" => {
            let bytes = hex::decode(as_str()?).map_err(|e| format!("bad $bytes hex: {e}"))?;
            Ok(Some(InteropValue::bytes(bytes)))
        }
        "$set" => {
            let items = val
                .as_array()
                .ok_or("$set payload must be an array")?
                .iter()
                .map(parse_input)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Some(InteropValue::Set(items)))
        }
        "$uuid" => {
            let u = uuid::Uuid::parse_str(as_str()?).map_err(|e| format!("bad $uuid: {e}"))?;
            Ok(Some(InteropValue::from(u)))
        }
        "$datetime" => Ok(Some(InteropValue::datetime_from_unix_micros(
            iso8601_to_unix_micros(as_str()?)?,
        ))),
        _ => Ok(None),
    }
}

// ── ISO 8601 parsing (harness-only) ──────────────────────────────────────────
//
// The SDK API takes unix micros (or SystemTime) — a naive datetime is
// unrepresentable there by design. The vectors carry ISO 8601 strings, so the
// harness parses them; a missing UTC offset is rejected HERE, which is where
// the reject_naive_datetime error vector lands for Rust.

/// Parse `YYYY-MM-DDTHH:MM:SS[.ffffff](Z|+HH:MM|-HH:MM)` to unix microseconds.
/// Rejects naive (offset-less) datetimes.
fn iso8601_to_unix_micros(s: &str) -> Result<i64, String> {
    let bad = || format!("unparseable ISO 8601 datetime: {s:?}");

    let (date, rest) = s.split_at_checked(10).ok_or_else(bad)?;
    let mut date_parts = date.split('-');
    let year: i64 = date_parts
        .next()
        .ok_or_else(bad)?
        .parse()
        .map_err(|_| bad())?;
    let month: i64 = date_parts
        .next()
        .ok_or_else(bad)?
        .parse()
        .map_err(|_| bad())?;
    let day: i64 = date_parts
        .next()
        .ok_or_else(bad)?
        .parse()
        .map_err(|_| bad())?;

    let rest = rest.strip_prefix('T').ok_or_else(bad)?;
    let (time, rest) = rest.split_at_checked(8).ok_or_else(bad)?;
    let mut time_parts = time.split(':');
    let hour: i64 = time_parts
        .next()
        .ok_or_else(bad)?
        .parse()
        .map_err(|_| bad())?;
    let minute: i64 = time_parts
        .next()
        .ok_or_else(bad)?
        .parse()
        .map_err(|_| bad())?;
    let second: i64 = time_parts
        .next()
        .ok_or_else(bad)?
        .parse()
        .map_err(|_| bad())?;

    // Optional fractional seconds, up to microsecond precision.
    let (frac_micros, rest) = if let Some(frac) = rest.strip_prefix('.') {
        let digits: String = frac.chars().take_while(char::is_ascii_digit).collect();
        if digits.is_empty() || digits.len() > 6 {
            return Err(bad());
        }
        let micros: i64 = format!("{digits:0<6}").parse().map_err(|_| bad())?;
        (micros, &frac[digits.len()..])
    } else {
        (0, rest)
    };

    // Offset — REQUIRED: interop rejects naive datetimes.
    let offset_seconds = match rest {
        "" => return Err(format!("naive datetime (no UTC offset): {s:?}")),
        "Z" | "z" => 0,
        _ => {
            let sign = match rest.as_bytes().first() {
                Some(b'+') => 1,
                Some(b'-') => -1,
                _ => return Err(bad()),
            };
            let (hh, mm) = rest[1..].split_once(':').ok_or_else(bad)?;
            let hh: i64 = hh.parse().map_err(|_| bad())?;
            let mm: i64 = mm.parse().map_err(|_| bad())?;
            sign * (hh * 3600 + mm * 60)
        }
    };

    // Days since epoch via the standard civil-calendar algorithm
    // (Howard Hinnant's days_from_civil), valid for the proleptic Gregorian
    // calendar over the full vector range.
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;

    let utc_seconds = days * 86_400 + hour * 3600 + minute * 60 + second - offset_seconds;
    Ok(utc_seconds * 1_000_000 + frac_micros)
}

// ── Key vectors ──────────────────────────────────────────────────────────────

#[test]
fn key_vectors_all_pass() {
    let doc = vectors();
    let key_vectors = doc["key_vectors"].as_array().expect("key_vectors array");
    assert_eq!(key_vectors.len(), 33, "expected 33 key vectors");

    for vector in key_vectors {
        let name = vector["name"].as_str().expect("vector name");
        let namespace = vector["namespace"].as_str().expect("namespace");
        let operation = vector["operation"].as_str().expect("operation");
        let args: Vec<InteropValue> = vector["args"]
            .as_array()
            .expect("args array")
            .iter()
            .map(|a| parse_input(a).unwrap_or_else(|e| panic!("[{name}] input parse: {e}")))
            .collect();

        // 1. Canonical argument bytes (exact hex).
        let encoded =
            canonical_args(&args).unwrap_or_else(|e| panic!("[{name}] canonical encode: {e}"));
        assert_eq!(
            hex::encode(&encoded),
            vector["canonical_args_hex"].as_str().expect("hex"),
            "[{name}] canonical argument bytes mismatch"
        );

        // 2. Full key (namespace validation + Blake2b-256 + formatting).
        let key = interop_key(namespace, operation, &args)
            .unwrap_or_else(|e| panic!("[{name}] interop_key: {e}"));
        assert_eq!(
            key,
            vector["expected_key"].as_str().expect("expected_key"),
            "[{name}] key mismatch"
        );

        // 3. args_hash is the hash segment of the key — cross-check the field.
        assert_eq!(
            key.rsplit(':').next().expect("key has segments"),
            vector["args_hash"].as_str().expect("args_hash"),
            "[{name}] args_hash mismatch"
        );
    }
}

// ── Value vectors ────────────────────────────────────────────────────────────

#[test]
fn value_vectors_all_pass() {
    let doc = vectors();
    let value_vectors = doc["value_vectors"].as_array().expect("value_vectors");
    assert_eq!(value_vectors.len(), 4, "expected 4 value vectors");

    for vector in value_vectors {
        let name = vector["name"].as_str().expect("vector name");
        let value =
            parse_input(&vector["value"]).unwrap_or_else(|e| panic!("[{name}] input parse: {e}"));

        let encoded =
            serialize_value(&value).unwrap_or_else(|e| panic!("[{name}] value encode: {e}"));
        assert_eq!(
            hex::encode(&encoded),
            vector["canonical_msgpack_hex"].as_str().expect("hex"),
            "[{name}] canonical value bytes mismatch"
        );

        // Interop readers must accept the exact document via the strict
        // (exactly-one-document) deserializer.
        cachekit::interop::deserialize::<serde_json::Value>(&encoded).unwrap_or_else(|e| {
            panic!("[{name}] strict deserialize failed on canonical bytes: {e}")
        });
    }
}

// ── Error vectors ────────────────────────────────────────────────────────────

#[test]
fn error_vectors_all_reject() {
    let doc = vectors();
    let error_vectors = doc["error_vectors"].as_array().expect("error_vectors");
    assert_eq!(error_vectors.len(), 9, "expected 9 error vectors");

    for vector in error_vectors {
        let name = vector["name"].as_str().expect("vector name");
        let namespace = vector["namespace"].as_str().unwrap_or("t");
        let operation = vector["operation"].as_str().unwrap_or("op");

        // Rejection may surface at input parse (naive datetime — the SDK API
        // cannot even represent one) or at interop_key (segment grammar,
        // encode-time range/finiteness checks). Either satisfies "MUST reject".
        let args: Result<Vec<InteropValue>, String> = vector["args"]
            .as_array()
            .expect("args array")
            .iter()
            .map(parse_input)
            .collect();

        match args {
            Err(reason) => {
                assert!(
                    name == "reject_naive_datetime",
                    "[{name}] unexpected harness-level rejection: {reason}"
                );
            }
            Ok(args) => {
                let result = interop_key(namespace, operation, &args);
                assert!(
                    result.is_err(),
                    "[{name}] MUST be rejected, but produced key: {result:?}"
                );
            }
        }
    }
}

// ── AAD + encryption vectors ─────────────────────────────────────────────────

#[cfg(feature = "encryption")]
#[test]
fn aad_vectors_all_pass() {
    use cachekit::EncryptionLayer;

    let doc = vectors();
    let aad_vectors = doc["aad_vectors"].as_array().expect("aad_vectors");
    assert_eq!(aad_vectors.len(), 1, "expected 1 AAD vector");

    for vector in aad_vectors {
        let name = vector["name"].as_str().expect("vector name");
        let tenant_id = vector["tenant_id"].as_str().expect("tenant_id");
        let cache_key = vector["cache_key"].as_str().expect("cache_key");
        assert!(!vector["compressed"].as_bool().expect("compressed flag"));

        // AAD does not depend on the key material; any valid master key works.
        let layer = EncryptionLayer::new(&[0u8; 32], tenant_id).expect("layer");
        let aad = layer.build_aad(cache_key, false);
        assert_eq!(
            hex::encode(&aad),
            vector["aad_hex"].as_str().expect("aad_hex"),
            "[{name}] AAD bytes mismatch"
        );
    }
}

#[cfg(feature = "encryption")]
#[test]
fn encryption_vectors_decrypt() {
    use cachekit::EncryptionLayer;

    let doc = vectors();
    let enc_vectors = doc["encryption_vectors"]
        .as_array()
        .expect("encryption_vectors");
    assert_eq!(enc_vectors.len(), 1, "expected 1 encryption vector");

    for vector in enc_vectors {
        let name = vector["name"].as_str().expect("vector name");
        let master_key = hex::decode(vector["master_key_hex"].as_str().expect("master key"))
            .expect("master key hex");
        let tenant_id = vector["tenant_id"].as_str().expect("tenant_id");
        let cache_key = vector["cache_key"].as_str().expect("cache_key");
        let ciphertext = hex::decode(vector["ciphertext_hex"].as_str().expect("ciphertext"))
            .expect("ciphertext hex");
        let expected_plaintext =
            hex::decode(vector["plaintext_hex"].as_str().expect("plaintext")).expect("hex");

        // Full cross-SDK decrypt: HKDF-SHA256 tenant derivation + AES-256-GCM
        // tag verification with the interop AAD. A wrong derived key, wrong
        // AAD, or wrong layout fails the auth tag — success here demonstrates
        // end-to-end cross-SDK decryption of an interop entry.
        let layer = EncryptionLayer::new(&master_key, tenant_id).expect("layer");
        let plaintext = layer
            .decrypt(&ciphertext, cache_key)
            .unwrap_or_else(|e| panic!("[{name}] decrypt failed: {e}"));
        assert_eq!(plaintext, expected_plaintext, "[{name}] plaintext mismatch");

        // The plaintext is the plain-MessagePack value (no ByteStorage
        // envelope): the strict interop reader must decode it directly.
        let decoded: BTreeMap<String, serde_json::Value> =
            cachekit::interop::deserialize(&plaintext)
                .unwrap_or_else(|e| panic!("[{name}] plaintext is not one msgpack doc: {e}"));
        assert_eq!(decoded["name"], serde_json::json!("alice"));
        assert_eq!(decoded["age"], serde_json::json!(30));
    }
}

// ── Cross-vector consistency checks the JSON structure implies ──────────────

#[test]
fn collapse_pair_produces_identical_keys() {
    // float 2.0 and int 2 must hash identically (number canonicalization).
    let float_key = interop_key("t", "op", &[InteropValue::from(2.0f64)]).unwrap();
    let int_key = interop_key("t", "op", &[InteropValue::from(2i64)]).unwrap();
    assert_eq!(float_key, int_key);
}

#[test]
fn non_utc_offset_matches_utc_instant() {
    // 2024-01-01T18:00:45.123456+05:30 is the same instant as
    // 2024-01-01T12:30:45.123456+00:00 — identical micros, identical key.
    let a = iso8601_to_unix_micros("2024-01-01T18:00:45.123456+05:30").unwrap();
    let b = iso8601_to_unix_micros("2024-01-01T12:30:45.123456+00:00").unwrap();
    assert_eq!(a, b);
}
