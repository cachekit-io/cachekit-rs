use rmp_serde::decode::ReadReader;
use serde::{de::DeserializeOwned, Serialize};

use crate::error::CachekitError;

/// Maximum MessagePack nesting depth accepted from an untrusted payload.
///
/// Every cache read decodes bytes the backend controls, and `rmp-serde`
/// recurses once per nesting level, so the failure mode of unbounded depth is
/// a **stack overflow** — an uncatchable process abort. `rmp-serde`'s own
/// default (1024) is not a safe bound: measured (rmp-serde 1.3.1, 2026-09) on
/// a 2 MiB thread stack — the tokio worker default — a debug build overflows
/// between 512 and 768 nested arrays, i.e. a ~700-byte forged entry; a release
/// build fits 1024 with little margin, and frame size grows with the target
/// type. 100 matches cachekit-ts (`DEFAULT_MAX_DEPTH`), leaves a wide margin on
/// every profile and target (wasm32 included), and sits inside the protocol's
/// required `32..=1024` window (`spec/interop-mode.md` → Decode bounds).
///
/// Depth is only half the bound: see `check_structure` for the allocation
/// half. `tests/decode_bounds_tests.rs` runs the shared `decode-bounds.json`
/// vectors against both decode entry points so a dependency bump cannot move
/// either silently.
pub const MAX_DECODE_DEPTH: usize = 100;

/// Header-only structural walk over one MessagePack document (LAB-2503).
///
/// Proves that a complete document lies within `bytes` and that no header
/// declares more elements or bytes than the remaining input can back — every
/// element costs at least one input byte, so `pending ≤ remaining` at every
/// step means Σ declared ≤ input. Allocates nothing; fails closed on the
/// reserved marker, truncation, and length overflow.
///
/// Why it is needed even though `rmp-serde` reads str/bin lazily: serde's
/// sequence visitors (`Vec<T>`, and the `Content` buffer that
/// `#[serde(untagged)]` targets decode through) reserve
/// `min(declared_len × size_of::<Element>(), 1 MiB)` bytes per collection from
/// `size_hint`, so without this walk a 500-byte payload of nested
/// `array32(0xFFFFFFFF)` headers requests `MAX_DECODE_DEPTH` MiB before the
/// first EOF error — an OOM kill on a Cloudflare Workers isolate. Trailing
/// bytes are left to the caller (auto mode ignores them; interop rejects them).
pub(crate) fn check_structure(bytes: &[u8]) -> Result<(), CachekitError> {
    fn reject(what: &str) -> CachekitError {
        CachekitError::Serialization(format!("decode bound: {what}"))
    }
    fn be(bytes: &[u8], pos: usize, width: usize) -> Result<u64, CachekitError> {
        let end = pos.checked_add(width).filter(|e| *e <= bytes.len());
        let field = &bytes[pos..end.ok_or_else(|| reject("input ends inside a length prefix"))?];
        Ok(field.iter().fold(0u64, |acc, b| (acc << 8) | u64::from(*b)))
    }

    let mut pos = 0usize;
    let mut pending: u64 = 1; // elements still owed by open headers (the root counts as one)
    while pending > 0 {
        let marker = *bytes
            .get(pos)
            .ok_or_else(|| reject("input ends before the document is complete"))?;
        pos += 1;
        pending -= 1;
        // (prefix bytes, payload bytes after the prefix, child elements)
        let (prefix, payload, children): (usize, u64, u64) = match marker {
            0x00..=0x7f | 0xc0 | 0xc2 | 0xc3 | 0xe0..=0xff => (0, 0, 0),
            0x80..=0x8f => (0, 0, 2 * u64::from(marker & 0x0f)),
            0x90..=0x9f => (0, 0, u64::from(marker & 0x0f)),
            0xa0..=0xbf => (0, u64::from(marker & 0x1f), 0),
            0xc1 => return Err(reject("reserved marker 0xc1")),
            0xc4 | 0xd9 => (1, be(bytes, pos, 1)?, 0),
            0xc5 | 0xda => (2, be(bytes, pos, 2)?, 0),
            0xc6 | 0xdb => (4, be(bytes, pos, 4)?, 0),
            0xc7 => (1, be(bytes, pos, 1)? + 1, 0), // ext: length prefix, then type byte + data
            0xc8 => (2, be(bytes, pos, 2)? + 1, 0),
            0xc9 => (4, be(bytes, pos, 4)? + 1, 0),
            0xca..=0xd3 => (0, 1u64 << (marker & 0x03), 0), // f32/f64/u8..u64/i8..i64: 4,8,1,2,4,8,1,2,4,8
            0xd4..=0xd8 => (0, 1 + (1u64 << (marker - 0xd4)), 0), // fixext: type byte + 1/2/4/8/16
            0xdc => (2, 0, be(bytes, pos, 2)?),
            0xdd => (4, 0, be(bytes, pos, 4)?),
            0xde => (2, 0, 2 * be(bytes, pos, 2)?),
            0xdf => (4, 0, 2 * be(bytes, pos, 4)?),
        };
        pos += prefix;
        let remaining = (bytes.len() - pos) as u64;
        if payload > remaining {
            return Err(reject("header declares more bytes than the input holds"));
        }
        pos += usize::try_from(payload)
            .map_err(|_| reject("header declares more bytes than the input holds"))?;
        pending += children;
        if pending > remaining - payload {
            return Err(reject(
                "header declares more elements than the input can back",
            ));
        }
    }
    Ok(())
}

/// Build a depth- and allocation-bounded `rmp_serde::Deserializer` over `bytes`.
///
/// Every decode of backend-supplied bytes (auto-mode [`deserialize`] and
/// [`crate::interop::deserialize`]) MUST go through here so the bounds cannot
/// drift between paths. Runs `check_structure` first, then applies
/// [`MAX_DECODE_DEPTH`].
pub(crate) fn bounded_deserializer(
    bytes: &[u8],
) -> Result<rmp_serde::Deserializer<ReadReader<&[u8]>>, CachekitError> {
    check_structure(bytes)?;
    let mut de = rmp_serde::Deserializer::new(bytes);
    // rmp-serde decrements its counter on entry and errors when it reaches 0, so
    // `set_max_depth(n)` admits n - 1 nested collections. +1 makes the constant mean
    // what it says: exactly MAX_DECODE_DEPTH levels decode, MAX_DECODE_DEPTH + 1 is
    // rejected (pinned by tests/decode_bounds_tests.rs).
    de.set_max_depth(MAX_DECODE_DEPTH + 1);
    Ok(de)
}

/// Serialize `value` to MessagePack bytes using named fields (map format).
pub fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>, CachekitError> {
    rmp_serde::to_vec_named(value).map_err(|e| CachekitError::Serialization(e.to_string()))
}

/// Deserialize `bytes` from MessagePack into `T` under the decode bounds
/// ([`MAX_DECODE_DEPTH`], `check_structure`). Trailing bytes are ignored
/// (auto mode is SDK-internal; interop mode's strict single-document read is
/// [`crate::interop::deserialize`]).
pub fn deserialize<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, CachekitError> {
    T::deserialize(&mut bounded_deserializer(bytes)?)
        .map_err(|e| CachekitError::Serialization(e.to_string()))
}
