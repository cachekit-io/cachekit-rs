use std::io::Read;

use rmp_serde::decode::ReadReader;
use serde::{de::DeserializeOwned, Serialize};

use crate::error::CachekitError;

/// Maximum MessagePack nesting depth accepted from an untrusted payload.
///
/// `rmp-serde` sizes containers lazily (no header-driven pre-allocation), so
/// nesting depth — one recursion frame per level — is the only decode
/// amplification axis this crate has, and the failure mode is a **stack
/// overflow**, which aborts the process and cannot be caught. `rmp-serde`'s
/// own default (1024) is not a safe bound here: measured on a 2 MiB thread
/// stack (the tokio worker default), a debug build overflows between 512 and
/// 768 nested arrays — a ~700-byte forged entry — while a release build fits
/// 1024 with little margin; frame size also grows with the target type. 100
/// matches cachekit-ts (`DEFAULT_MAX_DEPTH`), leaves a wide margin on every
/// profile and target (wasm32 included), and sits inside the protocol's
/// required `32..=1024` window (`spec/interop-mode.md` → Decode bounds).
/// `tests/decode_bounds_tests.rs` runs the shared `decode-bounds.json` vectors
/// against both decode entry points so a dependency bump cannot move this.
pub const MAX_DECODE_DEPTH: usize = 100;

/// Build a `rmp_serde::Deserializer` with the cachekit-owned depth bound applied.
///
/// Every decode of backend-supplied bytes (auto-mode [`deserialize`] and
/// [`crate::interop::deserialize`]) MUST go through here so the bound cannot
/// drift between paths.
pub(crate) fn bounded_deserializer<R: Read>(rd: R) -> rmp_serde::Deserializer<ReadReader<R>> {
    let mut de = rmp_serde::Deserializer::new(rd);
    // rmp-serde decrements its counter on entry and errors when it reaches 0, so
    // `set_max_depth(n)` admits n - 1 nested collections. +1 makes the constant mean
    // what it says: exactly MAX_DECODE_DEPTH levels decode, MAX_DECODE_DEPTH + 1 is
    // rejected (pinned by tests/decode_bounds_tests.rs).
    de.set_max_depth(MAX_DECODE_DEPTH + 1);
    de
}

/// Serialize `value` to MessagePack bytes using named fields (map format).
pub fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>, CachekitError> {
    rmp_serde::to_vec_named(value).map_err(|e| CachekitError::Serialization(e.to_string()))
}

/// Deserialize `bytes` from MessagePack into `T`, nesting bounded by
/// [`MAX_DECODE_DEPTH`]. Trailing bytes are ignored (auto mode is SDK-internal;
/// interop mode's strict single-document read is [`crate::interop::deserialize`]).
pub fn deserialize<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, CachekitError> {
    T::deserialize(&mut bounded_deserializer(bytes))
        .map_err(|e| CachekitError::Serialization(e.to_string()))
}
