//! Legacy cache-key derivation for `#[cachekit]` macro expansion.
//!
//! NOT public API — reachable only through the `#[doc(hidden)]` `__private`
//! module because proc-macro output expands in the caller's crate.
//!
//! The `{namespace}:{blake2b256-hex}` shape here is the *obsolete* protocol
//! RFC §3.1.5 format. It matches **no current protocol key format** (neither
//! auto-mode `spec/cache-key-format.md` nor `spec/interop-mode.md`, both in
//! cachekit-io/protocol), so keys it mints are SDK-internal only and can
//! never match another SDK. Cross-SDK keys are an interop/v1 concern
//! (`spec/interop-mode.md`; Rust keygen tracked in cachekit-rs#33).
//!
//! Known limitation (kept for byte-stability): the hash input uses the
//! *unqualified* function ident, so two same-named `#[cachekit]` functions in
//! different modules sharing a namespace and arg encoding collide.
//!
//! Byte-stability contract: changing this algorithm changes every existing
//! `#[cachekit]` user's keys — a full cache invalidation. Pinned vectors in
//! `tests/key_tests.rs` (hash layer) and `tests/macro_tests.rs` (full macro
//! pipeline incl. rmp-serde arg encoding) guard against accidental drift.

use blake2::{digest::consts::U32, Blake2b, Digest};

use crate::error::CachekitError;

type Blake2b256 = Blake2b<U32>;

/// Derive the `#[cachekit]` macro's cache key: Blake2b-256 over the
/// MessagePack tuple `(function_name, serialized_args)`, optionally prefixed
/// with `{namespace}:`. Legacy format — see module docs.
#[doc(hidden)]
pub fn generate_cache_key(
    namespace: &str,
    function_name: &str,
    serialized_args: &[u8],
) -> Result<String, CachekitError> {
    let key_material = rmp_serde::to_vec(&(function_name, serialized_args))
        .map_err(|e| CachekitError::Serialization(format!("cache key material: {e}")))?;

    let mut hasher = Blake2b256::new();
    hasher.update(&key_material);
    let hash = hasher.finalize();
    let hex_hash = hex::encode(hash);

    Ok(if namespace.is_empty() {
        hex_hash
    } else {
        format!("{namespace}:{hex_hash}")
    })
}
