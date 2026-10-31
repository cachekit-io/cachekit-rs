use bytes::Bytes;
use cachekit_core::msgpack_document_len;
use rmp_serde::decode::ReadRefReader;
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
/// Enforced by cachekit-core's `msgpack_document_len` walk. It
/// runs before every decode in [`deserialize`] and
/// [`crate::interop::deserialize`]. A client without encryption also runs it
/// once when a backend read or its own write admits bytes, and a decode of
/// bytes that passed then skips it (see `Payload`). Depth
/// counts every array and map header on the deepest path, empty ones included;
/// scalars, str, bin and ext are leaves.
/// Exactly `MAX_DECODE_DEPTH` levels are accepted, and one more is rejected
/// with a `decode bound:` error before `rmp-serde` recurses.
pub const MAX_DECODE_DEPTH: usize = 100;

/// A decoder reading a byte slice in place.
pub(crate) type SliceDeserializer<'a> = rmp_serde::Deserializer<ReadRefReader<'a, [u8]>>;

/// Build a depth- and allocation-bounded `rmp_serde::Deserializer` over
/// `bytes`, which must hold exactly one MessagePack document.
///
/// Every decode of backend-supplied bytes (auto-mode [`deserialize`] and
/// [`crate::interop::deserialize`]) MUST go through here, or through
/// `Payload::deserializer`, which skips the walk only for bytes whose mark
/// `Payload::walk` set after running these same checks. It runs cachekit-core's header-only
/// `msgpack_document_len` walk first. The walk proves that a complete
/// document lies within `bytes`, that it nests no deeper than
/// [`MAX_DECODE_DEPTH`], and that no header declares more elements or bytes
/// than the remaining input can back. Its rejection becomes a
/// `CachekitError::Serialization` starting `decode bound:`.
///
/// Why depth is checked by the walk and not only by `rmp-serde`: a guard that
/// admits a document deeper than the bound leaves the rejection to the decoder,
/// mid-recursion, after it has materialised the outer levels.
///
/// Why the walk is needed even though `rmp-serde` reads str/bin lazily: serde's
/// sequence visitors (`Vec<T>`, and the `Content` buffer that
/// `#[serde(untagged)]` targets decode through) reserve
/// `min(declared_len × size_of::<Element>(), 1 MiB)` bytes per collection from
/// `size_hint`, so without the walk a 500-byte payload of nested
/// `array32(0xFFFFFFFF)` headers requests `MAX_DECODE_DEPTH` MiB before the
/// first EOF error — an OOM kill on a Cloudflare Workers isolate.
///
/// It then rejects any byte after the document: no writer emits one, a lenient
/// read would decode a foreign container by its first byte (a cachekit-py CK
/// frame begins with `0x43`, the complete document 67), and after a decrypt the
/// spec requires a parse mismatch to fail. The walk leaves trailing bytes to
/// its caller and returns where the document ends, so the same walk settles
/// this: any length short of `bytes.len()` means bytes follow the document.
/// Finally it sets `rmp-serde`'s own depth limit as a backstop just above
/// [`MAX_DECODE_DEPTH`]: its default (1024) is deep enough to overflow a
/// debug-build thread stack.
///
/// The decoder reads the input in place (`from_read_ref`), so str and bin
/// payloads are handed to serde as borrowed slices. The default `ReadReader`
/// copies each one through an internal buffer first, one extra allocation and
/// copy per field on every cache read.
///
/// A successful decode consumes the whole document: rmp-serde rejects an array
/// or map its target leaves unread. Ext is the exception: a target that reads
/// only an ext's type byte leaves the body unread and is accepted. No value
/// this SDK writes contains ext.
pub(crate) fn bounded_deserializer(bytes: &[u8]) -> Result<SliceDeserializer<'_>, CachekitError> {
    check_bounds(bytes)?;
    Ok(depth_limited(bytes))
}

/// The checks [`bounded_deserializer`] runs before it decodes: cachekit-core's
/// header walk, then the single-document rule.
fn check_bounds(bytes: &[u8]) -> Result<(), CachekitError> {
    let len = msgpack_document_len(bytes, MAX_DECODE_DEPTH)
        .map_err(|e| CachekitError::Serialization(format!("decode bound: {e}")))?;
    // The error names no byte: on the secure path these bytes are decrypted
    // plaintext (CWE-532). Their count is not.
    if len < bytes.len() {
        return Err(CachekitError::Serialization(format!(
            "payload has {} trailing byte(s) after the MessagePack document — \
             readers must consume exactly one document",
            bytes.len() - len
        )));
    }
    Ok(())
}

/// The decoder [`bounded_deserializer`] returns once `bytes` passed
/// [`check_bounds`].
fn depth_limited(bytes: &[u8]) -> SliceDeserializer<'_> {
    let mut de = rmp_serde::Deserializer::from_read_ref(bytes);
    // rmp-serde decrements its counter on entry and errors when it reaches 0, so
    // `set_max_depth(n)` admits n - 1 levels, and it counts an ext value as a
    // level where the walk (and the spec) count it as a leaf. Ext is always a
    // leaf, so +2 admits every document the walk admits, including an ext at the
    // bound, and the walk's error is the one callers see for anything deeper
    // (both pinned by tests/decode_bounds_tests.rs). It is a backstop only:
    // rmp-serde does not count an enum's variant map as a level, so a recursive
    // enum nests past it, and only the walk bounds that.
    de.set_max_depth(MAX_DECODE_DEPTH + 2);
    de
}

/// Stored value bytes, and whether they already passed [`check_bounds`].
///
/// The walk guards a decode against bytes the backend controls, and a value
/// this process wrote needs it too: rmp-serde's own depth limit does not count
/// enum nesting, so a deep recursive enum would overflow the stack without it.
/// Bytes need the walk once, not on every read. [`Self::walk`] is the only way
/// to set the mark, and sets it only when the bytes pass, so a marked payload
/// decodes without walking again. L1 keeps the mark for the entry's lifetime.
#[derive(Clone, Debug)]
pub(crate) struct Payload {
    // Private, with `walked`: nothing outside `walk` can pair a mark with
    // other bytes.
    bytes: Bytes,
    walked: bool,
}

impl Payload {
    /// Bytes every decode walks: ciphertext, whose decrypted plaintext is
    /// walked instead, and bytes nothing has checked.
    #[cfg_attr(not(any(feature = "l1", feature = "encryption")), allow(dead_code))]
    pub(crate) fn unwalked(bytes: Bytes) -> Self {
        Self {
            bytes,
            walked: false,
        }
    }

    /// Walk `bytes` now and mark them only if they pass. A failure is not
    /// reported here; a decode walks the unmarked bytes again and reports it.
    pub(crate) fn walk(bytes: Bytes) -> Self {
        let walked = check_bounds(&bytes).is_ok();
        Self { bytes, walked }
    }

    /// The stored bytes, by reference: a clone is a refcount bump.
    pub(crate) fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    /// Whether the bytes passed the walk.
    #[cfg(all(test, feature = "l1", not(feature = "unsync")))]
    pub(crate) fn is_walked(&self) -> bool {
        self.walked
    }

    /// [`bounded_deserializer`] over the bytes, without the walk when they
    /// already passed it.
    pub(crate) fn deserializer(&self) -> Result<SliceDeserializer<'_>, CachekitError> {
        if self.walked {
            Ok(depth_limited(&self.bytes))
        } else {
            bounded_deserializer(&self.bytes)
        }
    }
}

/// [`deserialize`] for a payload a read returned: a marked payload skips the
/// walk (see [`Payload`]).
pub(crate) fn deserialize_payload<T: DeserializeOwned>(
    payload: &Payload,
) -> Result<T, CachekitError> {
    decode(payload.deserializer()?)
}

/// Decode one value from `de`, an auto-mode reader.
fn decode<T: DeserializeOwned>(mut de: SliceDeserializer<'_>) -> Result<T, CachekitError> {
    T::deserialize(&mut de).map_err(|e| CachekitError::Serialization(e.to_string()))
}

/// Serialize `value` to MessagePack bytes using named fields (map format).
pub fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>, CachekitError> {
    rmp_serde::to_vec_named(value).map_err(|e| CachekitError::Serialization(e.to_string()))
}

/// Deserialize `bytes` from MessagePack into `T` under the decode bounds:
/// cachekit-core's structural walk (depth ≤ [`MAX_DECODE_DEPTH`], no
/// overclaim) runs before anything is decoded, and `bytes` must hold exactly
/// one document — trailing bytes are rejected, as in interop mode
/// ([`crate::interop::deserialize`]).
pub fn deserialize<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, CachekitError> {
    decode(bounded_deserializer(bytes)?)
}
