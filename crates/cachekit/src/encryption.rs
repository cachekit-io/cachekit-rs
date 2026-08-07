//! Zero-knowledge encryption layer using AES-256-GCM with AAD v0x03 format.
//!
//! Wraps `cachekit_core::ZeroKnowledgeEncryptor` with tenant key derivation
//! and cache-key-bound Additional Authenticated Data (AAD). The AAD binding
//! prevents ciphertext substitution attacks within the same tenant (CVSS 8.5).
//!
//! # AAD v0x03 Format
//!
//! ```text
//! [version(0x03)][len(4)][tenant_id][len(4)][cache_key][len(4)][format][len(4)][compressed]
//! ```
//!
//! Each component is length-prefixed with a 4-byte big-endian u32 to prevent
//! collision attacks from boundary confusion.

use zeroize::Zeroizing;

use cachekit_core::{Keyring, ZeroKnowledgeEncryptor};

use crate::error::CachekitError;

/// AAD protocol version byte.
const AAD_VERSION: u8 = 0x03;

/// Zero-knowledge encryption layer with per-tenant key derivation.
///
/// Holds a derived encryption key (zeroized on drop), a [`Keyring`] of master
/// keys for multi-key decrypt during rotation, and the
/// `ZeroKnowledgeEncryptor` from cachekit-core for AES-256-GCM operations.
///
/// L1 stores **ciphertext**, not plaintext — the zero-knowledge property
/// is preserved across all cache layers.
///
/// # Key rotation
///
/// Writes always encrypt under the current master key. Reads decrypt via the
/// keyring: sequential attempts, current key first, then each decrypt-only
/// previous key in order, rebuilding the identical AAD per attempt
/// (cachekit-rs entries carry no per-entry key identity — sequential
/// attempts are the spec-assigned branch, `protocol/spec/encryption.md` →
/// "Key Rotation (Keyring)").
pub struct EncryptionLayer {
    encryptor: ZeroKnowledgeEncryptor,
    derived_key: Zeroizing<[u8; 32]>,
    keyring: Keyring,
    tenant_id: String,
}

impl EncryptionLayer {
    /// Create a new encryption layer with HKDF-derived tenant keys.
    ///
    /// Equivalent to [`Self::with_previous_keys`] with no previous keys.
    ///
    /// # Arguments
    /// * `master_key_bytes` — Raw master key (minimum 32 bytes for AES-256)
    /// * `tenant_id` — Tenant identifier for cryptographic isolation
    ///
    /// # Errors
    /// - Master key too short (< 32 bytes)
    /// - HKDF derivation failure
    /// - Encryptor initialization failure
    pub fn new(master_key_bytes: &[u8], tenant_id: &str) -> Result<Self, CachekitError> {
        Self::with_previous_keys(master_key_bytes, &[], tenant_id)
    }

    /// Create an encryption layer with decrypt-only previous master keys.
    ///
    /// `previous_keys` are retained during a key-rotation grace window, in
    /// attempt order. Writes always use `master_key_bytes`; reads attempt it
    /// first, then each previous key sequentially.
    ///
    /// # Errors
    /// - Any key too short (< 32 bytes)
    /// - More than 3 previous keys ([`CachekitError::Config`] — rejected,
    ///   never truncated)
    /// - The current master key also present in `previous_keys`
    ///   ([`CachekitError::Config`] — rotation is forward-only)
    /// - HKDF derivation failure
    /// - Encryptor initialization failure
    pub fn with_previous_keys(
        master_key_bytes: &[u8],
        previous_keys: &[&[u8]],
        tenant_id: &str,
    ) -> Result<Self, CachekitError> {
        for (i, key) in previous_keys.iter().enumerate() {
            if key.len() < 32 {
                return Err(CachekitError::Config(format!(
                    "previous master key {i} must be at least 32 bytes; got {}",
                    key.len()
                )));
            }
        }
        // Key-length and tenant checks are configuration errors, kept in the
        // same class as the previous-key and Keyring construction checks
        // below (LAB-683: config errors must not fold into crypto failures).
        if master_key_bytes.len() < 32 {
            return Err(CachekitError::Config(format!(
                "master key must be at least 32 bytes; got {}",
                master_key_bytes.len()
            )));
        }
        if tenant_id.is_empty() {
            return Err(CachekitError::Config(
                "tenant_id must not be empty".to_owned(),
            ));
        }
        if tenant_id.len() > 255 {
            return Err(CachekitError::Config(format!(
                "tenant_id must be at most 255 bytes; got {}",
                tenant_id.len()
            )));
        }

        let tenant_keys = cachekit_core::encryption::key_derivation::derive_tenant_keys(
            master_key_bytes,
            tenant_id,
        )
        .map_err(|e| CachekitError::Encryption(format!("key derivation failed: {e}")))?;

        let encryptor = ZeroKnowledgeEncryptor::new()
            .map_err(|e| CachekitError::Encryption(format!("encryptor init failed: {e}")))?;

        // The shared multi-key decrypt helper (cachekit-core, LAB-683) owns
        // the keyring invariants: cap of 3, forward-only self-collision,
        // sequential current-first attempts. Its construction failures are
        // configuration errors, not crypto failures.
        let keyring = Keyring::new(master_key_bytes, previous_keys)
            .map_err(|e| CachekitError::Config(format!("keyring: {e}")))?;

        Ok(Self {
            encryptor,
            derived_key: Zeroizing::new(tenant_keys.encryption_key),
            keyring,
            tenant_id: tenant_id.to_owned(),
        })
    }

    /// Encrypt plaintext with AAD bound to the cache key.
    ///
    /// Output format: `[nonce(12)][ciphertext + auth_tag(16)]`
    pub fn encrypt(&self, plaintext: &[u8], cache_key: &str) -> Result<Vec<u8>, CachekitError> {
        // compressed=false is normative, not a stub — see build_aad's invariant note.
        let aad = self.build_aad(cache_key, false);
        self.encryptor
            .encrypt_aes_gcm(plaintext, &*self.derived_key, &aad)
            .map_err(|e| CachekitError::Encryption(format!("encrypt failed: {e}")))
    }

    /// Decrypt ciphertext with AAD bound to the cache key.
    ///
    /// Returns the original plaintext. Fails if the cache key does not match
    /// the one used during encryption (ciphertext substitution protection).
    ///
    /// Decryption goes through the keyring: the current master key is
    /// attempted first, then each decrypt-only previous key in order, with
    /// the identical AAD per attempt. Entries written before a key rotation
    /// stay readable as long as their key remains in the previous list.
    pub fn decrypt(&self, ciphertext: &[u8], cache_key: &str) -> Result<Vec<u8>, CachekitError> {
        // compressed=false is normative, not a stub — see build_aad's invariant note.
        let aad = self.build_aad(cache_key, false);
        self.keyring
            .decrypt(&self.encryptor, ciphertext, &self.tenant_id, &aad)
            .map_err(|e| match e {
                // Config-class errors stay config-class (LAB-683 decision):
                // a derivation failure or caller bug must never masquerade as
                // a decrypt failure that fail-open callers read as a miss.
                cachekit_core::EncryptionError::KeyDerivation(_)
                | cachekit_core::EncryptionError::KeyringIndexOutOfRange { .. } => {
                    CachekitError::Config(format!("keyring decrypt misconfiguration: {e}"))
                }
                _ => CachekitError::Encryption(format!("decrypt failed: {e}")),
            })
    }

    /// Return the tenant ID used for key derivation.
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// Build AAD v0x03 for a given cache key and compression flag.
    ///
    /// Format: `[0x03][len][tenant_id][len][cache_key][len]["msgpack"][len]["True"/"False"]`
    ///
    /// All lengths are 4-byte big-endian u32 to prevent boundary-confusion attacks.
    ///
    /// # Invariant: `compressed` is always `false` in production
    ///
    /// `encrypt` and `decrypt` pass the literal `false`, and that is normative, not a
    /// gap: this SDK's only cross-SDK encrypted surface is interop mode, and
    /// `protocol/spec/interop-mode.md` ("Encryption in Interop Mode") mandates the AAD
    /// components `format = "msgpack"`, `compressed = "False"` — there is no
    /// compression in interop mode.
    ///
    /// Why a reader cannot recover from a wrong flag here: in the general flow a reader
    /// rebuilds the AAD from the **stored cleartext metadata** written alongside the
    /// ciphertext (`protocol/spec/encryption.md`, encrypt step 6 / decrypt step 3).
    /// cachekit-rs stores no such per-entry metadata — it writes plain
    /// `nonce ‖ ciphertext ‖ tag` with no header — so its readers have nothing to read
    /// the flag from and must reconstruct the AAD from the interop-pinned constants
    /// alone. Combined with the no-retry rule (a reader MUST NOT retry decryption with
    /// any alternative AAD input, `protocol/spec/encryption.md`), emitting
    /// `compressed = "True"` here would produce ciphertext that no conformant peer can
    /// authenticate, and none may probe for.
    ///
    /// So do not flip this literal to thread a live compression flag through
    /// `encrypt`/`decrypt`. If this SDK ever does compress, the flag must be *stored*
    /// per entry and threaded from that stored value — `protocol/spec/encryption.md`
    /// requires `compressed` to describe the actual plaintext, and claiming `"False"`
    /// over compressed bytes is the cachekit-py#166 conformance bug (round-trips
    /// in-process, fails authentication for every correct second reader). A compressed
    /// cross-SDK profile is a versioned protocol change (interop/v2, Multica LAB-1135),
    /// not an SDK flag.
    ///
    /// The `"True"`/`"False"` tokens are frozen byte-level protocol constants —
    /// normative byte table in `protocol/spec/encryption.md`, section
    /// "`compressed` tokens"; decided in
    /// [protocol#12](https://github.com/cachekit-io/protocol/issues/12) (resolved
    /// 2026-07-19: a spec correction, not a wire change). The `true` branch stays,
    /// exercised by conformance tests, so those bytes remain pinned.
    pub fn build_aad(&self, cache_key: &str, compressed: bool) -> Vec<u8> {
        let format_str = b"msgpack";
        let compressed_str = if compressed {
            b"True" as &[u8]
        } else {
            b"False"
        };

        let tenant_bytes = self.tenant_id.as_bytes();
        let key_bytes = cache_key.as_bytes();

        // Pre-allocate: version(1) + 4 length fields(16) + data
        let capacity =
            1 + 16 + tenant_bytes.len() + key_bytes.len() + format_str.len() + compressed_str.len();
        let mut aad = Vec::with_capacity(capacity);

        aad.push(AAD_VERSION);

        // All components are bounded: tenant_id <= 255 (validated in new()),
        // cache_key <= 1024 (validated by client), format/compressed are constants.
        // Safe to use len_u32 helper which saturates on overflow.

        // tenant_id
        aad.extend_from_slice(&len_u32(tenant_bytes.len()).to_be_bytes());
        aad.extend_from_slice(tenant_bytes);

        // cache_key
        aad.extend_from_slice(&len_u32(key_bytes.len()).to_be_bytes());
        aad.extend_from_slice(key_bytes);

        // format
        aad.extend_from_slice(&len_u32(format_str.len()).to_be_bytes());
        aad.extend_from_slice(format_str);

        // compressed flag
        aad.extend_from_slice(&len_u32(compressed_str.len()).to_be_bytes());
        aad.extend_from_slice(compressed_str);

        aad
    }
}

/// Convert a usize length to u32 for AAD encoding, saturating on overflow.
/// In practice all inputs are validated to fit (tenant_id <= 255, cache_key <= 1024).
#[allow(clippy::cast_possible_truncation)]
fn len_u32(len: usize) -> u32 {
    u32::try_from(len).unwrap_or(u32::MAX)
}

impl std::fmt::Debug for EncryptionLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptionLayer")
            .field("tenant_id", &self.tenant_id)
            .field("derived_key", &"[REDACTED]")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_MASTER_KEY: &[u8] = b"test_master_key_32_bytes_long!!!";
    const TEST_TENANT: &str = "test-tenant";

    #[test]
    fn roundtrip_encrypt_decrypt() {
        let layer = EncryptionLayer::new(TEST_MASTER_KEY, TEST_TENANT).unwrap();
        let plaintext = b"hello, zero-knowledge world";

        let ciphertext = layer.encrypt(plaintext, "my:key").unwrap();
        let decrypted = layer.decrypt(&ciphertext, "my:key").unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn wrong_cache_key_fails_decryption() {
        let layer = EncryptionLayer::new(TEST_MASTER_KEY, TEST_TENANT).unwrap();
        let ciphertext = layer.encrypt(b"secret", "key:a").unwrap();

        let result = layer.decrypt(&ciphertext, "key:b");
        assert!(result.is_err(), "decryption with wrong cache key must fail");
    }

    #[test]
    fn different_tenants_produce_different_ciphertext() {
        let layer_a = EncryptionLayer::new(TEST_MASTER_KEY, "tenant-a").unwrap();
        let layer_b = EncryptionLayer::new(TEST_MASTER_KEY, "tenant-b").unwrap();

        let ct_a = layer_a.encrypt(b"same data", "same:key").unwrap();
        let ct_b = layer_b.encrypt(b"same data", "same:key").unwrap();

        // Nonces differ, so ciphertext differs, but also keys differ
        assert_ne!(ct_a, ct_b);

        // Cross-tenant decryption must fail
        assert!(layer_b.decrypt(&ct_a, "same:key").is_err());
    }

    #[test]
    fn master_key_too_short() {
        let result = EncryptionLayer::new(b"short", "tenant");
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("at least 32 bytes"), "got: {msg}");
    }

    #[test]
    fn aad_v03_format() {
        let layer = EncryptionLayer::new(TEST_MASTER_KEY, TEST_TENANT).unwrap();
        let aad = layer.build_aad("user:42", false);

        // Version byte
        assert_eq!(aad[0], 0x03);

        // tenant_id length (4 bytes BE) + tenant_id
        let tenant_len = u32::from_be_bytes(aad[1..5].try_into().unwrap()) as usize;
        assert_eq!(tenant_len, TEST_TENANT.len());
        assert_eq!(&aad[5..5 + tenant_len], TEST_TENANT.as_bytes());

        // cache_key length + cache_key
        let offset = 5 + tenant_len;
        let key_len = u32::from_be_bytes(aad[offset..offset + 4].try_into().unwrap()) as usize;
        assert_eq!(key_len, 7); // "user:42"
        assert_eq!(&aad[offset + 4..offset + 4 + key_len], b"user:42");

        // format length + format
        let offset = offset + 4 + key_len;
        let fmt_len = u32::from_be_bytes(aad[offset..offset + 4].try_into().unwrap()) as usize;
        assert_eq!(&aad[offset + 4..offset + 4 + fmt_len], b"msgpack");

        // compressed length + compressed
        let offset = offset + 4 + fmt_len;
        let comp_len = u32::from_be_bytes(aad[offset..offset + 4].try_into().unwrap()) as usize;
        assert_eq!(&aad[offset + 4..offset + 4 + comp_len], b"False");
    }

    #[test]
    fn aad_compressed_flag() {
        let layer = EncryptionLayer::new(TEST_MASTER_KEY, TEST_TENANT).unwrap();
        let aad_false = layer.build_aad("k", false);
        let aad_true = layer.build_aad("k", true);

        assert_ne!(aad_false, aad_true);
        // "True" is at the end
        assert!(aad_true.ends_with(b"True"));
        assert!(aad_false.ends_with(b"False"));
    }

    // ── Keyring rotation ─────────────────────────────────────────────────────

    const K1: &[u8] = &[0x11; 32];
    const K2: &[u8] = &[0x22; 32];

    #[test]
    fn previous_key_decrypts_after_rotation() {
        // Written under k1 before the rotation...
        let old_layer = EncryptionLayer::new(K1, TEST_TENANT).unwrap();
        let ciphertext = old_layer.encrypt(b"pre-rotation value", "user:1").unwrap();

        // ...still readable with current=k2, previous=[k1] (identical AAD)...
        let rotated = EncryptionLayer::with_previous_keys(K2, &[K1], TEST_TENANT).unwrap();
        let plaintext = rotated.decrypt(&ciphertext, "user:1").unwrap();
        assert_eq!(plaintext, b"pre-rotation value");

        // ...and unreadable after a hard cut-over (previous=[]).
        let cut_over = EncryptionLayer::with_previous_keys(K2, &[], TEST_TENANT).unwrap();
        assert!(cut_over.decrypt(&ciphertext, "user:1").is_err());
    }

    #[test]
    fn rotated_layer_still_writes_under_current_key() {
        let rotated = EncryptionLayer::with_previous_keys(K2, &[K1], TEST_TENANT).unwrap();
        let ciphertext = rotated.encrypt(b"fresh write", "user:2").unwrap();

        // A current-key-only layer reads it: writes never use previous keys.
        let current_only = EncryptionLayer::new(K2, TEST_TENANT).unwrap();
        assert_eq!(
            current_only.decrypt(&ciphertext, "user:2").unwrap(),
            b"fresh write"
        );
    }

    #[test]
    fn exactly_three_previous_keys_is_accepted() {
        // Boundary success: a `>=` cap check instead of `>` would reject a
        // legitimate three-key rotation window and still pass every
        // rejecting-side test.
        let keys: Vec<[u8; 32]> = (1..=3).map(|i| [i; 32]).collect();
        let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();

        let layer = EncryptionLayer::with_previous_keys(K2, &refs, TEST_TENANT);
        assert!(
            layer.is_ok(),
            "cap is 3: exactly three previous keys must build"
        );
    }

    #[test]
    fn more_than_three_previous_keys_is_config_error() {
        let keys: Vec<[u8; 32]> = (1..=4).map(|i| [i; 32]).collect();
        let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();

        let result = EncryptionLayer::with_previous_keys(K2, &refs, TEST_TENANT);
        assert!(
            matches!(result, Err(CachekitError::Config(_))),
            "cap of 3 must reject, never truncate"
        );
    }

    #[test]
    fn current_key_in_previous_list_is_config_error() {
        let result = EncryptionLayer::with_previous_keys(K2, &[K1, K2], TEST_TENANT);
        assert!(
            matches!(result, Err(CachekitError::Config(_))),
            "forward-only: current key must not be decrypt-only"
        );
    }

    #[test]
    fn short_previous_key_is_config_error() {
        let short = [0x01u8; 16]; // core would accept 16; the rs SDK contract is 32
        let result = EncryptionLayer::with_previous_keys(K2, &[&short], TEST_TENANT);
        assert!(matches!(result, Err(CachekitError::Config(_))));
    }

    #[test]
    fn debug_redacts_key() {
        let layer = EncryptionLayer::new(TEST_MASTER_KEY, TEST_TENANT).unwrap();
        let debug = format!("{layer:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("test_master_key"));
    }
}
