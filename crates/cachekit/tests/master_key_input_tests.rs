//! Master key input rows from the shared protocol vectors, driven through
//! every public hex and raw-bytes key entry point.
//!
//! Vectors: `tests/vectors/encryption.json`, vendored verbatim from
//! cachekit-io/protocol test-vectors/encryption.json 1.3.0
//! (<https://github.com/cachekit-io/protocol/pull/161>)
//! (sha256 `f701951147a47c42a968850fe6cb73a544313728e46f45fec3d811c20ed4b377`).
//! Do not edit the JSON here; regenerate upstream and re-vendor.
//!
//! Every reject must be a `Config` error from the key validator, not merely
//! an `Err`: `CacheKit::secure` would also fail on the Redis connect a
//! wrongly accepted key reaches. `OTHER_KEY_HEX` fills the slot a row does not,
//! so the repeat-key check cannot fire in the decoder's place. The accepting
//! controls show no entry point passes by refusing everything.
//!
//! The accept row's decrypt through the `secure` preset and the rotation read
//! live in `src/intents.rs` (`secure_tests`): they need the crate-internal
//! preset route, as default CI has no Redis.
//!
//! Run with:
//!   cargo test --test master_key_input_tests --features redis,encryption

#![cfg(feature = "encryption")]

mod common;

use cachekit::config::CachekitConfigBuilder;
use cachekit::{CacheKit, CachekitConfig, CachekitError, EncryptionLayer};
use common::EnvGuard;
use serde_json::Value as Json;
use serial_test::serial;

const VECTORS_JSON: &str = include_str!("vectors/encryption.json");

/// sha256 of the vendored file, pinned so a local edit cannot drift from the
/// protocol copy unnoticed.
const VECTORS_SHA256: &str = "f701951147a47c42a968850fe6cb73a544313728e46f45fec3d811c20ed4b377"; // pragma: allowlist secret

/// A valid 32-byte key that no row decodes to (`default_tenant_interop`'s), for
/// the slot a row does not fill, so a previous-key row is never refused as a
/// repeat of the current key.
const OTHER_KEY_HEX: &str = "6161616161616161616161616161616161616161616161616161616161616161"; // pragma: allowlist secret

const TENANT: &str = "default";

#[test]
fn vendored_fixture_matches_the_pinned_sha256() {
    use sha2::{Digest, Sha256};
    assert_eq!(
        hex::encode(Sha256::digest(VECTORS_JSON.as_bytes())),
        VECTORS_SHA256,
        "tests/vectors/encryption.json differs from the pinned protocol copy: \
         re-vendor it from protocol and update VECTORS_SHA256"
    );
}

fn block() -> Json {
    let all: Json =
        serde_json::from_str(VECTORS_JSON).expect("vendored vector file must be valid JSON");
    all["master_key_input"].clone()
}

/// `(name, field)` of every row in `group`, the count asserted exactly so a
/// silently skipped row fails loudly.
fn rows(group: &str, field: &str, count: usize) -> Vec<(String, String)> {
    let rows: Vec<(String, String)> = block()[group]
        .as_array()
        .unwrap_or_else(|| panic!("{group} must be an array"))
        .iter()
        .map(|row| {
            let get = |k: &str| {
                row[k]
                    .as_str()
                    .unwrap_or_else(|| panic!("{group} row lacks {k}"))
                    .to_owned()
            };
            (get("name"), get(field))
        })
        .collect();
    assert_eq!(rows.len(), count, "{group} row count");
    rows
}

fn accept_hex() -> String {
    rows("accept_vectors", "master_key_hex", 1).remove(0).1
}

fn reject_rows() -> Vec<(String, String)> {
    rows("reject_vectors", "master_key_hex", 11)
}

fn raw_reject_rows() -> Vec<(String, Vec<u8>)> {
    rows("raw_reject_vectors", "raw_key_hex", 5)
        .into_iter()
        .map(|(name, h)| (name, hex::decode(h).expect("raw_key_hex is hex")))
        .collect()
}

#[track_caller]
fn assert_config_err<T>(result: Result<T, CachekitError>, names: &str, row: &str, entry: &str) {
    match result {
        Err(CachekitError::Config(msg)) if msg.contains(names) => {}
        Err(e) => panic!("{row} via {entry}: want a Config error naming {names:?}, got {e:?}"),
        Ok(_) => panic!("{row} via {entry}: accepted a key protocol requires refused"),
    }
}

// ── Hex entry points ─────────────────────────────────────────────────────────

#[test]
fn builder_encryption_rejects_every_hex_reject_row() {
    assert!(CacheKit::builder()
        .encryption(&accept_hex(), TENANT)
        .is_ok());
    for (row, key) in reject_rows() {
        assert_config_err(
            CacheKit::builder().encryption(&key, TENANT),
            "master key",
            &row,
            "CacheKitBuilder::encryption",
        );
    }
}

#[test]
fn config_builder_rejects_every_hex_reject_row() {
    let accept = accept_hex();
    assert!(CachekitConfigBuilder::new()
        .master_key(&accept)
        .and_then(|b| b.previous_master_keys(&[OTHER_KEY_HEX]))
        .is_ok());
    assert!(CachekitConfigBuilder::new()
        .master_key(OTHER_KEY_HEX)
        .and_then(|b| b.previous_master_keys(&[&accept]))
        .is_ok());
    for (row, key) in reject_rows() {
        assert_config_err(
            CachekitConfigBuilder::new().master_key(&key),
            "master_key",
            &row,
            "CachekitConfigBuilder::master_key",
        );
        assert_config_err(
            CachekitConfigBuilder::new()
                .master_key(OTHER_KEY_HEX)
                .and_then(|b| b.previous_master_keys(&[&key])),
            "previous_master_keys",
            &row,
            "CachekitConfigBuilder::previous_master_keys",
        );
    }
}

#[test]
#[serial]
fn config_from_env_rejects_every_hex_reject_row() {
    let accept = accept_hex();
    {
        let _env = EnvGuard::set(&[
            ("CACHEKIT_MASTER_KEY", Some(&accept)),
            ("CACHEKIT_PREVIOUS_MASTER_KEYS", Some(OTHER_KEY_HEX)),
        ]);
        assert!(CachekitConfig::from_env().is_ok());
    }
    for (row, key) in reject_rows() {
        let current = {
            let _env = EnvGuard::set(&[
                ("CACHEKIT_MASTER_KEY", Some(&key)),
                ("CACHEKIT_PREVIOUS_MASTER_KEYS", None),
            ]);
            CachekitConfig::from_env()
        };
        assert_config_err(
            current,
            "CACHEKIT_MASTER_KEY",
            &row,
            "CachekitConfig::from_env",
        );
        let previous = {
            let _env = EnvGuard::set(&[
                ("CACHEKIT_MASTER_KEY", Some(OTHER_KEY_HEX)),
                ("CACHEKIT_PREVIOUS_MASTER_KEYS", Some(&key)),
            ]);
            CachekitConfig::from_env()
        };
        assert_config_err(
            previous,
            "CACHEKIT_PREVIOUS_MASTER_KEYS",
            &row,
            "CachekitConfig::from_env (previous keys)",
        );
    }
}

/// Port 1 refuses at once, so a wrongly accepted key ends in a connect error,
/// which `assert_config_err` reports, rather than a hang.
#[cfg(feature = "redis")]
const UNREACHABLE_REDIS: &str = "redis://127.0.0.1:1";

#[cfg(feature = "redis")]
#[tokio::test]
async fn secure_rejects_every_hex_reject_row() {
    for (row, key) in reject_rows() {
        assert_config_err(
            CacheKit::secure(UNREACHABLE_REDIS, &key).await,
            "master_key_hex",
            &row,
            "CacheKit::secure",
        );
    }
}

#[cfg(feature = "redis")]
#[tokio::test]
#[serial]
async fn secure_from_env_rejects_every_hex_reject_row() {
    for (row, key) in reject_rows() {
        let current = {
            let _env = EnvGuard::set(&[
                ("CACHEKIT_MASTER_KEY", Some(&key)),
                ("CACHEKIT_PREVIOUS_MASTER_KEYS", None),
            ]);
            CacheKit::secure_from_env(UNREACHABLE_REDIS).await
        };
        assert_config_err(
            current,
            "CACHEKIT_MASTER_KEY",
            &row,
            "CacheKit::secure_from_env",
        );
        let previous = {
            let _env = EnvGuard::set(&[
                ("CACHEKIT_MASTER_KEY", Some(OTHER_KEY_HEX)),
                ("CACHEKIT_PREVIOUS_MASTER_KEYS", Some(&key)),
            ]);
            CacheKit::secure_from_env(UNREACHABLE_REDIS).await
        };
        assert_config_err(
            previous,
            "CACHEKIT_PREVIOUS_MASTER_KEYS",
            &row,
            "CacheKit::secure_from_env (previous keys)",
        );
    }
}

// ── Raw-bytes entry points ───────────────────────────────────────────────────

const RAW_LEN_ERR: &str = "exactly 32 bytes";

#[test]
fn raw_entry_points_accept_the_accept_rows_bytes() {
    let accept = hex::decode(accept_hex()).expect("accept row is hex");
    let other = hex::decode(OTHER_KEY_HEX).expect("hex");
    assert!(CacheKit::builder()
        .encryption_from_bytes(&accept, TENANT)
        .is_ok());
    assert!(CacheKit::builder()
        .encryption_from_bytes_with_previous(&accept, &[&other], TENANT)
        .is_ok());
    assert!(CacheKit::builder()
        .encryption_from_bytes_with_previous(&other, &[&accept], TENANT)
        .is_ok());
    assert!(EncryptionLayer::new(&accept, TENANT).is_ok());
    assert!(EncryptionLayer::with_previous_keys(&accept, &[&other], TENANT).is_ok());
    assert!(EncryptionLayer::with_previous_keys(&other, &[&accept], TENANT).is_ok());
}

#[test]
fn raw_entry_points_reject_every_raw_reject_row() {
    let other = hex::decode(OTHER_KEY_HEX).expect("hex");
    for (row, key) in raw_reject_rows() {
        let cases: [(&str, Result<(), CachekitError>); 6] = [
            (
                "CacheKitBuilder::encryption_from_bytes",
                CacheKit::builder()
                    .encryption_from_bytes(&key, TENANT)
                    .map(drop),
            ),
            (
                "CacheKitBuilder::encryption_from_bytes_with_previous (current)",
                CacheKit::builder()
                    .encryption_from_bytes_with_previous(&key, &[&other], TENANT)
                    .map(drop),
            ),
            (
                "CacheKitBuilder::encryption_from_bytes_with_previous (previous)",
                CacheKit::builder()
                    .encryption_from_bytes_with_previous(&other, &[&key], TENANT)
                    .map(drop),
            ),
            (
                "EncryptionLayer::new",
                EncryptionLayer::new(&key, TENANT).map(drop),
            ),
            (
                "EncryptionLayer::with_previous_keys (current)",
                EncryptionLayer::with_previous_keys(&key, &[&other], TENANT).map(drop),
            ),
            (
                "EncryptionLayer::with_previous_keys (previous)",
                EncryptionLayer::with_previous_keys(&other, &[&key], TENANT).map(drop),
            ),
        ];
        for (entry, result) in cases {
            assert_config_err(result, RAW_LEN_ERR, &row, entry);
        }
    }
}
