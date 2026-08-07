use cachekit::config::{CachekitConfig, CachekitConfigBuilder};
use serial_test::serial;
use std::time::Duration;

// ── from_env defaults ────────────────────────────────────────────────────────

#[test]
#[serial]
fn config_from_env_defaults() {
    // Clear every from_env-read variable so we get defaults.
    let _env = EnvGuard::set(&[
        ("CACHEKIT_API_KEY", None),
        ("CACHEKIT_API_URL", None),
        ("CACHEKIT_MASTER_KEY", None),
        ("CACHEKIT_PREVIOUS_MASTER_KEYS", None),
        ("CACHEKIT_DEFAULT_TTL", None),
    ]);

    let config = CachekitConfig::from_env().expect("from_env failed with no env vars");

    assert_eq!(config.api_url, "https://api.cachekit.io");
    assert_eq!(config.default_ttl, Duration::from_secs(300));
    assert_eq!(config.l1_capacity, 1000);
    assert_eq!(config.max_payload_bytes, 5 * 1024 * 1024);
    // api_key absent by default
    assert!(config.api_key.is_none());
    // master_key absent by default
    assert!(config.master_key.is_none());
}

#[test]
#[serial]
fn config_from_env_reads_api_key() {
    let _env = EnvGuard::set(&[("CACHEKIT_API_KEY", Some("test-key-123"))]);
    let config = CachekitConfig::from_env().expect("from_env failed");

    // Use .as_ref().map(|k| k.as_str()) NOT .as_deref()
    assert_eq!(
        config.api_key.as_ref().map(|k| k.as_str()),
        Some("test-key-123")
    );
}

#[test]
#[serial]
fn config_from_env_rejects_http_url() {
    let _env = EnvGuard::set(&[("CACHEKIT_API_URL", Some("http://insecure.example.com"))]);

    assert!(
        CachekitConfig::from_env().is_err(),
        "expected error for non-HTTPS api_url"
    );
}

#[test]
#[serial]
fn config_from_env_accepts_https_url() {
    let _env = EnvGuard::set(&[("CACHEKIT_API_URL", Some("https://custom.cachekit.io"))]);
    let config = CachekitConfig::from_env().expect("from_env failed");

    assert_eq!(config.api_url, "https://custom.cachekit.io");
}

#[test]
#[serial]
fn config_from_env_rejects_short_master_key() {
    // 31 bytes = 62 hex chars — too short
    let _env = EnvGuard::set(&[("CACHEKIT_MASTER_KEY", Some(&"aa".repeat(31)))]);

    assert!(
        CachekitConfig::from_env().is_err(),
        "expected error for short master key"
    );
}

#[test]
#[serial]
fn config_from_env_accepts_32_byte_master_key() {
    // 32 bytes = 64 hex chars — minimum valid
    let _env = EnvGuard::set(&[("CACHEKIT_MASTER_KEY", Some(&"ab".repeat(32)))]);
    let config = CachekitConfig::from_env().expect("from_env failed");

    assert!(config.master_key.is_some());
    assert_eq!(config.master_key.as_ref().unwrap().len(), 32);
}

#[test]
#[serial]
fn config_from_env_rejects_ttl_zero() {
    let _env = EnvGuard::set(&[("CACHEKIT_DEFAULT_TTL", Some("0"))]);

    assert!(
        CachekitConfig::from_env().is_err(),
        "expected error for TTL=0"
    );
}

#[test]
#[serial]
fn config_from_env_accepts_ttl_one() {
    let _env = EnvGuard::set(&[("CACHEKIT_DEFAULT_TTL", Some("1"))]);
    let config = CachekitConfig::from_env().expect("from_env failed");

    assert_eq!(config.default_ttl, Duration::from_secs(1));
}

// ── Debug redaction ───────────────────────────────────────────────────────────

#[test]
#[serial]
fn config_debug_redacts_secrets() {
    let _env = EnvGuard::set(&[
        ("CACHEKIT_API_KEY", Some("super-secret-key")),
        ("CACHEKIT_MASTER_KEY", Some(&"ab".repeat(32))),
    ]);
    let config = CachekitConfig::from_env().expect("from_env failed");

    let debug_str = format!("{config:?}");
    assert!(
        !debug_str.contains("super-secret-key"),
        "api_key leaked in debug: {debug_str}"
    );
    assert!(
        debug_str.contains("[REDACTED]"),
        "expected [REDACTED] in debug: {debug_str}"
    );
}

// ── Builder ───────────────────────────────────────────────────────────────────

#[test]
fn config_builder_basic() {
    let config = CachekitConfigBuilder::new()
        .api_key("my-api-key")
        .api_url("https://api.example.io")
        .expect("valid url")
        .default_ttl(Duration::from_secs(60))
        .expect("valid ttl")
        .namespace("myapp")
        .l1_capacity(500)
        .build();

    assert_eq!(config.api_url, "https://api.example.io");
    assert_eq!(config.default_ttl, Duration::from_secs(60));
    assert_eq!(config.namespace.as_deref(), Some("myapp"));
    assert_eq!(config.l1_capacity, 500);
    assert_eq!(
        config.api_key.as_ref().map(|k| k.as_str()),
        Some("my-api-key")
    );
}

#[test]
fn config_builder_rejects_http_url() {
    let result = CachekitConfigBuilder::new().api_url("http://not-secure.example.com");
    assert!(result.is_err(), "expected error for HTTP url in builder");
}

#[test]
fn config_builder_rejects_short_master_key() {
    let short_hex = "aa".repeat(31); // 31 bytes
    let result = CachekitConfigBuilder::new().master_key(&short_hex);
    assert!(
        result.is_err(),
        "expected error for short master key in builder"
    );
}

#[test]
fn config_builder_accepts_valid_master_key() {
    let valid_hex = "ab".repeat(32); // 32 bytes
    let config = CachekitConfigBuilder::new()
        .master_key(&valid_hex)
        .expect("valid master key")
        .build();
    assert!(config.master_key.is_some());
    assert_eq!(config.master_key.as_ref().unwrap().len(), 32);
}

// ── Previous master keys (key rotation) ───────────────────────────────────────

fn hexkey(byte: u8) -> String {
    hex::encode([byte; 32])
}

fn assert_config_err(result: Result<CachekitConfigBuilder, cachekit::CachekitError>, what: &str) {
    match result {
        Err(cachekit::CachekitError::Config(_)) => {}
        other => panic!(
            "{what}: expected CachekitError::Config, got {:?}",
            other.map(|_| "Ok(builder)")
        ),
    }
}

/// RAII guard for `#[serial]` env tests: records each variable's pre-test
/// value and restores it on drop — including on assertion failure — so a
/// test can never destroy state the surrounding shell exported.
struct EnvGuard {
    saved: Vec<(&'static str, Option<String>)>,
}

impl EnvGuard {
    /// Apply `(name, value)` pairs: `Some` sets the variable, `None` removes
    /// it. The prior value of every named variable is restored on drop.
    fn set(vars: &[(&'static str, Option<&str>)]) -> Self {
        let saved = vars
            .iter()
            .map(|(name, _)| (*name, std::env::var(name).ok()))
            .collect();
        for (name, value) in vars {
            match value {
                Some(v) => std::env::set_var(name, v),
                None => std::env::remove_var(name),
            }
        }
        Self { saved }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (name, value) in &self.saved {
            match value {
                Some(v) => std::env::set_var(name, v),
                None => std::env::remove_var(name),
            }
        }
    }
}

#[test]
fn config_builder_accepts_previous_master_keys() {
    let config = CachekitConfigBuilder::new()
        .master_key(&hexkey(0x22))
        .expect("valid master key")
        .previous_master_keys(&[hexkey(0x11).as_str(), hexkey(0x33).as_str()])
        .expect("valid previous keys")
        .build();

    assert_eq!(config.previous_master_keys.len(), 2);
    // Attempt order preserved: slice order.
    assert_eq!(config.previous_master_keys[0].as_slice(), &[0x11u8; 32]);
    assert_eq!(config.previous_master_keys[1].as_slice(), &[0x33u8; 32]);
}

#[test]
fn config_builder_accepts_exactly_three_previous_keys() {
    let keys: Vec<String> = (1..=3).map(hexkey).collect();
    let refs: Vec<&str> = keys.iter().map(String::as_str).collect();

    let config = CachekitConfigBuilder::new()
        .master_key(&hexkey(0x22))
        .expect("valid master key")
        .previous_master_keys(&refs)
        .expect("exactly three previous keys must be accepted")
        .build();

    assert_eq!(config.previous_master_keys.len(), 3);
}

#[test]
fn config_builder_rejects_more_than_three_previous_keys() {
    let keys: Vec<String> = (1..=4).map(hexkey).collect();
    let refs: Vec<&str> = keys.iter().map(String::as_str).collect();

    let result = CachekitConfigBuilder::new()
        .master_key(&hexkey(0x22))
        .expect("valid master key")
        .previous_master_keys(&refs);
    assert_config_err(result, "cap of 3 must reject, never truncate");
}

#[test]
fn config_builder_rejects_invalid_hex_previous_key() {
    let result = CachekitConfigBuilder::new().previous_master_keys(&["not-hex"]);
    assert_config_err(result, "invalid hex previous key");
}

#[test]
fn config_builder_rejects_short_previous_key() {
    let short_hex = "aa".repeat(31);
    let result = CachekitConfigBuilder::new().previous_master_keys(&[short_hex.as_str()]);
    assert_config_err(result, "short previous key");
}

#[test]
fn config_builder_rejects_master_key_in_previous_list() {
    let current = hexkey(0x22);

    // previous set after master_key
    let result = CachekitConfigBuilder::new()
        .master_key(&current)
        .expect("valid master key")
        .previous_master_keys(&[current.as_str()]);
    assert_config_err(result, "self-collision (previous after master)");

    // master_key set after previous — same invariant, other call order
    let result = CachekitConfigBuilder::new()
        .previous_master_keys(&[current.as_str()])
        .expect("valid previous keys")
        .master_key(&current);
    assert_config_err(result, "self-collision (master after previous)");
}

#[test]
#[serial]
fn config_from_env_reads_previous_master_keys() {
    let _env = EnvGuard::set(&[
        ("CACHEKIT_MASTER_KEY", Some(&hexkey(0x22))),
        (
            "CACHEKIT_PREVIOUS_MASTER_KEYS",
            // whitespace around commas tolerated
            Some(&format!("{}, {}", hexkey(0x11), hexkey(0x33))),
        ),
    ]);

    let config = CachekitConfig::from_env().expect("from_env failed");
    assert_eq!(config.previous_master_keys.len(), 2);
    assert_eq!(config.previous_master_keys[0].as_slice(), &[0x11u8; 32]);
    assert_eq!(config.previous_master_keys[1].as_slice(), &[0x33u8; 32]);
}

#[test]
#[serial]
fn config_from_env_rejects_more_than_three_previous_keys() {
    let val: Vec<String> = (1..=4).map(hexkey).collect();
    let _env = EnvGuard::set(&[
        ("CACHEKIT_MASTER_KEY", Some(&hexkey(0x99))),
        ("CACHEKIT_PREVIOUS_MASTER_KEYS", Some(&val.join(","))),
    ]);

    assert!(
        matches!(
            CachekitConfig::from_env(),
            Err(cachekit::CachekitError::Config(_))
        ),
        "cap of 3 must reject at load, never truncate"
    );
}

#[test]
#[serial]
fn config_from_env_rejects_master_key_in_previous_list() {
    let _env = EnvGuard::set(&[
        ("CACHEKIT_MASTER_KEY", Some(&hexkey(0x22))),
        (
            "CACHEKIT_PREVIOUS_MASTER_KEYS",
            Some(&format!("{},{}", hexkey(0x11), hexkey(0x22))),
        ),
    ]);

    assert!(
        matches!(
            CachekitConfig::from_env(),
            Err(cachekit::CachekitError::Config(_))
        ),
        "current key in previous list must fail at load"
    );
}

#[test]
#[serial]
fn config_from_env_rejects_empty_previous_key_entry() {
    let _env = EnvGuard::set(&[
        ("CACHEKIT_MASTER_KEY", Some(&hexkey(0x22))),
        (
            "CACHEKIT_PREVIOUS_MASTER_KEYS",
            // trailing comma → empty entry
            Some(&format!("{},", hexkey(0x11))),
        ),
    ]);

    assert!(
        CachekitConfig::from_env().is_err(),
        "empty entry must be rejected, not skipped"
    );
}

#[test]
#[serial]
fn config_from_env_tolerates_blank_previous_master_keys() {
    // Blanking the variable is how shell profiles, Compose files, and k8s
    // manifests retire it after a completed rotation — that must be a clean
    // cut-over, not a start-up failure.
    let _env = EnvGuard::set(&[
        ("CACHEKIT_MASTER_KEY", None),
        ("CACHEKIT_PREVIOUS_MASTER_KEYS", Some("  ")),
    ]);

    let config = CachekitConfig::from_env().expect("blank value must be treated as unset");
    assert!(config.previous_master_keys.is_empty());
}

/// Drift guard: the config-level cap (`MAX_PREVIOUS_MASTER_KEYS` is declared
/// outside the `encryption` feature gate, so `config.rs` cannot reference the
/// core const directly) must equal the core keyring's cap. CI's main test job
/// enables `encryption`, so this guard runs there.
#[test]
#[cfg(feature = "encryption")]
fn previous_key_cap_matches_core_keyring_cap() {
    assert_eq!(
        cachekit::config::MAX_PREVIOUS_MASTER_KEYS,
        cachekit_core::MAX_DECRYPT_ONLY_KEYS
    );
}

#[test]
#[serial]
fn config_from_env_rejects_previous_keys_without_master_key() {
    let _env = EnvGuard::set(&[
        ("CACHEKIT_MASTER_KEY", None),
        ("CACHEKIT_PREVIOUS_MASTER_KEYS", Some(&hexkey(0x11))),
    ]);

    assert!(
        matches!(
            CachekitConfig::from_env(),
            Err(cachekit::CachekitError::Config(_))
        ),
        "previous keys without a current master key must fail at load, not be silently dropped"
    );
}
