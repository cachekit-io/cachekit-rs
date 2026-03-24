use cachekit::config::{CachekitConfig, CachekitConfigBuilder};
use serial_test::serial;
use std::time::Duration;

// ── from_env defaults ────────────────────────────────────────────────────────

#[test]
#[serial]
fn config_from_env_defaults() {
    // Clear relevant env vars so we get defaults.
    std::env::remove_var("CACHEKIT_API_KEY");
    std::env::remove_var("CACHEKIT_API_URL");
    std::env::remove_var("CACHEKIT_MASTER_KEY");
    std::env::remove_var("CACHEKIT_DEFAULT_TTL");

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
    std::env::set_var("CACHEKIT_API_KEY", "test-key-123");
    let config = CachekitConfig::from_env().expect("from_env failed");
    std::env::remove_var("CACHEKIT_API_KEY");

    // Use .as_ref().map(|k| k.as_str()) NOT .as_deref()
    assert_eq!(
        config.api_key.as_ref().map(|k| k.as_str()),
        Some("test-key-123")
    );
}

#[test]
#[serial]
fn config_from_env_rejects_http_url() {
    std::env::set_var("CACHEKIT_API_URL", "http://insecure.example.com");
    let result = CachekitConfig::from_env();
    std::env::remove_var("CACHEKIT_API_URL");

    assert!(result.is_err(), "expected error for non-HTTPS api_url");
}

#[test]
#[serial]
fn config_from_env_accepts_https_url() {
    std::env::set_var("CACHEKIT_API_URL", "https://custom.cachekit.io");
    let config = CachekitConfig::from_env().expect("from_env failed");
    std::env::remove_var("CACHEKIT_API_URL");

    assert_eq!(config.api_url, "https://custom.cachekit.io");
}

#[test]
#[serial]
fn config_from_env_rejects_short_master_key() {
    // 31 bytes = 62 hex chars — too short
    std::env::set_var("CACHEKIT_MASTER_KEY", "aa".repeat(31));
    let result = CachekitConfig::from_env();
    std::env::remove_var("CACHEKIT_MASTER_KEY");

    assert!(result.is_err(), "expected error for short master key");
}

#[test]
#[serial]
fn config_from_env_accepts_32_byte_master_key() {
    // 32 bytes = 64 hex chars — minimum valid
    std::env::set_var("CACHEKIT_MASTER_KEY", "ab".repeat(32));
    let config = CachekitConfig::from_env().expect("from_env failed");
    std::env::remove_var("CACHEKIT_MASTER_KEY");

    assert!(config.master_key.is_some());
    assert_eq!(config.master_key.as_ref().unwrap().len(), 32);
}

#[test]
#[serial]
fn config_from_env_rejects_ttl_zero() {
    std::env::set_var("CACHEKIT_DEFAULT_TTL", "0");
    let result = CachekitConfig::from_env();
    std::env::remove_var("CACHEKIT_DEFAULT_TTL");

    assert!(result.is_err(), "expected error for TTL=0");
}

#[test]
#[serial]
fn config_from_env_accepts_ttl_one() {
    std::env::set_var("CACHEKIT_DEFAULT_TTL", "1");
    let config = CachekitConfig::from_env().expect("from_env failed");
    std::env::remove_var("CACHEKIT_DEFAULT_TTL");

    assert_eq!(config.default_ttl, Duration::from_secs(1));
}

// ── Debug redaction ───────────────────────────────────────────────────────────

#[test]
#[serial]
fn config_debug_redacts_secrets() {
    std::env::set_var("CACHEKIT_API_KEY", "super-secret-key");
    std::env::set_var("CACHEKIT_MASTER_KEY", "ab".repeat(32));
    let config = CachekitConfig::from_env().expect("from_env failed");
    std::env::remove_var("CACHEKIT_API_KEY");
    std::env::remove_var("CACHEKIT_MASTER_KEY");

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
