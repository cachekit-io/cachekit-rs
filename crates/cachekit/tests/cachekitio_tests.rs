#![cfg(feature = "cachekitio")]

use cachekit::backend::cachekitio::CachekitIO;

/// Builder succeeds with a valid api_key + HTTPS url, and api_url() reflects what was set.
#[test]
fn cachekitio_builder() {
    let backend = CachekitIO::builder()
        .api_key("test-api-key")
        .api_url("https://api.example.com")
        .build()
        .expect("build should succeed");

    assert_eq!(backend.api_url(), "https://api.example.com");
}

/// Building without an api_key must return a Config error.
#[test]
fn cachekitio_builder_missing_api_key() {
    let result = CachekitIO::builder()
        .api_url("https://api.example.com")
        .build();

    assert!(result.is_err(), "expected error for missing api_key");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("api_key"),
        "error message should mention api_key, got: {err}"
    );
}

/// Building with an http:// URL must be rejected (HTTPS enforcement).
#[test]
fn cachekitio_builder_http_rejected() {
    let result = CachekitIO::builder()
        .api_key("test-api-key")
        .api_url("http://api.example.com")
        .build();

    assert!(result.is_err(), "expected error for http:// URL");
    let err = result.unwrap_err().to_string();
    assert!(
        err.to_lowercase().contains("https"),
        "error message should mention HTTPS, got: {err}"
    );
}
