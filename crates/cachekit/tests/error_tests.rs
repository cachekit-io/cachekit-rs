use cachekit::error::{BackendError, BackendErrorKind, CachekitError};

#[test]
fn backend_error_display_transient() {
    let err = BackendError::transient("connection refused");
    let display = format!("{err}");
    assert!(display.contains("connection refused"), "display: {display}");
}

#[test]
fn backend_error_display_permanent() {
    let err = BackendError::permanent("unauthorized");
    let display = format!("{err}");
    assert!(display.contains("unauthorized"), "display: {display}");
}

#[test]
fn cachekit_error_from_backend_error() {
    let backend_err = BackendError::timeout("request timed out");
    let cachekit_err: CachekitError = backend_err.into();
    let display = format!("{cachekit_err}");
    assert!(display.contains("request timed out"), "display: {display}");
}

#[test]
fn payload_too_large_formatting() {
    let err = CachekitError::PayloadTooLarge {
        size: 6_000_000,
        limit: 5_242_880,
    };
    let display = format!("{err}");
    assert!(display.contains("6000000"), "display: {display}");
    assert!(display.contains("5242880"), "display: {display}");
}

#[test]
fn backend_error_kind_is_retryable() {
    assert!(BackendErrorKind::Transient.is_retryable());
    assert!(BackendErrorKind::Timeout.is_retryable());
    assert!(!BackendErrorKind::Permanent.is_retryable());
    assert!(!BackendErrorKind::Authentication.is_retryable());
}

#[test]
fn backend_error_from_http_status_401() {
    let err = BackendError::from_http_status(401, b"Unauthorized");
    assert_eq!(err.kind, BackendErrorKind::Authentication);
    assert!(!err.kind.is_retryable());
}

#[test]
fn backend_error_from_http_status_503() {
    let err = BackendError::from_http_status(503, b"Service Unavailable");
    assert_eq!(err.kind, BackendErrorKind::Transient);
    assert!(err.kind.is_retryable());
}

#[test]
fn backend_error_from_http_status_404() {
    let err = BackendError::from_http_status(404, b"Not Found");
    assert_eq!(err.kind, BackendErrorKind::Permanent);
    assert!(!err.kind.is_retryable());
}

#[test]
fn backend_error_truncates_long_body() {
    let long_body = b"x".repeat(1000);
    let err = BackendError::from_http_status(500, &long_body);
    // message should not be longer than 256 chars from body + status prefix
    let body_part = err.message.replace("HTTP 500: ", "");
    assert!(
        body_part.chars().count() <= 256,
        "body not truncated: {} chars",
        body_part.chars().count()
    );
}
