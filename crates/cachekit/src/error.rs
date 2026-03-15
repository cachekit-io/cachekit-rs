use thiserror::Error;

/// Top-level error type for all CacheKit operations.
#[derive(Debug, Error)]
pub enum CachekitError {
    /// An error originating from the cache backend.
    #[error("backend error: {0}")]
    Backend(#[from] BackendError),

    /// Serialization or deserialization failed.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Encryption or decryption failed.
    #[error("encryption error: {0}")]
    Encryption(String),

    /// Configuration is invalid or missing required values.
    #[error("configuration error: {0}")]
    Config(String),

    /// The payload exceeds the maximum allowed size.
    #[error("payload too large: {size} bytes (limit: {limit} bytes)")]
    PayloadTooLarge { size: usize, limit: usize },

    /// The cache key is invalid (empty, too long, or contains illegal bytes).
    #[error("invalid cache key: {0}")]
    InvalidKey(String),
}

// ── BackendErrorKind ─────────────────────────────────────────────────────────

/// Classifies backend errors to determine retry behaviour.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendErrorKind {
    /// Temporary failure — safe to retry (network blip, pool exhaustion).
    Transient,
    /// Permanent failure — retrying will not help (bad request, key not found).
    Permanent,
    /// Request did not complete within the deadline — safe to retry.
    Timeout,
    /// Credentials are invalid or missing — retrying will not help.
    Authentication,
}

impl BackendErrorKind {
    /// Returns `true` if it is safe to retry the operation.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Transient | Self::Timeout)
    }
}

impl std::fmt::Display for BackendErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transient => write!(f, "transient"),
            Self::Permanent => write!(f, "permanent"),
            Self::Timeout => write!(f, "timeout"),
            Self::Authentication => write!(f, "authentication"),
        }
    }
}

// ── BackendError ─────────────────────────────────────────────────────────────

/// A structured error from a cache backend.
#[derive(Debug, Error)]
#[error("{kind} backend error: {message}")]
pub struct BackendError {
    /// Classification of this error.
    pub kind: BackendErrorKind,
    /// Human-readable description.
    pub message: String,
    /// The underlying error that caused this backend error, if any.
    #[cfg(not(target_arch = "wasm32"))]
    #[source]
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
    /// The underlying error that caused this backend error, if any.
    #[cfg(target_arch = "wasm32")]
    #[source]
    pub source: Option<Box<dyn std::error::Error>>,
}

impl BackendError {
    /// Create a transient (retryable) backend error.
    pub fn transient(message: impl Into<String>) -> Self {
        Self { kind: BackendErrorKind::Transient, message: message.into(), source: None }
    }

    /// Create a permanent (non-retryable) backend error.
    pub fn permanent(message: impl Into<String>) -> Self {
        Self { kind: BackendErrorKind::Permanent, message: message.into(), source: None }
    }

    /// Create a timeout backend error.
    pub fn timeout(message: impl Into<String>) -> Self {
        Self { kind: BackendErrorKind::Timeout, message: message.into(), source: None }
    }

    /// Create an authentication backend error.
    pub fn auth(message: impl Into<String>) -> Self {
        Self { kind: BackendErrorKind::Authentication, message: message.into(), source: None }
    }

    /// Construct a [`BackendError`] from an HTTP status code and response body.
    ///
    /// The body is truncated to 256 Unicode scalar values to avoid inflating error messages.
    pub fn from_http_status(status: u16, body: &[u8]) -> Self {
        let body_str = std::str::from_utf8(body).unwrap_or("<non-utf8 body>");
        let truncated: String = body_str.chars().take(256).collect();
        let message = format!("HTTP {status}: {truncated}");

        let kind = match status {
            401 | 403 => BackendErrorKind::Authentication,
            408 | 429 | 500 | 502 | 503 | 504 => BackendErrorKind::Transient,
            _ if status >= 500 => BackendErrorKind::Transient,
            _ => BackendErrorKind::Permanent,
        };

        Self { kind, message, source: None }
    }
}
