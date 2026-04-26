//! CacheKit — production-ready caching for Rust.
//!
//! Supports cachekit.io SaaS, Redis, and Cloudflare Workers backends.
//! Zero-knowledge encryption via AES-256-GCM with HKDF key derivation.

// Production code lints — these only fire in src/, not tests/
#![warn(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#![warn(missing_docs)]

// Mutually exclusive feature guards
#[cfg(all(feature = "workers", feature = "redis"))]
compile_error!(
    "features `workers` and `redis` are mutually exclusive — Workers runtime cannot use fred"
);

#[cfg(all(feature = "workers", feature = "l1"))]
compile_error!("features `workers` and `l1` are mutually exclusive — moka requires std threads unavailable in wasm32");

/// Pluggable cache backend trait and implementations (CachekitIO, Redis, Workers).
pub mod backend;
/// High-level cache client with dual-layer (L1/L2) support.
pub mod client;
/// Configuration types and environment variable parsing.
pub mod config;
/// Error types for cache operations and backend communication.
pub mod error;
/// Cache key generation using Blake2b hashing.
pub mod key;
/// L1 cache hit-rate metrics for CachekitIO request headers.
pub mod metrics;
/// Serialization and deserialization of cached values via MessagePack.
pub mod serializer;
/// SDK session tracking (session ID and start timestamp).
pub mod session;
/// SSRF-safe URL validation for CachekitIO endpoints.
pub mod url_validator;

/// Intent-based cache presets (`CacheKit::minimal`, `::production`, `::encrypted`, `::io`).
mod intents;

/// Client-side AES-256-GCM encryption with HKDF key derivation.
#[cfg(feature = "encryption")]
pub mod encryption;

/// In-process L1 cache backed by [`moka`] with per-entry TTL.
#[cfg(feature = "l1")]
pub mod l1;

// Re-exports
pub use client::{CacheKit, CacheKitBuilder, SharedBackend};
pub use config::CachekitConfig;
pub use error::{BackendError, BackendErrorKind, CachekitError};

#[cfg(feature = "encryption")]
pub use client::SecureCache;
#[cfg(feature = "encryption")]
pub use encryption::EncryptionLayer;

#[cfg(feature = "macros")]
pub use cachekit_macros::cachekit;

/// Re-exports for proc-macro generated code. Not part of the public API.
#[doc(hidden)]
pub mod __private {
    pub use rmp_serde;
}

/// Convenient glob import for the most common types.
pub mod prelude {
    pub use crate::{
        BackendError, BackendErrorKind, CacheKit, CacheKitBuilder, CachekitConfig, CachekitError,
    };

    #[cfg(feature = "encryption")]
    pub use crate::{EncryptionLayer, SecureCache};
}
