//! CacheKit — production-ready caching for Rust.
//!
//! Supports cachekit.io SaaS, Redis, and Cloudflare Workers backends.
//! Zero-knowledge encryption via AES-256-GCM with HKDF key derivation.

// Mutually exclusive feature guards
#[cfg(all(feature = "workers", feature = "redis"))]
compile_error!("features `workers` and `redis` are mutually exclusive — Workers runtime cannot use fred");

#[cfg(all(feature = "workers", feature = "l1"))]
compile_error!("features `workers` and `l1` are mutually exclusive — moka requires std threads unavailable in wasm32");

pub mod backend;
pub mod client;
pub mod config;
pub mod error;
pub mod key;
pub mod serializer;

#[cfg(feature = "encryption")]
pub mod encryption;

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

/// Convenient glob import for the most common types.
pub mod prelude {
    pub use crate::{BackendError, BackendErrorKind, CacheKit, CacheKitBuilder, CachekitConfig, CachekitError};

    #[cfg(feature = "encryption")]
    pub use crate::{EncryptionLayer, SecureCache};
}
