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
pub mod encryption;
pub mod error;
pub mod key;
pub mod serializer;

#[cfg(feature = "l1")]
pub mod l1;

// Re-exports — populated as modules are implemented
// pub use client::CacheKit;
// pub use config::CachekitConfig;
// pub use error::{BackendError, BackendErrorKind, CachekitError};

/// Convenient glob import for the most common types.
/// Populated as modules are implemented.
pub mod prelude {}
