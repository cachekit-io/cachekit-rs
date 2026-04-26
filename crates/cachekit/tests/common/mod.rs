//! Shared test utilities for the cachekit integration tests.

#![allow(dead_code)]

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Mutex;

use cachekit::backend::{Backend, HealthStatus};
use cachekit::client::SharedBackend;
use cachekit::error::BackendError;

/// In-memory mock backend backed by a `Mutex<HashMap>` for use in tests.
///
/// The internal store uses `Arc<Mutex>` so cloning the backend shares state,
/// regardless of whether the outer wrapper is `Arc` or `Rc`.
#[derive(Debug, Default, Clone)]
pub struct MockBackend {
    pub store: std::sync::Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl MockBackend {
    /// Create a new mock and immediately wrap it as a [`SharedBackend`].
    ///
    /// Use [`MockBackend::new_with_handle`] when tests need to inspect the
    /// store directly (e.g., to verify ciphertext).
    pub fn shared() -> SharedBackend {
        let mock = Self::default();
        Self::into_shared(mock)
    }

    /// Create a new mock, returning both a [`SharedBackend`] and a clone
    /// for direct store inspection.
    pub fn new_with_handle() -> (SharedBackend, Self) {
        let mock = Self::default();
        let handle = mock.clone();
        (Self::into_shared(mock), handle)
    }

    fn into_shared(mock: Self) -> SharedBackend {
        #[cfg(not(any(target_arch = "wasm32", feature = "unsync")))]
        {
            std::sync::Arc::new(mock)
        }
        #[cfg(any(target_arch = "wasm32", feature = "unsync"))]
        {
            std::rc::Rc::new(mock)
        }
    }
}

#[cfg_attr(not(any(target_arch = "wasm32", feature = "unsync")), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", feature = "unsync"), async_trait(?Send))]
impl Backend for MockBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        Ok(self.store.lock().await.get(key).cloned())
    }

    async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        _ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        self.store.lock().await.insert(key.to_owned(), value);
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<bool, BackendError> {
        Ok(self.store.lock().await.remove(key).is_some())
    }

    async fn exists(&self, key: &str) -> Result<bool, BackendError> {
        Ok(self.store.lock().await.contains_key(key))
    }

    async fn health(&self) -> Result<HealthStatus, BackendError> {
        Ok(HealthStatus {
            is_healthy: true,
            latency_ms: 0.0,
            backend_type: "mock".to_owned(),
            details: HashMap::new(),
        })
    }
}
