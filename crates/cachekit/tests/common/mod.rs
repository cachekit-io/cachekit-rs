//! Shared test utilities for the cachekit integration tests.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Mutex;

use cachekit::backend::{Backend, HealthStatus};
use cachekit::error::BackendError;

/// In-memory mock backend backed by a `Mutex<HashMap>` for use in tests.
#[derive(Debug, Default, Clone)]
pub struct MockBackend {
    pub store: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl MockBackend {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

#[async_trait]
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
