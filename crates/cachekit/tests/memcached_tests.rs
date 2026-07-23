#[cfg(feature = "memcached")]
mod memcached_tests {
    use std::time::Duration;

    use cachekit::backend::memcached::MemcachedBackend;
    use cachekit::backend::Backend;

    #[tokio::test]
    async fn builder_missing_url() {
        let result = MemcachedBackend::builder().connect().await;
        assert!(result.is_err(), "expected error when no URL is provided");

        let err = result.err().map(|e| e.to_string()).unwrap_or_default();
        assert!(err.contains("url is required"), "unexpected error: {err}");
    }

    /// Live semantics against a real memcached.
    ///
    /// Requires `CACHEKIT_TEST_MEMCACHED_URL` (e.g. `tcp://127.0.0.1:11211`);
    /// skips with a notice otherwise so the default CI suite stays green
    /// without a memcached service.
    #[tokio::test]
    #[allow(clippy::expect_used)] // test-only: failures should panic loudly
    async fn memcached_live_semantics() {
        let Ok(url) = std::env::var("CACHEKIT_TEST_MEMCACHED_URL") else {
            eprintln!("skipping memcached_live_semantics: CACHEKIT_TEST_MEMCACHED_URL not set");
            return;
        };

        let backend = MemcachedBackend::builder()
            .url(&url)
            .connect()
            .await
            .expect("memcached should be reachable");

        // Unique per run so parallel/repeated runs never contend.
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let key = format!("cachekit-test:memcached:{nanos}");

        // Miss → None / false, not an error.
        assert_eq!(backend.get(&key).await.expect("get"), None);
        assert!(!backend.exists(&key).await.expect("exists"));
        assert!(!backend.delete(&key).await.expect("delete miss"));

        // Round-trip with no TTL.
        backend
            .set(&key, b"payload".to_vec(), None)
            .await
            .expect("set");
        assert_eq!(
            backend.get(&key).await.expect("get").as_deref(),
            Some(b"payload".as_slice())
        );
        assert!(backend.exists(&key).await.expect("exists"));

        // Overwrite.
        backend
            .set(&key, b"payload2".to_vec(), None)
            .await
            .expect("set");
        assert_eq!(
            backend.get(&key).await.expect("get").as_deref(),
            Some(b"payload2".as_slice())
        );

        // Delete → true once, false after.
        assert!(backend.delete(&key).await.expect("delete"));
        assert!(!backend.delete(&key).await.expect("second delete"));
        assert_eq!(backend.get(&key).await.expect("get"), None);

        // TTL expiry end-to-end (memcached rounds to whole seconds).
        backend
            .set(&key, b"short".to_vec(), Some(Duration::from_secs(1)))
            .await
            .expect("set with ttl");
        assert!(backend.exists(&key).await.expect("exists"));
        tokio::time::sleep(Duration::from_millis(2100)).await;
        assert_eq!(
            backend.get(&key).await.expect("get after expiry"),
            None,
            "entry must expire"
        );

        // Oversized values fail loudly client-side (default 1 MiB guard).
        let oversized = vec![0u8; 2 * 1024 * 1024];
        let err = backend
            .set(&key, oversized, None)
            .await
            .expect_err("oversized value must be rejected");
        assert!(
            err.to_string().contains("max item size"),
            "unexpected error: {err}"
        );

        // Health reports the server version.
        let status = backend.health().await.expect("health");
        assert!(status.is_healthy);
        assert_eq!(status.backend_type, "memcached");
        assert!(
            status.details.contains_key("version"),
            "details: {:?}",
            status.details
        );
    }
}
