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

    #[tokio::test]
    async fn connect_is_eager_and_fails_fast_on_bad_address() {
        // connect() promises a live server — a dead port must error here,
        // not on the first cache operation.
        let result = MemcachedBackend::builder()
            .url("tcp://127.0.0.1:1") // reserved port, nothing listens
            .connect_timeout(Duration::from_millis(200))
            .connect()
            .await;
        assert!(result.is_err(), "connect to a dead port must fail eagerly");
    }

    #[tokio::test]
    async fn wedged_server_fails_fast_instead_of_hanging() {
        // Panel round 2 (#5): an accepting-but-silent server must surface as
        // a bounded error, never wedge callers. The listener accepts TCP
        // connects and never writes a byte; connect()'s eager version ping
        // must fail via the per-connection socket timeout pinned on the URL,
        // inside the async connect budget.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        std::thread::spawn(move || {
            let mut held = Vec::new();
            while let Ok((sock, _)) = listener.accept() {
                held.push(sock); // hold open, stay silent
            }
        });

        let started = std::time::Instant::now();
        let result = MemcachedBackend::builder()
            .url(format!("tcp://{addr}"))
            .timeout(Duration::from_millis(300))
            .connect_timeout(Duration::from_millis(300))
            .connect()
            .await;
        let elapsed = started.elapsed();

        assert!(result.is_err(), "a silent server must not connect()");
        assert!(
            elapsed < Duration::from_secs(15),
            "wedged server must fail fast, took {elapsed:?}"
        );
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

        // refresh_ttl (bare touch wrapper, py parity): extends a live key,
        // reports a missing key as false.
        backend
            .set(&key, b"touched".to_vec(), Some(Duration::from_secs(1)))
            .await
            .expect("set");
        assert!(backend
            .refresh_ttl(&key, Some(Duration::from_secs(60)))
            .await
            .expect("refresh"));
        tokio::time::sleep(Duration::from_millis(2100)).await;
        assert_eq!(
            backend.get(&key).await.expect("get").as_deref(),
            Some(b"touched".as_slice()),
            "touched entry must outlive its original 1s TTL"
        );
        assert!(backend.delete(&key).await.expect("cleanup"));
        assert!(!backend
            .refresh_ttl(&key, Some(Duration::from_secs(60)))
            .await
            .expect("refresh missing"));

        // Key injection guard (CWE-93): protocol metacharacters are rejected
        // client-side, never sent on the wire.
        for bad in ["evil\r\nflush_all\r\n", "evil key", "evil\nkey", ""] {
            let err = backend
                .get(bad)
                .await
                .expect_err("protocol-unsafe key must be rejected");
            assert!(
                !err.kind.is_retryable(),
                "key rejection must be permanent: {err}"
            );
        }
        // The guard must not have executed an injected flush_all: a canary
        // written before the attempts is still present.
        backend
            .set(&key, b"canary".to_vec(), None)
            .await
            .expect("set canary");
        let _ = backend.get("evil\r\nflush_all\r\n").await;
        assert_eq!(
            backend.get(&key).await.expect("get").as_deref(),
            Some(b"canary".as_slice()),
            "cache must survive an injection attempt"
        );
        backend.delete(&key).await.expect("cleanup");

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
