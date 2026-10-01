#[cfg(feature = "redis")]
mod redis_tests {
    use cachekit::backend::redis::RedisBackendBuilder;
    use cachekit::backend::{Backend, LockableBackend};

    #[test]
    fn redis_builder_missing_url() {
        let result = RedisBackendBuilder::default().build();
        assert!(result.is_err(), "expected error when no URL is provided");

        let err = result.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("url is required"),
            "unexpected error message: {msg}"
        );
    }

    #[test]
    fn redis_builder_from_url() {
        // Connection is lazy — building succeeds even if Redis is not running.
        let result = RedisBackendBuilder::default()
            .url("redis://127.0.0.1:6379")
            .build();
        assert!(
            result.is_ok(),
            "expected Ok when a valid URL is provided, got: {result:?}"
        );
    }

    /// Live distributed-lock semantics against a real Redis.
    ///
    /// Requires `CACHEKIT_TEST_REDIS_URL` (e.g. `redis://127.0.0.1:6379`);
    /// skips with a notice otherwise so the default CI suite stays green
    /// without a Redis service.
    #[tokio::test]
    #[allow(clippy::expect_used)] // test-only: failures should panic loudly
    async fn redis_lock_live_semantics() {
        let Ok(url) = std::env::var("CACHEKIT_TEST_REDIS_URL") else {
            eprintln!("skipping redis_lock_live_semantics: CACHEKIT_TEST_REDIS_URL not set");
            return;
        };

        let backend = RedisBackendBuilder::default()
            .url(&url)
            .build()
            .expect("builder should succeed");
        let _handle = backend.connect().await.expect("redis should be reachable");

        // Unique per run so parallel/repeated runs never contend.
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let key = format!("cachekit-test:lock:{nanos}");

        // Free lock → acquired, returns a token.
        let lock_id = backend
            .acquire_lock(&key, 30_000)
            .await
            .expect("acquire should not error")
            .expect("first acquire should win");

        // Bare-key contract: the token is stored under `<key>:lock` on the
        // wire, byte-identical to cachekit-py's Redis lock namespace.
        let raw = backend
            .get(&format!("{key}:lock"))
            .await
            .expect("get should not error")
            .expect("lock key should exist on the wire");
        assert_eq!(raw, lock_id.as_bytes());

        // Held lock → contested acquire returns None, not an error.
        let contested = backend
            .acquire_lock(&key, 30_000)
            .await
            .expect("contested acquire should not error");
        assert!(contested.is_none(), "contested acquire must return None");

        // Wrong token → compare-and-delete refuses, lock still held.
        let released = backend
            .release_lock(&key, "not-the-token")
            .await
            .expect("release should not error");
        assert!(!released, "release with a foreign token must be a no-op");

        // Right token → released.
        let released = backend
            .release_lock(&key, &lock_id)
            .await
            .expect("release should not error");
        assert!(released, "holder release must succeed");

        // Double release → false (already gone), not an error.
        let released = backend
            .release_lock(&key, &lock_id)
            .await
            .expect("release should not error");
        assert!(!released, "double release must return false");

        // Lock is reacquirable after release.
        let reacquired = backend
            .acquire_lock(&key, 100)
            .await
            .expect("acquire should not error");
        assert!(reacquired.is_some(), "lock must be free after release");

        // timeout_ms is the lock TTL: after expiry the lock self-releases.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let after_expiry = backend
            .acquire_lock(&key, 30_000)
            .await
            .expect("acquire should not error")
            .expect("lock must self-release after timeout_ms");

        // Cleanup.
        backend
            .release_lock(&key, &after_expiry)
            .await
            .expect("cleanup release should not error");
    }

    /// A server that completes fred's connection handshake, then never answers
    /// again: no reply, no RST. This is the black-hole case, where an unbounded
    /// client waits until TCP gives up (minutes).
    fn spawn_silent_redis() -> std::net::SocketAddr {
        use std::io::{Read, Write};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind stub");
        let addr = listener.local_addr().expect("stub addr");
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                std::thread::spawn(move || {
                    let mut buf = [0u8; 4096];
                    // fred's handshake is sequential request/response (PING,
                    // CLIENT ID, INFO), so each read holds one command.
                    while let Ok(n) = stream.read(&mut buf) {
                        if n == 0 {
                            return;
                        }
                        let cmd = String::from_utf8_lossy(&buf[..n]).to_ascii_uppercase();
                        let reply: &[u8] = if cmd.contains("PING") {
                            b"+PONG\r\n"
                        } else if cmd.contains("CLIENT") || cmd.contains("INFO") {
                            b"-ERR stub\r\n"
                        } else {
                            continue; // every data command: read, never reply
                        };
                        if stream.write_all(reply).is_err() {
                            return;
                        }
                    }
                });
            }
        });
        addr
    }

    /// Without a command timeout these ops hang; the outer 30 s timeout makes
    /// that a fast failure instead of a stuck test run.
    #[tokio::test]
    #[allow(clippy::expect_used)] // test-only: failures should panic loudly
    async fn redis_ops_time_out_against_unresponsive_server() {
        use cachekit::error::BackendErrorKind;
        use std::time::{Duration, Instant};

        let addr = spawn_silent_redis();
        let backend = RedisBackendBuilder::default()
            .url(format!("redis://{addr}"))
            .build()
            .expect("builder should succeed");

        tokio::time::timeout(Duration::from_secs(30), async {
            let _handle = backend
                .connect()
                .await
                .expect("stub handshake should succeed");

            let start = Instant::now();
            let (get, set, lock) = tokio::join!(
                backend.get("k"),
                backend.set("k", b"v".to_vec(), None),
                backend.acquire_lock("k", 1_000),
            );
            let elapsed = start.elapsed();

            assert_eq!(
                get.expect_err("get must time out").kind,
                BackendErrorKind::Timeout
            );
            assert_eq!(
                set.expect_err("set must time out").kind,
                BackendErrorKind::Timeout
            );
            assert_eq!(
                lock.expect_err("acquire_lock must time out").kind,
                BackendErrorKind::Timeout
            );
            assert!(elapsed < Duration::from_secs(6), "ops took {elapsed:?}");
        })
        .await
        .expect("ops hung: no command timeout on the fred client");
    }
}
