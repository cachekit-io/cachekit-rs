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

    /// `disable_offline_queue` against a live Redis. The tests spawn tasks, so
    /// they need the `Send` futures `unsync` turns off.
    #[cfg(not(feature = "unsync"))]
    mod offline_queue {
        use cachekit::backend::redis::{RedisBackend, RedisBackendBuilder};
        use cachekit::backend::Backend;
        use cachekit::error::BackendErrorKind;
        use std::net::{SocketAddr, TcpListener, TcpStream};
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use std::sync::{Arc, Mutex};
        use std::time::Duration;

        /// A TCP proxy in front of Redis that a test can cut and restore. Cutting
        /// closes every proxied connection, so the client sees the drop at once.
        struct KillableProxy {
            addr: SocketAddr,
            up: Arc<AtomicBool>,
            /// Connections accepted while cut, each closed at once: the client's
            /// reconnect attempts.
            refused: Arc<AtomicUsize>,
            live: Arc<Mutex<Vec<TcpStream>>>,
        }

        impl KillableProxy {
            fn start(upstream: String) -> Self {
                let listener = TcpListener::bind("127.0.0.1:0").expect("bind proxy");
                let addr = listener.local_addr().expect("proxy addr");
                let up = Arc::new(AtomicBool::new(true));
                let refused = Arc::new(AtomicUsize::new(0));
                let live = Arc::new(Mutex::new(Vec::new()));
                let (is_up, refusals, conns) = (up.clone(), refused.clone(), live.clone());
                std::thread::spawn(move || {
                    for client in listener.incoming() {
                        let Ok(client) = client else { return };
                        if !is_up.load(Ordering::SeqCst) {
                            refusals.fetch_add(1, Ordering::SeqCst);
                            continue; // dropping `client` closes it
                        }
                        let server = TcpStream::connect(upstream.as_str()).expect("reach redis");
                        let pipes = [
                            (
                                client.try_clone().expect("clone"),
                                server.try_clone().expect("clone"),
                            ),
                            (
                                server.try_clone().expect("clone"),
                                client.try_clone().expect("clone"),
                            ),
                        ];
                        conns.lock().expect("lock").extend([client, server]);
                        for (mut from, mut to) in pipes {
                            std::thread::spawn(move || {
                                let _ = std::io::copy(&mut from, &mut to);
                                let _ = to.shutdown(std::net::Shutdown::Both);
                            });
                        }
                    }
                });
                Self {
                    addr,
                    up,
                    refused,
                    live,
                }
            }

            fn cut(&self) {
                self.up.store(false, Ordering::SeqCst);
                for conn in self.live.lock().expect("lock").drain(..) {
                    let _ = conn.shutdown(std::net::Shutdown::Both);
                }
            }

            fn restore(&self) {
                self.up.store(true, Ordering::SeqCst);
            }

            /// `redis_url` with its host and port pointed at this proxy.
            fn url(&self, redis_url: &str) -> String {
                let mut url = url::Url::parse(redis_url).expect("CACHEKIT_TEST_REDIS_URL is a URL");
                url.set_host(Some("127.0.0.1")).expect("set host");
                url.set_port(Some(self.addr.port())).expect("set port");
                url.to_string()
            }
        }

        fn upstream(redis_url: &str) -> String {
            let url = url::Url::parse(redis_url).expect("CACHEKIT_TEST_REDIS_URL is a URL");
            let host = url.host_str().expect("CACHEKIT_TEST_REDIS_URL has a host");
            format!("{host}:{}", url.port().unwrap_or(6379))
        }

        fn unique_prefix(test: &str) -> String {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos();
            format!("cachekit-test:{test}:{nanos}:")
        }

        /// The caller-side budget, like a service that gives the cache 500 ms
        /// before falling back: well under the 5 s command timeout, so a command
        /// fred queued would show up here as a caller timeout.
        const CALLER_BUDGET: Duration = Duration::from_millis(500);

        /// Send one `set` per key concurrently, each under `CALLER_BUDGET`, and
        /// require every one to fail with `Transient` before the caller gives up.
        async fn assert_all_refused(backend: &Arc<RedisBackend>, keys: &[String]) {
            let mut sets = tokio::task::JoinSet::new();
            for key in keys {
                let (backend, key) = (Arc::clone(backend), key.clone());
                sets.spawn(async move {
                    tokio::time::timeout(CALLER_BUDGET, backend.set(&key, vec![7; 1024], None))
                        .await
                });
            }
            while let Some(outcome) = sets.join_next().await {
                let err = outcome
                    .expect("task")
                    .expect("queued: the caller timed out instead of the command failing at once")
                    .expect_err("set must fail while the connection is down");
                assert_eq!(err.kind, BackendErrorKind::Transient, "{err}");
            }
        }

        /// With `auto_reconnect`, commands sent while fred reconnects fail at once
        /// and are never sent after the reconnect, and the backend still
        /// reconnects although commands were in flight when the connection closed.
        #[tokio::test]
        #[allow(clippy::expect_used)] // test-only: failures should panic loudly
        async fn redis_disable_offline_queue_refuses_while_reconnecting() {
            let Ok(url) = std::env::var("CACHEKIT_TEST_REDIS_URL") else {
                eprintln!("skipping redis_disable_offline_queue_refuses_while_reconnecting: CACHEKIT_TEST_REDIS_URL not set");
                return;
            };
            let proxy = KillableProxy::start(upstream(&url));
            let backend = Arc::new(
                RedisBackendBuilder::default()
                    .url(proxy.url(&url))
                    .auto_reconnect()
                    .disable_offline_queue()
                    .build()
                    .expect("builder should succeed"),
            );
            let _handle = backend.connect().await.expect("redis should be reachable");
            let prefix = unique_prefix("offline-reconnect");
            let probe = format!("{prefix}probe");
            backend
                .set(&probe, b"v".to_vec(), None)
                .await
                .expect("set on a healthy connection");

            // Keep commands in flight and queued across the cut.
            let writing = Arc::new(AtomicBool::new(true));
            let writer = tokio::spawn({
                let (backend, writing, prefix) =
                    (Arc::clone(&backend), writing.clone(), prefix.clone());
                async move {
                    let mut i = 0u32;
                    while writing.load(Ordering::SeqCst) {
                        let _ = tokio::time::timeout(
                            CALLER_BUDGET,
                            backend.set(&format!("{prefix}burst:{i}"), vec![1; 1024], None),
                        )
                        .await;
                        i += 1;
                    }
                }
            });
            tokio::time::sleep(Duration::from_millis(50)).await;
            proxy.cut();

            // Send only once fred has seen the drop: it is trying to reconnect.
            tokio::time::timeout(Duration::from_secs(10), async {
                while proxy.refused.load(Ordering::SeqCst) == 0 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("fred never tried to reconnect");
            writing.store(false, Ordering::SeqCst);
            writer.await.expect("writer task");

            let keys: Vec<String> = (0..20).map(|i| format!("{prefix}outage:{i}")).collect();
            assert_all_refused(&backend, &keys).await;

            proxy.restore();
            tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    if let Ok(Ok(Some(_))) =
                        tokio::time::timeout(CALLER_BUDGET, backend.get(&probe)).await
                    {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .expect("backend never reconnected");

            for key in &keys {
                assert!(
                    !backend.exists(key).await.expect("exists after reconnect"),
                    "{key} was sent after the reconnect"
                );
            }
            eprintln!(
                "redis_disable_offline_queue_refuses_while_reconnecting: {} refused at once, 0 sent after the reconnect",
                keys.len()
            );
        }

        /// Without `auto_reconnect`, fred reports the client connected after the
        /// connection closes for good; commands sent after that still fail at
        /// once instead of waiting in a queue nothing reads.
        #[tokio::test]
        #[allow(clippy::expect_used)] // test-only: failures should panic loudly
        async fn redis_disable_offline_queue_refuses_after_close_without_reconnect() {
            let Ok(url) = std::env::var("CACHEKIT_TEST_REDIS_URL") else {
                eprintln!("skipping redis_disable_offline_queue_refuses_after_close_without_reconnect: CACHEKIT_TEST_REDIS_URL not set");
                return;
            };
            let proxy = KillableProxy::start(upstream(&url));
            let backend = Arc::new(
                RedisBackendBuilder::default()
                    .url(proxy.url(&url))
                    .disable_offline_queue()
                    .build()
                    .expect("builder should succeed"),
            );
            let _handle = backend.connect().await.expect("redis should be reachable");
            let prefix = unique_prefix("offline-closed");
            backend
                .set(&format!("{prefix}probe"), b"v".to_vec(), None)
                .await
                .expect("set on a healthy connection");

            proxy.cut();
            // Send only once the backend has seen the drop.
            tokio::time::timeout(Duration::from_secs(10), async {
                while let Ok(Ok(_)) = tokio::time::timeout(CALLER_BUDGET, backend.health()).await {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("backend never saw the drop");

            let keys: Vec<String> = (0..20).map(|i| format!("{prefix}outage:{i}")).collect();
            assert_all_refused(&backend, &keys).await;
            // Nothing reads a queue later: the connection stays down, and
            // commands keep failing at once after Redis is reachable again.
            proxy.restore();
            assert_all_refused(&backend, &keys).await;

            let direct = RedisBackendBuilder::default()
                .url(&url)
                .build()
                .expect("builder should succeed");
            let _direct = direct.connect().await.expect("redis should be reachable");
            for key in &keys {
                assert!(
                    !direct.exists(key).await.expect("exists"),
                    "{key} reached Redis"
                );
            }
            eprintln!(
                "redis_disable_offline_queue_refuses_after_close_without_reconnect: {} refused at once, twice",
                keys.len()
            );
        }

        /// The option refuses only while the connection is down: a burst far
        /// larger than any queue length a caller would pick goes through on a
        /// healthy connection. (A fred `max_command_buffer_len` of N refused most
        /// of a burst of 10×N here, which is why the option is not a length bound.)
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        #[allow(clippy::expect_used)] // test-only: failures should panic loudly
        async fn redis_disable_offline_queue_refuses_nothing_on_a_healthy_connection() {
            let Ok(url) = std::env::var("CACHEKIT_TEST_REDIS_URL") else {
                eprintln!("skipping redis_disable_offline_queue_refuses_nothing_on_a_healthy_connection: CACHEKIT_TEST_REDIS_URL not set");
                return;
            };
            const BURST: usize = 1_000;
            for reconnects in [false, true] {
                let mut builder = RedisBackendBuilder::default()
                    .url(&url)
                    .disable_offline_queue();
                if reconnects {
                    builder = builder.auto_reconnect();
                }
                let backend = Arc::new(builder.build().expect("builder should succeed"));
                let _handle = backend.connect().await.expect("redis should be reachable");
                let prefix = unique_prefix("offline-healthy");

                let mut ops = tokio::task::JoinSet::new();
                for i in 0..BURST {
                    let (backend, key) = (Arc::clone(&backend), format!("{prefix}{i}"));
                    ops.spawn(async move {
                        backend.set(&key, vec![2; 1024], None).await?;
                        backend.get(&key).await
                    });
                }
                while let Some(outcome) = ops.join_next().await {
                    let value = outcome.expect("task").expect("healthy op refused");
                    assert_eq!(value.as_deref(), Some(&[2u8; 1024][..]));
                }
                eprintln!(
                    "redis_disable_offline_queue_refuses_nothing_on_a_healthy_connection: {BURST} set+get, 0 refused (auto_reconnect: {reconnects})"
                );
            }
        }
    }
}
