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
        use cachekit::backend::{Backend, LockableBackend, TtlInspectable};
        use cachekit::error::{BackendError, BackendErrorKind};
        use std::future::Future;
        use std::net::{SocketAddr, TcpListener, TcpStream};
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use std::sync::{Arc, Mutex};
        use std::time::Duration;

        /// A TCP proxy in front of Redis that a test can cut and restore. Cutting
        /// closes every proxied connection, so the client sees the drop at once.
        struct KillableProxy {
            addr: SocketAddr,
            up: Arc<AtomicBool>,
            /// While set, bytes from the client are dropped: commands are
            /// written but never reach Redis, and no reply comes back.
            stalled: Arc<AtomicBool>,
            /// Connections accepted while cut, each closed at once: the client's
            /// reconnect attempts.
            refused: Arc<AtomicUsize>,
            live: Arc<Mutex<Vec<TcpStream>>>,
        }

        /// Copy `from` to `to` until either closes, dropping what `from` sends
        /// while `drop_while` is set.
        fn pipe(mut from: TcpStream, mut to: TcpStream, drop_while: Option<&AtomicBool>) {
            use std::io::{Read, Write};

            let mut buf = [0u8; 16 * 1024];
            while let Ok(n @ 1..) = from.read(&mut buf) {
                if drop_while
                    .as_ref()
                    .is_some_and(|d| d.load(Ordering::SeqCst))
                {
                    continue;
                }
                if to.write_all(&buf[..n]).is_err() {
                    break;
                }
            }
            let _ = to.shutdown(std::net::Shutdown::Both);
        }

        impl KillableProxy {
            fn start(upstream: String) -> Self {
                let listener = TcpListener::bind("127.0.0.1:0").expect("bind proxy");
                let addr = listener.local_addr().expect("proxy addr");
                let up = Arc::new(AtomicBool::new(true));
                let stalled = Arc::new(AtomicBool::new(false));
                let refused = Arc::new(AtomicUsize::new(0));
                let live = Arc::new(Mutex::new(Vec::new()));
                let (is_up, is_stalled, refusals, conns) =
                    (up.clone(), stalled.clone(), refused.clone(), live.clone());
                std::thread::spawn(move || {
                    for client in listener.incoming() {
                        let Ok(client) = client else { return };
                        if !is_up.load(Ordering::SeqCst) {
                            refusals.fetch_add(1, Ordering::SeqCst);
                            continue; // dropping `client` closes it
                        }
                        let server = TcpStream::connect(upstream.as_str()).expect("reach redis");
                        let (c, s) = (
                            client.try_clone().expect("clone"),
                            server.try_clone().expect("clone"),
                        );
                        let to_redis = is_stalled.clone();
                        std::thread::spawn(move || pipe(c, s, Some(&to_redis)));
                        let (s, c) = (
                            server.try_clone().expect("clone"),
                            client.try_clone().expect("clone"),
                        );
                        std::thread::spawn(move || pipe(s, c, None));
                        conns.lock().expect("lock").extend([client, server]);
                    }
                });
                Self {
                    addr,
                    up,
                    stalled,
                    refused,
                    live,
                }
            }

            fn stall(&self) {
                self.stalled.store(true, Ordering::SeqCst);
            }

            fn cut(&self) {
                self.up.store(false, Ordering::SeqCst);
                for conn in self.live.lock().expect("lock").drain(..) {
                    let _ = conn.shutdown(std::net::Shutdown::Both);
                }
            }

            fn restore(&self) {
                self.stalled.store(false, Ordering::SeqCst);
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

        /// Keys these tests write expire, so a run leaves nothing behind.
        const TTL: Option<Duration> = Some(Duration::from_secs(60));

        /// Require `op` to fail with `Transient` before the caller gives up.
        async fn assert_refused<T: std::fmt::Debug>(
            what: &str,
            op: impl Future<Output = Result<T, BackendError>>,
        ) {
            let err = tokio::time::timeout(CALLER_BUDGET, op)
                .await
                .unwrap_or_else(|_| panic!("{what} queued: the caller timed out first"))
                .expect_err("must fail while the connection is down");
            assert_eq!(err.kind, BackendErrorKind::Transient, "{what}: {err}");
        }

        /// Send one `set` per key concurrently and require every one refused.
        async fn assert_sets_refused(backend: &Arc<RedisBackend>, keys: &[String]) {
            let mut sets = tokio::task::JoinSet::new();
            for key in keys {
                let (backend, key) = (Arc::clone(backend), key.clone());
                sets.spawn(async move {
                    assert_refused(&key, backend.set(&key, vec![7; 1024], TTL)).await;
                });
            }
            while let Some(done) = sets.join_next().await {
                done.expect("a set was not refused");
            }
        }

        /// Every backend operation is refused, not only `set`.
        async fn assert_every_op_refused(backend: &RedisBackend, key: &str) {
            assert_refused("get", backend.get(key)).await;
            assert_refused("delete", backend.delete(key)).await;
            assert_refused("exists", backend.exists(key)).await;
            assert_refused("ttl", backend.ttl(key)).await;
            assert_refused("health", backend.health()).await;
            assert_refused("acquire_lock", backend.acquire_lock(key, 1_000)).await;
            assert_refused("release_lock", backend.release_lock(key, "token")).await;
        }

        /// Poll until `get` succeeds again: the backend has reconnected.
        async fn wait_until_served(backend: &RedisBackend, key: &str) {
            tokio::time::timeout(Duration::from_secs(30), async {
                while !matches!(
                    tokio::time::timeout(CALLER_BUDGET, backend.get(key)).await,
                    Ok(Ok(Some(_)))
                ) {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .expect("backend never reconnected");
        }

        fn keys(prefix: &str, name: &str) -> Vec<String> {
            (0..20).map(|i| format!("{prefix}{name}:{i}")).collect()
        }

        /// fred notices a closed connection at once, but its client state
        /// stays `Connected` until its first reconnect attempt, 100 to 150 ms
        /// later. Long enough for the backend to see the drop, well inside
        /// that window.
        const SEE_THE_DROP: Duration = Duration::from_millis(20);

        /// With `auto_reconnect`, commands fail at once from the drop until the
        /// reconnect, both inside fred's first reconnect backoff and while it
        /// retries, and none is sent after the reconnect. The backend still
        /// reconnects although writes were in flight when the connection closed.
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
                .set(&probe, b"v".to_vec(), TTL)
                .await
                .expect("set on a healthy connection");

            // Keep writes in flight across the cut.
            let writing = Arc::new(AtomicBool::new(true));
            let writer = tokio::spawn({
                let (backend, writing, prefix) =
                    (Arc::clone(&backend), writing.clone(), prefix.clone());
                async move {
                    let mut i = 0u32;
                    while writing.load(Ordering::SeqCst) {
                        let key = format!("{prefix}burst:{i}");
                        let _ = tokio::time::timeout(
                            CALLER_BUDGET,
                            backend.set(&key, vec![1; 1024], TTL),
                        )
                        .await;
                        // A refused set returns without yielding.
                        tokio::time::sleep(Duration::from_millis(1)).await;
                        i += 1;
                    }
                }
            });
            tokio::time::sleep(Duration::from_millis(50)).await;
            proxy.cut();

            // Inside fred's first reconnect backoff.
            tokio::time::sleep(SEE_THE_DROP).await;
            let attempts_before_backoff_burst = proxy.refused.load(Ordering::SeqCst);
            let in_backoff = keys(&prefix, "backoff");
            assert_sets_refused(&backend, &in_backoff).await;
            assert_every_op_refused(&backend, &probe).await;

            // While fred retries.
            tokio::time::timeout(Duration::from_secs(10), async {
                while proxy.refused.load(Ordering::SeqCst) == 0 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("fred never tried to reconnect");
            writing.store(false, Ordering::SeqCst);
            writer.await.expect("writer task");
            let while_retrying = keys(&prefix, "retrying");
            assert_sets_refused(&backend, &while_retrying).await;

            proxy.restore();
            wait_until_served(&backend, &probe).await;
            for key in in_backoff.iter().chain(&while_retrying) {
                assert!(
                    !backend.exists(key).await.expect("exists after reconnect"),
                    "{key} was sent after the reconnect"
                );
            }
            eprintln!(
                "redis_disable_offline_queue_refuses_while_reconnecting: {} refused at once (first burst after {attempts_before_backoff_burst} reconnect attempts), 0 sent after the reconnect",
                in_backoff.len() + while_retrying.len()
            );
        }

        /// Writes already on the connection when it drops, whose callers gave
        /// up, fail with it instead of being sent after the reconnect.
        #[tokio::test]
        #[allow(clippy::expect_used)] // test-only: failures should panic loudly
        async fn redis_disable_offline_queue_drops_writes_in_flight_at_the_drop() {
            let Ok(url) = std::env::var("CACHEKIT_TEST_REDIS_URL") else {
                eprintln!("skipping redis_disable_offline_queue_drops_writes_in_flight_at_the_drop: CACHEKIT_TEST_REDIS_URL not set");
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
            let prefix = unique_prefix("offline-in-flight");
            let probe = format!("{prefix}probe");
            backend
                .set(&probe, b"v".to_vec(), TTL)
                .await
                .expect("set on a healthy connection");

            // Written to the connection, never answered; each caller gives up.
            proxy.stall();
            let in_flight = keys(&prefix, "in-flight");
            let mut sets = tokio::task::JoinSet::new();
            for key in &in_flight {
                let (backend, key) = (Arc::clone(&backend), key.clone());
                sets.spawn(async move {
                    let budget = Duration::from_millis(200);
                    tokio::time::timeout(budget, backend.set(&key, vec![3; 1024], TTL)).await
                });
            }
            while let Some(outcome) = sets.join_next().await {
                outcome
                    .expect("task")
                    .expect_err("a stalled set cannot complete");
            }

            proxy.cut();
            tokio::time::sleep(SEE_THE_DROP).await;
            proxy.restore();
            wait_until_served(&backend, &probe).await;
            for key in &in_flight {
                assert!(
                    !backend.exists(key).await.expect("exists after reconnect"),
                    "{key} was sent after the reconnect"
                );
            }
            eprintln!(
                "redis_disable_offline_queue_drops_writes_in_flight_at_the_drop: {} abandoned in flight, 0 sent after the reconnect",
                in_flight.len()
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
            let probe = format!("{prefix}probe");
            backend
                .set(&probe, b"v".to_vec(), TTL)
                .await
                .expect("set on a healthy connection");

            proxy.cut();
            tokio::time::sleep(SEE_THE_DROP).await;
            let outage = keys(&prefix, "outage");
            assert_sets_refused(&backend, &outage).await;
            assert_every_op_refused(&backend, &probe).await;
            // Nothing reads a queue later: the connection stays down, and
            // commands keep failing at once after Redis is reachable again.
            proxy.restore();
            assert_sets_refused(&backend, &outage).await;

            let direct = RedisBackendBuilder::default()
                .url(&url)
                .build()
                .expect("builder should succeed");
            let _direct = direct.connect().await.expect("redis should be reachable");
            for key in &outage {
                assert!(
                    !direct.exists(key).await.expect("exists"),
                    "{key} reached Redis"
                );
            }
            eprintln!(
                "redis_disable_offline_queue_refuses_after_close_without_reconnect: {} refused at once, twice",
                outage.len()
            );
        }

        /// A failed connect can be retried; a backend that connected cannot
        /// connect again.
        #[tokio::test]
        #[allow(clippy::expect_used)] // test-only: failures should panic loudly
        async fn redis_disable_offline_queue_connects_once() {
            let Ok(url) = std::env::var("CACHEKIT_TEST_REDIS_URL") else {
                eprintln!("skipping redis_disable_offline_queue_connects_once: CACHEKIT_TEST_REDIS_URL not set");
                return;
            };
            let proxy = KillableProxy::start(upstream(&url));
            let backend = RedisBackendBuilder::default()
                .url(proxy.url(&url))
                .auto_reconnect()
                .disable_offline_queue()
                .build()
                .expect("builder should succeed");

            proxy.cut();
            backend.connect().await.expect_err("redis is unreachable");
            proxy.restore();
            let _handle = backend
                .connect()
                .await
                .expect("a failed connect can be retried");
            backend.health().await.expect("connected");
            let err = backend
                .connect()
                .await
                .expect_err("second connect is refused");
            assert_eq!(err.kind, BackendErrorKind::Permanent, "{err}");
            backend.health().await.expect("still connected");
            eprintln!("redis_disable_offline_queue_connects_once: retried after a failure, refused a second connect");
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
                        backend.set(&key, vec![2; 1024], TTL).await?;
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
