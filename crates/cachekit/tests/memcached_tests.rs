#[cfg(feature = "memcached")]
mod memcached_tests {
    use std::time::Duration;

    use cachekit::backend::memcached::MemcachedBackend;
    use cachekit::backend::Backend;
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn builder_missing_url() {
        let result = MemcachedBackend::builder().connect().await;
        assert!(result.is_err(), "expected error when no URL is provided");

        let err = result.err().map(|e| e.to_string()).unwrap_or_default();
        assert!(err.contains("url is required"), "unexpected error: {err}");
    }

    #[tokio::test]
    #[serial]
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
    #[serial]
    async fn wedged_server_fails_fast_instead_of_hanging() {
        // An accepting-but-silent server must surface as
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

    /// In-process memcached ASCII stub: counts commands, and can answer a
    /// chosen `get` late or as a slow trickle, the shapes that leave a
    /// response on the wire after the client gave up on it.
    mod stub {
        use std::collections::HashMap;
        use std::io::{BufRead, BufReader, Read, Write};
        use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::{Arc, Mutex};
        use std::time::Duration;

        /// How a stalled `get` reply is delivered.
        #[derive(Clone, Copy)]
        pub enum Stall {
            /// The whole reply after `delay`.
            Late { delay: Duration },
            /// `bytes` bytes one per `drip`, then `pause`, then the rest.
            Trickle {
                bytes: usize,
                drip: Duration,
                pause: Duration,
            },
        }

        #[derive(Default)]
        pub struct Counts {
            pub conns: AtomicU64,
            pub version: AtomicU64,
            pub get: AtomicU64,
            pub set: AtomicU64,
            pub delete: AtomicU64,
            pub touch: AtomicU64,
            pub stalled: AtomicU64,
        }

        pub struct Stub {
            pub addr: SocketAddr,
            pub counts: Arc<Counts>,
            /// Gets 1-based numbers in this list are stalled.
            stall_gets: Arc<Mutex<Vec<u64>>>,
            reply_delay_ms: Arc<AtomicU64>,
            live: Arc<Mutex<Vec<TcpStream>>>,
        }

        impl Stub {
            pub fn start(stall: Option<Stall>, stall_gets: &[u64]) -> Self {
                let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
                let stub = Stub {
                    addr: listener.local_addr().expect("addr"),
                    counts: Arc::default(),
                    stall_gets: Arc::new(Mutex::new(stall_gets.to_vec())),
                    reply_delay_ms: Arc::default(),
                    live: Arc::default(),
                };
                let (counts, stall_gets, delay, live) = (
                    Arc::clone(&stub.counts),
                    Arc::clone(&stub.stall_gets),
                    Arc::clone(&stub.reply_delay_ms),
                    Arc::clone(&stub.live),
                );
                let store = Arc::new(Mutex::new(HashMap::<String, Vec<u8>>::new()));
                std::thread::spawn(move || {
                    for sock in listener.incoming().flatten() {
                        counts.conns.fetch_add(1, Ordering::SeqCst);
                        live.lock()
                            .expect("live")
                            .push(sock.try_clone().expect("clone"));
                        let (c, sg, d, st) = (
                            Arc::clone(&counts),
                            Arc::clone(&stall_gets),
                            Arc::clone(&delay),
                            Arc::clone(&store),
                        );
                        std::thread::spawn(move || {
                            let _ = serve(sock, &c, &sg, &d, &st, stall);
                        });
                    }
                });
                stub
            }

            pub fn url(&self) -> String {
                format!("tcp://{}", self.addr)
            }

            pub fn count(&self, field: fn(&Counts) -> &AtomicU64) -> u64 {
                field(&self.counts).load(Ordering::SeqCst)
            }

            /// Delay every reply, so concurrent callers each need a connection.
            pub fn set_reply_delay(&self, delay: Duration) {
                let ms = u64::try_from(delay.as_millis()).expect("ms");
                self.reply_delay_ms.store(ms, Ordering::SeqCst);
            }

            /// Send FIN on every server-side socket, as a restarting server
            /// does. Only the write half closes, so the client's next request
            /// draws no RST and its read sees EOF, as it does over a real
            /// network, where the read runs before any RST can return.
            pub fn restart(&self) {
                for sock in self.live.lock().expect("live").drain(..) {
                    let _ = sock.shutdown(Shutdown::Write);
                }
            }
        }

        fn serve(
            sock: TcpStream,
            c: &Counts,
            stall_gets: &Mutex<Vec<u64>>,
            delay_ms: &AtomicU64,
            store: &Mutex<HashMap<String, Vec<u8>>>,
            stall: Option<Stall>,
        ) -> std::io::Result<()> {
            let mut w = sock.try_clone()?;
            let mut r = BufReader::new(sock);
            let mut line = String::new();
            loop {
                line.clear();
                if r.read_line(&mut line)? == 0 {
                    return Ok(());
                }
                let parts: Vec<String> = line.split_whitespace().map(str::to_owned).collect();
                let Some(cmd) = parts.first() else { continue };
                let mut stalled = false;
                let out: Vec<u8> = match cmd.as_str() {
                    "version" => {
                        c.version.fetch_add(1, Ordering::SeqCst);
                        b"VERSION 1.6.99-stub\r\n".to_vec()
                    }
                    "get" => {
                        let n = c.get.fetch_add(1, Ordering::SeqCst) + 1;
                        stalled = stall_gets.lock().expect("stall").contains(&n);
                        let mut out = Vec::new();
                        if let Some(v) = store.lock().expect("store").get(&parts[1]) {
                            out.extend(format!("VALUE {} 0 {}\r\n", parts[1], v.len()).bytes());
                            out.extend(v);
                            out.extend(b"\r\n");
                        }
                        out.extend(b"END\r\n");
                        out
                    }
                    "set" => {
                        c.set.fetch_add(1, Ordering::SeqCst);
                        let len: usize = parts[4].parse().expect("len");
                        let mut data = vec![0u8; len + 2];
                        r.read_exact(&mut data)?;
                        data.truncate(len);
                        store.lock().expect("store").insert(parts[1].clone(), data);
                        b"STORED\r\n".to_vec()
                    }
                    "delete" => {
                        c.delete.fetch_add(1, Ordering::SeqCst);
                        let gone = store.lock().expect("store").remove(&parts[1]).is_some();
                        if gone {
                            b"DELETED\r\n".to_vec()
                        } else {
                            b"NOT_FOUND\r\n".to_vec()
                        }
                    }
                    "touch" => {
                        c.touch.fetch_add(1, Ordering::SeqCst);
                        let hit = store.lock().expect("store").contains_key(&parts[1]);
                        if hit {
                            b"TOUCHED\r\n".to_vec()
                        } else {
                            b"NOT_FOUND\r\n".to_vec()
                        }
                    }
                    _ => b"ERROR\r\n".to_vec(),
                };
                std::thread::sleep(Duration::from_millis(delay_ms.load(Ordering::SeqCst)));
                match (stalled, stall) {
                    (true, Some(Stall::Late { delay })) => {
                        c.stalled.fetch_add(1, Ordering::SeqCst);
                        std::thread::sleep(delay);
                        w.write_all(&out)?;
                    }
                    (true, Some(Stall::Trickle { bytes, drip, pause })) => {
                        c.stalled.fetch_add(1, Ordering::SeqCst);
                        let split = bytes.min(out.len());
                        for b in &out[..split] {
                            w.write_all(std::slice::from_ref(b))?;
                            std::thread::sleep(drip);
                        }
                        std::thread::sleep(pause);
                        w.write_all(&out[split..])?;
                    }
                    _ => w.write_all(&out)?,
                }
            }
        }
    }

    use cachekit::error::BackendErrorKind;
    use stub::{Stall, Stub};

    /// Keys whose values all have the same length, so a response read by the
    /// wrong request parses cleanly and only the key check can catch it.
    fn seed_keys(n: usize) -> Vec<(String, Vec<u8>)> {
        (0..n)
            .map(|i| (format!("k{i:02}"), format!("value-{i:02}").into_bytes()))
            .collect()
    }

    async fn stub_backend(stub: &Stub, timeout: Duration, pool_size: u32) -> MemcachedBackend {
        MemcachedBackend::builder()
            .url(stub.url())
            .timeout(timeout)
            .connect_timeout(timeout)
            .pool_size(pool_size)
            .connect()
            .await
            .expect("connect to stub")
    }

    /// Drive gets over seeded keys and hold the desync invariant: every `Ok`
    /// is that key's own value, and every error is retryable (a timeout, or a
    /// slow reconnect on a loaded runner). A response read by the wrong
    /// request shows up as a wrong value, a wrong miss, or a `BadResponse`
    /// ("key doesn't match"), which is Permanent.
    async fn assert_no_desync(
        backend: &MemcachedBackend,
        keys: &[(String, Vec<u8>)],
        rounds: usize,
    ) -> (usize, usize) {
        let (mut ok, mut errors) = (0, 0);
        for i in 0..rounds {
            let (key, want) = &keys[i % keys.len()];
            match backend.get(key).await {
                Ok(got) => {
                    assert_eq!(
                        got.as_ref(),
                        Some(want),
                        "get({key}) #{i} returned another response"
                    );
                    ok += 1;
                }
                Err(e) => {
                    assert!(
                        matches!(
                            e.kind,
                            BackendErrorKind::Timeout | BackendErrorKind::Transient
                        ),
                        "get({key}) #{i}: {e}"
                    );
                    errors += 1;
                }
            }
        }
        (ok, errors)
    }

    /// r2d2 worker threads alive in this process (Linux: thread names).
    #[cfg(target_os = "linux")]
    fn r2d2_threads() -> usize {
        std::fs::read_dir("/proc/self/task")
            .expect("task dir")
            .flatten()
            .filter(|t| {
                std::fs::read_to_string(t.path().join("comm"))
                    .is_ok_and(|name| name.starts_with("r2d2-worker"))
            })
            .count()
    }

    #[tokio::test]
    #[serial]
    async fn ops_send_no_version_command() {
        // The only `version` is connect()'s eager reachability ping; no op
        // pays a checkout ping (it used to cost one round trip per op).
        let stub = Stub::start(None, &[]);
        let backend = stub_backend(&stub, Duration::from_secs(1), 4).await;
        assert_eq!(stub.count(|c| &c.version), 1, "connect() pings once");

        for i in 0..25 {
            let key = format!("k{i}");
            backend.set(&key, b"v".to_vec(), None).await.expect("set");
            assert_eq!(
                backend.get(&key).await.expect("get").as_deref(),
                Some(b"v".as_slice())
            );
            assert!(backend.exists(&key).await.expect("exists"));
            assert!(backend
                .refresh_ttl(&key, Some(Duration::from_secs(60)))
                .await
                .expect("touch"));
            assert!(backend.delete(&key).await.expect("delete"));
        }

        assert_eq!(
            stub.count(|c| &c.version),
            1,
            "ops must send no version command"
        );
        assert_eq!(stub.count(|c| &c.set), 25);
        assert_eq!(stub.count(|c| &c.get), 50);
        assert_eq!(stub.count(|c| &c.touch), 25);
        assert_eq!(stub.count(|c| &c.delete), 25);
        assert_eq!(
            stub.count(|c| &c.conns),
            1,
            "sequential ops reuse one connection"
        );
    }

    #[tokio::test]
    #[serial]
    async fn late_reply_after_socket_timeout_never_reaches_another_request() {
        // Every 4th get is answered after the socket read timeout. The late
        // reply lands on the connection the timed-out op used; with one
        // connection, reusing it would hand that reply to the next request.
        let timeout = Duration::from_millis(150);
        let stalls: Vec<u64> = (1..=10).map(|n| n * 4).collect();
        let stub = Stub::start(
            Some(Stall::Late {
                delay: timeout + Duration::from_millis(100),
            }),
            &stalls,
        );
        let backend = stub_backend(&stub, timeout, 1).await;
        let keys = seed_keys(7);
        for (k, v) in &keys {
            backend.set(k, v.clone(), None).await.expect("seed");
        }

        let (ok, timeouts) = assert_no_desync(&backend, &keys, 40).await;

        assert!(
            stub.count(|c| &c.stalled) >= 5,
            "the stub must have stalled replies"
        );
        assert!(timeouts >= 5, "stalled gets must time out (got {timeouts})");
        assert!(ok >= 25, "gets after a stall must succeed (got {ok})");
        assert_eq!(stub.count(|c| &c.version), 1, "no checkout ping");
    }

    #[tokio::test]
    #[serial]
    async fn budget_expiry_mid_reply_never_leaks_into_later_requests() {
        // The first stalled get trickles half its reply slower than the async
        // op budget (3 × 200 + 200 = 800 ms), so the caller times out while
        // the blocking op is still reading. The op then hits a socket read
        // timeout mid-reply, and the rest of the reply arrives later. The
        // connection must not go back to the idle list.
        let timeout = Duration::from_millis(200);
        let stub = Stub::start(
            Some(Stall::Trickle {
                bytes: 15,
                drip: Duration::from_millis(70),
                pause: Duration::from_millis(500),
            }),
            &[8],
        );
        let backend = stub_backend(&stub, timeout, 1).await;
        let keys = seed_keys(7);
        for (k, v) in &keys {
            backend.set(k, v.clone(), None).await.expect("seed");
        }

        // Gets 1-7 succeed; get 8 stalls past the budget.
        assert_eq!(assert_no_desync(&backend, &keys, 7).await, (7, 0));
        let err = backend.get(&keys[0].0).await.expect_err("stalled get");
        assert_eq!(err.kind, BackendErrorKind::Timeout);
        assert!(
            err.message.contains("budget"),
            "the async budget must fire mid-reply: {err}"
        );

        // The abandoned op still holds the only connection; later gets wait
        // for it, then run on a fresh one.
        let (ok, _) = assert_no_desync(&backend, &keys, 20).await;
        assert!(ok >= 18, "gets after the stall must succeed (got {ok})");
        assert_eq!(stub.count(|c| &c.stalled), 1);
        assert!(
            stub.count(|c| &c.conns) >= 2,
            "the desynced connection was replaced"
        );
    }

    #[tokio::test]
    #[serial]
    async fn budget_expiry_then_clean_finish_keeps_the_connection() {
        // A reply that trickles in slower than the budget but completes: the
        // caller gets a Timeout, and the abandoned op, having parsed the whole
        // reply, returns its connection in sync.
        // Budget 3 × 300 + 300 = 1200 ms; the 30-byte reply takes 1500 ms,
        // and no single read waits longer than the 300 ms socket timeout.
        let timeout = Duration::from_millis(300);
        let stub = Stub::start(
            Some(Stall::Trickle {
                bytes: usize::MAX,
                drip: Duration::from_millis(50),
                pause: Duration::ZERO,
            }),
            &[1],
        );
        let backend = stub_backend(&stub, timeout, 1).await;
        let keys = seed_keys(3);
        for (k, v) in &keys {
            backend.set(k, v.clone(), None).await.expect("seed");
        }

        let err = backend.get(&keys[0].0).await.expect_err("stalled get");
        assert!(
            err.message.contains("budget"),
            "the async budget must fire: {err}"
        );
        let (ok, _) = assert_no_desync(&backend, &keys, 10).await;
        assert!(ok >= 9, "gets after the stall must succeed (got {ok})");
        assert_eq!(
            stub.count(|c| &c.conns),
            1,
            "a cleanly finished op keeps its connection"
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    #[serial]
    async fn discarded_connections_take_their_threads_with_them() {
        // Every get times out, so every op discards its client and opens a
        // new one. Each client's r2d2 pool runs 3 worker threads; they must
        // exit when the client is dropped, not pile up behind a pending
        // reaper job (30 s each with r2d2's default idle_timeout).
        let timeout = Duration::from_millis(50);
        let stalls: Vec<u64> = (1..=30).collect();
        let stub = Stub::start(
            Some(Stall::Late {
                delay: Duration::from_millis(100),
            }),
            &stalls,
        );
        let backend = stub_backend(&stub, timeout, 2).await;
        for i in 0..30 {
            let err = backend
                .get(&format!("k{i}"))
                .await
                .expect_err("stalled get");
            assert!(err.kind.is_retryable(), "{err}");
        }
        assert!(
            stub.count(|c| &c.conns) >= 30,
            "each timeout replaced its connection"
        );

        tokio::time::sleep(Duration::from_millis(300)).await;
        let live = r2d2_threads();
        assert!(
            live <= 3 * 2,
            "{live} r2d2 threads alive; at most 3 per pooled connection (pool_size 2)"
        );
        drop(backend);
    }

    #[cfg(not(feature = "unsync"))] // tokio::spawn needs Send futures
    #[tokio::test]
    #[serial]
    async fn server_restart_costs_one_retryable_failure() {
        // Without the checkout ping, a connection that died while idle fails
        // the op that finds it. That failure must be retryable, and it must
        // drop every other idle connection, not fail once per connection.
        let stub = Stub::start(None, &[]);
        let backend = std::sync::Arc::new(stub_backend(&stub, Duration::from_secs(1), 4).await);

        // Concurrent slow ops leave several idle connections.
        stub.set_reply_delay(Duration::from_millis(100));
        let calls: Vec<_> = (0..4)
            .map(|i| {
                let b = std::sync::Arc::clone(&backend);
                tokio::spawn(async move { b.set(&format!("w{i}"), b"v".to_vec(), None).await })
            })
            .collect();
        for call in calls {
            call.await.expect("join").expect("warm-up set");
        }
        assert!(stub.count(|c| &c.conns) >= 2, "several connections warmed");
        stub.set_reply_delay(Duration::ZERO);

        stub.restart();
        let mut failures = Vec::new();
        for i in 0..6 {
            if let Err(e) = backend.set(&format!("after{i}"), b"v".to_vec(), None).await {
                failures.push(e);
            }
        }
        assert_eq!(
            failures.len(),
            1,
            "exactly one failure per restart, got {failures:?}"
        );
        for e in &failures {
            assert!(
                e.kind.is_retryable(),
                "a lost connection must be retryable: {e}"
            );
        }
        assert_eq!(stub.count(|c| &c.version), 1, "no checkout ping");
    }

    /// Silent memcached stubs for the UDP and Unix-socket transports: each
    /// answers `version`, so `connect()` succeeds, then never answers a `delete`.
    /// `deletes` counts the `delete` requests that reached the stub. (Over UDP
    /// a `get` would not do: rust-memcache 0.19 never sends an ASCII `get` there.)
    mod silent {
        use std::io::{BufRead, BufReader, Write};
        use std::net::UdpSocket;
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;

        pub struct Silent {
            pub url: String,
            pub deletes: Arc<AtomicU64>,
            /// Keeps a Unix socket's directory alive.
            _dir: Option<tempfile::TempDir>,
        }

        const VERSION: &[u8] = b"VERSION 1.6.99-stub\r\n";

        /// Over UDP every request and reply is one datagram behind an 8-byte
        /// frame header: request id, sequence number, datagram count, reserved.
        pub fn udp(url: impl Fn(std::net::SocketAddr) -> String) -> Silent {
            let sock = UdpSocket::bind("127.0.0.1:0").expect("bind");
            let addr = sock.local_addr().expect("addr");
            let deletes = Arc::new(AtomicU64::new(0));
            let counter = Arc::clone(&deletes);
            std::thread::spawn(move || {
                let mut buf = [0u8; 1500];
                while let Ok((n, from)) = sock.recv_from(&mut buf) {
                    let Some(body) = buf[..n].get(8..) else {
                        continue;
                    };
                    if body.starts_with(b"version") {
                        let mut reply = buf[..2].to_vec();
                        reply.extend([0, 0, 0, 1, 0, 0]);
                        reply.extend(VERSION);
                        let _ = sock.send_to(&reply, from);
                    } else if body.starts_with(b"delete ") {
                        counter.fetch_add(1, Ordering::SeqCst);
                    }
                }
            });
            Silent {
                url: url(addr),
                deletes,
                _dir: None,
            }
        }

        /// Accepts every connection and holds it open.
        #[cfg(unix)]
        pub fn unix() -> Silent {
            use std::os::unix::net::UnixListener;

            let dir = tempfile::tempdir().expect("tempdir");
            let path = dir.path().join("memcached.sock");
            let listener = UnixListener::bind(&path).expect("bind");
            let deletes = Arc::new(AtomicU64::new(0));
            let counter = Arc::clone(&deletes);
            std::thread::spawn(move || {
                for sock in listener.incoming().flatten() {
                    let counter = Arc::clone(&counter);
                    std::thread::spawn(move || {
                        let mut w = sock.try_clone().expect("clone");
                        let mut line = String::new();
                        let mut r = BufReader::new(sock);
                        while r.read_line(&mut line).is_ok_and(|n| n > 0) {
                            if line.starts_with("version") {
                                let _ = w.write_all(VERSION);
                            } else if line.starts_with("delete ") {
                                counter.fetch_add(1, Ordering::SeqCst);
                            }
                            line.clear();
                        }
                    });
                }
            });
            Silent {
                url: format!("memcache://{}", path.display()),
                deletes,
                _dir: Some(dir),
            }
        }
    }

    /// Run `test` on its own thread and runtime, and fail if it is not done
    /// within `limit`. A connection with no socket timeout blocks its thread
    /// forever, and a runtime waits for its blocking tasks when it drops, so
    /// `#[tokio::test]` would hang instead of failing. A failed assertion is
    /// caught before the runtime drops, for the same reason.
    fn within<F>(limit: Duration, test: impl FnOnce() -> F + Send + 'static)
    where
        F: std::future::Future<Output = ()>,
    {
        let (done, finished) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            let outcome =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| rt.block_on(test())));
            rt.shutdown_background();
            let _ = done.send(outcome);
        });
        match finished.recv_timeout(limit) {
            Ok(Ok(())) => {}
            Ok(Err(panic)) => std::panic::resume_unwind(panic),
            Err(_) => {
                panic!("still running after {limit:?}: an operation blocked with no socket timeout")
            }
        }
    }

    /// `pool_size + 1` deletes against a server that never answers them. Each
    /// must fail within the op budget as a retryable error, and the last must
    /// still reach the server: a connection stuck in a read keeps its permit,
    /// so without socket timeouts the last delete times out waiting for one.
    async fn lost_replies_cost_no_permits(stub: silent::Silent) {
        use std::sync::atomic::Ordering;

        const POOL: u32 = 2;
        let timeout = Duration::from_millis(200);
        let backend = MemcachedBackend::builder()
            .url(&stub.url)
            .timeout(timeout)
            .connect_timeout(timeout)
            .pool_size(POOL)
            .connect()
            .await
            .expect("connect: the stub answers version");
        let budget = timeout * 3 + timeout;

        for i in 0..=POOL {
            let started = std::time::Instant::now();
            let err = backend
                .delete(&format!("k{i}"))
                .await
                .expect_err("the stub never answers a delete");
            let elapsed = started.elapsed();
            assert!(
                err.kind.is_retryable(),
                "delete #{i} on {}: {err}",
                stub.url
            );
            assert!(
                elapsed < budget + Duration::from_millis(500),
                "delete #{i} on {} took {elapsed:?}",
                stub.url
            );
        }
        // The stub counts on its own thread; give it a moment.
        let want = u64::from(POOL) + 1;
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while stub.deletes.load(Ordering::SeqCst) < want && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            stub.deletes.load(Ordering::SeqCst),
            want,
            "on {}, every delete must reach the server; a missing one waited for a permit",
            stub.url
        );
    }

    #[test]
    #[serial]
    fn lost_udp_replies_cost_no_permits() {
        within(Duration::from_secs(20), || async {
            lost_replies_cost_no_permits(silent::udp(|a| format!("memcache+udp://{a}"))).await;
            lost_replies_cost_no_permits(silent::udp(|a| format!("memcache://{a}?udp=true"))).await;
        });
    }

    #[cfg(unix)]
    #[test]
    #[serial]
    fn stalled_unix_socket_costs_no_permits() {
        within(Duration::from_secs(20), || async {
            lost_replies_cost_no_permits(silent::unix()).await;
        });
    }

    /// Live semantics against a real memcached.
    ///
    /// Requires `CACHEKIT_TEST_MEMCACHED_URL` (e.g. `tcp://127.0.0.1:11211`);
    /// skips with a notice otherwise so the default CI suite stays green
    /// without a memcached service.
    #[tokio::test]
    #[serial]
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
