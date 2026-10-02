//! Client wall time of cachekit.io operations, one JSON line per request.
//!
//! Times `CacheKit` calls against the cachekit.io dev endpoint and
//! appends one JSON object per request to `--out`. Two arms run in one process,
//! interleaved in ABBA blocks. Each block gets a new client, warmed by one GET
//! miss (a `warmup` row), so connection placement varies between blocks rather
//! than staying fixed per arm:
//!
//! - `sdk`: the real [`CachekitIO`] backend. Its wall time is the true SDK
//!   cost, but the backend exposes no response headers, so a row carries no
//!   `cf-ray` and its status is inferred from the result.
//! - `transport`: a [`Backend`] defined here over a `reqwest::Client` built
//!   with the options `CachekitIO` uses (rustls, no redirects, 30 s timeout,
//!   10 s connect timeout, HTTP/1.1), the same headers and the same URLs. It
//!   reads `cf-ray`, status, HTTP version and time to first byte, and counts
//!   DNS resolutions (one per new connection). Before trusting its legs, run it
//!   against `sdk`: the two must agree within the `sdk`/`sdk` A/A spread, or
//!   the copy has drifted from the backend.
//!
//! In both arms the client layer (keys, MessagePack, reliability off, L1 off)
//! is the real SDK. A new connection is detected for both arms from outside
//! the HTTP stack, by diffing this process's established TCP sockets to port
//! 443 before and after each request (`/proc`, Linux only; `null` elsewhere).
//!
//! Safety rails, enforced here:
//! - it writes, so it runs only against the hosts in `WRITABLE_HOSTS` (the dev
//!   endpoint): an allowlist, so no second production hostname can slip by;
//! - every key is appended (and synced) to `--ledger` before its PUT is sent,
//!   and every PUT carries a TTL of at most 900 s, so a crash leaves only keys
//!   the ledger names and the TTL removes;
//! - a 429, a 503, any other 4xx but 404, or a transport error stops the run
//!   (exit 3). Other 5xx are recorded and the run goes on, up to 5 of them, so
//!   a server's sporadic errors are counted rather than ending the run.
//!   Nothing is retried, so a limiter verdict is never retried into, and once
//!   one request in a burst fails, no task in that burst sends another;
//! - total requests, warm-ups and trace probes included, are capped
//!   (`--max-ops`, at most 2000) and paced under `--max-per-min`; a burst is at
//!   most 32 concurrent requests. A `#[cachekit]` cold miss sends four requests
//!   (GET, lock, PUT, unlock) and is budgeted at five, one spare; a call that
//!   sends more stops the run. Every request it sent is checked against the
//!   stop rules, because the macro itself swallows backend errors.
//!
//! ```text
//! CACHEKIT_API_KEY=… CACHEKIT_API_URL=https://… cargo run --release \
//!   --example wall_time_probe --features macros -- --run R1 --phase aa-warm --out rows.jsonl \
//!   --ledger keys.txt --arms sdk,sdk --samples 40 --block 10 --ops put,get,delete
//! ```

#![allow(clippy::print_stdout)]

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io::Write;
use std::net::ToSocketAddrs;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use cachekit::backend::cachekitio::CachekitIO;
use cachekit::backend::{Backend, HealthStatus, LockableBackend};
use cachekit::interop::{interop_key, InteropValue};
use cachekit::metrics::{metrics_headers, MetricsProvider};
use cachekit::session::session_headers;
use cachekit::url_validator::validate_cachekitio_url;
use cachekit::{BackendError, CacheKit, CachekitError};
use serde_json::{json, Value};

/// The only hosts this harness may write to.
const WRITABLE_HOSTS: [&str; 1] = ["api.dev.cachekit.io"];
const MAX_TTL_S: u64 = 900;
const MAX_OPS: usize = 2000;
const MAX_CONCURRENCY: usize = 32;
/// 5xx responses (503 excepted) a run records before it stops.
const MAX_SERVER_ERRORS: usize = 5;
/// Budget for one `#[cachekit]` cold miss: a serial leader sends four requests
/// (GET miss, lock, PUT, unlock); the fifth is a margin. A call that records
/// more than this stops the run.
const MACRO_REQUESTS: usize = 5;
const ABBA: [usize; 4] = [0, 1, 1, 0];

// ── Arguments ────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    Sdk,
    Transport,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Op {
    Put,
    Get,
    Head,
    Delete,
}

impl Op {
    fn method(self) -> &'static str {
        match self {
            Self::Put => "PUT",
            Self::Get => "GET",
            Self::Head => "HEAD",
            Self::Delete => "DELETE",
        }
    }
}

#[derive(Debug)]
struct Args {
    run: String,
    env: String,
    phase: String,
    out: String,
    ledger: String,
    key_prefix: String,
    arms: [Kind; 2],
    samples: usize,
    block: usize,
    gap: Duration,
    fresh_conn: bool,
    concurrency: usize,
    ops: Vec<Op>,
    macro_cold_miss: bool,
    size: usize,
    ttl: Duration,
    max_per_min: u32,
    max_ops: usize,
}

const USAGE: &str = "usage: wall_time_probe --run ID --phase NAME --out FILE --ledger FILE \
[--env dev] [--key-prefix P] [--arms sdk,sdk|sdk,transport|transport,transport] \
[--samples N per arm] [--block N] [--gap-ms MS] [--fresh-conn] [--concurrency C] \
[--ops put,get,head,delete | --macro-cold-miss] [--size BYTES] [--ttl-s S] \
[--max-per-min N] [--max-ops N]
env: CACHEKIT_API_KEY, CACHEKIT_API_URL";

const VALUE_FLAGS: [&str; 16] = [
    "run",
    "env",
    "phase",
    "out",
    "ledger",
    "key-prefix",
    "arms",
    "samples",
    "block",
    "gap-ms",
    "concurrency",
    "ops",
    "size",
    "ttl-s",
    "max-per-min",
    "max-ops",
];

fn parse_args(argv: &[String]) -> Result<Args, String> {
    let mut kv: HashMap<&str, &str> = HashMap::new();
    let mut flags: HashSet<&str> = HashSet::new();
    let mut it = argv.iter();
    while let Some(a) = it.next() {
        let name = a
            .strip_prefix("--")
            .ok_or_else(|| format!("unexpected argument {a:?}"))?;
        if name == "help" {
            return Err("flags:".into());
        }
        if matches!(name, "fresh-conn" | "macro-cold-miss") {
            flags.insert(name);
        } else if !VALUE_FLAGS.contains(&name) {
            return Err(format!("unknown flag --{name}"));
        } else {
            kv.insert(
                name,
                it.next().ok_or_else(|| format!("--{name} needs a value"))?,
            );
        }
    }
    let req = |k: &str| {
        kv.get(k)
            .map(|v| (*v).to_owned())
            .ok_or_else(|| format!("--{k} is required"))
    };
    let num = |k: &str, d: usize| -> Result<usize, String> {
        kv.get(k).map_or(Ok(d), |v| {
            v.parse().map_err(|_| format!("--{k}: not a number: {v:?}"))
        })
    };
    let kind = |s: &str| match s {
        "sdk" => Ok(Kind::Sdk),
        "transport" => Ok(Kind::Transport),
        _ => Err(format!("unknown arm {s:?}")),
    };
    let arms: Vec<Kind> = kv
        .get("arms")
        .unwrap_or(&"sdk,sdk")
        .split(',')
        .map(kind)
        .collect::<Result<_, _>>()?;
    let [a, b] = arms[..] else {
        return Err("--arms takes exactly two arms".into());
    };
    let ops: Vec<Op> = kv
        .get("ops")
        .unwrap_or(&"put,get,delete")
        .split(',')
        .map(|s| match s {
            "put" => Ok(Op::Put),
            "get" => Ok(Op::Get),
            "head" => Ok(Op::Head),
            "delete" => Ok(Op::Delete),
            _ => Err(format!("unknown op {s:?}")),
        })
        .collect::<Result<_, _>>()?;
    let epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let args = Args {
        run: req("run")?,
        env: kv.get("env").unwrap_or(&"dev").to_string(),
        phase: req("phase")?,
        out: req("out")?,
        ledger: req("ledger")?,
        key_prefix: kv
            .get("key-prefix")
            .map_or_else(|| format!("wall-time-probe-{epoch}"), |v| (*v).to_owned()),
        arms: [a, b],
        samples: num("samples", 20)?,
        block: num("block", 10)?,
        gap: Duration::from_millis(num("gap-ms", 0)? as u64),
        fresh_conn: flags.contains("fresh-conn"),
        concurrency: num("concurrency", 1)?,
        ops,
        macro_cold_miss: flags.contains("macro-cold-miss"),
        size: num("size", 1024)?,
        ttl: Duration::from_secs(num("ttl-s", 900)? as u64),
        max_per_min: u32::try_from(num("max-per-min", 60)?)
            .map_err(|_| "--max-per-min is too large")?,
        max_ops: num("max-ops", 400)?,
    };
    validate(&args)?;
    Ok(args)
}

fn validate(a: &Args) -> Result<(), String> {
    if a.block == 0 || a.samples == 0 || a.samples % a.block != 0 {
        return Err("--samples must be a positive multiple of --block".into());
    }
    if a.concurrency == 0 || a.concurrency > MAX_CONCURRENCY {
        return Err(format!("--concurrency must be 1..={MAX_CONCURRENCY}"));
    }
    if a.concurrency > 1 && a.block % a.concurrency != 0 {
        return Err("--block must be a multiple of --concurrency".into());
    }
    if a.ttl.is_zero() || a.ttl.as_secs() > MAX_TTL_S {
        return Err(format!("--ttl-s must be 1..={MAX_TTL_S}"));
    }
    if a.max_ops > MAX_OPS || a.max_per_min == 0 {
        return Err(format!(
            "--max-ops must be at most {MAX_OPS}, --max-per-min positive"
        ));
    }
    if a.env != "dev" {
        // The host allowlist is dev only, so any other label would mislabel rows.
        return Err(format!(
            "--env must be dev (the only writable host); got {:?}",
            a.env
        ));
    }
    if a.macro_cold_miss && a.concurrency > 1 {
        return Err("--macro-cold-miss runs serially; drop --concurrency".into());
    }
    if a.macro_cold_miss && a.arms.contains(&Kind::Transport) {
        // The cold-miss path takes the distributed lock, which only the real
        // backend implements; a transport arm would time a different call shape.
        return Err("--macro-cold-miss runs on sdk arms only".into());
    }
    let per_sample = if a.macro_cold_miss {
        MACRO_REQUESTS
    } else {
        a.ops.len()
    };
    // Bursts run each step twice (cold, then warm on the same pool).
    let steps = if a.concurrency > 1 { 2 } else { 1 };
    let blocks_per_arm = a.samples / a.block;
    let transport_arms = a.arms.iter().filter(|k| **k == Kind::Transport).count();
    // Plus one warm-up GET per block, and two trace probes per transport block.
    let total = 2 * a.samples * per_sample * steps
        + 2 * blocks_per_arm
        + 2 * transport_arms * blocks_per_arm;
    if total > a.max_ops {
        return Err(format!(
            "this run sends {total} requests, over --max-ops {}",
            a.max_ops
        ));
    }
    Ok(())
}

// ── Transport arm: a reqwest copy of the CachekitIO backend ──────────────────

/// What the transport arm saw on the wire for one request.
#[derive(Clone, Debug)]
struct Exchange {
    status: u16,
    version: String,
    ttfb: Duration,
    headers: BTreeMap<String, String>,
}

/// Counts resolutions: hyper resolves once per new connection, never per request.
struct CountingResolver {
    count: Arc<AtomicU64>,
}

impl reqwest::dns::Resolve for CountingResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        self.count.fetch_add(1, Ordering::SeqCst);
        let host = name.as_str().to_owned();
        Box::pin(async move {
            // getaddrinfo on the blocking pool, as reqwest's default resolver does.
            let addrs =
                tokio::task::spawn_blocking(move || (host.as_str(), 0).to_socket_addrs()).await??;
            let addrs: reqwest::dns::Addrs = Box::new(addrs.collect::<Vec<_>>().into_iter());
            Ok(addrs)
        })
    }
}

const KEEP_HEADERS: [&str; 6] = [
    "cf-ray",
    "x-cachekit-store-source",
    "x-cachekit-l0-status",
    "x-cachekit-freshness",
    "ratelimit-remaining",
    "retry-after",
];

struct Transport {
    client: reqwest::Client,
    api_key: String,
    api_url: String,
    provider: OnceLock<MetricsProvider>,
    resolves: Arc<AtomicU64>,
    /// Last exchange per (method, key); ops on one key never overlap.
    seen: Mutex<HashMap<(&'static str, String), Exchange>>,
}

impl Transport {
    fn new(api_key: &str, api_url: &str) -> Result<Self, CachekitError> {
        let resolves = Arc::new(AtomicU64::new(0));
        // Mirrors CachekitIOBuilder::build; the only addition is the resolver.
        let client = reqwest::Client::builder()
            .use_rustls_tls()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .dns_resolver(Arc::new(CountingResolver {
                count: resolves.clone(),
            }))
            .build()
            .map_err(|e| CachekitError::Config(format!("failed to build HTTP client: {e}")))?;
        Ok(Self {
            client,
            api_key: api_key.to_owned(),
            api_url: api_url.trim_end_matches('/').to_owned(),
            provider: OnceLock::new(),
            resolves,
            seen: Mutex::new(HashMap::new()),
        })
    }

    fn url(&self, key: &str) -> String {
        format!("{}/v1/cache/{}", self.api_url, urlencoding::encode(key))
    }

    fn take(&self, method: &'static str, key: &str) -> Option<Exchange> {
        self.seen.lock().ok()?.remove(&(method, key.to_owned()))
    }

    /// Send with the backend's standard headers; record what came back.
    async fn send(
        &self,
        method: &'static str,
        key: &str,
        req: reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, BackendError> {
        let mut req = req.bearer_auth(&self.api_key);
        for (name, value) in session_headers() {
            req = req.header(name, value);
        }
        for (name, value) in metrics_headers(self.provider.get()) {
            req = req.header(name, value);
        }
        let start = Instant::now();
        let resp = req.send().await.map_err(|e| wire_err(&e, &self.api_key))?;
        let headers = KEEP_HEADERS
            .iter()
            .filter_map(|h| {
                Some((
                    (*h).to_owned(),
                    resp.headers().get(*h)?.to_str().ok()?.to_owned(),
                ))
            })
            .collect();
        let ex = Exchange {
            status: resp.status().as_u16(),
            version: format!("{:?}", resp.version()),
            ttfb: start.elapsed(),
            headers,
        };
        if let Ok(mut seen) = self.seen.lock() {
            seen.insert((method, key.to_owned()), ex);
        }
        Ok(resp)
    }

    /// `GET /cdn-cgi/trace`: answered by the edge without the Worker, so on a
    /// reused connection its time to first byte is the round-trip floor.
    async fn trace(&self) -> Result<Exchange, BackendError> {
        let req = self.client.get(format!("{}/cdn-cgi/trace", self.api_url));
        self.send("GET", "/cdn-cgi/trace", req).await?;
        self.take("GET", "/cdn-cgi/trace")
            .ok_or_else(|| BackendError::permanent("trace not recorded"))
    }
}

fn wire_err(e: &reqwest::Error, api_key: &str) -> BackendError {
    let msg = BackendError::sanitize_message(&e.to_string(), api_key);
    if e.is_timeout() {
        BackendError::timeout(msg)
    } else {
        BackendError::transient(msg)
    }
}

/// Status handling copied from `CachekitIO`'s `Backend` impl.
#[async_trait]
impl Backend for Transport {
    fn attach_metrics(&self, provider: MetricsProvider) {
        self.provider.get_or_init(|| provider);
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        let resp = self
            .send("GET", key, self.client.get(self.url(key)))
            .await?;
        match resp.status().as_u16() {
            200 => Ok(Some(
                resp.bytes()
                    .await
                    .map_err(|e| wire_err(&e, &self.api_key))?
                    .to_vec(),
            )),
            404 => Ok(None),
            s => Err(BackendError::from_http_status(s, &[])),
        }
    }

    async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        let mut req = self
            .client
            .put(self.url(key))
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .body(value);
        if let Some(ttl) = ttl {
            req = req.header("X-TTL", ttl.as_secs().to_string());
        }
        let resp = self.send("PUT", key, req).await?;
        match resp.status().as_u16() {
            s if (200..300).contains(&s) => Ok(()),
            s => Err(BackendError::from_http_status(s, &[])),
        }
    }

    async fn delete(&self, key: &str) -> Result<bool, BackendError> {
        let resp = self
            .send("DELETE", key, self.client.delete(self.url(key)))
            .await?;
        match resp.status().as_u16() {
            200 | 204 => Ok(true),
            s => Err(BackendError::from_http_status(s, &[])),
        }
    }

    async fn exists(&self, key: &str) -> Result<bool, BackendError> {
        let resp = self
            .send("HEAD", key, self.client.head(self.url(key)))
            .await?;
        match resp.status().as_u16() {
            200 => Ok(true),
            404 => Ok(false),
            s => Err(BackendError::from_http_status(s, &[])),
        }
    }

    async fn health(&self) -> Result<HealthStatus, BackendError> {
        Err(BackendError::permanent(
            "health is not probed by this harness",
        ))
    }
}

// ── Macro arm: the real backend, with every request's result recorded ───────

/// One request the backend sent: method and HTTP status (`None` = no response).
type Part = (&'static str, Option<u16>);

/// The real `CachekitIO`, recording each request's outcome. `#[cachekit]`
/// fails open — it passes over GET errors and drops PUT and lock errors — so
/// without this record a 429 would be timed as a fast success and nothing
/// would stop the run.
struct Recorded {
    inner: CachekitIO,
    parts: Mutex<Vec<(Part, Option<String>)>>,
}

impl Recorded {
    fn note<T>(&self, method: &'static str, r: &Result<T, BackendError>, ok: impl Fn(&T) -> u16) {
        let entry = match r {
            Ok(v) => ((method, Some(ok(v))), None),
            Err(e) => ((method, http_status(&e.to_string())), Some(e.to_string())),
        };
        // These records are the macro stop rules' only input: never drop one.
        self.parts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(entry);
    }

    fn drain(&self) -> Vec<(Part, Option<String>)> {
        std::mem::take(
            &mut *self
                .parts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }
}

#[async_trait]
impl Backend for Recorded {
    fn attach_metrics(&self, provider: MetricsProvider) {
        self.inner.attach_metrics(provider);
    }
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        let r = self.inner.get(key).await;
        self.note("GET", &r, |v| if v.is_some() { 200 } else { 404 });
        r
    }
    async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        let r = self.inner.set(key, value, ttl).await;
        self.note("PUT", &r, |()| 200);
        r
    }
    async fn delete(&self, key: &str) -> Result<bool, BackendError> {
        let r = self.inner.delete(key).await;
        self.note("DELETE", &r, |_| 200);
        r
    }
    async fn exists(&self, key: &str) -> Result<bool, BackendError> {
        let r = self.inner.exists(key).await;
        self.note("HEAD", &r, |hit| if *hit { 200 } else { 404 });
        r
    }
    async fn health(&self) -> Result<HealthStatus, BackendError> {
        self.inner.health().await
    }
    fn as_lockable(&self) -> Option<&dyn LockableBackend> {
        Some(self)
    }
}

#[async_trait]
impl LockableBackend for Recorded {
    async fn acquire_lock(
        &self,
        key: &str,
        timeout_ms: u64,
    ) -> Result<Option<String>, BackendError> {
        // A null lock id means held, or a storage error on the server. Passed
        // through, the fill would poll GET up to 50 times, unpaced; as an
        // error it fills unlocked, and the recorded error stops the run.
        let r = match self.inner.acquire_lock(key, timeout_ms).await {
            Ok(None) => Err(BackendError::permanent("lock not granted")),
            r => r,
        };
        self.note("LOCK", &r, |_| 200);
        r
    }
    async fn release_lock(&self, key: &str, lock_id: &str) -> Result<bool, BackendError> {
        let r = self.inner.release_lock(key, lock_id).await;
        self.note("UNLOCK", &r, |released| if *released { 200 } else { 404 });
        r
    }
}

// ── Arms ─────────────────────────────────────────────────────────────────────

struct Arm {
    kind: Kind,
    slot: &'static str,
    cache: CacheKit,
    wire: Option<Arc<Transport>>,
    recorded: Option<Arc<Recorded>>,
}

/// `record` wraps the sdk arm's backend in [`Recorded`]; only the macro
/// regime needs it, so the plain sdk arm stays the bare SDK.
fn build_arm(
    kind: Kind,
    slot: &'static str,
    api_key: &str,
    api_url: &str,
    record: bool,
) -> Result<Arm, CachekitError> {
    let (mut wire, mut recorded) = (None, None);
    let backend: cachekit::SharedBackend = match kind {
        Kind::Sdk => {
            let io = CachekitIO::builder()
                .api_key(api_key)
                .api_url(api_url)
                .allow_custom_host(true)
                .build()?;
            if record {
                let r = Arc::new(Recorded {
                    inner: io,
                    parts: Mutex::new(Vec::new()),
                });
                recorded = Some(r.clone());
                r
            } else {
                Arc::new(io)
            }
        }
        Kind::Transport => {
            let t = Arc::new(Transport::new(api_key, api_url)?);
            wire = Some(t.clone());
            t
        }
    };
    // No L1, so every call reaches the network; no reliability, so nothing retries.
    let cache = CacheKit::builder().backend(backend).no_l1().build()?;
    Ok(Arm {
        kind,
        slot,
        cache,
        wire,
        recorded,
    })
}

/// Must equal the `namespace` and `interop` literals in the attribute below:
/// the ledger, and so cleanup, is built from these copies.
const MACRO_NS: &str = "wall-time-probe";
const MACRO_OP: &str = "cold-miss";

#[cachekit::cachekit(client = cache, ttl = 900, interop = "cold-miss", namespace = "wall-time-probe")]
async fn cold_miss(cache: &CacheKit, id: String) -> Result<String, CachekitError> {
    Ok(id)
}

// ── Measurement ──────────────────────────────────────────────────────────────

/// One request as timed by the client.
struct Timed {
    op: &'static str,
    method: &'static str,
    key: String,
    started: String,
    ended: String,
    total: Duration,
    outcome: Result<Option<bool>, String>,
    new_conn: Option<bool>,
    resolves: Option<u64>,
    exchange: Option<Exchange>,
    /// Each request a macro call sent, when the arm records them.
    parts: Vec<Part>,
}

/// Established TCP sockets of this process to port 443, by inode.
fn tls_sockets() -> Option<BTreeSet<u64>> {
    let mine: HashSet<u64> = std::fs::read_dir("/proc/self/fd")
        .ok()?
        .filter_map(|e| {
            let target = std::fs::read_link(e.ok()?.path()).ok()?;
            target
                .to_str()?
                .strip_prefix("socket:[")?
                .strip_suffix(']')?
                .parse()
                .ok()
        })
        .collect();
    let mut out = BTreeSet::new();
    for table in ["/proc/self/net/tcp", "/proc/self/net/tcp6"] {
        let Ok(text) = std::fs::read_to_string(table) else {
            continue;
        };
        for line in text.lines().skip(1) {
            let cols: Vec<&str> = line.split_whitespace().collect();
            // rem_address ends in the hex port; state 01 is ESTABLISHED.
            if cols.len() > 9 && cols[2].ends_with(":01BB") && cols[3] == "01" {
                if let Ok(inode) = cols[9].parse() {
                    if mine.contains(&inode) {
                        out.insert(inode);
                    }
                }
            }
        }
    }
    Some(out)
}

fn new_sockets(before: Option<&BTreeSet<u64>>, after: Option<&BTreeSet<u64>>) -> Option<usize> {
    Some(after?.difference(before?).count())
}

async fn timed_op(arm: &Arm, op: Op, key: &str, value: &str, ttl: Duration, serial: bool) -> Timed {
    let before = serial.then(tls_sockets).flatten();
    let resolves0 = arm.wire.as_ref().map(|w| w.resolves.load(Ordering::SeqCst));
    let started = iso_now();
    let t0 = Instant::now();
    let outcome = match op {
        Op::Put => arm
            .cache
            .set_with_ttl(key, &value, ttl)
            .await
            .map(|()| None),
        Op::Get => arm
            .cache
            .get::<String>(key)
            .await
            .map(|v| Some(v.is_some())),
        Op::Head => arm.cache.exists(key).await.map(Some),
        Op::Delete => arm.cache.delete(key).await.map(Some),
    };
    let total = t0.elapsed();
    let ended = iso_now();
    let after = serial.then(tls_sockets).flatten();
    let resolves = arm
        .wire
        .as_ref()
        .zip(resolves0)
        .map(|(w, r0)| w.resolves.load(Ordering::SeqCst) - r0);
    let op_label = match (op, &outcome) {
        (Op::Put, _) => "put",
        (Op::Get, Ok(Some(true))) => "get-hit",
        (Op::Get, _) => "get-miss",
        (Op::Head, Ok(Some(true))) => "head-hit",
        (Op::Head, _) => "head-miss",
        (Op::Delete, Ok(Some(true))) => "delete",
        (Op::Delete, _) => "delete-miss",
    };
    Timed {
        op: op_label,
        method: op.method(),
        key: key.to_owned(),
        started,
        ended,
        total,
        outcome: outcome.map_err(|e| e.to_string()),
        new_conn: new_sockets(before.as_ref(), after.as_ref()).map(|n| n > 0),
        resolves,
        exchange: arm.wire.as_ref().and_then(|w| w.take(op.method(), key)),
        parts: Vec::new(),
    }
}

async fn timed_cold_miss(arm: &Arm, id: &str) -> Timed {
    // Start from an empty record: the block's warm-up GET ran on this arm too.
    if let Some(r) = &arm.recorded {
        r.drain();
    }
    let before = tls_sockets();
    let started = iso_now();
    let t0 = Instant::now();
    let called = cold_miss(&arm.cache, id.to_owned()).await.map(|_| None);
    let total = t0.elapsed();
    let ended = iso_now();
    let after = tls_sockets();
    let recorded = arm.recorded.as_ref().map(|r| r.drain()).unwrap_or_default();
    // Report the first failed request; `Sink::row` judges every one of them.
    let outcome = match recorded.iter().find_map(|(_, err)| err.clone()) {
        Some(err) => Err(err),
        None => called.map_err(|e| e.to_string()),
    };
    Timed {
        op: "macro-cold-miss",
        method: "CALL",
        key: macro_key(id).unwrap_or_default(),
        started,
        ended,
        total,
        outcome,
        new_conn: new_sockets(before.as_ref(), after.as_ref()).map(|n| n > 0),
        resolves: None,
        exchange: None,
        parts: recorded.into_iter().map(|(p, _)| p).collect(),
    }
}

fn macro_key(id: &str) -> Result<String, CachekitError> {
    interop_key(MACRO_NS, MACRO_OP, &[InteropValue::from(id)])
}

// ── Output ───────────────────────────────────────────────────────────────────

struct Sink {
    args: Args,
    host: String,
    out: std::fs::File,
    ledger: std::fs::File,
    loadavg: Value,
    server_errors: usize,
    /// Distinguishes this process's rows from another run of the same phase
    /// under one run id: block numbers restart at 0 in every process.
    invocation: String,
}

fn ms(d: Duration) -> f64 {
    (d.as_secs_f64() * 100_000.0).round() / 100.0
}

impl Sink {
    /// Record a key before the request that writes it is sent.
    fn ledger(&mut self, key: &str) -> Result<(), String> {
        writeln!(self.ledger, "{key}")
            .and_then(|()| self.ledger.sync_data())
            .map_err(|e| format!("ledger: {e}"))
    }

    /// Write one JSON row, then apply the stop rules to it.
    fn row(
        &mut self,
        arm: &Arm,
        block: usize,
        sample: usize,
        t: &Timed,
        burst: Option<(&str, usize)>,
    ) -> Result<(), Stop> {
        let ex = t.exchange.as_ref();
        // The sdk arm sees no status; infer it the way the backend mapped it.
        let inferred = match &t.outcome {
            Ok(Some(false)) => Some(404),
            Ok(_) => Some(200),
            Err(msg) => http_status(msg),
        };
        let status = ex.map(|e| e.status).or(inferred);
        let ray = ex.and_then(|e| e.headers.get("cf-ray").cloned());
        let reused = t.new_conn == Some(false);
        let row = json!({
            "session_started": t.started,
            "session_ended": t.ended,
            "host": self.host,
            "label": t.op,
            "method": t.method,
            "path_class": if t.op == "trace" { "trace" } else { "cache" },
            "key": t.key,
            "status": status,
            "status_inferred": ex.is_none(),
            // reqwest is built without its http2 feature, so both arms speak
            // HTTP/1.1; the transport arm reports what it observed.
            "http_version": ex.map_or_else(|| "HTTP/1.1".to_owned(), |e| e.version.clone()),
            "protocol": "h1",
            "num_connects": t.new_conn.map(u8::from),
            "connection_new": t.new_conn,
            "dns_resolves": t.resolves,
            "dns_ms": null,
            "tcp_ms": null,
            "tls_ms": null,
            "pretransfer_ms": null,
            "ttfb_ms": ex.map(|e| ms(e.ttfb)),
            // Request sent -> first byte, comparable to curl's `wait` only on a reused connection.
            "wait_ms": ex.filter(|_| reused).map(|e| ms(e.ttfb)),
            "total_ms": ms(t.total),
            "exitcode": i32::from(t.outcome.is_err()),
            "errormsg": t.outcome.as_ref().err(),
            "headers": ex.map(|e| e.headers.clone()).unwrap_or_default(),
            "header_names": ex.map(|e| e.headers.keys().cloned().collect::<Vec<_>>()).unwrap_or_default(),
            "ray_id": ray,
            "run": self.args.run,
            "env": self.args.env,
            "phase": self.args.phase,
            "client": "rs",
            "client_version": env!("CARGO_PKG_VERSION"),
            "arm": match arm.kind { Kind::Sdk => "sdk", Kind::Transport => "transport" },
            "slot": arm.slot,
            "block": block,
            "sample": sample,
            "size": self.args.size,
            "burst": burst.map(|(phase, n)| json!({"step": phase, "new_connections": n})),
            "requests": (!t.parts.is_empty()).then(|| json!(t.parts)),
            "invocation": self.invocation,
            "loadavg": self.loadavg,
        });
        writeln!(self.out, "{row}").map_err(|e| Stop::Fault(format!("out: {e}")))?;
        println!(
            "  {:10} {} {:9} {:16} {:6} {:>4} new={:5} total={:7.1} ttfb={:>7} ray={}",
            self.args.phase,
            arm.slot,
            row["arm"].as_str().unwrap_or("?"),
            t.op,
            t.method,
            status.map_or_else(|| "-".to_owned(), |s| s.to_string()),
            t.new_conn.map_or_else(|| "?".to_owned(), |n| n.to_string()),
            ms(t.total),
            ex.map_or_else(|| "-".to_owned(), |e| format!("{:.1}", ms(e.ttfb))),
            row["ray_id"].as_str().unwrap_or("-"),
        );
        if t.parts.is_empty() {
            return self.judge(t.method, status, t.outcome.is_ok(), t);
        }
        // A macro call: every request it sent is judged on its own.
        if t.parts.len() > MACRO_REQUESTS {
            return Err(Stop::Refused(format!(
                "{}: sent {} requests, over the budget of {MACRO_REQUESTS}: {:?}",
                t.op,
                t.parts.len(),
                t.parts
            )));
        }
        let mut failed = false;
        for (method, part_status) in &t.parts {
            let ok = part_status.is_some_and(|s| (200..300).contains(&s) || s == 404);
            failed |= !ok;
            self.judge(method, *part_status, ok, t)?;
        }
        // A call that failed while every request it sent succeeded failed in
        // the client, where no rule above looks: stop rather than time it.
        if t.outcome.is_err() && !failed {
            return Err(Stop::Refused(format!("{}: {:?}", t.op, t.outcome)));
        }
        Ok(())
    }

    /// The stop rule for one request.
    fn judge(
        &mut self,
        method: &str,
        status: Option<u16>,
        ok: bool,
        t: &Timed,
    ) -> Result<(), Stop> {
        match status {
            Some(s) if ((200..300).contains(&s) || s == 404) && ok => Ok(()),
            // A 503 is this service's limiter or fail-closed verdict: stop.
            // Any other 5xx is counted and the run goes on, so a server's
            // sporadic errors become a measured rate instead of ending it.
            Some(s) if s >= 500 && s != 503 && self.server_errors < MAX_SERVER_ERRORS => {
                self.server_errors += 1;
                Ok(())
            }
            _ => Err(Stop::Refused(format!(
                "{method} in {}: status {status:?}, {:?}",
                t.op, t.outcome
            ))),
        }
    }
}

/// The status in a `BackendError::from_http_status` message (`HTTP 500: …`).
fn http_status(msg: &str) -> Option<u16> {
    let rest = &msg[msg.find("HTTP ")? + 5..];
    rest.get(..3)?.parse().ok()
}

fn loadavg() -> Value {
    std::fs::read_to_string("/proc/loadavg").map_or(Value::Null, |s| {
        json!(s
            .split_whitespace()
            .take(3)
            .filter_map(|v| v.parse::<f64>().ok())
            .collect::<Vec<_>>())
    })
}

/// UTC ISO-8601 with milliseconds.
fn iso_now() -> String {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let (days, rem) = (d.as_secs() / 86_400, d.as_secs() % 86_400);
    // Civil-from-days (H. Hinnant), valid for every date after 1970.
    let z = days + 719_468;
    let (era, doe) = (z / 146_097, z % 146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + u64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60,
        d.subsec_millis()
    )
}

// ── Schedule ─────────────────────────────────────────────────────────────────

/// Paces request starts at most `per_min` per minute across both arms.
struct Pacer {
    next: Instant,
    every: Duration,
}

impl Pacer {
    async fn wait(&mut self, n: u32) {
        tokio::time::sleep_until(self.next.into()).await;
        self.next = Instant::now().max(self.next) + self.every * n;
    }
}

enum Stop {
    Refused(String),
    Fault(String),
}

impl From<String> for Stop {
    fn from(e: String) -> Self {
        Self::Fault(e)
    }
}

#[allow(clippy::too_many_lines)] // one linear schedule reads better unsplit
async fn schedule(sink: &mut Sink, api_key: &str, api_url: &str) -> Result<(), Stop> {
    let a = &sink.args;
    let (arms, block, samples, concurrency) = (a.arms, a.block, a.samples, a.concurrency);
    let (gap, fresh, ttl, macro_mode) = (a.gap, a.fresh_conn, a.ttl, a.macro_cold_miss);
    let ops = a.ops.clone();
    let prefix = a.key_prefix.clone();
    let value: String = "x".repeat(a.size);
    let mut pacer = Pacer {
        next: Instant::now(),
        every: Duration::from_secs(60) / a.max_per_min,
    };
    let slots = ["A", "B"];
    let build = |i: usize| {
        build_arm(arms[i], slots[i], api_key, api_url, macro_mode)
            .map(Arc::new)
            .map_err(|e| Stop::Fault(e.to_string()))
    };
    let mut pool = [build(0)?, build(1)?];
    let mut counter = 0usize;
    let blocks = 2 * samples / block;
    for b in 0..blocks {
        let i = ABBA[b % 4];
        sink.loadavg = loadavg();
        // A new client per block, warmed by one GET miss that writes nothing.
        // With one connection per arm for the whole run, the edge server each
        // connection landed on would be a fixed per-arm offset that resampling
        // blocks cannot see; a connection per block makes it block-level noise
        // the A/A floor includes. The warm-up also absorbs the handshake.
        pool[i] = build(i)?;
        pacer.wait(1).await;
        let warm_key = format!("{prefix}:warmup{}:{}", slots[i], short_id());
        let mut t = timed_op(&pool[i], Op::Get, &warm_key, "", ttl, true).await;
        t.op = "warmup";
        sink.row(&pool[i], b, 0, &t, None)?;
        let mut s = 0;
        while s < block {
            // Idle first, so an idle regime's sample is the first request after the gap.
            tokio::time::sleep(gap).await;
            if fresh {
                pool[i] = build(i)?;
            }
            if concurrency > 1 {
                // One burst: `concurrency` samples at once, cold then warm on the same pool.
                for step in ["cold", "warm"] {
                    let before = tls_sockets();
                    pacer
                        .wait(u32::try_from(concurrency * ops.len()).unwrap_or(u32::MAX))
                        .await;
                    let mut set = tokio::task::JoinSet::new();
                    // The stop rules judge a burst only once it is in, so after
                    // any failed request no task in the burst sends another.
                    let halt = Arc::new(AtomicBool::new(false));
                    for k in 0..concurrency {
                        counter += 1;
                        let key = format!("{prefix}:rs{}{counter}:{}", slots[i], short_id());
                        if ops.contains(&Op::Put) {
                            sink.ledger(&key)?;
                        }
                        let (arm, ops, value) = (pool[i].clone(), ops.clone(), value.clone());
                        let halt = halt.clone();
                        set.spawn(async move {
                            let mut out = Vec::new();
                            for op in ops {
                                if halt.load(Ordering::SeqCst) {
                                    break;
                                }
                                let t = timed_op(&arm, op, &key, &value, ttl, false).await;
                                if t.outcome.is_err() {
                                    halt.store(true, Ordering::SeqCst);
                                }
                                out.push(t);
                            }
                            (k, out)
                        });
                    }
                    let mut results = set.join_all().await;
                    results.sort_by_key(|(k, _)| *k);
                    let new = new_sockets(before.as_ref(), tls_sockets().as_ref()).unwrap_or(0);
                    for (k, rows) in results {
                        for t in rows {
                            sink.row(&pool[i], b, s + k, &t, Some((step, new)))?;
                        }
                    }
                }
                s += concurrency;
            } else {
                counter += 1;
                if macro_mode {
                    let id = format!("{prefix}:{counter}:{}", short_id());
                    sink.ledger(&macro_key(&id).map_err(|e| e.to_string())?)?;
                    pacer
                        .wait(u32::try_from(MACRO_REQUESTS).unwrap_or(u32::MAX))
                        .await;
                    let t = timed_cold_miss(&pool[i], &id).await;
                    sink.row(&pool[i], b, s, &t, None)?;
                } else {
                    let key = format!("{prefix}:rs{}{counter}:{}", slots[i], short_id());
                    for op in &ops {
                        if *op == Op::Put {
                            sink.ledger(&key)?;
                        }
                        pacer.wait(1).await;
                        let t = timed_op(&pool[i], *op, &key, &value, ttl, true).await;
                        sink.row(&pool[i], b, s, &t, None)?;
                    }
                }
                s += 1;
            }
        }
        // The round-trip floor, on the transport arm's own connection, after
        // the block so it never warms a sample.
        if let Some(wire) = pool[i].wire.clone() {
            for _ in 0..2 {
                pacer.wait(1).await;
                let before = tls_sockets();
                let started = iso_now();
                let t0 = Instant::now();
                let traced = wire.trace().await;
                let total = t0.elapsed();
                // A failed trace is a row like any other, under the same rules.
                let (outcome, exchange) = match traced {
                    Ok(ex) => (Ok(None), Some(ex)),
                    Err(e) => (Err(e.to_string()), None),
                };
                let t = Timed {
                    op: "trace",
                    method: "GET",
                    key: String::new(),
                    started,
                    ended: iso_now(),
                    total,
                    outcome,
                    new_conn: new_sockets(before.as_ref(), tls_sockets().as_ref()).map(|n| n > 0),
                    resolves: None,
                    exchange,
                    parts: Vec::new(),
                };
                sink.row(&pool[i], b, block, &t, None)?;
            }
        }
    }
    if macro_mode {
        // The macro may still be filling or unlocking in the background; let
        // it finish rather than leave a lock to expire on its TTL.
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    Ok(())
}

fn short_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..8].to_owned()
}

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let args = match parse_args(&argv) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("wall_time_probe: {e}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    let (Ok(api_key), Ok(api_url)) = (
        std::env::var("CACHEKIT_API_KEY"),
        std::env::var("CACHEKIT_API_URL"),
    ) else {
        eprintln!("wall_time_probe: set CACHEKIT_API_KEY and CACHEKIT_API_URL\n{USAGE}");
        return ExitCode::from(2);
    };
    let host = match url::Url::parse(&api_url) {
        Ok(u) => u.host_str().unwrap_or_default().to_owned(),
        Err(e) => {
            eprintln!("wall_time_probe: CACHEKIT_API_URL: {e}");
            return ExitCode::from(2);
        }
    };
    // A trailing dot names the same host; compare without it.
    if !WRITABLE_HOSTS.contains(&host.trim_end_matches('.')) {
        eprintln!(
            "wall_time_probe: refusing {host}: this harness writes and deletes keys, \
             so it runs only against {WRITABLE_HOSTS:?}"
        );
        return ExitCode::from(2);
    }
    if let Err(e) = validate_cachekitio_url(&api_url, true) {
        eprintln!("wall_time_probe: CACHEKIT_API_URL: {e}");
        return ExitCode::from(2);
    }
    let open = |p: &str| {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(p)
    };
    let (out, ledger) = match (open(&args.out), open(&args.ledger)) {
        (Ok(o), Ok(l)) => (o, l),
        (Err(e), _) | (_, Err(e)) => {
            eprintln!("wall_time_probe: cannot open --out/--ledger: {e}");
            return ExitCode::from(2);
        }
    };
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("wall_time_probe: tokio runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    let mut sink = Sink {
        args,
        host,
        out,
        ledger,
        loadavg: Value::Null,
        server_errors: 0,
        invocation: format!(
            "{}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
            std::process::id()
        ),
    };
    match rt.block_on(schedule(&mut sink, &api_key, &api_url)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(Stop::Refused(why)) => {
            eprintln!("wall_time_probe: STOPPED (no retry): {why}");
            ExitCode::from(3)
        }
        Err(Stop::Fault(why)) => {
            eprintln!("wall_time_probe: {why}");
            ExitCode::FAILURE
        }
    }
}
