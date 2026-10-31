//! CPU cost of the client hot path, gated on instruction counts.
//!
//! Drives the public `CacheKit` API over an in-memory [`Backend`], so it
//! measures the client's own work — key resolution, MessagePack, L1, the
//! reliability stack, AES-256-GCM — and no network. Wall time on a shared
//! host swings by tens of percent, so the gate counts instructions under
//! valgrind's callgrind, which is deterministic on one thread. The runtime is
//! tokio `current_thread`: work stealing would add scheduler noise.
//!
//! ```text
//! bench_hot_path list                          every case name
//! bench_hot_path run <case> <size> <iters>     one case; indicative wall ns/op
//! bench_hot_path wall [filter]                 every case; indicative wall ns/op
//! bench_hot_path readme                        the README's Performance table
//! bench_hot_path instr [--runs N] [--jobs N] [--base BIN [--report FILE]] [filter]
//! ```
//!
//! `instr` runs each case `N` times (default 5) under
//! `valgrind --tool=callgrind --collect-atstart=no --toggle-collect=<measured>`,
//! so only the timed loop is counted and setup needs no differencing. It
//! reports the median instructions per op and the run-to-run spread. With
//! `--base`, it runs a second build of this example interleaved (ABBA) and
//! calls a delta only when it beats `max(3 x spread, 0.5%)`; `delete` cases
//! are reported but never fail (see [`UNGATED`]). `--report` writes the
//! verdict as Markdown, for CI's PR comment, and `--jobs` measures that many
//! cases at a time. Exit codes: 1 when any gated case got slower by that much;
//! 2 when nothing was measured or the run stopped part way (an unknown flag, a
//! filter matching no case, or valgrind failing). Wall time is printed for
//! orientation and is never a measured saving.

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, DefaultHasher};
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use cachekit::backend::{Backend, HealthStatus};
use cachekit::reliability::ReliabilityConfig;
use cachekit::{BackendError, CacheKit, CachekitError};

const OPS: [&str; 6] = ["l1_hit", "l2_hit", "set", "delete", "exists", "oneshot"];
const MODES: [&str; 4] = ["plain", "rel", "enc", "prev"];
/// The current master key of every encrypting reader and every writer but
/// `prev`'s.
const KEY: [u8; 32] = [0x42; 32];
/// A retired master key. A `prev` reader holds it as its one previous key and
/// reads only entries written under it, so every read fails the current key
/// before the previous key decrypts it: the rotation read path.
const PREV_KEY: [u8; 32] = [0x41; 32];
/// Every encrypting client's tenant: a `prev` reader decrypts only what a
/// writer of the same tenant wrote.
const TENANT: &str = "bench";
/// Value length in characters. The value is a `String` of `x`, so its stored
/// MessagePack is 2 bytes longer from 32 to 255 characters: 40 stores 42 B,
/// the smallest case, where per-call fixed costs dominate; 64 stores 66 B.
const SIZES: [usize; 4] = [40, 64, 1024, 65536];
const TTL: Duration = Duration::from_secs(300);
const WARM: usize = 16;
/// Demangled name of the function callgrind counts (see [`measured`]).
const MEASURED: &str = "bench_hot_path::measured";

// ── In-memory backend ────────────────────────────────────────────────────────

/// `DefaultHasher::new()` has fixed keys, so the map probes the same way in
/// every process; `RandomState` would add run-to-run instruction spread.
/// Values are [`Bytes`] so a write keeps the client's buffer, as the
/// cachekit.io backend's request body does: every copy a write case pays is
/// the client's own. A read still copies the value out in `get`.
type FixedMap = HashMap<String, Bytes, BuildHasherDefault<DefaultHasher>>;

#[derive(Default)]
struct MemBackend {
    store: Mutex<FixedMap>,
}

impl MemBackend {
    fn map(&self) -> std::sync::MutexGuard<'_, FixedMap> {
        // A poisoned lock means an op already panicked; the run is void anyway.
        self.store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[async_trait]
impl Backend for MemBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        Ok(self.map().get(key).map(|v| v.to_vec()))
    }
    async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        self.set_bytes(key, Bytes::from(value), ttl).await
    }
    async fn set_bytes(
        &self,
        key: &str,
        value: Bytes,
        _ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        self.map().insert(key.to_owned(), value);
        Ok(())
    }
    async fn delete(&self, key: &str) -> Result<bool, BackendError> {
        Ok(self.map().remove(key).is_some())
    }
    async fn exists(&self, key: &str) -> Result<bool, BackendError> {
        Ok(self.map().contains_key(key))
    }
    async fn health(&self) -> Result<HealthStatus, BackendError> {
        Ok(HealthStatus {
            is_healthy: true,
            latency_ms: 0.0,
            backend_type: "mem".into(),
            details: HashMap::new(),
        })
    }
}

// ── Cases ────────────────────────────────────────────────────────────────────

/// One bench case: `<op>/<mode>`, e.g. `l2_hit/enc`.
struct Bench {
    op: &'static str,
    mode: &'static str,
    enc: bool,
    reader: CacheKit,
    backend: Arc<MemBackend>,
    keys: Vec<String>,
    value: String,
}

fn client(
    backend: &Arc<MemBackend>,
    mode: &str,
    l1_capacity: usize,
) -> Result<CacheKit, CachekitError> {
    let b = CacheKit::builder()
        .backend(backend.clone())
        .l1_capacity(l1_capacity);
    match mode {
        "rel" => b.reliability(ReliabilityConfig::default()),
        "enc" => b.encryption_from_bytes(&KEY, TENANT)?,
        "prev" => b.encryption_from_bytes_with_previous(&KEY, &[&PREV_KEY], TENANT)?,
        _ => b,
    }
    .build()
}

/// A client that writes what a `mode` reader reads: under [`PREV_KEY`] for
/// `prev`, otherwise as the reader would. Its L1 is small: it only writes.
fn writer(backend: &Arc<MemBackend>, mode: &str) -> Result<CacheKit, CachekitError> {
    if mode != "prev" {
        return client(backend, mode, 8);
    }
    CacheKit::builder()
        .backend(backend.clone())
        .l1_capacity(8)
        .encryption_from_bytes(&PREV_KEY, TENANT)?
        .build()
}

/// Whether `<op>/<mode>` is a case. `prev` is the rotation read path, so it
/// pairs only with reads; `oneshot` prices building an encrypting client, so
/// it pairs only with the encrypted modes.
fn is_case(op: &str, mode: &str) -> bool {
    match op {
        "oneshot" => matches!(mode, "enc" | "prev"),
        "l1_hit" | "l2_hit" => true,
        _ => mode != "prev",
    }
}

impl Bench {
    /// Build the clients and pre-populate every key the measured loop touches,
    /// so the loop itself does exactly `iters` identical ops.
    async fn setup(case: &str, size: usize, iters: usize) -> Result<Self, CachekitError> {
        let (op, mode) = case.split_once('/').unwrap_or((case, ""));
        let op = OPS
            .into_iter()
            .find(|o| *o == op)
            .ok_or_else(|| CachekitError::Config(format!("unknown op in case {case:?}")))?;
        let mode = MODES
            .into_iter()
            .find(|m| *m == mode)
            .ok_or_else(|| CachekitError::Config(format!("unknown mode in case {case:?}")))?;
        if !is_case(op, mode) {
            return Err(CachekitError::Config(format!(
                "{case:?} is not a case; `bench_hot_path list` names them"
            )));
        }
        let backend = Arc::new(MemBackend::default());
        // Room for every key twice over, so no op pays an eviction: L1 splits
        // its capacity into shards, and a shard sized exactly to its keys
        // evicts a few of them when the hash spreads them unevenly.
        let reader = client(&backend, mode, 2 * (iters + WARM) + 8)?;
        let keys: Vec<String> = (0..iters + WARM)
            .map(|i| format!("bench:{op}:{i:08}"))
            .collect();
        let bench = Self {
            op,
            mode,
            enc: matches!(mode, "enc" | "prev"),
            reader,
            backend,
            keys,
            value: "x".repeat(size),
        };
        match op {
            // L2 hits need the value in the backend but not in the reader's
            // L1: write through a second client sharing the backend.
            "l2_hit" => {
                let writer = writer(&bench.backend, mode)?;
                for k in &bench.keys {
                    write(&writer, bench.enc, k, &bench.value).await?;
                }
            }
            "delete" => {
                for k in &bench.keys {
                    write(&bench.reader, bench.enc, k, &bench.value).await?;
                }
            }
            // Seeds keys[0] under PREV_KEY. `l1_hit/prev`'s first read, in the
            // warm-up, is an L2 hit that fills its L1 with that ciphertext;
            // `oneshot/prev` reads it from the backend on every op.
            _ if mode == "prev" => {
                let writer = writer(&bench.backend, mode)?;
                write(&writer, true, &bench.keys[0], &bench.value).await?;
            }
            _ => write(&bench.reader, bench.enc, &bench.keys[0], &bench.value).await?,
        }
        Ok(bench)
    }

    async fn op(&self, i: usize) -> Result<(), CachekitError> {
        let key = match self.op {
            "l2_hit" | "delete" => &self.keys[i],
            _ => &self.keys[0],
        };
        match self.op {
            "set" => write(&self.reader, self.enc, key, &self.value).await,
            "delete" if self.enc => self.reader.secure_cache()?.delete(key).await.map(drop),
            "delete" => self.reader.delete(key).await.map(drop),
            // A warm key: answered from L1, so the backend is never asked.
            "exists" => {
                let found = if self.enc {
                    self.reader.secure_cache()?.exists(key).await?
                } else {
                    self.reader.exists(key).await?
                };
                found.then_some(()).ok_or_else(|| {
                    CachekitError::Config(format!("{key}: expected exists, got absent"))
                })
            }
            // One client build plus one read, which its empty L1 sends to the
            // backend: what a one-read process pays past start-up (the warm-up
            // has already paid first-use costs, and L1 holds 8 entries).
            "oneshot" => read(&client(&self.backend, self.mode, 8)?, true, key).await,
            _ => read(&self.reader, self.enc, key).await,
        }
    }
}

async fn read(cache: &CacheKit, enc: bool, key: &str) -> Result<(), CachekitError> {
    let got: Option<String> = if enc {
        cache.secure_cache()?.get(key).await?
    } else {
        cache.get(key).await?
    };
    // A miss would measure the wrong path; fail the run instead.
    got.map(|v| drop(black_box(v)))
        .ok_or_else(|| CachekitError::Config(format!("{key}: expected a hit, got a miss")))
}

async fn write(
    cache: &CacheKit,
    enc: bool,
    key: &str,
    value: &String,
) -> Result<(), CachekitError> {
    if enc {
        cache.secure_cache()?.set_with_ttl(key, value, TTL).await
    } else {
        cache.set_with_ttl(key, value, TTL).await
    }
}

/// The only code callgrind counts (`--toggle-collect`). Synchronous and never
/// inlined, so its symbol is stable and everything `block_on` runs inside it,
/// the runtime's polling included, is attributed to the loop.
#[inline(never)]
fn measured(
    rt: &tokio::runtime::Runtime,
    bench: &Bench,
    range: std::ops::Range<usize>,
) -> Result<(), CachekitError> {
    rt.block_on(async {
        for i in range {
            bench.op(black_box(i)).await?;
        }
        Ok(())
    })
}

/// Run one case and return its indicative wall time in ns per op.
fn run(case: &str, size: usize, iters: usize) -> Result<f64, CachekitError> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| CachekitError::Config(format!("tokio runtime: {e}")))?;
    let bench = rt.block_on(Bench::setup(case, size, iters))?;
    // Warm-up on keys of its own, outside the counted function: first-use
    // costs (session id, lazy statics) belong to no op.
    measured_warm(&rt, &bench, iters..iters + WARM)?;
    let start = Instant::now();
    measured(&rt, &bench, 0..iters)?;
    Ok(start.elapsed().as_nanos() as f64 / iters as f64)
}

/// Same loop as [`measured`] under another symbol, so callgrind skips it.
#[inline(never)]
fn measured_warm(
    rt: &tokio::runtime::Runtime,
    bench: &Bench,
    range: std::ops::Range<usize>,
) -> Result<(), CachekitError> {
    rt.block_on(async {
        for i in range {
            bench.op(i).await?;
        }
        Ok(())
    })
}

/// Every case whose `<op>/<mode>/<size>` contains `filter`. A filter that
/// matches nothing is an error: an empty gate would pass vacuously.
fn cases(filter: Option<&str>) -> Result<Vec<(String, usize)>, String> {
    let mut out = Vec::new();
    for op in OPS {
        for mode in MODES.into_iter().filter(|m| is_case(op, m)) {
            for size in SIZES {
                let case = format!("{op}/{mode}");
                if filter.is_none_or(|f| format!("{case}/{size}").contains(f)) {
                    out.push((case, size));
                }
            }
        }
    }
    if out.is_empty() {
        return Err(format!(
            "no case matches {filter:?}; `bench_hot_path list` names them"
        ));
    }
    Ok(out)
}

/// Iterations per callgrind run: enough that per-op counts dwarf any one-off
/// work left in the loop, few enough that a 64 KiB case finishes in seconds.
fn instr_iters(size: usize) -> usize {
    if size > 4096 {
        200
    } else {
        2000
    }
}

// ── README table ─────────────────────────────────────────────────────────────

/// Columns of the README's Performance table. At 64 KiB a case's time is
/// mostly copying and allocation, and repeat runs disagreed by up to 3.5x, so
/// the table stops at 1 KiB; `wall` still covers 64 KiB.
const README_SIZES: [usize; 2] = [64, 1024];

/// Rows of the README's Performance table: the read and write paths, plain
/// and encrypted.
const README_ROWS: [(&str, &str); 6] = [
    ("l1_hit/plain", "`get`, L1 hit"),
    ("l2_hit/plain", "`get`, L2 hit"),
    ("set/plain", "`set_with_ttl`"),
    ("l1_hit/enc", "`secure_cache()` `get`, L1 hit"),
    ("l2_hit/enc", "`secure_cache()` `get`, L2 hit"),
    ("set/enc", "`secure_cache()` `set_with_ttl`"),
];

/// "640 ns", "12.3 µs".
fn ns_cell(ns: f64) -> String {
    if ns < 1_000.0 {
        format!("{ns:.0} ns")
    } else {
        format!("{:.1} µs", ns / 1_000.0)
    }
}

/// Wall ns/op of one `run`, in a process of its own. In one process, a case's
/// figure at 64 KiB depends on what ran before it: earlier large allocations
/// move glibc's mmap and trim thresholds, and with them whether each op pays
/// page faults (2-4x either way). A fresh process gives each case the same start.
fn wall_in_new_process(bin: &str, case: &str, size: usize) -> Result<f64, String> {
    let iters = (20 * instr_iters(size)).to_string();
    let out = Command::new(bin)
        .args(["run", case, &size.to_string(), &iters])
        .stderr(Stdio::inherit())
        .output()
        .map_err(|e| format!("cannot start {bin}: {e}"))?;
    if !out.status.success() {
        return Err(format!("{bin} run {case} {size} failed: {}", out.status));
    }
    String::from_utf8_lossy(&out.stdout)
        .split_once("ns/op=")
        .and_then(|(_, ns)| ns.trim().parse().ok())
        .ok_or_else(|| format!("no ns/op in the output of {case} {size}"))
}

/// The README's Performance table as Markdown. Each cell is the median wall
/// time per op of five runs, each in a new process.
fn readme() -> Result<bool, String> {
    let me = std::env::current_exe()
        .map_err(|e| e.to_string())?
        .display()
        .to_string();
    println!("| Operation | 64 B | 1 KiB |");
    println!("|:----------|-----:|------:|");
    for (case, label) in README_ROWS {
        let mut cells = Vec::new();
        for size in README_SIZES {
            let runs = (0..5)
                .map(|_| wall_in_new_process(&me, case, size))
                .collect::<Result<Vec<f64>, _>>()?;
            cells.push(ns_cell(median(&runs)));
        }
        println!("| {label} | {} |", cells.join(" | "));
    }
    Ok(true)
}

// ── Instruction counting ─────────────────────────────────────────────────────

/// The smallest delta `instr --base` calls. A case whose runs disagree gets
/// `3 x` its spread instead, when that is larger.
const MIN_FLOOR: f64 = 0.005;

/// Ops measured and reported but never failed. A delete takes the per-key
/// mutation lock, whose map is seeded afresh in each process, so its count
/// spreads by up to about 10% between runs of one build and its floor would
/// hide any regression worth catching.
const UNGATED: [&str; 1] = ["delete"];

/// `instr --base` verdicts, in the order `--report` lists them.
const SLOWER: &str = "SLOWER";
const SLOWER_UNGATED: &str = "slower, not gated";
const FASTER: &str = "faster";
const NOISE: &str = "within noise";

fn instructions(
    bin: &str,
    case: &str,
    size: usize,
    iters: usize,
    seq: usize,
) -> Result<f64, String> {
    let out = std::env::temp_dir().join(format!(
        "bench_hot_path.{}.{seq}.callgrind",
        std::process::id()
    ));
    let status = Command::new("valgrind")
        .args(["--tool=callgrind", "--collect-atstart=no"])
        .arg(format!("--toggle-collect={MEASURED}"))
        .arg(format!("--callgrind-out-file={}", out.display()))
        .args([bin, "run", case, &size.to_string(), &iters.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|e| format!("cannot start valgrind (is it installed?): {e}"))?;
    let text = std::fs::read_to_string(&out);
    let _ = std::fs::remove_file(&out);
    if !status.success() {
        return Err(format!(
            "{bin} run {case} {size} failed under valgrind: {status}"
        ));
    }
    let text = text.map_err(|e| format!("no callgrind output for {case}: {e}"))?;
    let total: u64 = text
        .lines()
        .find_map(|l| l.strip_prefix("summary:"))
        .and_then(|v| v.trim().parse().ok())
        .ok_or_else(|| format!("no summary line in callgrind output for {case}"))?;
    if total == 0 {
        // The toggle matched nothing: the symbol moved and every count would be 0.
        return Err(format!(
            "callgrind counted 0 instructions in {MEASURED} for {case}"
        ));
    }
    Ok(total as f64 / iters as f64)
}

fn median(v: &[f64]) -> f64 {
    let mut s = v.to_vec();
    s.sort_by(f64::total_cmp);
    s[s.len() / 2]
}

/// Run-to-run spread as a fraction of the median: (max - min) / median.
fn spread(v: &[f64]) -> f64 {
    let (lo, hi) = v
        .iter()
        .fold((f64::MAX, f64::MIN), |(lo, hi), x| (lo.min(*x), hi.max(*x)));
    (hi - lo) / median(v)
}

/// Ir/op of `runs` runs of one case under each of `bins`. Runs interleave ABBA,
/// alternating which build goes first, so drift cancels.
fn measure(
    bins: &[&str],
    case: &str,
    size: usize,
    runs: usize,
    seq: &AtomicUsize,
) -> Result<Vec<Vec<f64>>, String> {
    let iters = instr_iters(size);
    let mut out = vec![Vec::new(); bins.len()];
    for r in 0..runs {
        let mut order: Vec<usize> = (0..bins.len()).collect();
        if r % 2 == 1 {
            order.reverse();
        }
        for i in order {
            let n = seq.fetch_add(1, Ordering::Relaxed);
            out[i].push(instructions(bins[i], case, size, iters, n)?);
        }
    }
    Ok(out)
}

/// Copies of the base and head builds at paths of equal length, removed on
/// drop. The path is the measured process's `argv[0]`, and its length moves
/// the stack and heap layout: the same binary run from a shorter path counted
/// 6 instructions more per `exists` op (+0.8%), every run.
struct Staged {
    dir: PathBuf,
    base: String,
    head: String,
}

impl Staged {
    fn new(base: &str, head: &str) -> Result<Self, String> {
        let dir = std::env::temp_dir().join(format!("bench_hot_path.{}", std::process::id()));
        let stage = Self {
            base: dir.join("base/bench_hot_path").display().to_string(),
            head: dir.join("head/bench_hot_path").display().to_string(),
            dir,
        };
        for (from, to) in [(base, &stage.base), (head, &stage.head)] {
            let to = Path::new(to);
            std::fs::create_dir_all(to.parent().unwrap_or(&stage.dir))
                .and_then(|()| std::fs::copy(from, to))
                .map_err(|e| format!("cannot copy {from} to {}: {e}", to.display()))?;
        }
        Ok(stage)
    }
}

impl Drop for Staged {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// One case of `instr --base`.
struct Compared {
    case: String,
    size: usize,
    base: f64,
    head: f64,
    delta: f64,
    floor: f64,
    verdict: &'static str,
}

impl Compared {
    fn new(case: &str, size: usize, base: &[f64], head: &[f64]) -> Self {
        let (mb, mh) = (median(base), median(head));
        let delta = (mh - mb) / mb;
        let floor = (3.0 * spread(base).max(spread(head))).max(MIN_FLOOR);
        let op = case.split('/').next().unwrap_or(case);
        let verdict = if delta.abs() < floor {
            NOISE
        } else if delta < 0.0 {
            FASTER
        } else if UNGATED.contains(&op) {
            SLOWER_UNGATED
        } else {
            SLOWER
        };
        Self {
            case: case.to_owned(),
            size,
            base: mb,
            head: mh,
            delta,
            floor,
            verdict,
        }
    }
}

/// Run `f` on every case, `jobs` cases at a time. `progress` sees the results
/// so far after each case lands. The first error stops new cases from
/// starting; it comes back with every result measured before the stop.
fn each_case<T: Send>(
    all: &[(String, usize)],
    jobs: usize,
    f: impl Fn(&str, usize) -> Result<T, String> + Sync,
    progress: impl Fn(&[Option<T>]) + Sync,
) -> (Vec<Option<T>>, Option<String>) {
    let next = AtomicUsize::new(0);
    let error: Mutex<Option<String>> = Mutex::new(None);
    let results: Mutex<Vec<Option<T>>> = Mutex::new(all.iter().map(|_| None).collect());
    std::thread::scope(|s| {
        for _ in 0..jobs {
            s.spawn(|| loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= all.len() || lock(&error).is_some() {
                    break;
                }
                let (case, size) = &all[i];
                match f(case, *size) {
                    Ok(v) => {
                        let mut r = lock(&results);
                        r[i] = Some(v);
                        progress(&r);
                    }
                    Err(e) => {
                        lock(&error).get_or_insert(e);
                        break;
                    }
                }
            });
        }
    });
    let results = results.into_inner().unwrap_or_else(PoisonError::into_inner);
    let error = error.into_inner().unwrap_or_else(PoisonError::into_inner);
    (results, error)
}

/// A worker that panicked poisons the lock; the scope re-raises its panic, so
/// the run ends either way.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// `instr --report`: the verdict, and every case that needs a look, as
/// Markdown for a PR comment. A case not measured, because the run is still
/// going or stopped, reads "no result" and makes the verdict INCOMPLETE: a
/// missing case can hide a regression, so it never reads as PASS.
fn render(all: &[(String, usize)], rows: &[Option<Compared>], error: Option<&str>) -> String {
    let measured: Vec<&Compared> = rows.iter().flatten().collect();
    let verdict = if measured.iter().any(|c| c.verdict == SLOWER) {
        "FAIL"
    } else if measured.len() < all.len() {
        "INCOMPLETE"
    } else {
        "PASS"
    };
    let mut out = format!(
        "### Instruction counts: {verdict}\n\n{} of {} cases measured.",
        measured.len(),
        all.len()
    );
    if let Some(e) = error {
        out += &format!(" The run stopped: {e}.");
    }
    out += &format!(
        " A case fails when it is slower than the base by its floor or more: the larger of \
         {:.1}% and 3 x the run-to-run spread either build showed. `{}` cases are reported, \
         never failed: their count moves with a hash seed drawn in each process.\n\n",
        100.0 * MIN_FLOOR,
        UNGATED.join("`, `")
    );
    let head =
        "| Verdict | Case | Base Ir/op | Head Ir/op | Delta | Floor |\n|---|---|--:|--:|--:|--:|\n";
    let line = |c: &Compared| {
        let v = if c.verdict == SLOWER {
            "**SLOWER**"
        } else {
            c.verdict
        };
        format!(
            "| {v} | `{}/{}` | {:.0} | {:.0} | {:+.2}% | {:.2}% |\n",
            c.case,
            c.size,
            c.base,
            c.head,
            100.0 * c.delta,
            100.0 * c.floor
        )
    };
    let mut look = String::new();
    for c in measured.iter().filter(|c| c.verdict == SLOWER) {
        look += &line(c);
    }
    for ((case, size), _) in all.iter().zip(rows).filter(|(_, r)| r.is_none()) {
        look += &format!("| **no result** | `{case}/{size}` | | | | |\n");
    }
    for v in [SLOWER_UNGATED, FASTER] {
        for c in measured.iter().filter(|c| c.verdict == v) {
            look += &line(c);
        }
    }
    if !look.is_empty() {
        out += head;
        out += &look;
        out += "\n";
    }
    let quiet: Vec<_> = measured.iter().filter(|c| c.verdict == NOISE).collect();
    if !quiet.is_empty() {
        out += &format!(
            "<details><summary>{} cases within noise</summary>\n\n{head}",
            quiet.len()
        );
        for c in quiet {
            out += &line(c);
        }
        out += "\n</details>\n";
    }
    out
}

fn instr(args: &[String]) -> Result<bool, String> {
    let mut runs = 5usize;
    let mut jobs = 1usize;
    let mut base: Option<String> = None;
    let mut report: Option<String> = None;
    let mut filter: Option<String> = None;
    let mut it = args.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--runs" => {
                runs = it
                    .next()
                    .and_then(|v| v.parse().ok())
                    .ok_or("--runs needs a number")?
            }
            "--jobs" => {
                jobs = it
                    .next()
                    .and_then(|v| v.parse().ok())
                    .filter(|n| *n > 0)
                    .ok_or("--jobs needs a number above 0")?
            }
            "--base" => base = Some(it.next().ok_or("--base needs a binary path")?.clone()),
            "--report" => report = Some(it.next().ok_or("--report needs a file path")?.clone()),
            f if f.starts_with("--") => return Err(format!("unknown flag {f}")),
            f => filter = Some(f.to_owned()),
        }
    }
    if runs < 3 {
        return Err("--runs must be at least 3: a spread needs a median".into());
    }
    let me = std::env::current_exe()
        .map_err(|e| e.to_string())?
        .display()
        .to_string();
    let all = cases(filter.as_deref())?;
    let seq = AtomicUsize::new(0);
    // Rows print as cases finish, so with --jobs above 1 they come out of order.
    let Some(base) = base else {
        if report.is_some() {
            return Err("--report needs --base: it reports a comparison".into());
        }
        println!("{:16} {:>6} {:>12} {:>9}", "case", "size", "Ir/op", "A/A");
        let (_, error) = each_case(
            &all,
            jobs,
            |case, size| {
                let v = measure(&[&me], case, size, runs, &seq)?;
                let (m, s) = (median(&v[0]), spread(&v[0]));
                println!("{case:16} {size:>6} {m:>12.0} {:>8.3}%", 100.0 * s);
                Ok(())
            },
            |_| {},
        );
        return error.map_or(Ok(true), Err);
    };
    let stage = Staged::new(&base, &me)?;
    let (base, me) = (stage.base.as_str(), stage.head.as_str());
    let write = |rows: &[Option<Compared>], error: Option<&str>| match &report {
        // Write then rename, so a run killed mid-write leaves the last whole
        // report rather than an empty file.
        Some(path) => {
            let part = format!("{path}.part");
            std::fs::write(&part, render(&all, rows, error))
                .and_then(|()| std::fs::rename(&part, path))
                .map_err(|e| format!("cannot write the report to {path}: {e}"))
        }
        None => Ok(()),
    };
    // Written before the first case and after each one, so a run killed part
    // way (a timeout) leaves a report that reads INCOMPLETE.
    let none: Vec<Option<Compared>> = all.iter().map(|_| None).collect();
    write(&none, None)?;
    println!(
        "{:16} {:>6} {:>12} {:>12} {:>8} {:>8}  verdict",
        "case", "size", "base Ir/op", "cand Ir/op", "delta", "floor"
    );
    let (rows, error) = each_case(
        &all,
        jobs,
        |case, size| {
            let v = measure(&[base, me], case, size, runs, &seq)?;
            let c = Compared::new(case, size, &v[0], &v[1]);
            println!(
                "{case:16} {size:>6} {:>12.0} {:>12.0} {:>+7.2}% {:>7.2}%  {}",
                c.base,
                c.head,
                100.0 * c.delta,
                100.0 * c.floor,
                c.verdict
            );
            Ok(c)
        },
        |rows| {
            if let Err(e) = write(rows, None) {
                eprintln!("bench_hot_path: {e}");
            }
        },
    );
    write(&rows, error.as_deref())?;
    match error {
        Some(e) => Err(e),
        None => Ok(!rows.iter().flatten().any(|c| c.verdict == SLOWER)),
    }
}

fn usage() -> ExitCode {
    eprintln!("usage: bench_hot_path list | run <case> <size> <iters> | wall [filter] | readme | instr [--runs N] [--jobs N] [--base BIN [--report FILE]] [filter]");
    ExitCode::from(2)
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result: Result<bool, String> = match args.first().map(String::as_str) {
        Some("list") => cases(None).map(|all| {
            for (case, size) in all {
                println!("{case}/{size}");
            }
            true
        }),
        Some("run") if args.len() == 4 => {
            let (Ok(size), Ok(iters)) = (args[2].parse(), args[3].parse()) else {
                return usage();
            };
            run(&args[1], size, iters)
                .map(|ns| println!("{} size={size} ns/op={ns:.1}", args[1]))
                .map(|()| true)
                .map_err(|e| e.to_string())
        }
        Some("wall") => {
            println!(
                "{:16} {:>6} {:>12}  (indicative wall time, not a measured saving)",
                "case", "size", "ns/op"
            );
            cases(args.get(1).map(String::as_str))
                .and_then(|all| {
                    all.into_iter().try_for_each(|(case, size)| {
                        let ns =
                            run(&case, size, 20 * instr_iters(size)).map_err(|e| e.to_string())?;
                        println!("{case:16} {size:>6} {ns:>12.0}");
                        Ok(())
                    })
                })
                .map(|()| true)
        }
        Some("readme") if args.len() == 1 => readme(),
        Some("instr") => instr(&args[1..]),
        _ => return usage(),
    };
    match result {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        // 2, not 1: a run that measured nothing must not read as a regression.
        Err(e) => {
            eprintln!("bench_hot_path: {e}");
            ExitCode::from(2)
        }
    }
}
