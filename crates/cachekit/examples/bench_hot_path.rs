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
//! bench_hot_path instr [--runs N] [--base BIN] [filter]
//! ```
//!
//! `instr` runs each case `N` times (default 5) under
//! `valgrind --tool=callgrind --collect-atstart=no --toggle-collect=<measured>`,
//! so only the timed loop is counted and setup needs no differencing. It
//! reports the median instructions per op and the run-to-run spread. With
//! `--base`, it runs a second build of this example interleaved (ABBA) and
//! calls a delta only when it beats `max(3 x spread, 0.5%)`. Exit codes: 1 when
//! any case got slower by that much; 2 when nothing was measured (an unknown
//! flag, a filter matching no case, or valgrind failing). Wall time is printed
//! for orientation and is never a measured saving.

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, DefaultHasher};
use std::hint::black_box;
use std::process::{Command, ExitCode, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use cachekit::backend::{Backend, HealthStatus};
use cachekit::reliability::ReliabilityConfig;
use cachekit::{BackendError, CacheKit, CachekitError};

const OPS: [&str; 6] = ["l1_hit", "l2_hit", "set", "delete", "exists", "oneshot"];
const MODES: [&str; 4] = ["plain", "rel", "enc", "prev"];
/// The master key every encrypting client writes under.
const KEY: [u8; 32] = [0x42; 32];
/// A retired master key. A `prev` reader holds it as its one previous key and
/// reads only entries written under it, so every read fails the current key
/// before the previous key decrypts it: the rotation read path.
const PREV_KEY: [u8; 32] = [0x41; 32];
const SIZES: [usize; 3] = [64, 1024, 65536];
const TTL: Duration = Duration::from_secs(300);
const WARM: usize = 16;
/// Demangled name of the function callgrind counts (see [`measured`]).
const MEASURED: &str = "bench_hot_path::measured";

// ── In-memory backend ────────────────────────────────────────────────────────

/// `DefaultHasher::new()` has fixed keys, so the map probes the same way in
/// every process; `RandomState` would add run-to-run instruction spread.
type FixedMap = HashMap<String, Vec<u8>, BuildHasherDefault<DefaultHasher>>;

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
        Ok(self.map().get(key).cloned())
    }
    async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
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
        "enc" => b.encryption_from_bytes(&KEY, "bench")?,
        "prev" => b.encryption_from_bytes_with_previous(&KEY, &[&PREV_KEY], "bench")?,
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
        .encryption_from_bytes(&PREV_KEY, "bench")?
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
            enc: mode != "plain" && mode != "rel",
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
            // The reader's first read, in the warm-up, is an L2 hit that
            // fills its L1 with the previous key's ciphertext.
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
            // A process that does one cached read: build the client, whose
            // empty L1 sends the read to the backend.
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

fn instr(args: &[String]) -> Result<bool, String> {
    let mut runs = 5usize;
    let mut base: Option<String> = None;
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
            "--base" => base = Some(it.next().ok_or("--base needs a binary path")?.clone()),
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
    let mut regressed = false;
    let mut seq = 0;
    match &base {
        None => println!("{:16} {:>6} {:>12} {:>9}", "case", "size", "Ir/op", "A/A"),
        Some(_) => println!(
            "{:16} {:>6} {:>12} {:>12} {:>8} {:>8}  verdict",
            "case", "size", "base Ir/op", "cand Ir/op", "delta", "floor"
        ),
    }
    for (case, size) in all {
        let iters = instr_iters(size);
        let (mut a, mut b) = (Vec::new(), Vec::new());
        for r in 0..runs {
            seq += 1;
            let Some(base) = &base else {
                b.push(instructions(&me, &case, size, iters, seq)?);
                continue;
            };
            // ABBA: alternate which build goes first, so drift cancels.
            let order = if r % 2 == 0 {
                [base.as_str(), me.as_str()]
            } else {
                [me.as_str(), base.as_str()]
            };
            for bin in order {
                let v = instructions(bin, &case, size, iters, seq)?;
                if bin == base.as_str() {
                    a.push(v)
                } else {
                    b.push(v)
                }
            }
        }
        if base.is_none() {
            println!(
                "{case:16} {size:>6} {:>12.0} {:>8.3}%",
                median(&b),
                100.0 * spread(&b)
            );
            continue;
        }
        let (ma, mb) = (median(&a), median(&b));
        let delta = (mb - ma) / ma;
        let floor = (3.0 * spread(&a).max(spread(&b))).max(0.005);
        let verdict = if delta.abs() < floor {
            "within noise"
        } else if delta < 0.0 {
            "faster"
        } else {
            regressed = true;
            "SLOWER"
        };
        println!(
            "{case:16} {size:>6} {ma:>12.0} {mb:>12.0} {:>+7.2}% {:>7.2}%  {verdict}",
            100.0 * delta,
            100.0 * floor
        );
    }
    Ok(!regressed)
}

fn usage() -> ExitCode {
    eprintln!("usage: bench_hot_path list | run <case> <size> <iters> | wall [filter] | readme | instr [--runs N] [--base BIN] [filter]");
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
