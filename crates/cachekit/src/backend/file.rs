//! Local filesystem backend, byte-compatible with cachekit-py's File backend.
//!
//! On-disk layout (shared cross-SDK contract — a py and an rs process pointed
//! at the same directory read each other's entries):
//!
//! - **Filename**: lowercase-hex Blake2b-128 (16-byte digest) of the UTF-8
//!   cache key — filesystem-safe, no directory traversal.
//! - **Header** (14 bytes): `b"CK"` magic `[0:2]`, format version `1` `[2:3]`,
//!   reserved `0` `[3:4]`, flags `u16` BE `[4:6]`, expiry unix-seconds `u64`
//!   BE `[6:14]` (`0` = never expires). Payload follows the header.
//! - **Writes** are atomic: payload lands in a unique `{name}.tmp.{pid}.…`
//!   file, fsynced, then renamed over the final path. Readers see the old or
//!   the new entry, never a torn one.
//! - **Expiry is lazy**: expired and corrupt entries are unlinked when a
//!   read touches them, exactly like cachekit-py.
//! - **Orphan sweep on first write**: temp files older than 60 s, left by
//!   crashed writers (py's or rs's — both use the `.tmp.` marker), are
//!   removed by each backend instance's first `set`, once. cachekit-py sweeps
//!   at startup instead; rs defers it because the sweep reads the whole
//!   directory, and a short-lived reader of a large cache (a CLI hit) would
//!   pay that on every run. Only writers leave orphans, so a reader-only
//!   process never needs it.
//!
//! ## Concurrency model
//!
//! - **In-process**: operations on the same key serialize on one of
//!   64 mutexes (`LOCK_STRIPES`), picked by the first byte of the entry's
//!   Blake2b-128 filename. This closes the expire-unlink vs. concurrent-set
//!   lost-write race within a process: that race only ever involves one entry
//!   path, so one key always maps to one stripe. Keys on different stripes
//!   do not wait on each other's fsync; two keys share a stripe about 1 time
//!   in 64 and then still serialize (cachekit-py serializes everything on
//!   one `threading.RLock`, because its RLock also guards LRU and size
//!   bookkeeping; rs has none yet — porting eviction or size caps adds shared
//!   cross-key state, and these stripes must be revisited then).
//! - **Cross-process** (unix): advisory `flock` — shared on reads, exclusive
//!   on the in-place expiry rewrite — matching py's `fcntl.flock` usage,
//!   non-blocking with contention surfacing as a timeout error like py.
//!   Unlinks are additionally **inode-validated**: an entry is only removed
//!   if the path still names the inode the decision was made on. Combined
//!   with the in-process lock this fully closes the stale-unlink race within
//!   a process; cross-process it narrows the exposure to the stat-to-unlink
//!   instruction window (a rename landing exactly there loses one fresh
//!   entry — a cache miss, never corruption — the same class py accepts).
//! - Remaining accepted exposure, identical to py: `refresh_ttl` racing a
//!   concurrent replace in *another process* returns `false` (refresh what
//!   was opened is no longer the live entry), and an in-place expiry rewrite
//!   torn by power loss yields a wrong expiry, never a corrupt payload.
//!
//! Deliberate v1 divergences from cachekit-py (documented, not silent):
//! LRU eviction / size caps / entry-count caps and the mmap zero-copy read
//! path are not implemented — the directory grows until entries expire or
//! the caller clears it.
//!
//! Requires a tokio runtime: file I/O runs on the blocking thread pool via
//! `spawn_blocking` (except under the `unsync` feature, where it runs inline
//! on the caller — the sync-in-async trade-off cachekit-py documents).

use std::collections::HashMap;
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

use crate::backend::{run_blocking, Backend, HealthStatus, TtlInspectable};
use crate::error::{BackendError, BackendErrorKind};

// Header layout — byte-identical to cachekit-py's `backends/file/backend.py`.
const MAGIC: &[u8; 2] = b"CK";
const FORMAT_VERSION: u8 = 1;
const HEADER_SIZE: usize = 14;
const EXPIRY_OFFSET: u64 = 6;

/// TTL ceiling (10 years), matching cachekit-py's `MAX_TTL_SECONDS` overflow guard.
const MAX_TTL_SECS: u64 = 10 * 365 * 24 * 60 * 60;

/// Orphaned temp files older than this are swept by a backend's first `set`
/// (py uses the same threshold, but sweeps at startup).
const TEMP_FILE_MAX_AGE: Duration = Duration::from_secs(60);

/// Process-global sequence folded into temp-file names so two writes of the
/// same key in the same nanosecond tick can never collide on the temp path.
static TEMP_SEQ: AtomicU64 = AtomicU64::new(0);

// ── Header encode/decode ─────────────────────────────────────────────────────

fn now_secs() -> u64 {
    // A pre-epoch clock degrades to "now = 0": nothing reads as expired.
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn build_header(expiry_unix_secs: u64) -> [u8; HEADER_SIZE] {
    let mut h = [0u8; HEADER_SIZE];
    h[0..2].copy_from_slice(MAGIC);
    h[2] = FORMAT_VERSION;
    // [3] reserved and [4..6] flags stay zero (py writes the same).
    h[6..14].copy_from_slice(&expiry_unix_secs.to_be_bytes());
    h
}

enum ParsedHeader {
    /// Short file, wrong magic, or unknown version — unlink on sight.
    Corrupt,
    /// Past its expiry timestamp — unlink on sight.
    Expired,
    /// Nonzero reserved byte or flags: a transform this reader does not
    /// implement. A miss that MUST NOT delete, rewrite, or return the payload.
    Unsupported,
    /// Readable entry; `expiry` is 0 for never-expires.
    Live { expiry: u64 },
}

/// `now` is the reader clock in Unix seconds (a parameter so tests can pin it).
fn parse_header(bytes: &[u8], now: u64) -> ParsedHeader {
    if bytes.len() < HEADER_SIZE || &bytes[0..2] != MAGIC || bytes[2] != FORMAT_VERSION {
        return ParsedHeader::Corrupt;
    }
    if bytes[3] != 0 || bytes[4..6] != [0, 0] {
        return ParsedHeader::Unsupported;
    }
    let mut expiry_be = [0u8; 8];
    expiry_be.copy_from_slice(&bytes[6..14]);
    let expiry = u64::from_be_bytes(expiry_be);
    // Expired once the clock reaches the deadline (spec: `now >= expiry`).
    if expiry > 0 && now >= expiry {
        return ParsedHeader::Expired;
    }
    ParsedHeader::Live { expiry }
}

/// Read exactly the 14-byte header (or as much as the file holds).
fn fill_header(file: &mut fs::File) -> std::io::Result<([u8; HEADER_SIZE], usize)> {
    let mut header = [0u8; HEADER_SIZE];
    let mut filled = 0;
    while filled < HEADER_SIZE {
        match file.read(&mut header[filled..]) {
            Ok(0) => break, // short file → parses as Corrupt
            Ok(n) => filled += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok((header, filled))
}

// ── Error mapping ─────────────────────────────────────────────────────────────

/// Mirror of cachekit-py's `_classify_os_error` retry semantics.
fn classify_io(e: &std::io::Error) -> BackendErrorKind {
    use std::io::ErrorKind as K;
    match e.kind() {
        // Disk full may clear up; a busy/locked file may too.
        K::StorageFull => BackendErrorKind::Transient,
        K::PermissionDenied => BackendErrorKind::Transient,
        K::ReadOnlyFilesystem => BackendErrorKind::Permanent,
        K::TimedOut => BackendErrorKind::Timeout,
        _ if is_symlink_loop(e) => BackendErrorKind::Permanent,
        _ => BackendErrorKind::Transient,
    }
}

/// ELOOP — `O_NOFOLLOW` refused a symlink. `io::ErrorKind::FilesystemLoop`
/// is still unstable (io_error_more), so match the raw errno.
fn is_symlink_loop(e: &std::io::Error) -> bool {
    #[cfg(unix)]
    {
        e.raw_os_error() == Some(libc::ELOOP)
    }
    #[cfg(not(unix))]
    {
        let _ = e;
        false
    }
}

fn file_err(e: std::io::Error, what: &str) -> BackendError {
    BackendError {
        kind: classify_io(&e),
        message: format!("{what}: {e}"),
        source: Some(Box::new(e)),
    }
}

/// `true` when an open failed because the entry is absent — or is a symlink
/// rejected by `O_NOFOLLOW`, which py also treats as "not found" rather than
/// following it (symlink-swap defence in shared tmp directories).
fn is_missing(e: &std::io::Error) -> bool {
    e.kind() == std::io::ErrorKind::NotFound || is_symlink_loop(e)
}

// ── Open / lock / unlink helpers ─────────────────────────────────────────────

fn open_opts(read: bool, write: bool) -> fs::OpenOptions {
    let mut opts = fs::OpenOptions::new();
    opts.read(read).write(write);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        // Refuse to follow a symlink planted at the entry path (py parity).
        opts.custom_flags(libc::O_NOFOLLOW);
    }
    opts
}

/// Advisory cross-process lock, matching py's non-blocking `fcntl.flock`:
/// shared for reads, exclusive for the in-place expiry rewrite. Contention
/// surfaces as a timeout error (py raises its TIMEOUT type). The lock is
/// released when the file handle drops. No-op off unix (py falls back to
/// msvcrt there; rs has no Windows locking yet — in-process serialization
/// still applies).
fn flock_nb(file: &fs::File, exclusive: bool) -> Result<(), BackendError> {
    #[cfg(unix)]
    {
        use rustix::fs::{flock, FlockOperation};
        let op = if exclusive {
            FlockOperation::NonBlockingLockExclusive
        } else {
            FlockOperation::NonBlockingLockShared
        };
        flock(file, op).map_err(|errno| {
            if errno == rustix::io::Errno::WOULDBLOCK || errno == rustix::io::Errno::AGAIN {
                BackendError::timeout("file lock contended (held by another process)")
            } else {
                file_err(std::io::Error::from(errno), "failed to lock cache file")
            }
        })
    }
    #[cfg(not(unix))]
    {
        let _ = (file, exclusive);
        Ok(())
    }
}

fn unlink_quiet(path: &Path) {
    let _ = fs::remove_file(path);
}

/// Remove `path` only if it still names the inode `opened` was read from.
///
/// A stale-decision unlink (entry looked expired/corrupt on the fd we hold)
/// must never delete a *fresh* entry a concurrent writer just renamed over
/// the path — validate device+inode first. Off unix there is no inode API;
/// the in-process lock still serializes same-process races and we fall back
/// to a plain unlink (matching py's Windows behaviour).
fn unlink_if_same_inode(path: &Path, opened: &fs::File) {
    if still_linked(path, opened) {
        unlink_quiet(path);
    }
}

/// `true` if `path` still names the inode `opened` was read from (unix).
/// Off unix, assume yes (in-process lock covers same-process races).
fn still_linked(path: &Path, opened: &fs::File) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let (Ok(on_disk), Ok(held)) = (fs::symlink_metadata(path), opened.metadata()) else {
            return false;
        };
        on_disk.dev() == held.dev() && on_disk.ino() == held.ino()
    }
    #[cfg(not(unix))]
    {
        let _ = (path, opened);
        true
    }
}

// ── Sync cores (run on the blocking pool, under the key's stripe lock) ───────

fn read_entry(path: &Path) -> Result<Option<Vec<u8>>, BackendError> {
    let mut file = match open_opts(true, false).open(path) {
        Ok(f) => f,
        Err(e) if is_missing(&e) => return Ok(None),
        Err(e) => return Err(file_err(e, "failed to open cache file")),
    };
    flock_nb(&file, false)?;
    let mut data = Vec::new();
    file.read_to_end(&mut data)
        .map_err(|e| file_err(e, "failed to read cache file"))?;

    match parse_header(&data, now_secs()) {
        ParsedHeader::Corrupt | ParsedHeader::Expired => {
            unlink_if_same_inode(path, &file);
            Ok(None)
        }
        ParsedHeader::Unsupported => Ok(None),
        ParsedHeader::Live { .. } => Ok(Some(data.split_off(HEADER_SIZE))),
    }
}

/// Header-only probe used by `exists` and `ttl`. Corrupt/expired entries are
/// unlinked (inode-validated) and reported as absent, mirroring py.
fn probe_header(path: &Path) -> Result<Option<u64>, BackendError> {
    let mut file = match open_opts(true, false).open(path) {
        Ok(f) => f,
        Err(e) if is_missing(&e) => return Ok(None),
        Err(e) => return Err(file_err(e, "failed to open cache file")),
    };
    flock_nb(&file, false)?;
    let (header, filled) =
        fill_header(&mut file).map_err(|e| file_err(e, "failed to read cache file header"))?;
    match parse_header(&header[..filled], now_secs()) {
        ParsedHeader::Corrupt | ParsedHeader::Expired => {
            unlink_if_same_inode(path, &file);
            Ok(None)
        }
        ParsedHeader::Unsupported => Ok(None),
        ParsedHeader::Live { expiry } => Ok(Some(expiry)),
    }
}

fn write_entry(path: &Path, header: [u8; HEADER_SIZE], value: &[u8]) -> Result<(), BackendError> {
    let temp = temp_path(path);

    // Creation failure means we own nothing on disk — never unlink here, or
    // an EEXIST loser would delete another writer's in-flight temp file.
    let mut opts = open_opts(false, true);
    opts.create_new(true); // O_CREAT | O_EXCL — never reuse a planted file
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600); // owner-only, py parity
    }
    let mut file = opts
        .open(&temp)
        .map_err(|e| file_err(e, "failed to create cache temp file"))?;

    // From here the temp file is ours: clean it up on any failure.
    let write_all = |file: &mut fs::File| -> std::io::Result<()> {
        file.write_all(&header)?;
        file.write_all(value)?;
        // Data must be durable before the rename makes it visible.
        file.sync_all()
    };
    let result = write_all(&mut file).and_then(|()| {
        drop(file);
        fs::rename(&temp, path)
    });
    result.map_err(|e| {
        unlink_quiet(&temp);
        file_err(e, "failed to write cache file")
    })
}

fn delete_entry(path: &Path) -> Result<bool, BackendError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(file_err(e, "failed to delete cache file")),
    }
}

fn entry_ttl(path: &Path) -> Result<Option<Duration>, BackendError> {
    match probe_header(path)? {
        // Missing/expired/corrupt — or 0 = never expires: exists but has no
        // TTL to report (py parity).
        None | Some(0) => Ok(None),
        Some(expiry) => Ok(Some(Duration::from_secs(expiry.saturating_sub(now_secs())))),
    }
}

fn rewrite_expiry(path: &Path, new_expiry: u64) -> Result<bool, BackendError> {
    let mut file = match open_opts(true, true).open(path) {
        Ok(f) => f,
        Err(e) if is_missing(&e) => return Ok(false),
        Err(e) => return Err(file_err(e, "failed to open cache file")),
    };
    flock_nb(&file, true)?;

    let (header, filled) =
        fill_header(&mut file).map_err(|e| file_err(e, "failed to read cache file header"))?;
    match parse_header(&header[..filled], now_secs()) {
        ParsedHeader::Corrupt | ParsedHeader::Expired => {
            // An expired entry is absent, not refreshable (py parity).
            unlink_if_same_inode(path, &file);
            return Ok(false);
        }
        // Never rewrite an entry we cannot interpret.
        ParsedHeader::Unsupported => return Ok(false),
        ParsedHeader::Live { .. } => {}
    }

    // If a concurrent writer replaced the entry since we opened it, our fd
    // points at an orphaned inode — writing there would report success while
    // the live entry keeps its old expiry (silent early eviction). Refuse:
    // `false` honestly says "what you opened is no longer the live entry".
    if !still_linked(path, &file) {
        return Ok(false);
    }

    // Overwrite ONLY the 8-byte expiry field. Same trade-off py documents: a
    // torn write on power loss yields a wrong expiry, never a corrupt
    // payload — magic, version, and data are untouched.
    let do_write = |f: &mut fs::File| -> std::io::Result<()> {
        f.seek(SeekFrom::Start(EXPIRY_OFFSET))?;
        f.write_all(&new_expiry.to_be_bytes())?;
        f.sync_all()
    };
    do_write(&mut file).map_err(|e| file_err(e, "failed to refresh cache file TTL"))?;
    Ok(true)
}

fn temp_path(target: &Path) -> PathBuf {
    // `{name}.tmp.{pid}.{nanos}.{seq}` — contains the `.tmp.` marker py's
    // startup sweep and rs's first-write sweep look for, so a shared
    // directory gets cross-SDK orphan cleanup. The process-global sequence makes the name collision-proof
    // even for same-key writes landing in the same nanosecond tick.
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let seq = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let name = target
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    target.with_file_name(format!("{name}.tmp.{}.{nanos}.{seq}", std::process::id()))
}

/// Best-effort sweep of orphaned temp files older than [`TEMP_FILE_MAX_AGE`].
fn cleanup_temp_files(cache_dir: &Path) {
    let Ok(entries) = fs::read_dir(cache_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        if !name.to_string_lossy().contains(".tmp.") {
            continue;
        }
        // symlink_metadata never follows links — a planted symlink is skipped.
        let Ok(meta) = entry.path().symlink_metadata() else {
            continue;
        };
        if !meta.is_file() {
            continue;
        }
        let stale = meta
            .modified()
            .ok()
            .and_then(|m| m.elapsed().ok())
            .is_some_and(|age| age > TEMP_FILE_MAX_AGE);
        if stale {
            unlink_quiet(&entry.path());
        }
    }
}

// ── FileBackend ───────────────────────────────────────────────────────────────

/// Local filesystem cache backend.
///
/// See the [module docs](self) for the on-disk format, the concurrency
/// model, and the deliberate v1 divergences from cachekit-py.
#[derive(Debug, Clone)]
pub struct FileBackend {
    cache_dir: PathBuf,
    /// Per-key op serialization, striped by filename. Clones share them.
    /// Async so contended waiters cost a queued future, not a parked
    /// blocking-pool thread (a hot backend must not monopolize the pool).
    stripes: Arc<[Arc<tokio::sync::Mutex<()>>; LOCK_STRIPES]>,
    /// Whether this backend has swept orphaned temp files yet; the first
    /// `set` does it. Clones share it, so an instance sweeps once.
    swept: Arc<AtomicBool>,
}

/// Number of in-process lock stripes (a power of two, at most 256: the stripe
/// is the low bits of the filename's first byte).
const LOCK_STRIPES: usize = 64;

fn new_stripes() -> Arc<[Arc<tokio::sync::Mutex<()>>; LOCK_STRIPES]> {
    Arc::new(std::array::from_fn(|_| {
        Arc::new(tokio::sync::Mutex::new(()))
    }))
}

/// Stripe for an entry path: the first byte of its lowercase-hex Blake2b-128
/// filename, so every op on one key takes the same stripe.
fn stripe_of(path: &Path) -> usize {
    path.file_name()
        .and_then(|n| n.to_str())
        .and_then(|n| n.get(..2))
        .and_then(|b| u8::from_str_radix(b, 16).ok())
        .map_or(0, |b| usize::from(b) % LOCK_STRIPES)
}

impl FileBackend {
    /// Start building a [`FileBackend`].
    pub fn builder() -> FileBackendBuilder {
        FileBackendBuilder::default()
    }

    /// The resolved directory this backend stores entries in.
    #[must_use]
    pub fn cache_dir(&self) -> &Path {
        &self.cache_dir
    }

    /// Blake2b-128 hex of the key — the same digest `tracing` events carry as
    /// `key_hash`, so a log line names the file it touched.
    fn entry_path(&self, key: &str) -> PathBuf {
        self.cache_dir.join(crate::metrics::key_hash(key))
    }

    /// Run `f` on the blocking pool holding lock stripe `stripe` (see
    /// [`stripe_of`]).
    ///
    /// The lock is acquired in async context BEFORE the hop to the blocking
    /// pool: N contended ops queue as futures instead of parking N blocking
    /// threads on a mutex (which would starve every other `spawn_blocking`
    /// user in the process). The owned guard rides into the closure and
    /// releases when the I/O finishes.
    async fn locked<T: Send + 'static>(
        &self,
        stripe: usize,
        f: impl FnOnce() -> Result<T, BackendError> + Send + 'static,
    ) -> Result<T, BackendError> {
        let guard = Arc::clone(&self.stripes[stripe]).lock_owned().await;
        run_blocking(move || {
            let _guard = guard;
            f()
        })
        .await
    }
}

// ── Backend impl ──────────────────────────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
#[cfg_attr(not(feature = "unsync"), async_trait)]
#[cfg_attr(feature = "unsync", async_trait(?Send))]
impl Backend for FileBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        let path = self.entry_path(key);
        self.locked(stripe_of(&path), move || read_entry(&path))
            .await
    }

    async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        // None = never expires (expiry field 0). Sub-second TTLs round up to
        // 1s (Redis-backend parity); absurd TTLs are rejected like py.
        // The deadline is stored in whole seconds rounded down, as the spec,
        // py and ts write it, so an N-second TTL lives N-1 to N seconds.
        // Rounding up here would make rs entries outlive py/ts ones by 1s.
        let expiry = match ttl {
            None => 0,
            Some(d) => {
                let secs = d.as_secs().max(1);
                if secs > MAX_TTL_SECS {
                    return Err(BackendError::permanent(format!(
                        "TTL {secs}s out of range [1, {MAX_TTL_SECS}] (max 10 years)"
                    )));
                }
                now_secs().saturating_add(secs)
            }
        };

        if !self.swept.load(Ordering::Relaxed) {
            // No stripe: the sweep touches only temp files past the orphan
            // threshold, never an entry, and one stripe could not fence the
            // other stripes' writes anyway. Swapped inside the closure, so
            // only a sweep that actually runs uses up the flag; once running,
            // it finishes even if this set is cancelled.
            let swept = Arc::clone(&self.swept);
            let dir = self.cache_dir.clone();
            run_blocking(move || {
                if !swept.swap(true, Ordering::Relaxed) {
                    cleanup_temp_files(&dir);
                }
                Ok(())
            })
            .await?;
        }

        let path = self.entry_path(key);
        self.locked(stripe_of(&path), move || {
            write_entry(&path, build_header(expiry), &value)
        })
        .await
    }

    async fn delete(&self, key: &str) -> Result<bool, BackendError> {
        let path = self.entry_path(key);
        self.locked(stripe_of(&path), move || delete_entry(&path))
            .await
    }

    async fn exists(&self, key: &str) -> Result<bool, BackendError> {
        let path = self.entry_path(key);
        self.locked(stripe_of(&path), move || Ok(probe_header(&path)?.is_some()))
            .await
    }

    async fn health(&self) -> Result<HealthStatus, BackendError> {
        let start = std::time::Instant::now();

        // Write/read/delete round-trip — same probe as cachekit-py.
        let key = "__health_check__";
        let probe = b"health_check_data".to_vec();
        self.set(key, probe.clone(), Some(Duration::from_secs(60)))
            .await?;
        let read_back = self.get(key).await?;
        self.delete(key).await?;

        let round_trip_ok = read_back.as_deref() == Some(probe.as_slice());
        let latency = start.elapsed();

        let mut details = HashMap::new();
        details.insert(
            "cache_dir".to_string(),
            self.cache_dir.display().to_string(),
        );
        if !round_trip_ok {
            details.insert(
                "error".to_string(),
                "round-trip verification failed".to_string(),
            );
        }
        Ok(HealthStatus {
            is_healthy: round_trip_ok,
            latency_ms: latency.as_secs_f64() * 1000.0,
            backend_type: "file".to_string(),
            details,
        })
    }
}

// ── TtlInspectable impl ───────────────────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
#[cfg_attr(not(feature = "unsync"), async_trait)]
#[cfg_attr(feature = "unsync", async_trait(?Send))]
impl TtlInspectable for FileBackend {
    async fn ttl(&self, key: &str) -> Result<Option<Duration>, BackendError> {
        let path = self.entry_path(key);
        self.locked(stripe_of(&path), move || entry_ttl(&path))
            .await
    }

    async fn refresh_ttl(&self, key: &str, ttl: Duration) -> Result<bool, BackendError> {
        let secs = ttl.as_secs();
        // Duration::ZERO is rejected, matching the CachekitIO backend — in
        // this SDK "no expiry" is spelled `set(.., None)`, not a zero TTL
        // (py's `refresh_ttl(key, 0)` making an entry permanent is py's
        // spelling of the same contract).
        if secs == 0 {
            return Err(BackendError::permanent(
                "refresh_ttl requires at least 1 second".to_string(),
            ));
        }
        if secs > MAX_TTL_SECS {
            return Err(BackendError::permanent(format!(
                "TTL {secs}s out of range [1, {MAX_TTL_SECS}] (max 10 years)"
            )));
        }

        let new_expiry = now_secs().saturating_add(secs);
        let path = self.entry_path(key);
        self.locked(stripe_of(&path), move || rewrite_expiry(&path, new_expiry))
            .await
    }
}

// ── Builder ───────────────────────────────────────────────────────────────────

/// Builder for [`FileBackend`].
#[derive(Default)]
#[must_use]
pub struct FileBackendBuilder {
    cache_dir: Option<PathBuf>,
}

impl FileBackendBuilder {
    /// Set the cache directory (default: `<system temp dir>/cachekit`, the
    /// same default as cachekit-py).
    pub fn cache_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.cache_dir = Some(dir.into());
        self
    }

    /// Create the cache directory if needed and construct a [`FileBackend`].
    ///
    /// On unix, a **pre-existing** directory must be owned by the current
    /// user and not group/other-writable — a default like `/tmp/cachekit`
    /// pre-created by another local user would otherwise let them poison
    /// plaintext cache entries (CWE-377). Does not scan the directory: the
    /// sweep of orphaned temp files runs on the backend's first `set` (see the
    /// [module docs](self)). The one-time directory setup does its I/O
    /// inline; per-operation I/O runs on the blocking pool.
    ///
    /// # Errors
    ///
    /// Returns a config error if the directory cannot be created or
    /// resolved, is not owned by the current user, or is group/other-writable.
    pub fn build(self) -> Result<FileBackend, crate::error::CachekitError> {
        use crate::error::CachekitError;

        let dir = self
            .cache_dir
            .unwrap_or_else(|| std::env::temp_dir().join("cachekit"));

        let create = || -> std::io::Result<()> {
            let mut builder = fs::DirBuilder::new();
            builder.recursive(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700); // owner-only, py parity
            }
            builder.create(&dir)
        };
        create().map_err(|e| {
            CachekitError::Config(format!(
                "failed to create cache directory {}: {e}",
                dir.display()
            ))
        })?;

        // Resolve symlinks once up front (py resolves via realpath per key).
        let cache_dir = fs::canonicalize(&dir).map_err(|e| {
            CachekitError::Config(format!(
                "failed to resolve cache directory {}: {e}",
                dir.display()
            ))
        })?;

        // Ownership/mode gate for pre-existing directories (unix).
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let meta = fs::metadata(&cache_dir).map_err(|e| {
                CachekitError::Config(format!(
                    "failed to stat cache directory {}: {e}",
                    cache_dir.display()
                ))
            })?;
            let euid = rustix::process::geteuid().as_raw();
            if meta.uid() != euid {
                return Err(CachekitError::Config(format!(
                    "cache directory {} is owned by uid {} (expected {euid}) — refusing a \
                     directory another local user controls",
                    cache_dir.display(),
                    meta.uid()
                )));
            }
            if meta.mode() & 0o022 != 0 {
                return Err(CachekitError::Config(format!(
                    "cache directory {} is group/other-writable (mode {:o}) — another local \
                     user could plant or replace cache entries; chmod it to 0700",
                    cache_dir.display(),
                    meta.mode() & 0o777
                )));
            }
        }

        Ok(FileBackend {
            cache_dir,
            stripes: new_stripes(),
            swept: Arc::new(AtomicBool::new(false)),
        })
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)] // test-only: failures should panic loudly
mod tests {
    use super::*;

    /// Compile-time proof that FileBackend implements TtlInspectable.
    fn _assert_ttl_inspectable(_b: &dyn TtlInspectable) {}

    #[test]
    fn file_is_ttl_inspectable() {
        fn _check(backend: &FileBackend) {
            _assert_ttl_inspectable(backend);
        }
    }

    fn backend_at(dir: &Path) -> FileBackend {
        FileBackend {
            cache_dir: dir.to_path_buf(),
            stripes: new_stripes(),
            swept: Arc::new(AtomicBool::new(false)),
        }
    }

    #[test]
    fn filename_hash_matches_py_blake2b16_hex() {
        // cachekit-py: hashlib.blake2b(b"key", digest_size=16).hexdigest()
        let backend = backend_at(Path::new("/cache"));
        let path = backend.entry_path("ns:app:func:m.f:args:abc:v1");
        assert_eq!(
            path,
            PathBuf::from("/cache/bcb35ae6f64fa65b2770ab3af631b1ce")
        );
    }

    #[test]
    fn header_layout_is_py_byte_compatible() {
        let header = build_header(0x0102_0304_0506_0708);
        assert_eq!(&header[0..2], b"CK");
        assert_eq!(header[2], 1); // version
        assert_eq!(header[3], 0); // reserved
        assert_eq!(&header[4..6], &[0, 0]); // flags u16 BE
        assert_eq!(&header[6..14], &[1, 2, 3, 4, 5, 6, 7, 8]); // expiry u64 BE
    }

    // ── On-disk format: vectors generated by cachekit-py's header builder ────
    // (b"CK" + version + reserved + struct.pack(">H", 0) + struct.pack(">Q", expiry))

    fn write_raw(dir: &Path, name: &str, hex_bytes: &str) -> PathBuf {
        let path = dir.join(name);
        fs::write(&path, hex::decode(hex_bytes).unwrap()).unwrap();
        path
    }

    #[test]
    fn py_written_never_expires_entry_reads_back() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_raw(
            dir.path(),
            "entry",
            "434b01000000000000000000000068656c6c6f2066726f6d20707974686f6e",
        );
        let payload = read_entry(&path).expect("read should not error");
        assert_eq!(payload.as_deref(), Some(b"hello from python".as_slice()));
        assert!(path.exists(), "live entry must not be unlinked");
    }

    #[test]
    fn py_written_future_expiry_entry_reads_back() {
        let dir = tempfile::tempdir().expect("tempdir");
        // expiry 4102444800 = 2100-01-01
        let path = write_raw(
            dir.path(),
            "entry",
            "434b0100000000000000f48657006672657368",
        );
        let payload = read_entry(&path).expect("read should not error");
        assert_eq!(payload.as_deref(), Some(b"fresh".as_slice()));

        let remaining = entry_ttl(&path)
            .expect("ttl should not error")
            .expect("future expiry must report a TTL");
        assert!(remaining > Duration::from_secs(3600), "TTL: {remaining:?}");
    }

    #[test]
    fn py_written_expired_entry_is_unlinked_on_read() {
        let dir = tempfile::tempdir().expect("tempdir");
        // expiry 1 = 1970-01-01T00:00:01Z
        let path = write_raw(
            dir.path(),
            "entry",
            "434b0100000000000000000000017374616c65",
        );
        assert_eq!(read_entry(&path).expect("read should not error"), None);
        assert!(!path.exists(), "expired entry must be unlinked on read");
    }

    #[test]
    fn corrupt_and_short_entries_are_unlinked_on_read() {
        let dir = tempfile::tempdir().expect("tempdir");

        // Wrong magic ("XX"), otherwise well-formed.
        let bad_magic = write_raw(dir.path(), "bad-magic", "585801000000000000000000000061");
        assert_eq!(read_entry(&bad_magic).expect("no error"), None);
        assert!(!bad_magic.exists());

        // Unknown format version (2).
        let bad_version = write_raw(dir.path(), "bad-version", "434b02000000000000000000000061");
        assert_eq!(read_entry(&bad_version).expect("no error"), None);
        assert!(!bad_version.exists());

        // Shorter than the 14-byte header.
        let short = write_raw(dir.path(), "short", "434b01");
        assert_eq!(read_entry(&short).expect("no error"), None);
        assert!(!short.exists());
    }

    // ── Protocol vectors ─────────────────────────────────────────────────────
    //
    // `tests/vectors/file-backend.json`, vendored verbatim from
    // cachekit-io/protocol `test-vectors/file-backend.json` v1.1.0
    // (sha256 `8d9d8c4709baf9ef3a8f2d71d21fb5a56207bc7a05c9bc7a967ac567f2604615`).
    // Do not edit the JSON here; regenerate upstream and re-vendor. Lives in
    // this module, not `tests/`, because `parse_header` is crate-private.

    const FILE_BACKEND_VECTORS: &str = include_str!("../../tests/vectors/file-backend.json");

    fn file_backend_vectors() -> Vec<serde_json::Value> {
        let doc: serde_json::Value =
            serde_json::from_str(FILE_BACKEND_VECTORS).expect("vector file parses");
        assert_eq!(
            doc["version"], "1.1.0",
            "re-vendored vectors: update the pin"
        );
        doc["vectors"].as_array().expect("vectors array").clone()
    }

    #[test]
    #[allow(clippy::panic)] // test-only: an unknown reader_action must fail loudly
    fn file_backend_vectors_read_as_specified() {
        let vectors = file_backend_vectors();
        assert_eq!(vectors.len(), 5);
        for v in &vectors {
            let name = v["name"].as_str().expect("name");
            let file_hex = v["file_hex"].as_str().expect("file_hex");
            let original = hex::decode(file_hex).expect("file_hex is hex");
            let action = v["reader_action"].as_str().expect("reader_action");

            for probe in ["read_entry", "probe_header"] {
                let dir = tempfile::tempdir().expect("tempdir");
                let path = write_raw(dir.path(), "entry", file_hex);
                let found = match probe {
                    "read_entry" => {
                        let got = read_entry(&path).expect("read should not error");
                        if action == "return_payload" {
                            let want = hex::decode(v["payload_hex"].as_str().unwrap()).unwrap();
                            assert_eq!(got.as_deref(), Some(want.as_slice()), "{name}/{probe}");
                        }
                        got.is_some()
                    }
                    _ => probe_header(&path)
                        .expect("probe should not error")
                        .is_some(),
                };
                match action {
                    "return_payload" | "miss_preserve" => {
                        assert_eq!(found, action == "return_payload", "{name}/{probe}");
                        assert_eq!(
                            fs::read(&path).expect("file must remain"),
                            original,
                            "{name}/{probe}: file must be byte-for-byte unchanged"
                        );
                    }
                    // The spec lets a reader lazily remove an expired entry.
                    "miss_expired" => assert!(!found, "{name}/{probe}"),
                    other => panic!("{name}: unknown reader_action {other}"),
                }
            }

            // Evaluate at the vector's exact clock, where one is given.
            if let Some(now) = v["reader_now_unix_seconds"].as_u64() {
                let parsed = parse_header(&original, now);
                match action {
                    "miss_expired" => {
                        assert!(matches!(parsed, ParsedHeader::Expired), "{name} at {now}")
                    }
                    "miss_preserve" => {
                        assert!(
                            matches!(parsed, ParsedHeader::Unsupported),
                            "{name} at {now}"
                        )
                    }
                    _ => assert!(
                        matches!(parsed, ParsedHeader::Live { .. }),
                        "{name} at {now}"
                    ),
                }
            }
        }
    }

    #[test]
    fn unsupported_header_is_a_miss_on_every_path_and_never_touched() {
        for v in file_backend_vectors() {
            if v["reader_action"] != "miss_preserve" {
                continue;
            }
            let name = v["name"].as_str().unwrap();
            let file_hex = v["file_hex"].as_str().unwrap();
            let original = hex::decode(file_hex).unwrap();
            let dir = tempfile::tempdir().expect("tempdir");
            let path = write_raw(dir.path(), "entry", file_hex);

            assert_eq!(read_entry(&path).expect("get"), None, "{name}: get");
            assert_eq!(probe_header(&path).expect("exists"), None, "{name}: exists");
            assert_eq!(entry_ttl(&path).expect("ttl"), None, "{name}: ttl");
            assert!(
                !rewrite_expiry(&path, now_secs() + 3600).expect("refresh_ttl"),
                "{name}: refresh_ttl"
            );
            assert_eq!(
                fs::read(&path).expect("file must remain"),
                original,
                "{name}"
            );
        }
    }

    #[test]
    fn expiry_is_reached_at_the_deadline_second() {
        let header = build_header(1_000);
        assert!(matches!(
            parse_header(&header, 999),
            ParsedHeader::Live { expiry: 1_000 }
        ));
        assert!(matches!(
            parse_header(&header, 1_000),
            ParsedHeader::Expired
        ));
        assert!(matches!(
            parse_header(&header, 1_001),
            ParsedHeader::Expired
        ));
        // 0 = never expires, whatever the clock says.
        assert!(matches!(
            parse_header(&build_header(0), u64::MAX),
            ParsedHeader::Live { expiry: 0 }
        ));
    }

    #[test]
    fn corrupt_header_wins_over_flags() {
        // Wrong version with a nonzero flag is still corrupt (unlinkable):
        // the flag check runs after magic and version.
        let mut header = build_header(0);
        header[2] = 2;
        header[5] = 1;
        assert!(matches!(parse_header(&header, 0), ParsedHeader::Corrupt));
    }

    #[test]
    fn ttl_semantics_match_py() {
        let dir = tempfile::tempdir().expect("tempdir");

        // Never-expires: exists, but reports no TTL.
        let never = write_raw(
            dir.path(),
            "never",
            "434b01000000000000000000000068656c6c6f2066726f6d20707974686f6e",
        );
        assert_eq!(entry_ttl(&never).expect("no error"), None);
        assert!(never.exists());

        // Expired: absent, unlinked.
        let expired = write_raw(
            dir.path(),
            "expired",
            "434b0100000000000000000000017374616c65",
        );
        assert_eq!(entry_ttl(&expired).expect("no error"), None);
        assert!(!expired.exists());

        // Missing: absent, no error.
        assert_eq!(entry_ttl(&dir.path().join("nope")).expect("no error"), None);
    }

    #[test]
    fn rewrite_expiry_semantics_match_py() {
        let dir = tempfile::tempdir().expect("tempdir");

        // Live entry: refreshed in place — payload untouched, expiry moved.
        let live = write_raw(dir.path(), "live", "434b0100000000000000f48657006672657368");
        let new_expiry = now_secs() + 42;
        assert!(rewrite_expiry(&live, new_expiry).expect("no error"));
        let remaining = entry_ttl(&live).expect("no error").expect("has TTL");
        assert!(remaining <= Duration::from_secs(42));
        assert_eq!(
            read_entry(&live).expect("no error").as_deref(),
            Some(b"fresh".as_slice()),
            "payload must survive an in-place expiry rewrite"
        );

        // Expired entry: treated as absent and unlinked.
        let expired = write_raw(
            dir.path(),
            "expired",
            "434b0100000000000000000000017374616c65",
        );
        assert!(!rewrite_expiry(&expired, new_expiry).expect("no error"));
        assert!(!expired.exists());

        // Missing entry: false, no error.
        assert!(!rewrite_expiry(&dir.path().join("nope"), new_expiry).expect("no error"));
    }

    /// A `.tmp.` orphan two minutes old: past the sweep's 60 s threshold.
    fn plant_stale_temp(dir: &Path) -> PathBuf {
        let stale = dir.join("abc.tmp.123.456");
        fs::write(&stale, b"orphan").expect("write");
        fs::File::options()
            .write(true)
            .open(&stale)
            .expect("open")
            .set_modified(SystemTime::now() - Duration::from_secs(120))
            .expect("set mtime");
        stale
    }

    #[test]
    fn temp_file_sweep_removes_stale_keeps_fresh() {
        let dir = tempfile::tempdir().expect("tempdir");

        let stale = plant_stale_temp(dir.path());

        let fresh = dir.path().join("def.tmp.123.789");
        fs::write(&fresh, b"in flight").expect("write");

        let entry = dir.path().join("0123abcd");
        fs::write(&entry, b"not a temp file").expect("write");

        cleanup_temp_files(dir.path());

        assert!(!stale.exists(), "stale temp file must be swept");
        assert!(fresh.exists(), "fresh temp file must be kept");
        assert!(entry.exists(), "cache entries must never be swept");
    }

    #[tokio::test]
    async fn orphan_sweep_runs_on_first_set_not_on_build() {
        // A short-lived reader (a CLI hit) must not pay a whole-directory
        // scan: build and reads leave orphans alone, the first write sweeps.
        let dir = tempfile::tempdir().expect("tempdir");
        let cache_dir = dir.path().join("cache");
        let build = || {
            FileBackend::builder()
                .cache_dir(&cache_dir)
                .build()
                .expect("build")
        };
        drop(build()); // creates the directory owner-only

        let stale = plant_stale_temp(&cache_dir);

        let backend = build();
        assert!(stale.exists(), "build must not scan the directory");

        assert!(backend.get("k").await.expect("get").is_none());
        assert!(backend.exists("k").await.is_ok());
        assert!(stale.exists(), "reads must not sweep");

        // Clones share the once-per-instance flag: one sweep, not one per clone.
        let clone = backend.clone();
        clone.set("k", b"v".to_vec(), None).await.expect("set");
        assert!(!stale.exists(), "first set must sweep stale orphans");
        assert!(
            backend.swept.load(Ordering::Relaxed),
            "clones share the flag"
        );
    }

    // A set parked on its stripe needs the blocking pool; `unsync` runs I/O inline.
    #[cfg(not(feature = "unsync"))]
    #[tokio::test]
    async fn first_set_sweeps_without_waiting_for_its_stripe() {
        // The sweep touches only temp files past the orphan threshold, never
        // an entry, so it must neither wait for nor hold the key's stripe.
        let (_dir, backend) = private_backend();
        let stale = plant_stale_temp(backend.cache_dir());
        let stripe = stripe_of(&backend.entry_path("k"));
        let held = Arc::clone(&backend.stripes[stripe]).lock_owned().await;

        let set = {
            let b = backend.clone();
            tokio::spawn(async move { b.set("k", b"v".to_vec(), None).await })
        };
        tokio::time::timeout(Duration::from_secs(5), async {
            while stale.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the sweep must not wait for the set's stripe");
        assert!(
            !set.is_finished(),
            "the write itself still waits for its stripe"
        );

        drop(held);
        set.await.expect("join").expect("set");
    }

    // ── Hardening ────────────────────────────────────────────────────────────

    #[cfg(unix)]
    #[test]
    fn builder_rejects_group_or_other_writable_directory() {
        use std::os::unix::fs::PermissionsExt;

        // CWE-377: a pre-existing world/group-writable cache dir (e.g. a
        // planted /tmp/cachekit) lets another local user poison plaintext
        // entries — build() must refuse it.
        let dir = tempfile::tempdir().expect("tempdir");
        let target = dir.path().join("shared");
        fs::create_dir(&target).expect("mkdir");
        fs::set_permissions(&target, fs::Permissions::from_mode(0o770)).expect("chmod");

        let err = FileBackend::builder()
            .cache_dir(&target)
            .build()
            .expect_err("group-writable dir must be rejected");
        assert!(err.to_string().contains("writable"), "{err}");

        // Owner-only passes.
        fs::set_permissions(&target, fs::Permissions::from_mode(0o700)).expect("chmod");
        FileBackend::builder()
            .cache_dir(&target)
            .build()
            .expect("0700 dir must be accepted");
    }

    #[test]
    fn temp_paths_never_collide() {
        let target = Path::new("/cache/abc");
        let a = temp_path(target);
        let b = temp_path(target);
        assert_ne!(a, b, "same-key temp paths must be unique");
        assert!(a.to_string_lossy().contains(".tmp."), "py sweep pattern");
    }

    #[cfg(unix)]
    #[test]
    fn stale_unlink_decision_spares_replaced_entry() {
        // Stale-unlink race: a get() that decided "expired" must not delete
        // the fresh entry a concurrent set() renamed over the path.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_raw(
            dir.path(),
            "entry",
            "434b0100000000000000000000017374616c65", // expired
        );

        // Open the expired inode (the stale decision's fd)…
        let stale_fd = open_opts(true, false).open(&path).expect("open");

        // …then a "concurrent writer" replaces the entry (new inode).
        write_entry(&path, build_header(0), b"fresh").expect("replace");

        // The stale unlink must be refused: path now names a different inode.
        unlink_if_same_inode(&path, &stale_fd);
        assert_eq!(
            read_entry(&path).expect("no error").as_deref(),
            Some(b"fresh".as_slice()),
            "inode-validated unlink must spare the replaced entry"
        );

        // Same guard for refresh: the stale fd is no longer the live entry.
        assert!(!still_linked(&path, &stale_fd));
        let live_fd = open_opts(true, false).open(&path).expect("open");
        assert!(still_linked(&path, &live_fd));
    }

    #[cfg(unix)]
    #[test]
    fn flock_contention_surfaces_as_timeout() {
        use rustix::fs::{flock, FlockOperation};

        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_raw(
            dir.path(),
            "entry",
            "434b01000000000000000000000068656c6c6f2066726f6d20707974686f6e",
        );

        // Another "process" holds an exclusive lock…
        let holder = fs::File::open(&path).expect("open");
        flock(&holder, FlockOperation::NonBlockingLockExclusive).expect("lock");

        // …so our shared-lock read errors as a timeout (py parity), it does
        // not block and does not misreport the entry as missing.
        let err = read_entry(&path).expect_err("contended read must error");
        assert_eq!(err.kind, BackendErrorKind::Timeout, "{err}");
    }

    /// A 0700 tempdir and a backend built on it through the public builder.
    fn private_backend() -> (tempfile::TempDir, FileBackend) {
        let dir = tempfile::tempdir().expect("tempdir");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).expect("chmod");
        }
        let backend = FileBackend::builder()
            .cache_dir(dir.path())
            .build()
            .expect("build");
        (dir, backend)
    }

    #[test]
    fn stripe_is_the_filename_first_byte() {
        // Pinned digest from filename_hash_matches_py_blake2b16_hex: 0xbc… → 0xbc % 64.
        let backend = backend_at(Path::new("/cache"));
        assert_eq!(
            stripe_of(&backend.entry_path("ns:app:func:m.f:args:abc:v1")),
            0xbc % LOCK_STRIPES
        );
        for i in 0..1000 {
            let key = format!("k{i}");
            let name = crate::metrics::key_hash(&key);
            let first = u8::from_str_radix(&name[..2], 16).expect("hex");
            assert_eq!(
                stripe_of(&backend.entry_path(&key)),
                usize::from(first) % LOCK_STRIPES
            );
        }
    }

    // A blocking op held mid-flight needs the blocking pool; `unsync` runs I/O inline.
    #[cfg(not(feature = "unsync"))]
    #[tokio::test]
    async fn an_in_flight_op_blocks_its_key_and_no_other() {
        let (_dir, backend) = private_backend();
        let held = "held";
        let held_stripe = stripe_of(&backend.entry_path(held));
        let other = (0..)
            .map(|i| format!("other{i}"))
            .find(|k| stripe_of(&backend.entry_path(k)) != held_stripe)
            .expect("a key on another stripe");
        backend.set(held, b"h".to_vec(), None).await.expect("set");
        backend.set(&other, b"o".to_vec(), None).await.expect("set");

        // Stand in for a write to `held` stuck in its fsync: an op that takes
        // whatever lock the backend uses for `held` and parks until released.
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let in_flight = {
            let b = backend.clone();
            tokio::spawn(async move {
                b.locked(held_stripe, move || {
                    let _ = started_tx.send(());
                    let _ = release_rx.recv();
                    Ok(())
                })
                .await
            })
        };
        started_rx.await.expect("in-flight op started");

        let other_get = tokio::time::timeout(Duration::from_secs(5), backend.get(&other))
            .await
            .expect("a read of another key must not wait on an unrelated in-flight write");
        assert_eq!(other_get.expect("get").as_deref(), Some(b"o".as_slice()));
        assert!(
            tokio::time::timeout(Duration::from_millis(100), backend.get(held))
                .await
                .is_err(),
            "same-key ops must still serialize behind the in-flight op"
        );

        release_tx.send(()).expect("release");
        in_flight.await.expect("join").expect("in-flight op");
        assert_eq!(
            backend.get(held).await.expect("get").as_deref(),
            Some(b"h".as_slice())
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn interleaved_cross_key_set_get_loses_no_update() {
        // Many keys, so tasks land on shared and distinct stripes alike: every
        // read sees its own task's latest write, and every final value is the
        // last one written.
        let (_dir, backend) = private_backend();
        let tasks: Vec<_> = (0..16)
            .map(|t| {
                let b = backend.clone();
                tokio::spawn(async move {
                    let key = format!("task{t}");
                    for round in 0..25u32 {
                        let value = format!("{t}:{round}").into_bytes();
                        b.set(&key, value.clone(), None).await.expect("set");
                        assert_eq!(
                            b.get(&key).await.expect("get"),
                            Some(value),
                            "task {t} round {round}: update lost"
                        );
                    }
                })
            })
            .collect();
        for task in tasks {
            task.await.expect("join");
        }
        for t in 0..16 {
            assert_eq!(
                backend.get(&format!("task{t}")).await.expect("get"),
                Some(format!("{t}:24").into_bytes())
            );
        }
    }

    #[tokio::test]
    async fn expired_read_racing_fresh_set_never_loses_the_write() {
        // The stale-unlink race, end to end through the public API: an expired
        // entry is read (deciding to unlink) while a fresh set replaces it.
        // Whatever the interleaving, the fresh value must survive.
        let dir = tempfile::tempdir().expect("tempdir");
        // tempfile dirs follow the umask (e.g. 0775) — the builder's
        // ownership/mode gate would rightly reject that; make it 0700.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).expect("chmod");
        }
        let backend = FileBackend::builder()
            .cache_dir(dir.path())
            .build()
            .expect("build");
        let key = "raced";
        let path = backend.entry_path(key);

        for round in 0..50 {
            // Plant an already-expired entry directly (py-format bytes).
            fs::write(
                &path,
                hex::decode("434b0100000000000000000000017374616c65").unwrap(),
            )
            .expect("plant expired entry");

            let reader = {
                let b = backend.clone();
                tokio::spawn(async move { b.get(key).await })
            };
            let writer = {
                let b = backend.clone();
                tokio::spawn(async move { b.set(key, b"fresh".to_vec(), None).await })
            };
            reader.await.expect("join").expect("get");
            writer.await.expect("join").expect("set");

            assert_eq!(
                backend.get(key).await.expect("get").as_deref(),
                Some(b"fresh".as_slice()),
                "round {round}: fresh write lost to a stale expired-unlink"
            );
        }
    }
}
