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
//! - **Writes** are atomic: payload lands in `{name}.tmp.{pid}.{nanos}`,
//!   fsynced, then renamed over the final path. Readers see the old or the
//!   new entry, never a torn one.
//! - **Expiry is lazy**: expired and corrupt entries are unlinked when a
//!   read touches them, exactly like cachekit-py.
//!
//! Deliberate v1 divergences from cachekit-py (documented, not silent):
//! LRU eviction / size caps / entry-count caps and the mmap zero-copy read
//! path are not implemented — the directory grows until entries expire or
//! the caller clears it. py's advisory `flock` is also skipped: writes are
//! rename-atomic without it, and the only in-place mutation
//! ([`refresh_ttl`](TtlInspectable::refresh_ttl)) rewrites a single 8-byte
//! field, the same torn-write exposure py accepts on power loss.
//!
//! Requires a tokio runtime: every operation runs its file I/O on the
//! blocking thread pool via `spawn_blocking`, so the async executor never
//! stalls on disk.

use std::collections::HashMap;
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use blake2::{digest::consts::U16, Blake2b, Digest};

use crate::backend::{Backend, HealthStatus, TtlInspectable};
use crate::error::{BackendError, BackendErrorKind};

type Blake2b128 = Blake2b<U16>;

// Header layout — byte-identical to cachekit-py's `backends/file/backend.py`.
const MAGIC: &[u8; 2] = b"CK";
const FORMAT_VERSION: u8 = 1;
const HEADER_SIZE: usize = 14;
const EXPIRY_OFFSET: u64 = 6;

/// TTL ceiling (10 years), matching cachekit-py's `MAX_TTL_SECONDS` overflow guard.
const MAX_TTL_SECS: u64 = 10 * 365 * 24 * 60 * 60;

/// Orphaned temp files older than this are swept at build time (py parity).
const TEMP_FILE_MAX_AGE: Duration = Duration::from_secs(60);

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
    /// Readable entry; `expiry` is 0 for never-expires.
    Live { expiry: u64 },
}

fn parse_header(bytes: &[u8]) -> ParsedHeader {
    if bytes.len() < HEADER_SIZE || &bytes[0..2] != MAGIC || bytes[2] != FORMAT_VERSION {
        return ParsedHeader::Corrupt;
    }
    let mut expiry_be = [0u8; 8];
    expiry_be.copy_from_slice(&bytes[6..14]);
    let expiry = u64::from_be_bytes(expiry_be);
    if expiry > 0 && now_secs() > expiry {
        return ParsedHeader::Expired;
    }
    ParsedHeader::Live { expiry }
}

// ── Error mapping ─────────────────────────────────────────────────────────────

/// Mirror of cachekit-py's `_classify_os_error` retry semantics.
fn classify_io(e: &std::io::Error, is_directory: bool) -> BackendErrorKind {
    use std::io::ErrorKind as K;
    match e.kind() {
        // Disk full may clear up; a locked/busy file may too. Permission
        // denied on the cache *directory* will not fix itself.
        K::StorageFull => BackendErrorKind::Transient,
        K::PermissionDenied if is_directory => BackendErrorKind::Permanent,
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

fn file_err(e: std::io::Error, what: &str, is_directory: bool) -> BackendError {
    BackendError {
        kind: classify_io(&e, is_directory),
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

// ── Open helpers ──────────────────────────────────────────────────────────────

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

fn unlink_quiet(path: &Path) {
    let _ = fs::remove_file(path);
}

// ── Sync cores (run on the blocking pool) ─────────────────────────────────────

fn read_entry(path: &Path) -> Result<Option<Vec<u8>>, BackendError> {
    let mut file = match open_opts(true, false).open(path) {
        Ok(f) => f,
        Err(e) if is_missing(&e) => return Ok(None),
        Err(e) => return Err(file_err(e, "failed to open cache file", false)),
    };
    let mut data = Vec::new();
    file.read_to_end(&mut data)
        .map_err(|e| file_err(e, "failed to read cache file", false))?;

    match parse_header(&data) {
        ParsedHeader::Corrupt | ParsedHeader::Expired => {
            unlink_quiet(path);
            Ok(None)
        }
        ParsedHeader::Live { .. } => Ok(Some(data.split_off(HEADER_SIZE))),
    }
}

fn read_header(path: &Path) -> Result<Option<ParsedHeader>, BackendError> {
    let mut file = match open_opts(true, false).open(path) {
        Ok(f) => f,
        Err(e) if is_missing(&e) => return Ok(None),
        Err(e) => return Err(file_err(e, "failed to open cache file", false)),
    };
    let mut header = [0u8; HEADER_SIZE];
    let mut filled = 0;
    while filled < HEADER_SIZE {
        match file.read(&mut header[filled..]) {
            Ok(0) => break, // short file → Corrupt below
            Ok(n) => filled += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(file_err(e, "failed to read cache file header", false)),
        }
    }
    Ok(Some(parse_header(&header[..filled])))
}

fn write_entry(path: &Path, header: [u8; HEADER_SIZE], value: &[u8]) -> Result<(), BackendError> {
    let temp = temp_path(path);

    let write_all = || -> std::io::Result<()> {
        let mut opts = open_opts(false, true);
        opts.create_new(true); // O_CREAT | O_EXCL — never reuse a planted file
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600); // owner-only, py parity
        }
        let mut file = opts.open(&temp)?;
        file.write_all(&header)?;
        file.write_all(value)?;
        // Data must be durable before the rename makes it visible.
        file.sync_all()?;
        drop(file);
        fs::rename(&temp, path)
    };

    write_all().map_err(|e| {
        unlink_quiet(&temp);
        file_err(e, "failed to write cache file", false)
    })
}

fn delete_entry(path: &Path) -> Result<bool, BackendError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(file_err(e, "failed to delete cache file", false)),
    }
}

fn entry_ttl(path: &Path) -> Result<Option<Duration>, BackendError> {
    match read_header(path)? {
        None => Ok(None),
        Some(ParsedHeader::Corrupt | ParsedHeader::Expired) => {
            unlink_quiet(path);
            Ok(None)
        }
        // 0 = never expires: exists but has no TTL to report (py parity).
        Some(ParsedHeader::Live { expiry: 0 }) => Ok(None),
        Some(ParsedHeader::Live { expiry }) => {
            Ok(Some(Duration::from_secs(expiry.saturating_sub(now_secs()))))
        }
    }
}

fn rewrite_expiry(path: &Path, new_expiry: u64) -> Result<bool, BackendError> {
    let mut file = match open_opts(true, true).open(path) {
        Ok(f) => f,
        Err(e) if is_missing(&e) => return Ok(false),
        Err(e) => return Err(file_err(e, "failed to open cache file", false)),
    };

    let mut header = [0u8; HEADER_SIZE];
    let mut filled = 0;
    while filled < HEADER_SIZE {
        match file.read(&mut header[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(file_err(e, "failed to read cache file header", false)),
        }
    }
    match parse_header(&header[..filled]) {
        ParsedHeader::Corrupt | ParsedHeader::Expired => {
            // An expired entry is absent, not refreshable (py parity).
            unlink_quiet(path);
            return Ok(false);
        }
        ParsedHeader::Live { .. } => {}
    }

    // Overwrite ONLY the 8-byte expiry field. Same trade-off py documents: a
    // torn write yields a wrong expiry, never a corrupt payload — magic,
    // version, and data are untouched, so the entry just expires early/late.
    let do_write = |f: &mut fs::File| -> std::io::Result<()> {
        f.seek(SeekFrom::Start(EXPIRY_OFFSET))?;
        f.write_all(&new_expiry.to_be_bytes())?;
        f.sync_all()
    };
    do_write(&mut file).map_err(|e| file_err(e, "failed to refresh cache file TTL", false))?;
    Ok(true)
}

fn temp_path(target: &Path) -> PathBuf {
    // `{name}.tmp.{pid}.{nanos}` — the exact pattern py's startup sweep
    // globs for, so a shared directory gets cross-SDK orphan cleanup.
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let name = target
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    target.with_file_name(format!("{name}.tmp.{}.{nanos}", std::process::id()))
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
/// See the [module docs](self) for the on-disk format and the deliberate v1
/// divergences from cachekit-py.
#[derive(Debug, Clone)]
pub struct FileBackend {
    cache_dir: PathBuf,
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

    fn entry_path(&self, key: &str) -> PathBuf {
        let mut hasher = Blake2b128::new();
        hasher.update(key.as_bytes());
        self.cache_dir.join(hex::encode(hasher.finalize()))
    }
}

#[cfg(not(feature = "unsync"))]
async fn run_blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, BackendError> + Send + 'static,
) -> Result<T, BackendError> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| BackendError::permanent(format!("file backend blocking task failed: {e}")))?
}

/// `unsync` opts into single-threaded runtimes and drops `Send` from
/// `BackendError`, so results cannot cross `spawn_blocking`. Run the fast
/// local-disk I/O inline instead — the same sync-in-async trade-off
/// cachekit-py documents for its File backend.
#[cfg(feature = "unsync")]
async fn run_blocking<T>(f: impl FnOnce() -> Result<T, BackendError>) -> Result<T, BackendError> {
    f()
}

// ── Backend impl ──────────────────────────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
#[cfg_attr(not(feature = "unsync"), async_trait)]
#[cfg_attr(feature = "unsync", async_trait(?Send))]
impl Backend for FileBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        let path = self.entry_path(key);
        run_blocking(move || read_entry(&path)).await
    }

    async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        // None = never expires (expiry field 0). Sub-second TTLs round up to
        // 1s (Redis-backend parity); absurd TTLs are rejected like py.
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

        let path = self.entry_path(key);
        run_blocking(move || write_entry(&path, build_header(expiry), &value)).await
    }

    async fn delete(&self, key: &str) -> Result<bool, BackendError> {
        let path = self.entry_path(key);
        run_blocking(move || delete_entry(&path)).await
    }

    async fn exists(&self, key: &str) -> Result<bool, BackendError> {
        let path = self.entry_path(key);
        run_blocking(move || match read_header(&path)? {
            None => Ok(false),
            Some(ParsedHeader::Corrupt | ParsedHeader::Expired) => {
                unlink_quiet(&path);
                Ok(false)
            }
            Some(ParsedHeader::Live { .. }) => Ok(true),
        })
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
        run_blocking(move || entry_ttl(&path)).await
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
        run_blocking(move || rewrite_expiry(&path, new_expiry)).await
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
    /// Also sweeps orphaned temp files left by crashed writers (py parity).
    /// The one-time directory setup does its I/O inline; per-operation I/O
    /// runs on the blocking pool.
    ///
    /// # Errors
    ///
    /// Returns a config error if the directory cannot be created or resolved.
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

        cleanup_temp_files(&cache_dir);

        Ok(FileBackend { cache_dir })
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

    #[test]
    fn filename_hash_matches_py_blake2b16_hex() {
        // cachekit-py: hashlib.blake2b(b"key", digest_size=16).hexdigest()
        let backend = FileBackend {
            cache_dir: PathBuf::from("/cache"),
        };
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
        #[allow(clippy::unwrap_used)] // test vector is valid hex by construction
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

    #[test]
    fn temp_file_sweep_removes_stale_keeps_fresh() {
        let dir = tempfile::tempdir().expect("tempdir");

        let stale = dir.path().join("abc.tmp.123.456");
        fs::write(&stale, b"orphan").expect("write");
        let two_minutes_ago = SystemTime::now() - Duration::from_secs(120);
        fs::File::options()
            .write(true)
            .open(&stale)
            .expect("open")
            .set_modified(two_minutes_ago)
            .expect("set mtime");

        let fresh = dir.path().join("def.tmp.123.789");
        fs::write(&fresh, b"in flight").expect("write");

        let entry = dir.path().join("0123abcd");
        fs::write(&entry, b"not a temp file").expect("write");

        cleanup_temp_files(dir.path());

        assert!(!stale.exists(), "stale temp file must be swept");
        assert!(fresh.exists(), "fresh temp file must be kept");
        assert!(entry.exists(), "cache entries must never be swept");
    }
}
