//! Reliability tier: backpressure (bounded backend concurrency), retry with
//! exponential backoff + jitter, and a closed/open/half-open circuit breaker
//! around backend operations.
//!
//! All three are composed by a private `ReliableBackend` decorator around any
//! [`crate::backend::Backend`], applied by the builder when a [`ReliabilityConfig`] is set
//! (see [`crate::CacheKitBuilder::reliability`]). The intent presets
//! `production`, `encrypted`, and `io` enable it by default; `minimal` does
//! not — mirroring the TypeScript SDK's preset posture.
//!
//! Composition order is `backpressure(breaker(retry(op)))`:
//!
//! - The retry loop is *inside* the breaker (matching the TypeScript SDK's
//!   `ReliabilityExecutor`), so one exhausted retry sequence counts as a
//!   single breaker failure, and a fast-failing open breaker never spends
//!   time retrying.
//! - The concurrency limiter is *outermost*: one permit per logical cache
//!   operation, held across the entire breaker/retry sequence. That bounds
//!   in-flight work including retry amplification (K callers mid-backoff are
//!   still K permits — new work queues behind them instead of piling onto a
//!   struggling backend), and a shed call never touches breaker counters or
//!   half-open probe slots, so the breaker keeps measuring backend health,
//!   not caller-side overload. A permit holder never re-enters the limiter
//!   (backend ops don't nest), so holding permits across retry backoff
//!   cannot deadlock.
//!
//! Unlike the TypeScript breaker (which counts every error), only errors
//! classified retryable by [`crate::error::BackendErrorKind::is_retryable`] (`Transient`,
//! `Timeout`) count toward opening the circuit: they are the backend-health
//! signals. `Permanent` / `Authentication` errors are request-specific — five
//! malformed requests must not cut off healthy traffic.
//!
//! Requires a tokio runtime for backoff timers (`redis` and `cachekitio`
//! backends already do). Not available on wasm32 targets.

use std::future::Future;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;

use crate::backend::{Backend, HealthStatus, LockableBackend};
use crate::client::SharedBackend;
use crate::error::BackendError;
use crate::random_unit;

// ── Configuration ────────────────────────────────────────────────────────────

/// Retry policy configuration (truncated exponential backoff with jitter).
#[derive(Debug, Clone, PartialEq)]
pub struct RetryConfig {
    /// Total attempts, including the first (default: 3). `0` behaves as `1`.
    pub max_attempts: u32,
    /// Backoff base delay; attempt *n* waits `base_delay * 2^n` (default: 100 ms).
    pub base_delay: Duration,
    /// Backoff ceiling (default: 5 s).
    pub max_delay: Duration,
    /// Multiply each delay by a random factor in `[0.5, 1.5)` (default: true).
    pub jitter: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            jitter: true,
        }
    }
}

/// Circuit breaker configuration.
///
/// Defaults mirror the TypeScript SDK's production preset
/// (`PRODUCTION_RELIABILITY` in `cachekit-ts/src/intents.ts`).
#[derive(Debug, Clone, PartialEq)]
pub struct CircuitBreakerConfig {
    /// Retryable failures within [`Self::rolling_window`] before the circuit
    /// opens (default: 5).
    pub failure_threshold: u32,
    /// Successes in half-open state required to close the circuit (default: 3).
    pub success_threshold: u32,
    /// How long the circuit stays open before allowing half-open probes
    /// (default: 5 s).
    pub open_timeout: Duration,
    /// Maximum concurrent probe calls in half-open state (default: 3).
    pub half_open_max_calls: u32,
    /// Rolling window for failure counting (default: 60 s).
    pub rolling_window: Duration,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 3,
            open_timeout: Duration::from_secs(5),
            half_open_max_calls: 3,
            rolling_window: Duration::from_secs(60),
        }
    }
}

/// Backpressure configuration: bound how many backend data operations may be
/// in flight at once, so a slow or failing backend cannot exhaust the
/// caller's connection pool or memory.
///
/// Defaults mirror the Python SDK's `BackpressureConfig`
/// (`max_concurrent_requests: 100`, `queue_size: 1000`, `timeout: 0.1s`).
///
/// Over-limit calls first join a bounded waiting queue; a caller that finds
/// the queue full, or that waits longer than [`Self::acquire_timeout`]
/// without a permit freeing up, is shed with a
/// [`crate::error::BackendErrorKind::Backpressure`] error — never queued
/// unboundedly. Shed calls do not reach the backend and do not count toward
/// opening the circuit breaker.
///
/// On the `#[cachekit]` macro's plain path a shed is outage-class — exactly
/// like `CircuitOpen`, the wrapped function runs uncached (fail-open);
/// `secure` paths fail closed on a shed like on every other backend error.
#[derive(Debug, Clone, PartialEq)]
pub struct BackpressureConfig {
    /// Maximum backend data operations in flight at once (default: 100).
    /// `0` behaves as `1`; values above tokio's `Semaphore::MAX_PERMITS`
    /// (`usize::MAX >> 3`) are clamped to it, so `usize::MAX` reads as
    /// "effectively unbounded" rather than panicking the builder.
    pub max_concurrent: usize,
    /// Maximum callers waiting for a permit before further calls are shed
    /// immediately (default: 1000). `0` disables waiting entirely: a call
    /// that cannot take a permit on the spot is shed.
    pub max_queue: usize,
    /// How long a queued caller waits for a permit before it is shed
    /// (default: 100 ms).
    pub acquire_timeout: Duration,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 100,
            max_queue: 1000,
            acquire_timeout: Duration::from_millis(100),
        }
    }
}

/// Reliability stack configuration: which layers to apply around backend ops.
///
/// The `Default` enables all layers with production defaults. Disable a
/// layer by setting its field to `None`:
///
/// ```
/// use cachekit::reliability::ReliabilityConfig;
///
/// let retry_only = ReliabilityConfig {
///     circuit_breaker: None,
///     backpressure: None,
///     ..ReliabilityConfig::default()
/// };
/// assert!(retry_only.retry.is_some());
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct ReliabilityConfig {
    /// Retry policy, or `None` to propagate every error on first failure.
    pub retry: Option<RetryConfig>,
    /// Circuit breaker, or `None` to never fail fast.
    pub circuit_breaker: Option<CircuitBreakerConfig>,
    /// Concurrency limiter, or `None` for unbounded backend concurrency.
    pub backpressure: Option<BackpressureConfig>,
}

impl Default for ReliabilityConfig {
    fn default() -> Self {
        Self {
            retry: Some(RetryConfig::default()),
            circuit_breaker: Some(CircuitBreakerConfig::default()),
            backpressure: Some(BackpressureConfig::default()),
        }
    }
}

impl ReliabilityConfig {
    /// A config with every layer off — the documented preset opt-out.
    ///
    /// Prefer this over spelling out a struct literal with all-`None`
    /// fields: a literal breaks downstream code every time the stack gains
    /// a layer (it has, twice).
    ///
    /// ```
    /// use cachekit::reliability::ReliabilityConfig;
    ///
    /// assert!(ReliabilityConfig::disabled().is_disabled());
    /// assert!(!ReliabilityConfig::default().is_disabled());
    /// ```
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            retry: None,
            circuit_breaker: None,
            backpressure: None,
        }
    }

    /// `true` when no layer is enabled — the builder skips the (no-op)
    /// `ReliableBackend` decorator entirely. Lives here, next to the fields,
    /// so adding a layer cannot silently miss the builder gate again.
    #[must_use]
    pub fn is_disabled(&self) -> bool {
        self.retry.is_none() && self.circuit_breaker.is_none() && self.backpressure.is_none()
    }
}

// ── RetryPolicy ──────────────────────────────────────────────────────────────

/// Retries an operation on errors where [`crate::error::BackendErrorKind::is_retryable`] is
/// true, sleeping a truncated exponential backoff (with jitter) between
/// attempts. `Permanent` and `Authentication` errors propagate immediately.
#[derive(Debug)]
pub(crate) struct RetryPolicy {
    config: RetryConfig,
}

impl RetryPolicy {
    pub(crate) fn new(config: RetryConfig) -> Self {
        Self { config }
    }

    fn delay(&self, attempt: u32) -> Duration {
        let exp = self
            .config
            .base_delay
            .saturating_mul(2u32.saturating_pow(attempt));
        let capped = exp.min(self.config.max_delay);
        if self.config.jitter {
            capped.mul_f64(0.5 + random_unit())
        } else {
            capped
        }
    }

    pub(crate) async fn execute<T, F, Fut>(&self, f: F) -> Result<T, BackendError>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T, BackendError>>,
    {
        let mut attempt: u32 = 0;
        loop {
            match f().await {
                Ok(v) => return Ok(v),
                Err(e) if e.kind.is_retryable() && attempt + 1 < self.config.max_attempts => {
                    tokio::time::sleep(self.delay(attempt)).await;
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

// ── CircuitBreaker ───────────────────────────────────────────────────────────

/// Circuit breaker states. Test-only until the observability tier (LAB-101)
/// exposes breaker state at runtime — a public type with no producer is API
/// noise (expert-panel cut).
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CircuitState {
    /// Normal operation; calls pass through.
    Closed,
    /// Failing fast; calls return a [`crate::error::BackendErrorKind::CircuitOpen`] error
    /// without reaching the backend.
    Open,
    /// Probing recovery with a bounded number of trial calls.
    HalfOpen,
}

#[derive(Debug)]
enum State {
    Closed,
    Open { since: Instant },
    HalfOpen,
}

#[derive(Debug)]
struct BreakerInner {
    state: State,
    /// Timestamps of counted failures inside the rolling window.
    failures: Vec<Instant>,
    half_open_successes: u32,
    half_open_calls: u32,
}

/// How a completed call is reported back to the breaker.
enum Outcome {
    Success,
    /// A retryable-kind failure — a backend-health signal.
    Failure,
    /// A non-retryable failure (permanent/auth) — request-specific, does not
    /// count toward opening the circuit but must release its half-open slot,
    /// or a burst of permanent errors would wedge the breaker half-open.
    Neutral,
}

/// State machine: closed → (failures ≥ threshold in window) → open →
/// (open_timeout elapsed) → half-open → (successes ≥ threshold) → closed,
/// or (any counted failure) → open.
#[derive(Debug)]
pub(crate) struct CircuitBreaker {
    config: CircuitBreakerConfig,
    inner: Mutex<BreakerInner>,
}

impl CircuitBreaker {
    pub(crate) fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            inner: Mutex::new(BreakerInner {
                state: State::Closed,
                failures: Vec::new(),
                half_open_successes: 0,
                half_open_calls: 0,
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BreakerInner> {
        // A poisoned lock means a panic mid-update; breaker state is advisory,
        // so recovering the guard is strictly better than propagating panics.
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Current state (transitions open → half-open lazily on inspection).
    /// Test-only until the observability tier (LAB-101) needs it at runtime.
    #[cfg(test)]
    pub(crate) fn state(&self) -> CircuitState {
        let mut inner = self.lock();
        self.maybe_half_open(&mut inner);
        match inner.state {
            State::Closed => CircuitState::Closed,
            State::Open { .. } => CircuitState::Open,
            State::HalfOpen => CircuitState::HalfOpen,
        }
    }

    fn maybe_half_open(&self, inner: &mut BreakerInner) {
        if let State::Open { since } = inner.state {
            if since.elapsed() >= self.config.open_timeout {
                inner.state = State::HalfOpen;
                inner.half_open_successes = 0;
                inner.half_open_calls = 0;
            }
        }
    }

    /// Admit a call, or fail fast with a circuit-open error.
    ///
    /// Returns an RAII [`ProbePermit`]: if the guarded future is cancelled
    /// (caller timeout/`select!`) or panics before an outcome is recorded,
    /// the permit's `Drop` releases any half-open probe slot it took —
    /// otherwise `half_open_max_calls` cancelled probes would wedge the
    /// breaker half-open forever, fast-failing every call even against a
    /// recovered backend.
    fn try_acquire(&self) -> Result<ProbePermit<'_>, BackendError> {
        let mut inner = self.lock();
        self.maybe_half_open(&mut inner);
        match inner.state {
            State::Closed => Ok(ProbePermit {
                breaker: self,
                took_slot: false,
            }),
            State::Open { .. } => Err(BackendError::circuit_open(
                "circuit breaker is open: backend calls are failing fast",
            )),
            State::HalfOpen => {
                if inner.half_open_calls >= self.config.half_open_max_calls {
                    Err(BackendError::circuit_open(
                        "circuit breaker is half-open and the probe limit is reached",
                    ))
                } else {
                    inner.half_open_calls += 1;
                    Ok(ProbePermit {
                        breaker: self,
                        took_slot: true,
                    })
                }
            }
        }
    }

    fn record(&self, outcome: &Outcome) {
        let mut inner = self.lock();
        match outcome {
            Outcome::Success => {
                if matches!(inner.state, State::HalfOpen) {
                    inner.half_open_successes += 1;
                    if inner.half_open_successes >= self.config.success_threshold {
                        inner.state = State::Closed;
                        inner.failures.clear();
                        inner.half_open_successes = 0;
                        inner.half_open_calls = 0;
                    } else {
                        // Release this probe's slot. `half_open_calls` caps the
                        // number of *in-flight* probes, so a success that does
                        // not yet close the breaker must free its slot (exactly
                        // as `Neutral` does). Without this, a config with
                        // success_threshold > half_open_max_calls wedges the
                        // breaker half-open forever: the slots fill, successes
                        // stall below the threshold, and every subsequent call
                        // fails fast with CircuitOpen even against a healthy
                        // backend.
                        inner.half_open_calls = inner.half_open_calls.saturating_sub(1);
                    }
                }
            }
            Outcome::Failure => match inner.state {
                State::HalfOpen => {
                    inner.state = State::Open {
                        since: Instant::now(),
                    };
                    inner.half_open_successes = 0;
                    inner.half_open_calls = 0;
                }
                State::Closed => {
                    let now = Instant::now();
                    inner.failures.push(now);
                    let window = self.config.rolling_window;
                    inner.failures.retain(|t| now.duration_since(*t) <= window);
                    if inner.failures.len() >= self.config.failure_threshold as usize {
                        inner.state = State::Open { since: now };
                        inner.failures.clear();
                    }
                }
                // Open without an admitted call cannot report a failure;
                // ignore rather than extend the open window.
                State::Open { .. } => {}
            },
            Outcome::Neutral => {
                if matches!(inner.state, State::HalfOpen) {
                    inner.half_open_calls = inner.half_open_calls.saturating_sub(1);
                }
            }
        }
    }
}

// ── ProbePermit ──────────────────────────────────────────────────────────────

/// RAII token for a breaker-admitted call.
///
/// Slot accounting lives in exactly one of two places: [`Self::complete`]
/// (normal return — the outcome arms of `record` own the bookkeeping from
/// there) or `Drop` (cancel/panic — release the slot like `Neutral`, no
/// transition). Manual increment/decrement pairs leaked twice before this
/// guard existed; do not reintroduce them.
#[derive(Debug)]
struct ProbePermit<'a> {
    breaker: &'a CircuitBreaker,
    /// Whether this admission consumed a half-open probe slot.
    took_slot: bool,
}

impl ProbePermit<'_> {
    /// Report the call's outcome and disarm the drop-release.
    fn complete(mut self, outcome: &Outcome) {
        self.took_slot = false;
        self.breaker.record(outcome);
    }
}

impl Drop for ProbePermit<'_> {
    fn drop(&mut self) {
        if !self.took_slot {
            return;
        }
        // No outcome was recorded: the guarded future was cancelled mid-await
        // or panicked. Free the probe slot so the half-open window can keep
        // probing; if the breaker transitioned meanwhile (counters reset),
        // the saturating decrement is a no-op.
        let mut inner = self.breaker.lock();
        if matches!(inner.state, State::HalfOpen) {
            inner.half_open_calls = inner.half_open_calls.saturating_sub(1);
        }
    }
}

// ── ConcurrencyLimiter ───────────────────────────────────────────────────────

/// Bounds concurrent backend data operations with a semaphore and a bounded
/// waiting queue (two-phase, like the Python SDK's `BackpressureController`):
/// a saturated limiter admits up to `max_queue` waiters for at most
/// `acquire_timeout` each; everyone else is shed with a
/// [`crate::error::BackendErrorKind::Backpressure`] error.
#[derive(Debug)]
pub(crate) struct ConcurrencyLimiter {
    semaphore: tokio::sync::Semaphore,
    /// Callers currently waiting for a permit (phase-2 queue depth).
    waiting: std::sync::atomic::AtomicUsize,
    config: BackpressureConfig,
}

/// RAII guard for a slot in the waiting queue: decrements `waiting` on every
/// exit path, including cancellation mid-`acquire` (same lesson as
/// [`ProbePermit`] — manual increment/decrement pairs leak on cancel).
struct QueueSlot<'a> {
    waiting: &'a std::sync::atomic::AtomicUsize,
}

impl Drop for QueueSlot<'_> {
    fn drop(&mut self) {
        self.waiting
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

impl ConcurrencyLimiter {
    pub(crate) fn new(config: BackpressureConfig) -> Self {
        Self {
            // `Semaphore::new(0)` would shed every call after acquire_timeout
            // with nothing ever admitted — clamp like RetryConfig's "0
            // behaves as 1". The upper clamp matters too: `Semaphore::new`
            // PANICS above `MAX_PERMITS` (usize::MAX >> 3), and usize::MAX
            // is the natural "effectively unbounded" sentinel a caller will
            // reach for — a config value must never panic the builder.
            semaphore: tokio::sync::Semaphore::new(
                config
                    .max_concurrent
                    .clamp(1, tokio::sync::Semaphore::MAX_PERMITS),
            ),
            waiting: std::sync::atomic::AtomicUsize::new(0),
            config,
        }
    }

    /// Take a permit, or shed the call.
    ///
    /// Phase 1: a free permit is taken on the spot — no queue accounting.
    /// Phase 2 (saturated): join the bounded waiting queue and wait up to
    /// `acquire_timeout` for a permit; queue-full and wait-timeout both shed
    /// with a `Backpressure` error. The returned permit releases on drop, so
    /// a cancelled or panicking caller can never leak capacity.
    async fn acquire(&self) -> Result<tokio::sync::SemaphorePermit<'_>, BackendError> {
        use std::sync::atomic::Ordering;

        if let Ok(permit) = self.semaphore.try_acquire() {
            return Ok(permit);
        }
        if self.waiting.fetch_add(1, Ordering::AcqRel) >= self.config.max_queue {
            self.waiting.fetch_sub(1, Ordering::AcqRel);
            return Err(BackendError::backpressure(format!(
                "backpressure: waiting queue is full (max_queue={}), call shed without reaching the backend",
                self.config.max_queue
            )));
        }
        let _slot = QueueSlot {
            waiting: &self.waiting,
        };
        match tokio::time::timeout(self.config.acquire_timeout, self.semaphore.acquire()).await {
            Ok(Ok(permit)) => Ok(permit),
            // The semaphore is never closed; treat a close defensively as shed.
            Ok(Err(_closed)) => Err(BackendError::backpressure(
                "backpressure: limiter unavailable, call shed without reaching the backend",
            )),
            Err(_elapsed) => Err(BackendError::backpressure(format!(
                "backpressure: timed out waiting for a permit after {:?}, call shed without reaching the backend",
                self.config.acquire_timeout
            ))),
        }
    }
}

// ── ReliableBackend ──────────────────────────────────────────────────────────

/// Decorator that applies the reliability stack to every cache operation of
/// an inner [`Backend`]: `backpressure(breaker(retry(op)))`.
///
/// - `get`/`set`/`delete`/`exists` take a concurrency-limiter permit, are
///   retried on retryable errors, and gated by the circuit breaker.
/// - `health` passes through unguarded — it is a diagnostic and must keep
///   reporting truthfully while the breaker fails data calls fast (or the
///   limiter sheds them).
/// - [`Backend::as_lockable`] forwards to the inner backend so distributed
///   fill locks bypass the stack (locks are best-effort advisory).
pub(crate) struct ReliableBackend {
    inner: SharedBackend,
    retry: Option<RetryPolicy>,
    breaker: Option<CircuitBreaker>,
    limiter: Option<ConcurrencyLimiter>,
}

impl ReliableBackend {
    pub(crate) fn new(inner: SharedBackend, config: ReliabilityConfig) -> Self {
        Self {
            inner,
            retry: config.retry.map(RetryPolicy::new),
            breaker: config.circuit_breaker.map(CircuitBreaker::new),
            limiter: config.backpressure.map(ConcurrencyLimiter::new),
        }
    }

    async fn guarded<T, F, Fut>(&self, f: F) -> Result<T, BackendError>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T, BackendError>>,
    {
        // Outermost layer: one permit per logical operation, held across the
        // whole breaker/retry sequence (see the module docs for why). A shed
        // call returns here — before touching breaker state.
        let _permit = match &self.limiter {
            Some(limiter) => Some(limiter.acquire().await?),
            None => None,
        };
        let permit = match &self.breaker {
            Some(cb) => Some(cb.try_acquire()?),
            None => None,
        };
        let result = match &self.retry {
            Some(retry) => retry.execute(f).await,
            None => f().await,
        };
        if let Some(permit) = permit {
            let outcome = match &result {
                Ok(_) => Outcome::Success,
                Err(e) if e.kind.is_retryable() => Outcome::Failure,
                Err(_) => Outcome::Neutral,
            };
            permit.complete(&outcome);
        }
        result
    }
}

#[cfg_attr(not(feature = "unsync"), async_trait)]
#[cfg_attr(feature = "unsync", async_trait(?Send))]
impl Backend for ReliableBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        self.guarded(|| self.inner.get(key)).await
    }

    async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        // Clone per attempt: the inner call consumes the buffer.
        self.guarded(|| self.inner.set(key, value.clone(), ttl))
            .await
    }

    async fn delete(&self, key: &str) -> Result<bool, BackendError> {
        self.guarded(|| self.inner.delete(key)).await
    }

    async fn exists(&self, key: &str) -> Result<bool, BackendError> {
        self.guarded(|| self.inner.exists(key)).await
    }

    async fn health(&self) -> Result<HealthStatus, BackendError> {
        self.inner.health().await
    }

    fn as_lockable(&self) -> Option<&dyn LockableBackend> {
        self.inner.as_lockable()
    }
}

/// Wrap `inner` in a [`ReliableBackend`] and re-share it.
#[cfg(not(feature = "unsync"))]
pub(crate) fn wrap_reliable(inner: SharedBackend, config: ReliabilityConfig) -> SharedBackend {
    std::sync::Arc::new(ReliableBackend::new(inner, config))
}

/// Wrap `inner` in a [`ReliableBackend`] and re-share it (`?Send` variant).
#[cfg(feature = "unsync")]
pub(crate) fn wrap_reliable(inner: SharedBackend, config: ReliabilityConfig) -> SharedBackend {
    std::rc::Rc::new(ReliableBackend::new(inner, config))
}

// ── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::expect_used)] // test-only: failed acquire/probe should panic loudly
mod tests {
    use super::*;
    use crate::error::BackendErrorKind;

    fn breaker(failure_threshold: u32, open_timeout: Duration) -> CircuitBreaker {
        CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold,
            success_threshold: 2,
            open_timeout,
            half_open_max_calls: 2,
            rolling_window: Duration::from_secs(60),
        })
    }

    /// Admit a call and immediately report its outcome.
    fn admit_and(cb: &CircuitBreaker, outcome: &Outcome) {
        let permit = cb.try_acquire().expect("breaker admits the call");
        permit.complete(outcome);
    }

    #[test]
    fn breaker_opens_after_threshold_and_fails_fast() {
        let cb = breaker(3, Duration::from_secs(60));
        for _ in 0..3 {
            admit_and(&cb, &Outcome::Failure);
        }
        assert_eq!(cb.state(), CircuitState::Open);
        let err = cb.try_acquire().expect_err("open breaker fails fast");
        assert_eq!(err.kind, BackendErrorKind::CircuitOpen);
        assert!(!err.kind.is_retryable());
    }

    #[test]
    fn breaker_ignores_permanent_errors() {
        let cb = breaker(2, Duration::from_secs(60));
        for _ in 0..10 {
            admit_and(&cb, &Outcome::Neutral);
        }
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn breaker_half_open_recovers_on_successes() {
        let cb = breaker(1, Duration::from_millis(0));
        admit_and(&cb, &Outcome::Failure);
        // open_timeout of zero → immediately half-open on next inspection
        assert_eq!(cb.state(), CircuitState::HalfOpen);
        for _ in 0..2 {
            admit_and(&cb, &Outcome::Success);
        }
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn breaker_half_open_reopens_on_failure() {
        let cb = breaker(1, Duration::from_millis(0));
        admit_and(&cb, &Outcome::Failure);
        assert_eq!(cb.state(), CircuitState::HalfOpen);
        admit_and(&cb, &Outcome::Failure);
        // Freshly re-opened with a zero timeout flips half-open again on
        // inspection, so assert via the internal state before inspecting.
        assert!(matches!(cb.lock().state, State::Open { .. }));
    }

    #[test]
    fn breaker_half_open_slot_released_by_neutral_outcome() {
        let cb = breaker(1, Duration::from_millis(0));
        admit_and(&cb, &Outcome::Failure);
        assert_eq!(cb.state(), CircuitState::HalfOpen);
        // Exhaust both probe slots with permanent errors...
        admit_and(&cb, &Outcome::Neutral);
        admit_and(&cb, &Outcome::Neutral);
        // ...and the breaker still admits probes instead of wedging.
        let permit = cb
            .try_acquire()
            .expect("neutral outcomes release their probe slots");
        permit.complete(&Outcome::Neutral);
    }

    #[test]
    fn breaker_half_open_closes_when_success_threshold_exceeds_probe_cap() {
        // success_threshold (3) deliberately exceeds half_open_max_calls (1):
        // with a single in-flight probe slot, the breaker can only ever reach
        // three successes if each non-closing success RELEASES its slot. Before
        // the fix this wedged half-open forever — the slot filled after the
        // first success (which stalled at 1 < 3), so no further probe was
        // admitted and the breaker never re-closed.
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 1,
            success_threshold: 3,
            open_timeout: Duration::from_millis(0),
            half_open_max_calls: 1,
            rolling_window: Duration::from_secs(60),
        });
        admit_and(&cb, &Outcome::Failure);
        assert_eq!(cb.state(), CircuitState::HalfOpen);
        for _ in 0..3 {
            let permit = cb
                .try_acquire()
                .expect("a non-closing success must release its probe slot");
            permit.complete(&Outcome::Success);
        }
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn breaker_dropped_permit_releases_probe_slot() {
        // A probe future cancelled (caller timeout / select!) or panicked
        // before recording an outcome must not consume its slot forever:
        // exhaust every half-open slot with plain drops and the breaker must
        // still admit probes instead of wedging half-open until restart.
        let cb = breaker(1, Duration::from_millis(0));
        admit_and(&cb, &Outcome::Failure);
        assert_eq!(cb.state(), CircuitState::HalfOpen);
        for _ in 0..2 {
            let permit = cb.try_acquire().expect("half-open admits a probe");
            drop(permit); // cancelled before any outcome
        }
        let permit = cb
            .try_acquire()
            .expect("dropped permits release their probe slots");
        permit.complete(&Outcome::Success);
    }

    #[test]
    fn breaker_closed_permit_drop_does_not_touch_half_open_accounting() {
        // A call admitted while CLOSED holds no probe slot; cancelling it
        // must not free (or corrupt) slots in a half-open window that opened
        // after its admission.
        let cb = breaker(1, Duration::from_millis(0));
        let closed_permit = cb.try_acquire().expect("closed breaker admits calls");
        // Another call's failure opens the breaker, then zero timeout flips
        // it half-open with a fresh probe window.
        admit_and(&cb, &Outcome::Failure);
        assert_eq!(cb.state(), CircuitState::HalfOpen);
        let p1 = cb.try_acquire().expect("probe slot 1");
        let p2 = cb.try_acquire().expect("probe slot 2");
        drop(closed_permit); // must be a no-op: it never took a slot
        assert!(
            cb.try_acquire().is_err(),
            "probe cap must still be enforced after a closed-state permit drops"
        );
        p1.complete(&Outcome::Success);
        p2.complete(&Outcome::Success);
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn reliability_default_enables_backpressure_with_python_parity_defaults() {
        let config = ReliabilityConfig::default();
        let bp = config.backpressure.expect("backpressure is on by default");
        assert_eq!(bp.max_concurrent, 100);
        assert_eq!(bp.max_queue, 1000);
        assert_eq!(bp.acquire_timeout, Duration::from_millis(100));
    }

    #[tokio::test]
    async fn limiter_clamps_zero_max_concurrent_to_one() {
        let limiter = ConcurrencyLimiter::new(BackpressureConfig {
            max_concurrent: 0,
            max_queue: 0,
            acquire_timeout: Duration::from_millis(10),
        });
        let permit = limiter
            .acquire()
            .await
            .expect("0 behaves as 1 — one permit exists");
        drop(permit);
    }

    #[tokio::test]
    async fn limiter_clamps_huge_max_concurrent_instead_of_panicking() {
        // usize::MAX is the natural "unbounded" sentinel; Semaphore::new
        // panics above MAX_PERMITS, so the constructor must clamp.
        let limiter = ConcurrencyLimiter::new(BackpressureConfig {
            max_concurrent: usize::MAX,
            max_queue: 0,
            acquire_timeout: Duration::from_millis(10),
        });
        let permit = limiter.acquire().await.expect("clamped limiter admits");
        drop(permit);
    }

    #[tokio::test]
    async fn limiter_sheds_immediately_when_queue_disabled() {
        let limiter = ConcurrencyLimiter::new(BackpressureConfig {
            max_concurrent: 1,
            max_queue: 0,
            acquire_timeout: Duration::from_secs(5),
        });
        let _held = limiter.acquire().await.expect("first permit");
        let start = Instant::now();
        let err = limiter
            .acquire()
            .await
            .expect_err("saturated with no waiting queue");
        assert_eq!(err.kind, BackendErrorKind::Backpressure);
        assert!(!err.kind.is_retryable());
        assert!(
            start.elapsed() < Duration::from_millis(500),
            "queue-full sheds immediately, not after acquire_timeout"
        );
    }

    #[tokio::test]
    async fn limiter_waiting_slot_released_on_cancelled_wait() {
        // A waiter cancelled mid-acquire (caller timeout / select!) must free
        // its queue slot via the QueueSlot drop guard. With max_queue: 1, a
        // leaked slot would shed the next waiter instantly as queue-full;
        // joining the queue (observable as waiting out the acquire_timeout)
        // proves the slot was released.
        let limiter = ConcurrencyLimiter::new(BackpressureConfig {
            max_concurrent: 1,
            max_queue: 1,
            acquire_timeout: Duration::from_millis(100),
        });
        let _held = limiter.acquire().await.expect("first permit");
        let cancelled = tokio::time::timeout(Duration::from_millis(20), limiter.acquire()).await;
        assert!(cancelled.is_err(), "waiter cancelled from outside");

        let start = Instant::now();
        let err = limiter
            .acquire()
            .await
            .expect_err("permit never frees, waiter times out");
        assert_eq!(err.kind, BackendErrorKind::Backpressure);
        assert!(
            start.elapsed() >= Duration::from_millis(80),
            "must join the queue and wait out acquire_timeout — an instant \
             queue-full shed means the cancelled waiter leaked its slot"
        );
    }

    #[tokio::test]
    async fn limiter_sheds_queue_full_at_nonzero_boundary() {
        // cap 1, queue 1: with the permit held and one waiter parked, a
        // third caller must shed instantly as queue-full — pinning the
        // fetch_add boundary arithmetic at a nonzero max_queue.
        let limiter = ConcurrencyLimiter::new(BackpressureConfig {
            max_concurrent: 1,
            max_queue: 1,
            acquire_timeout: Duration::from_millis(200),
        });
        let _held = limiter.acquire().await.expect("first permit");
        let waiter = async {
            // Parks in the queue immediately and times out after 200 ms.
            limiter.acquire().await
        };
        let third = async {
            tokio::time::sleep(Duration::from_millis(50)).await; // waiter parked
            let start = Instant::now();
            let err = limiter.acquire().await.expect_err("queue of 1 is full");
            assert_eq!(err.kind, BackendErrorKind::Backpressure);
            assert!(
                start.elapsed() < Duration::from_millis(100),
                "queue-full sheds instantly, not after the wait timeout"
            );
        };
        let (waited, ()) = tokio::join!(waiter, third);
        waited.expect_err("the parked waiter itself times out");
    }

    #[test]
    fn retry_delay_is_capped_and_jittered() {
        let policy = RetryPolicy::new(RetryConfig {
            max_attempts: 5,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(300),
            jitter: true,
        });
        for attempt in 0..10 {
            let d = policy.delay(attempt);
            // cap 300ms × jitter [0.5, 1.5) → strictly under 450ms
            assert!(d < Duration::from_millis(450), "attempt {attempt}: {d:?}");
        }
        let no_jitter = RetryPolicy::new(RetryConfig {
            jitter: false,
            ..RetryConfig::default()
        });
        assert_eq!(no_jitter.delay(0), Duration::from_millis(100));
        assert_eq!(no_jitter.delay(1), Duration::from_millis(200));
        assert_eq!(no_jitter.delay(20), Duration::from_secs(5));
    }
}
