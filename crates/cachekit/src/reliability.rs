//! Reliability tier: retry with exponential backoff + jitter, and a
//! closed/open/half-open circuit breaker around backend operations.
//!
//! Both are composed by a private `ReliableBackend` decorator around any
//! [`crate::backend::Backend`], applied by the builder when a [`ReliabilityConfig`] is set
//! (see [`crate::CacheKitBuilder::reliability`]). The intent presets
//! `production`, `encrypted`, and `io` enable it by default; `minimal` does
//! not — mirroring the TypeScript SDK's preset posture.
//!
//! Composition order matches the TypeScript SDK's `ReliabilityExecutor`:
//! the retry loop is *inside* the breaker, so one exhausted retry sequence
//! counts as a single breaker failure, and a fast-failing open breaker never
//! spends time retrying.
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

/// Reliability stack configuration: which layers to apply around backend ops.
///
/// The `Default` enables both layers with production defaults. Disable a
/// layer by setting its field to `None`:
///
/// ```
/// use cachekit::reliability::ReliabilityConfig;
///
/// let retry_only = ReliabilityConfig {
///     circuit_breaker: None,
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
}

impl Default for ReliabilityConfig {
    fn default() -> Self {
        Self {
            retry: Some(RetryConfig::default()),
            circuit_breaker: Some(CircuitBreakerConfig::default()),
        }
    }
}

// ── Jitter ───────────────────────────────────────────────────────────────────

/// Uniform random in `[0, 1)`. uuid v4 is the crate's existing entropy source
/// (getrandom-backed); jitter needs decorrelation across clients, not crypto
/// quality — 53 bits is plenty.
fn random_unit() -> f64 {
    let bits = uuid::Uuid::new_v4().as_u128() & ((1u128 << 53) - 1);
    (bits as f64) / ((1u64 << 53) as f64)
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

/// Circuit breaker states, exposed for observability and tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
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
    fn try_acquire(&self) -> Result<(), BackendError> {
        let mut inner = self.lock();
        self.maybe_half_open(&mut inner);
        match inner.state {
            State::Closed => Ok(()),
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
                    Ok(())
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

// ── ReliableBackend ──────────────────────────────────────────────────────────

/// Decorator that applies the reliability stack to every cache operation of
/// an inner [`Backend`]: `breaker(retry(op))`.
///
/// - `get`/`set`/`delete`/`exists` are retried on retryable errors and gated
///   by the circuit breaker.
/// - `health` passes through unguarded — it is a diagnostic and must keep
///   reporting truthfully while the breaker fails data calls fast.
/// - [`Backend::as_lockable`] forwards to the inner backend so distributed
///   fill locks bypass retry/breaker (locks are best-effort advisory).
pub(crate) struct ReliableBackend {
    inner: SharedBackend,
    retry: Option<RetryPolicy>,
    breaker: Option<CircuitBreaker>,
}

impl ReliableBackend {
    pub(crate) fn new(inner: SharedBackend, config: ReliabilityConfig) -> Self {
        Self {
            inner,
            retry: config.retry.map(RetryPolicy::new),
            breaker: config.circuit_breaker.map(CircuitBreaker::new),
        }
    }

    async fn guarded<T, F, Fut>(&self, f: F) -> Result<T, BackendError>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T, BackendError>>,
    {
        if let Some(cb) = &self.breaker {
            cb.try_acquire()?;
        }
        let result = match &self.retry {
            Some(retry) => retry.execute(f).await,
            None => f().await,
        };
        if let Some(cb) = &self.breaker {
            let outcome = match &result {
                Ok(_) => Outcome::Success,
                Err(e) if e.kind.is_retryable() => Outcome::Failure,
                Err(_) => Outcome::Neutral,
            };
            cb.record(&outcome);
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

    #[test]
    fn breaker_opens_after_threshold_and_fails_fast() {
        let cb = breaker(3, Duration::from_secs(60));
        for _ in 0..3 {
            cb.try_acquire().expect("closed breaker admits calls");
            cb.record(&Outcome::Failure);
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
            cb.try_acquire().expect("closed breaker admits calls");
            cb.record(&Outcome::Neutral);
        }
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn breaker_half_open_recovers_on_successes() {
        let cb = breaker(1, Duration::from_millis(0));
        cb.try_acquire().expect("closed breaker admits calls");
        cb.record(&Outcome::Failure);
        // open_timeout of zero → immediately half-open on next inspection
        assert_eq!(cb.state(), CircuitState::HalfOpen);
        for _ in 0..2 {
            cb.try_acquire().expect("half-open admits probes");
            cb.record(&Outcome::Success);
        }
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn breaker_half_open_reopens_on_failure() {
        let cb = breaker(1, Duration::from_millis(0));
        cb.try_acquire().expect("closed breaker admits calls");
        cb.record(&Outcome::Failure);
        assert_eq!(cb.state(), CircuitState::HalfOpen);
        cb.try_acquire().expect("half-open admits a probe");
        cb.record(&Outcome::Failure);
        // Freshly re-opened with a zero timeout flips half-open again on
        // inspection, so assert via the internal state before inspecting.
        assert!(matches!(cb.lock().state, State::Open { .. }));
    }

    #[test]
    fn breaker_half_open_slot_released_by_neutral_outcome() {
        let cb = breaker(1, Duration::from_millis(0));
        cb.try_acquire().expect("closed breaker admits calls");
        cb.record(&Outcome::Failure);
        assert_eq!(cb.state(), CircuitState::HalfOpen);
        // Exhaust both probe slots with permanent errors...
        cb.try_acquire().expect("probe slot 1");
        cb.record(&Outcome::Neutral);
        cb.try_acquire().expect("probe slot 2");
        cb.record(&Outcome::Neutral);
        // ...and the breaker still admits probes instead of wedging.
        cb.try_acquire()
            .expect("neutral outcomes release their probe slots");
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
