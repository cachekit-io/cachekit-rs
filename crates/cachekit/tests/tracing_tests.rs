//! `tracing` feature (LAB-521): every completed cache operation emits one
//! debug event on the `cachekit` target carrying `key_hash` — never the key —
//! and circuit-breaker transitions emit on `cachekit::reliability`.
//!
//! Run with:
//!   cargo test --test tracing_tests --features tracing

#![cfg(all(feature = "tracing", not(target_arch = "wasm32")))]

mod common;

use std::fmt;
use std::sync::{Arc, Mutex};

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};

use crate::common::MockBackend;
use cachekit::metrics::key_hash;
use cachekit::CacheKit;

// ── Capturing subscriber ─────────────────────────────────────────────────────

/// A reaction a test installs on the subscriber: runs synchronously inside
/// `event()` for the first event whose rendering contains the trigger, before
/// that event is recorded — the shape of a real alerting layer that reads the
/// breaker back when it sees a transition.
type Hook = Box<dyn Fn(&str) + Send + Sync>;

/// Renders every event as `"<target> <LEVEL> field=value ..."`.
#[derive(Default, Clone)]
struct Capture {
    lines: Arc<Mutex<Vec<String>>>,
    on_event: Arc<Mutex<Option<(&'static str, Hook)>>>,
}

impl Capture {
    fn lines(&self) -> Vec<String> {
        self.lines.lock().expect("capture lock").clone()
    }

    fn on_event_containing(&self, trigger: &'static str, hook: Hook) {
        *self.on_event.lock().expect("hook lock") = Some((trigger, hook));
    }

    /// Take the hook if this line triggers it — taken before running, so a
    /// hook that emits nested events cannot re-enter itself, and no lock is
    /// held while it runs.
    fn take_hook_for(&self, line: &str) -> Option<Hook> {
        let mut slot = self.on_event.lock().expect("hook lock");
        match &*slot {
            Some((trigger, _)) if line.contains(trigger) => slot.take().map(|(_, hook)| hook),
            _ => None,
        }
    }
}

struct Render(String);

impl Visit for Render {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0.push_str(&format!(" {}={value:?}", field.name()));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.push_str(&format!(" {}={value}", field.name()));
    }
}

impl Subscriber for Capture {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _: &Id, _: &Record<'_>) {}

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let meta = event.metadata();
        let mut render = Render(format!("{} {}", meta.target(), meta.level()));
        event.record(&mut render);
        if let Some(hook) = self.take_hook_for(&render.0) {
            hook(&render.0);
        }
        self.lines.lock().expect("capture lock").push(render.0);
    }

    fn enter(&self, _: &Id) {}

    fn exit(&self, _: &Id) {}
}

fn has(lines: &[String], target: &str, needles: &[&str]) -> bool {
    lines
        .iter()
        .any(|l| l.starts_with(target) && needles.iter().all(|n| l.contains(n)))
}

// ── Operation events ─────────────────────────────────────────────────────────

#[tokio::test]
async fn operations_emit_hashed_key_events_and_never_the_key() {
    let capture = Capture::default();
    let _guard = tracing::subscriber::set_default(capture.clone());

    let client = CacheKit::builder()
        .backend(MockBackend::shared())
        .namespace("ns")
        .build()
        .expect("client builds");
    let raw_key = "user:42";

    let _: Option<u32> = client.get(raw_key).await.expect("get");
    client.set(raw_key, &1u32).await.expect("set");
    let _: Option<u32> = client.get(raw_key).await.expect("get");
    assert!(client.delete(raw_key).await.expect("delete"));

    let lines = capture.lines();
    assert!(
        !lines.iter().any(|l| l.contains(raw_key)),
        "raw key leaked into an event: {lines:?}"
    );

    let hash = format!("key_hash={}", key_hash("ns:user:42"));
    let hit = if cfg!(feature = "l1") {
        "outcome=l1_hit"
    } else {
        "outcome=l2_hit"
    };
    for needles in [
        ["op=get", "outcome=miss"],
        ["op=set", "ttl_secs=300"],
        ["op=get", hit],
        ["op=delete", "existed=true"],
    ] {
        assert!(
            has(&lines, "cachekit DEBUG", &[needles[0], needles[1], &hash]),
            "no event with {needles:?} and the namespaced key hash: {lines:?}"
        );
    }
    assert_eq!(
        lines.iter().filter(|l| l.starts_with("cachekit ")).count(),
        4,
        "exactly one event per operation: {lines:?}"
    );
}

// ── Breaker transitions ──────────────────────────────────────────────────────

#[cfg(feature = "reliability")]
mod breaker {
    use std::time::Duration;

    use super::{has, Capture};
    use crate::common::FailingBackend;
    use cachekit::reliability::{CircuitBreakerConfig, ReliabilityConfig};
    use cachekit::{CacheKit, CircuitState};

    #[tokio::test]
    async fn breaker_transitions_emit_on_the_reliability_target() {
        let capture = Capture::default();
        let _guard = tracing::subscriber::set_default(capture.clone());

        let client = CacheKit::builder()
            .backend(FailingBackend::shared())
            .no_l1()
            .reliability(ReliabilityConfig {
                retry: None,
                circuit_breaker: Some(CircuitBreakerConfig {
                    failure_threshold: 1,
                    open_timeout: Duration::from_millis(20),
                    ..CircuitBreakerConfig::default()
                }),
                backpressure: None,
            })
            .build()
            .expect("client builds");

        client
            .get::<u32>("k")
            .await
            .expect_err("backend is down: opens the circuit");
        assert!(
            has(
                &capture.lines(),
                "cachekit::reliability WARN",
                &["seq=1", "from=Closed", "to=Open", "circuit breaker opened"],
            ),
            "{:?}",
            capture.lines()
        );

        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(client.circuit_state(), Some(CircuitState::HalfOpen));
        assert!(
            has(
                &capture.lines(),
                "cachekit::reliability INFO",
                &["seq=2", "from=Open", "to=HalfOpen"],
            ),
            "{:?}",
            capture.lines()
        );
    }

    /// Events are emitted after the breaker lock is released, so while the
    /// subscriber handles `Closed → Open` another thread may read the breaker
    /// (an alerting layer confirming live state) — the deadlock that order
    /// prevents. With `open_timeout = 0` that read commits and emits
    /// `Open → HalfOpen` before the first callback returns, so the later
    /// transition reaches the subscriber first. Arrival order is wrong; `seq`,
    /// assigned under the lock, is the true order.
    ///
    /// The reader runs on its own thread with its own subscriber default:
    /// `tracing` drops events emitted from *inside* a callback on the same
    /// thread (re-entrancy guard), which is also why the race is cross-thread
    /// in production.
    #[cfg(not(feature = "unsync"))]
    #[tokio::test]
    async fn subscriber_may_read_the_breaker_and_seq_orders_out_of_order_events() {
        use std::sync::mpsc;
        use std::sync::{Arc, Mutex};

        let capture = Capture::default();
        let _guard = tracing::subscriber::set_default(capture.clone());

        let client = CacheKit::builder()
            .backend(FailingBackend::shared())
            .no_l1()
            .reliability(ReliabilityConfig {
                retry: None,
                circuit_breaker: Some(CircuitBreakerConfig {
                    failure_threshold: 1,
                    open_timeout: Duration::ZERO,
                    ..CircuitBreakerConfig::default()
                }),
                backpressure: None,
            })
            .build()
            .expect("client builds");

        // The "alerting layer": on the open event, another thread reads the
        // live breaker while this callback waits for its answer. Were the
        // breaker lock still held during emission, the reader would block and
        // the wait below would time out.
        let observed = Arc::new(Mutex::new(None));
        let (probe, recorder, sink) = (client.clone(), capture.clone(), Arc::clone(&observed));
        capture.on_event_containing(
            "to=Open",
            Box::new(move |_| {
                let (tx, rx) = mpsc::channel();
                let (probe, recorder) = (probe.clone(), recorder.clone());
                std::thread::spawn(move || {
                    let state =
                        tracing::subscriber::with_default(recorder, || probe.circuit_state());
                    let _ = tx.send(state);
                });
                *sink.lock().expect("observed lock") = rx
                    .recv_timeout(Duration::from_secs(5))
                    .expect("the breaker lock is released during emission");
            }),
        );

        client
            .get::<u32>("k")
            .await
            .expect_err("backend is down: opens the circuit");

        assert_eq!(
            *observed.lock().expect("observed lock"),
            Some(CircuitState::HalfOpen),
            "the concurrent read returned and saw the zero-timeout half-open"
        );
        let lines: Vec<String> = capture
            .lines()
            .into_iter()
            .filter(|l| l.starts_with("cachekit::reliability"))
            .collect();
        assert_eq!(lines.len(), 2, "{lines:?}");
        assert!(
            lines[0].contains("to=HalfOpen") && lines[0].contains("seq=2"),
            "the nested, later transition arrives first: {lines:?}"
        );
        assert!(
            lines[1].contains("to=Open") && lines[1].contains("seq=1"),
            "the earlier transition arrives second: {lines:?}"
        );
    }
}
