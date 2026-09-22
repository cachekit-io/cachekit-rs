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

/// Renders every event as `"<target> <LEVEL> field=value ..."`.
#[derive(Default, Clone)]
struct Capture(Arc<Mutex<Vec<String>>>);

impl Capture {
    fn lines(&self) -> Vec<String> {
        self.0.lock().expect("capture lock").clone()
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
        self.0.lock().expect("capture lock").push(render.0);
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
                &["from=Closed", "to=Open", "circuit breaker opened"],
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
                &["from=Open", "to=HalfOpen"],
            ),
            "{:?}",
            capture.lines()
        );
    }
}
