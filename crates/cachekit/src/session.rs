use std::sync::OnceLock;

struct SessionInfo {
    id: String,
    start_str: String,
}

static SESSION: OnceLock<SessionInfo> = OnceLock::new();

/// Current Unix time in milliseconds, via the JavaScript clock.
///
/// `std::time::SystemTime::now()` panics on `wasm32-unknown-unknown` ("time
/// not implemented on this platform"), which trapped every Workers request
/// (LAB-1079) — so wasm32 builds must read `js_sys::Date::now()` instead.
#[cfg(target_arch = "wasm32")]
fn now_epoch_millis() -> u64 {
    // Saturating float→int cast: NaN → 0, negative → 0, overflow → u64::MAX.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    {
        js_sys::Date::now() as u64
    }
}

/// Current Unix time in milliseconds, via the system clock (native targets).
#[cfg(not(target_arch = "wasm32"))]
fn now_epoch_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

fn get_or_create() -> &'static SessionInfo {
    SESSION.get_or_init(|| SessionInfo {
        id: uuid::Uuid::new_v4().to_string(),
        start_str: now_epoch_millis().to_string(),
    })
}

/// Return session identification headers. Values are static — zero allocations per call.
pub fn session_headers() -> [(&'static str, &'static str); 2] {
    let s = get_or_create();
    [
        ("X-CacheKit-Session-ID", s.id.as_str()),
        ("X-CacheKit-Session-Start", s.start_str.as_str()),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_id_is_uuid_v4_format() {
        let headers = session_headers();
        let id = headers[0].1;
        assert!(
            uuid::Uuid::parse_str(id).is_ok(),
            "Session ID should be valid UUID"
        );
        let parsed = uuid::Uuid::parse_str(id).unwrap();
        assert_eq!(parsed.get_version_num(), 4, "Should be UUID v4");
    }

    #[test]
    #[allow(clippy::expect_used)]
    fn session_start_is_reasonable_epoch_millis() {
        let headers = session_headers();
        let start_ms: u64 = headers[1].1.parse().expect("Should be numeric");
        // Plausibility window: after 2024-01-01, before 2100-01-01. Wide on
        // purpose — it exists to catch unit confusion (epoch seconds trip the
        // lower bound, micros the upper), not to expire on a schedule. Bounds
        // mirror tests/wasm_session_tests.rs — keep them in lockstep.
        assert!(start_ms > 1_704_067_200_000, "Should be after 2024");
        assert!(start_ms < 4_102_444_800_000, "Should be before 2100");
    }

    #[test]
    fn session_is_stable_across_calls() {
        let h1 = session_headers();
        let h2 = session_headers();
        assert_eq!(h1[0].1, h2[0].1, "Session ID should be stable");
        assert_eq!(h1[1].1, h2[1].1, "Session start should be stable");
    }
}
