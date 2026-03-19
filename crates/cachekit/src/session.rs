use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

struct SessionInfo {
    id: String,
    start_ms: u64,
}

static SESSION: OnceLock<SessionInfo> = OnceLock::new();

fn get_or_create() -> &'static SessionInfo {
    SESSION.get_or_init(|| {
        let id = uuid::Uuid::new_v4().to_string();
        let start_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        SessionInfo { id, start_ms }
    })
}

pub fn session_headers() -> [(&'static str, String); 2] {
    let s = get_or_create();
    [
        ("X-CacheKit-Session-ID", s.id.clone()),
        ("X-CacheKit-Session-Start", s.start_ms.to_string()),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_id_is_uuid_v4_format() {
        let headers = session_headers();
        let id = &headers[0].1;
        assert!(uuid::Uuid::parse_str(id).is_ok(), "Session ID should be valid UUID");
        let parsed = uuid::Uuid::parse_str(id).unwrap();
        assert_eq!(parsed.get_version_num(), 4, "Should be UUID v4");
    }

    #[test]
    fn session_start_is_reasonable_epoch_millis() {
        let headers = session_headers();
        let start_ms: u64 = headers[1].1.parse().expect("Should be numeric");
        // Should be after 2024-01-01 and before 2030-01-01
        assert!(start_ms > 1_704_067_200_000, "Should be after 2024");
        assert!(start_ms < 1_893_456_000_000, "Should be before 2030");
    }

    #[test]
    fn session_is_stable_across_calls() {
        let h1 = session_headers();
        let h2 = session_headers();
        assert_eq!(h1[0].1, h2[0].1, "Session ID should be stable");
        assert_eq!(h1[1].1, h2[1].1, "Session start should be stable");
    }
}
