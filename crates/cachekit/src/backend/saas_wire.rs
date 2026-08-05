//! JSON bodies for the SaaS lock + TTL endpoints (`spec/saas-api.md`).
//!
//! The wire contract is snake_case — `{"timeout_ms": N}` in, `{"lock_id":
//! "uuid" | null}` out. Do NOT add `#[serde(rename_all = "camelCase")]`:
//! that rename silently breaks every field against the deployed server
//! (LAB-411). The round-trip tests below pin the literal wire JSON so a
//! rename regression fails in CI instead of in production.

use serde::{Deserialize, Serialize};

#[derive(Serialize)]
pub(crate) struct LockAcquireRequest {
    pub(crate) timeout_ms: u64,
}

/// Contested acquire is `200 {"lock_id": null}` (canonical since protocol#22)
/// — callers branch on the body, never on a 409 status.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LockAcquireResponse {
    pub(crate) lock_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TtlResponse {
    pub(crate) ttl: Option<u64>,
}

#[derive(Serialize)]
pub(crate) struct RefreshTtlRequest {
    pub(crate) ttl: u64,
}

#[cfg(test)]
#[allow(clippy::expect_used)] // test-only: a wire-contract mismatch should panic loudly
mod tests {
    use super::*;

    #[test]
    fn lock_acquire_request_is_snake_case_on_the_wire() {
        let json =
            serde_json::to_string(&LockAcquireRequest { timeout_ms: 5000 }).expect("serializes");
        assert_eq!(json, r#"{"timeout_ms":5000}"#);
    }

    #[test]
    fn lock_acquire_response_parses_acquired() {
        let resp: LockAcquireResponse =
            serde_json::from_str(r#"{"lock_id":"a1b2c3d4-uuid"}"#).expect("parses");
        assert_eq!(resp.lock_id.as_deref(), Some("a1b2c3d4-uuid"));
    }

    #[test]
    fn lock_acquire_response_parses_contested_null() {
        // LAB-240: contested = 200 {"lock_id": null}, never a 409 branch.
        let resp: LockAcquireResponse =
            serde_json::from_str(r#"{"lock_id":null}"#).expect("parses");
        assert!(resp.lock_id.is_none());
    }

    #[test]
    fn lock_acquire_response_rejects_camel_case() {
        // LAB-411 tripwire: a camelCase rename must fail loudly here.
        assert!(serde_json::from_str::<LockAcquireResponse>(r#"{"lockId":"x"}"#).is_err());
    }

    #[test]
    fn ttl_response_parses_present_and_null() {
        let some: TtlResponse = serde_json::from_str(r#"{"ttl":3542}"#).expect("parses");
        assert_eq!(some.ttl, Some(3542));

        let none: TtlResponse = serde_json::from_str(r#"{"ttl":null}"#).expect("parses");
        assert!(none.ttl.is_none());
    }

    #[test]
    fn refresh_ttl_request_is_snake_case_on_the_wire() {
        let json = serde_json::to_string(&RefreshTtlRequest { ttl: 7200 }).expect("serializes");
        assert_eq!(json, r#"{"ttl":7200}"#);
    }
}
