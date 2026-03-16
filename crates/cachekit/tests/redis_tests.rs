#[cfg(feature = "redis")]
mod redis_tests {
    use cachekit::backend::redis::RedisBackendBuilder;

    #[test]
    fn redis_builder_missing_url() {
        let result = RedisBackendBuilder::default().build();
        assert!(result.is_err(), "expected error when no URL is provided");

        let err = result.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("url is required"),
            "unexpected error message: {msg}"
        );
    }

    #[test]
    fn redis_builder_from_url() {
        // Connection is lazy — building succeeds even if Redis is not running.
        let result = RedisBackendBuilder::default()
            .url("redis://127.0.0.1:6379")
            .build();
        assert!(
            result.is_ok(),
            "expected Ok when a valid URL is provided, got: {:?}",
            result
        );
    }
}
