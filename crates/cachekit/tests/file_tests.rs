#[cfg(feature = "file")]
mod file_tests {
    use std::time::Duration;

    use cachekit::backend::file::FileBackend;
    use cachekit::backend::{Backend, TtlInspectable};

    fn backend_in(dir: &tempfile::TempDir) -> FileBackend {
        // tempfile dirs follow the umask (e.g. 0775) — the builder's
        // ownership/mode gate would rightly reject that; make it 0700.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))
                .expect("chmod");
        }
        FileBackend::builder()
            .cache_dir(dir.path())
            .build()
            .expect("builder should succeed on a writable dir")
    }

    #[tokio::test]
    async fn set_get_delete_exists_round_trip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = backend_in(&dir);
        let key = "ns:app:func:m.f:args:abc:v1";

        assert_eq!(backend.get(key).await.expect("get"), None);
        assert!(!backend.exists(key).await.expect("exists"));

        backend
            .set(key, b"payload".to_vec(), None)
            .await
            .expect("set");
        assert_eq!(
            backend.get(key).await.expect("get").as_deref(),
            Some(b"payload".as_slice())
        );
        assert!(backend.exists(key).await.expect("exists"));

        assert!(backend.delete(key).await.expect("delete"));
        assert!(!backend.delete(key).await.expect("second delete"));
        assert_eq!(backend.get(key).await.expect("get"), None);
    }

    #[tokio::test]
    async fn set_overwrites_existing_entry() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = backend_in(&dir);

        backend.set("k", b"old".to_vec(), None).await.expect("set");
        backend.set("k", b"new".to_vec(), None).await.expect("set");
        assert_eq!(
            backend.get("k").await.expect("get").as_deref(),
            Some(b"new".as_slice())
        );
    }

    #[tokio::test]
    async fn empty_payload_round_trips() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = backend_in(&dir);

        backend.set("empty", Vec::new(), None).await.expect("set");
        assert_eq!(
            backend.get("empty").await.expect("get").as_deref(),
            Some(b"".as_slice())
        );
        assert!(backend.exists("empty").await.expect("exists"));
    }

    #[tokio::test]
    async fn ttl_expiry_end_to_end() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = backend_in(&dir);

        backend
            .set("short-lived", b"x".to_vec(), Some(Duration::from_secs(1)))
            .await
            .expect("set");
        assert!(backend.exists("short-lived").await.expect("exists"));

        // Expiry is whole-second granularity; 2.1s guarantees `now > expiry`.
        tokio::time::sleep(Duration::from_millis(2100)).await;
        assert_eq!(backend.get("short-lived").await.expect("get"), None);
        assert!(!backend.exists("short-lived").await.expect("exists"));
    }

    #[tokio::test]
    async fn ttl_inspection_and_refresh() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = backend_in(&dir);

        // No TTL on a never-expires entry.
        backend
            .set("forever", b"x".to_vec(), None)
            .await
            .expect("set");
        assert_eq!(backend.ttl("forever").await.expect("ttl"), None);

        // TTL reported on a bounded entry.
        backend
            .set("bounded", b"x".to_vec(), Some(Duration::from_secs(100)))
            .await
            .expect("set");
        let remaining = backend.ttl("bounded").await.expect("ttl").expect("has TTL");
        assert!(remaining <= Duration::from_secs(100));
        assert!(
            remaining >= Duration::from_secs(90),
            "remaining: {remaining:?}"
        );

        // Refresh slides the expiry.
        assert!(backend
            .refresh_ttl("bounded", Duration::from_secs(500))
            .await
            .expect("refresh"));
        let refreshed = backend.ttl("bounded").await.expect("ttl").expect("has TTL");
        assert!(
            refreshed > Duration::from_secs(400),
            "refreshed: {refreshed:?}"
        );

        // Missing key: false, not an error.
        assert!(!backend
            .refresh_ttl("nope", Duration::from_secs(5))
            .await
            .expect("refresh"));

        // Zero duration is rejected (CachekitIO parity).
        let err = backend
            .refresh_ttl("bounded", Duration::ZERO)
            .await
            .expect_err("zero TTL must be rejected");
        assert!(err.to_string().contains("at least 1 second"), "{err}");

        // Missing key TTL: None, not an error.
        assert_eq!(backend.ttl("nope").await.expect("ttl"), None);
    }

    #[tokio::test]
    async fn ttl_beyond_ten_years_is_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = backend_in(&dir);

        let eleven_years = Duration::from_secs(11 * 365 * 24 * 60 * 60);
        let err = backend
            .set("k", b"x".to_vec(), Some(eleven_years))
            .await
            .expect_err("absurd TTL must be rejected");
        assert!(err.to_string().contains("out of range"), "{err}");
    }

    #[tokio::test]
    async fn health_round_trip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = backend_in(&dir);

        let status = backend.health().await.expect("health");
        assert!(status.is_healthy);
        assert_eq!(status.backend_type, "file");
        assert!(status.details.contains_key("cache_dir"));

        // The probe cleans up after itself.
        assert!(!backend.exists("__health_check__").await.expect("exists"));
    }

    #[tokio::test]
    async fn distinct_keys_never_collide_on_disk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = backend_in(&dir);

        backend.set("a", b"1".to_vec(), None).await.expect("set");
        backend.set("b", b"2".to_vec(), None).await.expect("set");
        assert_eq!(
            backend.get("a").await.expect("get").as_deref(),
            Some(b"1".as_slice())
        );
        assert_eq!(
            backend.get("b").await.expect("get").as_deref(),
            Some(b"2".as_slice())
        );
        assert!(backend.delete("a").await.expect("delete"));
        assert!(
            backend.exists("b").await.expect("exists"),
            "b must survive a's delete"
        );
    }

    #[test]
    fn builder_creates_missing_directory() {
        let dir = tempfile::tempdir().expect("tempdir");
        let nested = dir.path().join("deeply").join("nested");
        let backend = FileBackend::builder()
            .cache_dir(&nested)
            .build()
            .expect("builder should create missing directories");
        assert!(nested.is_dir());
        assert!(backend.cache_dir().ends_with("nested"));
    }

    #[cfg(unix)]
    #[test]
    fn builder_sets_owner_only_directory_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().expect("tempdir");
        let nested = dir.path().join("perms");
        FileBackend::builder()
            .cache_dir(&nested)
            .build()
            .expect("build");
        let mode = std::fs::metadata(&nested)
            .expect("metadata")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o700, "cache dir must be owner-only");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn entries_are_written_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().expect("tempdir");
        let backend = backend_in(&dir);
        backend.set("k", b"v".to_vec(), None).await.expect("set");

        let entry = std::fs::read_dir(backend.cache_dir())
            .expect("read_dir")
            .flatten()
            .next()
            .expect("one entry");
        let mode = entry.metadata().expect("metadata").permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "cache files must be owner-only");
    }
}
