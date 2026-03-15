#[cfg(feature = "l1")]
mod tests {
    use cachekit::l1::L1Cache;
    use std::time::Duration;

    #[test]
    fn l1_set_and_get() {
        let cache = L1Cache::new(16);
        cache.set("key1", b"hello", Duration::from_secs(60));
        let result = cache.get("key1");
        assert_eq!(result, Some(b"hello".to_vec()));
    }

    #[test]
    fn l1_get_missing() {
        let cache = L1Cache::new(16);
        let result = cache.get("nonexistent");
        assert_eq!(result, None);
    }

    #[test]
    fn l1_delete() {
        let cache = L1Cache::new(16);
        cache.set("del_key", b"value", Duration::from_secs(60));
        assert!(cache.get("del_key").is_some());
        cache.delete("del_key");
        cache.run_pending_tasks();
        assert_eq!(cache.get("del_key"), None);
    }

    #[test]
    fn l1_expired_entry() {
        let cache = L1Cache::new(16);
        cache.set("exp_key", b"data", Duration::from_millis(1));
        std::thread::sleep(Duration::from_millis(50));
        cache.run_pending_tasks();
        assert_eq!(cache.get("exp_key"), None);
    }

    #[test]
    fn l1_capacity_eviction() {
        let cache = L1Cache::new(2);
        cache.set("a", b"1", Duration::from_secs(60));
        cache.set("b", b"2", Duration::from_secs(60));
        cache.set("c", b"3", Duration::from_secs(60));
        cache.run_pending_tasks();
        let count = ["a", "b", "c"]
            .iter()
            .filter(|k| cache.get(k).is_some())
            .count();
        assert!(count <= 2, "Expected at most 2 entries, got {count}");
    }
}
