use std::sync::Arc;

pub struct L1Stats {
    pub l1_hits: u64,
    pub l2_hits: u64,
    pub misses: u64,
    pub l1_enabled: bool,
}

pub type MetricsProvider = Arc<dyn Fn() -> Option<L1Stats> + Send + Sync>;

pub fn metrics_headers(provider: Option<&MetricsProvider>) -> Vec<(&'static str, String)> {
    let disabled = vec![("X-CacheKit-L1-Status", "disabled".to_string())];

    let provider = match provider {
        Some(p) => p,
        None => return disabled,
    };

    let stats = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (provider)())) {
        Ok(Some(s)) => s,
        _ => return disabled,
    };

    if !stats.l1_enabled {
        return disabled;
    }

    let total = stats.l1_hits + stats.l2_hits + stats.misses;
    let hit_rate = if total > 0 {
        stats.l1_hits as f64 / total as f64
    } else {
        0.0
    };

    vec![
        ("X-CacheKit-L1-Status", "miss".to_string()),
        ("X-CacheKit-L1-Hits", stats.l1_hits.to_string()),
        ("X-CacheKit-L2-Hits", stats.l2_hits.to_string()),
        ("X-CacheKit-Misses", stats.misses.to_string()),
        ("X-CacheKit-L1-Hit-Rate", format!("{:.3}", hit_rate)),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_when_no_provider() {
        let headers = metrics_headers(None);
        assert_eq!(headers.len(), 1);
        assert_eq!(headers[0], ("X-CacheKit-L1-Status", "disabled".to_string()));
    }

    #[test]
    fn disabled_when_l1_not_enabled() {
        let provider: MetricsProvider = Arc::new(|| {
            Some(L1Stats {
                l1_hits: 0,
                l2_hits: 0,
                misses: 0,
                l1_enabled: false,
            })
        });
        let headers = metrics_headers(Some(&provider));
        assert_eq!(headers[0].1, "disabled");
    }

    #[test]
    fn correct_hit_rate_calculation() {
        let provider: MetricsProvider = Arc::new(|| {
            Some(L1Stats {
                l1_hits: 3,
                l2_hits: 2,
                misses: 5,
                l1_enabled: true,
            })
        });
        let headers = metrics_headers(Some(&provider));
        let rate = headers
            .iter()
            .find(|h| h.0 == "X-CacheKit-L1-Hit-Rate")
            .unwrap();
        assert_eq!(rate.1, "0.300"); // 3 / (3+2+5)
    }

    #[test]
    fn zero_division_guard() {
        let provider: MetricsProvider = Arc::new(|| {
            Some(L1Stats {
                l1_hits: 0,
                l2_hits: 0,
                misses: 0,
                l1_enabled: true,
            })
        });
        let headers = metrics_headers(Some(&provider));
        let rate = headers
            .iter()
            .find(|h| h.0 == "X-CacheKit-L1-Hit-Rate")
            .unwrap();
        assert_eq!(rate.1, "0.000");
    }

    #[test]
    fn disabled_when_provider_panics() {
        let provider: MetricsProvider = Arc::new(|| panic!("boom"));
        let headers = metrics_headers(Some(&provider));
        assert_eq!(headers[0].1, "disabled");
    }
}
