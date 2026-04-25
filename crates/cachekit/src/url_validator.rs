use crate::error::CachekitError;

const ALLOWED_HOSTS: &[&str] = &["api.cachekit.io", "api.staging.cachekit.io"];

/// Validate that a CachekitIO API URL uses HTTPS, is not a private IP (SSRF
/// protection), and matches the allow-list unless `allow_custom_host` is set.
pub fn validate_cachekitio_url(
    url_str: &str,
    allow_custom_host: bool,
) -> Result<(), CachekitError> {
    let parsed = url::Url::parse(url_str)
        .map_err(|_| CachekitError::Config("CachekitIO API URL is malformed".to_string()))?;

    if parsed.scheme() != "https" {
        return Err(CachekitError::Config(
            "CachekitIO API URL must use HTTPS".to_string(),
        ));
    }

    // Check for private IPs using the parsed Host enum (handles IPv6 brackets correctly).
    match parsed.host() {
        Some(url::Host::Ipv4(v4)) if is_private_ip(std::net::IpAddr::V4(v4)) => {
            return Err(CachekitError::Config(
                "CachekitIO API URL must not point to a private IP address".to_string(),
            ));
        }
        Some(url::Host::Ipv6(v6)) if is_private_ip(std::net::IpAddr::V6(v6)) => {
            return Err(CachekitError::Config(
                "CachekitIO API URL must not point to a private IP address".to_string(),
            ));
        }
        _ => {}
    }

    if let Some(host) = parsed.host_str() {
        // Strip brackets from IPv6 host_str for allowlist matching.
        let host = host.trim_start_matches('[').trim_end_matches(']');
        if !allow_custom_host && !ALLOWED_HOSTS.contains(&host) {
            return Err(CachekitError::Config(
                "API URL hostname not permitted. See documentation.".to_string(),
            ));
        }
    }

    Ok(())
}

fn is_private_ip(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.octets()[0] == 0
        }
        std::net::IpAddr::V6(v6) => {
            // Detect IPv4-mapped (::ffff:x.x.x.x) addresses and check the embedded
            // IPv4 address against the private range blocklist.
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_private_ip(std::net::IpAddr::V4(v4));
            }
            v6.is_loopback()
                || v6.is_unspecified()
                // fe80::/10 (link-local) and fc00::/7 (unique local)
                || (v6.segments()[0] & 0xffc0) == 0xfe80
                || (v6.segments()[0] & 0xfe00) == 0xfc00
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_production_url() {
        assert!(validate_cachekitio_url("https://api.cachekit.io", false).is_ok());
    }

    #[test]
    fn accepts_staging_url() {
        assert!(validate_cachekitio_url("https://api.staging.cachekit.io", false).is_ok());
    }

    #[test]
    fn rejects_http() {
        assert!(validate_cachekitio_url("http://api.cachekit.io", false).is_err());
    }

    #[test]
    fn rejects_unknown_host() {
        assert!(validate_cachekitio_url("https://evil.com", false).is_err());
    }

    #[test]
    fn allows_custom_host() {
        assert!(validate_cachekitio_url("https://my-proxy.internal.com", true).is_ok());
    }

    #[test]
    fn blocks_private_ips_even_with_custom_host() {
        assert!(validate_cachekitio_url("https://127.0.0.1", true).is_err());
        assert!(validate_cachekitio_url("https://10.0.0.1", true).is_err());
        assert!(validate_cachekitio_url("https://192.168.1.1", true).is_err());
        assert!(validate_cachekitio_url("https://169.254.169.254", true).is_err());
    }

    #[test]
    fn blocks_ipv4_mapped_ipv6() {
        // ::ffff:127.0.0.1 and ::ffff:169.254.169.254 must be blocked
        assert!(validate_cachekitio_url("https://[::ffff:127.0.0.1]", true).is_err());
        assert!(validate_cachekitio_url("https://[::ffff:10.0.0.1]", true).is_err());
        assert!(validate_cachekitio_url("https://[::ffff:169.254.169.254]", true).is_err());
        assert!(validate_cachekitio_url("https://[::ffff:192.168.1.1]", true).is_err());
    }

    #[test]
    fn generic_error_message() {
        let err = validate_cachekitio_url("https://evil.com", false).unwrap_err();
        let msg = err.to_string();
        assert!(
            !msg.contains("api.cachekit.io"),
            "Should not enumerate allowlist"
        );
        assert!(
            !msg.contains("allow_custom_host"),
            "Should not reveal bypass flag"
        );
    }
}
