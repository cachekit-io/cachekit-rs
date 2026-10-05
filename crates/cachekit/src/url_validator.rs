use crate::error::CachekitError;

const ALLOWED_HOSTS: &[&str] = &["api.cachekit.io", "api.staging.cachekit.io"];

/// Validate that a CachekitIO API URL uses HTTPS, carries no credentials,
/// query or fragment, is not a private IP (SSRF protection), and matches the
/// allow-list unless `allow_custom_host` is set.
pub fn validate_cachekitio_url(
    url_str: &str,
    allow_custom_host: bool,
) -> Result<(), CachekitError> {
    cachekitio_base_url(url_str, allow_custom_host).map(|_| ())
}

/// [`validate_cachekitio_url`], then the URL as the parser serialized it,
/// trailing slashes trimmed: the base every request path is appended to.
/// Requests go to that serialization, never the raw input, so a client whose
/// URL parser differs from this one still reads the host that was checked.
pub(crate) fn cachekitio_base_url(
    url_str: &str,
    allow_custom_host: bool,
) -> Result<String, CachekitError> {
    let parsed = url::Url::parse(url_str)
        .map_err(|_| CachekitError::Config("CachekitIO API URL is malformed".to_string()))?;

    if parsed.scheme() != "https" {
        return Err(CachekitError::Config(
            "CachekitIO API URL must use HTTPS".to_string(),
        ));
    }

    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(CachekitError::Config(
            "CachekitIO API URL must not carry credentials".to_string(),
        ));
    }

    // Request paths are appended to the base URL, so a query or fragment in
    // it would swallow every one of them.
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(CachekitError::Config(
            "CachekitIO API URL must not carry a query or a fragment".to_string(),
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

    Ok(parsed.as_str().trim_end_matches('/').to_owned())
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
            if let Some(v4) = embedded_ipv4(v6) {
                return is_private_ip(std::net::IpAddr::V4(v4));
            }
            // fe80::/10 (link-local) and fec0::/10 (site-local), fc00::/7 (unique local)
            (v6.segments()[0] & 0xff80) == 0xfe80 || (v6.segments()[0] & 0xfe00) == 0xfc00
        }
    }
}

/// The IPv4 address an IPv6 form stands for, checked in its place:
/// IPv4-mapped `::ffff:a.b.c.d` and IPv4-compatible `::a.b.c.d` (which covers
/// `::` and `::1`), NAT64 `64:ff9b::/96`, and 6to4 `2002::/16`.
fn embedded_ipv4(v6: std::net::Ipv6Addr) -> Option<std::net::Ipv4Addr> {
    let v4 = |hi: u16, lo: u16| {
        let [a, b] = hi.to_be_bytes();
        let [c, d] = lo.to_be_bytes();
        std::net::Ipv4Addr::new(a, b, c, d)
    };
    match v6.segments() {
        [0x64, 0xff9b, 0, 0, 0, 0, hi, lo] => Some(v4(hi, lo)),
        [0x2002, hi, lo, ..] => Some(v4(hi, lo)),
        _ => v6.to_ipv4(),
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
    fn blocks_ipv6_private_forms() {
        for host in [
            "[::]",
            "[::1]",
            "[fe80::1]",
            "[febf::1]",        // top of link-local fe80::/10
            "[fec0::1]",        // site-local fec0::/10
            "[feff::1]",        // top of site-local
            "[fc00::1]",        // unique local fc00::/7
            "[fdff::1]",        // top of unique local
            "[::7f00:1]",       // IPv4-compatible 127.0.0.1
            "[::127.0.0.1]",    // same, dotted
            "[::a00:1]",        // IPv4-compatible 10.0.0.1
            "[64:ff9b::a00:1]", // NAT64 10.0.0.1
            "[64:ff9b::127.0.0.1]",
            "[64:ff9b::a9fe:a9fe]", // NAT64 169.254.169.254
            "[2002:a00:1::]",       // 6to4 10.0.0.1
            "[2002:7f00:1::1]",     // 6to4 127.0.0.1
            "[2002:c0a8:101::]",    // 6to4 192.168.1.1
        ] {
            let url = format!("https://{host}");
            assert!(
                validate_cachekitio_url(&url, true).is_err(),
                "{url} must be refused as private"
            );
        }
    }

    #[test]
    fn allows_public_ipv6_forms() {
        for host in [
            "[2001:db8::1]",
            "[64:ff9b::808:808]", // NAT64 of 8.8.8.8
            "[2002:808:808::1]",  // 6to4 of 8.8.8.8
            "[::ffff:8.8.8.8]",   // IPv4-mapped public
        ] {
            let url = format!("https://{host}");
            assert!(
                validate_cachekitio_url(&url, true).is_ok(),
                "{url} is public"
            );
        }
    }

    #[test]
    fn rejects_query_and_fragment() {
        // Request paths are appended to the configured URL as a string, so a
        // query or fragment there would swallow every one of them.
        for url in [
            "https://api.cachekit.io/?",
            "https://api.cachekit.io?",
            "https://api.cachekit.io/?x=1",
            "https://api.cachekit.io/#",
            "https://api.cachekit.io#frag",
        ] {
            let err = validate_cachekitio_url(url, false).unwrap_err();
            assert!(
                err.to_string().contains("query or a fragment"),
                "{url}: {err}"
            );
        }
        assert!(validate_cachekitio_url("https://proxy.example.com/base?", true).is_err());
        assert!(validate_cachekitio_url("https://proxy.example.com/base/", true).is_ok());
    }

    #[test]
    fn rejects_credentials() {
        for url in [
            "https://u:p@api.cachekit.io",
            "https://u@api.cachekit.io",
            "https://:p@api.cachekit.io",
        ] {
            for allow_custom_host in [false, true] {
                let err = validate_cachekitio_url(url, allow_custom_host).unwrap_err();
                assert!(
                    err.to_string().contains("must not carry credentials"),
                    "{url}: {err}"
                );
            }
        }
    }

    #[test]
    fn base_url_is_the_parsed_serialization() {
        // Requests go to the serialization of the URL that was checked, never
        // the raw input, so every URL parser downstream reads the same host.
        for (url, base) in [
            ("https://api.cachekit.io", "https://api.cachekit.io"),
            ("https://api.cachekit.io/", "https://api.cachekit.io"),
            ("https://API.cachekit.io:443//", "https://api.cachekit.io"),
            (
                "https://api.cachekit.io\\@evil.example",
                "https://api.cachekit.io/@evil.example",
            ),
            (
                "https://api.cachekit.io\\evil",
                "https://api.cachekit.io/evil",
            ),
        ] {
            assert_eq!(cachekitio_base_url(url, false).unwrap(), base, "{url}");
        }
        assert_eq!(
            cachekitio_base_url("https://proxy.example.com/base/", true).unwrap(),
            "https://proxy.example.com/base"
        );
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
