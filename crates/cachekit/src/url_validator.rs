use crate::error::CachekitError;

const ALLOWED_HOSTS: &[&str] = &["api.cachekit.io", "api.staging.cachekit.io"];

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

    if let Some(host) = parsed.host_str() {
        if let Ok(ip) = host.parse::<std::net::IpAddr>() {
            if is_private_ip(ip) {
                return Err(CachekitError::Config(
                    "CachekitIO API URL must not point to a private IP address".to_string(),
                ));
            }
        }

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
