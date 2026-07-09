// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundUrlError {
    MissingHost,
    ForbiddenHost { host: String, reason: &'static str },
}

impl fmt::Display for OutboundUrlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OutboundUrlError::MissingHost => write!(f, "outbound URL is missing a host"),
            OutboundUrlError::ForbiddenHost { host, reason } => {
                write!(f, "outbound URL host '{host}' is not allowed: {reason}")
            }
        }
    }
}

impl std::error::Error for OutboundUrlError {}

pub fn validate_outbound_url(url: &Url) -> Result<(), OutboundUrlError> {
    let Some(raw_host) = url.host_str() else {
        return Err(OutboundUrlError::MissingHost);
    };
    let normalized_host = raw_host.trim_end_matches('.').trim_matches(['[', ']']);

    if normalized_host.eq_ignore_ascii_case("localhost") {
        return Err(OutboundUrlError::ForbiddenHost {
            host: raw_host.to_string(),
            reason: "loopback host",
        });
    }

    let Ok(ip) = normalized_host.parse::<IpAddr>() else {
        return Ok(());
    };

    validate_outbound_ip(ip).map_err(|reason| OutboundUrlError::ForbiddenHost {
        host: raw_host.to_string(),
        reason,
    })
}

fn validate_outbound_ip(ip: IpAddr) -> Result<(), &'static str> {
    // Reject pure-IPv6 special forms first, before any IPv4 normalization. ::1 (loopback) and ::
    // (unspecified) are technically IPv4-compatible forms too, so normalizing first would map
    // them to a harmless-looking 0.0.0.1 / 0.0.0.0 and let them through.
    if let IpAddr::V6(v6) = ip {
        if v6.is_loopback() {
            return Err("loopback address");
        }
        if v6.is_unspecified() {
            return Err("unspecified address");
        }
        if v6.is_unicast_link_local() {
            return Err("link-local address");
        }
        if v6.is_unique_local() {
            return Err("private address");
        }
    }

    // Normalize IPv4-mapped (::ffff:a.b.c.d) AND IPv4-compatible (::a.b.c.d) IPv6 addresses to
    // their embedded IPv4 so the IPv4 rules below apply. The std is_* checks on the IPv6 variant
    // never inspect the embedded IPv4, so without this an attacker bypasses the guard with e.g.
    // ::ffff:127.0.0.1, ::127.0.0.1 (loopback) or ::169.254.169.254 (cloud metadata service).
    let ip = match ip {
        IpAddr::V6(v6) => match embedded_ipv4(v6) {
            Some(v4) => IpAddr::V4(v4),
            None => IpAddr::V6(v6),
        },
        other => other,
    };

    if ip.is_unspecified() {
        return Err("unspecified address");
    }

    if ip == IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254)) {
        return Err("metadata endpoint");
    }

    match ip {
        IpAddr::V4(ipv4) => {
            if ipv4.is_loopback() {
                return Err("loopback address");
            }
            if ipv4.is_link_local() {
                return Err("link-local address");
            }
            if ipv4.is_private() {
                return Err("private address");
            }
        }
        // Genuine IPv6 (no embedded IPv4) was already classified above.
        IpAddr::V6(_) => {}
    }

    Ok(())
}

/// Extract the embedded IPv4 from an IPv4-mapped (`::ffff:a.b.c.d`) or IPv4-compatible
/// (`::a.b.c.d`) IPv6 address. The pure-IPv6 specials `::` and `::1` are rejected by the caller
/// before this runs, so returning `None` here means a genuine IPv6 host.
fn embedded_ipv4(v6: Ipv6Addr) -> Option<Ipv4Addr> {
    if let Some(v4) = v6.to_ipv4_mapped() {
        return Some(v4);
    }
    // IPv4-compatible: the top 96 bits are zero and the low 32 bits carry the IPv4.
    let segs = v6.segments();
    if segs[0..6] == [0, 0, 0, 0, 0, 0] {
        let hi = segs[6].to_be_bytes();
        let lo = segs[7].to_be_bytes();
        let v4 = Ipv4Addr::new(hi[0], hi[1], lo[0], lo[1]);
        // `::` and `::1` are already handled by the caller; anything else is a real embedded v4.
        if !v4.is_unspecified() && v4 != Ipv4Addr::new(0, 0, 0, 1) {
            return Some(v4);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{OutboundUrlError, validate_outbound_url};
    use url::Url;

    #[test]
    fn validate_outbound_url_allows_public_hostname() {
        let url = Url::parse("https://example.com/webhook").expect("public URL should parse");
        assert!(validate_outbound_url(&url).is_ok());
    }

    #[test]
    fn validate_outbound_url_rejects_localhost() {
        let url = Url::parse("https://localhost/webhook").expect("localhost URL should parse");
        let err = validate_outbound_url(&url).expect_err("localhost should be rejected");
        assert!(matches!(
            err,
            OutboundUrlError::ForbiddenHost {
                reason: "loopback host",
                ..
            }
        ));
    }

    #[test]
    fn validate_outbound_url_rejects_loopback_ip() {
        let url = Url::parse("https://127.0.0.1/webhook").expect("loopback URL should parse");
        let err = validate_outbound_url(&url).expect_err("loopback IP should be rejected");
        assert!(matches!(
            err,
            OutboundUrlError::ForbiddenHost {
                reason: "loopback address",
                ..
            }
        ));
    }

    #[test]
    fn validate_outbound_url_rejects_private_ip() {
        let url = Url::parse("https://10.0.0.5/webhook").expect("private URL should parse");
        let err = validate_outbound_url(&url).expect_err("private IP should be rejected");
        assert!(matches!(
            err,
            OutboundUrlError::ForbiddenHost {
                reason: "private address",
                ..
            }
        ));
    }

    #[test]
    fn validate_outbound_url_rejects_metadata_endpoint() {
        let url = Url::parse("http://169.254.169.254/latest/meta-data").expect("metadata URL should parse");
        let err = validate_outbound_url(&url).expect_err("metadata endpoint should be rejected");
        assert!(matches!(
            err,
            OutboundUrlError::ForbiddenHost {
                reason: "metadata endpoint",
                ..
            }
        ));
    }

    #[test]
    fn validate_outbound_url_rejects_link_local_ipv6() {
        let url = Url::parse("https://[fe80::1]/hook").expect("IPv6 URL should parse");
        let err = validate_outbound_url(&url).expect_err("link-local IPv6 should be rejected");
        assert!(matches!(
            err,
            OutboundUrlError::ForbiddenHost {
                reason: "link-local address",
                ..
            }
        ));
    }

    #[test]
    fn validate_outbound_url_rejects_ipv4_mapped_loopback() {
        let url = Url::parse("http://[::ffff:127.0.0.1]/webhook").expect("mapped loopback URL should parse");
        let err = validate_outbound_url(&url).expect_err("IPv4-mapped loopback should be rejected");
        assert!(matches!(
            err,
            OutboundUrlError::ForbiddenHost {
                reason: "loopback address",
                ..
            }
        ));
    }

    #[test]
    fn validate_outbound_url_rejects_ipv4_mapped_private() {
        let url = Url::parse("http://[::ffff:10.0.0.5]/webhook").expect("mapped private URL should parse");
        let err = validate_outbound_url(&url).expect_err("IPv4-mapped private should be rejected");
        assert!(matches!(
            err,
            OutboundUrlError::ForbiddenHost {
                reason: "private address",
                ..
            }
        ));
    }

    #[test]
    fn validate_outbound_url_rejects_ipv4_mapped_metadata_endpoint() {
        let url = Url::parse("http://[::ffff:169.254.169.254]/latest/meta-data").expect("mapped metadata URL should parse");
        let err = validate_outbound_url(&url).expect_err("IPv4-mapped metadata endpoint should be rejected");
        assert!(matches!(
            err,
            OutboundUrlError::ForbiddenHost {
                reason: "metadata endpoint",
                ..
            }
        ));
    }

    #[test]
    fn validate_outbound_url_still_allows_public_ipv6() {
        // Pure public IPv6 (Google DNS) must remain allowed after normalization.
        let url = Url::parse("https://[2001:4860:4860::8888]/webhook").expect("public IPv6 URL should parse");
        assert!(validate_outbound_url(&url).is_ok());
    }

    #[test]
    fn validate_outbound_url_rejects_ipv4_compatible_loopback() {
        // IPv4-compatible form ::a.b.c.d (deprecated but still routable) must also be caught.
        let url = Url::parse("http://[::127.0.0.1]/webhook").expect("compatible loopback URL should parse");
        let err = validate_outbound_url(&url).expect_err("IPv4-compatible loopback should be rejected");
        assert!(matches!(
            err,
            OutboundUrlError::ForbiddenHost {
                reason: "loopback address",
                ..
            }
        ));
    }

    #[test]
    fn validate_outbound_url_rejects_ipv4_compatible_metadata_endpoint() {
        let url = Url::parse("http://[::169.254.169.254]/latest/meta-data").expect("compatible metadata URL should parse");
        let err = validate_outbound_url(&url).expect_err("IPv4-compatible metadata endpoint should be rejected");
        assert!(matches!(
            err,
            OutboundUrlError::ForbiddenHost {
                reason: "metadata endpoint",
                ..
            }
        ));
    }

    #[test]
    fn validate_outbound_url_rejects_ipv6_loopback_and_unspecified() {
        // ::1 / :: must stay rejected even though they look like IPv4-compatible forms.
        let err = validate_outbound_url(&Url::parse("http://[::1]/x").unwrap()).expect_err("::1 rejected");
        assert!(matches!(
            err,
            OutboundUrlError::ForbiddenHost {
                reason: "loopback address",
                ..
            }
        ));
        let err = validate_outbound_url(&Url::parse("http://[::]/x").unwrap()).expect_err(":: rejected");
        assert!(matches!(
            err,
            OutboundUrlError::ForbiddenHost {
                reason: "unspecified address",
                ..
            }
        ));
    }
}
