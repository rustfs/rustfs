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
use std::net::{IpAddr, Ipv4Addr};
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
        IpAddr::V6(ipv6) => {
            if ipv6.is_loopback() {
                return Err("loopback address");
            }
            if ipv6.is_unicast_link_local() {
                return Err("link-local address");
            }
            if ipv6.is_unique_local() {
                return Err("private address");
            }
        }
    }

    Ok(())
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
}
