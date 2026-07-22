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

use std::collections::HashSet;
#[cfg(test)]
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::OnceLock;
#[cfg(test)]
use std::sync::{Arc, Mutex};
use url::Url;

/// Comma-separated exact HTTP(S) origins that may resolve to otherwise restricted addresses.
///
/// This is an operator-owned process setting. Target configuration cannot extend it. Link-local,
/// metadata, and unspecified addresses remain forbidden even when their origin appears in the list.
pub const ENV_OUTBOUND_ALLOW_ORIGINS: &str = "RUSTFS_OUTBOUND_ALLOW_ORIGINS";

static OUTBOUND_POLICY: OnceLock<Result<OutboundPolicy, OutboundPolicyError>> = OnceLock::new();

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundPolicyError {
    NonUnicodeEnvironment,
    InvalidAllowedOrigin { position: usize },
    MissingHost,
    UnsupportedScheme { scheme: String },
    UserInfoNotAllowed,
    ForbiddenHost { host: String, reason: &'static str },
}

impl fmt::Display for OutboundPolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OutboundPolicyError::NonUnicodeEnvironment => {
                write!(f, "{ENV_OUTBOUND_ALLOW_ORIGINS} must contain valid Unicode")
            }
            OutboundPolicyError::InvalidAllowedOrigin { position } => {
                write!(f, "invalid origin at position {position} in {ENV_OUTBOUND_ALLOW_ORIGINS}")
            }
            OutboundPolicyError::MissingHost => write!(f, "outbound URL is missing a host"),
            OutboundPolicyError::UnsupportedScheme { scheme } => {
                write!(f, "outbound URL scheme '{scheme}' is not allowed")
            }
            OutboundPolicyError::UserInfoNotAllowed => write!(f, "outbound URL userinfo is not allowed"),
            OutboundPolicyError::ForbiddenHost { host, reason } => {
                write!(f, "outbound URL host '{host}' is not allowed: {reason}")
            }
        }
    }
}

impl std::error::Error for OutboundPolicyError {}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct OutboundPolicy {
    allowed_restricted_origins: HashSet<String>,
}

impl OutboundPolicy {
    pub fn from_env() -> Result<Self, OutboundPolicyError> {
        match std::env::var(ENV_OUTBOUND_ALLOW_ORIGINS) {
            Ok(value) => Self::from_allowed_origins(&value),
            Err(std::env::VarError::NotPresent) => Ok(Self::default()),
            Err(std::env::VarError::NotUnicode(_)) => Err(OutboundPolicyError::NonUnicodeEnvironment),
        }
    }

    pub fn from_env_cached() -> Result<&'static Self, OutboundPolicyError> {
        match OUTBOUND_POLICY.get_or_init(Self::from_env) {
            Ok(policy) => Ok(policy),
            Err(err) => Err(err.clone()),
        }
    }

    pub fn from_allowed_origins(value: &str) -> Result<Self, OutboundPolicyError> {
        let mut allowed_restricted_origins = HashSet::new();
        if value.trim().is_empty() {
            return Ok(Self::default());
        }
        for (position, entry) in value.split(',').map(str::trim).enumerate() {
            let position = position + 1;
            if entry.is_empty() {
                return Err(OutboundPolicyError::InvalidAllowedOrigin { position });
            }
            let url = Url::parse(entry).map_err(|_| OutboundPolicyError::InvalidAllowedOrigin { position })?;
            let origin = normalized_origin(&url).ok_or(OutboundPolicyError::InvalidAllowedOrigin { position })?;
            if url.path() != "/" || url.query().is_some() || url.fragment().is_some() {
                return Err(OutboundPolicyError::InvalidAllowedOrigin { position });
            }
            allowed_restricted_origins.insert(origin);
        }
        Ok(Self {
            allowed_restricted_origins,
        })
    }

    pub fn validate_url(&self, url: &Url) -> Result<(), OutboundPolicyError> {
        validate_http_url_shape(url)?;
        let raw_host = url.host_str().ok_or(OutboundPolicyError::MissingHost)?;
        let normalized_host = raw_host.trim_end_matches('.').trim_matches(['[', ']']);
        let result = if normalized_host.eq_ignore_ascii_case("localhost") {
            Err("loopback host")
        } else if let Ok(ip) = normalized_host.parse::<IpAddr>() {
            validate_policy_ip(ip)
        } else {
            Ok(())
        };
        match result {
            Ok(()) => Ok(()),
            Err(reason) if self.allows_restricted_origin(url) && restricted_reason_can_be_overridden(reason) => Ok(()),
            Err(reason) => Err(OutboundPolicyError::ForbiddenHost {
                host: raw_host.to_string(),
                reason,
            }),
        }
    }

    pub fn resolver_for(&self, url: &Url) -> Result<OutboundDnsResolver, OutboundPolicyError> {
        self.validate_url(url)?;
        let host = normalized_host(url).ok_or(OutboundPolicyError::MissingHost)?;
        Ok(OutboundDnsResolver::new(host, self.allows_restricted_origin(url)))
    }

    fn allows_restricted_origin(&self, url: &Url) -> bool {
        normalized_origin(url).is_some_and(|origin| self.allowed_restricted_origins.contains(&origin))
    }
}

#[derive(Clone, Debug)]
pub struct OutboundDnsResolver {
    allowed_restricted_host: Option<String>,
    #[cfg(test)]
    overrides: Option<DnsOverrideAnswers>,
}

#[cfg(test)]
type DnsOverrideAnswers = Arc<Mutex<HashMap<String, VecDeque<Vec<SocketAddr>>>>>;

impl OutboundDnsResolver {
    fn new(host: String, allow_restricted: bool) -> Self {
        Self {
            allowed_restricted_host: allow_restricted.then_some(host),
            #[cfg(test)]
            overrides: None,
        }
    }

    #[cfg(test)]
    fn with_overrides(mut self, overrides: HashMap<String, Vec<IpAddr>>) -> Self {
        let overrides = overrides
            .into_iter()
            .map(|(host, ips)| {
                let addresses = ips.into_iter().map(|ip| SocketAddr::new(ip, 0)).collect();
                (host, VecDeque::from([addresses]))
            })
            .collect();
        self.overrides = Some(Arc::new(Mutex::new(overrides)));
        self
    }

    #[cfg(test)]
    fn with_override_sequence(mut self, host: &str, answers: Vec<Vec<SocketAddr>>) -> Self {
        self.overrides = Some(Arc::new(Mutex::new(HashMap::from([(host.to_string(), VecDeque::from(answers))]))));
        self
    }
}

impl reqwest::dns::Resolve for OutboundDnsResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().trim_end_matches('.').to_ascii_lowercase();
        let allow_restricted = self.allowed_restricted_host.as_deref() == Some(host.as_str());
        #[cfg(test)]
        let overrides = self.overrides.clone();
        Box::pin(async move {
            #[cfg(test)]
            let overridden = overrides.as_ref().and_then(|entries| {
                let mut entries = entries.lock().expect("test DNS overrides must not be poisoned");
                let answers = entries.get_mut(&host)?;
                if answers.len() > 1 {
                    answers.pop_front()
                } else {
                    answers.front().cloned()
                }
            });
            #[cfg(not(test))]
            let overridden: Option<Vec<SocketAddr>> = None;

            let addresses = if let Some(addresses) = overridden {
                addresses
            } else {
                // Do not use the cached resolver in `crate::net`: every new connection must
                // classify the addresses returned at that boundary so DNS rebinding fails closed.
                tokio::net::lookup_host((host.as_str(), 0))
                    .await
                    .map_err(|err| std::io::Error::new(std::io::ErrorKind::NotFound, err))?
                    .collect()
            };
            let addrs = addresses
                .into_iter()
                .filter(|address| resolved_ip_allowed(address.ip(), allow_restricted))
                .collect::<Vec<_>>();
            if addrs.is_empty() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    format!("outbound DNS resolution for '{host}' returned no allowed addresses"),
                )
                .into());
            }
            Ok(Box::new(addrs.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

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

fn validate_http_url_shape(url: &Url) -> Result<(), OutboundPolicyError> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(OutboundPolicyError::UnsupportedScheme {
            scheme: url.scheme().to_string(),
        });
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(OutboundPolicyError::UserInfoNotAllowed);
    }
    Ok(())
}

fn normalized_host(url: &Url) -> Option<String> {
    url.host_str()
        .map(|host| host.trim_end_matches('.').trim_matches(&['[', ']'][..]).to_ascii_lowercase())
}

fn normalized_origin(url: &Url) -> Option<String> {
    validate_http_url_shape(url).ok()?;
    let host = normalized_host(url)?;
    let port = url.port_or_known_default()?;
    // `Url::origin()` preserves a trailing DNS dot, but the resolver treats dotted and undotted
    // names as the same host. Normalize it here so policy matching follows connection semantics.
    if host.parse::<Ipv6Addr>().is_ok() {
        Some(format!("{}://[{host}]:{port}", url.scheme()))
    } else {
        Some(format!("{}://{host}:{port}", url.scheme()))
    }
}

fn restricted_reason_can_be_overridden(reason: &str) -> bool {
    matches!(
        reason,
        "loopback host" | "loopback address" | "private address" | "shared address" | "reserved address"
    )
}

fn resolved_ip_allowed(ip: IpAddr, allow_restricted: bool) -> bool {
    match validate_policy_ip(ip) {
        Ok(()) => true,
        Err(reason) => allow_restricted && restricted_reason_can_be_overridden(reason),
    }
}

fn validate_policy_ip(ip: IpAddr) -> Result<(), &'static str> {
    if is_metadata_endpoint(ip) {
        return Err("metadata endpoint");
    }

    let original_v6 = match ip {
        IpAddr::V6(v6) => Some(v6),
        IpAddr::V4(_) => None,
    };
    let ip = match original_v6 {
        Some(v6) => match policy_embedded_ipv4(v6) {
            Some(v4) => IpAddr::V4(v4),
            None => IpAddr::V6(v6),
        },
        None => ip,
    };

    if is_metadata_endpoint(ip) {
        return Err("metadata endpoint");
    }
    validate_outbound_ip(ip)?;
    if original_v6.is_some_and(|v6| v6.segments()[0] == 0x2002) {
        return Err("reserved address");
    }

    match ip {
        IpAddr::V4(ipv4) => {
            let octets = ipv4.octets();
            if octets[0] == 100 && (64..=127).contains(&octets[1]) {
                return Err("shared address");
            }
            if octets[0] == 0
                || octets[0] >= 224
                || (octets[0] == 192 && ((octets[1] == 0 && matches!(octets[2], 0 | 2)) || (octets[1] == 88 && octets[2] == 99)))
                || (octets[0] == 198 && (octets[1] == 18 || octets[1] == 19))
                || (octets[0] == 198 && octets[1] == 51 && octets[2] == 100)
                || (octets[0] == 203 && octets[1] == 0 && octets[2] == 113)
            {
                return Err("reserved address");
            }
        }
        IpAddr::V6(ipv6) => {
            let segments = ipv6.segments();
            if segments[0] == 0x0064 && segments[1] == 0xff9b && segments[2] == 1 {
                return Err("translation address");
            }
            if is_special_use_ipv6(ipv6) {
                return Err("reserved address");
            }
        }
    }

    Ok(())
}

fn is_special_use_ipv6(ip: Ipv6Addr) -> bool {
    let segments = ip.segments();
    let bits = u128::from_be_bytes(ip.octets());
    let ietf_protocol_assignment = segments[0] == 0x2001
        && segments[1] < 0x0200
        && bits != 0x2001_0001_0000_0000_0000_0000_0000_0001
        && bits != 0x2001_0001_0000_0000_0000_0000_0000_0002
        && segments[1] != 0x0003
        && !(segments[1] == 0x0004 && segments[2] == 0x0112)
        && !(0x0020..=0x003f).contains(&segments[1]);

    (segments[0] & 0xff00) == 0xff00
        || (segments[0] & 0xffc0) == 0xfec0
        || (segments[0] == 0x0100 && segments[1..4] == [0, 0, 0])
        || ietf_protocol_assignment
        || segments[0] == 0x2002
        || (segments[0] == 0x2001 && segments[1] == 0x0db8)
        || (segments[0] == 0x3fff && segments[1] <= 0x0fff)
        || segments[0] == 0x5f00
}

fn is_metadata_endpoint(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ipv4) => matches!(
            ipv4.octets(),
            [169, 254, 169, 254]
                | [169, 254, 170, 2]
                | [169, 254, 170, 23]
                | [169, 254, 0, 23]
                | [100, 100, 100, 200]
                | [168, 63, 129, 16]
        ),
        IpAddr::V6(ipv6) => matches!(
            ipv6.segments(),
            [0xfd00, 0x0ec2, 0, 0, 0, 0, 0, 0x0254]
                | [0xfd00, 0x0ec2, 0, 0, 0, 0, 0, 0x0023]
                | [0xfd20, 0x00ce, 0, 0, 0, 0, 0, 0x0254]
        ),
    }
}

fn policy_embedded_ipv4(v6: Ipv6Addr) -> Option<Ipv4Addr> {
    if let Some(v4) = embedded_ipv4(v6) {
        return Some(v4);
    }
    let segments = v6.segments();
    if segments[..6] == [0, 0, 0, 0, 0xffff, 0] {
        let hi = segments[6].to_be_bytes();
        let lo = segments[7].to_be_bytes();
        return Some(Ipv4Addr::new(hi[0], hi[1], lo[0], lo[1]));
    }
    let octets = v6.octets();
    if octets[..12] == [0x00, 0x64, 0xff, 0x9b, 0, 0, 0, 0, 0, 0, 0, 0] {
        return Some(Ipv4Addr::new(octets[12], octets[13], octets[14], octets[15]));
    }
    if octets[0..2] == [0x20, 0x02] {
        return Some(Ipv4Addr::new(octets[2], octets[3], octets[4], octets[5]));
    }

    None
}

fn validate_outbound_ip(ip: IpAddr) -> Result<(), &'static str> {
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
    if let IpAddr::V4(ipv4) = ip {
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
    Ok(())
}

fn embedded_ipv4(v6: Ipv6Addr) -> Option<Ipv4Addr> {
    if let Some(v4) = v6.to_ipv4_mapped() {
        return Some(v4);
    }
    let segments = v6.segments();
    if segments[0..6] == [0, 0, 0, 0, 0, 0] {
        let hi = segments[6].to_be_bytes();
        let lo = segments[7].to_be_bytes();
        let v4 = Ipv4Addr::new(hi[0], hi[1], lo[0], lo[1]);
        if !v4.is_unspecified() && v4 != Ipv4Addr::new(0, 0, 0, 1) {
            return Some(v4);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{OutboundPolicy, OutboundUrlError, validate_outbound_url};
    use std::collections::HashMap;
    use std::net::SocketAddr;
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
    fn validate_outbound_url_rejects_alternate_ipv4_loopback_syntax() {
        for endpoint in ["http://2130706433/hook", "http://0177.0.0.1/hook", "http://0x7f000001/hook"] {
            let url = Url::parse(endpoint).expect("alternate IPv4 URL should parse");
            assert!(
                validate_outbound_url(&url).is_err(),
                "alternate loopback syntax must be rejected: {endpoint}"
            );
        }
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
    fn outbound_policy_rejects_special_use_addresses() {
        let policy = OutboundPolicy::default();
        for endpoint in [
            "http://0.0.0.1/hook",
            "http://192.0.2.1/hook",
            "http://198.18.0.1/hook",
            "http://198.51.100.1/hook",
            "http://203.0.113.1/hook",
            "http://224.0.0.1/hook",
            "http://[100::1]/hook",
            "http://[2001:100::1]/hook",
            "http://[2001:db8::1]/hook",
            "http://[3fff::1]/hook",
            "http://[5f00::1]/hook",
            "http://[ff02::1]/hook",
        ] {
            let url = Url::parse(endpoint).expect("special-use URL should parse");
            assert!(policy.validate_url(&url).is_err(), "special-use address must be rejected: {endpoint}");
        }
    }

    #[test]
    fn validate_outbound_url_preserves_legacy_shared_address_behavior() {
        let url = Url::parse("http://100.64.0.5/hook").expect("shared URL should parse");
        assert!(validate_outbound_url(&url).is_ok());
        assert!(OutboundPolicy::default().validate_url(&url).is_err());
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
    fn outbound_policy_rejects_embedded_private_destinations() {
        let policy = OutboundPolicy::default();
        for endpoint in [
            "http://[64:ff9b::7f00:1]/hook",
            "http://[64:ff9b::a00:1]/hook",
            "http://[64:ff9b::a9fe:a9fe]/latest/meta-data",
            "http://[2002:7f00:1::]/hook",
            "http://[2002:a00:1::]/hook",
            "http://[::ffff:0:7f00:1]/hook",
            "http://[::ffff:0:a9fe:a9fe]/latest/meta-data",
        ] {
            let url = Url::parse(endpoint).expect("embedded IPv4 URL should parse");
            assert!(
                policy.validate_url(&url).is_err(),
                "embedded private destination must be rejected: {endpoint}"
            );
        }
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

    #[test]
    fn outbound_policy_allows_only_the_exact_operator_origin() {
        let policy = OutboundPolicy::from_allowed_origins("http://127.0.0.1:9443").expect("operator origin should parse");

        assert!(
            policy
                .validate_url(&Url::parse("http://127.0.0.1:9443/hook").expect("allowed URL"))
                .is_ok()
        );
        assert!(
            policy
                .validate_url(&Url::parse("http://127.0.0.1:9444/hook").expect("wrong-port URL"))
                .is_err(),
            "an allowlisted origin must not authorize another port"
        );
        assert!(
            policy
                .validate_url(&Url::parse("http://[::1]:9443/hook").expect("different-host URL"))
                .is_err(),
            "an allowlisted origin must not authorize a different host"
        );

        let shared_policy =
            OutboundPolicy::from_allowed_origins("http://100.64.0.5:9443").expect("shared operator origin should parse");
        assert!(
            shared_policy
                .validate_url(&Url::parse("http://100.64.0.5:9443/hook").expect("shared URL"))
                .is_ok(),
            "an exact operator origin may opt into an internal shared address"
        );
    }

    #[test]
    fn outbound_policy_never_allows_metadata_or_unspecified_addresses() {
        let policy = OutboundPolicy::from_allowed_origins(
            "http://169.254.1.10,http://169.254.169.254,http://169.254.170.2,http://169.254.170.23,http://169.254.0.23,http://100.100.100.200,http://168.63.129.16,http://[fd00:ec2::254],http://[fd00:ec2::23],http://[fd20:ce::254],http://[64:ff9b:1::1],http://0.0.0.0",
        )
        .expect("syntactically valid origins should parse");

        for endpoint in [
            "http://169.254.1.10/hook",
            "http://169.254.169.254/latest",
            "http://169.254.170.2/credentials",
            "http://169.254.170.23/v1/credentials",
            "http://169.254.0.23/latest/meta-data",
            "http://100.100.100.200/latest/meta-data",
            "http://168.63.129.16/machine?comp=goalstate",
            "http://[fd00:ec2::254]/latest",
            "http://[fd00:ec2::23]/v1/credentials",
            "http://[fd20:ce::254]/computeMetadata/v1",
            "http://[64:ff9b:1::1]/hook",
            "http://0.0.0.0/hook",
        ] {
            assert!(
                policy
                    .validate_url(&Url::parse(endpoint).expect("test endpoint should parse"))
                    .is_err(),
                "endpoint must remain forbidden: {endpoint}"
            );
        }
    }

    #[test]
    fn outbound_policy_rejects_non_http_and_userinfo_origins() {
        for origin in [
            "ftp://webhook.internal",
            "https://user@webhook.internal",
            "https://webhook.internal/path",
        ] {
            assert!(
                OutboundPolicy::from_allowed_origins(origin).is_err(),
                "invalid operator origin must fail closed: {origin}"
            );
        }
        assert!(OutboundPolicy::from_allowed_origins("https://one.example,,https://two.example").is_err());
    }

    #[test]
    fn outbound_policy_rejects_non_http_and_userinfo_targets() {
        let policy = OutboundPolicy::default();
        for endpoint in ["ftp://webhook.test/hook", "https://user:secret@webhook.test/hook"] {
            assert!(
                policy
                    .validate_url(&Url::parse(endpoint).expect("target URL should parse"))
                    .is_err(),
                "target must be rejected: {endpoint}"
            );
        }
    }

    #[tokio::test]
    async fn outbound_dns_resolver_filters_rebinding_and_mixed_answers() {
        let policy = OutboundPolicy::default();
        let endpoint = Url::parse("https://webhook.test/hook").expect("endpoint should parse");
        let resolver = policy
            .resolver_for(&endpoint)
            .expect("public hostname should be accepted")
            .with_overrides(HashMap::from([
                (
                    "webhook.test".to_string(),
                    vec![
                        "10.0.0.5".parse().expect("private IP"),
                        "100.64.0.5".parse().expect("shared IP"),
                        "100.100.100.200".parse().expect("metadata IP"),
                        "8.8.8.8".parse().expect("public IP"),
                        "::ffff:127.0.0.1".parse().expect("mapped loopback IP"),
                    ],
                ),
                ("rebound.test".to_string(), vec!["127.0.0.1".parse().expect("loopback IP")]),
            ]));

        let addrs = reqwest::dns::Resolve::resolve(&resolver, "webhook.test".parse().expect("resolver hostname"))
            .await
            .expect("one public address should remain")
            .map(|addr| addr.ip())
            .collect::<Vec<_>>();
        assert_eq!(addrs, vec!["8.8.8.8".parse::<std::net::IpAddr>().expect("public IP")]);
        assert!(
            reqwest::dns::Resolve::resolve(&resolver, "rebound.test".parse().expect("resolver hostname"))
                .await
                .is_err(),
            "a rebound answer containing only restricted addresses must fail closed"
        );
    }

    #[tokio::test]
    async fn exact_operator_origin_allows_private_dns_answers_for_that_host_only() {
        let policy = OutboundPolicy::from_allowed_origins("https://webhook.test:9443").expect("operator origin should parse");
        let resolver = policy
            .resolver_for(&Url::parse("https://webhook.test:9443/hook").expect("endpoint should parse"))
            .expect("allowlisted endpoint should be accepted")
            .with_overrides(HashMap::from([
                ("webhook.test".to_string(), vec!["10.0.0.5".parse().expect("private IP")]),
                ("other.test".to_string(), vec!["10.0.0.6".parse().expect("private IP")]),
            ]));

        let allowed = reqwest::dns::Resolve::resolve(&resolver, "webhook.test".parse().expect("resolver hostname"))
            .await
            .expect("allowlisted host should resolve")
            .count();
        assert_eq!(allowed, 1);
        assert!(
            reqwest::dns::Resolve::resolve(&resolver, "other.test".parse().expect("resolver hostname"))
                .await
                .is_err(),
            "the allowlist must not authorize another private hostname"
        );
    }

    #[tokio::test]
    async fn exact_operator_origin_never_allows_metadata_dns_answers() {
        let policy = OutboundPolicy::from_allowed_origins("https://webhook.test:9443").expect("operator origin should parse");
        let resolver = policy
            .resolver_for(&Url::parse("https://webhook.test:9443/hook").expect("endpoint should parse"))
            .expect("allowlisted endpoint should be accepted")
            .with_overrides(HashMap::from([(
                "webhook.test".to_string(),
                vec![
                    "169.254.170.23".parse().expect("EKS credential IP"),
                    "169.254.0.23".parse().expect("Tencent metadata IP"),
                    "fd00:ec2::23".parse().expect("EKS credential IPv6"),
                    "fd20:ce::254".parse().expect("GCP metadata IPv6"),
                    "0.0.0.0".parse().expect("unspecified IP"),
                ],
            )]));

        assert!(
            reqwest::dns::Resolve::resolve(&resolver, "webhook.test".parse().expect("resolver hostname"))
                .await
                .is_err(),
            "metadata and unspecified answers must remain forbidden for an allowlisted origin"
        );
    }

    #[tokio::test]
    async fn outbound_dns_resolver_preserves_ipv6_scope_id() {
        use std::net::SocketAddrV6;

        let policy = OutboundPolicy::default();
        let scoped = SocketAddr::V6(SocketAddrV6::new("2001:4860:4860::8888".parse().expect("public IPv6"), 0, 0, 7));
        let resolver = policy
            .resolver_for(&Url::parse("https://webhook.test:9443/hook").expect("endpoint should parse"))
            .expect("public endpoint should be accepted")
            .with_override_sequence("webhook.test", vec![vec![scoped]]);

        let resolved = reqwest::dns::Resolve::resolve(&resolver, "webhook.test".parse().expect("resolver hostname"))
            .await
            .expect("public scoped address should resolve")
            .collect::<Vec<_>>();
        assert_eq!(resolved, vec![scoped]);
    }

    #[tokio::test]
    async fn reqwest_rechecks_dns_policy_for_each_new_connection() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind first connection listener");
        let address = listener.local_addr().expect("first connection address");
        let endpoint = Url::parse(&format!("http://rebind.test:{}/hook", address.port())).expect("endpoint should parse");
        let policy = OutboundPolicy::from_allowed_origins(&endpoint.origin().ascii_serialization())
            .expect("loopback test origin should be explicitly allowed");
        let resolver = policy
            .resolver_for(&endpoint)
            .expect("allowlisted endpoint should be accepted")
            .with_override_sequence(
                "rebind.test",
                vec![
                    vec![SocketAddr::new(address.ip(), 0)],
                    vec![SocketAddr::new("169.254.169.254".parse().expect("metadata IP"), 0)],
                ],
            );
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept first connection");
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request).await.expect("read first request");
            stream
                .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .expect("write first response");
        });
        let client = reqwest::Client::builder()
            .no_proxy()
            .dns_resolver(resolver)
            .timeout(std::time::Duration::from_secs(2))
            .build()
            .expect("build test client");

        assert_eq!(
            client
                .get(endpoint.clone())
                .send()
                .await
                .expect("first request should connect")
                .status(),
            reqwest::StatusCode::NO_CONTENT
        );
        server.await.expect("first connection server should finish");
        let error = client
            .get(endpoint)
            .send()
            .await
            .expect_err("the second connection must reject a metadata DNS answer");
        let mut error_chain = vec![error.to_string()];
        let mut source = std::error::Error::source(&error);
        while let Some(error) = source {
            error_chain.push(error.to_string());
            source = error.source();
        }
        assert!(
            error_chain
                .iter()
                .any(|message| message.contains("returned no allowed addresses")),
            "request must fail in the DNS policy layer: {error_chain:?}"
        );
    }
}
