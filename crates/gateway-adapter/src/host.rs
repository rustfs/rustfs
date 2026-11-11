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

//! The gateway's [`HostResolver`], built from RustFS's configured domain list.
//!
//! Responsible for: turning the `RUSTFS_SERVER_DOMAINS` list into the gateway's legacy RustFS
//! virtual-host resolver, or into path-style-only addressing when no domain is configured.
//! NOT responsible for: parsing a `Host` header, matching a domain, validating bucket names (all
//! done by [`LegacyRustfsVirtualHosts`]), reading the environment, or deciding that a console
//! deployment serves no domains (the caller passes an empty list then, as `s3_host_domains` in
//! `rustfs/src/server/http.rs` returns `None`).
//! Upstream: the RustFS configuration (`Config::server_domains`). Downstream: the gateway's
//! `ServiceBuilder`, which holds the resolver as `Arc<dyn HostResolver>`.
//!
//! # What "the same as the legacy multi-domain host" means
//!
//! RustFS today hands the same list to the s3s framework's multi-domain host, which ignores ports on both
//! sides when matching a domain, takes the whole prefix in front of a matched domain as the
//! bucket, compares a domain exactly (so a host is matched on the case it arrives in), and, for a
//! host no domain matches, falls back to CNAME-style: a host that is itself a bucket name
//! (lower-cased) is that bucket, and any other valid host (an address, or a host carrying a port)
//! is read path-style. [`LegacyRustfsVirtualHosts`] reproduces exactly that, so a bucket-shaped
//! host outside every configured domain is a bucket of its own and is **not** path-style; only an
//! address, a host with a port, or an empty domain list reads path-style. The tests below pin
//! each of those readings.

use std::sync::Arc;

use rustfs_gateway::{HostResolver, LegacyDomainError, LegacyRustfsVirtualHosts, PathStyleOnly};

/// Builds the host resolver for RustFS's configured domain list.
///
/// An empty list is path-style only. Otherwise the list is read as legacy RustFS reads it: a
/// later entry whose name without its port repeats an earlier one is dropped, and a list that
/// legacy RustFS refuses at start-up is refused here.
///
/// # Errors
///
/// [`LegacyDomainError`] for an entry that is not a domain, or for two entries where one equals or
/// is a subdomain of the other.
pub fn rustfs_host_resolver<I, S>(domains: I) -> Result<Arc<dyn HostResolver>, LegacyDomainError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let domains: Vec<S> = domains.into_iter().collect();
    if domains.is_empty() {
        return Ok(Arc::new(PathStyleOnly));
    }
    Ok(Arc::new(LegacyRustfsVirtualHosts::new(domains)?))
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
mod tests {
    use super::*;
    use http::Method;
    use rustfs_gateway::{Addressing, BucketName, HostQuery, HostRefusal, TargetKind};

    /// What the resolver reads `GET path` with `Host: host` as: the host-named bucket (`None` for
    /// path style), the routed target, and the refusal, if any.
    fn read(resolver: &dyn HostResolver, path: &str, host: &str) -> (Option<String>, TargetKind, Option<HostRefusal>) {
        let method = Method::GET;
        let request = http::Request::builder()
            .method(method.clone())
            .uri(path)
            .header("host", host)
            .body(())
            .expect("a valid request");
        let effective = rustfs_gateway_http::effective_host(&request).expect("an acceptable host");
        let query = HostQuery {
            host: &effective,
            path,
            method: &method,
        };
        let resolved = resolver.resolve(&query);
        let bucket = match &resolved.addressing {
            Addressing::VirtualHosted { bucket, .. } => Some(bucket.as_str().to_owned()),
            Addressing::Path => None,
        };
        (bucket, resolved.target, resolver.refusal(&query))
    }

    fn with_domains(domains: &[&str]) -> Arc<dyn HostResolver> {
        rustfs_host_resolver(domains).expect("a list RustFS accepts")
    }

    fn bucket(domains: &[&str], host: &str) -> Option<String> {
        read(&*with_domains(domains), "/key", host).0
    }

    /// Positive: a host under a configured domain names the bucket in front of it.
    #[test]
    fn host_under_domain_names_the_bucket() {
        assert_eq!(
            read(&*with_domains(&["example.com"]), "/key", "photos.example.com"),
            (Some("photos".to_owned()), TargetKind::Object, None)
        );
        assert_eq!(read(&*with_domains(&["example.com"]), "/", "photos.example.com").1, TargetKind::Bucket);
    }

    /// Positive: the port on the host, or on the configured domain, does not take part in matching.
    #[test]
    fn ports_are_ignored_on_both_sides() {
        assert_eq!(bucket(&["example.com"], "photos.example.com:9000").as_deref(), Some("photos"));
        assert_eq!(bucket(&["example.com:9000"], "photos.example.com").as_deref(), Some("photos"));
        assert_eq!(bucket(&["example.com:9000"], "photos.example.com:1234").as_deref(), Some("photos"));
    }

    /// Positive: the whole prefix in front of the domain is the bucket, dots kept.
    #[test]
    fn whole_prefix_is_the_bucket() {
        assert_eq!(bucket(&["example.com"], "my.dotted.bkt.example.com").as_deref(), Some("my.dotted.bkt"));
    }

    /// Positive: with several domains, each one resolves its own subdomains.
    #[test]
    fn every_configured_domain_resolves() {
        let domains = ["example.com", "s3.local"];
        assert_eq!(bucket(&domains, "abc.example.com").as_deref(), Some("abc"));
        assert_eq!(bucket(&domains, "xyz.s3.local").as_deref(), Some("xyz"));
    }

    /// Positive: the domain itself is the endpoint, so the bucket comes from the path.
    #[test]
    fn the_domain_itself_is_path_style() {
        assert_eq!(bucket(&["example.com"], "example.com"), None);
        assert_eq!(bucket(&["example.com"], "example.com:9000"), None);
    }

    /// Negative: no configured domain means path style only, for every host shape.
    #[test]
    fn no_domain_is_path_style_only() {
        for host in ["photos.example.com", "localhost", "localhost:9000", "10.0.0.1"] {
            assert_eq!(bucket(&[], host), None, "{host}");
        }
    }

    /// Negative: an address, and a host carrying a port that no domain matches, are path style.
    #[test]
    fn addresses_and_unmatched_ports_are_path_style() {
        for host in ["10.0.0.1", "10.0.0.1:9000", "localhost:9000", "other.org:9000"] {
            assert_eq!(bucket(&["example.com"], host), None, "{host}");
        }
    }

    /// Negative: a host that only ends in the same letters as a domain is not under it. It is not
    /// stripped to `bad`; the only reading it gets is the CNAME-style one, the whole host.
    #[test]
    fn suffix_lookalike_is_not_under_the_domain() {
        assert_eq!(bucket(&["example.com"], "badexample.com").as_deref(), Some("badexample.com"));
        assert_ne!(bucket(&["example.com"], "badexample.com").as_deref(), Some("bad"));
        assert_eq!(bucket(&["example.com"], "example.com.evil.org").as_deref(), Some("example.com.evil.org"));
        assert_ne!(bucket(&["example.com"], "example.com.evil.org").as_deref(), Some("example.com.evil"));
    }

    /// Negative: matching a configured domain is exact on case, as the legacy multi-domain host does it, so a
    /// mixed-case host is not stripped to its prefix; it is the lower-cased whole host.
    #[test]
    fn mixed_case_host_is_not_stripped_to_a_prefix() {
        assert_eq!(bucket(&["example.com"], "Photos.Example.COM").as_deref(), Some("photos.example.com"));
        assert_ne!(bucket(&["example.com"], "Photos.Example.COM").as_deref(), Some("photos"));
    }

    /// Negative: a host that is no domain at all is refused, not routed.
    #[test]
    fn a_host_that_is_no_domain_is_refused() {
        let resolver = with_domains(&["example.com"]);
        assert_eq!(read(&*resolver, "/k", "under_score.org").2, Some(HostRefusal::UnusableHost));
    }

    /// Negative: a bucket the host names that bucket rules refuse is refused.
    #[test]
    fn a_host_named_bucket_the_rules_refuse_is_refused() {
        let resolver = with_domains(&["example.com"]);
        assert_eq!(read(&*resolver, "/k", "ab.example.com").2, Some(HostRefusal::RefusedBucket));
        assert_eq!(read(&*resolver, "/k", "bad_host.example.com").2, Some(HostRefusal::RefusedBucket));
    }

    /// Negative: a list legacy RustFS refuses at start-up is refused.
    #[test]
    fn domain_lists_legacy_rustfs_refuses_are_refused() {
        for list in [
            vec!["example.com", "a.example.com"],
            vec!["example.com", "com"],
            vec!["bad_domain"],
            vec![""],
            vec!["example.com:notaport"],
        ] {
            assert!(rustfs_host_resolver(&list).is_err(), "{list:?}");
        }
    }

    /// Positive: a later repeat of a name, whatever its port, is dropped rather than refused, as
    /// `s3_host_domains` drops it before the legacy multi-domain host sees the list.
    #[test]
    fn a_repeat_of_a_domain_is_dropped() {
        assert_eq!(
            bucket(&["example.com:9411", "example.com:9000"], "xyz.example.com").as_deref(),
            Some("xyz")
        );
    }

    /// Positive: a bucket name the resolver reads is one the gateway's own types accept.
    #[test]
    fn the_resolved_bucket_is_a_valid_bucket_name() {
        let name = bucket(&["example.com"], "photos.example.com").expect("a bucket");
        assert!(BucketName::new(name).is_ok());
    }
}
