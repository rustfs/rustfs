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

//! Static-website hosting on the gateway stack: an INTERIM route to the legacy service
//! (rustfs/backlog#2734 T2.11, PR "W"; release blocker R-064).
//!
//! Responsible for: [`WebsiteHosts`], the predicate the stack dispatcher asks first, which says
//! whether a request's `Host` names a `RUSTFS_WEBSITE_DOMAINS` domain. Such a request is a
//! website request on legacy (index and error documents, redirects, always anonymous), and the
//! gateway has no website implementation, so while the gateway stack is selected it still goes to
//! the legacy service, which serves it with the released code.
//! NOT responsible for: serving the request (`app/metadata_route.rs` does, on legacy), or the
//! rule itself: [`WebsiteHosts::matches`] calls the legacy route's own
//! [`is_website_request`], so the two stacks cannot disagree on which requests are website ones.
//! Upstream: `server/stack.rs`. Downstream: `app/metadata_route.rs`.
//!
//! Interim. The final plan (PR "W2" of the T2.11 design) serves website requests from an
//! s3s-free RustFS service in this same tower position, built on the owned GetObject and
//! HeadObject use cases once that slice lands; that PR deletes this file.

use crate::app::metadata_route::is_website_request;
use http::{HeaderMap, Uri};
use std::sync::Arc;

/// The website domains, normalised as legacy normalises them (`RUSTFS_WEBSITE_DOMAINS`,
/// lower-cased, no port, no trailing dot). Empty when website hosting is off.
#[derive(Clone, Debug, Default)]
pub(crate) struct WebsiteHosts(Arc<[String]>);

impl WebsiteHosts {
    /// The same list the legacy metadata route is given.
    pub(crate) fn new(domains: Vec<String>) -> Self {
        Self(domains.into())
    }

    /// Whether legacy serves this request as a website request.
    pub(super) fn matches(&self, uri: &Uri, headers: &HeaderMap) -> bool {
        is_website_request(uri, headers, &self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::header::HOST;

    fn hosts(domains: &[&str]) -> WebsiteHosts {
        WebsiteHosts::new(domains.iter().map(|domain| (*domain).to_owned()).collect())
    }

    fn with_host(host: &[u8]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(HOST, http::HeaderValue::from_bytes(host).expect("test host header"));
        headers
    }

    fn matched(website: &WebsiteHosts, host: &str, target: &str) -> bool {
        website.matches(&target.parse().expect("test target"), &with_host(host.as_bytes()))
    }

    #[test]
    fn a_website_domain_or_a_bucket_under_it_is_a_website_request() {
        let website = hosts(&["site.test"]);

        for (host, target) in [
            // The bare domain: legacy answers it 404 as a website, never as ListBuckets.
            ("site.test", "/"),
            ("bucket.site.test", "/"),
            ("bucket.site.test", "/docs/index.html"),
            ("my.dotted.bucket.site.test", "/"),
            ("bucket.site.test:9000", "/"),
            ("Bucket.SITE.Test", "/"),
            ("bucket.site.test.", "/"),
            ("bucket.site.test.:9000", "/key"),
        ] {
            assert!(matched(&website, host, target), "{host} {target} must be a website request");
        }
    }

    #[test]
    fn every_configured_website_domain_is_matched() {
        let website = hosts(&["site.test", "pages.example"]);

        assert!(matched(&website, "a.site.test", "/"));
        assert!(matched(&website, "b.pages.example", "/"));
    }

    #[test]
    fn no_host_is_a_website_request_when_website_hosting_is_off() {
        let website = WebsiteHosts::default();

        for host in ["site.test", "bucket.site.test", "localhost:9000"] {
            assert!(!matched(&website, host, "/"), "{host} with no website domain configured");
        }
    }

    #[test]
    fn a_server_domain_that_is_not_a_website_domain_stays_on_the_gateway() {
        // RUSTFS_SERVER_DOMAINS=s3.test is validated never to overlap a website domain.
        let website = hosts(&["site.test"]);

        assert!(!matched(&website, "s3.test", "/"));
        assert!(!matched(&website, "bucket.s3.test", "/key"));
        assert!(!matched(&website, "bucket.s3.test:9000", "/?list-type=2"));
    }

    #[test]
    fn a_host_that_only_resembles_a_website_domain_stays_on_the_gateway() {
        let website = hosts(&["site.test"]);

        for host in [
            "127.0.0.1:9000",
            "localhost:9000",
            "unrelated.example",
            // A suffix without the label boundary.
            "evilsite.test",
            "bucket.evilsite.test",
            // The domain as a prefix of another one.
            "site.test.evil.example",
            "bucket.site.test.evil.example",
            // A label of the domain, not the domain.
            "site",
            "test",
            "[::1]:9000",
        ] {
            assert!(!matched(&website, host, "/"), "{host} must not be a website request");
        }
    }

    #[test]
    fn a_missing_or_unreadable_host_stays_on_the_gateway() {
        let website = hosts(&["site.test"]);
        let target: Uri = "/".parse().expect("test target");

        assert!(!website.matches(&target, &HeaderMap::new()), "no Host header");
        assert!(!website.matches(&target, &with_host(b"")), "an empty Host header");
        assert!(!website.matches(&target, &with_host(b"bucket .site.test")), "a Host that is no authority");
        assert!(!website.matches(&target, &with_host(b"bucket.site.\xfftest")), "a Host that is not UTF-8");
        // An absolute-form target names a website host, but legacy matches on the Host header only.
        let absolute: Uri = "http://bucket.site.test/".parse().expect("absolute target");
        assert!(!website.matches(&absolute, &with_host(b"127.0.0.1:9000")), "absolute-form target");
    }
}
