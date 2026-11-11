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

//! The `?metadata=true` listing extension on the gateway stack: an INTERIM route to the legacy
//! service (rustfs/backlog#2734 T2.11, PR "W").
//!
//! Responsible for: [`MetadataListing`], the predicate that says whether a request is
//! `GET /{bucket}?list-type=2&metadata=true` or `GET /{bucket}?versions&metadata=true`, the
//! MinIO-compatible listings that carry `<UserMetadata>`. No gateway DTO can carry that element,
//! and a gateway ListObjectsV2 or ListObjectVersions would answer the request as a plain listing
//! and silently drop it, so while the gateway stack is selected these requests still go to the
//! legacy service.
//! NOT responsible for: serving or authorizing the listing (`app/metadata_route.rs` does, on
//! legacy), or the rule itself: [`MetadataListing`] is the legacy route's own
//! [`MetadataListingHosts`], so the two stacks cannot disagree on which requests are metadata
//! listings, virtual-hosted ones included.
//! Upstream: `server/stack.rs`. Downstream: `app/metadata_route.rs`.
//!
//! Interim. The final plan (PR "M" of the T2.11 design) serves these two listings as gateway
//! dialect operations, authenticated by the gateway (rustfs/gateway#1588); that PR deletes this
//! file.

pub(crate) use crate::app::metadata_route::MetadataListingHosts as MetadataListing;

#[cfg(test)]
mod tests {
    use super::*;
    use http::header::HOST;
    use http::{HeaderMap, Method};

    fn path_style() -> MetadataListing {
        MetadataListing::new(None).expect("no server domain")
    }

    fn virtual_hosted() -> MetadataListing {
        MetadataListing::new(Some(&["s3.test".to_owned()])).expect("valid server domain")
    }

    fn matched(listing: &MetadataListing, method: Method, host: &str, target: &str) -> bool {
        let mut headers = HeaderMap::new();
        headers.insert(HOST, host.parse().expect("test host header"));
        listing.matches(&method, &target.parse().expect("test target"), &headers)
    }

    #[test]
    fn both_metadata_listings_of_a_path_style_bucket_are_matched() {
        let listing = path_style();

        for target in [
            "/bucket?list-type=2&metadata=true",
            "/bucket?versions&metadata=true",
            "/bucket?metadata=true&list-type=2",
            "/bucket?versions=&metadata=true&prefix=a%2F",
            "/bucket/?list-type=2&metadata=true",
            // The value is form-decoded, as legacy decodes it.
            "/bucket?list-type=2&metadata=%74rue",
        ] {
            assert!(matched(&listing, Method::GET, "127.0.0.1:9000", target), "{target}");
        }
    }

    #[test]
    fn a_metadata_listing_of_a_virtual_hosted_bucket_root_is_matched() {
        let listing = virtual_hosted();

        assert!(matched(&listing, Method::GET, "bucket.s3.test:9000", "/?list-type=2&metadata=true"));
        assert!(matched(&listing, Method::GET, "bucket.s3.test", "/?versions&metadata=true"));
    }

    #[test]
    fn a_listing_without_metadata_true_stays_on_the_gateway() {
        let listing = path_style();

        for target in [
            "/bucket?list-type=2",
            "/bucket?versions",
            "/bucket?list-type=2&metadata=false",
            "/bucket?list-type=2&metadata=",
            "/bucket?list-type=2&metadata",
            "/bucket?list-type=2&metadata=TRUE",
            "/bucket?list-type=2&metadata=1",
            // An encoded `=` makes one parameter named `metadata=true`, with no value.
            "/bucket?list-type=2&metadata%3Dtrue",
        ] {
            assert!(!matched(&listing, Method::GET, "127.0.0.1:9000", target), "{target}");
        }
    }

    #[test]
    fn metadata_true_on_something_other_than_a_listing_stays_on_the_gateway() {
        let listing = path_style();

        for target in [
            "/bucket?metadata=true",
            "/bucket?list-type=1&metadata=true",
            "/bucket?uploads&metadata=true",
            "/bucket?location&metadata=true",
            "/bucket",
        ] {
            assert!(!matched(&listing, Method::GET, "127.0.0.1:9000", target), "{target}");
        }
    }

    #[test]
    fn metadata_true_on_an_object_or_on_no_bucket_stays_on_the_gateway() {
        let listing = path_style();

        for target in [
            "/bucket/key?list-type=2&metadata=true",
            "/bucket/dir/key?versions&metadata=true",
            "/?list-type=2&metadata=true",
            "/?versions&metadata=true",
        ] {
            assert!(!matched(&listing, Method::GET, "127.0.0.1:9000", target), "{target}");
        }
        let listing = virtual_hosted();
        assert!(!matched(&listing, Method::GET, "bucket.s3.test", "/key?list-type=2&metadata=true"));
    }

    #[test]
    fn a_metadata_listing_with_another_method_stays_on_the_gateway() {
        let listing = path_style();

        for method in [Method::HEAD, Method::PUT, Method::POST, Method::DELETE] {
            assert!(
                !matched(&listing, method.clone(), "127.0.0.1:9000", "/bucket?list-type=2&metadata=true"),
                "{method}"
            );
        }
    }
}
