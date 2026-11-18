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
//! `DoubleSlashListBucketsCompatLayer` against the gateway stack alone: `GET //` lists buckets, as
//! it does on the legacy stack where the layer collapses the target to `/` before anything reads it.
//!
//! The rewrite is exactly `GET //`, the query kept, and the signature is verified over the `/` the
//! request was rewritten to: a client that signed `//` fails on both stacks. The gateway mechanism
//! is `address_paths_as_legacy_rustfs` (`rewrite_double_slash_root`).

use super::*;
use crate::server::gateway_handlers::test_support::signed_request_with_headers;

/// A request signed for `signed_path` and sent to `sent_target`.
fn signed_for_and_sent_to(
    method: Method,
    signed_path: &str,
    query: &str,
    sent_target: &str,
) -> Request<http_body_util::Empty<Bytes>> {
    let mut request = signed_request_with_headers(
        Signed::Header,
        method,
        signed_path,
        query,
        ROOT_ACCESS_KEY,
        ROOT_SECRET_KEY,
        HeaderMap::new(),
    );
    *request.uri_mut() = sent_target.parse().expect("a request target");
    request
}

#[tokio::test]
async fn a_double_slash_get_lists_buckets() {
    let probe = Probe::new();
    let stack = probe_stack(&probe);
    // The AWS browser client signs `/` and sends `//`.
    for (query, target) in [("", "//"), ("max-buckets=5", "//?max-buckets=5")] {
        let answer = observe(&stack, signed_for_and_sent_to(Method::GET, "/", query, target)).await;
        assert_eq!(answer.status, StatusCode::OK, "{target}: {}", answer.body);
        assert!(answer.body.contains("<ListAllMyBucketsResult"), "{target}: {}", answer.body);
    }
    // The control: the same request sent as `/`.
    let answer = observe(&stack, signed_for_and_sent_to(Method::GET, "/", "", "/")).await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    assert_eq!(probe.calls(), 3);
}

#[tokio::test]
async fn only_a_get_of_exactly_double_slash_is_collapsed() {
    let probe = Probe::new();
    let stack = probe_stack(&probe);
    for (method, target) in [
        (Method::PUT, "//"),
        (Method::DELETE, "//"),
        (Method::HEAD, "//"),
        (Method::GET, "///"),
        (Method::GET, "//photos"),
    ] {
        let answer = observe(&stack, signed_for_and_sent_to(method.clone(), "/", "", target)).await;
        assert_ne!(answer.status, StatusCode::OK, "{method} {target}: {}", answer.body);
        assert!(!answer.body.contains("<ListAllMyBucketsResult"), "{method} {target}: {}", answer.body);
    }
    assert_eq!(probe.calls(), 0, "none of them reached ListBuckets");
}

#[tokio::test]
async fn a_signature_over_the_double_slash_does_not_verify() {
    // The signature is checked over the collapsed path, so a client that signed what it sent fails.
    let probe = Probe::new();
    let stack = probe_stack(&probe);
    let answer = observe(&stack, signed_for_and_sent_to(Method::GET, "//", "", "//")).await;
    assert_eq!(answer.status, StatusCode::FORBIDDEN, "{}", answer.body);
    assert!(answer.body.contains("<Code>SignatureDoesNotMatch</Code>"), "{}", answer.body);
    assert_eq!(probe.calls(), 0);
}
