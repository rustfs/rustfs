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
//! `VirtualHostStyleHintLayer` against the gateway stack alone, on a deployment with no server
//! domain: a `PUT` or `DELETE` of `/` addressed to a dotted DNS host is `501 NotImplemented` with a
//! sentence that says why.
//!
//! The words differ on purpose and the difference is accepted (rustfs/backlog#2771, ruling 5): the
//! layer names the host and `RUSTFS_SERVER_DOMAINS`; the gateway's sentence is a constant, because
//! it is written before the request is authenticated. What must stay equal is the status, the
//! code, and which requests are hinted and which are served. The gateway mechanism is the host
//! resolver's diagnostic (`LegacyRustfsVirtualHosts`).

use super::*;
use crate::server::gateway_handlers::test_support::signed_request_with_headers;
use http::HeaderValue;

const HINT: &str = "This request addresses a bucket as a virtual host, and this gateway serves no base domain that the host of the request belongs to. Configure the virtual-hosted base domains this deployment answers for, or address the bucket in the request path.";

fn signed_to(method: Method, host: &'static str, path: &str) -> Request<http_body_util::Empty<Bytes>> {
    let mut headers = HeaderMap::new();
    headers.insert(http::header::HOST, HeaderValue::from_static(host));
    signed_request_with_headers(Signed::Header, method, path, "", ROOT_ACCESS_KEY, ROOT_SECRET_KEY, headers)
}

fn unsigned_to(method: Method, host: &str, path: &str) -> Request<http_body_util::Empty<Bytes>> {
    Request::builder()
        .method(method)
        .uri(path)
        .header("host", host)
        .body(http_body_util::Empty::new())
        .expect("request")
}

#[tokio::test]
async fn a_virtual_hosted_put_or_delete_of_the_root_is_not_implemented() {
    let probe = Probe::new();
    let stack = probe_stack(&probe);
    for host in ["my-bucket.s3.example.com", "my-bucket.localhost:9000"] {
        for method in [Method::PUT, Method::DELETE] {
            // The layer answers before authentication, so a signed and an unsigned caller agree.
            for request in [
                unsigned_to(method.clone(), host, "/"),
                signed_to(method.clone(), "my-bucket.s3.example.com", "/"),
            ] {
                let answer = observe(&stack, request).await;
                assert_eq!(answer.status, StatusCode::NOT_IMPLEMENTED, "{method} {host}: {}", answer.body);
                assert!(answer.body.contains("<Code>NotImplemented</Code>"), "{method} {host}: {}", answer.body);
                assert!(answer.body.contains(HINT), "{method} {host}: {}", answer.body);
            }
        }
    }
    assert_eq!(probe.calls(), 0);
}

#[tokio::test]
async fn the_hint_never_echoes_the_host_the_caller_chose() {
    let probe = Probe::new();
    let stack = probe_stack(&probe);
    let answer = observe(&stack, unsigned_to(Method::PUT, "attacker-chosen.example.com", "/")).await;
    assert_eq!(answer.status, StatusCode::NOT_IMPLEMENTED, "{}", answer.body);
    assert!(!answer.body.contains("attacker-chosen"), "{}", answer.body);
}

#[tokio::test]
async fn path_style_and_address_hosts_are_not_hinted() {
    let probe = Probe::new();
    let stack = probe_stack(&probe);
    // Hosts that cannot be a bucket subdomain: an address, a bare label.
    for host in ["127.0.0.1:9000", "[::1]:9000", "localhost:9000"] {
        let answer = observe(&stack, unsigned_to(Method::PUT, host, "/")).await;
        assert!(!answer.body.contains(HINT), "{host}: {}", answer.body);
    }
    // A path that names a bucket is path-style whatever the host.
    let answer = observe(&stack, unsigned_to(Method::PUT, "my-bucket.s3.example.com", "/photos/key")).await;
    assert!(!answer.body.contains(HINT), "{}", answer.body);
}

#[tokio::test]
async fn a_listing_of_the_root_on_a_dotted_host_is_served() {
    // `GET /` is `ListBuckets` path-style: the layer passes it, and so must the gateway.
    let probe = Probe::new();
    let stack = probe_stack(&probe);
    for host in ["s3.example.com", "my-bucket.s3.example.com"] {
        let answer = observe(
            &stack,
            signed_to(
                Method::GET,
                if host == "s3.example.com" {
                    "s3.example.com"
                } else {
                    "my-bucket.s3.example.com"
                },
                "/",
            ),
        )
        .await;
        assert_eq!(answer.status, StatusCode::OK, "{host}: {}", answer.body);
        assert!(answer.body.contains("<ListAllMyBucketsResult"), "{host}: {}", answer.body);
    }
    assert_eq!(probe.calls(), 2);
}

#[tokio::test]
async fn with_a_server_domain_the_hint_is_not_given() {
    // The layer is installed only when no domain is configured. With one, a request under that
    // domain is addressed to the bucket its host names, and a host outside it is path style:
    // neither is told to configure a domain.
    let probe = Probe::new();
    let stack = probe_stack_for(
        &probe,
        GatewayAssembly {
            server_domains: vec!["s3.example.com".to_owned()],
            ..assembly()
        },
    );
    for host in ["my-bucket.s3.example.com", "my-bucket.other.example.org"] {
        let answer = observe(&stack, unsigned_to(Method::PUT, host, "/")).await;
        assert!(!answer.body.contains(HINT), "{host}: {}", answer.body);
    }
}
