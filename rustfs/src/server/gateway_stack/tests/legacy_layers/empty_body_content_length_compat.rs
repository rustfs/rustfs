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
//! `EmptyBodyContentLengthCompatLayer`, its S3 half, against the gateway stack alone: a `GET`,
//! `HEAD` or `DELETE` on an S3 path that carries no `Content-Length` and no `Transfer-Encoding` is
//! served, and a body one of them does carry is never read.
//!
//! The layer adds `Content-Length: 0` to such a request so the legacy stack's validation finds a
//! length. The gateway mechanism is `leave_bodies_of_bodyless_operations_unread`: these operations
//! take no body, so nothing asks for a length. The layer's admin and console halves, and its place
//! in the internode lane, are not S3 and are not covered here.

use super::*;
use crate::server::gateway_handlers::test_support::signed_request_with_headers;
use http::HeaderValue;

fn bodyless(method: Method, path: &str) -> Request<http_body_util::Empty<Bytes>> {
    signed_request_with_headers(Signed::Header, method, path, "", ROOT_ACCESS_KEY, ROOT_SECRET_KEY, HeaderMap::new())
}

#[tokio::test]
async fn a_bodyless_s3_request_without_a_content_length_is_served() {
    let probe = Probe::new();
    let stack = probe_stack(&probe);
    let requests = [
        (Method::GET, "/"),
        (Method::GET, "/photos/key"),
        (Method::HEAD, "/photos/key"),
        (Method::DELETE, "/photos/key"),
    ];
    for (method, path) in &requests {
        let request = bodyless(method.clone(), path);
        assert!(
            !request.headers().contains_key(http::header::CONTENT_LENGTH),
            "the request carries no length"
        );
        assert!(!request.headers().contains_key(http::header::TRANSFER_ENCODING));
        let answer = observe(&stack, request).await;
        assert!(answer.status.is_success(), "{method} {path}: {} {}", answer.status, answer.body);
    }
    assert_eq!(probe.calls(), requests.len());
}

#[tokio::test]
async fn a_body_on_a_bodyless_operation_is_never_read() {
    // A body that never yields: an operation that read it would wait for the body deadline.
    let probe = Probe::new();
    let stack = probe_stack(&probe);
    for (method, path, framing) in [
        (Method::GET, "/photos/key", ("transfer-encoding", "chunked")),
        (Method::DELETE, "/photos/key", ("transfer-encoding", "chunked")),
        (Method::GET, "/photos/key", ("content-length", "5")),
        (Method::DELETE, "/photos/key", ("content-length", "5")),
    ] {
        let mut request =
            bodyless(method.clone(), path).map(|_| StalledBody(Arc::new(std::sync::atomic::AtomicBool::new(false))));
        request
            .headers_mut()
            .insert(http::HeaderName::from_static(framing.0), HeaderValue::from_static(framing.1));
        let answer = tokio::time::timeout(Duration::from_secs(5), observe(&stack, request))
            .await
            .unwrap_or_else(|_| panic!("{method} {path} {framing:?}: the body was waited for"));
        assert!(
            answer.status.is_success(),
            "{method} {path} {framing:?}: {} {}",
            answer.status,
            answer.body
        );
    }
    assert_eq!(probe.calls(), 4);
}

#[tokio::test]
async fn a_zero_content_length_is_the_same_request() {
    let probe = Probe::new();
    let stack = probe_stack(&probe);
    let mut request = bodyless(Method::DELETE, "/photos/key");
    request
        .headers_mut()
        .insert(http::header::CONTENT_LENGTH, HeaderValue::from_static("0"));
    let answer = observe(&stack, request).await;
    assert!(answer.status.is_success(), "{}", answer.body);
}

/// A body of `data` that records whether anything polled it.
struct CarriedBody {
    data: Option<Bytes>,
    polled: Arc<std::sync::atomic::AtomicBool>,
}

impl http_body::Body for CarriedBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        self.polled.store(true, std::sync::atomic::Ordering::SeqCst);
        std::task::Poll::Ready(self.data.take().map(http_body::Frame::data).map(Ok))
    }
}

#[tokio::test]
async fn the_body_a_bodyless_operation_carries_is_never_polled() {
    // Not only is the answer not waiting for it: nothing so much as asks the body for a frame.
    let probe = Probe::new();
    let stack = probe_stack(&probe);
    for method in [Method::GET, Method::HEAD, Method::DELETE] {
        let polled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut request = bodyless(method.clone(), "/photos/key").map(|_| CarriedBody {
            data: Some(Bytes::from_static(b"hello")),
            polled: Arc::clone(&polled),
        });
        request
            .headers_mut()
            .insert(http::header::CONTENT_LENGTH, HeaderValue::from_static("5"));
        let answer = observe(&stack, request).await;
        assert!(answer.status.is_success(), "{method}: {} {}", answer.status, answer.body);
        assert!(!polled.load(std::sync::atomic::Ordering::SeqCst), "{method}: body polled");
    }
    assert_eq!(probe.calls(), 3);
}
