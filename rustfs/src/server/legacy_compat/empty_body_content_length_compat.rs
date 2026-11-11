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
//! `EmptyBodyContentLengthCompatLayer`: an s3s compatibility patch, installed
//! by `server::http::external_service_stack!` (in both the external and the
//! internode lane) and pinned there by `stack_census`.
//!
//! Patches: s3s wants a `Content-Length` before it validates a request, while
//! some clients omit the header on routes whose request never carries a body.
//! Introduced in rustfs/rustfs#2888.
//! Replaced by: a gateway `StageFilter::on_wire` (gateway `docs/middleware.md`,
//! "Level 2: `StageFilter`"; "The nine RustFS tower patch layers", row 5).
//! Removed by: T2.12 (rustfs/backlog#2771).

use crate::admin::console::is_console_path;
use crate::server::is_admin_path;
use crate::server::layer::ConditionalCorsLayer;
use http::{HeaderValue, Method, Request as HttpRequest, Response};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::{Layer, Service};

/// Adds `Content-Length: 0` for routes whose requests are known to carry no
/// body, but where some S3-compatible clients omit the header entirely.
///
/// The normalization runs before authentication so downstream request
/// validation sees an explicit empty body length without requiring every
/// handler to special-case absent `Content-Length`.
#[derive(Clone)]
pub struct EmptyBodyContentLengthCompatLayer;

impl<S> Layer<S> for EmptyBodyContentLengthCompatLayer {
    type Service = EmptyBodyContentLengthCompatService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        EmptyBodyContentLengthCompatService { inner }
    }
}

#[derive(Clone)]
pub struct EmptyBodyContentLengthCompatService<S> {
    inner: S,
}

impl<S, ReqBody, ResBody> Service<HttpRequest<ReqBody>> for EmptyBodyContentLengthCompatService<S>
where
    S: Service<HttpRequest<ReqBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Send + 'static,
{
    type Response = Response<ResBody>;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, mut req: HttpRequest<ReqBody>) -> Self::Future {
        if should_force_zero_content_length_for_empty_body_route(&req) {
            req.headers_mut()
                .insert(http::header::CONTENT_LENGTH, HeaderValue::from_static("0"));
            req.headers_mut().remove(http::header::TRANSFER_ENCODING);
        }

        let mut inner = self.inner.clone();
        Box::pin(async move { inner.call(req).await.map_err(Into::into) })
    }
}

fn should_force_zero_content_length_for_empty_body_route<B>(req: &HttpRequest<B>) -> bool {
    if req.headers().contains_key(http::header::CONTENT_LENGTH) {
        return false;
    }

    if is_empty_body_admin_path(req.method(), req.uri()) {
        return true;
    }

    if req.headers().contains_key(http::header::TRANSFER_ENCODING) {
        return false;
    }

    if is_empty_body_console_path(req.method(), req.uri()) {
        return true;
    }

    is_empty_body_s3_path(req.method(), req.uri())
}

fn is_empty_body_admin_path(method: &Method, uri: &http::Uri) -> bool {
    let path = uri.path();
    match *method {
        Method::GET => is_admin_path(path),
        Method::PUT => matches!(
            path,
            "/minio/admin/v3/set-user-status"
                | "/minio/admin/v3/set-group-status"
                | "/minio/admin/v3/restore-config-history-kv"
                | "/rustfs/admin/v3/set-user-status"
                | "/rustfs/admin/v3/set-group-status"
                | "/rustfs/admin/v3/restore-config-history-kv"
        ),
        Method::POST => {
            matches!(
                path,
                "/minio/admin/v3/rebalance/start"
                    | "/minio/admin/v3/rebalance/stop"
                    | "/minio/admin/v3/background-heal/status"
                    | "/minio/admin/v3/pools/decommission"
                    | "/minio/admin/v3/pools/cancel"
                    | "/rustfs/admin/v3/rebalance/start"
                    | "/rustfs/admin/v3/rebalance/stop"
                    | "/rustfs/admin/v3/background-heal/status"
                    | "/rustfs/admin/v3/pools/decommission"
                    | "/rustfs/admin/v3/pools/cancel"
            ) || is_heal_status_query(path, uri.query())
        }
        _ => false,
    }
}

fn is_heal_status_query(path: &str, query: Option<&str>) -> bool {
    let Some(query) = query else {
        return false;
    };

    if !path.find("/v3/heal/").is_some_and(|index| path[..index].ends_with("/admin")) {
        return false;
    }

    query.split('&').any(|param| {
        param
            .split_once('=')
            .map(|(key, value)| key == "clientToken" && !value.is_empty())
            .unwrap_or(false)
    })
}

fn is_empty_body_s3_path(method: &Method, uri: &http::Uri) -> bool {
    matches!(*method, Method::GET | Method::HEAD | Method::DELETE) && ConditionalCorsLayer::is_s3_path(uri.path())
}

pub(in crate::server) fn is_empty_body_console_path(method: &Method, uri: &http::Uri) -> bool {
    matches!(*method, Method::GET | Method::HEAD) && is_console_path(uri.path())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::CONSOLE_PREFIX;
    use crate::server::layer::tests::HeaderCaptureService;
    use crate::server::{
        ADMIN_PREFIX, HEALTH_COMPAT_LIVE_PATH, HEALTH_PREFIX, HEALTH_READY_PATH, MINIO_ADMIN_PREFIX, MINIO_ADMIN_V3_PREFIX,
    };
    use crate::server::{FAVICON_PATH, LICENSE, VERSION};
    use http::Request;

    #[test]
    fn admin_chunked_put_without_content_length_is_normalized() {
        let request = Request::builder()
            .method(Method::PUT)
            .uri("/minio/admin/v3/set-user-status?accessKey=test&status=enabled")
            .body(())
            .expect("request");

        assert!(should_force_zero_content_length_for_empty_body_route(&request));
    }

    #[test]
    fn admin_empty_body_post_without_content_length_is_normalized() {
        let paths = [
            "/minio/admin/v3/rebalance/start",
            "/minio/admin/v3/rebalance/stop",
            "/minio/admin/v3/pools/decommission?pool=http%3A%2F%2Fminio-%7B1...4%7D%3A9000%2Fdata%7B1...2%7D",
            "/minio/admin/v3/pools/cancel?pool=http%3A%2F%2Fminio-%7B1...4%7D%3A9000%2Fdata%7B1...2%7D",
            "/rustfs/admin/v3/rebalance/start",
            "/rustfs/admin/v3/rebalance/stop",
            "/rustfs/admin/v3/pools/decommission?pool=http%3A%2F%2Fminio-%7B1...4%7D%3A9000%2Fdata%7B1...2%7D",
            "/rustfs/admin/v3/pools/cancel?pool=http%3A%2F%2Fminio-%7B1...4%7D%3A9000%2Fdata%7B1...2%7D",
        ];

        for path in paths {
            let request = Request::builder().method(Method::POST).uri(path).body(()).expect("request");

            assert!(
                should_force_zero_content_length_for_empty_body_route(&request),
                "{path} should force Content-Length: 0"
            );
        }
    }

    #[test]
    fn admin_empty_body_get_without_content_length_is_normalized() {
        let paths = [
            format!("{MINIO_ADMIN_PREFIX}/v3/is-admin"),
            format!("{MINIO_ADMIN_PREFIX}/v3/accountinfo"),
            format!("{MINIO_ADMIN_PREFIX}/v3/info"),
            format!("{ADMIN_PREFIX}/v3/is-admin"),
            format!("{ADMIN_PREFIX}/v3/accountinfo"),
            format!("{ADMIN_PREFIX}/v3/info"),
        ];

        for path in paths {
            let request = Request::builder()
                .method(Method::GET)
                .uri(path.as_str())
                .body(())
                .expect("request");

            assert!(
                should_force_zero_content_length_for_empty_body_route(&request),
                "{path} should force Content-Length: 0"
            );
        }
    }

    #[test]
    fn non_s3_non_admin_get_without_content_length_is_not_normalized() {
        let paths = ["/rustfs/rpc/read_file_stream", "/health", "/profile/cpu"];

        for path in paths {
            let request = Request::builder().method(Method::GET).uri(path).body(()).expect("request");

            assert!(
                !should_force_zero_content_length_for_empty_body_route(&request),
                "{path} should not force Content-Length: 0"
            );
        }
    }

    #[tokio::test]
    async fn empty_body_layer_inserts_zero_content_length_for_admin_get() {
        let capture = HeaderCaptureService::default();
        let headers = capture.headers();
        let mut service = EmptyBodyContentLengthCompatLayer.layer(capture);
        let request = Request::builder()
            .method(Method::GET)
            .uri("/rustfs/admin/v3/accountinfo")
            .body(())
            .expect("request");

        let _ = service.call(request).await.expect("service call");

        let headers = headers.lock().expect("captured headers").take().expect("captured headers");
        assert_eq!(headers.get(http::header::CONTENT_LENGTH).unwrap(), "0");
    }

    #[test]
    fn s3_empty_body_get_without_content_length_is_normalized() {
        let paths = [
            "/?x-id=ListBuckets",
            "/",
            "/bucket?list-type=2",
            "/bucket/object.txt",
            "/rustfs/administrator/object",
            "/minio/adminx/object",
        ];

        for path in paths {
            let request = Request::builder().method(Method::GET).uri(path).body(()).expect("request");

            assert!(
                should_force_zero_content_length_for_empty_body_route(&request),
                "{path} should force Content-Length: 0"
            );
        }
    }

    #[test]
    fn s3_empty_body_head_without_content_length_is_normalized() {
        let request = Request::builder()
            .method(Method::HEAD)
            .uri("/bucket/object.txt")
            .body(())
            .expect("request");

        assert!(should_force_zero_content_length_for_empty_body_route(&request));
    }

    #[test]
    fn console_empty_body_get_without_content_length_is_normalized() {
        let paths = [
            FAVICON_PATH.to_string(),
            format!("{CONSOLE_PREFIX}{LICENSE}"),
            format!("{CONSOLE_PREFIX}{VERSION}"),
            format!("{CONSOLE_PREFIX}{HEALTH_PREFIX}"),
            format!("{CONSOLE_PREFIX}{HEALTH_COMPAT_LIVE_PATH}"),
            format!("{CONSOLE_PREFIX}{HEALTH_READY_PATH}"),
            format!("{CONSOLE_PREFIX}/index.html"),
            format!("{CONSOLE_PREFIX}/assets/app.js"),
        ];

        for path in paths {
            let request = Request::builder()
                .method(Method::GET)
                .uri(path.as_str())
                .body(())
                .expect("request");

            assert!(
                should_force_zero_content_length_for_empty_body_route(&request),
                "{path} should force Content-Length: 0"
            );
        }
    }

    #[test]
    fn console_empty_body_head_without_content_length_is_normalized() {
        let paths = [
            format!("{CONSOLE_PREFIX}{HEALTH_PREFIX}"),
            format!("{CONSOLE_PREFIX}{HEALTH_COMPAT_LIVE_PATH}"),
            format!("{CONSOLE_PREFIX}{HEALTH_READY_PATH}"),
            format!("{CONSOLE_PREFIX}/index.html"),
        ];

        for path in paths {
            let request = Request::builder()
                .method(Method::HEAD)
                .uri(path.as_str())
                .body(())
                .expect("request");

            assert!(
                should_force_zero_content_length_for_empty_body_route(&request),
                "{path} should force Content-Length: 0"
            );
        }
    }

    #[tokio::test]
    async fn empty_body_layer_inserts_zero_content_length_for_s3_and_console_get() {
        for path in ["/?x-id=ListBuckets".to_string(), format!("{CONSOLE_PREFIX}/index.html")] {
            let capture = HeaderCaptureService::default();
            let headers = capture.headers();
            let mut service = EmptyBodyContentLengthCompatLayer.layer(capture);
            let request = Request::builder()
                .method(Method::GET)
                .uri(path.as_str())
                .body(())
                .expect("request");

            let _ = service.call(request).await.expect("service call");

            let headers = headers.lock().expect("captured headers").take().expect("captured headers");
            assert_eq!(headers.get(http::header::CONTENT_LENGTH).unwrap(), "0");
        }
    }

    #[tokio::test]
    async fn empty_body_layer_inserts_zero_content_length_for_admin_post() {
        let capture = HeaderCaptureService::default();
        let headers = capture.headers();
        let mut service = EmptyBodyContentLengthCompatLayer.layer(capture);
        let request = Request::builder()
            .method(Method::POST)
            .uri("/rustfs/admin/v3/rebalance/start")
            .body(())
            .expect("request");

        let _ = service.call(request).await.expect("service call");

        let headers = headers.lock().expect("captured headers").take().expect("captured headers");
        assert_eq!(headers.get(http::header::CONTENT_LENGTH).unwrap(), "0");
    }

    #[tokio::test]
    async fn empty_body_layer_inserts_zero_content_length_for_admin_put() {
        let capture = HeaderCaptureService::default();
        let headers = capture.headers();
        let mut service = EmptyBodyContentLengthCompatLayer.layer(capture);
        let request = Request::builder()
            .method(Method::PUT)
            .uri("/rustfs/admin/v3/set-group-status?group=test&status=enabled")
            .body(())
            .expect("request");

        let _ = service.call(request).await.expect("service call");

        let headers = headers.lock().expect("captured headers").take().expect("captured headers");
        assert_eq!(headers.get(http::header::CONTENT_LENGTH).unwrap(), "0");
    }

    #[tokio::test]
    async fn empty_body_layer_normalizes_admin_chunked_request_without_content_length() {
        let capture = HeaderCaptureService::default();
        let headers = capture.headers();
        let mut service = EmptyBodyContentLengthCompatLayer.layer(capture);
        let request = Request::builder()
            .method(Method::POST)
            .uri(format!("{MINIO_ADMIN_V3_PREFIX}/rebalance/start"))
            .header(http::header::TRANSFER_ENCODING, "chunked")
            .body(())
            .expect("request");

        let _ = service.call(request).await.expect("service call");

        let headers = headers.lock().expect("captured headers").take().expect("captured headers");
        assert_eq!(headers.get(http::header::CONTENT_LENGTH).unwrap(), "0");
        assert!(headers.get(http::header::TRANSFER_ENCODING).is_none());
    }

    #[test]
    fn admin_heal_status_query_without_content_length_is_normalized() {
        let paths = [
            format!("{ADMIN_PREFIX}/v3/heal/?clientToken=root-heal"),
            format!("{ADMIN_PREFIX}/v3/heal/bucket?clientToken=bucket-heal"),
        ];

        for path in paths {
            let request = Request::builder()
                .method(Method::POST)
                .uri(path.clone())
                .header(http::header::TRANSFER_ENCODING, "chunked")
                .body(())
                .expect("request");

            assert!(
                should_force_zero_content_length_for_empty_body_route(&request),
                "{path} should force Content-Length: 0"
            );
        }
    }

    #[test]
    fn admin_background_heal_status_without_content_length_is_normalized() {
        let paths = [
            format!("{MINIO_ADMIN_V3_PREFIX}/background-heal/status"),
            format!("{ADMIN_PREFIX}/v3/background-heal/status"),
        ];

        for path in paths {
            let request = Request::builder()
                .method(Method::POST)
                .uri(path.clone())
                .header(http::header::TRANSFER_ENCODING, "chunked")
                .body(())
                .expect("request");

            assert!(
                should_force_zero_content_length_for_empty_body_route(&request),
                "{path} should force Content-Length: 0"
            );
        }
    }

    #[test]
    fn admin_heal_start_without_status_token_is_not_normalized() {
        let request = Request::builder()
            .method(Method::POST)
            .uri(format!("{ADMIN_PREFIX}/v3/heal/"))
            .header(http::header::TRANSFER_ENCODING, "chunked")
            .body(())
            .expect("request");

        assert!(!should_force_zero_content_length_for_empty_body_route(&request));
    }

    #[test]
    fn s3_delete_object_version_without_content_length_is_normalized() {
        let request = Request::builder()
            .method(Method::DELETE)
            .uri("/bucket/object.txt?versionId=3HL4kqtJlcpXrof3Gj0OmxJnVBH40Nrjfkd")
            .body(())
            .expect("request");

        assert!(should_force_zero_content_length_for_empty_body_route(&request));
    }

    #[tokio::test]
    async fn empty_body_layer_inserts_zero_content_length_for_s3_delete_object_version() {
        let capture = HeaderCaptureService::default();
        let headers = capture.headers();
        let mut service = EmptyBodyContentLengthCompatLayer.layer(capture);
        let request = Request::builder()
            .method(Method::DELETE)
            .uri("/bucket/object.txt?versionId=3HL4kqtJlcpXrof3Gj0OmxJnVBH40Nrjfkd")
            .body(())
            .expect("request");

        let _ = service.call(request).await.expect("service call");

        let headers = headers.lock().expect("captured headers").take().expect("captured headers");
        assert_eq!(headers.get(http::header::CONTENT_LENGTH).unwrap(), "0");
    }

    #[tokio::test]
    async fn empty_body_layer_preserves_explicit_content_length_header() {
        let capture = HeaderCaptureService::default();
        let headers = capture.headers();
        let mut service = EmptyBodyContentLengthCompatLayer.layer(capture);
        let request = Request::builder()
            .method(Method::PUT)
            .uri("/minio/admin/v3/set-group-status?group=test&status=enabled")
            .header(http::header::CONTENT_LENGTH, "7")
            .body(())
            .expect("request");

        let _ = service.call(request).await.expect("service call");

        let headers = headers.lock().expect("captured headers").take().expect("captured headers");
        assert_eq!(headers.get(http::header::CONTENT_LENGTH).unwrap(), "7");
    }

    #[test]
    fn admin_request_with_explicit_content_length_is_left_unchanged() {
        let request = Request::builder()
            .method(Method::PUT)
            .uri("/minio/admin/v3/set-group-status?group=test&status=enabled")
            .header(http::header::CONTENT_LENGTH, "0")
            .body(())
            .expect("request");

        assert!(!should_force_zero_content_length_for_empty_body_route(&request));
    }

    #[test]
    fn admin_restore_config_history_without_content_length_is_normalized() {
        let request = Request::builder()
            .method(Method::PUT)
            .uri("/minio/admin/v3/restore-config-history-kv?restoreId=test")
            .body(())
            .expect("request");

        assert!(should_force_zero_content_length_for_empty_body_route(&request));
    }

    #[test]
    fn s3_put_object_is_not_normalized() {
        let request = Request::builder()
            .method(Method::PUT)
            .uri("/bucket/object")
            .body(())
            .expect("request");

        assert!(!should_force_zero_content_length_for_empty_body_route(&request));
    }

    #[test]
    fn s3_delete_bucket_without_content_length_is_normalized() {
        let request = Request::builder()
            .method(Method::DELETE)
            .uri("/bucket")
            .body(())
            .expect("request");

        assert!(should_force_zero_content_length_for_empty_body_route(&request));
    }

    #[test]
    fn s3_delete_object_without_version_id_is_normalized() {
        let request = Request::builder()
            .method(Method::DELETE)
            .uri("/bucket/object")
            .body(())
            .expect("request");

        assert!(should_force_zero_content_length_for_empty_body_route(&request));
    }

    #[test]
    fn s3_delete_with_transfer_encoding_is_not_normalized() {
        let request = Request::builder()
            .method(Method::DELETE)
            .uri("/bucket/object?versionId=3HL4kqtJlcpXrof3Gj0OmxJnVBH40Nrjfkd")
            .header(http::header::TRANSFER_ENCODING, "chunked")
            .body(())
            .expect("request");

        assert!(!should_force_zero_content_length_for_empty_body_route(&request));
    }

    #[test]
    fn s3_get_with_transfer_encoding_is_not_normalized() {
        let request = Request::builder()
            .method(Method::GET)
            .uri("/?x-id=ListBuckets")
            .header(http::header::TRANSFER_ENCODING, "chunked")
            .body(())
            .expect("request");

        assert!(!should_force_zero_content_length_for_empty_body_route(&request));
    }

    #[test]
    fn console_get_with_transfer_encoding_is_not_normalized() {
        let request = Request::builder()
            .method(Method::GET)
            .uri(format!("{CONSOLE_PREFIX}/index.html"))
            .header(http::header::TRANSFER_ENCODING, "chunked")
            .body(())
            .expect("request");

        assert!(!should_force_zero_content_length_for_empty_body_route(&request));
    }

    #[test]
    fn console_post_without_content_length_is_not_normalized() {
        let request = Request::builder()
            .method(Method::POST)
            .uri(format!("{CONSOLE_PREFIX}/upload"))
            .body(())
            .expect("request");

        assert!(!should_force_zero_content_length_for_empty_body_route(&request));
    }

    #[test]
    fn non_s3_delete_paths_are_not_normalized() {
        let paths = [
            "/minio/admin/v3/pools/cancel?versionId=unused",
            "/rustfs/admin/v3/pools/cancel?versionId=unused",
            "/rustfs/rpc/read_file_stream?versionId=unused",
            &format!("{CONSOLE_PREFIX}/index.html?versionId=unused"),
            "/health?versionId=unused",
            "/health/ready?versionId=unused",
            "/profile/cpu?versionId=unused",
            "/profile/memory?versionId=unused",
        ];

        for path in paths {
            let request = Request::builder().method(Method::DELETE).uri(path).body(()).expect("request");

            assert!(
                !should_force_zero_content_length_for_empty_body_route(&request),
                "{path} should not force Content-Length: 0"
            );
        }
    }

    #[test]
    fn non_empty_body_admin_post_path_is_not_normalized() {
        let request = Request::builder()
            .method(Method::POST)
            .uri("/minio/admin/v3/update-service-account")
            .body(())
            .expect("request");

        assert!(!should_force_zero_content_length_for_empty_body_route(&request));
    }
}
