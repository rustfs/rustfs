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
//! `StsQueryApiCompatLayer`: an s3s compatibility patch, installed by
//! `server::http::external_service_stack!` and pinned there by `stack_census`.
//!
//! Patches: s3s has no STS Query API, so the STS envelope
//! (`ResponseMetadata`, `ErrorResponse`, the request id) has to be written
//! around s3s's response, including the short-circuit errors of outer layers.
//! Introduced in rustfs/rustfs#5282.
//! Replaced by: STS calls registered as gateway extension operations behind a
//! `QueryPresent` route predicate (gateway `docs/middleware.md`, "The nine
//! RustFS tower patch layers", row 8); RustFS's own STS endpoint is served by
//! the `rustfs` dialect's form claim (rustfs/gateway#1375).
//! Removed by: T2.12 (rustfs/backlog#2771).

use super::{SerializedS3Error, xml_escape};
use crate::error::ApiError;
use crate::server::layer::is_sts_query_request;
use crate::storage_api::server::layer::request_context::RequestContext;
use crate::storage_api::server::legacy_compat::S3ErrorCode;
use bytes::{Bytes, BytesMut};
use futures::future::Either;
use http::{HeaderValue, Request as HttpRequest, Response, StatusCode};
use http_body::Body;
use http_body_util::{BodyExt, Full};
use quick_xml::events::Event;
use rustfs_utils::http::headers::AMZ_REQUEST_ID;
use std::borrow::Cow;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::{Layer, Service};

const STS_RESPONSE_METADATA_TAG: &str = "ResponseMetadata";

const STS_REQUEST_ID_TAG: &str = "RequestId";

const STS_SUCCESS_RESPONSE_TAGS: [&str; 2] = ["AssumeRoleResponse", "AssumeRoleWithWebIdentityResponse"];

#[derive(Clone)]
pub struct StsQueryApiCompatLayer;

#[derive(Clone, Copy, Debug)]
pub(crate) struct StsQueryRequest;

impl<S> Layer<S> for StsQueryApiCompatLayer {
    type Service = StsQueryApiCompatService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        StsQueryApiCompatService { inner }
    }
}

#[derive(Clone)]
pub struct StsQueryApiCompatService<S> {
    inner: S,
}

type StsBoxError = Box<dyn std::error::Error + Send + Sync>;

type StsBoxBody = tower_http::body::UnsyncBoxBody<Bytes, StsBoxError>;

impl<S, RequestBody> Service<HttpRequest<RequestBody>> for StsQueryApiCompatService<S>
where
    S: Service<HttpRequest<RequestBody>, Response = Response<StsBoxBody>> + Send + 'static,
    S::Future: Send + 'static,
    S::Error: From<StsBoxError> + Send + 'static,
{
    type Response = Response<StsBoxBody>;
    type Error = S::Error;
    type Future = Either<S::Future, Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: HttpRequest<RequestBody>) -> Self::Future {
        if !is_sts_query_request(req.method(), req.uri(), req.headers()) {
            return Either::Left(self.inner.call(req));
        }

        req.extensions_mut().insert(StsQueryRequest);
        let (request_id, request_id_header) = sts_response_request_id(&req);
        let future = self.inner.call(req);

        Either::Right(Box::pin(async move {
            let response = future.await?;
            let (mut parts, body) = response.into_parts();
            parts.headers.insert(AMZ_REQUEST_ID, request_id_header);

            let (body, changed) = convert_sts_query_response(body, parts.status, &request_id).await?;
            if changed {
                parts.headers.remove(http::header::CONTENT_LENGTH);
                parts
                    .headers
                    .insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/xml"));
            }
            let body = tower_http::body::UnsyncBoxBody::new(Full::from(body).map_err(|error| -> StsBoxError { match error {} }));
            let response = Response::from_parts(parts, body);

            Ok(response)
        }))
    }
}

fn sts_response_request_id<B>(req: &HttpRequest<B>) -> (String, HeaderValue) {
    let request_id = req
        .extensions()
        .get::<RequestContext>()
        .map(|context| context.request_id.as_str())
        .filter(|request_id| !request_id.trim().is_empty())
        .unwrap_or("sts-request-id")
        .to_owned();

    match HeaderValue::from_str(&request_id) {
        Ok(header) => (request_id, header),
        Err(_) => ("sts-request-id".to_owned(), HeaderValue::from_static("sts-request-id")),
    }
}

async fn convert_sts_query_response<RestBody>(
    body: RestBody,
    status: StatusCode,
    request_id: &str,
) -> Result<(Bytes, bool), RestBody::Error>
where
    RestBody: Body<Data = Bytes>,
{
    let bytes = BodyExt::collect(body).await?.to_bytes();
    let Ok(xml) = std::str::from_utf8(&bytes) else {
        return Ok((bytes, false));
    };

    let converted = if status.is_success() {
        add_sts_response_metadata(xml, request_id)
    } else {
        Some(Bytes::from(wrap_sts_error_response(xml, status, request_id)))
    };

    match converted {
        Some(xml) => Ok((xml, true)),
        None => Ok((bytes, false)),
    }
}

fn add_sts_response_metadata(xml: &str, request_id: &str) -> Option<Bytes> {
    let mut reader = quick_xml::Reader::from_str(xml);
    let mut depth = 0_usize;
    let mut root_prefix = None;

    loop {
        let event_start = usize::try_from(reader.buffer_position()).ok()?;
        match reader.read_event() {
            Ok(Event::Start(element)) => {
                if depth == 0 {
                    if !is_sts_success_response_tag(element.local_name().as_ref()) {
                        return None;
                    }
                    root_prefix = element.name().prefix().map(|prefix| prefix.as_ref().to_owned());
                } else if depth == 1 && element.local_name().as_ref() == STS_RESPONSE_METADATA_TAG {
                    return None;
                }
                depth += 1;
            }
            Ok(Event::Empty(element)) => {
                if depth == 0 {
                    return None;
                }
                if depth == 1 && element.local_name().as_ref() == STS_RESPONSE_METADATA_TAG {
                    return None;
                }
            }
            Ok(Event::End(element)) => {
                if depth == 0 {
                    return None;
                }
                if depth == 1 {
                    if !is_sts_success_response_tag(element.local_name().as_ref()) {
                        return None;
                    }
                    let prefix = root_prefix.as_deref().map(|prefix| format!("{prefix}:")).unwrap_or_default();
                    let metadata = format!(
                        "<{prefix}{STS_RESPONSE_METADATA_TAG}>\
                         <{prefix}{STS_REQUEST_ID_TAG}>{request_id}</{prefix}{STS_REQUEST_ID_TAG}>\
                         </{prefix}{STS_RESPONSE_METADATA_TAG}>",
                        request_id = xml_escape(request_id),
                    );
                    let capacity = xml.len().checked_add(metadata.len())?;
                    let mut converted = BytesMut::with_capacity(capacity);
                    converted.extend_from_slice(&xml.as_bytes()[..event_start]);
                    converted.extend_from_slice(metadata.as_bytes());
                    converted.extend_from_slice(&xml.as_bytes()[event_start..]);
                    return Some(converted.freeze());
                }
                depth -= 1;
            }
            Ok(Event::Eof) | Err(_) => return None,
            _ => {}
        }
    }
}

fn is_sts_success_response_tag(tag: &str) -> bool {
    STS_SUCCESS_RESPONSE_TAGS.contains(&tag)
}

fn wrap_sts_error_response(xml: &str, status: StatusCode, request_id: &str) -> String {
    let parsed = quick_xml::de::from_str::<SerializedS3Error>(xml).ok();
    let (code, message) = parsed
        .as_ref()
        .map(|error| {
            let message = match error.message.as_deref() {
                Some(message) => Cow::Borrowed(message),
                None if error.code == S3ErrorCode::SignatureDoesNotMatch.as_str() => {
                    Cow::Owned(ApiError::error_code_to_message(&S3ErrorCode::SignatureDoesNotMatch))
                }
                None => Cow::Borrowed(error.code.as_str()),
            };
            (error.code.as_str(), message)
        })
        .unwrap_or_else(|| {
            let (code, message) = sts_error_for_status(status);
            (code, Cow::Borrowed(message))
        });
    let error_type = if status.is_server_error() { "Receiver" } else { "Sender" };

    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <ErrorResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
        <Error><Type>{error_type}</Type><Code>{code}</Code><Message>{message}</Message></Error>\
         <RequestId>{request_id}</RequestId></ErrorResponse>",
        code = xml_escape(code),
        message = xml_escape(&message),
        request_id = xml_escape(request_id),
    )
}

fn sts_error_for_status(status: StatusCode) -> (&'static str, &'static str) {
    match status {
        StatusCode::BAD_REQUEST => ("InvalidRequest", "Invalid Request"),
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => ("AccessDenied", "Access Denied"),
        StatusCode::TOO_MANY_REQUESTS => ("TooManyRequests", "Request rate limit exceeded"),
        StatusCode::SERVICE_UNAVAILABLE => ("ServiceUnavailable", "Service Unavailable"),
        _ => ("InternalError", "Internal Server Error"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::compress::{HttpCompressionConfig, PathAwareHttpCompressionPredicate, PathCategoryInjectionLayer};
    use futures::future::{Ready, ready};
    use http::Request;
    use http::{HeaderMap, Method};
    use std::time::Instant;

    #[derive(Clone)]
    struct FixedStsResponse {
        status: StatusCode,
        body: Bytes,
        upstream_request_id: Option<&'static str>,
    }

    impl<RequestBody: Send + 'static> Service<Request<RequestBody>> for FixedStsResponse {
        type Response = Response<StsBoxBody>;
        type Error = StsBoxError;
        type Future = Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Request<RequestBody>) -> Self::Future {
            let body = tower_http::body::UnsyncBoxBody::new(
                Full::from(self.body.clone()).map_err(|error| -> StsBoxError { match error {} }),
            );
            let mut response = Response::builder()
                .status(self.status)
                .header(http::header::CONTENT_TYPE, "application/xml")
                .header(http::header::CONTENT_LENGTH, self.body.len());
            if let Some(request_id) = self.upstream_request_id {
                response = response.header(AMZ_REQUEST_ID, request_id);
            }
            ready(Ok(response.body(body).expect("fixed STS response")))
        }
    }

    async fn call_sts_compat_layer(
        status: StatusCode,
        body: &'static [u8],
        upstream_request_id: Option<&'static str>,
    ) -> (HeaderMap, String) {
        let mut service = StsQueryApiCompatLayer.layer(FixedStsResponse {
            status,
            body: Bytes::from_static(body),
            upstream_request_id,
        });
        let mut request = Request::builder()
            .method(Method::POST)
            .uri("/")
            .header(http::header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(())
            .expect("STS request");
        request.extensions_mut().insert(RequestContext {
            request_id: "server-request-id".to_string(),
            x_amz_request_id: "untrusted-upstream-id".to_string(),
            trace_id: None,
            span_id: None,
            start_time: Instant::now(),
        });

        let response = service.call(request).await.expect("STS compatibility response");
        let headers = response.headers().clone();
        let body = BodyExt::collect(response.into_body())
            .await
            .expect("collect STS response")
            .to_bytes();
        (headers, String::from_utf8(body.to_vec()).expect("STS response should be UTF-8 XML"))
    }

    #[tokio::test]
    async fn sts_compat_layer_uses_canonical_request_id_for_success_header_and_xml() {
        let (headers, body) = call_sts_compat_layer(
            StatusCode::OK,
            b"<AssumeRoleResponse><AssumeRoleResult/></AssumeRoleResponse>",
            Some("upstream-request-id"),
        )
        .await;

        assert_eq!(
            headers.get(AMZ_REQUEST_ID).and_then(|value| value.to_str().ok()),
            Some("server-request-id")
        );
        assert!(!headers.contains_key(http::header::CONTENT_LENGTH));
        assert!(body.contains("<ResponseMetadata><RequestId>server-request-id</RequestId></ResponseMetadata>"));
        assert!(!body.contains("untrusted-upstream-id"));
    }

    #[tokio::test]
    async fn sts_compat_layer_uses_same_request_id_for_error_header_and_xml() {
        let (headers, body) = call_sts_compat_layer(
            StatusCode::FORBIDDEN,
            b"<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>",
            None,
        )
        .await;

        assert_eq!(
            headers.get(AMZ_REQUEST_ID).and_then(|value| value.to_str().ok()),
            Some("server-request-id")
        );
        assert!(body.contains("<ErrorResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">"));
        assert!(body.contains("<RequestId>server-request-id</RequestId>"));
    }

    #[test]
    fn sts_response_request_id_rejects_empty_context_id() {
        let mut request = Request::new(());
        request.extensions_mut().insert(RequestContext {
            request_id: String::new(),
            x_amz_request_id: String::new(),
            trace_id: None,
            span_id: None,
            start_time: Instant::now(),
        });

        let (request_id, header) = sts_response_request_id(&request);
        assert_eq!(request_id, "sts-request-id");
        assert_eq!(header, HeaderValue::from_static("sts-request-id"));
    }

    #[test]
    fn sts_success_response_gets_response_metadata() {
        let xml = "<AssumeRoleResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
             <AssumeRoleResult><Credentials/></AssumeRoleResult></AssumeRoleResponse>";
        let converted = add_sts_response_metadata(xml, "request-123").expect("AssumeRole response should be converted");
        let converted = std::str::from_utf8(&converted).expect("converted response should be UTF-8 XML");

        assert!(converted.contains("<Credentials/>"));
        assert!(converted.contains("<ResponseMetadata><RequestId>request-123</RequestId></ResponseMetadata>"));
        assert!(converted.ends_with("</AssumeRoleResponse>"));
    }

    #[test]
    fn sts_web_identity_success_response_gets_response_metadata() {
        let xml = "<AssumeRoleWithWebIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
             <AssumeRoleWithWebIdentityResult><Credentials/></AssumeRoleWithWebIdentityResult>\
             </AssumeRoleWithWebIdentityResponse>";
        let converted =
            add_sts_response_metadata(xml, "request-123").expect("AssumeRoleWithWebIdentity response should be converted");
        let converted = std::str::from_utf8(&converted).expect("converted response should be UTF-8 XML");

        assert!(converted.contains("<Credentials/>"));
        assert!(converted.contains("<ResponseMetadata><RequestId>request-123</RequestId></ResponseMetadata>"));
        assert!(converted.ends_with("</AssumeRoleWithWebIdentityResponse>"));
    }

    #[test]
    fn sts_namespaced_success_response_gets_namespaced_metadata() {
        let xml = "<sts:AssumeRoleResponse xmlns:sts=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
                   <sts:AssumeRoleResult/></sts:AssumeRoleResponse>";
        let converted = add_sts_response_metadata(xml, "request-123").expect("namespaced response should be converted");
        let converted = std::str::from_utf8(&converted).expect("converted response should be UTF-8 XML");

        assert!(converted.contains("<sts:ResponseMetadata><sts:RequestId>request-123</sts:RequestId></sts:ResponseMetadata>"));
        assert!(converted.ends_with("</sts:AssumeRoleResponse>"));
    }

    #[test]
    fn sts_success_response_with_existing_metadata_is_unchanged() {
        let xml = "<AssumeRoleResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
                   <AssumeRoleResult/>\
                   <ResponseMetadata source=\"upstream\"><RequestId>existing</RequestId></ResponseMetadata>\
                   </AssumeRoleResponse>";

        assert!(add_sts_response_metadata(xml, "request-123").is_none());
    }

    #[test]
    fn sts_error_response_uses_query_api_envelope() {
        let converted = wrap_sts_error_response(
            "<Error><Code>AccessDenied</Code><Message>Access &amp; Denied</Message></Error>",
            StatusCode::FORBIDDEN,
            "request-456",
        );

        assert!(converted.contains("<ErrorResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">"));
        assert!(converted.contains("<Type>Sender</Type>"));
        assert!(converted.contains("<Code>AccessDenied</Code>"));
        assert!(converted.contains("<Message>Access &amp; Denied</Message>"));
        assert!(converted.contains("<RequestId>request-456</RequestId>"));
    }

    #[test]
    fn sts_signature_error_response_fills_missing_message() {
        let converted =
            wrap_sts_error_response("<Error><Code>SignatureDoesNotMatch</Code></Error>", StatusCode::FORBIDDEN, "request-456");
        let expected = ApiError::error_code_to_message(&S3ErrorCode::SignatureDoesNotMatch);

        assert!(converted.contains(&format!("<Message>{expected}</Message>")));
    }

    #[tokio::test]
    async fn sts_compat_layer_wraps_non_xml_short_circuit_errors() {
        let (headers, body) =
            call_sts_compat_layer(StatusCode::SERVICE_UNAVAILABLE, b"Service not ready: waiting for iam", None).await;

        assert_eq!(
            headers.get(AMZ_REQUEST_ID).and_then(|value| value.to_str().ok()),
            Some("server-request-id")
        );
        assert!(body.contains("<Type>Receiver</Type>"));
        assert!(body.contains("<Code>ServiceUnavailable</Code>"));
        assert!(body.contains("<Message>Service Unavailable</Message>"));
        assert!(body.contains("<RequestId>server-request-id</RequestId>"));
        assert!(!body.contains("waiting for iam"));
    }

    #[tokio::test]
    async fn sts_compat_layer_replaces_upstream_and_duplicate_error_request_ids() {
        let (headers, body) = call_sts_compat_layer(
            StatusCode::FORBIDDEN,
            b"<Error><Code>AccessDenied</Code><Message>Access Denied</Message>\
              <RequestId>body-request-id-1</RequestId><RequestId>body-request-id-2</RequestId></Error>",
            Some("header-request-id"),
        )
        .await;

        assert_eq!(
            headers.get(AMZ_REQUEST_ID).and_then(|value| value.to_str().ok()),
            Some("server-request-id")
        );
        assert_eq!(body.matches("<RequestId>").count(), 1);
        assert!(body.contains("<RequestId>server-request-id</RequestId>"));
        for stale_request_id in ["header-request-id", "body-request-id-1", "body-request-id-2"] {
            assert!(!body.contains(stale_request_id));
        }
    }

    #[tokio::test]
    async fn sts_compat_layer_falls_back_for_empty_and_malformed_error_xml() {
        for (body, status, expected_code, expected_message) in [
            (
                b"".as_slice(),
                StatusCode::SERVICE_UNAVAILABLE,
                "ServiceUnavailable",
                "Service Unavailable",
            ),
            (
                b"<Error><Code>AccessDenied".as_slice(),
                StatusCode::BAD_GATEWAY,
                "InternalError",
                "Internal Server Error",
            ),
        ] {
            let (headers, body) = call_sts_compat_layer(status, body, None).await;

            assert_eq!(
                headers.get(AMZ_REQUEST_ID).and_then(|value| value.to_str().ok()),
                Some("server-request-id")
            );
            assert!(body.contains("<Type>Receiver</Type>"));
            assert!(body.contains(&format!("<Code>{expected_code}</Code>")));
            assert!(body.contains(&format!("<Message>{expected_message}</Message>")));
            assert_eq!(body.matches("<RequestId>").count(), 1);
            assert!(body.contains("<RequestId>server-request-id</RequestId>"));
        }
    }

    #[tokio::test]
    async fn sts_compat_layer_prevents_pre_conversion_compression() {
        let xml = format!(
            "<AssumeRoleResponse><AssumeRoleResult>{}</AssumeRoleResult></AssumeRoleResponse>",
            "x".repeat(2048)
        );
        let mut service = tower::ServiceBuilder::new()
            .layer(StsQueryApiCompatLayer)
            .layer(tower_http::catch_panic::CatchPanicLayer::new())
            .layer(
                tower_http::compression::CompressionLayer::new().compress_when(PathAwareHttpCompressionPredicate::new(
                    HttpCompressionConfig {
                        enabled: true,
                        extensions: Vec::new(),
                        mime_patterns: vec!["application/xml".to_owned()],
                        min_size: 0,
                    },
                )),
            )
            .layer(PathCategoryInjectionLayer)
            .service(FixedStsResponse {
                status: StatusCode::OK,
                body: Bytes::from(xml),
                upstream_request_id: None,
            });
        let mut request = Request::builder()
            .method(Method::POST)
            .uri("/")
            .header(http::header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header(http::header::ACCEPT_ENCODING, "gzip")
            .body(())
            .expect("STS request");
        request.extensions_mut().insert(RequestContext {
            request_id: "server-request-id".to_string(),
            x_amz_request_id: String::new(),
            trace_id: None,
            span_id: None,
            start_time: Instant::now(),
        });

        let response = service.call(request).await.expect("STS compatibility response");
        assert!(
            !response.headers().contains_key(http::header::CONTENT_ENCODING),
            "STS XML must stay uncompressed until the compatibility envelope is complete"
        );
        let body = BodyExt::collect(response.into_body())
            .await
            .expect("collect STS response")
            .to_bytes();
        let body = String::from_utf8(body.to_vec()).expect("STS response should be UTF-8 XML");
        assert!(body.contains("<ResponseMetadata><RequestId>server-request-id</RequestId></ResponseMetadata>"));
    }
}
