// Copyright 2024 Albwebsolutions
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

//! S3 error response enhancement layer.
//!
//! Transforms minimal S3 XML error responses into professional, enterprise-grade
//! error pages with albwebfs branding, RequestId, HostId, and proper structure.

use bytes::Bytes;
use http::{header::CONTENT_TYPE, HeaderValue, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use quick_xml::de::from_str;
use quick_xml::se::to_string;
use serde::{Deserialize, Serialize};
use std::task::{Context, Poll};
use tower::Service;
use uuid::Uuid;

/// Branding constants for albwebfs error responses.
pub const SERVER_HEADER_VALUE: &str = "albwebfs";
pub const HOST_ID_PREFIX: &str = "albwebfs-";

/// S3 Error XML structure (AWS-compatible format).
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename = "Error", rename_all = "PascalCase", default)]
struct S3ErrorXml {
    #[serde(rename = "Code")]
    code: String,
    #[serde(rename = "Message")]
    message: String,
    #[serde(rename = "RequestId", skip_serializing_if = "String::is_empty")]
    request_id: String,
    #[serde(rename = "HostId", skip_serializing_if = "String::is_empty")]
    host_id: String,
    #[serde(rename = "Resource", skip_serializing_if = "String::is_empty")]
    resource: String,
    #[serde(rename = "BucketName", skip_serializing_if = "String::is_empty")]
    bucket_name: String,
    #[serde(rename = "Key", skip_serializing_if = "String::is_empty")]
    key: String,
}

/// Tower layer that enhances S3 error responses with professional branding.
#[derive(Clone, Debug)]
pub struct S3ErrorEnhancementLayer;

impl S3ErrorEnhancementLayer {
    pub fn new() -> Self {
        Self
    }
}

impl Default for S3ErrorEnhancementLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl<S, ReqBody, ResBody> tower::Layer<S> for S3ErrorEnhancementLayer
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send,
    ReqBody: Send + 'static,
    ResBody: http_body::Body<Data = Bytes> + Send + Unpin + 'static,
    ResBody::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
{
    type Service = S3ErrorEnhancement<S>;

    fn layer(&self, inner: S) -> Self::Service {
        S3ErrorEnhancement { inner }
    }
}

/// Service that wraps another service and enhances S3 error responses.
#[derive(Clone, Debug)]
pub struct S3ErrorEnhancement<S> {
    inner: S,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for S3ErrorEnhancement<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send,
    ReqBody: Send + 'static,
    ResBody: http_body::Body<Data = Bytes> + Send + Unpin + 'static,
    ResBody::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
{
    type Response = Response<Full<Bytes>>;
    type Error = S::Error;
    type Future = std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Self::Response, Self::Error>>
                + Send,
        >,
    >;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        let mut inner = self.inner.clone();
        let future = inner.call(req);

        Box::pin(async move {
            let response = future.await?;

            let (parts, body) = response.into_parts();
            let status = parts.status;
            let mut headers = parts.headers;

            // Only enhance 4xx and 5xx error responses
            if !status.is_client_error() && !status.is_server_error() {
                let body = BodyExt::collect(body)
                    .await
                    .map_err(|e| std::io::Error::other(e.into()))?
                    .to_bytes();
                return Ok(Response::from_parts(
                    parts,
                    Full::new(body),
                ));
            }

            // Check content-type for XML
            let is_xml = headers
                .get(CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(|v| v.contains("application/xml") || v.contains("text/xml"))
                .unwrap_or(false);

            let body_bytes = BodyExt::collect(body)
                .await
                .map_err(|e| std::io::Error::other(e.into()))?
                .to_bytes();

            let enhanced_body = if is_xml {
                enhance_s3_error_xml(&body_bytes, &headers, status)
            } else {
                body_bytes
            };

            // Set Server header for all error responses
            headers.insert(
                http::header::SERVER,
                HeaderValue::from_static(SERVER_HEADER_VALUE),
            );

            Ok(Response::from_parts(
                http::response::Parts {
                    status,
                    headers,
                    version: parts.version,
                    extensions: parts.extensions,
                },
                Full::new(enhanced_body),
            ))
        })
    }
}

/// Enhances S3 error XML with RequestId, HostId, and professional structure.
fn enhance_s3_error_xml(
    body: &[u8],
    headers: &http::HeaderMap,
    status: StatusCode,
) -> Bytes {
    let body_str = match std::str::from_utf8(body) {
        Ok(s) => s,
        Err(_) => return Bytes::copy_from_slice(body),
    };

    let mut err: S3ErrorXml = match from_str(body_str) {
        Ok(e) => e,
        Err(_) => {
            // Not valid XML or wrong structure - build minimal error from status
            let (code, message) = status_to_code_and_message(status);
            return build_enhanced_xml(S3ErrorXml {
                code,
                message,
                request_id: generate_request_id(headers),
                host_id: generate_host_id(),
                ..Default::default()
            });
        }
    };

    // Fill in missing fields
    if err.request_id.is_empty() {
        err.request_id = generate_request_id(headers);
    }
    if err.host_id.is_empty() {
        err.host_id = generate_host_id();
    }
    if err.message.is_empty() {
        let (code, message) = status_to_code_and_message(status);
        err.code = code;
        err.message = message;
    }

    build_enhanced_xml(err)
}

fn generate_request_id(headers: &http::HeaderMap) -> String {
    headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| Uuid::new_v4().to_string())
}

fn generate_host_id() -> String {
    format!("{}{}", HOST_ID_PREFIX, Uuid::new_v4().simple())
}

fn status_to_code_and_message(status: StatusCode) -> (String, String) {
    match status.as_u16() {
        400 => ("InvalidRequest".into(), "The request was invalid. Please verify your request and try again.".into()),
        401 => ("Unauthorized".into(), "Authentication required. Please provide valid credentials.".into()),
        403 => ("AccessDenied".into(), "Access denied. You do not have permission to perform this operation.".into()),
        404 => ("NoSuchKey".into(), "The specified resource does not exist.".into()),
        405 => ("MethodNotAllowed".into(), "The specified method is not allowed against this resource.".into()),
        409 => ("Conflict".into(), "The request could not be completed due to a conflict with the current state.".into()),
        416 => ("InvalidRange".into(), "The requested range is not satisfiable.".into()),
        429 => ("SlowDown".into(), "Too many requests. Please reduce your request rate and try again.".into()),
        500 => ("InternalError".into(), "We encountered an internal error. Please try again later.".into()),
        503 => ("ServiceUnavailable".into(), "The service is temporarily unavailable. Please retry your request.".into()),
        _ => (
            "InternalError".into(),
            format!("An error occurred ({}). Please try again.", status),
        ),
    }
}

fn escape_xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn build_enhanced_xml(err: S3ErrorXml) -> Bytes {
    let xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>{}"#,
        to_string(&err).unwrap_or_else(|_| format!(
            r#"<Error><Code>{}</Code><Message>{}</Message><RequestId>{}</RequestId><HostId>{}</HostId></Error>"#,
            escape_xml(&err.code),
            escape_xml(&err.message),
            err.request_id,
            err.host_id
        ))
    );
    Bytes::from(xml)
}
