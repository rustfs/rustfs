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

//! The s3s compatibility patch layers of the external HTTP stack, one file per
//! layer, grouped so that T2.12 (rustfs/backlog#2771) can delete them together
//! once the gateway stack replaces the s3s edge.
//!
//! Every module here patches a behaviour of the s3s edge rather than owning a
//! RustFS behaviour of its own. None of them is installed from here:
//! `server::http::external_service_stack!` installs them in the canonical
//! order documented beside it, and `server::http::tests::stack_census` pins
//! that order. Each file header names the s3s behaviour it patches, the
//! gateway mechanism that replaces it, and the task that removes it.
//!
//! Replaced by: the per-layer gateway mechanisms below (gateway
//! `docs/middleware.md`, "The nine RustFS tower patch layers"; the two layers
//! outside that table name their replacement in their own header).
//! Removed by: T2.12 (rustfs/backlog#2771).
//!
//! | Layer | Replaced by |
//! | --- | --- |
//! | `BodylessStatusFixLayer` | the gateway response invariants (row 1) |
//! | `HeadRequestBodyFixLayer` | the same invariants on the refusal path (row 2) |
//! | `DoubleSlashListBucketsCompatLayer` | the route table: `//` and `/` both name `ListBuckets` (row 3) |
//! | `VirtualHostStyleHintLayer` | `HostResolver`'s diagnostic (row 4) |
//! | `EmptyBodyContentLengthCompatLayer` | `StageFilter::on_wire` (row 5) |
//! | `S3ErrorMessageCompatLayer` | the dialect error-render policy, `StageFilter::on_response` as the escape hatch (row 6) |
//! | `ObjectAttributesEtagFixLayer` | the quirk table (`q-mpu-attributes-etag-0036`), `OpLayer<GetObjectAttributes>` to override (row 7) |
//! | `StsQueryApiCompatLayer` | STS extension operations behind a `QueryPresent` route predicate (row 8) |
//! | `IcebergRestErrorCompatLayer` | the `rustfs` dialect's claimed table-catalog surface (gateway ADR-0031) |
//! | `SigV4HeaderGuardLayer` | `rustfs-gateway-sig` signed-header enforcement (`SignedHeaderSet::parse_and_enforce`) |
//!
//! `ConditionalCorsLayer` (row 9) is outside the list rustfs/backlog#2787
//! moves and stays in `server/layer.rs`.
//!
//! This file also holds the three helpers shared by more than one patch layer
//! (`SerializedS3Error`, `xml_escape`, `is_xml_response`) and, under test, the
//! fixed-response fixture shared by the response-rewriting layers' tests.

mod bodyless_status_fix;
mod double_slash_list_buckets_compat;
mod empty_body_content_length_compat;
mod head_request_body_fix;
mod iceberg_rest_error_compat;
mod object_attributes_etag_fix;
mod s3_error_message_compat;
mod sigv4_header_guard;
mod sts_query_api_compat;
mod virtual_host_style_hint;

pub(crate) use bodyless_status_fix::BodylessStatusFixLayer;
pub(crate) use double_slash_list_buckets_compat::DoubleSlashListBucketsCompatLayer;
pub(crate) use empty_body_content_length_compat::EmptyBodyContentLengthCompatLayer;
pub(crate) use head_request_body_fix::HeadRequestBodyFixLayer;
pub(crate) use iceberg_rest_error_compat::IcebergRestErrorCompatLayer;
pub(crate) use object_attributes_etag_fix::ObjectAttributesEtagFixLayer;
pub(crate) use s3_error_message_compat::S3ErrorMessageCompatLayer;
pub(crate) use sigv4_header_guard::SigV4HeaderGuardLayer;
pub(crate) use sts_query_api_compat::{StsQueryApiCompatLayer, StsQueryRequest};
pub(crate) use virtual_host_style_hint::VirtualHostStyleHintLayer;

#[cfg(test)]
pub(super) use empty_body_content_length_compat::is_empty_body_console_path;
#[cfg(test)]
pub(super) use object_attributes_etag_fix::is_object_attributes_request;

use http::HeaderMap;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct SerializedS3Error {
    #[serde(rename = "Code")]
    code: String,
    #[serde(default, rename = "Message")]
    message: Option<String>,
}

fn xml_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&apos;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn is_xml_response(headers: &HeaderMap) -> bool {
    let is_xml = headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|content_type| content_type.to_ascii_lowercase().contains("xml"))
        .unwrap_or(false);
    if !is_xml {
        return false;
    }

    match headers
        .get(http::header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
    {
        Some(encoding) => encoding.trim().is_empty() || encoding.eq_ignore_ascii_case("identity"),
        None => true,
    }
}

#[cfg(test)]
mod test_support {
    use crate::server::hybrid::HybridBody;
    use bytes::Bytes;
    use futures::future::{Ready, ready};
    use http::Request;
    use http::{HeaderMap, Response, StatusCode};
    use http_body_util::Empty;
    use http_body_util::{BodyExt, Full};
    use std::convert::Infallible;
    use std::task::{Context, Poll};
    use tower::Service;

    #[derive(Clone)]
    pub(super) struct FixedHybridResponse {
        pub(super) status: StatusCode,
        pub(super) body: Bytes,
        pub(super) content_type: &'static str,
    }

    impl<B: Send + 'static> Service<Request<B>> for FixedHybridResponse {
        type Response = Response<HybridBody<Full<Bytes>, Empty<Bytes>>>;
        type Error = Infallible;
        type Future = Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Request<B>) -> Self::Future {
            let body = self.body.clone();
            ready(Ok(Response::builder()
                .status(self.status)
                .header(http::header::CONTENT_TYPE, self.content_type)
                .header(http::header::CONTENT_LENGTH, body.len().to_string())
                .body(HybridBody::Rest {
                    rest_body: Full::from(body),
                })
                .expect("fixed hybrid response")))
        }
    }

    pub(super) async fn collect_hybrid_response(
        response: Response<HybridBody<Full<Bytes>, Empty<Bytes>>>,
    ) -> (StatusCode, HeaderMap, String) {
        let status = response.status();
        let headers = response.headers().clone();
        let body = BodyExt::collect(response.into_body())
            .await
            .expect("collect hybrid body")
            .to_bytes();
        (
            status,
            headers,
            String::from_utf8(body.to_vec()).expect("hybrid response body should be UTF-8"),
        )
    }
}
