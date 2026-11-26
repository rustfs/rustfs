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

//! Compatibility layers retained on the external HTTP stack after the gateway
//! cutover, one file per layer.
//!
//! None of them is installed from here: `server::http::external_service_stack!`
//! installs them in the canonical order documented beside it, and
//! `server::http::tests::stack_census` / `middleware_matrix_census` pin that
//! order.
//!
//! T2.12 (`rustfs/backlog#2771`, 2026-11-11 ruling and 2026-11-24 audit):
//!
//! - Deleted outright (replaced by the gateway edge): `DoubleSlashListBucketsCompatLayer`,
//!   `VirtualHostStyleHintLayer`, `S3ErrorMessageCompatLayer`, `ObjectAttributesEtagFixLayer`,
//!   and `SigV4HeaderGuardLayer`. `server::gateway_stack::tests::legacy_layers` pins what each
//!   of them made the legacy stack answer against the gateway stack alone.
//! - Retained on the outer stack for active non-S3 or shared outer-edge responsibilities until
//!   each remaining non-S3 route has an equivalent post-cutover replacement:
//!
//! | Layer | Active outer-edge responsibility |
//! | --- | --- |
//! | `BodylessStatusFixLayer` | enforces bodyless `1xx`/`204`/`205`/`304` across gateway, website, console, and outer layers |
//! | `HeadRequestBodyFixLayer` | strips `HEAD` response bodies across gateway, website, console, and outer layers |
//! | `EmptyBodyContentLengthCompatLayer` | S3 half retired; keeps admin, console, and internode `Content-Length: 0` handling |
//! | `StsQueryApiCompatLayer` | wraps outer short-circuit STS errors in XML and marks requests for compression |
//! | `IcebergRestErrorCompatLayer` | converts table-catalog XML errors into Iceberg REST JSON envelopes |
//!
//! `ConditionalCorsLayer` is outside this module and stays in `server/layer.rs`.
//!
//! This file also holds the three helpers shared by more than one patch layer
//! (`SerializedS3Error`, `xml_escape`, `is_xml_response`) and, under test, the
//! fixed-response fixture shared by the response-rewriting layers' tests.

mod bodyless_status_fix;
mod empty_body_content_length_compat;
mod head_request_body_fix;
mod iceberg_rest_error_compat;
mod sts_query_api_compat;

pub(crate) use bodyless_status_fix::BodylessStatusFixLayer;
pub(crate) use empty_body_content_length_compat::EmptyBodyContentLengthCompatLayer;
pub(crate) use head_request_body_fix::HeadRequestBodyFixLayer;
pub(crate) use iceberg_rest_error_compat::IcebergRestErrorCompatLayer;
pub(crate) use sts_query_api_compat::{StsQueryApiCompatLayer, StsQueryRequest};

#[cfg(test)]
pub(super) use empty_body_content_length_compat::is_empty_body_console_path;

use http::HeaderMap;
use http_body::Body;
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

pub(crate) const MAX_XML_COMPAT_BODY_BYTES: u64 = 1024 * 1024;

fn is_xml_compat_body<B: Body>(body: &B) -> bool {
    body.size_hint()
        .upper()
        .is_some_and(|length| length <= MAX_XML_COMPAT_BODY_BYTES)
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
