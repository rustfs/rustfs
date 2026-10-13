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
//! `IcebergRestErrorCompatLayer`: an s3s compatibility patch, installed by
//! `server::http::external_service_stack!` and pinned there by `stack_census`.
//!
//! Patches: s3s renders every refusal on the table-catalog prefixes as an S3
//! XML error document, while Iceberg REST clients (Spark) require the JSON
//! error envelope. Introduced in rustfs/rustfs#4788.
//! Replaced by: the `rustfs` dialect's claimed table-catalog surface, whose
//! operations render their own error document (gateway ADR-0031 and
//! `docs/dialects.md`, "Path-prefix claims"), with `StageFilter::on_response`
//! as the escape hatch (gateway `docs/middleware.md`, "Level 2:
//! `StageFilter`"). This layer is not one of the nine rows of that table.
//! Removed by: T2.12 (rustfs/backlog#2771).

use super::{SerializedS3Error, is_xml_response};
use crate::server::hybrid::HybridBody;
use crate::server::is_table_catalog_path;
use bytes::Bytes;
use http::{HeaderValue, Method, Request as HttpRequest, Response, StatusCode};
use http_body::Body;
use http_body_util::BodyExt;
use serde::Serialize;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::{Layer, Service};

#[derive(Clone)]
pub struct IcebergRestErrorCompatLayer;

impl<S> Layer<S> for IcebergRestErrorCompatLayer {
    type Service = IcebergRestErrorCompatService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        IcebergRestErrorCompatService { inner }
    }
}

#[derive(Clone)]
pub struct IcebergRestErrorCompatService<S> {
    inner: S,
}

impl<S, ReqBody, RestBody, GrpcBody> Service<HttpRequest<ReqBody>> for IcebergRestErrorCompatService<S>
where
    S: Service<HttpRequest<ReqBody>, Response = Response<HybridBody<RestBody, GrpcBody>>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: Send + 'static,
    RestBody: Body<Data = Bytes> + From<Bytes> + Send + 'static,
    RestBody::Error: Into<S::Error> + Send + 'static,
    GrpcBody: Send + 'static,
{
    type Response = Response<HybridBody<RestBody, GrpcBody>>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: HttpRequest<ReqBody>) -> Self::Future {
        let catalog_path =
            (req.method() != Method::HEAD && is_table_catalog_path(req.uri().path())).then(|| req.uri().path().to_string());
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let response = inner.call(req).await?;
            if catalog_path.is_none() || response.status().is_success() || !is_xml_response(response.headers()) {
                return Ok(response);
            }

            let (parts, body) = response.into_parts();

            let response = match body {
                HybridBody::Rest { rest_body } => {
                    let (rest_body, converted_status) = convert_iceberg_error_in_xml(
                        rest_body,
                        parts.status,
                        catalog_path.as_deref().expect("catalog path was checked"),
                    )
                    .await
                    .map_err(Into::into)?;
                    let mut parts = parts;
                    if let Some(status) = converted_status {
                        parts.status = status;
                        parts.headers.remove(http::header::CONTENT_LENGTH);
                        parts
                            .headers
                            .insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
                    }
                    Response::from_parts(parts, HybridBody::Rest { rest_body })
                }
                HybridBody::Grpc { grpc_body } => Response::from_parts(parts, HybridBody::Grpc { grpc_body }),
            };

            Ok(response)
        })
    }
}

#[derive(Debug, Serialize)]
struct IcebergRestErrorEnvelope {
    error: IcebergRestError,
}

#[derive(Debug, Serialize)]
struct IcebergRestError {
    message: String,
    #[serde(rename = "type")]
    error_type: String,
    code: u16,
}

async fn convert_iceberg_error_in_xml<RestBody>(
    body: RestBody,
    status: StatusCode,
    path: &str,
) -> Result<(RestBody, Option<StatusCode>), RestBody::Error>
where
    RestBody: Body<Data = Bytes> + From<Bytes>,
{
    let bytes = BodyExt::collect(body).await?.to_bytes();
    let Some(parsed) = std::str::from_utf8(&bytes)
        .ok()
        .and_then(|xml| quick_xml::de::from_str::<SerializedS3Error>(xml).ok())
    else {
        return Ok((RestBody::from(bytes), None));
    };
    let status = iceberg_rest_status(status, &parsed.code);
    let envelope = IcebergRestErrorEnvelope {
        error: IcebergRestError {
            message: parsed.message.unwrap_or_else(|| parsed.code.clone()),
            error_type: iceberg_rest_error_type(&parsed.code, status, path),
            code: status.as_u16(),
        },
    };
    let Ok(json) = serde_json::to_vec(&envelope) else {
        return Ok((RestBody::from(bytes), None));
    };
    Ok((RestBody::from(Bytes::from(json)), Some(status)))
}

fn iceberg_rest_status(status: StatusCode, error_code: &str) -> StatusCode {
    if status == StatusCode::PRECONDITION_FAILED || error_code == "PreconditionFailed" {
        StatusCode::CONFLICT
    } else {
        status
    }
}

fn iceberg_rest_error_type(error_code: &str, status: StatusCode, path: &str) -> String {
    if error_code.ends_with("Exception") {
        return error_code.to_string();
    }
    match error_code {
        "AccessDenied" | "SignatureDoesNotMatch" => "ForbiddenException".to_string(),
        "InvalidAccessKeyId" => "NotAuthorizedException".to_string(),
        "InvalidArgument" | "InvalidRequest" => "BadRequestException".to_string(),
        "PreconditionFailed" => "CommitFailedException".to_string(),
        _ => match status {
            StatusCode::UNAUTHORIZED => "NotAuthorizedException".to_string(),
            StatusCode::FORBIDDEN => "ForbiddenException".to_string(),
            StatusCode::NOT_FOUND => iceberg_not_found_error_type(path).to_string(),
            StatusCode::CONFLICT => "CommitFailedException".to_string(),
            status if status.is_server_error() => "RESTException".to_string(),
            _ => "BadRequestException".to_string(),
        },
    }
}

fn iceberg_not_found_error_type(path: &str) -> &'static str {
    if path.contains("/views/") {
        "NoSuchViewException"
    } else if path.contains("/tables/") {
        "NoSuchTableException"
    } else {
        "NoSuchNamespaceException"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::legacy_compat::test_support::{FixedHybridResponse, collect_hybrid_response};
    use http::Request;
    use http_body_util::Full;

    #[tokio::test]
    async fn iceberg_rest_error_compat_converts_catalog_xml_errors() {
        let mut service = IcebergRestErrorCompatLayer.layer(FixedHybridResponse {
            status: StatusCode::NOT_FOUND,
            body: Bytes::from_static(b"<Error><Code>NoSuchTableException</Code><Message>missing</Message></Error>"),
            content_type: "application/xml",
        });
        let request = Request::builder()
            .method(Method::GET)
            .uri("/iceberg/v1/warehouse/namespaces/ns/tables/events")
            .body(())
            .expect("request");

        let response = service.call(request).await.expect("service response");
        let (status, headers, body) = collect_hybrid_response(response).await;

        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(headers.get(http::header::CONTENT_TYPE).unwrap(), "application/json");
        assert!(headers.get(http::header::CONTENT_LENGTH).is_none());
        assert!(body.contains("\"type\":\"NoSuchTableException\""));
    }

    #[tokio::test]
    async fn iceberg_rest_error_compat_leaves_non_catalog_errors_unchanged() {
        let input = Bytes::from_static(b"<Error><Code>NoSuchKey</Code><Message>missing</Message></Error>");
        let mut service = IcebergRestErrorCompatLayer.layer(FixedHybridResponse {
            status: StatusCode::NOT_FOUND,
            body: input.clone(),
            content_type: "application/xml",
        });
        let request = Request::builder()
            .method(Method::GET)
            .uri("/bucket/object")
            .body(())
            .expect("request");

        let response = service.call(request).await.expect("service response");
        let (status, headers, body) = collect_hybrid_response(response).await;

        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(headers.get(http::header::CONTENT_TYPE).unwrap(), "application/xml");
        assert_eq!(body.as_bytes(), input.as_ref());
    }

    #[tokio::test]
    async fn iceberg_rest_error_conversion_returns_standard_json_envelope() {
        let body = Full::from(Bytes::from_static(
            b"<Error><Code>NoSuchTableException</Code><Message>table not found</Message></Error>",
        ));

        let (body, status) =
            convert_iceberg_error_in_xml(body, StatusCode::NOT_FOUND, "/iceberg/v1/warehouse/namespaces/ns/tables/events")
                .await
                .expect("convert Iceberg error");
        let value: serde_json::Value =
            serde_json::from_slice(&BodyExt::collect(body).await.expect("collect converted body").to_bytes())
                .expect("JSON error envelope");

        assert_eq!(status, Some(StatusCode::NOT_FOUND));
        assert_eq!(value["error"]["code"], 404);
        assert_eq!(value["error"]["type"], "NoSuchTableException");
        assert_eq!(value["error"]["message"], "table not found");
    }

    #[tokio::test]
    async fn iceberg_rest_error_conversion_maps_precondition_to_commit_conflict() {
        let body = Full::from(Bytes::from_static(
            b"<Error><Code>PreconditionFailed</Code><Message>version token changed</Message></Error>",
        ));

        let (body, status) = convert_iceberg_error_in_xml(
            body,
            StatusCode::PRECONDITION_FAILED,
            "/_iceberg/v1/warehouse/namespaces/ns/tables/events",
        )
        .await
        .expect("convert Iceberg conflict");
        let value: serde_json::Value =
            serde_json::from_slice(&BodyExt::collect(body).await.expect("collect converted body").to_bytes())
                .expect("JSON error envelope");

        assert_eq!(status, Some(StatusCode::CONFLICT));
        assert_eq!(value["error"]["code"], 409);
        assert_eq!(value["error"]["type"], "CommitFailedException");
    }

    #[tokio::test]
    async fn iceberg_rest_error_conversion_preserves_forbidden_status() {
        let body = Full::from(Bytes::from_static(
            b"<Error><Code>SignatureDoesNotMatch</Code><Message>signature mismatch</Message></Error>",
        ));

        let (body, status) = convert_iceberg_error_in_xml(body, StatusCode::FORBIDDEN, "/iceberg/v1/config")
            .await
            .expect("convert Iceberg auth error");
        let value: serde_json::Value =
            serde_json::from_slice(&BodyExt::collect(body).await.expect("collect converted body").to_bytes())
                .expect("JSON error envelope");

        assert_eq!(status, Some(StatusCode::FORBIDDEN));
        assert_eq!(value["error"]["code"], 403);
        assert_eq!(value["error"]["type"], "ForbiddenException");
    }
}
