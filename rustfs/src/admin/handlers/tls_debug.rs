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

use super::profile::authorize_profile_request;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::app::context::resolve_outbound_tls_state;
use crate::server::ADMIN_PREFIX;
use http::StatusCode;
use http::{HeaderMap, HeaderValue};
use hyper::Method;
use matchit::Params;
use rustfs_tls_runtime::{OutboundOnlySnapshotArgs, TlsConsumerStatusItem, TlsDebugStatusResponse, TlsRuntimeStatusSnapshot};
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result};

pub fn register_tls_debug_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/debug/tls/status").as_str(),
        AdminOperation(&TlsStatusHandler {}),
    )?;
    Ok(())
}

pub struct TlsStatusHandler {}

const PROTOS_GRPC_CHANNEL_CONSUMER: &str = "protos_grpc_channel";
const RIO_HTTP_READER_CONSUMER: &str = "rio_http_reader";
const ECSTORE_TRANSITION_CLIENT_CONSUMER: &str = "ecstore_transition_client";

#[async_trait::async_trait]
impl Operation for TlsStatusHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_profile_request(&req).await?;

        let tls_path = rustfs_utils::get_env_opt_str(rustfs_config::ENV_RUSTFS_TLS_PATH).unwrap_or_default();
        let outbound = resolve_outbound_tls_state().await;
        let generation = outbound.generation.0;
        let has_root_ca = outbound.root_ca_pem.as_ref().is_some_and(|pem| !pem.is_empty());
        let has_mtls_identity = outbound.mtls_identity.is_some();
        let status = TlsRuntimeStatusSnapshot::from_outbound_only(OutboundOnlySnapshotArgs {
            source_path: tls_path,
            generation,
            reload_enabled: rustfs_utils::get_env_bool(
                rustfs_config::ENV_TLS_RELOAD_ENABLE,
                rustfs_config::DEFAULT_TLS_RELOAD_ENABLE,
            ),
            detect_mode: "poll",
            last_attempt_time: None,
            last_success_time: None,
            last_error: None,
            has_roots: has_root_ca,
            has_mtls_identity,
        });
        let payload = TlsDebugStatusResponse::builder(status)
            .push_consumers([
                TlsConsumerStatusItem {
                    consumer: PROTOS_GRPC_CHANNEL_CONSUMER,
                    generation,
                    has_root_ca,
                    has_mtls_identity,
                },
                TlsConsumerStatusItem {
                    consumer: RIO_HTTP_READER_CONSUMER,
                    generation,
                    has_root_ca,
                    has_mtls_identity,
                },
                TlsConsumerStatusItem {
                    consumer: ECSTORE_TRANSITION_CLIENT_CONSUMER,
                    generation,
                    has_root_ca,
                    has_mtls_identity,
                },
            ])
            .build();
        let body = serde_json::to_vec(&payload)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize tls status failed: {e}")))?;

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        Ok(S3Response::with_headers((StatusCode::OK, Body::from(body)), headers))
    }
}

#[cfg(test)]
mod tests {
    use super::TlsStatusHandler;
    use crate::admin::router::Operation;
    use http::{Extensions, HeaderMap, Uri};
    use hyper::Method;
    use matchit::Params;
    use rustfs_tls_runtime::{OutboundOnlySnapshotArgs, TlsConsumerStatusItem, TlsRuntimeStatusSnapshot};
    use s3s::{Body, S3ErrorCode, S3Request};

    fn build_tls_status_request() -> S3Request<Body> {
        S3Request {
            input: Body::empty(),
            method: Method::GET,
            uri: Uri::from_static("/rustfs/admin/debug/tls/status"),
            headers: HeaderMap::new(),
            extensions: Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    #[tokio::test]
    async fn tls_status_handler_rejects_missing_credentials() {
        let result = TlsStatusHandler {}.call(build_tls_status_request(), Params::new()).await;
        let err = result.expect_err("tls status handler must reject unauthenticated requests");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }

    #[test]
    fn tls_debug_response_schema_is_structured() {
        let status = TlsRuntimeStatusSnapshot::from_outbound_only(OutboundOnlySnapshotArgs {
            source_path: "/tmp/tls".to_string(),
            generation: 7,
            reload_enabled: true,
            detect_mode: "poll",
            last_attempt_time: Some(1),
            last_success_time: Some(2),
            last_error: Some("x".to_string()),
            has_roots: true,
            has_mtls_identity: false,
        });
        let payload = rustfs_tls_runtime::TlsDebugStatusResponse::builder(status)
            .push_consumers([TlsConsumerStatusItem {
                consumer: "protos_grpc_channel",
                generation: 7,
                has_root_ca: true,
                has_mtls_identity: false,
            }])
            .build();

        let json = serde_json::to_value(payload).expect("json should serialize");
        assert!(json.get("foundation").is_some());
        assert!(json.get("consumers").is_some());
        assert!(json["consumers"].is_array());
    }
}
