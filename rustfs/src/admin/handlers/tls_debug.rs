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
use crate::server::ADMIN_PREFIX;
use http::StatusCode;
use http::{HeaderMap, HeaderValue};
use hyper::Method;
use matchit::Params;
use rustfs_ecstore::client::transition_api::transition_tls_status_view;
use rustfs_protos::protos_tls_status_view;
use rustfs_rio::rio_tls_status_view;
use rustfs_tls_runtime::{TlsRuntimeStatusSnapshot, load_global_outbound_tls_state};
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Request, S3Response, S3Result};
use serde::Serialize;

pub fn register_tls_debug_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/debug/tls/status").as_str(),
        AdminOperation(&TlsStatusHandler {}),
    )?;
    Ok(())
}

pub struct TlsStatusHandler {}

#[derive(Debug, Clone, Serialize)]
struct TlsConsumerStatusItem {
    consumer: &'static str,
    generation: u64,
    has_root_ca: bool,
    has_mtls_identity: bool,
}

#[derive(Debug, Clone, Serialize)]
struct TlsDebugStatusResponse {
    foundation: TlsRuntimeStatusSnapshot,
    consumers: Vec<TlsConsumerStatusItem>,
}

#[async_trait::async_trait]
impl Operation for TlsStatusHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_profile_request(&req).await?;

        let tls_path = rustfs_utils::get_env_opt_str(rustfs_config::ENV_RUSTFS_TLS_PATH).unwrap_or_default();
        let outbound = load_global_outbound_tls_state().await;
        let status = TlsRuntimeStatusSnapshot::from_outbound_only(
            tls_path,
            outbound.generation.0,
            rustfs_utils::get_env_bool(rustfs_config::ENV_TLS_RELOAD_ENABLE, rustfs_config::DEFAULT_TLS_RELOAD_ENABLE),
            "poll",
            None,
            None,
            None,
            outbound.root_ca_pem.as_ref().is_some_and(|pem| !pem.is_empty()),
            outbound.mtls_identity.is_some(),
        );
        let protos = protos_tls_status_view().await;
        let rio = rio_tls_status_view().await;
        let ecstore = transition_tls_status_view().await;
        let payload = TlsDebugStatusResponse {
            foundation: status,
            consumers: vec![
                TlsConsumerStatusItem {
                    consumer: "protos_grpc_channel",
                    generation: protos.generation,
                    has_root_ca: protos.has_root_ca,
                    has_mtls_identity: protos.has_mtls_identity,
                },
                TlsConsumerStatusItem {
                    consumer: "rio_http_reader",
                    generation: rio.generation,
                    has_root_ca: rio.has_root_ca,
                    has_mtls_identity: rio.has_mtls_identity,
                },
                TlsConsumerStatusItem {
                    consumer: "ecstore_transition_client",
                    generation: ecstore.generation,
                    has_root_ca: ecstore.has_root_ca,
                    has_mtls_identity: ecstore.has_mtls_identity,
                },
            ],
        };

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        Ok(S3Response::with_headers(
            (StatusCode::OK, Body::from(serde_json::to_string(&payload).unwrap_or_default())),
            headers,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{TlsConsumerStatusItem, TlsDebugStatusResponse, TlsStatusHandler};
    use crate::admin::router::Operation;
    use rustfs_tls_runtime::TlsRuntimeStatusSnapshot;
    use http::{Extensions, HeaderMap, Uri};
    use hyper::Method;
    use matchit::Params;
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
        let status = TlsRuntimeStatusSnapshot::from_outbound_only(
            "/tmp/tls".to_string(),
            7,
            true,
            "poll",
            Some(1),
            Some(2),
            Some("x".to_string()),
            true,
            false,
        );
        let payload = TlsDebugStatusResponse {
            foundation: status,
            consumers: vec![TlsConsumerStatusItem {
                consumer: "protos_grpc_channel",
                generation: 7,
                has_root_ca: true,
                has_mtls_identity: false,
            }],
        };

        let json = serde_json::to_value(payload).expect("json should serialize");
        assert!(json.get("foundation").is_some());
        assert!(json.get("consumers").is_some());
        assert!(json["consumers"].is_array());
    }
}
