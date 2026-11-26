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

//! Exercises the scoped dialect, complete response conversion and optional envelope state.
//! Real IAM and legacy wire comparison belong to server/http.rs; these probes isolate the bridge.

use super::*;
use crate::admin::operation::AdminResponse;
use crate::server::gateway_handlers::handler_error;
use crate::server::gateway_handlers::test_support::{CLIENT, PEER, REQUEST_ID, Signed, request_context, signed_request};
use crate::server::gateway_stack::admin::{assemble, complete_response, dialect, envelope};
use crate::server::stack::claims_native_admin_read;
use crate::shared_types::RemoteAddr;
use crate::storage_api::server::http::access::ReqInfo;
use bytes::Bytes;
use http::{HeaderMap, Method, StatusCode};
use rustfs_gateway::Answer;
use rustfs_gateway::{Decision, Handler, HandlerResult, Operation, PathStyleOnly, Req, Resp, decide_with};
use rustfs_gateway_core::route::Predicate;
use rustfs_gateway_dialect_rustfs_admin::AdminResponse as WireResponse;
use rustfs_gateway_dialect_rustfs_admin::ops::{
    get_iceberg_config::GetIcebergConfig, get_v3_datausageinfo::GetV3Datausageinfo, get_v3_info::GetV3Info,
    get_v3_module_switches::GetV3ModuleSwitches, get_v3_object_data_cache_stats::GetV3ObjectDataCacheStats,
    get_v3_storageinfo::GetV3Storageinfo, get_v4_cluster_snapshot::GetV4ClusterSnapshot,
    get_v4_extensions_catalog::GetV4ExtensionsCatalog, get_v4_plugins_catalog::GetV4PluginsCatalog,
    get_v4_runtime_capabilities::GetV4RuntimeCapabilities,
};
use rustfs_s3_types::Body;
use rustfs_trusted_proxies::ClientInfo;
use std::net::{IpAddr, SocketAddr};
use std::sync::Mutex;

#[test]
fn native_admin_registers_only_selected_reads_and_four_claims() {
    let dialect = dialect().unwrap();
    let mut expected = [
        GetIcebergConfig::NAME,
        GetV3Info::NAME,
        GetV3Datausageinfo::NAME,
        GetV4PluginsCatalog::NAME,
        GetV4ClusterSnapshot::NAME,
        GetV4ExtensionsCatalog::NAME,
        GetV3ModuleSwitches::NAME,
        GetV3Storageinfo::NAME,
        GetV4RuntimeCapabilities::NAME,
        GetV3ObjectDataCacheStats::NAME,
    ];
    expected.sort();
    let mut names = dialect.operation_names().collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, expected);
    assert_eq!(
        dialect.claims().iter().map(|claim| claim.prefix).collect::<Vec<_>>(),
        ["/rustfs/admin", "/minio/admin", "/_iceberg/v1", "/iceberg/v1"]
    );
    for operation in dialect.claimed_operations() {
        assert_eq!(operation.entries().len(), 2, "canonical and alias rows for {}", operation.name());
        for row in operation.entries() {
            let path = row.template().as_str();
            assert_eq!(row.entry().selector.predicates(), &[Predicate::Method(Method::GET)], "{path}");
            let uri = format!("{path}?detail=1").parse().unwrap();
            assert!(claims_native_admin_read(&Method::GET, &uri), "{uri}");
            assert!(!claims_native_admin_read(&Method::HEAD, &uri), "{uri}");
            let neighbor = format!("{path}-neighbor").parse().unwrap();
            assert!(!claims_native_admin_read(&Method::GET, &neighbor), "{neighbor}");
        }
    }
    let stack = GatewayStack::assemble_admin(assembly()).unwrap();
    let mut names = stack.service.operations().collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, expected);
}

#[tokio::test]
async fn native_admin_complete_response_keeps_bytes_status_and_content_type() {
    let mut headers = HeaderMap::new();
    headers.insert(http::header::CONTENT_TYPE, "application/custom+json".parse().unwrap());
    headers.insert(http::header::CONTENT_LENGTH, "12".parse().unwrap());
    headers.insert("x-admin-result", "complete".parse().unwrap());
    let response =
        complete_response::<GetV3Info>(AdminResponse::with_headers(StatusCode::ACCEPTED, Body::from("opaque bytes"), headers))
            .await
            .unwrap();
    let (answer, status, headers) = response.into_parts();
    assert_eq!(status, 202);
    assert_eq!(headers[http::header::CONTENT_TYPE], "application/custom+json");
    assert_eq!(headers["x-admin-result"], "complete");
    assert!(!headers.contains_key(http::header::CONTENT_LENGTH));
    let Answer::Settled(output) = answer else { panic!("a complete response") };
    let rustfs_gateway_dialect_rustfs_admin::AdminBody::Complete(bytes) = output.body() else {
        panic!("complete bytes")
    };
    assert_eq!(bytes, b"opaque bytes");
}

struct Capture(Arc<Mutex<Vec<serde_json::Value>>>);

impl<O: Operation<Output = WireResponse>> Handler<O> for Capture {
    async fn call(&self, request: Req<O>) -> HandlerResult<O> {
        let envelope = envelope(request.context(), &ServerContextSlot::new()).map_err(handler_error)?;
        self.0.lock().unwrap().push(serde_json::json!({
            "uri": envelope.uri().to_string(),
            "host": envelope.headers()[http::header::HOST].to_str().unwrap(),
            "peer": envelope.remote_addr().map(|addr| addr.to_string()),
            "client": envelope.policy_source_addr().map(|addr| addr.ip().to_string()),
            "request_id": envelope.request_context().map(|ctx| ctx.request_id.clone()),
            "bucket": envelope.req_info().and_then(|info| info.bucket.clone()),
            "has_info": envelope.req_info().is_some(),
        }));
        Ok(Resp::new(WireResponse::empty()))
    }
}

/// The native admin host as production configures it, with a [`Capture`] on every registered operation.
fn capturing_stack(captured: &Arc<Mutex<Vec<serde_json::Value>>>) -> GatewayStack {
    let builder = GatewayStack::configured_builder(
        &assembly(),
        Arc::new(ScriptedIam { ready: true }),
        root().source(),
        decide_with(|_| Decision::Allow),
        std::convert::identity,
    )
    .unwrap();
    GatewayStack {
        keystone_auth: None,
        control_plane: false,
        object_lambda_fallback: None,
        service: builder
            .host_resolver(PathStyleOnly)
            .dialect(&dialect().unwrap())
            .register::<GetIcebergConfig, _>(Arc::new(Capture(Arc::clone(captured))))
            .register::<GetV3Datausageinfo, _>(Arc::new(Capture(Arc::clone(captured))))
            .register::<GetV4PluginsCatalog, _>(Arc::new(Capture(Arc::clone(captured))))
            .register::<GetV4ExtensionsCatalog, _>(Arc::new(Capture(Arc::clone(captured))))
            .register::<GetV3ModuleSwitches, _>(Arc::new(Capture(Arc::clone(captured))))
            .register::<GetV3Info, _>(Arc::new(Capture(Arc::clone(captured))))
            .register::<GetV3Storageinfo, _>(Arc::new(Capture(Arc::clone(captured))))
            .register::<GetV4RuntimeCapabilities, _>(Arc::new(Capture(Arc::clone(captured))))
            .register::<GetV3ObjectDataCacheStats, _>(Arc::new(Capture(Arc::clone(captured))))
            .build()
            .unwrap(),
    }
}

#[tokio::test]
async fn native_admin_envelope_preserves_raw_alias_host_facts_and_optional_req_info() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let stack = capturing_stack(&captured);
    let paths = [
        "/iceberg/v1/config",
        "/_iceberg/v1/config",
        "/minio/admin/v3/datausageinfo",
        "/minio/admin/v4/plugins/catalog",
        "/minio/admin/v4/extensions/catalog",
        "/minio/admin/v3/module-switches",
        "/minio/admin/v3/info",
        "/minio/admin/v3/storageinfo",
        "/minio/admin/v4/runtime/capabilities",
        "/minio/admin/v3/object-data-cache/stats",
    ];
    for path in paths {
        for injected in [false, true] {
            let mut request = signed_request(Signed::Header, Method::GET, path, "detail=1", ROOT_ACCESS_KEY, ROOT_SECRET_KEY);
            request.extensions_mut().insert(Some(RemoteAddr(SocketAddr::from(PEER))));
            request
                .extensions_mut()
                .insert(ClientInfo::direct(SocketAddr::from((CLIENT, 42000))));
            request.extensions_mut().insert(request_context(REQUEST_ID));
            if injected {
                request.extensions_mut().insert(ReqInfo {
                    bucket: Some("preserved-bucket".to_owned()),
                    ..ReqInfo::default()
                });
            }
            let response = stack.call(request).await;
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.into_body().collect().await.unwrap().to_bytes(), Bytes::new());
        }
    }
    let observed = captured.lock().unwrap();
    for (index, value) in observed.iter().enumerate() {
        assert_eq!(value["uri"], format!("{}?detail=1", paths[index / 2]));
        assert_eq!(value["host"], "localhost");
        assert_eq!(value["peer"], SocketAddr::from(PEER).to_string());
        assert_eq!(value["client"], IpAddr::from(CLIENT).to_string());
        assert_eq!(value["request_id"], REQUEST_ID);
        assert_eq!(value["has_info"], index % 2 == 1);
        assert_eq!(
            value["bucket"],
            if index % 2 == 1 {
                serde_json::json!("preserved-bucket")
            } else {
                serde_json::Value::Null
            }
        );
    }
    assert_eq!(observed.len(), paths.len() * 2);
}

#[tokio::test]
async fn native_admin_authenticated_request_refuses_an_uninstalled_server_context() {
    let stack = assemble(assembly(), Arc::new(ScriptedIam { ready: false }), root().source()).unwrap();
    for path in [
        "/iceberg/v1/config",
        "/_iceberg/v1/config",
        "/rustfs/admin/v3/datausageinfo",
        "/rustfs/admin/v4/plugins/catalog",
        "/rustfs/admin/v4/extensions/catalog",
        "/rustfs/admin/v3/module-switches",
        "/rustfs/admin/v3/info",
        "/minio/admin/v3/datausageinfo",
        "/minio/admin/v4/plugins/catalog",
        "/minio/admin/v4/extensions/catalog",
        "/minio/admin/v3/module-switches",
        "/minio/admin/v3/info",
        "/rustfs/admin/v3/storageinfo",
        "/minio/admin/v3/storageinfo",
        "/rustfs/admin/v4/runtime/capabilities",
        "/minio/admin/v4/runtime/capabilities",
        "/rustfs/admin/v3/object-data-cache/stats",
        "/minio/admin/v3/object-data-cache/stats",
    ] {
        let response = stack
            .call(signed_request(Signed::Header, Method::GET, path, "", ROOT_ACCESS_KEY, ROOT_SECRET_KEY))
            .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("<Code>ServiceUnavailable</Code>"), "{body}");
        assert!(body.contains("<Message>server context is not ready</Message>"), "{body}");
    }
}

/// GHSA-xm99-m3gq-83g8 on the native admin host: a signed request replayed with an `x-amz-*`
/// header its signature does not name is refused before any handler, and the same request without
/// the header is served (the control, which proves the refusal is the header's).
#[tokio::test]
async fn native_admin_refuses_an_unsigned_amz_header_before_any_handler() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let stack = capturing_stack(&captured);
    let signed = || {
        let mut request =
            signed_request(Signed::Header, Method::GET, "/rustfs/admin/v3/info", "", ROOT_ACCESS_KEY, ROOT_SECRET_KEY);
        request.extensions_mut().insert(Some(RemoteAddr(SocketAddr::from(PEER))));
        request
            .extensions_mut()
            .insert(ClientInfo::direct(SocketAddr::from((CLIENT, 42000))));
        request.extensions_mut().insert(request_context(REQUEST_ID));
        request
    };

    let control = stack.call(signed()).await;
    assert_eq!(control.status(), StatusCode::OK);
    assert_eq!(captured.lock().unwrap().len(), 1, "the control reached the handler");

    for (name, value) in [
        ("x-amz-copy-source", "/private-bucket/source"),
        ("x-amz-meta-user", "mallory"),
    ] {
        let mut request = signed();
        request.headers_mut().insert(name, http::HeaderValue::from_static(value));
        let response = stack.call(request).await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN, "{name}");
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body = String::from_utf8_lossy(&body);
        assert!(body.contains("<Code>AccessDenied</Code>"), "{name}: {body}");
        assert!(body.contains("not signed"), "{name}: {body}");
    }
    assert_eq!(captured.lock().unwrap().len(), 1, "a refused request reached a handler");
}
