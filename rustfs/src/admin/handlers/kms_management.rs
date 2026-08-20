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

//! KMS management route registration.

use super::kms_dynamic::current_kms_config_fingerprint;
use super::kms_keys::{CreateKeyHandler, DescribeKeyHandler, GenerateDataKeyHandler, ListKeysHandler};
use crate::admin::auth::authorize_admin_request;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::runtime_sources::{
    current_kms_runtime_service_manager, current_notification_system, current_or_init_kms_runtime_service_manager,
};
use crate::server::ADMIN_PREFIX;
use hyper::{HeaderMap, Method, StatusCode};
use matchit::Params;
use rustfs_kms::KmsBackend;
use rustfs_policy::policy::action::{Action, KmsAction};
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Request, S3Response, S3Result, s3_error};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

async fn kms_encryption_service_from_context() -> Option<std::sync::Arc<rustfs_kms::ObjectEncryptionService>> {
    let manager = kms_service_manager_from_context();
    manager.get_encryption_service().await
}

fn kms_service_manager_from_context() -> std::sync::Arc<rustfs_kms::KmsServiceManager> {
    match current_kms_runtime_service_manager() {
        Some(manager) => manager,
        None => {
            warn!("KMS service manager not initialized, initializing now as fallback");
            current_or_init_kms_runtime_service_manager()
        }
    }
}

fn backend_name(backend: &KmsBackend) -> &'static str {
    match backend {
        KmsBackend::Local => "local",
        KmsBackend::VaultKv2 => "vault-kv2",
        KmsBackend::VaultTransit => "vault-transit",
        KmsBackend::Static => "static",
        KmsBackend::Aws => "aws",
    }
}

fn kms_service_control_actions() -> Vec<Action> {
    vec![Action::KmsAction(KmsAction::ServiceControlAction)]
}

fn kms_configure_actions() -> Vec<Action> {
    vec![Action::KmsAction(KmsAction::ConfigureAction)]
}

fn kms_clear_cache_actions() -> Vec<Action> {
    vec![Action::KmsAction(KmsAction::ClearCacheAction)]
}

/// Admin gate for the KMS management endpoints, none of which act on a key.
///
/// The pre-check keeps these endpoints' historical missing-credentials message;
/// the shared gate reports "get cred failed".
async fn authorize_kms_management_request(req: &S3Request<Body>, actions: Vec<Action>) -> S3Result<()> {
    if req.credentials.is_none() {
        return Err(s3_error!(InvalidRequest, "authentication required"));
    }
    authorize_admin_request(req, actions).await?;
    Ok(())
}

/// Response of `POST /kms/clear-cache`.
///
/// Declared rather than built inline so the shape the console already depends
/// on is pinned by a type and a snapshot instead of by a `json!` literal that
/// any edit can silently reshape. The field names and values are exactly what
/// the inline literal produced.
#[derive(Debug, Serialize, Deserialize)]
pub struct KmsClearCacheResponse {
    pub status: String,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct KmsStatusResponse {
    pub backend_type: String,
    pub backend_status: String,
    pub cache_enabled: bool,
    pub cache_stats: Option<CacheStatsResponse>,
    pub default_key_id: Option<String>,
    /// Capability matrix of the active backend. Additive field: omitted by
    /// older servers, so it must stay optional for consumers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<rustfs_kms::backends::BackendCapabilities>,
    /// Per-node fingerprint of the running KMS configuration. Additive field:
    /// omitted by older servers, so it must stay optional for consumers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_config: Option<KmsClusterConfigStatus>,
}

/// Cluster-wide view of which KMS configuration each node is running.
///
/// KMS configuration is applied per node, so a runtime change that fails to
/// reach a peer leaves that peer serving a different backend. Comparing
/// redacted fingerprints makes that divergence observable and alertable.
#[derive(Debug, Serialize, Deserialize)]
pub struct KmsClusterConfigStatus {
    /// True only when every node answered with the same fingerprint.
    pub consistent: bool,
    pub nodes: Vec<KmsNodeConfigStatus>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct KmsNodeConfigStatus {
    /// Peer address, or `local` for the node serving this request.
    pub host: String,
    /// `None` when the node has no KMS configuration, or could not be asked at
    /// all, in which case `error` carries the reason.
    pub config_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

const LOCAL_NODE_HOST: &str = "local";
const UNKNOWN_PEER_HOST: &str = "<unknown>";

/// Collect the configuration fingerprint of this node and of every peer.
async fn collect_cluster_config_status() -> KmsClusterConfigStatus {
    let mut nodes = vec![KmsNodeConfigStatus {
        host: LOCAL_NODE_HOST.to_string(),
        config_fingerprint: current_kms_config_fingerprint().await,
        error: None,
    }];

    if let Some(notification_sys) = current_notification_system() {
        for peer in notification_sys.kms_config_fingerprints().await {
            nodes.push(KmsNodeConfigStatus {
                host: if peer.host.is_empty() {
                    UNKNOWN_PEER_HOST.to_string()
                } else {
                    peer.host
                },
                config_fingerprint: peer.fingerprint,
                error: peer.err.map(|err| err.to_string()),
            });
        }
    }

    let consistent = cluster_config_is_consistent(&nodes);
    KmsClusterConfigStatus { consistent, nodes }
}

/// Whether every node was observed running the same KMS configuration.
///
/// A node that could not be asked, or that answered without a fingerprint,
/// counts as divergent: the field exists to refuse agreement that was never
/// observed, so an unreachable peer never reads as converged.
fn cluster_config_is_consistent(nodes: &[KmsNodeConfigStatus]) -> bool {
    let Some(first) = nodes.first() else {
        return false;
    };
    if first.config_fingerprint.is_none() {
        return false;
    }
    nodes
        .iter()
        .all(|node| node.error.is_none() && node.config_fingerprint == first.config_fingerprint)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CacheStatsResponse {
    pub hit_count: u64,
    pub miss_count: u64,
    /// Entries currently cached. Additive field: omitted by older servers, so
    /// it must stay defaulted for consumers.
    #[serde(default)]
    pub entry_count: u64,
    /// Entries dropped since process start, whatever the cause. Additive
    /// field, same compatibility rule as `entry_count`.
    #[serde(default)]
    pub eviction_count: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct KmsConfigResponse {
    pub backend: String,
    pub cache_enabled: bool,
    pub cache_max_keys: usize,
    pub cache_ttl_seconds: u64,
    pub default_key_id: Option<String>,
}

pub fn register_kms_management_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/create-key").as_str(),
        AdminOperation(&CreateKeyHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/key/create").as_str(),
        AdminOperation(&CreateKeyHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/describe-key").as_str(),
        AdminOperation(&DescribeKeyHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/key/status").as_str(),
        AdminOperation(&DescribeKeyHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/list-keys").as_str(),
        AdminOperation(&ListKeysHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/generate-data-key").as_str(),
        AdminOperation(&GenerateDataKeyHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/status").as_str(),
        AdminOperation(&KmsStatusHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/status").as_str(),
        AdminOperation(&KmsStatusHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/config").as_str(),
        AdminOperation(&KmsConfigHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/clear-cache").as_str(),
        AdminOperation(&KmsClearCacheHandler {}),
    )?;

    Ok(())
}

/// Get KMS service status
pub struct KmsStatusHandler {}

#[async_trait::async_trait]
impl Operation for KmsStatusHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_kms_management_request(&req, kms_service_control_actions()).await?;

        let Some(service) = kms_encryption_service_from_context().await else {
            return Err(s3_error!(InternalError, "KMS service not initialized"));
        };

        let backend_status = match service.health_check().await {
            Ok(true) => "healthy".to_string(),
            Ok(false) => "unhealthy".to_string(),
            Err(e) => {
                warn!("KMS health check failed: {}", e);
                "error".to_string()
            }
        };

        let cache_stats = service.cache_stats().await.map(|stats| CacheStatsResponse {
            hit_count: stats.hits,
            miss_count: stats.misses,
            entry_count: stats.entries,
            eviction_count: stats.evictions,
        });
        let config = kms_service_manager_from_context().get_redacted_config().await;

        let response = KmsStatusResponse {
            backend_type: config
                .as_ref()
                .map(|cfg| backend_name(&cfg.backend).to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            backend_status,
            cache_enabled: config.as_ref().is_some_and(|cfg| cfg.enable_cache),
            cache_stats,
            default_key_id: service.get_default_key_id().cloned(),
            capabilities: Some(service.backend_capabilities()),
            cluster_config: Some(collect_cluster_config_status().await),
        };

        let data = serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/json".parse().expect("operation should succeed"));

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), headers))
    }
}

/// Get KMS configuration
pub struct KmsConfigHandler {}

#[async_trait::async_trait]
impl Operation for KmsConfigHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_kms_management_request(&req, kms_configure_actions()).await?;

        let Some(service) = kms_encryption_service_from_context().await else {
            return Err(s3_error!(InternalError, "KMS service not initialized"));
        };

        let config = kms_service_manager_from_context()
            .get_redacted_config()
            .await
            .ok_or_else(|| s3_error!(InternalError, "KMS config not available"))?;

        let response = KmsConfigResponse {
            backend: backend_name(&config.backend).to_string(),
            cache_enabled: config.enable_cache,
            cache_max_keys: config.cache_config.max_keys,
            cache_ttl_seconds: config.cache_config.effective_ttl().as_secs(),
            default_key_id: service.get_default_key_id().cloned(),
        };

        let data = serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/json".parse().expect("operation should succeed"));

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), headers))
    }
}

/// Clear KMS cache
pub struct KmsClearCacheHandler {}

#[async_trait::async_trait]
impl Operation for KmsClearCacheHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_kms_management_request(&req, kms_clear_cache_actions()).await?;

        let Some(service) = kms_encryption_service_from_context().await else {
            return Err(s3_error!(InternalError, "KMS service not initialized"));
        };

        match service.clear_cache().await {
            Ok(()) => {
                info!("KMS cache cleared successfully");
                let response = KmsClearCacheResponse {
                    status: "success".to_string(),
                    message: "cache cleared successfully".to_string(),
                };

                let data =
                    serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().expect("operation should succeed"));

                Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), headers))
            }
            Err(e) => {
                error!("Failed to clear KMS cache: {}", e);
                Err(s3_error!(InternalError, "failed to clear cache: {}", e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        KmsClearCacheResponse, authorize_kms_management_request, kms_clear_cache_actions, kms_configure_actions,
        kms_service_control_actions,
    };
    use crate::admin::handlers::kms_keys::stable_json_value;
    use hyper::HeaderMap;
    use rustfs_policy::policy::action::{Action, AdminAction, KmsAction};
    use s3s::{Body, S3Request};

    fn assert_has_action(actions: &[Action], action: Action) {
        assert!(actions.contains(&action), "expected action list to contain {action:?}");
    }

    fn assert_lacks_action(actions: &[Action], action: Action) {
        assert!(!actions.contains(&action), "expected action list not to contain {action:?}");
    }

    /// These endpoints authorize through the shared admin gate, which reports
    /// "get cred failed" for a credential-less request. The pre-check keeps the
    /// message these endpoints have always returned (rustfs/backlog#1829).
    #[tokio::test]
    async fn kms_management_gate_keeps_its_missing_credentials_message() {
        let req = S3Request {
            input: Body::from(String::new()),
            method: http::Method::GET,
            uri: "/rustfs/admin/v3/kms/status".parse().expect("uri should parse"),
            headers: HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };

        let err = authorize_kms_management_request(&req, kms_service_control_actions())
            .await
            .expect_err("a request without credentials must be rejected");
        assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidRequest);
        assert_eq!(err.message(), Some("authentication required"));
    }

    /// Every management endpoint must reach the shared gate, each with its own
    /// action set. The action lists are pinned above, but nothing else checks
    /// which handler asks for which, and a handler that lost its gate entirely
    /// would still serve its response.
    #[test]
    fn management_handlers_authorize_with_their_dedicated_actions() {
        let src = include_str!("kms_management.rs");

        for (handler, actions) in [
            ("KmsStatusHandler", "kms_service_control_actions()"),
            ("KmsConfigHandler", "kms_configure_actions()"),
            ("KmsClearCacheHandler", "kms_clear_cache_actions()"),
        ] {
            let block = src
                .split_once(&format!("impl Operation for {handler}"))
                .unwrap_or_else(|| panic!("{handler} impl should exist"))
                .1;
            let end = block
                .find("\nimpl Operation for")
                .or_else(|| block.find("\n#[cfg(test)]"))
                .unwrap_or(block.len());
            assert!(
                block[..end].contains(&format!("authorize_kms_management_request(&req, {actions})")),
                "{handler} must authorize through the shared gate with {actions}"
            );
        }
    }

    #[test]
    fn kms_management_auth_actions_use_dedicated_kms_actions() {
        assert_has_action(&kms_service_control_actions(), Action::KmsAction(KmsAction::ServiceControlAction));
        assert_has_action(&kms_configure_actions(), Action::KmsAction(KmsAction::ConfigureAction));
        assert_has_action(&kms_clear_cache_actions(), Action::KmsAction(KmsAction::ClearCacheAction));
    }

    /// The clear-cache body is a published client contract, so the shape is
    /// pinned rather than left to whatever the handler happens to build.
    #[test]
    fn kms_clear_cache_response_has_a_stable_json_shape() {
        insta::assert_json_snapshot!(
            "kms_admin_clear_cache_response",
            stable_json_value(KmsClearCacheResponse {
                status: "success".to_string(),
                message: "cache cleared successfully".to_string(),
            })
        );
    }

    /// The snapshot above pins the *type*; this pins that the handler actually
    /// serves it. Without this, reverting the handler body to a `json!` literal
    /// with any field names at all leaves the snapshot green — which is exactly
    /// the silent reshaping the named type was introduced to prevent.
    #[test]
    fn the_clear_cache_handler_serves_the_named_response_type() {
        let src = include_str!("kms_management.rs");
        let marker = "impl Operation for KmsClearCacheHandler";
        let block = src.split_once(marker).expect("clear-cache handler impl should exist").1;
        let block = &block[..block.find("\n#[cfg(test)]").unwrap_or(block.len())];
        assert!(
            block.contains("KmsClearCacheResponse {"),
            "the clear-cache handler must build its response from the named type"
        );
        assert!(
            !block.contains("serde_json::json!"),
            "the clear-cache handler must not rebuild its response as an inline literal"
        );
    }

    #[test]
    fn kms_clear_cache_rejects_server_info_fallback() {
        assert_lacks_action(&kms_clear_cache_actions(), Action::AdminAction(AdminAction::ServerInfoAdminAction));
    }

    /// The `capabilities` field is additive: payloads produced by older
    /// servers (without the field) must keep deserializing, and the field
    /// must be omitted from JSON when unset so existing consumers see an
    /// unchanged response shape.
    #[test]
    fn kms_status_response_capabilities_field_is_additive() {
        let legacy_json = serde_json::json!({
            "backend_type": "local",
            "backend_status": "healthy",
            "cache_enabled": true,
            "cache_stats": null,
            "default_key_id": null,
        });
        let legacy: super::KmsStatusResponse =
            serde_json::from_value(legacy_json).expect("legacy status payload should deserialize");
        assert!(legacy.capabilities.is_none());

        let serialized = serde_json::to_value(&legacy).expect("status response should serialize");
        assert!(serialized.get("capabilities").is_none(), "unset capabilities must be omitted");

        let with_capabilities = super::KmsStatusResponse {
            capabilities: Some(rustfs_kms::backends::BackendCapabilities::minimal()),
            ..legacy
        };
        let serialized = serde_json::to_value(&with_capabilities).expect("status response should serialize");
        let capabilities = serialized.get("capabilities").expect("capabilities must be present when set");
        assert_eq!(capabilities.get("encrypt"), Some(&serde_json::Value::Bool(true)));
        assert_eq!(capabilities.get("rotate"), Some(&serde_json::Value::Bool(false)));
    }

    fn node(host: &str, fingerprint: Option<&str>, error: Option<&str>) -> super::KmsNodeConfigStatus {
        super::KmsNodeConfigStatus {
            host: host.to_string(),
            config_fingerprint: fingerprint.map(str::to_string),
            error: error.map(str::to_string),
        }
    }

    #[test]
    fn cluster_config_is_consistent_only_when_every_node_reports_the_same_fingerprint() {
        assert!(super::cluster_config_is_consistent(&[
            node("local", Some("abc"), None),
            node("peer-1", Some("abc"), None),
        ]));
        assert!(!super::cluster_config_is_consistent(&[
            node("local", Some("abc"), None),
            node("peer-1", Some("def"), None),
        ]));
    }

    #[test]
    fn cluster_config_consistency_never_claims_agreement_it_did_not_observe() {
        // An unreachable peer, a peer whose build does not report a
        // fingerprint, and a node without any configuration all have to read as
        // divergent rather than silently agreeing.
        assert!(!super::cluster_config_is_consistent(&[
            node("local", Some("abc"), None),
            node("peer-1", None, Some("peer is not reachable")),
        ]));
        assert!(!super::cluster_config_is_consistent(&[
            node("local", Some("abc"), None),
            node("peer-1", None, None),
        ]));
        assert!(!super::cluster_config_is_consistent(&[
            node("local", None, None),
            node("peer-1", None, None),
        ]));
        assert!(!super::cluster_config_is_consistent(&[]));
    }

    /// The `cluster_config` field is additive for the same reason
    /// `capabilities` is: older servers omit it and consumers must keep
    /// deserializing their payloads.
    #[test]
    fn kms_status_response_cluster_config_field_is_additive() {
        let legacy_json = serde_json::json!({
            "backend_type": "local",
            "backend_status": "healthy",
            "cache_enabled": true,
            "cache_stats": null,
            "default_key_id": null,
        });
        let legacy: super::KmsStatusResponse =
            serde_json::from_value(legacy_json).expect("legacy status payload should deserialize");
        assert!(legacy.cluster_config.is_none());
        let serialized = serde_json::to_value(&legacy).expect("status response should serialize");
        assert!(serialized.get("cluster_config").is_none(), "unset cluster config must be omitted");

        let with_cluster_config = super::KmsStatusResponse {
            cluster_config: Some(super::KmsClusterConfigStatus {
                consistent: false,
                nodes: vec![node("local", Some("abc"), None), node("peer-1", None, Some("unreachable"))],
            }),
            ..legacy
        };
        let serialized = serde_json::to_value(&with_cluster_config).expect("status response should serialize");
        let cluster_config = serialized
            .get("cluster_config")
            .expect("cluster config must be present when set");
        assert_eq!(cluster_config.get("consistent"), Some(&serde_json::Value::Bool(false)));
        assert_eq!(
            cluster_config
                .get("nodes")
                .and_then(serde_json::Value::as_array)
                .map(Vec::len),
            Some(2)
        );
    }
}
