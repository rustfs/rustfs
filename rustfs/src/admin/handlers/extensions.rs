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

use crate::admin::{
    auth::validate_admin_request,
    handlers::plugins_instances,
    plugin_contract::{
        PluginContractDomain, PluginInstanceDiagnosticCode, PluginInstanceDiagnosticCount, PluginInstanceEntry,
        PluginInstanceSource, PluginOperationalStateContract,
    },
    router::{AdminOperation, Operation, S3Router},
};
use crate::auth::{check_key_valid, get_session_token};
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use http::{HeaderMap, HeaderValue, StatusCode};
use hyper::Method;
use matchit::Params;
use rustfs_extension_schema::{ExtensionKind, ExtensionSchema};
use rustfs_policy::policy::action::{Action, AdminAction};
use rustfs_targets::{builtin_extension_schemas, catalog::example_external_webhook_plugin, target_marketplace_extension_schema};
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Request, S3Response, S3Result, s3_error};
use serde::Serialize;
use std::collections::HashMap;

pub fn register_extension_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v4/extensions/catalog").as_str(),
        AdminOperation(&GetExtensionCatalogHandler {}),
    )?;
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v4/extensions/instances").as_str(),
        AdminOperation(&ListExtensionInstancesHandler {}),
    )?;

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ExtensionCatalogResponse {
    pub extensions: Vec<ExtensionSchema>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct ExtensionInstanceEntry {
    pub id: String,
    pub extension_id: String,
    pub kind: ExtensionKind,
    pub domain: PluginContractDomain,
    pub subsystem: String,
    pub account_id: String,
    pub service: String,
    pub status: String,
    pub source: PluginInstanceSource,
    pub enabled: bool,
    pub config: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operational_state: Option<PluginOperationalStateContract>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostic_codes: Vec<PluginInstanceDiagnosticCode>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ExtensionInstancesResponse {
    pub instances: Vec<ExtensionInstanceEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostic_counts: Vec<PluginInstanceDiagnosticCount>,
    pub truncated: bool,
    pub next_marker: Option<String>,
}

fn build_extension_catalog_response() -> ExtensionCatalogResponse {
    let mut extensions = builtin_extension_schemas();
    let example = example_external_webhook_plugin();
    extensions.push(target_marketplace_extension_schema(&example.manifest));
    extensions.sort_by(|a, b| a.extension_id.cmp(&b.extension_id));

    ExtensionCatalogResponse { extensions }
}

fn map_extension_instance(instance: PluginInstanceEntry) -> ExtensionInstanceEntry {
    ExtensionInstanceEntry {
        id: instance.id,
        extension_id: instance.plugin_id,
        kind: ExtensionKind::TargetPlugin,
        domain: instance.domain,
        subsystem: instance.subsystem,
        account_id: instance.account_id,
        service: instance.service,
        status: instance.status,
        source: instance.source,
        enabled: instance.enabled,
        config: instance.config,
        operational_state: instance.operational_state,
        diagnostic_codes: instance.diagnostic_codes,
    }
}

async fn authorize_extension_catalog_request(req: &S3Request<Body>) -> S3Result<()> {
    let Some(input_cred) = &req.credentials else {
        return Err(s3_error!(InvalidRequest, "authentication required"));
    };

    let (cred, owner) =
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

    validate_admin_request(
        &req.headers,
        &cred,
        owner,
        false,
        vec![Action::AdminAction(AdminAction::ServerInfoAdminAction)],
        req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
    )
    .await
}

async fn authorize_extension_instance_request(req: &S3Request<Body>) -> S3Result<()> {
    let Some(input_cred) = &req.credentials else {
        return Err(s3_error!(InvalidRequest, "authentication required"));
    };

    let (cred, owner) =
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

    validate_admin_request(
        &req.headers,
        &cred,
        owner,
        false,
        vec![Action::AdminAction(AdminAction::GetBucketTargetAction)],
        req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
    )
    .await
}

fn build_json_response(
    status: StatusCode,
    body: &impl Serialize,
    request_id: Option<&HeaderValue>,
) -> S3Result<S3Response<(StatusCode, Body)>> {
    let data = serde_json::to_vec(body).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
    let mut header = HeaderMap::new();
    header.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    if let Some(value) = request_id {
        header.insert("x-request-id", value.clone());
    }
    Ok(S3Response::with_headers((status, Body::from(data)), header))
}

pub struct GetExtensionCatalogHandler {}

#[async_trait::async_trait]
impl Operation for GetExtensionCatalogHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_extension_catalog_request(&req).await?;
        build_json_response(StatusCode::OK, &build_extension_catalog_response(), req.headers.get("x-request-id"))
    }
}

pub struct ListExtensionInstancesHandler {}

#[async_trait::async_trait]
impl Operation for ListExtensionInstancesHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_extension_instance_request(&req).await?;
        let filters = plugins_instances::extract_plugin_instance_filters(&req)?;
        let instances = plugins_instances::filter_plugin_instances(plugins_instances::collect_all_instances().await?, &filters);
        let diagnostic_counts = plugins_instances::collect_diagnostic_counts(&instances);
        let (instances, truncated, next_marker) = plugins_instances::paginate_plugin_instances(instances, &filters)?;
        let response = ExtensionInstancesResponse {
            instances: instances.into_iter().map(map_extension_instance).collect(),
            diagnostic_counts,
            truncated,
            next_marker,
        };
        build_json_response(StatusCode::OK, &response, req.headers.get("x-request-id"))
    }
}

#[cfg(test)]
mod tests {
    use super::{build_extension_catalog_response, map_extension_instance};
    use crate::admin::plugin_contract::{
        PluginContractDomain, PluginEnableState, PluginInstallState, PluginInstanceEntry, PluginInstanceSource,
        PluginOperationalRuntimeState, PluginOperationalStateContract,
    };
    use rustfs_extension_schema::{ExtensionKind, ExtensionRuntimeBoundary, validate_extension_schemas};
    use std::collections::HashMap;

    #[test]
    fn extension_handlers_require_admin_authorization_contract() {
        let src = include_str!("extensions.rs");
        let catalog_block = extract_block_between_markers(
            src,
            "impl Operation for GetExtensionCatalogHandler",
            "pub struct ListExtensionInstancesHandler",
        );
        let instances_block =
            extract_block_between_markers(src, "impl Operation for ListExtensionInstancesHandler", "#[cfg(test)]");

        assert!(
            catalog_block.contains("authorize_extension_catalog_request(&req).await?;"),
            "extension catalog should require admin authorization"
        );
        assert!(
            instances_block.contains("authorize_extension_instance_request(&req).await?;"),
            "extension instances should require admin authorization"
        );

        let catalog_auth = extract_block_between_markers(
            src,
            "async fn authorize_extension_catalog_request",
            "async fn authorize_extension_instance_request",
        );
        assert!(
            catalog_auth.contains("AdminAction::ServerInfoAdminAction"),
            "extension catalog should require server info admin permission"
        );
        let instance_auth =
            extract_block_between_markers(src, "async fn authorize_extension_instance_request", "fn build_json_response");
        assert!(
            instance_auth.contains("AdminAction::GetBucketTargetAction"),
            "extension instances should require target read permission"
        );
    }

    #[test]
    fn extension_catalog_exposes_valid_extension_schemas() {
        let response = build_extension_catalog_response();
        let webhook = response
            .extensions
            .iter()
            .find(|schema| schema.extension_id == "builtin:webhook")
            .expect("builtin webhook extension should be present");

        assert_eq!(webhook.kind, ExtensionKind::TargetPlugin);
        assert_eq!(webhook.runtime.boundary, ExtensionRuntimeBoundary::Builtin);
        assert!(!webhook.disabled_by_default);
        assert!(
            webhook
                .capabilities
                .iter()
                .any(|capability| capability.as_str() == "target.audit.v1")
        );
        assert!(
            webhook
                .capabilities
                .iter()
                .any(|capability| capability.as_str() == "target.notify.v1")
        );

        let s3_hooks = response
            .extensions
            .iter()
            .find(|schema| schema.extension_id == "builtin:s3-post-auth-hooks")
            .expect("builtin s3 hook extension should be present");
        assert_eq!(s3_hooks.kind, ExtensionKind::S3Hook);
        assert_eq!(s3_hooks.runtime.boundary, ExtensionRuntimeBoundary::Builtin);

        let diagnostics = response
            .extensions
            .iter()
            .find(|schema| schema.extension_id == "builtin:ops-diagnostics")
            .expect("builtin ops diagnostics extension should be present");
        assert_eq!(diagnostics.kind, ExtensionKind::OpsDiagnostics);
        assert_eq!(diagnostics.runtime.boundary, ExtensionRuntimeBoundary::Builtin);

        assert!(validate_extension_schemas(&response.extensions).is_ok());
    }

    #[test]
    fn extension_instance_view_maps_plugin_instance_identity() {
        let instance = PluginInstanceEntry {
            id: "builtin:webhook:notify:primary".to_string(),
            plugin_id: "builtin:webhook".to_string(),
            domain: PluginContractDomain::Notify,
            subsystem: "notify_webhook".to_string(),
            account_id: "primary".to_string(),
            service: "webhook".to_string(),
            status: "offline".to_string(),
            source: PluginInstanceSource::Config,
            enabled: true,
            config: HashMap::from([("endpoint".to_string(), "https://example.test/webhook".to_string())]),
            operational_state: Some(PluginOperationalStateContract {
                install_state: PluginInstallState::Installed,
                enable_state: PluginEnableState::Enabled,
                runtime_state: PluginOperationalRuntimeState::Offline,
            }),
            diagnostic_codes: Vec::new(),
        };

        let mapped = map_extension_instance(instance.clone());

        assert_eq!(mapped.id, instance.id);
        assert_eq!(mapped.extension_id, instance.plugin_id);
        assert_eq!(mapped.kind, ExtensionKind::TargetPlugin);
        assert_eq!(mapped.domain, instance.domain);
        assert_eq!(mapped.source, instance.source);
        assert_eq!(mapped.operational_state, instance.operational_state);
    }

    fn extract_block_between_markers<'a>(src: &'a str, start_marker: &str, end_marker: &str) -> &'a str {
        let start = src
            .find(start_marker)
            .unwrap_or_else(|| panic!("Expected marker `{start_marker}` in source"));
        let after_start = &src[start..];
        let end = after_start
            .find(end_marker)
            .unwrap_or_else(|| panic!("Expected end marker `{end_marker}` in source"));
        &after_start[..end]
    }
}
