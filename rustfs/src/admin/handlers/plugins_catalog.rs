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
    plugin_contract::{
        PluginCatalogDomainEntry, PluginCatalogEntry, PluginCatalogResponse, PluginContractDomain, PluginContractEntrypointKind,
        PluginContractPackaging, PluginDistributionContract, PluginRuntimeContract,
    },
    router::{AdminOperation, Operation, S3Router},
};
use crate::auth::{check_key_valid, get_session_token};
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use http::{HeaderMap, HeaderValue, StatusCode};
use hyper::Method;
use matchit::Params;
use rustfs_policy::policy::action::{Action, AdminAction};
use rustfs_targets::catalog::{
    builtin::builtin_audit_target_admin_descriptors, builtin::builtin_notify_target_admin_descriptors,
};
use rustfs_targets::{
    BuiltinTargetAdminDescriptor, builtin_target_marketplace_manifest, builtin_target_plugin_installation,
    catalog::example_external_webhook_plugin,
};
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Request, S3Response, S3Result, s3_error};
use serde::Serialize;
use std::collections::HashMap;

pub fn register_plugin_catalog_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v4/plugins/catalog").as_str(),
        AdminOperation(&GetPluginCatalogHandler {}),
    )?;

    Ok(())
}

fn target_domain_name_from_subsystem(subsystem: &str) -> PluginContractDomain {
    if subsystem.starts_with("audit_") {
        PluginContractDomain::Audit
    } else {
        PluginContractDomain::Notify
    }
}

fn build_catalog_response() -> PluginCatalogResponse {
    let mut plugins: HashMap<&'static str, PluginCatalogEntry> = HashMap::new();

    for descriptor in builtin_notify_target_admin_descriptors()
        .into_iter()
        .chain(builtin_audit_target_admin_descriptors())
    {
        merge_catalog_descriptor(&mut plugins, &descriptor);
    }

    let mut plugins = plugins.into_values().collect::<Vec<_>>();
    plugins.push(example_external_webhook_plugin_entry());
    plugins.sort_by(|a, b| a.target_type.cmp(&b.target_type));
    for plugin in &mut plugins {
        plugin.supported_domains.sort();
        plugin.domain_configs.sort_by_key(|a| a.domain);
    }

    PluginCatalogResponse { plugins }
}

fn example_external_webhook_plugin_entry() -> PluginCatalogEntry {
    let example = example_external_webhook_plugin();
    let manifest = example.manifest;

    PluginCatalogEntry {
        plugin_id: manifest.plugin_id.to_string(),
        target_type: manifest.target_type.to_string(),
        display_name: manifest.display_name.to_string(),
        provider: manifest.provider.to_string(),
        version: manifest.version.to_string(),
        packaging: PluginContractPackaging::from(manifest.packaging),
        entrypoint_kind: PluginContractEntrypointKind::from(manifest.entrypoint_kind),
        api_compatibility_version: manifest.api_compatibility_version.to_string(),
        runtime_contract: PluginRuntimeContract::from(manifest.runtime_contract),
        distribution: manifest.distribution.map(PluginDistributionContract::from),
        supported_domains: manifest.supported_domains.iter().copied().map(Into::into).collect(),
        secret_fields: manifest.secret_fields.iter().map(|field| (*field).to_string()).collect(),
        domain_configs: vec![PluginCatalogDomainEntry {
            domain: PluginContractDomain::Notify,
            subsystem: "notify_webhook".to_string(),
            valid_fields: example.valid_fields,
        }],
        installation: Some(example.installation.into()),
    }
}

fn merge_catalog_descriptor(plugins: &mut HashMap<&'static str, PluginCatalogEntry>, descriptor: &BuiltinTargetAdminDescriptor) {
    let manifest = descriptor.manifest();
    let marketplace = builtin_target_marketplace_manifest(manifest.target_type);
    let domain = target_domain_name_from_subsystem(descriptor.admin_metadata().subsystem());
    let domain_entry = PluginCatalogDomainEntry {
        domain,
        subsystem: descriptor.admin_metadata().subsystem().to_string(),
        valid_fields: descriptor.valid_fields().iter().map(|field| (*field).to_string()).collect(),
    };

    let entry = plugins.entry(manifest.plugin_id).or_insert_with(|| PluginCatalogEntry {
        plugin_id: manifest.plugin_id.to_string(),
        target_type: manifest.target_type.to_string(),
        display_name: manifest.display_name.to_string(),
        provider: manifest.provider.to_string(),
        version: manifest.version.to_string(),
        packaging: PluginContractPackaging::from(marketplace.packaging),
        entrypoint_kind: PluginContractEntrypointKind::from(marketplace.entrypoint_kind),
        api_compatibility_version: marketplace.api_compatibility_version.to_string(),
        runtime_contract: PluginRuntimeContract::from(marketplace.runtime_contract),
        distribution: marketplace.distribution.map(PluginDistributionContract::from),
        supported_domains: manifest.supported_domains.iter().copied().map(Into::into).collect(),
        secret_fields: manifest.secret_fields.iter().map(|field| (*field).to_string()).collect(),
        domain_configs: Vec::new(),
        installation: Some(builtin_target_plugin_installation(manifest).into()),
    });

    if !entry.domain_configs.iter().any(|existing| existing.domain == domain) {
        entry.domain_configs.push(domain_entry);
    }
}

async fn authorize_plugin_catalog_request(req: &S3Request<Body>) -> S3Result<()> {
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

pub struct GetPluginCatalogHandler {}

#[async_trait::async_trait]
impl Operation for GetPluginCatalogHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_plugin_catalog_request(&req).await?;
        build_json_response(StatusCode::OK, &build_catalog_response(), req.headers.get("x-request-id"))
    }
}

#[cfg(test)]
mod tests {
    use super::build_catalog_response;
    use crate::admin::plugin_contract::{
        PluginContractDomain, PluginContractEntrypointKind, PluginContractPackaging, PluginRuntimeTransport,
    };

    #[test]
    fn plugin_catalog_handlers_require_admin_authorization_contract() {
        let src = include_str!("plugins_catalog.rs");
        let handler_block = extract_block_between_markers(src, "impl Operation for GetPluginCatalogHandler", "#[cfg(test)]");

        assert!(
            handler_block.contains("authorize_plugin_catalog_request(&req).await?;"),
            "plugin catalog GET should require admin authorization"
        );
    }

    #[test]
    fn plugin_catalog_contains_representative_builtin_targets() {
        let response = build_catalog_response();

        let webhook = response
            .plugins
            .iter()
            .find(|plugin| plugin.plugin_id == "builtin:webhook")
            .expect("builtin webhook plugin should be present");
        assert_eq!(webhook.target_type, "webhook");
        assert_eq!(webhook.display_name, "Webhook");
        assert_eq!(webhook.packaging, PluginContractPackaging::Builtin);
        assert_eq!(webhook.entrypoint_kind, PluginContractEntrypointKind::Builtin);
        assert_eq!(webhook.api_compatibility_version, "rustfs.target-plugin.v1");
        assert_eq!(webhook.runtime_contract.protocol_version, "rustfs.target-runtime.v1");
        assert_eq!(webhook.runtime_contract.transport, PluginRuntimeTransport::InProcess);
        assert_eq!(webhook.distribution, None);
        assert!(webhook.supported_domains.contains(&PluginContractDomain::Audit));
        assert!(webhook.supported_domains.contains(&PluginContractDomain::Notify));
        assert!(
            webhook
                .domain_configs
                .iter()
                .any(|domain| domain.subsystem == "audit_webhook")
        );
        assert!(
            webhook
                .domain_configs
                .iter()
                .any(|domain| domain.subsystem == "notify_webhook")
        );

        let kafka = response
            .plugins
            .iter()
            .find(|plugin| plugin.plugin_id == "builtin:kafka")
            .expect("builtin kafka plugin should be present");
        assert_eq!(kafka.target_type, "kafka");
        assert!(kafka.domain_configs.iter().any(|domain| domain.subsystem == "audit_kafka"));
        assert!(kafka.domain_configs.iter().any(|domain| domain.subsystem == "notify_kafka"));
    }

    #[test]
    fn plugin_catalog_exposes_secret_fields_only_as_metadata() {
        let response = build_catalog_response();
        let webhook = response
            .plugins
            .iter()
            .find(|plugin| plugin.plugin_id == "builtin:webhook")
            .expect("builtin webhook plugin should be present");

        assert!(webhook.secret_fields.contains(&"auth_token".to_string()));
        assert!(!webhook.secret_fields.iter().any(|field| field.contains("https://")));
        assert!(!webhook.secret_fields.iter().any(|field| field.contains("password=")));
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
