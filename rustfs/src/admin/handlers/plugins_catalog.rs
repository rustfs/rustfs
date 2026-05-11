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
    router::{AdminOperation, Operation, S3Router},
};
use crate::auth::{check_key_valid, get_session_token};
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use http::{HeaderMap, HeaderValue, StatusCode};
use hyper::Method;
use matchit::Params;
use rustfs_policy::policy::action::{Action, AdminAction};
use rustfs_targets::catalog::builtin::{builtin_audit_target_admin_descriptors, builtin_notify_target_admin_descriptors};
use rustfs_targets::{BuiltinTargetAdminDescriptor, TargetDomain};
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
struct PluginCatalogDomainEntry {
    domain: TargetDomainName,
    subsystem: &'static str,
    valid_fields: &'static [&'static str],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
struct PluginCatalogEntry {
    plugin_id: &'static str,
    target_type: &'static str,
    display_name: &'static str,
    provider: &'static str,
    version: &'static str,
    supported_domains: Vec<TargetDomainName>,
    secret_fields: &'static [&'static str],
    domain_configs: Vec<PluginCatalogDomainEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct PluginCatalogResponse {
    plugins: Vec<PluginCatalogEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
enum TargetDomainName {
    Audit,
    Notify,
}

impl From<TargetDomain> for TargetDomainName {
    fn from(value: TargetDomain) -> Self {
        match value {
            TargetDomain::Audit => Self::Audit,
            TargetDomain::Notify => Self::Notify,
        }
    }
}

fn target_domain_name_from_subsystem(subsystem: &str) -> TargetDomainName {
    if subsystem.starts_with("audit_") {
        TargetDomainName::Audit
    } else {
        TargetDomainName::Notify
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
    plugins.sort_by(|a, b| a.target_type.cmp(b.target_type));
    for plugin in &mut plugins {
        plugin.supported_domains.sort();
        plugin.domain_configs.sort_by_key(|a| a.domain);
    }

    PluginCatalogResponse { plugins }
}

fn merge_catalog_descriptor(plugins: &mut HashMap<&'static str, PluginCatalogEntry>, descriptor: &BuiltinTargetAdminDescriptor) {
    let manifest = descriptor.manifest();
    let domain = target_domain_name_from_subsystem(descriptor.admin_metadata().subsystem());
    let domain_entry = PluginCatalogDomainEntry {
        domain,
        subsystem: descriptor.admin_metadata().subsystem(),
        valid_fields: descriptor.valid_fields(),
    };

    let entry = plugins.entry(manifest.plugin_id).or_insert_with(|| PluginCatalogEntry {
        plugin_id: manifest.plugin_id,
        target_type: manifest.target_type,
        display_name: manifest.display_name,
        provider: manifest.provider,
        version: manifest.version,
        supported_domains: manifest.supported_domains.iter().copied().map(Into::into).collect(),
        secret_fields: manifest.secret_fields,
        domain_configs: Vec::new(),
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
    use super::{TargetDomainName, build_catalog_response};

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
        assert!(webhook.supported_domains.contains(&TargetDomainName::Audit));
        assert!(webhook.supported_domains.contains(&TargetDomainName::Notify));
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

        assert!(webhook.secret_fields.contains(&"auth_token"));
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
