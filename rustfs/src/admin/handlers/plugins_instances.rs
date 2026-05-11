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
    handlers::target_descriptor::{
        AdminTargetSpec, TargetEndpointSource, TargetInstanceReadModel, admin_target_spec_from_builtin, build_json_response,
        collect_runtime_statuses, collect_target_instances, find_target_instance,
    },
    router::{AdminOperation, Operation, S3Router},
};
use crate::auth::{check_key_valid, get_session_token};
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_audit::audit_system;
use rustfs_config::audit::AUDIT_ROUTE_PREFIX;
use rustfs_config::notify::NOTIFY_ROUTE_PREFIX;
use rustfs_ecstore::config::{Config, KVS};
use rustfs_policy::policy::action::{Action, AdminAction};
use rustfs_targets::TargetDomain;
use rustfs_targets::catalog::builtin::{builtin_audit_target_admin_descriptors, builtin_notify_target_admin_descriptors};
use s3s::{Body, S3Request, S3Response, S3Result, s3_error};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::LazyLock;

pub fn register_plugin_instance_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v4/plugins/instances").as_str(),
        AdminOperation(&ListPluginInstancesHandler {}),
    )?;
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v4/plugins/instances/{id}").as_str(),
        AdminOperation(&GetPluginInstanceHandler {}),
    )?;

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
struct PluginInstanceEntry {
    id: String,
    plugin_id: String,
    domain: TargetDomainName,
    subsystem: String,
    account_id: String,
    service: String,
    status: String,
    source: TargetEndpointSource,
    enabled: bool,
    config: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct PluginInstancesResponse {
    instances: Vec<PluginInstanceEntry>,
}

static NOTIFICATION_TARGET_SPECS: LazyLock<Vec<AdminTargetSpec>> = LazyLock::new(|| {
    builtin_notify_target_admin_descriptors()
        .into_iter()
        .map(|descriptor| admin_target_spec_from_builtin(&descriptor))
        .collect()
});

static AUDIT_TARGET_SPECS: LazyLock<Vec<AdminTargetSpec>> = LazyLock::new(|| {
    builtin_audit_target_admin_descriptors()
        .into_iter()
        .map(|descriptor| admin_target_spec_from_builtin(&descriptor))
        .collect()
});

fn notification_target_specs() -> &'static [AdminTargetSpec] {
    &NOTIFICATION_TARGET_SPECS
}

fn audit_target_specs() -> &'static [AdminTargetSpec] {
    &AUDIT_TARGET_SPECS
}

fn map_instance(instance: TargetInstanceReadModel) -> PluginInstanceEntry {
    PluginInstanceEntry {
        id: instance.canonical_id,
        plugin_id: instance.plugin_id,
        domain: instance.domain.into(),
        subsystem: instance.subsystem,
        account_id: instance.account_id,
        service: instance.service,
        status: instance.status,
        source: instance.source,
        enabled: instance.enabled,
        config: kvs_to_map(instance.config),
    }
}

fn kvs_to_map(config: KVS) -> HashMap<String, String> {
    config.0.into_iter().map(|kv| (kv.key, kv.value)).collect()
}

async fn authorize_plugin_instance_request(req: &S3Request<Body>) -> S3Result<()> {
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

async fn load_server_config_from_store() -> S3Result<Config> {
    let Some(store) = rustfs_ecstore::global::new_object_layer_fn() else {
        return Ok(Config::new());
    };

    rustfs_ecstore::config::com::read_config_without_migrate(store)
        .await
        .map_err(|e| s3_error!(InternalError, "failed to read server config: {}", e))
}

async fn collect_notification_instances() -> S3Result<Vec<PluginInstanceEntry>> {
    let ns =
        rustfs_notify::notification_system().ok_or_else(|| s3_error!(InternalError, "notification system not initialized"))?;
    let runtime_statuses = collect_runtime_statuses(ns.get_target_values().await).await;
    let config = ns.config.read().await.clone();

    Ok(
        collect_target_instances(notification_target_specs(), NOTIFY_ROUTE_PREFIX, &config, runtime_statuses)
            .into_iter()
            .map(map_instance)
            .collect(),
    )
}

async fn collect_audit_instances() -> S3Result<Vec<PluginInstanceEntry>> {
    let mut runtime_statuses = HashMap::new();
    if let Some(system) = audit_system() {
        runtime_statuses = collect_runtime_statuses(system.get_target_values().await).await;
    }
    let config = load_server_config_from_store().await?;

    Ok(
        collect_target_instances(audit_target_specs(), AUDIT_ROUTE_PREFIX, &config, runtime_statuses)
            .into_iter()
            .map(map_instance)
            .collect(),
    )
}

async fn collect_all_instances() -> S3Result<Vec<PluginInstanceEntry>> {
    let mut instances = collect_notification_instances().await?;
    instances.extend(collect_audit_instances().await?);
    instances.sort_by(|a, b| a.service.cmp(&b.service).then_with(|| a.account_id.cmp(&b.account_id)));
    Ok(instances)
}

pub struct ListPluginInstancesHandler {}

#[async_trait::async_trait]
impl Operation for ListPluginInstancesHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_plugin_instance_request(&req).await?;
        let instances = collect_all_instances().await?;
        let data = serde_json::to_vec(&PluginInstancesResponse { instances })
            .map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
        Ok(build_json_response(StatusCode::OK, Body::from(data), req.headers.get("x-request-id")))
    }
}

pub struct GetPluginInstanceHandler {}

#[async_trait::async_trait]
impl Operation for GetPluginInstanceHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_plugin_instance_request(&req).await?;
        let instance_id = params
            .get("id")
            .ok_or_else(|| s3_error!(InvalidArgument, "missing required parameter: 'id'"))?;

        let notification_match = {
            let ns = rustfs_notify::notification_system()
                .ok_or_else(|| s3_error!(InternalError, "notification system not initialized"))?;
            let runtime_statuses = collect_runtime_statuses(ns.get_target_values().await).await;
            let config = ns.config.read().await.clone();
            find_target_instance(notification_target_specs(), NOTIFY_ROUTE_PREFIX, &config, runtime_statuses, instance_id)
        };

        let instance = if let Some(instance) = notification_match {
            instance
        } else {
            let mut runtime_statuses = HashMap::new();
            if let Some(system) = audit_system() {
                runtime_statuses = collect_runtime_statuses(system.get_target_values().await).await;
            }
            let config = load_server_config_from_store().await?;
            find_target_instance(audit_target_specs(), AUDIT_ROUTE_PREFIX, &config, runtime_statuses, instance_id)
                .ok_or_else(|| s3_error!(NoSuchKey, "plugin instance not found"))?
        };

        let data = serde_json::to_vec(&map_instance(instance))
            .map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
        Ok(build_json_response(StatusCode::OK, Body::from(data), req.headers.get("x-request-id")))
    }
}

#[cfg(test)]
mod tests {
    use super::{TargetDomainName, map_instance};
    use crate::admin::handlers::target_descriptor::{
        TargetEndpointSource, TargetInstanceReadModel, canonical_target_instance_id, collect_target_instances,
    };
    use rustfs_config::audit::AUDIT_WEBHOOK_SUB_SYS;
    use rustfs_config::notify::NOTIFY_ROUTE_PREFIX;
    use rustfs_config::notify::NOTIFY_WEBHOOK_SUB_SYS;
    use rustfs_config::{ENABLE_KEY, WEBHOOK_ENDPOINT};
    use rustfs_ecstore::config::{Config, KV, KVS};
    use rustfs_targets::TargetDomain;
    use std::collections::HashMap;

    fn enabled_kvs(value: &str) -> KVS {
        KVS(vec![KV {
            key: ENABLE_KEY.to_string(),
            value: value.to_string(),
            hidden_if_empty: false,
        }])
    }

    #[test]
    fn plugin_instance_handlers_require_admin_authorization_contract() {
        let src = include_str!("plugins_instances.rs");
        let list_block = extract_block_between_markers(
            src,
            "impl Operation for ListPluginInstancesHandler",
            "pub struct GetPluginInstanceHandler",
        );
        let detail_block = extract_block_between_markers(src, "impl Operation for GetPluginInstanceHandler", "#[cfg(test)]");

        assert!(
            list_block.contains("authorize_plugin_instance_request(&req).await?;"),
            "plugin instance list should require admin authorization"
        );
        assert!(
            detail_block.contains("authorize_plugin_instance_request(&req).await?;"),
            "plugin instance detail should require admin authorization"
        );
    }

    #[test]
    fn configured_instance_without_runtime_appears_offline() {
        let config = Config(HashMap::from([(
            NOTIFY_WEBHOOK_SUB_SYS.to_string(),
            HashMap::from([(
                "primary".to_string(),
                KVS(vec![
                    KV {
                        key: ENABLE_KEY.to_string(),
                        value: "on".to_string(),
                        hidden_if_empty: false,
                    },
                    KV {
                        key: WEBHOOK_ENDPOINT.to_string(),
                        value: "https://example.com/webhook".to_string(),
                        hidden_if_empty: false,
                    },
                ]),
            )]),
        )]));

        let instances =
            collect_target_instances(super::notification_target_specs(), NOTIFY_ROUTE_PREFIX, &config, HashMap::new());
        let primary = instances
            .into_iter()
            .find(|instance| instance.account_id == "primary" && instance.service == "webhook")
            .expect("configured instance should be present");

        assert_eq!(primary.status, "offline");
        assert_eq!(primary.source, TargetEndpointSource::Config);
    }

    #[test]
    fn env_only_instance_appears_with_env_source() {
        temp_env::with_vars(
            [
                ("RUSTFS_NOTIFY_WEBHOOK_ENABLE_ENV-ONLY", Some("on")),
                ("RUSTFS_NOTIFY_WEBHOOK_ENDPOINT_ENV-ONLY", Some("https://example.com/env")),
            ],
            || {
                let instances = collect_target_instances(
                    super::notification_target_specs(),
                    NOTIFY_ROUTE_PREFIX,
                    &Config(HashMap::new()),
                    HashMap::new(),
                );
                let env_only = instances
                    .into_iter()
                    .find(|instance| instance.account_id == "env-only")
                    .expect("env-only instance should be present");

                assert_eq!(env_only.source, TargetEndpointSource::Env);
                assert_eq!(env_only.status, "offline");
            },
        );
    }

    #[test]
    fn runtime_only_instance_appears_with_runtime_source() {
        let runtime_statuses = HashMap::from([(("runtime-only".to_string(), "webhook".to_string()), "online".to_string())]);
        let instances = collect_target_instances(
            super::notification_target_specs(),
            NOTIFY_ROUTE_PREFIX,
            &Config(HashMap::new()),
            runtime_statuses,
        );

        let runtime_only = instances
            .into_iter()
            .find(|instance| instance.account_id == "runtime-only")
            .expect("runtime-only instance should be present");

        assert_eq!(runtime_only.source, TargetEndpointSource::Runtime);
        assert_eq!(runtime_only.status, "online");
    }

    #[test]
    fn detail_identity_matches_list_identity() {
        let instance = TargetInstanceReadModel {
            canonical_id: canonical_target_instance_id("builtin:webhook", TargetDomain::Audit, "Primary"),
            plugin_id: "builtin:webhook".to_string(),
            domain: TargetDomain::Audit,
            subsystem: AUDIT_WEBHOOK_SUB_SYS.to_string(),
            account_id: "Primary".to_string(),
            service: "webhook".to_string(),
            status: "offline".to_string(),
            source: TargetEndpointSource::Config,
            enabled: true,
            config: enabled_kvs("on"),
        };

        let mapped = map_instance(instance.clone());
        assert_eq!(mapped.id, instance.canonical_id);
        assert_eq!(mapped.domain, TargetDomainName::Audit);
    }

    #[test]
    fn canonical_id_is_stable_and_lowercases_instance_segment() {
        assert_eq!(
            canonical_target_instance_id("builtin:webhook", TargetDomain::Notify, "PrimaryCase"),
            "builtin:webhook:notify:primarycase"
        );
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
