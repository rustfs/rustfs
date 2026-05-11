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
        AdminTargetSpec, TargetEndpointSource, TargetInstanceReadModel, admin_target_spec_from_builtin, build_enabled_target_kvs,
        build_json_response, collect_runtime_statuses, collect_target_instances, find_target_instance,
        target_module_disabled_reason, target_mutation_block_reason as shared_target_mutation_block_reason,
    },
    plugin_contract::{PluginContractDomain, PluginInstanceEntry, PluginInstanceSource, PluginInstancesResponse},
    router::{AdminOperation, Operation, S3Router},
};
use crate::auth::{check_key_valid, get_session_token};
use crate::server::{
    ADMIN_PREFIX, RemoteAddr, is_audit_module_enabled, is_notify_module_enabled, refresh_audit_module_enabled,
    refresh_notify_module_enabled, refresh_persisted_module_switches_from_store,
};
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_audit::{audit_system, start_audit_system as start_global_audit_system, system::AuditSystemState};
use rustfs_config::audit::AUDIT_ROUTE_PREFIX;
use rustfs_config::notify::NOTIFY_ROUTE_PREFIX;
use rustfs_config::{AUDIT_DEFAULT_DIR, EVENT_DEFAULT_DIR, MAX_ADMIN_REQUEST_BODY_SIZE};
use rustfs_ecstore::config::{Config, KVS};
use rustfs_policy::policy::action::{Action, AdminAction};
use rustfs_targets::catalog::builtin::{builtin_audit_target_admin_descriptors, builtin_notify_target_admin_descriptors};
use s3s::{Body, S3Request, S3Response, S3Result, s3_error};
use std::collections::HashMap;
use std::sync::LazyLock;
use tracing::warn;
use url::form_urlencoded;

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
    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v4/plugins/instances/{id}").as_str(),
        AdminOperation(&PutPluginInstanceHandler {}),
    )?;
    r.insert(
        Method::DELETE,
        format!("{}{}", ADMIN_PREFIX, "/v4/plugins/instances/{id}").as_str(),
        AdminOperation(&DeletePluginInstanceHandler {}),
    )?;

    Ok(())
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

#[derive(Debug, Clone, Copy)]
struct PluginInstanceDomainContext {
    domain: PluginContractDomain,
    specs: &'static [AdminTargetSpec],
    route_prefix: &'static str,
    default_queue_dir: &'static str,
}

fn plugin_instance_domain_context(domain: PluginContractDomain) -> PluginInstanceDomainContext {
    match domain {
        PluginContractDomain::Notify => PluginInstanceDomainContext {
            domain,
            specs: notification_target_specs(),
            route_prefix: NOTIFY_ROUTE_PREFIX,
            default_queue_dir: EVENT_DEFAULT_DIR,
        },
        PluginContractDomain::Audit => PluginInstanceDomainContext {
            domain,
            specs: audit_target_specs(),
            route_prefix: AUDIT_ROUTE_PREFIX,
            default_queue_dir: AUDIT_DEFAULT_DIR,
        },
    }
}

fn map_instance(instance: TargetInstanceReadModel) -> PluginInstanceEntry {
    PluginInstanceEntry {
        id: instance.canonical_id,
        plugin_id: instance.plugin_id,
        domain: PluginContractDomain::from(instance.domain),
        subsystem: instance.subsystem,
        account_id: instance.account_id,
        service: instance.service,
        status: instance.status,
        source: map_instance_source(instance.source),
        enabled: instance.enabled,
        config: kvs_to_map(instance.config),
    }
}

fn map_instance_source(source: TargetEndpointSource) -> PluginInstanceSource {
    match source {
        TargetEndpointSource::Config => PluginInstanceSource::Config,
        TargetEndpointSource::Env => PluginInstanceSource::Env,
        TargetEndpointSource::Mixed => PluginInstanceSource::Mixed,
        TargetEndpointSource::Runtime => PluginInstanceSource::Runtime,
    }
}

fn kvs_to_map(config: KVS) -> HashMap<String, String> {
    config.0.into_iter().map(|kv| (kv.key, kv.value)).collect()
}

#[derive(Debug, Clone)]
struct ResolvedPluginInstanceTarget {
    context: PluginInstanceDomainContext,
    target_name: String,
    target_spec: AdminTargetSpec,
}

#[derive(Debug, serde::Deserialize)]
struct KeyValue {
    key: String,
    value: String,
}

#[derive(Debug, serde::Deserialize)]
struct PluginInstanceBody {
    key_values: Vec<KeyValue>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct PluginInstanceFilters {
    domain: Option<PluginContractDomain>,
    service: Option<String>,
    status: Option<String>,
    source: Option<PluginInstanceSource>,
    enabled: Option<bool>,
    query: Option<String>,
    limit: Option<usize>,
    marker: Option<String>,
}

fn extract_plugin_instance_filters(req: &S3Request<Body>) -> S3Result<PluginInstanceFilters> {
    let mut filters = PluginInstanceFilters::default();

    if let Some(query) = req.uri.query() {
        for (key, value) in form_urlencoded::parse(query.as_bytes()) {
            let value = value.trim();
            if value.is_empty() {
                continue;
            }

            match key.as_ref() {
                "domain" => filters.domain = Some(parse_plugin_contract_domain(value)?),
                "service" => filters.service = Some(value.to_ascii_lowercase()),
                "status" => filters.status = Some(parse_instance_status(value)?),
                "source" => filters.source = Some(parse_plugin_instance_source(value)?),
                "enabled" => filters.enabled = Some(parse_bool_filter(value)?),
                "q" => filters.query = Some(value.to_ascii_lowercase()),
                "limit" => filters.limit = Some(parse_limit_filter(value)?),
                "marker" => filters.marker = Some(value.to_string()),
                _ => {}
            }
        }
    }

    Ok(filters)
}

fn parse_plugin_contract_domain(value: &str) -> S3Result<PluginContractDomain> {
    match value.to_ascii_lowercase().as_str() {
        "audit" => Ok(PluginContractDomain::Audit),
        "notify" => Ok(PluginContractDomain::Notify),
        _ => Err(s3_error!(InvalidArgument, "invalid plugin instance domain filter: '{}'", value)),
    }
}

fn parse_instance_status(value: &str) -> S3Result<String> {
    match value.to_ascii_lowercase().as_str() {
        "online" | "offline" => Ok(value.to_ascii_lowercase()),
        _ => Err(s3_error!(InvalidArgument, "invalid plugin instance status filter: '{}'", value)),
    }
}

fn parse_plugin_instance_source(value: &str) -> S3Result<PluginInstanceSource> {
    match value.to_ascii_lowercase().as_str() {
        "config" => Ok(PluginInstanceSource::Config),
        "env" => Ok(PluginInstanceSource::Env),
        "mixed" => Ok(PluginInstanceSource::Mixed),
        "runtime" => Ok(PluginInstanceSource::Runtime),
        _ => Err(s3_error!(InvalidArgument, "invalid plugin instance source filter: '{}'", value)),
    }
}

fn parse_bool_filter(value: &str) -> S3Result<bool> {
    value
        .parse::<bool>()
        .map_err(|_| s3_error!(InvalidArgument, "invalid plugin instance enabled filter: '{}'", value))
}

fn parse_limit_filter(value: &str) -> S3Result<usize> {
    let limit = value
        .parse::<usize>()
        .map_err(|_| s3_error!(InvalidArgument, "invalid plugin instance limit filter: '{}'", value))?;
    if limit == 0 {
        return Err(s3_error!(InvalidArgument, "invalid plugin instance limit filter: '{}'", value));
    }
    Ok(limit)
}

fn parse_plugin_instance_id(instance_id: &str) -> S3Result<(String, PluginContractDomain, String)> {
    let mut parts = instance_id.rsplitn(3, ':');
    let Some(target_name) = parts.next() else {
        return Err(s3_error!(InvalidArgument, "invalid plugin instance id: '{}'", instance_id));
    };
    let Some(domain) = parts.next() else {
        return Err(s3_error!(InvalidArgument, "invalid plugin instance id: '{}'", instance_id));
    };
    let Some(plugin_id) = parts.next() else {
        return Err(s3_error!(InvalidArgument, "invalid plugin instance id: '{}'", instance_id));
    };

    if target_name.is_empty() || plugin_id.is_empty() {
        return Err(s3_error!(InvalidArgument, "invalid plugin instance id: '{}'", instance_id));
    }

    Ok((
        plugin_id.to_string(),
        parse_plugin_contract_domain(domain)?,
        target_name.to_ascii_lowercase(),
    ))
}

fn resolve_plugin_instance_target(instance_id: &str) -> S3Result<ResolvedPluginInstanceTarget> {
    let (plugin_id, domain, target_name) = parse_plugin_instance_id(instance_id)?;
    let context = plugin_instance_domain_context(domain);

    let target_spec = context
        .specs
        .iter()
        .find(|spec| rustfs_targets::builtin_target_marketplace_manifest(spec.service).plugin_id == plugin_id)
        .cloned()
        .ok_or_else(|| s3_error!(InvalidArgument, "unsupported plugin instance id: '{}'", instance_id))?;

    Ok(ResolvedPluginInstanceTarget {
        context,
        target_name,
        target_spec,
    })
}

fn filter_plugin_instances(mut instances: Vec<PluginInstanceEntry>, filters: &PluginInstanceFilters) -> Vec<PluginInstanceEntry> {
    instances.retain(|instance| plugin_instance_matches_filters(instance, filters));
    instances
}

fn paginate_plugin_instances(
    instances: Vec<PluginInstanceEntry>,
    filters: &PluginInstanceFilters,
) -> S3Result<(Vec<PluginInstanceEntry>, bool, Option<String>)> {
    let start_index = if let Some(marker) = filters.marker.as_deref() {
        instances
            .iter()
            .position(|instance| instance.id == marker)
            .map(|index| index + 1)
            .ok_or_else(|| s3_error!(InvalidArgument, "invalid plugin instance marker: '{}'", marker))?
    } else {
        0
    };

    if start_index >= instances.len() {
        return Ok((Vec::new(), false, None));
    }

    let remaining = &instances[start_index..];
    let limit = filters.limit.unwrap_or(remaining.len());
    let page_len = remaining.len().min(limit);
    let page = remaining[..page_len].to_vec();
    let truncated = start_index + page_len < instances.len();
    let next_marker = truncated.then(|| page.last().expect("paginated page should not be empty").id.clone());

    Ok((page, truncated, next_marker))
}

fn plugin_instance_matches_filters(instance: &PluginInstanceEntry, filters: &PluginInstanceFilters) -> bool {
    if let Some(domain) = filters.domain
        && instance.domain != domain
    {
        return false;
    }

    if let Some(service) = filters.service.as_deref()
        && !instance.service.eq_ignore_ascii_case(service)
    {
        return false;
    }

    if let Some(status) = filters.status.as_deref()
        && !instance.status.eq_ignore_ascii_case(status)
    {
        return false;
    }

    if let Some(source) = filters.source
        && instance.source != source
    {
        return false;
    }

    if let Some(enabled) = filters.enabled
        && instance.enabled != enabled
    {
        return false;
    }

    if let Some(query) = filters.query.as_deref()
        && !plugin_instance_matches_query(instance, query)
    {
        return false;
    }

    true
}

fn plugin_instance_matches_query(instance: &PluginInstanceEntry, query: &str) -> bool {
    let query = query.to_ascii_lowercase();
    [
        instance.id.as_str(),
        instance.plugin_id.as_str(),
        instance.subsystem.as_str(),
        instance.account_id.as_str(),
        instance.service.as_str(),
    ]
    .into_iter()
    .any(|field| field.to_ascii_lowercase().contains(&query))
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

async fn authorize_plugin_instance_write_request(req: &S3Request<Body>) -> S3Result<()> {
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
        vec![Action::AdminAction(AdminAction::SetBucketTargetAction)],
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

fn has_any_audit_targets(config: &Config) -> bool {
    for spec in audit_target_specs() {
        let Some(targets) = config.0.get(spec.subsystem) else {
            continue;
        };
        if targets.keys().any(|key| key != rustfs_config::DEFAULT_DELIMITER) {
            return true;
        }
    }
    false
}

async fn apply_audit_runtime_config(config: Config) -> S3Result<()> {
    let has_targets = has_any_audit_targets(&config);

    if let Some(system) = audit_system() {
        match system.get_state().await {
            AuditSystemState::Running | AuditSystemState::Paused | AuditSystemState::Starting => {
                if has_targets {
                    system
                        .reload_config(config)
                        .await
                        .map_err(|e| s3_error!(InternalError, "failed to reload audit config: {}", e))?;
                } else {
                    system
                        .close()
                        .await
                        .map_err(|e| s3_error!(InternalError, "failed to stop audit system: {}", e))?;
                }
            }
            AuditSystemState::Stopped | AuditSystemState::Stopping => {
                if has_targets {
                    system
                        .start(config)
                        .await
                        .map_err(|e| s3_error!(InternalError, "failed to start audit system: {}", e))?;
                }
            }
        }
    } else if has_targets {
        start_global_audit_system(config)
            .await
            .map_err(|e| s3_error!(InternalError, "failed to start audit system: {}", e))?;
    }

    Ok(())
}

async fn update_audit_config_and_reload<F>(mut modifier: F) -> S3Result<()>
where
    F: FnMut(&mut Config) -> bool,
{
    let Some(store) = rustfs_ecstore::global::new_object_layer_fn() else {
        return Err(s3_error!(InternalError, "server storage not initialized"));
    };

    let mut config = rustfs_ecstore::config::com::read_config_without_migrate(store.clone())
        .await
        .map_err(|e| s3_error!(InternalError, "failed to read server config: {}", e))?;

    if !modifier(&mut config) {
        return Ok(());
    }

    rustfs_ecstore::config::com::save_server_config(store, &config)
        .await
        .map_err(|e| s3_error!(InternalError, "failed to save audit config: {}", e))?;

    apply_audit_runtime_config(config).await
}

fn plugin_instance_mutation_block_reason(
    context: PluginInstanceDomainContext,
    config: &Config,
    target_type: &str,
    target_name: &str,
    target_label: &str,
) -> Option<String> {
    shared_target_mutation_block_reason(context.specs, context.route_prefix, config, target_type, target_name, target_label)
}

async fn plugin_instance_operation_block_reason(context: PluginInstanceDomainContext, action: &str) -> Option<String> {
    if let Err(err) = refresh_persisted_module_switches_from_store().await {
        warn!(
            error = %err,
            "failed to reload persisted module switches before checking plugin instance operation gating"
        );
    }

    match context.domain {
        PluginContractDomain::Notify => {
            refresh_notify_module_enabled();
            target_module_disabled_reason("notify", rustfs_config::ENV_NOTIFY_ENABLE, is_notify_module_enabled(), action)
        }
        PluginContractDomain::Audit => {
            refresh_audit_module_enabled();
            target_module_disabled_reason("audit", rustfs_config::ENV_AUDIT_ENABLE, is_audit_module_enabled(), action)
        }
    }
}

async fn collect_notification_instances() -> S3Result<Vec<PluginInstanceEntry>> {
    let ns =
        rustfs_notify::notification_system().ok_or_else(|| s3_error!(InternalError, "notification system not initialized"))?;
    let runtime_statuses = collect_runtime_statuses(ns.get_target_values().await).await;
    let config = ns.config.read().await.clone();
    let context = plugin_instance_domain_context(PluginContractDomain::Notify);

    Ok(collect_target_instances(context.specs, context.route_prefix, &config, runtime_statuses)
        .into_iter()
        .map(map_instance)
        .collect())
}

async fn collect_audit_instances() -> S3Result<Vec<PluginInstanceEntry>> {
    let mut runtime_statuses = HashMap::new();
    if let Some(system) = audit_system() {
        runtime_statuses = collect_runtime_statuses(system.get_target_values().await).await;
    }
    let config = load_server_config_from_store().await?;
    let context = plugin_instance_domain_context(PluginContractDomain::Audit);

    Ok(collect_target_instances(context.specs, context.route_prefix, &config, runtime_statuses)
        .into_iter()
        .map(map_instance)
        .collect())
}

async fn collect_all_instances() -> S3Result<Vec<PluginInstanceEntry>> {
    let mut instances = collect_notification_instances().await?;
    instances.extend(collect_audit_instances().await?);
    instances.sort_by(|a, b| a.service.cmp(&b.service).then_with(|| a.account_id.cmp(&b.account_id)));
    Ok(instances)
}

async fn find_notification_instance(instance_id: &str) -> S3Result<Option<TargetInstanceReadModel>> {
    let ns =
        rustfs_notify::notification_system().ok_or_else(|| s3_error!(InternalError, "notification system not initialized"))?;
    let runtime_statuses = collect_runtime_statuses(ns.get_target_values().await).await;
    let config = ns.config.read().await.clone();
    let context = plugin_instance_domain_context(PluginContractDomain::Notify);
    Ok(find_target_instance(
        context.specs,
        context.route_prefix,
        &config,
        runtime_statuses,
        instance_id,
    ))
}

async fn find_audit_instance(instance_id: &str) -> S3Result<Option<TargetInstanceReadModel>> {
    let mut runtime_statuses = HashMap::new();
    if let Some(system) = audit_system() {
        runtime_statuses = collect_runtime_statuses(system.get_target_values().await).await;
    }
    let config = load_server_config_from_store().await?;
    let context = plugin_instance_domain_context(PluginContractDomain::Audit);
    Ok(find_target_instance(
        context.specs,
        context.route_prefix,
        &config,
        runtime_statuses,
        instance_id,
    ))
}

pub struct ListPluginInstancesHandler {}

#[async_trait::async_trait]
impl Operation for ListPluginInstancesHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_plugin_instance_request(&req).await?;
        let filters = extract_plugin_instance_filters(&req)?;
        let instances = filter_plugin_instances(collect_all_instances().await?, &filters);
        let (instances, truncated, next_marker) = paginate_plugin_instances(instances, &filters)?;
        let data = serde_json::to_vec(&PluginInstancesResponse {
            instances,
            truncated,
            next_marker,
        })
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

        let instance = if let Some(instance) = find_notification_instance(instance_id).await? {
            instance
        } else {
            find_audit_instance(instance_id)
                .await?
                .ok_or_else(|| s3_error!(NoSuchKey, "plugin instance not found"))?
        };

        let data = serde_json::to_vec(&map_instance(instance))
            .map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
        Ok(build_json_response(StatusCode::OK, Body::from(data), req.headers.get("x-request-id")))
    }
}

pub struct PutPluginInstanceHandler {}

#[async_trait::async_trait]
impl Operation for PutPluginInstanceHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_plugin_instance_write_request(&req).await?;
        let instance_id = params
            .get("id")
            .ok_or_else(|| s3_error!(InvalidArgument, "missing required parameter: 'id'"))?;
        let resolved = resolve_plugin_instance_target(instance_id)?;
        let context = resolved.context;

        if let Some(reason) = plugin_instance_operation_block_reason(context, "managing plugin instances from the console").await
        {
            return Err(s3_error!(InvalidRequest, "{reason}"));
        }

        let mut input = req.input;
        let body_bytes = input
            .store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE)
            .await
            .map_err(|_| s3_error!(InvalidRequest, "failed to read request body"))?;
        let body: PluginInstanceBody = serde_json::from_slice(&body_bytes)
            .map_err(|e| s3_error!(InvalidArgument, "invalid json body for plugin instance config: {}", e))?;

        match context.domain {
            PluginContractDomain::Notify => {
                let ns = rustfs_notify::notification_system()
                    .ok_or_else(|| s3_error!(InternalError, "notification system not initialized"))?;
                let config_snapshot = ns.config.read().await.clone();
                if let Some(reason) = plugin_instance_mutation_block_reason(
                    context,
                    &config_snapshot,
                    resolved.target_spec.subsystem,
                    &resolved.target_name,
                    "plugin instance",
                ) {
                    return Err(s3_error!(InvalidRequest, "{reason}"));
                }

                let kvs = build_enabled_target_kvs(
                    context.specs,
                    body.key_values.iter().map(|kv| (kv.key.as_str(), kv.value.as_str())),
                    resolved.target_spec.subsystem,
                    context.default_queue_dir,
                    "plugin instance",
                )
                .await?;

                ns.set_target_config(resolved.target_spec.subsystem, &resolved.target_name, kvs)
                    .await
                    .map_err(|e| s3_error!(InternalError, "failed to set plugin instance config: {}", e))?;
            }
            PluginContractDomain::Audit => {
                let config_snapshot = load_server_config_from_store().await?;
                if let Some(reason) = plugin_instance_mutation_block_reason(
                    context,
                    &config_snapshot,
                    resolved.target_spec.subsystem,
                    &resolved.target_name,
                    "plugin instance",
                ) {
                    return Err(s3_error!(InvalidRequest, "{reason}"));
                }

                let kvs = build_enabled_target_kvs(
                    context.specs,
                    body.key_values.iter().map(|kv| (kv.key.as_str(), kv.value.as_str())),
                    resolved.target_spec.subsystem,
                    context.default_queue_dir,
                    "plugin instance",
                )
                .await?;

                update_audit_config_and_reload(|config| {
                    config
                        .0
                        .entry(resolved.target_spec.subsystem.to_lowercase())
                        .or_default()
                        .insert(resolved.target_name.to_lowercase(), kvs.clone());
                    true
                })
                .await?;
            }
        }

        Ok(build_json_response(StatusCode::OK, Body::empty(), req.headers.get("x-request-id")))
    }
}

pub struct DeletePluginInstanceHandler {}

#[async_trait::async_trait]
impl Operation for DeletePluginInstanceHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_plugin_instance_write_request(&req).await?;
        let instance_id = params
            .get("id")
            .ok_or_else(|| s3_error!(InvalidArgument, "missing required parameter: 'id'"))?;
        let resolved = resolve_plugin_instance_target(instance_id)?;
        let context = resolved.context;

        if let Some(reason) = plugin_instance_operation_block_reason(context, "managing plugin instances from the console").await
        {
            return Err(s3_error!(InvalidRequest, "{reason}"));
        }

        match context.domain {
            PluginContractDomain::Notify => {
                let ns = rustfs_notify::notification_system()
                    .ok_or_else(|| s3_error!(InternalError, "notification system not initialized"))?;
                let config_snapshot = ns.config.read().await.clone();
                if let Some(reason) = plugin_instance_mutation_block_reason(
                    context,
                    &config_snapshot,
                    resolved.target_spec.subsystem,
                    &resolved.target_name,
                    "plugin instance",
                ) {
                    return Err(s3_error!(InvalidRequest, "{reason}"));
                }

                ns.remove_target_config(resolved.target_spec.subsystem, &resolved.target_name)
                    .await
                    .map_err(|e| s3_error!(InternalError, "failed to remove plugin instance config: {}", e))?;
            }
            PluginContractDomain::Audit => {
                let config_snapshot = load_server_config_from_store().await?;
                if let Some(reason) = plugin_instance_mutation_block_reason(
                    context,
                    &config_snapshot,
                    resolved.target_spec.subsystem,
                    &resolved.target_name,
                    "plugin instance",
                ) {
                    return Err(s3_error!(InvalidRequest, "{reason}"));
                }

                update_audit_config_and_reload(|config| {
                    let mut changed = false;
                    if let Some(targets) = config.0.get_mut(&resolved.target_spec.subsystem.to_lowercase()) {
                        if targets.remove(&resolved.target_name.to_lowercase()).is_some() {
                            changed = true;
                        }
                        if targets.is_empty() {
                            config.0.remove(&resolved.target_spec.subsystem.to_lowercase());
                        }
                    }
                    changed
                })
                .await?;
            }
        }

        Ok(build_json_response(StatusCode::OK, Body::empty(), req.headers.get("x-request-id")))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        PluginContractDomain, PluginInstanceFilters, extract_plugin_instance_filters, filter_plugin_instances, map_instance,
        paginate_plugin_instances, parse_bool_filter, parse_instance_status, parse_limit_filter, parse_plugin_contract_domain,
        parse_plugin_instance_id, parse_plugin_instance_source, resolve_plugin_instance_target,
    };
    use crate::admin::handlers::target_descriptor::{
        TargetEndpointSource, TargetInstanceReadModel, canonical_target_instance_id, collect_target_instances,
    };
    use crate::admin::plugin_contract::{PluginInstanceEntry, PluginInstanceSource};
    use http::{Extensions, HeaderMap, Uri};
    use hyper::Method;
    use rustfs_config::audit::AUDIT_WEBHOOK_SUB_SYS;
    use rustfs_config::notify::NOTIFY_ROUTE_PREFIX;
    use rustfs_config::notify::NOTIFY_WEBHOOK_SUB_SYS;
    use rustfs_config::{ENABLE_KEY, WEBHOOK_ENDPOINT};
    use rustfs_ecstore::config::{Config, KV, KVS};
    use rustfs_targets::TargetDomain;
    use s3s::{Body, S3Request};
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
        let detail_block = extract_block_between_markers(
            src,
            "impl Operation for GetPluginInstanceHandler",
            "pub struct PutPluginInstanceHandler",
        );
        let put_block = extract_block_between_markers(
            src,
            "impl Operation for PutPluginInstanceHandler",
            "pub struct DeletePluginInstanceHandler",
        );
        let delete_block = extract_block_between_markers(src, "impl Operation for DeletePluginInstanceHandler", "#[cfg(test)]");

        assert!(
            list_block.contains("authorize_plugin_instance_request(&req).await?;"),
            "plugin instance list should require admin authorization"
        );
        assert!(
            detail_block.contains("authorize_plugin_instance_request(&req).await?;"),
            "plugin instance detail should require admin authorization"
        );
        assert!(
            put_block.contains("authorize_plugin_instance_write_request(&req).await?;"),
            "plugin instance writes should require SetBucketTargetAction"
        );
        assert!(
            delete_block.contains("authorize_plugin_instance_write_request(&req).await?;"),
            "plugin instance deletion should require SetBucketTargetAction"
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
        assert_eq!(runtime_only.plugin_id, "builtin:webhook");
        assert_eq!(runtime_only.subsystem, NOTIFY_WEBHOOK_SUB_SYS);
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
        assert_eq!(mapped.domain, PluginContractDomain::Audit);
    }

    #[test]
    fn canonical_id_is_stable_and_lowercases_instance_segment() {
        assert_eq!(
            canonical_target_instance_id("builtin:webhook", TargetDomain::Notify, "PrimaryCase"),
            "builtin:webhook:notify:primarycase"
        );
    }

    #[test]
    fn parse_plugin_instance_id_extracts_plugin_domain_and_name() {
        let (plugin_id, domain, target_name) =
            parse_plugin_instance_id("builtin:webhook:notify:PrimaryCase").expect("instance id should parse");

        assert_eq!(plugin_id, "builtin:webhook");
        assert_eq!(domain, PluginContractDomain::Notify);
        assert_eq!(target_name, "primarycase");
    }

    #[test]
    fn parse_plugin_instance_id_rejects_invalid_shape() {
        let err = parse_plugin_instance_id("builtin:webhook").expect_err("truncated id should fail");
        assert!(err.to_string().contains("invalid plugin instance id"));
    }

    #[test]
    fn resolve_plugin_instance_target_uses_shared_specs() {
        let resolved =
            resolve_plugin_instance_target("builtin:webhook:audit:Primary").expect("builtin audit instance should resolve");

        assert_eq!(resolved.context.domain, PluginContractDomain::Audit);
        assert_eq!(resolved.target_name, "primary");
        assert_eq!(resolved.target_spec.service, "webhook");
        assert_eq!(resolved.target_spec.subsystem, AUDIT_WEBHOOK_SUB_SYS);
    }

    #[test]
    fn extract_plugin_instance_filters_parses_supported_query_fields() {
        let req = build_plugin_instances_request(
            "/rustfs/admin/v4/plugins/instances?domain=notify&service=webhook&status=offline&source=env&enabled=true&q=Primary&limit=25&marker=builtin:webhook:notify:seed",
        );

        let filters = extract_plugin_instance_filters(&req).expect("query should parse");
        assert_eq!(
            filters,
            PluginInstanceFilters {
                domain: Some(PluginContractDomain::Notify),
                service: Some("webhook".to_string()),
                status: Some("offline".to_string()),
                source: Some(PluginInstanceSource::Env),
                enabled: Some(true),
                query: Some("primary".to_string()),
                limit: Some(25),
                marker: Some("builtin:webhook:notify:seed".to_string()),
            }
        );
    }

    #[test]
    fn extract_plugin_instance_filters_rejects_invalid_enum_values() {
        let err = parse_plugin_contract_domain("invalid").expect_err("invalid domain should fail");
        assert!(err.to_string().contains("invalid plugin instance domain filter"));

        let err = parse_plugin_instance_source("weird").expect_err("invalid source should fail");
        assert!(err.to_string().contains("invalid plugin instance source filter"));

        let err = parse_instance_status("unknown").expect_err("invalid status should fail");
        assert!(err.to_string().contains("invalid plugin instance status filter"));

        let err = parse_bool_filter("maybe").expect_err("invalid bool should fail");
        assert!(err.to_string().contains("invalid plugin instance enabled filter"));

        let err = parse_limit_filter("0").expect_err("zero limit should fail");
        assert!(err.to_string().contains("invalid plugin instance limit filter"));
    }

    #[test]
    fn filter_plugin_instances_applies_all_supported_filters() {
        let matched = sample_instance(SampleInstance {
            id: "builtin:webhook:notify:primary",
            plugin_id: "builtin:webhook",
            domain: PluginContractDomain::Notify,
            subsystem: "notify_webhook",
            account_id: "primary",
            service: "webhook",
            status: "offline",
            source: PluginInstanceSource::Env,
            enabled: true,
        });
        let filtered = filter_plugin_instances(
            vec![
                matched.clone(),
                sample_instance(SampleInstance {
                    id: "builtin:webhook:audit:primary",
                    plugin_id: "builtin:webhook",
                    domain: PluginContractDomain::Audit,
                    subsystem: "audit_webhook",
                    account_id: "primary",
                    service: "webhook",
                    status: "offline",
                    source: PluginInstanceSource::Env,
                    enabled: true,
                }),
                sample_instance(SampleInstance {
                    id: "builtin:kafka:notify:secondary",
                    plugin_id: "builtin:kafka",
                    domain: PluginContractDomain::Notify,
                    subsystem: "notify_kafka",
                    account_id: "secondary",
                    service: "kafka",
                    status: "online",
                    source: PluginInstanceSource::Config,
                    enabled: false,
                }),
            ],
            &PluginInstanceFilters {
                domain: Some(PluginContractDomain::Notify),
                service: Some("webhook".to_string()),
                status: Some("offline".to_string()),
                source: Some(PluginInstanceSource::Env),
                enabled: Some(true),
                query: Some("primary".to_string()),
                limit: None,
                marker: None,
            },
        );

        assert_eq!(filtered, vec![matched]);
    }

    #[test]
    fn filter_plugin_instances_search_matches_multiple_identity_fields_case_insensitively() {
        let instances = vec![
            sample_instance(SampleInstance {
                id: "builtin:webhook:notify:primary",
                plugin_id: "builtin:webhook",
                domain: PluginContractDomain::Notify,
                subsystem: "notify_webhook",
                account_id: "Primary",
                service: "webhook",
                status: "offline",
                source: PluginInstanceSource::Config,
                enabled: true,
            }),
            sample_instance(SampleInstance {
                id: "builtin:kafka:notify:secondary",
                plugin_id: "builtin:kafka",
                domain: PluginContractDomain::Notify,
                subsystem: "notify_kafka",
                account_id: "secondary",
                service: "kafka",
                status: "online",
                source: PluginInstanceSource::Runtime,
                enabled: true,
            }),
        ];

        let filtered = filter_plugin_instances(
            instances,
            &PluginInstanceFilters {
                query: Some("NOTIFY_KAFKA".to_string().to_ascii_lowercase()),
                ..PluginInstanceFilters::default()
            },
        );

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].plugin_id, "builtin:kafka");
    }

    #[test]
    fn paginate_plugin_instances_returns_requested_page_and_next_marker() {
        let instances = vec![
            sample_instance(SampleInstance {
                id: "builtin:amqp:notify:a",
                plugin_id: "builtin:amqp",
                domain: PluginContractDomain::Notify,
                subsystem: "notify_amqp",
                account_id: "a",
                service: "amqp",
                status: "offline",
                source: PluginInstanceSource::Config,
                enabled: true,
            }),
            sample_instance(SampleInstance {
                id: "builtin:kafka:notify:b",
                plugin_id: "builtin:kafka",
                domain: PluginContractDomain::Notify,
                subsystem: "notify_kafka",
                account_id: "b",
                service: "kafka",
                status: "online",
                source: PluginInstanceSource::Env,
                enabled: true,
            }),
            sample_instance(SampleInstance {
                id: "builtin:webhook:notify:c",
                plugin_id: "builtin:webhook",
                domain: PluginContractDomain::Notify,
                subsystem: "notify_webhook",
                account_id: "c",
                service: "webhook",
                status: "offline",
                source: PluginInstanceSource::Runtime,
                enabled: true,
            }),
        ];

        let (page, truncated, next_marker) = paginate_plugin_instances(
            instances,
            &PluginInstanceFilters {
                limit: Some(2),
                ..PluginInstanceFilters::default()
            },
        )
        .expect("pagination should succeed");

        assert_eq!(page.len(), 2);
        assert!(truncated);
        assert_eq!(next_marker.as_deref(), Some("builtin:kafka:notify:b"));
    }

    #[test]
    fn paginate_plugin_instances_respects_marker_after_filtered_results() {
        let instances = vec![
            sample_instance(SampleInstance {
                id: "builtin:amqp:notify:a",
                plugin_id: "builtin:amqp",
                domain: PluginContractDomain::Notify,
                subsystem: "notify_amqp",
                account_id: "a",
                service: "amqp",
                status: "offline",
                source: PluginInstanceSource::Config,
                enabled: true,
            }),
            sample_instance(SampleInstance {
                id: "builtin:kafka:notify:b",
                plugin_id: "builtin:kafka",
                domain: PluginContractDomain::Notify,
                subsystem: "notify_kafka",
                account_id: "b",
                service: "kafka",
                status: "online",
                source: PluginInstanceSource::Env,
                enabled: true,
            }),
            sample_instance(SampleInstance {
                id: "builtin:webhook:notify:c",
                plugin_id: "builtin:webhook",
                domain: PluginContractDomain::Notify,
                subsystem: "notify_webhook",
                account_id: "c",
                service: "webhook",
                status: "offline",
                source: PluginInstanceSource::Runtime,
                enabled: true,
            }),
        ];

        let (page, truncated, next_marker) = paginate_plugin_instances(
            instances,
            &PluginInstanceFilters {
                marker: Some("builtin:amqp:notify:a".to_string()),
                ..PluginInstanceFilters::default()
            },
        )
        .expect("pagination should succeed");

        assert_eq!(page.len(), 2);
        assert_eq!(page[0].id, "builtin:kafka:notify:b");
        assert!(!truncated);
        assert_eq!(next_marker, None);
    }

    #[test]
    fn paginate_plugin_instances_rejects_unknown_marker() {
        let err = paginate_plugin_instances(
            vec![sample_instance(SampleInstance {
                id: "builtin:webhook:notify:c",
                plugin_id: "builtin:webhook",
                domain: PluginContractDomain::Notify,
                subsystem: "notify_webhook",
                account_id: "c",
                service: "webhook",
                status: "offline",
                source: PluginInstanceSource::Runtime,
                enabled: true,
            })],
            &PluginInstanceFilters {
                marker: Some("missing".to_string()),
                ..PluginInstanceFilters::default()
            },
        )
        .expect_err("unknown marker should fail");

        assert!(err.to_string().contains("invalid plugin instance marker"));
    }

    struct SampleInstance<'a> {
        id: &'a str,
        plugin_id: &'a str,
        domain: PluginContractDomain,
        subsystem: &'a str,
        account_id: &'a str,
        service: &'a str,
        status: &'a str,
        source: PluginInstanceSource,
        enabled: bool,
    }

    fn sample_instance(input: SampleInstance<'_>) -> PluginInstanceEntry {
        PluginInstanceEntry {
            id: input.id.to_string(),
            plugin_id: input.plugin_id.to_string(),
            domain: input.domain,
            subsystem: input.subsystem.to_string(),
            account_id: input.account_id.to_string(),
            service: input.service.to_string(),
            status: input.status.to_string(),
            source: input.source,
            enabled: input.enabled,
            config: HashMap::new(),
        }
    }

    fn build_plugin_instances_request(uri: &'static str) -> S3Request<Body> {
        S3Request {
            input: Body::empty(),
            method: Method::GET,
            uri: Uri::from_static(uri),
            headers: HeaderMap::new(),
            extensions: Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
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
