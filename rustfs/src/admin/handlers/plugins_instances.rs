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

use crate::admin::operation::{AdminRequest, AdminResponse, Operation};
use crate::admin::{
    auth::authorize_admin_envelope,
    handlers::audit_runtime_config::{remove_audit_target_config, set_audit_target_config},
    handlers::event::{converge_target_mutation_on_cluster, preflight_target_mutation_on_cluster},
    handlers::notify_runtime_access::{
        get_notification_system, load_notification_mutation_snapshot, remove_notification_target_config,
        set_notification_target_config,
    },
    handlers::supervise_admin_mutation,
    handlers::target_descriptor::{
        AdminTargetSpec, RuntimeHealthStatus, TargetEndpointSource, TargetInstanceReadModel, admin_target_spec_from_builtin,
        allowed_target_keys, build_enabled_target_kvs, build_validated_target_kvs, collect_target_instances,
        collect_validated_key_values, find_target_instance, owned_build_json_response, target_module_disabled_reason,
        target_mutation_block_reason as shared_target_mutation_block_reason,
    },
    handlers::target_read::{
        collect_audit_runtime_statuses, collect_notification_instance_runtime_statuses, collect_notification_runtime_statuses,
        load_server_config_from_store, load_target_read_snapshot,
    },
    plugin_contract::{
        PluginContractDomain, PluginInstanceDetail, PluginInstanceDiagnostic, PluginInstanceDiagnosticCode,
        PluginInstanceDiagnosticCount, PluginInstanceEntry, PluginInstanceSource, PluginInstancesResponse,
    },
    router::{AdminOperation, S3Router},
    runtime_sources::{app_context_from_envelope, current_object_store_handle_for_context},
    storage_api::config::{read_admin_server_config_snapshot, read_admin_server_config_with_generation},
    storage_api::s3::{self, S3ErrorCode},
};
use crate::server::{
    ADMIN_PREFIX, is_audit_module_enabled, is_notify_module_enabled, refresh_audit_module_enabled, refresh_notify_module_enabled,
    refresh_persisted_module_switches_from_store,
};
use hyper::{Method, StatusCode, Uri};
use matchit::Params;
use rustfs_config::audit::AUDIT_ROUTE_PREFIX;
use rustfs_config::notify::NOTIFY_ROUTE_PREFIX;
use rustfs_config::server_config::{Config, KVS};
use rustfs_config::{AUDIT_DEFAULT_DIR, EVENT_DEFAULT_DIR, MAX_ADMIN_REQUEST_BODY_SIZE};
use rustfs_policy::policy::action::{Action, AdminAction};
use rustfs_s3_types::{Body, S3Result, s3_error};
use rustfs_targets::catalog::builtin::{builtin_audit_target_admin_descriptors, builtin_notify_target_admin_descriptors};
use rustfs_targets::{builtin_target_plugin_operational_state, runtime_state_from_status_label};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
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

const REDACTED_SECRET_VALUE: &str = "***redacted***";

/// Secret fields for an instance, resolved from the plugin manifest carried by
/// the domain's admin target specs (single source of truth for redaction).
fn secret_fields_for_instance(domain: PluginContractDomain, subsystem: &str) -> &'static [&'static str] {
    plugin_instance_domain_context(domain)
        .specs
        .iter()
        .find(|spec| spec.subsystem == subsystem)
        .map(|spec| spec.secret_fields)
        .unwrap_or(&[])
}

fn map_instance_config(config: KVS, secret_fields: &[&str]) -> HashMap<String, String> {
    config
        .0
        .into_iter()
        .map(|kv| {
            let should_redact = secret_fields.iter().any(|field| field.eq_ignore_ascii_case(&kv.key));
            let value = if should_redact && !kv.value.is_empty() {
                REDACTED_SECRET_VALUE.to_string()
            } else {
                kv.value
            };
            (kv.key, value)
        })
        .collect()
}

/// Restores stored secret values for keys the client sent back as the
/// redacted placeholder. GET responses redact secrets, so a read-modify-write
/// round trip must not persist the placeholder literal as the new secret.
fn restore_redacted_secret_values(
    key_values: &mut [KeyValue],
    secret_fields: &[&str],
    stored_config: Option<&KVS>,
) -> S3Result<()> {
    for kv in key_values {
        if kv.value != REDACTED_SECRET_VALUE {
            continue;
        }
        if !secret_fields.iter().any(|field| field.eq_ignore_ascii_case(&kv.key)) {
            continue;
        }
        let stored = stored_config
            .and_then(|kvs| kvs.lookup(&kv.key))
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                s3_error!(
                    InvalidArgument,
                    "config key '{}' uses the redacted placeholder but there is no stored secret to preserve",
                    kv.key
                )
            })?;
        kv.value = stored;
    }
    Ok(())
}

/// Case-insensitive lookup of an instance's persisted config section.
fn stored_instance_config<'a>(config: &'a Config, subsystem: &str, target_name: &str) -> Option<&'a KVS> {
    config.0.get(subsystem).and_then(|section| {
        section
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(target_name))
            .map(|(_, kvs)| kvs)
    })
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
    let runtime_state = runtime_state_from_status_label(&instance.status);
    let domain = PluginContractDomain::from(instance.domain);
    let secret_fields = secret_fields_for_instance(domain, &instance.subsystem);
    let config = map_instance_config(instance.config, secret_fields);

    PluginInstanceEntry {
        id: instance.canonical_id,
        plugin_id: instance.plugin_id,
        domain,
        subsystem: instance.subsystem,
        account_id: instance.account_id,
        service: instance.service,
        status: instance.status,
        health_state: instance.health_state,
        health_reason: instance.health_reason,
        source: map_instance_source(instance.source),
        enabled: instance.enabled,
        config,
        operational_state: Some(builtin_target_plugin_operational_state(instance.enabled, runtime_state).into()),
        diagnostic_codes: Vec::new(),
    }
}

fn diagnostic(code: PluginInstanceDiagnosticCode, message: impl Into<String>) -> PluginInstanceDiagnostic {
    PluginInstanceDiagnostic {
        code,
        message: message.into(),
    }
}

fn collect_instance_diagnostics(
    instance: &TargetInstanceReadModel,
    module_disabled_reason: Option<String>,
) -> Vec<PluginInstanceDiagnostic> {
    let mut diagnostics = Vec::new();

    if let Some(reason) = module_disabled_reason {
        diagnostics.push(diagnostic(PluginInstanceDiagnosticCode::ModuleDisabled, reason));
    }

    if !instance.enabled {
        diagnostics.push(diagnostic(
            PluginInstanceDiagnosticCode::InstanceDisabled,
            "plugin instance is disabled in its effective configuration",
        ));
    }

    match instance.source {
        TargetEndpointSource::Env => diagnostics.push(diagnostic(
            PluginInstanceDiagnosticCode::EnvironmentManaged,
            "plugin instance is managed by environment variables and cannot be edited from persisted config",
        )),
        TargetEndpointSource::Mixed => diagnostics.push(diagnostic(
            PluginInstanceDiagnosticCode::MixedSource,
            "plugin instance is configured by both persisted config and environment variables",
        )),
        TargetEndpointSource::Config | TargetEndpointSource::Runtime => {}
    }

    if instance.status.eq_ignore_ascii_case("offline") {
        if instance.enabled && !instance.runtime_present {
            diagnostics.push(diagnostic(
                PluginInstanceDiagnosticCode::NotLoadedInRuntime,
                "plugin instance is enabled in config but not currently loaded in runtime",
            ));
        } else if instance.runtime_present {
            diagnostics.push(diagnostic(
                PluginInstanceDiagnosticCode::RuntimeOffline,
                "plugin instance exists in runtime but its health check is offline",
            ));
        }
    }

    diagnostics
}

/// Builds an instance detail view. Callers must run
/// [`refresh_persisted_module_switches`] once beforehand.
fn plugin_instance_detail(instance: TargetInstanceReadModel) -> PluginInstanceDetail {
    let domain = PluginContractDomain::from(instance.domain);
    let module_disabled_reason = module_disabled_block_reason(domain, "reading plugin instance diagnostics");
    let diagnostics = collect_instance_diagnostics(&instance, module_disabled_reason);
    let mut mapped = map_instance(instance);
    mapped.diagnostic_codes = diagnostics.iter().map(|item| item.code.clone()).collect();

    PluginInstanceDetail {
        config_revision: None,
        instance: mapped,
        diagnostics,
    }
}

fn plugin_instance_list_entry(instance: TargetInstanceReadModel, module_disabled_reason: Option<String>) -> PluginInstanceEntry {
    let diagnostics = collect_instance_diagnostics(&instance, module_disabled_reason);
    let mut mapped = map_instance(instance);
    mapped.diagnostic_codes = diagnostics.into_iter().map(|item| item.code).collect();
    mapped
}

fn map_instance_source(source: TargetEndpointSource) -> PluginInstanceSource {
    match source {
        TargetEndpointSource::Config => PluginInstanceSource::Config,
        TargetEndpointSource::Env => PluginInstanceSource::Env,
        TargetEndpointSource::Mixed => PluginInstanceSource::Mixed,
        TargetEndpointSource::Runtime => PluginInstanceSource::Runtime,
    }
}

#[derive(Debug, Clone)]
struct ResolvedPluginInstanceTarget {
    context: PluginInstanceDomainContext,
    target_name: String,
    target_spec: AdminTargetSpec,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyValue {
    key: String,
    value: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PluginInstanceBody {
    key_values: Vec<KeyValue>,
    #[serde(default)]
    expected_config: Option<HashMap<String, String>>,
    #[serde(default)]
    expected_revision: Option<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct PluginInstanceFilters {
    domain: Option<PluginContractDomain>,
    service: Option<String>,
    status: Option<String>,
    source: Option<PluginInstanceSource>,
    diagnostic_code: Option<PluginInstanceDiagnosticCode>,
    enabled: Option<bool>,
    query: Option<String>,
    limit: Option<usize>,
    marker: Option<String>,
}

pub(super) fn extract_plugin_instance_filters(uri: &Uri) -> S3Result<PluginInstanceFilters> {
    let mut filters = PluginInstanceFilters::default();

    if let Some(query) = uri.query() {
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
                "diagnostic_code" => filters.diagnostic_code = Some(parse_plugin_instance_diagnostic_code(value)?),
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

fn parse_plugin_instance_diagnostic_code(value: &str) -> S3Result<PluginInstanceDiagnosticCode> {
    match value.to_ascii_lowercase().as_str() {
        "module_disabled" => Ok(PluginInstanceDiagnosticCode::ModuleDisabled),
        "instance_disabled" => Ok(PluginInstanceDiagnosticCode::InstanceDisabled),
        "environment_managed" => Ok(PluginInstanceDiagnosticCode::EnvironmentManaged),
        "mixed_source" => Ok(PluginInstanceDiagnosticCode::MixedSource),
        "not_loaded_in_runtime" => Ok(PluginInstanceDiagnosticCode::NotLoadedInRuntime),
        "runtime_offline" => Ok(PluginInstanceDiagnosticCode::RuntimeOffline),
        _ => Err(s3_error!(InvalidArgument, "invalid plugin instance diagnostic_code filter: '{}'", value)),
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

pub(super) fn filter_plugin_instances(
    mut instances: Vec<PluginInstanceEntry>,
    filters: &PluginInstanceFilters,
) -> Vec<PluginInstanceEntry> {
    instances.retain(|instance| plugin_instance_matches_filters(instance, filters));
    instances
}

pub(super) fn paginate_plugin_instances(
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

pub(super) fn collect_diagnostic_counts(instances: &[PluginInstanceEntry]) -> Vec<PluginInstanceDiagnosticCount> {
    let mut counts = BTreeMap::<PluginInstanceDiagnosticCode, usize>::new();
    for instance in instances {
        for code in &instance.diagnostic_codes {
            *counts.entry(code.clone()).or_default() += 1;
        }
    }

    counts
        .into_iter()
        .map(|(code, count)| PluginInstanceDiagnosticCount { code, count })
        .collect()
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

    if let Some(diagnostic_code) = &filters.diagnostic_code
        && !instance.diagnostic_codes.contains(diagnostic_code)
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

/// The pre-check keeps this endpoint's historical missing-credentials message;
/// the shared gate reports "get cred failed".
async fn authorize_plugin_instance_request(req: &AdminRequest) -> S3Result<()> {
    if req.credentials().is_none() {
        return Err(s3_error!(InvalidRequest, "authentication required"));
    }

    authorize_admin_envelope(req.envelope(), vec![Action::AdminAction(AdminAction::GetBucketTargetAction)]).await?;
    Ok(())
}

/// The pre-check keeps this endpoint's historical missing-credentials message;
/// the shared gate reports "get cred failed".
async fn authorize_plugin_instance_write_request(req: &AdminRequest) -> S3Result<()> {
    if req.credentials().is_none() {
        return Err(s3_error!(InvalidRequest, "authentication required"));
    }

    authorize_admin_envelope(req.envelope(), vec![Action::AdminAction(AdminAction::SetBucketTargetAction)]).await?;
    Ok(())
}

fn plugin_instance_mutation_block_reason(
    context: PluginInstanceDomainContext,
    config: &Config,
    target_type: &str,
    target_name: &str,
    target_label: &str,
) -> S3Result<Option<String>> {
    shared_target_mutation_block_reason(context.specs, context.route_prefix, config, target_type, target_name, target_label)
}

/// Reloads persisted module switches from the config store. Call once per
/// admin request before evaluating [`module_disabled_block_reason`] so that
/// list/detail paths do not hit the store once per domain or per instance.
async fn refresh_persisted_module_switches() -> S3Result<()> {
    if let Err(err) = refresh_persisted_module_switches_from_store().await {
        warn!(
            error = %err,
            "failed to reload persisted module switches before checking plugin instance operation gating"
        );
        return Err(s3::error(S3ErrorCode::InternalError, "failed to refresh target module state"));
    }
    Ok(())
}

fn module_disabled_block_reason(domain: PluginContractDomain, action: &str) -> Option<String> {
    match domain {
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

async fn plugin_instance_operation_block_reason(context: PluginInstanceDomainContext, action: &str) -> S3Result<Option<String>> {
    refresh_persisted_module_switches().await?;
    Ok(module_disabled_block_reason(context.domain, action))
}

async fn plugin_instance_runtime_statuses(
    context: PluginInstanceDomainContext,
) -> HashMap<(String, String), RuntimeHealthStatus> {
    match context.domain {
        PluginContractDomain::Notify => collect_notification_runtime_statuses().await,
        PluginContractDomain::Audit => collect_audit_runtime_statuses().await,
    }
}

async fn plugin_instance_mutation_config_snapshot(context: PluginInstanceDomainContext) -> S3Result<Config> {
    match context.domain {
        PluginContractDomain::Notify => load_notification_mutation_snapshot().await.map(|(_, config)| config),
        PluginContractDomain::Audit => load_server_config_from_store().await,
    }
}

fn collect_domain_instances(
    context: PluginInstanceDomainContext,
    config: &Config,
    runtime_statuses: HashMap<(String, String), RuntimeHealthStatus>,
) -> S3Result<Vec<PluginInstanceEntry>> {
    let module_disabled_reason = module_disabled_block_reason(context.domain, "listing plugin instances");
    let mut entries = Vec::new();
    for instance in collect_target_instances(context.specs, context.route_prefix, config, runtime_statuses)? {
        entries.push(plugin_instance_list_entry(instance, module_disabled_reason.clone()));
    }
    Ok(entries)
}

pub(super) async fn collect_all_instances() -> S3Result<Vec<PluginInstanceEntry>> {
    refresh_persisted_module_switches().await?;
    let notify_context = plugin_instance_domain_context(PluginContractDomain::Notify);
    let audit_context = plugin_instance_domain_context(PluginContractDomain::Audit);
    let (config, (notify_statuses, audit_statuses)) = load_target_read_snapshot(async {
        tokio::join!(
            plugin_instance_runtime_statuses(notify_context),
            plugin_instance_runtime_statuses(audit_context)
        )
    })
    .await?;
    let mut notify_instances = collect_domain_instances(notify_context, &config, notify_statuses)?;
    let audit_instances = collect_domain_instances(audit_context, &config, audit_statuses)?;
    notify_instances.extend(audit_instances);
    notify_instances.sort_by(|a, b| a.service.cmp(&b.service).then_with(|| a.account_id.cmp(&b.account_id)));
    Ok(notify_instances)
}

async fn find_plugin_instance(instance_id: &str) -> S3Result<Option<TargetInstanceReadModel>> {
    let context = plugin_instance_domain_context(parse_plugin_instance_id(instance_id)?.1);
    let (config, runtime_statuses) = load_target_read_snapshot(plugin_instance_runtime_statuses(context)).await?;
    find_target_instance(context.specs, context.route_prefix, &config, runtime_statuses, instance_id)
}

fn effective_validation_fields(
    context: PluginInstanceDomainContext,
    subsystem: &str,
    effective: &KVS,
    requested: &[KeyValue],
) -> S3Result<HashMap<String, String>> {
    let allowed = allowed_target_keys(context.specs, subsystem);
    let requested = collect_validated_key_values(
        requested.iter().map(|kv| (kv.key.as_str(), kv.value.as_str())),
        &allowed,
        subsystem,
        "plugin instance",
    )?;
    let mut candidate: HashMap<String, String> = effective
        .0
        .iter()
        .filter(|kv| allowed.contains(kv.key.as_str()))
        .map(|kv| (kv.key.clone(), kv.value.clone()))
        .collect();
    candidate.extend(requested);
    Ok(candidate)
}

pub(super) fn merge_conditional_target_update(
    original: &KVS,
    expected: &HashMap<String, String>,
    changed_keys: &[String],
    validated: &KVS,
) -> S3Result<KVS> {
    let mut result = original.clone();
    for key in changed_keys {
        if key == rustfs_config::ENABLE_KEY {
            return Err(s3::error(
                S3ErrorCode::InvalidRequest,
                "target enable state cannot be changed using expected_config",
            ));
        }
        let value = validated
            .lookup(key)
            .ok_or_else(|| s3::error(S3ErrorCode::InvalidArgument, "missing validated target field"))?;
        if expected.get(key) != Some(&value) {
            result.insert(key.clone(), value);
        }
    }
    Ok(result)
}

pub(super) fn config_revision(instance_id: &str, generation: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"rustfs-plugin-instance-config-revision-v1\0");
    hasher.update(generation.as_bytes());
    hasher.update(b"\0");
    hasher.update(instance_id.as_bytes());
    faster_hex::hex_string(&hasher.finalize())
}

fn validate_expected_config(
    context: PluginInstanceDomainContext,
    config: &Config,
    instance_id: &str,
    expected: &HashMap<String, String>,
) -> S3Result<TargetInstanceReadModel> {
    if context.domain != PluginContractDomain::Notify {
        return Err(s3::error(
            S3ErrorCode::InvalidArgument,
            "expected_config is only supported for notification targets",
        ));
    }
    let resolved = resolve_plugin_instance_target(instance_id)?;
    let instance = find_target_instance(
        std::slice::from_ref(&resolved.target_spec),
        context.route_prefix,
        config,
        HashMap::new(),
        instance_id,
    )?
    .filter(|instance| instance.source == TargetEndpointSource::Config)
    .ok_or_else(|| s3::error(S3ErrorCode::InvalidRequest, "expected_config requires an existing config-managed target"))?;
    if instance.has_env_default {
        return Err(s3::error(
            S3ErrorCode::InvalidRequest,
            "notification target inherits environment configuration and cannot be edited",
        ));
    }
    if !instance.enabled {
        return Err(s3::error(
            S3ErrorCode::InvalidRequest,
            "disabled targets cannot be edited with expected_config",
        ));
    }
    if &map_instance_config(instance.config.clone(), secret_fields_for_instance(context.domain, &instance.subsystem)) != expected
    {
        return Err(s3::error(
            S3ErrorCode::InvalidRequest,
            "target configuration changed since it was read; refresh and retry",
        ));
    }
    Ok(instance)
}

async fn set_plugin_instance_config(
    context: PluginInstanceDomainContext,
    resolved: &ResolvedPluginInstanceTarget,
    kvs: KVS,
) -> S3Result<()> {
    match context.domain {
        PluginContractDomain::Notify => {
            set_notification_target_config(resolved.target_spec.subsystem, &resolved.target_name, kvs).await
        }
        PluginContractDomain::Audit => {
            set_audit_target_config(audit_target_specs(), resolved.target_spec.subsystem, &resolved.target_name, kvs).await
        }
    }
}

async fn remove_plugin_instance_config(
    context: PluginInstanceDomainContext,
    resolved: &ResolvedPluginInstanceTarget,
) -> S3Result<()> {
    match context.domain {
        PluginContractDomain::Notify => {
            remove_notification_target_config(resolved.target_spec.subsystem, &resolved.target_name).await
        }
        PluginContractDomain::Audit => {
            remove_audit_target_config(audit_target_specs(), resolved.target_spec.subsystem, &resolved.target_name).await
        }
    }
}

pub struct ListPluginInstancesHandler {}

#[async_trait::async_trait]
impl Operation for ListPluginInstancesHandler {
    async fn call(&self, req: AdminRequest, _params: Params<'_, '_>) -> S3Result<AdminResponse> {
        authorize_plugin_instance_request(&req).await?;
        let filters = extract_plugin_instance_filters(req.uri())?;
        let instances = filter_plugin_instances(collect_all_instances().await?, &filters);
        let diagnostic_counts = collect_diagnostic_counts(&instances);
        let (instances, truncated, next_marker) = paginate_plugin_instances(instances, &filters)?;
        let data = serde_json::to_vec(&PluginInstancesResponse {
            instances,
            diagnostic_counts,
            truncated,
            next_marker,
        })
        .map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
        Ok(owned_build_json_response(
            StatusCode::OK,
            Body::from(data),
            req.headers().get("x-request-id"),
        ))
    }
}

pub struct GetPluginInstanceHandler {}

#[async_trait::async_trait]
impl Operation for GetPluginInstanceHandler {
    async fn call(&self, req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        authorize_plugin_instance_request(&req).await?;
        let instance_id = params
            .get("id")
            .ok_or_else(|| s3_error!(InvalidArgument, "missing required parameter: 'id'"))?;

        refresh_persisted_module_switches().await?;
        let domain = parse_plugin_instance_id(instance_id)?.1;
        let (instance, generation) = if domain == PluginContractDomain::Notify {
            let app_context = app_context_from_envelope(req.envelope());
            let context = plugin_instance_domain_context(domain);
            let read_config = async {
                match current_object_store_handle_for_context(app_context.as_deref()) {
                    Some(store) => read_admin_server_config_with_generation(store)
                        .await
                        .map_err(|_| s3::error(S3ErrorCode::InternalError, "failed to read server config")),
                    None => Ok((Config::new(), None)),
                }
            };
            let ((config, generation), runtime) = tokio::try_join!(read_config, async {
                Ok::<_, rustfs_s3_types::S3Error>(
                    collect_notification_instance_runtime_statuses(context.specs, instance_id).await,
                )
            })?;
            let instance = find_target_instance(context.specs, context.route_prefix, &config, runtime, instance_id)?;
            (instance, generation.map(|value| value.to_string()))
        } else {
            (find_plugin_instance(instance_id).await?, None)
        };
        let instance = instance.ok_or_else(|| s3::error(S3ErrorCode::NoSuchKey, "plugin instance not found"))?;
        let mut detail = plugin_instance_detail(instance);
        detail.config_revision = generation.map(|value| config_revision(instance_id, &value));
        let data = serde_json::to_vec(&detail)
            .map_err(|e| s3::error(S3ErrorCode::InternalError, format!("failed to serialize response: {}", e)))?;
        Ok(owned_build_json_response(
            StatusCode::OK,
            Body::from(data),
            req.headers().get("x-request-id"),
        ))
    }
}

pub struct PutPluginInstanceHandler {}

#[async_trait::async_trait]
impl Operation for PutPluginInstanceHandler {
    async fn call(&self, mut req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        authorize_plugin_instance_write_request(&req).await?;
        let app_context = app_context_from_envelope(req.envelope());
        let instance_id = params
            .get("id")
            .ok_or_else(|| s3_error!(InvalidArgument, "missing required parameter: 'id'"))?;
        let resolved = resolve_plugin_instance_target(instance_id)?;
        let context = resolved.context;

        if let Some(reason) =
            plugin_instance_operation_block_reason(context, "managing plugin instances from the console").await?
        {
            return Err(s3_error!(InvalidRequest, "{reason}"));
        }

        let mut input = req.take_body();
        let body_bytes = input
            .store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE)
            .await
            .map_err(|_| s3_error!(InvalidRequest, "failed to read request body"))?;
        let mut body: PluginInstanceBody = serde_json::from_slice(&body_bytes)
            .map_err(|e| s3_error!(InvalidArgument, "invalid json body for plugin instance config: {}", e))?;

        let (config_snapshot, generation) = if context.domain == PluginContractDomain::Notify {
            let store = current_object_store_handle_for_context(app_context.as_deref())
                .ok_or_else(|| s3::error(S3ErrorCode::InternalError, "server storage not initialized"))?;
            let snapshot = read_admin_server_config_snapshot(store)
                .await
                .map_err(|_| s3::error(S3ErrorCode::InternalError, "failed to read server config"))?;
            (snapshot.config.clone(), snapshot.generation().map(|value| value.to_string()))
        } else {
            (plugin_instance_mutation_config_snapshot(context).await?, None)
        };
        if let Some(reason) = plugin_instance_mutation_block_reason(
            context,
            &config_snapshot,
            resolved.target_spec.subsystem,
            &resolved.target_name,
            "plugin instance",
        )? {
            return Err(s3_error!(InvalidRequest, "{reason}"));
        }

        if body.expected_revision.is_some() && body.expected_config.is_none() {
            return Err(s3::error(S3ErrorCode::InvalidArgument, "expected_revision requires expected_config"));
        }
        let effective_instance = if let Some(expected) = body.expected_config.as_ref() {
            let revision = generation
                .as_ref()
                .ok_or_else(|| s3::error(S3ErrorCode::InvalidRequest, "target configuration revision unavailable"))?;
            if body.expected_revision.as_deref() != Some(config_revision(instance_id, revision).as_str()) {
                return Err(s3::error(
                    S3ErrorCode::InvalidRequest,
                    "target configuration changed since it was read; refresh and retry",
                ));
            }
            Some(validate_expected_config(context, &config_snapshot, instance_id, expected)?.config)
        } else {
            None
        };

        let changed_keys: Vec<String> = body
            .expected_config
            .as_ref()
            .map(|expected| {
                body.key_values
                    .iter()
                    .filter(|kv| expected.get(&kv.key) != Some(&kv.value))
                    .map(|kv| kv.key.clone())
                    .collect()
            })
            .unwrap_or_default();

        // GET responses redact secret values; a read-modify-write round trip
        // sends the placeholder back, which must restore the stored secret
        // instead of persisting the placeholder literal.
        let stored_secrets = if body.expected_config.is_some() && context.domain == PluginContractDomain::Notify {
            effective_instance.as_ref()
        } else {
            stored_instance_config(&config_snapshot, resolved.target_spec.subsystem, &resolved.target_name)
        };
        restore_redacted_secret_values(&mut body.key_values, resolved.target_spec.secret_fields, stored_secrets)?;

        let validated_kvs = if let Some(effective) = effective_instance.as_ref() {
            let candidate = effective_validation_fields(context, resolved.target_spec.subsystem, effective, &body.key_values)?;
            build_validated_target_kvs(&resolved.target_spec, candidate, context.default_queue_dir).await?
        } else {
            build_enabled_target_kvs(
                context.specs,
                body.key_values.iter().map(|kv| (kv.key.as_str(), kv.value.as_str())),
                resolved.target_spec.subsystem,
                context.default_queue_dir,
                "plugin instance",
            )
            .await?
        };

        let kvs = if let Some(expected_config) = body.expected_config.as_ref() {
            let original = stored_instance_config(&config_snapshot, resolved.target_spec.subsystem, &resolved.target_name)
                .ok_or_else(|| {
                    s3::error(S3ErrorCode::InvalidRequest, "expected_config requires an existing config-managed target")
                })?;
            merge_conditional_target_update(original, expected_config, &changed_keys, &validated_kvs)?
        } else {
            validated_kvs
        };

        if context.domain == PluginContractDomain::Notify {
            let expected =
                stored_instance_config(&config_snapshot, resolved.target_spec.subsystem, &resolved.target_name).cloned();
            let expected_default =
                stored_instance_config(&config_snapshot, resolved.target_spec.subsystem, rustfs_config::DEFAULT_DELIMITER)
                    .cloned();
            let expected_generation = if body.expected_config.is_some() { generation } else { None };
            let system = get_notification_system().await?;
            let subsystem = resolved.target_spec.subsystem.to_owned();
            let target_name = resolved.target_name.clone();
            supervise_admin_mutation("plugin notification target mutation", async move {
                preflight_target_mutation_on_cluster(app_context.as_deref(), &subsystem).await?;
                let local = system
                    .set_target_config_if_unchanged(
                        &subsystem,
                        &target_name,
                        expected,
                        expected_default,
                        expected_generation,
                        kvs,
                    )
                    .await;
                if matches!(local, Err(rustfs_notify::NotificationError::ConfigurationConflict)) {
                    return Err(s3::error(
                        S3ErrorCode::InvalidRequest,
                        "target configuration changed during validation; retry the request",
                    ));
                }
                converge_target_mutation_on_cluster(app_context.as_deref(), &subsystem, local).await
            })
            .await?;
        } else {
            set_plugin_instance_config(context, &resolved, kvs).await?;
        }

        Ok(owned_build_json_response(
            StatusCode::OK,
            Body::empty(),
            req.headers().get("x-request-id"),
        ))
    }
}

pub struct DeletePluginInstanceHandler {}

#[async_trait::async_trait]
impl Operation for DeletePluginInstanceHandler {
    async fn call(&self, req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        authorize_plugin_instance_write_request(&req).await?;
        let instance_id = params
            .get("id")
            .ok_or_else(|| s3_error!(InvalidArgument, "missing required parameter: 'id'"))?;
        let resolved = resolve_plugin_instance_target(instance_id)?;
        let context = resolved.context;

        if let Some(reason) =
            plugin_instance_operation_block_reason(context, "managing plugin instances from the console").await?
        {
            return Err(s3_error!(InvalidRequest, "{reason}"));
        }

        let config_snapshot = plugin_instance_mutation_config_snapshot(context).await?;
        if let Some(reason) = plugin_instance_mutation_block_reason(
            context,
            &config_snapshot,
            resolved.target_spec.subsystem,
            &resolved.target_name,
            "plugin instance",
        )? {
            return Err(s3_error!(InvalidRequest, "{reason}"));
        }

        remove_plugin_instance_config(context, &resolved).await?;

        Ok(owned_build_json_response(
            StatusCode::OK,
            Body::empty(),
            req.headers().get("x-request-id"),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        PluginContractDomain, PluginInstanceBody, PluginInstanceFilters, collect_diagnostic_counts, collect_instance_diagnostics,
        extract_plugin_instance_filters, filter_plugin_instances, map_instance, paginate_plugin_instances, parse_bool_filter,
        parse_instance_status, parse_limit_filter, parse_plugin_contract_domain, parse_plugin_instance_diagnostic_code,
        parse_plugin_instance_id, parse_plugin_instance_source, resolve_plugin_instance_target,
    };
    use crate::admin::handlers::target_descriptor::{
        RuntimeHealthStatus, TargetEndpointSource, TargetInstanceReadModel, canonical_target_instance_id,
        collect_target_instances,
    };
    use crate::admin::operation::AdminRequest;
    use crate::admin::plugin_contract::{
        PluginInstanceDiagnosticCode, PluginInstanceDiagnosticCount, PluginInstanceEntry, PluginInstanceSource,
    };
    use http::{Extensions, HeaderMap, Uri};
    use hyper::Method;
    use rustfs_config::audit::AUDIT_WEBHOOK_SUB_SYS;
    use rustfs_config::notify::NOTIFY_ROUTE_PREFIX;
    use rustfs_config::notify::NOTIFY_WEBHOOK_SUB_SYS;
    use rustfs_config::server_config::{Config, KV, KVS};
    use rustfs_config::{ENABLE_KEY, WEBHOOK_AUTH_TOKEN, WEBHOOK_ENDPOINT};
    use rustfs_s3_types::Body;
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

        let read_auth_block = extract_block_between_markers(
            src,
            "async fn authorize_plugin_instance_request",
            "async fn authorize_plugin_instance_write_request",
        );
        assert!(
            read_auth_block.contains("AdminAction::GetBucketTargetAction"),
            "plugin instance read routes should require GetBucketTargetAction"
        );
    }

    /// Both instance gates authorize through the shared admin gate, which reports
    /// "get cred failed" for a credential-less request. The pre-check keeps the
    /// message these endpoints have always returned (rustfs/backlog#1829).
    #[tokio::test]
    async fn plugin_instance_gates_keep_their_missing_credentials_message() {
        let credential_less_request = || {
            crate::admin::operation::test_support::TestRequest::new(Method::GET, "/rustfs/admin/v4/plugins/instances")
                .headers(HeaderMap::new())
                .extensions(Extensions::new())
                .body(Body::from(String::new()))
                .build()
        };

        for err in [
            super::authorize_plugin_instance_request(&credential_less_request())
                .await
                .expect_err("a request without credentials must be rejected"),
            super::authorize_plugin_instance_write_request(&credential_less_request())
                .await
                .expect_err("a request without credentials must be rejected"),
        ] {
            assert_eq!(err.code(), &rustfs_s3_types::S3ErrorCode::InvalidRequest);
            assert_eq!(err.message(), Some("authentication required"));
        }
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
            collect_target_instances(super::notification_target_specs(), NOTIFY_ROUTE_PREFIX, &config, HashMap::new())
                .expect("collect target instances");
        let primary = instances
            .into_iter()
            .find(|instance| instance.account_id == "primary" && instance.service == "webhook")
            .expect("configured instance should be present");

        assert_eq!(primary.status, "offline");
        assert_eq!(primary.health_state, "offline");
        assert_eq!(primary.health_reason, "not_loaded_in_runtime");
        assert_eq!(primary.source, TargetEndpointSource::Config);
    }

    #[test]
    fn disabled_instance_without_runtime_reports_disabled_health() {
        let config = Config(HashMap::from([(
            NOTIFY_WEBHOOK_SUB_SYS.to_string(),
            HashMap::from([(
                "primary".to_string(),
                KVS(vec![
                    KV {
                        key: ENABLE_KEY.to_string(),
                        value: "off".to_string(),
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
            collect_target_instances(super::notification_target_specs(), NOTIFY_ROUTE_PREFIX, &config, HashMap::new())
                .expect("collect target instances");
        let primary = instances
            .into_iter()
            .find(|instance| instance.account_id == "primary" && instance.service == "webhook")
            .expect("disabled instance should be present");

        assert_eq!(primary.status, "offline");
        assert_eq!(primary.health_state, "disabled");
        assert_eq!(primary.health_reason, "disabled");
        assert!(!primary.runtime_present);
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
                )
                .expect("collect target instances");
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
        let runtime_statuses = HashMap::from([(
            ("runtime-only".to_string(), "webhook".to_string()),
            RuntimeHealthStatus {
                status: "online".to_string(),
                state: "online".to_string(),
                reason: "reachable".to_string(),
            },
        )]);
        let instances = collect_target_instances(
            super::notification_target_specs(),
            NOTIFY_ROUTE_PREFIX,
            &Config(HashMap::new()),
            runtime_statuses,
        )
        .expect("collect target instances");

        let runtime_only = instances
            .into_iter()
            .find(|instance| instance.account_id == "runtime-only")
            .expect("runtime-only instance should be present");

        assert_eq!(runtime_only.source, TargetEndpointSource::Runtime);
        assert_eq!(runtime_only.status, "online");
        assert_eq!(runtime_only.health_state, "online");
        assert_eq!(runtime_only.health_reason, "reachable");
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
            health_state: "offline".to_string(),
            health_reason: "not_loaded_in_runtime".to_string(),
            runtime_present: false,
            has_env_default: false,
            source: TargetEndpointSource::Config,
            enabled: true,
            config: enabled_kvs("on"),
        };

        let mapped = map_instance(instance.clone());
        assert_eq!(mapped.id, instance.canonical_id);
        assert_eq!(mapped.domain, PluginContractDomain::Audit);
        assert!(mapped.diagnostic_codes.is_empty());
    }

    #[test]
    fn map_instance_redacts_secret_config_fields() {
        let instance = TargetInstanceReadModel {
            canonical_id: canonical_target_instance_id("builtin:webhook", TargetDomain::Notify, "primary"),
            plugin_id: "builtin:webhook".to_string(),
            domain: TargetDomain::Notify,
            subsystem: NOTIFY_WEBHOOK_SUB_SYS.to_string(),
            account_id: "primary".to_string(),
            service: "webhook".to_string(),
            status: "online".to_string(),
            health_state: "online".to_string(),
            health_reason: "reachable".to_string(),
            runtime_present: true,
            has_env_default: false,
            source: TargetEndpointSource::Config,
            enabled: true,
            config: KVS(vec![
                KV {
                    key: WEBHOOK_ENDPOINT.to_string(),
                    value: "https://example.com/webhook".to_string(),
                    hidden_if_empty: false,
                },
                KV {
                    key: WEBHOOK_AUTH_TOKEN.to_string(),
                    value: "super-secret-token".to_string(),
                    hidden_if_empty: false,
                },
            ]),
        };

        let mapped = map_instance(instance);
        assert_eq!(
            mapped.config.get(WEBHOOK_ENDPOINT).map(String::as_str),
            Some("https://example.com/webhook")
        );
        assert_eq!(
            mapped.config.get(WEBHOOK_AUTH_TOKEN).map(String::as_str),
            Some(super::REDACTED_SECRET_VALUE)
        );
    }

    #[test]
    fn put_round_trip_restores_stored_secret_for_redacted_placeholder() {
        let mut key_values = vec![
            super::KeyValue {
                key: WEBHOOK_ENDPOINT.to_string(),
                value: "https://example.com/webhook".to_string(),
            },
            super::KeyValue {
                key: WEBHOOK_AUTH_TOKEN.to_string(),
                value: super::REDACTED_SECRET_VALUE.to_string(),
            },
        ];
        let mut stored = KVS::new();
        stored.insert(WEBHOOK_AUTH_TOKEN.to_string(), "stored-secret-token".to_string());

        super::restore_redacted_secret_values(&mut key_values, &[WEBHOOK_AUTH_TOKEN], Some(&stored))
            .expect("redacted secret should be restored from stored config");

        assert_eq!(key_values[0].value, "https://example.com/webhook");
        assert_eq!(key_values[1].value, "stored-secret-token");
    }

    #[test]
    fn put_round_trip_rejects_redacted_placeholder_without_stored_secret() {
        let mut key_values = vec![super::KeyValue {
            key: WEBHOOK_AUTH_TOKEN.to_string(),
            value: super::REDACTED_SECRET_VALUE.to_string(),
        }];

        let err = super::restore_redacted_secret_values(&mut key_values, &[WEBHOOK_AUTH_TOKEN], None)
            .expect_err("placeholder without a stored secret must be rejected");
        assert!(err.to_string().contains("no stored secret"));

        let empty_stored = KVS::new();
        let err = super::restore_redacted_secret_values(&mut key_values, &[WEBHOOK_AUTH_TOKEN], Some(&empty_stored))
            .expect_err("placeholder with an empty stored secret must be rejected");
        assert!(err.to_string().contains("no stored secret"));
    }

    #[test]
    fn put_round_trip_leaves_placeholder_on_non_secret_fields_untouched() {
        let mut key_values = vec![super::KeyValue {
            key: WEBHOOK_ENDPOINT.to_string(),
            value: super::REDACTED_SECRET_VALUE.to_string(),
        }];

        super::restore_redacted_secret_values(&mut key_values, &[WEBHOOK_AUTH_TOKEN], None)
            .expect("non-secret fields are not subject to placeholder restoration");
        assert_eq!(key_values[0].value, super::REDACTED_SECRET_VALUE);
    }

    #[test]
    fn stored_instance_config_lookup_is_case_insensitive_on_target_name() {
        let mut stored = KVS::new();
        stored.insert(WEBHOOK_AUTH_TOKEN.to_string(), "stored-secret-token".to_string());
        let config = Config(HashMap::from([(
            NOTIFY_WEBHOOK_SUB_SYS.to_string(),
            HashMap::from([("Primary".to_string(), stored)]),
        )]));

        let found = super::stored_instance_config(&config, NOTIFY_WEBHOOK_SUB_SYS, "primary")
            .expect("case-insensitive lookup should find the stored section");
        assert_eq!(found.lookup(WEBHOOK_AUTH_TOKEN).as_deref(), Some("stored-secret-token"));
    }

    #[test]
    fn expected_config_round_trips_redacted_read_and_rejects_stale_values() {
        let mut target = KVS::new();
        target.insert(ENABLE_KEY.into(), "on".into());
        target.insert(WEBHOOK_ENDPOINT.into(), "https://example.com/a".into());
        target.insert(WEBHOOK_AUTH_TOKEN.into(), "secret-one".into());
        let mut config = Config(HashMap::from([(
            NOTIFY_WEBHOOK_SUB_SYS.into(),
            HashMap::from([("primary".into(), target)]),
        )]));
        let context = super::plugin_instance_domain_context(PluginContractDomain::Notify);
        let id = canonical_target_instance_id("builtin:webhook", TargetDomain::Notify, "primary");
        let instance = super::find_target_instance(context.specs, context.route_prefix, &config, HashMap::new(), &id)
            .expect("find instance")
            .expect("configured target");
        let expected = super::map_instance(instance).config;
        assert_eq!(expected.get(WEBHOOK_AUTH_TOKEN).map(String::as_str), Some(super::REDACTED_SECRET_VALUE));
        super::validate_expected_config(context, &config, &id, &expected).expect("GET config should be accepted");
        config
            .0
            .get_mut(NOTIFY_WEBHOOK_SUB_SYS)
            .expect("subsystem")
            .get_mut("primary")
            .expect("target")
            .insert(WEBHOOK_AUTH_TOKEN.into(), "secret-two".into());
        super::validate_expected_config(context, &config, &id, &expected).expect("rotation remains redacted");
        config
            .0
            .get_mut(NOTIFY_WEBHOOK_SUB_SYS)
            .expect("subsystem")
            .get_mut("primary")
            .expect("target")
            .insert(WEBHOOK_ENDPOINT.into(), "https://example.com/b".into());
        assert!(super::validate_expected_config(context, &config, &id, &expected).is_err());
        config
            .0
            .get_mut(NOTIFY_WEBHOOK_SUB_SYS)
            .expect("subsystem")
            .get_mut("primary")
            .expect("target")
            .insert(ENABLE_KEY.into(), "off".into());
        let disabled = super::find_target_instance(context.specs, context.route_prefix, &config, HashMap::new(), &id)
            .expect("find disabled instance")
            .expect("disabled target exists");
        let disabled_expected = super::map_instance(disabled).config;
        let error = super::validate_expected_config(context, &config, &id, &disabled_expected)
            .expect_err("disabled target must not be enabled by editing");
        assert!(error.to_string().contains("disabled targets"));
        config.0.get_mut(NOTIFY_WEBHOOK_SUB_SYS).expect("subsystem").remove("primary");
        assert!(super::validate_expected_config(context, &config, &id, &expected).is_err());
        let legacy: PluginInstanceBody = serde_json::from_str(r#"{"key_values":[]}"#).expect("legacy input");
        assert!(legacy.expected_config.is_none());
    }

    #[test]
    #[serial_test::serial]
    fn conditional_edit_rejects_inherited_environment_defaults() {
        temp_env::with_vars(
            [(rustfs_config::notify::ENV_NOTIFY_WEBHOOK_ENDPOINT, Some("https://example.com/hook"))],
            || {
                let context = super::plugin_instance_domain_context(PluginContractDomain::Notify);
                let mut config = Config::new();
                let mut target = KVS::new();
                target.insert(ENABLE_KEY.into(), "on".into());
                config
                    .0
                    .entry(NOTIFY_WEBHOOK_SUB_SYS.into())
                    .or_default()
                    .insert("Primary".into(), target);
                let id = canonical_target_instance_id("builtin:webhook", TargetDomain::Notify, "primary");
                let error = super::validate_expected_config(context, &config, &id, &HashMap::new())
                    .expect_err("inherited environment defaults must prevent edits");
                assert!(error.to_string().contains("inherits environment configuration"));
            },
        );
    }

    #[test]
    fn conditional_edit_keeps_inherited_fields_in_default_layer() {
        let mut original = KVS::new();
        original.insert("endpoint".into(), "https://example.com/old".into());
        let expected = HashMap::from([
            ("endpoint".into(), "https://example.com/old".into()),
            ("auth_token".into(), super::REDACTED_SECRET_VALUE.into()),
            ("queue_limit".into(), "10".into()),
        ]);
        let mut validated = KVS::new();
        validated.insert("endpoint".into(), "https://example.com/new".into());
        validated.insert("auth_token".into(), "default-secret".into());
        validated.insert("queue_limit".into(), "10".into());
        let result = super::merge_conditional_target_update(&original, &expected, &["endpoint".into()], &validated)
            .expect("merge edited endpoint");
        assert_eq!(result.lookup("endpoint").as_deref(), Some("https://example.com/new"));
        assert_eq!(result.lookup("auth_token"), None);
        assert_eq!(result.lookup("queue_limit"), None);
        for value in ["replacement-secret", ""] {
            validated.insert("auth_token".into(), value.into());
            let changed = super::merge_conditional_target_update(&original, &expected, &["auth_token".into()], &validated)
                .expect("merge explicit secret change");
            assert_eq!(changed.lookup("auth_token").as_deref(), Some(value));
            assert_eq!(changed.lookup("queue_limit"), None);
        }
    }

    #[tokio::test]
    async fn partial_nats_update_validates_merged_authentication() {
        use rustfs_config::{NATS_ADDRESS, NATS_PASSWORD, NATS_SUBJECT, NATS_TOKEN, NATS_USERNAME};
        let context = super::plugin_instance_domain_context(PluginContractDomain::Notify);
        let subsystem = rustfs_config::notify::NOTIFY_NATS_SUB_SYS;
        let mut effective = KVS::new();
        effective.insert(NATS_ADDRESS.into(), "nats://127.0.0.1:4222".into());
        effective.insert(NATS_SUBJECT.into(), "events".into());
        effective.insert(NATS_USERNAME.into(), "user".into());
        effective.insert(NATS_PASSWORD.into(), "password".into());
        for mut requested in [
            vec![super::KeyValue {
                key: NATS_TOKEN.into(),
                value: "new-token".into(),
            }],
            vec![
                super::KeyValue {
                    key: NATS_ADDRESS.into(),
                    value: "nats://127.0.0.1:4222".into(),
                },
                super::KeyValue {
                    key: NATS_SUBJECT.into(),
                    value: "events".into(),
                },
                super::KeyValue {
                    key: NATS_USERNAME.into(),
                    value: "user".into(),
                },
                super::KeyValue {
                    key: NATS_PASSWORD.into(),
                    value: super::REDACTED_SECRET_VALUE.into(),
                },
                super::KeyValue {
                    key: NATS_TOKEN.into(),
                    value: "new-token".into(),
                },
            ],
        ] {
            super::restore_redacted_secret_values(&mut requested, &[NATS_PASSWORD, NATS_TOKEN], Some(&effective))
                .expect("restore placeholders before validation");
            let candidate =
                super::effective_validation_fields(context, subsystem, &effective, &requested).expect("merge request");
            assert_eq!(candidate.get(NATS_PASSWORD).map(String::as_str), Some("password"));
            let spec = context
                .specs
                .iter()
                .find(|spec| spec.subsystem == subsystem)
                .expect("NATS descriptor");
            let error = super::build_validated_target_kvs(spec, candidate, context.default_queue_dir)
                .await
                .expect_err("mixed NATS authentication must fail before connection");
            assert!(error.to_string().contains("only one auth method"));
        }
    }

    #[test]
    fn conditional_update_rejects_unknown_and_duplicate_keys() {
        let context = super::plugin_instance_domain_context(PluginContractDomain::Notify);
        let subsystem = rustfs_config::notify::NOTIFY_NATS_SUB_SYS;
        let unknown = [super::KeyValue {
            key: "unknown".into(),
            value: "value".into(),
        }];
        let duplicate = [
            super::KeyValue {
                key: rustfs_config::NATS_SUBJECT.into(),
                value: "one".into(),
            },
            super::KeyValue {
                key: rustfs_config::NATS_SUBJECT.into(),
                value: "two".into(),
            },
        ];
        for request in [&unknown[..], &duplicate[..]] {
            assert!(super::effective_validation_fields(context, subsystem, &KVS::new(), request).is_err());
        }
    }

    #[test]
    fn config_revision_is_scoped_to_instance_and_generation() {
        let revision = super::config_revision("builtin:webhook:notify:one", "00000000-0000-4000-8000-000000000001");
        assert_eq!(revision.len(), 64);
        assert_ne!(
            revision,
            super::config_revision("builtin:webhook:notify:two", "00000000-0000-4000-8000-000000000001")
        );
        assert_ne!(
            revision,
            super::config_revision("builtin:webhook:notify:one", "00000000-0000-4000-8000-000000000002")
        );
    }

    #[test]
    fn plugin_instance_body_rejects_unknown_fields() {
        let err = serde_json::from_str::<PluginInstanceBody>(r#"{"key_values":[],"unexpected_field":true}"#)
            .expect_err("unknown plugin instance body field should fail");
        assert!(err.to_string().contains("unknown field"));

        let err = serde_json::from_str::<PluginInstanceBody>(
            r#"{"key_values":[{"key":"endpoint","value":"http://example.com","extra":true}]}"#,
        )
        .expect_err("unknown plugin instance key-value field should fail");
        assert!(err.to_string().contains("unknown field"));
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
            "/rustfs/admin/v4/plugins/instances?domain=notify&service=webhook&status=offline&source=env&diagnostic_code=not_loaded_in_runtime&enabled=true&q=Primary&limit=25&marker=builtin:webhook:notify:seed",
        );

        let filters = extract_plugin_instance_filters(req.uri()).expect("query should parse");
        assert_eq!(
            filters,
            PluginInstanceFilters {
                domain: Some(PluginContractDomain::Notify),
                service: Some("webhook".to_string()),
                status: Some("offline".to_string()),
                source: Some(PluginInstanceSource::Env),
                diagnostic_code: Some(PluginInstanceDiagnosticCode::NotLoadedInRuntime),
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

        let err = parse_plugin_instance_diagnostic_code("mystery").expect_err("invalid diagnostic code should fail");
        assert!(err.to_string().contains("invalid plugin instance diagnostic_code filter"));

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
                diagnostic_code: None,
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
    fn filter_plugin_instances_can_match_diagnostic_code_summary() {
        let mut matched = sample_instance(SampleInstance {
            id: "builtin:webhook:notify:primary",
            plugin_id: "builtin:webhook",
            domain: PluginContractDomain::Notify,
            subsystem: "notify_webhook",
            account_id: "primary",
            service: "webhook",
            status: "offline",
            source: PluginInstanceSource::Config,
            enabled: true,
        });
        matched.diagnostic_codes = vec![PluginInstanceDiagnosticCode::NotLoadedInRuntime];

        let mut other = sample_instance(SampleInstance {
            id: "builtin:kafka:notify:secondary",
            plugin_id: "builtin:kafka",
            domain: PluginContractDomain::Notify,
            subsystem: "notify_kafka",
            account_id: "secondary",
            service: "kafka",
            status: "offline",
            source: PluginInstanceSource::Runtime,
            enabled: true,
        });
        other.diagnostic_codes = vec![PluginInstanceDiagnosticCode::RuntimeOffline];

        let filtered = filter_plugin_instances(
            vec![matched.clone(), other],
            &PluginInstanceFilters {
                diagnostic_code: Some(PluginInstanceDiagnosticCode::NotLoadedInRuntime),
                ..PluginInstanceFilters::default()
            },
        );

        assert_eq!(filtered, vec![matched]);
    }

    #[test]
    fn collect_diagnostic_counts_aggregates_filtered_instance_summaries() {
        let mut first = sample_instance(SampleInstance {
            id: "builtin:webhook:notify:primary",
            plugin_id: "builtin:webhook",
            domain: PluginContractDomain::Notify,
            subsystem: "notify_webhook",
            account_id: "primary",
            service: "webhook",
            status: "offline",
            source: PluginInstanceSource::Config,
            enabled: true,
        });
        first.diagnostic_codes = vec![
            PluginInstanceDiagnosticCode::ModuleDisabled,
            PluginInstanceDiagnosticCode::NotLoadedInRuntime,
        ];

        let mut second = sample_instance(SampleInstance {
            id: "builtin:kafka:notify:secondary",
            plugin_id: "builtin:kafka",
            domain: PluginContractDomain::Notify,
            subsystem: "notify_kafka",
            account_id: "secondary",
            service: "kafka",
            status: "offline",
            source: PluginInstanceSource::Runtime,
            enabled: true,
        });
        second.diagnostic_codes = vec![PluginInstanceDiagnosticCode::RuntimeOffline];

        let counts = collect_diagnostic_counts(&[first, second]);
        assert_eq!(
            counts,
            vec![
                PluginInstanceDiagnosticCount {
                    code: PluginInstanceDiagnosticCode::ModuleDisabled,
                    count: 1,
                },
                PluginInstanceDiagnosticCount {
                    code: PluginInstanceDiagnosticCode::NotLoadedInRuntime,
                    count: 1,
                },
                PluginInstanceDiagnosticCount {
                    code: PluginInstanceDiagnosticCode::RuntimeOffline,
                    count: 1,
                },
            ]
        );
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
    fn diagnostics_include_not_loaded_in_runtime_for_enabled_config_instance() {
        let instance = TargetInstanceReadModel {
            canonical_id: "builtin:webhook:notify:primary".to_string(),
            plugin_id: "builtin:webhook".to_string(),
            domain: TargetDomain::Notify,
            subsystem: NOTIFY_WEBHOOK_SUB_SYS.to_string(),
            account_id: "primary".to_string(),
            service: "webhook".to_string(),
            status: "offline".to_string(),
            health_state: "offline".to_string(),
            health_reason: "not_loaded_in_runtime".to_string(),
            runtime_present: false,
            has_env_default: false,
            source: TargetEndpointSource::Config,
            enabled: true,
            config: enabled_kvs("on"),
        };

        let diagnostics = collect_instance_diagnostics(&instance, None);
        assert!(
            diagnostics
                .iter()
                .any(|item| item.code == PluginInstanceDiagnosticCode::NotLoadedInRuntime)
        );
    }

    #[test]
    fn diagnostics_include_runtime_offline_when_runtime_presence_is_known() {
        let instance = TargetInstanceReadModel {
            canonical_id: "builtin:webhook:notify:primary".to_string(),
            plugin_id: "builtin:webhook".to_string(),
            domain: TargetDomain::Notify,
            subsystem: NOTIFY_WEBHOOK_SUB_SYS.to_string(),
            account_id: "primary".to_string(),
            service: "webhook".to_string(),
            status: "offline".to_string(),
            health_state: "offline".to_string(),
            health_reason: "unreachable".to_string(),
            runtime_present: true,
            has_env_default: false,
            source: TargetEndpointSource::Runtime,
            enabled: true,
            config: KVS::new(),
        };

        let diagnostics = collect_instance_diagnostics(&instance, None);
        assert!(
            diagnostics
                .iter()
                .any(|item| item.code == PluginInstanceDiagnosticCode::RuntimeOffline)
        );
    }

    #[test]
    fn diagnostics_include_source_and_module_reasons_without_guessing() {
        let instance = TargetInstanceReadModel {
            canonical_id: "builtin:webhook:audit:primary".to_string(),
            plugin_id: "builtin:webhook".to_string(),
            domain: TargetDomain::Audit,
            subsystem: AUDIT_WEBHOOK_SUB_SYS.to_string(),
            account_id: "primary".to_string(),
            service: "webhook".to_string(),
            status: "offline".to_string(),
            health_state: "disabled".to_string(),
            health_reason: "disabled".to_string(),
            runtime_present: false,
            has_env_default: false,
            source: TargetEndpointSource::Mixed,
            enabled: false,
            config: enabled_kvs("off"),
        };

        let diagnostics =
            collect_instance_diagnostics(&instance, Some("audit module is disabled; enable the audit module first".to_string()));

        assert!(
            diagnostics
                .iter()
                .any(|item| item.code == PluginInstanceDiagnosticCode::ModuleDisabled)
        );
        assert!(
            diagnostics
                .iter()
                .any(|item| item.code == PluginInstanceDiagnosticCode::InstanceDisabled)
        );
        assert!(
            diagnostics
                .iter()
                .any(|item| item.code == PluginInstanceDiagnosticCode::MixedSource)
        );
        assert!(
            !diagnostics
                .iter()
                .any(|item| item.code == PluginInstanceDiagnosticCode::NotLoadedInRuntime)
        );
    }

    #[tokio::test]
    async fn list_entry_exposes_diagnostic_code_summary() {
        let instance = TargetInstanceReadModel {
            canonical_id: "builtin:webhook:notify:primary".to_string(),
            plugin_id: "builtin:webhook".to_string(),
            domain: TargetDomain::Notify,
            subsystem: NOTIFY_WEBHOOK_SUB_SYS.to_string(),
            account_id: "primary".to_string(),
            service: "webhook".to_string(),
            status: "offline".to_string(),
            health_state: "offline".to_string(),
            health_reason: "not_loaded_in_runtime".to_string(),
            runtime_present: false,
            has_env_default: false,
            source: TargetEndpointSource::Config,
            enabled: true,
            config: enabled_kvs("on"),
        };

        let entry = super::plugin_instance_list_entry(instance, None);
        assert!(
            entry
                .diagnostic_codes
                .contains(&PluginInstanceDiagnosticCode::NotLoadedInRuntime),
            "list entry should include the offline diagnostic summary"
        );
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
            health_state: input.status.to_string(),
            health_reason: if input.status == "online" {
                "reachable"
            } else {
                "unreachable"
            }
            .to_string(),
            source: input.source,
            enabled: input.enabled,
            config: HashMap::new(),
            operational_state: None,
            diagnostic_codes: Vec::new(),
        }
    }

    fn build_plugin_instances_request(uri: &'static str) -> AdminRequest {
        crate::admin::operation::test_support::TestRequest::new(Method::GET, Uri::from_static(uri))
            .headers(HeaderMap::new())
            .extensions(Extensions::new())
            .body(Body::empty())
            .build()
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
