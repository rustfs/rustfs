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

use crate::admin::auth::validate_admin_request;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::service::config::{
    apply_dynamic_config_for_subsystem, is_dynamic_config_subsystem, signal_dynamic_config_reload,
};
use crate::admin::utils::{encode_compatible_admin_payload, is_compat_admin_request, read_compatible_admin_body};
use crate::auth::{check_key_valid, get_session_token};
use crate::error::ApiError;
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use http::{HeaderMap, HeaderValue, Uri};
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_config::{COMMENT_KEY, DEFAULT_DELIMITER, MAX_ADMIN_REQUEST_BODY_SIZE};
use rustfs_credentials::Credentials;
use rustfs_ecstore::config::com::{delete_config, read_config, read_config_without_migrate, save_config, save_server_config};
use rustfs_ecstore::config::{Config as ServerConfig, KV, KVS, get_global_server_config};
use rustfs_ecstore::disk::RUSTFS_META_BUCKET;
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::store_api::ListOperations;
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use serde::Serialize;
use std::collections::{BTreeSet, HashMap};
use time::OffsetDateTime;
use uuid::Uuid;

const REDACTED_VALUE: &str = "*redacted*";
const OCTET_STREAM_CONTENT_TYPE: &str = "application/octet-stream";
const JSON_CONTENT_TYPE: &str = "application/json";
const TEXT_CONTENT_TYPE: &str = "text/plain; charset=utf-8";
const CONFIG_HISTORY_PREFIX: &str = "config/history";
const CONFIG_HISTORY_SUFFIX: &str = ".kv";
const CONFIG_APPLIED_HEADER: &str = "x-rustfs-config-applied";
const CONFIG_APPLIED_COMPAT_HEADER: &str = "x-minio-config-applied";
const CONFIG_APPLIED_TRUE: &str = "true";

#[derive(Debug, Clone, PartialEq, Eq)]
struct ConfigEntry {
    key: String,
    value: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ConfigDirective {
    sub_system: String,
    target: String,
    entries: Vec<ConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ConfigSelector {
    sub_system: String,
    target: Option<String>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct ConfigHelpResponse {
    #[serde(rename = "subSystems")]
    sub_systems: Vec<String>,
    #[serde(rename = "subSys", skip_serializing_if = "Option::is_none")]
    sub_sys: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    key: Option<String>,
    #[serde(rename = "envOnly")]
    env_only: bool,
    entries: Vec<ConfigHelpEntry>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct ConfigHelpEntry {
    key: String,
    sensitive: bool,
    configured: bool,
    value: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct ConfigHistoryEntry {
    #[serde(rename = "RestoreID")]
    restore_id: String,
    #[serde(rename = "CreateTime", with = "time::serde::rfc3339")]
    create_time: OffsetDateTime,
    #[serde(rename = "Data", skip_serializing_if = "Option::is_none")]
    data: Option<String>,
}

pub fn register_config_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        format!("{ADMIN_PREFIX}/v3/get-config-kv").as_str(),
        AdminOperation(&GetConfigKVHandler {}),
    )?;
    r.insert(
        Method::PUT,
        format!("{ADMIN_PREFIX}/v3/set-config-kv").as_str(),
        AdminOperation(&SetConfigKVHandler {}),
    )?;
    r.insert(
        Method::DELETE,
        format!("{ADMIN_PREFIX}/v3/del-config-kv").as_str(),
        AdminOperation(&DelConfigKVHandler {}),
    )?;
    r.insert(
        Method::GET,
        format!("{ADMIN_PREFIX}/v3/help-config-kv").as_str(),
        AdminOperation(&HelpConfigKVHandler {}),
    )?;
    r.insert(
        Method::GET,
        format!("{ADMIN_PREFIX}/v3/list-config-history-kv").as_str(),
        AdminOperation(&ListConfigHistoryKVHandler {}),
    )?;
    r.insert(
        Method::DELETE,
        format!("{ADMIN_PREFIX}/v3/clear-config-history-kv").as_str(),
        AdminOperation(&ClearConfigHistoryKVHandler {}),
    )?;
    r.insert(
        Method::PUT,
        format!("{ADMIN_PREFIX}/v3/restore-config-history-kv").as_str(),
        AdminOperation(&RestoreConfigHistoryKVHandler {}),
    )?;
    r.insert(
        Method::GET,
        format!("{ADMIN_PREFIX}/v3/config").as_str(),
        AdminOperation(&GetConfigHandler {}),
    )?;
    r.insert(
        Method::PUT,
        format!("{ADMIN_PREFIX}/v3/config").as_str(),
        AdminOperation(&SetConfigHandler {}),
    )?;

    Ok(())
}

fn extract_query_params(uri: &Uri) -> HashMap<String, String> {
    let mut params = HashMap::new();

    if let Some(query) = uri.query() {
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            params.insert(key.into_owned(), value.into_owned());
        }
    }

    params
}

async fn validate_config_admin_request(req: &S3Request<Body>) -> S3Result<Credentials> {
    let Some(input_cred) = req.credentials.as_ref() else {
        return Err(s3_error!(InvalidRequest, "missing credentials"));
    };

    let (cred, owner) =
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

    let remote_addr = req
        .extensions
        .get::<Option<RemoteAddr>>()
        .and_then(|opt| opt.map(|addr| addr.0));
    validate_admin_request(
        &req.headers,
        &cred,
        owner,
        false,
        vec![Action::AdminAction(AdminAction::ConfigUpdateAdminAction)],
        remote_addr,
    )
    .await?;

    Ok(cred)
}

fn header_value(content_type: &str) -> S3Result<HeaderValue> {
    HeaderValue::from_str(content_type)
        .map_err(|err| S3Error::with_message(S3ErrorCode::InternalError, format!("invalid content type: {err}")))
}

fn response_with_content_type(status: StatusCode, body: Vec<u8>, content_type: &str) -> S3Result<S3Response<(StatusCode, Body)>> {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, header_value(content_type)?);
    Ok(S3Response::with_headers((status, Body::from(body)), headers))
}

fn encode_config_payload(path: &str, secret_key: &str, data: Vec<u8>, plain_content_type: &str) -> S3Result<(Vec<u8>, String)> {
    if is_compat_admin_request(path) {
        let (encoded, _) = encode_compatible_admin_payload(path, secret_key, data)?;
        Ok((encoded, OCTET_STREAM_CONTENT_TYPE.to_string()))
    } else {
        Ok((data, plain_content_type.to_string()))
    }
}

fn success_response(config_applied: bool) -> S3Result<S3Response<(StatusCode, Body)>> {
    if !config_applied {
        return Ok(S3Response::new((StatusCode::OK, Body::default())));
    }

    let mut headers = HeaderMap::new();
    headers.insert(CONFIG_APPLIED_HEADER, header_value(CONFIG_APPLIED_TRUE)?);
    headers.insert(CONFIG_APPLIED_COMPAT_HEADER, header_value(CONFIG_APPLIED_TRUE)?);
    Ok(S3Response::with_headers((StatusCode::OK, Body::default()), headers))
}

fn object_store() -> S3Result<std::sync::Arc<rustfs_ecstore::store::ECStore>> {
    new_object_layer_fn().ok_or_else(|| s3_error!(InternalError, "server storage not initialized"))
}

async fn load_server_config_from_store() -> S3Result<ServerConfig> {
    let store = object_store()?;
    read_config_without_migrate(store)
        .await
        .map_err(ApiError::from)
        .map_err(Into::into)
}

fn load_active_server_config() -> S3Result<ServerConfig> {
    get_global_server_config().ok_or_else(|| s3_error!(InternalError, "server config is not initialized"))
}

async fn save_server_config_to_store(config: &ServerConfig) -> S3Result<()> {
    let store = object_store()?;
    save_server_config(store, config)
        .await
        .map_err(ApiError::from)
        .map_err(Into::into)
}

fn history_object_name(restore_id: &str) -> String {
    format!("{CONFIG_HISTORY_PREFIX}/{restore_id}{CONFIG_HISTORY_SUFFIX}")
}

fn config_update_sub_system(directives: &[ConfigDirective]) -> S3Result<Option<&str>> {
    let sub_systems = directives
        .iter()
        .map(|directive| directive.sub_system.as_str())
        .collect::<BTreeSet<_>>();
    if sub_systems.len() > 1 {
        return Err(s3_error!(InvalidRequest, "config update must target a single subsystem"));
    }
    Ok(sub_systems.iter().next().copied())
}

fn history_restore_id_from_name(name: &str) -> Option<String> {
    name.strip_prefix(&format!("{CONFIG_HISTORY_PREFIX}/"))?
        .strip_suffix(CONFIG_HISTORY_SUFFIX)
        .map(ToString::to_string)
}

fn trim_history_entries(mut entries: Vec<ConfigHistoryEntry>, count: Option<usize>) -> Vec<ConfigHistoryEntry> {
    entries.sort_by(|lhs, rhs| lhs.create_time.cmp(&rhs.create_time));

    if let Some(count) = count
        && entries.len() > count
    {
        entries.drain(0..entries.len() - count);
    }

    entries
}

async fn save_server_config_history(data: &[u8]) -> S3Result<String> {
    let restore_id = Uuid::new_v4().to_string();
    let store = object_store()?;
    save_config(store, &history_object_name(&restore_id), data.to_vec())
        .await
        .map_err(ApiError::from)
        .map_err(S3Error::from)?;
    Ok(restore_id)
}

async fn read_server_config_history(restore_id: &str) -> S3Result<Vec<u8>> {
    let store = object_store()?;
    read_config(store, &history_object_name(restore_id))
        .await
        .map_err(ApiError::from)
        .map_err(Into::into)
}

async fn delete_server_config_history(restore_id: &str) -> S3Result<()> {
    let store = object_store()?;
    delete_config(store, &history_object_name(restore_id))
        .await
        .map_err(ApiError::from)
        .map_err(Into::into)
}

async fn list_server_config_history(with_data: bool, count: Option<usize>) -> S3Result<Vec<ConfigHistoryEntry>> {
    let store = object_store()?;
    let mut continuation_token = None;
    let mut entries = Vec::new();

    loop {
        let page = store
            .clone()
            .list_objects_v2(
                RUSTFS_META_BUCKET,
                CONFIG_HISTORY_PREFIX,
                continuation_token.clone(),
                None,
                1000,
                false,
                None,
                false,
            )
            .await
            .map_err(ApiError::from)
            .map_err(S3Error::from)?;

        for object in page.objects {
            if object.is_dir {
                continue;
            }

            let Some(restore_id) = history_restore_id_from_name(&object.name) else {
                continue;
            };

            let data = if with_data {
                Some(
                    String::from_utf8(
                        read_config(store.clone(), &object.name)
                            .await
                            .map_err(ApiError::from)
                            .map_err(S3Error::from)?,
                    )
                    .map_err(ApiError::other)
                    .map_err(S3Error::from)?,
                )
            } else {
                None
            };

            entries.push(ConfigHistoryEntry {
                restore_id,
                create_time: object.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH),
                data,
            });
        }

        if !page.is_truncated {
            break;
        }

        continuation_token = page.next_continuation_token;
        if continuation_token.is_none() {
            break;
        }
    }

    Ok(trim_history_entries(entries, count))
}

fn normalize_target(target: &str) -> String {
    if target.trim().is_empty() || target == "default" || target == DEFAULT_DELIMITER {
        DEFAULT_DELIMITER.to_string()
    } else {
        target.trim().to_string()
    }
}

fn format_scope(sub_system: &str, target: &str) -> String {
    if target == DEFAULT_DELIMITER {
        sub_system.to_string()
    } else {
        format!("{sub_system}:{target}")
    }
}

fn escape_config_value(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn format_kv_pair(key: &str, value: &str) -> String {
    format!(r#"{key}="{}""#, escape_config_value(value))
}

fn tokenize_config_line(line: &str) -> S3Result<Vec<String>> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    let mut escaped = false;

    for ch in line.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }

        if ch == '\\' {
            escaped = true;
            continue;
        }

        if let Some(active_quote) = quote {
            if ch == active_quote {
                quote = None;
            } else {
                current.push(ch);
            }
            continue;
        }

        match ch {
            '"' | '\'' => quote = Some(ch),
            c if c.is_whitespace() => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
            }
            _ => current.push(ch),
        }
    }

    if escaped {
        current.push('\\');
    }

    if quote.is_some() {
        return Err(s3_error!(InvalidRequest, "unterminated quoted config value"));
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    Ok(tokens)
}

fn parse_directive_scope(scope: &str) -> S3Result<(String, String)> {
    let (sub_system, target) = match scope.split_once(':') {
        Some((sub_system, target)) => (sub_system.trim(), normalize_target(target)),
        None => (scope.trim(), DEFAULT_DELIMITER.to_string()),
    };

    if sub_system.is_empty() {
        return Err(s3_error!(InvalidRequest, "missing config subsystem"));
    }

    Ok((sub_system.to_string(), target))
}

fn parse_config_directives(input: &str, allow_bare_keys: bool) -> S3Result<Vec<ConfigDirective>> {
    let mut directives = Vec::new();

    for raw_line in input.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let tokens = tokenize_config_line(line)?;
        if tokens.is_empty() {
            continue;
        }

        let (sub_system, target) = parse_directive_scope(&tokens[0])?;
        let mut entries = Vec::new();

        for token in tokens.iter().skip(1) {
            if let Some((key, value)) = token.split_once('=') {
                let key = key.trim();
                if key.is_empty() {
                    return Err(s3_error!(InvalidRequest, "config key cannot be empty"));
                }
                entries.push(ConfigEntry {
                    key: key.to_string(),
                    value: Some(value.to_string()),
                });
            } else if allow_bare_keys {
                entries.push(ConfigEntry {
                    key: token.trim().to_string(),
                    value: None,
                });
            } else {
                return Err(s3_error!(InvalidRequest, "config assignment must use key=value syntax"));
            }
        }

        directives.push(ConfigDirective {
            sub_system,
            target,
            entries,
        });
    }

    Ok(directives)
}

fn parse_config_selector(input: &str) -> S3Result<ConfigSelector> {
    let raw = input.trim();
    if raw.is_empty() {
        return Err(s3_error!(InvalidRequest, "missing config key selector"));
    }

    let (sub_system, target) = match raw.split_once(':') {
        Some((sub_system, target)) => (sub_system.trim(), Some(normalize_target(target))),
        None => (raw, None),
    };

    if sub_system.is_empty() {
        return Err(s3_error!(InvalidRequest, "missing config subsystem"));
    }

    Ok(ConfigSelector {
        sub_system: sub_system.to_string(),
        target,
    })
}

fn set_kvs_value(kvs: &mut KVS, key: &str, value: String) {
    if let Some(existing) = kvs.0.iter_mut().find(|entry| entry.key == key) {
        existing.value = value;
        return;
    }

    kvs.0.push(KV {
        key: key.to_string(),
        value,
        hidden_if_empty: false,
    });
}

fn is_sensitive_key_name(key: &str) -> bool {
    let normalized = key.trim().to_ascii_lowercase();
    normalized.contains("secret") || normalized.contains("password") || normalized == "token" || normalized.ends_with("_token")
}

fn apply_set_directives(config: &mut ServerConfig, directives: &[ConfigDirective]) {
    for directive in directives {
        let targets = config.0.entry(directive.sub_system.clone()).or_default();
        let kvs = targets.entry(directive.target.clone()).or_default();

        for entry in &directive.entries {
            let value = entry.value.clone().unwrap_or_default();
            set_kvs_value(kvs, &entry.key, value);
        }
    }

    config.set_defaults();
}

fn apply_delete_directives(config: &mut ServerConfig, directives: &[ConfigDirective]) {
    for directive in directives {
        let mut remove_subsystem = false;

        if let Some(targets) = config.0.get_mut(&directive.sub_system) {
            if directive.entries.is_empty() {
                targets.remove(&directive.target);
            } else if let Some(kvs) = targets.get_mut(&directive.target) {
                let keys = directive
                    .entries
                    .iter()
                    .map(|entry| entry.key.as_str())
                    .collect::<BTreeSet<_>>();
                kvs.0.retain(|entry| !keys.contains(entry.key.as_str()));
                if kvs.0.is_empty() {
                    targets.remove(&directive.target);
                }
            }

            remove_subsystem = targets.is_empty();
        }

        if remove_subsystem {
            config.0.remove(&directive.sub_system);
        }
    }

    config.set_defaults();
}

fn render_entry_value(entry: &KV, redact_secrets: bool) -> String {
    if redact_secrets && (entry.hidden_if_empty || is_sensitive_key_name(&entry.key)) && !entry.value.trim().is_empty() {
        REDACTED_VALUE.to_string()
    } else {
        entry.value.clone()
    }
}

fn sorted_kv_entries(kvs: &KVS) -> Vec<&KV> {
    let mut entries = kvs.0.iter().filter(|entry| entry.key != COMMENT_KEY).collect::<Vec<_>>();
    entries.sort_by(|lhs, rhs| lhs.key.cmp(&rhs.key));
    entries
}

fn render_scope_line(sub_system: &str, target: &str, kvs: &KVS, redact_secrets: bool) -> Option<String> {
    let entries = sorted_kv_entries(kvs);
    if entries.is_empty() {
        return None;
    }

    let pairs = entries
        .into_iter()
        .map(|entry| format_kv_pair(&entry.key, &render_entry_value(entry, redact_secrets)))
        .collect::<Vec<_>>();

    Some(format!("{} {}", format_scope(sub_system, target), pairs.join(" ")))
}

fn render_selected_config(config: &ServerConfig, selector: &ConfigSelector, redact_secrets: bool) -> S3Result<Vec<u8>> {
    let Some(targets) = config.0.get(&selector.sub_system) else {
        return Err(s3_error!(InvalidRequest, "config subsystem '{}' not found", selector.sub_system));
    };

    let mut lines = Vec::new();
    let mut sorted_targets = targets.iter().collect::<Vec<_>>();
    sorted_targets.sort_by(|(lhs, _), (rhs, _)| lhs.cmp(rhs));

    if let Some(target) = selector.target.as_ref() {
        let Some(kvs) = targets.get(target) else {
            return Err(s3_error!(
                InvalidRequest,
                "config target '{}' not found for subsystem '{}'",
                target,
                selector.sub_system
            ));
        };

        if let Some(line) = render_scope_line(&selector.sub_system, target, kvs, redact_secrets) {
            lines.push(line);
        }
    } else {
        for (target, kvs) in sorted_targets {
            if let Some(line) = render_scope_line(&selector.sub_system, target, kvs, redact_secrets) {
                lines.push(line);
            }
        }
    }

    Ok(lines.join("\n").into_bytes())
}

fn render_full_config(config: &ServerConfig) -> Vec<u8> {
    let mut subsystems = config.0.iter().collect::<Vec<_>>();
    subsystems.sort_by(|(lhs, _), (rhs, _)| lhs.cmp(rhs));

    let mut lines = Vec::new();
    for (sub_system, targets) in subsystems {
        let mut sorted_targets = targets.iter().collect::<Vec<_>>();
        sorted_targets.sort_by(|(lhs, _), (rhs, _)| lhs.cmp(rhs));

        for (target, kvs) in sorted_targets {
            if let Some(line) = render_scope_line(sub_system, target, kvs, false) {
                lines.push(line);
            }
        }
    }

    lines.join("\n").into_bytes()
}

fn collect_help_entries(config: &ServerConfig, sub_system: &str, key_filter: Option<&str>) -> S3Result<Vec<ConfigHelpEntry>> {
    let defaults = ServerConfig::new();
    let default_kvs = defaults
        .0
        .get(sub_system)
        .and_then(|targets| targets.get(DEFAULT_DELIMITER))
        .cloned()
        .unwrap_or_default();
    let configured_kvs = config
        .0
        .get(sub_system)
        .and_then(|targets| targets.get(DEFAULT_DELIMITER))
        .cloned()
        .unwrap_or_default();

    let mut keys = default_kvs
        .0
        .iter()
        .chain(configured_kvs.0.iter())
        .filter(|entry| entry.key != COMMENT_KEY)
        .map(|entry| entry.key.clone())
        .collect::<BTreeSet<_>>();

    if let Some(key_filter) = key_filter {
        keys.retain(|key| key == key_filter);
    }

    if keys.is_empty() {
        return Err(s3_error!(InvalidRequest, "config subsystem '{}' has no help entries", sub_system));
    }

    let entries = keys
        .into_iter()
        .map(|key| {
            let default_entry = default_kvs.0.iter().find(|entry| entry.key == key);
            let configured_entry = configured_kvs.0.iter().find(|entry| entry.key == key);
            let sensitive =
                configured_entry.or(default_entry).is_some_and(|entry| entry.hidden_if_empty) || is_sensitive_key_name(&key);
            let configured = configured_entry.is_some_and(|entry| !entry.value.trim().is_empty());
            let value = configured_entry
                .map(|entry| render_entry_value(entry, true))
                .or_else(|| default_entry.map(|entry| render_entry_value(entry, true)))
                .unwrap_or_default();

            ConfigHelpEntry {
                key,
                sensitive,
                configured,
                value,
            }
        })
        .collect();

    Ok(entries)
}

fn build_help_response(
    config: &ServerConfig,
    sub_system: Option<&str>,
    key: Option<&str>,
    env_only: bool,
) -> S3Result<ConfigHelpResponse> {
    let defaults = ServerConfig::new();
    let mut sub_systems = defaults
        .0
        .keys()
        .chain(config.0.keys())
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    sub_systems.sort();

    if let Some(sub_system) = sub_system.filter(|value| !value.trim().is_empty()) {
        let entries = collect_help_entries(config, sub_system, key.filter(|value| !value.trim().is_empty()))?;
        return Ok(ConfigHelpResponse {
            sub_systems,
            sub_sys: Some(sub_system.to_string()),
            key: key.filter(|value| !value.trim().is_empty()).map(ToString::to_string),
            env_only,
            entries,
        });
    }

    Ok(ConfigHelpResponse {
        sub_systems,
        sub_sys: None,
        key: None,
        env_only,
        entries: Vec::new(),
    })
}

pub struct GetConfigKVHandler {}

#[async_trait::async_trait]
impl Operation for GetConfigKVHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let cred = validate_config_admin_request(&req).await?;
        let queries = extract_query_params(&req.uri);
        let selector = parse_config_selector(
            queries
                .get("key")
                .ok_or_else(|| s3_error!(InvalidRequest, "missing config key selector"))?,
        )?;
        let config = load_active_server_config()?;
        let payload = render_selected_config(&config, &selector, true)?;
        let (body, content_type) = encode_config_payload(req.uri.path(), &cred.secret_key, payload, TEXT_CONTENT_TYPE)?;
        response_with_content_type(StatusCode::OK, body, &content_type)
    }
}

pub struct SetConfigKVHandler {}

#[async_trait::async_trait]
impl Operation for SetConfigKVHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let cred = validate_config_admin_request(&req).await?;
        let body = read_compatible_admin_body(req.input, MAX_ADMIN_REQUEST_BODY_SIZE, req.uri.path(), &cred.secret_key).await?;
        let directives = parse_config_directives(std::str::from_utf8(&body).map_err(ApiError::other)?, false)?;
        if directives.is_empty() {
            return Err(s3_error!(InvalidRequest, "config update body is empty"));
        }

        let sub_system = config_update_sub_system(&directives)?;
        let mut config = load_server_config_from_store().await?;
        apply_set_directives(&mut config, &directives);
        save_server_config_to_store(&config).await?;
        save_server_config_history(&body).await?;
        let mut config_applied = false;
        if let Some(sub_system) = sub_system
            && is_dynamic_config_subsystem(sub_system)
        {
            config_applied = apply_dynamic_config_for_subsystem(&config, sub_system).await?;
            if config_applied {
                signal_dynamic_config_reload(sub_system).await;
            }
        }

        success_response(config_applied)
    }
}

pub struct DelConfigKVHandler {}

#[async_trait::async_trait]
impl Operation for DelConfigKVHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let cred = validate_config_admin_request(&req).await?;
        let body = read_compatible_admin_body(req.input, MAX_ADMIN_REQUEST_BODY_SIZE, req.uri.path(), &cred.secret_key).await?;
        let directives = parse_config_directives(std::str::from_utf8(&body).map_err(ApiError::other)?, true)?;
        if directives.is_empty() {
            return Err(s3_error!(InvalidRequest, "config delete body is empty"));
        }

        let sub_system = config_update_sub_system(&directives)?;
        let mut config = load_server_config_from_store().await?;
        apply_delete_directives(&mut config, &directives);
        save_server_config_to_store(&config).await?;
        let mut config_applied = false;
        if let Some(sub_system) = sub_system
            && is_dynamic_config_subsystem(sub_system)
        {
            config_applied = apply_dynamic_config_for_subsystem(&config, sub_system).await?;
            if config_applied {
                signal_dynamic_config_reload(sub_system).await;
            }
        }

        success_response(config_applied)
    }
}

pub struct HelpConfigKVHandler {}

#[async_trait::async_trait]
impl Operation for HelpConfigKVHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_config_admin_request(&req).await?;
        let queries = extract_query_params(&req.uri);
        let config = load_active_server_config()?;
        let response = build_help_response(
            &config,
            queries.get("subSys").map(String::as_str),
            queries.get("key").map(String::as_str),
            queries.contains_key("env"),
        )?;
        let body = serde_json::to_vec(&response).map_err(ApiError::other)?;
        response_with_content_type(StatusCode::OK, body, JSON_CONTENT_TYPE)
    }
}

pub struct ListConfigHistoryKVHandler {}

#[async_trait::async_trait]
impl Operation for ListConfigHistoryKVHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let cred = validate_config_admin_request(&req).await?;
        let queries = extract_query_params(&req.uri);
        let count = queries
            .get("count")
            .ok_or_else(|| s3_error!(InvalidRequest, "missing count query parameter"))?
            .parse::<usize>()
            .map_err(ApiError::other)
            .map_err(S3Error::from)?;
        let entries = list_server_config_history(true, Some(count)).await?;
        let payload = serde_json::to_vec(&entries).map_err(ApiError::other).map_err(S3Error::from)?;
        let (body, content_type) = encode_config_payload(req.uri.path(), &cred.secret_key, payload, JSON_CONTENT_TYPE)?;
        response_with_content_type(StatusCode::OK, body, &content_type)
    }
}

pub struct ClearConfigHistoryKVHandler {}

#[async_trait::async_trait]
impl Operation for ClearConfigHistoryKVHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_config_admin_request(&req).await?;
        let queries = extract_query_params(&req.uri);
        let restore_id = queries
            .get("restoreId")
            .ok_or_else(|| s3_error!(InvalidRequest, "missing restoreId query parameter"))?;

        if restore_id == "all" {
            for entry in list_server_config_history(false, None).await? {
                delete_server_config_history(&entry.restore_id).await?;
            }
        } else {
            delete_server_config_history(restore_id).await?;
        }

        success_response(false)
    }
}

pub struct RestoreConfigHistoryKVHandler {}

#[async_trait::async_trait]
impl Operation for RestoreConfigHistoryKVHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_config_admin_request(&req).await?;
        let queries = extract_query_params(&req.uri);
        let restore_id = queries
            .get("restoreId")
            .ok_or_else(|| s3_error!(InvalidRequest, "missing restoreId query parameter"))?;
        let history = read_server_config_history(restore_id).await?;
        let directives = parse_config_directives(std::str::from_utf8(&history).map_err(ApiError::other)?, false)?;
        if directives.is_empty() {
            return Err(s3_error!(InvalidRequest, "history entry is empty"));
        }

        let mut config = load_server_config_from_store().await?;
        apply_set_directives(&mut config, &directives);
        save_server_config_to_store(&config).await?;
        delete_server_config_history(restore_id).await?;

        success_response(false)
    }
}

pub struct GetConfigHandler {}

#[async_trait::async_trait]
impl Operation for GetConfigHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let cred = validate_config_admin_request(&req).await?;
        let config = load_active_server_config()?;
        let payload = render_full_config(&config);
        let (body, content_type) = encode_config_payload(req.uri.path(), &cred.secret_key, payload, TEXT_CONTENT_TYPE)?;
        response_with_content_type(StatusCode::OK, body, &content_type)
    }
}

pub struct SetConfigHandler {}

#[async_trait::async_trait]
impl Operation for SetConfigHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let cred = validate_config_admin_request(&req).await?;
        let body = read_compatible_admin_body(req.input, MAX_ADMIN_REQUEST_BODY_SIZE, req.uri.path(), &cred.secret_key).await?;
        let directives = parse_config_directives(std::str::from_utf8(&body).map_err(ApiError::other)?, false)?;
        if directives.is_empty() {
            return Err(s3_error!(InvalidRequest, "full config body is empty"));
        }

        let mut config = ServerConfig::new();
        apply_set_directives(&mut config, &directives);
        save_server_config_to_store(&config).await?;
        save_server_config_history(&body).await?;

        success_response(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_config_line_handles_quotes_and_escapes() {
        let tokens = tokenize_config_line(r#"identity_openid client_id="console app" client_secret="s3cr\"et" enable=on"#)
            .expect("tokenize");

        assert_eq!(
            tokens,
            vec![
                "identity_openid".to_string(),
                "client_id=console app".to_string(),
                r#"client_secret=s3cr"et"#.to_string(),
                "enable=on".to_string(),
            ]
        );
    }

    #[test]
    fn parse_selector_supports_all_targets_and_default_target() {
        let all_targets = parse_config_selector("notify_webhook").expect("parse selector");
        assert_eq!(
            all_targets,
            ConfigSelector {
                sub_system: "notify_webhook".to_string(),
                target: None,
            }
        );

        let default_target = parse_config_selector("notify_webhook:").expect("parse selector");
        assert_eq!(
            default_target,
            ConfigSelector {
                sub_system: "notify_webhook".to_string(),
                target: Some(DEFAULT_DELIMITER.to_string()),
            }
        );
    }

    #[test]
    fn set_get_and_delete_config_kv_round_trip() {
        let mut config = ServerConfig::new();
        let directives = parse_config_directives(
            r#"identity_openid config_url="https://issuer.example" client_id="console" client_secret="secret-value""#,
            false,
        )
        .expect("parse directives");
        apply_set_directives(&mut config, &directives);

        let rendered = String::from_utf8(
            render_selected_config(
                &config,
                &ConfigSelector {
                    sub_system: "identity_openid".to_string(),
                    target: Some(DEFAULT_DELIMITER.to_string()),
                },
                true,
            )
            .expect("render config"),
        )
        .expect("utf8");
        assert!(rendered.contains(r#"client_id="console""#));
        assert!(rendered.contains(r#"client_secret="*redacted*""#));

        let delete_directives = parse_config_directives("identity_openid client_secret", true).expect("parse delete directives");
        apply_delete_directives(&mut config, &delete_directives);

        let rendered_after_delete = String::from_utf8(
            render_selected_config(
                &config,
                &ConfigSelector {
                    sub_system: "identity_openid".to_string(),
                    target: Some(DEFAULT_DELIMITER.to_string()),
                },
                false,
            )
            .expect("render config"),
        )
        .expect("utf8");
        assert!(!rendered_after_delete.contains("client_secret="));
    }

    #[test]
    fn full_config_export_can_be_reapplied() {
        let mut original = ServerConfig::new();
        apply_set_directives(
            &mut original,
            &parse_config_directives(
                r#"storage_class standard="EC:2" rrs="EC:1"
identity_openid config_url="https://issuer.example" client_id="console""#,
                false,
            )
            .expect("parse directives"),
        );

        let exported = String::from_utf8(render_full_config(&original)).expect("utf8 export");
        let mut restored = ServerConfig::new();
        apply_set_directives(&mut restored, &parse_config_directives(&exported, false).expect("parse exported config"));

        assert_eq!(render_full_config(&original), render_full_config(&restored));
    }

    #[test]
    fn build_help_response_reports_known_keys() {
        let mut config = ServerConfig::new();
        apply_set_directives(
            &mut config,
            &parse_config_directives(
                r#"identity_openid config_url="https://issuer.example" client_id="console" client_secret="secret-value""#,
                false,
            )
            .expect("parse directives"),
        );

        let response =
            build_help_response(&config, Some("identity_openid"), Some("client_secret"), false).expect("help response");

        assert_eq!(response.sub_sys.as_deref(), Some("identity_openid"));
        assert_eq!(response.entries.len(), 1);
        assert_eq!(response.entries[0].key, "client_secret");
        assert!(response.entries[0].sensitive);
        assert!(response.entries[0].configured);
        assert_eq!(response.entries[0].value, REDACTED_VALUE);
    }

    #[test]
    fn history_object_name_round_trips_restore_id() {
        let name = history_object_name("restore-123");
        assert_eq!(name, "config/history/restore-123.kv");
        assert_eq!(history_restore_id_from_name(&name).as_deref(), Some("restore-123"));
        assert!(history_restore_id_from_name("config/history/restore-123.txt").is_none());
    }

    #[test]
    fn trim_history_entries_keeps_most_recent_count_in_time_order() {
        let entries = vec![
            ConfigHistoryEntry {
                restore_id: "oldest".to_string(),
                create_time: OffsetDateTime::from_unix_timestamp(1).expect("timestamp"),
                data: None,
            },
            ConfigHistoryEntry {
                restore_id: "middle".to_string(),
                create_time: OffsetDateTime::from_unix_timestamp(2).expect("timestamp"),
                data: None,
            },
            ConfigHistoryEntry {
                restore_id: "newest".to_string(),
                create_time: OffsetDateTime::from_unix_timestamp(3).expect("timestamp"),
                data: None,
            },
        ];

        let trimmed = trim_history_entries(entries, Some(2));
        assert_eq!(trimmed.len(), 2);
        assert_eq!(trimmed[0].restore_id, "middle");
        assert_eq!(trimmed[1].restore_id, "newest");
    }

    #[test]
    fn restore_history_directives_merge_into_existing_config() {
        let mut config = ServerConfig::new();
        apply_set_directives(
            &mut config,
            &parse_config_directives(
                r#"storage_class standard="EC:2"
identity_openid client_id="existing-client""#,
                false,
            )
            .expect("parse initial directives"),
        );

        let history_directives = parse_config_directives(
            r#"identity_openid config_url="https://issuer.example" client_secret="restored-secret""#,
            false,
        )
        .expect("parse history directives");
        apply_set_directives(&mut config, &history_directives);

        let storage_class = String::from_utf8(
            render_selected_config(
                &config,
                &ConfigSelector {
                    sub_system: "storage_class".to_string(),
                    target: Some(DEFAULT_DELIMITER.to_string()),
                },
                false,
            )
            .expect("render storage class"),
        )
        .expect("utf8");
        assert!(storage_class.contains(r#"standard="EC:2""#));

        let oidc = String::from_utf8(
            render_selected_config(
                &config,
                &ConfigSelector {
                    sub_system: "identity_openid".to_string(),
                    target: Some(DEFAULT_DELIMITER.to_string()),
                },
                false,
            )
            .expect("render oidc"),
        )
        .expect("utf8");
        assert!(oidc.contains(r#"client_id="existing-client""#));
        assert!(oidc.contains(r#"config_url="https://issuer.example""#));
        assert!(oidc.contains(r#"client_secret="restored-secret""#));
    }
}
