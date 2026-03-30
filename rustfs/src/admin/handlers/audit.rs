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

use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::auth::{check_key_valid, get_session_token};
use crate::server::ADMIN_PREFIX;
use futures::stream::{FuturesUnordered, StreamExt};
use hashbrown::HashSet as HbHashSet;
use http::{HeaderMap, StatusCode};
use hyper::Method;
use matchit::Params;
use rustfs_audit::{audit_system, start_audit_system as start_global_audit_system, system::AuditSystemState};
use rustfs_config::audit::{AUDIT_MQTT_KEYS, AUDIT_MQTT_SUB_SYS, AUDIT_ROUTE_PREFIX, AUDIT_WEBHOOK_KEYS, AUDIT_WEBHOOK_SUB_SYS};
use rustfs_config::{DEFAULT_DELIMITER, ENABLE_KEY, ENV_PREFIX, EnableState, MAX_ADMIN_REQUEST_BODY_SIZE};
use rustfs_ecstore::config::Config;
use rustfs_targets::check_mqtt_broker_available;
use s3s::{Body, S3Request, S3Response, S3Result, header::CONTENT_TYPE, s3_error};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use tokio::net::lookup_host;
use tokio::sync::Semaphore;
use tokio::time::{Duration, sleep, timeout};
use tracing::{Span, warn};
use url::Url;

pub fn register_audit_target_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/audit/target/list").as_str(),
        AdminOperation(&ListAuditTargets {}),
    )?;

    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/audit/target/{target_type}/{target_name}").as_str(),
        AdminOperation(&AuditTargetConfig {}),
    )?;

    r.insert(
        Method::DELETE,
        format!("{}{}", ADMIN_PREFIX, "/v3/audit/target/{target_type}/{target_name}/reset").as_str(),
        AdminOperation(&RemoveAuditTarget {}),
    )?;

    Ok(())
}

#[derive(Debug, Deserialize)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Deserialize)]
pub struct AuditTargetBody {
    pub key_values: Vec<KeyValue>,
}

#[derive(Serialize, Debug)]
struct AuditEndpoint {
    account_id: String,
    service: String,
    status: String,
    source: AuditEndpointSource,
}

#[derive(Serialize, Debug)]
struct AuditEndpointsResponse {
    audit_endpoints: Vec<AuditEndpoint>,
}

type EndpointKey = (String, String);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum AuditEndpointSource {
    Config,
    Env,
    Mixed,
    Runtime,
}

async fn check_permissions(req: &S3Request<Body>) -> S3Result<()> {
    let Some(input_cred) = &req.credentials else {
        return Err(s3_error!(InvalidRequest, "credentials not found"));
    };
    check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;
    Ok(())
}

fn build_response(status: StatusCode, body: Body, request_id: Option<&http::HeaderValue>) -> S3Response<(StatusCode, Body)> {
    let mut header = HeaderMap::new();
    header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
    if let Some(v) = request_id {
        header.insert("x-request-id", v.clone());
    }
    S3Response::with_headers((status, body), header)
}

async fn retry_with_backoff<F, Fut, T>(mut operation: F, max_attempts: usize, base_delay: Duration) -> Result<T, Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Error>>,
{
    let mut attempts = 0;
    let mut delay = base_delay;
    let mut last_err = None;

    while attempts < max_attempts {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                last_err = Some(e);
                attempts += 1;
                if attempts < max_attempts {
                    sleep(delay).await;
                    delay = delay.saturating_mul(2);
                }
            }
        }
    }
    Err(last_err.unwrap_or_else(|| Error::other("retry_with_backoff: unknown error")))
}

async fn validate_queue_dir(queue_dir: &str) -> S3Result<()> {
    if !queue_dir.is_empty() {
        if !Path::new(queue_dir).is_absolute() {
            return Err(s3_error!(InvalidArgument, "queue_dir must be absolute path"));
        }
        retry_with_backoff(
            || async { tokio::fs::metadata(queue_dir).await.map(|_| ()) },
            3,
            Duration::from_millis(100),
        )
        .await
        .map_err(|e| match e.kind() {
            ErrorKind::NotFound => s3_error!(InvalidArgument, "queue_dir does not exist"),
            ErrorKind::PermissionDenied => s3_error!(InvalidArgument, "queue_dir exists but permission denied"),
            _ => s3_error!(InvalidArgument, "failed to access queue_dir: {}", e),
        })?;
    }
    Ok(())
}

fn config_enable_is_on(value: &str) -> bool {
    matches!(value.trim().to_ascii_lowercase().as_str(), "on" | "true" | "yes" | "1")
}

fn has_any_audit_targets(config: &Config) -> bool {
    for subsystem in [AUDIT_WEBHOOK_SUB_SYS, AUDIT_MQTT_SUB_SYS] {
        let Some(targets) = config.0.get(subsystem) else {
            continue;
        };
        if targets.keys().any(|key| key != DEFAULT_DELIMITER) {
            return true;
        }
    }
    false
}

fn collect_configured_audit_endpoint_keys(config: &Config) -> Vec<EndpointKey> {
    let mut endpoints = Vec::new();
    for (subsystem, service) in [(AUDIT_WEBHOOK_SUB_SYS, "webhook"), (AUDIT_MQTT_SUB_SYS, "mqtt")] {
        let Some(targets) = config.0.get(subsystem) else {
            continue;
        };

        for (target_name, kvs) in targets {
            if target_name == DEFAULT_DELIMITER {
                continue;
            }
            let enabled = kvs.lookup(ENABLE_KEY).as_deref().map(config_enable_is_on).unwrap_or(false);
            if enabled {
                endpoints.push((target_name.clone(), service.to_string()));
            }
        }
    }
    endpoints
}

fn collect_config_entry_keys(config: &Config) -> HbHashSet<EndpointKey> {
    let mut endpoints = HbHashSet::new();
    for (subsystem, service) in [(AUDIT_WEBHOOK_SUB_SYS, "webhook"), (AUDIT_MQTT_SUB_SYS, "mqtt")] {
        let Some(targets) = config.0.get(subsystem) else {
            continue;
        };

        for target_name in targets.keys() {
            if target_name == DEFAULT_DELIMITER {
                continue;
            }
            endpoints.insert((target_name.clone(), service.to_string()));
        }
    }
    endpoints
}

fn collect_env_endpoint_keys() -> HbHashSet<EndpointKey> {
    let mut endpoints = HbHashSet::new();

    for (service, valid_keys) in [("webhook", AUDIT_WEBHOOK_KEYS), ("mqtt", AUDIT_MQTT_KEYS)] {
        let env_prefix = format!("{ENV_PREFIX}{AUDIT_ROUTE_PREFIX}{service}{DEFAULT_DELIMITER}").to_uppercase();

        for (key, _value) in std::env::vars() {
            let Some(rest) = key.strip_prefix(&env_prefix) else {
                continue;
            };

            let mut parts = rest.rsplitn(2, DEFAULT_DELIMITER);
            let instance_id_part = parts.next().unwrap_or(DEFAULT_DELIMITER);
            let field_name_part = parts.next();

            let (field_name, instance_id) = match field_name_part {
                Some(field) => (field.to_lowercase(), instance_id_part.to_lowercase()),
                None => (instance_id_part.to_lowercase(), DEFAULT_DELIMITER.to_string()),
            };

            if instance_id == DEFAULT_DELIMITER || instance_id.is_empty() {
                continue;
            }

            if valid_keys.contains(&field_name.as_str()) {
                endpoints.insert((instance_id, service.to_string()));
            }
        }
    }

    endpoints
}

fn classify_audit_endpoint_source(
    config_targets: &HbHashSet<EndpointKey>,
    env_targets: &HbHashSet<EndpointKey>,
    key: &EndpointKey,
) -> AuditEndpointSource {
    match (config_targets.contains(key), env_targets.contains(key)) {
        (true, true) => AuditEndpointSource::Mixed,
        (true, false) => AuditEndpointSource::Config,
        (false, true) => AuditEndpointSource::Env,
        (false, false) => AuditEndpointSource::Runtime,
    }
}

fn audit_endpoint_source(config: &Config, target_type: &str, target_name: &str) -> AuditEndpointSource {
    let config_targets = collect_config_entry_keys(config);
    let env_targets = collect_env_endpoint_keys();
    let service = match target_type {
        AUDIT_WEBHOOK_SUB_SYS => "webhook",
        AUDIT_MQTT_SUB_SYS => "mqtt",
        _ => "",
    };

    classify_audit_endpoint_source(&config_targets, &env_targets, &(target_name.to_lowercase(), service.to_string()))
}

fn audit_target_mutation_block_reason(config: &Config, target_type: &str, target_name: &str) -> Option<String> {
    match audit_endpoint_source(config, target_type, target_name) {
        AuditEndpointSource::Env => Some(format!(
            "audit target '{}' is managed by environment variables and cannot be modified from the console",
            target_name
        )),
        AuditEndpointSource::Mixed => Some(format!(
            "audit target '{}' is configured by both persisted config and environment variables; remove the environment variables first",
            target_name
        )),
        AuditEndpointSource::Config | AuditEndpointSource::Runtime => None,
    }
}

fn merge_audit_endpoints(config: &Config, mut runtime_statuses: HashMap<EndpointKey, String>) -> Vec<AuditEndpoint> {
    let mut audit_endpoints = Vec::new();
    let mut seen = HashSet::new();
    let configured_keys = collect_configured_audit_endpoint_keys(config);
    let config_targets = collect_config_entry_keys(config);
    let env_targets = collect_env_endpoint_keys();

    for key in configured_keys {
        if !seen.insert(key.clone()) {
            continue;
        }
        let status = runtime_statuses.remove(&key).unwrap_or_else(|| "offline".to_string());
        let source = classify_audit_endpoint_source(&config_targets, &env_targets, &key);
        audit_endpoints.push(AuditEndpoint {
            account_id: key.0,
            service: key.1,
            status,
            source,
        });
    }

    for ((account_id, service), status) in runtime_statuses {
        let key = (account_id.clone(), service.clone());
        if seen.insert((account_id.clone(), service.clone())) {
            audit_endpoints.push(AuditEndpoint {
                account_id,
                service,
                status,
                source: classify_audit_endpoint_source(&config_targets, &env_targets, &key),
            });
        }
    }

    for key in &env_targets {
        if !seen.insert(key.clone()) {
            continue;
        }

        audit_endpoints.push(AuditEndpoint {
            account_id: key.0.clone(),
            service: key.1.clone(),
            status: "offline".to_string(),
            source: classify_audit_endpoint_source(&config_targets, &env_targets, key),
        });
    }

    audit_endpoints.sort_by(|a, b| a.service.cmp(&b.service).then_with(|| a.account_id.cmp(&b.account_id)));
    audit_endpoints
}

fn collect_validated_key_values(
    key_values: &[KeyValue],
    allowed_keys: &HashSet<&str>,
    target_type: &str,
) -> S3Result<HashMap<String, String>> {
    let mut kv_map = HashMap::new();
    let mut seen = HashSet::new();

    for kv in key_values {
        if !allowed_keys.contains(kv.key.as_str()) {
            return Err(s3_error!(
                InvalidArgument,
                "key '{}' not allowed for audit target type '{}'",
                kv.key,
                target_type
            ));
        }

        if !seen.insert(kv.key.as_str()) {
            return Err(s3_error!(InvalidArgument, "duplicate key '{}' in request body", kv.key));
        }

        kv_map.insert(kv.key.clone(), kv.value.clone());
    }

    Ok(kv_map)
}

fn extract_target_params<'a>(params: &'a Params<'_, '_>) -> S3Result<(&'a str, &'a str)> {
    let target_type = params
        .get("target_type")
        .ok_or_else(|| s3_error!(InvalidArgument, "missing required parameter: 'target_type'"))?;
    if target_type != AUDIT_WEBHOOK_SUB_SYS && target_type != AUDIT_MQTT_SUB_SYS {
        return Err(s3_error!(InvalidArgument, "unsupported audit target type: '{}'", target_type));
    }
    let target_name = params
        .get("target_name")
        .ok_or_else(|| s3_error!(InvalidArgument, "missing required parameter: 'target_name'"))?;
    Ok((target_type, target_name))
}

async fn load_server_config_from_store() -> S3Result<Config> {
    let Some(store) = rustfs_ecstore::global::new_object_layer_fn() else {
        return Ok(Config::new());
    };

    rustfs_ecstore::config::com::read_config_without_migrate(store)
        .await
        .map_err(|e| s3_error!(InternalError, "failed to read server config: {}", e))
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

pub struct AuditTargetConfig {}

#[async_trait::async_trait]
impl Operation for AuditTargetConfig {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let span = Span::current();
        let _enter = span.enter();
        let (target_type, target_name) = extract_target_params(&params)?;

        check_permissions(&req).await?;
        let config_snapshot = load_server_config_from_store().await?;
        if let Some(reason) = audit_target_mutation_block_reason(&config_snapshot, target_type, target_name) {
            return Err(s3_error!(InvalidRequest, "{reason}"));
        }

        let mut input = req.input;
        let body_bytes = input.store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE).await.map_err(|e| {
            warn!("failed to read request body: {:?}", e);
            s3_error!(InvalidRequest, "failed to read request body")
        })?;

        let audit_body: AuditTargetBody = serde_json::from_slice(&body_bytes)
            .map_err(|e| s3_error!(InvalidArgument, "invalid json body for audit target config: {}", e))?;

        let allowed_keys: HashSet<&str> = match target_type {
            AUDIT_WEBHOOK_SUB_SYS => AUDIT_WEBHOOK_KEYS.iter().cloned().collect(),
            AUDIT_MQTT_SUB_SYS => AUDIT_MQTT_KEYS.iter().cloned().collect(),
            _ => unreachable!(),
        };

        let kv_map = collect_validated_key_values(&audit_body.key_values, &allowed_keys, target_type)?;

        if target_type == AUDIT_WEBHOOK_SUB_SYS {
            let endpoint = kv_map
                .get("endpoint")
                .map(String::as_str)
                .ok_or_else(|| s3_error!(InvalidArgument, "endpoint is required"))?;
            let url = Url::parse(endpoint).map_err(|e| s3_error!(InvalidArgument, "invalid endpoint url: {}", e))?;
            let host = url
                .host_str()
                .ok_or_else(|| s3_error!(InvalidArgument, "endpoint missing host"))?;
            let port = url
                .port_or_known_default()
                .ok_or_else(|| s3_error!(InvalidArgument, "endpoint missing port"))?;
            let addr = format!("{host}:{port}");
            if addr.parse::<SocketAddr>().is_err() && lookup_host(&addr).await.is_err() {
                return Err(s3_error!(InvalidArgument, "invalid or unresolvable endpoint address"));
            }
            if let Some(queue_dir) = kv_map.get("queue_dir") {
                validate_queue_dir(queue_dir.as_str()).await?;
            }
            if kv_map.contains_key("client_cert") != kv_map.contains_key("client_key") {
                return Err(s3_error!(InvalidArgument, "client_cert and client_key must be specified as a pair"));
            }
        } else if target_type == AUDIT_MQTT_SUB_SYS {
            let endpoint = kv_map
                .get(rustfs_config::MQTT_BROKER)
                .map(String::as_str)
                .ok_or_else(|| s3_error!(InvalidArgument, "broker endpoint is required"))?;
            let topic = kv_map
                .get(rustfs_config::MQTT_TOPIC)
                .map(String::as_str)
                .ok_or_else(|| s3_error!(InvalidArgument, "topic is required"))?;
            let username = kv_map.get(rustfs_config::MQTT_USERNAME).map(String::as_str);
            let password = kv_map.get(rustfs_config::MQTT_PASSWORD).map(String::as_str);
            check_mqtt_broker_available(endpoint, topic, username, password)
                .await
                .map_err(|e| s3_error!(InvalidArgument, "MQTT Broker unavailable: {}", e))?;

            if let Some(queue_dir) = kv_map.get("queue_dir") {
                validate_queue_dir(queue_dir.as_str()).await?;
                if let Some(qos) = kv_map.get("qos") {
                    match qos.parse::<u8>() {
                        Ok(1) | Ok(2) => {}
                        Ok(0) => return Err(s3_error!(InvalidArgument, "qos should be 1 or 2 if queue_dir is set")),
                        _ => return Err(s3_error!(InvalidArgument, "qos must be an integer 0, 1, or 2")),
                    }
                }
            }
        }

        let mut kvs = rustfs_ecstore::config::KVS::new();
        for (key, value) in kv_map {
            kvs.insert(key, value);
        }
        kvs.insert(ENABLE_KEY.to_string(), EnableState::On.to_string());

        update_audit_config_and_reload(|config| {
            config
                .0
                .entry(target_type.to_lowercase())
                .or_default()
                .insert(target_name.to_lowercase(), kvs.clone());
            true
        })
        .await?;

        Ok(build_response(StatusCode::OK, Body::empty(), req.headers.get("x-request-id")))
    }
}

pub struct ListAuditTargets {}

#[async_trait::async_trait]
impl Operation for ListAuditTargets {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let span = Span::current();
        let _enter = span.enter();
        check_permissions(&req).await?;

        let mut runtime_statuses = HashMap::new();
        if let Some(system) = audit_system() {
            let targets = system.get_target_values().await;
            let semaphore = Arc::new(Semaphore::new(10));
            let mut futures = FuturesUnordered::new();

            for target in targets {
                let sem = Arc::clone(&semaphore);
                futures.push(async move {
                    let _permit = sem.acquire().await;
                    let status = match timeout(Duration::from_secs(3), target.is_active()).await {
                        Ok(Ok(true)) => "online",
                        _ => "offline",
                    };
                    ((target.id().id.clone(), target.id().name.to_string()), status.to_string())
                });
            }

            while let Some((key, status)) = futures.next().await {
                runtime_statuses.insert(key, status);
            }
        }

        let config = load_server_config_from_store().await?;
        let audit_endpoints = merge_audit_endpoints(&config, runtime_statuses);
        let data = serde_json::to_vec(&AuditEndpointsResponse { audit_endpoints })
            .map_err(|e| s3_error!(InternalError, "failed to serialize audit targets: {}", e))?;

        Ok(build_response(StatusCode::OK, Body::from(data), req.headers.get("x-request-id")))
    }
}

pub struct RemoveAuditTarget {}

#[async_trait::async_trait]
impl Operation for RemoveAuditTarget {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let span = Span::current();
        let _enter = span.enter();
        let (target_type, target_name) = extract_target_params(&params)?;

        check_permissions(&req).await?;
        let config_snapshot = load_server_config_from_store().await?;
        if let Some(reason) = audit_target_mutation_block_reason(&config_snapshot, target_type, target_name) {
            return Err(s3_error!(InvalidRequest, "{reason}"));
        }

        update_audit_config_and_reload(|config| {
            let mut changed = false;
            if let Some(targets) = config.0.get_mut(&target_type.to_lowercase()) {
                if targets.remove(&target_name.to_lowercase()).is_some() {
                    changed = true;
                }
                if targets.is_empty() {
                    config.0.remove(&target_type.to_lowercase());
                }
            }
            changed
        })
        .await?;

        Ok(build_response(StatusCode::OK, Body::empty(), req.headers.get("x-request-id")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_ecstore::config::{KV, KVS};
    use std::collections::{HashMap, HashSet};
    use temp_env::{with_var, with_vars};

    fn enabled_kvs(value: &str) -> KVS {
        KVS(vec![KV {
            key: ENABLE_KEY.to_string(),
            value: value.to_string(),
            hidden_if_empty: false,
        }])
    }

    #[test]
    fn merge_audit_endpoints_marks_config_env_and_mixed_sources() {
        let config = Config(HashMap::from([(
            AUDIT_WEBHOOK_SUB_SYS.to_string(),
            HashMap::from([
                ("mixed-target".to_string(), enabled_kvs("on")),
                ("config-target".to_string(), enabled_kvs("on")),
            ]),
        )]));

        with_vars(
            [
                ("RUSTFS_AUDIT_WEBHOOK_ENDPOINT_MIXED-TARGET", Some("https://example.com/hook")),
                ("RUSTFS_AUDIT_WEBHOOK_ENABLE_ENV-ONLY", Some("on")),
                ("RUSTFS_AUDIT_WEBHOOK_ENDPOINT_ENV-ONLY", Some("https://example.com/env")),
            ],
            || {
                let runtime = HashMap::from([
                    (("mixed-target".to_string(), "webhook".to_string()), "online".to_string()),
                    (("env-only".to_string(), "webhook".to_string()), "online".to_string()),
                ]);
                let merged = merge_audit_endpoints(&config, runtime);

                let mixed = merged
                    .iter()
                    .find(|entry| entry.account_id == "mixed-target")
                    .expect("mixed target should be present");
                assert_eq!(mixed.source, AuditEndpointSource::Mixed);

                let env_only = merged
                    .iter()
                    .find(|entry| entry.account_id == "env-only")
                    .expect("env-only target should be present");
                assert_eq!(env_only.source, AuditEndpointSource::Env);

                let config_only = merged
                    .iter()
                    .find(|entry| entry.account_id == "config-target")
                    .expect("config target should be present");
                assert_eq!(config_only.source, AuditEndpointSource::Config);
            },
        );
    }

    #[test]
    fn audit_target_mutation_block_reason_rejects_env_managed_target() {
        with_vars(
            [
                ("RUSTFS_AUDIT_WEBHOOK_ENABLE_PRIMARY", Some("on")),
                ("RUSTFS_AUDIT_WEBHOOK_ENDPOINT_PRIMARY", Some("https://example.com/hook")),
            ],
            || {
                let config = Config(HashMap::new());
                let reason = audit_target_mutation_block_reason(&config, AUDIT_WEBHOOK_SUB_SYS, "primary");
                assert!(reason.is_some());
                assert!(reason.unwrap().contains("managed by environment variables"));
            },
        );
    }

    #[test]
    fn audit_target_mutation_block_reason_rejects_mixed_target() {
        with_var("RUSTFS_AUDIT_WEBHOOK_ENDPOINT_PRIMARY", Some("https://example.com/hook"), || {
            let config = Config(HashMap::from([(
                AUDIT_WEBHOOK_SUB_SYS.to_string(),
                HashMap::from([("primary".to_string(), enabled_kvs("on"))]),
            )]));
            let reason = audit_target_mutation_block_reason(&config, AUDIT_WEBHOOK_SUB_SYS, "primary");
            assert!(reason.is_some());
            assert!(reason.unwrap().contains("both persisted config and environment variables"));
        });
    }

    #[test]
    fn merge_audit_endpoints_marks_disabled_config_with_env_override_as_mixed() {
        let config = Config(HashMap::from([(
            AUDIT_WEBHOOK_SUB_SYS.to_string(),
            HashMap::from([("mixed-disabled".to_string(), enabled_kvs("off"))]),
        )]));

        with_vars(
            [
                ("RUSTFS_AUDIT_WEBHOOK_ENABLE_MIXED-DISABLED", Some("on")),
                ("RUSTFS_AUDIT_WEBHOOK_ENDPOINT_MIXED-DISABLED", Some("https://example.com/hook")),
            ],
            || {
                let merged = merge_audit_endpoints(&config, HashMap::new());
                let mixed = merged
                    .iter()
                    .find(|entry| entry.account_id == "mixed-disabled")
                    .expect("mixed target should be present");
                assert_eq!(mixed.source, AuditEndpointSource::Mixed);
                assert_eq!(mixed.status, "offline");
            },
        );
    }

    #[test]
    fn merge_audit_endpoints_includes_env_only_target_without_runtime_status() {
        let config = Config(HashMap::new());

        with_vars(
            [
                ("RUSTFS_AUDIT_WEBHOOK_ENABLE_ENV-ONLY", Some("on")),
                ("RUSTFS_AUDIT_WEBHOOK_ENDPOINT_ENV-ONLY", Some("https://example.com/env")),
            ],
            || {
                let merged = merge_audit_endpoints(&config, HashMap::new());
                let env_only = merged
                    .iter()
                    .find(|entry| entry.account_id == "env-only")
                    .expect("env-only target should be present");
                assert_eq!(env_only.source, AuditEndpointSource::Env);
                assert_eq!(env_only.status, "offline");
            },
        );
    }

    #[test]
    fn collect_validated_key_values_rejects_duplicate_keys() {
        let allowed_keys: HashSet<&str> = ["endpoint", "auth_token"].into_iter().collect();
        let key_values = vec![
            KeyValue {
                key: "endpoint".to_string(),
                value: "https://example.com/one".to_string(),
            },
            KeyValue {
                key: "endpoint".to_string(),
                value: "https://example.com/two".to_string(),
            },
        ];

        let err = collect_validated_key_values(&key_values, &allowed_keys, AUDIT_WEBHOOK_SUB_SYS).unwrap_err();
        assert!(err.to_string().contains("duplicate key"));
    }
}
