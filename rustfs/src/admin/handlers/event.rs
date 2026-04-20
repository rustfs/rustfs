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
use futures::stream::{FuturesUnordered, StreamExt};
use hashbrown::HashSet as HbHashSet;
use http::{HeaderMap, StatusCode};
use hyper::Method;
use matchit::Params;
use rustfs_config::notify::{
    NOTIFY_MQTT_KEYS, NOTIFY_MQTT_SUB_SYS, NOTIFY_NATS_KEYS, NOTIFY_NATS_SUB_SYS, NOTIFY_PULSAR_KEYS, NOTIFY_PULSAR_SUB_SYS,
    NOTIFY_ROUTE_PREFIX, NOTIFY_WEBHOOK_KEYS, NOTIFY_WEBHOOK_SUB_SYS,
};
use rustfs_config::{DEFAULT_DELIMITER, ENABLE_KEY, EVENT_DEFAULT_DIR, EnableState, MAX_ADMIN_REQUEST_BODY_SIZE};
use rustfs_ecstore::config::Config;
use rustfs_policy::policy::action::{Action, AdminAction};
use rustfs_targets::{
    TargetError, check_mqtt_broker_available_with_tls, check_nats_server_available, check_pulsar_broker_available,
    config::{build_nats_args, build_pulsar_args, collect_env_target_instance_ids},
    target::mqtt::MQTTTlsConfig,
};
use s3s::{Body, S3Request, S3Response, S3Result, header::CONTENT_TYPE, s3_error};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::io::{Error, ErrorKind};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::time::{Duration, sleep, timeout};
use tracing::{Span, info, warn};
use url::Url;

pub fn register_notification_target_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/target/list").as_str(),
        AdminOperation(&ListNotificationTargets {}),
    )?;

    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/target/{target_type}/{target_name}").as_str(),
        AdminOperation(&NotificationTarget {}),
    )?;

    r.insert(
        Method::DELETE,
        format!("{}{}", ADMIN_PREFIX, "/v3/target/{target_type}/{target_name}/reset").as_str(),
        AdminOperation(&RemoveNotificationTarget {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/target/arns").as_str(),
        AdminOperation(&ListTargetsArns {}),
    )?;

    Ok(())
}

#[derive(Debug, Deserialize)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Deserialize)]
pub struct NotificationTargetBody {
    pub key_values: Vec<KeyValue>,
}

#[derive(Serialize, Debug)]
struct NotificationEndpoint {
    account_id: String,
    service: String,
    status: String,
    source: NotificationEndpointSource,
}

#[derive(Serialize, Debug)]
struct NotificationEndpointsResponse {
    notification_endpoints: Vec<NotificationEndpoint>,
}

type EndpointKey = (String, String);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum NotificationEndpointSource {
    Config,
    Env,
    Mixed,
    Runtime,
}

fn normalized_endpoint_key(account_id: &str, service: &str) -> EndpointKey {
    (account_id.to_lowercase(), service.to_string())
}

fn notification_target_specs() -> [(&'static str, &'static str, &'static [&'static str]); 4] {
    [
        (NOTIFY_WEBHOOK_SUB_SYS, "webhook", NOTIFY_WEBHOOK_KEYS),
        (NOTIFY_MQTT_SUB_SYS, "mqtt", NOTIFY_MQTT_KEYS),
        (NOTIFY_NATS_SUB_SYS, "nats", NOTIFY_NATS_KEYS),
        (NOTIFY_PULSAR_SUB_SYS, "pulsar", NOTIFY_PULSAR_KEYS),
    ]
}

fn notification_service_name(target_type: &str) -> Option<&'static str> {
    notification_target_specs()
        .into_iter()
        .find_map(|(subsystem, service, _)| (subsystem == target_type).then_some(service))
}

// --- Helper Functions ---

async fn authorize_notification_admin_request(req: &S3Request<Body>, action: AdminAction) -> S3Result<()> {
    let Some(input_cred) = &req.credentials else {
        return Err(s3_error!(InvalidRequest, "credentials not found"));
    };
    let (cred, owner) =
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;
    let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
    validate_admin_request(&req.headers, &cred, owner, false, vec![Action::AdminAction(action)], remote_addr).await
}

fn get_notification_system() -> S3Result<Arc<rustfs_notify::NotificationSystem>> {
    rustfs_notify::notification_system().ok_or_else(|| s3_error!(InternalError, "notification system not initialized"))
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

fn collect_configured_endpoint_keys(config: &Config) -> Vec<EndpointKey> {
    let mut endpoints = Vec::new();
    for (subsystem, service, _) in notification_target_specs() {
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
    for (subsystem, service, _) in notification_target_specs() {
        let Some(targets) = config.0.get(subsystem) else {
            continue;
        };

        for target_name in targets.keys() {
            if target_name == DEFAULT_DELIMITER {
                continue;
            }
            endpoints.insert(normalized_endpoint_key(target_name, service));
        }
    }
    endpoints
}

fn collect_env_endpoint_keys() -> HbHashSet<EndpointKey> {
    let mut endpoints = HbHashSet::new();

    for (_subsystem, service, valid_keys) in notification_target_specs() {
        let valid_keys = valid_keys.iter().map(|key| (*key).to_string()).collect::<HashSet<_>>();
        for instance_id in collect_env_target_instance_ids(NOTIFY_ROUTE_PREFIX, service, &valid_keys) {
            if instance_id != DEFAULT_DELIMITER && !instance_id.is_empty() {
                endpoints.insert(normalized_endpoint_key(&instance_id, service));
            }
        }
    }

    endpoints
}

fn classify_notification_endpoint_source(
    config_targets: &HbHashSet<EndpointKey>,
    env_targets: &HbHashSet<EndpointKey>,
    key: &EndpointKey,
) -> NotificationEndpointSource {
    match (config_targets.contains(key), env_targets.contains(key)) {
        (true, true) => NotificationEndpointSource::Mixed,
        (true, false) => NotificationEndpointSource::Config,
        (false, true) => NotificationEndpointSource::Env,
        (false, false) => NotificationEndpointSource::Runtime,
    }
}

fn notification_endpoint_source(config: &Config, target_type: &str, target_name: &str) -> NotificationEndpointSource {
    let config_targets = collect_config_entry_keys(config);
    let env_targets = collect_env_endpoint_keys();
    let service = notification_service_name(target_type).unwrap_or_default();

    let key = normalized_endpoint_key(target_name, service);
    classify_notification_endpoint_source(&config_targets, &env_targets, &key)
}

fn target_mutation_block_reason(config: &Config, target_type: &str, target_name: &str) -> Option<String> {
    match notification_endpoint_source(config, target_type, target_name) {
        NotificationEndpointSource::Env => Some(format!(
            "target '{}' is managed by environment variables and cannot be modified from the console",
            target_name
        )),
        NotificationEndpointSource::Mixed => Some(format!(
            "target '{}' is configured by both persisted config and environment variables; remove the environment variables first",
            target_name
        )),
        NotificationEndpointSource::Config | NotificationEndpointSource::Runtime => None,
    }
}

fn merge_notification_endpoints(config: &Config, runtime_statuses: HashMap<EndpointKey, String>) -> Vec<NotificationEndpoint> {
    let mut notification_endpoints = Vec::new();
    let mut seen = HashSet::new();
    let configured_keys = collect_configured_endpoint_keys(config);
    let config_targets = collect_config_entry_keys(config);
    let env_targets = collect_env_endpoint_keys();
    let mut normalized_runtime_statuses: HashMap<EndpointKey, (String, String, String)> = HashMap::new();
    for ((account_id, service), status) in runtime_statuses {
        let normalized = normalized_endpoint_key(&account_id, &service);
        normalized_runtime_statuses
            .entry(normalized)
            .or_insert((account_id, service, status));
    }

    for key in configured_keys {
        let normalized = normalized_endpoint_key(&key.0, &key.1);
        if !seen.insert(normalized.clone()) {
            continue;
        }
        let status = normalized_runtime_statuses
            .remove(&normalized)
            .map(|(_, _, status)| status)
            .unwrap_or_else(|| "offline".to_string());
        let source = classify_notification_endpoint_source(&config_targets, &env_targets, &normalized);
        notification_endpoints.push(NotificationEndpoint {
            account_id: key.0,
            service: key.1,
            status,
            source,
        });
    }

    for (normalized, (account_id, service, status)) in normalized_runtime_statuses {
        if seen.insert(normalized.clone()) {
            notification_endpoints.push(NotificationEndpoint {
                account_id,
                service,
                status,
                source: classify_notification_endpoint_source(&config_targets, &env_targets, &normalized),
            });
        }
    }

    for key in &env_targets {
        if !seen.insert(key.clone()) {
            continue;
        }

        notification_endpoints.push(NotificationEndpoint {
            account_id: key.0.clone(),
            service: key.1.clone(),
            status: "offline".to_string(),
            source: classify_notification_endpoint_source(&config_targets, &env_targets, key),
        });
    }

    notification_endpoints.sort_by(|a, b| a.service.cmp(&b.service).then_with(|| a.account_id.cmp(&b.account_id)));
    notification_endpoints
}

fn collect_online_target_arns(region: &str, target_statuses: Vec<(rustfs_targets::arn::TargetID, String)>) -> Vec<String> {
    target_statuses
        .into_iter()
        .filter_map(|(target_id, status)| (status == "online").then(|| target_id.to_arn(region).to_string()))
        .collect()
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
                "key '{}' not allowed for target type '{}'",
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

// --- Operations ---

pub struct NotificationTarget {}
#[async_trait::async_trait]
impl Operation for NotificationTarget {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let span = Span::current();
        let _enter = span.enter();
        let (target_type, target_name) = extract_target_params(&params)?;

        authorize_notification_admin_request(&req, AdminAction::SetBucketTargetAction).await?;
        let ns = get_notification_system()?;
        let config_snapshot = ns.config.read().await.clone();
        if let Some(reason) = target_mutation_block_reason(&config_snapshot, target_type, target_name) {
            return Err(s3_error!(InvalidRequest, "{reason}"));
        }

        let mut input = req.input;
        let body_bytes = input.store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE).await.map_err(|e| {
            warn!("failed to read request body: {:?}", e);
            s3_error!(InvalidRequest, "failed to read request body")
        })?;

        let notification_body: NotificationTargetBody = serde_json::from_slice(&body_bytes)
            .map_err(|e| s3_error!(InvalidArgument, "invalid json body for target config: {}", e))?;

        let allowed_keys: HashSet<&str> = notification_target_specs()
            .into_iter()
            .find_map(|(subsystem, _, valid_keys)| (subsystem == target_type).then(|| valid_keys.iter().copied().collect()))
            .unwrap_or_default();

        let kv_map = collect_validated_key_values(&notification_body.key_values, &allowed_keys, target_type)?;

        // Type-specific validation
        if target_type == NOTIFY_WEBHOOK_SUB_SYS {
            let endpoint = kv_map
                .get("endpoint")
                .map(String::as_str)
                .ok_or_else(|| s3_error!(InvalidArgument, "endpoint is required"))?;
            let parsed_endpoint = Url::parse(endpoint).map_err(|e| s3_error!(InvalidArgument, "invalid endpoint url: {}", e))?;
            match parsed_endpoint.scheme() {
                "http" | "https" => {}
                other => {
                    return Err(s3_error!(
                        InvalidArgument,
                        "unsupported endpoint scheme: {} (only http and https are allowed)",
                        other
                    ));
                }
            }
            if let Some(queue_dir) = kv_map.get("queue_dir") {
                validate_queue_dir(queue_dir.as_str()).await?;
            }
            if kv_map.contains_key("client_cert") != kv_map.contains_key("client_key") {
                return Err(s3_error!(InvalidArgument, "client_cert and client_key must be specified as a pair"));
            }
        } else if target_type == NOTIFY_MQTT_SUB_SYS {
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
            let tls = MQTTTlsConfig::from_values(
                kv_map.get(rustfs_config::MQTT_TLS_POLICY).map(String::as_str),
                kv_map.get(rustfs_config::MQTT_TLS_CA).map(String::as_str),
                kv_map.get(rustfs_config::MQTT_TLS_CLIENT_CERT).map(String::as_str),
                kv_map.get(rustfs_config::MQTT_TLS_CLIENT_KEY).map(String::as_str),
                kv_map.get(rustfs_config::MQTT_TLS_TRUST_LEAF_AS_CA).map(String::as_str),
                kv_map.get(rustfs_config::MQTT_WS_PATH_ALLOWLIST).map(String::as_str),
            )
            .map_err(|e| s3_error!(InvalidArgument, "invalid MQTT TLS settings: {}", e))?;
            let parsed_broker = Url::parse(endpoint).map_err(|e| s3_error!(InvalidArgument, "invalid broker URL: {}", e))?;
            rustfs_targets::target::mqtt::validate_mqtt_broker_url(&parsed_broker, &tls)
                .map_err(|e| s3_error!(InvalidArgument, "{}", e))?;
            check_mqtt_broker_available_with_tls(parsed_broker.as_str(), topic, username, password, &tls)
                .await
                .map_err(|e| match e {
                    TargetError::Configuration(_) => s3_error!(InvalidArgument, "{}", e),
                    _ => s3_error!(InvalidArgument, "MQTT broker check failed: {}", e),
                })?;

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
        } else if target_type == NOTIFY_NATS_SUB_SYS {
            if let Some(queue_dir) = kv_map.get("queue_dir") {
                validate_queue_dir(queue_dir.as_str()).await?;
            }
            let mut kvs = rustfs_ecstore::config::KVS::new();
            for (key, value) in &kv_map {
                kvs.insert(key.clone(), value.clone());
            }
            let args = build_nats_args(&kvs, EVENT_DEFAULT_DIR, rustfs_targets::target::TargetType::NotifyEvent)
                .map_err(|e| s3_error!(InvalidArgument, "{}", e))?;
            check_nats_server_available(&args).await.map_err(|e| match e {
                TargetError::Configuration(_) => s3_error!(InvalidArgument, "{}", e),
                _ => s3_error!(InvalidArgument, "NATS server check failed: {}", e),
            })?;
        } else if target_type == NOTIFY_PULSAR_SUB_SYS {
            if let Some(queue_dir) = kv_map.get("queue_dir") {
                validate_queue_dir(queue_dir.as_str()).await?;
            }
            let mut kvs = rustfs_ecstore::config::KVS::new();
            for (key, value) in &kv_map {
                kvs.insert(key.clone(), value.clone());
            }
            let args = build_pulsar_args(&kvs, EVENT_DEFAULT_DIR, rustfs_targets::target::TargetType::NotifyEvent)
                .map_err(|e| s3_error!(InvalidArgument, "{}", e))?;
            check_pulsar_broker_available(&args).await.map_err(|e| match e {
                TargetError::Configuration(_) => s3_error!(InvalidArgument, "{}", e),
                _ => s3_error!(InvalidArgument, "Pulsar broker check failed: {}", e),
            })?;
        }

        let mut kvs = rustfs_ecstore::config::KVS::new();
        for (key, value) in kv_map {
            kvs.insert(key, value);
        }
        kvs.insert(ENABLE_KEY.to_string(), EnableState::On.to_string());

        info!("Setting target config for type '{}', name '{}'", target_type, target_name);
        ns.set_target_config(target_type, target_name, kvs)
            .await
            .map_err(|e| s3_error!(InternalError, "failed to set target config: {}", e))?;

        Ok(build_response(StatusCode::OK, Body::empty(), req.headers.get("x-request-id")))
    }
}

pub struct ListNotificationTargets {}
#[async_trait::async_trait]
impl Operation for ListNotificationTargets {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let span = Span::current();
        let _enter = span.enter();
        authorize_notification_admin_request(&req, AdminAction::GetBucketTargetAction).await?;
        let ns = get_notification_system()?;

        let targets = ns.get_target_values().await;
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
                ((target.id().id, target.id().name), status.to_string())
            });
        }

        let mut runtime_statuses = HashMap::new();
        while let Some((key, status)) = futures.next().await {
            runtime_statuses.insert(key, status);
        }
        let config = ns.config.read().await.clone();
        let notification_endpoints = merge_notification_endpoints(&config, runtime_statuses);

        let data = serde_json::to_vec(&NotificationEndpointsResponse { notification_endpoints })
            .map_err(|e| s3_error!(InternalError, "failed to serialize targets: {}", e))?;

        Ok(build_response(StatusCode::OK, Body::from(data), req.headers.get("x-request-id")))
    }
}

pub struct ListTargetsArns {}
#[async_trait::async_trait]
impl Operation for ListTargetsArns {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let span = Span::current();
        let _enter = span.enter();
        authorize_notification_admin_request(&req, AdminAction::GetBucketTargetAction).await?;
        let ns = get_notification_system()?;

        let targets = ns.get_target_values().await;
        let region = req
            .region
            .clone()
            .ok_or_else(|| s3_error!(InvalidRequest, "region not found"))?;
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
                (target.id(), status.to_string())
            });
        }

        let mut target_statuses = Vec::new();
        while let Some(target_status) = futures.next().await {
            target_statuses.push(target_status);
        }

        let data_target_arn_list = collect_online_target_arns(region.as_str(), target_statuses);

        let data = serde_json::to_vec(&data_target_arn_list)
            .map_err(|e| s3_error!(InternalError, "failed to serialize targets: {}", e))?;

        Ok(build_response(StatusCode::OK, Body::from(data), req.headers.get("x-request-id")))
    }
}

pub struct RemoveNotificationTarget {}
#[async_trait::async_trait]
impl Operation for RemoveNotificationTarget {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let span = Span::current();
        let _enter = span.enter();
        let (target_type, target_name) = extract_target_params(&params)?;

        authorize_notification_admin_request(&req, AdminAction::SetBucketTargetAction).await?;
        let ns = get_notification_system()?;
        let config_snapshot = ns.config.read().await.clone();
        if let Some(reason) = target_mutation_block_reason(&config_snapshot, target_type, target_name) {
            return Err(s3_error!(InvalidRequest, "{reason}"));
        }

        info!("Removing target config for type '{}', name '{}'", target_type, target_name);
        ns.remove_target_config(target_type, target_name)
            .await
            .map_err(|e| s3_error!(InternalError, "failed to remove target config: {}", e))?;

        Ok(build_response(StatusCode::OK, Body::empty(), req.headers.get("x-request-id")))
    }
}

fn extract_param<'a>(params: &'a Params<'_, '_>, key: &str) -> S3Result<&'a str> {
    params
        .get(key)
        .ok_or_else(|| s3_error!(InvalidArgument, "missing required parameter: '{}'", key))
}

fn extract_target_params<'a>(params: &'a Params<'_, '_>) -> S3Result<(&'a str, &'a str)> {
    let target_type = extract_param(params, "target_type")?;
    if notification_service_name(target_type).is_none() {
        return Err(s3_error!(InvalidArgument, "unsupported target type: '{}'", target_type));
    }
    let target_name = extract_param(params, "target_name")?;
    Ok((target_type, target_name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_ecstore::config::{KV, KVS};
    use rustfs_targets::arn::TargetID;
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
    fn merge_notification_endpoints_keeps_configured_targets_after_runtime_loss() {
        let mut cfg_map = HashMap::new();
        cfg_map.insert(
            NOTIFY_WEBHOOK_SUB_SYS.to_string(),
            HashMap::from([("webhook-a".to_string(), enabled_kvs("on"))]),
        );
        cfg_map.insert(
            NOTIFY_MQTT_SUB_SYS.to_string(),
            HashMap::from([("mqtt-a".to_string(), enabled_kvs("on"))]),
        );
        let config = Config(cfg_map);

        let runtime = HashMap::from([(("webhook-a".to_string(), "webhook".to_string()), "online".to_string())]);
        let merged = merge_notification_endpoints(&config, runtime);

        let mqtt = merged
            .iter()
            .find(|entry| entry.account_id == "mqtt-a" && entry.service == "mqtt")
            .expect("mqtt-a should be present");
        assert_eq!(mqtt.status, "offline");
        assert_eq!(mqtt.source, NotificationEndpointSource::Config);

        let webhook = merged
            .iter()
            .find(|entry| entry.account_id == "webhook-a" && entry.service == "webhook")
            .expect("webhook-a should be present");
        assert_eq!(webhook.status, "online");
        assert_eq!(webhook.source, NotificationEndpointSource::Config);
    }

    #[test]
    fn merge_notification_endpoints_skips_disabled_and_default_entries() {
        let mut webhook_targets = HashMap::new();
        webhook_targets.insert(DEFAULT_DELIMITER.to_string(), enabled_kvs("on"));
        webhook_targets.insert("webhook-disabled".to_string(), enabled_kvs("off"));
        webhook_targets.insert("webhook-enabled".to_string(), enabled_kvs("on"));
        let config = Config(HashMap::from([(NOTIFY_WEBHOOK_SUB_SYS.to_string(), webhook_targets)]));

        let runtime = HashMap::from([
            (("webhook-enabled".to_string(), "webhook".to_string()), "online".to_string()),
            (("env-only".to_string(), "mqtt".to_string()), "offline".to_string()),
        ]);
        let merged = merge_notification_endpoints(&config, runtime);

        let env_only = merged
            .iter()
            .find(|entry| entry.account_id == "env-only" && entry.service == "mqtt")
            .expect("env-only should be present");
        assert_eq!(env_only.status, "offline");
        assert_eq!(env_only.source, NotificationEndpointSource::Runtime);

        let enabled = merged
            .iter()
            .find(|entry| entry.account_id == "webhook-enabled" && entry.service == "webhook")
            .expect("webhook-enabled should be present");
        assert_eq!(enabled.status, "online");
        assert_eq!(enabled.source, NotificationEndpointSource::Config);
    }

    #[test]
    fn merge_notification_endpoints_marks_env_and_mixed_sources() {
        let config = Config(HashMap::from([
            (
                NOTIFY_WEBHOOK_SUB_SYS.to_string(),
                HashMap::from([("mixed-target".to_string(), enabled_kvs("on"))]),
            ),
            (
                NOTIFY_MQTT_SUB_SYS.to_string(),
                HashMap::from([("config-target".to_string(), enabled_kvs("on"))]),
            ),
        ]));

        with_vars(
            [
                ("RUSTFS_NOTIFY_WEBHOOK_ENDPOINT_MIXED-TARGET", Some("https://example.com/hook")),
                ("RUSTFS_NOTIFY_WEBHOOK_ENABLE_ENV-ONLY", Some("on")),
                ("RUSTFS_NOTIFY_WEBHOOK_ENDPOINT_ENV-ONLY", Some("https://example.com/env")),
            ],
            || {
                let runtime = HashMap::from([
                    (("mixed-target".to_string(), "webhook".to_string()), "online".to_string()),
                    (("env-only".to_string(), "webhook".to_string()), "online".to_string()),
                ]);
                let merged = merge_notification_endpoints(&config, runtime);

                let mixed = merged
                    .iter()
                    .find(|entry| entry.account_id == "mixed-target")
                    .expect("mixed target should be present");
                assert_eq!(mixed.source, NotificationEndpointSource::Mixed);

                let env_only = merged
                    .iter()
                    .find(|entry| entry.account_id == "env-only")
                    .expect("env-only target should be present");
                assert_eq!(env_only.source, NotificationEndpointSource::Env);

                let config_only = merged
                    .iter()
                    .find(|entry| entry.account_id == "config-target")
                    .expect("config target should be present");
                assert_eq!(config_only.source, NotificationEndpointSource::Config);
            },
        );
    }

    #[test]
    fn target_mutation_block_reason_rejects_env_managed_target() {
        with_vars(
            [
                ("RUSTFS_NOTIFY_WEBHOOK_ENABLE_PRIMARY", Some("on")),
                ("RUSTFS_NOTIFY_WEBHOOK_ENDPOINT_PRIMARY", Some("https://example.com/hook")),
            ],
            || {
                let config = Config(HashMap::new());
                let reason = target_mutation_block_reason(&config, NOTIFY_WEBHOOK_SUB_SYS, "primary");
                assert!(reason.is_some());
                assert!(reason.unwrap().contains("managed by environment variables"));
            },
        );
    }

    #[test]
    fn target_mutation_block_reason_rejects_mixed_target() {
        with_var("RUSTFS_NOTIFY_WEBHOOK_ENDPOINT_PRIMARY", Some("https://example.com/hook"), || {
            let config = Config(HashMap::from([(
                NOTIFY_WEBHOOK_SUB_SYS.to_string(),
                HashMap::from([("primary".to_string(), enabled_kvs("on"))]),
            )]));
            let reason = target_mutation_block_reason(&config, NOTIFY_WEBHOOK_SUB_SYS, "primary");
            assert!(reason.is_some());
            assert!(reason.unwrap().contains("both persisted config and environment variables"));
        });
    }

    #[test]
    fn target_mutation_block_reason_allows_config_only_target() {
        let target_name = "config-only-target";
        let config = Config(HashMap::from([(
            NOTIFY_WEBHOOK_SUB_SYS.to_string(),
            HashMap::from([(target_name.to_string(), enabled_kvs("on"))]),
        )]));
        assert!(target_mutation_block_reason(&config, NOTIFY_WEBHOOK_SUB_SYS, target_name).is_none());
    }

    #[test]
    fn merge_notification_endpoints_marks_disabled_config_with_env_override_as_mixed() {
        let config = Config(HashMap::from([(
            NOTIFY_WEBHOOK_SUB_SYS.to_string(),
            HashMap::from([("mixed-disabled".to_string(), enabled_kvs("off"))]),
        )]));

        with_vars(
            [
                ("RUSTFS_NOTIFY_WEBHOOK_ENABLE_MIXED-DISABLED", Some("on")),
                ("RUSTFS_NOTIFY_WEBHOOK_ENDPOINT_MIXED-DISABLED", Some("https://example.com/hook")),
            ],
            || {
                let merged = merge_notification_endpoints(&config, HashMap::new());
                let mixed = merged
                    .iter()
                    .find(|entry| entry.account_id == "mixed-disabled")
                    .expect("mixed target should be present");
                assert_eq!(mixed.source, NotificationEndpointSource::Mixed);
                assert_eq!(mixed.status, "offline");
            },
        );
    }

    #[test]
    fn merge_notification_endpoints_includes_env_only_target_without_runtime_status() {
        let config = Config(HashMap::new());

        with_vars(
            [
                ("RUSTFS_NOTIFY_WEBHOOK_ENABLE_ENV-ONLY", Some("on")),
                ("RUSTFS_NOTIFY_WEBHOOK_ENDPOINT_ENV-ONLY", Some("https://example.com/env")),
            ],
            || {
                let merged = merge_notification_endpoints(&config, HashMap::new());
                let env_only = merged
                    .iter()
                    .find(|entry| entry.account_id == "env-only")
                    .expect("env-only target should be present");
                assert_eq!(env_only.source, NotificationEndpointSource::Env);
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

        let err = collect_validated_key_values(&key_values, &allowed_keys, NOTIFY_WEBHOOK_SUB_SYS).unwrap_err();
        assert!(err.to_string().contains("duplicate key"));
    }

    #[test]
    fn merge_notification_endpoints_marks_mixed_with_case_insensitive_instance_id() {
        let config = Config(HashMap::from([(
            NOTIFY_WEBHOOK_SUB_SYS.to_string(),
            HashMap::from([("PrimaryCase".to_string(), enabled_kvs("on"))]),
        )]));

        with_vars(
            [
                ("RUSTFS_NOTIFY_WEBHOOK_ENABLE_PRIMARYCASE", Some("on")),
                ("RUSTFS_NOTIFY_WEBHOOK_ENDPOINT_PRIMARYCASE", Some("https://example.com/hook")),
            ],
            || {
                let runtime = HashMap::from([(("PrimaryCase".to_string(), "webhook".to_string()), "online".to_string())]);
                let merged = merge_notification_endpoints(&config, runtime);
                let mixed = merged
                    .iter()
                    .find(|entry| entry.account_id == "PrimaryCase" && entry.service == "webhook")
                    .expect("mixed target should be present");
                assert_eq!(mixed.source, NotificationEndpointSource::Mixed);
            },
        );
    }

    #[test]
    fn collect_online_target_arns_filters_offline_targets() {
        let arns = collect_online_target_arns(
            "us-east-1",
            vec![
                (TargetID::new("webhook-a".to_string(), "webhook".to_string()), "online".to_string()),
                (TargetID::new("mqtt-a".to_string(), "mqtt".to_string()), "offline".to_string()),
            ],
        );

        assert_eq!(arns, vec!["arn:rustfs:sqs:us-east-1:webhook-a:webhook".to_string()]);
    }

    #[test]
    fn target_mutation_block_reason_allows_case_insensitive_config_target_lookup() {
        let config = Config(HashMap::from([(
            NOTIFY_WEBHOOK_SUB_SYS.to_string(),
            HashMap::from([("PrimaryCase".to_string(), enabled_kvs("on"))]),
        )]));

        with_vars(
            [
                ("RUSTFS_NOTIFY_WEBHOOK_ENABLE_PRIMARYCASE", None::<&str>),
                ("RUSTFS_NOTIFY_WEBHOOK_ENDPOINT_PRIMARYCASE", None::<&str>),
            ],
            || {
                assert!(target_mutation_block_reason(&config, NOTIFY_WEBHOOK_SUB_SYS, "primarycase").is_none());
            },
        );
    }

    #[test]
    fn notification_target_handlers_require_admin_authorization_contract() {
        let src = include_str!("event.rs");
        let put_block =
            extract_block_between_markers(src, "impl Operation for NotificationTarget", "pub struct ListNotificationTargets");
        let list_block =
            extract_block_between_markers(src, "impl Operation for ListNotificationTargets", "pub struct ListTargetsArns");
        let arns_block =
            extract_block_between_markers(src, "impl Operation for ListTargetsArns", "pub struct RemoveNotificationTarget");
        let delete_block = extract_block_between_markers(src, "impl Operation for RemoveNotificationTarget", "fn extract_param");

        assert!(
            put_block.contains("authorize_notification_admin_request(&req, AdminAction::SetBucketTargetAction).await?;"),
            "notification target writes should require SetBucketTargetAction"
        );
        assert!(
            list_block.contains("authorize_notification_admin_request(&req, AdminAction::GetBucketTargetAction).await?;"),
            "notification target list should require GetBucketTargetAction"
        );
        assert!(
            arns_block.contains("authorize_notification_admin_request(&req, AdminAction::GetBucketTargetAction).await?;"),
            "notification target arn listing should require GetBucketTargetAction"
        );
        assert!(
            delete_block.contains("authorize_notification_admin_request(&req, AdminAction::SetBucketTargetAction).await?;"),
            "notification target deletion should require SetBucketTargetAction"
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
