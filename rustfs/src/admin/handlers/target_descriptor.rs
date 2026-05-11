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

use futures::StreamExt;
use futures::future::BoxFuture;
use hashbrown::HashSet as HbHashSet;
use http::{HeaderMap, HeaderValue, StatusCode};
use rustfs_config::{
    AMQP_QUEUE_DIR, ENABLE_KEY, EnableState, KAFKA_BROKERS, KAFKA_QUEUE_DIR, KAFKA_TOPIC, MQTT_BROKER, MQTT_PASSWORD, MQTT_QOS,
    MQTT_TLS_CA, MQTT_TLS_CLIENT_CERT, MQTT_TLS_CLIENT_KEY, MQTT_TLS_POLICY, MQTT_TLS_TRUST_LEAF_AS_CA, MQTT_TOPIC,
    MQTT_USERNAME, MQTT_WS_PATH_ALLOWLIST, MYSQL_QUEUE_DIR, POSTGRES_QUEUE_DIR, REDIS_QUEUE_DIR,
};
use rustfs_ecstore::config::{Config, KVS};
use rustfs_targets::SharedTarget;
use rustfs_targets::{
    BuiltinTargetAdminDescriptor, TargetAdminMetadata, TargetDomain, TargetError, TargetRequestValidator,
    check_amqp_broker_available, check_kafka_broker_available, check_mqtt_broker_available_with_tls,
    check_mysql_server_available, check_nats_server_available, check_postgres_server_available, check_pulsar_broker_available,
    check_redis_server_available,
    config::{
        build_amqp_args, build_kafka_args, build_mysql_args, build_nats_args, build_postgres_args, build_pulsar_args,
        build_redis_args, collect_env_target_instance_ids, validate_redis_config,
    },
    target::{TargetType, mqtt::MQTTTlsConfig},
};
use s3s::{Body, S3Response, S3Result, header::CONTENT_TYPE, s3_error};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::io::{Error, ErrorKind};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::time::{Duration, sleep, timeout};
use url::Url;

pub(crate) type EndpointKey = (String, String);
type AdminRequestValidatorFn =
    Arc<dyn for<'a> Fn(&'a HashMap<String, String>, &'a str) -> BoxFuture<'a, S3Result<()>> + Send + Sync>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum TargetEndpointSource {
    Config,
    Env,
    Mixed,
    Runtime,
}

pub(crate) struct MergedTargetEndpoint {
    pub account_id: String,
    pub service: String,
    pub status: String,
    pub source: TargetEndpointSource,
}

#[derive(Clone)]
pub(crate) struct AdminTargetSpec {
    pub subsystem: &'static str,
    pub service: &'static str,
    pub valid_keys: &'static [&'static str],
    valid_keys_set: Arc<HashSet<String>>,
    validator: AdminRequestValidatorFn,
}

pub(crate) fn admin_target_spec_from_builtin(descriptor: &BuiltinTargetAdminDescriptor) -> AdminTargetSpec {
    let admin = descriptor.admin_metadata();
    AdminTargetSpec {
        subsystem: admin.subsystem(),
        service: descriptor.manifest().target_type,
        valid_keys: descriptor.valid_fields(),
        valid_keys_set: Arc::new(descriptor.valid_fields().iter().map(|key| (*key).to_string()).collect()),
        validator: validator_from_metadata(admin),
    }
}

fn validator_from_metadata(metadata: TargetAdminMetadata) -> AdminRequestValidatorFn {
    match metadata.request_validator() {
        TargetRequestValidator::Webhook => Arc::new(validate_webhook_request_entry),
        TargetRequestValidator::Mqtt => Arc::new(validate_mqtt_request_entry),
        TargetRequestValidator::Amqp(target_type) => {
            domain_request_validator(TargetDomain::from(target_type), validate_amqp_request)
        }
        TargetRequestValidator::Kafka(target_type) => {
            domain_request_validator(TargetDomain::from(target_type), validate_kafka_request)
        }
        TargetRequestValidator::MySql(target_type) => {
            Arc::new(move |kv_map, default_queue_dir| validate_mysql_request_entry(kv_map, default_queue_dir, target_type))
        }
        TargetRequestValidator::Nats(target_type) => {
            domain_request_validator(TargetDomain::from(target_type), validate_nats_request)
        }
        TargetRequestValidator::Postgres(target_type) => {
            domain_request_validator(TargetDomain::from(target_type), validate_postgres_request)
        }
        TargetRequestValidator::Pulsar(target_type) => {
            domain_request_validator(TargetDomain::from(target_type), validate_pulsar_request)
        }
        TargetRequestValidator::Redis {
            default_channel,
            target_type,
        } => redis_request_validator(TargetDomain::from(target_type), default_channel),
    }
}

fn domain_request_validator(
    domain: TargetDomain,
    validator: for<'a> fn(&'a HashMap<String, String>, &'a str, TargetDomain) -> BoxFuture<'a, S3Result<()>>,
) -> AdminRequestValidatorFn {
    Arc::new(move |kv_map, default_queue_dir| validator(kv_map, default_queue_dir, domain))
}

fn redis_request_validator(domain: TargetDomain, default_channel: &'static str) -> AdminRequestValidatorFn {
    Arc::new(move |kv_map, default_queue_dir| {
        Box::pin(validate_redis_request(kv_map, default_queue_dir, domain, default_channel))
    })
}

impl std::fmt::Debug for AdminTargetSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdminTargetSpec")
            .field("subsystem", &self.subsystem)
            .field("service", &self.service)
            .field("valid_keys", &self.valid_keys)
            .finish_non_exhaustive()
    }
}

impl AdminTargetSpec {
    pub(crate) async fn validate_request(&self, kv_map: &HashMap<String, String>, default_queue_dir: &str) -> S3Result<()> {
        (self.validator)(kv_map, default_queue_dir).await
    }
}

pub(crate) fn normalized_endpoint_key(account_id: &str, service: &str) -> EndpointKey {
    (account_id.to_lowercase(), service.to_string())
}

pub(crate) fn target_spec<'a>(specs: &'a [AdminTargetSpec], target_type: &str) -> Option<&'a AdminTargetSpec> {
    specs.iter().find(|spec| spec.subsystem == target_type)
}

pub(crate) fn target_service_name(specs: &[AdminTargetSpec], target_type: &str) -> Option<&'static str> {
    target_spec(specs, target_type).map(|spec| spec.service)
}

pub(crate) fn extract_supported_target_params<'a>(
    specs: &[AdminTargetSpec],
    params: &'a matchit::Params<'_, '_>,
    unsupported_target_label: &str,
) -> S3Result<(&'a str, &'a str)> {
    let target_type = params
        .get("target_type")
        .ok_or_else(|| s3_error!(InvalidArgument, "missing required parameter: 'target_type'"))?;
    if target_service_name(specs, target_type).is_none() {
        return Err(s3_error!(
            InvalidArgument,
            "unsupported {} target type: '{}'",
            unsupported_target_label,
            target_type
        ));
    }
    let target_name = params
        .get("target_name")
        .ok_or_else(|| s3_error!(InvalidArgument, "missing required parameter: 'target_name'"))?;
    Ok((target_type, target_name))
}

pub(crate) fn collect_configured_endpoint_keys(specs: &[AdminTargetSpec], config: &Config) -> Vec<EndpointKey> {
    let mut endpoints = Vec::new();
    for spec in specs {
        let Some(targets) = config.0.get(spec.subsystem) else {
            continue;
        };

        for (target_name, kvs) in targets {
            if target_name == rustfs_config::DEFAULT_DELIMITER {
                continue;
            }
            let enabled = kvs.lookup(ENABLE_KEY).as_deref().map(config_enable_is_on).unwrap_or(false);
            if enabled {
                endpoints.push((target_name.clone(), spec.service.to_string()));
            }
        }
    }
    endpoints
}

pub(crate) fn collect_config_entry_keys(specs: &[AdminTargetSpec], config: &Config) -> HbHashSet<EndpointKey> {
    let mut endpoints = HbHashSet::new();
    for spec in specs {
        let Some(targets) = config.0.get(spec.subsystem) else {
            continue;
        };

        for target_name in targets.keys() {
            if target_name == rustfs_config::DEFAULT_DELIMITER {
                continue;
            }
            endpoints.insert(normalized_endpoint_key(target_name, spec.service));
        }
    }
    endpoints
}

pub(crate) fn collect_env_endpoint_keys(specs: &[AdminTargetSpec], route_prefix: &str) -> HbHashSet<EndpointKey> {
    let mut endpoints = HbHashSet::new();
    for spec in specs {
        for instance_id in collect_env_target_instance_ids(route_prefix, spec.service, spec.valid_keys_set.as_ref()) {
            if instance_id != rustfs_config::DEFAULT_DELIMITER && !instance_id.is_empty() {
                endpoints.insert(normalized_endpoint_key(&instance_id, spec.service));
            }
        }
    }
    endpoints
}

pub(crate) fn classify_endpoint_source(
    config_targets: &HbHashSet<EndpointKey>,
    env_targets: &HbHashSet<EndpointKey>,
    key: &EndpointKey,
) -> TargetEndpointSource {
    match (config_targets.contains(key), env_targets.contains(key)) {
        (true, true) => TargetEndpointSource::Mixed,
        (true, false) => TargetEndpointSource::Config,
        (false, true) => TargetEndpointSource::Env,
        (false, false) => TargetEndpointSource::Runtime,
    }
}

fn collect_endpoint_sources(
    specs: &[AdminTargetSpec],
    route_prefix: &str,
    config: &Config,
) -> (HbHashSet<EndpointKey>, HbHashSet<EndpointKey>) {
    (collect_config_entry_keys(specs, config), collect_env_endpoint_keys(specs, route_prefix))
}

pub(crate) fn endpoint_source(
    specs: &[AdminTargetSpec],
    route_prefix: &str,
    config: &Config,
    target_type: &str,
    target_name: &str,
) -> TargetEndpointSource {
    let (config_targets, env_targets) = collect_endpoint_sources(specs, route_prefix, config);
    let service = target_service_name(specs, target_type).unwrap_or_default();
    let key = normalized_endpoint_key(target_name, service);
    classify_endpoint_source(&config_targets, &env_targets, &key)
}

pub(crate) fn target_mutation_block_reason(
    specs: &[AdminTargetSpec],
    route_prefix: &str,
    config: &Config,
    target_type: &str,
    target_name: &str,
    target_label: &str,
) -> Option<String> {
    match endpoint_source(specs, route_prefix, config, target_type, target_name) {
        TargetEndpointSource::Env => Some(format!(
            "{} '{}' is managed by environment variables and cannot be modified from the console",
            target_label, target_name
        )),
        TargetEndpointSource::Mixed => Some(format!(
            "{} '{}' is configured by both persisted config and environment variables; remove the environment variables first",
            target_label, target_name
        )),
        TargetEndpointSource::Config | TargetEndpointSource::Runtime => None,
    }
}

pub(crate) fn target_module_disabled_reason(module_name: &str, env_key: &str, enabled: bool, action: &str) -> Option<String> {
    (!enabled).then(|| {
        format!(
            "{module_name} module is disabled; enable the {module_name} module first in the console or set {env_key}=true before {action}"
        )
    })
}

pub(crate) fn build_json_response(
    status: StatusCode,
    body: Body,
    request_id: Option<&HeaderValue>,
) -> S3Response<(StatusCode, Body)> {
    let mut header = HeaderMap::new();
    header.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    if let Some(v) = request_id {
        header.insert("x-request-id", v.clone());
    }
    S3Response::with_headers((status, body), header)
}

pub(crate) async fn collect_runtime_statuses<E>(targets: Vec<SharedTarget<E>>) -> HashMap<EndpointKey, String>
where
    E: Send + Sync + 'static + Clone + serde::Serialize + serde::de::DeserializeOwned,
{
    let semaphore = Arc::new(Semaphore::new(10));
    let mut futures = futures::stream::FuturesUnordered::new();

    for target in targets {
        let sem = Arc::clone(&semaphore);
        futures.push(async move {
            let _permit = sem.acquire().await;
            let status = match tokio::time::timeout(Duration::from_secs(3), target.is_active()).await {
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

    runtime_statuses
}

pub(crate) fn merge_target_endpoints(
    specs: &[AdminTargetSpec],
    route_prefix: &str,
    config: &Config,
    runtime_statuses: HashMap<EndpointKey, String>,
) -> Vec<MergedTargetEndpoint> {
    let mut endpoints = Vec::new();
    let mut seen = HashSet::new();
    let configured_keys = collect_configured_endpoint_keys(specs, config);
    let (config_targets, env_targets) = collect_endpoint_sources(specs, route_prefix, config);
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

        endpoints.push(MergedTargetEndpoint {
            account_id: key.0,
            service: key.1,
            status,
            source: classify_endpoint_source(&config_targets, &env_targets, &normalized),
        });
    }

    for (normalized, (account_id, service, status)) in normalized_runtime_statuses {
        if seen.insert(normalized.clone()) {
            endpoints.push(MergedTargetEndpoint {
                account_id,
                service,
                status,
                source: classify_endpoint_source(&config_targets, &env_targets, &normalized),
            });
        }
    }

    for key in &env_targets {
        if !seen.insert(key.clone()) {
            continue;
        }

        endpoints.push(MergedTargetEndpoint {
            account_id: key.0.clone(),
            service: key.1.clone(),
            status: "offline".to_string(),
            source: classify_endpoint_source(&config_targets, &env_targets, key),
        });
    }

    endpoints.sort_by(|a, b| a.service.cmp(&b.service).then_with(|| a.account_id.cmp(&b.account_id)));
    endpoints
}

pub(crate) fn allowed_target_keys(specs: &[AdminTargetSpec], target_type: &str) -> HashSet<&'static str> {
    target_spec(specs, target_type)
        .map(|spec| spec.valid_keys.iter().copied().collect())
        .unwrap_or_default()
}

pub(crate) fn collect_validated_key_values<'a, I>(
    key_values: I,
    allowed_keys: &HashSet<&str>,
    target_type: &str,
    target_label: &str,
) -> S3Result<HashMap<String, String>>
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    let mut kv_map = HashMap::new();
    let mut seen = HashSet::new();

    for (key, value) in key_values {
        if !allowed_keys.contains(key) {
            return Err(s3_error!(
                InvalidArgument,
                "key '{}' not allowed for {} type '{}'",
                key,
                target_label,
                target_type
            ));
        }

        if !seen.insert(key) {
            return Err(s3_error!(InvalidArgument, "duplicate key '{}' in request body", key));
        }

        kv_map.insert(key.to_string(), value.to_string());
    }

    Ok(kv_map)
}

pub(crate) async fn validate_queue_dir(queue_dir: &str) -> S3Result<()> {
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

pub(crate) async fn validate_target_request(
    spec: &AdminTargetSpec,
    kv_map: &HashMap<String, String>,
    default_queue_dir: &str,
) -> S3Result<()> {
    spec.validate_request(kv_map, default_queue_dir).await
}

pub(crate) async fn build_enabled_target_kvs<'a, I>(
    specs: &[AdminTargetSpec],
    key_values: I,
    target_type: &str,
    default_queue_dir: &str,
    target_label: &str,
) -> S3Result<KVS>
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    let allowed_keys = allowed_target_keys(specs, target_type);
    let kv_map = collect_validated_key_values(key_values, &allowed_keys, target_type, target_label)?;
    let spec = target_spec(specs, target_type)
        .ok_or_else(|| s3_error!(InvalidArgument, "unsupported target type: '{}'", target_type))?;
    timeout(Duration::from_secs(10), validate_target_request(spec, &kv_map, default_queue_dir))
        .await
        .map_err(|_| s3_error!(InvalidArgument, "target validation timed out"))??;

    let mut kvs = KVS::new();
    for (key, value) in kv_map {
        kvs.insert(key, value);
    }
    kvs.insert(ENABLE_KEY.to_string(), EnableState::On.to_string());
    Ok(kvs)
}

fn config_enable_is_on(value: &str) -> bool {
    matches!(value.trim().to_ascii_lowercase().as_str(), "on" | "true" | "yes" | "1")
}

async fn retry_with_backoff<F, Fut, T>(mut operation: F, max_attempts: usize, base_delay: Duration) -> Result<T, Error>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, Error>>,
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

async fn validate_webhook_request(kv_map: &HashMap<String, String>) -> S3Result<()> {
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
    Ok(())
}

fn validate_webhook_request_entry<'a>(
    kv_map: &'a HashMap<String, String>,
    _default_queue_dir: &'a str,
) -> BoxFuture<'a, S3Result<()>> {
    Box::pin(validate_webhook_request(kv_map))
}

async fn validate_mqtt_request(kv_map: &HashMap<String, String>) -> S3Result<()> {
    let endpoint = kv_map
        .get(MQTT_BROKER)
        .map(String::as_str)
        .ok_or_else(|| s3_error!(InvalidArgument, "broker endpoint is required"))?;
    let topic = kv_map
        .get(MQTT_TOPIC)
        .map(String::as_str)
        .ok_or_else(|| s3_error!(InvalidArgument, "topic is required"))?;
    let username = kv_map.get(MQTT_USERNAME).map(String::as_str);
    let password = kv_map.get(MQTT_PASSWORD).map(String::as_str);
    let tls = MQTTTlsConfig::from_values(
        kv_map.get(MQTT_TLS_POLICY).map(String::as_str),
        kv_map.get(MQTT_TLS_CA).map(String::as_str),
        kv_map.get(MQTT_TLS_CLIENT_CERT).map(String::as_str),
        kv_map.get(MQTT_TLS_CLIENT_KEY).map(String::as_str),
        kv_map.get(MQTT_TLS_TRUST_LEAF_AS_CA).map(String::as_str),
        kv_map.get(MQTT_WS_PATH_ALLOWLIST).map(String::as_str),
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
        if let Some(qos) = kv_map.get(MQTT_QOS) {
            match qos.parse::<u8>() {
                Ok(1) | Ok(2) => {}
                Ok(0) => return Err(s3_error!(InvalidArgument, "qos should be 1 or 2 if queue_dir is set")),
                _ => return Err(s3_error!(InvalidArgument, "qos must be an integer 0, 1, or 2")),
            }
        }
    }

    Ok(())
}

fn validate_mqtt_request_entry<'a>(
    kv_map: &'a HashMap<String, String>,
    _default_queue_dir: &'a str,
) -> BoxFuture<'a, S3Result<()>> {
    Box::pin(validate_mqtt_request(kv_map))
}

fn validate_mysql_request_entry<'a>(
    kv_map: &'a HashMap<String, String>,
    default_queue_dir: &'a str,
    target_type: TargetType,
) -> BoxFuture<'a, S3Result<()>> {
    Box::pin(validate_mysql_request(kv_map, default_queue_dir, target_type))
}

fn validate_nats_request<'a>(
    kv_map: &'a HashMap<String, String>,
    default_queue_dir: &'a str,
    domain: TargetDomain,
) -> BoxFuture<'a, S3Result<()>> {
    Box::pin(async move { validate_nats_request_impl(kv_map, default_queue_dir, domain).await })
}

async fn validate_nats_request_impl(
    kv_map: &HashMap<String, String>,
    default_queue_dir: &str,
    domain: TargetDomain,
) -> S3Result<()> {
    if let Some(queue_dir) = kv_map.get("queue_dir") {
        validate_queue_dir(queue_dir.as_str()).await?;
    }
    let args = build_nats_args(&to_kvs(kv_map), default_queue_dir, domain.runtime_target_type())
        .map_err(|e| s3_error!(InvalidArgument, "{}", e))?;
    check_nats_server_available(&args).await.map_err(|e| match e {
        TargetError::Configuration(_) => s3_error!(InvalidArgument, "{}", e),
        _ => s3_error!(InvalidArgument, "NATS server check failed: {}", e),
    })
}

fn validate_kafka_request<'a>(
    kv_map: &'a HashMap<String, String>,
    default_queue_dir: &'a str,
    domain: TargetDomain,
) -> BoxFuture<'a, S3Result<()>> {
    Box::pin(async move { validate_kafka_request_impl(kv_map, default_queue_dir, domain).await })
}

async fn validate_kafka_request_impl(
    kv_map: &HashMap<String, String>,
    default_queue_dir: &str,
    domain: TargetDomain,
) -> S3Result<()> {
    if let Some(queue_dir) = kv_map.get(KAFKA_QUEUE_DIR) {
        validate_queue_dir(queue_dir.as_str()).await?;
    }

    if !kv_map.contains_key(KAFKA_BROKERS) {
        return Err(s3_error!(InvalidArgument, "Kafka brokers are required"));
    }
    if !kv_map.contains_key(KAFKA_TOPIC) {
        return Err(s3_error!(InvalidArgument, "Kafka topic is required"));
    }

    let args = build_kafka_args(&to_kvs(kv_map), default_queue_dir, domain.runtime_target_type())
        .map_err(|e| s3_error!(InvalidArgument, "{}", e))?;
    check_kafka_broker_available(&args).await.map_err(|e| match e {
        TargetError::Configuration(_) => s3_error!(InvalidArgument, "{}", e),
        _ => s3_error!(InvalidArgument, "Kafka broker check failed: {}", e),
    })
}

fn validate_amqp_request<'a>(
    kv_map: &'a HashMap<String, String>,
    default_queue_dir: &'a str,
    domain: TargetDomain,
) -> BoxFuture<'a, S3Result<()>> {
    Box::pin(async move { validate_amqp_request_impl(kv_map, default_queue_dir, domain).await })
}

async fn validate_amqp_request_impl(
    kv_map: &HashMap<String, String>,
    default_queue_dir: &str,
    domain: TargetDomain,
) -> S3Result<()> {
    if let Some(queue_dir) = kv_map.get(AMQP_QUEUE_DIR) {
        validate_queue_dir(queue_dir.as_str()).await?;
    }
    let args = build_amqp_args(&to_kvs(kv_map), default_queue_dir, domain.runtime_target_type())
        .map_err(|e| s3_error!(InvalidArgument, "{}", e))?;
    check_amqp_broker_available(&args).await.map_err(|e| match e {
        TargetError::Configuration(_) => s3_error!(InvalidArgument, "{}", e),
        _ => s3_error!(InvalidArgument, "AMQP broker check failed: {}", e),
    })
}

fn validate_pulsar_request<'a>(
    kv_map: &'a HashMap<String, String>,
    default_queue_dir: &'a str,
    domain: TargetDomain,
) -> BoxFuture<'a, S3Result<()>> {
    Box::pin(async move { validate_pulsar_request_impl(kv_map, default_queue_dir, domain).await })
}

async fn validate_pulsar_request_impl(
    kv_map: &HashMap<String, String>,
    default_queue_dir: &str,
    domain: TargetDomain,
) -> S3Result<()> {
    if let Some(queue_dir) = kv_map.get("queue_dir") {
        validate_queue_dir(queue_dir.as_str()).await?;
    }
    let args = build_pulsar_args(&to_kvs(kv_map), default_queue_dir, domain.runtime_target_type())
        .map_err(|e| s3_error!(InvalidArgument, "{}", e))?;
    check_pulsar_broker_available(&args).await.map_err(|e| match e {
        TargetError::Configuration(_) => s3_error!(InvalidArgument, "{}", e),
        _ => s3_error!(InvalidArgument, "Pulsar broker check failed: {}", e),
    })
}

async fn validate_mysql_request(
    kv_map: &HashMap<String, String>,
    default_queue_dir: &str,
    target_type: TargetType,
) -> S3Result<()> {
    if let Some(queue_dir) = kv_map.get(MYSQL_QUEUE_DIR) {
        validate_queue_dir(queue_dir.as_str()).await?;
    }

    let args =
        build_mysql_args(&to_kvs(kv_map), default_queue_dir, target_type).map_err(|e| s3_error!(InvalidArgument, "{}", e))?;
    check_mysql_server_available(&args).await.map_err(|e| match e {
        TargetError::Configuration(_) => s3_error!(InvalidArgument, "{}", e),
        _ => s3_error!(InvalidArgument, "MySQL server check failed: {}", e),
    })
}

fn validate_postgres_request<'a>(
    kv_map: &'a HashMap<String, String>,
    default_queue_dir: &'a str,
    domain: TargetDomain,
) -> BoxFuture<'a, S3Result<()>> {
    Box::pin(async move { validate_postgres_request_impl(kv_map, default_queue_dir, domain).await })
}

async fn validate_postgres_request_impl(
    kv_map: &HashMap<String, String>,
    default_queue_dir: &str,
    domain: TargetDomain,
) -> S3Result<()> {
    if let Some(queue_dir) = kv_map.get(POSTGRES_QUEUE_DIR) {
        validate_queue_dir(queue_dir.as_str()).await?;
    }
    let args = build_postgres_args(&to_kvs(kv_map), default_queue_dir, domain.runtime_target_type())
        .map_err(|e| s3_error!(InvalidArgument, "{}", e))?;
    check_postgres_server_available(&args).await.map_err(|e| match e {
        TargetError::Configuration(_) => s3_error!(InvalidArgument, "{}", e),
        _ => s3_error!(InvalidArgument, "PostgreSQL server check failed: {}", e),
    })
}

async fn validate_redis_request(
    kv_map: &HashMap<String, String>,
    default_queue_dir: &str,
    domain: TargetDomain,
    default_channel: &str,
) -> S3Result<()> {
    if let Some(queue_dir) = kv_map.get(REDIS_QUEUE_DIR) {
        validate_queue_dir(queue_dir.as_str()).await?;
    }

    validate_redis_config(&to_kvs(kv_map), default_queue_dir, default_channel)
        .map_err(|e| s3_error!(InvalidArgument, "{}", e))?;

    let args = build_redis_args(&to_kvs(kv_map), default_queue_dir, default_channel, domain.runtime_target_type())
        .map_err(|e| s3_error!(InvalidArgument, "{}", e))?;
    check_redis_server_available(&args).await.map_err(|e| match e {
        TargetError::Configuration(_) => s3_error!(InvalidArgument, "{}", e),
        _ => s3_error!(InvalidArgument, "Redis server check failed: {}", e),
    })
}

fn to_kvs(kv_map: &HashMap<String, String>) -> rustfs_ecstore::config::KVS {
    let mut kvs = rustfs_ecstore::config::KVS::new();
    for (key, value) in kv_map {
        kvs.insert(key.clone(), value.clone());
    }
    kvs
}
