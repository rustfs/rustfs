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
        AdminTargetSpec, AdminTargetValidator, EndpointKey, TargetDomain, TargetEndpointSource, allowed_target_keys,
        collect_validated_key_values as shared_collect_validated_key_values,
        merge_target_endpoints as shared_merge_target_endpoints, target_module_disabled_reason,
        target_mutation_block_reason as shared_target_mutation_block_reason, target_service_name, target_spec,
        validate_target_request,
    },
    router::{AdminOperation, Operation, S3Router},
};
use crate::auth::{check_key_valid, get_session_token};
use crate::server::{
    ADMIN_PREFIX, RemoteAddr, is_notify_module_enabled, refresh_notify_module_enabled,
    refresh_persisted_module_switches_from_store,
};
use futures::stream::{FuturesUnordered, StreamExt};
use http::{HeaderMap, StatusCode};
use hyper::Method;
use matchit::Params;
use rustfs_config::notify::{
    NOTIFY_KAFKA_KEYS, NOTIFY_KAFKA_SUB_SYS, NOTIFY_MQTT_KEYS, NOTIFY_MQTT_SUB_SYS, NOTIFY_MYSQL_KEYS, NOTIFY_MYSQL_SUB_SYS,
    NOTIFY_NATS_KEYS, NOTIFY_NATS_SUB_SYS, NOTIFY_POSTGRES_KEYS, NOTIFY_POSTGRES_SUB_SYS, NOTIFY_PULSAR_KEYS,
    NOTIFY_PULSAR_SUB_SYS, NOTIFY_REDIS_DEFAULT_CHANNEL, NOTIFY_REDIS_KEYS, NOTIFY_REDIS_SUB_SYS, NOTIFY_ROUTE_PREFIX,
    NOTIFY_WEBHOOK_KEYS, NOTIFY_WEBHOOK_SUB_SYS,
};
use rustfs_config::{ENABLE_KEY, EVENT_DEFAULT_DIR, EnableState, MAX_ADMIN_REQUEST_BODY_SIZE};
use rustfs_ecstore::config::Config;
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::{Body, S3Request, S3Response, S3Result, header::CONTENT_TYPE, s3_error};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::time::{Duration, timeout};
use tracing::{Span, info, warn};

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
    source: TargetEndpointSource,
}

#[derive(Serialize, Debug)]
struct NotificationEndpointsResponse {
    notification_endpoints: Vec<NotificationEndpoint>,
}

fn notification_target_specs() -> [AdminTargetSpec; 8] {
    [
        AdminTargetSpec {
            subsystem: NOTIFY_WEBHOOK_SUB_SYS,
            service: "webhook",
            valid_keys: NOTIFY_WEBHOOK_KEYS,
            validator: AdminTargetValidator::Webhook,
        },
        AdminTargetSpec {
            subsystem: NOTIFY_KAFKA_SUB_SYS,
            service: "kafka",
            valid_keys: NOTIFY_KAFKA_KEYS,
            validator: AdminTargetValidator::Kafka(TargetDomain::Notify),
        },
        AdminTargetSpec {
            subsystem: NOTIFY_MQTT_SUB_SYS,
            service: "mqtt",
            valid_keys: NOTIFY_MQTT_KEYS,
            validator: AdminTargetValidator::Mqtt,
        },
        AdminTargetSpec {
            subsystem: NOTIFY_MYSQL_SUB_SYS,
            service: "mysql",
            valid_keys: NOTIFY_MYSQL_KEYS,
            validator: AdminTargetValidator::MySql,
        },
        AdminTargetSpec {
            subsystem: NOTIFY_NATS_SUB_SYS,
            service: "nats",
            valid_keys: NOTIFY_NATS_KEYS,
            validator: AdminTargetValidator::Nats(TargetDomain::Notify),
        },
        AdminTargetSpec {
            subsystem: NOTIFY_POSTGRES_SUB_SYS,
            service: "postgres",
            valid_keys: NOTIFY_POSTGRES_KEYS,
            validator: AdminTargetValidator::Postgres(TargetDomain::Notify),
        },
        AdminTargetSpec {
            subsystem: NOTIFY_REDIS_SUB_SYS,
            service: "redis",
            valid_keys: NOTIFY_REDIS_KEYS,
            validator: AdminTargetValidator::Redis(TargetDomain::Notify, NOTIFY_REDIS_DEFAULT_CHANNEL),
        },
        AdminTargetSpec {
            subsystem: NOTIFY_PULSAR_SUB_SYS,
            service: "pulsar",
            valid_keys: NOTIFY_PULSAR_KEYS,
            validator: AdminTargetValidator::Pulsar(TargetDomain::Notify),
        },
    ]
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

fn target_mutation_block_reason(config: &Config, target_type: &str, target_name: &str) -> Option<String> {
    shared_target_mutation_block_reason(
        &notification_target_specs(),
        NOTIFY_ROUTE_PREFIX,
        config,
        target_type,
        target_name,
        "target",
    )
}

async fn notification_target_operation_block_reason(action: &str) -> Option<String> {
    if let Err(err) = refresh_persisted_module_switches_from_store().await {
        warn!(
            error = %err,
            "failed to reload persisted module switches before checking notification target operation gating"
        );
    }
    refresh_notify_module_enabled();
    target_module_disabled_reason("notify", rustfs_config::ENV_NOTIFY_ENABLE, is_notify_module_enabled(), action)
}

fn merge_notification_endpoints(config: &Config, runtime_statuses: HashMap<EndpointKey, String>) -> Vec<NotificationEndpoint> {
    shared_merge_target_endpoints(&notification_target_specs(), NOTIFY_ROUTE_PREFIX, config, runtime_statuses)
        .into_iter()
        .map(|endpoint| NotificationEndpoint {
            account_id: endpoint.account_id,
            service: endpoint.service,
            status: endpoint.status,
            source: endpoint.source,
        })
        .collect()
}

fn collect_online_target_arns(region: &str, target_statuses: Vec<(rustfs_targets::arn::TargetID, String)>) -> Vec<String> {
    target_statuses
        .into_iter()
        .filter_map(|(target_id, status)| (status == "online").then(|| target_id.to_arn(region).to_string()))
        .collect()
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
        if let Some(reason) = notification_target_operation_block_reason("managing notification targets from the console").await {
            return Err(s3_error!(InvalidRequest, "{reason}"));
        }
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

        let specs = notification_target_specs();
        let allowed_keys: HashSet<&str> = allowed_target_keys(&specs, target_type);

        let kv_map = shared_collect_validated_key_values(
            notification_body
                .key_values
                .iter()
                .map(|kv| (kv.key.as_str(), kv.value.as_str())),
            &allowed_keys,
            target_type,
            "target",
        )?;
        let spec = target_spec(&specs, target_type)
            .ok_or_else(|| s3_error!(InvalidArgument, "unsupported target type: '{}'", target_type))?;
        timeout(Duration::from_secs(10), validate_target_request(spec, &kv_map, EVENT_DEFAULT_DIR))
            .await
            .map_err(|_| s3_error!(InvalidArgument, "target validation timed out"))??;

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
        if let Some(reason) = notification_target_operation_block_reason(
            "querying notification target ARNs for bucket associations from the console",
        )
        .await
        {
            return Err(s3_error!(InvalidRequest, "{reason}"));
        }
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
        if let Some(reason) = notification_target_operation_block_reason("managing notification targets from the console").await {
            return Err(s3_error!(InvalidRequest, "{reason}"));
        }
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
    if target_service_name(&notification_target_specs(), target_type).is_none() {
        return Err(s3_error!(InvalidArgument, "unsupported target type: '{}'", target_type));
    }
    let target_name = extract_param(params, "target_name")?;
    Ok((target_type, target_name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use matchit::Router;
    use rustfs_config::DEFAULT_DELIMITER;
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
        assert_eq!(mqtt.source, TargetEndpointSource::Config);

        let webhook = merged
            .iter()
            .find(|entry| entry.account_id == "webhook-a" && entry.service == "webhook")
            .expect("webhook-a should be present");
        assert_eq!(webhook.status, "online");
        assert_eq!(webhook.source, TargetEndpointSource::Config);
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
        assert_eq!(env_only.source, TargetEndpointSource::Runtime);

        let enabled = merged
            .iter()
            .find(|entry| entry.account_id == "webhook-enabled" && entry.service == "webhook")
            .expect("webhook-enabled should be present");
        assert_eq!(enabled.status, "online");
        assert_eq!(enabled.source, TargetEndpointSource::Config);
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
                assert_eq!(mixed.source, TargetEndpointSource::Mixed);

                let env_only = merged
                    .iter()
                    .find(|entry| entry.account_id == "env-only")
                    .expect("env-only target should be present");
                assert_eq!(env_only.source, TargetEndpointSource::Env);

                let config_only = merged
                    .iter()
                    .find(|entry| entry.account_id == "config-target")
                    .expect("config target should be present");
                assert_eq!(config_only.source, TargetEndpointSource::Config);
            },
        );
    }

    #[test]
    fn merge_notification_endpoints_marks_kafka_env_and_mixed_sources() {
        let config = Config(HashMap::from([(
            NOTIFY_KAFKA_SUB_SYS.to_string(),
            HashMap::from([("mixed-kafka".to_string(), enabled_kvs("on"))]),
        )]));

        with_vars(
            [
                ("RUSTFS_NOTIFY_KAFKA_ENABLE_MIXED-KAFKA", Some("on")),
                ("RUSTFS_NOTIFY_KAFKA_BROKERS_MIXED-KAFKA", Some("127.0.0.1:9092")),
                ("RUSTFS_NOTIFY_KAFKA_ENABLE_ENV-KAFKA", Some("on")),
                ("RUSTFS_NOTIFY_KAFKA_BROKERS_ENV-KAFKA", Some("127.0.0.1:9093")),
            ],
            || {
                let runtime = HashMap::from([
                    (("mixed-kafka".to_string(), "kafka".to_string()), "online".to_string()),
                    (("env-kafka".to_string(), "kafka".to_string()), "online".to_string()),
                ]);
                let merged = merge_notification_endpoints(&config, runtime);

                let mixed = merged
                    .iter()
                    .find(|entry| entry.account_id == "mixed-kafka" && entry.service == "kafka")
                    .expect("mixed kafka target should be present");
                assert_eq!(mixed.source, TargetEndpointSource::Mixed);

                let env_only = merged
                    .iter()
                    .find(|entry| entry.account_id == "env-kafka" && entry.service == "kafka")
                    .expect("env kafka target should be present");
                assert_eq!(env_only.source, TargetEndpointSource::Env);
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
    fn notification_target_operation_block_reason_requires_notify_module_enable() {
        with_var(rustfs_config::ENV_NOTIFY_ENABLE, Some("false"), || {
            let reason = futures::executor::block_on(notification_target_operation_block_reason(
                "managing notification targets from the console",
            ));
            assert!(reason.is_some());
            assert!(reason.unwrap().contains("set RUSTFS_NOTIFY_ENABLE=true"));
        });
    }

    #[test]
    fn notification_target_operation_block_reason_allows_when_notify_module_enabled() {
        with_var(rustfs_config::ENV_NOTIFY_ENABLE, Some("true"), || {
            assert!(
                futures::executor::block_on(notification_target_operation_block_reason(
                    "managing notification targets from the console"
                ))
                .is_none()
            );
        });
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
                assert_eq!(mixed.source, TargetEndpointSource::Mixed);
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
                assert_eq!(env_only.source, TargetEndpointSource::Env);
                assert_eq!(env_only.status, "offline");
            },
        );
    }

    #[test]
    fn collect_validated_key_values_rejects_duplicate_keys() {
        let allowed_keys: HashSet<&str> = ["endpoint", "auth_token"].into_iter().collect();
        let key_values = [
            KeyValue {
                key: "endpoint".to_string(),
                value: "https://example.com/one".to_string(),
            },
            KeyValue {
                key: "endpoint".to_string(),
                value: "https://example.com/two".to_string(),
            },
        ];

        let err = shared_collect_validated_key_values(
            key_values.iter().map(|kv| (kv.key.as_str(), kv.value.as_str())),
            &allowed_keys,
            NOTIFY_WEBHOOK_SUB_SYS,
            "target",
        )
        .unwrap_err();
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
                assert_eq!(mixed.source, TargetEndpointSource::Mixed);
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
            put_block.contains("notification_target_operation_block_reason(")
                && put_block.contains("\"managing notification targets from the console\""),
            "notification target writes should reject requests when the notify module is disabled"
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
            arns_block.contains("notification_target_operation_block_reason(")
                && arns_block.contains("\"querying notification target ARNs for bucket associations from the console\""),
            "notification target arn listing should reject requests when the notify module is disabled"
        );
        assert!(
            delete_block.contains("authorize_notification_admin_request(&req, AdminAction::SetBucketTargetAction).await?;"),
            "notification target deletion should require SetBucketTargetAction"
        );
        assert!(
            delete_block.contains("notification_target_operation_block_reason(")
                && delete_block.contains("\"managing notification targets from the console\""),
            "notification target deletion should reject requests when the notify module is disabled"
        );
    }

    #[test]
    fn extract_target_params_accepts_kafka_target_type() {
        let mut router = Router::new();
        router
            .insert("/v3/target/{target_type}/{target_name}", ())
            .expect("route should insert");

        let params = router
            .at("/v3/target/notify_kafka/streaming")
            .expect("route should match")
            .params;
        let (target_type, target_name) = extract_target_params(&params).expect("kafka target type should be accepted");
        assert_eq!(target_type, NOTIFY_KAFKA_SUB_SYS);
        assert_eq!(target_name, "streaming");
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
