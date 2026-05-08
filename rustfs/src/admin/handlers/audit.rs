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
        build_json_response, collect_validated_key_values as shared_collect_validated_key_values,
        merge_target_endpoints as shared_merge_target_endpoints, target_module_disabled_reason,
        target_mutation_block_reason as shared_target_mutation_block_reason, target_service_name, target_spec,
        validate_target_request,
    },
    router::{AdminOperation, Operation, S3Router},
};
use crate::auth::{check_key_valid, get_session_token};
use crate::server::{
    ADMIN_PREFIX, RemoteAddr, is_audit_module_enabled, refresh_audit_module_enabled, refresh_persisted_module_switches_from_store,
};
use futures::stream::{FuturesUnordered, StreamExt};
use http::StatusCode;
use hyper::Method;
use matchit::Params;
use rustfs_audit::{audit_system, start_audit_system as start_global_audit_system, system::AuditSystemState};
use rustfs_config::audit::{
    AUDIT_AMQP_KEYS, AUDIT_AMQP_SUB_SYS, AUDIT_KAFKA_KEYS, AUDIT_KAFKA_SUB_SYS, AUDIT_MQTT_KEYS, AUDIT_MQTT_SUB_SYS,
    AUDIT_MYSQL_KEYS, AUDIT_MYSQL_SUB_SYS, AUDIT_NATS_KEYS, AUDIT_NATS_SUB_SYS, AUDIT_POSTGRES_KEYS, AUDIT_POSTGRES_SUB_SYS,
    AUDIT_PULSAR_KEYS, AUDIT_PULSAR_SUB_SYS, AUDIT_REDIS_DEFAULT_CHANNEL, AUDIT_REDIS_KEYS, AUDIT_REDIS_SUB_SYS,
    AUDIT_ROUTE_PREFIX, AUDIT_WEBHOOK_KEYS, AUDIT_WEBHOOK_SUB_SYS,
};
use rustfs_config::{AUDIT_DEFAULT_DIR, DEFAULT_DELIMITER, ENABLE_KEY, EnableState, MAX_ADMIN_REQUEST_BODY_SIZE};
use rustfs_ecstore::config::Config;
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::{Body, S3Request, S3Response, S3Result, s3_error};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::time::{Duration, timeout};
use tracing::{Span, warn};

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
    source: TargetEndpointSource,
}

#[derive(Serialize, Debug)]
struct AuditEndpointsResponse {
    audit_endpoints: Vec<AuditEndpoint>,
}

fn audit_target_specs() -> [AdminTargetSpec; 9] {
    [
        AdminTargetSpec {
            subsystem: AUDIT_WEBHOOK_SUB_SYS,
            service: "webhook",
            valid_keys: AUDIT_WEBHOOK_KEYS,
            validator: AdminTargetValidator::Webhook,
        },
        AdminTargetSpec {
            subsystem: AUDIT_AMQP_SUB_SYS,
            service: "amqp",
            valid_keys: AUDIT_AMQP_KEYS,
            validator: AdminTargetValidator::Amqp(TargetDomain::Audit),
        },
        AdminTargetSpec {
            subsystem: AUDIT_KAFKA_SUB_SYS,
            service: "kafka",
            valid_keys: AUDIT_KAFKA_KEYS,
            validator: AdminTargetValidator::Kafka(TargetDomain::Audit),
        },
        AdminTargetSpec {
            subsystem: AUDIT_MQTT_SUB_SYS,
            service: "mqtt",
            valid_keys: AUDIT_MQTT_KEYS,
            validator: AdminTargetValidator::Mqtt,
        },
        AdminTargetSpec {
            subsystem: AUDIT_MYSQL_SUB_SYS,
            service: "mysql",
            valid_keys: AUDIT_MYSQL_KEYS,
            validator: AdminTargetValidator::MySql,
        },
        AdminTargetSpec {
            subsystem: AUDIT_NATS_SUB_SYS,
            service: "nats",
            valid_keys: AUDIT_NATS_KEYS,
            validator: AdminTargetValidator::Nats(TargetDomain::Audit),
        },
        AdminTargetSpec {
            subsystem: AUDIT_POSTGRES_SUB_SYS,
            service: "postgres",
            valid_keys: AUDIT_POSTGRES_KEYS,
            validator: AdminTargetValidator::Postgres(TargetDomain::Audit),
        },
        AdminTargetSpec {
            subsystem: AUDIT_PULSAR_SUB_SYS,
            service: "pulsar",
            valid_keys: AUDIT_PULSAR_KEYS,
            validator: AdminTargetValidator::Pulsar(TargetDomain::Audit),
        },
        AdminTargetSpec {
            subsystem: AUDIT_REDIS_SUB_SYS,
            service: "redis",
            valid_keys: AUDIT_REDIS_KEYS,
            validator: AdminTargetValidator::Redis(TargetDomain::Audit, AUDIT_REDIS_DEFAULT_CHANNEL),
        },
    ]
}

async fn authorize_audit_admin_request(req: &S3Request<Body>, action: AdminAction) -> S3Result<()> {
    let Some(input_cred) = &req.credentials else {
        return Err(s3_error!(InvalidRequest, "credentials not found"));
    };
    let (cred, owner) =
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;
    let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
    validate_admin_request(&req.headers, &cred, owner, false, vec![Action::AdminAction(action)], remote_addr).await
}

fn has_any_audit_targets(config: &Config) -> bool {
    for spec in audit_target_specs() {
        let Some(targets) = config.0.get(spec.subsystem) else {
            continue;
        };
        if targets.keys().any(|key| key != DEFAULT_DELIMITER) {
            return true;
        }
    }
    false
}

fn audit_target_mutation_block_reason(config: &Config, target_type: &str, target_name: &str) -> Option<String> {
    shared_target_mutation_block_reason(
        &audit_target_specs(),
        AUDIT_ROUTE_PREFIX,
        config,
        target_type,
        target_name,
        "audit target",
    )
}

async fn audit_target_operation_block_reason(action: &str) -> Option<String> {
    if let Err(err) = refresh_persisted_module_switches_from_store().await {
        warn!(
            error = %err,
            "failed to reload persisted module switches before checking audit target operation gating"
        );
    }
    refresh_audit_module_enabled();
    target_module_disabled_reason("audit", rustfs_config::ENV_AUDIT_ENABLE, is_audit_module_enabled(), action)
}

fn merge_audit_endpoints(config: &Config, runtime_statuses: HashMap<EndpointKey, String>) -> Vec<AuditEndpoint> {
    shared_merge_target_endpoints(&audit_target_specs(), AUDIT_ROUTE_PREFIX, config, runtime_statuses)
        .into_iter()
        .map(|endpoint| AuditEndpoint {
            account_id: endpoint.account_id,
            service: endpoint.service,
            status: endpoint.status,
            source: endpoint.source,
        })
        .collect()
}

fn extract_target_params<'a>(params: &'a Params<'_, '_>) -> S3Result<(&'a str, &'a str)> {
    let target_type = params
        .get("target_type")
        .ok_or_else(|| s3_error!(InvalidArgument, "missing required parameter: 'target_type'"))?;
    if target_service_name(&audit_target_specs(), target_type).is_none() {
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

        authorize_audit_admin_request(&req, AdminAction::SetBucketTargetAction).await?;
        if let Some(reason) = audit_target_operation_block_reason("managing audit targets from the console").await {
            return Err(s3_error!(InvalidRequest, "{reason}"));
        }
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

        let specs = audit_target_specs();
        let allowed_keys: HashSet<&str> = allowed_target_keys(&specs, target_type);

        let kv_map = shared_collect_validated_key_values(
            audit_body.key_values.iter().map(|kv| (kv.key.as_str(), kv.value.as_str())),
            &allowed_keys,
            target_type,
            "audit target",
        )?;

        let spec = target_spec(&specs, target_type)
            .ok_or_else(|| s3_error!(InvalidArgument, "unsupported audit target type: '{}'", target_type))?;
        timeout(Duration::from_secs(10), validate_target_request(spec, &kv_map, AUDIT_DEFAULT_DIR))
            .await
            .map_err(|_| s3_error!(InvalidArgument, "audit target validation timed out"))??;

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

        Ok(build_json_response(StatusCode::OK, Body::empty(), req.headers.get("x-request-id")))
    }
}

pub struct ListAuditTargets {}

#[async_trait::async_trait]
impl Operation for ListAuditTargets {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let span = Span::current();
        let _enter = span.enter();
        authorize_audit_admin_request(&req, AdminAction::GetBucketTargetAction).await?;

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
                    ((target.id().id, target.id().name), status.to_string())
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

        Ok(build_json_response(StatusCode::OK, Body::from(data), req.headers.get("x-request-id")))
    }
}

pub struct RemoveAuditTarget {}

#[async_trait::async_trait]
impl Operation for RemoveAuditTarget {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let span = Span::current();
        let _enter = span.enter();
        let (target_type, target_name) = extract_target_params(&params)?;

        authorize_audit_admin_request(&req, AdminAction::SetBucketTargetAction).await?;
        if let Some(reason) = audit_target_operation_block_reason("managing audit targets from the console").await {
            return Err(s3_error!(InvalidRequest, "{reason}"));
        }
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

        Ok(build_json_response(StatusCode::OK, Body::empty(), req.headers.get("x-request-id")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use matchit::Router;
    use rustfs_config::ENV_PREFIX;
    use rustfs_ecstore::config::{KV, KVS};
    use std::collections::{HashMap, HashSet};
    use temp_env::{with_var, with_vars, with_vars_unset};

    fn enabled_kvs(value: &str) -> KVS {
        KVS(vec![KV {
            key: ENABLE_KEY.to_string(),
            value: value.to_string(),
            hidden_if_empty: false,
        }])
    }

    fn with_audit_webhook_target_env_cleared<F>(target_name: &str, f: F)
    where
        F: FnOnce(),
    {
        let target_name = target_name.to_ascii_uppercase();
        let mut env_keys = vec![format!(
            "{ENV_PREFIX}{}{DEFAULT_DELIMITER}{}{DEFAULT_DELIMITER}{target_name}",
            AUDIT_WEBHOOK_SUB_SYS.to_ascii_uppercase(),
            ENABLE_KEY.to_ascii_uppercase(),
        )];

        for key in AUDIT_WEBHOOK_KEYS {
            let env_key = format!(
                "{ENV_PREFIX}{}{DEFAULT_DELIMITER}{}{DEFAULT_DELIMITER}{target_name}",
                AUDIT_WEBHOOK_SUB_SYS.to_ascii_uppercase(),
                key.to_ascii_uppercase(),
            );
            if !env_keys.contains(&env_key) {
                env_keys.push(env_key);
            }
        }

        with_vars_unset(env_keys, f);
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
    fn merge_audit_endpoints_marks_kafka_env_and_mixed_sources() {
        let config = Config(HashMap::from([(
            AUDIT_KAFKA_SUB_SYS.to_string(),
            HashMap::from([("mixed-kafka".to_string(), enabled_kvs("on"))]),
        )]));

        with_vars(
            [
                ("RUSTFS_AUDIT_KAFKA_ENABLE_MIXED-KAFKA", Some("on")),
                ("RUSTFS_AUDIT_KAFKA_BROKERS_MIXED-KAFKA", Some("127.0.0.1:9092")),
                ("RUSTFS_AUDIT_KAFKA_ENABLE_ENV-KAFKA", Some("on")),
                ("RUSTFS_AUDIT_KAFKA_BROKERS_ENV-KAFKA", Some("127.0.0.1:9093")),
            ],
            || {
                let runtime = HashMap::from([
                    (("mixed-kafka".to_string(), "kafka".to_string()), "online".to_string()),
                    (("env-kafka".to_string(), "kafka".to_string()), "online".to_string()),
                ]);
                let merged = merge_audit_endpoints(&config, runtime);

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
    fn merge_audit_endpoints_marks_amqp_env_and_mixed_sources() {
        let config = Config(HashMap::from([(
            AUDIT_AMQP_SUB_SYS.to_string(),
            HashMap::from([("mixed-amqp".to_string(), enabled_kvs("on"))]),
        )]));

        with_vars(
            [
                ("RUSTFS_AUDIT_AMQP_ENABLE_MIXED-AMQP", Some("on")),
                ("RUSTFS_AUDIT_AMQP_URL_MIXED-AMQP", Some("amqp://127.0.0.1:5672/%2f")),
                ("RUSTFS_AUDIT_AMQP_ENABLE_ENV-AMQP", Some("on")),
                ("RUSTFS_AUDIT_AMQP_URL_ENV-AMQP", Some("amqp://127.0.0.1:5672/%2f")),
            ],
            || {
                let runtime = HashMap::from([
                    (("mixed-amqp".to_string(), "amqp".to_string()), "online".to_string()),
                    (("env-amqp".to_string(), "amqp".to_string()), "online".to_string()),
                ]);
                let merged = merge_audit_endpoints(&config, runtime);

                let mixed = merged
                    .iter()
                    .find(|entry| entry.account_id == "mixed-amqp" && entry.service == "amqp")
                    .expect("mixed amqp target should be present");
                assert_eq!(mixed.source, TargetEndpointSource::Mixed);

                let env_only = merged
                    .iter()
                    .find(|entry| entry.account_id == "env-amqp" && entry.service == "amqp")
                    .expect("env amqp target should be present");
                assert_eq!(env_only.source, TargetEndpointSource::Env);
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
    fn audit_target_operation_block_reason_requires_audit_module_enable() {
        with_var(rustfs_config::ENV_AUDIT_ENABLE, Some("false"), || {
            let reason =
                futures::executor::block_on(audit_target_operation_block_reason("managing audit targets from the console"));
            assert!(reason.is_some());
            assert!(reason.unwrap().contains("set RUSTFS_AUDIT_ENABLE=true"));
        });
    }

    #[test]
    fn audit_target_operation_block_reason_allows_when_audit_module_enabled() {
        with_var(rustfs_config::ENV_AUDIT_ENABLE, Some("true"), || {
            assert!(
                futures::executor::block_on(audit_target_operation_block_reason("managing audit targets from the console"))
                    .is_none()
            );
        });
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
                assert_eq!(mixed.source, TargetEndpointSource::Mixed);
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
            AUDIT_WEBHOOK_SUB_SYS,
            "audit target",
        )
        .unwrap_err();
        assert!(err.to_string().contains("duplicate key"));
    }

    #[test]
    fn collect_validated_key_values_rejects_unsupported_key() {
        let allowed_keys: HashSet<&str> = AUDIT_WEBHOOK_KEYS.iter().copied().collect();
        let key_values = [KeyValue {
            key: "not_a_real_key".to_string(),
            value: "/tmp/rustfs-audit".to_string(),
        }];

        let err = shared_collect_validated_key_values(
            key_values.iter().map(|kv| (kv.key.as_str(), kv.value.as_str())),
            &allowed_keys,
            AUDIT_WEBHOOK_SUB_SYS,
            "audit target",
        )
        .unwrap_err();
        assert!(err.to_string().contains("not allowed for audit target type"));
    }

    #[test]
    fn extract_target_params_rejects_missing_or_unsupported_values() {
        let mut root_router = Router::new();
        root_router.insert("/", ()).expect("route should insert");
        let missing_type_params = root_router.at("/").expect("route should match");
        let missing_type = extract_target_params(&missing_type_params.params).unwrap_err();
        assert!(missing_type.to_string().contains("missing required parameter: 'target_type'"));

        let mut full_router = Router::new();
        full_router
            .insert("/v3/audit/target/{target_type}/{target_name}", ())
            .expect("route should insert");
        let unsupported_type_params = full_router
            .at("/v3/audit/target/audit_unknown/primary")
            .expect("route should match");
        let unsupported_type = extract_target_params(&unsupported_type_params.params).unwrap_err();
        assert!(unsupported_type.to_string().contains("unsupported audit target type"));

        let supported_kafka_params = full_router
            .at("/v3/audit/target/audit_kafka/primary")
            .expect("route should match");
        let (target_type, target_name) =
            extract_target_params(&supported_kafka_params.params).expect("audit kafka target should be supported");
        assert_eq!(target_type, AUDIT_KAFKA_SUB_SYS);
        assert_eq!(target_name, "primary");

        let supported_amqp_params = full_router
            .at("/v3/audit/target/audit_amqp/primary")
            .expect("route should match");
        let (target_type, target_name) =
            extract_target_params(&supported_amqp_params.params).expect("audit amqp target should be supported");
        assert_eq!(target_type, AUDIT_AMQP_SUB_SYS);
        assert_eq!(target_name, "primary");

        let mut partial_router = Router::new();
        partial_router
            .insert("/v3/audit/target/{target_type}", ())
            .expect("route should insert");
        let missing_name_params = partial_router
            .at("/v3/audit/target/audit_webhook")
            .expect("route should match");
        let missing_name = extract_target_params(&missing_name_params.params).unwrap_err();
        assert!(missing_name.to_string().contains("missing required parameter: 'target_name'"));
    }

    #[test]
    fn merge_audit_endpoints_marks_mixed_with_case_insensitive_instance_id() {
        let config = Config(HashMap::from([(
            AUDIT_WEBHOOK_SUB_SYS.to_string(),
            HashMap::from([("PrimaryCase".to_string(), enabled_kvs("on"))]),
        )]));

        with_vars(
            [
                ("RUSTFS_AUDIT_WEBHOOK_ENABLE_PRIMARYCASE", Some("on")),
                ("RUSTFS_AUDIT_WEBHOOK_ENDPOINT_PRIMARYCASE", Some("https://example.com/hook")),
            ],
            || {
                let runtime = HashMap::from([(("PrimaryCase".to_string(), "webhook".to_string()), "online".to_string())]);
                let merged = merge_audit_endpoints(&config, runtime);
                let mixed = merged
                    .iter()
                    .find(|entry| entry.account_id == "PrimaryCase" && entry.service == "webhook")
                    .expect("mixed target should be present");
                assert_eq!(mixed.source, TargetEndpointSource::Mixed);
            },
        );
    }

    #[test]
    fn audit_target_mutation_block_reason_allows_case_insensitive_config_target_lookup() {
        let config = Config(HashMap::from([(
            AUDIT_WEBHOOK_SUB_SYS.to_string(),
            HashMap::from([("PrimaryCase".to_string(), enabled_kvs("on"))]),
        )]));

        with_audit_webhook_target_env_cleared("primarycase", || {
            assert!(audit_target_mutation_block_reason(&config, AUDIT_WEBHOOK_SUB_SYS, "primarycase").is_none());
        });
    }

    #[test]
    fn audit_target_mutation_block_reason_allows_runtime_only_target() {
        with_audit_webhook_target_env_cleared("primary", || {
            let config = Config(HashMap::new());
            assert!(audit_target_mutation_block_reason(&config, AUDIT_WEBHOOK_SUB_SYS, "primary").is_none());
        });
    }

    #[test]
    fn audit_target_handlers_require_admin_authorization_contract() {
        let src = include_str!("audit.rs");
        let put_block = extract_block_between_markers(src, "impl Operation for AuditTargetConfig", "pub struct ListAuditTargets");
        let list_block =
            extract_block_between_markers(src, "impl Operation for ListAuditTargets", "pub struct RemoveAuditTarget");
        let delete_block = extract_block_between_markers(src, "impl Operation for RemoveAuditTarget", "#[cfg(test)]");

        assert!(
            put_block.contains("authorize_audit_admin_request(&req, AdminAction::SetBucketTargetAction).await?;"),
            "audit target writes should require SetBucketTargetAction"
        );
        assert!(
            put_block.contains("audit_target_operation_block_reason(\"managing audit targets from the console\")"),
            "audit target writes should reject requests when the audit module is disabled"
        );
        assert!(
            list_block.contains("authorize_audit_admin_request(&req, AdminAction::GetBucketTargetAction).await?;"),
            "audit target list should require GetBucketTargetAction"
        );
        assert!(
            delete_block.contains("authorize_audit_admin_request(&req, AdminAction::SetBucketTargetAction).await?;"),
            "audit target deletion should require SetBucketTargetAction"
        );
        assert!(
            delete_block.contains("audit_target_operation_block_reason(\"managing audit targets from the console\")"),
            "audit target deletion should reject requests when the audit module is disabled"
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
