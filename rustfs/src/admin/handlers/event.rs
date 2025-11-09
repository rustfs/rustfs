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

use crate::admin::router::Operation;
use crate::auth::{check_key_valid, get_session_token};
use http::{HeaderMap, StatusCode};
use matchit::Params;
use rustfs_config::notify::{NOTIFY_MQTT_SUB_SYS, NOTIFY_WEBHOOK_SUB_SYS};
use rustfs_config::{ENABLE_KEY, EnableState};
use rustfs_targets::check_mqtt_broker_available;
use s3s::header::CONTENT_LENGTH;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, header::CONTENT_TYPE, s3_error};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::path::Path;
use tokio::net::lookup_host;
use tokio::time::{Duration, sleep};
use tracing::{Span, debug, error, info, warn};
use url::Url;

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
}

#[derive(Serialize, Debug)]
struct NotificationEndpointsResponse {
    notification_endpoints: Vec<NotificationEndpoint>,
}

async fn retry_with_backoff<F, Fut, T>(mut operation: F, max_attempts: usize, base_delay: Duration) -> Result<T, Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Error>>,
{
    assert!(max_attempts > 0, "max_attempts must be greater than 0");
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
                    warn!(
                        "Retry attempt {}/{} failed: {}. Retrying in {:?}",
                        attempts,
                        max_attempts,
                        last_err.as_ref().unwrap(),
                        delay
                    );
                    sleep(delay).await;
                    delay = delay.saturating_mul(2);
                }
            }
        }
    }
    Err(last_err.unwrap_or_else(|| Error::other("retry_with_backoff: unknown error")))
}

async fn retry_metadata(path: &str) -> Result<(), Error> {
    retry_with_backoff(|| async { tokio::fs::metadata(path).await.map(|_| ()) }, 3, Duration::from_millis(100)).await
}

async fn validate_queue_dir(queue_dir: &str) -> S3Result<()> {
    if !queue_dir.is_empty() {
        if !Path::new(queue_dir).is_absolute() {
            return Err(s3_error!(InvalidArgument, "queue_dir must be absolute path"));
        }

        if let Err(e) = retry_metadata(queue_dir).await {
            return match e.kind() {
                ErrorKind::NotFound => Err(s3_error!(InvalidArgument, "queue_dir does not exist")),
                ErrorKind::PermissionDenied => Err(s3_error!(InvalidArgument, "queue_dir exists but permission denied")),
                _ => Err(s3_error!(InvalidArgument, "failed to access queue_dir: {}", e)),
            };
        }
    }

    Ok(())
}

fn validate_cert_key_pair(cert: &Option<String>, key: &Option<String>) -> S3Result<()> {
    if cert.is_some() != key.is_some() {
        return Err(s3_error!(InvalidArgument, "client_cert and client_key must be specified as a pair"));
    }
    Ok(())
}

/// Set (create or update) a notification target
pub struct NotificationTarget {}
#[async_trait::async_trait]
impl Operation for NotificationTarget {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let span = Span::current();
        let _enter = span.enter();
        // 1. Analyze query parameters
        let (target_type, target_name) = extract_target_params(&params)?;

        // 2. Permission verification
        let Some(input_cred) = &req.credentials else {
            return Err(s3_error!(InvalidRequest, "credentials not found"));
        };
        let (_cred, _owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        // 3. Get notification system instance
        let Some(ns) = rustfs_notify::notification_system() else {
            return Err(s3_error!(InternalError, "notification system not initialized"));
        };

        // 4. The parsing request body is KVS (Key-Value Store)
        let mut input = req.input;
        let body = input.store_all_unlimited().await.map_err(|e| {
            warn!("failed to read request body: {:?}", e);
            s3_error!(InvalidRequest, "failed to read request body")
        })?;

        // 1. Get the allowed key range
        let allowed_keys: std::collections::HashSet<&str> = match target_type {
            NOTIFY_WEBHOOK_SUB_SYS => rustfs_config::notify::NOTIFY_WEBHOOK_KEYS.iter().cloned().collect(),
            NOTIFY_MQTT_SUB_SYS => rustfs_config::notify::NOTIFY_MQTT_KEYS.iter().cloned().collect(),
            _ => unreachable!(),
        };

        let notification_body: NotificationTargetBody = serde_json::from_slice(&body)
            .map_err(|e| s3_error!(InvalidArgument, "invalid json body for target config: {}", e))?;

        // 2. Filter and verify keys, and splice target_name
        let mut kvs_vec = Vec::new();
        let mut endpoint_val = None;
        let mut queue_dir_val = None;
        let mut client_cert_val = None;
        let mut client_key_val = None;
        let mut qos_val = None;
        let mut topic_val = String::new();

        for kv in notification_body.key_values.iter() {
            if !allowed_keys.contains(kv.key.as_str()) {
                return Err(s3_error!(
                    InvalidArgument,
                    "key '{}' not allowed for target type '{}'",
                    kv.key,
                    target_type
                ));
            }
            if kv.key == "endpoint" {
                endpoint_val = Some(kv.value.clone());
            }

            if target_type == NOTIFY_MQTT_SUB_SYS {
                if kv.key == rustfs_config::MQTT_BROKER {
                    endpoint_val = Some(kv.value.clone());
                }
                if kv.key == rustfs_config::MQTT_TOPIC {
                    topic_val = kv.value.clone();
                }
            }

            if kv.key == "queue_dir" {
                queue_dir_val = Some(kv.value.clone());
            }
            if kv.key == "client_cert" {
                client_cert_val = Some(kv.value.clone());
            }
            if kv.key == "client_key" {
                client_key_val = Some(kv.value.clone());
            }
            if kv.key == "qos" {
                qos_val = Some(kv.value.clone());
            }

            kvs_vec.push(rustfs_ecstore::config::KV {
                key: kv.key.clone(),
                value: kv.value.clone(),
                hidden_if_empty: false,
            });
        }

        if target_type == NOTIFY_WEBHOOK_SUB_SYS {
            let endpoint = endpoint_val
                .clone()
                .ok_or_else(|| s3_error!(InvalidArgument, "endpoint is required"))?;
            let url = Url::parse(&endpoint).map_err(|e| s3_error!(InvalidArgument, "invalid endpoint url: {}", e))?;
            let host = url
                .host_str()
                .ok_or_else(|| s3_error!(InvalidArgument, "endpoint missing host"))?;
            let port = url
                .port_or_known_default()
                .ok_or_else(|| s3_error!(InvalidArgument, "endpoint missing port"))?;
            let addr = format!("{host}:{port}");
            // First, try to parse as SocketAddr (IP:port)
            if addr.parse::<SocketAddr>().is_err() {
                // If not an IP:port, try DNS resolution
                if lookup_host(&addr).await.is_err() {
                    return Err(s3_error!(InvalidArgument, "invalid or unresolvable endpoint address"));
                }
            }
            if let Some(queue_dir) = queue_dir_val.clone() {
                validate_queue_dir(&queue_dir).await?;
            }
            validate_cert_key_pair(&client_cert_val, &client_key_val)?;
        }

        if target_type == NOTIFY_MQTT_SUB_SYS {
            let endpoint = endpoint_val.ok_or_else(|| s3_error!(InvalidArgument, "broker endpoint is required"))?;
            if topic_val.is_empty() {
                return Err(s3_error!(InvalidArgument, "topic is required"));
            }
            // Check MQTT Broker availability
            if let Err(e) = check_mqtt_broker_available(&endpoint, &topic_val).await {
                return Err(s3_error!(InvalidArgument, "MQTT Broker unavailable: {}", e));
            }

            if let Some(queue_dir) = queue_dir_val {
                validate_queue_dir(&queue_dir).await?;
                if let Some(qos) = qos_val {
                    match qos.parse::<u8>() {
                        Ok(qos_int) if qos_int == 1 || qos_int == 2 => {}
                        Ok(0) => {
                            return Err(s3_error!(InvalidArgument, "qos should be 1 or 2 if queue_dir is set"));
                        }
                        _ => {
                            return Err(s3_error!(InvalidArgument, "qos must be an integer 0, 1, or 2"));
                        }
                    }
                }
            }
        }

        // 3. Add ENABLE_KEY
        kvs_vec.push(rustfs_ecstore::config::KV {
            key: ENABLE_KEY.to_string(),
            value: EnableState::On.to_string(),
            hidden_if_empty: false,
        });

        let kvs = rustfs_ecstore::config::KVS(kvs_vec);

        // 5. Call notification system to set target configuration
        info!("Setting target config for type '{}', name '{}'", target_type, target_name);
        ns.set_target_config(target_type, target_name, kvs).await.map_err(|e| {
            error!("failed to set target config: {}", e);
            S3Error::with_message(S3ErrorCode::InternalError, format!("failed to set target config: {e}"))
        })?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        header.insert(CONTENT_LENGTH, "0".parse().unwrap());
        if let Some(v) = req.headers.get("x-request-id") {
            header.insert("x-request-id", v.clone());
        }
        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

/// Get a list of notification targets for all activities
pub struct ListNotificationTargets {}
#[async_trait::async_trait]
impl Operation for ListNotificationTargets {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let span = Span::current();
        let _enter = span.enter();
        debug!("ListNotificationTargets call start request params: {:?}", req.uri.query());

        // 1. Permission verification
        let Some(input_cred) = &req.credentials else {
            return Err(s3_error!(InvalidRequest, "credentials not found"));
        };
        let (_cred, _owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        // 2. Get notification system instance
        let Some(ns) = rustfs_notify::notification_system() else {
            return Err(s3_error!(InternalError, "notification system not initialized"));
        };

        // 3. Get the list of activity targets
        let active_targets = ns.get_active_targets().await;

        debug!("ListNotificationTargets call found {} active targets", active_targets.len());
        let mut notification_endpoints = Vec::new();
        for target_id in active_targets.iter() {
            notification_endpoints.push(NotificationEndpoint {
                account_id: target_id.id.clone(),
                service: target_id.name.to_string(),
                status: "online".to_string(),
            });
        }

        let response = NotificationEndpointsResponse { notification_endpoints };

        // 4. Serialize and return the result
        let data = serde_json::to_vec(&response).map_err(|e| {
            error!("Failed to serialize notification targets response: {:?}", response);
            S3Error::with_message(S3ErrorCode::InternalError, format!("failed to serialize targets: {e}"))
        })?;
        debug!("ListNotificationTargets call end, response data length: {}", data.len(),);
        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        if let Some(v) = req.headers.get("x-request-id") {
            header.insert("x-request-id", v.clone());
        }
        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

/// Get a list of notification targets for all activities
pub struct ListTargetsArns {}
#[async_trait::async_trait]
impl Operation for ListTargetsArns {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let span = Span::current();
        let _enter = span.enter();
        debug!("ListTargetsArns call start request params: {:?}", req.uri.query());

        // 1. Permission verification
        let Some(input_cred) = &req.credentials else {
            return Err(s3_error!(InvalidRequest, "credentials not found"));
        };
        let (_cred, _owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        // 2. Get notification system instance
        let Some(ns) = rustfs_notify::notification_system() else {
            return Err(s3_error!(InternalError, "notification system not initialized"));
        };

        // 3. Get the list of activity targets
        let active_targets = ns.get_active_targets().await;

        debug!("ListTargetsArns call found {} active targets", active_targets.len());

        let region = match req.region.clone() {
            Some(region) => region,
            None => return Err(s3_error!(InvalidRequest, "region not found")),
        };
        let mut data_target_arn_list = Vec::new();

        for target_id in active_targets.iter() {
            data_target_arn_list.push(target_id.to_arn(&region).to_string());
        }

        // 4. Serialize and return the result
        let data = serde_json::to_vec(&data_target_arn_list)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("failed to serialize targets: {e}")))?;
        debug!("ListTargetsArns call end, response data length: {}", data.len(),);
        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        if let Some(v) = req.headers.get("x-request-id") {
            header.insert("x-request-id", v.clone());
        }
        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

/// Delete a specified notification target
pub struct RemoveNotificationTarget {}
#[async_trait::async_trait]
impl Operation for RemoveNotificationTarget {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let span = Span::current();
        let _enter = span.enter();
        // 1. Analyze query parameters
        let (target_type, target_name) = extract_target_params(&params)?;

        // 2. Permission verification
        let Some(input_cred) = &req.credentials else {
            return Err(s3_error!(InvalidRequest, "credentials not found"));
        };
        let (_cred, _owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        // 3. Get notification system instance
        let Some(ns) = rustfs_notify::notification_system() else {
            return Err(s3_error!(InternalError, "notification system not initialized"));
        };

        // 4. Call notification system to remove target configuration
        info!("Removing target config for type '{}', name '{}'", target_type, target_name);
        ns.remove_target_config(target_type, target_name).await.map_err(|e| {
            error!("failed to remove target config: {}", e);
            S3Error::with_message(S3ErrorCode::InternalError, format!("failed to remove target config: {e}"))
        })?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        header.insert(CONTENT_LENGTH, "0".parse().unwrap());
        if let Some(v) = req.headers.get("x-request-id") {
            header.insert("x-request-id", v.clone());
        }
        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

fn extract_param<'a>(params: &'a Params<'_, '_>, key: &str) -> S3Result<&'a str> {
    params
        .get(key)
        .ok_or_else(|| s3_error!(InvalidArgument, "missing required parameter: '{}'", key))
}

fn extract_target_params<'a>(params: &'a Params<'_, '_>) -> S3Result<(&'a str, &'a str)> {
    let target_type = extract_param(params, "target_type")?;
    if target_type != NOTIFY_WEBHOOK_SUB_SYS && target_type != NOTIFY_MQTT_SUB_SYS {
        return Err(s3_error!(InvalidArgument, "unsupported target type: '{}'", target_type));
    }

    let target_name = extract_param(params, "target_name")?;
    Ok((target_type, target_name))
}
