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
use http::{HeaderMap, StatusCode};
use hyper::Method;
use matchit::Params;
use rustfs_config::notify::{NOTIFY_MQTT_SUB_SYS, NOTIFY_WEBHOOK_SUB_SYS};
use rustfs_config::{ENABLE_KEY, EnableState, MAX_ADMIN_REQUEST_BODY_SIZE};
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
}

#[derive(Serialize, Debug)]
struct NotificationEndpointsResponse {
    notification_endpoints: Vec<NotificationEndpoint>,
}

// --- Helper Functions ---

async fn check_permissions(req: &S3Request<Body>) -> S3Result<()> {
    let Some(input_cred) = &req.credentials else {
        return Err(s3_error!(InvalidRequest, "credentials not found"));
    };
    check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;
    Ok(())
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

// --- Operations ---

pub struct NotificationTarget {}
#[async_trait::async_trait]
impl Operation for NotificationTarget {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let span = Span::current();
        let _enter = span.enter();
        let (target_type, target_name) = extract_target_params(&params)?;

        check_permissions(&req).await?;
        let ns = get_notification_system()?;

        let mut input = req.input;
        let body_bytes = input.store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE).await.map_err(|e| {
            warn!("failed to read request body: {:?}", e);
            s3_error!(InvalidRequest, "failed to read request body")
        })?;

        let notification_body: NotificationTargetBody = serde_json::from_slice(&body_bytes)
            .map_err(|e| s3_error!(InvalidArgument, "invalid json body for target config: {}", e))?;

        let allowed_keys: HashSet<&str> = match target_type {
            NOTIFY_WEBHOOK_SUB_SYS => rustfs_config::notify::NOTIFY_WEBHOOK_KEYS.iter().cloned().collect(),
            NOTIFY_MQTT_SUB_SYS => rustfs_config::notify::NOTIFY_MQTT_KEYS.iter().cloned().collect(),
            _ => unreachable!(),
        };

        let kv_map: HashMap<&str, &str> = notification_body
            .key_values
            .iter()
            .map(|kv| (kv.key.as_str(), kv.value.as_str()))
            .collect();

        // Validate keys
        for key in kv_map.keys() {
            if !allowed_keys.contains(key) {
                return Err(s3_error!(InvalidArgument, "key '{}' not allowed for target type '{}'", key, target_type));
            }
        }

        // Type-specific validation
        if target_type == NOTIFY_WEBHOOK_SUB_SYS {
            let endpoint = kv_map
                .get("endpoint")
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
                validate_queue_dir(queue_dir).await?;
            }
            if kv_map.contains_key("client_cert") != kv_map.contains_key("client_key") {
                return Err(s3_error!(InvalidArgument, "client_cert and client_key must be specified as a pair"));
            }
        } else if target_type == NOTIFY_MQTT_SUB_SYS {
            let endpoint = kv_map
                .get(rustfs_config::MQTT_BROKER)
                .ok_or_else(|| s3_error!(InvalidArgument, "broker endpoint is required"))?;
            let topic = kv_map
                .get(rustfs_config::MQTT_TOPIC)
                .ok_or_else(|| s3_error!(InvalidArgument, "topic is required"))?;
            check_mqtt_broker_available(endpoint, topic)
                .await
                .map_err(|e| s3_error!(InvalidArgument, "MQTT Broker unavailable: {}", e))?;

            if let Some(queue_dir) = kv_map.get("queue_dir") {
                validate_queue_dir(queue_dir).await?;
                if let Some(qos) = kv_map.get("qos") {
                    match qos.parse::<u8>() {
                        Ok(1) | Ok(2) => {}
                        Ok(0) => return Err(s3_error!(InvalidArgument, "qos should be 1 or 2 if queue_dir is set")),
                        _ => return Err(s3_error!(InvalidArgument, "qos must be an integer 0, 1, or 2")),
                    }
                }
            }
        }

        let mut kvs_vec: Vec<_> = notification_body
            .key_values
            .into_iter()
            .map(|kv| rustfs_ecstore::config::KV {
                key: kv.key,
                value: kv.value,
                hidden_if_empty: false,
            })
            .collect();

        kvs_vec.push(rustfs_ecstore::config::KV {
            key: ENABLE_KEY.to_string(),
            value: EnableState::On.to_string(),
            hidden_if_empty: false,
        });

        info!("Setting target config for type '{}', name '{}'", target_type, target_name);
        ns.set_target_config(target_type, target_name, rustfs_ecstore::config::KVS(kvs_vec))
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
        check_permissions(&req).await?;
        let ns = get_notification_system()?;

        let targets = ns.get_target_values().await;
        let target_count = targets.len();

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
                NotificationEndpoint {
                    account_id: target.id().id.clone(),
                    service: target.id().name.to_string(),
                    status: status.to_string(),
                }
            });
        }

        let mut notification_endpoints = Vec::with_capacity(target_count);
        while let Some(endpoint) = futures.next().await {
            notification_endpoints.push(endpoint);
        }

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
        check_permissions(&req).await?;
        let ns = get_notification_system()?;

        let active_targets = ns.get_active_targets().await;
        let region = req
            .region
            .clone()
            .ok_or_else(|| s3_error!(InvalidRequest, "region not found"))?;

        let data_target_arn_list: Vec<_> = active_targets.iter().map(|id| id.to_arn(&region).to_string()).collect();

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

        check_permissions(&req).await?;
        let ns = get_notification_system()?;

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
    if target_type != NOTIFY_WEBHOOK_SUB_SYS && target_type != NOTIFY_MQTT_SUB_SYS {
        return Err(s3_error!(InvalidArgument, "unsupported target type: '{}'", target_type));
    }
    let target_name = extract_param(params, "target_name")?;
    Ok((target_type, target_name))
}
