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
use rustfs_config::notify::{ENABLE_KEY, ENABLE_ON, NOTIFY_MQTT_SUB_SYS, NOTIFY_WEBHOOK_SUB_SYS};
use rustfs_notify::EventName;
use rustfs_notify::rules::{BucketNotificationConfig, PatternRules};
use s3s::header::CONTENT_LENGTH;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, header::CONTENT_TYPE, s3_error};
use serde::{Deserialize, Serialize};
use serde_urlencoded::from_bytes;
use std::collections::HashMap;
use tracing::{debug, error, info, warn};

#[derive(Debug, Deserialize)]
struct TargetQuery {
    #[serde(rename = "targetType")]
    target_type: String,
    #[serde(rename = "targetName")]
    target_name: String,
}

#[derive(Debug, Deserialize)]
struct BucketQuery {
    #[serde(rename = "bucketName")]
    bucket_name: String,
}

/// Set (create or update) a notification target
pub struct SetNotificationTarget {}
#[async_trait::async_trait]
impl Operation for SetNotificationTarget {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        // 1. Analyze query parameters
        let query: TargetQuery = from_bytes(req.uri.query().unwrap_or("").as_bytes())
            .map_err(|e| s3_error!(InvalidArgument, "invalid query parameters: {}", e))?;

        let target_type = query.target_type.to_lowercase();
        if target_type != *NOTIFY_WEBHOOK_SUB_SYS && target_type != *NOTIFY_MQTT_SUB_SYS {
            return Err(s3_error!(InvalidArgument, "unsupported target type: {}", query.target_type));
        }

        // 2. Permission verification
        let Some(input_cred) = &req.credentials else {
            return Err(s3_error!(InvalidRequest, "credentials not found"));
        };
        let (_cred, _owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        // 3. Get notification system instance
        let Some(ns) = rustfs_notify::global::notification_system() else {
            return Err(s3_error!(InternalError, "notification system not initialized"));
        };

        // 4. The parsing request body is KVS (Key-Value Store)
        let mut input = req.input;
        let body = input.store_all_unlimited().await.map_err(|e| {
            warn!("failed to read request body: {:?}", e);
            s3_error!(InvalidRequest, "failed to read request body")
        })?;
        let mut kvs_map: HashMap<String, String> = serde_json::from_slice(&body)
            .map_err(|e| s3_error!(InvalidArgument, "invalid json body for target config: {}", e))?;
        // If there is an enable key, add an enable key value to "on"
        if !kvs_map.contains_key(ENABLE_KEY) {
            kvs_map.insert(ENABLE_KEY.to_string(), ENABLE_ON.to_string());
        }

        let kvs = rustfs_ecstore::config::KVS(
            kvs_map
                .into_iter()
                .map(|(key, value)| rustfs_ecstore::config::KV {
                    key,
                    value,
                    hidden_if_empty: false, // Set a default value
                })
                .collect(),
        );

        // 5. Call notification system to set target configuration
        info!("Setting target config for type '{}', name '{}'", &query.target_type, &query.target_name);
        ns.set_target_config(&query.target_type, &query.target_name, kvs)
            .await
            .map_err(|e| {
                error!("failed to set target config: {}", e);
                S3Error::with_message(S3ErrorCode::InternalError, format!("failed to set target config: {e}"))
            })?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        header.insert(CONTENT_LENGTH, "0".parse().unwrap());
        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

/// Get a list of notification targets for all activities
pub struct ListNotificationTargets {}
#[async_trait::async_trait]
impl Operation for ListNotificationTargets {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        debug!("ListNotificationTargets call start request params: {:?}", req.uri.query());

        // 1. Permission verification
        let Some(input_cred) = &req.credentials else {
            return Err(s3_error!(InvalidRequest, "credentials not found"));
        };
        let (_cred, _owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        // 2. Get notification system instance
        let Some(ns) = rustfs_notify::global::notification_system() else {
            return Err(s3_error!(InternalError, "notification system not initialized"));
        };

        // 3. Get the list of activity targets
        let active_targets = ns.get_active_targets().await;

        let region = match req.region.clone() {
            Some(region) => region,
            None => return Err(s3_error!(InvalidRequest, "region not found")),
        };
        let mut data_target_arn_list = Vec::new();
        for target_id in active_targets.iter() {
            let target_arn = target_id.to_arn(&region);
            data_target_arn_list.push(target_arn.to_string());
        }

        // 4. Serialize and return the result
        let data = serde_json::to_vec(&data_target_arn_list)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("failed to serialize targets: {e}")))?;
        debug!("ListNotificationTargets call end, response data length: {}", data.len(),);
        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

/// Delete a specified notification target
pub struct RemoveNotificationTarget {}
#[async_trait::async_trait]
impl Operation for RemoveNotificationTarget {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        // 1. Analyze query parameters
        let query: TargetQuery = from_bytes(req.uri.query().unwrap_or("").as_bytes())
            .map_err(|e| s3_error!(InvalidArgument, "invalid query parameters: {}", e))?;

        // 2. Permission verification
        let Some(input_cred) = &req.credentials else {
            return Err(s3_error!(InvalidRequest, "credentials not found"));
        };
        let (_cred, _owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        // 3. Get notification system instance
        let Some(ns) = rustfs_notify::global::notification_system() else {
            return Err(s3_error!(InternalError, "notification system not initialized"));
        };

        // 4. Call notification system to remove target configuration
        info!("Removing target config for type '{}', name '{}'", &query.target_type, &query.target_name);
        ns.remove_target_config(&query.target_type, &query.target_name)
            .await
            .map_err(|e| {
                error!("failed to remove target config: {}", e);
                S3Error::with_message(S3ErrorCode::InternalError, format!("failed to remove target config: {e}"))
            })?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        header.insert(CONTENT_LENGTH, "0".parse().unwrap());
        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

/// Set notification rules for buckets
pub struct SetBucketNotification {}
#[async_trait::async_trait]
impl Operation for SetBucketNotification {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        // 1. Analyze query parameters
        let query: BucketQuery = from_bytes(req.uri.query().unwrap_or("").as_bytes())
            .map_err(|e| s3_error!(InvalidArgument, "invalid query parameters: {}", e))?;

        // 2. Permission verification
        let Some(input_cred) = &req.credentials else {
            return Err(s3_error!(InvalidRequest, "credentials not found"));
        };
        let (_cred, _owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        // 3. Get notification system instance
        let Some(ns) = rustfs_notify::global::notification_system() else {
            return Err(s3_error!(InternalError, "notification system not initialized"));
        };

        // 4. The parsing request body is BucketNotificationConfig
        let mut input = req.input;
        let body = input.store_all_unlimited().await.map_err(|e| {
            warn!("failed to read request body: {:?}", e);
            s3_error!(InvalidRequest, "failed to read request body")
        })?;
        let config: BucketNotificationConfig = serde_json::from_slice(&body)
            .map_err(|e| s3_error!(InvalidArgument, "invalid json body for bucket notification config: {}", e))?;

        // 5. Load bucket notification configuration
        info!("Loading notification config for bucket '{}'", &query.bucket_name);
        ns.load_bucket_notification_config(&query.bucket_name, &config)
            .await
            .map_err(|e| {
                error!("failed to load bucket notification config: {}", e);
                S3Error::with_message(S3ErrorCode::InternalError, format!("failed to load bucket notification config: {e}"))
            })?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        header.insert(CONTENT_LENGTH, "0".parse().unwrap());
        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

/// Get notification rules for buckets
#[derive(Serialize)]
struct BucketRulesResponse {
    rules: HashMap<EventName, PatternRules>,
}
pub struct GetBucketNotification {}
#[async_trait::async_trait]
impl Operation for GetBucketNotification {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let query: BucketQuery = from_bytes(req.uri.query().unwrap_or("").as_bytes())
            .map_err(|e| s3_error!(InvalidArgument, "invalid query parameters: {}", e))?;

        let Some(input_cred) = &req.credentials else {
            return Err(s3_error!(InvalidRequest, "credentials not found"));
        };
        let (_cred, _owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        let Some(ns) = rustfs_notify::global::notification_system() else {
            return Err(s3_error!(InternalError, "notification system not initialized"));
        };

        let rules_map = ns.notifier.get_rules_map(&query.bucket_name);
        let response = BucketRulesResponse {
            rules: rules_map.unwrap_or_default().inner().clone(),
        };

        let data = serde_json::to_vec(&response)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("failed to serialize rules: {e}")))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

/// 删除存储桶的所有通知规则
pub struct RemoveBucketNotification {}
#[async_trait::async_trait]
impl Operation for RemoveBucketNotification {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let query: BucketQuery = from_bytes(req.uri.query().unwrap_or("").as_bytes())
            .map_err(|e| s3_error!(InvalidArgument, "invalid query parameters: {}", e))?;

        let Some(input_cred) = &req.credentials else {
            return Err(s3_error!(InvalidRequest, "credentials not found"));
        };
        let (_cred, _owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        let Some(ns) = rustfs_notify::global::notification_system() else {
            return Err(s3_error!(InternalError, "notification system not initialized"));
        };

        info!("Removing notification config for bucket '{}'", &query.bucket_name);
        ns.remove_bucket_notification_config(&query.bucket_name).await;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        header.insert(CONTENT_LENGTH, "0".parse().unwrap());
        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}
