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
use crate::admin::handlers::site_replication::site_replication_peer_deployment_id_for_endpoint;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::utils::read_compatible_admin_body;
use crate::auth::{check_key_valid, get_session_token};
use crate::error::ApiError;
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use http::{HeaderMap, HeaderValue, Uri};
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_credentials::Credentials;
use rustfs_ecstore::bucket::bucket_target_sys::{BucketTargetError, BucketTargetSys};
use rustfs_ecstore::bucket::metadata::BUCKET_TARGETS_FILE;
use rustfs_ecstore::bucket::metadata_sys;
use rustfs_ecstore::bucket::metadata_sys::get_replication_config;
use rustfs_ecstore::bucket::replication::BucketStats;
use rustfs_ecstore::bucket::replication::GLOBAL_REPLICATION_STATS;
use rustfs_ecstore::bucket::target::BucketTarget;
use rustfs_ecstore::error::StorageError;
use rustfs_ecstore::global::global_rustfs_port;
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::store_api::{BucketOperations, BucketOptions};
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use std::collections::HashMap;
use tracing::{debug, error, info, warn};
use url::Host;

fn extract_query_params(uri: &Uri) -> HashMap<String, String> {
    let mut params = HashMap::new();

    if let Some(query) = uri.query() {
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            params.insert(key.into_owned(), value.into_owned());
        }
    }

    params
}

fn map_bucket_target_error(err: BucketTargetError) -> S3Error {
    match err {
        BucketTargetError::BucketRemoteTargetNotFound { .. }
        | BucketTargetError::BucketRemoteArnTypeInvalid { .. }
        | BucketTargetError::BucketRemoteAlreadyExists { .. }
        | BucketTargetError::BucketRemoteArnInvalid { .. }
        | BucketTargetError::RemoteTargetConnectionErr { .. }
        | BucketTargetError::BucketReplicationSourceNotVersioned { .. }
        | BucketTargetError::BucketRemoteTargetNotVersioned { .. }
        | BucketTargetError::BucketRemoteRemoveDisallowed { .. } => {
            S3Error::with_message(S3ErrorCode::InvalidRequest, err.to_string())
        }
        BucketTargetError::Io(io_err) => S3Error::with_message(S3ErrorCode::InternalError, io_err.to_string()),
    }
}

pub fn register_replication_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/list-remote-targets").as_str(),
        AdminOperation(&ListRemoteTargetHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/replicationmetrics").as_str(),
        AdminOperation(&GetReplicationMetricsHandler {}),
    )?;

    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/set-remote-target").as_str(),
        AdminOperation(&SetRemoteTargetHandler {}),
    )?;

    r.insert(
        Method::DELETE,
        format!("{}{}", ADMIN_PREFIX, "/v3/remove-remote-target").as_str(),
        AdminOperation(&RemoveRemoteTargetHandler {}),
    )?;

    Ok(())
}

async fn validate_replication_admin_request(req: &S3Request<Body>, action: AdminAction) -> S3Result<Credentials> {
    let Some(input_cred) = req.credentials.as_ref() else {
        return Err(s3_error!(InvalidRequest, "get cred failed"));
    };

    let (cred, owner) =
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

    let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
    validate_admin_request(&req.headers, &cred, owner, false, vec![Action::AdminAction(action)], remote_addr).await?;

    Ok(cred)
}

#[allow(dead_code)]
fn is_local_host(_host: String) -> bool {
    false
}

//awscurl --service s3 --region us-east-1 --access_key rustfsadmin --secret_key rustfsadmin "http://:9000/rustfs/admin/v3/replicationmetrics?bucket=1"
pub struct GetReplicationMetricsHandler {}

#[async_trait::async_trait]
impl Operation for GetReplicationMetricsHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_replication_admin_request(&req, AdminAction::GetReplicationMetricsAction).await?;

        let queries = extract_query_params(&req.uri);

        let Some(bucket) = queries.get("bucket") else {
            return Err(s3_error!(InvalidRequest, "bucket is required"));
        };

        if bucket.is_empty() {
            return Err(s3_error!(InvalidRequest, "bucket is required"));
        }

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        if let Err(err) = get_replication_config(bucket).await {
            if err == StorageError::ConfigNotFound {
                info!("replication configuration not found for bucket '{}'", bucket);
                return Err(S3Error::with_message(
                    S3ErrorCode::ReplicationConfigurationNotFoundError,
                    "replication not found".to_string(),
                ));
            }
            error!("get_replication_config unexpected error: {:?}", err);
            return Err(ApiError::from(err).into());
        }

        // TODO cluster cache
        // In actual implementation, statistics would be obtained from cluster
        // This is simplified to get from local cache
        let bucket_stats = match GLOBAL_REPLICATION_STATS.get() {
            Some(s) => s.get_latest_replication_stats(bucket).await,
            None => BucketStats::default(),
        };

        let data = serde_json::to_vec(&bucket_stats.replication_stats)
            .map_err(|_| S3Error::with_message(S3ErrorCode::InternalError, "serialize failed"))?;
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), headers))
    }
}

pub struct SetRemoteTargetHandler {}

#[async_trait::async_trait]
impl Operation for SetRemoteTargetHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let cred = validate_replication_admin_request(&req, AdminAction::SetBucketTargetAction).await?;

        let queries = extract_query_params(&req.uri);

        let Some(bucket) = queries.get("bucket") else {
            return Err(s3_error!(InvalidRequest, "bucket is required"));
        };

        let update = queries.get("update").is_some_and(|v| v == "true");

        warn!("set remote target, bucket: {}, update: {}", bucket, update);

        if bucket.is_empty() {
            return Err(s3_error!(InvalidRequest, "bucket is required"));
        }

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let body =
            match read_compatible_admin_body(req.input, MAX_ADMIN_REQUEST_BODY_SIZE, req.uri.path(), &cred.secret_key).await {
                Ok(body) => body,
                Err(e) => {
                    warn!("get body failed, e: {:?}", e);
                    return Err(e);
                }
            };

        let mut remote_target: BucketTarget = serde_json::from_slice(&body).map_err(|e| {
            error!("Failed to parse BucketTarget from body: {}", e);
            ApiError::other(e)
        })?;

        let Ok(target_url) = remote_target.url() else {
            return Err(s3_error!(InvalidRequest, "invalid target url"));
        };

        let same_target = rustfs_utils::net::is_local_host(
            target_url.host().unwrap_or(Host::Domain("localhost")),
            target_url.port().unwrap_or(80),
            global_rustfs_port(),
        )
        .unwrap_or_default();

        if same_target && bucket == &remote_target.target_bucket {
            return Err(S3Error::with_message(S3ErrorCode::IncorrectEndpoint, "Same target".to_string()));
        }

        remote_target.source_bucket = bucket.clone();
        let site_endpoint = if remote_target.endpoint.starts_with("http://") || remote_target.endpoint.starts_with("https://") {
            remote_target.endpoint.clone()
        } else if remote_target.secure {
            format!("https://{}", remote_target.endpoint)
        } else {
            format!("http://{}", remote_target.endpoint)
        };
        if let Some(deployment_id) = site_replication_peer_deployment_id_for_endpoint(&site_endpoint).await {
            remote_target.deployment_id = deployment_id;
        }

        let bucket_target_sys = BucketTargetSys::get();

        if !update {
            let (arn, exist) = bucket_target_sys
                .get_remote_arn(bucket, Some(&remote_target), remote_target.deployment_id.as_str())
                .await;
            remote_target.arn = arn.clone();
            if exist && !arn.is_empty() {
                let arn_str = serde_json::to_string(&arn).unwrap_or_default();

                warn!("return exists, arn: {}", arn_str);
                // MinIO-compatible clients encrypt the request payload for this endpoint,
                // but they parse the success response directly as plain JSON string ARN.
                return Ok(S3Response::new((StatusCode::OK, Body::from(arn_str))));
            }
        }

        if remote_target.arn.is_empty() {
            return Err(S3Error::with_message(S3ErrorCode::InvalidRequest, "ARN is empty".to_string()));
        }

        if update {
            let Some(mut target) = bucket_target_sys
                .get_remote_bucket_target_by_arn(bucket, &remote_target.arn)
                .await
            else {
                return Err(S3Error::with_message(S3ErrorCode::InvalidRequest, "Target not found".to_string()));
            };

            target.credentials = remote_target.credentials;
            target.endpoint = remote_target.endpoint;
            target.secure = remote_target.secure;
            target.target_bucket = remote_target.target_bucket;

            target.path = remote_target.path;
            target.replication_sync = remote_target.replication_sync;
            target.bandwidth_limit = remote_target.bandwidth_limit;
            target.health_check_duration = remote_target.health_check_duration;

            warn!("update target, target: {:?}", target);
            remote_target = target;
        }

        let arn = remote_target.arn.clone();

        bucket_target_sys
            .set_target(bucket, &remote_target, update)
            .await
            .map_err(map_bucket_target_error)?;

        let targets = bucket_target_sys.list_bucket_targets(bucket).await.map_err(|e| {
            error!("Failed to list bucket targets: {}", e);
            S3Error::with_message(S3ErrorCode::InternalError, "Failed to list bucket targets".to_string())
        })?;
        let json_targets = serde_json::to_vec(&targets).map_err(|e| {
            error!("Serialization error: {}", e);
            S3Error::with_message(S3ErrorCode::InternalError, "Failed to serialize targets".to_string())
        })?;

        metadata_sys::update(bucket, BUCKET_TARGETS_FILE, json_targets)
            .await
            .map_err(|e| {
                error!("Failed to update bucket targets: {}", e);
                S3Error::with_message(S3ErrorCode::InternalError, format!("Failed to update bucket targets: {e}"))
            })?;

        let arn_str = serde_json::to_string(&arn).unwrap_or_default();

        // MinIO-compatible clients encrypt the request payload for this endpoint,
        // but they parse the success response directly as plain JSON string ARN.
        Ok(S3Response::new((StatusCode::OK, Body::from(arn_str))))
    }
}

pub struct ListRemoteTargetHandler {}

#[async_trait::async_trait]
impl Operation for ListRemoteTargetHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let queries = extract_query_params(&req.uri);
        let Some(_cred) = req.credentials else {
            error!("credentials null");
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        if let Some(bucket) = queries.get("bucket") {
            if bucket.is_empty() {
                error!("bucket parameter is empty");
                return Err(s3_error!(InvalidRequest, "bucket is required"));
            }

            let Some(store) = new_object_layer_fn() else {
                return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not initialized".to_string()));
            };

            store
                .get_bucket_info(bucket, &BucketOptions::default())
                .await
                .map_err(ApiError::from)?;

            let sys = BucketTargetSys::get();
            let targets = sys.list_targets(bucket, "").await;

            let json_targets = serde_json::to_vec(&targets).map_err(|e| {
                error!("Serialization error: {}", e);
                S3Error::with_message(S3ErrorCode::InternalError, "Failed to serialize targets".to_string())
            })?;

            let mut header = HeaderMap::new();
            header.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

            return Ok(S3Response::with_headers((StatusCode::OK, Body::from(json_targets)), header));
        }

        let targets: Vec<BucketTarget> = Vec::new();

        let json_targets = serde_json::to_vec(&targets).map_err(|e| {
            error!("Serialization error: {}", e);
            S3Error::with_message(S3ErrorCode::InternalError, "Failed to serialize targets".to_string())
        })?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(json_targets)), header))
    }
}

pub struct RemoveRemoteTargetHandler {}

#[async_trait::async_trait]
impl Operation for RemoveRemoteTargetHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_replication_admin_request(&req, AdminAction::SetBucketTargetAction).await?;

        debug!("remove remote target called");
        let queries = extract_query_params(&req.uri);
        let Some(bucket) = queries.get("bucket") else {
            return Err(s3_error!(InvalidRequest, "bucket is required"));
        };
        if bucket.is_empty() {
            return Err(s3_error!(InvalidRequest, "bucket is required"));
        }

        let Some(arn_str) = queries.get("arn") else {
            return Err(s3_error!(InvalidRequest, "arn is required"));
        };
        if arn_str.is_empty() {
            return Err(s3_error!(InvalidRequest, "arn is required"));
        };

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not initialized".to_string()));
        };

        store
            .get_bucket_info(bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let sys = BucketTargetSys::get();

        sys.remove_target(bucket, arn_str).await.map_err(map_bucket_target_error)?;

        let targets = sys.list_bucket_targets(bucket).await.map_err(|e| {
            error!("Failed to list bucket targets: {}", e);
            S3Error::with_message(S3ErrorCode::InternalError, "Failed to list bucket targets".to_string())
        })?;

        let json_targets = serde_json::to_vec(&targets).map_err(|e| {
            error!("Serialization error: {}", e);
            S3Error::with_message(S3ErrorCode::InternalError, "Failed to serialize targets".to_string())
        })?;

        metadata_sys::update(bucket, BUCKET_TARGETS_FILE, json_targets)
            .await
            .map_err(|e| {
                error!("Failed to update bucket targets: {}", e);
                S3Error::with_message(S3ErrorCode::InternalError, format!("Failed to update bucket targets: {e}"))
            })?;

        Ok(S3Response::new((StatusCode::NO_CONTENT, Body::from("".to_string()))))
    }
}

#[cfg(test)]
mod tests {
    use super::extract_query_params;
    use http::Uri;

    #[test]
    fn test_extract_query_params_decodes_percent_encoded_values() {
        let uri: Uri = "/rustfs/admin/v3/list-remote-targets?bucket=foo%2Fbar&flag=a+b"
            .parse()
            .expect("uri should parse");
        let params = extract_query_params(&uri);

        assert_eq!(params.get("bucket"), Some(&"foo/bar".to_string()));
        assert_eq!(params.get("flag"), Some(&"a b".to_string()));
    }
}
