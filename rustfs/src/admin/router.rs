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

use crate::admin::console::{is_console_path, make_console_server};
use crate::admin::handlers::oidc::is_oidc_path;
use crate::app::object_usecase::DefaultObjectUsecase;
use crate::auth::{check_key_valid, get_session_token};
use crate::error::ApiError;
use crate::license::license_check;
use crate::server::{ADMIN_PREFIX, HEALTH_PREFIX, HEALTH_READY_PATH, MINIO_ADMIN_PREFIX, PROFILE_CPU_PATH, PROFILE_MEMORY_PATH};
use crate::storage::access::{ReqInfo, authorize_request};
use aws_sdk_s3::primitives::ByteStream as AwsByteStream;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use http::HeaderValue;
use http::header::HeaderName;
use hyper::HeaderMap;
use hyper::Method;
use hyper::StatusCode;
use hyper::Uri;
use hyper::http::Extensions;
use matchit::Params;
use matchit::Router;
use reqwest::Url;
use rustfs_config::notify::NOTIFY_WEBHOOK_SUB_SYS;
use rustfs_config::{
    ENABLE_KEY, WEBHOOK_AUTH_TOKEN, WEBHOOK_CLIENT_CA, WEBHOOK_CLIENT_CERT, WEBHOOK_CLIENT_KEY, WEBHOOK_ENDPOINT,
    WEBHOOK_SKIP_TLS_VERIFY,
};
use rustfs_ecstore::bucket::bandwidth::monitor::BandwidthDetails;
use rustfs_ecstore::bucket::bucket_target_sys::{
    BucketTargetSys, PutObjectOptions, RemoveObjectOptions, S3ClientError, TargetClient,
};
use rustfs_ecstore::bucket::metadata::BUCKET_TARGETS_FILE;
use rustfs_ecstore::bucket::metadata_sys;
use rustfs_ecstore::bucket::replication::{
    BucketReplicationResyncStatus, BucketStats, GLOBAL_REPLICATION_STATS, ObjectOpts, ReplicationConfigurationExt, ResyncOpts,
    get_global_replication_pool,
};
use rustfs_ecstore::bucket::target::{BucketTarget, BucketTargetType, BucketTargets};
use rustfs_ecstore::bucket::versioning::VersioningApi;
use rustfs_ecstore::bucket::versioning_sys::BucketVersioningSys;
use rustfs_ecstore::config::com::read_config_without_migrate;
use rustfs_ecstore::config::{Config, get_global_server_config};
use rustfs_ecstore::global::GLOBAL_BOOT_TIME;
use rustfs_ecstore::notification_sys::get_global_notification_sys;
use rustfs_ecstore::rpc::PeerRestClient;
use rustfs_ecstore::store_api::{BucketOperations, BucketOptions};
use rustfs_ecstore::{
    global::{get_global_bucket_monitor, get_global_deployment_id, get_global_region},
    new_object_layer_fn,
};
use rustfs_filemeta::{ReplicationStatusType, ReplicationType};
use rustfs_madmin::utils::parse_duration;
use rustfs_notify::{Event as NotificationEvent, notification_system};
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_s3_common::EventName;
use rustfs_signer::pre_sign_v4;
use rustfs_utils::http::{
    SUFFIX_SOURCE_DELETEMARKER, SUFFIX_SOURCE_MTIME, SUFFIX_SOURCE_REPLICATION_CHECK, SUFFIX_SOURCE_REPLICATION_REQUEST,
    SUFFIX_SOURCE_VERSION_ID, get_source_scheme, insert_header,
};
use s3s::Body;
use s3s::S3Error;
use s3s::S3ErrorCode;
use s3s::S3Request;
use s3s::S3Response;
use s3s::S3Result;
use s3s::StdError;
use s3s::dto::{GetObjectInput, GetObjectOutput, IfMatch, IfNoneMatch, Range, StreamingBlob, Timestamp, TimestampFormat};
use s3s::header;
use s3s::route::S3Route;
use s3s::s3_error;
use s3s::stream::{ByteStream, DynByteStream};
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::SystemTime;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::sync::{broadcast, mpsc};
use tokio::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use tower::Service;
use tracing::{error, warn};
use url::form_urlencoded;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplicationExtRoute {
    MetricsV1,
    MetricsV2,
    Check,
    ResetStart,
    ResetStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplicationExtRequest {
    bucket: String,
    route: ReplicationExtRoute,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MiscExtRoute {
    ObjectLambda { bucket: String, object: String },
    ListenNotification { bucket: Option<String> },
}

#[derive(Debug, Clone, serde::Serialize, Default)]
struct ReplicationResetResponse {
    #[serde(rename = "Targets")]
    targets: Vec<ReplicationResetTarget>,
}

#[derive(Debug, Clone, serde::Serialize, Default)]
struct ReplicationResetTarget {
    #[serde(rename = "Arn")]
    arn: String,
    #[serde(rename = "ResetID")]
    reset_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplicationResetStartRequest {
    arn: String,
    reset_id: String,
    reset_before: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct ReplicationResetStatusRequest {
    arn: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, Default)]
struct ReplicationResetStatusResponse {
    #[serde(rename = "Targets")]
    targets: Vec<ReplicationResetStatusTarget>,
}

#[derive(Debug, Clone, serde::Serialize, Default)]
struct ReplicationResetStatusTarget {
    #[serde(rename = "Arn")]
    arn: String,
    #[serde(rename = "ResetID")]
    reset_id: String,
    #[serde(
        rename = "ResetBeforeDate",
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    reset_before_date: Option<OffsetDateTime>,
    #[serde(
        rename = "StartTime",
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    start_time: Option<OffsetDateTime>,
    #[serde(
        rename = "EndTime",
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    end_time: Option<OffsetDateTime>,
    #[serde(rename = "Status")]
    status: String,
    #[serde(rename = "ReplicatedCount")]
    replicated_count: i64,
    #[serde(rename = "ReplicatedSize")]
    replicated_size: i64,
    #[serde(rename = "FailedCount")]
    failed_count: i64,
    #[serde(rename = "FailedSize")]
    failed_size: i64,
    #[serde(rename = "Bucket", skip_serializing_if = "String::is_empty")]
    bucket: String,
    #[serde(rename = "Object", skip_serializing_if = "String::is_empty")]
    object: String,
    #[serde(rename = "Error", skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, Default)]
struct ReplicationCheckTargetStatus {
    #[serde(rename = "Arn")]
    arn: String,
    #[serde(rename = "Endpoint")]
    endpoint: String,
    #[serde(rename = "Bucket")]
    bucket: String,
    #[serde(rename = "Status")]
    status: String,
    #[serde(rename = "Error", skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplicationCheckFailureContext {
    BucketCheck,
    VersioningCheck,
    ReplicateObject,
    ReplicateDeleteMarker,
    DeleteObjectVersion,
    ObjectLockCheck,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ListenNotificationFilter {
    bucket: Option<String>,
    event_mask: u64,
    prefix: Option<String>,
    suffix: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObjectLambdaWebhookConfig {
    endpoint: Url,
    auth_token: String,
    client_cert: String,
    client_key: String,
    client_ca: String,
    skip_tls_verify: bool,
    response_header_timeout: Option<Duration>,
}

const LAMBDA_WEBHOOK_SUB_SYS: &str = "lambda_webhook";
const WEBHOOK_RESPONSE_HEADER_TIMEOUT: &str = "response_header_timeout";
const OBJECT_LAMBDA_PRESIGN_EXPIRES_SECS: i64 = 3600;

fn parse_query_pairs(uri: &Uri) -> Vec<(String, String)> {
    uri.query()
        .map(|query| {
            form_urlencoded::parse(query.as_bytes())
                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                .collect()
        })
        .unwrap_or_default()
}

fn query_value_exact(uri: &Uri, key: &str) -> Option<String> {
    parse_query_pairs(uri)
        .into_iter()
        .find_map(|(k, v)| if k == key { Some(v) } else { None })
}

fn query_values_exact(uri: &Uri, key: &str) -> Vec<String> {
    parse_query_pairs(uri)
        .into_iter()
        .filter_map(|(k, v)| if k == key { Some(v) } else { None })
        .collect()
}

fn is_valid_filter_rule_value(value: &str) -> bool {
    if value.len() > 1024 || value.contains('\\') {
        return false;
    }
    !value.split('/').any(|segment| segment == "." || segment == "..")
}

fn extract_bucket_for_bucket_level_path(path: &str) -> Option<String> {
    let bucket = path.strip_prefix('/')?;
    if bucket.is_empty() || bucket.contains('/') {
        return None;
    }
    Some(bucket.to_string())
}

fn extract_bucket_object_path(path: &str) -> Option<(String, String)> {
    let path = path.strip_prefix('/')?;
    let (bucket, object) = path.split_once('/')?;
    if bucket.is_empty() || object.is_empty() {
        return None;
    }
    Some((bucket.to_string(), object.to_string()))
}

fn parse_replication_extension_request(method: &Method, uri: &Uri) -> Option<ReplicationExtRequest> {
    let bucket = extract_bucket_for_bucket_level_path(uri.path())?;

    if method == Method::PUT && query_value_exact(uri, "replication-reset").as_deref() == Some("") {
        return Some(ReplicationExtRequest {
            bucket,
            route: ReplicationExtRoute::ResetStart,
        });
    }

    if method == Method::GET {
        if query_value_exact(uri, "replication-reset-status").as_deref() == Some("") {
            return Some(ReplicationExtRequest {
                bucket,
                route: ReplicationExtRoute::ResetStatus,
            });
        }
        if let Some(value) = query_value_exact(uri, "replication-metrics") {
            if value == "2" {
                return Some(ReplicationExtRequest {
                    bucket,
                    route: ReplicationExtRoute::MetricsV2,
                });
            }
            if value.is_empty() {
                return Some(ReplicationExtRequest {
                    bucket,
                    route: ReplicationExtRoute::MetricsV1,
                });
            }
        }
        if query_value_exact(uri, "replication-check").as_deref() == Some("") {
            return Some(ReplicationExtRequest {
                bucket,
                route: ReplicationExtRoute::Check,
            });
        }
    }

    None
}

fn parse_misc_extension_request(method: &Method, uri: &Uri) -> Option<MiscExtRoute> {
    if method != Method::GET {
        return None;
    }

    if query_value_exact(uri, "lambdaArn").is_some()
        && let Some((bucket, object)) = extract_bucket_object_path(uri.path())
    {
        return Some(MiscExtRoute::ObjectLambda { bucket, object });
    }

    if query_value_exact(uri, "events").is_some() {
        if uri.path() == "/" {
            return Some(MiscExtRoute::ListenNotification { bucket: None });
        }
        let path = uri.path().strip_suffix('/').unwrap_or(uri.path());
        if let Some(bucket) = extract_bucket_for_bucket_level_path(path) {
            return Some(MiscExtRoute::ListenNotification { bucket: Some(bucket) });
        }
    }

    None
}

fn validate_object_lambda_query(uri: &Uri) -> S3Result<()> {
    let lambda_arns = query_values_exact(uri, "lambdaArn");
    if lambda_arns.len() != 1 || lambda_arns[0].trim().is_empty() {
        return Err(s3_error!(InvalidRequest, "lambdaArn query parameter must be provided exactly once"));
    }

    let lambda_arn = lambda_arns[0].trim();
    let arn_parts = lambda_arn.split(':').collect::<Vec<_>>();
    let is_valid_arn = arn_parts.len() >= 6 && arn_parts[0] == "arn" && !arn_parts[1].is_empty() && !arn_parts[2].is_empty();
    if !is_valid_arn {
        return Err(s3_error!(InvalidRequest, "lambdaArn query parameter must be a valid ARN string"));
    }
    Ok(())
}

fn validate_listen_notification_query(uri: &Uri) -> S3Result<()> {
    let events = query_values_exact(uri, "events");
    if events.is_empty() {
        return Err(s3_error!(InvalidArgument, "events query parameter is required"));
    }

    for event in events {
        EventName::parse(&event).map_err(|_| s3_error!(InvalidArgument, "invalid event in events query parameter"))?;
    }

    let prefixes = query_values_exact(uri, "prefix");
    if prefixes.len() > 1 {
        return Err(s3_error!(InvalidArgument, "prefix query parameter must not be repeated"));
    }
    if let Some(prefix) = prefixes.first()
        && !is_valid_filter_rule_value(prefix)
    {
        return Err(s3_error!(InvalidArgument, "invalid prefix filter value"));
    }

    let suffixes = query_values_exact(uri, "suffix");
    if suffixes.len() > 1 {
        return Err(s3_error!(InvalidArgument, "suffix query parameter must not be repeated"));
    }
    if let Some(suffix) = suffixes.first()
        && !is_valid_filter_rule_value(suffix)
    {
        return Err(s3_error!(InvalidArgument, "invalid suffix filter value"));
    }

    let pings = query_values_exact(uri, "ping");
    if pings.len() > 1 {
        return Err(s3_error!(InvalidArgument, "ping query parameter must not be repeated"));
    }
    if let Some(ping) = pings.first() {
        let ping_interval = ping
            .parse::<u64>()
            .map_err(|_| s3_error!(InvalidArgument, "ping query parameter must be a positive integer"))?;
        if ping_interval == 0 {
            return Err(s3_error!(InvalidArgument, "ping query parameter must be greater than zero"));
        }
    }

    Ok(())
}

fn parse_listen_notification_filter(uri: &Uri, bucket: Option<&str>) -> S3Result<ListenNotificationFilter> {
    let mut event_mask = 0_u64;
    for event in query_values_exact(uri, "events") {
        event_mask |= EventName::parse(&event)
            .map_err(|_| s3_error!(InvalidArgument, "invalid event in events query parameter"))?
            .mask();
    }

    Ok(ListenNotificationFilter {
        bucket: bucket.map(str::to_string),
        event_mask,
        prefix: query_value_exact(uri, "prefix").filter(|value| !value.is_empty()),
        suffix: query_value_exact(uri, "suffix").filter(|value| !value.is_empty()),
    })
}

fn validate_misc_extension_request(uri: &Uri, route: &MiscExtRoute) -> S3Result<()> {
    match route {
        MiscExtRoute::ObjectLambda { .. } => validate_object_lambda_query(uri),
        MiscExtRoute::ListenNotification { .. } => validate_listen_notification_query(uri),
    }
}

fn query_pairs_without_key(uri: &Uri, excluded_key: &str) -> Vec<(String, String)> {
    parse_query_pairs(uri)
        .into_iter()
        .filter(|(key, _)| key != excluded_key)
        .collect()
}

fn uri_without_query_key(uri: &Uri, excluded_key: &str) -> S3Result<Uri> {
    let filtered = query_pairs_without_key(uri, excluded_key);
    let mut parts = uri.clone().into_parts();
    parts.path_and_query = if filtered.is_empty() {
        Some(
            uri.path()
                .parse()
                .map_err(|_| s3_error!(InvalidRequest, "failed to rebuild request URI"))?,
        )
    } else {
        let query = form_urlencoded::Serializer::new(String::new())
            .extend_pairs(filtered.iter().map(|(key, value)| (key.as_str(), value.as_str())))
            .finish();
        Some(
            format!("{}?{}", uri.path(), query)
                .parse()
                .map_err(|_| s3_error!(InvalidRequest, "failed to rebuild request URI"))?,
        )
    };
    Uri::from_parts(parts).map_err(|_| s3_error!(InvalidRequest, "failed to rebuild request URI"))
}

fn parse_optional_header(headers: &HeaderMap, name: HeaderName) -> S3Result<Option<String>> {
    headers
        .get(name)
        .map(|value| {
            value
                .to_str()
                .map(|parsed| parsed.to_string())
                .map_err(|_| s3_error!(InvalidRequest, "request header contains invalid utf-8"))
        })
        .transpose()
}

fn parse_optional_timestamp_header(headers: &HeaderMap, name: HeaderName) -> S3Result<Option<Timestamp>> {
    parse_optional_header(headers, name)?
        .map(|value| {
            Timestamp::parse(TimestampFormat::HttpDate, &value)
                .map_err(|_| s3_error!(InvalidRequest, "request timestamp header is invalid"))
        })
        .transpose()
}

fn parse_optional_etag_condition_header<T>(headers: &HeaderMap, name: HeaderName) -> S3Result<Option<T>>
where
    T: std::str::FromStr,
{
    parse_optional_header(headers, name)?
        .map(|value| {
            value
                .parse::<T>()
                .map_err(|_| s3_error!(InvalidRequest, "request etag condition header is invalid"))
        })
        .transpose()
}

fn build_object_lambda_get_request(req: &S3Request<Body>, bucket: &str, object: &str) -> S3Result<S3Request<GetObjectInput>> {
    let filtered_uri = uri_without_query_key(&req.uri, "lambdaArn")?;
    let part_number = query_value_exact(&filtered_uri, "partNumber")
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .parse::<i32>()
                .map_err(|_| s3_error!(InvalidArgument, "partNumber query parameter must be a positive integer"))
        })
        .transpose()?;
    let version_id = query_value_exact(&filtered_uri, "versionId").filter(|value| !value.is_empty());
    let range = parse_optional_header(&req.headers, http::header::RANGE)?
        .map(|value| Range::parse(&value).map_err(|_| s3_error!(InvalidArgument, "Range header is invalid")))
        .transpose()?;

    let mut builder = GetObjectInput::builder()
        .bucket(bucket.to_string())
        .key(object.to_string())
        .part_number(part_number)
        .version_id(version_id)
        .range(range)
        .if_match(parse_optional_etag_condition_header::<IfMatch>(&req.headers, http::header::IF_MATCH)?)
        .if_none_match(parse_optional_etag_condition_header::<IfNoneMatch>(
            &req.headers,
            http::header::IF_NONE_MATCH,
        )?)
        .if_modified_since(parse_optional_timestamp_header(&req.headers, http::header::IF_MODIFIED_SINCE)?)
        .if_unmodified_since(parse_optional_timestamp_header(&req.headers, http::header::IF_UNMODIFIED_SINCE)?);

    builder = builder.sse_customer_algorithm(parse_optional_header(
        &req.headers,
        HeaderName::from_static("x-amz-server-side-encryption-customer-algorithm"),
    )?);
    builder = builder.sse_customer_key(parse_optional_header(
        &req.headers,
        HeaderName::from_static("x-amz-server-side-encryption-customer-key"),
    )?);
    builder = builder.sse_customer_key_md5(parse_optional_header(
        &req.headers,
        HeaderName::from_static("x-amz-server-side-encryption-customer-key-md5"),
    )?);

    let input = builder
        .build()
        .map_err(|err| s3_error!(InvalidRequest, "failed to build object lambda get request: {err}"))?;

    Ok(S3Request {
        input,
        method: req.method.clone(),
        uri: filtered_uri,
        headers: req.headers.clone(),
        extensions: req.extensions.clone(),
        credentials: req.credentials.clone(),
        region: req.region.clone(),
        service: req.service.clone(),
        trailing_headers: req.trailing_headers.clone(),
    })
}

fn parse_object_lambda_arn(uri: &Uri) -> S3Result<rustfs_targets::arn::ARN> {
    let lambda_arn = query_value_exact(uri, "lambdaArn")
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| s3_error!(InvalidRequest, "lambdaArn query parameter must be provided exactly once"))?;

    lambda_arn
        .parse::<rustfs_targets::arn::ARN>()
        .map_err(|_| s3_error!(InvalidRequest, "lambdaArn query parameter must reference a supported target ARN"))
}

fn config_enable_is_on(value: &str) -> bool {
    matches!(value.trim().to_ascii_lowercase().as_str(), "on" | "true" | "yes" | "1")
}

fn resolve_object_lambda_webhook_config_from_server_config(
    config: &Config,
    arn: &rustfs_targets::arn::ARN,
) -> S3Result<ObjectLambdaWebhookConfig> {
    let target_name = arn.target_id.name.to_ascii_lowercase();
    if target_name != "webhook" && !target_name.starts_with("webhook-") {
        return Err(s3_error!(NotImplemented, "object lambda target type is not supported"));
    }

    let subsystem = config
        .0
        .get(LAMBDA_WEBHOOK_SUB_SYS)
        .or_else(|| config.0.get(NOTIFY_WEBHOOK_SUB_SYS))
        .ok_or_else(|| s3_error!(InvalidRequest, "object lambda webhook subsystem is not configured"))?;
    let kvs = subsystem
        .get(&arn.target_id.id)
        .ok_or_else(|| s3_error!(InvalidRequest, "object lambda target is not configured"))?;

    if !config_enable_is_on(&kvs.get(ENABLE_KEY)) {
        return Err(s3_error!(InvalidRequest, "object lambda target is disabled"));
    }

    let endpoint = kvs.lookup(WEBHOOK_ENDPOINT).unwrap_or_default();
    if endpoint.trim().is_empty() {
        return Err(s3_error!(InvalidRequest, "object lambda target endpoint is empty"));
    }

    let response_header_timeout = match kvs.lookup(WEBHOOK_RESPONSE_HEADER_TIMEOUT) {
        Some(value) if value.trim().is_empty() => None,
        Some(value) => Some(
            parse_duration(&value)
                .map_err(|_| s3_error!(InvalidRequest, "object lambda target response_header_timeout is invalid"))?,
        ),
        None => None,
    };

    Ok(ObjectLambdaWebhookConfig {
        endpoint: Url::parse(&endpoint).map_err(|_| s3_error!(InvalidRequest, "object lambda target endpoint is invalid"))?,
        auth_token: kvs.lookup(WEBHOOK_AUTH_TOKEN).unwrap_or_default(),
        client_cert: kvs.lookup(WEBHOOK_CLIENT_CERT).unwrap_or_default(),
        client_key: kvs.lookup(WEBHOOK_CLIENT_KEY).unwrap_or_default(),
        client_ca: kvs.lookup(WEBHOOK_CLIENT_CA).unwrap_or_default(),
        skip_tls_verify: config_enable_is_on(&kvs.lookup(WEBHOOK_SKIP_TLS_VERIFY).unwrap_or_default()),
        response_header_timeout,
    })
}

async fn load_current_server_config() -> S3Result<Config> {
    if let Some(system) = notification_system() {
        return Ok(system.config.read().await.clone());
    }

    if let Some(store) = new_object_layer_fn() {
        match read_config_without_migrate(store).await {
            Ok(config) => return Ok(config),
            Err(err) => {
                warn!("failed to reload current server config for object lambda request: {err}");
            }
        }
    }

    let config = get_global_server_config().ok_or_else(|| s3_error!(InternalError, "server config is not initialized"))?;
    Ok(config)
}

async fn resolve_object_lambda_webhook_config(uri: &Uri) -> S3Result<ObjectLambdaWebhookConfig> {
    let config = load_current_server_config().await?;
    let arn = parse_object_lambda_arn(uri)?;
    resolve_object_lambda_webhook_config_from_server_config(&config, &arn)
}

fn build_object_lambda_http_client(config: &ObjectLambdaWebhookConfig) -> S3Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder().user_agent(rustfs_targets::get_user_agent(rustfs_targets::ServiceType::Basis));

    if let Some(timeout) = config.response_header_timeout {
        builder = builder.timeout(timeout);
    }

    if config.skip_tls_verify {
        builder = builder.danger_accept_invalid_certs(true);
    } else if !config.client_ca.is_empty() {
        let ca_pem = std::fs::read(&config.client_ca)
            .map_err(|e| s3_error!(InternalError, "failed to read object lambda client_ca: {e}"))?;
        let ca = reqwest::Certificate::from_pem(&ca_pem)
            .map_err(|e| s3_error!(InternalError, "failed to parse object lambda client_ca: {e}"))?;
        builder = builder.add_root_certificate(ca);
    }

    if !config.client_cert.is_empty() || !config.client_key.is_empty() {
        if config.client_cert.is_empty() || config.client_key.is_empty() {
            return Err(s3_error!(
                InvalidRequest,
                "object lambda client_cert and client_key must be configured together"
            ));
        }

        let cert = std::fs::read(&config.client_cert)
            .map_err(|e| s3_error!(InternalError, "failed to read object lambda client_cert: {e}"))?;
        let key = std::fs::read(&config.client_key)
            .map_err(|e| s3_error!(InternalError, "failed to read object lambda client_key: {e}"))?;
        let identity = reqwest::Identity::from_pem(&[cert, key].concat())
            .map_err(|e| s3_error!(InternalError, "failed to build object lambda client identity: {e}"))?;
        builder = builder.identity(identity);
    }

    builder
        .build()
        .map_err(|e| s3_error!(InternalError, "failed to build object lambda http client: {e}"))
}

fn extract_request_scheme(headers: &HeaderMap, uri: &Uri) -> String {
    get_source_scheme(headers)
        .and_then(|value| {
            value
                .split(',')
                .next()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
        })
        .or_else(|| uri.scheme_str().map(str::to_owned))
        .unwrap_or_else(|| "http".to_string())
        .to_ascii_lowercase()
}

fn extract_request_host(headers: &HeaderMap, uri: &Uri) -> Option<String> {
    headers
        .get(http::header::HOST)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| uri.authority().map(|authority| authority.as_str().to_string()))
}

fn build_object_lambda_source_url(req: &S3Request<Body>) -> S3Result<String> {
    let credentials = req
        .credentials
        .as_ref()
        .ok_or_else(|| s3_error!(AccessDenied, "object lambda source URL requires authenticated credentials"))?;
    let host = extract_request_host(&req.headers, &req.uri)
        .ok_or_else(|| s3_error!(InvalidRequest, "object lambda source URL requires a valid host header"))?;
    let scheme = extract_request_scheme(&req.headers, &req.uri);
    let filtered_uri = uri_without_query_key(&req.uri, "lambdaArn")?;
    let path_and_query = filtered_uri
        .path_and_query()
        .map(|value| value.as_str().to_string())
        .unwrap_or_else(|| filtered_uri.path().to_string());
    let source_uri = format!("{scheme}://{host}{path_and_query}")
        .parse::<Uri>()
        .map_err(|e| s3_error!(InvalidRequest, "failed to construct object lambda source URL: {e}"))?;
    let region = req
        .region
        .clone()
        .or_else(get_global_region)
        .map(|value| value.as_str().to_string())
        .unwrap_or_else(|| "us-east-1".to_string());
    let session_token = get_session_token(&req.uri, &req.headers).unwrap_or_default().to_string();

    let presigned = pre_sign_v4(
        http::Request::builder()
            .method(Method::GET)
            .uri(source_uri)
            .header(http::header::HOST, host)
            .body(Body::default())
            .map_err(|e| s3_error!(InvalidRequest, "failed to build object lambda source request: {e}"))?,
        &credentials.access_key,
        credentials.secret_key.expose(),
        &session_token,
        &region,
        OBJECT_LAMBDA_PRESIGN_EXPIRES_SECS,
        OffsetDateTime::now_utc(),
    );

    Ok(presigned.uri().to_string())
}

fn build_object_lambda_event_payload(
    req: &S3Request<Body>,
    lambda_arn: &str,
    input_s3_url: &str,
    output_route: &str,
    output_token: &str,
) -> S3Result<Vec<u8>> {
    let request_headers = req
        .headers
        .iter()
        .filter_map(|(name, value)| value.to_str().ok().map(|value| (name.to_string(), value.to_string())))
        .collect::<std::collections::BTreeMap<_, _>>();

    serde_json::to_vec(&serde_json::json!({
        "getObjectContext": {
            "inputS3Url": input_s3_url,
            "outputRoute": output_route,
            "outputToken": output_token,
        },
        "configuration": {
            "accessPointArn": lambda_arn,
        },
        "userRequest": {
            "url": req.uri.to_string(),
            "headers": request_headers,
        },
        "protocolVersion": "rustfs-object-lambda-1.0",
    }))
    .map_err(|e| s3_error!(InternalError, "failed to serialize object lambda payload: {e}"))
}

fn validate_object_lambda_response_auth_headers(headers: &HeaderMap, output_route: &str, output_token: &str) -> S3Result<()> {
    let route = headers
        .get("x-amz-request-route")
        .and_then(|value| value.to_str().ok())
        .map(str::trim);
    let token = headers
        .get("x-amz-request-token")
        .and_then(|value| value.to_str().ok())
        .map(str::trim);

    if route == Some(output_route) && token == Some(output_token) {
        return Ok(());
    }

    Err(s3_error!(
        InvalidRequest,
        "object lambda target response is missing or contains invalid authorization headers"
    ))
}

fn format_timestamp_http_date(value: &Timestamp) -> S3Result<String> {
    let mut buf = Vec::new();
    value
        .format(TimestampFormat::HttpDate, &mut buf)
        .map_err(|_| s3_error!(InternalError, "failed to format timestamp header"))?;
    String::from_utf8(buf).map_err(|_| s3_error!(InternalError, "failed to format timestamp header"))
}

fn insert_string_header(headers: &mut HeaderMap, name: HeaderName, value: String) -> S3Result<()> {
    let header_value =
        HeaderValue::from_str(&value).map_err(|_| s3_error!(InternalError, "failed to build response header value"))?;
    headers.insert(name, header_value);
    Ok(())
}

fn build_get_object_response_headers(output: &GetObjectOutput, base_headers: &HeaderMap) -> S3Result<HeaderMap> {
    let mut headers = base_headers.clone();

    if let Some(accept_ranges) = &output.accept_ranges {
        insert_string_header(&mut headers, http::header::ACCEPT_RANGES, accept_ranges.clone())?;
    }
    if let Some(cache_control) = &output.cache_control {
        insert_string_header(&mut headers, http::header::CACHE_CONTROL, cache_control.clone())?;
    }
    if let Some(content_disposition) = &output.content_disposition {
        insert_string_header(&mut headers, http::header::CONTENT_DISPOSITION, content_disposition.clone())?;
    }
    if let Some(content_encoding) = &output.content_encoding {
        insert_string_header(&mut headers, http::header::CONTENT_ENCODING, content_encoding.clone())?;
    }
    if let Some(content_language) = &output.content_language {
        insert_string_header(&mut headers, http::header::CONTENT_LANGUAGE, content_language.clone())?;
    }
    if let Some(content_length) = output.content_length {
        insert_string_header(&mut headers, http::header::CONTENT_LENGTH, content_length.to_string())?;
    }
    if let Some(content_range) = &output.content_range {
        insert_string_header(&mut headers, http::header::CONTENT_RANGE, content_range.clone())?;
    }
    if let Some(content_type) = &output.content_type {
        insert_string_header(&mut headers, http::header::CONTENT_TYPE, content_type.to_string())?;
    }
    if let Some(etag) = &output.e_tag {
        headers.insert(
            http::header::ETAG,
            etag.to_http_header().map_err(|_| s3_error!(InternalError, "invalid etag"))?,
        );
    }
    if let Some(last_modified) = &output.last_modified {
        insert_string_header(&mut headers, http::header::LAST_MODIFIED, format_timestamp_http_date(last_modified)?)?;
    }
    if let Some(expires) = &output.expires {
        insert_string_header(&mut headers, http::header::EXPIRES, format_timestamp_http_date(expires)?)?;
    }
    if let Some(version_id) = &output.version_id {
        insert_string_header(&mut headers, HeaderName::from_static("x-amz-version-id"), version_id.clone())?;
    }
    if let Some(server_side_encryption) = &output.server_side_encryption {
        insert_string_header(
            &mut headers,
            HeaderName::from_static("x-amz-server-side-encryption"),
            server_side_encryption.as_str().to_string(),
        )?;
    }
    if let Some(sse_customer_algorithm) = &output.sse_customer_algorithm {
        insert_string_header(
            &mut headers,
            HeaderName::from_static("x-amz-server-side-encryption-customer-algorithm"),
            sse_customer_algorithm.clone(),
        )?;
    }
    if let Some(sse_customer_key_md5) = &output.sse_customer_key_md5 {
        insert_string_header(
            &mut headers,
            HeaderName::from_static("x-amz-server-side-encryption-customer-key-md5"),
            sse_customer_key_md5.clone(),
        )?;
    }
    if let Some(sse_kms_key_id) = &output.ssekms_key_id {
        insert_string_header(
            &mut headers,
            HeaderName::from_static("x-amz-server-side-encryption-aws-kms-key-id"),
            sse_kms_key_id.clone(),
        )?;
    }
    if let Some(checksum_crc32) = &output.checksum_crc32 {
        insert_string_header(&mut headers, HeaderName::from_static("x-amz-checksum-crc32"), checksum_crc32.clone())?;
    }
    if let Some(checksum_crc32c) = &output.checksum_crc32c {
        insert_string_header(&mut headers, HeaderName::from_static("x-amz-checksum-crc32c"), checksum_crc32c.clone())?;
    }
    if let Some(checksum_crc64nvme) = &output.checksum_crc64nvme {
        insert_string_header(
            &mut headers,
            HeaderName::from_static("x-amz-checksum-crc64nvme"),
            checksum_crc64nvme.clone(),
        )?;
    }
    if let Some(checksum_sha1) = &output.checksum_sha1 {
        insert_string_header(&mut headers, HeaderName::from_static("x-amz-checksum-sha1"), checksum_sha1.clone())?;
    }
    if let Some(checksum_sha256) = &output.checksum_sha256 {
        insert_string_header(&mut headers, HeaderName::from_static("x-amz-checksum-sha256"), checksum_sha256.clone())?;
    }
    if let Some(checksum_type) = &output.checksum_type {
        insert_string_header(
            &mut headers,
            HeaderName::from_static("x-amz-checksum-type"),
            checksum_type.as_str().to_string(),
        )?;
    }
    if let Some(storage_class) = &output.storage_class {
        insert_string_header(
            &mut headers,
            HeaderName::from_static("x-amz-storage-class"),
            storage_class.as_str().to_string(),
        )?;
    }
    if let Some(tag_count) = output.tag_count {
        insert_string_header(&mut headers, HeaderName::from_static("x-amz-tagging-count"), tag_count.to_string())?;
    }
    if let Some(expiration) = &output.expiration {
        insert_string_header(&mut headers, HeaderName::from_static("x-amz-expiration"), expiration.clone())?;
    }
    if let Some(restore) = &output.restore {
        insert_string_header(&mut headers, HeaderName::from_static("x-amz-restore"), restore.clone())?;
    }

    if let Some(metadata) = &output.metadata {
        for (key, value) in metadata {
            let header_name = format!("x-amz-meta-{key}");
            if let Ok(parsed_name) = HeaderName::from_bytes(header_name.as_bytes()) {
                let parsed_value = HeaderValue::from_str(value)
                    .map_err(|_| s3_error!(InternalError, "failed to build metadata response header"))?;
                headers.insert(parsed_name, parsed_value);
            }
        }
    }

    Ok(headers)
}

#[cfg_attr(not(test), allow(dead_code))]
fn convert_get_object_response(resp: S3Response<GetObjectOutput>) -> S3Result<S3Response<Body>> {
    let headers = build_get_object_response_headers(&resp.output, &resp.headers)?;

    let body = resp.output.body.map(Body::from).unwrap_or_else(|| Body::from(String::new()));

    Ok(S3Response {
        output: body,
        status: resp.status,
        headers,
        extensions: resp.extensions,
    })
}

fn clear_object_lambda_variant_headers(headers: &mut HeaderMap) {
    for name in [
        http::header::ACCEPT_RANGES,
        http::header::CACHE_CONTROL,
        http::header::CONTENT_DISPOSITION,
        http::header::CONTENT_ENCODING,
        http::header::CONTENT_LANGUAGE,
        http::header::CONTENT_LENGTH,
        http::header::CONTENT_RANGE,
        http::header::CONTENT_TYPE,
        http::header::ETAG,
        http::header::LAST_MODIFIED,
        http::header::EXPIRES,
        HeaderName::from_static("x-amz-checksum-crc32"),
        HeaderName::from_static("x-amz-checksum-crc32c"),
        HeaderName::from_static("x-amz-checksum-crc64nvme"),
        HeaderName::from_static("x-amz-checksum-sha1"),
        HeaderName::from_static("x-amz-checksum-sha256"),
        HeaderName::from_static("x-amz-checksum-type"),
        HeaderName::from_static("x-amz-tagging-count"),
        HeaderName::from_static("x-amz-request-route"),
        HeaderName::from_static("x-amz-request-token"),
    ] {
        headers.remove(name);
    }

    let metadata_headers = headers
        .keys()
        .filter(|name| name.as_str().starts_with("x-amz-meta-"))
        .cloned()
        .collect::<Vec<_>>();
    for name in metadata_headers {
        headers.remove(name);
    }
}

fn is_disallowed_object_lambda_response_header(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn build_object_lambda_passthrough_response(
    mut response_headers: HeaderMap,
    lambda_headers: &HeaderMap,
    status: StatusCode,
    body: Body,
) -> S3Response<Body> {
    clear_object_lambda_variant_headers(&mut response_headers);
    for (name, value) in lambda_headers {
        if !is_disallowed_object_lambda_response_header(name) && name != "x-amz-request-route" && name != "x-amz-request-token" {
            response_headers.insert(name.clone(), value.clone());
        }
    }

    S3Response {
        output: body,
        status: Some(status),
        headers: response_headers,
        extensions: Extensions::new(),
    }
}

async fn invoke_object_lambda_target(
    req: &S3Request<Body>,
    bucket: &str,
    object: &str,
    get_resp: S3Response<GetObjectOutput>,
) -> S3Result<S3Response<Body>> {
    let lambda_config = resolve_object_lambda_webhook_config(&req.uri).await?;
    let client = build_object_lambda_http_client(&lambda_config)?;
    let lambda_arn = query_value_exact(&req.uri, "lambdaArn")
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| s3_error!(InvalidRequest, "lambdaArn query parameter must be provided exactly once"))?;
    let input_s3_url = build_object_lambda_source_url(req)?;
    let output_route = Uuid::new_v4().to_string();
    let output_token = Uuid::new_v4().to_string();
    let event_payload = build_object_lambda_event_payload(req, &lambda_arn, &input_s3_url, &output_route, &output_token)?;

    let S3Response {
        output,
        headers: upstream_headers,
        ..
    } = get_resp;

    let response_headers = build_get_object_response_headers(&output, &upstream_headers)?;

    let mut request_builder = client
        .post(lambda_config.endpoint)
        .header("x-rustfs-object-lambda-bucket", bucket)
        .header("x-rustfs-object-lambda-key", object)
        .header("x-rustfs-object-lambda-request-uri", req.uri.to_string())
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(event_payload);

    if !lambda_config.auth_token.is_empty() {
        let tokens = lambda_config.auth_token.split_whitespace().collect::<Vec<_>>();
        request_builder = match tokens.as_slice() {
            [scheme, token] if !scheme.is_empty() && !token.is_empty() => {
                request_builder.header(reqwest::header::AUTHORIZATION, lambda_config.auth_token)
            }
            [token] if !token.is_empty() => request_builder.header(reqwest::header::AUTHORIZATION, format!("Bearer {token}")),
            _ => request_builder,
        };
    }

    if let Some(version_id) = output.version_id.as_deref() {
        request_builder = request_builder.header("x-rustfs-object-lambda-version-id", version_id);
    }

    let lambda_response = request_builder
        .send()
        .await
        .map_err(|e| s3_error!(InternalError, "object lambda target request failed: {e}"))?;

    let status = lambda_response.status();
    let lambda_headers = lambda_response.headers().clone();
    if status.is_success() {
        validate_object_lambda_response_auth_headers(&lambda_headers, &output_route, &output_token)?;
    }
    let body = Body::from(StreamingBlob::wrap(lambda_response.bytes_stream()));
    Ok(build_object_lambda_passthrough_response(response_headers, &lambda_headers, status, body))
}

struct ListenNotificationStream {
    inner: ReceiverStream<Result<Bytes, StdError>>,
}

struct PeerLiveEventCursor {
    client: PeerRestClient,
    next_sequence: u64,
}

const LISTEN_NOTIFICATION_PEER_BATCH_LIMIT: u32 = 128;
const LISTEN_NOTIFICATION_PEER_POLL_INTERVAL: Duration = Duration::from_millis(250);

impl Stream for ListenNotificationStream {
    type Item = Result<Bytes, StdError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = Pin::into_inner(self);
        this.inner.poll_next_unpin(cx)
    }
}

impl ByteStream for ListenNotificationStream {}

fn listen_notification_keepalive_plan(uri: &Uri) -> (Duration, Bytes) {
    if let Some(ping_seconds) = query_value_exact(uri, "ping").and_then(|v| v.parse::<u64>().ok()) {
        return (Duration::from_secs(ping_seconds), Bytes::from_static(b"{\"Records\":[]}\n"));
    }

    (Duration::from_millis(500), Bytes::from_static(b" "))
}

fn event_matches_listen_notification(event: &NotificationEvent, filter: &ListenNotificationFilter) -> bool {
    if let Some(bucket) = &filter.bucket
        && event.s3.bucket.name != *bucket
    {
        return false;
    }

    if filter.event_mask != 0 && event.event_name.mask() & filter.event_mask == 0 {
        return false;
    }

    let object_key = urlencoding::decode(&event.s3.object.key)
        .map(|decoded| decoded.into_owned())
        .unwrap_or_else(|_| event.s3.object.key.clone());

    if let Some(prefix) = &filter.prefix
        && !object_key.starts_with(prefix)
    {
        return false;
    }

    if let Some(suffix) = &filter.suffix
        && !object_key.ends_with(suffix)
    {
        return false;
    }

    true
}

fn serialize_listen_notification_event(event: &NotificationEvent) -> S3Result<Bytes> {
    #[derive(serde::Serialize)]
    struct ListenNotificationEnvelope<'a> {
        #[serde(rename = "Records")]
        records: [&'a NotificationEvent; 1],
    }

    serde_json::to_vec(&ListenNotificationEnvelope { records: [event] })
        .map(|mut payload| {
            payload.push(b'\n');
            Bytes::from(payload)
        })
        .map_err(|e| s3_error!(InternalError, "failed to serialize notification event: {e}"))
}

fn list_remote_live_event_peers() -> Vec<PeerLiveEventCursor> {
    get_global_notification_sys()
        .map(|system| {
            system
                .peer_clients
                .iter()
                .flatten()
                .cloned()
                .map(|client| PeerLiveEventCursor {
                    client,
                    next_sequence: 0,
                })
                .collect()
        })
        .unwrap_or_default()
}

fn deserialize_peer_live_events(payload: &[u8]) -> Result<Vec<NotificationEvent>, serde_json::Error> {
    serde_json::from_slice(payload)
}

async fn fan_in_remote_live_events(
    peers: &mut [PeerLiveEventCursor],
    filter: &ListenNotificationFilter,
    tx: &mpsc::Sender<Result<Bytes, StdError>>,
) -> bool {
    for peer in peers.iter_mut() {
        loop {
            let batch = match tokio::time::timeout(
                Duration::from_secs(2),
                peer.client
                    .get_live_events(peer.next_sequence, LISTEN_NOTIFICATION_PEER_BATCH_LIMIT),
            )
            .await
            {
                Ok(Ok(batch)) => batch,
                Ok(Err(err)) => {
                    warn!("failed to fetch live events from peer {}: {err}", peer.client.host);
                    break;
                }
                Err(_) => {
                    warn!("timed out fetching live events from peer {}", peer.client.host);
                    break;
                }
            };

            peer.next_sequence = batch.next_sequence.max(peer.next_sequence);

            if !batch.events.is_empty() {
                match deserialize_peer_live_events(&batch.events) {
                    Ok(events) => {
                        for event in events {
                            if !event_matches_listen_notification(&event, filter) {
                                continue;
                            }
                            match serialize_listen_notification_event(&event) {
                                Ok(serialized) => {
                                    if tx.send(Ok(serialized)).await.is_err() {
                                        return false;
                                    }
                                }
                                Err(err) => {
                                    warn!("failed to serialize remote listen notification event: {err}");
                                }
                            }
                        }
                    }
                    Err(err) => {
                        warn!("failed to decode live events from peer {}: {err}", peer.client.host);
                    }
                }
            }

            if !batch.truncated {
                break;
            }
        }
    }

    true
}

fn build_listen_notification_response(uri: &Uri, bucket: Option<&str>) -> S3Result<S3Response<Body>> {
    let (interval_duration, payload) = listen_notification_keepalive_plan(uri);
    let filter = parse_listen_notification_filter(uri, bucket)?;
    let mut live_events = notification_system().map(|system| system.subscribe_live_events());
    let mut peer_live_events = list_remote_live_event_peers();

    let (tx, rx) = mpsc::channel(16);
    let stream: DynByteStream = Box::pin(ListenNotificationStream {
        inner: ReceiverStream::new(rx),
    });

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval_duration);
        let mut peer_ticker = tokio::time::interval(LISTEN_NOTIFICATION_PEER_POLL_INTERVAL);
        // Skip the immediate first tick so behavior starts after interval duration.
        ticker.tick().await;
        peer_ticker.tick().await;
        loop {
            if let Some(events_rx) = live_events.as_mut() {
                tokio::select! {
                    _ = tx.closed() => break,
                    _ = ticker.tick() => {
                        if tx.send(Ok(payload.clone())).await.is_err() {
                            break;
                        }
                    }
                    event = events_rx.recv() => {
                        match event {
                            Ok(event) => {
                                if !event_matches_listen_notification(&event, &filter) {
                                    continue;
                                }
                                match serialize_listen_notification_event(&event) {
                                    Ok(serialized) => {
                                        if tx.send(Ok(serialized)).await.is_err() {
                                            break;
                                        }
                                    }
                                    Err(err) => {
                                        warn!("failed to serialize listen notification event: {err}");
                                    }
                                }
                            }
                            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                warn!("listen notification stream lagged and skipped {skipped} events");
                            }
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }
                    _ = peer_ticker.tick(), if !peer_live_events.is_empty() => {
                        if !fan_in_remote_live_events(&mut peer_live_events, &filter, &tx).await {
                            break;
                        }
                    }
                }
            } else {
                tokio::select! {
                    _ = tx.closed() => break,
                    _ = ticker.tick() => {
                        if tx.send(Ok(payload.clone())).await.is_err() {
                            break;
                        }
                    }
                    _ = peer_ticker.tick(), if !peer_live_events.is_empty() => {
                        if !fan_in_remote_live_events(&mut peer_live_events, &filter, &tx).await {
                            break;
                        }
                    }
                }
            }
        }
    });

    let mut resp = S3Response::with_status(Body::from(stream), StatusCode::OK);
    resp.headers
        .insert(header::CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
    resp.headers
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    resp.headers.insert("x-accel-buffering", HeaderValue::from_static("no"));
    Ok(resp)
}

async fn ensure_replication_bucket_exists(bucket: &str) -> S3Result<()> {
    let Some(store) = new_object_layer_fn() else {
        return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init"));
    };

    store
        .get_bucket_info(bucket, &BucketOptions::default())
        .await
        .map_err(ApiError::from)?;

    Ok(())
}

async fn ensure_replication_config_exists(bucket: &str) -> S3Result<()> {
    match metadata_sys::get_replication_config(bucket).await {
        Ok(_) => Ok(()),
        Err(rustfs_ecstore::error::StorageError::ConfigNotFound) => Err(s3_error!(ReplicationConfigurationNotFoundError)),
        Err(err) => Err(ApiError::from(err).into()),
    }
}

async fn build_replication_metrics_response(bucket: &str, route: ReplicationExtRoute) -> S3Result<S3Response<Body>> {
    let bucket_stats = match GLOBAL_REPLICATION_STATS.get() {
        Some(stats) => stats.get_latest_replication_stats(bucket).await,
        None => BucketStats::default(),
    };
    let bucket_stats = apply_replication_metrics_bandwidth_report(bucket_stats, collect_replication_metrics_bandwidth(bucket));
    let bucket_stats = apply_replication_metrics_runtime_fields(bucket_stats, route, replication_metrics_uptime_seconds());

    let body = serialize_replication_metrics_body(&bucket_stats, route)?;

    let mut resp = S3Response::with_status(Body::from(body), StatusCode::OK);
    resp.headers
        .insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    Ok(resp)
}

fn replication_metrics_uptime_seconds() -> i64 {
    GLOBAL_BOOT_TIME
        .get()
        .and_then(|boot_time| SystemTime::now().duration_since(*boot_time).ok())
        .map(|uptime| uptime.as_secs() as i64)
        .unwrap_or_default()
}

fn collect_replication_metrics_bandwidth(bucket: &str) -> HashMap<String, BandwidthDetails> {
    get_global_bucket_monitor()
        .map(|monitor| {
            monitor
                .get_report(|name| name == bucket)
                .bucket_stats
                .into_iter()
                .filter_map(|(opts, details)| {
                    if opts.replication_arn.is_empty() {
                        None
                    } else {
                        Some((opts.replication_arn, details))
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

fn apply_replication_metrics_bandwidth_report(
    mut bucket_stats: BucketStats,
    bandwidth_report: HashMap<String, BandwidthDetails>,
) -> BucketStats {
    for (arn, details) in bandwidth_report {
        let stat = bucket_stats.replication_stats.stats.entry(arn).or_default();
        stat.bandwidth_limit_bytes_per_sec = details.limit_bytes_per_sec;
        stat.current_bandwidth_bytes_per_sec = details.current_bandwidth_bytes_per_sec;
    }

    bucket_stats
}

fn apply_replication_metrics_runtime_fields(
    mut bucket_stats: BucketStats,
    route: ReplicationExtRoute,
    uptime_seconds: i64,
) -> BucketStats {
    if route == ReplicationExtRoute::MetricsV2 {
        bucket_stats.uptime = uptime_seconds;
    }
    bucket_stats
}

fn serialize_replication_metrics_body(bucket_stats: &BucketStats, route: ReplicationExtRoute) -> S3Result<Vec<u8>> {
    match route {
        ReplicationExtRoute::MetricsV1 => {
            serde_json::to_vec(&bucket_stats.replication_stats).map_err(|e| s3_error!(InternalError, "{e}"))
        }
        ReplicationExtRoute::MetricsV2 => serde_json::to_vec(bucket_stats).map_err(|e| s3_error!(InternalError, "{e}")),
        ReplicationExtRoute::Check | ReplicationExtRoute::ResetStart | ReplicationExtRoute::ResetStatus => {
            Err(s3_error!(InternalError, "invalid route for metrics response"))
        }
    }
}

async fn authorize_replication_extension_request(req: &mut S3Request<Body>, ext_req: &ReplicationExtRequest) -> S3Result<()> {
    let Some(input_cred) = req.credentials.as_ref() else {
        return Err(s3_error!(AccessDenied, "Signature is required"));
    };

    let (cred, is_owner) =
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

    req.extensions.insert(ReqInfo {
        cred: Some(cred),
        is_owner,
        bucket: Some(ext_req.bucket.clone()),
        object: None,
        version_id: None,
        region: get_global_region(),
        ..Default::default()
    });

    license_check().map_err(|er| match er.kind() {
        std::io::ErrorKind::PermissionDenied => s3_error!(AccessDenied, "{er}"),
        _ => {
            error!("license check failed due to unexpected error: {er}");
            s3_error!(InternalError, "License validation failed")
        }
    })?;

    let action = match ext_req.route {
        ReplicationExtRoute::MetricsV1 | ReplicationExtRoute::MetricsV2 | ReplicationExtRoute::Check => {
            Action::S3Action(S3Action::GetReplicationConfigurationAction)
        }
        ReplicationExtRoute::ResetStart | ReplicationExtRoute::ResetStatus => {
            Action::S3Action(S3Action::ResetBucketReplicationStateAction)
        }
    };
    authorize_request(req, action).await
}

fn parse_reset_start_target(uri: &Uri) -> S3Result<ReplicationResetStartRequest> {
    let arn = query_value_exact(uri, "arn").filter(|v| !v.is_empty()).unwrap_or_default();

    let now = OffsetDateTime::now_utc();
    let reset_before = match query_value_exact(uri, "older-than").filter(|v| !v.is_empty()) {
        Some(older_than) => {
            let duration = parse_duration(&older_than)
                .map_err(|err| s3_error!(InvalidRequest, "invalid older-than query parameter: {err}"))?;
            let duration = time::Duration::try_from(duration)
                .map_err(|err| s3_error!(InvalidRequest, "invalid older-than query parameter: {err}"))?;
            Some(now - duration)
        }
        None => Some(now),
    };

    let reset_id = query_value_exact(uri, "reset-id")
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    Ok(ReplicationResetStartRequest {
        arn,
        reset_id,
        reset_before,
    })
}

fn collect_resettable_replication_target_arns(config: &s3s::dto::ReplicationConfiguration) -> Vec<String> {
    let mut arns = Vec::new();
    let mut seen = HashSet::new();

    for rule in &config.rules {
        if rule.status == s3s::dto::ReplicationRuleStatus::from_static(s3s::dto::ReplicationRuleStatus::DISABLED) {
            continue;
        }

        let existing_object_enabled = rule.existing_object_replication.as_ref().is_some_and(|status| {
            status.status
                == s3s::dto::ExistingObjectReplicationStatus::from_static(s3s::dto::ExistingObjectReplicationStatus::ENABLED)
        });
        if !existing_object_enabled {
            continue;
        }

        let arn = if config.role.is_empty() {
            rule.destination.bucket.clone()
        } else {
            config.role.clone()
        };

        if seen.insert(arn.clone()) {
            arns.push(arn);
        }

        if !config.role.is_empty() {
            break;
        }
    }

    arns
}

fn resolve_replication_reset_target_arn(config: &s3s::dto::ReplicationConfiguration, requested_arn: &str) -> S3Result<String> {
    let resettable_arns = collect_resettable_replication_target_arns(config);

    if requested_arn.is_empty() {
        return match resettable_arns.as_slice() {
            [] => Err(s3_error!(
                InvalidRequest,
                "replication reset requires a target with existing object replication enabled"
            )),
            [arn] => Ok(arn.clone()),
            _ => Err(s3_error!(
                InvalidRequest,
                "arn query parameter is required when multiple replication targets are configured"
            )),
        };
    }

    let (has_arn, existing_object_enabled) = config.has_existing_object_replication(requested_arn);
    if !has_arn {
        return Err(s3_error!(InvalidRequest, "replication reset arn is not configured for this bucket"));
    }
    if !existing_object_enabled {
        return Err(s3_error!(
            InvalidRequest,
            "replication reset requires existing object replication to be enabled for the target"
        ));
    }

    Ok(requested_arn.to_string())
}

fn build_replication_reset_response(targets: Vec<ReplicationResetTarget>) -> S3Result<S3Response<Body>> {
    let data = serde_json::to_vec(&ReplicationResetResponse { targets }).map_err(|e| s3_error!(InternalError, "{e}"))?;
    let mut resp = S3Response::with_status(Body::from(data), StatusCode::OK);
    resp.headers
        .insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    Ok(resp)
}

fn apply_replication_reset_to_targets(targets: &mut BucketTargets, reset: &ReplicationResetStartRequest) -> S3Result<()> {
    let Some(target) = targets.targets.iter_mut().find(|target| target.arn == reset.arn) else {
        return Err(s3_error!(InvalidRequest, "replication reset arn is not configured for this bucket"));
    };

    target.reset_id = reset.reset_id.clone();
    target.reset_before_date = reset.reset_before;
    Ok(())
}

fn parse_reset_status_target(uri: &Uri) -> ReplicationResetStatusRequest {
    ReplicationResetStatusRequest {
        arn: query_value_exact(uri, "arn").filter(|v| !v.is_empty()),
    }
}

fn build_replication_reset_status_targets(
    status: &BucketReplicationResyncStatus,
    arn_filter: Option<&str>,
) -> Vec<ReplicationResetStatusTarget> {
    let mut targets = status
        .targets_map
        .iter()
        .filter(|(arn, _)| arn_filter.is_none_or(|filter| *arn == filter))
        .map(|(arn, target)| ReplicationResetStatusTarget {
            arn: arn.clone(),
            reset_id: target.resync_id.clone(),
            reset_before_date: target.resync_before_date,
            start_time: target.start_time,
            end_time: target.last_update,
            status: target.resync_status.to_string(),
            replicated_count: target.replicated_count,
            replicated_size: target.replicated_size,
            failed_count: target.failed_count,
            failed_size: target.failed_size,
            bucket: target.bucket.clone(),
            object: target.object.clone(),
            error: target.error.clone(),
        })
        .collect::<Vec<_>>();
    targets.sort_by(|left, right| left.arn.cmp(&right.arn));
    targets
}

fn build_replication_reset_status_response(
    status: BucketReplicationResyncStatus,
    arn_filter: Option<&str>,
) -> S3Result<S3Response<Body>> {
    let data = serde_json::to_vec(&ReplicationResetStatusResponse {
        targets: build_replication_reset_status_targets(&status, arn_filter),
    })
    .map_err(|e| s3_error!(InternalError, "{e}"))?;
    let mut resp = S3Response::with_status(Body::from(data), StatusCode::OK);
    resp.headers
        .insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    Ok(resp)
}

fn build_replication_check_response(mut targets: Vec<ReplicationCheckTargetStatus>) -> S3Result<S3Response<Body>> {
    targets.sort_by(|left, right| left.arn.cmp(&right.arn));

    if let Some(target) = targets.into_iter().find(|target| target.status != "OK") {
        let detail = target.error.unwrap_or_else(|| target.status.to_lowercase());
        return Err(s3_error!(
            InvalidRequest,
            "replication check failed for target {} (bucket {}): {}",
            target.arn,
            target.bucket,
            detail
        ));
    }

    Ok(S3Response::with_status(Body::empty(), StatusCode::OK))
}

fn format_replication_check_client_error(err: &S3ClientError, context: ReplicationCheckFailureContext) -> String {
    if err.code.as_deref() == Some("AccessDenied") {
        return match context {
            ReplicationCheckFailureContext::ReplicateObject => {
                "s3:ReplicateObject permissions missing for replication user".to_string()
            }
            ReplicationCheckFailureContext::ReplicateDeleteMarker => {
                "s3:ReplicateDelete permissions missing for replication user".to_string()
            }
            ReplicationCheckFailureContext::DeleteObjectVersion => {
                "s3:ReplicateDelete/s3:DeleteObject permissions missing for replication user".to_string()
            }
            ReplicationCheckFailureContext::BucketCheck => "target bucket check failed: access denied".to_string(),
            ReplicationCheckFailureContext::VersioningCheck => "target bucket versioning check failed: access denied".to_string(),
            ReplicationCheckFailureContext::ObjectLockCheck => "target object lock check failed: access denied".to_string(),
        };
    }

    let context = match context {
        ReplicationCheckFailureContext::BucketCheck => "target bucket check failed",
        ReplicationCheckFailureContext::VersioningCheck => "target bucket versioning check failed",
        ReplicationCheckFailureContext::ReplicateObject => "target replicate object check failed",
        ReplicationCheckFailureContext::ReplicateDeleteMarker => "target replicate delete-marker check failed",
        ReplicationCheckFailureContext::DeleteObjectVersion => "target delete object version check failed",
        ReplicationCheckFailureContext::ObjectLockCheck => "target object lock check failed",
    };

    match (err.code.as_deref(), err.message.as_deref()) {
        (Some("NoSuchBucket" | "NotFound"), _) => format!("{context}: target bucket does not exist"),
        (Some(code), Some(message)) if !message.is_empty() => format!("{context}: {code}: {message}"),
        (Some(code), _) => format!("{context}: {code}"),
        (None, Some(message)) if !message.is_empty() => format!("{context}: {message}"),
        _ => format!("{context}: {}", err.error),
    }
}

fn is_object_lock_not_enabled_error(err: &S3ClientError) -> bool {
    matches!(
        err.code.as_deref(),
        Some("ObjectLockConfigurationNotFoundError" | "ObjectLockConfigurationNotFound")
    ) || err.message.as_deref().is_some_and(|message| {
        message.contains("Object Lock configuration does not exist")
            || message.contains("Object Lock is not enabled for this bucket")
    })
}

fn validate_replication_check_config_targets(
    targets: &BucketTargets,
    config: &s3s::dto::ReplicationConfiguration,
) -> S3Result<()> {
    let configured_arns = targets
        .targets
        .iter()
        .filter(|target| target.target_type == BucketTargetType::ReplicationService)
        .map(|target| target.arn.as_str())
        .collect::<HashSet<_>>();

    for rule in &config.rules {
        if rule.status == s3s::dto::ReplicationRuleStatus::from_static(s3s::dto::ReplicationRuleStatus::DISABLED) {
            continue;
        }

        let configured_arn = if config.role.is_empty() {
            rule.destination.bucket.as_str()
        } else {
            config.role.as_str()
        };

        if configured_arns.contains(configured_arn) {
            continue;
        }

        return Err(s3_error!(
            InvalidRequest,
            "replication config with rule ID {} has a stale target",
            rule.id.clone().unwrap_or_default()
        ));
    }

    Ok(())
}

fn filter_replication_check_targets(targets: BucketTargets, config: &s3s::dto::ReplicationConfiguration) -> Vec<BucketTarget> {
    let referenced_arns = config
        .filter_target_arns(&ObjectOpts {
            op_type: ReplicationType::All,
            ..Default::default()
        })
        .into_iter()
        .collect::<HashSet<_>>();

    targets
        .targets
        .into_iter()
        .filter(|target| target.target_type == BucketTargetType::ReplicationService)
        .filter(|target| referenced_arns.is_empty() || referenced_arns.contains(&target.arn))
        .collect()
}

async fn check_replication_target(bucket: &str, target: &BucketTarget) -> ReplicationCheckTargetStatus {
    let mut result = ReplicationCheckTargetStatus {
        arn: target.arn.clone(),
        endpoint: target.endpoint.clone(),
        bucket: target.target_bucket.clone(),
        status: "OK".to_string(),
        error: None,
    };

    if target.target_bucket == bucket
        && !target.deployment_id.is_empty()
        && get_global_deployment_id().as_deref() == Some(target.deployment_id.as_str())
    {
        result.status = "FAILED".to_string();
        result.error = Some("target bucket must not match source bucket on the same deployment".to_string());
        return result;
    }

    let target_client = match resolve_replication_target_client(bucket, target).await {
        Ok(client) => client,
        Err(err) => {
            result.status = "FAILED".to_string();
            result.error = Some(err);
            return result;
        }
    };

    match target_client.bucket_exists(&target.target_bucket).await {
        Ok(true) => {}
        Ok(false) => {
            result.status = "FAILED".to_string();
            result.error = Some("target bucket does not exist".to_string());
            return result;
        }
        Err(err) => {
            result.status = "FAILED".to_string();
            result.error = Some(format_replication_check_client_error(&err, ReplicationCheckFailureContext::BucketCheck));
            return result;
        }
    }

    match target_client.get_bucket_versioning(&target.target_bucket).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            result.status = "FAILED".to_string();
            result.error = Some(format!("target bucket {} is not versioned", target.target_bucket));
            return result;
        }
        Err(err) => {
            result.status = "FAILED".to_string();
            result.error = Some(format_replication_check_client_error(
                &err,
                ReplicationCheckFailureContext::VersioningCheck,
            ));
            return result;
        }
    }

    let probe_key = format!(".rustfs-replication-check-{}", Uuid::new_v4());
    let (probe_version_id, probe_time) =
        match put_replication_probe_object(&target_client, &target.target_bucket, &probe_key).await {
            Ok(output) => output,
            Err(err) => {
                result.status = "FAILED".to_string();
                result.error = Some(format_replication_check_client_error(
                    &err,
                    ReplicationCheckFailureContext::ReplicateObject,
                ));
                return result;
            }
        };

    if let Err(err) = delete_replication_probe_object(
        &target_client,
        &target.target_bucket,
        &probe_key,
        probe_version_id.as_deref(),
        build_replication_probe_remove_options(probe_time, true),
    )
    .await
    {
        result.status = "FAILED".to_string();
        result.error = Some(format_replication_check_client_error(
            &err,
            ReplicationCheckFailureContext::ReplicateDeleteMarker,
        ));
        return result;
    }

    if let Err(err) = delete_replication_probe_object(
        &target_client,
        &target.target_bucket,
        &probe_key,
        probe_version_id.as_deref(),
        build_replication_probe_remove_options(probe_time, false),
    )
    .await
    {
        result.status = "FAILED".to_string();
        result.error = Some(format_replication_check_client_error(
            &err,
            ReplicationCheckFailureContext::DeleteObjectVersion,
        ));
        return result;
    }

    result
}

async fn resolve_replication_target_client(bucket: &str, target: &BucketTarget) -> Result<Arc<TargetClient>, String> {
    let target_sys = BucketTargetSys::get();
    match target_sys.get_remote_target_client(bucket, &target.arn).await {
        Some(client) => Ok(client),
        None => target_sys
            .get_remote_target_client_internal(target)
            .await
            .map(Arc::new)
            .map_err(|err| err.to_string()),
    }
}

fn build_replication_probe_put_options(now: OffsetDateTime) -> PutObjectOptions {
    PutObjectOptions {
        internal: rustfs_ecstore::bucket::bucket_target_sys::AdvancedPutOptions {
            source_version_id: Uuid::new_v4().to_string(),
            replication_status: ReplicationStatusType::Replica,
            source_mtime: now,
            replication_request: true,
            replication_validity_check: true,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn build_replication_probe_remove_options(now: OffsetDateTime, replication_delete_marker: bool) -> RemoveObjectOptions {
    RemoveObjectOptions {
        force_delete: false,
        governance_bypass: false,
        replication_delete_marker,
        replication_mtime: Some(now),
        replication_status: ReplicationStatusType::Replica,
        replication_request: true,
        replication_validity_check: true,
    }
}

async fn put_replication_probe_object(
    target_client: &TargetClient,
    target_bucket: &str,
    probe_key: &str,
) -> Result<(Option<String>, OffsetDateTime), S3ClientError> {
    let now = OffsetDateTime::now_utc();
    let options = build_replication_probe_put_options(now);
    let mut headers = HeaderMap::new();
    insert_header(&mut headers, SUFFIX_SOURCE_VERSION_ID, &options.internal.source_version_id);
    insert_header(
        &mut headers,
        SUFFIX_SOURCE_MTIME,
        options.internal.source_mtime.format(&Rfc3339).unwrap_or_default(),
    );
    insert_header(&mut headers, SUFFIX_SOURCE_REPLICATION_REQUEST, "true");
    insert_header(&mut headers, SUFFIX_SOURCE_REPLICATION_CHECK, "true");
    headers.insert(
        HeaderName::from_static("x-amz-replication-status"),
        HeaderValue::from_static(ReplicationStatusType::Replica.as_str()),
    );

    target_client
        .client
        .put_object()
        .bucket(target_bucket)
        .key(probe_key)
        .content_length(8)
        .body(AwsByteStream::from_static(b"aaaaaaaa"))
        .customize()
        .map_request(move |mut req| {
            for (key, value) in headers.clone() {
                req.headers_mut().insert(key.unwrap(), value);
            }
            Result::<_, std::io::Error>::Ok(req)
        })
        .send()
        .await
        .map(|output| (output.version_id().map(ToOwned::to_owned), now))
        .map_err(S3ClientError::from)
}

async fn delete_replication_probe_object(
    target_client: &TargetClient,
    target_bucket: &str,
    probe_key: &str,
    version_id: Option<&str>,
    options: RemoveObjectOptions,
) -> Result<(), S3ClientError> {
    let mut headers = HeaderMap::new();
    if options.replication_delete_marker {
        insert_header(&mut headers, SUFFIX_SOURCE_DELETEMARKER, "true");
    }
    if let Some(replication_mtime) = options.replication_mtime {
        insert_header(&mut headers, SUFFIX_SOURCE_MTIME, replication_mtime.format(&Rfc3339).unwrap_or_default());
    }
    headers.insert(
        HeaderName::from_static("x-amz-replication-status"),
        HeaderValue::from_static(options.replication_status.as_str()),
    );
    if options.replication_request {
        insert_header(&mut headers, SUFFIX_SOURCE_REPLICATION_REQUEST, "true");
    }
    if options.replication_validity_check {
        insert_header(&mut headers, SUFFIX_SOURCE_REPLICATION_CHECK, "true");
    }

    target_client
        .client
        .delete_object()
        .bucket(target_bucket)
        .key(probe_key)
        .set_version_id(version_id.map(ToOwned::to_owned))
        .customize()
        .map_request(move |mut req| {
            for (key, value) in headers.clone() {
                req.headers_mut().insert(key.unwrap(), value);
            }
            Result::<_, std::io::Error>::Ok(req)
        })
        .send()
        .await
        .map(|_| ())
        .map_err(S3ClientError::from)
}

async fn source_bucket_requires_object_lock(bucket: &str) -> S3Result<bool> {
    match metadata_sys::get_object_lock_config(bucket).await {
        Ok((config, _)) => Ok(config
            .object_lock_enabled
            .as_ref()
            .is_some_and(|state| state.as_str() == s3s::dto::ObjectLockEnabled::ENABLED)),
        Err(rustfs_ecstore::error::StorageError::ConfigNotFound) => Ok(false),
        Err(err) => Err(ApiError::from(err).into()),
    }
}

async fn run_replication_check(bucket: &str) -> S3Result<S3Response<Body>> {
    if !BucketVersioningSys::enabled(bucket).await {
        return Err(s3_error!(
            InvalidRequest,
            "replication validation requires bucket versioning to be enabled"
        ));
    }

    let source_requires_object_lock = source_bucket_requires_object_lock(bucket).await?;
    let (config, _) = metadata_sys::get_replication_config(bucket).await.map_err(ApiError::from)?;
    let targets = metadata_sys::list_bucket_targets(bucket).await.map_err(ApiError::from)?;
    validate_replication_check_config_targets(&targets, &config)?;
    let replication_targets = filter_replication_check_targets(targets, &config);

    if replication_targets.is_empty() {
        return Err(s3_error!(
            InvalidRequest,
            "replication check requires at least one configured replication target"
        ));
    }

    let mut statuses = Vec::with_capacity(replication_targets.len());
    for target in &replication_targets {
        let mut status = check_replication_target(bucket, target).await;
        if status.status == "OK" && source_requires_object_lock {
            let target_lock_enabled = match target_client_object_lock_enabled(bucket, target).await {
                Ok(enabled) => enabled,
                Err(err) => {
                    status.status = "FAILED".to_string();
                    status.error = Some(format_replication_check_client_error(
                        &err,
                        ReplicationCheckFailureContext::ObjectLockCheck,
                    ));
                    false
                }
            };
            if status.status == "OK" && !target_lock_enabled {
                status.status = "FAILED".to_string();
                status.error = Some(format!("target bucket {} is not object lock enabled", target.target_bucket));
            }
        }
        statuses.push(status);
    }

    build_replication_check_response(statuses)
}

async fn target_client_object_lock_enabled(bucket: &str, target: &BucketTarget) -> Result<bool, S3ClientError> {
    let target_client = resolve_replication_target_client(bucket, target)
        .await
        .map_err(S3ClientError::new)?;

    match target_client
        .client
        .get_object_lock_configuration()
        .bucket(&target.target_bucket)
        .send()
        .await
    {
        Ok(res) => Ok(res
            .object_lock_configuration()
            .and_then(|cfg| cfg.object_lock_enabled())
            .is_some_and(|state| state.as_str() == "Enabled")),
        Err(err) => {
            let err = S3ClientError::from(err);
            if is_object_lock_not_enabled_error(&err) {
                Ok(false)
            } else {
                Err(err)
            }
        }
    }
}

async fn start_replication_resync(bucket: &str, reset: &ReplicationResetStartRequest) -> S3Result<ReplicationResetTarget> {
    let (config, _) = metadata_sys::get_replication_config(bucket).await.map_err(ApiError::from)?;
    let resolved_arn = resolve_replication_reset_target_arn(&config, &reset.arn)?;
    let mut resolved_reset = reset.clone();
    resolved_reset.arn = resolved_arn.clone();

    let mut targets = metadata_sys::list_bucket_targets(bucket).await.map_err(ApiError::from)?;
    apply_replication_reset_to_targets(&mut targets, &resolved_reset)?;

    let json_targets = serde_json::to_vec(&targets).map_err(|e| s3_error!(InternalError, "{e}"))?;
    metadata_sys::update(bucket, BUCKET_TARGETS_FILE, json_targets)
        .await
        .map_err(ApiError::from)?;
    BucketTargetSys::get().update_all_targets(bucket, Some(&targets)).await;

    let Some(pool) = get_global_replication_pool() else {
        return Err(s3_error!(InternalError, "replication pool is not initialized"));
    };

    pool.start_bucket_resync(ResyncOpts {
        bucket: bucket.to_string(),
        arn: resolved_arn.clone(),
        resync_id: reset.reset_id.clone(),
        resync_before: reset.reset_before,
    })
    .await
    .map_err(|e| s3_error!(InternalError, "{e}"))?;

    Ok(ReplicationResetTarget {
        arn: resolved_arn,
        reset_id: reset.reset_id.clone(),
    })
}

async fn load_replication_resync_status(bucket: &str) -> S3Result<BucketReplicationResyncStatus> {
    let Some(pool) = get_global_replication_pool() else {
        return Err(s3_error!(InternalError, "replication pool is not initialized"));
    };

    pool.get_bucket_resync_status(bucket)
        .await
        .map_err(|e| s3_error!(InternalError, "{e}"))
}

async fn handle_replication_extension_request(
    req: &mut S3Request<Body>,
    ext_req: &ReplicationExtRequest,
) -> S3Result<S3Response<Body>> {
    authorize_replication_extension_request(req, ext_req).await?;
    ensure_replication_bucket_exists(&ext_req.bucket).await?;

    match ext_req.route {
        ReplicationExtRoute::MetricsV1 | ReplicationExtRoute::MetricsV2 => {
            ensure_replication_config_exists(&ext_req.bucket).await?;
            build_replication_metrics_response(&ext_req.bucket, ext_req.route).await
        }
        ReplicationExtRoute::Check => {
            let (versioning, _) = metadata_sys::get_versioning_config(&ext_req.bucket)
                .await
                .map_err(ApiError::from)?;
            if !versioning.enabled() && !BucketVersioningSys::enabled(&ext_req.bucket).await {
                return Err(s3_error!(
                    InvalidRequest,
                    "replication validation requires bucket versioning to be enabled"
                ));
            }
            ensure_replication_config_exists(&ext_req.bucket).await?;
            run_replication_check(&ext_req.bucket).await
        }
        ReplicationExtRoute::ResetStatus => {
            ensure_replication_config_exists(&ext_req.bucket).await?;
            let status_req = parse_reset_status_target(&req.uri);
            let status = load_replication_resync_status(&ext_req.bucket).await?;
            build_replication_reset_status_response(status, status_req.arn.as_deref())
        }
        ReplicationExtRoute::ResetStart => {
            ensure_replication_config_exists(&ext_req.bucket).await?;
            let target = parse_reset_start_target(&req.uri)?;
            let target = start_replication_resync(&ext_req.bucket, &target).await?;
            build_replication_reset_response(vec![target])
        }
    }
}

async fn authorize_misc_extension_request(req: &mut S3Request<Body>, route: &MiscExtRoute) -> S3Result<()> {
    let Some(input_cred) = req.credentials.as_ref() else {
        return Err(s3_error!(AccessDenied, "Signature is required"));
    };

    let (cred, is_owner) =
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

    let (bucket, object, action) = match route {
        MiscExtRoute::ObjectLambda { bucket, object } => {
            (Some(bucket.clone()), Some(object.clone()), Action::S3Action(S3Action::GetObjectAction))
        }
        MiscExtRoute::ListenNotification { bucket: Some(bucket) } => {
            (Some(bucket.clone()), None, Action::S3Action(S3Action::ListenBucketNotificationAction))
        }
        MiscExtRoute::ListenNotification { bucket: None } => (None, None, Action::S3Action(S3Action::ListenNotificationAction)),
    };

    req.extensions.insert(ReqInfo {
        cred: Some(cred),
        is_owner,
        bucket,
        object,
        version_id: None,
        region: get_global_region(),
        ..Default::default()
    });

    license_check().map_err(|er| match er.kind() {
        std::io::ErrorKind::PermissionDenied => s3_error!(AccessDenied, "{er}"),
        _ => {
            error!("license check failed due to unexpected error: {er}");
            s3_error!(InternalError, "License validation failed")
        }
    })?;

    authorize_request(req, action).await
}

async fn handle_misc_extension_request(req: &mut S3Request<Body>, route: &MiscExtRoute) -> S3Result<S3Response<Body>> {
    authorize_misc_extension_request(req, route).await?;
    validate_misc_extension_request(&req.uri, route)?;

    match route {
        MiscExtRoute::ObjectLambda { bucket, object } => {
            let get_req = build_object_lambda_get_request(req, bucket, object)?;
            let usecase = DefaultObjectUsecase::from_global();
            let get_resp = Box::pin(usecase.execute_get_object(get_req)).await?;
            invoke_object_lambda_target(req, bucket, object, get_resp).await
        }
        MiscExtRoute::ListenNotification { bucket } => {
            if let Some(bucket_name) = bucket {
                let Some(store) = new_object_layer_fn() else {
                    return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init"));
                };
                store
                    .get_bucket_info(bucket_name, &BucketOptions::default())
                    .await
                    .map_err(ApiError::from)?;
            }
            build_listen_notification_response(&req.uri, bucket.as_deref())
        }
    }
}

pub struct S3Router<T> {
    router: Router<T>,
    console_enabled: bool,
    console_router: Option<axum::routing::RouterIntoService<Body>>,
}

fn is_public_health_path(path: &str) -> bool {
    path == HEALTH_PREFIX || path == HEALTH_READY_PATH
}

fn is_admin_path(path: &str) -> bool {
    path.starts_with(ADMIN_PREFIX) || path.starts_with(MINIO_ADMIN_PREFIX)
}

fn canonicalize_admin_path(path: &str) -> std::borrow::Cow<'_, str> {
    if let Some(suffix) = path.strip_prefix(MINIO_ADMIN_PREFIX) {
        return std::borrow::Cow::Owned(format!("{ADMIN_PREFIX}{suffix}"));
    }

    std::borrow::Cow::Borrowed(path)
}

impl<T: Operation> S3Router<T> {
    pub fn new(console_enabled: bool) -> Self {
        let router = Router::new();

        let console_router = if console_enabled {
            Some(make_console_server().into_service::<Body>())
        } else {
            None
        };

        Self {
            router,
            console_enabled,
            console_router,
        }
    }

    pub fn insert(&mut self, method: Method, path: &str, operation: T) -> std::io::Result<()> {
        let path = Self::make_route_str(method, path);

        // warn!("set uri {}", &path);

        self.router.insert(path, operation).map_err(std::io::Error::other)?;

        Ok(())
    }

    fn make_route_str(method: Method, path: &str) -> String {
        format!("{}|{}", method.as_str(), path)
    }
}

#[cfg(test)]
impl<T: Operation> S3Router<T> {
    pub(crate) fn contains_route(&self, method: Method, path: &str) -> bool {
        let route = Self::make_route_str(method, path);
        self.router.at(&route).is_ok()
    }

    pub(crate) fn contains_compatible_route(&self, method: Method, path: &str) -> bool {
        let canonical_path = canonicalize_admin_path(path);
        let route = Self::make_route_str(method, canonical_path.as_ref());
        self.router.at(&route).is_ok()
    }
}

impl<T: Operation> Default for S3Router<T> {
    fn default() -> Self {
        Self::new(false)
    }
}

#[async_trait::async_trait]
impl<T> S3Route for S3Router<T>
where
    T: Operation,
{
    fn is_match(&self, method: &Method, uri: &Uri, headers: &HeaderMap, _: &mut Extensions) -> bool {
        if parse_replication_extension_request(method, uri).is_some() || parse_misc_extension_request(method, uri).is_some() {
            return true;
        }

        let path = uri.path();

        // Profiling endpoints
        if method == Method::GET && (path == PROFILE_CPU_PATH || path == PROFILE_MEMORY_PATH) {
            return true;
        }

        // Health check
        if (method == Method::HEAD || method == Method::GET) && is_public_health_path(path) {
            return true;
        }

        // AssumeRole
        if method == Method::POST
            && path == "/"
            && headers
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(|ct| ct.split(';').next().unwrap_or("").trim().to_lowercase())
                .map(|ct| ct == "application/x-www-form-urlencoded")
                .unwrap_or(false)
        {
            return true;
        }

        is_admin_path(path) || is_console_path(path)
    }

    // check_access before call
    async fn check_access(&self, req: &mut S3Request<Body>) -> S3Result<()> {
        if parse_replication_extension_request(&req.method, &req.uri).is_some()
            || parse_misc_extension_request(&req.method, &req.uri).is_some()
        {
            return match req.credentials {
                Some(_) => Ok(()),
                None => Err(s3_error!(AccessDenied, "Signature is required")),
            };
        }

        // Allow unauthenticated access to health check
        let path = req.uri.path();

        // Profiling endpoints
        if req.method == Method::GET && (path == PROFILE_CPU_PATH || path == PROFILE_MEMORY_PATH) {
            return Ok(());
        }

        // Health check
        if (req.method == Method::HEAD || req.method == Method::GET) && is_public_health_path(path) {
            return Ok(());
        }

        // Allow unauthenticated access to console static files if console is enabled
        if self.console_enabled && is_console_path(path) {
            return Ok(());
        }

        // Allow unauthenticated access to OIDC endpoints (user not yet authenticated)
        if is_oidc_path(path) {
            return Ok(());
        }

        // Allow unauthenticated STS requests to POST / (AssumeRoleWithWebIdentity
        // doesn't use SigV4 — the JWT token in the request body is the authentication).
        // The handler dispatches on the Action parameter: AssumeRole will reject if
        // credentials are missing, AssumeRoleWithWebIdentity will validate the JWT.
        // Require application/x-www-form-urlencoded Content-Type to narrow the bypass.
        if req.method == Method::POST
            && path == "/"
            && req.credentials.is_none()
            && req
                .headers
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(|ct| {
                    ct.split(';')
                        .next()
                        .unwrap_or("")
                        .trim()
                        .eq_ignore_ascii_case("application/x-www-form-urlencoded")
                })
                .unwrap_or(false)
        {
            return Ok(());
        }

        // For non-RPC admin requests, check credentials
        match req.credentials {
            Some(_) => Ok(()),
            None => Err(s3_error!(AccessDenied, "Signature is required")),
        }
    }

    async fn call(&self, mut req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        if let Some(ext_req) = parse_replication_extension_request(&req.method, &req.uri) {
            return handle_replication_extension_request(&mut req, &ext_req).await;
        }
        if let Some(ext_req) = parse_misc_extension_request(&req.method, &req.uri) {
            return Box::pin(handle_misc_extension_request(&mut req, &ext_req)).await;
        }

        // Console requests should be handled by console router first (including OPTIONS)
        // Console has its own CORS layer configured
        if self.console_enabled && is_console_path(req.uri.path()) {
            if let Some(console_router) = &self.console_router {
                let mut console_router = console_router.clone();
                let req = convert_request(req);
                let result = console_router.call(req).await;
                return match result {
                    Ok(resp) => Ok(convert_response(resp)),
                    Err(e) => Err(s3_error!(InternalError, "{}", e)),
                };
            }
            return Err(s3_error!(InternalError, "console is not enabled"));
        }

        let canonical_path = canonicalize_admin_path(req.uri.path());
        let uri = format!("{}|{}", &req.method, canonical_path.as_ref());

        if let Ok(mat) = self.router.at(&uri) {
            let op: &T = mat.value;
            let mut resp = op.call(req, mat.params).await?;
            resp.status = Some(resp.output.0);
            let response = resp.map_output(|x| x.1);

            return Ok(response);
        }

        Err(s3_error!(NotImplemented))
    }
}

#[async_trait::async_trait]
pub trait Operation: Send + Sync + 'static {
    // fn method() -> Method;
    // fn uri() -> &'static str;
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>>;
}

pub struct AdminOperation(pub &'static dyn Operation);

#[async_trait::async_trait]
impl Operation for AdminOperation {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        self.0.call(req, params).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderMap;
    use http::Method;
    use http::Uri;
    use s3s::S3Request;
    use time::macros::datetime;

    #[test]
    fn canonicalize_admin_path_maps_compat_prefix_to_rustfs_prefix() {
        assert_eq!(canonicalize_admin_path("/minio/admin/v3/info").as_ref(), "/rustfs/admin/v3/info");
        assert_eq!(canonicalize_admin_path("/rustfs/admin/v3/info").as_ref(), "/rustfs/admin/v3/info");
    }

    #[test]
    fn is_admin_path_accepts_rustfs_and_compat_prefixes() {
        assert!(is_admin_path("/rustfs/admin/v3/info"));
        assert!(is_admin_path("/minio/admin/v3/info"));
        assert!(!is_admin_path("/bucket/object"));
    }

    #[test]
    fn parse_replication_extension_request_matches_metrics_and_check() {
        let metrics: Uri = "/demo-bucket?replication-metrics".parse().expect("uri should parse");
        let metrics_v2: Uri = "/demo-bucket?replication-metrics=2".parse().expect("uri should parse");
        let check: Uri = "/demo-bucket?replication-check".parse().expect("uri should parse");
        let reset_status: Uri = "/demo-bucket?replication-reset-status".parse().expect("uri should parse");
        let reset_start: Uri = "/demo-bucket?replication-reset".parse().expect("uri should parse");

        let m = parse_replication_extension_request(&Method::GET, &metrics).expect("metrics route should parse");
        assert_eq!(m.bucket, "demo-bucket");
        assert_eq!(m.route, ReplicationExtRoute::MetricsV1);

        let v2 = parse_replication_extension_request(&Method::GET, &metrics_v2).expect("metrics v2 route should parse");
        assert_eq!(v2.bucket, "demo-bucket");
        assert_eq!(v2.route, ReplicationExtRoute::MetricsV2);

        let c = parse_replication_extension_request(&Method::GET, &check).expect("check route should parse");
        assert_eq!(c.bucket, "demo-bucket");
        assert_eq!(c.route, ReplicationExtRoute::Check);

        let rs = parse_replication_extension_request(&Method::GET, &reset_status).expect("reset status route should parse");
        assert_eq!(rs.bucket, "demo-bucket");
        assert_eq!(rs.route, ReplicationExtRoute::ResetStatus);

        let r = parse_replication_extension_request(&Method::PUT, &reset_start).expect("reset start route should parse");
        assert_eq!(r.bucket, "demo-bucket");
        assert_eq!(r.route, ReplicationExtRoute::ResetStart);
    }

    #[test]
    fn parse_replication_extension_request_rejects_object_level_and_invalid_query_values() {
        let object_level: Uri = "/demo-bucket/path/file?replication-metrics"
            .parse()
            .expect("uri should parse");
        let bucket_trailing_slash: Uri = "/demo-bucket/?replication-metrics".parse().expect("uri should parse");
        let invalid_value: Uri = "/demo-bucket?replication-metrics=1".parse().expect("uri should parse");
        let wrong_method: Uri = "/demo-bucket?replication-check".parse().expect("uri should parse");
        let wrong_method_reset: Uri = "/demo-bucket?replication-reset".parse().expect("uri should parse");
        let wrong_method_status: Uri = "/demo-bucket?replication-reset-status".parse().expect("uri should parse");

        assert!(parse_replication_extension_request(&Method::GET, &object_level).is_none());
        assert!(parse_replication_extension_request(&Method::GET, &bucket_trailing_slash).is_none());
        assert!(parse_replication_extension_request(&Method::GET, &invalid_value).is_none());
        assert!(parse_replication_extension_request(&Method::PUT, &wrong_method).is_none());
        assert!(parse_replication_extension_request(&Method::GET, &wrong_method_reset).is_none());
        assert!(parse_replication_extension_request(&Method::PUT, &wrong_method_status).is_none());
    }

    #[test]
    fn parse_reset_start_target_defaults_reset_before_and_supports_older_than() {
        let no_window: Uri = "/demo-bucket?replication-reset".parse().expect("uri should parse");
        let before_default = OffsetDateTime::now_utc();
        let parsed_default = parse_reset_start_target(&no_window).expect("default reset request should parse");
        let after_default = OffsetDateTime::now_utc();

        assert!(parsed_default.arn.is_empty());
        assert!(!parsed_default.reset_id.is_empty());
        let reset_before = parsed_default.reset_before.expect("default reset window should be set");
        assert!(reset_before >= before_default && reset_before <= after_default);

        let older_than: Uri = "/demo-bucket?replication-reset&arn=arn:target&reset-id=rid-1&older-than=1h"
            .parse()
            .expect("uri should parse");
        let before_window = OffsetDateTime::now_utc();
        let parsed_window = parse_reset_start_target(&older_than).expect("older-than reset request should parse");
        let after_window = OffsetDateTime::now_utc();

        assert_eq!(parsed_window.reset_id, "rid-1");
        let reset_before = parsed_window.reset_before.expect("older-than reset window should be set");
        assert!(reset_before <= after_window - time::Duration::minutes(59));
        assert!(reset_before >= before_window - time::Duration::hours(1) - time::Duration::seconds(1));
    }

    #[test]
    fn resolve_replication_reset_target_arn_uses_single_existing_object_target_by_default() {
        let config = s3s::dto::ReplicationConfiguration {
            role: String::new(),
            rules: vec![s3s::dto::ReplicationRule {
                delete_marker_replication: None,
                delete_replication: None,
                destination: s3s::dto::Destination {
                    bucket: "arn:replication:a".to_string(),
                    ..Default::default()
                },
                existing_object_replication: Some(s3s::dto::ExistingObjectReplication {
                    status: s3s::dto::ExistingObjectReplicationStatus::from_static(
                        s3s::dto::ExistingObjectReplicationStatus::ENABLED,
                    ),
                }),
                filter: None,
                id: Some("rule-a".to_string()),
                prefix: Some(String::new()),
                priority: None,
                source_selection_criteria: None,
                status: s3s::dto::ReplicationRuleStatus::from_static(s3s::dto::ReplicationRuleStatus::ENABLED),
            }],
        };

        let resolved = resolve_replication_reset_target_arn(&config, "").expect("single target should resolve");
        assert_eq!(resolved, "arn:replication:a");
    }

    #[test]
    fn resolve_replication_reset_target_arn_requires_arn_for_multiple_targets() {
        let config = s3s::dto::ReplicationConfiguration {
            role: String::new(),
            rules: vec![
                s3s::dto::ReplicationRule {
                    delete_marker_replication: None,
                    delete_replication: None,
                    destination: s3s::dto::Destination {
                        bucket: "arn:replication:a".to_string(),
                        ..Default::default()
                    },
                    existing_object_replication: Some(s3s::dto::ExistingObjectReplication {
                        status: s3s::dto::ExistingObjectReplicationStatus::from_static(
                            s3s::dto::ExistingObjectReplicationStatus::ENABLED,
                        ),
                    }),
                    filter: None,
                    id: Some("rule-a".to_string()),
                    prefix: Some(String::new()),
                    priority: None,
                    source_selection_criteria: None,
                    status: s3s::dto::ReplicationRuleStatus::from_static(s3s::dto::ReplicationRuleStatus::ENABLED),
                },
                s3s::dto::ReplicationRule {
                    delete_marker_replication: None,
                    delete_replication: None,
                    destination: s3s::dto::Destination {
                        bucket: "arn:replication:b".to_string(),
                        ..Default::default()
                    },
                    existing_object_replication: Some(s3s::dto::ExistingObjectReplication {
                        status: s3s::dto::ExistingObjectReplicationStatus::from_static(
                            s3s::dto::ExistingObjectReplicationStatus::ENABLED,
                        ),
                    }),
                    filter: None,
                    id: Some("rule-b".to_string()),
                    prefix: Some(String::new()),
                    priority: None,
                    source_selection_criteria: None,
                    status: s3s::dto::ReplicationRuleStatus::from_static(s3s::dto::ReplicationRuleStatus::ENABLED),
                },
            ],
        };

        let err = resolve_replication_reset_target_arn(&config, "").expect_err("multiple targets should require arn");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert!(err.message().unwrap_or_default().contains("arn query parameter is required"));
    }

    #[test]
    fn resolve_replication_reset_target_arn_rejects_target_without_existing_object_replication() {
        let config = s3s::dto::ReplicationConfiguration {
            role: String::new(),
            rules: vec![s3s::dto::ReplicationRule {
                delete_marker_replication: None,
                delete_replication: None,
                destination: s3s::dto::Destination {
                    bucket: "arn:replication:a".to_string(),
                    ..Default::default()
                },
                existing_object_replication: Some(s3s::dto::ExistingObjectReplication {
                    status: s3s::dto::ExistingObjectReplicationStatus::from_static(
                        s3s::dto::ExistingObjectReplicationStatus::DISABLED,
                    ),
                }),
                filter: None,
                id: Some("rule-a".to_string()),
                prefix: Some(String::new()),
                priority: None,
                source_selection_criteria: None,
                status: s3s::dto::ReplicationRuleStatus::from_static(s3s::dto::ReplicationRuleStatus::ENABLED),
            }],
        };

        let err = resolve_replication_reset_target_arn(&config, "arn:replication:a")
            .expect_err("target without existing object replication should fail");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert!(err.message().unwrap_or_default().contains("existing object replication"));
    }

    #[test]
    fn apply_replication_reset_to_targets_updates_matching_target() {
        let mut targets = BucketTargets {
            targets: vec![rustfs_ecstore::bucket::target::BucketTarget {
                arn: "arn:target".to_string(),
                ..Default::default()
            }],
        };
        let reset = ReplicationResetStartRequest {
            arn: "arn:target".to_string(),
            reset_id: "rid-1".to_string(),
            reset_before: Some(OffsetDateTime::now_utc()),
        };

        apply_replication_reset_to_targets(&mut targets, &reset).expect("target update should succeed");

        assert_eq!(targets.targets[0].reset_id, "rid-1");
        assert_eq!(targets.targets[0].reset_before_date, reset.reset_before);
    }

    #[test]
    fn build_replication_reset_status_response_serializes_sorted_targets() {
        let mut status = BucketReplicationResyncStatus::new();
        status.targets_map.insert(
            "arn:z".to_string(),
            rustfs_ecstore::bucket::replication::TargetReplicationResyncStatus {
                resync_id: "rid-z".to_string(),
                last_update: Some(datetime!(2025-01-03 00:00 UTC)),
                resync_status: rustfs_ecstore::bucket::replication::ResyncStatusType::ResyncFailed,
                failed_count: 2,
                failed_size: 4,
                bucket: "bucket-z".to_string(),
                error: Some("boom".to_string()),
                ..Default::default()
            },
        );
        status.targets_map.insert(
            "arn:a".to_string(),
            rustfs_ecstore::bucket::replication::TargetReplicationResyncStatus {
                resync_id: "rid-a".to_string(),
                last_update: Some(datetime!(2025-01-02 00:00 UTC)),
                resync_status: rustfs_ecstore::bucket::replication::ResyncStatusType::ResyncCompleted,
                replicated_count: 3,
                replicated_size: 9,
                bucket: "bucket-a".to_string(),
                ..Default::default()
            },
        );

        let response = build_replication_reset_status_response(status, None).expect("status response should build");
        let bytes = futures::executor::block_on(http_body_util::BodyExt::collect(response.output))
            .expect("body should read")
            .to_bytes();
        let payload: serde_json::Value = serde_json::from_slice(&bytes).expect("response must be json");

        assert_eq!(payload["Targets"][0]["Arn"], "arn:a");
        assert_eq!(payload["Targets"][0]["Bucket"], "bucket-a");
        assert_eq!(payload["Targets"][0]["Status"], "Completed");
        assert_eq!(payload["Targets"][0]["EndTime"], "2025-01-02T00:00:00Z");
        assert_eq!(payload["Targets"][1]["Arn"], "arn:z");
        assert_eq!(payload["Targets"][1]["Bucket"], "bucket-z");
        assert_eq!(payload["Targets"][1]["Status"], "Failed");
        assert_eq!(payload["Targets"][1]["EndTime"], "2025-01-03T00:00:00Z");
        assert_eq!(payload["Targets"][1]["Error"], "boom");
    }

    #[test]
    fn build_replication_reset_status_response_filters_targets_by_arn() {
        let mut status = BucketReplicationResyncStatus::new();
        status.targets_map.insert(
            "arn:z".to_string(),
            rustfs_ecstore::bucket::replication::TargetReplicationResyncStatus {
                resync_id: "rid-z".to_string(),
                last_update: Some(datetime!(2025-02-03 00:00 UTC)),
                resync_status: rustfs_ecstore::bucket::replication::ResyncStatusType::ResyncFailed,
                failed_count: 2,
                failed_size: 4,
                bucket: "bucket-z".to_string(),
                error: Some("boom".to_string()),
                ..Default::default()
            },
        );
        status.targets_map.insert(
            "arn:a".to_string(),
            rustfs_ecstore::bucket::replication::TargetReplicationResyncStatus {
                resync_id: "rid-a".to_string(),
                last_update: Some(datetime!(2025-02-02 00:00 UTC)),
                resync_status: rustfs_ecstore::bucket::replication::ResyncStatusType::ResyncCompleted,
                replicated_count: 3,
                replicated_size: 9,
                bucket: "bucket-a".to_string(),
                ..Default::default()
            },
        );

        let response = build_replication_reset_status_response(status, Some("arn:z")).expect("status response should build");
        let bytes = futures::executor::block_on(http_body_util::BodyExt::collect(response.output))
            .expect("body should read")
            .to_bytes();
        let payload: serde_json::Value = serde_json::from_slice(&bytes).expect("response must be json");

        assert_eq!(payload["Targets"].as_array().map(Vec::len), Some(1));
        assert_eq!(payload["Targets"][0]["Arn"], "arn:z");
        assert_eq!(payload["Targets"][0]["Bucket"], "bucket-z");
        assert_eq!(payload["Targets"][0]["Status"], "Failed");
        assert_eq!(payload["Targets"][0]["EndTime"], "2025-02-03T00:00:00Z");
        assert_eq!(payload["Targets"][0]["Error"], "boom");
    }

    #[test]
    fn build_replication_check_response_returns_empty_body_on_success() {
        let response = build_replication_check_response(vec![
            ReplicationCheckTargetStatus {
                arn: "arn:a".to_string(),
                endpoint: "remote-a:9000".to_string(),
                bucket: "bucket-a".to_string(),
                status: "OK".to_string(),
                error: None,
            },
            ReplicationCheckTargetStatus {
                arn: "arn:z".to_string(),
                endpoint: "remote-z:9000".to_string(),
                bucket: "bucket-z".to_string(),
                status: "OK".to_string(),
                error: None,
            },
        ])
        .expect("response should build");

        let bytes = futures::executor::block_on(http_body_util::BodyExt::collect(response.output))
            .expect("body should read")
            .to_bytes();
        assert!(bytes.is_empty());
    }

    #[test]
    fn build_replication_check_response_surfaces_first_failure() {
        let err = build_replication_check_response(vec![
            ReplicationCheckTargetStatus {
                arn: "arn:z".to_string(),
                endpoint: "remote-z:9000".to_string(),
                bucket: "bucket-z".to_string(),
                status: "FAILED".to_string(),
                error: Some("boom".to_string()),
            },
            ReplicationCheckTargetStatus {
                arn: "arn:a".to_string(),
                endpoint: "remote-a:9000".to_string(),
                bucket: "bucket-a".to_string(),
                status: "OK".to_string(),
                error: None,
            },
        ])
        .expect_err("failed target should surface as request error");

        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert!(err.message().unwrap_or_default().contains("arn:z"));
    }

    #[test]
    fn build_replication_check_response_rejects_empty_target_list_at_runtime_boundary() {
        let config = s3s::dto::ReplicationConfiguration {
            role: String::new(),
            rules: vec![],
        };
        let replication_targets = filter_replication_check_targets(BucketTargets::default(), &config);

        assert!(replication_targets.is_empty());
    }

    #[test]
    fn format_replication_check_client_error_prefers_structured_access_denied() {
        let err = S3ClientError::with_metadata(
            "AccessDenied: denied",
            None,
            Some("AccessDenied".to_string()),
            Some("denied".to_string()),
        );

        let formatted = format_replication_check_client_error(&err, ReplicationCheckFailureContext::BucketCheck);
        assert_eq!(formatted, "target bucket check failed: access denied");
    }

    #[test]
    fn format_replication_check_client_error_uses_remote_code_and_message() {
        let err = S3ClientError::with_metadata(
            "InvalidRequest: bucket versioning is suspended",
            None,
            Some("InvalidRequest".to_string()),
            Some("bucket versioning is suspended".to_string()),
        );

        let formatted = format_replication_check_client_error(&err, ReplicationCheckFailureContext::VersioningCheck);
        assert_eq!(
            formatted,
            "target bucket versioning check failed: InvalidRequest: bucket versioning is suspended"
        );
    }

    #[test]
    fn format_replication_check_client_error_maps_replicate_permission_failures() {
        let err = S3ClientError::with_metadata(
            "AccessDenied: denied",
            None,
            Some("AccessDenied".to_string()),
            Some("denied".to_string()),
        );

        let replicate_object = format_replication_check_client_error(&err, ReplicationCheckFailureContext::ReplicateObject);
        assert_eq!(replicate_object, "s3:ReplicateObject permissions missing for replication user");

        let replicate_delete = format_replication_check_client_error(&err, ReplicationCheckFailureContext::ReplicateDeleteMarker);
        assert_eq!(replicate_delete, "s3:ReplicateDelete permissions missing for replication user");

        let delete_object = format_replication_check_client_error(&err, ReplicationCheckFailureContext::DeleteObjectVersion);
        assert_eq!(
            delete_object,
            "s3:ReplicateDelete/s3:DeleteObject permissions missing for replication user"
        );
    }

    #[test]
    fn is_object_lock_not_enabled_error_recognizes_missing_configuration() {
        let code_only = S3ClientError::with_metadata(
            "ObjectLockConfigurationNotFoundError: missing",
            None,
            Some("ObjectLockConfigurationNotFoundError".to_string()),
            Some("missing".to_string()),
        );
        assert!(is_object_lock_not_enabled_error(&code_only));

        let message_only = S3ClientError::with_metadata(
            "Object Lock is not enabled for this bucket",
            None,
            None,
            Some("Object Lock is not enabled for this bucket".to_string()),
        );
        assert!(is_object_lock_not_enabled_error(&message_only));

        let access_denied = S3ClientError::with_metadata(
            "AccessDenied: denied",
            None,
            Some("AccessDenied".to_string()),
            Some("denied".to_string()),
        );
        assert!(!is_object_lock_not_enabled_error(&access_denied));
    }

    #[test]
    fn filter_replication_check_targets_only_keeps_configured_replication_targets() {
        let targets = BucketTargets {
            targets: vec![
                BucketTarget {
                    arn: "arn:replication:a".to_string(),
                    target_type: BucketTargetType::ReplicationService,
                    ..Default::default()
                },
                BucketTarget {
                    arn: "arn:replication:b".to_string(),
                    target_type: BucketTargetType::ReplicationService,
                    ..Default::default()
                },
                BucketTarget {
                    arn: "arn:ilm:c".to_string(),
                    target_type: BucketTargetType::IlmService,
                    ..Default::default()
                },
            ],
        };
        let config = s3s::dto::ReplicationConfiguration {
            role: String::new(),
            rules: vec![s3s::dto::ReplicationRule {
                delete_marker_replication: None,
                delete_replication: None,
                destination: s3s::dto::Destination {
                    bucket: "arn:replication:b".to_string(),
                    ..Default::default()
                },
                existing_object_replication: None,
                filter: None,
                id: None,
                prefix: Some(String::new()),
                priority: None,
                source_selection_criteria: None,
                status: s3s::dto::ReplicationRuleStatus::from_static(s3s::dto::ReplicationRuleStatus::ENABLED),
            }],
        };

        let filtered = filter_replication_check_targets(targets, &config);

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].arn, "arn:replication:b");
    }

    #[test]
    fn validate_replication_check_config_targets_rejects_stale_enabled_rule_target() {
        let targets = BucketTargets {
            targets: vec![BucketTarget {
                arn: "arn:replication:a".to_string(),
                target_type: BucketTargetType::ReplicationService,
                ..Default::default()
            }],
        };
        let config = s3s::dto::ReplicationConfiguration {
            role: String::new(),
            rules: vec![s3s::dto::ReplicationRule {
                delete_marker_replication: None,
                delete_replication: None,
                destination: s3s::dto::Destination {
                    bucket: "arn:replication:missing".to_string(),
                    ..Default::default()
                },
                existing_object_replication: None,
                filter: None,
                id: Some("rule-stale".to_string()),
                prefix: Some(String::new()),
                priority: None,
                source_selection_criteria: None,
                status: s3s::dto::ReplicationRuleStatus::from_static(s3s::dto::ReplicationRuleStatus::ENABLED),
            }],
        };

        let err = validate_replication_check_config_targets(&targets, &config).expect_err("stale target should be rejected");

        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert!(err.message().unwrap_or_default().contains("rule-stale"));
    }

    #[test]
    fn serialize_replication_metrics_body_v1_returns_replication_stats_only() {
        let mut stats = BucketStats {
            uptime: 99,
            ..Default::default()
        };
        stats.replication_stats.replica_count = 7;
        stats.proxy_stats.put_total = 3;

        let body =
            serialize_replication_metrics_body(&stats, ReplicationExtRoute::MetricsV1).expect("metrics v1 body should serialize");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("body should be json");

        assert_eq!(payload["replica_count"], 7);
        assert!(payload.get("uptime").is_none());
        assert!(payload.get("proxy_stats").is_none());
    }

    #[test]
    fn apply_replication_metrics_bandwidth_report_updates_existing_target_stats() {
        let mut stats = BucketStats::default();
        stats
            .replication_stats
            .stats
            .entry("arn:replication:a".to_string())
            .or_default()
            .replicated_count = 3;

        let bandwidth_report = HashMap::from([(
            "arn:replication:a".to_string(),
            BandwidthDetails {
                limit_bytes_per_sec: 2048,
                current_bandwidth_bytes_per_sec: 1536.5,
            },
        )]);

        let updated = apply_replication_metrics_bandwidth_report(stats, bandwidth_report);
        let stat = updated
            .replication_stats
            .stats
            .get("arn:replication:a")
            .expect("target stats should exist");

        assert_eq!(stat.replicated_count, 3);
        assert_eq!(stat.bandwidth_limit_bytes_per_sec, 2048);
        assert_eq!(stat.current_bandwidth_bytes_per_sec, 1536.5);
    }

    #[test]
    fn apply_replication_metrics_bandwidth_report_creates_missing_target_stats() {
        let bandwidth_report = HashMap::from([(
            "arn:replication:b".to_string(),
            BandwidthDetails {
                limit_bytes_per_sec: 4096,
                current_bandwidth_bytes_per_sec: 1024.25,
            },
        )]);

        let updated = apply_replication_metrics_bandwidth_report(BucketStats::default(), bandwidth_report);
        let stat = updated
            .replication_stats
            .stats
            .get("arn:replication:b")
            .expect("target stats should be created from bandwidth report");

        assert_eq!(stat.bandwidth_limit_bytes_per_sec, 4096);
        assert_eq!(stat.current_bandwidth_bytes_per_sec, 1024.25);
    }

    #[test]
    fn serialize_replication_metrics_body_v2_returns_full_bucket_stats() {
        let mut stats = BucketStats {
            uptime: 99,
            ..Default::default()
        };
        stats.replication_stats.replica_count = 7;
        stats.proxy_stats.put_total = 3;

        let body =
            serialize_replication_metrics_body(&stats, ReplicationExtRoute::MetricsV2).expect("metrics v2 body should serialize");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("body should be json");

        assert_eq!(payload["uptime"], 99);
        assert_eq!(payload["replication_stats"]["replica_count"], 7);
        assert_eq!(payload["proxy_stats"]["put_total"], 3);
    }

    #[test]
    fn apply_replication_metrics_runtime_fields_only_overrides_v2_uptime() {
        let stats = BucketStats {
            uptime: 99,
            ..Default::default()
        };

        let v1 = apply_replication_metrics_runtime_fields(stats.clone(), ReplicationExtRoute::MetricsV1, 42);
        let v2 = apply_replication_metrics_runtime_fields(stats, ReplicationExtRoute::MetricsV2, 42);

        assert_eq!(v1.uptime, 99);
        assert_eq!(v2.uptime, 42);
    }

    #[test]
    fn build_replication_probe_put_options_sets_replication_flags() {
        let now = OffsetDateTime::from_unix_timestamp(42).expect("timestamp should build");
        let options = build_replication_probe_put_options(now);

        assert_eq!(options.internal.replication_status, ReplicationStatusType::Replica);
        assert!(options.internal.replication_request);
        assert!(options.internal.replication_validity_check);
        assert_eq!(options.internal.source_mtime, now);
        assert!(!options.internal.source_version_id.is_empty());
    }

    #[test]
    fn build_replication_probe_remove_options_sets_replication_flags() {
        let now = OffsetDateTime::from_unix_timestamp(42).expect("timestamp should build");
        let options = build_replication_probe_remove_options(now, true);

        assert!(options.replication_delete_marker);
        assert_eq!(options.replication_status, ReplicationStatusType::Replica);
        assert!(options.replication_request);
        assert!(options.replication_validity_check);
        assert_eq!(options.replication_mtime, Some(now));
    }

    #[test]
    fn parse_misc_extension_request_matches_object_lambda_and_listen_notification() {
        let object_lambda: Uri = "/demo-bucket/path/to/object.txt?lambdaArn=arn%3Atarget"
            .parse()
            .expect("uri should parse");
        let listen_bucket: Uri = "/demo-bucket?events=s3:ObjectCreated:*".parse().expect("uri should parse");
        let listen_bucket_trailing_slash: Uri = "/demo-bucket/?events=s3:ObjectCreated:*".parse().expect("uri should parse");
        let listen_root: Uri = "/?events=s3:ObjectRemoved:*".parse().expect("uri should parse");

        let object_route = parse_misc_extension_request(&Method::GET, &object_lambda).expect("object lambda route should parse");
        assert_eq!(
            object_route,
            MiscExtRoute::ObjectLambda {
                bucket: "demo-bucket".to_string(),
                object: "path/to/object.txt".to_string()
            }
        );

        let listen_bucket_route =
            parse_misc_extension_request(&Method::GET, &listen_bucket).expect("bucket listen route should parse");
        assert_eq!(
            listen_bucket_route,
            MiscExtRoute::ListenNotification {
                bucket: Some("demo-bucket".to_string())
            }
        );

        let listen_bucket_trailing_slash_route = parse_misc_extension_request(&Method::GET, &listen_bucket_trailing_slash)
            .expect("bucket listen route with trailing slash should parse");
        assert_eq!(
            listen_bucket_trailing_slash_route,
            MiscExtRoute::ListenNotification {
                bucket: Some("demo-bucket".to_string())
            }
        );

        let listen_root_route = parse_misc_extension_request(&Method::GET, &listen_root).expect("root listen route should parse");
        assert_eq!(listen_root_route, MiscExtRoute::ListenNotification { bucket: None });
    }

    #[test]
    fn parse_misc_extension_request_rejects_invalid_paths_or_methods() {
        let bucket_without_object: Uri = "/demo-bucket?lambdaArn=arn%3Atarget".parse().expect("uri should parse");
        let wrong_method_lambda: Uri = "/demo-bucket/object?lambdaArn=arn%3Atarget"
            .parse()
            .expect("uri should parse");
        let object_level_listen: Uri = "/demo-bucket/object?events=s3:ObjectCreated:*"
            .parse()
            .expect("uri should parse");

        assert!(parse_misc_extension_request(&Method::GET, &bucket_without_object).is_none());
        assert!(parse_misc_extension_request(&Method::PUT, &wrong_method_lambda).is_none());
        assert!(parse_misc_extension_request(&Method::GET, &object_level_listen).is_none());
    }

    #[test]
    fn validate_listen_notification_query_accepts_valid_values() {
        let uri: Uri = "/demo-bucket?events=s3:ObjectCreated:*&prefix=logs/&suffix=.json&ping=3"
            .parse()
            .expect("uri should parse");

        assert!(validate_listen_notification_query(&uri).is_ok());
    }

    #[test]
    fn validate_listen_notification_query_rejects_invalid_event_or_duplicate_filters() {
        let invalid_event: Uri = "/demo-bucket?events=invalid-event".parse().expect("uri should parse");
        let duplicate_prefix: Uri = "/demo-bucket?events=s3:ObjectCreated:*&prefix=a&prefix=b"
            .parse()
            .expect("uri should parse");
        let invalid_ping: Uri = "/demo-bucket?events=s3:ObjectCreated:*&ping=0"
            .parse()
            .expect("uri should parse");

        assert_eq!(
            validate_listen_notification_query(&invalid_event)
                .expect_err("invalid event should fail")
                .code(),
            &S3ErrorCode::InvalidArgument
        );
        assert_eq!(
            validate_listen_notification_query(&duplicate_prefix)
                .expect_err("duplicate prefix should fail")
                .code(),
            &S3ErrorCode::InvalidArgument
        );
        assert_eq!(
            validate_listen_notification_query(&invalid_ping)
                .expect_err("invalid ping should fail")
                .code(),
            &S3ErrorCode::InvalidArgument
        );
    }

    #[test]
    fn validate_object_lambda_query_rejects_missing_empty_or_invalid_arn() {
        let missing: Uri = "/demo-bucket/object.txt".parse().expect("uri should parse");
        let empty: Uri = "/demo-bucket/object.txt?lambdaArn=".parse().expect("uri should parse");
        let duplicated: Uri = "/demo-bucket/object.txt?lambdaArn=a&lambdaArn=b"
            .parse()
            .expect("uri should parse");
        let invalid_format: Uri = "/demo-bucket/object.txt?lambdaArn=not-an-arn"
            .parse()
            .expect("uri should parse");

        assert_eq!(
            validate_object_lambda_query(&missing)
                .expect_err("missing lambdaArn should fail")
                .code(),
            &S3ErrorCode::InvalidRequest
        );
        assert_eq!(
            validate_object_lambda_query(&empty)
                .expect_err("empty lambdaArn should fail")
                .code(),
            &S3ErrorCode::InvalidRequest
        );
        assert_eq!(
            validate_object_lambda_query(&duplicated)
                .expect_err("duplicated lambdaArn should fail")
                .code(),
            &S3ErrorCode::InvalidRequest
        );
        assert_eq!(
            validate_object_lambda_query(&invalid_format)
                .expect_err("invalid lambdaArn should fail")
                .code(),
            &S3ErrorCode::InvalidRequest
        );
    }

    #[test]
    fn validate_object_lambda_query_accepts_arn() {
        let valid: Uri = "/demo-bucket/object.txt?lambdaArn=arn%3Aacme%3As3-object-lambda%3A%3Atransformer%3Awebhook"
            .parse()
            .expect("uri should parse");

        assert!(validate_object_lambda_query(&valid).is_ok());
    }

    #[test]
    fn resolve_object_lambda_webhook_config_from_server_config_accepts_enabled_webhook_target() {
        let arn = "arn:acme:s3-object-lambda::transformer:webhook"
            .parse::<rustfs_targets::arn::ARN>()
            .expect("arn should parse");
        let config = rustfs_ecstore::config::Config(std::collections::HashMap::from([(
            LAMBDA_WEBHOOK_SUB_SYS.to_string(),
            std::collections::HashMap::from([(
                "transformer".to_string(),
                rustfs_ecstore::config::KVS(vec![
                    rustfs_ecstore::config::KV {
                        key: ENABLE_KEY.to_string(),
                        value: "on".to_string(),
                        hidden_if_empty: false,
                    },
                    rustfs_ecstore::config::KV {
                        key: WEBHOOK_ENDPOINT.to_string(),
                        value: "https://example.com/transform".to_string(),
                        hidden_if_empty: false,
                    },
                    rustfs_ecstore::config::KV {
                        key: WEBHOOK_AUTH_TOKEN.to_string(),
                        value: "secret-token".to_string(),
                        hidden_if_empty: true,
                    },
                ]),
            )]),
        )]));

        let resolved = resolve_object_lambda_webhook_config_from_server_config(&config, &arn).expect("config should resolve");

        assert_eq!(resolved.endpoint.as_str(), "https://example.com/transform");
        assert_eq!(resolved.auth_token, "secret-token");
        assert!(!resolved.skip_tls_verify);
        assert!(resolved.response_header_timeout.is_none());
    }

    #[test]
    fn resolve_object_lambda_webhook_config_from_server_config_accepts_named_webhook_target() {
        let arn = "arn:acme:s3-object-lambda::transformer:webhook-csv"
            .parse::<rustfs_targets::arn::ARN>()
            .expect("arn should parse");
        let config = rustfs_ecstore::config::Config(std::collections::HashMap::from([(
            LAMBDA_WEBHOOK_SUB_SYS.to_string(),
            std::collections::HashMap::from([(
                "transformer".to_string(),
                rustfs_ecstore::config::KVS(vec![
                    rustfs_ecstore::config::KV {
                        key: ENABLE_KEY.to_string(),
                        value: "on".to_string(),
                        hidden_if_empty: false,
                    },
                    rustfs_ecstore::config::KV {
                        key: WEBHOOK_ENDPOINT.to_string(),
                        value: "https://example.com/transform-csv".to_string(),
                        hidden_if_empty: false,
                    },
                ]),
            )]),
        )]));

        let resolved = resolve_object_lambda_webhook_config_from_server_config(&config, &arn).expect("config should resolve");
        assert_eq!(resolved.endpoint.as_str(), "https://example.com/transform-csv");
    }

    #[test]
    fn resolve_object_lambda_webhook_config_from_server_config_parses_response_header_timeout() {
        let arn = "arn:acme:s3-object-lambda::transformer:webhook"
            .parse::<rustfs_targets::arn::ARN>()
            .expect("arn should parse");
        let config = rustfs_ecstore::config::Config(std::collections::HashMap::from([(
            LAMBDA_WEBHOOK_SUB_SYS.to_string(),
            std::collections::HashMap::from([(
                "transformer".to_string(),
                rustfs_ecstore::config::KVS(vec![
                    rustfs_ecstore::config::KV {
                        key: ENABLE_KEY.to_string(),
                        value: "on".to_string(),
                        hidden_if_empty: false,
                    },
                    rustfs_ecstore::config::KV {
                        key: WEBHOOK_ENDPOINT.to_string(),
                        value: "https://example.com/transform".to_string(),
                        hidden_if_empty: false,
                    },
                    rustfs_ecstore::config::KV {
                        key: WEBHOOK_RESPONSE_HEADER_TIMEOUT.to_string(),
                        value: "2s".to_string(),
                        hidden_if_empty: false,
                    },
                ]),
            )]),
        )]));

        let resolved = resolve_object_lambda_webhook_config_from_server_config(&config, &arn).expect("config should resolve");
        assert_eq!(resolved.response_header_timeout, Some(Duration::from_secs(2)));
    }

    #[test]
    fn resolve_object_lambda_webhook_config_from_server_config_accepts_notify_webhook_fallback() {
        let arn = "arn:acme:s3-object-lambda::transformer:webhook"
            .parse::<rustfs_targets::arn::ARN>()
            .expect("arn should parse");
        let config = rustfs_ecstore::config::Config(std::collections::HashMap::from([(
            NOTIFY_WEBHOOK_SUB_SYS.to_string(),
            std::collections::HashMap::from([(
                "transformer".to_string(),
                rustfs_ecstore::config::KVS(vec![
                    rustfs_ecstore::config::KV {
                        key: ENABLE_KEY.to_string(),
                        value: "on".to_string(),
                        hidden_if_empty: false,
                    },
                    rustfs_ecstore::config::KV {
                        key: WEBHOOK_ENDPOINT.to_string(),
                        value: "https://example.com/notify-transform".to_string(),
                        hidden_if_empty: false,
                    },
                ]),
            )]),
        )]));

        let resolved = resolve_object_lambda_webhook_config_from_server_config(&config, &arn).expect("config should resolve");
        assert_eq!(resolved.endpoint.as_str(), "https://example.com/notify-transform");
    }

    #[test]
    fn resolve_object_lambda_webhook_config_from_server_config_rejects_invalid_response_header_timeout() {
        let arn = "arn:acme:s3-object-lambda::transformer:webhook"
            .parse::<rustfs_targets::arn::ARN>()
            .expect("arn should parse");
        let config = rustfs_ecstore::config::Config(std::collections::HashMap::from([(
            LAMBDA_WEBHOOK_SUB_SYS.to_string(),
            std::collections::HashMap::from([(
                "transformer".to_string(),
                rustfs_ecstore::config::KVS(vec![
                    rustfs_ecstore::config::KV {
                        key: ENABLE_KEY.to_string(),
                        value: "on".to_string(),
                        hidden_if_empty: false,
                    },
                    rustfs_ecstore::config::KV {
                        key: WEBHOOK_ENDPOINT.to_string(),
                        value: "https://example.com/transform".to_string(),
                        hidden_if_empty: false,
                    },
                    rustfs_ecstore::config::KV {
                        key: WEBHOOK_RESPONSE_HEADER_TIMEOUT.to_string(),
                        value: "definitely-not-a-duration".to_string(),
                        hidden_if_empty: false,
                    },
                ]),
            )]),
        )]));

        let err =
            resolve_object_lambda_webhook_config_from_server_config(&config, &arn).expect_err("invalid timeout should fail");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn resolve_object_lambda_webhook_config_from_server_config_rejects_unsupported_or_disabled_targets() {
        let unsupported = "arn:acme:s3-object-lambda::transformer:mqtt"
            .parse::<rustfs_targets::arn::ARN>()
            .expect("arn should parse");
        let empty_config = rustfs_ecstore::config::Config(std::collections::HashMap::new());
        let unsupported_err = resolve_object_lambda_webhook_config_from_server_config(&empty_config, &unsupported)
            .expect_err("unsupported target type should fail");
        assert_eq!(unsupported_err.code(), &S3ErrorCode::NotImplemented);

        let webhook = "arn:acme:s3-object-lambda::transformer:webhook"
            .parse::<rustfs_targets::arn::ARN>()
            .expect("arn should parse");
        let disabled_config = rustfs_ecstore::config::Config(std::collections::HashMap::from([(
            LAMBDA_WEBHOOK_SUB_SYS.to_string(),
            std::collections::HashMap::from([(
                "transformer".to_string(),
                rustfs_ecstore::config::KVS(vec![
                    rustfs_ecstore::config::KV {
                        key: ENABLE_KEY.to_string(),
                        value: "off".to_string(),
                        hidden_if_empty: false,
                    },
                    rustfs_ecstore::config::KV {
                        key: WEBHOOK_ENDPOINT.to_string(),
                        value: "https://example.com/transform".to_string(),
                        hidden_if_empty: false,
                    },
                ]),
            )]),
        )]));

        let disabled_err = resolve_object_lambda_webhook_config_from_server_config(&disabled_config, &webhook)
            .expect_err("disabled target should fail");
        assert_eq!(disabled_err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn clear_object_lambda_variant_headers_removes_original_object_payload_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::CONTENT_LENGTH, HeaderValue::from_static("7"));
        headers.insert(http::header::CONTENT_TYPE, HeaderValue::from_static("text/plain"));
        headers.insert("x-amz-meta-demo", HeaderValue::from_static("value"));
        headers.insert("x-amz-version-id", HeaderValue::from_static("v1"));

        clear_object_lambda_variant_headers(&mut headers);

        assert!(headers.get(http::header::CONTENT_LENGTH).is_none());
        assert!(headers.get(http::header::CONTENT_TYPE).is_none());
        assert!(headers.get("x-amz-meta-demo").is_none());
        assert_eq!(headers.get("x-amz-version-id").and_then(|value| value.to_str().ok()), Some("v1"));
    }

    #[test]
    fn build_object_lambda_passthrough_response_preserves_target_status_and_filters_headers() {
        let mut upstream_headers = HeaderMap::new();
        upstream_headers.insert(http::header::CONTENT_LENGTH, HeaderValue::from_static("7"));
        upstream_headers.insert(http::header::CONTENT_TYPE, HeaderValue::from_static("text/plain"));
        upstream_headers.insert("x-amz-meta-demo", HeaderValue::from_static("value"));
        upstream_headers.insert("x-amz-version-id", HeaderValue::from_static("v1"));

        let mut lambda_headers = HeaderMap::new();
        lambda_headers.insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
        lambda_headers.insert("x-rustfs-lambda-error", HeaderValue::from_static("upstream"));
        lambda_headers.insert(http::header::CONNECTION, HeaderValue::from_static("keep-alive"));
        lambda_headers.insert("x-amz-request-route", HeaderValue::from_static("route-token"));
        lambda_headers.insert("x-amz-request-token", HeaderValue::from_static("request-token"));

        let response = build_object_lambda_passthrough_response(
            upstream_headers,
            &lambda_headers,
            StatusCode::BAD_GATEWAY,
            Body::from("lambda failed".to_string()),
        );

        assert_eq!(response.status, Some(StatusCode::BAD_GATEWAY));
        assert_eq!(
            response
                .headers
                .get(http::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some("application/json")
        );
        assert_eq!(
            response
                .headers
                .get("x-rustfs-lambda-error")
                .and_then(|value| value.to_str().ok()),
            Some("upstream")
        );
        assert!(response.headers.get(http::header::CONTENT_LENGTH).is_none());
        assert!(response.headers.get("x-amz-meta-demo").is_none());
        assert!(response.headers.get(http::header::CONNECTION).is_none());
        assert!(response.headers.get("x-amz-request-route").is_none());
        assert!(response.headers.get("x-amz-request-token").is_none());
        assert_eq!(response.headers.get("x-amz-version-id").and_then(|value| value.to_str().ok()), Some("v1"));
    }

    #[test]
    fn build_object_lambda_source_url_presigns_request_without_lambda_arn() {
        let req = S3Request {
            input: Body::from(String::new()),
            method: Method::GET,
            uri: "/demo-bucket/object.txt?lambdaArn=arn%3Aacme%3As3-object-lambda%3A%3Atransformer%3Awebhook&versionId=v1"
                .parse()
                .expect("uri should parse"),
            headers: HeaderMap::from_iter([(http::header::HOST, HeaderValue::from_static("localhost:9000"))]),
            extensions: http::Extensions::new(),
            credentials: Some(s3s::auth::Credentials {
                access_key: "rustfsadmin".to_string(),
                secret_key: s3s::auth::SecretKey::from("rustfssecret"),
            }),
            region: get_global_region(),
            service: None,
            trailing_headers: None,
        };

        let source_url = build_object_lambda_source_url(&req).expect("source url should build");
        let source_url = Url::parse(&source_url).expect("source url should parse");
        let query_pairs = source_url.query_pairs().collect::<std::collections::BTreeMap<_, _>>();

        assert_eq!(source_url.scheme(), "http");
        assert_eq!(source_url.host_str(), Some("localhost"));
        assert_eq!(source_url.port_or_known_default(), Some(9000));
        assert_eq!(source_url.path(), "/demo-bucket/object.txt");
        assert_eq!(query_pairs.get("versionId").map(|value| value.as_ref()), Some("v1"));
        assert!(!query_pairs.contains_key("lambdaArn"));
        let expires = query_pairs.get("X-Amz-Expires").and_then(|value| value.parse::<u64>().ok());
        assert_eq!(expires, Some(3600));
        assert_eq!(query_pairs.get("X-Amz-Algorithm").map(|value| value.as_ref()), Some("AWS4-HMAC-SHA256"));
        assert!(query_pairs.contains_key("X-Amz-Signature"));
    }

    #[test]
    fn build_object_lambda_event_payload_contains_required_context() {
        let req = S3Request {
            input: Body::from(String::new()),
            method: Method::GET,
            uri: "/demo-bucket/object.txt?lambdaArn=arn%3Aacme%3As3-object-lambda%3A%3Atransformer%3Awebhook"
                .parse()
                .expect("uri should parse"),
            headers: HeaderMap::from_iter([(http::header::HOST, HeaderValue::from_static("localhost:9000"))]),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };

        let payload = build_object_lambda_event_payload(
            &req,
            "arn:acme:s3-object-lambda::transformer:webhook",
            "https://example.com/source",
            "route-123",
            "token-456",
        )
        .expect("payload should serialize");
        let payload: serde_json::Value = serde_json::from_slice(&payload).expect("payload should be json");

        assert_eq!(payload["getObjectContext"]["inputS3Url"], "https://example.com/source");
        assert_eq!(payload["getObjectContext"]["outputRoute"], "route-123");
        assert_eq!(payload["getObjectContext"]["outputToken"], "token-456");
        assert_eq!(
            payload["configuration"]["accessPointArn"],
            "arn:acme:s3-object-lambda::transformer:webhook"
        );
        assert_eq!(
            payload["userRequest"]["url"],
            "/demo-bucket/object.txt?lambdaArn=arn%3Aacme%3As3-object-lambda%3A%3Atransformer%3Awebhook"
        );
    }

    #[test]
    fn validate_object_lambda_response_auth_headers_rejects_missing_or_mismatched_values() {
        let mut matching = HeaderMap::new();
        matching.insert("x-amz-request-route", HeaderValue::from_static("route-123"));
        matching.insert("x-amz-request-token", HeaderValue::from_static("token-456"));
        assert!(validate_object_lambda_response_auth_headers(&matching, "route-123", "token-456").is_ok());

        let missing = HeaderMap::new();
        let err = validate_object_lambda_response_auth_headers(&missing, "route-123", "token-456")
            .expect_err("missing auth headers should fail");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);

        let mut mismatched = HeaderMap::new();
        mismatched.insert("x-amz-request-route", HeaderValue::from_static("route-123"));
        mismatched.insert("x-amz-request-token", HeaderValue::from_static("wrong-token"));
        let err = validate_object_lambda_response_auth_headers(&mismatched, "route-123", "token-456")
            .expect_err("mismatched auth headers should fail");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn build_object_lambda_get_request_removes_lambda_arn_and_preserves_request_inputs() {
        let mut req = S3Request {
            input: Body::from(String::new()),
            method: Method::GET,
            uri: "/demo-bucket/object.txt?lambdaArn=arn%3Aacme%3As3-object-lambda%3A%3Atransformer%3Awebhook&versionId=v1&partNumber=7"
                .parse()
                .expect("uri should parse"),
            headers: HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };
        req.headers
            .insert(http::header::RANGE, HeaderValue::from_static("bytes=5-10"));
        req.headers
            .insert(http::header::IF_MATCH, HeaderValue::from_static("\"abc\""));

        let bridged = build_object_lambda_get_request(&req, "demo-bucket", "object.txt").expect("bridge request should build");

        assert_eq!(bridged.uri.path(), "/demo-bucket/object.txt");
        assert_eq!(bridged.uri.query(), Some("versionId=v1&partNumber=7"));
        assert_eq!(bridged.input.bucket, "demo-bucket");
        assert_eq!(bridged.input.key, "object.txt");
        assert_eq!(bridged.input.version_id.as_deref(), Some("v1"));
        assert_eq!(bridged.input.part_number, Some(7));
        assert_eq!(
            bridged.input.range,
            Some(Range::Int {
                first: 5,
                last: Some(10)
            })
        );
        assert!(bridged.input.if_match.is_some());
    }

    #[test]
    fn build_object_lambda_get_request_rejects_invalid_range_header() {
        let mut req = S3Request {
            input: Body::from(String::new()),
            method: Method::GET,
            uri: "/demo-bucket/object.txt?lambdaArn=arn%3Aacme%3As3-object-lambda%3A%3Atransformer%3Awebhook"
                .parse()
                .expect("uri should parse"),
            headers: HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };
        req.headers
            .insert(http::header::RANGE, HeaderValue::from_static("bytes=10-5"));

        let err = build_object_lambda_get_request(&req, "demo-bucket", "object.txt").expect_err("invalid range must fail");
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn convert_get_object_response_maps_core_headers() {
        let mut resp = S3Response::new(GetObjectOutput {
            body: Some(Body::from("payload".to_string()).into()),
            content_length: Some(7),
            content_type: Some("text/plain".to_string()),
            accept_ranges: Some("bytes".to_string()),
            version_id: Some("v1".to_string()),
            metadata: Some(std::collections::HashMap::from([("custom-key".to_string(), "custom-value".to_string())])),
            ..Default::default()
        });
        resp.status = Some(StatusCode::OK);

        let converted = convert_get_object_response(resp).expect("response conversion should succeed");

        assert_eq!(converted.status, Some(StatusCode::OK));
        assert_eq!(
            converted
                .headers
                .get(http::header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok()),
            Some("7")
        );
        assert_eq!(
            converted
                .headers
                .get(http::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some("text/plain")
        );
        assert_eq!(
            converted
                .headers
                .get(http::header::ACCEPT_RANGES)
                .and_then(|value| value.to_str().ok()),
            Some("bytes")
        );
        assert_eq!(
            converted
                .headers
                .get("x-amz-version-id")
                .and_then(|value| value.to_str().ok()),
            Some("v1")
        );
        assert_eq!(
            converted
                .headers
                .get("x-amz-meta-custom-key")
                .and_then(|value| value.to_str().ok()),
            Some("custom-value")
        );
    }

    #[tokio::test]
    async fn check_access_rejects_anonymous_replication_extension_request() {
        let router: S3Router<AdminOperation> = S3Router::new(false);
        let mut req = S3Request {
            input: Body::from(String::new()),
            method: Method::GET,
            uri: "/demo-bucket?replication-metrics".parse().expect("uri should parse"),
            headers: HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };

        let err = router
            .check_access(&mut req)
            .await
            .expect_err("anonymous extension request must be denied");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }

    #[tokio::test]
    async fn check_access_rejects_anonymous_misc_extension_request() {
        let router: S3Router<AdminOperation> = S3Router::new(false);
        let mut req = S3Request {
            input: Body::from(String::new()),
            method: Method::GET,
            uri: "/demo-bucket/path/object.txt?lambdaArn=arn%3Atarget"
                .parse()
                .expect("uri should parse"),
            headers: HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };

        let err = router
            .check_access(&mut req)
            .await
            .expect_err("anonymous extension request must be denied");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }

    #[test]
    fn listen_notification_keepalive_plan_defaults_to_space_keepalive() {
        let uri: Uri = "/demo-bucket?events=s3:ObjectCreated:Put".parse().expect("uri should parse");
        let (interval, payload) = listen_notification_keepalive_plan(&uri);
        assert_eq!(interval, Duration::from_millis(500));
        assert_eq!(payload, Bytes::from_static(b" "));
    }

    #[test]
    fn listen_notification_keepalive_plan_uses_empty_record_payload_when_ping_is_present() {
        let uri: Uri = "/demo-bucket?events=s3:ObjectCreated:Put&ping=3"
            .parse()
            .expect("uri should parse");
        let (interval, payload) = listen_notification_keepalive_plan(&uri);
        assert_eq!(interval, Duration::from_secs(3));
        assert_eq!(payload, Bytes::from_static(b"{\"Records\":[]}\n"));
    }

    #[test]
    fn parse_listen_notification_filter_expands_event_mask_and_filters() {
        let uri: Uri = "/demo-bucket?events=s3:ObjectCreated:*&events=s3:ObjectRemoved:Delete&prefix=logs/&suffix=.json"
            .parse()
            .expect("uri should parse");

        let filter = parse_listen_notification_filter(&uri, Some("demo-bucket")).expect("filter should parse");

        assert_eq!(filter.bucket.as_deref(), Some("demo-bucket"));
        assert_eq!(filter.prefix.as_deref(), Some("logs/"));
        assert_eq!(filter.suffix.as_deref(), Some(".json"));
        assert_ne!(filter.event_mask & EventName::ObjectCreatedPut.mask(), 0);
        assert_ne!(filter.event_mask & EventName::ObjectRemovedDelete.mask(), 0);
        assert_eq!(filter.event_mask & EventName::ObjectAccessedGet.mask(), 0);
    }

    #[test]
    fn event_matches_listen_notification_respects_bucket_event_and_object_filters() {
        let filter = ListenNotificationFilter {
            bucket: Some("demo-bucket".to_string()),
            event_mask: EventName::ObjectCreatedPut.mask() | EventName::ObjectCreatedPost.mask(),
            prefix: Some("logs/".to_string()),
            suffix: Some(".json".to_string()),
        };

        let matched = NotificationEvent::new_test_event("demo-bucket", "logs/app.json", EventName::ObjectCreatedPut);
        assert!(event_matches_listen_notification(&matched, &filter));

        let wrong_bucket = NotificationEvent::new_test_event("other-bucket", "logs/app.json", EventName::ObjectCreatedPut);
        assert!(!event_matches_listen_notification(&wrong_bucket, &filter));

        let wrong_event = NotificationEvent::new_test_event("demo-bucket", "logs/app.json", EventName::ObjectRemovedDelete);
        assert!(!event_matches_listen_notification(&wrong_event, &filter));

        let wrong_prefix = NotificationEvent::new_test_event("demo-bucket", "archive/app.json", EventName::ObjectCreatedPut);
        assert!(!event_matches_listen_notification(&wrong_prefix, &filter));

        let wrong_suffix = NotificationEvent::new_test_event("demo-bucket", "logs/app.txt", EventName::ObjectCreatedPut);
        assert!(!event_matches_listen_notification(&wrong_suffix, &filter));
    }

    #[test]
    fn event_matches_listen_notification_decodes_object_key_before_filtering() {
        let filter = ListenNotificationFilter {
            bucket: Some("demo-bucket".to_string()),
            event_mask: EventName::ObjectCreatedPut.mask(),
            prefix: Some("logs/".to_string()),
            suffix: Some(".json".to_string()),
        };

        let encoded = NotificationEvent::new_test_event("demo-bucket", "logs%2Fapp.json", EventName::ObjectCreatedPut);
        assert!(event_matches_listen_notification(&encoded, &filter));
    }

    #[test]
    fn serialize_listen_notification_event_wraps_records_payload() {
        let event = NotificationEvent::new_test_event("demo-bucket", "logs/app.json", EventName::ObjectCreatedPut);

        let payload = serialize_listen_notification_event(&event).expect("payload should serialize");
        let body = std::str::from_utf8(payload.as_ref()).expect("payload should be utf-8");

        assert!(body.contains("\"Records\":["));
        assert!(body.contains("\"name\":\"demo-bucket\""));
        assert!(body.contains("\"eventName\":\"ObjectCreatedPut\"") || body.contains("s3:ObjectCreated:Put"));
        assert!(body.ends_with('\n'));
    }

    #[tokio::test]
    async fn build_listen_notification_response_sets_event_stream_headers() {
        let uri: Uri = "/demo-bucket?events=s3:ObjectCreated:Put&ping=1"
            .parse()
            .expect("uri should parse");

        let resp = build_listen_notification_response(&uri, Some("demo-bucket")).expect("response should build");

        assert_eq!(
            resp.headers.get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()),
            Some("text/event-stream")
        );
        assert_eq!(resp.headers.get(header::CACHE_CONTROL).and_then(|v| v.to_str().ok()), Some("no-cache"));
        assert_eq!(resp.headers.get("x-accel-buffering").and_then(|v| v.to_str().ok()), Some("no"));
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Extra {
    pub credentials: Option<s3s::auth::Credentials>,
    pub region: Option<s3s::region::Region>,
    pub service: Option<String>,
}

fn convert_request(req: S3Request<Body>) -> http::Request<Body> {
    let (mut parts, _) = http::Request::new(Body::empty()).into_parts();
    parts.method = req.method;
    parts.uri = req.uri;
    parts.headers = req.headers;
    parts.extensions = req.extensions;
    parts.extensions.insert(Extra {
        credentials: req.credentials,
        region: req.region,
        service: req.service,
    });
    http::Request::from_parts(parts, req.input)
}

fn convert_response(resp: http::Response<axum::body::Body>) -> S3Response<Body> {
    let (parts, body) = resp.into_parts();
    let mut s3_resp = S3Response::new(Body::http_body_unsync(body));
    s3_resp.status = Some(parts.status);
    s3_resp.headers = parts.headers;
    s3_resp.extensions = parts.extensions;
    s3_resp
}
