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
use crate::server::{
    ADMIN_PREFIX, HEALTH_PREFIX, HEALTH_READY_PATH, MINIO_ADMIN_PREFIX, PROFILE_CPU_PATH, PROFILE_MEMORY_PATH, RPC_PREFIX,
};
use crate::storage::access::{ReqInfo, authorize_request};
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
use rustfs_ecstore::bucket::metadata_sys;
use rustfs_ecstore::bucket::replication::BucketStats;
use rustfs_ecstore::bucket::replication::GLOBAL_REPLICATION_STATS;
use rustfs_ecstore::bucket::versioning::VersioningApi;
use rustfs_ecstore::bucket::versioning_sys::BucketVersioningSys;
use rustfs_ecstore::rpc::verify_rpc_signature;
use rustfs_ecstore::store_api::{BucketOperations, BucketOptions};
use rustfs_ecstore::{global::get_global_region, new_object_layer_fn};
use rustfs_madmin::utils::parse_duration;
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_s3_common::EventName;
use s3s::Body;
use s3s::S3Error;
use s3s::S3ErrorCode;
use s3s::S3Request;
use s3s::S3Response;
use s3s::S3Result;
use s3s::StdError;
use s3s::dto::{GetObjectInput, GetObjectOutput, IfMatch, IfNoneMatch, Range, Timestamp, TimestampFormat};
use s3s::header;
use s3s::route::S3Route;
use s3s::s3_error;
use s3s::stream::{ByteStream, DynByteStream};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use tower::Service;
use tracing::error;
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
        if let Some(bucket) = extract_bucket_for_bucket_level_path(uri.path()) {
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

fn convert_get_object_response(resp: S3Response<GetObjectOutput>) -> S3Result<S3Response<Body>> {
    let mut headers = resp.headers.clone();

    if let Some(accept_ranges) = &resp.output.accept_ranges {
        insert_string_header(&mut headers, http::header::ACCEPT_RANGES, accept_ranges.clone())?;
    }
    if let Some(cache_control) = &resp.output.cache_control {
        insert_string_header(&mut headers, http::header::CACHE_CONTROL, cache_control.clone())?;
    }
    if let Some(content_disposition) = &resp.output.content_disposition {
        insert_string_header(&mut headers, http::header::CONTENT_DISPOSITION, content_disposition.clone())?;
    }
    if let Some(content_encoding) = &resp.output.content_encoding {
        insert_string_header(&mut headers, http::header::CONTENT_ENCODING, content_encoding.clone())?;
    }
    if let Some(content_language) = &resp.output.content_language {
        insert_string_header(&mut headers, http::header::CONTENT_LANGUAGE, content_language.clone())?;
    }
    if let Some(content_length) = resp.output.content_length {
        insert_string_header(&mut headers, http::header::CONTENT_LENGTH, content_length.to_string())?;
    }
    if let Some(content_range) = &resp.output.content_range {
        insert_string_header(&mut headers, http::header::CONTENT_RANGE, content_range.clone())?;
    }
    if let Some(content_type) = &resp.output.content_type {
        insert_string_header(&mut headers, http::header::CONTENT_TYPE, content_type.to_string())?;
    }
    if let Some(etag) = &resp.output.e_tag {
        headers.insert(
            http::header::ETAG,
            etag.to_http_header().map_err(|_| s3_error!(InternalError, "invalid etag"))?,
        );
    }
    if let Some(last_modified) = &resp.output.last_modified {
        insert_string_header(&mut headers, http::header::LAST_MODIFIED, format_timestamp_http_date(last_modified)?)?;
    }
    if let Some(expires) = &resp.output.expires {
        insert_string_header(&mut headers, http::header::EXPIRES, format_timestamp_http_date(expires)?)?;
    }
    if let Some(version_id) = &resp.output.version_id {
        insert_string_header(&mut headers, HeaderName::from_static("x-amz-version-id"), version_id.clone())?;
    }
    if let Some(server_side_encryption) = &resp.output.server_side_encryption {
        insert_string_header(
            &mut headers,
            HeaderName::from_static("x-amz-server-side-encryption"),
            server_side_encryption.as_str().to_string(),
        )?;
    }
    if let Some(sse_customer_algorithm) = &resp.output.sse_customer_algorithm {
        insert_string_header(
            &mut headers,
            HeaderName::from_static("x-amz-server-side-encryption-customer-algorithm"),
            sse_customer_algorithm.clone(),
        )?;
    }
    if let Some(sse_customer_key_md5) = &resp.output.sse_customer_key_md5 {
        insert_string_header(
            &mut headers,
            HeaderName::from_static("x-amz-server-side-encryption-customer-key-md5"),
            sse_customer_key_md5.clone(),
        )?;
    }
    if let Some(sse_kms_key_id) = &resp.output.ssekms_key_id {
        insert_string_header(
            &mut headers,
            HeaderName::from_static("x-amz-server-side-encryption-aws-kms-key-id"),
            sse_kms_key_id.clone(),
        )?;
    }
    if let Some(checksum_crc32) = &resp.output.checksum_crc32 {
        insert_string_header(&mut headers, HeaderName::from_static("x-amz-checksum-crc32"), checksum_crc32.clone())?;
    }
    if let Some(checksum_crc32c) = &resp.output.checksum_crc32c {
        insert_string_header(&mut headers, HeaderName::from_static("x-amz-checksum-crc32c"), checksum_crc32c.clone())?;
    }
    if let Some(checksum_crc64nvme) = &resp.output.checksum_crc64nvme {
        insert_string_header(
            &mut headers,
            HeaderName::from_static("x-amz-checksum-crc64nvme"),
            checksum_crc64nvme.clone(),
        )?;
    }
    if let Some(checksum_sha1) = &resp.output.checksum_sha1 {
        insert_string_header(&mut headers, HeaderName::from_static("x-amz-checksum-sha1"), checksum_sha1.clone())?;
    }
    if let Some(checksum_sha256) = &resp.output.checksum_sha256 {
        insert_string_header(&mut headers, HeaderName::from_static("x-amz-checksum-sha256"), checksum_sha256.clone())?;
    }
    if let Some(checksum_type) = &resp.output.checksum_type {
        insert_string_header(
            &mut headers,
            HeaderName::from_static("x-amz-checksum-type"),
            checksum_type.as_str().to_string(),
        )?;
    }
    if let Some(storage_class) = &resp.output.storage_class {
        insert_string_header(
            &mut headers,
            HeaderName::from_static("x-amz-storage-class"),
            storage_class.as_str().to_string(),
        )?;
    }
    if let Some(tag_count) = resp.output.tag_count {
        insert_string_header(&mut headers, HeaderName::from_static("x-amz-tagging-count"), tag_count.to_string())?;
    }
    if let Some(expiration) = &resp.output.expiration {
        insert_string_header(&mut headers, HeaderName::from_static("x-amz-expiration"), expiration.clone())?;
    }
    if let Some(restore) = &resp.output.restore {
        insert_string_header(&mut headers, HeaderName::from_static("x-amz-restore"), restore.clone())?;
    }

    if let Some(metadata) = &resp.output.metadata {
        for (key, value) in metadata {
            let header_name = format!("x-amz-meta-{key}");
            if let Ok(parsed_name) = HeaderName::from_bytes(header_name.as_bytes()) {
                let parsed_value = HeaderValue::from_str(value)
                    .map_err(|_| s3_error!(InternalError, "failed to build metadata response header"))?;
                headers.insert(parsed_name, parsed_value);
            }
        }
    }

    let body = resp.output.body.map(Body::from).unwrap_or_else(|| Body::from(String::new()));

    Ok(S3Response {
        output: body,
        status: resp.status,
        headers,
        extensions: resp.extensions,
    })
}

struct ListenNotificationStream {
    inner: ReceiverStream<Result<Bytes, StdError>>,
}

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

fn build_listen_notification_response(uri: &Uri) -> S3Response<Body> {
    let (interval_duration, payload) = listen_notification_keepalive_plan(uri);

    let (tx, rx) = mpsc::channel(16);
    let stream: DynByteStream = Box::pin(ListenNotificationStream {
        inner: ReceiverStream::new(rx),
    });

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval_duration);
        // Skip the immediate first tick so behavior starts after interval duration.
        ticker.tick().await;
        loop {
            tokio::select! {
                _ = tx.closed() => break,
                _ = ticker.tick() => {
                    if tx.send(Ok(payload.clone())).await.is_err() {
                        break;
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
    resp
}

async fn ensure_replication_bucket_ready(bucket: &str) -> S3Result<()> {
    let Some(store) = new_object_layer_fn() else {
        return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init"));
    };

    store
        .get_bucket_info(bucket, &BucketOptions::default())
        .await
        .map_err(ApiError::from)?;

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

    let body = match route {
        ReplicationExtRoute::MetricsV1 => {
            serde_json::to_vec(&bucket_stats.replication_stats).map_err(|e| s3_error!(InternalError, "{e}"))?
        }
        ReplicationExtRoute::MetricsV2 => serde_json::to_vec(&bucket_stats).map_err(|e| s3_error!(InternalError, "{e}"))?,
        ReplicationExtRoute::Check | ReplicationExtRoute::ResetStart | ReplicationExtRoute::ResetStatus => {
            return Err(s3_error!(InternalError, "invalid route for metrics response"));
        }
    };

    let mut resp = S3Response::with_status(Body::from(body), StatusCode::OK);
    resp.headers
        .insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    Ok(resp)
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

fn parse_reset_start_target(uri: &Uri) -> S3Result<ReplicationResetTarget> {
    let arn = query_value_exact(uri, "arn")
        .filter(|v| !v.is_empty())
        .ok_or_else(|| s3_error!(InvalidRequest, "arn query parameter is required"))?;

    if let Some(older_than) = query_value_exact(uri, "older-than")
        && !older_than.is_empty()
    {
        parse_duration(&older_than).map_err(|err| s3_error!(InvalidRequest, "invalid older-than query parameter: {err}"))?;
    }

    let reset_id = query_value_exact(uri, "reset-id")
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    Ok(ReplicationResetTarget { arn, reset_id })
}

fn build_replication_reset_response(targets: Vec<ReplicationResetTarget>) -> S3Result<S3Response<Body>> {
    let data = serde_json::to_vec(&ReplicationResetResponse { targets }).map_err(|e| s3_error!(InternalError, "{e}"))?;
    let mut resp = S3Response::with_status(Body::from(data), StatusCode::OK);
    resp.headers
        .insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    Ok(resp)
}

async fn handle_replication_extension_request(
    req: &mut S3Request<Body>,
    ext_req: &ReplicationExtRequest,
) -> S3Result<S3Response<Body>> {
    authorize_replication_extension_request(req, ext_req).await?;
    ensure_replication_bucket_ready(&ext_req.bucket).await?;

    match ext_req.route {
        ReplicationExtRoute::MetricsV1 | ReplicationExtRoute::MetricsV2 => {
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
            Ok(S3Response::with_status(Body::from(String::new()), StatusCode::OK))
        }
        ReplicationExtRoute::ResetStatus => build_replication_reset_response(Vec::new()),
        ReplicationExtRoute::ResetStart => {
            let target = parse_reset_start_target(&req.uri)?;
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
            let get_resp = usecase.execute_get_object(get_req).await?;
            convert_get_object_response(get_resp)
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
            Ok(build_listen_notification_response(&req.uri))
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

        is_admin_path(path) || path.starts_with(RPC_PREFIX) || is_console_path(path)
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

        // Check RPC signature verification
        if req.uri.path().starts_with(RPC_PREFIX) {
            // Skip signature verification for HEAD requests (health checks)
            if req.method != Method::HEAD {
                verify_rpc_signature(&req.uri.to_string(), &req.method, &req.headers).map_err(|e| {
                    error!("RPC signature verification failed: {}", e);
                    s3_error!(AccessDenied, "{}", e)
                })?;
            }
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
            return handle_misc_extension_request(&mut req, &ext_req).await;
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
        let invalid_value: Uri = "/demo-bucket?replication-metrics=1".parse().expect("uri should parse");
        let wrong_method: Uri = "/demo-bucket?replication-check".parse().expect("uri should parse");
        let wrong_method_reset: Uri = "/demo-bucket?replication-reset".parse().expect("uri should parse");
        let wrong_method_status: Uri = "/demo-bucket?replication-reset-status".parse().expect("uri should parse");

        assert!(parse_replication_extension_request(&Method::GET, &object_level).is_none());
        assert!(parse_replication_extension_request(&Method::GET, &invalid_value).is_none());
        assert!(parse_replication_extension_request(&Method::PUT, &wrong_method).is_none());
        assert!(parse_replication_extension_request(&Method::GET, &wrong_method_reset).is_none());
        assert!(parse_replication_extension_request(&Method::PUT, &wrong_method_status).is_none());
    }

    #[test]
    fn parse_misc_extension_request_matches_object_lambda_and_listen_notification() {
        let object_lambda: Uri = "/demo-bucket/path/to/object.txt?lambdaArn=arn%3Atarget"
            .parse()
            .expect("uri should parse");
        let listen_bucket: Uri = "/demo-bucket?events=s3:ObjectCreated:*".parse().expect("uri should parse");
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

    #[tokio::test]
    async fn build_listen_notification_response_sets_event_stream_headers() {
        let uri: Uri = "/demo-bucket?events=s3:ObjectCreated:Put&ping=1"
            .parse()
            .expect("uri should parse");

        let resp = build_listen_notification_response(&uri);

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
