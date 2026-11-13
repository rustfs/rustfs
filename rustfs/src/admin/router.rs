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

use super::route_table::{RouteTable, canonicalize_admin_path, is_admin_v4_fallback_path, route_key_canonical};
use super::storage_api::config::read_admin_config_without_migrate;
use crate::admin::console::{is_console_path, make_console_server};
use crate::admin::handlers::oidc::is_oidc_path;
use crate::admin::handlers::s3_extension;
use crate::admin::operation::{AdminRequest, AdminResponse, Operation as OwnedOperation};
use crate::admin::runtime_sources::{
    ServerContextSlot, current_object_store_handle, current_region, current_server_config, default_object_usecase,
};
use crate::admin::storage_api::access::ReqInfo;
use crate::admin::utils::{parse_query_pairs, query_value_exact, query_values_exact};
use crate::auth::{check_key_valid, constant_time_eq, get_session_token, reject_unsigned_amz_headers_on_sigv4_request};
use crate::license::license_check;
use crate::server::{
    ADMIN_PREFIX, HEALTH_PREFIX, HEALTH_READY_PATH, PROFILE_CPU_PATH, PROFILE_MEMORY_PATH, is_admin_path, is_sts_query_request,
    is_table_catalog_path,
};
use http::HeaderValue;
use http::header::HeaderName;
use hyper::HeaderMap;
use hyper::Method;
use hyper::StatusCode;
use hyper::Uri;
use hyper::http::Extensions;
use matchit::Params;
use reqwest::Url;
use rustfs_config::notify::NOTIFY_WEBHOOK_SUB_SYS;
use rustfs_config::server_config::Config;
use rustfs_config::{
    ENABLE_KEY, WEBHOOK_AUTH_TOKEN, WEBHOOK_CLIENT_CA, WEBHOOK_CLIENT_CERT, WEBHOOK_CLIENT_KEY, WEBHOOK_ENDPOINT,
    WEBHOOK_SKIP_TLS_VERIFY,
};
use rustfs_madmin::utils::parse_duration;
use rustfs_notify::notification_system;
use rustfs_signer::pre_sign_v4;
use rustfs_utils::egress::{OutboundDnsResolver, OutboundPolicy};
use rustfs_utils::http::get_source_scheme;
use rustfs_utils::http::headers::{
    AMZ_CHECKSUM_CRC32, AMZ_CHECKSUM_CRC32C, AMZ_CHECKSUM_CRC64NVME, AMZ_CHECKSUM_SHA1, AMZ_CHECKSUM_SHA256, AMZ_CHECKSUM_TYPE,
};
use s3s::Body;
use s3s::S3Error;
use s3s::S3ErrorCode;
use s3s::S3Request;
use s3s::S3Response;
use s3s::S3Result;
use s3s::auth::SigV4PathEncoding;
use s3s::dto::{GetObjectInput, GetObjectOutput, IfMatch, IfNoneMatch, Range, StreamingBlob, Timestamp, TimestampFormat};
use s3s::header;
use s3s::route::S3Route;
use s3s::s3_error;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::time::Duration;
use tower::Service;
use tracing::{error, warn};
use url::form_urlencoded;
use uuid::Uuid;

pub const ADMIN_OBJECT_ZIP_DOWNLOADS_PATH: &str = "/v3/object-zip-downloads";
pub(crate) const LOG_COMPONENT_ADMIN: &str = "admin";
const LOG_SUBSYSTEM_OBJECT_LAMBDA: &str = "object_lambda";
pub(crate) const EVENT_ADMIN_ROUTER_STATE: &str = "admin_router_state";

#[derive(Debug, Clone)]
struct ObjectLambdaWebhookConfig {
    endpoint: Url,
    auth_token: String,
    client_cert: String,
    client_key: String,
    client_ca: String,
    skip_tls_verify: bool,
    response_header_timeout: Option<Duration>,
    outbound_resolver: OutboundDnsResolver,
}

const LAMBDA_WEBHOOK_SUB_SYS: &str = "lambda_webhook";
const WEBHOOK_RESPONSE_HEADER_TIMEOUT: &str = "response_header_timeout";
const OBJECT_LAMBDA_PRESIGN_EXPIRES_SECS: i64 = 3600;

fn extract_bucket_object_path(path: &str) -> Option<(String, String)> {
    let path = path.strip_prefix('/')?;
    let (bucket, object) = path.split_once('/')?;
    if bucket.is_empty() || object.is_empty() {
        return None;
    }
    Some((bucket.to_string(), object.to_string()))
}

/// The bucket and key an object lambda request names: `GET /{bucket}/{key}?lambdaArn=…`.
fn parse_object_lambda_request(method: &Method, uri: &Uri) -> Option<(String, String)> {
    if method != Method::GET || query_value_exact(uri, "lambdaArn").is_none() {
        return None;
    }
    extract_bucket_object_path(uri.path())
}

fn validate_object_lambda_query(uri: &Uri) -> S3Result<()> {
    let lambda_arns = query_values_exact(uri, "lambdaArn");
    if lambda_arns.len() != 1 || lambda_arns[0].trim().is_empty() {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidRequest,
            "lambdaArn query parameter must be provided exactly once",
        ));
    }

    let lambda_arn = lambda_arns[0].trim();
    let arn_parts = lambda_arn.split(':').collect::<Vec<_>>();
    let is_valid_arn = arn_parts.len() >= 6 && arn_parts[0] == "arn" && !arn_parts[1].is_empty() && !arn_parts[2].is_empty();
    if !is_valid_arn {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidRequest,
            "lambdaArn query parameter must be a valid ARN string",
        ));
    }
    Ok(())
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
                .map_err(|_| S3Error::with_message(S3ErrorCode::InvalidRequest, "failed to rebuild request URI"))?,
        )
    } else {
        let query = form_urlencoded::Serializer::new(String::new())
            .extend_pairs(filtered.iter().map(|(key, value)| (key.as_str(), value.as_str())))
            .finish();
        Some(
            format!("{}?{}", uri.path(), query)
                .parse()
                .map_err(|_| S3Error::with_message(S3ErrorCode::InvalidRequest, "failed to rebuild request URI"))?,
        )
    };
    Uri::from_parts(parts).map_err(|_| S3Error::with_message(S3ErrorCode::InvalidRequest, "failed to rebuild request URI"))
}

fn parse_optional_header(headers: &HeaderMap, name: HeaderName) -> S3Result<Option<String>> {
    headers
        .get(name)
        .map(|value| {
            value
                .to_str()
                .map(|parsed| parsed.to_string())
                .map_err(|_| S3Error::with_message(S3ErrorCode::InvalidRequest, "request header contains invalid utf-8"))
        })
        .transpose()
}

fn parse_optional_timestamp_header(headers: &HeaderMap, name: HeaderName) -> S3Result<Option<Timestamp>> {
    parse_optional_header(headers, name)?
        .map(|value| {
            Timestamp::parse(TimestampFormat::HttpDate, &value)
                .map_err(|_| S3Error::with_message(S3ErrorCode::InvalidRequest, "request timestamp header is invalid"))
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
                .map_err(|_| S3Error::with_message(S3ErrorCode::InvalidRequest, "request etag condition header is invalid"))
        })
        .transpose()
}

fn build_object_lambda_get_request(req: &S3Request<Body>, bucket: &str, object: &str) -> S3Result<S3Request<GetObjectInput>> {
    let filtered_uri = uri_without_query_key(&req.uri, "lambdaArn")?;
    let part_number = query_value_exact(&filtered_uri, "partNumber")
        .filter(|value| !value.is_empty())
        .map(|value| {
            value.parse::<i32>().map_err(|_| {
                S3Error::with_message(S3ErrorCode::InvalidArgument, "partNumber query parameter must be a positive integer")
            })
        })
        .transpose()?;
    let version_id = query_value_exact(&filtered_uri, "versionId").filter(|value| !value.is_empty());
    let range = parse_optional_header(&req.headers, header::RANGE)?
        .map(|value| {
            Range::parse(&value).map_err(|_| S3Error::with_message(S3ErrorCode::InvalidArgument, "Range header is invalid"))
        })
        .transpose()?;

    let mut builder = GetObjectInput::builder()
        .bucket(bucket.to_string())
        .key(object.to_string())
        .part_number(part_number)
        .version_id(version_id)
        .range(range)
        .if_match(parse_optional_etag_condition_header::<IfMatch>(&req.headers, header::IF_MATCH)?)
        .if_none_match(parse_optional_etag_condition_header::<IfNoneMatch>(&req.headers, header::IF_NONE_MATCH)?)
        .if_modified_since(parse_optional_timestamp_header(&req.headers, header::IF_MODIFIED_SINCE)?)
        .if_unmodified_since(parse_optional_timestamp_header(&req.headers, header::IF_UNMODIFIED_SINCE)?);

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

    let input = builder.build().map_err(|err| {
        S3Error::with_message(S3ErrorCode::InvalidRequest, format!("failed to build object lambda get request: {err}"))
    })?;

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
        .ok_or_else(|| {
            S3Error::with_message(S3ErrorCode::InvalidRequest, "lambdaArn query parameter must be provided exactly once")
        })?;

    lambda_arn.parse::<rustfs_targets::arn::ARN>().map_err(|_| {
        S3Error::with_message(
            S3ErrorCode::InvalidRequest,
            "lambdaArn query parameter must reference a supported target ARN",
        )
    })
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
        return Err(S3Error::with_message(
            S3ErrorCode::NotImplemented,
            "object lambda target type is not supported",
        ));
    }

    let subsystem = config
        .0
        .get(LAMBDA_WEBHOOK_SUB_SYS)
        .or_else(|| config.0.get(NOTIFY_WEBHOOK_SUB_SYS))
        .ok_or_else(|| S3Error::with_message(S3ErrorCode::InvalidRequest, "object lambda webhook subsystem is not configured"))?;
    let kvs = subsystem
        .get(&arn.target_id.id)
        .ok_or_else(|| S3Error::with_message(S3ErrorCode::InvalidRequest, "object lambda target is not configured"))?;

    if !config_enable_is_on(&kvs.get(ENABLE_KEY)) {
        return Err(S3Error::with_message(S3ErrorCode::InvalidRequest, "object lambda target is disabled"));
    }

    let endpoint = kvs.lookup(WEBHOOK_ENDPOINT).unwrap_or_default();
    if endpoint.trim().is_empty() {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidRequest,
            "object lambda target endpoint is empty",
        ));
    }

    let response_header_timeout = match kvs.lookup(WEBHOOK_RESPONSE_HEADER_TIMEOUT) {
        Some(value) if value.trim().is_empty() => None,
        Some(value) => Some(parse_duration(&value).map_err(|_| {
            S3Error::with_message(S3ErrorCode::InvalidRequest, "object lambda target response_header_timeout is invalid")
        })?),
        None => None,
    };

    let parsed_endpoint = Url::parse(&endpoint)
        .map_err(|_| S3Error::with_message(S3ErrorCode::InvalidRequest, "object lambda target endpoint is invalid"))?;
    let outbound_resolver = outbound_policy()?
        .resolver_for(&parsed_endpoint)
        .map_err(|err| s3_error!(InvalidRequest, "object lambda target endpoint is not allowed: {}", err))?;

    Ok(ObjectLambdaWebhookConfig {
        endpoint: parsed_endpoint,
        auth_token: kvs.lookup(WEBHOOK_AUTH_TOKEN).unwrap_or_default(),
        client_cert: kvs.lookup(WEBHOOK_CLIENT_CERT).unwrap_or_default(),
        client_key: kvs.lookup(WEBHOOK_CLIENT_KEY).unwrap_or_default(),
        client_ca: kvs.lookup(WEBHOOK_CLIENT_CA).unwrap_or_default(),
        skip_tls_verify: config_enable_is_on(&kvs.lookup(WEBHOOK_SKIP_TLS_VERIFY).unwrap_or_default()),
        response_header_timeout,
        outbound_resolver,
    })
}

async fn load_current_server_config() -> S3Result<Config> {
    if let Some(system) = notification_system() {
        return Ok(system.config_snapshot().await);
    }

    if let Some(store) = current_object_store_handle() {
        match read_admin_config_without_migrate(store).await {
            Ok(config) => return Ok(config),
            Err(err) => {
                warn!(
                    event = EVENT_ADMIN_ROUTER_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_OBJECT_LAMBDA,
                    result = "config_reload_failed",
                    error = %err,
                    "admin router state"
                );
            }
        }
    }

    let config = current_server_config()
        .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "server config is not initialized"))?;
    Ok(config)
}

async fn resolve_object_lambda_webhook_config(uri: &Uri) -> S3Result<ObjectLambdaWebhookConfig> {
    let config = load_current_server_config().await?;
    let arn = parse_object_lambda_arn(uri)?;
    resolve_object_lambda_webhook_config_from_server_config(&config, &arn)
}

fn build_object_lambda_http_client(config: &ObjectLambdaWebhookConfig) -> S3Result<reqwest::Client> {
    build_object_lambda_http_client_with_resolver(config, config.outbound_resolver.clone())
}

fn build_object_lambda_http_client_with_resolver(
    config: &ObjectLambdaWebhookConfig,
    resolver: impl reqwest::dns::Resolve + 'static,
) -> S3Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .no_proxy()
        .dns_resolver(resolver)
        .redirect(reqwest::redirect::Policy::none())
        .user_agent(rustfs_targets::get_user_agent(rustfs_targets::ServiceType::Basis));

    if let Some(timeout) = config.response_header_timeout {
        builder = builder.timeout(timeout);
    }

    if config.skip_tls_verify {
        warn!(
            event = EVENT_ADMIN_ROUTER_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_OBJECT_LAMBDA,
            result = "tls_verification_disabled",
            endpoint_origin = %config.endpoint.origin().ascii_serialization(),
            "admin router state"
        );
        builder = builder.danger_accept_invalid_certs(true);
    } else if !config.client_ca.is_empty() {
        let ca_pem = std::fs::read(&config.client_ca).map_err(|e| {
            S3Error::with_message(S3ErrorCode::InternalError, format!("failed to read object lambda client_ca: {e}"))
        })?;
        let ca = reqwest::Certificate::from_pem(&ca_pem).map_err(|e| {
            S3Error::with_message(S3ErrorCode::InternalError, format!("failed to parse object lambda client_ca: {e}"))
        })?;
        builder = builder.add_root_certificate(ca);
    }

    if !config.client_cert.is_empty() || !config.client_key.is_empty() {
        if config.client_cert.is_empty() || config.client_key.is_empty() {
            return Err(s3_error!(
                InvalidRequest,
                "object lambda client_cert and client_key must be configured together"
            ));
        }

        let cert = std::fs::read(&config.client_cert).map_err(|e| {
            S3Error::with_message(S3ErrorCode::InternalError, format!("failed to read object lambda client_cert: {e}"))
        })?;
        let key = std::fs::read(&config.client_key).map_err(|e| {
            S3Error::with_message(S3ErrorCode::InternalError, format!("failed to read object lambda client_key: {e}"))
        })?;
        let identity = reqwest::Identity::from_pem(&[cert, key].concat()).map_err(|e| {
            S3Error::with_message(S3ErrorCode::InternalError, format!("failed to build object lambda client identity: {e}"))
        })?;
        builder = builder.identity(identity);
    }

    builder
        .build()
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("failed to build object lambda http client: {e}")))
}

async fn send_object_lambda_request(request: reqwest::RequestBuilder) -> S3Result<reqwest::Response> {
    request
        .send()
        .await
        .map_err(|_| S3Error::with_message(S3ErrorCode::InternalError, "object lambda target request failed"))
}

fn outbound_policy() -> S3Result<&'static OutboundPolicy> {
    OutboundPolicy::from_env_cached().map_err(|err| s3_error!(InvalidRequest, "invalid outbound policy: {}", err))
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
    let credentials = req.credentials.as_ref().ok_or_else(|| {
        S3Error::with_message(S3ErrorCode::AccessDenied, "object lambda source URL requires authenticated credentials")
    })?;
    let host = extract_request_host(&req.headers, &req.uri).ok_or_else(|| {
        S3Error::with_message(S3ErrorCode::InvalidRequest, "object lambda source URL requires a valid host header")
    })?;
    let scheme = extract_request_scheme(&req.headers, &req.uri);
    let filtered_uri = uri_without_query_key(&req.uri, "lambdaArn")?;
    let path_and_query = filtered_uri
        .path_and_query()
        .map(|value| value.as_str().to_string())
        .unwrap_or_else(|| filtered_uri.path().to_string());
    let source_uri = format!("{scheme}://{host}{path_and_query}").parse::<Uri>().map_err(|e| {
        S3Error::with_message(S3ErrorCode::InvalidRequest, format!("failed to construct object lambda source URL: {e}"))
    })?;
    let region = req
        .region
        .clone()
        .or_else(current_region)
        .map(|value| value.as_str().to_string())
        .unwrap_or_else(|| "us-east-1".to_string());
    let session_token = get_session_token(&req.uri, &req.headers).unwrap_or_default().to_string();

    let presigned = pre_sign_v4(
        http::Request::builder()
            .method(Method::GET)
            .uri(source_uri)
            .header(http::header::HOST, host)
            .body(Body::default())
            .map_err(|e| {
                S3Error::with_message(S3ErrorCode::InvalidRequest, format!("failed to build object lambda source request: {e}"))
            })?,
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
    .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("failed to serialize object lambda payload: {e}")))
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

    if route.is_some_and(|route| constant_time_eq(route, output_route))
        && token.is_some_and(|token| constant_time_eq(token, output_token))
    {
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
        .map_err(|_| S3Error::with_message(S3ErrorCode::InternalError, "failed to format timestamp header"))?;
    String::from_utf8(buf).map_err(|_| S3Error::with_message(S3ErrorCode::InternalError, "failed to format timestamp header"))
}

fn insert_string_header(headers: &mut HeaderMap, name: HeaderName, value: String) -> S3Result<()> {
    let header_value = HeaderValue::from_str(&value)
        .map_err(|_| S3Error::with_message(S3ErrorCode::InternalError, "failed to build response header value"))?;
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
            etag.to_http_header()
                .map_err(|_| S3Error::with_message(S3ErrorCode::InternalError, "invalid etag"))?,
        );
    }
    if let Some(last_modified) = &output.last_modified {
        insert_string_header(&mut headers, http::header::LAST_MODIFIED, format_timestamp_http_date(last_modified)?)?;
    }
    if let Some(expires) = &output.expires {
        insert_string_header(&mut headers, http::header::EXPIRES, expires.clone())?;
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
        insert_string_header(&mut headers, HeaderName::from_static(AMZ_CHECKSUM_CRC32), checksum_crc32.clone())?;
    }
    if let Some(checksum_crc32c) = &output.checksum_crc32c {
        insert_string_header(&mut headers, HeaderName::from_static(AMZ_CHECKSUM_CRC32C), checksum_crc32c.clone())?;
    }
    if let Some(checksum_crc64nvme) = &output.checksum_crc64nvme {
        insert_string_header(&mut headers, HeaderName::from_static(AMZ_CHECKSUM_CRC64NVME), checksum_crc64nvme.clone())?;
    }
    if let Some(checksum_sha1) = &output.checksum_sha1 {
        insert_string_header(&mut headers, HeaderName::from_static(AMZ_CHECKSUM_SHA1), checksum_sha1.clone())?;
    }
    if let Some(checksum_sha256) = &output.checksum_sha256 {
        insert_string_header(&mut headers, HeaderName::from_static(AMZ_CHECKSUM_SHA256), checksum_sha256.clone())?;
    }
    if let Some(checksum_type) = &output.checksum_type {
        insert_string_header(
            &mut headers,
            HeaderName::from_static(AMZ_CHECKSUM_TYPE),
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
                    .map_err(|_| S3Error::with_message(S3ErrorCode::InternalError, "failed to build metadata response header"))?;
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
        HeaderName::from_static(AMZ_CHECKSUM_CRC32),
        HeaderName::from_static(AMZ_CHECKSUM_CRC32C),
        HeaderName::from_static(AMZ_CHECKSUM_CRC64NVME),
        HeaderName::from_static(AMZ_CHECKSUM_SHA1),
        HeaderName::from_static(AMZ_CHECKSUM_SHA256),
        HeaderName::from_static(AMZ_CHECKSUM_TYPE),
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
        .ok_or_else(|| {
            S3Error::with_message(S3ErrorCode::InvalidRequest, "lambdaArn query parameter must be provided exactly once")
        })?;
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

    let lambda_response = send_object_lambda_request(request_builder).await?;

    let status = lambda_response.status();
    let lambda_headers = lambda_response.headers().clone();
    if status.is_success() {
        validate_object_lambda_response_auth_headers(&lambda_headers, &output_route, &output_token)?;
    }
    let body = Body::from(StreamingBlob::wrap(lambda_response.bytes_stream()));
    Ok(build_object_lambda_passthrough_response(response_headers, &lambda_headers, status, body))
}

async fn authorize_object_lambda_request(req: &mut S3Request<Body>, bucket: &str, object: &str) -> S3Result<()> {
    let Some(input_cred) = req.credentials.as_ref() else {
        return Err(S3Error::with_message(S3ErrorCode::AccessDenied, "Signature is required"));
    };

    let (cred, is_owner) =
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

    req.extensions.insert(ReqInfo {
        cred: Some(cred),
        is_owner,
        bucket: Some(bucket.to_owned()),
        object: Some(object.to_owned()),
        version_id: None,
        region: current_region(),
        ..Default::default()
    });

    license_check().map_err(|er| match er.kind() {
        std::io::ErrorKind::PermissionDenied => S3Error::with_message(S3ErrorCode::AccessDenied, format!("{er}")),
        _ => {
            error!(
                event = EVENT_ADMIN_ROUTER_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_OBJECT_LAMBDA,
                result = "license_check_failed",
                error = %er,
                "admin router state"
            );
            S3Error::with_message(S3ErrorCode::InternalError, "License validation failed")
        }
    })?;

    // The bridged GetObject hook evaluates the actual version and all read conditions.
    Ok(())
}

async fn authorized_object_lambda_get_request(
    req: &S3Request<Body>,
    bucket: &str,
    object: &str,
) -> S3Result<S3Request<GetObjectInput>> {
    let mut get_req = build_object_lambda_get_request(req, bucket, object)?;
    let slot = req
        .extensions
        .get::<Arc<ServerContextSlot>>()
        .cloned()
        .ok_or_else(server_context_not_ready_error)?;
    s3s::access::S3Access::get_object(&crate::storage::ecfs::FS::with_server_ctx(slot), &mut get_req).await?;
    Ok(get_req)
}

async fn handle_object_lambda_request(req: &mut S3Request<Body>, bucket: &str, object: &str) -> S3Result<S3Response<Body>> {
    authorize_object_lambda_request(req, bucket, object).await?;
    validate_object_lambda_query(&req.uri)?;
    let get_req = authorized_object_lambda_get_request(req, bucket, object).await?;
    let usecase = default_object_usecase();
    let get_resp = usecase.execute_get_object(get_req).await?;
    invoke_object_lambda_target(req, bucket, object, get_resp).await
}

pub struct S3Router<T> {
    table: RouteTable<T>,
    console_enabled: bool,
    console_router: Option<axum::routing::RouterIntoService<Body>>,
    /// This server's request-path context slot (backlog#1052 S2). Injected
    /// into every request's extensions at dispatch so the static admin
    /// operations can resolve their server's store.
    server_ctx: Option<Arc<ServerContextSlot>>,
}

fn is_public_health_path(path: &str) -> bool {
    path == HEALTH_PREFIX || path == HEALTH_READY_PATH
}

fn reject_unbound_custom_route_auth(req: &S3Request<Body>) -> S3Result<()> {
    // V2 does not bind custom route query arguments or payloads to the signer.
    let signed_v2 = req
        .headers
        .get_all(http::header::AUTHORIZATION)
        .iter()
        .any(|value| value.to_str().is_ok_and(|value| value.starts_with("AWS ")));
    let presigned_v2 = req
        .uri
        .query()
        .is_some_and(|query| url::form_urlencoded::parse(query.as_bytes()).any(|(key, _)| key == "Signature"));
    let post_policy = req.method == Method::POST
        && req.credentials.is_some()
        && req
            .headers
            .get(http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| {
                value
                    .split(';')
                    .next()
                    .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("multipart/form-data"))
            });
    if signed_v2 || presigned_v2 || post_policy {
        return Err(s3_error!(
            AccessDenied,
            "Header or query Signature Version 4 is required for this operation"
        ));
    }
    Ok(())
}

fn server_context_not_ready_error() -> S3Error {
    S3Error::with_message(S3ErrorCode::ServiceUnavailable, "server context is not ready")
}

fn is_object_zip_download_token_path(method: &Method, uri: &Uri) -> bool {
    if method != Method::GET {
        return false;
    }

    let path = canonicalize_admin_path(uri.path());
    path.starts_with(&format!("{ADMIN_PREFIX}{ADMIN_OBJECT_ZIP_DOWNLOADS_PATH}/"))
        && path.ends_with(".zip")
        && query_value_exact(uri, "token").is_some_and(|token| !token.is_empty())
}

impl<T> S3Router<T> {
    pub fn new(console_enabled: bool) -> Self {
        let console_router = if console_enabled {
            Some(make_console_server().into_service::<Body>())
        } else {
            None
        };

        Self {
            table: RouteTable::new(),
            console_enabled,
            console_router,
            server_ctx: None,
        }
    }

    /// Bind this router to its server's context slot (backlog#1052 S2); the
    /// slot is handed to every dispatched request via its extensions.
    pub fn set_server_ctx(&mut self, server_ctx: Arc<ServerContextSlot>) {
        self.server_ctx = Some(server_ctx);
    }

    pub fn insert(&mut self, method: Method, path: &str, operation: T) -> std::io::Result<()> {
        self.table.insert(method, path, operation)
    }
}

#[cfg(test)]
impl<T> S3Router<T> {
    pub(crate) fn contains_route(&self, method: Method, path: &str) -> bool {
        self.table.contains_route(method, path)
    }

    pub(crate) fn contains_compatible_route(&self, method: Method, path: &str) -> bool {
        self.table.contains_compatible_route(method, path)
    }

    pub(crate) fn registered_routes(&self) -> &[String] {
        self.table.registered_routes()
    }
}

impl<T> Default for S3Router<T> {
    fn default() -> Self {
        Self::new(false)
    }
}

#[async_trait::async_trait]
impl<T> S3Route for S3Router<T>
where
    T: OwnedOperation,
{
    fn is_match(&self, method: &Method, uri: &Uri, headers: &HeaderMap, extensions: &mut Extensions) -> bool {
        if s3_extension::route_key(method, uri).is_some() || parse_object_lambda_request(method, uri).is_some() {
            return true;
        }

        let path = uri.path();
        if is_table_catalog_path(path) {
            extensions.insert(SigV4PathEncoding::DoubleEncoded);
            return true;
        }

        // Profiling endpoints
        if method == Method::GET && (path == PROFILE_CPU_PATH || path == PROFILE_MEMORY_PATH) {
            return true;
        }

        // Health check
        if (method == Method::HEAD || method == Method::GET) && is_public_health_path(path) {
            return true;
        }

        // AssumeRole
        if is_sts_query_request(method, uri, headers) {
            return true;
        }

        is_admin_path(path) || is_console_path(path)
    }

    // check_access before call
    async fn check_access(&self, req: &mut S3Request<Body>) -> S3Result<()> {
        // GHSA-g8w9-qw9q-fghr: custom routes bypass `S3Access::check`, so the
        // SigV4 signed-header rule is enforced here as well.
        reject_unsigned_amz_headers_on_sigv4_request(&req.headers, req.uri.query())?;

        if let Some(server_ctx) = &self.server_ctx {
            req.extensions.insert(server_ctx.clone());
            if !is_public_health_path(req.uri.path()) && server_ctx.installed_app_context().is_none() {
                return Err(server_context_not_ready_error());
            }
        }
        if s3_extension::route_key(&req.method, &req.uri).is_some()
            || parse_object_lambda_request(&req.method, &req.uri).is_some()
        {
            reject_unbound_custom_route_auth(req)?;
            return match req.credentials {
                Some(_) => Ok(()),
                None => Err(S3Error::with_message(S3ErrorCode::AccessDenied, "Signature is required")),
            };
        }

        // Allow unauthenticated access to health check
        let path = req.uri.path();

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

        // Object ZIP downloads are browser-navigated with a short-lived token;
        // the handler validates the token before returning any bytes.
        if is_object_zip_download_token_path(&req.method, &req.uri) {
            return Ok(());
        }

        // Allow unauthenticated STS requests to POST / (AssumeRoleWithWebIdentity
        // doesn't use SigV4 — the JWT token in the request body is the authentication).
        // The handler dispatches on the Action parameter: AssumeRole will reject if
        // credentials are missing, AssumeRoleWithWebIdentity will validate the JWT.
        // Require application/x-www-form-urlencoded Content-Type to narrow the bypass.
        if req.credentials.is_none() && is_sts_query_request(&req.method, &req.uri, &req.headers) {
            return Ok(());
        }

        // For non-RPC admin requests, check credentials
        reject_unbound_custom_route_auth(req)?;
        match req.credentials {
            Some(_) => Ok(()),
            None => Err(S3Error::with_message(S3ErrorCode::AccessDenied, "Signature is required")),
        }
    }

    async fn call(&self, mut req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        if let Some(server_ctx) = &self.server_ctx {
            req.extensions.insert(server_ctx.clone());
            if !is_public_health_path(req.uri.path()) && server_ctx.installed_app_context().is_none() {
                return Err(server_context_not_ready_error());
            }
        }
        if let Some((bucket, object)) = parse_object_lambda_request(&req.method, &req.uri) {
            return Box::pin(handle_object_lambda_request(&mut req, &bucket, &object)).await;
        }
        // Extension routes are claimed ahead of the console.
        let extension_key = s3_extension::route_key(&req.method, &req.uri);

        // Console requests should be handled by console router first (including OPTIONS)
        // Console has its own CORS layer configured
        if extension_key.is_none() && self.console_enabled && is_console_path(req.uri.path()) {
            if let Some(console_router) = &self.console_router {
                let mut console_router = console_router.clone();
                let req = convert_request(req);
                let result = console_router.call(req).await;
                return match result {
                    Ok(resp) => Ok(convert_response(resp)),
                    Err(e) => Err(s3_error!(InternalError, "{}", e)),
                };
            }
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "console is not enabled"));
        }

        let key = extension_key.unwrap_or_else(|| route_key_canonical(&req.method, req.uri.path()));

        if let Ok(mat) = self.table.at(&key) {
            let op: &T = mat.value;
            let envelope = crate::app::RequestEnvelope::from_s3s(&req);
            let request = AdminRequest::new(envelope, req.input.into());
            let (status, headers, body) = op.call(request, mat.params).await?.into_parts();
            let mut response = S3Response::with_headers(body.into(), headers);
            response.status = Some(status);

            return Ok(response);
        }

        if is_admin_v4_fallback_path(req.uri.path()) {
            let mut resp = S3Response::new(Body::empty());
            resp.status = Some(StatusCode::UPGRADE_REQUIRED);
            return Ok(resp);
        }

        Err(S3Error::new(S3ErrorCode::NotImplemented))
    }
}

/// Registers one owned admin operation with the legacy router. The router
/// converts the request and response at its edge, so the operation never sees
/// an s3s type.
pub struct AdminOperation(pub(crate) &'static dyn OwnedOperation);

#[async_trait::async_trait]
impl OwnedOperation for AdminOperation {
    async fn call(&self, req: AdminRequest, params: Params<'_, '_>) -> rustfs_s3_types::S3Result<AdminResponse> {
        self.0.call(req, params).await
    }
}

#[cfg(test)]
mod s3_extension_tests;

#[cfg(test)]
mod tests {
    #[test]
    fn object_lambda_bridge_enforces_version_read_permissions() {
        crate::app::gating_test_env::run_large_stack_test("lambda-version-permissions", || async {
            use crate::admin::runtime_sources::AppContext;
            use crate::admin::storage_api::contract::bucket::{BucketOperations as _, MakeBucketOptions};
            let store = crate::app::gating_test_env::shared_gating_ecstore().await;
            let ambient = crate::app::gating_test_env::shared_gating_ambient().await;
            let slot = ServerContextSlot::new();
            assert!(slot.install(Arc::new(AppContext::new(store.clone(), ambient.iam(), ambient.kms()))));
            let bucket = format!("lambda-version-{}", uuid::Uuid::new_v4());
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("create bucket");
            for (grant_current, grant_versions, deny_versions) in [
                (true, false, false),
                (true, true, true),
                (true, true, false),
                (false, true, false),
            ] {
                let mut statements = Vec::new();
                if grant_current {
                    statements.push(serde_json::json!({"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":format!("arn:aws:s3:::{bucket}/*")}));
                }
                for (enabled, effect) in [(grant_versions, "Allow"), (deny_versions, "Deny")] {
                    if enabled {
                        statements.push(serde_json::json!({"Effect":effect,"Principal":{"AWS":"*"},"Action":"s3:GetObjectVersion","Resource":format!("arn:aws:s3:::{bucket}/*")}));
                    }
                }
                store
                    .update_bucket_metadata_config(
                        &bucket,
                        crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
                        serde_json::to_vec(&serde_json::json!({"Version":"2012-10-17","Statement":statements})).unwrap(),
                    )
                    .await
                    .expect("publish policy");
                for version in [None, Some("null"), Some("12345678-1234-1234-1234-123456789abc")] {
                    let mut req = router_request(Method::GET, "/");
                    req.uri = format!(
                        "/{bucket}/object?lambdaArn=arn:aws:s3-object-lambda:us-east-1:123456789012:accesspoint/test{}",
                        version.map(|v| format!("&versionId={v}")).unwrap_or_default()
                    )
                    .parse()
                    .unwrap();
                    req.extensions.insert(slot.clone());
                    req.extensions.insert(ReqInfo::default());
                    req.headers.insert(http::header::IF_NONE_MATCH, HeaderValue::from_static("*"));
                    let result = authorized_object_lambda_get_request(&req, &bucket, "object").await;
                    // Version-only Allow covers current reads; Version Deny overrides
                    // the explicit current-read Allow as well as version grants.
                    if deny_versions || (version.is_some() && !grant_versions) {
                        assert_eq!(
                            result
                                .expect_err("version read must be denied before usecase or webhook")
                                .code(),
                            &s3s::S3ErrorCode::AccessDenied
                        );
                    } else {
                        let request = result.expect("authorized read remains available");
                        assert_eq!(request.input.version_id.as_deref(), version);
                        assert_eq!(request.extensions.get::<ReqInfo>().unwrap().version_id.as_deref(), version);
                    }
                }
            }
        });
    }

    use super::*;
    use crate::admin::operation::{AdminRequest, AdminResponse, Operation as OwnedOperation};
    use crate::auth::constant_time_eq;
    use bytes::Bytes;
    use http::HeaderMap;
    use http::Method;
    use http::Uri;
    use rustfs_s3_types::{Body as OwnedBody, S3Error as OwnedS3Error, S3ErrorCode as OwnedS3ErrorCode};
    use s3s::S3Request;
    use std::io;
    use std::net::{IpAddr, SocketAddr};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[derive(Clone)]
    struct StaticResolver(IpAddr);

    impl reqwest::dns::Resolve for StaticResolver {
        fn resolve(&self, _name: reqwest::dns::Name) -> reqwest::dns::Resolving {
            let address = SocketAddr::new(self.0, 0);
            Box::pin(async move { Ok(Box::new(std::iter::once(address)) as reqwest::dns::Addrs) })
        }
    }

    fn object_lambda_test_config(endpoint: Url) -> ObjectLambdaWebhookConfig {
        object_lambda_test_config_with_policy(endpoint, &OutboundPolicy::default())
    }

    fn object_lambda_test_config_with_policy(endpoint: Url, policy: &OutboundPolicy) -> ObjectLambdaWebhookConfig {
        let outbound_resolver = policy
            .resolver_for(&endpoint)
            .expect("test endpoint should satisfy its outbound policy");
        ObjectLambdaWebhookConfig {
            endpoint,
            auth_token: String::new(),
            client_cert: String::new(),
            client_key: String::new(),
            client_ca: String::new(),
            skip_tls_verify: false,
            response_header_timeout: Some(Duration::from_secs(2)),
            outbound_resolver,
        }
    }

    #[derive(Clone, Default)]
    struct CapturedLog(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    struct CapturedLogWriter(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for CapturedLogWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("captured log lock").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for CapturedLog {
        type Writer = CapturedLogWriter;

        fn make_writer(&'writer self) -> Self::Writer {
            CapturedLogWriter(self.0.clone())
        }
    }

    impl CapturedLog {
        fn contents(&self) -> String {
            String::from_utf8(self.0.lock().expect("captured log lock").clone()).expect("captured logs must be UTF-8")
        }
    }

    struct StatusOperation(StatusCode);

    #[async_trait::async_trait]
    impl OwnedOperation for StatusOperation {
        async fn call(&self, _req: AdminRequest, _params: Params<'_, '_>) -> rustfs_s3_types::S3Result<AdminResponse> {
            Ok(AdminResponse::new(self.0, OwnedBody::empty()))
        }
    }

    struct IdentityOperation;

    #[async_trait::async_trait]
    impl OwnedOperation for IdentityOperation {
        async fn call(&self, req: AdminRequest, params: Params<'_, '_>) -> rustfs_s3_types::S3Result<AdminResponse> {
            let credentials = req
                .credentials()
                .ok_or_else(|| OwnedS3Error::with_message(OwnedS3ErrorCode::AccessDenied, "owned admin identity is required"))?;
            let identity_matches = credentials.access_key() == "AKIAOWNED"
                && credentials.secret_key_matches("owned-secret")
                && credentials
                    .session_token()
                    .is_some_and(|token| constant_time_eq(token, "owned-session"));
            if !identity_matches {
                return Err(OwnedS3Error::with_message(
                    OwnedS3ErrorCode::AccessDenied,
                    "owned admin identity is invalid",
                ));
            }
            if req.method() != Method::GET
                || req.uri().path() != "/__owned_admin/identity"
                || req.headers().get("x-owned-probe").and_then(|value| value.to_str().ok()) != Some("present")
                || req.region() != Some("us-east-2")
                || req.service() != Some("s3")
                || params.get("kind") != Some("identity")
            {
                return Err(OwnedS3Error::with_message(
                    OwnedS3ErrorCode::InvalidRequest,
                    "owned admin request facts changed at the router edge",
                ));
            }
            Ok(AdminResponse::new(StatusCode::NO_CONTENT, OwnedBody::empty()))
        }
    }

    struct EchoOperation;

    #[async_trait::async_trait]
    impl OwnedOperation for EchoOperation {
        async fn call(&self, req: AdminRequest, _params: Params<'_, '_>) -> rustfs_s3_types::S3Result<AdminResponse> {
            let mut headers = HeaderMap::new();
            headers.append("x-owned-repeat", HeaderValue::from_static("first"));
            headers.append("x-owned-repeat", HeaderValue::from_static("second"));
            Ok(AdminResponse::with_headers(StatusCode::IM_A_TEAPOT, req.into_body(), headers))
        }
    }

    static IDENTITY_OPERATION: IdentityOperation = IdentityOperation;
    static ECHO_OPERATION: EchoOperation = EchoOperation;

    fn owned_router(path: &str, operation: &'static dyn OwnedOperation) -> S3Router<AdminOperation> {
        let mut router = S3Router::new(false);
        router
            .insert(Method::GET, path, AdminOperation(operation))
            .expect("owned test route should register");
        router
    }

    fn router_request(method: Method, uri: &'static str) -> S3Request<Body> {
        S3Request {
            input: Body::empty(),
            method,
            uri: uri.parse().expect("uri should parse"),
            headers: HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    #[tokio::test]
    async fn legacy_admin_adapter_rejects_missing_identity_and_preserves_supplied_identity() {
        let router = owned_router("/__owned_admin/{kind}", &IDENTITY_OPERATION);

        let error = router
            .call(router_request(Method::GET, "/__owned_admin/identity"))
            .await
            .expect_err("missing credentials must stay denied");
        assert_eq!(error.code(), &S3ErrorCode::AccessDenied);

        let mut request = router_request(Method::GET, "/__owned_admin/identity");
        request.headers.insert("x-owned-probe", HeaderValue::from_static("present"));
        request
            .headers
            .insert("x-amz-security-token", HeaderValue::from_static("owned-session"));
        request.credentials = Some(s3s::auth::Credentials {
            access_key: "AKIAOWNED".to_owned(),
            secret_key: "owned-secret".into(),
        });
        request.region = Some("us-east-2".parse().expect("valid region"));
        request.service = Some("s3".to_owned());

        let response = router
            .call(request)
            .await
            .expect("supplied identity must reach the owned operation");
        assert_eq!(response.status, Some(StatusCode::NO_CONTENT));
    }

    #[tokio::test]
    async fn legacy_admin_adapter_preserves_status_repeated_headers_raw_body_and_size_hint() {
        let router = owned_router("/__owned_admin/echo", &ECHO_OPERATION);
        let raw = Bytes::from_static(&[0x00, 0x7f, 0x80, 0xff]);
        let mut request = router_request(Method::GET, "/__owned_admin/echo");
        request.input = Body::from(raw.clone());

        let response = router
            .call(request)
            .await
            .expect("owned response should cross the router edge");

        assert_eq!(response.status, Some(StatusCode::IM_A_TEAPOT));
        assert_eq!(http_body::Body::size_hint(&response.output).exact(), Some(raw.len() as u64));
        let repeated = response
            .headers
            .get_all("x-owned-repeat")
            .iter()
            .map(|value| value.to_str().expect("static header"))
            .collect::<Vec<_>>();
        assert_eq!(repeated, ["first", "second"]);
        assert_eq!(
            http_body_util::BodyExt::collect(response.output)
                .await
                .expect("raw body should read")
                .to_bytes(),
            raw
        );
    }

    #[tokio::test]
    async fn legacy_admin_adapter_preserves_body_errors() {
        let router = owned_router("/__owned_admin/echo", &ECHO_OPERATION);
        let stream = futures::stream::iter(vec![
            Ok::<Bytes, io::Error>(Bytes::from_static(b"before-error")),
            Err(io::Error::other("owned body sentinel")),
        ]);
        let mut request = router_request(Method::GET, "/__owned_admin/echo");
        request.input = OwnedBody::from_stream(stream).into();

        let response = router
            .call(request)
            .await
            .expect("owned response should cross the router edge");
        let error = http_body_util::BodyExt::collect(response.output)
            .await
            .expect_err("the body error must remain observable");

        assert!(error.to_string().contains("owned body sentinel"), "{error}");
    }

    #[test]
    fn canonicalize_admin_path_maps_compat_prefix_to_rustfs_prefix() {
        assert_eq!(canonicalize_admin_path("/minio/admin/v3/info").as_ref(), "/rustfs/admin/v3/info");
        assert_eq!(canonicalize_admin_path("/minio/admin").as_ref(), "/rustfs/admin");
        assert_eq!(canonicalize_admin_path("/rustfs/admin/v3/info").as_ref(), "/rustfs/admin/v3/info");
        assert_eq!(
            canonicalize_admin_path("/minio/administrator/object").as_ref(),
            "/minio/administrator/object"
        );
        assert_eq!(canonicalize_admin_path("/minio/adminx/object").as_ref(), "/minio/adminx/object");
    }

    #[test]
    fn admin_v4_fallback_path_matches_only_admin_v4_prefixes() {
        assert!(is_admin_v4_fallback_path("/rustfs/admin/v4/info-canned-policy"));
        assert!(is_admin_v4_fallback_path("/minio/admin/v4/add-canned-policy"));
        assert!(is_admin_v4_fallback_path("/rustfs/admin/v4"));
        assert!(!is_admin_v4_fallback_path("/rustfs/admin/v3/info-canned-policy"));
        assert!(!is_admin_v4_fallback_path("/minio/admin/v3/info-canned-policy"));
        assert!(!is_admin_v4_fallback_path("/rustfs/admin/v40/info-canned-policy"));
        assert!(!is_admin_v4_fallback_path("/minio/adminx/v4/info-canned-policy"));
    }

    #[tokio::test]
    async fn unmatched_admin_v4_request_returns_upgrade_required_for_sdk_downgrade() {
        let router: S3Router<StatusOperation> = S3Router::new(false);

        for (method, uri) in [
            (Method::GET, "/minio/admin/v4/info-canned-policy?name=readwrite"),
            (Method::PUT, "/rustfs/admin/v4/add-canned-policy?name=repro"),
        ] {
            let resp = router
                .call(router_request(method, uri))
                .await
                .expect("unmatched v4 admin request should return downgrade signal");

            assert_eq!(resp.status, Some(StatusCode::UPGRADE_REQUIRED), "{uri}");
        }
    }

    #[tokio::test]
    async fn unmatched_non_v4_admin_request_keeps_not_implemented_error() {
        let router: S3Router<StatusOperation> = S3Router::new(false);

        let err = router
            .call(router_request(Method::GET, "/rustfs/admin/v3/missing-route"))
            .await
            .expect_err("unknown v3 admin route must keep the existing error");

        assert_eq!(err.code(), &S3ErrorCode::NotImplemented);
    }

    #[tokio::test]
    async fn registered_admin_v4_route_is_not_shadowed_by_fallback() {
        let mut router: S3Router<StatusOperation> = S3Router::new(false);
        router
            .insert(
                Method::GET,
                "/rustfs/admin/v4/runtime/capabilities",
                StatusOperation(StatusCode::IM_A_TEAPOT),
            )
            .expect("route should insert");

        let resp = router
            .call(router_request(Method::GET, "/rustfs/admin/v4/runtime/capabilities"))
            .await
            .expect("registered v4 route must dispatch normally");

        assert_eq!(resp.status, Some(StatusCode::IM_A_TEAPOT));
    }

    #[test]
    fn is_admin_path_accepts_rustfs_and_compat_prefixes() {
        assert!(is_admin_path("/rustfs/admin/v3/info"));
        assert!(is_admin_path("/minio/admin/v3/info"));
        assert!(is_admin_path(&format!("{}/config", crate::server::TABLE_CATALOG_PREFIX)));
        assert!(is_admin_path("/_iceberg/v1/config"));
        assert!(!is_admin_path("/bucket/object"));
        assert!(!is_admin_path("/rustfs/administrator/object"));
        assert!(!is_admin_path("/minio/administrator/object"));
        assert!(!is_admin_path("/rustfs/adminx/object"));
        assert!(!is_admin_path("/minio/adminx/object"));
    }

    #[test]
    fn iceberg_routes_select_double_encoded_sigv4() {
        let router: S3Router<StatusOperation> = S3Router::new(false);
        for path in [
            "/iceberg/v1",
            "/iceberg/v1/warehouse/namespaces/ods%1Forders/tables/files",
            "/_iceberg/v1",
            "/_iceberg/v1/warehouse/namespaces/ods%1Forders/tables/files",
        ] {
            let mut extensions = Extensions::new();
            assert!(router.is_match(&Method::GET, &path.parse().unwrap(), &HeaderMap::new(), &mut extensions));
            assert_eq!(
                extensions.get::<s3s::auth::SigV4PathEncoding>(),
                Some(&s3s::auth::SigV4PathEncoding::DoubleEncoded),
                "{path}"
            );
        }
        for path in [
            "/bucket/name%20space",
            "/rustfs/admin/v3/info",
            "/minio/admin/v3/info",
            "/health",
            "/iceberg/v10/config",
            "/_iceberg/v10/config",
            "/iceberg/v1suffix/config",
        ] {
            let mut extensions = Extensions::new();
            router.is_match(&Method::GET, &path.parse().unwrap(), &HeaderMap::new(), &mut extensions);
            assert!(extensions.get::<s3s::auth::SigV4PathEncoding>().is_none(), "{path}");
        }
    }

    #[tokio::test]
    async fn iceberg_metadata_probe_passes_sigv4_verification() {
        use s3s::auth::SimpleAuth;
        use s3s::config::{S3Config, StaticConfigProvider};
        use s3s::service::S3ServiceBuilder;

        struct TestS3;
        #[async_trait::async_trait]
        impl s3s::S3 for TestS3 {}

        let mut router = S3Router::new(false);
        router
            .insert(
                Method::GET,
                "/iceberg/v1/warehouse/namespaces/{namespace}/tables/{table}",
                StatusOperation(StatusCode::NOT_FOUND),
            )
            .unwrap();
        let mut builder = S3ServiceBuilder::new(TestS3);
        builder.set_route(router);
        builder.set_auth(SimpleAuth::from_single("catalog-test-access", "catalog-test-secret"));
        let mut config = S3Config::default();
        config.presigned_url_max_skew_time_secs = u32::MAX;
        builder.set_config(Arc::new(StaticConfigProvider::new(Arc::new(config))));
        let service = builder.build();

        // Captured from botocore SigV4Auth, independently of the server signer.
        let request = http::Request::builder()
            .method(Method::GET)
            .uri("/iceberg/v1/warehouse/namespaces/ods%1Fkfk_log_order/tables/files")
            .header("host", "catalog.example:9000")
            .header("content-type", "application/json")
            .header("x-amz-date", "20200101T000000Z")
            .header("x-amz-content-sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
            .header(
                "authorization",
                concat!(
                    "AWS4-HMAC-SHA256 Credential=catalog-test-access/20200101/us-east-1/s3/aws4_request, ",
                    "SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, ",
                    "Signature=38357111b8355d0efdc6746cc5e94930c455d2f6741ea38c65e6556fe45d882d"
                ),
            )
            .body(Body::empty())
            .unwrap();
        let response = service.call(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn parse_object_lambda_request_names_bucket_and_key() {
        let object_lambda: Uri = "/demo-bucket/path/to/object.txt?lambdaArn=arn%3Atarget"
            .parse()
            .expect("uri should parse");

        assert_eq!(
            parse_object_lambda_request(&Method::GET, &object_lambda),
            Some(("demo-bucket".to_string(), "path/to/object.txt".to_string()))
        );
    }

    #[test]
    fn s3_route_claims_listen_notification_before_generated_s3_routes() {
        let router: S3Router<StatusOperation> = S3Router::new(false);
        let headers = HeaderMap::new();

        for uri in [
            "/?events=s3:ObjectRemoved:*&x-id=ListenNotification",
            "/demo-bucket?events=s3:ObjectCreated:*&x-id=ListenBucketNotification",
            "/demo-bucket/?events=s3:ObjectCreated:*&events=s3:ObjectRemoved:Delete",
        ] {
            let uri: Uri = uri.parse().expect("uri should parse");
            let mut extensions = http::Extensions::new();
            assert!(
                router.is_match(&Method::GET, &uri, &headers, &mut extensions),
                "listen notification custom route must claim {uri}"
            );
        }
    }

    #[test]
    fn parse_object_lambda_request_rejects_invalid_paths_or_methods() {
        let bucket_without_object: Uri = "/demo-bucket?lambdaArn=arn%3Atarget".parse().expect("uri should parse");
        let wrong_method_lambda: Uri = "/demo-bucket/object?lambdaArn=arn%3Atarget"
            .parse()
            .expect("uri should parse");
        let listen: Uri = "/demo-bucket/object?events=s3:ObjectCreated:*"
            .parse()
            .expect("uri should parse");

        assert!(parse_object_lambda_request(&Method::GET, &bucket_without_object).is_none());
        assert!(parse_object_lambda_request(&Method::PUT, &wrong_method_lambda).is_none());
        assert!(parse_object_lambda_request(&Method::GET, &listen).is_none());
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
        let config = rustfs_config::server_config::Config(std::collections::HashMap::from([(
            LAMBDA_WEBHOOK_SUB_SYS.to_string(),
            std::collections::HashMap::from([(
                "transformer".to_string(),
                rustfs_config::server_config::KVS(vec![
                    rustfs_config::server_config::KV {
                        key: ENABLE_KEY.to_string(),
                        value: "on".to_string(),
                        hidden_if_empty: false,
                    },
                    rustfs_config::server_config::KV {
                        key: WEBHOOK_ENDPOINT.to_string(),
                        value: "https://example.com/transform".to_string(),
                        hidden_if_empty: false,
                    },
                    rustfs_config::server_config::KV {
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
        let config = rustfs_config::server_config::Config(std::collections::HashMap::from([(
            LAMBDA_WEBHOOK_SUB_SYS.to_string(),
            std::collections::HashMap::from([(
                "transformer".to_string(),
                rustfs_config::server_config::KVS(vec![
                    rustfs_config::server_config::KV {
                        key: ENABLE_KEY.to_string(),
                        value: "on".to_string(),
                        hidden_if_empty: false,
                    },
                    rustfs_config::server_config::KV {
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
        let config = rustfs_config::server_config::Config(std::collections::HashMap::from([(
            LAMBDA_WEBHOOK_SUB_SYS.to_string(),
            std::collections::HashMap::from([(
                "transformer".to_string(),
                rustfs_config::server_config::KVS(vec![
                    rustfs_config::server_config::KV {
                        key: ENABLE_KEY.to_string(),
                        value: "on".to_string(),
                        hidden_if_empty: false,
                    },
                    rustfs_config::server_config::KV {
                        key: WEBHOOK_ENDPOINT.to_string(),
                        value: "https://example.com/transform".to_string(),
                        hidden_if_empty: false,
                    },
                    rustfs_config::server_config::KV {
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
        let config = rustfs_config::server_config::Config(std::collections::HashMap::from([(
            NOTIFY_WEBHOOK_SUB_SYS.to_string(),
            std::collections::HashMap::from([(
                "transformer".to_string(),
                rustfs_config::server_config::KVS(vec![
                    rustfs_config::server_config::KV {
                        key: ENABLE_KEY.to_string(),
                        value: "on".to_string(),
                        hidden_if_empty: false,
                    },
                    rustfs_config::server_config::KV {
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
        let config = rustfs_config::server_config::Config(std::collections::HashMap::from([(
            LAMBDA_WEBHOOK_SUB_SYS.to_string(),
            std::collections::HashMap::from([(
                "transformer".to_string(),
                rustfs_config::server_config::KVS(vec![
                    rustfs_config::server_config::KV {
                        key: ENABLE_KEY.to_string(),
                        value: "on".to_string(),
                        hidden_if_empty: false,
                    },
                    rustfs_config::server_config::KV {
                        key: WEBHOOK_ENDPOINT.to_string(),
                        value: "https://example.com/transform".to_string(),
                        hidden_if_empty: false,
                    },
                    rustfs_config::server_config::KV {
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
        let empty_config = rustfs_config::server_config::Config(std::collections::HashMap::new());
        let unsupported_err = resolve_object_lambda_webhook_config_from_server_config(&empty_config, &unsupported)
            .expect_err("unsupported target type should fail");
        assert_eq!(unsupported_err.code(), &S3ErrorCode::NotImplemented);

        let webhook = "arn:acme:s3-object-lambda::transformer:webhook"
            .parse::<rustfs_targets::arn::ARN>()
            .expect("arn should parse");
        let disabled_config = rustfs_config::server_config::Config(std::collections::HashMap::from([(
            LAMBDA_WEBHOOK_SUB_SYS.to_string(),
            std::collections::HashMap::from([(
                "transformer".to_string(),
                rustfs_config::server_config::KVS(vec![
                    rustfs_config::server_config::KV {
                        key: ENABLE_KEY.to_string(),
                        value: "off".to_string(),
                        hidden_if_empty: false,
                    },
                    rustfs_config::server_config::KV {
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
    fn resolve_object_lambda_webhook_config_from_server_config_rejects_loopback_endpoint() {
        let arn = "arn:acme:s3-object-lambda::transformer:webhook"
            .parse::<rustfs_targets::arn::ARN>()
            .expect("arn should parse");
        let config = rustfs_config::server_config::Config(std::collections::HashMap::from([(
            LAMBDA_WEBHOOK_SUB_SYS.to_string(),
            std::collections::HashMap::from([(
                "transformer".to_string(),
                rustfs_config::server_config::KVS(vec![
                    rustfs_config::server_config::KV {
                        key: ENABLE_KEY.to_string(),
                        value: "on".to_string(),
                        hidden_if_empty: false,
                    },
                    rustfs_config::server_config::KV {
                        key: WEBHOOK_ENDPOINT.to_string(),
                        value: "https://127.0.0.1/transform".to_string(),
                        hidden_if_empty: false,
                    },
                ]),
            )]),
        )]));

        let err = resolve_object_lambda_webhook_config_from_server_config(&config, &arn)
            .expect_err("loopback endpoint should be rejected");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert!(err.message().unwrap_or_default().contains("not allowed"));
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
            region: current_region(),
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

        for (route, token) in [
            ("Route-123", "token-456"),
            ("route-124", "token-456"),
            ("route-1234", "token-456"),
            ("route-123", "Token-456"),
            ("route-123", "token-457"),
            ("route-123", "token-4567"),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(
                "x-amz-request-route",
                HeaderValue::try_from(route).expect("test route must be a valid header"),
            );
            headers.insert(
                "x-amz-request-token",
                HeaderValue::try_from(token).expect("test token must be a valid header"),
            );
            assert!(
                validate_object_lambda_response_auth_headers(&headers, "route-123", "token-456").is_err(),
                "first-byte, last-byte, and length mismatches must fail: {route}/{token}"
            );
        }
    }

    #[test]
    fn object_lambda_auth_headers_use_constant_time_helper() {
        let source = include_str!("router.rs");
        let production = source.split_once("#[cfg(test)]").map_or(source, |(production, _)| production);
        assert!(!production.contains("route == Some(output_route)"));
        assert!(!production.contains("token == Some(output_token)"));
        assert!(production.contains("constant_time_eq(route, output_route)"));
        assert!(production.contains("constant_time_eq(token, output_token)"));
    }

    #[test]
    fn object_lambda_client_ignores_proxies_and_does_not_follow_redirects() {
        const CHILD_ENV: &str = "RUSTFS_TEST_OBJECT_LAMBDA_PROXY_CHILD";
        const TARGET_URL_ENV: &str = "RUSTFS_TEST_OBJECT_LAMBDA_TARGET_URL";
        const TARGET_ADDR_ENV: &str = "RUSTFS_TEST_OBJECT_LAMBDA_TARGET_ADDR";

        if std::env::var_os(CHILD_ENV).is_some() {
            let endpoint = Url::parse(&std::env::var(TARGET_URL_ENV).expect("child target URL")).expect("target URL");
            let address = std::env::var(TARGET_ADDR_ENV)
                .expect("child target address")
                .parse::<SocketAddr>()
                .expect("target address");
            let config = object_lambda_test_config(endpoint);
            let client = build_object_lambda_http_client_with_resolver(&config, StaticResolver(address.ip()))
                .expect("object lambda client should build");
            tokio::runtime::Runtime::new().expect("child runtime").block_on(async {
                let response = client
                    .get(config.endpoint)
                    .send()
                    .await
                    .expect("object lambda request should bypass environment proxy");
                assert_eq!(response.status(), reqwest::StatusCode::FOUND);
            });
            return;
        }

        use std::io::{Read, Write};
        use std::net::TcpListener as StdTcpListener;

        let target_listener = StdTcpListener::bind("127.0.0.1:0").expect("bind object lambda target");
        let target_address = target_listener.local_addr().expect("object lambda target address");
        let redirect_listener = StdTcpListener::bind("127.0.0.1:0").expect("bind redirect destination");
        let redirect_address = redirect_listener.local_addr().expect("redirect destination address");
        drop(redirect_listener);
        let target = std::thread::spawn(move || {
            let (mut stream, _) = target_listener.accept().expect("accept object lambda request");
            let mut request = [0_u8; 1024];
            let read = stream.read(&mut request).expect("read object lambda request");
            assert!(String::from_utf8_lossy(&request[..read]).starts_with("GET /transform HTTP/1.1"));
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:{}/metadata\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        redirect_address.port()
                    )
                    .as_bytes(),
                )
                .expect("write object lambda redirect");
        });
        let proxy_listener = StdTcpListener::bind("127.0.0.1:0").expect("reserve refused proxy address");
        let proxy_address = proxy_listener.local_addr().expect("proxy address");
        drop(proxy_listener);
        let target_url = format!("http://object-lambda.test:{}/transform", target_address.port());
        let proxy_url = format!("http://{proxy_address}");
        let output = std::process::Command::new(std::env::current_exe().expect("resolve current test executable"))
            .arg("object_lambda_client_ignores_proxies_and_does_not_follow_redirects")
            .arg("--nocapture")
            .env(CHILD_ENV, "1")
            .env(TARGET_URL_ENV, target_url)
            .env(TARGET_ADDR_ENV, target_address.to_string())
            .env("HTTP_PROXY", &proxy_url)
            .env("HTTPS_PROXY", &proxy_url)
            .env("ALL_PROXY", &proxy_url)
            .env("NO_PROXY", "")
            .output()
            .expect("run isolated object lambda proxy test child");

        assert!(
            output.status.success(),
            "proxy test child failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        target.join().expect("object lambda target should finish");
    }

    #[tokio::test]
    async fn object_lambda_client_uses_configured_outbound_policy_resolver() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind object lambda policy target");
        let address = listener.local_addr().expect("object lambda policy target address");
        let endpoint = Url::parse(&format!("http://{address}/transform")).expect("object lambda endpoint");
        let policy = OutboundPolicy::from_allowed_origins(&endpoint.origin().ascii_serialization())
            .expect("loopback test origin should be explicitly allowed");
        let config = object_lambda_test_config_with_policy(endpoint.clone(), &policy);
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept object lambda policy request");
            let mut request = [0_u8; 1024];
            let read = stream.read(&mut request).await.expect("read object lambda policy request");
            assert!(String::from_utf8_lossy(&request[..read]).starts_with("GET /transform HTTP/1.1"));
            stream
                .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .expect("write object lambda policy response");
        });

        let response = build_object_lambda_http_client(&config)
            .expect("object lambda policy client should build")
            .get(endpoint)
            .send()
            .await
            .expect("configured policy resolver should reach the allowed origin");
        assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);
        server.await.expect("object lambda policy target should finish");
    }

    #[tokio::test]
    async fn object_lambda_client_preserves_hostname_for_tls_sni() {
        use rustls::{
            ServerConfig, ServerConnection, StreamOwned,
            pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer},
        };
        use std::io::{Read, Write};
        use std::sync::{Arc, Once};

        static INSTALL_CRYPTO_PROVIDER: Once = Once::new();
        INSTALL_CRYPTO_PROVIDER.call_once(|| {
            let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        });
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["object-lambda.test".to_string()]).expect("cert should generate");
        let temp_dir = tempfile::tempdir().expect("create temp directory");
        let ca_path = temp_dir.path().join("object-lambda-ca.pem");
        std::fs::write(&ca_path, cert.pem()).expect("write object lambda CA");
        let server_config = Arc::new(
            ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(
                    vec![cert.der().clone()],
                    PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(signing_key.serialize_der())),
                )
                .expect("server certificate should be valid"),
        );
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind object lambda TLS server");
        let address = listener.local_addr().expect("object lambda TLS address");
        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept object lambda TLS request");
            let connection = ServerConnection::new(server_config).expect("server TLS connection");
            let mut stream = StreamOwned::new(connection, stream);
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request).expect("read object lambda TLS request");
            assert_eq!(stream.conn.server_name(), Some("object-lambda.test"));
            stream
                .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .expect("write object lambda TLS response");
        });
        let endpoint =
            Url::parse(&format!("https://object-lambda.test:{}/transform", address.port())).expect("object lambda TLS endpoint");
        let mut config = object_lambda_test_config(endpoint.clone());
        config.client_ca = ca_path.to_string_lossy().into_owned();
        // This is the only object-lambda test that performs a real TLS
        // handshake; under a full-suite nextest run the CPU contention from
        // neighboring tests pushes it past the helper's tight 2s request
        // deadline. SNI preservation, not latency, is under test here.
        config.response_header_timeout = Some(Duration::from_secs(30));

        let response = build_object_lambda_http_client_with_resolver(&config, StaticResolver(address.ip()))
            .expect("object lambda TLS client should build")
            .get(endpoint)
            .send()
            .await
            .expect("TLS request should keep the configured hostname for SNI");
        assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);
        server.join().expect("object lambda TLS server should finish");
    }

    #[test]
    fn object_lambda_tls_warning_redacts_endpoint_details() {
        let captured = CapturedLog::default();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_max_level(tracing::Level::WARN)
            .with_writer(captured.clone())
            .finish();
        let mut config = object_lambda_test_config(
            Url::parse("https://object-lambda.test/private?token=secret").expect("object lambda endpoint"),
        );
        config.skip_tls_verify = true;

        tracing::subscriber::with_default(subscriber, || {
            build_object_lambda_http_client_with_resolver(&config, StaticResolver(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)))
                .expect("object lambda client should build");
        });

        let logs = captured.contents();
        assert!(logs.contains("https://object-lambda.test"));
        for secret in ["/private", "token=secret"] {
            assert!(!logs.contains(secret), "TLS warning leaked {secret}: {logs}");
        }
    }

    #[tokio::test]
    async fn object_lambda_request_error_does_not_expose_endpoint_details() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("reserve refused endpoint");
        let address = listener.local_addr().expect("refused endpoint address");
        drop(listener);
        let request = reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("test client should build")
            .get(format!("http://{address}/private?token=secret"));

        let error = send_object_lambda_request(request)
            .await
            .expect_err("refused object lambda request should fail");
        let rendered = error.to_string();
        assert!(rendered.contains("object lambda target request failed"));
        assert!(!rendered.contains("/private"));
        assert!(!rendered.contains("token=secret"));
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
    async fn custom_routes_reject_signature_v2_arguments() {
        let router: S3Router<AdminOperation> = S3Router::new(false);
        for uri in [
            "/rustfs/admin/v3/add-user?accessKey=target",
            "/minio/admin/v3/set-user-or-group-policy?userOrGroup=target",
            "/?Action=AssumeRole",
            "/bucket?replication-metrics",
            "/iceberg/v1/config",
        ] {
            for presigned in [false, true] {
                let method = if uri.contains("replication-metrics") {
                    Method::GET
                } else {
                    Method::POST
                };
                let mut req = router_request(method, uri);
                req.credentials = Some(s3s::auth::Credentials {
                    access_key: "test".into(),
                    secret_key: "secret".into(),
                });
                req.headers
                    .insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/x-www-form-urlencoded"));
                if presigned {
                    let separator = if uri.contains('?') { '&' } else { '?' };
                    req.uri = format!("{uri}{separator}%53ignature=captured&AWSAccessKeyId=test&Expires=1790000000")
                        .parse()
                        .unwrap();
                } else {
                    req.headers
                        .insert(http::header::AUTHORIZATION, HeaderValue::from_static("AWS test:captured"));
                }
                let error = router
                    .check_access(&mut req)
                    .await
                    .expect_err("V2 custom arguments are not signed");
                assert_eq!(error.code(), &S3ErrorCode::AccessDenied, "{uri}");
                assert_eq!(
                    error.message(),
                    Some("Header or query Signature Version 4 is required for this operation")
                );
            }
        }
    }

    #[tokio::test]
    async fn custom_routes_preserve_verified_sigv4_access() {
        let router: S3Router<AdminOperation> = S3Router::new(false);
        for uri in ["/rustfs/admin/v3/info", "/bucket?replication-metrics", "/?Action=AssumeRole"] {
            let mut req = router_request(Method::GET, uri);
            req.credentials = Some(s3s::auth::Credentials {
                access_key: "test".into(),
                secret_key: "secret".into(),
            });
            req.headers.insert(http::header::AUTHORIZATION, HeaderValue::from_static(
                "AWS4-HMAC-SHA256 Credential=test/20260827/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=0000000000000000000000000000000000000000000000000000000000000000"
            ));
            router
                .check_access(&mut req)
                .await
                .expect("verified V4 requests remain accepted");
        }
    }

    #[tokio::test]
    async fn custom_routes_reject_post_policy_authentication() {
        let router: S3Router<AdminOperation> = S3Router::new(false);
        for content_type in ["multipart/form-data; boundary=upload", "Multipart/Form-Data; boundary=upload"] {
            let mut req = router_request(Method::POST, "/rustfs/admin/v3/service?action=stop");
            req.credentials = Some(s3s::auth::Credentials {
                access_key: "test".into(),
                secret_key: "secret".into(),
            });
            req.headers
                .insert(http::header::CONTENT_TYPE, HeaderValue::from_static(content_type));
            let error = router
                .check_access(&mut req)
                .await
                .expect_err("upload form signatures do not authorize admin routes");
            assert_eq!(error.code(), &S3ErrorCode::AccessDenied);
        }
    }

    /// GHSA-g8w9-qw9q-fghr: custom routes must apply the presigned
    /// signed-header rule too, since they never reach `S3Access::check`.
    #[tokio::test]
    async fn ghsa_g8w9_check_access_rejects_unsigned_amz_header_on_presigned_custom_route() {
        let router: S3Router<AdminOperation> = S3Router::new(false);
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-tagging", HeaderValue::from_static("owner=attacker"));
        let mut req = S3Request {
            input: Body::from(String::new()),
            method: Method::GET,
            uri: "/demo-bucket?replication-metrics&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20260827T000000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Credential=test%2F20260827%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=signature"
                .parse()
                .expect("uri should parse"),
            headers,
            extensions: http::Extensions::new(),
            credentials: Some(s3s::auth::Credentials {
                access_key: "test".into(),
                secret_key: s3s::auth::SecretKey::from("secret".to_string()),
            }),
            region: None,
            service: None,
            trailing_headers: None,
        };

        let err = router
            .check_access(&mut req)
            .await
            .expect_err("presigned custom-route request with an unsigned x-amz header must be denied");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
        assert_eq!(err.message(), Some(crate::auth::UNSIGNED_HEADERS_MESSAGE));
    }

    /// GHSA-xm99-m3gq-83g8: custom routes must apply the header-signed SigV4
    /// signed-header rule too, since they never reach `S3Access::check`.
    #[tokio::test]
    async fn ghsa_xm99_check_access_rejects_unsigned_amz_header_on_header_signed_custom_route() {
        let router: S3Router<AdminOperation> = S3Router::new(false);
        let mut headers = HeaderMap::new();
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential=test/20260827/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={}",
            "0".repeat(64)
        );
        headers.insert("authorization", authorization.parse().expect("authorization"));
        headers.insert("x-amz-date", HeaderValue::from_static("20260827T000000Z"));
        headers.insert("x-amz-content-sha256", HeaderValue::from_static("UNSIGNED-PAYLOAD"));
        headers.insert("x-amz-tagging", HeaderValue::from_static("owner=attacker"));
        let mut req = S3Request {
            input: Body::from(String::new()),
            method: Method::GET,
            uri: "/demo-bucket?replication-metrics".parse().expect("uri should parse"),
            headers,
            extensions: http::Extensions::new(),
            credentials: Some(s3s::auth::Credentials {
                access_key: "test".into(),
                secret_key: s3s::auth::SecretKey::from("secret".to_string()),
            }),
            region: None,
            service: None,
            trailing_headers: None,
        };

        let err = router
            .check_access(&mut req)
            .await
            .expect_err("header-signed custom-route request with an unsigned x-amz header must be denied");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
        assert_eq!(err.message(), Some(crate::auth::UNSIGNED_HEADERS_MESSAGE));
    }

    // backlog#1052 S2: the router hands its server's context slot to every
    // dispatched request via extensions, so the static admin operations can
    // resolve their server's store instead of the process default.
    #[tokio::test]
    async fn check_access_injects_server_context_slot_into_request_extensions() {
        let mut router: S3Router<AdminOperation> = S3Router::new(false);
        let server_ctx = ServerContextSlot::new();
        router.set_server_ctx(server_ctx.clone());

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

        let _ = router.check_access(&mut req).await;

        let injected = req
            .extensions
            .get::<Arc<ServerContextSlot>>()
            .expect("dispatch must inject the server context slot");
        assert!(Arc::ptr_eq(injected, &server_ctx), "the injected slot must be this router's slot");
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

    #[tokio::test]
    async fn check_access_rejects_anonymous_profile_request() {
        let router: S3Router<AdminOperation> = S3Router::new(false);
        let mut req = S3Request {
            input: Body::from(String::new()),
            method: Method::GET,
            uri: PROFILE_CPU_PATH.parse().expect("uri should parse"),
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
            .expect_err("anonymous profile request must be denied");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }

    #[tokio::test]
    async fn check_access_limits_anonymous_sts_exemption_to_form_content_type() {
        let router: S3Router<AdminOperation> = S3Router::new(false);
        let request = |content_type: Option<&'static str>| {
            let mut headers = HeaderMap::new();
            if let Some(content_type) = content_type {
                headers.insert(http::header::CONTENT_TYPE, HeaderValue::from_static(content_type));
            }
            S3Request {
                input: Body::from(String::from("Action=AssumeRoleWithWebIdentity")),
                method: Method::POST,
                uri: Uri::from_static("/"),
                headers,
                extensions: http::Extensions::new(),
                credentials: None,
                region: None,
                service: None,
                trailing_headers: None,
            }
        };

        for content_type in [None, Some("application/json")] {
            let mut req = request(content_type);
            let err = router
                .check_access(&mut req)
                .await
                .expect_err("anonymous STS request without form content type must be denied");
            assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
        }

        let mut req = request(Some("application/x-www-form-urlencoded"));
        router
            .check_access(&mut req)
            .await
            .expect("anonymous form-encoded STS request should reach identity validation");
    }

    #[tokio::test]
    async fn check_access_allows_object_zip_download_token_navigation() {
        let router: S3Router<AdminOperation> = S3Router::new(false);
        let mut req = S3Request {
            input: Body::from(String::new()),
            method: Method::GET,
            uri: "/rustfs/admin/v3/object-zip-downloads/example.zip?token=abc"
                .parse()
                .expect("uri should parse"),
            headers: HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };

        router
            .check_access(&mut req)
            .await
            .expect("token download navigation should reach the handler");
    }

    #[tokio::test]
    async fn check_access_rejects_object_zip_download_without_token() {
        let router: S3Router<AdminOperation> = S3Router::new(false);
        let mut req = S3Request {
            input: Body::from(String::new()),
            method: Method::GET,
            uri: "/rustfs/admin/v3/object-zip-downloads/example.zip"
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
            .expect_err("token download without token must be denied before handler");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }

    #[tokio::test]
    async fn check_access_rejects_anonymous_object_zip_download_post() {
        let router: S3Router<AdminOperation> = S3Router::new(false);
        let mut req = S3Request {
            input: Body::from(String::new()),
            method: Method::POST,
            uri: "/rustfs/admin/v3/object-zip-downloads?token=abc"
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
            .expect_err("token exception must not apply to POST");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }
}

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
