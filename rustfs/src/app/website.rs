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

//! Static website hosting: which requests a website host claims, and how they are answered.
//!
//! Responsible for: [`WebsiteHost`], the one copy of the website rules both S3 stacks serve —
//! the `Host` that selects a bucket, the index and error documents, the redirect rules and the
//! object's own redirect location — read anonymously through the owned GetObject and HeadObject
//! authorization and use cases, marking each GetObject with `WebsiteRead` so it reports the
//! object's redirect location.
//! NOT responsible for: where the stack mounts the host (`server/stack.rs`), or any object read
//! itself (the use cases).
//! Upstream: the gateway stack's website service. Downstream: the
//! owned GetObject and HeadObject authorization and use cases, and the bucket's website
//! configuration.

use crate::app::object_usecase::{DefaultObjectUsecase, GetObjectBody, GetObjectFields, GetObjectOutputFacts, HeadObjectFields};
use crate::app::runtime_sources::ServerContextSlot;
use crate::app::{Answered, RequestEnvelope, RequestHead};
use crate::storage::access_owned::object_read::{authorize_get_object, authorize_head_object};
use crate::storage::req_info::ReqInfo;
use crate::storage::request_markers::WebsiteRead;
use crate::storage::{get_bucket_website_config_for_store, validate_website_configuration};
use http::header::{CONTENT_TYPE, HOST, HeaderName};
use http::{Extensions, HeaderMap, HeaderValue, Method, Response, StatusCode, Uri};
use rustfs_gateway_stream::Body;
use rustfs_gateway_types::TimestampFormat;
use rustfs_gateway_types::persistence::{PersistedErrorDocument, PersistedRoutingRule as RoutingRule};
use rustfs_io_metrics::record_s3_op;
use rustfs_s3_ops::S3Operation;
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result};
use std::sync::Arc;

/// An answer of the website host.
pub(crate) type WebsiteResponse = Response<Body>;

/// The headers a website read drops before a lookup that must not be conditional or ranged.
const CONDITIONAL_HEADERS: [HeaderName; 5] = [
    http::header::RANGE,
    http::header::IF_MATCH,
    http::header::IF_NONE_MATCH,
    http::header::IF_MODIFIED_SINCE,
    http::header::IF_UNMODIFIED_SINCE,
];

/// The website host of one server: the configured website domains, the scheme its redirects
/// default to, and the server whose buckets it serves.
pub(crate) struct WebsiteHost {
    domains: Vec<String>,
    scheme: &'static str,
    server_ctx: Arc<ServerContextSlot>,
}

impl WebsiteHost {
    /// The host for `domains`, or `None` when no website domain is configured.
    pub(crate) fn new(domains: Vec<String>, scheme: &'static str, server_ctx: Arc<ServerContextSlot>) -> Option<Self> {
        (!domains.is_empty()).then_some(Self {
            domains,
            scheme,
            server_ctx,
        })
    }

    /// Whether the request's `Host` is a website domain, so this host answers it.
    pub(crate) fn claims(&self, uri: &Uri, headers: &HeaderMap) -> bool {
        website_target(uri, headers, &self.domains).is_some()
    }

    /// Answers one claimed request anonymously. Every failure is a website error page with the
    /// failure's status, never an S3 error document.
    pub(crate) async fn answer(&self, method: Method, uri: Uri, headers: HeaderMap, extensions: Extensions) -> WebsiteResponse {
        let head = method == Method::HEAD;
        let Some(target) = website_target(&uri, &headers, &self.domains) else {
            return website_error(StatusCode::NOT_FOUND, head);
        };
        let request = WebsiteRequest {
            method,
            uri,
            headers,
            extensions,
            server_ctx: Arc::clone(&self.server_ctx),
        };
        // The reads are large futures; boxed, a website request does not grow the caller's stack.
        match Box::pin(serve(&request, target, self.scheme)).await {
            Ok(response) => response,
            Err(err) => website_error(err.status_code().unwrap_or(StatusCode::INTERNAL_SERVER_ERROR), head),
        }
    }
}

#[derive(Clone, Debug)]
struct WebsiteTarget {
    bucket: String,
    key: String,
    trailing_slash: bool,
    invalid_path: bool,
}

fn website_target(uri: &Uri, headers: &HeaderMap, domains: &[String]) -> Option<WebsiteTarget> {
    if domains.is_empty() {
        return None;
    }
    let host_header = headers.get(HOST)?.to_str().ok()?;
    let authority = host_header.parse::<http::uri::Authority>().ok()?;
    let request_host = authority.host().trim_end_matches('.').to_ascii_lowercase();
    let bucket = domains.iter().find_map(|domain| {
        if request_host == *domain {
            return Some(String::new());
        }
        request_host.strip_suffix(&format!(".{domain}")).map(str::to_owned)
    })?;
    let raw_path = uri.path();
    let decoded = urlencoding::decode(raw_path);
    let invalid_path = decoded.is_err();
    let decoded = decoded.unwrap_or_default();
    let path = decoded.trim_start_matches('/');
    Some(WebsiteTarget {
        bucket,
        key: path.to_owned(),
        trailing_slash: path.is_empty() || path.ends_with('/'),
        invalid_path,
    })
}

/// One website request as the reads it makes see it.
struct WebsiteRequest {
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    extensions: Extensions,
    server_ctx: Arc<ServerContextSlot>,
}

impl WebsiteRequest {
    fn head(&self) -> bool {
        self.method == Method::HEAD
    }

    /// An anonymous read of `bucket/key`: no credentials and no owner, whatever the request carried.
    fn envelope(&self, method: Method, headers: HeaderMap, bucket: &str, key: &str, website_read: bool) -> RequestEnvelope {
        let mut extensions = self.extensions.clone();
        extensions.insert(Arc::clone(&self.server_ctx));
        let mut req_info = extensions.remove::<ReqInfo>().unwrap_or_default();
        req_info.cred = None;
        req_info.is_owner = false;
        req_info.bucket = Some(bucket.to_owned());
        req_info.object = Some(key.to_owned());
        req_info.version_id = None;
        extensions.insert(req_info);
        if website_read {
            extensions.insert(WebsiteRead);
        }
        let head = RequestHead {
            method,
            uri: self.uri.clone(),
            headers,
            credentials: None,
            region: None,
            service: None,
        };
        RequestEnvelope::from_parts(head, extensions)
    }

    fn usecase(&self) -> DefaultObjectUsecase {
        DefaultObjectUsecase::with_context(self.server_ctx.app_context())
    }

    /// GetObject of `bucket/key`, with the request's range and conditions when `apply_conditions`.
    async fn get_object(&self, bucket: &str, key: &str, apply_conditions: bool) -> S3Result<Answered<GetObjectOutputFacts>> {
        let (range, headers) = if apply_conditions {
            (validated_conditions(&self.headers)?, self.headers.clone())
        } else {
            (None, without_conditions(&self.headers))
        };
        let mut envelope = self.envelope(Method::GET, headers, bucket, key, true);
        authorize_get_object(&self.server_ctx, &mut envelope, bucket, key, None).await?;
        record_s3_op(S3Operation::GetObject);
        let fields = GetObjectFields {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            range,
            version_id: None,
            part_number: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
        };
        Box::pin(self.usecase().execute_get_object_fields(envelope, fields)).await
    }

    /// The redirect location HeadObject reports for `bucket/key`, read unconditionally.
    async fn head_redirect(&self, bucket: &str, key: &str) -> S3Result<Option<String>> {
        let mut envelope = self.envelope(Method::HEAD, without_conditions(&self.headers), bucket, key, false);
        authorize_head_object(&self.server_ctx, &mut envelope, bucket, key, None).await?;
        record_s3_op(S3Operation::HeadObject);
        let fields = HeadObjectFields {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            version_id: None,
            part_number: None,
            range: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
        };
        let answered = Box::pin(self.usecase().execute_head_object_fields(envelope, fields)).await?;
        Ok(answered.output.dto.website_redirect_location)
    }
}

fn header_text(headers: &HeaderMap, name: HeaderName) -> S3Result<Option<String>> {
    headers
        .get(name)
        .map(|value| {
            value
                .to_str()
                .map(str::to_owned)
                .map_err(|_| S3Error::new(S3ErrorCode::InvalidArgument))
        })
        .transpose()
}

/// The range a conditional read asks for, after refusing a malformed range or condition the
/// way the S3 GetObject decoder refuses it.
fn validated_conditions(headers: &HeaderMap) -> S3Result<Option<rustfs_gateway_types::ByteRange>> {
    let range = header_text(headers, http::header::RANGE)?
        .map(|value| GetObjectFields::parse_range(&value).ok_or_else(|| S3Error::new(S3ErrorCode::InvalidRange)))
        .transpose()?;
    for name in [http::header::IF_MATCH, http::header::IF_NONE_MATCH] {
        if headers
            .get(name)
            .is_some_and(|value| !GetObjectFields::valid_etag_condition(value.as_bytes()))
        {
            return Err(S3Error::new(S3ErrorCode::InvalidArgument));
        }
    }
    for name in [http::header::IF_MODIFIED_SINCE, http::header::IF_UNMODIFIED_SINCE] {
        if header_text(headers, name)?.is_some_and(|value| !GetObjectFields::valid_date_condition(&value)) {
            return Err(S3Error::new(S3ErrorCode::InvalidArgument));
        }
    }
    Ok(range)
}

fn without_conditions(headers: &HeaderMap) -> HeaderMap {
    let mut headers = headers.clone();
    for name in CONDITIONAL_HEADERS {
        headers.remove(name);
    }
    headers
}

async fn serve(req: &WebsiteRequest, target: WebsiteTarget, website_scheme: &'static str) -> S3Result<WebsiteResponse> {
    if !matches!(req.method, Method::GET | Method::HEAD) {
        return Ok(website_error(StatusCode::METHOD_NOT_ALLOWED, req.head()));
    }
    if target.invalid_path {
        return Ok(website_error(StatusCode::BAD_REQUEST, req.head()));
    }
    if target.bucket.is_empty() || target.bucket.starts_with('.') || target.bucket.ends_with('.') || target.bucket.contains("..")
    {
        return Ok(website_error(StatusCode::NOT_FOUND, req.head()));
    }

    let store = req
        .server_ctx
        .installed_object_store()
        .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "website object store unavailable"))?;
    let config = match get_bucket_website_config_for_store(&store, &target.bucket).await {
        Ok(config) => config,
        Err(crate::storage::StorageError::ConfigNotFound | crate::storage::StorageError::BucketNotFound(_)) => {
            return Ok(website_error(StatusCode::NOT_FOUND, req.head()));
        }
        Err(err) => return Err(crate::error::ApiError::from(err).into()),
    };
    if validate_website_configuration(&config).is_err() {
        return Ok(website_error(StatusCode::INTERNAL_SERVER_ERROR, req.head()));
    }

    if let Some(redirect) = config.redirect_all_requests_to.as_ref() {
        return website_redirect(
            &req.uri,
            &req.headers,
            redirect.host_name.as_str(),
            redirect.protocol.as_deref(),
            &target.key,
            StatusCode::MOVED_PERMANENTLY,
            website_scheme,
        );
    }

    let index_suffix = config
        .index_document
        .as_ref()
        .map(|doc| doc.suffix.as_str())
        .unwrap_or_default();
    let key = if target.key.is_empty() || target.trailing_slash {
        format!("{}{index_suffix}", target.key)
    } else {
        target.key.clone()
    };

    let rules = config.routing_rules.as_deref().unwrap_or_default();
    // A rule requiring a status cannot be evaluated until the object lookup completes.
    for rule in rules {
        if rule
            .condition
            .as_ref()
            .and_then(|condition| condition.http_error_code_returned_equals.as_ref())
            .is_some()
        {
            break;
        }
        if website_rule_matches(rule, &target.key, StatusCode::OK) {
            return website_rule_redirect(req, rule, &target.key, website_scheme);
        }
    }

    let response = req.get_object(&target.bucket, &key, true).await;
    if let Err(err) = &response
        && matches!(
            err.code(),
            S3ErrorCode::NotModified | S3ErrorCode::PreconditionFailed | S3ErrorCode::InvalidRange
        )
        && let Ok(Some(location)) = req.head_redirect(&target.bucket, &key).await
    {
        return website_location(location, StatusCode::MOVED_PERMANENTLY);
    }
    let status = match &response {
        Ok(_) => StatusCode::OK,
        Err(err) => err.status_code().unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
    };
    if status == StatusCode::NOT_FOUND && !target.trailing_slash && !target.key.is_empty() {
        let index_key = format!("{}/{index_suffix}", target.key);
        match req.get_object(&target.bucket, &index_key, false).await {
            Ok(_) => return website_relative_redirect(&req.uri, &format!("/{}/", target.key), StatusCode::FOUND),
            Err(err)
                if matches!(
                    err.code(),
                    S3ErrorCode::NoSuchKey | S3ErrorCode::NoSuchVersion | S3ErrorCode::AccessDenied
                ) => {}
            Err(err) => return Err(err),
        }
    }
    if let Some(rule) = rules.iter().find(|rule| website_rule_matches(rule, &target.key, status)) {
        return website_rule_redirect(req, rule, &target.key, website_scheme);
    }
    match response {
        Ok(response) => {
            if let Some(location) = response.output.dto.website_redirect_location.clone() {
                return website_location(location, StatusCode::MOVED_PERMANENTLY);
            }
            response_to_website(response, req.head())
        }
        Err(err) if matches!(err.code(), S3ErrorCode::NoSuchKey | S3ErrorCode::NoSuchVersion) => {
            website_error_with_document(req, &target.bucket, config.error_document.as_ref(), StatusCode::NOT_FOUND).await
        }
        Err(err) if err.code() == &S3ErrorCode::NotModified => Ok(website_not_modified(&err)),
        Err(_) if status.is_client_error() => {
            website_error_with_document(req, &target.bucket, config.error_document.as_ref(), status).await
        }
        Err(err) => Err(err),
    }
}

/// The object's page: its representation headers, and its body unless the request is `HEAD`.
fn response_to_website(response: Answered<GetObjectOutputFacts>, head: bool) -> S3Result<WebsiteResponse> {
    let Answered { output, mut headers, .. } = response;
    let GetObjectOutputFacts { dto, etag, body } = output;
    insert_website_header(&mut headers, http::header::CONTENT_TYPE, dto.content_type.as_deref())?;
    insert_website_header(
        &mut headers,
        http::header::CONTENT_LENGTH,
        dto.content_length.map(|length| length.to_string()).as_deref(),
    )?;
    insert_website_header(&mut headers, http::header::CONTENT_RANGE, dto.content_range.as_deref())?;
    let etag = etag.map(|tag| {
        if tag.weak {
            format!("W/\"{}\"", tag.opaque)
        } else {
            format!("\"{}\"", tag.opaque)
        }
    });
    insert_website_header(&mut headers, http::header::ETAG, etag.as_deref())?;
    insert_website_header(&mut headers, http::header::CACHE_CONTROL, dto.cache_control.as_deref())?;
    insert_website_header(&mut headers, http::header::CONTENT_ENCODING, dto.content_encoding.as_deref())?;
    insert_website_header(&mut headers, http::header::CONTENT_LANGUAGE, dto.content_language.as_deref())?;
    insert_website_header(&mut headers, http::header::CONTENT_DISPOSITION, dto.content_disposition.as_deref())?;
    let last_modified = dto
        .last_modified
        .map(|at| at.render(TimestampFormat::HttpDate).map_err(S3Error::internal_error))
        .transpose()?;
    insert_website_header(&mut headers, http::header::LAST_MODIFIED, last_modified.as_deref())?;
    let body = match body {
        _ if head => Body::empty(),
        None => Body::empty(),
        Some(GetObjectBody::Bytes(bytes)) => Body::from_bytes(bytes),
        Some(GetObjectBody::Stream(stream)) => Body::from_stream(stream).map_err(S3Error::internal_error)?,
    };
    let status = if dto.content_range.is_some() {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };
    Ok(answer(status, headers, body))
}

fn answer(status: StatusCode, headers: HeaderMap, body: Body) -> WebsiteResponse {
    let mut response = Response::new(body);
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    response
}

fn insert_website_header(headers: &mut HeaderMap, name: HeaderName, value: Option<&str>) -> S3Result<()> {
    if let Some(value) = value {
        headers.insert(name, HeaderValue::from_str(value).map_err(S3Error::internal_error)?);
    }
    Ok(())
}

async fn website_error_with_document(
    req: &WebsiteRequest,
    bucket: &str,
    error_document: Option<&PersistedErrorDocument>,
    status: StatusCode,
) -> S3Result<WebsiteResponse> {
    let Some(error_document) = error_document else {
        return Ok(website_error(status, req.head()));
    };
    match req.get_object(bucket, error_document.key.as_str(), false).await {
        Ok(response) => {
            let mut output = response_to_website(response, req.head())?;
            *output.status_mut() = status;
            Ok(output)
        }
        Err(err)
            if matches!(
                err.code(),
                S3ErrorCode::NoSuchKey | S3ErrorCode::NoSuchVersion | S3ErrorCode::AccessDenied
            ) =>
        {
            Ok(website_error(status, req.head()))
        }
        Err(err) => Err(err),
    }
}

fn website_error(status: StatusCode, head: bool) -> WebsiteResponse {
    let message = status.canonical_reason().unwrap_or("Error");
    let body = format!("<html><body>{message}</body></html>");
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/html; charset=utf-8"));
    answer(status, headers, if head { Body::empty() } else { Body::from_bytes(body.into()) })
}

fn website_not_modified(error: &S3Error) -> WebsiteResponse {
    let mut headers = error.headers().cloned().unwrap_or_default();
    headers.remove(http::header::CONTENT_LENGTH);
    headers.remove(http::header::CONTENT_TYPE);
    headers.remove(http::header::TRANSFER_ENCODING);
    answer(StatusCode::NOT_MODIFIED, headers, Body::empty())
}

fn website_location(location: String, status: StatusCode) -> S3Result<WebsiteResponse> {
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::LOCATION,
        HeaderValue::try_from(location)
            .map_err(|_| S3Error::with_message(S3ErrorCode::InternalError, "invalid website redirect location"))?,
    );
    Ok(answer(status, headers, Body::empty()))
}

fn website_relative_redirect(uri: &Uri, path: &str, status: StatusCode) -> S3Result<WebsiteResponse> {
    let mut url = url::Url::parse("http://localhost/").map_err(S3Error::internal_error)?;
    url.set_path(path);
    url.set_query(uri.query());
    let location = match url.query() {
        Some(query) => format!("{}?{query}", url.path()),
        None => url.path().to_owned(),
    };
    website_location(location, status)
}

fn website_redirect(
    uri: &Uri,
    headers: &HeaderMap,
    host: &str,
    protocol: Option<&str>,
    key: &str,
    status: StatusCode,
    website_scheme: &str,
) -> S3Result<WebsiteResponse> {
    let scheme = protocol.unwrap_or(website_scheme);
    let host = if host.is_empty() {
        headers.get(HOST).and_then(|h| h.to_str().ok()).unwrap_or_default()
    } else {
        host
    };
    let mut url = url::Url::parse(&format!("{scheme}://{host}/")).map_err(S3Error::internal_error)?;
    url.set_path(&format!("/{key}"));
    url.set_query(uri.query());
    website_location(url.into(), status)
}

fn website_rule_matches(rule: &RoutingRule, key: &str, status: StatusCode) -> bool {
    let Some(condition) = rule.condition.as_ref() else { return true };
    condition
        .key_prefix_equals
        .as_ref()
        .is_none_or(|prefix| key.starts_with(prefix))
        && condition
            .http_error_code_returned_equals
            .as_ref()
            .is_none_or(|code| code == status.as_str())
}

fn website_rule_redirect(req: &WebsiteRequest, rule: &RoutingRule, key: &str, website_scheme: &str) -> S3Result<WebsiteResponse> {
    let redirect = &rule.redirect;
    let key = if let Some(replace) = &redirect.replace_key_with {
        replace.clone()
    } else if let Some(replace) = &redirect.replace_key_prefix_with {
        let prefix = rule
            .condition
            .as_ref()
            .and_then(|condition| condition.key_prefix_equals.as_deref())
            .unwrap_or_default();
        format!("{replace}{}", key.strip_prefix(prefix).unwrap_or(key))
    } else {
        key.to_owned()
    };
    let status = redirect
        .http_redirect_code
        .as_deref()
        .and_then(|code| code.parse::<u16>().ok())
        .and_then(|code| StatusCode::from_u16(code).ok())
        .unwrap_or(StatusCode::MOVED_PERMANENTLY);
    website_redirect(
        &req.uri,
        &req.headers,
        redirect.host_name.as_deref().unwrap_or_default(),
        redirect.protocol.as_deref(),
        &key,
        status,
        website_scheme,
    )
}

#[cfg(test)]
mod tests;
