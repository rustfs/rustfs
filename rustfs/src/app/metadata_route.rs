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

//! MinIO-compatible metadata listing extension routes.

use super::bucket_usecase::DefaultBucketUsecase;
use super::object_usecase::DefaultObjectUsecase;
use crate::app::runtime_sources::ServerContextSlot;
use crate::auth::{check_key_valid, get_session_token};
use crate::storage::access::{ReqInfo, authorize_request, req_info_mut};
use crate::storage::{get_bucket_website_config_for_store, validate_website_configuration};
use async_trait::async_trait;
use http::header::CONTENT_TYPE;
use http::header::HOST;
use http::{Extensions, HeaderMap, HeaderValue, Method, StatusCode, Uri};
use rustfs_policy::policy::action::{Action, S3Action};
use s3s::access::S3Access;
use s3s::dto::{
    ETagCondition, EncodingType, GetObjectInput, GetObjectOutput, HeadObjectInput, ListObjectVersionsInput, ListObjectsV2Input,
    Range, RoutingRule, Timestamp, TimestampFormat,
};
use s3s::host::{MultiDomain, S3Host};
use s3s::route::S3Route;
use s3s::xml;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use std::sync::Arc;
use url::form_urlencoded;

pub(crate) struct MetadataRoute<A> {
    admin: A,
    host: Option<MultiDomain>,
    website_domains: Vec<String>,
    website_scheme: &'static str,
    server_ctx: Arc<ServerContextSlot>,
}

pub(crate) fn with_metadata_route<A>(
    admin: A,
    host: Option<MultiDomain>,
    website_domains: Vec<String>,
    website_scheme: &'static str,
    server_ctx: Arc<ServerContextSlot>,
) -> MetadataRoute<A> {
    MetadataRoute {
        admin,
        host,
        website_domains,
        website_scheme,
        server_ctx,
    }
}

#[async_trait]
impl<A> S3Route for MetadataRoute<A>
where
    A: S3Route,
{
    fn is_match(&self, method: &Method, uri: &Uri, headers: &HeaderMap, extensions: &mut Extensions) -> bool {
        website_target(uri, headers, &self.website_domains).is_some()
            || metadata_operation(method, uri, headers, self.host.as_ref()).is_some()
            || self.admin.is_match(method, uri, headers, extensions)
    }

    async fn check_access(&self, req: &mut S3Request<Body>) -> S3Result<()> {
        if let Some(target) = website_target(&req.uri, &req.headers, &self.website_domains) {
            req.extensions.insert(Arc::clone(&self.server_ctx));
            check_website_access(req, &target).await
        } else if let Some(target) = metadata_operation(&req.method, &req.uri, &req.headers, self.host.as_ref()) {
            check_metadata_access(req, target).await
        } else {
            self.admin.check_access(req).await
        }
    }

    async fn call(&self, req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        if let Some(target) = website_target(&req.uri, &req.headers, &self.website_domains) {
            let head = req.method == Method::HEAD;
            return call_website(req, target, Arc::clone(&self.server_ctx), self.website_scheme)
                .await
                .or_else(|err: S3Error| Ok(website_error(err.status_code().unwrap_or(StatusCode::INTERNAL_SERVER_ERROR), head)));
        }
        match metadata_operation(&req.method, &req.uri, &req.headers, self.host.as_ref()) {
            Some(BucketTarget {
                operation: MetadataOperation::ListObjectVersions,
                bucket,
            }) => call_list_object_versions(req, bucket).await,
            Some(BucketTarget {
                operation: MetadataOperation::ListObjectsV2,
                bucket,
            }) => call_list_objects_v2(req, bucket).await,
            None => self.admin.call(req).await,
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

async fn check_website_access(req: &mut S3Request<Body>, target: &WebsiteTarget) -> S3Result<()> {
    if req_info_mut(req).is_err() {
        req.extensions.insert(ReqInfo::default());
    }
    let req_info = req_info_mut(req)?;
    req_info.cred = None;
    req_info.is_owner = false;
    req_info.bucket = Some(target.bucket.clone());
    req_info.object = Some(target.key.clone());
    req_info.version_id = None;
    Ok(())
}

async fn call_website(
    req: S3Request<Body>,
    target: WebsiteTarget,
    server_ctx: Arc<ServerContextSlot>,
    website_scheme: &'static str,
) -> S3Result<S3Response<Body>> {
    if !matches!(req.method, Method::GET | Method::HEAD) {
        return Ok(website_error(StatusCode::METHOD_NOT_ALLOWED, req.method == Method::HEAD));
    }
    if target.invalid_path {
        return Ok(website_error(StatusCode::BAD_REQUEST, req.method == Method::HEAD));
    }
    if target.bucket.is_empty() || target.bucket.starts_with('.') || target.bucket.ends_with('.') || target.bucket.contains("..")
    {
        return Ok(website_error(StatusCode::NOT_FOUND, req.method == Method::HEAD));
    }

    let store = server_ctx
        .installed_object_store()
        .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "website object store unavailable"))?;
    let config = match get_bucket_website_config_for_store(&store, &target.bucket).await {
        Ok(config) => config,
        Err(crate::storage::StorageError::ConfigNotFound) => {
            return Ok(website_error(StatusCode::NOT_FOUND, req.method == Method::HEAD));
        }
        Err(crate::storage::StorageError::BucketNotFound(_)) => {
            return Ok(website_error(StatusCode::NOT_FOUND, req.method == Method::HEAD));
        }
        Err(err) => return Err(crate::error::ApiError::from(err).into()),
    };
    if validate_website_configuration(&config).is_err() {
        return Ok(website_error(StatusCode::INTERNAL_SERVER_ERROR, req.method == Method::HEAD));
    }

    if let Some(redirect) = config.redirect_all_requests_to.as_ref() {
        return website_redirect(
            &req,
            redirect.host_name.as_str(),
            redirect.protocol.as_ref().map(|protocol| protocol.as_str()),
            &target.key,
            StatusCode::MOVED_PERMANENTLY,
            website_scheme,
        );
    }

    let key = if target.key.is_empty() || target.trailing_slash {
        let suffix = config
            .index_document
            .as_ref()
            .map(|doc| doc.suffix.as_str())
            .unwrap_or_default();
        format!("{}{}", target.key, suffix)
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
            return website_rule_redirect(&req, rule, &target.key, website_scheme);
        }
    }

    let response = website_get_object(&req, &server_ctx, &target.bucket, &key, true).await;
    if let Err(err) = &response
        && matches!(
            err.code(),
            S3ErrorCode::NotModified | S3ErrorCode::PreconditionFailed | S3ErrorCode::InvalidRange
        )
        && let Ok(Some(location)) = website_head_redirect(&req, &server_ctx, &target.bucket, &key).await
    {
        return website_location(location, StatusCode::MOVED_PERMANENTLY);
    }
    let status = match &response {
        Ok(_) => StatusCode::OK,
        Err(err) => err.status_code().unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
    };
    if status == StatusCode::NOT_FOUND && !target.trailing_slash && !target.key.is_empty() {
        let index_key = format!(
            "{}/{}",
            target.key,
            config
                .index_document
                .as_ref()
                .map(|doc| doc.suffix.as_str())
                .unwrap_or_default()
        );
        match website_get_object(&req, &server_ctx, &target.bucket, &index_key, false).await {
            Ok(_) => return website_relative_redirect(&req, &format!("/{}/", target.key), StatusCode::FOUND),
            Err(err)
                if matches!(
                    err.code(),
                    S3ErrorCode::NoSuchKey | S3ErrorCode::NoSuchVersion | S3ErrorCode::AccessDenied
                ) => {}
            Err(err) => return Err(err),
        }
    }
    if let Some(rule) = rules.iter().find(|rule| website_rule_matches(rule, &target.key, status)) {
        return website_rule_redirect(&req, rule, &target.key, website_scheme);
    }
    match response {
        Ok(response) => {
            if let Some(location) = response.output.website_redirect_location.clone() {
                return website_location(location, StatusCode::MOVED_PERMANENTLY);
            }
            response_to_website(response, req.method == Method::HEAD)
        }
        Err(err) if matches!(err.code(), S3ErrorCode::NoSuchKey | S3ErrorCode::NoSuchVersion) => {
            website_error_with_document(&req, &server_ctx, &target.bucket, config.error_document.as_ref(), StatusCode::NOT_FOUND)
                .await
        }
        Err(err) if err.code() == &S3ErrorCode::NotModified => Ok(website_error(StatusCode::NOT_MODIFIED, true)),
        Err(_) if status.is_client_error() => {
            website_error_with_document(&req, &server_ctx, &target.bucket, config.error_document.as_ref(), status).await
        }
        Err(err) => Err(err),
    }
}

async fn website_get_object(
    original: &S3Request<Body>,
    server_ctx: &Arc<ServerContextSlot>,
    bucket: &str,
    key: &str,
    apply_conditions: bool,
) -> S3Result<S3Response<GetObjectOutput>> {
    let header = |name: http::header::HeaderName| {
        original
            .headers
            .get(name)
            .map(|value| {
                value
                    .to_str()
                    .map(str::to_owned)
                    .map_err(|_| S3Error::new(S3ErrorCode::InvalidArgument))
            })
            .transpose()
    };
    let input = if apply_conditions {
        GetObjectInput {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            range: header(http::header::RANGE)?
                .map(|value| Range::parse(&value).map_err(|_| S3Error::new(S3ErrorCode::InvalidRange)))
                .transpose()?,
            if_match: original
                .headers
                .get(http::header::IF_MATCH)
                .map(|value| {
                    ETagCondition::parse_http_header(value.as_bytes()).map_err(|_| S3Error::new(S3ErrorCode::InvalidArgument))
                })
                .transpose()?,
            if_none_match: original
                .headers
                .get(http::header::IF_NONE_MATCH)
                .map(|value| {
                    ETagCondition::parse_http_header(value.as_bytes()).map_err(|_| S3Error::new(S3ErrorCode::InvalidArgument))
                })
                .transpose()?,
            if_modified_since: header(http::header::IF_MODIFIED_SINCE)?
                .map(|value| {
                    Timestamp::parse(TimestampFormat::HttpDate, &value).map_err(|_| S3Error::new(S3ErrorCode::InvalidArgument))
                })
                .transpose()?,
            if_unmodified_since: header(http::header::IF_UNMODIFIED_SINCE)?
                .map(|value| {
                    Timestamp::parse(TimestampFormat::HttpDate, &value).map_err(|_| S3Error::new(S3ErrorCode::InvalidArgument))
                })
                .transpose()?,
            ..Default::default()
        }
    } else {
        GetObjectInput {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            ..Default::default()
        }
    };
    let mut request = S3Request {
        input,
        method: Method::GET,
        uri: original.uri.clone(),
        headers: if apply_conditions {
            original.headers.clone()
        } else {
            let mut headers = original.headers.clone();
            for name in [
                http::header::RANGE,
                http::header::IF_MATCH,
                http::header::IF_NONE_MATCH,
                http::header::IF_MODIFIED_SINCE,
                http::header::IF_UNMODIFIED_SINCE,
            ] {
                headers.remove(name);
            }
            headers
        },
        extensions: original.extensions.clone(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    };
    request.extensions.insert(Arc::clone(server_ctx));
    let req_info = request
        .extensions
        .get_mut::<ReqInfo>()
        .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "website request info missing"))?;
    req_info.bucket = Some(bucket.to_owned());
    req_info.object = Some(key.to_owned());
    req_info.cred = None;
    req_info.is_owner = false;
    request.extensions.insert(WebsiteRead);
    S3Access::get_object(&crate::storage::ecfs::FS::with_server_ctx(Arc::clone(server_ctx)), &mut request).await?;
    DefaultObjectUsecase::with_context(server_ctx.installed_app_context())
        .execute_get_object(request)
        .await
}

async fn website_head_redirect(
    original: &S3Request<Body>,
    server_ctx: &Arc<ServerContextSlot>,
    bucket: &str,
    key: &str,
) -> S3Result<Option<String>> {
    let mut headers = original.headers.clone();
    for name in [
        http::header::RANGE,
        http::header::IF_MATCH,
        http::header::IF_NONE_MATCH,
        http::header::IF_MODIFIED_SINCE,
        http::header::IF_UNMODIFIED_SINCE,
    ] {
        headers.remove(name);
    }
    let mut request = S3Request {
        input: HeadObjectInput {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            ..Default::default()
        },
        method: Method::HEAD,
        uri: original.uri.clone(),
        headers,
        extensions: original.extensions.clone(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    };
    request.extensions.insert(Arc::clone(server_ctx));
    let req_info = request
        .extensions
        .get_mut::<ReqInfo>()
        .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "website request info missing"))?;
    req_info.bucket = Some(bucket.to_owned());
    req_info.object = Some(key.to_owned());
    req_info.cred = None;
    req_info.is_owner = false;
    S3Access::head_object(&crate::storage::ecfs::FS::with_server_ctx(Arc::clone(server_ctx)), &mut request).await?;
    let response = DefaultObjectUsecase::with_context(server_ctx.installed_app_context())
        .execute_head_object(request)
        .await?;
    Ok(response.output.website_redirect_location)
}

#[derive(Clone)]
pub(crate) struct WebsiteRead;

fn response_to_website(response: S3Response<GetObjectOutput>, head: bool) -> S3Result<S3Response<Body>> {
    let output = response.output;
    let mut headers = response.headers;
    insert_website_header(&mut headers, http::header::CONTENT_TYPE, output.content_type.as_deref())?;
    insert_website_header(
        &mut headers,
        http::header::CONTENT_LENGTH,
        output.content_length.map(|length| length.to_string()).as_deref(),
    )?;
    insert_website_header(&mut headers, http::header::CONTENT_RANGE, output.content_range.as_deref())?;
    if let Some(tag) = output.e_tag.as_ref() {
        headers.insert(http::header::ETAG, tag.to_http_header().map_err(S3Error::internal_error)?);
    }
    insert_website_header(&mut headers, http::header::CACHE_CONTROL, output.cache_control.as_deref())?;
    insert_website_header(&mut headers, http::header::CONTENT_ENCODING, output.content_encoding.as_deref())?;
    insert_website_header(&mut headers, http::header::CONTENT_LANGUAGE, output.content_language.as_deref())?;
    insert_website_header(&mut headers, http::header::CONTENT_DISPOSITION, output.content_disposition.as_deref())?;
    if let Some(last_modified) = output.last_modified {
        let mut formatted = Vec::new();
        last_modified
            .format(TimestampFormat::HttpDate, &mut formatted)
            .map_err(S3Error::internal_error)?;
        headers.insert(
            http::header::LAST_MODIFIED,
            HeaderValue::from_bytes(&formatted).map_err(S3Error::internal_error)?,
        );
    }
    Ok(S3Response {
        output: if head {
            Body::empty()
        } else {
            output.body.map(Body::from).unwrap_or_default()
        },
        status: Some(if output.content_range.is_some() {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        }),
        headers,
        extensions: response.extensions,
    })
}

fn insert_website_header(headers: &mut HeaderMap, name: http::header::HeaderName, value: Option<&str>) -> S3Result<()> {
    if let Some(value) = value {
        headers.insert(name, HeaderValue::from_str(value).map_err(S3Error::internal_error)?);
    }
    Ok(())
}

async fn website_error_with_document(
    req: &S3Request<Body>,
    server_ctx: &Arc<ServerContextSlot>,
    bucket: &str,
    error_document: Option<&s3s::dto::ErrorDocument>,
    status: http::StatusCode,
) -> S3Result<S3Response<Body>> {
    let Some(error_document) = error_document else {
        return Ok(website_error(status, req.method == Method::HEAD));
    };
    match website_get_object(req, server_ctx, bucket, error_document.key.as_str(), false).await {
        Ok(response) => {
            let mut output = response_to_website(response, req.method == Method::HEAD)?;
            output.status = Some(status);
            Ok(output)
        }
        Err(err)
            if matches!(
                err.code(),
                S3ErrorCode::NoSuchKey | S3ErrorCode::NoSuchVersion | S3ErrorCode::AccessDenied
            ) =>
        {
            Ok(website_error(status, req.method == Method::HEAD))
        }
        Err(err) => Err(err),
    }
}

fn website_error(status: StatusCode, head: bool) -> S3Response<Body> {
    let message = status.canonical_reason().unwrap_or("Error");
    let body = format!("<html><body>{message}</body></html>");
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/html; charset=utf-8"));
    S3Response {
        output: if head { Body::empty() } else { Body::from(body.into_bytes()) },
        status: Some(status),
        headers,
        extensions: Extensions::new(),
    }
}

fn website_location(location: String, status: StatusCode) -> S3Result<S3Response<Body>> {
    let mut response = S3Response::new(Body::empty());
    response.status = Some(status);
    response.headers.insert(
        http::header::LOCATION,
        HeaderValue::try_from(location)
            .map_err(|_| S3Error::with_message(S3ErrorCode::InternalError, "invalid website redirect location"))?,
    );
    Ok(response)
}

fn website_relative_redirect(req: &S3Request<Body>, path: &str, status: StatusCode) -> S3Result<S3Response<Body>> {
    let mut url = url::Url::parse("http://localhost/").map_err(S3Error::internal_error)?;
    url.set_path(path);
    url.set_query(req.uri.query());
    let location = match url.query() {
        Some(query) => format!("{}?{query}", url.path()),
        None => url.path().to_owned(),
    };
    website_location(location, status)
}

fn website_redirect(
    req: &S3Request<Body>,
    host: &str,
    protocol: Option<&str>,
    key: &str,
    status: StatusCode,
    website_scheme: &str,
) -> S3Result<S3Response<Body>> {
    let scheme = protocol.unwrap_or(website_scheme);
    let host = if host.is_empty() {
        req.headers.get(HOST).and_then(|h| h.to_str().ok()).unwrap_or_default()
    } else {
        host
    };
    let mut url = url::Url::parse(&format!("{scheme}://{host}/")).map_err(S3Error::internal_error)?;
    url.set_path(&format!("/{key}"));
    url.set_query(req.uri.query());
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

fn website_rule_redirect(
    req: &S3Request<Body>,
    rule: &RoutingRule,
    key: &str,
    website_scheme: &str,
) -> S3Result<S3Response<Body>> {
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
        req,
        redirect.host_name.as_deref().unwrap_or_default(),
        redirect.protocol.as_ref().map(|protocol| protocol.as_str()),
        &key,
        status,
        website_scheme,
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MetadataOperation {
    ListObjectVersions,
    ListObjectsV2,
}

struct BucketTarget {
    operation: MetadataOperation,
    bucket: String,
}

fn metadata_operation(method: &Method, uri: &Uri, headers: &HeaderMap, host: Option<&MultiDomain>) -> Option<BucketTarget> {
    if method != Method::GET {
        return None;
    }
    let bucket = bucket_from_request(uri, headers, host).ok()?;

    let query = uri.query()?;
    if query_value(query, "metadata").as_deref() != Some("true") {
        return None;
    }
    let operation = if query_has(query, "versions") {
        MetadataOperation::ListObjectVersions
    } else if query_value(query, "list-type").as_deref() == Some("2") {
        MetadataOperation::ListObjectsV2
    } else {
        return None;
    };

    Some(BucketTarget { operation, bucket })
}

async fn check_metadata_access(req: &mut S3Request<Body>, target: BucketTarget) -> S3Result<()> {
    // Custom routes bypass the S3Access::check hook that normally resolves the
    // SigV4-verified credentials into ReqInfo, so resolve them here the same
    // way; otherwise a signed request is evaluated as anonymous and denied
    // (rustfs#4845). Anonymous requests keep cred=None and stay subject to the
    // bucket-policy-only evaluation in authorize_request.
    let (cred, is_owner) = if let Some(input_cred) = &req.credentials {
        let (cred, is_owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;
        (Some(cred), is_owner)
    } else {
        (None, false)
    };

    {
        if req_info_mut(req).is_err() {
            req.extensions.insert(ReqInfo::default());
        }
        let req_info = req_info_mut(req)?;
        req_info.cred = cred;
        req_info.is_owner = is_owner;
        req_info.bucket = Some(target.bucket);
    }

    let action = match target.operation {
        MetadataOperation::ListObjectVersions => Action::S3Action(S3Action::ListBucketVersionsAction),
        MetadataOperation::ListObjectsV2 => Action::S3Action(S3Action::ListBucketAction),
    };
    authorize_request(req, action).await
}

async fn call_list_object_versions(req: S3Request<Body>, bucket: String) -> S3Result<S3Response<Body>> {
    let input = list_object_versions_input(bucket, &req.uri, &req.headers)?;
    let request = req.map_input(|_| input);
    let output = DefaultBucketUsecase::from_global()
        .execute_list_object_versions_m(request)
        .await?;
    xml_response(&output.output)
}

async fn call_list_objects_v2(req: S3Request<Body>, bucket: String) -> S3Result<S3Response<Body>> {
    let input = list_objects_v2_input(bucket, &req.uri, &req.headers)?;
    let request = req.map_input(|_| input);
    let output = DefaultBucketUsecase::from_global().execute_list_objects_v2m(request).await?;
    xml_response(&output.output)
}

fn xml_response<T: xml::Serialize>(output: &T) -> S3Result<S3Response<Body>> {
    let mut body = Vec::with_capacity(1024);
    {
        let mut serializer = xml::Serializer::new(&mut body);
        serializer
            .decl()
            .and_then(|()| output.serialize(&mut serializer))
            .map_err(S3Error::internal_error)?;
    }

    let mut response = S3Response::new(Body::from(body));
    response
        .headers
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/xml"));
    Ok(response)
}

fn list_object_versions_input(bucket: String, uri: &Uri, headers: &HeaderMap) -> S3Result<ListObjectVersionsInput> {
    let query = uri.query().unwrap_or_default();

    Ok(ListObjectVersionsInput {
        bucket,
        delimiter: query_value(query, "delimiter"),
        encoding_type: parse_encoding_type(query_value(query, "encoding-type"))?,
        expected_bucket_owner: header_value(headers, "x-amz-expected-bucket-owner")?,
        key_marker: query_value(query, "key-marker"),
        max_keys: query_i32(query, "max-keys")?,
        optional_object_attributes: None,
        prefix: query_value(query, "prefix"),
        request_payer: header_value(headers, "x-amz-request-payer")?.map(Into::into),
        version_id_marker: query_value(query, "version-id-marker"),
    })
}

fn list_objects_v2_input(bucket: String, uri: &Uri, headers: &HeaderMap) -> S3Result<ListObjectsV2Input> {
    let query = uri.query().unwrap_or_default();

    Ok(ListObjectsV2Input {
        bucket,
        continuation_token: query_value(query, "continuation-token"),
        delimiter: query_value(query, "delimiter"),
        encoding_type: parse_encoding_type(query_value(query, "encoding-type"))?,
        expected_bucket_owner: header_value(headers, "x-amz-expected-bucket-owner")?,
        fetch_owner: query_bool(query, "fetch-owner")?,
        max_keys: query_i32(query, "max-keys")?,
        optional_object_attributes: None,
        prefix: query_value(query, "prefix"),
        request_payer: header_value(headers, "x-amz-request-payer")?.map(Into::into),
        start_after: query_value(query, "start-after"),
    })
}

fn bucket_from_request(uri: &Uri, headers: &HeaderMap, host: Option<&MultiDomain>) -> S3Result<String> {
    if let Some(host) = host
        && let Some(host_header) = headers.get(HOST).and_then(|value| value.to_str().ok())
    {
        let virtual_host = host.parse_host_header(host_header)?;
        if let Some(bucket) = virtual_host.bucket() {
            return if uri.path() == "/" {
                Ok(bucket.to_owned())
            } else {
                Err(s3_error!(InvalidRequest, "bucket-level metadata route requires a bucket path"))
            };
        }
    }

    path_style_bucket(uri)?.ok_or_else(|| s3_error!(InvalidRequest, "bucket name is required"))
}

fn path_style_bucket(uri: &Uri) -> S3Result<Option<String>> {
    let path = uri.path().trim_matches('/');
    if path.is_empty() {
        return Ok(None);
    }
    if path.contains('/') {
        return Err(s3_error!(InvalidRequest, "bucket-level metadata route does not accept an object path"));
    }
    urlencoding::decode(path)
        .map(|bucket| Some(bucket.into_owned()))
        .map_err(S3Error::internal_error)
}

fn query_has(query: &str, key: &str) -> bool {
    form_urlencoded::parse(query.as_bytes()).any(|(name, _)| name == key)
}

fn query_value(query: &str, key: &str) -> Option<String> {
    form_urlencoded::parse(query.as_bytes())
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.into_owned())
}

fn query_i32(query: &str, key: &str) -> S3Result<Option<i32>> {
    query_value(query, key)
        .map(|value| {
            value
                .parse::<i32>()
                .map_err(|_| s3_error!(InvalidArgument, "invalid integer query value"))
        })
        .transpose()
}

fn query_bool(query: &str, key: &str) -> S3Result<Option<bool>> {
    query_value(query, key)
        .map(|value| {
            value
                .parse::<bool>()
                .map_err(|_| s3_error!(InvalidArgument, "invalid boolean query value"))
        })
        .transpose()
}

fn parse_encoding_type(value: Option<String>) -> S3Result<Option<EncodingType>> {
    value
        .map(|value| {
            if value == EncodingType::URL {
                Ok(EncodingType::from_static(EncodingType::URL))
            } else {
                Err(s3_error!(InvalidArgument, "invalid encoding-type"))
            }
        })
        .transpose()
}

fn header_value(headers: &HeaderMap, name: &str) -> S3Result<Option<String>> {
    headers
        .get(name)
        .map(|value| {
            value
                .to_str()
                .map(str::to_owned)
                .map_err(|_| s3_error!(InvalidArgument, "invalid header value"))
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::{
        MetadataOperation, list_object_versions_input, list_objects_v2_input, metadata_operation, website_redirect,
        website_relative_redirect, website_rule_matches, website_target,
    };
    use http::header::HOST;
    use http::{Extensions, HeaderMap, Method, StatusCode, Uri};
    use s3s::dto::{Condition, EncodingType, Redirect, RoutingRule};
    use s3s::host::MultiDomain;
    use s3s::{Body, S3Request};

    fn uri(value: &str) -> Uri {
        value.parse().expect("test URI should parse")
    }

    fn website_request(value: &str) -> S3Request<Body> {
        let mut headers = HeaderMap::new();
        headers.insert(HOST, "site.example.com".parse().expect("valid host"));
        S3Request {
            input: Body::empty(),
            method: Method::GET,
            uri: uri(value),
            headers,
            extensions: Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    #[test]
    fn website_host_is_isolated_from_s3_host_and_keeps_directory_suffix() {
        let domains = vec!["example.com".to_owned()];
        let mut headers = HeaderMap::new();
        headers.insert(HOST, "share.example.com:9000".parse().expect("valid host"));
        let target = website_target(&uri("/docs/"), &headers, &domains).expect("website host matches");
        assert_eq!(target.bucket, "share");
        assert_eq!(target.key, "docs/");
        assert!(target.trailing_slash);
        headers.insert(HOST, "s3.other.test".parse().expect("valid host"));
        assert!(website_target(&uri("/share/docs/"), &headers, &domains).is_none());
        headers.insert(HOST, "my.bucket.example.com".parse().expect("valid host"));
        assert_eq!(
            website_target(&uri("/"), &headers, &domains)
                .expect("dotted bucket host matches")
                .bucket,
            "my.bucket"
        );
        headers.insert(HOST, "example.com".parse().expect("valid host"));
        assert_eq!(
            website_target(&uri("/share/key"), &headers, &domains)
                .expect("base domain is reserved")
                .bucket,
            ""
        );
        headers.insert(HOST, "share.example.com".parse().expect("valid host"));
        assert!(
            website_target(&uri("/%FF"), &headers, &domains)
                .expect("invalid path stays in website route")
                .invalid_path
        );
    }

    #[test]
    fn website_redirect_encodes_path_and_preserves_query() {
        let req = website_request("/old?download=1");
        let redirect = website_redirect(
            &req,
            "new.example.com",
            Some("https"),
            "new path/#one",
            StatusCode::MOVED_PERMANENTLY,
            "http",
        )
        .expect("valid redirect");
        assert_eq!(
            redirect.headers[http::header::LOCATION],
            "https://new.example.com/new%20path/%23one?download=1"
        );
        let directory = website_relative_redirect(&req, "/new path/", StatusCode::FOUND).expect("valid directory redirect");
        assert_eq!(directory.headers[http::header::LOCATION], "/new%20path/?download=1");
        let secure = website_redirect(&req, "new.example.com", None, "key", StatusCode::MOVED_PERMANENTLY, "https")
            .expect("TLS listener default scheme");
        assert_eq!(secure.headers[http::header::LOCATION], "https://new.example.com/key?download=1");
    }

    #[test]
    fn website_rules_choose_first_match_after_object_status_is_known() {
        let error_rule = RoutingRule {
            condition: Some(Condition {
                http_error_code_returned_equals: Some("404".to_owned()),
                key_prefix_equals: None,
            }),
            redirect: Redirect::default(),
        };
        let prefix_rule = RoutingRule {
            condition: Some(Condition {
                http_error_code_returned_equals: None,
                key_prefix_equals: Some("docs/".to_owned()),
            }),
            redirect: Redirect::default(),
        };
        let rules = [error_rule, prefix_rule];
        assert_eq!(
            rules
                .iter()
                .position(|rule| website_rule_matches(rule, "docs/missing", StatusCode::NOT_FOUND)),
            Some(0)
        );
        assert_eq!(
            rules
                .iter()
                .position(|rule| website_rule_matches(rule, "docs/existing", StatusCode::OK)),
            Some(1)
        );
    }

    #[test]
    fn metadata_operation_matches_path_style_extensions_only() {
        assert_eq!(
            metadata_operation(&Method::GET, &uri("/bucket?versions&metadata=true"), &HeaderMap::new(), None)
                .map(|target| target.operation),
            Some(MetadataOperation::ListObjectVersions)
        );
        assert_eq!(
            metadata_operation(&Method::GET, &uri("/bucket?list-type=2&metadata=true"), &HeaderMap::new(), None)
                .map(|target| target.operation),
            Some(MetadataOperation::ListObjectsV2)
        );
        assert_eq!(
            metadata_operation(&Method::GET, &uri("/?list-type=2&metadata=true"), &HeaderMap::new(), None)
                .map(|target| target.operation),
            None
        );
        assert_eq!(
            metadata_operation(&Method::GET, &uri("/bucket/key?list-type=2&metadata=true"), &HeaderMap::new(), None)
                .map(|target| target.operation),
            None
        );
        assert_eq!(
            metadata_operation(&Method::GET, &uri("/bucket?list-type=2"), &HeaderMap::new(), None).map(|target| target.operation),
            None
        );
        assert_eq!(
            metadata_operation(&Method::PUT, &uri("/bucket?list-type=2&metadata=true"), &HeaderMap::new(), None)
                .map(|target| target.operation),
            None
        );
    }

    #[test]
    fn metadata_operation_matches_virtual_hosted_bucket_root() {
        let host = MultiDomain::new(["example.com:9000"]).expect("valid test host domain");
        let mut headers = HeaderMap::new();
        headers.insert(HOST, "demo-bucket.example.com:9000".parse().expect("valid host header"));

        let target = metadata_operation(&Method::GET, &uri("/?list-type=2&metadata=true"), &headers, Some(&host))
            .expect("virtual-hosted bucket root should match metadata route");
        assert_eq!(target.operation, MetadataOperation::ListObjectsV2);
        assert_eq!(target.bucket, "demo-bucket");

        assert_eq!(
            metadata_operation(&Method::GET, &uri("/object.txt?list-type=2&metadata=true"), &headers, Some(&host))
                .map(|target| target.operation),
            None
        );
    }

    #[test]
    fn metadata_operation_matches_unconfigured_host_fallbacks() {
        let host = MultiDomain::new(["s3.example.com:9000"]).expect("valid test host domain");

        let mut path_style_headers = HeaderMap::new();
        path_style_headers.insert(HOST, "localhost:9000".parse().expect("valid host header"));
        let path_style = metadata_operation(
            &Method::GET,
            &uri("/path-bucket?list-type=2&metadata=true"),
            &path_style_headers,
            Some(&host),
        )
        .expect("unmatched host with a port should use path-style routing");
        assert_eq!(path_style.operation, MetadataOperation::ListObjectsV2);
        assert_eq!(path_style.bucket, "path-bucket");

        let mut cname_headers = HeaderMap::new();
        cname_headers.insert(HOST, "cdn.example.org".parse().expect("valid host header"));
        let cname = metadata_operation(&Method::GET, &uri("/?list-type=2&metadata=true"), &cname_headers, Some(&host))
            .expect("unmatched valid bucket host should use CNAME routing");
        assert_eq!(cname.operation, MetadataOperation::ListObjectsV2);
        assert_eq!(cname.bucket, "cdn.example.org");
    }

    #[test]
    fn list_objects_v2_input_parses_query_headers_and_decodes_bucket() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-expected-bucket-owner", "123456789012".parse().expect("valid header"));
        headers.insert("x-amz-request-payer", "requester".parse().expect("valid header"));

        let input = list_objects_v2_input(
            "demo bucket".to_string(),
            &uri("/demo%20bucket?list-type=2&metadata=true&prefix=logs%2F&delimiter=%2F&encoding-type=url&fetch-owner=true&max-keys=25&continuation-token=opaque&start-after=start"),
            &headers,
        )
        .expect("list objects v2 input should parse");

        assert_eq!(input.bucket, "demo bucket");
        assert_eq!(input.prefix.as_deref(), Some("logs/"));
        assert_eq!(input.delimiter.as_deref(), Some("/"));
        assert_eq!(input.encoding_type.as_ref().map(EncodingType::as_str), Some(EncodingType::URL));
        assert_eq!(input.fetch_owner, Some(true));
        assert_eq!(input.max_keys, Some(25));
        assert_eq!(input.continuation_token.as_deref(), Some("opaque"));
        assert_eq!(input.start_after.as_deref(), Some("start"));
        assert_eq!(input.expected_bucket_owner.as_deref(), Some("123456789012"));
        assert_eq!(input.request_payer.as_ref().map(|payer| payer.as_str()), Some("requester"));
    }

    #[test]
    fn list_object_versions_input_parses_markers_and_rejects_invalid_encoding() {
        let input = list_object_versions_input(
            "bucket".to_string(),
            &uri("/bucket?versions&metadata=true&prefix=logs%2F&delimiter=%2F&encoding-type=url&key-marker=start&version-id-marker=v1&max-keys=10"),
            &HeaderMap::new(),
        )
        .expect("list versions input should parse");

        assert_eq!(input.bucket, "bucket");
        assert_eq!(input.prefix.as_deref(), Some("logs/"));
        assert_eq!(input.delimiter.as_deref(), Some("/"));
        assert_eq!(input.encoding_type.as_ref().map(EncodingType::as_str), Some(EncodingType::URL));
        assert_eq!(input.key_marker.as_deref(), Some("start"));
        assert_eq!(input.version_id_marker.as_deref(), Some("v1"));
        assert_eq!(input.max_keys, Some(10));

        let err = list_object_versions_input(
            "bucket".to_string(),
            &uri("/bucket?versions&metadata=true&encoding-type=xml"),
            &HeaderMap::new(),
        )
        .expect_err("invalid encoding-type should be rejected");
        assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidArgument);
    }
}
