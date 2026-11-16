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

mod multipart;
mod owned;

use crate::runtime_sources::current_action_credentials;
#[cfg(feature = "webdav")]
use crate::runtime_sources::current_object_store_handle;
use crate::shared_types::RemoteAddr;
#[cfg(feature = "webdav")]
use crate::storage_api::protocols::client::capacity;
use crate::storage_api::protocols::client::{FS, MultipartStagingOperation, ReqInfo, RequestContext, authorize_request};
use http::{HeaderMap, Method};
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use rustfs_credentials;
use rustfs_gateway_types::compat::s3s_5761ddfe::{
    self as gateway_s3s_compat, leaf::RequestWire, request_context::GatewayRequestContext,
};
use rustfs_gateway_types::dto as gateway_dto;
#[cfg(test)]
use rustfs_gateway_types::{BucketName, OpaqueString};
use rustfs_protocols::common::SessionContext;
#[cfg(feature = "webdav")]
use rustfs_protocols::common::client::s3::{BucketCapacity, SessionCapacityView};
use rustfs_protocols::common::gateway::{AuthorizationError, S3Action, is_operation_supported};
use rustfs_trusted_proxies::ClientInfo;
use rustfs_utils::MaskedAccessKey;
#[cfg(test)]
use s3s::dto::*;
use s3s::{S3, S3Request, S3Result};
use std::fmt::Display;
use tracing::trace;

const LOG_COMPONENT_PROTOCOLS: &str = "protocols";
const LOG_SUBSYSTEM_STORAGE_CLIENT: &str = "storage_client";
const EVENT_PROTOCOL_STORAGE_CLIENT_REQUEST: &str = "protocol_storage_client_request";

const PATH_SEGMENT_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'<')
    .add(b'>')
    .add(b'?')
    .add(b'[')
    .add(b']')
    .add(b'`')
    .add(b'{')
    .add(b'}')
    .add(b'^')
    .add(b'|')
    .add(b'\\');

const QUERY_COMPONENT_ENCODE_SET: &AsciiSet = &PATH_SEGMENT_ENCODE_SET.add(b'&').add(b'+').add(b'/').add(b'=');

fn encode_path_segment(value: &str) -> String {
    utf8_percent_encode(value, PATH_SEGMENT_ENCODE_SET).to_string()
}

fn encode_object_key_path(key: &str) -> String {
    key.split('/').map(encode_path_segment).collect::<Vec<_>>().join("/")
}

fn encode_query_component(value: &str) -> String {
    utf8_percent_encode(value, QUERY_COMPONENT_ENCODE_SET).to_string()
}

fn append_query_param(uri: &mut String, first: &mut bool, key: &str, value: Option<&str>) {
    if *first {
        uri.push('?');
        *first = false;
    } else {
        uri.push('&');
    }

    uri.push_str(&encode_query_component(key));
    if let Some(value) = value {
        uri.push('=');
        uri.push_str(&encode_query_component(value));
    }
}

fn parse_protocol_uri(uri: String, context: String) -> S3Result<http::Uri> {
    uri.parse()
        .map_err(|e| s3s::S3Error::with_message(s3s::S3ErrorCode::InvalidRequest, format!("invalid URI for {context}: {e}")))
}

fn conversion_error(operation: &str, error: impl Display) -> rustfs_s3_types::S3Error {
    rustfs_s3_types::S3Error::with_message(
        rustfs_s3_types::S3ErrorCode::InternalError,
        format!("failed to convert {operation} through the gateway compatibility seam: {error}"),
    )
}

/// Formats an S3 copy-source header while preserving the literal object key.
/// The S3 copy-source parser URL-decodes the path and treats a raw `?versionId=`
/// as query data, so encode the complete key before building the header.
fn format_copy_source(bucket: &str, key: &str) -> String {
    format!("/{bucket}/{}", urlencoding::encode(key))
}

/// Converts a protocol copy source into the source the s3s storage path consumes.
///
/// A bucket source goes through the same header rendering and `CopySource::parse` that the
/// protocol layer used when it handed the storage client a header string, so bucket-name and
/// key validation and their errors are unchanged. Access point and Outposts sources are moved
/// across unchanged, and the storage copy path refuses them as it does for any other request.
fn legacy_copy_source(
    source: rustfs_gateway_types::CopySourceValue,
) -> Result<s3s::dto::CopySource, s3s::dto::ParseCopySourceError> {
    match source {
        rustfs_gateway_types::CopySourceValue::Bucket { bucket, key, version_id } => {
            let mut header = format_copy_source(&bucket, &key);
            if let Some(version_id) = version_id {
                header.push_str("?versionId=");
                header.push_str(&urlencoding::encode(&version_id));
            }
            s3s::dto::CopySource::parse(&header)
        }
        source => Ok(gateway_s3s_compat::copy_source::to_s3s(source)),
    }
}

/// Refuses a listing entry without an ETag, as the compatibility seam did while `Object.ETag` was required.
fn require_listed_etags(output: gateway_dto::ListObjectsV2Output) -> Result<gateway_dto::ListObjectsV2Output, String> {
    if output.contents.iter().any(|object| object.e_tag.is_none()) {
        return Err("ListObjectsV2 contents entry has no e_tag".to_owned());
    }
    Ok(output)
}

#[cfg(test)]
fn list_buckets_output_from_s3s(output: s3s::dto::ListBucketsOutput) -> Result<gateway_dto::ListBucketsOutput, String> {
    let s3s::dto::ListBucketsOutput {
        buckets,
        continuation_token,
        owner,
        prefix,
    } = output;

    // The storage inventory also contains RustFS-internal names such as
    // `sftp_host_keys`. They were representable by s3s but cannot be emitted
    // as gateway bucket names, so keep them out of protocol listings.
    let buckets = buckets
        .unwrap_or_default()
        .into_iter()
        .filter_map(|bucket| {
            let valid_name = bucket.name.as_ref().is_some_and(|name| BucketName::new(name.clone()).is_ok());
            valid_name.then_some(bucket)
        })
        .map(|bucket| gateway_s3s_compat::generated::shapes::bucket::bucket_from_s3s(bucket).map_err(|e| e.to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    let owner = owner
        .ok_or_else(|| "owner: the gateway shape requires this member".to_owned())
        .and_then(|owner| gateway_s3s_compat::generated::shapes::owner::owner_from_s3s(owner).map_err(|e| e.to_string()))?;

    Ok(gateway_dto::ListBucketsOutput {
        buckets,
        owner,
        continuation_token: continuation_token.map(OpaqueString::from),
        prefix,
    })
}

fn gateway_request_context(method: Method, uri: &http::Uri, headers: HeaderMap) -> GatewayRequestContext {
    GatewayRequestContext {
        method,
        raw_path: uri.path().to_owned(),
        raw_query: uri.query().unwrap_or_default().to_owned(),
        headers,
        principal: None,
        host_region: None,
        declares_trailers: false,
    }
}

fn trace_protocol_request(operation: &str, bucket: Option<&str>, object: Option<&str>) {
    trace!(
        event = EVENT_PROTOCOL_STORAGE_CLIENT_REQUEST,
        component = LOG_COMPONENT_PROTOCOLS,
        subsystem = LOG_SUBSYSTEM_STORAGE_CLIENT,
        operation,
        bucket = bucket.unwrap_or_default(),
        object = object.unwrap_or_default(),
        "Protocol storage client request"
    );
}

#[cfg(any(feature = "webdav", feature = "sftp"))]
fn session_list_buckets_envelope(
    server_context: std::sync::Arc<crate::runtime_sources::ServerContextSlot>,
    session_context: &SessionContext,
    request_headers: &HeaderMap,
    secure_transport: bool,
    credentials: rustfs_credentials::Credentials,
    is_owner: bool,
) -> crate::app::RequestEnvelope {
    let session_token = crate::auth::get_session_token(&http::Uri::from_static("/"), request_headers).map(str::to_owned);
    let request_credentials = crate::app::RequestCredentials::new(
        credentials.access_key.clone(),
        credentials.secret_key.clone(),
        session_token.clone(),
    )
    .unwrap_or_else(|_| crate::app::RequestCredentials::without_secret_key(credentials.access_key.clone(), session_token));
    let mut extensions = http::Extensions::default();
    extensions.insert(server_context);
    let remote_addr = std::net::SocketAddr::new(session_context.source_ip, 0);
    extensions.insert(Some(RemoteAddr(remote_addr)));
    let mut client_info = ClientInfo::direct(remote_addr);
    client_info.forwarded_proto = Some(if secure_transport { "https" } else { "http" }.to_string());
    extensions.insert(client_info);

    extensions.insert(ReqInfo {
        cred: Some(credentials),
        is_owner,
        keystone_session_present: None,
        bucket: None,
        object: None,
        version_id: None,
        replication_request_authorized: false,
        region: None,
        request_context: Some(RequestContext::fallback()),
        suppress_denial_log: false,
        suppress_denial_audit: false,
    });

    crate::app::RequestEnvelope::from_parts(
        crate::app::RequestHead {
            method: Method::GET,
            uri: http::Uri::from_static("/"),
            headers: request_headers.clone(),
            credentials: Some(request_credentials),
            region: None,
            service: None,
        },
        extensions,
    )
}

/// TTL for the cluster usable-capacity snapshot behind WebDAV quota
/// properties. WebDAV clients poll quota on a timer and the underlying
/// storage-info fan-out touches every disk (plus every peer when
/// distributed), so quota responses reuse the snapshot for this long.
#[cfg(feature = "webdav")]
const CLUSTER_CAPACITY_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(30);

/// Cached cluster usable capacity as `(loaded_at, (used_bytes, total_bytes))`.
#[cfg(feature = "webdav")]
type ClusterCapacitySnapshot = Option<(std::time::Instant, (u64, u64))>;

#[cfg(feature = "webdav")]
fn cluster_capacity_cache() -> &'static std::sync::Mutex<ClusterCapacitySnapshot> {
    static CACHE: std::sync::OnceLock<std::sync::Mutex<ClusterCapacitySnapshot>> = std::sync::OnceLock::new();
    CACHE.get_or_init(|| std::sync::Mutex::new(None))
}

/// Cluster usable capacity as (used, total) bytes — the same erasure-aware
/// numbers the console dashboard reports. Returns None while the object store
/// is not initialized or no capacity has been measured yet; quota properties
/// are omitted in that case.
#[cfg(feature = "webdav")]
async fn cluster_usable_capacity() -> Option<(u64, u64)> {
    if let Ok(cache) = cluster_capacity_cache().lock()
        && let Some((loaded_at, value)) = *cache
        && loaded_at.elapsed() < CLUSTER_CAPACITY_CACHE_TTL
    {
        return Some(value);
    }

    let store = current_object_store_handle()?;
    let info = capacity::StorageAdminApi::storage_info(store.as_ref()).await;
    let total = capacity::get_total_usable_capacity(&info.disks, &info) as u64;
    if total == 0 {
        // Disks are still initializing; reporting a zero-capacity drive would
        // make clients refuse writes, so omit quota properties instead.
        return None;
    }
    let free = capacity::get_total_usable_capacity_free(&info.disks, &info) as u64;
    let value = (total.saturating_sub(free), total);

    if let Ok(mut cache) = cluster_capacity_cache().lock() {
        *cache = Some((std::time::Instant::now(), value));
    }
    Some(value)
}

fn build_bucket_uri(bucket: &str, query: &[(&str, Option<&str>)]) -> S3Result<http::Uri> {
    let mut uri = format!("/{}", encode_path_segment(bucket));
    let mut first = true;
    for (key, value) in query {
        append_query_param(&mut uri, &mut first, key, *value);
    }
    parse_protocol_uri(uri, format!("bucket={bucket}"))
}

fn build_object_uri(bucket: &str, key: &str, query: &[(&str, Option<&str>)]) -> S3Result<http::Uri> {
    let mut uri = format!("/{}/{}", encode_path_segment(bucket), encode_object_key_path(key));
    let mut first = true;
    for (query_key, value) in query {
        append_query_param(&mut uri, &mut first, query_key, *value);
    }
    parse_protocol_uri(uri, format!("bucket={bucket} key={key}"))
}

fn build_list_objects_uri(input: &gateway_dto::ListObjectsV2Input) -> S3Result<http::Uri> {
    let max_keys = input.max_keys.map(|value| value.to_string());
    let query: Vec<_> = [
        ("list-type", Some("2")),
        ("prefix", input.prefix.as_deref()),
        ("delimiter", input.delimiter.as_deref()),
        ("max-keys", max_keys.as_deref()),
        ("continuation-token", input.continuation_token.as_ref().map(|value| value.as_str())),
        ("start-after", input.start_after.as_deref()),
    ]
    .into_iter()
    .filter(|(_, value)| value.is_some())
    .collect();
    build_bucket_uri(input.bucket.as_str(), &query)
}

/// Request parameters for creating S3 requests
#[derive(Debug)]
struct RequestParams<'a> {
    bucket: Option<String>,
    object: Option<String>,
    credentials: &'a rustfs_credentials::Credentials,
}

/// Protocol storage client that implements the StorageBackend trait
#[derive(Clone, Debug)]
pub struct ProtocolStorageClient {
    /// FS instance for storage operations
    fs: FS,
}

impl ProtocolStorageClient {
    /// Create a new protocol storage client
    pub fn new(fs: FS) -> Self {
        Self { fs }
    }

    async fn authorize_protocol_request(
        &self,
        session: &SessionContext,
        action: &S3Action,
        bucket: &str,
        object: Option<&str>,
        listing: Option<&gateway_dto::ListObjectsV2Input>,
    ) -> S3Result<()> {
        let context = self.fs.server_ctx().installed_app_context().ok_or_else(|| {
            s3s::S3Error::with_message(s3s::S3ErrorCode::InternalError, "protocol server application context is not initialized")
        })?;
        let (credentials, is_owner) =
            crate::auth::check_key_valid_with_context("", session.access_key(), Some(context.as_ref())).await?;
        let uri = if let Some(input) = listing {
            build_list_objects_uri(input)?
        } else if let Some(key) = object {
            build_object_uri(bucket, key, &[])?
        } else {
            build_bucket_uri(bucket, &[])?
        };
        let mut request = Self::create_request(
            (),
            Method::GET,
            uri,
            RequestParams {
                bucket: Some(bucket.to_owned()),
                object: object.map(str::to_owned),
                credentials: &credentials,
            },
        )?;
        request.extensions.insert(self.fs.server_ctx().clone());
        if let Some(info) = request.extensions.get_mut::<ReqInfo>() {
            info.is_owner = is_owner;
        }
        let remote_addr = std::net::SocketAddr::new(session.source_ip, 0);
        request.extensions.insert(Some(RemoteAddr(remote_addr)));
        let mut client_info = ClientInfo::direct(remote_addr);
        client_info.forwarded_proto = Some(if session.secure_transport { "https" } else { "http" }.to_owned());
        request.extensions.insert(client_info);
        request.headers = session.request_headers.clone();
        // Preserve staging semantics before distinct operations collapse to PutObject.
        if matches!(action, S3Action::CreateMultipartUpload | S3Action::UploadPart) {
            request.extensions.insert(MultipartStagingOperation);
        }
        let policy_action = if matches!(action, S3Action::HeadBucket) {
            S3Action::ListBucket.into()
        } else {
            action.clone().into()
        };
        authorize_request(&mut request, policy_action).await
    }

    /// Create a proper S3Request with ReqInfo extension for authorization
    fn create_request<T>(input: T, method: Method, uri: http::Uri, params: RequestParams<'_>) -> S3Result<S3Request<T>> {
        let mut extensions = http::Extensions::default();

        let is_owner = if let Some(global_cred) = current_action_credentials() {
            params.credentials.access_key == global_cred.access_key
        } else {
            false
        };

        let credentials = Some(s3s::auth::Credentials {
            access_key: params.credentials.access_key.clone(),
            secret_key: params.credentials.secret_key.clone().into(),
        });

        extensions.insert(ReqInfo {
            cred: Some(params.credentials.clone()),
            is_owner,
            keystone_session_present: None,
            bucket: params.bucket,
            object: params.object,
            version_id: None,
            replication_request_authorized: false,
            region: None,
            request_context: Some(RequestContext::fallback()),
            suppress_denial_log: false,
            suppress_denial_audit: false,
        });

        let req = S3Request {
            input,
            method,
            uri,
            headers: HeaderMap::default(),
            extensions,
            credentials,
            region: None,
            service: None,
            trailing_headers: None,
        };
        Ok(req)
    }
}

#[async_trait::async_trait]
impl rustfs_protocols::common::client::s3::StorageBackend for ProtocolStorageClient {
    type Error = rustfs_s3_types::S3Error;

    async fn authorize_operation(
        &self,
        session: &SessionContext,
        action: &S3Action,
        bucket: &str,
        object: Option<&str>,
    ) -> Result<(), AuthorizationError> {
        // ListBucket requires the complete listing input to evaluate request conditions.
        if matches!(action, S3Action::ListBucket) || !is_operation_supported(session.protocol, action) {
            return Err(AuthorizationError::AccessDenied);
        }
        self.authorize_protocol_request(session, action, bucket, object, None)
            .await
            .map_err(|error| match error.code() {
                s3s::S3ErrorCode::AccessDenied | s3s::S3ErrorCode::InvalidAccessKeyId => AuthorizationError::AccessDenied,
                _ => AuthorizationError::IamUnavailable,
            })
    }

    async fn authorize_list_objects(
        &self,
        session: &SessionContext,
        input: &gateway_dto::ListObjectsV2Input,
    ) -> Result<(), AuthorizationError> {
        if !is_operation_supported(session.protocol, &S3Action::ListBucket) {
            return Err(AuthorizationError::AccessDenied);
        }
        self.authorize_protocol_request(session, &S3Action::ListBucket, input.bucket.as_str(), None, Some(input))
            .await
            .map_err(|error| match error.code() {
                s3s::S3ErrorCode::AccessDenied | s3s::S3ErrorCode::InvalidAccessKeyId => AuthorizationError::AccessDenied,
                _ => AuthorizationError::IamUnavailable,
            })
    }

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        credentials: &rustfs_credentials::Credentials,
        start_pos: Option<u64>,
    ) -> Result<gateway_dto::GetObjectOutput, Self::Error> {
        trace_protocol_request("get_object", Some(bucket), Some(key));
        trace!(
            event = EVENT_PROTOCOL_STORAGE_CLIENT_REQUEST,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_STORAGE_CLIENT,
            operation = "get_object",
            bucket,
            object = %key,
            start_pos = ?start_pos,
            "Protocol storage client request"
        );

        self.get_object_owned(
            bucket,
            key,
            credentials,
            start_pos.map(|first| rustfs_gateway_types::ByteRange::From { first }),
        )
        .await
    }

    async fn put_object(
        &self,
        input: gateway_dto::PutObjectInput,
        credentials: &rustfs_credentials::Credentials,
    ) -> Result<gateway_dto::PutObjectOutput, Self::Error> {
        trace!(
            event = EVENT_PROTOCOL_STORAGE_CLIENT_REQUEST,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_STORAGE_CLIENT,
            operation = "put_object",
            bucket = %input.bucket.as_str(),
            object = %input.key.as_str(),
            "Protocol storage client request"
        );

        let bucket = input.bucket.as_str().to_owned();
        let key = input.key.as_str().to_owned();
        let uri = build_object_uri(&bucket, &key, &[])?;

        let content_length = input.content_length;
        let mut headers = HeaderMap::default();
        if content_length >= 0
            && let Ok(header_value) = content_length.to_string().parse()
        {
            headers.insert("content-length", header_value);
        }

        let input = owned::put_input(input)?;
        let envelope = Self::create_envelope_with_headers(
            Method::PUT,
            uri,
            RequestParams {
                bucket: Some(bucket),
                object: Some(key),
                credentials,
            },
            headers,
        );
        rustfs_io_metrics::record_s3_op(rustfs_s3_ops::S3Operation::PutObject);
        self.owned_usecase()
            .execute_put_object_owned(envelope, input)
            .await
            .map_err(owned::put_error)
            .and_then(owned::put_output)
    }

    async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
        credentials: &rustfs_credentials::Credentials,
    ) -> Result<gateway_dto::DeleteObjectOutput, Self::Error> {
        trace_protocol_request("delete_object", Some(bucket), Some(key));

        let uri = build_object_uri(bucket, key, &[])?;
        let envelope = Self::create_envelope(
            Method::DELETE,
            uri,
            RequestParams {
                bucket: Some(bucket.to_owned()),
                object: Some(key.to_owned()),
                credentials,
            },
        );
        rustfs_io_metrics::record_s3_op(rustfs_s3_ops::S3Operation::DeleteObject);
        self.owned_usecase()
            .execute_delete_object_fields(envelope, bucket.to_owned(), key.to_owned(), None)
            .await
    }

    async fn head_object(
        &self,
        bucket: &str,
        key: &str,
        credentials: &rustfs_credentials::Credentials,
    ) -> Result<gateway_dto::HeadObjectOutput, Self::Error> {
        trace_protocol_request("head_object", Some(bucket), Some(key));

        let uri = build_object_uri(bucket, key, &[])?;
        let envelope = Self::create_envelope(
            Method::HEAD,
            uri,
            RequestParams {
                bucket: Some(bucket.to_owned()),
                object: Some(key.to_owned()),
                credentials,
            },
        );
        rustfs_io_metrics::record_s3_op(rustfs_s3_ops::S3Operation::HeadObject);
        self.owned_usecase()
            .execute_head_object_fields(
                envelope,
                crate::app::object_usecase::HeadObjectFields {
                    bucket: bucket.to_owned(),
                    key: key.to_owned(),
                    version_id: None,
                    part_number: None,
                    range: None,
                    sse_customer_key: None,
                    sse_customer_key_md5: None,
                },
            )
            .await
            .and_then(|answered| owned::head_output(answered.output))
    }

    async fn head_bucket(
        &self,
        bucket: &str,
        _credentials: &rustfs_credentials::Credentials,
    ) -> Result<gateway_dto::HeadBucketOutput, Self::Error> {
        trace_protocol_request("head_bucket", Some(bucket), None);
        let _uri = build_bucket_uri(bucket, &[])?;
        rustfs_io_metrics::record_s3_op(rustfs_s3_ops::S3Operation::HeadBucket);
        let input = owned::head_bucket_input(bucket)?;
        self.owned_bucket_usecase().execute_head_bucket(input).await
    }

    async fn list_objects_v2(
        &self,
        input: gateway_dto::ListObjectsV2Input,
        credentials: &rustfs_credentials::Credentials,
    ) -> Result<gateway_dto::ListObjectsV2Output, Self::Error> {
        trace_protocol_request("list_objects_v2", Some(input.bucket.as_str()), None);

        let bucket = input.bucket.as_str().to_owned();
        let uri = build_list_objects_uri(&input)?;
        let envelope = Self::create_envelope(
            Method::GET,
            uri,
            RequestParams {
                bucket: Some(bucket),
                object: None,
                credentials,
            },
        );
        rustfs_io_metrics::record_s3_op(rustfs_s3_ops::S3Operation::ListObjectsV2);
        self.owned_bucket_usecase()
            .execute_list_objects_v2(envelope, input)
            .await
            .and_then(owned::listing_output)
    }

    async fn list_buckets(
        &self,
        credentials: &rustfs_credentials::Credentials,
    ) -> Result<gateway_dto::ListBucketsOutput, Self::Error> {
        trace!(
            event = EVENT_PROTOCOL_STORAGE_CLIENT_REQUEST,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_STORAGE_CLIENT,
            operation = "list_buckets",
            access_key = %MaskedAccessKey(&credentials.access_key),
            "Protocol storage client request"
        );

        let envelope = Self::create_envelope(
            Method::GET,
            http::Uri::from_static("/"),
            RequestParams {
                bucket: None,
                object: None,
                credentials,
            },
        );
        rustfs_io_metrics::record_s3_op(rustfs_s3_ops::S3Operation::ListBuckets);
        self.owned_bucket_usecase()
            .execute_list_buckets(envelope, gateway_dto::ListBucketsInput::default())
            .await
            .map(owned::bucket_listing_output)
    }

    #[cfg(any(feature = "webdav", feature = "sftp"))]
    async fn list_buckets_for_session(
        &self,
        session_context: &SessionContext,
        request_headers: &HeaderMap,
        secure_transport: bool,
    ) -> rustfs_s3_types::S3Result<gateway_dto::ListBucketsOutput> {
        trace!(
            event = EVENT_PROTOCOL_STORAGE_CLIENT_REQUEST,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_STORAGE_CLIENT,
            operation = "list_buckets",
            access_key = %MaskedAccessKey(&session_context.principal.user_identity.credentials.access_key),
            "Protocol storage client request"
        );

        let context = self.fs.server_ctx().installed_app_context().ok_or_else(|| {
            s3s::S3Error::with_message(s3s::S3ErrorCode::InternalError, "protocol server application context is not initialized")
        })?;
        // The session identity is captured at login. Re-check the key as
        // authorize_protocol_request does for every other operation, so a key
        // disabled or expired since then stops listing buckets. An invalid key
        // is an access denial, as in authorize_operation.
        let (credentials, is_owner) =
            crate::auth::check_key_valid_with_context("", session_context.access_key(), Some(context.as_ref()))
                .await
                .map_err(|e| match e.code() {
                    s3s::S3ErrorCode::InvalidAccessKeyId => s3s::S3Error::new(s3s::S3ErrorCode::AccessDenied),
                    _ => e,
                })?;
        let envelope = session_list_buckets_envelope(
            self.fs.server_ctx().clone(),
            session_context,
            request_headers,
            secure_transport,
            credentials,
            is_owner,
        );
        rustfs_io_metrics::record_s3_op(rustfs_s3_ops::S3Operation::ListBuckets);
        self.owned_bucket_usecase()
            .execute_list_buckets(envelope, gateway_dto::ListBucketsInput::default())
            .await
            .map(owned::bucket_listing_output)
    }

    /// Every input is a cache or snapshot read: quota reporting never triggers
    /// a scanner cycle or a live object listing.
    #[cfg(feature = "webdav")]
    async fn session_capacity_view(
        &self,
        session_context: &SessionContext,
        request_headers: &HeaderMap,
        secure_transport: bool,
    ) -> rustfs_s3_types::S3Result<Option<SessionCapacityView>> {
        // List buckets under the session's own authorization so the view
        // never covers buckets the caller cannot see.
        let listing = self
            .list_buckets_for_session(session_context, request_headers, secure_transport)
            .await?;

        let store = current_object_store_handle();
        let listed = listing.buckets;
        let mut buckets = Vec::with_capacity(listed.len());
        for bucket in listed {
            let name = bucket.name.as_str().to_owned();
            let quota_limit = match capacity::get_quota_config(&name).await {
                Ok((config, _)) => config.get_quota_limit(),
                // No quota configured, or the bucket was deleted between the
                // listing and this read: either way it has no limit.
                Err(e) if matches!(e, capacity::BucketConfigError::ConfigNotFound) || capacity::is_err_bucket_not_found(&e) => {
                    None
                }
                Err(e) => {
                    return Err(s3s::S3Error::with_message(
                        s3s::S3ErrorCode::InternalError,
                        format!("Failed to read bucket quota config: {e}"),
                    )
                    .into());
                }
            };
            let usage = match capacity::get_bucket_usage_memory(&name).await {
                Some(usage) => Some(usage),
                // Degraded windows (pre-v2 upgrade, or a bucket no complete
                // scanner cycle has covered) fall back to the last persisted
                // scanner snapshot; absent everywhere means unknown, which the
                // aggregation reports as zero.
                None => match &store {
                    Some(store) => capacity::lookup_degraded_bucket_usage_baseline(store.clone(), &name).await,
                    None => None,
                },
            };
            buckets.push(BucketCapacity { quota_limit, usage });
        }

        Ok(Some(SessionCapacityView {
            buckets,
            cluster_usable: cluster_usable_capacity().await,
        }))
    }

    async fn create_bucket(
        &self,
        bucket: &str,
        credentials: &rustfs_credentials::Credentials,
    ) -> Result<gateway_dto::CreateBucketOutput, Self::Error> {
        trace_protocol_request("create_bucket", Some(bucket), None);

        let uri = build_bucket_uri(bucket, &[])?;
        let envelope = Self::create_envelope(
            Method::PUT,
            uri,
            RequestParams {
                bucket: Some(bucket.to_owned()),
                object: None,
                credentials,
            },
        );
        rustfs_io_metrics::record_s3_op(rustfs_s3_ops::S3Operation::CreateBucket);
        let input = owned::create_bucket_input(bucket)?;
        self.owned_bucket_usecase().execute_create_bucket(envelope, input).await
    }

    async fn get_object_range(
        &self,
        bucket: &str,
        key: &str,
        credentials: &rustfs_credentials::Credentials,
        start_pos: u64,
        length: u64,
    ) -> Result<gateway_dto::GetObjectOutput, Self::Error> {
        trace_protocol_request("get_object_range", Some(bucket), Some(key));
        trace!(
            event = EVENT_PROTOCOL_STORAGE_CLIENT_REQUEST,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_STORAGE_CLIENT,
            operation = "get_object_range",
            bucket,
            object = %key,
            range_start = start_pos,
            range_length = length,
            "Protocol storage client request"
        );

        self.get_object_owned(
            bucket,
            key,
            credentials,
            Some(rustfs_gateway_types::ByteRange::FromTo {
                first: start_pos,
                last: start_pos + length - 1,
            }),
        )
        .await
    }

    async fn copy_object(
        &self,
        input: gateway_dto::CopyObjectInput,
        credentials: &rustfs_credentials::Credentials,
    ) -> Result<gateway_dto::CopyObjectOutput, Self::Error> {
        trace!(
            event = EVENT_PROTOCOL_STORAGE_CLIENT_REQUEST,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_STORAGE_CLIENT,
            operation = "copy_object",
            bucket = %input.bucket.as_str(),
            object = %input.key.as_str(),
            "Protocol storage client request"
        );

        let bucket = input.bucket.as_str().to_owned();
        let key = input.key.as_str().to_owned();
        let uri = build_object_uri(&bucket, &key, &[])?;
        let copy_source =
            legacy_copy_source(input.copy_source.clone()).map_err(|e| conversion_error("CopyObject copy_source", e))?;
        let context = gateway_request_context(Method::PUT, &uri, HeaderMap::new());
        let wire = RequestWire::of(&context);
        let input = gateway_s3s_compat::generated::ops::copy_object::input_to_s3s(input, copy_source, &wire)
            .map_err(|e| conversion_error("CopyObject input", e))?;

        let req = Self::create_request(
            input,
            Method::PUT,
            uri,
            RequestParams {
                bucket: Some(bucket),
                object: Some(key),
                credentials,
            },
        )?;

        match self.fs.copy_object(req).await {
            Ok(response) => gateway_s3s_compat::generated::ops::copy_object::output_from_s3s(response.output)
                .map_err(|e| conversion_error("CopyObject output", e)),
            Err(e) => Err(e.into()),
        }
    }

    async fn delete_bucket(
        &self,
        bucket: &str,
        credentials: &rustfs_credentials::Credentials,
    ) -> Result<gateway_dto::DeleteBucketOutput, Self::Error> {
        trace_protocol_request("delete_bucket", Some(bucket), None);
        let uri = build_bucket_uri(bucket, &[])?;
        let input = gateway_dto::DeleteBucketInput {
            bucket: rustfs_gateway_types::BucketName::new(bucket.to_owned())
                .map_err(|e| conversion_error("DeleteBucket bucket", e))?,
            expected_bucket_owner: None,
        };
        let envelope = Self::create_envelope(
            Method::DELETE,
            uri,
            RequestParams {
                bucket: Some(bucket.to_owned()),
                object: None,
                credentials,
            },
        );
        rustfs_io_metrics::record_s3_op(rustfs_s3_ops::S3Operation::DeleteBucket);
        self.owned_bucket_usecase().execute_delete_bucket(envelope, input).await
    }

    async fn create_multipart_upload(
        &self,
        input: gateway_dto::CreateMultipartUploadInput,
        credentials: &rustfs_credentials::Credentials,
    ) -> Result<gateway_dto::CreateMultipartUploadOutput, Self::Error> {
        trace!(
            event = EVENT_PROTOCOL_STORAGE_CLIENT_REQUEST,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_STORAGE_CLIENT,
            operation = "create_multipart_upload",
            bucket = %input.bucket.as_str(),
            object = %input.key.as_str(),
            "Protocol storage client request"
        );

        let bucket = input.bucket.as_str().to_owned();
        let key = input.key.as_str().to_owned();
        let uri = build_object_uri(&bucket, &key, &[("uploads", None)])?;
        let input = multipart::create_multipart_input(input)?;
        let envelope = Self::create_envelope(
            Method::POST,
            uri,
            RequestParams {
                bucket: Some(bucket),
                object: Some(key),
                credentials,
            },
        );
        rustfs_io_metrics::record_s3_op(rustfs_s3_ops::S3Operation::CreateMultipartUpload);
        self.owned_multipart_usecase()
            .execute_create_multipart_upload(envelope, input, None)
            .await
            .and_then(multipart::create_multipart_output)
    }

    async fn upload_part(
        &self,
        input: gateway_dto::UploadPartInput,
        credentials: &rustfs_credentials::Credentials,
    ) -> Result<gateway_dto::UploadPartOutput, Self::Error> {
        trace!(
            event = EVENT_PROTOCOL_STORAGE_CLIENT_REQUEST,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_STORAGE_CLIENT,
            operation = "upload_part",
            bucket = %input.bucket.as_str(),
            object = %input.key.as_str(),
            part_number = input.part_number,
            "Protocol storage client request"
        );

        let bucket = input.bucket.as_str().to_owned();
        let key = input.key.as_str().to_owned();
        let content_length = input.content_length;
        let input = gateway_s3s_compat::generated::ops::upload_part::input_to_s3s(input)
            .map_err(|e| conversion_error("UploadPart input", e))?;
        let part_number = input.part_number;
        let upload_id = input.upload_id.clone();
        let part_number = part_number.to_string();
        let uri = build_object_uri(
            &bucket,
            &key,
            &[
                ("partNumber", Some(part_number.as_str())),
                ("uploadId", Some(upload_id.as_str())),
            ],
        )?;

        let mut headers = HeaderMap::default();
        if let Ok(header_value) = content_length.to_string().parse() {
            headers.insert("content-length", header_value);
        }

        let req = Self::create_request(
            input,
            Method::PUT,
            uri,
            RequestParams {
                bucket: Some(bucket),
                object: Some(key),
                credentials,
            },
        )?;
        let req = S3Request { headers, ..req };

        match self.fs.upload_part(req).await {
            Ok(response) => gateway_s3s_compat::generated::ops::upload_part::output_from_s3s(response.output)
                .map_err(|e| conversion_error("UploadPart output", e)),
            Err(e) => Err(e.into()),
        }
    }

    async fn complete_multipart_upload(
        &self,
        input: gateway_dto::CompleteMultipartUploadInput,
        credentials: &rustfs_credentials::Credentials,
    ) -> Result<gateway_dto::CompleteMultipartUploadOutput, Self::Error> {
        trace!(
            event = EVENT_PROTOCOL_STORAGE_CLIENT_REQUEST,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_STORAGE_CLIENT,
            operation = "complete_multipart_upload",
            bucket = %input.bucket.as_str(),
            object = %input.key.as_str(),
            "Protocol storage client request"
        );

        let bucket = input.bucket.as_str().to_owned();
        let key = input.key.as_str().to_owned();
        // This scalar projection remains until the compatibility seam retires.
        let upload_id = gateway_s3s_compat::leaf::upload_id_to_s3s(&input.upload_id);
        let input = multipart::complete_multipart_input(input)?;
        let uri = build_object_uri(&bucket, &key, &[("uploadId", Some(upload_id.as_str()))])?;
        let envelope = Self::create_envelope(
            Method::POST,
            uri,
            RequestParams {
                bucket: Some(bucket),
                object: Some(key),
                credentials,
            },
        );
        rustfs_io_metrics::record_s3_op(rustfs_s3_ops::S3Operation::CompleteMultipartUpload);
        self.owned_multipart_usecase()
            .execute_complete_multipart_upload(envelope, input)
            .await
            .map_err(multipart::complete_multipart_error)
            .and_then(multipart::complete_multipart_output)
    }

    async fn abort_multipart_upload(
        &self,
        input: gateway_dto::AbortMultipartUploadInput,
        credentials: &rustfs_credentials::Credentials,
    ) -> Result<gateway_dto::AbortMultipartUploadOutput, Self::Error> {
        trace!(
            event = EVENT_PROTOCOL_STORAGE_CLIENT_REQUEST,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_STORAGE_CLIENT,
            operation = "abort_multipart_upload",
            bucket = %input.bucket.as_str(),
            object = %input.key.as_str(),
            "Protocol storage client request"
        );

        let bucket = input.bucket.as_str().to_owned();
        let key = input.key.as_str().to_owned();
        multipart::abort_multipart_input(&input)?;
        // This scalar projection remains until the compatibility seam retires.
        let upload_id = gateway_s3s_compat::leaf::upload_id_to_s3s(&input.upload_id);
        let uri = build_object_uri(&bucket, &key, &[("uploadId", Some(upload_id.as_str()))])?;
        let envelope = Self::create_envelope(
            Method::DELETE,
            uri,
            RequestParams {
                bucket: Some(bucket.clone()),
                object: Some(key.clone()),
                credentials,
            },
        );
        let target = crate::app::multipart_usecase::reject_abort_capabilities(&envelope).map(|()| {
            rustfs_io_metrics::record_s3_op(rustfs_s3_ops::S3Operation::AbortMultipartUpload);
            crate::app::multipart_usecase::AbortMultipartUploadTarget::Legacy { bucket, key, upload_id }
        });
        self.owned_multipart_usecase()
            .execute_abort_multipart_upload(envelope, target)
            .await?;
        Ok(gateway_dto::AbortMultipartUploadOutput::default())
    }

    async fn upload_part_copy(
        &self,
        input: gateway_dto::UploadPartCopyInput,
        credentials: &rustfs_credentials::Credentials,
    ) -> Result<gateway_dto::UploadPartCopyOutput, Self::Error> {
        trace!(
            event = EVENT_PROTOCOL_STORAGE_CLIENT_REQUEST,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_STORAGE_CLIENT,
            operation = "upload_part_copy",
            bucket = %input.bucket.as_str(),
            object = %input.key.as_str(),
            part_number = input.part_number,
            "Protocol storage client request"
        );

        let bucket = input.bucket.as_str().to_owned();
        let key = input.key.as_str().to_owned();
        let copy_source =
            legacy_copy_source(input.copy_source.clone()).map_err(|e| conversion_error("UploadPartCopy copy_source", e))?;
        let input = gateway_s3s_compat::generated::ops::upload_part_copy::input_to_s3s(input, copy_source)
            .map_err(|e| conversion_error("UploadPartCopy input", e))?;
        let part_number = input.part_number;
        let upload_id = input.upload_id.clone();
        let part_number = part_number.to_string();
        let uri = build_object_uri(
            &bucket,
            &key,
            &[
                ("partNumber", Some(part_number.as_str())),
                ("uploadId", Some(upload_id.as_str())),
            ],
        )?;

        let req = Self::create_request(
            input,
            Method::PUT,
            uri,
            RequestParams {
                bucket: Some(bucket),
                object: Some(key),
                credentials,
            },
        )?;

        match self.fs.upload_part_copy(req).await {
            Ok(response) => gateway_s3s_compat::generated::ops::upload_part_copy::output_from_s3s(response.output)
                .map_err(|e| conversion_error("UploadPartCopy output", e)),
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_credentials::{IAM_POLICY_CLAIM_NAME_SA, INHERITED_POLICY_TYPE};

    fn listed_object(e_tag: Option<&str>) -> gateway_dto::Object {
        gateway_dto::Object {
            key: rustfs_gateway_types::ObjectKey::new("dir/object.txt".to_owned()).expect("key"),
            e_tag: e_tag.map(|tag| rustfs_gateway_types::ETag::new(tag.to_owned()).expect("tag")),
            ..Default::default()
        }
    }

    #[test]
    fn require_listed_etags_refuses_an_entry_without_an_etag() {
        for contents in [
            vec![listed_object(None)],
            vec![listed_object(Some("abc")), listed_object(None)],
        ] {
            let output = gateway_dto::ListObjectsV2Output {
                contents,
                ..Default::default()
            };
            assert!(require_listed_etags(output).is_err());
        }
    }

    #[test]
    fn require_listed_etags_keeps_a_listing_whose_entries_all_have_etags() {
        for tags in [Vec::new(), vec!["abc", "def"]] {
            let output = gateway_dto::ListObjectsV2Output {
                contents: tags.iter().map(|tag| listed_object(Some(tag))).collect(),
                ..Default::default()
            };
            let kept = require_listed_etags(output).expect("listing with tags");
            let kept_tags: Vec<_> = kept
                .contents
                .iter()
                .map(|object| object.e_tag.as_ref().map(|tag| tag.opaque_tag().to_owned()))
                .collect();
            assert_eq!(kept_tags, tags.iter().map(|tag| Some((*tag).to_owned())).collect::<Vec<_>>());
        }
    }

    #[test]
    fn format_copy_source_preserves_encoded_and_query_like_keys() {
        assert_eq!(format_copy_source("bucket", "%2F"), "/bucket/%252F");
        assert_eq!(format_copy_source("bucket", "foo?versionId=1"), "/bucket/foo%3FversionId%3D1");
        assert_eq!(format_copy_source("bucket", "a/b"), "/bucket/a%2Fb");
    }

    fn bucket_source(bucket: &str, key: &str, version_id: Option<&str>) -> rustfs_gateway_types::CopySourceValue {
        rustfs_gateway_types::CopySourceValue::Bucket {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            version_id: version_id.map(str::to_owned),
        }
    }

    fn s3s_bucket(source: s3s::dto::CopySource) -> (String, String, Option<String>) {
        match source {
            s3s::dto::CopySource::Bucket { bucket, key, version_id } => {
                (bucket.to_string(), key.to_string(), version_id.map(|v| v.to_string()))
            }
            other => panic!("expected a bucket source, got {other:?}"),
        }
    }

    #[test]
    fn legacy_copy_source_keeps_a_plain_bucket_key() {
        let source = legacy_copy_source(bucket_source("bucket", "dir/object.txt", None)).expect("plain source");
        assert_eq!(s3s_bucket(source), ("bucket".to_owned(), "dir/object.txt".to_owned(), None));
    }

    #[test]
    fn legacy_copy_source_keeps_keys_that_need_percent_encoding() {
        for key in ["%2F", "foo?versionId=1", "a b+c", "\u{4e2d}\u{6587}", "x&y=z#frag"] {
            let source = legacy_copy_source(bucket_source("bucket", key, None)).expect("encoded source");
            assert_eq!(s3s_bucket(source), ("bucket".to_owned(), key.to_owned(), None), "{key}");
        }
    }

    #[test]
    fn legacy_copy_source_keeps_a_version_id() {
        let source =
            legacy_copy_source(bucket_source("bucket", "object", Some("3HL4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY"))).expect("versioned");
        assert_eq!(
            s3s_bucket(source),
            (
                "bucket".to_owned(),
                "object".to_owned(),
                Some("3HL4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY".to_owned())
            )
        );
    }

    #[test]
    fn legacy_copy_source_rejects_an_invalid_bucket_name_as_before() {
        for bucket in ["", "ab", "Upper", "under_score"] {
            assert!(
                matches!(
                    legacy_copy_source(bucket_source(bucket, "object", None)),
                    Err(s3s::dto::ParseCopySourceError::InvalidBucketName | s3s::dto::ParseCopySourceError::PatternMismatch)
                ),
                "{bucket}"
            );
        }
    }

    #[test]
    fn legacy_copy_source_rejects_an_overlong_key_as_before() {
        let key = "k".repeat(1025);
        assert!(matches!(
            legacy_copy_source(bucket_source("bucket", &key, None)),
            Err(s3s::dto::ParseCopySourceError::InvalidKey)
        ));
    }

    #[test]
    fn legacy_copy_source_hands_access_point_and_outpost_sources_to_the_refusing_copy_path() {
        let access_point = legacy_copy_source(rustfs_gateway_types::CopySourceValue::AccessPoint {
            partition: "aws".to_owned(),
            region: "us-east-1".to_owned(),
            account_id: "123456789012".to_owned(),
            access_point_name: "ap".to_owned(),
            key: "object".to_owned(),
            version_id: None,
        })
        .expect("access point source");
        assert!(matches!(access_point, s3s::dto::CopySource::AccessPoint { .. }));

        let outpost = legacy_copy_source(rustfs_gateway_types::CopySourceValue::Outpost {
            partition: "aws".to_owned(),
            region: "us-east-1".to_owned(),
            account_id: "123456789012".to_owned(),
            outpost_id: "op-01234567890123456/accesspoint/ap".to_owned(),
            key: "object".to_owned(),
            version_id: None,
        })
        .expect("outpost source");
        assert!(matches!(outpost, s3s::dto::CopySource::Outpost { .. }));
    }

    #[test]
    fn create_request_preserves_authenticated_service_account_identity() {
        let claims = std::collections::HashMap::from([
            ("parent".to_string(), serde_json::json!("alice")),
            (IAM_POLICY_CLAIM_NAME_SA.to_string(), serde_json::json!(INHERITED_POLICY_TYPE)),
        ]);
        let credentials = rustfs_credentials::Credentials {
            access_key: "service-account".to_string(),
            secret_key: "secret".to_string(),
            session_token: "signed-service-account-token".to_string(),
            parent_user: "alice".to_string(),
            groups: Some(vec!["developers".to_string()]),
            claims: Some(claims.clone()),
            ..Default::default()
        };

        let request = ProtocolStorageClient::create_request(
            ListObjectsV2Input::default(),
            Method::GET,
            http::Uri::from_static("/bucket?list-type=2"),
            RequestParams {
                bucket: Some("bucket".to_string()),
                object: None,
                credentials: &credentials,
            },
        )
        .expect("request should build");
        let envelope = ProtocolStorageClient::create_envelope(
            Method::GET,
            http::Uri::from_static("/bucket?list-type=2"),
            RequestParams {
                bucket: Some("bucket".to_owned()),
                object: None,
                credentials: &credentials,
            },
        );
        assert_eq!(envelope.uri(), &request.uri);
        assert_eq!(envelope.headers(), &request.headers);
        for request_info in [
            request.extensions.get::<ReqInfo>().expect("request info should be present"),
            envelope.req_info().expect("owned request info"),
        ] {
            let copied = request_info.cred.as_ref().expect("credentials should be present");
            assert_eq!(copied.access_key, credentials.access_key);
            assert_eq!(copied.secret_key, credentials.secret_key);
            assert_eq!(copied.session_token, credentials.session_token);
            assert_eq!(copied.parent_user, credentials.parent_user);
            assert_eq!(copied.groups, credentials.groups);
            assert_eq!(copied.claims, Some(claims.clone()));
            assert!(copied.is_service_account());
        }
    }

    #[cfg(any(feature = "webdav", feature = "sftp"))]
    #[test]
    fn request_extensions_preserve_authenticated_identity_and_source_ip() {
        use std::collections::HashMap;
        use std::net::{IpAddr, Ipv4Addr};

        let claims = HashMap::from([("parent".to_string(), serde_json::json!("alice"))]);
        let credentials = rustfs_credentials::Credentials {
            access_key: "service-account".to_string(),
            secret_key: "secret".to_string(),
            session_token: "session-token".to_string(),
            parent_user: "alice".to_string(),
            groups: Some(vec!["developers".to_string()]),
            claims: Some(claims.clone()),
            ..Default::default()
        };
        let source_ip = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10));

        let identity = rustfs_policy::auth::UserIdentity {
            credentials: rustfs_credentials::Credentials {
                secret_key: "stale-secret".to_string(),
                groups: None,
                claims: None,
                ..credentials.clone()
            },
            ..Default::default()
        };
        let principal = rustfs_protocols::common::ProtocolPrincipal::new(std::sync::Arc::new(identity));
        let session_context = SessionContext::new(principal, rustfs_protocols::Protocol::WebDav, source_ip);
        let mut headers = HeaderMap::new();
        headers.insert("user-agent", http::HeaderValue::from_static("webdav-client"));
        let request = session_list_buckets_envelope(
            crate::runtime_sources::ServerContextSlot::new(),
            &session_context,
            &headers,
            true,
            credentials.clone(),
            true,
        );
        let request_info = request.req_info().expect("request info should be present");
        let copied = request_info.cred.as_ref().expect("credentials should be present");
        let remote_addr = request.remote_addr().expect("remote address should be present");
        let client_info = request.client_info().expect("client info should be present");

        assert_eq!(copied.access_key, credentials.access_key);
        assert_eq!(copied.secret_key, credentials.secret_key);
        assert_eq!(copied.session_token, credentials.session_token);
        assert_eq!(copied.parent_user, credentials.parent_user);
        assert_eq!(copied.groups, credentials.groups);
        assert_eq!(copied.claims, Some(claims));
        assert!(request_info.is_owner, "ownership comes from the server's credential validation");
        let request_credentials = request.credentials().expect("request credentials");
        assert_eq!(request_credentials.access_key(), credentials.access_key);
        assert_eq!(request_credentials.secret_key(), Some(credentials.secret_key.as_str()));
        assert_eq!(remote_addr.ip(), source_ip);
        assert_eq!(client_info.real_ip, source_ip);
        assert_eq!(client_info.forwarded_proto.as_deref(), Some("https"));
        assert_eq!(request.headers().get("user-agent").expect("user agent"), "webdav-client");

        let insecure_request = session_list_buckets_envelope(
            crate::runtime_sources::ServerContextSlot::new(),
            &session_context,
            &headers,
            false,
            credentials,
            false,
        );
        assert!(!insecure_request.req_info().expect("request info").is_owner);
        let insecure_client_info = insecure_request.client_info().expect("client info should be present");
        assert_eq!(insecure_client_info.forwarded_proto.as_deref(), Some("http"));
    }

    #[test]
    fn build_object_uri_encodes_key_segments_without_flattening_slashes() {
        let uri = build_object_uri("bucket", "dir/file name%raw?x", &[]).expect("uri should parse");

        assert_eq!(uri.to_string(), "/bucket/dir/file%20name%25raw%3Fx");
    }

    #[test]
    fn build_object_uri_preserves_leading_slash_in_object_key() {
        let uri = build_object_uri("bucket", "/absolute/key", &[]).expect("uri should parse");

        assert_eq!(uri.to_string(), "/bucket//absolute/key");
    }

    #[test]
    fn build_object_uri_encodes_multipart_query_values() {
        let uri = build_object_uri(
            "bucket",
            "multipart object",
            &[("partNumber", Some("7")), ("uploadId", Some("upload/id+with=value"))],
        )
        .expect("uri should parse");

        assert_eq!(
            uri.to_string(),
            "/bucket/multipart%20object?partNumber=7&uploadId=upload%2Fid%2Bwith%3Dvalue"
        );
    }

    #[test]
    fn build_bucket_uri_encodes_list_type_query() {
        let uri = build_bucket_uri("bucket", &[("list-type", Some("2"))]).expect("uri should parse");

        assert_eq!(uri.to_string(), "/bucket?list-type=2");
    }
    #[test]
    #[serial_test::serial]
    fn protocol_mainflow_markers_versions_and_errors_match_legacy() {
        crate::app::gating_test_env::run_large_stack_test("protocol-mainflow-oracle", || async {
            use crate::runtime_sources::ServerContextSlot;
            use crate::storage_api::protocols::client::{
                BucketOperations, MakeBucketOptions, ObjectIO, StorageObjectOptions, StoragePutObjReader,
            };
            use http_body_util::BodyExt;
            use rustfs_protocols::common::client::s3::StorageBackend;
            let root = rustfs_credentials::Credentials {
                access_key: "protocol-oracle-root".into(),
                secret_key: "protocol-oracle-secret".into(),
                status: "on".into(),
                ..Default::default()
            };
            let (_temp, context, _iam) = crate::protocols::policy_test::protocol_test_context(root.clone()).await;
            let store = context.object_store();
            let slot = ServerContextSlot::new();
            assert!(slot.install(context));
            let fs = FS::with_server_ctx(slot);
            let backend = ProtocolStorageClient::new(fs.clone());
            for (bucket, key, data, versioned) in [
                ("protocol-version-oracle", "versioned-key", b"versioned data".as_slice(), true),
                ("protocol-marker-oracle", "folder/", b"".as_slice(), false),
            ] {
                store
                    .make_bucket(
                        bucket,
                        &MakeBucketOptions {
                            versioning_enabled: versioned,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("create bucket");
                let mut reader = StoragePutObjReader::from_vec(data.to_vec());
                store
                    .put_object(
                        bucket,
                        key,
                        &mut reader,
                        &StorageObjectOptions {
                            versioned,
                            mod_time: Some(
                                time::OffsetDateTime::from_unix_timestamp(1_700_000_000)
                                    .expect("valid test time")
                                    .replace_nanosecond(123_456_789)
                                    .expect("valid nanoseconds"),
                            ),
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("seed object");
                let params = || RequestParams {
                    bucket: Some(bucket.to_owned()),
                    object: Some(key.to_owned()),
                    credentials: &root,
                };
                let uri = build_object_uri(bucket, key, &[]).expect("object URI");
                let old = fs
                    .get_object(
                        ProtocolStorageClient::create_request(
                            GetObjectInput::builder()
                                .bucket(bucket.to_owned())
                                .key(key.to_owned())
                                .build()
                                .expect("old GET input"),
                            Method::GET,
                            uri.clone(),
                            params(),
                        )
                        .expect("old GET request"),
                    )
                    .await
                    .expect("old GET");
                let old = gateway_s3s_compat::generated::ops::get_object::output_from_s3s(old.output).expect("old GET output");
                if versioned {
                    assert_eq!(old.last_modified.expect("old LastModified").subsec_nanos(), 123_000_000);
                }
                let current = backend.get_object(bucket, key, &root, None).await.expect("protocol GET");
                assert_eq!(current.last_modified, old.last_modified);
                assert_eq!(current.version_id, old.version_id);
                assert_eq!(current.version_id.is_some(), versioned);
                assert_eq!(current.e_tag, old.e_tag);
                assert_eq!(current.content_length, old.content_length);
                let collect = |output: gateway_dto::GetObjectOutput| async move {
                    output
                        .body
                        .expect("GET body")
                        .into_body()
                        .collect()
                        .await
                        .expect("read body")
                        .to_bytes()
                };
                assert_eq!(collect(current).await, collect(old).await);
                let old = fs
                    .head_object(
                        ProtocolStorageClient::create_request(
                            HeadObjectInput::builder()
                                .bucket(bucket.to_owned())
                                .key(key.to_owned())
                                .build()
                                .expect("old HEAD input"),
                            Method::HEAD,
                            uri,
                            params(),
                        )
                        .expect("old HEAD request"),
                    )
                    .await
                    .expect("old HEAD");
                let old = gateway_s3s_compat::generated::ops::head_object::output_from_s3s(old.output).expect("old HEAD output");
                if versioned {
                    assert_eq!(old.last_modified.expect("old LastModified").subsec_nanos(), 123_000_000);
                }
                let current = backend.head_object(bucket, key, &root).await.expect("protocol HEAD");
                assert_eq!(current.last_modified, old.last_modified);
                assert_eq!(current.version_id, old.version_id);
                assert_eq!(current.e_tag, old.e_tag);
                assert_eq!(current.content_length, old.content_length);
                let deleted = backend.delete_object(bucket, key, &root).await.expect("protocol DELETE");
                if versioned {
                    assert_eq!(deleted.delete_marker, Some(true));
                }
                assert_eq!(deleted.version_id.is_some(), versioned);
                let old_error: rustfs_s3_types::S3Error = fs
                    .get_object(
                        ProtocolStorageClient::create_request(
                            GetObjectInput::builder()
                                .bucket(bucket.to_owned())
                                .key(key.to_owned())
                                .build()
                                .expect("old GET input"),
                            Method::GET,
                            build_object_uri(bucket, key, &[]).expect("object URI"),
                            params(),
                        )
                        .expect("old GET request"),
                    )
                    .await
                    .expect_err("old GET deleted object")
                    .into();
                let current = backend
                    .get_object(bucket, key, &root, None)
                    .await
                    .expect_err("protocol GET deleted object");
                assert_eq!(current.code(), old_error.code());
                assert_eq!(current.status_code(), old_error.status_code());
                assert_eq!(current.headers(), old_error.headers());
                if versioned {
                    let headers = current.headers().expect("delete marker error headers");
                    assert_eq!(
                        headers
                            .get("x-amz-version-id")
                            .expect("marker version")
                            .to_str()
                            .expect("version text"),
                        deleted.version_id.as_deref().expect("delete version")
                    );
                }
            }
        });
    }
    #[test]
    fn protocol_mainflow_retention_timestamp_matches_legacy_precision() {
        use crate::app::object_usecase::{GetObjectOutputFacts, HeadObjectOutputFacts};
        let instant = rustfs_gateway_types::Timestamp::from_secs_nanos(1_700_000_000, 123_456_789).expect("test timestamp");
        let get = || GetObjectOutputFacts {
            dto: gateway_dto::GetObjectOutput {
                object_lock_retain_until_date: Some(instant),
                ..Default::default()
            },
            ..Default::default()
        };
        let head = || HeadObjectOutputFacts {
            dto: gateway_dto::HeadObjectOutput {
                object_lock_retain_until_date: Some(instant),
                ..Default::default()
            },
            etag: None,
        };
        let old_get =
            gateway_s3s_compat::generated::ops::get_object::output_from_s3s(get().into_s3s().expect("old GET projection"))
                .expect("old GET output");
        let old_head =
            gateway_s3s_compat::generated::ops::head_object::output_from_s3s(head().into_s3s().expect("old HEAD projection"))
                .expect("old HEAD output");
        let old = (old_get.object_lock_retain_until_date, old_head.object_lock_retain_until_date);
        let expected =
            Some(rustfs_gateway_types::Timestamp::from_secs_nanos(1_700_000_000, 123_000_000).expect("millisecond timestamp"));
        assert_eq!(old, (expected, expected));
        let current = (
            owned::get_output(get())
                .expect("protocol GET output")
                .object_lock_retain_until_date,
            owned::head_output(head())
                .expect("protocol HEAD output")
                .object_lock_retain_until_date,
        );
        assert_eq!(current, old);
    }
}

#[cfg(test)]
mod put_tests {
    use super::*;
    use crate::app::object_usecase::PutObjectWriteInput;
    use rustfs_s3_types::S3ErrorCode;

    fn input() -> gateway_dto::PutObjectInput {
        gateway_dto::PutObjectInput {
            bucket: BucketName::new("protocol-put-oracle".to_owned()).expect("bucket"),
            key: rustfs_gateway_types::ObjectKey::new("object".to_owned()).expect("key"),
            content_length: 4,
            body: Some(rustfs_gateway_stream::ByteStream::from_bytes(bytes::Bytes::from_static(b"data"))),
            ..Default::default()
        }
    }

    fn fields(input: PutObjectWriteInput) -> serde_json::Value {
        serde_json::json!({
            "bucket": input.bucket, "key": input.key, "length": input.content_length,
            "metadata": input.metadata, "specific_if_none_match": input.specific_if_none_match,
            "retain_until": input.object_lock_retain_until_date.map(|at| at.unix_timestamp_nanos().to_string()),
            "cache_control": input.cache_control, "content_disposition": input.content_disposition,
            "content_encoding": input.content_encoding, "content_language": input.content_language,
            "content_type": input.content_type, "expires": input.expires, "tagging": input.tagging,
            "version_id": input.version_id, "server_side_encryption": input.server_side_encryption,
            "sse_customer_algorithm": input.sse_customer_algorithm, "sse_customer_key": input.sse_customer_key,
            "sse_customer_key_md5": input.sse_customer_key_md5, "ssekms_key_id": input.ssekms_key_id,
            "content_md5": input.content_md5, "legal_hold": input.object_lock_legal_hold_status,
            "lock_mode": input.object_lock_mode, "storage_class": input.storage_class,
            "website_redirect_location": input.website_redirect_location, "checksum_algorithm": input.checksum_algorithm,
            "crc32": input.checksum_crc32, "crc32c": input.checksum_crc32c, "crc64nvme": input.checksum_crc64nvme,
            "sha1": input.checksum_sha1, "sha256": input.checksum_sha256,
        })
    }

    fn legacy_fields(input: gateway_dto::PutObjectInput) -> Result<serde_json::Value, String> {
        let length = input.content_length;
        gateway_s3s_compat::put_object::input_to_s3s(input)
            .map(|mut input| {
                if length < 0 {
                    input.content_length = None;
                }
                fields(PutObjectWriteInput::from_legacy(input, None))
            })
            .map_err(|error| conversion_error("PutObject input", error).to_string())
    }

    #[test]
    fn protocol_put_condition_grammar_matches_legacy() {
        let mut observations = Vec::new();
        let values = [
            "",
            "*",
            "\"*\"",
            " * ",
            "\"abc\"",
            "W/\"abc\"",
            "abc",
            "\"abc\", \"def\"",
            "\"\"",
            " ",
            "\"abc",
            "abc\"",
            "\"a b\"",
            "\"a\\b\"",
            "\"a\"b\"",
            "\"a\n b\"",
            "W/abc",
            "a,b",
            "a b",
            "\tabc",
            "\"a\t b\"",
            "é",
            "\"é\"",
        ]
        .into_iter()
        .map(str::to_owned)
        .chain((0_u8..32).flat_map(|byte| {
            let value = char::from(byte);
            [value.to_string(), format!("\"{value}\"")]
        }));
        for value in values {
            for none in [false, true] {
                let make = || {
                    let mut request = input();
                    if none {
                        request.if_none_match = Some(value.to_owned());
                    } else {
                        request.if_match = Some(value.to_owned());
                    }
                    request
                };
                let old = legacy_fields(make());
                let current = owned::put_input(make()).map(fields).map_err(|error| error.to_string());
                observations.push((value.clone(), none, current, old));
            }
        }
        let mismatches: Vec<_> = observations
            .into_iter()
            .filter(|(_, _, current, old)| current != old)
            .collect();
        assert!(mismatches.is_empty(), "condition projection mismatches: {mismatches:#?}");
    }

    #[test]
    fn protocol_put_optional_values_match_legacy() {
        for value in ["", "value"] {
            let make = || {
                let mut request = input();
                request.cache_control = Some(value.to_owned());
                request.content_disposition = Some(value.to_owned());
                request.content_encoding = Some(value.to_owned());
                request.content_language = Some(value.to_owned());
                request.content_type = Some(value.to_owned());
                request.content_md5 = Some(value.to_owned());
                request.tagging = Some(value.to_owned());
                request.sse_customer_algorithm = Some(value.to_owned());
                request.sse_customer_key_md5 = Some(value.to_owned());
                request.ssekms_key_id = Some(value.to_owned());
                request.website_redirect_location = Some(value.to_owned());
                request.storage_class = Some(gateway_dto::StorageClass::custom(value.to_owned()));
                request.server_side_encryption = Some(gateway_dto::ServerSideEncryption::custom(value.to_owned()));
                request.object_lock_mode = Some(gateway_dto::ObjectLockMode::custom(value.to_owned()));
                request.object_lock_legal_hold_status = Some(gateway_dto::ObjectLockLegalHoldStatus::custom(value.to_owned()));
                request.checksum_algorithm = Some(gateway_dto::ChecksumAlgorithm::custom(value.to_owned()));
                request.expires = Some(OpaqueString::from(value));
                if !value.is_empty() {
                    request.metadata.insert("key".to_owned(), "value".to_owned());
                }
                request
            };
            assert_eq!(
                owned::put_input(make()).map(fields).map_err(|error| error.to_string()),
                legacy_fields(make())
            );
        }
    }

    #[test]
    fn protocol_put_checksum_input_matches_legacy() {
        let mut observations = Vec::new();
        for (header, value) in [
            ("x-amz-checksum-crc32", "AAAAAA=="),
            ("x-amz-checksum-crc32c", "AAAAAA=="),
            ("x-amz-checksum-crc64nvme", "AAAAAAAAAAA="),
            ("x-amz-checksum-sha1", "AAAAAAAAAAAAAAAAAAAAAAAAAAA="),
            ("x-amz-checksum-sha256", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
        ] {
            let make = || {
                let mut request = input();
                request.checksum_spec =
                    Some(rustfs_gateway_types::ChecksumSpec::parse_header(header, value).expect("test checksum"));
                request
            };
            observations.push((
                header,
                owned::put_input(make()).map(fields).map_err(|error| error.to_string()),
                legacy_fields(make()),
            ));
        }
        let mismatches: Vec<_> = observations.into_iter().filter(|(_, current, old)| current != old).collect();
        assert!(mismatches.is_empty(), "PutObject checksum input mismatches: {mismatches:#?}");
    }

    #[test]
    fn protocol_put_retain_until_matches_legacy() {
        let mut observations = Vec::new();
        for (secs, nanos) in [
            (1_700_000_000, 123_456_789),
            (-1, 123_456_789),
            (0, 123_456_789),
            (i64::MAX, 0),
        ] {
            let make = || {
                let mut request = input();
                request.object_lock_retain_until_date =
                    Some(rustfs_gateway_types::Timestamp::from_secs_nanos(secs, nanos).expect("instant"));
                request
            };
            observations.push((
                secs,
                nanos,
                owned::put_input(make()).map(fields).map_err(|error| error.to_string()),
                legacy_fields(make()),
            ));
        }
        let mismatches: Vec<_> = observations
            .into_iter()
            .filter(|(_, _, current, old)| current != old)
            .collect();
        assert!(mismatches.is_empty(), "retain-until projection mismatches: {mismatches:#?}");
    }

    #[test]
    fn protocol_put_output_matches_legacy() {
        use crate::app::object_usecase::PutObjectWriteOutput;
        let mut observations = Vec::new();
        for (raw, tag) in [
            (None, None),
            (Some("abc"), Some(s3s::dto::ETag::Strong("abc".to_owned()))),
            (Some("\"abc\""), Some(s3s::dto::ETag::Strong("abc".to_owned()))),
            (Some("W/\"abc\""), Some(s3s::dto::ETag::Weak("abc".to_owned()))),
            (Some("W/\"\"abc\"\""), Some(s3s::dto::ETag::Weak("\"abc\"".to_owned()))),
            (Some(""), Some(s3s::dto::ETag::Strong(String::new()))),
            (Some("\"\""), Some(s3s::dto::ETag::Strong(String::new()))),
            (Some("a\nb"), Some(s3s::dto::ETag::Strong("a\nb".to_owned()))),
        ] {
            let old = gateway_s3s_compat::put_object::output_from_s3s(PutObjectOutput {
                e_tag: tag,
                ..Default::default()
            })
            .map_err(|error| conversion_error("PutObject output", error));
            let current = owned::put_output(PutObjectWriteOutput {
                e_tag: raw.map(str::to_owned),
                ..Default::default()
            });
            observations.push((format!("etag {raw:?}"), outcome(&old), outcome(&current)));
        }
        for (index, checksum) in [
            (0, "AAAAAA=="),
            (1, "AAAAAA=="),
            (2, "AAAAAAAAAAA="),
            (3, "AAAAAAAAAAAAAAAAAAAAAAAAAAA="),
            (4, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
            (0, "bad"),
            (5, "AAAAAA=="),
        ] {
            let mut current = PutObjectWriteOutput {
                e_tag: Some("abc".to_owned()),
                ..Default::default()
            };
            let mut old = PutObjectOutput {
                e_tag: Some(s3s::dto::ETag::Strong("abc".to_owned())),
                ..Default::default()
            };
            match index {
                0 => {
                    current.checksum_crc32 = Some(checksum.to_owned());
                    old.checksum_crc32 = Some(checksum.to_owned());
                }
                1 => {
                    current.checksum_crc32c = Some(checksum.to_owned());
                    old.checksum_crc32c = Some(checksum.to_owned());
                }
                2 => {
                    current.checksum_crc64nvme = Some(checksum.to_owned());
                    old.checksum_crc64nvme = Some(checksum.to_owned());
                }
                3 => {
                    current.checksum_sha1 = Some(checksum.to_owned());
                    old.checksum_sha1 = Some(checksum.to_owned());
                }
                4 => {
                    current.checksum_sha256 = Some(checksum.to_owned());
                    old.checksum_sha256 = Some(checksum.to_owned());
                }
                _ => {
                    current.checksum_crc32 = Some(checksum.to_owned());
                    current.checksum_crc32c = Some(checksum.to_owned());
                    old.checksum_crc32 = Some(checksum.to_owned());
                    old.checksum_crc32c = Some(checksum.to_owned());
                }
            }
            let old =
                gateway_s3s_compat::put_object::output_from_s3s(old).map_err(|error| conversion_error("PutObject output", error));
            let current = owned::put_output(current);
            observations.push((format!("checksum {index} {checksum}"), outcome(&old), outcome(&current)));
        }
        let current = crate::app::object_usecase::PutObjectWriteOutput {
            e_tag: Some("abc".to_owned()),
            version_id: Some("version".to_owned()),
            expiration: Some("expiry".to_owned()),
            server_side_encryption: Some("AES256".to_owned()),
            sse_customer_algorithm: Some("AES256".to_owned()),
            sse_customer_key_md5: Some("md5".to_owned()),
            ssekms_key_id: Some("key-id".to_owned()),
            ..Default::default()
        };
        let old = PutObjectOutput {
            e_tag: Some(s3s::dto::ETag::Strong("abc".to_owned())),
            version_id: Some("version".to_owned()),
            expiration: Some("expiry".to_owned()),
            server_side_encryption: Some(ServerSideEncryption::from("AES256".to_owned())),
            sse_customer_algorithm: Some("AES256".to_owned()),
            sse_customer_key_md5: Some("md5".to_owned()),
            ssekms_key_id: Some("key-id".to_owned()),
            ..Default::default()
        };
        let old =
            gateway_s3s_compat::put_object::output_from_s3s(old).map_err(|error| conversion_error("PutObject output", error));
        observations.push(("populated output".to_owned(), outcome(&old), outcome(&owned::put_output(current))));
        let mismatches: Vec<_> = observations.into_iter().filter(|(_, old, current)| old != current).collect();
        assert!(mismatches.is_empty(), "PutObject output mismatches: {mismatches:#?}");
    }

    async fn legacy_put(
        fs: &FS,
        input: gateway_dto::PutObjectInput,
        credentials: &rustfs_credentials::Credentials,
    ) -> rustfs_s3_types::S3Result<gateway_dto::PutObjectOutput> {
        let bucket = input.bucket.as_str().to_owned();
        let key = input.key.as_str().to_owned();
        let uri = build_object_uri(&bucket, &key, &[])?;
        let length = input.content_length;
        let mut headers = HeaderMap::new();
        if length >= 0 {
            headers.insert("content-length", length.to_string().parse().expect("length header"));
        }
        let mut input =
            gateway_s3s_compat::put_object::input_to_s3s(input).map_err(|error| conversion_error("PutObject input", error))?;
        if length < 0 {
            input.content_length = None;
        }
        let request = ProtocolStorageClient::create_request(
            input,
            Method::PUT,
            uri,
            RequestParams {
                bucket: Some(bucket),
                object: Some(key),
                credentials,
            },
        )?;
        let response = fs.put_object(S3Request { headers, ..request }).await?;
        gateway_s3s_compat::put_object::output_from_s3s(response.output)
            .map_err(|error| conversion_error("PutObject output", error))
    }

    fn case_input(case: &str, key: &str) -> gateway_dto::PutObjectInput {
        let mut request = input();
        request.key = rustfs_gateway_types::ObjectKey::new(key.to_owned()).expect("case key");
        match case {
            "empty" => {
                request.content_type = Some(String::new());
                request.content_md5 = Some(String::new());
                request.storage_class = Some(gateway_dto::StorageClass::custom(String::new()));
            }
            "invalid-class" => request.storage_class = Some(gateway_dto::StorageClass::custom("NOT_A_CLASS".to_owned())),
            "short-body" => request.content_length = 5,
            "extra-body" => request.content_length = 3,
            "oversize" => {
                request.content_length =
                    i64::try_from(rustfs_config::MAX_SINGLE_PUT_OBJECT_SIZE + 1).expect("upload limit fits i64")
            }
            "unknown-length" => request.content_length = -1,
            "expires" => request.expires = Some(OpaqueString::from("not a date")),
            "empty-expires" => request.expires = Some(OpaqueString::from("")),
            "if-match" => request.if_match = Some("\"different\"".to_owned()),
            "bare-if-match" => request.if_match = Some("bare".to_owned()),
            "bad-if-match" => request.if_match = Some("\"unbalanced".to_owned()),
            "if-none-match" => request.if_none_match = Some("\"different\"".to_owned()),
            "bare-if-none-match" => request.if_none_match = Some("bare".to_owned()),
            "bad-if-none-match" => request.if_none_match = Some("\"unbalanced".to_owned()),
            "spaced-wildcard" => request.if_match = Some(" * ".to_owned()),
            "wildcard" => request.if_none_match = Some("*".to_owned()),
            "empty-event-hold" => request.object_lock_event_hold = Some(gateway_dto::ObjectLockEventHold::custom(String::new())),
            "event-hold-off" => request.object_lock_event_hold = Some(gateway_dto::ObjectLockEventHold::custom("OFF".to_owned())),
            "event-days-zero" => request.object_lock_event_hold_duration_days = Some(0),
            "event-years-zero" => request.object_lock_event_hold_duration_years = Some(0),
            "event-precedence" => {
                request.object_lock_event_hold = Some(gateway_dto::ObjectLockEventHold::custom("ON".to_owned()));
                request.if_match = Some("\"unbalanced".to_owned());
            }
            "event-hold" => request.object_lock_event_hold = Some(gateway_dto::ObjectLockEventHold::custom("ON".to_owned())),
            "event-days" => request.object_lock_event_hold_duration_days = Some(1),
            "event-years" => request.object_lock_event_hold_duration_years = Some(1),
            _ => (),
        }
        request
    }

    fn outcome(result: &rustfs_s3_types::S3Result<gateway_dto::PutObjectOutput>) -> serde_json::Value {
        match result {
            Ok(output) => serde_json::json!({"output": format!("{output:?}")}),
            Err(error) => {
                serde_json::json!({"code": format!("{:?}", error.code()), "status": error.status_code().map(|status| status.as_u16()), "message": error.message(), "headers": format!("{:?}", error.headers())})
            }
        }
    }

    #[test]
    fn protocol_put_error_projection_matches_legacy() {
        use rustfs_s3_types::S3Error;
        let mut observations = Vec::new();
        for (code, explicit) in [
            (S3ErrorCode::InvalidStorageClass, false),
            (S3ErrorCode::IncompleteBody, false),
            (S3ErrorCode::UnexpectedContent, false),
            (S3ErrorCode::Custom("ProtocolPutUnmapped".into()), false),
            (S3ErrorCode::InvalidStorageClass, true),
            (S3ErrorCode::IncompleteBody, true),
            (S3ErrorCode::UnexpectedContent, true),
            (S3ErrorCode::EntityTooLarge, true),
            (S3ErrorCode::NotImplemented, true),
            (S3ErrorCode::Custom("ProtocolPutUnmapped".into()), true),
        ] {
            let make = || {
                let mut error = S3Error::new(code.clone());
                if explicit {
                    error.set_message("explicit protocol error");
                    error.set_status_code(http::StatusCode::IM_A_TEAPOT);
                    let mut headers = HeaderMap::new();
                    headers.insert("x-protocol-test", http::HeaderValue::from_static("preserved"));
                    error.set_headers(headers);
                }
                error
            };
            let old: s3s::S3Error = make().into();
            let old = Err(S3Error::from(old));
            let current = Err(owned::put_error(make()));
            observations.push((code, explicit, outcome(&old), outcome(&current)));
        }
        let mismatches: Vec<_> = observations
            .into_iter()
            .filter(|(_, _, old, current)| old != current)
            .collect();
        assert!(mismatches.is_empty(), "PutObject error projection mismatches: {mismatches:#?}");
    }

    fn put_count() -> u64 {
        rustfs_io_metrics::s3_op_metrics_snapshot()
            .into_iter()
            .find(|row| row.op == rustfs_s3_ops::S3Operation::PutObject.as_str())
            .map_or(0, |row| row.total)
    }

    async fn object_state(backend: &ProtocolStorageClient, key: &str, root: &rustfs_credentials::Credentials) -> Option<Vec<u8>> {
        use http_body_util::BodyExt;
        use rustfs_protocols::common::client::s3::StorageBackend;
        match backend.get_object("protocol-put-oracle", key, root, None).await {
            Ok(output) => Some(
                output
                    .body
                    .expect("object body")
                    .into_body()
                    .collect()
                    .await
                    .expect("read object")
                    .to_bytes()
                    .to_vec(),
            ),
            Err(error) if error.code() == &S3ErrorCode::NoSuchKey => None,
            Err(error) => panic!("unexpected read after PUT: {error}"),
        }
    }

    #[test]
    #[serial_test::serial]
    fn protocol_put_runtime_matches_legacy() {
        crate::app::gating_test_env::run_large_stack_test("protocol-put-oracle", || async {
            compare_cases(&[
                ("plain", None),
                ("empty", None),
                ("unknown-length", Some(S3ErrorCode::UnexpectedContent)),
                ("expires", Some(S3ErrorCode::InvalidArgument)),
                ("empty-expires", None),
                ("if-match", None),
                ("bare-if-match", None),
                ("bad-if-match", Some(S3ErrorCode::InternalError)),
                ("if-none-match", Some(S3ErrorCode::NotImplemented)),
                ("bare-if-none-match", Some(S3ErrorCode::NotImplemented)),
                ("bad-if-none-match", Some(S3ErrorCode::InternalError)),
                ("spaced-wildcard", Some(S3ErrorCode::InternalError)),
                ("wildcard", None),
                ("empty-event-hold", None),
                ("event-hold-off", Some(S3ErrorCode::InternalError)),
                ("event-days-zero", Some(S3ErrorCode::InternalError)),
                ("event-years-zero", Some(S3ErrorCode::InternalError)),
                ("event-precedence", Some(S3ErrorCode::InternalError)),
                ("event-hold", Some(S3ErrorCode::InternalError)),
                ("event-days", Some(S3ErrorCode::InternalError)),
                ("event-years", Some(S3ErrorCode::InternalError)),
            ])
            .await;
        });
    }

    #[test]
    #[serial_test::serial]
    fn protocol_put_error_boundaries_match_legacy() {
        crate::app::gating_test_env::run_large_stack_test("protocol-put-error-oracle", || async {
            compare_cases(&[
                ("invalid-class", Some(S3ErrorCode::InvalidStorageClass)),
                ("short-body", Some(S3ErrorCode::IncompleteBody)),
                ("extra-body", Some(S3ErrorCode::UnexpectedContent)),
                ("oversize", Some(S3ErrorCode::EntityTooLarge)),
            ])
            .await;
        });
    }

    async fn compare_cases(cases: &[(&str, Option<S3ErrorCode>)]) {
        use crate::runtime_sources::ServerContextSlot;
        use crate::storage_api::protocols::client::{BucketOperations, MakeBucketOptions};
        use rustfs_protocols::common::client::s3::StorageBackend;
        let root = rustfs_credentials::Credentials {
            access_key: "protocol-put-root".into(),
            secret_key: "protocol-put-secret".into(),
            status: "on".into(),
            ..Default::default()
        };
        let (_temp, context, _iam) = crate::protocols::policy_test::protocol_test_context(root.clone()).await;
        context
            .object_store()
            .make_bucket("protocol-put-oracle", &MakeBucketOptions::default())
            .await
            .expect("create bucket");
        let slot = ServerContextSlot::new();
        assert!(slot.install(context));
        let backend = ProtocolStorageClient::new(FS::with_server_ctx(slot));
        let mut observations = Vec::new();
        for (case, expected) in cases.iter().cloned() {
            let old_key = format!("old/{case}");
            let current_key = format!("current/{case}");
            let before = put_count();
            let old = legacy_put(&backend.fs, case_input(case, &old_key), &root).await;
            let old_count = put_count() - before;
            let old_state = object_state(&backend, &old_key, &root).await;
            let before = put_count();
            let current = backend.put_object(case_input(case, &current_key), &root).await;
            let current_count = put_count() - before;
            let state = object_state(&backend, &current_key, &root).await;
            let expected_count = u64::from(expected != Some(S3ErrorCode::InternalError));
            observations.push((
                case,
                outcome(&old),
                outcome(&current),
                old.as_ref().err().map(|error| error.code().clone()),
                expected.clone(),
                state,
                expected.is_none().then(|| b"data".to_vec()),
                old_state,
                old_count,
                current_count,
                expected_count,
            ));
        }
        let mismatches: Vec<_> = observations
            .into_iter()
            .filter(
                |(
                    _,
                    old,
                    current,
                    old_code,
                    expected,
                    state,
                    expected_state,
                    old_state,
                    old_count,
                    current_count,
                    expected_count,
                )| {
                    old != current
                        || old_code != expected
                        || state != expected_state
                        || old_state != expected_state
                        || old_count != expected_count
                        || current_count != expected_count
                },
            )
            .collect();
        assert!(mismatches.is_empty(), "protocol PutObject mismatches: {mismatches:#?}");
    }
}

#[cfg(test)]
mod bucket_tests {
    use super::*;
    use crate::runtime_sources::ServerContextSlot;
    use crate::storage_api::protocols::client::{
        BucketOperations, MakeBucketOptions, ObjectIO, StorageObjectOptions, StoragePutObjReader,
    };
    use http_body_util::BodyExt;
    use rustfs_protocols::common::client::s3::StorageBackend;
    use rustfs_s3_ops::S3Operation;

    fn count(op: S3Operation) -> u64 {
        rustfs_io_metrics::s3_op_metrics_snapshot()
            .into_iter()
            .find(|row| row.op == op.as_str())
            .map_or(0, |row| row.total)
    }

    fn outcome<T: std::fmt::Debug>(result: &rustfs_s3_types::S3Result<T>) -> serde_json::Value {
        match result {
            Ok(output) => serde_json::json!({"output": format!("{output:?}")}),
            Err(error) => {
                serde_json::json!({"code": format!("{:?}",error.code()), "message": error.message(), "status": error.status_code().map(|status|status.as_u16()), "headers": format!("{:?}",error.headers())})
            }
        }
    }

    #[test]
    #[serial_test::serial]
    fn protocol_create_bucket_runtime_contract() {
        crate::app::gating_test_env::run_large_stack_test("protocol-create-bucket", || async {
            let root = rustfs_credentials::Credentials {
                access_key: "protocol-create-root".into(),
                secret_key: "protocol-create-secret".into(),
                status: "on".into(),
                ..Default::default()
            };
            let (_ambient_temp, ambient, _ambient_iam) = crate::protocols::policy_test::protocol_test_context(root.clone()).await;
            crate::runtime_sources::publish_test_app_context(ambient.clone());
            let (_target_temp, target, _target_iam) = crate::protocols::policy_test::protocol_test_context(root.clone()).await;
            let slot = ServerContextSlot::new();
            assert!(slot.install(target.clone()));
            let backend = ProtocolStorageClient::new(FS::with_server_ctx(slot));
            let mut observations = Vec::new();
            for name in [
                "protocol-created",
                "protocol-created",
                "xn--reserved",
                "",
                "bad\0bucket",
                "a",
                "a/b",
                "bad_bucket",
                "127.0.0.1",
                "bucket-",
            ] {
                let before = count(S3Operation::CreateBucket);
                let answer = outcome(&backend.create_bucket(name, &root).await);
                let present = target.object_store().get_bucket_info(name, &Default::default()).await.is_ok();
                observations.push((name, answer, count(S3Operation::CreateBucket) - before, present));
            }
            let ambient_created = ambient
                .object_store()
                .get_bucket_info("protocol-created", &Default::default())
                .await
                .is_ok();
            let uninstalled = ProtocolStorageClient::new(FS::with_server_ctx(ServerContextSlot::new()));
            let before = count(S3Operation::CreateBucket);
            let fallback = outcome(
                &uninstalled
                    .create_bucket("protocol-fallback-created", &rustfs_credentials::Credentials::default())
                    .await,
            );
            let fallback_present = ambient
                .object_store()
                .get_bucket_info("protocol-fallback-created", &Default::default())
                .await
                .is_ok();
            let actual = serde_json::json!({"rows": observations, "ambient_created":ambient_created,"fallback":fallback,"fallback_present":fallback_present,"fallback_count":count(S3Operation::CreateBucket)-before});
            let expected: serde_json::Value = serde_json::from_str(r#"{"ambient_created":false,"fallback":{"output":"Output { location: None }"},"fallback_count":1,"fallback_present":true,"rows":[["protocol-created",{"output":"Output { location: None }"},1,true],["protocol-created",{"output":"Output { location: None }"},1,true],["xn--reserved",{"output":"Output { location: None }"},1,true],["",{"code":"InternalError","headers":"None","message":"failed to convert through the gateway compatibility seam: bucket: not a bucket name the gateway can write","status":500},1,true],["bad\u0000bucket",{"code":"InternalError","headers":"None","message":"failed to convert through the gateway compatibility seam: bucket: not a bucket name the gateway can write","status":500},1,false],["a",{"code":"InternalError","headers":"None","message":"failed to convert through the gateway compatibility seam: bucket: not a bucket name the gateway can write","status":500},1,false],["a/b",{"code":"InternalError","headers":"None","message":"failed to convert through the gateway compatibility seam: bucket: not a bucket name the gateway can write","status":500},1,false],["bad_bucket",{"code":"InvalidBucketName","headers":"None","message":"The specified bucket is not valid.","status":400},1,false],["127.0.0.1",{"code":"InvalidBucketName","headers":"None","message":"The specified bucket is not valid.","status":400},1,false],["bucket-",{"code":"InvalidBucketName","headers":"None","message":"The specified bucket is not valid.","status":400},1,false]]}"#).expect("measured CreateBucket answers");
            assert_eq!(actual, expected, "CreateBucket answers, counts and bound storage match measured main");
        });
    }

    #[test]
    #[serial_test::serial]
    fn protocol_bucket_runtime_contract() {
        crate::app::gating_test_env::run_large_stack_test("protocol-bucket-contract", || async {
            let root = rustfs_credentials::Credentials {
                access_key: "protocol-bucket-root".into(),
                secret_key: "protocol-bucket-secret".into(),
                status: "on".into(),
                ..Default::default()
            };
            let (_temp, context, _iam) = crate::protocols::policy_test::protocol_test_context(root.clone()).await;
            let ambient = crate::runtime_sources::publish_test_app_context(context.clone());
            assert!(std::sync::Arc::ptr_eq(&ambient, &context), "fixture publishes its own context");
            let store = context.object_store();
            for bucket in ["protocol-bucket-empty", "protocol-bucket-full"] {
                store
                    .make_bucket(bucket, &MakeBucketOptions::default())
                    .await
                    .expect("seed bucket");
            }
            let mut reader = StoragePutObjReader::from_vec(b"retained".to_vec());
            store
                .put_object("protocol-bucket-full", "object", &mut reader, &StorageObjectOptions::default())
                .await
                .expect("seed object");
            let slot = ServerContextSlot::new();
            assert!(slot.install(context.clone()));
            let backend = ProtocolStorageClient::new(FS::with_server_ctx(slot));
            let mut observations = Vec::new();
            for bucket in [
                "protocol-bucket-full",
                "protocol-bucket-missing",
                "xn--reserved",
                "",
                "bad\0bucket",
            ] {
                let before = count(S3Operation::HeadBucket);
                let result = backend.head_bucket(bucket, &root).await;
                observations.push(("head", bucket, outcome(&result), count(S3Operation::HeadBucket) - before));
            }
            for bucket in [
                "protocol-bucket-full",
                "protocol-bucket-missing",
                "xn--reserved",
                "",
                "bad\0bucket",
                "protocol-bucket-empty",
            ] {
                let before = count(S3Operation::DeleteBucket);
                let result = backend.delete_bucket(bucket, &root).await;
                observations.push(("delete", bucket, outcome(&result), count(S3Operation::DeleteBucket) - before));
            }

            let full_present = store
                .get_bucket_info("protocol-bucket-full", &Default::default())
                .await
                .is_ok();
            let retained = match backend.get_object("protocol-bucket-full", "object", &root, None).await {
                Ok(output) => match output.body {
                    Some(body) => body
                        .into_body()
                        .collect()
                        .await
                        .map(|body| body.to_bytes().to_vec())
                        .map_err(|error| error.to_string()),
                    None => Err("missing retained body".to_owned()),
                },
                Err(error) => Err(format!("{:?}", error.code())),
            };
            let empty_present = store
                .get_bucket_info("protocol-bucket-empty", &Default::default())
                .await
                .is_ok();
            let expected: serde_json::Value = serde_json::from_str(r#"[
["head","protocol-bucket-full",{"output":"Output { bucket_region: \"\" }"},1],
["head","protocol-bucket-missing",{"code":"NoSuchBucket","headers":"None","message":"The specified bucket does not exist","status":404},1],
["head","xn--reserved",{"code":"NoSuchBucket","headers":"None","message":"The specified bucket does not exist","status":404},1],
["head","",{"output":"Output { bucket_region: \"\" }"},1],
["head","bad\u0000bucket",{"code":"InternalError","headers":"None","message":"failed to convert through the gateway compatibility seam: bucket: not a bucket name the gateway can write","status":500},1],
["delete","protocol-bucket-full",{"code":"BucketNotEmpty","headers":"None","message":"The bucket you tried to delete is not empty","status":409},1],
["delete","protocol-bucket-missing",{"code":"NoSuchBucket","headers":"None","message":"The specified bucket does not exist","status":404},1],
["delete","xn--reserved",{"code":"InternalError","headers":"None","message":"failed to convert DeleteBucket bucket through the gateway compatibility seam: invalid BucketName: the name uses a reserved form [rule: aws:bucketnamingrules]","status":500},0],
["delete","",{"code":"InternalError","headers":"None","message":"failed to convert DeleteBucket bucket through the gateway compatibility seam: invalid BucketName: the name is empty [rule: aws:bucketnamingrules]","status":500},0],
["delete","bad\u0000bucket",{"code":"InternalError","headers":"None","message":"failed to convert DeleteBucket bucket through the gateway compatibility seam: invalid BucketName: the name contains a NUL byte [rule: aws:bucketnamingrules]","status":500},0],
["delete","protocol-bucket-empty",{"output":"Output"},1]
]"#).expect("baseline answers");
            assert_eq!(
                (
                    serde_json::to_value(observations).expect("observations"),
                    full_present,
                    retained,
                    empty_present
                ),
                (expected, true, Ok::<Vec<u8>, String>(b"retained".to_vec()), false),
                "protocol bucket answers, counts, and storage match measured main"
            );
        });
    }

    #[test]
    #[serial_test::serial]
    fn protocol_bucket_context_isolation() {
        crate::app::gating_test_env::run_large_stack_test("protocol-bucket-context", || async {
            let root = rustfs_credentials::Credentials {
                access_key: "protocol-context-root".into(),
                secret_key: "protocol-context-secret".into(),
                status: "on".into(),
                ..Default::default()
            };
            let (_ambient_temp, ambient, _ambient_iam) = crate::protocols::policy_test::protocol_test_context(root.clone()).await;
            let published = crate::runtime_sources::publish_test_app_context(ambient.clone());
            assert!(std::sync::Arc::ptr_eq(&published, &ambient));
            let (_target_temp, target, _target_iam) = crate::protocols::policy_test::protocol_test_context(root.clone()).await;
            let bucket = "protocol-bucket-bound";
            for context in [&ambient, &target] {
                context
                    .object_store()
                    .make_bucket(bucket, &MakeBucketOptions::default())
                    .await
                    .expect("seed bound bucket");
            }
            ambient
                .object_store()
                .make_bucket("protocol-ambient-only", &MakeBucketOptions::default())
                .await
                .expect("seed ambient-only bucket");
            let slot = ServerContextSlot::new();
            assert!(slot.install(target.clone()));
            let backend = ProtocolStorageClient::new(FS::with_server_ctx(slot));
            backend.head_bucket(bucket, &root).await.expect("bound bucket exists");
            assert_eq!(
                backend
                    .head_bucket("protocol-ambient-only", &root)
                    .await
                    .expect_err("ambient bucket is not in target")
                    .code(),
                &rustfs_s3_types::S3ErrorCode::NoSuchBucket
            );
            backend.delete_bucket(bucket, &root).await.expect("delete bound bucket");
            let target_absent = target
                .object_store()
                .get_bucket_info(bucket, &Default::default())
                .await
                .is_err();
            let ambient_present = ambient
                .object_store()
                .get_bucket_info(bucket, &Default::default())
                .await
                .is_ok();
            assert_eq!((target_absent, ambient_present), (true, true), "only the bound store is changed");
            let uninstalled = ProtocolStorageClient::new(FS::with_server_ctx(ServerContextSlot::new()));
            let mut heads = Vec::new();
            for name in [bucket, ""] {
                heads.push((name, outcome(&uninstalled.head_bucket(name, &root).await)));
            }
            let deleted = outcome(&uninstalled.delete_bucket(bucket, &root).await);
            let present = ambient
                .object_store()
                .get_bucket_info(bucket, &Default::default())
                .await
                .is_ok();
            let actual = serde_json::json!({"heads":heads,"delete":deleted,"present":present});
            let expected: serde_json::Value = serde_json::from_str(r#"{"heads":[["protocol-bucket-bound",{"output":"Output { bucket_region: \"\" }"}],["",{"output":"Output { bucket_region: \"\" }"}]],"delete":{"output":"Output"},"present":false}"#).expect("measured compatibility context answers");
            assert_eq!(
                actual, expected,
                "uninstalled context preserves measured compatibility answers and store state"
            );
        });
    }

    #[tokio::test]
    async fn protocol_bucket_invalid_delete_names_match_legacy() {
        let backend = ProtocolStorageClient::new(FS::with_server_ctx(ServerContextSlot::new()));
        let credentials = rustfs_credentials::Credentials::default();
        let mut observations = Vec::new();
        for name in ["a", "a/b", "bad_bucket", "127.0.0.1", "bucket-"] {
            let before = count(S3Operation::DeleteBucket);
            let current = outcome(&backend.delete_bucket(name, &credentials).await);
            observations.push((name, current, count(S3Operation::DeleteBucket) - before));
        }
        let expected: serde_json::Value = serde_json::from_str(r#"[
["a",{"code":"InternalError","headers":"None","message":"failed to convert DeleteBucket bucket through the gateway compatibility seam: invalid BucketName: the name is shorter than the minimum length [rule: aws:bucketnamingrules]","status":500},0],
["a/b",{"code":"InternalError","headers":"None","message":"failed to convert DeleteBucket bucket through the gateway compatibility seam: invalid BucketName: the name contains a path separator [rule: aws:bucketnamingrules]","status":500},0],
["bad_bucket",{"code":"InternalError","headers":"None","message":"failed to convert DeleteBucket bucket through the gateway compatibility seam: invalid BucketName: the name contains a character the naming rules do not admit [rule: aws:bucketnamingrules]","status":500},0],
["127.0.0.1",{"code":"InternalError","headers":"None","message":"failed to convert DeleteBucket bucket through the gateway compatibility seam: invalid BucketName: the name uses a reserved form [rule: aws:bucketnamingrules]","status":500},0],
["bucket-",{"code":"InternalError","headers":"None","message":"failed to convert DeleteBucket bucket through the gateway compatibility seam: invalid BucketName: the name contains a character the naming rules do not admit [rule: aws:bucketnamingrules]","status":500},0]
]"#).expect("measured strict name refusals");
        assert_eq!(
            serde_json::to_value(observations).expect("delete name observations"),
            expected,
            "strict bucket refusal precedes dispatch and counting"
        );
    }
}

#[cfg(test)]
mod listing_tests {
    use super::*;
    use crate::runtime_sources::ServerContextSlot;
    use crate::storage_api::protocols::client::{
        BucketOperations, MakeBucketOptions, ObjectIO, StorageObjectOptions, StoragePutObjReader,
    };
    use rustfs_protocols::common::client::s3::StorageBackend;

    fn count(operation: rustfs_s3_ops::S3Operation) -> u64 {
        rustfs_io_metrics::s3_op_metrics_snapshot()
            .into_iter()
            .find(|row| row.op == operation.as_str())
            .map_or(0, |row| row.total)
    }

    fn input(bucket: &str) -> gateway_dto::ListObjectsV2Input {
        gateway_dto::ListObjectsV2Input {
            bucket: BucketName::new(bucket.to_owned()).expect("bucket"),
            ..Default::default()
        }
    }

    async fn listing_client() -> (tempfile::TempDir, rustfs_credentials::Credentials, ProtocolStorageClient) {
        let root = rustfs_credentials::Credentials {
            access_key: "listing-runtime-root".into(),
            secret_key: "listing-runtime-secret".into(),
            status: "on".into(),
            ..Default::default()
        };
        let (temp, context, _iam) = crate::protocols::policy_test::protocol_test_context(root.clone()).await;
        let ambient = crate::runtime_sources::publish_test_app_context(context.clone());
        assert!(std::sync::Arc::ptr_eq(&ambient, &context));
        let store = context.object_store();
        let bucket = "listing-runtime";
        for name in [bucket, "a-s3alias"] {
            store
                .make_bucket(name, &MakeBucketOptions::default())
                .await
                .expect("create bucket");
        }
        for key in ["a space&/one", "b", "dir/child"] {
            store
                .put_object(
                    bucket,
                    key,
                    &mut StoragePutObjReader::from_vec(b"data".to_vec()),
                    &StorageObjectOptions::default(),
                )
                .await
                .expect("seed object");
        }
        let slot = ServerContextSlot::new();
        assert!(slot.install(context));
        let backend = ProtocolStorageClient::new(FS::with_server_ctx(slot));
        (temp, root, backend)
    }

    #[test]
    #[serial_test::serial]
    fn protocol_listing_pages_and_encoding() {
        crate::app::gating_test_env::run_large_stack_test("protocol-listing-pages", || async {
            let (_temp, root, backend) = listing_client().await;
            let bucket = "listing-runtime";
            let mut first = input(bucket);
            first.max_keys = Some(1);
            first.encoding_type = Some(gateway_dto::EncodingType::URL);
            let before = count(rustfs_s3_ops::S3Operation::ListObjectsV2);
            let first = backend.list_objects_v2(first, &root).await.expect("first page");
            assert_eq!(count(rustfs_s3_ops::S3Operation::ListObjectsV2) - before, 1);
            assert!(first.is_truncated);
            assert_eq!(first.contents[0].key.as_str(), "a%20space%26/one");
            assert!(first.contents[0].e_tag.is_some());
            let token = first.next_continuation_token.expect("next page token");
            let mut second = input(bucket);
            second.continuation_token = Some(token.clone());
            let second = backend.list_objects_v2(second, &root).await.expect("second page");
            assert_eq!(second.continuation_token.as_ref(), Some(&token));
            assert_eq!(
                second.contents.iter().map(|object| object.key.as_str()).collect::<Vec<_>>(),
                ["b", "dir/child"]
            );
            let mut delimited = input(bucket);
            delimited.delimiter = Some("/".into());
            let delimited = backend.list_objects_v2(delimited, &root).await.expect("delimited page");
            assert_eq!(
                delimited
                    .common_prefixes
                    .iter()
                    .map(|prefix| prefix.prefix.as_str())
                    .collect::<Vec<_>>(),
                ["a space&/", "dir/"]
            );
        });
    }

    #[test]
    #[serial_test::serial]
    fn protocol_listing_refusals_and_limits() {
        crate::app::gating_test_env::run_large_stack_test("protocol-listing-errors", || async {
            let (_temp, root, backend) = listing_client().await;
            let bucket = "listing-runtime";
            let mut observations = Vec::new();
            for case in ["missing", "negative", "token", "zero", "large"] {
                let mut request = input(if case == "missing" { "listing-absent" } else { bucket });
                match case {
                    "negative" => request.max_keys = Some(-1),
                    "token" => request.continuation_token = Some(OpaqueString::from("%%%")),
                    "zero" => request.max_keys = Some(0),
                    "large" => request.max_keys = Some(1001),
                    _ => {}
                }
                let before = count(rustfs_s3_ops::S3Operation::ListObjectsV2);
                let result = backend.list_objects_v2(request, &root).await;
                let observed = match result {
                    Ok(output) => format!("ok:{}:{}:{}", output.max_keys, output.contents.len(), output.is_truncated),
                    Err(error) => format!("{:?}:{:?}:{:?}", error.code(), error.status_code(), error.message()),
                };
                observations.push((case, observed, count(rustfs_s3_ops::S3Operation::ListObjectsV2) - before));
            }
            assert_eq!(
                observations,
                vec![
                    (
                        "missing",
                        r#"NoSuchBucket:Some(404):Some("The specified bucket does not exist")"#.to_owned(),
                        1
                    ),
                    ("negative", r#"InvalidArgument:Some(400):Some("Invalid max keys")"#.to_owned(), 1),
                    ("token", r#"InvalidArgument:Some(400):Some("Invalid continuation token")"#.to_owned(), 1),
                    ("zero", "ok:0:0:false".to_owned(), 1),
                    ("large", "ok:1000:3:false".to_owned(), 1),
                ]
            );
            for credentials in [
                rustfs_credentials::Credentials::default(),
                rustfs_credentials::Credentials {
                    access_key: "listing-no-policy".into(),
                    secret_key: "listing-no-policy-secret".into(),
                    status: "on".into(),
                    ..Default::default()
                },
            ] {
                let before = count(rustfs_s3_ops::S3Operation::ListBuckets);
                let result = backend.list_buckets(&credentials).await;
                let error = result.expect_err("bucket discovery denied");
                assert_eq!(error.code(), &rustfs_s3_types::S3ErrorCode::AccessDenied);
                assert_eq!(error.message(), Some("Access Denied"));
                assert_eq!(count(rustfs_s3_ops::S3Operation::ListBuckets) - before, 1);
            }
        });
    }

    #[test]
    #[serial_test::serial]
    fn protocol_listing_bucket_context_and_defaults() {
        crate::app::gating_test_env::run_large_stack_test("protocol-listing-context", || async {
            let (_temp, root, backend) = listing_client().await;
            let bucket = "listing-runtime";
            let before = count(rustfs_s3_ops::S3Operation::ListBuckets);
            let buckets = backend.list_buckets(&root).await.expect("list buckets");
            assert_eq!(count(rustfs_s3_ops::S3Operation::ListBuckets) - before, 1);
            let names: Vec<_> = buckets.buckets.iter().map(|bucket| bucket.name.as_str()).collect();
            assert_eq!(names, [bucket]);
            assert_eq!(buckets.owner.id, crate::app::dto_values::owner().id);
            let (_other_temp, other_context, _other_iam) =
                crate::protocols::policy_test::protocol_test_context(root.clone()).await;
            other_context
                .object_store()
                .make_bucket("listing-context-other", &MakeBucketOptions::default())
                .await
                .expect("create other bucket");
            let other_slot = ServerContextSlot::new();
            assert!(other_slot.install(other_context));
            let other = ProtocolStorageClient::new(FS::with_server_ctx(other_slot));
            let other_buckets = other.list_buckets(&root).await.expect("bound bucket inventory");
            assert_eq!(
                other_buckets
                    .buckets
                    .iter()
                    .map(|bucket| bucket.name.as_str())
                    .collect::<Vec<_>>(),
                ["listing-context-other"]
            );
            let uninstalled = ProtocolStorageClient::new(FS::with_server_ctx(ServerContextSlot::new()));
            let uninstalled_buckets = uninstalled.list_buckets(&root).await.expect("ambient bucket inventory");
            assert_eq!(
                uninstalled_buckets
                    .buckets
                    .iter()
                    .map(|bucket| bucket.name.as_str())
                    .collect::<Vec<_>>(),
                [bucket]
            );
        });
    }

    #[test]
    fn protocol_listing_output_matches_legacy_projection() {
        use rustfs_gateway_types::{ETag, ObjectKey, Timestamp};
        let mut observations = Vec::new();
        let tags = [None, Some("abc".to_owned()), Some("a&\"b".to_owned()), Some(String::new())]
            .into_iter()
            .chain((0_u8..32).map(|byte| Some(char::from(byte).to_string())));
        let cases = tags
            .map(|tag| ("listing-output", "dir a/key&".to_owned(), tag, false))
            .chain([
                ("a-s3alias", "key".to_owned(), Some("abc".to_owned()), false),
                ("listing-output", "é".repeat(512), Some("abc".to_owned()), false),
                ("listing-output", "key".to_owned(), Some("abc".to_owned()), true),
                ("listing-output", "key".to_owned(), Some("a&b".to_owned()), true),
            ]);
        for (name, key, tag, weak) in cases {
            let mut output = gateway_dto::ListObjectsV2Output {
                name: crate::app::dto_values::stored_bucket_name(name).expect("stored bucket"),
                contents: vec![gateway_dto::Object {
                    key: ObjectKey::new(key.clone()).expect("key"),
                    e_tag: tag.as_ref().map(|value| {
                        if weak {
                            ETag::new_weak(value.clone()).expect("weak tag")
                        } else {
                            ETag::from_stored(value.clone())
                        }
                    }),
                    last_modified: Timestamp::from_secs_nanos(1700000000, 123456789).expect("timestamp"),
                    ..Default::default()
                }],
                encoding_type: Some(gateway_dto::EncodingType::URL),
                common_prefixes: vec![gateway_dto::CommonPrefix {
                    prefix: "prefix a/sub&".to_owned(),
                }],
                rustfs_on_demand_migration_list: Some("local-only".to_owned()),
                ..Default::default()
            };
            let current = owned::listing_output(output.clone());
            output.rustfs_on_demand_migration_list = None;
            let old = gateway_s3s_compat::generated::ops::list_objects_v2::output_to_s3s(output)
                .map_err(|error| conversion_error("ListObjectsV2 output", error))
                .and_then(|mut old| {
                    for object in old.contents.iter_mut().flatten() {
                        if let Some(key) = &mut object.key {
                            *key = key
                                .split('/')
                                .map(|part| urlencoding::encode(part).into_owned())
                                .collect::<Vec<_>>()
                                .join("/");
                        }
                        if let Some(s3s::dto::ETag::Strong(value) | s3s::dto::ETag::Weak(value)) = &mut object.e_tag {
                            *value = quick_xml::escape::escape(value.as_str()).into_owned();
                        }
                    }
                    for prefix in old.common_prefixes.iter_mut().flatten() {
                        if let Some(value) = &mut prefix.prefix {
                            *value = value
                                .split('/')
                                .map(|part| urlencoding::encode(part).into_owned())
                                .collect::<Vec<_>>()
                                .join("/");
                        }
                    }
                    gateway_s3s_compat::generated::ops::list_objects_v2::output_from_s3s(old)
                        .map_err(|error| error.to_string())
                        .and_then(require_listed_etags)
                        .map_err(|error| conversion_error("ListObjectsV2 output", error))
                });
            let describe = |result: rustfs_s3_types::S3Result<gateway_dto::ListObjectsV2Output>| match result {
                Ok(output) => format!("{output:?}"),
                Err(error) => format!("{:?}:{:?}:{:?}", error.code(), error.status_code(), error.message()),
            };
            observations.push(((name, key, tag, weak), describe(old), describe(current)));
        }
        let mismatches: Vec<_> = observations.into_iter().filter(|(_, old, current)| old != current).collect();
        assert!(mismatches.is_empty(), "listing projection mismatches: {mismatches:#?}");
    }

    #[test]
    fn protocol_listing_bucket_projection_preserves_strict_names_and_milliseconds() {
        let output = gateway_dto::ListBucketsOutput {
            buckets: ["listing-output", "a-s3alias", "sthree-x", "x--ol-s3", "x--x-s3"]
                .into_iter()
                .map(|name| gateway_dto::Bucket {
                    name: crate::app::dto_values::stored_bucket_name(name).expect("stored bucket name"),
                    creation_date: rustfs_gateway_types::Timestamp::from_secs_nanos(1700000000, 123456789).expect("timestamp"),
                    ..Default::default()
                })
                .collect(),
            owner: crate::app::dto_values::owner(),
            ..Default::default()
        };
        let old = gateway_s3s_compat::generated::ops::list_buckets::output_to_s3s(output.clone()).expect("legacy bucket output");
        let old = list_buckets_output_from_s3s(old).expect("legacy bucket projection");
        let current = owned::bucket_listing_output(output);
        assert_eq!(format!("{old:?}"), format!("{current:?}"));
        assert_eq!(
            current.buckets.iter().map(|bucket| bucket.name.as_str()).collect::<Vec<_>>(),
            ["listing-output"]
        );
        assert_eq!(current.buckets[0].creation_date.subsec_nanos(), 123000000);
    }
    #[cfg(any(feature = "webdav", feature = "sftp"))]
    #[test]
    fn protocol_listing_session_credential_projection_matches_legacy() {
        for secret in ["secret", ""] {
            for token in [None, Some("request-session-token")] {
                let credentials = rustfs_credentials::Credentials {
                    access_key: "listing-session".into(),
                    secret_key: secret.into(),
                    session_token: "identity-token".into(),
                    ..Default::default()
                };
                let session = SessionContext::new(
                    rustfs_protocols::common::ProtocolPrincipal::new(std::sync::Arc::new(rustfs_policy::auth::UserIdentity {
                        credentials: credentials.clone(),
                        ..Default::default()
                    })),
                    rustfs_protocols::Protocol::WebDav,
                    "127.0.0.1".parse().expect("loopback"),
                );
                let mut headers = HeaderMap::new();
                if let Some(token) = token {
                    headers.insert("x-amz-security-token", token.parse().expect("token header"));
                }
                let mut legacy = ProtocolStorageClient::create_request(
                    ListBucketsInput::default(),
                    Method::GET,
                    http::Uri::from_static("/"),
                    RequestParams {
                        bucket: None,
                        object: None,
                        credentials: &credentials,
                    },
                )
                .expect("legacy request");
                legacy.headers = headers.clone();
                let legacy = crate::app::RequestEnvelope::from_s3s(&legacy);
                let current =
                    session_list_buckets_envelope(ServerContextSlot::new(), &session, &headers, true, credentials, false);
                let legacy = legacy.credentials().expect("legacy credentials");
                let current = current.credentials().expect("owned credentials");
                assert_eq!(
                    (legacy.access_key(), legacy.secret_key(), legacy.session_token()),
                    (current.access_key(), current.secret_key(), current.session_token())
                );
            }
        }
    }
}
