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
use rustfs_gateway_types::{BucketName, OpaqueString};
use rustfs_protocols::common::SessionContext;
#[cfg(feature = "webdav")]
use rustfs_protocols::common::client::s3::{BucketCapacity, SessionCapacityView};
use rustfs_protocols::common::gateway::{AuthorizationError, S3Action, is_operation_supported};
use rustfs_trusted_proxies::ClientInfo;
use rustfs_utils::MaskedAccessKey;
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
fn session_list_buckets_request(
    input: ListBucketsInput,
    session_context: &SessionContext,
    request_headers: &HeaderMap,
    secure_transport: bool,
    credentials: rustfs_credentials::Credentials,
    is_owner: bool,
) -> S3Request<ListBucketsInput> {
    let request_credentials = s3s::auth::Credentials {
        access_key: credentials.access_key.clone(),
        secret_key: credentials.secret_key.clone().into(),
    };
    let mut extensions = http::Extensions::default();
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

    S3Request {
        input,
        method: Method::GET,
        uri: http::Uri::from_static("/"),
        headers: request_headers.clone(),
        extensions,
        credentials: Some(request_credentials),
        region: None,
        service: None,
        trailing_headers: None,
    }
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
        credentials: &rustfs_credentials::Credentials,
    ) -> Result<gateway_dto::HeadBucketOutput, Self::Error> {
        trace_protocol_request("head_bucket", Some(bucket), None);

        let input = HeadBucketInput::builder().bucket(bucket.to_string()).build().map_err(|e| {
            s3s::S3Error::with_message(s3s::S3ErrorCode::InvalidRequest, format!("Failed to build HeadBucketInput: {}", e))
        })?;

        let uri = build_bucket_uri(bucket, &[])?;
        let req = Self::create_request(
            input,
            Method::HEAD,
            uri,
            RequestParams {
                bucket: Some(bucket.to_string()),
                object: None,
                credentials,
            },
        )?;

        match self.fs.head_bucket(req).await {
            Ok(response) => gateway_s3s_compat::generated::ops::head_bucket::output_from_s3s(response.output)
                .map_err(|e| conversion_error("HeadBucket output", e)),
            Err(e) => Err(e.into()),
        }
    }

    async fn list_objects_v2(
        &self,
        input: gateway_dto::ListObjectsV2Input,
        credentials: &rustfs_credentials::Credentials,
    ) -> Result<gateway_dto::ListObjectsV2Output, Self::Error> {
        trace_protocol_request("list_objects_v2", Some(input.bucket.as_str()), None);

        let bucket = input.bucket.as_str().to_owned();
        let uri = build_list_objects_uri(&input)?;
        let input = gateway_s3s_compat::generated::ops::list_objects_v2::input_to_s3s(input)
            .map_err(|e| conversion_error("ListObjectsV2 input", e))?;
        let req = Self::create_request(
            input,
            Method::GET,
            uri,
            RequestParams {
                bucket: Some(bucket),
                object: None,
                credentials,
            },
        )?;

        match self.fs.list_objects_v2(req).await {
            Ok(response) => gateway_s3s_compat::generated::ops::list_objects_v2::output_from_s3s(response.output)
                .map_err(|e| e.to_string())
                .and_then(require_listed_etags)
                .map_err(|e| conversion_error("ListObjectsV2 output", e)),
            Err(e) => Err(e.into()),
        }
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

        let input = ListBucketsInput::builder().build().map_err(|e| {
            s3s::S3Error::with_message(s3s::S3ErrorCode::InvalidRequest, format!("Failed to build ListBucketsInput: {}", e))
        })?;

        let req = Self::create_request(
            input,
            Method::GET,
            http::Uri::from_static("/"),
            RequestParams {
                bucket: None,
                object: None,
                credentials,
            },
        )?;

        match self.fs.list_buckets(req).await {
            Ok(response) => list_buckets_output_from_s3s(response.output).map_err(|e| conversion_error("ListBuckets output", e)),
            Err(e) => Err(e.into()),
        }
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

        let input = ListBucketsInput::builder().build().map_err(|e| {
            s3s::S3Error::with_message(s3s::S3ErrorCode::InvalidRequest, format!("Failed to build ListBucketsInput: {}", e))
        })?;
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
        let mut request =
            session_list_buckets_request(input, session_context, request_headers, secure_transport, credentials, is_owner);
        request.extensions.insert(self.fs.server_ctx().clone());
        let response = self.fs.list_buckets(request).await?;
        list_buckets_output_from_s3s(response.output).map_err(|e| conversion_error("ListBuckets output", e))
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

        let input = CreateBucketInput::builder().bucket(bucket.to_string()).build().map_err(|e| {
            s3s::S3Error::with_message(s3s::S3ErrorCode::InvalidRequest, format!("Failed to build CreateBucketInput: {}", e))
        })?;

        let uri = build_bucket_uri(bucket, &[])?;
        let req = Self::create_request(
            input,
            Method::PUT,
            uri,
            RequestParams {
                bucket: Some(bucket.to_string()),
                object: None,
                credentials,
            },
        )?;

        match self.fs.create_bucket(req).await {
            Ok(response) => gateway_s3s_compat::generated::ops::create_bucket::output_from_s3s(response.output)
                .map_err(|e| conversion_error("CreateBucket output", e)),
            Err(e) => Err(e.into()),
        }
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
        let gateway_input = gateway_dto::DeleteBucketInput {
            bucket: rustfs_gateway_types::BucketName::new(bucket.to_owned())
                .map_err(|e| conversion_error("DeleteBucket bucket", e))?,
            expected_bucket_owner: None,
        };
        let context = gateway_request_context(Method::DELETE, &uri, HeaderMap::new());
        let wire = RequestWire::of(&context);
        let input = gateway_s3s_compat::generated::ops::delete_bucket::input_to_s3s(gateway_input, &wire)
            .map_err(|e| conversion_error("DeleteBucket input", e))?;
        let req = Self::create_request(
            input,
            Method::DELETE,
            uri,
            RequestParams {
                bucket: Some(bucket.to_string()),
                object: None,
                credentials,
            },
        )?;

        match self.fs.delete_bucket(req).await {
            Ok(response) => gateway_s3s_compat::generated::ops::delete_bucket::output_from_s3s(response.output)
                .map_err(|e| conversion_error("DeleteBucket output", e)),
            Err(e) => Err(e.into()),
        }
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
        let context = gateway_request_context(Method::POST, &uri, HeaderMap::new());
        let wire = RequestWire::of(&context);
        let input = gateway_s3s_compat::generated::ops::create_multipart_upload::input_to_s3s(input, &wire)
            .map_err(|e| conversion_error("CreateMultipartUpload input", e))?;

        let req = Self::create_request(
            input,
            Method::POST,
            uri,
            RequestParams {
                bucket: Some(bucket),
                object: Some(key),
                credentials,
            },
        )?;

        match self.fs.create_multipart_upload(req).await {
            Ok(response) => gateway_s3s_compat::generated::ops::create_multipart_upload::output_from_s3s(response.output)
                .map_err(|e| conversion_error("CreateMultipartUpload output", e)),
            Err(e) => Err(e.into()),
        }
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
        let input = gateway_s3s_compat::generated::ops::complete_multipart_upload::input_to_s3s(input)
            .map_err(|e| conversion_error("CompleteMultipartUpload input", e))?;
        let upload_id = input.upload_id.clone();
        let uri = build_object_uri(&bucket, &key, &[("uploadId", Some(upload_id.as_str()))])?;

        let req = Self::create_request(
            input,
            Method::POST,
            uri,
            RequestParams {
                bucket: Some(bucket),
                object: Some(key),
                credentials,
            },
        )?;

        match self.fs.complete_multipart_upload(req).await {
            Ok(response) => gateway_s3s_compat::generated::ops::complete_multipart_upload::output_from_s3s(response.output)
                .map_err(|e| conversion_error("CompleteMultipartUpload output", e)),
            Err(e) => Err(e.into()),
        }
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
        let input = gateway_s3s_compat::generated::ops::abort_multipart_upload::input_to_s3s(input)
            .map_err(|e| conversion_error("AbortMultipartUpload input", e))?;
        let upload_id = input.upload_id.clone();
        let uri = build_object_uri(&bucket, &key, &[("uploadId", Some(upload_id.as_str()))])?;

        let req = Self::create_request(
            input,
            Method::DELETE,
            uri,
            RequestParams {
                bucket: Some(bucket),
                object: Some(key),
                credentials,
            },
        )?;

        match self.fs.abort_multipart_upload(req).await {
            Ok(response) => gateway_s3s_compat::generated::ops::abort_multipart_upload::output_from_s3s(response.output)
                .map_err(|e| conversion_error("AbortMultipartUpload output", e)),
            Err(e) => Err(e.into()),
        }
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
        let request = session_list_buckets_request(
            ListBucketsInput::default(),
            &session_context,
            &headers,
            true,
            credentials.clone(),
            true,
        );
        let request_info = request.extensions.get::<ReqInfo>().expect("request info should be present");
        let copied = request_info.cred.as_ref().expect("credentials should be present");
        let remote_addr = request
            .extensions
            .get::<Option<RemoteAddr>>()
            .and_then(Option::as_ref)
            .expect("remote address should be present");
        let client_info = request.extensions.get::<ClientInfo>().expect("client info should be present");

        assert_eq!(copied.access_key, credentials.access_key);
        assert_eq!(copied.secret_key, credentials.secret_key);
        assert_eq!(copied.session_token, credentials.session_token);
        assert_eq!(copied.parent_user, credentials.parent_user);
        assert_eq!(copied.groups, credentials.groups);
        assert_eq!(copied.claims, Some(claims));
        assert!(request_info.is_owner, "ownership comes from the server's credential validation");
        let request_credentials = request.credentials.as_ref().expect("request credentials");
        assert_eq!(request_credentials.access_key, credentials.access_key);
        assert_eq!(request_credentials.secret_key.expose(), credentials.secret_key);
        assert_eq!(remote_addr.0.ip(), source_ip);
        assert_eq!(client_info.real_ip, source_ip);
        assert_eq!(client_info.forwarded_proto.as_deref(), Some("https"));
        assert_eq!(request.headers.get("user-agent").expect("user agent"), "webdav-client");

        let insecure_request =
            session_list_buckets_request(ListBucketsInput::default(), &session_context, &headers, false, credentials, false);
        assert!(!insecure_request.extensions.get::<ReqInfo>().expect("request info").is_owner);
        let insecure_client_info = insecure_request
            .extensions
            .get::<ClientInfo>()
            .expect("client info should be present");
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
