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

#[cfg(test)]
mod authorization_test;
#[cfg(test)]
mod copy_test;
mod multipart;
mod owned;
#[cfg(test)]
mod upload_copy_test;

#[cfg(feature = "webdav")]
use crate::runtime_sources::current_object_store_handle;
use crate::shared_types::RemoteAddr;
#[cfg(feature = "webdav")]
use crate::storage_api::protocols::client::capacity;
use crate::storage_api::protocols::client::{MultipartStagingOperation, ReqInfo, RequestContext, authorize_request};
use http::{HeaderMap, Method};
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use rustfs_credentials;
use rustfs_gateway_types::dto as gateway_dto;
#[cfg(test)]
use rustfs_gateway_types::{BucketName, OpaqueString};
use rustfs_protocols::common::SessionContext;
use rustfs_protocols::common::client::s3::MultipartRequest;
#[cfg(feature = "webdav")]
use rustfs_protocols::common::client::s3::{BucketCapacity, SessionCapacityView};
use rustfs_protocols::common::gateway::{AuthorizationError, S3Action, is_operation_supported};
use rustfs_trusted_proxies::ClientInfo;
use rustfs_utils::MaskedAccessKey;
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

fn parse_protocol_uri(uri: String, context: String) -> rustfs_s3_types::S3Result<http::Uri> {
    uri.parse().map_err(|e| {
        rustfs_s3_types::S3Error::with_message(
            rustfs_s3_types::S3ErrorCode::InvalidRequest,
            format!("invalid URI for {context}: {e}"),
        )
    })
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
#[cfg(test)]
fn format_copy_source(bucket: &str, key: &str) -> String {
    format!("/{bucket}/{}", urlencoding::encode(key))
}

/// Refuses a listing entry without an ETag, as the compatibility seam did while `Object.ETag` was required.
fn require_listed_etags(output: gateway_dto::ListObjectsV2Output) -> Result<gateway_dto::ListObjectsV2Output, String> {
    if output.contents.iter().any(|object| object.e_tag.is_none()) {
        return Err("ListObjectsV2 contents entry has no e_tag".to_owned());
    }
    Ok(output)
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

fn build_bucket_uri(bucket: &str, query: &[(&str, Option<&str>)]) -> rustfs_s3_types::S3Result<http::Uri> {
    let mut uri = format!("/{}", encode_path_segment(bucket));
    let mut first = true;
    for (key, value) in query {
        append_query_param(&mut uri, &mut first, key, *value);
    }
    parse_protocol_uri(uri, format!("bucket={bucket}"))
}

fn build_object_uri(bucket: &str, key: &str, query: &[(&str, Option<&str>)]) -> rustfs_s3_types::S3Result<http::Uri> {
    let mut uri = format!("/{}/{}", encode_path_segment(bucket), encode_object_key_path(key));
    let mut first = true;
    for (query_key, value) in query {
        append_query_param(&mut uri, &mut first, query_key, *value);
    }
    parse_protocol_uri(uri, format!("bucket={bucket} key={key}"))
}

fn build_list_objects_uri(input: &gateway_dto::ListObjectsV2Input) -> rustfs_s3_types::S3Result<http::Uri> {
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

/// Rejects reserved internal system buckets (`.rustfs.sys`, `.minio.sys`, and their sub-paths)
/// at the protocol boundary before IAM or policy evaluation runs.
fn is_reserved_meta_bucket(bucket: &str) -> bool {
    let trimmed = bucket.trim();
    let bytes = trimmed.as_bytes();
    (bytes.len() >= 11 && bytes[..11].eq_ignore_ascii_case(b".rustfs.sys"))
        || (bytes.len() >= 10 && bytes[..10].eq_ignore_ascii_case(b".minio.sys"))
}

/// The policy action a protocol operation is authorized as; HeadBucket is judged as ListBucket.
fn protocol_policy_action(action: &S3Action) -> rustfs_policy::policy::action::Action {
    if matches!(action, S3Action::HeadBucket) {
        S3Action::ListBucket.into()
    } else {
        action.clone().into()
    }
}

/// Protocol storage client that implements the StorageBackend trait
#[derive(Clone, Debug)]
pub struct ProtocolStorageClient {
    /// This server's late-bound application-context slot.
    server_ctx: std::sync::Arc<crate::runtime_sources::ServerContextSlot>,
}

impl ProtocolStorageClient {
    /// Create a protocol storage client bound to this server's context slot.
    pub fn new(server_ctx: std::sync::Arc<crate::runtime_sources::ServerContextSlot>) -> Self {
        rustfs_io_metrics::init_s3_metrics();
        rustfs_io_metrics::init_list_objects_metrics();
        Self { server_ctx }
    }

    async fn authorize_protocol_request(
        &self,
        session: &SessionContext,
        action: &S3Action,
        bucket: &str,
        object: Option<&str>,
        listing: Option<&gateway_dto::ListObjectsV2Input>,
    ) -> rustfs_s3_types::S3Result<()> {
        if is_reserved_meta_bucket(bucket) {
            return Err(rustfs_s3_types::s3_error!(AccessDenied, "Access Denied"));
        }
        let context = self.server_ctx.installed_app_context().ok_or_else(|| {
            rustfs_s3_types::S3Error::with_message(
                rustfs_s3_types::S3ErrorCode::InternalError,
                "protocol server application context is not initialized",
            )
        })?;
        let (credentials, is_owner) =
            crate::auth::check_key_valid_with_context("", session.access_key(), Some(context.as_ref())).await?;
        let mut envelope =
            self.protocol_authorization_envelope(session, action, bucket, object, listing, (credentials, is_owner))?;
        authorize_request(&mut envelope, protocol_policy_action(action)).await
    }

    /// The request the policy engine judges for one protocol operation: the session's peer,
    /// transport and policy headers, the re-validated key and owner flag, and the operation's
    /// bucket, key and listing query.
    fn protocol_authorization_envelope(
        &self,
        session: &SessionContext,
        action: &S3Action,
        bucket: &str,
        object: Option<&str>,
        listing: Option<&gateway_dto::ListObjectsV2Input>,
        (credentials, is_owner): (rustfs_credentials::Credentials, bool),
    ) -> rustfs_s3_types::S3Result<crate::app::RequestEnvelope> {
        let uri = if let Some(input) = listing {
            build_list_objects_uri(input)?
        } else if let Some(key) = object {
            build_object_uri(bucket, key, &[])?
        } else {
            build_bucket_uri(bucket, &[])?
        };
        let request_credentials =
            crate::app::RequestCredentials::new(credentials.access_key.clone(), credentials.secret_key.clone(), None)
                .unwrap_or_else(|_| crate::app::RequestCredentials::without_secret_key(credentials.access_key.clone(), None));
        let mut extensions = http::Extensions::default();
        extensions.insert(ReqInfo {
            cred: Some(credentials),
            is_owner,
            keystone_session_present: None,
            bucket: Some(bucket.to_owned()),
            object: object.map(str::to_owned),
            version_id: None,
            replication_request_authorized: false,
            region: None,
            request_context: Some(RequestContext::fallback()),
            suppress_denial_log: false,
            suppress_denial_audit: false,
        });
        extensions.insert(self.server_ctx.clone());
        let remote_addr = std::net::SocketAddr::new(session.source_ip, 0);
        extensions.insert(Some(RemoteAddr(remote_addr)));
        let mut client_info = ClientInfo::direct(remote_addr);
        client_info.forwarded_proto = Some(if session.secure_transport { "https" } else { "http" }.to_owned());
        extensions.insert(client_info);
        // Preserve staging semantics before distinct operations collapse to PutObject.
        if matches!(action, S3Action::CreateMultipartUpload | S3Action::UploadPart) {
            extensions.insert(MultipartStagingOperation);
        }
        Ok(crate::app::RequestEnvelope::from_parts(
            crate::app::RequestHead {
                method: Method::GET,
                uri,
                headers: session.request_headers.clone(),
                credentials: Some(request_credentials),
                region: None,
                service: None,
            },
            extensions,
        ))
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
                rustfs_s3_types::S3ErrorCode::AccessDenied | rustfs_s3_types::S3ErrorCode::InvalidAccessKeyId => {
                    AuthorizationError::AccessDenied
                }
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
                rustfs_s3_types::S3ErrorCode::AccessDenied | rustfs_s3_types::S3ErrorCode::InvalidAccessKeyId => {
                    AuthorizationError::AccessDenied
                }
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
        session: &SessionContext,
        input: gateway_dto::ListObjectsV2Input,
    ) -> Result<gateway_dto::ListObjectsV2Output, Self::Error> {
        trace_protocol_request("list_objects_v2", Some(input.bucket.as_str()), None);

        if !is_operation_supported(session.protocol, &S3Action::ListBucket) {
            return Err(rustfs_s3_types::S3Error::new(rustfs_s3_types::S3ErrorCode::AccessDenied));
        }

        let bucket = input.bucket.as_str().to_owned();
        let context = self.server_ctx.installed_app_context().ok_or_else(|| {
            rustfs_s3_types::S3Error::with_message(
                rustfs_s3_types::S3ErrorCode::InternalError,
                "protocol server application context is not initialized",
            )
        })?;
        let (credentials, is_owner) =
            crate::auth::check_key_valid_with_context("", session.access_key(), Some(context.as_ref())).await?;
        let mut envelope = self.protocol_authorization_envelope(
            session,
            &S3Action::ListBucket,
            &bucket,
            None,
            Some(&input),
            (credentials, is_owner),
        )?;
        // A listing can be filtered per principal (such as table-bucket list isolation),
        // so authorization must run on the exact envelope handed to the list use case.
        crate::storage::access_owned::listing_multipart::authorize_list_objects_v2(&self.server_ctx, &mut envelope, &bucket)
            .await?;
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

        let context = self.server_ctx.installed_app_context().ok_or_else(|| {
            rustfs_s3_types::S3Error::with_message(
                rustfs_s3_types::S3ErrorCode::InternalError,
                "protocol server application context is not initialized",
            )
        })?;
        // The session identity is captured at login. Re-check the key as
        // authorize_protocol_request does for every other operation, so a key
        // disabled or expired since then stops listing buckets. An invalid key
        // is an access denial, as in authorize_operation.
        let (credentials, is_owner) =
            crate::auth::check_key_valid_with_context("", session_context.access_key(), Some(context.as_ref()))
                .await
                .map_err(|e| match e.code() {
                    rustfs_s3_types::S3ErrorCode::InvalidAccessKeyId => {
                        rustfs_s3_types::S3Error::new(rustfs_s3_types::S3ErrorCode::AccessDenied)
                    }
                    _ => e,
                })?;
        let envelope = session_list_buckets_envelope(
            self.server_ctx.clone(),
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
                    return Err(rustfs_s3_types::S3Error::with_message(
                        rustfs_s3_types::S3ErrorCode::InternalError,
                        format!("Failed to read bucket quota config: {e}"),
                    ));
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
        let input = owned::copy_input(input)?;
        let envelope = Self::create_envelope(
            Method::PUT,
            uri,
            RequestParams {
                bucket: Some(bucket),
                object: Some(key),
                credentials,
            },
        );
        rustfs_io_metrics::record_s3_op(rustfs_s3_ops::S3Operation::CopyObject);
        self.owned_usecase()
            .execute_copy_object_owned(envelope, input)
            .await
            .map_err(owned::copy_error)
            .and_then(owned::copy_output)
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
        request: MultipartRequest<gateway_dto::UploadPartInput>,
        credentials: &rustfs_credentials::Credentials,
    ) -> Result<gateway_dto::UploadPartOutput, Self::Error> {
        let (input, upload_id) = request.into_parts();
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
        let part_number = input.part_number.to_string();
        let uri = build_object_uri(
            &bucket,
            &key,
            &[
                ("partNumber", Some(part_number.as_str())),
                ("uploadId", Some(upload_id.as_str())),
            ],
        )?;
        let input = multipart::upload_part_input(input, upload_id);
        let mut headers = HeaderMap::new();
        if let Ok(value) = content_length.to_string().parse() {
            headers.insert("content-length", value);
        }
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
        rustfs_io_metrics::record_s3_op(rustfs_s3_ops::S3Operation::UploadPart);
        let helper = crate::app::storage_api::context::OperationHelper::audit_only_from_view(
            &envelope.audit_view(),
            rustfs_s3_ops::S3Operation::UploadPart,
        );
        let result = self
            .owned_multipart_usecase()
            .execute_upload_part_owned(envelope, input)
            .await
            .map_err(multipart::upload_part_error);
        let empty_headers = HeaderMap::new();
        let headers = result.as_ref().map_or(&empty_headers, |output| &output.headers);
        let _helper = helper.complete_owned(&result, http::StatusCode::OK, headers);
        result.and_then(multipart::upload_part_output)
    }

    async fn complete_multipart_upload(
        &self,
        request: MultipartRequest<gateway_dto::CompleteMultipartUploadInput>,
        credentials: &rustfs_credentials::Credentials,
    ) -> Result<gateway_dto::CompleteMultipartUploadOutput, Self::Error> {
        let (input, upload_id) = request.into_parts();
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
        request: MultipartRequest<gateway_dto::AbortMultipartUploadInput>,
        credentials: &rustfs_credentials::Credentials,
    ) -> Result<gateway_dto::AbortMultipartUploadOutput, Self::Error> {
        let (input, upload_id) = request.into_parts();
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
        request: MultipartRequest<gateway_dto::UploadPartCopyInput>,
        credentials: &rustfs_credentials::Credentials,
    ) -> Result<gateway_dto::UploadPartCopyOutput, Self::Error> {
        let input = request.input();
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
        let (uri, input) = multipart::prepare_upload_part_copy(request)?;

        let envelope = Self::create_envelope(
            Method::PUT,
            uri,
            RequestParams {
                bucket: Some(bucket),
                object: Some(key),
                credentials,
            },
        );
        rustfs_io_metrics::record_s3_op(rustfs_s3_ops::S3Operation::UploadPartCopy);
        let helper = crate::app::storage_api::context::OperationHelper::audit_only_from_view(
            &envelope.audit_view(),
            rustfs_s3_ops::S3Operation::UploadPartCopy,
        );
        let result = Box::pin(self.owned_multipart_usecase().execute_upload_part_copy_owned(envelope, input))
            .await
            .map_err(owned::copy_error);
        let _helper = helper.complete_owned(&result, http::StatusCode::OK, &HeaderMap::new());
        result.and_then(multipart::upload_part_copy_output)
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

    fn owned_bucket(source: Option<rustfs_gateway_types::CopySourceValue>) -> (String, String, Option<String>) {
        match source {
            Some(rustfs_gateway_types::CopySourceValue::Bucket { bucket, key, version_id }) => (bucket, key, version_id),
            other => panic!("expected a bucket source, got {other:?}"),
        }
    }

    #[test]
    fn legacy_copy_source_keeps_a_plain_bucket_key() {
        let source = owned::copy_source(bucket_source("bucket", "dir/object.txt", None)).expect("plain source");
        assert_eq!(owned_bucket(source), ("bucket".to_owned(), "dir/object.txt".to_owned(), None));
    }

    #[test]
    fn legacy_copy_source_keeps_keys_that_need_percent_encoding() {
        for key in ["%2F", "foo?versionId=1", "a b+c", "\u{4e2d}\u{6587}", "x&y=z#frag"] {
            let source = owned::copy_source(bucket_source("bucket", key, None)).expect("encoded source");
            assert_eq!(owned_bucket(source), ("bucket".to_owned(), key.to_owned(), None), "{key}");
        }
    }

    #[test]
    fn legacy_copy_source_keeps_a_version_id() {
        let source =
            owned::copy_source(bucket_source("bucket", "object", Some("3HL4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY"))).expect("versioned");
        assert_eq!(
            owned_bucket(source),
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
            let error = owned::copy_source(bucket_source(bucket, "object", None)).expect_err("invalid bucket source");
            assert_eq!(
                (error.code(), error.status_code(), error.message()),
                (
                    &rustfs_s3_types::S3ErrorCode::InternalError,
                    Some(http::StatusCode::INTERNAL_SERVER_ERROR),
                    Some(
                        "failed to convert CopyObject copy_source through the gateway compatibility seam: ParseAmzCopySourceError: InvalidBucketName"
                    )
                ),
                "{bucket}"
            );
        }
    }

    #[test]
    fn legacy_copy_source_rejects_an_overlong_key_as_before() {
        let key = "k".repeat(1025);
        let error = owned::copy_source(bucket_source("bucket", &key, None)).expect_err("overlong key source");
        assert_eq!(
            (error.code(), error.status_code(), error.message()),
            (
                &rustfs_s3_types::S3ErrorCode::InternalError,
                Some(http::StatusCode::INTERNAL_SERVER_ERROR),
                Some(
                    "failed to convert CopyObject copy_source through the gateway compatibility seam: ParseAmzCopySourceError: InvalidKey"
                )
            )
        );
    }

    #[test]
    fn protocol_copy_refuses_access_point_and_outpost_sources() {
        let access_point = owned::copy_source(rustfs_gateway_types::CopySourceValue::AccessPoint {
            partition: "aws".to_owned(),
            region: "us-east-1".to_owned(),
            account_id: "123456789012".to_owned(),
            access_point_name: "ap".to_owned(),
            key: "object".to_owned(),
            version_id: None,
        })
        .expect("access point source");
        assert!(access_point.is_none());

        let outpost = owned::copy_source(rustfs_gateway_types::CopySourceValue::Outpost {
            partition: "aws".to_owned(),
            region: "us-east-1".to_owned(),
            account_id: "123456789012".to_owned(),
            outpost_id: "op-01234567890123456/accesspoint/ap".to_owned(),
            key: "object".to_owned(),
            version_id: None,
        })
        .expect("outpost source");
        assert!(outpost.is_none());
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

        let envelope = ProtocolStorageClient::create_envelope(
            Method::GET,
            http::Uri::from_static("/bucket?list-type=2"),
            RequestParams {
                bucket: Some("bucket".to_owned()),
                object: None,
                credentials: &credentials,
            },
        );
        assert_eq!(envelope.method(), &Method::GET);
        assert_eq!(envelope.uri(), &http::Uri::from_static("/bucket?list-type=2"));
        assert!(envelope.headers().is_empty());
        let request_credentials = envelope.credentials().expect("request credentials");
        assert_eq!(
            (
                request_credentials.access_key(),
                request_credentials.secret_key(),
                request_credentials.session_token()
            ),
            ("service-account", Some("secret"), None)
        );
        let request_info = envelope.req_info().expect("owned request info");
        assert_eq!(request_info.bucket.as_deref(), Some("bucket"));
        assert_eq!(request_info.object, None);
        assert_eq!(request_info.version_id, None);
        assert!(!request_info.is_owner);
        assert!(request_info.request_context.is_some());
        assert!(!request_info.suppress_denial_log);
        assert!(!request_info.suppress_denial_audit);
        let copied = request_info.cred.as_ref().expect("credentials should be present");
        assert_eq!(copied.access_key, credentials.access_key);
        assert_eq!(copied.secret_key, credentials.secret_key);
        assert_eq!(copied.session_token, credentials.session_token);
        assert_eq!(copied.parent_user, credentials.parent_user);
        assert_eq!(copied.groups, credentials.groups);
        assert_eq!(copied.claims, Some(claims));
        assert!(copied.is_service_account());
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
            use rustfs_s3_types::S3ErrorCode;
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
            let backend = ProtocolStorageClient::new(slot);
            for (bucket, key, data, versioned, deleted_get_code) in [
                (
                    "protocol-version-oracle",
                    "versioned-key",
                    b"versioned data".as_slice(),
                    true,
                    S3ErrorCode::NoSuchKey,
                ),
                // RustFS 1.0.5 returns NoSuchVersion for a missing directory marker.
                ("protocol-marker-oracle", "folder/", b"".as_slice(), false, S3ErrorCode::NoSuchVersion),
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
                let current = backend.get_object(bucket, key, &root, None).await.expect("protocol GET");
                if versioned {
                    assert_eq!(current.last_modified.expect("LastModified").subsec_nanos(), 123_000_000);
                }
                assert_eq!(current.version_id.is_some(), versioned);
                assert_eq!(current.content_length, Some(data.len() as i64));
                let expected_version = current.version_id.clone();
                let expected_etag = current.e_tag.clone();
                let expected_last_modified = current.last_modified;
                let body = current
                    .body
                    .expect("GET body")
                    .into_body()
                    .collect()
                    .await
                    .expect("read body")
                    .to_bytes();
                assert_eq!(body.as_ref(), data);
                let current = backend.head_object(bucket, key, &root).await.expect("protocol HEAD");
                assert_eq!(current.last_modified, expected_last_modified);
                assert_eq!(current.version_id, expected_version);
                assert_eq!(current.e_tag, expected_etag);
                assert_eq!(current.content_length, Some(data.len() as i64));
                let deleted = backend.delete_object(bucket, key, &root).await.expect("protocol DELETE");
                if versioned {
                    assert_eq!(deleted.delete_marker, Some(true));
                }
                assert_eq!(deleted.version_id.is_some(), versioned);
                let current = backend
                    .get_object(bucket, key, &root, None)
                    .await
                    .expect_err("protocol GET deleted object");
                assert_eq!(current.code(), &deleted_get_code, "GET {bucket}/{key} after DELETE");
                assert_eq!(current.status_code(), Some(http::StatusCode::NOT_FOUND));
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
        let truncated = rustfs_gateway_types::Timestamp::from_secs_nanos(1_700_000_000, 123_000_000).expect("milliseconds");
        assert_eq!(
            (truncated.secs(), truncated.subsec_millis(), truncated.subsec_nanos()),
            (1_700_000_000, 123, 123_000_000)
        );
        let expected = Some(truncated);
        let current = (
            owned::get_output(get())
                .expect("protocol GET output")
                .object_lock_retain_until_date,
            owned::head_output(head())
                .expect("protocol HEAD output")
                .object_lock_retain_until_date,
        );
        assert_eq!(current, (expected, expected));
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

    fn expected_base_fields() -> serde_json::Value {
        serde_json::json!({
            "bucket": "protocol-put-oracle", "key": "object", "length": 4,
            "metadata": null, "specific_if_none_match": false,
            "retain_until": null,
            "cache_control": null, "content_disposition": null,
            "content_encoding": null, "content_language": null,
            "content_type": null, "expires": null, "tagging": null,
            "version_id": null, "server_side_encryption": null,
            "sse_customer_algorithm": null, "sse_customer_key": null,
            "sse_customer_key_md5": null, "ssekms_key_id": null,
            "content_md5": null, "legal_hold": null,
            "lock_mode": null, "storage_class": null,
            "website_redirect_location": null, "checksum_algorithm": null,
            "crc32": null, "crc32c": null, "crc64nvme": null,
            "sha1": null, "sha256": null,
        })
    }

    #[test]
    fn protocol_put_condition_grammar_matches_legacy() {
        const SPECIFIC_ETAGS: &[&str] = &[
            "\"*\"",
            "\"abc\"",
            "W/\"abc\"",
            "abc",
            "\"abc\", \"def\"",
            "\"\"",
            "\"a b\"",
            "\"a\\b\"",
            "\"a\"b\"",
            "\"a\t b\"",
            "\"\t\"",
        ];
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
                let expected = if value.is_empty() || value == "*" {
                    Ok(expected_base_fields())
                } else if SPECIFIC_ETAGS.contains(&value.as_str()) {
                    let mut expected = expected_base_fields();
                    expected["specific_if_none_match"] = serde_json::json!(none);
                    Ok(expected)
                } else {
                    let field = if none { "if_none_match" } else { "if_match" };
                    Err(conversion_error(
                        "PutObject input",
                        format!("{field}: not an entity-tag condition the s3s input can hold"),
                    )
                    .to_string())
                };
                let current = owned::put_input(make()).map(fields).map_err(|error| error.to_string());
                observations.push((value.clone(), none, current, expected));
            }
        }
        let mismatches: Vec<_> = observations
            .into_iter()
            .filter(|(_, _, current, expected)| current != expected)
            .collect();
        assert!(mismatches.is_empty(), "condition projection mismatches: {mismatches:#?}");
    }

    #[test]
    fn protocol_put_optional_values_match_legacy() {
        for value in ["", "value"] {
            let mut request = input();
            macro_rules! strings {
                ($($field:ident),+) => {$(
                    request.$field = Some(value.to_owned());
                )+};
            }
            strings!(
                cache_control,
                content_disposition,
                content_encoding,
                content_language,
                content_type,
                content_md5,
                tagging,
                sse_customer_algorithm,
                sse_customer_key_md5,
                ssekms_key_id,
                website_redirect_location
            );
            macro_rules! enums {
                ($($field:ident => $kind:ident),+) => {$(
                    request.$field = Some(gateway_dto::$kind::custom(value));
                )+};
            }
            enums!(
                storage_class => StorageClass,
                server_side_encryption => ServerSideEncryption,
                object_lock_mode => ObjectLockMode,
                object_lock_legal_hold_status => ObjectLockLegalHoldStatus,
                checksum_algorithm => ChecksumAlgorithm
            );
            request.expires = Some(OpaqueString::from(value));
            let mut expected = expected_base_fields();
            if !value.is_empty() {
                request.metadata.insert("key".into(), "value".into());
                for key in [
                    "cache_control",
                    "content_disposition",
                    "content_encoding",
                    "content_language",
                    "content_type",
                    "content_md5",
                    "tagging",
                    "sse_customer_algorithm",
                    "sse_customer_key_md5",
                    "ssekms_key_id",
                    "website_redirect_location",
                    "storage_class",
                    "server_side_encryption",
                    "lock_mode",
                    "legal_hold",
                    "checksum_algorithm",
                    "expires",
                ] {
                    expected[key] = serde_json::json!(value);
                }
                expected["metadata"] = serde_json::json!({ "key": "value" });
            }
            assert_eq!(fields(owned::put_input(request).expect("optional values")), expected);
        }
    }

    #[test]
    fn protocol_put_checksum_input_matches_legacy() {
        let mut observations = Vec::new();
        for (header, json_field, value) in [
            ("x-amz-checksum-crc32", "crc32", "AAAAAA=="),
            ("x-amz-checksum-crc32c", "crc32c", "AAAAAA=="),
            ("x-amz-checksum-crc64nvme", "crc64nvme", "AAAAAAAAAAA="),
            ("x-amz-checksum-sha1", "sha1", "AAAAAAAAAAAAAAAAAAAAAAAAAAA="),
            ("x-amz-checksum-sha256", "sha256", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
        ] {
            let make = || {
                let mut request = input();
                request.checksum_spec =
                    Some(rustfs_gateway_types::ChecksumSpec::parse_header(header, value).expect("test checksum"));
                request
            };
            let mut expected = expected_base_fields();
            expected[json_field] = serde_json::json!(value);
            observations.push((
                header,
                owned::put_input(make()).map(fields).map_err(|error| error.to_string()),
                Ok(expected),
            ));
        }
        let mismatches: Vec<_> = observations
            .into_iter()
            .filter(|(_, current, expected)| current != expected)
            .collect();
        assert!(mismatches.is_empty(), "PutObject checksum input mismatches: {mismatches:#?}");
    }

    #[test]
    fn protocol_put_retain_until_matches_legacy() {
        let mut observations = Vec::new();
        for (secs, nanos, pinned_nanos) in [
            (1_700_000_000, 123_456_789, Some("1700000000123456789")),
            (-1, 123_456_789, Some("-876543211")),
            (0, 123_456_789, Some("123456789")),
            (i64::MAX, 0, None),
        ] {
            let make = || {
                let mut request = input();
                request.object_lock_retain_until_date =
                    Some(rustfs_gateway_types::Timestamp::from_secs_nanos(secs, nanos).expect("instant"));
                request
            };
            let expected = match pinned_nanos {
                Some(nanos_str) => {
                    let mut fields = expected_base_fields();
                    fields["retain_until"] = serde_json::json!(nanos_str);
                    Ok(fields)
                }
                None => Err(conversion_error(
                    "PutObject input",
                    "object_lock_retain_until_date: an instant outside what the s3s input can hold",
                )
                .to_string()),
            };
            observations.push((
                secs,
                nanos,
                owned::put_input(make()).map(fields).map_err(|error| error.to_string()),
                expected,
            ));
        }
        let mismatches: Vec<_> = observations
            .into_iter()
            .filter(|(_, _, current, expected)| current != expected)
            .collect();
        assert!(mismatches.is_empty(), "retain-until projection mismatches: {mismatches:#?}");
    }

    #[test]
    fn protocol_put_output_matches_legacy() {
        use crate::app::object_usecase::PutObjectWriteOutput;
        enum ExpectedETag<'a> {
            Strong(&'a str),
            Weak(&'a str),
        }
        let mut observations = Vec::new();
        for (raw, tag) in [
            (None, None),
            (Some("abc"), Some(ExpectedETag::Strong("abc"))),
            (Some("\"abc\""), Some(ExpectedETag::Strong("abc"))),
            (Some("W/\"abc\""), Some(ExpectedETag::Weak("abc"))),
            (Some("W/\"\"abc\"\""), Some(ExpectedETag::Weak("\"abc\""))),
            (Some(""), Some(ExpectedETag::Strong(""))),
            (Some("\"\""), Some(ExpectedETag::Strong(""))),
            (Some("a\nb"), Some(ExpectedETag::Strong("a\nb"))),
        ] {
            let old = match tag {
                None => Err(conversion_error(
                    "PutObject output",
                    "e_tag: a PutObject answer carries an entity tag and the gateway output cannot omit one",
                )),
                Some(_) if matches!(raw, Some("" | "\"\"" | "a\nb")) => {
                    Err(conversion_error("PutObject output", "e_tag: not an entity tag the gateway can write"))
                }
                Some(tag) => {
                    let tag = match tag {
                        ExpectedETag::Weak(value) => rustfs_gateway_types::ETag::new_weak(value.trim_matches('"').to_owned()),
                        ExpectedETag::Strong(value) => rustfs_gateway_types::ETag::new(value.to_owned()),
                    }
                    .expect("recorded entity tag");
                    Ok(gateway_dto::PutObjectOutput {
                        e_tag: tag,
                        ..Default::default()
                    })
                }
            };
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
            match index {
                0 => {
                    current.checksum_crc32 = Some(checksum.to_owned());
                }
                1 => {
                    current.checksum_crc32c = Some(checksum.to_owned());
                }
                2 => {
                    current.checksum_crc64nvme = Some(checksum.to_owned());
                }
                3 => {
                    current.checksum_sha1 = Some(checksum.to_owned());
                }
                4 => {
                    current.checksum_sha256 = Some(checksum.to_owned());
                }
                _ => {
                    current.checksum_crc32 = Some(checksum.to_owned());
                    current.checksum_crc32c = Some(checksum.to_owned());
                }
            }
            let old = match (index, checksum) {
                (5, _) => Err(conversion_error(
                    "PutObject output",
                    "checksum_spec: the gateway output carries one checksum, so a second would be lost",
                )),
                (_, "bad") => Err(conversion_error(
                    "PutObject output",
                    "checksum_crc32: not a checksum value of this algorithm's width",
                )),
                _ => {
                    let header = [
                        "x-amz-checksum-crc32",
                        "x-amz-checksum-crc32c",
                        "x-amz-checksum-crc64nvme",
                        "x-amz-checksum-sha1",
                        "x-amz-checksum-sha256",
                    ][index];
                    Ok(gateway_dto::PutObjectOutput {
                        e_tag: rustfs_gateway_types::ETag::new("abc").expect("tag"),
                        checksum_spec: Some(
                            rustfs_gateway_types::ChecksumSpec::parse_header(header, checksum).expect("recorded checksum"),
                        ),
                        ..Default::default()
                    })
                }
            };
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
        let old = Ok(gateway_dto::PutObjectOutput {
            e_tag: rustfs_gateway_types::ETag::new("abc").expect("tag"),
            version_id: Some("version".to_owned()),
            expiration: Some("expiry".into()),
            server_side_encryption: Some(gateway_dto::ServerSideEncryption::custom("AES256")),
            sse_customer_algorithm: Some("AES256".to_owned()),
            sse_customer_key_md5: Some("md5".to_owned()),
            ssekms_key_id: Some("key-id".to_owned()),
            ..Default::default()
        });
        observations.push(("populated output".to_owned(), outcome(&old), outcome(&owned::put_output(current))));
        let mismatches: Vec<_> = observations.into_iter().filter(|(_, old, current)| old != current).collect();
        assert!(mismatches.is_empty(), "PutObject output mismatches: {mismatches:#?}");
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
        for (code, explicit, expected_status, expected_message) in [
            (
                S3ErrorCode::InvalidStorageClass,
                false,
                Some(400_u16),
                Some("The storage class you specified is not valid."),
            ),
            (
                S3ErrorCode::IncompleteBody,
                false,
                Some(400),
                Some("You did not provide the number of bytes specified by the Content-Length HTTP header"),
            ),
            (
                S3ErrorCode::UnexpectedContent,
                false,
                Some(400),
                Some("This request does not support content."),
            ),
            (S3ErrorCode::Custom("ProtocolPutUnmapped".into()), false, None, None),
            (S3ErrorCode::InvalidStorageClass, true, Some(418), Some("explicit protocol error")),
            (S3ErrorCode::IncompleteBody, true, Some(418), Some("explicit protocol error")),
            (S3ErrorCode::UnexpectedContent, true, Some(418), Some("explicit protocol error")),
            (S3ErrorCode::EntityTooLarge, true, Some(418), Some("explicit protocol error")),
            (S3ErrorCode::NotImplemented, true, Some(418), Some("explicit protocol error")),
            (
                S3ErrorCode::Custom("ProtocolPutUnmapped".into()),
                true,
                Some(418),
                Some("explicit protocol error"),
            ),
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
            let expected = serde_json::json!({
                "code": format!("{code:?}"),
                "status": expected_status,
                "message": expected_message,
                "headers": if explicit { "Some({\"x-protocol-test\": \"preserved\"})" } else { "None" },
            });
            let current = Err(owned::put_error(make()));
            observations.push((code, explicit, expected, outcome(&current)));
        }
        let mismatches: Vec<_> = observations
            .into_iter()
            .filter(|(_, _, expected, current)| expected != current)
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
        let backend = ProtocolStorageClient::new(slot);
        let mut observations = Vec::new();
        for (case, expected) in cases.iter().cloned() {
            let current_key = format!("current/{case}");
            let before = put_count();
            let current = backend.put_object(case_input(case, &current_key), &root).await;
            let current_count = put_count() - before;
            let state = object_state(&backend, &current_key, &root).await;
            let expected_count = u64::from(expected != Some(S3ErrorCode::InternalError));
            observations.push((
                case,
                outcome(&current),
                current.as_ref().err().map(|error| error.code().clone()),
                expected.clone(),
                state,
                expected.is_none().then(|| b"data".to_vec()),
                current_count,
                expected_count,
            ));
        }
        let mismatches: Vec<_> = observations
            .into_iter()
            .filter(
                |(_, _current, current_code, expected, state, expected_state, current_count, expected_count)| {
                    current_code != expected || state != expected_state || current_count != expected_count
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
            let backend = ProtocolStorageClient::new(slot);
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
            let uninstalled = ProtocolStorageClient::new(ServerContextSlot::new());
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
            let backend = ProtocolStorageClient::new(slot);
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
            let backend = ProtocolStorageClient::new(slot);
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
            let uninstalled = ProtocolStorageClient::new(ServerContextSlot::new());
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
        let backend = ProtocolStorageClient::new(ServerContextSlot::new());
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
        let backend = ProtocolStorageClient::new(slot);
        (temp, root, backend)
    }

    fn session_for(credentials: &rustfs_credentials::Credentials, source_ip: &str) -> SessionContext {
        SessionContext::new(
            rustfs_protocols::common::ProtocolPrincipal::new(std::sync::Arc::new(rustfs_policy::auth::UserIdentity {
                credentials: credentials.clone(),
                ..Default::default()
            })),
            rustfs_protocols::Protocol::WebDav,
            source_ip.parse().expect("valid IP"),
        )
    }

    #[test]
    #[serial_test::serial]
    fn protocol_listing_pages_and_encoding() {
        crate::app::gating_test_env::run_large_stack_test("protocol-listing-pages", || async {
            let (_temp, root, backend) = listing_client().await;
            let root_session = session_for(&root, "127.0.0.1");
            let bucket = "listing-runtime";
            let mut first = input(bucket);
            first.max_keys = Some(1);
            first.encoding_type = Some(gateway_dto::EncodingType::URL);
            let before = count(rustfs_s3_ops::S3Operation::ListObjectsV2);
            let first = backend.list_objects_v2(&root_session, first).await.expect("first page");
            assert_eq!(count(rustfs_s3_ops::S3Operation::ListObjectsV2) - before, 1);
            assert!(first.is_truncated);
            assert_eq!(first.contents[0].key.as_str(), "a%20space%26/one");
            assert!(first.contents[0].e_tag.is_some());
            let token = first.next_continuation_token.expect("next page token");
            let mut second = input(bucket);
            second.continuation_token = Some(token.clone());
            let second = backend.list_objects_v2(&root_session, second).await.expect("second page");
            assert_eq!(second.continuation_token.as_ref(), Some(&token));
            assert_eq!(
                second.contents.iter().map(|object| object.key.as_str()).collect::<Vec<_>>(),
                ["b", "dir/child"]
            );
            let mut delimited = input(bucket);
            delimited.delimiter = Some("/".into());
            let delimited = backend
                .list_objects_v2(&root_session, delimited)
                .await
                .expect("delimited page");
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
            let root_session = session_for(&root, "127.0.0.1");
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
                let result = backend.list_objects_v2(&root_session, request).await;
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
            let other = ProtocolStorageClient::new(other_slot);
            let other_buckets = other.list_buckets(&root).await.expect("bound bucket inventory");
            assert_eq!(
                other_buckets
                    .buckets
                    .iter()
                    .map(|bucket| bucket.name.as_str())
                    .collect::<Vec<_>>(),
                ["listing-context-other"]
            );
            let uninstalled = ProtocolStorageClient::new(ServerContextSlot::new());
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
            let refusal = match tag.as_deref() {
                None => Some("ListObjectsV2 contents entry has no e_tag"),
                Some("") => Some("e_tag: not an entity tag the gateway can write"),
                Some(tag) if tag.bytes().any(|byte| byte < 32 && byte != b'\r') => {
                    Some("e_tag: not an entity tag the gateway can write")
                }
                _ if key.len() == 1024 => Some("key: not an object key the gateway can write"),
                _ => None,
            };
            if let Some(reason) = refusal {
                let error = current.expect_err("original listing refusal");
                assert_eq!(
                    (error.code().as_str(), error.status_code(), error.message()),
                    (
                        "InternalError",
                        Some(http::StatusCode::INTERNAL_SERVER_ERROR),
                        Some(
                            format!("failed to convert ListObjectsV2 output through the gateway compatibility seam: {reason}")
                                .as_str()
                        )
                    )
                );
                continue;
            }
            let key = match key.as_str() {
                "dir a/key&" => "dir%20a/key%26",
                "key" => "key",
                _ => panic!("unrecorded listing key"),
            };
            let escaped = match tag.as_deref().expect("recorded tag") {
                "abc" => "abc",
                "a&\"b" => "a&amp;&quot;b",
                "a&b" => "a&amp;b",
                "\r" => "&#13;",
                _ => panic!("unrecorded listing tag"),
            };
            let object = &mut output.contents[0];
            object.key = ObjectKey::new(key).expect("legacy encoded key");
            object.e_tag = Some(if weak { ETag::new_weak(escaped) } else { ETag::new(escaped) }.expect("legacy tag"));
            object.last_modified = Timestamp::from_secs_nanos(1_700_000_000, 123_000_000).expect("legacy precision");
            output.common_prefixes[0].prefix = "prefix%20a/sub%26".into();
            output.rustfs_on_demand_migration_list = None;
            assert_eq!(format!("{:?}", current.expect("listing")), format!("{output:?}"), "{name} {tag:?}");
        }
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
        let expected = gateway_dto::ListBucketsOutput {
            buckets: vec![gateway_dto::Bucket {
                name: BucketName::new("listing-output").expect("public bucket"),
                creation_date: rustfs_gateway_types::Timestamp::from_secs_nanos(1_700_000_000, 123_000_000)
                    .expect("legacy wire precision"),
                ..Default::default()
            }],
            owner: crate::app::dto_values::owner(),
            ..Default::default()
        };
        let current = owned::bucket_listing_output(output);
        assert_eq!(format!("{expected:?}"), format!("{current:?}"));
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
                let envelope =
                    session_list_buckets_envelope(ServerContextSlot::new(), &session, &headers, true, credentials, false);
                assert_eq!((envelope.method(), envelope.uri()), (&Method::GET, &http::Uri::from_static("/")));
                let projected = envelope.credentials().expect("owned credentials");
                assert_eq!(
                    (projected.access_key(), projected.secret_key(), projected.session_token()),
                    ("listing-session", (!secret.is_empty()).then_some(secret), token)
                );
                let info = envelope.req_info().expect("request info");
                assert_eq!(info.cred.as_ref().expect("session credentials").session_token, "identity-token");
            }
        }
    }

    #[tokio::test]
    async fn protocol_client_rejects_reserved_meta_buckets_before_iam() {
        use rustfs_protocols::common::client::s3::StorageBackend as _;

        let client = ProtocolStorageClient::new(ServerContextSlot::new());
        let session = SessionContext::new(
            rustfs_protocols::common::ProtocolPrincipal::new(std::sync::Arc::new(rustfs_policy::auth::UserIdentity {
                credentials: rustfs_credentials::Credentials {
                    access_key: "wildcard-protocol-user".into(),
                    secret_key: "secret".into(),
                    status: "on".into(),
                    ..Default::default()
                },
                ..Default::default()
            })),
            rustfs_protocols::Protocol::WebDav,
            "127.0.0.1".parse().expect("loopback"),
        );

        for reserved in [
            ".rustfs.sys",
            ".rustfs.sys/multipart",
            ".minio.sys",
            ".RUSTFS.SYS",
            " .minio.sys ",
        ] {
            for action in [
                S3Action::GetObject,
                S3Action::PutObject,
                S3Action::DeleteObject,
                S3Action::HeadBucket,
            ] {
                let err = client
                    .authorize_operation(&session, &action, reserved, Some("config/iam/users/admin/identity.json"))
                    .await
                    .expect_err("reserved meta bucket must be denied at protocol boundary");
                assert!(
                    matches!(err, AuthorizationError::AccessDenied),
                    "{reserved} {action:?}: expected AccessDenied, got {err:?}"
                );
            }
        }

        // Control: an ordinary bucket reaches the uninitialized context check (IamUnavailable),
        // proving the guard only short-circuits reserved buckets.
        let err = client
            .authorize_operation(&session, &S3Action::GetObject, "ordinary-bucket", Some("file.txt"))
            .await
            .expect_err("uninitialized server context must report IamUnavailable");
        assert!(matches!(err, AuthorizationError::IamUnavailable), "expected IamUnavailable, got {err:?}");
    }

    #[test]
    #[serial_test::serial]
    fn protocol_listing_applies_table_bucket_list_isolation() {
        crate::app::gating_test_env::run_large_stack_test("protocol-listing-table-isolation", || async {
            use crate::table_catalog::{
                EcStoreTableCatalogObjectBackend, IdentifierSegment, Namespace, ObjectTableCatalogStore,
                TABLE_BUCKET_CATALOG_TYPE, TABLE_BUCKET_MARKER_CONFIG, TABLE_CATALOG_ENTRY_VERSION, TableBucketEntry,
                TableCatalogEntryState, TableCatalogStore as _, TableEntry, default_table_metadata_file_path,
                synthetic_namespace_entry, table_bucket_marker_json,
            };
            use rustfs_iam::store::UserType;
            use rustfs_madmin::{AccountStatus, AddOrUpdateUserReq};
            use rustfs_policy::policy::Policy;
            use serde_json::json;

            let root = rustfs_credentials::Credentials {
                access_key: "table-proto-root".into(),
                secret_key: "table-proto-root-secret".into(),
                status: "on".into(),
                ..Default::default()
            };
            let (_temp, context, iam) = crate::protocols::policy_test::protocol_test_context(root.clone()).await;
            let _ambient = crate::runtime_sources::publish_test_app_context(context.clone());
            let store = context.object_store();
            let bucket = "proto-table-wh";
            store
                .make_bucket(bucket, &MakeBucketOptions::default())
                .await
                .expect("create table bucket");
            store
                .update_bucket_metadata_config(
                    bucket,
                    TABLE_BUCKET_MARKER_CONFIG,
                    table_bucket_marker_json().expect("table marker"),
                )
                .await
                .expect("enable table bucket");

            let catalog = ObjectTableCatalogStore::new(EcStoreTableCatalogObjectBackend::new(store.clone()));
            catalog
                .put_table_bucket(TableBucketEntry {
                    version: TABLE_CATALOG_ENTRY_VERSION,
                    table_bucket: bucket.to_owned(),
                    catalog_type: TABLE_BUCKET_CATALOG_TYPE.into(),
                    warehouse_root: format!("s3://{bucket}/"),
                    state: TableCatalogEntryState::Active,
                    properties: Default::default(),
                    active_rename_id: None,
                    created_at: None,
                    updated_at: None,
                })
                .await
                .expect("register warehouse");
            let namespace = Namespace::parse("analytics").expect("namespace");
            catalog
                .create_namespace(synthetic_namespace_entry(bucket, &namespace))
                .await
                .expect("create namespace");

            for (table_name, table_id) in [("orders", "orders-id"), ("payroll", "payroll-id")] {
                let table = IdentifierSegment::parse(table_name).expect("table name");
                let metadata_path = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
                catalog
                    .create_table(TableEntry {
                        version: TABLE_CATALOG_ENTRY_VERSION,
                        table_bucket: bucket.to_owned(),
                        namespace: namespace.public_name(),
                        table: table.as_str().into(),
                        table_id: table_id.into(),
                        table_uuid: format!("{table_id}-uuid"),
                        format: "ICEBERG".into(),
                        format_version: 2,
                        warehouse_location: format!("s3://{bucket}/tables/{table_id}"),
                        metadata_location: metadata_path.clone(),
                        version_token: "token-v1".into(),
                        generation: 1,
                        state: TableCatalogEntryState::Active,
                        properties: Default::default(),
                        created_at: None,
                        updated_at: None,
                    })
                    .await
                    .expect("register table");
                for key in [metadata_path, format!("tables/{table_id}/data/part-0.parquet")] {
                    store
                        .put_object(
                            bucket,
                            &key,
                            &mut StoragePutObjReader::from_vec(b"table-bytes".to_vec()),
                            &StorageObjectOptions::default(),
                        )
                        .await
                        .expect("seed table object");
                }
            }
            store
                .put_object(
                    bucket,
                    "plain.txt",
                    &mut StoragePutObjReader::from_vec(b"plain".to_vec()),
                    &StorageObjectOptions::default(),
                )
                .await
                .expect("seed plain object");

            let create_user_with_policy = |name: &str, policy: serde_json::Value| {
                let iam = iam.clone();
                let name = name.to_owned();
                async move {
                    iam.create_user(
                        &name,
                        &AddOrUpdateUserReq {
                            secret_key: format!("{name}-secret-key"),
                            policy: None,
                            status: AccountStatus::Enabled,
                        },
                    )
                    .await
                    .expect("create user");
                    let parsed = Policy::parse_config(&serde_json::to_vec(&policy).expect("json")).expect("policy");
                    iam.set_policy(&name, parsed).await.expect("store policy");
                    iam.policy_db_set(&name, UserType::Reg, false, &name)
                        .await
                        .expect("attach policy");
                    iam.get_user(&name).await.expect("load user").credentials
                }
            };

            let list_only = create_user_with_policy(
                "proto-list-only",
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Action": ["s3:ListBucket"],
                        "Resource": [format!("arn:aws:s3:::{bucket}")]
                    }]
                }),
            )
            .await;
            let orders_only = create_user_with_policy(
                "proto-orders-only",
                json!({
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": ["s3:ListBucket"],
                            "Resource": [format!("arn:aws:s3:::{bucket}")]
                        },
                        {
                            "Effect": "Allow",
                            "Action": ["admin:GetTableMetadata"],
                            "Resource": [format!("arn:aws:s3:::{bucket}/namespaces/analytics/tables/orders")],
                            "Condition": {"IpAddress": {"aws:SourceIp": "203.0.113.10/32"}}
                        }
                    ]
                }),
            )
            .await;

            let slot = ServerContextSlot::new();
            assert!(slot.install(context));
            let backend = ProtocolStorageClient::new(slot);

            let listed_keys = |session: SessionContext, prefix: Option<&str>| {
                let backend = backend.clone();
                let prefix = prefix.map(str::to_owned);
                async move {
                    let mut req = input(bucket);
                    req.prefix = prefix;
                    let out = backend.list_objects_v2(&session, req).await.expect("list_objects_v2");
                    out.contents
                        .into_iter()
                        .map(|obj| obj.key.as_str().to_owned())
                        .collect::<Vec<_>>()
                }
            };

            // List-only user sees only plain.txt; catalog keys and all table objects stay hidden.
            let list_only_session = session_for(&list_only, "203.0.113.10");
            assert_eq!(listed_keys(list_only_session.clone(), None).await, vec!["plain.txt"]);
            assert!(
                listed_keys(list_only_session.clone(), Some(".rustfs-table/"))
                    .await
                    .is_empty()
            );
            assert!(listed_keys(list_only_session, Some("tables/orders-id/")).await.is_empty());

            // Session-scoped IAM condition applies to admin:GetTableMetadata.
            let allowed_ip_keys = listed_keys(session_for(&orders_only, "203.0.113.10"), None).await;
            assert_eq!(allowed_ip_keys, vec!["plain.txt", "tables/orders-id/data/part-0.parquet"]);
            let denied_ip_keys = listed_keys(session_for(&orders_only, "198.51.100.20"), None).await;
            assert_eq!(denied_ip_keys, vec!["plain.txt"]);

            // Root sees both tables' data files, but reserved .rustfs-table/ keys remain hidden.
            let root_keys = listed_keys(session_for(&root, "127.0.0.1"), None).await;
            assert_eq!(
                root_keys,
                vec![
                    "plain.txt",
                    "tables/orders-id/data/part-0.parquet",
                    "tables/payroll-id/data/part-0.parquet",
                ]
            );
        });
    }
}
