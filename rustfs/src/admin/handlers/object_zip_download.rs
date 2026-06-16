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

use crate::admin::router::{ADMIN_OBJECT_ZIP_DOWNLOADS_PATH, AdminOperation, Operation, S3Router};
use crate::app::context::resolve_object_store_handle;
use crate::auth::{check_key_valid, get_session_token};
use crate::error::ApiError;
use crate::license::license_check;
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use crate::storage::access::{ReqInfo, authorize_request};
use aes_gcm::{
    Aes256Gcm, Key, Nonce,
    aead::{Aead, KeyInit},
};
use async_zip::{Compression, ZipEntryBuilder, base::write::ZipFileWriter};
use futures_lite::io::AsyncWriteExt;
use http::{HeaderMap, HeaderValue, StatusCode, header};
use hyper::{Method, Uri};
use matchit::Params;
use rand::RngExt;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_credentials::get_global_action_cred;
use rustfs_ecstore::{
    global::get_global_region,
    store_api::{ListOperations, ObjectIO, ObjectOperations, ObjectOptions},
};
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_storage_api::{BucketOperations, bucket::BucketOptions};
use rustfs_trusted_proxies::{ClientInfo, ValidationMode};
use rustfs_utils::{base64_decode_url_safe_no_pad, base64_encode_url_safe_no_pad};
use s3s::{Body, S3Request, S3Response, S3Result, dto::StreamingBlob, header::CONTENT_TYPE, s3_error};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::io;
use std::net::{IpAddr, SocketAddr};
use time::{Duration, OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;
use url::form_urlencoded;
use uuid::Uuid;

const OBJECT_ZIP_DOWNLOAD_TOKEN_TTL: Duration = Duration::minutes(5);
const ZIP_STREAM_BUFFER_SIZE: usize = 1024 * 1024;
const ZIP_OBJECT_BUFFER_SIZE: usize = 128 * 1024;
const ZIP_LIST_MAX_KEYS: i32 = 1000;

pub fn register_object_zip_download_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::POST,
        format!("{ADMIN_PREFIX}{ADMIN_OBJECT_ZIP_DOWNLOADS_PATH}").as_str(),
        AdminOperation(&CreateObjectZipDownloadHandler {}),
    )?;
    r.insert(
        Method::GET,
        format!("{ADMIN_PREFIX}{ADMIN_OBJECT_ZIP_DOWNLOADS_PATH}/{{id}}.zip").as_str(),
        AdminOperation(&DownloadObjectZipHandler {}),
    )?;

    Ok(())
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CreateObjectZipDownloadRequest {
    pub bucket: String,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub objects: Vec<String>,
    #[serde(default)]
    pub prefixes: Vec<String>,
    #[serde(default)]
    pub filename: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CreateObjectZipDownloadResponse {
    pub download_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
}

impl CreateObjectZipDownloadRequest {
    fn validate(&self) -> S3Result<()> {
        if self.bucket.trim().is_empty() {
            return Err(s3_error!(InvalidRequest, "bucket must not be empty"));
        }

        if self.objects.iter().any(|object| object.is_empty()) {
            return Err(s3_error!(InvalidRequest, "objects must not contain empty strings"));
        }

        if self.prefixes.iter().any(|prefix| prefix.is_empty()) {
            return Err(s3_error!(InvalidRequest, "prefixes must not contain empty strings"));
        }

        let has_prefix = self.prefix.as_deref().is_some_and(|prefix| !prefix.is_empty());
        if !has_prefix && self.objects.is_empty() && self.prefixes.is_empty() {
            return Err(s3_error!(InvalidRequest, "at least one of prefix, objects, or prefixes is required"));
        }

        Ok(())
    }
}

#[derive(Clone, Debug)]
struct ObjectZipDownloadToken {
    id: String,
    principal: String,
    req_info: ReqInfo,
    auth_context: ObjectZipDownloadAuthContext,
    request: CreateObjectZipDownloadRequest,
}

#[derive(Clone, Debug, Default)]
struct ObjectZipDownloadAuthContext {
    headers: HeaderMap,
    remote_addr: Option<RemoteAddr>,
    client_info: Option<ClientInfo>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ObjectZipDownloadTokenPayload {
    id: String,
    principal: String,
    req_info: ObjectZipDownloadReqInfoSnapshot,
    auth_context: ObjectZipDownloadAuthContextSnapshot,
    request: CreateObjectZipDownloadRequest,
    expires_at_unix: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ObjectZipDownloadReqInfoSnapshot {
    cred: Option<rustfs_credentials::Credentials>,
    is_owner: bool,
    bucket: Option<String>,
    object: Option<String>,
    version_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ObjectZipDownloadAuthContextSnapshot {
    headers: Vec<(String, String)>,
    remote_addr: Option<SocketAddr>,
    client_info: Option<ClientInfoSnapshot>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ClientInfoSnapshot {
    real_ip: IpAddr,
    forwarded_host: Option<String>,
    forwarded_proto: Option<String>,
    is_from_trusted_proxy: bool,
    proxy_ip: Option<IpAddr>,
    proxy_hops: usize,
    validation_mode: ValidationMode,
    warnings: Vec<String>,
}

#[derive(Debug)]
struct CreatedObjectZipDownloadToken {
    id: String,
    token: String,
    expires_at: OffsetDateTime,
}

struct PreparedZipArchive {
    file: File,
}

impl From<&ReqInfo> for ObjectZipDownloadReqInfoSnapshot {
    fn from(req_info: &ReqInfo) -> Self {
        let mut cred = req_info.cred.clone();
        if let Some(cred) = cred.as_mut() {
            cred.secret_key.clear();
            cred.session_token.clear();
        }

        Self {
            cred,
            is_owner: req_info.is_owner,
            bucket: req_info.bucket.clone(),
            object: req_info.object.clone(),
            version_id: req_info.version_id.clone(),
        }
    }
}

impl From<ObjectZipDownloadReqInfoSnapshot> for ReqInfo {
    fn from(snapshot: ObjectZipDownloadReqInfoSnapshot) -> Self {
        Self {
            cred: snapshot.cred,
            is_owner: snapshot.is_owner,
            bucket: snapshot.bucket,
            object: snapshot.object,
            version_id: snapshot.version_id,
            region: get_global_region(),
            request_context: None,
        }
    }
}

impl From<&ObjectZipDownloadAuthContext> for ObjectZipDownloadAuthContextSnapshot {
    fn from(context: &ObjectZipDownloadAuthContext) -> Self {
        Self {
            headers: context
                .headers
                .iter()
                .filter_map(|(name, value)| {
                    value
                        .to_str()
                        .ok()
                        .map(|value| (name.as_str().to_string(), value.to_string()))
                })
                .collect(),
            remote_addr: context.remote_addr.map(|addr| addr.0),
            client_info: context.client_info.as_ref().map(ClientInfoSnapshot::from),
        }
    }
}

impl TryFrom<ObjectZipDownloadAuthContextSnapshot> for ObjectZipDownloadAuthContext {
    type Error = s3s::S3Error;

    fn try_from(snapshot: ObjectZipDownloadAuthContextSnapshot) -> S3Result<Self> {
        let mut headers = HeaderMap::new();
        for (name, value) in snapshot.headers {
            let name = http::header::HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| s3_error!(AccessDenied, "invalid or expired download token"))?;
            let value =
                HeaderValue::from_str(&value).map_err(|_| s3_error!(AccessDenied, "invalid or expired download token"))?;
            headers.append(name, value);
        }

        Ok(Self {
            headers,
            remote_addr: snapshot.remote_addr.map(RemoteAddr),
            client_info: snapshot.client_info.map(ClientInfo::from),
        })
    }
}

impl From<&ClientInfo> for ClientInfoSnapshot {
    fn from(client_info: &ClientInfo) -> Self {
        Self {
            real_ip: client_info.real_ip,
            forwarded_host: client_info.forwarded_host.clone(),
            forwarded_proto: client_info.forwarded_proto.clone(),
            is_from_trusted_proxy: client_info.is_from_trusted_proxy,
            proxy_ip: client_info.proxy_ip,
            proxy_hops: client_info.proxy_hops,
            validation_mode: client_info.validation_mode,
            warnings: client_info.warnings.clone(),
        }
    }
}

impl From<ClientInfoSnapshot> for ClientInfo {
    fn from(snapshot: ClientInfoSnapshot) -> Self {
        Self {
            real_ip: snapshot.real_ip,
            forwarded_host: snapshot.forwarded_host,
            forwarded_proto: snapshot.forwarded_proto,
            is_from_trusted_proxy: snapshot.is_from_trusted_proxy,
            proxy_ip: snapshot.proxy_ip,
            proxy_hops: snapshot.proxy_hops,
            validation_mode: snapshot.validation_mode,
            warnings: snapshot.warnings,
        }
    }
}

fn download_token_encryption_key() -> S3Result<[u8; 32]> {
    let credentials =
        get_global_action_cred().ok_or_else(|| s3_error!(InternalError, "global action credentials are not initialized"))?;
    if credentials.secret_key.is_empty() {
        return Err(s3_error!(InternalError, "global action credentials are not initialized"));
    }
    let mut hasher = Sha256::new();
    hasher.update(b"rustfs-object-zip-download-token-v1");
    hasher.update(credentials.secret_key.as_bytes());
    Ok(hasher.finalize().into())
}

fn encode_download_token(payload: &ObjectZipDownloadTokenPayload) -> S3Result<String> {
    let payload_json =
        serde_json::to_vec(payload).map_err(|e| s3_error!(InternalError, "failed to serialize download token: {}", e))?;
    let key = download_token_encryption_key()?;
    let cipher = Aes256Gcm::new(&Key::<Aes256Gcm>::from(key));
    let mut nonce_bytes = [0_u8; 12];
    rand::rng().fill(&mut nonce_bytes);
    let nonce = Nonce::from(nonce_bytes);
    let ciphertext = cipher
        .encrypt(&nonce, payload_json.as_slice())
        .map_err(|_| s3_error!(InternalError, "failed to encrypt download token"))?;
    Ok(format!(
        "{}.{}",
        base64_encode_url_safe_no_pad(&nonce_bytes),
        base64_encode_url_safe_no_pad(&ciphertext)
    ))
}

fn decode_download_token(token: &str) -> S3Result<ObjectZipDownloadTokenPayload> {
    let Some((nonce_part, ciphertext_part)) = token.split_once('.') else {
        return Err(s3_error!(AccessDenied, "invalid or expired download token"));
    };
    if ciphertext_part.contains('.') {
        return Err(s3_error!(AccessDenied, "invalid or expired download token"));
    }

    let nonce = base64_decode_url_safe_no_pad(nonce_part.as_bytes())
        .map_err(|_| s3_error!(AccessDenied, "invalid or expired download token"))?;
    let ciphertext = base64_decode_url_safe_no_pad(ciphertext_part.as_bytes())
        .map_err(|_| s3_error!(AccessDenied, "invalid or expired download token"))?;
    let nonce: [u8; 12] = nonce
        .as_slice()
        .try_into()
        .map_err(|_| s3_error!(AccessDenied, "invalid or expired download token"))?;
    let key = download_token_encryption_key()?;
    let cipher = Aes256Gcm::new(&Key::<Aes256Gcm>::from(key));
    let payload = cipher
        .decrypt(&Nonce::from(nonce), ciphertext.as_slice())
        .map_err(|_| s3_error!(AccessDenied, "invalid or expired download token"))?;

    serde_json::from_slice(&payload).map_err(|_| s3_error!(AccessDenied, "invalid or expired download token"))
}

fn create_download_token(
    principal: String,
    req_info: ReqInfo,
    auth_context: ObjectZipDownloadAuthContext,
    request: CreateObjectZipDownloadRequest,
    now: OffsetDateTime,
) -> S3Result<CreatedObjectZipDownloadToken> {
    let id = Uuid::new_v4().to_string();
    let expires_at = now + OBJECT_ZIP_DOWNLOAD_TOKEN_TTL;
    let payload = ObjectZipDownloadTokenPayload {
        id: id.clone(),
        principal,
        req_info: ObjectZipDownloadReqInfoSnapshot::from(&req_info),
        auth_context: ObjectZipDownloadAuthContextSnapshot::from(&auth_context),
        request,
        expires_at_unix: expires_at.unix_timestamp(),
    };
    let token = encode_download_token(&payload)?;

    Ok(CreatedObjectZipDownloadToken { id, token, expires_at })
}

fn validate_download_token(id: &str, token: &str, now: OffsetDateTime) -> S3Result<ObjectZipDownloadToken> {
    let payload = decode_download_token(token)?;
    if payload.id != id {
        return Err(s3_error!(AccessDenied, "invalid or expired download token"));
    }
    let expires_at = OffsetDateTime::from_unix_timestamp(payload.expires_at_unix)
        .map_err(|_| s3_error!(AccessDenied, "invalid or expired download token"))?;
    if expires_at <= now {
        return Err(s3_error!(AccessDenied, "invalid or expired download token"));
    }

    Ok(ObjectZipDownloadToken {
        id: payload.id,
        principal: payload.principal,
        req_info: payload.req_info.into(),
        auth_context: payload.auth_context.try_into()?,
        request: payload.request,
    })
}

fn format_expires_at(value: OffsetDateTime) -> S3Result<String> {
    value
        .format(&Rfc3339)
        .map_err(|e| s3_error!(InternalError, "failed to format token expiration: {}", e))
}

fn build_download_url(id: &str, token: &str) -> String {
    format!("{ADMIN_PREFIX}{ADMIN_OBJECT_ZIP_DOWNLOADS_PATH}/{id}.zip?token={token}")
}

async fn authenticate_object_zip_download_request(req: &mut S3Request<Body>) -> S3Result<()> {
    let Some(input_cred) = &req.credentials else {
        return Err(s3_error!(InvalidRequest, "authentication required"));
    };

    let (cred, owner) =
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

    let remote_addr = req.extensions.get::<Option<RemoteAddr>>().cloned().unwrap_or_default();
    req.extensions.insert(ReqInfo {
        cred: Some(cred),
        is_owner: owner,
        region: get_global_region(),
        request_context: req.extensions.get().cloned(),
        ..Default::default()
    });
    req.extensions.insert(remote_addr);

    license_check().map_err(|er| match er.kind() {
        std::io::ErrorKind::PermissionDenied => s3_error!(AccessDenied, "{er}"),
        _ => {
            tracing::error!("license check failed due to unexpected error: {er}");
            s3_error!(InternalError, "License validation failed")
        }
    })?;

    Ok(())
}

fn authenticated_zip_download_principal(req: &S3Request<Body>) -> S3Result<String> {
    let req_info = req
        .extensions
        .get::<ReqInfo>()
        .ok_or_else(|| s3_error!(AccessDenied, "authentication required"))?;
    let credentials = req_info
        .cred
        .clone()
        .ok_or_else(|| s3_error!(AccessDenied, "authentication required"))?;

    Ok(credentials.access_key)
}

fn authenticated_zip_download_req_info(req: &S3Request<Body>) -> S3Result<ReqInfo> {
    let mut req_info = req
        .extensions
        .get::<ReqInfo>()
        .cloned()
        .ok_or_else(|| s3_error!(AccessDenied, "authentication required"))?;
    if let Some(cred) = req_info.cred.as_mut() {
        cred.secret_key.clear();
        cred.session_token.clear();
    }
    Ok(req_info)
}

fn authenticated_zip_download_auth_context(req: &S3Request<Body>) -> ObjectZipDownloadAuthContext {
    ObjectZipDownloadAuthContext {
        headers: req.headers.clone(),
        remote_addr: req.extensions.get::<Option<RemoteAddr>>().copied().flatten(),
        client_info: req.extensions.get::<ClientInfo>().cloned(),
    }
}

async fn authorize_object_zip_download_scope(
    req: &mut S3Request<Body>,
    request: &CreateObjectZipDownloadRequest,
) -> S3Result<()> {
    for prefix in listing_prefixes(request) {
        authorize_object_zip_download_list_prefix(req, &request.bucket, prefix).await?;
    }

    for object in &request.objects {
        authorize_object_zip_download_s3_action(req, &request.bucket, Some(object), S3Action::GetObjectAction).await?;
    }

    Ok(())
}

async fn authorize_object_zip_download_list_prefix(req: &mut S3Request<Body>, bucket: &str, prefix: &str) -> S3Result<()> {
    let original_uri = req.uri.clone();
    req.uri = uri_with_prefix_query(&original_uri, prefix)?;
    let result = authorize_object_zip_download_s3_action(req, bucket, None, S3Action::ListBucketAction).await;
    req.uri = original_uri;
    result
}

async fn authorize_object_zip_download_s3_action(
    req: &mut S3Request<Body>,
    bucket: &str,
    object: Option<&str>,
    action: S3Action,
) -> S3Result<()> {
    if let Some(req_info) = req.extensions.get_mut::<ReqInfo>() {
        req_info.bucket = Some(bucket.to_string());
        req_info.object = object.map(str::to_string);
        req_info.version_id = None;
    }

    authorize_request(req, Action::S3Action(action)).await
}

fn uri_with_prefix_query(uri: &Uri, prefix: &str) -> S3Result<Uri> {
    let existing_pairs: Vec<(String, String)> = uri
        .query()
        .map(|query| {
            form_urlencoded::parse(query.as_bytes())
                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                .collect()
        })
        .unwrap_or_default();
    let mut query = form_urlencoded::Serializer::new(String::new());
    for (key, value) in existing_pairs {
        if key != "prefix" {
            query.append_pair(&key, &value);
        }
    }
    query.append_pair("prefix", prefix);
    let path_and_query = format!("{}?{}", uri.path(), query.finish());
    let mut parts = uri.clone().into_parts();
    parts.path_and_query = Some(
        path_and_query
            .parse()
            .map_err(|e| s3_error!(InvalidRequest, "invalid prefix authorization URI: {}", e))?,
    );
    Uri::from_parts(parts).map_err(|e| s3_error!(InvalidRequest, "invalid prefix authorization URI: {}", e))
}

fn build_json_response(
    status: StatusCode,
    body: &impl Serialize,
    request_id: Option<&HeaderValue>,
) -> S3Result<S3Response<(StatusCode, Body)>> {
    let data = serde_json::to_vec(body).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
    let mut header = HeaderMap::new();
    header.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    if let Some(value) = request_id {
        header.insert("x-request-id", value.clone());
    }
    Ok(S3Response::with_headers((status, Body::from(data)), header))
}

fn query_value_exact(uri: &Uri, key: &str) -> Option<String> {
    uri.query().and_then(|query| {
        form_urlencoded::parse(query.as_bytes())
            .find_map(|(name, value)| if name == key { Some(value.into_owned()) } else { None })
    })
}

pub struct CreateObjectZipDownloadHandler {}

#[async_trait::async_trait]
impl Operation for CreateObjectZipDownloadHandler {
    async fn call(&self, mut req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authenticate_object_zip_download_request(&mut req).await?;

        let body = req
            .input
            .store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE)
            .await
            .map_err(|e| s3_error!(InvalidRequest, "failed to read request body: {}", e))?;
        let request: CreateObjectZipDownloadRequest =
            serde_json::from_slice(&body).map_err(|e| s3_error!(InvalidRequest, "invalid JSON: {}", e))?;
        request.validate()?;
        authorize_object_zip_download_scope(&mut req, &request).await?;
        let principal = authenticated_zip_download_principal(&req)?;
        let req_info = authenticated_zip_download_req_info(&req)?;
        let auth_context = authenticated_zip_download_auth_context(&req);
        let created = create_download_token(principal, req_info, auth_context, request, OffsetDateTime::now_utc())?;

        build_json_response(
            StatusCode::OK,
            &CreateObjectZipDownloadResponse {
                download_url: build_download_url(&created.id, &created.token),
                expires_at: Some(format_expires_at(created.expires_at)?),
            },
            req.headers.get("x-request-id"),
        )
    }
}

pub struct DownloadObjectZipHandler {}

#[async_trait::async_trait]
impl Operation for DownloadObjectZipHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let id = params.get("id").unwrap_or("");
        let token = query_value_exact(&req.uri, "token").unwrap_or_default();
        if id.is_empty() || token.is_empty() {
            return Err(s3_error!(AccessDenied, "invalid or expired download token"));
        }

        let record = validate_download_token(id, &token, OffsetDateTime::now_utc())?;
        let _token_id = &record.id;
        let _principal = &record.principal;
        let prepared = prepare_zip_download_archive(&record).await?;
        let body = build_zip_stream_body(prepared);

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/zip"));
        headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        headers.insert(
            header::CONTENT_DISPOSITION,
            HeaderValue::from_str(&format!(
                "attachment; filename=\"{}\"",
                sanitize_content_disposition_filename(record.request.filename.as_deref().unwrap_or("download.zip"))
            ))
            .map_err(|e| s3_error!(InternalError, "invalid download filename: {}", e))?,
        );

        Ok(S3Response::with_headers((StatusCode::OK, body), headers))
    }
}

fn sanitize_content_disposition_filename(filename: &str) -> String {
    let cleaned = filename
        .chars()
        .map(|ch| match ch {
            '"' | '\\' | '\r' | '\n' => '_',
            ch if ch.is_control() => '_',
            ch => ch,
        })
        .collect::<String>();
    if cleaned.trim().is_empty() {
        "download.zip".to_string()
    } else {
        cleaned
    }
}

fn zip_entry_name(base_prefix: Option<&str>, key: &str) -> S3Result<String> {
    let relative = base_prefix
        .filter(|base| !base.is_empty() && key.starts_with(*base))
        .map(|base| &key[base.len()..])
        .unwrap_or(key);
    let entry_name = relative.trim_start_matches('/').to_string();
    validate_zip_entry_name(&entry_name)?;
    Ok(entry_name)
}

fn validate_zip_entry_name(entry_name: &str) -> S3Result<()> {
    if entry_name.is_empty() {
        return Err(s3_error!(InvalidRequest, "zip entry name must not be empty"));
    }
    if entry_name.starts_with('/') || entry_name.contains('\\') || entry_name.split('/').any(|part| part == "..") {
        return Err(s3_error!(InvalidRequest, "zip entry name must be relative"));
    }
    Ok(())
}

async fn preflight_zip_items(request: &CreateObjectZipDownloadRequest, items: &[ZipDownloadItem]) -> S3Result<()> {
    let store = resolve_object_store_handle().ok_or_else(|| s3_error!(InternalError, "object store not initialized"))?;
    for item in items {
        store
            .get_object_info(&request.bucket, &item.key, &ObjectOptions::default())
            .await
            .map_err(storage_error_to_s3)?;
    }
    Ok(())
}

fn storage_error_to_s3(err: rustfs_ecstore::error::Error) -> s3s::S3Error {
    ApiError::from(err).into()
}

async fn validate_zip_download_request(record: &ObjectZipDownloadToken) -> S3Result<()> {
    let store = resolve_object_store_handle().ok_or_else(|| s3_error!(InternalError, "object store not initialized"))?;
    store
        .get_bucket_info(&record.request.bucket, &BucketOptions::default())
        .await
        .map_err(storage_error_to_s3)?;

    validate_zip_download_items(record, &collect_explicit_zip_items(&record.request)?).await?;
    Ok(())
}

async fn validate_zip_download_items(record: &ObjectZipDownloadToken, items: &[ZipDownloadItem]) -> S3Result<()> {
    authorize_zip_items_for_download(record, items).await?;
    preflight_zip_items(&record.request, items).await
}

fn listing_prefixes(request: &CreateObjectZipDownloadRequest) -> Vec<&str> {
    if request.objects.is_empty()
        && request.prefixes.is_empty()
        && let Some(prefix) = request.prefix.as_deref().filter(|prefix| !prefix.is_empty())
    {
        return vec![prefix];
    }

    request.prefixes.iter().map(String::as_str).collect()
}

async fn authorize_zip_items_for_download(record: &ObjectZipDownloadToken, items: &[ZipDownloadItem]) -> S3Result<()> {
    for item in items {
        let mut req = S3Request {
            input: Body::empty(),
            method: Method::GET,
            uri: format!("{ADMIN_PREFIX}{ADMIN_OBJECT_ZIP_DOWNLOADS_PATH}/{}.zip", record.id)
                .parse()
                .map_err(|e| s3_error!(InternalError, "failed to build download authorization URI: {}", e))?,
            headers: record.auth_context.headers.clone(),
            extensions: {
                let mut extensions = http::Extensions::new();
                extensions.insert(record.auth_context.remote_addr);
                if let Some(client_info) = record.auth_context.client_info.clone() {
                    extensions.insert(client_info);
                }
                extensions
            },
            credentials: None,
            region: get_global_region(),
            service: None,
            trailing_headers: None,
        };
        let mut req_info = record.req_info.clone();
        req_info.bucket = Some(record.request.bucket.clone());
        req_info.object = Some(item.key.clone());
        req_info.version_id = None;
        req.extensions.insert(req_info);
        authorize_request(&mut req, Action::S3Action(S3Action::GetObjectAction)).await?;
    }
    Ok(())
}

fn collect_explicit_zip_items(request: &CreateObjectZipDownloadRequest) -> S3Result<Vec<ZipDownloadItem>> {
    let mut items = Vec::new();
    let mut seen = HashSet::new();
    for key in &request.objects {
        push_zip_item(request, &mut seen, &mut items, key.clone())?;
    }
    Ok(items)
}

fn push_zip_item(
    request: &CreateObjectZipDownloadRequest,
    seen: &mut HashSet<String>,
    items: &mut Vec<ZipDownloadItem>,
    key: String,
) -> S3Result<()> {
    if seen.insert(key.clone()) {
        items.push(ZipDownloadItem {
            entry_name: zip_entry_name(request.prefix.as_deref(), &key)?,
            key,
        });
    }
    Ok(())
}

fn explicit_zip_item_keys(items: &[ZipDownloadItem]) -> HashSet<String> {
    items.iter().map(|item| item.key.clone()).collect()
}

fn normalized_listing_prefixes(request: &CreateObjectZipDownloadRequest) -> Vec<String> {
    let mut prefixes: Vec<String> = listing_prefixes(request).into_iter().map(str::to_string).collect();
    prefixes.sort();
    prefixes.dedup();

    let mut normalized = Vec::new();
    for prefix in prefixes {
        if normalized.iter().any(|existing: &String| prefix.starts_with(existing)) {
            continue;
        }
        normalized.push(prefix);
    }
    normalized
}

fn build_zip_stream_body(prepared: PreparedZipArchive) -> Body {
    let stream = ReaderStream::with_capacity(prepared.file, ZIP_STREAM_BUFFER_SIZE);
    Body::from(StreamingBlob::wrap(stream))
}

async fn prepare_zip_download_archive(record: &ObjectZipDownloadToken) -> S3Result<PreparedZipArchive> {
    validate_zip_download_request(record).await?;

    let temp_file = tempfile::tempfile().map_err(|err| s3_error!(InternalError, "failed to create ZIP temp file: {}", err))?;
    let std_file = temp_file;
    let mut file = File::from_std(std_file);

    generate_zip_archive(record, &mut file)
        .await
        .map_err(|err| s3_error!(InternalError, "failed to prepare ZIP download: {}", err))?;
    file.seek(std::io::SeekFrom::Start(0))
        .await
        .map_err(|err| s3_error!(InternalError, "failed to rewind ZIP temp file: {}", err))?;

    Ok(PreparedZipArchive { file })
}

async fn generate_zip_archive(record: &ObjectZipDownloadToken, file: &mut File) -> io::Result<()> {
    let mut zip = ZipFileWriter::with_tokio(file).force_zip64();
    let store = resolve_object_store_handle().ok_or_else(|| io::Error::other("object store not initialized"))?;

    let explicit_items = collect_explicit_zip_items(&record.request)
        .map_err(|err| io::Error::other(format!("failed to prepare explicit ZIP items: {err}")))?;
    let explicit_keys = explicit_zip_item_keys(&explicit_items);
    write_zip_items(&mut zip, &record.request.bucket, &explicit_items).await?;

    for prefix in normalized_listing_prefixes(&record.request) {
        let mut continuation_token = None;
        loop {
            let listed = store
                .clone()
                .list_objects_v2(
                    &record.request.bucket,
                    prefix.as_str(),
                    continuation_token.clone(),
                    None,
                    ZIP_LIST_MAX_KEYS,
                    false,
                    None,
                    false,
                )
                .await
                .map_err(|err| io::Error::other(format!("failed to list prefix `{prefix}` for ZIP stream: {err}")))?;

            let mut page_items = Vec::new();
            for object in listed.objects {
                if object.name == prefix && object.size == 0 {
                    continue;
                }
                if explicit_keys.contains(&object.name) {
                    continue;
                }
                page_items.push(ZipDownloadItem {
                    entry_name: zip_entry_name(record.request.prefix.as_deref(), &object.name)
                        .map_err(|err| io::Error::other(format!("failed to prepare ZIP entry name: {err}")))?,
                    key: object.name,
                });
            }
            validate_zip_download_items(record, &page_items)
                .await
                .map_err(|err| io::Error::other(format!("failed to validate ZIP page for prefix `{prefix}`: {err}")))?;
            write_zip_items(&mut zip, &record.request.bucket, &page_items).await?;

            if !listed.is_truncated {
                break;
            }
            continuation_token = listed.next_continuation_token;
            if continuation_token.is_none() {
                break;
            }
        }
    }

    zip.close()
        .await
        .map(|_| ())
        .map_err(|err| io::Error::other(format!("failed to finish ZIP archive: {err}")))
}

async fn write_zip_items<W>(
    zip: &mut async_zip::tokio::write::ZipFileWriter<W>,
    bucket: &str,
    items: &[ZipDownloadItem],
) -> io::Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    for item in items {
        write_zip_item(zip, bucket, item).await.map_err(|err| {
            io::Error::other(format!(
                "failed to write bucket `{}` object `{}` into ZIP entry `{}`: {}",
                bucket, item.key, item.entry_name, err
            ))
        })?;
    }
    Ok(())
}

async fn write_zip_item<W>(
    zip: &mut async_zip::tokio::write::ZipFileWriter<W>,
    bucket: &str,
    item: &ZipDownloadItem,
) -> io::Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let store = resolve_object_store_handle().ok_or_else(|| io::Error::other("object store not initialized"))?;
    let mut reader = store
        .get_object_reader(bucket, &item.key, None, HeaderMap::new(), &ObjectOptions::default())
        .await
        .map_err(|err| io::Error::other(format!("failed to open object reader: {err}")))?;
    let entry = ZipEntryBuilder::new(item.entry_name.clone().into(), Compression::Stored);
    let mut entry_writer = zip
        .write_entry_stream(entry)
        .await
        .map_err(|err| io::Error::other(format!("failed to start ZIP entry: {err}")))?;

    let mut buffer = vec![0_u8; ZIP_OBJECT_BUFFER_SIZE];
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        entry_writer.write_all(&buffer[..read]).await?;
    }
    entry_writer
        .close()
        .await
        .map_err(|err| io::Error::other(format!("failed to close ZIP entry: {err}")))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ZipDownloadItem {
    key: String,
    entry_name: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_credentials::init_global_action_credentials;
    use s3s::S3ErrorCode;

    fn valid_request() -> CreateObjectZipDownloadRequest {
        CreateObjectZipDownloadRequest {
            bucket: "photos".to_string(),
            prefix: Some("2026/".to_string()),
            objects: Vec::new(),
            prefixes: Vec::new(),
            filename: None,
        }
    }

    fn assert_invalid_request(request: CreateObjectZipDownloadRequest, expected_message: &str) {
        let err = request.validate().expect_err("request should be invalid");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert_eq!(err.message(), Some(expected_message));
    }

    #[test]
    fn validation_accepts_prefix_objects_or_prefixes() {
        valid_request().validate().expect("prefix selection should be valid");

        let mut object_request = valid_request();
        object_request.prefix = None;
        object_request.objects = vec!["a.txt".to_string()];
        object_request.validate().expect("object selection should be valid");

        let mut prefix_request = valid_request();
        prefix_request.prefix = None;
        prefix_request.prefixes = vec!["folder/".to_string()];
        prefix_request.validate().expect("prefixes selection should be valid");
    }

    #[test]
    fn validation_rejects_empty_bucket() {
        let mut request = valid_request();
        request.bucket = "   ".to_string();

        assert_invalid_request(request, "bucket must not be empty");
    }

    #[test]
    fn validation_rejects_empty_selection() {
        let mut request = valid_request();
        request.prefix = None;

        assert_invalid_request(request, "at least one of prefix, objects, or prefixes is required");
    }

    #[test]
    fn validation_rejects_empty_objects_and_prefixes() {
        let mut object_request = valid_request();
        object_request.objects = vec!["".to_string()];
        assert_invalid_request(object_request, "objects must not contain empty strings");

        let mut prefix_request = valid_request();
        prefix_request.prefixes = vec!["".to_string()];
        assert_invalid_request(prefix_request, "prefixes must not contain empty strings");
    }

    #[test]
    fn request_deserialization_defaults_optional_collections() {
        let request: CreateObjectZipDownloadRequest =
            serde_json::from_str(r#"{"bucket":"photos","prefix":"2026/"}"#).expect("request should deserialize");

        assert_eq!(request.bucket, "photos");
        assert_eq!(request.prefix.as_deref(), Some("2026/"));
        assert!(request.objects.is_empty());
        assert!(request.prefixes.is_empty());
        assert_eq!(request.filename, None);
    }

    #[test]
    fn request_deserialization_denies_unknown_fields() {
        let err =
            serde_json::from_str::<CreateObjectZipDownloadRequest>(r#"{"bucket":"photos","prefix":"2026/","unexpected":true}"#)
                .expect_err("unknown fields should be rejected");

        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    fn response_serialization_skips_absent_expires_at() {
        let response = CreateObjectZipDownloadResponse {
            download_url: "/rustfs/admin/v3/object-zip-downloads/id.zip?token=token".to_string(),
            expires_at: None,
        };

        let json = serde_json::to_value(response).expect("response should serialize");
        assert_eq!(json["download_url"], "/rustfs/admin/v3/object-zip-downloads/id.zip?token=token");
        assert!(json.get("expires_at").is_none());
    }

    #[test]
    fn create_download_token_returns_browser_download_url_parts() {
        let now = OffsetDateTime::UNIX_EPOCH;
        let created = create_test_download_token(now);

        assert!(!created.id.is_empty());
        assert!(!created.token.is_empty());
        assert_eq!(created.expires_at, now + OBJECT_ZIP_DOWNLOAD_TOKEN_TTL);
        let download_url = build_download_url(&created.id, &created.token);
        assert!(download_url.starts_with("/rustfs/admin/v3/object-zip-downloads/"));
        assert!(download_url.ends_with(&format!(".zip?token={}", created.token)));
    }

    #[test]
    fn validate_download_token_rejects_unknown_token() {
        let err = validate_download_token("missing", "missing", OffsetDateTime::UNIX_EPOCH)
            .expect_err("unknown token should be rejected");

        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }

    #[test]
    fn validate_download_token_rejects_mismatched_token_value() {
        let now = OffsetDateTime::UNIX_EPOCH;
        let created = create_test_download_token(now);

        let err = validate_download_token("different-id", &created.token, now).expect_err("wrong token id should be rejected");

        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }

    #[test]
    fn validate_download_token_rejects_expired_token() {
        let now = OffsetDateTime::UNIX_EPOCH;
        let created = create_test_download_token(now);

        let err = validate_download_token(&created.id, &created.token, now + OBJECT_ZIP_DOWNLOAD_TOKEN_TTL)
            .expect_err("expired token should be rejected");

        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }

    #[test]
    fn validate_download_token_accepts_unexpired_matching_token() {
        let now = OffsetDateTime::UNIX_EPOCH;
        let created = create_test_download_token(now);

        let record = validate_download_token(&created.id, &created.token, now).expect("matching token should be accepted");

        assert_eq!(record.id, created.id);
        assert_eq!(record.principal, "alice");
        assert_eq!(record.request.bucket, "photos");
        assert_eq!(record.req_info.cred.as_ref().map(|cred| cred.access_key.as_str()), Some("alice"));
        assert_eq!(record.req_info.cred.as_ref().map(|cred| cred.secret_key.as_str()), Some(""));
        assert_eq!(
            record
                .auth_context
                .headers
                .get("user-agent")
                .and_then(|value| value.to_str().ok()),
            Some("object-zip-test")
        );
        assert_eq!(
            record.auth_context.remote_addr.map(|addr| addr.0),
            Some("127.0.0.1:9000".parse().expect("test socket addr should parse"))
        );
        assert_eq!(
            record.auth_context.client_info.as_ref().map(|info| info.real_ip),
            Some("203.0.113.10".parse().expect("test IP should parse"))
        );
    }

    #[test]
    fn validate_download_token_rejects_tampered_payload() {
        let now = OffsetDateTime::UNIX_EPOCH;
        let created = create_test_download_token(now);
        let (nonce, ciphertext) = created
            .token
            .split_once('.')
            .expect("token should contain nonce and ciphertext");
        let mut ciphertext = base64_decode_url_safe_no_pad(ciphertext.as_bytes()).expect("ciphertext should decode");
        let first = ciphertext.first_mut().expect("ciphertext should not be empty");
        *first ^= 0x01;
        let tampered = format!("{}.{}", nonce, base64_encode_url_safe_no_pad(&ciphertext));

        let err = validate_download_token(&created.id, &tampered, now).expect_err("tampered token should be rejected");

        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }

    #[test]
    fn object_zip_download_tokens_are_stateless_encrypted_payloads() {
        let src = include_str!("object_zip_download.rs");

        assert!(
            src.contains("ObjectZipDownloadTokenPayload"),
            "object ZIP download token should carry an authenticated payload"
        );
        assert!(
            src.contains("get_global_action_cred"),
            "object ZIP download token should be verifiable by any node sharing global credentials"
        );
        assert!(
            src.contains("Aes256Gcm"),
            "object ZIP download token payload should not expose captured authorization headers in clear text"
        );
        assert!(
            !src.contains(&format!("{}{}{}", "OnceLock<", "RwLock<", "HashMap")),
            "object ZIP download tokens must not use process-local storage"
        );
    }

    #[test]
    fn create_download_token_does_not_expose_payload_plaintext() {
        let created = create_test_download_token(OffsetDateTime::UNIX_EPOCH);

        assert!(!created.token.contains("alice"));
        assert!(!created.token.contains("photos"));
        assert!(!created.token.contains("object-zip-test"));
    }

    #[test]
    fn uri_with_prefix_query_preserves_existing_query_and_overrides_prefix() {
        let uri: Uri = "/rustfs/admin/v3/object-zip-downloads?X-Amz-Algorithm=AWS4-HMAC-SHA256&prefix=old&marker=1"
            .parse()
            .expect("uri should parse");

        let updated = uri_with_prefix_query(&uri, "new/prefix/").expect("uri should be rewritten");
        let query = updated.query().expect("query should exist");

        assert!(query.contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"));
        assert!(query.contains("marker=1"));
        assert!(query.contains("prefix=new%2Fprefix%2F"));
        assert!(!query.contains("prefix=old"));
    }

    #[test]
    fn sanitize_content_disposition_filename_removes_unsafe_characters() {
        assert_eq!(sanitize_content_disposition_filename("bad\"\\\r\nname.zip"), "bad____name.zip");
        assert_eq!(sanitize_content_disposition_filename("   "), "download.zip");
    }

    #[test]
    fn zip_entry_name_strips_base_prefix() {
        assert_eq!(
            zip_entry_name(Some("2026/events/"), "2026/events/a.txt").expect("entry name should build"),
            "a.txt"
        );
        assert_eq!(
            zip_entry_name(Some("2026/events/"), "2026/events/nested/b.txt").expect("entry name should build"),
            "nested/b.txt"
        );
    }

    #[test]
    fn zip_entry_name_falls_back_to_full_key_when_outside_base_prefix() {
        assert_eq!(
            zip_entry_name(Some("2026/events/"), "other/a.txt").expect("entry name should build"),
            "other/a.txt"
        );
        assert_eq!(
            zip_entry_name(None, "/absolute-ish.txt").expect("entry name should build"),
            "absolute-ish.txt"
        );
    }

    #[test]
    fn zip_entry_name_rejects_empty_or_parent_directory_entries() {
        assert_eq!(
            zip_entry_name(Some("2026/events/"), "2026/events/")
                .expect_err("empty relative entry should be rejected")
                .code(),
            &S3ErrorCode::InvalidRequest
        );
        assert_eq!(
            zip_entry_name(None, "../secret.txt")
                .expect_err("parent directory entry should be rejected")
                .code(),
            &S3ErrorCode::InvalidRequest
        );
        assert_eq!(
            zip_entry_name(None, "..\\secret.txt")
                .expect_err("windows parent directory entry should be rejected")
                .code(),
            &S3ErrorCode::InvalidRequest
        );
    }

    #[test]
    fn collect_explicit_zip_items_deduplicates_keys_preserving_order() {
        let mut request = valid_request();
        request.prefix = Some("2026/events/".to_string());
        request.objects = vec![
            "2026/events/a.txt".to_string(),
            "2026/events/a.txt".to_string(),
            "2026/events/b.txt".to_string(),
        ];

        let items = collect_explicit_zip_items(&request).expect("items should collect");

        assert_eq!(
            items,
            vec![
                ZipDownloadItem {
                    key: "2026/events/a.txt".to_string(),
                    entry_name: "a.txt".to_string()
                },
                ZipDownloadItem {
                    key: "2026/events/b.txt".to_string(),
                    entry_name: "b.txt".to_string()
                },
            ]
        );
    }

    #[test]
    fn push_zip_item_deduplicates_across_explicit_and_listed_keys() {
        let mut request = valid_request();
        request.prefix = Some("2026/events/".to_string());
        request.objects = vec!["2026/events/a.txt".to_string()];

        let mut seen = HashSet::new();
        let mut items = collect_explicit_zip_items(&request).expect("explicit items should collect");
        seen.extend(items.iter().map(|item| item.key.clone()));

        push_zip_item(&request, &mut seen, &mut items, "2026/events/a.txt".to_string())
            .expect("duplicate listed item should be ignored");
        push_zip_item(&request, &mut seen, &mut items, "2026/events/nested/b.txt".to_string())
            .expect("new listed item should be added");

        assert_eq!(
            items,
            vec![
                ZipDownloadItem {
                    key: "2026/events/a.txt".to_string(),
                    entry_name: "a.txt".to_string()
                },
                ZipDownloadItem {
                    key: "2026/events/nested/b.txt".to_string(),
                    entry_name: "nested/b.txt".to_string()
                },
            ]
        );
    }

    #[test]
    fn listing_prefixes_uses_prefix_as_scope_only_when_no_explicit_selection() {
        let request = valid_request();

        assert_eq!(listing_prefixes(&request), vec!["2026/"]);

        let mut mixed = valid_request();
        mixed.objects = vec!["2026/a.txt".to_string()];
        mixed.prefixes = vec!["2026/nested/".to_string()];

        assert_eq!(listing_prefixes(&mixed), vec!["2026/nested/"]);
    }

    #[test]
    fn normalized_listing_prefixes_removes_duplicates_and_nested_prefixes() {
        let mut request = valid_request();
        request.prefix = None;
        request.prefixes = vec![
            "2026/".to_string(),
            "2026/reports/".to_string(),
            "2026/".to_string(),
            "2027/".to_string(),
        ];

        assert_eq!(normalized_listing_prefixes(&request), vec!["2026/".to_string(), "2027/".to_string()]);
    }

    #[test]
    fn download_handler_prepares_archive_before_streaming_response() {
        let src = include_str!("object_zip_download.rs");
        let handler_block = extract_block_between_markers(
            src,
            "impl Operation for DownloadObjectZipHandler",
            "fn sanitize_content_disposition_filename",
        );

        assert!(
            handler_block.contains("prepare_zip_download_archive(&record).await?;"),
            "download handler should fully prepare the ZIP archive before returning the ZIP response"
        );
        assert!(
            handler_block.contains("build_zip_stream_body(prepared)"),
            "download handler should stream the prepared archive instead of rebuilding request scope during response"
        );
        assert!(
            !handler_block.contains("tokio::spawn"),
            "download handler should not defer archive generation to a background task after returning 200"
        );
    }

    #[test]
    fn zip_download_preflight_preserves_storage_error_semantics() {
        let src = include_str!("object_zip_download.rs");
        let preflight_block = extract_block_between_markers(src, "async fn preflight_zip_items", "fn storage_error_to_s3");
        let validation_block =
            extract_block_between_markers(src, "async fn validate_zip_download_request", "async fn validate_zip_download_items");

        assert!(
            preflight_block.contains(".map_err(storage_error_to_s3)?;"),
            "preflight object checks should preserve storage-layer S3 semantics"
        );
        assert!(
            validation_block.contains(".map_err(storage_error_to_s3)?;"),
            "bucket and listing checks should preserve storage-layer S3 semantics"
        );
        assert!(
            preflight_block.contains(".get_object_info(&request.bucket, &item.key, &ObjectOptions::default())")
                && !preflight_block.contains("failed to prepare object"),
            "preflight object checks must not wrap expected missing object errors with a custom InternalError message"
        );
        assert!(
            validation_block.contains(".get_bucket_info(&record.request.bucket, &BucketOptions::default())")
                && !validation_block.contains("failed to validate bucket")
                && !validation_block.contains("failed to list prefix"),
            "bucket checks must not wrap expected user-facing errors with custom InternalError messages"
        );
    }

    #[test]
    fn object_zip_download_handler_requires_admin_authorization_contract() {
        let src = include_str!("object_zip_download.rs");
        let handler_block =
            extract_block_between_markers(src, "impl Operation for CreateObjectZipDownloadHandler", "#[cfg(test)]");

        assert!(
            handler_block.contains("authenticate_object_zip_download_request(&mut req).await?;"),
            "object ZIP download POST should authenticate the signed request"
        );
        assert!(
            handler_block.contains("authorize_object_zip_download_scope(&mut req, &request).await?;"),
            "object ZIP download POST should authorize the requested object/list scope"
        );
        assert!(
            !handler_block.contains("ServerInfoAdminAction"),
            "object ZIP download POST must not use broad server-info admin authorization for object reads"
        );
    }

    #[test]
    fn prefix_authorization_binds_body_prefix_to_list_bucket_query_conditions() {
        let src = include_str!("object_zip_download.rs");
        let scope_block = extract_block_between_markers(
            src,
            "async fn authorize_object_zip_download_scope",
            "async fn authorize_object_zip_download_s3_action",
        );

        assert!(
            scope_block.contains("listing_prefixes(request)"),
            "object ZIP download should authorize the actual prefixes used for listing"
        );
        assert!(
            !scope_block.contains("request.prefix.as_deref()"),
            "base prefix alone should not force ListBucket authorization for explicit object selection"
        );
        assert!(
            scope_block.contains("authorize_object_zip_download_list_prefix"),
            "prefix list authorization should bind body prefixes into s3:prefix conditions"
        );
    }

    fn extract_block_between_markers<'a>(src: &'a str, start_marker: &str, end_marker: &str) -> &'a str {
        let start = src
            .find(start_marker)
            .unwrap_or_else(|| panic!("Expected marker `{start_marker}` in source"));
        let after_start = &src[start..];
        let end = after_start
            .find(end_marker)
            .unwrap_or_else(|| panic!("Expected end marker `{end_marker}` in source"));
        &after_start[..end]
    }

    fn create_test_download_token(now: OffsetDateTime) -> CreatedObjectZipDownloadToken {
        ensure_test_signing_credentials();
        create_download_token("alice".to_string(), test_req_info(), test_auth_context(), valid_request(), now)
            .expect("token should be created")
    }

    fn ensure_test_signing_credentials() {
        if get_global_action_cred().is_none() {
            let _ = init_global_action_credentials(
                Some("TESTROOTACCESSKEY".to_string()),
                Some("TESTROOTSECRET1234567890".to_string()),
            );
        }
    }

    fn test_req_info() -> ReqInfo {
        let mut credentials = rustfs_credentials::Credentials {
            access_key: "alice".to_string(),
            secret_key: "should-not-be-serialized".to_string(),
            session_token: "should-not-be-serialized".to_string(),
            ..Default::default()
        };
        credentials.secret_key.clear();
        credentials.session_token.clear();
        ReqInfo {
            cred: Some(credentials),
            is_owner: false,
            bucket: Some("photos".to_string()),
            object: None,
            version_id: None,
            region: get_global_region(),
            request_context: None,
        }
    }

    fn test_auth_context() -> ObjectZipDownloadAuthContext {
        let mut headers = HeaderMap::new();
        headers.insert("user-agent", HeaderValue::from_static("object-zip-test"));
        ObjectZipDownloadAuthContext {
            headers,
            remote_addr: Some(RemoteAddr("127.0.0.1:9000".parse().expect("test socket addr should parse"))),
            client_info: Some(ClientInfo {
                real_ip: "203.0.113.10".parse().expect("test IP should parse"),
                forwarded_host: Some("console.example.test".to_string()),
                forwarded_proto: Some("https".to_string()),
                is_from_trusted_proxy: true,
                proxy_ip: Some("192.0.2.10".parse().expect("test IP should parse")),
                proxy_hops: 1,
                validation_mode: ValidationMode::HopByHop,
                warnings: Vec::new(),
            }),
        }
    }
}
