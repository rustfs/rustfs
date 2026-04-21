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
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]
#![allow(unused_must_use)]
#![allow(clippy::all)]

use crate::client::bucket_cache::BucketLocationCache;
use crate::client::{
    api_error_response::ErrorResponse,
    api_error_response::{err_invalid_argument, http_resp_to_error_response, to_error_response},
    api_get_options::GetObjectOptions,
    api_put_object::PutObjectOptions,
    api_put_object_multipart::UploadPartParams,
    api_s3_datatypes::{
        CompleteMultipartUpload, CompletePart, ListBucketResult, ListBucketV2Result, ListMultipartUploadsResult,
        ListObjectPartsResult, ObjectPart,
    },
    constants::{UNSIGNED_PAYLOAD, UNSIGNED_PAYLOAD_TRAILER},
    credentials::{CredContext, Credentials, SignatureType, Static},
};
use crate::{client::checksum::ChecksumMode, store_api::GetObjectReader};
use futures::{Future, StreamExt};
use http::{HeaderMap, HeaderName};
use http::{
    HeaderValue, Response, StatusCode,
    request::{Builder, Request},
};
use http_body::Body;
use http_body_util::BodyExt;
use hyper::body::Bytes;
use hyper::body::Incoming;
use hyper_rustls::{ConfigBuilderExt, HttpsConnector};
use hyper_util::{client::legacy::Client, client::legacy::connect::HttpConnector, rt::TokioExecutor};
use md5::Digest;
use md5::Md5;
use rand::{Rng, RngExt};
use rustfs_config::MAX_S3_CLIENT_RESPONSE_SIZE;
use rustfs_rio::HashReader;
use rustfs_utils::HashAlgorithm;
use rustfs_utils::{
    net::get_endpoint_url,
    retry::{
        DEFAULT_RETRY_CAP, DEFAULT_RETRY_UNIT, MAX_JITTER, MAX_RETRY, RetryTimer, is_http_status_retryable, is_s3code_retryable,
    },
};
use s3s::S3ErrorCode;
use s3s::dto::Owner;
use s3s::dto::ReplicationStatus;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::io::Cursor;
use std::pin::Pin;
use std::sync::atomic::{AtomicI32, Ordering};
use std::task::{Context, Poll};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use time::Duration;
use time::OffsetDateTime;
use tokio::io::BufReader;
use tracing::{debug, error, warn};
use url::{Url, form_urlencoded};
use uuid::Uuid;

const C_USER_AGENT: &str = "RustFS (linux; x86)";

const SUCCESS_STATUS: [StatusCode; 3] = [StatusCode::OK, StatusCode::NO_CONTENT, StatusCode::PARTIAL_CONTENT];

const C_UNKNOWN: i32 = -1;
const C_OFFLINE: i32 = 0;
const C_ONLINE: i32 = 1;

//pub type ReaderImpl = Box<dyn Reader + Send + Sync + 'static>;
pub enum ReaderImpl {
    Body(Bytes),
    ObjectBody(GetObjectReader),
}

pub type ReadCloser = BufReader<Cursor<Vec<u8>>>;

#[derive(Debug)]
pub struct TransitionClient {
    pub endpoint_url: Url,
    pub creds_provider: Arc<Mutex<Credentials<Static>>>,
    pub override_signer_type: SignatureType,
    pub secure: bool,
    pub http_client: Client<HttpsConnector<HttpConnector>, s3s::Body>,
    pub bucket_loc_cache: Arc<Mutex<BucketLocationCache>>,
    pub is_trace_enabled: Arc<Mutex<bool>>,
    pub trace_errors_only: Arc<Mutex<bool>>,
    pub s3_accelerate_endpoint: Arc<Mutex<String>>,
    pub s3_dual_stack_enabled: Arc<Mutex<bool>>,
    pub region: String,
    pub random: u64,
    pub lookup: BucketLookupType,
    pub md5_hasher: Arc<Mutex<Option<HashAlgorithm>>>,
    pub sha256_hasher: Option<HashAlgorithm>,
    pub health_status: AtomicI32,
    pub trailing_header_support: bool,
    pub max_retries: i64,
    pub tier_type: String,
}

#[derive(Debug, Default)]
pub struct Options {
    pub creds: Credentials<Static>,
    pub secure: bool,
    pub region: String,
    pub bucket_lookup: BucketLookupType,
    pub trailing_headers: bool,
    pub custom_md5: Option<HashAlgorithm>,
    pub custom_sha256: Option<HashAlgorithm>,
    pub max_retries: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum BucketLookupType {
    #[default]
    BucketLookupAuto,
    BucketLookupDNS,
    BucketLookupPath,
}

fn load_root_store_from_tls_path() -> Option<rustls::RootCertStore> {
    // Load the root certificate bundle from the path specified by the
    // RUSTFS_TLS_PATH environment variable.
    let tp = rustfs_utils::get_env_str(rustfs_config::ENV_RUSTFS_TLS_PATH, rustfs_config::DEFAULT_RUSTFS_TLS_PATH);
    // If no TLS path is configured, do not fall back to a CA bundle in the current directory.
    if tp.is_empty() {
        return None;
    }
    let ca = std::path::Path::new(&tp).join(rustfs_config::RUSTFS_CA_CERT);
    if !ca.exists() {
        return None;
    }

    let der_list = rustfs_utils::load_cert_bundle_der_bytes(ca.to_str().unwrap_or_default()).ok()?;
    let mut store = rustls::RootCertStore::empty();
    for der in der_list {
        if let Err(e) = store.add(der.into()) {
            warn!("Warning: failed to add certificate from '{}' to root store: {e}", ca.display());
        }
    }
    Some(store)
}

fn panic_payload_to_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }

    if let Some(message) = payload.downcast_ref::<&'static str>() {
        return (*message).to_string();
    }

    "unknown panic payload".to_string()
}

fn with_rustls_init_guard<T, F>(build: F) -> Result<T, std::io::Error>
where
    F: FnOnce() -> Result<T, std::io::Error>,
{
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(build)).unwrap_or_else(|payload| {
        let panic_message = panic_payload_to_message(payload);
        Err(std::io::Error::other(format!(
            "failed to initialize rustls crypto provider: {panic_message}. Ensure exactly one rustls crypto provider feature is enabled (aws-lc-rs or ring), or install one with CryptoProvider::install_default()"
        )))
    })
}

fn build_tls_config() -> Result<rustls::ClientConfig, std::io::Error> {
    with_rustls_init_guard(|| {
        let config = if let Some(store) = load_root_store_from_tls_path() {
            rustls::ClientConfig::builder()
                .with_root_certificates(store)
                .with_no_client_auth()
        } else {
            rustls::ClientConfig::builder().with_native_roots()?.with_no_client_auth()
        };

        Ok(config)
    })
}

impl TransitionClient {
    pub async fn new(endpoint: &str, opts: Options, tier_type: &str) -> Result<TransitionClient, std::io::Error> {
        let client = Self::private_new(endpoint, opts, tier_type).await?;

        Ok(client)
    }

    async fn private_new(endpoint: &str, opts: Options, tier_type: &str) -> Result<TransitionClient, std::io::Error> {
        if rustls::crypto::CryptoProvider::get_default().is_none() {
            // No default provider is set yet; try to install aws-lc-rs.
            // `install_default` can only fail if another thread races us and installs a provider
            // between our check and this call, which is still safe to ignore.
            if rustls::crypto::aws_lc_rs::default_provider().install_default().is_err() {
                debug!("rustls crypto provider was installed concurrently, skipping aws-lc-rs install");
            }
        } else {
            debug!("rustls crypto provider already installed, skipping aws-lc-rs install");
        }

        let endpoint_url = get_endpoint_url(endpoint, opts.secure)?;

        let tls = build_tls_config()?;

        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(tls)
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .build();
        let http_client = Client::builder(TokioExecutor::new()).build(https);

        let mut client = TransitionClient {
            endpoint_url,
            creds_provider: Arc::new(Mutex::new(opts.creds)),
            override_signer_type: SignatureType::SignatureDefault,
            secure: opts.secure,
            http_client,
            bucket_loc_cache: Arc::new(Mutex::new(BucketLocationCache::new())),
            is_trace_enabled: Arc::new(Mutex::new(false)),
            trace_errors_only: Arc::new(Mutex::new(false)),
            s3_accelerate_endpoint: Arc::new(Mutex::new("".to_string())),
            s3_dual_stack_enabled: Arc::new(Mutex::new(false)),
            region: opts.region,
            random: rand::rng().random_range(10..=50),
            lookup: opts.bucket_lookup,
            md5_hasher: Arc::new(Mutex::new(opts.custom_md5)),
            sha256_hasher: opts.custom_sha256,
            health_status: AtomicI32::new(C_UNKNOWN),
            trailing_header_support: opts.trailing_headers,
            max_retries: opts.max_retries,
            tier_type: tier_type.to_string(),
        };

        {
            if let Ok(mut md5_hasher) = client.md5_hasher.lock() {
                if md5_hasher.is_none() {
                    *md5_hasher = Some(HashAlgorithm::Md5);
                }
            }
        }
        if client.sha256_hasher.is_none() {
            client.sha256_hasher = Some(HashAlgorithm::SHA256);
        }

        client.trailing_header_support = opts.trailing_headers && client.override_signer_type == SignatureType::SignatureV4;

        client.max_retries = MAX_RETRY;
        if opts.max_retries > 0 {
            client.max_retries = opts.max_retries;
        }

        Ok(client)
    }

    fn endpoint_url(&self) -> Url {
        self.endpoint_url.clone()
    }

    fn trace_errors_only_off(&self) {
        if let Ok(mut trace_errors_only) = self.trace_errors_only.lock() {
            *trace_errors_only = false;
        }
    }

    fn trace_off(&self) {
        if let Ok(mut is_trace_enabled) = self.is_trace_enabled.lock() {
            *is_trace_enabled = false;
        }
        if let Ok(mut trace_errors_only) = self.trace_errors_only.lock() {
            *trace_errors_only = false;
        }
    }

    fn set_s3_transfer_accelerate(&self, accelerate_endpoint: &str) {
        if let Ok(mut endpoint) = self.s3_accelerate_endpoint.lock() {
            *endpoint = accelerate_endpoint.to_string();
        }
    }

    fn set_s3_enable_dual_stack(&self, enabled: bool) {
        if let Ok(mut dual_stack) = self.s3_dual_stack_enabled.lock() {
            *dual_stack = enabled;
        }
    }

    pub fn hash_materials(
        &self,
        is_md5_requested: bool,
        is_sha256_requested: bool,
    ) -> (HashMap<String, HashAlgorithm>, HashMap<String, Vec<u8>>) {
        // `hash_algos` declares which algorithms are active for this multipart upload.
        // `hash_sums` keeps the current part digest bytes and is refreshed on every loop.
        let mut hash_algos = HashMap::new();
        let mut hash_sums = HashMap::new();

        if is_md5_requested {
            hash_algos.insert("md5".to_string(), HashAlgorithm::Md5);
            hash_sums.insert("md5".to_string(), vec![]);
        }

        if is_sha256_requested {
            hash_algos.insert("sha256".to_string(), HashAlgorithm::SHA256);
            hash_sums.insert("sha256".to_string(), vec![]);
        }

        (hash_algos, hash_sums)
    }

    fn is_online(&self) -> bool {
        !self.is_offline()
    }

    fn mark_offline(&self) {
        self.health_status
            .compare_exchange(C_ONLINE, C_OFFLINE, Ordering::SeqCst, Ordering::SeqCst);
    }

    fn is_offline(&self) -> bool {
        self.health_status.load(Ordering::SeqCst) == C_OFFLINE
    }

    fn health_check(hc_duration: Duration) {
        let _ = hc_duration;
    }

    fn dump_http(&self, req: &Request<s3s::Body>, resp: &Response<Incoming>) -> Result<(), std::io::Error> {
        let mut resp_trace: Vec<u8>;

        //info!("{}{}", self.trace_output, "---------BEGIN-HTTP---------");
        //info!("{}{}", self.trace_output, "---------END-HTTP---------");

        Ok(())
    }

    pub async fn doit(&self, req: Request<s3s::Body>) -> Result<Response<Incoming>, std::io::Error> {
        let req_method;
        let req_uri;
        let req_headers;
        let resp;
        let http_client = self.http_client.clone();
        {
            req_method = req.method().clone();
            req_uri = req.uri().clone();
            req_headers = req.headers().clone();

            debug!("endpoint_url: {}", self.endpoint_url.as_str().to_string());
            resp = http_client.request(req);
        }
        let resp = resp.await;
        debug!("http_client url: {} {}", req_method, req_uri);
        debug!("http_client headers: {:?}", req_headers);
        if let Err(err) = resp {
            error!("http_client call error: {:?}", err);
            return Err(std::io::Error::other(err));
        }

        let resp = match resp {
            Ok(r) => r,
            Err(_) => return Err(std::io::Error::other("Unexpected error in response")),
        };
        debug!("http_resp: {:?}", resp);

        //let b = resp.body_mut().store_all_unlimited().await.unwrap().to_vec();
        //debug!("http_resp_body: {}", String::from_utf8(b).unwrap());

        //if self.is_trace_enabled && !(self.trace_errors_only && resp.status() == StatusCode::OK) {
        if !resp.status().is_success() {
            //self.dump_http(&cloned_req, &resp)?;
            let mut body_vec = Vec::new();
            let mut body = resp.into_body();
            while let Some(frame) = body.frame().await {
                let frame = frame.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
                if let Some(data) = frame.data_ref() {
                    body_vec.extend_from_slice(data);
                }
            }
            let body_str = String::from_utf8_lossy(&body_vec);
            warn!("err_body: {}", body_str);
            Err(std::io::Error::other(format!("http_client call error: {}", body_str)))
        } else {
            Ok(resp)
        }
    }

    pub async fn execute_method(
        &self,
        method: http::Method,
        metadata: &mut RequestMetadata,
    ) -> Result<Response<Incoming>, std::io::Error> {
        if self.is_offline() {
            let mut s = self.endpoint_url.to_string();
            s.push_str(" is offline.");
            return Err(std::io::Error::other(s));
        }

        let retryable: bool;
        //let mut body_seeker: BufferReader;
        let mut req_retry = self.max_retries;
        let mut resp: Response<Incoming>;

        //if metadata.content_body != nil {
        //body_seeker = BufferReader::new(metadata.content_body.read_all().await?);
        retryable = true;
        if !retryable {
            req_retry = 1;
        }
        //}

        let mut retry_timer = RetryTimer::new(req_retry, DEFAULT_RETRY_UNIT, DEFAULT_RETRY_CAP, MAX_JITTER, self.random);
        while retry_timer.next().await.is_some() {
            let req = self.new_request(&method, metadata).await?;

            resp = self.doit(req).await?;

            for http_status in SUCCESS_STATUS {
                if http_status == resp.status() {
                    return Ok(resp);
                }
            }

            let resp_status = resp.status();
            let h = resp.headers().clone();

            let mut body_vec = Vec::new();
            let mut body = resp.into_body();
            while let Some(frame) = body.frame().await {
                let frame = frame.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
                if let Some(data) = frame.data_ref() {
                    body_vec.extend_from_slice(data);
                }
            }
            let mut err_response =
                http_resp_to_error_response(resp_status, &h, body_vec.clone(), &metadata.bucket_name, &metadata.object_name);
            err_response.message = format!("remote tier error: {}", err_response.message);

            if self.region == "" {
                return match err_response.code {
                    S3ErrorCode::AuthorizationHeaderMalformed | S3ErrorCode::InvalidArgument /*S3ErrorCode::InvalidRegion*/ => {
                        //break;
                        Err(std::io::Error::other(err_response))
                    }
                    S3ErrorCode::AccessDenied => {
                        if err_response.region == "" {
                            return Err(std::io::Error::other(err_response));
                        }
                        if metadata.bucket_name != "" {
                            if let Ok(mut bucket_loc_cache) = self.bucket_loc_cache.lock() {
                                if let Some(location) = bucket_loc_cache.get(&metadata.bucket_name) {
                                    if location != err_response.region {
                                        bucket_loc_cache.set(&metadata.bucket_name, &err_response.region);
                                        //continue;
                                    }
                                }
                            }
                        } else if err_response.region != metadata.bucket_location {
                            metadata.bucket_location = err_response.region.clone();
                            //continue;
                        }
                        Err(std::io::Error::other(err_response))
                    }
                    _ => {
                        Err(std::io::Error::other(err_response))
                    }
                };
            }

            if is_s3code_retryable(err_response.code.as_str()) {
                continue;
            }

            if is_http_status_retryable(&resp_status) {
                continue;
            }

            break;
        }

        Err(std::io::Error::other("resp err"))
    }

    async fn new_request(
        &self,
        method: &http::Method,
        metadata: &mut RequestMetadata,
    ) -> Result<Request<s3s::Body>, std::io::Error> {
        let mut location = metadata.bucket_location.clone();
        if location == "" && metadata.bucket_name != "" {
            location = self.get_bucket_location(&metadata.bucket_name).await?;
        }

        let is_makebucket = metadata.object_name == "" && method == http::Method::PUT && metadata.query_values.len() == 0;
        let is_virtual_host = self.is_virtual_host_style_request(&self.endpoint_url, &metadata.bucket_name) && !is_makebucket;

        let target_url = self.make_target_url(
            &metadata.bucket_name,
            &metadata.object_name,
            &location,
            is_virtual_host,
            &metadata.query_values,
        )?;

        let Ok(mut req) = Request::builder()
            .method(method)
            .uri(target_url.to_string())
            .body(s3s::Body::empty())
        else {
            return Err(std::io::Error::other("create request error"));
        };

        let value;
        {
            if let Ok(mut creds_provider) = self.creds_provider.lock() {
                value = creds_provider.get_with_context(Some(self.cred_context()))?;
            } else {
                return Err(std::io::Error::other("Failed to acquire credentials provider lock"));
            }
        }

        let mut signer_type = value.signer_type.clone();
        let access_key_id = value.access_key_id;
        let secret_access_key = value.secret_access_key;
        let session_token = value.session_token;

        if self.override_signer_type != SignatureType::SignatureDefault {
            signer_type = self.override_signer_type.clone();
        }

        if value.signer_type == SignatureType::SignatureAnonymous {
            signer_type = SignatureType::SignatureAnonymous;
        }

        if metadata.expires != 0 && metadata.pre_sign_url {
            if signer_type == SignatureType::SignatureAnonymous {
                return Err(std::io::Error::other(err_invalid_argument(
                    "presigned urls cannot be generated with anonymous credentials.",
                )));
            }
            if metadata.extra_pre_sign_header.is_some() {
                if signer_type == SignatureType::SignatureV2 {
                    return Err(std::io::Error::other(err_invalid_argument(
                        "extra signed headers for presign with signature v2 is not supported.",
                    )));
                }
                let headers = req.headers_mut();
                if let Some(extra_headers) = metadata.extra_pre_sign_header.as_ref() {
                    for (k, v) in extra_headers {
                        headers.insert(k, v.clone());
                    }
                }
            }
            if signer_type == SignatureType::SignatureV2 {
                req = rustfs_signer::pre_sign_v2(req, &access_key_id, &secret_access_key, metadata.expires, is_virtual_host);
            } else if signer_type == SignatureType::SignatureV4 {
                req = rustfs_signer::pre_sign_v4(
                    req,
                    &access_key_id,
                    &secret_access_key,
                    &session_token,
                    &location,
                    metadata.expires,
                    OffsetDateTime::now_utc(),
                );
            }
            return Ok(req);
        }

        self.set_user_agent(&mut req);

        for (k, v) in metadata.custom_header.clone() {
            if let Some(key) = k {
                req.headers_mut().insert(key, v);
            }
        }

        //req.content_length = metadata.content_length;
        if metadata.content_length <= -1 {
            if let Ok(chunked_value) = HeaderValue::from_str(&vec!["chunked"].join(",")) {
                req.headers_mut().insert(http::header::TRANSFER_ENCODING, chunked_value);
            }
        }

        if metadata.content_md5_base64.len() > 0 {
            if let Ok(md5_value) = HeaderValue::from_str(&metadata.content_md5_base64) {
                req.headers_mut().insert("Content-Md5", md5_value);
            }
        }

        if signer_type == SignatureType::SignatureAnonymous {
            return Ok(req);
        }

        if signer_type == SignatureType::SignatureV2 {
            req = rustfs_signer::sign_v2(req, metadata.content_length, &access_key_id, &secret_access_key, is_virtual_host);
        } else if metadata.stream_sha256 && !self.secure {
            if metadata.trailer.len() > 0 {
                for (_, v) in &metadata.trailer {
                    req.headers_mut().insert(http::header::TRAILER, v.clone());
                }
            }
        } else {
            let mut sha_header = UNSIGNED_PAYLOAD.to_string();
            if metadata.content_sha256_hex != "" {
                sha_header = metadata.content_sha256_hex.clone();
                if metadata.trailer.len() > 0 {
                    return Err(std::io::Error::other("internal error: content_sha256_hex with trailer not supported"));
                }
            } else if metadata.trailer.len() > 0 {
                sha_header = UNSIGNED_PAYLOAD_TRAILER.to_string();
            }
            let header_name = "X-Amz-Content-Sha256"
                .parse::<HeaderName>()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
            let header_value = sha_header
                .parse()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
            req.headers_mut().insert(header_name, header_value);

            req = rustfs_signer::sign_v4_trailer(
                req,
                &access_key_id,
                &secret_access_key,
                &session_token,
                &location,
                metadata.trailer.clone(),
            );
        }

        if metadata.content_length > 0 {
            match &mut metadata.content_body {
                ReaderImpl::Body(content_body) => {
                    *req.body_mut() = s3s::Body::from(content_body.clone());
                }
                ReaderImpl::ObjectBody(content_body) => {
                    *req.body_mut() = s3s::Body::from(content_body.read_all().await?);
                }
            }
        }

        Ok(req)
    }

    pub fn set_user_agent(&self, req: &mut Request<s3s::Body>) {
        let headers = req.headers_mut();
        headers.insert("User-Agent", HeaderValue::from_static(C_USER_AGENT));
    }

    fn make_target_url(
        &self,
        bucket_name: &str,
        object_name: &str,
        bucket_location: &str,
        is_virtual_host_style: bool,
        query_values: &HashMap<String, String>,
    ) -> Result<Url, std::io::Error> {
        let scheme = self.endpoint_url.scheme();
        let host = self
            .endpoint_url
            .host()
            .ok_or_else(|| std::io::Error::other("Endpoint URL has no host"))?;
        let default_port = if scheme == "https" { 443 } else { 80 };
        let port = self.endpoint_url.port().unwrap_or(default_port);

        let mut url_str = format!("{scheme}://{host}:{port}/");

        if bucket_name != "" {
            if is_virtual_host_style {
                url_str = format!("{scheme}://{bucket_name}.{host}:{port}/");
                if object_name != "" {
                    url_str.push_str(object_name);
                }
            } else {
                url_str.push_str(bucket_name);
                url_str.push_str("/");
                if object_name != "" {
                    url_str.push_str(object_name);
                }
            }
        }

        if query_values.len() > 0 {
            let mut encoded = form_urlencoded::Serializer::new(String::new());
            for (k, v) in query_values {
                encoded.append_pair(&k, &v);
            }
            url_str.push_str("?");
            url_str.push_str(&encoded.finish());
        }

        Url::parse(&url_str).map_err(|e| std::io::Error::other(e.to_string()))
    }

    pub fn is_virtual_host_style_request(&self, url: &Url, bucket_name: &str) -> bool {
        // Contract:
        // - return true if we should use virtual-hosted-style addressing (bucket as subdomain)
        // Heuristics (aligned with AWS S3/MinIO clients):
        // - explicit DNS mode => true
        // - explicit PATH mode => false
        // - AUTO:
        //   - bucket must be non-empty and DNS compatible
        //   - endpoint host must be a DNS name (not an IPv4/IPv6 literal)
        //   - when using TLS (https), buckets with dots are avoided due to wildcard/cert issues
        if bucket_name.is_empty() {
            return false;
        }

        if self.lookup == BucketLookupType::BucketLookupDNS {
            return true;
        }

        if self.lookup == BucketLookupType::BucketLookupPath {
            return false;
        }

        false
    }

    pub fn cred_context(&self) -> CredContext {
        CredContext {
            //client: http_client,
            endpoint: self.endpoint_url.to_string(),
        }
    }
}

pub struct RequestMetadata {
    pub pre_sign_url: bool,
    pub bucket_name: String,
    pub object_name: String,
    pub query_values: HashMap<String, String>,
    pub custom_header: HeaderMap,
    pub extra_pre_sign_header: Option<HeaderMap>,
    pub expires: i64,
    pub bucket_location: String,
    pub content_body: ReaderImpl,
    pub content_length: i64,
    pub content_md5_base64: String,
    pub content_sha256_hex: String,
    pub stream_sha256: bool,
    pub add_crc: ChecksumMode,
    pub trailer: HeaderMap,
}

pub struct TransitionCore(pub Arc<TransitionClient>);

impl TransitionCore {
    pub async fn new(endpoint: &str, opts: Options) -> Result<Self, std::io::Error> {
        let client = TransitionClient::new(endpoint, opts, "").await?;
        Ok(Self(Arc::new(client)))
    }

    pub fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        marker: &str,
        delimiter: &str,
        max_keys: i64,
    ) -> Result<ListBucketResult, std::io::Error> {
        let client = self.0.clone();
        client.list_objects_query(bucket, prefix, marker, delimiter, max_keys, HeaderMap::new())
    }

    pub async fn list_objects_v2(
        &self,
        bucket_name: &str,
        object_prefix: &str,
        start_after: &str,
        continuation_token: &str,
        delimiter: &str,
        max_keys: i64,
    ) -> Result<ListBucketV2Result, std::io::Error> {
        let client = self.0.clone();
        client
            .list_objects_v2_query(
                bucket_name,
                object_prefix,
                continuation_token,
                true,
                false,
                delimiter,
                start_after,
                max_keys,
                HeaderMap::new(),
            )
            .await
    }

    /*pub fn copy_object(&self, source_bucket: &str, source_object: &str, dest_bucket: &str, dest_object: &str, metadata: HashMap<String, String>, src_opts: CopySrcOptions, dst_opts: PutObjectOptions) -> Result<ObjectInfo> {
        self.0.copy_object_do(source_bucket, source_object, dest_bucket, dest_object, metadata, src_opts, dst_opts)
    }*/

    pub fn copy_object_part(
        &self,
        src_bucket: &str,
        src_object: &str,
        dest_bucket: &str,
        dest_object: &str,
        upload_id: &str,
        part_id: i32,
        start_offset: i32,
        length: i64,
        metadata: HashMap<String, String>,
    ) -> Result<CompletePart, std::io::Error> {
        //self.0.copy_object_part_do(src_bucket, src_object, dest_bucket, dest_object, upload_id,
        //    part_id, start_offset, length, metadata)
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            crate::client::credentials::ErrorResponse {
                sts_error: crate::client::credentials::STSError {
                    r#type: "".to_string(),
                    code: "NotImplemented".to_string(),
                    message: format!(
                        "copy_object_part is not implemented for {src_bucket}/{src_object} -> {dest_bucket}/{dest_object}"
                    ),
                },
                request_id: "".to_string(),
            },
        ))
    }

    pub async fn put_object(
        &self,
        bucket: &str,
        object: &str,
        data: ReaderImpl,
        size: i64,
        md5_base64: &str,
        sha256_hex: &str,
        opts: &PutObjectOptions,
    ) -> Result<UploadInfo, std::io::Error> {
        let hook_reader = data; //newHook(data, opts.progress);
        let client = self.0.clone();
        client
            .put_object_do(bucket, object, hook_reader, md5_base64, sha256_hex, size, opts)
            .await
    }

    pub async fn new_multipart_upload(
        &self,
        bucket: &str,
        object: &str,
        opts: PutObjectOptions,
    ) -> Result<String, std::io::Error> {
        let client = self.0.clone();
        let result = client.initiate_multipart_upload(bucket, object, &opts).await?;
        Ok(result.upload_id)
    }

    pub fn list_multipart_uploads(
        &self,
        bucket: &str,
        prefix: &str,
        key_marker: &str,
        upload_id_marker: &str,
        delimiter: &str,
        max_uploads: i64,
    ) -> Result<ListMultipartUploadsResult, std::io::Error> {
        let client = self.0.clone();
        client.list_multipart_uploads_query(bucket, key_marker, upload_id_marker, prefix, delimiter, max_uploads)
    }

    pub async fn put_object_part(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_id: i64,
        data: ReaderImpl,
        size: i64,
        opts: PutObjectPartOptions,
    ) -> Result<ObjectPart, std::io::Error> {
        let mut p = UploadPartParams {
            bucket_name: bucket.to_string(),
            object_name: object.to_string(),
            upload_id: upload_id.to_string(),
            reader: data,
            part_number: part_id,
            md5_base64: opts.md5_base64,
            sha256_hex: opts.sha256_hex,
            size,
            //sse:           opts.sse,
            stream_sha256: !opts.disable_content_sha256,
            custom_header: opts.custom_header,
            trailer: opts.trailer,
        };
        let client = self.0.clone();
        client.upload_part(&mut p).await
    }

    pub async fn list_object_parts(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_number_marker: i64,
        max_parts: i64,
    ) -> Result<ListObjectPartsResult, std::io::Error> {
        let client = self.0.clone();
        client
            .list_object_parts_query(bucket, object, upload_id, part_number_marker, max_parts)
            .await
    }

    pub async fn complete_multipart_upload(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        parts: &[CompletePart],
        opts: PutObjectOptions,
    ) -> Result<UploadInfo, std::io::Error> {
        let client = self.0.clone();
        let res = client
            .complete_multipart_upload(bucket, object, upload_id, CompleteMultipartUpload { parts: parts.to_vec() }, &opts)
            .await?;
        Ok(res)
    }

    pub async fn abort_multipart_upload(&self, bucket_name: &str, object: &str, upload_id: &str) -> Result<(), std::io::Error> {
        let client = self.0.clone();
        client.abort_multipart_upload(bucket_name, object, upload_id).await
    }

    pub async fn get_bucket_policy(&self, bucket_name: &str) -> Result<String, std::io::Error> {
        let client = self.0.clone();
        client.get_bucket_policy(bucket_name).await
    }

    pub async fn put_bucket_policy(&self, bucket_name: &str, bucket_policy: &str) -> Result<(), std::io::Error> {
        let client = self.0.clone();
        client.put_bucket_policy(bucket_name, bucket_policy).await
    }

    pub async fn get_object(
        &self,
        bucket_name: &str,
        object_name: &str,
        opts: &GetObjectOptions,
    ) -> Result<(ObjectInfo, HeaderMap, ReadCloser), std::io::Error> {
        let client = self.0.clone();
        client.get_object_inner(bucket_name, object_name, opts).await
    }
}

#[derive(Debug, Clone, Default)]
pub struct PutObjectPartOptions {
    pub md5_base64: String,
    pub sha256_hex: String,
    //pub sse: encrypt.ServerSide,
    pub custom_header: HeaderMap,
    pub trailer: HeaderMap,
    pub disable_content_sha256: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ObjectInfo {
    pub etag: Option<String>,
    pub name: String,
    pub mod_time: Option<OffsetDateTime>,
    pub size: i64,
    pub content_type: Option<String>,
    #[serde(skip)]
    pub metadata: HeaderMap,
    pub user_metadata: HashMap<String, String>,
    pub user_tags: String,
    pub user_tag_count: usize,
    #[serde(skip)]
    pub owner: Owner,
    //pub grant: Vec<Grant>,
    pub storage_class: String,
    pub is_latest: bool,
    pub is_delete_marker: bool,
    pub version_id: Option<Uuid>,

    #[serde(skip, default = "replication_status_default")]
    pub replication_status: ReplicationStatus,
    pub replication_ready: bool,
    pub expiration: OffsetDateTime,
    pub expiration_rule_id: String,
    pub num_versions: usize,

    pub restore: RestoreInfo,

    pub checksum_crc32: String,
    pub checksum_crc32c: String,
    pub checksum_sha1: String,
    pub checksum_sha256: String,
    pub checksum_crc64nvme: String,
    pub checksum_mode: String,
}

fn replication_status_default() -> ReplicationStatus {
    ReplicationStatus::from_static(ReplicationStatus::PENDING)
}

impl Default for ObjectInfo {
    fn default() -> Self {
        Self {
            etag: None,
            name: "".to_string(),
            mod_time: None,
            size: 0,
            content_type: None,
            metadata: HeaderMap::new(),
            user_metadata: HashMap::new(),
            user_tags: "".to_string(),
            user_tag_count: 0,
            owner: Owner::default(),
            storage_class: "".to_string(),
            is_latest: false,
            is_delete_marker: false,
            version_id: None,
            replication_status: ReplicationStatus::from_static(ReplicationStatus::PENDING),
            replication_ready: false,
            expiration: OffsetDateTime::now_utc(),
            expiration_rule_id: "".to_string(),
            num_versions: 0,
            restore: RestoreInfo::default(),
            checksum_crc32: "".to_string(),
            checksum_crc32c: "".to_string(),
            checksum_sha1: "".to_string(),
            checksum_sha256: "".to_string(),
            checksum_crc64nvme: "".to_string(),
            checksum_mode: "".to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RestoreInfo {
    ongoing_restore: bool,
    expiry_time: OffsetDateTime,
}

impl Default for RestoreInfo {
    fn default() -> Self {
        Self {
            ongoing_restore: false,
            expiry_time: OffsetDateTime::now_utc(),
        }
    }
}

pub struct ObjectMultipartInfo {
    pub initiated: OffsetDateTime,
    //pub initiator: initiator,
    //pub owner:     owner,
    pub storage_class: String,
    pub key: String,
    pub size: i64,
    pub upload_id: String,
    //pub err: Error,
}

pub struct UploadInfo {
    pub bucket: String,
    pub key: String,
    pub etag: String,
    pub size: i64,
    pub last_modified: OffsetDateTime,
    pub location: String,
    pub version_id: String,
    pub expiration: OffsetDateTime,
    pub expiration_rule_id: String,
    pub checksum_crc32: String,
    pub checksum_crc32c: String,
    pub checksum_sha1: String,
    pub checksum_sha256: String,
    pub checksum_crc64nvme: String,
    pub checksum_mode: String,
}

impl Default for UploadInfo {
    fn default() -> Self {
        Self {
            bucket: "".to_string(),
            key: "".to_string(),
            etag: "".to_string(),
            size: 0,
            last_modified: OffsetDateTime::now_utc(),
            location: "".to_string(),
            version_id: "".to_string(),
            expiration: OffsetDateTime::now_utc(),
            expiration_rule_id: "".to_string(),
            checksum_crc32: "".to_string(),
            checksum_crc32c: "".to_string(),
            checksum_sha1: "".to_string(),
            checksum_sha256: "".to_string(),
            checksum_crc64nvme: "".to_string(),
            checksum_mode: "".to_string(),
        }
    }
}

/// Convert HTTP headers to ObjectInfo struct
/// This function parses various S3 response headers to construct an ObjectInfo struct
/// containing metadata about an S3 object.
pub fn to_object_info(bucket_name: &str, object_name: &str, h: &HeaderMap) -> Result<ObjectInfo, std::io::Error> {
    // Helper function to get header value as string
    let get_header = |name: &str| -> String { h.get(name).and_then(|val| val.to_str().ok()).unwrap_or("").to_string() };

    // Get and process the ETag
    let etag = {
        let etag_raw = get_header("ETag");
        // Remove surrounding quotes if present (trimming ETag)
        let trimmed = etag_raw.trim_start_matches('"').trim_end_matches('"');
        Some(trimmed.to_string())
    };

    // Parse content length if it exists
    let size = {
        let content_length_str = get_header("Content-Length");
        if !content_length_str.is_empty() {
            content_length_str
                .parse::<i64>()
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "Content-Length is not an integer"))?
        } else {
            -1
        }
    };

    // Parse Last-Modified time
    let mod_time = {
        let last_modified_str = get_header("Last-Modified");
        if !last_modified_str.is_empty() {
            // Parse HTTP date format (RFC 7231)
            // Using time crate to parse HTTP dates
            let parsed_time = OffsetDateTime::parse(&last_modified_str, &time::format_description::well_known::Rfc2822)
                .or_else(|_| OffsetDateTime::parse(&last_modified_str, &time::format_description::well_known::Rfc3339))
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "Last-Modified time format is invalid"))?;
            Some(parsed_time)
        } else {
            Some(OffsetDateTime::now_utc())
        }
    };

    // Get content type
    let content_type = {
        let content_type_raw = get_header("Content-Type");
        let content_type_trimmed = content_type_raw.trim();
        if content_type_trimmed.is_empty() {
            Some("application/octet-stream".to_string())
        } else {
            Some(content_type_trimmed.to_string())
        }
    };

    // Parse Expires time
    let expiration = {
        let expiry_str = get_header("Expires");
        if !expiry_str.is_empty() {
            OffsetDateTime::parse(&expiry_str, &time::format_description::well_known::Rfc2822)
                .or_else(|_| OffsetDateTime::parse(&expiry_str, &time::format_description::well_known::Rfc3339))
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "'Expires' is not in supported format"))?
        } else {
            OffsetDateTime::now_utc()
        }
    };

    // Extract user metadata (headers prefixed with "X-Amz-Meta-")
    let user_metadata = {
        let mut meta = HashMap::new();
        for (name, value) in h.iter() {
            let header_name = name.as_str().to_lowercase();
            if header_name.starts_with("x-amz-meta-") {
                if let Some(key) = header_name.strip_prefix("x-amz-meta-") {
                    if let Ok(value_str) = value.to_str() {
                        meta.insert(key.to_string(), value_str.to_string());
                    }
                }
            }
        }
        meta
    };

    let user_tag = {
        let user_tag_str = get_header("X-Amz-Tagging");
        user_tag_str
    };

    // Extract user tags count
    let user_tag_count = {
        let count_str = get_header("x-amz-tagging-count");
        if !count_str.is_empty() {
            count_str
                .parse::<usize>()
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "x-amz-tagging-count is not an integer"))?
        } else {
            0
        }
    };

    // Handle restore info
    let restore = {
        let restore_hdr = get_header("x-amz-restore");
        if !restore_hdr.is_empty() {
            // Simplified restore header parsing - in real implementation, this would parse the specific format
            // "ongoing-request=\"true\"" or "ongoing-request=\"false\", expiry-date=\"..."
            let ongoing_restore = restore_hdr.contains("ongoing-request=\"true\"");
            RestoreInfo {
                ongoing_restore,
                expiry_time: if ongoing_restore {
                    OffsetDateTime::now_utc()
                } else {
                    // Try to extract expiry date from the header
                    // This is simplified - real parsing would be more complex
                    OffsetDateTime::now_utc()
                },
            }
        } else {
            RestoreInfo::default()
        }
    };

    // Extract version ID
    let version_id = {
        let version_id_str = get_header("x-amz-version-id");
        if !version_id_str.is_empty() {
            Some(Uuid::parse_str(&version_id_str).unwrap_or_else(|_| Uuid::nil()))
        } else {
            None
        }
    };

    // Check if it's a delete marker
    let is_delete_marker = get_header("x-amz-delete-marker") == "true";

    // Get replication status
    let replication_status = {
        let status_str = get_header("x-amz-replication-status");
        ReplicationStatus::from_static(match status_str.as_str() {
            "COMPLETE" => ReplicationStatus::COMPLETE,
            "PENDING" => ReplicationStatus::PENDING,
            "FAILED" => ReplicationStatus::FAILED,
            "REPLICA" => ReplicationStatus::REPLICA,
            _ => ReplicationStatus::PENDING,
        })
    };

    // Extract expiration rule ID and time (simplified)
    let (expiration_time, expiration_rule_id) = {
        // In a real implementation, this would parse the x-amz-expiration header
        // which typically has format: "expiry-date="Fri, 11 Dec 2020 00:00:00 GMT", rule-id="myrule""
        let exp_header = get_header("x-amz-expiration");
        if !exp_header.is_empty() {
            // Simplified parsing - real implementation would be more thorough
            (OffsetDateTime::now_utc(), exp_header) // Placeholder
        } else {
            (OffsetDateTime::now_utc(), "".to_string())
        }
    };

    // Extract checksums
    let checksum_crc32 = get_header("x-amz-checksum-crc32");
    let checksum_crc32c = get_header("x-amz-checksum-crc32c");
    let checksum_sha1 = get_header("x-amz-checksum-sha1");
    let checksum_sha256 = get_header("x-amz-checksum-sha256");
    let checksum_crc64nvme = get_header("x-amz-checksum-crc64nvme");
    let checksum_mode = get_header("x-amz-checksum-mode");

    // Build and return the ObjectInfo struct
    Ok(ObjectInfo {
        etag,
        name: object_name.to_string(),
        mod_time,
        size,
        content_type,
        metadata: h.clone(),
        user_metadata,
        user_tags: "".to_string(), // Tags would need separate parsing
        user_tag_count,
        owner: Owner::default(),
        storage_class: get_header("x-amz-storage-class"),
        is_latest: true, // Would be determined by versioning settings
        is_delete_marker,
        version_id,
        replication_status,
        replication_ready: false, // Would be computed based on status
        expiration: expiration_time,
        expiration_rule_id,
        num_versions: 1, // Would be determined by versioning
        restore,
        checksum_crc32,
        checksum_crc32c,
        checksum_sha1,
        checksum_sha256,
        checksum_crc64nvme,
        checksum_mode,
    })
}

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

//#[derive(Clone)]
pub struct SendRequest {
    inner: hyper::client::conn::http1::SendRequest<s3s::Body>,
}

impl From<hyper::client::conn::http1::SendRequest<s3s::Body>> for SendRequest {
    fn from(inner: hyper::client::conn::http1::SendRequest<s3s::Body>) -> Self {
        Self { inner }
    }
}

impl tower::Service<Request<s3s::Body>> for SendRequest {
    type Response = Response<Incoming>;
    type Error = std::io::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(std::io::Error::other)
    }

    fn call(&mut self, req: Request<s3s::Body>) -> Self::Future {
        //let req = hyper::Request::builder().uri("/").body(http_body_util::Empty::<Bytes>::new()).unwrap();
        //let req = hyper::Request::builder().uri("/").body(Body::empty()).unwrap();

        let fut = self.inner.send_request(req);

        Box::pin(async move { fut.await.map_err(std::io::Error::other) })
    }
}

#[derive(Serialize, Deserialize)]
pub struct LocationConstraint {
    #[serde(rename = "$value")]
    pub field: String,
}

#[derive(Serialize, Deserialize)]
pub struct CreateBucketConfiguration {
    #[serde(rename = "LocationConstraint")]
    pub location_constraint: String,
}

#[cfg(test)]
mod tests {
    use super::{build_tls_config, load_root_store_from_tls_path, with_rustls_init_guard};

    #[test]
    fn rustls_guard_converts_panics_to_io_errors() {
        let err = with_rustls_init_guard(|| -> Result<(), std::io::Error> { panic!("missing provider") })
            .expect_err("panic should be converted into an io::Error");
        assert!(
            err.to_string().contains("missing provider"),
            "expected panic message to be preserved, got: {err}"
        );
    }

    #[test]
    fn build_tls_config_returns_result_without_panicking() {
        let outcome = std::panic::catch_unwind(build_tls_config);
        assert!(outcome.is_ok(), "TLS config creation should not panic");
    }

    /// When RUSTFS_TLS_PATH is not set, `load_root_store_from_tls_path` must return `None`
    /// (i.e. it must not silently look for a CA bundle in the current working directory).
    #[test]
    fn tls_path_unset_returns_none() {
        let result = temp_env::with_var_unset(rustfs_config::ENV_RUSTFS_TLS_PATH, || load_root_store_from_tls_path());
        assert!(result.is_none(), "expected None when RUSTFS_TLS_PATH is unset, but got a root store");
    }

    /// When RUSTFS_TLS_PATH is set to an empty string, `load_root_store_from_tls_path` must
    /// return `None` to avoid accidentally trusting a CA bundle in the current directory.
    #[test]
    fn tls_path_empty_returns_none() {
        let result = temp_env::with_var(rustfs_config::ENV_RUSTFS_TLS_PATH, Some(""), || load_root_store_from_tls_path());
        assert!(result.is_none(), "expected None when RUSTFS_TLS_PATH is empty, but got a root store");
    }

    /// Installing the rustls crypto provider when one is already set must not panic or return
    /// an error that surfaces to callers (the race-safe `get_default` check guards the install).
    #[test]
    fn provider_install_is_idempotent() {
        // Install once (may already be set by another test in this binary — that's fine).
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        // A second install attempt on an already-set provider must not panic.
        let outcome = std::panic::catch_unwind(|| {
            if rustls::crypto::CryptoProvider::get_default().is_none() {
                let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
            }
            // If a default is already present, the branch above is simply skipped.
        });
        assert!(outcome.is_ok(), "provider install guard must not panic when a provider is already set");
    }
}
