#![allow(clippy::map_entry)]
use bytes::Bytes;
use futures::Future;
use http::{HeaderMap, HeaderName};
use http::{
    HeaderValue, Response, StatusCode,
    request::{Builder, Request},
};
use hyper_rustls::{ConfigBuilderExt, HttpsConnector};
use hyper_util::{client::legacy::Client, client::legacy::connect::HttpConnector, rt::TokioExecutor};
use rand::Rng;
use serde::{Deserialize, Serialize};
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
use tracing::{debug, error};
use url::{Url, form_urlencoded};
use uuid::Uuid;

use crate::client::bucket_cache::BucketLocationCache;
use crate::client::{
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
use crate::signer;
use crate::{checksum::ChecksumMode, store_api::GetObjectReader};
use reader::hasher::{MD5, Sha256};
use rustfs_rio::HashReader;
use rustfs_utils::{
    net::get_endpoint_url,
    retry::{MAX_RETRY, new_retry_timer},
};
use s3s::S3ErrorCode;
use s3s::dto::ReplicationStatus;
use s3s::{Body, dto::Owner};

const C_USER_AGENT_PREFIX: &str = "RustFS (linux; x86)";
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

pub struct TransitionClient {
    pub endpoint_url: Url,
    pub creds_provider: Arc<Mutex<Credentials<Static>>>,
    pub override_signer_type: SignatureType,
    /*app_info: TODO*/
    pub secure: bool,
    pub http_client: Client<HttpsConnector<HttpConnector>, Body>,
    //pub http_trace:      Httptrace.ClientTrace,
    pub bucket_loc_cache: Arc<Mutex<BucketLocationCache>>,
    pub is_trace_enabled: Arc<Mutex<bool>>,
    pub trace_errors_only: Arc<Mutex<bool>>,
    //pub trace_output: io.Writer,
    pub s3_accelerate_endpoint: Arc<Mutex<String>>,
    pub s3_dual_stack_enabled: Arc<Mutex<bool>>,
    pub region: String,
    pub random: u64,
    pub lookup: BucketLookupType,
    //pub lookupFn: func(u url.URL, bucketName string) BucketLookupType,
    pub md5_hasher: Arc<Mutex<Option<MD5>>>,
    pub sha256_hasher: Option<Sha256>,
    pub health_status: AtomicI32,
    pub trailing_header_support: bool,
    pub max_retries: i64,
}

#[derive(Debug, Default)]
pub struct Options {
    pub creds: Credentials<Static>,
    pub secure: bool,
    //pub transport:    http.RoundTripper,
    //pub trace:        *httptrace.ClientTrace,
    pub region: String,
    pub bucket_lookup: BucketLookupType,
    //pub custom_region_via_url: func(u url.URL) string,
    //pub bucket_lookup_via_url: func(u url.URL, bucketName string) BucketLookupType,
    pub trailing_headers: bool,
    pub custom_md5: Option<MD5>,
    pub custom_sha256: Option<Sha256>,
    pub max_retries: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
enum BucketLookupType {
    #[default]
    BucketLookupAuto,
    BucketLookupDNS,
    BucketLookupPath,
}

impl TransitionClient {
    pub async fn new(endpoint: &str, opts: Options) -> Result<TransitionClient, std::io::Error> {
        let clnt = Self::private_new(endpoint, opts).await?;

        Ok(clnt)
    }

    async fn private_new(endpoint: &str, opts: Options) -> Result<TransitionClient, std::io::Error> {
        let endpoint_url = get_endpoint_url(endpoint, opts.secure)?;

        //let jar = cookiejar.New(cookiejar.Options{PublicSuffixList: publicsuffix.List})?;

        //#[cfg(feature = "ring")]
        //let _ = rustls::crypto::ring::default_provider().install_default();
        //#[cfg(feature = "aws-lc-rs")]
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let scheme = endpoint_url.scheme();
        let client;
        //if scheme == "https" {
        //    client = Client::builder(TokioExecutor::new()).build_http();
        //} else {
        let tls = rustls::ClientConfig::builder().with_native_roots()?.with_no_client_auth();
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(tls)
            .https_or_http()
            .enable_http1()
            .build();
        client = Client::builder(TokioExecutor::new()).build(https);
        //}

        let mut clnt = TransitionClient {
            endpoint_url,
            creds_provider: Arc::new(Mutex::new(opts.creds)),
            override_signer_type: SignatureType::SignatureDefault,
            secure: opts.secure,
            http_client: client,
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
        };

        {
            let mut md5_hasher = clnt.md5_hasher.lock().unwrap();
            if md5_hasher.is_none() {
                *md5_hasher = Some(MD5::new());
            }
        }
        if clnt.sha256_hasher.is_none() {
            clnt.sha256_hasher = Some(Sha256::new());
        }

        clnt.trailing_header_support = opts.trailing_headers && clnt.override_signer_type == SignatureType::SignatureV4;

        if opts.max_retries > 0 {
            clnt.max_retries = opts.max_retries;
        }

        Ok(clnt)
    }

    fn endpoint_url(&self) -> Url {
        self.endpoint_url.clone()
    }

    fn set_appinfo(&self, app_name: &str, app_version: &str) {
        /*if app_name != "" && app_version != "" {
            self.appInfo.app_name = app_name
            self.appInfo.app_version = app_version
        }*/
    }

    fn trace_errors_only_off(&self) {
        let mut trace_errors_only = self.trace_errors_only.lock().unwrap();
        *trace_errors_only = false;
    }

    fn trace_off(&self) {
        let mut is_trace_enabled = self.is_trace_enabled.lock().unwrap();
        *is_trace_enabled = false;
        let mut trace_errors_only = self.trace_errors_only.lock().unwrap();
        *trace_errors_only = false;
    }

    fn set_s3_transfer_accelerate(&self, accelerate_endpoint: &str) {
        todo!();
    }

    fn set_s3_enable_dual_stack(&self, enabled: bool) {
        todo!();
    }

    pub fn hash_materials(
        &self,
        is_md5_requested: bool,
        is_sha256_requested: bool,
    ) -> (HashMap<String, MD5>, HashMap<String, Vec<u8>>) {
        todo!();
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
        todo!();
    }

    fn dump_http(&self, req: &http::Request<Body>, resp: &http::Response<Body>) -> Result<(), std::io::Error> {
        let mut resp_trace: Vec<u8>;

        //info!("{}{}", self.trace_output, "---------END-HTTP---------");

        Ok(())
    }

    pub async fn doit(&self, req: http::Request<Body>) -> Result<http::Response<Body>, std::io::Error> {
        let req_method;
        let req_uri;
        let req_headers;
        let resp;
        let http_client = self.http_client.clone();
        {
            //let mut http_client = http_client.lock().unwrap();
            req_method = req.method().clone();
            req_uri = req.uri().clone();
            req_headers = req.headers().clone();

            debug!("endpoint_url: {}", self.endpoint_url.as_str().to_string());
            resp = http_client.request(req);
        }
        let resp = resp
            .await /*.map_err(Into::into)*/
            .map(|res| res.map(Body::from));
        debug!("http_client url: {} {}", req_method, req_uri);
        debug!("http_client headers: {:?}", req_headers);
        if let Err(err) = resp {
            error!("http_client call error: {:?}", err);
            return Err(std::io::Error::other(err));
        }

        let mut resp = resp.unwrap();
        debug!("http_resp: {:?}", resp);

        //if self.is_trace_enabled && !(self.trace_errors_only && resp.status() == StatusCode::OK) {
        if resp.status() != StatusCode::OK {
            //self.dump_http(&cloned_req, &resp)?;
            let b = resp.body_mut().store_all_unlimited().await.unwrap().to_vec();
            debug!("err_body: {}", String::from_utf8(b).unwrap());
        }

        Ok(resp)
    }

    pub async fn execute_method(
        &self,
        method: http::Method,
        metadata: &mut RequestMetadata,
    ) -> Result<http::Response<Body>, std::io::Error> {
        if self.is_offline() {
            let mut s = self.endpoint_url.to_string();
            s.push_str(" is offline.");
            return Err(std::io::Error::other(s));
        }

        let mut retryable: bool;
        //let mut body_seeker: BufferReader;
        let mut req_retry = self.max_retries;
        let mut resp: http::Response<Body>;

        //if metadata.content_body != nil {
        //body_seeker = BufferReader::new(metadata.content_body.read_all().await?);
        retryable = true;
        if !retryable {
            req_retry = 1;
        }
        //}

        //let mut retry_timer = RetryTimer::new();
        //while let Some(v) = retry_timer.next().await {
        for _ in [1; 1]
        /*new_retry_timer(req_retry, DefaultRetryUnit, DefaultRetryCap, MaxJitter)*/
        {
            let req = self.new_request(method, metadata).await?;

            resp = self.doit(req).await?;

            for http_status in SUCCESS_STATUS {
                if http_status == resp.status() {
                    return Ok(resp);
                }
            }

            let b = resp.body_mut().store_all_unlimited().await.unwrap().to_vec();
            let err_response = http_resp_to_error_response(resp, b.clone(), &metadata.bucket_name, &metadata.object_name);

            if self.region == "" {
                match err_response.code {
                    S3ErrorCode::AuthorizationHeaderMalformed | S3ErrorCode::InvalidArgument /*S3ErrorCode::InvalidRegion*/ => {
                        //break;
                        return Err(std::io::Error::other(err_response));
                    }
                    S3ErrorCode::AccessDenied => {
                        if err_response.region == "" {
                            return Err(std::io::Error::other(err_response));
                        }
                        if metadata.bucket_name != "" {
                            let mut bucket_loc_cache = self.bucket_loc_cache.lock().unwrap();
                            let location = bucket_loc_cache.get(&metadata.bucket_name);
                            if location.is_some() && location.unwrap() != err_response.region {
                                bucket_loc_cache.set(&metadata.bucket_name, &err_response.region);
                                //continue;
                            }
                        } else {
                            if err_response.region != metadata.bucket_location {
                                metadata.bucket_location = err_response.region.clone();
                                //continue;
                            }
                        }
                        return Err(std::io::Error::other(err_response));
                    }
                    _ => {
                        return Err(std::io::Error::other(err_response));
                    }
                }
            }

            break;
        }

        Err(std::io::Error::other("resp err"))
    }

    async fn new_request(
        &self,
        method: http::Method,
        metadata: &mut RequestMetadata,
    ) -> Result<http::Request<Body>, std::io::Error> {
        let location = metadata.bucket_location.clone();
        if location == "" {
            if metadata.bucket_name != "" {
                let location = self.get_bucket_location(&metadata.bucket_name).await?;
            }
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

        let mut req_builder = Request::builder().method(method).uri(target_url.to_string());

        let value;
        {
            let mut creds_provider = self.creds_provider.lock().unwrap();
            value = creds_provider.get_with_context(Some(self.cred_context()))?;
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
                    "Presigned URLs cannot be generated with anonymous credentials.",
                )));
            }
            if metadata.extra_pre_sign_header.is_some() {
                if signer_type == SignatureType::SignatureV2 {
                    return Err(std::io::Error::other(err_invalid_argument(
                        "Extra signed headers for Presign with Signature V2 is not supported.",
                    )));
                }
                for (k, v) in metadata.extra_pre_sign_header.as_ref().unwrap() {
                    req_builder = req_builder.header(k, v);
                }
            }
            if signer_type == SignatureType::SignatureV2 {
                req_builder =
                    signer::pre_sign_v2(req_builder, &access_key_id, &secret_access_key, metadata.expires, is_virtual_host);
            } else if signer_type == SignatureType::SignatureV4 {
                req_builder = signer::pre_sign_v4(
                    req_builder,
                    &access_key_id,
                    &secret_access_key,
                    &session_token,
                    &location,
                    metadata.expires,
                    OffsetDateTime::now_utc(),
                );
            }
            let req = match req_builder.body(Body::empty()) {
                Ok(req) => req,
                Err(err) => {
                    return Err(std::io::Error::other(err));
                }
            };
            return Ok(req);
        }

        self.set_user_agent(&mut req_builder);

        for (k, v) in metadata.custom_header.clone() {
            req_builder.headers_mut().expect("err").insert(k.expect("err"), v);
        }

        //req.content_length = metadata.content_length;
        if metadata.content_length <= -1 {
            let chunked_value = HeaderValue::from_str(&vec!["chunked"].join(",")).expect("err");
            req_builder
                .headers_mut()
                .expect("err")
                .insert(http::header::TRANSFER_ENCODING, chunked_value);
        }

        if metadata.content_md5_base64.len() > 0 {
            let md5_value = HeaderValue::from_str(&metadata.content_md5_base64).expect("err");
            req_builder.headers_mut().expect("err").insert("Content-Md5", md5_value);
        }

        if signer_type == SignatureType::SignatureAnonymous {
            let req = match req_builder.body(Body::empty()) {
                Ok(req) => req,
                Err(err) => {
                    return Err(std::io::Error::other(err));
                }
            };
            return Ok(req);
        }

        if signer_type == SignatureType::SignatureV2 {
            req_builder =
                signer::sign_v2(req_builder, metadata.content_length, &access_key_id, &secret_access_key, is_virtual_host);
        } else if metadata.stream_sha256 && !self.secure {
            if metadata.trailer.len() > 0 {
                //req.Trailer = metadata.trailer;
                for (_, v) in &metadata.trailer {
                    req_builder = req_builder.header(http::header::TRAILER, v.clone());
                }
            }
            //req_builder = signer::streaming_sign_v4(req_builder, &access_key_id,
            //  &secret_access_key, &session_token, &location, metadata.content_length, OffsetDateTime::now_utc(), self.sha256_hasher());
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
            req_builder = req_builder
                .header::<HeaderName, HeaderValue>("X-Amz-Content-Sha256".parse().unwrap(), sha_header.parse().expect("err"));

            req_builder = signer::sign_v4_trailer(
                req_builder,
                &access_key_id,
                &secret_access_key,
                &session_token,
                &location,
                metadata.trailer.clone(),
            );
        }

        let req;
        if metadata.content_length == 0 {
            req = req_builder.body(Body::empty());
        } else {
            match &mut metadata.content_body {
                ReaderImpl::Body(content_body) => {
                    req = req_builder.body(Body::from(content_body.clone()));
                }
                ReaderImpl::ObjectBody(content_body) => {
                    req = req_builder.body(Body::from(content_body.read_all().await?));
                }
            }
            //req = req_builder.body(s3s::Body::from(metadata.content_body.read_all().await?));
        }

        match req {
            Ok(req) => Ok(req),
            Err(err) => Err(std::io::Error::other(err)),
        }
    }

    pub fn set_user_agent(&self, req: &mut Builder) {
        let mut headers = req.headers_mut().expect("err");
        headers.insert("User-Agent", C_USER_AGENT.parse().expect("err"));
        /*if self.app_info.app_name != "" && self.app_info.app_version != "" {
            headers.insert("User-Agent", C_USER_AGENT+" "+self.app_info.app_name+"/"+self.app_info.app_version);
        }*/
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
        let host = self.endpoint_url.host().unwrap();
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
        if bucket_name == "" {
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

struct LockedRandSource {
    src: u64, //rand.Source,
}

impl LockedRandSource {
    fn int63(&self) -> i64 {
        /*let n = self.src.int63();
        n*/
        todo!();
    }

    fn seed(&self, seed: i64) {
        //self.src.seed(seed);
        todo!();
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
        let client = TransitionClient::new(endpoint, opts).await?;
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
        todo!();
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
            size: size,
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
    pub etag: String,
    pub name: String,
    pub mod_time: OffsetDateTime,
    pub size: usize,
    pub content_type: Option<String>,
    #[serde(skip)]
    pub metadata: HeaderMap,
    pub user_metadata: HashMap<String, String>,
    pub user_tags: String,
    pub user_tag_count: i64,
    #[serde(skip)]
    pub owner: Owner,
    //pub grant: Vec<Grant>,
    pub storage_class: String,
    pub is_latest: bool,
    pub is_delete_marker: bool,
    pub version_id: Uuid,

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
            etag: "".to_string(),
            name: "".to_string(),
            mod_time: OffsetDateTime::now_utc(),
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
            version_id: Uuid::nil(),
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
    //pub err error,
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

pub fn to_object_info(bucket_name: &str, object_name: &str, h: &HeaderMap) -> Result<ObjectInfo, std::io::Error> {
    todo!()
}

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

//#[derive(Clone)]
pub struct SendRequest {
    inner: hyper::client::conn::http1::SendRequest<Body>,
}

impl From<hyper::client::conn::http1::SendRequest<Body>> for SendRequest {
    fn from(inner: hyper::client::conn::http1::SendRequest<Body>) -> Self {
        Self { inner }
    }
}

impl tower::Service<Request<Body>> for SendRequest {
    type Response = Response<Body>;
    type Error = std::io::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(std::io::Error::other)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        //let req = hyper::Request::builder().uri("/").body(http_body_util::Empty::<Bytes>::new()).unwrap();
        //let req = hyper::Request::builder().uri("/").body(Body::empty()).unwrap();

        let fut = self.inner.send_request(req);

        Box::pin(async move { fut.await.map_err(std::io::Error::other).map(|res| res.map(Body::from)) })
    }
}

#[derive(Serialize, Deserialize)]
pub struct Document(pub String);
