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

// Exercise the production validator at the protocol boundary without importing the server crate.
#[path = "../../../rustfs/src/storage/authorization/admission/request_validation.rs"]
mod request_validation;

use async_trait::async_trait;
use http::{Extensions, HeaderMap, Method, Request, StatusCode, Uri};
use http_body_util::BodyExt;
use request_validation::{RequestViolation, validate_method_subresource};
use s3s::access::{S3Access, S3AccessContext};
use s3s::auth::SimpleAuth;
use s3s::dto::{
    AbortMultipartUploadInput, AbortMultipartUploadOutput, DeleteBucketInput, DeleteBucketOutput, DeleteBucketPolicyInput,
    DeleteBucketPolicyOutput, DeleteObjectInput, DeleteObjectOutput, DeleteObjectTaggingInput, DeleteObjectTaggingOutput,
    GetBucketAclInput, GetBucketAclOutput, GetObjectAclInput, GetObjectAclOutput, GetObjectInput, GetObjectOutput,
    HeadBucketInput, HeadBucketOutput, PutBucketAclInput, PutBucketAclOutput, PutObjectAclInput, PutObjectAclOutput,
    StreamingBlob,
};
use s3s::host::SingleDomain;
use s3s::route::S3Route;
use s3s::service::{S3Service, S3ServiceBuilder};
use s3s::{Body, S3Request, S3Response, S3Result, s3_error};
use s3s_sigv4::{
    AmzDate, EMPTY_STRING_SHA256_HASH, Payload, calculate_signature, create_canonical_request,
    create_presigned_canonical_request, create_string_to_sign,
};
use std::sync::{Arc, Mutex};
use time::{OffsetDateTime, macros::format_description};

#[derive(Default)]
struct State {
    object_deleted: bool,
    bucket_deleted: bool,
    calls: Vec<(&'static str, Option<String>)>,
}

#[derive(Clone, Default)]
struct ProbeS3(Arc<Mutex<State>>);

#[async_trait]
impl s3s::S3 for ProbeS3 {
    async fn delete_object(&self, req: S3Request<DeleteObjectInput>) -> S3Result<S3Response<DeleteObjectOutput>> {
        let mut state = self.0.lock().expect("probe state");
        state.object_deleted = true;
        state.calls.push(("DeleteObject", req.input.version_id));
        Ok(S3Response::new(DeleteObjectOutput::default()))
    }

    async fn delete_bucket(&self, _: S3Request<DeleteBucketInput>) -> S3Result<S3Response<DeleteBucketOutput>> {
        let mut state = self.0.lock().expect("probe state");
        state.bucket_deleted = true;
        state.calls.push(("DeleteBucket", None));
        Ok(S3Response::new(DeleteBucketOutput::default()))
    }

    async fn get_object(&self, _: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
        if self.0.lock().expect("probe state").object_deleted {
            return Err(s3_error!(NoSuchKey));
        }
        Ok(S3Response::new(GetObjectOutput {
            body: Some(StreamingBlob::from(Body::from("original object".to_owned()))),
            ..Default::default()
        }))
    }

    async fn head_bucket(&self, _: S3Request<HeadBucketInput>) -> S3Result<S3Response<HeadBucketOutput>> {
        if self.0.lock().expect("probe state").bucket_deleted {
            return Err(s3_error!(NoSuchBucket));
        }
        Ok(S3Response::new(HeadBucketOutput::default()))
    }

    async fn delete_object_tagging(
        &self,
        req: S3Request<DeleteObjectTaggingInput>,
    ) -> S3Result<S3Response<DeleteObjectTaggingOutput>> {
        self.0
            .lock()
            .expect("probe state")
            .calls
            .push(("DeleteObjectTagging", req.input.version_id));
        Ok(S3Response::new(DeleteObjectTaggingOutput::default()))
    }

    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        self.0
            .lock()
            .expect("probe state")
            .calls
            .push(("AbortMultipartUpload", Some(req.input.upload_id)));
        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
    }

    async fn delete_bucket_policy(
        &self,
        _: S3Request<DeleteBucketPolicyInput>,
    ) -> S3Result<S3Response<DeleteBucketPolicyOutput>> {
        self.0.lock().expect("probe state").calls.push(("DeleteBucketPolicy", None));
        Ok(S3Response::new(DeleteBucketPolicyOutput::default()))
    }

    async fn get_object_acl(&self, _: S3Request<GetObjectAclInput>) -> S3Result<S3Response<GetObjectAclOutput>> {
        self.0.lock().expect("probe state").calls.push(("GetObjectAcl", None));
        Ok(S3Response::new(GetObjectAclOutput::default()))
    }

    async fn put_object_acl(&self, _: S3Request<PutObjectAclInput>) -> S3Result<S3Response<PutObjectAclOutput>> {
        self.0.lock().expect("probe state").calls.push(("PutObjectAcl", None));
        Ok(S3Response::new(PutObjectAclOutput::default()))
    }

    async fn get_bucket_acl(&self, _: S3Request<GetBucketAclInput>) -> S3Result<S3Response<GetBucketAclOutput>> {
        self.0.lock().expect("probe state").calls.push(("GetBucketAcl", None));
        Ok(S3Response::new(GetBucketAclOutput::default()))
    }

    async fn put_bucket_acl(&self, _: S3Request<PutBucketAclInput>) -> S3Result<S3Response<PutBucketAclOutput>> {
        self.0.lock().expect("probe state").calls.push(("PutBucketAcl", None));
        Ok(S3Response::new(PutBucketAclOutput::default()))
    }
}

struct CustomRoute;

struct MethodValidation;

#[async_trait]
impl S3Access for MethodValidation {
    async fn check(&self, context: &mut S3AccessContext<'_>) -> S3Result<()> {
        validate_method_subresource(context.method(), context.uri().query()).map_err(|violation| match violation {
            RequestViolation::DeleteAcl => s3_error!(MethodNotAllowed),
        })
    }
}

#[derive(Clone)]
struct CustomRouteMatched;

#[async_trait]
impl S3Route for CustomRoute {
    fn is_match(&self, _: &Method, uri: &Uri, _: &HeaderMap, extensions: &mut Extensions) -> bool {
        if uri.path() == "/custom" {
            extensions.insert(CustomRouteMatched);
            return true;
        }
        false
    }

    async fn check_access(&self, req: &mut S3Request<Body>) -> S3Result<()> {
        assert!(req.extensions.get::<CustomRouteMatched>().is_some());
        if req.headers.contains_key("x-test-deny") {
            return Err(s3_error!(AccessDenied));
        }
        assert!(req.credentials.is_some(), "custom route retains verified credentials");
        Ok(())
    }

    async fn call(&self, req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        assert!(req.extensions.get::<CustomRouteMatched>().is_some());
        Ok(S3Response::new(Body::from("custom route".to_owned())))
    }
}

fn service(virtual_host: bool) -> (S3Service, ProbeS3) {
    let probe = ProbeS3::default();
    let mut builder = S3ServiceBuilder::new(probe.clone());
    builder.set_auth(SimpleAuth::from_single("test-key", "test-secret"));
    builder.set_access(MethodValidation);
    builder.set_route(CustomRoute);
    let mut config = s3s::config::S3Config::default();
    config.enable_sig_v2 = true;
    config.normalize_forward_slash_path = true;
    builder.set_config(Arc::new(s3s::config::StaticConfigProvider::new(Arc::new(config))));
    if virtual_host {
        builder.set_host(SingleDomain::new("s3.example.test").expect("test domain"));
    }
    (builder.build(), probe)
}

fn signed_request(method: Method, uri: &str, presigned: bool, secret: &str) -> Request<Body> {
    signed_request_with_credentials(method, uri, presigned, "test-key", secret)
}

fn signed_request_with_credentials(method: Method, uri: &str, presigned: bool, access_key: &str, secret: &str) -> Request<Body> {
    let uri: Uri = uri.parse().expect("test URI");
    let host = uri.authority().expect("test authority").as_str();
    let timestamp = OffsetDateTime::now_utc()
        .format(format_description!("[year][month][day]T[hour][minute][second]Z"))
        .expect("format SigV4 timestamp");
    let date = AmzDate::parse(&timestamp).expect("test date");
    let credential = format!("{access_key}/{}/us-east-1/s3/aws4_request", date.fmt_date());
    let mut query: Vec<(String, String)> = url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
        .into_owned()
        .collect();
    let mut request = Request::builder()
        .method(method.clone())
        .header("host", host)
        .header("content-length", "0");
    if presigned {
        query.extend([
            ("X-Amz-Algorithm".into(), "AWS4-HMAC-SHA256".into()),
            ("X-Amz-Credential".into(), credential),
            ("X-Amz-Date".into(), timestamp),
            ("X-Amz-Expires".into(), "300".into()),
            ("X-Amz-SignedHeaders".into(), "host".into()),
        ]);
        let canonical = create_presigned_canonical_request(method.as_str(), uri.path(), &query, [("host", host)]);
        let to_sign = create_string_to_sign(&canonical, &date, "us-east-1", "s3");
        query.push(("X-Amz-Signature".into(), calculate_signature(&to_sign, secret, &date, "us-east-1", "s3")));
        let query = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs(&query)
            .finish();
        request = request.uri(format!("http://{host}{}?{query}", uri.path()));
    } else {
        let canonical = create_canonical_request(
            method.as_str(),
            uri.path(),
            &query,
            [
                ("host", host),
                ("x-amz-content-sha256", EMPTY_STRING_SHA256_HASH),
                ("x-amz-date", &timestamp),
            ],
            Payload::SingleChunk(EMPTY_STRING_SHA256_HASH),
        );
        let to_sign = create_string_to_sign(&canonical, &date, "us-east-1", "s3");
        let signature = calculate_signature(&to_sign, secret, &date, "us-east-1", "s3");
        request = request
            .uri(uri.clone())
            .header("x-amz-date", timestamp)
            .header("x-amz-content-sha256", EMPTY_STRING_SHA256_HASH)
            .header("authorization", format!("AWS4-HMAC-SHA256 Credential={credential}, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={signature}"));
    }
    request.body(Body::empty()).expect("signed request")
}

async fn response(service: &S3Service, request: Request<Body>) -> (StatusCode, String) {
    let response = service.call(request).await.expect("S3 service response");
    let status = response.status();
    let bytes = response.into_body().collect().await.expect("response body").to_bytes();
    (status, String::from_utf8(bytes.to_vec()).expect("UTF-8 test response"))
}

#[tokio::test]
async fn delete_acl_preserves_objects_and_buckets_for_signed_requests() {
    for virtual_host in [false, true] {
        let base = if virtual_host {
            "http://bucket.s3.example.test"
        } else {
            "http://s3.example.test/bucket"
        };
        for presigned in [false, true] {
            let (service, probe) = service(virtual_host);
            for path in ["", "/", "/key"] {
                for query in [
                    "acl",
                    "acl=",
                    "%61cl",
                    "a%63l=anything",
                    "acl&acl=",
                    "acl&versionId=version-1",
                    "acl&tagging",
                    "acl&uploadId=upload-1",
                ] {
                    let uri = format!("{base}{path}?{query}");
                    let (status, body) = response(&service, signed_request(Method::DELETE, &uri, presigned, "test-secret")).await;
                    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED, "{uri}: {body}");
                    assert!(body.contains("<Code>MethodNotAllowed</Code>"), "{uri}: {body}");
                }
                let operation = if path == "/key" { "DeleteObject" } else { "DeleteBucket" };
                let uri = format!("{base}{path}?acl&x-id={operation}");
                let (status, body) = response(&service, signed_request(Method::DELETE, &uri, presigned, "test-secret")).await;
                assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED, "{uri}: {body}");
            }
            assert!(
                probe.0.lock().expect("probe state").calls.is_empty(),
                "refused requests must not reach S3 handlers"
            );
            let (status, body) =
                response(&service, signed_request(Method::GET, &format!("{base}/key"), presigned, "test-secret")).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(body, "original object");
            let (status, _) =
                response(&service, signed_request(Method::HEAD, &format!("{base}/"), presigned, "test-secret")).await;
            assert_eq!(status, StatusCode::OK, "bucket must still exist");
        }
    }
}

#[tokio::test]
async fn delete_acl_keeps_signature_verification_before_method_rejection() {
    let (service, probe) = service(false);
    for presigned in [false, true] {
        for query in ["acl", "versionId=version-1"] {
            let uri = format!("http://s3.example.test/bucket/key?{query}");
            let (status, body) = response(&service, signed_request(Method::DELETE, &uri, presigned, "wrong-secret")).await;
            assert_eq!(status, StatusCode::FORBIDDEN);
            assert!(body.contains("<Code>SignatureDoesNotMatch</Code>"), "{body}");
        }
    }
    assert!(probe.0.lock().expect("probe state").calls.is_empty());
}

#[tokio::test]
async fn delete_acl_keeps_supported_s3_operations_and_parameters() {
    for presigned in [false, true] {
        for (method, path, operation, parameter) in [
            (Method::DELETE, "/bucket/key", "DeleteObject", None),
            (Method::DELETE, "/bucket/key?versionId=version-1", "DeleteObject", Some("version-1")),
            (
                Method::DELETE,
                "/bucket/key?versionId=null&x-id=DeleteObject",
                "DeleteObject",
                Some("null"),
            ),
            (Method::DELETE, "/bucket/key?custom=value&other=acl", "DeleteObject", None),
            (Method::DELETE, "/bucket/key?ACL&acl-extra&%2561cl", "DeleteObject", None),
            (
                Method::DELETE,
                "/bucket/key?tagging&versionId=version-1",
                "DeleteObjectTagging",
                Some("version-1"),
            ),
            (Method::DELETE, "/bucket/key?uploadId=upload-1", "AbortMultipartUpload", Some("upload-1")),
            (Method::DELETE, "/bucket", "DeleteBucket", None),
            (Method::DELETE, "/bucket?policy", "DeleteBucketPolicy", None),
            (Method::GET, "/bucket/key?acl", "GetObjectAcl", None),
            (Method::PUT, "/bucket/key?acl", "PutObjectAcl", None),
            (Method::GET, "/bucket?acl", "GetBucketAcl", None),
            (Method::PUT, "/bucket?acl", "PutBucketAcl", None),
        ] {
            let (service, probe) = service(false);
            let uri = format!("http://s3.example.test{path}");
            let (status, body) = response(&service, signed_request(method, &uri, presigned, "test-secret")).await;
            assert!(status.is_success(), "{operation} {uri}: {status} {body}");
            assert_eq!(probe.0.lock().expect("probe state").calls, [(operation, parameter.map(str::to_owned))]);
        }
    }
}

#[tokio::test]
async fn delete_acl_preserves_custom_route_matching_and_access() {
    let (service, probe) = service(false);
    for presigned in [false, true] {
        let uri = "http://s3.example.test/custom?acl";
        let (status, body) = response(&service, signed_request(Method::DELETE, uri, presigned, "test-secret")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "custom route");

        let mut denied = signed_request(Method::DELETE, uri, presigned, "test-secret");
        denied
            .headers_mut()
            .insert("x-test-deny", http::HeaderValue::from_static("true"));
        let (status, body) = response(&service, denied).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert!(body.contains("<Code>AccessDenied</Code>"));
    }
    assert!(probe.0.lock().expect("probe state").calls.is_empty());
}

#[tokio::test]
async fn delete_acl_preserves_sigv2_header_and_query_authentication() {
    for presigned in [false, true] {
        for (query, secret, expected_status, expected_code) in [
            ("acl", "test-secret", StatusCode::METHOD_NOT_ALLOWED, Some("MethodNotAllowed")),
            ("acl", "wrong-secret", StatusCode::FORBIDDEN, Some("SignatureDoesNotMatch")),
            ("versionId=version-1", "test-secret", StatusCode::NO_CONTENT, None),
        ] {
            let (service, probe) = service(false);
            let date = OffsetDateTime::now_utc()
                .format(format_description!(
                    "[weekday repr:short], [day] [month repr:short] [year] [hour]:[minute]:[second] GMT"
                ))
                .expect("format SigV2 HTTP date");
            let request = Request::builder()
                .method(Method::DELETE)
                .uri(format!("http://s3.example.test/bucket/key?{query}"))
                .header("host", "s3.example.test")
                .header("content-length", "0")
                .header("date", date)
                .body(Body::empty())
                .expect("SigV2 request");
            let request = if presigned {
                let mut request =
                    rustfs_signer::try_pre_sign_v2(request, "test-key", secret, 60, false).expect("presign SigV2 request");
                let uri = request.uri().to_string();
                let (prefix, signature) = uri.rsplit_once("&Signature=").expect("SigV2 signature");
                let encoded = url::form_urlencoded::Serializer::new(String::new())
                    .append_pair("Signature", signature)
                    .finish();
                *request.uri_mut() = format!("{prefix}&{encoded}").parse().expect("encoded SigV2 URI");
                request
            } else {
                rustfs_signer::try_sign_v2(request, 0, "test-key", secret, false).expect("sign SigV2 request")
            };
            let (status, body) = response(&service, request).await;
            assert_eq!(status, expected_status, "{query}: {body}");
            if let Some(code) = expected_code {
                assert!(body.contains(&format!("<Code>{code}</Code>")), "{body}");
                assert!(probe.0.lock().expect("probe state").calls.is_empty());
            } else {
                assert_eq!(probe.0.lock().expect("probe state").calls, [("DeleteObject", Some("version-1".into()))]);
            }
        }
    }
}

async fn live_delete(
    env: &crate::common::RustFSTestEnvironment,
    path: &str,
    presigned: bool,
    secret: &str,
) -> (StatusCode, String) {
    let request =
        signed_request_with_credentials(Method::DELETE, &format!("{}{path}", env.url), presigned, &env.access_key, secret);
    let response = crate::common::local_http_client()
        .delete(request.uri().to_string())
        .headers(request.headers().clone())
        .send()
        .await
        .expect("send signed DELETE to RustFS");
    let status = response.status();
    (status, response.text().await.expect("read DELETE response"))
}

#[tokio::test]
async fn delete_acl_admission_through_gateway_edge() {
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{BucketVersioningStatus, Tag, Tagging, VersioningConfiguration};

    let mut env = crate::common::RustFSTestEnvironment::new()
        .await
        .expect("isolated S3 environment");
    env.start_rustfs_server_without_cleanup_with_env(&[("RUSTFS_REGION", "us-east-1")])
        .await
        .expect("start RustFS");
    let client = env.create_s3_client();
    let bucket = "acl-admission";
    let empty_bucket = "acl-admission-empty";
    env.create_test_bucket(bucket).await.expect("create object bucket");
    env.create_test_bucket(empty_bucket).await.expect("create empty bucket");
    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .expect("enable versioning");
    let object = client
        .put_object()
        .bucket(bucket)
        .key("key")
        .body(ByteStream::from_static(b"original object"))
        .send()
        .await
        .expect("create preserved object");
    let version = object.version_id().expect("versioned object ID");
    let version_query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("versionId", version)
        .finish();
    // The gateway wire parser refuses escaped parameter names before signature verification.
    let encoded_path = format!("/{bucket}/key?%61cl&{version_query}");
    let (status, body) = live_delete(&env, &encoded_path, false, &env.secret_key).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{encoded_path}: {body}");
    assert!(body.contains("<Code>InvalidArgument</Code>"), "{body}");

    for presigned in [false, true] {
        // SigV4 canonicalization rejects repeated parameter names before admission.
        let repeated_path = format!("/{bucket}/key?acl&acl=&tagging");
        let (status, body) = live_delete(&env, &repeated_path, presigned, &env.secret_key).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{repeated_path}: {body}");
        assert!(body.contains("<Code>AuthorizationHeaderMalformed</Code>"), "{body}");

        for path in [
            format!("/{empty_bucket}?acl"),
            format!("/{bucket}/key?acl&{version_query}"),
            format!("/{bucket}/key?acl&tagging"),
            format!("/{bucket}/key?acl&uploadId=upload-1"),
        ] {
            let (status, body) = live_delete(&env, &path, presigned, &env.secret_key).await;
            assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED, "{path}: {body}");
            assert!(body.contains("<Code>MethodNotAllowed</Code>"), "{body}");
            let (status, body) = live_delete(&env, &path, presigned, "wrong-secret").await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{path}: {body}");
            assert!(body.contains("<Code>SignatureDoesNotMatch</Code>"), "{body}");
        }
        client
            .head_bucket()
            .bucket(empty_bucket)
            .send()
            .await
            .expect("refused DELETE preserves empty bucket");
        let current = client
            .get_object()
            .bucket(bucket)
            .key("key")
            .send()
            .await
            .expect("refused DELETE leaves the current object readable");
        assert_eq!(current.version_id(), Some(version));
        let body = current.body.collect().await.expect("read preserved object").into_bytes();
        assert_eq!(body.as_ref(), b"original object");

        client
            .put_object_tagging()
            .bucket(bucket)
            .key("key")
            .version_id(version)
            .tagging(
                Tagging::builder()
                    .tag_set(Tag::builder().key("test").value("acl").build().expect("tag"))
                    .build()
                    .expect("tagging"),
            )
            .send()
            .await
            .expect("set object tags");
        let path = format!("/{bucket}/key?tagging&{version_query}");
        let (status, body) = live_delete(&env, &path, presigned, &env.secret_key).await;
        assert!(status.is_success(), "{path}: {body}");
        let tags = client
            .get_object_tagging()
            .bucket(bucket)
            .key("key")
            .version_id(version)
            .send()
            .await
            .expect("read cleared tags");
        assert!(tags.tag_set().is_empty());

        let upload = client
            .create_multipart_upload()
            .bucket(bucket)
            .key("upload")
            .send()
            .await
            .expect("start multipart upload");
        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("uploadId", upload.upload_id().expect("upload ID"))
            .finish();
        let path = format!("/{bucket}/upload?{query}");
        let (status, body) = live_delete(&env, &path, presigned, &env.secret_key).await;
        assert!(status.is_success(), "{path}: {body}");
        let uploads = client
            .list_multipart_uploads()
            .bucket(bucket)
            .send()
            .await
            .expect("list remaining uploads");
        assert!(uploads.uploads().is_empty());
    }
    let path = format!("/{bucket}/key?{version_query}");
    let (status, body) = live_delete(&env, &path, false, &env.secret_key).await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{path}: {body}");
    let versions = client
        .list_object_versions()
        .bucket(bucket)
        .send()
        .await
        .expect("list remaining versions");
    assert!(versions.versions().is_empty());
    let (status, body) = live_delete(&env, &format!("/{empty_bucket}"), false, &env.secret_key).await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
    env.stop_server();
}
