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

//! `S3Router::admit`, the access rule both edges call, over owned inputs only.

use super::{AdminOperation, S3Router};
use crate::admin::runtime_sources::ServerContextSlot;
use crate::server::console_prefix;
use http::{HeaderMap, HeaderValue, Method, Uri};
use rustfs_s3_types::{S3Error, S3ErrorCode};

const V4_REQUIRED: &str = "Header or query Signature Version 4 is required for this operation";
const SIGNATURE_REQUIRED: &str = "Signature is required";
const SIGV4: &str = "AWS4-HMAC-SHA256 Credential=AKIA/20260101/us-east-1/s3/aws4_request, \
                     SignedHeaders=host;x-amz-content-sha256;x-amz-date, \
                     Signature=0000000000000000000000000000000000000000000000000000000000000000";

fn router(console: bool) -> S3Router<AdminOperation> {
    S3Router::new(console)
}

fn headers(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in pairs {
        headers.append(*name, HeaderValue::from_static(value));
    }
    headers
}

fn sigv4() -> HeaderMap {
    headers(&[
        ("authorization", SIGV4),
        ("x-amz-date", "20260101T000000Z"),
        ("x-amz-content-sha256", "UNSIGNED-PAYLOAD"),
    ])
}

fn admit(
    router: &S3Router<AdminOperation>,
    method: Method,
    target: &str,
    headers: &HeaderMap,
    signed: bool,
) -> Result<(), S3Error> {
    let uri: Uri = target.parse().expect("static target");
    router.admit(&method, &uri, headers, signed)
}

fn assert_refused(result: Result<(), S3Error>, code: S3ErrorCode, message: &str, label: &str) {
    let error = result.expect_err(label);
    assert_eq!(error.code(), &code, "{label}");
    assert_eq!(error.message(), Some(message), "{label}");
}

#[test]
fn an_anonymous_admin_request_is_refused_under_both_prefixes_and_every_method() {
    for (method, target) in [
        (Method::GET, "/rustfs/admin/v3/info"),
        (Method::PUT, "/rustfs/admin/v3/add-user?accessKey=a"),
        (Method::GET, "/minio/admin/v3/info"),
        (Method::DELETE, "/minio/admin/v3/remove-user?accessKey=a"),
        (Method::GET, "/iceberg/v1/config"),
        (Method::GET, "/profile/cpu"),
    ] {
        let label = format!("{method} {target}");
        assert_refused(
            admit(&router(false), method, target, &HeaderMap::new(), false),
            S3ErrorCode::AccessDenied,
            SIGNATURE_REQUIRED,
            &label,
        );
    }
}

#[test]
fn a_sigv2_signature_is_refused_on_admin_extension_and_sts_routes() {
    let header_v2 = headers(&[("authorization", "AWS AKIA:c2lnbmF0dXJl")]);
    let form = headers(&[
        ("authorization", "AWS AKIA:c2lnbmF0dXJl"),
        ("content-type", "application/x-www-form-urlencoded"),
    ]);
    for (method, target, headers) in [
        (Method::GET, "/rustfs/admin/v3/info", &header_v2),
        (Method::PUT, "/bucket?replication-reset", &header_v2),
        (Method::GET, "/bucket/key?lambdaArn=arn", &header_v2),
        (Method::POST, "/", &form),
        (
            Method::GET,
            "/rustfs/admin/v3/info?AWSAccessKeyId=a&Expires=1&Signature=s",
            &HeaderMap::new(),
        ),
        (Method::GET, "/bucket?events=s3:ObjectCreated:*&Signature=s", &HeaderMap::new()),
    ] {
        let label = format!("{method} {target}");
        assert_refused(
            admit(&router(false), method, target, headers, true),
            S3ErrorCode::AccessDenied,
            V4_REQUIRED,
            &label,
        );
    }
}

#[test]
fn a_signed_multipart_post_is_refused_as_a_post_policy() {
    let policy = headers(&[("content-type", "multipart/form-data; boundary=x")]);
    for target in ["/rustfs/admin/v3/info", "/minio/admin/v3/import-iam"] {
        assert_refused(
            admit(&router(false), Method::POST, target, &policy, true),
            S3ErrorCode::AccessDenied,
            V4_REQUIRED,
            target,
        );
    }
}

#[test]
fn an_anonymous_extension_or_object_lambda_request_is_refused() {
    for (method, target) in [
        (Method::PUT, "/bucket?replication-reset"),
        (Method::GET, "/bucket?replication-metrics"),
        (Method::GET, "/bucket?events=s3:ObjectCreated:*"),
        (Method::GET, "/?events=s3:ObjectCreated:*"),
        (Method::GET, "/bucket/key?lambdaArn=arn"),
        // Object Lambda takes an admin-looking object path ahead of the admin rule.
        (Method::GET, "/rustfs/admin/v3/info?lambdaArn=arn"),
    ] {
        let label = format!("{method} {target}");
        assert_refused(
            admit(&router(false), method, target, &HeaderMap::new(), false),
            S3ErrorCode::AccessDenied,
            SIGNATURE_REQUIRED,
            &label,
        );
    }
}

#[test]
fn the_anonymous_sts_exemption_needs_an_unsigned_form_post() {
    for (method, target, headers) in [
        (Method::POST, "/", HeaderMap::new()),
        (Method::POST, "/", headers(&[("content-type", "application/json")])),
        (Method::GET, "/", headers(&[("content-type", "application/x-www-form-urlencoded")])),
    ] {
        let label = format!("{method} {target} {headers:?}");
        assert_refused(
            admit(&router(false), method, target, &headers, false),
            S3ErrorCode::AccessDenied,
            SIGNATURE_REQUIRED,
            &label,
        );
    }
}

#[test]
fn a_zip_download_without_a_token_or_by_post_is_refused() {
    for (method, target) in [
        (Method::GET, "/rustfs/admin/v3/object-zip-downloads/id.zip"),
        (Method::GET, "/rustfs/admin/v3/object-zip-downloads/id.zip?token="),
        (Method::GET, "/rustfs/admin/v3/object-zip-downloads/id?token=t"),
        (Method::POST, "/rustfs/admin/v3/object-zip-downloads/id.zip?token=t"),
    ] {
        let label = format!("{method} {target}");
        assert_refused(
            admit(&router(false), method, target, &HeaderMap::new(), false),
            S3ErrorCode::AccessDenied,
            SIGNATURE_REQUIRED,
            &label,
        );
    }
}

#[test]
fn a_console_path_is_anonymous_only_while_the_console_is_enabled() {
    let path = format!("{}/index.html", console_prefix());
    assert_refused(
        admit(&router(false), Method::GET, &path, &HeaderMap::new(), false),
        S3ErrorCode::AccessDenied,
        SIGNATURE_REQUIRED,
        "console disabled",
    );
    assert!(admit(&router(true), Method::GET, &path, &HeaderMap::new(), false).is_ok());
}

#[test]
fn a_server_without_its_context_refuses_everything_but_health() {
    let mut router = router(false);
    router.set_server_ctx(ServerContextSlot::new());
    for (method, target) in [
        (Method::GET, "/rustfs/admin/v3/info"),
        (Method::PUT, "/bucket?replication-reset"),
        (Method::POST, "/"),
    ] {
        let label = format!("{method} {target}");
        assert_refused(
            admit(&router, method, target, &sigv4(), true),
            S3ErrorCode::ServiceUnavailable,
            "server context is not ready",
            &label,
        );
    }
    for method in [Method::GET, Method::HEAD] {
        assert!(admit(&router, method.clone(), "/health", &HeaderMap::new(), false).is_ok(), "{method}");
    }
}

#[test]
fn an_unsigned_amz_header_on_a_sigv4_request_is_refused() {
    let mut forged = sigv4();
    forged.insert("x-amz-meta-forged", HeaderValue::from_static("1"));
    let expected = crate::auth::reject_unsigned_amz_headers_on_sigv4_request(&forged, None).expect_err("the rule refuses it");
    let refusal = admit(&router(false), Method::GET, "/rustfs/admin/v3/info", &forged, true).expect_err("refused");
    assert_eq!(refusal.code(), &S3ErrorCode::from(expected.code().clone()));
}

#[test]
fn a_sigv4_caller_is_admitted_on_every_claimed_route() {
    for (method, target) in [
        (Method::GET, "/rustfs/admin/v3/info"),
        (Method::PUT, "/minio/admin/v3/add-user?accessKey=a"),
        (Method::PUT, "/bucket?replication-reset"),
        (Method::GET, "/bucket/key?lambdaArn=arn"),
        (Method::GET, "/profile/cpu"),
    ] {
        assert!(admit(&router(false), method.clone(), target, &sigv4(), true).is_ok(), "{method} {target}");
    }
}

#[test]
fn the_anonymous_exemptions_admit_without_a_signature() {
    let form = headers(&[("content-type", "application/x-www-form-urlencoded; charset=utf-8")]);
    for (method, target, headers) in [
        (Method::GET, "/health", HeaderMap::new()),
        (Method::HEAD, "/health/ready", HeaderMap::new()),
        (Method::POST, "/", form),
        (Method::GET, "/rustfs/admin/v3/oidc/providers", HeaderMap::new()),
        (Method::GET, "/minio/admin/v3/oidc/authorize/provider", HeaderMap::new()),
        (Method::GET, "/rustfs/admin/v3/object-zip-downloads/id.zip?token=t", HeaderMap::new()),
    ] {
        assert!(
            admit(&router(false), method.clone(), target, &headers, false).is_ok(),
            "{method} {target}"
        );
    }
}
