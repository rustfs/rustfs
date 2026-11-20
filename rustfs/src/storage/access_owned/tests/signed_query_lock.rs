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

//! A presigned PutObject cannot gain an Object Lock legal hold through its query string.
//!
//! Tests: `authorize_put_object` on the owned request for a presigned PUT whose legal hold is a
//! signed header (refused without `s3:PutObjectLegalHold`) and one whose legal hold and user
//! metadata are query parameters (served as an ordinary PUT: the query hoist restores the user
//! metadata and never the legal hold).
//! Not tested: SigV4 verification of the presigned URL, the PutObject decoder, or storing the
//! object.
//! Upstream: `storage::authorization::tests::fixture`. Downstream: none.

use crate::storage::access_owned::object_write::{PutObjectAuthorization, authorize_put_object};
use crate::storage::authorization::resolve_req_info;
use crate::storage::authorization::tests::fixture::request;
use crate::storage::storage_api::contract::bucket::{BucketOperations as _, MakeBucketOptions};
use crate::storage::storage_api::runtime_sources_consumer::{AppContext, ServerContextSlot};
use http::Method;
use rustfs_s3_types::S3ErrorCode;
use std::sync::Arc;

const ACCESS_KEY: &str = "review-local-access";
const SECRET_KEY: &str = "review-local-secret";
const LEGAL_HOLD: &str = "x-amz-object-lock-legal-hold";

/// The URI of a presigned SigV4 PUT. `hoisted` moves the legal hold, and adds user metadata, into
/// the query; otherwise the legal hold is a signed header. The signature is a placeholder: it was
/// verified before authorization, which only reads the presigned marker set.
fn presigned_lock_put_uri(bucket: &str, key: &str, hoisted: bool) -> String {
    let signed_headers = if hoisted {
        "host"
    } else {
        "host;x-amz-object-lock-legal-hold"
    };
    let mut params = std::collections::BTreeMap::from([
        ("X-Amz-Algorithm", "AWS4-HMAC-SHA256".to_owned()),
        ("X-Amz-Credential", format!("{ACCESS_KEY}/20260101/us-east-1/s3/aws4_request")),
        ("X-Amz-Date", "20260101T000000Z".to_owned()),
        ("X-Amz-Expires", "300".to_owned()),
        ("X-Amz-SignedHeaders", signed_headers.to_owned()),
    ]);
    if hoisted {
        params.insert(LEGAL_HOLD, "ON".to_owned());
        params.insert("x-amz-meta-user-id", "u1".to_owned());
    }
    let query = params
        .into_iter()
        .map(|(k, v)| format!("{}={}", urlencoding::encode(k), urlencoding::encode(&v)))
        .collect::<Vec<_>>()
        .join("&");
    format!("/{bucket}/{key}?{query}&X-Amz-Signature={}", "0".repeat(64))
}

#[tokio::test]
async fn signed_query_metadata_cannot_add_legal_hold_without_permission() {
    let store = crate::app::gating_test_env::shared_gating_ecstore().await;
    let iam = rustfs_iam::init_iam_sys(store.clone()).await.expect("test IAM");
    let context = Arc::new(AppContext::with_default_interfaces(
        store.clone(),
        iam,
        Arc::new(rustfs_kms::KmsServiceManager::new()),
    ));
    assert!(context.publish_action_credentials(rustfs_credentials::Credentials {
        access_key: ACCESS_KEY.to_owned(),
        secret_key: SECRET_KEY.to_owned(),
        status: "on".to_owned(),
        ..Default::default()
    }));
    let slot = ServerContextSlot::new();
    assert!(slot.install(context));
    let bucket = format!("review-lock-permission-{}", uuid::Uuid::new_v4().simple());
    store
        .make_bucket(
            &bucket,
            &MakeBucketOptions {
                lock_enabled: true,
                ..Default::default()
            },
        )
        .await
        .expect("locked bucket");
    let policy = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"*"}},"Action":["s3:PutObject"],"Resource":["arn:aws:s3:::{bucket}/*"]}},{{"Effect":"Deny","Principal":{{"AWS":"*"}},"Action":["s3:PutObjectLegalHold","s3:PutObjectRetention"],"Resource":["arn:aws:s3:::{bucket}/*"]}}]}}"#
    );
    let mut metadata = (*crate::storage::get_bucket_metadata(&bucket).await.expect("bucket metadata")).clone();
    metadata.policy_config = Some(serde_json::from_str(&policy).expect("bucket policy"));
    metadata.policy_config_json = policy.into_bytes();
    crate::storage::storage_api::set_bucket_metadata(bucket.clone(), metadata)
        .await
        .expect("set bucket policy");

    // The caller the access phase resolves for the signing key.
    let caller = resolve_req_info(&slot, Some(ACCESS_KEY), "", None)
        .await
        .expect("resolve the signing caller");
    let put = |key: &'static str, hoisted: bool| {
        let mut builder = request(Method::PUT)
            .uri(&presigned_lock_put_uri(&bucket, key, hoisted))
            .header("host", "localhost")
            .header("content-length", "4")
            .header("content-md5", "CY9rzUYh03PK3k6DJie09g==")
            .credentials(ACCESS_KEY, SECRET_KEY)
            .extension(caller.clone())
            .server_context(&slot);
        if !hoisted {
            builder = builder.header(LEGAL_HOLD, "ON");
        }
        builder.build()
    };
    // The facts the PutObject decoder derives: the legal hold is read from the headers only.
    let facts = |key: &'static str, legal_hold: bool| PutObjectAuthorization {
        bucket: &bucket,
        key,
        version_id: None,
        retain_until: None,
        specific_etag: false,
        legal_hold,
        retention: false,
        post: false,
    };

    let mut header_control = put("header-control", false);
    let legal_hold = header_control.headers().contains_key(LEGAL_HOLD);
    assert!(legal_hold, "the control sends its legal hold as a signed header");
    let error = authorize_put_object(&mut header_control, &slot, facts("header-control", legal_hold), |_| Ok(()))
        .await
        .expect_err("a signed legal-hold header must reach the independent permission check");
    assert_eq!(error.code(), &S3ErrorCode::AccessDenied, "header control: {error:?}");

    let mut query_target = put("query-target", true);
    let legal_hold = query_target.headers().contains_key(LEGAL_HOLD);
    authorize_put_object(&mut query_target, &slot, facts("query-target", legal_hold), |_| Ok(()))
        .await
        .expect("ordinary metadata PUT should be authorized");
    assert!(
        !query_target.headers().contains_key(LEGAL_HOLD),
        "query metadata must not grant a separate Object Lock capability"
    );
    assert_eq!(
        query_target
            .headers()
            .get("x-amz-meta-user-id")
            .and_then(|value| value.to_str().ok()),
        Some("u1"),
        "signed query user metadata is restored as a header"
    );
}
