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

//! E2E: conditional write enforcement with the `s3:if-none-match` and `s3:if-match`
//! condition keys in a **bucket policy**, an **IAM identity policy**, and an **STS
//! AssumeRole session policy**. Covers PutObject and CompleteMultipartUpload, the two
//! operations that accept the conditional headers.

use crate::common::{
    AdminTransport, RustFSTestEnvironment, admin_add_canned_policy_via, admin_attach_user_policy_via, admin_create_user,
    awscurl_post_sts_form_urlencoded, build_test_s3_config, init_logging,
};
use aws_sdk_s3::Client;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use tracing::info;
use uuid::Uuid;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn assert_access_denied<E: ProvideErrorMetadata + std::fmt::Debug, R: std::fmt::Debug>(err: &SdkError<E, R>, context: &str) {
    assert_eq!(
        err.as_service_error().and_then(ProvideErrorMetadata::code),
        Some("AccessDenied"),
        "{context}: {err:?}"
    );
}

async fn admin_add_and_attach_policy(
    env: &RustFSTestEnvironment,
    policy_name: &str,
    policy_json: &str,
    username: &str,
) -> TestResult {
    admin_add_canned_policy_via(
        AdminTransport::Signed,
        &env.url,
        &env.access_key,
        &env.secret_key,
        policy_name,
        policy_json,
    )
    .await?;
    admin_attach_user_policy_via(AdminTransport::Signed, &env.url, &env.access_key, &env.secret_key, policy_name, username).await
}

/// Starts a one-part multipart upload as the admin so that only the complete step
/// runs under the policy being tested.
async fn admin_start_upload(
    admin: &Client,
    bucket: &str,
    key: &str,
) -> Result<(String, CompletedMultipartUpload), Box<dyn std::error::Error + Send + Sync>> {
    let upload = admin.create_multipart_upload().bucket(bucket).key(key).send().await?;
    let upload_id = upload.upload_id().ok_or("missing upload id")?.to_string();
    let part = admin
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"multipart-conditional-write"))
        .send()
        .await?;
    let completed = CompletedMultipartUpload::builder()
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .set_e_tag(part.e_tag().map(str::to_string))
                .build(),
        )
        .build();
    Ok((upload_id, completed))
}

/// Bucket policy in the shape of the AWS enforcement example: PutObject is allowed only
/// when the request carries `If-None-Match`.
#[tokio::test]
async fn test_e2e_bucket_policy_requires_if_none_match() -> TestResult {
    init_logging();
    let suffix = Uuid::new_v4();
    let user = format!("e2econdwrite-{suffix}");
    let user_secret = "longSecretKeyForCondWrite1!";
    let bucket = format!("e2e-cond-write-bkt-{suffix}");

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;
    let admin = env.create_s3_client();
    admin_create_user(&env, &user, user_secret).await?;
    admin.create_bucket().bucket(&bucket).send().await?;

    let bucket_policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": { "AWS": [user.clone()] },
            "Action": ["s3:PutObject"],
            "Resource": [format!("arn:aws:s3:::{bucket}/*")],
            "Condition": { "Null": { "s3:if-none-match": "false" } }
        }]
    })
    .to_string();
    admin
        .put_bucket_policy()
        .bucket(&bucket)
        .policy(&bucket_policy)
        .send()
        .await?;

    let client = env.create_s3_client_with_credentials(&user, user_secret);

    let denied = client
        .put_object()
        .bucket(&bucket)
        .key("plain.txt")
        .body(ByteStream::from_static(b"plain"))
        .send()
        .await
        .expect_err("PutObject without If-None-Match must be denied");
    assert_access_denied(&denied, "unconditional PutObject");

    client
        .put_object()
        .bucket(&bucket)
        .key("created.txt")
        .if_none_match("*")
        .body(ByteStream::from_static(b"created"))
        .send()
        .await?;

    let overwrite = client
        .put_object()
        .bucket(&bucket)
        .key("created.txt")
        .if_none_match("*")
        .body(ByteStream::from_static(b"overwrite"))
        .send()
        .await
        .expect_err("If-None-Match: * must not overwrite an existing key");
    assert_eq!(
        overwrite.raw_response().map(|response| response.status().as_u16()),
        Some(412),
        "allowed conditional write must still evaluate the precondition: {overwrite:?}"
    );

    let (upload_id, completed) = admin_start_upload(&admin, &bucket, "multipart.bin").await?;
    let denied = client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key("multipart.bin")
        .upload_id(&upload_id)
        .multipart_upload(completed.clone())
        .send()
        .await
        .expect_err("CompleteMultipartUpload without If-None-Match must be denied");
    assert_access_denied(&denied, "unconditional CompleteMultipartUpload");

    client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key("multipart.bin")
        .upload_id(&upload_id)
        .if_none_match("*")
        .multipart_upload(completed)
        .send()
        .await?;

    for key in ["created.txt", "multipart.bin"] {
        let _ = admin.delete_object().bucket(&bucket).key(key).send().await;
    }
    let _ = admin.delete_bucket().bucket(&bucket).send().await;

    info!("test_e2e_bucket_policy_requires_if_none_match passed");
    Ok(())
}

/// IAM identity policy: PutObject is allowed only when the request carries `If-Match`.
#[tokio::test]
async fn test_e2e_iam_policy_requires_if_match() -> TestResult {
    init_logging();
    let suffix = Uuid::new_v4();
    let user = format!("e2econdmatch-{suffix}");
    let user_secret = "longSecretKeyForCondMatch2!";
    let policy_name = format!("e2e-cond-match-pol-{suffix}");
    let bucket = format!("e2e-cond-match-bkt-{suffix}");
    let key = "guarded.txt";

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;
    let admin = env.create_s3_client();
    admin_create_user(&env, &user, user_secret).await?;

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["s3:PutObject"],
            "Resource": [format!("arn:aws:s3:::{bucket}/*")],
            "Condition": { "Null": { "s3:if-match": "false" } }
        }]
    })
    .to_string();
    admin_add_and_attach_policy(&env, &policy_name, &policy, &user).await?;

    admin.create_bucket().bucket(&bucket).send().await?;
    let original = admin
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(b"v1"))
        .send()
        .await?;
    let etag = original.e_tag().ok_or("missing ETag on initial PutObject")?.to_string();

    let client = env.create_s3_client_with_credentials(&user, user_secret);
    let denied = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(b"blind-overwrite"))
        .send()
        .await
        .expect_err("PutObject without If-Match must be denied");
    assert_access_denied(&denied, "unconditional PutObject");

    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .if_match(&etag)
        .body(ByteStream::from_static(b"v2"))
        .send()
        .await?;

    let _ = admin.delete_object().bucket(&bucket).key(key).send().await;
    let _ = admin.delete_bucket().bucket(&bucket).send().await;

    info!("test_e2e_iam_policy_requires_if_match passed");
    Ok(())
}

/// STS session policy: AssumeRole must accept `s3:if-none-match`, and the session may
/// only create objects with `If-None-Match: *`.
#[tokio::test]
async fn test_e2e_sts_session_policy_requires_if_none_match() -> TestResult {
    init_logging();
    let suffix = Uuid::new_v4();
    let parent = format!("e2e-sts-cond-par-{suffix}");
    let parent_secret = "longSecretKeyForCondParent3!";
    let policy_name = format!("e2e-sts-cond-rw-{suffix}");
    let bucket = format!("e2e-sts-cond-bkt-{suffix}");

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;
    let admin = env.create_s3_client();
    admin_create_user(&env, &parent, parent_secret).await?;

    let parent_policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            { "Effect": "Allow", "Action": ["s3:*"], "Resource": ["arn:aws:s3:::*"] },
            { "Effect": "Allow", "Action": ["sts:AssumeRole"], "Resource": ["arn:aws:s3:::*"] }
        ]
    })
    .to_string();
    admin_add_and_attach_policy(&env, &policy_name, &parent_policy, &parent).await?;
    admin.create_bucket().bucket(&bucket).send().await?;

    let session_policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["s3:PutObject"],
            "Resource": [format!("arn:aws:s3:::{bucket}/*")],
            "Condition": { "StringEquals": { "s3:if-none-match": "*" } }
        }]
    })
    .to_string();
    let body = format!(
        "Action=AssumeRole&Version=2011-06-15&DurationSeconds=3600&Policy={}",
        urlencoding::encode(&session_policy)
    );
    let url = format!("{}/", env.url.trim_end_matches('/'));
    let xml = awscurl_post_sts_form_urlencoded(&url, &body, &parent, parent_secret).await?;
    let field = |tag: &str| -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let open = format!("<{tag}>");
        let start = xml
            .find(&open)
            .ok_or_else(|| format!("missing {tag} in AssumeRole response"))?
            + open.len();
        let end = xml[start..]
            .find(&format!("</{tag}>"))
            .ok_or("unterminated AssumeRole field")?
            + start;
        Ok(xml[start..end].to_string())
    };
    let session = Client::from_conf(build_test_s3_config(
        &env.url,
        &field("AccessKeyId")?,
        &field("SecretAccessKey")?,
        Some(&field("SessionToken")?),
        "e2e-sts-session",
    ));

    let denied = session
        .put_object()
        .bucket(&bucket)
        .key("plain.txt")
        .body(ByteStream::from_static(b"plain"))
        .send()
        .await
        .expect_err("session PutObject without If-None-Match must be denied");
    assert_access_denied(&denied, "unconditional session PutObject");

    session
        .put_object()
        .bucket(&bucket)
        .key("created.txt")
        .if_none_match("*")
        .body(ByteStream::from_static(b"created"))
        .send()
        .await?;

    let _ = admin.delete_object().bucket(&bucket).key("created.txt").send().await;
    let _ = admin.delete_bucket().bucket(&bucket).send().await;

    info!("test_e2e_sts_session_policy_requires_if_none_match passed");
    Ok(())
}
