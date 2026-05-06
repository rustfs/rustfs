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

//! E2E: `s3:ExistingObjectTag` with **IAM identity policy**, **bucket policy**, and **STS AssumeRole
//! session policy** (`Policy` parameter) via `awscurl --service sts` with explicit
//! `Content-Type: application/x-www-form-urlencoded` on `POST /`.

use crate::common::{
    RustFSTestEnvironment, awscurl_available, awscurl_delete, awscurl_post_sts_form_urlencoded, awscurl_put, init_logging,
};
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{Delete, ObjectIdentifier, Tag, Tagging};
use aws_sdk_s3::{Client, Config};
use serial_test::serial;
use tracing::info;
use uuid::Uuid;

fn user_client(env: &RustFSTestEnvironment, access_key: &str, secret_key: &str) -> Client {
    let credentials = Credentials::new(access_key, secret_key, None, None, "e2e-existing-tag");
    let config = Config::builder()
        .credentials_provider(credentials)
        .region(Region::new("us-east-1"))
        .endpoint_url(&env.url)
        .force_path_style(true)
        .behavior_version_latest()
        .build();
    Client::from_conf(config)
}

fn sts_session_client(env: &RustFSTestEnvironment, access_key: &str, secret_key: &str, session_token: &str) -> Client {
    let credentials = Credentials::new(access_key, secret_key, Some(session_token.into()), None, "e2e-sts-session");
    let config = Config::builder()
        .credentials_provider(credentials)
        .region(Region::new("us-east-1"))
        .endpoint_url(&env.url)
        .force_path_style(true)
        .behavior_version_latest()
        .build();
    Client::from_conf(config)
}

fn extract_xml_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(xml[start..end].to_string())
}

fn parse_assume_role_credentials(xml: &str) -> Result<(String, String, String), Box<dyn std::error::Error + Send + Sync>> {
    let ak = extract_xml_tag(xml, "AccessKeyId").ok_or("missing AccessKeyId in AssumeRole response")?;
    let sk = extract_xml_tag(xml, "SecretAccessKey").ok_or("missing SecretAccessKey in AssumeRole response")?;
    let token = extract_xml_tag(xml, "SessionToken").ok_or("missing SessionToken in AssumeRole response")?;
    Ok((ak, sk, token))
}

async fn assume_role_with_session_policy(
    env: &RustFSTestEnvironment,
    parent_ak: &str,
    parent_sk: &str,
    session_policy_json: &str,
) -> Result<(String, String, String), Box<dyn std::error::Error + Send + Sync>> {
    let policy_enc = urlencoding::encode(session_policy_json);
    let body = format!("Action=AssumeRole&Version=2011-06-15&DurationSeconds=3600&Policy={}", policy_enc);
    let url = format!("{}/", env.url.trim_end_matches('/'));
    let xml = awscurl_post_sts_form_urlencoded(&url, &body, parent_ak, parent_sk).await?;
    parse_assume_role_credentials(&xml)
}

async fn admin_create_user(
    env: &RustFSTestEnvironment,
    username: &str,
    password: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let body = serde_json::json!({ "secretKey": password, "status": "enabled" }).to_string();
    let url = format!("{}/rustfs/admin/v3/add-user?accessKey={}", env.url, username);
    awscurl_put(&url, &body, &env.access_key, &env.secret_key).await?;
    Ok(())
}

async fn admin_add_canned_policy(
    env: &RustFSTestEnvironment,
    policy_name: &str,
    policy_json: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let url = format!("{}/rustfs/admin/v3/add-canned-policy?name={}", env.url, policy_name);
    awscurl_put(&url, policy_json, &env.access_key, &env.secret_key).await?;
    Ok(())
}

async fn admin_attach_policy_to_user(
    env: &RustFSTestEnvironment,
    policy_name: &str,
    username: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let url = format!(
        "{}/rustfs/admin/v3/set-user-or-group-policy?policyName={}&userOrGroup={}&isGroup=false",
        env.url, policy_name, username
    );
    awscurl_put(&url, "", &env.access_key, &env.secret_key).await?;
    Ok(())
}

async fn admin_remove_user(env: &RustFSTestEnvironment, username: &str) {
    let url = format!("{}/rustfs/admin/v3/remove-user?accessKey={}", env.url, username);
    let _ = awscurl_delete(&url, &env.access_key, &env.secret_key).await;
}

async fn admin_remove_policy(env: &RustFSTestEnvironment, policy_name: &str) {
    let url = format!("{}/rustfs/admin/v3/remove-canned-policy?name={}", env.url, policy_name);
    let _ = awscurl_delete(&url, &env.access_key, &env.secret_key).await;
}

async fn put_object_with_tagging_str(
    client: &Client,
    bucket: &str,
    key: &str,
    data: &[u8],
    tagging: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(data.to_vec()))
        .tagging(tagging)
        .send()
        .await?;
    Ok(())
}

async fn put_object_tag_kv(
    client: &Client,
    bucket: &str,
    key: &str,
    tag_key: &str,
    tag_value: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let tag = Tag::builder()
        .key(tag_key)
        .value(tag_value)
        .build()
        .map_err(|e| format!("Tag build: {e}"))?;
    let tagging = Tagging::builder()
        .tag_set(tag)
        .build()
        .map_err(|e| format!("Tagging build: {e}"))?;
    client
        .put_object_tagging()
        .bucket(bucket)
        .key(key)
        .tagging(tagging)
        .send()
        .await?;
    Ok(())
}

async fn cleanup_bucket_and_object(admin: &Client, bucket: &str, key: &str) {
    let _ = admin.delete_object().bucket(bucket).key(key).send().await;
    let _ = admin.delete_bucket().bucket(bucket).send().await;
}

/// IAM identity policy: GetObject allowed only when `s3:ExistingObjectTag/security` == `public`.
#[tokio::test]
#[serial]
async fn test_e2e_iam_policy_existing_object_tag_get_object() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    if !awscurl_available() {
        info!("Skipping test_e2e_iam_policy_existing_object_tag_get_object: awscurl not available");
        return Ok(());
    }

    let suffix = Uuid::new_v4();
    let user = format!("e2eiamtag-{suffix}");
    let user_secret = "longSecretKeyForTest123!";
    let policy_name = format!("e2e-iam-tag-pol-{suffix}");
    let bucket = format!("e2e-iam-tag-bkt-{suffix}");
    let key = "tagged-object.txt";

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let admin = env.create_s3_client();
    admin_create_user(&env, &user, user_secret).await?;

    let policy_doc = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["s3:GetObject"],
            "Resource": [format!("arn:aws:s3:::{}/*", bucket)],
            "Condition": { "StringEquals": { "s3:ExistingObjectTag/security": "public" } }
        }]
    })
    .to_string();

    admin_add_canned_policy(&env, &policy_name, &policy_doc).await?;
    admin_attach_policy_to_user(&env, &policy_name, &user).await?;

    admin.create_bucket().bucket(&bucket).send().await?;
    put_object_with_tagging_str(&admin, &bucket, key, b"hello-iam-tag", "security=public").await?;

    let uclient = user_client(&env, &user, user_secret);
    let out = uclient.get_object().bucket(&bucket).key(key).send().await?;
    let _ = out.body.collect().await?;

    put_object_tag_kv(&admin, &bucket, key, "security", "private").await?;
    let denied = uclient.get_object().bucket(&bucket).key(key).send().await;
    assert!(
        denied.is_err(),
        "GetObject must be denied when ExistingObjectTag no longer matches IAM policy"
    );

    cleanup_bucket_and_object(&admin, &bucket, key).await;
    admin_remove_user(&env, &user).await;
    admin_remove_policy(&env, &policy_name).await;

    info!("test_e2e_iam_policy_existing_object_tag_get_object passed");
    Ok(())
}

/// Bucket policy: same `ExistingObjectTag` condition; user has no canned IAM policy attached.
#[tokio::test]
#[serial]
async fn test_e2e_bucket_policy_existing_object_tag_get_object() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    if !awscurl_available() {
        info!("Skipping test_e2e_bucket_policy_existing_object_tag_get_object: awscurl not available");
        return Ok(());
    }

    let suffix = Uuid::new_v4();
    let user = format!("e2ebptag-{suffix}");
    let user_secret = "longSecretKeyForTest456!";
    let bucket = format!("e2e-bp-tag-bkt-{suffix}");
    let key = "obj.txt";

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let admin = env.create_s3_client();
    admin_create_user(&env, &user, user_secret).await?;

    admin.create_bucket().bucket(&bucket).send().await?;

    let deny_before = user_client(&env, &user, user_secret)
        .get_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await;
    assert!(deny_before.is_err(), "without bucket policy, user must be denied");

    let bp = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": { "AWS": [user.clone()] },
            "Action": ["s3:GetObject"],
            "Resource": [format!("arn:aws:s3:::{}/*", bucket)],
            "Condition": { "StringEquals": { "s3:ExistingObjectTag/security": "public" } }
        }]
    })
    .to_string();

    admin.put_bucket_policy().bucket(&bucket).policy(&bp).send().await?;
    put_object_with_tagging_str(&admin, &bucket, key, b"data", "security=public").await?;

    let uclient = user_client(&env, &user, user_secret);
    let ok = uclient.get_object().bucket(&bucket).key(key).send().await?;
    let _ = ok.body.collect().await?;

    put_object_tag_kv(&admin, &bucket, key, "security", "private").await?;
    let denied = uclient.get_object().bucket(&bucket).key(key).send().await;
    assert!(denied.is_err(), "GetObject must fail when tag no longer satisfies bucket policy");

    cleanup_bucket_and_object(&admin, &bucket, key).await;
    admin_remove_user(&env, &user).await;

    info!("test_e2e_bucket_policy_existing_object_tag_get_object passed");
    Ok(())
}

/// STS `AssumeRole` with inline `Policy` (session policy): GetObject only when `ExistingObjectTag/security` is `public`.
#[tokio::test]
#[serial]
async fn test_e2e_sts_assume_role_session_policy_existing_object_tag() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    if !awscurl_available() {
        info!("Skipping test_e2e_sts_assume_role_session_policy_existing_object_tag: awscurl not available");
        return Ok(());
    }

    let suffix = Uuid::new_v4();
    let parent = format!("e2e-sts-par-{suffix}");
    let parent_secret = "longSecretKeyForParentSts99!";
    let policy_readwrite = format!("e2e-sts-rw-{suffix}");
    let bucket = format!("e2e-sts-tag-bkt-{suffix}");
    let key = "sts-obj.txt";

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let admin = env.create_s3_client();
    admin_create_user(&env, &parent, parent_secret).await?;

    let rw = serde_json::to_string(&serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:*"],
                "Resource": ["arn:aws:s3:::*"]
            },
            {
                "Effect": "Allow",
                "Action": ["sts:AssumeRole"],
                "Resource": ["arn:aws:s3:::*"]
            }
        ]
    }))?;
    admin_add_canned_policy(&env, &policy_readwrite, &rw).await?;
    admin_attach_policy_to_user(&env, &policy_readwrite, &parent).await?;

    let parent_client = user_client(&env, &parent, parent_secret);
    parent_client.create_bucket().bucket(&bucket).send().await?;
    put_object_with_tagging_str(&parent_client, &bucket, key, b"sts-e2e-data", "security=public").await?;

    let session_policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["s3:GetObject"],
            "Resource": [format!("arn:aws:s3:::{}/*", bucket)],
            "Condition": { "StringEquals": { "s3:ExistingObjectTag/security": "public" } }
        }]
    })
    .to_string();

    let (ak, sk, token) = assume_role_with_session_policy(&env, &parent, parent_secret, &session_policy).await?;

    let session_client = sts_session_client(&env, &ak, &sk, &token);
    let ok = session_client.get_object().bucket(&bucket).key(key).send().await?;
    let _ = ok.body.collect().await?;

    put_object_tag_kv(&parent_client, &bucket, key, "security", "private").await?;
    let denied = session_client.get_object().bucket(&bucket).key(key).send().await;
    assert!(
        denied.is_err(),
        "session policy must deny GetObject when ExistingObjectTag no longer matches"
    );

    cleanup_bucket_and_object(&admin, &bucket, key).await;
    admin_remove_user(&env, &parent).await;
    admin_remove_policy(&env, &policy_readwrite).await;

    info!("test_e2e_sts_assume_role_session_policy_existing_object_tag passed");
    Ok(())
}

/// STS inline session policy: DeleteObjects must evaluate `s3:DeleteObject` per requested object key.
#[tokio::test]
#[serial]
async fn test_e2e_sts_session_policy_delete_objects_object_prefix_only() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    if !awscurl_available() {
        info!("Skipping test_e2e_sts_session_policy_delete_objects_object_prefix_only: awscurl not available");
        return Ok(());
    }

    let suffix = Uuid::new_v4();
    let parent = format!("e2e-sts-del-par-{suffix}");
    let parent_secret = "longSecretKeyForParentDelete99!";
    let policy_readwrite = format!("e2e-sts-del-rw-{suffix}");
    let bucket = format!("e2e-sts-del-bkt-{suffix}");
    let allowed_key = "allowed/table/data.parquet";
    let denied_key = "denied/table/data.parquet";

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let admin = env.create_s3_client();
    admin_create_user(&env, &parent, parent_secret).await?;

    let rw = serde_json::to_string(&serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:*"],
                "Resource": ["arn:aws:s3:::*"]
            },
            {
                "Effect": "Allow",
                "Action": ["sts:AssumeRole"],
                "Resource": ["arn:aws:s3:::*"]
            }
        ]
    }))?;
    admin_add_canned_policy(&env, &policy_readwrite, &rw).await?;
    admin_attach_policy_to_user(&env, &policy_readwrite, &parent).await?;

    let parent_client = user_client(&env, &parent, parent_secret);
    parent_client.create_bucket().bucket(&bucket).send().await?;
    parent_client
        .put_object()
        .bucket(&bucket)
        .key(allowed_key)
        .body(ByteStream::from_static(b"allowed-delete-data"))
        .send()
        .await?;
    parent_client
        .put_object()
        .bucket(&bucket)
        .key(denied_key)
        .body(ByteStream::from_static(b"denied-delete-data"))
        .send()
        .await?;

    let session_policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["s3:DeleteObject"],
            "Resource": [format!("arn:aws:s3:::{}/allowed/*", bucket)]
        }]
    })
    .to_string();

    let (ak, sk, token) = assume_role_with_session_policy(&env, &parent, parent_secret, &session_policy).await?;
    let session_client = sts_session_client(&env, &ak, &sk, &token);

    let delete = Delete::builder()
        .objects(ObjectIdentifier::builder().key(allowed_key).build()?)
        .objects(ObjectIdentifier::builder().key(denied_key).build()?)
        .build()?;

    let result = session_client.delete_objects().bucket(&bucket).delete(delete).send().await?;

    assert_eq!(result.deleted().len(), 1, "only the allowed-prefix object should be deleted");
    assert!(
        result.deleted().iter().any(|deleted| deleted.key() == Some(allowed_key)),
        "DeleteObjects response should report the allowed-prefix object as deleted"
    );

    assert_eq!(result.errors().len(), 1, "the out-of-prefix object should return one per-key error");
    let error = &result.errors()[0];
    assert_eq!(error.key(), Some(denied_key));
    assert_eq!(error.code(), Some("AccessDenied"));

    let allowed_head = parent_client.head_object().bucket(&bucket).key(allowed_key).send().await;
    assert!(allowed_head.is_err(), "allowed-prefix object should have been deleted");

    parent_client
        .head_object()
        .bucket(&bucket)
        .key(denied_key)
        .send()
        .await
        .expect("out-of-prefix object should remain after per-key AccessDenied");

    let _ = admin.delete_object().bucket(&bucket).key(allowed_key).send().await;
    let _ = admin.delete_object().bucket(&bucket).key(denied_key).send().await;
    let _ = admin.delete_bucket().bucket(&bucket).send().await;
    admin_remove_user(&env, &parent).await;
    admin_remove_policy(&env, &policy_readwrite).await;

    info!("test_e2e_sts_session_policy_delete_objects_object_prefix_only passed");
    Ok(())
}
