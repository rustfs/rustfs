// Copyright 2026 RustFS Team
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

//! Standard S3 deletion permissions and the explicit recursive-delete extension.

use crate::common::{
    AdminTransport, RustFSTestEnvironment, admin_add_canned_policy_via, admin_attach_user_policy_via, admin_create_user,
    init_logging,
};
use aws_sdk_s3::Client;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::delete_object::{DeleteObjectError, DeleteObjectOutput};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketVersioningStatus, Delete, ObjectIdentifier, VersioningConfiguration};
use futures::{StreamExt, TryStreamExt, stream};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::error::Error;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
type VersionSnapshot = BTreeSet<(String, String, bool)>;

async fn set_policy(env: &RustFSTestEnvironment, name: &str, policy: &Value) -> TestResult {
    admin_add_canned_policy_via(
        AdminTransport::Signed,
        &env.url,
        &env.access_key,
        &env.secret_key,
        name,
        &policy.to_string(),
    )
    .await
}

async fn policy_user(env: &RustFSTestEnvironment, policy_name: &str, policy: Option<Value>) -> TestResult<Client> {
    let username = Uuid::new_v4().simple().to_string();
    let secret = Uuid::new_v4().simple().to_string();
    admin_create_user(env, &username, &secret).await?;
    if let Some(policy) = policy {
        set_policy(env, policy_name, &policy).await?;
    }
    admin_attach_user_policy_via(AdminTransport::Signed, &env.url, &env.access_key, &env.secret_key, policy_name, &username)
        .await?;
    Ok(env.create_s3_client_with_credentials(&username, &secret))
}

async fn versioning(client: &Client, bucket: &str, status: BucketVersioningStatus) -> TestResult {
    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(VersioningConfiguration::builder().status(status).build())
        .send()
        .await?;
    Ok(())
}

async fn put(client: &Client, bucket: &str, key: &str) -> TestResult<String> {
    let result = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(b"delete authorization fixture"))
        .send()
        .await?;
    Ok(result.version_id().unwrap_or("null").to_string())
}

async fn versions(client: &Client, bucket: &str, prefix: &str) -> TestResult<VersionSnapshot> {
    let mut result = BTreeSet::new();
    let mut markers = (None, None);
    loop {
        let page = client
            .list_object_versions()
            .bucket(bucket)
            .prefix(prefix)
            .set_key_marker(markers.0.clone())
            .set_version_id_marker(markers.1.clone())
            .send()
            .await?;
        for version in page.versions() {
            result.insert((
                version.key().ok_or("listed version missing key")?.to_string(),
                version.version_id().ok_or("listed version missing ID")?.to_string(),
                false,
            ));
        }
        for marker in page.delete_markers() {
            result.insert((
                marker.key().ok_or("listed delete marker missing key")?.to_string(),
                marker.version_id().ok_or("listed delete marker missing ID")?.to_string(),
                true,
            ));
        }
        if page.is_truncated() != Some(true) {
            return Ok(result);
        }
        let next = (
            Some(
                page.next_key_marker()
                    .ok_or("truncated versions page missing next key marker")?
                    .to_string(),
            ),
            page.next_version_id_marker().map(str::to_string),
        );
        assert_ne!(markers, next, "ListObjectVersions pagination must advance");
        markers = next;
    }
}

async fn force_delete(client: &Client, bucket: &str, prefix: &str) -> Result<DeleteObjectOutput, SdkError<DeleteObjectError>> {
    client
        .delete_object()
        .bucket(bucket)
        .key(prefix)
        .customize()
        .mutate_request(|request| {
            request.headers_mut().insert("x-rustfs-force-delete", "true");
        })
        .send()
        .await
}

async fn replica_force_delete(
    client: &Client,
    bucket: &str,
    prefix: &str,
) -> Result<DeleteObjectOutput, SdkError<DeleteObjectError>> {
    client
        .delete_object()
        .bucket(bucket)
        .key(prefix)
        .customize()
        .mutate_request(|request| {
            request.headers_mut().insert("x-rustfs-force-delete", "true");
            request.headers_mut().insert("x-amz-replication-status", "REPLICA");
        })
        .send()
        .await
}

fn assert_denied<T, E>(result: Result<T, SdkError<E>>)
where
    T: std::fmt::Debug,
    E: ProvideErrorMetadata + std::fmt::Debug,
{
    let error = result.expect_err("request must be denied by its S3 permission");
    assert_eq!(
        error.as_service_error().and_then(ProvideErrorMetadata::code),
        Some("AccessDenied"),
        "expected an S3 authorization denial, got {error:?}"
    );
}

#[tokio::test]
async fn sdk_version_deletion_requires_only_delete_object_version() -> TestResult {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;
    let root = env.create_s3_client();
    let bucket = "delete-version-permissions";
    root.create_bucket().bucket(bucket).send().await?;
    put(&root, bucket, "single-null.txt").await?;
    put(&root, bucket, "batch-null.txt").await?;
    versioning(&root, bucket, BucketVersioningStatus::Enabled).await?;
    let old = put(&root, bucket, "single.txt").await?;
    let current = put(&root, bucket, "single.txt").await?;
    let batch_version = put(&root, bucket, "batch.txt").await?;
    let ordinary_version = put(&root, bucket, "ordinary.txt").await?;
    let user = policy_user(
        &env,
        "version-deleter",
        Some(json!({"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"s3:DeleteObjectVersion","Resource":format!("arn:aws:s3:::{bucket}/*")},
            {"Effect":"Deny","Action":"s3:DeleteObject","Resource":format!("arn:aws:s3:::{bucket}/*")}
        ]})),
    )
    .await?;

    user.delete_object()
        .bucket(bucket)
        .key("single.txt")
        .version_id(&old)
        .send()
        .await?;
    user.delete_object()
        .bucket(bucket)
        .key("single-null.txt")
        .version_id("null")
        .send()
        .await?;
    assert_denied(user.delete_object().bucket(bucket).key("ordinary.txt").send().await);

    let batch = user
        .delete_objects()
        .bucket(bucket)
        .delete(
            Delete::builder()
                .objects(
                    ObjectIdentifier::builder()
                        .key("batch.txt")
                        .version_id(&batch_version)
                        .build()?,
                )
                .objects(ObjectIdentifier::builder().key("batch-null.txt").version_id("null").build()?)
                .objects(ObjectIdentifier::builder().key("ordinary.txt").build()?)
                .build()?,
        )
        .send()
        .await?;
    assert_eq!(batch.deleted().len(), 2, "both explicit version items must succeed");
    assert_eq!(batch.errors().len(), 1, "only the unversioned item must be denied");
    assert_eq!(batch.errors()[0].key(), Some("ordinary.txt"));
    assert_eq!(batch.errors()[0].code(), Some("AccessDenied"));
    assert_eq!(
        versions(&root, bucket, "").await?,
        BTreeSet::from([
            ("single.txt".into(), current, false),
            ("ordinary.txt".into(), ordinary_version, false)
        ]),
        "version-only deletion must preserve the current single-object version and denied object"
    );
    Ok(())
}

#[tokio::test]
async fn sdk_list_bucket_and_list_bucket_versions_permissions_are_independent() -> TestResult {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;
    let root = env.create_s3_client();
    let bucket = "list-version-permissions";
    root.create_bucket().bucket(bucket).send().await?;
    versioning(&root, bucket, BucketVersioningStatus::Enabled).await?;
    put(&root, bucket, "visible.txt").await?;
    for (action, name) in [
        ("s3:ListBucket", "object-lister"),
        ("s3:ListBucketVersions", "version-lister"),
    ] {
        let user = policy_user(
            &env,
            name,
            Some(json!({"Version":"2012-10-17","Statement":[
                {"Effect":"Allow","Action":action,"Resource":format!("arn:aws:s3:::{bucket}")}
            ]})),
        )
        .await?;
        if action == "s3:ListBucket" {
            assert_eq!(user.list_objects_v2().bucket(bucket).send().await?.contents().len(), 1);
            assert_denied(user.list_object_versions().bucket(bucket).send().await);
        } else {
            assert_eq!(user.list_object_versions().bucket(bucket).send().await?.versions().len(), 1);
            assert_denied(user.list_objects_v2().bucket(bucket).send().await);
        }
    }
    Ok(())
}

#[tokio::test]
async fn console_admin_force_delete_removes_prefix_versions_and_delete_markers() -> TestResult {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;
    let root = env.create_s3_client();
    let bucket = "force-console-admin";
    root.create_bucket().bucket(bucket).send().await?;
    put(&root, bucket, "folder/null.txt").await?;
    versioning(&root, bucket, BucketVersioningStatus::Enabled).await?;
    for key in ["folder/a.txt", "folder/deep/b.txt", "single.txt"] {
        put(&root, bucket, key).await?;
        put(&root, bucket, key).await?;
        root.delete_object().bucket(bucket).key(key).send().await?;
    }
    put(&root, bucket, "keep.txt").await?;
    put(&root, bucket, "folder-sibling/keep.txt").await?;
    let keep = versions(&root, bucket, "keep.txt").await?;
    let sibling = versions(&root, bucket, "folder-sibling/").await?;
    let user = policy_user(&env, "consoleAdmin", None).await?;

    force_delete(&user, bucket, "folder/").await?;
    assert!(
        versions(&root, bucket, "folder/").await?.is_empty(),
        "force prefix deletion must remove null versions and markers"
    );
    assert_eq!(versions(&root, bucket, "folder-sibling/").await?, sibling);
    assert_eq!(
        versions(&root, bucket, "single.txt").await?.len(),
        3,
        "the separate key must survive folder deletion"
    );
    force_delete(&user, bucket, "single.txt").await?;
    assert!(
        versions(&root, bucket, "single.txt").await?.is_empty(),
        "explicit force deletion must remove every version of the selected key"
    );
    assert_eq!(versions(&root, bucket, "keep.txt").await?, keep);
    Ok(())
}

#[tokio::test]
async fn force_delete_authorizes_only_its_path_scope_without_list_permissions() -> TestResult {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;
    let root = env.create_s3_client();
    let bucket = "force-delete-only";
    root.create_bucket().bucket(bucket).send().await?;
    versioning(&root, bucket, BucketVersioningStatus::Enabled).await?;
    put(&root, bucket, "selected.txt").await?;
    put(&root, bucket, "selected.txt/child.txt").await?;
    put(&root, bucket, "selected.txt-sibling").await?;
    let sibling = versions(&root, bucket, "selected.txt-sibling").await?;
    let user = policy_user(
        &env,
        "delete-only",
        Some(json!({"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":["s3:DeleteObject","s3:DeleteObjectVersion"],"Resource":[
                format!("arn:aws:s3:::{bucket}/selected.txt"), format!("arn:aws:s3:::{bucket}/selected.txt/*")
            ]},
            {"Effect":"Deny","Action":["s3:DeleteObject","s3:DeleteObjectVersion"],"Resource":format!("arn:aws:s3:::{bucket}/selected.txt-sibling")}
        ]})),
    )
    .await?;
    assert_denied(user.list_objects_v2().bucket(bucket).send().await);
    assert_denied(user.list_object_versions().bucket(bucket).send().await);
    force_delete(&user, bucket, "selected.txt").await?;
    assert_eq!(
        versions(&root, bucket, "selected.txt").await?,
        sibling,
        "force deletion must remove the selected path and descendants without authorizing or deleting its similarly prefixed sibling"
    );
    Ok(())
}

#[tokio::test]
async fn force_directory_delete_cannot_remove_an_unauthorized_colliding_parent() -> TestResult {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;
    let root = env.create_s3_client();
    let bucket = "force-directory-collision";
    root.create_bucket().bucket(bucket).send().await?;
    versioning(&root, bucket, BucketVersioningStatus::Enabled).await?;
    let protected_parent_version = put(&root, bucket, "collision.txt").await?;
    for key in ["collision.txt/child", "collision.txt-sibling"] {
        put(&root, bucket, key).await?;
    }
    put(&root, bucket, "collision.txt").await?;
    root.delete_object().bucket(bucket).key("collision.txt").send().await?;
    let user = policy_user(
        &env,
        "parent-denier",
        Some(json!({"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":["s3:DeleteObject","s3:DeleteObjectVersion"],"Resource":format!("arn:aws:s3:::{bucket}/*")},
            {"Effect":"Deny","Action":"s3:DeleteObjectVersion","Resource":format!("arn:aws:s3:::{bucket}/collision.txt"),
             "Condition":{"StringEquals":{"s3:VersionId":protected_parent_version}}}
        ]})),
    )
    .await?;
    let mut expected = versions(&root, bucket, "").await?;
    expected.retain(|(key, _, _)| key != "collision.txt/child");
    force_delete(&user, bucket, "collision.txt/").await?;
    assert_eq!(
        versions(&root, bucket, "").await?,
        expected,
        "folder deletion must preserve the denied parent's historical versions and delete marker, plus its sibling"
    );
    Ok(())
}

#[tokio::test]
async fn force_unversioned_directory_requires_only_delete_object() -> TestResult {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;
    let root = env.create_s3_client();
    let bucket = "force-unversioned-permissions";
    root.create_bucket().bucket(bucket).send().await?;
    for key in ["folder/", "folder/child.txt", "outside.txt"] {
        put(&root, bucket, key).await?;
    }
    let outside = versions(&root, bucket, "outside.txt").await?;
    let user = policy_user(
        &env,
        "unversioned-deleter",
        Some(json!({"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"s3:DeleteObject","Resource":format!("arn:aws:s3:::{bucket}/*")},
            {"Effect":"Deny","Action":"s3:DeleteObjectVersion","Resource":format!("arn:aws:s3:::{bucket}/*")}
        ]})),
    )
    .await?;
    force_delete(&user, bucket, "folder/").await?;
    assert_eq!(
        versions(&root, bucket, "").await?,
        outside,
        "unversioned force deletion, including a synthetic nil directory marker, must use DeleteObject permission"
    );
    Ok(())
}

#[tokio::test]
async fn force_delete_denied_child_preserves_every_object_despite_bucket_allow() -> TestResult {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;
    let root = env.create_s3_client();
    let bucket = "force-child-denial";
    root.create_bucket().bucket(bucket).send().await?;
    for key in ["folder/a-allowed.txt", "folder/z-denied.txt"] {
        put(&root, bucket, key).await?;
    }
    let user = policy_user(
        &env,
        "child-denier",
        Some(json!({"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":["s3:DeleteObject","s3:DeleteObjectVersion","s3:ReplicateDelete"],"Resource":format!("arn:aws:s3:::{bucket}/*")},
            {"Effect":"Deny","Action":["s3:DeleteObject","s3:DeleteObjectVersion","s3:ReplicateDelete"],"Resource":format!("arn:aws:s3:::{bucket}/folder/z-denied.txt")}
        ]})),
    )
    .await?;
    root.put_bucket_policy()
        .bucket(bucket)
        .policy(json!({"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Principal":"*","Action":["s3:DeleteObject","s3:DeleteObjectVersion","s3:ReplicateDelete"],"Resource":format!("arn:aws:s3:::{bucket}/*")}
        ]}).to_string())
        .send().await?;
    let before = versions(&root, bucket, "folder/").await?;
    assert_denied(force_delete(&user, bucket, "folder/").await);
    assert_eq!(
        versions(&root, bucket, "folder/").await?,
        before,
        "a denied descendant must prevent every mutation in the force scope"
    );
    assert_denied(replica_force_delete(&user, bucket, "folder/").await);
    assert_eq!(
        versions(&root, bucket, "folder/").await?,
        before,
        "the REPLICA header must not bypass a descendant's ReplicateDelete denial"
    );

    root.delete_bucket_policy().bucket(bucket).send().await?;
    let replica_user = policy_user(
        &env,
        "replica-deleter",
        Some(json!({"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"s3:DeleteObject","Resource":format!("arn:aws:s3:::{bucket}/*")},
            {"Effect":"Allow","Action":"s3:ReplicateDelete","Resource":format!("arn:aws:s3:::{bucket}/*")}
        ]})),
    )
    .await?;
    replica_force_delete(&replica_user, bucket, "folder/").await?;
    assert!(
        versions(&root, bucket, "folder/").await?.is_empty(),
        "an authorized replica force request must check ReplicateDelete for its descendants"
    );
    Ok(())
}

#[tokio::test]
async fn force_delete_denied_historical_version_preserves_versions_and_markers() -> TestResult {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;
    let root = env.create_s3_client();
    let bucket = "force-version-denial";
    root.create_bucket().bucket(bucket).send().await?;
    put(&root, bucket, "folder/null.txt").await?;
    versioning(&root, bucket, BucketVersioningStatus::Enabled).await?;
    let protected_version = put(&root, bucket, "folder/versioned.txt").await?;
    put(&root, bucket, "folder/versioned.txt").await?;
    let marker = root.delete_object().bucket(bucket).key("folder/versioned.txt").send().await?;
    let marker_version = marker
        .version_id()
        .ok_or("versioned delete must return a marker version ID")?;
    put(&root, bucket, "folder/a-allowed.txt").await?;
    let policy = |version: &str| {
        json!({"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":["s3:DeleteObject","s3:DeleteObjectVersion"],"Resource":format!("arn:aws:s3:::{bucket}/*")},
            {"Effect":"Deny","Action":"s3:DeleteObjectVersion","Resource":format!("arn:aws:s3:::{bucket}/folder/*"),
             "Condition":{"StringEquals":{"s3:VersionId":version}}}
        ]})
    };
    let user = policy_user(&env, "version-denier", Some(policy(&protected_version))).await?;
    let before = versions(&root, bucket, "folder/").await?;
    for version in [protected_version.as_str(), "null", marker_version] {
        set_policy(&env, "version-denier", &policy(version)).await?;
        assert_denied(force_delete(&user, bucket, "folder/").await);
        assert_eq!(
            versions(&root, bucket, "folder/").await?,
            before,
            "denial of a historical, null, or delete-marker version must prevent recursive deletion"
        );
    }
    Ok(())
}

#[tokio::test]
async fn sdk_ordinary_deletion_preserves_versions_and_directory_children() -> TestResult {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;
    let root = env.create_s3_client();
    let user = policy_user(
        &env,
        "ordinary-deleter",
        Some(json!({"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"s3:DeleteObject","Resource":"arn:aws:s3:::*/*"},
            {"Effect":"Deny","Action":"s3:DeleteObjectVersion","Resource":"arn:aws:s3:::*/*"}
        ]})),
    )
    .await?;
    for state in ["unversioned", "enabled", "suspended"] {
        let bucket = format!("ordinary-directory-{state}");
        root.create_bucket().bucket(&bucket).send().await?;
        if state != "unversioned" {
            versioning(&root, &bucket, BucketVersioningStatus::Enabled).await?;
        }
        let historical = put(&root, &bucket, "object.txt").await?;
        if state == "suspended" {
            versioning(&root, &bucket, BucketVersioningStatus::Suspended).await?;
            put(&root, &bucket, "object.txt").await?;
        }
        put(&root, &bucket, "folder/").await?;
        put(&root, &bucket, "folder/child.txt").await?;
        let child_before = versions(&root, &bucket, "folder/child.txt").await?;
        user.delete_object().bucket(&bucket).key("folder/").send().await?;
        assert_eq!(
            versions(&root, &bucket, "folder/").await?,
            child_before,
            "ordinary {state} directory-key deletion must remove only its synthetic marker and preserve children"
        );
        let deleted = user.delete_object().bucket(&bucket).key("object.txt").send().await?;
        let object_versions = versions(&root, &bucket, "object.txt").await?;
        if state == "unversioned" {
            assert!(object_versions.is_empty());
        } else {
            assert_eq!(deleted.delete_marker(), Some(true));
            assert_eq!(
                object_versions.len(),
                2,
                "ordinary {state} deletion must retain its historical data version"
            );
            assert!(object_versions.contains(&("object.txt".into(), historical, false)));
            if state == "suspended" {
                assert!(object_versions.contains(&("object.txt".into(), "null".into(), true)));
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn sdk_delete_objects_force_header_keeps_explicit_item_scope() -> TestResult {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;
    let root = env.create_s3_client();
    let bucket = "batch-force-explicit-scope";
    root.create_bucket().bucket(bucket).send().await?;
    put(&root, bucket, "folder/").await?;
    put(&root, bucket, "folder/child.txt").await?;
    let child = versions(&root, bucket, "folder/child.txt").await?;
    let user = policy_user(&env, "consoleAdmin", None).await?;
    let result = user
        .delete_objects()
        .bucket(bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key("folder/").build()?)
                .build()?,
        )
        .customize()
        .mutate_request(|request| {
            request.headers_mut().insert("x-rustfs-force-delete", "true");
        })
        .send()
        .await?;
    assert!(result.errors().is_empty());
    assert_eq!(result.deleted().len(), 1);
    assert_eq!(
        versions(&root, bucket, "folder/").await?,
        child,
        "batch deletion must remove only the explicit directory marker even with the force header"
    );
    Ok(())
}

#[tokio::test]
async fn force_delete_checks_every_version_page_before_mutation() -> TestResult {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;
    let root = env.create_s3_client();
    let bucket = "force-delete-pagination";
    root.create_bucket().bucket(bucket).send().await?;
    versioning(&root, bucket, BucketVersioningStatus::Enabled).await?;
    stream::iter(0..1000)
        .map(|index| {
            let root = &root;
            async move { put(root, bucket, &format!("folder/{index:04}.txt")).await.map(|_| ()) }
        })
        .buffer_unordered(16)
        .try_collect::<Vec<_>>()
        .await?;
    put(&root, bucket, "folder/z-denied.txt").await?;
    let allow = json!({"Effect":"Allow","Action":["s3:DeleteObject","s3:DeleteObjectVersion"],"Resource":format!("arn:aws:s3:::{bucket}/*")});
    let user = policy_user(
        &env,
        "paged-deleter",
        Some(json!({"Version":"2012-10-17","Statement":[allow.clone(),
            {"Effect":"Deny","Action":"s3:DeleteObjectVersion","Resource":format!("arn:aws:s3:::{bucket}/folder/z-denied.txt")}
        ]})),
    )
    .await?;
    let before = versions(&root, bucket, "folder/").await?;
    assert_eq!(before.len(), 1001, "the denied key must be beyond one default versions page");
    assert_denied(force_delete(&user, bucket, "folder/").await);
    assert_eq!(
        versions(&root, bucket, "folder/").await?,
        before,
        "a denial on the second page must preserve the first page too"
    );

    set_policy(&env, "paged-deleter", &json!({"Version":"2012-10-17","Statement":[allow]})).await?;
    force_delete(&user, bucket, "folder/").await?;
    assert!(
        versions(&root, bucket, "folder/").await?.is_empty(),
        "authorized recursive deletion must cover all pages"
    );
    Ok(())
}
