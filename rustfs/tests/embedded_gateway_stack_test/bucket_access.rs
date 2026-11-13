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

//! Refusal-matrix rows of migration batch A2 (rustfs/backlog#2765).

use super::{GATEWAY, LEGACY, MatrixRow, Outcome, create_bucket_in_the_store, outcome, refusal, s3_client, with_stack};
use aws_sdk_s3::Client;
use aws_sdk_s3::types::BucketVersioningStatus;
use futures::future::BoxFuture;
use rustfs::app::context::get_global_app_context;
use rustfs::embedded::RustFSServer;
use rustfs_s3_ops::S3Operation;

pub(crate) const MATRIX: &[MatrixRow] = &[
    MatrixRow {
        operation: "GetBucketVersioning",
        metric: S3Operation::GetBucketVersioning,
        has_error_body: true,
        call: get_bucket_versioning,
    },
    MatrixRow {
        operation: "GetBucketPolicy",
        metric: S3Operation::GetBucketPolicy,
        has_error_body: true,
        call: get_bucket_policy,
    },
    MatrixRow {
        operation: "GetBucketPolicyStatus",
        metric: S3Operation::GetBucketPolicyStatus,
        has_error_body: true,
        call: get_bucket_policy_status,
    },
    MatrixRow {
        operation: "GetBucketAcl",
        metric: S3Operation::GetBucketAcl,
        has_error_body: true,
        call: get_bucket_acl,
    },
];

fn get_bucket_versioning<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.get_bucket_versioning().bucket(bucket).send().await) })
}

/// A policy that grants nobody anything the matrix exercises, so GetBucketPolicy has a document to
/// answer without changing what any other row is admitted for.
const INERT_POLICY: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":["arn:aws:iam::rustfs:user/nobody"]},"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::gateway-stack-matrix/*"]}]}"#;

fn get_bucket_policy<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move {
        store_bucket_config(bucket, "policy.json", INERT_POLICY).await;
        outcome(client.get_bucket_policy().bucket(bucket).send().await)
    })
}

fn get_bucket_policy_status<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.get_bucket_policy_status().bucket(bucket).send().await) })
}

fn get_bucket_acl<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.get_bucket_acl().bucket(bucket).send().await) })
}

/// Stores a bucket configuration document the way the metadata system holds it, so a read is
/// tested without the operation that writes it.
async fn store_bucket_config(bucket: &str, file: &str, document: &str) {
    let metadata = get_global_app_context()
        .expect("embedded server context")
        .bucket_metadata()
        .handle()
        .expect("bucket metadata system");
    metadata
        .read()
        .await
        .update(bucket, file, document.as_bytes().to_vec())
        .await
        .expect("store the bucket configuration");
}

/// The stored names a request is served for. The AWS SDK treats a `--x-s3` name as a directory
/// bucket and refuses it on the client, so `x--x-s3` is left out; `xn--abc` is refused with 400
/// by both stacks before any operation runs, and is pinned apart below.
const SDK_REACHABLE_NAMES: [&str; 3] = ["a-s3alias", "sthree-x", "x--ol-s3"];

const ENABLED_VERSIONING: &str = r#"<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>"#;

/// GetBucketVersioning answers a bucket the store holds under any name the store can hold, with
/// the status it holds, and refuses a bucket it does not hold.
async fn get_bucket_versioning_answers_every_stored_bucket(server: RustFSServer) {
    let client = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
    for name in SDK_REACHABLE_NAMES {
        create_bucket_in_the_store(name).await;
        let unconfigured = client.get_bucket_versioning().bucket(name).send().await.expect(name);
        assert_eq!(unconfigured.status(), None, "{name}");
    }
    store_bucket_config(SDK_REACHABLE_NAMES[0], "versioning.xml", ENABLED_VERSIONING).await;
    let enabled = client
        .get_bucket_versioning()
        .bucket(SDK_REACHABLE_NAMES[0])
        .send()
        .await
        .expect("enabled");
    assert_eq!(enabled.status(), Some(&BucketVersioningStatus::Enabled));

    create_bucket_in_the_store("xn--abc").await;
    let reserved = client
        .get_bucket_versioning()
        .bucket("xn--abc")
        .send()
        .await
        .expect_err("a refused name");
    assert_eq!(refusal(&reserved), (Some("InvalidBucketName".to_owned()), Some(400)), "{reserved:?}");

    let missing = client.get_bucket_versioning().bucket("a2-no-such-bucket").send().await;
    assert_eq!(
        refusal(&missing.expect_err("a missing bucket")),
        (Some("NoSuchBucket".to_owned()), Some(404))
    );
    server.shutdown().await;
}

#[test]
fn the_gateway_stack_answers_every_stored_bucket_for_versioning() {
    with_stack(GATEWAY, get_bucket_versioning_answers_every_stored_bucket);
}

#[test]
fn the_legacy_stack_answers_every_stored_bucket_for_versioning() {
    with_stack(LEGACY, get_bucket_versioning_answers_every_stored_bucket);
}

/// A bucket policy document: refused when there is none, answered verbatim when there is, and
/// the owner reads it even when the policy denies everyone; policy status follows the document.
async fn bucket_policy_reads_follow_the_stored_document(server: RustFSServer) {
    const DENY_ALL: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":{"AWS":["*"]},"Action":["s3:GetBucketPolicy"],"Resource":["arn:aws:s3:::a2-policy"]}]}"#;
    const PUBLIC_LIST: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:ListBucket"],"Resource":["arn:aws:s3:::a2-public"]}]}"#;
    let client = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
    create_bucket_in_the_store("a2-policy").await;
    create_bucket_in_the_store("a2-public").await;

    let none = client
        .get_bucket_policy()
        .bucket("a2-policy")
        .send()
        .await
        .expect_err("no policy yet");
    assert_eq!(refusal(&none), (Some("NoSuchBucketPolicy".to_owned()), Some(404)));
    let missing = client
        .get_bucket_policy()
        .bucket("a2-no-such-bucket")
        .send()
        .await
        .expect_err("no bucket");
    assert_eq!(refusal(&missing), (Some("NoSuchBucket".to_owned()), Some(404)), "{missing:?}");
    let status = client
        .get_bucket_policy_status()
        .bucket("a2-public")
        .send()
        .await
        .expect("status");
    assert_eq!(status.policy_status().and_then(|status| status.is_public()), Some(false));

    store_bucket_config("a2-policy", "policy.json", DENY_ALL).await;
    let owner = client
        .get_bucket_policy()
        .bucket("a2-policy")
        .send()
        .await
        .expect("the owner bypasses the Deny");
    assert_eq!(owner.policy(), Some(DENY_ALL));

    store_bucket_config("a2-public", "policy.json", PUBLIC_LIST).await;
    let status = client
        .get_bucket_policy_status()
        .bucket("a2-public")
        .send()
        .await
        .expect("status");
    assert_eq!(status.policy_status().and_then(|status| status.is_public()), Some(true));
    server.shutdown().await;
}

#[test]
fn the_gateway_stack_reads_the_stored_bucket_policy() {
    with_stack(GATEWAY, bucket_policy_reads_follow_the_stored_document);
}

#[test]
fn the_legacy_stack_reads_the_stored_bucket_policy() {
    with_stack(LEGACY, bucket_policy_reads_follow_the_stored_document);
}

/// GetBucketAcl answers the one owner with full control for any bucket the store holds.
async fn get_bucket_acl_answers_the_owner_with_full_control(server: RustFSServer) {
    use aws_sdk_s3::types::{Permission, Type};
    let client = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
    create_bucket_in_the_store(SDK_REACHABLE_NAMES[1]).await;

    let acl = client
        .get_bucket_acl()
        .bucket(SDK_REACHABLE_NAMES[1])
        .send()
        .await
        .expect("acl");
    assert!(acl.owner().and_then(|owner| owner.id()).is_some());
    let [grant] = acl.grants() else { panic!("one grant: {:?}", acl.grants()) };
    assert_eq!(grant.permission(), Some(&Permission::FullControl));
    assert_eq!(grant.grantee().map(|grantee| grantee.r#type()), Some(&Type::CanonicalUser));
    let grantee = grant.grantee().expect("a grantee");
    assert_eq!((grantee.id(), grantee.display_name()), (None, None), "the grantee names no identity");

    let missing = client
        .get_bucket_acl()
        .bucket("a2-no-such-bucket")
        .send()
        .await
        .expect_err("no bucket");
    assert_eq!(refusal(&missing), (Some("NoSuchBucket".to_owned()), Some(404)), "{missing:?}");
    server.shutdown().await;
}

#[test]
fn the_gateway_stack_answers_the_bucket_acl() {
    with_stack(GATEWAY, get_bucket_acl_answers_the_owner_with_full_control);
}

#[test]
fn the_legacy_stack_answers_the_bucket_acl() {
    with_stack(LEGACY, get_bucket_acl_answers_the_owner_with_full_control);
}
