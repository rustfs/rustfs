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

//! Refusal-matrix rows of migration batch A4 and of ListBuckets (rustfs/backlog#2765).

use super::{GATEWAY, LEGACY, MatrixRow, Outcome, outcome, s3_client, with_stack};
use aws_sdk_s3::Client;
use aws_sdk_s3::types::{BucketLocationConstraint, CreateBucketConfiguration};
use futures::future::BoxFuture;
use rustfs::embedded::RustFSServer;
use rustfs_s3_ops::S3Operation;

pub(crate) const MATRIX: &[MatrixRow] = &[
    MatrixRow {
        operation: "ListBuckets",
        metric: S3Operation::ListBuckets,
        has_error_body: true,
        call: list_buckets,
    },
    MatrixRow {
        operation: "HeadBucket",
        metric: S3Operation::HeadBucket,
        has_error_body: false,
        call: head_bucket,
    },
    MatrixRow {
        operation: "CreateBucket",
        metric: S3Operation::CreateBucket,
        has_error_body: true,
        call: create_bucket,
    },
    MatrixRow {
        operation: "DeleteBucket",
        metric: S3Operation::DeleteBucket,
        has_error_body: true,
        call: delete_bucket,
    },
];

fn list_buckets<'a>(client: &'a Client, _bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.list_buckets().send().await) })
}

fn head_bucket<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.head_bucket().bucket(bucket).send().await) })
}

/// Re-creating the existing matrix bucket: the default region answers it `200`.
fn create_bucket<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.create_bucket().bucket(bucket).send().await) })
}

/// Deletes a bucket of its own beside the matrix bucket, created in the store first when an
/// earlier call deleted it, so every caller meets an existing, empty bucket.
fn delete_bucket<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move {
        let doomed = format!("{bucket}-doomed");
        if !super::bucket_exists_in_the_store(&doomed).await {
            super::create_bucket_in_the_store(&doomed).await;
        }
        outcome(client.delete_bucket().bucket(doomed).send().await)
    })
}

/// One CreateBucket request by root: the bucket, its `LocationConstraint`, the refusal it gets
/// (`None` when served) and whether the bucket exists afterwards.
type CreateCase = (&'static str, Option<&'static str>, Option<(Option<&'static str>, Option<u16>)>, bool);

/// The server runs in the default region, so a foreign constraint and `EU` are refused. The four
/// reserved names are created as before; `xn--` is refused before the operation
/// runs.
const CREATE_CASES: [CreateCase; 9] = [
    ("create-fresh", None, None, true),
    ("create-fresh", None, None, true),
    (
        "create-foreign",
        Some("ap-south-1"),
        Some((Some("InvalidLocationConstraint"), Some(400))),
        false,
    ),
    ("create-eu", Some("EU"), Some((Some("InvalidLocationConstraint"), Some(400))), false),
    ("sthree-x", None, None, true),
    ("a-s3alias", None, None, true),
    ("x--ol-s3", None, None, true),
    // The SDK sends nothing for a `--x-s3` name: it treats it as a directory bucket and refuses it
    // on the client. The wire answer, `200` on the legacy stack, was measured with a signed curl.
    ("x--x-s3", None, Some((None, None)), false),
    ("xn--abc", None, Some((Some("InvalidBucketName"), Some(400))), false),
];

async fn create_bucket_answers_as_before(server: RustFSServer) {
    let root = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
    let mut observed = Vec::new();
    for (bucket, location, _, _) in CREATE_CASES {
        let configuration = location.map(|location| {
            CreateBucketConfiguration::builder()
                .location_constraint(BucketLocationConstraint::from(location))
                .build()
        });
        let created = root
            .create_bucket()
            .bucket(bucket)
            .set_create_bucket_configuration(configuration)
            .send()
            .await;
        let refused = outcome(created).err().map(|refusal| (refusal.code, refusal.status));
        let exists = super::bucket_exists_in_the_store(bucket).await;
        observed.push((bucket, location, refused, exists));
    }

    let expected: Vec<_> = CREATE_CASES
        .iter()
        .map(|&(bucket, location, refused, exists)| {
            (bucket, location, refused.map(|(code, status)| (code.map(str::to_owned), status)), exists)
        })
        .collect();
    assert_eq!(observed, expected);
    server.shutdown().await;
}

#[test]
fn the_gateway_stack_creates_buckets_as_before() {
    with_stack(GATEWAY, create_bucket_answers_as_before);
}

#[test]
fn the_legacy_stack_creates_buckets_as_before() {
    with_stack(LEGACY, create_bucket_answers_as_before);
}

/// One DeleteBucket request by root: the bucket, the force-delete header it carries, whether the
/// bucket holds an object, the refusal it gets (`None` when served) and whether the bucket still
/// exists afterwards.
type DeleteCase = (
    &'static str,
    Option<(&'static str, &'static str)>,
    bool,
    Option<(&'static str, u16)>,
    bool,
);

const NOT_EMPTY: Option<(&str, u16)> = Some(("BucketNotEmpty", 409));
const RUSTFS_INVALID: Option<(&str, u16)> = Some(("InvalidRequest", 400));
const MINIO_INVALID: Option<(&str, u16)> = Some(("InvalidArgument", 400));

/// The legacy answers, measured: `x-rustfs-force-delete` reads RustFS's wide boolean grammar,
/// `x-minio-force-delete` only `true` and `false`. The force rows use a bucket that holds an
/// object, so a refused value that deleted anyway, or a `false` read as `true`, cannot pass. A
/// name the AWS grammar reserves is deleted like any other.
const DELETE_CASES: [DeleteCase; 14] = [
    ("delete-missing", None, false, Some(("NoSuchBucket", 404)), false),
    ("delete-plain", None, false, None, false),
    ("sthree-delete", None, false, None, false),
    ("delete-full", None, true, NOT_EMPTY, true),
    ("delete-rustfs-true", Some(("x-rustfs-force-delete", "true")), true, None, false),
    ("delete-rustfs-upper", Some(("x-rustfs-force-delete", "TRUE")), true, None, false),
    ("delete-rustfs-one", Some(("x-rustfs-force-delete", "1")), true, None, false),
    (
        "delete-rustfs-maybe",
        Some(("x-rustfs-force-delete", "maybe")),
        true,
        RUSTFS_INVALID,
        true,
    ),
    ("delete-rustfs-false", Some(("x-rustfs-force-delete", "false")), true, NOT_EMPTY, true),
    ("delete-minio-true", Some(("x-minio-force-delete", "true")), true, None, false),
    ("delete-minio-upper", Some(("x-minio-force-delete", "TRUE")), true, MINIO_INVALID, true),
    ("delete-minio-one", Some(("x-minio-force-delete", "1")), true, MINIO_INVALID, true),
    ("delete-minio-maybe", Some(("x-minio-force-delete", "maybe")), true, MINIO_INVALID, true),
    ("delete-minio-false", Some(("x-minio-force-delete", "false")), true, NOT_EMPTY, true),
];

async fn delete_bucket_answers_as_before(server: RustFSServer) {
    let root = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
    let mut observed = Vec::new();
    for (bucket, header, holds_an_object, _, _) in DELETE_CASES {
        if bucket != "delete-missing" {
            super::create_bucket_in_the_store(bucket).await;
        }
        if holds_an_object {
            super::put_object_in_the_store(bucket, "kept").await;
        }
        let deleted = root
            .delete_bucket()
            .bucket(bucket)
            .customize()
            .mutate_request(move |request| {
                if let Some((name, value)) = header {
                    request.headers_mut().insert(name, value);
                }
            })
            .send()
            .await;
        let refused = outcome(deleted).err().map(|refusal| (refusal.code, refusal.status));
        let exists = super::bucket_exists_in_the_store(bucket).await;
        observed.push((bucket, refused, exists));
    }

    let expected: Vec<_> = DELETE_CASES
        .iter()
        .map(|&(bucket, _, _, refused, exists)| {
            (bucket, refused.map(|(code, status)| (Some(code.to_owned()), Some(status))), exists)
        })
        .collect();
    assert_eq!(observed, expected);
    server.shutdown().await;
}

#[test]
fn the_gateway_stack_deletes_buckets_as_before() {
    with_stack(GATEWAY, delete_bucket_answers_as_before);
}

#[test]
fn the_legacy_stack_deletes_buckets_as_before() {
    with_stack(LEGACY, delete_bucket_answers_as_before);
}

async fn force_delete_header_order(server: RustFSServer) {
    let endpoint = server.endpoint();
    let root = s3_client(&endpoint, server.access_key(), server.secret_key());
    let anonymous = super::anonymous_client(&endpoint).await;
    for (bucket, rustfs_value, minio_value, expected) in [
        ("force-order-true", "true", "false", None),
        ("force-order-false", "false", "true", Some(("BucketNotEmpty", 409))),
        ("force-order-invalid", "true", "maybe", Some(("InvalidArgument", 400))),
    ] {
        super::create_bucket_in_the_store(bucket).await;
        super::put_object_in_the_store(bucket, "kept").await;
        let result = root
            .delete_bucket()
            .bucket(bucket)
            .customize()
            .mutate_request(move |request| {
                request.headers_mut().insert("x-rustfs-force-delete", rustfs_value);
                request.headers_mut().insert("x-minio-force-delete", minio_value);
            })
            .send()
            .await;
        let actual = outcome(result).err().map(|e| (e.code, e.status));
        assert_eq!(actual, expected.map(|(code, status)| (Some(code.to_owned()), Some(status))), "{bucket}");
    }
    let result = anonymous
        .delete_bucket()
        .bucket("force-order-anonymous")
        .customize()
        .mutate_request(|request| {
            request.headers_mut().insert("x-minio-force-delete", "maybe");
        })
        .send()
        .await;
    let error = outcome(result).expect_err("invalid header must be refused");
    assert_eq!((error.code.as_deref(), error.status), (Some("InvalidArgument"), Some(400)));
    server.shutdown().await;
}

#[test]
fn the_legacy_stack_orders_force_delete_headers() {
    with_stack(LEGACY, force_delete_header_order);
}

#[test]
fn the_gateway_stack_orders_force_delete_headers() {
    with_stack(GATEWAY, force_delete_header_order);
}
