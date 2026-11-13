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
