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

use super::{MatrixRow, Outcome, outcome};
use aws_sdk_s3::Client;
use futures::future::BoxFuture;
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
];

fn list_buckets<'a>(client: &'a Client, _bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.list_buckets().send().await) })
}

fn head_bucket<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.head_bucket().bucket(bucket).send().await) })
}
