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

use super::{
    LEGACY, GATEWAY, MatrixRow, Outcome, RUSTFS_ONLY_NAMES, create_bucket_in_the_store, outcome, refusal, s3_client, with_stack,
};
use aws_sdk_s3::Client;
use futures::future::BoxFuture;
use rustfs::embedded::RustFSServer;
use rustfs_s3_ops::S3Operation;

pub(crate) const MATRIX: &[MatrixRow] = &[MatrixRow {
    operation: "GetBucketVersioning",
    metric: S3Operation::GetBucketVersioning,
    has_error_body: true,
    call: get_bucket_versioning,
}];

fn get_bucket_versioning<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.get_bucket_versioning().bucket(bucket).send().await) })
}

/// GetBucketVersioning answers a bucket the store holds under any name the store can hold, and
/// refuses a bucket it does not hold.
async fn get_bucket_versioning_answers_every_stored_bucket(server: RustFSServer) {
    let client = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
    for name in RUSTFS_ONLY_NAMES {
        create_bucket_in_the_store(name).await;
        let unconfigured = client.get_bucket_versioning().bucket(name).send().await.expect(name);
        assert_eq!(unconfigured.status(), None, "{name}");
    }

    let missing = client.get_bucket_versioning().bucket("a2-no-such-bucket").send().await;
    assert_eq!(refusal(&missing.expect_err("a missing bucket")), (Some("NoSuchBucket".to_owned()), Some(404)));
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

/// The legacy stack stores a configuration through PutBucketVersioning, which the gateway stack
/// does not serve yet, so the stored status is read back there only.
#[test]
fn the_legacy_stack_answers_the_stored_versioning_status() {
    use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};
    with_stack(LEGACY, |server| async move {
        let client = s3_client(&server.endpoint(), server.access_key(), server.secret_key());
        create_bucket_in_the_store("a2-versioned").await;
        let enabled = VersioningConfiguration::builder().status(BucketVersioningStatus::Enabled).build();
        client
            .put_bucket_versioning()
            .bucket("a2-versioned")
            .versioning_configuration(enabled)
            .send()
            .await
            .expect("PutBucketVersioning on the legacy stack");

        let answer = client.get_bucket_versioning().bucket("a2-versioned").send().await.expect("get");
        assert_eq!(answer.status(), Some(&BucketVersioningStatus::Enabled));
        server.shutdown().await;
    });
}
