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

//! Differential cases of the `object_misc` batch (see `super`).

use super::super::{Recorder, none};
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};

const BUCKET: &str = "dual-stack-object-misc";
const KEY: &str = "doomed.txt";

/// DeleteObject on a versioned bucket: the delete marker and its headers, the delete of that
/// marker by version, and the refusals the delete raises before storage.
pub(super) async fn cases(s: &mut Recorder, c: &Client) {
    s.step("CreateBucket", "object-misc-setup", c.create_bucket().bucket(BUCKET).send(), none)
        .await;
    let enabled = VersioningConfiguration::builder()
        .status(BucketVersioningStatus::Enabled)
        .build();
    let versioning = c.put_bucket_versioning().bucket(BUCKET).versioning_configuration(enabled);
    s.step("PutBucketVersioning", "object-misc-setup", versioning.send(), none)
        .await;
    let put = c.put_object().bucket(BUCKET).key(KEY).body(ByteStream::from_static(b"e2"));
    s.step("PutObject", "object-misc-setup", put.send(), none).await;

    let marker = s
        .step(
            "DeleteObject",
            "creates-delete-marker",
            c.delete_object().bucket(BUCKET).key(KEY).send(),
            none,
        )
        .await
        .and_then(|output| output.version_id().map(str::to_owned));
    if let Some(marker) = marker {
        let delete = c.delete_object().bucket(BUCKET).key(KEY).version_id(marker);
        s.step("DeleteObject", "removes-delete-marker", delete.send(), none).await;
    }
    let delete = c.delete_object().bucket(BUCKET).key(KEY).version_id("not-a-version");
    s.step("DeleteObject", "malformed-version", delete.send(), none).await;
    let delete = c
        .delete_object()
        .bucket(BUCKET)
        .key(KEY)
        .customize()
        .mutate_request(|request| {
            request.headers_mut().insert("x-rustfs-force-delete", "maybe");
        });
    s.step("DeleteObject", "invalid-force-delete", delete.send(), none).await;
}
