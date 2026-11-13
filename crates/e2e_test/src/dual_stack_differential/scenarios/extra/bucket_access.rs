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

//! Differential cases of the `bucket_access` batch (see `super`).

use super::super::{Recorder, none};
use aws_sdk_s3::Client;
use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};

const BUCKET: &str = "a2-bucket-access";

pub(super) async fn cases(s: &mut Recorder, c: &Client) {
    s.step("CreateBucket", "a2-setup", c.create_bucket().bucket(BUCKET).send(), none).await;
    versioning(s, c).await;
}

async fn versioning(s: &mut Recorder, c: &Client) {
    let status = |o: &aws_sdk_s3::operation::get_bucket_versioning::GetBucketVersioningOutput| {
        vec![("status", format!("{:?}", o.status())), ("mfa_delete", format!("{:?}", o.mfa_delete()))]
    };
    s.step("GetBucketVersioning", "unconfigured", c.get_bucket_versioning().bucket(BUCKET).send(), status)
        .await;
    let suspended = VersioningConfiguration::builder().status(BucketVersioningStatus::Suspended).build();
    let put = c.put_bucket_versioning().bucket(BUCKET).versioning_configuration(suspended);
    s.step("PutBucketVersioning", "a2-suspended", put.send(), none).await;
    s.step("GetBucketVersioning", "suspended", c.get_bucket_versioning().bucket(BUCKET).send(), status)
        .await;
}
