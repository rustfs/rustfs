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

//! Differential cases of the `bucket_replication_lifecycle` batch (see `super`).
//!
//! Replication has no case of its own: storing a replication configuration needs a remote target
//! registered through the admin API, which this script does not drive.

use super::super::{Fields, RETAIN_UNTIL_SECS, Recorder, none};
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::DateTime;
use aws_sdk_s3::types::{
    AbortIncompleteMultipartUpload, BucketLifecycleConfiguration, DefaultRetention, ExpirationStatus, LifecycleExpiration,
    LifecycleRule, LifecycleRuleAndOperator, LifecycleRuleFilter, NoncurrentVersionExpiration, ObjectLockConfiguration,
    ObjectLockEnabled, ObjectLockRetentionMode, ObjectLockRule, Tag,
};

/// A name RustFS creates and serves that the AWS bucket grammar reserves.
const RESERVED_BUCKET: &str = "sthree-dual-stack-a3";
const LOCK_BUCKET: &str = "dual-stack-a3-lock";

fn lifecycle_rules(
    output: &aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput,
) -> Fields {
    vec![("rules", format!("{:?}", output.rules()))]
}

fn object_lock(output: &aws_sdk_s3::operation::get_object_lock_configuration::GetObjectLockConfigurationOutput) -> Fields {
    vec![("configuration", format!("{:?}", output.object_lock_configuration()))]
}

pub(super) async fn cases(s: &mut Recorder, c: &Client) {
    s.step("CreateBucket", "a3-reserved-name", c.create_bucket().bucket(RESERVED_BUCKET).send(), none)
        .await;
    let get_lifecycle = || c.get_bucket_lifecycle_configuration().bucket(RESERVED_BUCKET);
    s.step("GetBucketLifecycleConfiguration", "a3-reserved-absent", get_lifecycle().send(), none)
        .await;
    let rule = LifecycleRule::builder()
        .id("members")
        .status(ExpirationStatus::Enabled)
        .filter(
            LifecycleRuleFilter::builder()
                .and(
                    LifecycleRuleAndOperator::builder()
                        .prefix("logs/")
                        .tags(Tag::builder().key("tier").value("cold").build().expect("tag"))
                        .object_size_greater_than(1)
                        .build(),
                )
                .build(),
        )
        .expiration(LifecycleExpiration::builder().expired_object_delete_marker(true).build())
        .noncurrent_version_expiration(
            NoncurrentVersionExpiration::builder()
                .noncurrent_days(7)
                .newer_noncurrent_versions(2)
                .build(),
        )
        .abort_incomplete_multipart_upload(AbortIncompleteMultipartUpload::builder().days_after_initiation(3).build())
        .build()
        .expect("lifecycle rule");
    // A stored date is answered through a different conversion on each edge of the migration.
    let dated = LifecycleRule::builder()
        .id("dated")
        .status(ExpirationStatus::Disabled)
        .filter(LifecycleRuleFilter::builder().prefix("archive/").build())
        .expiration(
            LifecycleExpiration::builder()
                .date(DateTime::from_secs(RETAIN_UNTIL_SECS))
                .build(),
        )
        .build()
        .expect("dated lifecycle rule");
    let lifecycle = BucketLifecycleConfiguration::builder()
        .rules(rule)
        .rules(dated)
        .build()
        .expect("lifecycle configuration");
    let put = c
        .put_bucket_lifecycle_configuration()
        .bucket(RESERVED_BUCKET)
        .lifecycle_configuration(lifecycle);
    s.step("PutBucketLifecycleConfiguration", "a3-every-member", put.send(), none)
        .await;
    s.step(
        "GetBucketLifecycleConfiguration",
        "a3-every-member",
        get_lifecycle().send(),
        lifecycle_rules,
    )
    .await;
    let get_lock = |bucket: &str| c.get_object_lock_configuration().bucket(bucket);
    s.step("GetObjectLockConfiguration", "a3-reserved-absent", get_lock(RESERVED_BUCKET).send(), none)
        .await;

    let create = c.create_bucket().bucket(LOCK_BUCKET).object_lock_enabled_for_bucket(true);
    s.step("CreateBucket", "a3-lock", create.send(), none).await;
    s.step(
        "GetObjectLockConfiguration",
        "a3-enabled-without-rule",
        get_lock(LOCK_BUCKET).send(),
        object_lock,
    )
    .await;
    let lock = ObjectLockConfiguration::builder()
        .object_lock_enabled(ObjectLockEnabled::Enabled)
        .rule(
            ObjectLockRule::builder()
                .default_retention(
                    DefaultRetention::builder()
                        .mode(ObjectLockRetentionMode::Compliance)
                        .years(1)
                        .build(),
                )
                .build(),
        )
        .build();
    let put = c
        .put_object_lock_configuration()
        .bucket(LOCK_BUCKET)
        .object_lock_configuration(lock);
    s.step("PutObjectLockConfiguration", "a3-years", put.send(), none).await;
    s.step("GetObjectLockConfiguration", "a3-years", get_lock(LOCK_BUCKET).send(), object_lock)
        .await;
}
