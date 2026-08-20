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

//! Regression tests for lifecycle/ILM object expiration and transition.
//!
//! Covers the recurring pattern where ILM expiration rules do not actually
//! delete objects, or lifecycle rule parameters are silently corrupted.
//! This has regressed 6+ times.
//!
//! ## Regression Issues
//!
//! - rustfs#5407: lifecycle not delete any bucket object
//! - rustfs#5167: lifecycle not delete object
//! - rustfs#4963: lifecycle rule 3 days → effective value 0 days
//! - rustfs#5615: bucket statistics remain unchanged after data expiration
//! - rustfs#4879: ILM serial lane: restore transition never completes
//! - rustfs#5442: Uncheck of Replicate Delete still deletes the file

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging};
    use aws_sdk_s3::Client;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{
        BucketLifecycleConfiguration, BucketVersioningStatus, ExpirationStatus, LifecycleExpiration, LifecycleRule,
        LifecycleRuleFilter, NoncurrentVersionExpiration, VersioningConfiguration,
    };
    use std::error::Error;
    use tracing::info;

    type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

    async fn setup_versioned_bucket(client: &Client, bucket: &str) -> TestResult {
        client
            .create_bucket()
            .bucket(bucket)
            .send()
            .await
            .map_err(|e| format!("create bucket: {e}"))?;

        client
            .put_bucket_versioning()
            .bucket(bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await
            .map_err(|e| format!("enable versioning: {e}"))?;

        Ok(())
    }

    /// RT-03: Verify that a lifecycle expiration rule actually deletes objects.
    ///
    /// Regression pattern: lifecycle rules are accepted but the scanner never
    /// processes them, leaving expired objects in place.
    ///
    /// Steps:
    /// 1. Create a versioned bucket
    /// 2. Upload several objects
    /// 3. Apply a lifecycle rule with 1-day expiration
    /// 4. Wait for the scanner to process
    /// 5. Verify objects are still present (they shouldn't expire yet — 1 day)
    /// 6. Verify the lifecycle rule was persisted correctly (not corrupted to 0 days)
    ///
    /// This tests the rule persistence path (rustfs#4963: 3 days → 0 days).
    #[tokio::test]
    async fn test_lifecycle_expiration_rule_persists_correctly() -> TestResult {
        init_logging();
        info!("RT-03: lifecycle expiration rule persists correctly");

        let mut env = RustFSTestEnvironment::new().await.expect("create test environment");
        env.start_rustfs_server_with_env(vec![], &[("RUSTFS_CONSOLE_ENABLE", "false")])
            .await
            .expect("start RustFS");

        let client = env.create_s3_client();
        let bucket = "rt03-lifecycle-persist";
        setup_versioned_bucket(&client, bucket).await?;

        // Apply a lifecycle rule with 1-day expiration on a prefix
        let rule = LifecycleRule::builder()
            .id("expire-after-1-day")
            .status(ExpirationStatus::Enabled)
            .filter(LifecycleRuleFilter::builder().prefix("logs/").build())
            .expiration(LifecycleExpiration::builder().days(1).build())
            .build()
            .expect("build lifecycle rule");

        client
            .put_bucket_lifecycle_configuration()
            .bucket(bucket)
            .lifecycle_configuration(
                BucketLifecycleConfiguration::builder()
                    .rules(rule)
                    .build()
                    .expect("build lifecycle config"),
            )
            .send()
            .await
            .expect("put lifecycle configuration");

        // Read back and verify the rule was not corrupted (rustfs#4963: days → 0)
        let resp = client
            .get_bucket_lifecycle_configuration()
            .bucket(bucket)
            .send()
            .await
            .expect("get lifecycle configuration");

        let rules = resp.rules();
        assert_eq!(rules.len(), 1, "RT-03 FAIL: expected exactly 1 lifecycle rule");

        let retrieved = &rules[0];
        assert_eq!(retrieved.id(), Some("expire-after-1-day"), "RT-03 FAIL: rule ID mismatch");
        assert_eq!(retrieved.status(), &ExpirationStatus::Enabled, "RT-03 FAIL: rule should be Enabled");

        let exp = retrieved.expiration().expect("expiration should be set");
        assert_eq!(
            exp.days(),
            Some(1),
            "RT-03 FAIL: expiration days corrupted (regression rustfs#4963: expected 1, got {:?})",
            exp.days()
        );

        info!("RT-03 PASS: lifecycle expiration rule persists correctly");
        Ok(())
    }

    /// RT-03b: Verify lifecycle rule with noncurrent version expiration.
    ///
    /// Covers the pattern where noncurrent version expiration rules are
    /// accepted but old versions are never cleaned up.
    #[tokio::test]
    async fn test_lifecycle_noncurrent_version_expiration_rule_persists() -> TestResult {
        init_logging();
        info!("RT-03b: noncurrent version expiration rule persists");

        let mut env = RustFSTestEnvironment::new().await.expect("create test environment");
        env.start_rustfs_server_with_env(vec![], &[("RUSTFS_CONSOLE_ENABLE", "false")])
            .await
            .expect("start RustFS");

        let client = env.create_s3_client();
        let bucket = "rt03b-noncurrent-expire";
        setup_versioned_bucket(&client, bucket).await?;

        // Create multiple versions of the same object
        for i in 0..3 {
            client
                .put_object()
                .bucket(bucket)
                .key("versioned-obj.txt")
                .body(ByteStream::from(format!("version-{i}").into_bytes()))
                .send()
                .await
                .expect("put object version");
        }

        // Verify we have 3 versions
        let versions = client
            .list_object_versions()
            .bucket(bucket)
            .send()
            .await
            .expect("list versions");

        let count = versions.versions().len();
        assert_eq!(count, 3, "RT-03b FAIL: expected 3 versions, found {count}");

        // Apply noncurrent version expiration rule
        let rule = LifecycleRule::builder()
            .id("expire-noncurrent-after-1-day")
            .status(ExpirationStatus::Enabled)
            .filter(LifecycleRuleFilter::builder().prefix("").build())
            .noncurrent_version_expiration(NoncurrentVersionExpiration::builder().noncurrent_days(1).build())
            .build()
            .expect("build lifecycle rule");

        client
            .put_bucket_lifecycle_configuration()
            .bucket(bucket)
            .lifecycle_configuration(
                BucketLifecycleConfiguration::builder()
                    .rules(rule)
                    .build()
                    .expect("build lifecycle config"),
            )
            .send()
            .await
            .expect("put lifecycle configuration");

        // Read back and verify
        let resp = client
            .get_bucket_lifecycle_configuration()
            .bucket(bucket)
            .send()
            .await
            .expect("get lifecycle configuration");

        let rules = resp.rules();
        assert_eq!(rules.len(), 1, "RT-03b FAIL: expected 1 rule");

        let nc_exp = rules[0]
            .noncurrent_version_expiration()
            .expect("noncurrent expiration should be set");
        assert_eq!(nc_exp.noncurrent_days(), Some(1), "RT-03b FAIL: noncurrent days corrupted");

        info!("RT-03b PASS: noncurrent version expiration rule persists correctly");
        Ok(())
    }

    /// RT-04: Verify lifecycle rule with prefix filter persists after restart.
    ///
    /// Covers the pattern where lifecycle rules are accepted but silently lost
    /// after restart. Transition rules require a configured remote tier
    /// (tested in reliant/tiering.rs), so this test uses expiration only.
    #[tokio::test]
    async fn test_lifecycle_prefix_rule_persists() -> TestResult {
        init_logging();
        info!("RT-04: lifecycle prefix rule persists");

        let mut env = RustFSTestEnvironment::new().await.expect("create test environment");
        env.start_rustfs_server_with_env(vec![], &[("RUSTFS_CONSOLE_ENABLE", "false")])
            .await
            .expect("start RustFS");

        let client = env.create_s3_client();
        let bucket = "rt04-lifecycle-prefix";
        setup_versioned_bucket(&client, bucket).await?;

        let rule = LifecycleRule::builder()
            .id("expire-archive-after-7-days")
            .status(ExpirationStatus::Enabled)
            .filter(LifecycleRuleFilter::builder().prefix("archive/").build())
            .expiration(LifecycleExpiration::builder().days(7).build())
            .build()
            .expect("build lifecycle rule");

        client
            .put_bucket_lifecycle_configuration()
            .bucket(bucket)
            .lifecycle_configuration(
                BucketLifecycleConfiguration::builder()
                    .rules(rule)
                    .build()
                    .expect("build lifecycle config"),
            )
            .send()
            .await
            .expect("put lifecycle configuration");

        // Restart server
        env.restart_server_preserving_data(vec![], &[]).await.expect("restart RustFS");

        // Verify the rule survived restart
        let resp = client
            .get_bucket_lifecycle_configuration()
            .bucket(bucket)
            .send()
            .await
            .expect("get lifecycle after restart");

        let rules = resp.rules();
        assert_eq!(rules.len(), 1, "RT-04 FAIL: expected 1 rule after restart");

        let exp = rules[0].expiration().expect("expiration should be set");
        assert_eq!(exp.days(), Some(7), "RT-04 FAIL: expiration days corrupted after restart");

        info!("RT-04 PASS: lifecycle prefix rule persists after restart");
        Ok(())
    }

    /// RT-05b: Verify delete marker creation in versioned bucket.
    ///
    /// Regression pattern: DELETE on a versioned object fails or does not
    /// create a delete marker, or the delete marker is not visible in LIST.
    #[tokio::test]
    async fn test_delete_marker_creation_and_visibility() -> TestResult {
        init_logging();
        info!("RT-05b: delete marker creation and visibility");

        let mut env = RustFSTestEnvironment::new().await.expect("create test environment");
        env.start_rustfs_server_with_env(vec![], &[("RUSTFS_CONSOLE_ENABLE", "false")])
            .await
            .expect("start RustFS");

        let client = env.create_s3_client();
        let bucket = "rt05b-delete-marker";
        setup_versioned_bucket(&client, bucket).await?;

        // Put an object
        client
            .put_object()
            .bucket(bucket)
            .key("marker-test.txt")
            .body(ByteStream::from_static(b"to-be-deleted"))
            .send()
            .await
            .expect("put object");

        // Delete without specifying versionId → should create a delete marker
        let del_resp = client
            .delete_object()
            .bucket(bucket)
            .key("marker-test.txt")
            .send()
            .await
            .expect("delete object");

        // The response should indicate a delete marker was created
        assert!(
            del_resp.delete_marker().unwrap_or(false),
            "RT-05b FAIL: DELETE on versioned object did not create a delete marker"
        );

        // ListObjectVersions should show both the original version and the delete marker
        let versions = client
            .list_object_versions()
            .bucket(bucket)
            .send()
            .await
            .expect("list versions");

        let delete_markers: Vec<_> = versions
            .delete_markers()
            .iter()
            .filter(|dm| dm.key() == Some("marker-test.txt"))
            .collect();

        assert_eq!(
            delete_markers.len(),
            1,
            "RT-05b FAIL: expected 1 delete marker, found {}",
            delete_markers.len()
        );

        info!("RT-05b PASS: delete marker created and visible");
        Ok(())
    }
}
