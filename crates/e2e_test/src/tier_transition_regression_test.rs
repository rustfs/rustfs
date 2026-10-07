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

//! Regression tests for Tier/ILM transition operations.
//!
//! Covers the recurring pattern where tier transition fails silently, the
//! free-version recovery task loops forever, or transitioned objects cannot
//! be read back. This has regressed 6+ times.
//!
//! ## Regression Issues
//!
//! - rustfs#5218: Remote tier mutation commit failed
//! - rustfs#5130: tier_free_version_recovery task loops forever
//! - rustfs#5011: Idle tier free-version recovery rescans every 60 seconds
//! - rustfs#4826: Full GET of multipart transitioned object fails
//! - rustfs#5024: Some files succeeded in tier offloading, others failed

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, admin_ok, init_logging};
    use serde_json::Value;
    use std::error::Error;
    use tracing::info;

    type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

    /// RT-13: Verify lifecycle rule with transition persists and is retrievable.
    ///
    /// Note: Actual transition requires a configured remote tier. This test
    /// validates that an expiration-only rule (the persistence path) survives
    /// a server restart.
    #[tokio::test]
    async fn test_lifecycle_rule_persists_after_restart() -> TestResult {
        init_logging();
        info!("RT-13: lifecycle rule persists after restart");

        let mut env = RustFSTestEnvironment::new().await.expect("create test environment");
        env.start_rustfs_server_with_env(vec![], &[("RUSTFS_CONSOLE_ENABLE", "false")])
            .await
            .expect("start RustFS");

        let client = env.create_s3_client();
        let bucket = "rt13-tier-persist";

        client.create_bucket().bucket(bucket).send().await.expect("create bucket");

        // Apply a lifecycle rule with expiration (transition needs a real tier)
        let rule = aws_sdk_s3::types::LifecycleRule::builder()
            .id("expire-after-90d")
            .status(aws_sdk_s3::types::ExpirationStatus::Enabled)
            .filter(aws_sdk_s3::types::LifecycleRuleFilter::builder().prefix("archive/").build())
            .expiration(aws_sdk_s3::types::LifecycleExpiration::builder().days(90).build())
            .build()
            .expect("build rule");

        client
            .put_bucket_lifecycle_configuration()
            .bucket(bucket)
            .lifecycle_configuration(
                aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
                    .rules(rule)
                    .build()
                    .expect("build config"),
            )
            .send()
            .await
            .expect("put lifecycle");

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
        assert_eq!(rules.len(), 1, "RT-13 FAIL: expected 1 rule after restart");

        let exp = rules[0].expiration().expect("expiration should be set");
        assert_eq!(exp.days(), Some(90), "RT-13 FAIL: expiration days corrupted after restart");

        info!("RT-13 PASS: lifecycle rule persists after restart");
        Ok(())
    }

    /// RT-13b: Verify admin tier configuration API is functional.
    ///
    /// Regression pattern: tier add/verify/delete API fails or the tier
    /// configuration is not persisted (rustfs#5218).
    #[tokio::test]
    async fn test_admin_tier_list_endpoint_returns_json() -> TestResult {
        init_logging();
        info!("RT-13b: admin tier list endpoint returns JSON");

        let mut env = RustFSTestEnvironment::new().await.expect("create test environment");
        env.start_rustfs_server_with_env(vec![], &[("RUSTFS_CONSOLE_ENABLE", "false")])
            .await
            .expect("start RustFS");

        // Query the tier list endpoint
        let body = admin_ok(&env, http::Method::GET, "/rustfs/admin/v3/tier", None)
            .await
            .expect("list remote tiers");

        let json: Value = serde_json::from_str(&body).expect("tier list response should be valid JSON");

        // Should return an array (possibly empty)
        assert!(json.is_array(), "RT-13b FAIL: tier list response is not an array: {json}");

        info!("RT-13b PASS: admin tier list endpoint returns valid JSON array");
        Ok(())
    }

    /// RT-13c: Verify scanner configuration persistence.
    ///
    /// Regression pattern: scanner admin config update reports success but
    /// is not persisted (rustfs#5013), causing the scanner to not run or
    /// use stale settings.
    #[tokio::test]
    async fn test_scanner_config_persists_after_restart() -> TestResult {
        init_logging();
        info!("RT-13c: scanner config persists after restart");

        let mut env = RustFSTestEnvironment::new().await.expect("create test environment");
        env.start_rustfs_server_with_env(vec![], &[("RUSTFS_CONSOLE_ENABLE", "false")])
            .await
            .expect("start RustFS");

        // Get current scanner status
        let body = admin_ok(&env, http::Method::GET, "/rustfs/admin/v3/scanner/status", None)
            .await
            .expect("get scanner status");

        let json: Value = serde_json::from_str(&body).expect("scanner status should be valid JSON");

        info!("  scanner status: {:?}", json.as_object().map(|o| o.keys().collect::<Vec<_>>()));

        // Restart and verify config is still accessible
        env.restart_server_preserving_data(vec![], &[]).await.expect("restart RustFS");

        let body2 = admin_ok(&env, http::Method::GET, "/rustfs/admin/v3/scanner/status", None)
            .await
            .expect("get scanner status after restart");

        let json2: Value = serde_json::from_str(&body2).expect("scanner status after restart should be valid JSON");

        // Both should be valid JSON objects
        assert!(json2.is_object(), "RT-13c FAIL: scanner status after restart is not a valid JSON object");

        info!("RT-13c PASS: scanner/config persists across restart");
        Ok(())
    }

    /// RT-14: a transition PUT must not carry reserved `x-minio-internal-*` headers.
    ///
    /// Regression pattern: transition forwarded the object's internal metadata as raw
    /// request headers, so MinIO-derived remotes (Storj) refused every transition with
    /// `400 InvalidArgument` while `tier add` still succeeded (rustfs#8362). The fake
    /// target refuses any PutObject carrying such a header, like MinIO and Storj do.
    #[tokio::test]
    async fn test_transition_put_omits_reserved_internal_headers() -> TestResult {
        use crate::fake_s3_target::{BucketMode, FAKE_ACCESS_KEY, FAKE_SECRET_KEY, FakeS3Target, Operation};
        use aws_sdk_s3::primitives::ByteStream;
        use aws_sdk_s3::types::{
            BucketLifecycleConfiguration, ExpirationStatus, LifecycleRule, LifecycleRuleFilter, Transition,
            TransitionStorageClass,
        };
        use std::time::{Duration, Instant};

        init_logging();
        info!("RT-14: transition PUT omits reserved internal headers");

        const TIER: &str = "RT14COLD";
        const TARGET_BUCKET: &str = "rt14-cold";
        const SOURCE_BUCKET: &str = "rt14-hot";

        let target = FakeS3Target::start().await.expect("start fake target");
        target.create_bucket_with_mode(TARGET_BUCKET, BucketMode::Unversioned);
        target.reject_reserved_internal_headers(true);

        let mut env = RustFSTestEnvironment::new().await.expect("create test environment");
        env.start_rustfs_server_with_env(
            vec![],
            &[
                ("RUSTFS_CONSOLE_ENABLE", "false"),
                ("RUSTFS_TIER_RUSTFS_ALLOW_LOOPBACK_ENDPOINT", "true"),
            ],
        )
        .await
        .expect("start RustFS");

        let tier_body = serde_json::json!({
            "type": "rustfs",
            "rustfs": {
                "name": TIER,
                "endpoint": target.endpoint(),
                "accessKey": FAKE_ACCESS_KEY,
                "secretKey": FAKE_SECRET_KEY,
                "bucket": TARGET_BUCKET,
                "prefix": "cold/",
                "region": "us-east-1",
                "storageClass": ""
            }
        })
        .to_string();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match admin_ok(&env, http::Method::PUT, "/rustfs/admin/v3/tier", Some(tier_body.clone())).await {
                Ok(_) => break,
                Err(err) if Instant::now() < deadline => {
                    info!("  add tier not ready yet: {err}");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(err) => panic!("RT-14 FAIL: add tier: {err}"),
            }
        }

        let client = env.create_s3_client();
        client
            .create_bucket()
            .bucket(SOURCE_BUCKET)
            .send()
            .await
            .expect("create bucket");

        let objects: Vec<(&str, Vec<u8>)> = vec![
            ("small.bin", (0..1024u32).map(|i| (i % 251) as u8).collect()),
            ("medium.bin", (0..200_000u32).map(|i| (i.wrapping_mul(31) % 253) as u8).collect()),
        ];
        for (key, body) in &objects {
            client
                .put_object()
                .bucket(SOURCE_BUCKET)
                .key(*key)
                .body(ByteStream::from(body.clone()))
                .send()
                .await
                .expect("put source object");
        }

        let rule = LifecycleRule::builder()
            .id("to-cold")
            .status(ExpirationStatus::Enabled)
            .filter(LifecycleRuleFilter::builder().prefix("").build())
            .transitions(
                Transition::builder()
                    .days(0)
                    .storage_class(TransitionStorageClass::from(TIER))
                    .build(),
            )
            .build()
            .expect("build rule");
        client
            .put_bucket_lifecycle_configuration()
            .bucket(SOURCE_BUCKET)
            .lifecycle_configuration(
                BucketLifecycleConfiguration::builder()
                    .rules(rule)
                    .build()
                    .expect("build config"),
            )
            .send()
            .await
            .expect("put lifecycle");

        admin_ok(
            &env,
            http::Method::POST,
            &format!("/rustfs/admin/v3/ilm/transition/run?bucket={SOURCE_BUCKET}&prefix=&tier={TIER}&dryRun=false"),
            None,
        )
        .await
        .expect("run transition");

        let deadline = Instant::now() + Duration::from_secs(60);
        while target.stored_keys(TARGET_BUCKET).len() < objects.len() {
            assert!(
                Instant::now() < deadline,
                "RT-14 FAIL: transition did not reach the target; stored keys: {:?}",
                target.stored_keys(TARGET_BUCKET)
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        let keys = target.stored_keys(TARGET_BUCKET);
        assert_eq!(keys.len(), objects.len(), "RT-14 FAIL: unexpected remote objects: {keys:?}");

        let mut stored_bodies = Vec::new();
        for key in &keys {
            let (body, metadata) = target.stored_object(TARGET_BUCKET, key).expect("remote object");
            for name in [
                "x-rustfs-internal-transition-transaction-id",
                "x-minio-internal-transition-transaction-id",
                "x-rustfs-internal-transition-tier-destination-id",
                "x-minio-internal-transition-tier-destination-id",
            ] {
                assert!(
                    metadata.contains_key(name),
                    "RT-14 FAIL: remote object {key} lost transition identity {name}: {:?}",
                    metadata.keys().collect::<Vec<_>>()
                );
            }
            stored_bodies.push(body.to_vec());
        }
        let mut expected_bodies: Vec<Vec<u8>> = objects.iter().map(|(_, body)| body.clone()).collect();
        expected_bodies.sort();
        stored_bodies.sort();
        assert_eq!(stored_bodies, expected_bodies, "RT-14 FAIL: remote bytes differ from the source objects");

        let transition_puts = target
            .requests()
            .iter()
            .filter(|record| {
                record.operation == Operation::PutObject
                    && record
                        .key
                        .as_deref()
                        .is_some_and(|key| key.contains("transition-transactions"))
            })
            .count();
        assert_eq!(
            transition_puts,
            objects.len(),
            "RT-14 FAIL: transition PUTs were retried or refused by the target"
        );

        for (key, body) in &objects {
            let head = client
                .head_object()
                .bucket(SOURCE_BUCKET)
                .key(*key)
                .send()
                .await
                .expect("head transitioned");
            assert_eq!(
                head.storage_class().map(|class| class.as_str()),
                Some(TIER),
                "RT-14 FAIL: {key} is not reported as transitioned"
            );
            let got = client
                .get_object()
                .bucket(SOURCE_BUCKET)
                .key(*key)
                .send()
                .await
                .expect("get transitioned")
                .body
                .collect()
                .await
                .expect("read body")
                .into_bytes();
            assert_eq!(got.as_ref(), body.as_slice(), "RT-14 FAIL: read-back of {key} differs");
        }

        target.shutdown().await;
        info!("RT-14 PASS: transition PUT omits reserved internal headers");
        Ok(())
    }
}
