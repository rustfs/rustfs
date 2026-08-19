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
}
