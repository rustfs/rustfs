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

use crate::common::{RustFSTestEnvironment, awscurl_delete, awscurl_get, awscurl_post, awscurl_put, init_logging};
use aws_sdk_s3::Client;
use serial_test::serial;
use tracing::{debug, info};

/// Test environment setup for quota tests
pub struct QuotaTestEnv {
    pub env: RustFSTestEnvironment,
    pub client: Client,
    pub bucket_name: String,
}

impl QuotaTestEnv {
    pub async fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let bucket_name = format!("quota-test-{}", uuid::Uuid::new_v4());
        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;
        let client = env.create_s3_client();

        Ok(Self {
            env,
            client,
            bucket_name,
        })
    }

    pub async fn create_bucket(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.env.create_test_bucket(&self.bucket_name).await?;
        Ok(())
    }

    pub async fn cleanup_bucket(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let objects = self.client.list_objects_v2().bucket(&self.bucket_name).send().await?;
        for object in objects.contents() {
            self.client
                .delete_object()
                .bucket(&self.bucket_name)
                .key(object.key().unwrap_or_default())
                .send()
                .await?;
        }
        self.env.delete_test_bucket(&self.bucket_name).await?;
        Ok(())
    }

    pub async fn set_bucket_quota(&self, quota_bytes: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{}/rustfs/admin/v3/quota/{}", self.env.url, self.bucket_name);
        let quota_config = serde_json::json!({
            "quota": quota_bytes,
            "quota_type": "HARD"
        });

        let response = awscurl_put(&url, &quota_config.to_string(), &self.env.access_key, &self.env.secret_key).await?;
        if response.contains("error") {
            Err(format!("Failed to set quota: {}", response).into())
        } else {
            Ok(())
        }
    }

    pub async fn get_bucket_quota(&self) -> Result<Option<u64>, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{}/rustfs/admin/v3/quota/{}", self.env.url, self.bucket_name);
        let response = awscurl_get(&url, &self.env.access_key, &self.env.secret_key).await?;

        if response.contains("error") {
            Err(format!("Failed to get quota: {}", response).into())
        } else {
            let quota_info: serde_json::Value = serde_json::from_str(&response)?;
            Ok(quota_info.get("quota").and_then(|v| v.as_u64()))
        }
    }

    pub async fn clear_bucket_quota(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{}/rustfs/admin/v3/quota/{}", self.env.url, self.bucket_name);
        let response = awscurl_delete(&url, &self.env.access_key, &self.env.secret_key).await?;

        if response.contains("error") {
            Err(format!("Failed to clear quota: {}", response).into())
        } else {
            Ok(())
        }
    }

    pub async fn get_bucket_quota_stats(&self) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{}/rustfs/admin/v3/quota-stats/{}", self.env.url, self.bucket_name);
        let response = awscurl_get(&url, &self.env.access_key, &self.env.secret_key).await?;

        if response.contains("error") {
            Err(format!("Failed to get quota stats: {}", response).into())
        } else {
            Ok(serde_json::from_str(&response)?)
        }
    }

    pub async fn check_bucket_quota(
        &self,
        operation_type: &str,
        operation_size: u64,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{}/rustfs/admin/v3/quota-check/{}", self.env.url, self.bucket_name);
        let check_request = serde_json::json!({
            "operation_type": operation_type,
            "operation_size": operation_size
        });

        let response = awscurl_post(&url, &check_request.to_string(), &self.env.access_key, &self.env.secret_key).await?;

        if response.contains("error") {
            Err(format!("Failed to check quota: {}", response).into())
        } else {
            Ok(serde_json::from_str(&response)?)
        }
    }

    pub async fn upload_object(&self, key: &str, size_bytes: usize) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let data = vec![0u8; size_bytes];
        self.client
            .put_object()
            .bucket(&self.bucket_name)
            .key(key)
            .body(aws_sdk_s3::primitives::ByteStream::from(data))
            .send()
            .await?;
        Ok(())
    }

    pub async fn object_exists(&self, key: &str) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        match self.client.head_object().bucket(&self.bucket_name).key(key).send().await {
            Ok(_) => Ok(true),
            Err(e) => {
                // Check for any 404-related errors and return false instead of propagating
                let error_str = e.to_string();
                if error_str.contains("404") || error_str.contains("Not Found") || error_str.contains("NotFound") {
                    Ok(false)
                } else {
                    // Also check the error code directly
                    if let Some(service_err) = e.as_service_error()
                        && service_err.is_not_found()
                    {
                        return Ok(false);
                    }
                    Err(e.into())
                }
            }
        }
    }

    pub async fn get_bucket_usage(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let stats = self.get_bucket_quota_stats().await?;
        Ok(stats.get("current_usage").and_then(|v| v.as_u64()).unwrap_or(0))
    }

    pub async fn set_bucket_quota_for(
        &self,
        bucket: &str,
        quota_bytes: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{}/rustfs/admin/v3/quota/{}", self.env.url, bucket);
        let quota_config = serde_json::json!({
            "quota": quota_bytes,
            "quota_type": "HARD"
        });

        let response = awscurl_put(&url, &quota_config.to_string(), &self.env.access_key, &self.env.secret_key).await?;
        if response.contains("error") {
            Err(format!("Failed to set quota: {}", response).into())
        } else {
            Ok(())
        }
    }

    /// Get bucket quota statistics for specific bucket
    pub async fn get_bucket_quota_stats_for(
        &self,
        bucket: &str,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        debug!("Getting quota stats for bucket: {}", bucket);

        let url = format!("{}/rustfs/admin/v3/quota-stats/{}", self.env.url, bucket);
        let response = awscurl_get(&url, &self.env.access_key, &self.env.secret_key).await?;

        if response.contains("error") {
            Err(format!("Failed to get quota stats: {}", response).into())
        } else {
            let stats: serde_json::Value = serde_json::from_str(&response)?;
            Ok(stats)
        }
    }

    /// Upload an object to specific bucket
    pub async fn upload_object_to_bucket(
        &self,
        bucket: &str,
        key: &str,
        size_bytes: usize,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        debug!("Uploading object {} with size {} bytes to bucket {}", key, size_bytes, bucket);

        let data = vec![0u8; size_bytes];

        self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(aws_sdk_s3::primitives::ByteStream::from(data))
            .send()
            .await?;

        info!("Successfully uploaded object: {} ({} bytes) to bucket: {}", key, size_bytes, bucket);
        Ok(())
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[tokio::test]
    #[serial]
    async fn test_quota_basic_operations() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        init_logging();
        let env = QuotaTestEnv::new().await?;

        // Create test bucket
        env.create_bucket().await?;

        // Set quota of 1MB
        env.set_bucket_quota(1024 * 1024).await?;

        // Verify quota is set
        let quota = env.get_bucket_quota().await?;
        assert_eq!(quota, Some(1024 * 1024));

        // Upload a 512KB object (should succeed)
        env.upload_object("test1.txt", 512 * 1024).await?;
        assert!(env.object_exists("test1.txt").await?);

        // Upload another 512KB object (should succeed, total 1MB)
        env.upload_object("test2.txt", 512 * 1024).await?;
        assert!(env.object_exists("test2.txt").await?);

        // Try to upload 1KB more (should fail due to quota)
        let upload_result = env.upload_object("test3.txt", 1024).await;
        assert!(upload_result.is_err());
        assert!(!env.object_exists("test3.txt").await?);

        // Clean up
        env.clear_bucket_quota().await?;
        env.cleanup_bucket().await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_quota_update_and_clear() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        init_logging();
        let env = QuotaTestEnv::new().await?;

        env.create_bucket().await?;

        // Set initial quota
        env.set_bucket_quota(512 * 1024).await?;
        assert_eq!(env.get_bucket_quota().await?, Some(512 * 1024));

        // Update quota to larger size
        env.set_bucket_quota(2 * 1024 * 1024).await?;
        assert_eq!(env.get_bucket_quota().await?, Some(2 * 1024 * 1024));

        // Upload 1MB object (should succeed with new quota)
        env.upload_object("large_file.txt", 1024 * 1024).await?;
        assert!(env.object_exists("large_file.txt").await?);

        // Clear quota
        env.clear_bucket_quota().await?;
        assert_eq!(env.get_bucket_quota().await?, None);

        // Upload another large object (should succeed with no quota)
        env.upload_object("unlimited_file.txt", 5 * 1024 * 1024).await?;
        assert!(env.object_exists("unlimited_file.txt").await?);

        env.cleanup_bucket().await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_quota_delete_operations() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        init_logging();
        let env = QuotaTestEnv::new().await?;

        env.create_bucket().await?;

        // Set quota of 1MB
        env.set_bucket_quota(1024 * 1024).await?;

        // Fill up to quota limit
        env.upload_object("file1.txt", 512 * 1024).await?;
        env.upload_object("file2.txt", 512 * 1024).await?;

        // Delete one file
        env.client
            .delete_object()
            .bucket(&env.bucket_name)
            .key("file1.txt")
            .send()
            .await?;

        assert!(!env.object_exists("file1.txt").await?);

        // Now we should be able to upload again (quota freed up)
        env.upload_object("file3.txt", 256 * 1024).await?;
        assert!(env.object_exists("file3.txt").await?);

        env.cleanup_bucket().await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_quota_usage_tracking() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        init_logging();
        let env = QuotaTestEnv::new().await?;

        env.create_bucket().await?;

        // Set quota
        env.set_bucket_quota(2 * 1024 * 1024).await?;

        // Upload some files
        env.upload_object("file1.txt", 512 * 1024).await?;
        env.upload_object("file2.txt", 256 * 1024).await?;

        // Check usage
        let usage = env.get_bucket_usage().await?;
        assert_eq!(usage, (512 + 256) * 1024);

        // Delete a file
        env.client
            .delete_object()
            .bucket(&env.bucket_name)
            .key("file1.txt")
            .send()
            .await?;

        // Check updated usage
        let updated_usage = env.get_bucket_usage().await?;
        assert_eq!(updated_usage, 256 * 1024);

        env.cleanup_bucket().await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_quota_statistics() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        init_logging();
        let env = QuotaTestEnv::new().await?;

        env.create_bucket().await?;

        // Set quota of 2MB
        env.set_bucket_quota(2 * 1024 * 1024).await?;

        // Upload files to use 1.5MB
        env.upload_object("file1.txt", 1024 * 1024).await?;
        env.upload_object("file2.txt", 512 * 1024).await?;

        // Get detailed quota statistics
        let stats = env.get_bucket_quota_stats().await?;

        assert_eq!(stats.get("bucket").unwrap().as_str().unwrap(), env.bucket_name);
        assert_eq!(stats.get("quota_limit").unwrap().as_u64().unwrap(), 2 * 1024 * 1024);
        assert_eq!(stats.get("current_usage").unwrap().as_u64().unwrap(), (1024 + 512) * 1024);
        assert_eq!(stats.get("remaining_quota").unwrap().as_u64().unwrap(), 512 * 1024);

        let usage_percentage = stats.get("usage_percentage").unwrap().as_f64().unwrap();
        assert!((usage_percentage - 75.0).abs() < 0.1);

        env.cleanup_bucket().await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_quota_check_api() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        init_logging();
        let env = QuotaTestEnv::new().await?;

        env.create_bucket().await?;

        // Set quota of 1MB
        env.set_bucket_quota(1024 * 1024).await?;

        // Upload 512KB file
        env.upload_object("existing_file.txt", 512 * 1024).await?;

        // Check if we can upload another 512KB (should succeed, exactly fill quota)
        let check_result = env.check_bucket_quota("PUT", 512 * 1024).await?;
        assert!(check_result.get("allowed").unwrap().as_bool().unwrap());
        assert_eq!(check_result.get("remaining_quota").unwrap().as_u64().unwrap(), 0);

        // Note: we haven't actually uploaded the second file yet, so current_usage is still 512KB
        // Check if we can upload 1KB (should succeed - we haven't used the full quota yet)
        let check_result = env.check_bucket_quota("PUT", 1024).await?;
        assert!(check_result.get("allowed").unwrap().as_bool().unwrap());
        assert_eq!(check_result.get("remaining_quota").unwrap().as_u64().unwrap(), 523264); // 511 * 1024

        // Check if we can upload 600KB (should fail - would exceed quota)
        let check_result = env.check_bucket_quota("PUT", 600 * 1024).await?;
        assert!(!check_result.get("allowed").unwrap().as_bool().unwrap());

        // Check delete operation (should always be allowed)
        let check_result = env.check_bucket_quota("DELETE", 512 * 1024).await?;
        assert!(check_result.get("allowed").unwrap().as_bool().unwrap());

        env.cleanup_bucket().await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_quota_multiple_buckets() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        init_logging();
        let env = QuotaTestEnv::new().await?;

        // Create two buckets in the same environment
        let bucket1 = format!("quota-test-{}-1", uuid::Uuid::new_v4());
        let bucket2 = format!("quota-test-{}-2", uuid::Uuid::new_v4());

        env.env.create_test_bucket(&bucket1).await?;
        env.env.create_test_bucket(&bucket2).await?;

        // Set different quotas for each bucket
        env.set_bucket_quota_for(&bucket1, 1024 * 1024).await?; // 1MB
        env.set_bucket_quota_for(&bucket2, 2 * 1024 * 1024).await?; // 2MB

        // Fill first bucket to quota
        env.upload_object_to_bucket(&bucket1, "big_file.txt", 1024 * 1024).await?;

        // Should still be able to upload to second bucket
        env.upload_object_to_bucket(&bucket2, "big_file.txt", 1024 * 1024).await?;
        env.upload_object_to_bucket(&bucket2, "another_file.txt", 512 * 1024).await?;

        // Verify statistics are independent
        let stats1 = env.get_bucket_quota_stats_for(&bucket1).await?;
        let stats2 = env.get_bucket_quota_stats_for(&bucket2).await?;

        assert_eq!(stats1.get("current_usage").unwrap().as_u64().unwrap(), 1024 * 1024);
        assert_eq!(stats2.get("current_usage").unwrap().as_u64().unwrap(), (1024 + 512) * 1024);

        // Clean up
        env.env.delete_test_bucket(&bucket1).await?;
        env.env.delete_test_bucket(&bucket2).await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_quota_error_handling() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        init_logging();
        let env = QuotaTestEnv::new().await?;

        env.create_bucket().await?;

        // Test invalid quota type
        let url = format!("{}/rustfs/admin/v3/quota/{}", env.env.url, env.bucket_name);

        let invalid_config = serde_json::json!({
            "quota": 1024,
            "quota_type": "SOFT"  // Invalid type
        });

        let response = awscurl_put(&url, &invalid_config.to_string(), &env.env.access_key, &env.env.secret_key).await;
        assert!(response.is_err());
        let error_msg = response.unwrap_err().to_string();
        assert!(error_msg.contains("InvalidArgument"));

        // Test operations on non-existent bucket
        let url = format!("{}/rustfs/admin/v3/quota/non-existent-bucket", env.env.url);
        let response = awscurl_get(&url, &env.env.access_key, &env.env.secret_key).await;
        assert!(response.is_err());
        let error_msg = response.unwrap_err().to_string();
        assert!(error_msg.contains("NoSuchBucket"));

        env.cleanup_bucket().await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_quota_http_endpoints() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        init_logging();
        let env = QuotaTestEnv::new().await?;

        env.create_bucket().await?;

        // Test 1: GET quota for bucket without quota config
        let url = format!("{}/rustfs/admin/v3/quota/{}", env.env.url, env.bucket_name);
        let response = awscurl_get(&url, &env.env.access_key, &env.env.secret_key).await?;
        assert!(response.contains("quota") && response.contains("null"));

        // Test 2: PUT quota - valid config
        let quota_config = serde_json::json!({
            "quota": 1048576,
            "quota_type": "HARD"
        });
        let response = awscurl_put(&url, &quota_config.to_string(), &env.env.access_key, &env.env.secret_key).await?;
        assert!(response.contains("success") || !response.contains("error"));

        // Test 3: GET quota after setting
        let response = awscurl_get(&url, &env.env.access_key, &env.env.secret_key).await?;
        assert!(response.contains("1048576"));

        // Test 4: GET quota stats
        let stats_url = format!("{}/rustfs/admin/v3/quota-stats/{}", env.env.url, env.bucket_name);
        let response = awscurl_get(&stats_url, &env.env.access_key, &env.env.secret_key).await?;
        assert!(response.contains("quota_limit") && response.contains("current_usage"));

        // Test 5: POST quota check
        let check_url = format!("{}/rustfs/admin/v3/quota-check/{}", env.env.url, env.bucket_name);
        let check_request = serde_json::json!({
            "operation_type": "PUT",
            "operation_size": 1024
        });
        let response = awscurl_post(&check_url, &check_request.to_string(), &env.env.access_key, &env.env.secret_key).await?;
        assert!(response.contains("allowed"));

        // Test 6: DELETE quota
        let response = awscurl_delete(&url, &env.env.access_key, &env.env.secret_key).await?;
        assert!(!response.contains("error"));

        // Test 7: GET quota after deletion
        let response = awscurl_get(&url, &env.env.access_key, &env.env.secret_key).await?;
        assert!(response.contains("quota") && response.contains("null"));

        // Test 8: Invalid quota type
        let invalid_config = serde_json::json!({
            "quota": 1024,
            "quota_type": "SOFT"
        });
        let response = awscurl_put(&url, &invalid_config.to_string(), &env.env.access_key, &env.env.secret_key).await;
        assert!(response.is_err());
        let error_msg = response.unwrap_err().to_string();
        assert!(error_msg.contains("InvalidArgument"));

        env.cleanup_bucket().await?;

        Ok(())
    }

    /// Test that a normal user with `readwrite` policy can read quota but cannot set/clear quota.
    #[tokio::test]
    #[serial]
    async fn test_quota_normal_user_permissions() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        init_logging();
        let env = QuotaTestEnv::new().await?;
        env.create_bucket().await?;

        // Admin sets quota first
        env.set_bucket_quota(1024 * 1024).await?;

        // Create a normal user via admin API
        let normal_ak = "normaluser";
        let normal_sk = "normaluser123";
        let add_user_url = format!("{}/rustfs/admin/v3/add-user?accessKey={}", env.env.url, normal_ak);
        let user_body = serde_json::json!({ "secretKey": normal_sk, "status": "enabled" }).to_string();
        awscurl_put(&add_user_url, &user_body, &env.env.access_key, &env.env.secret_key).await?;

        // Attach `readwrite` policy to the normal user
        let policy_url = format!(
            "{}/rustfs/admin/v3/set-user-or-group-policy?policyName=readwrite&userOrGroup={}&isGroup=false",
            env.env.url, normal_ak
        );
        awscurl_put(&policy_url, "", &env.env.access_key, &env.env.secret_key).await?;

        // Normal user reads quota — should succeed
        let get_url = format!("{}/rustfs/admin/v3/quota/{}", env.env.url, env.bucket_name);
        let resp = awscurl_get(&get_url, normal_ak, normal_sk).await?;
        let quota_info: serde_json::Value = serde_json::from_str(&resp)?;
        assert_eq!(quota_info.get("quota").and_then(|v| v.as_u64()), Some(1024 * 1024));

        // Normal user reads quota stats — should succeed
        let stats_url = format!("{}/rustfs/admin/v3/quota-stats/{}", env.env.url, env.bucket_name);
        let resp = awscurl_get(&stats_url, normal_ak, normal_sk).await?;
        assert!(resp.contains("quota_limit"));

        // Normal user sets quota — should be denied
        let set_resp = awscurl_put(
            &get_url,
            &serde_json::json!({"quota": 2048, "quota_type": "HARD"}).to_string(),
            normal_ak,
            normal_sk,
        )
        .await;
        assert!(set_resp.is_err(), "normal user should not be able to set quota");

        // Normal user clears quota — should be denied
        let del_resp = awscurl_delete(&get_url, normal_ak, normal_sk).await;
        assert!(del_resp.is_err(), "normal user should not be able to clear quota");

        env.cleanup_bucket().await?;
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_quota_copy_operations() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        init_logging();
        let env = QuotaTestEnv::new().await?;

        env.create_bucket().await?;

        // Set quota of 2MB
        env.set_bucket_quota(2 * 1024 * 1024).await?;

        // Upload initial file
        env.upload_object("original.txt", 1024 * 1024).await?;

        // Copy file - should succeed (1MB each, total 2MB)
        env.client
            .copy_object()
            .bucket(&env.bucket_name)
            .key("copy1.txt")
            .copy_source(format!("{}/{}", env.bucket_name, "original.txt"))
            .send()
            .await?;

        assert!(env.object_exists("copy1.txt").await?);

        // Try to copy again - should fail (1.5MB each, total 3MB > 2MB quota)
        let copy_result = env
            .client
            .copy_object()
            .bucket(&env.bucket_name)
            .key("copy2.txt")
            .copy_source(format!("{}/{}", env.bucket_name, "original.txt"))
            .send()
            .await;

        assert!(copy_result.is_err());
        assert!(!env.object_exists("copy2.txt").await?);

        env.cleanup_bucket().await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_quota_batch_delete() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        init_logging();
        let env = QuotaTestEnv::new().await?;

        env.create_bucket().await?;

        // Set quota of 2MB
        env.set_bucket_quota(2 * 1024 * 1024).await?;

        // Upload files to fill quota
        env.upload_object("file1.txt", 1024 * 1024).await?;
        env.upload_object("file2.txt", 1024 * 1024).await?;

        // Verify quota is full
        let upload_result = env.upload_object("file3.txt", 1024).await;
        assert!(upload_result.is_err());

        // Delete multiple objects using batch delete
        let objects = vec![
            aws_sdk_s3::types::ObjectIdentifier::builder()
                .key("file1.txt")
                .build()
                .unwrap(),
            aws_sdk_s3::types::ObjectIdentifier::builder()
                .key("file2.txt")
                .build()
                .unwrap(),
        ];

        let delete_result = env
            .client
            .delete_objects()
            .bucket(&env.bucket_name)
            .delete(
                aws_sdk_s3::types::Delete::builder()
                    .set_objects(Some(objects))
                    .quiet(true)
                    .build()
                    .unwrap(),
            )
            .send()
            .await?;

        assert_eq!(delete_result.deleted().len(), 2);

        // Now should be able to upload again (quota freed up)
        env.upload_object("file3.txt", 256 * 1024).await?;
        assert!(env.object_exists("file3.txt").await?);

        env.cleanup_bucket().await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_quota_multipart_upload() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        init_logging();
        let env = QuotaTestEnv::new().await?;

        env.create_bucket().await?;

        // Set quota of 10MB
        env.set_bucket_quota(10 * 1024 * 1024).await?;

        let key = "multipart_test.txt";
        let part_size = 5 * 1024 * 1024; // 5MB minimum per part (S3 requirement)

        // Test 1: Multipart upload within quota (single 5MB part)
        let create_result = env
            .client
            .create_multipart_upload()
            .bucket(&env.bucket_name)
            .key(key)
            .send()
            .await?;

        let upload_id = create_result.upload_id().unwrap();

        // Upload single 5MB part (S3 allows single part with any size ≥ 5MB for the only part)
        let part_data = vec![1u8; part_size];
        let part_result = env
            .client
            .upload_part()
            .bucket(&env.bucket_name)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(aws_sdk_s3::primitives::ByteStream::from(part_data))
            .send()
            .await?;

        let uploaded_parts = vec![
            aws_sdk_s3::types::CompletedPart::builder()
                .part_number(1)
                .e_tag(part_result.e_tag().unwrap())
                .build(),
        ];

        env.client
            .complete_multipart_upload()
            .bucket(&env.bucket_name)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                aws_sdk_s3::types::CompletedMultipartUpload::builder()
                    .set_parts(Some(uploaded_parts))
                    .build(),
            )
            .send()
            .await?;

        assert!(env.object_exists(key).await?);

        // Test 2: Multipart upload exceeds quota (should fail)
        // Upload 6MB filler (total now: 5MB + 6MB = 11MB > 10MB quota)
        let upload_filler = env.upload_object("filler.txt", 6 * 1024 * 1024).await;
        // This should fail due to quota
        assert!(upload_filler.is_err());

        // Verify filler doesn't exist
        assert!(!env.object_exists("filler.txt").await?);

        // Now try a multipart upload that exceeds quota
        // Current usage: 5MB (from Test 1), quota: 10MB
        // Trying to upload 6MB via multipart → should fail

        let create_result2 = env
            .client
            .create_multipart_upload()
            .bucket(&env.bucket_name)
            .key("over_quota.txt")
            .send()
            .await?;

        let upload_id2 = create_result2.upload_id().unwrap();

        let mut uploaded_parts2 = vec![];
        for part_num in 1..=2 {
            let part_data = vec![part_num as u8; part_size];
            let part_result = env
                .client
                .upload_part()
                .bucket(&env.bucket_name)
                .key("over_quota.txt")
                .upload_id(upload_id2)
                .part_number(part_num)
                .body(aws_sdk_s3::primitives::ByteStream::from(part_data))
                .send()
                .await?;

            uploaded_parts2.push(
                aws_sdk_s3::types::CompletedPart::builder()
                    .part_number(part_num)
                    .e_tag(part_result.e_tag().unwrap())
                    .build(),
            );
        }

        let complete_result = env
            .client
            .complete_multipart_upload()
            .bucket(&env.bucket_name)
            .key("over_quota.txt")
            .upload_id(upload_id2)
            .multipart_upload(
                aws_sdk_s3::types::CompletedMultipartUpload::builder()
                    .set_parts(Some(uploaded_parts2))
                    .build(),
            )
            .send()
            .await;

        assert!(complete_result.is_err());
        assert!(!env.object_exists("over_quota.txt").await?);

        env.cleanup_bucket().await?;

        Ok(())
    }
}
