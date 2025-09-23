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

//! Comprehensive KMS integration tests
//!
//! This module contains comprehensive end-to-end tests that combine multiple KMS features
//! and test real-world scenarios with mixed encryption types, large datasets, and
//! complex workflows.

use super::common::{
    EncryptionType, LocalKMSTestEnvironment, MultipartTestConfig, create_sse_c_config, test_all_multipart_encryption_types,
    test_kms_key_management, test_multipart_upload_with_config, test_sse_c_encryption, test_sse_kms_encryption,
    test_sse_s3_encryption,
};
use crate::common::{TEST_BUCKET, init_logging};
use serial_test::serial;
use tokio::time::{Duration, sleep};
use tracing::info;

/// Comprehensive test: Full KMS workflow with all encryption types
#[tokio::test]
#[serial]
async fn test_comprehensive_kms_full_workflow() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("🏁 开始KMS全功能综合测试");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    sleep(Duration::from_secs(3)).await;

    let s3_client = kms_env.base_env.create_s3_client();
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    // Phase 1: Test all single encryption types
    info!("📋 阶段1: 测试所有单文件加密类型");
    test_sse_s3_encryption(&s3_client, TEST_BUCKET).await?;
    test_sse_kms_encryption(&s3_client, TEST_BUCKET).await?;
    test_sse_c_encryption(&s3_client, TEST_BUCKET).await?;

    // Phase 2: Test KMS key management APIs
    info!("📋 阶段2: 测试KMS密钥管理API");
    test_kms_key_management(&kms_env.base_env.url, &kms_env.base_env.access_key, &kms_env.base_env.secret_key).await?;

    // Phase 3: Test all multipart encryption types
    info!("📋 阶段3: 测试所有分片上传加密类型");
    test_all_multipart_encryption_types(&s3_client, TEST_BUCKET, "comprehensive-multipart-test").await?;

    // Phase 4: Mixed workload test
    info!("📋 阶段4: 混合工作负载测试");
    test_mixed_encryption_workload(&s3_client, TEST_BUCKET).await?;

    kms_env.base_env.delete_test_bucket(TEST_BUCKET).await?;
    info!("✅ KMS全功能综合测试通过");
    Ok(())
}

/// Test mixed encryption workload with different file sizes and encryption types
async fn test_mixed_encryption_workload(
    s3_client: &aws_sdk_s3::Client,
    bucket: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("🔄 测试混合加密工作负载");

    // Test configuration: different sizes and encryption types
    let test_configs = vec![
        // Small single-part uploads (S3 allows <5MB for the final part)
        MultipartTestConfig::new("mixed-small-none", 1024 * 1024, 1, EncryptionType::None),
        MultipartTestConfig::new("mixed-small-sse-s3", 1024 * 1024, 1, EncryptionType::SSES3),
        MultipartTestConfig::new("mixed-small-sse-kms", 1024 * 1024, 1, EncryptionType::SSEKMS),
        // SSE-C multipart uploads must respect the 5MB minimum part-size to avoid inline storage paths
        MultipartTestConfig::new("mixed-medium-sse-s3", 5 * 1024 * 1024, 3, EncryptionType::SSES3),
        MultipartTestConfig::new("mixed-medium-sse-kms", 5 * 1024 * 1024, 3, EncryptionType::SSEKMS),
        MultipartTestConfig::new("mixed-medium-sse-c", 5 * 1024 * 1024, 3, create_sse_c_config()),
        // Large multipart files
        MultipartTestConfig::new("mixed-large-sse-s3", 10 * 1024 * 1024, 2, EncryptionType::SSES3),
        MultipartTestConfig::new("mixed-large-sse-kms", 10 * 1024 * 1024, 2, EncryptionType::SSEKMS),
        MultipartTestConfig::new("mixed-large-sse-c", 10 * 1024 * 1024, 2, create_sse_c_config()),
    ];

    for (i, config) in test_configs.iter().enumerate() {
        info!("🔄 执行混合测试 {}/{}: {:?}", i + 1, test_configs.len(), config.encryption_type);
        test_multipart_upload_with_config(s3_client, bucket, config).await?;
    }

    info!("✅ 混合加密工作负载测试通过");
    Ok(())
}

/// Comprehensive stress test: Large dataset with multiple encryption types
#[tokio::test]
#[serial]
async fn test_comprehensive_stress_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("💪 开始KMS压力测试");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    sleep(Duration::from_secs(3)).await;

    let s3_client = kms_env.base_env.create_s3_client();
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    // Large multipart uploads with different encryption types
    let stress_configs = vec![
        MultipartTestConfig::new("stress-sse-s3-large", 15 * 1024 * 1024, 4, EncryptionType::SSES3),
        MultipartTestConfig::new("stress-sse-kms-large", 15 * 1024 * 1024, 4, EncryptionType::SSEKMS),
        MultipartTestConfig::new("stress-sse-c-large", 15 * 1024 * 1024, 4, create_sse_c_config()),
    ];

    for config in stress_configs {
        info!(
            "💪 执行压力测试: {:?}, 总大小: {}MB",
            config.encryption_type,
            config.total_size() / (1024 * 1024)
        );
        test_multipart_upload_with_config(&s3_client, TEST_BUCKET, &config).await?;
    }

    kms_env.base_env.delete_test_bucket(TEST_BUCKET).await?;
    info!("✅ KMS压力测试通过");
    Ok(())
}

/// Test encryption key isolation and security
#[tokio::test]
#[serial]
async fn test_comprehensive_key_isolation() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("🔐 开始加密密钥隔离综合测试");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    sleep(Duration::from_secs(3)).await;

    let s3_client = kms_env.base_env.create_s3_client();
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    // Test different SSE-C keys to ensure isolation
    let key1 = "01234567890123456789012345678901";
    let key2 = "98765432109876543210987654321098";
    let key1_md5 = format!("{:x}", md5::compute(key1));
    let key2_md5 = format!("{:x}", md5::compute(key2));

    let config1 = MultipartTestConfig::new(
        "isolation-test-key1",
        5 * 1024 * 1024,
        2,
        EncryptionType::SSEC {
            key: key1.to_string(),
            key_md5: key1_md5,
        },
    );

    let config2 = MultipartTestConfig::new(
        "isolation-test-key2",
        5 * 1024 * 1024,
        2,
        EncryptionType::SSEC {
            key: key2.to_string(),
            key_md5: key2_md5,
        },
    );

    // Upload with different keys
    info!("🔐 上传文件用密钥1");
    test_multipart_upload_with_config(&s3_client, TEST_BUCKET, &config1).await?;

    info!("🔐 上传文件用密钥2");
    test_multipart_upload_with_config(&s3_client, TEST_BUCKET, &config2).await?;

    // Verify that files cannot be read with wrong keys
    info!("🔒 验证密钥隔离");
    let wrong_key = "11111111111111111111111111111111";
    let wrong_key_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, wrong_key);
    let wrong_key_md5 = format!("{:x}", md5::compute(wrong_key));

    // Try to read file encrypted with key1 using wrong key
    let wrong_read_result = s3_client
        .get_object()
        .bucket(TEST_BUCKET)
        .key(&config1.object_key)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&wrong_key_b64)
        .sse_customer_key_md5(&wrong_key_md5)
        .send()
        .await;

    assert!(wrong_read_result.is_err(), "应该无法用错误密钥读取加密文件");
    info!("✅ 确认密钥隔离正常工作");

    kms_env.base_env.delete_test_bucket(TEST_BUCKET).await?;
    info!("✅ 加密密钥隔离综合测试通过");
    Ok(())
}

/// Test concurrent encryption operations
#[tokio::test]
#[serial]
async fn test_comprehensive_concurrent_operations() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("⚡ 开始并发加密操作综合测试");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    sleep(Duration::from_secs(3)).await;

    let s3_client = kms_env.base_env.create_s3_client();
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    // Create multiple concurrent upload tasks
    let multipart_part_size = 5 * 1024 * 1024; // honour S3 minimum part size for multipart uploads
    let concurrent_configs = vec![
        MultipartTestConfig::new("concurrent-1-sse-s3", multipart_part_size, 2, EncryptionType::SSES3),
        MultipartTestConfig::new("concurrent-2-sse-kms", multipart_part_size, 2, EncryptionType::SSEKMS),
        MultipartTestConfig::new("concurrent-3-sse-c", multipart_part_size, 2, create_sse_c_config()),
        MultipartTestConfig::new("concurrent-4-none", multipart_part_size, 2, EncryptionType::None),
    ];

    // Execute uploads concurrently
    info!("⚡ 开始并发上传");
    let mut tasks = Vec::new();
    for config in concurrent_configs {
        let client = s3_client.clone();
        let bucket = TEST_BUCKET.to_string();
        tasks.push(tokio::spawn(
            async move { test_multipart_upload_with_config(&client, &bucket, &config).await },
        ));
    }

    // Wait for all tasks to complete
    for task in tasks {
        task.await??;
    }

    info!("✅ 所有并发操作完成");

    kms_env.base_env.delete_test_bucket(TEST_BUCKET).await?;
    info!("✅ 并发加密操作综合测试通过");
    Ok(())
}

/// Test encryption/decryption performance with different file sizes
#[tokio::test]
#[serial]
async fn test_comprehensive_performance_benchmark() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("📊 开始KMS性能基准测试");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    sleep(Duration::from_secs(3)).await;

    let s3_client = kms_env.base_env.create_s3_client();
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    // Performance test configurations with increasing file sizes
    let perf_configs = vec![
        ("small", MultipartTestConfig::new("perf-small", 1024 * 1024, 1, EncryptionType::SSES3)),
        (
            "medium",
            MultipartTestConfig::new("perf-medium", 5 * 1024 * 1024, 2, EncryptionType::SSES3),
        ),
        (
            "large",
            MultipartTestConfig::new("perf-large", 10 * 1024 * 1024, 3, EncryptionType::SSES3),
        ),
    ];

    for (size_name, config) in perf_configs {
        info!("📊 测试{}文件性能 ({}MB)", size_name, config.total_size() / (1024 * 1024));

        let start_time = std::time::Instant::now();
        test_multipart_upload_with_config(&s3_client, TEST_BUCKET, &config).await?;
        let duration = start_time.elapsed();

        let throughput_mbps = (config.total_size() as f64 / (1024.0 * 1024.0)) / duration.as_secs_f64();
        info!(
            "📊 {}文件测试完成: {:.2}秒, 吞吐量: {:.2} MB/s",
            size_name,
            duration.as_secs_f64(),
            throughput_mbps
        );
    }

    kms_env.base_env.delete_test_bucket(TEST_BUCKET).await?;
    info!("✅ KMS性能基准测试通过");
    Ok(())
}
