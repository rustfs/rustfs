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

//! End-to-end tests for Vault KMS backend
//!
//! These tests mirror the local KMS coverage but target the Vault backend.
//! They validate Vault bootstrap, admin API flows, encryption modes, and
//! multipart upload behaviour.

use crate::common::{TEST_BUCKET, init_logging};
use serial_test::serial;
use tokio::time::{Duration, sleep};
use tracing::{error, info};

use super::common::{
    VAULT_KEY_NAME, VaultTestEnvironment, get_kms_status, start_kms, test_all_multipart_encryption_types, test_error_scenarios,
    test_kms_key_management, test_sse_c_encryption, test_sse_kms_encryption, test_sse_s3_encryption,
};

/// Helper that brings up Vault, configures RustFS, and starts the KMS service.
struct VaultKmsTestContext {
    env: VaultTestEnvironment,
}

impl VaultKmsTestContext {
    async fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut env = VaultTestEnvironment::new().await?;

        env.start_vault().await?;
        env.setup_vault_transit().await?;

        env.start_rustfs_for_vault().await?;
        env.configure_vault_kms().await?;

        start_kms(&env.base_env.url, &env.base_env.access_key, &env.base_env.secret_key).await?;

        // Allow Vault to finish initialising token auth and transit engine.
        sleep(Duration::from_secs(2)).await;

        Ok(Self { env })
    }

    fn base_env(&self) -> &crate::common::RustFSTestEnvironment {
        &self.env.base_env
    }

    fn s3_client(&self) -> aws_sdk_s3::Client {
        self.env.base_env.create_s3_client()
    }
}

#[tokio::test]
#[serial]
async fn test_vault_kms_end_to_end() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("Starting Vault KMS End-to-End Test with default key {}", VAULT_KEY_NAME);

    let context = VaultKmsTestContext::new().await?;

    match get_kms_status(&context.base_env().url, &context.base_env().access_key, &context.base_env().secret_key).await {
        Ok(status) => info!("Vault KMS status after startup: {}", status),
        Err(err) => {
            error!("Failed to query Vault KMS status: {}", err);
            return Err(err);
        }
    }

    let s3_client = context.s3_client();
    context
        .base_env()
        .create_test_bucket(TEST_BUCKET)
        .await
        .expect("Failed to create test bucket");

    test_kms_key_management(&context.base_env().url, &context.base_env().access_key, &context.base_env().secret_key)
        .await
        .expect("Vault KMS key management test failed");

    test_sse_c_encryption(&s3_client, TEST_BUCKET)
        .await
        .expect("Vault SSE-C encryption test failed");

    test_sse_s3_encryption(&s3_client, TEST_BUCKET)
        .await
        .expect("Vault SSE-S3 encryption test failed");

    test_sse_kms_encryption(&s3_client, TEST_BUCKET)
        .await
        .expect("Vault SSE-KMS encryption test failed");

    test_error_scenarios(&s3_client, TEST_BUCKET)
        .await
        .expect("Vault KMS error scenario test failed");

    context
        .base_env()
        .delete_test_bucket(TEST_BUCKET)
        .await
        .expect("Failed to delete test bucket");

    info!("Vault KMS End-to-End Test completed successfully");
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_vault_kms_key_isolation() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("Starting Vault KMS SSE-C key isolation test");

    let context = VaultKmsTestContext::new().await?;

    let s3_client = context.s3_client();
    context
        .base_env()
        .create_test_bucket(TEST_BUCKET)
        .await
        .expect("Failed to create test bucket");

    let key1 = "01234567890123456789012345678901";
    let key2 = "98765432109876543210987654321098";
    let key1_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, key1);
    let key2_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, key2);
    let key1_md5 = format!("{:x}", md5::compute(key1));
    let key2_md5 = format!("{:x}", md5::compute(key2));

    let data1 = b"Vault data encrypted with key 1";
    let data2 = b"Vault data encrypted with key 2";

    s3_client
        .put_object()
        .bucket(TEST_BUCKET)
        .key("vault-object1")
        .body(aws_sdk_s3::primitives::ByteStream::from(data1.to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key1_b64)
        .sse_customer_key_md5(&key1_md5)
        .send()
        .await
        .expect("Failed to upload object1 with key1");

    s3_client
        .put_object()
        .bucket(TEST_BUCKET)
        .key("vault-object2")
        .body(aws_sdk_s3::primitives::ByteStream::from(data2.to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key2_b64)
        .sse_customer_key_md5(&key2_md5)
        .send()
        .await
        .expect("Failed to upload object2 with key2");

    let object1 = s3_client
        .get_object()
        .bucket(TEST_BUCKET)
        .key("vault-object1")
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key1_b64)
        .sse_customer_key_md5(&key1_md5)
        .send()
        .await
        .expect("Failed to download object1 with key1");

    let downloaded1 = object1.body.collect().await.expect("Failed to read object1").into_bytes();
    assert_eq!(downloaded1.as_ref(), data1);

    let wrong_key = s3_client
        .get_object()
        .bucket(TEST_BUCKET)
        .key("vault-object1")
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key2_b64)
        .sse_customer_key_md5(&key2_md5)
        .send()
        .await;
    assert!(wrong_key.is_err(), "Object1 should not decrypt with key2");

    context
        .base_env()
        .delete_test_bucket(TEST_BUCKET)
        .await
        .expect("Failed to delete test bucket");

    info!("Vault KMS SSE-C key isolation test completed successfully");
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_vault_kms_large_file() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("Starting Vault KMS large file SSE-S3 test");

    let context = VaultKmsTestContext::new().await?;
    let s3_client = context.s3_client();
    context
        .base_env()
        .create_test_bucket(TEST_BUCKET)
        .await
        .expect("Failed to create test bucket");

    let large_data = vec![0xCDu8; 1024 * 1024];
    let object_key = "vault-large-encrypted-file";

    let put_response = s3_client
        .put_object()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .body(aws_sdk_s3::primitives::ByteStream::from(large_data.clone()))
        .server_side_encryption(aws_sdk_s3::types::ServerSideEncryption::Aes256)
        .send()
        .await
        .expect("Failed to upload large SSE-S3 object");
    assert_eq!(
        put_response.server_side_encryption(),
        Some(&aws_sdk_s3::types::ServerSideEncryption::Aes256)
    );

    let get_response = s3_client
        .get_object()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .send()
        .await
        .expect("Failed to download large SSE-S3 object");
    assert_eq!(
        get_response.server_side_encryption(),
        Some(&aws_sdk_s3::types::ServerSideEncryption::Aes256)
    );

    let downloaded = get_response
        .body
        .collect()
        .await
        .expect("Failed to read large object body")
        .into_bytes();
    assert_eq!(downloaded.len(), large_data.len());
    assert_eq!(downloaded.as_ref(), large_data.as_slice());

    context
        .base_env()
        .delete_test_bucket(TEST_BUCKET)
        .await
        .expect("Failed to delete test bucket");

    info!("Vault KMS large file test completed successfully");
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_vault_kms_multipart_upload() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("Starting Vault KMS multipart upload encryption suite");

    let context = VaultKmsTestContext::new().await?;
    let s3_client = context.s3_client();
    context
        .base_env()
        .create_test_bucket(TEST_BUCKET)
        .await
        .expect("Failed to create test bucket");

    test_all_multipart_encryption_types(&s3_client, TEST_BUCKET, "vault-multipart")
        .await
        .expect("Vault multipart encryption test suite failed");

    context
        .base_env()
        .delete_test_bucket(TEST_BUCKET)
        .await
        .expect("Failed to delete test bucket");

    info!("Vault KMS multipart upload tests completed successfully");
    Ok(())
}
