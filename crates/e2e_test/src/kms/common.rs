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

#![allow(dead_code)]
#![allow(clippy::upper_case_acronyms)]

//! KMS-specific utilities for end-to-end tests
//!
//! This module provides KMS-specific functionality including:
//! - Vault server management and configuration
//! - KMS backend configuration (Local and Vault)
//! - SSE encryption testing utilities

use crate::common::{RustFSTestEnvironment, awscurl_get, awscurl_post, init_logging as common_init_logging};
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ServerSideEncryption;
use base64::Engine;
use serde_json;
use std::process::{Child, Command};
use std::time::Duration;
use tokio::fs;
use tokio::net::TcpStream;
use tokio::time::sleep;
use tracing::{debug, error, info};

// KMS-specific constants
pub const TEST_BUCKET: &str = "kms-test-bucket";

// Vault constants
pub const VAULT_URL: &str = "http://127.0.0.1:8200";
pub const VAULT_ADDRESS: &str = "127.0.0.1:8200";
pub const VAULT_TOKEN: &str = "dev-root-token";
pub const VAULT_TRANSIT_PATH: &str = "transit";
pub const VAULT_KEY_NAME: &str = "rustfs-master-key";

/// Initialize tracing for KMS tests with KMS-specific log levels
pub fn init_logging() {
    common_init_logging();
    // Additional KMS-specific logging configuration can be added here if needed
}

// KMS-specific helper functions
/// Configure KMS backend via admin API
pub async fn configure_kms(
    base_url: &str,
    config_json: &str,
    access_key: &str,
    secret_key: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let url = format!("{base_url}/rustfs/admin/v3/kms/configure");
    awscurl_post(&url, config_json, access_key, secret_key).await?;
    info!("KMS configured successfully");
    Ok(())
}

/// Start KMS service via admin API
pub async fn start_kms(
    base_url: &str,
    access_key: &str,
    secret_key: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let url = format!("{base_url}/rustfs/admin/v3/kms/start");
    awscurl_post(&url, "{}", access_key, secret_key).await?;
    info!("KMS started successfully");
    Ok(())
}

/// Get KMS status via admin API
pub async fn get_kms_status(
    base_url: &str,
    access_key: &str,
    secret_key: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let url = format!("{base_url}/rustfs/admin/v3/kms/status");
    let status = awscurl_get(&url, access_key, secret_key).await?;
    info!("KMS status retrieved: {}", status);
    Ok(status)
}

/// Create a default KMS key for testing and return the created key ID
pub async fn create_default_key(
    base_url: &str,
    access_key: &str,
    secret_key: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let create_key_body = serde_json::json!({
        "KeyUsage": "ENCRYPT_DECRYPT",
        "Description": "Default key for e2e testing"
    })
    .to_string();

    let url = format!("{base_url}/rustfs/admin/v3/kms/keys");
    let response = awscurl_post(&url, &create_key_body, access_key, secret_key).await?;

    // Parse response to get the actual key ID
    let create_result: serde_json::Value = serde_json::from_str(&response)?;
    let key_id = create_result["key_id"]
        .as_str()
        .ok_or("Failed to get key_id from create response")?
        .to_string();

    info!("Default KMS key created: {}", key_id);
    Ok(key_id)
}

/// Create a KMS key with a specific ID (by directly writing to the key directory)
pub async fn create_key_with_specific_id(key_dir: &str, key_id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use rand::RngCore;
    use std::collections::HashMap;
    use tokio::fs;

    // Create a 32-byte AES key
    let mut key_data = [0u8; 32];
    rand::rng().fill_bytes(&mut key_data);

    // Create the stored key structure that Local KMS backend expects
    let stored_key = serde_json::json!({
        "key_id": key_id,
        "version": 1u32,
        "algorithm": "AES_256",
        "usage": "EncryptDecrypt",
        "status": "Active",
        "metadata": HashMap::<String, String>::new(),
        "created_at": chrono::Utc::now().to_rfc3339(),
        "rotated_at": serde_json::Value::Null,
        "created_by": "e2e-test",
        "encrypted_key_material": key_data.to_vec(),
        "nonce": Vec::<u8>::new()
    });

    // Write the key to file with the specified ID as JSON
    let key_path = format!("{key_dir}/{key_id}.key");
    let content = serde_json::to_vec_pretty(&stored_key)?;
    fs::write(&key_path, &content).await?;

    info!("Created KMS key with ID '{}' at path: {}", key_id, key_path);
    Ok(())
}

/// Test SSE-C encryption with the given S3 client
pub async fn test_sse_c_encryption(s3_client: &Client, bucket: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("Testing SSE-C encryption");

    let test_key = "01234567890123456789012345678901"; // 32-byte key
    let test_key_b64 = base64::engine::general_purpose::STANDARD.encode(test_key);
    let test_key_md5 = format!("{:x}", md5::compute(test_key));
    let test_data = b"Hello, KMS SSE-C World!";
    let object_key = "test-sse-c-object";

    // Upload with SSE-C (customer-provided key encryption)
    // Note: For SSE-C, we should NOT set server_side_encryption, only the customer key headers
    let put_response = s3_client
        .put_object()
        .bucket(bucket)
        .key(object_key)
        .body(ByteStream::from(test_data.to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&test_key_b64)
        .sse_customer_key_md5(&test_key_md5)
        .send()
        .await?;

    info!("SSE-C upload successful, ETag: {:?}", put_response.e_tag());
    // For SSE-C, server_side_encryption should be None since customer provides the key
    // The encryption algorithm is specified via SSE-C headers instead

    // Download with SSE-C
    info!("Starting SSE-C download test");
    let get_response = s3_client
        .get_object()
        .bucket(bucket)
        .key(object_key)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&test_key_b64)
        .sse_customer_key_md5(&test_key_md5)
        .send()
        .await?;
    info!("SSE-C download successful");

    info!("Starting to collect response body");
    let downloaded_data = get_response.body.collect().await?.into_bytes();
    info!("Downloaded data length: {}, expected length: {}", downloaded_data.len(), test_data.len());
    assert_eq!(downloaded_data.as_ref(), test_data);
    // For SSE-C, we don't check server_side_encryption since it's customer-managed

    info!("SSE-C encryption test completed successfully");
    Ok(())
}

/// Test SSE-S3 encryption (server-managed keys)
pub async fn test_sse_s3_encryption(s3_client: &Client, bucket: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("Testing SSE-S3 encryption");

    let test_data = b"Hello, KMS SSE-S3 World!";
    let object_key = "test-sse-s3-object";

    // Upload with SSE-S3
    let put_response = s3_client
        .put_object()
        .bucket(bucket)
        .key(object_key)
        .body(ByteStream::from(test_data.to_vec()))
        .server_side_encryption(ServerSideEncryption::Aes256)
        .send()
        .await?;

    info!("SSE-S3 upload successful, ETag: {:?}", put_response.e_tag());
    assert_eq!(put_response.server_side_encryption(), Some(&ServerSideEncryption::Aes256));

    // Download object
    let get_response = s3_client.get_object().bucket(bucket).key(object_key).send().await?;

    let encryption = get_response.server_side_encryption().cloned();
    let downloaded_data = get_response.body.collect().await?.into_bytes();
    assert_eq!(downloaded_data.as_ref(), test_data);
    assert_eq!(encryption, Some(ServerSideEncryption::Aes256));

    info!("SSE-S3 encryption test completed successfully");
    Ok(())
}

/// Test SSE-KMS encryption (KMS-managed keys)
pub async fn test_sse_kms_encryption(
    s3_client: &aws_sdk_s3::Client,
    bucket: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("Testing SSE-KMS encryption");

    let object_key = "test-sse-kms-object";
    let test_data = b"Hello, SSE-KMS World! This data should be encrypted with KMS-managed keys.";

    // Upload object with SSE-KMS encryption
    let put_response = s3_client
        .put_object()
        .bucket(bucket)
        .key(object_key)
        .body(aws_sdk_s3::primitives::ByteStream::from(test_data.to_vec()))
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .send()
        .await?;

    info!("SSE-KMS upload successful, ETag: {:?}", put_response.e_tag());
    assert_eq!(put_response.server_side_encryption(), Some(&ServerSideEncryption::AwsKms));

    // Download object
    let get_response = s3_client.get_object().bucket(bucket).key(object_key).send().await?;

    let encryption = get_response.server_side_encryption().cloned();
    let downloaded_data = get_response.body.collect().await?.into_bytes();
    assert_eq!(downloaded_data.as_ref(), test_data);
    assert_eq!(encryption, Some(ServerSideEncryption::AwsKms));

    info!("SSE-KMS encryption test completed successfully");
    Ok(())
}

/// Test KMS key management APIs
pub async fn test_kms_key_management(
    base_url: &str,
    access_key: &str,
    secret_key: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("Testing KMS key management APIs");

    // Test CreateKey
    let create_key_body = serde_json::json!({
        "KeyUsage": "EncryptDecrypt",
        "Description": "Test key for e2e testing"
    })
    .to_string();

    let create_response =
        awscurl_post(&format!("{base_url}/rustfs/admin/v3/kms/keys"), &create_key_body, access_key, secret_key).await?;

    let create_result: serde_json::Value = serde_json::from_str(&create_response)?;
    let key_id = create_result["key_id"]
        .as_str()
        .ok_or("Failed to get key_id from create response")?;
    info!("Created key with ID: {}", key_id);

    // Test DescribeKey
    let describe_response = awscurl_get(&format!("{base_url}/rustfs/admin/v3/kms/keys/{key_id}"), access_key, secret_key).await?;

    info!("DescribeKey response: {}", describe_response);
    let describe_result: serde_json::Value = serde_json::from_str(&describe_response)?;
    info!("Parsed describe result: {:?}", describe_result);
    assert_eq!(describe_result["key_metadata"]["key_id"], key_id);
    info!("Successfully described key: {}", key_id);

    // Test ListKeys
    let list_response = awscurl_get(&format!("{base_url}/rustfs/admin/v3/kms/keys"), access_key, secret_key).await?;

    let list_result: serde_json::Value = serde_json::from_str(&list_response)?;
    let keys = list_result["keys"]
        .as_array()
        .ok_or("Failed to get keys array from list response")?;

    let found_key = keys.iter().any(|k| k["key_id"].as_str() == Some(key_id));
    assert!(found_key, "Created key not found in list");
    info!("Successfully listed keys, found created key");

    info!("KMS key management API tests completed successfully");
    Ok(())
}

/// Test error scenarios
pub async fn test_error_scenarios(s3_client: &Client, bucket: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("Testing error scenarios");

    // Test SSE-C with wrong key for download
    let test_key = "01234567890123456789012345678901";
    let wrong_key = "98765432109876543210987654321098";
    let test_key_b64 = base64::engine::general_purpose::STANDARD.encode(test_key);
    let wrong_key_b64 = base64::engine::general_purpose::STANDARD.encode(wrong_key);
    let test_key_md5 = format!("{:x}", md5::compute(test_key));
    let wrong_key_md5 = format!("{:x}", md5::compute(wrong_key));
    let test_data = b"Test data for error scenarios";
    let object_key = "test-error-object";

    // Upload with correct key (SSE-C)
    s3_client
        .put_object()
        .bucket(bucket)
        .key(object_key)
        .body(ByteStream::from(test_data.to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&test_key_b64)
        .sse_customer_key_md5(&test_key_md5)
        .send()
        .await?;

    // Try to download with wrong key - should fail
    let wrong_key_result = s3_client
        .get_object()
        .bucket(bucket)
        .key(object_key)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&wrong_key_b64)
        .sse_customer_key_md5(&wrong_key_md5)
        .send()
        .await;

    assert!(wrong_key_result.is_err(), "Download with wrong SSE-C key should fail");
    info!("✅ Correctly rejected download with wrong SSE-C key");

    info!("Error scenario tests completed successfully");
    Ok(())
}

/// Vault test environment management
pub struct VaultTestEnvironment {
    pub base_env: RustFSTestEnvironment,
    pub vault_process: Option<Child>,
}

impl VaultTestEnvironment {
    /// Create a new Vault test environment
    pub async fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let base_env = RustFSTestEnvironment::new().await?;

        Ok(Self {
            base_env,
            vault_process: None,
        })
    }

    /// Start Vault server in development mode
    pub async fn start_vault(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Starting Vault server in development mode");

        let vault_process = Command::new("vault")
            .args([
                "server",
                "-dev",
                "-dev-root-token-id",
                VAULT_TOKEN,
                "-dev-listen-address",
                VAULT_ADDRESS,
            ])
            .spawn()?;

        self.vault_process = Some(vault_process);

        // Wait for Vault to start
        self.wait_for_vault_ready().await?;

        Ok(())
    }

    async fn wait_for_vault_ready(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Waiting for Vault server to be ready...");

        for i in 0..30 {
            let port_check = TcpStream::connect(VAULT_ADDRESS).await.is_ok();
            if port_check {
                // Additional check by making a health request
                if let Ok(response) = reqwest::get(&format!("{VAULT_URL}/v1/sys/health")).await {
                    if response.status().is_success() {
                        info!("Vault server is ready after {} seconds", i);
                        return Ok(());
                    }
                }
            }

            if i == 29 {
                return Err("Vault server failed to become ready".into());
            }

            sleep(Duration::from_secs(1)).await;
        }

        Ok(())
    }

    /// Setup Vault transit secrets engine
    pub async fn setup_vault_transit(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = reqwest::Client::new();

        info!("Enabling Vault transit secrets engine");

        // Enable transit secrets engine
        let enable_response = client
            .post(format!("{VAULT_URL}/v1/sys/mounts/{VAULT_TRANSIT_PATH}"))
            .header("X-Vault-Token", VAULT_TOKEN)
            .json(&serde_json::json!({
                "type": "transit"
            }))
            .send()
            .await?;

        if !enable_response.status().is_success() && enable_response.status() != 400 {
            let error_text = enable_response.text().await?;
            return Err(format!("Failed to enable transit engine: {error_text}").into());
        }

        info!("Creating Vault encryption key");

        // Create encryption key
        let key_response = client
            .post(format!("{VAULT_URL}/v1/{VAULT_TRANSIT_PATH}/keys/{VAULT_KEY_NAME}"))
            .header("X-Vault-Token", VAULT_TOKEN)
            .json(&serde_json::json!({
                "type": "aes256-gcm96"
            }))
            .send()
            .await?;

        if !key_response.status().is_success() && key_response.status() != 400 {
            let error_text = key_response.text().await?;
            return Err(format!("Failed to create encryption key: {error_text}").into());
        }

        info!("Vault transit engine setup completed");
        Ok(())
    }

    /// Start RustFS server for Vault backend; dynamic configuration will be applied later.
    pub async fn start_rustfs_for_vault(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.base_env.start_rustfs_server(Vec::new()).await
    }

    /// Configure Vault KMS backend
    pub async fn configure_vault_kms(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let kms_config = serde_json::json!({
            "backend_type": "vault",
            "address": VAULT_URL,
            "auth_method": {
                "Token": {
                    "token": VAULT_TOKEN
                }
            },
            "mount_path": VAULT_TRANSIT_PATH,
            "kv_mount": "secret",
            "key_path_prefix": "rustfs/kms/keys",
            "default_key_id": VAULT_KEY_NAME,
            "skip_tls_verify": true
        })
        .to_string();

        configure_kms(&self.base_env.url, &kms_config, &self.base_env.access_key, &self.base_env.secret_key).await
    }
}

impl Drop for VaultTestEnvironment {
    fn drop(&mut self) {
        if let Some(mut process) = self.vault_process.take() {
            info!("Terminating Vault process");
            if let Err(e) = process.kill() {
                error!("Failed to kill Vault process: {}", e);
            } else {
                let _ = process.wait();
            }
        }
    }
}

/// Encryption types for multipart upload testing
#[derive(Debug, Clone)]
pub enum EncryptionType {
    None,
    SSES3,
    SSEKMS,
    SSEC { key: String, key_md5: String },
}

/// Configuration for multipart upload tests
#[derive(Debug, Clone)]
pub struct MultipartTestConfig {
    pub object_key: String,
    pub part_size: usize,
    pub total_parts: usize,
    pub encryption_type: EncryptionType,
}

impl MultipartTestConfig {
    pub fn new(object_key: impl Into<String>, part_size: usize, total_parts: usize, encryption_type: EncryptionType) -> Self {
        Self {
            object_key: object_key.into(),
            part_size,
            total_parts,
            encryption_type,
        }
    }

    pub fn total_size(&self) -> usize {
        self.part_size * self.total_parts
    }
}

/// Perform a comprehensive multipart upload test with the specified configuration
pub async fn test_multipart_upload_with_config(
    s3_client: &Client,
    bucket: &str,
    config: &MultipartTestConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let total_size = config.total_size();

    info!("🧪 Starting multipart upload test - {:?}", config.encryption_type);
    info!(
        "   Object: {}, parts: {}, part size: {} MB, total: {} MB",
        config.object_key,
        config.total_parts,
        config.part_size / (1024 * 1024),
        total_size / (1024 * 1024)
    );

    // Generate test data with patterns for verification
    let test_data: Vec<u8> = (0..total_size)
        .map(|i| {
            let part_num = i / config.part_size;
            let offset_in_part = i % config.part_size;
            ((part_num * 100 + offset_in_part / 1000) % 256) as u8
        })
        .collect();

    // Prepare encryption parameters
    let (sse_c_key_b64, sse_c_key_md5) = match &config.encryption_type {
        EncryptionType::SSEC { key, key_md5 } => {
            let key_b64 = base64::engine::general_purpose::STANDARD.encode(key);
            (Some(key_b64), Some(key_md5.clone()))
        }
        _ => (None, None),
    };

    // Step 1: Create multipart upload
    let mut create_request = s3_client.create_multipart_upload().bucket(bucket).key(&config.object_key);

    create_request = match &config.encryption_type {
        EncryptionType::None => create_request,
        EncryptionType::SSES3 => create_request.server_side_encryption(ServerSideEncryption::Aes256),
        EncryptionType::SSEKMS => create_request.server_side_encryption(ServerSideEncryption::AwsKms),
        EncryptionType::SSEC { .. } => create_request
            .sse_customer_algorithm("AES256")
            .sse_customer_key(sse_c_key_b64.as_ref().unwrap())
            .sse_customer_key_md5(sse_c_key_md5.as_ref().unwrap()),
    };

    let create_multipart_output = create_request.send().await?;
    let upload_id = create_multipart_output.upload_id().unwrap();
    info!("📋 Created multipart upload, ID: {}", upload_id);

    // Step 2: Upload parts
    let mut completed_parts = Vec::new();
    for part_number in 1..=config.total_parts {
        let start = (part_number - 1) * config.part_size;
        let end = std::cmp::min(start + config.part_size, total_size);
        let part_data = &test_data[start..end];

        info!("📤 Uploading part {} ({:.2} MB)", part_number, part_data.len() as f64 / (1024.0 * 1024.0));

        let mut upload_request = s3_client
            .upload_part()
            .bucket(bucket)
            .key(&config.object_key)
            .upload_id(upload_id)
            .part_number(part_number as i32)
            .body(ByteStream::from(part_data.to_vec()));

        // Add encryption headers for SSE-C parts
        if let EncryptionType::SSEC { .. } = &config.encryption_type {
            upload_request = upload_request
                .sse_customer_algorithm("AES256")
                .sse_customer_key(sse_c_key_b64.as_ref().unwrap())
                .sse_customer_key_md5(sse_c_key_md5.as_ref().unwrap());
        }

        let upload_part_output = upload_request.send().await?;
        let etag = upload_part_output.e_tag().unwrap().to_string();
        completed_parts.push(
            aws_sdk_s3::types::CompletedPart::builder()
                .part_number(part_number as i32)
                .e_tag(&etag)
                .build(),
        );

        debug!("Part {} uploaded with ETag {}", part_number, etag);
    }

    // Step 3: Complete multipart upload
    let completed_multipart_upload = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts))
        .build();

    info!("🔗 Completing multipart upload");
    let complete_output = s3_client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(&config.object_key)
        .upload_id(upload_id)
        .multipart_upload(completed_multipart_upload)
        .send()
        .await?;

    debug!("Multipart upload finalized with ETag {:?}", complete_output.e_tag());

    // Step 4: Download and verify
    info!("📥 Downloading object for verification");
    let mut get_request = s3_client.get_object().bucket(bucket).key(&config.object_key);

    // Add encryption headers for SSE-C GET
    if let EncryptionType::SSEC { .. } = &config.encryption_type {
        get_request = get_request
            .sse_customer_algorithm("AES256")
            .sse_customer_key(sse_c_key_b64.as_ref().unwrap())
            .sse_customer_key_md5(sse_c_key_md5.as_ref().unwrap());
    }

    let get_response = get_request.send().await?;

    // Verify encryption headers
    match &config.encryption_type {
        EncryptionType::None => {
            assert_eq!(get_response.server_side_encryption(), None);
        }
        EncryptionType::SSES3 => {
            assert_eq!(get_response.server_side_encryption(), Some(&ServerSideEncryption::Aes256));
        }
        EncryptionType::SSEKMS => {
            assert_eq!(get_response.server_side_encryption(), Some(&ServerSideEncryption::AwsKms));
        }
        EncryptionType::SSEC { .. } => {
            assert_eq!(get_response.sse_customer_algorithm(), Some("AES256"));
        }
    }

    // Verify data integrity
    let downloaded_data = get_response.body.collect().await?.into_bytes();
    assert_eq!(downloaded_data.len(), total_size);
    assert_eq!(&downloaded_data[..], &test_data[..]);

    info!("✅ Multipart upload test passed - {:?}", config.encryption_type);
    Ok(())
}

/// Create a standard SSE-C encryption configuration for testing
pub fn create_sse_c_config() -> EncryptionType {
    let key = "01234567890123456789012345678901"; // 32-byte key
    let key_md5 = format!("{:x}", md5::compute(key));
    EncryptionType::SSEC {
        key: key.to_string(),
        key_md5,
    }
}

/// Test all encryption types for multipart uploads
pub async fn test_all_multipart_encryption_types(
    s3_client: &Client,
    bucket: &str,
    base_object_key: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("🧪 Testing multipart uploads for every encryption type");

    let part_size = 5 * 1024 * 1024; // 5MB per part
    let total_parts = 2;

    // Test configurations for all encryption types
    let test_configs = vec![
        MultipartTestConfig::new(format!("{base_object_key}-no-encryption"), part_size, total_parts, EncryptionType::None),
        MultipartTestConfig::new(format!("{base_object_key}-sse-s3"), part_size, total_parts, EncryptionType::SSES3),
        MultipartTestConfig::new(format!("{base_object_key}-sse-kms"), part_size, total_parts, EncryptionType::SSEKMS),
        MultipartTestConfig::new(format!("{base_object_key}-sse-c"), part_size, total_parts, create_sse_c_config()),
    ];

    // Run tests for each encryption type
    for config in test_configs {
        test_multipart_upload_with_config(s3_client, bucket, &config).await?;
    }

    info!("✅ Multipart uploads succeeded for every encryption type");
    Ok(())
}

/// Local KMS test environment management
pub struct LocalKMSTestEnvironment {
    pub base_env: RustFSTestEnvironment,
    pub kms_keys_dir: String,
}

impl LocalKMSTestEnvironment {
    /// Create a new Local KMS test environment
    pub async fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let base_env = RustFSTestEnvironment::new().await?;
        let kms_keys_dir = format!("{}/kms-keys", base_env.temp_dir);
        fs::create_dir_all(&kms_keys_dir).await?;

        Ok(Self { base_env, kms_keys_dir })
    }

    /// Start RustFS server configured for Local KMS backend with a default key
    pub async fn start_rustfs_for_local_kms(&mut self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        // Create a default key first
        let default_key_id = "rustfs-e2e-test-default-key";
        create_key_with_specific_id(&self.kms_keys_dir, default_key_id).await?;

        let extra_args = vec![
            "--kms-enable",
            "--kms-backend",
            "local",
            "--kms-key-dir",
            &self.kms_keys_dir,
            "--kms-default-key-id",
            default_key_id,
        ];

        self.base_env.start_rustfs_server(extra_args).await?;
        Ok(default_key_id.to_string())
    }

    /// Configure Local KMS backend with a predefined default key
    pub async fn configure_local_kms(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        // Use a fixed, predictable default key ID
        let default_key_id = "rustfs-e2e-test-default-key";

        // Create the default key file first using our manual method
        create_key_with_specific_id(&self.kms_keys_dir, default_key_id).await?;

        // Configure KMS with the default key in one step
        let kms_config = serde_json::json!({
            "backend_type": "local",
            "key_dir": self.kms_keys_dir,
            "file_permissions": 0o600,
            "default_key_id": default_key_id
        })
        .to_string();

        configure_kms(&self.base_env.url, &kms_config, &self.base_env.access_key, &self.base_env.secret_key).await?;

        Ok(default_key_id.to_string())
    }
}
