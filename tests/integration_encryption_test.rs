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

//! Integration tests for object encryption functionality
//!
//! These tests verify the complete encryption workflow from S3 API
//! through to storage layer integration.

use rustfs_kms::{
    BucketEncryptionConfig, BucketEncryptionManager, KmsConfig, KmsManager,
    LocalKmsClient, ObjectEncryptionService, EncryptionAlgorithm,
    KmsMonitor, MonitoringConfig, OperationTimer, KmsOperation,
};
use rustfs_common::error::Result;
use serde_json::json;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::test;

/// Test data for encryption tests
const TEST_DATA: &[u8] = b"Hello, World! This is test data for encryption.";
const TEST_BUCKET: &str = "test-bucket";
const TEST_OBJECT: &str = "test-object";
const TEST_KEY_ID: &str = "test-key-123";

/// Setup test KMS infrastructure
async fn setup_test_kms() -> Result<(Arc<KmsManager>, Arc<ObjectEncryptionService>)> {
    let kms_config = KmsConfig {
        provider: "local".to_string(),
        endpoint: None,
        region: Some("us-east-1".to_string()),
        access_key_id: None,
        secret_access_key: None,
        timeout_secs: 30,
        max_retries: 3,
    };
    
    let local_client = LocalKmsClient::new();
    let kms_manager = Arc::new(KmsManager::new(kms_config, Box::new(local_client)));
    
    let encryption_service = Arc::new(ObjectEncryptionService::new(kms_manager.clone()));
    
    Ok((kms_manager, encryption_service))
}

/// Setup test bucket encryption configuration
async fn setup_test_bucket_config() -> Result<Arc<BucketEncryptionManager>> {
    let (kms_manager, _) = setup_test_kms().await?;
    let bucket_manager = Arc::new(BucketEncryptionManager::new(kms_manager));
    
    // Configure bucket encryption
    let config = BucketEncryptionConfig {
        algorithm: EncryptionAlgorithm::AES256,
        kms_key_id: Some(TEST_KEY_ID.to_string()),
        bucket_key_enabled: true,
    };
    
    bucket_manager.set_bucket_encryption(TEST_BUCKET, config).await?;
    
    Ok(bucket_manager)
}

#[test]
async fn test_end_to_end_encryption_workflow() {
    let (kms_manager, encryption_service) = setup_test_kms().await.unwrap();
    let bucket_manager = setup_test_bucket_config().await.unwrap();
    
    // Test data
    let data = Cursor::new(TEST_DATA.to_vec());
    let data_size = TEST_DATA.len() as u64;
    
    // Get bucket encryption config
    let bucket_config = bucket_manager
        .get_bucket_encryption(TEST_BUCKET)
        .await
        .unwrap()
        .unwrap();
    
    // Encrypt object
    let encrypt_result = encryption_service
        .encrypt_object(
            data,
            data_size,
            bucket_config.algorithm,
            bucket_config.kms_key_id.as_deref(),
            None,
        )
        .await
        .unwrap();
    
    // Read encrypted data
    let mut encrypted_data = Vec::new();
    let mut reader = encrypt_result.reader;
    reader.read_to_end(&mut encrypted_data).await.unwrap();
    
    // Verify encrypted data is different from original
    assert_ne!(encrypted_data, TEST_DATA);
    assert!(!encrypted_data.is_empty());
    
    // Prepare metadata for decryption
    let mut metadata = HashMap::new();
    metadata.insert(
        "encrypted_data_key".to_string(),
        encrypt_result.metadata.encrypted_data_key,
    );
    metadata.insert(
        "algorithm".to_string(),
        encrypt_result.metadata.algorithm.to_string(),
    );
    if let Some(kms_key_id) = encrypt_result.metadata.kms_key_id {
        metadata.insert("kms_key_id".to_string(), kms_key_id);
    }
    if let Some(iv) = encrypt_result.metadata.iv {
        metadata.insert("iv".to_string(), base64::encode(iv));
    }
    if let Some(tag) = encrypt_result.metadata.tag {
        metadata.insert("tag".to_string(), base64::encode(tag));
    }
    
    // Decrypt object
    let encrypted_cursor = Cursor::new(encrypted_data);
    let decrypt_result = encryption_service
        .decrypt_object(encrypted_cursor, &metadata)
        .await
        .unwrap();
    
    // Read decrypted data
    let mut decrypted_data = Vec::new();
    let mut decrypt_reader = decrypt_result;
    decrypt_reader.read_to_end(&mut decrypted_data).await.unwrap();
    
    // Verify decrypted data matches original
    assert_eq!(decrypted_data, TEST_DATA);
}

#[test]
async fn test_bucket_encryption_management() {
    let bucket_manager = setup_test_bucket_config().await.unwrap();
    
    // Test getting bucket encryption
    let config = bucket_manager
        .get_bucket_encryption(TEST_BUCKET)
        .await
        .unwrap()
        .unwrap();
    
    assert_eq!(config.algorithm, EncryptionAlgorithm::AES256);
    assert_eq!(config.kms_key_id, Some(TEST_KEY_ID.to_string()));
    assert!(config.bucket_key_enabled);
    
    // Test updating bucket encryption
    let new_config = BucketEncryptionConfig {
        algorithm: EncryptionAlgorithm::ChaCha20Poly1305,
        kms_key_id: Some("new-key-456".to_string()),
        bucket_key_enabled: false,
    };
    
    bucket_manager
        .set_bucket_encryption(TEST_BUCKET, new_config.clone())
        .await
        .unwrap();
    
    let updated_config = bucket_manager
        .get_bucket_encryption(TEST_BUCKET)
        .await
        .unwrap()
        .unwrap();
    
    assert_eq!(updated_config.algorithm, EncryptionAlgorithm::ChaCha20Poly1305);
    assert_eq!(updated_config.kms_key_id, Some("new-key-456".to_string()));
    assert!(!updated_config.bucket_key_enabled);
    
    // Test deleting bucket encryption
    bucket_manager
        .delete_bucket_encryption(TEST_BUCKET)
        .await
        .unwrap();
    
    let deleted_config = bucket_manager
        .get_bucket_encryption(TEST_BUCKET)
        .await
        .unwrap();
    
    assert!(deleted_config.is_none());
}

#[test]
async fn test_multiple_algorithm_support() {
    let (_, encryption_service) = setup_test_kms().await.unwrap();
    
    let algorithms = vec![
        EncryptionAlgorithm::AES256,
        EncryptionAlgorithm::ChaCha20Poly1305,
    ];
    
    for algorithm in algorithms {
        let data = Cursor::new(TEST_DATA.to_vec());
        let data_size = TEST_DATA.len() as u64;
        
        // Encrypt with specific algorithm
        let encrypt_result = encryption_service
            .encrypt_object(
                data,
                data_size,
                algorithm,
                Some(TEST_KEY_ID),
                None,
            )
            .await
            .unwrap();
        
        // Verify algorithm in metadata
        assert_eq!(encrypt_result.metadata.algorithm, algorithm);
        
        // Read encrypted data
        let mut encrypted_data = Vec::new();
        let mut reader = encrypt_result.reader;
        reader.read_to_end(&mut encrypted_data).await.unwrap();
        
        // Prepare metadata for decryption
        let mut metadata = HashMap::new();
        metadata.insert(
            "encrypted_data_key".to_string(),
            encrypt_result.metadata.encrypted_data_key,
        );
        metadata.insert(
            "algorithm".to_string(),
            encrypt_result.metadata.algorithm.to_string(),
        );
        if let Some(kms_key_id) = encrypt_result.metadata.kms_key_id {
            metadata.insert("kms_key_id".to_string(), kms_key_id);
        }
        if let Some(iv) = encrypt_result.metadata.iv {
            metadata.insert("iv".to_string(), base64::encode(iv));
        }
        if let Some(tag) = encrypt_result.metadata.tag {
            metadata.insert("tag".to_string(), base64::encode(tag));
        }
        
        // Decrypt and verify
        let encrypted_cursor = Cursor::new(encrypted_data);
        let decrypt_result = encryption_service
            .decrypt_object(encrypted_cursor, &metadata)
            .await
            .unwrap();
        
        let mut decrypted_data = Vec::new();
        let mut decrypt_reader = decrypt_result;
        decrypt_reader.read_to_end(&mut decrypted_data).await.unwrap();
        
        assert_eq!(decrypted_data, TEST_DATA);
    }
}

#[test]
async fn test_large_object_encryption() {
    let (_, encryption_service) = setup_test_kms().await.unwrap();
    
    // Create large test data (1MB)
    let large_data: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();
    let data = Cursor::new(large_data.clone());
    let data_size = large_data.len() as u64;
    
    // Encrypt large object
    let encrypt_result = encryption_service
        .encrypt_object(
            data,
            data_size,
            EncryptionAlgorithm::AES256,
            Some(TEST_KEY_ID),
            None,
        )
        .await
        .unwrap();
    
    // Read encrypted data
    let mut encrypted_data = Vec::new();
    let mut reader = encrypt_result.reader;
    reader.read_to_end(&mut encrypted_data).await.unwrap();
    
    // Verify encrypted data size is reasonable
    assert!(encrypted_data.len() >= large_data.len());
    assert_ne!(encrypted_data, large_data);
    
    // Prepare metadata for decryption
    let mut metadata = HashMap::new();
    metadata.insert(
        "encrypted_data_key".to_string(),
        encrypt_result.metadata.encrypted_data_key,
    );
    metadata.insert(
        "algorithm".to_string(),
        encrypt_result.metadata.algorithm.to_string(),
    );
    if let Some(kms_key_id) = encrypt_result.metadata.kms_key_id {
        metadata.insert("kms_key_id".to_string(), kms_key_id);
    }
    if let Some(iv) = encrypt_result.metadata.iv {
        metadata.insert("iv".to_string(), base64::encode(iv));
    }
    if let Some(tag) = encrypt_result.metadata.tag {
        metadata.insert("tag".to_string(), base64::encode(tag));
    }
    
    // Decrypt large object
    let encrypted_cursor = Cursor::new(encrypted_data);
    let decrypt_result = encryption_service
        .decrypt_object(encrypted_cursor, &metadata)
        .await
        .unwrap();
    
    // Read decrypted data
    let mut decrypted_data = Vec::new();
    let mut decrypt_reader = decrypt_result;
    decrypt_reader.read_to_end(&mut decrypted_data).await.unwrap();
    
    // Verify decrypted data matches original
    assert_eq!(decrypted_data, large_data);
}

#[test]
async fn test_monitoring_integration() {
    let config = MonitoringConfig::default();
    let monitor = Arc::new(KmsMonitor::new(config));
    
    // Test operation timing
    let timer = OperationTimer::start(KmsOperation::Encrypt, monitor.clone())
        .with_key_id(TEST_KEY_ID.to_string())
        .with_principal("test-user".to_string());
    
    // Simulate some work
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    
    timer.complete_success().await;
    
    // Verify metrics were recorded
    let metrics = monitor.get_operation_metrics(KmsOperation::Encrypt).await;
    assert!(metrics.is_some());
    
    let metrics = metrics.unwrap();
    assert_eq!(metrics.total_count, 1);
    assert_eq!(metrics.success_count, 1);
    assert!(metrics.avg_duration_ms > 0.0);
    
    // Verify audit log
    let audit_log = monitor.get_audit_log(None).await;
    assert_eq!(audit_log.len(), 1);
    assert_eq!(audit_log[0].operation, KmsOperation::Encrypt);
    assert_eq!(audit_log[0].key_id, Some(TEST_KEY_ID.to_string()));
    assert_eq!(audit_log[0].principal, Some("test-user".to_string()));
    
    // Generate monitoring report
    let report = monitor.generate_report().await;
    assert_eq!(report.total_operations, 1);
    assert_eq!(report.total_successes, 1);
    assert_eq!(report.overall_success_rate, 100.0);
}

#[test]
async fn test_error_handling() {
    let (_, encryption_service) = setup_test_kms().await.unwrap();
    
    // Test decryption with invalid metadata
    let data = Cursor::new(TEST_DATA.to_vec());
    let mut invalid_metadata = HashMap::new();
    invalid_metadata.insert("encrypted_data_key".to_string(), "invalid-key".to_string());
    invalid_metadata.insert("algorithm".to_string(), "AES256".to_string());
    
    let result = encryption_service
        .decrypt_object(data, &invalid_metadata)
        .await;
    
    assert!(result.is_err());
    
    // Test encryption with invalid algorithm (this should be caught at compile time,
    // but we can test with string parsing)
    let data = Cursor::new(TEST_DATA.to_vec());
    let data_size = TEST_DATA.len() as u64;
    
    // This should work fine with valid algorithm
    let result = encryption_service
        .encrypt_object(
            data,
            data_size,
            EncryptionAlgorithm::AES256,
            Some(TEST_KEY_ID),
            None,
        )
        .await;
    
    assert!(result.is_ok());
}

#[test]
async fn test_concurrent_operations() {
    let (_, encryption_service) = setup_test_kms().await.unwrap();
    let encryption_service = Arc::new(encryption_service);
    
    let mut handles = Vec::new();
    
    // Start multiple concurrent encryption operations
    for i in 0..10 {
        let service = encryption_service.clone();
        let test_data = format!("Test data {}", i);
        
        let handle = tokio::spawn(async move {
            let data = Cursor::new(test_data.as_bytes().to_vec());
            let data_size = test_data.len() as u64;
            
            let encrypt_result = service
                .encrypt_object(
                    data,
                    data_size,
                    EncryptionAlgorithm::AES256,
                    Some(TEST_KEY_ID),
                    None,
                )
                .await
                .unwrap();
            
            // Read encrypted data
            let mut encrypted_data = Vec::new();
            let mut reader = encrypt_result.reader;
            reader.read_to_end(&mut encrypted_data).await.unwrap();
            
            (encrypted_data, encrypt_result.metadata)
        });
        
        handles.push(handle);
    }
    
    // Wait for all operations to complete
    let results = futures::future::join_all(handles).await;
    
    // Verify all operations succeeded
    assert_eq!(results.len(), 10);
    for result in results {
        assert!(result.is_ok());
        let (encrypted_data, _metadata) = result.unwrap();
        assert!(!encrypted_data.is_empty());
    }
}