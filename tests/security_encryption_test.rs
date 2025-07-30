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

//! Security tests for object encryption functionality
//!
//! These tests verify the security properties of the encryption implementation,
//! including key management, data protection, and attack resistance.

use rustfs_kms::{
    BucketEncryptionConfig, BucketEncryptionManager, KmsConfig, KmsManager,
    LocalKmsClient, ObjectEncryptionService, EncryptionAlgorithm,
    SecretVec, SecretKey,
};
use rustfs_common::error::Result;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use tokio::io::{AsyncReadExt};
use tokio::test;

/// Test data for security tests
const SENSITIVE_DATA: &[u8] = b"This is highly sensitive data that must be protected!";
const TEST_BUCKET: &str = "security-test-bucket";
const TEST_KEY_ID: &str = "security-test-key-123";

/// Setup test KMS infrastructure for security tests
async fn setup_security_test_kms() -> Result<(Arc<KmsManager>, Arc<ObjectEncryptionService>)> {
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

#[test]
async fn test_data_confidentiality() {
    let (_, encryption_service) = setup_security_test_kms().await.unwrap();
    
    let data = Cursor::new(SENSITIVE_DATA.to_vec());
    let data_size = SENSITIVE_DATA.len() as u64;
    
    // Encrypt sensitive data
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
    
    // Verify encrypted data doesn't contain original data
    assert_ne!(encrypted_data, SENSITIVE_DATA);
    
    // Verify no plaintext patterns are visible in encrypted data
    let encrypted_str = String::from_utf8_lossy(&encrypted_data);
    assert!(!encrypted_str.contains("sensitive"));
    assert!(!encrypted_str.contains("protected"));
    assert!(!encrypted_str.contains("highly"));
    
    // Verify encrypted data appears random (basic entropy check)
    let mut byte_counts = [0u32; 256];
    for &byte in &encrypted_data {
        byte_counts[byte as usize] += 1;
    }
    
    // Check that no single byte value dominates (basic randomness test)
    let max_count = *byte_counts.iter().max().unwrap();
    let total_bytes = encrypted_data.len() as u32;
    let max_expected_ratio = 0.1; // No byte should appear more than 10% of the time
    
    assert!((max_count as f64 / total_bytes as f64) < max_expected_ratio);
}

#[test]
async fn test_key_isolation() {
    let (_, encryption_service) = setup_security_test_kms().await.unwrap();
    
    let data = Cursor::new(SENSITIVE_DATA.to_vec());
    let data_size = SENSITIVE_DATA.len() as u64;
    
    // Encrypt with first key
    let encrypt_result1 = encryption_service
        .encrypt_object(
            data,
            data_size,
            EncryptionAlgorithm::AES256,
            Some("key-1"),
            None,
        )
        .await
        .unwrap();
    
    let data2 = Cursor::new(SENSITIVE_DATA.to_vec());
    
    // Encrypt same data with different key
    let encrypt_result2 = encryption_service
        .encrypt_object(
            data2,
            data_size,
            EncryptionAlgorithm::AES256,
            Some("key-2"),
            None,
        )
        .await
        .unwrap();
    
    // Read both encrypted results
    let mut encrypted_data1 = Vec::new();
    let mut reader1 = encrypt_result1.reader;
    reader1.read_to_end(&mut encrypted_data1).await.unwrap();
    
    let mut encrypted_data2 = Vec::new();
    let mut reader2 = encrypt_result2.reader;
    reader2.read_to_end(&mut encrypted_data2).await.unwrap();
    
    // Verify different keys produce different encrypted data
    assert_ne!(encrypted_data1, encrypted_data2);
    
    // Verify different encrypted data keys
    assert_ne!(
        encrypt_result1.metadata.encrypted_data_key,
        encrypt_result2.metadata.encrypted_data_key
    );
    
    // Verify different KMS key IDs
    assert_ne!(
        encrypt_result1.metadata.kms_key_id,
        encrypt_result2.metadata.kms_key_id
    );
}

#[test]
async fn test_iv_uniqueness() {
    let (_, encryption_service) = setup_security_test_kms().await.unwrap();
    
    let mut ivs = Vec::new();
    
    // Encrypt same data multiple times
    for _ in 0..10 {
        let data = Cursor::new(SENSITIVE_DATA.to_vec());
        let data_size = SENSITIVE_DATA.len() as u64;
        
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
        
        if let Some(iv) = encrypt_result.metadata.iv {
            ivs.push(iv);
        }
    }
    
    // Verify all IVs are unique
    for i in 0..ivs.len() {
        for j in (i + 1)..ivs.len() {
            assert_ne!(ivs[i], ivs[j], "IVs must be unique for each encryption");
        }
    }
}

#[test]
async fn test_algorithm_isolation() {
    let (_, encryption_service) = setup_security_test_kms().await.unwrap();
    
    let data1 = Cursor::new(SENSITIVE_DATA.to_vec());
    let data2 = Cursor::new(SENSITIVE_DATA.to_vec());
    let data_size = SENSITIVE_DATA.len() as u64;
    
    // Encrypt with AES256
    let aes_result = encryption_service
        .encrypt_object(
            data1,
            data_size,
            EncryptionAlgorithm::AES256,
            Some(TEST_KEY_ID),
            None,
        )
        .await
        .unwrap();
    
    // Encrypt with ChaCha20Poly1305
    let chacha_result = encryption_service
        .encrypt_object(
            data2,
            data_size,
            EncryptionAlgorithm::ChaCha20Poly1305,
            Some(TEST_KEY_ID),
            None,
        )
        .await
        .unwrap();
    
    // Read encrypted data
    let mut aes_data = Vec::new();
    let mut aes_reader = aes_result.reader;
    aes_reader.read_to_end(&mut aes_data).await.unwrap();
    
    let mut chacha_data = Vec::new();
    let mut chacha_reader = chacha_result.reader;
    chacha_reader.read_to_end(&mut chacha_data).await.unwrap();
    
    // Verify different algorithms produce different results
    assert_ne!(aes_data, chacha_data);
    
    // Verify algorithm metadata is correct
    assert_eq!(aes_result.metadata.algorithm, EncryptionAlgorithm::AES256);
    assert_eq!(chacha_result.metadata.algorithm, EncryptionAlgorithm::ChaCha20Poly1305);
}

#[test]
async fn test_tamper_detection() {
    let (_, encryption_service) = setup_security_test_kms().await.unwrap();
    
    let data = Cursor::new(SENSITIVE_DATA.to_vec());
    let data_size = SENSITIVE_DATA.len() as u64;
    
    // Encrypt data
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
    
    // Tamper with encrypted data
    let mut tampered_data = encrypted_data.clone();
    if !tampered_data.is_empty() {
        tampered_data[0] ^= 0x01; // Flip one bit
    }
    
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
    
    // Try to decrypt tampered data
    let tampered_cursor = Cursor::new(tampered_data);
    let decrypt_result = encryption_service
        .decrypt_object(tampered_cursor, &metadata)
        .await;
    
    // Decryption should fail due to authentication tag mismatch
    assert!(decrypt_result.is_err());
}

#[test]
async fn test_metadata_tampering() {
    let (_, encryption_service) = setup_security_test_kms().await.unwrap();
    
    let data = Cursor::new(SENSITIVE_DATA.to_vec());
    let data_size = SENSITIVE_DATA.len() as u64;
    
    // Encrypt data
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
    
    // Prepare tampered metadata
    let mut tampered_metadata = HashMap::new();
    tampered_metadata.insert(
        "encrypted_data_key".to_string(),
        "tampered-key".to_string(), // Invalid key
    );
    tampered_metadata.insert(
        "algorithm".to_string(),
        encrypt_result.metadata.algorithm.to_string(),
    );
    if let Some(kms_key_id) = encrypt_result.metadata.kms_key_id {
        tampered_metadata.insert("kms_key_id".to_string(), kms_key_id);
    }
    if let Some(iv) = encrypt_result.metadata.iv {
        tampered_metadata.insert("iv".to_string(), base64::encode(iv));
    }
    if let Some(tag) = encrypt_result.metadata.tag {
        tampered_metadata.insert("tag".to_string(), base64::encode(tag));
    }
    
    // Try to decrypt with tampered metadata
    let encrypted_cursor = Cursor::new(encrypted_data);
    let decrypt_result = encryption_service
        .decrypt_object(encrypted_cursor, &tampered_metadata)
        .await;
    
    // Decryption should fail due to invalid metadata
    assert!(decrypt_result.is_err());
}

#[test]
async fn test_secret_memory_protection() {
    // Test SecretVec functionality
    let sensitive_data = b"super secret key material";
    let secret = SecretVec::new(sensitive_data.to_vec());
    
    // Verify we can access the data when needed
    assert_eq!(secret.expose_secret(), sensitive_data);
    
    // Test SecretKey functionality
    let key_material = [0u8; 32]; // 256-bit key
    let secret_key = SecretKey::new(key_material);
    
    // Verify key access
    assert_eq!(secret_key.expose_secret(), &key_material);
    
    // Test that secrets are properly zeroized when dropped
    // Note: This is more of a compile-time guarantee with zeroize crate
    drop(secret);
    drop(secret_key);
}

#[test]
async fn test_encryption_context_isolation() {
    let (_, encryption_service) = setup_security_test_kms().await.unwrap();
    
    let data1 = Cursor::new(SENSITIVE_DATA.to_vec());
    let data2 = Cursor::new(SENSITIVE_DATA.to_vec());
    let data_size = SENSITIVE_DATA.len() as u64;
    
    // Create different encryption contexts
    let mut context1 = HashMap::new();
    context1.insert("department".to_string(), "finance".to_string());
    context1.insert("classification".to_string(), "confidential".to_string());
    
    let mut context2 = HashMap::new();
    context2.insert("department".to_string(), "hr".to_string());
    context2.insert("classification".to_string(), "restricted".to_string());
    
    // Encrypt with different contexts
    let result1 = encryption_service
        .encrypt_object(
            data1,
            data_size,
            EncryptionAlgorithm::AES256,
            Some(TEST_KEY_ID),
            Some(context1),
        )
        .await
        .unwrap();
    
    let result2 = encryption_service
        .encrypt_object(
            data2,
            data_size,
            EncryptionAlgorithm::AES256,
            Some(TEST_KEY_ID),
            Some(context2),
        )
        .await
        .unwrap();
    
    // Read encrypted data
    let mut encrypted_data1 = Vec::new();
    let mut reader1 = result1.reader;
    reader1.read_to_end(&mut encrypted_data1).await.unwrap();
    
    let mut encrypted_data2 = Vec::new();
    let mut reader2 = result2.reader;
    reader2.read_to_end(&mut encrypted_data2).await.unwrap();
    
    // Verify different contexts produce different encrypted data
    assert_ne!(encrypted_data1, encrypted_data2);
    
    // Verify encryption contexts are preserved in metadata
    assert_ne!(
        result1.metadata.encryption_context,
        result2.metadata.encryption_context
    );
}

#[test]
async fn test_side_channel_resistance() {
    let (_, encryption_service) = setup_security_test_kms().await.unwrap();
    
    let mut timings = Vec::new();
    
    // Measure encryption timing for same-size inputs
    for _ in 0..10 {
        let data = Cursor::new(SENSITIVE_DATA.to_vec());
        let data_size = SENSITIVE_DATA.len() as u64;
        
        let start = std::time::Instant::now();
        
        let _result = encryption_service
            .encrypt_object(
                data,
                data_size,
                EncryptionAlgorithm::AES256,
                Some(TEST_KEY_ID),
                None,
            )
            .await
            .unwrap();
        
        let duration = start.elapsed();
        timings.push(duration.as_nanos());
    }
    
    // Calculate timing variance
    let mean = timings.iter().sum::<u128>() as f64 / timings.len() as f64;
    let variance = timings
        .iter()
        .map(|&x| {
            let diff = x as f64 - mean;
            diff * diff
        })
        .sum::<f64>() / timings.len() as f64;
    
    let std_dev = variance.sqrt();
    let coefficient_of_variation = std_dev / mean;
    
    // Timing should be relatively consistent (CV < 50%)
    // This is a basic check - real side-channel analysis would be more sophisticated
    assert!(coefficient_of_variation < 0.5, 
        "Timing variance too high: CV = {:.2}%", coefficient_of_variation * 100.0);
}

#[test]
async fn test_key_derivation_security() {
    let (kms_manager, _) = setup_security_test_kms().await.unwrap();
    
    let mut derived_keys = Vec::new();
    
    // Generate multiple data keys
    for _ in 0..5 {
        let key_spec = "AES_256".to_string();
        let response = kms_manager
            .generate_data_key(TEST_KEY_ID, &key_spec, None)
            .await
            .unwrap();
        
        derived_keys.push(response.plaintext_key);
    }
    
    // Verify all derived keys are unique
    for i in 0..derived_keys.len() {
        for j in (i + 1)..derived_keys.len() {
            assert_ne!(
                derived_keys[i].expose_secret(),
                derived_keys[j].expose_secret(),
                "Derived keys must be unique"
            );
        }
    }
    
    // Verify keys have proper entropy (basic check)
    for key in &derived_keys {
        let key_bytes = key.expose_secret();
        
        // Check key is not all zeros
        assert!(!key_bytes.iter().all(|&b| b == 0));
        
        // Check key is not all ones
        assert!(!key_bytes.iter().all(|&b| b == 0xFF));
        
        // Basic entropy check - no byte value should dominate
        let mut byte_counts = [0u32; 256];
        for &byte in key_bytes {
            byte_counts[byte as usize] += 1;
        }
        
        let max_count = *byte_counts.iter().max().unwrap();
        let total_bytes = key_bytes.len() as u32;
        
        // No single byte value should appear more than 25% of the time
        assert!((max_count as f64 / total_bytes as f64) < 0.25);
    }
}