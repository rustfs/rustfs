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

#[cfg(test)]
mod kms_tests {
    use crate::config::{VaultAuthMethod, VaultConfig};
    use crate::manager::KmsClient;
    use crate::types::*;
    use crate::vault_client::VaultKmsClient;

    use std::collections::HashMap;

    use url::Url;
    use uuid::Uuid;

    /// Create a test Vault configuration for local testing
    fn create_test_vault_config() -> VaultConfig {
        let token = std::env::var("VAULT_TOKEN").expect("VAULT_TOKEN environment variable must be set for testing");

        VaultConfig {
            address: Url::parse("http://localhost:8200").expect("Valid URL should parse successfully"),
            auth_method: VaultAuthMethod::Token { token },
            namespace: None,
            mount_path: "transit".to_string(),
            tls_config: None,
            headers: HashMap::new(),
        }
    }

    #[tokio::test]
    #[ignore = "Requires a running Vault instance"]
    async fn test_vault_client_creation() {
        let config = create_test_vault_config();
        let result = VaultKmsClient::new(config).await;

        match result {
            Ok(_client) => {
                println!("✓ Vault client created successfully");
            }
            Err(e) => {
                println!("✗ Failed to create Vault client: {e}");
                panic!("Vault client creation failed: {e}");
            }
        }
    }

    #[tokio::test]
    #[ignore = "Requires a running Vault instance"]
    async fn test_vault_key_operations() {
        let config = create_test_vault_config();
        let client = VaultKmsClient::new(config).await.expect("Failed to create Vault client");

        let key_id = format!("test-key-{}", Uuid::new_v4());

        // Test key creation
        println!("Creating key: {key_id}");
        let master_key = client
            .create_key(&key_id, "aes256-gcm96", None)
            .await
            .expect("Failed to create key");

        assert_eq!(master_key.key_id, key_id);
        println!("✓ Key created successfully: {}", master_key.key_id);

        // Skip key listing test as KV engine doesn't support native listing
        println!("Skipping key listing test (not supported by KV engine)...");
        // In a real implementation, you might maintain a separate index of keys
        // For this test, we'll just verify the key directly
        let key_info = client.describe_key(&key_id, None).await.expect("Failed to describe key");
        assert_eq!(key_info.key_id, key_id, "Key ID mismatch");
        println!("✓ Key verified via direct lookup");

        // Test key info retrieval
        println!("Getting key info...");
        let key_info = client.describe_key(&key_id, None).await.expect("Failed to get key info");

        assert_eq!(key_info.key_id, key_id);
        println!("✓ Key info retrieved: status={:?}", key_info.status);
    }

    #[tokio::test]
    #[ignore = "Requires a running Vault instance"]
    async fn test_vault_encrypt_decrypt() {
        let config = create_test_vault_config();
        let client = VaultKmsClient::new(config).await.expect("Failed to create Vault client");

        let key_id = format!("test-encrypt-key-{}", Uuid::new_v4());
        let plaintext = b"Hello, Vault KMS!";
        let context = HashMap::from([
            ("purpose".to_string(), "testing".to_string()),
            ("user".to_string(), "test-user".to_string()),
        ]);

        // Create a key for encryption
        println!("Creating encryption key: {key_id}");
        client
            .create_key(&key_id, "aes256-gcm96", None)
            .await
            .expect("Failed to create encryption key");

        // Test encryption
        println!("Encrypting data...");
        let encrypt_request = EncryptRequest {
            key_id: key_id.clone(),
            plaintext: plaintext.to_vec(),
            encryption_context: context.clone(),
            grant_tokens: vec![],
        };

        let encrypt_response = client.encrypt(&encrypt_request, None).await.expect("Failed to encrypt data");

        assert_eq!(encrypt_response.key_id, key_id);
        assert!(!encrypt_response.ciphertext.is_empty());
        println!("✓ Data encrypted successfully, ciphertext length: {}", encrypt_response.ciphertext.len());

        // Test decryption
        println!("Decrypting data...");
        let decrypt_request = DecryptRequest {
            ciphertext: encrypt_response.ciphertext,
            encryption_context: context,
            grant_tokens: vec![],
        };

        let decrypt_response = client.decrypt(&decrypt_request, None).await.expect("Failed to decrypt data");

        assert_eq!(decrypt_response, plaintext.to_vec());
        println!(
            "✓ Data decrypted successfully, matches original: {}",
            String::from_utf8_lossy(&decrypt_response)
        );
    }

    #[tokio::test]
    #[ignore = "Requires a running Vault instance"]
    async fn test_vault_data_key_generation() {
        let config = create_test_vault_config();
        let client = VaultKmsClient::new(config).await.expect("Failed to create Vault client");

        let key_id = format!("test-datakey-{}", Uuid::new_v4());
        let context = HashMap::from([("application".to_string(), "test-app".to_string())]);

        // Create a master key
        println!("Creating master key: {key_id}");
        client
            .create_key(&key_id, "aes256-gcm96", None)
            .await
            .expect("Failed to create master key");

        // Test data key generation
        println!("Generating data key...");
        let generate_request = GenerateKeyRequest {
            master_key_id: key_id.clone(),
            key_spec: "AES_256".to_string(),
            key_length: Some(32),
            encryption_context: context,
            grant_tokens: vec![],
        };

        let data_key = client
            .generate_data_key(&generate_request, None)
            .await
            .expect("Failed to generate data key");

        assert_eq!(data_key.key_id, key_id);
        assert!(data_key.plaintext.is_some());
        assert!(!data_key.ciphertext.is_empty());

        let plaintext_key = data_key.plaintext.expect("Data key plaintext should be present");
        assert_eq!(plaintext_key.len(), 32); // AES_256 key length

        println!("✓ Data key generated successfully:");
        println!("  - Key ID: {}", data_key.key_id);
        println!("  - Plaintext key length: {} bytes", plaintext_key.len());
        println!("  - Encrypted key length: {} bytes", data_key.ciphertext.len());
    }

    #[tokio::test]
    #[ignore = "Requires a running Vault instance"]
    async fn test_vault_error_handling() {
        let config = create_test_vault_config();
        let client = VaultKmsClient::new(config).await.expect("Failed to create Vault client");

        // Test decryption with invalid ciphertext format
        println!("Testing error handling with invalid ciphertext format...");
        let decrypt_request = DecryptRequest {
            ciphertext: b"invalid-ciphertext-format".to_vec(),
            encryption_context: HashMap::new(),
            grant_tokens: vec![],
        };

        let result = client.decrypt(&decrypt_request, None).await;
        assert!(result.is_err(), "Expected error for invalid ciphertext format");
        println!(
            "✓ Error handling works: {}",
            result.expect_err("Expected error for invalid ciphertext format")
        );

        // Test decryption with invalid ciphertext
        println!("Testing error handling with invalid ciphertext...");
        let decrypt_request = DecryptRequest {
            ciphertext: b"invalid-ciphertext".to_vec(),
            encryption_context: HashMap::new(),
            grant_tokens: vec![],
        };

        let result = client.decrypt(&decrypt_request, None).await;
        assert!(result.is_err(), "Expected error for invalid ciphertext");
        println!("✓ Error handling works: {}", result.expect_err("Expected error for invalid ciphertext"));
    }

    #[tokio::test]
    #[ignore = "Requires a running Vault instance"]
    async fn test_vault_integration_full() {
        println!("\n=== Full Vault Integration Test ===");

        let config = create_test_vault_config();
        let client = VaultKmsClient::new(config).await.expect("Failed to create Vault client");

        let key_id = format!("integration-test-{}", Uuid::new_v4());
        println!("Using key ID: {key_id}");

        // 1. Create key
        println!("\n1. Creating master key...");
        let master_key = client
            .create_key(&key_id, "aes256-gcm96", None)
            .await
            .expect("Failed to create key");
        println!("   ✓ Master key created: {}", master_key.key_id);

        // 2. Generate data key
        println!("\n2. Generating data key...");
        let generate_request = GenerateKeyRequest {
            master_key_id: key_id.clone(),
            key_spec: "AES_256".to_string(),
            key_length: Some(32),
            encryption_context: HashMap::from([("test".to_string(), "integration".to_string())]),
            grant_tokens: vec![],
        };
        let data_key = client
            .generate_data_key(&generate_request, None)
            .await
            .expect("Failed to generate data key");
        println!(
            "   ✓ Data key generated, length: {} bytes",
            data_key
                .plaintext
                .as_ref()
                .expect("Data key plaintext should be present")
                .len()
        );

        // 3. Encrypt with master key
        println!("\n3. Encrypting with master key...");
        let test_data = b"This is a comprehensive integration test for Vault KMS";
        let encrypt_request = EncryptRequest {
            key_id: key_id.clone(),
            plaintext: test_data.to_vec(),
            encryption_context: HashMap::from([
                ("operation".to_string(), "integration-test".to_string()),
                ("timestamp".to_string(), chrono::Utc::now().to_rfc3339()),
            ]),
            grant_tokens: vec![],
        };
        let encrypt_response = client.encrypt(&encrypt_request, None).await.expect("Failed to encrypt data");
        println!("   ✓ Data encrypted, ciphertext length: {} bytes", encrypt_response.ciphertext.len());

        // 4. Decrypt
        println!("\n4. Decrypting data...");
        let decrypt_request = DecryptRequest {
            ciphertext: encrypt_response.ciphertext,
            encryption_context: encrypt_request.encryption_context,
            grant_tokens: vec![],
        };
        let decrypt_response = client.decrypt(&decrypt_request, None).await.expect("Failed to decrypt data");

        let decrypted_data = decrypt_response;
        assert_eq!(decrypted_data, test_data);
        println!("   ✓ Data decrypted successfully: {}", String::from_utf8_lossy(&decrypted_data));

        // 5. Skip key listing (not supported by KV engine)
        println!("\n5. Skipping key listing (not supported by KV engine)...");
        // In a real implementation, you might maintain a separate index of keys
        // For this test, we'll just verify the key directly
        let key_exists = client.describe_key(&key_id, None).await.is_ok();
        assert!(key_exists, "Key not found via direct lookup");
        println!("   ✓ Key verified via direct lookup");

        // 6. Get key info
        println!("\n6. Getting key information...");
        let key_info = client.describe_key(&key_id, None).await.expect("Failed to get key info");
        println!("   ✓ Key info retrieved:");
        println!("     - Status: {:?}", key_info.status);
        println!("     - Usage: {:?}", key_info.usage);
        println!("     - Created: {:?}", key_info.created_at);

        println!("\n=== Integration Test Completed Successfully ===");
    }
}
