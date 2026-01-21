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

//! KMS Demo 2 - Comprehensive example demonstrating RustFS KMS with Vault backend
//!
//! This example demonstrates:
//! - Initializing and configuring KMS service with Vault backend
//! - Creating master keys stored in Vault
//! - Generating data encryption keys
//! - Encrypting and decrypting data using high-level APIs
//! - Key management operations with Vault
//! - Cache statistics
//!
//! Prerequisites:
//! - Vault server running at http://127.0.0.1:8200 (or set RUSTFS_KMS_VAULT_ADDRESS)
//! - Vault token (set RUSTFS_KMS_VAULT_TOKEN environment variable, or use default "dev-token" for dev mode)
//!
//! Run with: `cargo run --example demo2`
//! Or with custom Vault settings:
//!   RUSTFS_KMS_VAULT_ADDRESS=http://127.0.0.1:8200 RUSTFS_KMS_VAULT_TOKEN=your-token cargo run --example demo2

use rustfs_kms::{
    CreateKeyRequest, DescribeKeyRequest, EncryptionAlgorithm, GenerateDataKeyRequest, KeySpec, KeyUsage, KmsConfig, KmsError,
    ListKeysRequest, init_global_kms_service_manager,
};
use std::collections::HashMap;
use std::io::Cursor;
use tokio::io::AsyncReadExt;
use url::Url;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Note: Tracing is optional - if tracing-subscriber is not available,
    // the example will still work but with less detailed logging

    println!("=== RustFS KMS Demo 2 (Vault Backend) ===\n");

    // Step 1: Initialize global KMS service manager
    println!("1. Initializing KMS service manager...");
    let service_manager = init_global_kms_service_manager();
    println!("   ✓ Service manager initialized\n");

    // Step 2: Get Vault configuration from environment or use defaults
    println!("2. Configuring Vault backend...");
    let vault_address = std::env::var("RUSTFS_KMS_VAULT_ADDRESS").unwrap_or_else(|_| "http://127.0.0.1:8200".to_string());
    let vault_token = std::env::var("RUSTFS_KMS_VAULT_TOKEN").unwrap_or_else(|_| {
        println!("   ⚠️  No RUSTFS_KMS_VAULT_TOKEN found, using default 'dev-token'");
        println!("      For production, set RUSTFS_KMS_VAULT_TOKEN environment variable");
        "dev-token".to_string()
    });

    let vault_url = Url::parse(&vault_address).map_err(|e| format!("Invalid Vault address '{}': {}", vault_address, e))?;

    println!("   ✓ Vault address: {}", vault_address);
    println!("   ✓ Using token authentication\n");

    // Step 3: Configure KMS with Vault backend
    println!("3. Configuring KMS with Vault backend...");
    let config = KmsConfig::vault(vault_url, vault_token)
        .with_default_key("demo-key-master-1".to_string())
        .with_cache(true);
    service_manager.configure(config).await?;
    println!("   ✓ KMS configured with Vault backend\n");

    // Step 4: Start the KMS service
    println!("4. Starting KMS service...");
    service_manager.start().await?;
    println!("   ✓ KMS service started\n");

    // Step 5: Get the encryption service
    println!("5. Getting encryption service...");
    let encryption_service = rustfs_kms::get_global_encryption_service()
        .await
        .ok_or("Encryption service not available")?;
    println!("   ✓ Encryption service obtained\n");

    // Step 6: Create a master key (stored in Vault) or use existing one
    println!("6. Checking for existing master key in Vault...");
    let master_key_id = "demo-key-master-1".to_string();
    let describe_request = DescribeKeyRequest {
        key_id: master_key_id.clone(),
    };

    let master_key_id = match encryption_service.describe_key(describe_request).await {
        Ok(describe_response) => {
            // Key already exists, use it
            println!("   ✓ Master key already exists in Vault:");
            println!("     - Key ID: {}", describe_response.key_metadata.key_id);
            println!("     - Key State: {:?}", describe_response.key_metadata.key_state);
            println!("     - Key Usage: {:?}", describe_response.key_metadata.key_usage);
            println!("     - Created: {}\n", describe_response.key_metadata.creation_date);
            describe_response.key_metadata.key_id
        }
        Err(KmsError::KeyNotFound { .. }) => {
            // Key doesn't exist, create it
            println!("   Key not found, creating new master key in Vault...");
            let create_request = CreateKeyRequest {
                key_name: Some(master_key_id.clone()),
                key_usage: KeyUsage::EncryptDecrypt,
                description: Some("Demo master key for encryption (stored in Vault)".to_string()),
                policy: None,
                tags: {
                    let mut tags = HashMap::new();
                    tags.insert("environment".to_string(), "demo".to_string());
                    tags.insert("purpose".to_string(), "testing".to_string());
                    tags.insert("backend".to_string(), "vault".to_string());
                    tags
                },
                origin: Some("demo2.rs".to_string()),
            };

            let create_response = encryption_service.create_key(create_request).await?;
            println!("   ✓ Master key created in Vault:");
            println!("     - Key ID: {}", create_response.key_id);
            println!("     - Key State: {:?}", create_response.key_metadata.key_state);
            println!("     - Key Usage: {:?}", create_response.key_metadata.key_usage);
            println!("     - Created: {}\n", create_response.key_metadata.creation_date);
            create_response.key_id
        }
        Err(e) => {
            // Other error, return it
            return Err(Box::new(e) as Box<dyn std::error::Error>);
        }
    };

    // Step 7: Describe the key (retrieved from Vault)
    println!("7. Describing the master key (from Vault)...");
    let describe_request = DescribeKeyRequest {
        key_id: master_key_id.clone(),
    };
    let describe_response = encryption_service.describe_key(describe_request).await?;
    let metadata = describe_response.key_metadata;
    println!("   ✓ Key details (from Vault):");
    println!("     - Key ID: {}", metadata.key_id);
    println!("     - Description: {:?}", metadata.description);
    println!("     - Key Usage: {:?}", metadata.key_usage);
    println!("     - Key State: {:?}", metadata.key_state);
    println!("     - Tags: {:?}\n", metadata.tags);

    // Step 8: Generate a data encryption key (OPTIONAL - for demonstration only)
    // NOTE: This step is OPTIONAL and only for educational purposes!
    // In real usage, you can skip this step and go directly to Step 9.
    // encrypt_object() will automatically generate a data key internally.
    println!("8. [OPTIONAL] Generating a data encryption key (for demonstration)...");
    println!("   ⚠️  This step is OPTIONAL - only for understanding the two-layer key architecture:");
    println!("   - Master Key (CMK): Stored in Vault, used to encrypt/decrypt data keys");
    println!("   - Data Key (DEK): Generated per object, encrypted by master key");
    println!("   In production, you can skip this and use encrypt_object() directly!\n");

    let data_key_request = GenerateDataKeyRequest {
        key_id: master_key_id.clone(),
        key_spec: KeySpec::Aes256,
        encryption_context: {
            let mut context = HashMap::new();
            context.insert("bucket".to_string(), "demo-bucket".to_string());
            context.insert("object_key".to_string(), "demo-object.txt".to_string());
            context
        },
    };

    let data_key_response = encryption_service.generate_data_key(data_key_request).await?;
    println!("   ✓ Data key generated (for demonstration):");
    println!("     - Master Key ID: {}", data_key_response.key_id);
    println!("     - Data Key (plaintext) length: {} bytes", data_key_response.plaintext_key.len());
    println!(
        "     - Encrypted Data Key (ciphertext blob) length: {} bytes",
        data_key_response.ciphertext_blob.len()
    );
    println!("     - Note: This data key is NOT used in Step 9 - encrypt_object() generates its own!\n");

    // Step 9: Encrypt some data using high-level API
    // This is the RECOMMENDED way to encrypt data - everything is handled automatically!
    println!("9. Encrypting data using object encryption service (RECOMMENDED)...");
    println!("   ✅ This is all you need! encrypt_object() handles everything:");
    println!("   1. Validates/creates the master key in Vault (if needed)");
    println!("   2. Generates a NEW data key using the master key from Vault (independent of Step 8)");
    println!("   3. Uses the data key to encrypt the actual data");
    println!("   4. Stores the encrypted data key (ciphertext blob) in metadata");
    println!("   You only need to provide the master_key_id - everything else is handled!\n");

    let plaintext = b"Hello, RustFS KMS with Vault! This is a test message for encryption.";
    println!("   Plaintext: {}", String::from_utf8_lossy(plaintext));

    let reader = Cursor::new(plaintext);
    // Just provide the master_key_id - encrypt_object() handles everything internally!
    let encryption_result = encryption_service
        .encrypt_object(
            "demo-bucket",
            "demo-object.txt",
            reader,
            &EncryptionAlgorithm::Aes256,
            Some(&master_key_id), // Only need to provide master key ID
            None,
        )
        .await?;

    println!("   ✓ Data encrypted:");
    println!("     - Encrypted data length: {} bytes", encryption_result.ciphertext.len());
    println!("     - Algorithm: {}", encryption_result.metadata.algorithm);
    println!(
        "     - Master Key ID: {} (stored in Vault, used to encrypt the data key)",
        encryption_result.metadata.key_id
    );
    println!(
        "     - Encrypted Data Key length: {} bytes (stored in metadata)",
        encryption_result.metadata.encrypted_data_key.len()
    );
    println!("     - Original size: {} bytes\n", encryption_result.metadata.original_size);

    // Step 10: Decrypt the data using high-level API
    println!("10. Decrypting data...");
    println!("   Note: decrypt_object() has the ENTIRE decryption flow built-in:");
    println!("   1. Extracts the encrypted data key from metadata");
    println!("   2. Uses master key from Vault to decrypt the data key");
    println!("   3. Uses the decrypted data key to decrypt the actual data");
    println!("   You only need to provide the encrypted data and metadata!\n");

    let mut decrypted_reader = encryption_service
        .decrypt_object(
            "demo-bucket",
            "demo-object.txt",
            encryption_result.ciphertext.clone(),
            &encryption_result.metadata, // Contains everything needed for decryption
            None,
        )
        .await?;

    let mut decrypted_data = Vec::new();
    decrypted_reader.read_to_end(&mut decrypted_data).await?;

    println!("   ✓ Data decrypted:");
    println!("     - Decrypted text: {}\n", String::from_utf8_lossy(&decrypted_data));

    // Verify decryption
    assert_eq!(plaintext, decrypted_data.as_slice());
    println!("   ✓ Decryption verified: plaintext matches original\n");

    // Step 11: List all keys (from Vault)
    println!("11. Listing all keys (from Vault)...");
    let list_request = ListKeysRequest {
        limit: Some(10),
        marker: None,
        usage_filter: None,
        status_filter: None,
    };
    let list_response = encryption_service.list_keys(list_request).await?;
    println!("   ✓ Keys found in Vault: {}", list_response.keys.len());
    for (idx, key_info) in list_response.keys.iter().enumerate() {
        println!("     {}. {} ({:?})", idx + 1, key_info.key_id, key_info.status);
    }
    println!();

    // Step 12: Check cache statistics
    println!("12. Checking cache statistics...");
    if let Some((hits, misses)) = encryption_service.cache_stats().await {
        println!("   ✓ Cache statistics:");
        println!("     - Cache hits: {}", hits);
        println!("     - Cache misses: {}\n", misses);
    } else {
        println!("   - Cache is disabled\n");
    }

    // Step 13: Health check (verifies Vault connectivity)
    println!("13. Performing health check (Vault connectivity)...");
    let is_healthy = encryption_service.health_check().await?;
    println!("   ✓ KMS backend (Vault) is healthy: {}\n", is_healthy);

    // Step 14: Stop the service
    println!("14. Stopping KMS service...");
    service_manager.stop().await?;
    println!("   ✓ KMS service stopped\n");

    println!("=== Demo 2 (Vault Backend) completed successfully! ===");
    println!("\n💡 Tips:");
    println!("   - Keys are now stored in Vault at: {}/v1/secret/data/rustfs/kms/keys/", vault_address);
    println!("   - You can verify keys in Vault using: vault kv list secret/rustfs/kms/keys/");
    println!("   - For production, use proper Vault authentication (AppRole, etc.)");
    println!("   - See examples/VAULT_SETUP.md for detailed Vault configuration guide");

    Ok(())
}
