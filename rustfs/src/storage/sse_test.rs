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
mod tests {
    use crate::storage::sse::SseDekProvider;
    use crate::storage::sse::TestSseDekProvider;
    use rustfs_rio::{DecryptReader, EncryptReader, WarpReader};
    use std::io::Cursor;
    use tokio::io::AsyncReadExt;

    /// Test EncryptReader encryption and DecryptReader decryption integration without KMS
    /// This test verifies the complete encryption/decryption flow:
    /// 1. Create TestSseDekProvider with a test master key
    /// 2. Generate data encryption key (DEK) using the provider
    /// 3. Encrypt data using EncryptReader
    /// 4. Decrypt data using DecryptReader
    /// 5. Verify decrypted data matches original plaintext
    #[tokio::test]
    async fn test_encrypt_reader_decrypt_reader_integration_without_kms() {
        // Step 1: Create TestSseDekProvider with test master key
        let provider = TestSseDekProvider::new_with_key([0x42u8; 32]);

        // Step 2: Generate a data encryption key
        let bucket = "test-bucket";
        let key = "test-key";
        let kms_key_id = "default"; // Key ID is ignored in test provider

        let (data_key, _encrypted_dek) = provider
            .generate_sse_dek(bucket, key, kms_key_id)
            .await
            .expect("Failed to generate DEK");

        // Verify data key properties
        assert_eq!(data_key.plaintext_key.len(), 32);
        assert_eq!(data_key.nonce.len(), 12);

        // Step 3: Prepare test data
        let plaintext = b"Hello, World! This is a test message for encryption and decryption.";
        println!("Original plaintext: {:?}", String::from_utf8_lossy(plaintext));
        println!("Plaintext length: {} bytes", plaintext.len());

        // Step 4: Encrypt using EncryptReader (wrap Cursor with WarpReader)
        let plaintext_reader = WarpReader::new(Cursor::new(plaintext.to_vec()));
        let mut encrypt_reader = EncryptReader::new(plaintext_reader, data_key.plaintext_key, data_key.nonce);

        // Read encrypted data
        let mut encrypted_data = Vec::new();
        encrypt_reader
            .read_to_end(&mut encrypted_data)
            .await
            .expect("Failed to read encrypted data");

        println!("Encrypted data length: {} bytes", encrypted_data.len());
        println!(
            "First 16 bytes of encrypted data: {:02x?}",
            &encrypted_data[..16.min(encrypted_data.len())]
        );

        // Verify encrypted data is different from plaintext
        assert_ne!(
            &encrypted_data[..plaintext.len().min(encrypted_data.len())],
            plaintext,
            "Encrypted data should be different from plaintext"
        );

        // Step 5: Decrypt using DecryptReader (wrap Cursor with WarpReader)
        let encrypted_reader = WarpReader::new(Cursor::new(encrypted_data));
        let mut decrypt_reader = DecryptReader::new(encrypted_reader, data_key.plaintext_key, data_key.nonce);

        // Read decrypted data
        let mut decrypted_data = Vec::new();
        decrypt_reader
            .read_to_end(&mut decrypted_data)
            .await
            .expect("Failed to read decrypted data");

        println!("Decrypted data: {:?}", String::from_utf8_lossy(&decrypted_data));
        println!("Decrypted length: {} bytes", decrypted_data.len());

        // Step 6: Verify decrypted data matches original plaintext
        assert_eq!(decrypted_data, plaintext, "Decrypted data should match original plaintext");

        println!("✅ EncryptReader/DecryptReader integration test passed!");
    }

    /// Test EncryptReader with large data (10MB) without KMS
    #[tokio::test]
    async fn test_encrypt_reader_large_data_without_kms() {
        // Create TestSseDekProvider with test master key
        let provider = TestSseDekProvider::new_with_key([0x42u8; 32]);

        let bucket = "test-bucket";
        let key = "test-key-large";
        let kms_key_id = "default";

        let (data_key, _encrypted_dek) = provider
            .generate_sse_dek(bucket, key, kms_key_id)
            .await
            .expect("Failed to generate DEK");

        // Create 1MB of test data
        let plaintext_size = 1024 * 1024 * 10;
        let plaintext: Vec<u8> = (0..plaintext_size).map(|i| (i % 256) as u8).collect();
        println!("Testing with {} bytes of data", plaintext.len());

        // Encrypt (wrap with WarpReader)
        let plaintext_reader = WarpReader::new(Cursor::new(plaintext.clone()));
        let mut encrypt_reader = EncryptReader::new(plaintext_reader, data_key.plaintext_key, data_key.nonce);

        let mut encrypted_data = Vec::new();
        encrypt_reader
            .read_to_end(&mut encrypted_data)
            .await
            .expect("Failed to encrypt large data");

        println!("Encrypted {} bytes to {} bytes", plaintext.len(), encrypted_data.len());

        // Decrypt (wrap with WarpReader)
        let encrypted_reader = WarpReader::new(Cursor::new(encrypted_data));
        let mut decrypt_reader = DecryptReader::new(encrypted_reader, data_key.plaintext_key, data_key.nonce);

        let mut decrypted_data = Vec::new();
        decrypt_reader
            .read_to_end(&mut decrypted_data)
            .await
            .expect("Failed to decrypt large data");

        // Verify
        assert_eq!(decrypted_data.len(), plaintext.len(), "Decrypted size should match original");
        assert_eq!(decrypted_data, plaintext, "Decrypted data should match original plaintext");

        println!("✅ Large data encryption/decryption test passed!");
    }

    /// Test EncryptReader with different nonces produce different ciphertexts
    #[tokio::test]
    async fn test_encrypt_reader_different_nonces_produce_different_ciphertext() {
        // Create TestSseDekProvider with test master key
        let provider = TestSseDekProvider::new_with_key([0x42u8; 32]);

        let bucket = "test-bucket";
        let key = "test-key";
        let kms_key_id = "default";

        // Generate two different keys (with different nonces)
        let (data_key1, _) = provider
            .generate_sse_dek(bucket, key, kms_key_id)
            .await
            .expect("Failed to generate DEK 1");

        let (data_key2, _) = provider
            .generate_sse_dek(bucket, key, kms_key_id)
            .await
            .expect("Failed to generate DEK 2");

        // Verify nonces are different
        assert_ne!(data_key1.nonce, data_key2.nonce, "Different keys should have different nonces");

        // Same plaintext
        let plaintext = b"Same plaintext";

        // Encrypt with first key (wrap with WarpReader)
        let reader1 = WarpReader::new(Cursor::new(plaintext.to_vec()));
        let mut encrypt_reader1 = EncryptReader::new(reader1, data_key1.plaintext_key, data_key1.nonce);
        let mut encrypted1 = Vec::new();
        encrypt_reader1.read_to_end(&mut encrypted1).await.unwrap();

        // Encrypt with second key (wrap with WarpReader)
        let reader2 = WarpReader::new(Cursor::new(plaintext.to_vec()));
        let mut encrypt_reader2 = EncryptReader::new(reader2, data_key2.plaintext_key, data_key2.nonce);
        let mut encrypted2 = Vec::new();
        encrypt_reader2.read_to_end(&mut encrypted2).await.unwrap();

        // Verify ciphertexts are different (due to different nonces/keys)
        assert_ne!(
            encrypted1, encrypted2,
            "Same plaintext with different nonces should produce different ciphertext"
        );

        println!("✅ Different nonces produce different ciphertext - test passed!");
    }

    /// Test EncryptReader with decrypted DEK (simulating full cycle)
    #[tokio::test]
    async fn test_encrypt_reader_with_decrypted_dek() {
        // Create TestSseDekProvider with test master key
        let provider = TestSseDekProvider::new_with_key([0x42u8; 32]);

        let bucket = "test-bucket";
        let key = "test-key";
        let kms_key_id = "default";

        // Step 1: Generate DEK and get encrypted DEK
        let (data_key, encrypted_dek) = provider
            .generate_sse_dek(bucket, key, kms_key_id)
            .await
            .expect("Failed to generate DEK");

        let original_plaintext_key = data_key.plaintext_key;
        let original_nonce = data_key.nonce;

        // Step 2: Later, decrypt the DEK (simulating GET operation)
        let decrypted_plaintext_key = provider
            .decrypt_sse_dek(&encrypted_dek, kms_key_id)
            .await
            .expect("Failed to decrypt DEK");

        // Step 3: Verify decrypted key matches original
        assert_eq!(
            decrypted_plaintext_key, original_plaintext_key,
            "Decrypted DEK should match original plaintext key"
        );

        // Step 4: Use decrypted key to encrypt/decrypt data
        let plaintext = b"Test data with decrypted DEK";

        // Encrypt with original key (wrap with WarpReader)
        let reader = WarpReader::new(Cursor::new(plaintext.to_vec()));
        let mut encrypt_reader = EncryptReader::new(reader, original_plaintext_key, original_nonce);
        let mut encrypted_data = Vec::new();
        encrypt_reader.read_to_end(&mut encrypted_data).await.unwrap();

        // Decrypt with recovered key (simulating GET operation) (wrap with WarpReader)
        let reader = WarpReader::new(Cursor::new(encrypted_data));
        let mut decrypt_reader = DecryptReader::new(
            reader,
            decrypted_plaintext_key,
            original_nonce, // In real scenario, read from metadata
        );
        let mut decrypted_data = Vec::new();
        decrypt_reader.read_to_end(&mut decrypted_data).await.unwrap();

        // Step 5: Verify
        assert_eq!(decrypted_data, plaintext, "Data decrypted with recovered key should match original");

        println!("✅ Full cycle (generate -> encrypt DEK -> decrypt DEK -> decrypt data) test passed!");
    }
}
