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

use crate::{decrypt_data, encrypt_data};

const PASSWORD: &[u8] = "test_password".as_bytes();
const LONG_PASSWORD: &[u8] = "very_long_password_with_many_characters_for_testing_purposes_123456789".as_bytes();
const EMPTY_PASSWORD: &[u8] = b"";

#[test_case::test_case("hello world".as_bytes())]
#[test_case::test_case(&[])]
#[test_case::test_case(&[1, 2, 3])]
#[test_case::test_case(&[3, 2, 1])]
fn test_basic_encrypt_decrypt_roundtrip(input: &[u8]) -> Result<(), crate::Error> {
    let encrypted = encrypt_data(PASSWORD, input)?;
    let decrypted = decrypt_data(PASSWORD, &encrypted)?;
    assert_eq!(input, decrypted, "input is not equal output");
    Ok(())
}

#[test]
fn test_encrypt_decrypt_with_different_passwords() -> Result<(), crate::Error> {
    let data = b"sensitive data";
    let password1 = b"password1";
    let password2 = b"password2";

    let encrypted = encrypt_data(password1, data)?;

    // Decrypting with correct password should work
    let decrypted = decrypt_data(password1, &encrypted)?;
    assert_eq!(data, decrypted.as_slice());

    // Decrypting with wrong password should fail
    let result = decrypt_data(password2, &encrypted);
    assert!(result.is_err(), "Decryption with wrong password should fail");

    Ok(())
}

#[test]
fn test_encrypt_decrypt_empty_data() -> Result<(), crate::Error> {
    let empty_data = b"";
    let encrypted = encrypt_data(PASSWORD, empty_data)?;
    let decrypted = decrypt_data(PASSWORD, &encrypted)?;
    assert_eq!(empty_data, decrypted.as_slice());
    Ok(())
}

#[test]
fn test_encrypt_decrypt_large_data() -> Result<(), crate::Error> {
    // Test with 1MB of data
    let large_data = vec![0xAB; 1024 * 1024];
    let encrypted = encrypt_data(PASSWORD, &large_data)?;
    let decrypted = decrypt_data(PASSWORD, &encrypted)?;
    assert_eq!(large_data, decrypted);
    Ok(())
}

#[test]
fn test_encrypt_decrypt_with_empty_password() -> Result<(), crate::Error> {
    let data = b"test data";
    let encrypted = encrypt_data(EMPTY_PASSWORD, data)?;
    let decrypted = decrypt_data(EMPTY_PASSWORD, &encrypted)?;
    assert_eq!(data, decrypted.as_slice());
    Ok(())
}

#[test]
fn test_encrypt_decrypt_with_long_password() -> Result<(), crate::Error> {
    let data = b"test data with long password";
    let encrypted = encrypt_data(LONG_PASSWORD, data)?;
    let decrypted = decrypt_data(LONG_PASSWORD, &encrypted)?;
    assert_eq!(data, decrypted.as_slice());
    Ok(())
}

#[test]
fn test_encrypt_decrypt_binary_data() -> Result<(), crate::Error> {
    // Test with various binary patterns
    let binary_patterns = [
        vec![0x00; 100],                                     // All zeros
        vec![0xFF; 100],                                     // All ones
        (0..=255u8).cycle().take(1000).collect::<Vec<u8>>(), // Sequential pattern
        [0xAA, 0x55].repeat(500),                            // Alternating pattern
    ];

    for pattern in &binary_patterns {
        let encrypted = encrypt_data(PASSWORD, pattern)?;
        let decrypted = decrypt_data(PASSWORD, &encrypted)?;
        assert_eq!(pattern, &decrypted, "Binary pattern mismatch");
    }
    Ok(())
}

#[test]
fn test_encrypt_decrypt_unicode_data() -> Result<(), crate::Error> {
    let unicode_strings = [
        "Hello, 世界！🌍",
        "Тест на русском языке",
        "العربية اختبار",
        "🚀🔐💻🌟⭐",
        "Mixed: ASCII + 中文 + العربية + 🎉",
    ];

    for text in &unicode_strings {
        let data = text.as_bytes();
        let encrypted = encrypt_data(PASSWORD, data)?;
        let decrypted = decrypt_data(PASSWORD, &encrypted)?;
        assert_eq!(data, decrypted.as_slice(), "Unicode data mismatch for: {text}");
    }
    Ok(())
}

#[test]
fn test_decrypt_with_corrupted_data() {
    let data = b"test data";
    let encrypted = encrypt_data(PASSWORD, data).expect("Encryption should succeed");

    // Test various corruption scenarios
    let corruption_tests = [
        (0, "Corrupt first byte"),
        (encrypted.len() - 1, "Corrupt last byte"),
        (encrypted.len() / 2, "Corrupt middle byte"),
    ];

    for (corrupt_index, description) in &corruption_tests {
        let mut corrupted = encrypted.clone();
        corrupted[*corrupt_index] ^= 0xFF; // Flip all bits

        let result = decrypt_data(PASSWORD, &corrupted);
        assert!(result.is_err(), "{description} should cause decryption to fail");
    }
}

#[test]
fn test_decrypt_with_truncated_data() {
    let data = b"test data for truncation";
    let encrypted = encrypt_data(PASSWORD, data).expect("Encryption should succeed");

    // Test truncation at various lengths
    let truncation_lengths = [
        0,                   // Empty data
        10,                  // Very short
        32,                  // Salt length
        44,                  // Just before nonce
        encrypted.len() - 1, // Missing last byte
    ];

    for &length in &truncation_lengths {
        let truncated = &encrypted[..length.min(encrypted.len())];
        let result = decrypt_data(PASSWORD, truncated);
        assert!(result.is_err(), "Truncated data (length {length}) should cause decryption to fail");
    }
}

#[test]
fn test_decrypt_with_invalid_header() {
    let data = b"test data";
    let mut encrypted = encrypt_data(PASSWORD, data).expect("Encryption should succeed");

    // Corrupt the algorithm ID (byte 32)
    if encrypted.len() > 32 {
        encrypted[32] = 0xFF; // Invalid algorithm ID
        let result = decrypt_data(PASSWORD, &encrypted);
        assert!(result.is_err(), "Invalid algorithm ID should cause decryption to fail");
    }
}

#[test]
fn test_encryption_produces_different_outputs() -> Result<(), crate::Error> {
    let data = b"same data";

    // Encrypt the same data multiple times
    let encrypted1 = encrypt_data(PASSWORD, data)?;
    let encrypted2 = encrypt_data(PASSWORD, data)?;

    // Encrypted outputs should be different due to random salt and nonce
    assert_ne!(encrypted1, encrypted2, "Encryption should produce different outputs for same input");

    // But both should decrypt to the same original data
    let decrypted1 = decrypt_data(PASSWORD, &encrypted1)?;
    let decrypted2 = decrypt_data(PASSWORD, &encrypted2)?;
    assert_eq!(decrypted1, decrypted2);
    assert_eq!(data, decrypted1.as_slice());

    Ok(())
}

#[test]
fn test_encrypted_data_structure() -> Result<(), crate::Error> {
    let data = b"test data";
    let encrypted = encrypt_data(PASSWORD, data)?;

    // Encrypted data should be longer than original (due to salt, nonce, tag)
    assert!(encrypted.len() > data.len(), "Encrypted data should be longer than original");

    // Should have at least: 32 bytes salt + 1 byte ID + 12 bytes nonce + data + 16 bytes tag
    let min_expected_length = 32 + 1 + 12 + data.len() + 16;
    assert!(
        encrypted.len() >= min_expected_length,
        "Encrypted data length {} should be at least {}",
        encrypted.len(),
        min_expected_length
    );

    Ok(())
}

#[test]
fn test_password_variations() -> Result<(), crate::Error> {
    let data = b"test data";

    let password_variations = [
        b"a".as_slice(),                // Single character
        b"12345".as_slice(),            // Numeric
        b"!@#$%^&*()".as_slice(),       // Special characters
        b"\x00\x01\x02\x03".as_slice(), // Binary password
        "пароль тест".as_bytes(),       // Unicode password
        &[0xFF; 64],                    // Long binary password
    ];

    for password in &password_variations {
        let encrypted = encrypt_data(password, data)?;
        let decrypted = decrypt_data(password, &encrypted)?;
        assert_eq!(data, decrypted.as_slice(), "Failed with password: {password:?}");
    }

    Ok(())
}

#[test]
fn test_deterministic_with_same_salt_and_nonce() {
    // Note: This test is more for understanding the behavior
    // In real implementation, salt and nonce should be random
    let data = b"test data";

    let encrypted1 = encrypt_data(PASSWORD, data).expect("Encryption should succeed");
    let encrypted2 = encrypt_data(PASSWORD, data).expect("Encryption should succeed");

    // Due to random salt and nonce, outputs should be different
    assert_ne!(encrypted1, encrypted2, "Encryption should use random salt/nonce");
}

#[test]
fn test_cross_platform_compatibility() -> Result<(), crate::Error> {
    // Test data that might behave differently on different platforms
    let test_cases = [
        vec![0x00, 0x01, 0x02, 0x03],                              // Low values
        vec![0xFC, 0xFD, 0xFE, 0xFF],                              // High values
        (0..256u16).map(|x| (x % 256) as u8).collect::<Vec<u8>>(), // Full byte range
    ];

    for test_data in &test_cases {
        let encrypted = encrypt_data(PASSWORD, test_data)?;
        let decrypted = decrypt_data(PASSWORD, &encrypted)?;
        assert_eq!(test_data, &decrypted, "Cross-platform compatibility failed");
    }

    Ok(())
}

#[test]
fn test_memory_safety_with_large_passwords() -> Result<(), crate::Error> {
    let data = b"test data";

    // Test with very large passwords
    let large_passwords = [
        vec![b'a'; 1024],                                    // 1KB password
        vec![b'x'; 10 * 1024],                               // 10KB password
        (0..=255u8).cycle().take(5000).collect::<Vec<u8>>(), // 5KB varied password
    ];

    for password in &large_passwords {
        let encrypted = encrypt_data(password, data)?;
        let decrypted = decrypt_data(password, &encrypted)?;
        assert_eq!(data, decrypted.as_slice(), "Failed with large password of size {}", password.len());
    }

    Ok(())
}

#[test]
fn test_concurrent_encryption_safety() -> Result<(), crate::Error> {
    use std::sync::Arc;
    use std::thread;

    let data = Arc::new(b"concurrent test data".to_vec());
    let password = Arc::new(b"concurrent_password".to_vec());

    let handles: Vec<_> = (0..10)
        .map(|i| {
            let data = Arc::clone(&data);
            let password = Arc::clone(&password);

            thread::spawn(move || {
                let encrypted = encrypt_data(&password, &data).expect("Encryption should succeed");
                let decrypted = decrypt_data(&password, &encrypted).expect("Decryption should succeed");
                assert_eq!(**data, decrypted, "Thread {i} failed");
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("Thread should complete successfully");
    }

    Ok(())
}
