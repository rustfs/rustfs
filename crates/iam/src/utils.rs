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

use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header};
use rand::{Rng, RngCore};
use serde::{Serialize, de::DeserializeOwned};
use std::io::{Error, Result};

/// Generates a random access key of the specified length.
///
/// # Arguments
///
/// * `length` - The length of the access key to be generated.
///
/// # Returns
///
/// * `Result<String>` - A result containing the generated access key or an error if the length is invalid.
///
/// # Errors
///
/// * Returns an error if the length is less than 3.
///
pub fn gen_access_key(length: usize) -> Result<String> {
    const ALPHA_NUMERIC_TABLE: [char; 36] = [
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N',
        'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
    ];

    if length < 3 {
        return Err(Error::other("access key length is too short"));
    }

    let mut result = String::with_capacity(length);
    let mut rng = rand::rng();

    for _ in 0..length {
        result.push(ALPHA_NUMERIC_TABLE[rng.random_range(0..ALPHA_NUMERIC_TABLE.len())]);
    }

    Ok(result)
}

/// Generates a random secret key of the specified length.
///
/// # Arguments
///
/// * `length` - The length of the secret key to be generated.
///
/// # Returns
///
/// * `Result<String>` - A result containing the generated secret key or an error if the length is invalid.
///
/// # Errors
///
/// * Returns an error if the length is less than 8.
///
pub fn gen_secret_key(length: usize) -> Result<String> {
    use base64_simd::URL_SAFE_NO_PAD;

    if length < 8 {
        return Err(Error::other("secret key length is too short"));
    }
    let mut rng = rand::rng();

    let mut key = vec![0u8; URL_SAFE_NO_PAD.estimated_decoded_length(length)];
    rng.fill_bytes(&mut key);

    let encoded = URL_SAFE_NO_PAD.encode_to_string(&key);
    let key_str = encoded.replace("/", "+");

    Ok(key_str)
}

pub fn generate_jwt<T: Serialize>(claims: &T, secret: &str) -> std::result::Result<String, jsonwebtoken::errors::Error> {
    let header = Header::new(Algorithm::HS512);
    jsonwebtoken::encode(&header, &claims, &EncodingKey::from_secret(secret.as_bytes()))
}

pub fn extract_claims<T: DeserializeOwned + Clone>(
    token: &str,
    secret: &str,
) -> std::result::Result<jsonwebtoken::TokenData<T>, jsonwebtoken::errors::Error> {
    jsonwebtoken::decode::<T>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &jsonwebtoken::Validation::new(Algorithm::HS512),
    )
}

#[cfg(test)]
mod tests {
    use super::{extract_claims, gen_access_key, gen_secret_key, generate_jwt};
    use serde::{Deserialize, Serialize};

    #[test]
    fn test_gen_access_key_valid_length() {
        // Test valid access key generation
        let key = gen_access_key(10).unwrap();
        assert_eq!(key.len(), 10);

        // Test different lengths
        let key_20 = gen_access_key(20).unwrap();
        assert_eq!(key_20.len(), 20);

        let key_3 = gen_access_key(3).unwrap();
        assert_eq!(key_3.len(), 3);
    }

    #[test]
    fn test_gen_access_key_uniqueness() {
        // Test that generated keys are unique
        let key1 = gen_access_key(16).unwrap();
        let key2 = gen_access_key(16).unwrap();
        assert_ne!(key1, key2, "Generated access keys should be unique");
    }

    #[test]
    fn test_gen_access_key_character_set() {
        // Test that generated keys only contain valid characters
        let key = gen_access_key(100).unwrap();
        for ch in key.chars() {
            assert!(ch.is_ascii_alphanumeric(), "Access key should only contain alphanumeric characters");
            assert!(
                ch.is_ascii_uppercase() || ch.is_ascii_digit(),
                "Access key should only contain uppercase letters and digits"
            );
        }
    }

    #[test]
    fn test_gen_access_key_invalid_length() {
        // Test error cases for invalid lengths
        assert!(gen_access_key(0).is_err(), "Should fail for length 0");
        assert!(gen_access_key(1).is_err(), "Should fail for length 1");
        assert!(gen_access_key(2).is_err(), "Should fail for length 2");

        // Verify error message
        let error = gen_access_key(2).unwrap_err();
        assert_eq!(error.to_string(), "access key length is too short");
    }

    #[test]
    fn test_gen_secret_key_valid_length() {
        // Test valid secret key generation
        let key = gen_secret_key(10).unwrap();
        assert!(!key.is_empty(), "Secret key should not be empty");

        let key_20 = gen_secret_key(20).unwrap();
        assert!(!key_20.is_empty(), "Secret key should not be empty");
    }

    #[test]
    fn test_gen_secret_key_uniqueness() {
        // Test that generated secret keys are unique
        let key1 = gen_secret_key(16).unwrap();
        let key2 = gen_secret_key(16).unwrap();
        assert_ne!(key1, key2, "Generated secret keys should be unique");
    }

    #[test]
    fn test_gen_secret_key_base64_format() {
        // Test that secret key is valid base64-like format
        let key = gen_secret_key(32).unwrap();

        // Should not contain invalid characters for URL-safe base64
        for ch in key.chars() {
            assert!(
                ch.is_ascii_alphanumeric() || ch == '+' || ch == '-' || ch == '_',
                "Secret key should be URL-safe base64 compatible"
            );
        }
    }

    #[test]
    fn test_gen_secret_key_invalid_length() {
        // Test error cases for invalid lengths
        assert!(gen_secret_key(0).is_err(), "Should fail for length 0");
        assert!(gen_secret_key(7).is_err(), "Should fail for length 7");

        // Verify error message
        let error = gen_secret_key(5).unwrap_err();
        assert_eq!(error.to_string(), "secret key length is too short");
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
    struct Claims {
        sub: String,
        company: String,
        exp: usize, // Expiration time (as UTC timestamp)
    }

    #[test]
    fn test_generate_jwt_valid_token() {
        // Test JWT generation with valid claims
        let claims = Claims {
            sub: "user1".to_string(),
            company: "example".to_string(),
            exp: 9999999999, // Far future timestamp for testing
        };
        let secret = "my_secret";
        let token = generate_jwt(&claims, secret).unwrap();

        assert!(!token.is_empty(), "JWT token should not be empty");

        // JWT should have 3 parts separated by dots
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3, "JWT should have 3 parts (header.payload.signature)");

        // Each part should be non-empty
        for part in parts {
            assert!(!part.is_empty(), "JWT parts should not be empty");
        }
    }

    #[test]
    fn test_generate_jwt_different_secrets() {
        // Test that different secrets produce different tokens
        let claims = Claims {
            sub: "user1".to_string(),
            company: "example".to_string(),
            exp: 9999999999, // Far future timestamp for testing
        };

        let token1 = generate_jwt(&claims, "secret1").unwrap();
        let token2 = generate_jwt(&claims, "secret2").unwrap();

        assert_ne!(token1, token2, "Different secrets should produce different tokens");
    }

    #[test]
    fn test_generate_jwt_different_claims() {
        // Test that different claims produce different tokens
        let claims1 = Claims {
            sub: "user1".to_string(),
            company: "example".to_string(),
            exp: 9999999999, // Far future timestamp for testing
        };
        let claims2 = Claims {
            sub: "user2".to_string(),
            company: "example".to_string(),
            exp: 9999999999, // Far future timestamp for testing
        };

        let secret = "my_secret";
        let token1 = generate_jwt(&claims1, secret).unwrap();
        let token2 = generate_jwt(&claims2, secret).unwrap();

        assert_ne!(token1, token2, "Different claims should produce different tokens");
    }

    #[test]
    fn test_extract_claims_valid_token() {
        // Test JWT claims extraction with valid token
        let original_claims = Claims {
            sub: "user1".to_string(),
            company: "example".to_string(),
            exp: 9999999999, // Far future timestamp for testing
        };
        let secret = "my_secret";
        let token = generate_jwt(&original_claims, secret).unwrap();

        let decoded = extract_claims::<Claims>(&token, secret).unwrap();
        assert_eq!(decoded.claims, original_claims, "Decoded claims should match original claims");
    }

    #[test]
    fn test_extract_claims_invalid_secret() {
        // Test JWT claims extraction with wrong secret
        let claims = Claims {
            sub: "user1".to_string(),
            company: "example".to_string(),
            exp: 9999999999, // Far future timestamp for testing
        };
        let token = generate_jwt(&claims, "correct_secret").unwrap();

        let result = extract_claims::<Claims>(&token, "wrong_secret");
        assert!(result.is_err(), "Should fail with wrong secret");
    }

    #[test]
    fn test_extract_claims_invalid_token() {
        // Test JWT claims extraction with invalid token format
        let invalid_tokens = [
            "invalid.token",
            "not.a.jwt.token",
            "",
            "header.payload", // Missing signature
            "invalid_base64.invalid_base64.invalid_base64",
        ];

        for invalid_token in &invalid_tokens {
            let result = extract_claims::<Claims>(invalid_token, "secret");
            assert!(result.is_err(), "Should fail with invalid token: {invalid_token}");
        }
    }

    #[test]
    fn test_jwt_round_trip_consistency() {
        // Test complete round-trip: generate -> extract -> verify
        let original_claims = Claims {
            sub: "test_user".to_string(),
            company: "test_company".to_string(),
            exp: 9999999999, // Far future timestamp for testing
        };
        let secret = "test_secret_key";

        // Generate token
        let token = generate_jwt(&original_claims, secret).unwrap();

        // Extract claims
        let decoded = extract_claims::<Claims>(&token, secret).unwrap();

        // Verify claims match
        assert_eq!(decoded.claims, original_claims);

        // Verify token data structure
        assert!(matches!(decoded.header.alg, jsonwebtoken::Algorithm::HS512));
    }

    #[test]
    fn test_jwt_with_empty_claims() {
        // Test JWT with minimal claims
        let empty_claims = Claims {
            sub: String::new(),
            company: String::new(),
            exp: 9999999999, // Far future timestamp for testing
        };
        let secret = "secret";

        let token = generate_jwt(&empty_claims, secret).unwrap();
        let decoded = extract_claims::<Claims>(&token, secret).unwrap();

        assert_eq!(decoded.claims, empty_claims);
    }

    #[test]
    fn test_jwt_with_special_characters() {
        // Test JWT with special characters in claims
        let special_claims = Claims {
            sub: "user@example.com".to_string(),
            company: "Company & Co. (Ltd.)".to_string(),
            exp: 9999999999, // Far future timestamp for testing
        };
        let secret = "secret_with_special_chars!@#$%";

        let token = generate_jwt(&special_claims, secret).unwrap();
        let decoded = extract_claims::<Claims>(&token, secret).unwrap();

        assert_eq!(decoded.claims, special_claims);
    }

    #[test]
    fn test_access_key_length_boundaries() {
        // Test boundary conditions for access key length
        assert!(gen_access_key(3).is_ok(), "Length 3 should be valid (minimum)");
        assert!(gen_access_key(1000).is_ok(), "Large length should be valid");

        // Test that minimum length is enforced
        let min_key = gen_access_key(3).unwrap();
        assert_eq!(min_key.len(), 3);
    }

    #[test]
    fn test_secret_key_length_boundaries() {
        // Test boundary conditions for secret key length
        assert!(gen_secret_key(8).is_ok(), "Length 8 should be valid (minimum)");
        assert!(gen_secret_key(1000).is_ok(), "Large length should be valid");

        // Test that minimum length is enforced
        let result = gen_secret_key(8);
        assert!(result.is_ok(), "Minimum valid length should work");
    }
}
