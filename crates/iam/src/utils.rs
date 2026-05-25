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
use serde::{Serialize, de::DeserializeOwned};
use std::collections::HashSet;

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

pub fn extract_claims_allow_missing_exp<T: DeserializeOwned + Clone>(
    token: &str,
    secret: &str,
) -> std::result::Result<jsonwebtoken::TokenData<T>, jsonwebtoken::errors::Error> {
    let mut validation = jsonwebtoken::Validation::new(Algorithm::HS512);
    validation.required_spec_claims = HashSet::new();

    jsonwebtoken::decode::<T>(token, &DecodingKey::from_secret(secret.as_bytes()), &validation)
}

#[cfg(test)]
mod tests {
    use super::{extract_claims, generate_jwt};
    use serde::{Deserialize, Serialize};

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
}
