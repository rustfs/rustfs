use serde_json::json;
use time::OffsetDateTime;

use super::{decode::decode, encode::encode};

#[test]
fn test_jwt_encode_decode_basic() {
    let claims = json!({
        "exp": OffsetDateTime::now_utc().unix_timestamp() + 1000,
        "sub": "user123",
        "iat": OffsetDateTime::now_utc().unix_timestamp(),
        "role": "admin"
    });

    let secret = b"test_secret_key";
    let jwt_token = encode(secret, &claims).expect("Failed to encode JWT");

    let decoded = decode(&jwt_token, secret).expect("Failed to decode JWT");
    assert_eq!(decoded.claims, claims);
}

#[test]
fn test_jwt_encode_decode_with_complex_claims() {
    let claims = json!({
        "exp": OffsetDateTime::now_utc().unix_timestamp() + 3600,
        "sub": "user456",
        "iat": OffsetDateTime::now_utc().unix_timestamp(),
        "permissions": ["read", "write", "delete"],
        "metadata": {
            "department": "engineering",
            "level": 5,
            "active": true
        },
        "custom_field": null
    });

    let secret = b"complex_secret_key_123";
    let jwt_token = encode(secret, &claims).expect("Failed to encode complex JWT");

    let decoded = decode(&jwt_token, secret).expect("Failed to decode complex JWT");
    assert_eq!(decoded.claims, claims);
}

#[test]
fn test_jwt_decode_with_wrong_secret() {
    let claims = json!({
        "exp": OffsetDateTime::now_utc().unix_timestamp() + 1000,
        "sub": "user123"
    });

    let correct_secret = b"correct_secret";
    let wrong_secret = b"wrong_secret";

    let jwt_token = encode(correct_secret, &claims).expect("Failed to encode JWT");

    // Decoding with wrong secret should fail
    let result = decode(&jwt_token, wrong_secret);
    assert!(result.is_err(), "Decoding with wrong secret should fail");
}

#[test]
fn test_jwt_decode_invalid_token_format() {
    let secret = b"test_secret";

    // Test various invalid token formats
    let invalid_tokens = [
        "",                                                       // Empty token
        "invalid",                                                // Not a JWT format
        "header.payload",                                         // Missing signature
        "header.payload.signature.extra",                         // Too many parts
        "invalid.header.signature",                               // Invalid base64
        "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.invalid.signature", // Invalid payload
    ];

    for invalid_token in &invalid_tokens {
        let result = decode(invalid_token, secret);
        assert!(result.is_err(), "Invalid token '{}' should fail to decode", invalid_token);
    }
}

#[test]
fn test_jwt_with_expired_token() {
    let expired_claims = json!({
        "exp": OffsetDateTime::now_utc().unix_timestamp() - 1000, // Expired 1000 seconds ago
        "sub": "user123",
        "iat": OffsetDateTime::now_utc().unix_timestamp() - 2000
    });

    let secret = b"test_secret";
    let jwt_token = encode(secret, &expired_claims).expect("Failed to encode expired JWT");

    // Decoding expired token should fail
    let result = decode(&jwt_token, secret);
    assert!(result.is_err(), "Expired token should fail to decode");
}

#[test]
fn test_jwt_with_future_issued_at() {
    let future_claims = json!({
        "exp": OffsetDateTime::now_utc().unix_timestamp() + 3600,
        "sub": "user123",
        "iat": OffsetDateTime::now_utc().unix_timestamp() + 1000 // Issued in future
    });

    let secret = b"test_secret";
    let jwt_token = encode(secret, &future_claims).expect("Failed to encode future JWT");

    // Note: The current JWT implementation may not validate iat by default
    // This test documents the current behavior - future iat tokens may still decode successfully
    let result = decode(&jwt_token, secret);
    // For now, we just verify the token can be decoded, but in a production system
    // you might want to add custom validation for iat claims
    assert!(
        result.is_ok(),
        "Token decoding should succeed, but iat validation should be handled separately"
    );
}

#[test]
fn test_jwt_with_empty_claims() {
    let empty_claims = json!({
        "exp": OffsetDateTime::now_utc().unix_timestamp() + 1000, // Add required exp claim
    });
    let secret = b"test_secret";

    let jwt_token = encode(secret, &empty_claims).expect("Failed to encode empty claims JWT");
    let decoded = decode(&jwt_token, secret).expect("Failed to decode empty claims JWT");

    assert_eq!(decoded.claims, empty_claims);
}

#[test]
fn test_jwt_with_different_secret_lengths() {
    let claims = json!({
        "exp": OffsetDateTime::now_utc().unix_timestamp() + 1000,
        "sub": "user123"
    });

    // Test with various secret lengths
    let secrets = [
        b"a".as_slice(),                                                              // Very short
        b"short_key".as_slice(),                                                      // Short
        b"medium_length_secret_key".as_slice(),                                       // Medium
        b"very_long_secret_key_with_many_characters_for_testing_purposes".as_slice(), // Long
    ];

    for secret in &secrets {
        let jwt_token =
            encode(secret, &claims).unwrap_or_else(|_| panic!("Failed to encode JWT with secret length {}", secret.len()));

        let decoded =
            decode(&jwt_token, secret).unwrap_or_else(|_| panic!("Failed to decode JWT with secret length {}", secret.len()));

        assert_eq!(decoded.claims, claims);
    }
}

#[test]
fn test_jwt_with_special_characters_in_claims() {
    let claims = json!({
        "exp": OffsetDateTime::now_utc().unix_timestamp() + 1000,
        "sub": "user@example.com",
        "name": "John Doe",
        "description": "User with special chars: !@#$%^&*()_+-=[]{}|;':\",./<>?",
        "unicode": "测试用户 🚀 émojis",
        "newlines": "line1\nline2\r\nline3",
        "quotes": "He said \"Hello\" and she replied 'Hi'"
    });

    let secret = b"test_secret";
    let jwt_token = encode(secret, &claims).expect("Failed to encode JWT with special characters");

    let decoded = decode(&jwt_token, secret).expect("Failed to decode JWT with special characters");
    assert_eq!(decoded.claims, claims);
}

#[test]
fn test_jwt_with_large_payload() {
    // Create a large payload to test size limits
    let large_data = "x".repeat(10000); // 10KB of data
    let claims = json!({
        "exp": OffsetDateTime::now_utc().unix_timestamp() + 1000,
        "sub": "user123",
        "large_field": large_data
    });

    let secret = b"test_secret";
    let jwt_token = encode(secret, &claims).expect("Failed to encode large JWT");

    let decoded = decode(&jwt_token, secret).expect("Failed to decode large JWT");
    assert_eq!(decoded.claims, claims);
}

#[test]
fn test_jwt_token_structure() {
    let claims = json!({
        "exp": OffsetDateTime::now_utc().unix_timestamp() + 1000,
        "sub": "user123"
    });

    let secret = b"test_secret";
    let jwt_token = encode(secret, &claims).expect("Failed to encode JWT");

    // JWT should have exactly 3 parts separated by dots
    let parts: Vec<&str> = jwt_token.split('.').collect();
    assert_eq!(parts.len(), 3, "JWT should have exactly 3 parts");

    // Each part should be non-empty
    for (i, part) in parts.iter().enumerate() {
        assert!(!part.is_empty(), "JWT part {} should not be empty", i);
    }
}

#[test]
fn test_jwt_deterministic_encoding() {
    let claims = json!({
        "exp": 1234567890,  // Fixed timestamp for deterministic test
        "sub": "user123",
        "iat": 1234567800
    });

    let secret = b"test_secret";

    // Encode the same claims multiple times
    let token1 = encode(secret, &claims).expect("Failed to encode JWT 1");
    let token2 = encode(secret, &claims).expect("Failed to encode JWT 2");

    // Tokens should be identical for same input
    assert_eq!(token1, token2, "JWT encoding should be deterministic");
}

#[test]
fn test_jwt_cross_compatibility() {
    let claims = json!({
        "exp": OffsetDateTime::now_utc().unix_timestamp() + 1000,
        "sub": "user123"
    });

    let secret1 = b"secret1";
    let secret2 = b"secret2";

    // Encode with secret1
    let token1 = encode(secret1, &claims).expect("Failed to encode with secret1");

    // Decode with secret1 should work
    let decoded1 = decode(&token1, secret1).expect("Failed to decode with correct secret");
    assert_eq!(decoded1.claims, claims);

    // Decode with secret2 should fail
    let result2 = decode(&token1, secret2);
    assert!(result2.is_err(), "Decoding with different secret should fail");
}

#[test]
fn test_jwt_header_algorithm() {
    let claims = json!({
        "exp": OffsetDateTime::now_utc().unix_timestamp() + 1000,
        "sub": "user123"
    });

    let secret = b"test_secret";
    let jwt_token = encode(secret, &claims).expect("Failed to encode JWT");

    let decoded = decode(&jwt_token, secret).expect("Failed to decode JWT");

    // Verify the algorithm in header is HS512
    assert_eq!(decoded.header.alg, jsonwebtoken::Algorithm::HS512);
    assert_eq!(decoded.header.typ, Some("JWT".to_string()));
}

#[test]
fn test_jwt_claims_validation() {
    let now = OffsetDateTime::now_utc().unix_timestamp();

    let valid_claims = json!({
        "exp": now + 3600,  // Expires in 1 hour
        "iat": now - 60,    // Issued 1 minute ago
        "nbf": now - 30,    // Not before 30 seconds ago
        "sub": "user123"
    });

    let secret = b"test_secret";
    let jwt_token = encode(secret, &valid_claims).expect("Failed to encode valid JWT");

    let decoded = decode(&jwt_token, secret).expect("Failed to decode valid JWT");
    assert_eq!(decoded.claims, valid_claims);
}

#[test]
fn test_jwt_with_numeric_claims() {
    let claims = json!({
        "exp": OffsetDateTime::now_utc().unix_timestamp() + 1000,
        "sub": "user123",
        "age": 25,
        "score": 95.5,
        "count": 0,
        "negative": -10
    });

    let secret = b"test_secret";
    let jwt_token = encode(secret, &claims).expect("Failed to encode numeric JWT");

    let decoded = decode(&jwt_token, secret).expect("Failed to decode numeric JWT");
    assert_eq!(decoded.claims, claims);
}

#[test]
fn test_jwt_with_boolean_claims() {
    let claims = json!({
        "exp": OffsetDateTime::now_utc().unix_timestamp() + 1000,
        "sub": "user123",
        "is_admin": true,
        "is_active": false,
        "email_verified": true
    });

    let secret = b"test_secret";
    let jwt_token = encode(secret, &claims).expect("Failed to encode boolean JWT");

    let decoded = decode(&jwt_token, secret).expect("Failed to decode boolean JWT");
    assert_eq!(decoded.claims, claims);
}

#[test]
fn test_jwt_with_array_claims() {
    let claims = json!({
        "exp": OffsetDateTime::now_utc().unix_timestamp() + 1000,
        "sub": "user123",
        "roles": ["admin", "user", "moderator"],
        "permissions": [1, 2, 3, 4, 5],
        "tags": [],
        "mixed_array": ["string", 123, true, null]
    });

    let secret = b"test_secret";
    let jwt_token = encode(secret, &claims).expect("Failed to encode array JWT");

    let decoded = decode(&jwt_token, secret).expect("Failed to decode array JWT");
    assert_eq!(decoded.claims, claims);
}
