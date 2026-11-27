use crate::core::{
    CoreJsonWebKey, CoreJweContentEncryptionAlgorithm, CoreJwsSigningAlgorithm,
    CoreRsaPrivateSigningKey,
};
use crate::jwt::{
    InvalidJsonWebTokenTypeError, JsonWebToken, JsonWebTokenAccess, JsonWebTokenAlgorithm,
    JsonWebTokenJsonPayloadSerde, JsonWebTokenPayloadSerde,
};
use crate::{JsonWebKeyId, JsonWebTokenType};

use serde::{Deserialize, Serialize};

use std::string::ToString;

type CoreAlgorithm =
    JsonWebTokenAlgorithm<CoreJweContentEncryptionAlgorithm, CoreJwsSigningAlgorithm>;

pub const TEST_JWT: &str =
    "eyJhbGciOiJSUzI1NiIsImtpZCI6ImJpbGJvLmJhZ2dpbnNAaG9iYml0b24uZXhhbXBsZSJ9.SXTigJlzIGEgZ\
         GFuZ2Vyb3VzIGJ1c2luZXNzLCBGcm9kbywgZ29pbmcgb3V0IHlvdXIgZG9vci4gWW91IHN0ZXAgb250byB0aGU\
         gcm9hZCwgYW5kIGlmIHlvdSBkb24ndCBrZWVwIHlvdXIgZmVldCwgdGhlcmXigJlzIG5vIGtub3dpbmcgd2hlc\
         mUgeW91IG1pZ2h0IGJlIHN3ZXB0IG9mZiB0by4.MRjdkly7_-oTPTS3AXP41iQIGKa80A0ZmTuV5MEaHoxnW2e\
         5CZ5NlKtainoFmKZopdHM1O2U4mwzJdQx996ivp83xuglII7PNDi84wnB-BDkoBwA78185hX-Es4JIwmDLJK3l\
         fWRa-XtL0RnltuYv746iYTh_qHRD68BNt1uSNCrUCTJDt5aAE6x8wW1Kt9eRo4QPocSadnHXFxnt8Is9UzpERV\
         0ePPQdLuW3IS_de3xyIrDaLGdjluPxUAhb6L2aXic1U12podGU0KLUQSE_oI-ZnmKJ3F4uOZDnd6QZWJushZ41\
         Axf_fcIe8u9ipH84ogoree7vjbU5y18kDquDg";

const TEST_JWT_PAYLOAD: &str = "It\u{2019}s a dangerous business, Frodo, going out your \
                                    door. You step onto the road, and if you don't keep your feet, \
                                    there\u{2019}s no knowing where you might be swept off \
                                    to.";

pub const TEST_RSA_PUB_KEY: &str = "{
            \"kty\": \"RSA\",
            \"kid\": \"bilbo.baggins@hobbiton.example\",
            \"use\": \"sig\",
            \"n\": \"n4EPtAOCc9AlkeQHPzHStgAbgs7bTZLwUBZdR8_KuKPEHLd4rHVTeT\
                     -O-XV2jRojdNhxJWTDvNd7nqQ0VEiZQHz_AJmSCpMaJMRBSFKrKb2wqV\
                     wGU_NsYOYL-QtiWN2lbzcEe6XC0dApr5ydQLrHqkHHig3RBordaZ6Aj-\
                     oBHqFEHYpPe7Tpe-OfVfHd1E6cS6M1FZcD1NNLYD5lFHpPI9bTwJlsde\
                     3uhGqC0ZCuEHg8lhzwOHrtIQbS0FVbb9k3-tVTU4fg_3L_vniUFAKwuC\
                     LqKnS2BYwdq_mzSnbLY7h_qixoR7jig3__kRhuaxwUkRz5iaiQkqgc5g\
                     HdrNP5zw\",
            \"e\": \"AQAB\"
        }";

pub const TEST_ED_PUB_KEY_ED25519: &str = r#"{
        "kty": "OKP",
        "use": "sig",
        "alg": "EdDSA",
        "crv": "Ed25519",
        "x": "sfliRRhciU_d5qsuC5Vcydi-t8bRfxTg_4qulVatW4A"
    }"#;

pub const TEST_EC_PUB_KEY_P256: &str = r#"{
        "kty": "EC",
        "kid": "bilbo.baggins@hobbiton.example",
        "use": "sig",
        "crv": "P-256",
        "x": "t6PHivOTggpaX9lkMkis2p8kMhy-CktJAFTz6atReZw",
        "y": "ODobXupKlD0DeM1yRd7bX4XFNBO1HOgCT1UCu0KY3lc"
    }"#;
pub const TEST_EC_PUB_KEY_P384: &str = r#"{
        "kty": "EC",
        "kid": "bilbo.baggins@hobbiton.example",
        "use": "sig",
        "crv" : "P-384",
        "x": "9ywsUbxX59kJXFRiWHcx97wRKNiF8Hc9F5wI08n8h2ek_qAl0veEc36k1Qz6KLiL",
        "y": "6PWlqjRbaV7V8ohDscM243IneuLZmxDGLiGNA1w69fQhEDsvZtKLUQ5KiHLgR3op"
    }"#;

// This is the PEM form of the test private key from:
// https://tools.ietf.org/html/rfc7520#section-3.4
pub const TEST_RSA_PRIV_KEY: &str = "-----BEGIN RSA PRIVATE KEY-----\n\
         MIIEowIBAAKCAQEAn4EPtAOCc9AlkeQHPzHStgAbgs7bTZLwUBZdR8/KuKPEHLd4\n\
         rHVTeT+O+XV2jRojdNhxJWTDvNd7nqQ0VEiZQHz/AJmSCpMaJMRBSFKrKb2wqVwG\n\
         U/NsYOYL+QtiWN2lbzcEe6XC0dApr5ydQLrHqkHHig3RBordaZ6Aj+oBHqFEHYpP\n\
         e7Tpe+OfVfHd1E6cS6M1FZcD1NNLYD5lFHpPI9bTwJlsde3uhGqC0ZCuEHg8lhzw\n\
         OHrtIQbS0FVbb9k3+tVTU4fg/3L/vniUFAKwuCLqKnS2BYwdq/mzSnbLY7h/qixo\n\
         R7jig3//kRhuaxwUkRz5iaiQkqgc5gHdrNP5zwIDAQABAoIBAG1lAvQfhBUSKPJK\n\
         Rn4dGbshj7zDSr2FjbQf4pIh/ZNtHk/jtavyO/HomZKV8V0NFExLNi7DUUvvLiW7\n\
         0PgNYq5MDEjJCtSd10xoHa4QpLvYEZXWO7DQPwCmRofkOutf+NqyDS0QnvFvp2d+\n\
         Lov6jn5C5yvUFgw6qWiLAPmzMFlkgxbtjFAWMJB0zBMy2BqjntOJ6KnqtYRMQUxw\n\
         TgXZDF4rhYVKtQVOpfg6hIlsaoPNrF7dofizJ099OOgDmCaEYqM++bUlEHxgrIVk\n\
         wZz+bg43dfJCocr9O5YX0iXaz3TOT5cpdtYbBX+C/5hwrqBWru4HbD3xz8cY1TnD\n\
         qQa0M8ECgYEA3Slxg/DwTXJcb6095RoXygQCAZ5RnAvZlno1yhHtnUex/fp7AZ/9\n\
         nRaO7HX/+SFfGQeutao2TDjDAWU4Vupk8rw9JR0AzZ0N2fvuIAmr/WCsmGpeNqQn\n\
         ev1T7IyEsnh8UMt+n5CafhkikzhEsrmndH6LxOrvRJlsPp6Zv8bUq0kCgYEAuKE2\n\
         dh+cTf6ERF4k4e/jy78GfPYUIaUyoSSJuBzp3Cubk3OCqs6grT8bR/cu0Dm1MZwW\n\
         mtdqDyI95HrUeq3MP15vMMON8lHTeZu2lmKvwqW7anV5UzhM1iZ7z4yMkuUwFWoB\n\
         vyY898EXvRD+hdqRxHlSqAZ192zB3pVFJ0s7pFcCgYAHw9W9eS8muPYv4ZhDu/fL\n\
         2vorDmD1JqFcHCxZTOnX1NWWAj5hXzmrU0hvWvFC0P4ixddHf5Nqd6+5E9G3k4E5\n\
         2IwZCnylu3bqCWNh8pT8T3Gf5FQsfPT5530T2BcsoPhUaeCnP499D+rb2mTnFYeg\n\
         mnTT1B/Ue8KGLFFfn16GKQKBgAiw5gxnbocpXPaO6/OKxFFZ+6c0OjxfN2PogWce\n\
         TU/k6ZzmShdaRKwDFXisxRJeNQ5Rx6qgS0jNFtbDhW8E8WFmQ5urCOqIOYk28EBi\n\
         At4JySm4v+5P7yYBh8B8YD2l9j57z/s8hJAxEbn/q8uHP2ddQqvQKgtsni+pHSk9\n\
         XGBfAoGBANz4qr10DdM8DHhPrAb2YItvPVz/VwkBd1Vqj8zCpyIEKe/07oKOvjWQ\n\
         SgkLDH9x2hBgY01SbP43CvPk0V72invu2TGkI/FXwXWJLLG7tDSgw4YyfhrYrHmg\n\
         1Vre3XB9HH8MYBVB6UIexaAq4xSeoemRKTBesZro7OKjKT8/GmiO\n\
         -----END RSA PRIVATE KEY-----";

#[test]
fn test_jwt_algorithm_deserialization() {
    assert_eq!(
        serde_json::from_str::<CoreAlgorithm>("\"A128CBC-HS256\"").expect("failed to deserialize"),
        JsonWebTokenAlgorithm::Encryption(CoreJweContentEncryptionAlgorithm::Aes128CbcHmacSha256),
    );
    assert_eq!(
        serde_json::from_str::<CoreAlgorithm>("\"A128GCM\"").expect("failed to deserialize"),
        JsonWebTokenAlgorithm::Encryption(CoreJweContentEncryptionAlgorithm::Aes128Gcm),
    );
    assert_eq!(
        serde_json::from_str::<CoreAlgorithm>("\"HS256\"").expect("failed to deserialize"),
        JsonWebTokenAlgorithm::Signature(CoreJwsSigningAlgorithm::HmacSha256),
    );
    assert_eq!(
        serde_json::from_str::<CoreAlgorithm>("\"RS256\"").expect("failed to deserialize"),
        JsonWebTokenAlgorithm::Signature(CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256),
    );
    assert_eq!(
        serde_json::from_str::<CoreAlgorithm>("\"none\"").expect("failed to deserialize"),
        JsonWebTokenAlgorithm::None,
    );

    serde_json::from_str::<CoreAlgorithm>("\"invalid\"")
        .expect_err("deserialization should have failed");
}

#[test]
fn test_jwt_algorithm_serialization() {
    assert_eq!(
        serde_json::to_string::<CoreAlgorithm>(&JsonWebTokenAlgorithm::Encryption(
            CoreJweContentEncryptionAlgorithm::Aes128CbcHmacSha256
        ))
        .expect("failed to serialize"),
        "\"A128CBC-HS256\"",
    );
    assert_eq!(
        serde_json::to_string::<CoreAlgorithm>(&JsonWebTokenAlgorithm::Encryption(
            CoreJweContentEncryptionAlgorithm::Aes128Gcm
        ))
        .expect("failed to serialize"),
        "\"A128GCM\"",
    );
    assert_eq!(
        serde_json::to_string::<CoreAlgorithm>(&JsonWebTokenAlgorithm::Signature(
            CoreJwsSigningAlgorithm::HmacSha256
        ))
        .expect("failed to serialize"),
        "\"HS256\"",
    );
    assert_eq!(
        serde_json::to_string::<CoreAlgorithm>(&JsonWebTokenAlgorithm::Signature(
            CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256
        ))
        .expect("failed to serialize"),
        "\"RS256\"",
    );
    assert_eq!(
        serde_json::to_string::<CoreAlgorithm>(&JsonWebTokenAlgorithm::None)
            .expect("failed to serialize"),
        "\"none\"",
    );
}

#[derive(Clone, Debug)]
pub struct JsonWebTokenStringPayloadSerde;
impl JsonWebTokenPayloadSerde<String> for JsonWebTokenStringPayloadSerde {
    fn deserialize<DE: serde::de::Error>(payload: &[u8]) -> Result<String, DE> {
        Ok(String::from_utf8(payload.to_owned()).unwrap())
    }
    fn serialize(payload: &String) -> Result<String, serde_json::Error> {
        Ok(payload.to_string())
    }
}

#[test]
fn test_jwt_basic() {
    fn verify_jwt<A>(jwt_access: A, key: &CoreJsonWebKey, expected_payload: &str)
    where
        A: JsonWebTokenAccess<CoreJweContentEncryptionAlgorithm, CoreJwsSigningAlgorithm, String>,
        A::ReturnType: ToString,
    {
        {
            let header = jwt_access.unverified_header();
            assert_eq!(
                header.alg,
                JsonWebTokenAlgorithm::Signature(CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256)
            );
            assert_eq!(header.crit, None);
            assert_eq!(header.cty, None);
            assert_eq!(
                header.kid,
                Some(JsonWebKeyId::new(
                    "bilbo.baggins@hobbiton.example".to_string()
                ))
            );
            assert_eq!(header.typ, None);
        }
        assert_eq!(jwt_access.unverified_payload_ref(), expected_payload);

        assert_eq!(
            jwt_access
                .payload(&CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256, key)
                .expect("failed to validate payload")
                .to_string(),
            expected_payload
        );
    }

    let key: CoreJsonWebKey =
        serde_json::from_str(TEST_RSA_PUB_KEY).expect("deserialization failed");

    let jwt: JsonWebToken<
        CoreJweContentEncryptionAlgorithm,
        CoreJwsSigningAlgorithm,
        String,
        JsonWebTokenStringPayloadSerde,
    > = serde_json::from_value(serde_json::Value::String(TEST_JWT.to_string()))
        .expect("failed to deserialize");

    assert_eq!(
        serde_json::to_value(&jwt).expect("failed to serialize"),
        serde_json::Value::String(TEST_JWT.to_string())
    );

    verify_jwt(&jwt, &key, TEST_JWT_PAYLOAD);
    assert_eq!((&jwt).unverified_payload(), TEST_JWT_PAYLOAD);

    verify_jwt(jwt, &key, TEST_JWT_PAYLOAD);
}

#[test]
fn test_new_jwt() {
    let signing_key = CoreRsaPrivateSigningKey::from_pem(
        TEST_RSA_PRIV_KEY,
        Some(JsonWebKeyId::new(
            "bilbo.baggins@hobbiton.example".to_string(),
        )),
    )
    .unwrap();
    let new_jwt = JsonWebToken::<
        CoreJweContentEncryptionAlgorithm,
        _,
        _,
        JsonWebTokenStringPayloadSerde,
    >::new(
        TEST_JWT_PAYLOAD.to_owned(),
        &signing_key,
        &CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256,
    )
    .unwrap();
    assert_eq!(
        serde_json::to_value(new_jwt).expect("failed to serialize"),
        serde_json::Value::String(TEST_JWT.to_string())
    );
}

#[test]
fn test_invalid_signature() {
    let corrupted_jwt_str = TEST_JWT
        .to_string()
        .chars()
        .take(TEST_JWT.len() - 1)
        .collect::<String>()
        + "f";
    let jwt: JsonWebToken<
        CoreJweContentEncryptionAlgorithm,
        CoreJwsSigningAlgorithm,
        String,
        JsonWebTokenStringPayloadSerde,
    > = serde_json::from_value(serde_json::Value::String(corrupted_jwt_str))
        .expect("failed to deserialize");
    let key: CoreJsonWebKey =
        serde_json::from_str(TEST_RSA_PUB_KEY).expect("deserialization failed");

    // JsonWebTokenAccess for reference.
    (&jwt)
        .payload(&CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256, &key)
        .expect_err("signature verification should have failed");

    // JsonWebTokenAccess for owned value.
    jwt.payload(&CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256, &key)
        .expect_err("signature verification should have failed");
}

#[test]
fn test_invalid_deserialization() {
    #[derive(Debug, Deserialize, Serialize)]
    struct TestPayload {
        foo: String,
    }

    fn expect_deserialization_err<I: Into<String>>(jwt_str: I, pattern: &str) {
        let err = serde_json::from_value::<
            JsonWebToken<
                CoreJweContentEncryptionAlgorithm,
                CoreJwsSigningAlgorithm,
                TestPayload,
                JsonWebTokenJsonPayloadSerde,
            >,
        >(serde_json::Value::String(jwt_str.into()))
        .expect_err("deserialization should have failed");

        assert!(
            err.to_string().contains(pattern),
            "Error `{err}` must contain string `{pattern}`",
        );
    }

    // Too many dots
    expect_deserialization_err("a.b.c.d", "found 4 parts (expected 3)");

    // Invalid header base64
    expect_deserialization_err("a!.b.c", "Invalid base64url header encoding");

    // Invalid header utf-8 (after base64 decoding)
    expect_deserialization_err("gA.b.c", "Error(\"expected value\", line: 1, column: 1)");

    // Invalid header JSON
    expect_deserialization_err("bm90X2pzb24.b.c", "Failed to parse header JSON");

    let valid_header = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImJpbGJvLmJhZ2dpbnNAaG9iYml0b24uZXhhbXBsZSJ9";

    // Invalid payload base64
    expect_deserialization_err(
        format!("{valid_header}.b!.c"),
        "Invalid base64url payload encoding",
    );

    // Invalid payload utf-8 (after base64 decoding)
    expect_deserialization_err(
        format!("{valid_header}.gA.c"),
        "Error(\"expected value\", line: 1, column: 1)",
    );

    // Invalid payload JSON
    expect_deserialization_err(
        format!("{valid_header}.bm90X2pzb24.c"),
        "Failed to parse payload JSON",
    );

    let valid_body = "eyJmb28iOiAiYmFyIn0";

    // Invalid signature base64
    expect_deserialization_err(
        format!("{valid_header}.{valid_body}.c!"),
        "Invalid base64url signature encoding",
    );

    let deserialized = serde_json::from_value::<
        JsonWebToken<
            CoreJweContentEncryptionAlgorithm,
            CoreJwsSigningAlgorithm,
            TestPayload,
            JsonWebTokenJsonPayloadSerde,
        >,
    >(serde_json::Value::String(format!(
        "{valid_header}.{valid_body}.e2FiY30"
    )))
    .expect("failed to deserialize");
    assert_eq!(deserialized.unverified_payload().foo, "bar");
}

#[test]
fn test_json_web_token_type_normalization() {
    fn assert_token_type_normalization(
        jwt_type_string: &str,
        expected_normalized_jwt_type_string: &str,
    ) -> Result<(), InvalidJsonWebTokenTypeError> {
        let jwt_type = JsonWebTokenType::new(jwt_type_string.to_string());
        let normalized_jwt_type = jwt_type.normalize()?;

        assert_eq!(*normalized_jwt_type, expected_normalized_jwt_type_string);
        Ok(())
    }

    assert_token_type_normalization("jwt", "application/jwt").unwrap();
    assert_token_type_normalization("jwt;arg=some", "application/jwt;arg=some").unwrap();
    assert!(assert_token_type_normalization("jwt;arg=some/other", "").is_err());
    assert!(assert_token_type_normalization("/jwt;arg=some/other", "").is_err());
    assert!(assert_token_type_normalization("application/;arg=some/other", "").is_err());
    assert_token_type_normalization("application/jwt", "application/jwt").unwrap();
    assert_token_type_normalization(
        "application/jwt;arg=some/other",
        "application/jwt;arg=some/other",
    )
    .unwrap();
    assert_token_type_normalization("special/type", "special/type").unwrap();
    assert_token_type_normalization("special/type;arg=some", "special/type;arg=some").unwrap();
    assert_token_type_normalization("s/t;arg=some/o", "s/t;arg=some/o").unwrap();
}
