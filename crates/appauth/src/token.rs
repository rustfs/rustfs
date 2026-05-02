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

use rsa::{
    Pkcs1v15Encrypt, RsaPrivateKey, RsaPublicKey,
    pkcs8::{DecodePrivateKey, DecodePublicKey},
    pss::{BlindedSigningKey, Signature, VerifyingKey},
    sha2::Sha256,
    signature::{RandomizedSigner, Verifier},
    traits::PublicKeyParts,
};
use serde::{Deserialize, Serialize};
use std::io::{Error, ErrorKind, Result};

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Token {
    pub name: String, // Application ID
    pub expired: u64, // Expiry time (UNIX timestamp)
}

/// Legacy public-key encryption Token encoder.
///
/// Use `sign_license_token` for license issuance so verifiers only need a
/// public key.
#[deprecated(note = "use sign_license_token for signed license issuance")]
pub fn gencode(token: &Token, key: &str) -> Result<String> {
    let data = serde_json::to_vec(token)?;
    let mut rng = rand::rng();
    let public_key = RsaPublicKey::from_public_key_pem(key).map_err(Error::other)?;
    let encrypted_data = public_key.encrypt(&mut rng, Pkcs1v15Encrypt, &data).map_err(Error::other)?;
    Ok(base64_simd::URL_SAFE_NO_PAD.encode_to_string(&encrypted_data))
}

/// Legacy private-key Token decoder.
///
/// Use `parse_signed_license_token` or `parse_license_with_public_key` for
/// license verification so runtime services never need private key material.
#[deprecated(note = "use parse_signed_license_token or parse_license_with_public_key for signed license verification")]
pub fn parse(token: &str, key: &str) -> Result<Token> {
    let encrypted_data = base64_simd::URL_SAFE_NO_PAD
        .decode_to_vec(token.as_bytes())
        .map_err(Error::other)?;
    let private_key = RsaPrivateKey::from_pkcs8_pem(key).map_err(Error::other)?;
    let decrypted_data = private_key.decrypt(Pkcs1v15Encrypt, &encrypted_data).map_err(Error::other)?;
    serde_json::from_slice(&decrypted_data).map_err(Error::other)
}

/// Signs a license token with an RSA private key.
///
/// The returned token is base64url(signature || payload), where the signature is
/// RSASSA-PSS over the JSON payload using SHA-256.
pub fn sign_license_token(token: &Token, private_key_pem: &str) -> Result<String> {
    let payload = serde_json::to_vec(token)?;
    let mut rng = rand::rng();
    let private_key = RsaPrivateKey::from_pkcs8_pem(private_key_pem).map_err(Error::other)?;
    let signing_key = BlindedSigningKey::<Sha256>::new(private_key);
    let signature: Signature = signing_key.try_sign_with_rng(&mut rng, &payload).map_err(Error::other)?;
    let signature: Box<[u8]> = signature.into();

    let mut signed_payload = Vec::with_capacity(signature.as_ref().len() + payload.len());
    signed_payload.extend_from_slice(signature.as_ref());
    signed_payload.extend_from_slice(&payload);

    Ok(base64_simd::URL_SAFE_NO_PAD.encode_to_string(&signed_payload))
}

/// Verifies and parses a signed license token with an RSA public key.
pub fn parse_signed_license_token(token: &str, public_key_pem: &str) -> Result<Token> {
    let signed_payload = base64_simd::URL_SAFE_NO_PAD
        .decode_to_vec(token.as_bytes())
        .map_err(Error::other)?;
    let public_key = RsaPublicKey::from_public_key_pem(public_key_pem).map_err(Error::other)?;
    let signature_len = public_key.size();

    if signed_payload.len() <= signature_len {
        return Err(Error::new(ErrorKind::InvalidData, "license token is missing signed payload"));
    }

    let (signature, payload) = signed_payload.split_at(signature_len);
    let signature = Signature::try_from(signature).map_err(Error::other)?;
    let verifying_key = VerifyingKey::<Sha256>::new(public_key);
    verifying_key.verify(payload, &signature).map_err(Error::other)?;

    serde_json::from_slice(payload).map_err(Error::other)
}

pub fn parse_license_with_public_key(license: &str, public_key: &str) -> Result<Token> {
    parse_signed_license_token(license, public_key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsa::{
        RsaPrivateKey,
        pkcs8::{EncodePrivateKey, EncodePublicKey, LineEnding},
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn test_sign_license_token_and_parse_signed_license_token() {
        let mut rng = rand::rng();
        let bits = 2048;
        let private_key = RsaPrivateKey::new(&mut rng, bits).expect("Failed to generate private key");
        let public_key = RsaPublicKey::from(&private_key);

        let private_key_pem = private_key.to_pkcs8_pem(LineEnding::LF).unwrap();
        let public_key_pem = public_key.to_public_key_pem(LineEnding::LF).unwrap();

        let token = Token {
            name: "test_app".to_string(),
            expired: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() + 3600, // 1 hour from now
        };

        let encoded = sign_license_token(&token, &private_key_pem).expect("Failed to encode token");

        let decoded = parse_signed_license_token(&encoded, &public_key_pem).expect("Failed to decode token");

        assert_eq!(token.name, decoded.name);
        assert_eq!(token.expired, decoded.expired);
    }

    #[test]
    #[allow(deprecated)]
    fn test_legacy_gencode_and_parse_roundtrip() {
        let mut rng = rand::rng();
        let bits = 2048;
        let private_key = RsaPrivateKey::new(&mut rng, bits).expect("Failed to generate private key");
        let public_key = RsaPublicKey::from(&private_key);

        let private_key_pem = private_key.to_pkcs8_pem(LineEnding::LF).unwrap();
        let public_key_pem = public_key.to_public_key_pem(LineEnding::LF).unwrap();

        let token = Token {
            name: "test_app".to_string(),
            expired: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() + 3600,
        };

        let encoded = gencode(&token, &public_key_pem).expect("Failed to encode token");
        let decoded = parse(&encoded, &private_key_pem).expect("Failed to decode token");

        assert_eq!(token.name, decoded.name);
        assert_eq!(token.expired, decoded.expired);
    }

    #[test]
    fn test_parse_signed_license_token_rejects_tampered_payload() {
        let mut rng = rand::rng();
        let private_key = RsaPrivateKey::new(&mut rng, 2048).expect("Failed to generate private key");
        let public_key = RsaPublicKey::from(&private_key);
        let private_key_pem = private_key.to_pkcs8_pem(LineEnding::LF).unwrap();
        let public_key_pem = public_key.to_public_key_pem(LineEnding::LF).unwrap();
        let token = Token {
            name: "test_app".to_string(),
            expired: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() + 3600,
        };

        let encoded = sign_license_token(&token, &private_key_pem).expect("Failed to encode token");
        let mut signed_payload = base64_simd::URL_SAFE_NO_PAD
            .decode_to_vec(encoded.as_bytes())
            .expect("Failed to decode signed payload");
        let last_byte = signed_payload.last_mut().expect("Signed payload should not be empty");
        *last_byte ^= 0x01;
        let tampered = base64_simd::URL_SAFE_NO_PAD.encode_to_string(&signed_payload);

        let result = parse_signed_license_token(&tampered, &public_key_pem);

        assert!(result.is_err());
    }

    #[test]
    fn test_source_does_not_embed_private_key() {
        let source = include_str!("token.rs");
        let forbidden = ["BEGIN", "PRIVATE KEY"].join(" ");

        assert!(!source.contains(&forbidden));
    }

    #[test]
    fn test_parse_signed_license_token_rejects_invalid_token() {
        let mut rng = rand::rng();
        let private_key = RsaPrivateKey::new(&mut rng, 2048).expect("Failed to generate private key");
        let public_key = RsaPublicKey::from(&private_key);
        let public_key_pem = public_key.to_public_key_pem(LineEnding::LF).unwrap();

        let invalid_token = "invalid_base64_token";
        let result = parse_signed_license_token(invalid_token, &public_key_pem);

        assert!(result.is_err());
    }

    #[test]
    fn test_sign_license_token_with_invalid_signing_key() {
        let token = Token {
            name: "test_app".to_string(),
            expired: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() + 3600, // 1 hour from now
        };

        let invalid_key = "invalid_private_key";
        let result = sign_license_token(&token, invalid_key);

        assert!(result.is_err());
    }
}
