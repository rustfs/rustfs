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
};
use serde::{Deserialize, Serialize};
use std::io::{Error, Result};

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Token {
    pub name: String, // Application ID
    pub expired: u64, // Expiry time (UNIX timestamp)
}

/// Public key generation Token
/// [token] Token object
/// [key] Public key string
/// Returns the encrypted string processed by base64
pub fn gencode(token: &Token, key: &str) -> Result<String> {
    let data = serde_json::to_vec(token)?;
    let mut rng = rand::rng();
    let public_key = RsaPublicKey::from_public_key_pem(key).map_err(Error::other)?;
    let encrypted_data = public_key.encrypt(&mut rng, Pkcs1v15Encrypt, &data).map_err(Error::other)?;
    Ok(base64_simd::URL_SAFE_NO_PAD.encode_to_string(&encrypted_data))
}

/// Private key resolution Token
/// [token] Encrypted string processed by base64
/// [key] Private key string
/// Return to the Token object
pub fn parse(token: &str, key: &str) -> Result<Token> {
    let encrypted_data = base64_simd::URL_SAFE_NO_PAD
        .decode_to_vec(token.as_bytes())
        .map_err(Error::other)?;
    let private_key = RsaPrivateKey::from_pkcs8_pem(key).map_err(Error::other)?;
    let decrypted_data = private_key.decrypt(Pkcs1v15Encrypt, &encrypted_data).map_err(Error::other)?;
    let res: Token = serde_json::from_slice(&decrypted_data)?;
    Ok(res)
}

pub fn parse_license(license: &str) -> Result<Token> {
    parse(license, TEST_PRIVATE_KEY)
    // match parse(license, TEST_PRIVATE_KEY) {
    //     Ok(token) => {
    //         if token.expired > SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() {
    //             Ok(token)
    //         } else {
    //             Err("Token expired".into())
    //         }
    //     }
    //     Err(e) => Err(e),
    // }
}

static TEST_PRIVATE_KEY: &str = "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCj86SrJIuxSxR6\nBJ/dlJEUIj6NeBRnhLQlCDdovuz61+7kJXVcxaR66w4m8W7SLEUP+IlPtnn6vmiG\n7XMhGNHIr7r1JsEVVLhZmL3tKI66DEZl786ZhG81BWqUlmcooIPS8UEPZNqJXLuz\nVGhxNyVGbj/tV7QC2pSISnKaixc+nrhxvo7w56p5qrm9tik0PjTgfZsUePkoBsSN\npoRkAauS14MAzK6HGB75CzG3dZqXUNWSWVocoWtQbZUwFGXyzU01ammsHQDvc2xu\nK1RQpd1qYH5bOWZ0N0aPFwT0r59HztFXg9sbjsnuhO1A7OiUOkc6iGVuJ0wm/9nA\nwZIBqzgjAgMBAAECggEAPMpeSEbotPhNw2BrllE76ec4omPfzPJbiU+em+wPGoNu\nRJHPDnMKJbl6Kd5jZPKdOOrCnxfd6qcnQsBQa/kz7+GYxMV12l7ra+1Cnujm4v0i\nLTHZvPpp8ZLsjeOmpF3AAzsJEJgon74OqtOlVjVIUPEYKvzV9ijt4gsYq0zfdYv0\nhrTMzyrGM4/UvKLsFIBROAfCeWfA7sXLGH8JhrRAyDrtCPzGtyyAmzoHKHtHafcB\nuyPFw/IP8otAgpDk5iiQPNkH0WwzAQIm12oHuNUa66NwUK4WEjXTnDg8KeWLHHNv\nIfN8vdbZchMUpMIvvkr7is315d8f2cHCB5gEO+GWAQKBgQDR/0xNll+FYaiUKCPZ\nvkOCAd3l5mRhsqnjPQ/6Ul1lAyYWpoJSFMrGGn/WKTa/FVFJRTGbBjwP+Mx10bfb\ngUg2GILDTISUh54fp4zngvTi9w4MWGKXrb7I1jPkM3vbJfC/v2fraQ/r7qHPpO2L\nf6ZbGxasIlSvr37KeGoelwcAQQKBgQDH3hmOTS2Hl6D4EXdq5meHKrfeoicGN7m8\noQK7u8iwn1R9zK5nh6IXxBhKYNXNwdCQtBZVRvFjjZ56SZJb7lKqa1BcTsgJfZCy\nnI3Uu4UykrECAH8AVCVqBXUDJmeA2yE+gDAtYEjvhSDHpUfWxoGHr0B/Oqk2Lxc/\npRy1qV5fYwKBgBWSL/hYVf+RhIuTg/s9/BlCr9SJ0g3nGGRrRVTlWQqjRCpXeFOO\nJzYqSq9pFGKUggEQxoOyJEFPwVDo9gXqRcyov+Xn2kaXl7qQr3yoixc1YZALFDWY\nd1ySBEqQr0xXnV9U/gvEgwotPRnjSzNlLWV2ZuHPtPtG/7M0o1H5GZMBAoGAKr3N\nW0gX53o+my4pCnxRQW+aOIsWq1a5aqRIEFudFGBOUkS2Oz+fI1P1GdrRfhnnfzpz\n2DK+plp/vIkFOpGhrf4bBlJ2psjqa7fdANRFLMaAAfyXLDvScHTQTCcnVUAHQPVq\n2BlSH56pnugyj7SNuLV6pnql+wdhAmRN2m9o1h8CgYAbX2juSr4ioXwnYjOUdrIY\n4+ERvHcXdjoJmmPcAm4y5NbSqLXyU0FQmplNMt2A5LlniWVJ9KNdjAQUt60FZw/+\nr76LdxXaHNZghyx0BOs7mtq5unSQXamZ8KixasfhE9uz3ij1jXjG6hafWkS8/68I\nuWbaZqgvy7a9oPHYlKH7Jg==\n-----END PRIVATE KEY-----\n";

#[cfg(test)]
mod tests {
    use super::*;
    use rsa::{
        RsaPrivateKey,
        pkcs8::{EncodePrivateKey, EncodePublicKey, LineEnding},
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn test_gencode_and_parse() {
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

        let encoded = gencode(&token, &public_key_pem).expect("Failed to encode token");

        let decoded = parse(&encoded, &private_key_pem).expect("Failed to decode token");

        assert_eq!(token.name, decoded.name);
        assert_eq!(token.expired, decoded.expired);
    }

    #[test]
    fn test_parse_invalid_token() {
        let mut rng = rand::rng();
        let private_key_pem = RsaPrivateKey::new(&mut rng, 2048)
            .expect("Failed to generate private key")
            .to_pkcs8_pem(LineEnding::LF)
            .unwrap();

        let invalid_token = "invalid_base64_token";
        let result = parse(invalid_token, &private_key_pem);

        assert!(result.is_err());
    }

    #[test]
    fn test_gencode_with_invalid_key() {
        let token = Token {
            name: "test_app".to_string(),
            expired: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() + 3600, // 1 hour from now
        };

        let invalid_key = "invalid_public_key";
        let result = gencode(&token, invalid_key);

        assert!(result.is_err());
    }
}
