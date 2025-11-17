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
    use super::{gen_access_key, gen_secret_key, generate_jwt};
    use serde::{Deserialize, Serialize};

    #[test]
    fn test_gen_access_key() {
        let a = gen_access_key(10).unwrap();
        let b = gen_access_key(10).unwrap();

        assert_eq!(a.len(), 10);
        assert_eq!(b.len(), 10);
        assert_ne!(a, b);
    }

    #[test]
    fn test_gen_secret_key() {
        let a = gen_secret_key(10).unwrap();
        let b = gen_secret_key(10).unwrap();
        assert_ne!(a, b);
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Claims {
        sub: String,
        company: String,
    }

    #[test]
    fn test_generate_jwt() {
        let claims = Claims {
            sub: "user1".to_string(),
            company: "example".to_string(),
        };
        let secret = "my_secret";
        let token = generate_jwt(&claims, secret).unwrap();

        assert!(!token.is_empty());
    }

    // #[test]
    // fn test_extract_claims() {
    //     let claims = Claims {
    //         sub: "user1".to_string(),
    //         company: "example".to_string(),
    //     };
    //     let secret = "my_secret";
    //     let token = generate_jwt(&claims, secret).unwrap();
    //     let decoded_claims = extract_claims::<Claims>(&token, secret).unwrap();
    //     assert_eq!(decoded_claims.claims, claims);
    // }
}
