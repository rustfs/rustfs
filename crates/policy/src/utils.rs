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
    use super::generate_jwt;
    use serde::{Deserialize, Serialize};

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
