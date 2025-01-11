use crate::Error;
use jsonwebtoken::{encode, Algorithm, DecodingKey, EncodingKey, Header};
use rand::{Rng, RngCore};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub fn gen_access_key(length: usize) -> crate::Result<String> {
    const ALPHA_NUMERIC_TABLE: [char; 36] = [
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N',
        'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
    ];

    if length < 3 {
        return Err(Error::StringError("access key length is too short".into()));
    }

    let mut result = String::with_capacity(length);
    let mut rng = rand::thread_rng();

    for _ in 0..length {
        result.push(ALPHA_NUMERIC_TABLE[rng.gen_range(0..ALPHA_NUMERIC_TABLE.len())]);
    }

    Ok(result)
}

pub fn gen_secret_key(length: usize) -> crate::Result<String> {
    use base64_simd::URL_SAFE_NO_PAD;

    if length < 8 {
        return Err(Error::StringError("secret key length is too short".into()));
    }
    let mut rng = rand::thread_rng();

    let mut key = vec![0u8; URL_SAFE_NO_PAD.estimated_decoded_length(length)];
    rng.fill_bytes(&mut key);

    let encoded = URL_SAFE_NO_PAD.encode_to_string(&key);
    let key_str = encoded.replace("/", "+");

    Ok(key_str)
}

pub fn generate_jwt<T: Serialize>(claims: &T, secret: &str) -> Result<String, jsonwebtoken::errors::Error> {
    let header = Header::new(Algorithm::HS512);
    encode(&header, &claims, &EncodingKey::from_secret(secret.as_bytes()))
}

pub fn extract_claims<T: DeserializeOwned>(
    token: &str,
    secret: &str,
) -> Result<jsonwebtoken::TokenData<T>, jsonwebtoken::errors::Error> {
    jsonwebtoken::decode::<T>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &jsonwebtoken::Validation::new(Algorithm::HS512),
    )
}

#[cfg(test)]
mod tests {
    use super::{gen_access_key, gen_secret_key};

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
}
