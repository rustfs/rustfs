use jsonwebtoken::{Algorithm, EncodingKey, Header};

use crate::Error;
use crate::jwt::Claims;

pub fn encode(token_secret: &[u8], claims: &Claims) -> Result<String, Error> {
    Ok(jsonwebtoken::encode(
        &Header::new(Algorithm::HS512),
        claims,
        &EncodingKey::from_secret(token_secret),
    )?)
}
