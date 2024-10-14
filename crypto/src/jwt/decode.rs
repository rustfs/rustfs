use jsonwebtoken::{Algorithm, DecodingKey, TokenData, Validation};

use crate::jwt::Claims;
use crate::Error;

pub fn decode(token: &str, token_secret: &[u8]) -> Result<TokenData<Claims>, Error> {
    Ok(jsonwebtoken::decode(
        token,
        &DecodingKey::from_secret(token_secret),
        &Validation::new(Algorithm::HS512),
    )?)
}
