use crate::{
    AdditionalClaims, ExtraTokenFields, GenderClaim, IdToken, IdTokenFields,
    JweContentEncryptionAlgorithm, JwsSigningAlgorithm, OAuth2TokenResponse, StandardTokenResponse,
    TokenType,
};

/// Extends the base OAuth2 token response with an ID token.
pub trait TokenResponse<AC, GC, JE, JS>: OAuth2TokenResponse
where
    AC: AdditionalClaims,
    GC: GenderClaim,
    JE: JweContentEncryptionAlgorithm<KeyType = JS::KeyType>,
    JS: JwsSigningAlgorithm,
{
    /// Returns the ID token provided by the token response.
    ///
    /// OpenID Connect authorization servers should always return this field, but it is optional
    /// to allow for interoperability with authorization servers that only support OAuth2.
    fn id_token(&self) -> Option<&IdToken<AC, GC, JE, JS>>;
}

impl<AC, EF, GC, JE, JS, TT> TokenResponse<AC, GC, JE, JS>
    for StandardTokenResponse<IdTokenFields<AC, EF, GC, JE, JS>, TT>
where
    AC: AdditionalClaims,
    EF: ExtraTokenFields,
    GC: GenderClaim,
    JE: JweContentEncryptionAlgorithm<KeyType = JS::KeyType>,
    JS: JwsSigningAlgorithm,
    TT: TokenType,
{
    fn id_token(&self) -> Option<&IdToken<AC, GC, JE, JS>> {
        self.extra_fields().id_token()
    }
}
