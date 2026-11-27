use crate::jwt::{JsonWebToken, JsonWebTokenJsonPayloadSerde, NormalizedJsonWebTokenType};
use crate::user_info::UserInfoClaimsImpl;
use crate::{
    AdditionalClaims, Audience, AuthenticationContextClass, ClientId, ClientSecret, GenderClaim,
    IdTokenClaims, IssuerUrl, JsonWebKey, JsonWebKeyId, JsonWebKeySet, JsonWebTokenAccess,
    JsonWebTokenAlgorithm, JsonWebTokenHeader, JsonWebTokenType, JweContentEncryptionAlgorithm,
    JwsSigningAlgorithm, Nonce, SubjectIdentifier,
};

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

use std::collections::HashSet;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;

#[cfg(test)]
mod tests;

pub(crate) trait AudiencesClaim {
    fn audiences(&self) -> Option<&Vec<Audience>>;
}

pub(crate) trait IssuerClaim {
    fn issuer(&self) -> Option<&IssuerUrl>;
}

/// Error verifying claims.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum ClaimsVerificationError {
    /// Claims have expired.
    #[error("Expired: {0}")]
    Expired(String),
    /// Audience claim is invalid.
    #[error("Invalid audiences: {0}")]
    InvalidAudience(String),
    /// Authorization context class reference (`acr`) claim is invalid.
    #[error("Invalid authorization context class reference: {0}")]
    InvalidAuthContext(String),
    /// User authenticated too long ago.
    #[error("Invalid authentication time: {0}")]
    InvalidAuthTime(String),
    /// Issuer claim is invalid.
    #[error("Invalid issuer: {0}")]
    InvalidIssuer(String),
    /// Nonce is invalid.
    #[error("Invalid nonce: {0}")]
    InvalidNonce(String),
    /// Subject claim is invalid.
    #[error("Invalid subject: {0}")]
    InvalidSubject(String),
    /// An unexpected error occurred.
    #[error("{0}")]
    Other(String),
    /// Failed to verify the claims signature.
    #[error("Signature verification failed")]
    SignatureVerification(#[source] SignatureVerificationError),
    /// Unsupported argument or value.
    #[error("Unsupported: {0}")]
    Unsupported(String),
}

/// Error verifying claims signature.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum SignatureVerificationError {
    /// More than one key matches the supplied key constraints (e.g., key ID).
    #[error("Ambiguous key identification: {0}")]
    AmbiguousKeyId(String),
    /// Invalid signature for the supplied claims and signing key.
    #[error("Crypto error: {0}")]
    CryptoError(String),
    /// The supplied signature algorithm is disallowed by the verifier.
    #[error("Disallowed signature algorithm: {0}")]
    DisallowedAlg(String),
    /// The supplied key cannot be used in this context. This may occur if the key type does not
    /// match the signature type (e.g., an RSA key used to validate an HMAC) or the JWK usage
    /// disallows signatures.
    #[error("Invalid cryptographic key: {0}")]
    InvalidKey(String),
    /// The signing key needed for verifying the
    /// [JSON Web Token](https://tools.ietf.org/html/rfc7519)'s signature/MAC could not be found.
    /// This error can occur if the key ID (`kid`) specified in the JWT's
    /// [JOSE header](https://tools.ietf.org/html/rfc7519#section-5) does not match the ID of any
    /// key in the OpenID Connect provider's JSON Web Key Set (JWKS), typically retrieved from
    /// the provider's [JWKS document](
    /// http://openid.net/specs/openid-connect-discovery-1_0.html#ProviderMetadata). To support
    /// [rotation of asymmetric signing keys](
    /// http://openid.net/specs/openid-connect-core-1_0.html#RotateSigKeys), client applications
    /// should consider refreshing the JWKS document (via
    /// [`JsonWebKeySet::fetch`][crate::JsonWebKeySet::fetch]).
    ///
    /// This error can also occur if the identified
    /// [JSON Web Key](https://tools.ietf.org/html/rfc7517) is of the wrong type (e.g., an RSA key
    /// when the JOSE header specifies an ECDSA algorithm) or does not support signing.
    #[error("No matching key found")]
    NoMatchingKey,
    /// No signature present but claims must be signed.
    #[error("No signature found")]
    NoSignature,
    /// Unsupported signature algorithm.
    #[error("Unsupported signature algorithm: {0}")]
    UnsupportedAlg(String),
    /// An unexpected error occurred.
    #[error("Other error: {0}")]
    Other(String),
}

// This struct is intentionally private.
#[derive(Clone)]
pub(crate) struct JwtClaimsVerifier<'a, K>
where
    K: JsonWebKey,
{
    allowed_jose_types: Option<HashSet<NormalizedJsonWebTokenType>>,
    allowed_algs: Option<HashSet<K::SigningAlgorithm>>,
    aud_match_required: bool,
    client_id: ClientId,
    client_secret: Option<ClientSecret>,
    iss_required: bool,
    issuer: IssuerUrl,
    is_signature_check_enabled: bool,
    other_aud_verifier_fn: Arc<dyn Fn(&Audience) -> bool + 'a + Send + Sync>,
    signature_keys: JsonWebKeySet<K>,
}
impl<'a, K> JwtClaimsVerifier<'a, K>
where
    K: JsonWebKey,
{
    pub fn new(client_id: ClientId, issuer: IssuerUrl, signature_keys: JsonWebKeySet<K>) -> Self {
        JwtClaimsVerifier {
            allowed_algs: Some(
                [K::SigningAlgorithm::rsa_sha_256()]
                    .iter()
                    .cloned()
                    .collect(),
            ),
            allowed_jose_types: Some(HashSet::from([
                JsonWebTokenType::new("application/jwt".to_string())
                    .normalize()
                    .expect("application/jwt should be a valid JWT type"), // used by many IdP, but not standardized
                JsonWebTokenType::new("application/jose".to_string())
                    .normalize()
                    .expect("application/jose should be a valid JWT type"), // standard as defined in https://tools.ietf.org/html/rfc7515#section-4.1.9
                                                                            // we do not support JOSE+JSON, so we omit this here in the default configuration
            ])),
            aud_match_required: true,
            client_id,
            client_secret: None,
            iss_required: true,
            issuer,
            is_signature_check_enabled: true,
            // Secure default: reject all other audiences as untrusted, since any other audience
            // can potentially impersonate the user when by sending its copy of these claims
            // to this relying party.
            other_aud_verifier_fn: Arc::new(|_| false),
            signature_keys,
        }
    }

    pub fn require_audience_match(mut self, aud_required: bool) -> Self {
        self.aud_match_required = aud_required;
        self
    }

    pub fn require_issuer_match(mut self, iss_required: bool) -> Self {
        self.iss_required = iss_required;
        self
    }

    pub fn require_signature_check(mut self, sig_required: bool) -> Self {
        self.is_signature_check_enabled = sig_required;
        self
    }

    pub fn set_allowed_algs<I>(mut self, algs: I) -> Self
    where
        I: IntoIterator<Item = K::SigningAlgorithm>,
    {
        self.allowed_algs = Some(algs.into_iter().collect());
        self
    }
    pub fn allow_any_alg(mut self) -> Self {
        self.allowed_algs = None;
        self
    }

    /// Allows setting specific JOSE types. The verifier will check against them during verification.
    ///
    /// See [RFC 7515 section 4.1.9](https://tools.ietf.org/html/rfc7515#section-4.1.9) for more details.
    pub fn set_allowed_jose_types<I>(mut self, types: I) -> Self
    where
        I: IntoIterator<Item = NormalizedJsonWebTokenType>,
    {
        self.allowed_jose_types = Some(types.into_iter().collect());
        self
    }
    pub fn allow_all_jose_types(mut self) -> Self {
        self.allowed_jose_types = None;
        self
    }

    pub fn set_client_secret(mut self, client_secret: ClientSecret) -> Self {
        self.client_secret = Some(client_secret);
        self
    }

    pub fn set_other_audience_verifier_fn<T>(mut self, other_aud_verifier_fn: T) -> Self
    where
        T: Fn(&Audience) -> bool + 'a + Send + Sync,
    {
        self.other_aud_verifier_fn = Arc::new(other_aud_verifier_fn);
        self
    }

    fn validate_jose_header<JE>(
        &self,
        jose_header: &JsonWebTokenHeader<JE, K::SigningAlgorithm>,
    ) -> Result<(), ClaimsVerificationError>
    where
        JE: JweContentEncryptionAlgorithm<
            KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType,
        >,
    {
        // The 'typ' header field must either be omitted or have the canonicalized value JWT.
        // see https://tools.ietf.org/html/rfc7519#section-5.1
        if let Some(ref jwt_type) = jose_header.typ {
            if let Some(allowed_jose_types) = &self.allowed_jose_types {
                // Check according to https://tools.ietf.org/html/rfc7515#section-4.1.9
                // See https://tools.ietf.org/html/rfc2045#section-5.1 for the full Content-Type Header Field spec.
                //
                // For sake of simplicity, we do not support matching on application types with parameters like
                // application/example;part="1/2". If you know your parameters exactly, just set the whole Content Type manually.
                let valid_jwt_type = if let Ok(normalized_jwt_type) = jwt_type.normalize() {
                    allowed_jose_types.contains(&normalized_jwt_type)
                } else {
                    false
                };

                if !valid_jwt_type {
                    return Err(ClaimsVerificationError::Unsupported(format!(
                        "unexpected or unsupported JWT type `{}`",
                        **jwt_type
                    )));
                }
            }
        }

        // The 'cty' header field must be omitted, since it's only used for JWTs that contain
        // content types other than JSON-encoded claims. This may include nested JWTs, such as if
        // JWE encryption is used. This is currently unsupported.
        if let Some(ref content_type) = jose_header.cty {
            if content_type.to_uppercase() == "JWT" {
                return Err(ClaimsVerificationError::Unsupported(
                    "nested JWT's are not currently supported".to_string(),
                ));
            } else {
                return Err(ClaimsVerificationError::Unsupported(format!(
                    "unexpected or unsupported JWT content type `{}`",
                    **content_type
                )));
            }
        }

        // If 'crit' fields are specified, we must reject any we do not understand. Since this
        // implementation doesn't understand any of them, unconditionally reject the JWT. Note that
        // the spec prohibits this field from containing any of the standard headers or being empty.
        if jose_header.crit.is_some() {
            // https://tools.ietf.org/html/rfc7515#appendix-E
            return Err(ClaimsVerificationError::Unsupported(
                "critical JWT header fields are unsupported".to_string(),
            ));
        }
        Ok(())
    }

    pub fn verified_claims<A, C, JE, T>(&self, jwt: A) -> Result<T, ClaimsVerificationError>
    where
        A: JsonWebTokenAccess<JE, K::SigningAlgorithm, C, ReturnType = T>,
        C: AudiencesClaim + Debug + DeserializeOwned + IssuerClaim + Serialize,
        JE: JweContentEncryptionAlgorithm<
            KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType,
        >,
        T: AudiencesClaim + IssuerClaim,
    {
        {
            let jose_header = jwt.unverified_header();
            self.validate_jose_header(jose_header)?;

            // The code below roughly follows the validation steps described in
            // https://openid.net/specs/openid-connect-core-1_0.html#IDTokenValidation

            // 1. If the ID Token is encrypted, decrypt it using the keys and algorithms that the Client
            //    specified during Registration that the OP was to use to encrypt the ID Token. If
            //    encryption was negotiated with the OP at Registration time and the ID Token is not
            //    encrypted, the RP SHOULD reject it.

            if let JsonWebTokenAlgorithm::Encryption(ref encryption_alg) = jose_header.alg {
                return Err(ClaimsVerificationError::Unsupported(format!(
                    "JWE encryption is not currently supported (found algorithm `{}`)",
                    serde_plain::to_string(encryption_alg).unwrap_or_else(|err| panic!(
                        "encryption alg {encryption_alg:?} failed to serialize to a string: {err}"
                    )),
                )));
            }
        }

        // TODO: Add encryption (JWE) support
        {
            // 2. The Issuer Identifier for the OpenID Provider (which is typically obtained during
            //    Discovery) MUST exactly match the value of the iss (issuer) Claim.
            let unverified_claims = jwt.unverified_payload_ref();
            if self.iss_required {
                if let Some(issuer) = unverified_claims.issuer() {
                    if *issuer != self.issuer {
                        return Err(ClaimsVerificationError::InvalidIssuer(format!(
                            "expected `{}` (found `{}`)",
                            *self.issuer, **issuer
                        )));
                    }
                } else {
                    return Err(ClaimsVerificationError::InvalidIssuer(
                        "missing issuer claim".to_string(),
                    ));
                }
            }

            // 3. The Client MUST validate that the aud (audience) Claim contains its client_id value
            //    registered at the Issuer identified by the iss (issuer) Claim as an audience. The aud
            //    (audience) Claim MAY contain an array with more than one element. The ID Token MUST be
            //    rejected if the ID Token does not list the Client as a valid audience, or if it
            //    contains additional audiences not trusted by the Client.
            if self.aud_match_required {
                if let Some(audiences) = unverified_claims.audiences() {
                    if !audiences
                        .iter()
                        .any(|aud| (**aud).deref() == self.client_id.deref())
                    {
                        return Err(ClaimsVerificationError::InvalidAudience(format!(
                            "must contain `{}` (found audiences: {})",
                            *self.client_id,
                            audiences
                                .iter()
                                .map(|aud| format!("`{}`", Deref::deref(aud)))
                                .collect::<Vec<_>>()
                                .join(", ")
                        )));
                    } else if audiences.len() > 1 {
                        audiences
                            .iter()
                            .filter(|aud| (**aud).deref() != self.client_id.deref())
                            .find(|aud| !(self.other_aud_verifier_fn)(aud))
                            .map(|aud| {
                                Err(ClaimsVerificationError::InvalidAudience(format!(
                                    "`{}` is not a trusted audience",
                                    **aud,
                                )))
                            })
                            .unwrap_or(Ok(()))?;
                    }
                } else {
                    return Err(ClaimsVerificationError::InvalidAudience(
                        "missing audiences claim".to_string(),
                    ));
                }
            }
        }
        // Steps 4--5 (azp claim validation) are specific to the ID token.

        // 6. If the ID Token is received via direct communication between the Client and the Token
        //    Endpoint (which it is in this flow), the TLS server validation MAY be used to validate
        //    the issuer in place of checking the token signature. The Client MUST validate the
        //    signature of all other ID Tokens according to JWS [JWS] using the algorithm specified
        //    in the JWT alg Header Parameter. The Client MUST use the keys provided by the Issuer.
        if !self.is_signature_check_enabled {
            return Ok(jwt.unverified_payload());
        }

        // Borrow the header again. We had to drop the reference above to allow for the
        // early exit calling jwt.unverified_claims(), which takes ownership of the JWT.
        let signature_alg = jwt
            .signing_alg()
            .map_err(ClaimsVerificationError::SignatureVerification)?
            .to_owned();

        // 7. The alg value SHOULD be the default of RS256 or the algorithm sent by the Client
        //    in the id_token_signed_response_alg parameter during Registration.
        if let Some(ref allowed_algs) = self.allowed_algs {
            if !allowed_algs.contains(&signature_alg) {
                return Err(ClaimsVerificationError::SignatureVerification(
                    SignatureVerificationError::DisallowedAlg(format!(
                        "algorithm `{}` is not one of: {}",
                        serde_plain::to_string(&signature_alg).unwrap_or_else(|err| panic!(
                            "signature alg {signature_alg:?} failed to serialize to a string: {err}",
                        )),
                        allowed_algs
                            .iter()
                            .map(
                                |alg| serde_plain::to_string(alg).unwrap_or_else(|err| panic!(
                                    "signature alg {alg:?} failed to serialize to a string: {err}",
                                ))
                            )
                            .collect::<Vec<_>>()
                            .join(", "),
                    )),
                ));
            }
        }

        // NB: We must *not* trust the 'kid' (key ID) or 'alg' (algorithm) fields present in the
        // JOSE header, as an attacker could manipulate these while forging the JWT. The code
        // below must be secure regardless of how these fields are manipulated.

        if signature_alg.uses_shared_secret() {
            // 8. If the JWT alg Header Parameter uses a MAC based algorithm such as HS256,
            //    HS384, or HS512, the octets of the UTF-8 representation of the client_secret
            //    corresponding to the client_id contained in the aud (audience) Claim are used
            //    as the key to validate the signature. For MAC based algorithms, the behavior
            //    is unspecified if the aud is multi-valued or if an azp value is present that
            //    is different than the aud value.
            if let Some(ref client_secret) = self.client_secret {
                let key = K::new_symmetric(client_secret.secret().clone().into_bytes());
                return jwt
                    .payload(&signature_alg, &key)
                    .map_err(ClaimsVerificationError::SignatureVerification);
            } else {
                // The client secret isn't confidential for public clients, so anyone can forge a
                // JWT with a valid signature.
                return Err(ClaimsVerificationError::SignatureVerification(
                    SignatureVerificationError::DisallowedAlg(
                        "symmetric signatures are disallowed for public clients".to_string(),
                    ),
                ));
            }
        }

        // Section 10.1 of OpenID Connect Core 1.0 states that the JWT must include a key ID
        // if the JWK set contains more than one public key.

        let public_key = self
            .signing_key(jwt.unverified_header().kid.as_ref(), &signature_alg)
            .map_err(ClaimsVerificationError::SignatureVerification)?;

        jwt.payload(&signature_alg.clone(), public_key)
            .map_err(ClaimsVerificationError::SignatureVerification)

        // Steps 9--13 are specific to the ID token.
    }

    pub(crate) fn signing_key<'b>(
        &'b self,
        key_id: Option<&JsonWebKeyId>,
        signature_alg: &K::SigningAlgorithm,
    ) -> Result<&'b K, SignatureVerificationError> {
        // See if any key has a matching key ID (if supplied) and compatible type.
        let public_keys = self.signature_keys.filter_keys(key_id, signature_alg);
        if public_keys.is_empty() {
            Err(SignatureVerificationError::NoMatchingKey)
        } else if public_keys.len() == 1 {
            Ok(public_keys.first().expect("unreachable"))
        } else {
            Err(SignatureVerificationError::AmbiguousKeyId(format!(
                "JWK set must only contain one eligible public key, but found {} eligible keys: {}",
                public_keys.len(),
                public_keys
                    .iter()
                    .map(|key| format!(
                        "{} ({})",
                        key.key_id()
                            .map(|kid| format!("`{}`", **kid))
                            .unwrap_or_else(|| "null ID".to_string()),
                        serde_plain::to_string(key.key_type()).unwrap_or_else(|err| panic!(
                            "key type {:?} failed to serialize to a string: {}",
                            key.key_type(),
                            err,
                        ))
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            )))
        }
    }
}

/// Trait for verifying ID token nonces.
pub trait NonceVerifier {
    /// Verifies the nonce.
    ///
    /// Returns `Ok(())` if the nonce is valid, or a string describing the error otherwise.
    fn verify(self, nonce: Option<&Nonce>) -> Result<(), String>;
}

impl NonceVerifier for &Nonce {
    fn verify(self, nonce: Option<&Nonce>) -> Result<(), String> {
        if let Some(claims_nonce) = nonce {
            // Avoid timing side-channel.
            if Sha256::digest(claims_nonce.secret()) != Sha256::digest(self.secret()) {
                return Err("nonce mismatch".to_string());
            }
        } else {
            return Err("missing nonce claim".to_string());
        }
        Ok(())
    }
}

impl<F> NonceVerifier for F
where
    F: FnOnce(Option<&Nonce>) -> Result<(), String>,
{
    fn verify(self, nonce: Option<&Nonce>) -> Result<(), String> {
        self(nonce)
    }
}

/// ID token verifier.
#[derive(Clone)]
pub struct IdTokenVerifier<'a, K>
where
    K: JsonWebKey,
{
    acr_verifier_fn:
        Arc<dyn Fn(Option<&AuthenticationContextClass>) -> Result<(), String> + 'a + Send + Sync>,
    #[allow(clippy::type_complexity)]
    auth_time_verifier_fn:
        Arc<dyn Fn(Option<DateTime<Utc>>) -> Result<(), String> + 'a + Send + Sync>,
    iat_verifier_fn: Arc<dyn Fn(DateTime<Utc>) -> Result<(), String> + 'a + Send + Sync>,
    pub(crate) jwt_verifier: JwtClaimsVerifier<'a, K>,
    time_fn: Arc<dyn Fn() -> DateTime<Utc> + 'a + Send + Sync>,
}
impl<'a, K> IdTokenVerifier<'a, K>
where
    K: JsonWebKey,
{
    fn new(jwt_verifier: JwtClaimsVerifier<'a, K>) -> Self {
        IdTokenVerifier {
            // By default, accept authorization context reference (acr claim).
            acr_verifier_fn: Arc::new(|_| Ok(())),
            auth_time_verifier_fn: Arc::new(|_| Ok(())),
            // By default, accept any issued time (iat claim).
            iat_verifier_fn: Arc::new(|_| Ok(())),
            jwt_verifier,
            // By default, use the current system time.
            time_fn: Arc::new(Utc::now),
        }
    }

    /// Initializes a new verifier for a public client (i.e., one without a client secret).
    pub fn new_public_client(
        client_id: ClientId,
        issuer: IssuerUrl,
        signature_keys: JsonWebKeySet<K>,
    ) -> Self {
        Self::new(JwtClaimsVerifier::new(client_id, issuer, signature_keys))
    }

    /// Initializes a no-op verifier that performs no signature, audience, or issuer verification.
    /// The token's expiration time is still checked, and the token is otherwise required to conform to the expected format.
    pub fn new_insecure_without_verification() -> Self {
        let empty_issuer = IssuerUrl::new("https://0.0.0.0".to_owned())
            .expect("Creating empty issuer url mustn't fail");
        Self::new_public_client(
            ClientId::new(String::new()),
            empty_issuer,
            JsonWebKeySet::new(vec![]),
        )
        .insecure_disable_signature_check()
        .require_audience_match(false)
        .require_issuer_match(false)
    }

    /// Initializes a new verifier for a confidential client (i.e., one with a client secret).
    ///
    /// A confidential client verifier is required in order to verify ID tokens signed using a
    /// shared secret algorithm such as `HS256`, `HS384`, or `HS512`. For these algorithms, the
    /// client secret is the shared secret.
    pub fn new_confidential_client(
        client_id: ClientId,
        client_secret: ClientSecret,
        issuer: IssuerUrl,
        signature_keys: JsonWebKeySet<K>,
    ) -> Self {
        Self::new(
            JwtClaimsVerifier::new(client_id, issuer, signature_keys)
                .set_client_secret(client_secret),
        )
    }

    /// Specifies which JSON Web Signature algorithms are supported.
    pub fn set_allowed_algs<I>(mut self, algs: I) -> Self
    where
        I: IntoIterator<Item = K::SigningAlgorithm>,
    {
        self.jwt_verifier = self.jwt_verifier.set_allowed_algs(algs);
        self
    }

    /// Specifies that any signature algorithm is supported.
    pub fn allow_any_alg(mut self) -> Self {
        self.jwt_verifier = self.jwt_verifier.allow_any_alg();
        self
    }

    /// Allows setting specific JOSE types. The verifier will check against them during verification.
    ///
    /// See [RFC 7515 section 4.1.9](https://tools.ietf.org/html/rfc7515#section-4.1.9) for more details.
    pub fn set_allowed_jose_types<I>(mut self, types: I) -> Self
    where
        I: IntoIterator<Item = NormalizedJsonWebTokenType>,
    {
        self.jwt_verifier = self.jwt_verifier.set_allowed_jose_types(types);
        self
    }

    /// Allow all JSON Web Token Header types.
    pub fn allow_all_jose_types(mut self) -> Self {
        self.jwt_verifier = self.jwt_verifier.allow_all_jose_types();
        self
    }

    /// Specifies a function for verifying the `acr` claim.
    ///
    /// The function should return `Ok(())` if the claim is valid, or a string describing the error
    /// otherwise.
    pub fn set_auth_context_verifier_fn<T>(mut self, acr_verifier_fn: T) -> Self
    where
        T: Fn(Option<&AuthenticationContextClass>) -> Result<(), String> + 'a + Send + Sync,
    {
        self.acr_verifier_fn = Arc::new(acr_verifier_fn);
        self
    }

    /// Specifies a function for verifying the `auth_time` claim.
    ///
    /// The function should return `Ok(())` if the claim is valid, or a string describing the error
    /// otherwise.
    pub fn set_auth_time_verifier_fn<T>(mut self, auth_time_verifier_fn: T) -> Self
    where
        T: Fn(Option<DateTime<Utc>>) -> Result<(), String> + 'a + Send + Sync,
    {
        self.auth_time_verifier_fn = Arc::new(auth_time_verifier_fn);
        self
    }

    /// Enables signature verification.
    ///
    /// Signature verification is enabled by default, so this function is only useful if
    /// [`IdTokenVerifier::insecure_disable_signature_check`] was previously invoked.
    pub fn enable_signature_check(mut self) -> Self {
        self.jwt_verifier = self.jwt_verifier.require_signature_check(true);
        self
    }

    /// Disables signature verification.
    ///
    /// # Security Warning
    ///
    /// Unverified ID tokens may be subject to forgery. See [Section 16.3](
    /// https://openid.net/specs/openid-connect-core-1_0.html#TokenManufacture) for more
    /// information.
    pub fn insecure_disable_signature_check(mut self) -> Self {
        self.jwt_verifier = self.jwt_verifier.require_signature_check(false);
        self
    }

    /// Specifies whether the issuer claim must match the expected issuer URL for the provider.
    pub fn require_issuer_match(mut self, iss_required: bool) -> Self {
        self.jwt_verifier = self.jwt_verifier.require_issuer_match(iss_required);
        self
    }

    /// Specifies whether the audience claim must match this client's client ID.
    pub fn require_audience_match(mut self, aud_required: bool) -> Self {
        self.jwt_verifier = self.jwt_verifier.require_audience_match(aud_required);
        self
    }

    /// Specifies a function for returning the current time.
    ///
    /// This function is used for verifying the ID token expiration time.
    pub fn set_time_fn<T>(mut self, time_fn: T) -> Self
    where
        T: Fn() -> DateTime<Utc> + 'a + Send + Sync,
    {
        self.time_fn = Arc::new(time_fn);
        self
    }

    /// Specifies a function for verifying the ID token issue time.
    ///
    /// The function should return `Ok(())` if the claim is valid, or a string describing the error
    /// otherwise.
    pub fn set_issue_time_verifier_fn<T>(mut self, iat_verifier_fn: T) -> Self
    where
        T: Fn(DateTime<Utc>) -> Result<(), String> + 'a + Send + Sync,
    {
        self.iat_verifier_fn = Arc::new(iat_verifier_fn);
        self
    }

    /// Specifies a function for verifying audiences included in the `aud` claim that differ from
    /// this client's client ID.
    ///
    /// The function should return `true` if the audience is trusted, or `false` otherwise.
    ///
    /// [Section 3.1.3.7](https://openid.net/specs/openid-connect-core-1_0.html#IDTokenValidation)
    /// states that *"The ID Token MUST be rejected if the ID Token does not list the Client as a
    /// valid audience, or if it contains additional audiences not trusted by the Client."*
    pub fn set_other_audience_verifier_fn<T>(mut self, other_aud_verifier_fn: T) -> Self
    where
        T: Fn(&Audience) -> bool + 'a + Send + Sync,
    {
        self.jwt_verifier = self
            .jwt_verifier
            .set_other_audience_verifier_fn(other_aud_verifier_fn);
        self
    }

    pub(crate) fn verified_claims<'b, AC, GC, JE, N>(
        &self,
        jwt: &'b JsonWebToken<
            JE,
            K::SigningAlgorithm,
            IdTokenClaims<AC, GC>,
            JsonWebTokenJsonPayloadSerde,
        >,
        nonce_verifier: N,
    ) -> Result<&'b IdTokenClaims<AC, GC>, ClaimsVerificationError>
    where
        AC: AdditionalClaims,
        GC: GenderClaim,
        JE: JweContentEncryptionAlgorithm<
            KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType,
        >,
        N: NonceVerifier,
    {
        // The code below roughly follows the validation steps described in
        // https://openid.net/specs/openid-connect-core-1_0.html#IDTokenValidation

        // Steps 1--3 are handled by the generic JwtClaimsVerifier.
        let partially_verified_claims = self.jwt_verifier.verified_claims(jwt)?;

        self.verify_claims(partially_verified_claims, nonce_verifier)?;
        Ok(partially_verified_claims)
    }

    pub(crate) fn verified_claims_owned<AC, GC, JE, N>(
        &self,
        jwt: JsonWebToken<
            JE,
            K::SigningAlgorithm,
            IdTokenClaims<AC, GC>,
            JsonWebTokenJsonPayloadSerde,
        >,
        nonce_verifier: N,
    ) -> Result<IdTokenClaims<AC, GC>, ClaimsVerificationError>
    where
        AC: AdditionalClaims,
        GC: GenderClaim,
        JE: JweContentEncryptionAlgorithm<
            KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType,
        >,
        N: NonceVerifier,
    {
        // The code below roughly follows the validation steps described in
        // https://openid.net/specs/openid-connect-core-1_0.html#IDTokenValidation

        // Steps 1--3 are handled by the generic JwtClaimsVerifier.
        let partially_verified_claims = self.jwt_verifier.verified_claims(jwt)?;

        self.verify_claims(&partially_verified_claims, nonce_verifier)?;
        Ok(partially_verified_claims)
    }

    fn verify_claims<AC, GC, N>(
        &self,
        partially_verified_claims: &'_ IdTokenClaims<AC, GC>,
        nonce_verifier: N,
    ) -> Result<(), ClaimsVerificationError>
    where
        AC: AdditionalClaims,
        GC: GenderClaim,
        N: NonceVerifier,
    {
        // 4. If the ID Token contains multiple audiences, the Client SHOULD verify that an azp
        //    Claim is present.

        // There is significant confusion and contradiction in the OpenID Connect Core spec around
        // the azp claim. See https://bitbucket.org/openid/connect/issues/973/ for a detailed
        // discussion. Given the lack of clarity around how this claim should be used, we defer
        // any verification of it here until a use case becomes apparent. If such a use case does
        // arise, we most likely want to allow clients to pass in a function for validating the
        // azp claim rather than introducing logic that affects all clients of this library.

        // This naive implementation of the spec would almost certainly not be useful in practice:
        /*
        let azp_required = partially_verified_claims.audiences().len() > 1;

        // 5. If an azp (authorized party) Claim is present, the Client SHOULD verify that its
        //    client_id is the Claim Value.
        if let Some(authorized_party) = partially_verified_claims.authorized_party() {
            if *authorized_party != self.client_id {
                return Err(ClaimsVerificationError::InvalidAudience(format!(
                    "authorized party must match client ID `{}` (found `{}`",
                    *self.client_id, **authorized_party
                )));
            }
        } else if azp_required {
            return Err(ClaimsVerificationError::InvalidAudience(format!(
                "missing authorized party claim but multiple audiences found"
            )));
        }
        */

        // Steps 6--8 are handled by the generic JwtClaimsVerifier.

        // 9. The current time MUST be before the time represented by the exp Claim.
        let cur_time = (*self.time_fn)();
        if cur_time >= partially_verified_claims.expiration() {
            return Err(ClaimsVerificationError::Expired(format!(
                "ID token expired at {} (current time is {})",
                partially_verified_claims.expiration(),
                cur_time
            )));
        }

        // 10. The iat Claim can be used to reject tokens that were issued too far away from the
        //     current time, limiting the amount of time that nonces need to be stored to prevent
        //     attacks. The acceptable range is Client specific.
        (*self.iat_verifier_fn)(partially_verified_claims.issue_time())
            .map_err(ClaimsVerificationError::Expired)?;

        // 11. If a nonce value was sent in the Authentication Request, a nonce Claim MUST be
        //     present and its value checked to verify that it is the same value as the one that was
        //     sent in the Authentication Request. The Client SHOULD check the nonce value for
        //     replay attacks. The precise method for detecting replay attacks is Client specific.
        nonce_verifier
            .verify(partially_verified_claims.nonce())
            .map_err(ClaimsVerificationError::InvalidNonce)?;

        // 12. If the acr Claim was requested, the Client SHOULD check that the asserted Claim Value
        //     is appropriate. The meaning and processing of acr Claim Values is out of scope for
        //     this specification.
        (*self.acr_verifier_fn)(partially_verified_claims.auth_context_ref())
            .map_err(ClaimsVerificationError::InvalidAuthContext)?;

        // 13. If the auth_time Claim was requested, either through a specific request for this
        //     Claim or by using the max_age parameter, the Client SHOULD check the auth_time Claim
        //     value and request re-authentication if it determines too much time has elapsed since
        //     the last End-User authentication.
        (*self.auth_time_verifier_fn)(partially_verified_claims.auth_time())
            .map_err(ClaimsVerificationError::InvalidAuthTime)?;

        Ok(())
    }
}

/// User info verifier.
#[derive(Clone)]
pub struct UserInfoVerifier<'a, JE, K>
where
    JE: JweContentEncryptionAlgorithm<
        KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType,
    >,
    K: JsonWebKey,
{
    jwt_verifier: JwtClaimsVerifier<'a, K>,
    expected_subject: Option<SubjectIdentifier>,
    _phantom: PhantomData<JE>,
}
impl<'a, JE, K> UserInfoVerifier<'a, JE, K>
where
    JE: JweContentEncryptionAlgorithm<
        KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType,
    >,
    K: JsonWebKey,
{
    /// Instantiates a user info verifier.
    pub fn new(
        client_id: ClientId,
        issuer: IssuerUrl,
        signature_keys: JsonWebKeySet<K>,
        expected_subject: Option<SubjectIdentifier>,
    ) -> Self {
        UserInfoVerifier {
            jwt_verifier: JwtClaimsVerifier::new(client_id, issuer, signature_keys),
            expected_subject,
            _phantom: PhantomData,
        }
    }

    pub(crate) fn expected_subject(&self) -> Option<&SubjectIdentifier> {
        self.expected_subject.as_ref()
    }

    /// Specifies whether the issuer claim must match the expected issuer URL for the provider.
    pub fn require_issuer_match(mut self, iss_required: bool) -> Self {
        self.jwt_verifier = self.jwt_verifier.require_issuer_match(iss_required);
        self
    }

    /// Specifies whether the audience claim must match this client's client ID.
    pub fn require_audience_match(mut self, aud_required: bool) -> Self {
        self.jwt_verifier = self.jwt_verifier.require_audience_match(aud_required);
        self
    }

    pub(crate) fn verified_claims<AC, GC>(
        &self,
        user_info_jwt: JsonWebToken<
            JE,
            K::SigningAlgorithm,
            UserInfoClaimsImpl<AC, GC>,
            JsonWebTokenJsonPayloadSerde,
        >,
    ) -> Result<UserInfoClaimsImpl<AC, GC>, ClaimsVerificationError>
    where
        AC: AdditionalClaims,
        GC: GenderClaim,
    {
        let user_info = self.jwt_verifier.verified_claims(user_info_jwt)?;
        if self
            .expected_subject
            .iter()
            .all(|expected_subject| user_info.standard_claims.sub == *expected_subject)
        {
            Ok(user_info)
        } else {
            Err(ClaimsVerificationError::InvalidSubject(format!(
                "expected `{}` (found `{}`)",
                // This can only happen when self.expected_subject is not None.
                self.expected_subject.as_ref().unwrap().as_str(),
                user_info.standard_claims.sub.as_str()
            )))
        }
    }
}
