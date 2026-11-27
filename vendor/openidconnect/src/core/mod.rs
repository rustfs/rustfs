use crate::registration::{
    ClientMetadata, ClientRegistrationRequest, ClientRegistrationResponse,
    EmptyAdditionalClientMetadata, EmptyAdditionalClientRegistrationResponse,
    RegisterErrorResponseType,
};
use crate::{
    ApplicationType, AuthDisplay, AuthPrompt, AuthenticationFlow, ClaimName, ClaimType, Client,
    ClientAuthMethod, DeviceAuthorizationResponse, EmptyAdditionalClaims,
    EmptyAdditionalProviderMetadata, EmptyExtraDeviceAuthorizationFields, EmptyExtraTokenFields,
    ErrorResponseType, GenderClaim, GrantType, IdToken, IdTokenClaims, IdTokenFields,
    IdTokenVerifier, JsonWebKeySet, JweContentEncryptionAlgorithm, JweKeyManagementAlgorithm,
    JwsSigningAlgorithm, ProviderMetadata, ResponseMode, ResponseType, StandardErrorResponse,
    StandardTokenIntrospectionResponse, StandardTokenResponse, SubjectIdentifierType,
    UserInfoClaims, UserInfoJsonWebToken, UserInfoVerifier,
};

use base64::alphabet::URL_SAFE;
use base64::engine::general_purpose::NO_PAD;
use base64::engine::GeneralPurpose;
use oauth2::{EndpointNotSet, ResponseType as OAuth2ResponseType};
use serde::{Deserialize, Serialize};

use std::fmt::Display;

pub use crate::core::jwk::{
    CoreEdDsaPrivateSigningKey, CoreHmacKey, CoreJsonCurveType, CoreJsonWebKey, CoreJsonWebKeyType,
    CoreJsonWebKeyUse, CoreRsaPrivateSigningKey,
};

pub use oauth2::basic::{
    BasicErrorResponseType as CoreErrorResponseType,
    BasicRequestTokenError as CoreRequestTokenError,
    BasicRevocationErrorResponse as CoreRevocationErrorResponse, BasicTokenType as CoreTokenType,
};
pub use oauth2::StandardRevocableToken as CoreRevocableToken;

mod crypto;

// Private purely for organizational reasons; exported publicly above.
mod jwk;

/// Standard implementation of DeviceAuthorizationResponse which throws away extra received response fields.
pub type CoreDeviceAuthorizationResponse =
    DeviceAuthorizationResponse<EmptyExtraDeviceAuthorizationFields>;

/// OpenID Connect Core token introspection response.
pub type CoreTokenIntrospectionResponse =
    StandardTokenIntrospectionResponse<EmptyExtraTokenFields, CoreTokenType>;

/// OpenID Connect Core authentication flows.
pub type CoreAuthenticationFlow = AuthenticationFlow<CoreResponseType>;

/// OpenID Connect Core client.
pub type CoreClient<
    HasAuthUrl = EndpointNotSet,
    HasDeviceAuthUrl = EndpointNotSet,
    HasIntrospectionUrl = EndpointNotSet,
    HasRevocationUrl = EndpointNotSet,
    HasTokenUrl = EndpointNotSet,
    HasUserInfoUrl = EndpointNotSet,
> = Client<
    EmptyAdditionalClaims,
    CoreAuthDisplay,
    CoreGenderClaim,
    CoreJweContentEncryptionAlgorithm,
    CoreJsonWebKey,
    CoreAuthPrompt,
    StandardErrorResponse<CoreErrorResponseType>,
    CoreTokenResponse,
    CoreTokenIntrospectionResponse,
    CoreRevocableToken,
    CoreRevocationErrorResponse,
    HasAuthUrl,
    HasDeviceAuthUrl,
    HasIntrospectionUrl,
    HasRevocationUrl,
    HasTokenUrl,
    HasUserInfoUrl,
>;

/// OpenID Connect Core client metadata.
pub type CoreClientMetadata = ClientMetadata<
    EmptyAdditionalClientMetadata,
    CoreApplicationType,
    CoreClientAuthMethod,
    CoreGrantType,
    CoreJweContentEncryptionAlgorithm,
    CoreJweKeyManagementAlgorithm,
    CoreJsonWebKey,
    CoreResponseType,
    CoreSubjectIdentifierType,
>;

/// OpenID Connect Core client registration request.
pub type CoreClientRegistrationRequest = ClientRegistrationRequest<
    EmptyAdditionalClientMetadata,
    EmptyAdditionalClientRegistrationResponse,
    CoreApplicationType,
    CoreClientAuthMethod,
    CoreRegisterErrorResponseType,
    CoreGrantType,
    CoreJweContentEncryptionAlgorithm,
    CoreJweKeyManagementAlgorithm,
    CoreJsonWebKey,
    CoreResponseType,
    CoreSubjectIdentifierType,
>;

/// OpenID Connect Core client registration response.
pub type CoreClientRegistrationResponse = ClientRegistrationResponse<
    EmptyAdditionalClientMetadata,
    EmptyAdditionalClientRegistrationResponse,
    CoreApplicationType,
    CoreClientAuthMethod,
    CoreGrantType,
    CoreJweContentEncryptionAlgorithm,
    CoreJweKeyManagementAlgorithm,
    CoreJsonWebKey,
    CoreResponseType,
    CoreSubjectIdentifierType,
>;

/// OpenID Connect Core ID token.
pub type CoreIdToken = IdToken<
    EmptyAdditionalClaims,
    CoreGenderClaim,
    CoreJweContentEncryptionAlgorithm,
    CoreJwsSigningAlgorithm,
>;

/// OpenID Connect Core ID token claims.
pub type CoreIdTokenClaims = IdTokenClaims<EmptyAdditionalClaims, CoreGenderClaim>;

/// OpenID Connect Core ID token fields.
pub type CoreIdTokenFields = IdTokenFields<
    EmptyAdditionalClaims,
    EmptyExtraTokenFields,
    CoreGenderClaim,
    CoreJweContentEncryptionAlgorithm,
    CoreJwsSigningAlgorithm,
>;

/// OpenID Connect Core ID token verifier.
pub type CoreIdTokenVerifier<'a> = IdTokenVerifier<'a, CoreJsonWebKey>;

/// OpenID Connect Core token response.
pub type CoreTokenResponse = StandardTokenResponse<CoreIdTokenFields, CoreTokenType>;

/// OpenID Connect Core JSON Web Key Set.
pub type CoreJsonWebKeySet = JsonWebKeySet<CoreJsonWebKey>;

/// OpenID Connect Core provider metadata.
pub type CoreProviderMetadata = ProviderMetadata<
    EmptyAdditionalProviderMetadata,
    CoreAuthDisplay,
    CoreClientAuthMethod,
    CoreClaimName,
    CoreClaimType,
    CoreGrantType,
    CoreJweContentEncryptionAlgorithm,
    CoreJweKeyManagementAlgorithm,
    CoreJsonWebKey,
    CoreResponseMode,
    CoreResponseType,
    CoreSubjectIdentifierType,
>;

/// OpenID Connect Core user info claims.
pub type CoreUserInfoClaims = UserInfoClaims<EmptyAdditionalClaims, CoreGenderClaim>;

/// OpenID Connect Core user info JSON Web Token.
pub type CoreUserInfoJsonWebToken = UserInfoJsonWebToken<
    EmptyAdditionalClaims,
    CoreGenderClaim,
    CoreJweContentEncryptionAlgorithm,
    CoreJwsSigningAlgorithm,
>;

/// OpenID Connect Core user info verifier.
pub type CoreUserInfoVerifier<'a> =
    UserInfoVerifier<'a, CoreJweContentEncryptionAlgorithm, CoreJsonWebKey>;

/// OpenID Connect Core client application type.
///
/// These values are defined in
/// [Section 2 of OpenID Connect Dynamic Client Registration 1.0](
///     http://openid.net/specs/openid-connect-registration-1_0.html#ClientMetadata).
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum CoreApplicationType {
    /// Native Clients MUST only register `redirect_uri`s using custom URI schemes or URLs using
    /// the `http` scheme with `localhost` as the hostname. Authorization Servers MAY place
    /// additional constraints on Native Clients.
    Native,
    /// Web Clients using the OAuth Implicit Grant Type MUST only register URLs using the `https`
    /// scheme as `redirect_uri`s; they MUST NOT use `localhost` as the hostname.
    Web,
    /// An extension not defined by the OpenID Connect Dynamic Client Registration spec.
    Extension(String),
}
// FIXME: Once https://github.com/serde-rs/serde/issues/912 is resolved, use #[serde(other)] instead
// of custom serializer/deserializers. Right now this isn't possible because serde(other) only
// supports unit variants.
deserialize_from_str!(CoreApplicationType);
serialize_as_str!(CoreApplicationType);
impl CoreApplicationType {
    fn from_str(s: &str) -> Self {
        match s {
            "native" => CoreApplicationType::Native,
            "web" => CoreApplicationType::Web,
            ext => CoreApplicationType::Extension(ext.to_string()),
        }
    }
}
impl AsRef<str> for CoreApplicationType {
    fn as_ref(&self) -> &str {
        match *self {
            CoreApplicationType::Native => "native",
            CoreApplicationType::Web => "web",
            CoreApplicationType::Extension(ref ext) => ext.as_str(),
        }
    }
}
impl ApplicationType for CoreApplicationType {}

/// How the Authorization Server displays the authentication and consent user interface pages
/// to the End-User.
///
/// These values are defined in
/// [Section 3.1.2.1](http://openid.net/specs/openid-connect-core-1_0.html#AuthRequest).
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum CoreAuthDisplay {
    /// The Authorization Server SHOULD display the authentication and consent UI consistent
    /// with a full User Agent page view. If the display parameter is not specified, this is
    /// the default display mode.
    Page,
    /// The Authorization Server SHOULD display the authentication and consent UI consistent
    /// with a popup User Agent window. The popup User Agent window should be of an appropriate
    /// size for a login-focused dialog and should not obscure the entire window that it is
    /// popping up over.
    Popup,
    /// The Authorization Server SHOULD display the authentication and consent UI consistent
    /// with a device that leverages a touch interface.
    Touch,
    /// The Authorization Server SHOULD display the authentication and consent UI consistent
    /// with a "feature phone" type display.
    Wap,
    /// An extension not defined by the OpenID Connect Core spec.
    Extension(String),
}
deserialize_from_str!(CoreAuthDisplay);
serialize_as_str!(CoreAuthDisplay);
impl CoreAuthDisplay {
    fn from_str(s: &str) -> Self {
        match s {
            "page" => CoreAuthDisplay::Page,
            "popup" => CoreAuthDisplay::Popup,
            "touch" => CoreAuthDisplay::Touch,
            "wap" => CoreAuthDisplay::Wap,
            ext => CoreAuthDisplay::Extension(ext.to_string()),
        }
    }
}
impl AsRef<str> for CoreAuthDisplay {
    fn as_ref(&self) -> &str {
        match *self {
            CoreAuthDisplay::Page => "page",
            CoreAuthDisplay::Popup => "popup",
            CoreAuthDisplay::Touch => "touch",
            CoreAuthDisplay::Wap => "wap",
            CoreAuthDisplay::Extension(ref ext) => ext.as_str(),
        }
    }
}
impl AuthDisplay for CoreAuthDisplay {}
impl Display for CoreAuthDisplay {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.as_ref())
    }
}

/// Whether the Authorization Server should prompt the End-User for re-authentication and
/// consent.
///
/// These values are defined in
/// [Section 3.1.2.1](http://openid.net/specs/openid-connect-core-1_0.html#AuthRequest).
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum CoreAuthPrompt {
    /// The Authorization Server MUST NOT display any authentication or consent user interface
    /// pages. An error is returned if an End-User is not already authenticated or the Client
    /// does not have pre-configured consent for the requested Claims or does not fulfill other
    /// conditions for processing the request. The error code will typically be
    /// `login_required,` `interaction_required`, or another code defined in
    /// [Section 3.1.2.6](http://openid.net/specs/openid-connect-core-1_0.html#AuthError).
    /// This can be used as a method to check for existing authentication and/or consent.
    None,
    /// The Authorization Server SHOULD prompt the End-User for reauthentication. If it cannot
    /// reauthenticate the End-User, it MUST return an error, typically `login_required`.
    Login,
    /// The Authorization Server SHOULD prompt the End-User for consent before returning
    /// information to the Client. If it cannot obtain consent, it MUST return an error,
    /// typically `consent_required`.
    Consent,
    /// The Authorization Server SHOULD prompt the End-User to select a user account. This
    /// enables an End-User who has multiple accounts at the Authorization Server to select
    /// amongst the multiple accounts that they might have current sessions for. If it cannot
    /// obtain an account selection choice made by the End-User, it MUST return an error,
    /// typically `account_selection_required`.
    SelectAccount,
    /// An extension not defined by the OpenID Connect Core spec.
    Extension(String),
}
deserialize_from_str!(CoreAuthPrompt);
serialize_as_str!(CoreAuthPrompt);
impl CoreAuthPrompt {
    fn from_str(s: &str) -> Self {
        match s {
            "none" => CoreAuthPrompt::None,
            "login" => CoreAuthPrompt::Login,
            "consent" => CoreAuthPrompt::Consent,
            "select_account" => CoreAuthPrompt::SelectAccount,
            ext => CoreAuthPrompt::Extension(ext.to_string()),
        }
    }
}
impl AsRef<str> for CoreAuthPrompt {
    fn as_ref(&self) -> &str {
        match *self {
            CoreAuthPrompt::None => "none",
            CoreAuthPrompt::Login => "login",
            CoreAuthPrompt::Consent => "consent",
            CoreAuthPrompt::SelectAccount => "select_account",
            CoreAuthPrompt::Extension(ref ext) => ext.as_str(),
        }
    }
}
impl AuthPrompt for CoreAuthPrompt {}

impl Display for CoreAuthPrompt {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.as_ref())
    }
}

new_type![
    /// OpenID Connect Core claim name.
    #[derive(Deserialize, Hash, Ord, PartialOrd, Serialize)]
    CoreClaimName(String)
];
impl ClaimName for CoreClaimName {}

/// Representation of a Claim Value.
///
/// See [Section 5.6](http://openid.net/specs/openid-connect-core-1_0.html#ClaimTypes) for
/// further information.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum CoreClaimType {
    /// Aggregated Claim Type.
    ///
    /// See [Section 5.6.2](
    ///     http://openid.net/specs/openid-connect-core-1_0.html#AggregatedDistributedClaims)
    /// for details.
    Aggregated,
    /// Distributed Claim Type.
    ///
    /// See [Section 5.6.2](
    ///     http://openid.net/specs/openid-connect-core-1_0.html#AggregatedDistributedClaims)
    /// for details.
    Distributed,
    /// Normal Claims are represented as members in a JSON object. The Claim Name is the member
    /// name and the Claim Value is the member value.
    Normal,
    /// An extension not defined by the OpenID Connect Core spec.
    Extension(String),
}
deserialize_from_str!(CoreClaimType);
serialize_as_str!(CoreClaimType);
impl CoreClaimType {
    fn from_str(s: &str) -> Self {
        match s {
            "normal" => CoreClaimType::Normal,
            "aggregated" => CoreClaimType::Aggregated,
            "distributed" => CoreClaimType::Distributed,
            ext => CoreClaimType::Extension(ext.to_string()),
        }
    }
}
impl AsRef<str> for CoreClaimType {
    fn as_ref(&self) -> &str {
        match *self {
            CoreClaimType::Normal => "normal",
            CoreClaimType::Aggregated => "aggregated",
            CoreClaimType::Distributed => "distributed",
            CoreClaimType::Extension(ref ext) => ext.as_str(),
        }
    }
}
impl ClaimType for CoreClaimType {}

/// OpenID Connect Core client authentication method.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum CoreClientAuthMethod {
    /// Client secret passed via the HTTP Basic authentication scheme.
    ClientSecretBasic,
    /// Client authentication using a JSON Web Token signed with the client secret used as an HMAC
    /// key.
    ClientSecretJwt,
    /// Client secret passed via the POST request body.
    ClientSecretPost,
    /// JSON Web Token signed with a private key whose public key has been previously registered
    /// with the OpenID Connect provider.
    PrivateKeyJwt,
    /// The Client does not authenticate itself at the Token Endpoint, either because it uses only
    /// the Implicit Flow (and so does not use the Token Endpoint) or because it is a Public Client
    /// with no Client Secret or other authentication mechanism.
    None,
    /// An extension not defined by the OpenID Connect Core spec.
    Extension(String),
}
deserialize_from_str!(CoreClientAuthMethod);
serialize_as_str!(CoreClientAuthMethod);
impl CoreClientAuthMethod {
    fn from_str(s: &str) -> Self {
        match s {
            "client_secret_basic" => CoreClientAuthMethod::ClientSecretBasic,
            "client_secret_jwt" => CoreClientAuthMethod::ClientSecretJwt,
            "client_secret_post" => CoreClientAuthMethod::ClientSecretPost,
            "private_key_jwt" => CoreClientAuthMethod::PrivateKeyJwt,
            "none" => CoreClientAuthMethod::None,
            ext => CoreClientAuthMethod::Extension(ext.to_string()),
        }
    }
}
impl AsRef<str> for CoreClientAuthMethod {
    fn as_ref(&self) -> &str {
        match *self {
            CoreClientAuthMethod::ClientSecretBasic => "client_secret_basic",
            CoreClientAuthMethod::ClientSecretJwt => "client_secret_jwt",
            CoreClientAuthMethod::ClientSecretPost => "client_secret_post",
            CoreClientAuthMethod::PrivateKeyJwt => "private_key_jwt",
            CoreClientAuthMethod::None => "none",
            CoreClientAuthMethod::Extension(ref ext) => ext.as_str(),
        }
    }
}
impl ClientAuthMethod for CoreClientAuthMethod {}

new_type![
    /// OpenID Connect Core gender claim.
    #[derive(Deserialize, Hash, Ord, PartialOrd, Serialize)]
    CoreGenderClaim(String)
];
impl GenderClaim for CoreGenderClaim {}

/// OpenID Connect Core grant type.
///
// These are defined in various specs, including the Client Registration spec:
//   http://openid.net/specs/openid-connect-registration-1_0.html#ClientMetadata
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CoreGrantType {
    /// Authorization code grant.
    AuthorizationCode,
    /// Client credentials grant.
    ClientCredentials,
    /// Device Authorization Grant as described in [RFC 8628](https://tools.ietf.org/html/rfc8628).
    DeviceCode,
    /// Implicit grant.
    Implicit,
    /// JWT-based authentication as described in [RFC 7523](https://tools.ietf.org/html/rfc7523).
    JwtBearer,
    /// End user password grant.
    Password,
    /// Refresh token grant.
    RefreshToken,
    /// An extension not defined by any of the supported specifications.
    Extension(String),
}
deserialize_from_str!(CoreGrantType);
serialize_as_str!(CoreGrantType);
impl CoreGrantType {
    fn from_str(s: &str) -> Self {
        match s {
            "authorization_code" => CoreGrantType::AuthorizationCode,
            "client_credentials" => CoreGrantType::ClientCredentials,
            "urn:ietf:params:oauth:grant-type:device_code" => CoreGrantType::DeviceCode,
            "implicit" => CoreGrantType::Implicit,
            "urn:ietf:params:oauth:grant-type:jwt-bearer" => CoreGrantType::JwtBearer,
            "password" => CoreGrantType::Password,
            "refresh_token" => CoreGrantType::RefreshToken,
            ext => CoreGrantType::Extension(ext.to_string()),
        }
    }
}
impl AsRef<str> for CoreGrantType {
    fn as_ref(&self) -> &str {
        match *self {
            CoreGrantType::AuthorizationCode => "authorization_code",
            CoreGrantType::ClientCredentials => "client_credentials",
            CoreGrantType::DeviceCode => "urn:ietf:params:oauth:grant-type:device_code",
            CoreGrantType::Implicit => "implicit",
            CoreGrantType::JwtBearer => "urn:ietf:params:oauth:grant-type:jwt-bearer",
            CoreGrantType::Password => "password",
            CoreGrantType::RefreshToken => "refresh_token",
            CoreGrantType::Extension(ref ext) => ext.as_str(),
        }
    }
}
impl GrantType for CoreGrantType {}

/// OpenID Connect Core JWE encryption algorithms.
///
/// These algorithms represent the `enc` header parameter values for JSON Web Encryption.
/// The values are described in
/// [Section 5.1 of RFC 7518](https://tools.ietf.org/html/rfc7518#section-5.1).
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[non_exhaustive]
pub enum CoreJweContentEncryptionAlgorithm {
    /// AES-128 CBC HMAC SHA-256 authenticated encryption.
    #[serde(rename = "A128CBC-HS256")]
    Aes128CbcHmacSha256,
    /// AES-192 CBC HMAC SHA-384 authenticated encryption.
    #[serde(rename = "A192CBC-HS384")]
    Aes192CbcHmacSha384,
    /// AES-256 CBC HMAC SHA-512 authenticated encryption.
    #[serde(rename = "A256CBC-HS512")]
    Aes256CbcHmacSha512,
    /// AES-128 GCM.
    #[serde(rename = "A128GCM")]
    Aes128Gcm,
    /// AES-192 GCM.
    #[serde(rename = "A192GCM")]
    Aes192Gcm,
    /// AES-256 GCM.
    #[serde(rename = "A256GCM")]
    Aes256Gcm,
}
impl JweContentEncryptionAlgorithm for CoreJweContentEncryptionAlgorithm {
    type KeyType = CoreJsonWebKeyType;

    fn key_type(&self) -> Result<CoreJsonWebKeyType, String> {
        Ok(CoreJsonWebKeyType::Symmetric)
    }
}

/// OpenID Connect Core JWE key management algorithms.
///
/// These algorithms represent the `alg` header parameter values for JSON Web Encryption.
/// They are used to encrypt the Content Encryption Key (CEK) to produce the JWE Encrypted Key, or
/// to use key agreement to agree upon the CEK. The values are described in
/// [Section 4.1 of RFC 7518](https://tools.ietf.org/html/rfc7518#section-4.1).
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[non_exhaustive]
pub enum CoreJweKeyManagementAlgorithm {
    /// RSAES-PKCS1-V1_5.
    #[serde(rename = "RSA1_5")]
    RsaPkcs1V15,
    /// RSAES OAEP using default parameters.
    #[serde(rename = "RSA-OAEP")]
    RsaOaep,
    /// RSAES OAEP using SHA-256 and MGF1 with SHA-256.
    #[serde(rename = "RSA-OAEP-256")]
    RsaOaepSha256,
    /// AES-128 Key Wrap.
    #[serde(rename = "A128KW")]
    AesKeyWrap128,
    /// AES-192 Key Wrap.
    #[serde(rename = "A192KW")]
    AesKeyWrap192,
    /// AES-256 Key Wrap.
    #[serde(rename = "A256KW")]
    AesKeyWrap256,
    /// Direct use of a shared symmetric key as the Content Encryption Key (CEK).
    #[serde(rename = "dir")]
    Direct,
    /// Elliptic Curve Diffie-Hellman Ephemeral Static key agreement using Concat KDF.
    #[serde(rename = "ECDH-ES")]
    EcdhEs,
    /// ECDH-ES using Concat KDF and CEK wrapped with AES-128 Key Wrap.
    #[serde(rename = "ECDH-ES+A128KW")]
    EcdhEsAesKeyWrap128,
    /// ECDH-ES using Concat KDF and CEK wrapped with AES-192 Key Wrap.
    #[serde(rename = "ECDH-ES+A192KW")]
    EcdhEsAesKeyWrap192,
    /// ECDH-ES using Concat KDF and CEK wrapped with AES-256 Key Wrap.
    #[serde(rename = "ECDH-ES+A256KW")]
    EcdhEsAesKeyWrap256,
    /// Key wrapping with AES GCM using 128 bit key.
    #[serde(rename = "A128GCMKW")]
    Aes128Gcm,
    /// Key wrapping with AES GCM using 192 bit key.
    #[serde(rename = "A192GCMKW")]
    Aes192Gcm,
    /// Key wrapping with AES GCM using 256 bit key.
    #[serde(rename = "A256GCMKW")]
    Aes256Gcm,
    /// PBES2 with HMAC SHA-256 wrapped with AES-128 Key Wrap.
    #[serde(rename = "PBES2-HS256+A128KW")]
    PbEs2HmacSha256AesKeyWrap128,
    /// PBES2 with HMAC SHA-384 wrapped with AES-192 Key Wrap.
    #[serde(rename = "PBES2-HS384+A192KW")]
    PbEs2HmacSha384AesKeyWrap192,
    /// PBES2 with HMAC SHA-512 wrapped with AES-256 Key Wrap.
    #[serde(rename = "PBES2-HS512+A256KW")]
    PbEs2HmacSha512AesKeyWrap256,
}
impl JweKeyManagementAlgorithm for CoreJweKeyManagementAlgorithm {}

/// OpenID Connect Core JWS signing algorithms.
///
/// These algorithms represent the `alg` header parameter values for JSON Web Signature.
/// They are used to digitally sign or create a MAC of the contents of the JWS Protected Header and
/// the JWS Payload. The values are described in
/// [Section 3.1 of RFC 7518](https://tools.ietf.org/html/rfc7518#section-3.1).
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[non_exhaustive]
pub enum CoreJwsSigningAlgorithm {
    /// HMAC using SHA-256 (currently unsupported).
    #[serde(rename = "HS256")]
    HmacSha256,
    /// HMAC using SHA-384 (currently unsupported).
    #[serde(rename = "HS384")]
    HmacSha384,
    /// HMAC using SHA-512 (currently unsupported).
    #[serde(rename = "HS512")]
    HmacSha512,
    /// RSA SSA PKCS#1 v1.5 using SHA-256.
    #[serde(rename = "RS256")]
    RsaSsaPkcs1V15Sha256,
    /// RSA SSA PKCS#1 v1.5 using SHA-384.
    #[serde(rename = "RS384")]
    RsaSsaPkcs1V15Sha384,
    /// RSA SSA PKCS#1 v1.5 using SHA-512.
    #[serde(rename = "RS512")]
    RsaSsaPkcs1V15Sha512,
    /// ECDSA using P-256 and SHA-256 (currently unsupported).
    #[serde(rename = "ES256")]
    EcdsaP256Sha256,
    /// ECDSA using P-384 and SHA-384 (currently unsupported).
    #[serde(rename = "ES384")]
    EcdsaP384Sha384,
    /// ECDSA using P-521 and SHA-512 (currently unsupported).
    #[serde(rename = "ES512")]
    EcdsaP521Sha512,
    /// RSA SSA-PSS using SHA-256 and MGF1 with SHA-256.
    #[serde(rename = "PS256")]
    RsaSsaPssSha256,
    /// RSA SSA-PSS using SHA-384 and MGF1 with SHA-384.
    #[serde(rename = "PS384")]
    RsaSsaPssSha384,
    /// RSA SSA-PSS using SHA-512 and MGF1 with SHA-512.
    #[serde(rename = "PS512")]
    RsaSsaPssSha512,
    /// EdDSA signature (algorithm depends on `crv` header).
    #[serde(rename = "EdDSA")]
    EdDsa,
    /// No digital signature or MAC performed.
    ///
    /// # Security Warning
    ///
    /// This algorithm provides no security over the integrity of the JSON Web Token. Clients
    /// should be careful not to rely on unsigned JWT's for security purposes. See
    /// [Critical vulnerabilities in JSON Web Token libraries](
    ///     https://auth0.com/blog/critical-vulnerabilities-in-json-web-token-libraries/) for
    /// further discussion.
    #[serde(rename = "none")]
    None,
}
impl JwsSigningAlgorithm for CoreJwsSigningAlgorithm {
    type KeyType = CoreJsonWebKeyType;

    fn key_type(&self) -> Option<CoreJsonWebKeyType> {
        match *self {
            CoreJwsSigningAlgorithm::HmacSha256
            | CoreJwsSigningAlgorithm::HmacSha384
            | CoreJwsSigningAlgorithm::HmacSha512 => Some(CoreJsonWebKeyType::Symmetric),
            CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256
            | CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha384
            | CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha512
            | CoreJwsSigningAlgorithm::RsaSsaPssSha256
            | CoreJwsSigningAlgorithm::RsaSsaPssSha384
            | CoreJwsSigningAlgorithm::RsaSsaPssSha512 => Some(CoreJsonWebKeyType::RSA),
            CoreJwsSigningAlgorithm::EcdsaP256Sha256
            | CoreJwsSigningAlgorithm::EcdsaP384Sha384
            | CoreJwsSigningAlgorithm::EcdsaP521Sha512 => Some(CoreJsonWebKeyType::EllipticCurve),
            CoreJwsSigningAlgorithm::EdDsa => Some(CoreJsonWebKeyType::OctetKeyPair),
            CoreJwsSigningAlgorithm::None => None,
        }
    }

    fn uses_shared_secret(&self) -> bool {
        self.key_type()
            .map(|kty| kty == CoreJsonWebKeyType::Symmetric)
            .unwrap_or(false)
    }

    fn rsa_sha_256() -> Self {
        CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256
    }
}

/// OpenID Connect Core authentication error response types.
///
/// This type represents errors returned in a redirect from the Authorization Endpoint to the
/// client's redirect URI.
///
/// These values are defined across both
/// [Section 4.1.2.1](https://tools.ietf.org/html/rfc6749#section-4.1.2.1) of RFC 6749 and
/// [Section 3.1.2.6](https://openid.net/specs/openid-connect-core-1_0.html#AuthError) of the
/// OpenID Connect Core spec.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum CoreAuthErrorResponseType {
    /// The resource owner or authorization server denied the request.
    AccessDenied,
    /// The End-User is REQUIRED to select a session at the Authorization Server. The End-User MAY
    /// be authenticated at the Authorization Server with different associated accounts, but the
    /// End-User did not select a session. This error MAY be returned when the `prompt` parameter
    /// value in the Authentication Request is `none`, but the Authentication Request cannot be
    /// completed without displaying a user interface to prompt for a session to use.
    AccountSelectionRequired,
    /// The Authorization Server requires End-User consent. This error MAY be returned when the
    /// `prompt` parameter value in the Authentication Request is `none`, but the Authentication
    /// Request cannot be completed without displaying a user interface for End-User consent.
    ConsentRequired,
    /// The Authorization Server requires End-User interaction of some form to proceed. This error
    /// MAY be returned when the `prompt` parameter value in the Authentication Request is `none`,
    /// but the Authentication Request cannot be completed without displaying a user interface for
    /// End-User interaction.
    InteractionRequired,
    /// The request is missing a required parameter, includes an invalid parameter value, includes
    /// a parameter more than once, or is otherwise malformed.
    InvalidRequest,
    /// The `request` parameter contains an invalid Request Object.
    InvalidRequestObject,
    /// The `request_uri` in the Authorization Request returns an error or contains invalid data.
    InvalidRequestUri,
    /// The requested scope is invalid, unknown, or malformed.
    InvalidScope,
    /// The Authorization Server requires End-User authentication. This error MAY be returned when
    /// the `prompt` parameter value in the Authentication Request is `none`, but the Authentication
    /// Request cannot be completed without displaying a user interface for End-User authentication.
    LoginRequired,
    /// The OpenID Connect Provider does not support use of the `registration` parameter.
    RegistrationNotSupported,
    /// The OpenID Connect Provider does not support use of the `request` parameter.
    RequestNotSupported,
    /// The OpenID Connect Provider does not support use of the `request_uri` parameter.
    RequestUriNotSupported,
    /// The authorization server encountered an unexpected condition that prevented it from
    /// fulfilling the request. (This error code is needed because a 500 Internal Server Error HTTP
    /// status code cannot be returned to the client via an HTTP redirect.)
    ServerError,
    /// The authorization server is currently unable to handle the request due to a temporary
    /// overloading or maintenance of the server.  (This error code is needed because a 503 Service
    /// Unavailable HTTP status code cannot be returned to the client via an HTTP redirect.)
    TemporarilyUnavailable,
    /// The client is not authorized to request an authorization code using this method.
    UnauthorizedClient,
    /// The authorization server does not support obtaining an authorization code using this method.
    UnsupportedResponseType,
    /// An extension not defined by any of the supported specifications.
    Extension(String),
}
deserialize_from_str!(CoreAuthErrorResponseType);
serialize_as_str!(CoreAuthErrorResponseType);
impl CoreAuthErrorResponseType {
    fn from_str(s: &str) -> Self {
        match s {
            "access_denied" => CoreAuthErrorResponseType::AccessDenied,
            "account_selection_required" => CoreAuthErrorResponseType::AccountSelectionRequired,
            "consent_required" => CoreAuthErrorResponseType::ConsentRequired,
            "interaction_required" => CoreAuthErrorResponseType::InteractionRequired,
            "invalid_request" => CoreAuthErrorResponseType::InvalidRequest,
            "invalid_request_object" => CoreAuthErrorResponseType::InvalidRequestObject,
            "invalid_request_uri" => CoreAuthErrorResponseType::InvalidRequestUri,
            "invalid_scope" => CoreAuthErrorResponseType::InvalidScope,
            "login_required" => CoreAuthErrorResponseType::LoginRequired,
            "registration_not_supported" => CoreAuthErrorResponseType::RegistrationNotSupported,
            "request_not_supported" => CoreAuthErrorResponseType::RequestNotSupported,
            "request_uri_not_supported" => CoreAuthErrorResponseType::RequestUriNotSupported,
            "server_error" => CoreAuthErrorResponseType::ServerError,
            "temporarily_unavailable" => CoreAuthErrorResponseType::TemporarilyUnavailable,
            "unauthorized_client" => CoreAuthErrorResponseType::UnauthorizedClient,
            "unsupported_response_type" => CoreAuthErrorResponseType::UnsupportedResponseType,
            ext => CoreAuthErrorResponseType::Extension(ext.to_string()),
        }
    }
}
impl AsRef<str> for CoreAuthErrorResponseType {
    fn as_ref(&self) -> &str {
        match *self {
            CoreAuthErrorResponseType::AccessDenied => "access_denied",
            CoreAuthErrorResponseType::AccountSelectionRequired => "account_selection_required",
            CoreAuthErrorResponseType::ConsentRequired => "consent_required",
            CoreAuthErrorResponseType::InteractionRequired => "interaction_required",
            CoreAuthErrorResponseType::InvalidRequest => "invalid_request",
            CoreAuthErrorResponseType::InvalidRequestObject => "invalid_request_object",
            CoreAuthErrorResponseType::InvalidRequestUri => "invalid_request_uri",
            CoreAuthErrorResponseType::InvalidScope => "invalid_scope",
            CoreAuthErrorResponseType::LoginRequired => "login_required",
            CoreAuthErrorResponseType::RegistrationNotSupported => "registration_not_supported",
            CoreAuthErrorResponseType::RequestNotSupported => "request_not_supported",
            CoreAuthErrorResponseType::RequestUriNotSupported => "request_uri_not_supported",
            CoreAuthErrorResponseType::ServerError => "server_error",
            CoreAuthErrorResponseType::TemporarilyUnavailable => "temporarily_unavailable",
            CoreAuthErrorResponseType::UnauthorizedClient => "unauthorized_client",
            CoreAuthErrorResponseType::UnsupportedResponseType => "unsupported_response_type",
            CoreAuthErrorResponseType::Extension(ref ext) => ext.as_str(),
        }
    }
}

/// OpenID Connect Core registration error response type.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum CoreRegisterErrorResponseType {
    /// The value of one of the Client Metadata fields is invalid and the server has rejected this
    /// request. Note that an Authorization Server MAY choose to substitute a valid value for any
    /// requested parameter of a Client's Metadata.
    InvalidClientMetadata,
    /// The value of one or more `redirect_uri`s is invalid.
    InvalidRedirectUri,
    /// An extension not defined by any of the supported specifications.
    Extension(String),
}
deserialize_from_str!(CoreRegisterErrorResponseType);
serialize_as_str!(CoreRegisterErrorResponseType);
impl CoreRegisterErrorResponseType {
    fn from_str(s: &str) -> Self {
        match s {
            "invalid_client_metadata" => CoreRegisterErrorResponseType::InvalidClientMetadata,
            "invalid_redirect_uri" => CoreRegisterErrorResponseType::InvalidRedirectUri,
            ext => CoreRegisterErrorResponseType::Extension(ext.to_string()),
        }
    }
}
impl AsRef<str> for CoreRegisterErrorResponseType {
    fn as_ref(&self) -> &str {
        match *self {
            CoreRegisterErrorResponseType::InvalidClientMetadata => "invalid_client_metadata",
            CoreRegisterErrorResponseType::InvalidRedirectUri => "invalid_redirect_uri",
            CoreRegisterErrorResponseType::Extension(ref ext) => ext.as_str(),
        }
    }
}
impl ErrorResponseType for CoreRegisterErrorResponseType {}
impl RegisterErrorResponseType for CoreRegisterErrorResponseType {}
impl Display for CoreRegisterErrorResponseType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.as_ref())
    }
}

/// OpenID Connect Core response mode.
///
/// Informs the Authorization Server of the mechanism to be used for returning Authorization
/// Response parameters from the Authorization Endpoint.
///
/// The default Response Mode for the OAuth 2.0 `code` Response Type is the `query` encoding.
/// The default Response Mode for the OAuth 2.0 `token` Response Type is the `fragment` encoding.
/// These values are defined in
/// [OAuth 2.0 Multiple Response Type Encoding Practices](
///     http://openid.net/specs/oauth-v2-multiple-response-types-1_0.html#ResponseTypesAndModes)
/// and [OAuth 2.0 Form Post Response Mode](
///     http://openid.net/specs/oauth-v2-form-post-response-mode-1_0.html#FormPostResponseMode).
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum CoreResponseMode {
    /// In this mode, Authorization Response parameters are encoded in the query string added to
    /// the `redirect_uri` when redirecting back to the Client.
    Query,
    /// In this mode, Authorization Response parameters are encoded in the fragment added to the
    /// `redirect_uri` when redirecting back to the Client.
    Fragment,
    /// In this mode, Authorization Response parameters are encoded as HTML form values that are
    /// auto-submitted in the User Agent, and thus are transmitted via the HTTP `POST` method to the
    /// Client, with the result parameters being encoded in the body using the
    /// `application/x-www-form-urlencoded` format. The `action` attribute of the form MUST be the
    /// Client's Redirection URI. The method of the form attribute MUST be `POST`. Because the
    /// Authorization Response is intended to be used only once, the Authorization Server MUST
    /// instruct the User Agent (and any intermediaries) not to store or reuse the content of the
    /// response.
    ///
    /// Any technique supported by the User Agent MAY be used to cause the submission of the form,
    /// and any form content necessary to support this MAY be included, such as submit controls and
    /// client-side scripting commands. However, the Client MUST be able to process the message
    /// without regard for the mechanism by which the form submission was initiated.
    ///
    /// See [OAuth 2.0 Form Post Response Mode](
    ///     http://openid.net/specs/oauth-v2-form-post-response-mode-1_0.html#FormPostResponseMode)
    /// for further information.
    FormPost,
    /// An extension not defined by any of the supported specifications.
    Extension(String),
}
deserialize_from_str!(CoreResponseMode);
serialize_as_str!(CoreResponseMode);
impl CoreResponseMode {
    fn from_str(s: &str) -> Self {
        match s {
            "query" => CoreResponseMode::Query,
            "fragment" => CoreResponseMode::Fragment,
            "form_post" => CoreResponseMode::FormPost,
            ext => CoreResponseMode::Extension(ext.to_string()),
        }
    }
}
impl AsRef<str> for CoreResponseMode {
    fn as_ref(&self) -> &str {
        match *self {
            CoreResponseMode::Query => "query",
            CoreResponseMode::Fragment => "fragment",
            CoreResponseMode::FormPost => "form_post",
            CoreResponseMode::Extension(ref ext) => ext.as_str(),
        }
    }
}
impl ResponseMode for CoreResponseMode {}

/// OpenID Connect Core response type.
///
/// Informs the Authorization Server of the desired authorization processing flow, including what
/// parameters are returned from the endpoints used.
///
/// This type represents a single Response Type. Multiple Response Types are represented via the
/// `ResponseTypes` type, which wraps a `Vec<ResponseType>`.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum CoreResponseType {
    /// Used by the OAuth 2.0 Authorization Code Flow.
    Code,
    /// When supplied as the `response_type` parameter in an OAuth 2.0 Authorization Request, a
    /// successful response MUST include the parameter `id_token`.
    IdToken,
    /// When supplied as the `response_type` parameter in an OAuth 2.0 Authorization Request, the
    /// Authorization Server SHOULD NOT return an OAuth 2.0 Authorization Code, Access Token, Access
    /// Token Type, or ID Token in a successful response to the grant request. If a `redirect_uri`
    /// is supplied, the User Agent SHOULD be redirected there after granting or denying access.
    /// The request MAY include a `state` parameter, and if so, the Authorization Server MUST echo
    /// its value as a response parameter when issuing either a successful response or an error
    /// response. The default Response Mode for this Response Type is the query encoding. Both
    /// successful and error responses SHOULD be returned using the supplied Response Mode, or if
    /// none is supplied, using the default Response Mode.
    ///
    /// This Response Type is not generally used with OpenID Connect but may be supported by the
    /// Authorization Server.
    None,
    /// Used by the OAuth 2.0 Implicit Flow.
    Token,
    /// An extension not defined by the OpenID Connect Core spec.
    Extension(String),
}
deserialize_from_str!(CoreResponseType);
serialize_as_str!(CoreResponseType);
impl CoreResponseType {
    fn from_str(s: &str) -> Self {
        match s {
            "code" => CoreResponseType::Code,
            "id_token" => CoreResponseType::IdToken,
            "none" => CoreResponseType::None,
            "token" => CoreResponseType::Token,
            ext => CoreResponseType::Extension(ext.to_string()),
        }
    }
}
impl AsRef<str> for CoreResponseType {
    fn as_ref(&self) -> &str {
        match *self {
            CoreResponseType::Code => "code",
            CoreResponseType::IdToken => "id_token",
            CoreResponseType::None => "none",
            CoreResponseType::Token => "token",
            CoreResponseType::Extension(ref ext) => ext.as_str(),
        }
    }
}
impl ResponseType for CoreResponseType {
    fn to_oauth2(&self) -> OAuth2ResponseType {
        OAuth2ResponseType::new(self.as_ref().to_string())
    }
}

/// OpenID Connect Core Subject Identifier type.
///
/// A Subject Identifier is a locally unique and never reassigned identifier within the Issuer for
/// the End-User, which is intended to be consumed by the Client.
///
/// See [Section 8](http://openid.net/specs/openid-connect-core-1_0.html#SubjectIDTypes) for
/// further information.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum CoreSubjectIdentifierType {
    /// This provides a different `sub` value to each Client, so as not to enable Clients to
    /// correlate the End-User's activities without permission.
    Pairwise,
    /// This provides the same `sub` (subject) value to all Clients. It is the default if the
    /// provider has no `subject_types_supported` element in its discovery document.
    Public,
    /// An extension not defined by the OpenID Connect Core spec.
    Extension(String),
}
deserialize_from_str!(CoreSubjectIdentifierType);
serialize_as_str!(CoreSubjectIdentifierType);
impl CoreSubjectIdentifierType {
    fn from_str(s: &str) -> Self {
        match s {
            "pairwise" => CoreSubjectIdentifierType::Pairwise,
            "public" => CoreSubjectIdentifierType::Public,
            ext => CoreSubjectIdentifierType::Extension(ext.to_string()),
        }
    }
}
impl AsRef<str> for CoreSubjectIdentifierType {
    fn as_ref(&self) -> &str {
        match *self {
            CoreSubjectIdentifierType::Pairwise => "pairwise",
            CoreSubjectIdentifierType::Public => "public",
            CoreSubjectIdentifierType::Extension(ref ext) => ext.as_str(),
        }
    }
}
impl SubjectIdentifierType for CoreSubjectIdentifierType {}

pub(crate) fn base64_url_safe_no_pad() -> GeneralPurpose {
    GeneralPurpose::new(&URL_SAFE, NO_PAD.with_decode_allow_trailing_bits(true))
}

#[cfg(test)]
mod tests;
