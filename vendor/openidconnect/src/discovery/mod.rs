use crate::http_utils::{check_content_type, MIME_TYPE_JSON};
use crate::{
    AsyncHttpClient, AuthDisplay, AuthUrl, AuthenticationContextClass, ClaimName, ClaimType,
    ClientAuthMethod, GrantType, HttpRequest, HttpResponse, IssuerUrl, JsonWebKey, JsonWebKeySet,
    JsonWebKeySetUrl, JweContentEncryptionAlgorithm, JweKeyManagementAlgorithm,
    JwsSigningAlgorithm, LanguageTag, OpPolicyUrl, OpTosUrl, RegistrationUrl, ResponseMode,
    ResponseType, ResponseTypes, Scope, ServiceDocUrl, SubjectIdentifierType, SyncHttpClient,
    TokenUrl, UserInfoUrl,
};

use http::header::{HeaderValue, ACCEPT};
use http::method::Method;
use http::status::StatusCode;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, skip_serializing_none, VecSkipError};
use thiserror::Error;

use std::fmt::Debug;
use std::future::Future;

#[cfg(test)]
mod tests;

const CONFIG_URL_SUFFIX: &str = ".well-known/openid-configuration";

/// Trait for adding extra fields to [`ProviderMetadata`].
pub trait AdditionalProviderMetadata: Clone + Debug + DeserializeOwned + Serialize {}

// In order to support serde flatten, this must be an empty struct rather than an empty
// tuple struct.
/// Empty (default) extra [`ProviderMetadata`] fields.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
pub struct EmptyAdditionalProviderMetadata {}
impl AdditionalProviderMetadata for EmptyAdditionalProviderMetadata {}

/// Provider metadata returned by [OpenID Connect Discovery](
/// https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderMetadata).
#[serde_as]
#[skip_serializing_none]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[allow(clippy::type_complexity)]
pub struct ProviderMetadata<A, AD, CA, CN, CT, G, JE, JK, K, RM, RT, S>
where
    A: AdditionalProviderMetadata,
    AD: AuthDisplay,
    CA: ClientAuthMethod,
    CN: ClaimName,
    CT: ClaimType,
    G: GrantType,
    JE: JweContentEncryptionAlgorithm<
        KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType,
    >,
    JK: JweKeyManagementAlgorithm,
    K: JsonWebKey,
    RM: ResponseMode,
    RT: ResponseType,
    S: SubjectIdentifierType,
{
    issuer: IssuerUrl,
    authorization_endpoint: AuthUrl,
    token_endpoint: Option<TokenUrl>,
    userinfo_endpoint: Option<UserInfoUrl>,
    jwks_uri: JsonWebKeySetUrl,
    #[serde(default = "JsonWebKeySet::default", skip)]
    jwks: JsonWebKeySet<K>,
    registration_endpoint: Option<RegistrationUrl>,
    scopes_supported: Option<Vec<Scope>>,
    #[serde(bound(deserialize = "RT: ResponseType"))]
    response_types_supported: Vec<ResponseTypes<RT>>,
    #[serde(bound(deserialize = "RM: ResponseMode"))]
    response_modes_supported: Option<Vec<RM>>,
    #[serde(bound(deserialize = "G: GrantType"))]
    grant_types_supported: Option<Vec<G>>,
    acr_values_supported: Option<Vec<AuthenticationContextClass>>,
    #[serde(bound(deserialize = "S: SubjectIdentifierType"))]
    subject_types_supported: Vec<S>,
    #[serde(bound(deserialize = "K: JsonWebKey"))]
    #[serde_as(as = "VecSkipError<_>")]
    id_token_signing_alg_values_supported: Vec<K::SigningAlgorithm>,
    #[serde(
        bound(deserialize = "JK: JweKeyManagementAlgorithm"),
        default = "Option::default"
    )]
    #[serde_as(as = "Option<VecSkipError<_>>")]
    id_token_encryption_alg_values_supported: Option<Vec<JK>>,
    #[serde(
        bound(
            deserialize = "JE: JweContentEncryptionAlgorithm<KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType>"
        ),
        default = "Option::default"
    )]
    #[serde_as(as = "Option<VecSkipError<_>>")]
    id_token_encryption_enc_values_supported: Option<Vec<JE>>,
    #[serde(bound(deserialize = "K: JsonWebKey"), default = "Option::default")]
    #[serde_as(as = "Option<VecSkipError<_>>")]
    userinfo_signing_alg_values_supported: Option<Vec<K::SigningAlgorithm>>,
    #[serde(
        bound(deserialize = "JK: JweKeyManagementAlgorithm"),
        default = "Option::default"
    )]
    #[serde_as(as = "Option<VecSkipError<_>>")]
    userinfo_encryption_alg_values_supported: Option<Vec<JK>>,
    #[serde(
        bound(
            deserialize = "JE: JweContentEncryptionAlgorithm<KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType>"
        ),
        default = "Option::default"
    )]
    #[serde_as(as = "Option<VecSkipError<_>>")]
    userinfo_encryption_enc_values_supported: Option<Vec<JE>>,
    #[serde(bound(deserialize = "K: JsonWebKey"), default = "Option::default")]
    #[serde_as(as = "Option<VecSkipError<_>>")]
    request_object_signing_alg_values_supported: Option<Vec<K::SigningAlgorithm>>,
    #[serde(
        bound(deserialize = "JK: JweKeyManagementAlgorithm"),
        default = "Option::default"
    )]
    #[serde_as(as = "Option<VecSkipError<_>>")]
    request_object_encryption_alg_values_supported: Option<Vec<JK>>,
    #[serde(
        bound(
            deserialize = "JE: JweContentEncryptionAlgorithm<KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType>"
        ),
        default = "Option::default"
    )]
    #[serde_as(as = "Option<VecSkipError<_>>")]
    request_object_encryption_enc_values_supported: Option<Vec<JE>>,
    #[serde(bound(deserialize = "CA: ClientAuthMethod"))]
    token_endpoint_auth_methods_supported: Option<Vec<CA>>,
    #[serde(bound(deserialize = "K: JsonWebKey"), default = "Option::default")]
    #[serde_as(as = "Option<VecSkipError<_>>")]
    token_endpoint_auth_signing_alg_values_supported: Option<Vec<K::SigningAlgorithm>>,
    #[serde(bound(deserialize = "AD: AuthDisplay"))]
    display_values_supported: Option<Vec<AD>>,
    #[serde(bound(deserialize = "CT: ClaimType"))]
    claim_types_supported: Option<Vec<CT>>,
    #[serde(bound(deserialize = "CN: ClaimName"))]
    claims_supported: Option<Vec<CN>>,
    service_documentation: Option<ServiceDocUrl>,
    claims_locales_supported: Option<Vec<LanguageTag>>,
    ui_locales_supported: Option<Vec<LanguageTag>>,
    claims_parameter_supported: Option<bool>,
    request_parameter_supported: Option<bool>,
    request_uri_parameter_supported: Option<bool>,
    require_request_uri_registration: Option<bool>,
    op_policy_uri: Option<OpPolicyUrl>,
    op_tos_uri: Option<OpTosUrl>,

    #[serde(bound(deserialize = "A: AdditionalProviderMetadata"), flatten)]
    additional_metadata: A,
}
impl<A, AD, CA, CN, CT, G, JE, JK, K, RM, RT, S>
    ProviderMetadata<A, AD, CA, CN, CT, G, JE, JK, K, RM, RT, S>
where
    A: AdditionalProviderMetadata,
    AD: AuthDisplay,
    CA: ClientAuthMethod,
    CN: ClaimName,
    CT: ClaimType,
    G: GrantType,
    JE: JweContentEncryptionAlgorithm<
        KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType,
    >,
    JK: JweKeyManagementAlgorithm,
    K: JsonWebKey,
    RM: ResponseMode,
    RT: ResponseType,
    S: SubjectIdentifierType,
{
    /// Instantiates new provider metadata.
    pub fn new(
        issuer: IssuerUrl,
        authorization_endpoint: AuthUrl,
        jwks_uri: JsonWebKeySetUrl,
        response_types_supported: Vec<ResponseTypes<RT>>,
        subject_types_supported: Vec<S>,
        id_token_signing_alg_values_supported: Vec<K::SigningAlgorithm>,
        additional_metadata: A,
    ) -> Self {
        Self {
            issuer,
            authorization_endpoint,
            token_endpoint: None,
            userinfo_endpoint: None,
            jwks_uri,
            jwks: JsonWebKeySet::new(Vec::new()),
            registration_endpoint: None,
            scopes_supported: None,
            response_types_supported,
            response_modes_supported: None,
            grant_types_supported: None,
            acr_values_supported: None,
            subject_types_supported,
            id_token_signing_alg_values_supported,
            id_token_encryption_alg_values_supported: None,
            id_token_encryption_enc_values_supported: None,
            userinfo_signing_alg_values_supported: None,
            userinfo_encryption_alg_values_supported: None,
            userinfo_encryption_enc_values_supported: None,
            request_object_signing_alg_values_supported: None,
            request_object_encryption_alg_values_supported: None,
            request_object_encryption_enc_values_supported: None,
            token_endpoint_auth_methods_supported: None,
            token_endpoint_auth_signing_alg_values_supported: None,
            display_values_supported: None,
            claim_types_supported: None,
            claims_supported: None,
            service_documentation: None,
            claims_locales_supported: None,
            ui_locales_supported: None,
            claims_parameter_supported: None,
            request_parameter_supported: None,
            request_uri_parameter_supported: None,
            require_request_uri_registration: None,
            op_policy_uri: None,
            op_tos_uri: None,
            additional_metadata,
        }
    }

    field_getters_setters![
        pub self [self] ["provider metadata value"] {
            set_issuer -> issuer[IssuerUrl],
            set_authorization_endpoint -> authorization_endpoint[AuthUrl],
            set_token_endpoint -> token_endpoint[Option<TokenUrl>],
            set_userinfo_endpoint -> userinfo_endpoint[Option<UserInfoUrl>],
            set_jwks_uri -> jwks_uri[JsonWebKeySetUrl],
            set_jwks -> jwks[JsonWebKeySet<K>],
            set_registration_endpoint -> registration_endpoint[Option<RegistrationUrl>],
            set_scopes_supported -> scopes_supported[Option<Vec<Scope>>],
            set_response_types_supported -> response_types_supported[Vec<ResponseTypes<RT>>],
            set_response_modes_supported -> response_modes_supported[Option<Vec<RM>>],
            set_grant_types_supported -> grant_types_supported[Option<Vec<G>>],
            set_acr_values_supported
                -> acr_values_supported[Option<Vec<AuthenticationContextClass>>],
            set_subject_types_supported -> subject_types_supported[Vec<S>],
            set_id_token_signing_alg_values_supported
                -> id_token_signing_alg_values_supported[Vec<K::SigningAlgorithm>],
            set_id_token_encryption_alg_values_supported
                -> id_token_encryption_alg_values_supported[Option<Vec<JK>>],
            set_id_token_encryption_enc_values_supported
                -> id_token_encryption_enc_values_supported[Option<Vec<JE>>],
            set_userinfo_signing_alg_values_supported
                -> userinfo_signing_alg_values_supported[Option<Vec<K::SigningAlgorithm>>],
            set_userinfo_encryption_alg_values_supported
                -> userinfo_encryption_alg_values_supported[Option<Vec<JK>>],
            set_userinfo_encryption_enc_values_supported
                -> userinfo_encryption_enc_values_supported[Option<Vec<JE>>],
            set_request_object_signing_alg_values_supported
                -> request_object_signing_alg_values_supported[Option<Vec<K::SigningAlgorithm>>],
            set_request_object_encryption_alg_values_supported
                -> request_object_encryption_alg_values_supported[Option<Vec<JK>>],
            set_request_object_encryption_enc_values_supported
                -> request_object_encryption_enc_values_supported[Option<Vec<JE>>],
            set_token_endpoint_auth_methods_supported
                -> token_endpoint_auth_methods_supported[Option<Vec<CA>>],
            set_token_endpoint_auth_signing_alg_values_supported
                -> token_endpoint_auth_signing_alg_values_supported[Option<Vec<K::SigningAlgorithm>>],
            set_display_values_supported -> display_values_supported[Option<Vec<AD>>],
            set_claim_types_supported -> claim_types_supported[Option<Vec<CT>>],
            set_claims_supported -> claims_supported[Option<Vec<CN>>],
            set_service_documentation -> service_documentation[Option<ServiceDocUrl>],
            set_claims_locales_supported -> claims_locales_supported[Option<Vec<LanguageTag>>],
            set_ui_locales_supported -> ui_locales_supported[Option<Vec<LanguageTag>>],
            set_claims_parameter_supported -> claims_parameter_supported[Option<bool>],
            set_request_parameter_supported -> request_parameter_supported[Option<bool>],
            set_request_uri_parameter_supported -> request_uri_parameter_supported[Option<bool>],
            set_require_request_uri_registration -> require_request_uri_registration[Option<bool>],
            set_op_policy_uri -> op_policy_uri[Option<OpPolicyUrl>],
            set_op_tos_uri -> op_tos_uri[Option<OpTosUrl>],
        }
    ];

    /// Fetches the OpenID Connect Discovery document and associated JSON Web Key Set from the
    /// OpenID Connect Provider.
    pub fn discover<C>(
        issuer_url: &IssuerUrl,
        http_client: &C,
    ) -> Result<Self, DiscoveryError<<C as SyncHttpClient>::Error>>
    where
        C: SyncHttpClient,
    {
        let discovery_url = issuer_url
            .join(CONFIG_URL_SUFFIX)
            .map_err(DiscoveryError::UrlParse)?;

        http_client
            .call(
                Self::discovery_request(discovery_url.clone()).map_err(|err| {
                    DiscoveryError::Other(format!("failed to prepare request: {err}"))
                })?,
            )
            .map_err(DiscoveryError::Request)
            .and_then(|http_response| {
                Self::discovery_response(issuer_url, &discovery_url, http_response)
            })
            .and_then(|provider_metadata| {
                JsonWebKeySet::fetch(provider_metadata.jwks_uri(), http_client).map(|jwks| Self {
                    jwks,
                    ..provider_metadata
                })
            })
    }

    /// Asynchronously fetches the OpenID Connect Discovery document and associated JSON Web Key Set
    /// from the OpenID Connect Provider.
    pub fn discover_async<'c, C>(
        issuer_url: IssuerUrl,
        http_client: &'c C,
    ) -> impl Future<Output = Result<Self, DiscoveryError<<C as AsyncHttpClient<'c>>::Error>>> + 'c
    where
        Self: 'c,
        C: AsyncHttpClient<'c>,
    {
        Box::pin(async move {
            let discovery_url = issuer_url
                .join(CONFIG_URL_SUFFIX)
                .map_err(DiscoveryError::UrlParse)?;

            let provider_metadata = http_client
                .call(
                    Self::discovery_request(discovery_url.clone()).map_err(|err| {
                        DiscoveryError::Other(format!("failed to prepare request: {err}"))
                    })?,
                )
                .await
                .map_err(DiscoveryError::Request)
                .and_then(|http_response| {
                    Self::discovery_response(&issuer_url, &discovery_url, http_response)
                })?;

            JsonWebKeySet::fetch_async(provider_metadata.jwks_uri(), http_client)
                .await
                .map(|jwks| Self {
                    jwks,
                    ..provider_metadata
                })
        })
    }

    fn discovery_request(discovery_url: url::Url) -> Result<HttpRequest, http::Error> {
        http::Request::builder()
            .uri(discovery_url.to_string())
            .method(Method::GET)
            .header(ACCEPT, HeaderValue::from_static(MIME_TYPE_JSON))
            .body(Vec::new())
    }

    fn discovery_response<RE>(
        issuer_url: &IssuerUrl,
        discovery_url: &url::Url,
        discovery_response: HttpResponse,
    ) -> Result<Self, DiscoveryError<RE>>
    where
        RE: std::error::Error + 'static,
    {
        if discovery_response.status() != StatusCode::OK {
            return Err(DiscoveryError::Response(
                discovery_response.status(),
                discovery_response.body().to_owned(),
                format!(
                    "HTTP status code {} at {}",
                    discovery_response.status(),
                    discovery_url
                ),
            ));
        }

        check_content_type(discovery_response.headers(), MIME_TYPE_JSON).map_err(|err_msg| {
            DiscoveryError::Response(
                discovery_response.status(),
                discovery_response.body().to_owned(),
                err_msg,
            )
        })?;

        let provider_metadata = serde_path_to_error::deserialize::<_, Self>(
            &mut serde_json::Deserializer::from_slice(discovery_response.body()),
        )
        .map_err(DiscoveryError::Parse)?;

        if provider_metadata.issuer() != issuer_url {
            Err(DiscoveryError::Validation(format!(
                "unexpected issuer URI `{}` (expected `{}`)",
                provider_metadata.issuer().as_str(),
                issuer_url.as_str()
            )))
        } else {
            Ok(provider_metadata)
        }
    }

    /// Returns additional provider metadata fields.
    pub fn additional_metadata(&self) -> &A {
        &self.additional_metadata
    }
    /// Returns mutable additional provider metadata fields.
    pub fn additional_metadata_mut(&mut self) -> &mut A {
        &mut self.additional_metadata
    }
}

/// Error retrieving provider metadata.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum DiscoveryError<RE>
where
    RE: std::error::Error + 'static,
{
    /// An unexpected error occurred.
    #[error("Other error: {0}")]
    Other(String),
    /// Failed to parse server response.
    #[error("Failed to parse server response")]
    Parse(#[source] serde_path_to_error::Error<serde_json::Error>),
    /// An error occurred while sending the request or receiving the response (e.g., network
    /// connectivity failed).
    #[error("Request failed")]
    Request(#[source] RE),
    /// Server returned an invalid response.
    #[error("Server returned invalid response: {2}")]
    Response(StatusCode, Vec<u8>, String),
    /// Failed to parse discovery URL from issuer URL.
    #[error("Failed to parse URL")]
    UrlParse(#[source] url::ParseError),
    /// Failed to validate provider metadata.
    #[error("Validation error: {0}")]
    Validation(String),
}
