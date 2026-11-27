use crate::helpers::{DeserializeMapField, Timestamp};
use crate::http_utils::{auth_bearer, check_content_type, MIME_TYPE_JSON};
use crate::types::localized::split_language_tag_key;
use crate::types::{
    ApplicationType, AuthenticationContextClass, ClientAuthMethod, ClientConfigUrl,
    ClientContactEmail, ClientName, ClientUrl, GrantType, InitiateLoginUrl, LogoUrl, PolicyUrl,
    RegistrationAccessToken, RegistrationUrl, RequestUrl, ResponseType, ResponseTypes,
    SectorIdentifierUrl, SubjectIdentifierType, ToSUrl,
};
use crate::{
    AccessToken, AsyncHttpClient, ClientId, ClientSecret, ErrorResponseType, HttpRequest,
    HttpResponse, JsonWebKey, JsonWebKeySet, JsonWebKeySetUrl, JweContentEncryptionAlgorithm,
    JweKeyManagementAlgorithm, JwsSigningAlgorithm, LocalizedClaim, RedirectUrl,
    StandardErrorResponse, SyncHttpClient,
};

use chrono::{DateTime, Utc};
use http::header::{HeaderValue, ACCEPT, CONTENT_TYPE};
use http::method::Method;
use http::status::StatusCode;
use serde::de::{DeserializeOwned, Deserializer, MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};
use serde_with::{serde_as, skip_serializing_none};
use thiserror::Error;

use std::fmt::{Debug, Formatter, Result as FormatterResult};
use std::future::Future;
use std::marker::PhantomData;
use std::time::Duration;

#[cfg(test)]
mod tests;

/// Trait for adding extra fields to [`ClientMetadata`].
pub trait AdditionalClientMetadata: Debug + DeserializeOwned + Serialize {}

// In order to support serde flatten, this must be an empty struct rather than an empty
// tuple struct.
/// Empty (default) extra [`ClientMetadata`] fields.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
pub struct EmptyAdditionalClientMetadata {}
impl AdditionalClientMetadata for EmptyAdditionalClientMetadata {}

/// Client metadata used in dynamic client registration.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ClientMetadata<A, AT, CA, G, JE, JK, K, RT, S>
where
    A: AdditionalClientMetadata,
    AT: ApplicationType,
    CA: ClientAuthMethod,
    G: GrantType,
    JE: JweContentEncryptionAlgorithm<
        KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType,
    >,
    JK: JweKeyManagementAlgorithm,
    K: JsonWebKey,
    RT: ResponseType,
    S: SubjectIdentifierType,
{
    // To avoid implementing a custom deserializer that handles both language tags and flatten,
    // we wrap the language tag handling in its own flattened struct.
    #[serde(bound = "AT: ApplicationType", flatten)]
    standard_metadata: StandardClientMetadata<AT, CA, G, JE, JK, K, RT, S>,

    #[serde(bound = "A: AdditionalClientMetadata", flatten)]
    additional_metadata: A,
}
impl<A, AT, CA, G, JE, JK, K, RT, S> ClientMetadata<A, AT, CA, G, JE, JK, K, RT, S>
where
    A: AdditionalClientMetadata,
    AT: ApplicationType,
    CA: ClientAuthMethod,
    G: GrantType,
    JE: JweContentEncryptionAlgorithm<
        KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType,
    >,
    JK: JweKeyManagementAlgorithm,
    K: JsonWebKey,
    RT: ResponseType,
    S: SubjectIdentifierType,
{
    /// Instantiates new client metadata.
    pub fn new(redirect_uris: Vec<RedirectUrl>, additional_metadata: A) -> Self {
        Self {
            standard_metadata: StandardClientMetadata {
                redirect_uris,
                response_types: None,
                grant_types: None,
                application_type: None,
                contacts: None,
                client_name: None,
                logo_uri: None,
                client_uri: None,
                policy_uri: None,
                tos_uri: None,
                jwks_uri: None,
                jwks: None,
                sector_identifier_uri: None,
                subject_type: None,
                id_token_signed_response_alg: None,
                id_token_encrypted_response_alg: None,
                id_token_encrypted_response_enc: None,
                userinfo_signed_response_alg: None,
                userinfo_encrypted_response_alg: None,
                userinfo_encrypted_response_enc: None,
                request_object_signing_alg: None,
                request_object_encryption_alg: None,
                request_object_encryption_enc: None,
                token_endpoint_auth_method: None,
                token_endpoint_auth_signing_alg: None,
                default_max_age: None,
                require_auth_time: None,
                default_acr_values: None,
                initiate_login_uri: None,
                request_uris: None,
            },
            additional_metadata,
        }
    }
    field_getters_setters![
        pub self [self.standard_metadata] ["client metadata value"] {
            set_redirect_uris -> redirect_uris[Vec<RedirectUrl>],
            set_response_types -> response_types[Option<Vec<ResponseTypes<RT>>>],
            set_grant_types -> grant_types[Option<Vec<G>>],
            set_application_type -> application_type[Option<AT>],
            set_contacts -> contacts[Option<Vec<ClientContactEmail>>],
            set_client_name -> client_name[Option<LocalizedClaim<ClientName>>],
            set_logo_uri -> logo_uri[Option<LocalizedClaim<LogoUrl>>],
            set_client_uri -> client_uri[Option<LocalizedClaim<ClientUrl>>],
            set_policy_uri -> policy_uri[Option<LocalizedClaim<PolicyUrl>>],
            set_tos_uri -> tos_uri[Option<LocalizedClaim<ToSUrl>>],
            set_jwks_uri -> jwks_uri[Option<JsonWebKeySetUrl>],
            set_jwks -> jwks[Option<JsonWebKeySet<K>>],
            set_sector_identifier_uri -> sector_identifier_uri[Option<SectorIdentifierUrl>],
            set_subject_type -> subject_type[Option<S>],
            set_id_token_signed_response_alg -> id_token_signed_response_alg[Option<K::SigningAlgorithm>],
            set_id_token_encrypted_response_alg -> id_token_encrypted_response_alg[Option<JK>],
            set_id_token_encrypted_response_enc -> id_token_encrypted_response_enc[Option<JE>],
            set_userinfo_signed_response_alg -> userinfo_signed_response_alg[Option<K::SigningAlgorithm>],
            set_userinfo_encrypted_response_alg -> userinfo_encrypted_response_alg[Option<JK>],
            set_userinfo_encrypted_response_enc -> userinfo_encrypted_response_enc[Option<JE>],
            set_request_object_signing_alg -> request_object_signing_alg[Option<K::SigningAlgorithm>],
            set_request_object_encryption_alg -> request_object_encryption_alg[Option<JK>],
            set_request_object_encryption_enc -> request_object_encryption_enc[Option<JE>],
            set_token_endpoint_auth_method -> token_endpoint_auth_method[Option<CA>],
            set_token_endpoint_auth_signing_alg -> token_endpoint_auth_signing_alg[Option<K::SigningAlgorithm>],
            set_default_max_age -> default_max_age[Option<Duration>],
            set_require_auth_time -> require_auth_time[Option<bool>],
            set_default_acr_values -> default_acr_values[Option<Vec<AuthenticationContextClass>>],
            set_initiate_login_uri -> initiate_login_uri[Option<InitiateLoginUrl>],
            set_request_uris -> request_uris[Option<Vec<RequestUrl>>],
        }
    ];

    /// Returns additional client metadata fields.
    pub fn additional_metadata(&self) -> &A {
        &self.additional_metadata
    }
    /// Returns mutable additional client metadata fields.
    pub fn additional_metadata_mut(&mut self) -> &mut A {
        &mut self.additional_metadata
    }
}

#[derive(Clone, Debug, PartialEq)]
struct StandardClientMetadata<AT, CA, G, JE, JK, K, RT, S>
where
    AT: ApplicationType,
    CA: ClientAuthMethod,
    G: GrantType,
    JE: JweContentEncryptionAlgorithm<
        KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType,
    >,
    JK: JweKeyManagementAlgorithm,
    K: JsonWebKey,
    RT: ResponseType,
    S: SubjectIdentifierType,
{
    redirect_uris: Vec<RedirectUrl>,
    response_types: Option<Vec<ResponseTypes<RT>>>,
    grant_types: Option<Vec<G>>,
    application_type: Option<AT>,
    contacts: Option<Vec<ClientContactEmail>>,
    client_name: Option<LocalizedClaim<ClientName>>,
    logo_uri: Option<LocalizedClaim<LogoUrl>>,
    client_uri: Option<LocalizedClaim<ClientUrl>>,
    policy_uri: Option<LocalizedClaim<PolicyUrl>>,
    tos_uri: Option<LocalizedClaim<ToSUrl>>,
    jwks_uri: Option<JsonWebKeySetUrl>,
    jwks: Option<JsonWebKeySet<K>>,
    sector_identifier_uri: Option<SectorIdentifierUrl>,
    subject_type: Option<S>,
    id_token_signed_response_alg: Option<K::SigningAlgorithm>,
    id_token_encrypted_response_alg: Option<JK>,
    id_token_encrypted_response_enc: Option<JE>,
    userinfo_signed_response_alg: Option<K::SigningAlgorithm>,
    userinfo_encrypted_response_alg: Option<JK>,
    userinfo_encrypted_response_enc: Option<JE>,
    request_object_signing_alg: Option<K::SigningAlgorithm>,
    request_object_encryption_alg: Option<JK>,
    request_object_encryption_enc: Option<JE>,
    token_endpoint_auth_method: Option<CA>,
    token_endpoint_auth_signing_alg: Option<K::SigningAlgorithm>,
    default_max_age: Option<Duration>,
    require_auth_time: Option<bool>,
    default_acr_values: Option<Vec<AuthenticationContextClass>>,
    initiate_login_uri: Option<InitiateLoginUrl>,
    request_uris: Option<Vec<RequestUrl>>,
}
impl<'de, AT, CA, G, JE, JK, K, RT, S> Deserialize<'de>
    for StandardClientMetadata<AT, CA, G, JE, JK, K, RT, S>
where
    AT: ApplicationType,
    CA: ClientAuthMethod,
    G: GrantType,
    JE: JweContentEncryptionAlgorithm<
        KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType,
    >,
    JK: JweKeyManagementAlgorithm,
    K: JsonWebKey,
    RT: ResponseType,
    S: SubjectIdentifierType,
{
    /// Special deserializer that supports [RFC 5646](https://tools.ietf.org/html/rfc5646) language
    /// tags associated with human-readable client metadata fields.
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct MetadataVisitor<
            AT: ApplicationType,
            CA: ClientAuthMethod,
            G: GrantType,
            JE: JweContentEncryptionAlgorithm<
                KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType,
            >,
            JK: JweKeyManagementAlgorithm,
            K: JsonWebKey,
            RT: ResponseType,
            S: SubjectIdentifierType,
        >(PhantomData<(AT, CA, G, JE, JK, K, RT, S)>);
        impl<'de, AT, CA, G, JE, JK, K, RT, S> Visitor<'de> for MetadataVisitor<AT, CA, G, JE, JK, K, RT, S>
        where
            AT: ApplicationType,
            CA: ClientAuthMethod,
            G: GrantType,
            JE: JweContentEncryptionAlgorithm<
                KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType,
            >,
            JK: JweKeyManagementAlgorithm,
            K: JsonWebKey,
            RT: ResponseType,
            S: SubjectIdentifierType,
        {
            type Value = StandardClientMetadata<AT, CA, G, JE, JK, K, RT, S>;

            fn expecting(&self, formatter: &mut Formatter) -> FormatterResult {
                formatter.write_str("struct StandardClientMetadata")
            }
            fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
            where
                V: MapAccess<'de>,
            {
                // NB: The non-localized fields are actually Option<Option<_>> here so that we can
                // distinguish between omitted fields and fields explicitly set to `null`. The
                // latter is necessary so that we can detect duplicate fields (e.g., if a key is
                // present both with a null value and a non-null value, that's an error).
                let mut redirect_uris = None;
                let mut response_types = None;
                let mut grant_types = None;
                let mut application_type = None;
                let mut contacts = None;
                let mut client_name = None;
                let mut logo_uri = None;
                let mut client_uri = None;
                let mut policy_uri = None;
                let mut tos_uri = None;
                let mut jwks_uri = None;
                let mut jwks = None;
                let mut sector_identifier_uri = None;
                let mut subject_type = None;
                let mut id_token_signed_response_alg = None;
                let mut id_token_encrypted_response_alg = None;
                let mut id_token_encrypted_response_enc = None;
                let mut userinfo_signed_response_alg = None;
                let mut userinfo_encrypted_response_alg = None;
                let mut userinfo_encrypted_response_enc = None;
                let mut request_object_signing_alg = None;
                let mut request_object_encryption_alg = None;
                let mut request_object_encryption_enc = None;
                let mut token_endpoint_auth_method = None;
                let mut token_endpoint_auth_signing_alg = None;
                let mut default_max_age = None;
                let mut require_auth_time = None;
                let mut default_acr_values = None;
                let mut initiate_login_uri = None;
                let mut request_uris = None;

                macro_rules! field_case {
                    ($field:ident, $typ:ty, $language_tag:ident) => {{
                        $field = Some(<$typ>::deserialize_map_field(
                            &mut map,
                            stringify!($field),
                            $language_tag,
                            $field,
                        )?);
                    }};
                }

                while let Some(key) = map.next_key::<String>()? {
                    let (field_name, language_tag) = split_language_tag_key(&key);
                    match field_name {
                        "redirect_uris" => field_case!(redirect_uris, Vec<_>, language_tag),
                        "response_types" => field_case!(response_types, Option<_>, language_tag),
                        "grant_types" => field_case!(grant_types, Option<_>, language_tag),
                        "application_type" => {
                            field_case!(application_type, Option<_>, language_tag)
                        }
                        "contacts" => field_case!(contacts, Option<_>, language_tag),
                        "client_name" => {
                            field_case!(client_name, LocalizedClaim<Option<_>>, language_tag)
                        }
                        "logo_uri" => {
                            field_case!(logo_uri, LocalizedClaim<Option<_>>, language_tag)
                        }
                        "client_uri" => {
                            field_case!(client_uri, LocalizedClaim<Option<_>>, language_tag)
                        }
                        "policy_uri" => {
                            field_case!(policy_uri, LocalizedClaim<Option<_>>, language_tag)
                        }
                        "tos_uri" => field_case!(tos_uri, LocalizedClaim<Option<_>>, language_tag),
                        "jwks_uri" => field_case!(jwks_uri, Option<_>, language_tag),
                        "jwks" => field_case!(jwks, Option<_>, language_tag),
                        "sector_identifier_uri" => {
                            field_case!(sector_identifier_uri, Option<_>, language_tag)
                        }
                        "subject_type" => field_case!(subject_type, Option<_>, language_tag),
                        "id_token_signed_response_alg" => {
                            field_case!(id_token_signed_response_alg, Option<_>, language_tag)
                        }
                        "id_token_encrypted_response_alg" => {
                            field_case!(id_token_encrypted_response_alg, Option<_>, language_tag)
                        }
                        "id_token_encrypted_response_enc" => {
                            field_case!(id_token_encrypted_response_enc, Option<_>, language_tag)
                        }
                        "userinfo_signed_response_alg" => {
                            field_case!(userinfo_signed_response_alg, Option<_>, language_tag)
                        }
                        "userinfo_encrypted_response_alg" => {
                            field_case!(userinfo_encrypted_response_alg, Option<_>, language_tag)
                        }
                        "userinfo_encrypted_response_enc" => {
                            field_case!(userinfo_encrypted_response_enc, Option<_>, language_tag)
                        }
                        "request_object_signing_alg" => {
                            field_case!(request_object_signing_alg, Option<_>, language_tag)
                        }
                        "request_object_encryption_alg" => {
                            field_case!(request_object_encryption_alg, Option<_>, language_tag)
                        }
                        "request_object_encryption_enc" => {
                            field_case!(request_object_encryption_enc, Option<_>, language_tag)
                        }
                        "token_endpoint_auth_method" => {
                            field_case!(token_endpoint_auth_method, Option<_>, language_tag)
                        }
                        "token_endpoint_auth_signing_alg" => {
                            field_case!(token_endpoint_auth_signing_alg, Option<_>, language_tag)
                        }
                        "default_max_age" => {
                            field_case!(default_max_age, Option<u64>, language_tag)
                        }
                        "require_auth_time" => {
                            field_case!(require_auth_time, Option<_>, language_tag)
                        }
                        "default_acr_values" => {
                            field_case!(default_acr_values, Option<_>, language_tag)
                        }
                        "initiate_login_uri" => {
                            field_case!(initiate_login_uri, Option<_>, language_tag)
                        }
                        "request_uris" => field_case!(request_uris, Option<_>, language_tag),

                        // Ignore unknown fields.
                        _ => {
                            map.next_value::<serde::de::IgnoredAny>()?;
                            continue;
                        }
                    };
                }

                Ok(StandardClientMetadata {
                    redirect_uris: redirect_uris
                        .ok_or_else(|| serde::de::Error::missing_field("redirect_uris"))?,
                    response_types: response_types.flatten(),
                    grant_types: grant_types.flatten(),
                    application_type: application_type.flatten(),
                    contacts: contacts.flatten(),
                    client_name: client_name.and_then(LocalizedClaim::flatten_or_none),
                    logo_uri: logo_uri.and_then(LocalizedClaim::flatten_or_none),
                    client_uri: client_uri.and_then(LocalizedClaim::flatten_or_none),
                    policy_uri: policy_uri.and_then(LocalizedClaim::flatten_or_none),
                    tos_uri: tos_uri.and_then(LocalizedClaim::flatten_or_none),
                    jwks_uri: jwks_uri.flatten(),
                    jwks: jwks.flatten(),
                    sector_identifier_uri: sector_identifier_uri.flatten(),
                    subject_type: subject_type.flatten(),
                    id_token_signed_response_alg: id_token_signed_response_alg.flatten(),
                    id_token_encrypted_response_alg: id_token_encrypted_response_alg.flatten(),
                    id_token_encrypted_response_enc: id_token_encrypted_response_enc.flatten(),
                    userinfo_signed_response_alg: userinfo_signed_response_alg.flatten(),
                    userinfo_encrypted_response_alg: userinfo_encrypted_response_alg.flatten(),
                    userinfo_encrypted_response_enc: userinfo_encrypted_response_enc.flatten(),
                    request_object_signing_alg: request_object_signing_alg.flatten(),
                    request_object_encryption_alg: request_object_encryption_alg.flatten(),
                    request_object_encryption_enc: request_object_encryption_enc.flatten(),
                    token_endpoint_auth_method: token_endpoint_auth_method.flatten(),
                    token_endpoint_auth_signing_alg: token_endpoint_auth_signing_alg.flatten(),
                    default_max_age: default_max_age.flatten().map(Duration::from_secs),
                    require_auth_time: require_auth_time.flatten(),
                    default_acr_values: default_acr_values.flatten(),
                    initiate_login_uri: initiate_login_uri.flatten(),
                    request_uris: request_uris.flatten(),
                })
            }
        }
        deserializer.deserialize_map(MetadataVisitor(PhantomData))
    }
}
impl<AT, CA, G, JE, JK, K, RT, S> Serialize for StandardClientMetadata<AT, CA, G, JE, JK, K, RT, S>
where
    AT: ApplicationType,
    CA: ClientAuthMethod,
    G: GrantType,
    JE: JweContentEncryptionAlgorithm<
        KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType,
    >,
    JK: JweKeyManagementAlgorithm,
    K: JsonWebKey,
    RT: ResponseType,
    S: SubjectIdentifierType,
{
    #[allow(clippy::cognitive_complexity)]
    fn serialize<SE>(&self, serializer: SE) -> Result<SE::Ok, SE::Error>
    where
        SE: Serializer,
    {
        serialize_fields! {
            self -> serializer {
                [redirect_uris]
                [Option(response_types)]
                [Option(grant_types)]
                [Option(application_type)]
                [Option(contacts)]
                [LanguageTag(client_name)]
                [LanguageTag(logo_uri)]
                [LanguageTag(client_uri)]
                [LanguageTag(policy_uri)]
                [LanguageTag(tos_uri)]
                [Option(jwks_uri)]
                [Option(jwks)]
                [Option(sector_identifier_uri)]
                [Option(subject_type)]
                [Option(id_token_signed_response_alg)]
                [Option(id_token_encrypted_response_alg)]
                [Option(id_token_encrypted_response_enc)]
                [Option(userinfo_signed_response_alg)]
                [Option(userinfo_encrypted_response_alg)]
                [Option(userinfo_encrypted_response_enc)]
                [Option(request_object_signing_alg)]
                [Option(request_object_encryption_alg)]
                [Option(request_object_encryption_enc)]
                [Option(token_endpoint_auth_method)]
                [Option(token_endpoint_auth_signing_alg)]
                [Option(Seconds(default_max_age))]
                [Option(require_auth_time)]
                [Option(default_acr_values)]
                [Option(initiate_login_uri)]
                [Option(request_uris)]
            }
        }
    }
}

/// Dynamic client registration request.
#[derive(Clone, Debug)]
pub struct ClientRegistrationRequest<AC, AR, AT, CA, ET, G, JE, JK, K, RT, S>
where
    AC: AdditionalClientMetadata,
    AR: AdditionalClientRegistrationResponse,
    AT: ApplicationType,
    CA: ClientAuthMethod,
    ET: RegisterErrorResponseType,
    G: GrantType,
    JE: JweContentEncryptionAlgorithm<
        KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType,
    >,
    JK: JweKeyManagementAlgorithm,
    K: JsonWebKey,
    RT: ResponseType,
    S: SubjectIdentifierType,
{
    client_metadata: ClientMetadata<AC, AT, CA, G, JE, JK, K, RT, S>,
    initial_access_token: Option<AccessToken>,
    _phantom: PhantomData<(AR, ET)>,
}
impl<AC, AR, AT, CA, ET, G, JE, JK, K, RT, S>
    ClientRegistrationRequest<AC, AR, AT, CA, ET, G, JE, JK, K, RT, S>
where
    AC: AdditionalClientMetadata,
    AR: AdditionalClientRegistrationResponse,
    AT: ApplicationType,
    CA: ClientAuthMethod,
    ET: RegisterErrorResponseType + Send + Sync,
    G: GrantType,
    JE: JweContentEncryptionAlgorithm<
        KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType,
    >,
    JK: JweKeyManagementAlgorithm,
    K: JsonWebKey,
    RT: ResponseType,
    S: SubjectIdentifierType + Send + Sync,
{
    /// Instantiates a new dynamic client registration request.
    pub fn new(redirect_uris: Vec<RedirectUrl>, additional_metadata: AC) -> Self {
        Self {
            client_metadata: ClientMetadata::new(redirect_uris, additional_metadata),
            initial_access_token: None,
            _phantom: PhantomData,
        }
    }

    /// Submits this request to the specified registration endpoint using the specified synchronous
    /// HTTP client.
    pub fn register<C>(
        &self,
        registration_endpoint: &RegistrationUrl,
        http_client: &C,
    ) -> Result<
        ClientRegistrationResponse<AC, AR, AT, CA, G, JE, JK, K, RT, S>,
        ClientRegistrationError<ET, <C as SyncHttpClient>::Error>,
    >
    where
        C: SyncHttpClient,
    {
        self.prepare_registration(registration_endpoint)
            .and_then(|http_request| {
                http_client
                    .call(http_request)
                    .map_err(ClientRegistrationError::Request)
            })
            .and_then(Self::register_response)
    }

    /// Submits this request to the specified registration endpoint using the specified asynchronous
    /// HTTP client.
    pub fn register_async<'c, C>(
        &'c self,
        registration_endpoint: &'c RegistrationUrl,
        http_client: &'c C,
    ) -> impl Future<
        Output = Result<
            ClientRegistrationResponse<AC, AR, AT, CA, G, JE, JK, K, RT, S>,
            ClientRegistrationError<ET, <C as AsyncHttpClient<'c>>::Error>,
        >,
    > + 'c
    where
        Self: 'c,
        C: AsyncHttpClient<'c>,
    {
        Box::pin(async move {
            let http_request = self.prepare_registration(registration_endpoint)?;
            let http_response = http_client
                .call(http_request)
                .await
                .map_err(ClientRegistrationError::Request)?;
            Self::register_response(http_response)
        })
    }

    fn prepare_registration<RE>(
        &self,
        registration_endpoint: &RegistrationUrl,
    ) -> Result<HttpRequest, ClientRegistrationError<ET, RE>>
    where
        RE: std::error::Error + 'static,
    {
        let request_json = serde_json::to_string(self.client_metadata())
            .map_err(ClientRegistrationError::Serialize)?
            .into_bytes();

        let auth_header_opt = self.initial_access_token().map(auth_bearer);

        let mut request = http::Request::builder()
            .uri(registration_endpoint.to_string())
            .method(Method::POST)
            .header(ACCEPT, HeaderValue::from_static(MIME_TYPE_JSON))
            .header(CONTENT_TYPE, HeaderValue::from_static(MIME_TYPE_JSON));
        if let Some((header, value)) = auth_header_opt {
            request = request.header(header, value);
        }

        request.body(request_json).map_err(|err| {
            ClientRegistrationError::Other(format!("failed to prepare request: {err}"))
        })
    }

    fn register_response<RE>(
        http_response: HttpResponse,
    ) -> Result<
        ClientRegistrationResponse<AC, AR, AT, CA, G, JE, JK, K, RT, S>,
        ClientRegistrationError<ET, RE>,
    >
    where
        RE: std::error::Error + 'static,
    {
        // TODO: check for WWW-Authenticate response header if bearer auth was used (see
        //   https://tools.ietf.org/html/rfc6750#section-3)
        // TODO: other necessary response validation? check spec

        // Spec says that a successful response SHOULD use 201 Created, and a registration error
        // condition returns (no "SHOULD") 400 Bad Request. For now, only accept these two status
        // codes. We may need to relax the success status to improve interoperability.
        if http_response.status() != StatusCode::CREATED
            && http_response.status() != StatusCode::BAD_REQUEST
        {
            return Err(ClientRegistrationError::Response(
                http_response.status(),
                http_response.body().to_owned(),
                "unexpected HTTP status code".to_string(),
            ));
        }

        check_content_type(http_response.headers(), MIME_TYPE_JSON).map_err(|err_msg| {
            ClientRegistrationError::Response(
                http_response.status(),
                http_response.body().to_owned(),
                err_msg,
            )
        })?;

        let response_body =
            String::from_utf8(http_response.body().to_owned()).map_err(|parse_error| {
                ClientRegistrationError::Other(format!(
                    "couldn't parse response as UTF-8: {parse_error}"
                ))
            })?;

        if http_response.status() == StatusCode::BAD_REQUEST {
            let response_error: StandardErrorResponse<ET> = serde_path_to_error::deserialize(
                &mut serde_json::Deserializer::from_str(&response_body),
            )
            .map_err(ClientRegistrationError::Parse)?;
            return Err(ClientRegistrationError::ServerResponse(response_error));
        }

        serde_path_to_error::deserialize(&mut serde_json::Deserializer::from_str(&response_body))
            .map_err(ClientRegistrationError::Parse)
    }

    /// Returns the client metadata associated with this registration request.
    pub fn client_metadata(&self) -> &ClientMetadata<AC, AT, CA, G, JE, JK, K, RT, S> {
        &self.client_metadata
    }

    /// Returns the initial access token associated with this registration request.
    pub fn initial_access_token(&self) -> Option<&AccessToken> {
        self.initial_access_token.as_ref()
    }
    /// Sets the initial access token for this request.
    pub fn set_initial_access_token(mut self, access_token: Option<AccessToken>) -> Self {
        self.initial_access_token = access_token;
        self
    }

    field_getters_setters![
        pub self [self.client_metadata.standard_metadata] ["client metadata value"] {
            set_redirect_uris -> redirect_uris[Vec<RedirectUrl>],
            set_response_types -> response_types[Option<Vec<ResponseTypes<RT>>>],
            set_grant_types -> grant_types[Option<Vec<G>>],
            set_application_type -> application_type[Option<AT>],
            set_contacts -> contacts[Option<Vec<ClientContactEmail>>],
            set_client_name -> client_name[Option<LocalizedClaim<ClientName>>],
            set_logo_uri -> logo_uri[Option<LocalizedClaim<LogoUrl>>],
            set_client_uri -> client_uri[Option<LocalizedClaim<ClientUrl>>],
            set_policy_uri -> policy_uri[Option<LocalizedClaim<PolicyUrl>>],
            set_tos_uri -> tos_uri[Option<LocalizedClaim<ToSUrl>>],
            set_jwks_uri -> jwks_uri[Option<JsonWebKeySetUrl>],
            set_jwks -> jwks[Option<JsonWebKeySet<K>>],
            set_sector_identifier_uri -> sector_identifier_uri[Option<SectorIdentifierUrl>],
            set_subject_type -> subject_type[Option<S>],
            set_id_token_signed_response_alg -> id_token_signed_response_alg[Option<K::SigningAlgorithm>],
            set_id_token_encrypted_response_alg -> id_token_encrypted_response_alg[Option<JK>],
            set_id_token_encrypted_response_enc -> id_token_encrypted_response_enc[Option<JE>],
            set_userinfo_signed_response_alg -> userinfo_signed_response_alg[Option<K::SigningAlgorithm>],
            set_userinfo_encrypted_response_alg -> userinfo_encrypted_response_alg[Option<JK>],
            set_userinfo_encrypted_response_enc -> userinfo_encrypted_response_enc[Option<JE>],
            set_request_object_signing_alg -> request_object_signing_alg[Option<K::SigningAlgorithm>],
            set_request_object_encryption_alg -> request_object_encryption_alg[Option<JK>],
            set_request_object_encryption_enc -> request_object_encryption_enc[Option<JE>],
            set_token_endpoint_auth_method -> token_endpoint_auth_method[Option<CA>],
            set_token_endpoint_auth_signing_alg -> token_endpoint_auth_signing_alg[Option<K::SigningAlgorithm>],
            set_default_max_age -> default_max_age[Option<Duration>],
            set_require_auth_time -> require_auth_time[Option<bool>],
            set_default_acr_values -> default_acr_values[Option<Vec<AuthenticationContextClass>>],
            set_initiate_login_uri -> initiate_login_uri[Option<InitiateLoginUrl>],
            set_request_uris -> request_uris[Option<Vec<RequestUrl>>],
        }
    ];

    /// Returns additional client metadata fields.
    pub fn additional_metadata(&self) -> &AC {
        &self.client_metadata.additional_metadata
    }
    /// Returns mutable additional client metadata fields.
    pub fn additional_metadata_mut(&mut self) -> &mut AC {
        &mut self.client_metadata.additional_metadata
    }
}

/// Trait for adding extra fields to the [`ClientRegistrationResponse`].
pub trait AdditionalClientRegistrationResponse: Debug + DeserializeOwned + Serialize {}

// In order to support serde flatten, this must be an empty struct rather than an empty
// tuple struct.
/// Empty (default) extra [`ClientRegistrationResponse`] fields.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
pub struct EmptyAdditionalClientRegistrationResponse {}
impl AdditionalClientRegistrationResponse for EmptyAdditionalClientRegistrationResponse {}

/// Response to a dynamic client registration request.
#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Deserialize, Serialize)]
pub struct ClientRegistrationResponse<AC, AR, AT, CA, G, JE, JK, K, RT, S>
where
    AC: AdditionalClientMetadata,
    AR: AdditionalClientRegistrationResponse,
    AT: ApplicationType,
    CA: ClientAuthMethod,
    G: GrantType,
    JE: JweContentEncryptionAlgorithm<
        KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType,
    >,
    JK: JweKeyManagementAlgorithm,
    K: JsonWebKey,
    RT: ResponseType,
    S: SubjectIdentifierType,
{
    client_id: ClientId,
    client_secret: Option<ClientSecret>,
    registration_access_token: Option<RegistrationAccessToken>,
    registration_client_uri: Option<ClientConfigUrl>,
    #[serde_as(as = "Option<Timestamp>")]
    client_id_issued_at: Option<DateTime<Utc>>,
    #[serde_as(as = "Option<Timestamp>")]
    client_secret_expires_at: Option<DateTime<Utc>>,
    #[serde(bound = "AC: AdditionalClientMetadata", flatten)]
    client_metadata: ClientMetadata<AC, AT, CA, G, JE, JK, K, RT, S>,

    #[serde(bound = "AR: AdditionalClientRegistrationResponse", flatten)]
    additional_response: AR,
}
impl<AC, AR, AT, CA, G, JE, JK, K, RT, S>
    ClientRegistrationResponse<AC, AR, AT, CA, G, JE, JK, K, RT, S>
where
    AC: AdditionalClientMetadata,
    AR: AdditionalClientRegistrationResponse,
    AT: ApplicationType,
    CA: ClientAuthMethod,
    G: GrantType,
    JE: JweContentEncryptionAlgorithm<
        KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType,
    >,
    JK: JweKeyManagementAlgorithm,
    K: JsonWebKey,
    RT: ResponseType,
    S: SubjectIdentifierType,
{
    /// Instantiates a new dynamic client registration response.
    pub fn new(
        client_id: ClientId,
        redirect_uris: Vec<RedirectUrl>,
        additional_metadata: AC,
        additional_response: AR,
    ) -> Self {
        Self {
            client_id,
            client_secret: None,
            registration_access_token: None,
            registration_client_uri: None,
            client_id_issued_at: None,
            client_secret_expires_at: None,
            client_metadata: ClientMetadata::new(redirect_uris, additional_metadata),
            additional_response,
        }
    }

    /// Instantiates a new dynamic client registration response using the specified client metadata.
    pub fn from_client_metadata(
        client_id: ClientId,
        client_metadata: ClientMetadata<AC, AT, CA, G, JE, JK, K, RT, S>,
        additional_response: AR,
    ) -> Self {
        Self {
            client_id,
            client_secret: None,
            registration_access_token: None,
            registration_client_uri: None,
            client_id_issued_at: None,
            client_secret_expires_at: None,
            client_metadata,
            additional_response,
        }
    }

    field_getters_setters![
        pub self [self] ["response field"] {
            set_client_id -> client_id[ClientId],
            set_client_secret -> client_secret[Option<ClientSecret>],
            set_registration_access_token
              -> registration_access_token[Option<RegistrationAccessToken>],
            set_registration_client_uri -> registration_client_uri[Option<ClientConfigUrl>],
            set_client_id_issued_at -> client_id_issued_at[Option<DateTime<Utc>>],
            set_client_secret_expires_at -> client_secret_expires_at[Option<DateTime<Utc>>],
        }
    ];

    field_getters_setters![
        pub self [self.client_metadata.standard_metadata] ["client metadata value"] {
            set_redirect_uris -> redirect_uris[Vec<RedirectUrl>],
            set_response_types -> response_types[Option<Vec<ResponseTypes<RT>>>],
            set_grant_types -> grant_types[Option<Vec<G>>],
            set_application_type -> application_type[Option<AT>],
            set_contacts -> contacts[Option<Vec<ClientContactEmail>>],
            set_client_name -> client_name[Option<LocalizedClaim<ClientName>>],
            set_logo_uri -> logo_uri[Option<LocalizedClaim<LogoUrl>>],
            set_client_uri -> client_uri[Option<LocalizedClaim<ClientUrl>>],
            set_policy_uri -> policy_uri[Option<LocalizedClaim<PolicyUrl>>],
            set_tos_uri -> tos_uri[Option<LocalizedClaim<ToSUrl>>],
            set_jwks_uri -> jwks_uri[Option<JsonWebKeySetUrl>],
            set_jwks -> jwks[Option<JsonWebKeySet<K>>],
            set_sector_identifier_uri -> sector_identifier_uri[Option<SectorIdentifierUrl>],
            set_subject_type -> subject_type[Option<S>],
            set_id_token_signed_response_alg -> id_token_signed_response_alg[Option<K::SigningAlgorithm>],
            set_id_token_encrypted_response_alg -> id_token_encrypted_response_alg[Option<JK>],
            set_id_token_encrypted_response_enc -> id_token_encrypted_response_enc[Option<JE>],
            set_userinfo_signed_response_alg -> userinfo_signed_response_alg[Option<K::SigningAlgorithm>],
            set_userinfo_encrypted_response_alg -> userinfo_encrypted_response_alg[Option<JK>],
            set_userinfo_encrypted_response_enc -> userinfo_encrypted_response_enc[Option<JE>],
            set_request_object_signing_alg -> request_object_signing_alg[Option<K::SigningAlgorithm>],
            set_request_object_encryption_alg -> request_object_encryption_alg[Option<JK>],
            set_request_object_encryption_enc -> request_object_encryption_enc[Option<JE>],
            set_token_endpoint_auth_method -> token_endpoint_auth_method[Option<CA>],
            set_token_endpoint_auth_signing_alg -> token_endpoint_auth_signing_alg[Option<K::SigningAlgorithm>],
            set_default_max_age -> default_max_age[Option<Duration>],
            set_require_auth_time -> require_auth_time[Option<bool>],
            set_default_acr_values -> default_acr_values[Option<Vec<AuthenticationContextClass>>],
            set_initiate_login_uri -> initiate_login_uri[Option<InitiateLoginUrl>],
            set_request_uris -> request_uris[Option<Vec<RequestUrl>>],
        }
    ];

    /// Returns additional client metadata fields.
    pub fn additional_metadata(&self) -> &AC {
        &self.client_metadata.additional_metadata
    }
    /// Returns mutable additional client metadata fields.
    pub fn additional_metadata_mut(&mut self) -> &mut AC {
        &mut self.client_metadata.additional_metadata
    }

    /// Returns additional response fields.
    pub fn additional_response(&self) -> &AR {
        &self.additional_response
    }
    /// Returns mutable additional response fields.
    pub fn additional_response_mut(&mut self) -> &mut AR {
        &mut self.additional_response
    }
}

// TODO: implement client configuration endpoint request (Section 4)

/// Trait representing an error returned by the dynamic client registration endpoint.
pub trait RegisterErrorResponseType: ErrorResponseType + 'static {}

/// Error registering a client.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ClientRegistrationError<T, RE>
where
    RE: std::error::Error + 'static,
    T: RegisterErrorResponseType,
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
    #[error("Server returned invalid response with status {0}: {2}")]
    Response(StatusCode, Vec<u8>, String),
    /// Failed to serialize client metadata.
    #[error("Failed to serialize client metadata")]
    Serialize(#[source] serde_json::Error),
    /// Server returned an error.
    #[error("Server returned error: {0}")]
    ServerResponse(StandardErrorResponse<T>),
}
