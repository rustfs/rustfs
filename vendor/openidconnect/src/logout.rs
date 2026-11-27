use crate::core::{
    CoreAuthDisplay, CoreClaimName, CoreClaimType, CoreClientAuthMethod, CoreGrantType,
    CoreJsonWebKey, CoreJweContentEncryptionAlgorithm, CoreJweKeyManagementAlgorithm,
    CoreResponseMode, CoreResponseType, CoreSubjectIdentifierType,
};
use crate::helpers::join_vec;
use crate::types::{LogoutHint, PostLogoutRedirectUrl};
use crate::{
    AdditionalClaims, AdditionalProviderMetadata, ClientId, CsrfToken,
    EmptyAdditionalProviderMetadata, EndSessionUrl, GenderClaim, IdToken,
    JweContentEncryptionAlgorithm, JwsSigningAlgorithm, LanguageTag, ProviderMetadata,
};

use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use url::Url;

/// Additional metadata for providers implementing [OpenID Connect RP-Initiated
/// Logout 1.0](https://openid.net/specs/openid-connect-rpinitiated-1_0.html).
#[skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct LogoutProviderMetadata<A>
where
    A: AdditionalProviderMetadata,
{
    /// The end session endpoint as described in [OpenID Connect RP-Initiated
    /// Logout 1.0](https://openid.net/specs/openid-connect-rpinitiated-1_0.html).
    pub end_session_endpoint: Option<EndSessionUrl>,
    #[serde(bound = "A: AdditionalProviderMetadata", flatten)]
    /// A field for an additional struct implementing AdditionalProviderMetadata.
    pub additional_metadata: A,
}
impl<A> AdditionalProviderMetadata for LogoutProviderMetadata<A> where A: AdditionalProviderMetadata {}

/// Provider metadata returned by [OpenID Connect Discovery](
/// https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderMetadata)
/// that returns [`ProviderMetadata::additional_metadata`] for providers
/// implementing [OpenID Connect RP-Initiated Logout 1.0](
/// https://openid.net/specs/openid-connect-rpinitiated-1_0.html).
pub type ProviderMetadataWithLogout = ProviderMetadata<
    LogoutProviderMetadata<EmptyAdditionalProviderMetadata>,
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

/// A request to the end session endpoint.
pub struct LogoutRequest {
    end_session_endpoint: EndSessionUrl,
    parameters: LogoutRequestParameters,
}

#[derive(Default)]
struct LogoutRequestParameters {
    id_token_hint: Option<String>,
    logout_hint: Option<LogoutHint>,
    client_id: Option<ClientId>,
    post_logout_redirect_uri: Option<PostLogoutRedirectUrl>,
    state: Option<CsrfToken>,
    ui_locales: Vec<LanguageTag>,
}

impl From<EndSessionUrl> for LogoutRequest {
    fn from(value: EndSessionUrl) -> Self {
        LogoutRequest {
            end_session_endpoint: value,
            parameters: Default::default(),
        }
    }
}

impl LogoutRequest {
    /// Provides an ID token previously issued by this OpenID Connect Provider as a hint about
    /// the user's identity.
    pub fn set_id_token_hint<AC, GC, JE, JS>(
        mut self,
        id_token_hint: &IdToken<AC, GC, JE, JS>,
    ) -> Self
    where
        AC: AdditionalClaims,
        GC: GenderClaim,
        JE: JweContentEncryptionAlgorithm<KeyType = JS::KeyType>,
        JS: JwsSigningAlgorithm,
    {
        self.parameters.id_token_hint = Some(id_token_hint.to_string());
        self
    }

    /// Provides the OpenID Connect Provider with a hint about the user's identity.
    ///
    /// The nature of this hint is specific to each provider.
    pub fn set_logout_hint(mut self, logout_hint: LogoutHint) -> Self {
        self.parameters.logout_hint = Some(logout_hint);
        self
    }

    /// Provides the OpenID Connect Provider with the client identifier.
    ///
    /// When both this and `id_token_hint` are set, the provider must verify that
    /// this client id matches the one used when the ID token was issued.
    pub fn set_client_id(mut self, client_id: ClientId) -> Self {
        self.parameters.client_id = Some(client_id);
        self
    }

    /// Provides the OpenID Connect Provider with a URI to redirect to after
    /// the logout has been performed.
    pub fn set_post_logout_redirect_uri(mut self, redirect_uri: PostLogoutRedirectUrl) -> Self {
        self.parameters.post_logout_redirect_uri = Some(redirect_uri);
        self
    }

    /// Specify an opaque value that the OpenID Connect Provider should pass back
    /// to your application using the state parameter when redirecting to post_logout_redirect_uri.
    pub fn set_state(mut self, state: CsrfToken) -> Self {
        self.parameters.state = Some(state);
        self
    }

    /// Requests the preferred languages for the user interface presented by the OpenID Connect
    /// Provider.
    ///
    /// Languages should be added in order of preference.
    pub fn add_ui_locale(mut self, ui_locale: LanguageTag) -> Self {
        self.parameters.ui_locales.push(ui_locale);
        self
    }

    /// Returns the full logout URL. In order to logout, a GET request should be made to this URL
    /// by the client's browser.
    pub fn http_get_url(self) -> Url {
        let mut url = self.end_session_endpoint.url().to_owned();
        {
            let mut query = url.query_pairs_mut();

            macro_rules! add_pair {
                ($name:ident, $acc:expr) => {
                    if let Some($name) = self.parameters.$name {
                        query.append_pair(stringify!($name), $acc);
                    }
                };
            }

            add_pair!(id_token_hint, id_token_hint.as_str());
            add_pair!(logout_hint, logout_hint.secret());
            add_pair!(client_id, client_id.as_str());
            add_pair!(post_logout_redirect_uri, post_logout_redirect_uri.as_str());
            add_pair!(state, state.secret());

            if !self.parameters.ui_locales.is_empty() {
                query.append_pair("ui_locales", &join_vec(&self.parameters.ui_locales));
            }
        }

        if url.query() == Some("") {
            url.set_query(None);
        }

        url
    }
}

#[cfg(test)]
mod tests {
    use crate::core::{
        CoreGenderClaim, CoreJweContentEncryptionAlgorithm, CoreJwsSigningAlgorithm,
    };
    use crate::types::{LogoutHint, PostLogoutRedirectUrl};
    use crate::{
        AuthUrl, ClientId, CsrfToken, EmptyAdditionalClaims, EndSessionUrl, IdToken, IssuerUrl,
        JsonWebKeySetUrl, LanguageTag, LogoutProviderMetadata, LogoutRequest,
        ProviderMetadataWithLogout,
    };

    use url::Url;

    use std::str::FromStr;

    #[test]
    fn test_end_session_endpoint_deserialization() {
        // Fetched from: https://rp.certification.openid.net:8080/openidconnect-rs/
        //     rp-response_type-code/.well-known/openid-configuration
        // But pared down
        let json_response = "{\
            \"issuer\":\"https://rp.certification.openid.net:8080/openidconnect-rs/rp-response_type-code\",\
            \"authorization_endpoint\":\"https://rp.certification.openid.net:8080/openidconnect-rs/rp-response_type-code/authorization\",\
            \"jwks_uri\":\"https://rp.certification.openid.net:8080/static/jwks_3INbZl52IrrPCp2j.json\",\
            \"response_types_supported\":[],\
            \"subject_types_supported\":[],\
            \"id_token_signing_alg_values_supported\": [],\
            \"end_session_endpoint\":\"https://rp.certification.openid.net:8080/openidconnect-rs/rp-response_type-code/end_session\",\
            \"version\":\"3.0\"}";

        let new_provider_metadata = ProviderMetadataWithLogout::new(
            IssuerUrl::new(
                "https://rp.certification.openid.net:8080/openidconnect-rs/rp-response_type-code"
                    .to_string(),
            )
            .unwrap(),
            AuthUrl::new(
                "https://rp.certification.openid.net:8080/openidconnect-rs/\
                 rp-response_type-code/authorization"
                    .to_string(),
            )
            .unwrap(),
            JsonWebKeySetUrl::new(
                "https://rp.certification.openid.net:8080/static/jwks_3INbZl52IrrPCp2j.json"
                    .to_string(),
            )
            .unwrap(),
            vec![],
            vec![],
            vec![],
            LogoutProviderMetadata {
                end_session_endpoint: Some(EndSessionUrl::new(
                    "https://rp.certification.openid.net:8080/openidconnect-rs/rp-response_type-code/end_session"
                        .to_string()
                ).unwrap()),
                additional_metadata: Default::default(),
            },
        );

        let provider_metadata: ProviderMetadataWithLogout =
            serde_json::from_str(json_response).unwrap();
        assert_eq!(provider_metadata, new_provider_metadata);

        assert_eq!(
            Some(EndSessionUrl::new(
                "https://rp.certification.openid.net:8080/openidconnect-rs/rp-response_type-code/end_session"
                    .to_string()
            ).unwrap()),
            provider_metadata.additional_metadata().end_session_endpoint
        );
    }

    #[test]
    fn test_logout_request_with_no_parameters() {
        let endpoint = EndSessionUrl::new(
            "https://rp.certification.openid.net:8080/openidconnect-rs/rp-response_type-code/end_session"
                .to_string()
        ).unwrap();

        let logout_url = LogoutRequest::from(endpoint).http_get_url();

        assert_eq!(
            Url::parse(
                "https://rp.certification.openid.net:8080/openidconnect-rs/rp-response_type-code/end_session"
            ).unwrap(),
            logout_url
        );
    }

    #[test]
    fn test_logout_request_with_all_parameters() {
        let endpoint = EndSessionUrl::new(
            "https://rp.certification.openid.net:8080/openidconnect-rs/rp-response_type-code/end_session"
                .to_string()
        ).unwrap();

        let logout_url = LogoutRequest::from(endpoint)
            .set_id_token_hint(
                &IdToken::<
                    EmptyAdditionalClaims,
                    CoreGenderClaim,
                    CoreJweContentEncryptionAlgorithm,
                    CoreJwsSigningAlgorithm,
                >::from_str(
                    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwcz\
                    ovL3JwLmNlcnRpZmljYXRpb24ub3BlbmlkLm5ldDo4MDgwLyIsImV4c\
                    CI6MTUxNjIzOTAyMiwiaWF0IjoxNTE2MjM5MDIyLCJzdWIiOiJhc2Rm\
                    In0.cPwX6csO2uBEOZLVAGR7x5rHLRfD36MHpPy3JTk6orM",
                )
                .unwrap(),
            )
            .set_logout_hint(LogoutHint::new("johndoe".to_string()))
            .set_client_id(ClientId::new("asdf".to_string()))
            .set_post_logout_redirect_uri(
                PostLogoutRedirectUrl::new("https://localhost:8000/".to_string()).unwrap(),
            )
            .set_state(CsrfToken::new("asdf".to_string()))
            .add_ui_locale(LanguageTag::new("en-US".to_string()))
            .add_ui_locale(LanguageTag::new("fr-FR".to_string()))
            .http_get_url();

        assert_eq!(
            Url::parse(
                "https://rp.certification.openid.net:8080/openidconnect-rs\
                /rp-response_type-code/end_session?id_token_hint=eyJhbGciO\
                iJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL3JwLmNlcn\
                RpZmljYXRpb24ub3BlbmlkLm5ldDo4MDgwLyIsImV4cCI6MTUxNjIzOTAy\
                MiwiaWF0IjoxNTE2MjM5MDIyLCJzdWIiOiJhc2RmIn0.cPwX6csO2uBEOZ\
                LVAGR7x5rHLRfD36MHpPy3JTk6orM&logout_hint=johndoe&client_i\
                d=asdf&post_logout_redirect_uri=https%3A%2F%2Flocalhost%3A\
                8000%2F&state=asdf&ui_locales=en-US+fr-FR"
            )
            .unwrap(),
            logout_url
        );
    }
}
